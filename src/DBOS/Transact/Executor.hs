{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Internal executor lifecycle (Rule 4: plain Haskell, no Bluefin imports).
-- Mirrors @instance.rs@ and the dequeue half of @dequeue.rs@: launch runs
-- the recovery sweep synchronously before returning, so it never tears a
-- workflow off a runner that started the instant it did. Spawned workflows
-- are tracked so shutdown reaches them; their rows stay @PENDING@, which is
-- the point — a later launch recovers them. A task arriving after the sweep
-- is cancelled on arrival rather than outliving the shutdown meant to stop
-- it.
module DBOS.Transact.Executor
  ( Executor (..),
    dequeuePass,
    launchExecutor,
    shutdownExecutor,
    spawnWorkflow,
  )
where

import DBOS.Prelude
import Colog.Core.Action (LogAction (..))
import Control.Monad (filterM, unless, when)
import Data.List ((\\))
import Data.Maybe (isNothing)
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.SystemDB.Types (ApplicationVersion, ExecutorId, QueueName, SerializedWorkflowValue, WorkflowId (..), WorkflowName (..), internalQueueName)
import DBOS.Transact.Log (DbosLogMsg (..), DbosSeverity (..))
import DBOS.Transact.Registry (WorkflowRegistry, lookupWorkflow)
import DBOS.Transact.Workflow (WorkflowRunError, runWorkflow)
import DBOS.Transact.WorkflowExecutionParse (parseWorkflowExecution)
import DBOS.Transact.WorkflowExecutionTypes
  ( WorkflowExecution (..),
  )

data Executor = Executor
  { executorPool :: Postgres.Pool,
    executorId :: ExecutorId,
    executorVersion :: ApplicationVersion,
    executorRegistry :: WorkflowRegistry,
    executorLogger :: LogAction IO DbosLogMsg,
    executorTasks :: StrictTVar IO [Async IO (Either WorkflowRunError SerializedWorkflowValue)],
    executorClosed :: StrictTVar IO Bool
  }

launchExecutor ::
  Postgres.Pool ->
  ExecutorId ->
  ApplicationVersion ->
  WorkflowRegistry ->
  LogAction IO DbosLogMsg ->
  IO (Executor, [WorkflowId])
launchExecutor pool executorId version registry logger = do
  recovered <- Postgres.legacyReenqueueForRecovery pool executorId version internalQueueName
  tasks <- newTVarIO []
  closed <- newTVarIO False
  pure (Executor pool executorId version registry logger tasks closed, recovered)

-- | Run a registered workflow as a tracked task, pruning the handles of any
-- that have since finished. A task arriving after the sweep is cancelled on
-- arrival.
spawnWorkflow ::
  Executor ->
  WorkflowName ->
  WorkflowId ->
  Maybe SerializedWorkflowValue ->
  IO (Async IO (Either WorkflowRunError SerializedWorkflowValue))
spawnWorkflow executor name workflowId input = do
  task <- async (runWorkflow executor.executorPool executor.executorRegistry name workflowId input executor.executorId executor.executorVersion)
  arrivalsClosed <- atomically $ do
    modifyTVar executor.executorTasks (task :)
    readTVar executor.executorClosed
  when arrivalsClosed (cancel task)
  reapFinishedTasks executor
  pure task

-- | One supervisor pass over a queue: claim up to the stored limit and
-- dispatch every claimed workflow through the registry. Rows that name
-- nothing registered, or that no longer decode, are skipped with a logged
-- warning and their claim is released back to @ENQUEUED@, so a later pass
-- with the registration present picks them up instead of parking them
-- @PENDING@ forever.
dequeuePass ::
  Executor ->
  QueueName ->
  IO [Async IO (Either WorkflowRunError SerializedWorkflowValue)]
dequeuePass executor queueName = do
  claimed <-
    Postgres.dequeueWorkflows
      executor.executorPool
      queueName
      executor.executorId
      executor.executorVersion
  tasks <- traverse dispatch claimed
  pure [task | Just task <- tasks]
  where
    dispatch workflowId = do
      named <- resolveName workflowId
      case named of
        Nothing -> do
          Postgres.releaseWorkflowClaim executor.executorPool executor.executorId workflowId
          unLogAction executor.executorLogger (DbosLogMsg DbosWarn "dequeue skipped a workflow with no readable row" (Just workflowId))
          pure Nothing
        Just (name, input) -> case lookupWorkflow name executor.executorRegistry of
          Nothing -> do
            Postgres.releaseWorkflowClaim executor.executorPool executor.executorId workflowId
            unLogAction executor.executorLogger (DbosLogMsg DbosWarn "dequeue skipped an unregistered workflow" (Just workflowId))
            pure Nothing
          Just _ -> Just <$> spawnWorkflow executor name workflowId input
    resolveName workflowId = do
      row <- Postgres.fetchWorkflowExecutionRow executor.executorPool workflowId
      case row of
        Nothing -> pure Nothing
        Just statusRow -> case parseWorkflowExecution statusRow of
          Left _ -> pure Nothing
          Right execution -> pure ((,) <$> execution.workflowExecutionName <*> pure execution.workflowExecutionInputs)

-- | Cancel every tracked workflow and wait for quiet, draining late
-- arrivals: a spawn racing the snapshot is cancelled on arrival and reaped
-- by the next round. Their rows stay @PENDING@: a cancelled workflow is one
-- a later launch recovers, so an abrupt shutdown loses no work.
shutdownExecutor :: Executor -> IO ()
shutdownExecutor executor = do
  atomically (writeTVar executor.executorClosed True)
  drain
  where
    drain = do
      tasks <- readTVarIO executor.executorTasks
      mapM_ cancel tasks
      mapM_ waitCatch tasks
      reapFinishedTasks executor
      remaining <- readTVarIO executor.executorTasks
      unless (null remaining) drain

-- | Drop exactly the finished handles under an atomic update. Shared by the
-- spawn hot path and the shutdown drain.
reapFinishedTasks :: Executor -> IO ()
reapFinishedTasks executor = do
  tasks <- readTVarIO executor.executorTasks
  alive <- filterM (fmap isNothing . poll) tasks
  atomically (modifyTVar executor.executorTasks (\\ (tasks \\ alive)))
