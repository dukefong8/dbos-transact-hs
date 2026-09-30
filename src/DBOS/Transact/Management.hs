{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Workflow management operations on the launched instance. Transactional
-- validation and writes remain in the SystemDB class; this layer owns only
-- the launched-instance guard and engine error channel.
module DBOS.Transact.Management
  ( cancelWorkflows,
    cancelWorkflowsInWorkflow,
    resumeWorkflows,
    resumeWorkflowsInWorkflow,
    deleteWorkflows,
    deleteWorkflowsWithCaller,
    deleteWorkflowsInWorkflow,
    forkWorkflows,
    forkWorkflowsInWorkflow,
    forkFrom,
    forkFromInWorkflow,
    updateWorkflowAttributes,
    listWorkflows,
    listWorkflowsInWorkflow,
  )
where

import DBOS.Prelude
import Data.Text (Text)
import Data.Word (Word64)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Types (Fork, ForkOptions, ForkPoint, WorkflowFilter, WorkflowId (..), WorkflowRecord, cancelWorkflowStepName, deleteWorkflowStepName, forkOptionsValidate, forkValidate, forkWorkflowStepName, listWorkflowsStepName, resumeWorkflowStepName)
import DBOS.Transact.Connection (Connection (..), runSystemDB)
import DBOS.Transact.Context (Ctx, currentConnection, stepId, workflowId)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Step (runWorkflowStepWith, stepOptionsDefault)
import DBOS.Tracer (ManagementEvent (..), traceWith)

cancelWorkflows :: Monad m => Connection m -> [WorkflowId] -> Bool -> m (Either TransactError.Error [WorkflowId])
cancelWorkflows conn workflowIds cancelChildren = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.cancelWorkflows db workflowIds cancelChildren Nothing)
  case result of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    -- Mirrors @cancel_all@: what moved, not what was asked for — silence
    -- when nothing moved.
    Right cancelled -> do
      if null cancelled
        then pure ()
        else traceWith conn.connTracer (WorkflowsCancelled (length cancelled))
      pure (Right cancelled)

-- | Cancels workflows as a step of the calling workflow: the call takes a
-- step id, runs once, and replays its recorded ids. Same leaf rule as
-- every in-workflow call — inside a step body it runs plainly.
cancelWorkflowsInWorkflow :: (MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) => Ctx m -> [WorkflowId] -> Bool -> m (Either TransactError.Error [WorkflowId])
cancelWorkflowsInWorkflow ctx workflowIds cancelChildren =
  runWorkflowStepWith
    stepOptionsDefault
    ctx
    cancelWorkflowStepName
    (\inner -> cancelWorkflows (currentConnection inner) workflowIds cancelChildren)

resumeWorkflows :: Monad m => Connection m -> [WorkflowId] -> Maybe Text -> m (Either TransactError.Error [WorkflowId])
resumeWorkflows conn workflowIds queueName = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.resumeWorkflows db workflowIds queueName Nothing)
  case result of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    -- Mirrors @resume_all@: what moved against what was asked — an id that
    -- had already finished still counts as requested, so this announces
    -- unconditionally.
    Right resumed -> do
      traceWith conn.connTracer (WorkflowsResumed (length workflowIds) (length resumed))
      pure (Right resumed)

-- | Resumes workflows as a step of the calling workflow. Same leaf rule
-- as every in-workflow call.
resumeWorkflowsInWorkflow :: (MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) => Ctx m -> [WorkflowId] -> Maybe Text -> m (Either TransactError.Error [WorkflowId])
resumeWorkflowsInWorkflow ctx workflowIds queueName =
  runWorkflowStepWith
    stepOptionsDefault
    ctx
    resumeWorkflowStepName
    (\inner -> resumeWorkflows (currentConnection inner) workflowIds queueName)

deleteWorkflows :: Monad m => Connection m -> [WorkflowId] -> Bool -> m (Either TransactError.Error Word64)
deleteWorkflows conn workflowIds deleteChildren =
  deleteWorkflowsWithCaller conn workflowIds deleteChildren Nothing

-- | Deletes workflows naming the calling workflow's step, so the backend
-- refuses a target set containing the caller itself. The engine's
-- in-workflow entry passes the step the call was checkpointed under.
deleteWorkflowsWithCaller :: Monad m => Connection m -> [WorkflowId] -> Bool -> Maybe (WorkflowId, Int) -> m (Either TransactError.Error Word64)
deleteWorkflowsWithCaller conn workflowIds deleteChildren caller = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.deleteWorkflows db workflowIds deleteChildren caller)
  case result of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    -- Mirrors @delete_all@: silence when no rows went.
    Right deleted -> do
      if deleted == 0
        then pure ()
        else traceWith conn.connTracer (WorkflowsDeleted deleted)
      pure (Right deleted)

-- | Deletes workflows as a step of the calling workflow: the call takes a
-- step id, runs once, and replays its recorded count. A refusal (the
-- target set holds the caller) carries no checkpoint — a control error,
-- like every system-database failure — so a replay refuses again rather
-- than replaying a delete. Inside a step body the call runs plainly with
-- no caller, by the leaf rule.
deleteWorkflowsInWorkflow :: (MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) => Ctx m -> [WorkflowId] -> Bool -> m (Either TransactError.Error Word64)
deleteWorkflowsInWorkflow ctx workflowIds deleteChildren =
  runWorkflowStepWith
    stepOptionsDefault
    ctx
    deleteWorkflowStepName
    ( \inner -> case stepId inner of
        Just sid ->
          deleteWorkflowsWithCaller
            (currentConnection inner)
            workflowIds
            deleteChildren
            (Just (WorkflowId (workflowId inner), sid))
        Nothing -> deleteWorkflows (currentConnection inner) workflowIds deleteChildren
    )

forkWorkflows :: Monad m => Connection m -> [Fork] -> ForkOptions -> m (Either TransactError.Error [WorkflowId])
forkWorkflows conn forks options = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.forkWorkflows db forks options Nothing)
  case result of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    Right forked -> do
      announceFork conn forked
      pure (Right forked)

-- | Forks workflows as a step of the calling workflow: a fork generates a
-- new id, so a replay without this checkpoint would write a second fork
-- under a second id. Arguments are refused before the step id is taken,
-- so a refused call spends nothing. Same leaf rule as every in-workflow
-- call.
forkWorkflowsInWorkflow :: (MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) => Ctx m -> [Fork] -> ForkOptions -> m (Either TransactError.Error [WorkflowId])
forkWorkflowsInWorkflow ctx forks options =
  case (forkOptionsValidate options, traverse forkValidate forks) of
    (Left err, _) -> pure (Left (TransactError.ErrorSystemDatabase err))
    (_, Left err) -> pure (Left (TransactError.ErrorSystemDatabase err))
    (Right (), Right _) ->
      runWorkflowStepWith
        stepOptionsDefault
        ctx
        forkWorkflowStepName
        (\inner -> forkWorkflows (currentConnection inner) forks options)

forkFrom :: Monad m => Connection m -> [WorkflowId] -> ForkPoint -> ForkOptions -> m (Either TransactError.Error [WorkflowId])
forkFrom conn workflowIds point options = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.forkFrom db workflowIds point options Nothing)
  case result of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    Right forked -> do
      announceFork conn forked
      pure (Right forked)

-- | The fork announcement shared by both fork paths, mirroring
-- @fork_all@: one id names its fork, any other count — including zero —
-- reports the batch size.
announceFork :: Monad m => Connection m -> [WorkflowId] -> m ()
announceFork conn forked = case forked of
  [WorkflowId only] -> traceWith conn.connTracer (WorkflowForked only)
  many -> traceWith conn.connTracer (WorkflowsForked (length many))

-- | Forks from a point as a step of the calling workflow. Arguments are
-- refused before the step id is taken. Same leaf rule as every
-- in-workflow call.
forkFromInWorkflow :: (MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) => Ctx m -> [WorkflowId] -> ForkPoint -> ForkOptions -> m (Either TransactError.Error [WorkflowId])
forkFromInWorkflow ctx workflowIds point options =
  case forkOptionsValidate options of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    Right () ->
      runWorkflowStepWith
        stepOptionsDefault
        ctx
        forkWorkflowStepName
        (\inner -> forkFrom (currentConnection inner) workflowIds point options)

-- | Replaces the attributes attached to a workflow, or clears them when
-- given 'Nothing'. Mirrors Rust @DBOS::update_workflow_attributes@ outside
-- a workflow, where the call is plain: the encoding happens before any id
-- could be taken, and the backend replaces rather than merges.
updateWorkflowAttributes :: Monad m => Connection m -> WorkflowId -> Maybe Text -> m (Either TransactError.Error ())
updateWorkflowAttributes conn workflowId attributes = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.updateWorkflowAttributes db workflowId attributes Nothing)
  case result of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    -- Mirrors @update_workflow_attributes@: phrased as the request, not the
    -- effect — no count comes back, so this announces unconditionally.
    Right () -> do
      let WorkflowId workflowText = workflowId
      traceWith conn.connTracer (WorkflowAttributesReplaceAsked workflowText)
      pure (Right ())

-- | Reads the workflows matching a filter. Mirrors Rust
-- @DBOS::list_workflows@ outside a workflow, where the call is plain: every
-- filter is one @WHERE@ clause, so the default returns the whole table.
listWorkflows :: Monad m => Connection m -> WorkflowFilter -> m (Either TransactError.Error [WorkflowRecord])
listWorkflows conn filters = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.listWorkflows db filters Nothing)
  pure (either (Left . TransactError.ErrorSystemDatabase) Right result)

-- | Lists workflows as a step of the calling workflow, under the
-- cross-SDK name, so a step listing reads the same whichever SDK wrote
-- it. Same leaf rule as every in-workflow call.
listWorkflowsInWorkflow :: (MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) => Ctx m -> WorkflowFilter -> m (Either TransactError.Error [WorkflowRecord])
listWorkflowsInWorkflow ctx filters =
  runWorkflowStepWith
    stepOptionsDefault
    ctx
    listWorkflowsStepName
    (\inner -> listWorkflows (currentConnection inner) filters)
