{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Workflow management operations on the launched instance. Transactional
-- validation and writes remain in the SystemDB class; this layer owns only
-- the launched-instance guard and engine error channel.
module DBOS.Transact.Management
  ( ManagementEvent (..),
    cancelWorkflows,
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
import System.Log.FastLogger (ToLogStr (..))
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Types (Fork, ForkOptions, ForkPoint, WorkflowFilter, WorkflowId (..), WorkflowRecord, cancelWorkflowStepName, deleteWorkflowStepName, forkOptionsValidate, forkValidate, forkWorkflowStepName, listWorkflowsStepName, resumeWorkflowStepName)
import DBOS.Transact.Connection (Connection (..), runSystemDB)
import DBOS.Transact.Context (WorkflowCtx, currentConnection, stepCtxId, stepCtxStatus, stepStatusId, workflowCtxInner)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Step (runWorkflowStepWith, stepOptionsDefault)
import DBOS.Tracer (LogEvent (..), LogSeverity (..), runTracer)

-- | Operator-action events: the management surface's announcements.
-- Rendered lines keep the Rust @tracing!@ message bodies with their
-- @key=value@ span fields, so operators see the same text. Mirrors
-- @management.rs@'s @Connection@ impl: cancel guards on non-empty, delete
-- on a positive count, fork splits one id from many, the rest announce
-- unconditionally; the reads log nothing.
data ManagementEvent
  = WorkflowsCancelled { managementCancelled :: Int }
  | WorkflowsResumed { managementRequested :: Int, managementResumed :: Int }
  | WorkflowForked { managementForkedId :: Text }
  | WorkflowsForked { managementForked :: Int }
  | WorkflowsDeleted { managementDeleted :: Word64 }
  | WorkflowDelayMoveAsked { managementWorkflowId :: Text }
  | WorkflowAttributesReplaceAsked { managementWorkflowId :: Text }
  deriving stock (Eq, Show)

instance LogEvent ManagementEvent where
  eventSeverity WorkflowsCancelled {}             = SeverityInfo
  eventSeverity WorkflowsResumed {}               = SeverityInfo
  eventSeverity WorkflowForked {}                 = SeverityInfo
  eventSeverity WorkflowsForked {}                = SeverityInfo
  eventSeverity WorkflowsDeleted {}               = SeverityInfo
  eventSeverity WorkflowDelayMoveAsked {}         = SeverityInfo
  eventSeverity WorkflowAttributesReplaceAsked {} = SeverityInfo
  renderEvent (WorkflowsCancelled cancelled) =
    "cancelled workflows cancelled=" <> showText cancelled
  renderEvent (WorkflowsResumed requested resumed) =
    "resumed workflows onto their queues requested=" <> showText requested <> " resumed=" <> showText resumed
  renderEvent (WorkflowForked forkedId) =
    "forked the workflow onto its queue forked_id=" <> forkedId
  renderEvent (WorkflowsForked count) =
    "forked workflows onto their queues count=" <> showText count
  renderEvent (WorkflowsDeleted deleted) =
    "deleted workflows deleted=" <> showText deleted
  renderEvent (WorkflowDelayMoveAsked workflowId) =
    "asked to move the workflow's release time workflow_id=" <> workflowId
  renderEvent (WorkflowAttributesReplaceAsked workflowId) =
    "asked to replace the workflow's attributes workflow_id=" <> workflowId

instance ToLogStr ManagementEvent where
  toLogStr = toLogStr . renderLine

cancelWorkflows :: Monad m => Connection m -> [WorkflowId] -> Bool -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
cancelWorkflows conn workflowIds cancelChildren = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.cancelWorkflows db workflowIds cancelChildren Nothing)
  case result of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    -- Mirrors @cancel_all@: what moved, not what was asked for — silence
    -- when nothing moved.
    Right cancelled -> do
      if null cancelled
        then pure ()
        else runTracer conn.connTracer (WorkflowsCancelled (length cancelled))
      pure (Right cancelled)

-- | Cancels workflows as a step of the calling workflow: the call takes a
-- step id, runs once, and replays its recorded ids. Same leaf rule as
-- every in-workflow call — inside a step body it runs plainly.
cancelWorkflowsInWorkflow :: (MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) => WorkflowCtx exec m -> [WorkflowId] -> Bool -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
cancelWorkflowsInWorkflow wctx workflowIds cancelChildren =
  runWorkflowStepWith
    stepOptionsDefault
    wctx
    cancelWorkflowStepName
    (\_ -> cancelWorkflows conn workflowIds cancelChildren)
  where
    conn = currentConnection (workflowCtxInner wctx)

resumeWorkflows :: Monad m => Connection m -> [WorkflowId] -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
resumeWorkflows conn workflowIds queueName = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.resumeWorkflows db workflowIds queueName Nothing)
  case result of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    -- Mirrors @resume_all@: what moved against what was asked — an id that
    -- had already finished still counts as requested, so this announces
    -- unconditionally.
    Right resumed -> do
      runTracer conn.connTracer (WorkflowsResumed (length workflowIds) (length resumed))
      pure (Right resumed)

-- | Resumes workflows as a step of the calling workflow. Same leaf rule
-- as every in-workflow call.
resumeWorkflowsInWorkflow :: (MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) => WorkflowCtx exec m -> [WorkflowId] -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
resumeWorkflowsInWorkflow wctx workflowIds queueName =
  runWorkflowStepWith
    stepOptionsDefault
    wctx
    resumeWorkflowStepName
    (\_ -> resumeWorkflows conn workflowIds queueName)
  where
    conn = currentConnection (workflowCtxInner wctx)

deleteWorkflows :: Monad m => Connection m -> [WorkflowId] -> Bool -> m (Either (TransactError.Error TransactError.EngineOnly) Word64)
deleteWorkflows conn workflowIds deleteChildren =
  deleteWorkflowsWithCaller conn workflowIds deleteChildren Nothing

-- | Deletes workflows naming the calling workflow's step, so the backend
-- refuses a target set containing the caller itself. The engine's
-- in-workflow entry passes the step the call was checkpointed under.
deleteWorkflowsWithCaller :: Monad m => Connection m -> [WorkflowId] -> Bool -> Maybe (WorkflowId, Int) -> m (Either (TransactError.Error TransactError.EngineOnly) Word64)
deleteWorkflowsWithCaller conn workflowIds deleteChildren caller = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.deleteWorkflows db workflowIds deleteChildren caller)
  case result of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    -- Mirrors @delete_all@: silence when no rows went.
    Right deleted -> do
      if deleted == 0
        then pure ()
        else runTracer conn.connTracer (WorkflowsDeleted deleted)
      pure (Right deleted)

-- | Deletes workflows as a step of the calling workflow: the call takes a
-- step id, runs once, and replays its recorded count. A refusal (the
-- target set holds the caller) carries no checkpoint — a control error,
-- like every system-database failure — so a replay refuses again rather
-- than replaying a delete. Inside a step body the call runs plainly with
-- no caller, by the leaf rule.
deleteWorkflowsInWorkflow :: (MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) => WorkflowCtx exec m -> [WorkflowId] -> Bool -> m (Either (TransactError.Error TransactError.EngineOnly) Word64)
deleteWorkflowsInWorkflow wctx workflowIds deleteChildren =
  runWorkflowStepWith
    stepOptionsDefault
    wctx
    deleteWorkflowStepName
    ( \sctx -> case stepCtxStatus sctx of
        Just status ->
          deleteWorkflowsWithCaller
            conn
            workflowIds
            deleteChildren
            (Just (WorkflowId (stepCtxId sctx), stepStatusId status))
        Nothing -> deleteWorkflows conn workflowIds deleteChildren
    )
  where
    conn = currentConnection (workflowCtxInner wctx)

forkWorkflows :: Monad m => Connection m -> [Fork] -> ForkOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
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
forkWorkflowsInWorkflow :: (MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) => WorkflowCtx exec m -> [Fork] -> ForkOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
forkWorkflowsInWorkflow wctx forks options =
  case (forkOptionsValidate options, traverse forkValidate forks) of
    (Left err, _) -> pure (Left (TransactError.ErrorSystemDatabase err))
    (_, Left err) -> pure (Left (TransactError.ErrorSystemDatabase err))
    (Right (), Right _) ->
      runWorkflowStepWith
        stepOptionsDefault
        wctx
        forkWorkflowStepName
        (\_ -> forkWorkflows conn forks options)
  where
    conn = currentConnection (workflowCtxInner wctx)

forkFrom :: Monad m => Connection m -> [WorkflowId] -> ForkPoint -> ForkOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
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
  [WorkflowId only] -> runTracer conn.connTracer (WorkflowForked only)
  many -> runTracer conn.connTracer (WorkflowsForked (length many))

-- | Forks from a point as a step of the calling workflow. Arguments are
-- refused before the step id is taken. Same leaf rule as every
-- in-workflow call.
forkFromInWorkflow :: (MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) => WorkflowCtx exec m -> [WorkflowId] -> ForkPoint -> ForkOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
forkFromInWorkflow wctx workflowIds point options =
  case forkOptionsValidate options of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    Right () ->
      runWorkflowStepWith
        stepOptionsDefault
        wctx
        forkWorkflowStepName
        (\_ -> forkFrom conn workflowIds point options)
  where
    conn = currentConnection (workflowCtxInner wctx)

-- | Replaces the attributes attached to a workflow, or clears them when
-- given 'Nothing'. Mirrors Rust @DBOS::update_workflow_attributes@ outside
-- a workflow, where the call is plain: the encoding happens before any id
-- could be taken, and the backend replaces rather than merges.
updateWorkflowAttributes :: Monad m => Connection m -> WorkflowId -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
updateWorkflowAttributes conn workflowId attributes = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.updateWorkflowAttributes db workflowId attributes Nothing)
  case result of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    -- Mirrors @update_workflow_attributes@: phrased as the request, not the
    -- effect — no count comes back, so this announces unconditionally.
    Right () -> do
      let WorkflowId workflowText = workflowId
      runTracer conn.connTracer (WorkflowAttributesReplaceAsked workflowText)
      pure (Right ())

-- | Reads the workflows matching a filter. Mirrors Rust
-- @DBOS::list_workflows@ outside a workflow, where the call is plain: every
-- filter is one @WHERE@ clause, so the default returns the whole table.
listWorkflows :: Monad m => Connection m -> WorkflowFilter -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowRecord])
listWorkflows conn filters = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.listWorkflows db filters Nothing)
  pure (either (Left . TransactError.ErrorSystemDatabase) Right result)

-- | Lists workflows as a step of the calling workflow, under the
-- cross-SDK name, so a step listing reads the same whichever SDK wrote
-- it. Same leaf rule as every in-workflow call.
listWorkflowsInWorkflow :: (MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) => WorkflowCtx exec m -> WorkflowFilter -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowRecord])
listWorkflowsInWorkflow wctx filters =
  runWorkflowStepWith
    stepOptionsDefault
    wctx
    listWorkflowsStepName
    (\_ -> listWorkflows conn filters)
  where
    conn = currentConnection (workflowCtxInner wctx)
