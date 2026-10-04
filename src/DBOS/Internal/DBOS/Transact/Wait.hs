{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | External workflow waits through the running instance. SystemDB owns the
-- poll-and-wakeup semantics; this module supplies the connection's
-- configured polling interval and translates backend failures into engine
-- errors.
module DBOS.Transact.Wait
  ( -- * From inside a workflow
    selectWorkflow,
    joinWorkflows,
    -- * From the running instance
    waitForWorkflow,
    waitForFirstWorkflow,
    waitForWorkflows,
    -- * Tracing
    WaitEvent (..),
  )
where

import DBOS.Prelude
import DBOS.SystemDB.Class qualified as SystemDB
import Data.Text (Text)
import Data.Text qualified as Text
import System.Log.FastLogger (ToLogStr (..))
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (AwaitedOutcome, Outcome (..), Serialization (..), SerializedWorkflowValue (..), StepRecord (..), StepTiming (..), WorkflowId (..), selectWorkflowStepName, timestampNow)
import DBOS.Tracer (LogEvent (..), LogSeverity (..), runTracer)
import DBOS.Transact.Serialization (CodecError (..), decodeWorkflowValue, encodeWorkflowValue)
import DBOS.Transact.Connection (Connection (..), runSystemDB)
import DBOS.Transact.Context (WorkflowCtx, nextWorkflowStepId, withSystemDB, workflowConnection, workflowCtxId, workflowTracer)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Instance (DBOS, Executor (..), requireExecutor)

-- | Announcements from the wait paths, homed here with their owner.
-- Mirrors the @wait.rs@ debug sites: a replayed @select_workflow@ reads
-- its recorded winner back. The all-wait takes no placement and stays
-- quiet, as in the oracle.
data WaitEvent
  = SelectWorkflowReplaying { selectReplayedWinner :: Text }
  deriving stock (Eq, Show)

instance LogEvent WaitEvent where
  eventSeverity SelectWorkflowReplaying {} = SeverityDebug
  renderEvent (SelectWorkflowReplaying winner) =
    "replaying select_workflow; the same workflow wins again workflow_id=" <> winner

instance ToLogStr WaitEvent where
  toLogStr = toLogStr . renderLine

-- | Wait for the first of a set of workflows to finish, checkpointed as the
-- @DBOS.selectWorkflow@ step so a replay reads the same winner back. A set
-- with no ids has no answer it could give, so it is refused and the refusal
-- is recorded, exactly as the Rust @select_workflow@ records it. A recorded
-- winner must still be in the set: a workflow that changed which ids it
-- waits on has changed what this position of its code means. (Deviation: the
-- step id is taken at the call, not at the build, as everywhere in this
-- port.)
selectWorkflow :: (MonadSTM m, MonadDelay m, MonadTime m) => WorkflowCtx exec m -> [WorkflowId] -> m (Either (TransactError.Error TransactError.EngineOnly) WorkflowId)
selectWorkflow wctx workflowIds = do
  let workflowText = workflowCtxId wctx
      workflowId' = WorkflowId workflowText
      interval = (workflowConnection wctx).connOutcomePollInterval
  stepId' <- nextWorkflowStepId wctx
  checked <- withSystemDB wctx (\db -> SystemDB.checkStep db workflowId' stepId' selectWorkflowStepName)
  case checked of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    Right (Just recorded) ->
      case recorded.stepRecordError of
        -- The only refusal this call records is the empty set, so a replay
        -- reads that refusal back rather than re-deciding it — the Rust
        -- revive of the recorded error, narrowed to this call's one error.
        Just _errorText -> pure (Left (TransactError.InvalidArgument "select_workflow" "no workflow ids to wait for"))
        Nothing -> case recorded.stepRecordOutput of
          Nothing -> pure (Left (TransactError.StepFailed selectWorkflowStepName "recorded select_workflow has no output"))
          Just output ->
            case
                ( decodeWorkflowValue
                    "the id that won a select_workflow"
                    (Just (SerializedWorkflowValue output (Serialization <$> recorded.stepRecordSerialization))) ::
                    Either CodecError Text
                )
              of
              Left err -> pure (Left (TransactError.ErrorDeserialization "select_workflow" (codecMessage err)))
              Right winner ->
                if WorkflowId winner `elem` workflowIds
                  then do
                    runTracer (workflowTracer wctx) (SelectWorkflowReplaying winner)
                    pure (Right (WorkflowId winner))
                  else
                    pure
                      ( Left
                          ( TransactError.ErrorSystemDatabase
                              SystemDBError.UnexpectedStep
                                { workflowId = workflowText,
                                  stepId = stepId',
                                  expected = "a select_workflow over " <> summarize workflowIds,
                                  recorded = "a select_workflow won by " <> winner
                                }
                          )
                      )
    Right Nothing -> do
      startedAt <- timestampNow
      if null workflowIds
        then do
          let refused = TransactError.InvalidArgument "select_workflow" "no workflow ids to wait for" :: (TransactError.Error TransactError.EngineOnly)
          _ <-
            withSystemDB
              wctx
              ( \db ->
                  SystemDB.recordStep db workflowId' stepId' selectWorkflowStepName (OutcomeError (TransactError.encodeErrorText refused)) Nothing (Just (StepTiming startedAt startedAt))
              )
          pure (Left refused)
        else do
          winner <- withSystemDB wctx (\db -> SystemDB.awaitFirstWorkflowId db workflowIds interval)
          case winner of
            Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
            Right winnerId@(WorkflowId winnerText') -> do
              completedAt <- timestampNow
              let encoded = encodeWorkflowValue winnerText'
                  serialization = case encoded.serializedSerialization of
                    Nothing -> Nothing
                    Just (Serialization name) -> Just name
              _ <-
                withSystemDB
                  wctx
                  ( \db ->
                      SystemDB.recordStep db workflowId' stepId' selectWorkflowStepName (OutcomeOutput (Just encoded.serializedText)) serialization (Just (StepTiming startedAt completedAt))
                  )
              pure (Right winnerId)
  where
    codecMessage err =
      case err of
        CodecNotJson _ input -> "invalid JSON: " <> input
        CodecTypeMismatch _ detail -> Text.pack detail
    summarize ids =
      "[" <> Text.intercalate ", " [text | WorkflowId text <- ids] <> "]"

-- | Wait for every workflow in a set to finish. The all-wait pins nothing
-- worth a step id, so it is not checkpointed — the Rust @join_workflows@
-- takes no placement either.
joinWorkflows :: (MonadSTM m, MonadDelay m, MonadTime m) => WorkflowCtx exec m -> [WorkflowId] -> m (Either (TransactError.Error TransactError.EngineOnly) ())
joinWorkflows wctx workflowIds = do
  let interval = (workflowConnection wctx).connOutcomePollInterval
  result <- withSystemDB wctx (\db -> SystemDB.awaitWorkflowIds db workflowIds interval)
  pure (either (Left . TransactError.ErrorSystemDatabase) Right result)

waitForWorkflow :: (MonadMVar m, MonadDelay m, MonadTime m) => DBOS m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) AwaitedOutcome)
waitForWorkflow dbos awaitedWorkflowId = do
  running <- requireExecutor dbos "wait for a workflow"
  case running of
    Left err -> pure (Left err)
    Right executor -> do
      result <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.awaitWorkflowResult db awaitedWorkflowId executor.conn.connOutcomePollInterval True)
      pure (either (Left . TransactError.ErrorSystemDatabase) Right result)

waitForFirstWorkflow :: (MonadMVar m, MonadDelay m, MonadTime m) => DBOS m -> [WorkflowId] -> m (Either (TransactError.Error TransactError.EngineOnly) WorkflowId)
waitForFirstWorkflow dbos workflowIds = do
  running <- requireExecutor dbos "wait for the first workflow"
  case running of
    Left err -> pure (Left err)
    Right executor -> do
      result <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.awaitFirstWorkflowId db workflowIds executor.conn.connOutcomePollInterval)
      pure (either (Left . TransactError.ErrorSystemDatabase) Right result)

waitForWorkflows :: (MonadMVar m, MonadDelay m, MonadTime m) => DBOS m -> [WorkflowId] -> m (Either (TransactError.Error TransactError.EngineOnly) ())
waitForWorkflows dbos workflowIds = do
  running <- requireExecutor dbos "wait for workflows"
  case running of
    Left err -> pure (Left err)
    Right executor -> do
      result <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.awaitWorkflowIds db workflowIds executor.conn.connOutcomePollInterval)
      pure (either (Left . TransactError.ErrorSystemDatabase) Right result)
