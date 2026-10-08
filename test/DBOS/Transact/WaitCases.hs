{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | Shared in-workflow wait scenarios: one body per case, judged by one
-- pure check on each stack. The fixture carries workflow creation over
-- initialized rows, running selects and joins through a context, settling
-- and cancelling rows, and reading the select checkpoint. The live tree
-- ('DBOS.Transact.WaitTest') runs them over Postgres rows, the sim tree
-- ('DBOS.Transact.WaitTestSim') over the in-memory backend, and both
-- prove the same wait record. Fresh ids take a label so sim traces name
-- their winners deterministically.
module DBOS.Transact.WaitCases
  ( WaitFixture (..),
    wfRun,
    scenarioRefusal,
    scenarioWinnerLeft,
    scenarioJoinSettled,
    scenarioSelectFirst,
    scenarioSelectSettledFirst,
    scenarioReplayWinner,
    scenarioCancelled,
    scenarioJoinLast,
    scenarioEmptyAll,
    scenarioRepeated,
    checkRefusal,
    checkWinnerLeft,
    checkJoinSettled,
    checkSelectFirst,
    checkSelectSettledFirst,
    checkReplayWinner,
    checkCancelled,
    checkJoinLast,
    checkEmptyAll,
    checkRepeated,
  )
where

import DBOS.Prelude
import Data.Text qualified as Text
import DBOS.SystemDB (StepRecord (..), WorkflowId (..))
import DBOS.SystemDB qualified as SysDB
import DBOS.Transact
  ( EngineOnly,
    Error (..),
    WorkflowCtx,
    joinWorkflows,
    selectWorkflow,
  )

-- | What a stack must provide: labeled fresh workflow ids over initialized
-- rows, running selects and joins through a context for an id, settling
-- and cancelling rows, and reading the select checkpoint.
data WaitFixture m = WaitFixture
  { wfFreshWorkflowId :: Text -> m WorkflowId,
    wfCtx :: forall a. WorkflowId -> (forall exec. WorkflowCtx exec m -> m a) -> m a,
    wfSettle :: WorkflowId -> m (),
    wfCancel :: WorkflowId -> m (),
    wfCheckStep :: WorkflowId -> m (Maybe StepRecord)
  }

-- | Run a wait under a fresh scope: the rank-2 field is read by pattern
-- match because record-dot has no 'HasField' instance for polymorphic
-- fields.
wfRun :: WaitFixture m -> WorkflowId -> (forall exec. WorkflowCtx exec m -> m a) -> m a
wfRun (WaitFixture _ run _ _ _) = run

widTextOf :: WorkflowId -> Text
widTextOf (WorkflowId text) = text

-- | Ported from Rust @tests/waits.rs@: an empty first wait records its
-- refusal and a replay reads it back rather than deciding again.
scenarioRefusal :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
                => WaitFixture m -> m (Either (Error EngineOnly) WorkflowId, Maybe StepRecord, Either (Error EngineOnly) WorkflowId)
scenarioRefusal fx = do
  wid <- fx.wfFreshWorkflowId "refusal"
  first <- wfRun fx wid $ \ctx -> selectWorkflow ctx []
  checkpoint <- fx.wfCheckStep wid
  -- The replay looks at a set that now has an answer, and still reads the
  -- refusal back. A fresh scope is the replay's start.
  second <- wfRun fx wid $ \ctx -> selectWorkflow ctx [wid]
  pure (first, checkpoint, second)

-- | A recorded winner that left the set is refused: the winner must still
-- be waited on.
scenarioWinnerLeft :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
                   => WaitFixture m -> m (WorkflowId, Either (Error EngineOnly) WorkflowId, Either (Error EngineOnly) WorkflowId)
scenarioWinnerLeft fx = do
  wid <- fx.wfFreshWorkflowId "winner"
  fx.wfSettle wid
  first <- wfRun fx wid $ \ctx -> selectWorkflow ctx [wid]
  second <- wfRun fx wid $ \ctx -> selectWorkflow ctx [WorkflowId "hs-wait-elsewhere"]
  pure (wid, first, second)

-- | An all-wait completes over a settled workflow.
scenarioJoinSettled :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
                    => WaitFixture m -> m (Either (Error EngineOnly) ())
scenarioJoinSettled fx = do
  wid <- fx.wfFreshWorkflowId "join"
  fx.wfSettle wid
  wfRun fx wid $ \ctx -> joinWorkflows ctx [wid]

-- | Select reports the first workflow to settle, and checkpoints the win.
scenarioSelectFirst :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
                    => WaitFixture m -> m (WorkflowId, WorkflowId, Either (Error EngineOnly) WorkflowId, Maybe StepRecord)
scenarioSelectFirst fx = do
  first <- fx.wfFreshWorkflowId "first-a"
  second <- fx.wfFreshWorkflowId "first-b"
  fx.wfSettle second
  won <- wfRun fx first $ \ctx -> selectWorkflow ctx [first, second]
  checkpoint <- fx.wfCheckStep first
  pure (first, second, won, checkpoint)

-- | The symmetric direction: a settled first id wins over a pending set.
scenarioSelectSettledFirst :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
                           => WaitFixture m -> m (WorkflowId, WorkflowId, Either (Error EngineOnly) WorkflowId, Maybe StepRecord)
scenarioSelectSettledFirst fx = do
  first <- fx.wfFreshWorkflowId "sfirst-a"
  second <- fx.wfFreshWorkflowId "sfirst-b"
  fx.wfSettle first
  won <- wfRun fx first $ \ctx -> selectWorkflow ctx [first, second]
  checkpoint <- fx.wfCheckStep first
  pure (first, second, won, checkpoint)

-- | A replayed first-wait reads its recorded winner back.
scenarioReplayWinner :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
                     => WaitFixture m -> m (WorkflowId, Either (Error EngineOnly) WorkflowId, Either (Error EngineOnly) WorkflowId)
scenarioReplayWinner fx = do
  first <- fx.wfFreshWorkflowId "replay-a"
  second <- fx.wfFreshWorkflowId "replay-b"
  fx.wfSettle second
  won <- wfRun fx first $ \ctx -> selectWorkflow ctx [first, second]
  replayed <- wfRun fx first $ \ctx -> selectWorkflow ctx [first, second]
  pure (second, won, replayed)

-- | A cancelled workflow counts as settled.
scenarioCancelled :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
                  => WaitFixture m -> m (WorkflowId, Either (Error EngineOnly) WorkflowId)
scenarioCancelled fx = do
  wid <- fx.wfFreshWorkflowId "cancelled"
  other <- fx.wfFreshWorkflowId "cancelled-other"
  fx.wfCancel other
  outcome <- wfRun fx wid $ \ctx -> selectWorkflow ctx [wid, other]
  pure (other, outcome)

-- | Join returns when the last workflow settles.
scenarioJoinLast :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
                 => WaitFixture m -> m (Either (Error EngineOnly) ())
scenarioJoinLast fx = do
  wid <- fx.wfFreshWorkflowId "joinlast"
  other <- fx.wfFreshWorkflowId "joinlast-other"
  fx.wfSettle wid
  fx.wfSettle other
  wfRun fx wid $ \ctx -> joinWorkflows ctx [wid, other]

-- | An empty all-wait is satisfied and takes no step.
scenarioEmptyAll :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
                 => WaitFixture m -> m (Either (Error EngineOnly) (), Maybe StepRecord)
scenarioEmptyAll fx = do
  wid <- fx.wfFreshWorkflowId "empty"
  outcome <- wfRun fx wid $ \ctx -> joinWorkflows ctx []
  checkpoint <- fx.wfCheckStep wid
  pure (outcome, checkpoint)

-- | A repeated id is accepted by both waits.
scenarioRepeated :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
                 => WaitFixture m -> m (WorkflowId, Either (Error EngineOnly) WorkflowId, Either (Error EngineOnly) ())
scenarioRepeated fx = do
  wid <- fx.wfFreshWorkflowId "repeated"
  fx.wfSettle wid
  first <- wfRun fx wid $ \ctx -> selectWorkflow ctx [wid, wid]
  outcome <- wfRun fx wid $ \ctx -> joinWorkflows ctx [wid, wid]
  pure (wid, first, outcome)

-- * Checks

-- | The empty wait refuses, records the refusal as a step error, and the
-- replay reads the refusal back.
checkRefusal :: (Either (Error EngineOnly) WorkflowId, Maybe StepRecord, Either (Error EngineOnly) WorkflowId) -> Either String ()
checkRefusal (first, checkpoint, second) = do
  case first of
    Left (InvalidArgument operation detail) -> do
      unless (operation == "select_workflow") $ Left ("expected the select_workflow operation, got: " <> show operation)
      unless (detail == "no workflow ids to wait for") $ Left ("expected the empty-set detail, got: " <> show detail)
    other -> Left ("expected the empty wait to be refused, got: " <> show other)
  case checkpoint of
    Just record ->
      unless (record.stepRecordError /= Nothing) $ Left "expected the refusal to be recorded as a step error"
    Nothing -> Left "expected the refusal to be recorded"
  case second of
    Left (InvalidArgument operation _) ->
      unless (operation == "select_workflow") $ Left ("expected the replay to read the refusal, got: " <> show operation)
    other -> Left ("expected the replay to read the refusal, got: " <> show other)

-- | The settled winner returns, and the replay over a changed set names
-- the recorded winner in its refusal.
checkWinnerLeft :: (WorkflowId, Either (Error EngineOnly) WorkflowId, Either (Error EngineOnly) WorkflowId) -> Either String ()
checkWinnerLeft (wid, first, second) = do
  unless (first == Right wid) $ Left ("expected the settled winner, got: " <> show first)
  case second of
    Left (SystemDatabase err) -> case err of
      SysDB.UnexpectedStep {workflowId, stepId, recorded} -> do
        unless (workflowId == widTextOf wid) $ Left ("expected the waiting workflow to be named, got: " <> show workflowId)
        unless (stepId == 0) $ Left ("expected step 0, got: " <> show stepId)
        unless (widTextOf wid `Text.isInfixOf` recorded) $ Left "expected the recorded winner to be named"
      otherErr -> Left ("expected UnexpectedStep, got: " <> show otherErr)
    other -> Left ("expected the changed set to be refused, got: " <> show other)

-- | The settled all-wait completes.
checkJoinSettled :: Either (Error EngineOnly) () -> Either String ()
checkJoinSettled outcome =
  unless (outcome == Right ()) $ Left ("expected the settled join to complete, got: " <> show outcome)

-- | The second id wins and the win is checkpointed with an output.
checkSelectFirst :: (WorkflowId, WorkflowId, Either (Error EngineOnly) WorkflowId, Maybe StepRecord) -> Either String ()
checkSelectFirst (_first, second, won, checkpoint) = do
  unless (won == Right second) $ Left ("expected the second id to win, got: " <> show won)
  case checkpoint of
    Just record ->
      unless (record.stepRecordOutput /= Nothing) $ Left "expected the winning select to checkpoint its output"
    Nothing -> Left "expected the winning select to be recorded"

-- | The settled first id wins and the win is checkpointed with an output.
checkSelectSettledFirst :: (WorkflowId, WorkflowId, Either (Error EngineOnly) WorkflowId, Maybe StepRecord) -> Either String ()
checkSelectSettledFirst (first, _second, won, checkpoint) = do
  unless (won == Right first) $ Left ("expected the first id to win, got: " <> show won)
  case checkpoint of
    Just record ->
      unless (record.stepRecordOutput /= Nothing) $ Left "expected the winning select to checkpoint its output"
    Nothing -> Left "expected the winning select to be recorded"

-- | The replay reads the recorded winner back.
checkReplayWinner :: (WorkflowId, Either (Error EngineOnly) WorkflowId, Either (Error EngineOnly) WorkflowId) -> Either String ()
checkReplayWinner (second, won, replayed) = do
  unless (won == Right second) $ Left ("expected the winner, got: " <> show won)
  unless (replayed == Right second) $ Left ("expected the replay to read the winner back, got: " <> show replayed)

-- | The cancelled workflow counts as settled.
checkCancelled :: (WorkflowId, Either (Error EngineOnly) WorkflowId) -> Either String ()
checkCancelled (other, outcome) =
  unless (outcome == Right other) $ Left ("expected the cancelled workflow to count as settled, got: " <> show outcome)

-- | Join returns when the last workflow settles.
checkJoinLast :: Either (Error EngineOnly) () -> Either String ()
checkJoinLast outcome =
  unless (outcome == Right ()) $ Left ("expected the join to complete, got: " <> show outcome)

-- | The empty all-wait is satisfied and takes no step.
checkEmptyAll :: (Either (Error EngineOnly) (), Maybe StepRecord) -> Either String ()
checkEmptyAll (outcome, checkpoint) = do
  unless (outcome == Right ()) $ Left ("expected the empty join to complete, got: " <> show outcome)
  unless (checkpoint == Nothing) $ Left "expected the empty join to take no step"

-- | A repeated id is accepted by both waits.
checkRepeated :: (WorkflowId, Either (Error EngineOnly) WorkflowId, Either (Error EngineOnly) ()) -> Either String ()
checkRepeated (wid, first, outcome) = do
  unless (first == Right wid) $ Left ("expected the repeated id to win, got: " <> show first)
  unless (outcome == Right ()) $ Left ("expected the repeated join to complete, got: " <> show outcome)
