{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Shared durable-select scenarios: one body per case, judged by one pure
-- check on each stack. The check/record pair is pure engine logic over a
-- workflow context; scenarios reduce every comparison to stack-free facts
-- (booleans, step rows, winner ids, error literals) inside the scenario.
-- The live tree ('DBOS.Transact.SelectTest') runs them over Postgres
-- rows, the sim tree ('DBOS.Transact.SelectTestSim') over the in-memory
-- backend. Branch bodies never run in these cases — only identities
-- matter — so they are total values on both stacks.
module DBOS.Transact.SelectCases
  ( SelectFixture (..),
    scenarioFreshWinner,
    scenarioStaleWinner,
    scenarioControlError,
    checkFreshWinner,
    checkStaleWinner,
    checkControlError,
  )
where

import DBOS.Prelude
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.SystemDB (StepRecord (..), WorkflowId (..), selectStepStepName)
import DBOS.SystemDB qualified as SysDB
import DBOS.Transact
  ( Branches,
    EngineOnly,
    Error (..),
    PendingStep (..),
    Racing (..),
    WorkflowCtx,
  )
import DBOS.Transact.Select (checkSelect, controlError, newBranches, pushBranch, recordSelect)

-- | What a stack must provide: run one check/record pass over a fresh
-- workflow row, and read the select step rows back.
data SelectFixture m = SelectFixture
  { scfRun :: forall a. (forall exec. WorkflowCtx exec m -> m a) -> m a,
    scfListSteps :: m [StepRecord]
  }

-- | Run one check/record pass over the fixture's workflow row: the rank-2
-- field is read by pattern match because record-dot has no 'HasField'
-- instance for polymorphic fields.
scfWith :: SelectFixture m -> (forall exec. WorkflowCtx exec m -> m a) -> m a
scfWith (SelectFixture run _) = run

-- | Two named branches that claim no ids: enough for the stale-winner
-- check, which reads only the identities. The bodies never run, so they
-- are total values, identical on both stacks.
branches2 :: Branches
branches2 =
  pushBranch
    (PendingStep "charge" Nothing (pure (Right ()) :: IO (Either (Error EngineOnly) ())))
    (pushBranch (PendingStep "timeout" Nothing (pure (Right ()) :: IO (Either (Error EngineOnly) ()))) newBranches)

-- | A fresh select claims its id and records a winner: the row carries
-- step 0 under the select name with the winner as output, and a second
-- pass replays the winner.
scenarioFreshWinner :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => SelectFixture m -> m (Bool, Int, Int, Text, Maybe Text, Int)
scenarioFreshWinner fx = do
  isFresh <- scfWith fx $ \ctx -> do
    checked <- checkSelect ctx branches2
    case checked of
      Right (Fresh recording) -> do
        recorded <- recordSelect recording 1
        case recorded of
          Left _ -> pure False
          Right () -> pure True
      _ -> pure False
  rows <- fx.scfListSteps
  winner <- scfWith fx $ \ctx -> do
    replayed <- checkSelect ctx branches2
    case replayed of
      Right (Replay won _) -> pure won
      _ -> pure (-1)
  case rows of
    [StepRecord {stepRecordStepId = stepId, stepRecordStepName = name, stepRecordOutput = output}] ->
      pure (isFresh, 1, stepId, name, output, winner)
    _ -> pure (isFresh, length rows, -1, "", Nothing, winner)

-- | A winner outside the branches that exist now is refused, naming this
-- run's shape and the stale winner.
scenarioStaleWinner :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => SelectFixture m -> m (Bool, Maybe (Int, Text, Text))
scenarioStaleWinner fx = do
  isFresh <- scfWith fx $ \ctx -> do
    checked <- checkSelect ctx branches2
    case checked of
      Right (Fresh recording) -> do
        recorded <- recordSelect recording 5
        case recorded of
          Left _ -> pure False
          Right () -> pure True
      _ -> pure False
  refused <- scfWith fx $ \ctx -> do
    replayed <- checkSelect ctx branches2
    case replayed of
      Left (ErrorSystemDatabase (SysDB.UnexpectedStep {stepId, expected, recorded = recordedText})) ->
        pure (Just (stepId, expected, recordedText))
      _ -> pure Nothing
  pure (isFresh, refused)

-- | A control signal is not a race decision: cancellations and
-- interruptions read as control, failures and values do not. Pure: no
-- backend, no fixture.
scenarioControlError :: (Maybe (Error EngineOnly), Bool, Maybe (Error EngineOnly), Maybe (Error EngineOnly), Maybe (Error EngineOnly))
scenarioControlError =
  ( controlError (Left (Interrupted {workflowId = "wf"})),
    case controlError (Left (ErrorSystemDatabase (SysDB.WorkflowCancelled {workflowId = "wf"}))) of
      Just _ -> True
      Nothing -> False,
    controlError (Left (StepFailed "s" "boom")),
    controlError (Left (AwaitedWorkflowCancelled {workflowId = "child"})),
    controlError (Right (7 :: Int))
  )

-- * Checks

-- | The fresh select records step 0 under the select name with the winner
-- as output, and the replay reads the winner back.
checkFreshWinner :: (Bool, Int, Int, Text, Maybe Text, Int) -> Either String ()
checkFreshWinner (isFresh, rowCount, stepId, name, output, winner) = do
  unless isFresh $ Left "expected a fresh select"
  unless (rowCount == 1) $ Left ("expected the select's own row, got: " <> show rowCount)
  unless (stepId == 0) $ Left ("expected step 0, got: " <> show stepId)
  unless (name == selectStepStepName) $ Left ("expected the select step name, got: " <> show name)
  unless (output == Just "1") $ Left ("expected the winner as output, got: " <> show output)
  unless (winner == 1) $ Left ("expected the recorded winner to replay, got: " <> show winner)

-- | The stale winner is refused naming the shape and the stale branch.
checkStaleWinner :: (Bool, Maybe (Int, Text, Text)) -> Either String ()
checkStaleWinner (isFresh, refused) = do
  unless isFresh $ Left "expected a fresh select"
  case refused of
    Just (stepId, expected, recordedText) -> do
      unless (stepId == 0) $ Left ("expected step 0, got: " <> show stepId)
      unless ("a select over 2 branches" `Text.isInfixOf` expected) $
        Left ("expected this run's shape, got: " <> show expected)
      unless ("branch 5" `Text.isInfixOf` recordedText) $
        Left ("expected the stale winner, got: " <> show recordedText)
    Nothing -> Left "expected the stale-winner refusal"

-- | Control signals read as control; failures, cancellations-awaited, and
-- values do not.
checkControlError :: (Maybe (Error EngineOnly), Bool, Maybe (Error EngineOnly), Maybe (Error EngineOnly), Maybe (Error EngineOnly)) -> Either String ()
checkControlError (interrupted, cancelled, failed, awaited, value) = do
  unless (interrupted == Just (Interrupted {workflowId = "wf"})) $
    Left ("expected the interruption to be control, got: " <> show interrupted)
  unless cancelled $ Left "expected a cancellation to be control"
  unless (failed == Nothing) $ Left ("expected a failure not to be control, got: " <> show failed)
  unless (awaited == Nothing) $ Left ("expected an awaited cancellation not to be control, got: " <> show awaited)
  unless (value == Nothing) $ Left ("expected a value not to be control, got: " <> show value)
