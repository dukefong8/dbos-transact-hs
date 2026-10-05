{-# LANGUAGE OverloadedRecordDot #-}

-- | Shared sleep scenarios: one body per case, judged by one pure check on
-- each stack. The fixture carries every stack-specific operation (workflow
-- creation, running a sleep, reading its checkpoint, the in-step variant),
-- so the scenarios below are plain compositions with no effect constraints
-- of their own — the live tree runs them over Postgres, the sim tree over
-- the in-memory backend, and both prove the same record.
module DBOS.Transact.SleepCases
  ( SleepFixture (..),
    SleepCheckpointObservation (..),
    SleepReplayObservation (..),
    scenarioSleepCheckpoint,
    scenarioSleepReplay,
    scenarioSleepPlain,
    scenarioSleepInStep,
    checkSleepCheckpoint,
    checkSleepReplay,
    checkSleepPlain,
    checkSleepInStep,
  )
where

import DBOS.Prelude
import DBOS.SystemDB (Duration, StepRecord (..), WorkflowId, millisDuration, sleepStepName)
import DBOS.Transact (EngineOnly, Error)

-- | What a stack must provide to run the shared sleep scenarios: fresh
-- workflow ids, one sleep run, the sleep's checkpoint row, the free sleep,
-- and the sleep-inside-a-step variant (which takes no id).
data SleepFixture m = SleepFixture
  { sfFreshWorkflowId :: m WorkflowId,
    sfRunSleep :: WorkflowId -> Duration -> m (Either (Error EngineOnly) ()),
    sfCheckSleep :: WorkflowId -> m (Maybe StepRecord),
    sfPlainSleep :: Duration -> m (),
    sfRunInStep :: WorkflowId -> Duration -> m (Either (Error EngineOnly) (), Int, Int)
  }

-- | The outcome of one sleep plus the checkpoint row it left behind.
data SleepCheckpointObservation = SleepCheckpointObservation
  { scoOutcome :: Either (Error EngineOnly) (),
    scoCheckpoint :: Maybe StepRecord
  }

-- | Both runs of the replay case plus the checkpoint row before and after
-- the replay.
data SleepReplayObservation = SleepReplayObservation
  { sroFirst :: Either (Error EngineOnly) (),
    sroSecond :: Either (Error EngineOnly) (),
    sroBefore :: Maybe StepRecord,
    sroAfter :: Maybe StepRecord
  }

-- | One sleep: it waits, succeeds, and leaves a checkpoint carrying a wake
-- time.
scenarioSleepCheckpoint :: Monad m => SleepFixture m -> m SleepCheckpointObservation
scenarioSleepCheckpoint wf = do
  wid <- wf.sfFreshWorkflowId
  outcome <- wf.sfRunSleep wid (millisDuration 25)
  checkpoint <- wf.sfCheckSleep wid
  pure (SleepCheckpointObservation outcome checkpoint)

-- | A sleep, then the same step asked again with a much longer duration: the
-- replay adopts the recorded wake instead of starting its clock again.
scenarioSleepReplay :: Monad m => SleepFixture m -> m SleepReplayObservation
scenarioSleepReplay wf = do
  wid <- wf.sfFreshWorkflowId
  first <- wf.sfRunSleep wid (millisDuration 25)
  before <- wf.sfCheckSleep wid
  second <- wf.sfRunSleep wid (millisDuration 60000)
  after <- wf.sfCheckSleep wid
  pure (SleepReplayObservation first second before after)

-- | The free sleep outside a workflow waits plainly: no checkpoint, no id.
scenarioSleepPlain :: SleepFixture m -> m ()
scenarioSleepPlain wf = wf.sfPlainSleep (millisDuration 1)

-- | A sleep inside a step waits without consuming a step id.
scenarioSleepInStep :: Monad m => SleepFixture m -> m (Either (Error EngineOnly) (), Int, Int)
scenarioSleepInStep wf = do
  wid <- wf.sfFreshWorkflowId
  wf.sfRunInStep wid (millisDuration 5)

-- | The sleep succeeds and records its step with a wake time.
checkSleepCheckpoint :: SleepCheckpointObservation -> Either String ()
checkSleepCheckpoint obs = do
  case obs.scoOutcome of
    Left err -> Left ("the sleep must succeed, got: " <> show err)
    Right () -> pure ()
  case obs.scoCheckpoint of
    Nothing -> Left "expected a recorded sleep checkpoint"
    Just record
      | record.stepRecordStepName /= sleepStepName ->
          Left ("expected the sleep step, got: " <> show record.stepRecordStepName)
      | record.stepRecordOutput == Nothing ->
          Left "the sleep must record a wake time"
      | otherwise -> Right ()

-- | Both runs succeed and the replay keeps the recorded wake time and
-- output byte-for-byte: exactly one wait happened.
checkSleepReplay :: SleepReplayObservation -> Either String ()
checkSleepReplay obs = do
  case (obs.sroFirst, obs.sroSecond) of
    (Right (), Right ()) -> pure ()
    _ -> Left "both the sleep and its replay must succeed"
  case (obs.sroBefore, obs.sroAfter) of
    (Just before, Just after) -> do
      unless (after.stepRecordCompletedAt == before.stepRecordCompletedAt) $
        Left "the replay must keep the recorded wake time"
      unless (after.stepRecordOutput == before.stepRecordOutput) $
        Left "the replay must keep the recorded output"
    _ -> Left "expected the recorded sleep before and after the replay"

-- | The free sleep returns.
checkSleepPlain :: () -> Either String ()
checkSleepPlain () = Right ()

-- | The in-step sleep succeeds and the step counter does not move.
checkSleepInStep :: (Either (Error EngineOnly) (), Int, Int) -> Either String ()
checkSleepInStep (slept, before, after) = do
  case slept of
    Left err -> Left ("the in-step sleep must succeed, got: " <> show err)
    Right () -> pure ()
  unless (after == before + 1) $
    Left "the in-step sleep must take no id"
