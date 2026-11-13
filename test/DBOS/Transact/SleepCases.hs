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
    SleepRefusedObservation (..),
    SleepRefusedRecordObservation (..),
    scenarioSleepCheckpoint,
    scenarioSleepReplay,
    scenarioSleepPlain,
    scenarioSleepTimed,
    scenarioSleepInStep,
    scenarioSleepRefused,
    scenarioSleepRefusedRecorded,
    checkSleepCheckpoint,
    checkSleepReplay,
    checkSleepPlain,
    checkSleepTimed,
    checkSleepInStep,
    checkSleepRefused,
    checkSleepRefusedRecorded,
  )
where

import DBOS.Internal.Prelude
import Data.Word (Word64)
import Data.Text qualified as Text
import DBOS.SystemDB (Duration, StepRecord (..), WorkflowId, durationFromMs, millisDuration, secondsDuration, sleepStepName)
import DBOS.Transact (EngineOnly, Error (..))

-- | What a stack must provide to run the shared sleep scenarios: fresh
-- workflow ids, one sleep run, the sleep's checkpoint row, the free sleep,
-- the sleep-inside-a-step variant (which takes no id), and a refused sleep
-- with the id the next call would take (which must still be fresh).
data SleepFixture m = SleepFixture
  { sfFreshWorkflowId :: m WorkflowId,
    sfRunSleep :: WorkflowId -> Duration -> m (Either (Error EngineOnly) ()),
    sfCheckSleep :: WorkflowId -> m (Maybe StepRecord),
    sfPlainSleep :: Duration -> m (Either (Error EngineOnly) ()),
    sfRunInStep :: WorkflowId -> Duration -> m (Either (Error EngineOnly) (), Int, Int),
    sfRefusedSleep :: WorkflowId -> Duration -> m (Either (Error EngineOnly) (), Int)
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
scenarioSleepPlain :: SleepFixture m -> m (Either (Error EngineOnly) ())
scenarioSleepPlain wf = wf.sfPlainSleep (millisDuration 1)

-- | A sleep waits its duration on the monotonic clock from the database's
-- reading, not its wall-clock distance from the recorded instant: on a
-- database an hour off this process, a wall-clock wait would sleep an hour
-- too long, or not at all. Returns the outcome with the nanoseconds the
-- wait took on this process's monotonic clock. Mirrors the oracle's
-- skewed-clock sleep waits.
scenarioSleepTimed :: (MonadMonotonicTimeNSec m) => SleepFixture m -> m (Either (Error EngineOnly) (), Word64)
scenarioSleepTimed wf = do
  wid <- wf.sfFreshWorkflowId
  before <- getMonotonicTimeNSec
  outcome <- wf.sfRunSleep wid (secondsDuration 3)
  after <- getMonotonicTimeNSec
  pure (outcome, after - before)

-- | The sleep succeeds and takes about its duration — seconds, never
-- minutes — whatever the database's clock says.
checkSleepTimed :: (Either (Error EngineOnly) (), Word64) -> Either String ()
checkSleepTimed (outcome, elapsedNs) = do
  case outcome of
    Left err -> Left ("the sleep must succeed, got: " <> show err)
    Right () -> pure ()
  unless (elapsedNs >= 2000000000 && elapsedNs <= 60000000000) $
    Left ("the sleep must take about its duration, took (ns): " <> show elapsedNs)

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
checkSleepPlain :: Either (Error EngineOnly) () -> Either String ()
checkSleepPlain (Right ()) = Right ()
checkSleepPlain (Left err) = Left ("the plain sleep must succeed, got: " <> show err)

-- | The in-step sleep succeeds and the step counter does not move.
checkSleepInStep :: (Either (Error EngineOnly) (), Int, Int) -> Either String ()
checkSleepInStep (slept, before, after) = do
  case slept of
    Left err -> Left ("the in-step sleep must succeed, got: " <> show err)
    Right () -> pure ()
  unless (after == before + 1) $
    Left "the in-step sleep must take no id"

-- | A refused sleep and the id the next call would still take, plus the
-- checkpoint row, which must be absent: the pre-check runs before the
-- step id is taken.
data SleepRefusedObservation = SleepRefusedObservation
  { sroRefusedOutcome :: Either (Error EngineOnly) (),
    sroRefusedNextId :: Int,
    sroRefusedCheckpoint :: Maybe StepRecord
  }

-- | A sleep that cannot be recorded is refused at the call, before it
-- takes a step id: the next id is still fresh and nothing is recorded.
scenarioSleepRefused :: Monad m => SleepFixture m -> m SleepRefusedObservation
scenarioSleepRefused wf = do
  wid <- wf.sfFreshWorkflowId
  -- One millisecond past what the column holds: too long to record, so
  -- the pre-check refuses it.
  (refused, next) <- wf.sfRefusedSleep wid (millisDuration 9223372036854775808)
  checkpoint <- wf.sfCheckSleep wid
  pure (SleepRefusedObservation refused next checkpoint)

-- | The refusal arrives on the sleep channel, the next id is untouched,
-- and no step was recorded for the sleep.
checkSleepRefused :: SleepRefusedObservation -> Either String ()
checkSleepRefused obs = do
  case obs.sroRefusedOutcome of
    Left (InvalidArgument operation detail) -> do
      unless (operation == Text.pack "sleep") $ Left ("expected the sleep operation, got: " <> show operation)
      unless (detail == Text.pack "`duration` must fit in milliseconds as a 64-bit integer") $
        Left ("expected the duration refusal, got: " <> show detail)
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  unless (obs.sroRefusedNextId == 0) $
    Left ("a refused sleep must spend no step id, got: " <> show obs.sroRefusedNextId)
  case obs.sroRefusedCheckpoint of
    Nothing -> Right ()
    Just record -> Left ("a refused sleep must record nothing, got: " <> show record)

-- | A refused sleep's record and the replay that must adopt it: the first
-- run records the refusal as the step error, and a second run with a
-- short duration gets the same refusal back instead of deciding again.
data SleepRefusedRecordObservation = SleepRefusedRecordObservation
  { srroFirst :: Either (Error EngineOnly) (),
    srroBefore :: Maybe StepRecord,
    srroSecond :: Either (Error EngineOnly) (),
    srroAfter :: Maybe StepRecord
  }

-- | A sleep whose wake time fits in milliseconds but carries the clock
-- past the largest storable instant: recorded as the step's error, and
-- replayed as the same refusal.
scenarioSleepRefusedRecorded :: Monad m => SleepFixture m -> m SleepRefusedRecordObservation
scenarioSleepRefusedRecorded wf = do
  wid <- wf.sfFreshWorkflowId
  -- The plain sleep moves the clock past zero, so the maximum
  -- millisecond count overflows the sum on every clock.
  _ <- wf.sfPlainSleep (millisDuration 25) >>= either (error . show) pure
  pastMax <- case durationFromMs 9223372036854775807 of
    Nothing -> error "the maximum millisecond count must build"
    Just duration -> pure duration
  first <- wf.sfRunSleep wid pastMax
  before <- wf.sfCheckSleep wid
  second <- wf.sfRunSleep wid (millisDuration 25)
  after <- wf.sfCheckSleep wid
  pure (SleepRefusedRecordObservation first before second after)

-- | Both runs refuse identically, the record carries the refusal as its
-- error, and the replay leaves the row alone.
checkSleepRefusedRecorded :: SleepRefusedRecordObservation -> Either String ()
checkSleepRefusedRecorded obs = do
  let refused = \run -> case run of
        Left (InvalidArgument operation detail) -> do
          unless (operation == Text.pack "sleep") $ Left ("expected the sleep operation, got: " <> show operation)
          unless (Text.pack "must not end past" `Text.isInfixOf` detail) $
            Left ("expected the wake-time refusal, got: " <> show detail)
        other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  refused obs.srroFirst
  refused obs.srroSecond
  case obs.srroBefore of
    Just record -> case record.stepRecordError of
      Just _ -> pure ()
      Nothing -> Left "the refusal must be recorded as the step error"
    Nothing -> Left "expected the refusal recorded"
  unless (obs.srroAfter == obs.srroBefore) $
    Left "the replay must leave the recorded refusal alone"
