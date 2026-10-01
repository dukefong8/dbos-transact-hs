{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Durable sleep, one-to-one with Rust @sleep.rs@: record the wake time,
-- wait until it, and on replay wait only what is left of the original wait.
-- The recorded wake time is the step output and @completed_at@ is stamped at
-- the wake time, so an hour's sleep reads as an hour rather than an instant.
module DBOS.Transact.Sleep (sleepWorkflowStep, pendingSleep, sleepPlain, SleepEvent (..)) where

import DBOS.Prelude
import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Control.Monad.Class.MonadTime (MonadTime)
import Control.Monad.Class.MonadTimer (MonadDelay, threadDelay)
import Data.Int (Int64)
import System.Log.FastLogger (ToLogStr (..))
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Types (Duration, WorkflowId (..), durationAsMillis, sleepStepName, timestampNow, timestampToEpochMs)
import DBOS.Tracer (LogEvent (..), LogSeverity (..), runTracer, showSeverity)
import DBOS.Transact.Context (Ctx, contextTracer, nextStepId, stepId, withSystemDB, workflowId)
import DBOS.Transact.Checkpoint (PendingStep (..), StepDurability (..), StepPlacement (..), checkHere, placeCall)
import DBOS.Transact.Error qualified as TransactError

-- | Announcements from the sleep paths, homed here with their owner.
-- Mirrors the @sleep.rs@ debug sites: an uncheckpointed sleep inside a
-- step waits plainly, and every checkpointed sleep waits until its
-- recorded wake time. The free 'sleepPlain' takes no tracer and stays
-- quiet; its only in-engine caller is the in-step branch below.
data SleepEvent
  = SleepUncheckpointed { sleepDurationMs :: Integer }
  | SleepUntilWake { sleepStepId :: Int, sleepRemainingMs :: Int64 }
  deriving stock (Eq, Show)

instance LogEvent SleepEvent where
  eventSeverity SleepUncheckpointed {} = SeverityDebug
  eventSeverity SleepUntilWake {} = SeverityDebug
  renderEvent (SleepUncheckpointed durationMs) =
    "the sleep is not checkpointed: it is outside a workflow, or inside a step duration_ms=" <> showText durationMs
  renderEvent (SleepUntilWake stepId' remainingMs) =
    "sleeping until the recorded wake time step_id=" <> showText stepId' <> " remaining_ms=" <> showText remainingMs

instance ToLogStr SleepEvent where
  toLogStr event = toLogStr (showSeverity (eventSeverity event) <> " " <> renderEvent event)

sleepWorkflowStep :: (MonadSTM m, MonadTime m, MonadDelay m) => Ctx m -> Duration -> m (Either (TransactError.Error TransactError.EngineOnly) ())
sleepWorkflowStep ctx duration = placeCall ctx >>= driveSleep ctx duration

-- | A sleep built at its position and not yet run: the id is claimed at
-- the call so a replay rebuilds the same slot, and the wait runs when the
-- pending value is awaited or raced.
pendingSleep :: (MonadSTM m, MonadTime m, MonadDelay m) => Ctx m -> Duration -> m (PendingStep m (Either (TransactError.Error TransactError.EngineOnly) ()))
pendingSleep ctx duration = do
  placement <- placeCall ctx
  pure (PendingStep sleepStepName (Just placement) (driveSleep ctx duration placement))

-- | Drives a placed sleep: a plain wait inside a step or outside a
-- workflow, otherwise the recorded wake-time wait under the claimed id.
driveSleep :: (MonadSTM m, MonadTime m, MonadDelay m) => Ctx m -> Duration -> StepPlacement m -> m (Either (TransactError.Error TransactError.EngineOnly) ())
driveSleep ctx duration placement =
  case checkHere placement sleepStepName (Just ctx) of
    Left err -> pure (Left err)
    Right DurabilityPlain -> do
      -- Inside a step the sleep is plain: the enclosing step's checkpoint
      -- stands for everything its body did, and taking an id here would shift
      -- every step after it on replay. Mirrors Rust's @InsideStep@ placement.
      runTracer (contextTracer ctx) (SleepUncheckpointed (durationAsMillis duration))
      sleepPlain duration >> pure (Right ())
    Right (DurabilityRecorded ctx' stepId') -> do
      let workflowId' = WorkflowId (workflowId ctx')
      recordedWake <- withSystemDB ctx' (\db -> SystemDB.recordSleep db workflowId' stepId' duration)
      case recordedWake of
        Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
        Right wakeAt -> do
          now <- timestampNow
          let remainingMillis = max 0 (timestampToEpochMs wakeAt - timestampToEpochMs now)
          runTracer (contextTracer ctx') (SleepUntilWake stepId' remainingMillis)
          threadDelay (millisToMicros remainingMillis)
          pure (Right ())

-- | A plain wait, mirroring the free @sleep@ outside a workflow or inside
-- a step: no checkpoint, no id, just the delay.
sleepPlain :: MonadDelay m => Duration -> m ()
sleepPlain duration = threadDelay (fromInteger (durationAsMillis duration) * 1000)

millisToMicros :: Int64 -> Int
millisToMicros milliseconds = fromIntegral (milliseconds * 1000)
