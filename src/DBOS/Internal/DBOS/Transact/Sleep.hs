{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | Durable sleep, one-to-one with Rust @sleep.rs@: record the wake time,
-- wait until it, and on replay wait only what is left of the original wait.
-- The recorded wake time is the step output and @completed_at@ is stamped at
-- the wake time, so an hour's sleep reads as an hour rather than an instant.
module DBOS.Transact.Sleep (sleepStep, pendingSleep, sleepPlain, SleepEvent (..)) where

import Data.Int (Int64)
import DBOS.Prelude
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Types
  ( Duration,
    WorkflowId (..),
    durationAsMillis,
    durationToMs,
    sleepStepName,
    wakeArrives
  )
import DBOS.Transact.Checkpoint
  ( PendingStep (..),
    StepDurability (..),
    StepPlacement (..),
    checkHere,
    placeCall
  )
import DBOS.Transact.Context
  ( WorkflowCtx (wctxTracer),
    ownerToken,
    stepCtxBoundary,
    withSystemDB,
    workflowId
  )
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Logger (LogEvent (..), LogSeverity (..), runTracer)
import System.Log.FastLogger (ToLogStr (..))

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
  eventSeverity SleepUntilWake {}      = SeverityDebug
  renderEvent (SleepUncheckpointed durationMs) =
    "the sleep is not checkpointed: it is outside a workflow, or inside a step duration_ms=" <> showText durationMs
  renderEvent (SleepUntilWake stepId' remainingMs) =
    "sleeping until the recorded wake time step_id=" <> showText stepId' <> " remaining_ms=" <> showText remainingMs

instance ToLogStr SleepEvent where
  toLogStr = toLogStr . renderLine

sleepStep :: (MonadSTM m, MonadDelay m, MonadMonotonicTimeNSec m)
          => WorkflowCtx exec m -> Duration -> m (Either (TransactError.Error TransactError.EngineOnly) ())
sleepStep wctx duration =
  -- The duration is checked first, so a refusal claims no id — and only
  -- against a rule that reads no clock, so the first run and a replay
  -- decide it the same way. Mirrors Rust @sleep@'s pre-check.
  TransactError.refusing (TransactError.refusedArgument "sleep") (durationToMs "duration" duration) $ \_ ->
    placeCall wctx >>= driveSleep wctx duration

-- | A sleep built at its position and not yet run: the id is claimed at
-- the call so a replay rebuilds the same slot, and the wait runs when the
-- pending value is awaited or raced. The pre-check runs first for the
-- same reason as 'sleepStep': a refused sleep moves no counter.
pendingSleep :: (MonadSTM m, MonadDelay m, MonadMonotonicTimeNSec m)
             => WorkflowCtx exec m -> Duration -> m (Either (TransactError.Error TransactError.EngineOnly) (PendingStep exec m (Either (TransactError.Error TransactError.EngineOnly) ())))
pendingSleep wctx duration =
  TransactError.refusing (TransactError.refusedArgument "sleep") (durationToMs "duration" duration) $ \_ -> do
    placement <- placeCall wctx
    pure (Right (PendingStep sleepStepName (Just placement) (driveSleep wctx duration placement)))

-- | Drives a placed sleep: a plain wait inside a step or outside a
-- workflow, otherwise the recorded wake-time wait under the claimed id.
driveSleep :: (MonadSTM m, MonadDelay m, MonadMonotonicTimeNSec m)
           => WorkflowCtx exec m -> Duration -> StepPlacement exec m -> m (Either (TransactError.Error TransactError.EngineOnly) ())
driveSleep wctx duration placement =
  case checkHere placement sleepStepName (Just (stepCtxBoundary wctx)) of
    Left err -> pure (Left err)
    Right DurabilityPlain -> do
      -- Inside a step the sleep is plain: the enclosing step's checkpoint
      -- stands for everything its body did, and taking an id here would shift
      -- every step after it on replay. Mirrors Rust's @InsideStep@ placement.
      runTracer wctx.wctxTracer (SleepUncheckpointed (durationAsMillis duration))
      sleepPlain duration
    Right (DurabilityRecorded wctx' stepId') -> do
      let workflowId' = WorkflowId (workflowId wctx')
      recordedWake <- withSystemDB wctx' (\db -> SystemDB.recordSleep db workflowId' stepId' duration (ownerToken wctx'))
      TransactError.refusing (TransactError.refusedArgument "sleep") recordedWake $ \wake -> do
        -- A recorded refusal surfaces on the same channel as a fresh
        -- one: 'recordSleep' hands back the stored refusal, whatever
        -- run recorded it.
        -- The wait is measured on the monotonic clock from the database's
        -- reading, never against this host's wall clock, which need not
        -- agree with the clock the wake time was set on; a replay that
        -- woke long ago waits not at all. Mirrors the oracle's sleep wait.
          now <- getMonotonicTimeNSec
          let arrives = wakeArrives wake
              remainingMillis = fromIntegral ((if arrives > now then arrives - now else 0) `div` 1000000) :: Int64
          runTracer wctx'.wctxTracer (SleepUntilWake stepId' remainingMillis)
          threadDelay (millisToMicros remainingMillis)
          pure (Right ())

-- | A plain wait, mirroring the free @sleep@ outside a workflow or inside
-- a step: no checkpoint, no id, just the delay. Refused like every other
-- sleep when the duration cannot be recorded, so the same call does not
-- succeed in one place and fail in another.
sleepPlain :: MonadDelay m => Duration -> m (Either (TransactError.Error TransactError.EngineOnly) ())
sleepPlain duration =
  TransactError.refusing (TransactError.refusedArgument "sleep") (durationToMs "duration" duration) $ \_ ->
    threadDelay (fromInteger (durationAsMillis duration) * 1000) >> pure (Right ())

millisToMicros :: Int64 -> Int
millisToMicros milliseconds = fromIntegral (milliseconds * 1000)
