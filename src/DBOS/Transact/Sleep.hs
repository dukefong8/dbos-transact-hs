{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Durable sleep, one-to-one with Rust @sleep.rs@: record the wake time,
-- wait until it, and on replay wait only what is left of the original wait.
-- The recorded wake time is the step output and @completed_at@ is stamped at
-- the wake time, so an hour's sleep reads as an hour rather than an instant.
module DBOS.Transact.Sleep (sleepWorkflowStep, sleepPlain) where

import DBOS.Prelude
import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Control.Monad.Class.MonadTime (MonadTime)
import Control.Monad.Class.MonadTimer (MonadDelay, threadDelay)
import Data.Int (Int64)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Types (Duration, WorkflowId (..), durationAsMillis, timestampNow, timestampToEpochMs)
import DBOS.Transact.Context (Ctx, nextStepId, stepId, withSystemDB, workflowId)
import DBOS.Transact.Error qualified as TransactError

sleepWorkflowStep :: (MonadSTM m, MonadTime m, MonadDelay m) => Ctx m -> Duration -> m (Either TransactError.Error ())
sleepWorkflowStep ctx duration =
  case stepId ctx of
    -- Inside a step the sleep is plain: the enclosing step's checkpoint
    -- stands for everything its body did, and taking an id here would shift
    -- every step after it on replay. Mirrors Rust's @InsideStep@ placement.
    Just _ -> sleepPlain duration >> pure (Right ())
    Nothing -> do
      let workflowId' = WorkflowId (workflowId ctx)
      stepId <- nextStepId ctx
      recordedWake <- withSystemDB ctx (\db -> SystemDB.recordSleep db workflowId' stepId duration)
      case recordedWake of
        Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
        Right wakeAt -> do
          now <- timestampNow
          let remainingMillis = max 0 (timestampToEpochMs wakeAt - timestampToEpochMs now)
          threadDelay (millisToMicros remainingMillis)
          pure (Right ())

-- | A plain wait, mirroring the free @sleep@ outside a workflow or inside
-- a step: no checkpoint, no id, just the delay.
sleepPlain :: MonadDelay m => Duration -> m ()
sleepPlain duration = threadDelay (fromInteger (durationAsMillis duration) * 1000)

millisToMicros :: Int64 -> Int
millisToMicros milliseconds = fromIntegral (milliseconds * 1000)
