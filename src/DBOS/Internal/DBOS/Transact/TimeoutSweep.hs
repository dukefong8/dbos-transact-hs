{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The Rust @timeout_sweep.rs@ workflow-timeout sweep: one tracked task per
-- executor, each pass cancelling the application's unsettled workflows
-- whose deadline has passed, oldest first, so a workflow nobody runs still
-- times out. A full batch repasses at once; any other pass sleeps a
-- second. A failed pass is said out loud and the loop goes on: the next
-- pass finds the same rows. Shutdown aborts the task with every other one.
module DBOS.Transact.TimeoutSweep (TimeoutSweepEvent (..), spawnTimeoutSweep) where

import DBOS.Prelude
import Data.Int (Int64)
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Error (renderError)
import DBOS.SystemDB.Types (WorkflowId (..))
import DBOS.Transact.Connection (Connection (..), runSystemDB)
import DBOS.Transact.Logger (LogEvent (..), LogSeverity (..), runTracer)
import DBOS.Transact.Workflow (Tasks, spawnTracked)
import System.Log.FastLogger (ToLogStr (..))

-- | The timeout sweep's events: what each pass cancelled, and what a pass
-- that failed leaves for the next one. Mirrors @timeout_sweep.rs@.
data TimeoutSweepEvent
  = SweepCancelled { sweepWorkflowId :: Text }
  | SweepFailed { sweepDetail :: Text }
  deriving stock (Eq, Show)

instance LogEvent TimeoutSweepEvent where
  eventSeverity SweepCancelled {} = SeverityDebug
  eventSeverity SweepFailed {}    = SeverityWarning
  renderEvent (SweepCancelled workflowId) =
    "the workflow exceeded its deadline and is cancelled workflow_id=" <> workflowId
  renderEvent (SweepFailed detail) =
    "could not cancel workflows past their deadline error=" <> detail

instance ToLogStr TimeoutSweepEvent where
  toLogStr = toLogStr . renderLine

-- | The most workflows one pass cancels. A full batch means more may be
-- waiting, so the next pass runs at once rather than after the interval.
sweepBatch :: Int64
sweepBatch = 1000

-- | How long the sweep waits after a pass that didn't fill its batch, in
-- microseconds: io-classes' timer ticks in microseconds.
sweepIntervalMicros :: Int
sweepIntervalMicros = 1000000

-- | Starts the sweep as a monitor: shutdown aborts it once the workflows
-- have drained, because a workflow still running may wait on it to record
-- its timeout. Its own task rather than a step of the dequeue
-- supervisor's tick, so back-to-back full batches never hold up queue
-- reconciliation.
spawnTimeoutSweep :: (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m) => Tasks m -> Connection m -> m ()
spawnTimeoutSweep tasks conn = void (spawnTracked tasks "timeout sweep" (sweepForever conn))

-- | Runs passes until the task is aborted.
sweepForever :: (MonadTimer m) => Connection m -> m ()
sweepForever conn = do
  full <- sweepOnce conn
  unless full (threadDelay sweepIntervalMicros)
  sweepForever conn

-- | One pass: cancel up to a batch of expired rows, announcing each, and
-- report whether the batch filled.
sweepOnce :: Monad m => Connection m -> m Bool
sweepOnce conn = do
  cancelled <- runSystemDB conn.connSysdb (\db -> SystemDB.cancelTimedOutWorkflows db sweepBatch)
  case cancelled of
    Left dbError -> do
      runTracer conn.connTracer (SweepFailed (renderError dbError))
      pure False
    Right ids -> do
      mapM_ (\(WorkflowId widText) -> runTracer conn.connTracer (SweepCancelled widText)) ids
      pure (length ids >= fromIntegral sweepBatch)
