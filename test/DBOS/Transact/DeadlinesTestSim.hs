{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.DeadlinesTest' mirrored under IOSim over the in-memory
-- backend with the shared launch tail: deadlines are virtual, so the hang,
-- the 100ms cancellation, and the crash-and-relaunch all run
-- deterministically and fast. Scenarios and checks are shared; this module
-- owns the sim factory. Traces assert nothing structural: the shared
-- checks (recorded outcome, row status, kept deadline) carry the proof.
module DBOS.Transact.DeadlinesTestSim (tests) where

import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (simTracer)
import DBOS.Prelude
import Data.Int (Int64)
import DBOS.SystemDB (WorkflowId (..), WorkflowRecord (..))
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.IOSim (memLaunchOn, newMemDB, newMemDBSkewed, simInstance)
import DBOS.Transact.Step (WorkflowEvent (..))
import DBOS.Transact.TimeoutSweep (TimeoutSweepEvent (..))
import DBOS.Transact.DeadlinesCases
  ( DeadlinesFixture (..),
    checkBeatenDeadline,
    checkDelayedHoldBehind,
    checkDelayedReleaseAhead,
    checkKeptDeadline,
    checkPastDeadline,
    checkShutdownPending,
    checkSkewHoldsBudget,
    checkFinishedIdAdopts,
    checkSweepCancelsAbandoned,
    checkWithinDeadline,
    scenarioBeatenDeadline,
    scenarioDelayedHoldBehind,
    scenarioDelayedReleaseAhead,
    scenarioKeptDeadline,
    scenarioPastDeadline,
    scenarioShutdownPending,
    scenarioSkewHoldsBudget,
    scenarioFinishedIdAdopts,
    scenarioSweepCancelsAbandoned,
    scenarioWithinDeadline,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=))

-- | One fixture per leaf over a fresh in-memory database: the instance
-- launches over it, and relaunch runs the same launch tail (with its
-- recovery sweep) over the same store.
simDeadlinesFixture :: forall s. IOSim s (DeadlinesFixture (IOSim s))
simDeadlinesFixture = simDeadlinesFixtureSkewed 0

-- | The fixture over a database whose clock runs @skewMs@ ahead of this
-- process's monotonic clock (behind, when negative): the oracle's skew
-- tests run the database minutes off the process, and the engine must time
-- budgets from the database's reading all the same.
simDeadlinesFixtureSkewed :: forall s. Int64 -> IOSim s (DeadlinesFixture (IOSim s))
simDeadlinesFixtureSkewed skewMs = do
  mem <- newMemDBSkewed skewMs
  dbos <- simInstance
  pure
    DeadlinesFixture
      { dfSetup = pure (dbos, WorkflowId "sim-deadline"),
        dfLaunch = \_ -> memLaunchOn mem simTracer dbos,
        dfReadStatus = \wid -> do
          found <- SystemDB.getWorkflow mem wid
          case found of
            Left err -> error (show err)
            Right Nothing -> pure Nothing
            Right (Just record) -> pure (Just record.workflowRecordStatus),
        dfReadDeadline = \wid -> do
          found <- SystemDB.getWorkflow mem wid
          case found of
            Left err -> error (show err)
            Right Nothing -> pure Nothing
            Right (Just record) -> pure record.workflowRecordDeadline,
        dfPlantExpired = \wid -> do
          let WorkflowId widText = wid
              expired = (SystemDB.newWorkflow widText) {SystemDB.newWorkflowDeadline = Just (SystemDB.timestampFromEpochMs 1000)}
          planted <- SystemDB.initWorkflow mem expired Nothing
          case planted of
            Left err -> error (show err)
            Right _ -> pure (),
        dfPlantDelayed = \wid delay -> do
          let WorkflowId widText = wid
              delayed = (SystemDB.newWorkflow widText) {SystemDB.newWorkflowQueueName = Just "sweep-test-q", SystemDB.newWorkflowDelay = Just delay}
          planted <- SystemDB.initWorkflow mem delayed Nothing
          case planted of
            Left err -> error (show err)
            Right _ -> pure (),
        dfSweepDelayed = do
          swept <- SystemDB.transitionDelayedWorkflows mem
          case swept of
            Left err -> error (show err)
            Right _ -> pure ()
      }

tests :: TestTree
tests =
  testGroup
    "Workflow deadlines (Sim)"
    [ simCase simDeadlinesFixture "a workflow within its deadline is unaffected" scenarioWithinDeadline checkWithinDeadline noTrace,
      simCase simDeadlinesFixture "a workflow past its deadline is cancelled" scenarioPastDeadline checkPastDeadline noTrace,
      simCase simDeadlinesFixture "a recovered workflow keeps the deadline it already had" scenarioKeptDeadline checkKeptDeadline noTrace,
      simCase simDeadlinesFixture "shutdown does not durably cancel a workflow that has a deadline" scenarioShutdownPending checkShutdownPending noTrace,
      simCase simDeadlinesFixture "a deadline that loses to a recorded outcome reports that outcome" scenarioBeatenDeadline checkBeatenDeadline noTrace,
      simCase simDeadlinesFixture "a budget holds with the database clock in agreement" scenarioSkewHoldsBudget checkSkewHoldsBudget noTrace,
      simCase simDeadlinesFixture "restarting a finished id adopts without rerunning" scenarioFinishedIdAdopts checkFinishedIdAdopts traceFinishedIdAdopts,
      simCase (simDeadlinesFixtureSkewed 60000) "a budget holds with the database clock a minute ahead" scenarioSkewHoldsBudget checkSkewHoldsBudget noTrace,
      simCase (simDeadlinesFixtureSkewed (-60000)) "a budget holds with the database clock a minute behind" scenarioSkewHoldsBudget checkSkewHoldsBudget noTrace,
      simCase simDeadlinesFixture "a delayed release follows the database clock in agreement" scenarioDelayedReleaseAhead checkDelayedReleaseAhead noTrace,
      simCase (simDeadlinesFixtureSkewed 3600000) "a delayed release follows the database clock an hour ahead" scenarioDelayedReleaseAhead checkDelayedReleaseAhead noTrace,
      simCase simDeadlinesFixture "a delayed hold follows the database clock in agreement" scenarioDelayedHoldBehind checkDelayedHoldBehind noTrace,
      simCase (simDeadlinesFixtureSkewed (-3600000)) "a delayed hold follows the database clock an hour behind" scenarioDelayedHoldBehind checkDelayedHoldBehind noTrace,
      simCase simDeadlinesFixture "the sweep cancels an abandoned expired row" scenarioSweepCancelsAbandoned checkSweepCancelsAbandoned traceSweepCancels
    ]
  where
    noTrace _ = pure ()

-- | The sweep announces the row it cancels.
traceSweepCancels :: SimTrace a -> IO ()
traceSweepCancels tr = selectTraceEventsDynamic tr @?= [SweepCancelled "sim-deadline"]

-- | The restart announces its join: one step run, one recorded output,
-- one completion, and the second run adopts with the owned announcement,
-- as every finished-id join does.
traceFinishedIdAdopts :: SimTrace a -> IO ()
traceFinishedIdAdopts tr = selectTraceEventsDynamic tr @?= [StepRunning "only" 0, StepOutputRecorded "only" 0, WorkflowCompleted "sim-deadline", WorkflowAlreadyOwned "sim-deadline"]
