{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.DeadlinesTest' mirrored under IOSim over the in-memory
-- backend with the shared launch tail: deadlines are virtual, so the hang,
-- the 100ms cancellation, and the crash-and-relaunch all run
-- deterministically and fast. Scenarios and checks are shared; this module
-- owns the sim factory. Traces assert nothing structural: the shared
-- checks (recorded outcome, row status, kept deadline) carry the proof.
module DBOS.Transact.DeadlinesTestSim (tests) where

import Control.Monad.IOSim (IOSim)
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (simTracer)
import DBOS.Prelude
import DBOS.SystemDB (WorkflowId (..), WorkflowRecord (..))
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.IOSim (memLaunchOn, newMemDB, simInstance)
import DBOS.Transact.DeadlinesCases
  ( DeadlinesFixture (..),
    checkBeatenDeadline,
    checkKeptDeadline,
    checkPastDeadline,
    checkShutdownPending,
    checkWithinDeadline,
    scenarioBeatenDeadline,
    scenarioKeptDeadline,
    scenarioPastDeadline,
    scenarioShutdownPending,
    scenarioWithinDeadline,
  )
import Test.Tasty (TestTree, testGroup)

-- | One fixture per leaf over a fresh in-memory database: the instance
-- launches over it, and relaunch runs the same launch tail (with its
-- recovery sweep) over the same store.
simDeadlinesFixture :: forall s. IOSim s (DeadlinesFixture (IOSim s))
simDeadlinesFixture = do
  mem <- newMemDB
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
            Right (Just record) -> pure record.workflowRecordDeadline
      }

tests :: TestTree
tests =
  testGroup
    "Workflow deadlines (Sim)"
    [ simCase simDeadlinesFixture "a workflow within its deadline is unaffected" scenarioWithinDeadline checkWithinDeadline noTrace,
      simCase simDeadlinesFixture "a workflow past its deadline is cancelled" scenarioPastDeadline checkPastDeadline noTrace,
      simCase simDeadlinesFixture "a recovered workflow keeps the deadline it already had" scenarioKeptDeadline checkKeptDeadline noTrace,
      simCase simDeadlinesFixture "shutdown does not durably cancel a workflow that has a deadline" scenarioShutdownPending checkShutdownPending noTrace,
      simCase simDeadlinesFixture "a deadline that loses to a recorded outcome reports that outcome" scenarioBeatenDeadline checkBeatenDeadline noTrace
    ]
  where
    noTrace _ = pure ()
