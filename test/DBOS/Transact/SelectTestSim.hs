{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.SelectTest' mirrored under IOSim over the in-memory
-- backend: the same pure check/record logic over simulated contexts.
-- Scenarios and checks are shared; this module owns the sim factory and
-- the sim-only extra — the trace must carry no 'WorkflowEvent' at all,
-- proving the select path never touches the tracer.
module DBOS.Transact.SelectTestSim (tests) where

import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (simTracer)
import DBOS.Prelude
import DBOS.SystemDB (NewWorkflow (..), Submission (..), WorkflowId (..), newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.IOSim (memConnectionOn, newMemDB, simIdentity)
import DBOS.Transact (WorkflowEvent (..))
import DBOS.Transact.Connection (nextExecutionIdentity)
import DBOS.Transact.Context (newWorkflowCtx, newWorkflowState)
import DBOS.Transact.SelectCases
  ( SelectFixture (..),
    checkControlError,
    checkFreshWinner,
    checkStaleWinner,
    scenarioControlError,
    scenarioFreshWinner,
    scenarioStaleWinner,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase)

-- | One fixture per leaf over a fresh in-memory database: contexts build
-- the same workflow state as the live tree, and the check/record pairs
-- write step rows against it.
simSelectFixture :: forall s. IOSim s (SelectFixture (IOSim s))
simSelectFixture = do
  mem <- newMemDB
  created <- SystemDB.initWorkflow mem ((newWorkflow "sim-select") {newWorkflowName = Just "SimSelectTest"}) Nothing Fresh Nothing
  case created of
    Left err -> error (show err)
    Right _ ->
      pure
        SelectFixture
          { scfRun = \action -> do
              conn <- memConnectionOn mem simTracer
              identity <- nextExecutionIdentity conn
              state <- newWorkflowState "sim-select" Nothing identity
              ctx <- newWorkflowCtx conn simIdentity state
              action ctx,
            scfListSteps = SystemDB.listSteps mem (WorkflowId "sim-select") True Nothing Nothing Nothing >>= either (error . show) pure
          }

tests :: TestTree
tests =
  testGroup
    "Durable select (Sim)"
    [ simCase simSelectFixture "a fresh select claims its id and records a winner" scenarioFreshWinner checkFreshWinner traceSelectSilent,
      simCase simSelectFixture "a winner outside the branches that exist now is refused" scenarioStaleWinner checkStaleWinner traceSelectSilent,
      testCase "a control signal is not a race decision" (either fail pure (checkControlError scenarioControlError))
    ]

-- | The select path never touches the tracer: no 'WorkflowEvent' may be
-- emitted by any select case.
traceSelectSilent :: SimTrace a -> IO ()
traceSelectSilent tr =
  assertBool "placement must emit no workflow events" (null (selectTraceEventsDynamic tr :: [WorkflowEvent]))
