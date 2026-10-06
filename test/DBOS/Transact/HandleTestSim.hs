{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.HandleTest' mirrored over simulated data: the same
-- scenarios and the same checks as the live tree, with values asserted
-- here — including the 'IOSim' typed trace assertions, which stay in this
-- module. The memory backend ('MemSystemDB', fresh per case) records rows
-- and steps for real, so retrieves, results, and awaits assert what live
-- asserts. Nothing prints: traces speak through types, not lines.
module DBOS.Transact.HandleTestSim (tests) where

import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (simTracer)
import DBOS.Prelude
import DBOS.SystemDB (WorkflowId (..))
import DBOS.SystemDB.IOSim (newMemDB, simEntropy, simGeneratedId, simIdentity)
import DBOS.Transact (EngineEvent (..), WorkflowEvent (..), configNew)
import DBOS.Transact.Connection (SomeSystemDB (..))
import DBOS.Transact.HandleCases
  ( HandleFixture (..),
    checkDeletedAbsent,
    checkDropHandle,
    checkFailError,
    checkResultAdopts,
    checkRetrieveStatus,
    checkScopedAwait,
    mkHandleFixture,
    scenarioDeletedAbsent,
    scenarioDropHandle,
    scenarioFailError,
    scenarioResultAdopts,
    scenarioRetrieveStatus,
    scenarioScopedAwait,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=))

-- | The sim half of the shared handle fixture: a fresh 'MemSystemDB' per
-- case (so cases stay isolated) passed in as 'SomeSystemDB' with the sim
-- carrier as 'SomeTracer', over the same 'mkHandleFixture' builder live
-- uses. Only the atoms differ.
simHandleFixture :: forall s. IOSim s (HandleFixture (IOSim s))
simHandleFixture = do
  mem <- newMemDB
  ids <- newTVarIO 0
  entropy <- newTVarIO 0
  mkHandleFixture
    (configNew "sim-app" "")
    simIdentity
    "sim-app"
    (WorkflowId . ("sim-" <>))
    (simGeneratedId ids)
    (simEntropy entropy)
    (SomeSystemDB mem)
    simTracer

tests :: TestTree
tests =
  testGroup
    "Workflow handle (Sim)"
    [ simCase simHandleFixture "a retrieved handle names its workflow and reads its status" scenarioRetrieveStatus checkRetrieveStatus traceRetrieveStatus,
      simCase simHandleFixture "a handle result adopts the recorded output" scenarioResultAdopts checkResultAdopts traceResultAdopts,
      simCase simHandleFixture "a handle result reports the error a failed run recorded" scenarioFailError checkFailError traceFailError,
      simCase simHandleFixture "a handle over a deleted row reports its absence" scenarioDeletedAbsent checkDeletedAbsent traceDeletedAbsent,
      simCase simHandleFixture "dropping a handle does not stop the workflow" scenarioDropHandle checkDropHandle traceDropHandle,
      simCase simHandleFixture "a scoped await records the child's result under the parent" scenarioScopedAwait checkScopedAwait traceScopedAwait
    ]

-- | The run records its step and completes; the shutdown ends the run.
traceRetrieveStatus :: forall a. SimTrace a -> IO ()
traceRetrieveStatus tr = do
  selectTraceEventsDynamic tr @?= [StepRunning "double" 0, StepOutputRecorded "double" 0, WorkflowCompleted "sim-handle-id"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | Same shape as the retrieval: one step, one completion.
traceResultAdopts :: forall a. SimTrace a -> IO ()
traceResultAdopts tr = do
  selectTraceEventsDynamic tr @?= [StepRunning "double" 0, StepOutputRecorded "double" 0, WorkflowCompleted "sim-handle-res-id"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | The body's own failure is recorded as the workflow's failure.
traceFailError :: forall a. SimTrace a -> IO ()
traceFailError tr = do
  selectTraceEventsDynamic tr @?= [WorkflowFailed "sim-handle-fail-id"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | The stepless body completes; the delete leaves no further trace.
traceDeletedAbsent :: forall a. SimTrace a -> IO ()
traceDeletedAbsent tr = do
  selectTraceEventsDynamic tr @?= [WorkflowCompleted "sim-handle-del-id"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | Same shape as the retrieval: the dropped handle changes nothing.
traceDropHandle :: forall a. SimTrace a -> IO ()
traceDropHandle tr = do
  selectTraceEventsDynamic tr @?= [StepRunning "double" 0, StepOutputRecorded "double" 0, WorkflowCompleted "sim-handle-drop-id"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | The child records its step and completes; the parent-side await adopts
-- the recorded output without launching the parent.
traceScopedAwait :: forall a. SimTrace a -> IO ()
traceScopedAwait tr = do
  selectTraceEventsDynamic tr @?= [StepRunning "double" 0, StepOutputRecorded "double" 0, WorkflowCompleted "sim-handle-await-child"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]
