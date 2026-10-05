{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.CheckpointTest' mirrored under IOSim over the in-memory
-- backend: the same pure placement logic over simulated contexts.
-- Scenarios and checks are shared; this module owns the sim factory and
-- the sim-only extra — the trace must carry no 'WorkflowEvent' at all,
-- proving placement never touches the tracer.
module DBOS.Transact.CheckpointTestSim (tests) where

import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (simTracer)
import DBOS.Prelude
import DBOS.SystemDB.IOSim (memConnectionOn, newMemDB, simIdentity)
import DBOS.Transact (WorkflowEvent (..))
import DBOS.Transact.Checkpoint (takenPlacement)
import DBOS.Transact.CheckpointCases
  ( CheckpointFixture (..),
    checkBoundaryRecords,
    checkCapturedParent,
    checkClientPlain,
    checkInStepOwnBody,
    checkLeafRule,
    checkOutside,
    checkPlacementNames,
    checkRecordedDurable,
    checkRecordedRefused,
    checkSiblingRefused,
    checkTakenOther,
    scenarioBoundaryRecords,
    scenarioCapturedParent,
    scenarioClientPlain,
    scenarioInStepOwnBody,
    scenarioLeafRule,
    scenarioOutside,
    scenarioPlacementNames,
    scenarioRecordedDurable,
    scenarioRecordedRefused,
    scenarioSiblingRefused,
    scenarioTakenOther,
  )
import DBOS.Transact.Connection (nextExecutionIdentity)
import DBOS.Transact.Context (newWorkflowCtx, newWorkflowState)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool)

-- | One fixture per leaf over a fresh in-memory database: contexts build
-- the same workflow state ("wf-1") as the live tree, and the
-- taken-placement probe runs over a second simulated connection.
simCheckpointFixture :: forall s. IOSim s (CheckpointFixture (IOSim s))
simCheckpointFixture = do
  mem <- newMemDB
  let simCtx = do
        conn <- memConnectionOn mem simTracer
        identity <- nextExecutionIdentity conn
        state <- newWorkflowState "wf-1" Nothing identity
        newWorkflowCtx conn simIdentity state
  pure
    ( CheckpointFixture
        { ccfWithCtx = \run -> simCtx >>= run,
          ccfTakenOther = \ctx -> do
            other <- memConnectionOn mem simTracer
            takenPlacement other "get_event" ctx
        }
    )

tests :: TestTree
tests =
  testGroup
    "Checkpoint placement (Sim)"
    [ simCase simCheckpointFixture "outside a workflow takes no id and records nothing" scenarioOutside checkOutside traceCheckpointSilent,
      simCase simCheckpointFixture "at a step boundary the call records under the allocated id" scenarioBoundaryRecords checkBoundaryRecords traceCheckpointSilent,
      simCase simCheckpointFixture "a call built through a captured parent while a step body runs is plain" scenarioCapturedParent checkCapturedParent traceCheckpointSilent,
      simCase simCheckpointFixture "a taken placement through a captured parent under another connection is plain" scenarioTakenOther checkTakenOther traceCheckpointSilent,
      simCase simCheckpointFixture "inside a step body the call is plain by the leaf rule" scenarioLeafRule checkLeafRule traceCheckpointSilent,
      simCase simCheckpointFixture "a recorded call polled at its boundary stays durable" scenarioRecordedDurable checkRecordedDurable traceCheckpointSilent,
      simCase simCheckpointFixture "a recorded call carried into a step is refused" scenarioRecordedRefused checkRecordedRefused traceCheckpointSilent,
      simCase simCheckpointFixture "a client's call stays plain wherever it is driven" scenarioClientPlain checkClientPlain traceCheckpointSilent,
      simCase simCheckpointFixture "an in-step call polled in its own body stays plain" scenarioInStepOwnBody checkInStepOwnBody traceCheckpointSilent,
      simCase simCheckpointFixture "an in-step call carried to a sibling body is refused" scenarioSiblingRefused checkSiblingRefused traceCheckpointSilent,
      simCase simCheckpointFixture "only outside has no workflow around it" scenarioPlacementNames checkPlacementNames traceCheckpointSilent
    ]

-- | Placement never touches the tracer: no 'WorkflowEvent' may be
-- emitted by any placement case.
traceCheckpointSilent :: SimTrace a -> IO ()
traceCheckpointSilent tr =
  assertBool "placement must emit no workflow events" (null (selectTraceEventsDynamic tr :: [WorkflowEvent]))
