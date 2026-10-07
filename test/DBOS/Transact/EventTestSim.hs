{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.EventTest' mirrored over simulated data: the same
-- scenarios and the same checks as the live tree, with values asserted
-- here — including the 'IOSim' typed trace assertions, which stay in this
-- module. The memory backend ('MemSystemDB', fresh per case) records rows,
-- steps, and events for real, so publishes, reads, and replays assert what
-- live asserts. Nothing prints: traces speak through types, not lines.
module DBOS.Transact.EventTestSim (tests) where

import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (simTracer)
import DBOS.Prelude
import DBOS.SystemDB (WorkflowId (..))
import DBOS.SystemDB.IOSim (newMemDB, simEntropy, simGeneratedId, simIdentity)
import DBOS.Transact
  (
  configNew,
  )
import DBOS.Transact.Recovery (EngineEvent (..))
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Step (WorkflowEvent (..))
import DBOS.Transact.Connection (SomeSystemDB (..))
import DBOS.Transact.EventCases
  ( EventFixture (..),
    checkCapturedRead,
    checkCheckpointedRead,
    checkOutOfOrderIds,
    checkPublishReplay,
    checkRecoveryKeepsFirst,
    checkRefusedSet,
    checkReplayNoRepublish,
    checkWrongInstance,
    mkEventFixture,
    scenarioCapturedRead,
    scenarioCheckpointedRead,
    scenarioOutOfOrderIds,
    scenarioPublishReplay,
    scenarioRecoveryKeepsFirst,
    scenarioRefusedSet,
    scenarioReplayNoRepublish,
    scenarioWrongInstance,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=))

-- | The sim half of the shared event fixture: a fresh 'MemSystemDB' per
-- case (so cases stay isolated) passed in as 'SomeSystemDB' with the sim
-- carrier as 'SomeTracer', over the same 'mkEventFixture' builder live
-- uses. Only the atoms differ.
simEventFixture :: forall s. IOSim s (EventFixture (IOSim s))
simEventFixture = do
  mem <- newMemDB
  ids <- newTVarIO 0
  entropy <- newTVarIO 0
  mkEventFixture
    (configNew "sim-app" "")
    (configNew "sim-other" "")
    simIdentity
    simIdentity {identityAppName = "sim-other"}
    "sim-app"
    (WorkflowId . ("sim-" <>))
    (simGeneratedId ids)
    (simEntropy entropy)
    (SomeSystemDB mem)
    simTracer

tests :: TestTree
tests =
  testGroup
    "Workflow events (Sim)"
    [ simCase simEventFixture "a workflow can publish and replay an event" scenarioPublishReplay checkPublishReplay traceEventSilent,
      simCase simEventFixture "a reading workflow is checkpointed and a reading step is not" scenarioCheckpointedRead checkCheckpointedRead traceCheckpointedRead,
      simCase simEventFixture "a refused set event spends no step id" scenarioRefusedSet checkRefusedSet traceEventSilent,
      simCase simEventFixture "a getEvent through a captured parent is plain and moves no ids" scenarioCapturedRead checkCapturedRead traceEventSilent,
      simCase simEventFixture "a replayed set event does not republish" scenarioReplayNoRepublish checkReplayNoRepublish traceEventSilent,
      simCase simEventFixture "progress events survive recovery without republishing" scenarioRecoveryKeepsFirst checkRecoveryKeepsFirst traceRecoveryKeepsFirst,
      simCase simEventFixture "reading through another instance from inside a workflow is refused" scenarioWrongInstance checkWrongInstance traceWrongInstance,
      simCase simEventFixture "library calls driven out of build order keep the ids they were built with" scenarioOutOfOrderIds checkOutOfOrderIds traceOutOfOrderIds
    ]

-- | The in-step read runs its step and records its output; nothing else
-- announces.
traceCheckpointedRead :: forall a. SimTrace a -> IO ()
traceCheckpointedRead tr = do
  selectTraceEventsDynamic tr @?= [StepOutputRecorded "read" 0]
  selectTraceEventsDynamic tr @?= ([] :: [EngineEvent])

-- | Direct event scopes touch no announced engine paths: publishes, reads,
-- and refusals record rows without emitting workflow events.
traceEventSilent :: forall a. SimTrace a -> IO ()
traceEventSilent tr = do
  selectTraceEventsDynamic tr @?= ([] :: [WorkflowEvent])
  selectTraceEventsDynamic tr @?= ([] :: [EngineEvent])

-- | The first run publishes and parks, the shutdown cancels it pending,
-- the relaunch recovers and announces, the driven pass and the supervisor
-- race to resume it (the loser parks on the spent release and dies at
-- shutdown — it adopts every step, so exactly-once still holds), and the
-- join adopts the completed run. Both launches shut down.
traceRecoveryKeepsFirst :: forall a. SimTrace a -> IO ()
traceRecoveryKeepsFirst tr = do
  selectTraceEventsDynamic tr @?= [WorkflowAlreadyOwned "sim-event-recovery-id", WorkflowCompleted "sim-event-recovery-id"]
  selectTraceEventsDynamic tr @?= [EngineCancelledRunning 1, EngineShutdown "sim-app", EngineRecovered 1, EngineLaunched "sim-app" "sim-executor" "0.0.0", EngineCancelledRunning 2, EngineShutdown "sim-app"]

-- | The cross-instance read fails the run; the plain in-step read runs its
-- step and completes. Both instances shut down.
traceWrongInstance :: forall a. SimTrace a -> IO ()
traceWrongInstance tr = do
  selectTraceEventsDynamic tr @?= [WorkflowFailed "sim-event-wrong-id", StepRunning "read" 0, StepOutputRecorded "read" 0, WorkflowCompleted "sim-event-in-step-id"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app", EngineShutdown "sim-other"]

-- | The out-of-order run completes; the driven step records its output.
traceOutOfOrderIds :: forall a. SimTrace a -> IO ()
traceOutOfOrderIds tr = do
  selectTraceEventsDynamic tr @?= [StepOutputRecorded "after" 4, WorkflowCompleted "sim-joins-out-of-order"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]
