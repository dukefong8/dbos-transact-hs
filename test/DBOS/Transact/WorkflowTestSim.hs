{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

-- | 'DBOS.Transact.WorkflowTest' mirrored over simulated data: the same
-- scenarios and the same assertions as the live tree, with values asserted
-- here — including the 'IOSim' typed trace assertions, which stay in this
-- module. Each case prints its sim's 'Say' trace inline, so a plain
-- @-- $> tasty@ run shows the workflow announcements with no extra
-- plumbing. The memory backend ('MemSystemDB', fresh per case) records
-- rows, steps, and dedup holds for real, so joins, replays, and row reads
-- assert what live asserts; what needs a fleet stays live-only and says
-- so: recovery sweeps (including the recorded-await replay, which needs a
-- second process and a deleted child), unregistered skips, queued
-- supervision, and the fan-out/select timing.
module DBOS.Transact.WorkflowTestSim (tests) where

import Control.Monad.IOSim (IOSim, SimTrace, runSimOrThrow, selectTraceEventsDynamic)
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (runSimCase, simTracer)
import DBOS.Prelude
import DBOS.SystemDB (WorkflowId (..))
import DBOS.SystemDB.IOSim (newMemDB, simEntropy, simGeneratedId, simIdentity)
import DBOS.Transact
  (
  configNew,
  )
import DBOS.Transact.Logger (runTracer)
import DBOS.Transact.Recovery (EngineEvent (..))
import DBOS.Transact.Step (WorkflowEvent (..))
import DBOS.Transact.Connection (SomeSystemDB (..))
import DBOS.Transact.WorkflowCases
  ( WfFixture (..),
    checkAppErrorRoundtrip,
    checkAwaitInsideStep,
    checkAwaitRecorded,
    checkCancelledChildAwaited,
    checkCascadeDeadline,
    checkCaptureChildRefused,
    checkChildBudgetWins,
    checkChildIdsInBuildOrder,
    checkChildInsideStepRefused,
    checkControlSelect,
    checkDbFailureNotOutcome,
    checkDeadlineInherited,
    checkDropFuture,
    checkAttributes,
    checkShutdownCancels,
    checkBudgetCancels,
    checkStepErrorRecorded,
    checkStepsTaken,
    checkWrongInstance,
    checkDeclinedDeadline,
    checkFanout,
    checkFreshJoinPolls,
    checkJoinHeldKey,
    checkEnqueuedChildReplays,
    checkJoinTakesId,
    checkLiftChildError,
    checkLosingTokenFired,
    checkNoMiscounts,
    checkPanic,
    checkPlainStepAtStart,
    checkRegisteredResult,
    checkDerivedChildAdopted,
    checkAssignedChildAdopted,
    checkRootNoParent,
    checkRowBeforeBody,
    checkRunBeforeLaunch,
    checkScopedBody,
    checkScopedSelect,
    checkSelectStepRaces,
    checkStaleAwaitRefused,
    checkStepIdPairs,
    checkUnawaitedChild,
    checkZeroNoInput,
    mkWfFixture,
    timeoutOptionsCase,
    scenarioAppErrorRoundtrip,
    scenarioAwaitInsideStep,
    scenarioAwaitRecorded,
    scenarioBudgetCancels,
    scenarioCancelledChildAwaited,
    scenarioCascadeDeadline,
    scenarioCaptureChildRefused,
    scenarioChildBudgetWins,
    scenarioChildIdsInBuildOrder,
    scenarioChildInsideStepRefused,
    scenarioControlSelect,
    scenarioDbFailureNotOutcome,
    scenarioDeadlineInherited,
    scenarioDropFuture,
    scenarioAttributes,
    scenarioShutdownCancels,
    scenarioBudgetCancels,
    scenarioStepErrorRecorded,
    scenarioStepsTaken,
    scenarioWrongInstance,
    scenarioDeclinedDeadline,
    scenarioFanout,
    scenarioFreshJoinPolls,
    scenarioJoinHeldKey,
    scenarioEnqueuedChildReplays,
    scenarioJoinTakesId,
    scenarioLiftChildError,
    scenarioLosingTokenFired,
    scenarioPanic,
    scenarioPlainStepAtStart,
    scenarioRegisteredRecordsResult,
    scenarioDerivedChildAdopted,
    scenarioAssignedChildAdopted,
    scenarioRootNoParent,
    scenarioRowBeforeBody,
    scenarioRetrieveBeforeLaunch,
    scenarioScopedBody,
    scenarioScopedSelect,
    scenarioSelectStepRaces,
    scenarioStaleAwaitRefused,
    scenarioStepIdPairs,
    scenarioUnawaitedChild,
    scenarioZeroNoInput,
    taskAbortAllWaits,
    taskEarlyFinishNotSwept,
    taskEmptySweep,
    taskFinishedNotRegistered,
    taskRefusedAfterSweep
  )
import Test.Tasty (DependencyType (..), TestTree, dependentTestGroup, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

tests :: TestTree
tests =
  -- Sequential: cases announce through one shared stderr, so parallel
  -- 'printSimTrace' calls would interleave their lines mid-character.
  -- 'AllFinish' keeps every case running on a failure, in order.
  dependentTestGroup
    "Workflow execution (Sim)"
    AllFinish
    [ simCase simWfFixture "a registered workflow starts and records its result" scenarioRegisteredRecordsResult checkRegisteredResult traceRegisteredResult,
      -- IO only: crash-and-relaunch recovery sweep (MemSystemDB delegates
      -- reenqueueForRecovery to the canned mock; see ADR-0020).
      testCase "a recovery run replays completed steps after a body interruption" (pure ()),
      -- IO only: same recovery sweep as above.
      testCase "an unregistered workflow is skipped and the rest recover" (pure ()),
      testCase "timeouts, options, and child ids compose without a database" timeoutOptionsCase,
      simCase simWfFixture "starting a taken id joins the existing run" scenarioJoinTakesId checkJoinTakesId traceJoinTakesId,
      simCase simWfFixture "a fresh start is local and a join polls" scenarioFreshJoinPolls checkFreshJoinPolls traceFreshJoinPolls,
      simCase simWfFixture "awaiting a child is recorded as a step" scenarioAwaitRecorded checkAwaitRecorded traceAwaitRecorded,
      simCase simWfFixture "a recorded await of another workflow is refused" scenarioStaleAwaitRefused checkStaleAwaitRefused traceStaleAwaitRefused,
      simCase simWfFixture "awaiting a child inside a step is covered by that step" scenarioAwaitInsideStep checkAwaitInsideStep traceAwaitInsideStep,
      simCase simWfFixture "child starts and awaits keep their ids in build order" scenarioChildIdsInBuildOrder checkChildIdsInBuildOrder traceChildIdsInBuildOrder,
      simCase simWfFixture "runs claim their pairs of step ids adjacently" scenarioStepIdPairs checkStepIdPairs traceStepIdPairs,
      simCase simWfFixture "a select step races a step against a child's result" scenarioSelectStepRaces checkSelectStepRaces traceSelectStepRaces,
      simCase simWfFixture "a scoped select races two pending steps" scenarioScopedSelect checkScopedSelect traceScopedSelect,
      simCase simWfFixture "a converted body runs through the scoped entries" scenarioScopedBody checkScopedBody traceScopedBody,
      simCase simWfFixture "a control signal winning a select records no winner" scenarioControlSelect checkControlSelect traceControlSelect,
      simCase simWfFixture "a losing step has its cancellation token fired" scenarioLosingTokenFired checkLosingTokenFired traceLosingTokenFired,
      simCase simWfFixture "a cancelled child is an awaited cancellation in the parent" scenarioCancelledChildAwaited checkCancelledChildAwaited traceCancelledChildAwaited,
      -- IO only: recorded-await replay across two launches (needs the
      -- recovery sweep).
      testCase "a replayed parent reads the recorded outcome rather than waiting again" (pure ()),
      simCase simWfFixture "a child inherits its parent's deadline" scenarioDeadlineInherited checkDeadlineInherited traceDeadlineInherited,
      simCase simWfFixture "a child's own timeout replaces the inherited deadline" scenarioChildBudgetWins checkChildBudgetWins traceChildBudgetWins,
      simCase simWfFixture "a child can decline the inherited deadline" scenarioDeclinedDeadline checkDeclinedDeadline traceDeclinedDeadline,
      simCase simWfFixture "a parent and its child hit an inherited deadline independently" scenarioCascadeDeadline checkCascadeDeadline traceCascadeDeadline,
      simCase simWfFixture "a parent starts a child under a derived id and replay adopts it" scenarioDerivedChildAdopted checkDerivedChildAdopted traceDerivedChildAdopted,
      simCase simWfFixture "starting a child inside a step is refused, not recorded" scenarioChildInsideStepRefused checkChildInsideStepRefused traceChildInsideStepRefused,
      simCase simWfFixture "starting a child through a captured parent is refused, not recorded" scenarioCaptureChildRefused checkCaptureChildRefused traceCaptureChildRefused,
      simCase simWfFixture "a child that fails differently is started through lift" scenarioLiftChildError checkLiftChildError traceLiftChildError,
      -- IO only: the body performs real IO (the foreign charge call),
      -- which the simulator cannot run.
      testCase "a foreign error is converted at the boundary" (pure ()),
      simCase simWfFixture "a child started and never awaited is still recorded" scenarioUnawaitedChild checkUnawaitedChild traceUnawaitedChild,
      simCase simWfFixture "children started in a loop run concurrently" scenarioFanout checkFanout traceFanout,
      -- IO only: first-to-settle timing is wall-clock-bound.
      testCase "select reports the first workflow to settle, not the first started" (pure ()),
      simCase simWfFixture "an assigned child id wins over the derived one" scenarioAssignedChildAdopted checkAssignedChildAdopted traceAssignedChildAdopted,
      simCase simWfFixture "a workflow started outside a workflow has no parent" scenarioRootNoParent checkRootNoParent traceRootNoParent,
      simCase simWfFixture "a start position holding a plain step is refused" scenarioPlainStepAtStart checkPlainStepAtStart tracePlainStepAtStart,
      simCase simWfFixture "a child started through another instance is refused" scenarioWrongInstance checkWrongInstance traceWrongInstance,
      simCase simWfFixture "a child joining a held key is recorded as the workflow it joined" scenarioJoinHeldKey checkJoinHeldKey traceJoinHeldKey,
      simCase simWfFixture "an in-workflow enqueue is a recorded child start that replays" scenarioEnqueuedChildReplays checkEnqueuedChildReplays traceEnqueuedChildReplays,
      simCase simWfFixture "a zero-argument workflow records no input" scenarioZeroNoInput checkZeroNoInput traceZeroNoInput,
      simCase simWfFixture "the row exists before the body starts" scenarioRowBeforeBody checkRowBeforeBody traceRowBeforeBody,
      simCase simWfFixture "a panicking workflow leaves its row pending" scenarioPanic checkPanic tracePanic,
      simCase simWfFixture "retrieving before launch is refused" scenarioRetrieveBeforeLaunch checkRunBeforeLaunch traceRunBeforeLaunch,
      simCase simWfFixture "an application error round-trips as itself" scenarioAppErrorRoundtrip checkAppErrorRoundtrip traceAppErrorRoundtrip,
      simCase simWfFixture "a database failure is not the workflow outcome" scenarioDbFailureNotOutcome checkDbFailureNotOutcome traceDbFailureNotOutcome,
      simCase simWfFixture "a workflow records the steps it took" scenarioStepsTaken checkStepsTaken traceStepsTaken,
      simCase simWfFixture "shutdown cancels a running workflow and leaves it pending" scenarioShutdownCancels checkShutdownCancels traceShutdownCancels,
      simCase simWfFixture "dropping the future does not stop the workflow" scenarioDropFuture checkDropFuture traceDropFuture,
      simCase simWfFixture "a budget cancels the workflow durably" scenarioBudgetCancels checkBudgetCancels traceBudgetCancels,
      simCase simWfFixture "a started workflow carries the attributes it was given" scenarioAttributes checkAttributes traceAttributes,
      simCase simWfFixture "a step error is recorded in its column" scenarioStepErrorRecorded checkStepErrorRecorded traceStepErrorRecorded,
      -- Sim only: typed trace assertions live only in sim.
      testCase "workflow announcements carry their counts and ids" $ do
        (_, tr) <- runSimCase demoTrace
        selectTraceEventsDynamic tr
          @?= [ WorkflowEnqueued "sim-wf-enqueued" "sim-queue",
                WorkflowAlreadyOwned "sim-wf-owned",
                WorkflowChildJoined "sim-parent" 0 "sim-child",
                WorkflowDedupJoined "sim-holder" "sim-key",
                WorkflowDeadlineRaced "sim-wf-raced",
                WorkflowOutcomeRecordFailed "sim-detail",
                WorkflowSuperseded "sim-wf-first",
                WorkflowControlEnded "sim-control"
              ],
      tasksSimTests
    ]

-- | The oracle's @Tasks@ behaviour over the cooperative scheduler: the
-- same bodies the live tree runs, judged by the same assertions. The
-- bodies emit no tracer events, so each leaf runs 'runSimOrThrow' once
-- with no trace to print.
tasksSimTests :: TestTree
tasksSimTests =
  testGroup
    "Tasks"
    [       testCase "abortAll waits until every task has departed" $ do
        let simRun :: forall s. IOSim s Int
            simRun = taskAbortAllWaits @(IOSim s)
        (@?= 2) (runSimOrThrow simRun),
      testCase "a task that finished on its own is not left in the registry" $ do
        let simRun :: forall s. IOSim s Int
            simRun = taskFinishedNotRegistered @(IOSim s) simWaitDeparture
        (@?= 0) (runSimOrThrow simRun),
      testCase "a task arriving after the sweep is aborted on arrival" $ do
        let simRun :: forall s. IOSim s Bool
            simRun = taskRefusedAfterSweep @(IOSim s)
        (@?= False) (runSimOrThrow simRun),
      testCase "an empty sweep returns at once" $ do
        let simRun :: forall s. IOSim s Int
            simRun = taskEmptySweep @(IOSim s)
        (@?= 0) (runSimOrThrow simRun),
      -- IO only: real preemption, not cooperation.
      testCase "a spawn refused after abort fills its channel instead of hanging" (pure ()),
      testCase "a task finishing before registration is not swept as aborted" $ do
        let simRun :: forall s. IOSim s [Int]
            simRun = taskEarlyFinishNotSwept @(IOSim s) simWaitDeparture
        checkNoMiscounts (runSimOrThrow simRun)
    ]

-- | The announcement shapes no staged case reaches: a superseded write,
-- a raced deadline, and a failed outcome write need races and faults the
-- sim does not stage; the rest ride the cases above. Hand-emitted through
-- the say-carrier, asserted by type.
demoTrace :: forall s. IOSim s ()
demoTrace = do
  runTracer simTracer (WorkflowEnqueued "sim-wf-enqueued" "sim-queue")
  runTracer simTracer (WorkflowAlreadyOwned "sim-wf-owned")
  runTracer simTracer (WorkflowChildJoined "sim-parent" 0 "sim-child")
  runTracer simTracer (WorkflowDedupJoined "sim-holder" "sim-key")
  runTracer simTracer (WorkflowDeadlineRaced "sim-wf-raced")
  runTracer simTracer (WorkflowOutcomeRecordFailed "sim-detail")
  runTracer simTracer (WorkflowSuperseded "sim-wf-first")
  runTracer simTracer (WorkflowControlEnded "sim-control")

-- | The sim half of the shared workflow fixture: a fresh 'MemSystemDB'
-- per case (so cases stay isolated) passed in as 'SomeSystemDB' with the
-- sim carrier as 'SomeTracer', over the same 'mkWfFixture' builder live
-- uses. Only the atoms differ.
simWfFixture :: forall s. IOSim s (WfFixture (IOSim s))
simWfFixture = do
  mem <- newMemDB
  ids <- newTVarIO 0
  entropy <- newTVarIO 0
  mkWfFixture
    (configNew "sim-app" "")
    simIdentity
    "sim-app"
    (WorkflowId . ("sim-" <>))
    (simGeneratedId ids)
    (simEntropy entropy)
    (SomeSystemDB mem)
    simTracer

-- | Typed event assertions for the converted sim leaves: what the engine
-- emitted, constructor by constructor. Read off the say trace once, then
-- pinned here so the watcher verifies events without printing them.
traceRegisteredResult :: forall a. SimTrace a -> IO ()
traceRegisteredResult tr = do
  selectTraceEventsDynamic tr @?= [StepRunning "double" 0, StepOutputRecorded "double" 0, WorkflowCompleted "sim-wf-double"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

traceJoinTakesId :: forall a. SimTrace a -> IO ()
traceJoinTakesId tr = do
  selectTraceEventsDynamic tr @?= [WorkflowAlreadyOwned "sim-join-start", WorkflowCompleted "sim-join-start"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

traceFreshJoinPolls :: forall a. SimTrace a -> IO ()
traceFreshJoinPolls tr = do
  selectTraceEventsDynamic tr @?= [WorkflowAlreadyOwned "sim-local-id", WorkflowCompleted "sim-local-id"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

traceAwaitRecorded :: forall a. SimTrace a -> IO ()
traceAwaitRecorded tr = do
  selectTraceEventsDynamic tr @?= [WorkflowCompleted "sim-await-parent-0", WorkflowCompleted "sim-await-parent"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The body's own failure is recorded as the workflow's failure.
traceAppErrorRoundtrip :: forall a. SimTrace a -> IO ()
traceAppErrorRoundtrip tr = do
  selectTraceEventsDynamic tr @?= [WorkflowFailed "sim-app-err-id"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The cross-instance refusal writes nothing and the parent fails.
traceWrongInstance :: forall a. SimTrace a -> IO ()
traceWrongInstance tr = do
  selectTraceEventsDynamic tr @?= [WorkflowFailed "sim-wrong-instance-parent"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app", EngineShutdown "sim-app"]

-- The two steps run and record, then the workflow completes.
traceStepsTaken :: forall a. SimTrace a -> IO ()
traceStepsTaken tr = do
  selectTraceEventsDynamic tr
    @?= [ StepRunning "one" 0,
          StepOutputRecorded "one" 0,
          StepRunning "two" 1,
          StepOutputRecorded "two" 1,
          WorkflowCompleted "sim-steps-listed-id"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The executor shuts down with a run still gated: the shutdown
-- announces the cancelled task and then itself.
traceShutdownCancels :: forall a. SimTrace a -> IO ()
traceShutdownCancels tr = do
  selectTraceEventsDynamic tr @?= [EngineCancelledRunning 1, EngineShutdown "sim-app"]
  selectTraceEventsDynamic tr @?= ([] :: [WorkflowEvent])

-- The released run finishes and completes.
traceDropFuture :: forall a. SimTrace a -> IO ()
traceDropFuture tr = do
  selectTraceEventsDynamic tr @?= [WorkflowCompleted "sim-drop-future-id"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The child settles, then the attributed parent completes.
traceAttributes :: forall a. SimTrace a -> IO ()
traceAttributes tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowCompleted "sim-attributes-parent-0",
          WorkflowCompleted "sim-attributes-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The step's error is recorded, then the workflow fails.
traceStepErrorRecorded :: forall a. SimTrace a -> IO ()
traceStepErrorRecorded tr = do
  selectTraceEventsDynamic tr
    @?= [ StepErrorRecorded "charge" 0,
          WorkflowFailed "sim-step-err-id"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The millisecond budget cancels the run: the deadline event names the
-- workflow, then the executor shuts down.
traceBudgetCancels :: forall a. SimTrace a -> IO ()
traceBudgetCancels tr = do
  selectTraceEventsDynamic tr @?= [WorkflowDeadlineCancelled "sim-budget-id"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The holder enqueues, the parent joins it by key, both complete: the
-- recorded start and await name the holder.
traceJoinHeldKey :: forall a. SimTrace a -> IO ()
traceJoinHeldKey tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowEnqueued "sim-join-holder" "join-q-sim-join-holder",
          WorkflowDedupJoined "sim-join-holder" "order-42-sim-join-holder",
          WorkflowCompleted "sim-join-holder",
          WorkflowCompleted "sim-join-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The parent enqueues its child and completes; the replay joins the existing
-- run instead of enqueuing another.
traceEnqueuedChildReplays :: forall a. SimTrace a -> IO ()
traceEnqueuedChildReplays tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowEnqueued "sim-enqueued-child-parent-0" "enqueued-child-q-sim-enqueued-child-q",
          WorkflowCompleted "sim-enqueued-child-parent",
          WorkflowAlreadyOwned "sim-enqueued-child-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The backend failure leaves the row pending: the control end carries
-- the database error and no outcome is recorded.
traceDbFailureNotOutcome :: forall a. SimTrace a -> IO ()
traceDbFailureNotOutcome tr = do
  selectTraceEventsDynamic tr
    @?= [WorkflowControlEnded "system database error: connection reset by peer"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The body's panic escapes: the engine announces the panicked workflow
-- and leaves its row pending.
tracePanic :: forall a. SimTrace a -> IO ()
tracePanic tr = do
  selectTraceEventsDynamic tr @?= [WorkflowPanicked "sim-panic-id"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- Nothing launched, so nothing is announced.
traceRunBeforeLaunch :: forall a. SimTrace a -> IO ()
traceRunBeforeLaunch tr =
  selectTraceEventsDynamic tr @?= ([] :: [WorkflowEvent])

-- The zero-argument workflow runs and completes.
traceZeroNoInput :: forall a. SimTrace a -> IO ()
traceZeroNoInput tr = do
  selectTraceEventsDynamic tr @?= [WorkflowCompleted "sim-zero-id"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The body found its own row and the workflow completes.
traceRowBeforeBody :: forall a. SimTrace a -> IO ()
traceRowBeforeBody tr = do
  selectTraceEventsDynamic tr @?= [WorkflowCompleted "sim-row-id"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The parent records the child, the crash-and-relaunch recovers and
-- dequeues it (its step runs), and the replay adopts the id.
traceDerivedChildAdopted :: forall a. SimTrace a -> IO ()
traceDerivedChildAdopted tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowCompleted "sim-child-parent",
          StepRunning "double" 0,
          StepOutputRecorded "double" 0,
          WorkflowCompleted "sim-child-parent-0",
          WorkflowAlreadyOwned "sim-child-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The assigned child recovers under its chosen id; the replay adopts it.
traceAssignedChildAdopted :: forall a. SimTrace a -> IO ()
traceAssignedChildAdopted tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowCompleted "sim-assigned-parent",
          WorkflowCompleted "sim-assigned-parent-chosen",
          WorkflowAlreadyOwned "sim-assigned-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The plain-step refusal: the parent ends on the control end that names
-- the wanted start and the recorded plain step.
tracePlainStepAtStart :: forall a. SimTrace a -> IO ()
tracePlainStepAtStart tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowControlEnded "workflow sim-stale-parent step 0 was recorded as \"a plain step named child\", but \"a child workflow start of child\" was expected"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The root runs and completes.
traceRootNoParent :: forall a. SimTrace a -> IO ()
traceRootNoParent tr = do
  selectTraceEventsDynamic tr @?= [WorkflowCompleted "sim-root-id"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The three 400 ms children settle together, then the parent completes.
traceFanout :: forall a. SimTrace a -> IO ()
traceFanout tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowCompleted "sim-fanout-parent-0",
          WorkflowCompleted "sim-fanout-parent-1",
          WorkflowCompleted "sim-fanout-parent-2",
          WorkflowCompleted "sim-fanout-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The parent completes first; the detached child then runs its step and
-- settles.
traceUnawaitedChild :: forall a. SimTrace a -> IO ()
traceUnawaitedChild tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowCompleted "sim-unawaited-parent",
          StepRunning "double" 0,
          StepOutputRecorded "double" 0,
          WorkflowCompleted "sim-unawaited-parent-0"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The refused child fails, then the parent reports it and completes.
traceLiftChildError :: forall a. SimTrace a -> IO ()
traceLiftChildError tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowFailed "sim-lift-parent-0",
          WorkflowCompleted "sim-lift-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The refusal records no start row: the parent just fails.
traceChildInsideStepRefused :: forall a. SimTrace a -> IO ()
traceChildInsideStepRefused tr = do
  selectTraceEventsDynamic tr @?= [WorkflowFailed "sim-childleaf-parent"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The refusal records no start row: the parent just fails (through the
-- captured parent rather than the stepped context — same events).
traceCaptureChildRefused :: forall a. SimTrace a -> IO ()
traceCaptureChildRefused tr = do
  selectTraceEventsDynamic tr @?= [WorkflowFailed "sim-childleaf-captured-parent"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- Both the child and the parent are cancelled by the inherited deadline,
-- child first.
traceCascadeDeadline :: forall a. SimTrace a -> IO ()
traceCascadeDeadline tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowDeadlineCancelled "sim-cascade-deadline-parent-0",
          WorkflowDeadlineCancelled "sim-cascade-deadline-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The first (inheriting) child settles, then the declining one, then
-- the parent.
traceDeclinedDeadline :: forall a. SimTrace a -> IO ()
traceDeclinedDeadline tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowCompleted "sim-decline-deadline-parent-0",
          WorkflowCompleted "sim-decline-deadline-parent-2",
          WorkflowCompleted "sim-decline-deadline-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The child runs under its own budget and both complete.
traceChildBudgetWins :: forall a. SimTrace a -> IO ()
traceChildBudgetWins tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowCompleted "sim-child-budget-parent-0",
          WorkflowCompleted "sim-child-budget-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The child runs under the inherited deadline and both complete.
traceDeadlineInherited :: forall a. SimTrace a -> IO ()
traceDeadlineInherited tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowCompleted "sim-inherit-deadline-parent-0",
          WorkflowCompleted "sim-inherit-deadline-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The child's own budget cancels it, the parent's await records the
-- cancellation as its step error, and the parent fails.
traceCancelledChildAwaited :: forall a. SimTrace a -> IO ()
traceCancelledChildAwaited tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowDeadlineCancelled "sim-awaited-cancel-parent-0",
          WorkflowFailed "sim-awaited-cancel-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The fast step wins the race (id 1, behind the losing slow step at 0),
-- then the run completes.
traceLosingTokenFired :: forall a. SimTrace a -> IO ()
traceLosingTokenFired tr = do
  selectTraceEventsDynamic tr
    @?= [ StepOutputRecorded "fast" 1,
          WorkflowCompleted "sim-race-token-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The control signal ends the losing step and the run; the parent is
-- left PENDING, which its control end says.
traceControlSelect :: forall a. SimTrace a -> IO ()
traceControlSelect tr = do
  selectTraceEventsDynamic tr
    @?= [ StepControlEnded "interrupted" 0 1,
          WorkflowControlEnded "the workflow sim-race-control-parent was interrupted by shutdown and left PENDING"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The child settles, the select wins on the await arm, the parent
-- completes.
-- The converted body's run path: the scoped step runner announces the
-- run and its recorded output; the recorded sleep stays silent.
traceScopedBody :: forall a. SimTrace a -> IO ()
traceScopedBody tr = do
  selectTraceEventsDynamic tr @?= [StepRunning "double" 0, StepOutputRecorded "double" 0, WorkflowCompleted "sim-scoped-body-wf"]

-- The fast arm records under its branch id; the select's own position
-- records silently, and the loser leaves no trace.
traceScopedSelect :: forall a. SimTrace a -> IO ()
traceScopedSelect tr = do
  selectTraceEventsDynamic tr @?= [StepOutputRecorded "fast" 1]

traceSelectStepRaces :: forall a. SimTrace a -> IO ()
traceSelectStepRaces tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowCompleted "sim-race-parent-0",
          WorkflowCompleted "sim-race-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- Each child settles as its own pair awaits it: -0, -2, -4, parent.
traceStepIdPairs :: forall a. SimTrace a -> IO ()
traceStepIdPairs tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowCompleted "sim-pairs-parent-0",
          WorkflowCompleted "sim-pairs-parent-2",
          WorkflowCompleted "sim-pairs-parent-4",
          WorkflowCompleted "sim-pairs-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The three children settle in build order, then the parent completes.
traceChildIdsInBuildOrder :: forall a. SimTrace a -> IO ()
traceChildIdsInBuildOrder tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowCompleted "sim-order-parent-0",
          WorkflowCompleted "sim-order-parent-1",
          WorkflowCompleted "sim-order-parent-2",
          WorkflowCompleted "sim-order-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The child settles, then the enclosing step records its output, then
-- the parent completes.
traceAwaitInsideStep :: forall a. SimTrace a -> IO ()
traceAwaitInsideStep tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowCompleted "sim-await-step-parent-0",
          StepOutputRecorded "collect" 1,
          WorkflowCompleted "sim-await-step-parent"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- The child settles first; the parent then ends on the control end whose
-- detail names the awaited child and the recorded stranger.
traceStaleAwaitRefused :: forall a. SimTrace a -> IO ()
traceStaleAwaitRefused tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowCompleted "sim-await-wrong-0",
          WorkflowControlEnded "workflow sim-await-wrong step 1 was recorded as \"an await of somebody-elses-workflow\", but \"an await of sim-await-wrong-0\" was expected"
        ]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | The IOSim half of the departure wait: nothing to observe, because the
-- simulator advances time only when no thread is runnable — a parent
-- parked in a tick cannot resume before a self-terminating child has run
-- to completion, its departure commit included.
simWaitDeparture :: forall s. ThreadId (IOSim s) -> IOSim s ()
simWaitDeparture _ = threadDelay 1000
