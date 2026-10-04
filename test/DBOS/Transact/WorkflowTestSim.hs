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
import Data.Aeson (FromJSON (..), ToJSON (..), Value (..), object)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.IOSimTracer (runSimCase, simTracer)
import DBOS.Prelude
import DBOS.SystemDB (AwaitedOutcome (..), Outcome (..), StepRecord (..), Timestamp (..), WorkflowId (..), WorkflowRecord (..), WorkflowStatus (..), addTimeout, defaultWorkflowFilter, getWorkflow, listWorkflowSteps)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.IOSim (memLaunchOn, newMemDB, simEntropy, simGeneratedId, simIdentity, simInstance)
import DBOS.Transact (CodecError, Ctx, DBOS, Executor, DuplicationPolicy (..), EngineEvent (..), EngineOnly, Enqueue (..), Error (..), Provenance (..), RunOptions (..), SelectArm (..), Serialization (..), SerializedWorkflowValue (..), SomeSystemDB (..), StartOptions (..), Timeout (..), WorkflowEvent (..), WorkflowHandle (..), WorkflowKey, WorkflowRef, application, awaitChild, cancellationToken, childWorkflowId, configNew, decodeErrorText, decodeWorkflowValue, encodeWorkflowValue, enqueueNew, firstStepStatus, handleResult, handleStatus, handleWorkflowId, millisDuration, newWorkflowKey, nextStepMarker, pendingAwait, pendingWorkflowStepWith, registerDBOSWorkflow, registerDBOSWorkflowRef, resolveTimeoutDeadline, retrieveWorkflow, runDBOSWorkflow, runDBOSWorkflowRef, runOptionsDefault, runOptionsToStartOptions, runTracer, runWorkflowStep, runWorkflowStepWith, secondsDuration, selectStep, shutdown, startChildWorkflow, startDBOSWorkflowRef, startOptionsDefault, stepOptionsDefault, timeoutBudget, tokenCancelled, waitForWorkflow, withAttempt, withSystemDB, workflowId)
import DBOS.Transact.WorkflowTest
  ( JoinOutcome (..),
    WfFixture (..),
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
    checkStepErrorRecorded,
    checkStepsTaken,
    checkWrongInstance,
    checkDeclinedDeadline,
    checkFanout,
    checkFreshJoinPolls,
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
    checkScopedSelect,
    checkSelectStepRaces,
    checkStaleAwaitRefused,
    checkStepIdPairs,
    checkUnawaitedChild,
    checkZeroNoInput,
    mkWfFixture,
    scenarioAppErrorRoundtrip,
    scenarioAwaitInsideStep,
    scenarioAwaitRecorded,
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
    scenarioStepErrorRecorded,
    scenarioStepsTaken,
    scenarioWrongInstance,
    scenarioDeclinedDeadline,
    scenarioFanout,
    scenarioFreshJoinPolls,
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
    scenarioScopedSelect,
    scenarioSelectStepRaces,
    scenarioStaleAwaitRefused,
    scenarioStepIdPairs,
    scenarioUnawaitedChild,
    scenarioZeroNoInput,
    simWaitDeparture,
    taskAbortAllWaits,
    taskEarlyFinishNotSwept,
    taskEmptySweep,
    taskFinishedNotRegistered,
    taskRefusedAfterSweep
  )
import Test.Tasty (DependencyType (..), TestTree, dependentTestGroup, testGroup)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

tests :: TestTree
tests =
  -- Sequential: cases announce through one shared stderr, so parallel
  -- 'printSimTrace' calls would interleave their lines mid-character.
  -- 'AllFinish' keeps every case running on a failure, in order.
  dependentTestGroup
    "Workflow execution (Sim)"
    AllFinish
    [ simCase "a registered workflow starts and records its result" scenarioRegisteredRecordsResult checkRegisteredResult traceRegisteredResult,
      -- IO only: crash-and-relaunch recovery sweep (MemSystemDB delegates
      -- reenqueueForRecovery to the canned mock; see ADR-0020).
      testCase "a recovery run replays completed steps after a body interruption" (pure ()),
      -- IO only: same recovery sweep as above.
      testCase "an unregistered workflow is skipped and the rest recover" (pure ()),
      testCase "timeouts, options, and child ids compose without a database" $ do
        let budget = secondsDuration 60
            now = Timestamp 1000
        timeoutBudget Inherit @?= Nothing
        timeoutBudget None @?= Nothing
        timeoutBudget (Explicit budget) @?= Just budget
        resolveTimeoutDeadline (Explicit budget) (Just (enqueueNew "q")) Nothing now @?= Nothing
        resolveTimeoutDeadline (Explicit budget) Nothing Nothing now @?= addTimeout now budget
        resolveTimeoutDeadline None Nothing (Just now) now @?= Nothing
        resolveTimeoutDeadline Inherit Nothing (Just now) now @?= Just now
        resolveTimeoutDeadline Inherit Nothing Nothing now @?= Nothing
        runOptionsDefault @?= RunOptions Nothing Inherit Nothing
        startOptionsDefault @?= StartOptions Nothing Inherit Nothing Nothing
        runOptionsToStartOptions runOptionsDefault @?= startOptionsDefault
        childWorkflowId (Just "chosen") (Just ("parent", 3)) "generated" @?= "chosen"
        childWorkflowId Nothing (Just ("parent", 0)) "generated" @?= "parent-0"
        childWorkflowId Nothing (Just ("parent", 2)) "generated" @?= "parent-2"
        childWorkflowId Nothing Nothing "generated" @?= "generated",
      simCase "starting a taken id joins the existing run" scenarioJoinTakesId checkJoinTakesId traceJoinTakesId,
      simCase "a fresh start is local and a join polls" scenarioFreshJoinPolls checkFreshJoinPolls traceFreshJoinPolls,
      simCase "awaiting a child is recorded as a step" scenarioAwaitRecorded checkAwaitRecorded traceAwaitRecorded,
      simCase "a recorded await of another workflow is refused" scenarioStaleAwaitRefused checkStaleAwaitRefused traceStaleAwaitRefused,
      simCase "awaiting a child inside a step is covered by that step" scenarioAwaitInsideStep checkAwaitInsideStep traceAwaitInsideStep,
      simCase "child starts and awaits keep their ids in build order" scenarioChildIdsInBuildOrder checkChildIdsInBuildOrder traceChildIdsInBuildOrder,
      simCase "runs claim their pairs of step ids adjacently" scenarioStepIdPairs checkStepIdPairs traceStepIdPairs,
      simCase "a select step races a step against a child's result" scenarioSelectStepRaces checkSelectStepRaces traceSelectStepRaces,
      simCase "a scoped select races two pending steps" scenarioScopedSelect checkScopedSelect traceScopedSelect,
      simCase "a control signal winning a select records no winner" scenarioControlSelect checkControlSelect traceControlSelect,
      simCase "a losing step has its cancellation token fired" scenarioLosingTokenFired checkLosingTokenFired traceLosingTokenFired,
      simCase "a cancelled child is an awaited cancellation in the parent" scenarioCancelledChildAwaited checkCancelledChildAwaited traceCancelledChildAwaited,
      -- IO only: recorded-await replay across two launches (needs the
      -- recovery sweep).
      testCase "a replayed parent reads the recorded outcome rather than waiting again" (pure ()),
      simCase "a child inherits its parent's deadline" scenarioDeadlineInherited checkDeadlineInherited traceDeadlineInherited,
      simCase "a child's own timeout replaces the inherited deadline" scenarioChildBudgetWins checkChildBudgetWins traceChildBudgetWins,
      simCase "a child can decline the inherited deadline" scenarioDeclinedDeadline checkDeclinedDeadline traceDeclinedDeadline,
      simCase "a parent and its child hit an inherited deadline independently" scenarioCascadeDeadline checkCascadeDeadline traceCascadeDeadline,
      simCase "a parent starts a child under a derived id and replay adopts it" scenarioDerivedChildAdopted checkDerivedChildAdopted traceDerivedChildAdopted,
      simCase "starting a child inside a step is refused, not recorded" scenarioChildInsideStepRefused checkChildInsideStepRefused traceChildInsideStepRefused,
      simCase "starting a child through a captured parent is refused, not recorded" scenarioCaptureChildRefused checkCaptureChildRefused traceCaptureChildRefused,
      simCase "a child that fails differently is started through lift" scenarioLiftChildError checkLiftChildError traceLiftChildError,
      -- IO only: the body performs real IO (the foreign charge call),
      -- which the simulator cannot run.
      testCase "a foreign error is converted at the boundary" (pure ()),
      simCase "a child started and never awaited is still recorded" scenarioUnawaitedChild checkUnawaitedChild traceUnawaitedChild,
      simCase "children started in a loop run concurrently" scenarioFanout checkFanout traceFanout,
      -- IO only: first-to-settle timing is wall-clock-bound.
      testCase "select reports the first workflow to settle, not the first started" (pure ()),
      simCase "an assigned child id wins over the derived one" scenarioAssignedChildAdopted checkAssignedChildAdopted traceAssignedChildAdopted,
      simCase "a workflow started outside a workflow has no parent" scenarioRootNoParent checkRootNoParent traceRootNoParent,
      simCase "a start position holding a plain step is refused" scenarioPlainStepAtStart checkPlainStepAtStart tracePlainStepAtStart,
      simCase "a child started through another instance is refused" scenarioWrongInstance checkWrongInstance traceWrongInstance,
      testCase "a child joining a held key is recorded as the workflow it joined" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "joiner"
              queueName = "sim-join-q"
              dedupKey = "order-42"
              holderText = "sim-join-holder"
              parentText = "sim-join-parent"
              derivedText = parentText <> "-0"
              joinQueue =
                (enqueueNew queueName)
                  { deduplication_id = Just dedupKey,
                    duplication_policy = ReturnExisting
                  }
              childBody :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody () _ = pure (Right 9)
          childRef <- registerUnitRef dbos childKey childBody
          let parentBody () ctx = do
                started <- startChildWorkflow ctx childRef (startOptionsDefault {startQueue = Just joinQueue}) Nothing
                case started of
                  Left err -> pure (Left err)
                  Right handle -> do
                    result <- awaitWfSim ctx handle
                    case result of
                      Left err -> pure (Left err)
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right n -> pure (Right n)
                          Left _  -> pure (Left (StepFailed "parent" "bad child output"))
                      Right _ -> pure (Left (StepFailed "parent" "no child output"))
          orFail =<< registerWfSim dbos parentKey parentBody
          exec <- memLaunchOn mem simTracer dbos
          -- The holder parks on its queue with the key held; nothing runs
          -- it here, so the test stages what the queue runner would do and
          -- records its completion directly. (Live parks it on a delay
          -- instead and the supervisor runs it; the asserted join is the
          -- same.)
          holderStarted <-
            startWfRefSim
              exec
              childRef
              (startOptionsDefault {startWorkflowId = Just holderText, startQueue = Just (enqueueNew queueName) {deduplication_id = Just dedupKey}})
              Nothing
          case holderStarted of
            Left err -> throwIO (userError (show err))
            Right _  -> pure ()
          orFailSys =<< SystemDB.recordWorkflowOutcome mem (WorkflowId holderText) (OutcomeOutput (Just "9"))
          outcome <- runWfSim exec parentKey (WorkflowId parentText) Nothing
          derived <- getWorkflow mem (WorkflowId derivedText)
          listed <- SystemDB.listWorkflowSteps mem (WorkflowId parentText) False Nothing Nothing Nothing
          children <- SystemDB.getWorkflowChildren mem (WorkflowId parentText)
          pure (outcome, derived, listed, children)
        case outcome of
          (outcome, derived, listed, children) -> do
            case outcome of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "the parent reads the joined workflow's output" (Right 9) decoded
              other -> fail ("expected the joined output, got: " <> show other)
            derived @?= Right Nothing
            case listed of
              Right
                [ StepRecord {stepRecordStepName = startName, stepRecordChildWorkflowId = Just (WorkflowId startedChild)},
                  StepRecord
                    { stepRecordStepName = awaitName,
                      stepRecordOutput = Just awaitOutput,
                      stepRecordChildWorkflowId = Just (WorkflowId awaitedChild)
                    }
                  ] -> do
                  startName @?= "child"
                  startedChild @?= "sim-join-holder"
                  awaitName @?= "DBOS.getResult"
                  awaitOutput @?= "9"
                  awaitedChild @?= "sim-join-holder"
              other -> fail ("expected the joining start and its recorded await, got: " <> show other)
            children @?= Right [],
      simCase "a zero-argument workflow records no input" scenarioZeroNoInput checkZeroNoInput traceZeroNoInput,
      simCase "the row exists before the body starts" scenarioRowBeforeBody checkRowBeforeBody traceRowBeforeBody,
      simCase "a panicking workflow leaves its row pending" scenarioPanic checkPanic tracePanic,
      simCase "retrieving before launch is refused" scenarioRetrieveBeforeLaunch checkRunBeforeLaunch traceRunBeforeLaunch,
      simCase "an application error round-trips as itself" scenarioAppErrorRoundtrip checkAppErrorRoundtrip traceAppErrorRoundtrip,
      simCase "a database failure is not the workflow outcome" scenarioDbFailureNotOutcome checkDbFailureNotOutcome traceDbFailureNotOutcome,
      simCase "a workflow records the steps it took" scenarioStepsTaken checkStepsTaken traceStepsTaken,
      simCase "shutdown cancels a running workflow and leaves it pending" scenarioShutdownCancels checkShutdownCancels traceShutdownCancels,
      simCase "dropping the future does not stop the workflow" scenarioDropFuture checkDropFuture traceDropFuture,
      -- Sim only: not yet mirrored on IO (needs wall-clock
      -- budget/body scaling).
      testCase "a budget cancels the workflow durably" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let key = newWorkflowKey "slow"
              workflowText = "sim-budget-id"
              body :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              body () _ = threadDelay 1000000 >> pure (Right 7)
          ref <- registerUnitRef dbos key body
          exec <- memLaunchOn mem simTracer dbos
          -- A millisecond budget against a second-long body: the clock
          -- wins on virtual time, deterministically.
          ran <-
            runWfRefSim
              exec
              ref
              (runOptionsDefault {runWorkflowId = Just workflowText, runTimeout = Explicit (millisDuration 1)})
              Nothing
          row <- getWorkflow mem (WorkflowId workflowText)
          pure (ran, row)
        case outcome of
          (ran, row) -> do
            case ran of
              Left (ErrorSystemDatabase (SystemDB.WorkflowCancelled {})) -> pure ()
              other                                                      -> fail ("expected the durable cancellation, got: " <> show other)
            case row of
              Right (Just found) -> found.workflowRecordStatus @?= Cancelled
              other              -> fail ("expected the row CANCELLED, got: " <> show other),
      simCase "a started workflow carries the attributes it was given" scenarioAttributes checkAttributes traceAttributes,
      simCase "a step error is recorded in its column" scenarioStepErrorRecorded checkStepErrorRecorded traceStepErrorRecorded,
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
      testCase "a task finishing before registration is not swept as aborted" $ do
        let simRun :: forall s. IOSim s [Int]
            simRun = taskEarlyFinishNotSwept @(IOSim s) simWaitDeparture
        checkNoMiscounts (runSimOrThrow simRun),
      -- IO only: real preemption, not cooperation.
      testCase "a spawn refused after abort fills its channel instead of hanging" (pure ())
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

-- | One sim leaf: build the sim fixture, drive the shared scenario
-- through @runSimCase@, judge the value by the shared check and the
-- trace by typed event assertions. Nothing prints: the watcher stays
-- quiet and the trace speaks through types, not lines. The mirror of
-- 'liveCase': same scenario, same value check, sim runner plus events.
simCase ::
  String ->
  (forall s. WfFixture (IOSim s) -> IOSim s a) ->
  (a -> Either String ()) ->
  (forall x. SimTrace x -> IO ()) ->
  TestTree
simCase name scen check traceCheck = testCase name $ do
  (out, tr) <- runSimCase (simWfFixture >>= scen)
  either fail pure (check out)
  traceCheck tr

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

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through. Local
-- copies are deliberate: this module carries only the aliases it uses.
runWfSim :: Executor (IOSim s) -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> IOSim s (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWfSim = runDBOSWorkflow

runWfRefSim :: Executor (IOSim s) -> WorkflowRef (IOSim s) EngineOnly -> RunOptions -> Maybe SerializedWorkflowValue -> IOSim s (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWfRefSim = runDBOSWorkflowRef

startWfRefSim :: Executor (IOSim s) -> WorkflowRef (IOSim s) EngineOnly -> StartOptions -> Maybe SerializedWorkflowValue -> IOSim s (Either (Error EngineOnly) (WorkflowHandle (IOSim s) EngineOnly))
startWfRefSim = startDBOSWorkflowRef

retrieveWfSim :: DBOS (IOSim s) -> WorkflowId -> IOSim s (Either (Error EngineOnly) (WorkflowHandle (IOSim s) EngineOnly))
retrieveWfSim = retrieveWorkflow

awaitWfSim :: Ctx (IOSim s) -> WorkflowHandle (IOSim s) EngineOnly -> IOSim s (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
awaitWfSim = awaitChild

resultWfSim :: WorkflowHandle (IOSim s) EngineOnly -> IOSim s (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWfSim = handleResult

statusWfSim :: WorkflowHandle (IOSim s) EngineOnly -> IOSim s (Either (Error EngineOnly) (Maybe WorkflowStatus))
statusWfSim = handleStatus

-- | The sim-side registration aliases: a locally defined body has no
-- signature, so the channel's @e@ stays ambiguous; these pin it while
-- leaving @s@ universally quantified.
registerWfSim :: (FromJSON a, ToJSON r) => DBOS (IOSim s) -> WorkflowKey -> (a -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) r)) -> IOSim s (Either (Error EngineOnly) ())
registerWfSim = registerDBOSWorkflow

registerWfRefSim :: (FromJSON a, ToJSON r) => DBOS (IOSim s) -> WorkflowKey -> (a -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) r)) -> IOSim s (Either (Error EngineOnly) (WorkflowRef (IOSim s) EngineOnly))
registerWfRefSim = registerDBOSWorkflowRef

-- * Helpers

-- | A started reader for inspecting a row's status: polls until the row
-- appears, so background starts are observed rather than raced. Virtual
-- time makes the settling instant.
waitForRow :: DBOS (IOSim s) -> WorkflowId -> IOSim s WorkflowStatus
waitForRow dbos wid = go (20 :: Int)
  where
    go 0 = throwIO (userError "the workflow row never appeared")
    go n = do
      retrieved <- retrieveWfSim dbos wid
      case retrieved of
        Left err -> throwIO (userError (show err))
        Right handle -> do
          status <- statusWfSim handle
          case status of
            Left err           -> throwIO (userError (show err))
            Right (Just found) -> pure found
            Right Nothing      -> threadDelay 1000 >> go (n - 1)

-- | Register a @() -> Int@ body under IOSim, pinning the JSON types the
-- polymorphic registration cannot infer from a local binding.
registerUnitRef :: DBOS (IOSim s) -> WorkflowKey -> (() -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)) -> IOSim s (WorkflowRef (IOSim s) EngineOnly)
registerUnitRef dbos key body = orFail =<< registerWfRefSim dbos key body

-- | Register an @Int -> Int@ body under IOSim, pinning the JSON types the
-- polymorphic registration cannot infer from a local binding.
registerIntRef :: DBOS (IOSim s) -> WorkflowKey -> (Int -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)) -> IOSim s (WorkflowRef (IOSim s) EngineOnly)
registerIntRef dbos key body = orFail =<< registerWfRefSim dbos key body

-- | Register a body at its own error channel, leaving @s@ and @e@ to the
-- call site: the polymorphic registration cannot infer them from a local
-- binding, and a locally written channel is the point of the lift case.
registerRefOf :: forall e s a r. (FromJSON a, ToJSON r, ToJSON e) => DBOS (IOSim s) -> WorkflowKey -> (a -> Ctx (IOSim s) -> IOSim s (Either (Error e) r)) -> IOSim s (Either (Error EngineOnly) (WorkflowRef (IOSim s) e))
registerRefOf = registerDBOSWorkflowRef

orFail :: Either (Error EngineOnly) a -> IOSim s a
orFail result = case result of
  Left err    -> throwIO (userError (show err))
  Right value -> pure value

orFailSys :: Either SystemDB.Error a -> IOSim s a
orFailSys result = case result of
  Left err    -> throwIO (userError (show err))
  Right value -> pure value

data Refused = Refused
  deriving stock (Eq, Show)

instance ToJSON Refused where
  toJSON _ = object []

instance FromJSON Refused where
  parseJSON _ = pure Refused

data GaveUp = GaveUp
  deriving stock (Eq, Show)

instance ToJSON GaveUp where
  toJSON _ = object []

instance FromJSON GaveUp where
  parseJSON _ = pure GaveUp
