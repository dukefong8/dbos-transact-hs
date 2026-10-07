{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.ManagementTest' mirrored over simulated data: the framed
-- cases run the same scenarios and checks as the live tree, with values
-- asserted here — including the 'IOSim' typed trace assertions, which stay
-- in this module. The memory backend ('MemSystemDB', fresh per case)
-- cancels, resumes, deletes, and retrieves rows for real, so the framed
-- in this module.
module DBOS.Transact.ManagementTestSim (tests) where

import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import Data.Aeson (FromJSON, ToJSON)
import DBOS.DualStack (simCase)
import DBOS.SystemDB
  ( SerializedWorkflowValue (..),
    WorkflowId (..),
    WorkflowStatus (..))
import DBOS.IOSimTracer (printSimTrace, runSimCase, simTracer)
import DBOS.Prelude
import DBOS.Transact
  (
    EngineOnly,
    CodecError,
    WorkflowCtx,
    DBOS,
    Executor,
    Error (..),
    WorkflowHandle,
    WorkflowKey,
    WorkflowRef,
    decodeWorkflowValue,
    handleResult,
    handleStatus,
    registerDBOSWorkflow,
    registerDBOSWorkflowRef,
    retrieveWorkflow,
    runDBOSWorkflow,
    runStep,
    runTracer,
    configNew)
import DBOS.Transact.Recovery (EngineEvent (..))
import DBOS.Transact.Management (ManagementEvent (..))
import DBOS.Transact.Step (WorkflowEvent (..))
import DBOS.Transact.Connection (SomeSystemDB (..))
import DBOS.SystemDB.IOSim (newMemDB, simEntropy, simGeneratedId, simIdentity)
import DBOS.Transact.ManagementCases
  ( MgmtFixture (..),
    checkCancelMissing,
    checkCancelResumeRun,
    checkCancelTree,
    checkDelete,
    checkForkFromBeginning,
    checkForkFromFailure,
    checkForkFromStep,
    checkForkTakesIdAndQueue,
    checkBulkCancelResume,
    checkBulkFork,
    checkForkPartitioned,
    checkAttributes,
    checkDelayRelease,
    checkResumeMissing,
    checkResumeOntoQueue,
    checkRetrieve,
    checkUnlaunched,
    mkMgmtFixture,
    scenarioCancelMissing,
    scenarioCancelResumeRun,
    scenarioCancelTree,
    scenarioDelete,
    scenarioForkFromBeginning,
    scenarioForkFromFailure,
    scenarioForkFromStep,
    scenarioForkTakesIdAndQueue,
    scenarioBulkCancelResume,
    scenarioBulkFork,
    scenarioForkPartitioned,
    scenarioAttributes,
    scenarioDelayRelease,
    scenarioResumeMissing,
    scenarioResumeOntoQueue,
    scenarioRetrieve,
    scenarioUnlaunched,
  )
import Test.Tasty (DependencyType (..), TestTree, dependentTestGroup)
import Test.Tasty.HUnit (testCase, (@?=))

-- | The sim half of the shared management fixture: a fresh 'MemSystemDB'
-- per case (so cases stay isolated) passed in as 'SomeSystemDB' with the
-- sim carrier as 'SomeTracer', over the same 'mkMgmtFixture' builder live
-- uses. Only the atoms differ.
simMgmtFixture :: forall s. IOSim s (MgmtFixture (IOSim s))
simMgmtFixture = do
  mem <- newMemDB
  ids <- newTVarIO 0
  entropy <- newTVarIO 0
  mkMgmtFixture
    (configNew "sim-app" "")
    simIdentity
    "sim-app"
    (simGeneratedId ids)
    (simEntropy entropy)
    (SomeSystemDB mem)
    simTracer
    (pure "sim")

tests :: TestTree
tests =
  -- Sequential: cases announce through one shared stderr, so parallel
  -- 'printSimTrace' calls would interleave their lines mid-character.
  -- 'AllFinish' keeps every case running on a failure, in order.
  dependentTestGroup
    "Workflow management (Sim)"
    AllFinish
    [ simCase simMgmtFixture "the management surface needs a launched instance" scenarioUnlaunched checkUnlaunched traceMgmtSilent,
      simCase simMgmtFixture "cancelling a workflow that does not exist is not an error" scenarioCancelMissing checkCancelMissing traceMgmtShutdown,
      simCase simMgmtFixture "resuming a workflow that does not exist is an error" scenarioResumeMissing checkResumeMissing traceMgmtShutdown,
      simCase simMgmtFixture "cancelling makes a workflow terminal and leaves it resumable" scenarioCancelResumeRun checkCancelResumeRun traceCancelResumeRun,
      simCase simMgmtFixture "resuming onto a named queue puts the workflow there" scenarioResumeOntoQueue checkResumeOntoQueue traceResumeOntoQueue,
      simCase simMgmtFixture "cancelling a tree reaches the children" scenarioCancelTree checkCancelTree traceCancelTree,
      simCase simMgmtFixture "deleting a workflow removes its row" scenarioDelete checkDelete traceDelete,
      simCase simMgmtFixture "a workflow can be retrieved by id" scenarioRetrieve checkRetrieve traceRetrieve,
      simCase simMgmtFixture "forking from the beginning runs the workflow again under a new id" scenarioForkFromBeginning checkForkFromBeginning traceForkFromBeginning,
      simCase simMgmtFixture "a fork takes the id it is given" scenarioForkTakesIdAndQueue checkForkTakesIdAndQueue traceForkTakesIdAndQueue,
      simCase simMgmtFixture "forking from a chosen step replays the steps below it" scenarioForkFromStep checkForkFromStep traceForkFromStep,
      simCase simMgmtFixture "forking from the last failure restarts at the failed step" scenarioForkFromFailure checkForkFromFailure traceForkFromFailure,
      simCase simMgmtFixture "bulk cancel and resume hand back every id" scenarioBulkCancelResume checkBulkCancelResume traceBulkCancelResume,
      simCase simMgmtFixture "bulk forking hands back one new id per source, in order" scenarioBulkFork checkBulkFork traceBulkFork,
      simCase simMgmtFixture "a fork onto a partitioned queue carries the key it is given" scenarioForkPartitioned checkForkPartitioned traceForkPartitioned,
      simCase simMgmtFixture "attributes are replaced and can be searched" scenarioAttributes checkAttributes traceAttributes,
      simCase simMgmtFixture "a delayed workflow can be released sooner" scenarioDelayRelease checkDelayRelease traceDelayRelease,
      testCase "management announces through its tracer" $ do
        (_, tr) <- runSimCase demoTrace
        printSimTrace tr
        selectTraceEventsDynamic tr
          @?= [ WorkflowsCancelled 2,
                WorkflowsResumed 3 2,
                WorkflowForked "sim-mgmt-fork-1",
                WorkflowsForked 0,
                WorkflowsDeleted 1,
                WorkflowDelayMoveAsked "sim-mgmt-delayed",
                WorkflowAttributesReplaceAsked "sim-mgmt-attributed"
              ]
    ]

-- | The silent management path: the unlaunched call announces nothing.
-- Launched leaves shut down, which announces; see 'traceMgmtShutdown'.
traceMgmtSilent :: SimTrace a -> IO ()
traceMgmtSilent tr = do
  selectTraceEventsDynamic tr @?= ([] :: [WorkflowEvent])
  selectTraceEventsDynamic tr @?= ([] :: [EngineEvent])
  selectTraceEventsDynamic tr @?= ([] :: [ManagementEvent])

-- | A launched leaf with no announcements of its own still shuts down.
traceMgmtShutdown :: SimTrace a -> IO ()
traceMgmtShutdown tr = do
  selectTraceEventsDynamic tr @?= ([] :: [WorkflowEvent])
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]
  selectTraceEventsDynamic tr @?= ([] :: [ManagementEvent])
-- | The cancel moves the queued run, the resume re-enqueues it, the driven
-- pass runs it, and the shutdown ends the run.
traceCancelResumeRun :: forall a. SimTrace a -> IO ()
traceCancelResumeRun tr = do
  selectTraceEventsDynamic tr @?= [WorkflowsCancelled 1, WorkflowsResumed 1 1]
  selectTraceEventsDynamic tr @?= [WorkflowEnqueued "hs-l2-mgmt-cancel-resume-sim" "no-runner-here", WorkflowCompleted "hs-l2-mgmt-cancel-resume-sim"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | Same shape: cancel, re-enqueue, run, shutdown.
traceResumeOntoQueue :: forall a. SimTrace a -> IO ()
traceResumeOntoQueue tr = do
  selectTraceEventsDynamic tr @?= [WorkflowsCancelled 1, WorkflowsResumed 1 1]
  selectTraceEventsDynamic tr @?= [WorkflowEnqueued "hs-l2-mgmt-resume-queue-sim" "no-runner-here", WorkflowCompleted "hs-l2-mgmt-resume-queue-sim"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | The parent runs and completes, then the tree cancel moves the parked
-- child alone (the finished parent is already terminal).
traceCancelTree :: forall a. SimTrace a -> IO ()
traceCancelTree tr = do
  selectTraceEventsDynamic tr @?= [WorkflowCompleted "hs-l2-mgmt-tree-parent-sim"]
  selectTraceEventsDynamic tr @?= [WorkflowsCancelled 1]
  selectTraceEventsDynamic tr @?= [EngineCancelledRunning 1, EngineShutdown "sim-app"]

-- | The stepped run records its step and completes; the delete is silent.
traceDelete :: forall a. SimTrace a -> IO ()
traceDelete tr = do
  selectTraceEventsDynamic tr @?= [StepRunning "work" 0, StepOutputRecorded "work" 0, WorkflowCompleted "hs-l2-mgmt-delete-sim"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | The stepless run completes; handle reads are silent.
traceRetrieve :: forall a. SimTrace a -> IO ()
traceRetrieve tr = do
  selectTraceEventsDynamic tr @?= [WorkflowCompleted "hs-l2-mgmt-retrieve-sim"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | The source fails, the fork is announced, the driven pass runs the fork to
-- success on the retry, and the shutdown ends the run.
traceForkFromBeginning :: forall a. SimTrace a -> IO ()
traceForkFromBeginning tr = do
  selectTraceEventsDynamic tr @?= [WorkflowFailed "hs-l2-mgmt-fork-src-sim", WorkflowCompleted "hs-l2-mgmt-fork-src-sim-fork"]
  selectTraceEventsDynamic tr @?= [WorkflowForked "hs-l2-mgmt-fork-src-sim-fork"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | The source runs, the chosen fork is announced, the driven pass runs it,
-- and the shutdown ends the run.
traceForkTakesIdAndQueue :: forall a. SimTrace a -> IO ()
traceForkTakesIdAndQueue tr = do
  selectTraceEventsDynamic tr @?= [WorkflowCompleted "hs-l2-mgmt-fork-placed-src-sim", WorkflowCompleted "hs-l2-mgmt-fork-placed-sim"]
  selectTraceEventsDynamic tr @?= [WorkflowForked "hs-l2-mgmt-fork-placed-sim"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | The source runs all three steps; the fork replays the step below the fork
-- point and re-runs the steps at and above it.
traceForkFromStep :: forall a. SimTrace a -> IO ()
traceForkFromStep tr = do
  selectTraceEventsDynamic tr
    @?= [ StepRunning "one" 0,
          StepOutputRecorded "one" 0,
          StepRunning "two" 1,
          StepOutputRecorded "two" 1,
          StepRunning "three" 2,
          StepOutputRecorded "three" 2,
          WorkflowCompleted "hs-l2-mgmt-fork-step-src-sim",
          StepReplaying "one" 0,
          StepRunning "two" 1,
          StepOutputRecorded "two" 1,
          StepRunning "three" 2,
          StepOutputRecorded "three" 2,
          WorkflowCompleted "hs-l2-mgmt-fork-step-src-sim-fork"
        ]
  selectTraceEventsDynamic tr @?= [WorkflowForked "hs-l2-mgmt-fork-step-src-sim-fork"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | The source fails before running step two; the fork restarts from the
-- beginning (no step recorded the failure, so the last step is the fork
-- point), re-runs both steps, and recovers.
traceForkFromFailure :: forall a. SimTrace a -> IO ()
traceForkFromFailure tr = do
  selectTraceEventsDynamic tr
    @?= [ StepRunning "one" 0,
          StepOutputRecorded "one" 0,
          WorkflowFailed "hs-l2-mgmt-fork-fail-src-sim",
          StepRunning "one" 0,
          StepOutputRecorded "one" 0,
          StepRunning "two" 1,
          StepOutputRecorded "two" 1,
          WorkflowCompleted "hs-l2-mgmt-fork-fail-src-sim-fork"
        ]
  selectTraceEventsDynamic tr @?= [WorkflowForked "hs-l2-mgmt-fork-fail-src-sim-fork"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | Two enqueues (silent: only the start path announces), a two-id cancel, a
-- two-id resume, and the shutdown.
traceBulkCancelResume :: forall a. SimTrace a -> IO ()
traceBulkCancelResume tr = do
  selectTraceEventsDynamic tr @?= ([] :: [WorkflowEvent])
  selectTraceEventsDynamic tr @?= [WorkflowsCancelled 2, WorkflowsResumed 2 2]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | Both sources run, one batch announcement names both forks, and both forks
-- complete in source order.
traceBulkFork :: forall a. SimTrace a -> IO ()
traceBulkFork tr = do
  selectTraceEventsDynamic tr
    @?= [ WorkflowCompleted "hs-l2-mgmt-bulk-fork-1-sim",
          WorkflowCompleted "hs-l2-mgmt-bulk-fork-2-sim",
          WorkflowCompleted "hs-l2-mgmt-bulk-fork-1-sim-fork",
          WorkflowCompleted "hs-l2-mgmt-bulk-fork-2-sim-fork"
        ]
  selectTraceEventsDynamic tr @?= [WorkflowsForked 2]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | The source runs, the single fork is announced, and the shutdown ends the
-- run. The fork itself never runs: the case asserts the row, not the run.
traceForkPartitioned :: forall a. SimTrace a -> IO ()
traceForkPartitioned tr = do
  selectTraceEventsDynamic tr @?= [WorkflowCompleted "hs-l2-mgmt-fork-key-src-sim"]
  selectTraceEventsDynamic tr @?= [WorkflowForked "hs-l2-mgmt-fork-key-src-sim-fork"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | The tagged run completes; updates and searches are silent.
traceAttributes :: forall a. SimTrace a -> IO ()
traceAttributes tr = do
  selectTraceEventsDynamic tr @?= [WorkflowCompleted "hs-l2-mgmt-attributes-sim"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | The delayed start is announced, the driven pass transitions and runs it.
traceDelayRelease :: forall a. SimTrace a -> IO ()
traceDelayRelease tr = do
  selectTraceEventsDynamic tr @?= [WorkflowEnqueued "hs-l2-mgmt-delayed-sim" "_dbos_internal_queue", WorkflowCompleted "hs-l2-mgmt-delayed-sim"]
  selectTraceEventsDynamic tr @?= [EngineShutdown "sim-app"]

-- | One of every management announcement, through the say-carrier: the
-- sim half of the live FastLogger lines.
demoTrace :: forall s. IOSim s ()
demoTrace = do
  runTracer simTracer (WorkflowsCancelled 2)
  runTracer simTracer (WorkflowsResumed 3 2)
  runTracer simTracer (WorkflowForked "sim-mgmt-fork-1")
  runTracer simTracer (WorkflowsForked 0)
  runTracer simTracer (WorkflowsDeleted 1)
  runTracer simTracer (WorkflowDelayMoveAsked "sim-mgmt-delayed")
  runTracer simTracer (WorkflowAttributesReplaceAsked "sim-mgmt-attributed")

-- | The sim case bodies, top-level so their rank-2 signatures can name
-- the simulation: captured state and refs arrive as parameters.
echoIntBody :: forall exec s. Int -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
echoIntBody input _ = pure (Right input)

forkableBody :: forall s. StrictTVar (IOSim s) Int -> forall exec. Int -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
forkableBody attempts _ _ = do
  attempt <- readTVarIO attempts
  atomically (modifyTVar attempts (+ 1))
  if attempt == 0
    then pure (Left (ErrorConfig "the first attempt fails"))
    else pure (Right 8)

stagedBody ran _ wctx = do
  outcomes <-
    traverse
      (\name -> runStep wctx name (const (atomically (modifyTVar ran (<> [name])) >> pure (0 :: Int))))
      ["one", "two", "three"]
  pure (fmap (const 0) (sequenceA outcomes))

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through. Local
-- copies are deliberate: this module carries only the aliases it uses.
runWfSim :: Executor (IOSim s) -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> IOSim s (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWfSim = runDBOSWorkflow

retrieveWfSim :: DBOS (IOSim s) -> WorkflowId -> IOSim s (Either (Error EngineOnly) (WorkflowHandle (IOSim s) EngineOnly))
retrieveWfSim = retrieveWorkflow

resultWfSim :: WorkflowHandle (IOSim s) EngineOnly -> IOSim s (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWfSim = handleResult

statusWfSim :: WorkflowHandle (IOSim s) EngineOnly -> IOSim s (Either (Error EngineOnly) (Maybe WorkflowStatus))
statusWfSim = handleStatus

-- | The sim-side registration aliases: a locally defined body has no
-- signature, so the channel's @e@ stays ambiguous; these pin it while
-- leaving @s@ universally quantified.
registerWfSim :: (FromJSON a, ToJSON r) => DBOS (IOSim s) -> WorkflowKey -> (forall exec. a -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) r)) -> IOSim s (Either (Error EngineOnly) ())
registerWfSim = registerDBOSWorkflow

registerWfRefSim :: (FromJSON a, ToJSON r) => DBOS (IOSim s) -> WorkflowKey -> (forall exec. a -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) r)) -> IOSim s (Either (Error EngineOnly) (WorkflowRef (IOSim s) EngineOnly))
registerWfRefSim = registerDBOSWorkflowRef

-- * Helpers


-- | Register an @Int -> Int@ body under IOSim, pinning the JSON types the
-- polymorphic registration cannot infer from a local binding.
registerIntRef :: DBOS (IOSim s) -> WorkflowKey -> (forall exec. Int -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)) -> IOSim s (WorkflowRef (IOSim s) EngineOnly)
registerIntRef dbos key body = orFail =<< registerWfRefSim dbos key body

registerIntWorkflow :: DBOS (IOSim s) -> WorkflowKey -> (forall exec. Int -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)) -> IOSim s ()
registerIntWorkflow dbos key body = orFail =<< registerWfSim dbos key body

orFail :: Either (Error EngineOnly) a -> IOSim s a
orFail result = case result of
  Left err -> throwIO (userError (show err))
  Right value -> pure value

decodeChildId :: SerializedWorkflowValue -> Either CodecError WorkflowId
decodeChildId output = WorkflowId <$> decodeWorkflowValue "result" (Just output)
