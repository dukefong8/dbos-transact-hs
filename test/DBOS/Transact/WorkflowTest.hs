{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Workflow execution behavior through the class-backed engine runner.
module DBOS.Transact.WorkflowTest
  ( tests,
    WfFixture (..),
    scenarioRegisteredRecordsResult,
    liveWfFixture,
    JoinOutcome (..),
    scenarioJoinTakesId,
    scenarioFreshJoinPolls,
    scenarioAwaitRecorded,
    scenarioStaleAwaitRefused,
    scenarioAwaitInsideStep,
    scenarioChildIdsInBuildOrder,
    scenarioStepIdPairs,
    scenarioScopedBody,
    scenarioScopedSelect,
    scenarioSelectStepRaces,
    scenarioControlSelect,
    scenarioLosingTokenFired,
    scenarioCancelledChildAwaited,
    scenarioDeadlineInherited,
    scenarioChildBudgetWins,
    scenarioDeclinedDeadline,
    scenarioCascadeDeadline,
    scenarioCaptureChildRefused,
    scenarioChildInsideStepRefused,
    scenarioLiftChildError,
    scenarioUnawaitedChild,
    scenarioFanout,
    scenarioRootNoParent,
    scenarioPlainStepAtStart,
    scenarioDerivedChildAdopted,
    scenarioAssignedChildAdopted,
    scenarioZeroNoInput,
    scenarioRowBeforeBody,
    scenarioPanic,
    scenarioRetrieveBeforeLaunch,
    scenarioAppErrorRoundtrip,
    scenarioDbFailureNotOutcome,
    scenarioStepsTaken,
    scenarioShutdownCancels,
    scenarioDropFuture,
    scenarioAttributes,
    scenarioStepErrorRecorded,
    scenarioWrongInstance,
    waitForRowShared,
    checkRegisteredResult,
    checkJoinTakesId,
    checkFreshJoinPolls,
    checkAwaitRecorded,
    checkStaleAwaitRefused,
    checkAwaitInsideStep,
    checkChildIdsInBuildOrder,
    checkStepIdPairs,
    checkScopedBody,
    checkScopedSelect,
    checkSelectStepRaces,
    checkControlSelect,
    checkLosingTokenFired,
    checkCancelledChildAwaited,
    checkDeadlineInherited,
    checkChildBudgetWins,
    checkDeclinedDeadline,
    checkCascadeDeadline,
    checkCaptureChildRefused,
    checkChildInsideStepRefused,
    checkLiftChildError,
    checkUnawaitedChild,
    checkFanout,
    checkRootNoParent,
    checkPlainStepAtStart,
    checkDerivedChildAdopted,
    checkAssignedChildAdopted,
    checkZeroNoInput,
    checkRowBeforeBody,
    checkPanic,
    checkRunBeforeLaunch,
    checkAppErrorRoundtrip,
    checkDbFailureNotOutcome,
    checkStepsTaken,
    checkShutdownCancels,
    checkDropFuture,
    checkAttributes,
    checkStepErrorRecorded,
    checkWrongInstance,
    liveCase,
    TaskCase,
    taskAbortAllWaits,
    taskFinishedNotRegistered,
    taskRefusedAfterSweep,
    taskEmptySweep,
    taskEarlyFinishNotSwept,
    simWaitDeparture,
    checkNoMiscounts,
    mkWfFixture,
  )
where

import DBOS.Prelude
import Control.Concurrent.Class.MonadSTM.Strict (atomically, newTVarIO, readTVarIO, writeTVar)
import Control.Monad.Class.MonadTimer (threadDelay)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.IOSim (IOSim)
import Data.Aeson (FromJSON (..), ToJSON (..), Value (..), object, withObject, (.:), (.=))
import Data.Int (Int64)
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Data.Word (Word32)
import DBOS.SystemDB (AwaitedOutcome (..), NewWorkflow (..), StepRecord (..), Submission (..), WorkflowId (..), WorkflowRecord (..), Timestamp (..), addTimeout, getWorkflow, listWorkflowSteps, newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  (
    application,
    decodeErrorText,
    EngineOnly, CodecError,
    Config (..),
    Environment (..),
    Error (..),
    RunOptions (..),
    Serialization (..),
    Serializer (..),
    SelectArm (..),
    SerializedWorkflowValue (..),
    StartOptions (..),
    Provenance (..),
    WorkflowHandle (..),
    Timeout (..),
    DBOS,
    WorkflowCtx,
    stepCtxCancellationToken,
    Executor,
    Enqueue (..),
    Identity (..),
    DuplicationPolicy (..),
    QueueConflict (..),
    SomeTracer (..),
    WorkflowStatus (..),
    acquireLoggerBackend,
    awaitChild,
    defaultQueueOptions,
    firstStepStatus,
    configFromEnv,
    decodeWorkflowValue,
    encodeWorkflowValue,
    enqueueNew,
    handleResult,
    handleStatus,
    ioTracer,
    launchOn,
    launchWithEnvironment,
    newDBOS,
    newWorkflowKey,
    registerDBOSWorkflowRef,
    registerDBOSWorkflow,
    WorkflowKey,
    WorkflowRef,
    registerQueue,
    resolveTimeoutDeadline,
    retrieveWorkflow,
    runDBOSWorkflow,
    runDBOSWorkflowRef,
    nextWorkflowMarker,
    runOptionsDefault,
    runOptionsToStartOptions,
    nullTracer,
    pendingAwait,
    pendingWorkflowStep,
    pendingWorkflowStepWith,
    runWorkflowStep,
    runWorkflowStepWith,
    sleepWorkflowStep,
    selectWorkflow,
    startChildWorkflow,
    millisDuration,
    secondsDuration,
    selectStep,
    shutdown,
    startDBOSWorkflowRef,
    startOptionsDefault,
    stepOptionsDefault,
    withStep,
    withWorkflow,
    timeoutBudget,
    waitForWorkflow,
    awaitChild,
    pendingAwait,
  )
import DBOS.Transact.Context
  ( withSystemDB,
    spawnLocal,
    tokenCancelled,
    workflowId
  )
import DBOS.Transact.Workflow (abortAll, childWorkflowId, newTasks, spawnTracked, tasksSpawner)
import DBOS.Transact.Connection
  ( Connection,
    Owner (..),
    SomeSystemDB (..),
    newConnection,
    uuidWorkflowId,
    runSystemDB
  )
import DBOS.SystemDB.Retry (uuidEntropy)
import DBOS.Transact.ContextTest (ctxOver)
import GHC.Conc (ThreadStatus (..), threadStatus)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
  withResource acquireLoggerBackend snd $ \getLogger ->
  testGroup
    "Workflow execution"
    [ liveCase getBackend (ioTracer . fst <$> getLogger) "a registered workflow starts and records its result" scenarioRegisteredRecordsResult checkRegisteredResult,
      -- IO only: crash-and-relaunch recovery sweep (MemSystemDB delegates
      -- reenqueueForRecovery to the canned mock; see ADR-0020).
      testCase "a recovery run replays completed steps after a body interruption" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-recovery-" <> Text.take 12 suffix
            appVersion = "hs-l2-recovery-version-" <> suffix
            executorId = "hs-l2-recovery-executor-" <> suffix
            workflowText = "hs-l2-recovery-workflow-" <> suffix
            workflowId = WorkflowId workflowText
            key = newWorkflowKey "recover"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
        shouldCrash <- newIORef True
        bodyCalls <- newIORef (0 :: Int)
        let body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = do
              completedStep <- runWorkflowStep wctx "once" (const (modifyIORef' bodyCalls (+ 1) >> pure (value * 2)))
              case completedStep of
                Left err -> pure (Left err)
                Right result -> do
                  crash <- readIORef shouldCrash
                  if crash
                    then ioError (userError "interrupted after checkpoint")
                    else pure (Right result)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchExec dbos isolatedEnvironment
          first <- try (runWf exec key workflowId (Just (encodeWorkflowValue (21 :: Int)))) :: IO (Either SomeException (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
          _ <- case first of
            Left exception -> assertBool "body interruption escapes without a workflow outcome" ("interrupted after checkpoint" `Text.isInfixOf` Text.pack (show exception))
            Right result -> fail (show result)
          assertEqual "the step ran before interruption" 1 =<< readIORef bodyCalls
          shutdown dbos
          writeIORef shouldCrash False
          _ <- launchExec dbos isolatedEnvironment
          settled <- timeout 10000000 (waitForWorkflow dbos workflowId)
          case settled of
            Just (Right (AwaitedSucceeded (Just output) serialization)) -> do
              let stored = SerializedWorkflowValue output (Serialization <$> serialization)
                  decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "recovery completes the same workflow" (Right 42) decoded
            other -> fail (show other)
          assertEqual "the replay adopts the recorded step" 1 =<< readIORef bodyCalls
          adopted <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          case adopted of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "a duplicate run awaits and adopts the stored result" (Right 42) decoded
            other -> fail (show other),
      -- IO only: same recovery sweep as above.
      testCase "an unregistered workflow is skipped and the rest recover" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-skip-" <> Text.take 12 suffix
            appVersion = "hs-l2-skip-version-" <> suffix
            -- One executor identity across both instances, as the oracle's
            -- default executor id gives both of its objects: recovery sweeps
            -- rows stamped with this executor's own id.
            execId = "hs-l2-skip-exec-" <> suffix
            firstExec = execId
            secondExec = execId
            ghostText = "hs-l2-skip-ghost-" <> suffix
            keeperText = "hs-l2-skip-keeper-" <> suffix
            ghostKey = newWorkflowKey "ghost"
            keeperKey = newWorkflowKey "keeper"
        gate <- newEmptyMVar
        enteredGhost <- newEmptyMVar
        enteredKeeper <- newEmptyMVar
        config0 <- configFromEnv appName
        let firstConfig = config0 {configAppVersion = Just appVersion, configExecutorId = Just firstExec}
            gated :: StrictMVar IO () -> forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
            gated entered () _ = putMVar entered () >> takeMVar gate >> pure (Right ())
        bracket (newDBOS firstConfig) shutdown $ \first -> do
          ghostRegistered <- registerDBOSWorkflowRef first ghostKey (gated enteredGhost)
          ghostRef <- case ghostRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          keeperRegistered <- registerDBOSWorkflowRef first keeperKey (gated enteredKeeper)
          keeperRef <- case keeperRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          execFirst <- launchExec first isolatedEnvironment
          -- Ghost first, so the sweep meets the skip before the recovery.
          ghostWorker <- async (runWfRef execFirst ghostRef (runOptionsDefault {runWorkflowId = Just ghostText}) Nothing)
          keeperWorker <- async (runWfRef execFirst keeperRef (runOptionsDefault {runWorkflowId = Just keeperText}) Nothing)
          entered <- timeout 15000000 (takeMVar enteredGhost >> takeMVar enteredKeeper)
          case entered of
            Nothing -> fail "the abandoned runs never started"
            Just _ -> pure ()
          -- Abandoned mid-run: both block at the gate until shutdown.
          shutdown first
          cancel ghostWorker
          cancel keeperWorker
        -- A new instance on the same database, with ghost's code removed.
        let secondConfig = config0 {configAppVersion = Just appVersion, configExecutorId = Just secondExec}
        bracket (newDBOS secondConfig) shutdown $ \second -> do
          keeperRegistered <- registerDBOSWorkflowRef second keeperKey (\() _ -> pure (Right ()) :: IO (Either (Error EngineOnly) ()))
          case keeperRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          _ <- launchExec second isolatedEnvironment
          settled <- timeout 10000000 (waitForWorkflow second (WorkflowId keeperText))
          case settled of
            Just (Right (AwaitedSucceeded _ _)) -> pure ()
            other -> fail ("expected the keeper to recover, got: " <> show other)
          ghostStatus <- readWorkflowStatus getBackend (WorkflowId ghostText)
          ghostStatus @?= Just Pending,
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
      -- The double-click: the same id while the first run still owns it
      -- joins rather than failing — one id, one execution.
      liveCase getBackend (ioTracer . fst <$> getLogger) "starting a taken id joins the existing run" scenarioJoinTakesId checkJoinTakesId,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a fresh start is local and a join polls" scenarioFreshJoinPolls checkFreshJoinPolls,
      liveCase getBackend (ioTracer . fst <$> getLogger) "awaiting a child is recorded as a step" scenarioAwaitRecorded checkAwaitRecorded,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a recorded await of another workflow is refused" scenarioStaleAwaitRefused checkStaleAwaitRefused,
      liveCase getBackend (ioTracer . fst <$> getLogger) "awaiting a child inside a step is covered by that step" scenarioAwaitInsideStep checkAwaitInsideStep,
      liveCase getBackend (ioTracer . fst <$> getLogger) "child starts and awaits keep their ids in build order" scenarioChildIdsInBuildOrder checkChildIdsInBuildOrder,
      liveCase getBackend (ioTracer . fst <$> getLogger) "runs claim their pairs of step ids adjacently" scenarioStepIdPairs checkStepIdPairs,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a select step races a step against a child's result" scenarioSelectStepRaces checkSelectStepRaces,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a scoped select races two pending steps" scenarioScopedSelect checkScopedSelect,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a converted body runs through the scoped entries" scenarioScopedBody checkScopedBody,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a control signal winning a select records no winner" scenarioControlSelect checkControlSelect,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a losing step has its cancellation token fired" scenarioLosingTokenFired checkLosingTokenFired,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a cancelled child is an awaited cancellation in the parent" scenarioCancelledChildAwaited checkCancelledChildAwaited,
      -- IO only: recorded-await replay across two launches (needs the
      -- recovery sweep).
      testCase "a replayed parent reads the recorded outcome rather than waiting again" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-replay-await-" <> Text.take 12 suffix
            appVersion = "hs-l2-replay-await-version-" <> suffix
            executorId = "hs-l2-replay-await-executor-" <> suffix
            parentText = "hs-l2-replay-await-parent-" <> suffix
            childText = parentText <> "-0"
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "parent"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            childBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            childBody () _ = pure (Right 5)
        bracket (newDBOS config) shutdown $ \first -> do
          childRegistered <- registerDBOSWorkflowRef first childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          -- First process: the child finishes and the await is recorded,
          -- then the parent is killed before it can finish.
          let parentBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              parentBody () wctx = do
                started <- startChildWorkflow wctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWf wctx wfHandle
                    case awaited of
                      Left err -> pure (Left err)
                      Right (Just stored) -> do
                        threadDelay 30000000
                        pure $ case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> pure (Left (StepFailed "parent" "no child output"))
          parentRegistered <- registerDBOSWorkflowRef first parentKey parentBody
          parentRef <- case parentRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          execFirst <- launchExec first isolatedEnvironment
          _ <- startWfRef execFirst parentRef (startOptionsDefault {startWorkflowId = Just parentText}) Nothing
          reader <- getBackend
          let awaitRecorded = go (200 :: Int)
                where
                  go 0 = fail "the await was never recorded"
                  go n = do
                    rows <- listWorkflowSteps reader (WorkflowId parentText) False Nothing Nothing Nothing
                    case rows of
                      Right found | length found >= 2 -> pure ()
                      _ -> threadDelay 50000 >> go (n - 1)
          awaitRecorded
        -- The child is gone. Only the parent's recorded copy of its result
        -- is left, so finishing is only possible from the recorded row.
        reader <- getBackend
        deleted <- SystemDB.deleteWorkflows reader [WorkflowId childText] False Nothing
        case deleted of
          Left err -> fail (show err)
          Right _ -> pure ()
        gone <- getWorkflow reader (WorkflowId childText)
        gone @?= Right Nothing
        -- Second process: recovery replays the parent, which must not go
        -- looking for the child.
        bracket (newDBOS config) shutdown $ \second -> do
          childRegistered <- registerDBOSWorkflowRef second childKey childBody
          childRef2 <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let parentBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              parentBody () wctx = do
                started <- startChildWorkflow wctx childRef2 startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWf wctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          parentRegistered <- registerDBOSWorkflowRef second parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          _ <- launchExec second isolatedEnvironment
          settled <- timeout 15000000 (waitForWorkflow second (WorkflowId parentText))
          case settled of
            Just (Right (AwaitedSucceeded (Just output) _)) -> do
              let stored = SerializedWorkflowValue output Nothing
                  decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the replayed parent read the recorded await" (Right 5) decoded
            other -> fail ("expected the replayed parent to finish, got: " <> show other),
      liveCase getBackend (ioTracer . fst <$> getLogger) "a child inherits its parent's deadline" scenarioDeadlineInherited checkDeadlineInherited,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a child's own timeout replaces the inherited deadline" scenarioChildBudgetWins checkChildBudgetWins,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a child can decline the inherited deadline" scenarioDeclinedDeadline checkDeclinedDeadline,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a parent and its child hit an inherited deadline independently" scenarioCascadeDeadline checkCascadeDeadline,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a parent starts a child under a derived id and replay adopts it" scenarioDerivedChildAdopted checkDerivedChildAdopted,
      liveCase getBackend (ioTracer . fst <$> getLogger) "starting a child inside a step is refused, not recorded" scenarioChildInsideStepRefused checkChildInsideStepRefused,
      liveCase getBackend (ioTracer . fst <$> getLogger) "starting a child through a captured parent is refused, not recorded" scenarioCaptureChildRefused checkCaptureChildRefused,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a child that fails differently is started through lift" scenarioLiftChildError checkLiftChildError,
      -- IO only: the body performs real IO (the foreign charge call),
      -- which the simulator cannot run.
      testCase "a foreign error is converted at the boundary" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-foreign-" <> Text.take 12 suffix
            payText = "hs-l2-foreign-pay-" <> suffix
            payKey = newWorkflowKey "pay"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          let payBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error PaymentError) ())
              payBody () _ = do
                charged <- charge
                pure $ case charged of
                  Left refused -> Left (application (Gateway (Text.pack (show refused))))
                  Right () -> Right ()
          payRegistered <- registerDBOSWorkflow dbos payKey payBody
          case payRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchExec dbos isolatedEnvironment
          ran <- runDBOSWorkflow exec payKey (WorkflowId payText) (Just (encodeWorkflowValue ()))
          case ran of
            Left (Application (Gateway {reason})) -> reason @?= "the gateway refused the card"
            other -> fail ("expected the boundary conversion, got: " <> show other)
          reader <- getBackend
          payRow <- getWorkflow reader (WorkflowId payText)
          case payRow of
            Right (Just row) -> do
              row.workflowRecordStatus @?= Error
              case row.workflowRecordError of
                Just recorded -> case decodeErrorText recorded :: Either Text (Error PaymentError) of
                  Right (Application (Gateway {reason})) -> reason @?= "the gateway refused the card"
                  other -> fail ("expected the encoded error in the column, got: " <> show other)
                Nothing -> fail "the pay workflow recorded no error"
            other -> fail ("expected the pay row, got: " <> show other),
      liveCase getBackend (ioTracer . fst <$> getLogger) "a child started and never awaited is still recorded" scenarioUnawaitedChild checkUnawaitedChild,
      liveCase getBackend (ioTracer . fst <$> getLogger) "children started in a loop run concurrently" scenarioFanout checkFanout,
      -- IO only: first-to-settle timing is wall-clock-bound.
      testCase "select reports the first workflow to settle, not the first started" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-first-" <> Text.take 12 suffix
            waiterText = "hs-l2-first-waiter-" <> suffix
            childKey = newWorkflowKey "blocked"
            childText n = "hs-l2-first-blocked-" <> Text.pack (show (n :: Int)) <> "-" <> suffix
        entered <- mapM (const newEmptyMVar) [0, 1, 2 :: Int]
        gates <- mapM (const newEmptyMVar) [0, 1, 2 :: Int]
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            childBody :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            childBody n _ = putMVar (entered !! n) () >> takeMVar (gates !! n) >> pure (Right n)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          exec <- launchExec dbos isolatedEnvironment
          mapM_
            ( \n -> do
                startedChild <-
                  startWfRef
                    exec
                    childRef
                    (startOptionsDefault {startWorkflowId = Just (childText n)})
                    (Just (encodeWorkflowValue n))
                case startedChild of
                  Left err -> fail (show err)
                  Right _ -> pure ()
            )
            [0, 1, 2 :: Int]
          -- All three are at their gates, so nothing has settled and none
          -- can win by accident.
          mapM_ takeMVar entered
          reader <- getBackend
          waiterCreated <-
            SystemDB.initWorkflow
              reader
              ((newWorkflow waiterText) {newWorkflowName = Just "L2FirstWaiter"})
              Nothing
              Fresh
              Nothing
          case waiterCreated of
            Left err -> fail (show err)
            Right _ -> pure ()
          waiterCtx <- ctxOver reader nullTracer waiterText
          -- Released after the wait is in flight, so the wait genuinely
          -- waits rather than reading an already-settled row.
          winner <- newEmptyMVar
          waiter <- async (selectWorkflow waiterCtx (map (WorkflowId . childText) [0, 1, 2]) >>= putMVar winner)
          threadDelay 200000
          -- The middle one, neither the first id passed nor the first
          -- started: the answer has to name it.
          putMVar (gates !! 1) ()
          decided <- timeout 15000000 (takeMVar winner)
          cancel waiter
          case decided of
            Just (Right won) -> won @?= WorkflowId (childText 1)
            other -> fail ("expected the middle workflow to win, got: " <> show other)
          mapM_ (`putMVar` ()) [gates !! 0, gates !! 2],
      liveCase getBackend (ioTracer . fst <$> getLogger) "an assigned child id wins over the derived one" scenarioAssignedChildAdopted checkAssignedChildAdopted,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a workflow started outside a workflow has no parent" scenarioRootNoParent checkRootNoParent,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a start position holding a plain step is refused" scenarioPlainStepAtStart checkPlainStepAtStart,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a child started through another instance is refused" scenarioWrongInstance checkWrongInstance,
      testCase "a child joining a held key is recorded as the workflow it joined" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-join-" <> Text.take 12 suffix
            queueName = "hs-l2-join-q-" <> Text.take 12 suffix
            dedupKey = "order-42-" <> Text.take 12 suffix
            holderText = "hs-l2-join-holder-" <> suffix
            parentText = "hs-l2-join-parent-" <> suffix
            derivedText = parentText <> "-0"
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "joiner"
            joinQueue =
              (enqueueNew queueName)
                { deduplicationId = Just dedupKey,
                  duplicationPolicy = ReturnExisting
                }
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            childBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            childBody () _ = pure (Right 9)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let parentBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              parentBody () wctx = do
                started <- startChildWorkflow wctx childRef (startOptionsDefault {startQueue = Just joinQueue}) Nothing
                case started of
                  Left err -> pure (Left err)
                  Right handle -> do
                    result <- awaitWf wctx handle
                    case result of
                      Left err -> pure (Left err)
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right n -> pure (Right n)
                          Left _ -> pure (Left (StepFailed "parent" "bad child output"))
                      Right _ -> pure (Left (StepFailed "parent" "no child output"))
          parentRegistered <- registerDBOSWorkflow dbos parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchExec dbos isolatedEnvironment
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          -- The holder, enqueued before the parent runs and still waiting
          -- when the child starts: a delay holds the key without running.
          let holderQueue =
                (enqueueNew queueName)
                  { deduplicationId = Just dedupKey,
                    delay = Just (secondsDuration 3)
                  }
          holder <- startWfRef exec childRef (startOptionsDefault {startWorkflowId = Just holderText, startQueue = Just holderQueue}) Nothing
          case holder of
            Left err -> fail (show err)
            Right _ -> pure ()
          outcome <- timeout 30000000 (runWf exec parentKey (WorkflowId parentText) Nothing)
          case outcome of
            Just (Right (Just stored)) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the parent reads the joined workflow's output" (Right 9) decoded
            other -> fail ("expected the joined output, got: " <> show other)
          reader <- getBackend
          derived <- getWorkflow reader (WorkflowId derivedText)
          derived @?= Right Nothing
          listed <- listWorkflowSteps reader (WorkflowId parentText) True Nothing Nothing Nothing
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
                startedChild @?= holderText
                awaitName @?= "DBOS.getResult"
                awaitOutput @?= "9"
                awaitedChild @?= holderText
            other -> fail ("expected the joining start and its recorded await, got: " <> show other)
          -- The other half of the relationship is deliberately absent: the
          -- holder has its own owner already, so the join resolves through
          -- the parent's replay without listing among its children.
          children <- SystemDB.getWorkflowChildren reader (WorkflowId parentText)
          children @?= Right [],
      liveCase getBackend (ioTracer . fst <$> getLogger) "a zero-argument workflow records no input" scenarioZeroNoInput checkZeroNoInput,
      liveCase getBackend (ioTracer . fst <$> getLogger) "the row exists before the body starts" scenarioRowBeforeBody checkRowBeforeBody,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a panicking workflow leaves its row pending" scenarioPanic checkPanic,
      liveCase getBackend (ioTracer . fst <$> getLogger) "retrieving before launch is refused" scenarioRetrieveBeforeLaunch checkRunBeforeLaunch,
      liveCase getBackend (ioTracer . fst <$> getLogger) "an application error round-trips as itself" scenarioAppErrorRoundtrip checkAppErrorRoundtrip,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a database failure is not the workflow outcome" scenarioDbFailureNotOutcome checkDbFailureNotOutcome,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a workflow records the steps it took" scenarioStepsTaken checkStepsTaken,
      liveCase getBackend (ioTracer . fst <$> getLogger) "shutdown cancels a running workflow and leaves it pending" scenarioShutdownCancels checkShutdownCancels,
      liveCase getBackend (ioTracer . fst <$> getLogger) "dropping the future does not stop the workflow" scenarioDropFuture checkDropFuture,
      -- Sim only: not yet mirrored on IO (needs wall-clock
      -- budget/body scaling).
      testCase "a budget cancels the workflow durably" (pure ()),
      liveCase getBackend (ioTracer . fst <$> getLogger) "a started workflow carries the attributes it was given" scenarioAttributes checkAttributes,
      liveCase getBackend (ioTracer . fst <$> getLogger) "a step error is recorded in its column" scenarioStepErrorRecorded checkStepErrorRecorded,
      -- Sim only: typed trace assertions live only in sim.
      testCase "workflow announcements carry their counts and ids" (pure ()),
      tasksTests
    ]

-- * Shared workflow fixture: one body over any backend.

-- | How a tree instantiation builds its world: a fresh unlaunched
-- instance, the stack-specific launch, a fresh workflow id, and
-- backend-agnostic row and step reads. The tracer arrives as a
-- parameter — FastLogger on IO, the sim carrier on IOSim — and both
-- launches go through it over an explicitly built connection, so the
-- scenario drives the same engine calls on both stacks. Live fills the
-- rest with Postgres and per-test UUIDs; the sim tree with
-- 'MemSystemDB' and deterministic ids.
data WfFixture m = WfFixture
  { wfNewDBOS :: m (DBOS m),
    wfLaunch :: DBOS m -> m (Executor m),
    wfFreshId :: Text -> m WorkflowId,
    wfReadRow :: WorkflowId -> m (Maybe WorkflowRecord),
    wfListSteps :: WorkflowId -> m [StepRecord],
    wfChildren :: WorkflowId -> m [WorkflowId],
    -- | A second, separately launchable instance over the same backend:
    -- its own connection, registry, and identity (executor and version
    -- suffixed), for the cross-instance refusals.
    wfSecondInstance :: m (DBOS m, m (Executor m)),
    -- | The fixture's connection and application identity, for scenarios
    -- that build a workflow scope directly instead of through a runner.
    wfConn :: m (Connection m),
    wfIdentity :: Identity,
    -- | The backend the tree passed in, for scenarios that seed or read
    -- durable state directly (e.g. planting a stale await).
    wfSystemDB :: SomeSystemDB m
  }

-- | A registered workflow starts and records its result: the smallest
-- proof of the 'WfFixture' plumbing. Returns the decoded result and the
-- stored row. Engine errors throw (via 'MonadThrow'), so both trees
-- assert on plain values.
scenarioRegisteredRecordsResult ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Int, WorkflowRecord)
scenarioRegisteredRecordsResult fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newWorkflowKey "double"
        -- Converted body: registered through the scoped entry and using the
        -- scoped step runner. The body takes WorkflowCtx and every call
        -- it makes takes that view or one derived from it.
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body value wctx = runWorkflowStep wctx "double" (const (pure (value * 2)))
    registered <- registerDBOSWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "wf-double"
    (result :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec key wid (Just (encodeWorkflowValue (21 :: Int)))
    decoded <- case result of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the doubled result, got: " <> show other))
    row <- fx.wfReadRow wid
    case row of
      Just found -> pure (decoded, found)
      Nothing -> throwIO (userError "expected exactly one successful row")

-- | One fixture builder over any backend: the tree passes its
-- 'SomeSystemDB' and 'SomeTracer' in (plus config, identity, connection
-- app name, id naming, and id/entropy generators), and the connection,
-- the launch, and the row reads all go through them. Live passes
-- Postgres + FastLogger; sim passes 'MemSystemDB' + the sim carrier.
-- Backend construction lives here once; per-side factories supply only
-- the atoms.
mkWfFixture ::
  forall m.
  (MonadMVar m, MonadSTM m, MonadThrow m) =>
  Config ->
  Identity ->
  Text ->
  (Text -> WorkflowId) ->
  m Text ->
  m Word32 ->
  SomeSystemDB m ->
  SomeTracer m ->
  m (WfFixture m)
mkWfFixture config identity connApp nameScheme genId genEntropy sysdb tracer = do
  let mkConn = do
        instanceId <- genId
        newConnection
          sysdb
          RustSerde
          (Just connApp)
          (secondsDuration 1)
          OwnerApplication
          instanceId
          genId
          genEntropy
          tracer
  conn <- mkConn
  secondConn <- mkConn
  let config2 =
        config
          { configAppVersion = (<> "-other") <$> config.configAppVersion,
            configExecutorId = (<> "-other") <$> config.configExecutorId
          }
      identity2 =
        identity
          { identityAppVersion = identity.identityAppVersion <> "-other",
            identityExecutorId = identity.identityExecutorId <> "-other"
          }
  pure
    WfFixture
      { wfNewDBOS = newDBOS config,
        wfLaunch = \dbos -> launchOn dbos conn identity,
        wfFreshId = pure . nameScheme,
        wfReadRow = \wid -> do
          found <- runSystemDB sysdb (\db -> getWorkflow db wid)
          case found of
            Left err -> throwIO (userError (show err))
            Right row -> pure row,
        wfListSteps = \wid -> do
          listed <- runSystemDB sysdb (\db -> listWorkflowSteps db wid True Nothing Nothing Nothing)
          case listed of
            Left err -> throwIO (userError (show err))
            Right steps -> pure steps,
        wfChildren = \wid -> do
          children <- runSystemDB sysdb (\db -> SystemDB.getWorkflowChildren db wid)
          case children of
            Left err -> throwIO (userError (show err))
            Right ids -> pure ids,
        wfSecondInstance = do
          dbos2 <- newDBOS config2
          pure (dbos2, launchOn dbos2 secondConn identity2),
        wfConn = pure conn,
        wfIdentity = identity,
        wfSystemDB = sysdb
      }

-- | The shared tree over a real backend: every test owns its rows via
-- fresh UUIDs (application, executor, workflow id). The tracer arrives
-- as a parameter — FastLogger on IO, the sim carrier on IOSim — and the
-- launch goes through it over an explicitly built connection, so the
-- scenario drives the same launch call on both stacks. Production launch
-- (supervisor, prepare, version check) stays covered by the unconverted
-- live cases that still use @launchWithEnvironment@.
liveWfFixture :: IO Postgres.PostgresSystemDB -> IO (SomeTracer IO) -> IO (WfFixture IO)
liveWfFixture getBackend getTracer = do
  fresh <- UUID.V4.nextRandom
  let suffix = Text.pack (UUID.toString fresh)
      appName = "hs-l2-workflow-" <> Text.take 12 suffix
      appVersion = "hs-l2-version-" <> suffix
      executorId = "hs-l2-executor-" <> suffix
  config0 <- configFromEnv appName
  let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
      identity =
        Identity
          { identityAppName = appName,
            identityAppVersion = appVersion,
            identityExecutorId = executorId,
            identityAppId = ""
          }
  backend <- getBackend
  tracer <- getTracer
  mkWfFixture
    config
    identity
    -- The connection's application name is the workflow rows' own (the
    -- dequeue and recovery sweeps scope by it), not a separate test name.
    appName
    (\prefix -> WorkflowId ("hs-l2-" <> prefix <> "-" <> suffix))
    uuidWorkflowId
    uuidEntropy
    (SomeSystemDB backend)
    tracer

-- | What a joining double-start observes: both callers' decoded results,
-- how many times the body entered, the stored row's status, both handles'
-- workflow ids, and the first handle's status while the run still owns it.
-- A record (not a tuple): seven fields stay readable at both call sites.
data JoinOutcome = JoinOutcome
  { joinFirst :: Int,
    joinSecond :: Int,
    joinEntered :: Int,
    joinRowStatus :: WorkflowStatus,
    joinFirstId :: Text,
    joinSecondId :: Text,
    joinFirstPending :: WorkflowStatus
  }
  deriving stock (Eq, Show)

-- | A started id joined by a second start: one id, one execution. The
-- body parks on an MVar gate so the second start lands while the first
-- run still owns the id; the gate release lets both handles resolve.
-- Result waits are bounded (virtual time in sim, fifteen seconds live),
-- so a lost wakeup fails the case instead of hanging the suite.
scenarioJoinTakesId ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m JoinOutcome
scenarioJoinTakesId fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    entered <- newTVarIO (0 :: Int)
    release <- newEmptyMVar
    let key = newWorkflowKey "slow"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body () _ = do
          atomically (modifyTVar entered (+ 1))
          takeMVar release
          pure (Right 7)
    refE <- registerDBOSWorkflowRef dbos key body
    ref <- case refE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "join-start"
    let WorkflowId widText = wid
        startOpts = startOptionsDefault {startWorkflowId = Just widText}
    (firstE :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <- startDBOSWorkflowRef exec ref startOpts Nothing
    firstHandle <- case firstE of
      Left err -> throwIO (userError (show err))
      Right h -> pure h
    pendingE <- handleStatus firstHandle
    firstPending <- case pendingE of
      Right (Just status) -> pure status
      other -> throwIO (userError ("expected the started row PENDING: " <> show other))
    (secondE :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <- startDBOSWorkflowRef exec ref startOpts Nothing
    secondHandle <- case secondE of
      Left err -> throwIO (userError (show err))
      Right h -> pure h
    putMVar release ()
    first <- awaitSettled firstHandle
    second <- awaitSettled secondHandle
    count <- readTVarIO entered
    row <- fx.wfReadRow wid
    case (first, second, row) of
      (Right (Just firstStored), Right (Just secondStored), Just found) -> do
        let firstDecoded = decodeWorkflowValue "result" (Just firstStored) :: Either CodecError Int
            secondDecoded = decodeWorkflowValue "result" (Just secondStored) :: Either CodecError Int
        case (firstDecoded, secondDecoded) of
          (Right a, Right b) ->
            pure
              JoinOutcome
                { joinFirst = a,
                  joinSecond = b,
                  joinEntered = count,
                  joinRowStatus = found.workflowRecordStatus,
                  joinFirstId = firstHandle.workflowId,
                  joinSecondId = secondHandle.workflowId,
                  joinFirstPending = firstPending
                }
          other -> throwIO (userError ("expected both handles to resolve: " <> show other))
      other -> throwIO (userError ("expected both handles and one row: " <> show other))

-- | A bounded wait for a handle to settle: virtual time in sim, fifteen
-- seconds live. A lost wakeup fails the case instead of hanging the
-- suite. Shared by the scenarios that resolve handles.
awaitSettled ::
  forall m.
  (MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WorkflowHandle m EngineOnly ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
awaitSettled handle = do
  (settled :: Maybe (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))) <- timeout 15000000 (handleResult handle)
  case settled of
    Just r -> pure r
    Nothing -> throwIO (userError "the handle never settled")

-- | A fresh start is local, a join polls: the first start runs the body
-- in-process, while a second start of the same id and a retrieve observe
-- it through polling handles. Returns the three provenance labels and the
-- decoded result.
scenarioFreshJoinPolls ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Text, Text, Text, Int)
scenarioFreshJoinPolls fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    release <- newEmptyMVar
    let key = newWorkflowKey "quick"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body () _ = takeMVar release >> pure (Right 7)
    refE <- registerDBOSWorkflowRef dbos key body
    ref <- case refE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "local-id"
    let WorkflowId widText = wid
        startOpts = startOptionsDefault {startWorkflowId = Just widText}
        label :: WorkflowHandle m EngineOnly -> Text
        label (WorkflowHandle _ _ provenance') = case provenance' of
          Local _ -> "local"
          Polling {} -> "polling"
    (firstE :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <- startDBOSWorkflowRef exec ref startOpts Nothing
    firstHandle <- case firstE of
      Left err -> throwIO (userError (show err))
      Right h -> pure h
    (joinE :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <- startDBOSWorkflowRef exec ref startOpts Nothing
    joinHandle <- case joinE of
      Left err -> throwIO (userError (show err))
      Right h -> pure h
    (retrieveE :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <- retrieveWorkflow dbos wid
    retrieveHandle <- case retrieveE of
      Left err -> throwIO (userError (show err))
      Right h -> pure h
    putMVar release ()
    result <- awaitSettled firstHandle
    decoded <- case result of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the local await to resolve, got: " <> show other))
    pure (label firstHandle, label joinHandle, label retrieveHandle, decoded)

-- | Awaiting a child is recorded as a step: the parent starts the child
-- and awaits it, so the parent's history holds the start and a
-- @DBOS.getResult@ checkpoint naming the child. Returns the decoded
-- result, the parent's steps, and the derived child id.
scenarioAwaitRecorded ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Int, [StepRecord], Text)
scenarioAwaitRecorded fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 99)
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault Nothing
          case started of
            Left err -> pure (Left err)
            Right wfHandle -> do
              awaited <- awaitChild wctx wfHandle
              pure $ case awaited of
                Left err -> Left err
                Right (Just stored) ->
                  case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                    Right value -> Right value
                    Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                Right Nothing -> Left (StepFailed "parent" "no child output")
    parentReg <- registerDBOSWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "await-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec parentKey wid Nothing
    decoded <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the awaited child, got: " <> show other))
    steps <- fx.wfListSteps wid
    pure (decoded, steps, childText)

-- | Three children started and then awaited: the starts claim ids in
-- build order and the awaits follow in the same order, so the history is
-- starts 0-2 then awaits 3-5. Returns the summed result, the parent's
-- steps, the parent id, and the first-built child's recorded output.
scenarioChildIdsInBuildOrder ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Int, [StepRecord], Text, Maybe Text)
scenarioChildIdsInBuildOrder fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "parent"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody n _ = pure (Right n)
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    -- The oracle drives the built starts through `join!` backwards; the
    -- port claims and waits at the call, so call order is build order and
    -- the rows are the contract both pin.
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: [Either (Error EngineOnly) (WorkflowHandle m EngineOnly)]) <-
            mapM (\n -> startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowValue (n :: Int)))) [1, 2, 3]
          case sequence started of
            Left err -> pure (Left err)
            Right handles -> do
              awaited <- mapM (awaitChild wctx) handles
              case sequence awaited of
                Left err -> pure (Left err)
                Right outputs -> case mapM (decodeWorkflowValue "result") outputs of
                  Left _ -> pure (Left (StepFailed "parent" "bad child output"))
                  Right (numbers :: [Int]) -> pure (Right (sum numbers))
    parentReg <- registerDBOSWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "order-parent"
    let WorkflowId parentText = wid
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec parentKey wid Nothing
    decoded <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the summed children, got: " <> show other))
    steps <- fx.wfListSteps wid
    firstChild <- fx.wfReadRow (WorkflowId (parentText <> "-0"))
    pure (decoded, steps, parentText, firstChild >>= (.workflowRecordOutput))

-- | One row's status, for scenarios that report it bare.
rowStatus :: Maybe WorkflowRecord -> Maybe WorkflowStatus
rowStatus = fmap (.workflowRecordStatus)

-- | A child started through another instance is refused: the reference
-- was registered on a second instance, so the running instance refuses
-- to start it before anything is written. Returns the run, the derived
-- row, and the parent's steps.
scenarioWrongInstance ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, [StepRecord], Text)
scenarioWrongInstance fx =
  bracket fx.wfNewDBOS shutdown $ \owner -> do
    (other, launchOther) <- fx.wfSecondInstance
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 1)
    childRefE <- registerDBOSWorkflowRef other childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault Nothing
          case started of
            Left err -> pure (Left err)
            Right wfHandle -> do
              awaited <- awaitChild wctx wfHandle
              pure $ case awaited of
                Left err -> Left err
                Right (Just stored) ->
                  case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                    Right value -> Right value
                    Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                Right Nothing -> Left (StepFailed "parent" "no child output")
    parentReg <- registerDBOSWorkflow owner parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    _ <- launchOther
    exec <- fx.wfLaunch owner
    wid <- fx.wfFreshId "wrong-instance-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec parentKey wid Nothing
    missing <- fx.wfReadRow (WorkflowId childText)
    steps <- fx.wfListSteps wid
    shutdown other
    pure (ran, missing, steps, childText)

-- | A row that must appear, observed by polling the fixture's own read:
-- the row is written before the body is entered, so its presence means
-- the run is gated, not merely started. Shared by the shutdown and
-- dropped-future scenarios.
waitForRowShared ::
  forall m.
  (MonadDelay m, MonadThrow m) =>
  (WorkflowId -> m (Maybe WorkflowRecord)) ->
  WorkflowId ->
  m ()
waitForRowShared readRow wid = go (200 :: Int)
  where
    go 0 = throwIO (userError "the workflow row never appeared")
    go n = do
      row <- readRow wid
      case row of
        Just _ -> pure ()
        Nothing -> threadDelay 50000 >> go (n - 1)

-- | A workflow records the steps it took: two steps compose and list in
-- order. Returns the run and the steps.
scenarioStepsTaken ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord])
scenarioStepsTaken fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newWorkflowKey "two-steps"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body value wctx = do
          first <- runWorkflowStep wctx "one" (const (pure (value + 1)))
          case first of
            Left err -> pure (Left err)
            Right stepped -> runWorkflowStep wctx "two" (const (pure (stepped * 2)))
    registered <- registerDBOSWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "steps-listed-id"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runDBOSWorkflow exec key wid (Just (encodeWorkflowValue (21 :: Int)))
    steps <- fx.wfListSteps wid
    pure (ran, steps)

-- | Shutdown cancels a running workflow and leaves it pending: the run
-- is gated when the executor shuts down, the caller is cancelled, and
-- the row stays @PENDING@ for the next launch to recover. Returns the
-- row's status before and after.
scenarioShutdownCancels ::
  forall m.
  (MonadAsync m, MonadDelay m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Maybe WorkflowStatus, Maybe WorkflowStatus)
scenarioShutdownCancels fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    gate <- newEmptyMVar
    let key = newWorkflowKey "gated"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body () _ = takeMVar gate >> pure (Right 7)
    refE <- registerDBOSWorkflowRef dbos key body
    ref <- case refE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "shutdown-run-id"
    worker <-
      async
        ( runDBOSWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just (let WorkflowId t = wid in t)}) Nothing ::
            m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
        )
    waitForRowShared fx.wfReadRow wid
    before <- rowStatus <$> fx.wfReadRow wid
    shutdown dbos
    cancel worker
    after <- rowStatus <$> fx.wfReadRow wid
    pure (before, after)

-- | Dropping the future does not stop the workflow: cancelling the
-- caller leaves the row pending and the body still gated; released, the
-- run finishes on its own. Returns the gated row's status and the
-- settled outcome.
scenarioDropFuture ::
  forall m.
  (MonadAsync m, MonadDelay m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Maybe WorkflowStatus, Either (Error EngineOnly) AwaitedOutcome)
scenarioDropFuture fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    gate <- newEmptyMVar
    let key = newWorkflowKey "gated"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body () _ = takeMVar gate >> pure (Right 7)
    refE <- registerDBOSWorkflowRef dbos key body
    ref <- case refE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "drop-future-id"
    worker <-
      async
        ( runDBOSWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just (let WorkflowId t = wid in t)}) Nothing ::
            m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
        )
    waitForRowShared fx.wfReadRow wid
    -- Dropping the waiter stops the watching, not the workflow: the run
    -- is detached onto the executor, so cancelling the caller leaves the
    -- row pending and the body still gated.
    cancel worker
    gated <- rowStatus <$> fx.wfReadRow wid
    putMVar gate ()
    settled <- waitForWorkflow dbos wid
    pure (gated, settled)

-- | A started workflow carries the attributes it was given: the run's
-- attributes land on the parent's row, and the child, naming nothing of
-- its own, inherits nothing. Returns the run, both rows, and the tenant.
scenarioAttributes ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord, Text)
scenarioAttributes fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "attributed"
        tenant = "acme-tenant"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody _ _ = pure (Right 9)
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowValue (0 :: Int)))
          case started of
            Left err -> pure (Left err)
            Right handle -> do
              result <- awaitChild wctx handle
              pure $ case result of
                Left err -> Left err
                Right (Just stored) ->
                  case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                    Right n -> Right n
                    Left _ -> Left (StepFailed "parent" "bad child output")
                Right _ -> Left (StepFailed "parent" "no child output")
    parentRefE <- registerDBOSWorkflowRef dbos parentKey parentBody
    parentRef <- case parentRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "attributes-parent"
    let WorkflowId parentText = wid
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runDBOSWorkflowRef exec parentRef (runOptionsDefault {runWorkflowId = Just parentText, runAttributes = Just (Map.singleton "tenant" (String tenant))}) Nothing
    parentRow <- fx.wfReadRow wid
    childRow <- fx.wfReadRow (WorkflowId (parentText <> "-0"))
    pure (ran, parentRow, childRow, tenant)

-- | A step error is recorded in its column: the step fails, the run
-- returns the step error, and the column holds the shortfall. Returns
-- the run and the parent's steps.
scenarioStepErrorRecorded ::
  forall m.
  (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord])
scenarioStepErrorRecorded fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newWorkflowKey "charger"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body () wctx = runWorkflowStepWith stepOptionsDefault wctx "charge" (const (pure (Left (StepFailed "charge" "short by 12"))))
    registered <- registerDBOSWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "step-err-id"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec key wid Nothing
    steps <- fx.wfListSteps wid
    pure (ran, steps)

-- | An application error round-trips as itself: the body's own failure
-- comes back unchanged through the engine. Returns the run.
scenarioAppErrorRoundtrip ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
scenarioAppErrorRoundtrip fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newWorkflowKey "flaky"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body _ _ = pure (Left (StepFailed "flaky" "boom"))
    registered <- registerDBOSWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "app-err-id"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runDBOSWorkflow exec key wid (Just (encodeWorkflowValue (21 :: Int)))
    pure ran

-- | A database failure is not the workflow outcome: the backend error
-- comes back as itself and the row is left pending with no error
-- column, so a later recovery can retry. Returns the run and the row.
scenarioDbFailureNotOutcome ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord)
scenarioDbFailureNotOutcome fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newWorkflowKey "blips"
        backendErr =
          SystemDB.Backend
            ( SystemDB.BackendError
                { SystemDB.backendMessage = "connection reset by peer",
                  SystemDB.backendSqlState = Nothing,
                  SystemDB.backendKind = SystemDB.Connection
                }
            )
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
        body () _ = pure (Left (ErrorSystemDatabase backendErr))
    registered <- registerDBOSWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "blip-id"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec key wid Nothing
    row <- fx.wfReadRow wid
    pure (ran, row)

-- | A panicking workflow leaves its row pending: the body's exception
-- escapes the run as itself, and the row stays @PENDING@ with no error
-- column. Returns the escaped outcome and the row.
scenarioPanic ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either SomeException (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)), Maybe WorkflowRecord)
scenarioPanic fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newWorkflowKey "explodes"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
        body () _ = throwIO (userError "boom")
    registered <- registerDBOSWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "panic-id"
    outcome <- try (runDBOSWorkflow exec key wid Nothing)
    row <- fx.wfReadRow wid
    pure (outcome, row)

-- | Retrieving before launch is refused: the unlaunched instance has no
-- executor, naming the call. Returns the refusal. (Running before launch
-- moved to the type level: the runner takes the launch-produced
-- 'Executor', so that call is unconstructible and this runtime surface is
-- the remaining not-launched refusal.)
scenarioRetrieveBeforeLaunch ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
scenarioRetrieveBeforeLaunch fx = do
  dbos <- fx.wfNewDBOS
  wid <- fx.wfFreshId "unlaunched-id"
  retrieved <- (retrieveWorkflow dbos wid :: m (Either (Error EngineOnly) (WorkflowHandle m EngineOnly)))
  pure $ case retrieved of
    Left err -> Left err
    Right _ -> Right Nothing

-- | A zero-argument workflow records no input: the row's input column
-- stays null however the workflow ran. Returns the run and its row.
scenarioZeroNoInput ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord)
scenarioZeroNoInput fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newWorkflowKey "zero"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
        body () _ = pure (Right ())
    registered <- registerDBOSWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "zero-id"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec key wid Nothing
    row <- fx.wfReadRow wid
    pure (ran, row)

-- | The row exists before the body starts: the body reads its own row
-- through the context's database and finds it. Returns the run.
scenarioRowBeforeBody ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
scenarioRowBeforeBody fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newWorkflowKey "sees-itself"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Bool)
        body () wctx = do
          row <- withSystemDB wctx (\db -> SystemDB.getWorkflow db (WorkflowId (workflowId wctx)))
          pure (Right (case row of Right (Just _) -> True; _ -> False))
    registered <- registerDBOSWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "row-id"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec key wid Nothing
    pure ran

-- | A start position holding a plain step is refused: the parent parks
-- before its first step id is allocated, a plain step is planted at the
-- start position, and the start then finds output where a child link
-- should be. The refusal happens early, so nothing was created to be
-- orphaned. Returns the run and the derived row.
scenarioPlainStepAtStart ::
  forall m.
  (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord)
scenarioPlainStepAtStart fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "waiter"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody _ _ = pure (Right 1)
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    gate <- newEmptyMVar
    entered <- newEmptyMVar
    let parentBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody _ wctx = do
          putMVar entered ()
          takeMVar gate
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowValue (1 :: Int)))
          case started of
            Left err -> pure (Left err)
            Right _ -> pure (Right 0)
    parentReg <- registerDBOSWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "stale-parent"
    let WorkflowId parentText = wid
        derivedText = parentText <> "-0"
    worker <-
      async
        ( runDBOSWorkflow exec parentKey wid (Just (encodeWorkflowValue (0 :: Int))) ::
            m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
        )
    enteredOk <- timeout 15000000 (takeMVar entered)
    case enteredOk of
      Nothing -> throwIO (userError "the parent never reached its gate")
      Just () -> pure ()
    planted <-
      runSystemDB fx.wfSystemDB $ \db ->
        SystemDB.recordStep db wid 0 "child" (SystemDB.OutcomeOutput (Just "1")) Nothing Nothing
    case planted of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    putMVar gate ()
    outcome <- timeout 15000000 (wait worker)
    ran <- case outcome of
      Just r -> pure r
      Nothing -> throwIO (userError "the plain-step refusal never returned")
    missing <- fx.wfReadRow (WorkflowId derivedText)
    pure (ran, missing)

-- | A parent starts a child under a derived id and replay adopts it: the
-- child start detaches the child onto the executor, which runs it; a
-- second run of the parent adopts the recorded child id instead of
-- starting another. Returns the child id, the (empty) recovery and
-- dequeue results, the settled child, and the replayed parent's run.
scenarioDerivedChildAdopted ::
  forall m.
  (MonadDelay m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Text, [WorkflowId], Either (Error EngineOnly) [WorkflowId], Either (Error EngineOnly) AwaitedOutcome, Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
scenarioDerivedChildAdopted fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "double"
        parentKey = newWorkflowKey "parent"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody value wctx = runWorkflowStep wctx "double" (const (pure (value * 2)))
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        parentBody _ wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowValue (21 :: Int)))
          pure ((.workflowId) <$> started)
    parentReg <- registerDBOSWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "child-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (first :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec parentKey wid (Just (encodeWorkflowValue (0 :: Int)))
    _ <- case first of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
        Right _ -> pure ()
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the parent's child id, got: " <> show other))
    -- The child start detaches the child onto the executor; the replay
    -- adopts the recorded id instead of starting another.
    settled <- waitForWorkflow dbos (WorkflowId childText)
    (replayed :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec parentKey wid (Just (encodeWorkflowValue (0 :: Int)))
    pure (childText, [], Right [], settled, replayed)

-- | The assigned-id variant: the child was started under a chosen id, so
-- it runs under that id and no derived row ever exists. Returns the
-- chosen id, the (empty) recovery and dequeue results, the settled
-- child, the replayed parent's run, and both rows.
scenarioAssignedChildAdopted ::
  forall m.
  (MonadDelay m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Text, [WorkflowId], Either (Error EngineOnly) [WorkflowId], Either (Error EngineOnly) AwaitedOutcome, Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord)
scenarioAssignedChildAdopted fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "namer"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody _ _ = pure (Right 7)
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    wid <- fx.wfFreshId "assigned-parent"
    let WorkflowId parentText = wid
        chosenText = parentText <> "-chosen"
        derivedText = parentText <> "-0"
        parentBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        parentBody _ wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef (startOptionsDefault {startWorkflowId = Just chosenText}) (Just (encodeWorkflowValue (21 :: Int)))
          pure ((.workflowId) <$> started)
    parentReg <- registerDBOSWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    (first :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec parentKey wid (Just (encodeWorkflowValue (0 :: Int)))
    _ <- case first of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
        Right _ -> pure ()
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the parent's child id, got: " <> show other))
    settled <- waitForWorkflow dbos (WorkflowId chosenText)
    (replayed :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec parentKey wid (Just (encodeWorkflowValue (0 :: Int)))
    chosenRow <- fx.wfReadRow (WorkflowId chosenText)
    derivedRow <- fx.wfReadRow (WorkflowId derivedText)
    pure (chosenText, [], Right [], settled, replayed, chosenRow, derivedRow)

-- | A workflow started outside a workflow has no parent: a root run
-- records no parent link. Returns the run and its row.
scenarioRootNoParent ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord)
scenarioRootNoParent fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newWorkflowKey "root"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body () _ = pure (Right 1)
    registered <- registerDBOSWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "root-id"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec key wid Nothing
    row <- fx.wfReadRow wid
    pure (ran, row)

-- | Children started in a loop run concurrently: all three start before
-- any await, so their sleeps overlap and the whole fan-out takes about
-- one child's delay rather than three. Returns the summed result, the
-- children, and the elapsed milliseconds — measured on the wall clock
-- live, on the virtual clock in sim, where a serialized run would still
-- show three delays.
scenarioFanout ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [WorkflowId], Int64)
scenarioFanout fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "fan"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody n _ = threadDelay 400000 >> pure (Right n)
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    -- Start all three first, then collect: awaiting inside the first loop
    -- would serialize them.
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: [Either (Error EngineOnly) (WorkflowHandle m EngineOnly)]) <-
            mapM (\n -> startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowValue (n :: Int)))) [0, 1, 2]
          case sequence started of
            Left err -> pure (Left err)
            Right handles -> do
              results <- mapM (awaitChild wctx) handles
              case sequence results of
                Left err -> pure (Left err)
                Right outputs -> case mapM (decodeWorkflowValue "result") outputs of
                  Left _ -> pure (Left (StepFailed "fan" "bad child output"))
                  Right (numbers :: [Int]) -> pure (Right (sum numbers))
    parentReg <- registerDBOSWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "fanout-parent"
    began <- SystemDB.timestampNow
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec parentKey wid Nothing
    ended <- SystemDB.timestampNow
    children <- fx.wfChildren wid
    let tookMs = SystemDB.timestampToEpochMs ended - SystemDB.timestampToEpochMs began
    pure (ran, children, tookMs)

-- | A child started and never awaited is still recorded: the handle is
-- dropped, but the start row is what makes a child adoptable, and the
-- detached child outlives the parent's interest and records its result.
-- Returns the parent's run, its steps, the child's awaited outcome, the
-- child row, the parent's children, and the parent id.
scenarioUnawaitedChild ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord], Either (Error EngineOnly) AwaitedOutcome, Maybe WorkflowRecord, [WorkflowId], Text)
scenarioUnawaitedChild fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "forgetful"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody value wctx = runWorkflowStep wctx "double" (const (pure (value * 2)))
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
        parentBody _ wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowValue (21 :: Int)))
          case started of
            Left err -> pure (Left err)
            Right _ -> pure (Right ())
    parentReg <- registerDBOSWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "unawaited-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runDBOSWorkflow exec parentKey wid (Just (encodeWorkflowValue (0 :: Int)))
    steps <- fx.wfListSteps wid
    found <- waitForWorkflow dbos (WorkflowId childText)
    childRow <- fx.wfReadRow (WorkflowId childText)
    children <- fx.wfChildren wid
    pure (ran, steps, found, childRow, children, parentText)

-- | A child that fails differently is started through lift: the child's
-- own error channel crosses the boundary as itself on its row, and the
-- parent reports the refused child through its own channel. Returns the
-- parent's run and the child's row.
scenarioLiftChildError ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error GaveUp) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord)
scenarioLiftChildError fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let shipKey = newWorkflowKey "ship"
        billKey = newWorkflowKey "bill"
        shipBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error Refused) ())
        shipBody () _ = pure (Left (application Refused))
    shipRefE <- registerDBOSWorkflowRef dbos shipKey shipBody
    shipRef <- case shipRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let billBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error GaveUp) Bool)
        billBody () wctx = do
          (started :: Either (Error GaveUp) (WorkflowHandle m Refused)) <-
            startChildWorkflow wctx shipRef startOptionsDefault Nothing
          case started of
            Left err -> pure (Left err)
            Right handle -> do
              awaited <- awaitChild wctx handle
              let refusedChild = case awaited of
                    Left (Application Refused) -> True
                    _ -> False
              marker <- nextWorkflowMarker wctx
              (refusedStart :: Either (Error GaveUp) ()) <-
                withStep wctx marker (firstStepStatus 2) $ \_sctx -> do
                  inside <- startChildWorkflow wctx shipRef startOptionsDefault Nothing
                  pure (case inside of
                    Left err -> Left err
                    Right _ -> Right ())
              pure $ case refusedStart of
                Left (InsideStep _) -> Right refusedChild
                Left err -> Left err
                Right () -> Right False
    billRefE <- registerDBOSWorkflowRef dbos billKey billBody
    billRef <- case billRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    billWid <- fx.wfFreshId "lift-parent"
    let WorkflowId billText = billWid
        shipText = billText <> "-0"
    ran <-
      runDBOSWorkflowRef exec billRef (runOptionsDefault {runWorkflowId = Just billText}) (Just (encodeWorkflowValue ()))
    childRow <- fx.wfReadRow (WorkflowId shipText)
    pure (ran, childRow)

-- | Starting a child through a captured parent while a step body runs is
-- refused, not recorded: the depth says what the context cannot, so the
-- engine refuses it with @InsideStep@ before anything is written and no
-- start row appears. Returns the run and the parent's steps.
scenarioCaptureChildRefused ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord])
scenarioCaptureChildRefused fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "double"
        parentKey = newWorkflowKey "badparent"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody value wctx = runWorkflowStep wctx "double" (const (pure (value * 2)))
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let -- Converted body: the scoped shape with the captured-parent start.
        -- The step body captures the workflow view and starts through it —
        -- the same capture the context-level shape made — and the refusal
        -- still fires through the shared depth backstop.
        badBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        badBody _ wctx = do
          marker <- nextWorkflowMarker wctx
          outcome <- withStep wctx marker (firstStepStatus 0) (\_ -> startChildWorkflow wctx childRef startOptionsDefault Nothing)
          pure $ case outcome of
            Left err -> Left err
            Right handle -> Left (ErrorConfig ("started through a captured parent: " <> handle.workflowId))
    parentReg <- registerDBOSWorkflow dbos parentKey badBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "childleaf-captured-parent"
    (result :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec parentKey wid (Just (encodeWorkflowValue (0 :: Int)))
    steps <- fx.wfListSteps wid
    pure (result, steps)

-- | Starting a child inside a step is refused, not recorded: the leaf
-- start is attempted inside a step scope, so the engine refuses it with
-- @InsideStep@ and no start row appears. Returns the run and the
-- parent's steps.
scenarioChildInsideStepRefused ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord])
scenarioChildInsideStepRefused fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "double"
        parentKey = newWorkflowKey "badparent"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody value wctx = runWorkflowStep wctx "double" (const (pure (value * 2)))
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let badBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        badBody _ wctx = do
          marker <- nextWorkflowMarker wctx
          outcome <- withStep wctx marker (firstStepStatus 0) (\_sctx -> startChildWorkflow wctx childRef startOptionsDefault Nothing)
          pure $ case outcome of
            Left err -> Left err
            Right handle -> Left (ErrorConfig ("started inside a step: " <> handle.workflowId))
    parentReg <- registerDBOSWorkflow dbos parentKey badBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "childleaf-parent"
    (result :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec parentKey wid (Just (encodeWorkflowValue (0 :: Int)))
    steps <- fx.wfListSteps wid
    pure (result, steps)

-- | A parent and its child hit an inherited deadline independently: the
-- parent's budget expires while it awaits a sleeping child, which carries
-- the same deadline and cancels itself too. The interrupted await
-- checkpointed nothing — nobody answered it — so a resumed parent asks
-- the child's then-settled row again. Returns the parent's run, the
-- child's awaited outcome, and the parent's steps.
scenarioCascadeDeadline ::
  forall m.
  (MonadDelay m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Either (Error EngineOnly) AwaitedOutcome, [StepRecord])
scenarioCascadeDeadline fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = threadDelay 30000000 >> pure (Right 1)
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault Nothing
          case started of
            Left err -> pure (Left err)
            Right wfHandle -> do
              awaited <- awaitChild wctx wfHandle
              pure $ case awaited of
                Left err -> Left err
                Right (Just stored) ->
                  case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                    Right value -> Right value
                    Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                Right Nothing -> Left (StepFailed "parent" "no child output")
    parentRefE <- registerDBOSWorkflowRef dbos parentKey parentBody
    parentRef <- case parentRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "cascade-deadline-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runDBOSWorkflowRef exec parentRef (runOptionsDefault {runWorkflowId = Just parentText, runTimeout = Explicit (millisDuration 400)}) Nothing
    childOutcome <- waitForWorkflow dbos (WorkflowId childText)
    steps <- fx.wfListSteps wid
    pure (ran, childOutcome, steps)

-- | A child can decline the inherited deadline: two children under one
-- bounded parent — the first says nothing, the second declines — are
-- together the difference the timeout sum exists for. Returns the
-- parent's run and all three rows.
scenarioDeclinedDeadline ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord, Maybe WorkflowRecord)
scenarioDeclinedDeadline fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 1)
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          let childPair opts = do
                (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
                  startChildWorkflow wctx childRef opts Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitChild wctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          first <- childPair startOptionsDefault
          second <- childPair (startOptionsDefault {startTimeout = None})
          pure $ case (first, second) of
            (Right x, Right y) -> Right (x + y)
            _ -> Left (StepFailed "parent" "a child failed")
    parentRefE <- registerDBOSWorkflowRef dbos parentKey parentBody
    parentRef <- case parentRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "decline-deadline-parent"
    let WorkflowId parentText = wid
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runDBOSWorkflowRef exec parentRef (runOptionsDefault {runWorkflowId = Just parentText, runTimeout = Explicit (secondsDuration 300)}) Nothing
    parentRow <- fx.wfReadRow wid
    inheritedRow <- fx.wfReadRow (WorkflowId (parentText <> "-0"))
    detachedRow <- fx.wfReadRow (WorkflowId (parentText <> "-2"))
    pure (ran, parentRow, inheritedRow, detachedRow)

-- | A child's own timeout replaces the inherited deadline: given its own
-- budget, the child records that timeout and a deadline that outlives its
-- parent's instead of copying the parent's instant.
scenarioChildBudgetWins ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord)
scenarioChildBudgetWins fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 1)
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef (startOptionsDefault {startTimeout = Explicit (secondsDuration 3600)}) Nothing
          case started of
            Left err -> pure (Left err)
            Right wfHandle -> do
              awaited <- awaitChild wctx wfHandle
              pure $ case awaited of
                Left err -> Left err
                Right (Just stored) ->
                  case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                    Right value -> Right value
                    Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                Right Nothing -> Left (StepFailed "parent" "no child output")
    parentRefE <- registerDBOSWorkflowRef dbos parentKey parentBody
    parentRef <- case parentRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "child-budget-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runDBOSWorkflowRef exec parentRef (runOptionsDefault {runWorkflowId = Just parentText, runTimeout = Explicit (secondsDuration 60)}) Nothing
    parentRow <- fx.wfReadRow wid
    childRow <- fx.wfReadRow (WorkflowId childText)
    pure (ran, parentRow, childRow)

-- | A child inherits its parent's deadline: the parent's budget becomes
-- a wall-clock deadline stored on its row, and the child copies the same
-- instant instead of deriving a fresh budget. Returns the parent's run
-- and both rows.
scenarioDeadlineInherited ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord)
scenarioDeadlineInherited fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 1)
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault Nothing
          case started of
            Left err -> pure (Left err)
            Right wfHandle -> do
              awaited <- awaitChild wctx wfHandle
              pure $ case awaited of
                Left err -> Left err
                Right (Just stored) ->
                  case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                    Right value -> Right value
                    Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                Right Nothing -> Left (StepFailed "parent" "no child output")
    parentRefE <- registerDBOSWorkflowRef dbos parentKey parentBody
    parentRef <- case parentRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "inherit-deadline-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runDBOSWorkflowRef exec parentRef (runOptionsDefault {runWorkflowId = Just parentText, runTimeout = Explicit (secondsDuration 300)}) Nothing
    parentRow <- fx.wfReadRow wid
    childRow <- fx.wfReadRow (WorkflowId childText)
    pure (ran, parentRow, childRow)

-- | A cancelled child is an awaited cancellation in the parent: the
-- child carries its own budget, so it cancels itself while the parent
-- waits — that is the awaited workflow's outcome, not the parent's own
-- cancellation. Returns the parent's run, its steps, both rows' statuses,
-- and the child id.
scenarioCancelledChildAwaited ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord], Maybe WorkflowStatus, Maybe WorkflowStatus, Text)
scenarioCancelledChildAwaited fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = threadDelay 30000000 >> pure (Right 1)
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef (startOptionsDefault {startTimeout = Explicit (millisDuration 300)}) Nothing
          case started of
            Left err -> pure (Left err)
            Right wfHandle -> do
              awaited <- awaitChild wctx wfHandle
              pure $ case awaited of
                Left err -> Left err
                Right (Just stored) ->
                  case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                    Right value -> Right value
                    Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                Right Nothing -> Left (StepFailed "parent" "no child output")
    parentReg <- registerDBOSWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "awaited-cancel-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    settled <-
      timeout 15000000
        ( runDBOSWorkflow exec parentKey wid Nothing ::
            m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
        )
    ran <- case settled of
      Just r -> pure r
      Nothing -> throwIO (userError "the awaited cancellation never returned")
    steps <- fx.wfListSteps wid
    parentRow <- fx.wfReadRow wid
    childRow <- fx.wfReadRow (WorkflowId childText)
    pure (ran, steps, (.workflowRecordStatus) <$> parentRow, (.workflowRecordStatus) <$> childRow, childText)

-- | A converted body: registered through the scoped entry, it runs with
-- the scoped step runner and the scoped sleep through the real run path —
-- the erased transition hands the body a WorkflowCtx. Returns the decoded
-- result and the recorded step names.
scenarioScopedBody ::
  forall m.
  (MonadAsync m, MonadDelay m, MonadFork m, MonadMask m, MonadMVar m, MonadSTM m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) Int, [(Int, Text)])
scenarioScopedBody fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newWorkflowKey "scoped-body"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body value wctx = do
          stepped <- runWorkflowStep wctx "double" (\_ -> pure (value * 2))
          case stepped of
            Left err -> pure (Left err)
            Right doubled -> do
              slept <- sleepWorkflowStep wctx (millisDuration 1)
              pure (doubled <$ slept)
    registered <- registerDBOSWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "scoped-body-wf"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runDBOSWorkflow exec key wid (Just (encodeWorkflowValue (21 :: Int)))
    decoded <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right value -> pure (Right value)
        Left err -> pure (Left (ErrorDeserialization "result" (Text.pack (show err))))
      Right Nothing -> pure (Right 0)
      Left err -> pure (Left err)
    steps <- fx.wfListSteps wid
    pure (decoded, map (\row -> (row.stepRecordStepId, row.stepRecordStepName)) steps)

-- | The converted body's value is the doubled input and its rows are the
-- step and the sleep, in order.
checkScopedBody :: (Either (Error EngineOnly) Int, [(Int, Text)]) -> Either String ()
checkScopedBody (outcome, steps)
  | outcome /= Right 42 = Left ("expected the doubled value, got: " <> show outcome)
  | steps /= [(0, "double"), (1, "DBOS.sleep")] = Left ("unexpected rows: " <> show steps)
  | otherwise = Right ()

-- | The scoped select: two pending steps built through the workflow view,
-- raced by 'selectStep' over the same view. The fast arm wins, the
-- select records its own position after both branch ids, and the loser
-- leaves no row. Returns the winner's value and the recorded steps.
scenarioScopedSelect ::
  forall m.
  (MonadAsync m, MonadDelay m, MonadFork m, MonadMask m, MonadMVar m, MonadSTM m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) Int, [(Int, Text)])
scenarioScopedSelect fx = do
  bracket fx.wfNewDBOS shutdown $ \_dbos -> do
    wid <- fx.wfFreshId "scoped-select-parent"
    let WorkflowId widText = wid
    created <-
      runSystemDB fx.wfSystemDB $ \db ->
        SystemDB.initWorkflow db ((newWorkflow widText) {newWorkflowName = Just "L2ScopedSelect"}) Nothing Fresh Nothing
    case created of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    conn <- fx.wfConn
    outcome <-
      withWorkflow conn fx.wfIdentity wid Nothing $ \wctx -> do
        slow <- pendingWorkflowStep wctx "slow" (\_ -> threadDelay 30000000 >> pure (Right (2 :: Int)))
        fast <- pendingWorkflowStep wctx "fast" (\_ -> pure (Right (1 :: Int)))
        selectStep
          wctx
          [ SelectArm "slow" slow (\armOutcome -> pure (armOutcome >>= \value -> Right value)),
            SelectArm "fast" fast (\armOutcome -> pure (armOutcome >>= \value -> Right value))
          ]
    steps <- fx.wfListSteps wid
    pure (outcome, map (\row -> (row.stepRecordStepId, row.stepRecordStepName)) steps)

-- | The winner is the fast arm's value; the rows are the fast step under
-- its branch id and the select's own position after both branches.
checkScopedSelect :: (Either (Error EngineOnly) Int, [(Int, Text)]) -> Either String ()
checkScopedSelect (outcome, steps)
  | outcome /= Right 1 = Left ("expected the fast arm's value, got: " <> show outcome)
  | steps /= [(1, "fast"), (2, "DBOS.selectStep")] = Left ("unexpected rows: " <> show steps)
  | otherwise = Right ()

-- | A losing step has its cancellation token fired: the loser registers
-- a watcher on its token, then parks; the winner waits for that
-- registration, so dropping the loser must fire the token for work the
-- runtime cannot stop by dropping it. Returns the winner's value and
-- whether the loser's watcher observed the fire.
scenarioLosingTokenFired ::
  forall m.
  (MonadAsync m, MonadDelay m, MonadFork m, MonadMask m, MonadMVar m, MonadSTM m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Int, Bool)
scenarioLosingTokenFired fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    released <- newEmptyMVar
    watching <- newEmptyMVar
    let parentKey = newWorkflowKey "parent"
        parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          slow <- pendingWorkflowStep wctx "slow" $ \inner -> do
            token <- stepCtxCancellationToken inner
            _ <- async $ do
              let watch = do
                    cancelled <- tokenCancelled token
                    if cancelled then pure () else threadDelay 1000 >> watch
              watch
              putMVar released ()
            putMVar watching ()
            threadDelay 30000000
            pure (Right (2 :: Int))
          fast <- pendingWorkflowStep wctx "fast" (\_ -> takeMVar watching >> pure (Right (1 :: Int)))
          selectStep
            wctx
            [ SelectArm "slow" slow (\outcome -> pure (outcome >>= \value -> Right value)),
              SelectArm "fast" fast (\outcome -> pure (outcome >>= \value -> Right value))
            ]
    parentReg <- registerDBOSWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "race-token-parent"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec parentKey wid Nothing
    decoded <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the fast step's value, got: " <> show other))
    fired <- timeout 15000000 (takeMVar released)
    pure (decoded, maybe False (const True) fired)

-- | A control signal winning a select records no winner: the arm whose
-- outcome is the interrupted error wins, no step row is written, and the
-- row stays @PENDING@. Returns the run's outcome, the parent's steps,
-- the row's status, and the parent id.
scenarioControlSelect ::
  forall m.
  (MonadAsync m, MonadDelay m, MonadFork m, MonadMask m, MonadMVar m, MonadSTM m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord], Maybe WorkflowStatus, Text)
scenarioControlSelect fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let parentKey = newWorkflowKey "parent"
    wid <- fx.wfFreshId "race-control-parent"
    let WorkflowId parentText = wid
        parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          interrupted <- pendingWorkflowStep wctx "interrupted" (\_ -> pure (Left (Interrupted {workflowId = parentText})))
          slow <- pendingWorkflowStep wctx "slow" (\_ -> threadDelay 30000000 >> pure (Right (1 :: Int)))
          selectStep
            wctx
            [ SelectArm "interrupted" interrupted (\outcome -> pure (outcome >>= \value -> Right value)),
              SelectArm "slow" slow (\outcome -> pure (outcome >>= \value -> Right value))
            ]
    parentReg <- registerDBOSWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec parentKey wid Nothing
    steps <- fx.wfListSteps wid
    row <- fx.wfReadRow wid
    pure (ran, steps, (.workflowRecordStatus) <$> row, parentText)

-- | A select step races a never-finishing step against a child's result:
-- the await wins, and every branch is built before the race, so the ids
-- follow source order — the losing step claims 1 without a row, the
-- await 2, and the race itself 3. Returns the winner's value, the
-- parent's steps, and the derived child id.
scenarioSelectStepRaces ::
  forall m.
  (MonadAsync m, MonadDelay m, MonadFork m, MonadMask m, MonadMVar m, MonadSTM m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Int, [StepRecord], Text)
scenarioSelectStepRaces fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 7)
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    -- Never finishes, so the await wins however long the child's row
    -- takes to settle.
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault Nothing
          case started of
            Left err -> pure (Left err)
            Right childHandle -> do
              slow <- pendingWorkflowStep wctx "slow" (\_ -> threadDelay 30000000 >> pure (Right (0 :: Int)))
              awaited <- pendingAwait wctx childHandle
              selectStep
                wctx
                [ SelectArm "slow" slow (\outcome -> pure (outcome >>= \value -> Right value)),
                  SelectArm "DBOS.getResult" awaited $ \outcome ->
                    pure $ case outcome of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left _ -> Left (StepFailed "parent" "bad child output")
                      Right Nothing -> Left (StepFailed "parent" "no child output")
                ]
    parentReg <- registerDBOSWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "race-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec parentKey wid Nothing
    decoded <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the race's winner, got: " <> show other))
    steps <- fx.wfListSteps wid
    pure (decoded, steps, childText)

-- | A run claims its start and its await together, so each await sits
-- immediately behind its own start and a replay rebuilds the same pairs
-- however the children interleave. Returns the summed result, the
-- parent's steps, and the parent id.
scenarioStepIdPairs ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Int, [StepRecord], Text)
scenarioStepIdPairs fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "parent"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody n _ = pure (Right n)
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          let pair n = do
                (startedPair :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
                  startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowValue (n :: Int)))
                case startedPair of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitChild wctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          a <- pair 1
          b <- pair 2
          c <- pair 3
          pure $ case (a, b, c) of
            (Right x, Right y, Right z) -> Right (x + y + z)
            _ -> Left (StepFailed "parent" "a started child failed")
    parentReg <- registerDBOSWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "pairs-parent"
    let WorkflowId parentText = wid
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec parentKey wid Nothing
    decoded <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the summed children, got: " <> show other))
    steps <- fx.wfListSteps wid
    pure (decoded, steps, parentText)

-- | Awaiting a child inside a step is covered by that step: the parent
-- history holds the child start and the enclosing @collect@ step whose
-- output is the child's value — no @DBOS.getResult@ checkpoint of its
-- own. Returns the enclosing step's value, the parent's steps, and the
-- derived child id.
scenarioAwaitInsideStep ::
  forall m.
  (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Int, [StepRecord], Text)
scenarioAwaitInsideStep fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 41)
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault Nothing
          case started of
            Left err -> pure (Left err)
            Right wfHandle ->
              runWorkflowStepWith stepOptionsDefault wctx "collect" $ \_sctx -> do
                awaited <- awaitChild wctx wfHandle
                pure $ case awaited of
                  Left err -> Left err
                  Right (Just stored) ->
                    case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                      Right value -> Right value
                      Left err -> Left (StepFailed "collect" (Text.pack (show err)))
                  Right Nothing -> Left (StepFailed "collect" "no child output")
    parentReg <- registerDBOSWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "await-step-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runDBOSWorkflow exec parentKey wid Nothing
    decoded <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the enclosing step's value, got: " <> show other))
    steps <- fx.wfListSteps wid
    pure (decoded, steps, childText)

-- | An await recorded at the position the parent is about to reach,
-- naming a workflow that is not the one it holds a handle to, is refused
-- rather than consumed: the parent ends on the engine's
-- @UnexpectedStep@. The parent runs on a fork and parks on a gate after
-- its start step, so the planted row lands while the run owns the id —
-- the same sequence on both stacks (cooperative in sim). Returns the
-- settled run and the derived child id the refusal must name.
scenarioStaleAwaitRefused ::
  forall m.
  (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  WfFixture m ->
  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Text)
scenarioStaleAwaitRefused fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newWorkflowKey "child"
        parentKey = newWorkflowKey "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 1)
    childRefE <- registerDBOSWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    gate <- newEmptyMVar
    entered <- newEmptyMVar
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault Nothing
          case started of
            Left err -> pure (Left err)
            Right wfHandle -> do
              putMVar entered ()
              takeMVar gate
              awaited <- awaitChild wctx wfHandle
              pure $ case awaited of
                Left err -> Left err
                Right (Just stored) ->
                  case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                    Right value -> Right value
                    Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                Right Nothing -> Left (StepFailed "parent" "no child output")
    parentReg <- registerDBOSWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "await-wrong"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    worker <-
      async
        ( runDBOSWorkflow exec parentKey wid Nothing ::
            m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
        )
    enteredOk <- timeout 15000000 (takeMVar entered)
    case enteredOk of
      Nothing -> throwIO (userError "the parent never recorded its start")
      Just () -> pure ()
    planted <-
      runSystemDB fx.wfSystemDB $ \db ->
        SystemDB.recordChildResult
          db
          wid
          1
          (WorkflowId "somebody-elses-workflow")
          (SystemDB.OutcomeOutput (Just "7"))
          Nothing
          Nothing
    case planted of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    putMVar gate ()
    outcome <- timeout 15000000 (wait worker)
    case outcome of
      Just ran -> pure (ran, childText)
      Nothing -> throwIO (userError "the stale-await refusal never returned")

-- * Shared checks and the live interpreter: every converted case is one
-- line per tree over a shared scenario and a shared pure check
-- (ContextTest's @checkScopeStatus@ pattern). The sim tree mirrors each
-- line with its own one-line interpreter over the sim fixture plus
-- @runSimCase@; the pair diff then proves the trees match
-- name-for-name. A single imported @TestTree@ cannot cover both
-- backends: tasty leaves are @IO@, while sim execution is rank-2
-- (@forall s. IOSim s a@) with trace printing on top.

-- | One live leaf: build the fixture over the FastLogger tracer, drive
-- the shared scenario, judge by the shared check.
liveCase :: IO Postgres.PostgresSystemDB -> IO (SomeTracer IO) -> String -> (WfFixture IO -> IO a) -> (a -> Either String ()) -> TestTree
liveCase getBackend getTracer name scen check = testCase name (liveWfFixture getBackend getTracer >>= scen >>= either fail pure . check)

-- | The shared verdicts both trees assert. Pure so either runner can own
-- the failure; messages match the assertions they replace.
checkRegisteredResult :: (Int, WorkflowRecord) -> Either String ()
checkRegisteredResult (n, row)
  | n /= 42 = Left ("expected the doubled result 42, got: " <> show n)
  | row.workflowRecordStatus /= Success = Left ("expected the row SUCCESS, got: " <> show row.workflowRecordStatus)
  | row.workflowRecordName /= Just "double" = Left ("expected the workflow name double, got: " <> show row.workflowRecordName)
  | row.workflowRecordOutput /= Just "42" = Left ("expected the output 42, got: " <> show row.workflowRecordOutput)
  | row.workflowRecordInput /= Just "21" = Left ("expected the input 21, got: " <> show row.workflowRecordInput)
  | row.workflowRecordSerialization /= Just "rust_serde" = Left ("expected rust_serde, got: " <> show row.workflowRecordSerialization)
  | otherwise = Right ()

-- | One id, one execution: both callers read the run and the handles
-- name the same workflow.
checkJoinTakesId :: JoinOutcome -> Either String ()
checkJoinTakesId out
  | out.joinFirst /= 7 = Left ("expected the first caller to read 7, got: " <> show out.joinFirst)
  | out.joinSecond /= 7 = Left ("expected the joining caller to read 7, got: " <> show out.joinSecond)
  | out.joinEntered /= 1 = Left ("expected one execution, got: " <> show out.joinEntered)
  | out.joinRowStatus /= Success = Left ("expected the row SUCCESS, got: " <> show out.joinRowStatus)
  | out.joinFirstId /= out.joinSecondId = Left ("expected both handles to name one workflow, got: " <> show (out.joinFirstId, out.joinSecondId))
  | out.joinFirstPending /= Pending = Left ("expected the started row PENDING, got: " <> show out.joinFirstPending)
  | otherwise = Right ()

-- | A fresh start runs local; joins and retrieves poll.
checkFreshJoinPolls :: (Text, Text, Text, Int) -> Either String ()
checkFreshJoinPolls (firstLabel, joinLabel, retrieveLabel, n)
  | firstLabel /= "local" = Left ("expected a local handle for the fresh start, got: " <> show firstLabel)
  | joinLabel /= "polling" = Left ("expected a polling handle for the join, got: " <> show joinLabel)
  | retrieveLabel /= "polling" = Left ("expected a polling handle from retrieve, got: " <> show retrieveLabel)
  | n /= 7 = Left ("expected the local handle to read 7, got: " <> show n)
  | otherwise = Right ()

-- | The parent history holds the start and its recorded await.
checkAwaitRecorded :: (Int, [StepRecord], Text) -> Either String ()
checkAwaitRecorded (n, steps, childText)
  | n /= 99 = Left ("expected the parent to read 99, got: " <> show n)
  | otherwise = case steps of
      [ StepRecord {stepRecordStepName = startName, stepRecordChildWorkflowId = Just (WorkflowId startedChild)},
        StepRecord
          { stepRecordStepName = awaitName,
            stepRecordOutput = Just awaitOutput,
            stepRecordChildWorkflowId = Just (WorkflowId awaitedChild)
          }
        ]
        | startName /= "child" -> Left ("expected the start step, got: " <> show startName)
        | startedChild /= childText -> Left ("expected the start to name the child, got: " <> show startedChild)
        | awaitName /= "DBOS.getResult" -> Left ("expected the recorded await, got: " <> show awaitName)
        | awaitOutput /= "99" -> Left ("expected the awaited output 99, got: " <> show awaitOutput)
        | awaitedChild /= childText -> Left ("expected the await to name the child, got: " <> show awaitedChild)
        | otherwise -> Right ()
      other -> Left ("expected the start and the recorded await, got: " <> show other)

-- | The refusal names the await it was reaching for and the outcome it
-- found instead.
checkStaleAwaitRefused :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Text) -> Either String ()
checkStaleAwaitRefused (ran, childText) = case ran of
  Left (ErrorSystemDatabase (SystemDB.UnexpectedStep {stepId, expected, recorded}))
    | stepId /= 1 -> Left ("expected the refusal at step 1, got: " <> show stepId)
    | not (childText `Text.isInfixOf` expected) ->
        Left ("expected the await of " <> Text.unpack childText <> " in " <> Text.unpack expected)
    | not ("somebody-elses-workflow" `Text.isInfixOf` recorded) ->
        Left ("expected the planted workflow in " <> Text.unpack recorded)
    | otherwise -> Right ()
  other -> Left ("expected the stale-await refusal, got: " <> show other)

-- | The enclosing step carries the child's value; no separate await
-- checkpoint exists beside it.
checkAwaitInsideStep :: (Int, [StepRecord], Text) -> Either String ()
checkAwaitInsideStep (n, steps, childText)
  | n /= 41 = Left ("expected the enclosing step to carry 41, got: " <> show n)
  | otherwise = case steps of
      [ StepRecord {stepRecordStepName = startName, stepRecordChildWorkflowId = Just (WorkflowId startedChild)},
        StepRecord {stepRecordStepName = collectName, stepRecordOutput = Just collectOutput}
        ]
        | startName /= "child" -> Left ("expected the start step, got: " <> show startName)
        | startedChild /= childText -> Left ("expected the start to name the child, got: " <> show startedChild)
        | collectName /= "collect" -> Left ("expected the enclosing step, got: " <> show collectName)
        | collectOutput /= "41" -> Left ("expected the enclosing step's output 41, got: " <> show collectOutput)
        | otherwise -> Right ()
      other -> Left ("expected the start and the enclosing step only, got: " <> show other)

-- | Starts claim ids in build order, then the awaits follow in the same
-- order; the first-built child is the one that returned 1.
checkChildIdsInBuildOrder :: (Int, [StepRecord], Text, Maybe Text) -> Either String ()
checkChildIdsInBuildOrder (n, steps, parentText, firstChildOutput)
  | n /= 6 = Left ("expected 1 + 2 + 3, got: " <> show n)
  | firstChildOutput /= Just "1" = Left ("expected the first-built child to have returned 1, got: " <> show firstChildOutput)
  | table /= expected = Left ("expected the three starts and their awaits, got: " <> show table)
  | otherwise = Right ()
  where
    table = map (\row -> (row.stepRecordStepId, row.stepRecordStepName, row.stepRecordChildWorkflowId)) steps
    child k = WorkflowId (parentText <> "-" <> Text.pack (show k))
    expected =
      [ (0, "child", Just (child 0)),
        (1, "child", Just (child 1)),
        (2, "child", Just (child 2)),
        (3, "DBOS.getResult", Just (child 0)),
        (4, "DBOS.getResult", Just (child 1)),
        (5, "DBOS.getResult", Just (child 2))
      ]

-- | The refusal names the call; nothing was written for the refused
-- start, and the parent recorded no step.
checkWrongInstance :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, [StepRecord], Text) -> Either String ()
checkWrongInstance (ran, missing, steps, _childText) = case ran of
  Left (WrongInstance {operation})
    | not ("workflow" `Text.isInfixOf` operation) -> Left ("expected the call named in " <> Text.unpack operation)
    | missing /= Nothing -> Left ("expected no child written, got: " <> show missing)
    | not (null steps) -> Left ("expected no start on the parent, got: " <> show steps)
    | otherwise -> Right ()
  other -> Left ("expected a wrong-instance refusal, got: " <> show other)

-- | Two steps compose to 44 and list in order.
checkStepsTaken :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord]) -> Either String ()
checkStepsTaken (ran, steps) = case ran of
  Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
    Right 44 -> case map (.stepRecordStepName) steps of
      ["one", "two"] -> Right ()
      other -> Left ("expected two steps in order, got: " <> show other)
    other -> Left ("expected two steps to compose to 44, got: " <> show other)
  other -> Left ("expected the workflow to run, got: " <> show other)

-- | The row stayed @PENDING@ through the shutdown.
checkShutdownCancels :: (Maybe WorkflowStatus, Maybe WorkflowStatus) -> Either String ()
checkShutdownCancels (before, after)
  | before /= Just Pending = Left ("expected the gated row PENDING, got: " <> show before)
  | after /= Just Pending = Left ("expected the row PENDING after shutdown, got: " <> show after)
  | otherwise = Right ()

-- | The gated row stayed pending while the caller was gone, and the run
-- still finished once released.
checkDropFuture :: (Maybe WorkflowStatus, Either (Error EngineOnly) AwaitedOutcome) -> Either String ()
checkDropFuture (gated, settled)
  | gated /= Just Pending = Left ("expected the gated row PENDING, got: " <> show gated)
  | otherwise = case settled of
      Right (AwaitedSucceeded (Just output) serialization) -> do
        let stored = SerializedWorkflowValue output (Serialization <$> serialization)
            decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
        if decoded /= Right 7
          then Left ("expected the dropped run to record 7, got: " <> show decoded)
          else Right ()
      other -> Left ("expected the dropped run to finish, got: " <> show other)

-- | The parent carries the tenant; the child inherits nothing.
checkAttributes :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord, Text) -> Either String ()
checkAttributes (ran, parentRow, childRow, tenant)
  | Left err <- ran = Left ("expected the attributed run, got: " <> show err)
  | otherwise = case (parentRow, childRow) of
      (Just parent, Just child)
        | Just attributes <- parent.workflowRecordAttributes,
          tenant `Text.isInfixOf` attributes ->
            if child.workflowRecordAttributes /= Nothing
              then Left ("expected the child to inherit nothing, got: " <> show child.workflowRecordAttributes)
              else Right ()
        | otherwise -> Left ("expected the tenant in the parent's attributes, got: " <> show parent.workflowRecordAttributes)
      _ -> Left "expected both rows"

-- | The step error came back and its column holds the shortfall.
checkStepErrorRecorded :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord]) -> Either String ()
checkStepErrorRecorded (ran, steps) = case ran of
  Left (StepFailed step message)
    | step /= "charge" -> Left ("expected the failing step, got: " <> show step)
    | message /= "short by 12" -> Left ("expected the shortfall, got: " <> show message)
    | otherwise -> case steps of
        [StepRecord {stepRecordStepName = name, stepRecordError = Just recorded}]
          | name /= "charge" -> Left ("expected the failed step, got: " <> show name)
          | not ("short by 12" `Text.isInfixOf` recorded) -> Left ("expected the shortfall in " <> Text.unpack recorded)
          | otherwise -> Right ()
        other -> Left ("expected the failed step, got: " <> show other)
  other -> Left ("expected the step error back, got: " <> show other)

-- | The body's own failure came back unchanged.
checkAppErrorRoundtrip :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue) -> Either String ()
checkAppErrorRoundtrip ran = case ran of
  Left (StepFailed step message)
    | step /= "flaky" -> Left ("expected the failing step name, got: " <> show step)
    | message /= "boom" -> Left ("expected the failing step message, got: " <> show message)
    | otherwise -> Right ()
  other -> Left ("expected the application error back, got: " <> show other)

-- | The backend failure came back as itself and the row stays pending
-- with no error column.
checkDbFailureNotOutcome :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord) -> Either String ()
checkDbFailureNotOutcome (ran, row) = case ran of
  Left (ErrorSystemDatabase _) -> case row of
    Just found
      | found.workflowRecordStatus /= Pending -> Left ("expected the row PENDING, got: " <> show found.workflowRecordStatus)
      | found.workflowRecordError /= Nothing -> Left ("expected no recorded error, got: " <> show found.workflowRecordError)
      | otherwise -> Right ()
    Nothing -> Left "expected the failing workflow's row"
  other -> Left ("expected the database failure back, got: " <> show other)

-- | The body's exception escaped and the row stays @PENDING@ with no
-- recorded error.
checkPanic :: (Either SomeException (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)), Maybe WorkflowRecord) -> Either String ()
checkPanic (outcome, row)
  | Right other <- outcome = Left ("expected the body's exception to escape, got: " <> show other)
  | otherwise = case row of
      Just found
        | found.workflowRecordStatus /= Pending -> Left ("expected the row PENDING, got: " <> show found.workflowRecordStatus)
        | found.workflowRecordError /= Nothing -> Left ("expected no recorded error, got: " <> show found.workflowRecordError)
        | otherwise -> Right ()
      Nothing -> Left "expected the panicking workflow's row"

-- | The unlaunched run was refused by name.
checkRunBeforeLaunch :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue) -> Either String ()
checkRunBeforeLaunch ran = case ran of
  Left ErrorNotLaunched {} -> Right ()
  other -> Left ("expected a not-launched refusal, got: " <> show other)

-- | The zero-argument run recorded no input.
checkZeroNoInput :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord) -> Either String ()
checkZeroNoInput (ran, row)
  | Left err <- ran = Left ("expected the workflow to run, got: " <> show err)
  | otherwise = case row of
      Just record
        | record.workflowRecordInput /= Nothing -> Left ("expected no recorded input, got: " <> show record.workflowRecordInput)
        | otherwise -> Right ()
      Nothing -> Left "expected the zero-argument row"

-- | The body saw its own row: the run decodes to True.
checkRowBeforeBody :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue) -> Either String ()
checkRowBeforeBody ran = case ran of
  Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Bool of
    Right True -> Right ()
    other -> Left ("expected the body to find its own row, got: " <> show other)
  other -> Left ("expected the workflow to run, got: " <> show other)

-- | The refusal names the child start it wanted and the plain step it
-- found, and nothing was created to be orphaned.
checkPlainStepAtStart :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord) -> Either String ()
checkPlainStepAtStart (ran, missing) = case ran of
  Left (ErrorSystemDatabase (SystemDB.UnexpectedStep {stepId, expected, recorded}))
    | stepId /= 0 -> Left ("expected the refusal at step 0, got: " <> show stepId)
    | not ("child workflow start" `Text.isInfixOf` expected) -> Left ("expected the wanted start in " <> Text.unpack expected)
    | not ("plain step" `Text.isInfixOf` recorded) -> Left ("expected the plain step in " <> Text.unpack recorded)
    | missing /= Nothing -> Left ("expected nothing created to be orphaned, got: " <> show missing)
    | otherwise -> Right ()
  other -> Left ("expected the unexpected-step refusal, got: " <> show other)

-- | The child ran under its derived id and the replay adopted the
-- recorded child id rather than starting another.
checkDerivedChildAdopted :: (Text, [WorkflowId], Either (Error EngineOnly) [WorkflowId], Either (Error EngineOnly) AwaitedOutcome, Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) -> Either String ()
checkDerivedChildAdopted (childText, recovered, dequeued, settled, replayed)
  -- The child start detaches the child onto the executor, so it may finish
  -- before the shutdown: recovery then finds nothing pending and the
  -- dequeue nothing enqueued. Either way the outcome and the adoption are
  -- the contract.
  | recovered /= [] && recovered /= [WorkflowId childText] = Left ("unexpected recovery result: " <> show recovered)
  | dequeued /= Right [] && dequeued /= Right [WorkflowId childText] = Left ("unexpected dequeue result: " <> show dequeued)
  | otherwise = case settled of
      Right (AwaitedSucceeded (Just raw) serialization) -> do
        let stored = SerializedWorkflowValue raw (Serialization <$> serialization)
            decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
        if decoded /= Right 42
          then Left ("expected recovery to run the recorded child, got: " <> show decoded)
          else case replayed of
            Right (Just stored') -> case decodeWorkflowValue "result" (Just stored') :: Either CodecError Text of
              Right adopted
                | adopted == childText -> Right ()
                | otherwise -> Left ("expected the replay to adopt " <> Text.unpack childText <> ", got: " <> Text.unpack adopted)
              Left err -> Left ("expected the replayed parent's id, got: " <> show err)
            other -> Left ("expected the replay to adopt the recorded child, got: " <> show other)
      other -> Left ("expected the recovered child to settle, got: " <> show other)

-- | The assigned child ran under its chosen id; the derived row never
-- existed and the replay adopted the chosen id.
checkAssignedChildAdopted :: (Text, [WorkflowId], Either (Error EngineOnly) [WorkflowId], Either (Error EngineOnly) AwaitedOutcome, Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord) -> Either String ()
checkAssignedChildAdopted (chosenText, recovered, dequeued, settled, replayed, chosenRow, derivedRow)
  | recovered /= [] && recovered /= [WorkflowId chosenText] = Left ("unexpected recovery result: " <> show recovered)
  | dequeued /= Right [] && dequeued /= Right [WorkflowId chosenText] = Left ("unexpected dequeue result: " <> show dequeued)
  | derivedRow /= Nothing = Left ("expected no derived row, got: " <> show derivedRow)
  | otherwise = case settled of
      Right (AwaitedSucceeded (Just raw) serialization) -> do
        let stored = SerializedWorkflowValue raw (Serialization <$> serialization)
            decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
        if decoded /= Right 7
          then Left ("expected the assigned child to record 7, got: " <> show decoded)
          else case chosenRow of
            Just row
              | row.workflowRecordParentWorkflowId /= Just (WorkflowId (Text.dropEnd (Text.length "-chosen") chosenText)) ->
                  Left ("expected the chosen row to link its parent, got: " <> show row.workflowRecordParentWorkflowId)
              | otherwise -> case replayed of
                  Right (Just stored') -> case decodeWorkflowValue "result" (Just stored') :: Either CodecError Text of
                    Right adopted
                      | adopted == chosenText -> Right ()
                      | otherwise -> Left ("expected the replay to adopt " <> Text.unpack chosenText <> ", got: " <> Text.unpack adopted)
                    Left err -> Left ("expected the replayed parent's id, got: " <> show err)
                  other -> Left ("expected the replay to adopt the recorded child, got: " <> show other)
            Nothing -> Left "expected the chosen row"
      other -> Left ("expected the recovered child to settle, got: " <> show other)

-- | The root ran and its row links no parent.
checkRootNoParent :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord) -> Either String ()
checkRootNoParent (ran, row)
  | Left err <- ran = Left ("expected the workflow to run, got: " <> show err)
  | otherwise = case row of
      Just found
        | found.workflowRecordParentWorkflowId /= Nothing ->
            Left ("expected no parent link, got: " <> show found.workflowRecordParentWorkflowId)
        | otherwise -> Right ()
      Nothing -> Left "expected the root row"

-- | The fan-out summed 0 + 1 + 2 with three children listed, all three
-- overlaps close to one delay rather than three.
checkFanout :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [WorkflowId], Int64) -> Either String ()
checkFanout (ran, children, tookMs)
  | Right (Just stored) <- ran, Right 3 == (decodeWorkflowValue "result" (Just stored) :: Either CodecError Int) =
      if length children /= 3
        then Left ("expected three children, got: " <> show children)
        else if tookMs >= 1200
          then Left ("three 400 ms children took " <> show tookMs <> " ms, which is serial rather than concurrent")
          else Right ()
  | otherwise = Left ("expected the fan-out total, got: " <> show ran)

-- | The parent ran and recorded the lone start; the abandoned child
-- still finished with 42, carries the parent link, and lists as the
-- parent's child.
checkUnawaitedChild ::
  (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord], Either (Error EngineOnly) AwaitedOutcome, Maybe WorkflowRecord, [WorkflowId], Text) ->
  Either String ()
checkUnawaitedChild (ran, steps, found, childRow, children, parentText)
  | Right _ <- ran = case steps of
      [StepRecord {stepRecordChildWorkflowId = Just (WorkflowId recorded)}]
        | recorded /= childText -> Left ("expected the lone start step to name " <> Text.unpack childText <> ", got: " <> show recorded)
        | otherwise -> case found of
            Right (AwaitedSucceeded (Just output) serialization) -> do
              let stored = SerializedWorkflowValue output (Serialization <$> serialization)
                  decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              if decoded /= Right 42
                then Left ("expected the abandoned child to record 42, got: " <> show decoded)
                else case childRow of
                  Just row
                    | row.workflowRecordParentWorkflowId /= Just (WorkflowId parentText) ->
                        Left ("expected the child to link its parent, got: " <> show row.workflowRecordParentWorkflowId)
                    | children /= [WorkflowId childText] ->
                        Left ("expected the child listed under its parent, got: " <> show children)
                    | otherwise -> Right ()
                  Nothing -> Left "expected the child row"
            other -> Left ("expected the abandoned child to finish, got: " <> show other)
      other -> Left ("expected the lone start step, got: " <> show other)
  | otherwise = Left ("expected the parent to run, got: " <> show ran)
  where
    childText = parentText <> "-0"

-- | The parent reported the refused child and the child's own error sits
-- in its column, in the child's channel rather than the parent's.
checkLiftChildError :: (Either (Error GaveUp) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord) -> Either String ()
checkLiftChildError (ran, childRow)
  | Right (Just stored) <- ran, Right True == (decodeWorkflowValue "result" (Just stored) :: Either CodecError Bool) =
      case childRow of
        Just row
          | row.workflowRecordStatus /= Error -> Left ("expected the child row Error, got: " <> show row.workflowRecordStatus)
          | otherwise -> case row.workflowRecordError of
              Just recorded -> case decodeErrorText recorded :: Either Text (Error Refused) of
                Right (Application Refused) -> Right ()
                other -> Left ("expected the child's own error in the column, got: " <> show other)
              Nothing -> Left "the child recorded no error"
        Nothing -> Left "expected the child row"
  | otherwise = Left ("expected the parent to report the child's refusal, got: " <> show ran)

-- | The engine refused the in-step start and nothing was recorded.
checkChildInsideStepRefused :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord]) -> Either String ()
checkChildInsideStepRefused (result, steps)
  | Left (InsideStep operation) <- result = if operation == "starting a workflow"
      then if null steps
        then Right ()
        else Left ("expected no recorded start, got: " <> show steps)
      else Left ("expected the leaf refusal, got: " <> show operation)
  | otherwise = Left ("expected the leaf refusal, got: " <> show result)

-- | The captured-parent start is refused with the leaf error and records
-- nothing: same verdict as the in-step start, through the depth rather
-- than the scope field.
checkCaptureChildRefused :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord]) -> Either String ()
checkCaptureChildRefused (result, steps)
  | Left (InsideStep operation) <- result = if operation == "starting a workflow"
      then if null steps
        then Right ()
        else Left ("expected no recorded start, got: " <> show steps)
      else Left ("expected the leaf refusal, got: " <> show operation)
  | otherwise = Left ("expected the leaf refusal, got: " <> show result)

-- | The parent and the child each ended cancelled by the inherited
-- deadline, and the interrupted await left only the child start recorded.
checkCascadeDeadline :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Either (Error EngineOnly) AwaitedOutcome, [StepRecord]) -> Either String ()
checkCascadeDeadline (ran, childOutcome, steps)
  | not (parentCancelled ran) = Left ("expected the parent's own deadline cancellation, got: " <> show ran)
  | childOutcome /= Right AwaitedCancelled = Left ("expected the child cancelled independently, got: " <> show childOutcome)
  | map (.stepRecordStepName) steps /= ["child"] = Left ("expected the lone start only, got: " <> show steps)
  | otherwise = Right ()
  where
    parentCancelled result = case result of
      Left (ErrorSystemDatabase (SystemDB.WorkflowCancelled {})) -> True
      _ -> False

-- | Both children ran; the silent one inherited the parent's exact
-- instant, the declining one carries neither deadline nor timeout.
checkDeclinedDeadline :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord, Maybe WorkflowRecord) -> Either String ()
checkDeclinedDeadline (ran, parentRow, inheritedRow, detachedRow)
  | Right (Just stored) <- ran, Right 2 == (decodeWorkflowValue "result" (Just stored) :: Either CodecError Int) = case (parentRow, inheritedRow, detachedRow) of
      (Just parent, Just inheritedChild, Just detachedChild) -> case parent.workflowRecordDeadline of
        Nothing -> Left "the parent has no deadline"
        Just parentDeadline
          | inheritedChild.workflowRecordDeadline /= Just parentDeadline ->
              Left ("expected silence to inherit the parent's instant verbatim, got: " <> show inheritedChild.workflowRecordDeadline)
          | detachedChild.workflowRecordDeadline /= Nothing ->
              Left ("expected the declining child to carry no deadline, got: " <> show detachedChild.workflowRecordDeadline)
          | detachedChild.workflowRecordTimeout /= Nothing ->
              Left ("expected the declining child to carry no timeout, got: " <> show detachedChild.workflowRecordTimeout)
          | otherwise -> Right ()
      _ -> Left ("expected all three rows, got: " <> show (parentRow, inheritedRow, detachedRow))
  | otherwise = Left ("expected the parent's output of both children, got: " <> show ran)

-- | The child's own budget won: it records its own timeout and a
-- deadline that outlives its parent's.
checkChildBudgetWins :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord) -> Either String ()
checkChildBudgetWins (ran, parentRow, childRow)
  | Right _ <- ran = case (parentRow, childRow) of
      (Just parent, Just child) -> case (parent.workflowRecordDeadline, child.workflowRecordDeadline) of
        (Just parentDeadline, Just childDeadline)
          | SystemDB.timestampToEpochMs childDeadline <= SystemDB.timestampToEpochMs parentDeadline ->
              Left "expected the child's deadline to outlive its parent's"
          | child.workflowRecordTimeout /= Just (secondsDuration 3600) ->
              Left ("expected the child's own timeout, got: " <> show child.workflowRecordTimeout)
          | otherwise -> Right ()
        other -> Left ("expected both deadlines, got: " <> show other)
      _ -> Left ("expected both rows, got: " <> show (parentRow, childRow))
  | otherwise = Left ("expected the parent to run, got: " <> show ran)

-- | The child carries the parent's exact deadline instant, not a fresh
-- budget; only the parent's row records the timeout it came from.
checkDeadlineInherited :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord) -> Either String ()
checkDeadlineInherited (ran, parentRow, childRow)
  | Right _ <- ran = case (parentRow, childRow) of
      (Just parent, Just child) -> case parent.workflowRecordDeadline of
        Nothing -> Left "the parent has no deadline"
        Just deadline'
          | child.workflowRecordDeadline /= Just deadline' -> Left ("expected the same instant, not a fresh budget, in " <> show child.workflowRecordDeadline)
          | child.workflowRecordTimeout /= Nothing -> Left ("expected the child to record no timeout, got: " <> show child.workflowRecordTimeout)
          | parent.workflowRecordTimeout /= Just (secondsDuration 300) -> Left ("expected the parent's own timeout, got: " <> show parent.workflowRecordTimeout)
          | otherwise -> Right ()
      _ -> Left ("expected both rows, got: " <> show (parentRow, childRow))
  | otherwise = Left ("expected the parent to run, got: " <> show ran)

-- | The parent ends on the child's cancellation, which is also what its
-- recorded await column holds; the parent row failed and the child row
-- is cancelled.
checkCancelledChildAwaited ::
  (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord], Maybe WorkflowStatus, Maybe WorkflowStatus, Text) ->
  Either String ()
checkCancelledChildAwaited (ran, steps, parentStatus, childStatus, childText)
  | not (namesChild ran) = Left ("expected an awaited cancellation of " <> Text.unpack childText <> ", got: " <> show ran)
  | not (recordsChild steps) = Left ("expected a recorded awaited cancellation of " <> Text.unpack childText <> ", got: " <> show steps)
  | parentStatus /= Just Error = Left ("expected the failed parent row, got: " <> show parentStatus)
  | childStatus /= Just Cancelled = Left ("expected the cancelled child row, got: " <> show childStatus)
  | otherwise = Right ()
  where
    namesChild result = case result of
      Left (AwaitedWorkflowCancelled {workflowId}) -> workflowId == childText
      _ -> False
    recordsChild rows = case [row | row <- rows, row.stepRecordStepName == "DBOS.getResult"] of
      [awaitRow] -> case awaitRow.stepRecordError of
        Just recorded -> case decodeErrorText recorded :: Either Text (Error EngineOnly) of
          Right (AwaitedWorkflowCancelled {workflowId}) -> workflowId == childText
          _ -> False
        Nothing -> False
      _ -> False

-- | The fast step won and the losing step's token fired for its watcher.
checkLosingTokenFired :: (Int, Bool) -> Either String ()
checkLosingTokenFired (n, fired)
  | n /= 1 = Left ("expected the fast step's 1, got: " <> show n)
  | not fired = Left "the losing step's token never fired"
  | otherwise = Right ()

-- | The interrupted arm won: the control signal comes back, no step row
-- exists, and the row stays @PENDING@ for a later recovery.
checkControlSelect :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord], Maybe WorkflowStatus, Text) -> Either String ()
checkControlSelect (ran, steps, status, parentText)
  | Left (Interrupted {workflowId}) <- ran, workflowId == parentText = case steps of
      [] -> if status == Just Pending
        then Right ()
        else Left ("expected the parent row PENDING, got: " <> show status)
      other -> Left ("expected no step rows, got: " <> show other)
  | otherwise = Left ("expected the control signal back, got: " <> show ran)

-- | The await arm won and produced the child's value; the losing step
-- left no row, so the history is the start, the await, and the select.
checkSelectStepRaces :: (Int, [StepRecord], Text) -> Either String ()
checkSelectStepRaces (n, steps, childText)
  | n /= 7 = Left ("expected the await arm's 7, got: " <> show n)
  | table /= expected = Left ("expected the start, the await and the select, got: " <> show table)
  | otherwise = Right ()
  where
    table = map (\row -> (row.stepRecordStepId, row.stepRecordStepName, row.stepRecordChildWorkflowId)) steps
    expected =
      [ (0, "child", Just (WorkflowId childText)),
        -- Id 1 is the losing step, built and dropped without a row.
        (2, "DBOS.getResult", Just (WorkflowId childText)),
        (3, "DBOS.selectStep", Nothing)
      ]

-- | Each await sits immediately behind its own start: the pairs are
-- (0,1), (2,3), (4,5), naming the child derived at the start's position.
checkStepIdPairs :: (Int, [StepRecord], Text) -> Either String ()
checkStepIdPairs (n, steps, parentText)
  | n /= 6 = Left ("expected 1 + 2 + 3, got: " <> show n)
  | table /= expected = Left ("expected each await behind its own start, got: " <> show table)
  | otherwise = Right ()
  where
    table = map (\row -> (row.stepRecordStepId, row.stepRecordStepName, row.stepRecordChildWorkflowId)) steps
    child k = WorkflowId (parentText <> "-" <> Text.pack (show k))
    expected =
      [ (0, "child", Just (child 0)),
        (1, "DBOS.getResult", Just (child 0)),
        (2, "child", Just (child 2)),
        (3, "DBOS.getResult", Just (child 2)),
        (4, "child", Just (child 4)),
        (5, "DBOS.getResult", Just (child 4))
      ]

-- | What every shared task case needs from io-classes: a constraint
-- synonym, not a class — the bodies stay ordinary functions, and the one
-- stack-specific operation arrives as an argument.
type TaskCase m =
  (MonadFork m, MonadMask m, MonadSTM m, MonadMVar m, MonadDelay m)

-- | Block until a spawned thread has finished. The registry's departure
-- hook runs before the thread ends, so waiting on the thread turns "has
-- this task departed?" from a bet on a delay — the bet a fixed sleep lost
-- under load — into an observation. Bounded like 'waitFor' in
-- "DBOS.Transact.ContextTest": a waiter that never wakes fails the case
-- instead of hanging the suite.
waitFinished :: ThreadId IO -> IO ()
waitFinished tid = do
  settled <- timeout 5000000 (pollFinished tid)
  case settled of
    Just () -> pure ()
    Nothing -> fail ("the spawned task never finished: " <> show tid)

-- | Poll a thread's status until it has ended.
pollFinished :: ThreadId IO -> IO ()
pollFinished tid = do
  status <- threadStatus tid
  case status of
    ThreadFinished -> pure ()
    ThreadDied -> pure ()
    _ -> threadDelay 500 >> pollFinished tid

-- | The IOSim half of the departure wait: nothing to observe, because the
-- simulator advances time only when no thread is runnable — a parent
-- parked in a tick cannot resume before a self-terminating child has run
-- to completion, its departure commit included.
simWaitDeparture :: forall s. ThreadId (IOSim s) -> IOSim s ()
simWaitDeparture _ = threadDelay 1000

-- | No swept task may be miscounted as aborted: any nonzero sweep count
-- fails the case. Shared with the sim tree, which judges its leaves by
-- this same assertion.
checkNoMiscounts :: [Int] -> IO ()
checkNoMiscounts counts = case [count | count <- counts, count /= 0] of
  [] -> pure ()
  miscounts -> fail ("dead tasks swept as aborted: " <> show (length miscounts))

-- | Two parked tasks: the sweep must count both. The parks are never
-- waited out — the sweep kills the sleepers — but the margin is what keeps
-- a parent descheduled between the forks and the sweep from finding the
-- tasks finished on their own. The simulator is immune either way: time
-- advances only when nothing is runnable.
taskAbortAllWaits :: TaskCase m => m Int
taskAbortAllWaits = do
  tasks <- newTasks
  _ <- spawnTracked tasks (threadDelay 10000000)
  _ <- spawnTracked tasks (threadDelay 10000000)
  abortAll tasks

-- | A trivial body, awaited past its departure, leaves the sweep nothing
-- to kill.
taskFinishedNotRegistered ::
  (MonadFork m, MonadMask m, MonadSTM m, MonadMVar m) =>
  (ThreadId m -> m ()) ->
  m Int
taskFinishedNotRegistered awaitDeparture = do
  tasks <- newTasks
  spawned <- spawnTracked tasks (pure ())
  mapM_ awaitDeparture spawned
  abortAll tasks

-- | The sweep closes the registry; the arrival that follows is refused and
-- must never run.
taskRefusedAfterSweep :: TaskCase m => m Bool
taskRefusedAfterSweep = do
  tasks <- newTasks
  _ <- abortAll tasks
  ran <- newTVarIO False
  _ <- spawnTracked tasks (threadDelay 1000 >> atomically (writeTVar ran True))
  threadDelay 5000
  readTVarIO ran

taskEmptySweep :: (MonadFork m, MonadSTM m, MonadMVar m) => m Int
taskEmptySweep = newTasks >>= abortAll

-- | A trivial body can depart between the fork and the parent's
-- registration on a preemptive scheduler; the registration must consume
-- that early departure rather than list a dead thread, so a later sweep
-- finds nothing to kill and counts nothing aborted. Only the IO half can
-- reach that interleaving — the cooperative simulator never preempts a
-- forked child (io-sim's @Fork@ appends it to the runqueue and resumes the
-- parent), so there the case checks the ordinary path.
taskEarlyFinishNotSwept ::
  (MonadFork m, MonadMask m, MonadSTM m, MonadMVar m) =>
  (ThreadId m -> m ()) ->
  m [Int]
taskEarlyFinishNotSwept awaitDeparture =
  mapM
    ( \_ -> do
        tasks <- newTasks
        spawned <- spawnTracked tasks (pure ())
        mapM_ awaitDeparture spawned
        abortAll tasks
    )
    [1 .. 200 :: Int]

-- | The oracle's @Tasks@ behaviour: one body per case, shared with the
-- sim tree — the IO leaves run here under real threads and real
-- preemption (where the fork/registration race bites); the IOSim leaves
-- run in "DBOS.Transact.WorkflowTestSim" under the cooperative scheduler
-- (where the same engine code path is deterministic). The checks live
-- beside each leaf, not in the bodies: @?=@ is 'IO'-only, so a body
-- polymorphic in the stack returns a value and each tree judges it by
-- the same assertion.
tasksTests :: TestTree
tasksTests =
  testGroup
    "Tasks"
    [ testCase "abortAll waits until every task has departed" (taskAbortAllWaits @IO >>= (@?= 2)),
      testCase "a task that finished on its own is not left in the registry" (taskFinishedNotRegistered @IO waitFinished >>= (@?= 0)),
      testCase "a task arriving after the sweep is aborted on arrival" (taskRefusedAfterSweep @IO >>= (@?= False)),
      testCase "an empty sweep returns at once" (taskEmptySweep @IO >>= (@?= 0)),
      -- IO only: real preemption, not cooperation.
      testCase "a spawn refused after abort fills its channel instead of hanging" $ do
        -- Closed-arrival kill lands before the child ever runs; each
        -- iteration must fill promptly, and the per-iteration bound turns
        -- a lost fill into a failure instead of a hung suite.
        filled <- mapM (\_ -> do
            tasks <- newTasks
            _ <- abortAll tasks
            channel <- spawnLocal (tasksSpawner tasks) (\_ -> pure ())
            timeout 1000000 (readMVar channel)
          ) [1 .. 50 :: Int]
        case [() | Nothing <- filled] of
          [] -> pure ()
          missing -> fail ("refused spawns left channels empty: " <> show (length missing)),
      testCase "a task finishing before registration is not swept as aborted" (taskEarlyFinishNotSwept @IO waitFinished >>= checkNoMiscounts)
    ]

-- | Launch over the isolated environment and hand back the executor:
-- the live cases' one-call form of @launchWithEnvironment@ plus unwrap.
launchExec :: DBOS IO -> Environment -> IO (Executor IO)
launchExec dbos env = do
  started <- launchWithEnvironment dbos env
  case started of
    Left err -> fail (show err)
    Right executor -> pure executor

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through: each
-- pins the error channel to 'EngineOnly' (the channel the test bodies
-- declare), so a call site outside an annotated body does not leave it
-- for the compiler to guess. Local copies are deliberate — this module
-- carries only the aliases it uses, and a sibling test module repeats
-- the ones it needs.
runWf :: Executor IO -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWf = runDBOSWorkflow

runWfRef :: Executor IO -> WorkflowRef IO EngineOnly -> RunOptions -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWfRef = runDBOSWorkflowRef

startWfRef :: Executor IO -> WorkflowRef IO EngineOnly -> StartOptions -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
startWfRef = startDBOSWorkflowRef

retrieveWf :: DBOS IO -> WorkflowId -> IO (Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
retrieveWf = retrieveWorkflow

awaitWf :: WorkflowCtx exec IO -> WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
awaitWf = awaitChild

resultWf :: WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWf = handleResult

statusWf :: WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe WorkflowStatus))
statusWf = handleStatus

-- | Wait until a workflow row appears, so background runs are observed
-- rather than raced.
waitForRow :: DBOS IO -> WorkflowId -> IO ()
waitForRow dbos wid = go (20 :: Int)
  where
    go 0 = fail "the workflow row never appeared"
    go n = do
      retrieved <- retrieveWf dbos wid
      case retrieved of
        Left err -> fail (show err)
        Right handle -> do
          status <- statusWf handle
          case status of
            Right (Just _) -> pure ()
            Right Nothing -> threadDelay 100000 >> go (n - 1)
            Left err -> fail (show err)

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | The status a workflow row carries: a reader over the suite backend,
-- so assertions hold after shutdown. The launched instances below keep
-- their own pools: each needs a distinct application identity.
readWorkflowStatus :: IO Postgres.PostgresSystemDB -> WorkflowId -> IO (Maybe WorkflowStatus)
readWorkflowStatus getBackend wid = do
  backend <- getBackend
  found <- getWorkflow backend wid
  case found of
    Left err -> fail (show err)
    Right Nothing -> pure Nothing
    Right (Just WorkflowRecord {workflowRecordStatus = status}) -> pure (Just status)

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }

-- | The child's channel in the lift case: a nullary failure, held as
-- itself across the column.
data Refused = Refused
  deriving stock (Eq, Show)

instance ToJSON Refused where
  toJSON _ = object []

instance FromJSON Refused where
  parseJSON _ = pure Refused

-- | The parent's channel in the lift case.
data GaveUp = GaveUp
  deriving stock (Eq, Show)

instance ToJSON GaveUp where
  toJSON _ = object []

instance FromJSON GaveUp where
  parseJSON _ = pure GaveUp

-- | Somebody else's error type: 'Show' and nothing more, so it cannot
-- cross a column and is converted at the boundary.
data GatewayRefused = GatewayRefused
  deriving stock (Eq)

instance Show GatewayRefused where
  show _ = "the gateway refused the card"

-- | The workflow's own error: serializable, and what the boundary
-- conversion lands in.
data PaymentError = Gateway {reason :: Text}
  deriving stock (Eq)

instance Show PaymentError where
  show (Gateway reason) = "charging failed: " <> Text.unpack reason

instance ToJSON PaymentError where
  toJSON (Gateway reason) = object ["Gateway" .= object ["reason" .= reason]]

instance FromJSON PaymentError where
  parseJSON = withObject "PaymentError" $ \o -> do
    gateway <- o .: "Gateway"
    Gateway <$> gateway .: "reason"

-- | A foreign call whose error has no serde counterpart.
charge :: IO (Either GatewayRefused ())
charge = pure (Left GatewayRefused)
