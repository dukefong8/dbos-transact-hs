{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Workflow execution behavior through the class-backed engine
-- runner: the live tree. Shared scenario bodies and checks live in
-- 'DBOS.Transact.WorkflowCases'.
module DBOS.Transact.WorkflowTest (tests) where

import DBOS.DualStack (liveCase)
import DBOS.Prelude
import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, (.:), (.=))
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (AwaitedOutcome (..), NewWorkflow (..), Submission (..), WorkflowId (..), WorkflowRecord (..), getWorkflow, listSteps, newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  (
    application,
    EngineOnly, CodecError,
    Config (..),
    Environment (..),
    Error (..),
    RunOptions (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    StartOptions (..),
    WorkflowHandle (..),
    DBOS,
    WorkflowCtx,
    Executor,
    WorkflowStatus (..),
    awaitChild,
    configFromEnv,
    decodeWorkflowValue,
    encodeWorkflowValue,
    launchWithEnvironment,
    newDBOS,
    newWorkflowKey,
    registerWorkflowRef,
    registerWorkflow,
    WorkflowKey,
    WorkflowRef,
    runWorkflow,
    runWorkflowRef,
    runOptionsDefault,
    runStep,
    selectWorkflow,
    startChildWorkflow,
    shutdown,
    startWorkflowRef,
    startOptionsDefault,
    waitForWorkflow,
  )
import DBOS.Transact.Logger (SomeTracer (..), acquireLoggerBackend, ioTracer, nullTracer)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Error (decodeErrorText)
import DBOS.Transact.Workflow (abortAll, newTasks, tasksSpawner)
import DBOS.Transact.Connection (SomeSystemDB (..), uuidWorkflowId)
import DBOS.Transact.Context (spawnLocal)
import DBOS.SystemDB.Retry (uuidEntropy)
import DBOS.Transact.ContextTest (ctxOver)
import GHC.Conc (ThreadStatus (..), threadStatus)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))
import DBOS.Transact.WorkflowCases
  ( timeoutOptionsCase,
    WfFixture (..),
    scenarioRegisteredRecordsResult,
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
    scenarioBudgetCancels,
    scenarioWrongInstance,
    scenarioJoinHeldKey,
    scenarioEnqueuedChildReplays,
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
    checkBudgetCancels,
    checkWrongInstance,
    checkJoinHeldKey,
    checkEnqueuedChildReplays,
    taskAbortAllWaits,
    taskFinishedNotRegistered,
    taskRefusedAfterSweep,
    taskEmptySweep,
    taskEarlyFinishNotSwept,
    checkNoMiscounts,
    mkWfFixture
  )



tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
  withResource acquireLoggerBackend snd $ \getLogger ->
  testGroup
    "Workflow execution"
    [ liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a registered workflow starts and records its result" scenarioRegisteredRecordsResult checkRegisteredResult,
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
              completedStep <- runStep wctx "once" (const (modifyIORef' bodyCalls (+ 1) >> pure (value * 2)))
              case completedStep of
                Left err -> pure (Left err)
                Right result -> do
                  crash <- readIORef shouldCrash
                  if crash
                    then ioError (userError "interrupted after checkpoint")
                    else pure (Right result)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
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
          ghostRegistered <- registerWorkflowRef first ghostKey (gated enteredGhost)
          ghostRef <- case ghostRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          keeperRegistered <- registerWorkflowRef first keeperKey (gated enteredKeeper)
          keeperRef <- case keeperRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          execFirst <- launchExec first isolatedEnvironment
          -- Ghost first, so the sweep meets the skip before the recovery.
          ghostWorker <- async (runWfRef execFirst ghostRef (runOptionsDefault {runWorkflowId = Just (WorkflowId ghostText)}) Nothing)
          keeperWorker <- async (runWfRef execFirst keeperRef (runOptionsDefault {runWorkflowId = Just (WorkflowId keeperText)}) Nothing)
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
          keeperRegistered <- registerWorkflowRef second keeperKey (\() _ -> pure (Right ()) :: IO (Either (Error EngineOnly) ()))
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
      testCase "timeouts, options, and child ids compose without a database" timeoutOptionsCase,
      -- The double-click: the same id while the first run still owns it
      -- joins rather than failing — one id, one execution.
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "starting a taken id joins the existing run" scenarioJoinTakesId checkJoinTakesId,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a fresh start is local and a join polls" scenarioFreshJoinPolls checkFreshJoinPolls,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "awaiting a child is recorded as a step" scenarioAwaitRecorded checkAwaitRecorded,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a recorded await of another workflow is refused" scenarioStaleAwaitRefused checkStaleAwaitRefused,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "awaiting a child inside a step is covered by that step" scenarioAwaitInsideStep checkAwaitInsideStep,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "child starts and awaits keep their ids in build order" scenarioChildIdsInBuildOrder checkChildIdsInBuildOrder,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "runs claim their pairs of step ids adjacently" scenarioStepIdPairs checkStepIdPairs,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a select step races a step against a child's result" scenarioSelectStepRaces checkSelectStepRaces,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a scoped select races two pending steps" scenarioScopedSelect checkScopedSelect,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a converted body runs through the scoped entries" scenarioScopedBody checkScopedBody,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a control signal winning a select records no winner" scenarioControlSelect checkControlSelect,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a losing step has its cancellation token fired" scenarioLosingTokenFired checkLosingTokenFired,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a cancelled child is an awaited cancellation in the parent" scenarioCancelledChildAwaited checkCancelledChildAwaited,
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
          childRegistered <- registerWorkflowRef first childKey childBody
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
          parentRegistered <- registerWorkflowRef first parentKey parentBody
          parentRef <- case parentRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          execFirst <- launchExec first isolatedEnvironment
          _ <- startWfRef execFirst parentRef (startOptionsDefault {startWorkflowId = Just (WorkflowId parentText)}) Nothing
          reader <- getBackend
          let awaitRecorded = go (200 :: Int)
                where
                  go 0 = fail "the await was never recorded"
                  go n = do
                    rows <- listSteps reader (WorkflowId parentText) False Nothing Nothing Nothing
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
          childRegistered <- registerWorkflowRef second childKey childBody
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
          parentRegistered <- registerWorkflowRef second parentKey parentBody
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
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a child inherits its parent's deadline" scenarioDeadlineInherited checkDeadlineInherited,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a child's own timeout replaces the inherited deadline" scenarioChildBudgetWins checkChildBudgetWins,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a child can decline the inherited deadline" scenarioDeclinedDeadline checkDeclinedDeadline,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a parent and its child hit an inherited deadline independently" scenarioCascadeDeadline checkCascadeDeadline,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a parent starts a child under a derived id and replay adopts it" scenarioDerivedChildAdopted checkDerivedChildAdopted,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "starting a child inside a step is refused, not recorded" scenarioChildInsideStepRefused checkChildInsideStepRefused,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "starting a child through a captured parent is refused, not recorded" scenarioCaptureChildRefused checkCaptureChildRefused,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a child that fails differently is started through lift" scenarioLiftChildError checkLiftChildError,
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
          payRegistered <- registerWorkflow dbos payKey payBody
          case payRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchExec dbos isolatedEnvironment
          ran <- runWorkflow exec payKey (WorkflowId payText) (Just (encodeWorkflowValue ()))
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
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a child started and never awaited is still recorded" scenarioUnawaitedChild checkUnawaitedChild,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "children started in a loop run concurrently" scenarioFanout checkFanout,
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
          childRegistered <- registerWorkflowRef dbos childKey childBody
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
                    (startOptionsDefault {startWorkflowId = Just (WorkflowId (childText n))})
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
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "an assigned child id wins over the derived one" scenarioAssignedChildAdopted checkAssignedChildAdopted,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a workflow started outside a workflow has no parent" scenarioRootNoParent checkRootNoParent,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a start position holding a plain step is refused" scenarioPlainStepAtStart checkPlainStepAtStart,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a child started through another instance is refused" scenarioWrongInstance checkWrongInstance,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a child joining a held key is recorded as the workflow it joined" scenarioJoinHeldKey checkJoinHeldKey,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "an in-workflow enqueue is a recorded child start that replays" scenarioEnqueuedChildReplays checkEnqueuedChildReplays,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a zero-argument workflow records no input" scenarioZeroNoInput checkZeroNoInput,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "the row exists before the body starts" scenarioRowBeforeBody checkRowBeforeBody,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a panicking workflow leaves its row pending" scenarioPanic checkPanic,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "retrieving before launch is refused" scenarioRetrieveBeforeLaunch checkRunBeforeLaunch,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "an application error round-trips as itself" scenarioAppErrorRoundtrip checkAppErrorRoundtrip,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a database failure is not the workflow outcome" scenarioDbFailureNotOutcome checkDbFailureNotOutcome,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a workflow records the steps it took" scenarioStepsTaken checkStepsTaken,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "shutdown cancels a running workflow and leaves it pending" scenarioShutdownCancels checkShutdownCancels,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "dropping the future does not stop the workflow" scenarioDropFuture checkDropFuture,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a budget cancels the workflow durably" scenarioBudgetCancels checkBudgetCancels,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a started workflow carries the attributes it was given" scenarioAttributes checkAttributes,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a step error is recorded in its column" scenarioStepErrorRecorded checkStepErrorRecorded,
      -- Sim only: typed trace assertions live only in sim.
      testCase "workflow announcements carry their counts and ids" (pure ()),
      tasksTests
    ]


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
runWf = runWorkflow

runWfRef :: Executor IO -> WorkflowRef IO EngineOnly -> RunOptions -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWfRef = runWorkflowRef

startWfRef :: Executor IO -> WorkflowRef IO EngineOnly -> StartOptions -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
startWfRef = startWorkflowRef

awaitWf :: WorkflowCtx exec IO -> WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
awaitWf = awaitChild

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
