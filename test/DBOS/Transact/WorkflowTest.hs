{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Workflow execution behavior through the class-backed engine runner.
module DBOS.Transact.WorkflowTest (tests) where

import DBOS.Prelude
import Control.Concurrent.Class.MonadSTM.Strict (atomically, newTVarIO, readTVarIO, writeTVar)
import Control.Monad.Class.MonadTimer (threadDelay)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.IOSim (runSimOrThrow)
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (AwaitedOutcome (..), NewWorkflow (..), StepRecord (..), Submission (..), WorkflowId (..), WorkflowRecord (..), Timestamp (..), addTimeout, getWorkflow, listWorkflowSteps, newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  ( CodecError,
    Config (..),
    Environment (..),
    Error (..),
    RunOptions (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    StartOptions (..),
    Tasks,
    Timeout (..),
    Ctx,
    DBOS,
    Enqueue (..),
    DuplicationPolicy (..),
    QueueConflict (..),
    WorkflowStatus (..),
    abortAll,
    childWorkflowId,
    defaultQueueOptions,
    firstStepStatus,
    configFromEnv,
    decodeWorkflowValue,
    encodeWorkflowValue,
    enqueueNew,
    handleResult,
    handleStatus,
    handleWorkflowId,
    launchWithEnvironment,
    newDBOS,
    newTasks,
    newWorkflowKey,
    registerDBOSWorkflow,
    registerDBOSWorkflowRef,
    registerQueue,
    resolveTimeoutDeadline,
    retrieveWorkflow,
    runDBOSWorkflow,
    runDBOSWorkflowRef,
    nextStepMarker,
    runOptionsDefault,
    runOptionsToStartOptions,
    nullTracer,
    runWorkflowStep,
    selectWorkflow,
    spawnTracked,
    startChildWorkflow,
    secondsDuration,
    shutdown,
    startDBOSWorkflowRef,
    startOptionsDefault,
    withAttempt,
    timeoutBudget,
    waitForWorkflow,
    withSystemDB,
    workflowId,
  )
import DBOS.Transact.ContextTest (ctxOver)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
  testGroup
    "Workflow execution"
    [ testCase "a registered workflow starts and records its result" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-workflow-" <> Text.take 12 suffix
            appVersion = "hs-l2-version-" <> suffix
            executorId = "hs-l2-executor-" <> suffix
            workflowText = "hs-l2-workflow-id-" <> suffix
            key = newWorkflowKey "double"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          result <- runDBOSWorkflow dbos key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          case result of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "typed result is stored" (Right 42) decoded
            other -> fail (show other),
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
        let body :: Int -> Ctx IO -> IO (Either Error Int)
            body value ctx = do
              completedStep <- runWorkflowStep ctx "once" (const (modifyIORef' bodyCalls (+ 1) >> pure (value * 2)))
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
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          first <- try (runDBOSWorkflow dbos key workflowId (Just (encodeWorkflowValue (21 :: Int)))) :: IO (Either SomeException (Either Error (Maybe SerializedWorkflowValue)))
          _ <- case first of
            Left exception -> assertBool "body interruption escapes without a workflow outcome" ("interrupted after checkpoint" `Text.isInfixOf` Text.pack (show exception))
            Right result -> fail (show result)
          assertEqual "the step ran before interruption" 1 =<< readIORef bodyCalls
          shutdown dbos
          writeIORef shouldCrash False
          restarted <- launchWithEnvironment dbos isolatedEnvironment
          case restarted of
            Left err -> fail (show err)
            Right () -> pure ()
          settled <- timeout 10000000 (waitForWorkflow dbos workflowId)
          case settled of
            Just (Right (AwaitedSucceeded (Just output) serialization)) -> do
              let stored = SerializedWorkflowValue output (Serialization <$> serialization)
                  decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "recovery completes the same workflow" (Right 42) decoded
            other -> fail (show other)
          assertEqual "the replay adopts the recorded step" 1 =<< readIORef bodyCalls
          adopted <- runDBOSWorkflow dbos key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          case adopted of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "a duplicate run awaits and adopts the stored result" (Right 42) decoded
            other -> fail (show other),
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
          started <- launchWithEnvironment first isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          -- Ghost first, so the sweep meets the skip before the recovery.
          ghostWorker <- async (runDBOSWorkflowRef first ghostRef (runOptionsDefault {runWorkflowId = Just ghostText}) Nothing)
          keeperWorker <- async (runDBOSWorkflowRef first keeperRef (runOptionsDefault {runWorkflowId = Just keeperText}) Nothing)
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
          keeperRegistered <- registerDBOSWorkflowRef second keeperKey (\() _ -> pure (Right ()))
          case keeperRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          relaunched <- launchWithEnvironment second isolatedEnvironment
          case relaunched of
            Left err -> fail (show err)
            Right () -> pure ()
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
      testCase "starting a taken id joins the existing run" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-join-" <> Text.take 12 suffix
            appVersion = "hs-l2-join-version-" <> suffix
            executorId = "hs-l2-join-executor-" <> suffix
            key = newWorkflowKey "slow"
            startWid = "hs-l2-join-start-" <> suffix
            startOpts = startOptionsDefault {startWorkflowId = Just startWid}
        entered <- newIORef (0 :: Int)
        release <- newEmptyMVar
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            body :: () -> Ctx IO -> IO (Either Error Int)
            body () _ = modifyIORef' entered (+ 1) >> takeMVar release >> pure (Right 7)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRef dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          -- Returns at once, before the workflow finishes: it is blocked
          -- at the gate.
          first <- startDBOSWorkflowRef dbos ref startOpts Nothing
          second <- case first of
            Left err -> fail (show err)
            Right firstHandle -> do
              handleWorkflowId firstHandle @?= startWid
              status <- handleStatus firstHandle
              case status of
                Right (Just Pending) -> pure ()
                other -> fail ("expected the started row PENDING: " <> show other)
              -- The double-click: the same id while the first run still
              -- owns it joins rather than failing.
              joined <- startDBOSWorkflowRef dbos ref startOpts Nothing
              case joined of
                Left err -> fail (show err)
                Right secondHandle -> do
                  handleWorkflowId secondHandle @?= startWid
                  pure (firstHandle, secondHandle)
          putMVar release ()
          results <- timeout 15000000 (mapM handleResult [fst second, snd second])
          case results of
            Just [Right (Just firstStored), Right (Just secondStored)] -> do
              let firstDecoded = decodeWorkflowValue "result" (Just firstStored) :: Either CodecError Int
                  secondDecoded = decodeWorkflowValue "result" (Just secondStored) :: Either CodecError Int
              assertEqual "the first caller reads the run" (Right 7) firstDecoded
              assertEqual "the joining caller reads the same run" (Right 7) secondDecoded
            other -> fail ("expected both handles to resolve: " <> show other)
          assertEqual "one id, one execution" 1 =<< readIORef entered
          reader <- getBackend
          found <- getWorkflow reader (WorkflowId startWid)
          case found of
            Right (Just row) -> row.workflowRecordStatus @?= Success
            other -> fail ("expected exactly one successful row: " <> show other),
      testCase "a parent starts a child under a derived id and replay adopts it" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-child-" <> Text.take 12 suffix
            appVersion = "hs-l2-child-version-" <> suffix
            executorId = "hs-l2-child-executor-" <> suffix
            parentText = "hs-l2-child-parent-" <> suffix
            childKey = newWorkflowKey "double"
            parentKey = newWorkflowKey "parent"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            childBody :: Int -> Ctx IO -> IO (Either Error Int)
            childBody value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let parentBody :: Int -> Ctx IO -> IO (Either Error Text)
              parentBody _ ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault (Just (encodeWorkflowValue (21 :: Int)))
                pure (handleWorkflowId <$> started)
          parentRegistered <- registerDBOSWorkflow dbos parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          first <- runDBOSWorkflow dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int)))
          childId <- case first of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Text
              case decoded of
                Right cid -> cid <$ (cid @?= parentText <> "-0")
                Left err -> fail (show err)
            other -> fail (show other)
          -- The child is recorded but nothing runs it here: relaunch and
          -- recovery executes it, then a fresh handle adopts the outcome.
          shutdown dbos
          relaunched <- launchWithEnvironment dbos isolatedEnvironment
          case relaunched of
            Left err -> fail (show err)
            Right () -> pure ()
          retrieved <- retrieveWorkflow dbos (WorkflowId childId)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              result <- handleResult handle
              case result of
                Right (Just stored) -> do
                  let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                  assertEqual "recovery runs the recorded child" (Right 42) decoded
                other -> fail (show other)
          replayed <- runDBOSWorkflow dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int)))
          case replayed of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Text
              assertEqual "replay adopts the recorded child, starting none" (Right childId) decoded
            other -> fail (show other),
      testCase "starting a child inside a step is refused, not recorded" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-childleaf-" <> Text.take 12 suffix
            appVersion = "hs-l2-childleaf-version-" <> suffix
            executorId = "hs-l2-childleaf-executor-" <> suffix
            parentText = "hs-l2-childleaf-parent-" <> suffix
            childKey = newWorkflowKey "double"
            parentKey = newWorkflowKey "badparent"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            childBody :: Int -> Ctx IO -> IO (Either Error Int)
            childBody value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let badBody :: Int -> Ctx IO -> IO (Either Error Text)
              badBody _ ctx = do
                marker <- nextStepMarker ctx
                outcome <- withAttempt ctx marker (firstStepStatus 0) (\inner -> startChildWorkflow inner childRef startOptionsDefault Nothing)
                pure $ case outcome of
                  Left err -> Left err
                  Right handle -> Left (ErrorConfig ("started inside a step: " <> handleWorkflowId handle))
          parentRegistered <- registerDBOSWorkflow dbos parentKey badBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          result <- runDBOSWorkflow dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int)))
          case result of
            Left (InsideStep operation) -> operation @?= "starting a workflow"
            other -> fail ("expected the leaf refusal, got: " <> show other),
      testCase "a child started and never awaited is still recorded" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-unawaited-" <> Text.take 12 suffix
            parentText = "hs-l2-unawaited-parent-" <> suffix
            childText = parentText <> "-0"
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "forgetful"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            childBody :: Int -> Ctx IO -> IO (Either Error Int)
            childBody value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          -- Started, and the handle dropped without ever being awaited: the
          -- start row is what makes a child adoptable, not the await.
          let parentBody :: Int -> Ctx IO -> IO (Either Error ())
              parentBody _ ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault (Just (encodeWorkflowValue (21 :: Int)))
                case started of
                  Left err -> pure (Left err)
                  Right _ -> pure (Right ())
          parentRegistered <- registerDBOSWorkflow dbos parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runDBOSWorkflow dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int)))
          case ran of
            Right _ -> pure ()
            other -> fail ("expected the parent to run, got: " <> show other)
          reader <- getBackend
          listed <- listWorkflowSteps reader (WorkflowId parentText) False Nothing Nothing Nothing
          case listed of
            Right [StepRecord {stepRecordChildWorkflowId = Just (WorkflowId recorded)}] ->
              recorded @?= childText
            other -> fail ("expected the lone start step, got: " <> show other)
          -- The child outlives the parent's interest in it and finishes
          -- on its own: the start detached it onto the executor, so the
          -- parent returning does not stop it.
          found <- timeout 15000000 (waitForWorkflow dbos (WorkflowId childText))
          case found of
            Just (Right (AwaitedSucceeded (Just output) serialization)) -> do
              let stored = SerializedWorkflowValue output (Serialization <$> serialization)
                  decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the abandoned child still records its result" (Right 42) decoded
            other -> fail ("expected the abandoned child to finish, got: " <> show other)
          foundRow <- getWorkflow reader (WorkflowId childText)
          case foundRow of
            Right (Just row) -> row.workflowRecordParentWorkflowId @?= Just (WorkflowId parentText)
            other -> fail ("expected the child row, got: " <> show other)
          children <- SystemDB.getWorkflowChildren reader (WorkflowId parentText)
          children @?= Right [WorkflowId childText],
      testCase "children started in a loop run concurrently" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-fanout-" <> Text.take 12 suffix
            parentText = "hs-l2-fanout-parent-" <> suffix
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "fan"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            childBody :: Int -> Ctx IO -> IO (Either Error Int)
            childBody n _ = threadDelay 400000 >> pure (Right n)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          -- Start all three first, then collect: awaiting inside the
          -- first loop would serialize them.
          let parentBody :: () -> Ctx IO -> IO (Either Error Int)
              parentBody () ctx = do
                started <- mapM (\n -> startChildWorkflow ctx childRef startOptionsDefault (Just (encodeWorkflowValue (n :: Int)))) [0, 1, 2]
                case sequence started of
                  Left err -> pure (Left err)
                  Right handles -> do
                    results <- mapM handleResult handles
                    case sequence results of
                      Left err -> pure (Left err)
                      Right outputs -> case mapM (decodeWorkflowValue "result") outputs of
                        Left _ -> pure (Left (StepFailed "fan" "bad child output"))
                        Right (numbers :: [Int]) -> pure (Right (sum numbers))
          parentRegistered <- registerDBOSWorkflow dbos parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          began <- SystemDB.timestampNow
          ran <- runDBOSWorkflow dbos parentKey (WorkflowId parentText) Nothing
          ended <- SystemDB.timestampNow
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "0 + 1 + 2" (Right 3) decoded
            other -> fail ("expected the fan-out total, got: " <> show other)
          let tookMs = SystemDB.timestampToEpochMs ended - SystemDB.timestampToEpochMs began
          assertBool ("three 400ms children took " <> show tookMs <> "ms, which is serial rather than concurrent") (tookMs < 1200)
          reader <- getBackend
          children <- SystemDB.getWorkflowChildren reader (WorkflowId parentText)
          case children of
            Right ids -> length ids @?= 3
            other -> fail ("expected three children, got: " <> show other),
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
            childBody :: Int -> Ctx IO -> IO (Either Error Int)
            childBody n _ = putMVar (entered !! n) () >> takeMVar (gates !! n) >> pure (Right n)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          mapM_
            ( \n -> do
                startedChild <-
                  startDBOSWorkflowRef
                    dbos
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
      testCase "an assigned child id wins over the derived one" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-assigned-" <> Text.take 12 suffix
            parentText = "hs-l2-assigned-parent-" <> suffix
            chosenText = "hs-l2-assigned-chosen-" <> suffix
            derivedText = parentText <> "-0"
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "namer"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            childBody :: Int -> Ctx IO -> IO (Either Error Int)
            childBody _ _ = pure (Right 7)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let parentBody :: Int -> Ctx IO -> IO (Either Error Text)
              parentBody _ ctx = do
                started <- startChildWorkflow ctx childRef (startOptionsDefault {startWorkflowId = Just chosenText}) (Just (encodeWorkflowValue (21 :: Int)))
                pure (handleWorkflowId <$> started)
          parentRegistered <- registerDBOSWorkflow dbos parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          first <- runDBOSWorkflow dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int)))
          case first of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Text
              decoded @?= Right chosenText
            other -> fail (show other)
          reader <- getBackend
          chosen <- getWorkflow reader (WorkflowId chosenText)
          case chosen of
            Right (Just row) -> row.workflowRecordParentWorkflowId @?= Just (WorkflowId parentText)
            other -> fail ("expected the assigned row, got: " <> show other)
          derived <- getWorkflow reader (WorkflowId derivedText)
          derived @?= Right Nothing
          -- The assigned child runs through recovery like a derived one.
          shutdown dbos
          relaunched <- launchWithEnvironment dbos isolatedEnvironment
          case relaunched of
            Left err -> fail (show err)
            Right () -> pure ()
          retrieved <- retrieveWorkflow dbos (WorkflowId chosenText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              result <- handleResult handle
              case result of
                Right (Just stored) -> do
                  let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                  assertEqual "the assigned child records its result" (Right 7) decoded
                other -> fail (show other),
      testCase "a workflow started outside a workflow has no parent" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-root-" <> Text.take 12 suffix
            workflowText = "hs-l2-root-id-" <> suffix
            key = newWorkflowKey "root"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: () -> Ctx IO -> IO (Either Error Int)
            body () _ = pure (Right 1)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runDBOSWorkflow dbos key (WorkflowId workflowText) Nothing
          case ran of
            Right _ -> pure ()
            other -> fail ("expected the workflow to run, got: " <> show other)
          reader <- getBackend
          found <- getWorkflow reader (WorkflowId workflowText)
          case found of
            Right (Just row) -> row.workflowRecordParentWorkflowId @?= Nothing
            other -> fail ("expected the root row, got: " <> show other),
      testCase "a start position holding a plain step is refused" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-stale-" <> Text.take 12 suffix
            parentText = "hs-l2-stale-parent-" <> suffix
            derivedText = parentText <> "-0"
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "waiter"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            childBody :: Int -> Ctx IO -> IO (Either Error Int)
            childBody _ _ = pure (Right 1)
        gate <- newEmptyMVar
        entered <- newEmptyMVar
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          -- The parent waits before its first step id is allocated, which
          -- is the window the plain step is planted in.
          let parentBody :: Int -> Ctx IO -> IO (Either Error Int)
              parentBody _ ctx = do
                putMVar entered ()
                takeMVar gate
                started <- startChildWorkflow ctx childRef startOptionsDefault (Just (encodeWorkflowValue (1 :: Int)))
                case started of
                  Left err -> pure (Left err)
                  Right _ -> pure (Right 0)
          parentRegistered <- registerDBOSWorkflow dbos parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          worker <- async (runDBOSWorkflow dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int))))
          enteredOk <- timeout 15000000 (takeMVar entered)
          case enteredOk of
            Nothing -> fail "the parent never reached its gate"
            Just _ -> pure ()
          reader <- getBackend
          planted <- SystemDB.recordStep reader (WorkflowId parentText) 0 "child" (SystemDB.OutcomeOutput (Just "1")) Nothing Nothing
          case planted of
            Left err -> fail (show err)
            Right _ -> pure ()
          putMVar gate ()
          outcome <- timeout 15000000 (wait worker)
          case outcome of
            Just (Left (ErrorSystemDatabase (SystemDB.UnexpectedStep {stepId, expected, recorded}))) -> do
              stepId @?= 0
              assertBool "says what it wanted" ("child workflow start" `Text.isInfixOf` expected)
              assertBool "and what it found" ("plain step" `Text.isInfixOf` recorded)
            other -> fail ("expected the unexpected-step refusal, got: " <> show other)
          -- The point of refusing early: nothing was created to be orphaned.
          missing <- getWorkflow reader (WorkflowId derivedText)
          missing @?= Right Nothing,
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
                { deduplication_id = Just dedupKey,
                  duplication_policy = ReturnExisting
                }
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            childBody :: () -> Ctx IO -> IO (Either Error Int)
            childBody () _ = pure (Right 9)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let parentBody :: () -> Ctx IO -> IO (Either Error Int)
              parentBody () ctx = do
                started <- startChildWorkflow ctx childRef (startOptionsDefault {startQueue = Just joinQueue}) Nothing
                case started of
                  Left err -> pure (Left err)
                  Right handle -> do
                    result <- handleResult handle
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
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          -- The holder, enqueued before the parent runs and still waiting
          -- when the child starts: a delay holds the key without running.
          let holderQueue =
                (enqueueNew queueName)
                  { deduplication_id = Just dedupKey,
                    delay = Just (secondsDuration 3)
                  }
          holder <- startDBOSWorkflowRef dbos childRef (startOptionsDefault {startWorkflowId = Just holderText, startQueue = Just holderQueue}) Nothing
          case holder of
            Left err -> fail (show err)
            Right _ -> pure ()
          outcome <- timeout 30000000 (runDBOSWorkflow dbos parentKey (WorkflowId parentText) Nothing)
          case outcome of
            Just (Right (Just stored)) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the parent reads the joined workflow's output" (Right 9) decoded
            other -> fail ("expected the joined output, got: " <> show other)
          reader <- getBackend
          derived <- getWorkflow reader (WorkflowId derivedText)
          derived @?= Right Nothing
          listed <- listWorkflowSteps reader (WorkflowId parentText) False Nothing Nothing Nothing
          case listed of
            Right [StepRecord {stepRecordStepName = name, stepRecordChildWorkflowId = Just (WorkflowId recorded)}] -> do
              name @?= "child"
              recorded @?= holderText
            other -> fail ("expected the joining start step, got: " <> show other)
          -- The other half of the relationship is deliberately absent: the
          -- holder has its own owner already, so the join resolves through
          -- the parent's replay without listing among its children.
          children <- SystemDB.getWorkflowChildren reader (WorkflowId parentText)
          children @?= Right [],
      testCase "a zero-argument workflow records no input" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-zero-" <> Text.take 12 suffix
            workflowText = "hs-l2-zero-id-" <> suffix
            key = newWorkflowKey "zero"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: () -> Ctx IO -> IO (Either Error ())
            body () _ = pure (Right ())
        reader <- getBackend
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runDBOSWorkflow dbos key (WorkflowId workflowText) Nothing
          case ran of
            Right _ -> pure ()
            other -> fail (show other)
          row <- SystemDB.getWorkflow reader (WorkflowId workflowText)
          case row of
            Right (Just record) -> record.workflowRecordInput @?= Nothing
            other -> fail (show other),
      testCase "the row exists before the body starts" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-row-" <> Text.take 12 suffix
            workflowText = "hs-l2-row-id-" <> suffix
            key = newWorkflowKey "sees-itself"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: () -> Ctx IO -> IO (Either Error Bool)
            body () ctx = do
              row <- withSystemDB ctx (\db -> SystemDB.getWorkflow db (WorkflowId (workflowId ctx)))
              pure (Right (case row of Right (Just _) -> True; _ -> False))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runDBOSWorkflow dbos key (WorkflowId workflowText) Nothing
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Bool
              assertEqual "the body found its own row" (Right True) decoded
            other -> fail (show other),
      testCase "a panicking workflow leaves its row pending" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-panic-" <> Text.take 12 suffix
            workflowText = "hs-l2-panic-id-" <> suffix
            key = newWorkflowKey "explodes"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: () -> Ctx IO -> IO (Either Error ())
            body () _ = liftIO (throwIO (userError "boom"))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          outcome <- try (runDBOSWorkflow dbos key (WorkflowId workflowText) Nothing)
          case (outcome :: Either SomeException (Either Error (Maybe SerializedWorkflowValue))) of
            Left _ -> pure ()
            Right other -> fail ("expected the body's exception to escape, got: " <> show other)
          retrieved <- retrieveWorkflow dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              status <- handleStatus handle
              case status of
                Right (Just Pending) -> pure ()
                other -> fail ("expected the row PENDING, got: " <> show other),
      testCase "running before launch is refused" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-unlaunched-" <> Text.take 12 suffix
            workflowText = "hs-l2-unlaunched-id-" <> suffix
            key = newWorkflowKey "double"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runDBOSWorkflow dbos key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          case ran of
            Left ErrorNotLaunched {} -> pure ()
            other -> fail ("expected a not-launched refusal, got: " <> show other),
      testCase "an application error round-trips as itself" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-app-err-" <> Text.take 12 suffix
            workflowText = "hs-l2-app-err-id-" <> suffix
            key = newWorkflowKey "flaky"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body _ _ = pure (Left (StepFailed "flaky" "boom"))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runDBOSWorkflow dbos key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          case ran of
            Left (StepFailed step message) -> do
              step @?= "flaky"
              message @?= "boom"
            other -> fail ("expected the application error back, got: " <> show other),
      testCase "a database failure is not the workflow outcome" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-blip-" <> Text.take 12 suffix
            workflowText = "hs-l2-blip-id-" <> suffix
            key = newWorkflowKey "blips"
            backendErr =
              SystemDB.Backend
                ( SystemDB.BackendError
                    { SystemDB.backendMessage = "connection reset by peer",
                      SystemDB.backendSqlState = Nothing,
                      SystemDB.backendKind = SystemDB.Connection
                    }
                )
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: () -> Ctx IO -> IO (Either Error ())
            body () _ = pure (Left (ErrorSystemDatabase backendErr))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runDBOSWorkflow dbos key (WorkflowId workflowText) Nothing
          case ran of
            Left (ErrorSystemDatabase _) -> pure ()
            other -> fail ("expected the database failure back, got: " <> show other)
          reader <- getBackend
          found <- getWorkflow reader (WorkflowId workflowText)
          case found of
            Right (Just row) -> do
              row.workflowRecordStatus @?= Pending
              row.workflowRecordError @?= Nothing
            other -> fail ("expected the row left pending with no error, got: " <> show other),
      testCase "a workflow records the steps it took" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-steps-listed-" <> Text.take 12 suffix
            workflowText = "hs-l2-steps-listed-id-" <> suffix
            key = newWorkflowKey "two-steps"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body value ctx = do
              first <- runWorkflowStep ctx "one" (const (pure (value + 1)))
              case first of
                Left err -> pure (Left err)
                Right stepped -> runWorkflowStep ctx "two" (const (pure (stepped * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runDBOSWorkflow dbos key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "two steps compose" (Right 44) decoded
            other -> fail (show other)
          reader <- getBackend
          listed <- listWorkflowSteps reader (WorkflowId workflowText) False Nothing Nothing Nothing
          case listed of
            Right [StepRecord {stepRecordStepName = first}, StepRecord {stepRecordStepName = second}] ->
              [first, second] @?= ["one", "two"]
            other -> fail ("expected two steps in order, got: " <> show other),
      testCase "shutdown cancels a running workflow and leaves it pending" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-shutdown-run-" <> Text.take 12 suffix
            workflowText = "hs-l2-shutdown-run-id-" <> suffix
            key = newWorkflowKey "gated"
        gate <- newEmptyMVar
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: () -> Ctx IO -> IO (Either Error Int)
            body () _ = takeMVar gate >> pure (Right 7)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRef dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          worker <- async (runDBOSWorkflowRef dbos ref (runOptionsDefault {runWorkflowId = Just workflowText}) Nothing)
          waitForRow dbos (WorkflowId workflowText)
          shutdown dbos
          cancel worker
          status <- readWorkflowStatus getBackend (WorkflowId workflowText)
          status @?= Just Pending,
      testCase "dropping the future does not stop the workflow" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-drop-future-" <> Text.take 12 suffix
            workflowText = "hs-l2-drop-future-id-" <> suffix
            key = newWorkflowKey "gated"
        gate <- newEmptyMVar
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: () -> Ctx IO -> IO (Either Error Int)
            body () _ = takeMVar gate >> pure (Right 7)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRef dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          worker <- async (runDBOSWorkflowRef dbos ref (runOptionsDefault {runWorkflowId = Just workflowText}) Nothing)
          waitForRow dbos (WorkflowId workflowText)
          -- Dropping the waiter stops the watching, not the workflow: the
          -- run is detached onto the executor, so cancelling the caller
          -- leaves the row pending and the body still gated.
          cancel worker
          status <- readWorkflowStatus getBackend (WorkflowId workflowText)
          status @?= Just Pending
          -- Released, the run finishes on its own — no recovery needed.
          putMVar gate ()
          settled <- timeout 15000000 (waitForWorkflow dbos (WorkflowId workflowText))
          case settled of
            Just (Right (AwaitedSucceeded (Just output) serialization)) -> do
              let stored = SerializedWorkflowValue output (Serialization <$> serialization)
                  decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the dropped run still records its result" (Right 7) decoded
            other -> fail ("expected the dropped run to finish, got: " <> show other),
      tasksTests
    ]

-- | The oracle's @Tasks@ behavior, driven under @IOSim@ so the
-- scheduling is deterministic and the DB is not involved.
tasksTests :: TestTree
tasksTests =
  testGroup
    "Tasks"
    [ testCase "abortAll waits until every task has departed" $ do
        let aborted = runSimOrThrow $ do
              tasks <- newTasks
              _ <- spawnTracked tasks (threadDelay 1000000)
              _ <- spawnTracked tasks (threadDelay 1000000)
              abortAll tasks
        aborted @?= 2,
      testCase "a task that finished on its own is not left in the registry" $ do
        let aborted = runSimOrThrow $ do
              tasks <- newTasks
              _ <- spawnTracked tasks (pure ())
              threadDelay 1000
              abortAll tasks
        aborted @?= 0,
      testCase "a task arriving after the sweep is aborted on arrival" $ do
        let ran = runSimOrThrow $ do
              tasks <- newTasks
              _ <- abortAll tasks
              flag <- newTVarIO False
              _ <- spawnTracked tasks (threadDelay 1000 >> atomically (writeTVar flag True))
              threadDelay 5000
              readTVarIO flag
        ran @?= False,
      testCase "an empty sweep returns at once" $ do
        let aborted = runSimOrThrow (newTasks >>= abortAll)
        aborted @?= 0
    ]

-- | Wait until a workflow row appears, so background runs are observed
-- rather than raced.
waitForRow :: DBOS IO -> WorkflowId -> IO ()
waitForRow dbos wid = go (20 :: Int)
  where
    go 0 = fail "the workflow row never appeared"
    go n = do
      retrieved <- retrieveWorkflow dbos wid
      case retrieved of
        Left err -> fail (show err)
        Right handle -> do
          status <- handleStatus handle
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
