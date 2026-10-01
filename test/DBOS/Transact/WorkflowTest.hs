{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Workflow execution behavior through the class-backed engine runner.
module DBOS.Transact.WorkflowTest (tests) where

import DBOS.Prelude
import Control.Concurrent.Class.MonadSTM.Strict (atomically, newTVarIO, readTVarIO, writeTVar)
import Control.Monad.Class.MonadTimer (threadDelay)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.IOSim (runSimOrThrow)
import Data.Aeson (FromJSON (..), ToJSON (..), Value (..), object, withObject, (.:), (.=))
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
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
    SelectArm (..),
    SerializedWorkflowValue (..),
    StartOptions (..),
    Provenance (..),
    WorkflowHandle (..),
    Tasks,
    Timeout (..),
    Ctx,
    DBOS,
    Enqueue (..),
    DuplicationPolicy (..),
    QueueConflict (..),
    WorkflowStatus (..),
    abortAll,
    awaitChild,
    cancellationToken,
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
    WorkflowKey,
    WorkflowRef,
    registerQueue,
    resolveTimeoutDeadline,
    retrieveWorkflow,
    runDBOSWorkflow,
    runDBOSWorkflowRef,
    nextStepMarker,
    runOptionsDefault,
    runOptionsToStartOptions,
    nullTracer,
    pendingAwait,
    pendingWorkflowStepWith,
    runWorkflowStep,
    runWorkflowStepWith,
    selectWorkflow,
    spawnTracked,
    spawnLocal,
    startChildWorkflow,
    tasksSpawner,
    millisDuration,
    secondsDuration,
    selectStep,
    shutdown,
    startDBOSWorkflowRef,
    startOptionsDefault,
    stepOptionsDefault,
    tokenCancelled,
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
            body :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
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
          result <- runWf dbos key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
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
        let body :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
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
          first <- try (runWf dbos key workflowId (Just (encodeWorkflowValue (21 :: Int)))) :: IO (Either SomeException (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
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
          adopted <- runWf dbos key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
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
          ghostWorker <- async (runWfRef first ghostRef (runOptionsDefault {runWorkflowId = Just ghostText}) Nothing)
          keeperWorker <- async (runWfRef first keeperRef (runOptionsDefault {runWorkflowId = Just keeperText}) Nothing)
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
            body :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
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
          first <- startWfRef dbos ref startOpts Nothing
          second <- case first of
            Left err -> fail (show err)
            Right firstHandle -> do
              handleWorkflowId firstHandle @?= startWid
              status <- statusWf firstHandle
              case status of
                Right (Just Pending) -> pure ()
                other -> fail ("expected the started row PENDING: " <> show other)
              -- The double-click: the same id while the first run still
              -- owns it joins rather than failing.
              joined <- startWfRef dbos ref startOpts Nothing
              case joined of
                Left err -> fail (show err)
                Right secondHandle -> do
                  handleWorkflowId secondHandle @?= startWid
                  pure (firstHandle, secondHandle)
          putMVar release ()
          results <- timeout 15000000 (mapM resultWf [fst second, snd second])
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
      testCase "a fresh start is local and a join polls" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-local-" <> Text.take 12 suffix
            appVersion = "hs-l2-local-version-" <> suffix
            executorId = "hs-l2-local-executor-" <> suffix
            workflowText = "hs-l2-local-id-" <> suffix
            key = newWorkflowKey "quick"
        config0 <- configFromEnv appName
        release <- newEmptyMVar
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            body :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            body () _ = takeMVar release >> pure (Right 7)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRef dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right registeredRef -> pure registeredRef
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          firstStarted <- startWfRef dbos ref (startOptionsDefault {startWorkflowId = Just workflowText}) Nothing
          firstHandle <- case firstStarted of
            Left err -> fail (show err)
            Right handle@(WorkflowHandle _ _ provenance') -> do
              case provenance' of
                Local _ -> pure ()
                _ -> fail "expected a local handle for the fresh start"
              pure handle
          joined <- startWfRef dbos ref (startOptionsDefault {startWorkflowId = Just workflowText}) Nothing
          case joined of
            Left err -> fail (show err)
            Right (WorkflowHandle _ _ provenance') -> case provenance' of
              Polling {} -> pure ()
              _ -> fail "expected a polling handle for the join"
          retrieved <- retrieveWf dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right (WorkflowHandle _ _ provenance') -> case provenance' of
              Polling {} -> pure ()
              _ -> fail "expected a polling handle from retrieve"
          putMVar release ()
          result <- timeout 15000000 (resultWf firstHandle)
          case result of
            Just (Right (Just stored)) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the local handle reads the task's outcome" (Right 7) decoded
            other -> fail ("expected the local await to resolve, got: " <> show other),
      testCase "awaiting a child is recorded as a step" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-await-" <> Text.take 12 suffix
            appVersion = "hs-l2-await-version-" <> suffix
            executorId = "hs-l2-await-executor-" <> suffix
            parentText = "hs-l2-await-parent-" <> suffix
            childText = parentText <> "-0"
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "parent"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            childBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody () _ = pure (Right 99)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
              parentBody () ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWf ctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          parentRegistered <- registerDBOSWorkflow dbos parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runWf dbos parentKey (WorkflowId parentText) Nothing
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the parent reads the awaited child" (Right 99) decoded
            other -> fail (show other)
          reader <- getBackend
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
                startedChild @?= childText
                awaitName @?= "DBOS.getResult"
                awaitOutput @?= "99"
                awaitedChild @?= childText
            other -> fail ("expected the start and the recorded await, got: " <> show other),
      testCase "a recorded await of another workflow is refused" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-stale-await-" <> Text.take 12 suffix
            appVersion = "hs-l2-stale-await-version-" <> suffix
            executorId = "hs-l2-stale-await-executor-" <> suffix
            parentText = "hs-l2-await-wrong-" <> suffix
            childText = parentText <> "-0"
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "parent"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            childBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody () _ = pure (Right 1)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          gate <- newEmptyMVar
          entered <- newEmptyMVar
          let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
              parentBody () ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    putMVar entered ()
                    takeMVar gate
                    awaited <- awaitWf ctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          parentRegistered <- registerDBOSWorkflow dbos parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          worker <- async (runWf dbos parentKey (WorkflowId parentText) Nothing)
          enteredOk <- timeout 15000000 (takeMVar entered)
          case enteredOk of
            Nothing -> fail "the parent never recorded its start"
            Just _ -> pure ()
          -- An await recorded at the position this parent is about to reach,
          -- naming a workflow that is not the one it holds a handle to.
          reader <- getBackend
          planted <-
            SystemDB.recordChildResult
              reader
              (WorkflowId parentText)
              1
              (WorkflowId "somebody-elses-workflow")
              (SystemDB.OutcomeOutput (Just "7"))
              Nothing
              Nothing
          case planted of
            Left err -> fail (show err)
            Right () -> pure ()
          putMVar gate ()
          outcome <- timeout 15000000 (wait worker)
          case outcome of
            Just (Left (ErrorSystemDatabase (SystemDB.UnexpectedStep {stepId, expected, recorded}))) -> do
              stepId @?= 1
              assertBool ("says which workflow it was awaiting in " <> Text.unpack expected) (childText `Text.isInfixOf` expected)
              assertBool ("and whose outcome it found in " <> Text.unpack recorded) ("somebody-elses-workflow" `Text.isInfixOf` recorded)
            other -> fail ("expected the stale-await refusal, got: " <> show other),
      testCase "awaiting a child inside a step is covered by that step" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-await-step-" <> Text.take 12 suffix
            appVersion = "hs-l2-await-step-version-" <> suffix
            executorId = "hs-l2-await-step-executor-" <> suffix
            parentText = "hs-l2-await-step-parent-" <> suffix
            childText = parentText <> "-0"
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "parent"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            childBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody () _ = pure (Right 41)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
              parentBody () ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle ->
                    runWorkflowStepWith stepOptionsDefault ctx "collect" $ \inner -> do
                      awaited <- awaitWf inner wfHandle
                      pure $ case awaited of
                        Left err -> Left err
                        Right (Just stored) ->
                          case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                            Right value -> Right value
                            Left err -> Left (StepFailed "collect" (Text.pack (show err)))
                        Right Nothing -> Left (StepFailed "collect" "no child output")
          parentRegistered <- registerDBOSWorkflow dbos parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runWf dbos parentKey (WorkflowId parentText) Nothing
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the enclosing step carries the child's value" (Right 41) decoded
            other -> fail (show other)
          reader <- getBackend
          listed <- listWorkflowSteps reader (WorkflowId parentText) True Nothing Nothing Nothing
          case listed of
            Right
              [ StepRecord {stepRecordStepName = startName, stepRecordChildWorkflowId = Just (WorkflowId startedChild)},
                StepRecord {stepRecordStepName = collectName, stepRecordOutput = Just collectOutput}
                ] -> do
                startName @?= "child"
                startedChild @?= childText
                collectName @?= "collect"
                collectOutput @?= "41"
            other -> fail ("expected the start and the enclosing step only, got: " <> show other),
      testCase "child starts and awaits keep their ids in build order" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-order-" <> Text.take 12 suffix
            appVersion = "hs-l2-order-version-" <> suffix
            executorId = "hs-l2-order-executor-" <> suffix
            parentText = "hs-l2-order-parent-" <> suffix
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "parent"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            childBody :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody n _ = pure (Right n)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          -- The oracle drives the built starts through `join!` backwards;
          -- the port claims and waits at the call, so call order is build
          -- order and the rows are the contract both pin.
          let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
              parentBody () ctx = do
                started <- mapM (\n -> startChildWorkflow ctx childRef startOptionsDefault (Just (encodeWorkflowValue (n :: Int)))) [1, 2, 3]
                case sequence started of
                  Left err -> pure (Left err)
                  Right handles -> do
                    awaited <- mapM (awaitWf ctx) handles
                    case sequence awaited of
                      Left err -> pure (Left err)
                      Right outputs -> case mapM (decodeWorkflowValue "result") outputs of
                        Left _ -> pure (Left (StepFailed "parent" "bad child output"))
                        Right (numbers :: [Int]) -> pure (Right (sum numbers))
          parentRegistered <- registerDBOSWorkflow dbos parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runWf dbos parentKey (WorkflowId parentText) Nothing
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "1 + 2 + 3" (Right 6) decoded
            other -> fail (show other)
          reader <- getBackend
          listed <- listWorkflowSteps reader (WorkflowId parentText) False Nothing Nothing Nothing
          case listed of
            Right rows -> do
              let table = map (\row -> (row.stepRecordStepId, row.stepRecordStepName, row.stepRecordChildWorkflowId)) rows
              table
                @?= [ (0, "child", Just (WorkflowId (parentText <> "-0"))),
                      (1, "child", Just (WorkflowId (parentText <> "-1"))),
                      (2, "child", Just (WorkflowId (parentText <> "-2"))),
                      (3, "DBOS.getResult", Just (WorkflowId (parentText <> "-0"))),
                      (4, "DBOS.getResult", Just (WorkflowId (parentText <> "-1"))),
                      (5, "DBOS.getResult", Just (WorkflowId (parentText <> "-2")))
                    ]
            other -> fail ("expected the three starts and their awaits, got: " <> show other)
          -- Which child is which: the start built first passed 1, so its
          -- derived id is the child that returned 1.
          firstChild <- getWorkflow reader (WorkflowId (parentText <> "-0"))
          case firstChild of
            Right (Just row) -> row.workflowRecordOutput @?= Just "1"
            other -> fail ("expected the first-built child, got: " <> show other),
      testCase "runs claim their pairs of step ids adjacently" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-pairs-" <> Text.take 12 suffix
            appVersion = "hs-l2-pairs-version-" <> suffix
            executorId = "hs-l2-pairs-executor-" <> suffix
            parentText = "hs-l2-pairs-parent-" <> suffix
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "parent"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            childBody :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody n _ = pure (Right n)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          -- A run claims its start and its await together, so each await
          -- sits immediately behind its own start and a replay rebuilds
          -- the same pairs however the children interleave.
          let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
              parentBody () ctx = do
                let pair n = do
                      startedPair <- startChildWorkflow ctx childRef startOptionsDefault (Just (encodeWorkflowValue (n :: Int)))
                      case startedPair of
                        Left err -> pure (Left err)
                        Right wfHandle -> do
                          awaited <- awaitWf ctx wfHandle
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
          parentRegistered <- registerDBOSWorkflow dbos parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runWf dbos parentKey (WorkflowId parentText) Nothing
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "1 + 2 + 3" (Right 6) decoded
            other -> fail (show other)
          reader <- getBackend
          listed <- listWorkflowSteps reader (WorkflowId parentText) False Nothing Nothing Nothing
          case listed of
            Right rows -> do
              let table = map (\row -> (row.stepRecordStepId, row.stepRecordStepName, row.stepRecordChildWorkflowId)) rows
              table
                @?= [ (0, "child", Just (WorkflowId (parentText <> "-0"))),
                      (1, "DBOS.getResult", Just (WorkflowId (parentText <> "-0"))),
                      (2, "child", Just (WorkflowId (parentText <> "-2"))),
                      (3, "DBOS.getResult", Just (WorkflowId (parentText <> "-2"))),
                      (4, "child", Just (WorkflowId (parentText <> "-4"))),
                      (5, "DBOS.getResult", Just (WorkflowId (parentText <> "-4")))
                    ]
            other -> fail ("expected each await behind its own start, got: " <> show other),
      testCase "a select step races a step against a child's result" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-race-" <> Text.take 12 suffix
            appVersion = "hs-l2-race-version-" <> suffix
            executorId = "hs-l2-race-executor-" <> suffix
            parentText = "hs-l2-race-parent-" <> suffix
            childText = parentText <> "-0"
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "parent"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            childBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody () _ = pure (Right 7)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          -- Never finishes, so the await wins however long the child's row
          -- takes to settle. Every branch is built before the race, so the
          -- ids follow source order: the losing step claims 1, the await 2,
          -- and the race itself 3.
          let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
              parentBody () ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right childHandle -> do
                    slow <- pendingWorkflowStepWith stepOptionsDefault ctx "slow" (\_ -> threadDelay 30000000 >> pure (Right (0 :: Int)))
                    awaited <- pendingAwait ctx childHandle
                    selectStep
                      ctx
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
          parentRegistered <- registerDBOSWorkflow dbos parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runWf dbos parentKey (WorkflowId parentText) Nothing
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the await won and its arm produced the answer" (Right 7) decoded
            other -> fail ("expected the race's winner, got: " <> show other)
          reader <- getBackend
          listed <- listWorkflowSteps reader (WorkflowId parentText) False Nothing Nothing Nothing
          case listed of
            Right rows -> do
              let table = map (\row -> (row.stepRecordStepId, row.stepRecordStepName, row.stepRecordChildWorkflowId)) rows
              table
                @?= [ (0, "child", Just (WorkflowId childText)),
                      -- Id 1 is the losing step, built and dropped without a row.
                      (2, "DBOS.getResult", Just (WorkflowId childText)),
                      (3, "DBOS.selectStep", Nothing)
                    ]
            other -> fail ("expected the start, the await and the select, got: " <> show other),
      testCase "a control signal winning a select records no winner" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-race-control-" <> Text.take 12 suffix
            appVersion = "hs-l2-race-control-version-" <> suffix
            executorId = "hs-l2-race-control-executor-" <> suffix
            parentText = "hs-l2-race-control-parent-" <> suffix
            parentKey = newWorkflowKey "parent"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            parentBody () ctx = do
              interrupted <- pendingWorkflowStepWith stepOptionsDefault ctx "interrupted" (\_ -> pure (Left (Interrupted {workflowId = parentText})))
              slow <- pendingWorkflowStepWith stepOptionsDefault ctx "slow" (\_ -> threadDelay 30000000 >> pure (Right (1 :: Int)))
              selectStep
                ctx
                [ SelectArm "interrupted" interrupted (\outcome -> pure (outcome >>= \value -> Right value)),
                  SelectArm "slow" slow (\outcome -> pure (outcome >>= \value -> Right value))
                ]
        bracket (newDBOS config) shutdown $ \dbos -> do
          parentRegistered <- registerDBOSWorkflow dbos parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runWf dbos parentKey (WorkflowId parentText) Nothing
          case ran of
            Left (Interrupted {workflowId}) -> workflowId @?= parentText
            other -> fail ("expected the control signal back, got: " <> show other)
          reader <- getBackend
          listed <- listWorkflowSteps reader (WorkflowId parentText) False Nothing Nothing Nothing
          listed @?= Right []
          parentRow <- getWorkflow reader (WorkflowId parentText)
          case parentRow of
            Right (Just row) -> row.workflowRecordStatus @?= Pending
            other -> fail ("expected the parent row PENDING, got: " <> show other),
      testCase "a losing step has its cancellation token fired" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-race-token-" <> Text.take 12 suffix
            appVersion = "hs-l2-race-token-version-" <> suffix
            executorId = "hs-l2-race-token-executor-" <> suffix
            parentText = "hs-l2-race-token-parent-" <> suffix
            parentKey = newWorkflowKey "parent"
        config0 <- configFromEnv appName
        released <- newEmptyMVar
        watching <- newEmptyMVar
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            parentBody () ctx = do
              -- The loser registers a watcher on its token and says so, then
              -- parks; the winner waits for that registration, so dropping
              -- the loser must fire the token for work the runtime cannot
              -- stop by dropping it.
              slow <- pendingWorkflowStepWith stepOptionsDefault ctx "slow" $ \inner -> do
                token <- cancellationToken inner
                _ <- async $ do
                  let watch = do
                        cancelled <- tokenCancelled token
                        if cancelled then pure () else threadDelay 1000 >> watch
                  watch
                  putMVar released ()
                putMVar watching ()
                threadDelay 30000000
                pure (Right (2 :: Int))
              fast <- pendingWorkflowStepWith stepOptionsDefault ctx "fast" (\_ -> takeMVar watching >> pure (Right (1 :: Int)))
              selectStep
                ctx
                [ SelectArm "slow" slow (\outcome -> pure (outcome >>= \value -> Right value)),
                  SelectArm "fast" fast (\outcome -> pure (outcome >>= \value -> Right value))
                ]
        bracket (newDBOS config) shutdown $ \dbos -> do
          parentRegistered <- registerDBOSWorkflow dbos parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runWf dbos parentKey (WorkflowId parentText) Nothing
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the fast step won" (Right 1) decoded
            other -> fail ("expected the fast step's value, got: " <> show other)
          fired <- timeout 15000000 (takeMVar released)
          case fired of
            Just _ -> pure ()
            Nothing -> fail "the losing step's token never fired",
      testCase "a cancelled child is an awaited cancellation in the parent" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-awaited-cancel-" <> Text.take 12 suffix
            appVersion = "hs-l2-awaited-cancel-version-" <> suffix
            executorId = "hs-l2-awaited-cancel-executor-" <> suffix
            parentText = "hs-l2-awaited-cancel-parent-" <> suffix
            childText = parentText <> "-0"
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "parent"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            childBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody () _ = threadDelay 30000000 >> pure (Right 1)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          -- Its own budget, so the child cancels itself while the parent
          -- waits. That is the awaited workflow's outcome, not the
          -- parent's own cancellation.
          let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
              parentBody () ctx = do
                started <- startChildWorkflow ctx childRef (startOptionsDefault {startTimeout = Explicit (millisDuration 300)}) Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWf ctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          parentRegistered <- registerDBOSWorkflow dbos parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- timeout 15000000 (runWf dbos parentKey (WorkflowId parentText) Nothing)
          case ran of
            Just (Left (AwaitedWorkflowCancelled {workflowId})) -> workflowId @?= childText
            other -> fail ("expected an awaited cancellation, got: " <> show other)
          reader <- getBackend
          listed <- listWorkflowSteps reader (WorkflowId parentText) True Nothing Nothing Nothing
          case listed of
            Right rows -> case [row | row <- rows, row.stepRecordStepName == "DBOS.getResult"] of
              [awaitRow] ->
                case awaitRow.stepRecordError of
                  Just recorded ->
                    case decodeErrorText recorded :: Either Text (Error EngineOnly) of
                      Right (AwaitedWorkflowCancelled {workflowId}) -> workflowId @?= childText
                      other -> fail ("expected a recorded awaited cancellation, got: " <> show other)
                  Nothing -> fail "the await recorded no error"
              other -> fail ("expected one recorded await, got: " <> show other)
            other -> fail ("expected the parent's steps, got: " <> show other)
          parentRow <- getWorkflow reader (WorkflowId parentText)
          case parentRow of
            Right (Just row) -> row.workflowRecordStatus @?= Error
            other -> fail ("expected the failed parent row, got: " <> show other)
          childRow <- getWorkflow reader (WorkflowId childText)
          case childRow of
            Right (Just row) -> row.workflowRecordStatus @?= Cancelled
            other -> fail ("expected the cancelled child row, got: " <> show other),
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
            childBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody () _ = pure (Right 5)
        bracket (newDBOS config) shutdown $ \first -> do
          childRegistered <- registerDBOSWorkflowRef first childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          -- First process: the child finishes and the await is recorded,
          -- then the parent is killed before it can finish.
          let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
              parentBody () ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWf ctx wfHandle
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
          started <- launchWithEnvironment first isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          _ <- startWfRef first parentRef (startOptionsDefault {startWorkflowId = Just parentText}) Nothing
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
          let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
              parentBody () ctx = do
                started <- startChildWorkflow ctx childRef2 startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWf ctx wfHandle
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
          relaunched <- launchWithEnvironment second isolatedEnvironment
          case relaunched of
            Left err -> fail (show err)
            Right () -> pure ()
          settled <- timeout 15000000 (waitForWorkflow second (WorkflowId parentText))
          case settled of
            Just (Right (AwaitedSucceeded (Just output) _)) -> do
              let stored = SerializedWorkflowValue output Nothing
                  decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the replayed parent read the recorded await" (Right 5) decoded
            other -> fail ("expected the replayed parent to finish, got: " <> show other),
      testCase "a child inherits its parent's deadline" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-inherit-deadline-" <> Text.take 12 suffix
            appVersion = "hs-l2-inherit-deadline-version-" <> suffix
            executorId = "hs-l2-inherit-deadline-executor-" <> suffix
            parentText = "hs-l2-inherit-deadline-parent-" <> suffix
            childText = parentText <> "-0"
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "parent"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            childBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody () _ = pure (Right 1)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
              parentBody () ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWf ctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          parentRegistered <- registerDBOSWorkflowRef dbos parentKey parentBody
          parentRef <- case parentRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runWfRef dbos parentRef (runOptionsDefault {runWorkflowId = Just parentText, runTimeout = Explicit (secondsDuration 300)}) Nothing
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          reader <- getBackend
          parentRow <- getWorkflow reader (WorkflowId parentText)
          childRow <- getWorkflow reader (WorkflowId childText)
          case (parentRow, childRow) of
            (Right (Just parent), Right (Just child)) -> do
              parentDeadline <- case parent.workflowRecordDeadline of
                Just deadline' -> pure deadline'
                Nothing -> fail "the parent has no deadline"
              assertEqual "the same instant, not a fresh budget" (Just parentDeadline) child.workflowRecordDeadline
              child.workflowRecordTimeout @?= Nothing
              parent.workflowRecordTimeout @?= Just (secondsDuration 300)
            other -> fail ("expected both rows, got: " <> show other),
      testCase "a child's own timeout replaces the inherited deadline" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-child-budget-" <> Text.take 12 suffix
            appVersion = "hs-l2-child-budget-version-" <> suffix
            executorId = "hs-l2-child-budget-executor-" <> suffix
            parentText = "hs-l2-child-budget-parent-" <> suffix
            childText = parentText <> "-0"
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "parent"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            childBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody () _ = pure (Right 1)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
              parentBody () ctx = do
                started <- startChildWorkflow ctx childRef (startOptionsDefault {startTimeout = Explicit (secondsDuration 3600)}) Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWf ctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          parentRegistered <- registerDBOSWorkflowRef dbos parentKey parentBody
          parentRef <- case parentRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runWfRef dbos parentRef (runOptionsDefault {runWorkflowId = Just parentText, runTimeout = Explicit (secondsDuration 60)}) Nothing
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          reader <- getBackend
          parentRow <- getWorkflow reader (WorkflowId parentText)
          childRow <- getWorkflow reader (WorkflowId childText)
          case (parentRow, childRow) of
            (Right (Just parent), Right (Just child)) -> do
              parentDeadline <- case parent.workflowRecordDeadline of
                Just deadline' -> pure deadline'
                Nothing -> fail "the parent has no deadline"
              childDeadline <- case child.workflowRecordDeadline of
                Just deadline' -> pure deadline'
                Nothing -> fail "the child has no deadline"
              assertBool "the child's own timeout won: it outlives its parent" (SystemDB.timestampToEpochMs childDeadline > SystemDB.timestampToEpochMs parentDeadline)
              child.workflowRecordTimeout @?= Just (secondsDuration 3600)
            other -> fail ("expected both rows, got: " <> show other),
      testCase "a child can decline the inherited deadline" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-decline-deadline-" <> Text.take 12 suffix
            appVersion = "hs-l2-decline-deadline-version-" <> suffix
            executorId = "hs-l2-decline-deadline-executor-" <> suffix
            parentText = "hs-l2-decline-deadline-parent-" <> suffix
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "parent"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            childBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody () _ = pure (Right 1)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          -- Two children under one bounded parent: the first says nothing,
          -- the second declines. Together they are the difference the
          -- timeout sum exists for.
          let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
              parentBody () ctx = do
                let childPair opts = do
                      started <- startChildWorkflow ctx childRef opts Nothing
                      case started of
                        Left err -> pure (Left err)
                        Right wfHandle -> do
                          awaited <- awaitWf ctx wfHandle
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
          parentRegistered <- registerDBOSWorkflowRef dbos parentKey parentBody
          parentRef <- case parentRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runWfRef dbos parentRef (runOptionsDefault {runWorkflowId = Just parentText, runTimeout = Explicit (secondsDuration 300)}) Nothing
          case ran of
            Left err -> fail (show err)
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "both children ran" (Right 2) decoded
            other -> fail ("expected the parent's output, got: " <> show other)
          reader <- getBackend
          parentRow <- getWorkflow reader (WorkflowId parentText)
          inheritedRow <- getWorkflow reader (WorkflowId (parentText <> "-0"))
          detachedRow <- getWorkflow reader (WorkflowId (parentText <> "-2"))
          case (parentRow, inheritedRow, detachedRow) of
            (Right (Just parent), Right (Just inheritedChild), Right (Just detachedChild)) -> do
              parentDeadline <- case parent.workflowRecordDeadline of
                Just deadline' -> pure deadline'
                Nothing -> fail "the parent has no deadline"
              assertEqual "silence inherits the parent's instant verbatim" (Just parentDeadline) inheritedChild.workflowRecordDeadline
              detachedChild.workflowRecordDeadline @?= Nothing
              detachedChild.workflowRecordTimeout @?= Nothing
            other -> fail ("expected all three rows, got: " <> show other),
      testCase "a parent and its child hit an inherited deadline independently" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-cascade-deadline-" <> Text.take 12 suffix
            appVersion = "hs-l2-cascade-deadline-version-" <> suffix
            executorId = "hs-l2-cascade-deadline-executor-" <> suffix
            parentText = "hs-l2-cascade-deadline-parent-" <> suffix
            childText = parentText <> "-0"
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "parent"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            childBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody () _ = threadDelay 30000000 >> pure (Right 1)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
              parentBody () ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWf ctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          parentRegistered <- registerDBOSWorkflowRef dbos parentKey parentBody
          parentRef <- case parentRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runWfRef dbos parentRef (runOptionsDefault {runWorkflowId = Just parentText, runTimeout = Explicit (millisDuration 400)}) Nothing
          case ran of
            Left (ErrorSystemDatabase (SystemDB.WorkflowCancelled {workflowId})) -> workflowId @?= parentText
            other -> fail ("expected the parent's own deadline cancellation, got: " <> show other)
          reader <- getBackend
          let childSettled = go (200 :: Int)
                where
                  go 0 = fail "the child was not cancelled by the deadline it inherited"
                  go n = do
                    row <- getWorkflow reader (WorkflowId childText)
                    case row of
                      Right (Just child) | child.workflowRecordStatus == Cancelled -> pure ()
                      _ -> threadDelay 50000 >> go (n - 1)
          childSettled
          -- The interrupted await checkpointed nothing: the wait was
          -- answered by nobody, so a resumed parent asks the child's
          -- then-settled row again.
          listed <- listWorkflowSteps reader (WorkflowId parentText) False Nothing Nothing Nothing
          case listed of
            Right [StepRecord {stepRecordStepName = name}] -> name @?= "child"
            other -> fail ("expected the lone start only, got: " <> show other),
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
            childBody :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let parentBody :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Text)
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
          first <- runWf dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int)))
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
          retrieved <- retrieveWf dbos (WorkflowId childId)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              result <- resultWf handle
              case result of
                Right (Just stored) -> do
                  let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                  assertEqual "recovery runs the recorded child" (Right 42) decoded
                other -> fail (show other)
          replayed <- runWf dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int)))
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
            childBody :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let badBody :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Text)
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
          result <- runWf dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int)))
          case result of
            Left (InsideStep operation) -> operation @?= "starting a workflow"
            other -> fail ("expected the leaf refusal, got: " <> show other),
      testCase "a child that fails differently is started through lift" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-lift-" <> Text.take 12 suffix
            billText = "hs-l2-lift-parent-" <> suffix
            shipText = billText <> "-0"
            shipKey = newWorkflowKey "ship"
            billKey = newWorkflowKey "bill"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          let shipBody :: () -> Ctx IO -> IO (Either (Error Refused) ())
              shipBody () _ = pure (Left (application Refused))
          shipRegistered <- registerRefOf @Refused dbos shipKey shipBody
          shipRef <- case shipRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let billBody () ctx = do
                started <- startChildWorkflow ctx shipRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right handle -> do
                    awaited <- awaitChild ctx handle
                    let refusedChild = case awaited of
                          Left (Application Refused) -> True
                          _ -> False
                    marker <- nextStepMarker ctx
                    refusedStart <- withAttempt ctx marker (firstStepStatus 2) $ \inner -> do
                      inside <- startChildWorkflow inner shipRef startOptionsDefault Nothing
                      pure (case inside of
                        Left err -> Left err
                        Right _ -> Right ())
                    pure $ case refusedStart of
                      Left (InsideStep _) -> Right refusedChild
                      Left err -> Left err
                      Right () -> Right False
          billRegistered <- registerRefOf @GaveUp dbos billKey billBody
          billRef <- case billRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runDBOSWorkflowRef dbos billRef (runOptionsDefault {runWorkflowId = Just billText}) (Just (encodeWorkflowValue ()))
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Bool
              decoded @?= Right True
            other -> fail ("expected the parent to report the child's refusal, got: " <> show other)
          reader <- getBackend
          childRow <- getWorkflow reader (WorkflowId shipText)
          case childRow of
            Right (Just row) -> do
              row.workflowRecordStatus @?= Error
              case row.workflowRecordError of
                Just recorded -> case decodeErrorText recorded :: Either Text (Error Refused) of
                  Right (Application Refused) -> pure ()
                  other -> fail ("expected the child's own error in the column, got: " <> show other)
                Nothing -> fail "the child recorded no error"
            other -> fail ("expected the child row, got: " <> show other),
      testCase "a foreign error is converted at the boundary" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-foreign-" <> Text.take 12 suffix
            payText = "hs-l2-foreign-pay-" <> suffix
            payKey = newWorkflowKey "pay"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          let payBody :: () -> Ctx IO -> IO (Either (Error PaymentError) ())
              payBody () _ = do
                charged <- charge
                pure $ case charged of
                  Left refused -> Left (application (Gateway (Text.pack (show refused))))
                  Right () -> Right ()
          payRegistered <- registerDBOSWorkflow dbos payKey payBody
          case payRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runDBOSWorkflow dbos payKey (WorkflowId payText) (Just (encodeWorkflowValue ()))
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
            childBody :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          -- Started, and the handle dropped without ever being awaited: the
          -- start row is what makes a child adoptable, not the await.
          let parentBody :: Int -> Ctx IO -> IO (Either (Error EngineOnly) ())
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
          ran <- runWf dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int)))
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
            childBody :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody n _ = threadDelay 400000 >> pure (Right n)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          -- Start all three first, then collect: awaiting inside the
          -- first loop would serialize them.
          let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
              parentBody () ctx = do
                started <- mapM (\n -> startChildWorkflow ctx childRef startOptionsDefault (Just (encodeWorkflowValue (n :: Int)))) [0, 1, 2]
                case sequence started of
                  Left err -> pure (Left err)
                  Right handles -> do
                    results <- mapM (awaitWf ctx) handles
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
          ran <- runWf dbos parentKey (WorkflowId parentText) Nothing
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
            childBody :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
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
                  startWfRef
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
            childBody :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody _ _ = pure (Right 7)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let parentBody :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Text)
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
          first <- runWf dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int)))
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
          retrieved <- retrieveWf dbos (WorkflowId chosenText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              result <- resultWf handle
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
            body :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
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
          ran <- runWf dbos key (WorkflowId workflowText) Nothing
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
            childBody :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
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
          let parentBody :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
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
          worker <- async (runWf dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int))))
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
      testCase "a child started through another instance is refused" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            otherName = "hs-l2-wrong-other-" <> Text.take 12 suffix
            ownerName = "hs-l2-wrong-owner-" <> Text.take 12 suffix
            parentText = "hs-l2-wrong-instance-parent-" <> suffix
            childText = parentText <> "-0"
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "parent"
        otherConfig0 <- configFromEnv otherName
        ownerConfig0 <- configFromEnv ownerName
        let otherConfig = otherConfig0 {configAppVersion = Just ("other-v-" <> suffix), configExecutorId = Just ("other-exec-" <> suffix)}
            ownerConfig = ownerConfig0 {configAppVersion = Just ("owner-v-" <> suffix), configExecutorId = Just ("owner-exec-" <> suffix)}
            childBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody () _ = pure (Right 1)
        bracket (newDBOS otherConfig) shutdown $ \other ->
          bracket (newDBOS ownerConfig) shutdown $ \owner -> do
            childRegistered <- registerDBOSWorkflowRef other childKey childBody
            childRef <- case childRegistered of
              Left err -> fail (show err)
              Right ref -> pure ref
            let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
                parentBody () ctx = do
                  started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                  case started of
                    Left err -> pure (Left err)
                    Right wfHandle -> do
                      awaited <- awaitWf ctx wfHandle
                      pure $ case awaited of
                        Left err -> Left err
                        Right (Just stored) ->
                          case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                            Right value -> Right value
                            Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                        Right Nothing -> Left (StepFailed "parent" "no child output")
            parentRegistered <- registerDBOSWorkflow owner parentKey parentBody
            case parentRegistered of
              Left err -> fail (show err)
              Right () -> pure ()
            launchedOther <- launchWithEnvironment other isolatedEnvironment
            case launchedOther of
              Left err -> fail (show err)
              Right () -> pure ()
            launchedOwner <- launchWithEnvironment owner isolatedEnvironment
            case launchedOwner of
              Left err -> fail (show err)
              Right () -> pure ()
            ran <- runWf owner parentKey (WorkflowId parentText) Nothing
            case ran of
              Left (WrongInstance {operation}) -> assertBool ("names the call in " <> Text.unpack operation) ("workflow" `Text.isInfixOf` operation)
              other -> fail ("expected a wrong-instance refusal, got: " <> show other)
            -- Refused before anything was written: no child, and no start
            -- on the parent.
            reader <- getBackend
            missing <- getWorkflow reader (WorkflowId childText)
            missing @?= Right Nothing
            listed <- listWorkflowSteps reader (WorkflowId parentText) False Nothing Nothing Nothing
            listed @?= Right [],
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
            childBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody () _ = pure (Right 9)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
              parentBody () ctx = do
                started <- startChildWorkflow ctx childRef (startOptionsDefault {startQueue = Just joinQueue}) Nothing
                case started of
                  Left err -> pure (Left err)
                  Right handle -> do
                    result <- awaitWf ctx handle
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
          holder <- startWfRef dbos childRef (startOptionsDefault {startWorkflowId = Just holderText, startQueue = Just holderQueue}) Nothing
          case holder of
            Left err -> fail (show err)
            Right _ -> pure ()
          outcome <- timeout 30000000 (runWf dbos parentKey (WorkflowId parentText) Nothing)
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
      testCase "a zero-argument workflow records no input" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-zero-" <> Text.take 12 suffix
            workflowText = "hs-l2-zero-id-" <> suffix
            key = newWorkflowKey "zero"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: () -> Ctx IO -> IO (Either (Error EngineOnly) ())
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
          ran <- runWf dbos key (WorkflowId workflowText) Nothing
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
            body :: () -> Ctx IO -> IO (Either (Error EngineOnly) Bool)
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
          ran <- runWf dbos key (WorkflowId workflowText) Nothing
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
            body :: () -> Ctx IO -> IO (Either (Error EngineOnly) ())
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
          outcome <- try (runWf dbos key (WorkflowId workflowText) Nothing)
          case (outcome :: Either SomeException (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))) of
            Left _ -> pure ()
            Right other -> fail ("expected the body's exception to escape, got: " <> show other)
          retrieved <- retrieveWf dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              status <- statusWf handle
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
            body :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            body value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runWf dbos key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
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
            body :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
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
          ran <- runWf dbos key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
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
            body :: () -> Ctx IO -> IO (Either (Error EngineOnly) ())
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
          ran <- runWf dbos key (WorkflowId workflowText) Nothing
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
            body :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
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
          ran <- runWf dbos key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
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
            body :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
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
          worker <- async (runWfRef dbos ref (runOptionsDefault {runWorkflowId = Just workflowText}) Nothing)
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
            body :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
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
          worker <- async (runWfRef dbos ref (runOptionsDefault {runWorkflowId = Just workflowText}) Nothing)
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
      testCase "a started workflow carries the attributes it was given" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-attributes-" <> Text.take 12 suffix
            parentText = "hs-l2-attributes-parent-" <> suffix
            tenant = "acme-" <> Text.take 12 suffix
            childKey = newWorkflowKey "child"
            parentKey = newWorkflowKey "attributed"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            childBody :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            childBody _ _ = pure (Right 9)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerDBOSWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          -- The child names nothing of its own, so it inherits nothing.
          let parentBody :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
              parentBody () ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault (Just (encodeWorkflowValue (0 :: Int)))
                case started of
                  Left err -> pure (Left err)
                  Right handle -> do
                    result <- awaitWf ctx handle
                    case result of
                      Left err -> pure (Left err)
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right n -> pure (Right n)
                          Left _ -> pure (Left (StepFailed "parent" "bad child output"))
                      Right _ -> pure (Left (StepFailed "parent" "no child output"))
          parentRegistered <- registerDBOSWorkflowRef dbos parentKey parentBody
          parentRef <- case parentRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <-
            runWfRef
              dbos
              parentRef
              (runOptionsDefault {runWorkflowId = Just parentText, runAttributes = Just (Map.singleton "tenant" (String tenant))})
              Nothing
          case ran of
            Right _ -> pure ()
            other -> fail ("expected the attributed run, got: " <> show other)
          reader <- getBackend
          parentRow <- getWorkflow reader (WorkflowId parentText)
          case parentRow of
            Right (Just row) -> case row.workflowRecordAttributes of
              Just attributes -> assertBool ("expected the tenant in " <> Text.unpack attributes) (tenant `Text.isInfixOf` attributes)
              Nothing -> fail "the parent row carries no attributes"
            other -> fail ("expected the parent row, got: " <> show other)
          childRow <- getWorkflow reader (WorkflowId (parentText <> "-0"))
          case childRow of
            Right (Just row) -> row.workflowRecordAttributes @?= Nothing
            other -> fail ("expected the child row, got: " <> show other),
      testCase "a step error is recorded in its column" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-step-err-" <> Text.take 12 suffix
            workflowText = "hs-l2-step-err-id-" <> suffix
            key = newWorkflowKey "charger"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: () -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            body () ctx = runWorkflowStepWith stepOptionsDefault ctx "charge" (const (pure (Left (StepFailed "charge" "short by 12"))))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runWf dbos key (WorkflowId workflowText) Nothing
          case ran of
            Left (StepFailed step message) -> do
              step @?= "charge"
              message @?= "short by 12"
            other -> fail ("expected the step error back, got: " <> show other)
          reader <- getBackend
          -- Payloads loaded: the flag gates output AND error together,
          -- as the oracle's @step_payloads@ does.
          listed <- listWorkflowSteps reader (WorkflowId workflowText) True Nothing Nothing Nothing
          case listed of
            Right [StepRecord {stepRecordStepName = name, stepRecordError = Just recorded}] -> do
              name @?= "charge"
              assertBool ("expected the shortfall in " <> Text.unpack recorded) ("short by 12" `Text.isInfixOf` recorded)
            other -> fail ("expected the failed step, got: " <> show other),
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
        aborted @?= 0,
      testCase "a spawn refused after abort fills its channel instead of hanging" $ do
        -- Closed-arrival kill lands before the child ever runs (this is
        -- IO with real preemption, where the race actually bites — under
        -- IOSim's cooperative scheduling the child runs anyway). Each
        -- iteration must fill promptly; the per-iteration bound turns a
        -- lost fill into a failure instead of a hung suite.
        filled <- mapM (\_ -> do
            tasks <- newTasks
            _ <- abortAll tasks
            channel <- spawnLocal (tasksSpawner tasks) (\_ -> pure ())
            timeout 1000000 (readMVar channel)
          ) [1 .. 50 :: Int]
        case [() | Nothing <- filled] of
          [] -> pure ()
          missing -> fail ("refused spawns left channels empty: " <> show (length missing)),
      testCase "a task finishing before registration is not swept as aborted" $ do
        -- A trivial body on a parallel scheduler can depart between the
        -- fork and the parent's registration. The registration must
        -- consume that early departure rather than list a dead thread, so
        -- a later sweep finds nothing to kill and counts nothing aborted.
        counts <- mapM (\_ -> do
            tasks <- newTasks
            _ <- spawnTracked tasks (pure ())
            -- Let the trivial body depart first: the sweep must find an
            -- empty registry, not the dead thread, and count nothing.
            threadDelay 1000
            abortAll tasks
          ) [1 .. 200 :: Int]
        case [count | count <- counts, count /= 0] of
          [] -> pure ()
          miscounts -> fail ("dead tasks swept as aborted: " <> show (length miscounts))
    ]

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through: each
-- pins the error channel to 'EngineOnly' (the channel the test bodies
-- declare), so a call site outside an annotated body does not leave it
-- for the compiler to guess. Local copies are deliberate — this module
-- carries only the aliases it uses, and a sibling test module repeats
-- the ones it needs.
runWf :: DBOS IO -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWf = runDBOSWorkflow

runWfRef :: DBOS IO -> WorkflowRef IO EngineOnly -> RunOptions -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWfRef = runDBOSWorkflowRef

startWfRef :: DBOS IO -> WorkflowRef IO EngineOnly -> StartOptions -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
startWfRef = startDBOSWorkflowRef

retrieveWf :: DBOS IO -> WorkflowId -> IO (Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
retrieveWf = retrieveWorkflow

awaitWf :: Ctx IO -> WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
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

-- | Register a body at its own error channel, leaving @e@ to the call
-- site: the polymorphic registration cannot infer it from a local binding,
-- and a locally written channel is the point of the lift case.
registerRefOf :: forall e a r. (FromJSON a, ToJSON r, ToJSON e) => DBOS IO -> WorkflowKey -> (a -> Ctx IO -> IO (Either (Error e) r)) -> IO (Either (Error EngineOnly) (WorkflowRef IO e))
registerRefOf = registerDBOSWorkflowRef

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
