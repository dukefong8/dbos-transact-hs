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
import Colog.Core.Action (LogAction (..))
import DBOS.SystemDB (AwaitedOutcome (..), WorkflowId (..), Timestamp (..), addTimeout)
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
    WorkflowStatus (..),
    abortAll,
    childWorkflowId,
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
    resolveTimeoutDeadline,
    retrieveWorkflow,
    runDBOSWorkflow,
    runDBOSWorkflowRef,
    nextStepMarker,
    runOptionsDefault,
    runOptionsToStartOptions,
    runWorkflowStep,
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
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

tests :: TestTree
tests =
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
      testCase "a reference starts by id and runs to its recorded result" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-ref-" <> Text.take 12 suffix
            appVersion = "hs-l2-ref-version-" <> suffix
            executorId = "hs-l2-ref-executor-" <> suffix
            key = newWorkflowKey "double"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRef dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runDBOSWorkflowRef dbos ref runOptionsDefault (Just (encodeWorkflowValue (21 :: Int)))
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "ref run records its result" (Right 42) decoded
            other -> fail (show other)
          let startWid = "hs-l2-ref-start-" <> suffix
              startOpts = startOptionsDefault {startWorkflowId = Just startWid}
          first <- startDBOSWorkflowRef dbos ref startOpts (Just (encodeWorkflowValue (8 :: Int)))
          second <- startDBOSWorkflowRef dbos ref startOpts (Just (encodeWorkflowValue (8 :: Int)))
          case (first, second) of
            (Right firstHandle, Right secondHandle) -> do
              handleWorkflowId firstHandle @?= startWid
              handleWorkflowId secondHandle @?= startWid
              -- A start records without running: a single read sees
              -- PENDING, and awaiting now would poll a row nothing runs.
              -- (Joining via runDBOSWorkflow would join-await the same
              -- foreign claim, so the test never does that.)
              status <- handleStatus firstHandle
              case status of
                Right (Just Pending) -> pure ()
                other -> fail ("expected the started row PENDING: " <> show other)
              -- Recovery runs what a start recorded: relaunch, and the
              -- supervisor claims the pending row and executes it.
              shutdown dbos
              relaunched <- launchWithEnvironment dbos isolatedEnvironment
              case relaunched of
                Left err -> fail (show err)
                Right () -> pure ()
              retrieved <- retrieveWorkflow dbos (WorkflowId startWid)
              case retrieved of
                Left err -> fail (show err)
                Right handle -> do
                  result <- handleResult handle
                  case result of
                    Right (Just stored) -> do
                      let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                      assertEqual "recovery runs the started workflow" (Right 16) decoded
                    other -> fail (show other)
            _ -> fail "expected two joins on one id",
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
        readerConfig <- Postgres.configFromEnv
        reader <- Postgres.acquirePostgresSystemDB readerConfig (LogAction (const (pure ())))
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
            other -> fail (show other)
          Postgres.releasePostgresSystemDB reader,
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

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }
