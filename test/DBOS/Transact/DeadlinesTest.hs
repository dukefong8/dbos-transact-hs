{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Workflow deadlines against the live backend, ported from Rust
-- @tests/deadlines.rs@: a run inside its budget is unaffected, and one that
-- outlives it is cancelled durably and reports the cancellation.
module DBOS.Transact.DeadlinesTest (tests) where

import DBOS.Prelude
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (Timestamp, WorkflowId (..), WorkflowRecord (..), WorkflowStatus (..), getWorkflow, millisDuration, secondsDuration)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  ( CodecError,
    Config (..),
    Ctx,
    Environment (..),
    Error (..),
    RunOptions (..),
    Timeout (..),
    WorkflowStatus (..),
    configFromEnv,
    decodeWorkflowValue,
    encodeWorkflowValue,
    handleStatus,
    launchWithEnvironment,
    newDBOS,
    newWorkflowKey,
    nullTracer,
    registerDBOSWorkflowRef,
    retrieveWorkflow,
    runDBOSWorkflowRef,
    runOptionsDefault,
    shutdown,
  )
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertEqual, testCase, (@?=))

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
  testGroup
    "Workflow deadlines"
    [ testCase "a workflow within its deadline is unaffected" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-within-deadline-" <> Text.take 12 suffix
            workflowText = "hs-l2-within-deadline-id-" <> suffix
            key = newWorkflowKey "quick"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: () -> Ctx IO -> IO (Either Error Int)
            body () _ = pure (Right 7)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRef dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <-
            runDBOSWorkflowRef
              dbos
              ref
              (runOptionsDefault {runWorkflowId = Just workflowText, runTimeout = Explicit (secondsDuration 30)})
              Nothing
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "a run inside its budget records its result" (Right 7) decoded
            other -> fail (show other)
          retrieved <- retrieveWorkflow dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              status <- handleStatus handle
              case status of
                Right (Just Success) -> pure ()
                other -> fail ("expected the row SUCCESS, got: " <> show other),
      testCase "a workflow past its deadline is cancelled" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-past-deadline-" <> Text.take 12 suffix
            workflowText = "hs-l2-past-deadline-id-" <> suffix
            key = newWorkflowKey "runs-forever"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: () -> Ctx IO -> IO (Either Error Int)
            body () _ = do
              threadDelay 30000000
              pure (Right 1)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRef dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <-
            runDBOSWorkflowRef
              dbos
              ref
              (runOptionsDefault {runWorkflowId = Just workflowText, runTimeout = Explicit (millisDuration 100)})
              Nothing
          case ran of
            Left (ErrorSystemDatabase (SystemDB.WorkflowCancelled {workflowId})) -> workflowId @?= workflowText
            other -> fail ("expected the cancellation, got: " <> show other)
          retrieved <- retrieveWorkflow dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              status <- handleStatus handle
              case status of
                Right (Just Cancelled) -> pure ()
                other -> fail ("expected the row CANCELLED, got: " <> show other),
      testCase "a recovered workflow keeps the deadline it already had" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-kept-deadline-" <> Text.take 12 suffix
            workflowText = "hs-l2-kept-deadline-id-" <> suffix
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
          worker <-
            async
              ( runDBOSWorkflowRef
                  dbos
                  ref
                  (runOptionsDefault {runWorkflowId = Just workflowText, runTimeout = Explicit (secondsDuration 30)})
                  Nothing
              )
          first <- waitForDeadline getBackend (WorkflowId workflowText)
          shutdown dbos
          cancel worker
          relaunched <- launchWithEnvironment dbos isolatedEnvironment
          case relaunched of
            Left err -> fail (show err)
            Right () -> pure ()
          second <- waitForDeadline getBackend (WorkflowId workflowText)
          second @?= first
          putMVar gate ()
          ran <-
            runDBOSWorkflowRef
              dbos
              ref
              (runOptionsDefault {runWorkflowId = Just workflowText, runTimeout = Explicit (secondsDuration 30)})
              Nothing
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the recovered run records its result" (Right 7) decoded
            other -> fail (show other),
      testCase "shutdown does not durably cancel a workflow that has a deadline" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-shutdown-deadline-" <> Text.take 12 suffix
            workflowText = "hs-l2-shutdown-deadline-id-" <> suffix
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
          worker <-
            async
              ( runDBOSWorkflowRef
                  dbos
                  ref
                  (runOptionsDefault {runWorkflowId = Just workflowText, runTimeout = Explicit (secondsDuration 30)})
                  Nothing
              )
          _ <- waitForDeadline getBackend (WorkflowId workflowText)
          shutdown dbos
          cancel worker
          status <- readWorkflowStatus getBackend (WorkflowId workflowText)
          status @?= Just Pending,
      testCase "a deadline that loses to a recorded outcome reports that outcome" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-beaten-deadline-" <> Text.take 12 suffix
            workflowText = "hs-l2-beaten-deadline-id-" <> suffix
            key = newWorkflowKey "quick"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: () -> Ctx IO -> IO (Either Error Int)
            body () _ = pure (Right 7)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRef dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          first <-
            runDBOSWorkflowRef
              dbos
              ref
              (runOptionsDefault {runWorkflowId = Just workflowText, runTimeout = Explicit (secondsDuration 30)})
              Nothing
          case first of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "a run inside its budget records its result" (Right 7) decoded
            other -> fail (show other)
          again <-
            runDBOSWorkflowRef
              dbos
              ref
              (runOptionsDefault {runWorkflowId = Just workflowText, runTimeout = Explicit (millisDuration 1)})
              Nothing
          case again of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the recorded outcome beats the expired budget" (Right 7) decoded
            other -> fail (show other)
          retrieved <- retrieveWorkflow dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              status <- handleStatus handle
              case status of
                Right (Just Success) -> pure ()
                other -> fail ("expected the row SUCCESS, got: " <> show other)
    ]

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted. The
-- launched instances below keep their own pools: each needs a distinct
-- application identity.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | The status a workflow row carries: a reader over the suite backend,
-- so assertions hold after shutdown.
readWorkflowStatus :: IO Postgres.PostgresSystemDB -> WorkflowId -> IO (Maybe WorkflowStatus)
readWorkflowStatus getBackend workflowId = do
  backend <- getBackend
  found <- getWorkflow backend workflowId
  case found of
    Left err -> fail (show err)
    Right Nothing -> pure Nothing
    Right (Just WorkflowRecord {workflowRecordStatus = status}) -> pure (Just status)

-- | The deadline stamped on a workflow row, waiting until the row carries
-- one: the row is written before the body starts, so a bounded poll always
-- terminates.
waitForDeadline :: IO Postgres.PostgresSystemDB -> WorkflowId -> IO Timestamp
waitForDeadline getBackend workflowId = go (50 :: Int)
  where
    go 0 = fail "the workflow row never carried a deadline"
    go n = do
      deadline <- readDeadline getBackend workflowId
      case deadline of
        Just due -> pure due
        Nothing -> threadDelay 200000 >> go (n - 1)

-- | The deadline a workflow row carries, if any: a reader over the suite
-- backend.
readDeadline :: IO Postgres.PostgresSystemDB -> WorkflowId -> IO (Maybe Timestamp)
readDeadline getBackend workflowId = do
  backend <- getBackend
  found <- getWorkflow backend workflowId
  case found of
    Left err -> fail (show err)
    Right Nothing -> pure Nothing
    Right (Just WorkflowRecord {workflowRecordDeadline = deadline}) -> pure deadline

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }
