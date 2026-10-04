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
  (
    EngineOnly, CodecError,
    Config (..),
    Ctx,
    WorkflowCtx,
    workflowCtxInner,
    DBOS,
    Executor,
    Environment (..),
    Error (..),
    RunOptions (..),
    SerializedWorkflowValue (..),
    Timeout (..),
    WorkflowHandle,
    WorkflowRef,
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
    registerDBOSWorkflowRefScoped,
    retrieveWorkflow,
    runDBOSWorkflowRef,
    runOptionsDefault,
    shutdown,
  )
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertEqual, testCase, (@?=))

-- | Launch over the isolated environment and hand back the executor.
launchDeadlinesExec :: DBOS IO -> Environment -> IO (Executor IO)
launchDeadlinesExec dbos env = do
  started <- launchWithEnvironment dbos env
  case started of
    Left err -> fail (show err)
    Right executor -> pure executor

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
            body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body () _ = pure (Right 7)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRefScoped dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          exec <- launchDeadlinesExec dbos isolatedEnvironment
          ran <-
            runWfRef
              exec
              ref
              (runOptionsDefault {runWorkflowId = Just workflowText, runTimeout = Explicit (secondsDuration 30)})
              Nothing
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "a run inside its budget records its result" (Right 7) decoded
            other -> fail (show other)
          retrieved <- retrieveWf dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              status <- statusWf handle
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
            body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body () _ = do
              threadDelay 30000000
              pure (Right 1)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRefScoped dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          exec <- launchDeadlinesExec dbos isolatedEnvironment
          ran <-
            runWfRef
              exec
              ref
              (runOptionsDefault {runWorkflowId = Just workflowText, runTimeout = Explicit (millisDuration 100)})
              Nothing
          case ran of
            Left (ErrorSystemDatabase (SystemDB.WorkflowCancelled {workflowId})) -> workflowId @?= workflowText
            other -> fail ("expected the cancellation, got: " <> show other)
          retrieved <- retrieveWf dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              status <- statusWf handle
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
            body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body () _ = takeMVar gate >> pure (Right 7)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRefScoped dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          exec <- launchDeadlinesExec dbos isolatedEnvironment
          worker <-
            async
              ( runWfRef
                  exec
                  ref
                  (runOptionsDefault {runWorkflowId = Just workflowText, runTimeout = Explicit (secondsDuration 30)})
                  Nothing
              )
          first <- waitForDeadline getBackend (WorkflowId workflowText)
          shutdown dbos
          cancel worker
          exec <- launchDeadlinesExec dbos isolatedEnvironment
          second <- waitForDeadline getBackend (WorkflowId workflowText)
          second @?= first
          putMVar gate ()
          ran <-
            runWfRef
              exec
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
            body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body () _ = takeMVar gate >> pure (Right 7)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRefScoped dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          exec <- launchDeadlinesExec dbos isolatedEnvironment
          worker <-
            async
              ( runWfRef
                  exec
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
            body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body () _ = pure (Right 7)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRefScoped dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          exec <- launchDeadlinesExec dbos isolatedEnvironment
          first <-
            runWfRef
              exec
              ref
              (runOptionsDefault {runWorkflowId = Just workflowText, runTimeout = Explicit (secondsDuration 30)})
              Nothing
          case first of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "a run inside its budget records its result" (Right 7) decoded
            other -> fail (show other)
          again <-
            runWfRef
              exec
              ref
              (runOptionsDefault {runWorkflowId = Just workflowText, runTimeout = Explicit (millisDuration 1)})
              Nothing
          case again of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the recorded outcome beats the expired budget" (Right 7) decoded
            other -> fail (show other)
          retrieved <- retrieveWf dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              status <- statusWf handle
              case status of
                Right (Just Success) -> pure ()
                other -> fail ("expected the row SUCCESS, got: " <> show other)
    ]

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through. Local
-- copies are deliberate: this module carries only the aliases it uses.
runWfRef :: Executor IO -> WorkflowRef IO EngineOnly -> RunOptions -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWfRef = runDBOSWorkflowRef

retrieveWf :: DBOS IO -> WorkflowId -> IO (Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
retrieveWf = retrieveWorkflow

statusWf :: WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe WorkflowStatus))
statusWf = handleStatus

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
