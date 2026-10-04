{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Public-behavior tests for the @handle.rs@ workflow handle.
module DBOS.Transact.HandleTest (tests) where

import DBOS.Prelude
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (SerializedWorkflowValue (..), WorkflowId (..), WorkflowStatus (..))
import DBOS.SystemDB (NewWorkflow (..), Submission (..), getResultStepName, initWorkflow, newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  (
    EngineOnly, CodecError,
    Config (..),
    DBOS,
    Executor,
    Environment (..),
    Error (..),
    WorkflowCtx,
    Identity (..),
    WorkflowHandle,
    WorkflowId (..),
    WorkflowKey,
    awaitChild,
    configFromEnv,
    decodeWorkflowValue,
    deleteWorkflows,
    encodeWorkflowValue,
    handleResult,
    handleStatus,
    handleWorkflowId,
    launchWithEnvironment,
    newDBOS,
    newWorkflowKey,
    nullTracer,
    registerDBOSWorkflow,
    renderTransactError,
    retrieveWorkflow,
    runDBOSWorkflow,
    runWorkflowStep,
    shutdown,
    withWorkflow,
  )
import DBOS.Transact.ContextTest (connOver)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertEqual, testCase, (@?=))

-- | Launch over the isolated environment and hand back the executor.
launchHandleExec :: DBOS IO -> Environment -> IO (Executor IO)
launchHandleExec dbos env = do
  started <- launchWithEnvironment dbos env
  case started of
    Left err -> fail (show err)
    Right executor -> pure executor

-- | One backend for the whole group: the scoped-await case records under a
-- parent row, so it needs live reads as well as a launched instance.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | The application identity the scoped await installs.
handleTestIdentity :: Identity
handleTestIdentity =
  Identity
    { identityAppName = "test-app",
      identityAppVersion = "1.0.0",
      identityExecutorId = "test-executor",
      identityAppId = ""
    }

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
  testGroup
    "Workflow handle"
    [ testCase "a retrieved handle names its workflow and reads its status" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-handle-" <> Text.take 12 suffix
            appVersion = "hs-l2-handle-version-" <> suffix
            executorId = "hs-l2-handle-executor-" <> suffix
            workflowText = "hs-l2-handle-id-" <> suffix
            key = newWorkflowKey "double"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runWorkflowStep wctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchHandleExec dbos isolatedEnvironment
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          retrieved <- retrieveWf dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              handleWorkflowId handle @?= workflowText
              status <- statusWf handle
              case status of
                Right (Just _) -> pure ()
                other -> fail (show other),
      testCase "a handle result adopts the recorded output" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-handle-res-" <> Text.take 12 suffix
            appVersion = "hs-l2-handle-res-version-" <> suffix
            executorId = "hs-l2-handle-res-executor-" <> suffix
            workflowText = "hs-l2-handle-res-id-" <> suffix
            key = newWorkflowKey "double"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runWorkflowStep wctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchHandleExec dbos isolatedEnvironment
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          retrieved <- retrieveWf dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              result <- resultWf handle
              case result of
                Right (Just stored) -> do
                  let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                  assertEqual "handle adopts the recorded output" (Right 42) decoded
                other -> fail (show other),
      testCase "a handle result reports the error a failed run recorded" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-handle-fail-" <> Text.take 12 suffix
            workflowText = "hs-l2-handle-fail-id-" <> suffix
            key = newWorkflowKey "fails"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body _ _ = pure (Left (StepFailed "body" "boom"))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchHandleExec dbos isolatedEnvironment
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowValue (1 :: Int)))
          case ran of
            Left _ -> pure ()
            Right other -> fail ("expected the run to fail, got: " <> show other)
          retrieved <- retrieveWf dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              result <- resultWf handle
              case result of
                Left (StepFailed {step, message}) -> do
                  -- The polling path decodes the recorded envelope back
                  -- into the caller's channel: the failure comes back as
                  -- itself, fields and all, as the oracle's serde
                  -- round-trip pins it.
                  step @?= "body"
                  message @?= "boom"
                other -> fail ("expected the recorded failure, got: " <> show other),
      testCase "a handle over a deleted row reports its absence" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-handle-del-" <> Text.take 12 suffix
            workflowText = "hs-l2-handle-del-id-" <> suffix
            key = newWorkflowKey "delete-me"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value _ = pure (Right (value + 1))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchHandleExec dbos isolatedEnvironment
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowValue (1 :: Int)))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          deleted <- deleteWorkflows dbos [WorkflowId workflowText] True
          case deleted of
            Left err -> fail (show err)
            Right count -> count @?= 1
          retrieved <- retrieveWf dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              status <- statusWf handle
              status @?= Right Nothing,
      testCase "dropping a handle does not stop the workflow" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-handle-drop-" <> Text.take 12 suffix
            workflowText = "hs-l2-handle-drop-id-" <> suffix
            key = newWorkflowKey "double"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runWorkflowStep wctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchHandleExec dbos isolatedEnvironment
          worker <- async (runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int))))
          -- Retrieved and immediately dropped while the run is in flight.
          _ <- retrieveWf dbos (WorkflowId workflowText)
          outcome <- wait worker
          case outcome of
            Left err -> fail (show err)
            Right _ -> pure ()
          retrieved <- retrieveWf dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              result <- resultWf handle
              case result of
                Right (Just stored) -> do
                  let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                  assertEqual "a fresh handle reads the completed result" (Right 42) decoded
                other -> fail (show other),
      testCase "a scoped await records the child's result under the parent" $ do
        backend <- getBackend
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-handle-await-" <> Text.take 12 suffix
            childText = "hs-l2-handle-await-child-" <> suffix
            parentText = "hs-l2-handle-await-parent-" <> suffix
            key = newWorkflowKey "double"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runWorkflowStep wctx "double" (const (pure (value * 2)))
            parentRow = (newWorkflow parentText) {newWorkflowName = Just "L2HandleAwaiter"}
        created <- initWorkflow backend parentRow Nothing Fresh Nothing
        case created of
          Left err -> fail (show err)
          Right _ -> pure ()
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchHandleExec dbos isolatedEnvironment
          ran <- runWf exec key (WorkflowId childText) (Just (encodeWorkflowValue (21 :: Int)))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          retrieved <- retrieveWf dbos (WorkflowId childText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              conn <- connOver backend nullTracer
              awaited <- withWorkflow conn handleTestIdentity (WorkflowId parentText) Nothing $ \wctx ->
                awaitChild wctx handle
              case awaited of
                Right (Just stored) -> do
                  let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                  assertEqual "the scoped await adopts the recorded output" (Right 42) decoded
                other -> fail (show other)
              steps <- SystemDB.listWorkflowSteps backend (WorkflowId parentText) True Nothing Nothing Nothing
              case steps of
                Right rows -> map (.stepRecordStepName) rows @?= [getResultStepName]
                other -> fail (show other)
    ]

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through. Local
-- copies are deliberate: this module carries only the aliases it uses.
runWf :: Executor IO -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWf = runDBOSWorkflow

retrieveWf :: DBOS IO -> WorkflowId -> IO (Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
retrieveWf = retrieveWorkflow

resultWf :: WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWf = handleResult

statusWf :: WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe WorkflowStatus))
statusWf = handleStatus

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }
