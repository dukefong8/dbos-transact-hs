{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module DbosTransact.Compat.StepSpec
  ( tests
  ) where

import Control.Exception (SomeException, bracket, try)
import Data.Aeson (FromJSON, ToJSON)
import Data.Aeson qualified as Aeson
import Data.Either (isLeft)
import Data.IORef (newIORef, readIORef, modifyIORef')
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Time (UTCTime(..), fromGregorian, secondsToDiffTime)
import DbosTransact.Config (defaultDBOSConfig)
import DbosTransact.Error (DBOSError(..))
import qualified DbosTransact.Compat.Go as Go
import DbosTransact.SystemDB.Schema (SchemaName(..))
import DbosTransact.SystemDB.Postgres qualified as Postgres
import DbosTransact.Workflow (WorkflowStatus(..), WorkflowStatusType(..))
import GHC.Generics (Generic)
import Hasql.Connection qualified as Connection
import Hasql.Connection.Settings qualified as Settings
import Hasql.Session qualified as Session
import System.Environment (lookupEnv)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), assertBool, testCase)

data StepPayload = StepPayload
  { payloadText :: Text
  , payloadCount :: Int
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)

tests :: TestTree
tests = testGroup "Compat.StepSpec"
  [ testCase "runAsStep outside a workflow fails" testRunAsStepOutsideWorkflowFails
  , testCase "custom step names are recorded and replayed" testCustomStepNameReplay
  , testCase "user-defined JSON object roundtrips through checkpoint storage" testJsonRoundtrip
  , testCase "step retries until max retries is reached" testStepRetries
  , testCase "step does not retry when max retries is zero" testStepNoRetry
  , testCase "PostgreSQL operation persistence records step output" testPostgresOperationPersistence
  ]

testRunAsStepOutsideWorkflowFails :: IO ()
testRunAsStepOutsideWorkflowFails = do
  ctx <- Go.newDBOSContext defaultDBOSConfig
  result <- try (Go.runAsStep ctx [] (pure (1 :: Int))) :: IO (Either SomeException Int)
  assertBool "expected runAsStep outside workflow to throw" (isLeft result)

testCustomStepNameReplay :: IO ()
testCustomStepNameReplay = do
  ctx <- Go.newDBOSContext defaultDBOSConfig
  counter <- newIORef (0 :: Int)
  Go.registerWorkflow ctx "named-step" $ \workflowCtx (_input :: ()) ->
    Go.runAsStep workflowCtx [Go.WithStepName "custom"] $ do
      modifyIORef' counter (+ 1)
      pure (123 :: Int)
  firstHandle <- Go.runWorkflow ctx "named-step" () [Go.WithWorkflowID "wf-named-step"]
  secondHandle <- Go.runWorkflow ctx "named-step" () [Go.WithWorkflowID "wf-named-step"]
  Go.getResult firstHandle >>= (@?= Right (123 :: Int))
  Go.getResult secondHandle >>= (@?= Right (123 :: Int))
  readIORef counter >>= (@?= 1)

testJsonRoundtrip :: IO ()
testJsonRoundtrip = do
  ctx <- Go.newDBOSContext defaultDBOSConfig
  counter <- newIORef (0 :: Int)
  let payload = StepPayload "hello" 7
  Go.registerWorkflow ctx "json-step" $ \workflowCtx (_input :: ()) ->
    Go.runAsStep workflowCtx [Go.WithStepName "json-payload"] $ do
      modifyIORef' counter (+ 1)
      pure payload
  firstHandle <- Go.runWorkflow ctx "json-step" () [Go.WithWorkflowID "wf-json-step"]
  secondHandle <- Go.runWorkflow ctx "json-step" () [Go.WithWorkflowID "wf-json-step"]
  Go.getResult firstHandle >>= (@?= Right payload)
  Go.getResult secondHandle >>= (@?= Right payload)
  readIORef counter >>= (@?= 1)

testStepRetries :: IO ()
testStepRetries = do
  ctx <- Go.newDBOSContext defaultDBOSConfig
  attempts <- newIORef (0 :: Int)
  Go.registerWorkflow ctx "retry-step" $ \workflowCtx (_input :: ()) ->
    Go.runAsStep workflowCtx
      [ Go.WithStepName "retrying"
      , Go.WithStepMaxRetries 2
      , Go.WithBackoffFactor 1.5
      , Go.WithBaseInterval 0
      , Go.WithMaxInterval 0
      ] $ do
        modifyIORef' attempts (+ 1)
        current <- readIORef attempts
        if current < 2
          then ioError (userError "try again")
          else pure ("done" :: Text)
  handle <- Go.runWorkflow ctx "retry-step" () [Go.WithWorkflowID "wf-retry-step"]
  Go.getResult handle >>= (@?= Right ("done" :: Text))
  readIORef attempts >>= (@?= 2)

testStepNoRetry :: IO ()
testStepNoRetry = do
  ctx <- Go.newDBOSContext defaultDBOSConfig
  attempts <- newIORef (0 :: Int)
  Go.registerWorkflow ctx "no-retry-step" $ \workflowCtx (_input :: ()) ->
    Go.runAsStep workflowCtx [Go.WithStepName "no-retry", Go.WithStepMaxRetries 0] $ do
      modifyIORef' attempts (+ 1)
      (ioError (userError "fail once") :: IO Text)
  handle <- Go.runWorkflow ctx "no-retry-step" () [Go.WithWorkflowID "wf-no-retry-step"] :: IO (Go.WorkflowHandle Text)
  result <- Go.getResult handle :: IO (Either DBOSError Text)
  assertBool ("expected workflow error, got " <> show result) (isWorkflowExecutionError result)
  readIORef attempts >>= (@?= 1)

testPostgresOperationPersistence :: IO ()
testPostgresOperationPersistence = do
  settings <- testSettings
  let schemaName = SchemaName "dbos_hs_step_spec"
      workflowID = "pg-step-workflow"
      payload = Aeson.toJSON (StepPayload "stored" 3)
  resetSchema settings schemaName
  Postgres.runMigrations settings schemaName >>= (@?= Right ())
  Postgres.insertWorkflowStatus settings schemaName (testStatus workflowID WorkflowPending) >>= (@?= Right ())
  Postgres.recordOperationResult settings schemaName workflowID 1 "custom-step" (Right payload) >>= (@?= Right ())
  Postgres.checkOperationExecution settings schemaName workflowID 1 "custom-step" >>= (@?= Right (Just (Postgres.OperationSucceeded payload)))

isWorkflowExecutionError :: Either DBOSError output -> Bool
isWorkflowExecutionError (Left (WorkflowExecutionError _)) = True
isWorkflowExecutionError _ = False

testSettings :: IO Settings.Settings
testSettings = do
  host <- textEnvDefault "DBOS_TEST_PGHOST" "localhost"
  user <- textEnvDefault "DBOS_TEST_PGUSER" "postgres"
  database <- textEnvDefault "DBOS_TEST_PGDATABASE" "dbos_starter_clojure"
  password <- textEnvDefault "PGPASSWORD" "dbos"
  pure $
    Settings.host host
      <> Settings.user user
      <> Settings.dbname database
      <> Settings.password password

resetSchema :: Settings.Settings -> SchemaName -> IO ()
resetSchema settings (SchemaName schemaName) =
  runTestSession settings (Session.script ("DROP SCHEMA IF EXISTS " <> schemaName <> " CASCADE;")) >>= (@?= Right ())

runTestSession :: Settings.Settings -> Session.Session a -> IO (Either Text a)
runTestSession settings session =
  bracket (Connection.acquire settings) releaseConnection $ \case
    Left err -> pure (Left (Text.pack (show err)))
    Right connection -> do
      result <- Connection.use connection session
      pure (either (Left . Text.pack . show) Right result)

releaseConnection :: Either err Connection.Connection -> IO ()
releaseConnection = either (const (pure ())) Connection.release

textEnvDefault :: String -> Text -> IO Text
textEnvDefault name fallback = do
  value <- lookupEnv name
  pure (maybe fallback Text.pack value)

testStatus :: Text -> WorkflowStatusType -> WorkflowStatus
testStatus workflowID statusType = WorkflowStatus
  { statusWorkflowId = workflowID
  , statusType = statusType
  , statusName = "pg-step-workflow"
  , statusInput = Just Aeson.Null
  , statusOutput = Nothing
  , statusError = Nothing
  , statusExecutorId = Nothing
  , statusApplicationVersion = Nothing
  , statusApplicationId = Nothing
  , statusCreatedAt = fixedTime
  , statusUpdatedAt = fixedTime
  , statusCompletedAt = Nothing
  , statusRecoveryAttempts = 0
  , statusQueueName = Nothing
  , statusWorkflowTimeout = Nothing
  , statusWorkflowDeadline = Nothing
  , statusDeduplicationId = Nothing
  , statusPriority = 0
  , statusQueuePartitionKey = Nothing
  , statusParentWorkflowId = Nothing
  , statusClassName = Nothing
  , statusConfigName = Nothing
  , statusSerialization = "json"
  , statusDelayUntil = Nothing
  }

fixedTime :: UTCTime
fixedTime = UTCTime (fromGregorian 2026 1 1) (secondsToDiffTime 0)
