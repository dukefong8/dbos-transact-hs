{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module DbosTransact.Compat.WorkflowSpec
  ( tests
  ) where

import Control.Exception (SomeException, bracket, try)
import Data.Aeson qualified as Aeson
import Data.Either (isLeft)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Time (UTCTime(..), fromGregorian, secondsToDiffTime)
import DbosTransact.Config (defaultDBOSConfig)
import DbosTransact.Error (DBOSError(..))
import DbosTransact.SystemDB.Schema (SchemaName(..))
import DbosTransact.SystemDB.Postgres qualified as Postgres
import DbosTransact.Workflow (WorkflowStatus(..), WorkflowStatusType(..))
import qualified DbosTransact.Compat.Go as Go
import Hasql.Connection qualified as Connection
import Hasql.Connection.Settings qualified as Settings
import Hasql.Session qualified as Session
import System.Environment (lookupEnv)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), assertBool, assertFailure, testCase)

tests :: TestTree
tests = testGroup "Compat.WorkflowSpec"
  [ testCase "registered workflow returns success result and status" testSimpleWorkflowSuccess
  , testCase "workflow exceptions are captured on the handle" testWorkflowErrorPropagation
  , testCase "workflow body can use placeholder runAsStep" testWorkflowWithStepPlaceholder
  , testCase "same name registration is rejected" testNameCollision
  , testCase "different explicit names allow different Haskell types" testTypedNames
  , testCase "PostgreSQL workflow persistence records outcomes" testPostgresWorkflowPersistence
  ]

testSimpleWorkflowSuccess :: IO ()
testSimpleWorkflowSuccess = do
  ctx <- Go.newDBOSContext defaultDBOSConfig
  Go.registerWorkflow ctx "increment" $ \_ (input :: Int) -> pure (input + 1)
  handle <- Go.runWorkflow ctx "increment" (41 :: Int) [Go.WithWorkflowID "wf-success"]
  Go.getWorkflowID handle @?= "wf-success"
  Go.getResult handle >>= (@?= Right (42 :: Int))
  status <- either (assertFailure . show) pure =<< Go.getStatus handle
  statusType status @?= WorkflowSuccess
  statusWorkflowId status @?= "wf-success"
  statusName status @?= "increment"

testWorkflowErrorPropagation :: IO ()
testWorkflowErrorPropagation = do
  ctx <- Go.newDBOSContext defaultDBOSConfig
  Go.registerWorkflow ctx "explode" $ \_ (_input :: ()) -> (ioError (userError "boom") :: IO Text)
  handle <- Go.runWorkflow ctx "explode" () [Go.WithWorkflowID "wf-error"] :: IO (Go.WorkflowHandle Text)
  result <- Go.getResult handle
  assertBool ("expected workflow error, got " <> show result) (isWorkflowExecutionError result)
  status <- either (assertFailure . show) pure =<< Go.getStatus handle
  statusType status @?= WorkflowError
  statusError status @?= Just "user error (boom)"

testWorkflowWithStepPlaceholder :: IO ()
testWorkflowWithStepPlaceholder = do
  ctx <- Go.newDBOSContext defaultDBOSConfig
  Go.registerWorkflow ctx "step-workflow" $ \workflowCtx (input :: Text) ->
    Go.runAsStep workflowCtx [Go.WithStepName "placeholder-step"] (pure ("step:" <> input))
  handle <- Go.runWorkflow ctx "step-workflow" ("ok" :: Text) [Go.WithWorkflowID "wf-step-placeholder"]
  Go.getResult handle >>= (@?= Right ("step:ok" :: Text))

testNameCollision :: IO ()
testNameCollision = do
  ctx <- Go.newDBOSContext defaultDBOSConfig
  Go.registerWorkflow ctx "duplicate" $ \_ (input :: Int) -> pure input
  duplicate <- try $ Go.registerWorkflow ctx "duplicate" $ \_ (input :: Int) -> pure (input + 1)
  assertBool "expected duplicate workflow registration to throw" (isLeft (duplicate :: Either SomeException ()))

testTypedNames :: IO ()
testTypedNames = do
  ctx <- Go.newDBOSContext defaultDBOSConfig
  Go.registerWorkflow ctx "int-identity" $ \_ (input :: Int) -> pure input
  Go.registerWorkflow ctx "text-identity" $ \_ (input :: Text) -> pure input
  intHandle <- Go.runWorkflow ctx "int-identity" (7 :: Int) [Go.WithWorkflowID "wf-int"]
  textHandle <- Go.runWorkflow ctx "text-identity" ("seven" :: Text) [Go.WithWorkflowID "wf-text"]
  Go.getResult intHandle >>= (@?= Right (7 :: Int))
  Go.getResult textHandle >>= (@?= Right ("seven" :: Text))

testPostgresWorkflowPersistence :: IO ()
testPostgresWorkflowPersistence = do
  settings <- testSettings
  let schemaName = SchemaName "dbos_hs_workflow_spec"
      workflowID = "pg-workflow"
  resetSchema settings schemaName
  Postgres.runMigrations settings schemaName >>= (@?= Right ())
  Postgres.insertWorkflowStatus settings schemaName (testStatus workflowID WorkflowPending) >>= (@?= Right ())
  Postgres.updateWorkflowOutcome settings schemaName workflowID (Right (Aeson.String "ok")) >>= (@?= Right ())
  Postgres.awaitWorkflowResult settings schemaName workflowID >>= (@?= Right (Just ("SUCCESS", Just "\"ok\"", Nothing)))

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
  , statusName = "pg-workflow"
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
