{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module DBOS.SystemDBHasqlTest
  ( tests,
  )
where

import Bluefin.Eff (runEff)
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (concurrently)
import Control.Exception (bracket)
import Data.Aeson (Value, object, (.=))
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB
  ( MessageUUID (..),
    NotificationRow (..),
    WorkflowStartDecision (..),
    fetchNotification,
    fetchOperationCheckpoint,
    fetchWorkflowExecutionRow,
    fetchWorkflowStatus,
    fetchWorkflowStatuses,
    fetchMigrationVersion,
    recordOperationOutput,
    tryStartWorkflow,
    updateWorkflowOutcome,
  )
import DBOS.SystemDB.Postgres
  ( acquirePool,
    releasePool,
    runDbOrFail,
  )
import DBOS.Transact
  ( ApplicationVersion (..),
    ExecutorId (..),
    Millis (..),
    OperationCheckpointReplay (..),
    OperationCheckpointResult (..),
    OperationId (..),
    OperationName (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    WorkflowExecution (..),
    WorkflowExecutionDecodeError,
    WorkflowExecutionRow (..),
    WorkflowId (..),
    WorkflowName (..),
    WorkflowOutcome (..),
    WorkflowStatus (..),
    CodecError (..),
    checkOperationExecution,
    decodeWorkflowValue,
    encodeWorkflowValue,
    getWorkflowExecution,
    nullLogAction,
    parseWorkflowExecution,
    withOperationCheckpointStore,
    withWorkflowExecutionStore,
  )
import Hasql.Pool qualified as Pool
import Hasql.Session qualified as Session
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), testCase)

tests :: TestTree
tests =
  testGroup
    "System DB Hasql"
    [ testCase "expects the Rust migration ceiling this port tracks" $ do
        version <- withDBOSPool fetchMigrationVersion
        version @?= Just 108,
      testCase "gets a parsed workflow execution from live Python DBOS workflow_status rows" $
        withDBOSPool $ \pool -> do
          installFixtureRows pool
          result <-
            runEff $ \io ->
              withWorkflowExecutionStore (fetchWorkflowExecutionRow pool) $ \store ->
                getWorkflowExecution io nullLogAction store (WorkflowId workflowRowId)
          result @?= Right (Just expectedWorkflowExecution),
      testCase "checks operation execution against live Python DBOS operation_outputs rows" $
        withDBOSPool $ \pool -> do
          installFixtureRows pool
          result <-
            runEff $ \io ->
              withOperationCheckpointStore
                (fetchWorkflowStatus pool)
                (fetchOperationCheckpoint pool)
                $ \store ->
                  checkOperationExecution
                    io
                    nullLogAction
                    store
                    (WorkflowId operationWorkflowId)
                    (OperationId 1)
                    (OperationName "TryConcExec.testConcStep")
          result @?= Right (ReplayOperation (CheckpointOutput stepOutput)),
      testCase "fetches live Python DBOS notifications rows" $
        withDBOSPool $ \pool -> do
          installFixtureRows pool
          result <- fetchNotification pool (MessageUUID notificationMessageId)
          result @?= Just expectedNotification,
      testCase "a batch status fetch skips rows with unknown statuses" $
        withDBOSPool $ \pool -> do
          goodId <- WorkflowId . UUID.toText <$> UUID.V4.nextRandom
          badId <- WorkflowId . UUID.toText <$> UUID.V4.nextRandom
          executorId <- ExecutorId . UUID.toText <$> UUID.V4.nextRandom
          decision <- tryStartWorkflow pool goodId (WorkflowName "skipWorkflow") Nothing executorId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          let WorkflowId badText = badId
          runDbOrFail pool $
            Session.script
              ("insert into dbos.workflow_status (workflow_uuid, status, created_at, updated_at) values ('" <> badText <> "', 'BOGUS', 1, 1)")
          statuses <- fetchWorkflowStatuses pool [goodId, badId]
          statuses @?= [(goodId, Pending)],
      testCase "starts a workflow with real JSON inputs and reads them back" $
        withDBOSPool $ \pool -> do
          workflowId <- WorkflowId . UUID.toText <$> UUID.V4.nextRandom
          executorId <- ExecutorId . UUID.toText <$> UUID.V4.nextRandom
          let inputs = encodeWorkflowValue (object ["name" .= ("s" :: Text)])
          decision <- tryStartWorkflow pool workflowId (WorkflowName "inputWorkflow") (Just inputs) executorId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          fetched <- fetchWorkflowExecutionRow pool workflowId
          ((.rowWorkflowInputs) =<< fetched) @?= Just (inputs.serializedText)
          ((.rowWorkflowSerialization) =<< fetched) @?= Just "json",
      testCase "finishes a workflow with real JSON output and reads it back" $
        withDBOSPool $ \pool -> do
          workflowId <- WorkflowId . UUID.toText <$> UUID.V4.nextRandom
          executorId <- ExecutorId . UUID.toText <$> UUID.V4.nextRandom
          let output = encodeWorkflowValue (object ["ok" .= True])
          decision <- tryStartWorkflow pool workflowId (WorkflowName "outputWorkflow") Nothing executorId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          updateWorkflowOutcome pool workflowId executorId Success (Just output) Nothing
          fetched <- fetchWorkflowExecutionRow pool workflowId
          case fetched of
            Nothing -> fail "expected a workflow_status row"
            Just row -> case parseWorkflowExecution row of
              Left err -> fail ("row failed to parse: " <> show err)
              Right execution -> case execution.workflowExecutionOutcome of
                Just (WorkflowSucceeded stored) ->
                  (decodeWorkflowValue "result" (Just stored) :: Either CodecError Value)
                    @?= Right (object ["ok" .= True] :: Value)
                other -> fail ("unexpected workflow outcome: " <> show other),
      testCase "mirrors sync simple workflow single-owner and replay behavior" $
        withDBOSPool $ \pool -> do
          installSimpleWorkflowRows pool
          executions <- newIORef (0 :: Int)
          executorId <- ExecutorId . UUID.toText <$> UUID.V4.nextRandom

          (firstResult, secondResult) <-
            concurrently
              (runSimpleWorkflowAttempt pool executorId executions)
              (runSimpleWorkflowAttempt pool executorId executions)

          firstResult @?= workflowOutput
          secondResult @?= workflowOutput
          readIORef executions >>= (@?= 1)

          updateWorkflowOutcome pool simpleWorkflowId executorId Pending Nothing Nothing

          (firstRecovery, secondRecovery) <-
            concurrently
              (runSimpleWorkflowAttempt pool executorId executions)
              (runSimpleWorkflowAttempt pool executorId executions)

          firstRecovery @?= workflowOutput
          secondRecovery @?= workflowOutput
          readIORef executions >>= (@?= 1)

          replayResult <-
            runEff $ \io ->
              withOperationCheckpointStore
                (fetchWorkflowStatus pool)
                (fetchOperationCheckpoint pool)
                $ \store ->
                  checkOperationExecution
                    io
                    nullLogAction
                    store
                    simpleWorkflowId
                    simpleOperationId
                    simpleStepName
          replayResult @?= Right (ReplayOperation (CheckpointOutput stepOutput))
    ]

withDBOSPool :: (Pool.Pool -> IO a) -> IO a
withDBOSPool =
  bracket acquirePool releasePool

installFixtureRows :: Pool.Pool -> IO ()
installFixtureRows pool =
  runDbOrFail pool (Session.script fixtureSQL)

installSimpleWorkflowRows :: Pool.Pool -> IO ()
installSimpleWorkflowRows pool =
  runDbOrFail pool (Session.script simpleWorkflowCleanupSQL)

runSimpleWorkflowAttempt ::
  Pool.Pool ->
  ExecutorId ->
  IORef Int ->
  IO SerializedWorkflowValue
runSimpleWorkflowAttempt pool executorId executions = do
  decision <- tryStartWorkflow pool simpleWorkflowId simpleWorkflowName Nothing executorId (ApplicationVersion "v1")
  case decision of
    StartWorkflow -> do
      replayResult <-
        runEff $ \io ->
          withOperationCheckpointStore
            (fetchWorkflowStatus pool)
            (fetchOperationCheckpoint pool)
            $ \store ->
              checkOperationExecution
                io
                nullLogAction
                store
                simpleWorkflowId
                simpleOperationId
                simpleStepName
      case replayResult of
        Right (ReplayOperation (CheckpointOutput output)) ->
          updateWorkflowOutcome pool simpleWorkflowId executorId Success (Just workflowOutput) Nothing >> pure output
        Right RunOperation -> do
          atomicModifyIORef' executions (\count -> (count + 1, ()))
          recordOperationOutput pool simpleWorkflowId simpleOperationId simpleStepName stepOutput
          updateWorkflowOutcome pool simpleWorkflowId executorId Success (Just workflowOutput) Nothing
          pure workflowOutput
        other ->
          fail ("unexpected operation replay result: " <> show other)
    AwaitWorkflow -> do
      result <- awaitSimpleWorkflowResult pool 50
      case result of
        Right (Just execution)
          | execution.workflowExecutionOutcome == Just (WorkflowSucceeded workflowOutput) ->
              pure workflowOutput
        other ->
          fail ("unexpected workflow execution result: " <> show other)

awaitSimpleWorkflowResult ::
  Pool.Pool ->
  Int ->
  IO (Either WorkflowExecutionDecodeError (Maybe WorkflowExecution))
awaitSimpleWorkflowResult pool remaining = do
  result <-
    runEff $ \io ->
      withWorkflowExecutionStore (fetchWorkflowExecutionRow pool) $ \store ->
        getWorkflowExecution io nullLogAction store simpleWorkflowId
  case result of
    Right (Just execution)
      | execution.workflowExecutionOutcome == Just (WorkflowSucceeded workflowOutput) ->
          pure result
    _ | remaining <= 0 ->
        pure result
    _ -> do
      threadDelay 100000
      awaitSimpleWorkflowResult pool (remaining - 1)

fixtureSQL :: Text.Text
fixtureSQL =
  Text.unlines
    [ "delete from dbos.notifications where message_uuid = 'hs-message-1';",
      "delete from dbos.operation_outputs where workflow_uuid in ('hs-op-wf');",
      "delete from dbos.workflow_status where workflow_uuid in ('hs-wf-1', 'hs-op-wf', 'hs-notification-wf');",
      "insert into dbos.workflow_status (workflow_uuid, status, name, output, error, executor_id, created_at, updated_at, application_version, recovery_attempts, queue_name, inputs, serialization, priority, parent_workflow_id) values",
      "('hs-wf-1', 'SUCCESS', 'testConcWorkflow', '{\"ok\":true}', null, 'local', 1, 2, 'v1', 1, 'default', null, 'json', 0, null),",
      "('hs-op-wf', 'SUCCESS', 'operationWorkflow', 'null', null, 'local', 3, 4, 'v1', 1, 'default', null, 'json', 0, null),",
      "('hs-notification-wf', 'PENDING', 'notificationWorkflow', null, null, 'local', 5, 6, 'v1', 0, 'default', null, 'json', 0, null)",
      "on conflict (workflow_uuid) do update set status = excluded.status, name = excluded.name, output = excluded.output, error = excluded.error, executor_id = excluded.executor_id, created_at = excluded.created_at, updated_at = excluded.updated_at, application_version = excluded.application_version, recovery_attempts = excluded.recovery_attempts, queue_name = excluded.queue_name, inputs = excluded.inputs, serialization = excluded.serialization, priority = excluded.priority, parent_workflow_id = excluded.parent_workflow_id;",
      "insert into dbos.operation_outputs (workflow_uuid, function_id, function_name, output, error, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization) values",
      "('hs-op-wf', 1, 'TryConcExec.testConcStep', 'null', null, null, 100, 120, 'json')",
      "on conflict (workflow_uuid, function_id) do update set function_name = excluded.function_name, output = excluded.output, error = excluded.error, child_workflow_id = excluded.child_workflow_id, started_at_epoch_ms = excluded.started_at_epoch_ms, completed_at_epoch_ms = excluded.completed_at_epoch_ms, serialization = excluded.serialization;",
      "insert into dbos.notifications (destination_uuid, topic, message, message_uuid, serialization, consumed) values",
      "('hs-notification-wf', 'testtopic', '\"hello\"', 'hs-message-1', 'json', false)",
      "on conflict (message_uuid) do update set destination_uuid = excluded.destination_uuid, topic = excluded.topic, message = excluded.message, serialization = excluded.serialization, consumed = excluded.consumed;"
    ]

simpleWorkflowCleanupSQL :: Text.Text
simpleWorkflowCleanupSQL =
  Text.unlines
    [ "delete from dbos.operation_outputs where workflow_uuid = 'hs-simple-wf';",
      "delete from dbos.workflow_status where workflow_uuid = 'hs-simple-wf';"
    ]

workflowRowId :: Text.Text
workflowRowId = "hs-wf-1"

operationWorkflowId :: Text.Text
operationWorkflowId = "hs-op-wf"

notificationMessageId :: Text.Text
notificationMessageId = "hs-message-1"

simpleWorkflowId :: WorkflowId
simpleWorkflowId = WorkflowId "hs-simple-wf"

simpleWorkflowName :: WorkflowName
simpleWorkflowName = WorkflowName "TryConcExec.testConcWorkflow"

simpleOperationId :: OperationId
simpleOperationId = OperationId 1

simpleStepName :: OperationName
simpleStepName = OperationName "TryConcExec.testConcStep"

expectedWorkflowExecution :: WorkflowExecution
expectedWorkflowExecution =
  WorkflowExecution
    { workflowExecutionId = WorkflowId workflowRowId,
      workflowExecutionStatus = Success,
      workflowExecutionName = Just (WorkflowName "testConcWorkflow"),
      workflowExecutionParentId = Nothing,
      workflowExecutionInputs = Nothing,
      workflowExecutionOutcome =
        Just
          ( WorkflowSucceeded
              ( SerializedWorkflowValue
                  { serializedText = "{\"ok\":true}",
                    serializedSerialization = Just (Serialization "json")
                  }
              )
          ),
      workflowExecutionExecutor = Just (ExecutorId "local"),
      workflowExecutionCreatedAt = Just (Millis 1),
      workflowExecutionUpdatedAt = Just (Millis 2),
      workflowExecutionRecoveryAttempts = Just 1,
      workflowExecutionQueueName = Just "default",
      workflowExecutionSerialization = Just (Serialization "json"),
      workflowExecutionApplicationVersion = Just (ApplicationVersion "v1")
    }

stepOutput :: SerializedWorkflowValue
stepOutput =
  SerializedWorkflowValue
    { serializedText = "null",
      serializedSerialization = Just (Serialization "json")
    }

workflowOutput :: SerializedWorkflowValue
workflowOutput =
  SerializedWorkflowValue
    { serializedText = "null",
      serializedSerialization = Just (Serialization "json")
    }

expectedNotification :: NotificationRow
expectedNotification =
  NotificationRow
    { notificationDestinationId = WorkflowId "hs-notification-wf",
      notificationTopic = "testtopic",
      notificationMessage =
        SerializedWorkflowValue
          { serializedText = "\"hello\"",
            serializedSerialization = Just (Serialization "json")
          },
      notificationMessageUUID = MessageUUID notificationMessageId,
      notificationConsumed = False
    }
