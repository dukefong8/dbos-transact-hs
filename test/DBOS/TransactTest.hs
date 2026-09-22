{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module DBOS.TransactTest
  ( tests,
  )
where

import Data.Text (Text)
import Bluefin.Eff (runEff)
import DBOS.Transact
  ( ApplicationVersion (..),
    AwaitedWorkflowResult (..),
    ExecutorId (..),
    Millis (..),
    OperationCheckpoint (..),
    OperationCheckpointDecodeError (..),
    OperationCheckpointReplay (..),
    OperationCheckpointReplayError (..),
    OperationCheckpointResult (..),
    OperationExecutionCheckError (..),
    OperationId (..),
    OperationName (..),
    SerializedWorkflowValue (..),
    Serialization (..),
    WorkflowExecution (..),
    WorkflowExecutionDecodeError (..),
    WorkflowExecutionRow (..),
    WorkflowId (..),
    WorkflowName (..),
    WorkflowOutcome (..),
    WorkflowStatus (..),
    WorkflowStatusDecodeError (..),
    checkOperationExecution,
    getWorkflowExecution,
    nullLogAction,
    parseOperationCheckpoint,
    parseWorkflowExecution,
    parseWorkflowStatus,
    replayOperationCheckpoint,
    withOperationCheckpointStore,
    withWorkflowExecutionStore,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), testCase)

tests :: TestTree
tests =
  testGroup
    "DBOS Transact"
    [ workflowExecutionTests,
      operationCheckpointTests
    ]

workflowExecutionTests :: TestTree
workflowExecutionTests =
  testGroup
    "Workflow Execution"
    [ testCase "parses Python DBOS workflow status strings" $ do
        traverse parseWorkflowStatus knownPythonStatuses
          @?= Right
            [ Pending,
              Success,
              Error,
              MaxRecoveryAttemptsExceeded,
              Cancelled,
              Enqueued,
              Delayed
            ],
      testCase "rejects unknown Python DBOS workflow status strings" $
        parseWorkflowStatus "RUNNING" @?= Left (UnknownWorkflowStatus "RUNNING"),
      testCase "parses a completed workflow execution row with output" $
        parseWorkflowExecution completedWorkflowRow
          @?= Right completedWorkflowExecution,
      testCase "parses Python DBOS recovery attempts from workflow status rows" $
        (.workflowExecutionRecoveryAttempts) <$> parseWorkflowExecution completedWorkflowRow
          @?= Right (Just 1),
      testCase "parses Python DBOS parent workflow links for child workflows" $
        (.workflowExecutionParentId) <$> parseWorkflowExecution childWorkflowRow
          @?= Right (Just (WorkflowId "parent-wf")),
      testCase "parses direct-inserted Python workflow inputs with row serialization" $
        ((.workflowExecutionInputs) <$> parseWorkflowExecution directInsertedWorkflowRow)
          @?= Right
            ( Just
                SerializedWorkflowValue
                  { serializedText = "{\"positionalArgs\":[\"s\",1,{\"k\":\"k\",\"v\":[\"v\"]}]}",
                    serializedSerialization = Just (Serialization "portable_json")
                  }
            ),
      testCase "does not parse cancelled Python workflows as successful output" $
        (.workflowExecutionOutcome) <$> parseWorkflowExecution cancelledWorkflowRow
          @?= Right (Just WorkflowCancelled),
      testCase "rejects a workflow execution row with both output and error" $
        parseWorkflowExecution conflictingWorkflowRow
          @?= Left (WorkflowExecutionConflict conflictingWorkflowRow),
      testCase "gets a parsed workflow execution from a scoped store capability" $ do
        result <-
          runEff $ \io ->
            withWorkflowExecutionStore fetchCompletedWorkflowRow $ \store ->
              getWorkflowExecution io nullLogAction store (WorkflowId "wf-1")
        result @?= Right (Just completedWorkflowExecution),
      testCase "gets no workflow execution from a scoped store capability when missing" $ do
        result <-
          runEff $ \io ->
            withWorkflowExecutionStore fetchMissingWorkflowRow $ \store ->
              getWorkflowExecution io nullLogAction store (WorkflowId "wf-missing")
        result @?= Right Nothing
    ]

operationCheckpointTests :: TestTree
operationCheckpointTests =
  testGroup
    "Operation Checkpoints"
    [ testCase "runs an operation when Python DBOS has no checkpoint" $
        replayOperationCheckpoint (OperationName "TryConcExec.testConcStep") Nothing
          @?= Right RunOperation,
      testCase "aborts operation execution checks when Python workflow status is cancelled" $ do
        result <-
          runEff $ \io ->
            withOperationCheckpointStore fetchCancelledWorkflowStatus fetchCheckpoint $ \store ->
              checkOperationExecution
                io
                nullLogAction
                store
                (WorkflowId "cancelled-workflow-id")
                (OperationId 1)
                (OperationName "TryConcExec.testConcStep")
        result @?= Left (WorkflowExecutionCancelled (WorkflowId "cancelled-workflow-id")),
      testCase "parses a Python operation_outputs row with serialized output" $
        parseOperationCheckpoint
          (OperationId 1)
          (OperationName "TryConcExec.testConcStep")
          (Just stepOutput)
          Nothing
          Nothing
          Nothing
          Nothing
          @?= Right successfulStepCheckpoint,
      testCase "parses Python operation_outputs timing for step sequencing" $
        parseOperationCheckpoint
          (OperationId 2)
          (OperationName "TryConcExec2.step2")
          (Just stepOutput)
          Nothing
          Nothing
          (Just (Millis 100))
          (Just (Millis 120))
          @?= Right sequencedStepCheckpoint,
      testCase "parses a Python get-result checkpoint link with serialized output" $
        parseOperationCheckpoint
          (OperationId 3)
          (OperationName "DBOS.getResult")
          (Just stepOutput)
          Nothing
          (Just (WorkflowId "child-workflow-id"))
          Nothing
          Nothing
          @?= Right
            ( OperationCheckpoint
                { checkpointOperationId = OperationId 3,
                  checkpointOperationName = OperationName "DBOS.getResult",
                  checkpointStartedAt = Nothing,
                  checkpointCompletedAt = Nothing,
                  checkpointResult =
                    CheckpointAwaitedWorkflowResult
                      (WorkflowId "child-workflow-id")
                      (AwaitedWorkflowOutput stepOutput)
                }
            ),
      testCase "rejects a Python operation_outputs row with both output and error" $
        parseOperationCheckpoint
          (OperationId 4)
          (OperationName "TryConcExec.testConcStep")
          (Just stepOutput)
          (Just stepError)
          Nothing
          Nothing
          Nothing
          @?= Left
            ( ConflictingOperationCheckpointValues
                (OperationId 4)
                (OperationName "TryConcExec.testConcStep")
            ),
      testCase "replays a matching Python DBOS checkpoint output" $
        replayOperationCheckpoint
          (OperationName "TryConcExec.testConcStep")
          (Just successfulStepCheckpoint)
          @?= Right (ReplayOperation (CheckpointOutput stepOutput)),
      testCase "rejects a checkpoint recorded for a different operation name" $
        replayOperationCheckpoint
          (OperationName "TryConcExec.otherStep")
          (Just successfulStepCheckpoint)
          @?= Left
            ( UnexpectedOperationName
                (OperationId 1)
                (OperationName "TryConcExec.otherStep")
                (OperationName "TryConcExec.testConcStep")
            ),
      testCase "replays a child workflow checkpoint link" $
        replayOperationCheckpoint
          (OperationName "TryConcExec.childWorkflow")
          (Just childWorkflowCheckpoint)
          @?= Right
            ( ReplayOperation
                (CheckpointChildWorkflow (WorkflowId "child-workflow-id"))
            )
    ]

knownPythonStatuses :: [Text]
knownPythonStatuses =
  [ "PENDING",
    "SUCCESS",
    "ERROR",
    "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
    "CANCELLED",
    "ENQUEUED",
    "DELAYED"
  ]

completedWorkflowRow :: WorkflowExecutionRow
completedWorkflowRow =
  WorkflowExecutionRow
    { rowWorkflowId = WorkflowId "wf-1",
      rowWorkflowStatus = "SUCCESS",
      rowWorkflowName = Just "testConcWorkflow",
      rowWorkflowParentId = Nothing,
      rowWorkflowInputs = Nothing,
      rowWorkflowOutput =
        Just
          SerializedWorkflowValue
            { serializedText = "{\"ok\":true}",
              serializedSerialization = Just (Serialization "json")
            },
      rowWorkflowError = Nothing,
      rowWorkflowExecutor = Just "local",
      rowWorkflowCreatedAt = Just (Millis 1),
      rowWorkflowUpdatedAt = Just (Millis 2),
      rowWorkflowRecoveryAttempts = Just 1,
      rowWorkflowQueueName = Just "default",
      rowWorkflowSerialization = Just "json",
      rowWorkflowApplicationVersion = Just "v1"
    }

completedWorkflowExecution :: WorkflowExecution
completedWorkflowExecution =
  WorkflowExecution
    { workflowExecutionId = WorkflowId "wf-1",
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

childWorkflowRow :: WorkflowExecutionRow
childWorkflowRow =
  completedWorkflowRow
    { rowWorkflowId = WorkflowId "parent-wf-1",
      rowWorkflowParentId = Just (WorkflowId "parent-wf"),
      rowWorkflowName = Just "child_workflow"
    }

directInsertedWorkflowRow :: WorkflowExecutionRow
directInsertedWorkflowRow =
  completedWorkflowRow
    { rowWorkflowId = WorkflowId "direct-inserted-wf",
      rowWorkflowStatus = "ENQUEUED",
      rowWorkflowName = Just "workflowPortable",
      rowWorkflowInputs = Just "{\"positionalArgs\":[\"s\",1,{\"k\":\"k\",\"v\":[\"v\"]}]}",
      rowWorkflowOutput = Nothing,
      rowWorkflowSerialization = Just "portable_json",
      rowWorkflowQueueName = Just "testq"
    }

cancelledWorkflowRow :: WorkflowExecutionRow
cancelledWorkflowRow =
  completedWorkflowRow
    { rowWorkflowId = WorkflowId "cancelled-wf",
      rowWorkflowStatus = "CANCELLED",
      rowWorkflowOutput =
        Just
          SerializedWorkflowValue
            { serializedText = "5",
              serializedSerialization = Just (Serialization "json")
            }
    }

conflictingWorkflowRow :: WorkflowExecutionRow
conflictingWorkflowRow =
  WorkflowExecutionRow
    { rowWorkflowId = WorkflowId "wf-2",
      rowWorkflowStatus = "ERROR",
      rowWorkflowName = Nothing,
      rowWorkflowParentId = Nothing,
      rowWorkflowInputs = Nothing,
      rowWorkflowOutput =
        Just
          SerializedWorkflowValue
            { serializedText = "{\"ok\":false}",
              serializedSerialization = Just (Serialization "json")
            },
      rowWorkflowError =
        Just
          SerializedWorkflowValue
            { serializedText = "{\"error\":\"boom\"}",
              serializedSerialization = Just (Serialization "json")
            },
      rowWorkflowExecutor = Nothing,
      rowWorkflowCreatedAt = Nothing,
      rowWorkflowUpdatedAt = Nothing,
      rowWorkflowRecoveryAttempts = Nothing,
      rowWorkflowQueueName = Nothing,
      rowWorkflowSerialization = Nothing,
      rowWorkflowApplicationVersion = Nothing
    }

fetchCompletedWorkflowRow :: WorkflowId -> IO (Maybe WorkflowExecutionRow)
fetchCompletedWorkflowRow workflowId =
  pure $
    if workflowId == WorkflowId "wf-1"
      then Just completedWorkflowRow
      else Nothing

fetchMissingWorkflowRow :: WorkflowId -> IO (Maybe WorkflowExecutionRow)
fetchMissingWorkflowRow =
  const (pure Nothing)

successfulStepCheckpoint :: OperationCheckpoint
successfulStepCheckpoint =
  OperationCheckpoint
    { checkpointOperationId = OperationId 1,
      checkpointOperationName = OperationName "TryConcExec.testConcStep",
      checkpointStartedAt = Nothing,
      checkpointCompletedAt = Nothing,
      checkpointResult = CheckpointOutput stepOutput
    }

sequencedStepCheckpoint :: OperationCheckpoint
sequencedStepCheckpoint =
  OperationCheckpoint
    { checkpointOperationId = OperationId 2,
      checkpointOperationName = OperationName "TryConcExec2.step2",
      checkpointStartedAt = Just (Millis 100),
      checkpointCompletedAt = Just (Millis 120),
      checkpointResult = CheckpointOutput stepOutput
    }

childWorkflowCheckpoint :: OperationCheckpoint
childWorkflowCheckpoint =
  OperationCheckpoint
    { checkpointOperationId = OperationId 2,
      checkpointOperationName = OperationName "TryConcExec.childWorkflow",
      checkpointStartedAt = Nothing,
      checkpointCompletedAt = Nothing,
      checkpointResult = CheckpointChildWorkflow (WorkflowId "child-workflow-id")
    }

stepOutput :: SerializedWorkflowValue
stepOutput =
  SerializedWorkflowValue
    { serializedText = "null",
      serializedSerialization = Just (Serialization "json")
    }

stepError :: SerializedWorkflowValue
stepError =
  SerializedWorkflowValue
    { serializedText = "{\"message\":\"boom\"}",
      serializedSerialization = Just (Serialization "json")
    }

fetchCancelledWorkflowStatus :: WorkflowId -> IO (Maybe WorkflowStatus)
fetchCancelledWorkflowStatus =
  const (pure (Just Cancelled))

fetchCheckpoint :: WorkflowId -> OperationId -> IO (Maybe OperationCheckpoint)
fetchCheckpoint _ _ =
  pure (Just successfulStepCheckpoint)
