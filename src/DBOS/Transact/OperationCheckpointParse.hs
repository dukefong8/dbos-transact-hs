module DBOS.Transact.OperationCheckpointParse
  ( parseOperationCheckpoint,
  )
where

import DBOS.Transact.OperationCheckpointTypes
  ( AwaitedWorkflowResult (..),
    OperationCheckpoint (..),
    OperationCheckpointDecodeError (..),
    OperationCheckpointResult (..),
    OperationId,
    OperationName,
  )
import DBOS.SystemDB.Types (Timestamp)
import DBOS.Transact.WorkflowExecutionTypes (SerializedWorkflowValue, WorkflowId)

parseOperationCheckpoint ::
  OperationId ->
  OperationName ->
  Maybe SerializedWorkflowValue ->
  Maybe SerializedWorkflowValue ->
  Maybe WorkflowId ->
  Maybe Timestamp ->
  Maybe Timestamp ->
  Either OperationCheckpointDecodeError OperationCheckpoint
parseOperationCheckpoint operationId operationName output errorValue childWorkflowId startedAt completedAt =
  OperationCheckpoint operationId operationName startedAt completedAt
    <$> parseCheckpointResult operationId operationName output errorValue childWorkflowId

parseCheckpointResult ::
  OperationId ->
  OperationName ->
  Maybe SerializedWorkflowValue ->
  Maybe SerializedWorkflowValue ->
  Maybe WorkflowId ->
  Either OperationCheckpointDecodeError OperationCheckpointResult
parseCheckpointResult operationId operationName output errorValue childWorkflowId =
  case (output, errorValue, childWorkflowId) of
    (Just _, Just _, _) ->
      Left (ConflictingOperationCheckpointValues operationId operationName)
    (Just value, Nothing, Nothing) ->
      Right (CheckpointOutput value)
    (Nothing, Just value, Nothing) ->
      Right (CheckpointError value)
    (Nothing, Nothing, Just workflowId) ->
      Right (CheckpointChildWorkflow workflowId)
    (Just value, Nothing, Just workflowId) ->
      Right
        ( CheckpointAwaitedWorkflowResult
            workflowId
            (AwaitedWorkflowOutput value)
        )
    (Nothing, Just value, Just workflowId) ->
      Right
        ( CheckpointAwaitedWorkflowResult
            workflowId
            (AwaitedWorkflowError value)
        )
    (Nothing, Nothing, Nothing) ->
      Left (EmptyOperationCheckpoint operationId operationName)
