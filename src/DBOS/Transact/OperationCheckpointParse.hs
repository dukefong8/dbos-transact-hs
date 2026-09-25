-- | Legacy checkpoint parsing for the starter seam: one column per result
-- shape, exactly one of which may be present. See
-- "DBOS.Transact.OperationCheckpointTypes" for the seam status; do not
-- extend.
module DBOS.Transact.OperationCheckpointParse
  ( parseOperationCheckpoint,
  )
where

import DBOS.Prelude
import DBOS.Transact.OperationCheckpointTypes
  ( AwaitedWorkflowResult (..),
    OperationCheckpoint (..),
    OperationCheckpointDecodeError (..),
    OperationCheckpointResult (..),
    OperationId,
    OperationName,
  )
import DBOS.SystemDB.Types (SerializedWorkflowValue, Timestamp, WorkflowId)

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
