module DBOS.Transact.WorkflowExecutionParse
  ( WorkflowExecutionDecodeError (..),
    parseWorkflowExecution,
  )
where

import DBOS.Transact.WorkflowExecutionStatus
  ( WorkflowStatus (..),
    WorkflowStatusDecodeError,
    parseWorkflowStatus,
  )
import DBOS.Transact.WorkflowExecutionTypes
  ( ApplicationVersion (..),
    ExecutorId (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    WorkflowExecution (..),
    WorkflowExecutionRow (..),
    WorkflowName (..),
    WorkflowOutcome (..),
  )

data WorkflowExecutionDecodeError
  = WorkflowExecutionUnknownStatus WorkflowStatusDecodeError
  | WorkflowExecutionConflict WorkflowExecutionRow
  | WorkflowExecutionMissingOutcome WorkflowExecutionRow
  deriving (Eq, Show)

parseWorkflowExecution ::
  WorkflowExecutionRow ->
  Either WorkflowExecutionDecodeError WorkflowExecution
parseWorkflowExecution row = do
  status <-
    case parseWorkflowStatus (rowWorkflowStatus row) of
      Left err -> Left (WorkflowExecutionUnknownStatus err)
      Right parsed -> Right parsed
  outcome <- parseWorkflowOutcome row status
  pure
    WorkflowExecution
      { workflowExecutionId = rowWorkflowId row,
        workflowExecutionStatus = status,
        workflowExecutionName = WorkflowName <$> rowWorkflowName row,
        workflowExecutionParentId = rowWorkflowParentId row,
        workflowExecutionInputs = parseWorkflowInputs row,
        workflowExecutionOutcome = outcome,
        workflowExecutionExecutor = ExecutorId <$> rowWorkflowExecutor row,
        workflowExecutionCreatedAt = rowWorkflowCreatedAt row,
        workflowExecutionUpdatedAt = rowWorkflowUpdatedAt row,
        workflowExecutionRecoveryAttempts = rowWorkflowRecoveryAttempts row,
        workflowExecutionQueueName = rowWorkflowQueueName row,
        workflowExecutionSerialization = Serialization <$> rowWorkflowSerialization row,
        workflowExecutionApplicationVersion = ApplicationVersion
          <$> rowWorkflowApplicationVersion row
      }

parseWorkflowInputs :: WorkflowExecutionRow -> Maybe SerializedWorkflowValue
parseWorkflowInputs row =
  (\payload -> SerializedWorkflowValue payload (Serialization <$> rowWorkflowSerialization row))
    <$> rowWorkflowInputs row

parseWorkflowOutcome ::
  WorkflowExecutionRow ->
  WorkflowStatus ->
  Either WorkflowExecutionDecodeError (Maybe WorkflowOutcome)
parseWorkflowOutcome row status =
  case status of
    Cancelled -> Right (Just WorkflowCancelled)
    _ -> parseNonCancelledWorkflowOutcome row status

parseNonCancelledWorkflowOutcome ::
  WorkflowExecutionRow ->
  WorkflowStatus ->
  Either WorkflowExecutionDecodeError (Maybe WorkflowOutcome)
parseNonCancelledWorkflowOutcome row status =
  case (rowWorkflowOutput row, rowWorkflowError row) of
    (Just _, Just _) ->
      Left (WorkflowExecutionConflict row)
    (Just output, Nothing) ->
      Right
        ( Just
            ( WorkflowSucceeded output
            )
        )
    (Nothing, Just errorValue) ->
      Right
        ( Just
            ( WorkflowFailed errorValue
            )
        )
    (Nothing, Nothing)
      | status `elem` [Pending, Enqueued, Delayed] ->
          Right Nothing
      | otherwise ->
          Left (WorkflowExecutionMissingOutcome row)
