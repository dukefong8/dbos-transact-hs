{-# LANGUAGE OverloadedRecordDot #-}

module DBOS.Transact.WorkflowExecutionParse
  ( WorkflowExecutionDecodeError (..),
    parseWorkflowExecution,
  )
where

import DBOS.SystemDB.Types
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
  deriving stock (Eq, Show)

parseWorkflowExecution ::
  WorkflowExecutionRow ->
  Either WorkflowExecutionDecodeError WorkflowExecution
parseWorkflowExecution row = do
  status <-
    case parseWorkflowStatus (row.rowWorkflowStatus) of
      Left err -> Left (WorkflowExecutionUnknownStatus err)
      Right parsed -> Right parsed
  outcome <- parseWorkflowOutcome row status
  pure
    WorkflowExecution
      { workflowExecutionId = row.rowWorkflowId,
        workflowExecutionStatus = status,
        workflowExecutionName = WorkflowName <$> row.rowWorkflowName,
        workflowExecutionParentId = row.rowWorkflowParentId,
        workflowExecutionInputs = parseWorkflowInputs row,
        workflowExecutionOutcome = outcome,
        workflowExecutionExecutor = ExecutorId <$> row.rowWorkflowExecutor,
        workflowExecutionCreatedAt = row.rowWorkflowCreatedAt,
        workflowExecutionUpdatedAt = row.rowWorkflowUpdatedAt,
        workflowExecutionRecoveryAttempts = row.rowWorkflowRecoveryAttempts,
        workflowExecutionQueueName = row.rowWorkflowQueueName,
        workflowExecutionSerialization = Serialization <$> row.rowWorkflowSerialization,
        workflowExecutionApplicationVersion = ApplicationVersion
          <$> row.rowWorkflowApplicationVersion
      }

parseWorkflowInputs :: WorkflowExecutionRow -> Maybe SerializedWorkflowValue
parseWorkflowInputs row =
  (\payload -> SerializedWorkflowValue payload (Serialization <$> row.rowWorkflowSerialization))
    <$> row.rowWorkflowInputs

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
  case (row.rowWorkflowOutput, row.rowWorkflowError) of
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
