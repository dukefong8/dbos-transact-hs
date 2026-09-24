{-# LANGUAGE OverloadedStrings #-}

module DBOS.Transact.WorkflowExecutionTypes
  ( ApplicationVersion (..),
    Duration (..),
    ExecutorId (..),
    SerializedWorkflowValue (..),
    Serialization (..),
    Timestamp (..),
    WorkflowExecution (..),
    WorkflowExecutionRow (..),
    WorkflowId (..),
    WorkflowName (..),
    WorkflowOutcome (..),
  )
where

import Data.Int (Int64)
import Data.Text (Text)
import DBOS.SystemDB.Types
  ( ApplicationVersion (..),
    Duration (..),
    Timestamp (..),
    ExecutorId (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    WorkflowId (..),
    WorkflowName (..),
    WorkflowStatus,
  )

data WorkflowOutcome
  = WorkflowSucceeded SerializedWorkflowValue
  | WorkflowFailed SerializedWorkflowValue
  | WorkflowCancelled
  deriving stock (Eq, Show)

data WorkflowExecution = WorkflowExecution
  { workflowExecutionId :: WorkflowId,
    workflowExecutionStatus :: WorkflowStatus,
    workflowExecutionName :: Maybe WorkflowName,
    workflowExecutionParentId :: Maybe WorkflowId,
    workflowExecutionInputs :: Maybe SerializedWorkflowValue,
    workflowExecutionOutcome :: Maybe WorkflowOutcome,
    workflowExecutionExecutor :: Maybe ExecutorId,
    workflowExecutionCreatedAt :: Maybe Timestamp,
    workflowExecutionUpdatedAt :: Maybe Timestamp,
    workflowExecutionRecoveryAttempts :: Maybe Int64,
    workflowExecutionQueueName :: Maybe Text,
    workflowExecutionSerialization :: Maybe Serialization,
    workflowExecutionApplicationVersion :: Maybe ApplicationVersion
  }
  deriving stock (Eq, Show)

data WorkflowExecutionRow = WorkflowExecutionRow
  { rowWorkflowId :: WorkflowId,
    rowWorkflowStatus :: Text,
    rowWorkflowName :: Maybe Text,
    rowWorkflowParentId :: Maybe WorkflowId,
    rowWorkflowInputs :: Maybe Text,
    rowWorkflowOutput :: Maybe SerializedWorkflowValue,
    rowWorkflowError :: Maybe SerializedWorkflowValue,
    rowWorkflowExecutor :: Maybe Text,
    rowWorkflowCreatedAt :: Maybe Timestamp,
    rowWorkflowUpdatedAt :: Maybe Timestamp,
    rowWorkflowRecoveryAttempts :: Maybe Int64,
    rowWorkflowQueueName :: Maybe Text,
    rowWorkflowSerialization :: Maybe Text,
    rowWorkflowApplicationVersion :: Maybe Text
  }
  deriving stock (Eq, Show)
