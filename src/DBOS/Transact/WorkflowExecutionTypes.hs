{-# LANGUAGE OverloadedStrings #-}

module DBOS.Transact.WorkflowExecutionTypes
  ( ApplicationVersion (..),
    ExecutorId (..),
    Millis (..),
    SerializedWorkflowValue (..),
    Serialization (..),
    WorkflowExecution (..),
    WorkflowExecutionRow (..),
    WorkflowId (..),
    WorkflowName (..),
    WorkflowOutcome (..),
  )
where

import Data.Int (Int64)
import Data.Text (Text)
import DBOS.Transact.WorkflowExecutionStatus (WorkflowStatus)

newtype WorkflowId = WorkflowId Text
  deriving (Eq, Show)

newtype WorkflowName = WorkflowName Text
  deriving (Eq, Show)

newtype ExecutorId = ExecutorId Text
  deriving (Eq, Show)

newtype ApplicationVersion = ApplicationVersion Text
  deriving (Eq, Show)

newtype Millis = Millis Int64
  deriving (Eq, Show)

newtype Serialization = Serialization Text
  deriving (Eq, Show)

data SerializedWorkflowValue = SerializedWorkflowValue
  { serializedText :: Text,
    serializedSerialization :: Maybe Serialization
  }
  deriving (Eq, Show)

data WorkflowOutcome
  = WorkflowSucceeded SerializedWorkflowValue
  | WorkflowFailed SerializedWorkflowValue
  | WorkflowCancelled
  deriving (Eq, Show)

data WorkflowExecution = WorkflowExecution
  { workflowExecutionId :: WorkflowId,
    workflowExecutionStatus :: WorkflowStatus,
    workflowExecutionName :: Maybe WorkflowName,
    workflowExecutionParentId :: Maybe WorkflowId,
    workflowExecutionInputs :: Maybe SerializedWorkflowValue,
    workflowExecutionOutcome :: Maybe WorkflowOutcome,
    workflowExecutionExecutor :: Maybe ExecutorId,
    workflowExecutionCreatedAt :: Maybe Millis,
    workflowExecutionUpdatedAt :: Maybe Millis,
    workflowExecutionRecoveryAttempts :: Maybe Int64,
    workflowExecutionQueueName :: Maybe Text,
    workflowExecutionSerialization :: Maybe Serialization,
    workflowExecutionApplicationVersion :: Maybe ApplicationVersion
  }
  deriving (Eq, Show)

data WorkflowExecutionRow = WorkflowExecutionRow
  { rowWorkflowId :: WorkflowId,
    rowWorkflowStatus :: Text,
    rowWorkflowName :: Maybe Text,
    rowWorkflowParentId :: Maybe WorkflowId,
    rowWorkflowInputs :: Maybe Text,
    rowWorkflowOutput :: Maybe SerializedWorkflowValue,
    rowWorkflowError :: Maybe SerializedWorkflowValue,
    rowWorkflowExecutor :: Maybe Text,
    rowWorkflowCreatedAt :: Maybe Millis,
    rowWorkflowUpdatedAt :: Maybe Millis,
    rowWorkflowRecoveryAttempts :: Maybe Int64,
    rowWorkflowQueueName :: Maybe Text,
    rowWorkflowSerialization :: Maybe Text,
    rowWorkflowApplicationVersion :: Maybe Text
  }
  deriving (Eq, Show)
