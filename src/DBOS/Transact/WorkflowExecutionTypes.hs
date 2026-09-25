{-# LANGUAGE OverloadedStrings #-}

-- | Legacy workflow-execution rows for the starter seam (Rule 4:
-- plain Haskell, no Bluefin imports). The engine v1 read a workflow row
-- into 'WorkflowExecution' and decided run-vs-replay from it; the class
-- backend reads 'DBOS.SystemDB.Types.WorkflowRecord' instead, and every
-- live value type ('SerializedWorkflowValue', 'WorkflowId', and friends)
-- is defined in "DBOS.SystemDB.Types" — this module re-exports nothing.
-- Do not extend: deletion is tracked in @docs/p76-tdd-plan.md@ L2 item 3,
-- blocked only on the L3 starter rewire (@app/Main.hs@ still runs the
-- legacy executor over these rows).
module DBOS.Transact.WorkflowExecutionTypes
  ( WorkflowExecution (..),
    WorkflowExecutionRow (..),
    WorkflowOutcome (..),
  )
where

import DBOS.Prelude
import Data.Int (Int64)
import Data.Text (Text)
import DBOS.SystemDB.Types
  ( ApplicationVersion,
    ExecutorId,
    Serialization,
    SerializedWorkflowValue,
    Timestamp,
    WorkflowId,
    WorkflowName,
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
