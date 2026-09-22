module DBOS.Transact.OperationCheckpointTypes
  ( OperationCheckpoint (..),
    OperationCheckpointDecodeError (..),
    OperationCheckpointReplay (..),
    OperationCheckpointReplayError (..),
    OperationCheckpointResult (..),
    AwaitedWorkflowResult (..),
    OperationId (..),
    OperationName (..),
    SerializedWorkflowValue (..),
  )
where

import DBOS.Transact.WorkflowExecutionTypes (SerializedWorkflowValue (..), WorkflowId)
import DBOS.Transact.WorkflowExecutionTypes qualified as WorkflowExecution
import Data.Text (Text)

newtype OperationId = OperationId Int
  deriving stock (Eq, Show)

newtype OperationName = OperationName Text
  deriving stock (Eq, Show)

data OperationCheckpointResult
  = CheckpointOutput SerializedWorkflowValue
  | CheckpointError SerializedWorkflowValue
  | CheckpointChildWorkflow WorkflowId
  | CheckpointAwaitedWorkflowResult WorkflowId AwaitedWorkflowResult
  deriving stock (Eq, Show)

data AwaitedWorkflowResult
  = AwaitedWorkflowOutput SerializedWorkflowValue
  | AwaitedWorkflowError SerializedWorkflowValue
  deriving stock (Eq, Show)

data OperationCheckpoint = OperationCheckpoint
  { checkpointOperationId :: OperationId,
    checkpointOperationName :: OperationName,
    checkpointStartedAt :: Maybe WorkflowExecution.Millis,
    checkpointCompletedAt :: Maybe WorkflowExecution.Millis,
    checkpointResult :: OperationCheckpointResult
  }
  deriving stock (Eq, Show)

data OperationCheckpointReplay
  = RunOperation
  | ReplayOperation OperationCheckpointResult
  deriving stock (Eq, Show)

data OperationCheckpointReplayError
  = UnexpectedOperationName OperationId OperationName OperationName
  deriving stock (Eq, Show)

data OperationCheckpointDecodeError
  = EmptyOperationCheckpoint OperationId OperationName
  | ConflictingOperationCheckpointValues OperationId OperationName
  deriving stock (Eq, Show)
