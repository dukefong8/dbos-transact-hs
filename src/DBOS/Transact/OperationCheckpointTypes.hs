-- | Legacy operation checkpoints for the starter seam (Rule 4: plain
-- Haskell, no Bluefin imports). The engine v1 read a step row into
-- 'OperationCheckpoint' and decided run-vs-replay from it; the class
-- backend reads 'DBOS.SystemDB.Types.StepRecord' instead, and the new
-- engine shares only the placement decision via "DBOS.Transact.Checkpoint".
-- Do not extend: deletion is tracked in @docs/p76-tdd-plan.md@ L2 item 3,
-- blocked only on the L3 starter rewire (@app/Main.hs@ still runs legacy
-- steps over these rows through 'DBOS.Transact.Store').
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

import DBOS.Prelude
import DBOS.SystemDB.Types (SerializedWorkflowValue (..), Timestamp, WorkflowId)
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
    checkpointStartedAt :: Maybe Timestamp,
    checkpointCompletedAt :: Maybe Timestamp,
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
