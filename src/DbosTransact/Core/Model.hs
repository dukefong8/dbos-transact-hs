{-# LANGUAGE DerivingStrategies #-}

module DbosTransact.Core.Model where

import Data.Map.Strict (Map)
import Data.Text (Text)
import DbosTransact.Workflow

newtype WorkflowId = WorkflowId Text
  deriving stock (Eq, Ord, Show)

newtype StepId = StepId Int
  deriving stock (Eq, Ord, Show)

data StepRecord = StepRecord
  { srName          :: Text
  , srOutput        :: Maybe Text
  , srError         :: Maybe Text
  , srSerialization :: Text
  }
  deriving stock (Eq, Show)

data CoreState = CoreState
  { csWorkflows :: Map WorkflowId WorkflowStatus
  , csSteps     :: Map (WorkflowId, StepId) StepRecord
  }
  deriving stock (Eq, Show)

emptyCoreState :: CoreState
emptyCoreState = CoreState mempty mempty

data Command
  = StartWorkflow WorkflowStatus
  | CompleteWorkflow WorkflowId Text
  | FailWorkflow WorkflowId Text
  | CancelWorkflow WorkflowId
  | BeginStep WorkflowId StepId Text
  | RecordStep WorkflowId StepId StepRecord
  | CheckStep WorkflowId StepId Text
  deriving stock (Eq, Show)

data Event
  = WorkflowStarted WorkflowId
  | WorkflowCompleted WorkflowId
  | WorkflowFailed WorkflowId
  | WorkflowCancelledEvent WorkflowId
  | StepRecorded WorkflowId StepId
  | StepReplayed WorkflowId StepId
  deriving stock (Eq, Show)
