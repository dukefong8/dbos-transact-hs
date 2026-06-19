{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module DbosTransact.Workflow
  ( WorkflowName
  , workflowName
  , RegisteredWorkflow(..)
  , WorkflowOptions(..)
  , defaultWorkflowOptions
  , DeduplicationPolicy(..)
  , WorkflowHandle(..)
  , registerWorkflow
  , runWorkflow
  , childWorkflow
  , WorkflowStatus(..)
  , WorkflowStatusType(..)
  ) where

import Bluefin.Eff (Eff)
import Data.Aeson (FromJSON, ToJSON, Value)
import Data.Text (Text)
import Data.Time (NominalDiffTime, UTCTime)
import DbosTransact.Effects (DBOS, WorkflowScope)
import DbosTransact.Error (DBOSError)

newtype WorkflowName input output = WorkflowName Text
  deriving stock (Eq, Ord, Show)

workflowName :: Text -> WorkflowName input output
workflowName = WorkflowName

data RegisteredWorkflow input output = RegisteredWorkflow
  { registeredName :: WorkflowName input output
  , registeredFqn  :: Text
  }

data DeduplicationPolicy
  = DeduplicationReject
  | DeduplicationReturnExisting
  deriving stock (Eq, Show)

data WorkflowOptions = WorkflowOptions
  { workflowIdOption           :: Maybe Text
  , workflowQueueName          :: Maybe Text
  , workflowApplicationVersion :: Maybe Text
  , workflowMaxRetries         :: Maybe Int
  , workflowDeduplicationId    :: Maybe Text
  , workflowDeduplicationPolicy :: DeduplicationPolicy
  , workflowPriority           :: Int
  , workflowAuthenticatedUser  :: Maybe Text
  , workflowAssumedRole        :: Maybe Text
  , workflowAuthenticatedRoles :: [Text]
  , workflowQueuePartitionKey  :: Maybe Text
  , workflowDelay              :: Maybe NominalDiffTime
  , workflowPortable           :: Bool
  }
  deriving stock (Eq, Show)

defaultWorkflowOptions :: WorkflowOptions
defaultWorkflowOptions = WorkflowOptions
  { workflowIdOption = Nothing
  , workflowQueueName = Nothing
  , workflowApplicationVersion = Nothing
  , workflowMaxRetries = Nothing
  , workflowDeduplicationId = Nothing
  , workflowDeduplicationPolicy = DeduplicationReject
  , workflowPriority = 0
  , workflowAuthenticatedUser = Nothing
  , workflowAssumedRole = Nothing
  , workflowAuthenticatedRoles = []
  , workflowQueuePartitionKey = Nothing
  , workflowDelay = Nothing
  , workflowPortable = False
  }

data WorkflowStatusType
  = WorkflowPending
  | WorkflowEnqueued
  | WorkflowDelayed
  | WorkflowSuccess
  | WorkflowError
  | WorkflowCancelled
  | WorkflowMaxRecoveryAttemptsExceeded
  deriving stock (Eq, Ord, Show, Enum, Bounded)

data WorkflowStatus = WorkflowStatus
  { statusWorkflowId          :: Text
  , statusType                :: WorkflowStatusType
  , statusName                :: Text
  , statusInput               :: Maybe Value
  , statusOutput              :: Maybe Value
  , statusError               :: Maybe Text
  , statusExecutorId          :: Maybe Text
  , statusApplicationVersion  :: Maybe Text
  , statusApplicationId       :: Maybe Text
  , statusCreatedAt           :: UTCTime
  , statusUpdatedAt           :: UTCTime
  , statusCompletedAt         :: Maybe UTCTime
  , statusRecoveryAttempts    :: Int
  , statusQueueName           :: Maybe Text
  , statusWorkflowTimeout     :: Maybe NominalDiffTime
  , statusWorkflowDeadline    :: Maybe UTCTime
  , statusDeduplicationId     :: Maybe Text
  , statusPriority            :: Int
  , statusQueuePartitionKey   :: Maybe Text
  , statusParentWorkflowId    :: Maybe Text
  , statusClassName           :: Maybe Text
  , statusConfigName          :: Maybe Text
  , statusSerialization       :: Text
  , statusDelayUntil          :: Maybe UTCTime
  }
  deriving stock (Eq, Show)

data WorkflowHandle output = WorkflowHandle
  { workflowId :: Text
  , getResult  :: IO (Either DBOSError output)
  , getStatus  :: IO (Either DBOSError WorkflowStatus)
  }

registerWorkflow
  :: (ToJSON input, FromJSON input, ToJSON output, FromJSON output)
  => DBOS r
  -> WorkflowName input output
  -> (forall wf. WorkflowScope wf -> input -> Eff es output)
  -> Eff es (RegisteredWorkflow input output)
registerWorkflow _ _ _ = error "DbosTransact.Workflow.registerWorkflow: not implemented"

runWorkflow
  :: (ToJSON input, FromJSON output)
  => DBOS r
  -> RegisteredWorkflow input output
  -> input
  -> WorkflowOptions
  -> Eff es (WorkflowHandle output)
runWorkflow _ _ _ _ = error "DbosTransact.Workflow.runWorkflow: not implemented"

childWorkflow
  :: (ToJSON input, FromJSON output)
  => WorkflowScope wf
  -> RegisteredWorkflow input output
  -> input
  -> WorkflowOptions
  -> Eff es (WorkflowHandle output)
childWorkflow _ _ _ _ = error "DbosTransact.Workflow.childWorkflow: not implemented"
