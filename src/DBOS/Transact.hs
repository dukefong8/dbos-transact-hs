module DBOS.Transact
  ( ApplicationVersion (..),
    AwaitedWorkflowResult (..),
    ExecutorId (..),
    Millis (..),
    OperationCheckpoint (..),
    OperationCheckpointDecodeError (..),
    OperationCheckpointReplay (..),
    OperationCheckpointReplayError (..),
    OperationCheckpointResult (..),
    OperationExecutionCheckError (..),
    OperationId (..),
    OperationName (..),
    OperationCheckpointStore,
    IdempotencyKey (..),
    MessageUUID (..),
    NotificationRow (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    SendMessage (..),
    Topic (..),
    WorkflowExecution (..),
    WorkflowExecutionDecodeError (..),
    WorkflowExecutionStore,
    WorkflowExecutionRow (..),
    WorkflowId (..),
    WorkflowName (..),
    WorkflowOutcome (..),
    WorkflowStatus (..),
    WorkflowStatusDecodeError (..),
    checkOperationExecution,
    getWorkflowExecution,
    notificationRowForMessage,
    nullTopicSentinel,
    parseOperationCheckpoint,
    parseWorkflowExecution,
    parseWorkflowStatus,
    replayOperationCheckpoint,
    withOperationCheckpointStore,
    withWorkflowExecutionStore,
  )
where

import Bluefin.Capability.Ask
  ( Ask,
    ask,
    runAsk,
  )
import Bluefin.Eff
  ( Eff,
    type (:&),
    type (<:),
  )
import Bluefin.IO
  ( IOE,
    effIO,
  )
import DBOS.Transact.OperationCheckpointParse
  ( parseOperationCheckpoint,
  )
import DBOS.Transact.OperationCheckpointReplay
  ( replayOperationCheckpoint,
  )
import DBOS.Transact.OperationCheckpointTypes
  ( AwaitedWorkflowResult (..),
    OperationCheckpoint (..),
    OperationCheckpointDecodeError (..),
    OperationCheckpointReplay (..),
    OperationCheckpointReplayError (..),
    OperationCheckpointResult (..),
    OperationId (..),
    OperationName (..),
  )
import DBOS.SystemDB.Types
  ( IdempotencyKey (..),
    MessageUUID (..),
    NotificationRow (..),
    SendMessage (..),
    Topic (..),
    notificationRowForMessage,
    nullTopicSentinel,
  )
import DBOS.Transact.WorkflowExecutionParse
  ( WorkflowExecutionDecodeError (..),
    parseWorkflowExecution,
  )
import DBOS.Transact.WorkflowExecutionStatus
  ( WorkflowStatus (..),
    WorkflowStatusDecodeError (..),
    parseWorkflowStatus,
  )
import DBOS.Transact.WorkflowExecutionTypes
  ( ApplicationVersion (..),
    ExecutorId (..),
    Millis (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    WorkflowExecution (..),
    WorkflowExecutionRow (..),
    WorkflowId (..),
    WorkflowName (..),
    WorkflowOutcome (..),
  )

type WorkflowExecutionStore e = Ask (WorkflowId -> IO (Maybe WorkflowExecutionRow)) e

type OperationCheckpointStore e =
  Ask
    ( WorkflowId -> IO (Maybe WorkflowStatus),
      WorkflowId -> OperationId -> IO (Maybe OperationCheckpoint)
    )
    e

data OperationExecutionCheckError
  = WorkflowExecutionNotFound WorkflowId
  | WorkflowExecutionCancelled WorkflowId
  | OperationReplayRejected OperationCheckpointReplayError
  deriving (Eq, Show)

withWorkflowExecutionStore ::
  (WorkflowId -> IO (Maybe WorkflowExecutionRow)) ->
  (forall db. WorkflowExecutionStore db -> Eff (db :& es) a) ->
  Eff es a
withWorkflowExecutionStore =
  runAsk

getWorkflowExecution ::
  (db <: es, io <: es) =>
  IOE io ->
  WorkflowExecutionStore db ->
  WorkflowId ->
  Eff es (Either WorkflowExecutionDecodeError (Maybe WorkflowExecution))
getWorkflowExecution io store workflowId = do
  fetchWorkflowExecutionRow <- ask store
  row <- effIO io (fetchWorkflowExecutionRow workflowId)
  pure $ traverse parseWorkflowExecution row

withOperationCheckpointStore ::
  (WorkflowId -> IO (Maybe WorkflowStatus)) ->
  (WorkflowId -> OperationId -> IO (Maybe OperationCheckpoint)) ->
  (forall db. OperationCheckpointStore db -> Eff (db :& es) a) ->
  Eff es a
withOperationCheckpointStore getStatus getCheckpoint =
  runAsk (getStatus, getCheckpoint)

checkOperationExecution ::
  (db <: es, io <: es) =>
  IOE io ->
  OperationCheckpointStore db ->
  WorkflowId ->
  OperationId ->
  OperationName ->
  Eff es (Either OperationExecutionCheckError OperationCheckpointReplay)
checkOperationExecution io store workflowId operationId operationName = do
  (getWorkflowStatus, getOperationCheckpoint) <- ask store
  workflowStatus <- effIO io (getWorkflowStatus workflowId)
  case workflowStatus of
    Nothing ->
      pure (Left (WorkflowExecutionNotFound workflowId))
    Just Cancelled ->
      pure (Left (WorkflowExecutionCancelled workflowId))
    Just _ -> do
      checkpoint <- effIO io (getOperationCheckpoint workflowId operationId)
      pure (mapReplayError (replayOperationCheckpoint operationName checkpoint))

mapReplayError ::
  Either OperationCheckpointReplayError OperationCheckpointReplay ->
  Either OperationExecutionCheckError OperationCheckpointReplay
mapReplayError result =
  case result of
    Left err -> Left (OperationReplayRejected err)
    Right replay -> Right replay
