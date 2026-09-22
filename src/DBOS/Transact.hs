module DBOS.Transact
  ( -- * Workflow executions
    ApplicationVersion (..),
    ExecutorId (..),
    Millis (..),
    WorkflowExecution (..),
    WorkflowExecutionDecodeError (..),
    WorkflowExecutionStore,
    WorkflowExecutionRow (..),
    WorkflowId (..),
    WorkflowName (..),
    WorkflowOutcome (..),
    WorkflowStatus (..),
    WorkflowStatusDecodeError (..),
    getWorkflowExecution,
    parseWorkflowExecution,
    parseWorkflowStatus,
    withWorkflowExecutionStore,

    -- * Operation checkpoints
    AwaitedWorkflowResult (..),
    OperationCheckpoint (..),
    OperationCheckpointDecodeError (..),
    OperationCheckpointReplay (..),
    OperationCheckpointReplayError (..),
    OperationCheckpointResult (..),
    OperationExecutionCheckError (..),
    OperationId (..),
    OperationName (..),
    OperationCheckpointStore,
    StepError (..),
    checkOperationExecution,
    parseOperationCheckpoint,
    replayOperationCheckpoint,
    runStep,
    sleepStep,
    withOperationCheckpointStore,

    -- * Durable value codec
    CodecError (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    decodeWorkflowValue,
    encodeUnit,
    encodeWorkflowValue,

    -- * Structured logging
    DbosLogMsg (..),
    DbosSeverity (..),
    nullLogAction,
    withStdoutLogger,

    -- * Executor lifecycle
    Executor (..),
    dequeuePass,
    launchExecutor,
    shutdownExecutor,
    spawnWorkflow,
    superviseForever,
    -- * Workflow registry and runner
    DbosDbError (..),
    DuplicateWorkflowName (..),
    WorkflowBody,
    WorkflowRegistry,
    WorkflowRunError (..),
    emptyRegistry,
    lookupWorkflow,
    registerWorkflow,
    runWorkflow,

    -- * Workflow messages
    IdempotencyKey (..),
    MessageUUID (..),
    NotificationRow (..),
    SendMessage (..),
    Topic (..),
    messageUUIDForSend,
    notificationRowForMessage,
    nullTopicSentinel,
  )
where

import Bluefin.Capability.Ask (Ask, ask, runAsk)
import Bluefin.Eff (Eff, type (:&), type (<:))
import Bluefin.IO (IOE, effIO)
import Colog.Core.Action (LogAction (..))
import Data.Text (pack)
import DBOS.Transact.Codec (CodecError (..), decodeWorkflowValue, encodeUnit, encodeWorkflowValue)
import DBOS.Transact.Executor (Executor (..), dequeuePass, launchExecutor, shutdownExecutor, spawnWorkflow)
import DBOS.Transact.Log (DbosLogMsg (..), DbosSeverity (..), nullLogAction, withStdoutLogger)
import DBOS.Transact.OperationCheckpointParse (parseOperationCheckpoint)
import DBOS.Transact.OperationCheckpointReplay (replayOperationCheckpoint)
import DBOS.Transact.Registry
  ( DuplicateWorkflowName (..),
    WorkflowBody,
    WorkflowRegistry,
    emptyRegistry,
    lookupWorkflow,
    registerWorkflow,
  )
import DBOS.Transact.Step (StepError (..), runStep, sleepStep)
import DBOS.Transact.Supervisor (superviseForever)
import DBOS.Transact.Workflow (WorkflowRunError (..), runWorkflow)
import DBOS.Transact.OperationCheckpointTypes (AwaitedWorkflowResult (..), OperationCheckpoint (..), OperationCheckpointDecodeError (..), OperationCheckpointReplay (..), OperationCheckpointReplayError (..), OperationCheckpointResult (..), OperationId (..), OperationName (..))
import GHC.Stack (HasCallStack)

import DBOS.SystemDB.Types (IdempotencyKey (..), MessageUUID (..), NotificationRow (..), SendMessage (..), Topic (..), messageUUIDForSend, notificationRowForMessage, nullTopicSentinel)
import DBOS.SystemDB.Postgres (DbosDbError (..))
import DBOS.Transact.WorkflowExecutionParse (WorkflowExecutionDecodeError (..), parseWorkflowExecution)
import DBOS.Transact.WorkflowExecutionStatus (WorkflowStatus (..), WorkflowStatusDecodeError (..), parseWorkflowStatus)
import DBOS.Transact.WorkflowExecutionTypes (ApplicationVersion (..), ExecutorId (..), Millis (..), Serialization (..), SerializedWorkflowValue (..), WorkflowExecution (..), WorkflowExecutionRow (..), WorkflowId (..), WorkflowName (..), WorkflowOutcome (..))

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
  deriving stock (Eq, Show)

withWorkflowExecutionStore ::
  (WorkflowId -> IO (Maybe WorkflowExecutionRow)) ->
  (forall db. WorkflowExecutionStore db -> Eff (db :& es) a) ->
  Eff es a
withWorkflowExecutionStore =
  runAsk

getWorkflowExecution ::
  (HasCallStack, db <: es, io <: es) =>
  IOE io ->
  LogAction IO DbosLogMsg ->
  WorkflowExecutionStore db ->
  WorkflowId ->
  Eff es (Either WorkflowExecutionDecodeError (Maybe WorkflowExecution))
getWorkflowExecution io logger store workflowId@(WorkflowId wid) = do
  effIO io (unLogAction logger (DbosLogMsg DbosInfo (pack "getWorkflowExecution: " <> wid) (Just workflowId)))
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
  (HasCallStack, db <: es, io <: es) =>
  IOE io ->
  LogAction IO DbosLogMsg ->
  OperationCheckpointStore db ->
  WorkflowId ->
  OperationId ->
  OperationName ->
  Eff es (Either OperationExecutionCheckError OperationCheckpointReplay)
checkOperationExecution io logger store workflowId@(WorkflowId wid) operationId operationName = do
  effIO io (unLogAction logger (DbosLogMsg DbosInfo (pack "checkOperationExecution: " <> wid) (Just workflowId)))
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
    Left err     -> Left (OperationReplayRejected err)
    Right replay -> Right replay
