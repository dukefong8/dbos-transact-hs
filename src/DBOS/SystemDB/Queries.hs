{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module DBOS.SystemDB.Queries
  ( fetchNotificationSession,
    fetchOperationCheckpointSession,
    fetchWorkflowExecutionRowSession,
    fetchWorkflowStatusSession,
    recordOperationOutputSession,
    tryStartWorkflowSession,
    updateWorkflowOutcomeSession,
  )
where

import Control.Applicative ((<|>))
import Data.Int (Int32, Int64)
import Data.Text (Text)
import DBOS.SystemDB.Types
  ( MessageUUID (..),
    NotificationRow (..),
  )
import DBOS.Transact.OperationCheckpointParse
  ( parseOperationCheckpoint,
  )
import DBOS.Transact.OperationCheckpointTypes
  ( OperationCheckpoint (..),
    OperationCheckpointDecodeError,
    OperationId (..),
  )
import DBOS.Transact.OperationCheckpointTypes qualified as OperationCheckpointTypes
import DBOS.Transact.WorkflowExecutionStatus
  ( WorkflowStatus (..),
    parseWorkflowStatus,
  )
import DBOS.Transact.WorkflowExecutionTypes
  ( Millis (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    WorkflowExecutionRow (..),
    WorkflowId (..),
    WorkflowName (..),
  )
import Hasql.PostgresqlTypes ()
import Hasql.Session (Session)
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import Hasql.TH
  ( maybeStatement,
    resultlessStatement,
    singletonStatement,
  )

type WorkflowExecutionRowRaw =
  ( Text,
    Text,
    Maybe Text,
    Maybe Text,
    Maybe Text,
    Maybe Text,
    Maybe Text,
    Maybe Text,
    Maybe Int64,
    Maybe Int64,
    Maybe Int64,
    Maybe Text,
    Maybe Text,
    Maybe Text
  )

type OperationCheckpointRaw =
  ( Int32,
    Text,
    Maybe Text,
    Maybe Text,
    Maybe Text,
    Maybe Int64,
    Maybe Int64,
    Maybe Text
  )

type NotificationRaw =
  ( Text,
    Text,
    Text,
    Text,
    Maybe Text,
    Bool
  )

fetchWorkflowExecutionRowSession ::
  WorkflowId ->
  Session (Maybe WorkflowExecutionRow)
fetchWorkflowExecutionRowSession (WorkflowId workflowId) =
  fmap decodeWorkflowExecutionRow
    <$> Session.statement workflowId fetchWorkflowExecutionRowStatement

fetchWorkflowStatusSession ::
  WorkflowId ->
  Session (Maybe WorkflowStatus)
fetchWorkflowStatusSession (WorkflowId workflowId) = do
  rawStatus <- Session.statement workflowId fetchWorkflowStatusStatement
  pure (rawStatus >>= either (error . show) Just . parseWorkflowStatus)

fetchOperationCheckpointSession ::
  WorkflowId ->
  OperationId ->
  Session (Maybe OperationCheckpoint)
fetchOperationCheckpointSession (WorkflowId workflowId) (OperationId operationId) = do
  rawCheckpoint <-
    Session.statement
      (workflowId, fromIntegral operationId :: Int32)
      fetchOperationCheckpointStatement
  pure (rawCheckpoint >>= either (error . show) Just . decodeOperationCheckpoint)

fetchNotificationSession ::
  MessageUUID ->
  Session (Maybe NotificationRow)
fetchNotificationSession (MessageUUID messageUUID) =
  fmap decodeNotificationRow
    <$> Session.statement messageUUID fetchNotificationStatement

tryStartWorkflowSession ::
  WorkflowId ->
  WorkflowName ->
  Session Bool
tryStartWorkflowSession (WorkflowId workflowId) (WorkflowName workflowName) =
  Session.statement
    (workflowId, workflowName)
    tryStartWorkflowStatement

updateWorkflowOutcomeSession ::
  WorkflowId ->
  WorkflowStatus ->
  Maybe SerializedWorkflowValue ->
  Maybe SerializedWorkflowValue ->
  Session ()
updateWorkflowOutcomeSession (WorkflowId workflowId) status output errorValue =
  let serialization =
        serializedWorkflowSerialization output
          <|> serializedWorkflowSerialization errorValue
   in Session.statement
        ( workflowId,
          workflowStatusText status,
          serializedText <$> output,
          serializedText <$> errorValue,
          serialization
        )
        updateWorkflowOutcomeStatement

recordOperationOutputSession ::
  WorkflowId ->
  OperationId ->
  OperationCheckpointTypes.OperationName ->
  SerializedWorkflowValue ->
  Session ()
recordOperationOutputSession
  (WorkflowId workflowId)
  (OperationId operationId)
  (OperationCheckpointTypes.OperationName operationName)
  output =
    Session.statement
      ( workflowId,
        fromIntegral operationId :: Int32,
        operationName,
        serializedText output,
        serializedWorkflowSerialization (Just output)
      )
      recordOperationOutputStatement

fetchWorkflowExecutionRowStatement ::
  Statement.Statement Text (Maybe WorkflowExecutionRowRaw)
fetchWorkflowExecutionRowStatement =
  [maybeStatement|
    select
      workflow_uuid :: text,
      status :: text,
      name :: text?,
      parent_workflow_id :: text?,
      inputs :: text?,
      output :: text?,
      error :: text?,
      executor_id :: text?,
      created_at :: int8?,
      updated_at :: int8?,
      recovery_attempts :: int8?,
      queue_name :: text?,
      serialization :: text?,
      application_version :: text?
    from dbos.workflow_status
    where workflow_uuid = $1 :: text
  |]

fetchWorkflowStatusStatement ::
  Statement.Statement Text (Maybe Text)
fetchWorkflowStatusStatement =
  [maybeStatement|
    select status :: text
    from dbos.workflow_status
    where workflow_uuid = $1 :: text
  |]

fetchOperationCheckpointStatement ::
  Statement.Statement
    (Text, Int32)
    (Maybe OperationCheckpointRaw)
fetchOperationCheckpointStatement =
  [maybeStatement|
    select
      function_id :: int4,
      function_name :: text,
      output :: text?,
      error :: text?,
      child_workflow_id :: text?,
      started_at_epoch_ms :: int8?,
      completed_at_epoch_ms :: int8?,
      serialization :: text?
    from dbos.operation_outputs
    where workflow_uuid = $1 :: text
      and function_id = $2 :: int4
  |]

fetchNotificationStatement ::
  Statement.Statement Text (Maybe NotificationRaw)
fetchNotificationStatement =
  [maybeStatement|
    select
      destination_uuid :: text,
      topic :: text,
      message :: text,
      message_uuid :: text,
      serialization :: text?,
      consumed :: bool
    from dbos.notifications
    where message_uuid = $1 :: text
  |]

tryStartWorkflowStatement ::
  Statement.Statement (Text, Text) Bool
tryStartWorkflowStatement =
  [singletonStatement|
    with inserted as (
      insert into dbos.workflow_status
        (
          workflow_uuid,
          status,
          name,
          executor_id,
          created_at,
          updated_at,
          application_version,
          recovery_attempts,
          queue_name,
          inputs,
          serialization,
          priority,
          parent_workflow_id
        )
      values
        (
          $1 :: text,
          'PENDING',
          $2 :: text,
          'hs-active',
          (extract(epoch from clock_timestamp()) * 1000)::bigint,
          (extract(epoch from clock_timestamp()) * 1000)::bigint,
          'v1',
          1,
          'default',
          null,
          'json',
          0,
          null
        )
      on conflict (workflow_uuid) do nothing
      returning workflow_uuid
    ),
    claimed as (
      update dbos.workflow_status
      set executor_id = 'hs-active',
          recovery_attempts = coalesce(recovery_attempts, 0) + 1,
          updated_at = (extract(epoch from clock_timestamp()) * 1000)::bigint
      where workflow_uuid = $1 :: text
        and status = 'PENDING'
        and executor_id is null
        and not exists (select 1 from inserted)
      returning workflow_uuid
    )
    select
      (
        exists (select 1 from inserted)
        or exists (select 1 from claimed)
      ) :: bool
  |]

updateWorkflowOutcomeStatement ::
  Statement.Statement
    (Text, Text, Maybe Text, Maybe Text, Maybe Text)
    ()
updateWorkflowOutcomeStatement =
  [resultlessStatement|
    update dbos.workflow_status
    set status = $2 :: text,
        output = $3 :: text?,
        error = $4 :: text?,
        serialization = coalesce($5 :: text?, serialization),
        executor_id = case
          when ($2 :: text) = 'PENDING' then null
          else 'local'
        end,
        deduplication_id = null,
        updated_at = (extract(epoch from clock_timestamp()) * 1000)::bigint,
        completed_at = case
          when ($2 :: text) = 'PENDING' then null
          else (extract(epoch from clock_timestamp()) * 1000)::bigint
        end
    where workflow_uuid = $1 :: text
  |]

recordOperationOutputStatement ::
  Statement.Statement
    (Text, Int32, Text, Text, Maybe Text)
    ()
recordOperationOutputStatement =
  [resultlessStatement|
    insert into dbos.operation_outputs
      (
        workflow_uuid,
        function_id,
        function_name,
        output,
        error,
        child_workflow_id,
        started_at_epoch_ms,
        completed_at_epoch_ms,
        serialization
      )
    values
      (
        $1 :: text,
        $2 :: int4,
        $3 :: text,
        $4 :: text,
        null,
        null,
        (extract(epoch from clock_timestamp()) * 1000)::bigint,
        (extract(epoch from clock_timestamp()) * 1000)::bigint,
        $5 :: text?
      )
    on conflict (workflow_uuid, function_id) do nothing
  |]

decodeWorkflowExecutionRow :: WorkflowExecutionRowRaw -> WorkflowExecutionRow
decodeWorkflowExecutionRow
  ( workflowId,
    status,
    name,
    parentId,
    inputs,
    output,
    errorValue,
    executor,
    createdAt,
    updatedAt,
    recoveryAttempts,
    queueName,
    serialization,
    applicationVersion
    ) =
    WorkflowExecutionRow
      { rowWorkflowId = WorkflowId workflowId,
        rowWorkflowStatus = status,
        rowWorkflowName = name,
        rowWorkflowParentId = WorkflowId <$> parentId,
        rowWorkflowInputs = inputs,
        rowWorkflowOutput = serializedWorkflowValue output serialization,
        rowWorkflowError = serializedWorkflowValue errorValue serialization,
        rowWorkflowExecutor = executor,
        rowWorkflowCreatedAt = Millis <$> createdAt,
        rowWorkflowUpdatedAt = Millis <$> updatedAt,
        rowWorkflowRecoveryAttempts = recoveryAttempts,
        rowWorkflowQueueName = queueName,
        rowWorkflowSerialization = serialization,
        rowWorkflowApplicationVersion = applicationVersion
      }

decodeOperationCheckpoint ::
  OperationCheckpointRaw ->
  Either OperationCheckpointDecodeError OperationCheckpoint
decodeOperationCheckpoint
  ( operationId,
    operationName,
    output,
    errorValue,
    childWorkflowId,
    startedAt,
    completedAt,
    serialization
    ) =
    parseOperationCheckpoint
      decodedOperationId
      decodedOperationName
      (serializedWorkflowValue output serialization)
      (serializedWorkflowValue errorValue serialization)
      (WorkflowId <$> childWorkflowId)
      (Millis <$> startedAt)
      (Millis <$> completedAt)
    where
      decodedOperationId = OperationId (fromIntegral operationId)
      decodedOperationName = OperationCheckpointTypes.OperationName operationName

decodeNotificationRow :: NotificationRaw -> NotificationRow
decodeNotificationRow
  ( destination,
    topic,
    message,
    messageUUID,
    serialization,
    consumed
    ) =
    NotificationRow
      { notificationDestinationId = WorkflowId destination,
        notificationTopic = topic,
        notificationMessage =
          SerializedWorkflowValue
            { serializedText = message,
              serializedSerialization = Serialization <$> serialization
            },
        notificationMessageUUID = MessageUUID messageUUID,
        notificationConsumed = consumed
      }

serializedWorkflowValue ::
  Maybe Text ->
  Maybe Text ->
  Maybe SerializedWorkflowValue
serializedWorkflowValue value serialization =
  SerializedWorkflowValue <$> value <*> pure (Serialization <$> serialization)

serializedWorkflowSerialization :: Maybe SerializedWorkflowValue -> Maybe Text
serializedWorkflowSerialization value =
  case value >>= serializedSerialization of
    Just (Serialization serialization) -> Just serialization
    Nothing -> Nothing

workflowStatusText :: WorkflowStatus -> Text
workflowStatusText status =
  case status of
    Pending -> "PENDING"
    Success -> "SUCCESS"
    Error -> "ERROR"
    MaxRecoveryAttemptsExceeded -> "MAX_RECOVERY_ATTEMPTS_EXCEEDED"
    Cancelled -> "CANCELLED"
    Enqueued -> "ENQUEUED"
    Delayed -> "DELAYED"
