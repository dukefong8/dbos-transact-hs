{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TypeFamilies        #-}
{-# OPTIONS_GHC -Wno-orphans #-}

-- | Postgres system database: @[typedSql| ... |]@ sessions over an explicit
-- pool, in one module. The session half pins each statement's SQL
-- (explicit column lists; star selects are rejected at compile time) and
-- decodes rows into domain types at the boundary; the pool half runs those
-- sessions with polling loops for blocking reads. Plain Haskell, no Bluefin
-- imports — the Bluefin seam lives outward of this module.
module DBOS.SystemDB.Postgres
  (     Pool.Pool,
    WorkflowStartDecision (..),
    DbosMigration (..),
    postgresEventStore,
    postgresStepStore,
    acquirePool,
    dequeueWorkflows,
    dequeueWorkflowsSession,
    enqueueWorkflow,
    enqueueWorkflowSession,
    fetchMigrationVersion,
    fetchNotification,
    fetchNotificationSession,
    fetchOperationCheckpoint,
    fetchOperationCheckpointSession,
    fetchQueueWorkerConcurrency,
    fetchQueueWorkerConcurrencySession,
    fetchRecvStepSession,
    fetchWorkflowExecutionRow,
    fetchWorkflowExecutionRowSession,
    fetchWorkflowStatus,
    fetchWorkflowStatusSession,
    fetchWorkflowStatuses,
    fetchWorkflowStatusesSession,
    getEvent,
    getEventBlocking,
    getEventSession,
    listWorkflowIdsByName,
    listWorkflowIdsByNameSession,
    migrationVersionSession,
    probeNotificationSession,
    recordOperationError,
    recordOperationErrorSession,
    recordOperationOutput,
    recordOperationOutputSession,
    recordRecvSession,
    recordSleep,
    recordSleepSession,
    recvMessage,
    reenqueueForRecovery,
    reenqueueForRecoverySession,
    registerQueue,
    registerQueueSession,
    releasePool,
    releaseWorkflowClaim,
    releaseWorkflowClaimSession,
    runDb,
    runDbOrFail,
    sendMessage,
    sendMessages,
    sendMessagesSession,
    setEvent,
    setEventSession,
    takeNotificationSession,
    tryStartWorkflow,
    tryStartWorkflowSession,
    updateQueueWorkerConcurrency,
    updateQueueWorkerConcurrencySession,
    updateWorkflowOutcome,
    updateWorkflowOutcomeSession,
  )
where

import Control.Applicative ((<|>))
import Control.Concurrent (threadDelay)
import Control.Exception (throwIO)
import Control.Monad (join)
import Data.Functor (void)
import Data.Int (Int64)
import Data.Maybe (fromMaybe, listToMaybe, mapMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Data.Word (Word16)
import DBOS.SystemDB.Types (MessageUUID (..), NotificationRow (..), OnExistingQueue (..), QueueName (..), SendMessage (..), Topic (..), WorkflowStatus (..), messageUUIDForSend, nullTopicSentinel, parseWorkflowStatus, recvStepName, workflowStatusText)
import DBOS.Transact.OperationCheckpointParse (parseOperationCheckpoint)
import DBOS.Transact.OperationCheckpointTypes (OperationCheckpoint (..), OperationCheckpointDecodeError, OperationId (..), OperationName (..))
import DBOS.SystemDB.Error (BackendError (..), BackendErrorKind (..), Error (..))
import DBOS.Transact.OperationCheckpointTypes qualified as OperationCheckpointTypes
import DBOS.Transact.Store (EventStore (..), StepStore (..))
import DBOS.SystemDB.Types (Duration (..), Timestamp (..), addTimeout, timestampNow, timestampToEpochMs)
import DBOS.Transact.WorkflowExecutionTypes (ApplicationVersion (..), ExecutorId (..), Serialization (..), SerializedWorkflowValue (..), WorkflowExecutionRow (..), WorkflowId (..), WorkflowName (..))
import Hasql.Connection.Settings qualified as Connection
import Hasql.Decoders qualified as Decoders
import Hasql.Errors qualified as Errors
import Hasql.Pool qualified as Pool
import Hasql.Pool.Config qualified as PoolConfig
import Hasql.PostgresqlTypes ()
import Hasql.Session (Session)
import IHP.TypedSql.Hasql (sqlExecTypedSession, sqlQueryTypedSession, typedSql)
import IHP.TypedSql.Id (Id' (..), PrimaryKey)
import IHP.TypedSql.Row (TypedSqlRow (..))
import IHP.TypedSql.RowType (SqlRow)
import System.Environment (lookupEnv)
import Text.Read (readMaybe)
-- | Primary keys are plain text ids in the DBOS system schema.
type instance PrimaryKey "workflow_status" = Text

type instance PrimaryKey "operation_outputs" = Int

type instance PrimaryKey "notifications" = Text

type WorkflowExecutionRowRaw =
  SqlRow
    '[ '("workflow_uuid", Id' "workflow_status"),
       '("status", Maybe Text),
       '("name", Maybe Text),
       '("parent_workflow_id", Maybe Text),
       '("inputs", Maybe Text),
       '("output", Maybe Text),
       '("error", Maybe Text),
       '("executor_id", Maybe Text),
       '("created_at", Int64),
       '("updated_at", Int64),
       '("recovery_attempts", Maybe Int64),
       '("queue_name", Maybe Text),
       '("serialization", Maybe Text),
       '("application_version", Maybe Text)
     ]

type OperationCheckpointRaw =
  SqlRow
    '[ '("function_id", Id' "operation_outputs"),
       '("function_name", Text),
       '("output", Maybe Text),
       '("error", Maybe Text),
       '("child_workflow_id", Maybe Text),
       '("started_at_epoch_ms", Maybe Int64),
       '("completed_at_epoch_ms", Maybe Int64),
       '("serialization", Maybe Text)
     ]

type NotificationRaw =
  SqlRow
    '[ '("destination_uuid", Id' "workflow_status"),
       '("topic", Maybe Text),
       '("message", Text),
       '("message_uuid", Id' "notifications"),
       '("serialization", Maybe Text),
       '("consumed", Bool)
     ]

fetchWorkflowExecutionRowSession ::
  WorkflowId ->
  Session (Maybe WorkflowExecutionRow)
fetchWorkflowExecutionRowSession (WorkflowId workflowId) =
  fmap decodeWorkflowExecutionRow
    <$> sqlQueryTypedSession [typedSql|
    select
      workflow_uuid,
      status,
      name,
      parent_workflow_id,
      inputs,
      output,
      error,
      executor_id,
      created_at,
      updated_at,
      recovery_attempts,
      queue_name,
      serialization,
      application_version
    from dbos.workflow_status
    where workflow_uuid = ${workflowId}
    limit 1
  |]

fetchWorkflowStatusSession ::
  WorkflowId ->
  Session (Maybe WorkflowStatus)
fetchWorkflowStatusSession (WorkflowId workflowId) = do
  rawStatus <-
    sqlQueryTypedSession [typedSql|
      select status
      from dbos.workflow_status
      where workflow_uuid = ${workflowId}
      limit 1
    |]
  -- The column is nullable in the schema, so the row cardinality (@Maybe@)
  -- nests with the column nullability. A null status fails downstream as
  -- @UnknownWorkflowStatus ""@ rather than crashing the row decode.
  pure (join rawStatus >>= either (error . show) Just . parseWorkflowStatus)

fetchOperationCheckpointSession ::
  WorkflowId ->
  OperationId ->
  Session (Maybe OperationCheckpoint)
fetchOperationCheckpointSession (WorkflowId workflowId) (OperationId operationId) = do
  let functionId = fromIntegral operationId :: Int
  rawCheckpoint <-
    sqlQueryTypedSession [typedSql|
      select
        function_id,
        function_name,
        output,
        error,
        child_workflow_id,
        started_at_epoch_ms,
        completed_at_epoch_ms,
        serialization
      from dbos.operation_outputs
      where workflow_uuid = ${workflowId}
        and function_id = ${functionId}
      limit 1
    |]
  pure (rawCheckpoint >>= either (error . show) Just . decodeOperationCheckpoint)

fetchNotificationSession ::
  MessageUUID ->
  Session (Maybe NotificationRow)
fetchNotificationSession (MessageUUID messageUUID) =
  fmap decodeNotificationRow
    <$> sqlQueryTypedSession [typedSql|
    select
      destination_uuid,
      topic,
      message,
      message_uuid,
      serialization,
      consumed
    from dbos.notifications
    where message_uuid = ${messageUUID}
    limit 1
  |]

-- | Highest applied migration version. The ceiling this port tracks is 108
-- (ranges 1–47 + 100–108); a higher value means the Rust corpus moved and the
-- pinned queries must be re-verified. The single-column table selects as a
-- full-table model, decoded by the instance below.
newtype DbosMigration = DbosMigration Int64
  deriving stock (Eq, Show)

instance TypedSqlRow DbosMigration where
  typedSqlRowDecoder = DbosMigration <$> Decoders.column (Decoders.nonNullable Decoders.int8)

migrationVersionSession :: Session (Maybe DbosMigration)
migrationVersionSession =  sqlQueryTypedSession [typedSql|
    select version
    from dbos.dbos_migrations
    order by version desc
    limit 1
  |]

tryStartWorkflowSession ::
  WorkflowId ->
  WorkflowName ->
  Maybe SerializedWorkflowValue ->
  ExecutorId ->
  ApplicationVersion ->
  Session Bool
tryStartWorkflowSession (WorkflowId workflowId) (WorkflowName workflowName) inputs (ExecutorId executorId) (ApplicationVersion applicationVersion) =
  -- The singleton select infers @AtMostOneRow@; the query shape provably
  -- returns one row, and a missing row safely reads as "not started", which
  -- the caller resolves through the await path.
  let inputText = (.serializedText) <$> inputs
      serializationText = case serializedWorkflowSerialization inputs of
        Just tag -> tag
        Nothing  -> "json"
   in fromMaybe False
        <$> sqlQueryTypedSession [typedSql|
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
          ${workflowId},
          'PENDING',
          ${workflowName},
          ${executorId},
          (extract(epoch from clock_timestamp()) * 1000)::bigint,
          (extract(epoch from clock_timestamp()) * 1000)::bigint,
          ${applicationVersion},
          1,
          null,
          ${inputText},
          ${serializationText},
          0,
          null
        )
      on conflict (workflow_uuid) do nothing
      returning workflow_uuid
    ),
    claimed as (
      update dbos.workflow_status
      set executor_id = ${executorId},
          recovery_attempts = coalesce(recovery_attempts, 0) + 1,
          updated_at = (extract(epoch from clock_timestamp()) * 1000)::bigint
      where workflow_uuid = ${workflowId}
        and status = 'PENDING'
        and executor_id is null
        and not exists (select 1 from inserted)
      returning workflow_uuid
    )
    select
      (
        exists (select 1 from inserted)
        or exists (select 1 from claimed)
      )
  |]

updateWorkflowOutcomeSession ::
  WorkflowId ->
  ExecutorId ->
  WorkflowStatus ->
  Maybe SerializedWorkflowValue ->
  Maybe SerializedWorkflowValue ->
  Session ()
updateWorkflowOutcomeSession (WorkflowId workflowId) (ExecutorId executorId) status output errorValue =
  let serialization =
        serializedWorkflowSerialization output
          <|> serializedWorkflowSerialization errorValue
      statusText = workflowStatusText status
      outputText = (.serializedText) <$> output
      errorText = (.serializedText) <$> errorValue
   in void $ sqlExecTypedSession [typedSql|
          update dbos.workflow_status
          set status = ${statusText}::text,
              output = ${outputText}::text,
              error = ${errorText}::text,
              serialization = coalesce(${serialization}::text, serialization),
              executor_id = case
                when ${statusText}::text = 'PENDING' then null
                else ${executorId}
              end,
              deduplication_id = null,
              updated_at = (extract(epoch from clock_timestamp()) * 1000)::bigint,
              completed_at = case
                when ${statusText}::text = 'PENDING' then null
                else (extract(epoch from clock_timestamp()) * 1000)::bigint
              end
          where workflow_uuid = ${workflowId}
        |]

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
    let functionId = fromIntegral operationId :: Int
        outputText = output.serializedText
        serialization = serializedWorkflowSerialization (Just output)
     in void $ sqlExecTypedSession [typedSql|
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
                ${workflowId},
                ${functionId},
                ${operationName},
                ${outputText},
                null,
                null,
                (extract(epoch from clock_timestamp()) * 1000)::bigint,
                (extract(epoch from clock_timestamp()) * 1000)::bigint,
                ${serialization}::text
              )
            on conflict (workflow_uuid, function_id) do nothing
          |]

recordOperationErrorSession ::
  WorkflowId ->
  OperationId ->
  OperationCheckpointTypes.OperationName ->
  SerializedWorkflowValue ->
  Session ()
recordOperationErrorSession
  (WorkflowId workflowId)
  (OperationId operationId)
  (OperationCheckpointTypes.OperationName operationName)
  errorValue =
    let functionId = fromIntegral operationId :: Int
        errorText = errorValue.serializedText
        serialization = serializedWorkflowSerialization (Just errorValue)
     in void $ sqlExecTypedSession [typedSql|
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
                ${workflowId},
                ${functionId},
                ${operationName},
                null,
                ${errorText},
                null,
                (extract(epoch from clock_timestamp()) * 1000)::bigint,
                (extract(epoch from clock_timestamp()) * 1000)::bigint,
                ${serialization}::text
              )
            on conflict (workflow_uuid, function_id) do nothing
          |]

-- | Record a durable sleep: the wake time is the output and 'completed_at'
-- is stamped at the wake time (in the future), so a replay waits only the
-- remainder instead of the whole duration.
recordSleepSession ::
  WorkflowId ->
  OperationId ->
  OperationCheckpointTypes.OperationName ->
  SerializedWorkflowValue ->
  Timestamp ->
  Timestamp ->
  Session ()
recordSleepSession
  (WorkflowId workflowId)
  (OperationId operationId)
  (OperationCheckpointTypes.OperationName operationName)
  output
  (Timestamp startedMs)
  (Timestamp completedMs) =
    let functionId = fromIntegral operationId :: Int
        outputText = output.serializedText
        serialization = serializedWorkflowSerialization (Just output)
     in void $ sqlExecTypedSession [typedSql|
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
                ${workflowId},
                ${functionId},
                ${operationName},
                ${outputText},
                null,
                null,
                ${startedMs},
                ${completedMs},
                ${serialization}::text
              )
            on conflict (workflow_uuid, function_id) do nothing
          |]

-- | Publish a workflow event, replacing any value already under the key.
setEventSession ::
  WorkflowId ->
  Text ->
  SerializedWorkflowValue ->
  Session ()
setEventSession (WorkflowId workflowId) key value =
  let valueText = value.serializedText
      serialization = serializedWorkflowSerialization (Just value)
   in void $ sqlExecTypedSession [typedSql|
          insert into dbos.workflow_events
            (
              workflow_uuid,
              key,
              value,
              serialization
            )
          values
            (
              ${workflowId},
              ${key},
              ${valueText},
              ${serialization}::text
            )
          on conflict (workflow_uuid, key) do update
          set value = excluded.value,
              serialization = excluded.serialization
        |]

-- | Read a published event by name, or nothing when no value is there yet.
-- A missing key is a value ('Nothing'), never an error.
getEventSession ::
  WorkflowId ->
  Text ->
  Session (Maybe SerializedWorkflowValue)
getEventSession (WorkflowId workflowId) key =
  fmap decodeEventValue
    <$> sqlQueryTypedSession [typedSql|
    select value, serialization
    from dbos.workflow_events
    where workflow_uuid = ${workflowId}
      and key = ${key}
    limit 1
  |]
  where
    decodeEventValue row =
      SerializedWorkflowValue
        { serializedText = row.value,
          serializedSerialization = Serialization <$> row.serialization
        }

-- | Insert one row per message, keyed so a resend under the same idempotency
-- key is a no-op. Mirrors @deliver@: the message id is @key::destination@ for
-- a keyed send and @fallback::destination@ otherwise, always scoped per
-- recipient. The batch is one statement, so it is all-or-nothing.
sendMessagesSession ::
  [MessageUUID] ->
  [SendMessage] ->
  Session ()
sendMessagesSession fallbackIds messages =
  void $ sqlExecTypedSession [typedSql|
    insert into dbos.notifications
      (
        destination_uuid,
        topic,
        message,
        message_uuid,
        serialization
      )
    select
      m.destination_uuid,
      m.topic,
      m.message,
      m.message_uuid,
      s.serialization
    from unnest(
      ${destinationIds}::text[],
      ${topics}::text[],
      ${payloads}::text[],
      ${messageIds}::text[]
    ) as m(destination_uuid, topic, message, message_uuid),
    (select ${serialization}::text) as s(serialization)
    on conflict (message_uuid) do nothing
  |]
  where
    destinationIds =
      [ let WorkflowId destination = m.sendDestinationId
         in destination
        | m <- messages
      ]
    topics =
      [ maybe nullTopicSentinel (\(Topic name) -> name) m.sendTopic
        | m <- messages
      ]
    payloads = [m.sendMessageBody.serializedText | m <- messages]
    messageIds = zipWith messageUUIDForSend fallbackIds messages
    -- One serialization for the batch, as in the oracle.
    serialization =
      case messages of
        []      -> "json"
        (m : _) -> maybe "json" (\(Serialization tag) -> tag) m.sendMessageBody.serializedSerialization

-- | Whether an unconsumed message is waiting on the topic. A yes/no question,
-- bounded to one row: the poll a waiting @recv@ runs once per interval.
probeNotificationSession ::
  WorkflowId ->
  Maybe Topic ->
  Session Bool
probeNotificationSession (WorkflowId workflowId) topic = do
  found <-
    sqlQueryTypedSession [typedSql|
      select 1
      from dbos.notifications
      where destination_uuid = ${workflowId}
        and topic = ${storedTopic}
        and consumed = false
      limit 1
    |]
  pure (maybe False (const True) found)
  where
    storedTopic = maybe nullTopicSentinel (\(Topic name) -> name) topic

-- | Take the oldest unconsumed message and record it as the @DBOS.recv@ step
-- in one statement, so a taken message is never one nobody recorded. Returns
-- the message when one was taken. A second take of the same step (a duplicate
-- execution racing the winner) takes another message but the step row already
-- exists, so the insert is a no-op and the caller falls back to the winner's
-- record instead of dying on the conflict.
takeNotificationSession ::
  WorkflowId ->
  OperationId ->
  Maybe Topic ->
  Timestamp ->
  Timestamp ->
  Session (Maybe SerializedWorkflowValue)
takeNotificationSession
  (WorkflowId workflowId)
  (OperationId operationId)
  topic
  (Timestamp startedMs)
  (Timestamp completedMs) = do
    rows <-
      sqlQueryTypedSession [typedSql|
        with taken as (
          update dbos.notifications
          set consumed = true
          where message_uuid = (
            select message_uuid
            from dbos.notifications
            where destination_uuid = ${workflowId}
              and topic = ${storedTopic}
              and consumed = false
            order by created_at_epoch_ms asc
            limit 1
          )
          and consumed = false
          returning message, serialization
        )
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
        select
          ${workflowId},
          ${functionId},
          ${recvStepName},
          taken.message,
          null,
          null,
          ${startedMs},
          ${completedMs},
          taken.serialization
        from taken
        on conflict (workflow_uuid, function_id) do nothing
        returning output, serialization
      |]
    pure (listToMaybe rows >>= decodeTaken)
  where
    storedTopic = maybe nullTopicSentinel (\(Topic name) -> name) topic
    functionId = fromIntegral operationId :: Int
    decodeTaken row = serializedWorkflowValue row.output row.serialization

-- | The @DBOS.recv@ step as recorded, distinguishing three cases: no step
-- (@Nothing@), a recorded absence such as a timeout (@Just Nothing@), and a
-- taken message (@Just (Just value)@). SQL NULL output is the recorded
-- absence, matching the oracle's @Outcome::Output(None)@.
fetchRecvStepSession ::
  WorkflowId ->
  OperationId ->
  Session (Maybe (Maybe SerializedWorkflowValue))
fetchRecvStepSession (WorkflowId workflowId) (OperationId operationId) =
  fmap decodeRecvStep
    <$> sqlQueryTypedSession [typedSql|
    select output, serialization
    from dbos.operation_outputs
    where workflow_uuid = ${workflowId}
      and function_id = ${functionId}
      and function_name = ${recvStepName}
    limit 1
  |]
  where
    functionId = fromIntegral operationId :: Int
    decodeRecvStep row = serializedWorkflowValue row.output row.serialization

-- | Record a @DBOS.recv@ step that took nothing (a timeout). The absence is
-- SQL NULL output, which a replay reads back as @Just Nothing@.
recordRecvSession ::
  WorkflowId ->
  OperationId ->
  Maybe SerializedWorkflowValue ->
  Timestamp ->
  Timestamp ->
  Session ()
recordRecvSession
  (WorkflowId workflowId)
  (OperationId operationId)
  taken
  (Timestamp startedMs)
  (Timestamp completedMs) =
    void $ sqlExecTypedSession [typedSql|
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
          ${workflowId},
          ${functionId},
          ${recvStepName},
          ${outputText}::text,
          null,
          null,
          ${startedMs},
          ${completedMs},
          ${serialization}::text
        )
      on conflict (workflow_uuid, function_id) do nothing
    |]
  where
    functionId = fromIntegral operationId :: Int
    outputText = (.serializedText) <$> taken
    serialization = serializedWorkflowSerialization taken

-- | Workflow ids with the given name, newest first. The approval list asks
-- the database rather than remembering what this process started.
listWorkflowIdsByNameSession ::
  Text ->
  Int64 ->
  Session [WorkflowId]
listWorkflowIdsByNameSession workflowName limitCount = do
  rows <-
    sqlQueryTypedSession [typedSql|
      select workflow_uuid
      from dbos.workflow_status
      where name = ${workflowName}
      order by created_at desc, workflow_uuid desc
      limit ${limitCount}
    |]
  pure [WorkflowId key | Id key <- rows]

-- | Register a queue, seeding its worker concurrency. 'LeaveExisting' leaves
-- an existing row alone; 'UpdateExisting' overwrites it.
registerQueueSession ::
  QueueName ->
  Maybe Int ->
  OnExistingQueue ->
  Session ()
registerQueueSession (QueueName queueName) workerConcurrency onExisting =
  case onExisting of
    LeaveExisting ->
      void $ sqlExecTypedSession [typedSql|
        insert into dbos.queues
          (name, worker_concurrency, created_at, updated_at)
        values
          (
            ${queueName},
            ${workerConcurrency},
            (extract(epoch from clock_timestamp()) * 1000)::bigint,
            (extract(epoch from clock_timestamp()) * 1000)::bigint
          )
        on conflict (name) do nothing
      |]
    UpdateExisting ->
      overwrite
  where
    overwrite =
      void $ sqlExecTypedSession [typedSql|
        insert into dbos.queues
          (name, worker_concurrency, created_at, updated_at)
        values
          (
            ${queueName},
            ${workerConcurrency},
            (extract(epoch from clock_timestamp()) * 1000)::bigint,
            (extract(epoch from clock_timestamp()) * 1000)::bigint
          )
        on conflict (name) do update
        set worker_concurrency = excluded.worker_concurrency,
            updated_at = excluded.updated_at
      |]

-- | The limit stored in the queue's row, which is what a dequeue honours.
fetchQueueWorkerConcurrencySession ::
  QueueName ->
  Session (Maybe Int)
fetchQueueWorkerConcurrencySession (QueueName queueName) = do
  stored <-
    sqlQueryTypedSession [typedSql|
      select worker_concurrency
      from dbos.queues
      where name = ${queueName}
      limit 1
    |]
  pure (join stored)

-- | The Apply button: change the stored limit without a restart. Every worker
-- re-reads the row, so the whole fleet picks it up.
updateQueueWorkerConcurrencySession ::
  QueueName ->
  Int ->
  Session ()
updateQueueWorkerConcurrencySession (QueueName queueName) workerConcurrency =
  void $ sqlExecTypedSession [typedSql|
    update dbos.queues
    set worker_concurrency = ${workerConcurrency},
        updated_at = (extract(epoch from clock_timestamp()) * 1000)::bigint
    where name = ${queueName}
  |]

-- | Record a workflow as ENQUEUED on a queue. Whichever executor next polls
-- the queue claims it; the enqueuer does not run it just because it asked.
enqueueWorkflowSession ::
  WorkflowId ->
  WorkflowName ->
  QueueName ->
  Session ()
enqueueWorkflowSession (WorkflowId workflowId) (WorkflowName workflowName) (QueueName queueName) =
  void $ sqlExecTypedSession [typedSql|
    insert into dbos.workflow_status
      (
        workflow_uuid,
        status,
        name,
        queue_name,
        executor_id,
        created_at,
        updated_at,
        application_version,
        recovery_attempts,
        priority,
        inputs,
        serialization,
        parent_workflow_id
      )
    values
      (
        ${workflowId},
        'ENQUEUED',
        ${workflowName},
        ${queueName},
        null,
        (extract(epoch from clock_timestamp()) * 1000)::bigint,
        (extract(epoch from clock_timestamp()) * 1000)::bigint,
        'v1',
        0,
        0,
        null,
        'json',
        null
      )
    on conflict (workflow_uuid) do nothing
  |]

-- | Claim up to the stored worker-concurrency limit, counting this executor's
-- own running workflows against it, oldest first. A missing queue row or a
-- NULL limit means unbounded, matching the oracle (the internal queue carries
-- no row). The candidates are locked @SKIP LOCKED@ so peers step over rows
-- another executor is claiming, and the outer update re-checks @ENQUEUED@ so
-- a lost race claims nothing.
dequeueWorkflowsSession ::
  QueueName ->
  ExecutorId ->
  ApplicationVersion ->
  Session [WorkflowId]
dequeueWorkflowsSession (QueueName queueName) (ExecutorId executorId) (ApplicationVersion applicationVersion) = do
  claimed <-
    sqlQueryTypedSession [typedSql|
      with candidates as (
        select w.workflow_uuid
        from dbos.workflow_status w
        where w.queue_name = ${queueName}
          and w.status = 'ENQUEUED'
        order by w.priority asc, w.created_at asc
        limit (
          select case
            when count(*) filter (where q.worker_concurrency is not null) = 0 then null
            else greatest(
              max(q.worker_concurrency) - (
                select count(*)
                from dbos.workflow_status r
                where r.queue_name = ${queueName}
                  and r.status = 'PENDING'
                  and r.executor_id = ${executorId}
              ),
              0
            )
          end
          from dbos.queues q
          where q.name = ${queueName}
        )
        for update skip locked
      )
      update dbos.workflow_status
      set status = 'PENDING',
          executor_id = ${executorId},
          application_version = ${applicationVersion},
          started_at_epoch_ms = (extract(epoch from clock_timestamp()) * 1000)::bigint,
          updated_at = (extract(epoch from clock_timestamp()) * 1000)::bigint,
          recovery_attempts = coalesce(recovery_attempts, 0) + 1
      where workflow_uuid in (select workflow_uuid from candidates)
        and status = 'ENQUEUED'
      returning workflow_uuid
    |]
  pure [WorkflowId key | Id key <- claimed]

-- | Statuses for a batch of workflows, for the queue tab's counts.
fetchWorkflowStatusesSession ::
  [WorkflowId] ->
  Session [(WorkflowId, WorkflowStatus)]
fetchWorkflowStatusesSession workflowIds = do
  rows <-
    sqlQueryTypedSession [typedSql|
      select workflow_uuid, status
      from dbos.workflow_status
      where workflow_uuid = any(${ids}::text[])
    |]
  -- One undecodable row (a status this port does not know yet) skips instead
  -- of failing the whole batch; the queue tab counts what it can read.
  pure (mapMaybe decodeStatus rows)
  where
    ids = [workflowId | WorkflowId workflowId <- workflowIds]
    decodeStatus row = case row.workflow_uuid of
      Id key -> (,) (WorkflowId key) <$> either (const Nothing) Just (parseWorkflowStatus (fromMaybe "" row.status))

-- | Return a dead executor's abandoned workflows to their queues. One
-- statement: @PENDING@ rows it owns go back to @ENQUEUED@ with no start
-- time, so the queue wait does not read as execution. A repeat costs
-- nothing — a row a live executor has since claimed no longer matches.
reenqueueForRecoverySession ::
  ExecutorId ->
  ApplicationVersion ->
  QueueName ->
  Session [WorkflowId]
reenqueueForRecoverySession (ExecutorId executorId) (ApplicationVersion applicationVersion) (QueueName recoveryQueue) = do
  recovered <-
    sqlQueryTypedSession [typedSql|
      update dbos.workflow_status
      set status = 'ENQUEUED',
          started_at_epoch_ms = null,
          updated_at = (extract(epoch from clock_timestamp()) * 1000)::bigint,
          queue_name = coalesce(nullif(queue_name, ''), ${recoveryQueue})
      where status = 'PENDING'
        and executor_id = ${executorId}
        and application_version = ${applicationVersion}
      returning workflow_uuid
    |]
  pure [WorkflowId key | Id key <- recovered]

-- | Release this executor's own claim on a workflow it will not run back to
-- @ENQUEUED@, so a later pass — here or after a restart — can pick it up.
-- The @PENDING@-and-owner guard means a row another executor has since
-- claimed (or finished) is never touched; a repeat costs nothing.
releaseWorkflowClaimSession ::
  ExecutorId ->
  WorkflowId ->
  Session ()
releaseWorkflowClaimSession (ExecutorId executorId) (WorkflowId workflowId) =
  void $ sqlExecTypedSession [typedSql|
    update dbos.workflow_status
    set status = 'ENQUEUED',
        executor_id = null,
        started_at_epoch_ms = null,
        updated_at = (extract(epoch from clock_timestamp()) * 1000)::bigint
    where workflow_uuid = ${workflowId}
      and status = 'PENDING'
      and executor_id = ${executorId}
  |]

decodeWorkflowExecutionRow :: WorkflowExecutionRowRaw -> WorkflowExecutionRow
decodeWorkflowExecutionRow row =
  WorkflowExecutionRow
    { rowWorkflowId = case row.workflow_uuid of Id key -> WorkflowId key,
      -- @status@ is nullable in the schema (zero such rows in practice); an
      -- absent status fails downstream as @UnknownWorkflowStatus ""@ rather
      -- than crashing the row decode.
      rowWorkflowStatus = fromMaybe "" row.status,
      rowWorkflowName = row.name,
      rowWorkflowParentId = WorkflowId <$> row.parent_workflow_id,
      rowWorkflowInputs = row.inputs,
      rowWorkflowOutput = serializedWorkflowValue row.output row.serialization,
      rowWorkflowError = serializedWorkflowValue row.error row.serialization,
      rowWorkflowExecutor = row.executor_id,
      rowWorkflowCreatedAt = Just (Timestamp row.created_at),
      rowWorkflowUpdatedAt = Just (Timestamp row.updated_at),
      rowWorkflowRecoveryAttempts = row.recovery_attempts,
      rowWorkflowQueueName = row.queue_name,
      rowWorkflowSerialization = row.serialization,
      rowWorkflowApplicationVersion = row.application_version
    }

decodeOperationCheckpoint ::
  OperationCheckpointRaw ->
  Either OperationCheckpointDecodeError OperationCheckpoint
decodeOperationCheckpoint row =
  parseOperationCheckpoint
    decodedOperationId
    decodedOperationName
    (serializedWorkflowValue row.output row.serialization)
    (serializedWorkflowValue row.error row.serialization)
    (WorkflowId <$> row.child_workflow_id)
    (Timestamp <$> row.started_at_epoch_ms)
    (Timestamp <$> row.completed_at_epoch_ms)
  where
    decodedOperationId = case row.function_id of Id key -> OperationId (fromIntegral key)
    decodedOperationName = OperationCheckpointTypes.OperationName row.function_name

decodeNotificationRow :: NotificationRaw -> NotificationRow
decodeNotificationRow row =
  NotificationRow
    { notificationDestinationId = case row.destination_uuid of Id key -> WorkflowId key,
      -- @topic@ is nullable in the schema; Python writes the
      -- @__null__topic__@ sentinel instead, so a null here fails downstream
      -- rather than crashing the row decode.
      notificationTopic = fromMaybe "" row.topic,
      notificationMessage =
        SerializedWorkflowValue
          { serializedText = row.message,
            serializedSerialization = Serialization <$> row.serialization
          },
      notificationMessageUUID = case row.message_uuid of Id key -> MessageUUID key,
      notificationConsumed = row.consumed
    }

serializedWorkflowValue ::
  Maybe Text ->
  Maybe Text ->
  Maybe SerializedWorkflowValue
serializedWorkflowValue value serialization =
  SerializedWorkflowValue <$> value <*> pure (Serialization <$> serialization)

serializedWorkflowSerialization :: Maybe SerializedWorkflowValue -> Maybe Text
serializedWorkflowSerialization value =
  case value >>= (.serializedSerialization) of
    Just (Serialization serialization) -> Just serialization
    Nothing                            -> Nothing

data WorkflowStartDecision
  = StartWorkflow
  | AwaitWorkflow
  deriving stock (Eq, Show)

-- | A database call that never answered: pool exhaustion, a lost connection,
-- a session error. Thrown as the shared 'Error' channel's 'Backend' case
-- (never recorded) so a transient outage cannot become a permanent @ERROR@
-- workflow outcome; the row stays @PENDING@ for a later launch to recover.
-- Classification mirrors the oracle's: contention and connection trouble
-- may pass, a rejected statement will not.
classifyUsageError :: Pool.UsageError -> Error
classifyUsageError usage =
  Backend
    BackendError
      { backendMessage = message,
        backendSqlState = sqlState,
        backendKind = kind
      }
  where
    (message, sqlState, kind) = case usage of
      Pool.ConnectionUsageError connectionError ->
        (Errors.toDetailedText connectionError, Nothing, Connection)
      Pool.AcquisitionTimeoutUsageError ->
        ("connection acquisition timed out", Nothing, Connection)
      Pool.SessionUsageError sessionError ->
        (Errors.toDetailedText sessionError, sqlStateOf sessionError, sessionKind sessionError)
    sqlStateOf sessionError = lookup "code" (Errors.toDetails sessionError)
    sessionKind sessionError
      | Just code <- sqlStateOf sessionError,
        code `elem` ["40001", "40P01"] = Transient
      | Errors.isTransient sessionError = Connection
      | Just code <- sqlStateOf sessionError,
        "08" `Text.isPrefixOf` code = Connection
      | otherwise = Permanent

acquirePool :: IO Pool.Pool
acquirePool = do
  settings <- getConnectionSettings
  Pool.acquire
    ( PoolConfig.settings
        [ PoolConfig.size 8,
          PoolConfig.acquisitionTimeout 10,
          PoolConfig.agingTimeout 1800,
          PoolConfig.idlenessTimeout 1800,
          PoolConfig.staticConnectionSettings settings
        ]
    )

releasePool :: Pool.Pool -> IO ()
releasePool =
  Pool.release

runDb :: Pool.Pool -> Session a -> IO (Either Pool.UsageError a)
runDb =
  Pool.use

runDbOrFail :: Pool.Pool -> Session a -> IO a
runDbOrFail pool session = do
  result <- runDb pool session
  case result of
    Left err    -> throwIO (classifyUsageError err)
    Right value -> pure value

fetchWorkflowExecutionRow ::
  Pool.Pool ->
  WorkflowId ->
  IO (Maybe WorkflowExecutionRow)
fetchWorkflowExecutionRow pool workflowId =
  runDbOrFail pool (fetchWorkflowExecutionRowSession workflowId)

fetchWorkflowStatus ::
  Pool.Pool ->
  WorkflowId ->
  IO (Maybe WorkflowStatus)
fetchWorkflowStatus pool workflowId =
  runDbOrFail pool (fetchWorkflowStatusSession workflowId)

fetchOperationCheckpoint ::
  Pool.Pool ->
  WorkflowId ->
  OperationId ->
  IO (Maybe OperationCheckpoint)
fetchOperationCheckpoint pool workflowId operationId =
  runDbOrFail pool (fetchOperationCheckpointSession workflowId operationId)

fetchNotification ::
  Pool.Pool ->
  MessageUUID ->
  IO (Maybe NotificationRow)
fetchNotification pool messageUUID =
  runDbOrFail pool (fetchNotificationSession messageUUID)

fetchMigrationVersion ::
  Pool.Pool ->
  IO (Maybe Int64)
fetchMigrationVersion pool =
  fmap (\(DbosMigration version) -> version)
    <$> runDbOrFail pool migrationVersionSession

tryStartWorkflow ::
  Pool.Pool ->
  WorkflowId ->
  WorkflowName ->
  Maybe SerializedWorkflowValue ->
  ExecutorId ->
  ApplicationVersion ->
  IO WorkflowStartDecision
tryStartWorkflow pool workflowId workflowName inputs executorId applicationVersion = do
  started <- runDbOrFail pool (tryStartWorkflowSession workflowId workflowName inputs executorId applicationVersion)
  pure $
    if started
      then StartWorkflow
      else AwaitWorkflow

updateWorkflowOutcome ::
  Pool.Pool ->
  WorkflowId ->
  ExecutorId ->
  WorkflowStatus ->
  Maybe SerializedWorkflowValue ->
  Maybe SerializedWorkflowValue ->
  IO ()
updateWorkflowOutcome pool workflowId executorId status output errorValue =
  runDbOrFail pool (updateWorkflowOutcomeSession workflowId executorId status output errorValue)

recordOperationOutput ::
  Pool.Pool ->
  WorkflowId ->
  OperationId ->
  OperationName ->
  SerializedWorkflowValue ->
  IO ()
recordOperationOutput pool workflowId operationId operationName output =
  runDbOrFail pool (recordOperationOutputSession workflowId operationId operationName output)

recordOperationError ::
  Pool.Pool ->
  WorkflowId ->
  OperationId ->
  OperationName ->
  SerializedWorkflowValue ->
  IO ()
recordOperationError pool workflowId operationId operationName errorValue =
  runDbOrFail pool (recordOperationErrorSession workflowId operationId operationName errorValue)

recordSleep ::
  Pool.Pool ->
  WorkflowId ->
  OperationId ->
  OperationName ->
  SerializedWorkflowValue ->
  Timestamp ->
  Timestamp ->
  IO ()
recordSleep pool workflowId operationId operationName output started completed =
  runDbOrFail pool (recordSleepSession workflowId operationId operationName output started completed)

setEvent ::
  Pool.Pool ->
  WorkflowId ->
  Text ->
  SerializedWorkflowValue ->
  IO ()
setEvent pool workflowId key value =
  runDbOrFail pool (setEventSession workflowId key value)

getEvent ::
  Pool.Pool ->
  WorkflowId ->
  Text ->
  IO (Maybe SerializedWorkflowValue)
getEvent pool workflowId key =
  runDbOrFail pool (getEventSession workflowId key)

-- | Block until the key is published or the deadline passes. Absence at the
-- deadline is 'Nothing', never an error. A zero timeout degrades to one poll.
-- Polling, not LISTEN/NOTIFY: semantically identical, no connection held.
getEventBlocking ::
  Pool.Pool ->
  WorkflowId ->
  Text ->
  Duration ->
  IO (Maybe SerializedWorkflowValue)
getEventBlocking pool workflowId key duration = do
  start <- timestampNow
  let deadline = fromMaybe start (addTimeout start duration)
  poll deadline
  where
    poll deadline = do
      found <- getEvent pool workflowId key
      case found of
        Just value -> pure (Just value)
        Nothing -> do
          now <- timestampNow
          if now >= deadline
            then pure Nothing
            else do
              threadDelay blockingPollIntervalMicros
              poll deadline

blockingPollIntervalMicros :: Int
blockingPollIntervalMicros = 50000

-- | Deliver one message. A resend under the same idempotency key is a no-op.
sendMessage ::
  Pool.Pool ->
  SendMessage ->
  IO ()
sendMessage pool message =
  sendMessages pool [message]

-- | Deliver a batch in one statement, so it is all-or-nothing. Fallback ids
-- are generated once, so a retry after a lost acknowledgement does not
-- deliver everything a second time.
sendMessages ::
  Pool.Pool ->
  [SendMessage] ->
  IO ()
sendMessages pool messages = do
  fallbacks <- traverse (const (fmap (MessageUUID . UUID.toText) UUID.V4.nextRandom)) messages
  runDbOrFail pool (sendMessagesSession fallbacks messages)

-- | Block until a message is waiting on the topic, then take the oldest one
-- and record it as the @DBOS.recv@ step in one statement. A replay returns
-- the message the first run took and takes no other; absence at the deadline
-- is recorded, so a replay of a timeout stays a timeout.
recvMessage ::
  Pool.Pool ->
  WorkflowId ->
  OperationId ->
  Duration ->
  Maybe Topic ->
  IO (Maybe SerializedWorkflowValue)
recvMessage pool workflowId recvId duration topic = do
  recorded <- runDbOrFail pool (fetchRecvStepSession workflowId recvId)
  case recorded of
    Just taken -> pure taken
    Nothing -> do
      startedAt <- timestampNow
      let deadline = fromMaybe startedAt (addTimeout startedAt duration)
      waitForMessage startedAt deadline
  where
    waitForMessage startedAt deadline = do
      hasMessage <- runDbOrFail pool (probeNotificationSession workflowId topic)
      if hasMessage
        then takeMessage startedAt deadline
        else do
          now <- timestampNow
          let remaining = timestampToEpochMs deadline - timestampToEpochMs now
          if remaining <= 0
            then recordTimeout startedAt
            else do
              threadDelay (millisToMicros (min remaining recvPollIntervalMs))
              waitForMessage startedAt deadline
    takeMessage startedAt deadline = do
      completedAt <- timestampNow
      taken <-
        runDbOrFail pool (takeNotificationSession workflowId recvId topic startedAt completedAt)
      case taken of
        Just value -> pure (Just value)
        -- Nothing taken: either no message is waiting, or a duplicate
        -- execution won the step row first. A recorded step (a message or a
        -- timeout) is the answer either way; only a missing row keeps waiting.
        Nothing -> do
          recorded <- runDbOrFail pool (fetchRecvStepSession workflowId recvId)
          case recorded of
            Just replayed -> pure replayed
            Nothing       -> waitForMessage startedAt deadline
    recordTimeout startedAt = do
      completedAt <- timestampNow
      runDbOrFail pool (recordRecvSession workflowId recvId Nothing startedAt completedAt)
      pure Nothing

-- | How long a waiting @recv@ sleeps between probes.
recvPollIntervalMs :: Int64
recvPollIntervalMs = 50

-- | Workflow ids with the given name, newest first.
listWorkflowIdsByName ::
  Pool.Pool ->
  Text ->
  Int64 ->
  IO [WorkflowId]
listWorkflowIdsByName pool workflowName limitCount =
  runDbOrFail pool (listWorkflowIdsByNameSession workflowName limitCount)

millisToMicros :: Int64 -> Int
millisToMicros ms = fromIntegral (ms * 1000)

-- | Register a queue, seeding its worker concurrency.
registerQueue ::
  Pool.Pool ->
  QueueName ->
  Int ->
  OnExistingQueue ->
  IO ()
registerQueue pool queueName workerConcurrency onExisting =
  runDbOrFail pool (registerQueueSession queueName (Just workerConcurrency) onExisting)

-- | The limit stored in the queue's row.
fetchQueueWorkerConcurrency ::
  Pool.Pool ->
  QueueName ->
  IO (Maybe Int)
fetchQueueWorkerConcurrency pool queueName =
  runDbOrFail pool (fetchQueueWorkerConcurrencySession queueName)

-- | Change the stored limit without a restart.
updateQueueWorkerConcurrency ::
  Pool.Pool ->
  QueueName ->
  Int ->
  IO ()
updateQueueWorkerConcurrency pool queueName workerConcurrency =
  runDbOrFail pool (updateQueueWorkerConcurrencySession queueName workerConcurrency)

-- | Record a workflow as ENQUEUED on a queue.
enqueueWorkflow ::
  Pool.Pool ->
  WorkflowId ->
  WorkflowName ->
  QueueName ->
  IO ()
enqueueWorkflow pool workflowId workflowName queueName =
  runDbOrFail pool (enqueueWorkflowSession workflowId workflowName queueName)

-- | Claim up to the stored limit, counting this executor's running workflows.
dequeueWorkflows ::
  Pool.Pool ->
  QueueName ->
  ExecutorId ->
  ApplicationVersion ->
  IO [WorkflowId]
dequeueWorkflows pool queueName executorId applicationVersion =
  runDbOrFail pool (dequeueWorkflowsSession queueName executorId applicationVersion)

-- | Statuses for a batch of workflows.
fetchWorkflowStatuses ::
  Pool.Pool ->
  [WorkflowId] ->
  IO [(WorkflowId, WorkflowStatus)]
fetchWorkflowStatuses pool workflowIds =
  runDbOrFail pool (fetchWorkflowStatusesSession workflowIds)

-- | Return a dead executor's abandoned workflows to their queues, reporting
-- what moved. Synchronous in @launch@, before it returns, so it never tears
-- a workflow off a runner that started the instant it did.
reenqueueForRecovery ::
  Pool.Pool ->
  ExecutorId ->
  ApplicationVersion ->
  QueueName ->
  IO [WorkflowId]
reenqueueForRecovery pool executorId applicationVersion recoveryQueue =
  runDbOrFail pool (reenqueueForRecoverySession executorId applicationVersion recoveryQueue)

-- | Release this executor's own claim on a workflow it will not run, so a
-- later pass can pick it up.
releaseWorkflowClaim ::
  Pool.Pool ->
  ExecutorId ->
  WorkflowId ->
  IO ()
releaseWorkflowClaim pool executorId workflowId =
  runDbOrFail pool (releaseWorkflowClaimSession executorId workflowId)

-- | The engine's durable seams over a live pool: what production bodies
-- receive. Simulations build the same records over an in-memory model.
postgresStepStore :: Pool.Pool -> StepStore IO
postgresStepStore pool =
  StepStore
    { stepFetchResult = fetchOperationCheckpoint pool,
      stepRecordOutput = \workflowId operationId operationName output ->
        recordOperationOutput pool workflowId operationId operationName output
    }

-- | Workflow events over a live pool.
postgresEventStore :: Pool.Pool -> EventStore IO
postgresEventStore pool =
  EventStore
    { eventGet = getEvent pool,
      eventSet = setEvent pool
    }

getConnectionSettings :: IO Connection.Settings
getConnectionSettings = do
  databaseURL <- lookupEnv "DBOS_DATABASE_URL"
  case databaseURL of
    Just url -> pure (Connection.connectionString (toText url))
    Nothing  -> getConnectionSettingsFromPGEnv

getConnectionSettingsFromPGEnv :: IO Connection.Settings
getConnectionSettingsFromPGEnv = do
  host <- lookupEnv "PGHOST"
  port <- lookupEnv "PGPORT"
  dbname <- lookupEnv "PGDATABASE"
  user <- lookupEnv "PGUSER"
  password <- lookupEnv "PGPASSWORD"
  pure $
    mconcat
      [ Connection.hostAndPort
          (maybe "127.0.0.1" toText host)
          (maybe 5432 parsePort port),
        Connection.dbname (maybe "dbos" toText dbname),
        Connection.user (maybe "postgres" toText user),
        Connection.password (maybe "pgpasswd" toText password)
      ]

parsePort :: String -> Word16
parsePort raw =
  maybe 5432 fromIntegral (readMaybe raw :: Maybe Int)

toText :: String -> Text
toText =
  Text.pack
