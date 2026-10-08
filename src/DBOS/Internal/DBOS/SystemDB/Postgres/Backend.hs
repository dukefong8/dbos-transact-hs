{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE LambdaCase          #-}
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
module DBOS.SystemDB.Postgres.Backend
  (     Pool.Pool,
    WorkflowStartDecision (..),
    DbosMigration (..),
    acquirePool,
    dequeueWorkflows,
    dequeueWorkflowsSession,
    fetchMigrationVersion,
    fetchNotification,
    fetchNotificationSession,
    fetchQueueWorkerConcurrency,
    fetchQueueWorkerConcurrencySession,
    fetchRecvStepSession,
    fetchWorkflowStatus,
    fetchWorkflowStatusSession,
    fetchWorkflowStatuses,
    fetchWorkflowStatusesSession,
    legacyGetEvent,
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
    recordSleepSession,
    recvMessage,
    legacyReenqueueForRecovery,
    reenqueueForRecoverySession,
    registerQueue,
    registerQueueSession,
    releasePool,
    releaseWorkflowClaim,
    releaseWorkflowClaimSession,
    runDb,
    runDbOrFail,
    legacySendMessage,
    legacySendMessages,
    sendMessagesSession,
    legacySetEvent,
    setEventSession,
    takeNotificationSession,
    updateQueueWorkerConcurrency,
    updateQueueWorkerConcurrencySession,
    -- * Backend handle (postgres.rs rewrite, Phase 7)
    Settings (..),
    Config (..),
    defaultSettings,
    configNew,
    configFromEnv,
    PostgresSystemDB (..),
    fromPool,
    acquirePostgresSystemDB,
    activatePostgresSystemDB,
    releasePostgresSystemDB,
    withPostgresSystemDB,
    verifySystemDatabase,
    migrationCeiling,
    runSession,
    isTransportFailure,
    pollingLimit,
    classifyUsageError,
    isUniqueViolation,
    isForeignKeyViolation,
  )
where

import DBOS.Prelude

import Data.Aeson (eitherDecodeStrict)
import Data.Aeson qualified as Aeson
import Data.ByteString.Lazy qualified as LBS
import Data.Int (Int64)
import Data.List qualified as List
import Data.Map.Strict qualified as Map
import Data.Set qualified as Set
import Data.Text qualified as Text
import Data.Text.Encoding (decodeUtf8)
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB.Class (SystemDB (..))
import DBOS.SystemDB.Error (BackendError (..), BackendErrorKind (..), Error (..), invalidInput)
import DBOS.SystemDB.Notify (Registry, Subscription, eventKey, eventsChannel, messageKey, newRegistry, notified, subscribe, subscribeExclusive, unsubscribe)
import DBOS.SystemDB.Postgres.Notifier (Notifier, enable, notifierNew, run, signal, stop)
import DBOS.SystemDB.Postgres.Statements qualified as Statements
import DBOS.SystemDB.Retry (RetryPolicy (..), SysdbEvent (..), defaultRetryPolicy, uuidEntropy, withRetry)
import DBOS.SystemDB.Types (ApplicationRowCounts (..), ApplicationVersion (..), Applications (..), AwaitedOutcome (..), Debounce (..), DebounceHolder (..), DebounceRequest (..), Duration (..), EncodedValue (..), EventRecord (..), ExecutorId (..), Fork (..), ForkOptions (..), ForkPoint (..), GetEventCaller (..), IdempotencyKey (..), MessageUUID (..), NewQueue (..), NewSchedule (..), NewWorkflow (..), NotificationRecord (..), NotificationRow (..), OnExistingQueue (..), Outcome (..), OutcomeWrite (..), QueueName (..), QueueRecord (..), RateLimit (..), RenameBatching (..), RenameFrom, ResolvedLimits (..), ScheduleFilter (..), ScheduleRecord (..), ScheduleStatus (..), ScheduleUpdate (..), SendMessage (..), Serialization (..), SerializedWorkflowValue (..), StepRecord (..), StepTiming (..), Timestamp (..), Topic (..), VersionInfo (..), WorkflowFilter (..), WorkflowId (..), WorkflowInitResult (..), WorkflowRecord (..), WorkflowStatus (..), addTimeout, applyQueueUpdate, changeIsLeave, changeSet, claimsOwnership, createScheduleStepName, debounceStepName, debounceValidate, deleteScheduleStepName, dequeueSweepCap, durationAsMillis, durationFromMs, durationFromSecs, durationSince, forkOptionsValidate, forkValidate, getScheduleStepName, initialStatus, internalQueueName, isQueueUpdateEmpty, isScheduleUpdateEmpty, isTerminal, isValidApplicationName, listSchedulesStepName, messageUUIDForSend, nullTopicSentinel, outcomeColumns, outcomeStatus, parseScheduleStatus, parseWorkflowStatus, pauseScheduleStepName, queueResolvedLimits, recvStepName, renameFromApplication, resolveWorkflowDelay, resumeScheduleStepName, scheduleStatusText, secondsDuration, sendBulkStepName, sendStepName, sleepStepName, timestampFromEpochMs, timestampFromIso8601, timestampNow, timestampToEpochMs, timestampToIso8601, updateScheduleStepName, upsertScheduleStepName, validateAttributes, validateNewWorkflow, workflowStatusText)
import DBOS.SystemDB.Types qualified as Types
import DBOS.Transact.Logger (SomeTracer, runTracer)
import Hasql.Connection.Settings qualified as Connection
import Hasql.Decoders qualified as Decoders
import Hasql.Errors qualified as Errors
import Hasql.Pool qualified as Pool
import Hasql.Pool.Config qualified as PoolConfig
import Hasql.Session (Session)
import Hasql.Session qualified as Session
import Hasql.Transaction qualified as Tx
import Hasql.Transaction.Sessions qualified as TxSessions
import IHP.TypedSql.Hasql (sqlExecTypedSession, sqlQueryTypedSession, typedSql)

import IHP.TypedSql.Id (Id' (..))
import IHP.TypedSql.Row (TypedSqlRow (..))
import IHP.TypedSql.RowType (SqlRow)
import System.Environment (lookupEnv)
import System.Timeout qualified as Timeout

type NotificationRaw =
  SqlRow
    '[ '("destination_uuid", Id' "workflow_status"),
       '("topic", Maybe Text),
       '("message", Text),
       '("message_uuid", Id' "notifications"),
       '("serialization", Maybe Text),
       '("consumed", Bool)
     ]

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

-- | Highest applied migration version. The ceiling this port tracks is 114
-- (ranges 1–47 + 100–114); a higher value means the Rust corpus moved and the
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

recordOperationOutputSession ::
  WorkflowId ->
  Int ->
  Text ->
  SerializedWorkflowValue ->
  Session ()
recordOperationOutputSession
  (WorkflowId workflowId)
  operationId
  operationName
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
  Int ->
  Text ->
  SerializedWorkflowValue ->
  Session ()
recordOperationErrorSession
  (WorkflowId workflowId)
  operationId
  operationName
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
  Int ->
  Text ->
  SerializedWorkflowValue ->
  Timestamp ->
  Timestamp ->
  Session ()
recordSleepSession
  (WorkflowId workflowId)
  operationId
  operationName
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
  Int ->
  Maybe Topic ->
  Timestamp ->
  Timestamp ->
  Session (Maybe SerializedWorkflowValue)
takeNotificationSession
  (WorkflowId workflowId)
  operationId
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
  Int ->
  Session (Maybe (Maybe SerializedWorkflowValue))
fetchRecvStepSession (WorkflowId workflowId) operationId =
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
  Int ->
  Maybe SerializedWorkflowValue ->
  Timestamp ->
  Timestamp ->
  Session ()
recordRecvSession
  (WorkflowId workflowId)
  operationId
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

-- | Whether a database error's message is really a transport failure wearing
-- a SQLSTATE. Only consulted for the internal-error class, where the code
-- says nothing beyond "something went wrong". The needles are Go's @net@
-- package errors, which is what CockroachDB embeds when the connection to a
-- client dies under it. Deliberately narrow: a false match here classifies a
-- permanent failure as a connection one, and 'withRetry' retries those
-- forever. Mirrors Rust @is_transport_failure@, including ASCII-only
-- case-folding.
isTransportFailure :: Text -> Bool
isTransportFailure message = any (`Text.isInfixOf` folded) needles
  where
    folded = Text.map asciiLower message
    asciiLower c
      | 'A' <= c && c <= 'Z' = toEnum (fromEnum c + 32)
      | otherwise = c
    needles =
      [ "i/o timeout",
        "broken pipe",
        "connection reset by peer",
        "connection refused",
        "use of closed network connection"
      ]

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
        (Errors.toDetailedText connectionError, Nothing, connectionKind connectionError)
      Pool.AcquisitionTimeoutUsageError ->
        ("connection acquisition timed out", Nothing, Connection)
      Pool.SessionUsageError sessionError ->
        (Errors.toDetailedText sessionError, sessionSqlState sessionError, sessionKind sessionError)

-- | The retry verdict for a hasql session failure. Mirrors Rust @classify@:
-- the @40@ class is transient; @08@, @53@ and @57@ are connection failures
-- (@53@ grouped with connections following Python, since running out of
-- connections is a failure to obtain one); @XX@ reads the message for a
-- transport failure wearing an internal-error code; everything else the
-- server reports is permanent. A failure with no SQLSTATE never reached the
-- database: a lost connection may pass, while driver, script and type
-- failures will not.
sessionKind :: Errors.SessionError -> BackendErrorKind
sessionKind sessionError = case sessionError of
  Errors.ConnectionSessionError _ -> Connection
  Errors.StatementSessionError _ _ _ _ _ (Errors.ServerStatementError (Errors.ServerError code message _ _ _)) ->
    case Text.take 2 code of
      "40"                              -> Transient
      "08"                              -> Connection
      "53"                              -> Connection
      "57"                              -> Connection
      "XX" | isTransportFailure message -> Connection
      _                                 -> Permanent
  _ -> Permanent

-- | The SQLSTATE a session failure carries, if the server reported one.
sessionSqlState :: Errors.SessionError -> Maybe Text
sessionSqlState (Errors.StatementSessionError _ _ _ _ _ (Errors.ServerStatementError (Errors.ServerError code _ _ _ _))) = Just code
sessionSqlState _                                                                                                        = Nothing

-- | Connection-establishment failures: networking trouble and uncategorized
-- libpq errors may pass; authentication and compatibility failures will not.
connectionKind :: Errors.ConnectionError -> BackendErrorKind
connectionKind (Errors.NetworkingConnectionError _) = Connection
connectionKind (Errors.OtherConnectionError _)      = Connection
connectionKind _                                    = Permanent

-- Backend handle (postgres.rs rewrite, Phase 7). New code lands here behind
-- the @Postgres.*@ names the facade no longer re-exports; the old
-- pool-function block below stays untouched until P7.6 deletes it. Bodies
-- start as @undefined@ and are implemented one TDD cycle at a time.

-- | How a handle behaves against a database it is already connected to.
-- Split from 'Config' because the two constructors need different halves:
-- 'acquirePostgresSystemDB' must be told how to reach the database, while
-- 'fromPool' is handed a live pool and only needs this. Mirrors Rust
-- @Settings@ with owned 'Text' (lifetimes need no translation); @schema@
-- must be @"dbos"@ (ADR-0010), and @notificationCoalesce@ is the notifier's
-- coalescing window ('Nothing' takes the default).
data Settings = Settings
  { settingsSchema               :: Text,
    settingsRetry                :: RetryPolicy,
    settingsExecutorId           :: Maybe Text,
    settingsApplicationName      :: Maybe Text,
    settingsPollingConcurrency   :: Maybe Word32,
    settingsNotificationCoalesce :: Maybe Duration
  }
  deriving stock (Eq, Show)

-- | The defaults every implementation shares. Mirrors Rust
-- @Settings::default@.
defaultSettings :: Settings
defaultSettings =
  Settings
    { settingsSchema = "dbos",
      settingsRetry = defaultRetryPolicy,
      settingsExecutorId = Nothing,
      settingsApplicationName = Nothing,
      settingsPollingConcurrency = Nothing,
      settingsNotificationCoalesce = Nothing
    }

-- | How to reach the system database. Mirrors Rust @Config@ minus the two
-- fields Haskell never uses: @use_listen_notify@ (a migration input, and
-- Haskell does not migrate) and @migrate@ (ADR-0004: the Rust runner owns
-- migrations; 'acquirePostgresSystemDB' verifies instead). @maxConnections@
-- defaults to 10, as in @Config::new@.
data Config = Config
  { configUrl            :: Text,
    configMaxConnections :: Word32,
    configSettings       :: Settings
  }
  deriving stock (Eq, Show)

-- | A configuration with the defaults every implementation shares. Mirrors
-- Rust @Config::new@.
configNew :: Text -> Config
configNew url =
  Config
    { configUrl = url,
      configMaxConnections = 10,
      configSettings = defaultSettings
    }

-- | A configuration from the environment: @DBOS_DATABASE_URL@, falling back
-- to the @PG*@ variables (the standard libpq variables — no credentials are
-- invented when they are unset). What the tests and the starter app build on.
configFromEnv :: IO Config
configFromEnv = do
  databaseURL <- lookupEnv "DBOS_DATABASE_URL"
  case databaseURL of
    Just url -> pure (configNew (Text.pack url))
    Nothing -> do
      host <- lookupEnv "PGHOST"
      port <- lookupEnv "PGPORT"
      dbname <- lookupEnv "PGDATABASE"
      user <- lookupEnv "PGUSER"
      password <- lookupEnv "PGPASSWORD"
      -- Userinfo carries a password only when PGPASSWORD set one; otherwise
      -- libpq's own pgpass/PGPASSWORD resolution applies, as it would for any
      -- other client. No credential is invented here.
      let userinfo = maybe "postgres" Text.pack user <> maybe "" ((":" <>) . Text.pack) password
      pure
        ( configNew
            ( "postgresql://"
                <> userinfo
                <> "@"
                <> maybe "127.0.0.1" Text.pack host
                <> ":"
                <> maybe "5432" Text.pack port
                <> "/"
                <> maybe "dbos" Text.pack dbname
            )
        )

-- | A system database backed by PostgreSQL. Mirrors Rust
-- @PostgresSystemDatabase@ minus the listener (the receive side of
-- LISTEN\/NOTIFY lands with its own port) and minus what polling-only drops:
-- no @Tables@ (typedSql pins @dbos.@ identifiers). @psdbPollingPermits@ is
-- the polling-concurrency semaphore as an STM counter ('pollingLimit' sets
-- its initial count); @psdbNotify@ is the waiter registry every wait
-- subscribes to and 'signal' wakes; @psdbNotifier@ is the outbound half
-- whose flush loop the engine spawns in P7.6; @psdbLog@ is the explicit
-- logger the retry loop warns through (Rule 5).
data PostgresSystemDB = PostgresSystemDB
  { psdbPool            :: Pool.Pool,
    psdbRetry           :: RetryPolicy,
    psdbExecutorId      :: Maybe Text,
    psdbApplicationName :: Maybe Text,
    psdbPollingPermits  :: StrictTVar IO Int,
    psdbNotify          :: Registry,
    psdbNotifier        :: Notifier,
    -- | The notifier's flush loop, once 'activatePostgresSystemDB' has
    -- spawned it. Mirrors the oracle's @notifier_task@: 'close' takes it,
    -- waits out the final flush, and only then releases the pool.
    psdbNotifierTask    :: StrictMVar IO (Maybe (Async IO ())),
    psdbLog             :: SomeTracer IO
  }

-- | Builds a handle around a live pool. Mirrors Rust @from_pool@: the pool
-- size travels separately because a hasql pool does not report it, and the
-- schema is checked (only @"dbos"@ is served) rather than rendered.
fromPool :: Pool.Pool -> Word32 -> Settings -> SomeTracer IO -> IO PostgresSystemDB
fromPool pool poolSize settings tracer = do
  when (settings.settingsSchema /= "dbos") $
    throwIO (invalidInput "schema" ("only the dbos schema is served, not " <> settings.settingsSchema))
  permits <- newTVarIO (pollingLimit settings.settingsPollingConcurrency poolSize)
  registry <- newRegistry
  notifier <- notifierNew pool registry settings.settingsNotificationCoalesce tracer
  taskVar <- newMVar Nothing
  pure
    PostgresSystemDB
      { psdbPool = pool,
        psdbRetry = settings.settingsRetry,
        psdbExecutorId = settings.settingsExecutorId,
        psdbApplicationName = settings.settingsApplicationName,
        psdbPollingPermits = permits,
        psdbNotify = registry,
        psdbNotifier = notifier,
        psdbNotifierTask = taskVar,
        psdbLog = tracer
      }

-- | Connects and verifies the schema is at the migration ceiling (114).
-- Verify-only: Haskell never migrates (ADR-0004) and never creates the
-- database; a missing or drifted schema is a 'Backend' 'Permanent' error,
-- not a build step.
acquirePostgresSystemDB :: Config -> SomeTracer IO -> IO PostgresSystemDB
acquirePostgresSystemDB config tracer = do
  pool <-
    Pool.acquire
      ( PoolConfig.settings
          [ PoolConfig.size (fromIntegral config.configMaxConnections),
            PoolConfig.acquisitionTimeout 10,
            PoolConfig.agingTimeout 1800,
            PoolConfig.idlenessTimeout 1800,
            PoolConfig.staticConnectionSettings (Connection.connectionString config.configUrl)
          ]
      )
  env <- fromPool pool config.configMaxConnections config.configSettings tracer
  verifySystemDatabase env >>= \case
    Left err -> Pool.release pool >> throwIO err
    Right () -> pure env

-- | Bracketed handle: acquires, runs, and releases the pool. Mirrors Rust
-- @close@ being what a shutdown waits on — here there are no tasks, so
-- closing is releasing the idle connections.
-- | Enables the notifier and spawns its flush loop. Separate from
-- construction because the two have different owners, exactly as in the
-- oracle: 'acquirePostgresSystemDB' connects, this activates, and 'close'
-- stops. Call once per handle.
activatePostgresSystemDB :: PostgresSystemDB -> IO ()
activatePostgresSystemDB env = do
  enable env.psdbNotifier
  task <- async (run env.psdbNotifier)
  _ <- swapMVar env.psdbNotifierTask (Just task)
  pure ()

-- | Stops the notifier's flush loop and releases the pool. The final flush
-- is a database write, so the loop is stopped *before* the pool closes, not
-- after — the opposite order would turn every queued payload into a logged
-- failure. A handle that was never activated skips the wait.
releasePostgresSystemDB :: PostgresSystemDB -> IO ()
releasePostgresSystemDB env = do
  stop env.psdbNotifier
  stopped <- takeMVar env.psdbNotifierTask
  putMVar env.psdbNotifierTask Nothing
  case stopped of
    Nothing   -> pure ()
    Just task -> wait task
  Pool.release env.psdbPool

withPostgresSystemDB :: Config -> SomeTracer IO -> (PostgresSystemDB -> IO a) -> IO a
withPostgresSystemDB config tracer =
  bracket (acquirePostgresSystemDB config tracer) releasePostgresSystemDB

-- | Reads back the highest applied migration and requires the ceiling this
-- port tracks (114). A higher value means the Rust corpus moved and the
-- pinned queries must be re-verified.
verifySystemDatabase :: PostgresSystemDB -> IO (Either Error ())
verifySystemDatabase env = do
  result <- runSession env "verify" migrationVersionSession
  pure $ case result of
    Left err -> Left err
    Right Nothing ->
      Left (Backend (BackendError "the system database has no migrations applied" Nothing Permanent))
    Right (Just (DbosMigration version))
      | version == migrationCeiling -> Right ()
      | otherwise ->
          Left
            ( Backend
                ( BackendError
                    ("the system database is at migration " <> Text.pack (show version) <> ", expected " <> Text.pack (show migrationCeiling))
                    Nothing
                    Permanent
                )
            )

-- | The migration ceiling this port tracks (ranges 1–47 + 100–114). A
-- higher value means the Rust corpus moved and the pinned queries must be
-- re-verified.
migrationCeiling :: Int64
migrationCeiling = 114

-- | Runs one session through the pool, classifying failures into the shared
-- 'Error' channel and retrying through 'withRetry' under the operation's
-- name — the Haskell shape of Rust's per-method @with_retry@ wrapper.
runSession :: PostgresSystemDB -> Text -> Session a -> IO (Either Error a)
runSession env operation session =
  withRetry env.psdbRetry operation env.psdbLog uuidEntropy $ do
    result <- Pool.use env.psdbPool session
    pure $ case result of
      Left usage  -> Left (classifyUsageError usage)
      Right value -> Right value

-- | The polling cap for a pool of @poolSize@ connections. Mirrors Rust
-- @polling_limit@: @Nothing@ takes half the pool and at least one;
-- @Just 0@ switches the cap off (Haskell's inexhaustible count is
-- 'maxBound', where Rust uses @Semaphore::MAX_PERMITS@).
pollingLimit :: Maybe Word32 -> Word32 -> Int
pollingLimit configured poolSize =
  case configured of
    Just 0  -> maxBound
    Just n  -> fromIntegral n
    Nothing -> max 1 (fromIntegral poolSize `div` 2)

-- | Whether a failure is a primary-key or unique-index collision.
-- @23505 unique_violation@: for streams another writer claimed the offset
-- this one computed — contention, not an error. Mirrors Rust
-- @is_unique_violation@, checked by code.
isUniqueViolation :: Errors.SessionError -> Bool
isUniqueViolation sessionError = sessionSqlState sessionError == Just "23505"

-- | Whether a failure is the destination foreign key rejecting an address
-- that does not exist. @23503 foreign_key_violation@, checked by code
-- rather than message text. Mirrors Rust @is_foreign_key_violation@.
isForeignKeyViolation :: Errors.SessionError -> Bool
isForeignKeyViolation sessionError = sessionSqlState sessionError == Just "23503"

-- | Maps a decoded row to the domain record. Mirrors Rust
-- @workflow_from_row@: the status spelling and the roles JSON are the two
-- fallible spots ('Malformed'); a timeout that cannot be stored reads as
-- absent, exactly as @and_then(duration_from_ms)@; every other @Option@
-- column unwraps to its zero value.
workflowRecordFromRow :: Statements.WorkflowRowRaw -> Either Error WorkflowRecord
workflowRecordFromRow row = do
  status <- case parseWorkflowStatus row.status of
    Left _       -> Left (Malformed ("unknown workflow status \"" <> row.status <> "\""))
    Right parsed -> Right parsed
  roles <- decodeRoles row.authenticated_roles
  pure
    WorkflowRecord
      { workflowRecordId = Types.WorkflowId row.workflow_uuid,
        workflowRecordStatus = status,
        workflowRecordName = row.name,
        workflowRecordClassName = row.class_name,
        workflowRecordConfigName = row.config_name,
        workflowRecordInput = row.inputs,
        workflowRecordOutput = row.output,
        workflowRecordError = row.error,
        workflowRecordSerialization = row.serialization,
        workflowRecordExecutorId = row.executor_id,
        workflowRecordApplicationVersion = row.application_version,
        workflowRecordRecoveryAttempts = fromMaybe 0 row.recovery_attempts,
        workflowRecordQueueName = row.queue_name,
        workflowRecordCreatedAt = timestampFromEpochMs row.created_at,
        workflowRecordUpdatedAt = timestampFromEpochMs row.updated_at,
        workflowRecordStartedAt = timestampFromEpochMs <$> row.started_at_epoch_ms,
        workflowRecordCompletedAt = timestampFromEpochMs <$> row.completed_at,
        workflowRecordForkedFrom = Types.WorkflowId <$> row.forked_from,
        workflowRecordParentWorkflowId = Types.WorkflowId <$> row.parent_workflow_id,
        workflowRecordWasForkedFrom = fromMaybe False row.was_forked_from,
        workflowRecordOwnerXid = row.owner_xid,
        workflowRecordApplicationId = row.application_id,
        workflowRecordAuthenticatedUser = row.authenticated_user,
        workflowRecordAuthenticatedRoles = roles,
        workflowRecordAssumedRole = row.assumed_role,
        workflowRecordRequest = row.request,
        workflowRecordApplicationName = row.application_name,
        workflowRecordDeduplicationId = row.deduplication_id,
        workflowRecordPriority = fromMaybe 0 row.priority,
        workflowRecordQueuePartitionKey = row.queue_partition_key,
        workflowRecordRateLimited = fromMaybe False row.rate_limited,
        workflowRecordScheduleName = row.schedule_name,
        workflowRecordTimeout = durationFromMs =<< row.workflow_timeout_ms,
        workflowRecordDeadline = timestampFromEpochMs <$> row.workflow_deadline_epoch_ms,
        workflowRecordDelayUntil = timestampFromEpochMs <$> row.delay_until_epoch_ms,
        workflowRecordDebounceDeadline = timestampFromEpochMs <$> row.debounce_deadline_epoch_ms,
        workflowRecordIsDebounced = fromMaybe False row.is_debounced,
        workflowRecordAttributes = row.attributes
      }

-- | Decodes the roles column, treating @NULL@ as no roles. A value that is
-- not a JSON array of strings is 'Malformed': another implementation wrote
-- something this one does not understand. Mirrors Rust @decode_roles@.
decodeRoles :: Maybe Text -> Either Error [Text]
decodeRoles Nothing = Right []
decodeRoles (Just json) =
  case eitherDecodeStrict (encodeUtf8 json) :: Either String [Text] of
    Left err    -> Left (Malformed ("authenticated_roles is not a JSON array of strings: " <> Text.pack err))
    Right roles -> Right roles

-- | Resolves a filter into the guards the listing binds. Mirrors
-- @list_workflows@'s decisions: statuses become their stored spellings,
-- prefixes get their wildcards escaped (so a caller's @%@ or @_@ is
-- literal) with @%@ appended, and application scoping is decided here
-- because it depends on the handle too — @Unset@ with no explicit ids means
-- the handle's own application plus the unclaimed rows, while @AnyApplication@ and an
-- id-keyed read see everything. NOTE: @caller@ is not yet recorded as the
-- @DBOS.listWorkflows@ step; that checkpoint lands with 'recordStep'.
listParams :: PostgresSystemDB -> WorkflowFilter -> Statements.WorkflowListParams
listParams env workflowFilter =
  Statements.WorkflowListParams
    { listLoadInput = workflowFilter.workflowFilterLoadInput,
      listLoadOutput = workflowFilter.workflowFilterLoadOutput,
      listWorkflowIds = workflowFilter.workflowFilterWorkflowIds,
      listWorkflowIdPrefixes = escapeLike <$> workflowFilter.workflowFilterWorkflowIdPrefixes,
      listNamedApplications = namedApplications,
      listUnsetApplication = unsetApplication,
      listNames = workflowFilter.workflowFilterNames,
      listClassNames = workflowFilter.workflowFilterClassNames,
      listConfigNames = workflowFilter.workflowFilterConfigNames,
      listStatuses = workflowStatusText <$> workflowFilter.workflowFilterStatus,
      listApplicationVersions = workflowFilter.workflowFilterApplicationVersions,
      listExecutorIds = workflowFilter.workflowFilterExecutorIds,
      listAuthenticatedUsers = workflowFilter.workflowFilterAuthenticatedUsers,
      listQueueNames = workflowFilter.workflowFilterQueueNames,
      listScheduleNames = workflowFilter.workflowFilterScheduleNames,
      listDeduplicationIds = workflowFilter.workflowFilterDeduplicationIds,
      listParentWorkflowIds = workflowFilter.workflowFilterParentWorkflowIds,
      listForkedFrom = workflowFilter.workflowFilterForkedFrom,
      listQueuesOnly = workflowFilter.workflowFilterQueuesOnly,
      listIsFork = workflowFilter.workflowFilterIsFork,
      listHasParent = workflowFilter.workflowFilterHasParent,
      listWasForkedFrom = workflowFilter.workflowFilterWasForkedFrom,
      listIsDebounced = workflowFilter.workflowFilterIsDebounced,
      listCreatedAfter = timestampToEpochMs <$> workflowFilter.workflowFilterCreatedAfter,
      listCreatedBefore = timestampToEpochMs <$> workflowFilter.workflowFilterCreatedBefore,
      listCompletedAfter = timestampToEpochMs <$> workflowFilter.workflowFilterCompletedAfter,
      listCompletedBefore = timestampToEpochMs <$> workflowFilter.workflowFilterCompletedBefore,
      listStartedAfter = timestampToEpochMs <$> workflowFilter.workflowFilterStartedAfter,
      listStartedBefore = timestampToEpochMs <$> workflowFilter.workflowFilterStartedBefore,
      listAttributes = workflowFilter.workflowFilterAttributes,
      listSortDesc = workflowFilter.workflowFilterSortDesc,
      listLimit = workflowFilter.workflowFilterLimit,
      listOffset = workflowFilter.workflowFilterOffset
    }
  where
    -- A workflow id is a global address, so asking for one by id is an
    -- identity read and must not be scoped to the handle's application.
    idKeyed = not (null workflowFilter.workflowFilterWorkflowIds)
    (namedApplications, unsetApplication) =
      case workflowFilter.workflowFilterApplications of
        AnyApplication -> (Nothing, Nothing)
        Named names
          | null names -> (Nothing, Nothing)
          | otherwise -> (Just names, Nothing)
        Unset
          | idKeyed -> (Nothing, Nothing)
          | otherwise -> (Nothing, env.psdbApplicationName)

-- | Appends @%@ and escapes the caller's wildcards, so a prefix is matched
-- literally. Mirrors the oracle's escaping: backslash first, then @%@ and
-- @_@.
escapeLike :: Text -> Text
escapeLike = (<> "%") . Text.concatMap escape
  where
    escape char = case char of
      '\\' -> "\\\\"
      '%'  -> "\\%"
      '_'  -> "\\_"
      _    -> Text.singleton char

-- | Resolves a 'NewWorkflow' and the submission's decisions into the bind
-- list. The owner identity and the clock reading are parameters because the
-- oracle generates both *outside* the retry: an owner that changed between
-- attempts would fail to recognise its own write.
initParams :: PostgresSystemDB -> NewWorkflow -> WorkflowStatus -> Bool -> Bool -> Text -> Timestamp -> Statements.InitWorkflowParams
initParams env new status queued claiming ownerXid now =
  Statements.InitWorkflowParams
    { initParamWorkflowId = new.newWorkflowId,
      initParamStatus = workflowStatusText status,
      initParamName = new.newWorkflowName,
      initParamClassName = new.newWorkflowClassName,
      initParamConfigName = new.newWorkflowConfigName,
      initParamQueueName = new.newWorkflowQueueName,
      initParamDeduplicationId = new.newWorkflowDeduplicationId,
      initParamPriority = new.newWorkflowPriority,
      initParamQueuePartitionKey = new.newWorkflowQueuePartitionKey,
      initParamDelayUntil = timestampToEpochMs <$> (new.newWorkflowDelay >>= addTimeout now),
      initParamAuthenticatedUser = emptyToNone new.newWorkflowAuthenticatedUser,
      initParamAssumedRole = emptyToNone new.newWorkflowAssumedRole,
      initParamAuthenticatedRoles = encodeRoles new.newWorkflowAuthenticatedRoles,
      initParamExecutorId = new.newWorkflowExecutorId,
      initParamApplicationVersion = new.newWorkflowApplicationVersion,
      initParamApplicationId = new.newWorkflowApplicationId,
      initParamCreatedAt = timestampToEpochMs now,
      initParamUpdatedAt = timestampToEpochMs now,
      initParamInitialAttempts = if queued then 0 else 1,
      initParamTimeoutMs = millisOf <$> new.newWorkflowTimeout,
      initParamDeadline = timestampToEpochMs <$> new.newWorkflowDeadline,
      initParamParentWorkflowId = Nothing,
      initParamOwnerXid = ownerXid,
      initParamSerialization = new.newWorkflowSerialization,
      initParamAttributes = new.newWorkflowAttributes,
      initParamScheduleName = new.newWorkflowScheduleName,
      initParamDebounceDeadline = timestampToEpochMs <$> new.newWorkflowDebounceDeadline,
      initParamIsDebounced = new.newWorkflowIsDebounced,
      initParamApplicationName = new.newWorkflowApplicationName <|> env.psdbApplicationName,
      initParamIncrement = if claiming && not queued then 1 else 0,
      initParamClaiming = claiming
    }
  where
    millisOf = fromInteger . durationAsMillis

-- | Whether the failure is the deduplication index, which is only reported
-- when the caller actually supplied a key on a queue — an unexpected
-- violation stays a backend error rather than being mislabelled.
queueDeduplicated :: NewWorkflow -> Error -> Bool
queueDeduplicated new (Backend backend) =
  backend.backendSqlState == Just "23505"
    && new.newWorkflowQueueName /= Nothing
    && new.newWorkflowDeduplicationId /= Nothing
queueDeduplicated _ _ = False

-- | The stored row's identity fields must be the ones this call offers; a
-- mismatch is nondeterminism and rolls the caller's transaction back. Shared
-- by the commit-time check in the caller branch and by 'finishInit'.
initConflict :: NewWorkflow -> Statements.WorkflowInitRaw -> Maybe Error
initConflict new row =
  case [entry | entry@(_, stored, offered) <- conflicts, stored /= offered] of
    (field, stored, offered) : _ ->
      Just
        ConflictingWorkflow
          { workflowId = new.newWorkflowId,
            detail = "existing " <> field <> " is " <> debugOption stored <> ", but " <> debugOption offered <> " was provided"
          }
    [] -> Nothing
  where
    conflicts =
      [ ("function name", row.name, new.newWorkflowName),
        ("class name", row.class_name, new.newWorkflowClassName),
        ("config name", row.config_name, new.newWorkflowConfigName)
      ]

-- | What the upsert's row says about whether this call may run the workflow.
-- Mirrors the oracle's checks, in its order: an unreadable status is
-- malformed, a different function under the same id is a conflict, a
-- differing queue is only a warning (the stored queue wins), a spent
-- recovery budget parks the workflow — a write, so it happens before the
-- error — and another owner's row is recorded but not claimed.
finishInit :: PostgresSystemDB -> NewWorkflow -> Maybe Int64 -> Bool -> Text -> Statements.WorkflowInitRaw -> IO (Either Error WorkflowInitResult)
finishInit env new maxRecoveryAttempts claiming ownerXid row =
  case row.status of
    Nothing -> pure (Left (Malformed ("workflow " <> new.newWorkflowId <> " has a null status")))
    Just raw -> case parseWorkflowStatus raw of
      Left _ -> pure (Left (Malformed ("unknown workflow status \"" <> raw <> "\"")))
      Right status -> do
        case initConflict new row of
          Just err -> pure (Left err)
          Nothing -> do
            when (row.queue_name /= new.newWorkflowQueueName) $
              runTracer
                env.psdbLog
                (SysdbQueueMismatch new.newWorkflowId)
            let ownerDiffers = row.owner_xid /= Just ownerXid
                recoveryAttempts = fromMaybe 0 row.recovery_attempts
                spent = case maxRecoveryAttempts of
                  Just limit -> not (isTerminal status) && recoveryAttempts > limit + 1 && ownerDiffers
                  Nothing    -> False
            if spent
              then do
                _ <- runSession env "park_workflow" (Statements.parkWorkflowSession new.newWorkflowId)
                pure (Left (ErrorMaxRecoveryAttemptsExceeded {workflowId = new.newWorkflowId, limit = fromMaybe 0 maxRecoveryAttempts}))
              else
                pure
                  ( Right
                      WorkflowInitResult
                        { initResultStatus = status,
                          initResultRecoveryAttempts = recoveryAttempts,
                          initResultDeadline = timestampFromEpochMs <$> row.workflow_deadline_epoch_ms,
                          initResultSerialization = row.serialization,
                          initResultShouldExecute = not (ownerDiffers && not claiming && row.owner_xid /= Nothing)
                        }
                  )


-- | Maps the row the step check returned: no status is malformed, a
-- cancelled workflow is reported, an absent step is 'Nothing', a renamed
-- step is unexpected, and anything else is the recorded step. Shared by
-- every check the step and event paths make.
stepCheckToRecord :: WorkflowId -> Int -> Text -> Statements.StepCheckRaw -> Either Error (Maybe StepRecord)
stepCheckToRecord wid stepId stepName row = case row.stepCheckStatus of
  Nothing -> Left (Malformed ("workflow " <> widText <> " has a null status"))
  Just statusText
    | statusText == "CANCELLED" -> Left (WorkflowCancelled {workflowId = widText})
    | otherwise -> case row.stepCheckStepId of
        Nothing -> Right Nothing
        Just recordedStepId -> case row.stepCheckStepName of
          Just recordedName
            | recordedName /= stepName ->
                Left
                  UnexpectedStep
                    { workflowId = widText,
                      stepId = recordedStepId,
                      expected = stepName,
                      recorded = recordedName
                    }
          _ ->
            Right
              ( Just
                  StepRecord
                    { stepRecordWorkflowId = wid,
                      stepRecordStepId = recordedStepId,
                      stepRecordStepName = fromMaybe "" row.stepCheckStepName,
                      stepRecordOutput = row.stepCheckOutput,
                      stepRecordError = row.stepCheckError,
                      stepRecordChildWorkflowId = Types.WorkflowId <$> row.stepCheckChildWorkflowId,
                      stepRecordSerialization = row.stepCheckSerialization,
                      stepRecordStartedAt = timestampFromEpochMs <$> row.stepCheckStartedAt,
                      stepRecordCompletedAt = timestampFromEpochMs <$> row.stepCheckCompletedAt
                    }
              )
  where
    widText = case wid of Types.WorkflowId text -> text

-- | The encoded value a recorded step holds, for a replay that adopts it.
-- A step with no output is a recorded absence — a timeout — and stays
-- 'Nothing' rather than becoming an empty value.
stepEncodedValue :: StepRecord -> Maybe EncodedValue
stepEncodedValue step =
  (\value -> EncodedValue value step.stepRecordSerialization) <$> step.stepRecordOutput

-- | Which of the two things a checkpointed sleep is. A durable sleep is
-- stamped complete at its wake time, so its duration is the sleep; a
-- deadline is stamped now, because the caller registering it usually
-- returns long before it. Mirrors Rust @SleepKind@.
data SleepKind = DurableSleep | DeadlineSleep

-- | The one checkpointed sleep, against whichever kind the caller needs.
-- Composed from the step methods rather than a transaction, matching the
-- oracle: the step record *is* the write, and the check-then-record race is
-- caught by the insert's conflict handling. Mirrors @checkpoint_sleep@.
checkpointSleep :: PostgresSystemDB -> SleepKind -> WorkflowId -> Int -> Duration -> IO (Either Error Timestamp)
checkpointSleep env kind wid stepId duration = do
  startedAt <- timestampNow
  case addTimeout startedAt duration of
    Nothing -> pure (Left (invalidInput "duration" "does not resolve to a representable wake time"))
    Just wakeAt -> do
      existing <- checkStep env wid stepId sleepStepName
      case existing of
        Left err -> pure (Left err)
        Right (Just step) -> pure (decodeWakeTime wid stepId step.stepRecordOutput)
        Right Nothing -> do
          let completedAt = case kind of
                DurableSleep  -> wakeAt
                DeadlineSleep -> startedAt
          recorded <-
            recordStep env
                  wid
                  stepId
                  sleepStepName
                  (OutcomeOutput (Just (Text.pack (show (timestampToEpochMs wakeAt)))))
                  (Just portableJson)
                  (Just (StepTiming startedAt completedAt))

          case recorded of
            Right () -> pure (Right wakeAt)
            Left (StepAlreadyRecorded {}) -> do
              adopted <- checkStep env wid stepId sleepStepName
              pure $ case adopted of
                Left err          -> Left err
                Right (Just step) -> decodeWakeTime wid stepId step.stepRecordOutput
                Right Nothing     -> Left (Malformed "sleep reported as recorded but cannot be read back")
            Left err -> pure (Left err)

-- | The wait's re-read cadence with nothing pushing: one second, the short
-- interval every implementation uses without a listener (ADR-0010).
getEventPollInterval :: Duration
getEventPollInterval = secondsDuration 1

-- | Looks for an event until it is published or the deadline passes. The
-- loop is what delivers; a wakeup only ever shortens the interval, which is
-- why the subscription is taken before the first look. Absence at the
-- deadline is 'Nothing', never an error, and a deadline already past answers
-- at once. Mirrors the oracle's subscribe-then-look-wait-look loop.
pollEvent :: PostgresSystemDB -> Text -> Text -> Timestamp -> IO (Either Error (Maybe EncodedValue))
pollEvent env workflowId key deadline =
  bracket
    (subscribe env.psdbNotify (eventKey workflowId key))
    unsubscribe
    go
  where
    go subscription = do
      found <- runPolling env "get_event" (Statements.eventValueSession workflowId key)
      case found of
        Left err -> pure (Left err)
        Right (Just raw) -> pure (Right (Just (EncodedValue raw.eventRawValue raw.eventRawSerialization)))
        Right Nothing -> do
          now <- timestampNow
          let remaining = fromMaybe (Duration 0) (durationSince deadline now)
          if remaining <= Duration 0
            then pure (Right Nothing)
            else do
              waitForWakeup subscription (min remaining getEventPollInterval)
              go subscription

-- | Runs a transaction through the pool: no retry of its own (the outer
-- 'withRetry' owns the policy), read-committed, and classified like every
-- other database failure. Mirrors where the oracle's transactions sit
-- relative to its retry wrapper.
runTransaction :: PostgresSystemDB -> Text -> Tx.Transaction a -> IO (Either Error a)
runTransaction env = runTransactionAt env TxSessions.ReadCommitted

-- | A transaction at an explicit isolation level: the claim sweeps need
-- repeatable read (or serializable when a queue-wide budget meets a
-- partition key), the rest read committed. Mirrors where the oracle sets
-- @SET TRANSACTION ISOLATION LEVEL@.
runTransactionAt :: PostgresSystemDB -> TxSessions.IsolationLevel -> Text -> Tx.Transaction a -> IO (Either Error a)
runTransactionAt env isolation operation transaction =
  withRetry env.psdbRetry operation env.psdbLog uuidEntropy $ do
    result <- Pool.use env.psdbPool (TxSessions.transactionNoRetry isolation TxSessions.Write transaction)
    pure $ case result of
      Left usage  -> Left (classifyUsageError usage)
      Right value -> Right value





-- | The values @upsert_queue@ writes: the queue's limits, the coalescing
-- column set, and the owner the caller or the handle resolved to.
queueInsertParams :: NewQueue -> Int64 -> Maybe Text -> Statements.QueueInsertParams
queueInsertParams queue updatedAt owner =
  Statements.QueueInsertParams
    { queueInsertName = queue.newQueueName,
      queueInsertConcurrency = queue.newQueueConcurrency,
      queueInsertWorkerConcurrency = queue.newQueueWorkerConcurrency,
      queueInsertRateLimitMax = (.rateLimitLimit) <$> queue.newQueueRateLimit,
      queueInsertRateLimitPeriodSec = (durationSeconds . (.rateLimitPeriod)) <$> queue.newQueueRateLimit,
      queueInsertPriorityEnabled = queue.newQueuePriorityEnabled,
      queueInsertPartitionQueue = queue.newQueuePartitionQueue,
      queueInsertPartitionConcurrency = queue.newQueuePartitionConcurrency,
      queueInsertPartitionWorkerConcurrency = queue.newQueuePartitionWorkerConcurrency,
      queueInsertPartitionRateLimitMax = (.rateLimitLimit) <$> queue.newQueuePartitionRateLimit,
      queueInsertPartitionRateLimitPeriodSec = (durationSeconds . (.rateLimitPeriod)) <$> queue.newQueuePartitionRateLimit,
      queueInsertPollingIntervalSec = durationSeconds queue.newQueuePollingInterval,
      queueInsertUpdatedAt = updatedAt,
      queueInsertApplicationName = owner
    }

-- | A sweep needs partition concurrency 1 and nothing else; the partition
-- worker bound is deliberately not checked. Mirrors the oracle's validation.
validateSweepLimits :: ResolvedLimits -> Either Error ()
validateSweepLimits limits
  | limits.resolvedPartitionConcurrency /= Just 1
      || isJust limits.resolvedConcurrency
      || isJust limits.resolvedRateLimit
      || isJust limits.resolvedPartitionRateLimit =
      Left (invalidInput "queue" "a partitioned sweep needs partition concurrency 1 and no other limit")
  | otherwise = Right ()

-- | The values a bounce writes.
debounceBounceParams :: DebounceRequest -> Maybe Text -> Statements.DebounceBounceParams
debounceBounceParams request app =
  Statements.DebounceBounceParams
    { debounceBounceWorkflowName = request.debounceRequestWorkflowName,
      debounceBounceQueueName = request.debounceRequestQueueName,
      debounceBounceDeduplicationId = request.debounceRequestDeduplicationId,
      debounceBounceDelayUntil = timestampToEpochMs request.debounceRequestDelayUntil,
      debounceBounceSerialization = request.debounceRequestSerialization,
      debounceBounceApplicationName = app,
      debounceBounceClassName = request.debounceRequestClassName,
      debounceBounceConfigName = request.debounceRequestConfigName
    }

-- | The holder row as the domain value.
debounceHeld :: Statements.DebounceHolderRaw -> Debounce
debounceHeld raw =
  DebounceHeld
    DebounceHolder
      { debounceHolderWorkflowId = raw.debounceHolderWorkflowId,
        debounceHolderIsDebounced = raw.debounceHolderIsDebounced,
        debounceHolderWorkflowName = raw.debounceHolderWorkflowName,
        debounceHolderClassName = raw.debounceHolderClassName,
        debounceHolderConfigName = raw.debounceHolderConfigName,
        debounceHolderApplicationName = raw.debounceHolderApplicationName
      }

-- | The stored output of a debounce step as the value it records. A missing
-- output, or one this build cannot read, is malformed — a step that ran has
-- an answer. Mirrors the oracle's @replayed_output@.
replayedDebounce :: WorkflowId -> Int -> Statements.StepCheckRaw -> Either Error Debounce
replayedDebounce (Types.WorkflowId widText) stepId raw = case raw.stepCheckOutput of
  Nothing ->
    Left
      ( Malformed
          ( "workflow " <> widText <> " step " <> Text.pack (show stepId)
              <> " ("
              <> debounceStepName
              <> ") has no recorded output"
          )
      )
  Just output -> case eitherDecodeStrict (encodeUtf8 output) of
    Left err ->
      Left
        ( Malformed
            ( "workflow " <> widText <> " step " <> Text.pack (show stepId)
                <> " ("
                <> debounceStepName
                <> ") has an output this build cannot read: "
                <> Text.pack err
            )
        )
    Right value -> Right value

-- | A duration as the milliseconds a rate-limit window binds, saturating
-- where the oracle saturates. Mirrors @i64::try_from(period.as_millis())@.
periodMillis :: Duration -> Int64
periodMillis duration = fromInteger (min (toInteger (maxBound :: Int64)) (durationAsMillis duration))

-- | The values an update writes: the merged row's every updatable column.
queueUpdateParams :: Text -> QueueRecord -> Int64 -> Statements.QueueUpdateParams
queueUpdateParams name record updatedAt =
  Statements.QueueUpdateParams
    { queueUpdateName = name,
      queueUpdateConcurrency = record.queueRecordConcurrency,
      queueUpdateWorkerConcurrency = record.queueRecordWorkerConcurrency,
      queueUpdateRateLimitMax = (.rateLimitLimit) <$> record.queueRecordRateLimit,
      queueUpdateRateLimitPeriodSec = (durationSeconds . (.rateLimitPeriod)) <$> record.queueRecordRateLimit,
      queueUpdatePriorityEnabled = record.queueRecordPriorityEnabled,
      queueUpdatePartitionQueue = record.queueRecordPartitionQueue,
      queueUpdatePartitionConcurrency = record.queueRecordPartitionConcurrency,
      queueUpdatePartitionWorkerConcurrency = record.queueRecordPartitionWorkerConcurrency,
      queueUpdatePartitionRateLimitMax = (.rateLimitLimit) <$> record.queueRecordPartitionRateLimit,
      queueUpdatePartitionRateLimitPeriodSec = (durationSeconds . (.rateLimitPeriod)) <$> record.queueRecordPartitionRateLimit,
      queueUpdatePollingIntervalSec = durationSeconds record.queueRecordPollingInterval,
      queueUpdateUpdatedAt = updatedAt
    }

-- | A stored notification row as the domain record. Total, like the
-- oracle's map: the row carries every field the record needs.
notificationRecordFromRaw :: Statements.NotificationRecordRaw -> NotificationRecord
notificationRecordFromRaw raw =
  NotificationRecord
    { notificationRecordMessageUuid = raw.notificationRawMessageUuid,
      notificationRecordTopic = raw.notificationRawTopic,
      notificationRecordMessage = raw.notificationRawMessage,
      notificationRecordSerialization = raw.notificationRawSerialization,
      notificationRecordCreatedAt = timestampFromEpochMs raw.notificationRawCreatedAt,
      notificationRecordConsumed = raw.notificationRawConsumed
    }

-- | A stored event row as the domain record. Total.
eventRecordFromRaw :: Statements.EventRecordRaw -> EventRecord
eventRecordFromRaw raw =
  EventRecord
    { eventKey = raw.eventRawKey,
      eventValue = raw.eventRawValue,
      eventSerialization = raw.eventRawSerialization
    }

-- | A stored queue row as the domain record. An unpaired rate-limit column
-- reads as no limit, deliberately, not an error; a null or non-duration
-- polling interval is 'Malformed'. Mirrors the oracle's @queue_from_row@.
queueRecordFromRow :: Statements.QueueRowRaw -> Either Error QueueRecord
queueRecordFromRow row = do
  rateLimit <- pairedRateLimit row.queueRowRateLimitMax row.queueRowRateLimitPeriodSec "rate_limit_period_sec"
  partitionRateLimit <- pairedRateLimit row.queueRowPartitionRateLimitMax row.queueRowPartitionRateLimitPeriodSec "partition_rate_limit_period_sec"
  pollingInterval <- case row.queueRowPollingIntervalSec of
    Nothing -> Left (Malformed "polling_interval_sec is null")
    Just secs ->
      maybe
        (Left (Malformed ("polling_interval_sec is not a duration: " <> Text.pack (show secs))))
        Right
        (durationFromSecs secs)
  pure
    QueueRecord
      { queueRecordName = row.queueRowName,
        queueRecordConcurrency = row.queueRowConcurrency,
        queueRecordWorkerConcurrency = row.queueRowWorkerConcurrency,
        queueRecordRateLimit = rateLimit,
        queueRecordPriorityEnabled = row.queueRowPriorityEnabled,
        queueRecordPartitionQueue = row.queueRowPartitionQueue,
        queueRecordPartitionConcurrency = row.queueRowPartitionConcurrency,
        queueRecordPartitionWorkerConcurrency = row.queueRowPartitionWorkerConcurrency,
        queueRecordPartitionRateLimit = partitionRateLimit,
        queueRecordPollingInterval = pollingInterval,
        queueRecordApplicationName = row.queueRowApplicationName
      }

-- | A rate limit read from its two columns: a pair is a limit only when both
-- are present, and a present-but-invalid period is malformed.
pairedRateLimit :: Maybe Int -> Maybe Double -> Text -> Either Error (Maybe RateLimit)
pairedRateLimit (Just limit) (Just secs) column =
  case durationFromSecs secs of
    Nothing     -> Left (Malformed (column <> " is not a duration: " <> Text.pack (show secs)))
    Just period -> Right (Just (RateLimit {rateLimitLimit = limit, rateLimitPeriod = period}))
pairedRateLimit _ _ _ = Right Nothing

-- | A duration as the fractional seconds the schema stores.
durationSeconds :: Duration -> Double
durationSeconds (Duration diffTime) = realToFrac diffTime

-- | The caller's bounce as one commit: a recorded step is the answer and
-- the work never re-runs; otherwise the bounce runs and its output is
-- recorded. Mirrors the oracle's @run_transactional_step@ for the debounce
-- lane. A failure before anything is written commits nothing, so the replay
-- runs the work again.
debounceCallerTx :: Maybe Text -> WorkflowId -> Text -> Int -> Timestamp -> Int64 -> DebounceRequest -> Tx.Transaction (Either Error Debounce)
debounceCallerTx app callerWid callerText callerStep startedAt completedAt request = do
  checked <- Tx.statement (callerText, callerStep) Statements.checkStepStatement
  case fmap (.stepCheckStepId) checked of
    Just (Just _) -> case checked of
      Just raw -> case stepCheckToRecord callerWid callerStep debounceStepName raw of
        Left err -> pure (Left err)
        Right _  -> pure (replayedDebounce callerWid callerStep raw)
      Nothing -> pure (Left (Malformed "unreachable: recorded step vanished"))
    _ -> do
      bounced <- Tx.statement (debounceBounceParams request app) Statements.debounceBounceStatement
      value <- case bounced of
        Just workflowId -> do
          -- The bounce replaces the held workflow's inputs, which readers
          -- find in @workflow_input@ before the legacy column.
          void $ Tx.statement (workflowId, request.debounceRequestInputs) Statements.debounceInputStatement
          pure (Right (Debounced {debounceWorkflowId = workflowId}))
        Nothing -> do
          holder <- Tx.statement (request.debounceRequestQueueName, request.debounceRequestDeduplicationId) Statements.debounceHolderStatement
          pure (Right (maybe DebounceUnheld debounceHeld holder))
      case value of
        Left err -> pure (Left err)
        Right debounce -> do
          _ <-
            Tx.statement
              ()
              ( Statements.recordStepStatement
                  Statements.RecordStepParams
                  { recordStepWorkflowId = callerText,
                    recordStepStepId = callerStep,
                    recordStepStepName = debounceStepName,
                    recordStepOutput = Just (decodeUtf8 (LBS.toStrict (Aeson.encode debounce))),
                    recordStepError = Nothing,
                    recordStepSerialization = Just portableJson,
                    recordStepStartedAt = Just (timestampToEpochMs startedAt),
                    recordStepCompletedAt = Just completedAt,
                    recordStepApplicationName = app,
                    recordStepChildWorkflowId = Nothing
                  }
              )
          pure (Right debounce)

-- | A workflow id as the text the statements bind. A top-level helper
-- rather than a local one so the transaction bodies below can use it without
-- a pattern binding in layout-sensitive positions.
unwrapWorkflowId :: WorkflowId -> Text
unwrapWorkflowId (Types.WorkflowId widText) = widText

-- | The stored status spelling is uppercase; the step output uses serde's
-- variant names, as the debounce output does.
instance Aeson.ToJSON ScheduleStatus where
  toJSON Active = Aeson.String "Active"
  toJSON Paused = Aeson.String "Paused"

instance Aeson.FromJSON ScheduleStatus where
  parseJSON (Aeson.String "Active") = pure Active
  parseJSON (Aeson.String "Paused") = pure Paused
  parseJSON _                       = fail "ScheduleStatus must be Active or Paused"

-- | A schedule row as step-output JSON. Field names follow the record, so a
-- replay in any SDK reads what this one wrote.
instance Aeson.ToJSON ScheduleRecord where
  toJSON record =
    Aeson.object
      [ "schedule_id" Aeson..= record.scheduleRecordId,
        "schedule_name" Aeson..= record.scheduleRecordName,
        "workflow_name" Aeson..= record.scheduleRecordWorkflowName,
        "workflow_class_name" Aeson..= record.scheduleRecordWorkflowClassName,
        "expression" Aeson..= record.scheduleRecordExpression,
        "status" Aeson..= record.scheduleRecordStatus,
        "context" Aeson..= record.scheduleRecordContext,
        "last_fired_at" Aeson..= record.scheduleRecordLastFiredAt,
        "automatic_backfill" Aeson..= record.scheduleRecordAutomaticBackfill,
        "cron_timezone" Aeson..= record.scheduleRecordCronTimezone,
        "queue_name" Aeson..= record.scheduleRecordQueueName,
        "application_name" Aeson..= record.scheduleRecordApplicationName
      ]

instance Aeson.FromJSON ScheduleRecord where
  parseJSON = Aeson.withObject "ScheduleRecord" $ \o ->
    ScheduleRecord
      <$> o Aeson..: "schedule_id"
      <*> o Aeson..: "schedule_name"
      <*> o Aeson..: "workflow_name"
      <*> o Aeson..: "workflow_class_name"
      <*> o Aeson..: "expression"
      <*> o Aeson..: "status"
      <*> o Aeson..: "context"
      <*> o Aeson..: "last_fired_at"
      <*> o Aeson..: "automatic_backfill"
      <*> o Aeson..: "cron_timezone"
      <*> o Aeson..: "queue_name"
      <*> o Aeson..: "application_name"

-- | Any recorded step output as the value it holds. A missing output, or one
-- this build cannot read, is malformed — a step that ran has an answer.
-- Mirrors the oracle's @replayed_output@; 'replayedDebounce' is the same
-- function at the debounce lane's type.
replayStepOutput :: Aeson.FromJSON a
                 => WorkflowId -> Int -> Text -> Statements.StepCheckRaw -> Either Error a
replayStepOutput wid stepId stepName raw = case raw.stepCheckOutput of
  Nothing ->
    Left
      ( Malformed
          ( "workflow " <> unwrapWorkflowId wid <> " step " <> Text.pack (show stepId)
              <> " ("
              <> stepName
              <> ") has no recorded output"
          )
      )
  Just output -> case eitherDecodeStrict (encodeUtf8 output) of
    Left err ->
      Left
        ( Malformed
            ( "workflow " <> unwrapWorkflowId wid <> " step " <> Text.pack (show stepId)
                <> " ("
                <> stepName
                <> ") has an output this build cannot read: "
                <> Text.pack err
            )
        )
    Right value -> Right value

-- | One caller's step around any transaction body: a recorded step is the
-- answer and the work never re-runs; otherwise the work runs and its JSON
-- output is recorded. Mirrors the oracle's @run_transactional_step@. A
-- failure before anything is written commits nothing, so the replay runs
-- the work again.
runCallerStep :: (Aeson.ToJSON a, Aeson.FromJSON a)
              => PostgresSystemDB -> Text -> WorkflowId -> Int -> Timestamp -> Tx.Transaction (Either Error a) -> IO (Either Error a)
runCallerStep env stepName callerWid callerStep startedAt work = do
  completedAt <- timestampToEpochMs <$> timestampNow
  let callerText = unwrapWorkflowId callerWid
  result <- runTransaction env stepName $ do
    checked <- Tx.statement (callerText, callerStep) Statements.checkStepStatement
    case fmap (.stepCheckStepId) checked of
      Just (Just _) -> case checked of
        Just raw -> case stepCheckToRecord callerWid callerStep stepName raw of
          Left err -> pure (Left err)
          Right _  -> pure (replayStepOutput callerWid callerStep stepName raw)
        Nothing -> pure (Left (Malformed "unreachable: recorded step vanished"))
      _ -> do
        outcome <- work
        case outcome of
          Left err -> pure (Left err)
          Right value -> do
            _ <-
              Tx.statement
                ()
                ( Statements.recordStepStatement
                    Statements.RecordStepParams
                      { recordStepWorkflowId = callerText,
                        recordStepStepId = callerStep,
                        recordStepStepName = stepName,
                        recordStepOutput = Just (decodeUtf8 (LBS.toStrict (Aeson.encode value))),
                        recordStepError = Nothing,
                        recordStepSerialization = Just portableJson,
                        recordStepStartedAt = Just (timestampToEpochMs startedAt),
                        recordStepCompletedAt = Just completedAt,
                        recordStepApplicationName = env.psdbApplicationName,
                        recordStepChildWorkflowId = Nothing
                      }
                )
            pure (Right value)
  pure (join result)

-- | The application a row keyed by name should end up owned by, having read
-- which holds it now: a nameless writer leaves an existing owner intact,
-- one whose name already matches proceeds, and a different name is
-- 'RegisteredByAnother'. Mirrors Rust @resolve_owning_application@.
resolveOwner :: Text -> Text -> Maybe Text -> Maybe (Maybe Text) -> Either Error (Maybe Text)
resolveOwner kind name claimant holder =
  case holder of
    Nothing -> Right claimant
    Just Nothing -> Right claimant
    Just (Just owner) -> case claimant of
      Nothing -> Right (Just owner)
      Just claimantName
        | claimantName == owner -> Right (Just owner)
        | otherwise ->
            Left
              RegisteredByAnother
                { kind = kind,
                  name = name,
                  holder = owner,
                  claimant = Just claimantName
                }

-- Schedules: the cron registry (postgres.rs schedule methods)

-- | A stored schedule row as the domain record. The status spelling and the
-- ISO-8601 last firing are the two fallible spots; a value this build does
-- not know is 'Malformed' rather than guessed. Mirrors the oracle's
-- @schedule_from_row@.
scheduleRecordFromRow :: Statements.ScheduleRowRaw -> Either Error ScheduleRecord
scheduleRecordFromRow row = do
  status <- case parseScheduleStatus row.scheduleRowStatus of
    Nothing ->
      Left
        ( Malformed
            ("schedule status " <> Text.pack (show row.scheduleRowStatus) <> " is not one this build knows")
        )
    Just parsed -> Right parsed
  lastFiredAt <- traverse parseFiredAt row.scheduleRowLastFiredAt
  pure
    ScheduleRecord
      { scheduleRecordId = row.scheduleRowId,
        scheduleRecordName = row.scheduleRowName,
        scheduleRecordWorkflowName = row.scheduleRowWorkflowName,
        scheduleRecordWorkflowClassName = row.scheduleRowWorkflowClassName,
        scheduleRecordExpression = row.scheduleRowExpression,
        scheduleRecordStatus = status,
        scheduleRecordContext = row.scheduleRowContext,
        scheduleRecordLastFiredAt = lastFiredAt,
        scheduleRecordAutomaticBackfill = row.scheduleRowAutomaticBackfill,
        scheduleRecordCronTimezone = row.scheduleRowCronTimezone,
        scheduleRecordQueueName = row.scheduleRowQueueName,
        scheduleRecordApplicationName = row.scheduleRowApplicationName
      }
  where
    parseFiredAt stored = case timestampFromIso8601 stored of
      Nothing ->
        Left
          ( Malformed
              ("last_fired_at " <> Text.pack (show stored) <> " is not an ISO-8601 instant")
          )
      Just instant -> Right instant

-- | The values a schedule insert or upsert writes. The id is generated once
-- by the caller (outside any retry, so an attempt cannot insert a second
-- row); the owner is the resolve's answer.
scheduleInsertParams :: NewSchedule -> Text -> Maybe Text -> Statements.ScheduleInsertParams
scheduleInsertParams new scheduleId owner =
  Statements.ScheduleInsertParams
    { scheduleInsertId = scheduleId,
      scheduleInsertName = new.newScheduleName,
      scheduleInsertWorkflowName = new.newScheduleWorkflowName,
      scheduleInsertWorkflowClassName = new.newScheduleWorkflowClassName,
      scheduleInsertExpression = new.newScheduleExpression,
      scheduleInsertStatus = scheduleStatusText new.newScheduleStatus,
      scheduleInsertContext = new.newScheduleContext,
      scheduleInsertLastFiredAt = timestampToIso8601 <$> new.newScheduleLastFiredAt,
      scheduleInsertAutomaticBackfill = new.newScheduleAutomaticBackfill,
      scheduleInsertCronTimezone = new.newScheduleCronTimezone,
      scheduleInsertQueueName = new.newScheduleQueueName,
      scheduleInsertApplicationName = owner
    }

-- | The values a partial update writes: each column with whether the update
-- names it. A @Leave@ keeps the stored value; the placeholder value is
-- ignored by the statement's @CASE WHEN@ guard.
scheduleUpdateParams :: Text -> ScheduleUpdate -> Statements.ScheduleUpdateParams
scheduleUpdateParams name update =
  Statements.ScheduleUpdateParams
    { updateScheduleName = name,
      updateScheduleSetExpression = named update.scheduleUpdateExpression,
      updateScheduleExpression = fromMaybe "" (changeSet update.scheduleUpdateExpression),
      updateScheduleSetContext = named update.scheduleUpdateContext,
      updateScheduleContext = fromMaybe "" (changeSet update.scheduleUpdateContext),
      updateScheduleSetAutomaticBackfill = named update.scheduleUpdateAutomaticBackfill,
      updateScheduleAutomaticBackfill = fromMaybe False (changeSet update.scheduleUpdateAutomaticBackfill),
      updateScheduleSetCronTimezone = named update.scheduleUpdateCronTimezone,
      updateScheduleCronTimezone = join (changeSet update.scheduleUpdateCronTimezone),
      updateScheduleSetQueueName = named update.scheduleUpdateQueueName,
      updateScheduleQueueName = join (changeSet update.scheduleUpdateQueueName)
    }
  where
    named = not . changeIsLeave

-- | The listing's narrowings with the application scope already resolved:
-- @Unset@ means the handle's own application plus the unclaimed, @AnyApplication@ and
-- an empty @Named@ narrow nothing, and a @Named@ list keeps the unclaimed.
-- A prefix arrives escaped with @%@ appended, so a caller's wildcard matches
-- itself. Mirrors @list_schedules@.
scheduleListParams :: PostgresSystemDB -> ScheduleFilter -> Statements.ScheduleListParams
scheduleListParams env scheduleFilter =
  Statements.ScheduleListParams
    { listScheduleStatuses = scheduleStatusText <$> scheduleFilter.scheduleFilterStatuses,
      listScheduleWorkflowNames = scheduleFilter.scheduleFilterWorkflowNames,
      listScheduleNamePrefixes = escapeLike <$> scheduleFilter.scheduleFilterNamePrefixes,
      listScheduleApplications = case scheduleFilter.scheduleFilterApplications of
        AnyApplication -> Nothing
        Named []       -> Nothing
        Named names    -> Just names
        Unset          -> (: []) <$> env.psdbApplicationName
    }

-- | Whether a create's failure is a unique-index collision, and which index
-- it was: the primary key names the id, any other index the name. Mirrors
-- the oracle's constraint check, read from the server message.
scheduleCollision :: Text -> Text -> Error -> Error
scheduleCollision scheduleId scheduleName err@(Backend backend)
  | backend.backendSqlState == Just "23505" =
      if "_pkey" `Text.isInfixOf` backend.backendMessage
        then AlreadyRegistered {kind = "Schedule id", name = scheduleId}
        else AlreadyRegistered {kind = "Schedule", name = scheduleName}
  | otherwise = err
scheduleCollision _ _ err = err

-- | A write addressed to a schedule name nothing holds. Mirrors the
-- oracle's @Error::NotRegistered { kind: "Schedule" }@.
missingSchedule :: Text -> Either Error a
missingSchedule name = Left (NotRegistered {kind = "Schedule", name = name})

-- | The upsert of one schedule: resolve the owner, write, and read the
-- owner back — the conflict clause's @COALESCE@ declines to claim without
-- saying why. Shared with 'applySchedules', which runs many under one
-- commit. Mirrors @upsert_schedule_on@.
upsertScheduleOn :: PostgresSystemDB -> NewSchedule -> Text -> Tx.Transaction (Either Error ())
upsertScheduleOn env new scheduleId = do
  let claimant = new.newScheduleApplicationName <|> env.psdbApplicationName
  holder <- Tx.statement new.newScheduleName Statements.scheduleOwnerStatement
  case resolveOwner "Schedule" new.newScheduleName claimant holder of
    Left err -> pure (Left err)
    Right owner -> do
      Tx.statement (scheduleInsertParams new scheduleId owner) Statements.scheduleUpsertStatement
      after <- Tx.statement new.newScheduleName Statements.scheduleOwnerStatement
      pure (() <$ resolveOwner "Schedule" new.newScheduleName claimant after)

-- | Applies a whole declaration as one commit: each schedule upserts, and a
-- collision condemns the transaction so nothing written earlier commits.
-- Mirrors @apply_schedules@.
applySchedulesTx :: PostgresSystemDB -> [(NewSchedule, Text)] -> Tx.Transaction (Either Error ())
applySchedulesTx env = go
  where
    go [] = pure (Right ())
    go ((new, scheduleId) : rest) = do
      outcome <- upsertScheduleOn env new scheduleId
      case outcome of
        Left err -> do
          Tx.condemn
          pure (Left err)
        Right () -> go rest

-- | A version row as the domain record.
versionInfoFromRow :: Statements.VersionRowRaw -> VersionInfo
versionInfoFromRow row =
  VersionInfo
    { versionInfoApplicationName = row.versionRowApplicationName,
      versionInfoId = row.versionRowId,
      versionInfoName = row.versionRowName,
      versionInfoTimestamp = timestampFromEpochMs row.versionRowTimestamp,
      versionInfoCreatedAt = timestampFromEpochMs row.versionRowCreatedAt
    }

-- | Renames a table's remaining rows, in batches when asked: the in-flight
-- rows are already renamed by the transaction, so the loop walks what is
-- left. Mirrors @rename_application_in_batches@.
renameInBatches :: PostgresSystemDB -> Text -> RenameFrom -> Text -> RenameBatching -> IO (Either Error Int64)
renameInBatches env table source newName batching =
  case batching of
    Batched size -> loop Nothing 0
      where
        loop watermark total = do
          bound <-
            runSession
              env
              "rename_row_batch"
              (Session.statement (renamedFrom, watermark) (Statements.renameBatchBoundStatement table source size))
          case bound of
            Left err -> pure (Left err)
            -- Fewer rows remain than one batch: the rest move in one
            -- statement, as the oracle's @update_rest@ does.
            Right Nothing -> do
              rest <-
                runSession
                  env
                  "rename_row_batch"
                  (Session.statement (newName, renamedFrom) (Statements.renameRowsStatement table source ""))
              pure ((total +) <$> rest)
            Right (Just upper) -> do
              moved <-
                runSession
                  env
                  "rename_row_batch"
                  (Session.statement (newName, watermark, upper) (Statements.renameBatchRangeStatement table source))
              case moved of
                Left err   -> pure (Left err)
                Right rows -> loop (Just upper) (total + rows)
    _ -> runSession env "rename_rows" (Session.statement (newName, renamedFrom) (Statements.renameRowsStatement table source ""))
  where
    renamedFrom = renameFromApplication source

-- | Creates the fork rows and copies the source's history forward, as one
-- commit. The timeout must fit in milliseconds as a 64-bit integer, which
-- is a caller error rather than a truncation.
runFork :: PostgresSystemDB -> [Text] -> [Text] -> [Int] -> ForkOptions -> IO (Either Error [WorkflowId])
runFork env sources forkedIds steps options =
  case forkTimeoutMs options.forkOptionsTimeout of
    Left err -> pure (Left err)
    Right timeoutMs -> do
      let queue = fromMaybe (unwrapQueueName internalQueueName) options.forkOptionsQueueName
          params =
            Statements.ForkParams
              { forkSources = sources,
                forkIds = forkedIds,
                forkSteps = steps,
                forkApplicationVersion = options.forkOptionsApplicationVersion,
                forkQueue = queue,
                forkPartitionKey = options.forkOptionsQueuePartitionKey,
                forkTimeoutMs = timeoutMs,
                forkApplicationName = env.psdbApplicationName,
                forkReplaceFrom = map fst options.forkOptionsReplacementChildren,
                forkReplaceTo = map snd options.forkOptionsReplacementChildren,
                forkCopiesAnything = any (> 0) steps
              }
      result <- runTransaction env "fork_workflows" (Statements.forkTx params)
      pure $ case result of
        Left err             -> Left err
        Right (Just missing) -> Left (NonExistentWorkflow {workflowIds = missing})
        Right Nothing        -> Right (map Types.WorkflowId forkedIds)
  where
    unwrapQueueName (QueueName name) = name
    forkTimeoutMs Nothing = Right Nothing
    forkTimeoutMs (Just duration) =
      let millis = durationAsMillis duration
       in if millis > toInteger (maxBound :: Int64)
            then Left (invalidInput "timeout" "must fit in milliseconds as a 64-bit integer")
            else Right (Just (fromInteger millis))

-- | Sends one message or a batch: the oracle's @deliver@, whose only
-- difference between the two is the step name. Fork fan-out is refused
-- until @descendant_forks@ lands; an empty batch still records its step,
-- because a step id that nothing occupies is a hole a replay has to guess
-- about.
sendInternal :: PostgresSystemDB -> Text -> [SendMessage] -> Maybe Text -> Maybe (WorkflowId, Int) -> Bool -> IO (Either Error ())
sendInternal _ _ messages _ Nothing _ | null messages = pure (Right ())
sendInternal env stepName messages serialization caller sendToForks = do
  expanded <- if sendToForks then expandForks env messages else pure (Right messages)
  case expanded of
    Left err         -> pure (Left err)
    Right recipients -> deliverTo env stepName recipients serialization caller

-- | Every message copied once per recipient: the destination itself and
-- every workflow forked from it, transitively. One send fans out to a whole
-- fork tree, and each recipient gets its own scoped message id. Mirrors the
-- oracle's fork fan-out.
expandForks :: PostgresSystemDB -> [SendMessage] -> IO (Either Error [SendMessage])
expandForks env messages = do
  let roots = [destination | message <- messages, let Types.WorkflowId destination = message.sendDestinationId]
  forks <- descendantForks env roots
  pure $ case forks of
    Left err -> Left err
    Right byRoot ->
      Right
        [ (message {sendDestinationId = Types.WorkflowId recipient})
          | message <- messages,
            let Types.WorkflowId destination = message.sendDestinationId,
            recipient <- destination : Map.findWithDefault [] destination byRoot
        ]

-- | Every descendant of every root, level by level, deduplicated and
-- sorted. Terminates because the seen set only grows. Mirrors
-- @descendant_forks@.
descendantForks :: PostgresSystemDB -> [Text] -> IO (Either Error (Map.Map Text [Text]))
descendantForks env roots = go Map.empty Set.empty roots
  where
    go children seen frontier = do
      level <- runSession env "send_messages" (Statements.directForksSession frontier)
      case level of
        Left err -> pure (Left err)
        Right pairs -> do
          let fresh = [forkedId | (forkedId, _) <- pairs, Set.notMember forkedId seen]
              children' = foldr (\(forkedId, source) acc -> Map.insertWith (++) source [forkedId] acc) children pairs
              seen' = foldr Set.insert seen fresh
          if null fresh
            then pure (Right (Map.map (List.sort . List.nub) children'))
            else go children' seen' fresh

-- | Delivers a batch once the recipient set is settled.
deliverTo :: PostgresSystemDB -> Text -> [SendMessage] -> Maybe Text -> Maybe (WorkflowId, Int) -> IO (Either Error ())
deliverTo env stepName messages serialization caller =
  case validateSendKeys messages of
    Left err -> pure (Left err)
    Right () -> do
      fallbacks <- traverse (const (MessageUUID . UUID.toText <$> UUID.V4.nextRandom)) messages
      timing <- StepTiming <$> timestampNow <*> timestampNow
      let destinations = [destination | message <- messages, let Types.WorkflowId destination = message.sendDestinationId]
          topics = [maybe nullTopicSentinel (\(Topic stored) -> stored) message.sendTopic | message <- messages]
          payloads = [(.serializedText) message.sendMessageBody | message <- messages]
          messageIds =
            [ let MessageUUID scoped = messageUUIDForSend fallback message in scoped
              | (message, fallback) <- zip messages fallbacks
            ]
          sendStep = do
            (Types.WorkflowId widText, stepId) <- caller
            pure
              Statements.SendStep
                { sendStepWorkflowId = widText,
                  sendStepStepId = stepId,
                  sendStepName = stepName,
                  sendStepStartedAt = timestampToEpochMs timing.stepTimingStartedAt,
                  sendStepCompletedAt = timestampToEpochMs timing.stepTimingCompletedAt
                }
          params =
            Statements.SendMessagesParams
              { sendParamsDestinations = destinations,
                sendParamsTopics = topics,
                sendParamsPayloads = payloads,
                sendParamsMessageIds = messageIds,
                sendParamsSerialization = serialization,
                sendParamsStep = sendStep
              }
      result <- runTransaction env "send_messages" (Statements.sendMessagesTx params)
      pure $ case result of
        Left err
          | foreignKeyError err ->
              Left (NonExistentWorkflow {workflowIds = List.sort (List.nub destinations)})
          | otherwise -> Left err
        Right () -> Right ()

-- | The foreign key on the destination is what catches an address that does
-- not exist, so a message can never be left pointing at nothing.
foreignKeyError :: Error -> Bool
foreignKeyError (Backend backend) = backend.backendSqlState == Just "23503"
foreignKeyError _                 = False

-- | The keys a batch may carry: an empty key is a caller error, and two
-- messages under one key would give two rows the same primary key, so the
-- second would be silently discarded as a duplicate of the first. Mirrors
-- the oracle's check.
validateSendKeys :: [SendMessage] -> Either Error ()
validateSendKeys messages = go Set.empty messages
  where
    go _ [] = Right ()
    go seen (message : rest) = case message.sendIdempotencyKey of
      Nothing -> go seen rest
      Just (IdempotencyKey key)
        | Text.null key -> Left (invalidInput "idempotency_key" "must be absent rather than empty")
        | Set.member key seen -> Left (invalidInput "idempotency_key" (key <> " is used by more than one message"))
        | otherwise -> go (Set.insert key seen) rest

-- | Waits until a message is waiting or the deadline passes, woken by the
-- exclusive subscription the caller holds. Absence at the deadline is not an
-- error: the caller's transaction records the timeout. Mirrors the oracle's
-- look-wait-look loop.
pollForMessage :: PostgresSystemDB -> Subscription -> Text -> Text -> Timestamp -> IO (Either Error ())
pollForMessage env subscription workflowId topic deadline = do
  probe <- runPolling env "recv" (Statements.recvProbeSession workflowId topic)
  case probe of
    Left err -> pure (Left err)
    Right True -> pure (Right ())
    Right False -> do
      now <- timestampNow
      let remaining = fromMaybe (Duration 0) (durationSince deadline now)
      if remaining <= Duration 0
        then pure (Right ())
        else do
          waitForWakeup subscription (min remaining getEventPollInterval)
          pollForMessage env subscription workflowId topic deadline

-- | The encoding this layer uses for values it produces itself: a sleep's
-- wake time is a plain number written and read by the system database, so it
-- is stored legibly rather than in whatever the workflow chose. Mirrors
-- Rust @PORTABLE_JSON@.
portableJson :: Text
portableJson = "portable_json"

-- | The wake time a recorded sleep holds, which every execution must agree
-- on. A sleep with no recorded output, or one that is not epoch
-- milliseconds, is 'Malformed'. Mirrors Rust @decode_wake_time@.
decodeWakeTime :: WorkflowId -> Int -> Maybe Text -> Either Error Timestamp
decodeWakeTime wid stepId output = case output of
  Nothing ->
    Left (Malformed ("workflow " <> widText <> " step " <> Text.pack (show stepId) <> " is a sleep with no recorded wake time"))
  Just recorded -> case readMaybe (Text.unpack (Text.strip recorded)) :: Maybe Int64 of
    Nothing ->
      Left (Malformed ("workflow " <> widText <> " step " <> Text.pack (show stepId) <> " wake time " <> Text.pack (show recorded) <> " is not epoch milliseconds"))
    Just epochMs -> Right (timestampFromEpochMs epochMs)
  where
    widText = case wid of Types.WorkflowId text -> text

-- | Treats an empty string as an absent value, for the fields where another
-- SDK legitimately writes @""@. Mirrors Rust @empty_to_none@.
emptyToNone :: Maybe Text -> Maybe Text
emptyToNone value = value >>= \text -> if Text.null text then Nothing else Just text

-- | The roles column, which is @NULL@ when there are none: the column is
-- always a JSON array of strings, and empty maps to @NULL@ so "no roles" has
-- one representation. Mirrors Rust @encode_roles@.
encodeRoles :: [Text] -> Maybe Text
encodeRoles roles =
  if null roles
    then Nothing
    else Just (decodeUtf8 (LBS.toStrict (Aeson.encode roles)))

-- | Rust's @Debug@ spelling of an optional string, for the conflict detail.
debugOption :: Maybe Text -> Text
debugOption Nothing      = "None"
debugOption (Just value) = "Some(\"" <> value <> "\")"

-- | What a polled row says about settling. Mirrors @await_workflow_result@'s
-- match: terminal statuses report their outcome, the three live statuses say
-- nothing yet, and a failed workflow with no recorded error is 'Malformed'
-- rather than an empty message a caller would try to deserialize.
settledOutcome :: Text -> Statements.WorkflowStatusRaw -> Either Error (Maybe AwaitedOutcome)
settledOutcome wid row = do
  status <- case row.status of
    Nothing -> Left (Malformed ("workflow " <> wid <> " has a null status"))
    Just raw -> case parseWorkflowStatus raw of
      Left _       -> Left (Malformed ("unknown workflow status \"" <> raw <> "\""))
      Right parsed -> Right parsed
  case status of
    Success -> Right (Just (AwaitedSucceeded row.output row.serialization))
    Error -> case row.error of
      Nothing      -> Left (Malformed ("workflow " <> wid <> " failed with no error recorded"))
      Just message -> Right (Just (AwaitedFailed message row.serialization))
    Cancelled -> Right (Just AwaitedCancelled)
    MaxRecoveryAttemptsExceeded -> Right (Just (AwaitedParked (fromMaybe 0 row.recovery_attempts)))
    Pending -> Right Nothing
    Enqueued -> Right Nothing
    Delayed -> Right Nothing

-- | A poll interval in microseconds, for 'threadDelay'.
durationMicros :: Duration -> Int
durationMicros = fromInteger . (* 1000) . durationAsMillis

-- | Sleeps until the key is woken or the interval passes, whichever comes
-- first. A wakeup only ever shortens the interval: the caller re-reads the
-- database either way, so both outcomes mean "look again".
waitForWakeup :: Subscription -> Duration -> IO ()
waitForWakeup subscription interval =
  void (Timeout.timeout (durationMicros interval) (notified subscription))

-- | Runs one polling read: the retry loop wraps the permit so a poll that is
-- backing off is not holding one, and the permit is released before the
-- caller sleeps, so the cap bounds concurrent queries rather than waiters.
-- Mirrors where the oracle takes its semaphore permit.
runPolling :: PostgresSystemDB -> Text -> Session a -> IO (Either Error a)
runPolling env operation session =
  withRetry env.psdbRetry operation env.psdbLog uuidEntropy $
    withPollingPermit env $ do
      result <- Pool.use env.psdbPool session
      pure $ case result of
        Left usage  -> Left (classifyUsageError usage)
        Right value -> Right value

-- | One polling-concurrency permit, held for the length of a query.
withPollingPermit :: PostgresSystemDB -> IO a -> IO a
withPollingPermit env = bracket_ acquire release
  where
    acquire = atomically $ do
      permits <- readTVar env.psdbPollingPermits
      check (permits > 0)
      writeTVar env.psdbPollingPermits (permits - 1)
    release = atomically $ modifyTVar env.psdbPollingPermits (+ 1)

-- | The Postgres backend. Methods are implemented one TDD cycle at a time;
-- unwritten ones are 'undefined' so the suite stays green while the port
-- grows. Every method takes the environment explicitly and applies
-- 'runSession' to it.
instance SystemDB PostgresSystemDB IO where
  initWorkflow env new maxRecoveryAttempts submission caller =
    case caller of
      -- The atomic child start: the row (with its back-pointer) and the
      -- parent record commit together, so a crash can never leave one
      -- without the other. A stored rival child is nondeterminism, reported
      -- after the commit like the other post-write guards.
      Just caller -> case validateNewWorkflow new of
        Left err -> pure (Left err)
        Right () -> do
          ownerXid <- UUID.toText <$> UUID.V4.nextRandom
          now <- timestampNow
          let status = initialStatus new
              queued = status == Enqueued || status == Delayed
              claiming = claimsOwnership submission
              parentText = unwrapWorkflowId caller.initCallerParentWorkflowId
              params =
                (initParams env new status queued claiming ownerXid now)
                  { Statements.initParamParentWorkflowId = Just parentText
                  }
          result <- runTransaction env "init_workflow" $ do
            row <- Tx.statement () (Statements.initWorkflowStatement params)
            stored <-
              Tx.statement
                ()
                ( Statements.recordChildWorkflowStatement
                    Statements.RecordChildWorkflowParams
                      { recordChildParentId = parentText,
                        recordChildChildId = new.newWorkflowId,
                        recordChildStepId = caller.initCallerStepId,
                        recordChildStepName = caller.initCallerStepName,
                        recordChildStartedAt = Just (timestampToEpochMs caller.initCallerStartedAt),
                        recordChildCompletedAt = Just (timestampToEpochMs now),
                        recordChildApplicationName = env.psdbApplicationName
                      }
                )
            -- The input lands where readers look, in the same commit, and
            -- only when this attempt created the row: a submission that
            -- found an existing row must not touch its recorded input.
            when (row.owner_xid == Just ownerXid) $
              void $
                Tx.statement
                  (new.newWorkflowId, new.newWorkflowInput)
                  Statements.initWorkflowInputStatement
            case (initConflict new row, stored) of
              (Just err, _) -> do
                -- The stored row is another workflow's: nothing this call
                -- wrote commits. Mirrors Rust's commit-time conflict check.
                Tx.condemn
                pure (Left err)
              (Nothing, Just childId)
                | childId == new.newWorkflowId -> pure (Right (row, stored))
              (Nothing, _) -> do
                -- A rival execution owns this parent step: roll the child row
                -- and the step back rather than leave them pointing at each
                -- other. Mirrors record_child_workflow_on's conflict.
                Tx.condemn
                pure (Left (StepAlreadyRecorded {workflowId = parentText, stepId = caller.initCallerStepId}))
          case result of
            Left err
              | queueDeduplicated new err ->
                  pure
                    ( Left
                        QueueDeduplicated
                          { workflowId = new.newWorkflowId,
                            queueName = fromMaybe "" new.newWorkflowQueueName,
                            deduplicationId = fromMaybe "" new.newWorkflowDeduplicationId
                          }
                    )
              | otherwise -> pure (Left err)
            Right (Left conflict) -> pure (Left conflict)
            Right (Right (row, _stored)) -> finishInit env new maxRecoveryAttempts claiming ownerXid row
      Nothing -> case validateNewWorkflow new of
        Left err -> pure (Left err)
        Right () -> do
          ownerXid <- UUID.toText <$> UUID.V4.nextRandom
          now <- timestampNow
          let status = initialStatus new
              queued = status == Enqueued || status == Delayed
              claiming = claimsOwnership submission
              params = initParams env new status queued claiming ownerXid now
          -- The status row and the input commit together, so a crash can
          -- never leave one without the other; the input lands only when
          -- this attempt created the row.
          result <- runTransaction env "init_workflow" $ do
            row <- Tx.statement () (Statements.initWorkflowStatement params)
            when (row.owner_xid == Just ownerXid) $
              void $
                Tx.statement
                  (new.newWorkflowId, new.newWorkflowInput)
                  Statements.initWorkflowInputStatement
            pure row
          case result of
            Left err
              | queueDeduplicated new err ->
                  pure
                    ( Left
                        QueueDeduplicated
                          { workflowId = new.newWorkflowId,
                            queueName = fromMaybe "" new.newWorkflowQueueName,
                            deduplicationId = fromMaybe "" new.newWorkflowDeduplicationId
                          }
                    )
              | otherwise -> pure (Left err)
            Right row -> finishInit env new maxRecoveryAttempts claiming ownerXid row
  getWorkflow env wid = do
    result <- runSession env "get_workflow" (Statements.getWorkflowSession widText)
    pure (result >>= traverse workflowRecordFromRow)
    where
      Types.WorkflowId widText = wid
  listWorkflows env workflowFilter _caller = do
    result <- runSession env "list_workflows" (Statements.listWorkflowsSession (listParams env workflowFilter))
    pure (result >>= traverse workflowRecordFromRow)
  getWorkflowChildren env wid = do
    result <- runSession env "get_workflow_children" (Statements.descendantsSession widText)
    pure (map Types.WorkflowId <$> result)
    where
      Types.WorkflowId widText = wid
  recordWorkflowOutcome env wid outcome = do
    let (output, errorText) = outcomeColumns outcome
        Types.WorkflowId widText = wid
        statusText = workflowStatusText (outcomeStatus outcome)
    -- The status change and the payload commit together, and the payload
    -- lands only behind a change that matched: a refused outcome leaves no
    -- orphan row behind.
    result <-
      runTransaction env "record_workflow_outcome" $ do
        updated <- Tx.statement () (Statements.recordWorkflowOutcomeStatement widText statusText output errorText)
        when (updated > 0) $
          void $
            Tx.statement () (Statements.recordWorkflowOutputStatement widText output errorText)
        pure updated
    pure $ case result of
      Left err -> Left err
      Right updated
        | updated > 0 -> Right Recorded
        | otherwise -> Right AlreadyFinished
  awaitWorkflowResult env wid pollInterval failIfMissing = loop env (unwrap wid)
    where
      loop env widText = do
        result <- runPolling env "await_workflow_result" (Statements.workflowStatusSession widText)
        case result of
          Left err -> pure (Left err)
          Right Nothing
            | failIfMissing -> pure (Left (NonExistentWorkflow {workflowIds = [widText]}))
            | otherwise -> sleepThen env widText
          Right (Just row) -> case settledOutcome widText row of
            Left err             -> pure (Left err)
            Right (Just outcome) -> pure (Right outcome)
            Right Nothing        -> sleepThen env widText
      sleepThen env widText = do
        threadDelay (durationMicros pollInterval)
        loop env widText
      unwrap (Types.WorkflowId widText) = widText
  awaitFirstWorkflowId env workflowIds pollInterval
    | null workflowIds = pure (Left (invalidInput "workflow_ids" "must name at least one workflow to wait for"))
    | otherwise = loop env (map unwrap workflowIds)
    where
      loop env ids = do
        result <- runPolling env "await_first_workflow_id" (Statements.firstSettledSession ids)
        case result of
          Left err -> pure (Left err)
          Right (Just winner) -> pure (Right (Types.WorkflowId winner))
          Right Nothing -> do
            threadDelay (durationMicros pollInterval)
            loop env ids
      unwrap (Types.WorkflowId widText) = widText
  awaitWorkflowIds env workflowIds pollInterval
    | null workflowIds = pure (Right ())
    | otherwise = loop env (Set.fromList (map unwrap workflowIds))
    where
      loop env outstanding = do
        result <- runPolling env "await_workflow_ids" (Statements.settledIdsSession (Set.toList outstanding))
        case result of
          Left err -> pure (Left err)
          Right settled -> do
            let remaining = foldr Set.delete outstanding settled
            if Set.null remaining
              then pure (Right ())
              else do
                threadDelay (durationMicros pollInterval)
                loop env remaining
      unwrap (Types.WorkflowId widText) = widText
  setWorkflowDelay env wid delay _caller = do
    now <- timestampNow
    case resolveWorkflowDelay delay now of
      Nothing -> pure (Left (invalidInput "delay" "does not resolve to a representable instant"))
      Just resolved -> do
        result <- runSession env "set_workflow_delay" (Statements.setWorkflowDelaySession (unwrap wid) (timestampToEpochMs resolved))
        pure (() <$ result)
    where
      unwrap (Types.WorkflowId widText) = widText
  clearQueueAssignment env wid = do
    now <- timestampNow
    result <- runSession env "clear_queue_assignment" (Statements.clearQueueAssignmentSession (unwrap wid) (timestampToEpochMs now))
    pure (fmap (> 0) result)
    where
      unwrap (Types.WorkflowId widText) = widText
  updateWorkflowAttributes env wid attributes _caller =
    case validateAttributes attributes of
      Left err -> pure (Left err)
      Right () -> do
        result <- runSession env "update_workflow_attributes" (Statements.updateWorkflowAttributesSession (unwrap wid) attributes)
        pure (() <$ result)
    where
      unwrap (Types.WorkflowId widText) = widText
  reenqueueForRecovery env executorIds applicationVersion recoveryQueue
    | null executorIds = pure (Right [])
    | otherwise = do
        result <-
          runSession
            env
            "reenqueue_for_recovery"
            (Statements.reenqueueForRecoverySession recoveryQueue executorIds applicationVersion env.psdbApplicationName)
        pure (map Types.WorkflowId <$> result)
  transitionDelayedWorkflows env = do
    now <- timestampNow
    result <- runSession env "transition_delayed_workflows" (Statements.transitionDelayedSession (timestampToEpochMs now) env.psdbApplicationName)
    pure (fromIntegral <$> result)
  cancelWorkflows env workflowIds cancelChildren _caller
    | null workflowIds = pure (Right [])
    | otherwise = do
        targets <- if cancelChildren
          then do
            descendants <- traverse (\wid -> getWorkflowChildren env wid) workflowIds
            case sequence descendants of
              Left err     -> pure (Left err)
              Right levels -> pure (Right (map unwrap workflowIds <> concatMap (map unwrap) levels))
          else pure (Right (map unwrap workflowIds))
        case targets of
          Left err -> pure (Left err)
          Right ids -> do
            result <- runSession env "cancel_workflows" (Statements.cancelBatchSession (List.nub ids))
            pure (map Types.WorkflowId <$> result)
    where
      unwrap (Types.WorkflowId widText) = widText
  resumeWorkflows env workflowIds queue _caller
    | null workflowIds = pure (Right [])
    | otherwise = do
        existing <- runSession env "resume_workflows" (Statements.existingWorkflowsSession ids)
        case existing of
          Left err -> pure (Left err)
          Right present -> do
            let missing = [widText | widText <- ids, widText `notElem` present]
            if not (null missing)
              then pure (Left (NonExistentWorkflow {workflowIds = missing}))
              else do
                -- The oracle defaults a nameless resume onto the internal
                -- queue; a NULL queue_name would leave the row unclaimed.
                resumed <- runSession env "resume_workflows" (Statements.resumeWorkflowsSession ids (queue <|> Just (unwrapQueueName internalQueueName)))
                pure (map Types.WorkflowId <$> resumed)
    where
      ids = map unwrap workflowIds
      unwrap (Types.WorkflowId widText) = widText
      unwrapQueueName (QueueName name) = name
  deleteWorkflows env workflowIds deleteChildren caller
    | null workflowIds = pure (Right 0)
    | otherwise = do
        children <- if deleteChildren
          then do
            descendants <- traverse (\wid -> getWorkflowChildren env wid) workflowIds
            pure (concatMap (map unwrap) <$> sequence descendants)
          else pure (Right [])
        case children of
          Left err -> pure (Left err)
          Right descendants -> do
            let targets = List.nub (List.sort (map unwrap workflowIds <> descendants))
            -- A workflow cannot delete itself, and cannot delete an
            -- ancestor whose tree it is inside. Refused here, before
            -- anything runs: the step checkpoint for the delete lands in
            -- @operation_outputs@ in the same transaction, so it would land
            -- as an orphan of the status row the delete just removed, and
            -- the workflow would go on running with no row to record its
            -- outcome on. Only the caller's own id is checked,
            -- not its ancestry — an ancestor is a target only when it was
            -- named with children, and then the walk above has already put
            -- the caller in the targets.
            case caller of
              Just (Types.WorkflowId callerId, _)
                | callerId `elem` targets ->
                    pure
                      ( Left
                          ( invalidInput
                              "workflow_ids"
                              ( "workflow " <> callerId <> " cannot delete itself: the step checkpoint for the delete "
                                  <> "is written in the same transaction and would outlive the row it references"
                              )
                          )
                      )
              _ -> do
                result <- runSession env "delete_workflows" (Statements.deleteWorkflowsSession targets)
                pure (fromIntegral <$> result)
    where
      unwrap (Types.WorkflowId widText) = widText
  forkWorkflows env forks options _caller
    | null forks = pure (Right [])
    | otherwise =
        case (forkOptionsValidate options, traverse forkValidate forks) of
          (Left err, _) -> pure (Left err)
          (_, Left err) -> pure (Left err)
          (Right (), Right _) -> do
            forkedIds <- traverse (maybe (UUID.toText <$> UUID.V4.nextRandom) pure . (.forkForkedId)) forks
            runFork env (map (.forkSourceId) forks) forkedIds (map (.forkStartStep) forks) options
  forkFrom env workflowIds point options _caller
    | null workflowIds = pure (Right [])
    | otherwise =
        case forkOptionsValidate options of
          Left err -> pure (Left err)
          Right () -> do
            let sources = map unwrap workflowIds
            points <- case point of
              ForkStep step      -> pure (Right (replicate (length sources) step))
              ForkStepNamed name -> resolvePoints env sources "MAX(function_id)" (Just name)
              ForkLastStep       -> resolvePoints env sources "MAX(function_id)" Nothing
              ForkLastFailure    -> resolvePoints env sources "COALESCE(MAX(function_id) FILTER (WHERE error IS NOT NULL), MAX(function_id))" Nothing
            case points of
              Left err -> pure (Left err)
              Right steps -> do
                forkedIds <- traverse (const (UUID.toText <$> UUID.V4.nextRandom)) sources
                runFork env sources forkedIds steps options
    where
      unwrap (Types.WorkflowId widText) = widText
      resolvePoints env sources aggregate named = do
        result <- runSession env "fork_from" (Session.statement (sources, named) (Statements.forkPointsStatement aggregate))
        case result of
          Left err -> pure (Left err)
          Right rows -> do
            let resolved = Map.fromList rows
                missing = [source | source <- sources, not (Map.member source resolved)]
            if not (null missing)
              then pure (Left (NoForkPoint {workflowIds = missing, stepName = named}))
              else pure (Right [fromMaybe 0 (Map.lookup source resolved) | source <- sources])
  sendMessage env message serialization caller sendToForks =
    sendInternal env sendStepName [message] serialization caller sendToForks
  sendMessages env messages serialization caller sendToForks =
    sendInternal env sendBulkStepName messages serialization caller sendToForks
  recv env wid stepId timeoutStepId topic timeout = do
    let storedTopic = fromMaybe nullTopicSentinel topic
        widText = unwrap wid
    startedAt <- timestampNow
    replay <- checkStep env wid stepId recvStepName
    case replay of
      Left err -> pure (Left err)
      -- A replay returns the message the first run took and does not take
      -- another, including the absence a timeout produced.
      Right (Just step) -> pure (Right (stepEncodedValue step))
      Right Nothing -> do
        acquired <- subscribeExclusive env.psdbNotify (messageKey widText (Just storedTopic))
        case acquired of
          Nothing -> pure (Left ConcurrentRecv {workflowId = widText, topic = topic})
          Just subscription ->
            ( do
                deadline <- checkpointSleep env DeadlineSleep wid timeoutStepId timeout
                case deadline of
                  Left err -> pure (Left err)
                  Right wakeAt -> do
                    waited <- pollForMessage env subscription widText storedTopic wakeAt
                    case waited of
                      Left err -> pure (Left err)
                      Right () -> do
                        completedAt <- timestampNow
                        taken <-
                          runTransaction
                            env
                            "recv"
                            ( Statements.recvTx
                                Statements.RecvParams
                                  { recvWorkflowId = widText,
                                    recvStepId = stepId,
                                    recvTopic = storedTopic,
                                    recvStartedAt = timestampToEpochMs startedAt,
                                    recvCompletedAt = timestampToEpochMs completedAt
                                  }
                            )
                        pure $ case taken of
                          Left err -> Left err
                          Right (Statements.RecvAdopted row) ->
                            fmap (>>= stepEncodedValue) (stepCheckToRecord wid stepId recvStepName row)
                          Right (Statements.RecvTook message) ->
                            Right ((\raw -> EncodedValue raw.encodedRawValue raw.encodedRawSerialization) <$> message)
            )
              `finally` unsubscribe subscription
    where
      unwrap (Types.WorkflowId widText) = widText
  -- DEFERRED (P7.4): streams are the one family held back — they are the
  -- only consumers of the STREAM_CLOSED sentinel and the offset-retry loop,
  -- and nothing the engine needs today touches them. The ported tests are
  -- in @DBOS.SystemDB.PostgresTest.streamTests@, ready to wire the day these
  -- land; the stubs stay 'undefined' so a caller cannot mistake them for
  -- working.
  writeStream = undefined
  closeStream = undefined
  close env = releasePostgresSystemDB env
  checkStep env wid stepId stepName = do
    result <- runSession env "check_step" (Statements.checkStepSession (unwrap wid) stepId)
    pure $ case result of
      Left err         -> Left err
      Right Nothing    -> Left (NonExistentWorkflow {workflowIds = [unwrap wid]})
      Right (Just row) -> stepCheckToRecord wid stepId stepName row
    where
      unwrap (Types.WorkflowId widText) = widText
  recordStep env wid stepId stepName outcome serialization timing
    | Text.null widText = pure (Left (invalidInput "workflow_id" "must not be empty"))
    | stepId < 0 = pure (Left (invalidInput "step_id" "must not be negative"))
    | otherwise = do
        let (output, errorText) = outcomeColumns outcome
            params =
              Statements.RecordStepParams
                { recordStepWorkflowId = widText,
                  recordStepStepId = stepId,
                  recordStepStepName = stepName,
                  recordStepOutput = output,
                  recordStepError = errorText,
                  recordStepSerialization = serialization,
                  recordStepStartedAt = timestampToEpochMs . (.stepTimingStartedAt) <$> timing,
                  recordStepCompletedAt = timestampToEpochMs . (.stepTimingCompletedAt) <$> timing,
                  recordStepApplicationName = env.psdbApplicationName,
                  recordStepChildWorkflowId = Nothing
                }
        result <- runSession env "record_step" (Statements.recordStepSession params)
        case result of
          Left err -> pure (Left err)
          Right stored -> do
            let ours = stored == (timestampToEpochMs . (.stepTimingCompletedAt) <$> timing)
            if not ours
              then pure (Left (StepAlreadyRecorded {workflowId = widText, stepId = stepId}))
              else case env.psdbExecutorId of
                Nothing -> pure (Right ())
                Just executorId -> do
                  claimed <- runSession env "record_step" (Statements.claimExecutorSession widText executorId)
                  pure (() <$ claimed)
    where
      widText = case wid of Types.WorkflowId text -> text
  listSteps env wid loadOutput limit offset _caller = do
    result <- runSession env "list_workflow_steps" (Statements.listStepsSession (unwrap wid) loadOutput limit offset)
    pure (fmap (map toRecord) result)
    where
      unwrap (Types.WorkflowId widText) = widText
      toRecord row =
        StepRecord
          { stepRecordWorkflowId = wid,
            stepRecordStepId = row.stepRowId,
            stepRecordStepName = row.stepRowName,
            stepRecordOutput = row.stepRowOutput,
            stepRecordError = row.stepRowError,
            stepRecordChildWorkflowId = Types.WorkflowId <$> row.stepRowChildWorkflowId,
            stepRecordSerialization = row.stepRowSerialization,
            stepRecordStartedAt = timestampFromEpochMs <$> row.stepRowStartedAt,
            stepRecordCompletedAt = timestampFromEpochMs <$> row.stepRowCompletedAt
          }
  recordSleep env wid stepId duration = checkpointSleep env DurableSleep wid stepId duration
  setEvent env wid stepId key value serialization = do
    startedAt <- timestampNow
    completedAt <- timestampNow
    let params =
          Statements.SetEventParams
            { setEventWorkflowId = unwrap wid,
              setEventStepId = stepId,
              setEventKey = key,
              setEventValue = value,
              setEventSerialization = serialization,
              setEventStartedAt = timestampToEpochMs startedAt,
              setEventCompletedAt = timestampToEpochMs completedAt
            }
    result <- runTransaction env "set_event" (Statements.setEventTx params)
    case result of
      Left err -> pure (Left err)
      Right row -> do
        -- Committed, so a reader woken by this finds the row. No trigger does
        -- this (migration 44 removes it), so the writer must — after the
        -- commit, not inside it.
        signal env.psdbNotifier eventsChannel (unwrap wid) key
        pure (() <$ traverse (stepCheckToRecord wid stepId "DBOS.setEvent") row)
    where
      unwrap (Types.WorkflowId widText) = widText
  getEvent env wid key timeout caller = do
    startedAt <- timestampNow
    -- A replay returns what the first run saw and does not wait again —
    -- including the absence a timeout produced, which is a result too.
    replay <- case caller of
      Nothing -> pure (Right Nothing)
      Just c -> do
        found <- checkStep env c.getEventCallerWorkflowId c.getEventCallerStepId "DBOS.getEvent"
        pure (fmap (fmap stepEncodedValue) found)
    case replay of
      Left err -> pure (Left err)
      -- A recorded step is the answer, value or recorded absence alike.
      Right (Just value) -> pure (Right value)
      Right Nothing -> do
        deadlineOrError <- case caller of
          Just c -> do
            woke <- checkpointSleep env DeadlineSleep c.getEventCallerWorkflowId c.getEventCallerTimeoutStepId timeout
            pure (either Left Right woke)
          -- A timeout too large to represent is one that never elapses.
          Nothing -> pure (Right (fromMaybe (Timestamp maxBound) (addTimeout startedAt timeout)))
        case deadlineOrError of
          Left err -> pure (Left err)
          Right deadline -> do
            found <- pollEvent env (unwrap wid) key deadline
            case found of
              Left err -> pure (Left err)
              Right value -> case caller of
                Nothing -> pure (Right value)
                Just c -> do
                  completedAt <- timestampNow
                  let params =
                        Statements.GetEventRecordParams
                          { getEventRecordWorkflowId = unwrap c.getEventCallerWorkflowId,
                            getEventRecordStepId = c.getEventCallerStepId,
                            getEventRecordOutput = (.encodedValue) <$> value,
                            getEventRecordSerialization = value >>= (.encodedSerialization),
                            getEventRecordStartedAt = timestampToEpochMs startedAt,
                            getEventRecordCompletedAt = timestampToEpochMs completedAt
                          }
                  recorded <- runTransaction env "get_event" (Statements.getEventRecordTx params)
                  pure $ case recorded of
                    Left err -> Left err
                    Right Nothing -> Right value
                    Right (Just row) ->
                      fmap (>>= stepEncodedValue)
                        (stepCheckToRecord c.getEventCallerWorkflowId c.getEventCallerStepId "DBOS.getEvent" row)
    where
      unwrap (Types.WorkflowId widText) = widText
  getAllNotifications env wid = do
    result <- runSession env "get_all_notifications" (Session.statement (unwrap wid) Statements.allNotificationsStatement)
    pure (fmap (map notificationRecordFromRaw) result)
    where
      unwrap (Types.WorkflowId widText) = widText
  getAllEvents env wid = do
    result <- runSession env "get_all_events" (Session.statement (unwrap wid) Statements.allEventsStatement)
    pure (fmap (map eventRecordFromRaw) result)
    where
      unwrap (Types.WorkflowId widText) = widText
  -- DEFERRED (P7.4): see writeStream above.
  readStreamValue = undefined
  getAllStreamEntries = undefined
  createApplicationVersion env versionName applicationName = do
    let claimant = applicationName <|> env.psdbApplicationName
    holder <- runSession env "create_application_version" (Statements.versionHolderSession versionName)
    case holder of
      Left err -> pure (Left err)
      Right existing -> case resolveOwner "Application version" versionName claimant existing of
        Left err -> pure (Left err)
        Right _ -> do
          claimed <- runSession env "create_application_version" (Statements.versionClaimSession versionName claimant)
          case claimed of
            Left err -> pure (Left err)
            Right rows -> do
              when (rows == 0) $ do
                versionId <- UUID.toText <$> UUID.V4.nextRandom
                _ <- runSession env "create_application_version" (Statements.versionInsertSession versionId versionName claimant)
                pure ()
              pure (Right ())
  listApplicationVersions env = do
    result <- runSession env "list_application_versions" (Statements.listApplicationVersionsSession env.psdbApplicationName)
    pure (map versionInfoFromRow <$> result)
  getLatestApplicationVersion env applicationName = do
    result <- runSession env "get_latest_application_version" (Statements.latestApplicationVersionSession (applicationName <|> env.psdbApplicationName))
    pure (fmap versionInfoFromRow <$> result)
  updateApplicationVersionTimestamp env versionName timestamp applicationName = do
    let claimant = applicationName <|> env.psdbApplicationName
    holder <- runSession env "update_application_version_timestamp" (Statements.versionHolderSession versionName)
    case holder of
      Left err -> pure (Left err)
      Right existing -> case resolveOwner "Application version" versionName claimant existing of
        Left err -> pure (Left err)
        Right owner -> do
          result <-
            runSession
              env
              "update_application_version_timestamp"
              (Statements.updateVersionTimestampSession versionName (timestampToEpochMs timestamp) owner)
          pure (() <$ result)
  upsertQueue env queue onExisting = do
    let name = queue.newQueueName
        owner = queue.newQueueApplicationName <|> env.psdbApplicationName
    before <- runSession env "upsert_queue" (Session.statement name Statements.queueOwnerStatement)
    case before of
      Left err -> pure (Left err)
      Right existing -> case resolveOwner "Queue" name owner existing of
        Left err -> pure (Left err)
        Right _ -> do
          updatedAt <- timestampToEpochMs <$> timestampNow
          outcome <- runTransaction env "upsert_queue" $ do
            -- The oracle resolves the owner again after the write and rolls
            -- back on a mismatch; resolving once before the insert leaves
            -- nothing to roll back, and the insert's COALESCE protects the
            -- owner either way.
            ownerNow <- Tx.statement name Statements.queueOwnerStatement
            case resolveOwner "Queue" name owner ownerNow of
              Left err -> pure (Left err)
              Right _ -> do
                _ <- Tx.statement (queueInsertParams queue updatedAt owner) (Statements.queueInsertStatement onExisting)
                pure (Right ())
          case outcome of
            Left err         -> pure (Left err)
            Right (Left err) -> pure (Left err)
            Right (Right ()) -> pure (Right (isNothing existing))
  startQueuedWorkflows env queue executorId applicationVersion partitionKey localRunning partitionLocalRunning =
    case partitionKey of
      Just "" -> pure (Left (invalidInput "partition_key" "must be absent rather than empty"))
      _ -> do
        let limits = queueResolvedLimits queue
            queueName = queue.queueRecordName
            app = env.psdbApplicationName
            hasSharedBudget =
              isJust limits.resolvedConcurrency
                || isJust limits.resolvedPartitionConcurrency
                || isJust limits.resolvedRateLimit
                || isJust limits.resolvedPartitionRateLimit
            hasWriteSkew = isJust partitionKey && (isJust limits.resolvedConcurrency || isJust limits.resolvedRateLimit)
            rateLimited = isJust limits.resolvedRateLimit || isJust limits.resolvedPartitionRateLimit
            isolation
              | not hasSharedBudget = TxSessions.ReadCommitted
              | hasWriteSkew = TxSessions.Serializable
              | otherwise = TxSessions.RepeatableRead
            narrow current available = Just (maybe available (min available) current)
            budget =
              foldr
                (\available current -> narrow current available)
                Nothing
                ( [max 0 (fromIntegral cap - localRunning) | Just cap <- [limits.resolvedWorkerConcurrency]]
                    <> [max 0 (fromIntegral cap - partitionLocalRunning) | Just cap <- [limits.resolvedPartitionWorkerConcurrency], isJust partitionKey]
                )
        result <- runTransactionAt env isolation "start_queued_workflows" $ do
          case budget of
            Just 0 -> pure []
            _ -> do
              afterRate <- case limits.resolvedRateLimit of
                Nothing -> pure budget
                Just rateLimit -> do
                  recent <- Tx.statement (app, queueName, periodMillis rateLimit.rateLimitPeriod) Statements.dequeueRateLimitCountStatement
                  pure (narrow budget (max 0 (fromIntegral rateLimit.rateLimitLimit - recent)))
              afterPartitionRate <- case (limits.resolvedPartitionRateLimit, partitionKey) of
                (Just rateLimit, Just key) -> do
                  recent <- Tx.statement (app, key, queueName, periodMillis rateLimit.rateLimitPeriod) Statements.dequeuePartitionRateLimitCountStatement
                  pure (narrow afterRate (max 0 (fromIntegral rateLimit.rateLimitLimit - recent)))
                _ -> pure afterRate
              afterConcurrency <- case limits.resolvedConcurrency of
                Nothing -> pure afterPartitionRate
                Just cap -> do
                  running <- Tx.statement (app, queueName) Statements.dequeueConcurrencyCountStatement
                  pure (narrow afterPartitionRate (max 0 (fromIntegral cap - running)))
              afterPartitionConcurrency <- case (limits.resolvedPartitionConcurrency, partitionKey) of
                (Just cap, Just key) -> do
                  running <- Tx.statement (app, key, queueName) Statements.dequeuePartitionConcurrencyCountStatement
                  pure (narrow afterConcurrency (max 0 (fromIntegral cap - running)))
                _ -> pure afterConcurrency
              case afterPartitionConcurrency of
                Just 0 -> pure []
                _ -> do
                  latest <- Tx.statement app Statements.latestVersionNameStatement
                  let isLatest = maybe True (== applicationVersion) latest
                      candidatesStatement =
                        if hasSharedBudget
                          then Statements.dequeueCandidatesNowaitStatement
                          else Statements.dequeueCandidatesSkipLockedStatement
                  candidates <-
                    Tx.statement
                      (app, partitionKey, applicationVersion, queueName, isLatest, afterPartitionConcurrency)
                      candidatesStatement
                  if null candidates
                    then pure []
                    else do
                      flipped <- Tx.statement (app, executorId, applicationVersion, rateLimited, candidates) Statements.dequeueClaimStatement
                      pure (filter (`elem` flipped) candidates)
        pure (fmap (map WorkflowId) result)
  getQueuePartitions env queueName =
    runSession
      env
      "get_queue_partitions"
      (Session.statement (queueName, env.psdbApplicationName) Statements.queuePartitionsStatement)
  startQueuedPartitionedWorkflows env queue executorId applicationVersion maxTasks = do
    let limits = queueResolvedLimits queue
        queueName = queue.queueRecordName
        app = env.psdbApplicationName
    case validateSweepLimits limits of
      Left err -> pure (Left err)
      Right () -> case maxTasks of
        Just 0 -> pure (Right [])
        _ -> do
          result <- runTransaction env "start_queued_partitioned_workflows" $ do
            latest <- Tx.statement app Statements.latestVersionNameStatement
            let isLatest = maybe True (== applicationVersion) latest
                cap = fromIntegral dequeueSweepCap
                sweepLimit = maybe cap (min cap) maxTasks
                candidatesStatement =
                  if sweepLimit < cap
                    then Statements.partitionSweepRandomCandidatesStatement
                    else Statements.partitionSweepCandidatesStatement
            candidates <-
              Tx.statement
                (queueName, app, applicationVersion, sweepLimit, isLatest)
                candidatesStatement
            if null candidates
              then pure []
              else do
                locked <- Tx.statement (candidates, queueName, applicationVersion, app, isLatest) Statements.partitionSweepLockStatement
                let claiming = filter (`elem` locked) candidates
                if null claiming
                  then pure []
                  else do
                    flipped <-
                      Tx.statement
                        (claiming, queueName, applicationVersion, app, executorId, isLatest)
                        Statements.partitionSweepFlipStatement
                    pure (filter (`elem` flipped) claiming)
          pure (fmap (map WorkflowId) result)
  getQueue env name = do
    result <- runSession env "get_queue" (Session.statement name Statements.queueByNameStatement)
    pure (result >>= traverse queueRecordFromRow)
  listQueues env applications = do
    let scope = case applications of
          AnyApplication -> Nothing
          Named []       -> Nothing
          Named names    -> Just names
          Unset          -> (: []) <$> env.psdbApplicationName
    result <- runSession env "list_queues" (Session.statement scope Statements.queueListStatement)
    pure (result >>= traverse queueRecordFromRow)
  updateQueue env name update validate = do
    updatedAt <- timestampToEpochMs <$> timestampNow
    result <- runTransaction env "update_queue" $ do
      stored <- Tx.statement name Statements.queueByNameForUpdateStatement
      case stored of
        Nothing -> pure (Left (NotRegistered {kind = "Queue", name = name}))
        Just raw -> case queueRecordFromRow raw of
          Left err -> pure (Left err)
          Right record
            | isQueueUpdateEmpty update -> pure (Right (Right record))
            | otherwise -> case validate record (applyQueueUpdate update record) of
                Left err -> pure (Left err)
                Right () -> do
                  written <- Tx.statement (queueUpdateParams name (applyQueueUpdate update record) updatedAt) Statements.queueUpdateStatement
                  case written of
                    Nothing  -> pure (Right (Left (Malformed "the update wrote no queue row")))
                    Just row -> pure (Right (queueRecordFromRow row))
    case result of
      Left err              -> pure (Left err)
      Right (Left err)      -> pure (Left err)
      Right (Right outcome) -> pure outcome
  debounceDelayedWorkflow env request caller =
    case caller of
      Just (callerWid, callerStep) -> case debounceValidate request of
        Left err -> pure (Left err)
        Right () -> do
          startedAt <- timestampNow
          completedAt <- timestampToEpochMs <$> timestampNow
          let app = request.debounceRequestApplicationName <|> env.psdbApplicationName
          Types.WorkflowId callerText <- pure callerWid
          outcome <- runTransaction env "debounce_delayed_workflow" (debounceCallerTx app callerWid callerText callerStep startedAt completedAt request)
          pure (join outcome)
      Nothing -> case debounceValidate request of
        Left err -> pure (Left err)
        Right () -> do
          let app = request.debounceRequestApplicationName <|> env.psdbApplicationName
          result <- runTransaction env "debounce_delayed_workflow" $ do
            bounced <- Tx.statement (debounceBounceParams request app) Statements.debounceBounceStatement
            case bounced of
              Just workflowId -> do
                void $ Tx.statement (workflowId, request.debounceRequestInputs) Statements.debounceInputStatement
                pure (Debounced {debounceWorkflowId = workflowId})
              Nothing -> do
                holder <- Tx.statement (request.debounceRequestQueueName, request.debounceRequestDeduplicationId) Statements.debounceHolderStatement
                pure (maybe DebounceUnheld debounceHeld holder)
          pure result

-- | The caller's bounce as one commit: a recorded step is the answer and
-- the work never re-runs; otherwise the bounce runs and its output is
-- recorded. Mirrors the oracle's @run_transactional_step@ for the debounce
-- lane. A failure before anything is written commits nothing, so the replay
-- runs the work again.
  getDeduplicationKeyHolder env queueName deduplicationId = do
    result <-
      runSession
        env
        "get_deduplication_key_holder"
        (Session.statement (queueName, deduplicationId) Statements.deduplicationHolderStatement)
    pure (fmap (fmap WorkflowId) result)
  deleteQueue env name = do
    result <- runSession env "delete_queue" (Session.statement name Statements.queueDeleteStatement)
    pure (() <$ result)
  createSchedule env new caller = do
    -- Generated once, outside the retry: a retry after a lost commit
    -- acknowledgement must find its own row rather than insert a second
    -- one under a fresh id.
    scheduleId <- maybe (UUID.toText <$> UUID.V4.nextRandom) pure new.newScheduleId
    let owner = new.newScheduleApplicationName <|> env.psdbApplicationName
        work = do
          holder <- Tx.statement new.newScheduleName Statements.scheduleOwnerStatement
          case resolveOwner "Schedule" new.newScheduleName owner holder of
            Left err -> pure (Left err)
            Right resolvedOwner -> do
              Tx.statement (scheduleInsertParams new scheduleId resolvedOwner) Statements.scheduleInsertStatement
              pure (Right ())
        collide result = case result of
          Left err      -> Left (scheduleCollision scheduleId new.newScheduleName err)
          Right outcome -> outcome
    case caller of
      Just (callerWid, callerStep) -> do
        startedAt <- timestampNow
        result <- runCallerStep env createScheduleStepName callerWid callerStep startedAt work
        pure (case result of
          Left err -> Left (scheduleCollision scheduleId new.newScheduleName err)
          Right () -> Right ())
      Nothing -> do
        result <- runTransaction env "create_schedule" work
        pure (collide result)
  upsertSchedule env new caller = do
    scheduleId <- maybe (UUID.toText <$> UUID.V4.nextRandom) pure new.newScheduleId
    case caller of
      Just (callerWid, callerStep) -> do
        startedAt <- timestampNow
        runCallerStep env upsertScheduleStepName callerWid callerStep startedAt
          (upsertScheduleOn env new scheduleId)
      Nothing -> do
        result <- runTransaction env "upsert_schedule" (upsertScheduleOn env new scheduleId)
        pure (join result)
  applySchedules env schedules = do
    ids <- traverse (maybe (UUID.toText <$> UUID.V4.nextRandom) pure . (.newScheduleId)) schedules
    result <- runTransaction env "apply_schedules" (applySchedulesTx env (zip schedules ids))
    pure (join result)
  getSchedule env name caller =
    case caller of
      Just (callerWid, callerStep) -> do
        startedAt <- timestampNow
        runCallerStep env getScheduleStepName callerWid callerStep startedAt $ do
          row <- Tx.statement name Statements.scheduleByNameStatement
          pure (traverse scheduleRecordFromRow row)
      Nothing -> do
        result <- runSession env "get_schedule" (Session.statement name Statements.scheduleByNameStatement)
        pure (result >>= traverse scheduleRecordFromRow)
  listSchedules env scheduleFilter caller =
    case caller of
      Just (callerWid, callerStep) -> do
        startedAt <- timestampNow
        runCallerStep env listSchedulesStepName callerWid callerStep startedAt $ do
          rows <- Tx.statement (scheduleListParams env scheduleFilter) Statements.scheduleListStatement
          pure (traverse scheduleRecordFromRow rows)
      Nothing -> do
        result <-
          runSession
            env
            "list_schedules"
            (Session.statement (scheduleListParams env scheduleFilter) Statements.scheduleListStatement)
        pure (result >>= traverse scheduleRecordFromRow)
  updateSchedule env name update caller =
    case caller of
      Just (callerWid, callerStep) -> do
        startedAt <- timestampNow
        runCallerStep env updateScheduleStepName callerWid callerStep startedAt $ do
          -- An empty update still has to say whether the schedule exists, so
          -- it becomes a read rather than an early return: silence would
          -- report a typo as success.
          if isScheduleUpdateEmpty update
            then do
              exists <- Tx.statement name Statements.scheduleExistsStatement
              pure (maybe (missingSchedule name) (const (Right ())) exists)
            else do
              changed <- Tx.statement (scheduleUpdateParams name update) Statements.scheduleUpdateStatement
              pure (if changed > 0 then Right () else missingSchedule name)
      Nothing -> do
        result <- runTransaction env "update_schedule" $ do
          if isScheduleUpdateEmpty update
            then do
              exists <- Tx.statement name Statements.scheduleExistsStatement
              pure (maybe (missingSchedule name) (const (Right ())) exists)
            else do
              changed <- Tx.statement (scheduleUpdateParams name update) Statements.scheduleUpdateStatement
              pure (if changed > 0 then Right () else missingSchedule name)
        pure (join result)
  setScheduleStatus env name status caller =
    case caller of
      Just (callerWid, callerStep) -> do
        startedAt <- timestampNow
        let stepName = case status of
              Paused -> pauseScheduleStepName
              Active -> resumeScheduleStepName
        runCallerStep env stepName callerWid callerStep startedAt $ do
          updated <- Tx.statement (name, scheduleStatusText status) Statements.setScheduleStatusStatement
          pure (if updated > 0 then Right () else missingSchedule name)
      Nothing -> do
        result <- runTransaction env "set_schedule_status" $ do
          updated <- Tx.statement (name, scheduleStatusText status) Statements.setScheduleStatusStatement
          pure (if updated > 0 then Right () else missingSchedule name)
        pure (join result)
  updateScheduleLastFiredAt env name lastFiredAt = do
    result <-
      runSession
        env
        "update_schedule_last_fired_at"
        (Session.statement (name, timestampToIso8601 lastFiredAt) Statements.updateScheduleLastFiredAtStatement)
    pure (() <$ result)
  deleteSchedule env name caller =
    case caller of
      Just (callerWid, callerStep) -> do
        startedAt <- timestampNow
        runCallerStep env deleteScheduleStepName callerWid callerStep startedAt $ do
          Tx.statement name Statements.deleteScheduleStatement
          pure (Right ())
      Nothing -> do
        result <- runSession env "delete_schedule" (Session.statement name Statements.deleteScheduleStatement)
        pure (() <$ result)
  renameApplication env source newName batching
    | not (isValidApplicationName newName) =
        pure (Left (invalidInput "new_name" "must be 3 to 30 characters of lowercase letters, digits, dashes and underscores"))
    | renameFromApplication source == Just newName =
        pure (Left (invalidInput "new_name" ("\"" <> newName <> "\" already holds that name")))
    | batching == Batched 0 =
        pure (Left (invalidInput "batching" "a batch must hold at least one workflow"))
    | otherwise = do
        let renamedFrom = renameFromApplication source
        inFlight <-
          runTransaction
            env
            "rename_application"
            ( do
                queues <- Tx.statement (newName, renamedFrom) (Statements.renameRowsStatement "dbos.queues" source "")
                schedules <- Tx.statement (newName, renamedFrom) (Statements.renameRowsStatement "dbos.workflow_schedules" source "")
                versions <- Tx.statement (newName, renamedFrom) (Statements.renameRowsStatement "dbos.application_versions" source "")
                workflows <-
                  Tx.statement
                    (newName, renamedFrom)
                    (Statements.renameRowsStatement "dbos.workflow_status" source " and status in ('PENDING', 'ENQUEUED', 'DELAYED')")
                pure (queues, schedules, versions, workflows)
            )
        case inFlight of
          Left err -> pure (Left err)
          Right (queues, schedules, versions, workflows) -> do
            terminal <- renameInBatches env "dbos.workflow_status" source newName batching
            steps <- renameInBatches env "dbos.operation_outputs" source newName batching
            pure $ case (terminal, steps) of
              (Left err, _) -> Left err
              (_, Left err) -> Left err
              (Right terminalRows, Right stepRows) ->
                Right
                  ApplicationRowCounts
                    { rowCountQueues = fromIntegral queues,
                      rowCountSchedules = fromIntegral schedules,
                      rowCountVersions = fromIntegral versions,
                      rowCountWorkflows = fromIntegral (workflows + terminalRows),
                      rowCountSteps = fromIntegral stepRows
                    }
  recordChildWorkflow env parent child stepId stepName startedAt =
    case child of
      Types.WorkflowId childText | Text.null childText -> pure (Left (invalidInput "child_workflow_id" "must not be empty"))
      Types.WorkflowId childText -> do
        now <- timestampToEpochMs <$> timestampNow
        let params =
              Statements.RecordChildWorkflowParams
                { recordChildParentId = unwrap parent,
                  recordChildChildId = childText,
                  recordChildStepId = stepId,
                  recordChildStepName = stepName,
                  recordChildStartedAt = timestampToEpochMs <$> startedAt,
                  recordChildCompletedAt = now <$ startedAt,
                  recordChildApplicationName = env.psdbApplicationName
                }
        result <- runSession env "record_child_workflow" (Session.statement () (Statements.recordChildWorkflowStatement params))
        pure $ case result of
          Left err -> Left err
          -- The same child is an idempotent retry; a different one (or none,
          -- where the row already held a non-child step) is nondeterminism.
          Right stored
            | stored == Just childText -> Right ()
            | otherwise -> Left (StepAlreadyRecorded {workflowId = unwrap parent, stepId = stepId})
    where
      unwrap (Types.WorkflowId widText) = widText
  recordChildResult env parent stepId child outcome serialization timing
    | Text.null parentText = pure (Left (invalidInput "workflow_id" "must not be empty"))
    | stepId < 0 = pure (Left (invalidInput "step_id" "must not be negative"))
    | otherwise = do
        let (output, errorText) = outcomeColumns outcome
            params =
              Statements.RecordStepParams
                { recordStepWorkflowId = parentText,
                  recordStepStepId = stepId,
                  recordStepStepName = Types.getResultStepName,
                  recordStepOutput = output,
                  recordStepError = errorText,
                  recordStepSerialization = serialization,
                  recordStepStartedAt = timestampToEpochMs . (.stepTimingStartedAt) <$> timing,
                  recordStepCompletedAt = timestampToEpochMs . (.stepTimingCompletedAt) <$> timing,
                  recordStepApplicationName = env.psdbApplicationName,
                  recordStepChildWorkflowId = Just childText
                }
        result <- runSession env "record_child_result" (Statements.recordStepSession params)
        case result of
          Left err -> pure (Left err)
          Right stored -> do
            let ours = stored == (timestampToEpochMs . (.stepTimingCompletedAt) <$> timing)
            if not ours
              then pure (Left (StepAlreadyRecorded {workflowId = parentText, stepId = stepId}))
              else case env.psdbExecutorId of
                Nothing -> pure (Right ())
                Just executorId -> do
                  claimed <- runSession env "record_child_result" (Statements.claimExecutorSession parentText executorId)
                  pure (() <$ claimed)
    where
      Types.WorkflowId parentText = parent
      Types.WorkflowId childText = child

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

fetchWorkflowStatus ::
  Pool.Pool ->
  WorkflowId ->
  IO (Maybe WorkflowStatus)
fetchWorkflowStatus pool workflowId =
  runDbOrFail pool (fetchWorkflowStatusSession workflowId)

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

recordOperationOutput ::
  Pool.Pool ->
  WorkflowId ->
  Int ->
  Text ->
  SerializedWorkflowValue ->
  IO ()
recordOperationOutput pool workflowId operationId operationName output =
  runDbOrFail pool (recordOperationOutputSession workflowId operationId operationName output)

recordOperationError ::
  Pool.Pool ->
  WorkflowId ->
  Int ->
  Text ->
  SerializedWorkflowValue ->
  IO ()
recordOperationError pool workflowId operationId operationName errorValue =
  runDbOrFail pool (recordOperationErrorSession workflowId operationId operationName errorValue)

legacySetEvent ::
  Pool.Pool ->
  WorkflowId ->
  Text ->
  SerializedWorkflowValue ->
  IO ()
legacySetEvent pool workflowId key value =
  runDbOrFail pool (setEventSession workflowId key value)

legacyGetEvent ::
  Pool.Pool ->
  WorkflowId ->
  Text ->
  IO (Maybe SerializedWorkflowValue)
legacyGetEvent pool workflowId key =
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
      found <- legacyGetEvent pool workflowId key
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
legacySendMessage ::
  Pool.Pool ->
  SendMessage ->
  IO ()
legacySendMessage pool message =
  legacySendMessages pool [message]

-- | Deliver a batch in one statement, so it is all-or-nothing. Fallback ids
-- are generated once, so a retry after a lost acknowledgement does not
-- deliver everything a second time.
legacySendMessages ::
  Pool.Pool ->
  [SendMessage] ->
  IO ()
legacySendMessages pool messages = do
  fallbacks <- traverse (const (fmap (MessageUUID . UUID.toText) UUID.V4.nextRandom)) messages
  runDbOrFail pool (sendMessagesSession fallbacks messages)

-- | Block until a message is waiting on the topic, then take the oldest one
-- and record it as the @DBOS.recv@ step in one statement. A replay returns
-- the message the first run took and takes no other; absence at the deadline
-- is recorded, so a replay of a timeout stays a timeout.
recvMessage ::
  Pool.Pool ->
  WorkflowId ->
  Int ->
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
legacyReenqueueForRecovery ::
  Pool.Pool ->
  ExecutorId ->
  ApplicationVersion ->
  QueueName ->
  IO [WorkflowId]
legacyReenqueueForRecovery pool executorId applicationVersion recoveryQueue =
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
  -- Only pass a password when PGPASSWORD names one; otherwise the connection
  -- uses libpq's own pgpass/service resolution. No credential is invented.
  password <- lookupEnv "PGPASSWORD"
  pure $
    mconcat
      [ Connection.hostAndPort
          (maybe "127.0.0.1" toText host)
          (maybe 5432 parsePort port),
        Connection.dbname (maybe "dbos" toText dbname),
        Connection.user (maybe "postgres" toText user),
        maybe mempty (Connection.password . toText) password
      ]

parsePort :: String -> Word16
parsePort raw =
  maybe 5432 fromIntegral (readMaybe raw :: Maybe Int)

toText :: String -> Text
toText =
  Text.pack
