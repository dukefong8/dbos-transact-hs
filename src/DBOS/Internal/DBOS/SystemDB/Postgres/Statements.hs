{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TypeFamilies        #-}
{-# OPTIONS_GHC -Wno-orphans #-}

-- | Statements for the Postgres backend: one statement per query the
-- @SystemDB@ instance runs, with explicit column lists. The port's own seam,
-- not a Rust module counterpart: Rust builds these queries with @format!@
-- over quoted identifiers, which Haskell spells as static SQL over the fixed
-- @dbos@ schema (ADR-0010).
--
-- Preference order for the SQL, per the port's rule: @[typedSql| ... |]@
-- wherever the shape allows it (ADR-0007 discipline, compile-time checking),
-- then plain hasql statements with the same explicit column lists — which is
-- what typedSql itself compiles to — for what typedSql cannot express: wide
-- rows (@workflow_status@ selects 38 columns and @SqlRow@ caps at 16) and
-- filters whose guards are @CASE@-and-null expressions rather than dynamic
-- SQL. @hasql-dynamic-statements@ is not used here at all.
--
-- Sessions decode rows into total raw shapes; fallible mapping
-- (@workflow_from_row@ and friends) lives in @DBOS.SystemDB.Postgres@.
-- Parameters are plain 'Text', as Rust's @&str@.
module DBOS.SystemDB.Postgres.Statements
  ( WorkflowRowRaw (..),
    WorkflowListParams (..),
    WorkflowStatusRaw,
    WorkflowInitRaw,
    getWorkflowSession,
    listWorkflowsSession,
    workflowStatusSession,
    firstSettledSession,
    settledIdsSession,
    recordWorkflowOutcomeStatement,
    recordWorkflowOutputStatement,
    StepCheckRaw (..),
    checkStepSession,
    checkStepStatement,
    RecordStepParams (..),
    recordStepSession,
    recordStepStatement,
    VersionRowRaw (..),
    versionHolderSession,
    versionClaimSession,
    versionInsertSession,
    listApplicationVersionsSession,
    latestApplicationVersionSession,
    updateVersionTimestampSession,
    directForksSession,
    renameRowsStatement,
    renameBatchBoundStatement,
    renameBatchRangeStatement,
    StepRowRaw (..),
    listStepsSession,
    ForkParams (..),
    forkTx,
    forkPointsStatement,
    transitionDelayedSession,
    cancelBatchSession,
    existingWorkflowsSession,
    resumeWorkflowsSession,
    deleteWorkflowsSession,
    RecvTxResult (..),
    EncodedValueRaw (..),
    RecvParams (..),
    recvProbeSession,
    recvTx,
    SendMessagesParams (..),
    SendStep (..),
    sendMessagesTx,
    SetEventParams (..),
    setEventTx,
    EventValueRaw (..),
    eventValueSession,
    GetEventRecordParams (..),
    getEventRecordTx,
    claimExecutorSession,
    setWorkflowDelaySession,
    clearQueueAssignmentSession,
    updateWorkflowAttributesSession,
    reenqueueForRecoverySession,
    InitWorkflowParams (..),
    initWorkflowInputStatement,
    initWorkflowStatement,
    parkWorkflowSession,
    descendantsSession,
    QueueRowRaw (..),
    queueByNameStatement,
    queueByNameForUpdateStatement,
    queueListStatement,
    queueOwnerStatement,
    QueueUpdateParams (..),
    queueUpdateStatement,
    dequeueRateLimitCountStatement,
    dequeuePartitionRateLimitCountStatement,
    dequeueConcurrencyCountStatement,
    dequeuePartitionConcurrencyCountStatement,
    latestVersionNameStatement,
    dequeueCandidatesNowaitStatement,
    dequeueCandidatesSkipLockedStatement,
    dequeueClaimStatement,
    partitionSweepCandidatesStatement,
    partitionSweepRandomCandidatesStatement,
    partitionSweepLockStatement,
    partitionSweepFlipStatement,
    DebounceBounceParams (..),
    debounceBounceStatement,
    debounceInputStatement,
    DebounceHolderRaw (..),
    debounceHolderStatement,
    NotificationRecordRaw (..),
    allNotificationsStatement,
    EventRecordRaw (..),
    allEventsStatement,
    RecordChildWorkflowParams (..),
    recordChildWorkflowStatement,
    QueueInsertParams (..),
    queueInsertStatement,
    queueDeleteStatement,
    queuePartitionsStatement,
    deduplicationHolderStatement,
    ScheduleRowRaw (..),
    scheduleOwnerStatement,
    ScheduleInsertParams (..),
    scheduleInsertStatement,
    scheduleUpsertStatement,
    scheduleByNameStatement,
    ScheduleListParams (..),
    scheduleListStatement,
    scheduleExistsStatement,
    ScheduleUpdateParams (..),
    scheduleUpdateStatement,
    setScheduleStatusStatement,
    updateScheduleLastFiredAtStatement,
    deleteScheduleStatement,
  )
where

import Data.Functor.Contravariant (contramap)
import Data.Int (Int32, Int64)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Word (Word32)
import DBOS.Prelude
import DBOS.SystemDB.Types (OnExistingQueue (..), RenameFrom (..))
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import Hasql.Transaction qualified as Tx
import IHP.TypedSql.Hasql (sqlExecTypedSession, sqlExecTypedStatement, sqlQueryTypedSession, sqlQueryTypedStatement, typedSql)
import IHP.TypedSql.Id (Id' (..), PrimaryKey)
import IHP.TypedSql.RowType (SqlRow)

-- | Primary keys are plain text ids in the DBOS system schema, as everywhere
-- else in this port.
type instance PrimaryKey "workflow_status" = Text

type instance PrimaryKey "operation_outputs" = Int

type instance PrimaryKey "notifications" = Text

-- | A @workflow_status@ row as decoded: everything total, with nullability
-- following what Rust reads (@Option@ columns stay 'Maybe', directly-read
-- columns do not). Fallible conversions (status spelling, roles JSON,
-- timeout duration) are left to the mapping in @DBOS.SystemDB.Postgres@.
data WorkflowRowRaw = WorkflowRowRaw
  { workflow_uuid              :: Text,
    status                     :: Text,
    name                       :: Maybe Text,
    class_name                 :: Maybe Text,
    config_name                :: Maybe Text,
    serialization              :: Maybe Text,
    executor_id                :: Maybe Text,
    application_version        :: Maybe Text,
    recovery_attempts          :: Maybe Int64,
    queue_name                 :: Maybe Text,
    created_at                 :: Int64,
    updated_at                 :: Int64,
    started_at_epoch_ms        :: Maybe Int64,
    completed_at               :: Maybe Int64,
    forked_from                :: Maybe Text,
    parent_workflow_id         :: Maybe Text,
    was_forked_from            :: Maybe Bool,
    owner_xid                  :: Maybe Text,
    application_id             :: Maybe Text,
    authenticated_user         :: Maybe Text,
    authenticated_roles        :: Maybe Text,
    assumed_role               :: Maybe Text,
    request                    :: Maybe Text,
    deduplication_id           :: Maybe Text,
    priority                   :: Maybe Int,
    queue_partition_key        :: Maybe Text,
    rate_limited               :: Maybe Bool,
    schedule_name              :: Maybe Text,
    workflow_timeout_ms        :: Maybe Int64,
    workflow_deadline_epoch_ms :: Maybe Int64,
    delay_until_epoch_ms       :: Maybe Int64,
    debounce_deadline_epoch_ms :: Maybe Int64,
    is_debounced               :: Maybe Bool,
    application_name           :: Maybe Text,
    attributes                 :: Maybe Text,
    inputs                     :: Maybe Text,
    output                     :: Maybe Text,
    error                      :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | Reads one workflow with its payloads, or nothing if there is no such
-- id. Mirrors @get_workflow@: @WORKFLOW_COLUMNS@ plus all three payload
-- columns, each read from its payload table first and the legacy
-- @workflow_status@ column second, as every SDK does.
getWorkflowSession :: Text -> Session.Session (Maybe WorkflowRowRaw)
getWorkflowSession workflowId =
  Session.statement workflowId (Statement.preparable sql encoder decoder)
  where
    sql =
      "select " <> workflowColumns <> ", "
        <> payloadColumn "workflow_input" "inputs"
        <> ", "
        <> payloadColumn "workflow_output" "output"
        <> ", "
        <> payloadColumn "workflow_output" "error"
        <> " \
      \from dbos.workflow_status where workflow_uuid = $1"
    encoder = Encoders.param (Encoders.nonNullable Encoders.text)
    decoder = Decoders.rowMaybe workflowRowDecoder

-- | One payload column as the payload table's value falling back to the
-- legacy @workflow_status@ column, for a query whose @FROM@ is
-- @workflow_status@ unaliased. A correlated subquery on the primary key
-- rather than a join, so @workflow_status@ stays the only table in scope
-- and no other column needs qualifying. Mirrors the oracle's
-- @payload_column@.
payloadColumn :: Text -> Text -> Text
payloadColumn table name =
  "coalesce((select p."
    <> name
    <> " from dbos."
    <> table
    <> " p where p.workflow_uuid = dbos.workflow_status.workflow_uuid), "
    <> name
    <> ") as "
    <> name

-- | @WORKFLOW_COLUMNS@: every column @workflow_from_row@ reads, in order.
-- Shared by every workflow read so the lists cannot drift.
workflowColumns :: Text
workflowColumns =
  "workflow_uuid, status, name, class_name, config_name, \
  \serialization, executor_id, application_version, recovery_attempts, \
  \queue_name, created_at, updated_at, started_at_epoch_ms, completed_at, \
  \forked_from, parent_workflow_id, was_forked_from, owner_xid, \
  \application_id, authenticated_user, authenticated_roles, assumed_role, \
  \request, deduplication_id, priority, queue_partition_key, rate_limited, \
  \schedule_name, workflow_timeout_ms, workflow_deadline_epoch_ms, \
  \delay_until_epoch_ms, debounce_deadline_epoch_ms, is_debounced, \
  \application_name, attributes::text as attributes"

-- | The shared @workflow_status@ row decoder: @WORKFLOW_COLUMNS@ plus
-- payloads, in the order every workflow read selects them.
workflowRowDecoder :: Decoders.Row WorkflowRowRaw
workflowRowDecoder =
  WorkflowRowRaw
    <$> Decoders.column (Decoders.nonNullable Decoders.text)
    <*> Decoders.column (Decoders.nonNullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.varchar)
    <*> Decoders.column (Decoders.nullable Decoders.varchar)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.int8)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nonNullable Decoders.int8)
    <*> Decoders.column (Decoders.nonNullable Decoders.int8)
    <*> Decoders.column (Decoders.nullable Decoders.int8)
    <*> Decoders.column (Decoders.nullable Decoders.int8)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.bool)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> (fmap fromIntegral <$> Decoders.column (Decoders.nullable Decoders.int4))
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.bool)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.int8)
    <*> Decoders.column (Decoders.nullable Decoders.int8)
    <*> Decoders.column (Decoders.nullable Decoders.int8)
    <*> Decoders.column (Decoders.nullable Decoders.int8)
    <*> Decoders.column (Decoders.nullable Decoders.bool)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)

-- | Where a listing's narrowing has been resolved to what the SQL binds:
-- empty lists mean "this filter is absent" (@cardinality(...) = 0@), and the
-- application scoping is pre-decided because it depends on both the filter
-- and the handle. Mirrors the parameters @list_workflows@ builds in Rust.
data WorkflowListParams = WorkflowListParams
  { listLoadInput           :: Bool,
    listLoadOutput          :: Bool,
    listWorkflowIds         :: [Text],
    listWorkflowIdPrefixes  :: [Text],
    listNamedApplications   :: Maybe [Text],
    listUnsetApplication    :: Maybe Text,
    listNames               :: [Text],
    listClassNames          :: [Text],
    listConfigNames         :: [Text],
    listStatuses            :: [Text],
    listApplicationVersions :: [Text],
    listExecutorIds         :: [Text],
    listAuthenticatedUsers  :: [Text],
    listQueueNames          :: [Text],
    listScheduleNames       :: [Text],
    listDeduplicationIds    :: [Text],
    listParentWorkflowIds   :: [Text],
    listForkedFrom          :: [Text],
    listQueuesOnly          :: Bool,
    listIsFork              :: Maybe Bool,
    listHasParent           :: Maybe Bool,
    listWasForkedFrom       :: Maybe Bool,
    listIsDebounced         :: Maybe Bool,
    listCreatedAfter        :: Maybe Int64,
    listCreatedBefore       :: Maybe Int64,
    listCompletedAfter      :: Maybe Int64,
    listCompletedBefore     :: Maybe Int64,
    listStartedAfter        :: Maybe Int64,
    listStartedBefore       :: Maybe Int64,
    listAttributes          :: Maybe Text,
    listSortDesc            :: Bool,
    listLimit               :: Maybe Int64,
    listOffset              :: Maybe Int64
  }
  deriving stock (Eq, Show)

-- | Lists workflows: every filter is an ANDed guard, so the SQL is one
-- static statement whatever the filter says. Mirrors @list_workflows@
-- (Rust builds the same guards dynamically; the CASE-and-null-guard form
-- keeps typedSql's static-SQL discipline, see ADR-0010):
--
-- * @cardinality($n) = 0@ for an absent list, @= any($n)@ otherwise — one
--   placeholder per list, whatever its length.
-- * @$n is null or ...@ for absent scalars, including @LIMIT@\/@OFFSET@,
--   where Postgres treats @NULL@ as \"all\" and \"none\".
-- * @case when $n then column end@ declines payloads as typed NULLs.
-- * @created_at@ alone orders the rows, as every implementation does.
listWorkflowsSession :: WorkflowListParams -> Session.Session [WorkflowRowRaw]
listWorkflowsSession params =
  Session.statement params (Statement.preparable listWorkflowsSql listWorkflowsEncoder listWorkflowsDecoder)

listWorkflowsSql :: Text
listWorkflowsSql =
  "select "
    <> workflowColumns
    <> ", \
       \case when $1 then coalesce((select p.inputs from dbos.workflow_input p where p.workflow_uuid = dbos.workflow_status.workflow_uuid), inputs) end as inputs, \
       \case when $2 then coalesce((select p.output from dbos.workflow_output p where p.workflow_uuid = dbos.workflow_status.workflow_uuid), output) end as output, \
       \case when $2 then coalesce((select p.error from dbos.workflow_output p where p.workflow_uuid = dbos.workflow_status.workflow_uuid), error) end as error \
       \from dbos.workflow_status \
       \where \
       \(cardinality($3::text[]) = 0 or workflow_uuid = any($3)) \
       \and (cardinality($4::text[]) = 0 or workflow_uuid like any($4)) \
       \and ($5::text[] is null or application_name = any($5) or application_name is null) \
       \and ($6::text is null or application_name = $6 or application_name is null) \
       \and (cardinality($7::text[]) = 0 or name = any($7)) \
       \and (cardinality($8::text[]) = 0 or class_name = any($8)) \
       \and (cardinality($9::text[]) = 0 or config_name = any($9)) \
       \and (cardinality($10::text[]) = 0 or status = any($10)) \
       \and (cardinality($11::text[]) = 0 or application_version = any($11)) \
       \and (cardinality($12::text[]) = 0 or executor_id = any($12)) \
       \and (cardinality($13::text[]) = 0 or authenticated_user = any($13)) \
       \and (cardinality($14::text[]) = 0 or queue_name = any($14)) \
       \and (cardinality($15::text[]) = 0 or schedule_name = any($15)) \
       \and (cardinality($16::text[]) = 0 or deduplication_id = any($16)) \
       \and (cardinality($17::text[]) = 0 or parent_workflow_id = any($17)) \
       \and (cardinality($18::text[]) = 0 or forked_from = any($18)) \
       \and (not $19 or queue_name is not null) \
       \and ($20::boolean is null or (forked_from is not null) = $20) \
       \and ($21::boolean is null or (parent_workflow_id is not null) = $21) \
       \and ($22::boolean is null or was_forked_from = $22) \
       \and ($23::boolean is null or is_debounced = $23) \
       \and ($24::bigint is null or created_at >= $24) \
       \and ($25::bigint is null or created_at <= $25) \
       \and ($26::bigint is null or completed_at >= $26) \
       \and ($27::bigint is null or completed_at <= $27) \
       \and ($28::bigint is null or started_at_epoch_ms >= $28) \
       \and ($29::bigint is null or started_at_epoch_ms <= $29) \
       \and ($30::text is null or attributes @> $30::jsonb) \
       \order by \
       \case when $31 then created_at end desc, \
       \case when not $31 then created_at end asc \
       \limit $32 offset $33"

listWorkflowsEncoder :: Encoders.Params WorkflowListParams
listWorkflowsEncoder =
  mconcat
    [ contramap (.listLoadInput) boolParam, -- $1
      contramap (.listLoadOutput) boolParam, -- $2
      contramap (.listWorkflowIds) textArrayParam, -- $3
      contramap (.listWorkflowIdPrefixes) textArrayParam, -- $4
      contramap (.listNamedApplications) maybeTextArrayParam, -- $5
      contramap (.listUnsetApplication) maybeTextParam, -- $6
      contramap (.listNames) textArrayParam, -- $7
      contramap (.listClassNames) textArrayParam, -- $8
      contramap (.listConfigNames) textArrayParam, -- $9
      contramap (.listStatuses) textArrayParam, -- $10
      contramap (.listApplicationVersions) textArrayParam, -- $11
      contramap (.listExecutorIds) textArrayParam, -- $12
      contramap (.listAuthenticatedUsers) textArrayParam, -- $13
      contramap (.listQueueNames) textArrayParam, -- $14
      contramap (.listScheduleNames) textArrayParam, -- $15
      contramap (.listDeduplicationIds) textArrayParam, -- $16
      contramap (.listParentWorkflowIds) textArrayParam, -- $17
      contramap (.listForkedFrom) textArrayParam, -- $18
      contramap (.listQueuesOnly) boolParam, -- $19
      contramap (.listIsFork) maybeBoolParam, -- $20
      contramap (.listHasParent) maybeBoolParam, -- $21
      contramap (.listWasForkedFrom) maybeBoolParam, -- $22
      contramap (.listIsDebounced) maybeBoolParam, -- $23
      contramap (.listCreatedAfter) maybeInt8Param, -- $24
      contramap (.listCreatedBefore) maybeInt8Param, -- $25
      contramap (.listCompletedAfter) maybeInt8Param, -- $26
      contramap (.listCompletedBefore) maybeInt8Param, -- $27
      contramap (.listStartedAfter) maybeInt8Param, -- $28
      contramap (.listStartedBefore) maybeInt8Param, -- $29
      contramap (.listAttributes) maybeTextParam, -- $30
      contramap (.listSortDesc) boolParam, -- $31
      contramap (.listLimit) maybeInt8Param, -- $32
      contramap (.listOffset) maybeInt8Param -- $33
    ]
  where
    boolParam = Encoders.param (Encoders.nonNullable Encoders.bool)
    maybeBoolParam = Encoders.param (Encoders.nullable Encoders.bool)
    maybeInt8Param = Encoders.param (Encoders.nullable Encoders.int8)
    maybeTextParam = Encoders.param (Encoders.nullable Encoders.text)
    textArrayParam = Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text)))
    maybeTextArrayParam = Encoders.param (Encoders.nullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text)))

listWorkflowsDecoder :: Decoders.Result [WorkflowRowRaw]
listWorkflowsDecoder = Decoders.rowList workflowRowDecoder

-- | The narrow read a wait makes: four columns and the attempt count, not
-- the whole row, because this runs once per interval for as long as the
-- caller waits. Mirrors the oracle's @await_workflow_result@ select.
type WorkflowStatusRaw =
  SqlRow
    '[ '("status", Maybe Text),
       '("output", Maybe Text),
       '("error", Maybe Text),
       '("serialization", Maybe Text),
       '("recovery_attempts", Maybe Int64)
     ]

-- | Reads the columns a wait settles on, or nothing if the row is absent.
workflowStatusSession :: Text -> Session.Session (Maybe WorkflowStatusRaw)
workflowStatusSession workflowId =
  sqlQueryTypedSession [typedSql|
    select status, coalesce((select p.output from dbos.workflow_output p where p.workflow_uuid = dbos.workflow_status.workflow_uuid), output) as output, coalesce((select p.error from dbos.workflow_output p where p.workflow_uuid = dbos.workflow_status.workflow_uuid), error) as error, serialization, recovery_attempts
    from dbos.workflow_status
    where workflow_uuid = ${workflowId}
  |]

-- | The first settled member of a set, as the id alone. One row, because one
-- answer ends the wait; unsettled means @PENDING@, @ENQUEUED@ or @DELAYED@,
-- the same three every implementation excludes. Mirrors
-- @await_first_workflow_id@.
firstSettledSession :: [Text] -> Session.Session (Maybe Text)
firstSettledSession workflowIds =
  fmap unwrapId
    <$> sqlQueryTypedSession [typedSql|
      select workflow_uuid
      from dbos.workflow_status
      where workflow_uuid = any(${workflowIds})
        and status not in ('PENDING', 'ENQUEUED', 'DELAYED')
      limit 1
    |]

-- | Every settled member of a set: the next pass has to ask about the rest,
-- so a count would not do. Mirrors @await_workflow_ids@.
settledIdsSession :: [Text] -> Session.Session [Text]
settledIdsSession workflowIds =
  map unwrapId
    <$> sqlQueryTypedSession [typedSql|
      select workflow_uuid
      from dbos.workflow_status
      where workflow_uuid = any(${workflowIds})
        and status not in ('PENDING', 'ENQUEUED', 'DELAYED')
    |]

-- | The primary key out of its @Id'@ wrapper: this layer's ids are plain
-- text, and the wrapper is typedSql's convention for a key column.
unwrapId :: Id' "workflow_status" -> Text
unwrapId (Id key) = key

-- | Records a terminal outcome, but only while the workflow is still
-- pending: the @status = 'PENDING'@ gate is the whole mechanism, so a
-- superseded executor updates nothing and learns it lost rather than
-- clobbering the winner's result. Finishing also releases the deduplication
-- key, whose unique index spans every status — a key left on a finished row
-- would be held forever. The legacy @output@ and @error@ columns are cleared
-- rather than written: readers fall back to them, so a stale error must not
-- survive beside a new outcome. Returns the rows affected. Mirrors
-- @record_workflow_outcome@'s status update.
recordWorkflowOutcomeStatement :: Text -> Text -> Maybe Text -> Maybe Text -> Statement.Statement () Int64
recordWorkflowOutcomeStatement workflowId statusText output errorText =
  sqlExecTypedStatement [typedSql|
    update dbos.workflow_status
    set status = ${statusText},
      output = null,
      error = null,
      updated_at = (extract(epoch from now()) * 1000)::bigint,
      completed_at = (extract(epoch from now()) * 1000)::bigint,
      deduplication_id = null
    where workflow_uuid = ${workflowId} and status = 'PENDING'
  |]

-- | Records a finished workflow's outcome in its own table, behind a status
-- change that landed: an outcome the gate refused leaves no orphan payload.
-- An upsert, because a resumed or rewound workflow finishes more than once.
-- Mirrors @record_workflow_outcome@'s payload write.
recordWorkflowOutputStatement :: Text -> Maybe Text -> Maybe Text -> Statement.Statement () Int64
recordWorkflowOutputStatement workflowId output errorText =
  sqlExecTypedStatement [typedSql|
    insert into dbos.workflow_output (workflow_uuid, output, error)
    values (${workflowId}, ${output}, ${errorText})
    on conflict (workflow_uuid) do update
    set output = excluded.output, error = excluded.error
  |]

-- | Everything @init_workflow@ binds, with the derived values already
-- resolved: the initial status, the attempt count and increment, the owner
-- identity, the clock reading, and the delay turned into an instant. Mirrors
-- the oracle's bind list, in its order.
data InitWorkflowParams = InitWorkflowParams
  { initParamWorkflowId         :: Text,
    initParamStatus             :: Text,
    initParamName               :: Maybe Text,
    initParamClassName          :: Maybe Text,
    initParamConfigName         :: Maybe Text,
    initParamQueueName          :: Maybe Text,
    initParamDeduplicationId    :: Maybe Text,
    initParamPriority           :: Int,
    initParamQueuePartitionKey  :: Maybe Text,
    initParamDelayUntil         :: Maybe Int64,
    initParamAuthenticatedUser  :: Maybe Text,
    initParamAssumedRole        :: Maybe Text,
    initParamAuthenticatedRoles :: Maybe Text,
    initParamExecutorId         :: Maybe Text,
    initParamApplicationVersion :: Maybe Text,
    initParamApplicationId      :: Maybe Text,
    initParamCreatedAt          :: Int64,
    initParamUpdatedAt          :: Int64,
    initParamInitialAttempts    :: Int64,
    initParamTimeoutMs          :: Maybe Int64,
    initParamDeadline           :: Maybe Int64,
    initParamParentWorkflowId   :: Maybe Text,
    initParamOwnerXid           :: Text,
    initParamSerialization      :: Maybe Text,
    initParamAttributes         :: Maybe Text,
    initParamScheduleName       :: Maybe Text,
    initParamDebounceDeadline   :: Maybe Int64,
    initParamIsDebounced        :: Bool,
    initParamApplicationName    :: Maybe Text,
    initParamIncrement          :: Int64,
    initParamClaiming           :: Bool
  }
  deriving stock (Eq, Show)

-- | The columns @init_workflow@ reads back: enough to decide whether this
-- caller may run the workflow, and what the row already holds.
type WorkflowInitRaw =
  SqlRow
    '[ '("recovery_attempts", Maybe Int64),
       '("status", Maybe Text),
       '("name", Maybe Text),
       '("class_name", Maybe Text),
       '("config_name", Maybe Text),
       '("queue_name", Maybe Text),
       '("workflow_deadline_epoch_ms", Maybe Int64),
       '("owner_xid", Maybe Text),
       '("serialization", Maybe Text)
     ]

-- | Records a workflow, reconciling with any row already under that id. The
-- @ON CONFLICT@ arm counts recovery attempts only for rows that are not
-- merely queued, and re-stamps the executor only when the row is unowned,
-- this attempt's own, or the submission may claim — so a duplicate fresh
-- submission never takes another executor's row. Mirrors @init_workflow@.
-- | The statement behind the init transaction, exposed so the atomic child
-- start runs it on its own transaction.
initWorkflowStatement :: InitWorkflowParams -> Statement.Statement () WorkflowInitRaw
initWorkflowStatement params =
  sqlQueryTypedStatement [typedSql|
    insert into dbos.workflow_status
      (workflow_uuid, status, name, class_name, config_name,
       queue_name, deduplication_id, priority, queue_partition_key, delay_until_epoch_ms,
       authenticated_user, assumed_role, authenticated_roles,
       executor_id, application_version, application_id,
       created_at, updated_at, recovery_attempts,
       workflow_timeout_ms, workflow_deadline_epoch_ms,
       parent_workflow_id, owner_xid, serialization, attributes, schedule_name,
       debounce_deadline_epoch_ms, is_debounced, application_name)
    values
      (${initParamWorkflowId}, ${initParamStatus}, ${initParamName}, ${initParamClassName}, ${initParamConfigName},
       ${initParamQueueName}, ${initParamDeduplicationId}, ${initParamPriority}, ${initParamQueuePartitionKey}, ${initParamDelayUntil},
       ${initParamAuthenticatedUser}, ${initParamAssumedRole}, ${initParamAuthenticatedRoles},
       ${initParamExecutorId}, ${initParamApplicationVersion}, ${initParamApplicationId},
       ${initParamCreatedAt}, ${initParamUpdatedAt}, ${initParamInitialAttempts},
       ${initParamTimeoutMs}, ${initParamDeadline},
       ${initParamParentWorkflowId}, ${initParamOwnerXid}, ${initParamSerialization}, ${initParamAttributes}::text::jsonb, ${initParamScheduleName},
       ${initParamDebounceDeadline}, ${initParamIsDebounced}, ${initParamApplicationName})
    on conflict (workflow_uuid) do update set
      recovery_attempts = case
          when dbos.workflow_status.status != 'ENQUEUED' and dbos.workflow_status.status != 'DELAYED'
          then dbos.workflow_status.recovery_attempts + ${initParamIncrement}
          else dbos.workflow_status.recovery_attempts
        end,
      updated_at = (extract(epoch from now()) * 1000)::bigint,
      executor_id = case
          when excluded.status = 'ENQUEUED' or excluded.status = 'DELAYED'
          then dbos.workflow_status.executor_id
          when dbos.workflow_status.owner_xid is null
            or dbos.workflow_status.owner_xid = excluded.owner_xid
            or ${initParamClaiming}
          then excluded.executor_id
          else dbos.workflow_status.executor_id
        end
    returning recovery_attempts, status, name, class_name::text, config_name::text, queue_name,
      workflow_deadline_epoch_ms, owner_xid, serialization
  |]
  where
    InitWorkflowParams {..} = params

-- | Records a workflow's input in its own table, replacing any orphaned row
-- a deleted status row left behind. The caller runs this only when its own
-- attempt created the status row: a submission that found an existing row
-- must not touch the input the workflow was recorded with. Mirrors the
-- oracle's @workflow_input@ upsert in @init_workflow@.
initWorkflowInputStatement :: Statement.Statement (Text, Maybe Text) Int64
initWorkflowInputStatement =
  Statement.preparable sql encoder (Decoders.rowsAffected)
  where
    sql =
      "insert into dbos.workflow_input (workflow_uuid, inputs) values ($1, $2) \
      \on conflict (workflow_uuid) do update set inputs = excluded.inputs"
    encoder =
      contramap fst (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap snd (Encoders.param (Encoders.nullable Encoders.text))


-- | Parks a workflow that has been recovered more often than allowed, and
-- releases what a parked workflow should not hold. Committed before the
-- error is reported: parking is the one error path that is itself a write.
-- Mirrors the oracle's park statement.
parkWorkflowSession :: Text -> Session.Session Int64
parkWorkflowSession workflowId =
  sqlExecTypedSession [typedSql|
    update dbos.workflow_status
    set status = 'MAX_RECOVERY_ATTEMPTS_EXCEEDED',
      deduplication_id = null,
      started_at_epoch_ms = null,
      queue_name = null,
      updated_at = (extract(epoch from now()) * 1000)::bigint,
      completed_at = (extract(epoch from now()) * 1000)::bigint
    where workflow_uuid = ${workflowId} and status = 'PENDING'
  |]

-- | A step's recorded result, as the replay gate reads it. The join's
-- columns are nullable because the step may not have run; @status@ is the
-- workflow's, read in the same statement so the two cannot straddle a
-- cancellation. Mirrors @step_from_row@'s fields.
data StepCheckRaw = StepCheckRaw
  { stepCheckStatus          :: Maybe Text,
    stepCheckStepId          :: Maybe Int,
    stepCheckStepName        :: Maybe Text,
    stepCheckChildWorkflowId :: Maybe Text,
    stepCheckSerialization   :: Maybe Text,
    stepCheckStartedAt       :: Maybe Int64,
    stepCheckCompletedAt     :: Maybe Int64,
    stepCheckOutput          :: Maybe Text,
    stepCheckError           :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | The workflow's status and the recorded step at one position, in one
-- statement so the two come from a single snapshot: two queries could read
-- @PENDING@, then see a cancellation, then replay a step of a cancelled
-- workflow. No row at all means no such workflow — the outer table drives
-- the join. Mirrors @check_step_on@; hand-written rather than typedSql
-- because LEFT JOIN nullability is what the columns turn on.
checkStepSession :: Text -> Int -> Session.Session (Maybe StepCheckRaw)
checkStepSession workflowId stepId =
  Session.statement (workflowId, stepId) checkStepStatement

-- | The statement behind 'checkStepSession', exposed so the transaction
-- bodies (events) can read a step inside their own commit.
checkStepStatement :: Statement.Statement (Text, Int) (Maybe StepCheckRaw)
checkStepStatement = Statement.preparable sql encoder decoder
  where
    sql =
      "select s.status, o.function_id, o.function_name, o.child_workflow_id, \
      \o.serialization, o.started_at_epoch_ms, o.completed_at_epoch_ms, o.output, o.error \
      \from dbos.workflow_status s \
      \left join dbos.operation_outputs o \
      \  on o.workflow_uuid = s.workflow_uuid and o.function_id = $2 \
      \where s.workflow_uuid = $1"
    encoder =
      contramap fst (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (fromIntegral . snd) (Encoders.param (Encoders.nonNullable Encoders.int4))
    decoder =
      Decoders.rowMaybe
        ( StepCheckRaw
            <$> Decoders.column (Decoders.nullable Decoders.text)
            <*> (fmap fromIntegral <$> Decoders.column (Decoders.nullable Decoders.int4))
            <*> Decoders.column (Decoders.nullable Decoders.text)
            <*> Decoders.column (Decoders.nullable Decoders.text)
            <*> Decoders.column (Decoders.nullable Decoders.text)
            <*> Decoders.column (Decoders.nullable Decoders.int8)
            <*> Decoders.column (Decoders.nullable Decoders.int8)
            <*> Decoders.column (Decoders.nullable Decoders.text)
            <*> Decoders.column (Decoders.nullable Decoders.text)
        )

-- | Everything @record_step@ binds. Mirrors the oracle's statement, whose
-- parameter list is the union of the plain step and the child-result form.
data RecordStepParams = RecordStepParams
  { recordStepWorkflowId      :: Text,
    recordStepStepId          :: Int,
    recordStepStepName        :: Text,
    recordStepOutput          :: Maybe Text,
    recordStepError           :: Maybe Text,
    recordStepSerialization   :: Maybe Text,
    recordStepStartedAt       :: Maybe Int64,
    recordStepCompletedAt     :: Maybe Int64,
    recordStepApplicationName :: Maybe Text,
    recordStepChildWorkflowId :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | Records a step's result, and reports back the completion timestamp the
-- row holds — which is this caller's own when the insert won, and the
-- stored one when it lost. @DO UPDATE@ setting a column to itself is
-- deliberate: it makes @RETURNING@ fire on conflict, where @DO NOTHING@
-- would return no row and could not tell a rival execution from this
-- caller's retry. Mirrors @record_step_on@'s insert.
recordStepSession :: RecordStepParams -> Session.Session (Maybe Int64)
recordStepSession params =
  Session.statement () (recordStepStatement params)

-- | The statement behind 'recordStepSession', exposed for transaction bodies.
recordStepStatement :: RecordStepParams -> Statement.Statement () (Maybe Int64)
recordStepStatement params =
  sqlQueryTypedStatement [typedSql|
    insert into dbos.operation_outputs
      (workflow_uuid, function_id, function_name, output, error, serialization,
       started_at_epoch_ms, completed_at_epoch_ms, application_name, child_workflow_id)
    values
      (${recordStepWorkflowId}, ${recordStepStepId}, ${recordStepStepName}, ${recordStepOutput}, ${recordStepError}, ${recordStepSerialization},
       ${recordStepStartedAt}, ${recordStepCompletedAt}, ${recordStepApplicationName}, ${recordStepChildWorkflowId})
    on conflict (workflow_uuid, function_id) do update
    set completed_at_epoch_ms = dbos.operation_outputs.completed_at_epoch_ms
    returning completed_at_epoch_ms
  |]
  where
    RecordStepParams {..} = params

-- | Re-stamps the executor that is advancing the workflow, guarded so an
-- identical stamp writes nothing. Winning the checkpoint is what proves the
-- claim. Mirrors the oracle's conditional claim.
claimExecutorSession :: Text -> Text -> Session.Session Int64
claimExecutorSession workflowId executorId =
  sqlExecTypedSession [typedSql|
    update dbos.workflow_status
    set executor_id = ${executorId}
    where workflow_uuid = ${workflowId} and executor_id is distinct from ${executorId}
  |]

-- | What a @recv@ transaction decided: an adopted rival's step, or the
-- message this attempt took (or the absence a timeout produced).
data RecvTxResult
  = RecvAdopted StepCheckRaw
  | RecvTook (Maybe EncodedValueRaw)
  deriving stock (Eq, Show)

-- | A message's payload and format, as taking it reads them.
data EncodedValueRaw = EncodedValueRaw
  { encodedRawValue         :: Text,
    encodedRawSerialization :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | A registered application version, as the listing reads it.
data VersionRowRaw = VersionRowRaw
  { versionRowId              :: Text,
    versionRowName            :: Text,
    versionRowTimestamp       :: Int64,
    versionRowCreatedAt       :: Int64,
    versionRowApplicationName :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | The versions this handle may see: its own application's plus the
-- unclaimed. One statement serves the listing, the latest, and the holder
-- read, since they differ only in ordering and bounds. Hand-written rather
-- than typedSql: a select of the whole table would infer IHP's model type
-- for @application_versions@, which this layer does not have.
versionsStatement :: Text -> Bool -> Statement.Statement (Maybe Text) [VersionRowRaw]
versionsStatement orderClause limitOne =
  Statement.preparable sql encoder decoder
  where
    sql =
      "select version_id, version_name, version_timestamp, created_at, application_name \
      \from dbos.application_versions \
      \where ($1::text is null or application_name = $1 or application_name is null) "
        <> orderClause
        <> (if limitOne then " limit 1" else "")
    encoder = Encoders.param (Encoders.nullable Encoders.text)
    decoder =
      Decoders.rowList
        ( VersionRowRaw
            <$> Decoders.column (Decoders.nonNullable Decoders.text)
            <*> Decoders.column (Decoders.nonNullable Decoders.text)
            <*> Decoders.column (Decoders.nonNullable Decoders.int8)
            <*> Decoders.column (Decoders.nonNullable Decoders.int8)
            <*> Decoders.column (Decoders.nullable Decoders.text)
        )

-- | The application that holds a name, if any, so the caller can decide
-- whether to claim it. Mirrors @resolve_owning_application@'s read.
versionHolderSession :: Text -> Session.Session (Maybe (Maybe Text))
versionHolderSession versionName =
  fmap listToMaybe
    ( sqlQueryTypedSession [typedSql|
        select application_name
        from dbos.application_versions
        where version_name = ${versionName}
      |]
    )

-- | Claims a nameless version row, or inserts one when no row exists yet.
versionClaimSession :: Text -> Maybe Text -> Session.Session Int64
versionClaimSession versionName applicationName =
  sqlExecTypedSession [typedSql|
    update dbos.application_versions set application_name = ${applicationName}
    where version_name = ${versionName} and application_name is null
  |]

versionInsertSession :: Text -> Text -> Maybe Text -> Session.Session ()
versionInsertSession versionId versionName applicationName =
  void
    ( sqlExecTypedSession [typedSql|
        insert into dbos.application_versions (version_id, version_name, application_name)
        values (${versionId}, ${versionName}, ${applicationName})
        on conflict do nothing
      |]
    )

-- | The versions this handle may see, newest first. Mirrors
-- @list_application_versions@.
listApplicationVersionsSession :: Maybe Text -> Session.Session [VersionRowRaw]
listApplicationVersionsSession applicationName =
  Session.statement applicationName (versionsStatement "order by version_timestamp desc" False)

-- | The newest version this handle may see, if any. Mirrors
-- @get_latest_application_version@.
latestApplicationVersionSession :: Maybe Text -> Session.Session (Maybe VersionRowRaw)
latestApplicationVersionSession applicationName =
  fmap listToMaybe (Session.statement applicationName (versionsStatement "order by version_timestamp desc" True))

-- | Moves a version's timestamp, claiming a nameless row for the owner the
-- caller resolved. Mirrors @update_application_version_timestamp@.
updateVersionTimestampSession :: Text -> Int64 -> Maybe Text -> Session.Session Int64
updateVersionTimestampSession versionName timestamp owner =
  sqlExecTypedSession [typedSql|
    update dbos.application_versions
    set version_timestamp = ${timestamp}, application_name = ${owner}
    where version_name = ${versionName}
      and (application_name is null or application_name = ${owner})
  |]

-- | The @application_name@ predicate a rename moves rows by. The parameter
-- index differs per statement, so it is passed in. Mirrors Rust
-- @rename_source_predicate@.
renameSourcePredicate :: RenameFrom -> Int -> Text
renameSourcePredicate source param =
  case source of
    RenameApplication _             -> "application_name = $" <> index
    RenameApplicationAndUnclaimed _ -> "(application_name = $" <> index <> " or application_name is null)"
    RenameUnclaimed                 -> "($" <> index <> "::text is null and application_name is null)"
  where
    index = Text.pack (show param)

-- | The direct forks of a set of workflows: one level of the fork tree, as
-- @(forked_id, forked_from)@ pairs. The walk itself is the caller's, level
-- by level. Mirrors @descendant_forks@'s per-level select.
directForksSession :: [Text] -> Session.Session [(Text, Text)]
directForksSession roots =
  fmap (map (\(row :: SqlRow '[ '("workflow_uuid", Id' "workflow_status"), '("forked_from", Maybe Text) ]) -> (unwrapId row.workflow_uuid, fromMaybe "" row.forked_from)))
    (sqlQueryTypedSession [typedSql|
      select workflow_uuid, forked_from
      from dbos.workflow_status
      where forked_from = any(${roots}) and forked_from is not null
    |])

-- | Renames one table's rows in one statement: @$1@ is the new name, @$2@
-- the source application (null for unclaimed). Mirrors the oracle's
-- single-statement update.
renameRowsStatement :: Text -> RenameFrom -> Text -> Statement.Statement (Text, Maybe Text) Int64
renameRowsStatement table source guard =
  Statement.preparable
    ("update " <> table <> " set application_name = $1 where " <> renameSourcePredicate source 2 <> guard)
    ( contramap fst (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap snd (Encoders.param (Encoders.nullable Encoders.text))
    )
    Decoders.rowsAffected

-- | The batched rename's upper watermark: the id at the batch's far edge,
-- or nothing when the table holds no more matching rows.
renameBatchBoundStatement :: Text -> RenameFrom -> Word32 -> Statement.Statement (Maybe Text, Maybe Text) (Maybe Text)
renameBatchBoundStatement table source batchSize =
  Statement.preparable
    ( "select distinct workflow_uuid from "
        <> table
        <> " where "
        <> renameSourcePredicate source 1
        <> " and workflow_uuid > coalesce($2, '') order by workflow_uuid limit 1 offset "
        <> Text.pack (show (max 0 (batchSize - 1)))
    )
    ( contramap fst (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap snd (Encoders.param (Encoders.nullable Encoders.text))
    )
    (Decoders.rowMaybe (Decoders.column (Decoders.nonNullable Decoders.text)))

-- | Renames the rows inside one batch: above the watermark and no further
-- than the batch's far edge.
renameBatchRangeStatement :: Text -> RenameFrom -> Statement.Statement (Text, Maybe Text, Text) Int64
renameBatchRangeStatement table source =
  Statement.preparable
    ( "update "
        <> table
        <> " set application_name = $1 where "
        <> renameSourcePredicate source 3
        <> " and workflow_uuid > coalesce($2, '') and workflow_uuid <= $3"
    )
    ( contramap (\(a, _, _) -> a) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, b, _) -> b) (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap (\(_, _, c) -> c) (Encoders.param (Encoders.nonNullable Encoders.text))
    )
    Decoders.rowsAffected

-- | A step row as the listing reads it: the recorded columns plus the
-- payloads, which are @NULL@ when the caller does not want them.
data StepRowRaw = StepRowRaw
  { stepRowId              :: Int,
    stepRowName            :: Text,
    stepRowChildWorkflowId :: Maybe Text,
    stepRowSerialization   :: Maybe Text,
    stepRowStartedAt       :: Maybe Int64,
    stepRowCompletedAt     :: Maybe Int64,
    stepRowOutput          :: Maybe Text,
    stepRowError           :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | The steps of one workflow, in the order they were recorded. Hand-written
-- rather than typedSql: the composite primary key of @operation_outputs@
-- makes the workflow-id parameter's inferred type the table's key type,
-- which is not what this layer binds.
listStepsSession :: Text -> Bool -> Maybe Int64 -> Maybe Int64 -> Session.Session [StepRowRaw]
listStepsSession workflowId loadOutput limit offset =
  Session.statement (workflowId, loadOutput, limit, offset) listStepsStatement

listStepsStatement :: Statement.Statement (Text, Bool, Maybe Int64, Maybe Int64) [StepRowRaw]
listStepsStatement =
  Statement.preparable sql encoder decoder
  where
    sql =
      "select function_id, function_name, child_workflow_id, serialization, \
      \started_at_epoch_ms, completed_at_epoch_ms, \
      \case when $2 then output end as output, \
      \case when $2 then error end as error \
      \from dbos.operation_outputs \
      \where workflow_uuid = $1 \
      \order by function_id \
      \limit $3 offset $4"
    encoder =
      contramap (\(a, _, _, _) -> a) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, b, _, _) -> b) (Encoders.param (Encoders.nonNullable Encoders.bool))
        <> contramap (\(_, _, c, _) -> c) (Encoders.param (Encoders.nullable Encoders.int8))
        <> contramap (\(_, _, _, d) -> d) (Encoders.param (Encoders.nullable Encoders.int8))
    decoder =
      Decoders.rowList
        ( StepRowRaw
            <$> (fromIntegral <$> Decoders.column (Decoders.nonNullable Decoders.int4))
            <*> Decoders.column (Decoders.nonNullable Decoders.text)
            <*> Decoders.column (Decoders.nullable Decoders.text)
            <*> Decoders.column (Decoders.nullable Decoders.text)
            <*> Decoders.column (Decoders.nullable Decoders.int8)
            <*> Decoders.column (Decoders.nullable Decoders.int8)
            <*> Decoders.column (Decoders.nullable Decoders.text)
            <*> Decoders.column (Decoders.nullable Decoders.text)
        )

-- | Everything a fork's transaction binds: the sources and their new ids
-- and start steps, the options the fork rows inherit, and the child
-- replacements a caller asked for.
data ForkParams = ForkParams
  { forkSources            :: [Text],
    forkIds                :: [Text],
    forkSteps              :: [Int],
    forkApplicationVersion :: Maybe Text,
    forkQueue              :: Text,
    forkPartitionKey       :: Maybe Text,
    forkTimeoutMs          :: Maybe Int64,
    forkApplicationName    :: Maybe Text,
    forkReplaceFrom        :: [Text],
    forkReplaceTo          :: [Text],
    forkCopiesAnything     :: Bool
  }
  deriving stock (Eq, Show)

-- | Creates the fork rows and, for every start step past the beginning,
-- copies the source's history forward: steps, per-step event history, the
-- latest event per key, and streams. One commit, so a fork cannot exist
-- half-copied. Returns the sources that do not exist, so the caller can
-- report them before anything is written. Mirrors @fork_on@ (fork points
-- are resolved by the caller, one statement earlier than the oracle's
-- in-transaction resolution).
forkTx :: ForkParams -> Tx.Transaction (Maybe [Text])
forkTx params = do
  present <- Tx.statement (params.forkSources) existingSourcesStatement
  let missing = [source | source <- params.forkSources, source `notElem` present]
  if not (null missing)
    then pure (Just missing)
    else do
      void (Tx.statement (forkInsertParams params) forkInsertStatement)
      void (Tx.statement (params.forkSources, params.forkIds) forkCopyInputStatement)
      void (Tx.statement (params.forkSources) markForkedFromStatement)
      if params.forkCopiesAnything
        then do
          void (Tx.statement (forkCopyStepsParams params) forkCopyStepsStatement)
          void (Tx.statement (forkCopyTripleParams params) forkCopyHistoryStatement)
          void (Tx.statement (forkCopyTripleParams params) forkCopyEventsStatement)
          void (Tx.statement (forkCopyTripleParams params) forkCopyStreamsStatement)
        else pure ()
      pure Nothing

-- | The sources that exist, so a fork can report the rest.
existingSourcesStatement :: Statement.Statement [Text] [Text]
existingSourcesStatement =
  Statement.preparable
    "select workflow_uuid from dbos.workflow_status where workflow_uuid = any($1)"
    (Encoders.param (Encoders.nonNullable textArray))
    (Decoders.rowList (Decoders.column (Decoders.nonNullable Decoders.text)))
  where
    textArray = Encoders.foldableArray (Encoders.nonNullable Encoders.text)

-- | The fork rows: queued, inheriting the source's identity, pointed back
-- at the source, on the internal queue unless told otherwise. The input
-- travels separately, into @workflow_input@, so readers find it where they
-- look first.
forkInsertStatement :: Statement.Statement ([Text], [Text], [Int32], Maybe Text, Text, Maybe Text, Maybe Int64, Maybe Text) Int64
forkInsertStatement =
  Statement.preparable sql encoder (Decoders.rowsAffected)
  where
    sql =
      "insert into dbos.workflow_status (workflow_uuid, status, name, class_name, config_name, \
      \application_version, application_id, authenticated_user, authenticated_roles, \
      \assumed_role, serialization, request, queue_name, \
      \queue_partition_key, forked_from, attributes, workflow_timeout_ms, application_name) \
      \select m.fork_id, 'ENQUEUED', w.name, w.class_name, w.config_name, \
      \  coalesce($4, w.application_version), w.application_id, w.authenticated_user, \
      \  w.authenticated_roles, w.assumed_role, w.serialization, w.request, \
      \  $5, $6, w.workflow_uuid, w.attributes, $7, coalesce(w.application_name, $8) \
      \from unnest($1::text[], $2::text[], $3::int4[]) as m(source_id, fork_id, start_step) \
      \join dbos.workflow_status w on w.workflow_uuid = m.source_id"
    textArray = Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text)))
    int4Array = Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.int4)))
    encoder =
      contramap (\(a, _, _, _, _, _, _, _) -> a) textArray
        <> contramap (\(_, b, _, _, _, _, _, _) -> b) textArray
        <> contramap (\(_, _, c, _, _, _, _, _) -> c) int4Array
        <> contramap (\(_, _, _, d, _, _, _, _) -> d) (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap (\(_, _, _, _, e, _, _, _) -> e) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, _, _, _, f, _, _) -> f) (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap (\(_, _, _, _, _, _, g, _) -> g) (Encoders.param (Encoders.nullable Encoders.int8))
        <> contramap (\(_, _, _, _, _, _, _, h) -> h) (Encoders.param (Encoders.nullable Encoders.text))

-- | Copies each fork's input under the fork's id, reading it the way
-- every reader reads it: the payload table first, the legacy column for a
-- source written before migration 109. An upsert for the same orphaned-row
-- reason the init write is. Mirrors the oracle's fork input copy.
forkCopyInputStatement :: Statement.Statement ([Text], [Text]) Int64
forkCopyInputStatement =
  Statement.preparable sql encoder (Decoders.rowsAffected)
  where
    sql =
      "insert into dbos.workflow_input (workflow_uuid, inputs) \
      \select m.fork_id, coalesce(i.inputs, w.inputs) \
      \from unnest($1::text[], $2::text[]) as m(source_id, fork_id) \
      \join dbos.workflow_status w on w.workflow_uuid = m.source_id \
      \left join dbos.workflow_input i on i.workflow_uuid = m.source_id \
      \on conflict (workflow_uuid) do update set inputs = excluded.inputs"
    textArray = Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text)))
    encoder =
      contramap fst textArray
        <> contramap snd textArray

-- | Marks the sources as forked from, which is what a later fork walks.
markForkedFromStatement :: Statement.Statement [Text] Int64
markForkedFromStatement =
  Statement.preparable
    "update dbos.workflow_status set was_forked_from = true where workflow_uuid = any($1)"
    (Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text))))
    (Decoders.rowsAffected)

forkInsertParams :: ForkParams -> ([Text], [Text], [Int32], Maybe Text, Text, Maybe Text, Maybe Int64, Maybe Text)
forkInsertParams params =
  ( params.forkSources,
    params.forkIds,
    map fromIntegral params.forkSteps,
    params.forkApplicationVersion,
    params.forkQueue,
    params.forkPartitionKey,
    params.forkTimeoutMs,
    params.forkApplicationName
  )

forkCopyStepsParams :: ForkParams -> ([Text], [Text], [Int32], [Text], [Text], Maybe Text)
forkCopyStepsParams params =
  ( params.forkSources,
    params.forkIds,
    map fromIntegral params.forkSteps,
    params.forkReplaceFrom,
    params.forkReplaceTo,
    params.forkApplicationName
  )

forkCopyTripleParams :: ForkParams -> ([Text], [Text], [Int32])
forkCopyTripleParams params = (params.forkSources, params.forkIds, map fromIntegral params.forkSteps)

-- | Copies the source's steps before the start step, replacing child
-- references the caller asked to replace.
forkCopyStepsStatement :: Statement.Statement ([Text], [Text], [Int32], [Text], [Text], Maybe Text) Int64
forkCopyStepsStatement =
  Statement.preparable sql encoder (Decoders.rowsAffected)
  where
    sql =
      "insert into dbos.operation_outputs (workflow_uuid, function_id, output, error, \
      \serialization, function_name, child_workflow_id, started_at_epoch_ms, \
      \completed_at_epoch_ms, application_name) \
      \select m.fork_id, o.function_id, o.output, o.error, o.serialization, o.function_name, \
      \  coalesce(r.replacement, o.child_workflow_id), o.started_at_epoch_ms, \
      \  o.completed_at_epoch_ms, coalesce(w.application_name, $6) \
      \from unnest($1::text[], $2::text[], $3::int4[]) as m(source_id, fork_id, start_step) \
      \join dbos.operation_outputs o on o.workflow_uuid = m.source_id and o.function_id < m.start_step \
      \join dbos.workflow_status w on w.workflow_uuid = m.source_id \
      \left join unnest($4::text[], $5::text[]) as r(original, replacement) on r.original = o.child_workflow_id"
    textArray = Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text)))
    int4Array = Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.int4)))
    encoder =
      contramap (\(a, _, _, _, _, _) -> a) textArray
        <> contramap (\(_, b, _, _, _, _) -> b) textArray
        <> contramap (\(_, _, c, _, _, _) -> c) int4Array
        <> contramap (\(_, _, _, d, _, _) -> d) textArray
        <> contramap (\(_, _, _, _, e, _) -> e) textArray
        <> contramap (\(_, _, _, _, _, f) -> f) (Encoders.param (Encoders.nullable Encoders.text))

-- | Copies the per-step event history forward.
forkCopyHistoryStatement :: Statement.Statement ([Text], [Text], [Int32]) Int64
forkCopyHistoryStatement =
  Statement.preparable sql encoder (Decoders.rowsAffected)
  where
    sql =
      "insert into dbos.workflow_events_history (workflow_uuid, function_id, key, value, serialization) \
      \select m.fork_id, h.function_id, h.key, h.value, h.serialization \
      \from unnest($1::text[], $2::text[], $3::int4[]) as m(source_id, fork_id, start_step) \
      \join dbos.workflow_events_history h on h.workflow_uuid = m.source_id and h.function_id < m.start_step"
    textArray = Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text)))
    int4Array = Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.int4)))
    encoder =
      contramap (\(a, _, _) -> a) textArray
        <> contramap (\(_, b, _) -> b) textArray
        <> contramap (\(_, _, c) -> c) int4Array

-- | Copies the latest event per key, which is what a reader sees.
forkCopyEventsStatement :: Statement.Statement ([Text], [Text], [Int32]) Int64
forkCopyEventsStatement =
  Statement.preparable sql encoder (Decoders.rowsAffected)
  where
    sql =
      "insert into dbos.workflow_events (workflow_uuid, key, value, serialization) \
      \select fork_id, key, value, serialization from ( \
      \  select m.fork_id, h.key, h.value, h.serialization, \
      \    row_number() over (partition by m.fork_id, h.key order by h.function_id desc) as rn \
      \  from unnest($1::text[], $2::text[], $3::int4[]) as m(source_id, fork_id, start_step) \
      \  join dbos.workflow_events_history h on h.workflow_uuid = m.source_id and h.function_id < m.start_step \
      \) latest where rn = 1"
    textArray = Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text)))
    int4Array = Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.int4)))
    encoder =
      contramap (\(a, _, _) -> a) textArray
        <> contramap (\(_, b, _) -> b) textArray
        <> contramap (\(_, _, c) -> c) int4Array

-- | Copies stream entries forward. Streams are otherwise deferred, but a
-- fork's copy is independent of the stream methods and keeps fork fidelity.
forkCopyStreamsStatement :: Statement.Statement ([Text], [Text], [Int32]) Int64
forkCopyStreamsStatement =
  Statement.preparable sql encoder (Decoders.rowsAffected)
  where
    sql =
      "insert into dbos.streams (workflow_uuid, function_id, key, value, serialization, \"offset\") \
      \select m.fork_id, s.function_id, s.key, s.value, s.serialization, s.\"offset\" \
      \from unnest($1::text[], $2::text[], $3::int4[]) as m(source_id, fork_id, start_step) \
      \join dbos.streams s on s.workflow_uuid = m.source_id and s.function_id < m.start_step"
    textArray = Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text)))
    int4Array = Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.int4)))
    encoder =
      contramap (\(a, _, _) -> a) textArray
        <> contramap (\(_, b, _) -> b) textArray
        <> contramap (\(_, _, c) -> c) int4Array

-- | The fork points of a batch, one row per source: the last step, or the
-- last failure with the last step as its fallback. Mirrors
-- @resolve_fork_points_on@ (the caller resolves, the oracle resolves inside
-- its transaction).
forkPointsStatement :: Text -> Statement.Statement ([Text], Maybe Text) [(Text, Int)]
forkPointsStatement aggregate =
  Statement.preparable
    ( "select workflow_uuid, "
        <> aggregate
        <> " as start_step from dbos.operation_outputs "
        <> "where workflow_uuid = any($1) and ($2::text is null or function_name = $2) "
        <> "group by workflow_uuid"
    )
    encoder
    ( Decoders.rowList
        ( (,)
            <$> Decoders.column (Decoders.nonNullable Decoders.text)
            <*> (fromIntegral <$> Decoders.column (Decoders.nonNullable Decoders.int4))
        )
    )
  where
    encoder =
      contramap fst (Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text))))
        <> contramap snd (Encoders.param (Encoders.nullable Encoders.text))

-- | Releases every delayed workflow whose time has come: a sweep, scoped
-- to the handle's own application plus the unclaimed rows. Clearing the
-- debounce key belongs here rather than in a second statement — once
-- released the workflow is committed to running, and a later debounce with
-- the same key must start a fresh workflow. Mirrors
-- @transition_delayed_workflows@.
transitionDelayedSession :: Int64 -> Maybe Text -> Session.Session Int64
transitionDelayedSession now applicationName =
  sqlExecTypedSession [typedSql|
    update dbos.workflow_status
    set status = 'ENQUEUED',
      updated_at = ${now},
      deduplication_id = case when is_debounced then null else deduplication_id end
    where status = 'DELAYED' and delay_until_epoch_ms <= ${now}
      and (${applicationName}::text is null
           or application_name = ${applicationName}
           or application_name is null)
  |]

-- | Cancels the ids that are still live, releasing what a cancelled
-- workflow should not hold. The terminal-status guard is what makes
-- cancellation safe to repeat. Mirrors @cancel_batch@.
cancelBatchSession :: [Text] -> Session.Session [Text]
cancelBatchSession workflowIds =
  map unwrapId
    <$> sqlQueryTypedSession [typedSql|
      update dbos.workflow_status
      set status = 'CANCELLED',
        queue_name = null,
        deduplication_id = null,
        started_at_epoch_ms = null,
        updated_at = (extract(epoch from now()) * 1000)::bigint,
        completed_at = (extract(epoch from now()) * 1000)::bigint
      where workflow_uuid = any(${workflowIds})
        and status not in ('SUCCESS', 'ERROR', 'CANCELLED')
      returning workflow_uuid
    |]

-- | Which of these ids exist, so a resume can report the rest as missing.
existingWorkflowsSession :: [Text] -> Session.Session [Text]
existingWorkflowsSession workflowIds =
  map unwrapId
    <$> sqlQueryTypedSession [typedSql|
      select workflow_uuid from dbos.workflow_status
      where workflow_uuid = any(${workflowIds})
    |]

-- | Resumes the ids that have not finished: the status guard leaves a
-- success or an error alone rather than overwriting its result. Mirrors
-- @resume_workflows@.
resumeWorkflowsSession :: [Text] -> Maybe Text -> Session.Session [Text]
resumeWorkflowsSession workflowIds queue =
  map unwrapId
    <$> sqlQueryTypedSession [typedSql|
      update dbos.workflow_status
      set status = 'ENQUEUED',
        queue_name = ${queue},
        recovery_attempts = 0,
        workflow_deadline_epoch_ms = null,
        deduplication_id = null,
        started_at_epoch_ms = null,
        completed_at = null,
        updated_at = (extract(epoch from now()) * 1000)::bigint
      where workflow_uuid = any(${workflowIds}) and status not in ('SUCCESS', 'ERROR')
      returning workflow_uuid
    |]

-- | Deletes the ids, whatever their status. Mirrors @delete_workflows@.
deleteWorkflowsSession :: [Text] -> Session.Session Int64
deleteWorkflowsSession workflowIds =
  Session.statement workflowIds deleteWorkflowsStatement

-- | Removes workflows' payload rows and steps with their status rows, in one
-- statement: none of the three tables cascades from @workflow_status@ any
-- more — the payload tables never had a key to it, and migration 112 dropped
-- the steps' — so the status delete no longer takes them with it.
-- Notifications, events, their history and streams still declare
-- @ON DELETE CASCADE@, and go with the row. Mirrors the oracle's delete
-- transaction, which names the same four deletes.
deleteWorkflowsStatement :: Statement.Statement [Text] Int64
deleteWorkflowsStatement =
  Statement.preparable sql encoder (Decoders.rowsAffected)
  where
    sql =
      "with del_input as (delete from dbos.workflow_input where workflow_uuid = any($1)), \
      \del_output as (delete from dbos.workflow_output where workflow_uuid = any($1)), \
      \del_steps as (delete from dbos.operation_outputs where workflow_uuid = any($1)) \
      \delete from dbos.workflow_status where workflow_uuid = any($1)"
    encoder = Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text)))

-- | Everything @recv@'s taking transaction binds.
data RecvParams = RecvParams
  { recvWorkflowId  :: Text,
    recvStepId      :: Int,
    recvTopic       :: Text,
    recvStartedAt   :: Int64,
    recvCompletedAt :: Int64
  }
  deriving stock (Eq, Show)

-- | Whether something is waiting for this workflow and topic: a yes/no
-- question, bounded so a producer outrunning its consumer does not ship a
-- row per waiting message once per interval. Mirrors the oracle's probe.
recvProbeSession :: Text -> Text -> Session.Session Bool
recvProbeSession workflowId topic =
  Session.statement (workflowId, topic) recvProbeStatement

recvProbeStatement :: Statement.Statement (Text, Text) Bool
recvProbeStatement =
  Statement.preparable sql encoder decoder
  where
    sql =
      "select 1 from dbos.notifications \
      \where destination_uuid = $1 and topic = $2 and consumed = false limit 1"
    encoder = contramap fst textParam <> contramap snd textParam
    textParam = Encoders.param (Encoders.nonNullable Encoders.text)
    decoder = fmap (maybe False (const True)) (Decoders.rowMaybe (Decoders.column (Decoders.nonNullable Decoders.int4)))

-- | Takes the oldest waiting message and records the step in one commit —
-- and it has to be one: a message taken but not recorded is gone, marked
-- consumed, so a replay would report a timeout for a message that reached
-- nobody. A rival's record is adopted; a conflict on the record rolls the
-- consumption back with it. Mirrors @recv@'s closing transaction.
recvTx :: RecvParams -> Tx.Transaction RecvTxResult
recvTx params = do
  existing <- Tx.statement (params.recvWorkflowId, params.recvStepId) checkStepStatement
  case existing of
    Just row | row.stepCheckStepId /= Nothing -> pure (RecvAdopted row)
    _ -> do
      taken <- Tx.statement (params.recvWorkflowId, params.recvTopic) consumeStatement
      Tx.statement () (recordStepStatement (recvStepRecord params taken))
      pure (RecvTook taken)

-- | The step a @recv@ writes: the message's own payload under the sender's
-- own format, and a @NULL@ output when the wait timed out.
recvStepRecord :: RecvParams -> Maybe EncodedValueRaw -> RecordStepParams
recvStepRecord params taken =
  RecordStepParams
    { recordStepWorkflowId = params.recvWorkflowId,
      recordStepStepId = params.recvStepId,
      recordStepStepName = "DBOS.recv",
      recordStepOutput = (.encodedRawValue) <$> taken,
      recordStepError = Nothing,
      recordStepSerialization = taken >>= (.encodedRawSerialization),
      recordStepStartedAt = Just params.recvStartedAt,
      recordStepCompletedAt = Just params.recvCompletedAt,
      recordStepApplicationName = Nothing,
      recordStepChildWorkflowId = Nothing
    }

-- | Marks the oldest waiting message consumed and returns it. The outer
-- @consumed = FALSE@ is not a restatement of the subquery: at read
-- committed two receivers resolve the same message, and the loser
-- re-evaluates this predicate against the winner's committed row and
-- matches nothing. Mirrors the oracle's consume.
consumeStatement :: Statement.Statement (Text, Text) (Maybe EncodedValueRaw)
consumeStatement =
  Statement.preparable sql encoder decoder
  where
    sql =
      "update dbos.notifications set consumed = true \
      \where message_uuid = ( \
      \    select message_uuid from dbos.notifications \
      \    where destination_uuid = $1 and topic = $2 and consumed = false \
      \    order by created_at_epoch_ms asc limit 1 \
      \) and consumed = false \
      \returning message, serialization"
    encoder = contramap fst textParam <> contramap snd textParam
    textParam = Encoders.param (Encoders.nonNullable Encoders.text)
    decoder =
      Decoders.rowMaybe
        ( EncodedValueRaw
            <$> Decoders.column (Decoders.nonNullable Decoders.text)
            <*> Decoders.column (Decoders.nullable Decoders.text)
        )

-- | Everything a send's transaction binds: one row per recipient (the
-- destination, its topic sentinel, the payload, the scoped message id), the
-- serialization, and — when a workflow is sending — the step that records
-- it.
data SendMessagesParams = SendMessagesParams
  { sendParamsDestinations  :: [Text],
    sendParamsTopics        :: [Text],
    sendParamsPayloads      :: [Text],
    sendParamsMessageIds    :: [Text],
    sendParamsSerialization :: Maybe Text,
    sendParamsStep          :: Maybe SendStep
  }
  deriving stock (Eq, Show)

-- | The step a send records, when a workflow is sending.
data SendStep = SendStep
  { sendStepWorkflowId  :: Text,
    sendStepStepId      :: Int,
    sendStepName        :: Text,
    sendStepStartedAt   :: Int64,
    sendStepCompletedAt :: Int64
  }
  deriving stock (Eq, Show)

-- | Delivers one batch of messages, as one commit with its step record: a
-- step committed without its messages would make a replay skip a send that
-- never happened. A replay writes nothing, and a duplicate message id is
-- discarded (@DO NOTHING@), which is what makes a re-send idempotent.
-- Mirrors @deliver@ (the fork fan-out is deferred: @send_to_forks@ is
-- refused until @descendant_forks@ lands).
sendMessagesTx :: SendMessagesParams -> Tx.Transaction ()
sendMessagesTx params = do
  replay <- case params.sendParamsStep of
    Nothing -> pure False
    Just step -> do
      existing <- Tx.statement (step.sendStepWorkflowId, step.sendStepStepId) checkStepStatement
      pure (maybe False (\row -> row.stepCheckStepId /= Nothing) existing)
  if replay
    then pure ()
    else do
      if null params.sendParamsDestinations
        then pure ()
        else void (Tx.statement (params.sendParamsDestinations, params.sendParamsTopics, params.sendParamsPayloads, params.sendParamsMessageIds, params.sendParamsSerialization) sendInsertStatement)
      case params.sendParamsStep of
        Nothing   -> pure ()
        Just step -> void (Tx.statement () (recordStepStatement (sendStepRecord step)))

-- | The step a send records: a void success under the send's own name.
sendStepRecord :: SendStep -> RecordStepParams
sendStepRecord step =
  RecordStepParams
    { recordStepWorkflowId = step.sendStepWorkflowId,
      recordStepStepId = step.sendStepStepId,
      recordStepStepName = step.sendStepName,
      recordStepOutput = Nothing,
      recordStepError = Nothing,
      recordStepSerialization = Nothing,
      recordStepStartedAt = Just step.sendStepStartedAt,
      recordStepCompletedAt = Just step.sendStepCompletedAt,
      recordStepApplicationName = Nothing,
      recordStepChildWorkflowId = Nothing
    }

-- | The batched insert: one statement for the whole batch, so it is
-- all-or-nothing, and @ON CONFLICT (message_uuid) DO NOTHING@ so a resend
-- under the same idempotency key is a no-op. The foreign key on the
-- destination is what catches an address that does not exist. Mirrors the
-- oracle's @unnest@ insert.
sendInsertStatement :: Statement.Statement ([Text], [Text], [Text], [Text], Maybe Text) Int64
sendInsertStatement =
  Statement.preparable sql encoder (Decoders.rowsAffected)
  where
    sql =
      "insert into dbos.notifications (destination_uuid, topic, message, message_uuid, serialization) \
      \select * from unnest($1::text[], $2::text[], $3::text[], $4::text[]) as m(destination_uuid, topic, message, message_uuid), \
      \(select $5::text) as s(serialization) \
      \on conflict (message_uuid) do nothing"
    textArray = Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text)))
    encoder =
      contramap (\(a, _, _, _, _) -> a) textArray
        <> contramap (\(_, b, _, _, _) -> b) textArray
        <> contramap (\(_, _, c, _, _) -> c) textArray
        <> contramap (\(_, _, _, d, _) -> d) textArray
        <> contramap (\(_, _, _, _, e) -> e) (Encoders.param (Encoders.nullable Encoders.text))

-- | Everything @set_event@ binds inside its transaction: the workflow, the
-- step it is recorded under, and the value.
data SetEventParams = SetEventParams
  { setEventWorkflowId    :: Text,
    setEventStepId        :: Int,
    setEventKey           :: Text,
    setEventValue         :: Text,
    setEventSerialization :: Maybe Text,
    setEventStartedAt     :: Int64,
    setEventCompletedAt   :: Int64
  }
  deriving stock (Eq, Show)

-- | Publishes an event, as one commit with its step record. The check is
-- the replay gate: a row already at this step means the event was published
-- before, and the transaction writes nothing. Returns the row the check
-- found, if any, so the caller can tell a replay from a cancellation or a
-- renamed step. Mirrors @set_event@'s transaction (the notifier's wakeup is
-- omitted: polling-only, ADR-0010).
setEventTx :: SetEventParams -> Tx.Transaction (Maybe StepCheckRaw)
setEventTx params = do
  existing <- Tx.statement (params.setEventWorkflowId, params.setEventStepId) checkStepStatement
  case existing of
    -- A row exists whenever the workflow does (the join's outer table), so
    -- only a recorded step is a replay.
    Just row | row.stepCheckStepId /= Nothing -> pure (Just row)
    _ -> do
      Tx.statement () (eventUpsertStatement params.setEventWorkflowId params.setEventKey params.setEventValue params.setEventSerialization)
      Tx.statement () (eventHistoryUpsertStatement params.setEventWorkflowId params.setEventStepId params.setEventKey params.setEventValue params.setEventSerialization)
      Tx.statement () (recordStepStatement (setEventStepRecord params))
      pure Nothing

-- | The current value of a key, which is what a reader sees. Mirrors the
-- oracle's events upsert.
eventUpsertStatement :: Text -> Text -> Text -> Maybe Text -> Statement.Statement () Int64
eventUpsertStatement workflowId key value serialization =
  sqlExecTypedStatement [typedSql|
    insert into dbos.workflow_events (workflow_uuid, key, value, serialization)
    values (${workflowId}, ${key}, ${value}, ${serialization})
    on conflict (workflow_uuid, key) do update
    set value = excluded.value, serialization = excluded.serialization
  |]

-- | The per-step history, which is what a fork copies forward. Mirrors the
-- oracle's history upsert.
eventHistoryUpsertStatement :: Text -> Int -> Text -> Text -> Maybe Text -> Statement.Statement () Int64
eventHistoryUpsertStatement workflowId stepId key value serialization =
  sqlExecTypedStatement [typedSql|
    insert into dbos.workflow_events_history (workflow_uuid, function_id, key, value, serialization)
    values (${workflowId}, ${stepId}, ${key}, ${value}, ${serialization})
    on conflict (workflow_uuid, key, function_id) do update
    set value = excluded.value, serialization = excluded.serialization
  |]

-- | The step record a @set_event@ writes: a void success under the
-- @DBOS.setEvent@ name, carrying the timing fixed outside the retry.
setEventStepRecord :: SetEventParams -> RecordStepParams
setEventStepRecord params =
  RecordStepParams
    { recordStepWorkflowId = params.setEventWorkflowId,
      recordStepStepId = params.setEventStepId,
      recordStepStepName = "DBOS.setEvent",
      recordStepOutput = Nothing,
      recordStepError = Nothing,
      recordStepSerialization = Nothing,
      recordStepStartedAt = Just params.setEventStartedAt,
      recordStepCompletedAt = Just params.setEventCompletedAt,
      recordStepApplicationName = Nothing,
      recordStepChildWorkflowId = Nothing
    }

-- | An event's stored value and format, as a reader takes it.
data EventValueRaw = EventValueRaw
  { eventRawValue         :: Text,
    eventRawSerialization :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | Reads a key's current value, or nothing if it has not been published.
-- The pass that finds a row is the one that answers the call, so a miss
-- carries nothing back. Mirrors the oracle's @get_event@ select.
eventValueSession :: Text -> Text -> Session.Session (Maybe EventValueRaw)
eventValueSession workflowId key =
  fmap (fmap toRaw . listToMaybe) (sqlQueryTypedSession [typedSql|
    select value, serialization
    from dbos.workflow_events
    where workflow_uuid = ${workflowId} and key = ${key}
  |])
  where
    toRaw row = EventValueRaw row.value row.serialization

-- | Everything @get_event@'s recording transaction binds: whose step records
-- the answer, what was found, and the timing fixed outside the retry.
data GetEventRecordParams = GetEventRecordParams
  { getEventRecordWorkflowId    :: Text,
    getEventRecordStepId        :: Int,
    getEventRecordOutput        :: Maybe Text,
    getEventRecordSerialization :: Maybe Text,
    getEventRecordStartedAt     :: Int64,
    getEventRecordCompletedAt   :: Int64
  }
  deriving stock (Eq, Show)

-- | Records the answer a caller-less wait found, as one commit with the
-- check that makes it safe: the wait between the opening check and this one
-- is exactly when another execution would have recorded its own answer, so
-- a rival's record is adopted rather than overwritten. Returns the row the
-- check found, if any. Mirrors @get_event@'s closing transaction.
getEventRecordTx :: GetEventRecordParams -> Tx.Transaction (Maybe StepCheckRaw)
getEventRecordTx params = do
  existing <- Tx.statement (params.getEventRecordWorkflowId, params.getEventRecordStepId) checkStepStatement
  case existing of
    Just row | row.stepCheckStepId /= Nothing -> pure (Just row)
    _ -> do
      Tx.statement () (recordStepStatement (getEventStepRecord params))
      pure Nothing

-- | The step record a @get_event@ writes: the value it saw under the value's
-- own format, and a @NULL@ output when the wait timed out — which is how
-- every implementation spells "there was nothing".
getEventStepRecord :: GetEventRecordParams -> RecordStepParams
getEventStepRecord params =
  RecordStepParams
    { recordStepWorkflowId = params.getEventRecordWorkflowId,
      recordStepStepId = params.getEventRecordStepId,
      recordStepStepName = "DBOS.getEvent",
      recordStepOutput = params.getEventRecordOutput,
      recordStepError = Nothing,
      recordStepSerialization = params.getEventRecordSerialization,
      recordStepStartedAt = Just params.getEventRecordStartedAt,
      recordStepCompletedAt = Just params.getEventRecordCompletedAt,
      recordStepApplicationName = Nothing,
      recordStepChildWorkflowId = Nothing
    }

-- | Moves a delayed workflow's release time. The @status = 'DELAYED'@ guard
-- is the point: a released workflow is running or queued, and pushing its
-- delay out would not recall it. Mirrors @set_workflow_delay@.
setWorkflowDelaySession :: Text -> Int64 -> Session.Session Int64
setWorkflowDelaySession workflowId delayUntil =
  sqlExecTypedSession [typedSql|
    update dbos.workflow_status
    set delay_until_epoch_ms = ${delayUntil},
      updated_at = (extract(epoch from now()) * 1000)::bigint
    where workflow_uuid = ${workflowId} and status = 'DELAYED'
  |]

-- | Returns a queued workflow to its queue: the @queue_name IS NOT NULL@
-- guard is what makes this a return rather than an enqueue, since a workflow
-- that never came from a queue has none to go back to. Mirrors
-- @clear_queue_assignment@.
clearQueueAssignmentSession :: Text -> Int64 -> Session.Session Int64
clearQueueAssignmentSession workflowId now =
  sqlExecTypedSession [typedSql|
    update dbos.workflow_status
    set started_at_epoch_ms = null,
      status = 'ENQUEUED',
      updated_at = ${now}
    where workflow_uuid = ${workflowId} and queue_name is not null and status = 'PENDING'
  |]

-- | Replaces a workflow's attributes. Mirrors @update_workflow_attributes@.
updateWorkflowAttributesSession :: Text -> Maybe Text -> Session.Session Int64
updateWorkflowAttributesSession workflowId attributes =
  sqlExecTypedSession [typedSql|
    update dbos.workflow_status
    set attributes = ${attributes}::text::jsonb,
      updated_at = (extract(epoch from now()) * 1000)::bigint
    where workflow_uuid = ${workflowId}
  |]

-- | The recovery sweep: pending workflows owned by the given executors, at
-- the given version, are enqueued onto the recovery queue. @NULLIF@ covers
-- rows an older implementation wrote, where "not queued" is the empty
-- string; @started_at_epoch_ms@ is cleared because the row has not started.
-- The application scope is the handle's own plus the unclaimed rows.
-- Mirrors @reenqueue_for_recovery@.
reenqueueForRecoverySession :: Text -> [Text] -> Text -> Maybe Text -> Session.Session [Text]
reenqueueForRecoverySession recoveryQueue executorIds applicationVersion applicationName =
  map unwrapId
    <$> sqlQueryTypedSession [typedSql|
      update dbos.workflow_status
      set status = 'ENQUEUED',
        started_at_epoch_ms = null,
        updated_at = (extract(epoch from now()) * 1000)::bigint,
        queue_name = coalesce(nullif(queue_name, ''), ${recoveryQueue})
      where status = 'PENDING'
        and executor_id = any(${executorIds})
        and application_version = ${applicationVersion}
        and (${applicationName}::text is null
             or application_name = ${applicationName}
             or application_name is null)
      returning workflow_uuid
    |]

-- | Every workflow descended from the root at any depth, level by level.
-- One session (hence one connection) for the whole walk, retried as one
-- unit by the caller — mirroring @get_workflow_children@ holding a single
-- connection. Terminates because the seen set only grows, so a cycle stops
-- rather than loops; the root is excluded by comparison, which also states
-- the contract that a workflow is not its own descendant.
descendantsSession :: Text -> Session.Session [Text]
descendantsSession root = go [] Set.empty [root]
  where
    go acc _ [] = pure acc
    go acc seen frontier = do
      children <- directChildrenSession frontier
      let (seen', fresh) = foldl' absorb (seen, []) children
      go (acc <> fresh) seen' fresh
    absorb (seen, fresh) child
      | child == root = (seen, fresh)
      | Set.member child seen = (seen, fresh)
      | otherwise = (Set.insert child seen, fresh <> [child])

-- | One breadth-first level: the ids whose parent is one of the given ids.
-- An array parameter (@parent_workflow_id = ANY($1)@), which is why this is
-- a plain statement rather than typedSql. Mirrors @direct_children@.
directChildrenSession :: [Text] -> Session.Session [Text]
directChildrenSession parents =
  Session.statement parents (Statement.preparable sql encoder decoder)
  where
    sql = "select workflow_uuid from dbos.workflow_status where parent_workflow_id = any($1)"
    encoder = Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text)))
    decoder = Decoders.rowList (Decoders.column (Decoders.nonNullable Decoders.text))

-- Queues: the registry rows (postgres.rs queue methods)

-- | A queue row as the registry stores it. Periods are fractional seconds, so
-- the fallible mapping to 'DBOS.SystemDB.Types.Duration' lives with the
-- mapper in @DBOS.SystemDB.Postgres@, not here.
data QueueRowRaw = QueueRowRaw
  { queueRowName                        :: Text,
    queueRowConcurrency                 :: Maybe Int,
    queueRowWorkerConcurrency           :: Maybe Int,
    queueRowRateLimitMax                :: Maybe Int,
    queueRowRateLimitPeriodSec          :: Maybe Double,
    queueRowPriorityEnabled             :: Bool,
    queueRowPartitionQueue              :: Bool,
    queueRowPartitionConcurrency        :: Maybe Int,
    queueRowPartitionWorkerConcurrency  :: Maybe Int,
    queueRowPartitionRateLimitMax       :: Maybe Int,
    queueRowPartitionRateLimitPeriodSec :: Maybe Double,
    queueRowPollingIntervalSec          :: Maybe Double,
    queueRowApplicationName             :: Maybe Text
  }

-- | The queue columns every queue read selects, in the oracle's order.
queueColumns :: Text
queueColumns =
  "name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec, \
  \priority_enabled, partition_queue, partition_concurrency, partition_worker_concurrency, \
  \partition_rate_limit_max, partition_rate_limit_period_sec, polling_interval_sec, application_name"

queueRowDecoder :: Decoders.Row QueueRowRaw
queueRowDecoder =
  QueueRowRaw
    <$> Decoders.column (Decoders.nonNullable Decoders.text)
    <*> (fmap fromIntegral <$> Decoders.column (Decoders.nullable Decoders.int4))
    <*> (fmap fromIntegral <$> Decoders.column (Decoders.nullable Decoders.int4))
    <*> (fmap fromIntegral <$> Decoders.column (Decoders.nullable Decoders.int4))
    <*> Decoders.column (Decoders.nullable Decoders.float8)
    <*> Decoders.column (Decoders.nonNullable Decoders.bool)
    <*> Decoders.column (Decoders.nonNullable Decoders.bool)
    <*> (fmap fromIntegral <$> Decoders.column (Decoders.nullable Decoders.int4))
    <*> (fmap fromIntegral <$> Decoders.column (Decoders.nullable Decoders.int4))
    <*> (fmap fromIntegral <$> Decoders.column (Decoders.nullable Decoders.int4))
    <*> Decoders.column (Decoders.nullable Decoders.float8)
    <*> Decoders.column (Decoders.nullable Decoders.float8)
    <*> Decoders.column (Decoders.nullable Decoders.text)

-- | One queue by name, unscoped: a peer's queue is still returned. Mirrors
-- @get_queue@.
queueByNameStatement :: Statement.Statement Text (Maybe QueueRowRaw)
queueByNameStatement =
  Statement.preparable
    ("select " <> queueColumns <> " from dbos.queues where name = $1")
    (Encoders.param (Encoders.nonNullable Encoders.text))
    (Decoders.rowMaybe queueRowDecoder)

-- | Every queue this handle may see, ordered by name. A null scope narrows
-- nothing, which is what @Any@ and an empty @Named@ list both mean. Mirrors
-- @list_queues@.
queueListStatement :: Statement.Statement (Maybe [Text]) [QueueRowRaw]
queueListStatement =
  Statement.preparable
    ( "select " <> queueColumns <> " from dbos.queues \
      \where ($1::text[] is null or application_name = any($1) or application_name is null) \
      \order by name" )
    (Encoders.param (Encoders.nullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text))))
    (Decoders.rowList queueRowDecoder)

-- | The owner of a queue row, if the row exists at all: the existence probe
-- and the owner resolve @upsert_queue@ runs before its write.
queueOwnerStatement :: Statement.Statement Text (Maybe (Maybe Text))
queueOwnerStatement =
  Statement.preparable
    "select application_name from dbos.queues where name = $1"
    (Encoders.param (Encoders.nonNullable Encoders.text))
    (Decoders.rowMaybe (Decoders.column (Decoders.nullable Decoders.text)))

-- | The fourteen values @upsert_queue@ writes.
data QueueInsertParams = QueueInsertParams
  { queueInsertName                        :: Text,
    queueInsertConcurrency                 :: Maybe Int,
    queueInsertWorkerConcurrency           :: Maybe Int,
    queueInsertRateLimitMax                :: Maybe Int,
    queueInsertRateLimitPeriodSec          :: Maybe Double,
    queueInsertPriorityEnabled             :: Bool,
    queueInsertPartitionQueue              :: Bool,
    queueInsertPartitionConcurrency        :: Maybe Int,
    queueInsertPartitionWorkerConcurrency  :: Maybe Int,
    queueInsertPartitionRateLimitMax       :: Maybe Int,
    queueInsertPartitionRateLimitPeriodSec :: Maybe Double,
    queueInsertPollingIntervalSec          :: Double,
    queueInsertUpdatedAt                   :: Int64,
    queueInsertApplicationName             :: Maybe Text
  }

-- | Registers a queue, updating the stored limits or leaving them alone. The
-- update clause never steals the owner: @COALESCE@ keeps whatever the row
-- already carries. Mirrors @upsert_queue@'s insert.
queueInsertStatement :: OnExistingQueue -> Statement.Statement QueueInsertParams ()
queueInsertStatement onExisting =
  Statement.preparable
    ( "insert into dbos.queues (name, concurrency, worker_concurrency, rate_limit_max, \
      \rate_limit_period_sec, priority_enabled, partition_queue, partition_concurrency, \
      \partition_worker_concurrency, partition_rate_limit_max, partition_rate_limit_period_sec, \
      \polling_interval_sec, updated_at, application_name) \
      \values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) "
        <> onConflict
    )
    encoder
    Decoders.noResult
  where
    onConflict = case onExisting of
      UpdateExisting ->
        "on conflict (name) do update set concurrency = excluded.concurrency, \
        \worker_concurrency = excluded.worker_concurrency, rate_limit_max = excluded.rate_limit_max, \
        \rate_limit_period_sec = excluded.rate_limit_period_sec, priority_enabled = excluded.priority_enabled, \
        \partition_queue = excluded.partition_queue, partition_concurrency = excluded.partition_concurrency, \
        \partition_worker_concurrency = excluded.partition_worker_concurrency, \
        \partition_rate_limit_max = excluded.partition_rate_limit_max, \
        \partition_rate_limit_period_sec = excluded.partition_rate_limit_period_sec, \
        \polling_interval_sec = excluded.polling_interval_sec, updated_at = excluded.updated_at, \
        \application_name = coalesce(dbos.queues.application_name, excluded.application_name)"
      LeaveExisting -> "on conflict (name) do nothing"
    encoder =
      contramap (.queueInsertName) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (.queueInsertConcurrency) (Encoders.param (Encoders.nullable (contramap fromIntegral Encoders.int4)))
        <> contramap (.queueInsertWorkerConcurrency) (Encoders.param (Encoders.nullable (contramap fromIntegral Encoders.int4)))
        <> contramap (.queueInsertRateLimitMax) (Encoders.param (Encoders.nullable (contramap fromIntegral Encoders.int4)))
        <> contramap (.queueInsertRateLimitPeriodSec) (Encoders.param (Encoders.nullable Encoders.float8))
        <> contramap (.queueInsertPriorityEnabled) (Encoders.param (Encoders.nonNullable Encoders.bool))
        <> contramap (.queueInsertPartitionQueue) (Encoders.param (Encoders.nonNullable Encoders.bool))
        <> contramap (.queueInsertPartitionConcurrency) (Encoders.param (Encoders.nullable (contramap fromIntegral Encoders.int4)))
        <> contramap (.queueInsertPartitionWorkerConcurrency) (Encoders.param (Encoders.nullable (contramap fromIntegral Encoders.int4)))
        <> contramap (.queueInsertPartitionRateLimitMax) (Encoders.param (Encoders.nullable (contramap fromIntegral Encoders.int4)))
        <> contramap (.queueInsertPartitionRateLimitPeriodSec) (Encoders.param (Encoders.nullable Encoders.float8))
        <> contramap (.queueInsertPollingIntervalSec) (Encoders.param (Encoders.nonNullable Encoders.float8))
        <> contramap (.queueInsertUpdatedAt) (Encoders.param (Encoders.nonNullable Encoders.int8))
        <> contramap (.queueInsertApplicationName) (Encoders.param (Encoders.nullable Encoders.text))

-- | Deletes a queue. Deleting nothing is success, as in the oracle.
queueDeleteStatement :: Statement.Statement Text ()
queueDeleteStatement =
  Statement.preparable
    "delete from dbos.queues where name = $1"
    (Encoders.param (Encoders.nonNullable Encoders.text))
    Decoders.noResult

-- | The partition keys with eligible work, ascending, one row per key. The
-- recursive loose index scan is the oracle's; null keys are never emitted.
-- Mirrors @get_queue_partitions@.
queuePartitionsStatement :: Statement.Statement (Text, Maybe Text) [Text]
queuePartitionsStatement =
  Statement.preparable
    ( "with recursive partitions as ( \
      \(select min(queue_partition_key) as pk from dbos.workflow_status \
      \where queue_name = $1 and status = 'ENQUEUED' \
      \and ($2::text is null or application_name = $2 or application_name is null) \
      \and queue_partition_key is not null) \
      \union all \
      \(select (select min(queue_partition_key) from dbos.workflow_status \
      \where queue_name = $1 and status = 'ENQUEUED' \
      \and ($2::text is null or application_name = $2 or application_name is null) \
      \and queue_partition_key > partitions.pk) \
      \from partitions where partitions.pk is not null) ) \
      \select pk from partitions where pk is not null" )
    ( contramap fst (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap snd (Encoders.param (Encoders.nullable Encoders.text))
    )
    (Decoders.rowList (Decoders.column (Decoders.nonNullable Decoders.text)))

-- | The workflow holding a deduplication key, if any. No status filter: a
-- terminal transition clears the key. Mirrors @get_deduplication_key_holder@.
deduplicationHolderStatement :: Statement.Statement (Text, Text) (Maybe Text)
deduplicationHolderStatement =
  Statement.preparable
    "select workflow_uuid from dbos.workflow_status where queue_name = $1 and deduplication_id = $2"
    ( contramap fst (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap snd (Encoders.param (Encoders.nonNullable Encoders.text))
    )
    (Decoders.rowMaybe (Decoders.column (Decoders.nonNullable Decoders.text)))

-- | One queue by name, locked for the length of an update transaction.
-- Mirrors the @FOR UPDATE@ read @update_queue@ opens with.
queueByNameForUpdateStatement :: Statement.Statement Text (Maybe QueueRowRaw)
queueByNameForUpdateStatement =
  Statement.preparable
    ("select " <> queueColumns <> " from dbos.queues where name = $1 for update")
    (Encoders.param (Encoders.nonNullable Encoders.text))
    (Decoders.rowMaybe queueRowDecoder)

-- | The thirteen values an update writes: every updatable column set to the
-- merged row's value (a field the update left alone carries its stored
-- value, which is what writing it back means) plus the new timestamp.
data QueueUpdateParams = QueueUpdateParams
  { queueUpdateName                        :: Text,
    queueUpdateConcurrency                 :: Maybe Int,
    queueUpdateWorkerConcurrency           :: Maybe Int,
    queueUpdateRateLimitMax                :: Maybe Int,
    queueUpdateRateLimitPeriodSec          :: Maybe Double,
    queueUpdatePriorityEnabled             :: Bool,
    queueUpdatePartitionQueue              :: Bool,
    queueUpdatePartitionConcurrency        :: Maybe Int,
    queueUpdatePartitionWorkerConcurrency  :: Maybe Int,
    queueUpdatePartitionRateLimitMax       :: Maybe Int,
    queueUpdatePartitionRateLimitPeriodSec :: Maybe Double,
    queueUpdatePollingIntervalSec          :: Double,
    queueUpdateUpdatedAt                   :: Int64
  }

-- | Writes the merged row back and returns it. Static SQL rather than the
-- oracle's built-up statement list: writing every column to its merged value
-- reaches the same row without dynamic SQL.
queueUpdateStatement :: Statement.Statement QueueUpdateParams (Maybe QueueRowRaw)
queueUpdateStatement =
  Statement.preparable
    ( "update dbos.queues set concurrency = $2, worker_concurrency = $3, \
      \rate_limit_max = $4, rate_limit_period_sec = $5, priority_enabled = $6, \
      \partition_queue = $7, partition_concurrency = $8, partition_worker_concurrency = $9, \
      \partition_rate_limit_max = $10, partition_rate_limit_period_sec = $11, \
      \polling_interval_sec = $12, updated_at = $13 \
      \where name = $1 returning "
        <> queueColumns
    )
    encoder
    (Decoders.rowMaybe queueRowDecoder)
  where
    encoder =
      contramap (.queueUpdateName) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (.queueUpdateConcurrency) (Encoders.param (Encoders.nullable (contramap fromIntegral Encoders.int4)))
        <> contramap (.queueUpdateWorkerConcurrency) (Encoders.param (Encoders.nullable (contramap fromIntegral Encoders.int4)))
        <> contramap (.queueUpdateRateLimitMax) (Encoders.param (Encoders.nullable (contramap fromIntegral Encoders.int4)))
        <> contramap (.queueUpdateRateLimitPeriodSec) (Encoders.param (Encoders.nullable Encoders.float8))
        <> contramap (.queueUpdatePriorityEnabled) (Encoders.param (Encoders.nonNullable Encoders.bool))
        <> contramap (.queueUpdatePartitionQueue) (Encoders.param (Encoders.nonNullable Encoders.bool))
        <> contramap (.queueUpdatePartitionConcurrency) (Encoders.param (Encoders.nullable (contramap fromIntegral Encoders.int4)))
        <> contramap (.queueUpdatePartitionWorkerConcurrency) (Encoders.param (Encoders.nullable (contramap fromIntegral Encoders.int4)))
        <> contramap (.queueUpdatePartitionRateLimitMax) (Encoders.param (Encoders.nullable (contramap fromIntegral Encoders.int4)))
        <> contramap (.queueUpdatePartitionRateLimitPeriodSec) (Encoders.param (Encoders.nullable Encoders.float8))
        <> contramap (.queueUpdatePollingIntervalSec) (Encoders.param (Encoders.nonNullable Encoders.float8))
        <> contramap (.queueUpdateUpdatedAt) (Encoders.param (Encoders.nonNullable Encoders.int8))

-- Dequeue: the claim sweeps (postgres.rs start_queued_workflows and
-- start_queued_partitioned_workflows)

-- | The queue-wide rate-limit count: starts in the window that were rate
-- limited. @$3@ is the window in milliseconds.
dequeueRateLimitCountStatement :: Statement.Statement (Maybe Text, Text, Int64) Int64
dequeueRateLimitCountStatement =
  Statement.preparable
    ( "select count(*) from dbos.workflow_status where queue_name = $2 \
      \and rate_limited = true and status not in ('ENQUEUED', 'DELAYED') \
      \and started_at_epoch_ms > (extract(epoch from now()) * 1000)::bigint - $3 \
      \and ($1::text is null or application_name = $1 or application_name is null)" )
    ( contramap (\(app, _, _) -> app) (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap (\(_, queue, _) -> queue) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, period) -> period) (Encoders.param (Encoders.nonNullable Encoders.int8))
    )
    (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))

-- | The per-partition rate-limit count, for one partition key.
dequeuePartitionRateLimitCountStatement :: Statement.Statement (Maybe Text, Text, Text, Int64) Int64
dequeuePartitionRateLimitCountStatement =
  Statement.preparable
    ( "select count(*) from dbos.workflow_status where queue_name = $3 \
      \and rate_limited = true and status not in ('ENQUEUED', 'DELAYED') \
      \and started_at_epoch_ms > (extract(epoch from now()) * 1000)::bigint - $4 \
      \and queue_partition_key = $2 \
      \and ($1::text is null or application_name = $1 or application_name is null)" )
    ( contramap (\(app, _, _, _) -> app) (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap (\(_, key, _, _) -> key) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, queue, _) -> queue) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, _, period) -> period) (Encoders.param (Encoders.nonNullable Encoders.int8))
    )
    (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))

-- | The queue-wide concurrency count: everything currently pending.
dequeueConcurrencyCountStatement :: Statement.Statement (Maybe Text, Text) Int64
dequeueConcurrencyCountStatement =
  Statement.preparable
    ( "select count(*) from dbos.workflow_status where queue_name = $2 and status = 'PENDING' \
      \and ($1::text is null or application_name = $1 or application_name is null)" )
    ( contramap fst (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap snd (Encoders.param (Encoders.nonNullable Encoders.text))
    )
    (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))

-- | The per-partition concurrency count, for one partition key.
dequeuePartitionConcurrencyCountStatement :: Statement.Statement (Maybe Text, Text, Text) Int64
dequeuePartitionConcurrencyCountStatement =
  Statement.preparable
    ( "select count(*) from dbos.workflow_status where queue_name = $3 and status = 'PENDING' \
      \and queue_partition_key = $2 \
      \and ($1::text is null or application_name = $1 or application_name is null)" )
    ( contramap (\(app, _, _) -> app) (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap (\(_, key, _) -> key) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, queue) -> queue) (Encoders.param (Encoders.nonNullable Encoders.text))
    )
    (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))

-- | The newest visible application version's name, for the dequeue's
-- @is_latest@ check: a version row that is not the newest is only claimable
-- when the version predicate names it exactly.
latestVersionNameStatement :: Statement.Statement (Maybe Text) (Maybe Text)
latestVersionNameStatement =
  Statement.preparable
    ( "select version_name from dbos.application_versions \
      \where ($1::text is null or application_name = $1 or application_name is null) \
      \order by version_timestamp desc limit 1" )
    (Encoders.param (Encoders.nullable Encoders.text))
    (Decoders.rowMaybe (Decoders.column (Decoders.nonNullable Decoders.text)))

-- | The candidates for one claim: the queue's eligible head, priority first,
-- locked so a rival process cannot take them. @$5@ says the caller's version
-- is the latest, which admits unversioned rows; @$6@ is the task budget
-- (null means no limit). The two lock modes are separate statements because
-- the mode is not a parameter: a shared-budget claim waits for no lock
-- (@nowait@), and a claim with no shared budget skips locked rows.
dequeueCandidatesNowaitStatement :: Statement.Statement (Maybe Text, Maybe Text, Text, Text, Bool, Maybe Int64) [Text]
dequeueCandidatesNowaitStatement = dequeueCandidatesStatement "for update nowait"

dequeueCandidatesSkipLockedStatement :: Statement.Statement (Maybe Text, Maybe Text, Text, Text, Bool, Maybe Int64) [Text]
dequeueCandidatesSkipLockedStatement = dequeueCandidatesStatement "for update skip locked"

dequeueCandidatesStatement :: Text -> Statement.Statement (Maybe Text, Maybe Text, Text, Text, Bool, Maybe Int64) [Text]
dequeueCandidatesStatement lock =
  Statement.preparable
    ( "select workflow_uuid from dbos.workflow_status where queue_name = $4 \
      \and status = 'ENQUEUED' \
      \and (application_version = $3 or ($5::bool and application_version is null)) \
      \and ($1::text is null or application_name = $1 or application_name is null) \
      \and ($2::text is null or queue_partition_key = $2) \
      \order by priority asc, created_at asc limit $6 "
        <> lock
    )
    encoder
    (Decoders.rowList (Decoders.column (Decoders.nonNullable Decoders.text)))
  where
    encoder =
      contramap (\(app, _, _, _, _, _) -> app) (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap (\(_, key, _, _, _, _) -> key) (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap (\(_, _, version, _, _, _) -> version) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, _, queue, _, _) -> queue) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, _, _, isLatest, _) -> isLatest) (Encoders.param (Encoders.nonNullable Encoders.bool))
        <> contramap (\(_, _, _, _, _, limit) -> limit) (Encoders.param (Encoders.nullable Encoders.int8))

-- | Flips the claimed candidates to pending and stamps them. Only rows that
-- are still enqueued and still this handle's to take come back.
dequeueClaimStatement :: Statement.Statement (Maybe Text, Text, Text, Bool, [Text]) [Text]
dequeueClaimStatement =
  Statement.preparable
    ( "update dbos.workflow_status set status = 'PENDING', executor_id = $2, \
      \application_version = $3, started_at_epoch_ms = (extract(epoch from now()) * 1000)::bigint, \
      \rate_limited = $4, updated_at = (extract(epoch from now()) * 1000)::bigint, \
      \application_name = coalesce(application_name, $1), \
      \recovery_attempts = recovery_attempts + 1, \
      \workflow_deadline_epoch_ms = case when workflow_timeout_ms is not null \
      \and workflow_deadline_epoch_ms is null \
      \then (extract(epoch from now()) * 1000)::bigint + workflow_timeout_ms \
      \else workflow_deadline_epoch_ms end \
      \where workflow_uuid = any($5::text[]) and status = 'ENQUEUED' \
      \and ($1::text is null or application_name = $1 or application_name is null) \
      \returning workflow_uuid" )
    ( contramap (\(app, _, _, _, _) -> app) (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap (\(_, executor, _, _, _) -> executor) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, version, _, _) -> version) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, _, rateLimited, _) -> rateLimited) (Encoders.param (Encoders.nonNullable Encoders.bool))
        <> contramap (\(_, _, _, _, candidates) -> candidates) (Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text))))
    )
    (Decoders.rowList (Decoders.column (Decoders.nonNullable Decoders.text)))

-- | One head per partition with no pending work, for the partition sweep.
-- @$4@ bounds the sweep, @$5@ is the @is_latest@ flag. The two orderings are
-- separate statements: a bounded sweep takes a random sample of partitions,
-- an unbounded one walks the keys in order.
partitionSweepCandidatesStatement :: Statement.Statement (Text, Maybe Text, Text, Int64, Bool) [Text]
partitionSweepCandidatesStatement = partitionSweepCandidates "partitions.pk asc"

partitionSweepRandomCandidatesStatement :: Statement.Statement (Text, Maybe Text, Text, Int64, Bool) [Text]
partitionSweepRandomCandidatesStatement = partitionSweepCandidates "random()"

partitionSweepCandidates :: Text -> Statement.Statement (Text, Maybe Text, Text, Int64, Bool) [Text]
partitionSweepCandidates sweepOrder =
  Statement.preparable
    ( "with recursive partitions as ( \
      \(select min(queue_partition_key) as pk from dbos.workflow_status \
      \where queue_name = $1 and status = 'ENQUEUED' \
      \and ($2::text is null or application_name = $2 or application_name is null) \
      \and queue_partition_key is not null) \
      \union all \
      \(select (select min(queue_partition_key) from dbos.workflow_status \
      \where queue_name = $1 and status = 'ENQUEUED' \
      \and ($2::text is null or application_name = $2 or application_name is null) \
      \and queue_partition_key > partitions.pk) \
      \from partitions where partitions.pk is not null) ), \
      \chosen as ( select partitions.pk from partitions \
      \where partitions.pk is not null and not exists ( \
      \select 1 from dbos.workflow_status where queue_name = $1 and status = 'PENDING' \
      \and queue_partition_key is not null and queue_partition_key = partitions.pk ) \
      \order by "
        <> sweepOrder
        <> " limit $4 ) \
      \select head.workflow_uuid from chosen join lateral ( \
      \select workflow_uuid from dbos.workflow_status \
      \where queue_name = $1 and status = 'ENQUEUED' \
      \and ($2::text is null or application_name = $2 or application_name is null) \
      \and queue_partition_key = chosen.pk \
      \and (application_version = $3 or ($5::bool and application_version is null)) \
      \order by priority asc, created_at asc, workflow_uuid asc limit 1 ) head on true \
      \order by chosen.pk asc" )
    ( contramap (\(queue, _, _, _, _) -> queue) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, app, _, _, _) -> app) (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap (\(_, _, version, _, _) -> version) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, _, sweepLimit, _) -> sweepLimit) (Encoders.param (Encoders.nonNullable Encoders.int8))
        <> contramap (\(_, _, _, _, isLatest) -> isLatest) (Encoders.param (Encoders.nonNullable Encoders.bool))
    )
    (Decoders.rowList (Decoders.column (Decoders.nonNullable Decoders.text)))

-- | Locks the sweep's candidates so a rival cannot take them.
partitionSweepLockStatement :: Statement.Statement ([Text], Text, Text, Maybe Text, Bool) [Text]
partitionSweepLockStatement =
  Statement.preparable
    ( "select workflow_uuid from dbos.workflow_status \
      \where workflow_uuid = any($1::text[]) and status = 'ENQUEUED' \
      \and queue_name = $2 and queue_partition_key is not null \
      \and (application_version = $3 or ($5::bool and application_version is null)) \
      \and ($4::text is null or application_name = $4 or application_name is null) \
      \for update skip locked" )
    ( contramap (\(candidates, _, _, _, _) -> candidates) (Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text))))
        <> contramap (\(_, queue, _, _, _) -> queue) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, version, _, _) -> version) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, _, app, _) -> app) (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap (\(_, _, _, _, isLatest) -> isLatest) (Encoders.param (Encoders.nonNullable Encoders.bool))
    )
    (Decoders.rowList (Decoders.column (Decoders.nonNullable Decoders.text)))

-- | Flips a partition sweep's locked candidates to pending. A sweep is never
-- rate limited, so the flag is written false.
partitionSweepFlipStatement :: Statement.Statement ([Text], Text, Text, Maybe Text, Text, Bool) [Text]
partitionSweepFlipStatement =
  Statement.preparable
    ( "update dbos.workflow_status set status = 'PENDING', executor_id = $5, \
      \application_version = $3, started_at_epoch_ms = (extract(epoch from now()) * 1000)::bigint, \
      \rate_limited = false, updated_at = (extract(epoch from now()) * 1000)::bigint, \
      \application_name = coalesce(application_name, $4), \
      \recovery_attempts = recovery_attempts + 1, \
      \workflow_deadline_epoch_ms = case when workflow_timeout_ms is not null \
      \and workflow_deadline_epoch_ms is null \
      \then (extract(epoch from now()) * 1000)::bigint + workflow_timeout_ms \
      \else workflow_deadline_epoch_ms end \
      \where workflow_uuid = any($1::text[]) and status = 'ENQUEUED' \
      \and queue_name = $2 and queue_partition_key is not null \
      \and (application_version = $3 or ($6::bool and application_version is null)) \
      \and ($4::text is null or application_name = $4 or application_name is null) \
      \returning workflow_uuid" )
    ( contramap (\(candidates, _, _, _, _, _) -> candidates) (Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text))))
        <> contramap (\(_, queue, _, _, _, _) -> queue) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, version, _, _, _) -> version) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, _, app, _, _) -> app) (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap (\(_, _, _, _, executor, _) -> executor) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, _, _, _, isLatest) -> isLatest) (Encoders.param (Encoders.nonNullable Encoders.bool))
    )
    (Decoders.rowList (Decoders.column (Decoders.nonNullable Decoders.text)))

-- Debounce: the delayed-workflow bounce (postgres.rs debounce_delayed_workflow)

-- | The bounce: extends a delayed row's deadline without ever pushing it
-- past its debounce deadline, replaces its inputs, and returns the workflow
-- it bounced, if any.
data DebounceBounceParams = DebounceBounceParams
  { debounceBounceWorkflowName    :: Text,
    debounceBounceQueueName       :: Text,
    debounceBounceDeduplicationId :: Text,
    debounceBounceDelayUntil      :: Int64,
    debounceBounceSerialization   :: Maybe Text,
    debounceBounceApplicationName :: Maybe Text,
    debounceBounceClassName       :: Maybe Text,
    debounceBounceConfigName      :: Maybe Text
  }

debounceBounceStatement :: Statement.Statement DebounceBounceParams (Maybe Text)
debounceBounceStatement =
  Statement.preparable
    ( "update dbos.workflow_status set \
      \delay_until_epoch_ms = case when debounce_deadline_epoch_ms is not null \
      \and debounce_deadline_epoch_ms < $4 then debounce_deadline_epoch_ms else $4 end, \
      \serialization = $5, \
      \updated_at = (extract(epoch from now()) * 1000)::bigint, \
      \application_name = coalesce(application_name, $6) \
      \where name = $1 and queue_name = $2 and deduplication_id = $3 \
      \and class_name is not distinct from $7 and config_name is not distinct from $8 \
      \and status = 'DELAYED' and is_debounced = true \
      \and ($6::text is null or application_name = $6 or application_name is null) \
      \returning workflow_uuid" )
    ( contramap (.debounceBounceWorkflowName) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (.debounceBounceQueueName) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (.debounceBounceDeduplicationId) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (.debounceBounceDelayUntil) (Encoders.param (Encoders.nonNullable Encoders.int8))
        <> contramap (.debounceBounceSerialization) (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap (.debounceBounceApplicationName) (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap (.debounceBounceClassName) (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap (.debounceBounceConfigName) (Encoders.param (Encoders.nullable Encoders.text))
    )
    (Decoders.rowMaybe (Decoders.column (Decoders.nonNullable Decoders.text)))

-- | Records a bounce's replacement inputs where readers look first. An
-- upsert, because a workflow enqueued before migration 109 has no row.
-- Mirrors the oracle's @workflow_input@ write on a bounce.
debounceInputStatement :: Statement.Statement (Text, Maybe Text) Int64
debounceInputStatement =
  Statement.preparable sql encoder (Decoders.rowsAffected)
  where
    sql =
      "insert into dbos.workflow_input (workflow_uuid, inputs) values ($1, $2) \
      \on conflict (workflow_uuid) do update set inputs = excluded.inputs"
    encoder =
      contramap fst (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap snd (Encoders.param (Encoders.nullable Encoders.text))

-- | The workflow holding a deduplication key in a queue, deliberately
-- unscoped by application: the bounce is a global address.
data DebounceHolderRaw = DebounceHolderRaw
  { debounceHolderWorkflowId      :: Text,
    debounceHolderIsDebounced     :: Bool,
    debounceHolderWorkflowName    :: Maybe Text,
    debounceHolderClassName       :: Maybe Text,
    debounceHolderConfigName      :: Maybe Text,
    debounceHolderApplicationName :: Maybe Text
  }

debounceHolderStatement :: Statement.Statement (Text, Text) (Maybe DebounceHolderRaw)
debounceHolderStatement =
  Statement.preparable
    ( "select workflow_uuid, is_debounced, name, class_name::text, config_name::text, application_name \
      \from dbos.workflow_status where queue_name = $1 and deduplication_id = $2" )
    ( contramap fst (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap snd (Encoders.param (Encoders.nonNullable Encoders.text))
    )
    ( Decoders.rowMaybe
        ( DebounceHolderRaw
            <$> Decoders.column (Decoders.nonNullable Decoders.text)
            <*> Decoders.column (Decoders.nonNullable Decoders.bool)
            <*> Decoders.column (Decoders.nullable Decoders.text)
            <*> Decoders.column (Decoders.nullable Decoders.text)
            <*> Decoders.column (Decoders.nullable Decoders.text)
            <*> Decoders.column (Decoders.nullable Decoders.text)
        )
    )

-- Schedules: the cron registry (postgres.rs schedule methods)

-- | A schedule row as the registry stores it: every column is text except
-- @automatic_backfill@, and @last_fired_at@ holds an ISO-8601 instant rather
-- than epoch milliseconds. The fallible mapping to the domain record — the
-- status spelling and the instant — lives with the mapper in
-- @DBOS.SystemDB.Postgres@, not here.
data ScheduleRowRaw = ScheduleRowRaw
  { scheduleRowId                :: Text,
    scheduleRowName              :: Text,
    scheduleRowWorkflowName      :: Text,
    scheduleRowWorkflowClassName :: Maybe Text,
    scheduleRowExpression        :: Text,
    scheduleRowStatus            :: Text,
    scheduleRowContext           :: Text,
    scheduleRowLastFiredAt       :: Maybe Text,
    scheduleRowAutomaticBackfill :: Bool,
    scheduleRowCronTimezone      :: Maybe Text,
    scheduleRowQueueName         :: Maybe Text,
    scheduleRowApplicationName   :: Maybe Text
  }

-- | The schedule columns every schedule read selects, in the oracle's order.
scheduleColumns :: Text
scheduleColumns =
  "schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, status, \
  \context, last_fired_at, automatic_backfill, cron_timezone, queue_name, application_name"

scheduleRowDecoder :: Decoders.Row ScheduleRowRaw
scheduleRowDecoder =
  ScheduleRowRaw
    <$> Decoders.column (Decoders.nonNullable Decoders.text)
    <*> Decoders.column (Decoders.nonNullable Decoders.text)
    <*> Decoders.column (Decoders.nonNullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nonNullable Decoders.text)
    <*> Decoders.column (Decoders.nonNullable Decoders.text)
    <*> Decoders.column (Decoders.nonNullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nonNullable Decoders.bool)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)
    <*> Decoders.column (Decoders.nullable Decoders.text)

-- | The owner of a schedule row, if the row exists at all: the existence
-- probe and the owner resolve @create_schedule@ and @upsert_schedule_on@
-- run before their writes. A peer's row is still returned.
scheduleOwnerStatement :: Statement.Statement Text (Maybe (Maybe Text))
scheduleOwnerStatement =
  Statement.preparable
    "select application_name from dbos.workflow_schedules where schedule_name = $1"
    (Encoders.param (Encoders.nonNullable Encoders.text))
    (Decoders.rowMaybe (Decoders.column (Decoders.nullable Decoders.text)))

-- | The twelve values a schedule insert or upsert writes. The id and the
-- owner are resolved by the caller: the id's fallback generates a UUID once
-- outside the retry (a fresh one per attempt would insert a second row), and
-- the owner is the resolve's answer.
data ScheduleInsertParams = ScheduleInsertParams
  { scheduleInsertId                :: Text,
    scheduleInsertName              :: Text,
    scheduleInsertWorkflowName      :: Text,
    scheduleInsertWorkflowClassName :: Maybe Text,
    scheduleInsertExpression        :: Text,
    scheduleInsertStatus            :: Text,
    scheduleInsertContext           :: Text,
    scheduleInsertLastFiredAt       :: Maybe Text,
    scheduleInsertAutomaticBackfill :: Bool,
    scheduleInsertCronTimezone      :: Maybe Text,
    scheduleInsertQueueName         :: Maybe Text,
    scheduleInsertApplicationName   :: Maybe Text
  }
  deriving stock (Eq, Show)

scheduleInsertEncoder :: Encoders.Params ScheduleInsertParams
scheduleInsertEncoder =
  contramap (.scheduleInsertId) (Encoders.param (Encoders.nonNullable Encoders.text))
    <> contramap (.scheduleInsertName) (Encoders.param (Encoders.nonNullable Encoders.text))
    <> contramap (.scheduleInsertWorkflowName) (Encoders.param (Encoders.nonNullable Encoders.text))
    <> contramap (.scheduleInsertWorkflowClassName) (Encoders.param (Encoders.nullable Encoders.text))
    <> contramap (.scheduleInsertExpression) (Encoders.param (Encoders.nonNullable Encoders.text))
    <> contramap (.scheduleInsertStatus) (Encoders.param (Encoders.nonNullable Encoders.text))
    <> contramap (.scheduleInsertContext) (Encoders.param (Encoders.nonNullable Encoders.text))
    <> contramap (.scheduleInsertLastFiredAt) (Encoders.param (Encoders.nullable Encoders.text))
    <> contramap (.scheduleInsertAutomaticBackfill) (Encoders.param (Encoders.nonNullable Encoders.bool))
    <> contramap (.scheduleInsertCronTimezone) (Encoders.param (Encoders.nullable Encoders.text))
    <> contramap (.scheduleInsertQueueName) (Encoders.param (Encoders.nullable Encoders.text))
    <> contramap (.scheduleInsertApplicationName) (Encoders.param (Encoders.nullable Encoders.text))

-- | Registers a schedule: a plain insert, so the unique index refuses a
-- name already taken. Mirrors @create_schedule@'s insert.
scheduleInsertStatement :: Statement.Statement ScheduleInsertParams ()
scheduleInsertStatement =
  Statement.preparable
    ( "insert into dbos.workflow_schedules \
      \(schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, status, \
      \context, last_fired_at, automatic_backfill, cron_timezone, queue_name, application_name) \
      \values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)" )
    scheduleInsertEncoder
    Decoders.noResult

-- | Registers or re-registers a schedule. The conflict clause takes the
-- definition columns from the new row and leaves @schedule_id@, @status@ and
-- @last_fired_at@ stored, so a redeployment cannot restart a schedule or
-- forget where it had got to; ownership is claimed, never taken
-- (@COALESCE@). Mirrors @upsert_schedule_on@'s insert.
scheduleUpsertStatement :: Statement.Statement ScheduleInsertParams ()
scheduleUpsertStatement =
  Statement.preparable
    ( "insert into dbos.workflow_schedules \
      \(schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, status, \
      \context, last_fired_at, automatic_backfill, cron_timezone, queue_name, application_name) \
      \values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
      \on conflict (schedule_name) do update set \
      \workflow_name = excluded.workflow_name, \
      \workflow_class_name = excluded.workflow_class_name, \
      \schedule = excluded.schedule, \
      \context = excluded.context, \
      \automatic_backfill = excluded.automatic_backfill, \
      \cron_timezone = excluded.cron_timezone, \
      \queue_name = excluded.queue_name, \
      \application_name = coalesce(dbos.workflow_schedules.application_name, \
      \                            excluded.application_name)" )
    scheduleInsertEncoder
    Decoders.noResult

-- | One schedule by name, unscoped: a peer's schedule is still returned.
-- Mirrors @get_schedule@'s select.
scheduleByNameStatement :: Statement.Statement Text (Maybe ScheduleRowRaw)
scheduleByNameStatement =
  Statement.preparable
    ("select " <> scheduleColumns <> " from dbos.workflow_schedules where schedule_name = $1")
    (Encoders.param (Encoders.nonNullable Encoders.text))
    (Decoders.rowMaybe scheduleRowDecoder)

-- | Everything @list_schedules@ binds: the three narrowings, and the
-- application scope already resolved to the list it means (@Nothing@ narrows
-- nothing). Static SQL with empty-array guards rather than the oracle's
-- built-up query; the prefix patterns arrive already escaped, each ending in
-- @%@.
data ScheduleListParams = ScheduleListParams
  { listScheduleStatuses      :: [Text],
    listScheduleWorkflowNames :: [Text],
    listScheduleNamePrefixes  :: [Text],
    listScheduleApplications  :: Maybe [Text]
  }
  deriving stock (Eq, Show)

-- | Lists schedules, every narrowing an ANDed guard, ordered by name. The
-- application scope includes the unclaimed rows, because a listing is a
-- search rather than an addressed read. Mirrors @list_schedules@.
scheduleListStatement :: Statement.Statement ScheduleListParams [ScheduleRowRaw]
scheduleListStatement =
  Statement.preparable
    ( "select " <> scheduleColumns <> " from dbos.workflow_schedules \
      \where (cardinality($1::text[]) = 0 or status = any($1::text[])) \
      \and (cardinality($2::text[]) = 0 or workflow_name = any($2::text[])) \
      \and (cardinality($3::text[]) = 0 or schedule_name like any($3::text[])) \
      \and ($4::text[] is null or application_name = any($4) or application_name is null) \
      \order by schedule_name" )
    encoder
    (Decoders.rowList scheduleRowDecoder)
  where
    encoder =
      contramap (.listScheduleStatuses) (Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text))))
        <> contramap (.listScheduleWorkflowNames) (Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text))))
        <> contramap (.listScheduleNamePrefixes) (Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text))))
        <> contramap (.listScheduleApplications) (Encoders.param (Encoders.nullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text))))

-- | Whether a schedule exists, for the empty update that still has to say
-- whether the name is real. The explicit @::int4@ is the oracle's, chosen
-- so the decode is one type on every backend.
scheduleExistsStatement :: Statement.Statement Text (Maybe Int32)
scheduleExistsStatement =
  Statement.preparable
    "select 1::int4 from dbos.workflow_schedules where schedule_name = $1"
    (Encoders.param (Encoders.nonNullable Encoders.text))
    (Decoders.rowMaybe (Decoders.column (Decoders.nonNullable Decoders.int4)))

-- | Everything @update_schedule@ writes: the name, and each definition
-- column paired with whether the update names it. Static SQL with
-- @CASE WHEN@ guards rather than the oracle's built-up statement list: a
-- @Leave@ keeps the stored value, and a @Set Nothing@ clears a nullable
-- column, which writing back the merged value could not express.
data ScheduleUpdateParams = ScheduleUpdateParams
  { updateScheduleName                 :: Text,
    updateScheduleSetExpression        :: Bool,
    updateScheduleExpression           :: Text,
    updateScheduleSetContext           :: Bool,
    updateScheduleContext              :: Text,
    updateScheduleSetAutomaticBackfill :: Bool,
    updateScheduleAutomaticBackfill    :: Bool,
    updateScheduleSetCronTimezone      :: Bool,
    updateScheduleCronTimezone         :: Maybe Text,
    updateScheduleSetQueueName         :: Bool,
    updateScheduleQueueName            :: Maybe Text
  }
  deriving stock (Eq, Show)

scheduleUpdateStatement :: Statement.Statement ScheduleUpdateParams Int64
scheduleUpdateStatement =
  Statement.preparable
    ( "update dbos.workflow_schedules set \
      \schedule = case when $2 then $3 else schedule end, \
      \context = case when $4 then $5 else context end, \
      \automatic_backfill = case when $6 then $7 else automatic_backfill end, \
      \cron_timezone = case when $8 then $9 else cron_timezone end, \
      \queue_name = case when $10 then $11 else queue_name end \
      \where schedule_name = $1" )
    encoder
    (Decoders.rowsAffected)
  where
    encoder =
      contramap (.updateScheduleName) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (.updateScheduleSetExpression) (Encoders.param (Encoders.nonNullable Encoders.bool))
        <> contramap (.updateScheduleExpression) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (.updateScheduleSetContext) (Encoders.param (Encoders.nonNullable Encoders.bool))
        <> contramap (.updateScheduleContext) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (.updateScheduleSetAutomaticBackfill) (Encoders.param (Encoders.nonNullable Encoders.bool))
        <> contramap (.updateScheduleAutomaticBackfill) (Encoders.param (Encoders.nonNullable Encoders.bool))
        <> contramap (.updateScheduleSetCronTimezone) (Encoders.param (Encoders.nonNullable Encoders.bool))
        <> contramap (.updateScheduleCronTimezone) (Encoders.param (Encoders.nullable Encoders.text))
        <> contramap (.updateScheduleSetQueueName) (Encoders.param (Encoders.nonNullable Encoders.bool))
        <> contramap (.updateScheduleQueueName) (Encoders.param (Encoders.nullable Encoders.text))

-- | Pauses or resumes a schedule. Mirrors @set_schedule_status@.
setScheduleStatusStatement :: Statement.Statement (Text, Text) Int64
setScheduleStatusStatement =
  Statement.preparable
    "update dbos.workflow_schedules set status = $2 where schedule_name = $1"
    ( contramap fst (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap snd (Encoders.param (Encoders.nonNullable Encoders.text))
    )
    (Decoders.rowsAffected)

-- | Records a firing. The no-rows outcome is deliberately not reported: this
-- races a concurrent delete, and the scheduler loop has nothing to absorb.
-- Mirrors @update_schedule_last_fired_at@.
updateScheduleLastFiredAtStatement :: Statement.Statement (Text, Text) ()
updateScheduleLastFiredAtStatement =
  Statement.preparable
    "update dbos.workflow_schedules set last_fired_at = $2 where schedule_name = $1"
    ( contramap fst (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap snd (Encoders.param (Encoders.nonNullable Encoders.text))
    )
    Decoders.noResult

-- | Deletes a schedule. Deleting nothing is success, as in the oracle.
deleteScheduleStatement :: Statement.Statement Text ()
deleteScheduleStatement =
  Statement.preparable
    "delete from dbos.workflow_schedules where schedule_name = $1"
    (Encoders.param (Encoders.nonNullable Encoders.text))
    Decoders.noResult

-- Reads: everything sent and everything published (postgres.rs
-- get_all_notifications and get_all_events)

-- | A message a workflow was sent, read or not. @consumed@ rather than a
-- delete on receive, so the read reports everything and not merely what is
-- still waiting.
data NotificationRecordRaw = NotificationRecordRaw
  { notificationRawMessageUuid   :: Text,
    notificationRawTopic         :: Maybe Text,
    notificationRawMessage       :: Text,
    notificationRawSerialization :: Maybe Text,
    notificationRawCreatedAt     :: Int64,
    notificationRawConsumed      :: Bool
  }

-- | Every message a workflow was sent, in arrival order.
allNotificationsStatement :: Statement.Statement Text [NotificationRecordRaw]
allNotificationsStatement =
  Statement.preparable
    ( "select message_uuid, topic::text, message, serialization, created_at_epoch_ms, consumed \
      \from dbos.notifications where destination_uuid = $1 order by created_at_epoch_ms" )
    (Encoders.param (Encoders.nonNullable Encoders.text))
    ( Decoders.rowList
        ( NotificationRecordRaw
            <$> Decoders.column (Decoders.nonNullable Decoders.text)
            <*> Decoders.column (Decoders.nullable Decoders.text)
            <*> Decoders.column (Decoders.nonNullable Decoders.text)
            <*> Decoders.column (Decoders.nullable Decoders.text)
            <*> Decoders.column (Decoders.nonNullable Decoders.int8)
            <*> Decoders.column (Decoders.nonNullable Decoders.bool)
        )
    )

-- | An event a workflow published.
data EventRecordRaw = EventRecordRaw
  { eventRawKey           :: Text,
    eventRawValue         :: Text,
    eventRawSerialization :: Maybe Text
  }

-- | Every event a workflow published, ordered by key (the table's primary
-- key orders by workflow first, so the order is stated, not inherited).
allEventsStatement :: Statement.Statement Text [EventRecordRaw]
allEventsStatement =
  Statement.preparable
    "select key, value, serialization from dbos.workflow_events where workflow_uuid = $1 order by key"
    (Encoders.param (Encoders.nonNullable Encoders.text))
    ( Decoders.rowList
        ( EventRecordRaw
            <$> Decoders.column (Decoders.nonNullable Decoders.text)
            <*> Decoders.column (Decoders.nonNullable Decoders.text)
            <*> Decoders.column (Decoders.nullable Decoders.text)
        )
    )

-- Child workflows: the parent's launch step (postgres.rs
-- record_child_workflow_on)

-- | The launch of one child: whose step it occupies, under what name, and
-- the half pair of timestamps (a start without a completion measures
-- nothing, so the completion is stamped only when a start is offered).
data RecordChildWorkflowParams = RecordChildWorkflowParams
  { recordChildParentId        :: Text,
    recordChildChildId         :: Text,
    recordChildStepId          :: Int,
    recordChildStepName        :: Text,
    recordChildStartedAt       :: Maybe Int64,
    recordChildCompletedAt     :: Maybe Int64,
    recordChildApplicationName :: Maybe Text
  }

-- | Records the launch, and reports back the stored child id: the same child
-- is an idempotent retry, a different one is nondeterminism. The self-update
-- reads the row back rather than writing it, so a retry stamps a new clock
-- reading without failing the comparison.
recordChildWorkflowStatement :: RecordChildWorkflowParams -> Statement.Statement () (Maybe Text)
recordChildWorkflowStatement params =
  sqlQueryTypedStatement [typedSql|
    insert into dbos.operation_outputs
      (workflow_uuid, function_id, function_name, child_workflow_id,
       started_at_epoch_ms, completed_at_epoch_ms, application_name)
    values
      (${recordChildParentId}, ${recordChildStepId}, ${recordChildStepName}, ${recordChildChildId},
       ${recordChildStartedAt}, ${recordChildCompletedAt}, ${recordChildApplicationName})
    on conflict (workflow_uuid, function_id) do update
    set child_workflow_id = dbos.operation_outputs.child_workflow_id
    returning child_workflow_id
  |]
  where
    RecordChildWorkflowParams {..} = params
