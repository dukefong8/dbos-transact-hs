{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

module DBOS.SystemDB.Types
  ( IdempotencyKey (..),
    MessageUUID (..),
    NotificationRow (..),
    QueueName (..),
    SendMessage (..),
    Topic (..),
    Timestamp (..),
    Duration (..),
    WorkflowId (..),
    WorkflowName (..),
    ExecutorId (..),
    ApplicationVersion (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    WorkflowStatus (..),
    WorkflowStatusDecodeError (..),
    parseWorkflowStatus,
    workflowStatusText,
    isTerminal,
    internalQueueName,
    message,
    messageTo,
    messageUUIDForSend,
    notificationRowForMessage,
    nullTopicSentinel,
    timestampFromEpochMs,
    timestampToEpochMs,
    timestampNow,
    timestampToSystemTime,
    timestampFromSystemTime,
    durationFromMs,
    durationFromSecs,
    durationAsMillis,
    addTimeout,
    durationSince,
    timestampToIso8601,
    timestampFromIso8601,
    NewWorkflow (..),
    WorkflowRecord (..),
    Change (..),
    Submission (..),
    OutcomeWrite (..),
    WorkflowDelay (..),
    StepTiming (..),
    RateLimit (..),
    RenameFrom (..),
    RenameBatching (..),
    ApplicationRowCounts (..),
    OnExistingQueue (..),
    QueueRecord (..),
    QueueUpdate (..),
    ResolvedLimits (..),
    NewQueue (..),
    ScheduleStatus (..),
    ScheduleRecord (..),
    NewSchedule (..),
    ScheduleUpdate (..),
    ScheduleFilter (..),
    Applications (..),
    WorkflowFilter (..),
    StepRecord (..),
    Outcome (..),
    AwaitedOutcome (..),
    EncodedValue (..),
    EventRecord (..),
    StreamRead (..),
    StreamRecord (..),
    NotificationRecord (..),
    VersionInfo (..),
    DebounceRequest (..),
    Debounce (..),
    DebounceHolder (..),
    Fork (..),
    ForkPoint (..),
    ForkOptions (..),
    GetEventCaller (..),
    InitWorkflowCaller (..),
    WorkflowInitResult (..),
    WrittenBy (..),
    newWorkflow,
    validateNewWorkflow,
    validateAttributes,
    initialStatus,
    sleepStepName,
    recvStepName,
    defaultChange,
    changeSet,
    changeIsLeave,
    claimsOwnership,
    resolveWorkflowDelay,
    isValidApplicationName,
    renameFromApplication,
    defaultRenameBatchSize,
    defaultRenameBatching,
    zeroRowCounts,
    sendStepName,
    sendBulkStepName,
    writeStreamStepName,
    closeStreamStepName,
    setEventStepName,
    getEventStepName,
    getResultStepName,
    selectWorkflowStepName,
    selectStepStepName,
    debounceStepName,
    cancelWorkflowStepName,
    resumeWorkflowStepName,
    deleteWorkflowStepName,
    forkWorkflowStepName,
    setWorkflowDelayStepName,
    updateWorkflowAttributesStepName,
    listWorkflowsStepName,
    listWorkflowStepsStepName,
    createScheduleStepName,
    upsertScheduleStepName,
    getScheduleStepName,
    listSchedulesStepName,
    updateScheduleStepName,
    pauseScheduleStepName,
    resumeScheduleStepName,
    deleteScheduleStepName,
    streamClosedSentinel,
    dequeueSweepCap,
    secondsDuration,
    millisDuration,
    durationIsZero,
    newQueue,
    queueHasPartitionLimits,
    queueIsLegacyPartitioned,
    queueResolvedLimits,
    resolvedIsPartitioned,
    defaultQueueUpdate,
    isQueueUpdateEmpty,
    applyQueueUpdate,
    scheduleStatusText,
    parseScheduleStatus,
    newSchedule,
    defaultScheduleUpdate,
    isScheduleUpdateEmpty,
    defaultScheduleFilter,
    defaultWorkflowFilter,
    forkNew,
    forkValidate,
    forkOptionsValidate,
    defaultForkOptions,
    debounceValidate,
    outcomeStatus,
    outcomeColumns,
  )
where

import DBOS.Prelude
import Data.Char (isAscii, isAsciiLower, isDigit)
import Data.Int (Int64)
import Data.List (find)
import Data.Maybe (fromMaybe, isJust)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding (encodeUtf8)
import Data.Word (Word32, Word64)
import Data.Aeson (FromJSON (..), ToJSON (..), Value (..), eitherDecodeStrict, object, withObject, withScientific, withText, (.:), (.=))
import DBOS.SystemDB.Error (Error, invalidInput)
import Control.Monad (guard)
import Control.Monad.Class.MonadTime (MonadTime, getCurrentTime)
import Data.Time.Clock (nominalDiffTimeToSeconds)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime, utcTimeToPOSIXSeconds)
import Data.Time.Clock.System (SystemTime (..))
import Data.Time.Format.ISO8601 (iso8601ParseM, iso8601Show)
import Data.Time.LocalTime (zonedTimeToUTC)

newtype Topic = Topic Text
  deriving stock (Eq, Show)

newtype IdempotencyKey = IdempotencyKey Text
  deriving stock (Eq, Show)

newtype MessageUUID = MessageUUID Text
  deriving stock (Eq, Show)

newtype QueueName = QueueName Text
  deriving stock (Eq, Show)

data SendMessage = SendMessage
  { sendDestinationId  :: WorkflowId,
    sendMessageBody    :: SerializedWorkflowValue,
    sendTopic          :: Maybe Topic,
    sendIdempotencyKey :: Maybe IdempotencyKey
  }
  deriving stock (Eq, Show)

data NotificationRow = NotificationRow
  { notificationDestinationId :: WorkflowId,
    notificationTopic         :: Text,
    notificationMessage       :: SerializedWorkflowValue,
    notificationMessageUUID   :: MessageUUID,
    notificationConsumed      :: Bool
  }
  deriving stock (Eq, Show)

nullTopicSentinel :: Text
nullTopicSentinel = "__null__topic__"

-- | The queue abandoned work returns to. Mirrors @INTERNAL_QUEUE@: recovery
-- is a re-enqueue, and whichever executor next polls the queue runs it.
internalQueueName :: QueueName
internalQueueName = QueueName "_dbos_internal_queue"

-- | A message to a workflow on the default topic, mirroring @Message::new@.
message :: WorkflowId -> SerializedWorkflowValue -> SendMessage
message destination body =
  SendMessage
    { sendDestinationId = destination,
      sendMessageBody = body,
      sendTopic = Nothing,
      sendIdempotencyKey = Nothing
    }

-- | A message to a workflow on a named topic, mirroring
-- @Message { topic: Some(..), ..Message::new(..) }@.
messageTo :: WorkflowId -> Topic -> SerializedWorkflowValue -> SendMessage
messageTo destination topic body =
  (message destination body) {sendTopic = Just topic}

notificationRowForMessage :: MessageUUID -> SendMessage -> NotificationRow
notificationRowForMessage generatedUUID send =
  NotificationRow
    { notificationDestinationId = send.sendDestinationId,
      notificationTopic = maybe nullTopicSentinel (\(Topic topic) -> topic) send.sendTopic,
      notificationMessage = send.sendMessageBody,
      notificationMessageUUID = messageUUIDForSend generatedUUID send,
      notificationConsumed = False
    }

-- | The stored id for a send: the idempotency key when one is given, the
-- generated fallback otherwise. Both branches are scoped per recipient: the
-- insert ends @ON CONFLICT (message_uuid) DO NOTHING@, so an unscoped
-- fallback shared across destinations would collide with itself and deliver
-- to one destination alone. Mirrors the oracle's @deliver@.
messageUUIDForSend :: MessageUUID -> SendMessage -> MessageUUID
messageUUIDForSend (MessageUUID fallback) send =
  let WorkflowId destination = send.sendDestinationId
   in case send.sendIdempotencyKey of
        Nothing                   -> MessageUUID (fallback <> "::" <> destination)
        Just (IdempotencyKey key) -> MessageUUID (key <> "::" <> destination)


-- | A workflow being created, as opposed to one being read back. Mirrors
-- Rust @NewWorkflow@: a caller sets what creation means, and the database
-- stamps the rest (@status@ from queue and delay, @created_at@/@updated_at@
-- at insert). Lifetimes evaporate — every @&str@ is an owned 'Text'.
data NewWorkflow = NewWorkflow
  { newWorkflowId                 :: Text,
    newWorkflowName               :: Maybe Text,
    newWorkflowClassName          :: Maybe Text,
    newWorkflowConfigName         :: Maybe Text,
    newWorkflowInput              :: Maybe Text,
    newWorkflowSerialization      :: Maybe Text,
    newWorkflowQueueName          :: Maybe Text,
    newWorkflowDeduplicationId    :: Maybe Text,
    newWorkflowPriority           :: Int,
    newWorkflowQueuePartitionKey  :: Maybe Text,
    newWorkflowDelay              :: Maybe Duration,
    newWorkflowIsDebounced        :: Bool,
    newWorkflowDebounceDeadline   :: Maybe Timestamp,
    newWorkflowTimeout            :: Maybe Duration,
    newWorkflowDeadline           :: Maybe Timestamp,
    newWorkflowExecutorId         :: Maybe Text,
    newWorkflowApplicationName    :: Maybe Text,
    newWorkflowApplicationVersion :: Maybe Text,
    newWorkflowApplicationId      :: Maybe Text,
    newWorkflowAuthenticatedUser  :: Maybe Text,
    newWorkflowAuthenticatedRoles :: [Text],
    newWorkflowAssumedRole        :: Maybe Text,
    newWorkflowScheduleName       :: Maybe Text,
    newWorkflowAttributes         :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | A workflow with an id and nothing else set. Mirrors @NewWorkflow::new@.
newWorkflow :: Text -> NewWorkflow
newWorkflow workflowId =
  NewWorkflow
    { newWorkflowId = workflowId,
      newWorkflowName = Nothing,
      newWorkflowClassName = Nothing,
      newWorkflowConfigName = Nothing,
      newWorkflowInput = Nothing,
      newWorkflowSerialization = Nothing,
      newWorkflowQueueName = Nothing,
      newWorkflowDeduplicationId = Nothing,
      newWorkflowPriority = 0,
      newWorkflowQueuePartitionKey = Nothing,
      newWorkflowDelay = Nothing,
      newWorkflowIsDebounced = False,
      newWorkflowDebounceDeadline = Nothing,
      newWorkflowTimeout = Nothing,
      newWorkflowDeadline = Nothing,
      newWorkflowExecutorId = Nothing,
      newWorkflowApplicationName = Nothing,
      newWorkflowApplicationVersion = Nothing,
      newWorkflowApplicationId = Nothing,
      newWorkflowAuthenticatedUser = Nothing,
      newWorkflowAuthenticatedRoles = [],
      newWorkflowAssumedRole = Nothing,
      newWorkflowScheduleName = Nothing,
      newWorkflowAttributes = Nothing
    }

-- | Rejects values the system database will not store. Mirrors
-- @NewWorkflow::validate@: an empty string is not a missing value ('Nothing'
-- is), and a zero delay or timeout is a caller that meant 'Nothing'.
validateNewWorkflow :: NewWorkflow -> Either Error ()
validateNewWorkflow new
  | Text.null new.newWorkflowId = Left (invalidInput "workflow_id" "must not be empty")
  | otherwise = case findEmpty optionalFields of
      Just field -> Left (invalidInput field "must be absent rather than empty")
      Nothing -> case findZero [("delay", new.newWorkflowDelay), ("timeout", new.newWorkflowTimeout)] of
        Just field -> Left (invalidInput field "must be a positive, non-zero duration")
        Nothing    -> validateAttributes new.newWorkflowAttributes
  where
    optionalFields =
      [ ("name", new.newWorkflowName),
        ("class_name", new.newWorkflowClassName),
        ("config_name", new.newWorkflowConfigName),
        ("queue_name", new.newWorkflowQueueName),
        ("deduplication_id", new.newWorkflowDeduplicationId),
        ("queue_partition_key", new.newWorkflowQueuePartitionKey),
        ("schedule_name", new.newWorkflowScheduleName),
        ("input", new.newWorkflowInput),
        ("serialization", new.newWorkflowSerialization),
        ("application_version", new.newWorkflowApplicationVersion)
      ]
    findEmpty fields = case find (maybe False Text.null . snd) fields of
      Just (field, _) -> Just field
      Nothing         -> Nothing
    findZero durations = case find (maybe False durationIsZero . snd) durations of
      Just (field, _) -> Just field
      Nothing         -> Nothing

-- | Rejects attributes that are not a JSON object. The contract is an
-- object, not arbitrary JSON: the @attributes @>@ containment query only
-- matches objects, so a stored array or scalar would silently never match.
-- Mirrors @validate_attributes@.
validateAttributes :: Maybe Text -> Either Error ()
validateAttributes Nothing = Right ()
validateAttributes (Just raw) =
  case eitherDecodeStrict (encodeUtf8 raw) :: Either String Value of
    Right (Object _) -> Right ()
    Right _          -> Left (invalidInput "attributes" "must be a JSON object")
    Left err         -> Left (invalidInput "attributes" ("must be a JSON object: " <> Text.pack err))

-- | The status a workflow starts in, which follows from the queue and the
-- delay — never a caller's choice. Mirrors @NewWorkflow::initial_status@.
initialStatus :: NewWorkflow -> WorkflowStatus
initialStatus new = case (new.newWorkflowQueueName, new.newWorkflowDelay) of
  (Nothing, _)      -> Pending
  (Just _, Nothing) -> Enqueued
  (Just _, Just _)  -> Delayed

-- | A row of @workflow_status@. Mirrors Rust @WorkflowRecord@: shaped like
-- the row, not like the domain — fields mirror columns one for one,
-- including their nullability. Every payload field holds encoded text; the
-- @serialization@ column records which format it is in.
data WorkflowRecord = WorkflowRecord
  { workflowRecordId                 :: WorkflowId,
    workflowRecordStatus             :: WorkflowStatus,
    workflowRecordName               :: Maybe Text,
    workflowRecordClassName          :: Maybe Text,
    workflowRecordConfigName         :: Maybe Text,
    workflowRecordInput              :: Maybe Text,
    workflowRecordOutput             :: Maybe Text,
    workflowRecordError              :: Maybe Text,
    workflowRecordSerialization      :: Maybe Text,
    workflowRecordExecutorId         :: Maybe Text,
    workflowRecordApplicationVersion :: Maybe Text,
    workflowRecordRecoveryAttempts   :: Int64,
    workflowRecordQueueName          :: Maybe Text,
    workflowRecordCreatedAt          :: Timestamp,
    workflowRecordUpdatedAt          :: Timestamp,
    workflowRecordStartedAt          :: Maybe Timestamp,
    workflowRecordCompletedAt        :: Maybe Timestamp,
    workflowRecordForkedFrom         :: Maybe WorkflowId,
    workflowRecordParentWorkflowId   :: Maybe WorkflowId,
    workflowRecordWasForkedFrom      :: Bool,
    workflowRecordOwnerXid           :: Maybe Text,
    workflowRecordApplicationId      :: Maybe Text,
    workflowRecordAuthenticatedUser  :: Maybe Text,
    workflowRecordAuthenticatedRoles :: [Text],
    workflowRecordAssumedRole        :: Maybe Text,
    workflowRecordRequest            :: Maybe Text,
    workflowRecordApplicationName    :: Maybe Text,
    workflowRecordDeduplicationId    :: Maybe Text,
    workflowRecordPriority           :: Int,
    workflowRecordQueuePartitionKey  :: Maybe Text,
    workflowRecordRateLimited        :: Bool,
    workflowRecordScheduleName       :: Maybe Text,
    workflowRecordTimeout            :: Maybe Duration,
    workflowRecordDeadline           :: Maybe Timestamp,
    workflowRecordDelayUntil         :: Maybe Timestamp,
    workflowRecordDebounceDeadline   :: Maybe Timestamp,
    workflowRecordIsDebounced        :: Bool,
    workflowRecordAttributes         :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | The step names the engine records for its own operations. Stored
-- contract: each lands in @operation_outputs.function_name@, where a replay
-- compares it. Mirrors Rust @step_names@.
sleepStepName :: Text
sleepStepName = "DBOS.sleep"

-- | The step name @recv@ records. A cross-SDK constant.
recvStepName :: Text
recvStepName = "DBOS.recv"

-- | Whether a partial update touches a field. The third state a plain
-- 'Maybe' cannot carry: for a nullable column, "leave it alone" and "set it
-- to NULL" are different requests. Mirrors Rust @Change@.
data Change a
  = Leave
  | Set a
  deriving stock (Eq, Show)

-- | Leaves the field alone, so a record update narrows rather than widens.
-- Hand-written because deriving would demand @a@ be empty, which says
-- nothing here. Mirrors @Change::default@.
defaultChange :: Change a
defaultChange = Leave

-- | The value to store, if this changes anything. Mirrors @Change::set@.
changeSet :: Change a -> Maybe a
changeSet change =
  case change of
    Set value -> Just value
    Leave     -> Nothing

-- | Whether this leaves the field alone. Mirrors @Change::is_leave@.
changeIsLeave :: Change a -> Bool
changeIsLeave change =
  case change of
    Leave -> True
    Set _ -> False

-- | Why a workflow is being submitted, which decides whether it may claim a
-- row someone holds. Mirrors Rust @Submission@.
data Submission
  = Fresh
  | Recovery
  | Dequeue
  deriving stock (Eq, Show)

-- | Whether this submission is told it owns the workflow. Recovery and
-- dequeue may claim a row another owner holds; a fresh start may not.
-- Mirrors @Submission::claims_ownership@.
claimsOwnership :: Submission -> Bool
claimsOwnership submission =
  case submission of
    Recovery -> True
    Dequeue  -> True
    Fresh    -> False

-- | The result of trying to record a workflow's final outcome. Mirrors Rust
-- @OutcomeWrite@: losing the race is not an error.
data OutcomeWrite
  = Recorded
  | AlreadyFinished
  deriving stock (Eq, Show)

-- | When a delayed workflow should become eligible to run. A sum type
-- because the two forms are alternatives, not options. Mirrors Rust
-- @WorkflowDelay@.
data WorkflowDelay
  = DelayFor Duration
  | DelayUntil Timestamp
  deriving stock (Eq, Show)

-- | The instant this delay expires, resolved against @now@ if relative.
-- Mirrors @WorkflowDelay::resolve@.
resolveWorkflowDelay :: WorkflowDelay -> Timestamp -> Maybe Timestamp
resolveWorkflowDelay delay now =
  case delay of
    DelayFor duration -> addTimeout now duration
    DelayUntil at     -> Just at

-- | When a step ran, start and finish together. A pair because the database
-- records completed steps; a start with no finish yields no duration.
-- Mirrors Rust @StepTiming@.
data StepTiming = StepTiming
  { stepTimingStartedAt   :: Timestamp,
    stepTimingCompletedAt :: Timestamp
  }
  deriving stock (Eq, Show)

-- | How many workflows a queue may start per window. One value rather than
-- two 'Maybe's: a limit with no window is unenforceable. Mirrors Rust
-- @RateLimit@.
data RateLimit = RateLimit
  { rateLimitLimit  :: Int,
    rateLimitPeriod :: Duration
  }
  deriving stock (Eq, Show)

-- | Whether a name is usable as an application name: 3 to 256 characters of
-- lowercase letters, digits, dashes and underscores. The length is in bytes,
-- which is characters here because nothing outside ASCII passes. Mirrors
-- @is_valid_application_name@.
isValidApplicationName :: Text -> Bool
isValidApplicationName name =
  Text.length name >= 3
    && Text.length name <= 256
    && Text.all validChar name
  where
    -- 'isDigit' alone would admit Unicode decimal digits; conjoining
    -- 'isAscii' keeps the oracle's ASCII-only rule.
    validChar c =
      isAsciiLower c
        || (isDigit c && isAscii c)
        || c == '-'
        || c == '_'

-- | Which rows a rename moves. A sum type rather than a name plus a flag:
-- naming no application while adopting nothing selects no rows at all.
-- Mirrors Rust @RenameFrom@.
data RenameFrom
  = RenameApplication Text
  | RenameApplicationAndUnclaimed Text
  | RenameUnclaimed
  deriving stock (Eq, Show)

-- | The application being renamed, if the source names one. Mirrors
-- @RenameFrom::application@. Spelled @renameFromApplication@ (not
-- @renameApplication@): the class below takes the single-word camelCase
-- name for the engine operation, so the accessor keeps the type name.
renameFromApplication :: RenameFrom -> Maybe Text
renameFromApplication source =
  case source of
    RenameApplication name             -> Just name
    RenameApplicationAndUnclaimed name -> Just name
    RenameUnclaimed                    -> Nothing

-- | The batch size every implementation defaults to.
defaultRenameBatchSize :: Word32
defaultRenameBatchSize = 10000

-- | How a rename moves the rows that do not move atomically. Mirrors Rust
-- @RenameBatching@: terminal workflows and their steps may lag the rest.
data RenameBatching
  = Unbatched
  | Batched Word32
  deriving stock (Eq, Show)

-- | Batched at the shared default. Mirrors @RenameBatching::default@.
defaultRenameBatching :: RenameBatching
defaultRenameBatching = Batched defaultRenameBatchSize

-- | What a rename moved, by table. Mirrors Rust @ApplicationRowCounts@.
data ApplicationRowCounts = ApplicationRowCounts
  { rowCountQueues    :: Word64,
    rowCountSchedules :: Word64,
    rowCountVersions  :: Word64,
    rowCountWorkflows :: Word64,
    rowCountSteps     :: Word64
  }
  deriving stock (Eq, Show)

-- | No rows moved. Mirrors @ApplicationRowCounts::default@.
zeroRowCounts :: ApplicationRowCounts
zeroRowCounts = ApplicationRowCounts 0 0 0 0 0

-- | What registering a queue that already exists should do to it. Never its
-- owner, which moves only by rename. Mirrors Rust @OnExistingQueue@.
data OnExistingQueue
  = UpdateExisting
  | LeaveExisting
  deriving stock (Eq, Show)

-- | The step names the engine records for bulk sends, streams, events,
-- children, selections, management calls and schedules. Stored contract
-- like 'sleepStepName' and 'recvStepName'; most are cross-SDK spellings,
-- documented per name in the oracle. Mirrors Rust @step_names@.
sendStepName :: Text
sendStepName = "DBOS.send"

sendBulkStepName :: Text
sendBulkStepName = "DBOS.sendBulk"

writeStreamStepName :: Text
writeStreamStepName = "DBOS.writeStream"

closeStreamStepName :: Text
closeStreamStepName = "DBOS.closeStream"

setEventStepName :: Text
setEventStepName = "DBOS.setEvent"

getEventStepName :: Text
getEventStepName = "DBOS.getEvent"

getResultStepName :: Text
getResultStepName = "DBOS.getResult"

selectWorkflowStepName :: Text
selectWorkflowStepName = "DBOS.selectWorkflow"

selectStepStepName :: Text
selectStepStepName = "DBOS.selectStep"

debounceStepName :: Text
debounceStepName = "DBOS.debounceDelayedWorkflow"

cancelWorkflowStepName :: Text
cancelWorkflowStepName = "DBOS.cancelWorkflow"

resumeWorkflowStepName :: Text
resumeWorkflowStepName = "DBOS.resumeWorkflow"

deleteWorkflowStepName :: Text
deleteWorkflowStepName = "DBOS.deleteWorkflow"

forkWorkflowStepName :: Text
forkWorkflowStepName = "DBOS.forkWorkflow"

setWorkflowDelayStepName :: Text
setWorkflowDelayStepName = "DBOS.setWorkflowDelay"

updateWorkflowAttributesStepName :: Text
updateWorkflowAttributesStepName = "DBOS.updateWorkflowAttributes"

listWorkflowsStepName :: Text
listWorkflowsStepName = "DBOS.listWorkflows"

listWorkflowStepsStepName :: Text
listWorkflowStepsStepName = "DBOS.listWorkflowSteps"

createScheduleStepName :: Text
createScheduleStepName = "DBOS.createSchedule"

upsertScheduleStepName :: Text
upsertScheduleStepName = "DBOS.upsertSchedule"

getScheduleStepName :: Text
getScheduleStepName = "DBOS.getSchedule"

listSchedulesStepName :: Text
listSchedulesStepName = "DBOS.listSchedules"

updateScheduleStepName :: Text
updateScheduleStepName = "DBOS.updateSchedule"

pauseScheduleStepName :: Text
pauseScheduleStepName = "DBOS.pauseSchedule"

resumeScheduleStepName :: Text
resumeScheduleStepName = "DBOS.resumeSchedule"

deleteScheduleStepName :: Text
deleteScheduleStepName = "DBOS.deleteSchedule"

-- | The closing sentinel of a stream, arriving like any other value.
-- Mirrors @STREAM_CLOSED@.
streamClosedSentinel :: Text
streamClosedSentinel = "__DBOS_STREAM_CLOSED__"

-- | Cap on the partitioned-dequeue sweep. Mirrors
-- @PARTITIONED_DEQUEUE_SWEEP_CAP@.
dequeueSweepCap :: Word32
dequeueSweepCap = 8192


-- | A queue as the registry holds it. Periods are fractional seconds in the
-- schema, read through 'durationFromSecs'. Mirrors Rust @QueueRecord@.
data QueueRecord = QueueRecord
  { queueRecordName                       :: Text,
    queueRecordConcurrency                :: Maybe Int,
    queueRecordWorkerConcurrency          :: Maybe Int,
    queueRecordRateLimit                  :: Maybe RateLimit,
    queueRecordPriorityEnabled            :: Bool,
    queueRecordPartitionQueue             :: Bool,
    queueRecordPartitionConcurrency       :: Maybe Int,
    queueRecordPartitionWorkerConcurrency :: Maybe Int,
    queueRecordPartitionRateLimit         :: Maybe RateLimit,
    queueRecordPollingInterval            :: Duration,
    queueRecordApplicationName            :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | Whether any per-partition limit is set, which is what partitions a
-- queue. Mirrors @QueueRecord::has_partition_limits@.
queueHasPartitionLimits :: QueueRecord -> Bool
queueHasPartitionLimits record =
  case record of
    QueueRecord {queueRecordPartitionConcurrency = Just _, queueRecordPartitionWorkerConcurrency = _, queueRecordPartitionRateLimit = _} -> True
    QueueRecord {queueRecordPartitionConcurrency = _, queueRecordPartitionWorkerConcurrency = Just _, queueRecordPartitionRateLimit = _} -> True
    QueueRecord {queueRecordPartitionConcurrency = _, queueRecordPartitionWorkerConcurrency = _, queueRecordPartitionRateLimit = Just _} -> True
    _                                                                                                                                    -> False

-- | A row written with the deprecated flag and no per-partition limits.
-- Mirrors @QueueRecord::is_legacy_partitioned@.
queueIsLegacyPartitioned :: QueueRecord -> Bool
queueIsLegacyPartitioned record =
  record.queueRecordPartitionQueue && not (queueHasPartitionLimits record)

-- | Every limit on a queue, resolved to the scope it is actually enforced
-- at. Mirrors Rust @ResolvedLimits@.
data ResolvedLimits = ResolvedLimits
  { resolvedConcurrency                :: Maybe Int,
    resolvedWorkerConcurrency          :: Maybe Int,
    resolvedRateLimit                  :: Maybe RateLimit,
    resolvedPartitionConcurrency       :: Maybe Int,
    resolvedPartitionWorkerConcurrency :: Maybe Int,
    resolvedPartitionRateLimit         :: Maybe RateLimit
  }
  deriving stock (Eq, Show)

-- | Whether the queue is partitioned, which any per-partition limit makes
-- it. Mirrors @ResolvedLimits::is_partitioned@.
resolvedIsPartitioned :: ResolvedLimits -> Bool
resolvedIsPartitioned limits =
  case limits of
    ResolvedLimits {resolvedPartitionConcurrency = Just _, resolvedPartitionWorkerConcurrency = _, resolvedPartitionRateLimit = _} -> True
    ResolvedLimits {resolvedPartitionConcurrency = _, resolvedPartitionWorkerConcurrency = Just _, resolvedPartitionRateLimit = _} -> True
    ResolvedLimits {resolvedPartitionConcurrency = _, resolvedPartitionWorkerConcurrency = _, resolvedPartitionRateLimit = Just _} -> True
    _                                                                                                                              -> False

-- | This row's limits, each at the scope it is actually enforced at. The
-- deprecated flag re-scopes rather than adds: under it the queue-wide
-- limits move into the partition fields. Everything that enforces a limit
-- reads it through here, never off the columns. Mirrors
-- @QueueRecord::resolved_limits@.
queueResolvedLimits :: QueueRecord -> ResolvedLimits
queueResolvedLimits record
  | queueIsLegacyPartitioned record =
      ResolvedLimits
        { resolvedConcurrency = Nothing,
          resolvedWorkerConcurrency = Nothing,
          resolvedRateLimit = Nothing,
          resolvedPartitionConcurrency = record.queueRecordConcurrency,
          resolvedPartitionWorkerConcurrency = record.queueRecordWorkerConcurrency,
          resolvedPartitionRateLimit = record.queueRecordRateLimit
        }
  | otherwise =
      ResolvedLimits
        { resolvedConcurrency = record.queueRecordConcurrency,
          resolvedWorkerConcurrency = record.queueRecordWorkerConcurrency,
          resolvedRateLimit = record.queueRecordRateLimit,
          resolvedPartitionConcurrency = record.queueRecordPartitionConcurrency,
          resolvedPartitionWorkerConcurrency = record.queueRecordPartitionWorkerConcurrency,
          resolvedPartitionRateLimit = record.queueRecordPartitionRateLimit
        }

-- | A queue to register, as the caller supplies it. Separate from
-- 'QueueRecord' because registering does not require an owner. Mirrors Rust
-- @NewQueue@.
data NewQueue = NewQueue
  { newQueueName                       :: Text,
    newQueueConcurrency                :: Maybe Int,
    newQueueWorkerConcurrency          :: Maybe Int,
    newQueueRateLimit                  :: Maybe RateLimit,
    newQueuePriorityEnabled            :: Bool,
    newQueuePartitionQueue             :: Bool,
    newQueuePartitionConcurrency       :: Maybe Int,
    newQueuePartitionWorkerConcurrency :: Maybe Int,
    newQueuePartitionRateLimit         :: Maybe RateLimit,
    newQueuePollingInterval            :: Duration,
    newQueueApplicationName            :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | A queue with no limits, polling once a second. Mirrors @NewQueue::new@.
newQueue :: Text -> NewQueue
newQueue name =
  NewQueue
    { newQueueName = name,
      newQueueConcurrency = Nothing,
      newQueueWorkerConcurrency = Nothing,
      newQueueRateLimit = Nothing,
      newQueuePriorityEnabled = False,
      newQueuePartitionQueue = False,
      newQueuePartitionConcurrency = Nothing,
      newQueuePartitionWorkerConcurrency = Nothing,
      newQueuePartitionRateLimit = Nothing,
      newQueuePollingInterval = secondsDuration 1,
      newQueueApplicationName = Nothing
    }

-- | The fields of a registered queue that a partial update may change.
-- Ownership is not here: a queue changes hands only through a rename.
-- Mirrors Rust @QueueUpdate@.
data QueueUpdate = QueueUpdate
  { queueUpdateConcurrency                :: Change (Maybe Int),
    queueUpdateWorkerConcurrency          :: Change (Maybe Int),
    queueUpdateRateLimit                  :: Change (Maybe RateLimit),
    queueUpdatePriorityEnabled            :: Change Bool,
    queueUpdatePartitionQueue             :: Change Bool,
    queueUpdatePartitionConcurrency       :: Change (Maybe Int),
    queueUpdatePartitionWorkerConcurrency :: Change (Maybe Int),
    queueUpdatePartitionRateLimit         :: Change (Maybe RateLimit),
    queueUpdatePollingInterval            :: Change Duration
  }
  deriving stock (Eq, Show)

-- | An update that changes nothing. Mirrors @QueueUpdate::default@.
defaultQueueUpdate :: QueueUpdate
defaultQueueUpdate =
  QueueUpdate
    { queueUpdateConcurrency = Leave,
      queueUpdateWorkerConcurrency = Leave,
      queueUpdateRateLimit = Leave,
      queueUpdatePriorityEnabled = Leave,
      queueUpdatePartitionQueue = Leave,
      queueUpdatePartitionConcurrency = Leave,
      queueUpdatePartitionWorkerConcurrency = Leave,
      queueUpdatePartitionRateLimit = Leave,
      queueUpdatePollingInterval = Leave
    }

-- | Whether this would change nothing. Mirrors @QueueUpdate::is_empty@.
isQueueUpdateEmpty :: QueueUpdate -> Bool
isQueueUpdateEmpty update =
  changeIsLeave update.queueUpdateConcurrency
    && changeIsLeave update.queueUpdateWorkerConcurrency
    && changeIsLeave update.queueUpdateRateLimit
    && changeIsLeave update.queueUpdatePriorityEnabled
    && changeIsLeave update.queueUpdatePartitionQueue
    && changeIsLeave update.queueUpdatePartitionConcurrency
    && changeIsLeave update.queueUpdatePartitionWorkerConcurrency
    && changeIsLeave update.queueUpdatePartitionRateLimit
    && changeIsLeave update.queueUpdatePollingInterval

-- | This update applied to a record, giving the row as it would be after
-- the write. The flag follows the limits, so the merged row carries the
-- flag the write will store. Mirrors @QueueUpdate::apply_to@.
applyQueueUpdate :: QueueUpdate -> QueueRecord -> QueueRecord
applyQueueUpdate update record =
  record
    { queueRecordConcurrency = fromChange update.queueUpdateConcurrency record.queueRecordConcurrency,
      queueRecordWorkerConcurrency = fromChange update.queueUpdateWorkerConcurrency record.queueRecordWorkerConcurrency,
      queueRecordRateLimit = fromChange update.queueUpdateRateLimit record.queueRecordRateLimit,
      queueRecordPriorityEnabled = fromChange update.queueUpdatePriorityEnabled record.queueRecordPriorityEnabled,
      queueRecordPartitionQueue = mergedPartitionQueue,
      queueRecordPartitionConcurrency = mergedPartitionConcurrency,
      queueRecordPartitionWorkerConcurrency = mergedPartitionWorkerConcurrency,
      queueRecordPartitionRateLimit = mergedPartitionRateLimit,
      queueRecordPollingInterval = fromChange update.queueUpdatePollingInterval record.queueRecordPollingInterval
    }
  where
    fromChange change stored = fromMaybe stored (changeSet change)
    mergedPartitionConcurrency = fromChange update.queueUpdatePartitionConcurrency record.queueRecordPartitionConcurrency
    mergedPartitionWorkerConcurrency = fromChange update.queueUpdatePartitionWorkerConcurrency record.queueRecordPartitionWorkerConcurrency
    mergedPartitionRateLimit = fromChange update.queueUpdatePartitionRateLimit record.queueRecordPartitionRateLimit
    mergedPartitionQueue = case changeSet update.queueUpdatePartitionQueue of
      Just flag -> flag
      Nothing
        | changeIsLeave update.queueUpdatePartitionConcurrency
            && changeIsLeave update.queueUpdatePartitionWorkerConcurrency
            && changeIsLeave update.queueUpdatePartitionRateLimit ->
            record.queueRecordPartitionQueue
        | otherwise ->
            isJust mergedPartitionConcurrency
              || isJust mergedPartitionWorkerConcurrency
              || isJust mergedPartitionRateLimit

-- | Whether a schedule fires. Pausing keeps the row and the last firing, so
-- resuming picks up where it left off. Stored uppercase, like
-- 'WorkflowStatus'. Mirrors Rust @ScheduleStatus@.
data ScheduleStatus
  = Active
  | Paused
  deriving stock (Eq, Show)

-- | The stored spelling.
scheduleStatusText :: ScheduleStatus -> Text
scheduleStatusText status =
  case status of
    Active -> "ACTIVE"
    Paused -> "PAUSED"

-- | Parses a stored value, or 'Nothing' for anything unrecognised — a
-- status written by a newer implementation must report, not guess. Mirrors
-- @ScheduleStatus::parse@.
parseScheduleStatus :: Text -> Maybe ScheduleStatus
parseScheduleStatus raw =
  case raw of
    "ACTIVE" -> Just Active
    "PAUSED" -> Just Paused
    _        -> Nothing

-- | A registered schedule, as stored: a definition (expression, workflow,
-- context, queue) plus runtime state (status, last firing). Mirrors Rust
-- @ScheduleRecord@.
data ScheduleRecord = ScheduleRecord
  { scheduleRecordId                :: Text,
    scheduleRecordName              :: Text,
    scheduleRecordWorkflowName      :: Text,
    scheduleRecordWorkflowClassName :: Maybe Text,
    scheduleRecordExpression        :: Text,
    scheduleRecordStatus            :: ScheduleStatus,
    scheduleRecordContext           :: Text,
    scheduleRecordLastFiredAt       :: Maybe Timestamp,
    scheduleRecordAutomaticBackfill :: Bool,
    scheduleRecordCronTimezone      :: Maybe Text,
    scheduleRecordQueueName         :: Maybe Text,
    scheduleRecordApplicationName   :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | A schedule to register, as the caller supplies it. Mirrors Rust
-- @NewSchedule@.
data NewSchedule = NewSchedule
  { newScheduleId                :: Maybe Text,
    newScheduleName              :: Text,
    newScheduleWorkflowName      :: Text,
    newScheduleWorkflowClassName :: Maybe Text,
    newScheduleExpression        :: Text,
    newScheduleStatus            :: ScheduleStatus,
    newScheduleContext           :: Text,
    newScheduleLastFiredAt       :: Maybe Timestamp,
    newScheduleAutomaticBackfill :: Bool,
    newScheduleCronTimezone      :: Maybe Text,
    newScheduleQueueName         :: Maybe Text,
    newScheduleApplicationName   :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | An active schedule with no context. Mirrors @NewSchedule::new@.
newSchedule :: Text -> Text -> Text -> NewSchedule
newSchedule name workflow expression =
  NewSchedule
    { newScheduleId = Nothing,
      newScheduleName = name,
      newScheduleWorkflowName = workflow,
      newScheduleWorkflowClassName = Nothing,
      newScheduleExpression = expression,
      newScheduleStatus = Active,
      newScheduleContext = "null",
      newScheduleLastFiredAt = Nothing,
      newScheduleAutomaticBackfill = False,
      newScheduleCronTimezone = Nothing,
      newScheduleQueueName = Nothing,
      newScheduleApplicationName = Nothing
    }

-- | The fields of a registered schedule that a partial update may change:
-- definition only, never identity, status or last firing. Mirrors Rust
-- @ScheduleUpdate@.
data ScheduleUpdate = ScheduleUpdate
  { scheduleUpdateExpression        :: Change Text,
    scheduleUpdateContext           :: Change Text,
    scheduleUpdateAutomaticBackfill :: Change Bool,
    scheduleUpdateCronTimezone      :: Change (Maybe Text),
    scheduleUpdateQueueName         :: Change (Maybe Text)
  }
  deriving stock (Eq, Show)

-- | An update that changes nothing. Mirrors @ScheduleUpdate::default@.
defaultScheduleUpdate :: ScheduleUpdate
defaultScheduleUpdate =
  ScheduleUpdate
    { scheduleUpdateExpression = Leave,
      scheduleUpdateContext = Leave,
      scheduleUpdateAutomaticBackfill = Leave,
      scheduleUpdateCronTimezone = Leave,
      scheduleUpdateQueueName = Leave
    }

-- | Whether this would change nothing. Mirrors @ScheduleUpdate::is_empty@.
isScheduleUpdateEmpty :: ScheduleUpdate -> Bool
isScheduleUpdateEmpty update =
  changeIsLeave update.scheduleUpdateExpression
    && changeIsLeave update.scheduleUpdateContext
    && changeIsLeave update.scheduleUpdateAutomaticBackfill
    && changeIsLeave update.scheduleUpdateCronTimezone
    && changeIsLeave update.scheduleUpdateQueueName

-- | Which applications' rows a query covers. 'Unset' lets the query decide;
-- 'Any' is the operator's cross-application view; 'Named' names
-- applications plus the unclaimed rows. Mirrors Rust @Applications@.
data Applications
  = Unset
  | Any
  | Named [Text]
  deriving stock (Eq, Show)

-- | Which workflows to list, and how much of each to load. Every field is a
-- narrowing, and the default narrows nothing. Mirrors Rust @WorkflowFilter@.
data WorkflowFilter = WorkflowFilter
  { workflowFilterWorkflowIds         :: [Text],
    workflowFilterWorkflowIdPrefixes  :: [Text],
    workflowFilterApplications        :: Applications,
    workflowFilterNames               :: [Text],
    workflowFilterClassNames          :: [Text],
    workflowFilterConfigNames         :: [Text],
    workflowFilterStatus              :: [WorkflowStatus],
    workflowFilterApplicationVersions :: [Text],
    workflowFilterExecutorIds         :: [Text],
    workflowFilterAuthenticatedUsers  :: [Text],
    workflowFilterQueueNames          :: [Text],
    workflowFilterQueuesOnly          :: Bool,
    workflowFilterScheduleNames       :: [Text],
    workflowFilterDeduplicationIds    :: [Text],
    workflowFilterIsDebounced         :: Maybe Bool,
    workflowFilterParentWorkflowIds   :: [Text],
    workflowFilterHasParent           :: Maybe Bool,
    workflowFilterForkedFrom          :: [Text],
    -- | Whether the workflow is itself a fork, i.e. whether it has a
    -- @forked_from@. Mirrors Rust @is_fork@.
    workflowFilterIsFork            :: Maybe Bool,
    workflowFilterWasForkedFrom       :: Maybe Bool,
    workflowFilterCreatedAfter        :: Maybe Timestamp,
    workflowFilterCreatedBefore       :: Maybe Timestamp,
    workflowFilterCompletedAfter      :: Maybe Timestamp,
    workflowFilterCompletedBefore     :: Maybe Timestamp,
    workflowFilterStartedAfter        :: Maybe Timestamp,
    workflowFilterStartedBefore       :: Maybe Timestamp,
    workflowFilterAttributes          :: Maybe Text,
    workflowFilterLimit               :: Maybe Int64,
    workflowFilterOffset              :: Maybe Int64,
    workflowFilterSortDesc            :: Bool,
    workflowFilterLoadInput           :: Bool,
    workflowFilterLoadOutput          :: Bool
  }
  deriving stock (Eq, Show)

-- | Narrows nothing, and loads everything. Mirrors
-- @WorkflowFilter::default@.
defaultWorkflowFilter :: WorkflowFilter
defaultWorkflowFilter =
  WorkflowFilter
    { workflowFilterWorkflowIds = [],
      workflowFilterWorkflowIdPrefixes = [],
      workflowFilterApplications = Unset,
      workflowFilterNames = [],
      workflowFilterClassNames = [],
      workflowFilterConfigNames = [],
      workflowFilterStatus = [],
      workflowFilterApplicationVersions = [],
      workflowFilterExecutorIds = [],
      workflowFilterAuthenticatedUsers = [],
      workflowFilterQueueNames = [],
      workflowFilterQueuesOnly = False,
      workflowFilterScheduleNames = [],
      workflowFilterDeduplicationIds = [],
      workflowFilterIsDebounced = Nothing,
      workflowFilterParentWorkflowIds = [],
      workflowFilterHasParent = Nothing,
      workflowFilterForkedFrom = [],
      workflowFilterIsFork = Nothing,
      workflowFilterWasForkedFrom = Nothing,
      workflowFilterCreatedAfter = Nothing,
      workflowFilterCreatedBefore = Nothing,
      workflowFilterCompletedAfter = Nothing,
      workflowFilterCompletedBefore = Nothing,
      workflowFilterStartedAfter = Nothing,
      workflowFilterStartedBefore = Nothing,
      workflowFilterAttributes = Nothing,
      workflowFilterLimit = Nothing,
      workflowFilterOffset = Nothing,
      workflowFilterSortDesc = False,
      workflowFilterLoadInput = True,
      workflowFilterLoadOutput = True
    }

-- | Which schedules a listing returns. Every field narrows; an empty filter
-- returns the table. Mirrors Rust @ScheduleFilter@.
data ScheduleFilter = ScheduleFilter
  { scheduleFilterStatuses      :: [ScheduleStatus],
    scheduleFilterWorkflowNames :: [Text],
    scheduleFilterNamePrefixes  :: [Text],
    scheduleFilterApplications  :: Applications
  }
  deriving stock (Eq, Show)

-- | Narrows nothing. Mirrors @ScheduleFilter::default@.
defaultScheduleFilter :: ScheduleFilter
defaultScheduleFilter =
  ScheduleFilter
    { scheduleFilterStatuses = [],
      scheduleFilterWorkflowNames = [],
      scheduleFilterNamePrefixes = [],
      scheduleFilterApplications = Unset
    }

-- | A step's recorded result, as @operation_outputs@ holds it. The field
-- names do not match the column names: @step_id@ and @step_name@ are stored
-- in @function_id@ and @function_name@. Mirrors Rust @StepRecord@.
data StepRecord = StepRecord
  { stepRecordWorkflowId      :: WorkflowId,
    stepRecordStepId          :: Int,
    stepRecordStepName        :: Text,
    stepRecordOutput          :: Maybe Text,
    stepRecordError           :: Maybe Text,
    stepRecordChildWorkflowId :: Maybe WorkflowId,
    stepRecordSerialization   :: Maybe Text,
    stepRecordStartedAt       :: Maybe Timestamp,
    stepRecordCompletedAt     :: Maybe Timestamp
  }
  deriving stock (Eq, Show)

-- | How a step or a workflow ended: with a value, or with an error. A sum
-- type because the two columns are independently nullable and a row carrying
-- both would be simultaneously successful and failed. Mirrors Rust
-- @Outcome@.
data Outcome
  = OutcomeOutput (Maybe Text)
  | OutcomeError Text
  deriving stock (Eq, Show)

-- | The terminal status this outcome puts a workflow in. Mirrors
-- @Outcome::status@.
outcomeStatus :: Outcome -> WorkflowStatus
outcomeStatus outcome =
  case outcome of
    OutcomeOutput _ -> Success
    OutcomeError _  -> Error

-- | The payload columns an outcome writes: a success carries output and no
-- error, a failure the reverse. Mirrors @Outcome::columns@.
outcomeColumns :: Outcome -> (Maybe Text, Maybe Text)
outcomeColumns outcome =
  case outcome of
    OutcomeOutput value -> (value, Nothing)
    OutcomeError message -> (Nothing, Just message)

-- | What an awaited workflow did. 'Nothing' output is a void return, which
-- is a success and not an absent result. Mirrors Rust @AwaitedOutcome@.
data AwaitedOutcome
  = AwaitedSucceeded {awaitedOutput :: Maybe Text, awaitedSerialization :: Maybe Text}
  | AwaitedFailed {awaitedError :: Text, awaitedSerialization :: Maybe Text}
  | AwaitedCancelled
  | AwaitedParked {awaitedRecoveryAttempts :: Int64}
  deriving stock (Eq, Show)

-- | An encoded payload and the format it is encoded in. Payloads cross this
-- boundary as opaque strings; the format travels with the value. Mirrors
-- Rust @EncodedValue@.
data EncodedValue = EncodedValue
  { encodedValue         :: Text,
    encodedSerialization :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | A key/value a workflow published, as @workflow_events@ holds it.
-- Mirrors Rust @EventRecord@.
data EventRecord = EventRecord
  { eventKey           :: Text,
    eventValue         :: Text,
    eventSerialization :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | One offset of a stream, read together with its producer's liveness.
-- Mirrors Rust @StreamRead@.
data StreamRead = StreamRead
  { streamStatus :: WorkflowStatus,
    streamValue  :: Maybe EncodedValue
  }
  deriving stock (Eq, Show)

-- | One entry of a workflow's stream, as @streams@ holds it. Mirrors Rust
-- @StreamRecord@.
data StreamRecord = StreamRecord
  { streamKey           :: Text,
    streamOffset        :: Int,
    streamValue         :: Text,
    streamSerialization :: Maybe Text,
    streamStepId        :: Int
  }
  deriving stock (Eq, Show)

-- | A message sent to a workflow, as @notifications@ holds it. Mirrors Rust
-- @NotificationRecord@.
data NotificationRecord = NotificationRecord
  { notificationRecordMessageUuid   :: Text,
    notificationRecordTopic         :: Maybe Text,
    notificationRecordMessage       :: Text,
    notificationRecordSerialization :: Maybe Text,
    notificationRecordCreatedAt     :: Timestamp,
    notificationRecordConsumed      :: Bool
  }
  deriving stock (Eq, Show)

-- | A registered version of the application. Mirrors Rust @VersionInfo@.
data VersionInfo = VersionInfo
  { versionInfoApplicationName :: Maybe Text,
    versionInfoId              :: Text,
    versionInfoName            :: Text,
    versionInfoTimestamp       :: Timestamp,
    versionInfoCreatedAt       :: Timestamp
  }
  deriving stock (Eq, Show)

-- | A request to debounce a workflow onto a deduplication key. Every field
-- is named at the call site on purpose: five are optional texts, and
-- transposing two would compile. No smart constructor, for the same reason.
-- Mirrors Rust @DebounceRequest@.
data DebounceRequest = DebounceRequest
  { debounceRequestWorkflowName    :: Text,
    debounceRequestClassName       :: Maybe Text,
    debounceRequestConfigName      :: Maybe Text,
    debounceRequestQueueName       :: Text,
    debounceRequestDeduplicationId :: Text,
    debounceRequestDelayUntil      :: Timestamp,
    debounceRequestInputs          :: Maybe Text,
    debounceRequestSerialization   :: Maybe Text,
    debounceRequestApplicationName :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | Rejects the values no writer could have stored, so a bounce cannot
-- silently match nothing. Mirrors @DebounceRequest::validate@.
debounceValidate :: DebounceRequest -> Either Error ()
debounceValidate request
  | Text.null request.debounceRequestWorkflowName =
      Left (invalidInput "workflow_name" "must not be empty")
  | Text.null request.debounceRequestQueueName =
      Left (invalidInput "queue_name" "must not be empty")
  | Text.null request.debounceRequestDeduplicationId =
      Left (invalidInput "deduplication_id" "must not be empty")
  | otherwise = case findEmpty optionals of
      Just field -> Left (invalidInput field "must be absent rather than empty")
      Nothing    -> Right ()
  where
    optionals =
      [ ("class_name", request.debounceRequestClassName),
        ("config_name", request.debounceRequestConfigName),
        ("inputs", request.debounceRequestInputs),
        ("serialization", request.debounceRequestSerialization),
        ("application_name", request.debounceRequestApplicationName)
      ]
    findEmpty fields = case find (maybe False Text.null . snd) fields of
      Just (field, _) -> Just field
      Nothing         -> Nothing

-- | What a debounce did, or why it could not. Three outcomes rather than a
-- flat record of which exactly one group is populated. Mirrors Rust
-- @Debounce@.
data Debounce
  = Debounced {debounceWorkflowId :: Text}
  | DebounceHeld DebounceHolder
  | DebounceUnheld
  deriving stock (Eq, Show)

-- | The workflow holding a deduplication key, described rather than merely
-- reported so the caller can tell a collision from a coincidence. Mirrors
-- Rust @DebounceHolder@.
data DebounceHolder = DebounceHolder
  { debounceHolderWorkflowId      :: Text,
    debounceHolderIsDebounced     :: Bool,
    debounceHolderWorkflowName    :: Maybe Text,
    debounceHolderClassName       :: Maybe Text,
    debounceHolderConfigName      :: Maybe Text,
    debounceHolderApplicationName :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | One workflow to fork, and where its fork picks up. A struct per fork
-- rather than three parallel lists, so lengths cannot disagree. Mirrors
-- Rust @Fork@.
data Fork = Fork
  { forkSourceId  :: Text,
    forkForkedId  :: Maybe Text,
    forkStartStep :: Int
  }
  deriving stock (Eq, Show)

-- | A fork restarting from the beginning, with a generated id. Mirrors
-- @Fork::new@.
forkNew :: Text -> Fork
forkNew source =
  Fork
    { forkSourceId = source,
      forkForkedId = Nothing,
      forkStartStep = 0
    }

-- | Rejects ids the schema cannot key on. Mirrors @Fork::validate@.
forkValidate :: Fork -> Either Error ()
forkValidate fork
  | Text.null fork.forkSourceId =
      Left (invalidInput "source_id" "must not be empty")
  | fork.forkForkedId == Just "" =
      Left (invalidInput "forked_id" "must be absent rather than empty")
  | fork.forkStartStep < 0 =
      Left (invalidInput "start_step" "must not be negative")
  | otherwise = Right ()

-- | Which step a fork restarts from, when the caller wants it worked out
-- rather than stated. 'Step' is prefixed because 'WrittenBy' owns the bare
-- name in this module. Mirrors Rust @ForkPoint@.
data ForkPoint
  = ForkLastFailure
  | ForkLastStep
  | ForkStep Int
  | ForkStepNamed Text
  deriving stock (Eq, Show)

-- | How forked workflows are created. Mirrors Rust @ForkOptions@.
data ForkOptions = ForkOptions
  { forkOptionsApplicationVersion  :: Maybe Text,
    forkOptionsQueueName           :: Maybe Text,
    forkOptionsQueuePartitionKey   :: Maybe Text,
    forkOptionsTimeout             :: Maybe Duration,
    forkOptionsReplacementChildren :: [(Text, Text)]
  }
  deriving stock (Eq, Show)

-- | All options unset. Mirrors @ForkOptions::default@.
defaultForkOptions :: ForkOptions
defaultForkOptions =
  ForkOptions
    { forkOptionsApplicationVersion = Nothing,
      forkOptionsQueueName = Nothing,
      forkOptionsQueuePartitionKey = Nothing,
      forkOptionsTimeout = Nothing,
      forkOptionsReplacementChildren = []
    }

-- | Applies the same rules 'validateNewWorkflow' applies to the same
-- columns, plus the replacement map's own: one child named twice would
-- match a copied step twice. Mirrors @ForkOptions::validate@.
forkOptionsValidate :: ForkOptions -> Either Error ()
forkOptionsValidate options =
  case findEmpty optionals of
    Just field -> Left (invalidInput field "must be absent rather than empty")
    Nothing -> case options.forkOptionsTimeout of
      Just timeout | durationIsZero timeout ->
        Left (invalidInput "timeout" "must be absent rather than zero")
      _ -> validateChildren options.forkOptionsReplacementChildren
  where
    optionals =
      [ ("application_version", options.forkOptionsApplicationVersion),
        ("queue_name", options.forkOptionsQueueName),
        ("queue_partition_key", options.forkOptionsQueuePartitionKey)
      ]
    findEmpty fields = case find (maybe False Text.null . snd) fields of
      Just (field, _) -> Just field
      Nothing         -> Nothing
    validateChildren children = go children []
      where
        go [] _ = Right ()
        go ((original, _) : rest) seen
          | Text.null original =
              Left (invalidInput "replacement_children" "a replaced child id must not be empty")
          | original `elem` seen =
              Left (invalidInput "replacement_children" (original <> " is replaced more than once"))
          | otherwise = go rest (original : seen)

-- | The workflow a @get_event@ runs on behalf of, and the steps it records
-- against. Two step ids because they are not interchangeable: one records
-- the read, the other the deadline. Mirrors Rust @GetEventCaller@.
data GetEventCaller = GetEventCaller
  { getEventCallerWorkflowId    :: WorkflowId,
    getEventCallerStepId        :: Int,
    getEventCallerTimeoutStepId :: Int
  }
  deriving stock (Eq, Show)

-- | The workflow an @init_workflow@ creates a child on behalf of, and the
-- step the start is recorded under. Everything the parent contributes is
-- here, including the child's own id. Mirrors Rust @InitWorkflowCaller@.
data InitWorkflowCaller = InitWorkflowCaller
  { initCallerParentWorkflowId :: WorkflowId,
    initCallerStepId           :: Int,
    initCallerStepName         :: Text,
    initCallerStartedAt        :: Timestamp
  }
  deriving stock (Eq, Show)

-- | What the database said about a workflow after initialising it. Mirrors
-- Rust @WorkflowInitResult@.
data WorkflowInitResult = WorkflowInitResult
  { initResultStatus           :: WorkflowStatus,
    initResultRecoveryAttempts :: Int64,
    initResultDeadline         :: Maybe Timestamp,
    initResultSerialization    :: Maybe Text,
    initResultShouldExecute    :: Bool
  }
  deriving stock (Eq, Show)

-- | Who wrote to a stream, which decides whether the write is itself a
-- durable step. Mirrors Rust @WrittenBy@.
data WrittenBy
  = Workflow
  | Step
  deriving stock (Eq, Show)

-- | Where a workflow is in its lifecycle. Defined here because Rust defines
-- it in @types.rs@; the stored spellings are a wire format shared with every
-- other DBOS implementation. Mirrors Rust @WorkflowStatus@.
data WorkflowStatus
  = Pending
  | Success
  | Error
  | MaxRecoveryAttemptsExceeded
  | Cancelled
  | Enqueued
  | Delayed
  deriving stock (Eq, Show)

newtype WorkflowStatusDecodeError
  = UnknownWorkflowStatus Text
  deriving stock (Eq, Show)

-- | Parses a stored value; an unrecognised spelling reports rather than
-- guessing, because a status written by a newer implementation is a real
-- possibility in a shared database. Mirrors @WorkflowStatus::parse@.
parseWorkflowStatus :: Text -> Either WorkflowStatusDecodeError WorkflowStatus
parseWorkflowStatus raw =
  case raw of
    "PENDING" -> Right Pending
    "SUCCESS" -> Right Success
    "ERROR" -> Right Error
    "MAX_RECOVERY_ATTEMPTS_EXCEEDED" -> Right MaxRecoveryAttemptsExceeded
    "CANCELLED" -> Right Cancelled
    "ENQUEUED" -> Right Enqueued
    "DELAYED" -> Right Delayed
    other -> Left (UnknownWorkflowStatus other)

-- | The stored spelling.
workflowStatusText :: WorkflowStatus -> Text
workflowStatusText status =
  case status of
    Pending                     -> "PENDING"
    Success                     -> "SUCCESS"
    Error                       -> "ERROR"
    MaxRecoveryAttemptsExceeded -> "MAX_RECOVERY_ATTEMPTS_EXCEEDED"
    Cancelled                   -> "CANCELLED"
    Enqueued                    -> "ENQUEUED"
    Delayed                     -> "DELAYED"

-- | Whether the workflow has finished and will not run again. Parked rather
-- than finished, 'MaxRecoveryAttemptsExceeded' can still be resumed.
isTerminal :: WorkflowStatus -> Bool
isTerminal status =
  case status of
    Success   -> True
    Error     -> True
    Cancelled -> True
    _         -> False

-- | Identity and payload wrappers over the oracle's @String@ fields. Rust
-- spells these as bare @String@/@&str@ in @types.rs@ and the modules above
-- it; the newtypes are the port's encoding of the same values, owned by this
-- module so @types.rs@ stays one Haskell module.
newtype WorkflowId = WorkflowId Text
  deriving stock (Eq, Show)

newtype WorkflowName = WorkflowName Text
  deriving stock (Eq, Ord, Show)

newtype ExecutorId = ExecutorId Text
  deriving stock (Eq, Show)

newtype ApplicationVersion = ApplicationVersion Text
  deriving stock (Eq, Show)

newtype Serialization = Serialization Text
  deriving stock (Eq, Show)

data SerializedWorkflowValue = SerializedWorkflowValue
  { serializedText :: Text,
    serializedSerialization :: Maybe Serialization
  }
  deriving stock (Eq, Show)

-- | An instant, as epoch milliseconds. Mirrors Rust @Timestamp@: exactly what
-- the @BIGINT@ columns hold, so reading and writing are lossless. Callers
-- wanting a calendar type convert at the edge ('timestampToSystemTime').
newtype Timestamp = Timestamp Int64
  deriving stock (Eq, Ord)

-- | Mirrors Rust @Timestamp@'s @Display@: @1786492800000ms@.
instance Show Timestamp where
  show (Timestamp ms) = show ms <> "ms"

-- | A span of time. Non-negative by construction: smart constructors return
-- 'Nothing' where Rust @duration_from_ms@/@duration_from_secs@ do (negative,
-- NaN, infinite, or larger than @std::time::Duration@ can hold). Stored as
-- 'NominalDiffTime' so both column units (integer milliseconds, fractional
-- seconds) share one in-memory type.
newtype Duration = Duration NominalDiffTime
  deriving stock (Eq, Ord, Show)

-- | Wraps a stored value. Total, like @Timestamp::from_epoch_ms@.
timestampFromEpochMs :: Int64 -> Timestamp
timestampFromEpochMs = Timestamp

-- | The stored value. Total, like @Timestamp::as_epoch_ms@.
timestampToEpochMs :: Timestamp -> Int64
timestampToEpochMs (Timestamp ms) = ms

-- | The current time, truncated to milliseconds. A clock set before 1970
-- gives the epoch rather than panicking, mirroring @Timestamp::now@.
-- Reads through 'MonadTime' rather than 'IO' so the same call returns real
-- time in production and virtual time under @IOSim@ (ADR-0008, amended).
timestampNow :: MonadTime m => m Timestamp
timestampNow = do
  now <- getCurrentTime
  let ms = truncate (1000 * utcTimeToPOSIXSeconds now) :: Int64
  pure (if ms < 0 then Timestamp 0 else Timestamp ms)

-- | Converts to a 'SystemTime', or 'Nothing' for an instant before the epoch,
-- mirroring @Timestamp::to_system_time@.
timestampToSystemTime :: Timestamp -> Maybe SystemTime
timestampToSystemTime (Timestamp ms)
  | ms < 0 = Nothing
  | otherwise = Just (MkSystemTime secs nanos)
  where
    secs = ms `div` 1000
    nanos = fromIntegral ((ms `mod` 1000) * 1000000) :: Word32

-- | Converts from a 'SystemTime', or 'Nothing' if it cannot be stored: before
-- the epoch, or so far after it that the milliseconds overflow 'Int64'.
-- Mirrors @Timestamp::from_system_time@.
timestampFromSystemTime :: SystemTime -> Maybe Timestamp
timestampFromSystemTime (MkSystemTime secs nanos)
  | secs < 0 = Nothing
  | ms > toInteger (maxBound :: Int64) = Nothing
  | otherwise = Just (Timestamp (fromInteger ms))
  where
    ms = toInteger secs * 1000 + toInteger nanos `div` 1000000

-- | Reads a duration stored as integer milliseconds. 'Nothing' for a
-- negative value, mirroring @duration_from_ms@.
durationFromMs :: Int64 -> Maybe Duration
durationFromMs ms
  | ms < 0 = Nothing
  | otherwise = Just (Duration (fromIntegral ms / 1000))

-- | Reads a duration stored as fractional seconds. 'Nothing' for anything
-- that is not one: negative, NaN, infinite, or larger than 'Duration' can
-- hold (the columns are wide enough to carry @1e300@). Mirrors
-- @duration_from_secs@, which uses @try_from_secs_f64@ for the same four.
durationFromSecs :: Double -> Maybe Duration
durationFromSecs secs
  | isNaN secs || isInfinite secs || secs < 0 = Nothing
  | toRational secs >= toRational (maxBound :: Word64) + 1 = Nothing
  | otherwise = Just (Duration (realToFrac secs))

-- | The total number of whole milliseconds contained by this 'Duration',
-- truncating any sub-millisecond part. 'Integer' rather than 'Int64' because
-- a large second-based duration can exceed what the millisecond columns hold;
-- callers bound-check via 'addTimeout'. Mirrors @Duration::as_millis@, whose
-- @u128@ is likewise wider than the storage.
durationAsMillis :: Duration -> Integer
durationAsMillis (Duration d) = floor (toRational (nominalDiffTimeToSeconds d) * 1000)

-- | This instant plus a duration, or 'Nothing' on overflow. Mirrors
-- @Timestamp::checked_add@: adding a timeout to a deadline is checked, and a
-- deadline plus a deadline will not typecheck.
addTimeout :: Timestamp -> Duration -> Maybe Timestamp
addTimeout (Timestamp start) duration =
  let total = toInteger start + durationAsMillis duration
   in if total > toInteger (maxBound :: Int64)
        then Nothing
        else Just (Timestamp (fromInteger total))

-- | How long after @earlier@ this instant is, or 'Nothing' if it is not
-- after it. Equal instants give @Just@ zero. Mirrors
-- @Timestamp::duration_since@.
durationSince :: Timestamp -> Timestamp -> Maybe Duration
durationSince (Timestamp later) (Timestamp earlier)
  | later < earlier = Nothing
  | otherwise = Just (Duration (fromInteger (toInteger later - toInteger earlier) / 1000))

-- | Formats as ISO-8601 in UTC: @2026-08-12T14:30:00.123Z@, or
-- @2026-08-12T00:00:00Z@ on a whole second. Via 'iso8601Show' over the
-- millisecond the columns hold: the fraction appears only when there is one.
-- For @workflow_schedules.last_fired_at@, the one column holding a formatted
-- instant rather than epoch milliseconds.
timestampToIso8601 :: Timestamp -> Text
timestampToIso8601 (Timestamp ms) =
  Text.pack (iso8601Show (posixSecondsToUTCTime (fromInteger (toInteger ms) / 1000)))

-- | Reads an ISO-8601 instant, or 'Nothing' if the text is not one. Via
-- 'iso8601ParseM', which already reads what the four implementations write
-- (Python's @+00:00@ offset, TypeScript's @.000Z@, Go's @RFC3339Nano@ and
-- Java's bare @Z@) and truncates sub-millisecond precision on the way to
-- whole milliseconds. One guard the library needs: it rolls hour 24 and
-- second 60 forward instead of reporting them, so the canonical clock
-- positions are range-checked up front and the library judges the rest.
-- A leap second (@:60@) stays rejected — POSIX milliseconds hold no distinct
-- leap instant.
timestampFromIso8601 :: Text -> Maybe Timestamp
timestampFromIso8601 text = do
  guard (validClockFields text)
  utc <- case iso8601ParseM raw of
    -- 'UTCTime' reads the @Z@ spelling but not numeric offsets;
    -- 'ZonedTime' reads the offsets but not @Z@. Together they read what
    -- the four implementations write.
    Just u  -> Just u
    Nothing -> zonedTimeToUTC <$> iso8601ParseM raw
  let ms = floor (toRational (utcTimeToPOSIXSeconds utc) * 1000) :: Integer
  guard (ms >= toInteger (minBound :: Int64) && ms <= toInteger (maxBound :: Int64))
  pure (Timestamp (fromInteger ms))
  where
    raw = Text.unpack text
    validClockFields t = case Text.uncons (snd (Text.break (== 'T') t)) of
      Just ('T', clock) ->
        case (parseFixed 2 (Text.take 2 clock), parseFixed 2 (Text.take 2 (Text.drop 3 clock)), parseFixed 2 (Text.take 2 (Text.drop 6 clock))) of
          (Just hh, Just mm, Just ss) -> hh <= 23 && mm <= 59 && ss <= 59
          _                           -> True
      _ -> True

-- | An all-digit field of exactly the given width.
parseFixed :: Int -> Text -> Maybe Integer
parseFixed width t = do
  guard (Text.length t == width && not (Text.null t) && Text.all isDigit t)
  pure (parseDigits t)

parseDigits :: Text -> Integer
parseDigits = Text.foldl' (\n c -> n * 10 + toInteger (fromEnum c - fromEnum '0')) 0

-- | A whole number of seconds. Total, like @Duration::from_secs@.
secondsDuration :: Word64 -> Duration
secondsDuration secs = Duration (fromIntegral secs)

-- | A whole number of milliseconds. Total, like @Duration::from_millis@.
millisDuration :: Word64 -> Duration
millisDuration ms = Duration (fromIntegral ms / 1000)

-- | Whether this duration spans no time. Mirrors @Duration::is_zero@.
durationIsZero :: Duration -> Bool
durationIsZero duration = durationAsMillis duration == 0

-- * JSON codec (the replay envelope)
--
-- Management calls checkpointed as steps record their results as JSON, so
-- the rows they hand back need a round trip. The envelope is opaque: only
-- this port's replays decode it, so Haskell record names are the schema,
-- instants reuse their ISO-8601 spelling, and wire spellings are reused
-- where they already exist (statuses as @"PENDING"@, spans as epoch
-- milliseconds).

instance ToJSON WorkflowId where
  toJSON (WorkflowId text) = toJSON text

instance FromJSON WorkflowId where
  parseJSON = withText "WorkflowId" (pure . WorkflowId)

instance ToJSON WorkflowStatus where
  toJSON status = toJSON (workflowStatusText status)

instance FromJSON WorkflowStatus where
  parseJSON = withText "WorkflowStatus" $ \text ->
    case parseWorkflowStatus text of
      Right status -> pure status
      Left err -> fail (show err)

-- | Instants as ISO-8601 text, the same spelling the schema reads. Lives
-- here (not orphaned in @Postgres@) so statements and the replay envelope
-- share one shape.
instance ToJSON Timestamp where
  toJSON = toJSON . timestampToIso8601

instance FromJSON Timestamp where
  parseJSON = withText "Timestamp" $ \text ->
    case timestampFromIso8601 text of
      Just timestamp -> pure timestamp
      Nothing -> fail ("not an ISO-8601 instant: " <> Text.unpack text)

instance ToJSON Duration where
  toJSON duration = toJSON (durationAsMillis duration)

instance FromJSON Duration where
  parseJSON = withScientific "Duration" (\ms -> pure (Duration (fromIntegral (floor ms :: Integer) / 1000)))

instance ToJSON WorkflowRecord where
  toJSON record =
    object
      [ "workflowRecordId" .= record.workflowRecordId,
        "workflowRecordStatus" .= record.workflowRecordStatus,
        "workflowRecordName" .= record.workflowRecordName,
        "workflowRecordClassName" .= record.workflowRecordClassName,
        "workflowRecordConfigName" .= record.workflowRecordConfigName,
        "workflowRecordInput" .= record.workflowRecordInput,
        "workflowRecordOutput" .= record.workflowRecordOutput,
        "workflowRecordError" .= record.workflowRecordError,
        "workflowRecordSerialization" .= record.workflowRecordSerialization,
        "workflowRecordExecutorId" .= record.workflowRecordExecutorId,
        "workflowRecordApplicationVersion" .= record.workflowRecordApplicationVersion,
        "workflowRecordRecoveryAttempts" .= record.workflowRecordRecoveryAttempts,
        "workflowRecordQueueName" .= record.workflowRecordQueueName,
        "workflowRecordCreatedAt" .= record.workflowRecordCreatedAt,
        "workflowRecordUpdatedAt" .= record.workflowRecordUpdatedAt,
        "workflowRecordStartedAt" .= record.workflowRecordStartedAt,
        "workflowRecordCompletedAt" .= record.workflowRecordCompletedAt,
        "workflowRecordForkedFrom" .= record.workflowRecordForkedFrom,
        "workflowRecordParentWorkflowId" .= record.workflowRecordParentWorkflowId,
        "workflowRecordWasForkedFrom" .= record.workflowRecordWasForkedFrom,
        "workflowRecordOwnerXid" .= record.workflowRecordOwnerXid,
        "workflowRecordApplicationId" .= record.workflowRecordApplicationId,
        "workflowRecordAuthenticatedUser" .= record.workflowRecordAuthenticatedUser,
        "workflowRecordAuthenticatedRoles" .= record.workflowRecordAuthenticatedRoles,
        "workflowRecordAssumedRole" .= record.workflowRecordAssumedRole,
        "workflowRecordRequest" .= record.workflowRecordRequest,
        "workflowRecordApplicationName" .= record.workflowRecordApplicationName,
        "workflowRecordDeduplicationId" .= record.workflowRecordDeduplicationId,
        "workflowRecordPriority" .= record.workflowRecordPriority,
        "workflowRecordQueuePartitionKey" .= record.workflowRecordQueuePartitionKey,
        "workflowRecordRateLimited" .= record.workflowRecordRateLimited,
        "workflowRecordScheduleName" .= record.workflowRecordScheduleName,
        "workflowRecordTimeout" .= record.workflowRecordTimeout,
        "workflowRecordDeadline" .= record.workflowRecordDeadline,
        "workflowRecordDelayUntil" .= record.workflowRecordDelayUntil,
        "workflowRecordDebounceDeadline" .= record.workflowRecordDebounceDeadline,
        "workflowRecordIsDebounced" .= record.workflowRecordIsDebounced,
        "workflowRecordAttributes" .= record.workflowRecordAttributes
      ]

instance FromJSON WorkflowRecord where
  parseJSON = withObject "WorkflowRecord" $ \o ->
    WorkflowRecord
      <$> o .: "workflowRecordId"
      <*> o .: "workflowRecordStatus"
      <*> o .: "workflowRecordName"
      <*> o .: "workflowRecordClassName"
      <*> o .: "workflowRecordConfigName"
      <*> o .: "workflowRecordInput"
      <*> o .: "workflowRecordOutput"
      <*> o .: "workflowRecordError"
      <*> o .: "workflowRecordSerialization"
      <*> o .: "workflowRecordExecutorId"
      <*> o .: "workflowRecordApplicationVersion"
      <*> o .: "workflowRecordRecoveryAttempts"
      <*> o .: "workflowRecordQueueName"
      <*> o .: "workflowRecordCreatedAt"
      <*> o .: "workflowRecordUpdatedAt"
      <*> o .: "workflowRecordStartedAt"
      <*> o .: "workflowRecordCompletedAt"
      <*> o .: "workflowRecordForkedFrom"
      <*> o .: "workflowRecordParentWorkflowId"
      <*> o .: "workflowRecordWasForkedFrom"
      <*> o .: "workflowRecordOwnerXid"
      <*> o .: "workflowRecordApplicationId"
      <*> o .: "workflowRecordAuthenticatedUser"
      <*> o .: "workflowRecordAuthenticatedRoles"
      <*> o .: "workflowRecordAssumedRole"
      <*> o .: "workflowRecordRequest"
      <*> o .: "workflowRecordApplicationName"
      <*> o .: "workflowRecordDeduplicationId"
      <*> o .: "workflowRecordPriority"
      <*> o .: "workflowRecordQueuePartitionKey"
      <*> o .: "workflowRecordRateLimited"
      <*> o .: "workflowRecordScheduleName"
      <*> o .: "workflowRecordTimeout"
      <*> o .: "workflowRecordDeadline"
      <*> o .: "workflowRecordDelayUntil"
      <*> o .: "workflowRecordDebounceDeadline"
      <*> o .: "workflowRecordIsDebounced"
      <*> o .: "workflowRecordAttributes"
