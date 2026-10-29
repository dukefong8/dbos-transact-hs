{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | The port's own simulator backend (P7.7 seed, no Rust counterpart).
-- 'MockSystemDB' supplies deterministic canned answers for seam coverage;
-- 'MemSystemDB' stores lifecycle, messaging, event, and stream state for
-- shared live/sim workflow scenarios.
module DBOS.SystemDB.IOSim
  ( MockSystemDB (..),
    MemSystemDB (..),
    newMemDB,
    newMemDBSkewed,
    newMemDBWithApplication,
    memSetApplication,
    memConnectionOn,
    memLaunchOn,
    memLaunchOnWith,
    memDBOSOn,
    simConnectionWith,
    simInstance,
    simLaunchWith,
    simDBOSWith,
    simIdentity,
    simGeneratedId,
    simEntropy,
    verifyCallerStep,
  )
where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim)
import Data.Int (Int64)
import Data.Aeson (Value (..), eitherDecodeStrict, encode)
import Data.Aeson.KeyMap qualified as KeyMap
import Data.ByteString.Lazy qualified as LBS
import Data.Text.Encoding (decodeUtf8)
import Data.List (foldl', nub, partition, sort, sortOn)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Set qualified as Set
import Data.Text qualified as Text
import DBOS.SystemDB.Error (Error (..), decodePortableError, encodePortableError)
import DBOS.SystemDB
  ( Applications (..),
    AwaitedOutcome (..),
    DatabaseTime (..),
    databaseTimeRead,
    Debounce (..),
    DebounceHolder (..),
    DebounceRequest (..),
    EncodedValue (..),
    Error (..),
    EventRecord (..),
    Fork (..),
    ForkOptions (..),
    ForkPoint (..),
    GetEventCaller (..),
    InitWorkflowCaller (..),
    NewQueue (..),
    NewSchedule (..),
    NewWorkflow (..),
    NotificationRecord (..),
    OnExistingQueue (..),
    Outcome (..),
    OutcomeWrite (..),
    portableJsonSerialization,
    QueueName (..),
    QueueRecord (..),
    QueueUpdate (..),
    ScheduleFilter (..),
    ScheduleRecord (..),
    ScheduleUpdate (..),
    SendMessage (..),
    SerializedWorkflowValue (..),
    StepRecord (..),
    StepTiming (..),
    AwaitedStream (..),
    StreamRead (..),
    StreamRecord (..),
    SysRewindOptions (..),
    SystemDB (..),
    Timestamp,
    Topic (..),
    VersionInfo (..),
    WorkflowDelay (..),
    WorkflowFilter (..),
    WorkflowId (..),
    WorkflowInitResult (..),
    WorkflowKey (..),
    WorkflowOwner (..),
    WorkflowRecord (..),
    WorkflowStatus (..),
    WrittenBy (..),
    addTimeout,
    changeSet,
    debounceStepName,
    debounceValidate,
    dequeueSweepCap,
    durationAsMillis,
    durationSince,
    forkOptionsValidate,
    forkValidate,
    getEventStepName,
    invalidInput,
    getResultStepName,
    IdempotencyKey (..),
    initialStatus,
    internalQueueName,
    isActiveStatus,
    isQueueUpdateEmpty,
    newWorkflow,
    nullTopicSentinel,
    recvStepName,
    rewindStepName,
    durationToMs,
    saturateTimeout,
    shouldRecord,
    secondsDuration,
    sendBulkStepName,
    sleepStepName,
    sysRewindValidate,
    sendStepName,
    validateSendKeys,
    workflowKeyFromRow,
    workflowStatusText,
    writeStreamStepName,
    closeStreamStepName,
    streamClosedSentinel,
    portableJsonSerialization,
    isStreamClosed,
    setEventStepName,
    timestampFromEpochMs,
    timestampNow,
    timestampToEpochMs,
    zeroRowCounts)
import DBOS.SystemDB.Postgres.Statements (CallerStep (..), refuseSelfTarget)
import DBOS.Transact
  ( DBOS,
    Executor,
    Serializer (..),
    configNew,
    newDBOS,
  )
import DBOS.Transact.Logger (SomeTracer (..))
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Instance (launchExecutor, launchOn, launchOnWithQueues)
import DBOS.Transact.Connection
  ( Connection,
    Owner (..),
    SomeSystemDB (..),
    newConnection
  )
import DBOS.Transact.ContextSimData
  ( mockEvent,
    mockEventBody,
    mockMessageBody,
    mockNotification,
    mockStreamBody,
    mockStreamRecord,
  )
import DBOS.Transact.ManagementSimData
  ( mockDebounced,
    mockOutput,
    mockPartition,
    mockPartitionedId,
    mockQueue,
    mockQueueName,
    mockQueuedId,
    mockSchedule,
    mockScheduleName,
    mockSerialization,
    mockVersion,
  )
import DBOS.Transact.WorkflowSimData
  ( mockChildId,
    mockHolderId,
    mockInitResult,
    mockStep,
    mockTimestamp,
    mockWorkflow,
  )

-- | A marker backend: it owns no state, and every method answers with the
-- canned data the mirror tests expect.
data MockSystemDB = MockSystemDB
  deriving stock (Eq, Show)

instance SystemDB MockSystemDB (IOSim s) where
  databaseTime _ = do
    clock <- databaseTimeRead (timestampToEpochMs mockTimestamp)
    pure (Right clock)
  initWorkflow _ new _ = do
    clock <- databaseTimeRead (timestampToEpochMs mockTimestamp)
    pure (Right ((mockInitResult new) {initResultClock = clock}))
  getWorkflow = \cases
    _ (WorkflowId "missing") -> pure (Right Nothing)
    _ (WorkflowId wid) -> pure (Right (Just (mockWorkflow (WorkflowId wid))))
  listWorkflows = \cases _ filter _ -> pure (Right (map (mockWorkflow . WorkflowId) filter.workflowFilterWorkflowIds))
  getWorkflowChildren = \cases _ _ -> pure (Right [mockChildId])
  recordWorkflowOutcome = \cases _ _ _ _ -> pure (Right Recorded)
  awaitWorkflowResult = \cases _ _ _ _ -> pure (Right (AwaitedSucceeded (Just mockOutput) (Just mockSerialization)))
  -- Placeholder, not a mirror: 'MockSystemDB' answers every call with a
  -- canned value (see the module doc), so an empty first-wait gets the
  -- mock winner instead of the live refusal. The 'MemSystemDB' mirror
  -- refuses like Postgres; shared engine cases pin that refusal.
  awaitFirstWorkflowId = \cases
    _ [] _ -> pure (Right (WorkflowId "mock"))
    _ (first : _) _ -> pure (Right first)
  awaitWorkflowIds = \cases _ _ _ -> pure (Right ())
  setWorkflowDelay = \cases _ _ _ _ -> pure (Right ())
  clearQueueAssignment = \cases _ _ -> pure (Right True)
  updateWorkflowAttributes = \cases _ _ _ _ -> pure (Right ())
  reenqueueForRecovery = \cases _ _ _ _ -> pure (Right [])
  transitionDelayedWorkflows = \cases _ -> pure (Right 0)
  cancelIfTimedOut = \cases _ _ _ -> pure (Right False)
  cancelTimedOutWorkflows = \cases _ _ -> pure (Right [])
  deadLetterWorkflows = \cases _ ids _ -> pure (Right ids)
  cancelWorkflows = \cases _ ids _ _ _ -> pure (Right (filter (/= WorkflowId "never-existed") ids))
  resumeWorkflows = \cases
    _ ids _ _ _ | WorkflowId "never-existed" `elem` ids -> pure (Left (NonExistentWorkflow {workflowIds = map (\(WorkflowId wid) -> wid) ids}))
    _ ids _ _ _ -> pure (Right ids)
  deleteWorkflows = \cases _ ids _ _ -> pure (Right (fromIntegral (length ids)))
  forkWorkflows = \cases _ forks _ _ -> pure (Right (map (\fork -> WorkflowId fork.forkSourceId) forks))
  forkFrom = \cases _ ids _ _ _ -> pure (Right ids)
  rewindWorkflow = \cases _ _ _ _ _ -> pure (Right ())
  sendMessage = \cases _ _ _ _ _ _ -> pure (Right ())
  sendMessages = \cases _ _ _ _ _ _ -> pure (Right ())
  recv = \cases _ _ _ _ _ _ _ -> pure (Right (Just (EncodedValue mockMessageBody (Just mockSerialization))))
  writeStream = \cases _ _ _ _ _ _ _ _ -> pure (Right ())
  closeStream = \cases _ _ _ _ _ _ -> pure (Right ())
  close = \cases _ -> pure ()
  checkStep = \cases _ _ _ _ -> pure (Right Nothing)
  recordStep = \cases _ _ _ _ _ _ _ _ -> pure (Right ())
  listSteps = \cases _ wid _ _ _ _ -> pure (Right [mockStep wid 0 "mock-step"])
  recordSleep = \cases _ _ _ _ _ -> pure (Right mockTimestamp)
  setEvent = \cases _ _ _ _ _ _ _ -> pure (Right ())
  getEvent = \cases _ _ _ _ _ -> pure (Right (Just (EncodedValue mockEventBody (Just mockSerialization))))
  getAllNotifications = \cases _ _ -> pure (Right [mockNotification])
  getAllEvents = \cases _ _ -> pure (Right [mockEvent])
  readStreamValues = \cases _ _ _ _ _ -> pure (Right (StreamRead Pending [EncodedValue mockStreamBody (Just mockSerialization)]))
  awaitStreamValues = \cases _ _ _ _ _ _ _ -> pure (Right (Values [EncodedValue mockStreamBody (Just mockSerialization)]))
  getAllStreamEntries = \cases _ _ -> pure (Right [mockStreamRecord])
  createApplicationVersion = \cases _ _ _ -> pure (Right ())
  listApplicationVersions = \cases _ -> pure (Right [mockVersion])
  getLatestApplicationVersion = \cases _ _ -> pure (Right (Just mockVersion))
  updateApplicationVersionTimestamp = \cases _ _ _ _ -> pure (Right ())
  upsertQueue = \cases _ _ _ -> pure (Right True)
  startQueuedWorkflows = \cases _ _ _ _ _ _ _ _ -> pure (Right [mockQueuedId])
  getQueuePartitions = \cases _ _ -> pure (Right [mockPartition])
  startQueuedPartitionedWorkflows = \cases _ _ _ _ _ _ -> pure (Right [mockPartitionedId])
  getQueue = \cases _ name -> pure (Right (Just (mockQueue name)))
  listQueues = \cases _ _ -> pure (Right [mockQueue mockQueueName])
  updateQueue = \cases _ name _ _ -> pure (Right (mockQueue name))
  debounceDelayedWorkflow = \cases _ _ _ -> pure (Right (Debounced mockDebounced))
  getDeduplicationKeyHolder = \cases _ _ _ -> pure (Right (Just mockHolderId))
  deleteQueue = \cases _ _ -> pure (Right ())
  createSchedule = \cases _ _ _ -> pure (Right ())
  upsertSchedule = \cases _ _ _ -> pure (Right ())
  applySchedules = \cases _ _ -> pure (Right ())
  getSchedule = \cases _ name _ -> pure (Right (Just (mockSchedule name)))
  listSchedules = \cases _ _ _ -> pure (Right [mockSchedule mockScheduleName])
  updateSchedule = \cases _ _ _ _ -> pure (Right ())
  setScheduleStatus = \cases _ _ _ _ -> pure (Right ())
  updateScheduleLastFiredAt = \cases _ _ _ -> pure (Right ())
  deleteSchedule = \cases _ _ _ -> pure (Right ())
  renameApplication = \cases _ _ _ _ -> pure (Right zeroRowCounts)
  recordChildWorkflow = \cases _ _ _ _ _ _ _ -> pure (Right ())
  recordChildResult = \cases _ _ _ _ _ _ _ _ -> pure (Right ())

-- * The sim backend wrapped in a connection

-- | The sim backend wrapped in a connection carrying the given tracer: a
-- test's say-carrier makes simulated engine calls print while staying
-- typed-assertable. Constructing this is what needs
-- @instance SystemDB MockSystemDB (IOSim s)@.
simConnectionWith :: SomeTracer (IOSim s) -> IOSim s (Connection (IOSim s))
simConnectionWith tracer = do
  ids <- newTVarIO 0
  entropy <- newTVarIO 0
  instances <- newTVarIO 0
  -- The counter is per call, so two Mock connections made side by side
  -- share an instance id; trees that stage the cross-instance refusal use
  -- 'memConnectionOn', whose counter lives in the shared data.
  instanceId <- simInstanceId instances
  newConnection
    (SomeSystemDB MockSystemDB)
    PortableJson
    (Just "sim-app")
    (secondsDuration 1)
    OwnerApplication
    instanceId
    (simGeneratedId ids)
    (simEntropy entropy)
    tracer

-- | Deterministic instance ids for sim: @sim-instance-1@, ...
simInstanceId :: StrictTVar (IOSim s) Int -> IOSim s Text
simInstanceId counter = do
  n <- atomically $ do
    current <- readTVar counter
    writeTVar counter (current + 1)
    pure current
  pure (Text.pack ("sim-instance-" <> show n))

-- | Fresh owner tokens, mirroring live init stamping one UUID per attempt.
-- Shares the instance counter (formats keep them distinct); runs inside
-- the caller's 'atomically', like every other Mem write.
simOwnerText :: MemSystemDB s -> STM (IOSim s) Text
simOwnerText db = do
  current <- readTVar db.memInstanceCounter
  writeTVar db.memInstanceCounter (current + 1)
  pure (Text.pack ("sim-owner-" <> show current))

-- | Deterministic ids for sim: @sim-1@, @sim-2@, ...
simGeneratedId :: StrictTVar (IOSim s) Int -> IOSim s Text
simGeneratedId ids = do
  n <- atomically $ do
    current <- readTVar ids
    writeTVar ids (current + 1)
    pure current
  pure (Text.pack ("sim-" <> show n))

-- | Deterministic entropy for sim: a counter.
simEntropy :: StrictTVar (IOSim s) Int -> IOSim s Word32
simEntropy entropy = do
  n <- atomically $ do
    current <- readTVar entropy
    writeTVar entropy (current + 1)
    pure current
  pure (fromIntegral n)

-- | A @DBOS (IOSim s)@ with an open registry and no executor installed:
-- the sim equivalent of an unlaunched instance. Register workflows on it,
-- then launch. The config names the sim application; nothing connects.
simInstance :: IOSim s (DBOS (IOSim s))
simInstance = newDBOS (configNew "sim-app" "")

-- | The identity simulated launches stamp: the same values the previous
-- revision hardcoded at each launch site, in one place.
simIdentity :: Identity
simIdentity =
  Identity
    { identityAppName = "sim-app",
      identityAppVersion = "0.0.0",
      identityExecutorId = "sim-executor",
      identityAppId = ""
    }

-- | Freezes the registry and installs the sim executor over the snapshot:
-- the sim equivalent of a launch, with no database behind it.
simLaunchWith :: SomeTracer (IOSim s) -> DBOS (IOSim s) -> IOSim s (Executor (IOSim s))
simLaunchWith tracer dbos = do
  conn <- simConnectionWith tracer
  launchOn dbos conn simIdentity

-- | A launched sim instance carrying the given tracer.
simDBOSWith :: SomeTracer (IOSim s) -> IOSim s (DBOS (IOSim s))
simDBOSWith tracer = do
  dbos <- simInstance
  simLaunchWith tracer dbos
  pure dbos

-- * Stateful sim backend: an in-memory workflow lifecycle

-- | An in-memory workflow lifecycle for sim trees that assert the same
-- rows live trees read: init ownership (a second init joins instead of
-- executing), outcome recording, step checkpoints, dedup holds, and
-- cancel/delete/fork state. Fresh per 'newMemDB', so cases stay isolated.
-- The stateless 'MockSystemDB' keeps its canned answers untouched for
-- existing suites; methods outside the lifecycle delegate to it.
data MemSystemDB s = MemSystemDB
  { memRows :: StrictTVar (IOSim s) (Map Text WorkflowRecord),
    memSteps :: StrictTVar (IOSim s) (Map (Text, Int) StepRecord),
    memDedup :: StrictTVar (IOSim s) (Map (Text, Text) Text),
    -- | Registered queues by name.
    memQueues :: StrictTVar (IOSim s) (Map Text QueueRecord),
    -- | Parked messages by (destination, topic), each tagged with its
    -- stored id; 'recv' blocks on this TVar, so a 'sendMessage' genuinely
    -- wakes a parked reader. A workflow never reads another's queue, as
    -- live addresses rows per recipient.
    memMessages :: StrictTVar (IOSim s) (Map (Text, Text) [(Text, EncodedValue)]),
    -- | Message ids already stored, never removed: the in-memory
    -- @ON CONFLICT (message_uuid) DO NOTHING@. Consumed rows keep
    -- occupying the key live, so a resend stays a no-op after a receive.
    memMessageIds :: StrictTVar (IOSim s) (Set.Set Text),
    -- | Delivered notifications by destination, for
    -- 'getAllNotifications'.
    memNotifications :: StrictTVar (IOSim s) (Map Text [NotificationRecord]),
    -- | The step that consumed each message, by (destination, message id):
    -- the in-memory @consumed_by_function_id@. Written when a 'recv' takes
    -- a message, read when a rewind decides what goes.
    memConsumedBy :: StrictTVar (IOSim s) (Map (Text, Text) Int),
    -- | Published events by (workflow, key); readers block on this TVar.
    memEvents :: StrictTVar (IOSim s) (Map (Text, Text) EncodedValue),
    -- | Every published event by (workflow, key, step): the undo log a
    -- rewind restores from, as @workflow_events_history@ is live.
    memEventHistory :: StrictTVar (IOSim s) (Map (Text, Text, Int) EncodedValue),
    -- | Append-only workflow streams keyed by workflow and stream key.
    memStreams :: StrictTVar (IOSim s) (Map (Text, Text) [StreamRecord]),
    -- | Registered schedules by name.
    memSchedules :: StrictTVar (IOSim s) (Map Text ScheduleRecord),
    -- | Registered application versions, newest last.
    memVersions :: StrictTVar (IOSim s) [VersionInfo],
    -- | Names each connection launched over this data, so two instances
    -- sharing one database are still two — the distinction the
    -- cross-instance refusal reads.
    memInstanceCounter :: StrictTVar (IOSim s) Int,
    -- | The application this listener claims queues for. Mirrors the
    -- Postgres backend's @psdbApplicationName@: a claim sees the
    -- listener's own application's rows and unclaimed rows; 'Nothing'
    -- sees everything. Settable so one database can model several
    -- applications' executors.
    memApplicationName :: StrictTVar (IOSim s) (Maybe Text),
    -- | How far the database's clock runs ahead of this process's monotonic
    -- clock, in milliseconds (behind, when negative): the skew knob the
    -- deadline tests turn. The clock reading 'initWorkflow' returns carries
    -- the offset, so budgets are timed from the database's clock however
    -- far off it is. Fixed per database; plain tests leave it zero.
    memClockSkewMs :: Int64
  }

-- | Fresh simulated data: empty rows, steps, dedup holds, queues,
-- messages, events, streams, schedules, versions, and an instance counter.
newMemDB :: IOSim s (MemSystemDB s)
newMemDB = newMemDBWithApplication Nothing

-- | Fresh simulated data whose clock runs @skewMs@ ahead of this process's
-- monotonic clock (behind, when negative).
newMemDBSkewed :: Int64 -> IOSim s (MemSystemDB s)
newMemDBSkewed skewMs = do
  db <- newMemDB
  pure (db {memClockSkewMs = skewMs})

-- | This database's clock: the process clock plus the skew the deadline
-- tests turn. Every row stamp, delay, and deadline comparison in the sim
-- reads through here, the way every unqualified @now()@ in the SQL reads
-- the database's clock — so a skewed database stays self-consistent.
memDbNow :: MemSystemDB s -> IOSim s Timestamp
memDbNow db = do
  now <- timestampNow
  pure (timestampFromEpochMs (timestampToEpochMs now + db.memClockSkewMs))

-- | Fresh simulated data for a listener of the given application.
newMemDBWithApplication :: Maybe Text -> IOSim s (MemSystemDB s)
newMemDBWithApplication application =
  MemSystemDB
    <$> newTVarIO Map.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO Set.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO []
    <*> newTVarIO 0
    <*> newTVarIO application
    <*> pure 0

-- | Re-points the listener's application between sweeps, so one database
-- can model several applications' executors.
memSetApplication :: Maybe Text -> MemSystemDB s -> IOSim s ()
memSetApplication application db = atomically (writeTVar db.memApplicationName application)

-- | The Postgres application filter: the listener sees its own
-- application's rows and unclaimed rows; a listener with no application
-- sees everything.
visibleToApplication :: Maybe Text -> WorkflowRecord -> Bool
visibleToApplication application row = case application of
  Nothing -> True
  Just name -> row.workflowRecordApplicationName == Just name || isNothing row.workflowRecordApplicationName

-- | A connection over simulated data carrying the given tracer.
memConnectionOn :: MemSystemDB s -> SomeTracer (IOSim s) -> IOSim s (Connection (IOSim s))
memConnectionOn mem tracer = do
  ids <- newTVarIO 0
  entropy <- newTVarIO 0
  instanceId <- simInstanceId mem.memInstanceCounter
  newConnection
    (SomeSystemDB mem)
    PortableJson
    (Just "sim-app")
    (secondsDuration 1)
    OwnerApplication
    instanceId
    (simGeneratedId ids)
    (simEntropy entropy)
    tracer

-- | Launch carrying the given tracer over simulated data. Unlike
-- 'launchWithEnvironment', this always launches: there is no
-- existing-executor guard, so callers must launch once per executor
-- lifetime and relaunch only after 'shutdown'.
memLaunchOn :: MemSystemDB s -> SomeTracer (IOSim s) -> DBOS (IOSim s) -> IOSim s (Executor (IOSim s))
memLaunchOn mem tracer dbos = memLaunchOnWith mem tracer dbos Nothing

-- | 'memLaunchOn' with an explicit listen set for the supervisor: the sim
-- counterpart of a config carrying 'configListenQueues'. The engine's
-- queue discovery and worker staffing run unchanged; only the filter the
-- supervisor staffs from differs.
memLaunchOnWith :: MemSystemDB s -> SomeTracer (IOSim s) -> DBOS (IOSim s) -> Maybe [Text] -> IOSim s (Executor (IOSim s))
memLaunchOnWith mem tracer dbos listen = do
  conn <- memConnectionOn mem tracer
  executor <- launchOnWithQueues dbos conn simIdentity listen
  -- The same launch tail the IO path runs: application-version registration,
  -- recovery of this executor's pending rows, the launch announcement, and
  -- the supervisor fork — over the simulated backends.
  launchExecutor dbos executor >>= either (error . show) pure

-- | A launched sim instance over simulated data carrying the given tracer.
memDBOSOn :: MemSystemDB s -> SomeTracer (IOSim s) -> IOSim s (DBOS (IOSim s))
memDBOSOn mem tracer = do
  dbos <- simInstance
  memLaunchOn mem tracer dbos
  pure dbos

-- | A fresh row from the creation request: the database stamps the status
-- (from queue and delay, never the caller's choice) and the clock; the
-- parent link rides the init caller, which is how a child start records
-- its own start step in the same breath.
memFreshRow :: NewWorkflow -> Maybe InitWorkflowCaller -> Text -> WorkflowRecord
memFreshRow new caller creator =
  WorkflowRecord
    { workflowRecordId = WorkflowId new.newWorkflowId,
      workflowRecordStatus = initialStatus new,
      workflowRecordName = new.newWorkflowName,
      workflowRecordClassName = new.newWorkflowClassName,
      workflowRecordConfigName = new.newWorkflowConfigName,
      workflowRecordInput = new.newWorkflowInput,
      workflowRecordOutput = Nothing,
      workflowRecordError = Nothing,
      workflowRecordSerialization = new.newWorkflowSerialization,
      workflowRecordExecutorId = new.newWorkflowExecutorId,
      workflowRecordApplicationVersion = new.newWorkflowApplicationVersion,
      workflowRecordRecoveryAttempts = 0,
      workflowRecordQueueName = new.newWorkflowQueueName,
      workflowRecordCreatedAt = mockTimestamp,
      workflowRecordUpdatedAt = mockTimestamp,
      workflowRecordStartedAt = Nothing,
      workflowRecordCompletedAt = Nothing,
      workflowRecordForkedFrom = Nothing,
      workflowRecordParentWorkflowId = (.initCallerParentWorkflowId) <$> caller,
      workflowRecordWasForkedFrom = False,
      workflowRecordOwnerXid = if initialStatus new == Pending then Just creator else Nothing,
      workflowRecordCreatorXid = Just creator,
      workflowRecordApplicationId = new.newWorkflowApplicationId,
      workflowRecordAuthenticatedUser = new.newWorkflowAuthenticatedUser,
      workflowRecordAuthenticatedRoles = new.newWorkflowAuthenticatedRoles,
      workflowRecordAssumedRole = new.newWorkflowAssumedRole,
      workflowRecordRequest = Nothing,
      workflowRecordApplicationName = new.newWorkflowApplicationName,
      workflowRecordDeduplicationId = new.newWorkflowDeduplicationId,
      workflowRecordPriority = new.newWorkflowPriority,
      workflowRecordQueuePartitionKey = new.newWorkflowQueuePartitionKey,
      workflowRecordRateLimited = False,
      workflowRecordScheduleName = new.newWorkflowScheduleName,
      workflowRecordTimeout = new.newWorkflowTimeout,
      workflowRecordDeadline = new.newWorkflowDeadline,
      workflowRecordDelayUntil = Nothing,
      workflowRecordDebounceDeadline = Nothing,
      workflowRecordIsDebounced = new.newWorkflowIsDebounced,
      workflowRecordAttributes = new.newWorkflowAttributes
    }

-- | The recorded outcome a terminal row reports: anything else is still
-- running, so the waiter blocks (STM retry) rather than reading a stale
-- answer — the sim has real threads, so a concurrently running body
-- settles it deterministically.
memAwaited :: WorkflowRecord -> Maybe AwaitedOutcome
memAwaited row = case row.workflowRecordStatus of
  Success -> Just (AwaitedSucceeded row.workflowRecordOutput row.workflowRecordSerialization)
  Error -> case row.workflowRecordError of
    Just message -> Just (AwaitedFailed message row.workflowRecordSerialization)
    Nothing -> Nothing
  Cancelled -> Just AwaitedCancelled
  MaxRecoveryAttemptsExceeded -> Just (AwaitedParked row.workflowRecordRecoveryAttempts)
  _ -> Nothing

-- | A retried request is idempotent: an existing id joins, so the same id
-- initialized twice executes once. A held dedup key under a fresh id is
-- the collision; the same id retrying its own key joins instead.
memInitResult :: DatabaseTime -> Text -> WorkflowRecord -> WorkflowInitResult
memInitResult clock creator row =
  WorkflowInitResult
    { initResultStatus = row.workflowRecordStatus,
      initResultRecoveryAttempts = row.workflowRecordRecoveryAttempts,
      initResultDeadline = row.workflowRecordDeadline,
      initResultClock = clock,
      initResultCreatorXid = creator,
      initResultSerialization = row.workflowRecordSerialization,
      initResultShouldExecute = row.workflowRecordCreatorXid == Just creator
    }

-- | The timeout sweep's predicate, as the SQL spells it: unsettled, with a
-- deadline the database's clock has passed. The sim's clock is the one its
-- 'timestampNow' reads, so a test advances time the way the database would.
memTimedOut :: Timestamp -> WorkflowRecord -> Bool
memTimedOut now row =
  row.workflowRecordStatus `elem` [Pending, Enqueued, Delayed]
    && case row.workflowRecordDeadline of
      Just due -> due <= now
      Nothing  -> False

-- | What a timeout writes: a cancel's columns plus @owner_xid@, so no
-- execution holds the row, and both stamps read the database's clock.
memTimedOutRow :: Timestamp -> WorkflowRecord -> WorkflowRecord
memTimedOutRow now row =
  row
    { workflowRecordStatus = Cancelled,
      workflowRecordOwnerXid = Nothing,
      workflowRecordQueueName = Nothing,
      workflowRecordDeduplicationId = Nothing,
      workflowRecordStartedAt = Nothing,
      workflowRecordUpdatedAt = now,
      workflowRecordCompletedAt = Just now
    }

-- | The holds a write releases, as index-key/row pairs: the SQL
-- deduplication index keys on the row's @(queue, deduplication_id)@, so
-- any write that clears the id — outcome, release, cancel — or removes
-- the row must drop the entry, or a later enqueue under the key spins
-- forever against a stale hold. A hold drops only while the index still
-- names its row, so a reassigned key is never clobbered.
dropMemHolds :: Map Text WorkflowRecord -> [Text] -> Map (Text, Text) Text -> Map (Text, Text) Text
dropMemHolds oldRows moved held = foldr drop held (releasedHolds oldRows moved)
  where
    releasedHolds rows wids =
      [ ((queue, key), wid)
      | wid <- wids,
        Just row <- [Map.lookup wid rows],
        Just queue <- [row.workflowRecordQueueName],
        Just key <- [row.workflowRecordDeduplicationId]
      ]
    drop (indexKey, wid) index = case Map.lookup indexKey index of
      Just holder | holder == wid -> Map.delete indexKey index
      _ -> index

instance SystemDB (MemSystemDB s) (IOSim s) where
  databaseTime db = do
    -- The sim's clock, as every simulated write reads it: a test advances
    -- time the way the database would.
    now <- memDbNow db
    clock <- databaseTimeRead (timestampToEpochMs now)
    pure (Right clock)
  initWorkflow db new caller = do
    -- The wake instant a delayed enqueue waits for, as the SQL init's
    -- @delay_until@: without it a DELAYED row would never come due and the
    -- transition would pass it by forever.
    now <- memDbNow db
    -- One creator token per call, like the SQL init's per-attempt UUID:
    -- a retry presents the same token only if the caller kept it, so a
    -- match is this attempt's own insert.
    creator <- atomically (simOwnerText db)
    outcome <- atomically $ do
      rows <- readTVar db.memRows
      -- A stale parent starts nothing: the check opens the write, as the
      -- SQL take-tx does, so the child row and the parent record never
      -- commit behind a hand-off.
      let owned = case caller of
            Nothing -> Right ()
            Just call ->
              let WorkflowId parentText = call.initCallerParentWorkflowId
               in case Map.lookup parentText rows of
                    Just row | row.workflowRecordOwnerXid == Just call.initCallerToken -> Right ()
                    _ -> Left (WorkflowConflict {workflowId = parentText})
      case owned of
        Left conflict -> pure (Left conflict)
        Right () -> case Map.lookup new.newWorkflowId rows of
          Just row -> case initConflict row of
            Just conflict -> pure (Left conflict)
            Nothing
              | new.newWorkflowIsDebounced && row.workflowRecordCreatorXid /= Just creator ->
                  -- A debounced creation must be new: a reused pinned id
                  -- joins nothing. The holder's own key races back to the
                  -- bounce; anything else is a conflict. Mirrors the
                  -- oracle's debounced conflict arm; the row keeps its
                  -- deduplication id only while it waits, so a matching
                  -- one is a holder of the key.
                  case (new.newWorkflowDeduplicationId, new.newWorkflowQueueName) of
                    (Just deduplicationId, Just queueName)
                      | row.workflowRecordDeduplicationId == Just deduplicationId && row.workflowRecordQueueName == Just queueName ->
                          pure (Left QueueDeduplicated {workflowId = new.newWorkflowId, queueName = queueName, deduplicationId = deduplicationId})
                    _ ->
                      pure
                        ( Left
                            ConflictingWorkflow
                              { workflowId = new.newWorkflowId,
                                detail = "a debounce enqueues a new workflow, but a workflow with this id already exists"
                              }
                        )
              | otherwise -> pure (Right row)
          Nothing -> case (new.newWorkflowQueueName, new.newWorkflowDeduplicationId) of
            (Just queue, Just key) -> do
              held <- readTVar db.memDedup
              case Map.lookup (queue, key) held of
                Just _ ->
                  pure (Left (QueueDeduplicated {workflowId = new.newWorkflowId, queueName = queue, deduplicationId = key}))
                Nothing -> do
                  writeTVar db.memDedup (Map.insert (queue, key) new.newWorkflowId held)
                  inserted <- insertFresh rows now creator
                  pure (Right inserted)
            _ -> do
              inserted <- insertFresh rows now creator
              pure (Right inserted)
    clock <- databaseTimeRead (timestampToEpochMs now)
    -- Should-execute compares the row's creator to this call's token, as
    -- the SQL finish does: a fresh insert's stored creator always matches,
    -- so no override is needed.
    pure $ case outcome of
      Left err -> Left err
      Right row -> Right (memInitResult clock creator row)
    where
      -- The sim's row carries no class/config names, so the identity
      -- conflict it can model is the function name: a start that names a
      -- different function under an existing id is refused, as the SQL's
      -- commit-time guard does. The detail keeps the oracle's debug
      -- spelling so the live and sim answers read alike.
      initConflict row = case (row.workflowRecordName, new.newWorkflowName) of
        (stored, offered) | stored /= offered ->
          Just
            ConflictingWorkflow
              { workflowId = new.newWorkflowId,
                detail = "existing function name is " <> debugMaybe stored <> ", but " <> debugMaybe offered <> " was provided"
              }
        _ -> Nothing
      debugMaybe Nothing = "None"
      debugMaybe (Just value) = "Some(\"" <> value <> "\")"
      insertFresh rows now creator = do
        -- The SQL insert turns a PENDING row's budget into a deadline
        -- against the database's clock; a queued row keeps none until its
        -- claim. A recovered or resumed row keeps whatever it already had.
        let deadlineAt = case new.newWorkflowDeadline of
              Just _ -> new.newWorkflowDeadline
              Nothing
                | initialStatus new == Pending -> new.newWorkflowTimeout >>= addTimeout now
                | otherwise -> Nothing
            -- The release is the delay from now, capped at the debounce
            -- deadline, as the init statement resolves both: a delay
            -- longer than the timeout lands on the deadline exactly.
            cappedRelease = case (saturateTimeout now <$> new.newWorkflowDelay, saturateTimeout now <$> new.newWorkflowDebounceTimeout) of
              (Just release, Just deadline) -> Just (min release deadline)
              (release, _) -> release
            row =
              (memFreshRow new caller creator)
                { workflowRecordDelayUntil = cappedRelease,
                  workflowRecordDebounceDeadline = saturateTimeout now <$> new.newWorkflowDebounceTimeout,
                  workflowRecordDeadline = deadlineAt
                }
        writeTVar db.memRows (Map.insert new.newWorkflowId row rows)
        case caller of
          -- The init caller records the parent's start step in the same
          -- breath, so a replay finds the child without starting one.
          Just call -> do
            steps <- readTVar db.memSteps
            let WorkflowId parentText = call.initCallerParentWorkflowId
                entry =
                  StepRecord
                    { stepRecordWorkflowId = call.initCallerParentWorkflowId,
                      stepRecordStepId = call.initCallerStepId,
                      stepRecordStepName = call.initCallerStepName,
                      stepRecordOutput = Nothing,
                      stepRecordError = Nothing,
                      stepRecordChildWorkflowId = Just (WorkflowId new.newWorkflowId),
                      stepRecordSerialization = Nothing,
                      stepRecordStartedAt = Just call.initCallerStartedAt,
                      stepRecordCompletedAt = Nothing
                    }
            writeTVar db.memSteps (Map.insert (parentText, call.initCallerStepId) entry steps)
          Nothing -> pure ()
        pure row
  getWorkflow db (WorkflowId wid) = Right . Map.lookup wid <$> readTVarIO db.memRows
  listWorkflows db filters _ = do
    rows <- readTVarIO db.memRows
    pure
      ( Right
          ( case filters.workflowFilterWorkflowIds of
              [] -> [row | row <- Map.elems rows, queuedHere row, attributesHere row]
              ids -> [row | wid <- ids, Just row <- [Map.lookup wid rows], queuedHere row, attributesHere row]
          )
      )
    where
      -- The only list guards the sim halves assert: a queue-name filter
      -- admits a row only when the row names a listed queue, as the SQL
      -- @cardinality = 0 or queue_name = any@ guard does (a null queue
      -- never matches a non-empty filter), and an attributes filter admits
      -- a row only when the row's JSON contains the filter's, as @attributes
      -- @> filter@ does. Every other filter stays unmirrored.
      queuedHere row = case filters.workflowFilterQueueNames of
        [] -> True
        names -> row.workflowRecordQueueName `elem` (Just <$> names)
      attributesHere row = memAttributesContain row.workflowRecordAttributes filters.workflowFilterAttributes
  getWorkflowChildren db (WorkflowId wid) = do
    rows <- readTVarIO db.memRows
    pure (Right [row.workflowRecordId | row <- Map.elems rows, row.workflowRecordParentWorkflowId == Just (WorkflowId wid)])
  recordWorkflowOutcome db (WorkflowId wid) outcome token = atomically $ do
    rows <- readTVar db.memRows
    case Map.lookup wid rows of
      -- Only a PENDING row still owned by this call records: anything else
      -- is AlreadyFinished, and the caller adopts what the row holds, as
      -- the SQL outcome's status-and-owner gate decides.
      Just row | row.workflowRecordStatus == Pending && row.workflowRecordOwnerXid == Just token -> do
        -- Finishing releases the deduplication key, as the SQL outcome's
        -- @deduplication_id = null@ does: a key left on a finished row
        -- would be held forever, and the unique index spans every status.
        -- The hold drops only while it still names this workflow, so a
        -- reassigned key is never clobbered. Finishing also clears the
        -- owner, as the SQL outcome does: the row belongs to nobody.
        let row' = (memApplyOutcome outcome row) {workflowRecordDeduplicationId = Nothing, workflowRecordOwnerXid = Nothing}
        writeTVar db.memRows (Map.insert wid row' rows)
        modifyTVar db.memDedup (dropMemHolds rows [wid])
        pure (Right Recorded)
      _ -> pure (Right AlreadyFinished)
    where
      memApplyOutcome (OutcomeOutput output) row =
        row {workflowRecordStatus = Success, workflowRecordOutput = output}
      memApplyOutcome (OutcomeError message) row =
        row {workflowRecordStatus = Error, workflowRecordError = Just message}
  awaitWorkflowResult db (WorkflowId wid) _duration _includeSteps = atomically $ do
    rows <- readTVar db.memRows
    case Map.lookup wid rows >>= memAwaited of
      Just outcome -> pure (Right outcome)
      -- Still running: block for the row's next write, as the polling
      -- backend would. A row nothing runs deadlocks the sim loudly rather
      -- than answering stale data.
      Nothing -> retry
  setWorkflowDelay db (WorkflowId wid) delay _ = do
    now <- memDbNow db
    case delay of
      -- Mirrors the write: an unstorable relative delay is refused by
      -- 'durationToMs', a fitting one saturates at the end of time.
      DelayFor duration -> case durationToMs "delay" duration of
        Left err -> pure (Left err)
        Right _ -> do
          atomically (modifyTVar db.memRows (Map.adjust (\row -> row {workflowRecordDelayUntil = Just (saturateTimeout now duration)}) wid))
          pure (Right ())
      DelayUntil instant -> do
        atomically (modifyTVar db.memRows (Map.adjust (\row -> row {workflowRecordDelayUntil = Just instant}) wid))
        pure (Right ())
  updateWorkflowAttributes db (WorkflowId wid) attributes _ =
    atomically (modifyTVar db.memRows (Map.adjust (\row -> row {workflowRecordAttributes = attributes}) wid))
      >> pure (Right ())
  cancelIfTimedOut db (WorkflowId wid) owner = do
    now <- memDbNow db
    atomically $ do
      rows <- readTVar db.memRows
      case Map.lookup wid rows of
        Just row
          | memTimedOut now row && row.workflowRecordOwnerXid == Just owner -> do
              writeTVar db.memRows (Map.insert wid (memTimedOutRow now row) rows)
              pure (Right True)
        _ -> pure (Right False)
  cancelTimedOutWorkflows db limit = do
    now <- memDbNow db
    atomically $ do
      rows <- readTVar db.memRows
      application <- readTVar db.memApplicationName
      let inScope row = case application of
            Nothing   -> True
            Just name -> row.workflowRecordApplicationName == Just name || row.workflowRecordApplicationName == Nothing
          due = [wid | (wid, row) <- sortOn (\entry -> (snd entry).workflowRecordDeadline) (Map.toList rows), memTimedOut now row, inScope row]
          targets = take (fromIntegral limit) due
          rows' = foldl' (\acc wid -> Map.adjust (memTimedOutRow now) wid acc) rows targets
      writeTVar db.memRows rows'
      pure (Right (map WorkflowId targets))
  deadLetterWorkflows db workflowIds minRecoveryAttempts = do
    now <- memDbNow db
    atomically $ do
      rows <- readTVar db.memRows
      -- Mirrors the SQL guard: only a PENDING row at or above the floor
      -- moves, so a row that settled or reset meanwhile is left alone.
      let move widText row
            | row.workflowRecordStatus == Pending && row.workflowRecordRecoveryAttempts >= minRecoveryAttempts =
                row
                  { workflowRecordStatus = MaxRecoveryAttemptsExceeded,
                    workflowRecordOwnerXid = Nothing,
                    workflowRecordQueueName = Nothing,
                    workflowRecordDeduplicationId = Nothing,
                    workflowRecordStartedAt = Nothing,
                    workflowRecordUpdatedAt = now,
                    workflowRecordCompletedAt = Just now
                  }
            | otherwise = row
          requested = [widText | WorkflowId widText <- workflowIds]
          rows' = foldl' (\acc widText -> Map.adjust (move widText) widText acc) rows requested
          parked = [WorkflowId widText | widText <- requested, Just row <- [Map.lookup widText rows], row.workflowRecordStatus == Pending, row.workflowRecordRecoveryAttempts >= minRecoveryAttempts]
      writeTVar db.memRows rows'
      pure (Right parked)
  cancelWorkflows db ids withChildren _ fromWorkflow = atomically $ do
    rows <- readTVar db.memRows    -- Transitive descendants, like the backend's per-id traverse through
    -- 'descendantsSession': one level would leave a grandchild tree
    -- half-cancelled.
    let requested = [wid | WorkflowId wid <- ids]
        targets
          | withChildren = nub (requested <> concatMap (memChildTree rows) requested)
          | otherwise = requested
        (moved, rows') = Map.mapAccumWithKey move [] rows
        -- A workflow cannot cancel itself, nor the tree it is inside:
        -- refused before anything moves, as the SQL cancel refuses.
        refused = (fromWorkflow >>= (\callerId -> refuseSelfTarget callerId requested "cancel"))
          <|> case (fromWorkflow, withChildren) of
            (Just callerId, True)
              | callerId `elem` targets, callerId `notElem` requested ->
                  Just (InvalidInput {field = "workflow_ids", detail = "workflow " <> callerId <> " cannot cancel the tree of " <> memRootContaining rows requested callerId <> ", which contains it"})
              | otherwise -> Nothing
            _ -> Nothing
        move acc wid row
          -- Mirrors the SQL guard: anything but a finished row moves.
          | wid `elem` targets && row.workflowRecordStatus `notElem` [Success, Error, Cancelled] =
              ( WorkflowId wid : acc,
                row
                  { workflowRecordStatus = Cancelled,
                    workflowRecordOwnerXid = Nothing,
                    workflowRecordQueueName = Nothing,
                    workflowRecordDeduplicationId = Nothing,
                    workflowRecordStartedAt = Nothing,
                    workflowRecordCompletedAt = Nothing
                  }
              )
          | otherwise = (acc, row)
    case refused of
      Just err -> pure (Left err)
      Nothing -> do
        writeTVar db.memRows rows'
        modifyTVar db.memDedup (dropMemHolds rows [wid | WorkflowId wid <- moved])
        pure (Right moved)
  resumeWorkflows db ids queue _ fromWorkflow
    | null ids = pure (Right [])
    -- A workflow cannot resume itself: refused before anything runs,
    -- as the SQL resume refuses.
    | Just err <- fromWorkflow >>= (\callerId -> refuseSelfTarget callerId [wid | WorkflowId wid <- ids] "resume") =
        pure (Left err)
    | otherwise = do
        rows <- readTVarIO db.memRows
        let missing = [wid | WorkflowId wid <- ids, Map.notMember wid rows]
        rows <- readTVarIO db.memRows
        rows <- readTVarIO db.memRows
        let missing = [wid | WorkflowId wid <- ids, Map.notMember wid rows]
        case missing of
          _ : _ -> pure (Left (NonExistentWorkflow {workflowIds = missing}))
          [] -> atomically $ do
            let QueueName internal = internalQueueName
                (moved, rows') = Map.mapAccumWithKey move [] rows
                move acc wid row
                  | WorkflowId wid `elem` ids && row.workflowRecordStatus `notElem` [Success, Error] =
                      ( WorkflowId wid : acc,
                        row
                          { workflowRecordStatus = Enqueued,
                            workflowRecordQueueName = queue <|> Just internal,
                            workflowRecordRecoveryAttempts = 0,
                            workflowRecordDeadline = Nothing,
                            workflowRecordOwnerXid = Nothing,
                            workflowRecordDeduplicationId = Nothing,
                            workflowRecordStartedAt = Nothing,
                            workflowRecordCompletedAt = Nothing
                          }
                      )
                  | otherwise = (acc, row)
            writeTVar db.memRows rows'
            pure (Right moved)
  deleteWorkflows db ids _ owner = atomically $ do
    rows <- readTVar db.memRows
    -- A step running the delete opens with its ownership check: a stale
    -- execution removes nothing. Client and engine deletes skip it. The row
    -- checked is named by the same value the fence reads its token from.
    case owner of
      Just o ->
        case Map.lookup (widTextOf o.ownerWorkflowId) rows of
          Just row | row.workflowRecordOwnerXid == Just o.ownerToken -> remove rows
          _ -> pure (Left (WorkflowConflict {workflowId = widTextOf o.ownerWorkflowId}))
      Nothing -> remove rows
    where
      remove rows = do
        let gone = length (filter (`Map.member` rows) [wid | WorkflowId wid <- ids])
        writeTVar db.memRows (foldr Map.delete rows [wid | WorkflowId wid <- ids])
        modifyTVar db.memSteps (Map.filterWithKey (\(wid, _) _ -> wid `notElem` [w | WorkflowId w <- ids]))
        -- Deleted rows take their holds with them, as deleting drops the
        -- SQL index entries.
        modifyTVar db.memDedup (dropMemHolds rows [wid | WorkflowId wid <- ids])
        pure (Right (fromIntegral gone))
  forkWorkflows db forks options _
    | null forks = pure (Right [])
    | otherwise = case (forkOptionsValidate options, traverse forkValidate forks) of
    (Left err, _) -> pure (Left err)
    (_, Left err) -> pure (Left err)
    (Right (), Right _) -> memFork db [(fork.forkSourceId, fork.forkForkedId, fork.forkStartStep) | fork <- forks] options
  forkFrom db ids point options _
    | null ids = pure (Right [])
    | otherwise = case (forkOptionsValidate options, traverse (forkValidate . synthesized) sources) of
    (Left err, _) -> pure (Left err)
    (_, Left err) -> pure (Left err)
    (Right (), Right _) -> do
      steps <- readTVarIO db.memSteps
      memFork db [(source, Nothing, memStartStep point source steps) | WorkflowId source <- ids] options
    where
      sources = [source | WorkflowId source <- ids]
      givenStep = case point of
        ForkStep step -> step
        _ -> 0
      synthesized source = Fork source Nothing givenStep
      memStartStep (ForkStep step) _ _ = step
      -- Mirrors the SQL 'COALESCE(MAX ... FILTER (WHERE error IS NOT NULL),
      -- MAX ...)': the failed step, falling back to the last step.
      memStartStep ForkLastFailure source steps =
        case [sid | ((wid, sid), record) <- Map.toList steps, wid == source, record.stepRecordError /= Nothing] of
          [] -> memMaxStep source steps
          failed -> maximum failed
      memStartStep ForkLastStep source steps = memMaxStep source steps
      memStartStep (ForkStepNamed name) source steps = memNamedStep source name steps
      memMaxStep source steps = maximum (-1 : [sid | ((wid, sid), _) <- Map.toList steps, wid == source])
      memNamedStep source name steps = maximum (-1 : [sid | ((wid, sid), record) <- Map.toList steps, wid == source, record.stepRecordStepName == name])
  rewindWorkflow db (WorkflowId wid) cut sysOpts caller = case sysRewindValidate cut sysOpts of
    Left err -> pure (Left err)
    Right () -> memRewind db wid cut sysOpts caller
  checkStep db (WorkflowId wid) stepId name = do
    steps <- readTVarIO db.memSteps
    -- A row recorded under another name is the old code's step, refused
    -- like Postgres refuses it: the name check @operation_outputs@
    -- applies is what a patch asks about.
    pure $ case Map.lookup (wid, stepId) steps of
      Nothing -> Right Nothing
      Just record
        | record.stepRecordStepName == name -> Right (Just record)
        | otherwise -> Left (UnexpectedStep {workflowId = wid, stepId = stepId, expected = name, recorded = record.stepRecordStepName})
  recordStep db (WorkflowId wid) stepId name outcome serialization timing token = atomically $ do
    rows <- readTVar db.memRows
    case Map.lookup wid rows of
      -- The row's current owner is the only writer: anything else is a
      -- superseded execution reporting the conflict, as the SQL
      -- @check_owner_on@ refuses it.
      Just row | row.workflowRecordOwnerXid == Just token -> do
        steps <- readTVar db.memSteps
        let prior = Map.lookup (wid, stepId) steps
            entry =
              StepRecord
                { stepRecordWorkflowId = WorkflowId wid,
                  stepRecordStepId = stepId,
                  stepRecordStepName = name,
                  stepRecordOutput = case outcome of OutcomeOutput output -> output; _ -> memPriorOutput prior,
                  stepRecordError = case outcome of OutcomeError message -> Just message; _ -> Nothing,
                  stepRecordChildWorkflowId = prior >>= (.stepRecordChildWorkflowId),
                  stepRecordSerialization = serialization,
                  stepRecordStartedAt = timing >>= (Just . (.stepTimingStartedAt)),
                  stepRecordCompletedAt = timing >>= (Just . (.stepTimingCompletedAt))
                }
        -- A rival record behind a token this call holds is nondeterminism,
        -- told apart from this caller's own retry by the completion time.
        case prior of
          Just kept | kept.stepRecordCompletedAt /= entry.stepRecordCompletedAt ->
            pure (Left (StepNondeterminism {workflowId = wid, stepId = stepId}))
          _ -> do
            writeTVar db.memSteps (Map.insert (wid, stepId) entry steps)
            pure (Right ())
      _ -> pure (Left (WorkflowConflict {workflowId = wid}))
    where
      memPriorOutput (Just record) = record.stepRecordOutput
      memPriorOutput Nothing = Nothing
  listSteps db (WorkflowId wid) loadOutput limit offset _ = do
    steps <- readTVarIO db.memSteps
    let ordered = sortOn (.stepRecordStepId) [record | ((w, _), record) <- Map.toList steps, w == wid]
        start = maybe 0 fromIntegral offset
        count = maybe (length ordered) fromIntegral limit
        page = take count (drop start ordered)
        strip record
          | loadOutput = record
          | otherwise = record {stepRecordOutput = Nothing, stepRecordError = Nothing}
    pure (Right (map strip page))
  recordChildWorkflow db parent child stepId name _time token = atomically $ do
    rows <- readTVar db.memRows
    case Map.lookup parentText rows of
      -- Only the row's current owner records the start: anything else is a
      -- superseded execution reporting the conflict.
      Just row | row.workflowRecordOwnerXid == Just token -> do
        steps <- readTVar db.memSteps
        let entry = case Map.lookup (parentText, stepId) steps of
              Just record -> record {stepRecordChildWorkflowId = Just child}
              Nothing ->
                StepRecord
                  { stepRecordWorkflowId = parent,
                    stepRecordStepId = stepId,
                    stepRecordStepName = name,
                    stepRecordOutput = Nothing,
                    stepRecordError = Nothing,
                    stepRecordChildWorkflowId = Just child,
                    stepRecordSerialization = Nothing,
                    stepRecordStartedAt = Nothing,
                    stepRecordCompletedAt = Nothing
                  }
        -- The same child is an idempotent retry; a different one (or none,
        -- where the row already held a non-child step) is nondeterminism.
        case Map.lookup (parentText, stepId) steps of
          Just kept | kept.stepRecordChildWorkflowId /= Just child ->
            pure (Left (StepNondeterminism {workflowId = parentText, stepId = stepId}))
          _ -> do
            writeTVar db.memSteps (Map.insert (parentText, stepId) entry steps)
            pure (Right ())
      _ -> pure (Left (WorkflowConflict {workflowId = parentText}))
    where
      WorkflowId parentText = parent
  getDeduplicationKeyHolder db queue key = do
    held <- readTVarIO db.memDedup
    pure (Right (WorkflowId <$> Map.lookup (queue, key) held))
  -- Recovery: pending rows owned by the named executors at the named
  -- version are re-enqueued onto the recovery queue, as the SQL sweep
  -- does. Nothing runs them here — a launch installs no supervisor in
  -- sim — but the rows are the same rows live reads.
  reenqueueForRecovery db executorIds applicationVersion recoveryQueue = atomically $ do
    rows <- readTVar db.memRows
    let candidates =
          [ (widText, row)
            | (widText, row) <- Map.toList rows,
              row.workflowRecordStatus == Pending,
              maybe False (`elem` executorIds) row.workflowRecordExecutorId,
              row.workflowRecordApplicationVersion == Just applicationVersion
          ]
        requeue row =
          row
            { workflowRecordStatus = Enqueued,
              workflowRecordStartedAt = Nothing,
              workflowRecordQueueName = case row.workflowRecordQueueName of
                Just queue | not (Text.null queue) -> Just queue
                _ -> Just recoveryQueue
            }
    writeTVar db.memRows (foldr (\(widText, row) -> Map.insert widText (requeue row)) rows candidates)
    -- The index follows the row: the hold moves from the old queue to the
    -- recovery queue with it, as the SQL update re-keys the entry.
    let rekey (widText, row) index = case (row.workflowRecordQueueName, row.workflowRecordDeduplicationId) of
          (Just oldQueue, Just key) ->
            let newQueue
                  | Text.null oldQueue = recoveryQueue
                  | otherwise = oldQueue
             in Map.insert (newQueue, key) widText (dropMemHolds rows [widText] index)
          _ -> index
    modifyTVar db.memDedup (\held -> foldr rekey held candidates)
    pure (Right [WorkflowId widText | (widText, _) <- candidates])
  -- Delayed rows whose deadline has passed are enqueued; a debounced
  -- holder releases its key, as the SQL transition does.
  transitionDelayedWorkflows db = do
    now <- memDbNow db
    atomically $ do
      rows <- readTVar db.memRows
      let due row = row.workflowRecordStatus == Delayed && maybe False (<= now) row.workflowRecordDelayUntil
          move row =
            row
              { workflowRecordStatus = Enqueued,
                workflowRecordDeduplicationId = if row.workflowRecordIsDebounced then Nothing else row.workflowRecordDeduplicationId
              }
          (moved, rows') = Map.mapAccum (\n row -> if due row then (n + 1, move row) else (n, row)) (0 :: Int) rows
      writeTVar db.memRows rows'
      -- A released debounced key frees its hold with it, as clearing the
      -- column drops the SQL index entry.
      modifyTVar
        db.memDedup
        (dropMemHolds rows [wid | (wid, row) <- Map.toList rows, row.workflowRecordIsDebounced, due row])
      pure (Right (fromIntegral moved))
  clearQueueAssignment _ = clearQueueAssignment MockSystemDB
  sendMessage db message serialization caller owner sendToForks = memSend db sendStepName [message] serialization caller owner sendToForks
  sendMessages db messages serialization caller owner sendToForks = memSend db sendBulkStepName messages serialization caller owner sendToForks
  -- A parked reader blocks on the messages TVar, so a send genuinely
  -- wakes it; the engine's own duration bounds the wait. The deadline sleep
  -- step is recorded before the wait, unconditionally like the live
  -- checkpoint, so a replay that waited has the same remainder left; the
  -- recv step with no output records the absence a timeout produced, so a
  -- replay adopts it.
  recv db wid stepId timeoutStepId topic duration token = do
    let WorkflowId widText = wid
        storedTopic = fromMaybe nullTopicSentinel topic
    replayed <- atomically (Map.lookup (widText, stepId) <$> readTVar db.memSteps)
    case replayed of
      Just record -> pure (Right (memEncodedFromStep record))
      Nothing -> do
        slept <- recordSleep db wid timeoutStepId duration token
        case slept of
          Left err -> pure (Left err)
          Right _ -> do
            -- The take opens with the ownership check, as the SQL take-tx
            -- does: a stale execution's consume is refused before anything
            -- is taken, so the message stays parked for the owner.
            rights <- atomically $ do
              rows <- readTVar db.memRows
              pure $ case Map.lookup widText rows of
                Just row | row.workflowRecordOwnerXid == Just token -> Right ()
                _ -> Left (WorkflowConflict {workflowId = widText})
            case rights of
              Left conflict -> pure (Left conflict)
              Right () -> do
                waited <- memTimeout (fromInteger (durationAsMillis duration * 1000)) (atomically (memTakeMessage db widText storedTopic))
                case waited of
                  Just (uuid, value) -> do
                    memRecordRecvStep db wid stepId recvStepName (Just value)
                    atomically $ do
                      notifications <- readTVar db.memNotifications
                      writeTVar db.memNotifications (Map.adjust (map (markConsumed uuid)) widText notifications)
                      consumed <- readTVar db.memConsumedBy
                      writeTVar db.memConsumedBy (Map.insert (widText, uuid) stepId consumed)
                    pure (Right (Just value))
                  Nothing -> do
                    memRecordRecvStep db wid stepId recvStepName Nothing
                    pure (Right Nothing)
  writeStream db wid@(WorkflowId widText) stepId key value serialization writtenBy token = do
    let stepName = if isStreamClosed value serialization then closeStreamStepName else writeStreamStepName
    now <- memDbNow db
    -- The write opens with the ownership check, before the replay adopt, as
    -- the oracle orders it: a stale execution is refused rather than handed
    -- the recorded answer. The check shares the append's transaction, so no
    -- hand-off can land between them. A missing row is refused the same way,
    -- as the SQL check refuses what no select finds.
    atomically $ do
      rows <- readTVar db.memRows
      case Map.lookup widText rows of
        Just row | row.workflowRecordOwnerXid == Just token -> do
          -- A replay must not append a second entry. Only a workflow-level
          -- write records a step to find; a step-level write is replayed by
          -- its step being replayed.
          steps <- readTVar db.memSteps
          case writtenBy of
            Workflow | Map.member (widText, stepId) steps -> pure (Right ())
            _ -> do
              streams <- readTVar db.memStreams
              let streamKey' = (widText, key)
                  entries = fromMaybe [] (Map.lookup streamKey' streams)
                  entry = StreamRecord {streamKey = key, streamOffset = length entries, streamValue = value, streamSerialization = serialization, streamStepId = stepId}
              writeTVar db.memStreams (Map.insert streamKey' (entries <> [entry]) streams)
              when (writtenBy == Workflow) $ do
                let record = StepRecord {stepRecordWorkflowId = wid, stepRecordStepId = stepId, stepRecordStepName = stepName, stepRecordOutput = Nothing, stepRecordError = Nothing, stepRecordChildWorkflowId = Nothing, stepRecordSerialization = Nothing, stepRecordStartedAt = Just now, stepRecordCompletedAt = Just now}
                writeTVar db.memSteps (Map.insert (widText, stepId) record steps)
              pure (Right ())
        _ -> pure (Left (WorkflowConflict {workflowId = widText}))
  closeStream db wid stepId key writtenBy token =
    writeStream db wid stepId key streamClosedSentinel (Just portableJsonSerialization) writtenBy token
  close _ = close MockSystemDB
  recordSleep db wid stepId duration token = do
    now <- memDbNow db
    case addTimeout now duration of
      Nothing -> pure (Left (invalidInput "duration" "does not resolve to a representable wake time"))
      Just wakeAt -> do
        existing <- checkStep db wid stepId sleepStepName
        case existing of
          Left err -> pure (Left err)
          Right (Just step) -> case step.stepRecordOutput >>= readMaybe . Text.unpack of
            Just millis -> pure (Right (timestampFromEpochMs millis))
            Nothing -> pure (Right wakeAt)
          Right Nothing -> do
            recorded <-
              recordStep
                db
                wid
                stepId
                sleepStepName
                (OutcomeOutput (Just (Text.pack (show (timestampToEpochMs wakeAt)))))
                Nothing
                (Just (StepTiming now wakeAt))
                token
            case recorded of
              -- A rival recorded this sleep between the check and the write:
              -- adopt its wake time, as the live checkpoint does, rather than
              -- disagree with a record the workflow will replay.
              Left (StepNondeterminism {}) -> do
                adopted <- checkStep db wid stepId sleepStepName
                case adopted of
                  Right (Just step) -> case step.stepRecordOutput >>= readMaybe . Text.unpack of
                    Just millis -> pure (Right (timestampFromEpochMs millis))
                    Nothing -> pure (Left (Malformed "sleep reported as recorded but cannot be read back"))
                  Left err -> pure (Left err)
                  Right Nothing -> pure (Left (Malformed "sleep reported as recorded but cannot be read back"))
              Left err -> pure (Left err)
              Right () -> pure (Right wakeAt)
              Left err -> pure (Left err)
              Right () -> pure (Right wakeAt)
  setEvent db wid stepId key value serialization token = do
    now <- memDbNow db
    -- The publish opens with the ownership check, before the replay adopt,
    -- as the oracle orders it: a stale execution is refused rather than
    -- handed the recorded answer. The check shares the publish's
    -- transaction, so no hand-off can land between them.
    atomically $ do
      rows <- readTVar db.memRows
      case Map.lookup (widTextOf wid) rows of
        Just row | row.workflowRecordOwnerXid == Just token -> do
          -- A replay adopts the recorded publish instead of republishing it,
          -- mirroring PG's conflict adoption.
          steps <- readTVar db.memSteps
          case Map.member (widTextOf wid, stepId) steps of
            True -> pure (Right ())
            False -> do
              events <- readTVar db.memEvents
              writeTVar db.memEvents (Map.insert (widTextOf wid, key) (EncodedValue value serialization) events)
              history <- readTVar db.memEventHistory
              writeTVar db.memEventHistory (Map.insert (widTextOf wid, key, stepId) (EncodedValue value serialization) history)
              memInsertStep db wid stepId setEventStepName (Just value) serialization now
              pure (Right ())
        _ -> pure (Left (WorkflowConflict {workflowId = widTextOf wid}))
  -- A recorded getEvent step is the answer (value or recorded absence);
  -- otherwise the reader blocks on the events TVar until the value lands
  -- or its duration passes.
  getEvent db wid key duration caller = do
    let widText = widTextOf wid
    replayed <- case caller of
      Nothing -> pure Nothing
      Just c -> atomically (Map.lookup (widTextOf c.getEventCallerWorkflowId, c.getEventCallerStepId) <$> readTVar db.memSteps)
    case replayed of
      Just record -> pure (Right (memEncodedFromStep record))
      Nothing -> do
        now <- memDbNow db
        case addTimeout now duration of
          Nothing -> pure (Left (invalidInput "duration" "does not resolve to a representable wake time"))
          Just wakeAt -> do
            -- Mirror PG's deadline sleep checkpoint: the timeout owns its
            -- step id, stamped now. A replayed deadline stands.
            deadline <- case caller of
              Nothing -> pure (Right wakeAt)
              Just c -> do
                existing <- checkStep db c.getEventCallerWorkflowId c.getEventCallerTimeoutStepId sleepStepName
                case existing of
                  Left err -> pure (Left err)
                  Right (Just _) -> pure (Right wakeAt)
                  Right Nothing -> do
                    recorded <-
                      recordStep
                        db
                        c.getEventCallerWorkflowId
                        c.getEventCallerTimeoutStepId
                        sleepStepName
                        (OutcomeOutput (Just (Text.pack (show (timestampToEpochMs wakeAt)))))
                        Nothing
                        (Just (StepTiming now now))
                        c.getEventCallerToken
                    case recorded of
                      Left err -> pure (Left err)
                      Right () -> pure (Right wakeAt)
            case deadline of
              Left err -> pure (Left err)
              Right _ -> do
                -- Poll once before waiting: a present value must not lose
                -- to the timeout (a zero timeout would otherwise always miss).
                present <- atomically (Map.lookup (widText, key) <$> readTVar db.memEvents)
                waited <- case present of
                  Just value -> pure (Just value)
                  Nothing -> memTimeout (fromInteger (durationAsMillis duration * 1000)) (atomically (memWaitEvent db widText key))
                case caller of
                  Nothing -> pure (Right waited)
                  Just c -> do
                    -- The record opens with the caller's ownership check: a
                    -- stale reader's take and record roll back together.
                    rights <- atomically $ do
                      rows <- readTVar db.memRows
                      pure $ case Map.lookup (widTextOf c.getEventCallerWorkflowId) rows of
                        Just row | row.workflowRecordOwnerXid == Just c.getEventCallerToken -> Right ()
                        _ -> Left (WorkflowConflict {workflowId = widTextOf c.getEventCallerWorkflowId})
                    case rights of
                      Left conflict -> pure (Left conflict)
                      Right () -> do
                        completedAt <- memDbNow db
                        atomically (memInsertStep db c.getEventCallerWorkflowId c.getEventCallerStepId getEventStepName ((.encodedValue) <$> waited) (waited >>= (.encodedSerialization)) completedAt)
                        pure (Right waited)
  getAllNotifications db (WorkflowId widText) = do
    notifications <- readTVarIO db.memNotifications
    pure (Right (fromMaybe [] (Map.lookup widText notifications)))
  getAllEvents db (WorkflowId widText) = do
    events <- readTVarIO db.memEvents
    pure
      ( Right
          [ EventRecord {eventKey = key, eventValue = value.encodedValue, eventSerialization = value.encodedSerialization}
            | ((eventWid, key), value) <- Map.toList events,
              eventWid == widText
          ]
      )
  readStreamValues db (WorkflowId widText) key offset limit
    | offset < 0 = pure (Left (invalidInput "offset" "must not be negative"))
    | limit < 1 = pure (Left (invalidInput "limit" "must be at least 1"))
    | otherwise = atomically $ do
        rows <- readTVar db.memRows
        streams <- readTVar db.memStreams
        case Map.lookup widText rows of
          Nothing -> pure (Left (NonExistentWorkflow {workflowIds = [widText]}))
          Just workflow -> do
            let entries = fromMaybe [] (Map.lookup (widText, key) streams)
                page = take limit (drop offset entries)
                values = [EncodedValue entry.streamValue entry.streamSerialization | entry <- page]
            pure (Right (StreamRead workflow.workflowRecordStatus values))
  awaitStreamValues db wid@(WorkflowId widText) key offset limit deadline pollInterval
    | durationAsMillis pollInterval < 1 = pure (Left (invalidInput "polling_interval" "must be at least 1 millisecond"))
    | otherwise = loop False
    where
      -- Mirrors the Postgres await: values win, then a final look once the
      -- producer leaves Pending/Enqueued/Delayed (a cancel or timeout can
      -- mark it terminal while its last writes are still committing), then
      -- bounded waits. A cancel or timeout sets a terminal status out of
      -- band, so the drain stops at the first empty offset.
      loop finalLook = do
        page <- readStreamValues db wid key offset limit
        case page of
          Left err -> pure (Left err)
          Right found
            | not (null found.streamValues) -> pure (Right (Values found.streamValues))
            | finalLook -> pure (Right Ended)
            | found.streamStatus `notElem` [Pending, Enqueued, Delayed] -> loop True
            | otherwise -> do
                now <- memDbNow db
                -- A passed deadline is a timeout, like the Postgres backend:
                -- 'durationSince' is 'Nothing' once the end is past, which a
                -- missing deadline also reports, so match those separately.
                case deadline of
                  Nothing -> do
                    threadDelay (fromInteger (durationAsMillis pollInterval * 1000))
                    loop False
                  Just endAt -> case durationSince endAt now of
                    Nothing -> pure (Right TimedOut)
                    Just remaining
                      | durationAsMillis remaining <= 0 -> pure (Right TimedOut)
                      | otherwise -> do
                          threadDelay (fromInteger (durationAsMillis (shorter pollInterval remaining) * 1000))
                          loop False
      shorter first second
        | durationAsMillis first <= durationAsMillis second = first
        | otherwise = second
  getAllStreamEntries db (WorkflowId widText) = do
    streams <- readTVarIO db.memStreams
    pure
      ( Right
          ( sortOn (\entry -> (entry.streamKey, entry.streamOffset))
              [entry | ((streamWorkflow, _), entries) <- Map.toList streams, streamWorkflow == widText, entry <- entries]
          )
      )
  createApplicationVersion db versionName applicationName = do
    now <- memDbNow db
    atomically $ do
      versions <- readTVar db.memVersions
      let existing = [v | v <- versions, v.versionInfoName == versionName, v.versionInfoApplicationName == applicationName]
      if null existing
        then do
          writeTVar db.memVersions (versions <> [VersionInfo {versionInfoApplicationName = applicationName, versionInfoId = versionName, versionInfoName = versionName, versionInfoTimestamp = now, versionInfoCreatedAt = now}])
          pure (Right ())
        else pure (Right ())
  listApplicationVersions db = Right <$> readTVarIO db.memVersions
  getLatestApplicationVersion db applicationName = do
    versions <- readTVarIO db.memVersions
    let scoped = [v | v <- versions, v.versionInfoApplicationName == applicationName || v.versionInfoApplicationName == Nothing]
    pure (Right (case sortOn (.versionInfoTimestamp) scoped of [] -> Nothing; vs -> Just (last vs)))
  updateApplicationVersionTimestamp db versionName timestamp applicationName =
    atomically (modifyTVar db.memVersions (map (\v -> if v.versionInfoName == versionName && v.versionInfoApplicationName == applicationName then v {versionInfoTimestamp = timestamp} else v)))
      >> pure (Right ())
  upsertQueue db queue onExisting = atomically $ do
    queues <- readTVar db.memQueues
    application <- readTVar db.memApplicationName
    let existing = Map.lookup queue.newQueueName queues
    case existing of
      Just _ | onExisting == LeaveExisting -> pure (Right False)
      _ -> do
        -- The SQL upsert falls back to the backend's application when the
        -- request names none, so a registered queue is never unclaimed.
        writeTVar db.memQueues (Map.insert queue.newQueueName (memQueueRecord queue {newQueueApplicationName = queue.newQueueApplicationName <|> application}) queues)
        pure (Right (isNothing existing))
  -- The claim: worker/concurrency budgets from the queue's limits (rate
  -- limits are not modelled — Mem keeps no dequeue history), candidates
  -- in priority/created order, flipped to PENDING with the executor and
  -- version stamped, exactly the SQL claim's columns.
  startQueuedWorkflows db queue executorId applicationVersion partitionKey localRunning partitionLocalRunning ownerToken = do
    now <- memDbNow db
    atomically $ do
      rows <- readTVar db.memRows
      application <- readTVar db.memApplicationName
      let queueName = queue.queueRecordName
          workerBudget = case queue.queueRecordWorkerConcurrency of
            Just cap -> Just (max 0 (fromIntegral cap - localRunning))
            Nothing -> Nothing
          partitionWorkerBudget = case (queue.queueRecordPartitionWorkerConcurrency, partitionKey) of
            (Just cap, Just _) -> Just (max 0 (fromIntegral cap - partitionLocalRunning))
            _ -> Nothing
          narrow current available = Just (maybe available (min available) current)
          budget = foldr (\available current -> narrow current available) Nothing [b | Just b <- [workerBudget, partitionWorkerBudget]]
          running = length [() | row <- Map.elems rows, visibleToApplication application row, row.workflowRecordStatus == Pending, row.workflowRecordQueueName == Just queueName]
          concurrencyBudget = case queue.queueRecordConcurrency of
            Just cap -> Just (max 0 (fromIntegral cap - fromIntegral running))
            Nothing -> Nothing
          partitionConcurrencyBudget = case (queue.queueRecordPartitionConcurrency, partitionKey) of
            (Just cap, Just key) ->
              Just (max 0 (fromIntegral cap - fromIntegral (length [() | row <- Map.elems rows, visibleToApplication application row, row.workflowRecordStatus == Pending, row.workflowRecordQueueName == Just queueName, row.workflowRecordQueuePartitionKey == Just key])))
            _ -> Nothing
          budget' = foldr (\available current -> narrow current available) budget [b | Just b <- [concurrencyBudget, partitionConcurrencyBudget]]
      case budget' of
        Just n | n == 0 -> pure (Right [])
        _ -> do
          versions <- readTVar db.memVersions
          let latest = case sortOn (.versionInfoTimestamp) versions of [] -> Nothing; vs -> Just (last vs).versionInfoName
              isLatest = maybe True (== applicationVersion) latest
              candidates =
                [ widText
                  | (widText, row) <- sortOn (\(_, row) -> (row.workflowRecordPriority, row.workflowRecordCreatedAt)) (Map.toList rows),
                    row.workflowRecordQueueName == Just queueName,
                    visibleToApplication application row,
                    row.workflowRecordStatus == Enqueued,
                    row.workflowRecordApplicationVersion == Just applicationVersion || (isLatest && isNothing row.workflowRecordApplicationVersion),
                    case partitionKey of
                      Nothing -> True
                      Just key -> row.workflowRecordQueuePartitionKey == Just key
                ]
              claimed = maybe candidates (\n -> take (fromIntegral n) candidates) budget'
          writeTVar db.memRows (foldr (memClaimRow executorId applicationVersion now ownerToken) rows claimed)
          pure (Right (map WorkflowId claimed))
  getQueuePartitions db queueName = do
    rows <- readTVarIO db.memRows
    application <- readTVarIO db.memApplicationName
    pure
      ( Right (sort (Set.toList (Set.fromList [key | row <- Map.elems rows, visibleToApplication application row, row.workflowRecordQueueName == Just queueName, row.workflowRecordStatus == Enqueued, Just key <- [row.workflowRecordQueuePartitionKey]])))
      )
  -- One head per partition with no pending work, in key order.
  startQueuedPartitionedWorkflows db queue executorId applicationVersion maxTasks ownerToken = do
    now <- memDbNow db
    atomically $ do
      rows <- readTVar db.memRows
      application <- readTVar db.memApplicationName
      versions0 <- readTVar db.memVersions
      let queueName = queue.queueRecordName
          cap = fromIntegral dequeueSweepCap
          limit = maybe cap (min cap) (fromIntegral <$> maxTasks)
          versions = sortOn (.versionInfoTimestamp) versions0
          latest = case versions of [] -> Nothing; vs -> Just (last vs).versionInfoName
          isLatest = maybe True (== applicationVersion) latest
          keys =
            [ key
              | key <- sort (Set.toList (Set.fromList [key | row <- Map.elems rows, visibleToApplication application row, row.workflowRecordQueueName == Just queueName, row.workflowRecordStatus == Enqueued, Just key <- [row.workflowRecordQueuePartitionKey]])),
                not (any (\row -> row.workflowRecordQueueName == Just queueName && row.workflowRecordQueuePartitionKey == Just key && row.workflowRecordStatus == Pending) (Map.elems rows))
            ]
          heads =
            take limit
              [ widText
                | key <- keys,
                  (widText, _) : _ <-
                    [ sortOn (\(widText, row) -> (row.workflowRecordPriority, row.workflowRecordCreatedAt, widText))
                        [ (widText, row)
                          | (widText, row) <- Map.toList rows,
                            row.workflowRecordQueueName == Just queueName,
                            visibleToApplication application row,
                            row.workflowRecordStatus == Enqueued,
                            row.workflowRecordApplicationVersion == Just applicationVersion || (isLatest && isNothing row.workflowRecordApplicationVersion),
                            row.workflowRecordQueuePartitionKey == Just key
                        ]
                    ]
              ]
      writeTVar db.memRows (foldr (memClaimRow executorId applicationVersion now ownerToken) rows heads)
      pure (Right (map WorkflowId heads))
  getQueue db name = do
    queues <- readTVarIO db.memQueues
    pure (Right (Map.lookup name queues))
  listQueues db applications = do
    queues <- readTVarIO db.memQueues
    application <- readTVarIO db.memApplicationName
    let visible row = case applications of
          -- The listener's own application plus the unclaimed rows, as the
          -- SQL claim's @$1 is null or application_name = $1@ guard does: a
          -- listener with no application sees everything.
          Unset -> case application of
            Nothing -> True
            Just name -> row.queueRecordApplicationName == Nothing || row.queueRecordApplicationName == Just name
          Named [] -> True
          Named names -> maybe True (`elem` names) row.queueRecordApplicationName
          AnyApplication -> True
    pure (Right [row | row <- Map.elems queues, visible row])
  updateQueue db name update validate = do
    queues <- readTVarIO db.memQueues
    case Map.lookup name queues of
      Nothing -> pure (Left (NotRegistered {kind = "Queue", name = name}))
      Just record
        | isQueueUpdateEmpty update -> pure (Right record)
        | otherwise -> case validate record (memApplyQueueUpdate update record) of
            Left err -> pure (Left err)
            Right () -> do
              let updated = memApplyQueueUpdate update record
              atomically (modifyTVar db.memQueues (Map.insert name updated))
              pure (Right updated)
  debounceDelayedWorkflow db request caller = memDebounce db request caller
  deleteQueue db name = atomically (modifyTVar db.memQueues (Map.delete name)) >> pure (Right ())
  createSchedule db new _caller = do
    now <- memDbNow db
    atomically $ do
      schedules <- readTVar db.memSchedules
      if Map.member new.newScheduleName schedules
        then pure (Left (Malformed ("schedule " <> new.newScheduleName <> " already exists")))
        else do
          writeTVar db.memSchedules (Map.insert new.newScheduleName (memScheduleRecord new now) schedules)
          pure (Right ())
  upsertSchedule db new _caller = do
    now <- memDbNow db
    atomically $ do
      schedules <- readTVar db.memSchedules
      writeTVar db.memSchedules (Map.insert new.newScheduleName (memScheduleRecord new now) schedules)
      pure (Right ())
  applySchedules db schedules = do
    now <- memDbNow db
    atomically $ do
      stored <- readTVar db.memSchedules
      let applied = foldr (\new acc -> Map.insert new.newScheduleName (memScheduleRecord new now) acc) stored schedules
      writeTVar db.memSchedules applied
      pure (Right ())
  getSchedule db name _caller = do
    schedules <- readTVarIO db.memSchedules
    pure (Right (Map.lookup name schedules))
  listSchedules db scheduleFilter _caller = do
    schedules <- readTVarIO db.memSchedules
    pure
      ( Right
          [ record
            | record <- Map.elems schedules,
              null scheduleFilter.scheduleFilterStatuses || record.scheduleRecordStatus `elem` scheduleFilter.scheduleFilterStatuses,
              null scheduleFilter.scheduleFilterWorkflowNames || record.scheduleRecordWorkflowName `elem` scheduleFilter.scheduleFilterWorkflowNames,
              null scheduleFilter.scheduleFilterNamePrefixes || any (`Text.isPrefixOf` record.scheduleRecordName) scheduleFilter.scheduleFilterNamePrefixes
          ]
      )
  -- An empty first-wait has no answer it could ever give, so it is
  -- refused like the live backend ('Backend.awaitFirstWorkflowId'), not
  -- parked forever: a poll over an empty set matches nothing, which would
  -- hang this wait instead of failing it.
  awaitFirstWorkflowId _ [] _ = pure (Left (invalidInput "workflow_ids" "must name at least one workflow to wait for"))
  awaitFirstWorkflowId db ids _duration = atomically $ do
    rows <- readTVar db.memRows
    case [wid | wid <- ids, Just row <- [Map.lookup (widTextOf wid) rows], isJust (memAwaited row)] of
      (wid : _) -> pure (Right wid)
      [] -> retry
  awaitWorkflowIds db ids _duration = atomically $ do
    rows <- readTVar db.memRows
    let outstanding = [wid | wid <- ids, maybe True (isNothing . memAwaited) (Map.lookup (widTextOf wid) rows)]
    if null outstanding then pure (Right ()) else retry
  updateSchedule db name update _caller = atomically $ do
    schedules <- readTVar db.memSchedules
    case Map.lookup name schedules of
      Nothing -> pure (Left (NotRegistered {kind = "Schedule", name = name}))
      Just record -> do
        writeTVar db.memSchedules (Map.insert name (memApplyScheduleUpdate update record) schedules)
        pure (Right ())
  setScheduleStatus db name status _caller = atomically $ do
    schedules <- readTVar db.memSchedules
    case Map.lookup name schedules of
      Nothing -> pure (Left (NotRegistered {kind = "Schedule", name = name}))
      Just record -> do
        writeTVar db.memSchedules (Map.insert name (record {scheduleRecordStatus = status}) schedules)
        pure (Right ())
  updateScheduleLastFiredAt db name lastFiredAt = atomically $ do
    modifyTVar db.memSchedules (Map.adjust (\record -> record {scheduleRecordLastFiredAt = Just lastFiredAt}) name)
    pure (Right ())
  deleteSchedule db name _caller = atomically (modifyTVar db.memSchedules (Map.delete name)) >> pure (Right ())
  renameApplication _ = renameApplication MockSystemDB
  -- The child's result lands under the await's own step id, named for the
  -- cross-SDK contract as the live backend names it.
  recordChildResult db parent stepId child outcome serialization timing token = atomically $ do
    rows <- readTVar db.memRows
    case Map.lookup parentText rows of
      -- Only the row's current owner records the result, as for a step.
      Just row | row.workflowRecordOwnerXid == Just token -> do
        steps <- readTVar db.memSteps
        let prior = Map.lookup (parentText, stepId) steps
            entry =
              StepRecord
                { stepRecordWorkflowId = parent,
                  stepRecordStepId = stepId,
                  stepRecordStepName = getResultStepName,
                  stepRecordOutput = case outcome of OutcomeOutput output -> output; _ -> prior >>= (.stepRecordOutput),
                  stepRecordError = case outcome of OutcomeError message -> Just message; _ -> Nothing,
                  stepRecordChildWorkflowId = Just child,
                  stepRecordSerialization = serialization,
                  stepRecordStartedAt = timing >>= (Just . (.stepTimingStartedAt)),
                  stepRecordCompletedAt = timing >>= (Just . (.stepTimingCompletedAt))
                }
        case prior of
          Just kept | kept.stepRecordCompletedAt /= entry.stepRecordCompletedAt ->
            pure (Left (StepNondeterminism {workflowId = parentText, stepId = stepId}))
          _ -> do
            writeTVar db.memSteps (Map.insert (parentText, stepId) entry steps)
            pure (Right ())
      _ -> pure (Left (WorkflowConflict {workflowId = parentText}))
    where
      WorkflowId parentText = parent

-- | Forks onto fresh rows, copying the source's history below the fork
-- point: the fork replays what came before and runs what follows. A fork
-- id the call does not name derives deterministically from its source, so
-- the double execution agrees.
memFork :: MemSystemDB s -> [(Text, Maybe Text, Int)] -> ForkOptions -> IOSim s (Either Error [WorkflowId])
memFork db forks options = atomically $ do
  rows <- readTVar db.memRows
  steps <- readTVar db.memSteps
  owner <- simOwnerText db
  let (ids, rows', steps') = foldr (memForkOne options owner) ([], rows, steps) forks
  writeTVar db.memRows rows'
  writeTVar db.memSteps steps'
  pure (Right ids)
  where
    memForkOne options' owner' (source, chosen, startStep) (ids, rows, steps) =
      let QueueName internal = internalQueueName
          forkedText = fromMaybe (source <> "-fork") chosen
          -- A fork restarts on the internal queue unless told otherwise, and
          -- is always enqueued: mirrors the SQL insert ('ENQUEUED', the
          -- queue default in 'runFork'), so a driven dequeue claims it.
          status = Enqueued
          queueName = options'.forkOptionsQueueName <|> Just internal
          row = case Map.lookup source rows of
            Just origin ->
              origin
                { workflowRecordId = WorkflowId forkedText,
                  workflowRecordStatus = status,
                  workflowRecordOutput = Nothing,
                  workflowRecordError = Nothing,
                  workflowRecordQueueName = queueName,
                  workflowRecordQueuePartitionKey = options'.forkOptionsQueuePartitionKey,
                  workflowRecordForkedFrom = Just (WorkflowId source),
                  workflowRecordWasForkedFrom = True,
                  workflowRecordRecoveryAttempts = 0
                }
            Nothing ->
              (memFreshRow (newWorkflow forkedText) Nothing owner')
                { workflowRecordStatus = status,
                  workflowRecordQueueName = queueName,
                  workflowRecordQueuePartitionKey = options'.forkOptionsQueuePartitionKey,
                  workflowRecordForkedFrom = Just (WorkflowId source),
                  workflowRecordWasForkedFrom = True
                }
          -- Mirrors the SQL copy: every copied step that names a replaced
          -- child is rewritten, not only the step that started it.
          rewritten child =
            fromMaybe child (lookupReplacement child)
          lookupReplacement (WorkflowId childText) =
            WorkflowId <$> lookup childText options'.forkOptionsReplacementChildren
          copied =
            [ ( (forkedText, sid),
                record
                  { stepRecordWorkflowId = WorkflowId forkedText,
                    stepRecordChildWorkflowId = rewritten <$> record.stepRecordChildWorkflowId
                  }
              )
              | ((wid, sid), record) <- Map.toList steps,
                wid == source,
                sid < startStep
            ]
          steps' = foldr (uncurry Map.insert) steps copied
       in (WorkflowId forkedText : ids, Map.insert forkedText row rows, steps')

-- | Whether the row's JSON attributes contain the filter's, as the SQL
-- @attributes @> filter@ guard does: every filter key is present with a
-- contained value, recursively; anything else compares by equality. A null
-- filter matches every row (@$30 is null@); unparseable JSON matches nothing.
memAttributesContain :: Maybe Text -> Maybe Text -> Bool
memAttributesContain _ Nothing = True
memAttributesContain Nothing (Just _) = False
memAttributesContain (Just row) (Just wanted) =
  case (eitherDecodeStrict (encodeUtf8 row), eitherDecodeStrict (encodeUtf8 wanted)) of
    (Right rowValue, Right wantedValue) -> jsonContains rowValue wantedValue
    _ -> False
  where
    jsonContains (Object rowFields) (Object wantedFields) =
      all (\(key, value) -> maybe False (`jsonContains` value) (KeyMap.lookup key rowFields)) (KeyMap.toList wantedFields)
    jsonContains rowValue wantedValue = rowValue == wantedValue

-- * Helpers for the in-memory subsystems

-- | Every workflow forked from the root, transitively, excluding the root
-- itself. Mirrors the live fork expansion over the fork table.
memDescendants :: Map Text WorkflowRecord -> Text -> [Text]
memDescendants rows root = go [] [root]
  where
    go seen [] = seen
    go seen (parent : rest) =
      let children =
            [ wid
              | (wid, row) <- Map.toList rows,
                row.workflowRecordForkedFrom == Just (WorkflowId parent),
                wid `notElem` seen
            ]
       in go (seen <> children) (rest <> children)

-- | Every workflow parented under the root, transitively, excluding the
-- root itself. Mirrors the SQL 'descendantsSession' walk over
-- @parent_workflow_id@ — not the fork link 'memDescendants' follows.
memChildTree :: Map Text WorkflowRecord -> Text -> [Text]
memChildTree rows root = go [] [root]
  where
    go seen [] = seen
    go seen (parent : rest) =
      let children =
            [ wid
              | (wid, row) <- Map.toList rows,
                row.workflowRecordParentWorkflowId == Just (WorkflowId parent),
                wid `notElem` seen
            ]
       in go (seen <> children) (rest <> children)

-- | The nearest named ancestor of a workflow: what the tree a cancel
-- cascade reached it through is called. Falls back to naming every target,
-- as the oracle joins them, when the ancestry gives out. Mirrors the SQL
-- cancel's @rootContaining@ walk over the rows already in hand.
memRootContaining :: Map Text WorkflowRecord -> [Text] -> Text -> Text
memRootContaining rows roots start = go [start] start
  where
    go seen current = case Map.lookup current rows of
      Just row -> case row.workflowRecordParentWorkflowId of
        Just (WorkflowId parent)
          | parent `elem` roots -> parent
          | parent `notElem` seen -> go (current : seen) parent
          | otherwise -> Text.intercalate ", " roots
        _ -> Text.intercalate ", " roots
      _ -> Text.intercalate ", " roots

widTextOf :: WorkflowId -> Text
widTextOf (WorkflowId widText) = widText

-- | The encoded value a recorded step stands for: the raw output plus the
-- serialization, or 'Nothing' for a recorded absence (a recv timeout).
memEncodedFromStep :: StepRecord -> Maybe EncodedValue
memEncodedFromStep record = (\raw -> EncodedValue raw record.stepRecordSerialization) <$> record.stepRecordOutput

-- | Insert a completed step at a position, the shape the recv/getEvent/
-- setEvent checkpoints use.
memInsertStep :: MemSystemDB s -> WorkflowId -> Int -> Text -> Maybe Text -> Maybe Text -> Timestamp -> STM (IOSim s) ()
memInsertStep db wid stepId name output serialization now = do
  steps <- readTVar db.memSteps
  let entry =
        StepRecord
          { stepRecordWorkflowId = wid,
            stepRecordStepId = stepId,
            stepRecordStepName = name,
            stepRecordOutput = output,
            stepRecordError = Nothing,
            stepRecordChildWorkflowId = Nothing,
            stepRecordSerialization = serialization,
            stepRecordStartedAt = Just now,
            stepRecordCompletedAt = Just now
          }
  writeTVar db.memSteps (Map.insert (widTextOf wid, stepId) entry steps)

memRecordRecvStep :: MemSystemDB s -> WorkflowId -> Int -> Text -> Maybe EncodedValue -> IOSim s ()
memRecordRecvStep db wid stepId name value = do
  now <- memDbNow db
  atomically (memInsertStep db wid stepId name ((.encodedValue) <$> value) (value >>= (.encodedSerialization)) now)

-- | Take the first parked message for a destination on a topic, blocking
-- until one lands. Hands back the stored id with the value, so the taker
-- can record which step consumed it.
memTakeMessage :: MemSystemDB s -> Text -> Text -> STM (IOSim s) (Text, EncodedValue)
memTakeMessage db wid topic = do
  stored <- readTVar db.memMessages
  case Map.lookup (wid, topic) stored of
    Just ((uuid, value) : _) -> do
      writeTVar db.memMessages (Map.adjust (drop 1) (wid, topic) stored)
      pure (uuid, value)
    _ -> retry

-- | Marks one notification consumed, leaving the rest of the
-- destination's rows alone.
markConsumed :: Text -> NotificationRecord -> NotificationRecord
markConsumed uuid notification
  | notification.notificationRecordMessageUuid == uuid = notification {notificationRecordConsumed = True}
  | otherwise = notification

-- | Wait for an event to be published, blocking until it lands.
memWaitEvent :: MemSystemDB s -> Text -> Text -> STM (IOSim s) EncodedValue
memWaitEvent db widText key = do
  events <- readTVar db.memEvents
  case Map.lookup (widText, key) events of
    Just value -> pure value
    Nothing -> retry

-- | A duration-bounded wait, pinned to the simulation monad so the
-- class-polymorphic 'timeout' cannot settle elsewhere.
memTimeout :: forall s a. Int -> IOSim s a -> IOSim s (Maybe a)
memTimeout micros action = timeout @(IOSim s) micros action

-- | Bounce a debounce onto its deduplication key, recording the caller's
-- step when one calls. Mirrors the Postgres lane exactly: validate first;
-- a recorded caller step is the answer and the work never re-runs;
-- otherwise the bounce runs (extend the matching DELAYED debounced row, or
-- report its holder, or report the key free) and the caller step records
-- the JSON outcome beside it, all in one STM transaction.
memDebounce :: MemSystemDB s -> DebounceRequest -> Maybe (WorkflowId, Int) -> IOSim s (Either Error Debounce)
memDebounce db request caller = case debounceValidate request of
  Left err -> pure (Left err)
  Right () -> do
    now <- memDbNow db
    application <- readTVarIO db.memApplicationName
    let scope = request.debounceRequestApplicationName <|> application
    atomically $ do
      rows <- readTVar db.memRows
      steps <- readTVar db.memSteps
      case caller of
        Just (callerWid, callerStep)
          | Just stored <- Map.lookup (widTextOf callerWid, callerStep) steps ->
              pure (memReplayDebounce callerWid callerStep stored)
        _ -> do
          let (bounced, rows') = memBounce scope request now rows
          writeTVar db.memRows rows'
          case caller of
            Nothing -> pure (Right bounced)
            Just (callerWid, callerStep) -> do
              writeTVar db.memSteps (Map.insert (widTextOf callerWid, callerStep) (memDebounceStep callerWid callerStep now bounced) steps)
              pure (Right bounced)

-- | The bounce itself, as pure row surgery: extend the one DELAYED
-- debounced row holding the key (the period from the clock, capped at its
-- deadline, inputs replaced), or describe whoever holds the key, or report
-- it free. First in key order, as the SQL update's single returning row
-- is. The class and instance match null-agnostically, as the update's
-- @NULLIF@ does: an empty string is absence on either side.
memBounce :: Maybe Text -> DebounceRequest -> Timestamp -> Map Text WorkflowRecord -> (Debounce, Map Text WorkflowRecord)
memBounce scope request now rows = case Map.lookupMin (Map.filter isBouncable rows) of
  Just (wid, row) ->
    let release = saturateTimeout now request.debounceRequestPeriod
        capped = case row.workflowRecordDebounceDeadline of
          Just deadline | deadline < release -> deadline
          _ -> release
        row' =
          row
            { workflowRecordDelayUntil = Just capped,
              workflowRecordSerialization = request.debounceRequestSerialization,
              workflowRecordApplicationName = row.workflowRecordApplicationName <|> scope,
              workflowRecordInput = request.debounceRequestInputs
            }
     in (Debounced wid, Map.insert wid row' rows)
  Nothing -> case Map.lookupMin (Map.filter isHolder rows) of
    Nothing -> (DebounceUnheld, rows)
    Just (_, row) -> (DebounceHeld (memDebounceHolder row), rows)
  where
    queue = request.debounceRequestQueueName
    key = request.debounceRequestDeduplicationId
    isHolder row = row.workflowRecordQueueName == Just queue && row.workflowRecordDeduplicationId == Just key
    -- The update's guard, conjunct for conjunct: the holder's whole key
    -- matches, the row still waits, it debounces, and the scope admits
    -- it. Empty spells absence on either side through the shared
    -- row-key reader, as the update's @NULLIF@ does.
    isBouncable row =
      isHolder row
        && holderKey row == Just request.debounceRequestWorkflow
        && row.workflowRecordStatus == Delayed
        && row.workflowRecordIsDebounced
        && (scope == Nothing || row.workflowRecordApplicationName == scope || row.workflowRecordApplicationName == Nothing)
    holderKey row = case row.workflowRecordName of
      Nothing -> Nothing
      Just name -> Just (workflowKeyFromRow name row.workflowRecordClassName row.workflowRecordConfigName)

-- | The holder as the domain describes it: what holds the key, not merely
-- that something does, so the caller can tell a collision from a
-- coincidence. Mirrors the backend's @debounceHeld@.
memDebounceHolder :: WorkflowRecord -> DebounceHolder
memDebounceHolder row =
  let WorkflowId wid = row.workflowRecordId
   in DebounceHolder
        { debounceHolderWorkflowId = wid,
          debounceHolderIsDebounced = row.workflowRecordIsDebounced,
          debounceHolderWorkflowName = row.workflowRecordName,
          debounceHolderClassName = row.workflowRecordClassName,
          debounceHolderConfigName = row.workflowRecordConfigName,
          debounceHolderApplicationName = row.workflowRecordApplicationName
        }

-- | The caller step a bounce records: the JSON outcome under the debounce
-- step name, started and finished now. Mirrors the backend's record beside
-- its bounce.
memDebounceStep :: WorkflowId -> Int -> Timestamp -> Debounce -> StepRecord
memDebounceStep callerWid callerStep now bounced =
  StepRecord
    { stepRecordWorkflowId = callerWid,
      stepRecordStepId = callerStep,
      stepRecordStepName = debounceStepName,
      stepRecordOutput = Just (decodeUtf8 (LBS.toStrict (encode bounced))),
      stepRecordError = Nothing,
      stepRecordChildWorkflowId = Nothing,
      stepRecordSerialization = Nothing,
      stepRecordStartedAt = Just now,
      stepRecordCompletedAt = Just now
    }

-- | A recorded bounce step as the answer it holds. A missing output, or one
-- this build cannot read, is malformed — a step that ran has an answer.
-- Mirrors the backend's @replayedDebounce@.
memReplayDebounce :: WorkflowId -> Int -> StepRecord -> Either Error Debounce
memReplayDebounce (WorkflowId widText) stepId stored = case stored.stepRecordOutput of
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

-- | The caller's verified state, mirroring 'checkCallerStep': the same
-- refusals (a missing caller row, a cancelled caller, a position recorded
-- under another name), the same replay decoding, and the same freshness.
-- The expected name is the step the call replays: 'rewindStepName' for a
-- rewind, the send's own name for a send.
verifyCallerStep :: Text -> Maybe (WorkflowId, Int) -> Map Text WorkflowRecord -> Map (Text, Int) StepRecord -> CallerStep
verifyCallerStep expectedName caller rows steps = case caller of
  Nothing -> CallerAbsent
  Just (callerWid, callerStep) ->
    let callerText = widTextOf callerWid
     in case Map.lookup callerText rows of
          Nothing -> CallerRefused (NonExistentWorkflow {workflowIds = [callerText]})
          Just callerRow
            | callerRow.workflowRecordStatus == Cancelled ->
                CallerRefused (WorkflowCancelled {workflowId = callerText})
            | otherwise -> case Map.lookup (callerText, callerStep) steps of
                Nothing -> CallerFresh
                Just record -> case record.stepRecordStepName of
                  name | name /= expectedName ->
                    CallerRefused (UnexpectedStep {workflowId = callerText, stepId = callerStep, expected = expectedName, recorded = name})
                  _ -> CallerReplayed (replayMemStep record)
  where
    replayMemStep record = case record.stepRecordError of
      Just text -> case simCallerEncoding record.stepRecordSerialization of
        Left err -> Left err
        Right SimCallerPortable -> case decodePortableError text of
          Right err -> Left err
          Left message -> Left (Malformed ("unreadable recorded step error: " <> message))
        Right SimCallerNative -> case eitherDecodeStrict (encodeUtf8 text) of
          Right err -> Left err
          Left _ -> Left (Malformed "unreadable recorded step error")
      Nothing -> Right ()

-- | How a sim caller step's recorded bytes are tagged, mirroring the live
-- 'callerEncoding': NULL/empty/legacy native, `portable_json` portable, any
-- other tag refused.
data SimCallerEncoding
  = SimCallerNative
  | SimCallerPortable

simCallerEncoding :: Maybe Text -> Either Error SimCallerEncoding
simCallerEncoding Nothing = Right SimCallerNative
simCallerEncoding (Just "") = Right SimCallerNative
simCallerEncoding (Just "rust_serde") = Right SimCallerNative
simCallerEncoding (Just tag)
  | tag == portableJsonSerialization = Right SimCallerPortable
simCallerEncoding (Just name) = Left (UnknownSerialization {name})

-- | Deliver messages to their topics and destinations, appending a
-- notification per delivery so 'getAllNotifications' sees them, and record
-- the caller's step. A send writes the messages TVar, which is what wakes
-- a parked 'recv'.
memSend :: MemSystemDB s -> Text -> [SendMessage] -> Maybe Text -> Maybe (WorkflowId, Int) -> Maybe WorkflowOwner -> Bool -> IOSim s (Either Error ())
memSend db stepName messages serialization caller owner sendToForks = do
  now <- memDbNow db
  -- The duplicate-key refusal applies to the caller's list, before fan-out,
  -- exactly as live checks: one key fanning out is legal.
  case validateSendKeys messages of
    Left err -> pure (Left err)
    Right () -> atomically $ do
      rows <- readTVar db.memRows
      stored <- readTVar db.memMessages
      seen <- readTVar db.memMessageIds
      notifications <- readTVar db.memNotifications
      steps <- readTVar db.memSteps
      -- An execution's send opens with its ownership check, before the
      -- caller-step verdicts: a stale execution delivers nothing.
      let owned = case owner of
            Nothing -> Right ()
            Just o -> case Map.lookup (widTextOf o.ownerWorkflowId) rows of
              Just row | row.workflowRecordOwnerXid == Just o.ownerToken -> Right ()
              _ -> Left (WorkflowConflict {workflowId = widTextOf o.ownerWorkflowId})
      case (owned, verifyCallerStep stepName caller rows steps) of
        (Left conflict, _) -> pure (Left conflict)
        (_, CallerRefused err) -> pure (Left err)
        (_, CallerReplayed _) -> pure (Right ())
        _ -> do
          let topicOf m = maybe nullTopicSentinel (\(Topic topic) -> topic) m.sendTopic
              bodyOf m = EncodedValue m.sendMessageBody.serializedText serialization
            -- Mirror the live fan-out: the destination itself plus every
            -- workflow forked from it, transitively, sorted and deduplicated.
              expand m =
                let destination = widTextOf m.sendDestinationId
                 in destination : if sendToForks then sort (nub (memDescendants rows destination)) else []
              -- The stored id, scoped exactly as live scopes row ids (see
              -- 'messageUUIDForSend'): a resend finds its id taken and lands
              -- nowhere, like @ON CONFLICT (message_uuid) DO NOTHING@.
              uuidOf m recipient = case m.sendIdempotencyKey of
                Nothing -> Nothing
                Just (IdempotencyKey key) -> Just (key <> "::" <> recipient)
              deliveries =
                [ (recipient, topicOf m, bodyOf m, uuidOf m recipient)
                | m <- messages,
                  recipient <- expand m
                ]
              -- Notification ids stay unique per row, as live stores the
              -- message uuid: keyed deliveries reuse their scoped id (the
              -- seen-set admits each id once), unkeyed ones take the next
              -- per-destination-topic counter, seeded from the rows already
              -- stored so a later batch never reuses an earlier number.
              seedCounts :: Map (Text, Text) Int
              seedCounts =
                Map.fromListWith
                  (+)
                  [ ((wid, fromMaybe nullTopicSentinel notification.notificationRecordTopic), 1)
                  | (wid, priorNotifications) <- Map.toList notifications,
                    notification <- priorNotifications
                  ]
              insertOne (wid, topic, value, maybeUuid) (st, no, sn, counters) = case maybeUuid of
                Just uuid | Set.member uuid sn -> (st, no, sn, counters)
                Just uuid ->
                  ( Map.insertWith (<>) (wid, topic) [(uuid, value)] st,
                    Map.insertWith (<>) wid [notificationFor uuid topic value] no,
                    Set.insert uuid sn,
                    counters
                  )
                Nothing ->
                  let next = Map.findWithDefault 0 (wid, topic) counters + 1
                      uuid = topic <> ":" <> Text.pack (show next)
                   in ( Map.insertWith (<>) (wid, topic) [(uuid, value)] st,
                        Map.insertWith (<>) wid [notificationFor uuid topic value] no,
                        sn,
                        Map.insert (wid, topic) next counters
                      )
              (stored', notifications', seen', _) = foldr insertOne (stored, notifications, seen, seedCounts) deliveries
              notificationFor uuid topic value =
                NotificationRecord
                    { notificationRecordMessageUuid = uuid,
                      notificationRecordTopic = Just topic,
                      notificationRecordMessage = value.encodedValue,
                      notificationRecordSerialization = value.encodedSerialization,
                      notificationRecordCreatedAt = now,
                      notificationRecordConsumed = False
                    }
          writeTVar db.memMessages stored'
          writeTVar db.memMessageIds seen'
          writeTVar db.memNotifications notifications'
          case caller of
            Nothing -> pure (Right ())
            Just (callerWid, callerStep) -> do
              let entry =
                    StepRecord
                    { stepRecordWorkflowId = callerWid,
                      stepRecordStepId = callerStep,
                      stepRecordStepName = stepName,
                      stepRecordOutput = Nothing,
                      stepRecordError = Nothing,
                      stepRecordChildWorkflowId = Nothing,
                      stepRecordSerialization = Nothing,
                      stepRecordStartedAt = Just now,
                      stepRecordCompletedAt = Just now
                    }
              writeTVar db.memSteps (Map.insert (widTextOf callerWid, callerStep) entry steps)
              pure (Right ())

-- | Discards a finished workflow's history from the cut on and re-enqueues
-- it under the same id. Mirrors 'rewindTx' map by map: caller verification
-- first (missing, cancelled, or misnamed caller refused; recorded calls
-- replayed), then the guarded status read, event deletion with restoration
-- from the history undo log, stream close removal, step and history
-- deletion, message deletion by consume step, output clearing, and the row
-- reset. A recordable refusal commits a failure step for the caller
-- through the should-record gate, exactly as live does; anything else
-- returns unrecorded, and nothing has run yet either way. The status guard
-- is vacuous here — one atomic block admits no interleaving — but the
-- shape stays. Stored statuses are the ADT itself, so the unknown-spelling
-- refusal has no sim path; consumed rows with no recorded step are kept,
-- as live keeps rows whose @consumed_by_function_id@ stayed null.
memRewind :: MemSystemDB s -> Text -> Int -> SysRewindOptions -> Maybe (WorkflowId, Int) -> IOSim s (Either Error ())
memRewind db wid cut sysOpts caller = do
  now <- memDbNow db
  atomically $ do
    rows <- readTVar db.memRows
    steps <- readTVar db.memSteps
    case verifyCallerStep rewindStepName caller rows steps of
      CallerReplayed replayed -> pure replayed
      CallerRefused err -> refuseWith steps now err
      _ -> case Map.lookup wid rows of
        Nothing -> refuseWith steps now (NonExistentWorkflow {workflowIds = [wid]})
        Just row
          | isActiveStatus row.workflowRecordStatus ->
              refuseWith steps now (WorkflowNotRewindable {workflowId = wid, status = workflowStatusText row.workflowRecordStatus})
          | otherwise -> do
            events <- readTVar db.memEvents
            history <- readTVar db.memEventHistory
            streams <- readTVar db.memStreams
            stored <- readTVar db.memMessages
            seen <- readTVar db.memMessageIds
            notifications <- readTVar db.memNotifications
            consumed <- readTVar db.memConsumedBy
            let cutKeys = nub [key | ((w, key, step), _) <- Map.toList history, w == wid, step >= cut]
                restored =
                  [ ((wid, key), value)
                  | key <- cutKeys,
                    (_, value) <- take 1 (sortOn (negate . fst) [(step, value) | ((w, k, step), value) <- Map.toList history, w == wid, k == key, step < cut])
                  ]
                events' = foldr (\(key, value) acc -> Map.insert key value acc) (foldr Map.delete events [(wid, key) | key <- cutKeys]) restored
                isClose entry =
                  entry.streamStepId >= cut
                    && isStreamClosed entry.streamValue entry.streamSerialization
                streams' = Map.mapWithKey (\(w, _) entries -> if w == wid then filter (not . isClose) entries else entries) streams
                steps' = Map.filterWithKey (\(w, step) _ -> w /= wid || step < cut) steps
                history' = Map.filterWithKey (\(w, _, step) _ -> w /= wid || step < cut) history
                keepNotif notification =
                  notification.notificationRecordConsumed
                    && maybe True (< cut) (Map.lookup (wid, notification.notificationRecordMessageUuid) consumed)
                prior = fromMaybe [] (Map.lookup wid notifications)
                (kept, dropped) = partition keepNotif prior
                stored' = Map.filterWithKey (\(w, _) _ -> w /= wid) stored
                seen' = foldr (Set.delete . (.notificationRecordMessageUuid)) seen dropped
                QueueName internal = internalQueueName
                row' =
                  row
                    { workflowRecordStatus = Enqueued,
                      workflowRecordQueueName = Just (fromMaybe internal sysOpts.sysRewindQueueName),
                      workflowRecordQueuePartitionKey = sysOpts.sysRewindQueuePartitionKey,
                      workflowRecordApplicationVersion = sysOpts.sysRewindApplicationVersion <|> row.workflowRecordApplicationVersion,
                      workflowRecordRecoveryAttempts = 0,
                      workflowRecordOwnerXid = Nothing,
                      workflowRecordDeadline = Nothing,
                      workflowRecordDeduplicationId = Nothing,
                      workflowRecordStartedAt = Nothing,
                      workflowRecordCompletedAt = Nothing,
                      workflowRecordOutput = Nothing,
                      workflowRecordError = Nothing,
                      workflowRecordUpdatedAt = now
                    }
            writeTVar db.memEvents events'
            writeTVar db.memEventHistory history'
            writeTVar db.memStreams streams'
            writeTVar db.memSteps steps'
            writeTVar db.memMessages stored'
            writeTVar db.memMessageIds seen'
            writeTVar db.memNotifications (Map.insert wid kept notifications)
            writeTVar db.memRows (Map.insert wid row' rows)
            let finalSteps = case caller of
                  Just (callerWid, callerStep) ->
                    Map.insert (widTextOf callerWid, callerStep) (memCallerEntry callerWid callerStep now (Right ())) steps'
                  Nothing -> steps'
            writeTVar db.memSteps finalSteps
            pure (Right ())
  where
    -- | Settles a pre-write refusal through the should-record gate, as
    -- live does: recordable failures commit a failure step for the
    -- caller, anything else returns unrecorded.
    refuseWith steps now err = case caller of
      Just (callerWid, callerStep) | shouldRecord err -> do
        writeTVar db.memSteps (Map.insert (widTextOf callerWid, callerStep) (memCallerEntry callerWid callerStep now (Left err)) steps)
        pure (Left err)
      _ -> pure (Left err)
    -- | The caller's step row: the unit output on success, the structured
    -- error on a recordable refusal.
    memCallerEntry callerWid callerStep now outcome =
      StepRecord
        { stepRecordWorkflowId = callerWid,
          stepRecordStepId = callerStep,
          stepRecordStepName = rewindStepName,
          stepRecordOutput = either (const Nothing) (const (Just "null")) outcome,
          stepRecordError = either (Just . memErrorJson) (const Nothing) outcome,
          stepRecordChildWorkflowId = Nothing,
          stepRecordSerialization = Just portableJsonSerialization,
          stepRecordStartedAt = Just now,
          stepRecordCompletedAt = Just now
        }
    memErrorJson :: Error -> Text
    memErrorJson = encodePortableError

memQueueRecord :: NewQueue -> QueueRecord
memQueueRecord queue =
  QueueRecord
    { queueRecordName = queue.newQueueName,
      queueRecordConcurrency = queue.newQueueConcurrency,
      queueRecordWorkerConcurrency = queue.newQueueWorkerConcurrency,
      queueRecordRateLimit = queue.newQueueRateLimit,
      queueRecordPriorityEnabled = queue.newQueuePriorityEnabled,
      queueRecordPartitionQueue = queue.newQueuePartitionQueue,
      queueRecordPartitionConcurrency = queue.newQueuePartitionConcurrency,
      queueRecordPartitionWorkerConcurrency = queue.newQueuePartitionWorkerConcurrency,
      queueRecordPartitionRateLimit = queue.newQueuePartitionRateLimit,
      queueRecordPollingInterval = queue.newQueuePollingInterval,
      queueRecordApplicationName = queue.newQueueApplicationName
    }

memApplyQueueUpdate :: QueueUpdate -> QueueRecord -> QueueRecord
memApplyQueueUpdate update record =
  record
    { queueRecordConcurrency = fromMaybe record.queueRecordConcurrency (changeSet update.queueUpdateConcurrency),
      queueRecordWorkerConcurrency = fromMaybe record.queueRecordWorkerConcurrency (changeSet update.queueUpdateWorkerConcurrency),
      queueRecordRateLimit = fromMaybe record.queueRecordRateLimit (changeSet update.queueUpdateRateLimit),
      queueRecordPriorityEnabled = fromMaybe record.queueRecordPriorityEnabled (changeSet update.queueUpdatePriorityEnabled),
      queueRecordPartitionQueue = fromMaybe record.queueRecordPartitionQueue (changeSet update.queueUpdatePartitionQueue),
      queueRecordPartitionConcurrency = fromMaybe record.queueRecordPartitionConcurrency (changeSet update.queueUpdatePartitionConcurrency),
      queueRecordPartitionWorkerConcurrency = fromMaybe record.queueRecordPartitionWorkerConcurrency (changeSet update.queueUpdatePartitionWorkerConcurrency),
      queueRecordPartitionRateLimit = fromMaybe record.queueRecordPartitionRateLimit (changeSet update.queueUpdatePartitionRateLimit),
      queueRecordPollingInterval = fromMaybe record.queueRecordPollingInterval (changeSet update.queueUpdatePollingInterval)
    }

memScheduleRecord :: NewSchedule -> Timestamp -> ScheduleRecord
memScheduleRecord new _now =
  ScheduleRecord
    { scheduleRecordId = fromMaybe new.newScheduleName new.newScheduleId,
      scheduleRecordName = new.newScheduleName,
      scheduleRecordWorkflowName = new.newScheduleWorkflowName,
      scheduleRecordWorkflowClassName = new.newScheduleWorkflowClassName,
      scheduleRecordExpression = new.newScheduleExpression,
      scheduleRecordStatus = new.newScheduleStatus,
      scheduleRecordContext = new.newScheduleContext,
      scheduleRecordLastFiredAt = new.newScheduleLastFiredAt,
      scheduleRecordAutomaticBackfill = new.newScheduleAutomaticBackfill,
      scheduleRecordCronTimezone = new.newScheduleCronTimezone,
      scheduleRecordQueueName = new.newScheduleQueueName,
      scheduleRecordApplicationName = new.newScheduleApplicationName
    }

memApplyScheduleUpdate :: ScheduleUpdate -> ScheduleRecord -> ScheduleRecord
memApplyScheduleUpdate update record =
  record
    { scheduleRecordExpression = fromMaybe record.scheduleRecordExpression (changeSet update.scheduleUpdateExpression),
      scheduleRecordContext = fromMaybe record.scheduleRecordContext (changeSet update.scheduleUpdateContext),
      scheduleRecordAutomaticBackfill = fromMaybe record.scheduleRecordAutomaticBackfill (changeSet update.scheduleUpdateAutomaticBackfill),
      scheduleRecordCronTimezone = fromMaybe record.scheduleRecordCronTimezone (changeSet update.scheduleUpdateCronTimezone),
      scheduleRecordQueueName = fromMaybe record.scheduleRecordQueueName (changeSet update.scheduleUpdateQueueName)
    }

-- | Flip one claimed row to PENDING with the executor and version stamped,
-- bump its recovery attempt, and arm its deadline from its timeout the
-- first time, as the SQL claim does.
memClaimRow :: Text -> Text -> Timestamp -> Text -> Text -> Map Text WorkflowRecord -> Map Text WorkflowRecord
memClaimRow executorId applicationVersion now ownerToken widText = Map.adjust claim widText
  where
    claim row =
      row
        { workflowRecordStatus = Pending,
          workflowRecordExecutorId = Just executorId,
          workflowRecordApplicationVersion = Just applicationVersion,
          workflowRecordOwnerXid = Just ownerToken,
          workflowRecordStartedAt = Just now,
          workflowRecordUpdatedAt = now,
          workflowRecordRecoveryAttempts = row.workflowRecordRecoveryAttempts + 1,
          workflowRecordDeadline = case (row.workflowRecordTimeout, row.workflowRecordDeadline) of
            (Just budget, Nothing) -> addTimeout now budget
            _ -> row.workflowRecordDeadline
        }
