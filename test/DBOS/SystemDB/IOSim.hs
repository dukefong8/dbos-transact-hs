{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | The port's own sim backend (P7.7 seed, no Rust counterpart). Every
-- method answers with deterministic canned data, keyed on the arguments the
-- mirror tests pass, so the whole 'SystemDB' seam can be exercised under
-- @IOSim@ without a database. The mocks are a test seam, not an in-memory
-- database: they exist to prove the wiring and to mirror the backend test
-- names; the live semantics stay in 'DBOS.SystemDB.PostgresTest'.
module DBOS.SystemDB.IOSim
  ( MockSystemDB (..),
    MemSystemDB (..),
    newMemDB,
    newMemDBWithApplication,
    memSetApplication,
    memConnectionOn,
    memLaunchOn,
    memDBOSOn,
    simConnectionWith,
    simInstance,
    simLaunchWith,
    simDBOSWith,
    simIdentity,
    simGeneratedId,
    simEntropy,
  )
where

import DBOS.Prelude
import Control.Concurrent.Class.MonadSTM.Strict (STM, StrictTVar, atomically, modifyTVar, newTVarIO, readTVar, retry, writeTVar)
import Control.Monad.IOSim (IOSim)
import Data.List (nub, sort, sortOn)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Set qualified as Set
import Data.Text (Text)
import Text.Read (readMaybe)
import Data.Text qualified as Text
import Data.Word (Word32)
import DBOS.SystemDB
  ( Applications (..),
    AwaitedOutcome (..),
    Change (..),
    Debounce (..),
    DebounceRequest (..),
    EncodedValue (..),
    Error (..),
    EventRecord (..),
    Fork (..),
    ForkOptions (..),
    ForkPoint (..),
    GetEventCaller (..),
    InitWorkflowCaller (..),
    MessageUUID (..),
    NewQueue (..),
    NewSchedule (..),
    NewWorkflow (..),
    NotificationRecord (..),
    OnExistingQueue (..),
    Outcome (..),
    OutcomeWrite (..),
    QueueRecord (..),
    QueueUpdate (..),
    ScheduleFilter (..),
    ScheduleRecord (..),
    ScheduleStatus (..),
    ScheduleUpdate (..),
    SendMessage (..),
    SerializedWorkflowValue (..),
    StepRecord (..),
    StepTiming (..),
    StreamRead (..),
    StreamRecord (..),
    SystemDB (..),
    Timestamp,
    Topic (..),
    VersionInfo (..),
    WorkflowDelay (..),
    WorkflowFilter (..),
    WorkflowId (..),
    WorkflowInitResult (..),
    WorkflowRecord (..),
    WorkflowStatus (..),
    addTimeout,
    changeSet,
    dequeueSweepCap,
    durationAsMillis,
    getEventStepName,
    invalidInput,
    getResultStepName,
    initialStatus,
    isQueueUpdateEmpty,
    isScheduleUpdateEmpty,
    newWorkflow,
    nullTopicSentinel,
    recvStepName,
    secondsDuration,
    sendBulkStepName,
    sleepStepName,
    sendStepName,
    setEventStepName,
    timestampFromEpochMs,
    timestampNow,
    timestampToEpochMs,
    zeroRowCounts,
  )
import DBOS.Transact
  ( DBOS,
    Executor,
    Identity (..),
    Serializer (..),
    SomeTracer (..),
    configNew,
    launchExecutor,
    launchOn,
    newDBOS,
  )
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
  initWorkflow = \cases _ new _ _ _ -> pure (Right (mockInitResult new))
  getWorkflow = \cases
    _ (WorkflowId "missing") -> pure (Right Nothing)
    _ (WorkflowId wid) -> pure (Right (Just (mockWorkflow (WorkflowId wid))))
  listWorkflows = \cases _ filter _ -> pure (Right (map (mockWorkflow . WorkflowId) filter.workflowFilterWorkflowIds))
  getWorkflowChildren = \cases _ _ -> pure (Right [mockChildId])
  recordWorkflowOutcome = \cases _ _ _ -> pure (Right Recorded)
  awaitWorkflowResult = \cases _ _ _ _ -> pure (Right (AwaitedSucceeded (Just mockOutput) (Just mockSerialization)))
  awaitFirstWorkflowId = \cases
    _ [] _ -> pure (Right (WorkflowId "mock"))
    _ (first : _) _ -> pure (Right first)
  awaitWorkflowIds = \cases _ _ _ -> pure (Right ())
  setWorkflowDelay = \cases _ _ _ _ -> pure (Right ())
  clearQueueAssignment = \cases _ _ -> pure (Right True)
  updateWorkflowAttributes = \cases _ _ _ _ -> pure (Right ())
  reenqueueForRecovery = \cases _ _ _ _ -> pure (Right [])
  transitionDelayedWorkflows = \cases _ -> pure (Right 0)
  cancelWorkflows = \cases _ ids _ _ -> pure (Right (filter (/= WorkflowId "never-existed") ids))
  resumeWorkflows = \cases
    _ ids _ _ | WorkflowId "never-existed" `elem` ids -> pure (Left (NonExistentWorkflow {workflowIds = map (\(WorkflowId wid) -> wid) ids}))
    _ ids _ _ -> pure (Right ids)
  deleteWorkflows = \cases _ ids _ _ -> pure (Right (fromIntegral (length ids)))
  forkWorkflows = \cases _ forks _ _ -> pure (Right (map (\fork -> WorkflowId fork.forkSourceId) forks))
  forkFrom = \cases _ ids _ _ _ -> pure (Right ids)
  sendMessage = \cases _ _ _ _ _ -> pure (Right ())
  sendMessages = \cases _ _ _ _ _ -> pure (Right ())
  recv = \cases _ _ _ _ _ _ -> pure (Right (Just (EncodedValue mockMessageBody (Just mockSerialization))))
  writeStream = \cases _ _ _ _ _ _ _ -> pure (Right ())
  closeStream = \cases _ _ _ _ -> pure (Right ())
  close = \cases _ -> pure ()
  checkStep = \cases _ _ _ _ -> pure (Right Nothing)
  recordStep = \cases _ _ _ _ _ _ _ -> pure (Right ())
  listSteps = \cases _ wid _ _ _ _ -> pure (Right [mockStep wid 0 "mock-step"])
  recordSleep = \cases _ _ _ _ -> pure (Right mockTimestamp)
  setEvent = \cases _ _ _ _ _ _ -> pure (Right ())
  getEvent = \cases _ _ _ _ _ -> pure (Right (Just (EncodedValue mockEventBody (Just mockSerialization))))
  getAllNotifications = \cases _ _ -> pure (Right [mockNotification])
  getAllEvents = \cases _ _ -> pure (Right [mockEvent])
  readStreamValue = \cases _ _ _ _ -> pure (Right (StreamRead Pending (Just (EncodedValue mockStreamBody (Just mockSerialization)))))
  getAllStreamEntries = \cases _ _ -> pure (Right [mockStreamRecord])
  createApplicationVersion = \cases _ _ _ -> pure (Right ())
  listApplicationVersions = \cases _ -> pure (Right [mockVersion])
  getLatestApplicationVersion = \cases _ _ -> pure (Right (Just mockVersion))
  updateApplicationVersionTimestamp = \cases _ _ _ _ -> pure (Right ())
  upsertQueue = \cases _ _ _ -> pure (Right True)
  startQueuedWorkflows = \cases _ _ _ _ _ _ _ -> pure (Right [mockQueuedId])
  getQueuePartitions = \cases _ _ -> pure (Right [mockPartition])
  startQueuedPartitionedWorkflows = \cases _ _ _ _ _ -> pure (Right [mockPartitionedId])
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
  recordChildWorkflow = \cases _ _ _ _ _ _ -> pure (Right ())
  recordChildResult = \cases _ _ _ _ _ _ _ -> pure (Right ())

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
    RustSerde
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
    -- | Parked messages by topic (the null-topic sentinel when absent);
    -- 'recv' blocks on this TVar, so a 'sendMessage' genuinely wakes a
    -- parked reader.
    memMessages :: StrictTVar (IOSim s) (Map Text [EncodedValue]),
    -- | Delivered notifications by destination, for
    -- 'getAllNotifications'.
    memNotifications :: StrictTVar (IOSim s) (Map Text [NotificationRecord]),
    -- | Published events by (workflow, key); readers block on this TVar.
    memEvents :: StrictTVar (IOSim s) (Map (Text, Text) EncodedValue),
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
    memApplicationName :: StrictTVar (IOSim s) (Maybe Text)
  }

-- | Fresh simulated data: empty rows, steps, dedup holds, queues,
-- messages, events, schedules, versions, and a fresh instance counter.
newMemDB :: IOSim s (MemSystemDB s)
newMemDB = newMemDBWithApplication Nothing

-- | Fresh simulated data for a listener of the given application.
newMemDBWithApplication :: Maybe Text -> IOSim s (MemSystemDB s)
newMemDBWithApplication application =
  MemSystemDB
    <$> newTVarIO Map.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO Map.empty
    <*> newTVarIO []
    <*> newTVarIO 0
    <*> newTVarIO application

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
    RustSerde
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
memLaunchOn mem tracer dbos = do
  conn <- memConnectionOn mem tracer
  executor <- launchOn dbos conn simIdentity
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
memFreshRow new caller owner =
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
      workflowRecordOwnerXid = Just owner,
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
      workflowRecordDebounceDeadline = new.newWorkflowDebounceDeadline,
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
memInitResult :: WorkflowRecord -> WorkflowInitResult
memInitResult row =
  WorkflowInitResult
    { initResultStatus = row.workflowRecordStatus,
      initResultRecoveryAttempts = row.workflowRecordRecoveryAttempts,
      initResultDeadline = row.workflowRecordDeadline,
      initResultSerialization = row.workflowRecordSerialization,
      initResultShouldExecute = False
    }

instance SystemDB (MemSystemDB s) (IOSim s) where
  initWorkflow db new _maxAttempts _submission caller = atomically $ do
    rows <- readTVar db.memRows
    case Map.lookup new.newWorkflowId rows of
      Just row -> pure (Right (memInitResult row))
      Nothing -> case (new.newWorkflowQueueName, new.newWorkflowDeduplicationId) of
        (Just queue, Just key) -> do
          held <- readTVar db.memDedup
          case Map.lookup (queue, key) held of
            Just _ ->
              pure (Left (QueueDeduplicated {workflowId = new.newWorkflowId, queueName = queue, deduplicationId = key}))
            Nothing -> do
              writeTVar db.memDedup (Map.insert (queue, key) new.newWorkflowId held)
              insertFresh rows
        _ -> insertFresh rows
    where
      insertFresh rows = do
        owner <- simOwnerText db
        let row = memFreshRow new caller owner
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
        pure
          ( Right
              ( WorkflowInitResult
                  { initResultStatus = initialStatus new,
                    initResultRecoveryAttempts = 0,
                    initResultDeadline = new.newWorkflowDeadline,
                    initResultSerialization = new.newWorkflowSerialization,
                    initResultShouldExecute = True
                  }
              )
          )
  getWorkflow db (WorkflowId wid) = Right . Map.lookup wid <$> readTVarIO db.memRows
  listWorkflows db filters _ = do
    rows <- readTVarIO db.memRows
    pure
      ( Right
          ( case filters.workflowFilterWorkflowIds of
              [] -> Map.elems rows
              ids -> [row | wid <- ids, Just row <- [Map.lookup wid rows]]
          )
      )
  getWorkflowChildren db (WorkflowId wid) = do
    rows <- readTVarIO db.memRows
    pure (Right [row.workflowRecordId | row <- Map.elems rows, row.workflowRecordParentWorkflowId == Just (WorkflowId wid)])
  recordWorkflowOutcome db (WorkflowId wid) outcome = atomically $ do
    rows <- readTVar db.memRows
    case Map.lookup wid rows of
      Nothing -> pure (Right Recorded)
      Just row -> do
        writeTVar db.memRows (Map.insert wid (memApplyOutcome outcome row) rows)
        pure (Right Recorded)
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
    now <- timestampNow
    let releaseAt = case delay of
          DelayFor duration -> addTimeout now duration
          DelayUntil instant -> Just instant
    atomically (modifyTVar db.memRows (Map.adjust (\row -> row {workflowRecordDelayUntil = releaseAt}) wid))
    pure (Right ())
  updateWorkflowAttributes db (WorkflowId wid) attributes _ =
    atomically (modifyTVar db.memRows (Map.adjust (\row -> row {workflowRecordAttributes = attributes}) wid))
      >> pure (Right ())
  cancelWorkflows db ids _ _ = atomically $ do
    rows <- readTVar db.memRows
    let (moved, rows') = Map.mapAccumWithKey move [] rows
        move acc wid row
          | WorkflowId wid `elem` ids && row.workflowRecordStatus `elem` [Pending, Enqueued, Delayed] =
              (WorkflowId wid : acc, row {workflowRecordStatus = Cancelled})
          | otherwise = (acc, row)
    writeTVar db.memRows rows'
    pure (Right moved)
  resumeWorkflows db ids _ _ = do
    rows <- readTVarIO db.memRows
    -- No runner exists behind the sim to re-enqueue onto, so resume echoes
    -- the ids (waits read whatever the rows recorded); only the unknown
    -- id is refused, as live.
    case [wid | WorkflowId wid <- ids, Map.notMember wid rows] of
      _ : _ -> pure (Left (NonExistentWorkflow {workflowIds = [wid | WorkflowId wid <- ids]}))
      [] -> pure (Right ids)
  deleteWorkflows db ids _ _ = atomically $ do
    rows <- readTVar db.memRows
    let gone = length (filter (`Map.member` rows) [wid | WorkflowId wid <- ids])
    writeTVar db.memRows (foldr Map.delete rows [wid | WorkflowId wid <- ids])
    modifyTVar db.memSteps (Map.filterWithKey (\(wid, _) _ -> wid `notElem` [w | WorkflowId w <- ids]))
    pure (Right (fromIntegral gone))
  forkWorkflows db forks options _ = memFork db [(fork.forkSourceId, fork.forkForkedId, fork.forkStartStep) | fork <- forks] options
  forkFrom db ids point options _ = do
    steps <- readTVarIO db.memSteps
    memFork db [(source, Nothing, memStartStep point source steps) | WorkflowId source <- ids] options
    where
      memStartStep (ForkStep step) _ _ = step
      memStartStep ForkLastFailure source steps = memCopyBelow (memMaxStep source steps) steps
      memStartStep ForkLastStep source steps = memCopyBelow (memMaxStep source steps) steps
      memStartStep (ForkStepNamed name) source steps = memCopyBelow (memNamedStep source name steps) steps
      memMaxStep source steps = maximum (-1 : [sid | ((wid, sid), _) <- Map.toList steps, wid == source])
      memNamedStep source name steps = maximum (-1 : [sid | ((wid, sid), record) <- Map.toList steps, wid == source, record.stepRecordStepName == name])
      memCopyBelow highest _ = highest + 1
  checkStep db (WorkflowId wid) stepId _name = Right . Map.lookup (wid, stepId) <$> readTVarIO db.memSteps
  recordStep db (WorkflowId wid) stepId name outcome serialization timing = atomically $ do
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
    writeTVar db.memSteps (Map.insert (wid, stepId) entry steps)
    pure (Right ())
    where
      memPriorOutput (Just record) = record.stepRecordOutput
      memPriorOutput Nothing = Nothing
  listSteps db (WorkflowId wid) _ _ _ _ = do
    steps <- readTVarIO db.memSteps
    pure (Right [record | ((w, _), record) <- Map.toList steps, w == wid])
  recordChildWorkflow db parent child stepId name _time = atomically $ do
    steps <- readTVar db.memSteps
    let WorkflowId parentText = parent
        entry = case Map.lookup (parentText, stepId) steps of
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
    writeTVar db.memSteps (Map.insert (parentText, stepId) entry steps)
    pure (Right ())
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
    pure (Right [WorkflowId widText | (widText, _) <- candidates])
  -- Delayed rows whose deadline has passed are enqueued; a debounced
  -- holder releases its key, as the SQL transition does.
  transitionDelayedWorkflows db = do
    now <- timestampNow
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
      pure (Right (fromIntegral moved))
  clearQueueAssignment _ = clearQueueAssignment MockSystemDB
  sendMessage db message serialization caller sendToForks = memSend db sendStepName [message] serialization caller sendToForks
  sendMessages db messages serialization caller sendToForks = memSend db sendBulkStepName messages serialization caller sendToForks
  -- A parked reader blocks on the messages TVar, so a send genuinely
  -- wakes it; the engine's own duration bounds the wait, and a timeout
  -- records the step with no output so a replay adopts the absence.
  recv db wid stepId _timeoutStepId topic duration = do
    let WorkflowId widText = wid
        storedTopic = fromMaybe nullTopicSentinel topic
    replayed <- atomically (Map.lookup (widText, stepId) <$> readTVar db.memSteps)
    case replayed of
      Just record -> pure (Right (memEncodedFromStep record))
      Nothing -> do
        waited <- memTimeout (fromInteger (durationAsMillis duration * 1000)) (atomically (memTakeMessage db storedTopic))
        case waited of
          Just value -> do
            memRecordRecvStep db wid stepId recvStepName (Just value)
            pure (Right (Just value))
          Nothing -> do
            memRecordRecvStep db wid stepId recvStepName Nothing
            pure (Right Nothing)
  writeStream _ = writeStream MockSystemDB
  closeStream _ = closeStream MockSystemDB
  close _ = close MockSystemDB
  recordSleep db wid stepId duration = do
    now <- timestampNow
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
            case recorded of
              Left err -> pure (Left err)
              Right () -> pure (Right wakeAt)
  setEvent db wid stepId key value serialization = do
    now <- timestampNow
    atomically $ do
      events <- readTVar db.memEvents
      writeTVar db.memEvents (Map.insert (widTextOf wid, key) (EncodedValue value serialization) events)
      memInsertStep db wid stepId setEventStepName (Just value) serialization now
      pure (Right ())
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
        waited <- memTimeout (fromInteger (durationAsMillis duration * 1000)) (atomically (memWaitEvent db widText key))
        case caller of
          Nothing -> pure (Right waited)
          Just c -> do
            now <- timestampNow
            atomically (memInsertStep db c.getEventCallerWorkflowId c.getEventCallerStepId getEventStepName ((.encodedValue) <$> waited) (waited >>= (.encodedSerialization)) now)
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
  readStreamValue _ = readStreamValue MockSystemDB
  getAllStreamEntries _ = getAllStreamEntries MockSystemDB
  createApplicationVersion db versionName applicationName = do
    now <- timestampNow
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
    let existing = Map.lookup queue.newQueueName queues
    case existing of
      Just _ | onExisting == LeaveExisting -> pure (Right False)
      _ -> do
        writeTVar db.memQueues (Map.insert queue.newQueueName (memQueueRecord queue) queues)
        pure (Right (isNothing existing))
  -- The claim: worker/concurrency budgets from the queue's limits (rate
  -- limits are not modelled — Mem keeps no dequeue history), candidates
  -- in priority/created order, flipped to PENDING with the executor and
  -- version stamped, exactly the SQL claim's columns.
  startQueuedWorkflows db queue executorId applicationVersion partitionKey localRunning partitionLocalRunning = do
    now <- timestampNow
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
          writeTVar db.memRows (foldr (memClaimRow executorId applicationVersion now) rows claimed)
          pure (Right (map WorkflowId claimed))
  getQueuePartitions db queueName = do
    rows <- readTVarIO db.memRows
    application <- readTVarIO db.memApplicationName
    pure
      ( Right (sort (Set.toList (Set.fromList [key | row <- Map.elems rows, visibleToApplication application row, row.workflowRecordQueueName == Just queueName, row.workflowRecordStatus == Enqueued, Just key <- [row.workflowRecordQueuePartitionKey]])))
      )
  -- One head per partition with no pending work, in key order.
  startQueuedPartitionedWorkflows db queue executorId applicationVersion maxTasks = do
    now <- timestampNow
    atomically $ do
      rows <- readTVar db.memRows
      application <- readTVar db.memApplicationName
      let queueName = queue.queueRecordName
          cap = fromIntegral dequeueSweepCap
          limit = maybe cap (min cap) (fromIntegral <$> maxTasks)
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
                    [ sortOn (\(_, row) -> (row.workflowRecordPriority, row.workflowRecordCreatedAt))
                        [ (widText, row)
                          | (widText, row) <- Map.toList rows,
                            row.workflowRecordQueueName == Just queueName,
                            visibleToApplication application row,
                            row.workflowRecordQueuePartitionKey == Just key
                        ]
                    ]
              ]
      writeTVar db.memRows (foldr (memClaimRow executorId applicationVersion now) rows heads)
      pure (Right (map WorkflowId heads))
  getQueue db name = do
    queues <- readTVarIO db.memQueues
    pure (Right (Map.lookup name queues))
  listQueues db applications = do
    queues <- readTVarIO db.memQueues
    let visible row = case applications of
          Unset -> True
          Named [] -> True
          Named names -> maybe True (`elem` names) row.queueRecordApplicationName
          Any -> True
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
  debounceDelayedWorkflow _ = debounceDelayedWorkflow MockSystemDB
  deleteQueue db name = atomically (modifyTVar db.memQueues (Map.delete name)) >> pure (Right ())
  createSchedule db new _caller = do
    now <- timestampNow
    atomically $ do
      schedules <- readTVar db.memSchedules
      if Map.member new.newScheduleName schedules
        then pure (Left (Malformed ("schedule " <> new.newScheduleName <> " already exists")))
        else do
          writeTVar db.memSchedules (Map.insert new.newScheduleName (memScheduleRecord new now) schedules)
          pure (Right ())
  upsertSchedule db new _caller = do
    now <- timestampNow
    atomically $ do
      schedules <- readTVar db.memSchedules
      writeTVar db.memSchedules (Map.insert new.newScheduleName (memScheduleRecord new now) schedules)
      pure (Right ())
  applySchedules db schedules = do
    now <- timestampNow
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
  recordChildResult db parent stepId child outcome serialization timing = atomically $ do
    steps <- readTVar db.memSteps
    let WorkflowId parentText = parent
        prior = Map.lookup (parentText, stepId) steps
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
    writeTVar db.memSteps (Map.insert (parentText, stepId) entry steps)
    pure (Right ())

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
      let forkedText = fromMaybe (source <> "-fork") chosen
          status = case options'.forkOptionsQueueName of
            Just _ -> Enqueued
            Nothing -> Pending
          row = case Map.lookup source rows of
            Just origin ->
              origin
                { workflowRecordId = WorkflowId forkedText,
                  workflowRecordStatus = status,
                  workflowRecordOutput = Nothing,
                  workflowRecordError = Nothing,
                  workflowRecordQueueName = options'.forkOptionsQueueName,
                  workflowRecordForkedFrom = Just (WorkflowId source),
                  workflowRecordWasForkedFrom = True,
                  workflowRecordRecoveryAttempts = 0
                }
            Nothing ->
              (memFreshRow (newWorkflow forkedText) Nothing owner')
                { workflowRecordStatus = status,
                  workflowRecordQueueName = options'.forkOptionsQueueName,
                  workflowRecordForkedFrom = Just (WorkflowId source),
                  workflowRecordWasForkedFrom = True
                }
          copied =
            [ ((forkedText, sid), record {stepRecordWorkflowId = WorkflowId forkedText})
              | ((wid, sid), record) <- Map.toList steps,
                wid == source,
                sid < startStep
            ]
          steps' = foldr (uncurry Map.insert) steps copied
       in (WorkflowId forkedText : ids, Map.insert forkedText row rows, steps')

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
  now <- timestampNow
  atomically (memInsertStep db wid stepId name ((.encodedValue) <$> value) (value >>= (.encodedSerialization)) now)

-- | Take the first parked message on a topic, blocking until one lands.
memTakeMessage :: MemSystemDB s -> Text -> STM (IOSim s) EncodedValue
memTakeMessage db topic = do
  stored <- readTVar db.memMessages
  case Map.lookup topic stored of
    Just (value : _) -> do
      writeTVar db.memMessages (Map.adjust (drop 1) topic stored)
      pure value
    _ -> retry

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

-- | Deliver messages to their topics and destinations, appending a
-- notification per delivery so 'getAllNotifications' sees them, and record
-- the caller's step. A send writes the messages TVar, which is what wakes
-- a parked 'recv'.
memSend :: MemSystemDB s -> Text -> [SendMessage] -> Maybe Text -> Maybe (WorkflowId, Int) -> Bool -> IOSim s (Either Error ())
memSend db stepName messages serialization caller sendToForks = do
  now <- timestampNow
  atomically $ do
    rows <- readTVar db.memRows
    stored <- readTVar db.memMessages
    notifications <- readTVar db.memNotifications
    steps <- readTVar db.memSteps
    -- A replay must not send again: like the live insert, a caller step
    -- that already stands means this batch went out before.
    case caller of
      Just (callerWid, callerStep)
        | Map.member (widTextOf callerWid, callerStep) steps -> pure (Right ())
      _ -> do
        let topicOf m = maybe nullTopicSentinel (\(Topic topic) -> topic) m.sendTopic
            bodyOf m = EncodedValue m.sendMessageBody.serializedText serialization
            -- Mirror the live fan-out: the destination itself plus every
            -- workflow forked from it, transitively, sorted and deduplicated.
            expand m =
              let destination = widTextOf m.sendDestinationId
               in destination : if sendToForks then sort (nub (memDescendants rows destination)) else []
            deliveries =
              [ (topicOf m, bodyOf m, m {sendDestinationId = WorkflowId recipient})
                | m <- messages,
                  recipient <- expand m
              ]
            stored' = foldr (\(topic, value, _) acc -> Map.insertWith (<>) topic [value] acc) stored deliveries
            notificationFor topic value =
              let MessageUUID uuid = MessageUUID (topic <> ":" <> Text.pack (show (length (fromMaybe [] (Map.lookup topic stored)) + 1)))
               in NotificationRecord
                    { notificationRecordMessageUuid = uuid,
                      notificationRecordTopic = Just topic,
                      notificationRecordMessage = value.encodedValue,
                      notificationRecordSerialization = value.encodedSerialization,
                      notificationRecordCreatedAt = now,
                      notificationRecordConsumed = False
                    }
            notifications' =
              foldr
                (\(topic, value, m) acc -> Map.insertWith (<>) (widTextOf m.sendDestinationId) [notificationFor topic value] acc)
                notifications
                deliveries
        writeTVar db.memMessages stored'
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
memClaimRow :: Text -> Text -> Timestamp -> Text -> Map Text WorkflowRecord -> Map Text WorkflowRecord
memClaimRow executorId applicationVersion now widText = Map.adjust claim widText
  where
    claim row =
      row
        { workflowRecordStatus = Pending,
          workflowRecordExecutorId = Just executorId,
          workflowRecordApplicationVersion = Just applicationVersion,
          workflowRecordStartedAt = Just now,
          workflowRecordUpdatedAt = now,
          workflowRecordRecoveryAttempts = row.workflowRecordRecoveryAttempts + 1,
          workflowRecordDeadline = case (row.workflowRecordTimeout, row.workflowRecordDeadline) of
            (Just budget, Nothing) -> addTimeout now budget
            _ -> row.workflowRecordDeadline
        }
