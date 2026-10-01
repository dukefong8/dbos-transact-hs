{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

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
    memConnectionOn,
    memLaunchOn,
    memDBOSOn,
    simConnectionWith,
    simInstance,
    simLaunchWith,
    simDBOSWith,
  )
where

import DBOS.Prelude
import Control.Concurrent.Class.MonadSTM.Strict (StrictTVar, atomically, modifyTVar, newTVarIO, readTVar, retry, writeTVar)
import Control.Monad.IOSim (IOSim)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Word (Word32)
import DBOS.SystemDB
  ( AwaitedOutcome (..),
    Debounce (..),
    EncodedValue (..),
    Error (..),
    EventRecord (..),
    Fork (..),
    ForkOptions (..),
    ForkPoint (..),
    InitWorkflowCaller (..),
    NewWorkflow (..),
    NotificationRecord (..),
    Outcome (..),
    OutcomeWrite (..),
    getResultStepName,
    QueueRecord (..),
    ScheduleRecord (..),
    ScheduleStatus (..),
    StepRecord (..),
    StepTiming (..),
    StreamRead (..),
    StreamRecord (..),
    SystemDB (..),
    Timestamp,
    VersionInfo (..),
    WorkflowDelay (..),
    WorkflowFilter (..),
    WorkflowId (..),
    WorkflowInitResult (..),
    WorkflowRecord (..),
    WorkflowStatus (..),
    addTimeout,
    initialStatus,
    newWorkflow,
    secondsDuration,
    timestampFromEpochMs,
    timestampNow,
    zeroRowCounts,
  )
import DBOS.Transact
  ( Connection,
    DBOS,
    Identity (..),
    Owner (..),
    Serializer (..),
    SomeSystemDB (..),
    SomeTracer (..),
    configNew,
    launchOn,
    newConnection,
    newDBOS,
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
  listWorkflowSteps = \cases _ wid _ _ _ _ -> pure (Right [mockStep wid 0 "mock-step"])
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
simLaunchWith :: SomeTracer (IOSim s) -> DBOS (IOSim s) -> IOSim s ()
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
    -- | Names each connection launched over this data, so two instances
    -- sharing one database are still two — the distinction the
    -- cross-instance refusal reads.
    memInstanceCounter :: StrictTVar (IOSim s) Int
  }

-- | Fresh simulated data: empty rows, steps, dedup holds, and a fresh
-- instance counter.
newMemDB :: IOSim s (MemSystemDB s)
newMemDB = MemSystemDB <$> newTVarIO Map.empty <*> newTVarIO Map.empty <*> newTVarIO Map.empty <*> newTVarIO 0

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

-- | Launch carrying the given tracer over simulated data.
memLaunchOn :: MemSystemDB s -> SomeTracer (IOSim s) -> DBOS (IOSim s) -> IOSim s ()
memLaunchOn mem tracer dbos = do
  conn <- memConnectionOn mem tracer
  launchOn dbos conn simIdentity

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
memFreshRow :: NewWorkflow -> Maybe InitWorkflowCaller -> WorkflowRecord
memFreshRow new caller =
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
      workflowRecordOwnerXid = Nothing,
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
        let row = memFreshRow new caller
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
  listWorkflowSteps db (WorkflowId wid) _ _ _ _ = do
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
  reenqueueForRecovery _ = reenqueueForRecovery MockSystemDB
  transitionDelayedWorkflows _ = transitionDelayedWorkflows MockSystemDB
  clearQueueAssignment _ = clearQueueAssignment MockSystemDB
  sendMessage _ = sendMessage MockSystemDB
  sendMessages _ = sendMessages MockSystemDB
  recv _ = recv MockSystemDB
  writeStream _ = writeStream MockSystemDB
  closeStream _ = closeStream MockSystemDB
  close _ = close MockSystemDB
  recordSleep _ = recordSleep MockSystemDB
  setEvent _ = setEvent MockSystemDB
  getEvent _ = getEvent MockSystemDB
  getAllNotifications _ = getAllNotifications MockSystemDB
  getAllEvents _ = getAllEvents MockSystemDB
  readStreamValue _ = readStreamValue MockSystemDB
  getAllStreamEntries _ = getAllStreamEntries MockSystemDB
  createApplicationVersion _ = createApplicationVersion MockSystemDB
  listApplicationVersions _ = listApplicationVersions MockSystemDB
  getLatestApplicationVersion _ = getLatestApplicationVersion MockSystemDB
  updateApplicationVersionTimestamp _ = updateApplicationVersionTimestamp MockSystemDB
  upsertQueue _ = upsertQueue MockSystemDB
  startQueuedWorkflows _ = startQueuedWorkflows MockSystemDB
  getQueuePartitions _ = getQueuePartitions MockSystemDB
  startQueuedPartitionedWorkflows _ = startQueuedPartitionedWorkflows MockSystemDB
  getQueue _ = getQueue MockSystemDB
  listQueues _ = listQueues MockSystemDB
  updateQueue _ = updateQueue MockSystemDB
  debounceDelayedWorkflow _ = debounceDelayedWorkflow MockSystemDB
  deleteQueue _ = deleteQueue MockSystemDB
  createSchedule _ = createSchedule MockSystemDB
  upsertSchedule _ = upsertSchedule MockSystemDB
  applySchedules _ = applySchedules MockSystemDB
  getSchedule _ = getSchedule MockSystemDB
  listSchedules _ = listSchedules MockSystemDB
  awaitFirstWorkflowId _ = awaitFirstWorkflowId MockSystemDB
  awaitWorkflowIds _ = awaitWorkflowIds MockSystemDB
  updateSchedule _ = updateSchedule MockSystemDB
  setScheduleStatus _ = setScheduleStatus MockSystemDB
  updateScheduleLastFiredAt _ = updateScheduleLastFiredAt MockSystemDB
  deleteSchedule _ = deleteSchedule MockSystemDB
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
  let (ids, rows', steps') = foldr (memForkOne options) ([], rows, steps) forks
  writeTVar db.memRows rows'
  writeTVar db.memSteps steps'
  pure (Right ids)
  where
    memForkOne options' (source, chosen, startStep) (ids, rows, steps) =
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
              (memFreshRow (newWorkflow forkedText) Nothing)
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
