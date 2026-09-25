{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The port's own sim backend (P7.7 seed, no Rust counterpart). Every
-- method answers with deterministic canned data, keyed on the arguments the
-- mirror tests pass, so the whole 'SystemDB' seam can be exercised under
-- @IOSim@ without a database. The mocks are a test seam, not an in-memory
-- database: they exist to prove the wiring and to mirror the backend test
-- names; the live semantics stay in 'DBOS.SystemDB.PostgresTest'.
module DBOS.SystemDB.IOSim
  ( IOSimSystemDB (..),
    simConnection,
    simExecutor,
    simInstance,
    simLaunch,
    simDBOS,
  )
where

import DBOS.Prelude
import Control.Concurrent.Class.MonadMVar.Strict (modifyMVar_, newMVar, readMVar)
import Control.Concurrent.Class.MonadSTM.Strict (StrictTVar, atomically, newTVarIO, readTVar, writeTVar)
import Control.Monad.IOSim (IOSim)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Word (Word32, Word64)
import DBOS.SystemDB (Error (..), SystemDB (..))
import DBOS.SystemDB.Types
  ( AwaitedOutcome (..),
    Debounce (..),
    EncodedValue (..),
    EventRecord (..),
    Fork (..),
    NewWorkflow (..),
    NotificationRecord (..),
    OutcomeWrite (..),
    QueueRecord (..),
    ScheduleRecord (..),
    ScheduleStatus (..),
    StepRecord (..),
    StreamRead (..),
    StreamRecord (..),
    VersionInfo (..),
    WorkflowFilter (..),
    WorkflowId (..),
    WorkflowInitResult (..),
    WorkflowRecord (..),
    WorkflowStatus (..),
    secondsDuration,
    zeroRowCounts,
  )
import DBOS.SystemDB.Types qualified as Types (Timestamp, timestampFromEpochMs)
import DBOS.Transact.Config (Serializer (..))
import DBOS.Transact.Connection (Connection (..), Owner (..), SomeSystemDB (..), newConnection)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Instance (DBOS (..), Executor (..))
import DBOS.Transact.Log (nullLogAction)
import DBOS.Transact.Registry (Snapshot, newRegistry, snapshotRegistry)
import DBOS.Transact.Workflow (newTasks)

-- | A marker backend: it owns no state, and every method answers with the
-- canned data the mirror tests expect.
data IOSimSystemDB = IOSimSystemDB
  deriving stock (Eq, Show)

instance SystemDB IOSimSystemDB (IOSim s) where
  initWorkflow = \cases _ new _ _ _ -> pure (Right (mockInitResult new))
  getWorkflow = \cases
    _ (WorkflowId "missing") -> pure (Right Nothing)
    _ (WorkflowId wid) -> pure (Right (Just (mockWorkflow (WorkflowId wid))))
  listWorkflows = \cases _ filter _ -> pure (Right (map (mockWorkflow . WorkflowId) filter.workflowFilterWorkflowIds))
  getWorkflowChildren = \cases _ _ -> pure (Right [WorkflowId "mock-child"])
  recordWorkflowOutcome = \cases _ _ _ -> pure (Right Recorded)
  awaitWorkflowResult = \cases _ _ _ _ -> pure (Right (AwaitedSucceeded (Just "mock-output") (Just "rust_serde")))
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
  recv = \cases _ _ _ _ _ _ -> pure (Right (Just (EncodedValue "mock-message" (Just "rust_serde"))))
  writeStream = \cases _ _ _ _ _ _ _ -> pure (Right ())
  closeStream = \cases _ _ _ _ -> pure (Right ())
  close = \cases _ -> pure ()
  checkStep = \cases _ _ _ _ -> pure (Right Nothing)
  recordStep = \cases _ _ _ _ _ _ _ -> pure (Right ())
  listWorkflowSteps = \cases _ wid _ _ _ _ -> pure (Right [mockStep wid 0 "mock-step"])
  recordSleep = \cases _ _ _ _ -> pure (Right mockTimestamp)
  setEvent = \cases _ _ _ _ _ _ -> pure (Right ())
  getEvent = \cases _ _ _ _ _ -> pure (Right (Just (EncodedValue "mock-event" (Just "rust_serde"))))
  getAllNotifications = \cases _ _ -> pure (Right [mockNotification])
  getAllEvents = \cases _ _ -> pure (Right [mockEvent])
  readStreamValue = \cases _ _ _ _ -> pure (Right (StreamRead Pending (Just (EncodedValue "mock-stream" (Just "rust_serde")))))
  getAllStreamEntries = \cases _ _ -> pure (Right [mockStreamRecord])
  createApplicationVersion = \cases _ _ _ -> pure (Right ())
  listApplicationVersions = \cases _ -> pure (Right [mockVersion])
  getLatestApplicationVersion = \cases _ _ -> pure (Right (Just mockVersion))
  updateApplicationVersionTimestamp = \cases _ _ _ _ -> pure (Right ())
  upsertQueue = \cases _ _ _ -> pure (Right True)
  startQueuedWorkflows = \cases _ _ _ _ _ _ _ -> pure (Right [WorkflowId "mock-queued"])
  getQueuePartitions = \cases _ _ -> pure (Right ["mock-partition"])
  startQueuedPartitionedWorkflows = \cases _ _ _ _ _ -> pure (Right [WorkflowId "mock-partitioned"])
  getQueue = \cases _ name -> pure (Right (Just (mockQueue name)))
  listQueues = \cases _ _ -> pure (Right [mockQueue "mock-queue"])
  updateQueue = \cases _ name _ _ -> pure (Right (mockQueue name))
  debounceDelayedWorkflow = \cases _ _ _ -> pure (Right (Debounced "mock-debounced"))
  getDeduplicationKeyHolder = \cases _ _ _ -> pure (Right (Just (WorkflowId "mock-holder")))
  deleteQueue = \cases _ _ -> pure (Right ())
  createSchedule = \cases _ _ _ -> pure (Right ())
  upsertSchedule = \cases _ _ _ -> pure (Right ())
  applySchedules = \cases _ _ -> pure (Right ())
  getSchedule = \cases _ name _ -> pure (Right (Just (mockSchedule name)))
  listSchedules = \cases _ _ _ -> pure (Right [mockSchedule "mock-schedule"])
  updateSchedule = \cases _ _ _ _ -> pure (Right ())
  setScheduleStatus = \cases _ _ _ _ -> pure (Right ())
  updateScheduleLastFiredAt = \cases _ _ _ -> pure (Right ())
  deleteSchedule = \cases _ _ _ -> pure (Right ())
  renameApplication = \cases _ _ _ _ -> pure (Right zeroRowCounts)
  recordChildWorkflow = \cases _ _ _ _ _ _ -> pure (Right ())
  recordChildResult = \cases _ _ _ _ _ _ _ -> pure (Right ())

-- * Canned data

mockTimestamp :: Types.Timestamp
mockTimestamp = Types.timestampFromEpochMs 1000

mockInitResult :: NewWorkflow -> WorkflowInitResult
mockInitResult new =
  WorkflowInitResult
    { initResultStatus = Pending,
      initResultRecoveryAttempts = 0,
      initResultDeadline = new.newWorkflowDeadline,
      initResultSerialization = new.newWorkflowSerialization,
      initResultShouldExecute = True
    }

mockWorkflow :: WorkflowId -> WorkflowRecord
mockWorkflow wid =
  WorkflowRecord
    { workflowRecordId = wid,
      workflowRecordStatus = Pending,
      workflowRecordName = Just "mock-workflow",
      workflowRecordClassName = Nothing,
      workflowRecordConfigName = Nothing,
      workflowRecordInput = Just "null",
      workflowRecordOutput = Nothing,
      workflowRecordError = Nothing,
      workflowRecordSerialization = Just "rust_serde",
      workflowRecordExecutorId = Just "mock-executor",
      workflowRecordApplicationVersion = Just "0.0.0",
      workflowRecordRecoveryAttempts = 0,
      workflowRecordQueueName = Just "mock-queue",
      workflowRecordCreatedAt = mockTimestamp,
      workflowRecordUpdatedAt = mockTimestamp,
      workflowRecordStartedAt = Nothing,
      workflowRecordCompletedAt = Nothing,
      workflowRecordForkedFrom = Nothing,
      workflowRecordParentWorkflowId = Nothing,
      workflowRecordWasForkedFrom = False,
      workflowRecordOwnerXid = Nothing,
      workflowRecordApplicationId = Nothing,
      workflowRecordAuthenticatedUser = Nothing,
      workflowRecordAuthenticatedRoles = [],
      workflowRecordAssumedRole = Nothing,
      workflowRecordRequest = Nothing,
      workflowRecordApplicationName = Just "mock-app",
      workflowRecordDeduplicationId = Nothing,
      workflowRecordPriority = 0,
      workflowRecordQueuePartitionKey = Nothing,
      workflowRecordRateLimited = False,
      workflowRecordScheduleName = Nothing,
      workflowRecordTimeout = Nothing,
      workflowRecordDeadline = Nothing,
      workflowRecordDelayUntil = Nothing,
      workflowRecordDebounceDeadline = Nothing,
      workflowRecordIsDebounced = False,
      workflowRecordAttributes = Nothing
    }

mockStep :: WorkflowId -> Int -> Text -> StepRecord
mockStep wid stepId' name =
  StepRecord
    { stepRecordWorkflowId = wid,
      stepRecordStepId = stepId',
      stepRecordStepName = name,
      stepRecordOutput = Just "\"mock\"",
      stepRecordError = Nothing,
      stepRecordChildWorkflowId = Nothing,
      stepRecordSerialization = Just "rust_serde",
      stepRecordStartedAt = Just mockTimestamp,
      stepRecordCompletedAt = Just mockTimestamp
    }

mockQueue :: Text -> QueueRecord
mockQueue name =
  QueueRecord
    { queueRecordName = name,
      queueRecordConcurrency = Nothing,
      queueRecordWorkerConcurrency = Nothing,
      queueRecordRateLimit = Nothing,
      queueRecordPriorityEnabled = False,
      queueRecordPartitionQueue = False,
      queueRecordPartitionConcurrency = Nothing,
      queueRecordPartitionWorkerConcurrency = Nothing,
      queueRecordPartitionRateLimit = Nothing,
      queueRecordPollingInterval = secondsDuration 1,
      queueRecordApplicationName = Just "mock-app"
    }

mockSchedule :: Text -> ScheduleRecord
mockSchedule name =
  ScheduleRecord
    { scheduleRecordId = "mock-schedule-id",
      scheduleRecordName = name,
      scheduleRecordWorkflowName = "mock-workflow",
      scheduleRecordWorkflowClassName = Nothing,
      scheduleRecordExpression = "* * * * *",
      scheduleRecordStatus = Active,
      scheduleRecordContext = "{}",
      scheduleRecordLastFiredAt = Nothing,
      scheduleRecordAutomaticBackfill = False,
      scheduleRecordCronTimezone = Nothing,
      scheduleRecordQueueName = Nothing,
      scheduleRecordApplicationName = Just "mock-app"
    }

mockVersion :: VersionInfo
mockVersion =
  VersionInfo
    { versionInfoApplicationName = Just "mock-app",
      versionInfoId = "mock-version-id",
      versionInfoName = "0.0.0",
      versionInfoCreatedAt = mockTimestamp,
      versionInfoTimestamp = mockTimestamp
    }

mockEvent :: EventRecord
mockEvent = EventRecord {eventKey = "mock-key", eventValue = "\"mock\"", eventSerialization = Just "rust_serde"}

mockNotification :: NotificationRecord
mockNotification =
  NotificationRecord
    { notificationRecordMessageUuid = "mock-message-uuid",
      notificationRecordTopic = Nothing,
      notificationRecordMessage = "\"mock\"",
      notificationRecordSerialization = Just "rust_serde",
      notificationRecordCreatedAt = mockTimestamp,
      notificationRecordConsumed = False
    }

mockStreamRecord :: StreamRecord
mockStreamRecord =
  StreamRecord
    { streamKey = "mock-key",
      streamOffset = 0,
      streamValue = "\"mock\"",
      streamSerialization = Just "rust_serde",
      streamStepId = 0
    }

-- * The sim backend wrapped in a connection

-- | The sim backend wrapped in a connection. Constructing this is what
-- needs @instance SystemDB IOSimSystemDB (IOSim s)@.
simConnection :: IOSim s (Connection (IOSim s))
simConnection = do
  ids <- newTVarIO 0
  entropy <- newTVarIO 0
  newConnection
    (SomeSystemDB IOSimSystemDB)
    RustSerde
    (Just "sim-app")
    (secondsDuration 1)
    OwnerApplication
    (simGeneratedId ids)
    (simEntropy entropy)

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
-- then 'simLaunch'.
simInstance :: IOSim s (DBOS (IOSim s))
simInstance = do
  registry <- newRegistry
  executorVar <- newMVar Nothing
  lifecycleVar <- newMVar ()
  pure
    DBOS
      { dbos_config = undefined,
        dbos_registry = registry,
        dbos_logger = nullLogAction,
        dbos_executor = executorVar,
        dbos_lifecycle = lifecycleVar
      }

-- | Freezes the registry and installs the sim executor over the snapshot:
-- the sim equivalent of a launch, with no database behind it.
simLaunch :: DBOS (IOSim s) -> IOSim s ()
simLaunch dbos = do
  workflows <- snapshotRegistry dbos.dbos_registry
  executor <- simExecutorWith workflows
  modifyMVar_ dbos.dbos_executor (const (pure (Just executor)))

-- | A @DBOS (IOSim s)@ already launched over the mock backend.
simDBOS :: IOSim s (DBOS (IOSim s))
simDBOS = do
  dbos <- simInstance
  simLaunch dbos
  pure dbos

-- | The sim executor over a registry snapshot: the connection checked in
-- the same breath.
simExecutor :: IOSim s (Executor (IOSim s))
simExecutor = do
  registry <- newRegistry
  workflows <- snapshotRegistry registry
  simExecutorWith workflows

simExecutorWith :: Snapshot (IOSim s) -> IOSim s (Executor (IOSim s))
simExecutorWith workflows = do
  conn <- simConnection
  tasks <- newTasks
  let identity =
        Identity
          { identityAppName = "sim-app",
            identityAppVersion = "0.0.0",
            identityExecutorId = "sim-executor",
            identityAppId = ""
          }
  pure Executor {conn = conn, identity = identity, workflows = workflows, listen_queues = Nothing, tasks = tasks}
