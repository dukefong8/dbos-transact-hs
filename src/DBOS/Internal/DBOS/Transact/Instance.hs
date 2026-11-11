{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Instance and executor lifecycle, the Haskell port of Rust @instance.rs@.
-- Launch resolves one deployment identity, connects the class backend
-- through a 'Connection', registers the version, performs recovery before
-- returning, and freezes the workflow registry as one snapshot. Shutdown
-- aborts every tracked task and then closes the connection, so an aborted
-- workflow stays @PENDING@ for the next launch to recover.
module DBOS.Transact.Instance
  ( DBOS (..),
    Executor (..),
    newDBOS,
    registerWorkflow,
    isLaunched,
    dbosExecutorId,
    dbosAppVersion,
    dbosAppId,
    launch,
    launchWithEnvironment,
    launchOn,
    launchOnWithQueues,
    launchExecutor,
    shutdown,
    shutdownWith,
    ShutdownOptions (..),
    shutdownOptionsDefault,
    requireExecutor,
    withExecutor,
    registerWorkflowRef,
    registerDataSource,
    clearCheckpoints,
    runWorkflow,
    startWorkflow,
    runWorkflowRef,
    enqueueWorkflow,
    enqueueWorkflowByName,
    retrieveWorkflow,
    getWorkflowEvent,
    getWorkflowEventSerialized,
    getAllEvents,
    getWorkflowRecord,
    sendWorkflowMessage,
    sendWorkflowMessages,
    listWorkflowIdsByName,
    fetchWorkflowStatuses,
    getWorkflowStatus,
    cancelWorkflow,
    cancelWorkflows,
    resumeWorkflow,
    resumeWorkflows,
    setWorkflowDelay,
    deleteWorkflow,
    deleteWorkflows,
    forkWorkflow,
    forkWorkflows,
    forkFrom,
    rewindWorkflow,
    rewindWorkflows,
    dequeueWorkflows,
    updateWorkflowAttributes,
    mergeWorkflowAttributes,
    listAppVersions,
    latestAppVersion,
    promoteAppVersion,
    promoteAppVersionFor,
    listWorkflowSteps,
    listWorkflowStepsWith,
    listWorkflowStepsSerialized,
    listWorkflowStepsSerializedWith,
    listWorkflows,
    listQueuedWorkflows,
  )
where

import Data.Aeson (FromJSON, ToJSON, Value)
import Data.Int (Int64)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import DBOS.Prelude
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types
  ( AppScope,
    Duration,
    EncodedValue (..),
    EventRecord (..),
    Fork,
    ForkOptions,
    ForkPoint,
    IdempotencyKey,
    RewindOptions (..),
    EncodedSendMessage (..),
    NewWorkflow (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    StepRecord,
    Timestamp (..),
    durationAsMillis,
    forkNew,
    Topic,
    VersionInfo (..),
    WorkflowDelay (..),
    WorkflowFilter (..),
    WorkflowId (..),
    WorkflowIdReusePolicy (..),
    WorkflowInitResult,
    QualifiedWorkflowName,
    qualifiedName,
    qualifiedClassName,
    qualifiedConfigName,
    WorkflowRecord (..),
    WorkflowStatus,
    defaultWorkflowFilter,
    newWorkflow,
    renderQualifiedWorkflowName,
    secondsDuration
  )
import DBOS.Transact.Serialization
  ( CodecError (..),
    SerializationError,
    WorkflowInput (..),
    codecErrorText,
    codecUnknownTag,
    decodeWorkflowValue,
    unWorkflowInput
  )
import DBOS.Transact.Serialization qualified as Serialization
import DBOS.Transact.Config (Config (..), serializerName)
import DBOS.Transact.Config qualified as Config
import DBOS.Transact.Connection
  ( Connection (..),
    releaseConnection,
    acquireConnection,
    generatedWorkflowId,
    withConnection
  )
import DBOS.Transact.Context (WorkflowCtx)
import DBOS.Transact.Dequeue (dequeuePass, superviseForever)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Handle (WorkflowHandle, pollingHandle)
import DBOS.Transact.Identity (Environment, Identity (..), readEnvironment, resolve)
import DBOS.Transact.Logger (SomeTracer, acquireLoggerBackend, ioTracer, runTracer)
import DBOS.Transact.Management qualified as Management
import DBOS.Transact.Management (ManagementEvent (..), StepInfo)
import DBOS.Transact.Recovery (EngineEvent (..), reenqueueForRecovery)
import DBOS.Transact.Datasource
  ( DataSource (..),
    DataSourceRegistry,
    clearDatasourceCheckpoints,
    freezeDataSourceRegistry,
    newDataSourceRegistry,
    snapshotDatasources,
    thawDataSourceRegistry
  )
import DBOS.Transact.Datasource qualified as Datasource (registerDataSource)
import DBOS.Transact.Registry
  ( Registry,
    Snapshot,
    bindRegistryInstance,
    lookupSnapshotWorkflow,
    newRegistry,
    snapshotRegistry,
    snapshotSize,
    thawRegistry
  )
import DBOS.Transact.TimeoutSweep (spawnTimeoutSweep)
import DBOS.Transact.Workflow
  ( EnqueueOptions (..),
    ErasedWorkflow,
    RunOptions (..),
    StartOptions,
    Submitted (..),
    Tasks,
    WorkflowRef,
    abortAll,
    newTasks,
    runRegisteredWorkflow,
    runningTasks,
    spawnTracked,
    waitAllUntil
  )
import DBOS.Transact.Workflow qualified as Workflow
  ( EnqueueOptions (..),
    enqueueWorkflow,
    initOrJoin,
    queueRequest,
    registerTypedWorkflow,
    registerWorkflowRef,
    runWorkflowRef,
    startWorkflow,
    storedPriority,
    timeoutBudget,
    validateEnqueueOptions
  )

-- | An instance is the application's stable configuration and registry;
-- its executor slot is empty until launch and may be filled again after a
-- shutdown. The lifecycle lock serializes launch against shutdown.
data DBOS m = DBOS
  { dbos_config    :: Config,
    dbos_registry  :: Registry m (ErasedWorkflow m),
    dbos_datasources :: DataSourceRegistry m,
    dbos_executor  :: StrictMVar m (Maybe (Executor m)),
    dbos_lifecycle :: StrictMVar m ()
  }

-- | The running process identity and the resources it owns. Mirrors Rust
-- @Executor@: the connection, the resolved identity, the launch-time
-- registry snapshot, the queues this executor listens on, and the tracked
-- tasks shutdown reaches. One task set per 'Role', because shutdown stops
-- them in turn: the producers first so the drained set cannot grow, then
-- the workflows with a timeout, and the monitors last. The tracer is the
-- launch's FastLogger backend, announced through by workers and workflow
-- contexts; its release runs at shutdown, after the tasks it outlives.
data Executor m = Executor
  { conn          :: Connection m,
    identity      :: Identity,
    datasources   :: DataSourceRegistry m,
    workflows     :: Snapshot m (ErasedWorkflow m),
    listen_queues :: Maybe [Text],
    producerTasks :: Tasks m,
    workflowTasks :: Tasks m,
    monitorTasks  :: Tasks m,
    stopping      :: StrictTVar m Bool,
    releaseTracer :: m ()
  }

-- | What 'shutdownWith' does with the workflows still running. A struct
-- rather than a bare argument, matching the other @...With@ calls.
-- Mirrors Rust @ShutdownOptions@.
data ShutdownOptions = ShutdownOptions
  { shutdownWorkflowCompletionTimeout :: Duration
  }
  deriving stock (Eq, Show)

-- | Wait for nothing: every running workflow is cancelled at once, as
-- 'shutdown' always did. Mirrors Rust @ShutdownOptions::default@.
shutdownOptionsDefault :: ShutdownOptions
shutdownOptionsDefault = ShutdownOptions { shutdownWorkflowCompletionTimeout = secondsDuration 0 }

-- | How often a draining shutdown says which workflows it is still
-- waiting for. Mirrors Rust @DRAIN_REPORT_INTERVAL@.
drainReportInterval :: Duration
drainReportInterval = secondsDuration 1

newDBOS :: MonadMVar m => Config -> m (DBOS m)
newDBOS config' = do
  registry <- newRegistry
  datasources <- newDataSourceRegistry
  executor <- newMVar Nothing
  lifecycle <- newMVar ()
  pure
    DBOS
      { dbos_config = config',
        dbos_registry = registry,
        dbos_datasources = datasources,
        dbos_executor = executor,
        dbos_lifecycle = lifecycle
      }

-- | Register one typed workflow under its full identity triple. The registry
-- lock makes registration and launch's snapshot mutually exclusive. The body
-- takes the explicit context it runs in.
-- | Register one typed workflow whose body takes the scoped workflow view:
-- the converted shape. The body can only reach the scoped entries and must
-- downgrade explicitly at call sites not yet converted; converted and
-- unconverted bodies share the registry, so conversion proceeds one body
-- at a time.
registerWorkflow :: (FromJSON argument, ToJSON result, ToJSON e, MonadMVar m)
                 => DBOS m -> QualifiedWorkflowName -> (forall exec. argument -> WorkflowCtx exec m -> m (Either (TransactError.Error e) result)) -> m (Either (TransactError.Error TransactError.EngineOnly) ())
registerWorkflow dbos key body = Workflow.registerTypedWorkflow dbos.dbos_registry key body

-- | 'registerWorkflowRef' for a reference-typed workflow body.
registerWorkflowRef :: (FromJSON argument, ToJSON result, ToJSON e, MonadMVar m)
                    => DBOS m -> QualifiedWorkflowName -> (forall exec. argument -> WorkflowCtx exec m -> m (Either (TransactError.Error e) result)) -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowRef m e))
registerWorkflowRef dbos key body = Workflow.registerWorkflowRef dbos.dbos_registry key body

isLaunched :: MonadMVar m => DBOS m -> m Bool
isLaunched dbos = maybe False (const True) <$> readMVar dbos.dbos_executor

-- | Identifies this process among the executors sharing the database, or
-- 'TransactError.NotLaunched' naming the call when no executor is
-- installed. Mirrors Rust @DBOS::executor_id@, which reads the launched
-- executor through @executor("executor_id")@.
dbosExecutorId :: MonadMVar m
               => DBOS m -> m (Either (TransactError.Error TransactError.EngineOnly) Text)
dbosExecutorId dbos =
  fmap (\executor -> executor.identity.identityExecutorId) <$> requireExecutor dbos "executor_id"

-- | The version of the application's code, as workflow rows record it, or
-- 'TransactError.NotLaunched' naming the call when no executor is
-- installed. Mirrors Rust @DBOS::app_version@.
dbosAppVersion :: MonadMVar m
               => DBOS m -> m (Either (TransactError.Error TransactError.EngineOnly) Text)
dbosAppVersion dbos =
  fmap (\executor -> executor.identity.identityAppVersion) <$> requireExecutor dbos "app_version"

-- | This application's DBOS Cloud id, empty off DBOS Cloud, or
-- 'TransactError.NotLaunched' naming the call when no executor is
-- installed. Mirrors Rust @DBOS::app_id@.
dbosAppId :: MonadMVar m => DBOS m -> m (Either (TransactError.Error TransactError.EngineOnly) Text)
dbosAppId dbos =
  fmap (\executor -> executor.identity.identityAppId) <$> requireExecutor dbos "app_id"

-- | Resolve identity, connect, verify and prepare the backend before
-- installing the executor. A failed start thaws the registry so the caller
-- can correct configuration or registration and retry.
launch :: DBOS IO -> IO (Either (TransactError.Error TransactError.EngineOnly) (Executor IO))
launch dbos = readEnvironment >>= launchWithEnvironment dbos

-- | Launch against an explicit environment snapshot. The ordinary entry
-- point reads the process environment; this form keeps environment
-- resolution deterministic for hosted runtimes and live tests.
launchWithEnvironment :: DBOS IO -> Environment -> IO (Either (TransactError.Error TransactError.EngineOnly) (Executor IO))
launchWithEnvironment dbos environment =
  withMVar dbos.dbos_lifecycle $ \_ -> do
    existing <- readMVar dbos.dbos_executor
    case existing of
      Just executor -> do
        -- Idempotent launch: warn and hand back the running executor rather
        -- than failing a caller who was merely defensive.
        runTracer executor.conn.connTracer (EngineAlreadyLaunched executor.identity.identityAppName)
        pure (Right executor)
      Nothing -> case Config.validateConfig dbos.dbos_config of
        Left err -> pure (Left err)
        Right () -> case resolve dbos.dbos_config environment of
          Left err -> pure (Left err)
          Right resolved -> do
            snapshot <- snapshotRegistry dbos.dbos_registry
            freezeDataSourceRegistry dbos.dbos_datasources
            -- A logger acquisition failure must reopen the datasource
            -- registry, or every later registration would report
            -- AlreadyLaunched on an instance that never launched.
            (backend, releaseFastLogger) <- acquireLoggerBackend `onException` thawDataSourceRegistry dbos.dbos_datasources
            let tracer = ioTracer backend
            started <- startExecutor dbos.dbos_config resolved snapshot dbos.dbos_datasources tracer releaseFastLogger `onException` (releaseFastLogger >> thawRegistry dbos.dbos_registry >> thawDataSourceRegistry dbos.dbos_datasources)
            case started of
              Left err -> releaseFastLogger >> thawRegistry dbos.dbos_registry >> thawDataSourceRegistry dbos.dbos_datasources >> pure (Left err)
              Right executor -> do
                launched <-
                  launchExecutor dbos executor
                    `onException` (releaseConnection executor.conn >> releaseFastLogger >> thawRegistry dbos.dbos_registry >> thawDataSourceRegistry dbos.dbos_datasources)
                -- A 'Left' here already closed the connection inside
                -- 'launchExecutor's prepare-failure branch; only the logger
                -- and the thaws remain.
                case launched of
                  Left err -> releaseFastLogger >> thawRegistry dbos.dbos_registry >> thawDataSourceRegistry dbos.dbos_datasources >> pure (Left err)
                  Right executor' -> pure (Right executor')

-- | Shutdown is idempotent and reopens registration for a later launch.
-- Tasks are aborted and waited for before the connection closes, so a
-- workflow caught mid-run stays @PENDING@ and is recovered by the next
-- | Shutdown is idempotent and reopens registration for a later launch.
-- With the default options this waits for nothing: every running
-- workflow is cancelled at once and stays @PENDING@, as 'shutdown' always
-- did. Mirrors Rust @DBOS::shutdown@.
shutdown :: (MonadMVar m, MonadFork m, MonadAsync m, MonadDelay m, MonadMonotonicTimeNSec m, MonadCatch m) => DBOS m -> m ()
shutdown dbos = shutdownWith dbos shutdownOptionsDefault

-- | Stops the executor and closes the system database, first waiting up to
-- @shutdownWorkflowCompletionTimeout@ for the workflows running in this
-- process to finish. In order: the dequeue loop stops so
-- nothing new starts here (the port runs no scheduler loop); running workflows drain to the timeout; what
-- is left is cancelled and stays @PENDING@; the timeout sweep stops; the
-- system database closes last. The instance stays launched until the
-- drain is over, so a workflow finishing during the wait can still call
-- into it. Idempotent, and a second call made while the first is waiting
-- returns once the first has finished (both serialize on the lifecycle
-- lock). Mirrors Rust @DBOS::shutdown_with@.
shutdownWith :: (MonadMVar m, MonadFork m, MonadAsync m, MonadDelay m, MonadMonotonicTimeNSec m, MonadCatch m) => DBOS m -> ShutdownOptions -> m ()
shutdownWith dbos options = do
  -- The teardown runs as a task of its own, so a caller that stops
  -- waiting leaves it to finish rather than cutting the shutdown short
  -- halfway through. The lifecycle lock lives inside that task, not the
  -- waiter: a dropped wait must not reopen the lifecycle while the
  -- teardown still holds it, or a second shutdown would tear down twice
  -- and a relaunch would be wiped when the first teardown takes the
  -- executor it just installed. The async wrapper delivers the result
  -- whether the teardown returns, throws, or is cancelled; the waiter
  -- reads it from the task, so there is no box to leave empty.
  task <- async (tearDownIfLaunched dbos options)
  outcome <- waitCatch task
  either throwIO pure (outcome :: Either SomeException ())

-- | Takes the lifecycle lock and tears down the installed executor, if
-- any: the lock lives in the teardown task rather than its waiter, so a
-- dropped wait neither reopens the lifecycle early nor lets a second
-- shutdown or a relaunch interleave with the teardown still running.
tearDownIfLaunched :: (MonadMVar m, MonadFork m, MonadAsync m, MonadDelay m, MonadMonotonicTimeNSec m) => DBOS m -> ShutdownOptions -> m ()
tearDownIfLaunched dbos options = withMVar dbos.dbos_lifecycle $ \_ -> do
  current <- readMVar dbos.dbos_executor
  case current of
    Nothing -> pure ()
    Just executor -> tearDown dbos executor options

-- | The whole of 'shutdownWith', run as its own task: stop the executor
-- while still installed, take it once nothing is running that could call
-- back into the instance, then close. A workflow given time to finish may
-- call back into this instance — to send, to start a child — and an
-- instance that already said it was not launched would fail it with an
-- error its body could record as its outcome. Mirrors Rust @tear_down@.
tearDown :: (MonadMVar m, MonadFork m, MonadAsync m, MonadDelay m, MonadMonotonicTimeNSec m) => DBOS m -> Executor m -> ShutdownOptions -> m ()
tearDown dbos executor options = do
  stop executor options.shutdownWorkflowCompletionTimeout
  modifyMVar_ dbos.dbos_executor (const (pure Nothing))
  close executor
  -- The announcement runs after the close: the tracer is FastLogger, not
  -- the database, so it survives the release. The registries thaw last
  -- with the tracer release, reopening registration for a later launch.
  runTracer executor.conn.connTracer (EngineShutdown executor.identity.identityAppName)
  executor.releaseTracer
  thawRegistry dbos.dbos_registry
  thawDataSourceRegistry dbos.dbos_datasources

-- | Stops this executor starting workflows, waits up to the timeout for
-- those running to finish, and cancels whatever is still running at the
-- deadline. The producers stop first so the drained set cannot grow: a
-- dequeue whose claim has committed starts what it claimed, and that
-- workflow drains with the rest. A zero timeout waits for nothing.
-- Mirrors Rust @Executor::stop@.
stop :: (MonadMVar m, MonadFork m, MonadAsync m, MonadDelay m, MonadMonotonicTimeNSec m) => Executor m -> Duration -> m ()
stop executor timeout = do
  atomically (writeTVar executor.stopping True)
  let draining = timeout /= secondsDuration 0
  when draining $ do
    deadline <- deadlineNs timeout
    producersDone <- waitAllUntil executor.producerTasks deadline
    -- Waited on before the drain, not alongside it: a dequeue
    -- mid-dispatch is about to add to the set the drain watches, and a
    -- drain that saw it empty first would finish before that workflow
    -- started.
    when producersDone (drain executor deadline)
  producers <- abortAll executor.producerTasks
  abandoned <- abortAll executor.workflowTasks
  if draining
    then do
      unless (null producers) $
        runTracer executor.conn.connTracer (EngineProducersCut producers)
      unless (null abandoned) $
        runTracer executor.conn.connTracer (EngineWorkflowsCut (length abandoned) abandoned)
    else
      unless (null abandoned) $
        runTracer executor.conn.connTracer (EngineCancelledRunning (length abandoned) abandoned)

-- | Waits for the running workflows to finish on their own, up to the
-- deadline, saying once a second which ones are still waited on. Waits
-- only: what is left is cancelled by 'stop' after this returns. Mirrors
-- Rust @Executor::drain@.
drain :: (MonadMVar m, MonadAsync m, MonadDelay m, MonadMonotonicTimeNSec m) => Executor m -> Word64 -> m ()
drain executor deadline = do
  now <- getMonotonicTimeNSec
  let reportAt = min (now + fromInteger (durationAsMillis drainReportInterval * 1000000)) deadline
  done <- waitAllUntil executor.workflowTasks reportAt
  unless done $
    if reportAt >= deadline
      then pure ()
      else do
        running <- runningTasks executor.workflowTasks
        runTracer executor.conn.connTracer (EngineDrainWaiting (length running) running)
        drain executor deadline

-- | Stops the background monitors and closes the system database, once no
-- workflow is running: a workflow still running may wait on the sweep to
-- record its timeout, and everything before the close could still
-- checkpoint. Mirrors Rust @Executor::close@.
close :: (MonadFork m, MonadSTM m, MonadMVar m) => Executor m -> m ()
close executor = do
  _ <- abortAll executor.monitorTasks
  releaseConnection executor.conn

-- | A monotonic-nanosecond deadline the given duration from now.
deadlineNs :: (MonadMonotonicTimeNSec m) => Duration -> m Word64
deadlineNs timeout = do
  now <- getMonotonicTimeNSec
  pure (now + fromInteger (durationAsMillis timeout * 1000000))

runWorkflow :: forall m e. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m, FromJSON e)
            => Executor m -> QualifiedWorkflowName -> WorkflowId -> Maybe WorkflowInput -> m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue))
runWorkflow executor key workflowId input = do
  outcome <-
    runRegisteredWorkflow
      executor.workflowTasks
      executor.conn
      executor.identity
      executor.workflows
      key
      workflowId
      (unWorkflowInput <$> input)
      WorkflowIdReturnExisting
      executor.datasources
  pure outcome

-- | Starts the referenced workflow via the launched executor: what
-- @WorkflowRef::start_with@ becomes when the call site holds a reference.
startWorkflow :: (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
              => Executor m -> WorkflowRef m e -> StartOptions -> Maybe WorkflowInput -> m (Either (TransactError.Error c) (WorkflowHandle m e))
startWorkflow executor ref options input =
  Workflow.startWorkflow executor.workflowTasks executor.conn executor.identity executor.workflows ref options (unWorkflowInput <$> input) executor.datasources

-- | Runs the referenced workflow via the launched executor and waits: a
-- start followed by an await under the same id.
runWorkflowRef :: forall m e. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m, FromJSON e)
               => Executor m -> WorkflowRef m e -> RunOptions -> Maybe WorkflowInput -> m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue))
runWorkflowRef executor ref options input = do
  outcome <-
    Workflow.runWorkflowRef executor.workflowTasks executor.conn executor.identity executor.workflows ref options (unWorkflowInput <$> input) executor.datasources
  pure outcome

enqueueWorkflow :: (MonadMVar m)
                => DBOS m -> QualifiedWorkflowName -> WorkflowId -> Maybe WorkflowInput -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) WorkflowInitResult)
enqueueWorkflow dbos key workflowId input queueName = do
  withExecutor dbos "enqueue a workflow" $ \executor ->
    case lookupSnapshotWorkflow key executor.workflows of
      Nothing -> pure (Left (TransactError.NotRegistered key))
      Just _ ->
        Workflow.enqueueWorkflow
          executor.conn
          executor.identity
          key
          workflowId
          (unWorkflowInput <$> input)
          queueName

-- | Enqueues a workflow by name, without checking this process's
-- registrations: one another service or language implements. Outside a
-- workflow body this writes the row a client's enqueue writes, except it
-- names this executor on the row. The row belongs to this application
-- unless the options name another, and carries no version unless one is
-- named, so the application's latest version dequeues it. Mirrors Rust
-- @DBOS::enqueue@ outside a workflow (the in-workflow face is
-- 'enqueueWorkflowByNameInWorkflow').
enqueueWorkflowByName :: (MonadMVar m)
                      => DBOS m -> QualifiedWorkflowName -> EnqueueOptions -> Maybe WorkflowInput -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
enqueueWorkflowByName dbos name options input =
  withExecutor dbos "enqueue a workflow" $ \executor ->
    case Workflow.validateEnqueueOptions name options of
      Left err -> pure (Left err)
      Right () -> do
        generated <- generatedWorkflowId executor.conn
        let request = Workflow.queueRequest options
            workflowText = fromMaybe generated options.workflowId
            rawInput = unWorkflowInput <$> input
            serialization = case rawInput >>= (.serializedSerialization) of
              Just (Serialization serializationName) -> Just serializationName
              Nothing -> Just (serializerName executor.conn.connSerializer)
            new =
              (newWorkflow workflowText)
              { newWorkflowName = Just (qualifiedName name),
                newWorkflowClassName = qualifiedClassName name,
                newWorkflowConfigName = qualifiedConfigName name,
                newWorkflowInput = (.serializedText) <$> rawInput,
                newWorkflowSerialization = serialization,
                newWorkflowQueueName = Just options.queueName,
                newWorkflowDeduplicationId = options.deduplicationId,
                newWorkflowPriority = Workflow.storedPriority request,
                newWorkflowQueuePartitionKey = options.partitionKey,
                newWorkflowDelay = options.delay,
                newWorkflowTimeout = Workflow.timeoutBudget options.timeout,
                newWorkflowDeadline = Nothing,
                newWorkflowAttributes = Serialization.encodeAttributes options.attributes,
                newWorkflowExecutorId = Just executor.identity.identityExecutorId,
                newWorkflowAppName = options.appName <|> Just executor.identity.identityAppName,
                newWorkflowAppVersion = options.appVersion
              }
        started <- Workflow.initOrJoin executor.conn new (Just request) options.workflowIdReusePolicy Nothing
        pure $ case started of
          Right (SubmittedCreated _)     -> Right (pollingHandle executor.conn workflowText True)
          Right (SubmittedJoined holder) -> Right holder
          Left err                       -> Left (TransactError.refusedStart (Just options.queueName) err)

dequeueWorkflows :: (MonadAsync m, MonadMVar m, MonadFork m, MonadMask m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                 => DBOS m -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
dequeueWorkflows dbos = do
  withExecutor dbos "dequeue workflows" $ \executor ->
    dequeuePass
      executor.workflowTasks
      executor.conn
      executor.identity
      executor.workflows
      executor.listen_queues
      executor.datasources

-- | A handle to a workflow by id. Nothing here has seen the row: the id is
-- the caller's, taken on faith, which is the one handle shape that waits
-- for a row to appear rather than reporting it missing.
retrieveWorkflow :: MonadMVar m
                 => DBOS m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
retrieveWorkflow dbos (WorkflowId workflowText) =
  withExecutor dbos "retrieve a workflow" $ \executor ->
    pure (Right (pollingHandle executor.conn workflowText False))

cancelWorkflows :: (MonadMVar m)
                => DBOS m -> [WorkflowId] -> Bool -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
cancelWorkflows dbos workflowIds includeChildren =
  withExecutor dbos "cancel workflows" $ \executor ->
    Management.cancelWorkflows executor.conn workflowIds includeChildren Nothing

resumeWorkflows :: (MonadMVar m)
                => DBOS m -> [WorkflowId] -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
resumeWorkflows dbos workflowIds queueName = do
  withExecutor dbos "resume workflows" $ \executor -> Management.resumeWorkflows executor.conn workflowIds queueName Nothing

-- | Moves a delayed workflow's release: brought forward to now, the
-- supervisor releases it on its next pass. The launched-instance guard
-- first, as every method on this surface expects.
setWorkflowDelay :: (MonadMVar m)
                 => DBOS m -> WorkflowId -> WorkflowDelay -> m (Either (TransactError.Error TransactError.EngineOnly) ())
setWorkflowDelay dbos workflowId delay = do
  withExecutor dbos "set a workflow delay" $ \executor -> Management.setWorkflowDelay executor.conn workflowId delay

deleteWorkflows :: (MonadMVar m)
                => DBOS m -> [WorkflowId] -> Bool -> m (Either (TransactError.Error TransactError.EngineOnly) Word64)
deleteWorkflows dbos workflowIds includeChildren = do
  withExecutor dbos "delete workflows" $ \executor -> Management.deleteWorkflows executor.conn workflowIds includeChildren

forkWorkflows :: (MonadMVar m)
              => DBOS m -> [Fork] -> ForkOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
forkWorkflows dbos forks options = do
  withExecutor dbos "fork workflows" $ \executor -> Management.forkWorkflows executor.conn forks options

forkFrom :: (MonadMVar m)
         => DBOS m -> [WorkflowId] -> ForkPoint -> ForkOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
forkFrom dbos workflowIds point options = do
  withExecutor dbos "fork from workflows" $ \executor -> Management.forkFrom executor.conn workflowIds point options

-- | Replaces the attributes attached to a workflow, or clears them when
-- given 'Nothing'. The launched-instance guard first, as every method on
-- this surface expects; the write itself lives in 'Management'.
updateWorkflowAttributes :: (MonadMVar m)
                         => DBOS m -> WorkflowId -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
updateWorkflowAttributes dbos workflowId attributes = do
  withExecutor dbos "update workflow attributes" $ \executor -> Management.updateWorkflowAttributes executor.conn workflowId attributes

-- | Merges keys into a workflow's attributes, in one statement. The
-- launched-instance guard first; the write itself lives in 'Management'.
mergeWorkflowAttributes :: (MonadMVar m)
                        => DBOS m -> WorkflowId -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
mergeWorkflowAttributes dbos workflowId attributes = do
  withExecutor dbos "merge workflow attributes" $ \executor -> Management.mergeWorkflowAttributes executor.conn workflowId attributes

-- | Reads the workflows matching a filter. The launched-instance guard
-- first; the read itself lives in 'Management'.
listWorkflows :: (MonadMVar m)
              => DBOS m -> WorkflowFilter -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowRecord])
listWorkflows dbos filters = do
  withExecutor dbos "list workflows" $ \executor -> Management.listWorkflows executor.conn filters

-- | The registered application versions, newest first. The
-- launched-instance guard first; the read itself lives in 'Management',
-- which the client's calls share. Mirrors Rust @DBOS::list_app_versions@.
listAppVersions :: (MonadMVar m)
                => DBOS m -> m (Either (TransactError.Error TransactError.EngineOnly) [VersionInfo])
listAppVersions dbos = do
  withExecutor dbos "list application versions" $ \executor -> Management.listAppVersions executor.conn

-- | The latest registered application version. The launched-instance guard
-- first; the read itself lives in 'Management'. Mirrors Rust
-- @DBOS::latest_app_version@.
latestAppVersion :: (MonadMVar m)
                 => DBOS m -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe VersionInfo))
latestAppVersion dbos = do
  withExecutor dbos "read the latest application version" $ \executor -> Management.latestAppVersion executor.conn

-- | Promotes an already-registered version to be the latest for this
-- application's fleet: promoting an older one is a rollback. The
-- launched-instance guard first; the write itself lives in 'Management'.
-- Mirrors Rust @DBOS::set_latest_app_version@.
promoteAppVersion :: (MonadMVar m)
                  => DBOS m -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
promoteAppVersion dbos version = do
  withExecutor dbos "promote an application version" $ \executor -> Management.promoteAppVersion executor.conn version

-- | Promotes a version for the named application. The launched-instance
-- guard first; the write itself lives in 'Management'. Mirrors Rust
-- @DBOS::set_latest_app_version_for@.
promoteAppVersionFor :: (MonadMVar m)
                     => DBOS m -> Text -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
promoteAppVersionFor dbos version application = do
  withExecutor dbos "promote an application version" $ \executor -> Management.promoteAppVersionFor executor.conn version application

-- | The shortcut Python calls @list_queued_workflows@: the same filter with
-- the queue-only narrowing applied.
listQueuedWorkflows :: (MonadMVar m)
                    => DBOS m -> WorkflowFilter -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowRecord])
listQueuedWorkflows dbos filters = listWorkflows dbos (filters {workflowFilterQueuesOnly = True})

-- | Reads one workflow's steps in execution order with outputs and errors
-- decoded; an id with no row lists nothing rather than failing.
listWorkflowSteps :: (MonadMVar m)
                  => DBOS m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) [StepInfo])
listWorkflowSteps dbos workflowId =
  listWorkflowStepsWith dbos workflowId Management.defaultListWorkflowStepsOptions

-- | 'listWorkflowSteps' with the listing's options.
listWorkflowStepsWith :: (MonadMVar m)
                      => DBOS m -> WorkflowId -> Management.ListWorkflowStepsOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [StepInfo])
listWorkflowStepsWith dbos workflowId options = do
  withExecutor dbos "list a workflow's steps" $ \executor -> Management.listWorkflowStepsWith executor.conn workflowId options

-- | The raw Rust-shaped step listing. Internal to the port; apps use
-- 'listWorkflowSteps'.
listWorkflowStepsSerialized :: (MonadMVar m)
                            => DBOS m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) [StepRecord])
listWorkflowStepsSerialized dbos workflowId =
  listWorkflowStepsSerializedWith dbos workflowId Management.defaultListWorkflowStepsOptions

-- | The raw listing with options. Internal to the port.
listWorkflowStepsSerializedWith :: (MonadMVar m)
                                => DBOS m -> WorkflowId -> Management.ListWorkflowStepsOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [StepRecord])
listWorkflowStepsSerializedWith dbos workflowId options = do
  withExecutor dbos "list a workflow's steps" $ \executor -> Management.listWorkflowStepsSerializedWith executor.conn workflowId options

-- | Cancels one workflow, children skipped: the singular of
-- 'cancelWorkflows'. Mirrors Rust @DBOS::cancel@.
cancelWorkflow :: (MonadMVar m)
               => DBOS m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) ())
cancelWorkflow dbos workflowId = void <$> cancelWorkflows dbos [workflowId] False

-- | Resumes one workflow, optionally onto a queue, and hands back its
-- handle. Mirrors Rust @DBOS::resume@ / @resume_with@.
resumeWorkflow :: (MonadMVar m)
               => DBOS m -> WorkflowId -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
resumeWorkflow dbos workflowId queue = do
  resumed <- resumeWorkflows dbos [workflowId] queue
  case resumed of
    Left err -> pure (Left err)
    Right _  -> retrieveWorkflow dbos workflowId

-- | Deletes one workflow, children skipped: the singular of
-- 'deleteWorkflows'. Mirrors Rust @DBOS::delete@.
deleteWorkflow :: (MonadMVar m)
               => DBOS m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) ())
deleteWorkflow dbos workflowId = void <$> deleteWorkflows dbos [workflowId] False

-- | Forks one workflow and hands back the new workflow's handle.
-- @Nothing@ forks from the top (Rust @ForkFrom::Beginning@); @Just point@
-- from a chosen step (Rust @ForkFrom::Step@/@LastFailure@). Mirrors Rust
-- @DBOS::fork@ / @fork_with@.
forkWorkflow :: (MonadMVar m)
             => DBOS m -> WorkflowId -> Maybe ForkPoint -> ForkOptions -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
forkWorkflow dbos workflowId from options = do
  forked <- case from of
    Nothing -> do
      let WorkflowId workflowText = workflowId
      forkWorkflows dbos [forkNew workflowText] options
    Just point -> forkFrom dbos [workflowId] point options
  case forked of
    Left err         -> pure (Left err)
    Right []         -> pure (Left (TransactError.fromSystemDB (SystemDBError.InvalidInput {field = "fork", detail = "the fork produced no workflow"})))
    Right (newId : _) -> retrieveWorkflow dbos newId

-- | Rewinds a workflow in place, keeping its id: the datasource
-- checkpoints go first, best effort, and the system-database rewind last,
-- so a failure leaves the row as it was and the rewind retries safely.
-- Mirrors Python @rewind_workflow@'s ordering; the client form
-- ('Client.clientRewindWorkflows') skips the checkpoint drop, so rewound
-- datasource transactions are not re-executed there.
rewindWorkflows :: (MonadMVar m)
                => DBOS m -> WorkflowId -> RewindOptions -> m (Either (TransactError.Error TransactError.EngineOnly) ())
rewindWorkflows dbos workflowId options =
  withExecutor dbos "rewind workflow" $ \executor -> do
    sources <- snapshotDatasources dbos.dbos_datasources
    outcomes <- traverse (\source -> source.dsDeleteCheckpoints workflowId options.rewindStartStep) sources
    case sequence outcomes of
      Left backendErr -> pure (Left (TransactError.fromSystemDB (SystemDBError.Backend backendErr)))
      Right _ -> Management.rewindWorkflows executor.conn workflowId options

-- | Rewinds one workflow and hands back its handle. Mirrors Rust
-- @DBOS::rewind@ / @rewind_with@.
rewindWorkflow :: (MonadMVar m)
               => DBOS m -> WorkflowId -> RewindOptions -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
rewindWorkflow dbos workflowId options = do
  rewound <- rewindWorkflows dbos workflowId options
  case rewound of
    Left err -> pure (Left err)
    Right () -> retrieveWorkflow dbos workflowId

-- | One workflow's status, or 'Nothing' when no row matches: the singular
-- of 'fetchWorkflowStatuses'.
getWorkflowStatus :: (MonadMVar m) => DBOS m -> WorkflowId -> m (Maybe WorkflowStatus)
getWorkflowStatus dbos workflowId = lookup workflowId <$> fetchWorkflowStatuses dbos [workflowId]

-- | Read another workflow's event from outside a workflow, waiting up to
-- the duration (zero is a poll). Nothing is checkpointed: there is no caller
-- to record against. The typed form decodes by the row's tag; a foreign tag
-- is @UnknownSerialization@ and a bad body @Deserialization@.
getWorkflowEvent :: (FromJSON value, MonadMVar m, MonadDelay m, MonadTime m)
                 => DBOS m -> WorkflowId -> Text -> Duration -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe value))
getWorkflowEvent dbos workflowId key wait = do
  found <- getWorkflowEventSerialized dbos workflowId key wait
  pure $ case found of
    Left err -> Left err
    Right Nothing -> Right Nothing
    Right (Just stored) ->
      case decodeWorkflowValue "event value" (Just stored) of
        Left err -> Left (case codecUnknownTag err of
          Just tag -> TransactError.fromSerializationError (Serialization.UnknownSerialization {name = tag})
          Nothing -> TransactError.fromSerializationError (Serialization.Deserialization "event value" (codecErrorText err)))
        Right value -> Right (Just value)

-- | The raw, tag-preserving outside read. Internal to the port; apps use
-- 'getWorkflowEvent'.
getWorkflowEventSerialized :: (MonadMVar m, MonadDelay m, MonadTime m)
                           => DBOS m -> WorkflowId -> Text -> Duration -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe SerializedWorkflowValue))
getWorkflowEventSerialized dbos workflowId key wait = do
  withExecutor dbos "read an event" $ \executor -> do
    found <- withConnection executor.conn (\db -> SystemDB.getEvent db workflowId key wait Nothing)
    pure $ case found of
      Left err -> Left err
      Right Nothing -> Right Nothing
      Right (Just encoded) ->
        Right (Just (SerializedWorkflowValue encoded.encodedValue (Serialization <$> encoded.encodedSerialization)))

-- | Every event of a workflow as key -> latest value, decoded by each
-- event's tag. The outside-caller form of Python's @get_all_events@; duplicate
-- keys resolve to the last row, matching the oracle's dict construction.
getAllEvents :: (MonadMVar m)
             => DBOS m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) (Map Text Value))
getAllEvents dbos workflowId = do
  withExecutor dbos "read all events" $ \executor -> do
    found <- withConnection executor.conn (\db -> SystemDB.getAllEvents db workflowId)
    pure $ case found of
      Left err -> Left err
      Right records -> Map.fromList <$> traverse decode records
  where
    decode record =
      case (decodeWorkflowValue "event value" (Just (SerializedWorkflowValue record.eventValue (Serialization <$> record.eventSerialization))) :: Either CodecError Value) of
        Left err -> Left (case codecUnknownTag err of
          Just tag -> TransactError.fromSerializationError (Serialization.UnknownSerialization {name = tag})
          Nothing -> TransactError.fromSerializationError (Serialization.Deserialization "event value" (codecErrorText err)))
        Right value -> Right (record.eventKey, value)

-- | The full row, not just the status enum: Python's @retrieve_workflow(...)
-- .get_status()@ / @get_workflow@ shape. The payload columns are raw text and
-- their tags; decode them with the serialization codec.
getWorkflowRecord :: (MonadMVar m)
                  => DBOS m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe WorkflowRecord))
getWorkflowRecord dbos workflowId = do
  withExecutor dbos "get workflow" $ \executor ->
    withConnection executor.conn (\db -> SystemDB.getWorkflow db workflowId)

-- | Send one message to a workflow from outside, the caller unrecorded.
sendWorkflowMessage :: (MonadMVar m)
                    => DBOS m -> WorkflowId -> Maybe Topic -> Maybe IdempotencyKey -> SerializedWorkflowValue -> m (Either (TransactError.Error TransactError.EngineOnly) ())
sendWorkflowMessage dbos destination topic idempotencyKey value = do
  withExecutor dbos "send a message" $ \executor -> do
    let message =
          EncodedSendMessage
            { sendDestinationId = destination,
              sendMessageBody = value,
              sendTopic = topic,
              sendIdempotencyKey = idempotencyKey
            }
    written <- withConnection executor.conn (\db -> SystemDB.sendMessage db message ((\(Serialization name) -> name) <$> value.serializedSerialization) Nothing Nothing False)
    pure written

-- | Send a batch from outside in one transaction: all or none.
sendWorkflowMessages :: (MonadMVar m)
                     => DBOS m -> [EncodedSendMessage] -> m (Either (TransactError.Error TransactError.EngineOnly) ())
sendWorkflowMessages dbos messages = do
  withExecutor dbos "send messages" $ \executor -> do
    written <- withConnection executor.conn (\db -> SystemDB.sendMessages db messages Nothing Nothing Nothing False)
    pure written

-- | Ids of the most recent workflows with this name, newest first, capped.
-- | The most recent ids registered under one workflow name: the "last N
-- by name" list the clients render. Descending on purpose — the oracle's
-- filter defaults to oldest-first, which is useless under a limit on a
-- used database (the newest run falls outside the window).
listWorkflowIdsByName :: (MonadMVar m)
                      => DBOS m -> Text -> Int64 -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
listWorkflowIdsByName dbos name limit = do
  withExecutor dbos "list workflows" $ \executor -> do
    listed <-
      withConnection
        executor.conn
        ( \db ->
            SystemDB.listWorkflows
              db
              ( defaultWorkflowFilter
                  { workflowFilterNames = [name],
                    workflowFilterLimit = Just limit,
                    workflowFilterSortDesc = True
                  }
              )
              Nothing
        )
    pure $ case listed of
      Left err      -> Left err
      Right records -> Right [record.workflowRecordId | record <- records]

-- | Statuses for the ids that still have a row; a missing row is dropped.
fetchWorkflowStatuses :: (MonadMVar m) => DBOS m -> [WorkflowId] -> m [(WorkflowId, WorkflowStatus)]
fetchWorkflowStatuses dbos workflowIds = do
  running <- requireExecutor dbos "fetch workflow statuses"
  case running of
    Left _ -> pure []
    Right executor -> do
      fetched <-
        traverse
          (\workflowId -> do
              row <- withConnection executor.conn (\db -> SystemDB.getWorkflow db workflowId)
              pure (workflowId, either (const Nothing) (fmap (.workflowRecordStatus)) row))
          workflowIds
      pure [(workflowId, status) | (workflowId, Just status) <- fetched]

requireExecutor :: MonadMVar m
                => DBOS m -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) (Executor m))
requireExecutor dbos operation = do
  current <- readMVar dbos.dbos_executor
  pure $ case current of
    Nothing       -> Left (TransactError.NotLaunched operation)
    Just executor -> Right executor

-- | Runs the body against the launched executor, or answers
-- 'TransactError.NotLaunched' naming the call when no executor is
-- installed: the one home for the read-MVar-plus-@case@ every 'DBOS m'
-- entry point repeats.
withExecutor :: MonadMVar m
             => DBOS m -> Text -> (Executor m -> m (Either (TransactError.Error TransactError.EngineOnly) a)) -> m (Either (TransactError.Error TransactError.EngineOnly) a)
withExecutor dbos operation body = either (pure . Left) body =<< requireExecutor dbos operation

-- | Register a datasource on the instance unless launch has frozen the
-- registry or the name is taken: the created-before-launch rule. A failed
-- launch or shutdown thaws it again.
registerDataSource :: MonadMVar m
                   => DBOS m -> DataSource m -> m (Either (TransactError.Error TransactError.EngineOnly) ())
registerDataSource dbos source = Datasource.registerDataSource dbos.dbos_datasources source

-- | Clear a finished workflow's checkpoints from every registered
-- datasource, best effort and silent.
clearCheckpoints :: (MonadMVar m, MonadCatch m) => DBOS m -> WorkflowId -> m ()
clearCheckpoints dbos wid = clearDatasourceCheckpoints dbos.dbos_datasources wid

-- | Installs an executor over a caller-built connection: the seam tests
-- launch arbitrary backends through. The snapshot is taken from the
-- instance's own registry, the task registry is fresh, and no queues are
-- listened on.
launchOn :: (MonadSTM m, MonadMVar m) => DBOS m -> Connection m -> Identity -> m (Executor m)
launchOn dbos conn identity = do
  -- The reference's registry learns which connection this launch installed,
  -- so a child start can tell another instance's reference apart.
  bindRegistryInstance dbos.dbos_registry conn.connInstanceId
  workflows <- snapshotRegistry dbos.dbos_registry
  producers <- newTasks
  flows <- newTasks
  monitors <- newTasks
  stopping <- newTVarIO False
  let executor = Executor {conn = conn, identity = identity, datasources = dbos.dbos_datasources, workflows = workflows, listen_queues = Nothing, producerTasks = producers, workflowTasks = flows, monitorTasks = monitors, stopping = stopping, releaseTracer = pure ()}
  modifyMVar_ dbos.dbos_executor (const (pure (Just executor)))
  pure executor

-- | 'launchOn' with an explicit listen set: the programmatic counterpart
-- of 'Config.configListenQueues', which the config-file launch path
-- installs. Like 'launchOn', the instance ends up holding the executor —
-- the filtered one — so the dequeue sweep and a later 'launchExecutor'
-- see the listen set rather than a filtered copy that was dropped.
launchOnWithQueues :: (MonadSTM m, MonadMVar m)
                   => DBOS m -> Connection m -> Identity -> Maybe [Text] -> m (Executor m)
launchOnWithQueues dbos conn identity listen = do
  executor <- launchOn dbos conn identity
  let listened = executor {listen_queues = listen}
  modifyMVar_ dbos.dbos_executor (const (pure (Just listened)))
  pure listened

-- | Complete a launch over an executor a caller has built: register the
-- application version, recover this executor's pending rows, announce the
-- launch, and fork the supervisor. The IO launch and the simulated launches
-- build the executor differently — pool vs caller connection, configured
-- queues vs none — and run this same tail from here on. (launchOn has
-- already bound the registry and installed the executor; the repeats below
-- keep this tail self-contained for the IO builder path, which does
-- neither — both operations are idempotent.)
launchExecutor :: (MonadMVar m, MonadFork m, MonadMask m, MonadAsync m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
               => DBOS m -> Executor m -> m (Either (TransactError.Error TransactError.EngineOnly) (Executor m))
launchExecutor dbos executor = do
  prepared <- prepare executor.conn executor.identity
  case prepared of
    Left err -> releaseConnection executor.conn >> pure (Left err)
    Right _ -> do
      bindRegistryInstance dbos.dbos_registry executor.conn.connInstanceId
      runTracer executor.conn.connTracer (EngineLaunched executor.identity.identityAppName executor.identity.identityExecutorId executor.identity.identityAppVersion)
      _ <- spawnTracked executor.producerTasks "queues" (superviseForever executor.producerTasks executor.workflowTasks executor.stopping executor.conn executor.identity executor.workflows executor.listen_queues executor.datasources)
      _ <- spawnTimeoutSweep executor.monitorTasks executor.conn
      modifyMVar_ dbos.dbos_executor (const (pure (Just executor)))
      pure (Right executor)

startExecutor :: Config -> Identity -> Snapshot IO (ErasedWorkflow IO) -> DataSourceRegistry IO -> SomeTracer IO -> IO () -> IO (Either (TransactError.Error TransactError.EngineOnly) (Executor IO))
startExecutor config' identity' workflowsSnapshot sources tracer releaseTracer = do
  when (snapshotSize workflowsSnapshot == 0) $
    runTracer tracer EngineNoWorkflows
  acquired <- try (acquireConnection config' identity' tracer) :: IO (Either SystemDBError.Error (Connection IO))
  case acquired of
    Left err -> pure (Left (TransactError.fromSystemDB err))
    Right conn' -> do
      producers' <- newTasks
      flows' <- newTasks
      monitors' <- newTasks
      stopping' <- newTVarIO False
      pure
        ( Right
            Executor
              { conn = conn',
                identity = identity',
                workflows = workflowsSnapshot,
                datasources = sources,
                listen_queues = config'.configListenQueues,
                producerTasks = producers',
                workflowTasks = flows',
                monitorTasks = monitors',
                stopping = stopping',
                releaseTracer = releaseTracer
              }
        )

prepare :: Monad m
        => Connection m -> Identity -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
prepare conn identity' = do
  registered <- withConnection conn (\db -> SystemDB.createApplicationVersion db identity'.identityAppVersion Nothing)
  case registered of
    Left err -> pure (Left err)
    Right () -> do
      latest <- withConnection conn (\db -> SystemDB.getLatestApplicationVersion db Nothing)
      case latest of
        Left err -> pure (Left err)
        Right (Just version) -> do
          when (version.versionInfoName /= identity'.identityAppVersion) $
            runTracer conn.connTracer (EngineVersionStale identity'.identityAppVersion version.versionInfoName)
          reenqueueForRecovery conn identity'.identityExecutorId identity'.identityAppVersion
        Right Nothing -> reenqueueForRecovery conn identity'.identityExecutorId identity'.identityAppVersion
