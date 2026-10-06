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
    registerDBOSWorkflow,
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
    requireExecutor,
    registerDBOSWorkflowRef,
    registerDBOSDataSource,
    clearDBOSCheckpoints,
    runDBOSWorkflow,
    startDBOSWorkflowRef,
    runDBOSWorkflowRef,
    enqueueDBOSWorkflow,
    retrieveWorkflow,
    getWorkflowEvent,
    sendWorkflowMessage,
    sendWorkflowMessages,
    listWorkflowIdsByName,
    fetchWorkflowStatuses,
    cancelWorkflows,
    resumeWorkflows,
    setWorkflowDelay,
    deleteWorkflows,
    forkWorkflows,
    forkFrom,
    dequeueDBOSWorkflows,
    updateWorkflowAttributes,
    listWorkflows,
  )
where

import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Control.Monad.Class.MonadThrow qualified as MThrow
import Data.Aeson (FromJSON, ToJSON)
import Data.Int (Int64)
import Data.Text (Text)
import Data.Word (Word64)
import DBOS.Prelude
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (Duration, EncodedValue (..), Fork, ForkOptions, ForkPoint, IdempotencyKey, SendMessage (..), Serialization (..), SerializedWorkflowValue (..), Topic, WorkflowDelay (..), WorkflowFilter (..), VersionInfo (..), WorkflowId (..), WorkflowInitResult, WorkflowRecord (..), WorkflowStatus, defaultWorkflowFilter)
import DBOS.Transact.Config (Config (..))
import DBOS.Transact.Config qualified as Config
import DBOS.Transact.Connection (Connection (..), closeConnection, forApplication, runSystemDB)
import DBOS.Transact.Context (WorkflowCtx)
import DBOS.Transact.Dequeue (dequeuePass, superviseForever)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Handle (WorkflowHandle, pollingHandle)
import DBOS.Transact.Identity (Environment, Identity (..), readEnvironment, resolve)
import DBOS.Tracer (SomeTracer, acquireLoggerBackend, ioTracer, runTracer)
import DBOS.Transact.Management qualified as Management
import DBOS.Transact.Management (ManagementEvent (..))
import DBOS.Transact.Recovery (EngineEvent (..), reenqueueForRecovery)
import DBOS.Transact.Datasource (DataSource (..), DataSourceRegistry, clearDatasourceCheckpoints, freezeDataSourceRegistry, newDataSourceRegistry, registerDataSource, thawDataSourceRegistry)
import DBOS.Transact.Registry (Registry, Snapshot, WorkflowKey, WorkflowRef, bindRegistryInstance, lookupSnapshotWorkflow, newRegistry, registerTypedWorkflow, registerWorkflowRef, renderWorkflowKey, snapshotRegistry, snapshotSize, thawRegistry)
import DBOS.Transact.Workflow (RunOptions (..), StartOptions, Tasks, abortAll, enqueueWorkflow, newTasks, runRegisteredWorkflow, runWorkflowRef, spawnTracked, startWorkflowRef)

-- | An instance is the application's stable configuration and registry;
-- its executor slot is empty until launch and may be filled again after a
-- shutdown. The lifecycle lock serializes launch against shutdown.
data DBOS m = DBOS
  { dbos_config    :: Config,
    dbos_registry  :: Registry m,
    dbos_datasources :: DataSourceRegistry m,
    dbos_executor  :: StrictMVar m (Maybe (Executor m)),
    dbos_lifecycle :: StrictMVar m ()
  }

-- | The running process identity and the resources it owns. Mirrors Rust
-- @Executor@: the connection, the resolved identity, the launch-time
-- registry snapshot, the queues this executor listens on, and the tracked
-- tasks shutdown reaches. The tracer is the launch's FastLogger backend,
-- announced through by workers and workflow contexts; its release runs at
-- shutdown, after the tasks it outlives.
data Executor m = Executor
  { conn          :: Connection m,
    identity      :: Identity,
    datasources   :: DataSourceRegistry m,
    workflows     :: Snapshot m,
    listen_queues :: Maybe [Text],
    tasks         :: Tasks m,
    releaseTracer :: m ()
  }

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
registerDBOSWorkflow :: (FromJSON argument, ToJSON result, ToJSON e, MonadMVar m) => DBOS m -> WorkflowKey -> (forall exec. argument -> WorkflowCtx exec m -> m (Either (TransactError.Error e) result)) -> m (Either (TransactError.Error TransactError.EngineOnly) ())
registerDBOSWorkflow dbos key body = registerTypedWorkflow dbos.dbos_registry key body

-- | 'registerDBOSWorkflowRef' for a reference-typed workflow body.
registerDBOSWorkflowRef :: (FromJSON argument, ToJSON result, ToJSON e, MonadMVar m) => DBOS m -> WorkflowKey -> (forall exec. argument -> WorkflowCtx exec m -> m (Either (TransactError.Error e) result)) -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowRef m e))
registerDBOSWorkflowRef dbos key body = registerWorkflowRef dbos.dbos_registry key body

isLaunched :: MonadMVar m => DBOS m -> m Bool
isLaunched dbos = maybe False (const True) <$> readMVar dbos.dbos_executor

-- | Identifies this process among the executors sharing the database, or
-- 'TransactError.ErrorNotLaunched' naming the call when no executor is
-- installed. Mirrors Rust @DBOS::executor_id@, which reads the launched
-- executor through @executor("executor_id")@.
dbosExecutorId :: MonadMVar m => DBOS m -> m (Either (TransactError.Error TransactError.EngineOnly) Text)
dbosExecutorId dbos =
  fmap (\executor -> executor.identity.identityExecutorId) <$> requireExecutor dbos "executor_id"

-- | The version of the application's code, as workflow rows record it, or
-- 'TransactError.ErrorNotLaunched' naming the call when no executor is
-- installed. Mirrors Rust @DBOS::app_version@.
dbosAppVersion :: MonadMVar m => DBOS m -> m (Either (TransactError.Error TransactError.EngineOnly) Text)
dbosAppVersion dbos =
  fmap (\executor -> executor.identity.identityAppVersion) <$> requireExecutor dbos "app_version"

-- | This application's DBOS Cloud id, empty off DBOS Cloud, or
-- 'TransactError.ErrorNotLaunched' naming the call when no executor is
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
      Just executor -> pure (Right executor)
      Nothing -> case Config.validateConfig dbos.dbos_config of
        Left err -> pure (Left err)
        Right () -> case resolve dbos.dbos_config environment of
          Left err -> pure (Left err)
          Right resolved -> do
            snapshot <- snapshotRegistry dbos.dbos_registry
            freezeDataSourceRegistry dbos.dbos_datasources
            (backend, releaseFastLogger) <- acquireLoggerBackend
            let tracer = ioTracer backend
            started <- startExecutor dbos.dbos_config resolved snapshot dbos.dbos_datasources tracer releaseFastLogger `onException` (releaseFastLogger >> thawRegistry dbos.dbos_registry)
            case started of
              Left err -> releaseFastLogger >> thawRegistry dbos.dbos_registry >> thawDataSourceRegistry dbos.dbos_datasources >> pure (Left err)
              Right executor -> do
                launched <-
                  launchExecutor dbos executor
                    `onException` (closeConnection executor.conn >> thawRegistry dbos.dbos_registry >> thawDataSourceRegistry dbos.dbos_datasources)
                case launched of
                  Left err -> releaseFastLogger >> thawRegistry dbos.dbos_registry >> thawDataSourceRegistry dbos.dbos_datasources >> pure (Left err)
                  Right executor' -> pure (Right executor')

-- | Shutdown is idempotent and reopens registration for a later launch.
-- Tasks are aborted and waited for before the connection closes, so a
-- workflow caught mid-run stays @PENDING@ and is recovered by the next
-- launch rather than lost.
shutdown :: (MonadMVar m, MonadFork m, MonadSTM m) => DBOS m -> m ()
shutdown dbos =
  withMVar dbos.dbos_lifecycle $ \_ -> do
    current <- readMVar dbos.dbos_executor
    case current of
      Nothing -> pure ()
      Just executor -> do
        modifyMVar_ dbos.dbos_executor (const (pure Nothing))
        cancelled <- abortAll executor.tasks
        when (cancelled > 0) $
          runTracer executor.conn.connTracer (EngineCancelledRunning cancelled)
        closeConnection executor.conn
        runTracer executor.conn.connTracer (EngineShutdown executor.identity.identityAppName)
        executor.releaseTracer
        thawRegistry dbos.dbos_registry
        thawDataSourceRegistry dbos.dbos_datasources

runDBOSWorkflow :: forall m e. (MonadFork m, MThrow.MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, FromJSON e) => Executor m -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue))
runDBOSWorkflow executor key workflowId input = do
  outcome <-
    runRegisteredWorkflow
      executor.tasks
      executor.conn
      executor.identity
      executor.workflows
      key
      workflowId
      input
  _ <- MThrow.try (clearDatasourceCheckpoints executor.datasources workflowId) :: m (Either SomeException ())
  pure outcome

-- | Starts the referenced workflow via the launched executor: what
-- @WorkflowRef::start_with@ becomes when the call site holds a reference.
startDBOSWorkflowRef :: (MonadFork m, MThrow.MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, FromJSON e) => Executor m -> WorkflowRef m e -> StartOptions -> Maybe SerializedWorkflowValue -> m (Either (TransactError.Error c) (WorkflowHandle m e))
startDBOSWorkflowRef executor ref options input =
  startWorkflowRef executor.tasks executor.conn executor.identity executor.workflows ref options input

-- | Runs the referenced workflow via the launched executor and waits: a
-- start followed by an await under the same id.
runDBOSWorkflowRef :: forall m e. (MonadFork m, MThrow.MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, FromJSON e) => Executor m -> WorkflowRef m e -> RunOptions -> Maybe SerializedWorkflowValue -> m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue))
runDBOSWorkflowRef executor ref options input = do
  outcome <-
    runWorkflowRef executor.tasks executor.conn executor.identity executor.workflows ref options input
  case options.runWorkflowId of
    Just workflowId' -> do
      _ <- MThrow.try (clearDatasourceCheckpoints executor.datasources workflowId') :: m (Either SomeException ())
      pure outcome
    Nothing -> pure outcome

enqueueDBOSWorkflow :: (MonadMVar m, Monad m) => DBOS m -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) WorkflowInitResult)
enqueueDBOSWorkflow dbos key workflowId input queueName = do
  running <- requireExecutor dbos "enqueue a workflow"
  case running of
    Left err -> pure (Left err)
    Right executor ->
      case lookupSnapshotWorkflow key executor.workflows of
        Nothing -> pure (Left (TransactError.ErrorWorkflowNotRegistered (renderWorkflowKey key)))
        Just _ ->
          enqueueWorkflow
            executor.conn
            executor.identity
            key
            workflowId
            input
            queueName

dequeueDBOSWorkflows :: (MonadMVar m, MonadSTM m, MonadFork m, MThrow.MonadMask m, MonadTimer m, MonadTime m) => DBOS m -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
dequeueDBOSWorkflows dbos = do
  running <- requireExecutor dbos "dequeue workflows"
  case running of
    Left err -> pure (Left err)
    Right executor ->
      dequeuePass
        executor.tasks
        executor.conn
        executor.identity
        executor.workflows
        executor.listen_queues

-- | A handle to a workflow by id. Nothing here has seen the row: the id is
-- the caller's, taken on faith, which is the one handle shape that waits
-- for a row to appear rather than reporting it missing.
retrieveWorkflow :: MonadMVar m => DBOS m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
retrieveWorkflow dbos (WorkflowId workflowText) = do
  running <- requireExecutor dbos "retrieve a workflow"
  pure $ case running of
    Left err       -> Left err
    Right executor -> Right (pollingHandle executor.conn workflowText False)

cancelWorkflows :: (MonadMVar m, Monad m) => DBOS m -> [WorkflowId] -> Bool -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
cancelWorkflows dbos workflowIds includeChildren = do
  running <- requireExecutor dbos "cancel workflows"
  case running of
    Left err       -> pure (Left err)
    Right executor -> Management.cancelWorkflows executor.conn workflowIds includeChildren

resumeWorkflows :: (MonadMVar m, Monad m) => DBOS m -> [WorkflowId] -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
resumeWorkflows dbos workflowIds queueName = do
  running <- requireExecutor dbos "resume workflows"
  case running of
    Left err       -> pure (Left err)
    Right executor -> Management.resumeWorkflows executor.conn workflowIds queueName

-- | Moves a delayed workflow's release: brought forward to now, the
-- supervisor releases it on its next pass. The launched-instance guard
-- first, as every method on this surface expects.
setWorkflowDelay :: (MonadMVar m, Monad m) => DBOS m -> WorkflowId -> WorkflowDelay -> m (Either (TransactError.Error TransactError.EngineOnly) ())
setWorkflowDelay dbos workflowId delay = do
  running <- requireExecutor dbos "set a workflow delay"
  case running of
    Left err -> pure (Left err)
    Right executor -> do
      result <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.setWorkflowDelay db workflowId delay Nothing)
      case result of
        Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
        -- Mirrors @set_workflow_delay@: phrased as the request, not the
        -- effect — the row may already be released, so this announces
        -- unconditionally.
        Right () -> do
          let WorkflowId workflowText = workflowId
          runTracer executor.conn.connTracer (WorkflowDelayMoveAsked workflowText)
          pure (Right ())

deleteWorkflows :: (MonadMVar m, Monad m) => DBOS m -> [WorkflowId] -> Bool -> m (Either (TransactError.Error TransactError.EngineOnly) Word64)
deleteWorkflows dbos workflowIds includeChildren = do
  running <- requireExecutor dbos "delete workflows"
  case running of
    Left err       -> pure (Left err)
    Right executor -> Management.deleteWorkflows executor.conn workflowIds includeChildren

forkWorkflows :: (MonadMVar m, Monad m) => DBOS m -> [Fork] -> ForkOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
forkWorkflows dbos forks options = do
  running <- requireExecutor dbos "fork workflows"
  case running of
    Left err       -> pure (Left err)
    Right executor -> Management.forkWorkflows executor.conn forks options

forkFrom :: (MonadMVar m, Monad m) => DBOS m -> [WorkflowId] -> ForkPoint -> ForkOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
forkFrom dbos workflowIds point options = do
  running <- requireExecutor dbos "fork from workflows"
  case running of
    Left err       -> pure (Left err)
    Right executor -> Management.forkFrom executor.conn workflowIds point options

-- | Replaces the attributes attached to a workflow, or clears them when
-- given 'Nothing'. The launched-instance guard first, as every method on
-- this surface expects; the write itself lives in 'Management'.
updateWorkflowAttributes :: (MonadMVar m, Monad m) => DBOS m -> WorkflowId -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
updateWorkflowAttributes dbos workflowId attributes = do
  running <- requireExecutor dbos "update workflow attributes"
  case running of
    Left err       -> pure (Left err)
    Right executor -> Management.updateWorkflowAttributes executor.conn workflowId attributes

-- | Reads the workflows matching a filter. The launched-instance guard
-- first; the read itself lives in 'Management'.
listWorkflows :: (MonadMVar m, Monad m) => DBOS m -> WorkflowFilter -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowRecord])
listWorkflows dbos filters = do
  running <- requireExecutor dbos "list workflows"
  case running of
    Left err       -> pure (Left err)
    Right executor -> Management.listWorkflows executor.conn filters

-- | Read another workflow's event from outside a workflow, waiting up to
-- the duration (zero is a poll). Nothing is checkpointed: there is no caller
-- to record against, which is what makes this the outside-caller surface.
getWorkflowEvent :: (MonadMVar m, MonadSTM m, MonadDelay m, MonadTime m) => DBOS m -> WorkflowId -> Text -> Duration -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe SerializedWorkflowValue))
getWorkflowEvent dbos workflowId key wait = do
  running <- requireExecutor dbos "read an event"
  case running of
    Left err -> pure (Left err)
    Right executor -> do
      found <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.getEvent db workflowId key wait Nothing)
      pure $ case found of
        Left err -> Left (TransactError.ErrorSystemDatabase err)
        Right Nothing -> Right Nothing
        Right (Just encoded) ->
          Right (Just (SerializedWorkflowValue encoded.encodedValue (Serialization <$> encoded.encodedSerialization)))

-- | Send one message to a workflow from outside, the caller unrecorded.
sendWorkflowMessage :: (MonadMVar m, Monad m) => DBOS m -> WorkflowId -> Maybe Topic -> Maybe IdempotencyKey -> SerializedWorkflowValue -> m (Either (TransactError.Error TransactError.EngineOnly) ())
sendWorkflowMessage dbos destination topic idempotencyKey value = do
  running <- requireExecutor dbos "send a message"
  case running of
    Left err -> pure (Left err)
    Right executor -> do
      let message =
            SendMessage
              { sendDestinationId = destination,
                sendMessageBody = value,
                sendTopic = topic,
                sendIdempotencyKey = idempotencyKey
              }
      written <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.sendMessage db message ((\(Serialization name) -> name) <$> value.serializedSerialization) Nothing False)
      pure (either (Left . TransactError.ErrorSystemDatabase) Right written)

-- | Send a batch from outside in one transaction: all or none.
sendWorkflowMessages :: (MonadMVar m, Monad m) => DBOS m -> [SendMessage] -> m (Either (TransactError.Error TransactError.EngineOnly) ())
sendWorkflowMessages dbos messages = do
  running <- requireExecutor dbos "send messages"
  case running of
    Left err -> pure (Left err)
    Right executor -> do
      written <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.sendMessages db messages Nothing Nothing False)
      pure (either (Left . TransactError.ErrorSystemDatabase) Right written)

-- | Ids of the most recent workflows with this name, newest first, capped.
-- | The most recent ids registered under one workflow name: the "last N
-- by name" list the clients render. Descending on purpose — the oracle's
-- filter defaults to oldest-first, which is useless under a limit on a
-- used database (the newest run falls outside the window).
listWorkflowIdsByName :: (MonadMVar m, Monad m) => DBOS m -> Text -> Int64 -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
listWorkflowIdsByName dbos name limit = do
  running <- requireExecutor dbos "list workflows"
  case running of
    Left err -> pure (Left err)
    Right executor -> do
      listed <-
        runSystemDB
          executor.conn.connSysdb
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
        Left err      -> Left (TransactError.ErrorSystemDatabase err)
        Right records -> Right [record.workflowRecordId | record <- records]

-- | Statuses for the ids that still have a row; a missing row is dropped.
fetchWorkflowStatuses :: (MonadMVar m, Monad m) => DBOS m -> [WorkflowId] -> m [(WorkflowId, WorkflowStatus)]
fetchWorkflowStatuses dbos workflowIds = do
  running <- requireExecutor dbos "fetch workflow statuses"
  case running of
    Left _ -> pure []
    Right executor -> do
      fetched <-
        traverse
          (\workflowId -> do
              row <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.getWorkflow db workflowId)
              pure (workflowId, either (const Nothing) (fmap (.workflowRecordStatus)) row))
          workflowIds
      pure [(workflowId, status) | (workflowId, Just status) <- fetched]

requireExecutor :: MonadMVar m => DBOS m -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) (Executor m))
requireExecutor dbos operation = do
  current <- readMVar dbos.dbos_executor
  pure $ case current of
    Nothing       -> Left (TransactError.ErrorNotLaunched operation)
    Just executor -> Right executor

-- | Register a datasource on the instance unless launch has frozen the
-- registry or the name is taken: the created-before-launch rule. A failed
-- launch or shutdown thaws it again.
registerDBOSDataSource :: MonadMVar m => DBOS m -> DataSource m -> m (Either (TransactError.Error TransactError.EngineOnly) ())
registerDBOSDataSource dbos source = registerDataSource dbos.dbos_datasources source

-- | Clear a finished workflow's checkpoints from every registered
-- datasource, best effort and silent.
clearDBOSCheckpoints :: (MonadMVar m, MThrow.MonadCatch m) => DBOS m -> WorkflowId -> m ()
clearDBOSCheckpoints dbos wid = clearDatasourceCheckpoints dbos.dbos_datasources wid

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
  tasks <- newTasks
  let executor = Executor {conn = conn, identity = identity, datasources = dbos.dbos_datasources, workflows = workflows, listen_queues = Nothing, tasks = tasks, releaseTracer = pure ()}
  modifyMVar_ dbos.dbos_executor (const (pure (Just executor)))
  pure executor

-- | 'launchOn' with an explicit listen set: the programmatic counterpart
-- of 'Config.configListenQueues', which the config-file launch path
-- installs. Like 'launchOn', the instance ends up holding the executor —
-- the filtered one — so the dequeue sweep and a later 'launchExecutor'
-- see the listen set rather than a filtered copy that was dropped.
launchOnWithQueues :: (MonadSTM m, MonadMVar m) => DBOS m -> Connection m -> Identity -> Maybe [Text] -> m (Executor m)
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
launchExecutor :: (MonadSTM m, MonadMVar m, MonadFork m, MThrow.MonadMask m, MonadDelay m, MonadTimer m, MonadTime m) => DBOS m -> Executor m -> m (Either (TransactError.Error TransactError.EngineOnly) (Executor m))
launchExecutor dbos executor = do
  prepared <- prepare executor.conn executor.identity
  case prepared of
    Left err -> closeConnection executor.conn >> pure (Left err)
    Right _ -> do
      bindRegistryInstance dbos.dbos_registry executor.conn.connInstanceId
      runTracer executor.conn.connTracer (EngineLaunched executor.identity.identityAppName executor.identity.identityExecutorId executor.identity.identityAppVersion)
      _ <- spawnTracked executor.tasks (superviseForever executor.tasks executor.conn executor.identity executor.workflows executor.listen_queues)
      modifyMVar_ dbos.dbos_executor (const (pure (Just executor)))
      pure (Right executor)

startExecutor :: Config -> Identity -> Snapshot IO -> DataSourceRegistry IO -> SomeTracer IO -> IO () -> IO (Either (TransactError.Error TransactError.EngineOnly) (Executor IO))
startExecutor config' identity' workflowsSnapshot sources tracer releaseTracer = do
  when (snapshotSize workflowsSnapshot == 0) $
    runTracer tracer EngineNoWorkflows
  acquired <- try (forApplication config' identity' tracer) :: IO (Either SystemDBError.Error (Connection IO))
  case acquired of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    Right conn' -> do
      tasks' <- newTasks
      pure
        ( Right
            Executor
              { conn = conn',
                identity = identity',
                workflows = workflowsSnapshot,
                datasources = sources,
                listen_queues = config'.configListenQueues,
                tasks = tasks',
                releaseTracer = releaseTracer
              }
        )

prepare :: Monad m => Connection m -> Identity -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
prepare conn identity' = do
  registered <- runSystemDB conn.connSysdb (\db -> SystemDB.createApplicationVersion db identity'.identityAppVersion Nothing)
  case registered of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    Right () -> do
      latest <- runSystemDB conn.connSysdb (\db -> SystemDB.getLatestApplicationVersion db Nothing)
      case latest of
        Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
        Right (Just version) -> do
          when (version.versionInfoName /= identity'.identityAppVersion) $
            runTracer conn.connTracer (EngineVersionStale identity'.identityAppVersion version.versionInfoName)
          reenqueueForRecovery conn identity'.identityExecutorId identity'.identityAppVersion
        Right Nothing -> reenqueueForRecovery conn identity'.identityExecutorId identity'.identityAppVersion
