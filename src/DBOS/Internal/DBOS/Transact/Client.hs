{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}

-- | The client: reaching a DBOS application from outside it. Mirrors Rust
-- @client.rs@: a client talks only to the system database — it enqueues
-- workflows, reads their status, and waits on their results — without
-- registering a workflow, running one, or holding any application code.
-- It never runs a workflow, never migrates (connect verifies instead), and
-- has no application version of its own.
--
-- Queue, schedule, and version surfaces shared with the application ride the
-- same SystemDB methods.
module DBOS.Transact.Client
  ( -- * Configuration
    ClientConfig (..),
    clientConfigNew,
    clientConfigFromEnv,
    validateClientConfig,
    clientOutcomePollInterval,
    -- * Client lifecycle
    Client (..),
    connectClient,
    closeClient,
    withClientTransaction,
    -- * Workflows from outside
    EnqueueOptions (..),
    enqueueOptionsNew,
    enqueueOptionsOn,
    enqueueClientWorkflow,
    enqueueClientWorkflowWith,
    retrieveClientWorkflow,
    workflowStatusClient,
    -- * Messages from outside
    clientSendMessage,
    clientSendMessages,
    clientSendMessageInTx,
    clientSendMessagesInTx,
    clientGetEvent,
    -- * Lifecycle from outside
    clientCancelWorkflows,
    clientResumeWorkflows,
    clientDeleteWorkflows,
    clientForkWorkflows,
    -- * Versions from outside
    clientListApplicationVersions,
    clientLatestApplicationVersion,
    clientPromoteVersion,
    clientListWorkflows,
    clientListWorkflowSteps,
  )
where

import Data.Aeson (Value)
import Data.Map.Strict (Map)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Hasql.Transaction qualified as Tx
import DBOS.Prelude
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Postgres.Backend (PostgresSystemDB, Settings (..))
import DBOS.SystemDB.Postgres.Backend qualified as Postgres
import DBOS.SystemDB.Retry (uuidEntropy)
import DBOS.SystemDB.Types (Duration, EncodedValue (..), Fork (..), ForkOptions (..), IdempotencyKey (..), NewWorkflow (..), SendMessage (..), Serialization (..), SerializedWorkflowValue (..), StepRecord, Submission (..), Topic (..), VersionInfo (..), WorkflowFilter (..), WorkflowId (..), WorkflowRecord (..), WorkflowStatus, durationIsZero, newWorkflow, timestampNow, validateSendKeys)
import DBOS.Transact.Config (Serializer (..), databaseUrlEnv, defaultOutcomePollInterval, serializerName)
import DBOS.Transact.Connection (Connection (..), Owner (..), SomeSystemDB (..), generatedWorkflowId, newConnection, releaseConnection, runSystemDB, withConnection)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Handle (WorkflowHandle, pollingHandle)
import DBOS.Transact.Identity (validateAppName)
import DBOS.Transact.Logger (nullTracer)
import DBOS.Transact.Serialization (encodeAttributes)
import DBOS.Transact.Workflow (Enqueue (..), enqueueNew, maxRecoveryAttempts, resolveEnqueueCollision, storedPriority, validateEnqueue)
import System.Environment (lookupEnv)

-- | Everything a 'Client' needs. A separate type from 'Config' rather than
-- a mode of it: a client has no executor id, no application version, no
-- listen set, and no migrate — four fields that must not be set is four
-- fields that should not exist.
data ClientConfig = ClientConfig
  { appName              :: Maybe Text,
    databaseUrl          :: Text,
    maxConnections       :: Word,
    schema               :: Text,
    serializer           :: Serializer,
    useListenNotify      :: Bool,
    pollingConcurrency   :: Maybe Word,
    outcomePollInterval  :: Maybe Duration,
    notificationCoalesce :: Maybe Duration
  }
  deriving stock (Eq, Show)

-- | A nameless client reaching the database at the URL. Nameless because a
-- name is a claim: set 'app_name' to make it deliberately.
clientConfigNew :: Text -> ClientConfig
clientConfigNew databaseUrl =
  ClientConfig
    { appName = Nothing,
      databaseUrl = databaseUrl,
      maxConnections = 5,
      schema = "dbos",
      serializer = RustSerde,
      useListenNotify = True,
      pollingConcurrency = Nothing,
      outcomePollInterval = Nothing,
      notificationCoalesce = Nothing
    }

-- | 'clientConfigNew', taking the database URL from @DBOS_DATABASE_URL@.
-- The URL is the only thing it reads; 'app_name' stays 'Nothing'.
clientConfigFromEnv :: IO ClientConfig
clientConfigFromEnv = do
  url <- lookupEnv (Text.unpack databaseUrlEnv)
  pure (clientConfigNew (maybe "" Text.pack url))

-- | Checks what can be checked before anything is connected.
validateClientConfig :: ClientConfig -> Either (TransactError.Error TransactError.EngineOnly) ()
validateClientConfig config
  | Text.null config.databaseUrl =
      Left
        ( TransactError.ErrorConfig
            ( "no database URL: set `database_url`, or the "
                <> databaseUrlEnv
                <> " environment variable if the configuration came from `ClientConfig::from_env`"
            )
        )
  | Just name <- config.appName,
    Left err <- validateAppName name =
      Left err
  | Text.null config.schema = Left (TransactError.ErrorConfig "`schema` cannot be empty")
  | config.maxConnections == 0 = Left (TransactError.ErrorConfig "`max_connections` cannot be zero")
  | Just interval <- config.outcomePollInterval,
    durationIsZero interval =
      Left (TransactError.ErrorConfig "`outcome_poll_interval` cannot be zero")
  | otherwise = Right ()

-- | How often a waiting caller looks, resolved.
clientOutcomePollInterval :: ClientConfig -> Duration
clientOutcomePollInterval config =
  maybe defaultOutcomePollInterval id config.outcomePollInterval

-- | A connection to an application's system database, from outside it,
-- plus the transaction runner where the backend hosts caller transactions.
-- Mirrors the oracle's @Client(Arc<Connection>)@.
data Client m = Client
  { conn :: Connection m,
    -- | Run caller-composed statements on the system database, when the
    -- backend can host them: set from the concrete backend at connect, so
    -- only live Postgres clients run transactions and anything else is a
    -- clean refusal instead of a missing method.
    clientTxRunner :: Maybe ClientTxRunner
  }

-- | A system-database transaction runner, ranked so one runner serves any
-- composed block. Built from the concrete backend at connect.
newtype ClientTxRunner = ClientTxRunner (forall a. Text -> Tx.Transaction a -> IO (Either SystemDBError.Error a))

-- | Connects to the system database. Eager: an unreachable database is
-- reported here rather than on the first call. Migrates nothing, verifies
-- instead: 'acquirePostgresSystemDB' already checks the schema ceiling.
-- The connection's owner is 'OwnerClient', which is what makes a
-- workflow's read through it a plain wait rather than a refused instance.
connectClient :: ClientConfig -> IO (Either (TransactError.Error TransactError.EngineOnly) (Client IO))
connectClient config =
  case validateClientConfig config of
    Left err -> pure (Left err)
    Right () -> do
      acquired <- try (Postgres.acquirePostgresSystemDB backendConfig nullTracer) :: IO (Either SystemDBError.Error PostgresSystemDB)
      case acquired of
        Left err -> pure (Left (TransactError.SystemDatabase err))
        Right backend ->
          bracketOnError (pure backend) Postgres.releasePostgresSystemDB $ \systemDB -> do
            Postgres.activatePostgresSystemDB systemDB
            -- A client connection is named too, though no reference is
            -- ever registered on it: the id keeps the shape uniform.
            instanceId <- uuidWorkflowId
            conn <-
              newConnection
                (SomeSystemDB systemDB)
                config.serializer
                config.appName
                (clientOutcomePollInterval config)
                OwnerClient
                instanceId
                uuidWorkflowId
                uuidEntropy
                nullTracer
            pure (Right (Client conn (Just (ClientTxRunner (Postgres.runTransaction systemDB)))))
  where
    backendConfig =
      Postgres.Config
        { Postgres.configUrl = config.databaseUrl,
          Postgres.configMaxConnections = fromIntegral config.maxConnections,
          Postgres.configSettings =
            (Postgres.defaultSettings :: Settings)
              { settingsSchema = config.schema,
                settingsExecutorId = Nothing,
                settingsApplicationName = config.appName,
                settingsPollingConcurrency = fromIntegral <$> config.pollingConcurrency,
                settingsNotificationCoalesce = config.notificationCoalesce
              }
        }
    uuidWorkflowId = Text.pack . UUID.toString <$> UUID.V4.nextRandom

-- | Closes the connection: the notifier stops before the pool closes, so
-- nothing queued is lost to a closed pool.
closeClient ::  Client m -> m ()
closeClient client = releaseConnection client.conn

-- | Run caller-composed statements on the system database in one
-- transaction: the caller's own writes and sends commit together or not at
-- all, mirroring Python @send_in_transaction@'s caller-owned transaction.
-- Statement failures escape and abort the transaction; backend failures
-- return 'Left'. A client whose backend cannot host transactions is refused
-- rather than run partially.
withClientTransaction :: Client IO -> Text -> Tx.Transaction a -> IO (Either (TransactError.Error TransactError.EngineOnly) a)
withClientTransaction client operation action = case client.clientTxRunner of
  Nothing -> pure (Left (TransactError.InvalidArgument "client transaction" "the client backend cannot run caller transactions"))
  Just (ClientTxRunner run) -> do
    result <- run operation action
    pure (either (Left . TransactError.SystemDatabase) Right result)

-- | Send a batch with the caller's writes in one transaction: the writes run
-- first, then the sends, all in the caller's commit. The batch is validated
-- up front — nothing runs on a refusal — while foreign-key failures abort
-- through the insert, as Python's raise aborts the caller's transaction.
-- No step is recorded, like every client send.
clientSendMessagesInTx :: Client IO -> Text -> Tx.Transaction () -> [SendMessage] -> Maybe Text -> [Text] -> Bool -> IO (Either (TransactError.Error TransactError.EngineOnly) ())
clientSendMessagesInTx client operation callerWrites messages serialization fallbacks sendToForks =
  case validateSendKeys messages of
    Left err -> pure (Left (TransactError.SystemDatabase err))
    Right ()
      | length fallbacks /= length messages ->
          pure (Left (TransactError.SystemDatabase (SystemDBError.invalidInput "fallbacks" "one fallback id per message")))
      | otherwise -> runExceptT $ do
          composed <- ExceptT (withClientTransaction client operation (callerWrites >> Postgres.sendBatchInTx messages serialization fallbacks sendToForks))
          -- The piece condemns on its own refusal, so a 'Left' here already
          -- rolled the caller writes back; map it out rather than swallowing it.
          ExceptT (pure (first TransactError.SystemDatabase composed))

-- | Send one message with the caller's writes in one transaction, as
-- 'clientSendMessagesInTx' sends a batch.
clientSendMessageInTx :: Client IO -> Text -> Tx.Transaction () -> SendMessage -> Maybe Text -> Text -> Bool -> IO (Either (TransactError.Error TransactError.EngineOnly) ())
clientSendMessageInTx client operation callerWrites message serialization fallback sendToForks =
  clientSendMessagesInTx client operation callerWrites [message] serialization [fallback] sendToForks

-- | What an enqueue may say about how, beside the workflow and its input.
-- Mirrors Rust @EnqueueOptions@: the queue-shaped asks live on 'Enqueue',
-- where the runtime's start keeps them too, so the two surfaces never spell
-- the same things differently.
data EnqueueOptions = EnqueueOptions
  { queue      :: Enqueue,
    workflowId :: Maybe Text,
    className  :: Maybe Text,
    configName :: Maybe Text,
    appName    :: Maybe Text,
    appVersion :: Maybe Text,
    timeout    :: Maybe Duration,
    attributes :: Maybe (Map Text Value)
  }
  deriving stock (Eq, Show)

-- | A plain enqueue onto a queue, asking for nothing else.
enqueueOptionsNew :: Text -> EnqueueOptions
enqueueOptionsNew queueName = enqueueOptionsOn (enqueueNew queueName)

-- | The same, for an enqueue that has something to ask of the queue itself.
enqueueOptionsOn :: Enqueue -> EnqueueOptions
enqueueOptionsOn shape =
  EnqueueOptions
    { queue = shape,
      workflowId = Nothing,
      className = Nothing,
      configName = Nothing,
      appName = Nothing,
      appVersion = Nothing,
      timeout = Nothing,
      attributes = Nothing
    }

-- | Enqueues a workflow by name, onto a queue, and hands back a polling
-- handle to it. Asks for nothing else: the client's app name is stamped,
-- no version is recorded, and the id is generated.
enqueueClientWorkflow :: MonadSTM m
                      => Client m -> Text -> Text -> Maybe SerializedWorkflowValue -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
enqueueClientWorkflow client workflowName queueName input =
  enqueueClientWorkflowWith client workflowName (enqueueOptionsNew queueName) input

-- | 'enqueueClientWorkflow', with something to say about how. A call that
-- names no version records none — which the dequeue admits only from the
-- latest registered version — while a named version pins the row to
-- executors running exactly that code. A deduplication collision is refused,
-- unless the policy asks to join the holder instead.
enqueueClientWorkflowWith :: MonadSTM m
                          => Client m -> Text -> EnqueueOptions -> Maybe SerializedWorkflowValue -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
enqueueClientWorkflowWith client workflowName options input = do
  case validateEnqueue options.queue of
    Left err -> pure (Left err)
    Right () -> do
      generated <- generatedWorkflowId client.conn
      let shape = options.queue
          workflowText = fromMaybe generated options.workflowId
          serialization = case input >>= (.serializedSerialization) of
            Just (Serialization name) -> Just name
            Nothing                   -> Just (serializerName client.conn.connSerializer)
          applicationName = case options.appName of
            Just name -> Just name
            Nothing   -> client.conn.connAppName
          new =
            (newWorkflow workflowText)
              { newWorkflowName = Just workflowName,
                newWorkflowClassName = options.className,
                newWorkflowConfigName = options.configName,
                newWorkflowInput = (.serializedText) <$> input,
                newWorkflowSerialization = serialization,
                newWorkflowQueueName = Just shape.name,
                newWorkflowDeduplicationId = shape.deduplicationId,
                newWorkflowPriority = storedPriority shape,
                newWorkflowQueuePartitionKey = shape.partitionKey,
                newWorkflowDelay = shape.delay,
                newWorkflowTimeout = options.timeout,
                newWorkflowDeadline = Nothing,
                newWorkflowAttributes = encodeAttributes options.attributes,
                newWorkflowExecutorId = Nothing,
                newWorkflowApplicationName = applicationName,
                newWorkflowApplicationVersion = options.appVersion
              }
      initialized <- runSystemDB client.conn.connSysdb (\db -> SystemDB.initWorkflow db new (Just maxRecoveryAttempts) Fresh Nothing)
      case initialized of
        Right _  -> pure (Right (handle True workflowText))
        Left err -> resolveEnqueueCollision client.conn shape workflowText err
  where
    handle failMissing workflowText =
      pollingHandle client.conn workflowText failMissing

-- | A handle to a workflow that already exists, by id. Always polling, and
-- never checked: the id is not read until the handle is used. The one
-- handle that waits for a row to appear rather than reporting its absence.
retrieveClientWorkflow :: Client m -> Text -> WorkflowHandle m e
retrieveClientWorkflow client workflowId =
  pollingHandle client.conn workflowId False

-- | A workflow's status, or 'Nothing' if there is no such workflow. The
-- one-shot read behind 'handleStatus', for a caller with an id and no
-- handle.
workflowStatusClient :: Monad m
                     => Client m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe WorkflowStatus))
workflowStatusClient client workflowId = do
  result <- withConnection client.conn (\db -> SystemDB.getWorkflow db workflowId)
  pure $ case result of
    Left err            -> Left err
    Right Nothing       -> Right Nothing
    Right (Just record) -> Right (Just record.workflowRecordStatus)

-- | Send one message to a workflow from outside, the caller unrecorded.
clientSendMessage :: Monad m
                  => Client m -> WorkflowId -> Maybe Topic -> Maybe IdempotencyKey -> SerializedWorkflowValue -> m (Either (TransactError.Error TransactError.EngineOnly) ())
clientSendMessage client destination topic idempotencyKey value = do
  let message =
        SendMessage
          { sendDestinationId = destination,
            sendMessageBody = value,
            sendTopic = topic,
            sendIdempotencyKey = idempotencyKey
          }
  written <- withConnection client.conn (\db -> SystemDB.sendMessage db message ((\(Serialization name) -> name) <$> value.serializedSerialization) Nothing False)
  pure written

-- | Send a batch from outside in one transaction: all or none.
clientSendMessages :: Monad m
                   => Client m -> [SendMessage] -> m (Either (TransactError.Error TransactError.EngineOnly) ())
clientSendMessages client messages = do
  written <- withConnection client.conn (\db -> SystemDB.sendMessages db messages Nothing Nothing False)
  pure written

-- | Read another workflow's event from outside, waiting up to the duration.
clientGetEvent :: (MonadDelay m, MonadTime m)
               => Client m -> WorkflowId -> Text -> Duration -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe SerializedWorkflowValue))
clientGetEvent client workflowId key wait = do
  found <- withConnection client.conn (\db -> SystemDB.getEvent db workflowId key wait Nothing)
  pure $ case found of
    Left err -> Left err
    Right Nothing -> Right Nothing
    Right (Just encoded) ->
      Right (Just (SerializedWorkflowValue encoded.encodedValue (Serialization <$> encoded.encodedSerialization)))

-- | Cancel workflows from outside.
clientCancelWorkflows :: Monad m
                      => Client m -> [WorkflowId] -> Bool -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
clientCancelWorkflows client workflowIds includeChildren = do
  result <- withConnection client.conn (\db -> SystemDB.cancelWorkflows db workflowIds includeChildren Nothing)
  pure result

-- | Resume workflows from outside, optionally onto a queue.
clientResumeWorkflows :: Monad m
                      => Client m -> [WorkflowId] -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
clientResumeWorkflows client workflowIds queueName = do
  result <- withConnection client.conn (\db -> SystemDB.resumeWorkflows db workflowIds queueName Nothing)
  pure result

-- | Delete workflows from outside.
clientDeleteWorkflows :: Monad m
                      => Client m -> [WorkflowId] -> Bool -> m (Either (TransactError.Error TransactError.EngineOnly) Word64)
clientDeleteWorkflows client workflowIds includeChildren = do
  result <- withConnection client.conn (\db -> SystemDB.deleteWorkflows db workflowIds includeChildren Nothing)
  pure result

-- | Fork workflows from outside.
clientForkWorkflows :: Monad m
                    => Client m -> [Fork] -> ForkOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
clientForkWorkflows client forks options = do
  result <- withConnection client.conn (\db -> SystemDB.forkWorkflows db forks options Nothing)
  pure result

-- | The registered application versions.
clientListApplicationVersions :: Monad m
                              => Client m -> m (Either (TransactError.Error TransactError.EngineOnly) [VersionInfo])
clientListApplicationVersions client = do
  result <- withConnection client.conn SystemDB.listApplicationVersions
  pure result

-- | The latest registered application version, optionally scoped to one
-- application. Unscoped reads the global latest, which concurrent tenants
-- can move under you: prefer the scoped form in tests and deployments.
clientLatestApplicationVersion :: Monad m
                               => Client m -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe VersionInfo))
clientLatestApplicationVersion client application = do
  result <- withConnection client.conn (\db -> SystemDB.getLatestApplicationVersion db application)
  pure result

-- | Promote a version by stamping it now, making it the latest: a rollback
-- is a promotion of the older version.
clientPromoteVersion :: (MonadDelay m, MonadTime m)
                     => Client m -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
clientPromoteVersion client version = do
  now <- timestampNow
  result <- withConnection client.conn (\db -> SystemDB.updateApplicationVersionTimestamp db version now Nothing)
  pure result

-- | List workflows by filter from outside.
clientListWorkflows :: Monad m
                    => Client m -> WorkflowFilter -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowRecord])
clientListWorkflows client filters = do
  result <- withConnection client.conn (\db -> SystemDB.listWorkflows db filters Nothing)
  pure result

-- | Reads one workflow's steps, outputs and errors included. Nothing is
-- checkpointed: a client has no step counter of its own to agree with a
-- workflow's, and an id with no row lists nothing rather than failing.
-- Mirrors Rust @Client::list_workflow_steps@.
clientListWorkflowSteps :: Monad m
                        => Client m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) [StepRecord])
clientListWorkflowSteps client workflowId = do
  result <- withConnection client.conn (\db -> SystemDB.listSteps db workflowId True Nothing Nothing Nothing)
  pure result
