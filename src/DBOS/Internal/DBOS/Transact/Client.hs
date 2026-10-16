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
    clientEnqueueWorkflow,
    clientEnqueueWorkflowInTx,
    clientEnqueueWorkflowWith,
    clientRetrieveWorkflow,
    clientWorkflowStatus,
    clientWaitFirst,
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
    clientRewindWorkflows,
    clientUpdateWorkflowAttributes,
    clientSetWorkflowDelay,
    -- * Queues from outside
    clientListQueues,
    clientListQueuedWorkflows,
    clientReadStream,
    clientReadStreamFrom,
    clientRegisterQueue,
    clientRetrieveQueue,
    clientUpdateQueue,
    clientDeleteQueue,
    -- * Versions from outside
    clientListApplicationVersions,
    clientLatestApplicationVersion,
    clientPromoteVersion,
    clientListWorkflows,
    clientListWorkflowSteps,
  )
where

import Data.Aeson (FromJSON, Value)
import Data.List qualified as List
import Data.Map.Strict (Map)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.Prelude
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Postgres.Backend (PostgresSystemDB, Settings (..))
import DBOS.SystemDB.Postgres.Backend qualified as Postgres
import DBOS.SystemDB.Retry (uuidEntropy)
import DBOS.SystemDB.Types (Applications (..), Change (..), Duration, EncodedValue (..), Fork (..), ForkOptions (..), IdempotencyKey (..), NewQueue (..), NewWorkflow (..), OnExistingQueue (..), RewindOptions (..), SendMessage (..), Serialization (..), SerializedWorkflowValue (..), StepRecord, Submission (..), Topic (..), VersionInfo (..), WorkflowDelay, WorkflowFilter (..), WorkflowId (..), WorkflowRecord (..), WorkflowStatus, durationIsZero, newWorkflow, queueIsLegacyPartitioned, sysRewindOptions, timestampNow, validateNewWorkflow, validateSendKeys)
import DBOS.Transact.Config (Serializer (..), databaseUrlEnv, defaultOutcomePollInterval, serializerName)
import DBOS.Transact.Connection (Connection (..), Owner (..), SomeSystemDB (..), generatedWorkflowId, newConnection, releaseConnection, runSystemDB, withConnection)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Handle (WorkflowHandle, WorkflowHandle (workflowId), pollingHandle)
import DBOS.Transact.Identity (validateAppName)
import DBOS.Transact.Logger (nullTracer)
import DBOS.Transact.Management (checkRewindOptions, setWorkflowDelay, updateWorkflowAttributes)
import DBOS.Transact.Queue (Queue, QueueChange (partitionConcurrency, partitionWorkerConcurrency, partitionRateLimit), QueueConflict (..), QueueOptions, changeBothSet, queueChangeToUpdate, queueFromRecord, queueOptionsAfterChange, queueOptionsToNewQueue, reserved, validateQueueOptions)
import DBOS.Transact.Serialization (encodeAttributes)
import DBOS.Transact.Stream (ReadStreamOptions, StreamReader, readStreamOutside)
import DBOS.Transact.Workflow (DuplicationPolicy (..), Enqueue (..), enqueueNew, maxRecoveryAttempts, resolveEnqueueCollision, storedPriority, validateEnqueue)
import Hasql.Transaction qualified as Tx
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
  { conn           :: Connection m,
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
--
-- Two deliberate divergences from Rust @Client::close@, decided 2026-10-15
-- under the oracle rule. First, a closed client stays usable: @Pool.release@
-- is documented to leave the pool usable ("use this function to reset the
-- connections in the pool"), so a later call reconnects rather than failing
-- as unreachable — pinned by the client test. Second, there is no @DBOS
-- client closed@ announcement: the client carries @nullTracer@, so there is
-- no channel to announce through, unlike the engine's shutdown.
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
clientEnqueueWorkflow :: MonadSTM m
                      => Client m -> Text -> Text -> Maybe SerializedWorkflowValue -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
clientEnqueueWorkflow client workflowName queueName input =
  clientEnqueueWorkflowWith client workflowName (enqueueOptionsNew queueName) input

-- | 'clientEnqueueWorkflow', with something to say about how. A call that
-- names no version records none — which the dequeue admits only from the
-- latest registered version — while a named version pins the row to
-- executors running exactly that code. A deduplication collision is refused,
-- unless the policy asks to join the holder instead.
clientEnqueueWorkflowWith :: MonadSTM m
                          => Client m -> Text -> EnqueueOptions -> Maybe SerializedWorkflowValue -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
clientEnqueueWorkflowWith client workflowName options input = do
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

-- | Enqueue a workflow with the caller's writes in one transaction: the
-- status row, the input, and the caller's writes commit together or not
-- at all, mirroring Python @enqueue_in_transaction@. The shape is
-- validated up front — nothing runs on a refusal — and @ReturnExisting@
-- is refused there: attaching would retry the insert, which a
-- caller-owned transaction cannot survive (Python's rule). A stored rival
-- condemns the whole transaction; an identical row is a no-op join. The
-- returned handle is polling, and is only usable once the transaction
-- commits.
clientEnqueueWorkflowInTx :: Client IO -> Text -> Text -> Tx.Transaction () -> EnqueueOptions -> Maybe SerializedWorkflowValue -> IO (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle IO e))
clientEnqueueWorkflowInTx client workflowName operation callerWrites options input =
  case validateEnqueue options.queue of
    Left err -> pure (Left err)
    Right () -> case options.queue.duplicationPolicy of
      ReturnExisting ->
        pure (Left (TransactError.InvalidArgument "enqueue in transaction" "duplication_policy 'return-existing' is not supported by enqueue_in_transaction. Use enqueue instead."))
      _ -> do
        generated <- generatedWorkflowId client.conn
        let shape = options.queue
            workflowText = fromMaybe generated options.workflowId
            serialization = case input >>= (.serializedSerialization) of
              Just (Serialization name) -> Just name
              Nothing -> Just (serializerName client.conn.connSerializer)
            applicationName = case options.appName of
              Just name -> Just name
              Nothing -> client.conn.connAppName
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
        case validateNewWorkflow new of
          Left err -> pure (Left (TransactError.SystemDatabase err))
          Right () -> do
            ownerXid <- Text.pack . UUID.toString <$> UUID.V4.nextRandom
            now <- timestampNow
            composed <- withClientTransaction client operation (callerWrites >> Postgres.enqueueInTx new applicationName ownerXid now)
            pure $ case composed of
              Left err -> Left err
              Right (Left sysErr) -> Left (TransactError.SystemDatabase sysErr)
              Right (Right ()) -> Right (pollingHandle client.conn workflowText True)

-- | A handle to a workflow that already exists, by id. Always polling, and
-- never checked: the id is not read until the handle is used. The one
-- handle that waits for a row to appear rather than reporting its absence.
clientRetrieveWorkflow :: Client m -> Text -> WorkflowHandle m e
clientRetrieveWorkflow client workflowId =
  pollingHandle client.conn workflowId False

-- | A workflow's status, or 'Nothing' if there is no such workflow. The
-- one-shot read behind 'handleStatus', for a caller with an id and no
-- handle.
clientWorkflowStatus :: Monad m
                     => Client m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe WorkflowStatus))
clientWorkflowStatus client workflowId = do
  result <- withConnection client.conn (\db -> SystemDB.getWorkflow db workflowId)
  pure $ case result of
    Left err            -> Left err
    Right Nothing       -> Right Nothing
    Right (Just record) -> Right (Just record.workflowRecordStatus)

-- | Wait for the first of a set of handles to settle and hand back the
-- winner. Mirrors Python @wait_first@: the set must be non-empty and
-- duplicate-free, since the answer names one handle out of the set. An id
-- naming no row is waited for, not reported, like every first-wait.
clientWaitFirst :: (MonadDelay m, MonadTime m)
                => Client m -> [WorkflowHandle m e] -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
clientWaitFirst client handles
  | null handles = pure (Left (TransactError.InvalidArgument "wait_first" "handles must not be empty"))
  | List.nub ids /= ids = pure (Left (TransactError.InvalidArgument "wait_first" "handles must not contain duplicate workflow IDs"))
  | otherwise = do
      winner <- withConnection client.conn (\db -> SystemDB.awaitFirstWorkflowId db (map WorkflowId ids) client.conn.connOutcomePollInterval)
      pure $ case winner of
        Left err -> Left err
        Right (WorkflowId text) -> Right (clientRetrieveWorkflow client text)
  where
    ids = [h.workflowId | h <- handles]

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

-- | Rewind a workflow from outside and hand back its handle: the
-- system-database rewind only, with no datasource checkpoint deletion —
-- rewound datasource transactions are not re-executed through this path.
-- For workflows that use datasources, rewind from a launched instance
-- instead. Mirrors Rust @Client::rewind_with@, whose polling handle this
-- builds through 'clientRetrieveWorkflow'.
clientRewindWorkflows :: Monad m
                      => Client m -> WorkflowId -> RewindOptions -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
clientRewindWorkflows client workflowId@(WorkflowId workflowText) options =
  case checkRewindOptions options of
    Left err -> pure (Left err)
    Right () -> do
      result <- withConnection client.conn (\db -> SystemDB.rewindWorkflow db workflowId options.rewindStartStep (sysRewindOptions options) Nothing)
      case result of
        Left err -> pure (Left err)
        Right () -> pure (Right (clientRetrieveWorkflow client workflowText))

-- | Replace a workflow's attributes from outside, or clear them when
-- given 'Nothing'. The write lives in 'Management', as the engine's own
-- update does; the backend replaces rather than merges (Rust parity —
-- Python's @merge@ has no counterpart in either).
clientUpdateWorkflowAttributes :: Monad m
                               => Client m -> WorkflowId -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
clientUpdateWorkflowAttributes client workflowId attributes =
  updateWorkflowAttributes client.conn workflowId attributes

-- | Move a delayed workflow's release from outside.
clientSetWorkflowDelay :: Monad m
                       => Client m -> WorkflowId -> WorkflowDelay -> m (Either (TransactError.Error TransactError.EngineOnly) ())
clientSetWorkflowDelay client workflowId delay =
  setWorkflowDelay client.conn workflowId delay

-- | Every queue this client's application can see: its own, plus the
-- unclaimed ones. A nameless client sees every queue. Mirrors the oracle's
-- reader, which takes no application: passing no scope lets the backend
-- fall back to the connection's own application name.
clientListQueues :: Monad m
                 => Client m -> m (Either (TransactError.Error TransactError.EngineOnly) [Queue])
clientListQueues client = do
  result <- withConnection client.conn (\db -> SystemDB.listQueues db Unset)
  pure (fmap (map queueFromRecord) result)

-- | Register a queue from outside, or report the one already registered
-- under the name. Mirrors Rust @Connection::register_queue@ with
-- @app_version = None@: a client runs none of the application's code, so
-- there is no version to weigh against the registered ones and
-- 'UpdateIfLatestVersion' is refused with 'ErrorConfig' — while
-- 'AlwaysUpdate' (Python's default) and 'NeverUpdate' write through. The
-- row is re-read after the write, as Python re-reads it.
clientRegisterQueue :: Monad m
                    => Client m -> Text -> QueueOptions -> QueueConflict -> m (Either (TransactError.Error TransactError.EngineOnly) Queue)
clientRegisterQueue client queueName options conflict = case reserved queueName of
  Just refusal -> pure (Left refusal)
  Nothing -> case validateQueueOptions queueName options of
    Left err -> pure (Left err)
    Right () -> case conflict of
      UpdateIfLatestVersion ->
        pure
          ( Left
              ( TransactError.ErrorConfig
                  ( "registering queue `" <> queueName <> "`: `QueueConflict::UpdateIfLatestVersion` needs an application version to compare against, and a client has none; ask for `AlwaysUpdate` or `NeverUpdate`"
                  )
              )
          )
      AlwaysUpdate -> upsert UpdateExisting
      NeverUpdate -> upsert LeaveExisting
  where
    upsert policy = do
      let new = (queueOptionsToNewQueue queueName options) {newQueueApplicationName = client.conn.connAppName}
      inserted <- withConnection client.conn (\db -> SystemDB.upsertQueue db new policy)
      case inserted of
        Left err -> pure (Left err)
        Right _ -> do
          result <- withConnection client.conn (\db -> SystemDB.getQueue db queueName)
          pure $ case result of
            Left err -> Left err
            Right Nothing -> Left (TransactError.ErrorConfig ("queue `" <> queueName <> "` was not returned after registration"))
            Right (Just record) -> Right (queueFromRecord record)

-- | The queue registered under a name from outside, or 'Nothing'.
clientRetrieveQueue :: Monad m
                    => Client m -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe Queue))
clientRetrieveQueue client queueName = do
  result <- withConnection client.conn (\db -> SystemDB.getQueue db queueName)
  pure (fmap (fmap queueFromRecord) result)

-- | Change a registered queue's limits from outside, leaving what the
-- change does not name. The same guards as the engine path: the reserved
-- name, the both-spellings change, the legacy-partitioned row, and the
-- merged validation — a client update cannot leave a queue in a state a
-- registration would have refused.
clientUpdateQueue :: Monad m
                  => Client m -> Text -> QueueChange -> m (Either (TransactError.Error TransactError.EngineOnly) Queue)
clientUpdateQueue client queueName change = case reserved queueName of
  Just refusal -> pure (Left refusal)
  Nothing -> case changeBothSet queueName change of
    Just refusal -> pure (Left refusal)
    Nothing -> do
      stored <- withConnection client.conn (\db -> SystemDB.getQueue db queueName)
      case stored of
        Left err -> pure (Left err)
        Right Nothing -> pure (Left (TransactError.ErrorConfig ("queue `" <> queueName <> "` is not registered")))
        Right (Just record) -> do
          let touchesPartition =
                change.partitionConcurrency /= Leave
                  || change.partitionWorkerConcurrency /= Leave
                  || change.partitionRateLimit /= Leave
          if touchesPartition && queueIsLegacyPartitioned record
            then
              pure
                ( Left
                    ( TransactError.ErrorConfig
                        ( "queue `" <> queueName <> "`: this queue is registered with the deprecated `partition_queue` flag, under which its queue-wide limits already apply per partition; re-register it with the per-partition limits instead"
                        )
                    )
                )
            else do
              let currentQueue = queueFromRecord record
                  desired = queueOptionsAfterChange change currentQueue
              case validateQueueOptions queueName desired of
                Left err -> pure (Left err)
                Right () -> do
                  let update = queueChangeToUpdate change currentQueue
                  updated <- withConnection client.conn (\db -> SystemDB.updateQueue db queueName update (\_ _ -> Right ()))
                  pure (queueFromRecord <$> updated)

-- | Remove a queue's registration from outside. Removing one that is not
-- registered is not an error.
clientDeleteQueue :: Monad m
                  => Client m -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
clientDeleteQueue client queueName = case reserved queueName of
  Just refusal -> pure (Left refusal)
  Nothing -> do
    result <- withConnection client.conn (\db -> SystemDB.deleteQueue db queueName)
    pure (() <$ result)

-- | The registered application versions.
clientListApplicationVersions :: Monad m
                              => Client m -> m (Either (TransactError.Error TransactError.EngineOnly) [VersionInfo])
clientListApplicationVersions client = do
  withConnection client.conn SystemDB.listApplicationVersions

-- | The latest registered application version, scoped to this client's
-- application, or global when the client is nameless. Mirrors the oracle's
-- reader, which takes no application: reading claims nothing, so it has
-- nothing to say a name about. Passing no scope lets the backend fall back
-- to the connection's own application name.
clientLatestApplicationVersion :: Monad m
                               => Client m -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe VersionInfo))
clientLatestApplicationVersion client = do
  result <- withConnection client.conn (\db -> SystemDB.getLatestApplicationVersion db Nothing)
  pure result

-- | Promote a version by stamping it now, making it the latest: a rollback
-- is a promotion of the older version.
clientPromoteVersion :: (MonadDelay m, MonadTime m)
                     => Client m -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
clientPromoteVersion client version = do
  now <- timestampNow
  result <- withConnection client.conn (\db -> SystemDB.updateApplicationVersionTimestamp db version now Nothing)
  pure result

-- | List queued workflows by filter from outside: the same filters as
-- 'clientListWorkflows', with @queues-only@ forced — only rows carrying a
-- queue are listed. Mirrors Python @list_queued_workflows@, which is
-- 'list_workflows' with @queues_only=True@.
clientListQueuedWorkflows :: Monad m
                          => Client m -> WorkflowFilter -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowRecord])
clientListQueuedWorkflows client filters = do
  result <- withConnection client.conn (\db -> SystemDB.listWorkflows db (filters {workflowFilterQueuesOnly = True}) Nothing)
  pure result

-- | List workflows by filter from outside.
clientListWorkflows :: Monad m
                    => Client m -> WorkflowFilter -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowRecord])
clientListWorkflows client filters = do
  result <- withConnection client.conn (\db -> SystemDB.listWorkflows db filters Nothing)
  pure result

-- | Read one value's stream from the beginning, from outside the
-- application. Nothing is checkpointed: a client has no workflow to record
-- a read against. Mirrors Rust @Client::read_stream@.
clientReadStream :: (FromJSON value, MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m)
                 => Client m -> WorkflowId -> Text -> ReadStreamOptions -> m (StreamReader (TransactError.Error TransactError.EngineOnly) m value)
clientReadStream client workflowId key options = readStreamOutside client.conn workflowId key 0 options

-- | 'clientReadStream', starting at an offset rather than the beginning.
clientReadStreamFrom :: (FromJSON value, MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m)
                     => Client m -> WorkflowId -> Text -> Int -> ReadStreamOptions -> m (StreamReader (TransactError.Error TransactError.EngineOnly) m value)
clientReadStreamFrom client = readStreamOutside client.conn

-- | Reads one workflow's steps, outputs and errors included. Nothing is
-- checkpointed: a client has no step counter of its own to agree with a
-- workflow's, and an id with no row lists nothing rather than failing.
-- Mirrors Rust @Client::list_workflow_steps@.
clientListWorkflowSteps :: Monad m
                        => Client m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) [StepRecord])
clientListWorkflowSteps client workflowId = do
  result <- withConnection client.conn (\db -> SystemDB.listSteps db workflowId True Nothing Nothing Nothing)
  pure result
