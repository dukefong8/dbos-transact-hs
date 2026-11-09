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
    queueRequest,
    refuseEmptyQueueName,
    clientEnqueueWorkflow,
    clientEnqueueWorkflowInTx,
    clientEnqueueWorkflowWith,
    clientEnqueueWorkflowWithDebounce,
    refuseNamelessWorkflow,
    clientRetrieveWorkflow,
    clientWorkflowStatus,
    clientWorkflowRecord,
    clientWaitFirst,
    -- * Messages from outside
    clientSendMessage,
    clientSendMessages,
    clientSendMessageInTx,
    clientSendMessagesInTx,
    clientGetEvent,
    clientGetEventSerialized,
    -- * Lifecycle from outside
    clientCancelWorkflows,
    clientResumeWorkflows,
    clientDeleteWorkflows,
    clientForkWorkflows,
    clientRewindWorkflows,
    clientUpdateWorkflowAttributes,
    clientMergeWorkflowAttributes,
    clientPromoteVersionFor,
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
    clientListAppVersions,
    clientLatestAppVersion,
    clientPromoteVersion,
    clientListWorkflows,
    clientListWorkflowSteps,
    clientListWorkflowStepsWith,
    clientListWorkflowStepsSerialized,
    clientListWorkflowStepsSerializedWith,
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
import DBOS.SystemDB.Types (AppScope (..), Change (..), Duration, EncodedValue (..), Fork (..), ForkOptions (..), IdempotencyKey (..), IdleTransactionTimeout (..), NewQueue (..), NewWorkflow (..), OnExistingQueue (..), PollingConcurrency (PollingDefault), RewindOptions (..), EncodedSendMessage (..), Serialization (..), SerializedWorkflowValue (..), StepRecord (..), Topic (..), VersionInfo (..), WorkflowDelay, WorkflowFilter (..), WorkflowId (..), WorkflowIdReusePolicy (..), QualifiedWorkflowName, qualifiedName, qualifiedClassName, qualifiedConfigName, WorkflowRecord (..), WorkflowStatus, durationIsZero, idleTimeoutValidate, newWorkflow, queueIsLegacyPartitioned, sysRewindOptions, timestampNow, validateNewWorkflow, validateSendKeys)
import DBOS.Transact.Config (Serializer (..), systemDatabaseUrlEnv, defaultOutcomePollInterval, serializerName)
import DBOS.Transact.Connection (Connection (..), Owner (..), SomeSystemDB (..), generatedWorkflowId, newConnection, releaseConnection, withConnection)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Handle (WorkflowHandle, WorkflowHandle (workflowId), pollingHandle)
import DBOS.Transact.Identity (validateAppName)
import DBOS.Transact.Logger (nullTracer)
import DBOS.Transact.Management (ListWorkflowStepsOptions (..), StepInfo, checkRewindOptions, defaultListWorkflowStepsOptions, latestAppVersion, listAppVersions, mergeWorkflowAttributes, promoteAppVersion, promoteAppVersionFor, setWorkflowDelay, stepInfoFromRecord, updateWorkflowAttributes)
import DBOS.Transact.Queue (Queue, QueueChange (partitionConcurrency, partitionWorkerConcurrency, partitionRateLimit), QueueConflict (..), QueueOptions, changeBothSet, queueChangeToUpdate, queueFromRecord, queueOptionsAfterChange, queueOptionsToNewQueue, reserved, validateQueueOptions)
import DBOS.Transact.Serialization (CodecError (..), WorkflowInput (..), codecErrorText, codecUnknownTag, decodeWorkflowValue, encodeAttributes, unWorkflowInput)
import DBOS.Transact.Stream (ReadStreamOptions, StreamReader, readStreamOutside)
import DBOS.Transact.Workflow (DuplicationPolicy (..), QueueRequest (..), Submitted (..), queueRequestNew, initOrJoin, storedPriority, validateStart)
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
    -- | Accepted for Rust @ClientConfig@ fidelity. The port polls and has no
    -- listener, so this field is inert. Rust defaults it to false (#101): a
    -- listener pointed at executors that do not push waits out the
    -- 60-second fallback.
    useListenNotify        :: Bool,
    pollingConcurrency     :: PollingConcurrency,
    outcomePollInterval    :: Maybe Duration,
    notificationCoalesce   :: Maybe Duration,
    idleTransactionTimeout :: IdleTransactionTimeout
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
      serializer = PortableJson,
      useListenNotify = False,
      pollingConcurrency = PollingDefault,
      outcomePollInterval = Nothing,
      notificationCoalesce = Nothing,
      idleTransactionTimeout = Default
    }

-- | 'clientConfigNew', taking the database URL from @DBOS_SYSTEM_DATABASE_URL@.
-- The URL is the only thing it reads; 'app_name' stays 'Nothing'.
clientConfigFromEnv :: IO ClientConfig
clientConfigFromEnv = do
  url <- lookupEnv (Text.unpack systemDatabaseUrlEnv)
  pure (clientConfigNew (maybe "" Text.pack url))

-- | Checks what can be checked before anything is connected.
validateClientConfig :: ClientConfig -> Either (TransactError.Error TransactError.EngineOnly) ()
validateClientConfig config
  | Text.null config.databaseUrl =
      Left
        ( TransactError.ErrorConfig
            ( "no database URL: set `database_url`, or the "
                <> systemDatabaseUrlEnv
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
  | Left err <- idleTimeoutValidate config.idleTransactionTimeout =
      Left (TransactError.ErrorConfig (SystemDBError.renderError err))
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
        Left err -> pure (Left (TransactError.fromSystemDB err))
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
          Postgres.configIdleTransactionTimeout = config.idleTransactionTimeout,
          Postgres.configSettings =
            (Postgres.defaultSettings :: Settings)
              { settingsSchema = config.schema,
                settingsExecutorId = Nothing,
                settingsAppName = config.appName,
                settingsPollingConcurrency = config.pollingConcurrency,
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
    pure (either (Left . TransactError.fromSystemDB) Right result)

-- | Send a batch with the caller's writes in one transaction: the writes run
-- first, then the sends, all in the caller's commit. The batch is validated
-- up front — nothing runs on a refusal — while foreign-key failures abort
-- through the insert, as Python's raise aborts the caller's transaction.
-- No step is recorded, like every client send.
clientSendMessagesInTx :: Client IO -> Text -> Tx.Transaction () -> [EncodedSendMessage] -> Maybe Text -> [Text] -> Bool -> IO (Either (TransactError.Error TransactError.EngineOnly) ())
clientSendMessagesInTx client operation callerWrites messages serialization fallbacks sendToForks =
  case validateSendKeys messages of
    Left err -> pure (Left (TransactError.fromSystemDB err))
    Right ()
      | length fallbacks /= length messages ->
          pure (Left (TransactError.fromSystemDB (SystemDBError.invalidInput "fallbacks" "one fallback id per message")))
      | otherwise -> runExceptT $ do
          composed <- ExceptT (withClientTransaction client operation (callerWrites >> Postgres.sendBatchInTx messages serialization fallbacks sendToForks))
          -- The piece condemns on its own refusal, so a 'Left' here already
          -- rolled the caller writes back; map it out rather than swallowing it.
          ExceptT (pure (first TransactError.fromSystemDB composed))

-- | Send one message with the caller's writes in one transaction, as
-- 'clientSendMessagesInTx' sends a batch.
clientSendMessageInTx :: Client IO -> Text -> Tx.Transaction () -> EncodedSendMessage -> Maybe Text -> Text -> Bool -> IO (Either (TransactError.Error TransactError.EngineOnly) ())
clientSendMessageInTx client operation callerWrites message serialization fallback sendToForks =
  clientSendMessagesInTx client operation callerWrites [message] serialization [fallback] sendToForks

-- | What a client may say about an enqueue, beyond the input. Flat, as
-- Python's and Java's @EnqueueOptions@ are: what to ask of the queue sits
-- beside everything else, so an enqueue by name is one type. A start says
-- the same queue-shaped things in a 'QueueRequest', under
-- 'StartOptions' 'startQueue', because there the queue is optional and
-- those settings exist only with one. Apart from the queue's own name,
-- the two spell each setting the same way, and are checked by the same
-- rules. Mirrors Rust @EnqueueOptions@.
data EnqueueOptions = EnqueueOptions
  { queueName :: Text,
    deduplicationId :: Maybe Text,
    duplicationPolicy :: DuplicationPolicy,
    priority :: Maybe Word32,
    partitionKey :: Maybe Text,
    delay :: Maybe Duration,
    workflowId :: Maybe Text,
    workflowIdReusePolicy :: WorkflowIdReusePolicy,
    appName :: Maybe Text,
    appVersion :: Maybe Text,
    timeout :: Maybe Duration,
    attributes :: Maybe (Map Text Value)
  }
  deriving stock (Eq, Show)

-- | The refusal a client enqueue of a nameless workflow gets: nothing could
-- ever dequeue the row. A client takes the name from its caller; an
-- executor's starts take theirs from a registration, which refuses it.
-- Mirrors Rust @EnqueueOptions::validate@.
refuseNamelessWorkflow :: QualifiedWorkflowName -> Text -> Maybe (TransactError.Error e)
refuseNamelessWorkflow workflow queueName
  | Text.null (qualifiedName workflow) =
      Just
        ( TransactError.InvalidArgument
            "enqueue a workflow"
            ("queue `" <> queueName <> "`: `workflow` must not be empty")
        )
  | otherwise = Nothing

-- | Refuses an empty queue name up front, naming the field the caller
-- wrote — rather than leaving the insert to refuse the column under the
-- name a start's request gives it. Mirrors the @queue_name@ arm of Rust
-- @EnqueueOptions::validate@.
refuseEmptyQueueName :: Text -> Maybe (TransactError.Error e)
refuseEmptyQueueName queueName
  | Text.null queueName =
      Just (TransactError.InvalidArgument "enqueue a workflow" "`queue_name` must not be empty")
  | otherwise = Nothing

-- | A plain enqueue onto a queue, asking for nothing else.
enqueueOptionsNew :: Text -> EnqueueOptions
enqueueOptionsNew queueName =
  let request = queueRequestNew queueName
   in EnqueueOptions
        { queueName = request.name,
          deduplicationId = request.deduplicationId,
          duplicationPolicy = request.duplicationPolicy,
          priority = request.priority,
          partitionKey = request.partitionKey,
          delay = request.delay,
          workflowId = Nothing,
          workflowIdReusePolicy = WorkflowIdReturnExisting,
          appName = Nothing,
          appVersion = Nothing,
          timeout = Nothing,
          attributes = Nothing
        }

-- | The queue-shaped half, as a start would say it, which is what the row
-- and the shared checks are written against. Mirrors Rust
-- @EnqueueOptions::queue_request@.
queueRequest :: EnqueueOptions -> QueueRequest
queueRequest options =
  (queueRequestNew options.queueName)
    { deduplicationId = options.deduplicationId,
      duplicationPolicy = options.duplicationPolicy,
      priority = options.priority,
      partitionKey = options.partitionKey,
      delay = options.delay
    }

-- | Enqueues a workflow, onto a queue, and hands back a polling
-- handle to it. The workflow is the name it was registered under; a free
-- function's name alone converts into one. Mirrors @Client::enqueue@.
clientEnqueueWorkflow :: MonadSTM m
                      => Client m -> QualifiedWorkflowName -> Text -> Maybe WorkflowInput -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
clientEnqueueWorkflow client workflow queueName input =
  clientEnqueueWorkflowWith client workflow (enqueueOptionsNew queueName) input

-- | 'clientEnqueueWorkflow', with something to say about how. A call that
-- names no version records none — which the dequeue admits only from the
-- latest registered version — while a named version pins the row to
-- executors running exactly that code. A deduplication collision is refused,
-- unless the policy asks to join the holder instead.
clientEnqueueWorkflowWith :: MonadSTM m
                          => Client m -> QualifiedWorkflowName -> EnqueueOptions -> Maybe WorkflowInput -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
clientEnqueueWorkflowWith client workflow options input =
  clientEnqueueWorkflowWithDebounce client workflow options Nothing input

-- | 'clientEnqueueWorkflowWith', stamping the fresh row debounced when the
-- call is a debounce's fresh creation. The flag is two-level, as the
-- oracle's @Option<Debounced>@ is: 'Nothing' is a plain enqueue, while
-- 'Just' (even of no cap) marks a debounced creation whose release frees
-- the key — a debounce with no hold cap still debounces. Only the
-- debouncer passes 'Just'. Mirrors the oracle's @debounce@ argument to
-- @enqueue_encoded@.
clientEnqueueWorkflowWithDebounce :: MonadSTM m
                                   => Client m -> QualifiedWorkflowName -> EnqueueOptions -> Maybe (Maybe Duration) -> Maybe WorkflowInput -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
clientEnqueueWorkflowWithDebounce client workflow options debounceCreation input =
  case refuseNamelessWorkflow workflow options.queueName of
    Just err -> pure (Left err)
    Nothing -> case refuseEmptyQueueName options.queueName of
      Just err -> pure (Left err)
      Nothing -> case validateStart (Just (queueRequest options)) options.timeout options.workflowId options.workflowIdReusePolicy of
        Left err -> pure (Left err)
        Right () -> do
          generated <- generatedWorkflowId client.conn
          let request = queueRequest options
              workflowText = fromMaybe generated options.workflowId
              serialization = case rawInput >>= (.serializedSerialization) of
                Just (Serialization name) -> Just name
                Nothing                   -> Just (serializerName client.conn.connSerializer)
              applicationName = case options.appName of
                Just name -> Just name
                Nothing   -> client.conn.connAppName
              new =
                (newWorkflow workflowText)
                { newWorkflowName = Just (qualifiedName workflow),
                  newWorkflowClassName = qualifiedClassName workflow,
                  newWorkflowConfigName = qualifiedConfigName workflow,
                  newWorkflowInput = (.serializedText) <$> rawInput,
                  newWorkflowSerialization = serialization,
                  newWorkflowQueueName = Just options.queueName,
                  newWorkflowDeduplicationId = options.deduplicationId,
                  newWorkflowPriority = storedPriority request,
                  newWorkflowQueuePartitionKey = options.partitionKey,
                  newWorkflowDelay = options.delay,
                  newWorkflowIsDebounced = isJust debounceCreation,
                  newWorkflowDebounceTimeout = fromMaybe Nothing debounceCreation,
                  newWorkflowTimeout = options.timeout,
                  newWorkflowDeadline = Nothing,
                  newWorkflowAttributes = encodeAttributes options.attributes,
                  newWorkflowExecutorId = Nothing,
                  newWorkflowAppName = applicationName,
                  newWorkflowAppVersion = options.appVersion
                }
          started <- initOrJoin client.conn new (Just request) options.workflowIdReusePolicy Nothing
          pure $ case started of
            Right (SubmittedCreated _)     -> Right (handle True workflowText)
            Right (SubmittedJoined holder) -> Right holder
            Left err                       -> Left (TransactError.refusedStart (Just options.queueName) err)
  where
    rawInput = unWorkflowInput <$> input
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
clientEnqueueWorkflowInTx :: Client IO -> QualifiedWorkflowName -> Text -> Tx.Transaction () -> EnqueueOptions -> Maybe WorkflowInput -> IO (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle IO e))
clientEnqueueWorkflowInTx client workflow operation callerWrites options input =
  case refuseNamelessWorkflow workflow options.queueName of
    Just err -> pure (Left err)
    Nothing -> case refuseEmptyQueueName options.queueName of
      Just err -> pure (Left err)
      Nothing -> case validateStart (Just (queueRequest options)) options.timeout options.workflowId options.workflowIdReusePolicy of
        Left err -> pure (Left err)
        Right () -> case options.duplicationPolicy of
          ReturnExisting ->
            pure (Left (TransactError.InvalidArgument "enqueue in transaction" "duplication_policy 'return-existing' is not supported by enqueue_in_transaction. Use enqueue instead."))
          _ -> do
            generated <- generatedWorkflowId client.conn
            let request = queueRequest options
                rawInput = unWorkflowInput <$> input
                workflowText = fromMaybe generated options.workflowId
                serialization = case rawInput >>= (.serializedSerialization) of
                  Just (Serialization name) -> Just name
                  Nothing -> Just (serializerName client.conn.connSerializer)
                applicationName = case options.appName of
                  Just name -> Just name
                  Nothing -> client.conn.connAppName
                new =
                  (newWorkflow workflowText)
                    { newWorkflowName = Just (qualifiedName workflow),
                      newWorkflowClassName = qualifiedClassName workflow,
                      newWorkflowConfigName = qualifiedConfigName workflow,
                      newWorkflowInput = (.serializedText) <$> rawInput,
                      newWorkflowSerialization = serialization,
                      newWorkflowQueueName = Just options.queueName,
                      newWorkflowDeduplicationId = options.deduplicationId,
                      newWorkflowPriority = storedPriority request,
                      newWorkflowQueuePartitionKey = options.partitionKey,
                      newWorkflowDelay = options.delay,
                      newWorkflowTimeout = options.timeout,
                      newWorkflowDeadline = Nothing,
                      newWorkflowAttributes = encodeAttributes options.attributes,
                      newWorkflowExecutorId = Nothing,
                      newWorkflowAppName = applicationName,
                      newWorkflowAppVersion = options.appVersion
                    }
            case validateNewWorkflow new of
              Left err -> pure (Left (TransactError.refusedStart (Just options.queueName) err))
              Right () -> do
                creatorXid <- Text.pack . UUID.toString <$> UUID.V4.nextRandom
                now <- timestampNow
                composed <- withClientTransaction client operation (callerWrites >> Postgres.enqueueInTx new applicationName options.workflowIdReusePolicy creatorXid now)
                pure $ case composed of
                  Left err -> Left err
                  Right (Left sysErr) -> Left (TransactError.refusedStart (Just options.queueName) sysErr)
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

-- | The full row, matching Python's @retrieve_workflow(...).get_status()@ and
-- @get_workflow@; the payload columns stay raw with their tags.
clientWorkflowRecord :: Monad m
                     => Client m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe WorkflowRecord))
clientWorkflowRecord client workflowId =
  withConnection client.conn (\db -> SystemDB.getWorkflow db workflowId)

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
        EncodedSendMessage
          { sendDestinationId = destination,
            sendMessageBody = value,
            sendTopic = topic,
            sendIdempotencyKey = idempotencyKey
          }
  written <- withConnection client.conn (\db -> SystemDB.sendMessage db message ((\(Serialization name) -> name) <$> value.serializedSerialization) Nothing Nothing False)
  pure written

-- | Send a batch from outside in one transaction: all or none.
clientSendMessages :: Monad m
                   => Client m -> [EncodedSendMessage] -> m (Either (TransactError.Error TransactError.EngineOnly) ())
clientSendMessages client messages = do
  written <- withConnection client.conn (\db -> SystemDB.sendMessages db messages Nothing Nothing Nothing False)
  pure written

-- | Read another workflow's event from outside, waiting up to the duration.
-- The raw, tag-preserving form: nothing is decoded, so a foreign tag is
-- handed back for the caller to inspect.
clientGetEventSerialized :: (MonadDelay m, MonadTime m)
                         => Client m -> WorkflowId -> Text -> Duration -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe SerializedWorkflowValue))
clientGetEventSerialized client workflowId key wait = do
  found <- withConnection client.conn (\db -> SystemDB.getEvent db workflowId key wait Nothing)
  pure $ case found of
    Left err -> Left err
    Right Nothing -> Right Nothing
    Right (Just encoded) ->
      Right (Just (SerializedWorkflowValue encoded.encodedValue (Serialization <$> encoded.encodedSerialization)))

-- | 'clientGetEventSerialized' decoded into the caller's type by the row's
-- tag — the Rust/TypeScript client shape. A foreign tag is reported as
-- @UnknownSerialization@; a bad body as @Deserialization@, matching the
-- in-workflow read.
clientGetEvent :: (FromJSON value, MonadDelay m, MonadTime m)
               => Client m -> WorkflowId -> Text -> Duration -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe value))
clientGetEvent client workflowId key wait = do
  found <- clientGetEventSerialized client workflowId key wait
  pure $ case found of
    Left err -> Left err
    Right Nothing -> Right Nothing
    Right (Just stored) ->
      case decodeWorkflowValue "event value" (Just stored) of
        Left err -> Left (case codecUnknownTag err of
          Just tag -> TransactError.UnknownSerialization {name = tag}
          Nothing -> TransactError.Deserialization "event value" (codecErrorText err))
        Right value -> Right (Just value)

-- | Cancel workflows from outside.
clientCancelWorkflows :: Monad m
                      => Client m -> [WorkflowId] -> Bool -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
clientCancelWorkflows client workflowIds includeChildren = do
  result <- withConnection client.conn (\db -> SystemDB.cancelWorkflows db workflowIds includeChildren Nothing Nothing)
  pure result

-- | Resume workflows from outside, optionally onto a queue.
clientResumeWorkflows :: Monad m
                      => Client m -> [WorkflowId] -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
clientResumeWorkflows client workflowIds queueName = do
  result <- withConnection client.conn (\db -> SystemDB.resumeWorkflows db workflowIds queueName Nothing Nothing)
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
-- update does.
clientUpdateWorkflowAttributes :: Monad m
                               => Client m -> WorkflowId -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
clientUpdateWorkflowAttributes client workflowId attributes =
  updateWorkflowAttributes client.conn workflowId attributes

-- | Merge keys into a workflow's attributes from outside, in one
-- statement. The write lives in 'Management', as the engine's own merge
-- does.
clientMergeWorkflowAttributes :: Monad m
                              => Client m -> WorkflowId -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
clientMergeWorkflowAttributes client workflowId attributes =
  mergeWorkflowAttributes client.conn workflowId attributes

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
      let new = (queueOptionsToNewQueue queueName options) {newQueueAppName = client.conn.connAppName}
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
clientListAppVersions :: Monad m
                              => Client m -> m (Either (TransactError.Error TransactError.EngineOnly) [VersionInfo])
clientListAppVersions client = listAppVersions client.conn

-- | The latest registered application version, scoped to this client's
-- application, or global when the client is nameless. Mirrors the oracle's
-- reader, which takes no application: reading claims nothing, so it has
-- nothing to say a name about. Passing no scope lets the backend fall back
-- to the connection's own application name.
clientLatestAppVersion :: Monad m
                               => Client m -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe VersionInfo))
clientLatestAppVersion client = latestAppVersion client.conn

-- | Promote an already-registered version to be the latest, stamping it
-- past every other version's on the database's clock: promoting an older
-- one is a rollback. A name that matches nothing is not an error. Mirrors
-- Rust @Client::set_latest_app_version@, which promotes through the same
-- shared body as the application's own call.
clientPromoteVersion :: Monad m
                     => Client m -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
clientPromoteVersion client version = promoteAppVersion client.conn version

-- | Promote a version for the named application: how one operator tool
-- rolls a peer application back. Mirrors Rust
-- @Client::set_latest_app_version_for@.
clientPromoteVersionFor :: Monad m
                        => Client m -> Text -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
clientPromoteVersionFor client version application = promoteAppVersionFor client.conn version application

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

-- | Reads one workflow's steps with their outputs and errors decoded into
-- the typed 'StepInfo' view. The raw Rust-shaped listing remains available
-- internally as 'clientListWorkflowStepsSerialized'.
clientListWorkflowSteps :: Monad m
                        => Client m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) [StepInfo])
clientListWorkflowSteps client workflowId =
  clientListWorkflowStepsWith client workflowId defaultListWorkflowStepsOptions

-- | 'clientListWorkflowSteps' with the listing's options: choose whether to
-- read the payload columns, and page in step order.
clientListWorkflowStepsWith :: Monad m
                            => Client m -> WorkflowId -> ListWorkflowStepsOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [StepInfo])
clientListWorkflowStepsWith client workflowId options = do
  listed <- clientListWorkflowStepsSerializedWith client workflowId options
  pure (map (stepInfoFromRecord options) <$> listed)

-- | The raw, tag-preserving step listing: outputs stay encoded and carry
-- their @serialization@ tag. Internal to the port; apps use
-- 'clientListWorkflowSteps'.
clientListWorkflowStepsSerialized :: Monad m
                                  => Client m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) [StepRecord])
clientListWorkflowStepsSerialized client workflowId =
  clientListWorkflowStepsSerializedWith client workflowId defaultListWorkflowStepsOptions

-- | The raw listing with options. Internal to the port.
clientListWorkflowStepsSerializedWith :: Monad m
                                      => Client m -> WorkflowId -> ListWorkflowStepsOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [StepRecord])
clientListWorkflowStepsSerializedWith client workflowId options = do
  result <- withConnection client.conn (\db -> SystemDB.listSteps db workflowId options.loadOutput options.limit options.offset Nothing)
  pure result
