{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The client: reaching a DBOS application from outside it. Mirrors Rust
-- @client.rs@: a client talks only to the system database — it enqueues
-- workflows, reads their status, and waits on their results — without
-- registering a workflow, running one, or holding any application code.
-- It never runs a workflow, never migrates (connect verifies instead), and
-- has no application version of its own.
--
-- Queue, schedule, and version surfaces shared with the application ride the
-- same SystemDB methods; porting them onto 'Client' is L2 follow-up (NOTE).
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
    clientAppName,
    -- * Workflows from outside
    EnqueueOptions (..),
    enqueueOptionsNew,
    enqueueOptionsOn,
    enqueueClientWorkflow,
    enqueueClientWorkflowWith,
    retrieveClientWorkflow,
    workflowStatusClient,
  )
where

import DBOS.Prelude
import Colog.Core.Action (LogAction (..))
import Data.Aeson (Value)
import Data.Map.Strict (Map)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Data.Word (Word)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Postgres (PostgresSystemDB, Settings (..))
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.SystemDB.Retry (uuidEntropy)
import DBOS.SystemDB.Types
  ( Duration,
    NewWorkflow (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    Submission (..),
    WorkflowId (..),
    WorkflowStatus,
    durationIsZero,
    newWorkflow,
  )
import DBOS.Transact.Codec (encodeAttributes)
import DBOS.Transact.Config (Serializer (..), databaseUrlEnv, defaultOutcomePollInterval, serializerName)
import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import DBOS.Transact.Connection (Connection (..), Owner (..), SomeSystemDB (..), closeConnection, generatedWorkflowId, newConnection, runSystemDB)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Handle (WorkflowHandle, pollingHandle)
import DBOS.Transact.Identity (validateAppName)
import DBOS.Transact.Workflow (Enqueue (..), enqueueNew, maxRecoveryAttempts, resolveEnqueueCollision, storedPriority, validateEnqueue)
import System.Environment (lookupEnv)

-- | Everything a 'Client' needs. A separate type from 'Config' rather than
-- a mode of it: a client has no executor id, no application version, no
-- listen set, and no migrate — four fields that must not be set is four
-- fields that should not exist.
data ClientConfig = ClientConfig
  { app_name :: Maybe Text,
    database_url :: Text,
    max_connections :: Word,
    schema :: Text,
    serializer :: Serializer,
    use_listen_notify :: Bool,
    polling_concurrency :: Maybe Word,
    outcome_poll_interval :: Maybe Duration,
    notification_coalesce :: Maybe Duration
  }
  deriving stock (Eq, Show)

-- | A nameless client reaching the database at the URL. Nameless because a
-- name is a claim: set 'app_name' to make it deliberately.
clientConfigNew :: Text -> ClientConfig
clientConfigNew databaseUrl =
  ClientConfig
    { app_name = Nothing,
      database_url = databaseUrl,
      max_connections = 5,
      schema = "dbos",
      serializer = RustSerde,
      use_listen_notify = True,
      polling_concurrency = Nothing,
      outcome_poll_interval = Nothing,
      notification_coalesce = Nothing
    }

-- | 'clientConfigNew', taking the database URL from @DBOS_DATABASE_URL@.
-- The URL is the only thing it reads; 'app_name' stays 'Nothing'.
clientConfigFromEnv :: IO ClientConfig
clientConfigFromEnv = do
  url <- lookupEnv (Text.unpack databaseUrlEnv)
  pure (clientConfigNew (maybe "" Text.pack url))

-- | Checks what can be checked before anything is connected.
validateClientConfig :: ClientConfig -> Either TransactError.Error ()
validateClientConfig config
  | Text.null config.database_url =
      Left
        ( TransactError.ErrorConfig
            ( "no database URL: set `database_url`, or the "
                <> databaseUrlEnv
                <> " environment variable if the configuration came from `ClientConfig::from_env`"
            )
        )
  | Just name <- config.app_name,
    Left err <- validateAppName name =
      Left err
  | Text.null config.schema = Left (TransactError.ErrorConfig "`schema` cannot be empty")
  | config.max_connections == 0 = Left (TransactError.ErrorConfig "`max_connections` cannot be zero")
  | Just interval <- config.outcome_poll_interval,
    durationIsZero interval =
      Left (TransactError.ErrorConfig "`outcome_poll_interval` cannot be zero")
  | otherwise = Right ()

-- | How often a waiting caller looks, resolved.
clientOutcomePollInterval :: ClientConfig -> Duration
clientOutcomePollInterval config =
  maybe defaultOutcomePollInterval id config.outcome_poll_interval

-- | A connection to an application's system database, from outside it. It
-- holds a connection and nothing else — not an executor. Mirrors the
-- oracle's @Client(Arc<Connection>)@.
newtype Client m = Client
  { conn :: Connection m
  }

-- | Connects to the system database. Eager: an unreachable database is
-- reported here rather than on the first call. Migrates nothing, verifies
-- instead: 'acquirePostgresSystemDB' already checks the schema ceiling.
-- The connection's owner is 'OwnerClient', which is what makes a
-- workflow's read through it a plain wait rather than a refused instance.
connectClient :: ClientConfig -> IO (Either TransactError.Error (Client IO))
connectClient config =
  case validateClientConfig config of
    Left err -> pure (Left err)
    Right () -> do
      acquired <- try (Postgres.acquirePostgresSystemDB backendConfig backendLogger) :: IO (Either SystemDBError.Error PostgresSystemDB)
      case acquired of
        Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
        Right backend ->
          bracketOnError (pure backend) Postgres.releasePostgresSystemDB $ \systemDB -> do
            Postgres.activatePostgresSystemDB systemDB
            conn <-
              newConnection
                (SomeSystemDB systemDB)
                config.serializer
                config.app_name
                (clientOutcomePollInterval config)
                OwnerClient
                uuidWorkflowId
                uuidEntropy
            pure (Right (Client conn))
  where
    backendConfig =
      Postgres.Config
        { Postgres.configUrl = config.database_url,
          Postgres.configMaxConnections = fromIntegral config.max_connections,
          Postgres.configSettings =
            (Postgres.defaultSettings :: Settings)
              { settingsSchema = config.schema,
                settingsExecutorId = Nothing,
                settingsApplicationName = config.app_name,
                settingsPollingConcurrency = fromIntegral <$> config.polling_concurrency,
                settingsNotificationCoalesce = config.notification_coalesce
              }
        }
    backendLogger = LogAction (const (pure ()))
    uuidWorkflowId = Text.pack . UUID.toString <$> UUID.V4.nextRandom

-- | Closes the connection: the notifier stops before the pool closes, so
-- nothing queued is lost to a closed pool.
closeClient :: Monad m => Client m -> m ()
closeClient client = closeConnection client.conn

-- | The application this client acts for, or 'Nothing' if nameless.
clientAppName :: Client m -> Maybe Text
clientAppName client = client.conn.connAppName

-- | What an enqueue may say about how, beside the workflow and its input.
-- Mirrors Rust @EnqueueOptions@: the queue-shaped asks live on 'Enqueue',
-- where the runtime's start keeps them too, so the two surfaces never spell
-- the same things differently.
data EnqueueOptions = EnqueueOptions
  { queue :: Enqueue,
    workflow_id :: Maybe Text,
    class_name :: Maybe Text,
    config_name :: Maybe Text,
    app_name :: Maybe Text,
    app_version :: Maybe Text,
    timeout :: Maybe Duration,
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
      workflow_id = Nothing,
      class_name = Nothing,
      config_name = Nothing,
      app_name = Nothing,
      app_version = Nothing,
      timeout = Nothing,
      attributes = Nothing
    }

-- | Enqueues a workflow by name, onto a queue, and hands back a polling
-- handle to it. Asks for nothing else: the client's app name is stamped,
-- no version is recorded, and the id is generated.
enqueueClientWorkflow :: MonadSTM m => Client m -> Text -> Text -> Maybe SerializedWorkflowValue -> m (Either TransactError.Error (WorkflowHandle m))
enqueueClientWorkflow client workflowName queueName input =
  enqueueClientWorkflowWith client workflowName (enqueueOptionsNew queueName) input

-- | 'enqueueClientWorkflow', with something to say about how. A call that
-- names no version records none — which the dequeue admits only from the
-- latest registered version — while a named version pins the row to
-- executors running exactly that code. A deduplication collision is refused,
-- unless the policy asks to join the holder instead.
enqueueClientWorkflowWith :: MonadSTM m => Client m -> Text -> EnqueueOptions -> Maybe SerializedWorkflowValue -> m (Either TransactError.Error (WorkflowHandle m))
enqueueClientWorkflowWith client workflowName options input = do
  case validateEnqueue options.queue of
    Left err -> pure (Left err)
    Right () -> do
      generated <- generatedWorkflowId client.conn
      let shape = options.queue
          workflowText = fromMaybe generated options.workflow_id
          serialization = case input >>= (.serializedSerialization) of
            Just (Serialization name) -> Just name
            Nothing -> Just (serializerName client.conn.connSerializer)
          applicationName = case options.app_name of
            Just name -> Just name
            Nothing -> client.conn.connAppName
          new =
            (newWorkflow workflowText)
              { newWorkflowName = Just workflowName,
                newWorkflowClassName = options.class_name,
                newWorkflowConfigName = options.config_name,
                newWorkflowInput = (.serializedText) <$> input,
                newWorkflowSerialization = serialization,
                newWorkflowQueueName = Just shape.name,
                newWorkflowDeduplicationId = shape.deduplication_id,
                newWorkflowPriority = storedPriority shape,
                newWorkflowQueuePartitionKey = shape.partition_key,
                newWorkflowDelay = shape.delay,
                newWorkflowTimeout = options.timeout,
                newWorkflowDeadline = Nothing,
                newWorkflowAttributes = encodeAttributes options.attributes,
                newWorkflowExecutorId = Nothing,
                newWorkflowApplicationName = applicationName,
                newWorkflowApplicationVersion = options.app_version
              }
      initialized <- runSystemDB client.conn.connSysdb (\db -> SystemDB.initWorkflow db new (Just maxRecoveryAttempts) Fresh Nothing)
      case initialized of
        Right _ -> pure (Right (handle True workflowText))
        Left err -> resolveEnqueueCollision client.conn shape workflowText err
  where
    handle failMissing workflowText =
      pollingHandle client.conn workflowText failMissing

-- | A handle to a workflow that already exists, by id. Always polling, and
-- never checked: the id is not read until the handle is used. The one
-- handle that waits for a row to appear rather than reporting its absence.
retrieveClientWorkflow :: Client m -> Text -> WorkflowHandle m
retrieveClientWorkflow client workflowId =
  pollingHandle client.conn workflowId False

-- | A workflow's status, or 'Nothing' if there is no such workflow. The
-- one-shot read behind 'handleStatus', for a caller with an id and no
-- handle.
workflowStatusClient :: Monad m => Client m -> WorkflowId -> m (Either TransactError.Error (Maybe WorkflowStatus))
workflowStatusClient client workflowId = do
  result <- runSystemDB client.conn.connSysdb (\db -> SystemDB.getWorkflow db workflowId)
  pure $ case result of
    Left err -> Left (TransactError.ErrorSystemDatabase err)
    Right Nothing -> Right Nothing
    Right (Just record) -> Right (Just record.workflowRecordStatus)
