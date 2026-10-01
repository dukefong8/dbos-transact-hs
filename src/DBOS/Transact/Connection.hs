{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}

-- | A connection to the system database, and the settings every call
-- through it carries. Mirrors Rust @connection.rs@: the backend handle
-- behind an existential (the port's @Box<dyn SystemDatabase>@), beside the
-- serializer, the application name, the outcome poll interval, and which
-- surface opened it. Shared by the executor and the client, exactly as the
-- oracle's @Arc<Connection>@ is.
--
-- Deviation: Rust's @Connection::for_client@ lives in @client.rs@ here,
-- because it takes a @ClientConfig@ and the client module imports this one
-- — the surface that owns the configuration is the surface that builds the
-- connection. 'forApplication' stays, because @Config@ and @Identity@ are
-- below both.
--
-- Two counters ride the connection so the engine needs no IO-only
-- primitive to mint per-execution or per-workflow identity: an
-- 'ExecutionIdentity' counter (Rust's pointer identity) and an injected
-- @m Text@ workflow-id generator (a v4 UUID under IO, a counter under
-- @IOSim@). Both live here because a body reaches them through its 'Ctx'.
module DBOS.Transact.Connection
  ( -- * The backend behind a connection
    SomeSystemDB (..),
    runSystemDB,

    -- * Which surface opened it
    Owner (..),

    -- * The connection
    Connection (..),
    newConnection,
    forApplication,
    closeConnection,

    -- * Per-execution identity and generated ids
    ExecutionIdentity (..),
    nextExecutionIdentity,
    generatedWorkflowId,
    uuidWorkflowId,
  )
where

import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Data.Word (Word32)
import DBOS.Prelude
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres (Settings (..))
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.SystemDB.Retry (uuidEntropy)
import DBOS.SystemDB.Types (Duration)
import DBOS.Tracer (SomeTracer)
import DBOS.Transact.Config (Config (..), Serializer, outcomePollInterval)
import DBOS.Transact.Identity (Identity (..))

-- | A system-database backend, hidden behind its class dictionary. Mirrors
-- the oracle's @Box<dyn SystemDatabase>@: the handle type is existential,
-- so a connection does not name the backend and tests may open one over a
-- stub. @m@ is the effect monad the backend's methods run in (@IO@ in
-- production, @IOSim@ in tests).
data SomeSystemDB m where
  SomeSystemDB :: SystemDB.SystemDB db m => db -> SomeSystemDB m

-- | Run a class method against the hidden backend, passing the handle
-- explicitly. This is the one place the existential is unpacked.
runSystemDB :: SomeSystemDB m -> (forall db. SystemDB.SystemDB db m => db -> m a) -> m a
runSystemDB (SomeSystemDB db) action = action db

-- | Which of the two surfaces a connection was opened for. Mirrors Rust
-- @Owner@; the @Owner@ prefix is the documented collision deviation,
-- because 'DBOS.Transact.Client.Client' owns the bare @Client@ spelling.
data Owner
  = -- | An executor's: the connection an application runs its workflows
    -- through, with a step counter behind it.
    OwnerApplication
  | -- | A client's, which runs nothing.
    OwnerClient
  deriving stock (Eq, Show)

-- | What tells a re-run of the same workflow id apart from the run itself.
-- Rust uses the address of the execution's context; here it is a counter
-- minted per execution, which is equality-only in exactly the same way and
-- needs no IO-only @Unique@.
newtype ExecutionIdentity = ExecutionIdentity Int
  deriving stock (Eq, Show)

-- | A connection to the system database and the settings every call
-- through it carries. Field names keep the oracle's, prefixed where the
-- bare spelling collides with another ported record.
data Connection m = Connection
  { connSysdb               :: SomeSystemDB m,
    connSerializer          :: Serializer,
    connAppName             :: Maybe Text,
    connOutcomePollInterval :: Duration,
    connOwner               :: Owner,
    -- | What tells one connection apart from another in this process: the
    -- analogue of the oracle's @Arc<Connection>@ pointer, which
    -- @StepPlacement::of@ compares to raise @WrongInstance@. Minted once
    -- per connection, so two instances over one database are still two.
    connInstanceId          :: Text,
    connExecutionCounter    :: StrictTVar m Int,
    connGenerateWorkflowId  :: m Text,
    connEntropy             :: m Word32,
    connTracer              :: SomeTracer m
  }

-- | Wraps an acquired backend in a connection. The surface that owns the
-- configuration calls this after acquiring and activating the backend, and
-- supplies the @m Text@ workflow-id generator the engine mints ids with
-- when a caller names none, plus the tracer the connection's workers and
-- workflow contexts announce resource-lifetime events through.
newConnection :: MonadSTM m => SomeSystemDB m -> Serializer -> Maybe Text -> Duration -> Owner -> Text -> m Text -> m Word32 -> SomeTracer m -> m (Connection m)
newConnection sysdb serializer appName pollInterval owner instanceId generate entropy tracer = do
  counter <- newTVarIO 0
  pure
    Connection
      { connSysdb = sysdb,
        connSerializer = serializer,
        connAppName = appName,
        connOutcomePollInterval = pollInterval,
        connOwner = owner,
        connInstanceId = instanceId,
        connExecutionCounter = counter,
        connGenerateWorkflowId = generate,
        connEntropy = entropy,
        connTracer = tracer
      }

-- | Mint the identity of the next execution over this connection.
nextExecutionIdentity :: MonadSTM m => Connection m -> m ExecutionIdentity
nextExecutionIdentity conn = do
  n <- atomically $ do
    current <- readTVar conn.connExecutionCounter
    writeTVar conn.connExecutionCounter (current + 1)
    pure current
  pure (ExecutionIdentity n)

-- | Mint a workflow id for a caller that named none.
generatedWorkflowId :: MonadSTM m => Connection m -> m Text
generatedWorkflowId conn = conn.connGenerateWorkflowId

-- | Connects for an application. The executor's half of the connect: it
-- takes the resolved identity, because an application's every call is
-- stamped with it. Acquires and activates the backend (verify-only: the
-- Haskell port never migrates, ADR-0004/0010).
forApplication :: Config -> Identity -> SomeTracer IO -> IO (Connection IO)
forApplication config identity tracer = do
  backend <- Postgres.acquirePostgresSystemDB (backendConfig config identity) tracer
  Postgres.activatePostgresSystemDB backend
  -- The workflow-id generator is this connection's only UUID source; one
  -- draw names the connection itself.
  instanceId <- uuidWorkflowId
  newConnection
    (SomeSystemDB backend)
    config.configSerializer
    (Just identity.identityAppName)
    (outcomePollInterval config)
    OwnerApplication
    instanceId
    uuidWorkflowId
    uuidEntropy
    tracer

-- | The production id generator: a fresh v4 UUID, as the oracle mints.
uuidWorkflowId :: IO Text
uuidWorkflowId = Text.pack . UUID.toString <$> UUID.V4.nextRandom

-- | Releases the connections, the notifier and the pool. Idempotent, and
-- on this type rather than on its holders because the connection is what is
-- being closed: the executor reaches it after stopping its workflows, the
-- client with nothing to stop first.
closeConnection :: Connection m -> m ()
closeConnection conn = runSystemDB conn.connSysdb SystemDB.close

backendConfig :: Config -> Identity -> Postgres.Config
backendConfig config identity =
  Postgres.Config
    { Postgres.configUrl = config.configDatabaseUrl,
      Postgres.configMaxConnections = fromIntegral config.configMaxConnections,
      Postgres.configSettings =
        (Postgres.defaultSettings :: Settings)
          { settingsSchema = config.configSchema,
            settingsExecutorId = Just identity.identityExecutorId,
            settingsApplicationName = Just identity.identityAppName,
            settingsPollingConcurrency = fromIntegral <$> config.configPollingConcurrency,
            settingsNotificationCoalesce = config.configNotificationCoalesce
          }
    }
