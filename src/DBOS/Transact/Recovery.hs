{-# LANGUAGE OverloadedStrings #-}

-- | Startup recovery sweep. Rust @recovery.rs@ expresses recovery as one
-- SystemDB call: only rows owned by this executor and stamped with this
-- application version are re-enqueued onto the internal queue.
module DBOS.Transact.Recovery (EngineEvent (..), reenqueueForRecovery) where

import DBOS.Prelude
import Data.Text (Text)
import System.Log.FastLogger (ToLogStr (..))
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Types (QueueName (..), WorkflowId, internalQueueName)
import DBOS.Tracer (LogEvent (..), LogSeverity (..), runTracer, showSeverity)
import DBOS.Transact.Connection (Connection (..), runSystemDB)
import DBOS.Transact.Error qualified as TransactError

-- | Engine-lifecycle events: instance launch and shutdown, client
-- connect and close, registry. Mirrors the @instance.rs@, @connection.rs@,
-- @client.rs@ and @registry.rs@ @tracing!@ calls; span fields ride on the
-- constructors so FastLogger lines carry the same @key=value@ pairs.
data EngineEvent
  = EngineLaunched { engineAppName :: Text, engineExecutorId :: Text, engineAppVersion :: Text }
  | EngineShutdown { engineShutdownAppName :: Text }
  | EngineVersionStale { engineVersion :: Text, engineLatestVersion :: Text }
  | EngineNoWorkflows
  | EngineRecovered { engineRecoveredCount :: Int }
  | EngineCancelledRunning { engineCancelledCount :: Int }
  deriving stock (Eq, Show)

instance LogEvent EngineEvent where
  eventSeverity EngineLaunched {}         = SeverityInfo
  eventSeverity EngineShutdown {}         = SeverityInfo
  eventSeverity EngineVersionStale {}     = SeverityWarning
  eventSeverity EngineNoWorkflows         = SeverityWarning
  eventSeverity (EngineRecovered 0)       = SeverityDebug
  eventSeverity EngineRecovered {}        = SeverityInfo
  eventSeverity EngineCancelledRunning {} = SeverityInfo
  renderEvent (EngineLaunched app exec ver) =
    "DBOS launched app_name=" <> app <> " executor_id=" <> exec <> " app_version=" <> ver
  renderEvent (EngineShutdown app) = "DBOS shut down app_name=" <> app
  renderEvent (EngineVersionStale ver latest) =
    "this executor is not running the latest registered application version: it will "
      <> "recover and dequeue only work stamped with its own version app_version="
      <> ver
      <> " latest_version="
      <> latest
  renderEvent EngineNoWorkflows =
    "no workflows are registered: this executor will recover nothing and dequeue nothing. Register before calling `launch`"
  renderEvent (EngineRecovered 0) =
    "no workflows to recover"
  renderEvent (EngineRecovered count) =
    "re-enqueued workflows a previous run left PENDING workflows=" <> showText count
  renderEvent (EngineCancelledRunning count) =
    "cancelled workflows still running; they stay PENDING cancelled=" <> showText count

instance ToLogStr EngineEvent where
  toLogStr event = toLogStr (showSeverity (eventSeverity event) <> " " <> renderEvent event)

reenqueueForRecovery :: Monad m => Connection m -> Text -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
reenqueueForRecovery conn executorId applicationVersion = do
  let QueueName recoveryQueue = internalQueueName
  result <-
    runSystemDB conn.connSysdb (\db -> SystemDB.reenqueueForRecovery db [executorId] applicationVersion recoveryQueue)
  case result of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    Right recovered -> do
      runTracer conn.connTracer (EngineRecovered (length recovered))
      pure (Right recovered)
