{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | How a DBOS application is configured. Mirrors Rust @config.rs@: the
-- fields, @Config::new@, @Config::from_env@ (which reads @DBOS_DATABASE_URL@
-- and no other variable — the identity variables are launch's to read, so a
-- hand-written config and one from here resolve to the same identity),
-- @validate@, and @outcome_poll_interval@.
module DBOS.Transact.Config
  ( databaseUrlEnv,
    Serializer (..),
    serializerName,
    Config (..),
    configNew,
    configFromEnv,
    validateConfig,
    outcomePollInterval,
    defaultOutcomePollInterval,
  )
where

import DBOS.Prelude
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.SystemDB.Types (Duration, durationIsZero, secondsDuration)
import DBOS.Transact.Error (Error (..))
import System.Environment (lookupEnv)

-- | The one variable @from_env@ reads.
databaseUrlEnv :: Text
databaseUrlEnv = "DBOS_DATABASE_URL"

-- | How payloads are encoded.
data Serializer = RustSerde
  deriving stock (Eq, Show)

-- | Names the encoding for a stored row.
serializerName :: Serializer -> Text
serializerName RustSerde = "rust_serde"

-- | The application's configuration.
data Config = Config
  { configAppName :: Text,
    configDatabaseUrl :: Text,
    configMaxConnections :: Word,
    configSchema :: Text,
    configExecutorId :: Maybe Text,
    configAppVersion :: Maybe Text,
    configSerializer :: Serializer,
    configUseListenNotify :: Bool,
    configMigrate :: Bool,
    configPollingConcurrency :: Maybe Word,
    configOutcomePollInterval :: Maybe Duration,
    configListenQueues :: Maybe [Text],
    configNotificationCoalesce :: Maybe Duration
  }
  deriving stock (Eq, Show)

-- | A configuration with the defaults every implementation shares. Mirrors
-- @Config::new@.
configNew :: Text -> Text -> Config
configNew appName databaseUrl =
  Config
    { configAppName = appName,
      configDatabaseUrl = databaseUrl,
      configMaxConnections = 10,
      configSchema = "dbos",
      configExecutorId = Nothing,
      configAppVersion = Nothing,
      configSerializer = RustSerde,
      configUseListenNotify = True,
      configMigrate = True,
      configPollingConcurrency = Nothing,
      configOutcomePollInterval = Nothing,
      configListenQueues = Nothing,
      configNotificationCoalesce = Nothing
    }

-- | @configNew@, taking the database URL from @DBOS_DATABASE_URL@. A
-- missing or empty variable leaves the URL empty rather than failing here;
-- launch is where an empty URL is reported.
configFromEnv :: Text -> IO Config
configFromEnv appName = do
  url <- lookupEnv (Text.unpack databaseUrlEnv)
  pure (configNew appName (maybe "" Text.pack url))

-- | Mirrors @Config::validate@: refuse a configuration that cannot work,
-- naming the field the deployment set. Launch is the caller.
validateConfig :: Config -> Either Error ()
validateConfig config
  | Text.null config.configDatabaseUrl =
      Left
        ( ErrorConfig
            ( "no database URL: set `database_url`, or the "
                <> databaseUrlEnv
                <> " environment variable if the configuration came from `Config::from_env`"
            )
        )
  | Text.null config.configSchema = Left (ErrorConfig "`schema` cannot be empty")
  | config.configMaxConnections == 0 = Left (ErrorConfig "`max_connections` cannot be zero")
  | Just interval <- config.configOutcomePollInterval,
    durationIsZero interval =
      -- Unlike `notification_coalesce`, where zero turns coalescing off and is
      -- a setting, a zero poll interval is a busy loop against the database
      -- rather than a faster answer.
      Left (ErrorConfig "`outcome_poll_interval` cannot be zero")
  | otherwise = Right ()

-- | The interval to poll for workflow outcomes at. Mirrors
-- @outcome_poll_interval@.
outcomePollInterval :: Config -> Duration
outcomePollInterval config =
  fromMaybe defaultOutcomePollInterval config.configOutcomePollInterval

-- | The interval every implementation polls at. Mirrors
-- @DEFAULT_OUTCOME_POLL_INTERVAL@.
defaultOutcomePollInterval :: Duration
defaultOutcomePollInterval = secondsDuration 1
