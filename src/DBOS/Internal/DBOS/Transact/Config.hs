{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | How a DBOS application is configured. Mirrors Rust @config.rs@: the
-- fields, @Config::new@, @Config::from_env@ (which reads @DBOS_DATABASE_URL@
-- and no other variable — the identity variables are launch's to read, so a
-- hand-written config and one from here resolve to the same identity),
-- @validate@, and @outcome_poll_interval@.
--
-- The system database comes from @DBOS_DATABASE_URL@; the application's own
-- datasource reads @APP_DATABASE_URL@ (see 'appDatabaseUrlFromEnv'), so the
-- two may point at different servers.
module DBOS.Transact.Config
  ( databaseUrlEnv,
    appDatabaseUrlEnv,
    appDatabaseUrlFromEnv,
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
import Data.Text qualified as Text
import DBOS.SystemDB.Types (Duration, durationIsZero, secondsDuration)
import DBOS.Transact.Error (EngineOnly, Error (..))
import System.Environment (lookupEnv)

-- | The one variable @from_env@ reads: the system database.
databaseUrlEnv :: Text
databaseUrlEnv = "DBOS_DATABASE_URL"

-- | The variable the *application* datasource reads: the app's own database,
-- which may be a different server than the system database. Distinct from
-- the system database's @DBOS_DATABASE_URL@, and from the plain
-- @DATABASE_URL@ that libpq tooling and the compile-time typedSql describe
-- already use; a single-database deployment sets all three to the same URL.
appDatabaseUrlEnv :: Text
appDatabaseUrlEnv = "APP_DATABASE_URL"

-- | The application datasource URL from the environment, or 'Nothing' when
-- unset or empty. The caller decides the fallback (usually the system URL).
appDatabaseUrlFromEnv :: IO (Maybe Text)
appDatabaseUrlFromEnv = do
  url <- lookupEnv (Text.unpack appDatabaseUrlEnv)
  pure (Text.pack <$> url >>= \t -> if Text.null t then Nothing else Just t)

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
validateConfig :: Config -> Either (Error EngineOnly) ()
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
