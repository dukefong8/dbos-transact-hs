{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The identity an executor runs under, and the rules that resolve it.
-- Mirrors Rust @identity.rs@: the environment is taken as a snapshot
-- ('Environment') so 'resolve' is a pure function of a config and that
-- snapshot, and the deployment outranks the configuration only on DBOS
-- Cloud.
module DBOS.Transact.Identity
  ( appVersionEnv,
    cloudEnv,
    appIdEnv,
    cloudAppNameEnv,
    executorIdEnv,
    defaultExecutorId,
    Environment (..),
    Identity (..),
    readEnvironment,
    resolve,
    validateAppName,
  )
where

import DBOS.Prelude
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.Transact.Config (Config (..))
import DBOS.Transact.Error (Error (..))
import System.Environment (lookupEnv)

-- | The variable holding the application version.
appVersionEnv :: Text
appVersionEnv = "DBOS__APPVERSION"

-- | The variable marking a DBOS Cloud deployment.
cloudEnv :: Text
cloudEnv = "DBOS__CLOUD"

-- | The variable holding the application id.
appIdEnv :: Text
appIdEnv = "DBOS__APPID"

-- | The variable holding the application's name on DBOS Cloud.
cloudAppNameEnv :: Text
cloudAppNameEnv = "DBOS_APP_NAME"

-- | The variable a deployment uses to name the VM this process runs on.
executorIdEnv :: Text
executorIdEnv = "DBOS__VMID"

-- | What an executor with no deployment-supplied id calls itself.
defaultExecutorId :: Text
defaultExecutorId = "local"

-- | The process environment, as a snapshot: a pure function of a 'Config'
-- and this is testable, where reading the real environment inside 'resolve'
-- would not be.
data Environment = Environment
  { -- | @DBOS__CLOUD@, which decides who wins every question in 'resolve'.
    environmentCloud :: Bool,
    -- | @DBOS__APPID@, empty when unset.
    environmentAppId :: Text,
    -- | @DBOS_APP_NAME@, the application's name on DBOS Cloud.
    environmentAppName :: Maybe Text,
    -- | @DBOS__APPVERSION@.
    environmentAppVersion :: Maybe Text,
    -- | @DBOS__VMID@, a deployment's way of naming the VM.
    environmentExecutorId :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | Reads the environment this process was started with. An empty variable
-- is an unset one throughout; @cloud@ is a case-insensitive @true@ and
-- anything else is false, as Java's @Boolean.parseBoolean@ has it.
readEnvironment :: IO Environment
readEnvironment = do
  let var name = do
        value <- lookupEnv (Text.unpack name)
        pure (Text.pack <$> value >>= \text -> if Text.null text then Nothing else Just text)
  cloudText <- lookupEnv (Text.unpack cloudEnv)
  appId <- var appIdEnv
  appName <- var cloudAppNameEnv
  appVersion <- var appVersionEnv
  executorId <- var executorIdEnv
  pure
    Environment
      { environmentCloud = maybe False (\text -> Text.toLower (Text.pack text) == "true") cloudText,
        environmentAppId = maybe "" id appId,
        environmentAppName = appName,
        environmentAppVersion = appVersion,
        environmentExecutorId = executorId
      }


-- | The identity an executor runs under.
data Identity = Identity
  { identityAppName :: Text,
    identityAppVersion :: Text,
    identityExecutorId :: Text,
    identityAppId :: Text
  }
  deriving stock (Eq, Show)

-- | Reconciles what the application was configured with against what the
-- deployment says: the environment first, then the configuration on top —
-- except on DBOS Cloud, where the deployment is the authority and the
-- configuration is not consulted at all.
resolve :: Config -> Environment -> Either Error Identity
resolve config environment = do
  let (appName, appVersion, executorId)
        | environment.environmentCloud =
            ( maybe "" id environment.environmentAppName,
              environment.environmentAppVersion,
              environment.environmentExecutorId
            )
        | otherwise =
            ( config.configAppName,
              config.configAppVersion <|> environment.environmentAppVersion,
              config.configExecutorId <|> environment.environmentExecutorId
            )
  if Text.null appName
    then
      Left
        ( ErrorConfig
            ( if environment.environmentCloud
                then cloudAppNameEnv <> " must be set when " <> cloudEnv <> " is true"
                else "`app_name` cannot be empty"
            )
        )
    else pure ()
  validateAppName appName
  appVersion' <- case appVersion of
    Nothing ->
      Left
        ( ErrorConfig
            ( "no application version: set `app_version`, or the " <> appVersionEnv
                <> " environment variable. Nothing computes one — a Rust build is not "
                <> "reproducible, so a computed version would change under a rebuild that "
                <> "changed no code and leave the previous run's workflows unrecoverable"
            )
        )
    Just version -> pure version
  pure
    Identity
      { identityAppName = appName,
        identityAppVersion = appVersion',
        identityExecutorId = maybe defaultExecutorId id executorId,
        identityAppId = environment.environmentAppId
      }
  where
    (<|>) :: Maybe a -> Maybe a -> Maybe a
    Just value <|> _ = Just value
    Nothing <|> other = other

-- | The rule the other implementations share: 3–256 characters of lowercase
-- letters, digits, dashes and underscores. Checked rather than trusted
-- because the name is an ownership key: a row stamped with a name no other
-- executor spells the same way is a row nothing claims.
validateAppName :: Text -> Either Error ()
validateAppName name =
  case Text.length name of
    0 -> bad "cannot be empty"
    length'
      | length' <= 2 -> bad "must be at least 3 characters"
      | length' >= 257 -> bad "must be at most 256 characters"
    _ -> case Text.find (\char -> not (isAllowed char)) name of
      Just char ->
        bad
          ( "may contain only lowercase letters, digits, dashes and underscores, but has "
              <> Text.pack (show char)
          )
      Nothing -> Right ()
  where
    isAllowed char = (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-' || char == '_'
    bad why = Left (ErrorConfig ("`app_name` " <> why <> ": " <> Text.pack (show name)))
