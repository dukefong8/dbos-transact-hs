{-# LANGUAGE OverloadedStrings #-}

-- | Starts the starter without serving it: config, workflows, queue and the
-- events tab's order id, plus the WAI application and the teardown action.
-- "Main" mounts this under @/starter@ next to the widget store.
module Starter.Run (start) where

import Data.Text (pack)
import DBOS.Prelude
import DBOS.Transact (Config (..), Environment (..), QueueConflict (..), QueueOptions (..), configFromEnv, defaultQueueOptions, launchWithEnvironment, newDBOS, registerQueue, shutdown)
import IHP.Router.WAI (routeTrieMiddleware)
import Network.Wai (Application)
import Starter.App (StarterApp (..))
import Starter.Route (dispatchStarter, starterNotFound, starterRouteTrie)
import Starter.Workflows (defaultWorkerConcurrency, demoQueueName, registerStarterWorkflows)
import System.Environment (lookupEnv)

-- | The environment snapshot the starter has always launched with: identity
-- from the config (and its env overrides), not from the ambient cloud vars.
isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }

start :: IO (Application, IO ())
start = do
  -- The default version is app-specific because the system database's
  -- version registry admits one application name per version, and a shared
  -- dev database holds other apps at plain 0.1.0.
  applicationVersion <- maybe "hs-starter-0.1.0" pack <$> lookupEnv "DBOS_APP_VERSION"
  executorId <- maybe "hs-starter-executor" pack <$> lookupEnv "DBOS_EXECUTOR_ID"
  config0 <- configFromEnv "dbos-hs-starter"
  let config =
        config0
          { configAppVersion = Just applicationVersion,
            configExecutorId = Just executorId
          }
  dbos <- newDBOS config
  refs <- registerStarterWorkflows dbos >>= either (fail . show) pure
  exec <- launchWithEnvironment dbos isolatedEnvironment >>= either (fail . show) pure
  registeredQueue <-
    registerQueue
      dbos
      demoQueueName
      (defaultQueueOptions {worker_concurrency = Just defaultWorkerConcurrency})
      NeverUpdate
  _ <- either (fail . show) pure registeredQueue
  orderId <- newTVarIO Nothing
  let app = StarterApp {staDbos = dbos, staExec = exec, staOrderId = orderId, staRefs = refs}
      application = routeTrieMiddleware (starterRouteTrie (dispatchStarter app)) starterNotFound
  pure (application, shutdown dbos)
