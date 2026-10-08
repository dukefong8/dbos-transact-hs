{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Starts the widget store without serving it: the application record, the
-- launched DBOS instance and its datasource are all set up, and the caller
-- gets the WAI application plus the teardown action. "Main" mounts this
-- under @/widget-store@ next to the starter.
module WidgetStore.Run (start) where

import Prelude
import DBOS.Transact
  ( Config (..),
    acquireAppDataSourceInFromEnv,
    configFromEnv,
    launch,
    newDBOS,
    newWorkflowKey,
    registerDataSource,
    registerWorkflowRef,
    releaseAppDataSource,
    runAppSession,
    shutdown,
    toDataSource,
    verifyAppDataSource,
  )
import Data.Text (Text)
import Data.Text qualified as Text
import IHP.Router.WAI (routeTrieMiddleware)
import Network.Wai (Application)
import WidgetStore.App (WidgetApp (..))
import WidgetStore.Route (dispatchWidget, widgetNotFound, widgetRouteTrie)
import WidgetStore.Store (createSchemaSession)
import WidgetStore.Steps (pgCheckoutSteps, pgDispatchSteps)
import WidgetStore.Workflows (checkoutWorkflow, dispatchWorkflow)

-- | The version this build runs as. Recovery only resumes workflows stamped
-- with the running executor's own version, so a release's version is the
-- natural answer (the Rust port uses its crate version). The name is
-- app-specific because the system database's version registry admits one
-- application name per version, and dev databases hold test instances at
-- plain @0.1.0@.
widgetStoreVersion :: Text
widgetStoreVersion = "hs-widget-store-0.1.0"

start :: IO (Application, IO ())
start = do
  config0 <- configFromEnv "dbos-hs-widget-store"
  let config =
        config0
          { configAppVersion = Just widgetStoreVersion,
            -- The storefront never drains queues; listening to all (the
            -- default) would sweep other apps' queued rows in a shared
            -- database.
            configListenQueues = Just []
          }
  -- The app datasource reads the app's own database: @DATABASE_URL@ when set,
  -- else the system URL. The typedSql statements in Store.hs name widget_store
  -- directly; this schema must stay in step with them.
  app <- acquireAppDataSourceInFromEnv "widget_store" config0.configDatabaseUrl 5
  created <- runAppSession app createSchemaSession
  either (die . Text.pack . show) pure created
  verified <- verifyAppDataSource app
  either (die . Text.pack . show) pure verified
  dbos <- newDBOS config
  let ds = toDataSource app
  _ <- registerDataSource dbos ds >>= either (die . Text.pack . show) pure
  -- Registered before launch, because recovery starts inside it: a workflow
  -- the registry does not know by name is one the recovering executor
  -- cannot resume.
  dispatchRef <-
    registerWorkflowRef dbos (newWorkflowKey "DispatchOrderWorkflow") (dispatchWorkflow ds pgDispatchSteps)
      >>= either (die . Text.pack . show) pure
  checkoutRef <-
    registerWorkflowRef dbos (newWorkflowKey "CheckoutWorkflow") (\() -> checkoutWorkflow ds pgCheckoutSteps dispatchRef)
      >>= either (die . Text.pack . show) pure
  exec <- launch dbos >>= either (die . Text.pack . show) pure
  let widget = WidgetApp {waDbos = dbos, waExec = exec, waApp = app, waCheckout = checkoutRef}
      application = routeTrieMiddleware (widgetRouteTrie (dispatchWidget widget)) widgetNotFound
  pure (application, shutdown dbos >> releaseAppDataSource app)

die :: Text -> IO a
die = fail . Text.unpack
