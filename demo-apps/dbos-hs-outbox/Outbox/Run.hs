{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Starts the outbox demo without serving it: the application record, the
-- launched DBOS instance and its datasource are all set up, and the caller
-- gets the WAI application plus the teardown action. "Main" mounts this
-- under @/outbox@ next to the starter and the widget store.
module Outbox.Run (start) where

import Data.Text (Text)
import DBOS.Transact
  ( Config (..),
    QueueConflict (..),
    acquireAppDataSourceInFromEnv,
    configFromEnv,
    defaultQueueOptions,
    launch,
    newDBOS,
    newWorkflowKey,
    registerDataSource,
    registerWorkflow,
    registerWorkflowRef,
    registerQueue,
    releaseAppDataSource,
    runAppSession,
    shutdown,
    toDataSource,
    verifyAppDataSource,
  )
import Data.Text qualified as Text
import IHP.Router.WAI (routeTrieMiddleware)
import Network.Wai (Application)
import Outbox.App (OutboxApp (..), outboxAppName, outboxVersion)
import Outbox.Route (dispatchOutbox, outboxNotFound, outboxRouteTrie)
import Outbox.Store (createSchemaSession)
import Outbox.Workflows (notificationQueueName, placeOrderWorkflow, placeOrderWorkflowName, sendNotificationWorkflow, sendNotificationWorkflowName)
import Prelude

-- | The version this build runs as. Recovery only resumes workflows stamped
-- with the running executor's own version, and the system database's version
-- registry admits one application name per version.
start :: IO (Application, IO ())
start = do
  config0 <- configFromEnv outboxAppName
  let config =
        config0
          { configAppVersion = Just outboxVersion,
            -- The outbox drains only its own notification queue: listening to
            -- all (the default) would sweep other apps' queued rows in a
            -- shared database.
            configListenQueues = Just [notificationQueueName]
          }
  -- The app datasource reads the app's own database: @APP_DATABASE_URL@
  -- when set, else the system URL. The typedSql statements in Store.hs name
  -- outbox_store directly; this schema must stay in step with them.
  app <- acquireAppDataSourceInFromEnv "outbox_store" config0.configDatabaseUrl 5
  created <- runAppSession app createSchemaSession
  either (die . Text.pack . show) pure created
  verified <- verifyAppDataSource app
  either (die . Text.pack . show) pure verified
  dbos <- newDBOS config
  let ds = toDataSource app
  _ <- registerDataSource dbos ds >>= either (die . Text.pack . show) pure
  -- Registered before launch, because recovery starts inside it.
  placeRef <-
    registerWorkflowRef dbos (newWorkflowKey placeOrderWorkflowName) (placeOrderWorkflow ds)
      >>= either (die . Text.pack . show) pure
  _ <-
    registerWorkflow dbos (newWorkflowKey sendNotificationWorkflowName) (sendNotificationWorkflow ds)
      >>= either (die . Text.pack . show) pure
  exec <- launch dbos >>= either (die . Text.pack . show) pure
  -- The queue the transactional-enqueue variant's notifications run on.
  -- Registered after launch (it needs the executor), like the starter's.
  _ <- registerQueue dbos notificationQueueName defaultQueueOptions NeverUpdate >>= either (die . Text.pack . show) pure
  let outbox = OutboxApp {obDbos = dbos, obExec = exec, obApp = app, obPlaceOrder = placeRef}
      application = routeTrieMiddleware (outboxRouteTrie (dispatchOutbox outbox)) outboxNotFound
  pure (application, shutdown dbos >> releaseAppDataSource app)

die :: Text -> IO a
die = fail . Text.unpack
