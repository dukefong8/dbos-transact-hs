{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Starts the queue-worker demo without serving it: the launched DBOS
-- instance owns the workflow and its queue, and the caller gets the WAI
-- application plus the teardown action. "Main" mounts this under
-- @/queue-worker@ next to the other demos.
--
-- Single process on purpose (the approval chose it over the Python demo's
-- two-process split): the handlers enqueue through the executor while the
-- same process's dequeue loop drains the queue, the way the starter's queue
-- tab already works.
module QueueWorker.Run (start) where

import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.Transact
  ( Config (..),
    QueueConflict (..),
    configFromEnv,
    defaultQueueOptions,
    launch,
    newDBOS,
    newWorkflowKey,
    registerWorkflow,
    registerQueue,
    shutdown,
  )
import IHP.Router.WAI (routeTrieMiddleware)
import Network.Wai (Application)
import Prelude
import QueueWorker.App (QueueWorkerApp (..))
import QueueWorker.Route (dispatchQueueWorker, queueWorkerNotFound, queueWorkerRouteTrie)
import QueueWorker.Workflows (workerQueueName, workerWorkflow, workerWorkflowName)

-- | The version this build runs as. Recovery only resumes workflows stamped
-- with the running executor's own version, and the system database's version
-- registry admits one application name per version.
queueWorkerVersion :: Text
queueWorkerVersion = "hs-queue-worker-0.1.0"

start :: IO (Application, IO ())
start = do
  config0 <- configFromEnv "dbos-hs-queue-worker"
  let config =
        config0
          { configAppVersion = Just queueWorkerVersion,
            -- This process drains only its own queue: listening to all (the
            -- default) would sweep other apps' queued rows in a shared
            -- database.
            configListenQueues = Just [workerQueueName]
          }
  dbos <- newDBOS config
  -- Registered before launch, because recovery starts inside it.
  _ <-
    registerWorkflow dbos (newWorkflowKey workerWorkflowName) workerWorkflow
      >>= either (die . Text.pack . show) pure
  exec <- launch dbos >>= either (die . Text.pack . show) pure
  -- The queue the handlers submit workflows to. Registered after launch (it
  -- needs the executor), like the starter's.
  _ <- registerQueue dbos workerQueueName defaultQueueOptions NeverUpdate >>= either (die . Text.pack . show) pure
  let worker = QueueWorkerApp {qwDbos = dbos, qwExec = exec}
      application = routeTrieMiddleware (queueWorkerRouteTrie (dispatchQueueWorker worker)) queueWorkerNotFound
  pure (application, shutdown dbos)

die :: Text -> IO a
die = fail . Text.unpack
