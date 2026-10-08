{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Starts the queue-patterns demo without serving it: the launched DBOS
-- instance owns the workflows and their queues, and the caller gets the WAI
-- application plus the teardown action. "Main" mounts this under
-- @/queue-patterns@ next to the other demos.
module QueuePatterns.Run (start) where

import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.SystemDB (RateLimit (..))
import DBOS.Transact
  ( Config (..),
    Debouncer (..),
    QueueConflict (..),
    QueueOptions (..),
    configFromEnv,
    defaultQueueOptions,
    launch,
    newDBOS,
    newWorkflowKey,
    registerWorkflow,
    registerWorkflowRef,
    registerQueue,
    secondsDuration,
    shutdown,
  )
import IHP.Router.WAI (routeTrieMiddleware)
import Network.Wai (Application)
import Prelude
import QueuePatterns.App (QueuePatternsApp (..))
import QueuePatterns.Route (dispatchQueuePatterns, queuePatternsNotFound, queuePatternsRouteTrie)
import QueuePatterns.Workflows (concurrencyQueueName, debouncerQueueName, debouncerWorkflow, debouncerWorkflowName, fairQueueConcurrencyManager, fairQueueWorkflow, fairWorkflowName, fairManagerWorkflowName, partitionedQueueName, rateLimitedQueueName, rateLimitedQueueWorkflow, rateLimitedWorkflowName)

-- | The version this build runs as. Recovery only resumes workflows stamped
-- with the running executor's own version, and the system database's version
-- registry admits one application name per version.
queuePatternsVersion :: Text
queuePatternsVersion = "hs-queue-patterns-0.1.0"

start :: IO (Application, IO ())
start = do
  config0 <- configFromEnv "dbos-hs-queue-patterns"
  let config =
        config0
          { configAppVersion = Just queuePatternsVersion,
            -- This process drains only its own queues: listening to all (the
            -- default) would sweep other apps' queued rows in a shared
            -- database.
            configListenQueues = Just [concurrencyQueueName, partitionedQueueName, rateLimitedQueueName, debouncerQueueName]
          }
  dbos <- newDBOS config
  -- Registered before launch, because recovery starts inside it.
  fairRef <-
    registerWorkflowRef dbos (newWorkflowKey fairWorkflowName) (\() -> fairQueueWorkflow)
      >>= either (die . Text.pack . show) pure
  managerRef <-
    registerWorkflowRef dbos (newWorkflowKey fairManagerWorkflowName) (fairQueueConcurrencyManager fairRef)
      >>= either (die . Text.pack . show) pure
  _ <-
    registerWorkflow dbos (newWorkflowKey rateLimitedWorkflowName) (\() -> rateLimitedQueueWorkflow)
      >>= either (die . Text.pack . show) pure
  debouncedRef <-
    registerWorkflowRef dbos (newWorkflowKey debouncerWorkflowName) debouncerWorkflow
      >>= either (die . Text.pack . show) pure
  exec <- launch dbos >>= either (die . Text.pack . show) pure
  -- The three queues, mirroring the Python @DBOS.register_queue@ calls.
  -- Registered after launch (they need the executor), like the starter's.
  _ <- registerQueue dbos concurrencyQueueName (defaultQueueOptions {concurrency = Just 5}) NeverUpdate >>= either (die . Text.pack . show) pure
  _ <- registerQueue dbos partitionedQueueName (defaultQueueOptions {partitionConcurrency = Just 1}) NeverUpdate >>= either (die . Text.pack . show) pure
  _ <- registerQueue dbos rateLimitedQueueName (defaultQueueOptions {rateLimit = Just (RateLimit 2 (secondsDuration 10))}) NeverUpdate >>= either (die . Text.pack . show) pure
  _ <- registerQueue dbos debouncerQueueName defaultQueueOptions NeverUpdate >>= either (die . Text.pack . show) pure
  let patterns = QueuePatternsApp {qpDbos = dbos, qpExec = exec, qpFairManager = managerRef, qpDebouncer = debouncedRef}
      application = routeTrieMiddleware (queuePatternsRouteTrie (dispatchQueuePatterns patterns)) queuePatternsNotFound
  pure (application, shutdown dbos)

die :: Text -> IO a
die = fail . Text.unpack
