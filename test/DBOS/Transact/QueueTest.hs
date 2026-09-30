{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Queue engine types and the resolution boundary for rows written by
-- older SDKs.
module DBOS.Transact.QueueTest (tests) where

import DBOS.Prelude
import DBOS.SystemDB (AwaitedOutcome (..), Change (..), QueueName (..), QueueRecord (..), RateLimit (..), WorkflowInitResult (..), WorkflowStatus (..), internalQueueName, secondsDuration)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.Transact
  ( Config (..),
    CodecError,
    Error (..),
    Environment (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    Queue (..),
    QueueChange (..),
    QueueConflict (..),
    QueueOptions (..),
    configFromEnv,
    defaultQueueChange,
    defaultQueueOptions,
    deleteQueue,
    decodeWorkflowValue,
    encodeWorkflowValue,
    enqueueDBOSWorkflow,
    handleStatus,
    isLaunched,
    launchWithEnvironment,
    listQueues,
    newDBOS,
    newWorkflowKey,
    queue,
    queueFromRecord,
    queueIsPartitioned,
    registerQueue,
    registerDBOSWorkflow,
    retrieveWorkflow,
    runDBOSWorkflow,
    waitForWorkflow,
    WorkflowId (..),
    Ctx,
    shutdown,
    updateQueue,
  )
import Control.Monad (when)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Workflow queues"
    [ testCase "queue options default to no limits and poll once a second" $ do
        let options = defaultQueueOptions
        options.concurrency @?= Nothing
        options.worker_concurrency @?= Nothing
        options.polling_interval @?= secondsDuration 1
        options.priority_enabled @?= False,
      testCase "a legacy-partitioned row re-scopes its limits" $ do
        let record =
              QueueRecord
                { queueRecordName = "legacy",
                  queueRecordConcurrency = Just 8,
                  queueRecordWorkerConcurrency = Just 3,
                  queueRecordRateLimit = Nothing,
                  queueRecordPriorityEnabled = True,
                  queueRecordPartitionQueue = True,
                  queueRecordPartitionConcurrency = Nothing,
                  queueRecordPartitionWorkerConcurrency = Nothing,
                  queueRecordPartitionRateLimit = Nothing,
                  queueRecordPollingInterval = secondsDuration 1,
                  queueRecordApplicationName = Just "app"
                }
            receipt = queueFromRecord record
        receipt.name @?= "legacy"
        receipt.concurrency @?= Nothing
        receipt.worker_concurrency @?= Nothing
        receipt.partition_concurrency @?= Just 8
        receipt.partition_worker_concurrency @?= Just 3
        assertBool "the resolved receipt is partitioned" (queueIsPartitioned receipt),
      testCase "a registered queue updates, lists and deletes through the instance" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-queue-" <> Text.take 12 suffix
            queueName = "hs-l2-queue-" <> Text.take 12 suffix
            version = "hs-l2-version-" <> suffix
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          let key = newWorkflowKey "queued"
              body :: Int -> Ctx IO -> IO (Either Error Int)
              body input _ = pure (Right input)
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          let options =
                QueueOptions
                  { concurrency = Nothing,
                    worker_concurrency = Just 3,
                    polling_interval = secondsDuration 1,
                    rate_limit = Nothing,
                    priority_enabled = False,
                    partition_concurrency = Nothing,
                    partition_worker_concurrency = Nothing,
                    partition_rate_limit = Nothing
                  }
          queueRegistered <- registerQueue dbos queueName options AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right receipt -> receipt.worker_concurrency @?= Just 3
          let workflowId = WorkflowId ("hs-l2-enqueue-" <> suffix)
              input = encodeWorkflowValue (7 :: Int)
          enqueued <- enqueueDBOSWorkflow dbos key workflowId (Just input) queueName
          case enqueued of
            Left err -> fail (show err)
            Right result -> result.initResultStatus @?= Enqueued
          waited <- waitForWorkflow dbos workflowId
          case waited of
            Left err -> fail (show err)
            Right (AwaitedSucceeded (Just output) serialization) -> do
              let stored = SerializedWorkflowValue output (Serialization <$> serialization)
                  decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the queue supervisor executes the registered body" (Right 7) decoded
            Right other -> fail (show other)
          completed <- runDBOSWorkflow dbos key workflowId (Just input)
          case completed of
            Right (Just output) -> do
              let decoded = decodeWorkflowValue "result" (Just output) :: Either CodecError Int
              assertEqual "the queue worker executes the registered body" (Right 7) decoded
            other -> fail (show other)
          leftAlone <- registerQueue dbos queueName defaultQueueOptions NeverUpdate
          case leftAlone of
            Left err -> fail (show err)
            Right receipt -> receipt.worker_concurrency @?= Just 3
          let change =
                QueueChange
                  { concurrency = Leave,
                    worker_concurrency = Set (Just 2),
                    polling_interval = Leave,
                    rate_limit = Leave,
                    priority_enabled = Leave,
                    partition_concurrency = Leave,
                    partition_worker_concurrency = Leave,
                    partition_rate_limit = Leave
                  }
          updated <- updateQueue dbos queueName change
          case updated of
            Left err -> fail (show err)
            Right receipt -> receipt.worker_concurrency @?= Just 2
          listed <- listQueues dbos
          case listed of
            Left err -> fail (show err)
            Right queues -> assertBool "the application's queue is listed" (queueName `elem` map (.name) queues)
          fetched <- queue dbos queueName
          case fetched of
            Left err -> fail (show err)
            Right (Just receipt) -> assertEqual "a queue read sees the updated limits" (Just 2) receipt.worker_concurrency
            Right Nothing -> fail "registered queue disappeared"
          removed <- deleteQueue dbos queueName
          assertEqual "delete succeeds" (Right ()) removed
          assertEqual "shutdown sees the executor" True =<< isLaunched dbos,
      testCase "a queue's worker concurrency runs that many at once in one process" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-queueconc-" <> Text.take 12 suffix
            queueName = "hs-l2-queueconc-" <> Text.take 12 suffix
            version = "hs-l2-version-" <> suffix
            key = newWorkflowKey "blocking"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          gate <- newTVarIO False
          active <- newTVarIO (0 :: Int)
          peak <- newTVarIO (0 :: Int)
          let body :: Int -> Ctx IO -> IO (Either Error Int)
              body input _ = do
                atomically $ do
                  now <- readTVar active
                  let running = now + 1
                  writeTVar active running
                  high <- readTVar peak
                  when (running > high) (writeTVar peak running)
                atomically $ do
                  open <- readTVar gate
                  if open then pure () else retry
                atomically (modifyTVar active (subtract 1))
                pure (Right input)
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          let options =
                QueueOptions
                  { concurrency = Nothing,
                    worker_concurrency = Just 2,
                    polling_interval = secondsDuration 1,
                    rate_limit = Nothing,
                    priority_enabled = False,
                    partition_concurrency = Nothing,
                    partition_worker_concurrency = Nothing,
                    partition_rate_limit = Nothing
                  }
          queueRegistered <- registerQueue dbos queueName options AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right receipt -> receipt.worker_concurrency @?= Just 2
          let workflowIds = [WorkflowId ("hs-l2-queueconc-" <> suffix <> "-" <> Text.pack (show n)) | n <- [1 :: Int, 2, 3]]
              input = encodeWorkflowValue (7 :: Int)
          mapM_
            ( \workflowId -> do
                enqueued <- enqueueDBOSWorkflow dbos key workflowId (Just input) queueName
                case enqueued of
                  Left err -> fail (show err)
                  Right _ -> pure ()
            )
            workflowIds
          reachedTwo <- pollUntil (10 * 1000000) (atomically (readTVar peak) >>= \high -> pure (high >= 2))
          assertBool "the worker concurrency lets two run at once" reachedTwo
          atomically (writeTVar gate True)
          mapM_
            ( \workflowId -> do
                waited <- waitForWorkflow dbos workflowId
                case waited of
                  Left err -> fail (show err)
                  Right (AwaitedSucceeded _ _) -> pure ()
                  Right other -> fail (show other)
            )
            workflowIds
          high <- readTVarIO peak
          assertEqual "the local worker budget never exceeds its limit" 2 high,
      testCase "listen queues narrow what this process dequeues" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-listen-" <> Text.take 12 suffix
            fastQueue = "hs-l2-listen-fast-" <> Text.take 12 suffix
            slowQueue = "hs-l2-listen-slow-" <> Text.take 12 suffix
            fastText = "hs-l2-listen-fast-wf-" <> suffix
            slowText = "hs-l2-listen-slow-wf-" <> suffix
            version = "hs-l2-version-" <> suffix
            executorId = "hs-l2-executor-" <> suffix
        base <- configFromEnv appName
        let config =
              base
                { configAppVersion = Just version,
                  configExecutorId = Just executorId,
                  configListenQueues = Just [fastQueue]
                }
        bracket (newDBOS config) shutdown $ \dbos -> do
          let key = newWorkflowKey "either"
              body :: Int -> Ctx IO -> IO (Either Error Int)
              body input _ = pure (Right input)
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          mapM_
            ( \queueName -> do
                queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
                case queueRegistered of
                  Left err -> fail (show err)
                  Right _ -> pure ()
            )
            [fastQueue, slowQueue]
          let enqueueOne wid input queueName = do
                enqueued <- enqueueDBOSWorkflow dbos key wid (Just (encodeWorkflowValue (input :: Int))) queueName
                case enqueued of
                  Left err -> fail (show err)
                  Right _ -> pure ()
          enqueueOne (WorkflowId fastText) 1 fastQueue
          enqueueOne (WorkflowId slowText) 2 slowQueue
          waited <- timeout 15000000 (waitForWorkflow dbos (WorkflowId fastText))
          case waited of
            Just (Right (AwaitedSucceeded (Just output) _)) -> do
              let decoded = decodeWorkflowValue "result" (Just (SerializedWorkflowValue output Nothing)) :: Either CodecError Int
              assertEqual "the listened queue runs" (Right 1) decoded
            other -> fail ("expected the listened workflow to run, got: " <> show other)
          -- By now several sweeps have run; the unlistened queue has not
          -- been touched — its workflow stays ENQUEUED for a peer that
          -- does listen to it.
          retrieved <- retrieveWorkflow dbos (WorkflowId slowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              status <- handleStatus handle
              status @?= Right (Just Enqueued),
      testCase "an empty listen set dequeues from no registered queue" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-listen-none-" <> Text.take 12 suffix
            ignoredQueue = "hs-l2-listen-ignored-" <> Text.take 12 suffix
            ignoredText = "hs-l2-listen-ignored-wf-" <> suffix
            internalText = "hs-l2-listen-internal-wf-" <> suffix
            version = "hs-l2-version-" <> suffix
            executorId = "hs-l2-executor-" <> suffix
            QueueName internalName = internalQueueName
        base <- configFromEnv appName
        let config =
              base
                { configAppVersion = Just version,
                  configExecutorId = Just executorId,
                  configListenQueues = Just []
                }
        bracket (newDBOS config) shutdown $ \dbos -> do
          let key = newWorkflowKey "nothing"
              body :: Int -> Ctx IO -> IO (Either Error Int)
              body input _ = pure (Right input)
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          queueRegistered <- registerQueue dbos ignoredQueue defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          let enqueueOne wid input queueName = do
                enqueued <- enqueueDBOSWorkflow dbos key wid (Just (encodeWorkflowValue (input :: Int))) queueName
                case enqueued of
                  Left err -> fail (show err)
                  Right _ -> pure ()
          enqueueOne (WorkflowId ignoredText) 1 ignoredQueue
          enqueueOne (WorkflowId internalText) 2 internalName
          -- The internal queue proves the loop is running at all, rather
          -- than the assertion below passing because nothing works.
          waited <- timeout 15000000 (waitForWorkflow dbos (WorkflowId internalText))
          case waited of
            Just (Right (AwaitedSucceeded (Just output) _)) -> do
              let decoded = decodeWorkflowValue "result" (Just (SerializedWorkflowValue output Nothing)) :: Either CodecError Int
              assertEqual "the internal queue still runs" (Right 2) decoded
            other -> fail ("expected the internal workflow to run, got: " <> show other)
          retrieved <- retrieveWorkflow dbos (WorkflowId ignoredText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              status <- handleStatus handle
              status @?= Right (Just Enqueued),
      testCase "listen queues never exclude the internal queue" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-listen-int-" <> Text.take 12 suffix
            otherQueue = "hs-l2-listen-other-" <> Text.take 12 suffix
            internalText = "hs-l2-listen-int-wf-" <> suffix
            version = "hs-l2-version-" <> suffix
            executorId = "hs-l2-executor-" <> suffix
            QueueName internalName = internalQueueName
        base <- configFromEnv appName
        let config =
              base
                { configAppVersion = Just version,
                  configExecutorId = Just executorId,
                  configListenQueues = Just [otherQueue]
                }
        bracket (newDBOS config) shutdown $ \dbos -> do
          let key = newWorkflowKey "internal"
              body :: Int -> Ctx IO -> IO (Either Error Int)
              body input _ = pure (Right input)
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          enqueued <- enqueueDBOSWorkflow dbos key (WorkflowId internalText) (Just (encodeWorkflowValue (4 :: Int))) internalName
          case enqueued of
            Left err -> fail (show err)
            Right _ -> pure ()
          waited <- timeout 15000000 (waitForWorkflow dbos (WorkflowId internalText))
          case waited of
            Just (Right (AwaitedSucceeded (Just output) _)) -> do
              let decoded = decodeWorkflowValue "result" (Just (SerializedWorkflowValue output Nothing)) :: Either CodecError Int
              assertEqual "the internal queue runs under a filter" (Right 4) decoded
            other -> fail ("expected the internal workflow to run, got: " <> show other),
      testCase "the internal queue name is reserved" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-reserved-" <> Text.take 12 suffix
            QueueName internalName = internalQueueName
        base <- configFromEnv appName
        let config = base {configAppVersion = Just ("hs-l2-version-" <> suffix), configExecutorId = Just ("hs-l2-executor-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          refused <- registerQueue dbos internalName defaultQueueOptions AlwaysUpdate
          case refused of
            Left (ErrorConfig message) -> assertBool "names the reservation" ("reserved" `Text.isInfixOf` message)
            other -> fail ("expected a configuration refusal, got: " <> show other)
          refusedUpdate <- updateQueue dbos internalName defaultQueueChange
          case refusedUpdate of
            Left (ErrorConfig message) -> assertBool "names the reservation" ("reserved" `Text.isInfixOf` message)
            other -> fail ("expected a configuration refusal, got: " <> show other)
          refusedDelete <- deleteQueue dbos internalName
          case refusedDelete of
            Left (ErrorConfig message) -> assertBool "names the reservation" ("reserved" `Text.isInfixOf` message)
            other -> fail ("expected a configuration refusal, got: " <> show other),
      testCase "registering before launch is refused" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-unlaunched-" <> Text.take 12 suffix
            queueName = "hs-l2-queue-" <> Text.take 12 suffix
        base <- configFromEnv appName
        let config = base {configAppVersion = Just ("hs-l2-version-" <> suffix), configExecutorId = Just ("hs-l2-executor-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          refused <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          case refused of
            Left ErrorNotLaunched {} -> pure ()
            other -> fail ("expected a not-launched refusal, got: " <> show other),
      testCase "incoherent limits are refused before they reach the row" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-invalid-" <> Text.take 12 suffix
            queueName = "hs-l2-checked-" <> Text.take 12 suffix
        base <- configFromEnv appName
        let config = base {configAppVersion = Just ("hs-l2-version-" <> suffix), configExecutorId = Just ("hs-l2-executor-" <> suffix)}
            rateLimit limit period = RateLimit {rateLimitLimit = limit, rateLimitPeriod = period}
            cases =
              [ ( "a rate limit admitting nothing",
                  (defaultQueueOptions :: QueueOptions) {rate_limit = Just (rateLimit 0 (secondsDuration 1))},
                  "rate_limit.limit"
                ),
                ( "a rate limit over no window",
                  (defaultQueueOptions :: QueueOptions) {rate_limit = Just (rateLimit 1 (secondsDuration 0))},
                  "rate_limit.period"
                ),
                ( "no workflows at all per partition",
                  (defaultQueueOptions :: QueueOptions) {partition_concurrency = Just 0},
                  "partition_concurrency"
                ),
                ( "a partition allowed more than the whole queue",
                  (defaultQueueOptions :: QueueOptions) {concurrency = Just 2, partition_concurrency = Just 4},
                  "must not exceed"
                ),
                ( "a partition allowed to start faster than the whole queue",
                  (defaultQueueOptions :: QueueOptions)
                    { rate_limit = Just (rateLimit 10 (secondsDuration 60)),
                      partition_rate_limit = Just (rateLimit 5 (secondsDuration 1))
                    },
                  "must not exceed `rate_limit`"
                )
              ]
        bracket (newDBOS config) shutdown $ \dbos -> do
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          mapM_
            ( \(what, options, fragment) -> do
                refused <- registerQueue dbos queueName options AlwaysUpdate
                case refused of
                  Left (ErrorConfig message)
                    | fragment `Text.isInfixOf` message -> pure ()
                    | otherwise -> fail (Text.unpack what <> ": refusal misses " <> Text.unpack fragment <> ", got: " <> Text.unpack message)
                  other -> fail (Text.unpack what <> " was accepted: " <> show other)
            )
            cases
          missing <- queue dbos queueName
          missing @?= Right Nothing,
      testCase "an update cannot leave a queue incoherent" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-incoherent-" <> Text.take 12 suffix
            queueName = "hs-l2-incoherent-q-" <> Text.take 12 suffix
        base <- configFromEnv appName
        let config = base {configAppVersion = Just ("hs-l2-version-" <> suffix), configExecutorId = Just ("hs-l2-executor-" <> suffix)}
            coherent =
              (defaultQueueOptions :: QueueOptions)
                { concurrency = Just 2,
                  worker_concurrency = Just 2
                }
        bracket (newDBOS config) shutdown $ \dbos -> do
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          registered <- registerQueue dbos queueName coherent AlwaysUpdate
          case registered of
            Left err -> fail (show err)
            Right _ -> pure ()
          -- Only wrong beside the concurrency the row already holds, so
          -- the merged result is what gets checked.
          refused <-
            updateQueue
              dbos
              queueName
              (defaultQueueChange {worker_concurrency = Set (Just 5)})
          case refused of
            Left (ErrorConfig message) -> assertBool "refuses the pair" ("must not exceed" `Text.isInfixOf` message)
            other -> fail ("expected a pair refusal, got: " <> show other)
          stored <- queue dbos queueName
          case stored of
            Right (Just receipt) -> receipt.worker_concurrency @?= Just 2
            other -> fail ("expected the stored limits untouched, got: " <> show other)
          -- Raising both together is coherent, and accepted.
          raised <-
            updateQueue
              dbos
              queueName
              (defaultQueueChange {concurrency = Set (Just 5), worker_concurrency = Set (Just 5)})
          case raised of
            Left err -> fail (show err)
            Right receipt -> receipt.worker_concurrency @?= Just 5
    ]

-- | Polls a condition until it holds or the budget runs out.
pollUntil :: Int -> IO Bool -> IO Bool
pollUntil remaining check
  | remaining <= 0 = check
  | otherwise = do
      ok <- check
      if ok
        then pure True
        else threadDelay 100000 >> pollUntil (remaining - 100000) check

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }
