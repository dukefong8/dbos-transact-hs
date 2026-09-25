{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Queue engine types and the resolution boundary for rows written by
-- older SDKs.
module DBOS.Transact.QueueTest (tests) where

import DBOS.Prelude
import DBOS.SystemDB (AwaitedOutcome (..), Change (..), QueueRecord (..), WorkflowInitResult (..), WorkflowStatus (..), secondsDuration)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.Transact
  ( Config (..),
    CodecError,
    Error,
    Environment (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    Queue (..),
    QueueChange (..),
    QueueConflict (..),
    QueueOptions (..),
    configFromEnv,
    defaultQueueOptions,
    deleteQueue,
    decodeWorkflowValue,
    encodeWorkflowValue,
    enqueueDBOSWorkflow,
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
          assertEqual "the local worker budget never exceeds its limit" 2 high
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
