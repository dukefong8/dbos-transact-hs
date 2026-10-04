{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | Queue engine types and the resolution boundary for rows written by
-- older SDKs.
module DBOS.Transact.QueueTest (tests) where

import Control.Monad (forM_, when)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Map.Strict qualified as Map
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import DBOS.Prelude
import DBOS.SystemDB (AwaitedOutcome (..), Change (..), NewQueue (..), OnExistingQueue (..), QueueName (..), QueueRecord (..), RateLimit (..), SystemDB (getQueue, upsertQueue), WorkflowFilter (..), WorkflowInitResult (..), WorkflowRecord (..), WorkflowStatus (..), defaultWorkflowFilter, getWorkflow, internalQueueName, newQueue, secondsDuration)
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact (CodecError, Config (..), Ctx, DBOS, WorkflowCtx, workflowCtxInner, Executor, DuplicationPolicy (..), EngineOnly, Enqueue (..), Environment (..), Error (..), Queue (..), QueueChange (..), QueueConflict (..), QueueOptions (..), Serialization (..), SerializedWorkflowValue (..),     RunOptions (..),
    StartOptions (..), Timeout (..),     WorkflowId (..),
    WorkflowKey,
    WorkflowRef,
    WorkflowHandle (..), configFromEnv, decodeWorkflowValue, defaultQueueChange, defaultQueueOptions, deleteQueue, encodeWorkflowValue, enqueueDBOSWorkflow, enqueueNew, handleResult, handleStatus, handleWorkflowId, isLaunched,
    launchWithEnvironment,
    listQueues,
    listWorkflows, newDBOS, newWorkflowKey, nullTracer, queue, queueFromRecord, queueIsPartitioned, registerDBOSWorkflowRefScoped, registerDBOSWorkflowScoped, registerQueue, renderTransactError, retrieveWorkflow, runDBOSWorkflow, runDBOSWorkflowRef, runOptionsDefault,     shutdown, startChildWorkflowScoped, startDBOSWorkflowRef, startOptionsDefault, updateQueue, waitForWorkflow)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

-- | Launch over the isolated environment and hand back the executor:
-- the one-call form of @launchWithEnvironment@ plus unwrap.
launchQueueExec :: DBOS IO -> Environment -> IO (Executor IO)
launchQueueExec dbos env = do
  started <- launchWithEnvironment dbos env
  case started of
    Left err -> fail (show err)
    Right executor -> pure executor

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
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
              body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body input _ = pure (Right input)
          registered <- registerDBOSWorkflowScoped dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchQueueExec dbos isolatedEnvironment
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
            Left err      -> fail (show err)
            Right receipt -> receipt.worker_concurrency @?= Just 3
          let workflowId = WorkflowId ("hs-l2-enqueue-" <> suffix)
              input = encodeWorkflowValue (7 :: Int)
          enqueued <- enqueueDBOSWorkflow dbos key workflowId (Just input) queueName
          case enqueued of
            Left err     -> fail (show err)
            Right result -> result.initResultStatus @?= Enqueued
          waited <- waitForWorkflow dbos workflowId
          case waited of
            Left err -> fail (show err)
            Right (AwaitedSucceeded (Just output) serialization) -> do
              let stored = SerializedWorkflowValue output (Serialization <$> serialization)
                  decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the queue supervisor executes the registered body" (Right 7) decoded
            Right other -> fail (show other)
          completed <- runWf exec key workflowId (Just input)
          case completed of
            Right (Just output) -> do
              let decoded = decodeWorkflowValue "result" (Just output) :: Either CodecError Int
              assertEqual "the queue worker executes the registered body" (Right 7) decoded
            other -> fail (show other)
          leftAlone <- registerQueue dbos queueName defaultQueueOptions NeverUpdate
          case leftAlone of
            Left err      -> fail (show err)
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
            Left err      -> fail (show err)
            Right receipt -> receipt.worker_concurrency @?= Just 2
          listed <- listQueues dbos
          case listed of
            Left err     -> fail (show err)
            Right queues -> assertBool "the application's queue is listed" (queueName `elem` map (.name) queues)
          fetched <- queue dbos queueName
          case fetched of
            Left err             -> fail (show err)
            Right (Just receipt) -> assertEqual "a queue read sees the updated limits" (Just 2) receipt.worker_concurrency
            Right Nothing        -> fail "registered queue disappeared"
          removed <- deleteQueue dbos queueName
          assertEqual "delete succeeds" (Right ()) removed
          assertEqual "shutdown sees the executor" True =<< isLaunched dbos,
      testCase "a queued workflow with a legacy input runs with it" $ do
        fresh <- UUID.V4.nextRandom
        backend <- getBackend
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-queue-legacy-" <> Text.take 12 suffix
            queueName = "hs-l2-legacy-" <> Text.take 12 suffix
            version = "hs-l2-version-" <> suffix
            workflowText = "hs-l2-enqueue-legacy-" <> suffix
            workflowId = WorkflowId workflowText
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          let key = newWorkflowKey "doubles"
              body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body input _ = pure (Right (input * 2))
          registered <- registerDBOSWorkflowScoped dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchQueueExec dbos isolatedEnvironment
          enqueued <- enqueueDBOSWorkflow dbos key workflowId (Just (encodeWorkflowValue (21 :: Int))) queueName
          case enqueued of
            Left err -> fail (show err)
            Right result -> result.initResultStatus @?= Enqueued
          -- Make the row legacy: move its input into the status column and
          -- drop the payload-table row, as a pre-109 writer left it.
          moved <-
            Postgres.runSession backend "fixture" $
              Session.statement workflowText $
                Statement.preparable
                  "update dbos.workflow_status set inputs = (select inputs from dbos.workflow_input where workflow_uuid = $1) where workflow_uuid = $1"
                  (Encoders.param (Encoders.nonNullable Encoders.text))
                  Decoders.rowsAffected
          case moved of
            Left err -> fail (show err)
            Right 1 -> pure ()
            Right n -> fail ("expected one moved row, got: " <> show n)
          dropped <-
            Postgres.runSession backend "fixture" $
              Session.statement workflowText $
                Statement.preparable
                  "delete from dbos.workflow_input where workflow_uuid = $1"
                  (Encoders.param (Encoders.nonNullable Encoders.text))
                  Decoders.rowsAffected
          case dropped of
            Left err -> fail (show err)
            Right 1 -> pure ()
            Right n -> fail ("expected one dropped row, got: " <> show n)
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          waited <- waitForWorkflow dbos workflowId
          case waited of
            Left err -> fail (show err)
            Right (AwaitedSucceeded (Just output) serialization) -> do
              let stored = SerializedWorkflowValue output (Serialization <$> serialization)
                  decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the legacy input reaches the body" (Right 42) decoded
            Right other -> fail (show other),
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
          let body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
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
          registered <- registerDBOSWorkflowScoped dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchQueueExec dbos isolatedEnvironment
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
            Left err      -> fail (show err)
            Right receipt -> receipt.worker_concurrency @?= Just 2
          let workflowIds = [WorkflowId ("hs-l2-queueconc-" <> suffix <> "-" <> Text.pack (show n)) | n <- [1 :: Int, 2, 3]]
              input = encodeWorkflowValue (7 :: Int)
          mapM_
            ( \workflowId -> do
                enqueued <- enqueueDBOSWorkflow dbos key workflowId (Just input) queueName
                case enqueued of
                  Left err -> fail (show err)
                  Right _  -> pure ()
            )
            workflowIds
          reachedTwo <- pollUntil (10 * 1000000) (atomically (readTVar peak) >>= \high -> pure (high >= 2))
          assertBool "the worker concurrency lets two run at once" reachedTwo
          atomically (writeTVar gate True)
          mapM_
            ( \workflowId -> do
                waited <- waitForWorkflow dbos workflowId
                case waited of
                  Left err                     -> fail (show err)
                  Right (AwaitedSucceeded _ _) -> pure ()
                  Right other                  -> fail (show other)
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
              body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body input _ = pure (Right input)
          registered <- registerDBOSWorkflowScoped dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchQueueExec dbos isolatedEnvironment
          mapM_
            ( \queueName -> do
                queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
                case queueRegistered of
                  Left err -> fail (show err)
                  Right _  -> pure ()
            )
            [fastQueue, slowQueue]
          let enqueueOne wid input queueName = do
                enqueued <- enqueueDBOSWorkflow dbos key wid (Just (encodeWorkflowValue (input :: Int))) queueName
                case enqueued of
                  Left err -> fail (show err)
                  Right _  -> pure ()
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
          retrieved <- retrieveWf dbos (WorkflowId slowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              status <- statusWf handle
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
              body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body input _ = pure (Right input)
          registered <- registerDBOSWorkflowScoped dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchQueueExec dbos isolatedEnvironment
          queueRegistered <- registerQueue dbos ignoredQueue defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _  -> pure ()
          let enqueueOne wid input queueName = do
                enqueued <- enqueueDBOSWorkflow dbos key wid (Just (encodeWorkflowValue (input :: Int))) queueName
                case enqueued of
                  Left err -> fail (show err)
                  Right _  -> pure ()
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
          retrieved <- retrieveWf dbos (WorkflowId ignoredText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              status <- statusWf handle
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
              body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body input _ = pure (Right input)
          registered <- registerDBOSWorkflowScoped dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchQueueExec dbos isolatedEnvironment
          enqueued <- enqueueDBOSWorkflow dbos key (WorkflowId internalText) (Just (encodeWorkflowValue (4 :: Int))) internalName
          case enqueued of
            Left err -> fail (show err)
            Right _  -> pure ()
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
          exec <- launchQueueExec dbos isolatedEnvironment
          refused <- registerQueue dbos internalName defaultQueueOptions AlwaysUpdate
          case refused of
            Left (ErrorConfig message) -> assertBool "names the reservation" ("reserved" `Text.isInfixOf` message)
            other                      -> fail ("expected a configuration refusal, got: " <> show other)
          refusedUpdate <- updateQueue dbos internalName defaultQueueChange
          case refusedUpdate of
            Left (ErrorConfig message) -> assertBool "names the reservation" ("reserved" `Text.isInfixOf` message)
            other                      -> fail ("expected a configuration refusal, got: " <> show other)
          refusedDelete <- deleteQueue dbos internalName
          case refusedDelete of
            Left (ErrorConfig message) -> assertBool "names the reservation" ("reserved" `Text.isInfixOf` message)
            other                      -> fail ("expected a configuration refusal, got: " <> show other),
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
            other                    -> fail ("expected a not-launched refusal, got: " <> show other),
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
          exec <- launchQueueExec dbos isolatedEnvironment
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
          exec <- launchQueueExec dbos isolatedEnvironment
          registered <- registerQueue dbos queueName coherent AlwaysUpdate
          case registered of
            Left err -> fail (show err)
            Right _  -> pure ()
          -- Only wrong beside the concurrency the row already holds, so
          -- the merged result is what gets checked.
          refused <-
            updateQueue
              dbos
              queueName
              (defaultQueueChange {worker_concurrency = Set (Just 5)})
          case refused of
            Left (ErrorConfig message) -> assertBool "refuses the pair" ("must not exceed" `Text.isInfixOf` message)
            other                      -> fail ("expected a pair refusal, got: " <> show other)
          stored <- queue dbos queueName
          case stored of
            Right (Just receipt) -> receipt.worker_concurrency @?= Just 2
            other                -> fail ("expected the stored limits untouched, got: " <> show other)
          -- Raising both together is coherent, and accepted.
          raised <-
            updateQueue
              dbos
              queueName
              (defaultQueueChange {concurrency = Set (Just 5), worker_concurrency = Set (Just 5)})
          case raised of
            Left err      -> fail (show err)
            Right receipt -> receipt.worker_concurrency @?= Just 5,
      testCase "a dequeue stamps the deadline an enqueue left open" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-deadline-app-" <> Text.take 12 suffix
            queueName = "hs-l2-deadline-q-" <> Text.take 12 suffix
            version = "hs-l2-deadline-v-" <> suffix
            workflowText = "budget-starts-on-dequeue-" <> suffix
            key = newWorkflowKey "budgeted"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          refRegistered <- registerRefOf dbos key (\() _ -> pure (Right (1 :: Int)))
          ref <- case refRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          exec <- launchQueueExec dbos isolatedEnvironment
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions UpdateIfLatestVersion
          case queueRegistered of
            Left err -> fail (show err)
            Right _  -> pure ()
          let options =
                startOptionsDefault
                  { startWorkflowId = Just workflowText,
                    startQueue = Just (enqueueNew queueName),
                    startTimeout = Explicit (secondsDuration 300)
                  }
          startedRun <- startDBOSWorkflowRef exec ref options Nothing
          handle <- case startedRun of
            Left err     -> fail (show (err :: Error EngineOnly))
            Right handle -> pure handle
          ran <- resultWf handle
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              decoded @?= Right 1
            other -> fail ("expected the queued run to finish, got: " <> show other)
          reader <- getBackend
          row <- getWorkflow reader (WorkflowId workflowText)
          case row of
            Right (Just found) -> do
              found.workflowRecordTimeout @?= Just (secondsDuration 300)
              case found.workflowRecordDeadline of
                Just _  -> pure ()
                Nothing -> fail "the dequeue left the budget without an expiry"
            other -> fail ("expected the queued row, got: " <> show other),
      testCase "an explicit timeout on a queued workflow records no deadline yet" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-enqueue-timeout-app-" <> Text.take 12 suffix
            queueName = "hs-l2-unpolled-q-" <> Text.take 12 suffix
            version = "hs-l2-enqueue-timeout-v-" <> suffix
            workflowText = "queued-with-a-budget-" <> suffix
            key = newWorkflowKey "queued"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          refRegistered <- registerRefOf dbos key (\() _ -> pure (Right (1 :: Int)))
          ref <- case refRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          exec <- launchQueueExec dbos isolatedEnvironment
          let options =
                startOptionsDefault
                  { startWorkflowId = Just workflowText,
                    startQueue = Just (enqueueNew queueName),
                    startTimeout = Explicit (secondsDuration 300)
                  }
          -- No register_queue anywhere: the row must stay as the enqueue
          -- left it, so the read follows the start at once.
          startedRun <- startDBOSWorkflowRef exec ref options Nothing
          case startedRun of
            Left err -> fail (show (err :: Error EngineOnly))
            Right _  -> pure ()
          reader <- getBackend
          row <- getWorkflow reader (WorkflowId workflowText)
          case row of
            Right (Just found) -> do
              found.workflowRecordTimeout @?= Just (secondsDuration 300)
              case found.workflowRecordDeadline of
                Nothing -> pure ()
                Just _  -> fail "the deadline waits for the dequeue that starts the clock"
            other -> fail ("expected the queued row, got: " <> show other),
      testCase "a partition key is recorded on the row" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-enqueue-partition-app-" <> Text.take 12 suffix
            queueName = "hs-l2-partition-q-" <> Text.take 12 suffix
            version = "hs-l2-enqueue-partition-v-" <> suffix
            workflowText = "sharded-" <> suffix
            key = newWorkflowKey "partitioned"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          refRegistered <- registerRefOf dbos key (\() _ -> pure (Right (1 :: Int)))
          ref <- case refRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          exec <- launchQueueExec dbos isolatedEnvironment
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions UpdateIfLatestVersion
          case queueRegistered of
            Left err -> fail (show err)
            Right _  -> pure ()
          let options =
                startOptionsDefault
                  { startWorkflowId = Just workflowText,
                    startQueue = Just ((enqueueNew queueName) {partition_key = Just "tenant-7", priority = Just 4})
                  }
          startedRun <- startDBOSWorkflowRef exec ref options Nothing
          case startedRun of
            Left err -> fail (show (err :: Error EngineOnly))
            Right _  -> pure ()
          reader <- getBackend
          row <- getWorkflow reader (WorkflowId workflowText)
          case row of
            Right (Just found) -> do
              found.workflowRecordQueueName @?= Just queueName
              found.workflowRecordQueuePartitionKey @?= Just "tenant-7"
              found.workflowRecordPriority @?= 4
            other -> fail ("expected the queued row, got: " <> show other),
      testCase "an unprioritised workflow stores the sentinel" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-enqueue-sentinel-app-" <> Text.take 12 suffix
            queueName = "hs-l2-sentinel-q-" <> Text.take 12 suffix
            version = "hs-l2-enqueue-sentinel-v-" <> suffix
            workflowText = "no-priority-" <> suffix
            key = newWorkflowKey "plain"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          refRegistered <- registerRefOf dbos key (\() _ -> pure (Right (1 :: Int)))
          ref <- case refRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          exec <- launchQueueExec dbos isolatedEnvironment
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions UpdateIfLatestVersion
          case queueRegistered of
            Left err -> fail (show err)
            Right _  -> pure ()
          let options =
                startOptionsDefault
                  { startWorkflowId = Just workflowText,
                    startQueue = Just ((enqueueNew queueName) {delay = Just (secondsDuration 30)})
                  }
          startedRun <- startDBOSWorkflowRef exec ref options Nothing
          case startedRun of
            Left err -> fail (show (err :: Error EngineOnly))
            Right _  -> pure ()
          reader <- getBackend
          row <- getWorkflow reader (WorkflowId workflowText)
          case row of
            Right (Just found) -> do
              found.workflowRecordPriority @?= 0
              found.workflowRecordDeduplicationId @?= Nothing
              found.workflowRecordQueuePartitionKey @?= Nothing
            other -> fail ("expected the queued row, got: " <> show other),
      testCase "an incoherent enqueue is refused" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-enqueue-validation-app-" <> Text.take 12 suffix
            queueName = "hs-l2-validation-q-" <> Text.take 12 suffix
            version = "hs-l2-enqueue-validation-v-" <> suffix
            key = newWorkflowKey "checked"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          refRegistered <- registerRefOf dbos key (\() _ -> pure (Right (1 :: Int)))
          ref <- case refRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          exec <- launchQueueExec dbos isolatedEnvironment
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions UpdateIfLatestVersion
          case queueRegistered of
            Left err -> fail (show err)
            Right _  -> pure ()
          -- Past what the priority column holds: i32 max plus one.
          let bad = (enqueueNew queueName) {priority = Just 2147483648}
          startedRun <- startDBOSWorkflowRef exec ref (startOptionsDefault {startQueue = Just bad}) Nothing
          case startedRun of
            Left (ErrorConfig message) -> assertBool "refuses the priority" ("`priority` must be at most 2147483647" `Text.isInfixOf` message)
            other -> fail ("expected a priority refusal, got: " <> show (other :: Either (Error EngineOnly) (WorkflowHandle IO EngineOnly)))
          -- Refused before anything is written: a bad enqueue costs a
          -- round trip, not a row.
          listed <- listWorkflows dbos (defaultWorkflowFilter {workflowFilterQueueNames = [queueName]})
          case listed of
            Right [] -> pure ()
            other    -> fail ("a refused enqueue wrote a row anyway: " <> show other),
      testCase "a delayed enqueue waits before it is dequeued" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-enqueue-delay-app-" <> Text.take 12 suffix
            queueName = "hs-l2-delay-q-" <> Text.take 12 suffix
            version = "hs-l2-enqueue-delay-v-" <> suffix
            workflowText = "held-back-" <> suffix
            key = newWorkflowKey "delayed"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          ran <- newTVarIO (0 :: Int)
          let body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body () _ = do
                atomically (modifyTVar ran (+ 1))
                pure (Right 7)
          refRegistered <- registerRefOf dbos key body
          ref <- case refRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          exec <- launchQueueExec dbos isolatedEnvironment
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions UpdateIfLatestVersion
          case queueRegistered of
            Left err -> fail (show err)
            Right _  -> pure ()
          let options =
                startOptionsDefault
                  { startWorkflowId = Just workflowText,
                    startQueue = Just ((enqueueNew queueName) {delay = Just (secondsDuration 3)})
                  }
          startedRun <- startDBOSWorkflowRef exec ref options Nothing
          handle <- case startedRun of
            Left err     -> fail (show (err :: Error EngineOnly))
            Right handle -> pure handle
          status <- statusWf handle
          case status of
            Right (Just Delayed) -> pure ()
            other -> fail ("a delayed enqueue must not be ENQUEUED yet, got: " <> show other)
          -- Comfortably inside the delay, and after several supervisor
          -- sweeps: the row is not eligible, so no worker may have taken it.
          threadDelay 1500000
          early <- atomically (readTVar ran)
          early @?= 0
          finished <- resultWf handle
          case finished of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              decoded @?= Right 7
            other -> fail ("the delayed workflow was never released, got: " <> show other),
      testCase "a deduplication id admits one waiting workflow" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-enqueue-dedup-app-" <> Text.take 12 suffix
            queueName = "hs-l2-dedup-q-" <> Text.take 12 suffix
            version = "hs-l2-enqueue-dedup-v-" <> suffix
            firstText = "dedup-first-" <> suffix
            secondText = "dedup-second-" <> suffix
            thirdText = "dedup-third-" <> suffix
            key = newWorkflowKey "deduped"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          refRegistered <- registerRefOf dbos key (\() _ -> pure (Right (3 :: Int)))
          ref <- case refRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          exec <- launchQueueExec dbos isolatedEnvironment
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions UpdateIfLatestVersion
          case queueRegistered of
            Left err -> fail (show err)
            Right _  -> pure ()
          -- Delayed, so the first workflow is still holding the key when
          -- the second arrives.
          let held = (enqueueNew queueName) {deduplication_id = Just "order-42", delay = Just (secondsDuration 3)}
          firstRun <-
            startDBOSWorkflowRef
              exec
              ref
              (startOptionsDefault {startWorkflowId = Just firstText, startQueue = Just held})
              Nothing
          first <- case firstRun of
            Left err     -> fail (show (err :: Error EngineOnly))
            Right handle -> pure handle
          secondRun <-
            startDBOSWorkflowRef
              exec
              ref
              (startOptionsDefault {startWorkflowId = Just secondText, startQueue = Just held})
              Nothing
          case secondRun of
            Left err -> assertBool "the refusal names the key" ("order-42" `Text.isInfixOf` renderTransactError (err :: Error EngineOnly))
            other    -> fail ("a second workflow took a held deduplication key: " <> show other)
          firstResult <- resultWf first
          case firstResult of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              decoded @?= Right 3
            other -> fail ("the first workflow never ran, got: " <> show other)
          -- Finishing released the key, so the same one is enqueueable again.
          thirdRun <-
            startDBOSWorkflowRef
              exec
              ref
              (startOptionsDefault {startWorkflowId = Just thirdText, startQueue = Just ((enqueueNew queueName) {deduplication_id = Just "order-42"})})
              Nothing
          case thirdRun of
            Left err -> fail ("the key was not released when the holder finished: " <> show (err :: Error EngineOnly))
            Right _  -> pure (),
      testCase "return existing joins the workflow holding the key" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-enqueue-join-app-" <> Text.take 12 suffix
            queueName = "hs-l2-join-q-" <> Text.take 12 suffix
            version = "hs-l2-enqueue-join-v-" <> suffix
            firstText = "join-first-" <> suffix
            secondText = "join-second-" <> suffix
            thirdText = "join-third-" <> suffix
            key = newWorkflowKey "deduped"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          refRegistered <- registerRefOf dbos key (\() _ -> pure (Right (7 :: Int)))
          ref <- case refRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          exec <- launchQueueExec dbos isolatedEnvironment
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions UpdateIfLatestVersion
          case queueRegistered of
            Left err -> fail (show err)
            Right _  -> pure ()
          -- Delayed, so the holder is still waiting when the second caller arrives.
          let joining = (enqueueNew queueName) {deduplication_id = Just "order-42", delay = Just (secondsDuration 3), duplication_policy = ReturnExisting}
          firstRun <-
            startDBOSWorkflowRef
              exec
              ref
              (startOptionsDefault {startWorkflowId = Just firstText, startQueue = Just joining})
              Nothing
          first <- case firstRun of
            Left err     -> fail (show (err :: Error EngineOnly))
            Right handle -> pure handle
          secondRun <-
            startDBOSWorkflowRef
              exec
              ref
              (startOptionsDefault {startWorkflowId = Just secondText, startQueue = Just joining})
              Nothing
          second <- case secondRun of
            Left err     -> fail ("the second enqueue was refused rather than joined: " <> show (err :: Error EngineOnly))
            Right handle -> pure handle
          handleWorkflowId second @?= firstText
          reader <- getBackend
          loser <- getWorkflow reader (WorkflowId secondText)
          case loser of
            Right Nothing -> pure ()
            other         -> fail ("the losing enqueue wrote a row of its own: " <> show other)
          firstResult <- resultWf first
          secondResult <- resultWf second
          case (firstResult, secondResult) of
            (Right (Just firstStored), Right (Just secondStored)) -> do
              let firstDecoded = decodeWorkflowValue "result" (Just firstStored) :: Either CodecError Int
                  secondDecoded = decodeWorkflowValue "result" (Just secondStored) :: Either CodecError Int
              firstDecoded @?= Right 7
              secondDecoded @?= Right 7
            other -> fail ("both handles resolve to the one workflow that ran, got: " <> show other)
          -- The holder has finished, so the key is free and the same policy
          -- claims it rather than joining.
          thirdRun <-
            startDBOSWorkflowRef
              exec
              ref
              (startOptionsDefault {startWorkflowId = Just thirdText, startQueue = Just ((enqueueNew queueName) {deduplication_id = Just "order-42", duplication_policy = ReturnExisting})})
              Nothing
          third <- case thirdRun of
            Left err     -> fail ("the released key was not claimable: " <> show (err :: Error EngineOnly))
            Right handle -> pure handle
          handleWorkflowId third @?= thirdText,
      testCase "priority orders the backlog lower first" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-enqueue-priority-app-" <> Text.take 12 suffix
            queueName = "hs-l2-priority-q-" <> Text.take 12 suffix
            version = "hs-l2-enqueue-priority-v-" <> suffix
            key = newWorkflowKey "ordered"
            submitted = [("low", Just 9), ("high", Just 1), ("none", Nothing), ("mid", Just 5)]
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          order <- newTVarIO []
          let body :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
              body name _ = do
                atomically (modifyTVar order (++ [name]))
                pure (Right name)
          refRegistered <- registerTextRefOf dbos key body
          ref <- case refRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          exec <- launchQueueExec dbos isolatedEnvironment
          -- Enqueued before the queue is registered, which is what holds
          -- the backlog back: no worker exists for a queue with no row.
          handles <- flip mapM submitted $ \(name, priority) -> do
            let workflowText = name <> "-" <> suffix
            startedRun <-
              startDBOSWorkflowRef
                exec
                ref
                (startOptionsDefault {startWorkflowId = Just workflowText, startQueue = Just ((enqueueNew queueName) {priority = priority})})
                (Just (encodeWorkflowValue name))
            case startedRun of
              Left err     -> fail (show (err :: Error EngineOnly))
              Right handle -> pure handle
          -- The backlog is complete, so registering the queue is what
          -- starts its worker.
          queueRegistered <- registerQueue dbos queueName (defaultQueueOptions {worker_concurrency = Just 1}) UpdateIfLatestVersion
          case queueRegistered of
            Left err -> fail (show err)
            Right _  -> pure ()
          results <- mapM resultWf handles
          case sequence results of
            Left err -> fail ("a prioritised workflow never ran: " <> show (err :: Error EngineOnly))
            Right _  -> pure ()
          ran <- atomically (readTVar order)
          ran @?= ["none", "high", "mid", "low"],
      testCase "updating a queue changes what a running worker honours" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-queue-update-limits-app-" <> Text.take 12 suffix
            queueName = "hs-l2-update-q-" <> Text.take 12 suffix
            version = "hs-l2-queue-update-limits-v-" <> suffix
            key = newWorkflowKey "held"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          active <- newTVarIO (0 :: Int)
          peak <- newTVarIO (0 :: Int)
          let body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body () _ = do
                now <- atomically $ do
                  running <- readTVar active
                  let running' = running + 1
                  writeTVar active running'
                  high <- readTVar peak
                  when (running' > high) (writeTVar peak running')
                  pure running'
                threadDelay 600000
                atomically (modifyTVar active (subtract 1))
                pure (Right 1)
          refRegistered <- registerRefOf dbos key body
          ref <- case refRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          exec <- launchQueueExec dbos isolatedEnvironment
          queueRegistered <-
            registerQueue
              dbos
              queueName
              (defaultQueueOptions {worker_concurrency = Just 1})
              UpdateIfLatestVersion
          case queueRegistered of
            Left err -> fail (show err)
            Right _  -> pure ()
          handles <- flip mapM [0 .. 5 :: Int] $ \n -> do
            let workflowText = "fanned-" <> Text.pack (show n) <> "-" <> suffix
            startedRun <-
              startDBOSWorkflowRef
                exec
                ref
                (startOptionsDefault {startWorkflowId = Just workflowText, startQueue = Just (enqueueNew queueName)})
                Nothing
            case startedRun of
              Left err     -> fail (show (err :: Error EngineOnly))
              Right handle -> pure handle
          -- One at a time to begin with.
          threadDelay 1200000
          firstPeak <- atomically (readTVar peak)
          firstPeak @?= 1
          updated <- updateQueue dbos queueName (defaultQueueChange {worker_concurrency = Set (Just 3)})
          case updated of
            Left err    -> fail (show err)
            Right queue -> queue.worker_concurrency @?= Just 3
          results <- mapM resultWf handles
          case sequence results of
            Left err -> fail ("a queued workflow failed: " <> show (err :: Error EngineOnly))
            Right _  -> pure ()
          lastPeak <- atomically (readTVar peak)
          lastPeak @?= 3,
      testCase "a partitioned queue runs one workflow per key at a time" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-partition-runner-app-" <> Text.take 12 suffix
            queueName = "hs-l2-partitioned-q-" <> Text.take 12 suffix
            version = "hs-l2-partition-runner-v-" <> suffix
            key = newWorkflowKey "sharded"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          live <- newTVarIO Map.empty
          perKeyPeak <- newTVarIO (0 :: Int)
          overlapPeak <- newTVarIO (0 :: Int)
          let body :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
              body partition _ = do
                atomically $ do
                  counts <- readTVar live
                  let mine = 1 + Map.findWithDefault 0 partition counts
                      counts' = Map.insert partition mine counts
                  writeTVar live counts'
                  high <- readTVar perKeyPeak
                  when (mine > high) (writeTVar perKeyPeak mine)
                  wide <- readTVar overlapPeak
                  when (Map.size counts' > wide) (writeTVar overlapPeak (Map.size counts'))
                threadDelay 600000
                atomically $ do
                  counts <- readTVar live
                  case Map.lookup partition counts of
                    Just 1  -> writeTVar live (Map.delete partition counts)
                    Just n  -> writeTVar live (Map.insert partition (n - 1) counts)
                    Nothing -> pure ()
                pure (Right partition)
          refRegistered <- registerTextRefOf dbos key body
          ref <- case refRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          exec <- launchQueueExec dbos isolatedEnvironment
          queueRegistered <-
            registerQueue
              dbos
              queueName
              (defaultQueueOptions {partition_concurrency = Just 1})
              UpdateIfLatestVersion
          case queueRegistered of
            Left err -> fail (show err)
            Right _  -> pure ()
          handles <- flip mapM ["tenant-a", "tenant-b"] $ \partition ->
            flip mapM [0 .. 1 :: Int] $ \n -> do
              let workflowText = partition <> "-" <> Text.pack (show n) <> "-" <> suffix
              startedRun <-
                startDBOSWorkflowRef
                  exec
                  ref
                  (startOptionsDefault {startWorkflowId = Just workflowText, startQueue = Just ((enqueueNew queueName) {partition_key = Just partition})})
                  (Just (encodeWorkflowValue partition))
              case startedRun of
                Left err     -> fail (show (err :: Error EngineOnly))
                Right handle -> pure handle
          results <- mapM (mapM resultWf) handles
          case sequence (concat results) of
            Left err -> fail ("a partitioned workflow never ran: " <> show (err :: Error EngineOnly))
            Right _  -> pure ()
          keyPeak <- atomically (readTVar perKeyPeak)
          keyPeak @?= 1
          overlap <- atomically (readTVar overlapPeak)
          overlap @?= 2,
      testCase "a counted partitioned queue runs its limit per key" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-partition-counted-app-" <> Text.take 12 suffix
            queueName = "hs-l2-counted-q-" <> Text.take 12 suffix
            version = "hs-l2-partition-counted-v-" <> suffix
            key = newWorkflowKey "sharded"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          live <- newTVarIO Map.empty
          perKeyPeak <- newTVarIO (0 :: Int)
          let body :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
              body partition _ = do
                atomically $ do
                  counts <- readTVar live
                  let mine = 1 + Map.findWithDefault 0 partition counts
                  writeTVar live (Map.insert partition mine counts)
                  high <- readTVar perKeyPeak
                  when (mine > high) (writeTVar perKeyPeak mine)
                threadDelay 600000
                atomically $ do
                  counts <- readTVar live
                  case Map.lookup partition counts of
                    Just 1  -> writeTVar live (Map.delete partition counts)
                    Just n  -> writeTVar live (Map.insert partition (n - 1) counts)
                    Nothing -> pure ()
                pure (Right partition)
          refRegistered <- registerTextRefOf dbos key body
          ref <- case refRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          exec <- launchQueueExec dbos isolatedEnvironment
          queueRegistered <-
            registerQueue
              dbos
              queueName
              (defaultQueueOptions {partition_concurrency = Just 2})
              UpdateIfLatestVersion
          case queueRegistered of
            Left err -> fail (show err)
            Right _  -> pure ()
          handles <- flip mapM ["tenant-a", "tenant-b"] $ \partition ->
            flip mapM [0 .. 2 :: Int] $ \n -> do
              let workflowText = partition <> "-" <> Text.pack (show n) <> "-" <> suffix
              startedRun <-
                startDBOSWorkflowRef
                  exec
                  ref
                  (startOptionsDefault {startWorkflowId = Just workflowText, startQueue = Just ((enqueueNew queueName) {partition_key = Just partition})})
                  (Just (encodeWorkflowValue partition))
              case startedRun of
                Left err     -> fail (show (err :: Error EngineOnly))
                Right handle -> pure handle
          results <- mapM (mapM resultWf) handles
          case sequence (concat results) of
            Left err -> fail ("a partitioned workflow never ran: " <> show (err :: Error EngineOnly))
            Right _  -> pure ()
          keyPeak <- atomically (readTVar perKeyPeak)
          keyPeak @?= 2,
      testCase "a stored row cannot redefine the internal queue" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-internal-queue-row-app-" <> Text.take 12 suffix
            version = "hs-l2-internal-queue-row-v-" <> suffix
            workflowText = "on-the-internal-queue-" <> suffix
            key = newWorkflowKey "internal"
            QueueName internalText = internalQueueName
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          refRegistered <- registerRefOf dbos key (\() _ -> pure (Right (9 :: Int)))
          ref <- case refRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          reader <- getBackend
          -- Ownerless on purpose: the internal row is a global singleton,
          -- so no single run can own it; leaving the owner null keeps the
          -- upsert repeatable while the stored 300s interval still proves
          -- the engine ignores it.
          written <-
            upsertQueue
              reader
              ((newQueue internalText) {newQueuePollingInterval = secondsDuration 300, newQueueWorkerConcurrency = Just 1, newQueueApplicationName = Nothing})
              UpdateExisting
          case written of
            Left err -> fail ("could not write the queue row: " <> show err)
            Right _  -> pure ()
          exec <- launchQueueExec dbos isolatedEnvironment
          startedRun <-
            startDBOSWorkflowRef
              exec
              ref
              (startOptionsDefault {startWorkflowId = Just workflowText, startQueue = Just (enqueueNew internalText)})
              Nothing
          handle <- case startedRun of
            Left err     -> fail (show (err :: Error EngineOnly))
            Right handle -> pure handle
          ran <- resultWf handle
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              decoded @?= Right 9
            other -> fail ("the internal queue took the stored row's polling interval, got: " <> show other),
      testCase "another application's queue is not dequeued from" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-queue-owner-scope-app-" <> Text.take 12 suffix
            queueName = "belongs-to-a-peer-" <> Text.take 12 suffix
            peerName = "some-other-application-" <> Text.take 12 suffix
            version = "hs-l2-queue-owner-scope-v-" <> suffix
            workflowText = "enqueued-onto-a-peers-queue-" <> suffix
            key = newWorkflowKey "scoped"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          ran <- newTVarIO (0 :: Int)
          let body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body () _ = do
                atomically (modifyTVar ran (+ 1))
                pure (Right 1)
          refRegistered <- registerRefOf dbos key body
          ref <- case refRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          reader <- getBackend
          written <-
            upsertQueue
              reader
              ((newQueue queueName) {newQueueApplicationName = Just peerName})
              UpdateExisting
          case written of
            Left err -> fail ("could not write the queue row: " <> show err)
            Right _  -> pure ()
          exec <- launchQueueExec dbos isolatedEnvironment
          startedRun <-
            startDBOSWorkflowRef
              exec
              ref
              (startOptionsDefault {startWorkflowId = Just workflowText, startQueue = Just (enqueueNew queueName)})
              Nothing
          case startedRun of
            Left err -> fail (show (err :: Error EngineOnly))
            Right _  -> pure ()
          -- Several reconciles' worth: if this queue were going to enter
          -- the set, it would have.
          threadDelay 3000000
          early <- atomically (readTVar ran)
          early @?= 0
          row <- getWorkflow reader (WorkflowId workflowText)
          case row of
            Right (Just found) -> found.workflowRecordStatus @?= Enqueued
            other              -> fail ("the workflow left the queue it was enqueued on, got: " <> show other),
      testCase "an unhonourable queue configuration is refused" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-unhonourable-" <> Text.take 12 suffix
            queueName = "hs-l2-checked-" <> Text.take 12 suffix
            rateLimit limit period = RateLimit {rateLimitLimit = limit, rateLimitPeriod = period}
            cases :: [(Text, QueueOptions, Text)]
            cases =
              [ ( "a per-partition rate limit over no window",
                  defaultQueueOptions {partition_rate_limit = Just (rateLimit 1 (secondsDuration 0))},
                  "partition_rate_limit.period"
                ),
                ( "a partition's worker limit above the partition's own",
                  (defaultQueueOptions :: QueueOptions) {partition_concurrency = Just 2, partition_worker_concurrency = Just 4},
                  "must not exceed `partition_concurrency`"
                ),
                ( "a partition's worker limit above this process's own",
                  (defaultQueueOptions :: QueueOptions) {worker_concurrency = Just 2, partition_worker_concurrency = Just 4},
                  "must not exceed `worker_concurrency`"
                ),
                ( "a partition allowed to start faster than the whole queue",
                  (defaultQueueOptions :: QueueOptions)
                    { rate_limit = Just (rateLimit 10 (secondsDuration 1)),
                      partition_rate_limit = Just (rateLimit 100 (secondsDuration 1))
                    },
                  "must not exceed `rate_limit`"
                )
              ]
        base <- configFromEnv appName
        let config = base {configAppVersion = Just ("hs-l2-version-" <> suffix), configExecutorId = Just ("hs-l2-executor-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          exec <- launchQueueExec dbos isolatedEnvironment
          forM_ cases $ \(what, options, expected) -> do
            refused <- registerQueue dbos queueName options UpdateIfLatestVersion
            case refused of
              Left (ErrorConfig message) -> assertBool (Text.unpack what) (expected `Text.isInfixOf` message)
              other                      -> fail (Text.unpack what <> " was accepted: " <> show other)
          stored <- queue dbos queueName
          case stored of
            Right Nothing -> pure ()
            other         -> fail ("a refused registration wrote a row anyway: " <> show other)
          -- The other side of the rate comparison: a larger count over a
          -- longer window is the slower rate, and slower is allowed.
          accepted <-
            registerQueue
              dbos
              queueName
              (defaultQueueOptions {rate_limit = Just (rateLimit 10 (secondsDuration 1)), partition_rate_limit = Just (rateLimit 100 (secondsDuration 60))})
              UpdateIfLatestVersion
          case accepted of
            Left err -> fail ("a slower per-partition rate should be honoured: " <> show err)
            Right _  -> pure (),
      testCase "a per-process limit may equal the fleet limit" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-equal-limits-app-" <> Text.take 12 suffix
            queueName = "hs-l2-equal-q-" <> Text.take 12 suffix
        base <- configFromEnv appName
        let config = base {configAppVersion = Just ("hs-l2-version-" <> suffix), configExecutorId = Just ("hs-l2-executor-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          exec <- launchQueueExec dbos isolatedEnvironment
          registered <-
            registerQueue
              dbos
              queueName
              (defaultQueueOptions {concurrency = Just 3, worker_concurrency = Just 3})
              UpdateIfLatestVersion
          case registered of
            Left err -> fail (show err)
            Right receipt -> do
              receipt.concurrency @?= Just 3
              receipt.worker_concurrency @?= Just 3,
      testCase "adding a per-partition limit to a legacy row is refused" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-legacy-update-app-" <> Text.take 12 suffix
            queueName = "hs-l2-legacy-q-" <> Text.take 12 suffix
        base <- configFromEnv appName
        let config = base {configAppVersion = Just ("hs-l2-version-" <> suffix), configExecutorId = Just ("hs-l2-executor-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          reader <- getBackend
          written <-
            upsertQueue
              reader
              ((newQueue queueName) {newQueueConcurrency = Just 1, newQueueWorkerConcurrency = Just 1, newQueuePartitionQueue = True, newQueueApplicationName = Just appName})
              UpdateExisting
          case written of
            Left err -> fail ("could not write the legacy row: " <> show err)
            Right _  -> pure ()
          exec <- launchQueueExec dbos isolatedEnvironment
          stored <- queue dbos queueName
          case stored of
            Right (Just receipt) -> do
              assertBool "the flag re-scopes the row" (queueIsPartitioned receipt)
              receipt.partition_concurrency @?= Just 1
              receipt.partition_worker_concurrency @?= Just 1
              receipt.concurrency @?= Nothing
            other -> fail ("expected the legacy row, got: " <> show other)
          refused <- updateQueue dbos queueName (defaultQueueChange {partition_concurrency = Set (Just 4)})
          case refused of
            Left (ErrorConfig message) -> assertBool "names the deprecated flag" ("deprecated `partition_queue`" `Text.isInfixOf` message)
            other                      -> fail ("the update was accepted: " <> show other),
      testCase "a queue registered after launch is dequeued from" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-late-queue-app-" <> Text.take 12 suffix
            queueName = "hs-l2-late-q-" <> Text.take 12 suffix
            version = "hs-l2-late-queue-v-" <> suffix
            workflowText = "late-run-" <> suffix
            key = newWorkflowKey "late"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          refRegistered <- registerRefOf dbos key (\() _ -> pure (Right (1 :: Int)))
          ref <- case refRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          exec <- launchQueueExec dbos isolatedEnvironment
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions UpdateIfLatestVersion
          case queueRegistered of
            Left err -> fail (show err)
            Right _  -> pure ()
          startedRun <-
            startDBOSWorkflowRef
              exec
              ref
              (startOptionsDefault {startWorkflowId = Just workflowText, startQueue = Just (enqueueNew queueName)})
              Nothing
          handle <- case startedRun of
            Left err     -> fail (show (err :: Error EngineOnly))
            Right handle -> pure handle
          ran <- resultWf handle
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              decoded @?= Right 1
            other -> fail ("the late queue never ran its workflow, got: " <> show other),
      testCase "a queue this process never registered is dequeued from" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-unregistered-queue-app-" <> Text.take 12 suffix
            queueName = "hs-l2-ghost-q-" <> Text.take 12 suffix
            version = "hs-l2-unregistered-queue-v-" <> suffix
            workflowText = "ghost-run-" <> suffix
            key = newWorkflowKey "ghost"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          refRegistered <- registerRefOf dbos key (\() _ -> pure (Right (2 :: Int)))
          ref <- case refRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          reader <- getBackend
          -- The row is written straight to the database: no
          -- register_queue anywhere on this process.
          written <-
            upsertQueue
              reader
              ((newQueue queueName) {newQueueApplicationName = Just appName})
              UpdateExisting
          case written of
            Left err -> fail ("could not write the queue row: " <> show err)
            Right _  -> pure ()
          exec <- launchQueueExec dbos isolatedEnvironment
          startedRun <-
            startDBOSWorkflowRef
              exec
              ref
              (startOptionsDefault {startWorkflowId = Just workflowText, startQueue = Just (enqueueNew queueName)})
              Nothing
          handle <- case startedRun of
            Left err     -> fail (show (err :: Error EngineOnly))
            Right handle -> pure handle
          ran <- resultWf handle
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              decoded @?= Right 2
            other -> fail ("the unregistered queue never ran its workflow, got: " <> show other),
      testCase "an inherited deadline reaches a queued child" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-inherited-deadline-app-" <> Text.take 12 suffix
            queueName = "hs-l2-inherited-q-" <> Text.take 12 suffix
            version = "hs-l2-inherited-deadline-v-" <> suffix
            parentText = "hs-l2-inherited-parent-" <> suffix
            childText = "hs-l2-inherited-child-" <> suffix
            parentKey = newWorkflowKey "parent"
            childKey = newWorkflowKey "child"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just version}
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerRefOf dbos childKey (\() _ -> pure (Right (0 :: Int)))
          childRef <- case childRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          let parentBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
              parentBody () wctx = do
                startedChild <-
                  startChildWorkflowScoped
                    wctx
                    childRef
                    (startOptionsDefault {startWorkflowId = Just childText, startQueue = Just (enqueueNew queueName)})
                    Nothing
                case startedChild of
                  Left err     -> pure (Left err)
                  Right handle -> pure (Right (handleWorkflowId handle))
          parentRegistered <- registerUnitTextRefOf dbos parentKey parentBody
          parentRef <- case parentRegistered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          exec <- launchQueueExec dbos isolatedEnvironment
          -- The child is enqueued onto a queue nothing polls: the
          -- assertion is about what the enqueue wrote, so the row has to
          -- stay as the enqueue left it.
          ran <-
            runDBOSWorkflowRef
              exec
              parentRef
              (runOptionsDefault {runWorkflowId = Just parentText, runTimeout = Explicit (secondsDuration 300)})
              (Just (encodeWorkflowValue ()))
          case ran of
            Right (Just _) -> pure ()
            other          -> fail ("the parent did not run: " <> show other)
          reader <- getBackend
          parentRow <- getWorkflow reader (WorkflowId parentText)
          childRow <- getWorkflow reader (WorkflowId childText)
          case (parentRow, childRow) of
            (Right (Just parent), Right (Just child)) -> do
              child.workflowRecordDeadline @?= parent.workflowRecordDeadline
              child.workflowRecordTimeout @?= Nothing
            other -> fail ("expected both rows, got: " <> show other),
      testCase "a queue carries a rate limit and priority ordering" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-queue-limits-app-" <> Text.take 12 suffix
            queueName = "hs-l2-limited-q-" <> Text.take 12 suffix
            rateLimit limit period = RateLimit {rateLimitLimit = limit, rateLimitPeriod = period}
        base <- configFromEnv appName
        let config = base {configAppVersion = Just ("hs-l2-version-" <> suffix), configExecutorId = Just ("hs-l2-executor-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          exec <- launchQueueExec dbos isolatedEnvironment
          registered <-
            registerQueue
              dbos
              queueName
              (defaultQueueOptions {rate_limit = Just (rateLimit 5 (secondsDuration 30)), priority_enabled = True})
              UpdateIfLatestVersion
          case registered of
            Left err -> fail (show err)
            Right receipt -> do
              receipt.rate_limit @?= Just (rateLimit 5 (secondsDuration 30))
              assertBool "priority ordering is reported" receipt.priority_enabled
              assertBool "nothing partitioned" (not (queueIsPartitioned receipt))
          -- Changed at runtime, like every other limit: cleared, and
          -- priority turned back off.
          updated <-
            updateQueue
              dbos
              queueName
              (defaultQueueChange {rate_limit = Set Nothing, priority_enabled = Set False})
          case updated of
            Left err -> fail (show err)
            Right receipt -> do
              receipt.rate_limit @?= Nothing
              assertBool "priority ordering is off" (not receipt.priority_enabled),
      testCase "per-partition limits partition a queue" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-queue-partition-limits-app-" <> Text.take 12 suffix
            queueName = "hs-l2-sharded-" <> Text.take 12 suffix
        base <- configFromEnv appName
        let config = base {configAppVersion = Just ("hs-l2-version-" <> suffix), configExecutorId = Just ("hs-l2-executor-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          exec <- launchQueueExec dbos isolatedEnvironment
          registered <-
            registerQueue
              dbos
              queueName
              (defaultQueueOptions {concurrency = Just 60, worker_concurrency = Just 10, partition_concurrency = Just 4, partition_worker_concurrency = Just 2})
              UpdateIfLatestVersion
          case registered of
            Left err -> fail (show err)
            Right receipt -> do
              assertBool "a partition limit partitions it" (queueIsPartitioned receipt)
              receipt.concurrency @?= Just 60
              receipt.partition_concurrency @?= Just 4
              receipt.partition_worker_concurrency @?= Just 2
          reader <- getBackend
          stored <- getQueue reader queueName
          case stored of
            Right (Just record) -> assertBool "the derived flag is written" record.queueRecordPartitionQueue
            other               -> fail ("expected the queue row, got: " <> show other)
          -- Clearing the last partition limit un-partitions the queue, flag included.
          updated <-
            updateQueue
              dbos
              queueName
              (defaultQueueChange {partition_concurrency = Set Nothing, partition_worker_concurrency = Set Nothing})
          case updated of
            Left err -> fail (show err)
            Right receipt -> do
              assertBool "un-partitioned again" (not (queueIsPartitioned receipt))
              receipt.concurrency @?= Just 60
          storedAgain <- getQueue reader queueName
          case storedAgain of
            Right (Just record) -> assertBool "the flag follows the limits back off" (not record.queueRecordPartitionQueue)
            other               -> fail ("expected the queue row, got: " <> show other),
      testCase "re-registering updates the stored limits" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-reregister-app-" <> Text.take 12 suffix
            queueName = "hs-l2-reregister-q-" <> Text.take 12 suffix
        base <- configFromEnv appName
        let config = base {configAppVersion = Just ("hs-l2-version-" <> suffix), configExecutorId = Just ("hs-l2-executor-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          exec <- launchQueueExec dbos isolatedEnvironment
          first <-
            registerQueue
              dbos
              queueName
              (defaultQueueOptions {concurrency = Just 3})
              UpdateIfLatestVersion
          case first of
            Left err -> fail (show err)
            Right _  -> pure ()
          second <-
            registerQueue
              dbos
              queueName
              (defaultQueueOptions {concurrency = Just 7})
              UpdateIfLatestVersion
          case second of
            Left err -> fail (show err)
            Right receipt -> receipt.concurrency @?= Just 7
          stored <- queue dbos queueName
          case stored of
            Right (Just receipt) -> receipt.concurrency @?= Just 7
            other                -> fail ("expected the updated row, got: " <> show other)
    ]

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through. Local
-- copies are deliberate: this module carries only the aliases it uses.
runWf :: Executor IO -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWf = runDBOSWorkflow

retrieveWf :: DBOS IO -> WorkflowId -> IO (Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
retrieveWf = retrieveWorkflow

resultWf :: WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWf = handleResult

statusWf :: WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe WorkflowStatus))
statusWf = handleStatus

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

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | Register a body at the engine-only channel: the polymorphic
-- registration cannot infer the JSON types from a local binding.
registerRefOf :: DBOS IO -> WorkflowKey -> (forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)) -> IO (Either (Error EngineOnly) (WorkflowRef IO EngineOnly))
registerRefOf = registerDBOSWorkflowRefScoped

-- | Register a @Text -> Text@ body at the engine-only channel.
registerTextRefOf :: DBOS IO -> WorkflowKey -> (forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)) -> IO (Either (Error EngineOnly) (WorkflowRef IO EngineOnly))
registerTextRefOf = registerDBOSWorkflowRefScoped

-- | Register a @() -> Text@ body at the engine-only channel.
registerUnitTextRefOf :: DBOS IO -> WorkflowKey -> (forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)) -> IO (Either (Error EngineOnly) (WorkflowRef IO EngineOnly))
registerUnitTextRefOf = registerDBOSWorkflowRefScoped
