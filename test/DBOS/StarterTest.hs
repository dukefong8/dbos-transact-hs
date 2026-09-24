{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Haskell mirror of the Rust starter acceptance flow
-- (demo-apps/dbos-rust-starter): a three-step workflow with progress events,
-- crash recovery, queues, blocking event reads, and approval messages.
-- Each tab becomes tests here as its engine slices land.
module DBOS.StarterTest
  ( tests,
  )
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, mapConcurrently, poll, wait, withAsync)
import Control.Concurrent.STM (readTVarIO)
import Control.Exception (Exception, bracket, throwIO, try)
import Data.Aeson (object, (.=))
import Data.Int (Int64)
import Data.Maybe (isJust)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Text (Text, pack)
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB.Postgres (runDbOrFail, takeNotificationSession)
import DBOS.SystemDB
  ( BackendError (..),
    BackendErrorKind (..),
    Error (..),
    OnExistingQueue (..),
    QueueName (..),
    Topic (..),
    WorkflowStartDecision (..),
    acquirePool,
    dequeueWorkflows,
    enqueueWorkflow,
    fetchQueueWorkerConcurrency,
    fetchWorkflowExecutionRow,
    fetchWorkflowStatus,
    fetchWorkflowStatuses,
    getEvent,
    getEventBlocking,
    internalQueueName,
    listWorkflowIdsByName,
    messageTo,
    millisDuration,
    postgresStepStore,
    recvMessage,
    recordOperationError,
    recordSleep,
    reenqueueForRecovery,
    registerQueue,
    releasePool,
    sendMessage,
    sendMessages,
    setEvent,
    timestampFromEpochMs,
    timestampNow,
    timestampToEpochMs,
    tryStartWorkflow,
    updateQueueWorkerConcurrency,
  )
import DBOS.Transact
  ( ApplicationVersion (..),
    CodecError (..),
    DuplicateWorkflowName (..),
    Executor (..),
    ExecutorId (..),
    OperationId (..),
    OperationName (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    StepError (..),
    WorkflowExecution (..),
    WorkflowId (..),
    WorkflowName (..),
    WorkflowOutcome (..),
    WorkflowRunError (..),
    WorkflowStatus (..),
    decodeWorkflowValue,
    dequeuePass,
    emptyRegistry,
    encodeWorkflowValue,
    launchExecutor,
    nullLogAction,
    parseWorkflowExecution,
    runStep,
    runWorkflow,
    registerWorkflow,
    shutdownExecutor,
    sleepStep,
    spawnWorkflow,
    superviseForever,
  )
import Hasql.Pool qualified as Pool
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), assertBool, testCase)

tests :: TestTree
tests =
  testGroup
    "Starter mirror"
    [       testCase "runs a named step once and replays the recorded output" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-step"
          executorId <- freshExecutorId "hs-exec-step"
          decision <- tryStartWorkflow pool workflowId (WorkflowName "starterStepWorkflow") Nothing executorId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          executions <- newIORef (0 :: Int)
          let body = do
                atomicModifyIORef' executions (\count -> (count + 1, ()))
                pure (encodeWorkflowValue (object ["ok" .= True]))
          first <- runStep (postgresStepStore pool) workflowId (OperationId 1) (OperationName "step_one") body
          second <- runStep (postgresStepStore pool) workflowId (OperationId 1) (OperationName "step_one") body
          first @?= Right (encodeWorkflowValue (object ["ok" .= True]))
          second @?= first
          readIORef executions >>= (@?= 1),
      testCase "replays a recorded step error without running the body" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-step-err"
          executorId <- freshExecutorId "hs-exec-step-err"
          decision <- tryStartWorkflow pool workflowId (WorkflowName "starterStepErrorWorkflow") Nothing executorId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          let failure = encodeWorkflowValue (object ["message" .= ("boom" :: Text)])
          recordOperationError pool workflowId (OperationId 1) (OperationName "step_one") failure
          executions <- newIORef (0 :: Int)
          result <- runStep (postgresStepStore pool) workflowId (OperationId 1) (OperationName "step_one") (increment executions)
          result @?= Left (StepRecordedError failure)
          readIORef executions >>= (@?= 0),
      testCase "publishes a workflow event and reads it back by name" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-events"
          executorId <- freshExecutorId "hs-exec-events"
          decision <- tryStartWorkflow pool workflowId (WorkflowName "starterEventsWorkflow") Nothing executorId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          missing <- getEvent pool workflowId "steps_event"
          missing @?= Nothing
          setEvent pool workflowId "steps_event" (encodeWorkflowValue (1 :: Int))
          published <- getEvent pool workflowId "steps_event"
          (decodeWorkflowValue "result" published :: Either CodecError Int) @?= Right 1,
      testCase "a blocking event read waits for a published key" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-blocking"
          executorId <- freshExecutorId "hs-exec-blocking"
          decision <- tryStartWorkflow pool workflowId (WorkflowName "starterBlockingWorkflow") Nothing executorId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          late <- async (threadDelay 150000 >> setEvent pool workflowId "shipped" (encodeWorkflowValue ("yes" :: Text)))
          found <- getEventBlocking pool workflowId "shipped" (millisDuration 2000)
          wait late
          (decodeWorkflowValue "result" found :: Either CodecError (Maybe Text)) @?= Right (Just "yes"),
      testCase "a blocking event read reports absence at its deadline" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-absent"
          executorId <- freshExecutorId "hs-exec-absent"
          decision <- tryStartWorkflow pool workflowId (WorkflowName "starterAbsentWorkflow") Nothing executorId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          found <- getEventBlocking pool workflowId "never_published" (millisDuration 200)
          found @?= Nothing,
      testCase "a durable sleep waits then replays the remainder" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-sleep"
          executorId <- freshExecutorId "hs-exec-sleep"
          decision <- tryStartWorkflow pool workflowId (WorkflowName "starterSleepWorkflow") Nothing executorId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          before <- currentTimeMillis
          sleepStep pool workflowId (OperationId 1) (millisDuration 300)
          afterFirst <- currentTimeMillis
          (afterFirst - before) `assertAtLeast` 250
          sleepStep pool workflowId (OperationId 1) (millisDuration 300)
          afterSecond <- currentTimeMillis
          assertBool ("replay slept too long: " <> show (afterSecond - afterFirst)) (afterSecond - afterFirst < 150),
      testCase "a replayed sleep waits out the recorded remainder" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-sleep-replay"
          executorId <- freshExecutorId "hs-exec-sleep-replay"
          decision <- tryStartWorkflow pool workflowId (WorkflowName "starterSleepReplayWorkflow") Nothing executorId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          -- A crash mid-sleep: the first run recorded a wake time 600ms out
          -- but never got there. The replay must wait the remainder instead
          -- of returning at once.
          now <- timestampNow
          let wakeAtMs = timestampToEpochMs now + 600
          recordSleep
            pool
            workflowId
            (OperationId 1)
            (OperationName "DBOS.sleep")
            (SerializedWorkflowValue (pack (show wakeAtMs)) (Just (Serialization "portable_json")))
            now
            (timestampFromEpochMs wakeAtMs)
          before <- currentTimeMillis
          sleepStep pool workflowId (OperationId 1) (millisDuration 50)
          after <- currentTimeMillis
          (after - before) `assertAtLeast` 450,
      testCase "sends a message and a parked recv takes it exactly once" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-recv"
          executorId <- freshExecutorId "hs-exec-recv"
          decision <- tryStartWorkflow pool workflowId (WorkflowName "starterRecvWorkflow") Nothing executorId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          sendMessage pool (messageTo workflowId (Topic "approval") (encodeWorkflowValue ("approve" :: Text)))
          first <- recvMessage pool workflowId (OperationId 1) (millisDuration 2000) (Just (Topic "approval"))
          first @?= Just (encodeWorkflowValue ("approve" :: Text))
          second <- recvMessage pool workflowId (OperationId 1) (millisDuration 50) (Just (Topic "approval"))
          second @?= first,
      testCase "a recv with nothing to take reports absence at its deadline" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-recv-none"
          executorId <- freshExecutorId "hs-exec-recv-none"
          decision <- tryStartWorkflow pool workflowId (WorkflowName "starterRecvNoneWorkflow") Nothing executorId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          result <- recvMessage pool workflowId (OperationId 1) (millisDuration 150) (Just (Topic "approval"))
          result @?= Nothing,
      testCase "a second take of one recv step reads the winner's record" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-recv-race"
          executorId <- freshExecutorId "hs-exec-recv-race"
          decision <- tryStartWorkflow pool workflowId (WorkflowName "starterRecvRaceWorkflow") Nothing executorId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          sendMessages
            pool
            [ messageTo workflowId (Topic "approval") (encodeWorkflowValue ("one" :: Text)),
              messageTo workflowId (Topic "approval") (encodeWorkflowValue ("two" :: Text))
            ]
          -- Two takes of the same step (a duplicate execution): the first
          -- wins the step row; the second must survive the conflict instead
          -- of dying on it.
          now <- timestampNow
          first <- runDbOrFail pool (takeNotificationSession workflowId (OperationId 1) (Just (Topic "approval")) now now)
          assertBool "expected the first take to win a message" (isJust first)
          _ <- runDbOrFail pool (takeNotificationSession workflowId (OperationId 1) (Just (Topic "approval")) now now)
          replayed <- recvMessage pool workflowId (OperationId 1) (millisDuration 2000) (Just (Topic "approval"))
          replayed @?= first,
      testCase "send_bulk delivers every message in one call" $
        withDBOSPool $ \pool -> do
          executorId <- freshExecutorId "hs-exec-bulk"
          firstId <- freshWorkflowId "hs-starter-bulk-a"
          secondId <- freshWorkflowId "hs-starter-bulk-b"
          mapM_
            (\workflowId -> tryStartWorkflow pool workflowId (WorkflowName "starterBulkWorkflow") Nothing executorId (ApplicationVersion "v1"))
            [firstId, secondId]
          sendMessages
            pool
            [ messageTo firstId (Topic "approval") (encodeWorkflowValue ("yes" :: Text)),
              messageTo secondId (Topic "approval") (encodeWorkflowValue ("yes" :: Text))
            ]
          first <- recvMessage pool firstId (OperationId 1) (millisDuration 2000) (Just (Topic "approval"))
          second <- recvMessage pool secondId (OperationId 1) (millisDuration 2000) (Just (Topic "approval"))
          first @?= Just (encodeWorkflowValue ("yes" :: Text))
          second @?= Just (encodeWorkflowValue ("yes" :: Text)),
      testCase "lists workflows by name, newest first" $
        withDBOSPool $ \pool -> do
          listName <- uniqueWorkflowName "starterListWorkflow"
          executorId <- freshExecutorId "hs-exec-list"
          firstId <- freshWorkflowId "hs-starter-list-a"
          secondId <- freshWorkflowId "hs-starter-list-b"
          first <- tryStartWorkflow pool firstId listName Nothing executorId (ApplicationVersion "v1")
          first @?= StartWorkflow
          threadDelay 20000
          second <- tryStartWorkflow pool secondId listName Nothing executorId (ApplicationVersion "v1")
          second @?= StartWorkflow
          listed <- listWorkflowIdsByName pool (workflowNameText listName) 10
          listed @?= [secondId, firstId],
      testCase "runs a registered workflow and records its outcome" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-run"
          executorId <- freshExecutorId "hs-exec-run"
          registry <- case registerWorkflow
            (WorkflowName "greetWorkflow")
            (\_ _ _ -> pure (encodeWorkflowValue ("hi" :: Text)))
            emptyRegistry of
            Left err -> fail ("registration failed: " <> show err)
            Right registry -> pure registry
          result <- runWorkflow pool registry (WorkflowName "greetWorkflow") workflowId Nothing executorId (ApplicationVersion "v1")
          result @?= Right (encodeWorkflowValue ("hi" :: Text))
          fetched <- fetchWorkflowExecutionRow pool workflowId
          case fetched of
            Nothing -> fail "expected a workflow_status row"
            Just row -> case parseWorkflowExecution row of
              Left err -> fail ("row failed to parse: " <> show err)
              Right execution ->
                execution.workflowExecutionOutcome
                  @?= Just (WorkflowSucceeded (encodeWorkflowValue ("hi" :: Text))),
      testCase "a second run replays the recorded SUCCESS without running the body" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-rerun"
          executorId <- freshExecutorId "hs-exec-rerun"
          executions <- newIORef (0 :: Int)
          registry <- case registerWorkflow
            (WorkflowName "countedWorkflow")
            (\_ _ _ -> increment executions)
            emptyRegistry of
            Left err -> fail ("registration failed: " <> show err)
            Right registry -> pure registry
          first <- runWorkflow pool registry (WorkflowName "countedWorkflow") workflowId Nothing executorId (ApplicationVersion "v1")
          second <- runWorkflow pool registry (WorkflowName "countedWorkflow") workflowId Nothing executorId (ApplicationVersion "v1")
          second @?= first
          readIORef executions >>= (@?= 1),
      testCase "a second run replays the recorded ERROR without running the body" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-rerun-err"
          executorId <- freshExecutorId "hs-exec-rerun-err"
          executions <- newIORef (0 :: Int)
          registry <- case registerWorkflow
            (WorkflowName "failingWorkflow")
            (\_ _ _ -> increment executions >> throwIO (userError "boom"))
            emptyRegistry of
            Left err -> fail ("registration failed: " <> show err)
            Right registry -> pure registry
          first <- runWorkflow pool registry (WorkflowName "failingWorkflow") workflowId Nothing executorId (ApplicationVersion "v1")
          case first of
            Left (WorkflowBodyFailed _) -> pure ()
            other -> fail ("expected a body failure, got: " <> show other)
          second <- runWorkflow pool registry (WorkflowName "failingWorkflow") workflowId Nothing executorId (ApplicationVersion "v1")
          -- The replay reports the same failure string the first run did.
          second @?= first
          readIORef executions >>= (@?= 1),
      testCase "a database failure escapes without recording an ERROR outcome" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-dbdown"
          executorId <- freshExecutorId "hs-exec-dbdown"
          registry <- case registerWorkflow
            (WorkflowName "dbDownWorkflow")
            (\_ _ _ -> throwIO (Backend (BackendError "pool exhausted" Nothing Connection)))
            emptyRegistry of
            Left err -> fail ("registration failed: " <> show err)
            Right registry -> pure registry
          -- A transient outage is not a body failure: it must reach the
          -- caller as an exception, leaving the row PENDING for recovery
          -- instead of a permanent ERROR.
          outcome <-
            try (runWorkflow pool registry (WorkflowName "dbDownWorkflow") workflowId Nothing executorId (ApplicationVersion "v1")) ::
              IO (Either Error (Either WorkflowRunError SerializedWorkflowValue))
          case outcome of
            Left (Backend _) -> pure ()
            Left other -> fail ("expected a backend failure, got: " <> show other)
            Right _ -> fail "expected the database failure to escape"
          status <- fetchWorkflowStatus pool workflowId
          status @?= Just Pending,
      testCase "a run against another executor's claim loses it without running" $        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-claim"
          ownerId <- freshExecutorId "hs-exec-owner"
          decision <- tryStartWorkflow pool workflowId (WorkflowName "claimedWorkflow") Nothing ownerId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          executions <- newIORef (0 :: Int)
          registry <- case registerWorkflow
            (WorkflowName "claimedWorkflow")
            (\_ _ _ -> increment executions)
            emptyRegistry of
            Left err -> fail ("registration failed: " <> show err)
            Right registry -> pure registry
          thiefId <- freshExecutorId "hs-exec-thief"
          result <- runWorkflow pool registry (WorkflowName "claimedWorkflow") workflowId Nothing thiefId (ApplicationVersion "v1")
          result @?= Left (WorkflowClaimLost workflowId)
          readIORef executions >>= (@?= 0),
      testCase "rejects a duplicate workflow registration" $ do
        let first =
              registerWorkflow
                (WorkflowName "dupWorkflow")
                (\_ _ _ -> pure (encodeWorkflowValue ()))
                emptyRegistry
        case first of
          Left err -> fail ("first registration failed: " <> show err)
          Right registry ->
            case registerWorkflow
              (WorkflowName "dupWorkflow")
              (\_ _ _ -> pure (encodeWorkflowValue ()))
              registry of
              Left (DuplicateWorkflowName name) -> name @?= WorkflowName "dupWorkflow"
              Right _ -> fail "expected a duplicate registration to fail",
      testCase "running an unregistered workflow name fails" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-unknown"
          executorId <- freshExecutorId "hs-exec-unknown"
          result <- runWorkflow pool emptyRegistry (WorkflowName "missingWorkflow") workflowId Nothing executorId (ApplicationVersion "v1")
          case result of
            Left (WorkflowNotRegistered _) -> pure ()
            other -> fail ("expected WorkflowNotRegistered, got: " <> show other),
      testCase "recovery re-enqueues workflows a dead executor left PENDING" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-abandoned"
          deadId <- freshExecutorId "hs-dead-exec"
          decision <- tryStartWorkflow pool workflowId (WorkflowName "abandonedWorkflow") Nothing deadId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          recovered <-
            reenqueueForRecovery
              pool
              deadId
              (ApplicationVersion "v1")
              internalQueueName
          assertBool ("expected the abandoned workflow back: " <> show recovered) (workflowId `elem` recovered)
          row <- fetchWorkflowExecutionRow pool workflowId
          case row of
            Nothing -> fail "expected a workflow_status row"
            Just statusRow -> case parseWorkflowExecution statusRow of
              Left err -> fail ("row failed to parse: " <> show err)
              Right execution -> execution.workflowExecutionStatus @?= Enqueued,
      testCase "a second sweep after recovery finds nothing to move" $
        withDBOSPool $ \pool -> do
          recovered <-
            reenqueueForRecovery
              pool
              (ExecutorId "hs-no-such-executor")
              (ApplicationVersion "v1")
              internalQueueName
          recovered @?= [],
      testCase "launch recovers the previous run's abandoned workflows" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-launch"
          launchId <- freshExecutorId "hs-launch-exec"
          decision <- tryStartWorkflow pool workflowId (WorkflowName "abandonedLaunchWorkflow") Nothing launchId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          -- The launch names the same executor, so it recovers what that
          -- executor abandoned.
          (_executor, recovered) <-
            launchExecutor pool launchId (ApplicationVersion "v1") emptyRegistry nullLogAction
          assertBool ("expected the abandoned workflow back: " <> show recovered) (workflowId `elem` recovered)
          status <- fetchWorkflowStatus pool workflowId
          status @?= Just Enqueued,
      testCase "shutdown leaves running workflows PENDING for the next launch" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-shutdown"
          claimId <- freshExecutorId "hs-claim-exec"
          registry <- case registerWorkflow
            (WorkflowName "blockWorkflow")
            (\_ _ _ -> threadDelay 30000000 >> pure (encodeWorkflowValue ()))
            emptyRegistry of
            Left err -> fail ("registration failed: " <> show err)
            Right registry -> pure registry
          -- The launch owns the row it is about to run: starting under any
          -- other id would lose the claim instead of executing. The start
          -- comes after the launch so the launch's own recovery sweep does
          -- not re-enqueue the just-started row first.
          (executor, _) <- launchExecutor pool claimId (ApplicationVersion "v1") registry nullLogAction
          decision <- tryStartWorkflow pool workflowId (WorkflowName "blockWorkflow") Nothing claimId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          task <- spawnWorkflow executor (WorkflowName "blockWorkflow") workflowId Nothing
          awaitRow pool workflowId 50
          shutdownExecutor executor
          status <- fetchWorkflowStatus pool workflowId
          status @?= Just Pending
          finished <- poll task
          assertBool "expected the cancelled task to be done" (isJust finished),
      testCase "a queue runs three of five and honours a raised limit" $
        withDBOSPool $ \pool -> do
          queueName <- uniqueQueueName "hs-starter-queue"
          registerQueue pool queueName 3 LeaveExisting
          stored <- fetchQueueWorkerConcurrency pool queueName
          stored @?= Just 3
          executorId <- freshExecutorId "hs-exec-queue"
          workflowIds <- traverse (const (freshWorkflowId "hs-starter-queued")) [1 .. 5 :: Int]
          mapM_
            ( \workflowId ->
                enqueueWorkflow pool workflowId (WorkflowName "starterQueuedWorkflow") queueName
            )
            workflowIds
          firstBatch <- dequeueWorkflows pool queueName executorId (ApplicationVersion "v1")
          length firstBatch @?= 3
          secondBatch <- dequeueWorkflows pool queueName executorId (ApplicationVersion "v1")
          secondBatch @?= []
          updateQueueWorkerConcurrency pool queueName 5
          raised <- fetchQueueWorkerConcurrency pool queueName
          raised @?= Just 5
          thirdBatch <- dequeueWorkflows pool queueName executorId (ApplicationVersion "v1")
          length thirdBatch @?= 2
          let allClaimed = firstBatch <> thirdBatch
          statuses <- fetchWorkflowStatuses pool allClaimed
          map snd statuses @?= replicate 5 Pending,
      testCase "registering a queue with LeaveExisting leaves an existing row alone" $
        withDBOSPool $ \pool -> do
          queueName <- uniqueQueueName "hs-starter-queue-keep"
          registerQueue pool queueName 3 LeaveExisting
          updateQueueWorkerConcurrency pool queueName 7
          registerQueue pool queueName 3 LeaveExisting
          stored <- fetchQueueWorkerConcurrency pool queueName
          stored @?= Just 7,
      testCase "crash mid-run and resume after the last finished step" $
        withDBOSPool $ \pool -> do
          workflowId <- freshWorkflowId "hs-starter-crash"
          executorId <- freshExecutorId "hs-exec-crash"
          decision <- tryStartWorkflow pool workflowId (WorkflowName "ExampleWorkflow") Nothing executorId (ApplicationVersion "v1")
          decision @?= StartWorkflow
          executions <- newIORef (0 :: Int)

          -- First attempt: step one finishes, then the process "crashes"
          -- before step two. The step row and the event are already durable.
          firstAttempt <-
            try (exampleWorkflow pool workflowId executions (Just (OperationId 2)))
          case firstAttempt of
            Left SimulatedCrash -> pure ()
            Right _ -> fail "expected the first attempt to crash"

          publishedAfterCrash <- getEvent pool workflowId "steps_event"
          (decodeWorkflowValue "result" publishedAfterCrash :: Either CodecError Int) @?= Right 1

          -- Restart: the same workflow body runs again. Step one replays from
          -- its checkpoint instead of running a second time.
          exampleWorkflow pool workflowId executions Nothing
          readIORef executions >>= (@?= 3)

          finalStep <- getEvent pool workflowId "steps_event"
          (decodeWorkflowValue "result" finalStep :: Either CodecError Int) @?= Right 3,
      testCase "a supervisor pass claims up to the limit while work is running" $
        withDBOSPool $ \pool -> do
          queueName <- uniqueQueueName "hs-starter-super"
          registerQueue pool queueName 2 LeaveExisting
          executorId <- freshExecutorId "hs-exec-super"
          workflowIds <- traverse (const (freshWorkflowId "hs-starter-super-wf")) [1 .. 3 :: Int]
          mapM_
            (\workflowId -> enqueueWorkflow pool workflowId (WorkflowName "blockQueuedWorkflow") queueName)
            workflowIds
          registry <- case registerWorkflow
            (WorkflowName "blockQueuedWorkflow")
            (\_ _ _ -> threadDelay 30000000 >> pure (encodeWorkflowValue ("done" :: Text)))
            emptyRegistry of
            Left err -> fail ("registration failed: " <> show err)
            Right registry -> pure registry
          (executor, _) <-
            launchExecutor pool executorId (ApplicationVersion "v1") registry nullLogAction
          first <- dequeuePass executor queueName
          length first @?= 2
          -- The claimed bodies are still blocked, so still PENDING: nothing
          -- more is claimable under the limit.
          second <- dequeuePass executor queueName
          length second @?= 0
          shutdownExecutor executor
          statuses <- fetchWorkflowStatuses pool workflowIds
          let pendingCount = length (filter ((== Pending) . snd) statuses)
          pendingCount @?= 2,
      testCase "a skipped claim is released so a later pass picks it up" $
        withDBOSPool $ \pool -> do
          queueName <- uniqueQueueName "hs-starter-skip"
          registerQueue pool queueName 3 LeaveExisting
          workflowId <- freshWorkflowId "hs-starter-skip-wf"
          enqueueWorkflow pool workflowId (WorkflowName "lateWorkflow") queueName
          skipId <- freshExecutorId "hs-exec-skip"
          (skipper, _) <-
            launchExecutor pool skipId (ApplicationVersion "v1") emptyRegistry nullLogAction
          first <- dequeuePass skipper queueName
          length first @?= 0
          -- The skip must not park the claim PENDING: nothing would ever
          -- requeue it while this executor lives, so the supervisor would
          -- spin with zero progress.
          status <- fetchWorkflowStatus pool workflowId
          status @?= Just Enqueued
          shutdownExecutor skipper
          registry <- case registerWorkflow
            (WorkflowName "lateWorkflow")
            (\_ _ _ -> pure (encodeWorkflowValue ("done" :: Text)))
            emptyRegistry of
            Left err -> fail ("registration failed: " <> show err)
            Right registry -> pure registry
          runId <- freshExecutorId "hs-exec-skip-run"
          (runner, _) <-
            launchExecutor pool runId (ApplicationVersion "v1") registry nullLogAction
          second <- dequeuePass runner queueName
          length second @?= 1
          mapM_ wait second
          shutdownExecutor runner
          final <- fetchWorkflowStatus pool workflowId
          final @?= Just Success,
      testCase "concurrent spawns stay tracked for shutdown" $
        withDBOSPool $ \pool -> do
          executorId <- freshExecutorId "hs-exec-track"
          registry <- case registerWorkflow
            (WorkflowName "trackWorkflow")
            (\_ _ _ -> threadDelay 30000000 >> pure (encodeWorkflowValue ("done" :: Text)))
            emptyRegistry of
            Left err -> fail ("registration failed: " <> show err)
            Right registry -> pure registry
          (executor, _) <-
            launchExecutor pool executorId (ApplicationVersion "v1") registry nullLogAction
          workflowIds <- traverse (const (freshWorkflowId "hs-starter-track-wf")) [1 .. 5 :: Int]
          _ <- mapConcurrently (\workflowId -> spawnWorkflow executor (WorkflowName "trackWorkflow") workflowId Nothing) workflowIds
          -- Every arrival survives pruning, even under concurrent spawns, so
          -- shutdown reaches every one of them.
          tracked <- readTVarIO executor.executorTasks
          length tracked @?= 5
          shutdownExecutor executor,
      testCase "dispatched work runs to SUCCESS" $
        withDBOSPool $ \pool -> do
          queueName <- uniqueQueueName "hs-starter-done"
          registerQueue pool queueName 3 LeaveExisting
          executorId <- freshExecutorId "hs-exec-done"
          workflowIds <- traverse (const (freshWorkflowId "hs-starter-done-wf")) [1 .. 3 :: Int]
          mapM_
            (\workflowId -> enqueueWorkflow pool workflowId (WorkflowName "quickWorkflow") queueName)
            workflowIds
          registry <- case registerWorkflow
            (WorkflowName "quickWorkflow")
            (\_ _ _ -> pure (encodeWorkflowValue ("done" :: Text)))
            emptyRegistry of
            Left err -> fail ("registration failed: " <> show err)
            Right registry -> pure registry
          (executor, _) <-
            launchExecutor pool executorId (ApplicationVersion "v1") registry nullLogAction
          dispatched <- dequeuePass executor queueName
          length dispatched @?= 3
          mapM_ wait dispatched
          statuses <- fetchWorkflowStatuses pool workflowIds
          map snd statuses @?= replicate 3 Success,
      testCase "the supervisor loop dispatches queued work until stopped" $
        withDBOSPool $ \pool -> do
          queueName <- uniqueQueueName "hs-starter-loop"
          registerQueue pool queueName 10 LeaveExisting
          executorId <- freshExecutorId "hs-exec-loop"
          workflowIds <- traverse (const (freshWorkflowId "hs-starter-loop-wf")) [1 .. 2 :: Int]
          mapM_
            (\workflowId -> enqueueWorkflow pool workflowId (WorkflowName "loopWorkflow") queueName)
            workflowIds
          registry <- case registerWorkflow
            (WorkflowName "loopWorkflow")
            (\_ _ _ -> pure (encodeWorkflowValue ("done" :: Text)))
            emptyRegistry of
            Left err -> fail ("registration failed: " <> show err)
            Right registry -> pure registry
          (executor, _) <-
            launchExecutor pool executorId (ApplicationVersion "v1") registry nullLogAction
          withAsync (superviseForever executor [queueName] (millisDuration 50)) $ \_ ->
            awaitSuccesses pool workflowIds 100
          statuses <- fetchWorkflowStatuses pool workflowIds
          map snd statuses @?= replicate 2 Success
    ]

awaitSuccesses :: Pool.Pool -> [WorkflowId] -> Int -> IO ()
awaitSuccesses pool workflowIds remaining = do
  statuses <- fetchWorkflowStatuses pool workflowIds
  if all (== Success) (map snd statuses)
    then pure ()
    else
      if remaining <= 0
        then fail ("workflows never finished: " <> show statuses)
        else threadDelay 100000 >> awaitSuccesses pool workflowIds (remaining - 1)

-- | The starter's ExampleWorkflow, mirrored: three steps and a progress event
-- after each. The optional crash id names the step the first attempt dies
-- before starting, simulating a process crash between two committed steps.
exampleWorkflow :: Pool.Pool -> WorkflowId -> IORef Int -> Maybe OperationId -> IO ()
exampleWorkflow pool workflowId executions crashAfter = do
  crashBefore (OperationId 1)
  _ <- runStep (postgresStepStore pool) workflowId (OperationId 1) (OperationName "step_one") (countedStep executions)
  setEvent pool workflowId "steps_event" (encodeWorkflowValue (1 :: Int))
  crashBefore (OperationId 2)
  _ <- runStep (postgresStepStore pool) workflowId (OperationId 2) (OperationName "step_two") (countedStep executions)
  setEvent pool workflowId "steps_event" (encodeWorkflowValue (2 :: Int))
  crashBefore (OperationId 3)
  _ <- runStep (postgresStepStore pool) workflowId (OperationId 3) (OperationName "step_three") (countedStep executions)
  setEvent pool workflowId "steps_event" (encodeWorkflowValue (3 :: Int))
  where
    crashBefore stepId =
      case crashAfter of
        Just crashId | crashId == stepId -> throwIO SimulatedCrash
        _ -> pure ()
    countedStep stepExecutions = do
      atomicModifyIORef' stepExecutions (\count -> (count + 1, ()))
      pure (encodeWorkflowValue ())

-- | The crash button, as an exception.
data SimulatedCrash = SimulatedCrash
  deriving stock (Eq, Show)

instance Exception SimulatedCrash

awaitRow :: Pool.Pool -> WorkflowId -> Int -> IO ()
awaitRow pool workflowId remaining = do
  row <- fetchWorkflowExecutionRow pool workflowId
  case row of
    Just _ -> pure ()
    Nothing
      | remaining <= 0 -> fail "workflow row never appeared"
      | otherwise -> threadDelay 100000 >> awaitRow pool workflowId (remaining - 1)

uniqueQueueName :: Text -> IO QueueName
uniqueQueueName prefix =
  (QueueName . (prefix <>) . ("-" <>) . UUID.toText) <$> UUID.V4.nextRandom

uniqueWorkflowName :: Text -> IO WorkflowName
uniqueWorkflowName prefix =
  (WorkflowName . (prefix <>) . ("-" <>) . UUID.toText) <$> UUID.V4.nextRandom

workflowNameText :: WorkflowName -> Text
workflowNameText (WorkflowName name) = name

increment :: IORef Int -> IO SerializedWorkflowValue
increment executions = do
  atomicModifyIORef' executions (\count -> (count + 1, ()))
  pure (encodeWorkflowValue (object ["ok" .= True]))

withDBOSPool :: (Pool.Pool -> IO a) -> IO a
withDBOSPool = bracket acquirePool releasePool

freshWorkflowId :: Text -> IO WorkflowId
freshWorkflowId prefix =
  (WorkflowId . (prefix <>) . ("-" <>) . UUID.toText) <$> UUID.V4.nextRandom

freshExecutorId :: Text -> IO ExecutorId
freshExecutorId prefix =
  (ExecutorId . (prefix <>) . ("-" <>) . UUID.toText) <$> UUID.V4.nextRandom

currentTimeMillis :: IO Int64
currentTimeMillis = round . (* 1000) <$> getPOSIXTime

assertAtLeast :: Int64 -> Int64 -> IO ()
assertAtLeast actual bound =
  assertBool ("expected at least " <> show bound <> "ms, got " <> show actual) (actual >= bound)
