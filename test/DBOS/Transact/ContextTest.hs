{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The context seam: a 'Ctx' is threaded explicitly, readers answer from
-- it, and step scopes are rebound rather than mutated. The stub backend
-- exists so a context can be built without a database; it is the seed of
-- the P7.7 in-memory backend.
module DBOS.Transact.ContextTest (tests, stubConnection, testIdentity, testCtx, ctxOver) where

import DBOS.Prelude
import Control.Monad.IO.Class (liftIO)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.List (isInfixOf)
import Data.Text (Text)
import DBOS.SystemDB (SystemDB (..))
import DBOS.SystemDB.Postgres (PostgresSystemDB)
import DBOS.Transact
  ( Connection (..),
    Ctx,
    Identity (..),
    Owner (..),
    Serializer (..),
    SomeSystemDB (..),
    StepMarker (..),
    cancelToken,
    cancellationToken,
    currentConnection,
    currentIdentity,
    deadline,
    firstStepStatus,
    inStep,
    isSameExecution,
    newConnection,
    newCtx,
    newWorkflowState,
    nextAttempt,
    nextExecutionIdentity,
    nextStepId,
    nextStepMarker,
    secondsDuration,
    stepId,
    stepStatus,
    stepStatusCurrentAttempt,
    stepStatusId,
    stepStatusMaxAttempts,
    tokenCancelled,
    uuidEntropy,
    uuidWorkflowId,
    withAttempt,
    workflowId,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

-- | A backend that refuses every call: enough to build a context for tests
-- that never reach the database.
data StubDB = StubDB

instance SystemDB StubDB IO where
  initWorkflow = stub
  getWorkflow = stub
  listWorkflows = stub
  getWorkflowChildren = stub
  recordWorkflowOutcome = stub
  awaitWorkflowResult = stub
  awaitFirstWorkflowId = stub
  awaitWorkflowIds = stub
  setWorkflowDelay = stub
  clearQueueAssignment = stub
  updateWorkflowAttributes = stub
  reenqueueForRecovery = stub
  transitionDelayedWorkflows = stub
  cancelWorkflows = stub
  resumeWorkflows = stub
  deleteWorkflows = stub
  forkWorkflows = stub
  forkFrom = stub
  sendMessage = stub
  sendMessages = stub
  recv = stub
  writeStream = stub
  closeStream = stub
  close = stub
  checkStep = stub
  recordStep = stub
  listWorkflowSteps = stub
  recordSleep = stub
  setEvent = stub
  getEvent = stub
  getAllNotifications = stub
  getAllEvents = stub
  readStreamValue = stub
  getAllStreamEntries = stub
  createApplicationVersion = stub
  listApplicationVersions = stub
  getLatestApplicationVersion = stub
  updateApplicationVersionTimestamp = stub
  upsertQueue = stub
  startQueuedWorkflows = stub
  getQueuePartitions = stub
  startQueuedPartitionedWorkflows = stub
  getQueue = stub
  listQueues = stub
  updateQueue = stub
  debounceDelayedWorkflow = stub
  getDeduplicationKeyHolder = stub
  deleteQueue = stub
  createSchedule = stub
  upsertSchedule = stub
  applySchedules = stub
  getSchedule = stub
  listSchedules = stub
  updateSchedule = stub
  setScheduleStatus = stub
  updateScheduleLastFiredAt = stub
  deleteSchedule = stub
  renameApplication = stub
  recordChildWorkflow = stub
  recordChildResult = stub

stub :: a
stub = error "StubDB: this test never reaches the database"

-- | A connection over the stub backend.
stubConnection :: IO (Connection IO)
stubConnection =
  newConnection
    (SomeSystemDB StubDB)
    RustSerde
    (Just "test-app")
    (secondsDuration 1)
    OwnerApplication
    uuidWorkflowId
    uuidEntropy

testIdentity :: Identity
testIdentity =
  Identity
    { identityAppName = "test-app",
      identityAppVersion = "1.0.0",
      identityExecutorId = "test-executor",
      identityAppId = ""
    }

-- | A context for a workflow with no deadline.
testCtx :: IO (Ctx IO)
testCtx = do
  conn <- stubConnection
  identity <- nextExecutionIdentity conn
  state <- newWorkflowState "wf-1" Nothing identity
  newCtx conn testIdentity state

-- | A context over a live backend, for tests that reach the database.
ctxOver :: PostgresSystemDB -> Text -> IO (Ctx IO)
ctxOver backend workflowText = do
  conn <- newConnection (SomeSystemDB backend) RustSerde (Just "test-app") (secondsDuration 1) OwnerApplication uuidWorkflowId uuidEntropy
  identity <- nextExecutionIdentity conn
  state <- newWorkflowState workflowText Nothing identity
  newCtx conn testIdentity state

tests :: TestTree
tests =
  testGroup
    "Context"
    [ testCase "a context reads its workflow id" $ do
        ctx <- testCtx
        workflowId ctx @?= "wf-1",
      testCase "a workflow's step ids are zero based and allocated once" $ do
        ctx <- testCtx
        result <- (,,) <$> nextStepId ctx <*> nextStepId ctx <*> nextStepId ctx
        result @?= (0, 1, 2),
      testCase "step ids stay dense while markers spend their own sequence" $ do
        ctx <- testCtx
        first <- nextStepId ctx
        _ <- nextStepMarker ctx
        second <- nextStepId ctx
        _ <- nextStepMarker ctx
        third <- nextStepId ctx
        (first, second, third) @?= (0, 1, 2),
      testCase "withAttempt scopes a step and leaves the outer scope alone" $ do
        ctx <- testCtx
        innerMarker <- nextStepMarker ctx
        let outside = stepId ctx
        inner <- withAttempt ctx innerMarker (firstStepStatus 4) (pure . stepId)
        let after = stepId ctx
        (outside, inner, after) @?= (Nothing, Just 4, Nothing),
      testCase "a scope reports its status and id" $ do
        ctx <- testCtx
        marker <- nextStepMarker ctx
        let proper = stepStatus ctx
            properFlag = inStep ctx
        scoped <-
          withAttempt ctx marker (firstStepStatus 3) $ \stepped ->
            pure (stepStatus stepped, stepId stepped, inStep stepped)
        case (proper, properFlag, scoped) of
          (Nothing, False, (Just status, Just 3, True)) -> do
            stepStatusId status @?= 3
            stepStatusCurrentAttempt status @?= 1
          other -> fail ("expected proper Nothing and scoped status: " <> show other),
      testCase "a first attempt reports its step, attempt 1 of 1" $ do
        let status = firstStepStatus 3
        stepStatusId status @?= 3
        stepStatusCurrentAttempt status @?= 1
        stepStatusMaxAttempts status @?= 1,
      testCase "a retry keeps the step and moves the attempt" $ do
        let second = nextAttempt (firstStepStatus 3)
        stepStatusId second @?= 3
        stepStatusCurrentAttempt second @?= 2
        stepStatusMaxAttempts second @?= 1,
      testCase "a fresh token is quiet until fired" $ do
        ctx <- testCtx
        token <- cancellationToken ctx
        quiet <- tokenCancelled token
        cancelToken token
        fired <- tokenCancelled token
        (quiet, fired) @?= (False, True),
      testCase "each attempt watches a token of its own" $ do
        ctx <- testCtx
        firstMarker <- nextStepMarker ctx
        secondMarker <- nextStepMarker ctx
        first <- withAttempt ctx firstMarker (firstStepStatus 0) cancellationToken
        second <- withAttempt ctx secondMarker (firstStepStatus 1) cancellationToken
        cancelToken first
        firstFired <- tokenCancelled first
        secondFired <- tokenCancelled second
        (firstFired, secondFired) @?= (True, False),
      testCase "a deadline rides the workflow state" $ do
        ctx <- testCtx
        deadline ctx @?= Nothing,
      testCase "two contexts over one workflow share its step counter" $ do
        conn <- stubConnection
        identity <- nextExecutionIdentity conn
        state <- newWorkflowState "wf-1" Nothing identity
        first <- newCtx conn testIdentity state
        second <- newCtx conn testIdentity state
        one <- nextStepId first
        two <- nextStepId second
        (one, two) @?= (0, 1),
      testCase "a re-run of one id is a different execution" $ do
        conn <- stubConnection
        firstId <- nextExecutionIdentity conn
        secondId <- nextExecutionIdentity conn
        firstState <- newWorkflowState "wf-1" Nothing firstId
        secondState <- newWorkflowState "wf-1" Nothing secondId
        first <- newCtx conn testIdentity firstState
        second <- newCtx conn testIdentity secondState
        isSameExecution first first @?= True
        isSameExecution first second @?= False,
      testCase "the connection and identity travel with the context" $ do
        ctx <- testCtx
        currentIdentity ctx @?= testIdentity
        (currentConnection ctx).connAppName @?= Just ("test-app" :: Text),
      testCase "nested runners isolate" $ do
        conn <- stubConnection
        outerId <- nextExecutionIdentity conn
        innerId <- nextExecutionIdentity conn
        outerState <- newWorkflowState "wf-1" Nothing outerId
        innerState <- newWorkflowState "wf-1" Nothing innerId
        outer <- newCtx conn testIdentity outerState
        inner <- newCtx conn testIdentity innerState
        (workflowId outer, workflowId inner) @?= ("wf-1", "wf-1")
        isSameExecution outer inner @?= False,
      testCase "IO interop runs beside the context" $ do
        ctx <- testCtx
        ref <- newIORef ("" :: Text)
        writeIORef ref "done"
        result <- readIORef ref
        result @?= "done",
      testCase "a throw from an engine call reaches the caller" $ do
        ctx <- testCtx
        outcome <- try (nextStepId ctx >> throwIO (userError "boom") >> pure 0) :: IO (Either SomeException Int)
        case outcome of
          Left err -> assertBool "the original throw escapes" ("boom" `isInfixOf` show err)
          Right _ -> fail "expected the throw to escape",
      testCase "a cooperative flag cancels a wait promptly" $ do
        stop <- newTVarIO False
        done <- newEmptyMVar
        _ <- forkIO (waitForFlag stop done)
        threadDelay 200000
        atomically (writeTVar stop True)
        waitFor (takeMVar done),
      testCase "a fork handed the context shares its counter" $ do
        ctx <- testCtx
        first <- nextStepId ctx
        seen <- newEmptyMVar
        _ <- forkIO (nextStepId ctx >>= putMVar seen)
        second <- waitFor (takeMVar seen)
        (first, second) @?= (0, 1),
      testCase "concurrent contexts are isolated from each other" $ do
        first <- newEmptyMVar
        second <- newEmptyMVar
        let child name box = do
              conn <- stubConnection
              identity <- nextExecutionIdentity conn
              state <- newWorkflowState name Nothing identity
              ctx <- newCtx conn testIdentity state
              putMVar box (workflowId ctx)
        a <- async (child "a" first)
        b <- async (child "b" second)
        wait a
        wait b
        x <- takeMVar first
        y <- takeMVar second
        (x, y) @?= ("a", "b")
    ]

-- | A wait that polls a cooperative flag instead of sleeping through it.
waitForFlag :: StrictTVar IO Bool -> StrictMVar IO () -> IO ()
waitForFlag stop done = do
  flag <- readTVarIO stop
  if flag
    then putMVar done ()
    else threadDelay 50000 >> waitForFlag stop done

-- | A result that must arrive, not a wait that may hang the suite.
waitFor :: IO a -> IO a
waitFor action = do
  result <- timeout 5000000 action
  case result of
    Just value -> pure value
    Nothing -> fail "the waiter was never woken"


