{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | The context seam as test trees polymorphic on 'SystemDB' and
-- 'Tracer': every scenario is written once against @io-classes@
-- constraints and runs over any backend a fixture builds a 'Connection'
-- on. This module holds the scenarios plus the live tree, which runs
-- under @main@ on a real 'PostgresSystemDB' with a FastLogger tracer;
-- 'ContextTestSim' holds the same tree over 'MockSystemDB' for eval.
--
-- 'ctxOver' stays exported for the suites that build their own contexts
-- on top ('EventTest', 'ManagementTest', 'CheckpointTest').
module DBOS.Transact.ContextTest
  ( tests,
    ctxOver,
    connOver,
    Fixture (..),
    scenarioWorkflowId,
    scenarioStepIds,
    scenarioDenseIds,
    scenarioAttemptScope,
    scenarioScopeStatus,
    scenarioFirstAttempt,
    scenarioRetryAttempt,
    scenarioTokenFire,
    scenarioAttemptTokens,
    scenarioDeadline,
    scenarioSharedCounter,
    scenarioRerunIdentity,
    scenarioTravelsWith,
    scenarioNestedRunners,
    scenarioStateInterop,
    scenarioThrowEscape,
    scenarioCoopFlag,
    scenarioForkCounter,
    scenarioNestedScope,
    scenarioTokenOutsideStep,
    scenarioConcurrentIsolation,
    scenarioExecCounters,
    scenarioRaceCancelled,
    scenarioRaceCompletes,
    scenarioStepView,
    checkScopeStatus,
    checkThrowEscape,
  )
where

import DBOS.Prelude
import Control.Monad.Class.MonadThrow qualified as MThrow
import Data.List (isInfixOf)
import Data.Text (Text)
import DBOS.SystemDB.Postgres (PostgresSystemDB)
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  ( Connection (..),
    Ctx,
    Identity (..),
    LogEvent (..),
    Owner (..),
    Serializer (..),
    SomeSystemDB (..),
    SomeTracer (..),
    StepCtx,
    StepStatus (..),
    Timestamp (..),
    WorkflowCtx,
    WorkflowId (..),
    acquireLoggerBackend,
    cancelToken,
    cancellationToken,
    currentConnection,
    currentIdentity,
    deadline,
    firstStepStatus,
    inStep,
    ioTracer,
    isSameExecution,
    newConnection,
    newCtx,
    newWorkflowState,
    nextAttempt,
    nextExecutionIdentity,
    nextStepId,
    nextStepMarker,
    nextWorkflowMarker,
    raceCancel,
    nextWorkflowStepId,
    nullTracer,
    secondsDuration,
    stepCtxId,
    stepCtxStatus,
    stepId,
    stepMarker,
    stepStatus,
    stepStatusCurrentAttempt,
    stepStatusId,
    stepStatusMaxAttempts,
    tokenCancelled,
    uuidEntropy,
    uuidWorkflowId,
    withAttempt,
    withStep,
    withWorkflow,
    workflowId,
  )
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (testCase, (@?=))

-- * Live fixtures: one backend and one FastLogger tracer for the group,
-- passed explicitly — the same polymorphic GADT fields the sim tree fills
-- with its mock backend and sim tracer.

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | A connection over a live backend with an explicit tracer.
connOver :: PostgresSystemDB -> SomeTracer IO -> IO (Connection IO)
connOver backend tracer = do
  instanceId <- uuidWorkflowId
  newConnection
    (SomeSystemDB backend)
    RustSerde
    (Just "test-app")
    (secondsDuration 1)
    OwnerApplication
    instanceId
    uuidWorkflowId
    uuidEntropy
    tracer

testIdentity :: Identity
testIdentity =
  Identity
    { identityAppName = "test-app",
      identityAppVersion = "1.0.0",
      identityExecutorId = "test-executor",
      identityAppId = ""
    }

-- | A context over a live backend with an explicit tracer, for tests that
-- reach the database.
ctxOver :: PostgresSystemDB -> SomeTracer IO -> Text -> IO (Ctx IO)
ctxOver backend tracer workflowText = do
  conn <- connOver backend tracer
  identity <- nextExecutionIdentity conn
  state <- newWorkflowState workflowText Nothing identity
  newCtx conn testIdentity state

-- | The shared tree over a real backend and a FastLogger tracer.
liveFixture :: PostgresSystemDB -> SomeTracer IO -> Fixture IO
liveFixture backend tracer =
  Fixture
    { fixtureMkCtx = ctxOver backend tracer,
      fixtureMkConn = connOver backend tracer,
      fixtureIdentity = testIdentity,
      fixtureAppName = "test-app"
    }

-- * The shared tree.

-- | How a tree instantiation builds its world: contexts and connections
-- over any backend, the identity they carry, and the application name the
-- connection reports.
data Fixture m = Fixture
  { fixtureMkCtx    :: Text -> m (Ctx m),
    fixtureMkConn   :: m (Connection m),
    fixtureIdentity :: Identity,
    fixtureAppName  :: Text
  }

-- * Scenarios: each written once, returning a plain value the trees assert.

scenarioWorkflowId :: MonadSTM m => Fixture m -> m Text
scenarioWorkflowId fx = workflowId <$> fx.fixtureMkCtx "wf-1"

scenarioStepIds :: MonadSTM m => Fixture m -> m (Int, Int, Int)
scenarioStepIds fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  (,,) <$> nextStepId ctx <*> nextStepId ctx <*> nextStepId ctx

scenarioDenseIds :: MonadSTM m => Fixture m -> m (Int, Int, Int)
scenarioDenseIds fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  first <- nextStepId ctx
  _ <- nextStepMarker ctx
  second <- nextStepId ctx
  _ <- nextStepMarker ctx
  third <- nextStepId ctx
  pure (first, second, third)

scenarioAttemptScope :: (MonadSTM m, MonadCatch m) => Fixture m -> m (Maybe Int, Maybe Int, Maybe Int)
scenarioAttemptScope fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  innerMarker <- nextStepMarker ctx
  let outside = stepId ctx
  inner <- withAttempt ctx innerMarker (firstStepStatus 4) (pure . stepId)
  let after = stepId ctx
  pure (outside, inner, after)

scenarioScopeStatus :: (MonadSTM m, MonadCatch m) => Fixture m -> m (Maybe StepStatus, Bool, (Maybe StepStatus, Maybe Int, Bool))
scenarioScopeStatus fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  marker <- nextStepMarker ctx
  let proper = stepStatus ctx
      properFlag = inStep ctx
  scoped <-
    withAttempt ctx marker (firstStepStatus 3) $ \stepped ->
      pure (stepStatus stepped, stepId stepped, inStep stepped)
  pure (proper, properFlag, scoped)

scenarioFirstAttempt :: Applicative m => Fixture m -> m (Int, Word, Word)
scenarioFirstAttempt _ =
  let status = firstStepStatus 3
   in pure (stepStatusId status, stepStatusCurrentAttempt status, stepStatusMaxAttempts status)

scenarioRetryAttempt :: Applicative m => Fixture m -> m (Int, Word, Word)
scenarioRetryAttempt _ =
  let second = nextAttempt (firstStepStatus 3)
   in pure (stepStatusId second, stepStatusCurrentAttempt second, stepStatusMaxAttempts second)

scenarioTokenFire :: MonadSTM m => Fixture m -> m (Bool, Bool)
scenarioTokenFire fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  token <- cancellationToken ctx
  quiet <- tokenCancelled token
  cancelToken token
  fired <- tokenCancelled token
  pure (quiet, fired)

scenarioAttemptTokens :: (MonadSTM m, MonadCatch m) => Fixture m -> m (Bool, Bool)
scenarioAttemptTokens fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  firstMarker <- nextStepMarker ctx
  secondMarker <- nextStepMarker ctx
  first <- withAttempt ctx firstMarker (firstStepStatus 0) cancellationToken
  second <- withAttempt ctx secondMarker (firstStepStatus 1) cancellationToken
  cancelToken first
  firstFired <- tokenCancelled first
  secondFired <- tokenCancelled second
  pure (firstFired, secondFired)

scenarioDeadline :: MonadSTM m => Fixture m -> m (Maybe Timestamp)
scenarioDeadline fx = deadline <$> fx.fixtureMkCtx "wf-1"

scenarioSharedCounter :: MonadSTM m => Fixture m -> m (Int, Int)
scenarioSharedCounter fx = do
  conn <- fx.fixtureMkConn
  identity <- nextExecutionIdentity conn
  state <- newWorkflowState "wf-1" Nothing identity
  first <- newCtx conn fx.fixtureIdentity state
  second <- newCtx conn fx.fixtureIdentity state
  (,) <$> nextStepId first <*> nextStepId second

scenarioRerunIdentity :: MonadSTM m => Fixture m -> m (Bool, Bool)
scenarioRerunIdentity fx = do
  conn <- fx.fixtureMkConn
  firstId <- nextExecutionIdentity conn
  secondId <- nextExecutionIdentity conn
  firstState <- newWorkflowState "wf-1" Nothing firstId
  secondState <- newWorkflowState "wf-1" Nothing secondId
  first <- newCtx conn fx.fixtureIdentity firstState
  second <- newCtx conn fx.fixtureIdentity secondState
  pure (isSameExecution first first, isSameExecution first second)

scenarioTravelsWith :: MonadSTM m => Fixture m -> m (Identity, Maybe Text)
scenarioTravelsWith fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  pure (currentIdentity ctx, (currentConnection ctx).connAppName)

scenarioNestedRunners :: MonadSTM m => Fixture m -> m (Text, Text, Bool)
scenarioNestedRunners fx = do
  conn <- fx.fixtureMkConn
  outerId <- nextExecutionIdentity conn
  innerId <- nextExecutionIdentity conn
  outerState <- newWorkflowState "wf-1" Nothing outerId
  innerState <- newWorkflowState "wf-1" Nothing innerId
  outer <- newCtx conn fx.fixtureIdentity outerState
  inner <- newCtx conn fx.fixtureIdentity innerState
  pure (workflowId outer, workflowId inner, isSameExecution outer inner)

scenarioStateInterop :: MonadSTM m => Fixture m -> m Text
scenarioStateInterop fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  ref <- newTVarIO ("" :: Text)
  atomically (writeTVar ref "done")
  _ <- pure ctx
  readTVarIO ref

scenarioThrowEscape :: (MonadSTM m, MonadCatch m) => Fixture m -> m (Either MThrow.SomeException Int)
scenarioThrowEscape fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  MThrow.try (nextStepId ctx >> MThrow.throwIO (userError "boom") >> pure 0)

scenarioCoopFlag :: (MonadMVar m, MonadFork m, MonadTimer m, MonadThrow m) => Fixture m -> m ()
scenarioCoopFlag _ = do
  stop <- newTVarIO False
  done <- newEmptyMVar
  _ <- forkIO (waitForFlag stop done)
  threadDelay 200000
  atomically (writeTVar stop True)
  waitFor (takeMVar done)

scenarioForkCounter :: (MonadMVar m, MonadFork m, MonadTimer m, MonadThrow m) => Fixture m -> m (Int, Int)
scenarioForkCounter fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  first <- nextStepId ctx
  seen <- newEmptyMVar
  _ <- forkIO (nextStepId ctx >>= putMVar seen)
  second <- waitFor (takeMVar seen)
  pure (first, second)

scenarioNestedScope :: (MonadSTM m, MonadCatch m) => Fixture m -> m (Maybe Int, Maybe Int, Bool)
scenarioNestedScope fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  let outside = stepId ctx
  marker <- nextStepMarker ctx
  (innerId, matches) <-
    withAttempt ctx marker (firstStepStatus 0) $ \inner ->
      pure (stepId inner, stepMarker inner == Just marker)
  pure (outside, innerId, matches)

scenarioTokenOutsideStep :: MonadSTM m => Fixture m -> m Bool
scenarioTokenOutsideStep fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  token <- cancellationToken ctx
  tokenCancelled token

scenarioConcurrentIsolation :: (MonadMVar m, MonadAsync m) => Fixture m -> m (Text, Text)
scenarioConcurrentIsolation fx = do
  first <- newEmptyMVar
  second <- newEmptyMVar
  let child name box = do
        conn <- fx.fixtureMkConn
        identity <- nextExecutionIdentity conn
        state <- newWorkflowState name Nothing identity
        ctx <- newCtx conn fx.fixtureIdentity state
        putMVar box (workflowId ctx)
  a <- async (child "a" first)
  b <- async (child "b" second)
  wait a
  wait b
  (,) <$> takeMVar first <*> takeMVar second

scenarioExecCounters :: MonadSTM m => Fixture m -> m ((Int, Int), Int)
scenarioExecCounters fx = do
  conn <- fx.fixtureMkConn
  let ident = fx.fixtureIdentity
  first <- withWorkflow conn ident (WorkflowId "wf-1") Nothing $ \wctx -> do
    a <- nextWorkflowStepId wctx
    b <- nextWorkflowStepId wctx
    pure (a, b)
  second <- withWorkflow conn ident (WorkflowId "wf-1") Nothing $ \wctx ->
    nextWorkflowStepId wctx
  pure (first, second)

scenarioRaceCompletes :: (MonadSTM m, MonadAsync m) => Fixture m -> m (Maybe Text)
scenarioRaceCompletes fx = do
  ctx <- fx.fixtureMkCtx "wf-race"
  raceCancel ctx (pure "done")

scenarioRaceCancelled :: (MonadSTM m, MonadAsync m, MonadMVar m, MonadCatch m) => Fixture m -> m (Maybe Text)
scenarioRaceCancelled fx = do
  ctx <- fx.fixtureMkCtx "wf-race-cancel"
  marker <- nextStepMarker ctx
  withAttempt ctx marker (firstStepStatus 0) $ \inner -> do
    token <- cancellationToken inner
    cancelToken token
    -- The action never completes; the already-fired token decides it.
    raceCancel inner (takeMVar =<< newEmptyMVar)

scenarioStepView :: (MonadSTM m, MonadCatch m) => Fixture m -> m (Text, Maybe StepStatus)
scenarioStepView fx = do
  conn <- fx.fixtureMkConn
  let ident = fx.fixtureIdentity
  withWorkflow conn ident (WorkflowId "wf-9") Nothing $ \wctx -> do
    marker <- nextWorkflowMarker wctx
    withStep wctx marker (firstStepStatus 7) $ \sctx ->
      pure (stepCtxId sctx, stepCtxStatus sctx)

-- * Shared checks: pure verdicts; both trees turn them into assertions,
-- so the IOSim typed assertions live alongside the same checks here.

checkEq :: (Eq a, Show a) => a -> a -> Either String ()
checkEq expected actual
  | expected == actual = Right ()
  | otherwise = Left ("expected " <> show expected <> ", got " <> show actual)

checkScopeStatus :: (Maybe StepStatus, Bool, (Maybe StepStatus, Maybe Int, Bool)) -> Either String ()
checkScopeStatus result =
  case result of
    (Nothing, False, (Just status, Just 3, True)) ->
      checkEq (3 :: Int, 1 :: Word) (stepStatusId status, stepStatusCurrentAttempt status)
    other -> Left ("expected proper Nothing and scoped status, got: " <> show other)

checkThrowEscape :: Either MThrow.SomeException Int -> Either String ()
checkThrowEscape outcome =
  case outcome of
    Left err
      | "boom" `isInfixOf` show err -> Right ()
      | otherwise -> Left ("the wrong throw escaped: " <> show err)
    Right _ -> Left "expected the throw to escape"

-- * Shared helpers, polymorphic over the same vocabulary.

-- | A wait that polls a cooperative flag instead of sleeping through it.
waitForFlag :: (MonadSTM m, MonadMVar m, MonadDelay m) => StrictTVar m Bool -> StrictMVar m () -> m ()
waitForFlag stop done = do
  flag <- readTVarIO stop
  if flag
    then putMVar done ()
    else threadDelay 50000 >> waitForFlag stop done

-- | A result that must arrive, not a wait that may hang the suite. Under
-- IOSim the timeout is virtual, so a hung wait fails instantly; live it
-- throws after five seconds, which tasty reports as a failure.
waitFor :: (MonadTimer m, MonadThrow m) => m a -> m a
waitFor action = do
  result <- timeout 5000000 action
  case result of
    Just value -> pure value
    Nothing -> MThrow.throwIO (userError "the waiter was never woken")

-- * The live tree: the shared scenarios over a real 'PostgresSystemDB'
-- with a FastLogger tracer, for @main@.

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    withResource acquireLoggerBackend snd $ \getLogger ->
      testGroup
        "Context"
        [ testCase "a context reads its workflow id" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioWorkflowId fx
            res @?= "wf-1",
          testCase "a workflow's step ids are zero based and allocated once" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioStepIds fx
            res @?= (0, 1, 2),
          testCase "step ids stay dense while markers spend their own sequence" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioDenseIds fx
            res @?= (0, 1, 2),
          testCase "withAttempt scopes a step and leaves the outer scope alone" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioAttemptScope fx
            res @?= (Nothing, Just 4, Nothing),
          testCase "a scope reports its status and id" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioScopeStatus fx
            either fail pure (checkScopeStatus res),
          testCase "a first attempt reports its step, attempt 1 of 1" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioFirstAttempt fx
            res @?= (3, 1, 1),
          testCase "a retry keeps the step and moves the attempt" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioRetryAttempt fx
            res @?= (3, 2, 1),
          testCase "a fresh token is quiet until fired" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioTokenFire fx
            res @?= (False, True),
          testCase "each attempt watches a token of its own" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioAttemptTokens fx
            res @?= (True, False),
          testCase "a deadline rides the workflow state" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioDeadline fx
            res @?= Nothing,
          testCase "two contexts over one workflow share its step counter" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioSharedCounter fx
            res @?= (0, 1),
          testCase "a re-run of one id is a different execution" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioRerunIdentity fx
            res @?= (True, False),
          testCase "the connection and identity travel with the context" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioTravelsWith fx
            res @?= (testIdentity, Just ("test-app" :: Text)),
          testCase "nested runners isolate" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioNestedRunners fx
            res @?= ("wf-1", "wf-1", False),
          testCase "state interop runs beside the context" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioStateInterop fx
            res @?= "done",
          testCase "a throw from an engine call reaches the caller" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioThrowEscape fx
            either fail pure (checkThrowEscape res),
          testCase "a cooperative flag cancels a wait promptly" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            scenarioCoopFlag fx,
          testCase "a fork handed the context shares its counter" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioForkCounter fx
            res @?= (0, 1),
          testCase "a nested scope reports the step that encloses it" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioNestedScope fx
            res @?= (Nothing, Just 0, True),
          testCase "a cancellation token outside a step never fires" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioTokenOutsideStep fx
            res @?= False,
          testCase "concurrent contexts are isolated from each other" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioConcurrentIsolation fx
            res @?= ("a", "b"),
          testCase "separate executions own independent step counters" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioExecCounters fx
            res @?= ((0, 1), 0),
          testCase "a step view reads its status with the workflow id" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioStepView fx
            res @?= ("wf-9", Just (firstStepStatus 7)),
          testCase "raceCancel returns the value when the token stays quiet" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioRaceCompletes fx
            res @?= Just "done",
          testCase "raceCancel reports cancellation when the token has fired" $ do
            fx <- liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)
            res <- scenarioRaceCancelled fx
            res @?= Nothing
        ]
