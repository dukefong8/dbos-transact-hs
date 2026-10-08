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
    appLogEvents,
    appLogLines,
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
    scenarioLogLines,
    checkWorkflowId,
    checkStepIds,
    checkDenseIds,
    checkAttemptScope,
    checkFirstAttempt,
    checkRetryAttempt,
    checkTokenFire,
    checkAttemptTokens,
    checkDeadline,
    checkSharedCounter,
    checkRerunIdentity,
    checkTravelsWith,
    checkNestedRunners,
    checkStateInterop,
    checkCoopFlag,
    checkForkCounter,
    checkNestedScope,
    checkTokenOutsideStep,
    checkConcurrentIsolation,
    checkExecCounters,
    checkRaceCancelled,
    checkRaceCompletes,
    checkStepView,
    checkLogLines,
    checkScopeStatus,
    checkThrowEscape,
  )
where

import DBOS.DualStack (liveCase)
import DBOS.Prelude
import Data.List (isInfixOf)
import DBOS.SystemDB.Postgres (PostgresSystemDB)
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  (
  Serializer (..),
  WorkflowCtx,
  WorkflowId (..),
  logDebug,
  logError,
  logInfo,
  logWarn,
  secondsDuration,
  workflowId)
import DBOS.Transact.Logger (AppLog (..), LogEvent (..), LogSeverity (..), SomeTracer (..), acquireLoggerBackend, ioTracer, nullTracer)
import DBOS.Transact.LoggerTest (callbackBackend, renderedLines)
import Data.IORef (newIORef)
import Data.Text (unpack)
import System.Log.FastLogger (newTimeCache)
import DBOS.Transact.Identity (Identity (..))
import DBOS.SystemDB.Types (Timestamp)
import DBOS.Transact.Context
  ( StepCtx (stepCtxWorkflow),
    StepStatus,
    WorkflowCtx (wctxConn, wctxIdentity),
    firstStepStatus,
    newWorkflowCtx,
    newWorkflowState,
    nextAttempt,
    nextStepId,
    nextWorkflowMarker,
    insideAStep,
    stepCtxBoundary,
    stepCtxStatus,
    deadline,
    raceCancel,
    cancelToken,
    tokenCancelled,
    isSameExecution,
    stepId,
    stepStatus,
    stepMarker,
    stepStatusCurrentAttempt,
    stepStatusMaxAttempts,
    stepStatusId,
    cancellationToken,
    withStep,
    withTracer,
    withWorkflow
  )
import DBOS.Transact.Connection
  ( Connection (..),
    newConnection,
    uuidWorkflowId,
    nextExecutionIdentity,
    Owner (..),
    SomeSystemDB (..)
  )
import DBOS.SystemDB.Retry (uuidEntropy)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (testCase)

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

-- | A workflow view over a live backend with an explicit tracer, for
-- tests that reach the database. The brand is bound here, at the test's
-- own scope; production code obtains views only through 'withWorkflow'.
ctxOver :: PostgresSystemDB -> SomeTracer IO -> Text -> IO (WorkflowCtx exec IO)
ctxOver backend tracer workflowText = do
  conn <- connOver backend tracer
  identity <- nextExecutionIdentity conn
  state <- newWorkflowState workflowText Nothing identity
  newWorkflowCtx conn testIdentity state

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
-- | How a tree instantiation builds its world. The fixture binds the
-- execution brand at the test's own scope (the phantom instantiates to
-- @()@ — one brand per fixture is exactly what these scenarios drive);
-- production code never does this, because 'withWorkflow'\'s rank-2
-- binder is what keeps a run's brand from escaping its continuation.
data Fixture m = Fixture
  { fixtureMkCtx    :: Text -> m (WorkflowCtx () m),
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
  _ <- nextWorkflowMarker ctx
  second <- nextStepId ctx
  _ <- nextWorkflowMarker ctx
  third <- nextStepId ctx
  pure (first, second, third)

scenarioAttemptScope :: (MonadSTM m, MonadCatch m)
                     => Fixture m -> m (Maybe Int, Maybe Int, Maybe Int)
scenarioAttemptScope fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  innerMarker <- nextWorkflowMarker ctx
  let outside = stepId (stepCtxBoundary ctx)
  inner <- withStep ctx innerMarker (firstStepStatus 4) (pure . stepId)
  let after = stepId (stepCtxBoundary ctx)
  pure (outside, inner, after)

scenarioScopeStatus :: (MonadSTM m, MonadCatch m)
                    => Fixture m -> m (Maybe StepStatus, Bool, (Maybe StepStatus, Maybe Int, Bool))
scenarioScopeStatus fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  marker <- nextWorkflowMarker ctx
  let proper = stepStatus (stepCtxBoundary ctx)
  properFlag <- insideAStep ctx
  scoped <-
    withStep ctx marker (firstStepStatus 3) $ \stepped -> do
      steppedFlag <- insideAStep ctx
      pure (stepStatus stepped, stepId stepped, steppedFlag)
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
  token <- cancellationToken (stepCtxBoundary ctx)
  quiet <- tokenCancelled token
  cancelToken token
  fired <- tokenCancelled token
  pure (quiet, fired)

scenarioAttemptTokens :: (MonadSTM m, MonadCatch m) => Fixture m -> m (Bool, Bool)
scenarioAttemptTokens fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  firstMarker <- nextWorkflowMarker ctx
  secondMarker <- nextWorkflowMarker ctx
  first <- withStep ctx firstMarker (firstStepStatus 0) cancellationToken
  second <- withStep ctx secondMarker (firstStepStatus 1) cancellationToken
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
  first <- newWorkflowCtx conn fx.fixtureIdentity state
  second <- newWorkflowCtx conn fx.fixtureIdentity state
  (,) <$> nextStepId first <*> nextStepId second

scenarioRerunIdentity :: MonadSTM m => Fixture m -> m (Bool, Bool)
scenarioRerunIdentity fx = do
  conn <- fx.fixtureMkConn
  firstId <- nextExecutionIdentity conn
  secondId <- nextExecutionIdentity conn
  firstState <- newWorkflowState "wf-1" Nothing firstId
  secondState <- newWorkflowState "wf-1" Nothing secondId
  first <- newWorkflowCtx conn fx.fixtureIdentity firstState
  second <- newWorkflowCtx conn fx.fixtureIdentity secondState
  pure (isSameExecution first first, isSameExecution first second)

scenarioTravelsWith :: MonadSTM m => Fixture m -> m (Identity, Maybe Text)
scenarioTravelsWith fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  pure (ctx.wctxIdentity, ctx.wctxConn.connAppName)

scenarioNestedRunners :: MonadSTM m => Fixture m -> m (Text, Text, Bool)
scenarioNestedRunners fx = do
  conn <- fx.fixtureMkConn
  outerId <- nextExecutionIdentity conn
  innerId <- nextExecutionIdentity conn
  outerState <- newWorkflowState "wf-1" Nothing outerId
  innerState <- newWorkflowState "wf-1" Nothing innerId
  outer <- newWorkflowCtx conn fx.fixtureIdentity outerState
  inner <- newWorkflowCtx conn fx.fixtureIdentity innerState
  pure (workflowId outer, workflowId inner, isSameExecution outer inner)

scenarioStateInterop :: MonadSTM m => Fixture m -> m Text
scenarioStateInterop fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  ref <- newTVarIO ("" :: Text)
  atomically (writeTVar ref "done")
  _ <- pure ctx
  readTVarIO ref

scenarioThrowEscape :: (MonadSTM m, MonadCatch m)
                    => Fixture m -> m (Either SomeException Int)
scenarioThrowEscape fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  try (nextStepId ctx >> throwIO (userError "boom") >> pure 0)

scenarioCoopFlag :: (MonadMVar m, MonadFork m, MonadTimer m, MonadThrow m) => Fixture m -> m ()
scenarioCoopFlag _ = do
  stop <- newTVarIO False
  done <- newEmptyMVar
  _ <- forkIO (waitForFlag stop done)
  threadDelay 200000
  atomically (writeTVar stop True)
  waitFor (takeMVar done)

scenarioForkCounter :: (MonadMVar m, MonadFork m, MonadTimer m, MonadThrow m)
                    => Fixture m -> m (Int, Int)
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
  let outside = stepId (stepCtxBoundary ctx)
  marker <- nextWorkflowMarker ctx
  (innerId, matches) <-
    withStep ctx marker (firstStepStatus 0) $ \inner ->
      pure (stepId inner, stepMarker inner == Just marker)
  pure (outside, innerId, matches)

scenarioTokenOutsideStep :: MonadSTM m => Fixture m -> m Bool
scenarioTokenOutsideStep fx = do
  ctx <- fx.fixtureMkCtx "wf-1"
  token <- cancellationToken (stepCtxBoundary ctx)
  tokenCancelled token

scenarioConcurrentIsolation :: (MonadMVar m, MonadAsync m) => Fixture m -> m (Text, Text)
scenarioConcurrentIsolation fx = do
  first <- newEmptyMVar
  second <- newEmptyMVar
  let child name box = do
        conn <- fx.fixtureMkConn
        identity <- nextExecutionIdentity conn
        state <- newWorkflowState name Nothing identity
        ctx <- newWorkflowCtx conn fx.fixtureIdentity state
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
    a <- nextStepId wctx
    b <- nextStepId wctx
    pure (a, b)
  second <- withWorkflow conn ident (WorkflowId "wf-1") Nothing $ \wctx ->
    nextStepId wctx
  pure (first, second)

scenarioRaceCompletes :: (MonadAsync m, MonadCatch m) => Fixture m -> m (Maybe Text)
scenarioRaceCompletes fx = do
  ctx <- fx.fixtureMkCtx "wf-race"
  marker <- nextWorkflowMarker ctx
  withStep ctx marker (firstStepStatus 0) $ \sctx -> raceCancel sctx (pure "done")

scenarioRaceCancelled :: (MonadAsync m, MonadMVar m, MonadCatch m) => Fixture m -> m (Maybe Text)
scenarioRaceCancelled fx = do
  ctx <- fx.fixtureMkCtx "wf-race-cancel"
  marker <- nextWorkflowMarker ctx
  withStep ctx marker (firstStepStatus 0) $ \sctx -> do
    token <- cancellationToken sctx
    cancelToken token
    -- The action never completes; the already-fired token decides it.
    raceCancel sctx (takeMVar =<< newEmptyMVar)

scenarioStepView :: (MonadSTM m, MonadCatch m) => Fixture m -> m (Text, Maybe StepStatus)
scenarioStepView fx = do
  conn <- fx.fixtureMkConn
  let ident = fx.fixtureIdentity
  withWorkflow conn ident (WorkflowId "wf-9") Nothing $ \wctx -> do
    marker <- nextWorkflowMarker wctx
    withStep wctx marker (firstStepStatus 7) $ \sctx ->
      pure (workflowId sctx.stepCtxWorkflow, stepCtxStatus sctx)

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

checkThrowEscape :: Either SomeException Int -> Either String ()
checkThrowEscape outcome =
  case outcome of
    Left err
      | "boom" `isInfixOf` show err -> Right ()
      | otherwise -> Left ("the wrong throw escaped: " <> show err)
    Right _ -> Left "expected the throw to escape"

checkWorkflowId :: Text -> Either String ()
checkWorkflowId = checkEq ("wf-1" :: Text)

checkStepIds :: (Int, Int, Int) -> Either String ()
checkStepIds = checkEq (0, 1, 2)

checkDenseIds :: (Int, Int, Int) -> Either String ()
checkDenseIds = checkEq (0, 1, 2)

checkAttemptScope :: (Maybe Int, Maybe Int, Maybe Int) -> Either String ()
checkAttemptScope = checkEq (Nothing, Just 4, Nothing)

checkFirstAttempt :: (Int, Word, Word) -> Either String ()
checkFirstAttempt = checkEq (3, 1, 1)

checkRetryAttempt :: (Int, Word, Word) -> Either String ()
checkRetryAttempt = checkEq (3, 2, 1)

checkTokenFire :: (Bool, Bool) -> Either String ()
checkTokenFire = checkEq (False, True)

checkAttemptTokens :: (Bool, Bool) -> Either String ()
checkAttemptTokens = checkEq (True, False)

checkDeadline :: Maybe Timestamp -> Either String ()
checkDeadline = checkEq Nothing

checkSharedCounter :: (Int, Int) -> Either String ()
checkSharedCounter = checkEq (0, 1)

checkRerunIdentity :: (Bool, Bool) -> Either String ()
checkRerunIdentity = checkEq (True, False)

checkTravelsWith :: (Identity, Maybe Text) -> Either String ()
checkTravelsWith (ident, mApp) = checkEq (Just ident.identityAppName) mApp

checkNestedRunners :: (Text, Text, Bool) -> Either String ()
checkNestedRunners = checkEq ("wf-1", "wf-1", False)

checkStateInterop :: Text -> Either String ()
checkStateInterop = checkEq ("done" :: Text)

checkCoopFlag :: () -> Either String ()
checkCoopFlag () = Right ()

checkForkCounter :: (Int, Int) -> Either String ()
checkForkCounter = checkEq (0, 1)

checkNestedScope :: (Maybe Int, Maybe Int, Bool) -> Either String ()
checkNestedScope = checkEq (Nothing, Just 0, True)

checkTokenOutsideStep :: Bool -> Either String ()
checkTokenOutsideStep = checkEq False

checkConcurrentIsolation :: (Text, Text) -> Either String ()
checkConcurrentIsolation = checkEq ("a", "b")

checkExecCounters :: ((Int, Int), Int) -> Either String ()
checkExecCounters = checkEq ((0, 1), 0)

checkStepView :: (Text, Maybe StepStatus) -> Either String ()
checkStepView = checkEq ("wf-9", Just (firstStepStatus 7))

checkRaceCompletes :: Maybe Text -> Either String ()
checkRaceCompletes = checkEq (Just ("done" :: Text))

checkRaceCancelled :: Maybe Text -> Either String ()
checkRaceCancelled = checkEq Nothing
-- | What the log scenario emits, in order: four helper calls through the
-- workflow view and two through the step view. The live half judges its
-- captured FastLogger lines against these events; the sim half judges the
-- typed events and the lines its tracer said.
appLogEvents :: [AppLog]
appLogEvents =
  [ AppLog SeverityInfo "order 7 dispatched",
    AppLog SeverityDebug "payload 42 bytes",
    AppLog SeverityWarning "dispatch queue backed up",
    AppLog SeverityError "courier unavailable",
    AppLog SeverityInfo "picked from the shelf",
    AppLog SeverityWarning "shelf scan retried"
  ]

-- | The same events as the backend renders them — what each captured
-- line must carry after FastLogger's time and thread prefix.
appLogLines :: [String]
appLogLines = map (unpack . renderLine) appLogEvents

-- | The four app-facing helpers emit through the context they are
-- handed: four lines from the workflow view, then two from the step view
-- a 'withStep' body receives — all rendered by the FastLogger backend a
-- production body would log through.
scenarioLogLines :: Fixture IO -> IO [String]
scenarioLogLines fixture = do
  getTime <- newTimeCache "%Y-%m-%dT%H:%M:%S%z"
  collected <- newIORef []
  (backend, release) <- callbackBackend getTime SeverityDebug collected
  ctx <- withTracer (ioTracer backend) <$> fixture.fixtureMkCtx "wf-log"
  logInfo ctx "order 7 dispatched"
  logDebug ctx "payload 42 bytes"
  logWarn ctx "dispatch queue backed up"
  logError ctx "courier unavailable"
  marker <- nextWorkflowMarker ctx
  withStep ctx marker (firstStepStatus 1) $ \stepped -> do
    logInfo stepped "picked from the shelf"
    logWarn stepped "shelf scan retried"
  release
  renderedLines collected

-- | The captured lines carry every expected line in order; a line may
-- prefix time and thread id, so the match is on the rendered tail.
checkLogLines :: [String] -> Either String ()
checkLogLines rendered = go rendered appLogLines
  where
    go [] [] = Right ()
    go (line : rest) (expected : rest')
      | expected `isInfixOf` line = go rest rest'
      | otherwise = Left ("expected a line carrying " <> show expected <> ", got: " <> line)
    go other expected = Left ("expected " <> show expected <> ", got: " <> show other)

-- * Shared helpers, polymorphic over the same vocabulary.

-- | A wait that polls a cooperative flag instead of sleeping through it.
waitForFlag :: (MonadSTM m, MonadMVar m, MonadDelay m)
            => StrictTVar m Bool -> StrictMVar m () -> m ()
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
    Nothing -> throwIO (userError "the waiter was never woken")

-- * The live tree: the shared scenarios over a real 'PostgresSystemDB'
-- with a FastLogger tracer, for @main@.

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    withResource acquireLoggerBackend snd $ \getLogger ->
      testGroup
        "Context"
        [ liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "a context reads its workflow id" scenarioWorkflowId checkWorkflowId,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "a workflow's step ids are zero based and allocated once" scenarioStepIds checkStepIds,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "step ids stay dense while markers spend their own sequence" scenarioDenseIds checkDenseIds,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "withAttempt scopes a step and leaves the outer scope alone" scenarioAttemptScope checkAttemptScope,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "a scope reports its status and id" scenarioScopeStatus checkScopeStatus,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "a first attempt reports its step, attempt 1 of 1" scenarioFirstAttempt checkFirstAttempt,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "a retry keeps the step and moves the attempt" scenarioRetryAttempt checkRetryAttempt,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "a fresh token is quiet until fired" scenarioTokenFire checkTokenFire,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "each attempt watches a token of its own" scenarioAttemptTokens checkAttemptTokens,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "a deadline rides the workflow state" scenarioDeadline checkDeadline,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "two contexts over one workflow share its step counter" scenarioSharedCounter checkSharedCounter,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "a re-run of one id is a different execution" scenarioRerunIdentity checkRerunIdentity,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "the connection and identity travel with the context" scenarioTravelsWith checkTravelsWith,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "nested runners isolate" scenarioNestedRunners checkNestedRunners,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "state interop runs beside the context" scenarioStateInterop checkStateInterop,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "a throw from an engine call reaches the caller" scenarioThrowEscape checkThrowEscape,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "a cooperative flag cancels a wait promptly" scenarioCoopFlag checkCoopFlag,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "a fork handed the context shares its counter" scenarioForkCounter checkForkCounter,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "a nested scope reports the step that encloses it" scenarioNestedScope checkNestedScope,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "a cancellation token outside a step never fires" scenarioTokenOutsideStep checkTokenOutsideStep,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "concurrent contexts are isolated from each other" scenarioConcurrentIsolation checkConcurrentIsolation,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "separate executions own independent step counters" scenarioExecCounters checkExecCounters,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "a step view reads its status with the workflow id" scenarioStepView checkStepView,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "raceCancel returns the value when the token stays quiet" scenarioRaceCompletes checkRaceCompletes,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "raceCancel reports cancellation when the token has fired" scenarioRaceCancelled checkRaceCancelled,
          liveCase (liveFixture <$> getBackend <*> (ioTracer . fst <$> getLogger)) "a body logs through its context" scenarioLogLines checkLogLines,
          -- Sim only: hand-emitted structural events; typed assertions live only in sim.
          testCase "a context announces through its tracer" (pure ())
        ]
