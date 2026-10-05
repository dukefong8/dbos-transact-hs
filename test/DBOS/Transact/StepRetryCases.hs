{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Shared step-retry scenarios: one body per case, judged by one pure check
-- on each stack. Bodies count attempts through STM counters (so the same
-- body runs live and under IOSim), waits are virtual under IOSim, and the
-- fixture carries the workflow scope, cancellation, and step listing. The
-- live tree ('DBOS.Transact.StepRetryTest') runs them over Postgres rows,
-- the sim tree ('DBOS.Transact.StepRetryTestSim') over the in-memory
-- backend, and both prove the same retry record.
module DBOS.Transact.StepRetryCases
  ( StepRetryFixture (..),
    scenarioRetryThird,
    scenarioRetryExhausted,
    scenarioRetryDefault,
    scenarioRetryReplay,
    scenarioRetryDeclined,
    scenarioRetryMidDecline,
    scenarioStepTimeout,
    scenarioStepWithinTimeout,
    scenarioTimeoutStopsBody,
    scenarioTimeoutFreshRetry,
    scenarioTimeoutAllTimeout,
    scenarioPlainNotPreemptible,
    scenarioTokenQuiet,
    scenarioTokenFirst,
    scenarioTokenDrop,
    scenarioPreemptible,
    checkRetryThird,
    checkRetryExhausted,
    checkRetryDefault,
    checkRetryReplay,
    checkRetryDeclined,
    checkRetryMidDecline,
    checkStepTimeout,
    checkStepWithinTimeout,
    checkTimeoutStopsBody,
    checkTimeoutFreshRetry,
    checkTimeoutAllTimeout,
    checkPlainNotPreemptible,
    checkTokenQuiet,
    checkTokenFirst,
    checkTokenDrop,
    checkPreemptible,
  )
where

import DBOS.Prelude
import DBOS.SystemDB (StepRecord (..), WorkflowId, millisDuration)
import DBOS.SystemDB qualified as SystemDB
import DBOS.Transact
  ( EngineOnly,
    Error (..),
    StepCtx,
    StepOptions (..),
    WorkflowCtx,
    runStepWith,
    stepCtxCancellationToken,
    stepOptionsDefault,
  )
import DBOS.Transact.Context (tokenCancelled)

-- | What a stack must provide: fresh workflow ids over initialized rows,
-- a workflow scope to run steps through, cancellation by workflow id, and
-- the step listing (to prove a preempted step recorded nothing).
data StepRetryFixture m = StepRetryFixture
  { srfFreshWorkflowId :: m WorkflowId,
    srfRun :: forall a. WorkflowId -> (forall exec. WorkflowCtx exec m -> m a) -> m a,
    srfCancel :: [WorkflowId] -> m (),
    srfListSteps :: WorkflowId -> m [StepRecord]
  }

-- | Run a step under a fresh scope: the rank-2 field is read by pattern
-- match because record-dot has no 'HasField' instance for polymorphic
-- fields.
runStepScope :: StepRetryFixture m -> WorkflowId -> (forall exec. WorkflowCtx exec m -> m a) -> m a
runStepScope (StepRetryFixture _ run _ _) = run

-- | Mirrors Rust @tests/retries.rs@: a step that fails twice succeeds on
-- the third attempt.
scenarioRetryThird :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadAsync m, MonadFork m, MonadMVar m) => StepRetryFixture m -> m (Either (Error EngineOnly) Int, Int, [StepRecord])
scenarioRetryThird fx = do
  wid <- fx.srfFreshWorkflowId
  attempts <- newTVarIO (0 :: Int)
  let body :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) Int)
      body _ = do
        attempt <- readTVarIO attempts
        atomically (modifyTVar attempts (+ 1))
        if attempt < 2
          then pure (Left (StepFailed "flaky" "boom"))
          else pure (Right (42 :: Int))
      options = stepOptionsDefault {maxAttempts = 3, interval = millisDuration 1}
  outcome <- runStepScope fx wid $ \wctx -> runStepWith options wctx "flaky" body
  made <- readTVarIO attempts
  steps <- fx.srfListSteps wid
  pure (outcome, made, steps)

-- | Exhausted retries carry every attempt's failure.
scenarioRetryExhausted :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadAsync m, MonadFork m, MonadMVar m) => StepRetryFixture m -> m (Either (Error EngineOnly) Int, Int)
scenarioRetryExhausted fx = do
  wid <- fx.srfFreshWorkflowId
  attempts <- newTVarIO (0 :: Int)
  let body :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) Int)
      body _ = do
        atomically (modifyTVar attempts (+ 1))
        pure (Left (StepFailed "doomed" "boom"))
      options = stepOptionsDefault {maxAttempts = 2, interval = millisDuration 1}
  outcome <- runStepScope fx wid $ \wctx -> runStepWith options wctx "doomed" body
  made <- readTVarIO attempts
  pure (outcome, made)

-- | The default does not retry and does not wrap.
scenarioRetryDefault :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadAsync m, MonadFork m, MonadMVar m) => StepRetryFixture m -> m (Either (Error EngineOnly) Int, Int)
scenarioRetryDefault fx = do
  wid <- fx.srfFreshWorkflowId
  attempts <- newTVarIO (0 :: Int)
  let body :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) Int)
      body _ = do
        atomically (modifyTVar attempts (+ 1))
        pure (Left (StepFailed "plain" "boom"))
  outcome <- runStepScope fx wid $ \wctx -> runStepWith stepOptionsDefault wctx "plain" body
  made <- readTVarIO attempts
  pure (outcome, made)

-- | A retried step replays from its single checkpoint: the second run
-- reads the recording without running the body again.
scenarioRetryReplay :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadAsync m, MonadFork m, MonadMVar m) => StepRetryFixture m -> m (Either (Error EngineOnly) Int, Either (Error EngineOnly) Int, Int)
scenarioRetryReplay fx = do
  wid <- fx.srfFreshWorkflowId
  attempts <- newTVarIO (0 :: Int)
  let body :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) Int)
      body _ = do
        attempt <- readTVarIO attempts
        atomically (modifyTVar attempts (+ 1))
        if attempt < 1
          then pure (Left (StepFailed "flaky" "boom"))
          else pure (Right (7 :: Int))
      options = stepOptionsDefault {maxAttempts = 3, interval = millisDuration 1}
  first <- runStepScope fx wid $ \wctx -> runStepWith options wctx "flaky" body
  second <- runStepScope fx wid $ \wctx -> runStepWith options wctx "flaky" body
  made <- readTVarIO attempts
  pure (first, second, made)

-- | A declined failure stops retrying immediately.
scenarioRetryDeclined :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadAsync m, MonadFork m, MonadMVar m) => StepRetryFixture m -> m (Either (Error EngineOnly) Int, Int)
scenarioRetryDeclined fx = do
  wid <- fx.srfFreshWorkflowId
  attempts <- newTVarIO (0 :: Int)
  let body :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) Int)
      body _ = do
        atomically (modifyTVar attempts (+ 1))
        pure (Left (StepFailed "declined" "boom"))
      options =
        stepOptionsDefault
          { maxAttempts = 3,
            interval = millisDuration 1,
            shouldRetry = Just (const False)
          }
  outcome <- runStepScope fx wid $ \wctx -> runStepWith options wctx "declined" body
  made <- readTVarIO attempts
  pure (outcome, made)

-- | Declining mid-policy keeps the earlier failures.
scenarioRetryMidDecline :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadAsync m, MonadFork m, MonadMVar m) => StepRetryFixture m -> m (Either (Error EngineOnly) Int, Int)
scenarioRetryMidDecline fx = do
  wid <- fx.srfFreshWorkflowId
  attempts <- newTVarIO (0 :: Int)
  let body :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) Int)
      body _ = do
        attempt <- readTVarIO attempts
        atomically (modifyTVar attempts (+ 1))
        pure (Left (StepFailed "pick" (if attempt == 0 then "first" else "second")))
      declinesSecond err = case err of
        StepFailed _ message -> message /= "second"
        _ -> True
      options =
        stepOptionsDefault
          { maxAttempts = 3,
            interval = millisDuration 1,
            shouldRetry = Just declinesSecond
          }
  outcome <- runStepScope fx wid $ \wctx -> runStepWith options wctx "pick" body
  made <- readTVarIO attempts
  pure (outcome, made)

-- | Mirrors the timeout cases of Rust @tests/timeouts.rs@: a step that
-- hangs is stopped at its timeout.
scenarioStepTimeout :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadAsync m, MonadFork m, MonadMVar m) => StepRetryFixture m -> m (Either (Error EngineOnly) Int)
scenarioStepTimeout fx = do
  wid <- fx.srfFreshWorkflowId
  let options = (stepOptionsDefault :: StepOptions EngineOnly) {timeout = Just (millisDuration 5)}
  let body :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) Int)
      body _ = threadDelay 50000 >> pure (Right (1 :: Int))
  runStepScope fx wid $ \wctx -> runStepWith options wctx "slow" body

-- | A step within its timeout is unaffected.
scenarioStepWithinTimeout :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadAsync m, MonadFork m, MonadMVar m) => StepRetryFixture m -> m (Either (Error EngineOnly) Int)
scenarioStepWithinTimeout fx = do
  wid <- fx.srfFreshWorkflowId
  let options = (stepOptionsDefault :: StepOptions EngineOnly) {timeout = Just (millisDuration 500)}
  let body :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) Int)
      body _ = pure (Right (9 :: Int))
  runStepScope fx wid $ \wctx -> runStepWith options wctx "quick" body

-- | A timed-out body stops rather than continuing: after well past its
-- hang, the body never ran to completion.
scenarioTimeoutStopsBody :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadAsync m, MonadFork m, MonadMVar m) => StepRetryFixture m -> m (Either (Error EngineOnly) Int, Bool)
scenarioTimeoutStopsBody fx = do
  wid <- fx.srfFreshWorkflowId
  ran <- newTVarIO False
  let body :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) Int)
      body _ = threadDelay 100000 >> atomically (writeTVar ran True) >> pure (Right (1 :: Int))
      options = (stepOptionsDefault :: StepOptions EngineOnly) {timeout = Just (millisDuration 5)}
  outcome <- runStepScope fx wid $ \wctx -> runStepWith options wctx "slow" body
  threadDelay 150000
  finished <- readTVarIO ran
  pure (outcome, finished)

-- | A timed-out attempt is retried with a fresh timeout: two hangs, then
-- the immediate success.
scenarioTimeoutFreshRetry :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadAsync m, MonadFork m, MonadMVar m) => StepRetryFixture m -> m (Either (Error EngineOnly) Int, Int)
scenarioTimeoutFreshRetry fx = do
  wid <- fx.srfFreshWorkflowId
  attempts <- newTVarIO (0 :: Int)
  let body :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) Int)
      body _ = do
        attempt <- readTVarIO attempts
        atomically (modifyTVar attempts (+ 1))
        if attempt < 2
          then threadDelay 100000 >> pure (Right (1 :: Int))
          else pure (Right (42 :: Int))
      options = stepOptionsDefault {maxAttempts = 3, interval = millisDuration 1, timeout = Just (millisDuration 20)}
  outcome <- runStepScope fx wid $ \wctx -> runStepWith options wctx "flaky" body
  made <- readTVarIO attempts
  pure (outcome, made)

-- | Every attempt timing out reports each timeout.
scenarioTimeoutAllTimeout :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadAsync m, MonadFork m, MonadMVar m) => StepRetryFixture m -> m (Either (Error EngineOnly) Int)
scenarioTimeoutAllTimeout fx = do
  wid <- fx.srfFreshWorkflowId
  let body :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) Int)
      body _ = threadDelay 100000 >> pure (Right (1 :: Int))
      options = stepOptionsDefault {maxAttempts = 2, interval = millisDuration 1, timeout = Just (millisDuration 10)}
  runStepScope fx wid $ \wctx -> runStepWith options wctx "slow" body

-- | A plain step is not preemptible: cancelling its workflow does not stop
-- it, and it still returns its value once its gate opens.
scenarioPlainNotPreemptible :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadAsync m, MonadFork m, MonadMVar m) => StepRetryFixture m -> m (Either (Error EngineOnly) Int)
scenarioPlainNotPreemptible fx = do
  wid <- fx.srfFreshWorkflowId
  gate <- newEmptyMVar
  let body :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) Int)
      body _ = takeMVar gate >> pure (Right (7 :: Int))
  worker <- async (runStepScope fx wid $ \wctx -> runStepWith stepOptionsDefault wctx "plain" body)
  threadDelay 200000
  fx.srfCancel [wid]
  putMVar gate ()
  wait worker

-- | A completed step leaves its token alone.
scenarioTokenQuiet :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadAsync m, MonadFork m, MonadMVar m) => StepRetryFixture m -> m (Either (Error EngineOnly) Int, Bool)
scenarioTokenQuiet fx = do
  wid <- fx.srfFreshWorkflowId
  seen <- newTVarIO True
  let body :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) Int)
      body ctx = do
        token <- stepCtxCancellationToken ctx
        fired <- tokenCancelled token
        atomically (writeTVar seen fired)
        pure (Right (1 :: Int))
  outcome <- runStepScope fx wid $ \wctx -> runStepWith stepOptionsDefault wctx "quiet" body
  quiet <- readTVarIO seen
  pure (outcome, quiet)

-- | The cancellation token fires before the body is dropped: the timed-out
-- body observes the fired token.
scenarioTokenFirst :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadAsync m, MonadFork m, MonadMVar m) => StepRetryFixture m -> m (Either (Error EngineOnly) Int, Bool)
scenarioTokenFirst fx = do
  wid <- fx.srfFreshWorkflowId
  gate <- newEmptyMVar
  probe <- newTVarIO (pure False)
  let body :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) Int)
      body ctx = do
        token <- stepCtxCancellationToken ctx
        atomically (writeTVar probe (tokenCancelled token))
        takeMVar gate >> pure (Right (1 :: Int))
      options = (stepOptionsDefault :: StepOptions EngineOnly) {timeout = Just (millisDuration 50)}
  outcome <- runStepScope fx wid $ \wctx -> runStepWith options wctx "slow" body
  probeAction <- readTVarIO probe
  fired <- probeAction
  pure (outcome, fired)

-- | A dropped step fires its cancellation token: killing the worker does
-- not run the body, but the token fires first.
scenarioTokenDrop :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadAsync m, MonadFork m, MonadMVar m) => StepRetryFixture m -> m Bool
scenarioTokenDrop fx = do
  wid <- fx.srfFreshWorkflowId
  gate <- newEmptyMVar
  started <- newEmptyMVar
  probe <- newTVarIO (pure False)
  let body :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) Int)
      body ctx = do
        token <- stepCtxCancellationToken ctx
        atomically (writeTVar probe (tokenCancelled token))
        putMVar started ()
        takeMVar gate >> pure (Right (1 :: Int))
  worker <- async (runStepScope fx wid $ \wctx -> runStepWith stepOptionsDefault wctx "dropped" body)
  takeMVar started
  cancel worker
  probeAction <- readTVarIO probe
  probeAction

-- | A preemptible step stops and records no outcome: cancelling its
-- workflow ends the run with 'WorkflowCancelled' and the step table stays
-- empty, so a resume runs it again.
scenarioPreemptible :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadAsync m, MonadFork m, MonadMVar m) => StepRetryFixture m -> m (Either (Error EngineOnly) Int, [StepRecord])
scenarioPreemptible fx = do
  wid <- fx.srfFreshWorkflowId
  gate <- newEmptyMVar
  started <- newEmptyMVar
  let body :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) Int)
      body _ = putMVar started () >> takeMVar gate >> pure (Right (7 :: Int))
      options = stepOptionsDefault {preemptible = True, maxAttempts = 3, interval = millisDuration 1, timeout = Just (millisDuration 50)}
  worker <- async (runStepScope fx wid $ \wctx -> runStepWith options wctx "preemptible" body)
  takeMVar started
  fx.srfCancel [wid]
  outcome <- wait worker
  steps <- fx.srfListSteps wid
  pure (outcome, steps)

-- * Checks

-- | The third attempt succeeds after two failures, and the attempts
-- share one step id.
checkRetryThird :: (Either (Error EngineOnly) Int, Int, [StepRecord]) -> Either String ()
checkRetryThird (outcome, made, steps) = do
  unless (outcome == Right 42) $ Left ("expected 42 on the third attempt, got: " <> show outcome)
  unless (made == 3) $ Left ("expected three attempts, got: " <> show made)
  unless (length steps == 1) $ Left ("expected the attempts to share one step id, got: " <> show (length steps))

-- | Exhaustion names the step, counts both attempts, and keeps both
-- failures.
checkRetryExhausted :: (Either (Error EngineOnly) Int, Int) -> Either String ()
checkRetryExhausted (outcome, made) = do
  case outcome of
    Left MaxStepRetriesExceeded {step, attempts, errors} -> do
      unless (step == "doomed") $ Left ("expected the doomed step, got: " <> show step)
      unless (attempts == 2) $ Left ("expected two attempts, got: " <> show attempts)
      unless (length errors == 2) $ Left ("expected two failures, got: " <> show (length errors))
    other -> Left ("expected MaxStepRetriesExceeded, got: " <> show other)
  unless (made == 2) $ Left ("expected the body to run twice, got: " <> show made)

-- | The default runs once and returns the failure unwrapped.
checkRetryDefault :: (Either (Error EngineOnly) Int, Int) -> Either String ()
checkRetryDefault (outcome, made) = do
  unless (outcome == Left (StepFailed "plain" "boom")) $ Left ("expected the bare failure, got: " <> show outcome)
  unless (made == 1) $ Left ("expected one attempt, got: " <> show made)

-- | The replay returns the recording without running the body again.
checkRetryReplay :: (Either (Error EngineOnly) Int, Either (Error EngineOnly) Int, Int) -> Either String ()
checkRetryReplay (first, second, made) = do
  unless (first == Right 7) $ Left ("expected 7 from the first run, got: " <> show first)
  unless (second == Right 7) $ Left ("expected 7 from the replay, got: " <> show second)
  unless (made == 2) $ Left ("expected two body runs, got: " <> show made)

-- | The declined failure returns immediately after one attempt.
checkRetryDeclined :: (Either (Error EngineOnly) Int, Int) -> Either String ()
checkRetryDeclined (outcome, made) = do
  unless (outcome == Left (StepFailed "declined" "boom")) $ Left ("expected the declined failure, got: " <> show outcome)
  unless (made == 1) $ Left ("expected one attempt, got: " <> show made)

-- | The mid-policy decline keeps both failures and stops at two attempts.
checkRetryMidDecline :: (Either (Error EngineOnly) Int, Int) -> Either String ()
checkRetryMidDecline (outcome, made) = do
  case outcome of
    Left MaxStepRetriesExceeded {attempts, errors} -> do
      unless (attempts == 2) $ Left ("expected two attempts, got: " <> show attempts)
      unless (length errors == 2) $ Left ("expected two failures, got: " <> show (length errors))
      unless (any (== StepFailed "pick" "first") errors) $ Left "expected the first failure to be kept"
      unless (any (== StepFailed "pick" "second") errors) $ Left "expected the declining failure to be kept"
    other -> Left ("expected MaxStepRetriesExceeded, got: " <> show other)
  unless (made == 2) $ Left ("expected the body to run twice, got: " <> show made)

-- | The hanging step is stopped at its timeout, naming the step.
checkStepTimeout :: Either (Error EngineOnly) Int -> Either String ()
checkStepTimeout outcome = case outcome of
  Left StepTimeout {step} ->
    unless (step == "slow") $ Left ("expected the slow step to time out, got: " <> show step)
  other -> Left ("expected StepTimeout, got: " <> show other)

-- | A step within its timeout is unaffected.
checkStepWithinTimeout :: Either (Error EngineOnly) Int -> Either String ()
checkStepWithinTimeout outcome =
  unless (outcome == Right 9) $ Left ("expected 9 within its timeout, got: " <> show outcome)

-- | The timed-out body never ran to completion.
checkTimeoutStopsBody :: (Either (Error EngineOnly) Int, Bool) -> Either String ()
checkTimeoutStopsBody (outcome, finished) = do
  case outcome of
    Left StepTimeout {} -> pure ()
    other -> Left ("expected StepTimeout, got: " <> show other)
  unless (not finished) $ Left "expected the timed-out body never to finish"

-- | Two timeouts, then the fresh success on the third attempt.
checkTimeoutFreshRetry :: (Either (Error EngineOnly) Int, Int) -> Either String ()
checkTimeoutFreshRetry (outcome, made) = do
  unless (outcome == Right 42) $ Left ("expected 42 after the timeouts, got: " <> show outcome)
  unless (made == 3) $ Left ("expected three attempts, got: " <> show made)

-- | Every attempt timing out reports each timeout.
checkTimeoutAllTimeout :: Either (Error EngineOnly) Int -> Either String ()
checkTimeoutAllTimeout outcome = case outcome of
  Left MaxStepRetriesExceeded {attempts, errors} -> do
    unless (attempts == 2) $ Left ("expected two attempts, got: " <> show attempts)
    unless (length errors == 2) $ Left ("expected two failures, got: " <> show (length errors))
    unless (all isTimeout errors) $ Left "expected every error to be a timeout"
  other -> Left ("expected MaxStepRetriesExceeded, got: " <> show other)
  where
    isTimeout StepTimeout {} = True
    isTimeout _ = False

-- | The plain step survives cancellation and returns its value.
checkPlainNotPreemptible :: Either (Error EngineOnly) Int -> Either String ()
checkPlainNotPreemptible outcome =
  unless (outcome == Right 7) $ Left ("expected the plain step to complete, got: " <> show outcome)

-- | The completed step never saw its token fire.
checkTokenQuiet :: (Either (Error EngineOnly) Int, Bool) -> Either String ()
checkTokenQuiet (outcome, seen) = do
  unless (outcome == Right 1) $ Left ("expected the quiet step to succeed, got: " <> show outcome)
  unless (not seen) $ Left "expected the completed step to leave its token alone"

-- | The timed-out body observed the fired token.
checkTokenFirst :: (Either (Error EngineOnly) Int, Bool) -> Either String ()
checkTokenFirst (outcome, fired) = do
  case outcome of
    Left StepTimeout {} -> pure ()
    other -> Left ("expected StepTimeout, got: " <> show other)
  unless fired $ Left "expected the token to fire before the body was dropped"

-- | The dropped step's token fired.
checkTokenDrop :: Bool -> Either String ()
checkTokenDrop fired =
  unless fired $ Left "expected the dropped step to fire its cancellation token"

-- | The preempted run ends cancelled with no recorded outcome.
checkPreemptible :: (Either (Error EngineOnly) Int, [StepRecord]) -> Either String ()
checkPreemptible (outcome, steps) = do
  case outcome of
    Left (ErrorSystemDatabase SystemDB.WorkflowCancelled {}) -> pure ()
    other -> Left ("expected WorkflowCancelled, got: " <> show other)
  unless (null steps) $ Left ("expected no recorded outcome, got: " <> show steps)
