{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Shared step-runner scenarios: one body per case, judged by one pure
-- check on each stack, over the shared 'StepFixture'. The live tree
-- ('DBOS.Transact.StepTest') runs them over Postgres with per-case UUIDs;
-- the sim tree ('DBOS.Transact.StepTestSim') over one 'MemSystemDB' per case
-- with deterministic names. Engine errors throw (via 'MonadThrow'), so both
-- trees assert on plain values; sim-only trace assertions stay in the sim
-- tree.
--
-- Bodies that capture per-case state (counters, observations) live as
-- top-level helpers taking that state explicitly: @MonoLocalBinds@ cannot
-- generalize a @let@-bound rank-2 body.
module DBOS.Transact.StepCases
  ( StepFixture (..),
    scenarioRecordReplay,
    scenarioNestedPlain,
    scenarioScopedView,
    scenarioNestedStepView,
    scenarioPendingScoped,
    scenarioDurableSleep,
    scenarioNestedEnclosing,
    scenarioTokenQuiet,
    checkRecordReplay,
    checkNestedPlain,
    checkScopedView,
    checkNestedStepView,
    checkPendingScoped,
    checkDurableSleep,
    checkNestedEnclosing,
    checkTokenQuiet,
  )
where

import DBOS.Prelude
import DBOS.SystemDB (WorkflowId (..))
import DBOS.Transact
  ( EngineOnly,
    Error (..),
    StepCtx,
    StepOptions (..),
    millisDuration,
    pendingStep,
    runNestedStep,
    runStep,
    runStepWith,
    sleepStep,
    stepOptionsDefault)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Checkpoint (PendingStep (..))
import DBOS.Transact.Checkpoint (pendingStepId)
import DBOS.Transact.Connection (Connection)
import DBOS.Transact.Context
  ( StepStatus,
    firstStepStatus,
    stepCtxStatus,
    stepId,
    stepStatus,
    stepStatusCurrentAttempt,
    stepStatusId,
    stepStatusMaxAttempts,
    withWorkflow,
  )

-- | How a tree instantiates its world: a fresh connection per run over a
-- shared backend, per-case workflow ids, the stack identity, row creation,
-- and checkpoint reads.
data StepFixture m = StepFixture
  { sfConnection :: m (Connection m),
    sfFreshId :: Text -> m WorkflowId,
    sfIdentity :: Identity,
    sfInitRow :: WorkflowId -> m (),
    sfCheckStep :: WorkflowId -> Int -> Text -> m Bool,
    sfListStepNames :: WorkflowId -> m [Text]
  }

-- | Abort on an engine-channel failure, naming it. Centralized so the call
-- sites never leave the error channel ambiguous.
orThrow :: forall m a. (MonadThrow m) => Either (Error EngineOnly) a -> m a
orThrow = either (throwIO . userError . show) pure

-- | A recorded workflow step runs once and replays: two executions over
-- fresh contexts, exactly as a recovered run does. Returns both results,
-- the body's run count, and the observed step id.
scenarioRecordReplay ::
  forall m.
  (MonadSTM m, MonadTime m, MonadCatch m) =>
  StepFixture m ->
  m (Int, Int, Int, Maybe Int)
scenarioRecordReplay fx = do
  wid <- fx.sfFreshId "record"
  calls <- newTVarIO (0 :: Int)
  observed <- newTVarIO Nothing
  fx.sfInitRow wid
  let runOnce = do
        conn <- fx.sfConnection
        withWorkflow conn fx.sfIdentity wid Nothing $ \wctx ->
          runStep wctx "test_step" (countingBody calls observed)
  first <- runOnce >>= orThrow
  second <- runOnce >>= orThrow
  count <- readTVarIO calls
  seen <- readTVarIO observed
  pure (first, second, count, seen)

-- | The counting body: records its step id, counts entries, answers 42.
countingBody ::
  forall exec m.
  (MonadSTM m) =>
  StrictTVar m Int ->
  StrictTVar m (Maybe Int) ->
  StepCtx exec m ->
  m Int
countingBody calls observed sctx = do
  atomically (writeTVar observed (stepId sctx))
  atomically (modifyTVar calls (+ 1))
  pure 42

-- | A step inside a step body runs plainly and takes no id. Returns the
-- outer result with whether each level checkpointed.
scenarioNestedPlain ::
  forall m.
  (MonadSTM m, MonadTime m, MonadCatch m) =>
  StepFixture m ->
  m (Int, Bool, Bool)
scenarioNestedPlain fx = do
  wid <- fx.sfFreshId "nested"
  fx.sfInitRow wid
  conn <- fx.sfConnection
  outer <-
    withWorkflow conn fx.sfIdentity wid Nothing $ \wctx ->
      runStep wctx "outer" nestingBody
  result <- orThrow outer
  placed <- fx.sfCheckStep wid 0 "outer"
  free <- fx.sfCheckStep wid 1 "inner"
  pure (result, placed, free)

-- | The nesting body: the inner call runs plainly inside the outer step.
nestingBody ::
  forall exec m.
  (MonadThrow m) =>
  StepCtx exec m ->
  m Int
nestingBody s = do
  inner <- runNestedStep s "inner" (\_ -> pure (7 :: Int))
  case inner of
    Right n -> pure (n + 1)
    Left err -> throwIO (userError (show (err :: Error EngineOnly)))

-- | A scoped step runs once and replays through the workflow view: the body
-- reads its narrowed view's status, proving the handoff as well as the
-- checkpoint. Returns the first result, the observed status, the replay,
-- and the run count.
scenarioScopedView ::
  forall m.
  (MonadSTM m, MonadTime m, MonadCatch m) =>
  StepFixture m ->
  m (Int, Maybe StepStatus, Int, Int)
scenarioScopedView fx = do
  wid <- fx.sfFreshId "scoped"
  calls <- newTVarIO (0 :: Int)
  observed <- newTVarIO Nothing
  fx.sfInitRow wid
  let runOnce = do
        conn <- fx.sfConnection
        withWorkflow conn fx.sfIdentity wid Nothing $ \wctx ->
          runStep wctx "scoped_step" (scopedBody calls observed)
  first <- runOnce >>= orThrow
  seen <- readTVarIO observed
  replay <- runOnce >>= orThrow
  count <- readTVarIO calls
  pure (first, seen, replay, count)

-- | The scoped body: records its narrowed status, counts entries, answers 42.
scopedBody ::
  forall exec m.
  (MonadSTM m) =>
  StrictTVar m Int ->
  StrictTVar m (Maybe StepStatus) ->
  StepCtx exec m ->
  m Int
scopedBody calls observed s = do
  atomically (writeTVar observed (stepCtxStatus s))
  atomically (modifyTVar calls (+ 1))
  pure 42

-- | A nested step through the step view is plain and takes no id: the same
-- shape as 'scenarioNestedPlain', kept as its own case because the trees
-- assert different seams around it (checkpoints live, traces sim).
scenarioNestedStepView ::
  forall m.
  (MonadSTM m, MonadTime m, MonadCatch m) =>
  StepFixture m ->
  m (Int, Bool, Bool)
scenarioNestedStepView = scenarioNestedPlain

-- | A pending scoped step claims its id at build and replays: the build
-- hands back step zero, the drive records under it, and a second run adopts
-- the recording. Returns the first outcome, the claimed id, the replay,
-- and the run count.
scenarioPendingScoped ::
  forall m.
  (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) =>
  StepFixture m ->
  m (Int, Maybe Int, Int, Int)
scenarioPendingScoped fx = do
  wid <- fx.sfFreshId "pending"
  calls <- newTVarIO (0 :: Int)
  fx.sfInitRow wid
  let runOnce = do
        conn <- fx.sfConnection
        withWorkflow conn fx.sfIdentity wid Nothing $ \wctx -> do
          pending <- pendingStep wctx "pending_step" (pendingBody calls)
          outcome <- pending.pendingRun
          result <- orThrow outcome
          pure (result, pendingStepId pending)
  (first, claimed) <- runOnce
  (replay, _) <- runOnce
  count <- readTVarIO calls
  pure (first, claimed, replay, count)

-- | The pending body: counts entries, answers 42.
pendingBody ::
  forall exec m.
  (MonadSTM m) =>
  StrictTVar m Int ->
  StepCtx exec m ->
  m (Either (Error EngineOnly) Int)
pendingBody calls _ = do
  atomically (modifyTVar calls (+ 1))
  pure (Right 42)

-- | Durable sleep reuses its recorded wake time: the replay adopts the
-- original wake instead of sleeping again. Returns both outcomes.
scenarioDurableSleep ::
  forall m.
  (MonadSTM m, MonadTime m, MonadDelay m, MonadThrow m) =>
  StepFixture m ->
  m ((), ())
scenarioDurableSleep fx = do
  wid <- fx.sfFreshId "sleep"
  fx.sfInitRow wid
  let runOnce = do
        conn <- fx.sfConnection
        withWorkflow conn fx.sfIdentity wid Nothing $ \wctx ->
          sleepStep wctx (millisDuration 25)
  first <- runOnce >>= orThrow
  replay <- runOnce >>= orThrow
  pure (first, replay)

-- | A nested step reports the step that encloses it: the outer body fails
-- once, and both attempts observe the inner call carrying the enclosing
-- status whole. Returns the leading result, the retried outcome, the
-- attempt count, the nested observations, and the recorded step names.
scenarioNestedEnclosing ::
  forall m.
  (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) =>
  StepFixture m ->
  m ((), (), Int, [(Maybe StepStatus, Maybe StepStatus, Maybe Int)], [Text])
scenarioNestedEnclosing fx = do
  wid <- fx.sfFreshId "nested-status"
  seen <- newTVarIO ([] :: [(Maybe StepStatus, Maybe StepStatus, Maybe Int)])
  attempts <- newTVarIO (0 :: Int)
  fx.sfInitRow wid
  conn <- fx.sfConnection
  (first, outcome) <-
    withWorkflow conn fx.sfIdentity wid Nothing $ \wctx -> do
      first <- runStep wctx "first" (\_ -> pure ())
      outcome <- runStepWith (stepOptionsDefault {maxAttempts = 2, interval = millisDuration 1}) wctx "outer" (enclosingBody seen attempts)
      pure (first, outcome)
  firstResult <- orThrow first
  outcomeResult <- orThrow outcome
  count <- readTVarIO attempts
  observed <- readTVarIO seen
  names <- fx.sfListStepNames wid
  pure (firstResult, outcomeResult, count, observed, names)

-- | The enclosing body: observes the inner call's status against the outer
-- status, fails the first attempt, and succeeds the retry.
enclosingBody ::
  forall exec m.
  (MonadSTM m) =>
  StrictTVar m [(Maybe StepStatus, Maybe StepStatus, Maybe Int)] ->
  StrictTVar m Int ->
  StepCtx exec m ->
  m (Either (Error EngineOnly) ())
enclosingBody seen attempts sctx = do
  let outer = stepStatus sctx
  inner <- runNestedStep sctx "inner" (innerObserve seen outer)
  case inner of
    Left err -> pure (Left err)
    Right () -> do
      attempt <- readTVarIO attempts
      atomically (modifyTVar attempts (+ 1))
      if attempt == 0
        then pure (Left (StepFailed "outer" "boom"))
        else pure (Right ())

-- | The inner observation: records the enclosing status beside its own.
innerObserve ::
  forall exec m.
  (MonadSTM m) =>
  StrictTVar m [(Maybe StepStatus, Maybe StepStatus, Maybe Int)] ->
  Maybe StepStatus ->
  StepCtx exec m ->
  m ()
innerObserve seen outer innerSctx = do
  let innerStatus = stepStatus innerSctx
      innerId = stepId innerSctx
  atomically (modifyTVar seen (++ [(outer, innerStatus, innerId)]))

-- | A cancellation token outside a step never fires on its own: the
-- abandoned step blows its deadline, which fires the workflow scope's token.
-- Returns whether the abandonment timed out.
scenarioTokenQuiet ::
  forall m.
  (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) =>
  StepFixture m ->
  m Bool
scenarioTokenQuiet fx = do
  wid <- fx.sfFreshId "quiet-token"
  fx.sfInitRow wid
  conn <- fx.sfConnection
  abandoned <-
    withWorkflow conn fx.sfIdentity wid Nothing $ \wctx -> do
      let hanging :: forall exec. StepCtx exec m -> m (Either (Error EngineOnly) ())
          hanging _ = threadDelay 1000000 >> pure (Right ())
          options = (stepOptionsDefault :: StepOptions EngineOnly) {timeout = Just (millisDuration 20)}
      runStepWith options wctx "times-out" hanging
  pure $ case abandoned of
    Left StepTimeout {} -> True
    _ -> False

-- | The first execution returns the body's result, the replay adopts it, the
-- body ran once inside step zero.
checkRecordReplay :: (Int, Int, Int, Maybe Int) -> Either String ()
checkRecordReplay = checkEq (42, 42, 1, Just 0)

-- | The outer body sees the inner result, the outer step checkpointed, and
-- the inner call took no id.
checkNestedPlain :: (Int, Bool, Bool) -> Either String ()
checkNestedPlain = checkEq (8, True, False)

-- | The scoped runner returns the body's result inside step zero, and the
-- replay adopts it without re-running.
checkScopedView :: (Int, Maybe StepStatus, Int, Int) -> Either String ()
checkScopedView = checkEq (42, Just (firstStepStatus 0), 42, 1)

-- | Same shape as 'checkNestedPlain': the step view agrees with the workflow
-- view.
checkNestedStepView :: (Int, Bool, Bool) -> Either String ()
checkNestedStepView = checkNestedPlain

-- | The pending step returns the body's result with its id claimed at build,
-- and the replay adopts both without re-running.
checkPendingScoped :: (Int, Maybe Int, Int, Int) -> Either String ()
checkPendingScoped = checkEq (42, Just 0, 42, 1)

-- | Both sleeps succeed; the replay adopts the original wake time.
checkDurableSleep :: ((), ()) -> Either String ()
checkDurableSleep = checkEq ((), ())

-- | The leading step runs, the outer step succeeds on its retry after two
-- bodies, each attempt observes the enclosing status whole, and only the
-- durable steps checkpoint.
checkNestedEnclosing :: ((), (), Int, [(Maybe StepStatus, Maybe StepStatus, Maybe Int)], [Text]) -> Either String ()
checkNestedEnclosing ((), (), count, observed, names) = do
  checkEq 2 count
  checkEq 2 (length observed)
  checkEq ["first", "outer"] names
  case observed of
    [(outer1, inner1, id1), (outer2, inner2, id2)] -> do
      checkEq (Just 1) id1
      checkEq (Just 1) id2
      checkEq outer1 inner1
      checkEq outer2 inner2
      case (outer1, outer2) of
        (Just firstStatus, Just secondStatus) -> do
          checkEq 1 (stepStatusId firstStatus)
          checkEq 1 (stepStatusId secondStatus)
          checkEq 1 (stepStatusCurrentAttempt firstStatus)
          checkEq 2 (stepStatusCurrentAttempt secondStatus)
          checkEq 2 (stepStatusMaxAttempts firstStatus)
          checkEq 2 (stepStatusMaxAttempts secondStatus)
        _ -> Left ("expected enclosing statuses, got: " <> show observed)
    _ -> Left ("expected two nested observations, got: " <> show observed)

-- | The step blows its deadline.
checkTokenQuiet :: Bool -> Either String ()
checkTokenQuiet = checkEq True

-- | Pure verdicts; both trees judge through these.
checkEq :: (Eq a, Show a) => a -> a -> Either String ()
checkEq expected actual
  | expected == actual = Right ()
  | otherwise = Left ("expected " <> show expected <> ", got " <> show actual)
