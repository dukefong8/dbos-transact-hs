{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Shared workflow-deadline scenarios: one body per case, judged by one
-- pure check on each stack. Deadlines are virtual under IOSim, so the
-- hang, the 100ms cancellation, and the recovery all run deterministically
-- on both stacks. The fixture carries instance setup over a fresh
-- identity, relaunch, and row reads; scenarios register bodies, run them
-- with explicit budgets, and shut down exactly where the live cases do.
-- The live tree ('DBOS.Transact.DeadlinesTest') runs them over Postgres
-- rows with real launches, the sim tree ('DBOS.Transact.DeadlinesTestSim')
-- over the in-memory backend with the shared launch tail (which recovers
-- like the supervisor does).
module DBOS.Transact.DeadlinesCases
  ( DeadlinesFixture (..),
    waitForDeadline,
    scenarioWithinDeadline,
    scenarioPastDeadline,
    scenarioKeptDeadline,
    scenarioShutdownPending,
    scenarioBeatenDeadline,
    scenarioSweepCancelsAbandoned,
    checkWithinDeadline,
    checkPastDeadline,
    checkKeptDeadline,
    checkShutdownPending,
    checkBeatenDeadline,
    checkSweepCancelsAbandoned,
    scenarioSkewHoldsBudget,
    checkSkewHoldsBudget,
    scenarioDelayedReleaseAhead,
    checkDelayedReleaseAhead,
    scenarioDelayedHoldBehind,
    checkDelayedHoldBehind,
    scenarioFinishedIdAdopts,
    checkFinishedIdAdopts,
  )
where

import DBOS.Internal.Prelude
import DBOS.SystemDB
  ( Duration,
    Timestamp,
    WorkflowId (..),
    WorkflowStatus (..),
    millisDuration,
    secondsDuration,
    timestampFromEpochMs
  )
import DBOS.Internal.Transact.Serialization (decodeWorkflowValue, CodecError (..))
import DBOS.Transact
  (
    DBOS,
    EngineOnly,
    Error (..),
    Executor,
    RunOptions (..),
    SerializedWorkflowValue (..),
    Timeout (..),
    WorkflowCtx,
    newQualifiedWorkflowName,
    registerWorkflowRef,
    runStep,
    runWorkflowRef,
    runOptionsDefault,
    shutdown,
  )

-- | What a stack must provide: a fresh instance, executor, and workflow
-- id per scenario, relaunch over the same instance, and row reads for the
-- status and the stamped deadline.
data DeadlinesFixture m = DeadlinesFixture
  { dfSetup :: m (DBOS m, WorkflowId),
    dfLaunch :: DBOS m -> m (Executor m),
    dfReadStatus :: WorkflowId -> m (Maybe WorkflowStatus),
    dfReadDeadline :: WorkflowId -> m (Maybe Timestamp),
    dfPlantExpired :: WorkflowId -> m (),
    dfPlantDelayed :: WorkflowId -> Duration -> m (),
    dfSweepDelayed :: m ()
  }

-- | The deadline stamped on a workflow row, waiting until the row carries
-- one: the row is written before the body starts, so a bounded poll
-- always terminates.
waitForDeadline :: (MonadSTM m, MonadDelay m) => DeadlinesFixture m -> WorkflowId -> m Timestamp
waitForDeadline fx wid = go (50 :: Int)
  where
    go 0 = error "the workflow row never carried a deadline"
    go n = do
      deadline <- fx.dfReadDeadline wid
      case deadline of
        Just due -> pure due
        Nothing -> threadDelay 200000 >> go (n - 1)

-- | A workflow within its deadline is unaffected: the run records its
-- result and the row reads SUCCESS.
scenarioWithinDeadline :: forall m. (MonadAsync m, MonadMVar m, MonadFork m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m, MonadMask m)
                       => DeadlinesFixture m -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowStatus)
scenarioWithinDeadline fx = do
  (dbos, wid) <- fx.dfSetup
  let body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = pure (Right 7)
  ref <- registerWorkflowRef dbos (newQualifiedWorkflowName "quick") body >>= either (error . show) pure
  exec <- fx.dfLaunch dbos
  ran <- runWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just wid, runTimeout = Explicit (secondsDuration 30)}) Nothing
  status <- fx.dfReadStatus wid
  shutdown dbos
  pure (ran, status)

-- | A workflow past its deadline is cancelled: the run reports the
-- cancellation naming the workflow and the row reads CANCELLED.
scenarioPastDeadline :: forall m. (MonadAsync m, MonadMVar m, MonadFork m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m, MonadMask m)
                     => DeadlinesFixture m -> m (WorkflowId, Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowStatus)
scenarioPastDeadline fx = do
  (dbos, wid) <- fx.dfSetup
  let body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = do
        threadDelay 30000000
        pure (Right 1)
  ref <- registerWorkflowRef dbos (newQualifiedWorkflowName "runs-forever") body >>= either (error . show) pure
  exec <- fx.dfLaunch dbos
  ran <- runWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just wid, runTimeout = Explicit (millisDuration 100)}) Nothing
  status <- fx.dfReadStatus wid
  shutdown dbos
  pure (wid, ran, status)

-- | A recovered workflow keeps the deadline it already had: the deadline
-- stamped before the crash is the deadline after the relaunch, and the
-- resumed run records its result.
scenarioKeptDeadline :: forall m. (MonadMVar m, MonadFork m, MonadAsync m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m, MonadMask m)
                     => DeadlinesFixture m -> m (Timestamp, Timestamp, Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
scenarioKeptDeadline fx = do
  (dbos, wid) <- fx.dfSetup
  gate <- newEmptyMVar
  let body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = takeMVar gate >> pure (Right 7)
  ref <- registerWorkflowRef dbos (newQualifiedWorkflowName "gated") body >>= either (error . show) pure
  exec <- fx.dfLaunch dbos
  worker <- async (runWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just wid, runTimeout = Explicit (secondsDuration 30)}) Nothing)
  first <- waitForDeadline fx wid
  shutdown dbos
  cancel worker
  exec2 <- fx.dfLaunch dbos
  second <- waitForDeadline fx wid
  putMVar gate ()
  ran <- runWorkflowRef exec2 ref (runOptionsDefault {runWorkflowId = Just wid, runTimeout = Explicit (secondsDuration 30)}) Nothing
  shutdown dbos
  pure (first, second, ran)

-- | Shutdown does not durably cancel a workflow that has a deadline: the
-- row stays pending.
scenarioShutdownPending :: forall m. (MonadMVar m, MonadFork m, MonadAsync m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m, MonadMask m)
                        => DeadlinesFixture m -> m (Maybe WorkflowStatus)
scenarioShutdownPending fx = do
  (dbos, wid) <- fx.dfSetup
  gate <- newEmptyMVar
  let body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = takeMVar gate >> pure (Right 7)
  ref <- registerWorkflowRef dbos (newQualifiedWorkflowName "gated") body >>= either (error . show) pure
  exec <- fx.dfLaunch dbos
  worker <- async (runWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just wid, runTimeout = Explicit (secondsDuration 30)}) Nothing)
  _ <- waitForDeadline fx wid
  shutdown dbos
  cancel worker
  fx.dfReadStatus wid

-- | A deadline that loses to a recorded outcome reports that outcome: the
-- completed run wins, and a 1ms budget against the recording still reads
-- the result.
scenarioBeatenDeadline :: forall m. (MonadAsync m, MonadMVar m, MonadFork m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m, MonadMask m)
                       => DeadlinesFixture m -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowStatus)
scenarioBeatenDeadline fx = do
  (dbos, wid) <- fx.dfSetup
  let body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = pure (Right 7)
  ref <- registerWorkflowRef dbos (newQualifiedWorkflowName "quick") body >>= either (error . show) pure
  exec <- fx.dfLaunch dbos
  first <- runWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just wid, runTimeout = Explicit (secondsDuration 30)}) Nothing
  second <- runWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just wid, runTimeout = Explicit (millisDuration 1)}) Nothing
  status <- fx.dfReadStatus wid
  shutdown dbos
  pure (first, second, status)

-- * Checks

decoded :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue) -> Either String Int
decoded outcome = case outcome of
  Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
    Right value -> Right value
    Left err -> Left ("expected the result to decode, got: " <> show err)
  other -> Left ("expected a recorded result, got: " <> show other)

-- | The run inside its budget records 7 and the row reads SUCCESS.
checkWithinDeadline :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowStatus) -> Either String ()
checkWithinDeadline (ran, status) = do
  value <- decoded ran
  unless (value == 7) $ Left ("expected 7 inside its budget, got: " <> show value)
  unless (status == Just Success) $ Left ("expected the row SUCCESS, got: " <> show status)

-- | The expired run reports the cancellation naming the workflow and the
-- row reads CANCELLED.
checkPastDeadline :: (WorkflowId, Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowStatus) -> Either String ()
checkPastDeadline (WorkflowId widText, ran, status) = do
  case ran of
    Left (WorkflowCancelled {workflowId}) ->
      unless (workflowId == widText) $ Left ("expected the cancellation to name the workflow, got: " <> show workflowId)
    other -> Left ("expected the cancellation, got: " <> show other)
  unless (status == Just Cancelled) $ Left ("expected the row CANCELLED, got: " <> show status)

-- | The relaunch keeps the stamped deadline and the resumed run records 7.
checkKeptDeadline :: (Timestamp, Timestamp, Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) -> Either String ()
checkKeptDeadline (first, second, ran) = do
  unless (second == first) $ Left "expected the relaunch to keep the stamped deadline"
  value <- decoded ran
  unless (value == 7) $ Left ("expected the recovered run to record 7, got: " <> show value)

-- | Shutdown leaves a deadline workflow pending, not cancelled.
checkShutdownPending :: Maybe WorkflowStatus -> Either String ()
checkShutdownPending status =
  unless (status == Just Pending) $ Left ("expected the row PENDING, got: " <> show status)

-- | The recorded outcome beats the expired budget and the row reads
-- SUCCESS.
checkBeatenDeadline :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowStatus) -> Either String ()
checkBeatenDeadline (first, second, status) = do
  firstValue <- decoded first
  unless (firstValue == 7) $ Left ("expected 7 from the first run, got: " <> show firstValue)
  secondValue <- decoded second
  unless (secondValue == 7) $ Left ("expected the recorded outcome to beat the budget, got: " <> show secondValue)
  unless (status == Just Success) $ Left ("expected the row SUCCESS, got: " <> show status)

-- | The sweep cancels an abandoned expired row: a PENDING row with a past
-- deadline and no execution running it reads CANCELLED once a launched
-- executor's sweep passes, with no local timer involved. Returns the
-- polled status; the bounded poll fails loudly when no sweep runs.
scenarioSweepCancelsAbandoned :: forall m. (MonadAsync m, MonadMVar m, MonadFork m, MonadMonotonicTimeNSec m, MonadMask m, MonadDelay m) => DeadlinesFixture m -> m (Maybe WorkflowStatus)
scenarioSweepCancelsAbandoned fx = do
  (dbos, wid) <- fx.dfSetup
  fx.dfPlantExpired wid
  _ <- fx.dfLaunch dbos
  status <- go fx wid (50 :: Int)
  shutdown dbos
  pure status
  where
    go _ _ 0 = error "the sweep never cancelled the abandoned row"
    go fx wid n = do
      status <- fx.dfReadStatus wid
      case status of
        Just Cancelled -> pure status
        _ -> threadDelay 200000 >> go fx wid (n - 1)

-- | The abandoned row reads CANCELLED.
checkSweepCancelsAbandoned :: Maybe WorkflowStatus -> Either String ()
checkSweepCancelsAbandoned status = unless (status == Just Cancelled) $ Left ("expected the abandoned row CANCELLED, got: " <> show status)

-- | Restarting a finished id adopts without rerunning: the second run
-- returns the recorded outcome and the row stays SUCCESS, with no second
-- body execution behind it. Returns both outcomes with the row status.
scenarioFinishedIdAdopts :: forall m. (MonadAsync m, MonadMVar m, MonadFork m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m, MonadMask m)
                         => DeadlinesFixture m -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowStatus)
scenarioFinishedIdAdopts fx = do
  (dbos, wid) <- fx.dfSetup
  let body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () wctx = runStep wctx "only" (const (pure 7))
  ref <- registerWorkflowRef dbos (newQualifiedWorkflowName "finished-once") body >>= either (error . show) pure
  exec <- fx.dfLaunch dbos
  first <- runWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just wid, runTimeout = None}) Nothing
  second <- runWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just wid, runTimeout = None}) Nothing
  status <- fx.dfReadStatus wid
  shutdown dbos
  pure (first, second, status)

-- | Both runs report the recorded 7 and the row stays SUCCESS.
checkFinishedIdAdopts :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowStatus) -> Either String ()
checkFinishedIdAdopts (first, second, status) = do
  firstValue <- decoded first
  unless (firstValue == 7) $ Left ("expected 7 from the first run, got: " <> show firstValue)
  secondValue <- decoded second
  unless (secondValue == 7) $ Left ("expected the recorded outcome on restart, got: " <> show secondValue)
  unless (status == Just Success) $ Left ("expected the row SUCCESS, got: " <> show status)

-- | A budget holds however far the database's clock is from this process's:
-- the body runs for its budget, not less, and is cancelled soon after it,
-- not when this process's clock would say. The sim backend skews its clock
-- reading per fixture; live runs in agreement. Returns the row status with
-- the measured run time in milliseconds.
scenarioSkewHoldsBudget :: forall m. (MonadAsync m, MonadMVar m, MonadFork m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m, MonadMask m)
                        => DeadlinesFixture m -> m (Maybe WorkflowStatus, Integer)
scenarioSkewHoldsBudget fx = do
  (dbos, wid) <- fx.dfSetup
  let body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = do
        threadDelay 30000000
        pure (Right 1)
  ref <- registerWorkflowRef dbos (newQualifiedWorkflowName "bounded") body >>= either (error . show) pure
  exec <- fx.dfLaunch dbos
  before <- getCurrentTime
  _ran <- runWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just wid, runTimeout = Explicit (millisDuration 500)}) Nothing
  after <- getCurrentTime
  status <- fx.dfReadStatus wid
  shutdown dbos
  pure (status, round (diffUTCTime after before * 1000))

-- | The row reads CANCELLED after about the budget: half the budget at
-- least (the body starts a little after the budget does), well under it
-- plus any clock skew.
checkSkewHoldsBudget :: (Maybe WorkflowStatus, Integer) -> Either String ()
checkSkewHoldsBudget (status, elapsedMs) = do
  unless (status == Just Cancelled) $ Left ("expected the row CANCELLED, got: " <> show status)
  unless (elapsedMs >= 250 && elapsedMs <= 10000) $ Left ("expected about a 500ms run, got: " <> show elapsedMs <> "ms")

-- | A delayed release follows the database clock when it runs ahead: the
-- row becomes runnable once the database's own reading passes its release.
-- A sweep comparing on the process clock against the database-resolved
-- release would leave the row delayed, so this twin fails with the sweep
-- reverted to a bound process now under skew — the oracle's own
-- regression case. Mirrors Rust
-- @delays_hold_with_the_database_clock_ahead@.
scenarioDelayedReleaseAhead :: MonadDelay m => DeadlinesFixture m -> m (Maybe WorkflowStatus, Maybe WorkflowStatus)
scenarioDelayedReleaseAhead fx = do
  (_, wid) <- fx.dfSetup
  fx.dfPlantDelayed wid (secondsDuration 3)
  before <- fx.dfReadStatus wid
  threadDelay 5000000
  fx.dfSweepDelayed
  after <- fx.dfReadStatus wid
  pure (before, after)

-- | Delayed before the sweep, runnable after it.
checkDelayedReleaseAhead :: (Maybe WorkflowStatus, Maybe WorkflowStatus) -> Either String ()
checkDelayedReleaseAhead (before, after) = do
  unless (before == Just Delayed) $ Left ("expected the row delayed before the sweep, got: " <> show before)
  unless (after == Just Enqueued) $ Left ("expected the sweep to release the row, got: " <> show after)

-- | A delayed release holds while the database clock has not reached it:
-- a two-hour delay swept at once stays delayed. This pins the hold side
-- of the contract — a sweep that released unconditionally, or a write
-- that resolved to a past instant, would release it. (Unlike the ahead
-- twin it does not discriminate clocks: with the skew behind, a
-- process-clock sweep holds this row too. The oracle's behind-direction
-- clock coverage rides the bounce and the moved delay, which land in
-- S10.) Mirrors Rust @delays_hold_with_the_database_clock_behind@.
scenarioDelayedHoldBehind :: Monad m => DeadlinesFixture m -> m (Maybe WorkflowStatus, Maybe WorkflowStatus)
scenarioDelayedHoldBehind fx = do
  (_, wid) <- fx.dfSetup
  fx.dfPlantDelayed wid (secondsDuration 7200)
  before <- fx.dfReadStatus wid
  fx.dfSweepDelayed
  after <- fx.dfReadStatus wid
  pure (before, after)

-- | Delayed before the sweep, delayed after it.
checkDelayedHoldBehind :: (Maybe WorkflowStatus, Maybe WorkflowStatus) -> Either String ()
checkDelayedHoldBehind (before, after) = do
  unless (before == Just Delayed) $ Left ("expected the row delayed before the sweep, got: " <> show before)
  unless (after == Just Delayed) $ Left ("expected the sweep to hold the row, got: " <> show after)
