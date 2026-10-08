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
    checkWithinDeadline,
    checkPastDeadline,
    checkKeptDeadline,
    checkShutdownPending,
    checkBeatenDeadline,
  )
where

import DBOS.Prelude
import DBOS.SystemDB (Timestamp, WorkflowId (..), WorkflowStatus (..), millisDuration, secondsDuration)
import DBOS.SystemDB qualified as SysDB
import DBOS.Transact
  ( CodecError,
    DBOS,
    EngineOnly,
    Error (..),
    Executor,
    RunOptions (..),
    SerializedWorkflowValue (..),
    Timeout (..),
    WorkflowCtx,
    decodeWorkflowValue,
    newWorkflowKey,
    registerWorkflowRef,
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
    dfReadDeadline :: WorkflowId -> m (Maybe Timestamp)
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
scenarioWithinDeadline :: forall m. (MonadMVar m, MonadFork m, MonadTime m, MonadTimer m, MonadMask m) => DeadlinesFixture m -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowStatus)
scenarioWithinDeadline fx = do
  (dbos, wid) <- fx.dfSetup
  let body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = pure (Right 7)
  ref <- registerWorkflowRef dbos (newWorkflowKey "quick") body >>= either (error . show) pure
  exec <- fx.dfLaunch dbos
  ran <- runWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just wid, runTimeout = Explicit (secondsDuration 30)}) Nothing
  status <- fx.dfReadStatus wid
  shutdown dbos
  pure (ran, status)

-- | A workflow past its deadline is cancelled: the run reports the
-- cancellation naming the workflow and the row reads CANCELLED.
scenarioPastDeadline :: forall m. (MonadMVar m, MonadFork m, MonadTime m, MonadTimer m, MonadMask m) => DeadlinesFixture m -> m (WorkflowId, Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowStatus)
scenarioPastDeadline fx = do
  (dbos, wid) <- fx.dfSetup
  let body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = do
        threadDelay 30000000
        pure (Right 1)
  ref <- registerWorkflowRef dbos (newWorkflowKey "runs-forever") body >>= either (error . show) pure
  exec <- fx.dfLaunch dbos
  ran <- runWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just wid, runTimeout = Explicit (millisDuration 100)}) Nothing
  status <- fx.dfReadStatus wid
  shutdown dbos
  pure (wid, ran, status)

-- | A recovered workflow keeps the deadline it already had: the deadline
-- stamped before the crash is the deadline after the relaunch, and the
-- resumed run records its result.
scenarioKeptDeadline :: forall m. (MonadMVar m, MonadFork m, MonadAsync m, MonadTime m, MonadTimer m, MonadMask m) => DeadlinesFixture m -> m (Timestamp, Timestamp, Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
scenarioKeptDeadline fx = do
  (dbos, wid) <- fx.dfSetup
  gate <- newEmptyMVar
  let body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = takeMVar gate >> pure (Right 7)
  ref <- registerWorkflowRef dbos (newWorkflowKey "gated") body >>= either (error . show) pure
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
scenarioShutdownPending :: forall m. (MonadMVar m, MonadFork m, MonadAsync m, MonadTime m, MonadTimer m, MonadMask m) => DeadlinesFixture m -> m (Maybe WorkflowStatus)
scenarioShutdownPending fx = do
  (dbos, wid) <- fx.dfSetup
  gate <- newEmptyMVar
  let body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = takeMVar gate >> pure (Right 7)
  ref <- registerWorkflowRef dbos (newWorkflowKey "gated") body >>= either (error . show) pure
  exec <- fx.dfLaunch dbos
  worker <- async (runWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just wid, runTimeout = Explicit (secondsDuration 30)}) Nothing)
  _ <- waitForDeadline fx wid
  shutdown dbos
  cancel worker
  fx.dfReadStatus wid

-- | A deadline that loses to a recorded outcome reports that outcome: the
-- completed run wins, and a 1ms budget against the recording still reads
-- the result.
scenarioBeatenDeadline :: forall m. (MonadMVar m, MonadFork m, MonadTime m, MonadTimer m, MonadMask m) => DeadlinesFixture m -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowStatus)
scenarioBeatenDeadline fx = do
  (dbos, wid) <- fx.dfSetup
  let body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = pure (Right 7)
  ref <- registerWorkflowRef dbos (newWorkflowKey "quick") body >>= either (error . show) pure
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
    Left (ErrorSystemDatabase (SysDB.WorkflowCancelled {workflowId})) ->
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
