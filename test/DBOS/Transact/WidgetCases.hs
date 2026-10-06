{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | The widget store's shared dual-stack scenarios, fixture, and pure checks.
--
-- The scenario bodies carry polymorphic effect constraints; the two trees
-- supply separate interpretations: IO with Postgres handlers and a real
-- launch on the live side, IOSim with STM handlers over MemSystemDB on the
-- sim side. Positive-case checks prove the transactional step's
-- once-and-only-once guarantee by exact counts (app state and per-step
-- commit counts), never at-least-once presence.
module DBOS.Transact.WidgetCases
  ( -- * Shared step-table vocabulary
    OrderId (..),
    CheckoutSteps (..),
    DispatchSteps (..),
    namedStep,

    -- * Fixture and observation
    WidgetCase,
    WidgetFixture (..),
    WidgetObservation (..),

    -- * Bodies and durations
    checkoutBody,
    dispatchBody,
    paymentTimeout,
    dispatchTick,

    -- * Composition scenarios
    scenarioPaidCheckout,
    scenarioRefusedPayment,
    scenarioCannedPaidWriteRefused,
    scenarioCrashWhileWaiting,
    scenarioCrashMidDispatch,
    scenarioLostAck,
    scenarioKilledWhileWaiting,
    scenarioKilledMidDispatch,

    -- * Table scenarios
    TableObservation (..),
    scenarioTableCreate,
    scenarioTableReserveRace,
    scenarioTableFailing,
    scenarioTableStatusCodes,

    -- * Checks
    checkPaidCheckout,
    checkRefusedPayment,
    checkCannedPaidWriteRefused,
    checkCrashWhileWaiting,
    checkCrashMidDispatch,
    checkLostAck,
    lostAckOnce,
    captureCheckoutThread,
    captureDispatchThread,
    waitThread,
    checkTableCreate,
    checkTableReserveRace,
    checkTableFailing,
    checkTableStatusCodes,
  )
where

import Control.Monad.Except (ExceptT (..), runExceptT)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.Prelude
import DBOS.SystemDB (BackendErrorKind (..))
import DBOS.Transact
  ( BackendError (..),
    CodecError,
    DBOS,
    DataSource (..),
    Duration,
    EngineOnly,
    Error,
    Executor,
    IsolationLevel (..),
    StartOptions (..),
    StepCtx,
    Topic (..),
    TransactionConfig (..),
    Tx,
    WorkflowCtx,
    WorkflowId (..),
    WorkflowRef,
    WorkflowStatus (..),
    decodeWorkflowValue,
    encodeWorkflowValue,
    getWorkflowEvent,
    isTerminal,
    millisDuration,
    recv,
    runTxStep,
    secondsDuration,
    sendWorkflowMessage,
    setEvent,
    shutdown,
    sleepStep,
    startChildWorkflow,
    startDBOSWorkflowRef,
    startOptionsDefault,
  )

-- * Shared step-table vocabulary

-- | Order ids cross the steps boundary as a newtype; table columns stay 'Int'.
newtype OrderId = OrderId Int
  deriving stock (Eq, Show)

-- | The checkout's transactional capabilities: one step per field. The
-- 'StepCtx' parameter is the capability that scopes each call.
data CheckoutSteps exec m = CheckoutSteps
  { coCreate :: StepCtx exec m -> m OrderId,
    coReserve :: StepCtx exec m -> m Bool,
    coUndo :: StepCtx exec m -> m (),
    coSetStatus :: StepCtx exec m -> OrderId -> Int -> m ()
  }

-- | The dispatch's capabilities. 'coSetStatus' is repeated from the checkout
-- table per the share-by-two rule (records are cheap; embed at three).
data DispatchSteps exec m = DispatchSteps
  { doTick :: StepCtx exec m -> OrderId -> m (),
    doSetStatus :: StepCtx exec m -> OrderId -> Int -> m ()
  }

-- | The step config for a named transactional step, spelled the way the
-- oracle names its function. SERIALIZABLE mirrors the Python datasource's
-- default: the reserve guard leans on it, and the runner's retry loop absorbs
-- the serialization failures it can produce.
namedStep :: Text -> TransactionConfig
namedStep name = TransactionConfig {txName = Just name, txIsolation = Just Serializable}

-- * Fixture and observation

-- | The union of effects the shared bodies and scenarios run under; both
-- @IO@ and @IOSim s@ satisfy it.
type WidgetCase m =
  ( Monad m,
    MonadSTM m,
    MonadMVar m,
    MonadTimer m,
    MonadTime m,
    MonadDelay m,
    MonadAsync m,
    MonadFork m,
    MonadMask m,
    MonadCatch m
  )

-- | The per-stack interpretation. Capabilities observe, never stage
-- (ADR-0020): the datasource and step tables are the real seam, the reads
-- and waits are observations, and the id/launch/relaunch atoms are the only
-- scheduler-shaped surface. Rank-n fields are read by pattern.
data WidgetFixture m = WidgetFixture
  { wfDataSource :: DataSource m,
    wfDBOS :: DBOS m,
    wfMkCheckout :: forall exec. Tx m -> CheckoutSteps exec m,
    wfMkDispatch :: forall exec. Tx m -> DispatchSteps exec m,
    wfMkFailingCheckout :: forall exec. Tx m -> CheckoutSteps exec m,
    wfCheckoutRef :: WorkflowRef m EngineOnly,
    wfFailingCheckoutRef :: WorkflowRef m EngineOnly,
    wfLostAckCheckoutRef :: WorkflowRef m EngineOnly,
    wfLoseNextAck :: m (),
    wfCheckoutThread :: m (ThreadId m),
    wfDispatchThread :: m (ThreadId m),
    wfLaunch :: m (Executor m),
    wfRelaunch :: m (Executor m),
    wfFreshWorkflowId :: m WorkflowId,
    wfSetInventory :: Int -> m (),
    wfWithTableStep :: forall a. (forall exec. StepCtx exec m -> m a) -> m a,
    wfReadInventory :: m Int,
    wfReadOrders :: m [(Int, Int, Int)],
    wfReadStepCommits :: m (Map Text (Map Text Int)),
    wfReadStatus :: WorkflowId -> m (Maybe WorkflowStatus)
  }

-- | The normalized verdict input every shared check judges: exact app state,
-- per-step commit counts (per workflow text id, so the dispatch child is
-- visible), the checkout's terminal status, and the published order id.
data WidgetObservation = WidgetObservation
  { woCheckoutId :: WorkflowId,
    woInventory :: Int,
    woOrders :: [(Int, Int, Int)],
    woCommits :: Map Text (Map Text Int),
    woCheckoutStatus :: Maybe WorkflowStatus,
    woPublishedOrderId :: Maybe Text
  }
  deriving stock (Eq, Show)

-- * Bodies

-- | How long a checkout waits to be told whether it was paid for. Virtual in
-- the simulator, so tests stay fast and deterministic.
paymentTimeout :: Duration
paymentTimeout = secondsDuration 30

-- | One dispatch tick. Shared by both stacks at the same duration: the
-- simulator's clock is virtual, so a fuller duration is free there and keeps
-- the live mid-dispatch observation window.
dispatchTick :: Duration
dispatchTick = millisDuration 1000

-- | The checkout: create, reserve, publish the payment id, wait for the
-- payment, then dispatch or compensate. Mirrors the oracle's
-- @checkout_workflow@; every application write is a transactional step whose
-- checkpoint shares its commit.
checkoutBody ::
  forall exec m.
  WidgetCase m =>
  DataSource m ->
  (Tx m -> CheckoutSteps exec m) ->
  WorkflowRef m EngineOnly ->
  () ->
  WorkflowCtx exec m ->
  m (Either (Error EngineOnly) Text)
checkoutBody ds mkCheckout dispatchRef () wctx = runExceptT $ do
  orderId <- ExceptT (runTxStep ds (namedStep "create_order") wctx (\sctx tx -> Right . (\(OrderId oid) -> oid) <$> (mkCheckout tx).coCreate sctx))
  onShelf <- ExceptT (runTxStep ds (namedStep "reserve_inventory") wctx (\sctx tx -> Right <$> (mkCheckout tx).coReserve sctx))
  if not onShelf
    then do
      _ <- ExceptT (runTxStep ds (namedStep "cancel_order") wctx (\sctx tx -> Right <$> (mkCheckout tx).coSetStatus sctx (OrderId orderId) (-1)))
      _ <- ExceptT (setEvent wctx "payment_id" (Nothing :: Maybe Text))
      pure "no-inventory"
    else do
      _ <- ExceptT (setEvent wctx "payment_id" (Just (Text.pack (show orderId))))
      decision <- (ExceptT (recv wctx (Just (Topic "payment_status")) paymentTimeout) :: ExceptT (Error EngineOnly) m (Maybe Text))
      case decision of
        Just status | status == "paid" -> do
          _ <- ExceptT (runTxStep ds (namedStep "mark_order_paid") wctx (\sctx tx -> Right <$> (mkCheckout tx).coSetStatus sctx (OrderId orderId) 2))
          _ <- ExceptT (startChildWorkflow wctx dispatchRef startOptionsDefault (Just (encodeWorkflowValue orderId)))
          _ <- ExceptT (setEvent wctx "order_id" (Text.pack (show orderId)))
          pure "paid"
        _ -> do
          _ <- ExceptT (runTxStep ds (namedStep "undo_reserve_inventory") wctx (\sctx tx -> Right <$> (mkCheckout tx).coUndo sctx))
          _ <- ExceptT (runTxStep ds (namedStep "cancel_order") wctx (\sctx tx -> Right <$> (mkCheckout tx).coSetStatus sctx (OrderId orderId) (-1)))
          _ <- ExceptT (setEvent wctx "order_id" (Text.pack (show orderId)))
          pure "cancelled"

-- | The dispatch: three durable ticks, then done.
dispatchBody ::
  forall exec m.
  WidgetCase m =>
  DataSource m ->
  (Tx m -> DispatchSteps exec m) ->
  Int ->
  WorkflowCtx exec m ->
  m (Either (Error EngineOnly) Text)
dispatchBody ds mkDispatch orderId wctx = go (3 :: Int)
  where
    go 0 = pure (Right "dispatched")
    go n = do
      slept <- sleepStep wctx dispatchTick
      case slept of
        Left err -> pure (Left err)
        Right () -> do
          _ <- (runTxStep ds (namedStep "update_order_progress") wctx (\sctx tx -> Right <$> (mkDispatch tx).doTick sctx (OrderId orderId)) :: m (Either (Error EngineOnly) ()))
          go (n - 1)

-- * Observation helpers

-- | Poll an observation until it holds, bounded on both stacks (real time in
-- IO, virtual time in IOSim). An observation, never a staged effect.
waitUntil :: WidgetCase m => Text -> Int -> m Bool -> m ()
waitUntil what attempts act = go attempts
  where
    go 0 = throwIO (userError (Text.unpack ("timed out waiting for " <> what)))
    go n = do
      ok <- act
      if ok then pure () else threadDelay 100000 >> go (n - 1)

-- | Wait for a durable event to be published (blocking reads on both stacks).
waitEvent :: WidgetCase m => WidgetFixture m -> WorkflowId -> Text -> m ()
waitEvent wf wid key = waitUntil ("event " <> key) 30 $ do
  found <- getWorkflowEvent wf.wfDBOS wid key (millisDuration 1000)
  pure (case found of Right (Just _) -> True; _ -> False)

-- | Wait for a step's commit to land (used before a crash, when the row
-- itself cannot say so yet).
waitCommit :: WidgetCase m => WidgetFixture m -> WorkflowId -> Text -> m ()
waitCommit wf wid stepName = waitUntil ("the commit for " <> stepName) 150 $ do
  commits <- wf.wfReadStepCommits
  pure (Map.member stepName (Map.findWithDefault Map.empty (widTextOf wid) commits))
  where
    widTextOf (WorkflowId widText) = widText

-- | Wait for the app-table state to satisfy a predicate. The dispatch needs
-- three one-second ticks plus start latency, so the bound is 15 s of polling
-- on both stacks (real in IO, virtual in IOSim).
waitOrders :: WidgetCase m => WidgetFixture m -> Text -> ([(Int, Int, Int)] -> Bool) -> m ()
waitOrders wf what predicate = waitUntil what 150 (predicate <$> wf.wfReadOrders)

-- | Wait for the checkout's row to reach a terminal status: the event that
-- says the body finished is published just before the outcome flips the row,
-- so observations wait for the row instead of racing it.
waitStatus :: WidgetCase m => WidgetFixture m -> WorkflowId -> m ()
waitStatus wf wid = waitUntil "the checkout to settle" 150 $ do
  status <- wf.wfReadStatus wid
  pure (maybe False isTerminal status)

observe :: WidgetCase m => WidgetFixture m -> WorkflowId -> m WidgetObservation
observe wf wid = do
  inventory <- wf.wfReadInventory
  orders <- wf.wfReadOrders
  commits <- wf.wfReadStepCommits
  status <- wf.wfReadStatus wid
  published <- getWorkflowEvent wf.wfDBOS wid "order_id" (millisDuration 500) >>= \case
    Right (Just stored) -> pure (either (const Nothing) Just (decodeWorkflowValue "order_id" (Just stored) :: Either CodecError Text))
    _ -> pure Nothing
  pure
    WidgetObservation
      { woCheckoutId = wid,
        woInventory = inventory,
        woOrders = orders,
        woCommits = commits,
        woCheckoutStatus = status,
        woPublishedOrderId = published
      }

-- * Composition scenarios

-- | A paid checkout: one order created, one unit reserved, the paid mark, the
-- dispatch child's three ticks — each exactly once.
scenarioPaidCheckout :: WidgetCase m => WidgetFixture m -> m WidgetObservation
scenarioPaidCheckout wf = do
  wid <- wf.wfFreshWorkflowId
  exec <- wf.wfLaunch
  _ <- startDBOSWorkflowRef exec wf.wfCheckoutRef (startOptionsDefault {startWorkflowId = Just wid}) Nothing
  waitEvent wf wid "payment_id"
  _ <- sendWorkflowMessage wf.wfDBOS wid (Just (Topic "payment_status")) Nothing (encodeWorkflowValue ("paid" :: Text))
  waitEvent wf wid "order_id"
  waitStatus wf wid
  waitOrders wf "the order to dispatch" (any (\(_, status, _) -> status == 1))
  observe wf wid

-- | A refused payment: the reservation is undone exactly once and the order
-- is cancelled, with no dispatch child at all.
scenarioRefusedPayment :: WidgetCase m => WidgetFixture m -> m WidgetObservation
scenarioRefusedPayment wf = do
  wid <- wf.wfFreshWorkflowId
  exec <- wf.wfLaunch
  _ <- startDBOSWorkflowRef exec wf.wfCheckoutRef (startOptionsDefault {startWorkflowId = Just wid}) Nothing
  waitEvent wf wid "payment_id"
  _ <- sendWorkflowMessage wf.wfDBOS wid (Just (Topic "payment_status")) Nothing (encodeWorkflowValue ("failed" :: Text))
  waitEvent wf wid "order_id"
  waitStatus wf wid
  observe wf wid

-- | The paid write refuses: the checkout stops with the reservation standing,
-- the failed step leaves no commit and the child never starts. The settle
-- delay is real in IO and virtual in IOSim, so a swallow regression has time
-- to publish.
scenarioCannedPaidWriteRefused :: WidgetCase m => WidgetFixture m -> m WidgetObservation
scenarioCannedPaidWriteRefused wf = do
  wid <- wf.wfFreshWorkflowId
  exec <- wf.wfLaunch
  _ <- startDBOSWorkflowRef exec wf.wfFailingCheckoutRef (startOptionsDefault {startWorkflowId = Just wid}) Nothing
  waitEvent wf wid "payment_id"
  _ <- sendWorkflowMessage wf.wfDBOS wid (Just (Topic "payment_status")) Nothing (encodeWorkflowValue ("paid" :: Text))
  threadDelay 2000000
  observe wf wid

-- | A crash while parked on the payment wait: after relaunch the checkout
-- replays its committed steps — no duplicate order, no duplicate reservation,
-- no new commit — and then completes normally when paid.
scenarioCrashWhileWaiting :: WidgetCase m => WidgetFixture m -> m (WidgetObservation, WidgetObservation, WidgetObservation)
scenarioCrashWhileWaiting wf = do
  wid <- wf.wfFreshWorkflowId
  exec <- wf.wfLaunch
  _ <- startDBOSWorkflowRef exec wf.wfCheckoutRef (startOptionsDefault {startWorkflowId = Just wid}) Nothing
  waitEvent wf wid "payment_id"
  before <- observe wf wid
  shutdown wf.wfDBOS
  _ <- wf.wfRelaunch
  waitEvent wf wid "payment_id"
  after <- observe wf wid
  _ <- sendWorkflowMessage wf.wfDBOS wid (Just (Topic "payment_status")) Nothing (encodeWorkflowValue ("paid" :: Text))
  waitEvent wf wid "order_id"
  waitStatus wf wid
  waitOrders wf "the order to dispatch" (any (\(_, status, _) -> status == 1))
  final <- observe wf wid
  pure (before, after, final)

-- | A crash mid-dispatch: the recorded ticks replay and the remaining ones
-- run, leaving exactly three tick commits in total.
scenarioCrashMidDispatch :: WidgetCase m => WidgetFixture m -> m WidgetObservation
scenarioCrashMidDispatch wf = do
  wid <- wf.wfFreshWorkflowId
  exec <- wf.wfLaunch
  _ <- startDBOSWorkflowRef exec wf.wfCheckoutRef (startOptionsDefault {startWorkflowId = Just wid}) Nothing
  waitEvent wf wid "payment_id"
  _ <- sendWorkflowMessage wf.wfDBOS wid (Just (Topic "payment_status")) Nothing (encodeWorkflowValue ("paid" :: Text))
  waitEvent wf wid "order_id"
  waitOrders wf "a dispatch tick in flight" (any (\(_, status, progress) -> status == 2 && progress <= 2))
  shutdown wf.wfDBOS
  _ <- wf.wfRelaunch
  waitOrders wf "the order to dispatch" (any (\(_, status, _) -> status == 1))
  waitStatus wf wid
  observe wf wid

-- * Table scenarios (direct step scopes, no workflow)

-- | The direct table cases' verdict input: app state plus the (empty)
-- checkpoint record — direct ops must never write one.
data TableObservation = TableObservation
  { toInventory :: Int,
    toOrders :: [(Int, Int, Int)],
    toCommits :: Map Text (Map Text Int)
  }
  deriving stock (Eq, Show)

readTableState :: WidgetCase m => WidgetFixture m -> m TableObservation
readTableState wf = do
  inventory <- wf.wfReadInventory
  orders <- wf.wfReadOrders
  commits <- wf.wfReadStepCommits
  pure TableObservation {toInventory = inventory, toOrders = orders, toCommits = commits}

-- | One table op in its own transaction — the same per-step commit shape the
-- workflows spend — with failures surfaced as values.
withTx :: WidgetCase m => WidgetFixture m -> (Tx m -> m a) -> m (Either BackendError a)
withTx wf act = do
  let DataSource {dsWithTransaction = runTx} = wf.wfDataSource
  runTx Nothing act

txOp :: WidgetCase m => WidgetFixture m -> (Tx m -> m a) -> m a
txOp wf act = withTx wf act >>= either (throwIO . userError . show) pure

-- | The rank-n step-table builders, read by pattern (house record rules).
checkoutOf :: forall m exec. WidgetFixture m -> Tx m -> CheckoutSteps exec m
checkoutOf wf = case wf of WidgetFixture {wfMkCheckout = mk} -> mk

dispatchOf :: forall m exec. WidgetFixture m -> Tx m -> DispatchSteps exec m
dispatchOf wf = case wf of WidgetFixture {wfMkDispatch = mk} -> mk

failingCheckoutOf :: forall m exec. WidgetFixture m -> Tx m -> CheckoutSteps exec m
failingCheckoutOf wf = case wf of WidgetFixture {wfMkFailingCheckout = mk} -> mk

-- | The create op mints three ids; direct ops leave no checkpoint.
scenarioTableCreate :: WidgetCase m => WidgetFixture m -> m ([Int], TableObservation)
scenarioTableCreate wf = do
  let WidgetFixture {wfWithTableStep = withTable} = wf
  ids <- withTable $ \s -> mapM (\_ -> txOp wf (\tx -> (checkoutOf wf tx).coCreate s)) [1 :: Int, 2, 3]
  state <- readTableState wf
  pure ([oid | OrderId oid <- ids], state)

-- | Two reserves race; exactly one may win and stock lands exactly at zero.
scenarioTableReserveRace :: WidgetCase m => WidgetFixture m -> m (Int, TableObservation)
scenarioTableReserveRace wf = do
  wf.wfSetInventory 1
  let WidgetFixture {wfWithTableStep = withTable} = wf
      racer =
        withTable $ \s -> do
          outcome <- withTx wf (\tx -> (checkoutOf wf tx).coReserve s)
          pure (either (const False) id outcome)
  first <- async racer
  second <- async racer
  a <- wait first
  b <- wait second
  state <- readTableState wf
  let winners = (if a then 1 else 0) + (if b then 1 else 0) :: Int
  pure (winners, state)

-- | The canned third-call refusal: the first two ops stand, the third —
-- including anything it wrote — is absent.
scenarioTableFailing :: WidgetCase m => WidgetFixture m -> m (Bool, TableObservation)
scenarioTableFailing wf = do
  let WidgetFixture {wfWithTableStep = withTable} = wf
  threw <- withTable $ \s -> do
    oid <- txOp wf (\tx -> (failingCheckoutOf wf tx).coCreate s)
    reserved <- txOp wf (\tx -> (failingCheckoutOf wf tx).coReserve s)
    if not reserved
      then pure False
      else do
        result <- try (withTx wf (\tx -> (failingCheckoutOf wf tx).coSetStatus s oid 2))
        pure (either (const True) (either (const True) (const False)) (result :: Either SomeException (Either BackendError ())))
  state <- readTableState wf
  pure (threw, state)

-- | Three ticks walk a paid order to dispatched: status 1, progress 0.
scenarioTableStatusCodes :: WidgetCase m => WidgetFixture m -> m ((Int, Int, Int), TableObservation)
scenarioTableStatusCodes wf = do
  let WidgetFixture {wfWithTableStep = withTable} = wf
  withTable $ \s -> do
    oid <- txOp wf (\tx -> (checkoutOf wf tx).coCreate s)
    reserved <- txOp wf (\tx -> (checkoutOf wf tx).coReserve s)
    if not reserved
      then pure ()
      else do
        _ <- txOp wf (\tx -> (checkoutOf wf tx).coSetStatus s oid 2)
        mapM_ (\_ -> txOp wf (\tx -> (dispatchOf wf tx).doTick s oid)) [1 :: Int, 2, 3]
  state <- readTableState wf
  let order = case [row | row@(oid, _, _) <- state.toOrders, oid == 1] of
        (row : _) -> row
        [] -> (0, 0, 0)
  pure (order, state)

-- * Abrupt-death fault injection

-- | Record the thread every step op runs on. A step handler runs on its
-- workflow's thread, so the fixture can hand a crash scenario the id to
-- 'killThread' — the abrupt-death counterpart of the engine's cooperative
-- shutdown.
captureCheckoutThread :: (MonadFork m, MonadSTM m) => StrictTVar m (Maybe (ThreadId m)) -> CheckoutSteps exec m -> CheckoutSteps exec m
captureCheckoutThread captured steps =
  steps
    { coCreate = \s -> noteThread captured >> steps.coCreate s,
      coReserve = \s -> noteThread captured >> steps.coReserve s,
      coUndo = \s -> noteThread captured >> steps.coUndo s,
      coSetStatus = \s oid status -> noteThread captured >> steps.coSetStatus s oid status
    }

captureDispatchThread :: (MonadFork m, MonadSTM m) => StrictTVar m (Maybe (ThreadId m)) -> DispatchSteps exec m -> DispatchSteps exec m
captureDispatchThread captured steps =
  steps
    { doTick = \s oid -> noteThread captured >> steps.doTick s oid,
      doSetStatus = \s oid status -> noteThread captured >> steps.doSetStatus s oid status
    }

noteThread :: (MonadFork m, MonadSTM m) => StrictTVar m (Maybe (ThreadId m)) -> m ()
noteThread captured = do
  tid <- myThreadId
  atomically (writeTVar captured (Just tid))

waitThread :: WidgetCase m => Text -> StrictTVar m (Maybe (ThreadId m)) -> m (ThreadId m)
waitThread what captured = do
  waitUntil ("the " <> what <> " workflow's thread") 150 (maybe False (const True) <$> readTVarIO captured)
  found <- readTVarIO captured
  pure (maybe (error "waitThread: no thread was captured") id found)

-- | Kill the parked checkout with an async exception (abrupt death, no
-- cooperative shutdown), relaunch, and finish: the replay must not duplicate
-- the reserved steps.
scenarioKilledWhileWaiting :: WidgetCase m => WidgetFixture m -> m (WidgetObservation, WidgetObservation, WidgetObservation)
scenarioKilledWhileWaiting wf = do
  wid <- wf.wfFreshWorkflowId
  exec <- wf.wfLaunch
  _ <- startDBOSWorkflowRef exec wf.wfCheckoutRef (startOptionsDefault {startWorkflowId = Just wid}) Nothing
  waitEvent wf wid "payment_id"
  before <- observe wf wid
  killed <- wf.wfCheckoutThread
  killThread killed
  threadDelay 200000
  shutdown wf.wfDBOS
  _ <- wf.wfRelaunch
  waitEvent wf wid "payment_id"
  after <- observe wf wid
  _ <- sendWorkflowMessage wf.wfDBOS wid (Just (Topic "payment_status")) Nothing (encodeWorkflowValue ("paid" :: Text))
  waitEvent wf wid "order_id"
  waitStatus wf wid
  waitOrders wf "the order to dispatch" (any (\(_, status, _) -> status == 1))
  final <- observe wf wid
  pure (before, after, final)

-- | Kill the dispatch child mid-tick and resume: exactly three tick commits
-- in total, the recorded ones replayed rather than re-run.
scenarioKilledMidDispatch :: WidgetCase m => WidgetFixture m -> m WidgetObservation
scenarioKilledMidDispatch wf = do
  wid <- wf.wfFreshWorkflowId
  exec <- wf.wfLaunch
  _ <- startDBOSWorkflowRef exec wf.wfCheckoutRef (startOptionsDefault {startWorkflowId = Just wid}) Nothing
  waitEvent wf wid "payment_id"
  _ <- sendWorkflowMessage wf.wfDBOS wid (Just (Topic "payment_status")) Nothing (encodeWorkflowValue ("paid" :: Text))
  waitEvent wf wid "order_id"
  waitOrders wf "a dispatch tick in flight" (any (\(_, status, progress) -> status == 2 && progress <= 2))
  killed <- wf.wfDispatchThread
  killThread killed
  threadDelay 200000
  shutdown wf.wfDBOS
  _ <- wf.wfRelaunch
  waitOrders wf "the order to dispatch" (any (\(_, status, _) -> status == 1))
  waitStatus wf wid
  observe wf wid

-- * Commit-boundary fault injection

-- | A datasource that loses the acknowledgement of the next successful
-- transaction: the commit lands, the caller sees a transport-class failure.
-- The synthetic error carries no SQLSTATE, so the runner classifies it as
-- non-retriable control — the run stops without recording an outcome, and the
-- next execution's pre-check replays what committed. This is the
-- commit-boundary fault the exactly-once proof needs.
lostAckOnce :: MonadSTM m => StrictTVar m Int -> DataSource m -> DataSource m
lostAckOnce armed ds =
  ds
    { dsWithTransaction = \isolation action -> do
        outcome <- runReal isolation action
        case outcome of
          Left err -> pure (Left err)
          Right _ -> do
            shouldLose <- atomically $ do
              n <- readTVar armed
              if n > 0
                then writeTVar armed (n - 1) >> pure True
                else pure False
            pure (if shouldLose then Left lostAckError else outcome)
    }
  where
    DataSource {dsWithTransaction = runReal} = ds
    lostAckError =
      BackendError
        { backendMessage = "injected: the commit landed but its acknowledgement was lost",
          backendSqlState = Nothing,
          backendKind = Permanent
        }

-- | Arm the fault, start the checkout, let the committed step land, crash,
-- relaunch, and finish: the paid-checkout check then proves the committed
-- step replayed instead of re-committing.
scenarioLostAck :: WidgetCase m => WidgetFixture m -> m WidgetObservation
scenarioLostAck wf = do
  wid <- wf.wfFreshWorkflowId
  wf.wfLoseNextAck
  exec <- wf.wfLaunch
  _ <- startDBOSWorkflowRef exec wf.wfLostAckCheckoutRef (startOptionsDefault {startWorkflowId = Just wid}) Nothing
  waitCommit wf wid "create_order"
  threadDelay 200000
  shutdown wf.wfDBOS
  _ <- wf.wfRelaunch
  waitEvent wf wid "payment_id"
  _ <- sendWorkflowMessage wf.wfDBOS wid (Just (Topic "payment_status")) Nothing (encodeWorkflowValue ("paid" :: Text))
  waitEvent wf wid "order_id"
  waitStatus wf wid
  waitOrders wf "the order to dispatch" (any (\(_, status, _) -> status == 1))
  observe wf wid

-- * Checks

commitsFor :: WidgetObservation -> WorkflowId -> Map Text Int
commitsFor wo (WorkflowId widText) = Map.findWithDefault Map.empty widText wo.woCommits

childCommits :: WidgetObservation -> [Map Text Int]
childCommits wo = [commits | (widText, commits) <- Map.toList wo.woCommits, widText /= checkoutText wo]

checkoutText :: WidgetObservation -> Text
checkoutText wo = let WorkflowId widText = wo.woCheckoutId in widText

checkPaidCheckout :: WidgetObservation -> Either String ()
checkPaidCheckout wo
  | wo.woInventory /= 4 = Left ("inventory must be down by exactly one, got " <> show wo.woInventory)
  | wo.woOrders /= [(1, 1, 0)] = Left ("the order must be dispatched with no progress left, got " <> show wo.woOrders)
  | commitsFor wo wo.woCheckoutId /= Map.fromList [("create_order", 1), ("reserve_inventory", 1), ("mark_order_paid", 1)] =
      Left ("the checkout must commit each step exactly once, got " <> show (commitsFor wo wo.woCheckoutId))
  | childCommits wo /= [Map.singleton "update_order_progress" 3] =
      Left ("the dispatch child must commit exactly three ticks, got " <> show (childCommits wo))
  | wo.woCheckoutStatus /= Just Success = Left ("the checkout must succeed, got " <> show wo.woCheckoutStatus)
  | wo.woPublishedOrderId /= Just "1" = Left ("the checkout must publish order 1, got " <> show wo.woPublishedOrderId)
  | otherwise = Right ()

checkRefusedPayment :: WidgetObservation -> Either String ()
checkRefusedPayment wo
  | wo.woInventory /= 5 = Left ("inventory must be restored to exactly five, got " <> show wo.woInventory)
  | wo.woOrders /= [(1, -1, 3)] = Left ("the order must be cancelled with full progress, got " <> show wo.woOrders)
  | commitsFor wo wo.woCheckoutId /= Map.fromList [("create_order", 1), ("reserve_inventory", 1), ("undo_reserve_inventory", 1), ("cancel_order", 1)] =
      Left ("the checkout must commit each step exactly once, got " <> show (commitsFor wo wo.woCheckoutId))
  | childCommits wo /= [] = Left ("a refused checkout must start no child, got " <> show (childCommits wo))
  | wo.woCheckoutStatus /= Just Success = Left ("the checkout must settle successfully, got " <> show wo.woCheckoutStatus)
  | wo.woPublishedOrderId /= Just "1" = Left ("the checkout must publish order 1, got " <> show wo.woPublishedOrderId)
  | otherwise = Right ()

checkCannedPaidWriteRefused :: WidgetObservation -> Either String ()
checkCannedPaidWriteRefused wo
  | wo.woInventory /= 4 = Left ("the reservation must stand, got " <> show wo.woInventory)
  | wo.woOrders /= [(1, 0, 3)] = Left ("the order must stay pending with full progress, got " <> show wo.woOrders)
  | commitsFor wo wo.woCheckoutId /= Map.fromList [("create_order", 1), ("reserve_inventory", 1)] =
      Left ("the failed paid write must leave no commit, got " <> show (commitsFor wo wo.woCheckoutId))
  | childCommits wo /= [] = Left ("the failed checkout must start no child, got " <> show (childCommits wo))
  | wo.woPublishedOrderId /= Nothing = Left ("the failed checkout must publish no order id, got " <> show wo.woPublishedOrderId)
  | otherwise = Right ()

-- | The effect record a crash must leave byte-identical: app state, commit
-- counts, and what was published.
effectsOf :: WidgetObservation -> (Int, [(Int, Int, Int)], Map Text (Map Text Int), Maybe Text)
effectsOf wo = (wo.woInventory, wo.woOrders, wo.woCommits, wo.woPublishedOrderId)

checkCrashWhileWaiting :: (WidgetObservation, WidgetObservation, WidgetObservation) -> Either String ()
checkCrashWhileWaiting (before, after, final)
  | effectsOf before /= effectsOf after =
      Left ("the recovery must not duplicate effects: before " <> show (effectsOf before) <> ", after " <> show (effectsOf after))
  | otherwise = checkPaidCheckout final

checkCrashMidDispatch :: WidgetObservation -> Either String ()
checkCrashMidDispatch = checkPaidCheckout

-- | The lost acknowledgement must leave exactly the paid-checkout record: the
-- committed step replays, never re-commits.
checkLostAck :: WidgetObservation -> Either String ()
checkLostAck = checkPaidCheckout

checkTableCreate :: ([Int], TableObservation) -> Either String ()
checkTableCreate (ids, to)
  | ids /= [1, 2, 3] = Left ("the create op must mint exactly ids 1,2,3, got " <> show ids)
  | to.toOrders /= [(1, 0, 3), (2, 0, 3), (3, 0, 3)] = Left ("three pending orders with full progress, got " <> show to.toOrders)
  | to.toInventory /= 5 = Left ("create must not touch inventory, got " <> show to.toInventory)
  | to.toCommits /= Map.empty = Left ("direct table ops must record no checkpoints, got " <> show to.toCommits)
  | otherwise = Right ()

checkTableReserveRace :: (Int, TableObservation) -> Either String ()
checkTableReserveRace (winners, to)
  | winners /= 1 = Left ("exactly one reserve may win, got " <> show winners)
  | to.toInventory /= 0 = Left ("the one winner leaves stock exactly zero, got " <> show to.toInventory)
  | to.toOrders /= [] = Left ("a reserve mints no order, got " <> show to.toOrders)
  | to.toCommits /= Map.empty = Left ("direct table ops must record no checkpoints, got " <> show to.toCommits)
  | otherwise = Right ()

checkTableFailing :: (Bool, TableObservation) -> Either String ()
checkTableFailing (threw, to)
  | not threw = Left "the third call must abort"
  | to.toInventory /= 4 = Left ("the first two ops must stand, got " <> show to.toInventory)
  | to.toOrders /= [(1, 0, 3)] = Left ("the create stands and the status write is absent, got " <> show to.toOrders)
  | to.toCommits /= Map.empty = Left ("direct table ops must record no checkpoints, got " <> show to.toCommits)
  | otherwise = Right ()

checkTableStatusCodes :: ((Int, Int, Int), TableObservation) -> Either String ()
checkTableStatusCodes (order, to)
  | order /= (1, 1, 0) = Left ("three ticks dispatch the order, got " <> show order)
  | to.toCommits /= Map.empty = Left ("direct table ops must record no checkpoints, got " <> show to.toCommits)
  | otherwise = Right ()
