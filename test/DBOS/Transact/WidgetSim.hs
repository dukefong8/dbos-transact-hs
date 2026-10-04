{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The widget store's checkout and dispatch over the transactional-step
-- engine under IOSim — the sim half of the dual-stack mirror. The shared
-- scenarios, fixture, and checks live in "DBOS.Transact.WidgetCases"; this
-- module owns the sim interpretation: STM step handlers over TVars, the
-- checkpoint-map fake datasource (the exactly-once instrument), and
-- MemSystemDB plus the sim carrier.
module DBOS.Transact.WidgetSim (tests) where

import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM, StrictTVar, atomically, modifyTVar, newTVarIO, readTVar, readTVarIO, writeTVar)
import Control.Monad.IOSim (IOSim, SimEventType (..), SimTrace, traceEvents)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (printSimTrace, simTracer)
import DBOS.Prelude
import DBOS.SystemDB.IOSim (memLaunchOn, newMemDB, simConnectionWith, simInstance)
import DBOS.Transact
  ( DBOS,
    DataSource (..),
    Identity (..),
    RecordedOutcome (..),
    StepCtx,
    Tx (..),
    WorkflowId (..),
    firstStepStatus,
    handleStatus,
    newWorkflowKey,
    nextWorkflowMarker,
    registerDBOSWorkflowRef,
    retrieveWorkflow,
    withStep,
    withWorkflow,
  )
import DBOS.Transact.WidgetCases
  ( CheckoutSteps (..),
    lostAckOnce,
    DispatchSteps (..),
    OrderId (..),
    WidgetFixture (..),
    checkCannedPaidWriteRefused,
    checkCrashMidDispatch,
    captureCheckoutThread,
    captureDispatchThread,
    waitThread,
    checkCrashWhileWaiting,
    checkLostAck,
    checkPaidCheckout,
    checkRefusedPayment,
    checkoutBody,
    dispatchBody,
    checkTableCreate,
    checkTableFailing,
    checkTableReserveRace,
    checkTableStatusCodes,
    scenarioCannedPaidWriteRefused,
    scenarioCrashMidDispatch,
    scenarioCrashWhileWaiting,
    scenarioKilledMidDispatch,
    scenarioKilledWhileWaiting,
    scenarioLostAck,
    scenarioPaidCheckout,
    scenarioRefusedPayment,
    scenarioTableCreate,
    scenarioTableFailing,
    scenarioTableReserveRace,
    scenarioTableStatusCodes,
  )
import Test.Tasty (DependencyType (..), TestTree, dependentTestGroup)
import Test.Tasty.HUnit (assertBool, testCase)

-- * The sim storefront

-- | The storefront's tables: inventory, orders (id -> status, progress), the
-- order-id sequence, and the checkpoint map — the local
-- @transaction_completion@ the fake datasource writes.
data WidgetStore m = WidgetStore
  { wsInventory :: StrictTVar m Int,
    wsOrders :: StrictTVar m (Map Int (Int, Int)),
    wsNextOrder :: StrictTVar m Int,
    wsCommits :: StrictTVar m (Map (Text, Int) (Text, RecordedOutcome))
  }

newWidgetStore :: MonadSTM m => Int -> m (WidgetStore m)
newWidgetStore stock = do
  wsInventory <- newTVarIO stock
  wsOrders <- newTVarIO Map.empty
  wsNextOrder <- newTVarIO 1
  wsCommits <- newTVarIO Map.empty
  pure WidgetStore {wsInventory = wsInventory, wsOrders = wsOrders, wsNextOrder = wsNextOrder, wsCommits = wsCommits}

-- | The STM handler: one atomic block per op. The split read-then-write of
-- the old fakes is closed — concurrent checkouts cannot mint one id or
-- oversell one unit.
stmCheckoutSteps :: WidgetStore (IOSim s) -> CheckoutSteps exec (IOSim s)
stmCheckoutSteps store =
  CheckoutSteps
    { coCreate = \_ -> atomically $ do
        oid <- readTVar store.wsNextOrder
        writeTVar store.wsNextOrder (oid + 1)
        modifyTVar store.wsOrders (Map.insert oid (0, 3))
        pure (OrderId oid),
      coReserve = \_ -> atomically $ do
        stock <- readTVar store.wsInventory
        if stock > 0
          then writeTVar store.wsInventory (stock - 1) >> pure True
          else pure False,
      coUndo = \_ -> atomically (modifyTVar store.wsInventory (+ 1)),
      coSetStatus = \_ (OrderId oid) status -> atomically $
        modifyTVar store.wsOrders (Map.adjust (\(_, progress) -> (status, progress)) oid)
    }

stmDispatchSteps :: WidgetStore (IOSim s) -> DispatchSteps exec (IOSim s)
stmDispatchSteps store =
  DispatchSteps
    { doTick = \_ (OrderId oid) -> atomically $
        modifyTVar store.wsOrders $
          Map.adjust
            (\(status, progress) -> if progress <= 1 then (1, 0) else (status, progress - 1))
            oid,
      doSetStatus = \_ (OrderId oid) status -> atomically $
        modifyTVar store.wsOrders (Map.adjust (\(_, progress) -> (status, progress)) oid)
    }

-- | The canned refusal: the third call — the paid write — aborts via
-- 'throwSTM', so the whole block vanishes.
failingCheckoutSteps :: StrictTVar (IOSim s) Int -> WidgetStore (IOSim s) -> CheckoutSteps exec (IOSim s)
failingCheckoutSteps calls store =
  CheckoutSteps
    { coCreate = \s -> do
        _ <- atomically (modifyTVar calls (+ 1))
        (stmCheckoutSteps store).coCreate s,
      coReserve = \s -> do
        _ <- atomically (modifyTVar calls (+ 1))
        (stmCheckoutSteps store).coReserve s,
      coUndo = (stmCheckoutSteps store).coUndo,
      coSetStatus = \s oid status -> do
        n <- atomically (modifyTVar calls (+ 1) >> readTVar calls)
        if n == 3
          then atomically (throwSTM (userError "mark_order_paid refused"))
          else (stmCheckoutSteps store).coSetStatus s oid status
    }

-- * The fake datasource (the exactly-once instrument)

-- | The app datasource: every op runs in a transaction; the fake's
-- 'atomically' block is the shared commit. Checkpoints are modelled for real
-- — an insert-or-@False@ map keyed by (workflow, step id), the
-- @transaction_completion@ PK — so a duplicate commit is observable and the
-- engine's adopt signal is exercised, which is what makes the
-- once-and-only-once checks provable under IOSim.
mkWidgetDs :: WidgetStore (IOSim s) -> DataSource (IOSim s)
mkWidgetDs store =
  DataSource
    { dsName = "widget-db",
      dsSchema = "dbos",
      dsCheck = \(WorkflowId widText) _name step -> do
        commits <- readTVarIO store.wsCommits
        pure (Right (snd <$> Map.lookup (widText, step) commits)),
      dsWithTransaction = \_ action -> Right <$> action (Tx (\_ _ -> error "widget fake: statements unsupported")),
      dsRecordOutput = \(Tx _) (WorkflowId widText) name step text ->
        insertCommit store (widText, step) (name, RecordedOutput text),
      dsRecordError = \(Tx _) (WorkflowId widText) name step text ->
        insertCommit store (widText, step) (name, RecordedError text),
      dsStepName = \(WorkflowId widText) step -> do
        commits <- readTVarIO store.wsCommits
        pure (Right (fst <$> Map.lookup (widText, step) commits)),
      dsDeleteCheckpoints = \(WorkflowId widText) step -> do
        atomically (modifyTVar store.wsCommits (Map.filterWithKey (\(w, s) _ -> w /= widText || s < step)))
        pure (Right ())
    }

-- | Insert a checkpoint unless the step already holds one — the
-- @transaction_completion@ primary key, and the signal the engine's adopt
-- path consumes.
insertCommit :: MonadSTM m => WidgetStore m -> (Text, Int) -> (Text, RecordedOutcome) -> m Bool
insertCommit store key value = atomically $ do
  commits <- readTVar store.wsCommits
  if Map.member key commits
    then pure False
    else do
      writeTVar store.wsCommits (Map.insert key value commits)
      pure True

-- * The sim fixture

-- | One sim interpretation of the shared fixture: a fresh Mem database, the
-- widget store, both checkout registrations, the sim launch, and the
-- observation capabilities the shared scenarios use.
simWidgetFixture :: IOSim s (WidgetFixture (IOSim s))
simWidgetFixture = do
  mem <- newMemDB
  dbos <- simInstance
  store <- newWidgetStore 5
  calls <- newTVarIO 0
  let ds = mkWidgetDs store
  checkoutTid <- newTVarIO Nothing
  dispatchTid <- newTVarIO Nothing
  dispatchRef <- either (error . show) id <$> registerDBOSWorkflowRef dbos (newWorkflowKey "DispatchOrderWorkflow") (dispatchBody ds (\_tx -> captureDispatchThread dispatchTid (stmDispatchSteps store)))
  checkoutRef <- either (error . show) id <$> registerDBOSWorkflowRef dbos (newWorkflowKey "CheckoutWorkflow") (checkoutBody ds (\_tx -> captureCheckoutThread checkoutTid (stmCheckoutSteps store)) dispatchRef)
  failingRef <- either (error . show) id <$> registerDBOSWorkflowRef dbos (newWorkflowKey "CheckoutCannedFailWorkflow") (checkoutBody ds (\_tx -> failingCheckoutSteps calls store) dispatchRef)
  acked <- newTVarIO 0
  let lostDs = lostAckOnce acked ds
  lostRef <- either (error . show) id <$> registerDBOSWorkflowRef dbos (newWorkflowKey "CheckoutLostAckWorkflow") (checkoutBody lostDs (\_tx -> stmCheckoutSteps store) dispatchRef)
  pure
    WidgetFixture
      { wfDataSource = ds,
        wfDBOS = dbos,
        wfMkCheckout = \_tx -> stmCheckoutSteps store,
        wfMkDispatch = \_tx -> stmDispatchSteps store,
        wfMkFailingCheckout = \_tx -> failingCheckoutSteps calls store,
        wfCheckoutRef = checkoutRef,
        wfFailingCheckoutRef = failingRef,
        wfLostAckCheckoutRef = lostRef,
        wfLoseNextAck = atomically (writeTVar acked 1),
        wfCheckoutThread = waitThread "checkout" checkoutTid,
        wfDispatchThread = waitThread "dispatch" dispatchTid,
        wfLaunch = memLaunchOn mem simTracer dbos,
        wfRelaunch = memLaunchOn mem simTracer dbos,
        wfFreshWorkflowId = pure (WorkflowId "widget-wf-1"),
        wfSetInventory = \n -> atomically (writeTVar store.wsInventory n),
        wfWithTableStep = \body -> withTableStep store body,
        wfReadInventory = readTVarIO store.wsInventory,
        wfReadOrders = map (\(oid, (status, progress)) -> (oid, status, progress)) . Map.toList <$> readTVarIO store.wsOrders,
        wfReadStepCommits = do
          commits <- readTVarIO store.wsCommits
          pure (Map.fromListWith (Map.unionWith (+)) [(widText, Map.singleton name 1) | ((widText, _), (name, _)) <- Map.toList commits]),
        wfReadStatus = \wid -> do
          found <- retrieveWorkflow dbos wid
          case found of
            Left _ -> pure Nothing
            Right wfHandle -> either (const Nothing) id <$> handleStatus wfHandle
      }

-- * The table step scope (sim interpretation)

-- | A sim identity for the direct table cases.
tableIdentity :: Identity
tableIdentity =
  Identity
    { identityAppName = "sim-app",
      identityAppVersion = "0.0.0",
      identityExecutorId = "sim-executor",
      identityAppId = ""
    }

-- | Run one table op under a real step scope: the workflow view, then a
-- step view, exactly the shape the workflows use.
withTableStep :: WidgetStore (IOSim s) -> (forall exec. StepCtx exec (IOSim s) -> IOSim s a) -> IOSim s a
withTableStep _ body = do
  conn <- simConnectionWith simTracer
  withWorkflow conn tableIdentity (WorkflowId "widget-tables") Nothing $ \wctx -> do
    marker <- nextWorkflowMarker wctx
    withStep wctx marker (firstStepStatus 0) body


-- * Scheduler-event assertions (sim-only)

-- | The scheduler's own record. These tighten individual sim leaves: the
-- shared checks judge outcomes, these assert the concurrency that produced
-- them actually happened in the simulator — never in IO.
eventTypes :: SimTrace a -> [SimEventType]
eventTypes = map (\(_, _, _, eventType) -> eventType) . traceEvents

countEvents :: (SimEventType -> Bool) -> SimTrace a -> Int
countEvents predicate = length . filter predicate . eventTypes

isFork, isCommit, isTxBlocked, isWakeup, isDelay, isUnblocked :: SimEventType -> Bool
isFork EventThreadForked {} = True
isFork _ = False
isCommit EventTxCommitted {} = True
isCommit _ = False
isTxBlocked EventTxBlocked {} = True
isTxBlocked _ = False
isWakeup EventTxWakeup {} = True
isWakeup _ = False
isDelay EventThreadDelay {} = True
isDelay _ = False
isUnblocked EventUnblocked {} = True
isUnblocked _ = False

-- | The reserve race must actually race: both racer threads are forked, and a
-- commit lands after the second fork (the winner's; the stock setup commits
-- before either racer exists).
traceTableReserveRace :: SimTrace a -> IO ()
traceTableReserveRace tr = do
  let types = eventTypes tr
      forkIndexes = [i | (i, e) <- zip [0 :: Int ..] types, isFork e]
      commitIndexes = [i | (i, e) <- zip [0 :: Int ..] types, isCommit e]
  assertBool "both reserve racers must be forked" (length forkIndexes >= 2)
  assertBool "the winning reserve must commit after both forks" $
    case forkIndexes of
      (_ : secondFork : _) -> any (> secondFork) commitIndexes
      _ -> False

-- | The crash-while-waiting mirror: the relaunch re-forks the recovered
-- workflow, it blocks on its durable wait, and a wakeup releases it.
traceCrashWhileWaiting :: SimTrace a -> IO ()
traceCrashWhileWaiting tr = do
  assertBool "the recovered workflow must be re-forked" (countEvents isFork tr >= 2)
  assertBool "the parked workflow must block on its wait" (countEvents isTxBlocked tr >= 1 || countEvents isDelay tr >= 1)
  assertBool "a wakeup must release the wait" (countEvents isWakeup tr >= 1 || countEvents isUnblocked tr >= 1)

-- | The crash-mid-dispatch mirror: the ticks sleep on the sim clock and the
-- recovered run commits its remaining ticks.
traceCrashMidDispatch :: SimTrace a -> IO ()
traceCrashMidDispatch tr = do
  assertBool "the dispatch ticks must sleep on the sim clock" (countEvents isDelay tr >= 1)
  assertBool "the recovered dispatch must commit its remaining ticks" (countEvents isCommit tr >= 4)

-- * The tree

tests :: TestTree
tests =
  dependentTestGroup
    "Widget composition (IOSim)"
    AllFinish
    [ simCase simWidgetFixture "a paid checkout dispatches the order and keeps inventory down" scenarioPaidCheckout checkPaidCheckout printSimTrace,
      simCase simWidgetFixture "a refused payment restores inventory and cancels the order" scenarioRefusedPayment checkRefusedPayment printSimTrace,
      simCase simWidgetFixture "a refused paid write stops the checkout instead of dispatching" scenarioCannedPaidWriteRefused checkCannedPaidWriteRefused printSimTrace,
      simCase simWidgetFixture "a crash while waiting for payment replays the reserved steps" scenarioCrashWhileWaiting checkCrashWhileWaiting traceCrashWhileWaiting,
      simCase simWidgetFixture "a crash mid-dispatch resumes the remaining ticks" scenarioCrashMidDispatch checkCrashMidDispatch traceCrashMidDispatch,
      simCase simWidgetFixture "the create op mints ids in order" scenarioTableCreate checkTableCreate printSimTrace,
      simCase simWidgetFixture "the reserve op never oversells under a race" scenarioTableReserveRace checkTableReserveRace traceTableReserveRace,
      simCase simWidgetFixture "the failing table's third call aborts whole" scenarioTableFailing checkTableFailing printSimTrace,
      simCase simWidgetFixture "the table status codes match the live assertions" scenarioTableStatusCodes checkTableStatusCodes printSimTrace,
      simCase simWidgetFixture "a lost acknowledgement replays the committed step instead of re-running it" scenarioLostAck checkLostAck printSimTrace,
      simCase simWidgetFixture "a killed checkout replays the reserved steps" scenarioKilledWhileWaiting checkCrashWhileWaiting printSimTrace,
      simCase simWidgetFixture "a killed dispatch resumes the remaining ticks" scenarioKilledMidDispatch checkCrashMidDispatch traceCrashMidDispatch
    ]
