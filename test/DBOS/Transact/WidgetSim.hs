{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The widget store's checkout and dispatch composed over the
-- transactional-step engine — the port's own end-to-end seam. Mirrors the
-- Python widget-store oracle (probe 2026-10-02): a paid order walks
-- @PENDING(0) -> PAID(2) -> DISPATCHED(1)@ with inventory @5 -> 4@; a
-- refused payment walks @PENDING(0) -> CANCELLED(-1)@ with inventory
-- restored. Application tables are TVars behind a fake 'DataSource', so
-- the cases run under IOSim; the engine functions are the real ones
-- ('runTransaction', 'setEvent', 'recv', 'sleepWorkflowStep',
-- 'startChildWorkflow', 'runDBOSWorkflow', 'sendWorkflowMessage',
-- 'getWorkflowEvent').
module DBOS.Transact.WidgetSim (tests) where

import DBOS.Prelude
import Control.Monad.Except (ExceptT (..), runExceptT)
import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM, StrictTVar, atomically, modifyTVar, newTVarIO, readTVar, readTVarIO, writeTVar)
import Control.Monad.IOSim (IOSim)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.IOSimTracer (printSimTrace, runSimCase, simTracer)
import DBOS.SystemDB (BackendErrorKind (..))
import DBOS.SystemDB.IOSim (memLaunchOn, newMemDB, simConnectionWith, simInstance)
import DBOS.Transact
  ( BackendError (..),
    CodecError,
    Ctx,
    WorkflowCtx,
    workflowCtxInner,
    DataSource (..),
    EngineOnly,
    Executor,
    Error (..),
    Identity (..),
    StepCtx,
    RecordedOutcome (..),
    SerializedWorkflowValue (..),
    Topic (..),
    TransactionConfig (..),
    Tx (..),
    WorkflowId (..),
    WorkflowRef,
    decodeWorkflowValue,
    encodeWorkflowValue,
    firstStepStatus,
    getWorkflowEvent,
    millisDuration,
    newWorkflowKey,
    nextWorkflowMarker,
    recv,
    registerDBOSWorkflowRefScoped,
    runDBOSWorkflow,
    runTransaction,
    sendWorkflowMessage,
    setEvent,
    sleepWorkflowStep,
    startChildWorkflow,
    startDBOSWorkflowRef,
    startOptionsDefault,
    StartOptions (..),
    withStep,
    withWorkflow,
    recvScoped,
    runTransactionScoped,
    setEventScoped,
    sleepWorkflowStepScoped,
    startChildWorkflowScoped,
  )
import Test.Tasty (DependencyType (..), TestTree, dependentTestGroup)
import Test.Tasty.HUnit (testCase, (@?=))

-- * Step tables (A3)

-- | The checkout's transactional capabilities: every field is one step and
-- one 'atomically' block, mirroring the Postgres handler's one transaction
-- per step. The 'StepCtx' parameter is the capability that scopes each
-- call to a step — implementations do not read it.
newtype OrderId = OrderId Int
  deriving stock (Eq, Show)

data CheckoutOps exec m = CheckoutOps
  { coCreate :: StepCtx exec m -> m OrderId,
    coReserve :: StepCtx exec m -> m Bool,
    coUndo :: StepCtx exec m -> m (),
    coSetStatus :: StepCtx exec m -> OrderId -> Int -> m ()
  }

-- | The dispatch's capabilities: the tick loop's decrement and the status
-- writes. 'coSetStatus' is repeated from the checkout table per the
-- share-by-two rule (records are cheap; embed at three).
data DispatchOps exec m = DispatchOps
  { doTick :: StepCtx exec m -> OrderId -> m (),
    doSetStatus :: StepCtx exec m -> OrderId -> Int -> m ()
  }

-- | The STM handler: one atomic block per op. The split read-then-write of
-- the old fakes is closed — concurrent checkouts cannot mint one id or
-- oversell one unit.
stmCheckoutOps :: WidgetStore (IOSim s) -> CheckoutOps exec (IOSim s)
stmCheckoutOps store =
  CheckoutOps
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

stmDispatchOps :: WidgetStore (IOSim s) -> DispatchOps exec (IOSim s)
stmDispatchOps store =
  DispatchOps
    { doTick = \_ (OrderId oid) -> atomically $
        modifyTVar store.wsOrders $
          Map.adjust
            (\(status, progress) -> if progress <= 1 then (1, 0) else (status, progress - 1))
            oid,
      doSetStatus = \_ (OrderId oid) status -> atomically $
        modifyTVar store.wsOrders (Map.adjust (\(_, progress) -> (status, progress)) oid)
    }

-- | The deterministic seed-8 shape: the table counts its calls and the
-- third (the paid write) aborts via 'throwSTM', so the whole block —
-- including anything it wrote — vanishes. Replaces the old
-- 'BackendError'-injection fake with a domain-level refusal.
failingCheckoutOps :: StrictTVar (IOSim s) Int -> WidgetStore (IOSim s) -> CheckoutOps exec (IOSim s)
failingCheckoutOps calls store =
  CheckoutOps
    { coCreate = \s -> do
        _ <- atomically (modifyTVar calls (+ 1))
        (stmCheckoutOps store).coCreate s,
      coReserve = \s -> do
        _ <- atomically (modifyTVar calls (+ 1))
        (stmCheckoutOps store).coReserve s,
      coUndo = (stmCheckoutOps store).coUndo,
      coSetStatus = \s oid status -> do
        n <- atomically (modifyTVar calls (+ 1) >> readTVar calls)
        if n == 3
          then atomically (throwSTM (userError "mark_order_paid refused"))
          else (stmCheckoutOps store).coSetStatus s oid status
    }

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
-- step view, exactly the shape the workflows will use.
withTableStep :: WidgetStore (IOSim s) -> (forall exec. StepCtx exec (IOSim s) -> IOSim s a) -> IOSim s a
withTableStep _ body = do
  conn <- simConnectionWith simTracer
  withWorkflow conn tableIdentity (WorkflowId "widget-tables") Nothing $ \wctx -> do
    marker <- nextWorkflowMarker wctx
    withStep wctx marker (firstStepStatus 0) body

scenarioTableCreate :: IOSim s ([Int], Map Int (Int, Int))
scenarioTableCreate = do
  store <- newWidgetStore 5
  -- The table is built inside the scope's continuation so its phantom
  -- execution unifies with the scope it is spent in.
  ids <- withTableStep store $ \s -> do
    let ops = stmCheckoutOps store
    mapM (\_ -> ops.coCreate s) [1 :: Int, 2, 3]
  orders <- readTVarIO store.wsOrders
  pure ([oid | OrderId oid <- ids], orders)

scenarioTableReserveRace :: IOSim s (Int, Int)
scenarioTableReserveRace = do
  store <- newWidgetStore 1
  first <- async (withTableStep store (\s -> (stmCheckoutOps store).coReserve s))
  second <- async (withTableStep store (\s -> (stmCheckoutOps store).coReserve s))
  a <- wait first
  b <- wait second
  stock <- readTVarIO store.wsInventory
  pure (length (filter id [a, b]), stock)

scenarioTableFailing :: IOSim s (Bool, Map Int (Int, Int), Int)
scenarioTableFailing = do
  calls <- newTVarIO 0
  store <- newWidgetStore 5
  threw <-
    withTableStep store $ \s -> do
      let ops = failingCheckoutOps calls store
      oid <- ops.coCreate s
      reserved <- ops.coReserve s
      if not reserved
        then pure False
        else do
          result <- try (ops.coSetStatus s oid 2)
          pure (either (const True) (const False) (result :: Either SomeException ()))
  orders <- readTVarIO store.wsOrders
  stock <- readTVarIO store.wsInventory
  pure (threw, orders, stock)

scenarioTableStatusCodes :: IOSim s (Int, Int)
scenarioTableStatusCodes = do
  store <- newWidgetStore 5
  withTableStep store $ \s -> do
    let checkout = stmCheckoutOps store
        dispatch = stmDispatchOps store
    oid <- checkout.coCreate s
    reserved <- checkout.coReserve s
    if not reserved
      then pure ()
      else do
        checkout.coSetStatus s oid 2
        mapM_ (\_ -> dispatch.doTick s oid) [1 :: Int, 2, 3]
  orders <- readTVarIO store.wsOrders
  pure (Map.findWithDefault (0, 0) 1 orders)

-- * Application tables (the fake's state)

-- | The storefront's tables: inventory, orders (id -> status, progress),
-- and the order-id sequence.
data WidgetStore m = WidgetStore
  { wsInventory :: StrictTVar m Int,
    wsOrders :: StrictTVar m (Map Int (Int, Int)),
    wsNextOrder :: StrictTVar m Int
  }

newWidgetStore :: MonadSTM m => Int -> m (WidgetStore m)
newWidgetStore stock =
  WidgetStore <$> newTVarIO stock <*> newTVarIO Map.empty <*> newTVarIO 1

-- | The app datasource: every op runs in a transaction; the fake's
-- 'atomically' block is the shared commit. Checkpoint writes are no-ops —
-- the engine's step ids still advance, which is what replay relies on.
widgetConfig :: TransactionConfig
widgetConfig = TransactionConfig {txName = Just "widget_step", txIsolation = Nothing}

mkWidgetDs :: Monad m => DataSource m
mkWidgetDs =
  DataSource
    { dsName = "widget-db",
      dsSchema = "dbos",
      dsCheck = \_ _ _ -> pure (Right Nothing),
      dsWithTransaction = \_ action -> Right <$> action (Tx (\_ _ -> error "widget fake: statements unsupported")),
      dsRecordOutput = \_ _ _ _ _ -> pure True,
      dsRecordError = \_ _ _ _ _ -> pure True,
      dsStepName = \_ _ -> pure (Right Nothing),
      dsDeleteCheckpoints = \_ _ -> pure (Right ())
    }

-- | A datasource whose third transaction fails: the checkout's paid write
-- cannot commit. The checkout must stop with the failure instead of walking
-- on to the dispatch child (the seed-8 fuzz finding).
failingWidgetDs :: MonadSTM m => StrictTVar m Int -> DataSource m
failingWidgetDs calls =
  mkWidgetDs
    { dsWithTransaction = \_ action -> do
        n <- atomically (modifyTVar calls (+ 1) >> readTVar calls)
        if n == 3
          then
            pure
              ( Left
                  BackendError
                    { backendMessage = "mark_order_paid refused",
                      backendSqlState = Nothing,
                      backendKind = Permanent
                    }
              )
          else Right <$> action (Tx (\_ _ -> error "widget fake: statements unsupported"))
    }

-- * The workflows

-- | One app transaction at the engine's engine-only channel.
widgetStep :: DataSource (IOSim s) -> WorkflowCtx exec (IOSim s) -> (Tx (IOSim s) -> IOSim s ()) -> IOSim s (Either (Error EngineOnly) ())
widgetStep ds wctx action = runTransactionScoped ds wctx widgetConfig (\tx -> Right <$> action tx)

-- | The checkout workflow: create, reserve, publish the payment id, wait
-- for the payment, then dispatch or compensate. Mirrors the oracle's
-- @checkout_workflow@ (create before reserve; the payment id event is the
-- workflow's own id in the oracle — here the order id names the order).
checkoutBody :: forall s exec. DataSource (IOSim s) -> WidgetStore (IOSim s) -> WorkflowRef (IOSim s) EngineOnly -> () -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Text)
checkoutBody ds store dispatchRef () wctx = runExceptT $ do
  orderId <- ExceptT (runTransactionScoped ds wctx widgetConfig (\tx -> Right <$> createOrder store tx))
  onShelf <- ExceptT (runTransactionScoped ds wctx widgetConfig (\tx -> Right <$> reserveInventory store tx))
  if not onShelf
    then do
      _ <- ExceptT (widgetStep ds wctx (\tx -> setStatus store orderId (-1) tx))
      _ <- ExceptT (setEventScoped wctx "payment_id" (Nothing :: Maybe Text))
      pure "no-inventory"
    else do
      _ <- ExceptT (setEventScoped wctx "payment_id" (Just (Text.pack (show orderId))))
      ExceptT (recvScoped wctx (Just (Topic "payment_status")) (millisDuration 5000) :: IOSim s (Either (Error EngineOnly) (Maybe Text))) >>= \case
        Just status | status == "paid" -> do
          _ <- ExceptT (widgetStep ds wctx (\tx -> setStatus store orderId 2 tx))
          _ <- ExceptT (startChildWorkflowScoped wctx dispatchRef startOptionsDefault (Just (encodeWorkflowValue orderId)))
          _ <- ExceptT (setEventScoped wctx "order_id" (Text.pack (show orderId)))
          pure "paid"
        _ -> do
          _ <- ExceptT (widgetStep ds wctx (\tx -> undoReserve store tx))
          _ <- ExceptT (widgetStep ds wctx (\tx -> setStatus store orderId (-1) tx))
          _ <- ExceptT (setEventScoped wctx "order_id" (Text.pack (show orderId)))
          pure "cancelled"

-- | The dispatch workflow: three ticks 50ms apart (the oracle runs ten
-- one-second ticks; shortened so the simulation stays fast).
dispatchBody :: forall s exec. DataSource (IOSim s) -> WidgetStore (IOSim s) -> Int -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Text)
dispatchBody ds store orderId wctx = go (3 :: Int)
  where
    go 0 = pure (Right "dispatched")
    go n = do
      slept <- sleepWorkflowStepScoped wctx (millisDuration 50)
      case slept of
        Left err -> pure (Left err)
        Right () -> do
          _ <- widgetStep ds wctx (\tx -> tickOrder store orderId tx)
          go (n - 1)

-- * App operations

createOrder :: MonadSTM m => WidgetStore m -> Tx m -> m Int
createOrder store _ = do
  orderId <- readTVarIO store.wsNextOrder
  atomically $ do
    writeTVar store.wsNextOrder (orderId + 1)
    modifyTVar store.wsOrders (Map.insert orderId (0, 3))
  pure orderId

reserveInventory :: MonadSTM m => WidgetStore m -> Tx m -> m Bool
reserveInventory store _ = do
  stock <- readTVarIO store.wsInventory
  if stock > 0
    then atomically (writeTVar store.wsInventory (stock - 1)) >> pure True
    else pure False

undoReserve :: MonadSTM m => WidgetStore m -> Tx m -> m ()
undoReserve store _ = atomically (modifyTVar store.wsInventory (+ 1))

setStatus :: MonadSTM m => WidgetStore m -> Int -> Int -> Tx m -> m ()
setStatus store orderId status _ =
  atomically (modifyTVar store.wsOrders (Map.adjust (\(_, progress) -> (status, progress)) orderId))

tickOrder :: MonadSTM m => WidgetStore m -> Int -> Tx m -> m ()
tickOrder store orderId _ =
  atomically $
    modifyTVar store.wsOrders $
      Map.adjust
        ( \(status, progress) ->
            if progress <= 1 then (1, 0) else (status, progress - 1)
        )
        orderId

-- * The composition scenario

-- | One checkout to completion: start it under a fixed id, answer the
-- payment on the topic the workflow waits on, await the order id, and
-- report the final store state. The started checkout is left to run; the
-- event wait is what tells us it finished publishing.
scenarioCheckout :: Maybe Text -> IOSim s ((Int, Maybe Text), Map Int (Int, Int), Int)
scenarioCheckout payment = do
  mem <- newMemDB
  dbos <- simInstance
  store <- newWidgetStore 5
  dispatchRef <- either (error . show) id <$> registerDBOSWorkflowRefScoped dbos (newWorkflowKey "DispatchOrderWorkflow") (dispatchBody mkWidgetDs store)
  checkoutRef <- either (error . show) id <$> registerDBOSWorkflowRefScoped dbos (newWorkflowKey "CheckoutWorkflow") (checkoutBody mkWidgetDs store dispatchRef)
  exec <- memLaunchOn mem simTracer dbos
  let wid = WorkflowId "widget-wf-1"
  _ <- startDBOSWorkflowRef exec checkoutRef (startOptionsDefault {startWorkflowId = Just "widget-wf-1"}) Nothing
  paymentId <- getWorkflowEvent dbos wid "payment_id" (millisDuration 1000)
  case payment of
    Just decision -> do
      _ <- sendWorkflowMessage dbos wid (Just (Topic "payment_status")) Nothing (encodeWorkflowValue decision)
      pure ()
    Nothing -> pure ()
  orderIdResult <- getWorkflowEvent dbos wid "order_id" (millisDuration 2000)
  -- Let the spawned dispatch workflow finish: sleep advances simulated
  -- time, and the child runs while this thread is parked.
  mapM_ (\_ -> threadDelay 100000) [1 .. 30 :: Int]
  inventory <- readTVarIO store.wsInventory
  orders <- readTVarIO store.wsOrders
  let orderCount = case orderIdResult of
        Left _ -> 0
        Right Nothing -> 0
        Right (Just _) -> Map.size orders
      paymentText = case paymentId of
        Left _ -> Nothing
        Right Nothing -> Nothing
        Right (Just stored) -> case decodeWorkflowValue "payment_id" (Just stored) :: Either CodecError Text of
          Left _ -> Nothing
          Right text -> Just text
  pure ((inventory, paymentText), orders, orderCount)

-- | The seed-8 shape at the sim seam: the paid write cannot commit, so the
-- checkout must stop before the child starts and before @order_id@ is
-- published. A swallow regression walks on and dispatches the order anyway.
scenarioPaidStepFails :: IOSim s (Bool, Map Int (Int, Int), Int)
scenarioPaidStepFails = do
  mem <- newMemDB
  dbos <- simInstance
  store <- newWidgetStore 5
  calls <- newTVarIO 0
  dispatchRef <- either (error . show) id <$> registerDBOSWorkflowRefScoped dbos (newWorkflowKey "DispatchOrderWorkflow") (dispatchBody mkWidgetDs store)
  checkoutRef <- either (error . show) id <$> registerDBOSWorkflowRefScoped dbos (newWorkflowKey "CheckoutWorkflow") (checkoutBody (failingWidgetDs calls) store dispatchRef)
  exec <- memLaunchOn mem simTracer dbos
  let wid = WorkflowId "widget-wf-fail"
  _ <- startDBOSWorkflowRef exec checkoutRef (startOptionsDefault {startWorkflowId = Just "widget-wf-fail"}) Nothing
  paymentId <- getWorkflowEvent dbos wid "payment_id" (millisDuration 1000)
  case paymentId of
    Right (Just _) -> do
      _ <- sendWorkflowMessage dbos wid (Just (Topic "payment_status")) Nothing (encodeWorkflowValue ("paid" :: Text))
      pure ()
    _ -> pure ()
  -- Give a swallow regression time to publish @order_id@ and dispatch.
  mapM_ (\_ -> threadDelay 100000) [1 .. 30 :: Int]
  orderIdResult <- getWorkflowEvent dbos wid "order_id" (millisDuration 0)
  inventory <- readTVarIO store.wsInventory
  orders <- readTVarIO store.wsOrders
  let published = case orderIdResult of
        Right (Just _) -> True
        _ -> False
  pure (published, orders, inventory)

tests :: TestTree
tests =
  dependentTestGroup
    "Widget composition (IOSim)"
    AllFinish
    [ testCase "a paid checkout dispatches the order and keeps inventory down" $ do
        (res, tr) <- runSimCase (scenarioCheckout (Just "paid"))
        printSimTrace tr
        res @?= ((4, Just "1"), Map.singleton 1 (1, 0), 1),
      testCase "a refused payment restores inventory and cancels the order" $ do
        (res, tr) <- runSimCase (scenarioCheckout (Just "failed"))
        printSimTrace tr
        res @?= ((5, Just "1"), Map.singleton 1 (-1, 3), 1),
      testCase "a failed paid write stops the checkout instead of dispatching" $ do
        (res, tr) <- runSimCase scenarioPaidStepFails
        printSimTrace tr
        res @?= (False, Map.singleton 1 (0, 3), 4),
      -- * Step tables (A3): direct handler cases over the same store, no
      -- engine — the table ops are what the workflows will spend once the
      -- rewire lands. See docs/widget-step-tables.md.
      testCase "the create op mints ids in order" $ do
        ((ids, orders), tr) <- runSimCase scenarioTableCreate
        printSimTrace tr
        ids @?= [1, 2, 3]
        Map.keys orders @?= [1, 2, 3],
      testCase "the reserve op never oversells under a race" $ do
        ((winners, stock), tr) <- runSimCase scenarioTableReserveRace
        printSimTrace tr
        winners @?= 1
        stock @?= 0,
      testCase "the failing table's third call aborts whole" $ do
        ((threw, orders, stock), tr) <- runSimCase scenarioTableFailing
        printSimTrace tr
        threw @?= True
        orders @?= Map.singleton 1 (0, 3)
        stock @?= 4,
      testCase "the table status codes match the live assertions" $ do
        (order, tr) <- runSimCase scenarioTableStatusCodes
        printSimTrace tr
        order @?= (1, 0)
    ]
