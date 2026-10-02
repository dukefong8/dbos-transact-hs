{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | The two workflows the storefront runs (the Rust port's @workflows.rs@).
--
-- The checkout reserves inventory, publishes the id to pay against, then
-- parks on a durable @recv@ for the payment webhook — a wait that outlives
-- the process, because the deadline is a row rather than a timer in memory.
-- A paid checkout marks the order paid and starts the dispatch child
-- without awaiting it; a refused (or never-answered) checkout puts the
-- widget back on the shelf. Every application write is a transactional step
-- through "WidgetStore.Store": the write and its checkpoint share one
-- commit, which is what the Rust port's @store.rs@ documents as the gap it
-- cannot close.
module WidgetStore.Workflows
  ( -- * Topics and keys
    paymentStatusTopic,
    paymentIdEvent,
    orderIdEvent,
    paidStatus,
    paymentTimeout,
    dispatchTick,

    -- * Workflows
    checkoutWorkflow,
    dispatchWorkflow,
  )
where

import Control.Monad.Except (ExceptT (..), runExceptT)
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.Prelude
import DBOS.Transact (Ctx, DataSource, Duration, EngineOnly, Error, IsolationLevel (..), Topic (..), TransactionConfig (..), Tx (..), WorkflowRef, encodeWorkflowValue, millisDuration, recv, runTransaction, secondsDuration, setEvent, sleepWorkflowStep, startChildWorkflow, startOptionsDefault, workflowId)
import IHP.TypedSql.Id (Id' (..))
import WidgetStore.Store

-- * Topics and keys (workflows.rs)

-- | The topic the payment webhook sends its answer on.
paymentStatusTopic :: Text
paymentStatusTopic = "payment_status"

-- | The key the checkout publishes the id to pay against — the workflow's
-- own id.
paymentIdEvent :: Text
paymentIdEvent = "payment_id"

-- | The key the checkout publishes the order it created, once the outcome is
-- settled.
orderIdEvent :: Text
orderIdEvent = "order_id"

-- | The status string the webhook sends when the card was charged. Anything
-- else is a refusal.
paidStatus :: Text
paidStatus = "paid"

-- | How long a checkout waits to be told whether it was paid for. The
-- storefront's event wait uses the same deadline.
paymentTimeout :: Duration
paymentTimeout = secondsDuration 60

-- | How long each dispatch tick takes, so the progress bar has something to
-- show.
dispatchTick :: Duration
dispatchTick = millisDuration 1000

-- | A transactional step named the way the oracle names its function, so
-- the tracer and the checkpoint sequence read the same in every port. The
-- isolation matches the Python datasource's default (SERIALIZABLE): the
-- reserve-inventory guard leans on it, and the runner's retry loop is what
-- absorbs the serialization failures it can produce.
namedStep :: Text -> TransactionConfig
namedStep name = TransactionConfig {txName = Just name, txIsolation = Just Serializable}

-- * Transaction bodies (one commit each, with the step's checkpoint)

createOrderTx :: Tx IO -> IO Int
createOrderTx (Tx run) = do
  Id key <- run createOrderStatement ()
  pure key

reserveInventoryTx :: Tx IO -> IO Bool
reserveInventoryTx (Tx run) = (> 0) <$> run reserveInventoryStatement ()

undoReserveTx :: Tx IO -> IO ()
undoReserveTx (Tx run) = run undoReserveInventoryStatement ()

setOrderStatusTx :: Int -> Int -> Tx IO -> IO ()
setOrderStatusTx status orderId (Tx run) = run (setOrderStatusStatement status (Id orderId)) ()

-- | Tick one unit off the order's progress and mark it dispatched when it
-- runs out — both inside the step's transaction.
tickOrderTx :: Int -> Tx IO -> IO ()
tickOrderTx orderId (Tx run) = do
  remaining <- run (updateOrderProgressStatement (Id orderId)) ()
  case remaining of
    [0] -> run (setOrderStatusStatement orderStatusDispatched (Id orderId)) ()
    _   -> pure ()

-- * Workflows

-- | Buys a widget: reserve it, wait to be paid, then either dispatch it or
-- put it back. Started under the browser's idempotency key, so a
-- double-clicked Buy button joins the checkout already running rather than
-- reserving a second widget. The @ExceptT@ do-block is the oracle's @?@:
-- the first failed step ends the workflow.
checkoutWorkflow ::
  DataSource IO ->
  WorkflowRef IO EngineOnly ->
  () ->
  Ctx IO ->
  IO (Either (Error EngineOnly) ())
checkoutWorkflow ds dispatchRef () ctx = runExceptT $ do
  let wid = workflowId ctx
  orderId <- ExceptT (runTransaction ds ctx (namedStep "create_order") (\tx -> Right <$> createOrderTx tx))
  onShelf <- ExceptT (runTransaction ds ctx (namedStep "reserve_inventory") (\tx -> Right <$> reserveInventoryTx tx))
  if not onShelf
    then do
      ExceptT (runTransaction ds ctx (namedStep "cancel_order") (\tx -> Right <$> setOrderStatusTx orderStatusCancelled orderId tx))
      -- An empty payment id is how the storefront hears "no": it is waiting
      -- on this key, and leaving it unpublished would only make it wait out
      -- its own timeout for an answer that is already known.
      ExceptT (setEvent ctx paymentIdEvent ("" :: Text))
      pure ()
    else do
      ExceptT (setEvent ctx paymentIdEvent wid)
      ExceptT (recv ctx (Just (Topic paymentStatusTopic)) paymentTimeout) >>= \case
        Just status | status == paidStatus -> do
          ExceptT (runTransaction ds ctx (namedStep "mark_order_paid") (\tx -> Right <$> setOrderStatusTx orderStatusPaid orderId tx))
          -- A child workflow, started and not awaited: dispatching takes ten
          -- seconds and the buyer should not be kept waiting for it.
          _ <- ExceptT (startChildWorkflow ctx dispatchRef startOptionsDefault (Just (encodeWorkflowValue orderId)))
          ExceptT (setEvent ctx orderIdEvent (Text.pack (show orderId)))
        _ -> do
          -- Refused, or nobody answered before the deadline: the widget
          -- goes back on the shelf.
          ExceptT (runTransaction ds ctx (namedStep "undo_reserve_inventory") (\tx -> Right <$> undoReserveTx tx))
          ExceptT (runTransaction ds ctx (namedStep "cancel_order") (\tx -> Right <$> setOrderStatusTx orderStatusCancelled orderId tx))
          ExceptT (setEvent ctx orderIdEvent (Text.pack (show orderId)))

-- | Walks a paid order to the buyer, one tick a second, and marks it
-- dispatched at the end. @sleepWorkflowStep@ is durable, so the ticks
-- already taken stay taken and a restart picks up the count where it
-- stopped.
dispatchWorkflow ::
  DataSource IO ->
  Int ->
  Ctx IO ->
  IO (Either (Error EngineOnly) ())
dispatchWorkflow ds orderId ctx = go dispatchTicks
  where
    go :: Int -> IO (Either (Error EngineOnly) ())
    go 0 = pure (Right ())
    go n = do
      slept <- sleepWorkflowStep ctx dispatchTick
      case slept of
        Left err -> pure (Left err)
        Right () -> do
          ticked <- runTransaction ds ctx (namedStep "update_order_progress") (\tx -> Right <$> tickOrderTx orderId tx) :: IO (Either (Error EngineOnly) ())
          case ticked of
            Left err -> pure (Left err)
            Right () -> go (n - 1)
