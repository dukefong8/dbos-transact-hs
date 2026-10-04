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
import Prelude
import DBOS.Transact (DataSource, Duration, EngineOnly, Error, IsolationLevel (..), Topic (..), TransactionConfig (..), Tx, WorkflowCtx, WorkflowRef, encodeWorkflowValue, millisDuration, recv, runTxStep, secondsDuration, setEvent, sleepStep, startChildWorkflow, startOptionsDefault, workflowId)
import WidgetStore.Steps (CheckoutSteps (..), DispatchSteps (..), OrderId (..))
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

-- * Workflows

-- | Buys a widget: reserve it, wait to be paid, then either dispatch it or
-- put it back. Started under the browser's idempotency key, so a
-- double-clicked Buy button joins the checkout already running rather than
-- reserving a second widget. The @ExceptT@ do-block is the oracle's @?@:
-- the first failed step ends the workflow.
checkoutWorkflow ::
  forall exec.
  DataSource IO ->
  (Tx IO -> CheckoutSteps exec IO) ->
  WorkflowRef IO EngineOnly ->
  WorkflowCtx exec IO ->
  IO (Either (Error EngineOnly) ())
checkoutWorkflow ds mkCheckout dispatchRef wctx = runExceptT $ do
  let wid = workflowId wctx
  -- The checkpoint payload stays a plain Int; the OrderId boundary is the
  -- steps table, unwrapped at the transaction edge (as in WidgetTest).
  orderId <- ExceptT (runTxStep ds (namedStep "create_order") wctx (\sctx tx -> Right . (\(OrderId oid) -> oid) <$> (mkCheckout tx).coCreate sctx))
  onShelf <- ExceptT (runTxStep ds (namedStep "reserve_inventory") wctx (\sctx tx -> Right <$> (mkCheckout tx).coReserve sctx))
  if not onShelf
    then do
      ExceptT (runTxStep ds (namedStep "cancel_order") wctx (\sctx tx -> Right <$> (mkCheckout tx).coSetStatus sctx (OrderId orderId) orderStatusCancelled))
      -- An empty payment id is how the storefront hears "no": it is waiting
      -- on this key, and leaving it unpublished would only make it wait out
      -- its own timeout for an answer that is already known.
      ExceptT (setEvent wctx paymentIdEvent ("" :: Text))
      pure ()
    else do
      ExceptT (setEvent wctx paymentIdEvent wid)
      ExceptT (recv wctx (Just (Topic paymentStatusTopic)) paymentTimeout) >>= \case
        Just paid | paid == paidStatus -> do
          ExceptT (runTxStep ds (namedStep "mark_order_paid") wctx (\sctx tx -> Right <$> (mkCheckout tx).coSetStatus sctx (OrderId orderId) orderStatusPaid))
          -- A child workflow, started and not awaited: dispatching takes ten
          -- seconds and the buyer should not be kept waiting for it.
          _ <- ExceptT (startChildWorkflow wctx dispatchRef startOptionsDefault (Just (encodeWorkflowValue orderId)))
          ExceptT (setEvent wctx orderIdEvent (Text.pack (show orderId)))
        _ -> do
          -- Refused, or nobody answered before the deadline: the widget
          -- goes back on the shelf.
          ExceptT (runTxStep ds (namedStep "undo_reserve_inventory") wctx (\sctx tx -> Right <$> (mkCheckout tx).coUndo sctx))
          ExceptT (runTxStep ds (namedStep "cancel_order") wctx (\sctx tx -> Right <$> (mkCheckout tx).coSetStatus sctx (OrderId orderId) orderStatusCancelled))
          ExceptT (setEvent wctx orderIdEvent (Text.pack (show orderId)))

-- | Walks a paid order to the buyer, one tick a second, and marks it
-- dispatched at the end. @sleepStep@ is durable, so the ticks
-- already taken stay taken and a restart picks up the count where it
-- stopped.
dispatchWorkflow ::
  forall exec.
  DataSource IO ->
  (Tx IO -> DispatchSteps exec IO) ->
  Int ->
  WorkflowCtx exec IO ->
  IO (Either (Error EngineOnly) ())
dispatchWorkflow ds mkDispatch orderId wctx = runExceptT (go dispatchTicks)
  where
    go :: Int -> ExceptT (Error EngineOnly) IO ()
    go 0 = pure ()
    go n = do
      ExceptT (sleepStep wctx dispatchTick)
      ExceptT (runTxStep ds (namedStep "update_order_progress") wctx (\sctx tx -> Right <$> (mkDispatch tx).doTick sctx (OrderId orderId)))
      go (n - 1)
