{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | The outbox demo's workflows, ported from the Python transactional-outbox's
-- two patterns (@atomic_workflow.py@ and @transactional_enqueue.py@).
--
-- Variant A (@placeOrderWorkflow@) is the single-workflow outbox: one
-- @DBOS.workflow@ guarantees every order is inserted __and__ its
-- notification sent, atomically, despite failures. If the process crashes
-- after the insert but before the notification, recovery completes the
-- notification on restart.
--
-- Variant B (@sendNotificationWorkflow@) is the enqueued consumer of the
-- classic transactional outbox: the API handler inserts the order and calls
-- @dbos.enqueue_workflow@ in the __same__ transaction (see
-- 'Outbox.Store.enqueueNotificationStatement'), and this workflow sends the
-- notification, then marks it sent. It runs exactly once per committed order.
module Outbox.Workflows
  ( -- * Names and keys
    placeOrderWorkflowName,
    sendNotificationWorkflowName,
    notificationQueueName,
    orderIdEventKey,
    sentStatus,
    notifyDelay,
    NotifyEnvelope (..),

    -- * Bodies
    placeOrderWorkflow,
    sendNotificationWorkflow,
  )
where

import Control.Monad.Class.MonadTimer (threadDelay)
import Control.Monad.Except (ExceptT (..), runExceptT)
import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, (.:), (.=))
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.Transact (DataSource, EngineOnly, Error, IsolationLevel (..), StepCtx, TransactionConfig (..), Tx (..), WorkflowCtx, logInfo, runStep, runTxStep, setEvent)
import Outbox.Store (insertOrderStatement, setNotificationStatusStatement)
import IHP.TypedSql.Id (Id' (..))
import Prelude

-- * Names and keys (transactional_enqueue.py / atomic_workflow.py)

-- | The single-workflow outbox (@atomic_workflow.py@).
placeOrderWorkflowName :: Text
placeOrderWorkflowName = "place_order_workflow"

-- | The enqueued notification consumer (@transactional_enqueue.py@).
sendNotificationWorkflowName :: Text
sendNotificationWorkflowName = "send_notification_workflow"

-- | Workflows enqueued from the order transaction run on this queue.
notificationQueueName :: Text
notificationQueueName = "notification_queue"

-- | The key the atomic workflow publishes the created id under.
orderIdEventKey :: Text
orderIdEventKey = "order_id_event"

-- | The status an order carries once its notification went out.
sentStatus :: Text
sentStatus = "SENT"

-- | How long the simulated notification send takes (the Python
-- @time.sleep(3)@ standing in for an email/Kafka/webhook call).
notifyDelay :: Int
notifyDelay = 3000000

-- | A transactional step named the way the oracle names its function, so the
-- tracer and the checkpoint sequence read the same in every port.
namedStep :: Text -> TransactionConfig
namedStep name = TransactionConfig {txName = Just name, txIsolation = Just Serializable}

-- | An order as the workflows carry it: customer, item, quantity.
type OrderInput = (Text, Text, Int)

-- | A notification as the enqueued consumer carries it: order id, customer, item.
type NotifyInput = (Int, Text, Text)

-- | The input envelope @dbos.enqueue_workflow@ writes: positional arguments
-- under @positionalArgs@, named arguments under @namedArgs@. The Python
-- framework unwraps this before calling the workflow; the Haskell engine
-- reads the stored input as-is, so the consumer takes the envelope and
-- extracts the triple itself. @namedArgs@ is always empty here and ignored.
newtype NotifyEnvelope = NotifyEnvelope
  { notifyArgs :: NotifyInput
  }
  deriving stock (Eq, Show)

instance FromJSON NotifyEnvelope where
  parseJSON = withObject "NotifyEnvelope" $ \o -> NotifyEnvelope <$> o .: "positionalArgs"

instance ToJSON NotifyEnvelope where
  toJSON (NotifyEnvelope args) = object ["positionalArgs" .= args, "namedArgs" .= object []]

-- * Bodies

-- | Place an order and send its notification, atomically (the
-- @place_order_workflow@). The @ExceptT@ do-block is the oracle's @?@: the
-- first failed step ends the workflow.
placeOrderWorkflow ::
  forall exec.
  DataSource IO ->
  OrderInput ->
  WorkflowCtx exec IO ->
  IO (Either (Error EngineOnly) Int)
placeOrderWorkflow ds (customer, item, quantity) wctx = runExceptT $ do
  orderId <-
    ExceptT (runTxStep ds (namedStep "insert_order") wctx (\sctx (Tx run) -> Right <$> fromId <$> run (insertOrderStatement customer item quantity) ()))
  ExceptT (setEvent wctx orderIdEventKey orderId)
  ExceptT (runStep wctx "send_order_notification" (sendNotification orderId customer item))
  ExceptT (runTxStep ds (namedStep "update_notification_status") wctx (\sctx (Tx run) -> Right <$> run (setNotificationStatusStatement sentStatus (Id orderId)) ()))
  pure orderId
  where
    fromId :: Id' "orders" -> Int
    fromId (Id key) = key

-- | Send a notification for an order, then mark it sent (the
-- @send_notification_workflow@, transactionally enqueued alongside the
-- order). Because the enqueue and the insert shared one commit, this runs
-- exactly once per committed order.
sendNotificationWorkflow ::
  forall exec.
  DataSource IO ->
  NotifyEnvelope ->
  WorkflowCtx exec IO ->
  IO (Either (Error EngineOnly) ())
sendNotificationWorkflow ds (NotifyEnvelope (orderId, customer, item)) wctx = runExceptT $ do
  ExceptT (runStep wctx "send_order_notification" (sendNotification orderId customer item))
  ExceptT (runTxStep ds (namedStep "update_notification_status") wctx (\sctx (Tx run) -> Right <$> run (setNotificationStatusStatement sentStatus (Id orderId)) ()))

-- | Simulate sending an order confirmation (the Python
-- @send_order_notification@ step). In the classic pattern a background poller
-- would read the outbox and call this; here the workflow calls it directly
-- and the engine retries it until it succeeds.
sendNotification ::
  forall exec.
  Int ->
  Text ->
  Text ->
  StepCtx exec IO ->
  IO ()
sendNotification orderId customer item sctx = do
  logInfo sctx ("Sending notification for order " <> Text.pack (show orderId) <> ": " <> item <> " for " <> customer)
  threadDelay notifyDelay
  logInfo sctx ("Notification sent for order " <> Text.pack (show orderId) <> ": " <> item <> " for " <> customer)
