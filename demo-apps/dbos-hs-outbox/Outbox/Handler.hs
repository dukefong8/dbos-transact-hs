{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The outbox demo's handlers: each route's work happens here through
-- 'RouteHandler', returning a view model (the htmx surface renders it) or a
-- plain value (the JSON surface encodes it). The dispatch in "Outbox.Route"
-- picks the runner; the handlers never touch a 'Response'.
module Outbox.Handler
  ( getOutboxPage,
    getOrders,
    placeOrderAtomic,
    placeOrderEnqueued,
  )
where

import Control.Monad.Except (ExceptT (..), runExceptT)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson qualified as Aeson
import Data.ByteString.Lazy qualified as LBS
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding (encodeUtf8)
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.Transact (Duration, EngineOnly, Error, IsolationLevel (..), StartOptions (..), TransactionConfig (..), Tx (..), WorkflowId (..), encodeWorkflowValue, getWorkflowEvent, runTxOutside, secondsDuration, startWorkflow, startOptionsDefault, toDataSource)
import Demo.Http (RouteHandler, runAppOr500, throwRouteError)
import Hasql.Session qualified as Session
import IHP.TypedSql.Id (Id' (..))
import Network.HTTP.Types (status500)
import Outbox.App (OutboxApp (..), outboxAppName, outboxVersion)
import Outbox.Store (OutboxOrder, decodeOrderRow, enqueueNotificationStatement, insertOrderStatement, listOrdersStatement)
import Outbox.View (OutboxPage (..))
import Outbox.Workflows (orderIdEventKey, notificationQueueName, sendNotificationWorkflowName)
import Prelude

-- * Pages and reads

getOutboxPage :: OutboxApp -> RouteHandler OutboxPage
getOutboxPage app = OutboxPage <$> getOrders app

getOrders :: OutboxApp -> RouteHandler [OutboxOrder]
getOrders app = map decodeOrderRow <$> runAppOr500 app.obApp (Session.statement () listOrdersStatement)

-- * Writes

-- | Variant A: start the atomic workflow and answer with the refreshed list.
-- The workflow publishes the id right after its insert, so the wait is short:
-- the slow notification send happens after.
placeOrderAtomic :: OutboxApp -> Text -> Text -> Int -> RouteHandler [OutboxOrder]
placeOrderAtomic app customer item quantity = do
  started <- liftIO (startPlaceOrder app customer item quantity)
  case started of
    Left err -> throwRouteError status500 (textBody ("start failed: " <> Text.pack (show err)))
    Right wid -> do
      found <- liftIO (getWorkflowEvent app.obDbos wid orderIdEventKey orderEventTimeout)
      case found of
        Left err -> throwRouteError status500 (textBody ("event wait failed: " <> Text.pack (show err)))
        Right _ -> getOrders app

-- | Variant B: insert the order and transactionally enqueue its notification
-- workflow in one commit (the classic transactional outbox, without an
-- outbox table). Runs outside any workflow, through the app pool.
placeOrderEnqueued :: OutboxApp -> Text -> Text -> Int -> RouteHandler [OutboxOrder]
placeOrderEnqueued app customer item quantity = do
  inserted <- liftIO (runTxOutside (toDataSource app.obApp) insertConfig (\tx -> insertAndEnqueue tx customer item quantity))
  case inserted of
    Left err -> throwRouteError status500 (textBody ("transaction failed: " <> Text.pack (show err)))
    Right _ -> getOrders app

-- * Helpers

startPlaceOrder :: OutboxApp -> Text -> Text -> Int -> IO (Either (Error EngineOnly) WorkflowId)
startPlaceOrder app customer item quantity = runExceptT $ do
  wid <- liftIO (WorkflowId <$> UUID.toText <$> UUID.V4.nextRandom)
  let input = Just (encodeWorkflowValue (customer, item, quantity))
      options = startOptionsDefault {startWorkflowId = Just wid}
  _ <- ExceptT (startWorkflow app.obExec app.obPlaceOrder options input)
  pure wid

-- | One transaction: the order row and the enqueued notification workflow
-- commit (or roll back) together.
insertAndEnqueue :: Tx IO -> Text -> Text -> Int -> IO Int
insertAndEnqueue (Tx run) customer item quantity = do
  orderId <- fromId <$> run (insertOrderStatement customer item quantity) ()
  _ <- run (enqueueNotificationStatement outboxVersion outboxAppName sendNotificationWorkflowName notificationQueueName (Aeson.toJSON orderId) (Aeson.toJSON customer) (Aeson.toJSON item)) ()
  pure orderId
  where
    fromId :: Id' "orders" -> Int
    fromId (Id key) = key

insertConfig :: TransactionConfig
insertConfig = TransactionConfig {txName = Just "insert_order", txIsolation = Just Serializable}

-- | How long the atomic POST waits for the workflow's order-id event. The id
-- is published right after the insert, well before the 3s notification step.
orderEventTimeout :: Duration
orderEventTimeout = secondsDuration 30

textBody :: Text -> LBS.ByteString
textBody = LBS.fromStrict . encodeUtf8
