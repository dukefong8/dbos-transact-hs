{-# LANGUAGE OverloadedStrings #-}

-- | The widget store's handlers, mirroring the Playground/hs Todo app's
-- @Todo.Handler@: each returns a view model through the 'RouteHandler' monad
-- (or a plain value the JSON surface encodes), the dispatch in
-- "WidgetStore.Route" chooses the runner. The engine calls that wait on
-- workflows live here; the dispatch never touches the database directly.
module WidgetStore.Handler
  ( getStorePage,
    getStorePanel,
    getProduct,
    getOrders,
    getOrder,
    restockProduct,
    startCheckout,
    settlePayment,
  )
where

import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Prelude
import DBOS.Transact (EngineOnly, Error, StartOptions (..), Topic (..), WorkflowId (..), decodeWorkflowValue, encodeWorkflowValue, getWorkflowEvent, sendWorkflowMessage, startDBOSWorkflowRef, startOptionsDefault)
import Demo.Http (RouteHandler, runAppOr500, throwRouteError)
import Hasql.Session qualified as Session
import IHP.TypedSql.Id (Id' (..))
import Network.HTTP.Types (status404, status500)
import Text.Read (readMaybe)
import WidgetStore.App (WidgetApp (..))
import WidgetStore.Store
import WidgetStore.View (StorePage (..), StorePanel (..))
import WidgetStore.Workflows (orderIdEvent, paymentIdEvent, paymentStatusTopic, paymentTimeout)

-- * Pages and panels

getStorePage :: WidgetApp -> RouteHandler StorePage
getStorePage app = do
  loadedProduct <- getProduct app
  orders <- getOrders app
  key <- liftIO freshIdempotencyKey
  pure StorePage {storePageKey = key, storePageProduct = loadedProduct, storePageOrders = orders}

getStorePanel :: WidgetApp -> RouteHandler StorePanel
getStorePanel app = do
  loadedProduct <- getProduct app
  key <- liftIO freshIdempotencyKey
  pure StorePanel {storePanelKey = key, storePanelProduct = loadedProduct}

-- * Reads

getProduct :: WidgetApp -> RouteHandler Product
getProduct app = do
  found <- runAppOr500 app.waApp (Session.statement () productStatement)
  case found of
    Nothing  -> throwRouteError status500 "product not found"
    Just row -> pure (decodeProductRow row)

getOrders :: WidgetApp -> RouteHandler [Order]
getOrders app = map decodeOrderRow <$> runAppOr500 app.waApp (Session.statement () ordersStatement)

getOrder :: WidgetApp -> Int -> RouteHandler Order
getOrder app rawId = do
  found <- runAppOr500 app.waApp (Session.statement () (orderStatement (Id rawId)))
  case found of
    Nothing  -> throwRouteError status404 "Order not found"
    Just row -> pure (decodeOrderRow row)

-- * Writes

restockProduct :: WidgetApp -> RouteHandler ()
restockProduct app = runAppOr500 app.waApp (Session.statement () restockStatement)

-- * Workflow-facing handlers

-- | Starts a checkout and answers with the id to pay against. The idempotency
-- key is the workflow id, which is what makes a double-clicked Buy button
-- harmless: the second POST joins the checkout already running instead of
-- starting another. A 'Left' is the storefront's "checkout failed" (no
-- inventory); the API surface spells it 500, the htmx surface a retry panel.
startCheckout :: WidgetApp -> Text -> RouteHandler (Either Text Text)
startCheckout app key = do
  started <- liftIO (startCheckoutWorkflow app key)
  case started of
    Left message -> pure (Left (Text.pack (show message)))
    Right () -> do
      paymentId <- awaitEvent app (WorkflowId key) paymentIdEvent
      pure $ case paymentId of
        Right pid | not (Text.null pid) -> Right pid
        _                               -> Left "Checkout failed"

-- | The payment provider's callback: tells the waiting checkout whether the
-- card was charged, then waits for it to settle the order.
settlePayment :: WidgetApp -> Text -> Text -> RouteHandler (Either Text Order)
settlePayment app paymentId status = do
  sent <- liftIO (sendWorkflowMessage app.waDbos (WorkflowId paymentId) (Just (Topic paymentStatusTopic)) Nothing (encodeWorkflowValue status))
  case sent of
    Left err -> pure (Left (Text.pack (show err)))
    Right () -> do
      orderId <- awaitEvent app (WorkflowId paymentId) orderIdEvent
      case orderId of
        Right oid | not (Text.null oid) -> do
          case readMaybe (Text.unpack oid) of
            Nothing -> pure (Left "Checkout failed")
            Just rawId -> do
              found <- runAppOr500 app.waApp (Session.statement () (orderStatement (Id rawId)))
              pure $ case found of
                Nothing  -> Left "Checkout failed"
                Just row -> Right (decodeOrderRow row)
        _ -> pure (Left "Checkout failed")

-- * Helpers

-- | Pinned to 'EngineOnly' so the start failure channel is the engine's, not
-- an ambiguous phantom. The handle is dropped: the workflow goes on to wait
-- for a payment, and no HTTP request should be held open for that.
startCheckoutWorkflow :: WidgetApp -> Text -> IO (Either (Error EngineOnly) ())
startCheckoutWorkflow app key = do
  started <- startDBOSWorkflowRef app.waExec app.waCheckout (startOptionsDefault {startWorkflowId = Just key}) Nothing
  pure (void started)

-- | Wait for one published event, decoded to 'Text'. The same deadline the
-- workflow gives the payment, because both ends are waiting on the same
-- exchange.
awaitEvent :: WidgetApp -> WorkflowId -> Text -> RouteHandler (Either Text Text)
awaitEvent app workflow key = do
  found <- liftIO (getWorkflowEvent app.waDbos workflow key paymentTimeout)
  pure $ case found of
    Left err -> Left (Text.pack (show err))
    Right Nothing -> Left ("event " <> key <> " was never published")
    Right (Just value) -> case decodeWorkflowValue key (Just value) of
      Left err      -> Left (Text.pack (show err))
      Right decoded -> Right decoded

freshIdempotencyKey :: IO Text
freshIdempotencyKey = UUID.toText <$> UUID.V4.nextRandom
