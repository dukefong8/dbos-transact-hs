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
import Control.Monad.Except (ExceptT (..), liftEither, runExcept, runExceptT, throwError)
import Control.Monad.IO.Class (liftIO)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Prelude
import DBOS.Transact (EngineOnly, Error, StartOptions (..), Topic (..), WorkflowId (..), decodeWorkflowValue, encodeWorkflowValue, getWorkflowEvent, sendWorkflowMessage, startWorkflow, startOptionsDefault)
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
startCheckout app key = runExceptT $ do
  ExceptT (either (Left . Text.pack . show) Right <$> liftIO (startCheckoutWorkflow app key))
  paymentId <- ExceptT (awaitEvent app (WorkflowId key) paymentIdEvent)
  if Text.null paymentId then throwError "Checkout failed" else pure paymentId

-- | The payment provider's callback: tells the waiting checkout whether the
-- card was charged, then waits for it to settle the order.
settlePayment :: WidgetApp -> Text -> Text -> RouteHandler (Either Text Order)
settlePayment app paymentId status = runExceptT $ do
  ExceptT (either (Left . Text.pack . show) Right <$> liftIO (sendWorkflowMessage app.waDbos (WorkflowId paymentId) (Just (Topic paymentStatusTopic)) Nothing (encodeWorkflowValue status)))
  oid <- ExceptT (awaitEvent app (WorkflowId paymentId) orderIdEvent)
  if Text.null oid then throwError "Checkout failed" else pure ()
  rawId <- maybe (throwError "Checkout failed") pure (readMaybe (Text.unpack oid))
  -- The one action that is not IO: the route monad owns the 500 mapping, so
  -- it stays the only place the transformer is named.
  ExceptT (maybe (Left "Checkout failed") (Right . decodeOrderRow) <$> runAppOr500 app.waApp (Session.statement () (orderStatement (Id rawId))))

-- * Helpers

-- | Pinned to 'EngineOnly' so the start failure channel is the engine's, not
-- an ambiguous phantom. The handle is dropped: the workflow goes on to wait
-- for a payment, and no HTTP request should be held open for that.
startCheckoutWorkflow :: WidgetApp -> Text -> IO (Either (Error EngineOnly) ())
startCheckoutWorkflow app key = do
  started <- startWorkflow app.waExec app.waCheckout (startOptionsDefault {startWorkflowId = Just (WorkflowId key)}) Nothing
  pure (void started)

-- | Wait for one published event, decoded to 'Text'. The same deadline the
-- workflow gives the payment, because both ends are waiting on the same
-- exchange.
awaitEvent :: WidgetApp -> WorkflowId -> Text -> RouteHandler (Either Text Text)
awaitEvent app workflow key = do
  found <- liftIO (getWorkflowEvent app.waDbos workflow key paymentTimeout)
  pure (runExcept $ do
    stored <- liftEither (either (Left . Text.pack . show) Right found)
    value <- maybe (throwError ("event " <> key <> " was never published")) pure stored
    liftEither (either (Left . Text.pack . show) Right (decodeWorkflowValue key (Just value))))

freshIdempotencyKey :: IO Text
freshIdempotencyKey = UUID.toText <$> UUID.V4.nextRandom
