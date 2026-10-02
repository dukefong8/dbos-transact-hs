{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}

-- | The widget store's routes, mirroring the Playground/hs Todo app's
-- @Todo.Route@: an ihp-router trie, one dispatch case per route, and the
-- 'RouteHandler' runners from "Demo.Http" that turn a handler result
-- into a response. The handlers themselves live in "WidgetStore.Handler".
--
-- Content negotiation is the one addition: a request carrying htmx's
-- @HX-Request@ header gets the rendered fragment, everyone else the Rust
-- port's JSON/text surface (the same URLs).
module WidgetStore.Route
  ( WidgetRoute (..),
    dispatchWidget,
    widgetRouteTrie,
    widgetNotFound,
  )
where

import DBOS.Prelude
import Data.Text (Text)
import IHP.Router.WAI (HasPath (..), UrlCapture (..), routes)
import Network.HTTP.Types (StdMethod (..), status200, status404, status500)
import Network.Wai (Application, Response, responseLBS)
import System.Exit (ExitCode (..))
import System.Posix.Process (exitImmediately)
import WidgetStore.App (WidgetApp (..))
import WidgetStore.Handler
import Demo.Http
import WidgetStore.Store
import WidgetStore.View

data WidgetRoute
  = IndexAction
  | ProductAction
  | OrdersAction
  | OrderAction {orderId :: Int}
  | StoreAction
  | RestockAction
  | CheckoutAction {idempotencyKey :: Text}
  | PaymentAction {paymentId :: Text, paymentStatus :: Text}
  | CrashAction
  deriving (Eq, Show)

$(pure []) -- declaration-group boundary

[routes|WidgetRoute
GET /widget-store                       IndexAction
GET /widget-store/product                                ProductAction
GET /widget-store/orders                                 OrdersAction
GET /widget-store/order/{orderId}                        OrderAction { orderId = #orderId }
GET /widget-store/store                                  StoreAction
POST /widget-store/restock                               RestockAction
POST /widget-store/checkout/{idempotencyKey}             CheckoutAction { idempotencyKey = #idempotencyKey }
POST /widget-store/payment_webhook/{paymentId}/{paymentStatus} PaymentAction { paymentId = #paymentId, paymentStatus = #paymentStatus }
POST /widget-store/crash_application                     CrashAction
|]

dispatchWidget :: WidgetApp -> WidgetRoute -> Application
dispatchWidget app route req respond = case route of
  IndexAction ->
    runView pageView (getStorePage app) req respond
  ProductAction ->
    runJson (getProduct app) req respond
  OrdersAction
    | isHtmx req -> runView ordersList (getOrders app) req respond
    | otherwise -> runJson (getOrders app) req respond
  OrderAction {orderId = rawId}
    | isHtmx req -> runView orderStatusPanel (getOrder app rawId) req respond
    | otherwise -> runJson (getOrder app rawId) req respond
  StoreAction ->
    runView storePanelView (getStorePanel app) req respond
  RestockAction
    | isHtmx req -> runView storePanelView (restockProduct app >> getStorePanel app) req respond
    | otherwise -> runEmpty (restockProduct app) req respond
  CheckoutAction {idempotencyKey = key}
    -- The htmx surface shows the failure and offers a retry; the API
    -- surface answers the Rust handler's 500.
    | isHtmx req -> runView (either checkoutErrorPanel paymentPanel) (startCheckout app key) req respond
    | otherwise -> runRespond checkoutApiResponse (startCheckout app key) req respond
  PaymentAction {paymentId = payment, paymentStatus = status}
    | isHtmx req -> runView (either checkoutErrorPanel orderStatusPanel) (settlePayment app payment status) req respond
    | otherwise -> runRespond paymentApiResponse (settlePayment app payment status) req respond
  CrashAction -> crashHandler req respond

-- | The Rust handler's 500 ("Checkout failed"); the htmx surface shows a
-- retry panel instead (in the dispatch above).
checkoutApiResponse :: Either Text Text -> Response
checkoutApiResponse outcome = case outcome of
  Right paymentId -> textResponse status200 paymentId
  Left _ -> textResponse status500 "Checkout failed"

paymentApiResponse :: Either Text Order -> Response
paymentApiResponse outcome = case outcome of
  Right order -> textResponse status200 (showText order.orderId)
  Left _ -> textResponse status500 "Checkout failed"

-- | Crashes the application. For demonstration purposes only :) The pause is
-- so the response reaches the browser that asked for it; the exit is the
-- rudest one available, because a graceful shutdown would prove nothing.
crashHandler :: Application
crashHandler _req respond = do
  _ <- forkIO do
    threadDelay 100000
    putStrLn "Simulating application crash"
    exitImmediately (ExitFailure 1)
  respond (textResponse status200 "Crashing application...")

widgetNotFound :: Application
widgetNotFound _req respond = respond (responseLBS status404 [] "Not Found")
