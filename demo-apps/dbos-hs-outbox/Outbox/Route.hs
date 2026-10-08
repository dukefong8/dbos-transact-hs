{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}

-- | The outbox demo's routes: an ihp-router trie, one dispatch case per
-- route, and the 'RouteHandler' runners from "Demo.Http". Content
-- negotiation matches the widget store: a request carrying htmx's
-- @HX-Request@ header gets the rendered fragment, everyone else the Python
-- demo's JSON surface (the same URLs).
module Outbox.Route
  ( OutboxRoute (..),
    dispatchOutbox,
    outboxRouteTrie,
    outboxNotFound,
  )
where

import Data.Aeson qualified as Aeson
import Data.Aeson ((.:))
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as LBS
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding (decodeUtf8')
import Demo.Http
import IHP.Router.WAI (HasPath (..), routes)
import Network.HTTP.Types (StdMethod (..), hContentType, status400, status404)
import Network.HTTP.Types.URI (parseQuery)
import Network.Wai (Application, Request, queryString, requestHeaders, responseLBS, strictRequestBody)
import Outbox.App (OutboxApp (..))
import Outbox.Handler
import Outbox.View
import Prelude
import Text.Read (readMaybe)

data OutboxRoute
  = IndexAction
  | OrdersAction
  | AtomicAction
  | EnqueueAction
  deriving (Eq, Show)

$(pure []) -- declaration-group boundary

[routes|OutboxRoute
GET /outbox                       IndexAction
GET /outbox/orders                                 OrdersAction
POST /outbox/orders                                AtomicAction
POST /outbox/enqueue-orders                        EnqueueAction
|]

dispatchOutbox :: OutboxApp -> OutboxRoute -> Application
dispatchOutbox app route req respond = case route of
  IndexAction ->
    runView pageView (getOutboxPage app) req respond
  OrdersAction
    | isHtmx req -> runView ordersList (getOrders app) req respond
    | otherwise -> runJson (getOrders app) req respond
  AtomicAction -> do
    form <- readOrderForm req
    case form of
      Left err -> respond (textResponse status400 err)
      Right (customer, item, quantity)
        | isHtmx req -> runView ordersList (placeOrderAtomic app customer item quantity) req respond
        | otherwise -> runJson (placeOrderAtomic app customer item quantity) req respond
  EnqueueAction -> do
    form <- readOrderForm req
    case form of
      Left err -> respond (textResponse status400 err)
      Right (customer, item, quantity)
        | isHtmx req -> runView ordersList (placeOrderEnqueued app customer item quantity) req respond
        | otherwise -> runJson (placeOrderEnqueued app customer item quantity) req respond

-- | Read the order form the way the two surfaces post it: htmx controls post
-- urlencoded fields ('postedParam'), the API surface posts the Python demo's
-- JSON body (@{"customer","item","quantity"}@).
readOrderForm :: Request -> IO (Either Text (Text, Text, Int))
readOrderForm req
  | isJson req = do
      body <- strictRequestBody req
      case (Aeson.decode body :: Maybe OrderForm) of
        Just form -> pure (Right (form.formCustomer, form.formItem, form.formQuantity))
        Nothing -> pure (Left "expected customer, item and quantity")
  | otherwise = do
      params <- formParams req
      case (lookup "customer" params, lookup "item" params, lookup "quantity" params) of
        (Just c, Just i, Just q) -> pure (finish c i q)
        _ -> pure (Left "expected customer, item and quantity")
  where
    -- One body read for all three keys: 'postedParam' consumes
    -- 'strictRequestBody', so three calls would lose the second and third
    -- key. Query string first (where a rendered URL carries values), then
    -- one urlencoded form body (what hx-post produces).
    formParams r = do
      body <- strictRequestBody r
      pure (decodedPairs (queryString r) <> decodedPairs (parseQuery (LBS.toStrict body)))
    decodedPairs pairs =
      [ (keyText, valueText)
        | (rawKey, rawValue) <- pairs,
          Right keyText <- [decodeUtf8' rawKey],
          Just raw <- [rawValue],
          Right valueText <- [decodeUtf8' raw]
      ]
    isJson r = any (\(h, v) -> h == hContentType && "application/json" `BS.isInfixOf` v) (requestHeaders r)
    finish c i q =
      case readMaybe (Text.unpack q) of
        Just n -> Right (c, i, n :: Int)
        Nothing -> Left "quantity must be a number"

-- | The Python demo's JSON body (@{"customer","item","quantity"}@).
data OrderForm = OrderForm
  { formCustomer :: Text,
    formItem     :: Text,
    formQuantity :: Int
  }

instance Aeson.FromJSON OrderForm where
  parseJSON = Aeson.withObject "OrderForm" $ \o ->
    OrderForm <$> o .: "customer" <*> o .: "item" <*> o .: "quantity"

outboxNotFound :: Application
outboxNotFound _req respond = respond (responseLBS status404 [] "Not Found")
