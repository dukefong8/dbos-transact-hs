{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}

-- | The queue-patterns demo's routes: an ihp-router trie, one dispatch case
-- per route, and the 'RouteHandler' runners from "Demo.Http". The active tab
-- travels in @?tab=@ (the way the starter keeps its task id in the URL);
-- content negotiation matches the other demos.
module QueuePatterns.Route
  ( QueuePatternsRoute (..),
    dispatchQueuePatterns,
    queuePatternsRouteTrie,
    queuePatternsNotFound,
  )
where

import Control.Monad.IO.Class (liftIO)
import Data.ByteString.Lazy qualified as LBS
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8')
import Demo.Http
import IHP.Router.WAI (HasPath (..), UrlCapture (..), routes)
import Network.HTTP.Types (StdMethod (..), status400, status404)
import Network.HTTP.Types.URI (parseQuery)
import Network.Wai (Application, Request, queryString, responseLBS, strictRequestBody)
import Prelude
import QueuePatterns.App (QueuePatternsApp (..))
import QueuePatterns.Handler
import QueuePatterns.View

data QueuePatternsRoute
  = IndexAction
  | WorkflowsListAction
  | FairSubmitAction
  | RateLimitedSubmitAction
  | DebouncerSubmitAction
  deriving (Eq, Show)

$(pure []) -- declaration-group boundary

[routes|QueuePatternsRoute
GET /queue-patterns                       IndexAction
GET /queue-patterns/workflows                                 WorkflowsListAction
POST /queue-patterns/workflows/fair_queue                     FairSubmitAction
POST /queue-patterns/workflows/rate_limited_queue             RateLimitedSubmitAction
POST /queue-patterns/workflows/debouncer                      DebouncerSubmitAction
|]

dispatchQueuePatterns :: QueuePatternsApp -> QueuePatternsRoute -> Application
dispatchQueuePatterns app route req respond = case route of
  IndexAction -> do
    tab <- liftIO (parseTab <$> postedParam req "tab")
    runView pageView (getPatternsPage app tab) req respond
  WorkflowsListAction -> do
    tab <- liftIO (parseTab <$> postedParam req "tab")
    if isHtmx req
      then runView workflowsList (getPatternRows app tab) req respond
      else runJson (getPatternRows app tab) req respond
  FairSubmitAction -> do
    tenant <- fromMaybe "" <$> liftIO (postedParam req "tenant_id")
    if isHtmx req
      then runView workflowsList (submitFair app tenant) req respond
      else runJson (submitFair app tenant) req respond
  RateLimitedSubmitAction
    | isHtmx req -> runView workflowsList (submitRateLimited app) req respond
    | otherwise -> runJson (submitRateLimited app) req respond
  DebouncerSubmitAction -> do
    form <- readDebounceForm req
    case form of
      Left err -> respond (textResponse status400 err)
      Right (tenant, input)
        | isHtmx req -> runView workflowsList (submitDebounced app tenant input) req respond
        | otherwise -> runJson (submitDebounced app tenant input) req respond

-- | The active tab, defaulting to the fair queue like the Python frontend.
parseTab :: Maybe Text -> PatternsTab
parseTab (Just "rate-limited") = RateLimitedTab
parseTab (Just "debouncer") = DebouncerTab
parseTab _ = FairQueueTab

-- | Read the debouncer form: urlencoded @tenant_id@/@input@ in one body read
-- (a second 'postedParam' call would find an empty body).
readDebounceForm :: Request -> IO (Either Text (Text, Text))
readDebounceForm req = do
  body <- strictRequestBody req
  let params = decodedPairs (queryString req) <> decodedPairs (parseQuery (LBS.toStrict body))
  case (lookup "tenant_id" params, lookup "input" params) of
    (Just tenant, Just input) -> pure (Right (tenant, input))
    _ -> pure (Left "expected tenant_id and input")
  where
    decodedPairs pairs =
      [ (keyText, valueText)
        | (rawKey, rawValue) <- pairs,
          Right keyText <- [decodeUtf8' rawKey],
          Just raw <- [rawValue],
          Right valueText <- [decodeUtf8' raw]
      ]

queuePatternsNotFound :: Application
queuePatternsNotFound _req respond = respond (responseLBS status404 [] "Not Found")
