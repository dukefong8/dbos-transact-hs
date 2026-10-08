{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}

-- | The queue-worker demo's routes: an ihp-router trie, one dispatch case per
-- route, and the 'RouteHandler' runners from "Demo.Http". Content
-- negotiation matches the other demos: a request carrying htmx's
-- @HX-Request@ header gets the rendered fragment, everyone else the Python
-- demo's JSON surface (the same URLs).
module QueueWorker.Route
  ( QueueWorkerRoute (..),
    dispatchQueueWorker,
    queueWorkerRouteTrie,
    queueWorkerNotFound,
  )
where

import Demo.Http
import IHP.Router.WAI (HasPath (..), UrlCapture (..), routes)
import Network.HTTP.Types (StdMethod (..), status404)
import Network.Wai (Application, responseLBS)
import Prelude
import QueueWorker.App (QueueWorkerApp (..))
import QueueWorker.Handler
import QueueWorker.View

data QueueWorkerRoute
  = IndexAction
  | WorkflowsListAction
  | WorkflowsEnqueueAction
  deriving (Eq, Show)

$(pure []) -- declaration-group boundary

[routes|QueueWorkerRoute
GET /queue-worker                       IndexAction
GET /queue-worker/workflows                                 WorkflowsListAction
POST /queue-worker/workflows                                WorkflowsEnqueueAction
|]

dispatchQueueWorker :: QueueWorkerApp -> QueueWorkerRoute -> Application
dispatchQueueWorker app route req respond = case route of
  IndexAction ->
    runView pageView (getQueueWorkerPage app) req respond
  WorkflowsListAction
    | isHtmx req -> runView workflowsList (getWorkflows app) req respond
    | otherwise -> runJson (getWorkflows app) req respond
  WorkflowsEnqueueAction
    | isHtmx req -> runView workflowsList (enqueueWorkflow app) req respond
    | otherwise -> runJson (enqueueWorkflow app) req respond

queueWorkerNotFound :: Application
queueWorkerNotFound _req respond = respond (responseLBS status404 [] "Not Found")
