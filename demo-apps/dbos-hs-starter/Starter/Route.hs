{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}

-- | The starter's routes, mirroring the widget app's split: the ihp-router
-- trie, one dispatch case per route, and the 'Demo.Http' runners. The Rust
-- starter's JSON surface is unchanged; a request carrying htmx's
-- @HX-Request@ header gets the rendered fragment instead, and the three
-- body-carrying actions read htmx's posted values where the API reads JSON.
module Starter.Route
  ( StarterRoute (..),
    starterRouteTrie,
    dispatchStarter,
    starterNotFound,
  )
where

import Control.Monad.IO.Class (liftIO)
import Prelude
import Data.Aeson (FromJSON (..), eitherDecodeStrict, encode, object, withObject, (.:), (.=))
import Data.ByteString.Lazy qualified as LBS
import Data.Maybe (fromMaybe)
import Data.Text (Text, pack)
import Data.Text qualified as Text
import Data.Text.Encoding (encodeUtf8)
import Demo.Http
  ( RouteHandler,
    isHtmx,
    postedParam,
    runEmpty,
    runJson,
    runRespond,
    runText,
    runView,
    runViewTriggering,
    textResponse,
    throwRouteError,
  )
import IHP.Router.Trie (RouteTrie)
import IHP.Router.WAI (HasPath (..), UrlCapture (..), routes)
import Network.HTTP.Types (StdMethod (..), status200, status400, status404)
import Network.Wai (Application, Request, Response, responseLBS, strictRequestBody)
import System.Exit (ExitCode (..))
import System.IO (hFlush, stdout)
import System.Posix.Process (exitImmediately)
import Text.Read (readMaybe)

import Starter.App (StarterApp (..))
import Starter.Handler
import Starter.View
import Starter.Workflows

data StarterRoute
  = IndexAction
  | StartWorkflowAction {taskId :: Text}
  | LastStepAction {taskId :: Text}
  | CrashAction
  | QueueStatusAction
  | QueueEnqueueAction
  | QueueConcurrencyAction
  | EventsStartAction
  | EventsStatusAction
  | EventsReadAction
  | MessagesStartAction
  | MessagesStatusAction
  | MessagesRespondAction
  | MessagesRespondAllAction
  deriving (Eq, Show)

$(pure [])

starterRouteTrie :: (StarterRoute -> Application) -> RouteTrie

[routes|StarterRoute
GET /starter                          IndexAction
POST /starter/workflow/{taskId}       StartWorkflowAction { taskId = #taskId }
GET /starter/last_step/{taskId}       LastStepAction { taskId = #taskId }
POST /starter/crash                   CrashAction
GET /starter/queue/status             QueueStatusAction
POST /starter/queue/enqueue           QueueEnqueueAction
POST /starter/queue/concurrency       QueueConcurrencyAction
POST /starter/events/start            EventsStartAction
GET /starter/events/status            EventsStatusAction
POST /starter/events/read             EventsReadAction
POST /starter/messages/start          MessagesStartAction
GET /starter/messages/status          MessagesStatusAction
POST /starter/messages/respond        MessagesRespondAction
POST /starter/messages/respond-all    MessagesRespondAllAction
|]

dispatchStarter :: StarterApp -> StarterRoute -> Application
dispatchStarter app route req respond = case route of
  IndexAction -> do
    tab <- liftIO (postedParam req "tab")
    task <- liftIO (postedParam req "id")
    runView pageView (getPageView app (fromMaybe "workflows" tab) task) req respond
  StartWorkflowAction {taskId = task}
    | isHtmx req -> runView timelineView (startWorkflow app task >> getWorkflowProgress app task) req respond
    | otherwise -> runEmpty (startWorkflow app task) req respond
  LastStepAction {taskId = task}
    | isHtmx req -> runViewTriggering workflowTrigger timelineView (getWorkflowProgress app task) req respond
    | otherwise -> runRespond lastStepResponse (getWorkflowProgress app task) req respond
  CrashAction -> crashHandler req respond
  QueueStatusAction
    | isHtmx req -> runViewTriggering queueTrigger queueCountsView (getQueueStatus app) req respond
    | otherwise -> runJson (getQueueStatus app) req respond
  QueueEnqueueAction
    | isHtmx req -> runViewTriggering queueTrigger queueCountsView (enqueueWorkflows app >> getQueueStatus app) req respond
    | otherwise -> runEmpty (enqueueWorkflows app) req respond
  QueueConcurrencyAction ->
    let action = do
          requested <- concurrencyParam req
          applyConcurrency app requested
     in if isHtmx req
          then runViewTriggering queueTrigger queueCountsView (action >> getQueueStatus app) req respond
          else runEmpty action req respond
  EventsStartAction
    | isHtmx req -> runViewTriggering eventsTrigger eventKeysView (startOrder app >> getEventsStatus app) req respond
    | otherwise -> runText (startOrderText app) req respond
  EventsStatusAction
    | isHtmx req -> runViewTriggering eventsTrigger eventKeysView (getEventsStatus app) req respond
    | otherwise -> runJson (getEventsStatus app) req respond
  EventsReadAction ->
    let action = do
          key <- eventKeyParam req
          readEvent app key
     in if isHtmx req
          then runView readResultView action req respond
          else runJson action req respond
  MessagesStartAction
    | isHtmx req -> runViewTriggering messagesTrigger approvalRowsView (startApproval app >> getApprovals app) req respond
    | otherwise -> runText (startApprovalText app) req respond
  MessagesStatusAction
    | isHtmx req -> runViewTriggering messagesTrigger approvalRowsView (getApprovals app) req respond
    | otherwise -> runJson (getApprovals app) req respond
  MessagesRespondAction ->
    let action = do
          (workflowId, decision) <- respondParam req
          respondApproval app workflowId decision
     in if isHtmx req
          then runViewTriggering messagesTrigger approvalRowsView (action >> getApprovals app) req respond
          else runEmpty action req respond
  MessagesRespondAllAction ->
    let action = do
          decision <- decisionParam req
          respondAllApprovals app decision
     in if isHtmx req
          then runViewTriggering messagesTrigger approvalRowsView (action >> getApprovals app) req respond
          else runText ((pack . show) <$> action) req respond

-- | Crash like kill -9: die at once with no cleanup, so in-flight rows stay
-- PENDING for the next launch to recover. Plain exitWith would only kill
-- this warp worker thread (same wart the shutdown path routes around).
-- Prints the oracle's crash line first, flushed: immediate exit would
-- otherwise swallow block-buffered stdout.
crashHandler :: Application
crashHandler _req _respond = do
  putStrLn "Simulating application crash"
  hFlush stdout
  exitImmediately (ExitFailure 1)

starterNotFound :: Application
starterNotFound _req respond = respond (responseLBS status404 [] "Not Found")

-- * Responses

lastStepResponse :: WorkflowProgress -> Response
lastStepResponse progress = textResponse status200 (pack (show progress.wpLastStep))

-- * Parameters (htmx posts values; the API sends JSON)

concurrencyParam :: Request -> RouteHandler Int
concurrencyParam req = do
  requested <-
    if isHtmx req
      then readPostedInt req "concurrency"
      else do
        parsed <- readJsonBody req :: RouteHandler (Either Text ConcurrencyRequest)
        case parsed of
          Left err -> jsonError err
          Right request -> pure request.concurrencyRequestValue
  pure (case requested of
          Just n | n >= 1 -> n
          _ -> defaultWorkerConcurrency)

eventKeyParam :: Request -> RouteHandler Text
eventKeyParam req
  | isHtmx req = fromMaybe "shipped" <$> liftIO (postedParam req "key")
  | otherwise = do
      parsed <- readJsonBody req :: RouteHandler (Either Text ReadRequest)
      case parsed of
        Left err -> jsonError err
        Right request -> pure request.readRequestKey

respondParam :: Request -> RouteHandler (Text, Text)
respondParam req
  | isHtmx req = do
      workflowId <- fromMaybe "" <$> liftIO (postedParam req "workflow_id")
      decision <- fromMaybe "approved" <$> liftIO (postedParam req "decision")
      pure (workflowId, decision)
  | otherwise = do
      parsed <- readJsonBody req :: RouteHandler (Either Text RespondRequest)
      case parsed of
        Left err -> jsonError err
        Right request -> pure (request.respondWorkflowId, request.respondDecision)

decisionParam :: Request -> RouteHandler Text
decisionParam req
  | isHtmx req = fromMaybe "approved" <$> liftIO (postedParam req "decision")
  | otherwise = do
      parsed <- readJsonBody req :: RouteHandler (Either Text RespondAllRequest)
      case parsed of
        Left err -> jsonError err
        Right request -> pure request.respondAllDecision

readPostedInt :: Request -> Text -> RouteHandler (Maybe Int)
readPostedInt req key = do
  posted <- liftIO (postedParam req key)
  pure (posted >>= readMaybe . Text.unpack)

jsonError :: Text -> RouteHandler a
jsonError err = throwRouteError status400 (LBS.fromStrict (encodeUtf8 err))

readJsonBody :: FromJSON a => Request -> RouteHandler (Either Text a)
readJsonBody request = do
  body <- liftIO (strictRequestBody request)
  pure $ case eitherDecodeStrict (LBS.toStrict body) of
    Left err -> Left (pack err)
    Right value -> Right value

data ConcurrencyRequest = ConcurrencyRequest {concurrencyRequestValue :: Maybe Int}

instance FromJSON ConcurrencyRequest where
  parseJSON = withObject "ConcurrencyRequest" \o -> ConcurrencyRequest <$> o .: "concurrency"

data ReadRequest = ReadRequest {readRequestKey :: Text}

instance FromJSON ReadRequest where
  parseJSON = withObject "ReadRequest" \o -> ReadRequest <$> o .: "key"

data RespondRequest = RespondRequest {respondWorkflowId :: Text, respondDecision :: Text}

instance FromJSON RespondRequest where
  parseJSON = withObject "RespondRequest" \o ->
    RespondRequest <$> o .: "workflow_id" <*> o .: "decision"

data RespondAllRequest = RespondAllRequest {respondAllDecision :: Text}

instance FromJSON RespondAllRequest where
  parseJSON = withObject "RespondAllRequest" \o -> RespondAllRequest <$> o .: "decision"

-- * HX-Trigger payloads

workflowTrigger :: WorkflowProgress -> LBS.ByteString
workflowTrigger progress =
  encode (object ["workflowHighlight" .= object ["step" .= workflowHighlightStep progress]])

queueTrigger :: QueueStatus -> LBS.ByteString
queueTrigger status =
  encode (object ["queueActive" .= object ["running" .= queueIsRunning status]])

eventsTrigger :: EventsStatus -> LBS.ByteString
eventsTrigger status =
  encode (object ["eventPublishing" .= object ["publishing" .= eventsArePublishing status]])

messagesTrigger :: [ApprovalRow] -> LBS.ByteString
messagesTrigger rows =
  encode (object ["messageWaiting" .= object ["waiting" .= messagesAreWaiting rows]])
