{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}

-- | The DBOS Haskell starter: the Workflows, Queues, Events and Messages tabs
-- of the starter app, mirroring demo-apps/dbos-rust-starter over the same
-- HTTP surface. Durations are shorter than the Rust original so the E2E gate
-- stays fast; every behavior is the same. The index page is app/page.html: the
-- Rust app.html byte-for-byte (same CSS, same JS) except the four code panels,
-- which show the Haskell bodies under the same highlight-hook classes. It is
-- read once at startup, so run the app from the repo root.
module Main (main) where

import Control.Concurrent (myThreadId, threadDelay, throwTo)
import Control.Concurrent.Async (async, cancel)
import Control.Concurrent.STM (TVar, atomically, modifyTVar', newTVarIO, readTVarIO, writeTVar)
import Control.Monad (filterM, replicateM_)
import Data.Aeson (FromJSON (..), ToJSON (..), eitherDecodeStrict, encode, object, withObject, (.:), (.=))
import Data.Aeson.Key (fromText)
import Data.ByteString.Lazy qualified as LBS
import Data.Int (Int64)
import Data.Map.Strict qualified as Map
import Data.Text (Text, pack)
import Data.Text qualified as Text
import Data.Text.Encoding (encodeUtf8)
import Data.Time.Clock (diffUTCTime, getCurrentTime)
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB
  ( Pool,
    QueueConflict (..),
    QueueName (..),
    Topic (..),
    acquirePool,
    enqueueWorkflow,
    fetchQueueWorkerConcurrency,
    fetchWorkflowStatuses,
    getEvent,
    getEventBlocking,
    internalQueueName,
    listWorkflowIdsByName,
    messageTo,
    recvMessage,
    registerQueue,
    releasePool,
    sendMessage,
    sendMessages,
    setEvent,
    tryStartWorkflow,
    updateQueueWorkerConcurrency,
  )
import DBOS.Transact
  ( ApplicationVersion (..),
    Executor (..),
    ExecutorId (..),
    Millis (..),
    OperationId (..),
    OperationName (..),
    SerializedWorkflowValue,
    WorkflowBody,
    WorkflowId (..),
    WorkflowName (..),
    WorkflowRegistry,
    decodeWorkflowValue,
    emptyRegistry,
    encodeWorkflowValue,
    launchExecutor,
    registerWorkflow,
    runStep,
    shutdownExecutor,
    sleepStep,
    spawnWorkflow,
    superviseForever,
    withStdoutLogger,
  )
import IHP.Router.Trie (RouteTrie)
import IHP.Router.WAI (HasPath (..), UrlCapture (..), routeTrieMiddleware, routes)
import Network.HTTP.Types (StdMethod (..), status200, status400, status404)
import Network.Wai (Application, Request, Response, responseLBS, strictRequestBody)
import Network.Wai.Handler.Warp (run)
import System.Environment (lookupEnv)
import System.Exit (ExitCode (..))
import System.IO (hFlush, stdout)
import System.Posix.Process (exitImmediately)
import System.Posix.Signals (Handler (..), installHandler, sigINT, sigTERM)
import Text.Read (readMaybe)

-- ---------------------------------------------------------------------------
-- Durations: shorter than the Rust original so the E2E gate stays fast.
-- ---------------------------------------------------------------------------

stepDurationMs :: Int64
stepDurationMs = 2000

orderStepMs :: Int64
orderStepMs = 1000

queueSleepMs :: Int64
queueSleepMs = 2000

eventReadTimeoutMs :: Int64
eventReadTimeoutMs = 12000

approvalTimeoutMs :: Int64
approvalTimeoutMs = 60000

supervisorIntervalMs :: Int64
supervisorIntervalMs = 1000

stepsEventKey :: Text
stepsEventKey = "steps_event"

orderKeys :: [Text]
orderKeys = ["accepted", "charged", "shipped"]

approvalTopic :: Topic
approvalTopic = Topic "approval"

approvalWorkflowName :: Text
approvalWorkflowName = "ApprovalWorkflow"

decisionEventKey :: Text
decisionEventKey = "decision"

demoQueueName :: QueueName
demoQueueName = QueueName "demo-queue"

defaultWorkerConcurrency :: Int
defaultWorkerConcurrency = 3

enqueueBatchSize :: Int
enqueueBatchSize = 5

approvalListLimit :: Int64
approvalListLimit = 20

-- ---------------------------------------------------------------------------
-- Routes
-- ---------------------------------------------------------------------------

data StarterRoute
  = IndexAction
  | StartWorkflowAction { taskId :: Text }
  | LastStepAction { taskId :: Text }
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

[routes|
GET /                         IndexAction
POST /workflow/{taskId}       StartWorkflowAction
GET /last_step/{taskId}       LastStepAction
POST /crash                   CrashAction
GET /queue/status             QueueStatusAction
POST /queue/enqueue           QueueEnqueueAction
POST /queue/concurrency       QueueConcurrencyAction
POST /events/start            EventsStartAction
GET /events/status            EventsStatusAction
POST /events/read             EventsReadAction
POST /messages/start          MessagesStartAction
GET /messages/status          MessagesStatusAction
POST /messages/respond        MessagesRespondAction
POST /messages/respond-all    MessagesRespondAllAction
|]

data App = App
  { appPool :: Pool,
    appExecutor :: Executor,
    appOrderId :: TVar (Maybe WorkflowId),
    appQueuedIds :: TVar [WorkflowId],
    appPage :: LBS.ByteString
  }

-- ---------------------------------------------------------------------------
-- Responses and request bodies
-- ---------------------------------------------------------------------------

jsonOk :: ToJSON a => a -> Response
jsonOk = responseLBS status200 [("Content-Type", "application/json")] . encode

textOk :: Text -> Response
textOk = responseLBS status200 [("Content-Type", "text/plain; charset=utf-8")] . LBS.fromStrict . encodeUtf8

badRequest :: Text -> Response
badRequest = responseLBS status400 [("Content-Type", "text/plain; charset=utf-8")] . LBS.fromStrict . encodeUtf8

notFound :: Application
notFound _ respond = respond (responseLBS status404 [] "Not Found")

readJsonBody :: FromJSON a => Request -> IO (Either Text a)
readJsonBody request = do
  body <- strictRequestBody request
  pure (either (Left . pack . show) Right (eitherDecodeStrict (LBS.toStrict body)))

data ConcurrencyRequest = ConcurrencyRequest { concurrencyRequestValue :: Maybe Int }

instance FromJSON ConcurrencyRequest where
  parseJSON = withObject "ConcurrencyRequest" $ \o -> ConcurrencyRequest <$> o .: "concurrency"

data ReadRequest = ReadRequest { readRequestKey :: Text }

instance FromJSON ReadRequest where
  parseJSON = withObject "ReadRequest" $ \o -> ReadRequest <$> o .: "key"

data RespondRequest = RespondRequest { respondWorkflowId :: Text, respondDecision :: Text }

instance FromJSON RespondRequest where
  parseJSON = withObject "RespondRequest" $ \o ->
    RespondRequest <$> o .: "workflow_id" <*> o .: "decision"

data RespondAllRequest = RespondAllRequest { respondAllDecision :: Text }

instance FromJSON RespondAllRequest where
  parseJSON = withObject "RespondAllRequest" $ \o -> RespondAllRequest <$> o .: "decision"

-- ---------------------------------------------------------------------------
-- Workflows
-- ---------------------------------------------------------------------------

exampleWorkflowBody :: WorkflowBody
exampleWorkflowBody pool workflowId _ = do
  _ <- runStep pool workflowId (OperationId 1) (OperationName "step_one") (sleepMillis stepDurationMs)
  setEvent pool workflowId stepsEventKey (encodeWorkflowValue (1 :: Int))
  _ <- runStep pool workflowId (OperationId 2) (OperationName "step_two") (sleepMillis stepDurationMs)
  setEvent pool workflowId stepsEventKey (encodeWorkflowValue (2 :: Int))
  _ <- runStep pool workflowId (OperationId 3) (OperationName "step_three") (sleepMillis stepDurationMs)
  setEvent pool workflowId stepsEventKey (encodeWorkflowValue (3 :: Int))
  pure (encodeWorkflowValue ("Workflow completed" :: Text))
  where
    sleepMillis ms = threadDelay (fromIntegral ms * 1000) >> pure (encodeWorkflowValue ())

orderWorkflowBody :: WorkflowBody
orderWorkflowBody pool workflowId _ = do
  mapM_ publish (zip [1 ..] orderKeys)
  pure (encodeWorkflowValue ("Order complete" :: Text))
  where
    publish (stage, key) = do
      sleepStep pool workflowId (OperationId stage) (Millis orderStepMs)
      setEvent pool workflowId key (encodeWorkflowValue (key <> " at step " <> pack (show stage)))

approvalWorkflowBody :: WorkflowBody
approvalWorkflowBody pool workflowId _ = do
  decision <- recvMessage pool workflowId (OperationId 1) (Millis approvalTimeoutMs) (Just approvalTopic)
  let outcome = case decision of
        Just stored -> case decodeWorkflowValue "result" (Just stored) of
          Right text -> text :: Text
          Left _ -> "expired"
        Nothing -> "expired"
  setEvent pool workflowId decisionEventKey (encodeWorkflowValue outcome)
  pure (encodeWorkflowValue outcome)

enqueuedWorkflowBody :: WorkflowBody
enqueuedWorkflowBody pool workflowId _ = do
  sleepStep pool workflowId (OperationId 1) (Millis queueSleepMs)
  pure (encodeWorkflowValue ("Enqueued workflow completed" :: Text))

-- ---------------------------------------------------------------------------
-- Dispatch
-- ---------------------------------------------------------------------------

dispatch :: App -> StarterRoute -> Application
dispatch app IndexAction _ respond =
  respond (responseLBS status200 [("Content-Type", "text/html; charset=utf-8")] app.appPage)
dispatch app (StartWorkflowAction taskId) _ respond = do
  _ <- startBackground app (WorkflowName "ExampleWorkflow") (WorkflowId taskId)
  respond (textOk "")
dispatch app (LastStepAction taskId) _ respond = do
  step <- getEvent app.appPool (WorkflowId taskId) stepsEventKey
  respond (textOk (progressText step))
  where
    progressText Nothing = "0"
    progressText (Just stored) = case decodeWorkflowValue "result" (Just stored) of
      Right n -> pack (show (n :: Int))
      Left _ -> "0"
-- Crash like kill -9: die at once with no cleanup, so in-flight rows stay
-- PENDING for the next launch to recover. Plain exitWith would only kill
-- this warp worker thread (same wart the shutdown path routes around).
-- Prints the oracle's crash line first, flushed: immediate exit would
-- otherwise swallow block-buffered stdout.
dispatch _ CrashAction _ _ = do
  putStrLn "Simulating application crash"
  hFlush stdout
  exitImmediately (ExitFailure 1)
dispatch app QueueStatusAction _ respond = do
  workerConcurrency <- fetchQueueWorkerConcurrency app.appPool demoQueueName
  queuedIds <- readTVarIO app.appQueuedIds
  statuses <- fetchWorkflowStatuses app.appPool queuedIds
  let counts :: [(Text, Int)]
      counts = Map.toList (Map.fromListWith (+) [(Text.toUpper (pack (show status)), 1) | (_, status) <- statuses])
  respond
    ( jsonOk
        ( object
            [ "worker_concurrency" .= maybe defaultWorkerConcurrency id workerConcurrency,
              "workflow_counts" .= object [fromText name .= count | (name, count) <- counts]
            ]
        )
    )
dispatch app QueueEnqueueAction _ respond = do
  replicateM_ enqueueBatchSize $ do
    workflowId <- freshId "queued"
    enqueueWorkflow app.appPool workflowId (WorkflowName "EnqueuedWorkflow") demoQueueName
    atomically (modifyTVar' app.appQueuedIds (workflowId :))
  respond (textOk "")
dispatch app QueueConcurrencyAction request respond = do
  parsed <- readJsonBody request
  case parsed of
    Left err -> respond (badRequest err)
    Right ConcurrencyRequest { concurrencyRequestValue = requested } -> do
      let concurrency = case requested of
            Just n | n >= 1 -> n
            _ -> defaultWorkerConcurrency
      updateQueueWorkerConcurrency app.appPool demoQueueName concurrency
      respond (textOk "")
dispatch app EventsStartAction _ respond = do
  workflowId <- startBackground app (WorkflowName "OrderWorkflow") =<< freshId "order"
  atomically (writeTVar app.appOrderId (Just workflowId))
  respond (textOk (workflowIdText workflowId))
dispatch app EventsStatusAction _ respond = do
  current <- readTVarIO app.appOrderId
  keys <- traverse (readKey current) orderKeys
  respond
    ( jsonOk
        ( object
            [ "workflow_id" .= fmap workflowIdText current,
              "keys" .= [object ["key" .= key, "value" .= value] | (key, value) <- keys]
            ]
        )
    )
  where
    readKey Nothing key = pure (key, Nothing :: Maybe Text)
    readKey (Just workflowId) key = do
      stored <- getEvent app.appPool workflowId key
      pure (key, stored >>= decodeToText)
dispatch app EventsReadAction request respond = do
  parsed <- readJsonBody request
  case parsed of
    Left err -> respond (badRequest err)
    Right ReadRequest { readRequestKey = key } -> do
      current <- readTVarIO app.appOrderId
      case current of
        Nothing -> respond (jsonOk (object ["key" .= key, "value" .= (Nothing :: Maybe Text), "waited_ms" .= (0 :: Int)]))
        Just workflowId -> do
          started <- getCurrentTime
          stored <- getEventBlocking app.appPool workflowId key (Millis eventReadTimeoutMs)
          finished <- getCurrentTime
          let waitedMs = round (diffUTCTime finished started * 1000) :: Int
          respond (jsonOk (object ["key" .= key, "value" .= (stored >>= decodeToText), "waited_ms" .= waitedMs]))
dispatch app MessagesStartAction _ respond = do
  workflowId <- startBackground app (WorkflowName approvalWorkflowName) =<< freshId "approval"
  respond (textOk (workflowIdText workflowId))
dispatch app MessagesStatusAction _ respond = do
  ids <- listWorkflowIdsByName app.appPool approvalWorkflowName approvalListLimit
  approvals <- traverse readApproval ids
  respond (jsonOk approvals)
  where
    readApproval workflowId@(WorkflowId text) = do
      stored <- getEvent app.appPool workflowId decisionEventKey
      pure (object ["workflow_id" .= text, "decision" .= (stored >>= decodeToText)])
dispatch app MessagesRespondAction request respond = do
  parsed <- readJsonBody request
  case parsed of
    Left err -> respond (badRequest err)
    Right RespondRequest { respondWorkflowId = workflowId, respondDecision = decision } -> do
      sendMessage app.appPool (messageTo (WorkflowId workflowId) approvalTopic (encodeWorkflowValue decision))
      respond (textOk "")
dispatch app MessagesRespondAllAction request respond = do
  parsed <- readJsonBody request
  case parsed of
    Left err -> respond (badRequest err)
    Right RespondAllRequest { respondAllDecision = decision } -> do
      ids <- listWorkflowIdsByName app.appPool approvalWorkflowName approvalListLimit
      waiting <- filterM (fmap (== Nothing) . readDecision) ids
      sendMessages app.appPool [messageTo workflowId approvalTopic (encodeWorkflowValue decision) | workflowId <- waiting]
      respond (textOk (pack (show (length waiting))))
  where
    readDecision workflowId = do
      stored <- getEvent app.appPool workflowId decisionEventKey
      pure (stored >>= decodeToText)

decodeToText :: SerializedWorkflowValue -> Maybe Text
decodeToText stored = case decodeWorkflowValue "result" (Just stored) of
  Right text -> Just text
  Left _ -> Nothing

workflowIdText :: WorkflowId -> Text
workflowIdText (WorkflowId text) = text

-- | Start a workflow and return at once; the handle is dropped. The id is an
-- idempotency key: starting the same id twice joins the workflow already
-- running rather than failing.
startBackground :: App -> WorkflowName -> WorkflowId -> IO WorkflowId
startBackground app name workflowId = do
  _ <- tryStartWorkflow app.appPool workflowId name Nothing app.appExecutor.executorId app.appExecutor.executorVersion
  _ <- spawnWorkflow app.appExecutor name workflowId Nothing
  pure workflowId

freshId :: Text -> IO WorkflowId
freshId prefix = (WorkflowId . (prefix <>) . ("-" <>) . UUID.toText) <$> UUID.V4.nextRandom

-- ---------------------------------------------------------------------------
-- View: app/page.html, served as read at startup.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- Wiring
-- ---------------------------------------------------------------------------

mustRegister :: WorkflowName -> WorkflowBody -> WorkflowRegistry -> IO WorkflowRegistry
mustRegister name body registry = case registerWorkflow name body registry of
  Left err -> fail ("duplicate workflow registration: " <> show err)
  Right updated -> pure updated

buildRegistry :: IO WorkflowRegistry
buildRegistry = do
  withExample <- mustRegister (WorkflowName "ExampleWorkflow") exampleWorkflowBody emptyRegistry
  withOrder <- mustRegister (WorkflowName "OrderWorkflow") orderWorkflowBody withExample
  withApproval <- mustRegister (WorkflowName approvalWorkflowName) approvalWorkflowBody withOrder
  mustRegister (WorkflowName "EnqueuedWorkflow") enqueuedWorkflowBody withApproval

main :: IO ()
main = withStdoutLogger $ \logger -> do
  port <- maybe 8081 id . (>>= readMaybe) <$> lookupEnv "PORT"
  executorId <- maybe "hs-starter-executor" pack <$> lookupEnv "DBOS_EXECUTOR_ID"
  applicationVersion <- maybe "0.1.0" pack <$> lookupEnv "DBOS_APP_VERSION"
  pool <- acquirePool
  registry <- buildRegistry
  (executor, _) <-
    launchExecutor pool (ExecutorId executorId) (ApplicationVersion applicationVersion) registry logger
  registerQueue pool demoQueueName defaultWorkerConcurrency NeverUpdate
  page <- LBS.readFile "app/page.html"
  orderId <- newTVarIO Nothing
  queuedIds <- newTVarIO []
  supervisor <- async (superviseForever executor [demoQueueName, internalQueueName] (Millis supervisorIntervalMs))
  mainThread <- myThreadId
  -- exitSuccess only terminates the calling thread: run from the signal
  -- handler it kills just the handler and the process lingers in warp.
  -- Throwing to the main thread ends the process with a quiet code 0.
  let shutdown = do
        cancel supervisor
        shutdownExecutor executor
        releasePool pool
        throwTo mainThread ExitSuccess
  _ <- installHandler sigINT (CatchOnce shutdown) Nothing
  _ <- installHandler sigTERM (CatchOnce shutdown) Nothing
  run port (routeTrieMiddleware (starterRouteTrie (dispatch (App pool executor orderId queuedIds page))) notFound)
