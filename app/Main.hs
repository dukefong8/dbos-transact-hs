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

import DBOS.Prelude
import Control.Monad (filterM, replicateM_)
import Data.Aeson (FromJSON (..), ToJSON (..), eitherDecodeStrict, encode, object, withObject, (.:), (.=))
import Data.Aeson.Key (fromText)
import Data.ByteString.Lazy qualified as LBS
import Data.Int (Int64)
import Data.Word (Word64)
import Data.Map.Strict qualified as Map
import Data.Text (Text, pack)
import Data.Text qualified as Text
import Data.Text.Encoding (encodeUtf8)
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (Change (..), SendMessage (..), Topic (..), millisDuration)
import DBOS.Transact
  ( Config (..),
    Ctx,
    DBOS,
    Environment (..),
    Error,
    Queue (..),
    QueueChange (..),
    QueueConflict (..),
    QueueOptions (..),
    SerializedWorkflowValue (..),
    WorkflowId (..),
    WorkflowKey,
    configFromEnv,
    decodeWorkflowValue,
    defaultQueueChange,
    defaultQueueOptions,
    encodeWorkflowValue,
    enqueueDBOSWorkflow,
    fetchWorkflowStatuses,
    getWorkflowEvent,
    launchWithEnvironment,
    listWorkflowIdsByName,
    newDBOS,
    newWorkflowKey,
    queue,
    recv,
    registerDBOSWorkflow,
    registerQueue,
    runDBOSWorkflow,
    runWorkflowStep,
    sendWorkflowMessage,
    sendWorkflowMessages,
    setEvent,
    shutdown,
    sleepWorkflowStep,
    updateQueue,
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

stepDurationMs :: Word64
stepDurationMs = 2000

orderStepMs :: Word64
orderStepMs = 1000

queueSleepMs :: Word64
queueSleepMs = 2000

eventReadTimeoutMs :: Word64
eventReadTimeoutMs = 12000

approvalTimeoutMs :: Word64
approvalTimeoutMs = 60000

stepsEventKey :: Text
stepsEventKey = "steps_event"

orderKeys :: [Text]
orderKeys = ["accepted", "charged", "shipped"]

approvalTopic :: Topic
approvalTopic = Topic "approval"

approvalWorkflowName :: Text
approvalWorkflowName = "ApprovalWorkflow"

enqueuedWorkflowName :: Text
enqueuedWorkflowName = "EnqueuedWorkflow"

decisionEventKey :: Text
decisionEventKey = "decision"

demoQueueName :: Text
demoQueueName = "demo-queue"

defaultWorkerConcurrency :: Int
defaultWorkerConcurrency = 3

enqueueBatchSize :: Int
enqueueBatchSize = 5

approvalListLimit :: Int64
approvalListLimit = 20

queueListLimit :: Int64
queueListLimit = 200

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
  { appDBOS :: DBOS IO,
    appOrderId :: StrictTVar IO (Maybe WorkflowId),
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
-- Workflows: registered bodies over the explicit context.
-- ---------------------------------------------------------------------------

exampleWorkflowBody :: () -> Ctx IO -> IO (Either Error Text)
exampleWorkflowBody () ctx = do
  first <- stepSleep ctx "step_one" stepDurationMs
  case first of
    Left err -> pure (Left err)
    Right () -> do
      published <- setEvent ctx stepsEventKey (1 :: Int)
      case published of
        Left err -> pure (Left err)
        Right () -> continue ctx
  where
    continue innerCtx = do
      second <- stepSleep innerCtx "step_two" stepDurationMs
      case second of
        Left err -> pure (Left err)
        Right () -> do
          published <- setEvent innerCtx stepsEventKey (2 :: Int)
          case published of
            Left err -> pure (Left err)
            Right () -> do
              third <- stepSleep innerCtx "step_three" stepDurationMs
              case third of
                Left err -> pure (Left err)
                Right () -> do
                  lastPublished <- setEvent innerCtx stepsEventKey (3 :: Int)
                  pure (lastPublished >> Right "Workflow completed")

orderWorkflowBody :: () -> Ctx IO -> IO (Either Error Text)
orderWorkflowBody () ctx = do
  published <- mapM publish (zip [1 ..] orderKeys)
  pure (sequence_ published >> Right "Order complete")
  where
    publish (stage, key) = do
      slept <- sleepWorkflowStep ctx (millisDuration orderStepMs)
      case slept of
        Left err -> pure (Left err)
        Right () -> setEvent ctx key (key <> " at step " <> pack (show (stage :: Int)))

approvalWorkflowBody :: () -> Ctx IO -> IO (Either Error Text)
approvalWorkflowBody () ctx = do
  decision <- recv ctx (Just approvalTopic) (millisDuration approvalTimeoutMs)
  case (decision :: Either Error (Maybe Text)) of
    Left err -> pure (Left err)
    Right stored -> do
      let outcome = maybe "expired" id stored
      published <- setEvent ctx decisionEventKey outcome
      pure (published >> Right outcome)

enqueuedWorkflowBody :: () -> Ctx IO -> IO (Either Error Text)
enqueuedWorkflowBody () ctx = do
  slept <- sleepWorkflowStep ctx (millisDuration queueSleepMs)
  pure (slept >> Right "Enqueued workflow completed")

stepSleep :: Ctx IO -> Text -> Word64 -> IO (Either Error ())
stepSleep ctx name milliseconds =
  runWorkflowStep ctx name (const (threadDelay (fromIntegral milliseconds * 1000) >> pure ()))

-- ---------------------------------------------------------------------------
-- Dispatch
-- ---------------------------------------------------------------------------

dispatch :: App -> StarterRoute -> Application
dispatch app IndexAction _ respond =
  respond (responseLBS status200 [("Content-Type", "text/html; charset=utf-8")] app.appPage)
dispatch app (StartWorkflowAction taskId) _ respond = do
  _ <- startBackground app (newWorkflowKey "ExampleWorkflow") (WorkflowId taskId)
  respond (textOk "")
dispatch app (LastStepAction taskId) _ respond = do
  step <- getWorkflowEvent app.appDBOS (WorkflowId taskId) stepsEventKey (millisDuration 0)
  respond (textOk (progressText step))
  where
    progressText (Left _) = "0"
    progressText (Right Nothing) = "0"
    progressText (Right (Just stored)) = case decodeWorkflowValue "result" (Just stored) of
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
  workerConcurrency <- fetchQueueWorkerConcurrency app.appDBOS demoQueueName
  -- The queue's rows, not this process's memory: a batch enqueued before a
  -- restart is still on the queue, and the counts must add up against it.
  listed <- listWorkflowIdsByName app.appDBOS enqueuedWorkflowName queueListLimit
  let queuedIds = either (const []) id listed
  statuses <- fetchWorkflowStatuses app.appDBOS queuedIds
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
    _ <- enqueueDBOSWorkflow app.appDBOS (newWorkflowKey enqueuedWorkflowName) workflowId Nothing demoQueueName
    pure ()
  respond (textOk "")
dispatch app QueueConcurrencyAction request respond = do
  parsed <- readJsonBody request
  case parsed of
    Left err -> respond (badRequest err)
    Right ConcurrencyRequest { concurrencyRequestValue = requested } -> do
      let concurrency = case requested of
            Just n | n >= 1 -> n
            _ -> defaultWorkerConcurrency
      _ <- updateQueue app.appDBOS demoQueueName (defaultQueueChange {worker_concurrency = Set (Just concurrency)})
      respond (textOk "")
dispatch app EventsStartAction _ respond = do
  workflowId <- startBackground app (newWorkflowKey "OrderWorkflow") =<< freshId "order"
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
      stored <- getWorkflowEvent app.appDBOS workflowId key (millisDuration 0)
      pure (key, either (const Nothing) (>>= decodeToText) stored)
dispatch app EventsReadAction request respond = do
  parsed <- readJsonBody request
  case parsed of
    Left err -> respond (badRequest err)
    Right ReadRequest { readRequestKey = key } -> do
      current <- readTVarIO app.appOrderId
      case current of
        Nothing -> respond (jsonOk (object ["key" .= key, "value" .= (Nothing :: Maybe Text), "waited_ms" .= (0 :: Int)]))
        Just workflowId -> do
          started <- getMonotonicTimeNSec
          stored <- getWorkflowEvent app.appDBOS workflowId key (millisDuration eventReadTimeoutMs)
          finished <- getMonotonicTimeNSec
          let waitedMs = fromIntegral ((finished - started) `div` 1000000) :: Int
          respond (jsonOk (object ["key" .= key, "value" .= (either (const Nothing) (>>= decodeToText) stored), "waited_ms" .= waitedMs]))
dispatch app MessagesStartAction _ respond = do
  workflowId <- startBackground app (newWorkflowKey approvalWorkflowName) =<< freshId "approval"
  respond (textOk (workflowIdText workflowId))
dispatch app MessagesStatusAction _ respond = do
  listed <- listWorkflowIdsByName app.appDBOS approvalWorkflowName approvalListLimit
  ids <- case listed of
    Left _ -> pure []
    Right workflowIds -> pure workflowIds
  approvals <- traverse readApproval ids
  respond (jsonOk approvals)
  where
    readApproval workflowId@(WorkflowId text) = do
      stored <- getWorkflowEvent app.appDBOS workflowId decisionEventKey (millisDuration 0)
      pure (object ["workflow_id" .= text, "decision" .= (either (const Nothing) (>>= decodeToText) stored)])
dispatch app MessagesRespondAction request respond = do
  parsed <- readJsonBody request
  case parsed of
    Left err -> respond (badRequest err)
    Right RespondRequest { respondWorkflowId = workflowId, respondDecision = decision } -> do
      _ <- sendWorkflowMessage app.appDBOS (WorkflowId workflowId) (Just approvalTopic) Nothing (encodeWorkflowValue decision)
      respond (textOk "")
dispatch app MessagesRespondAllAction request respond = do
  parsed <- readJsonBody request
  case parsed of
    Left err -> respond (badRequest err)
    Right RespondAllRequest { respondAllDecision = decision } -> do
      listed <- listWorkflowIdsByName app.appDBOS approvalWorkflowName approvalListLimit
      ids <- case listed of
        Left _ -> pure []
        Right workflowIds -> pure workflowIds
      waiting <- filterM (fmap (== Nothing) . readDecision) ids
      _ <-
        sendWorkflowMessages
          app.appDBOS
          [ SendMessage
              { sendDestinationId = workflowId,
                sendMessageBody = encodeWorkflowValue decision,
                sendTopic = Just approvalTopic,
                sendIdempotencyKey = Nothing
              }
            | workflowId <- waiting
          ]
      respond (textOk (pack (show (length waiting))))
  where
    readDecision workflowId = do
      stored <- getWorkflowEvent app.appDBOS workflowId decisionEventKey (millisDuration 0)
      pure (either (const Nothing) (>>= decodeToText) stored)

decodeToText :: SerializedWorkflowValue -> Maybe Text
decodeToText stored = case decodeWorkflowValue "result" (Just stored) of
  Right text -> Just text
  Left _ -> Nothing

workflowIdText :: WorkflowId -> Text
workflowIdText (WorkflowId text) = text

fetchQueueWorkerConcurrency :: DBOS IO -> Text -> IO (Maybe Int)
fetchQueueWorkerConcurrency dbos name = do
  found <- queue dbos name
  pure (either (const Nothing) (>>= (.worker_concurrency)) found)

-- | Start a workflow and return at once; the task is dropped. The id is an
-- idempotency key: starting the same id twice joins the workflow already
-- running rather than failing.
startBackground :: App -> WorkflowKey -> WorkflowId -> IO WorkflowId
startBackground app key workflowId = do
  _ <- async (runDBOSWorkflow app.appDBOS key workflowId Nothing)
  pure workflowId

freshId :: Text -> IO WorkflowId
freshId prefix = (WorkflowId . (prefix <>) . ("-" <>) . UUID.toText) <$> UUID.V4.nextRandom

-- ---------------------------------------------------------------------------
-- Wiring
-- ---------------------------------------------------------------------------

registerAll :: DBOS IO -> IO (Either Error ())
registerAll dbos = do
  results <-
    sequence
      [ registerDBOSWorkflow dbos (newWorkflowKey "ExampleWorkflow") exampleWorkflowBody,
        registerDBOSWorkflow dbos (newWorkflowKey "OrderWorkflow") orderWorkflowBody,
        registerDBOSWorkflow dbos (newWorkflowKey approvalWorkflowName) approvalWorkflowBody,
        registerDBOSWorkflow dbos (newWorkflowKey enqueuedWorkflowName) enqueuedWorkflowBody
      ]
  pure (sequence_ results)

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }

main :: IO ()
main = do
  port <- maybe 8081 id . (>>= readMaybe) <$> lookupEnv "PORT"
  applicationVersion <- maybe "0.1.0" pack <$> lookupEnv "DBOS_APP_VERSION"
  executorId <- maybe "hs-starter-executor" pack <$> lookupEnv "DBOS_EXECUTOR_ID"
  config0 <- configFromEnv "dbos-hs-starter"
  let config = config0 {configAppVersion = Just applicationVersion, configExecutorId = Just executorId}
  dbos <- newDBOS config
  registered <- registerAll dbos
  case registered of
    Left err -> fail (show err)
    Right () -> pure ()
  launched <- launchWithEnvironment dbos isolatedEnvironment
  case launched of
    Left err -> fail (show err)
    Right () -> pure ()
  registeredQueue <-
    registerQueue
      dbos
      demoQueueName
      (defaultQueueOptions {worker_concurrency = Just defaultWorkerConcurrency})
      NeverUpdate
  case registeredQueue of
    Left err -> fail (show err)
    Right _ -> pure ()
  page <- LBS.readFile "app/page.html"
  orderId <- newTVarIO Nothing
  mainThread <- myThreadId
  -- exitSuccess only terminates the calling thread: run from the signal
  -- handler it kills just the handler and the process lingers in warp.
  -- Throwing to the main thread ends the process with a quiet code 0.
  let stop = do
        shutdown dbos
        throwTo mainThread ExitSuccess
  _ <- installHandler sigINT (CatchOnce stop) Nothing
  _ <- installHandler sigTERM (CatchOnce stop) Nothing
  run port (routeTrieMiddleware (starterRouteTrie (dispatch (App dbos orderId page))) notFound)
