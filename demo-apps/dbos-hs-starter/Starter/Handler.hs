{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The starter's handlers, mirroring the widget app's split: every route's
-- work happens here through 'RouteHandler', returning a view model (the htmx
-- surface renders it) or a plain value (the JSON surface encodes it). The
-- dispatch in "Starter.Route" picks the runner and parses the request
-- surface; the handlers never touch a 'Response'.
module Starter.Handler
  ( getPageView,
    getWorkflowProgress,
    startWorkflow,
    getQueueStatus,
    enqueueWorkflows,
    applyConcurrency,
    getEventsStatus,
    startOrder,
    readEvent,
    getApprovals,
    startApproval,
    respondApproval,
    respondAllApprovals,
    workflowIdText,
  )
where

import Control.Monad (replicateM_)
import Control.Monad.IO.Class (liftIO)
import DBOS.Prelude
import DBOS.SystemDB (Change (..), SendMessage (..), millisDuration)
import DBOS.Transact
  ( DBOS,
    EngineOnly,
    Error,
    Queue (..),
    QueueChange (..),
    QueueConflict (..),
    QueueOptions (..),
    SerializedWorkflowValue (..),
    StartOptions (..),
    WorkflowId (..),
    WorkflowKey,
    decodeWorkflowValue,
    WorkflowRef,
    defaultQueueChange,
    enqueueDBOSWorkflow,
    encodeWorkflowValue,
    fetchWorkflowStatuses,
    getWorkflowEvent,
    listWorkflowIdsByName,
    newWorkflowKey,
    queue,
    startDBOSWorkflowRef,
    startOptionsDefault,
    sendWorkflowMessage,
    sendWorkflowMessages,
    updateQueue,
  )
import Data.Int (Int64)
import Data.Map.Strict qualified as Map
import Data.Text (Text, pack)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Demo.Http (RouteHandler)
import Starter.App (StarterApp (..))
import Starter.View
  ( ApprovalRow (..),
    EventKey (..),
    EventsStatus (..),
    PageView (..),
    QueueStatus (..),
    ReadResult (..),
    WorkflowProgress (..),
  )
import Starter.Workflows

-- * Pages

getPageView :: StarterApp -> Text -> Maybe Text -> RouteHandler PageView
getPageView app tab taskId = do
  queue <- getQueueStatus app
  events <- getEventsStatus app
  approvals <- getApprovals app
  progress <- case taskId of
    Nothing -> pure WorkflowProgress {wpTaskId = Nothing, wpLastStep = 0, wpFinished = False}
    Just task -> getWorkflowProgress app task
  pure
    PageView
      { pvTab = tab,
        pvQueue = queue,
        pvEvents = events,
        pvProgress = progress,
        pvApprovals = approvals
      }

-- * Workflows tab

getWorkflowProgress :: StarterApp -> Text -> RouteHandler WorkflowProgress
getWorkflowProgress app task = do
  step <- liftIO (lastStepOf app task)
  pure WorkflowProgress {wpTaskId = Just task, wpLastStep = step, wpFinished = step >= 3}

lastStepOf :: StarterApp -> Text -> IO Int
lastStepOf app task = do
  stored <- getWorkflowEvent app.staDbos (WorkflowId task) stepsEventKey (millisDuration 0)
  pure $ case stored of
    Right (Just recorded) -> case decodeWorkflowValue "result" (Just recorded) of
      Right step -> step
      Left _ -> 0
    _ -> 0

startWorkflow :: StarterApp -> Text -> RouteHandler ()
startWorkflow app taskId = do
  _ <- liftIO (startBackground app app.staRefs.starterExampleRef (WorkflowId taskId))
  pure ()

-- * Queues tab

getQueueStatus :: StarterApp -> RouteHandler QueueStatus
getQueueStatus app = do
  workerConcurrency <- liftIO (fetchQueueWorkerConcurrency app.staDbos demoQueueName)
  listed <- liftIO (listWorkflowIdsByName app.staDbos enqueuedWorkflowName queueListLimit)
  let queuedIds = either (const []) id listed
  statuses <- liftIO (fetchWorkflowStatuses app.staDbos queuedIds)
  pure
    QueueStatus
      { qsWorkerConcurrency = maybe defaultWorkerConcurrency id workerConcurrency,
        qsCounts = Map.toList (Map.fromListWith (+) [(Text.toUpper (pack (show status)), 1) | (_, status) <- statuses])
      }

enqueueWorkflows :: StarterApp -> RouteHandler ()
enqueueWorkflows app =
  liftIO $
    replicateM_ enqueueBatchSize $ do
      workflowId <- freshId "queued"
      _ <- enqueueDBOSWorkflow app.staDbos (newWorkflowKey enqueuedWorkflowName) workflowId Nothing demoQueueName
      pure ()

applyConcurrency :: StarterApp -> Int -> RouteHandler ()
applyConcurrency app requested = do
  _ <-
    liftIO
      ( updateQueue
          app.staDbos
          demoQueueName
          (defaultQueueChange {worker_concurrency = Set (Just (max 1 requested))})
      )
  pure ()

-- * Events tab

getEventsStatus :: StarterApp -> RouteHandler EventsStatus
getEventsStatus app = do
  current <- liftIO (readTVarIO app.staOrderId)
  keys <- liftIO (traverse (readKey current) orderKeys)
  pure EventsStatus {esWorkflowId = workflowIdText <$> current, esKeys = keys}
  where
    readKey Nothing key = pure EventKey {ekKey = key, ekValue = Nothing}
    readKey (Just workflowId) key = do
      stored <- getWorkflowEvent app.staDbos workflowId key (millisDuration 0)
      pure EventKey {ekKey = key, ekValue = either (const Nothing) (>>= decodeToText) stored}

startOrder :: StarterApp -> RouteHandler WorkflowId
startOrder app = do
  workflowId <- liftIO (startBackground app app.staRefs.starterOrderRef =<< freshId "order")
  liftIO (atomically (writeTVar app.staOrderId (Just workflowId)))
  pure workflowId

readEvent :: StarterApp -> Text -> RouteHandler ReadResult
readEvent app key = do
  current <- liftIO (readTVarIO app.staOrderId)
  case current of
    Nothing -> pure ReadResult {rrKey = key, rrValue = Nothing, rrWaitedMs = 0}
    Just workflowId -> do
      started <- liftIO getMonotonicTimeNSec
      stored <- liftIO (getWorkflowEvent app.staDbos workflowId key (millisDuration eventReadTimeoutMs))
      finished <- liftIO getMonotonicTimeNSec
      pure
        ReadResult
          { rrKey = key,
            rrValue = either (const Nothing) (>>= decodeToText) stored,
            rrWaitedMs = fromIntegral ((finished - started) `div` 1000000)
          }

-- * Messages tab

getApprovals :: StarterApp -> RouteHandler [ApprovalRow]
getApprovals app = do
  listed <- liftIO (listWorkflowIdsByName app.staDbos approvalWorkflowName approvalListLimit)
  let ids = either (const []) id listed
  liftIO (traverse readApproval ids)
  where
    readApproval workflowId@(WorkflowId text) = do
      stored <- getWorkflowEvent app.staDbos workflowId decisionEventKey (millisDuration 0)
      pure ApprovalRow {arWorkflowId = text, arDecision = either (const Nothing) (>>= decodeToText) stored}

startApproval :: StarterApp -> RouteHandler WorkflowId
startApproval app = liftIO (startBackground app app.staRefs.starterApprovalRef =<< freshId "approval")

respondApproval :: StarterApp -> Text -> Text -> RouteHandler ()
respondApproval app workflowId decision = do
  _ <- liftIO (sendWorkflowMessage app.staDbos (WorkflowId workflowId) (Just approvalTopic) Nothing (encodeWorkflowValue decision))
  pure ()

respondAllApprovals :: StarterApp -> Text -> RouteHandler Int
respondAllApprovals app decision = do
  rows <- getApprovals app
  let waiting = [row | row <- rows, row.arDecision == Nothing]
  _ <-
    liftIO
      ( sendWorkflowMessages
          app.staDbos
          [ SendMessage
              { sendDestinationId = WorkflowId row.arWorkflowId,
                sendMessageBody = encodeWorkflowValue decision,
                sendTopic = Just approvalTopic,
                sendIdempotencyKey = Nothing
              }
            | row <- waiting
          ]
      )
  pure (length waiting)

-- * Helpers

decodeToText :: SerializedWorkflowValue -> Maybe Text
decodeToText stored = case decodeWorkflowValue "result" (Just stored) of
  Right text -> Just text
  Left _ -> Nothing

workflowIdText :: WorkflowId -> Text
workflowIdText (WorkflowId text) = text

fetchQueueWorkerConcurrency :: DBOS IO -> Text -> IO (Maybe Int)
fetchQueueWorkerConcurrency dbos name = do
  found <- queue dbos name
  pure $ case found of
    Right (Just registered) -> registered.worker_concurrency
    _ -> Nothing

-- | Start a workflow and return at once; the task is dropped. The id is an
-- idempotency key: starting the same id twice joins the workflow already
-- running rather than failing.
startBackground :: StarterApp -> WorkflowRef IO EngineOnly -> WorkflowId -> IO WorkflowId
startBackground app ref (WorkflowId widText) = do
  _ <- startDBOSWorkflowRef app.staDbos ref (startOptionsDefault {startWorkflowId = Just widText}) Nothing
  pure (WorkflowId widText)

freshId :: Text -> IO WorkflowId
freshId prefix = (WorkflowId . (prefix <>) . ("-" <>) . UUID.toText) <$> UUID.V4.nextRandom
