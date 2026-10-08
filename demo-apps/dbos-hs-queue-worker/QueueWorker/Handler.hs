{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The queue-worker demo's handlers: every route's work happens here through
-- 'RouteHandler', returning the workflow list (the htmx surface renders it,
-- the JSON surface encodes it). The dispatch in "QueueWorker.Route" picks
-- the runner; the handlers never touch a 'Response'.
module QueueWorker.Handler
  ( getQueueWorkerPage,
    getWorkflows,
    enqueueWorkflow,
  )
where

import Control.Monad.IO.Class (liftIO)
import Data.Int (Int64)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.Transact (DBOS, EngineOnly, Error, WorkflowId (..), decodeWorkflowValue, encodeWorkflowValue, fetchWorkflowStatuses, getWorkflowEvent, listWorkflowIdsByName, millisDuration, newWorkflowKey)
import DBOS.Transact qualified as Transact
import Demo.Http (RouteHandler)
import QueueWorker.App (QueueWorkerApp (..))
import QueueWorker.View (QueueWorkerPage (..), WorkflowStatus (..))
import QueueWorker.Workflows (Progress (..), defaultNumSteps, progressEventKey, workerQueueName, workerWorkflowName)
import Prelude

getQueueWorkerPage :: QueueWorkerApp -> RouteHandler QueueWorkerPage
getQueueWorkerPage app = QueueWorkerPage <$> getWorkflows app

-- | List all workflows and their progress (the Python @list_workflows@).
-- Progress may be missing when the workflow has not started executing yet.
getWorkflows :: QueueWorkerApp -> RouteHandler [WorkflowStatus]
getWorkflows app = liftIO $ do
  listed <- listWorkflowIdsByName app.qwDbos workerWorkflowName listLimit
  let ids = either (const []) id listed
  statuses <- fetchWorkflowStatuses app.qwDbos ids
  let table = [(widText wid, Text.pack (show status)) | (wid, status) <- statuses]
  traverse (readStatus app.qwDbos) table
  where
    widText (WorkflowId t) = t

-- | Enqueue one background workflow (the Python @enqueue_workflow@).
enqueueWorkflow :: QueueWorkerApp -> RouteHandler [WorkflowStatus]
enqueueWorkflow app = do
  _ <- liftIO (enqueueOne app)
  getWorkflows app

-- * Helpers

enqueueOne :: QueueWorkerApp -> IO (Either (Error EngineOnly) ())
enqueueOne app = do
  wid <- WorkflowId . ("worker-" <>) . UUID.toText <$> UUID.V4.nextRandom
  enqueued <-
    Transact.enqueueWorkflow
      app.qwDbos
      (newWorkflowKey workerWorkflowName)
      wid
      (Just (encodeWorkflowValue defaultNumSteps))
      workerQueueName
  pure (voidResult enqueued)
  where
    voidResult (Left err) = Left err
    voidResult (Right _) = Right ()

readStatus :: DBOS IO -> (Text, Text) -> IO WorkflowStatus
readStatus dbos (wid, status) = do
  stored <- getWorkflowEvent dbos (WorkflowId wid) progressEventKey (millisDuration 0)
  let progress :: Maybe Progress
      progress = do
        recorded <- either (const Nothing) id stored
        either (const Nothing) Just (decodeWorkflowValue "result" (Just recorded))
  pure WorkflowStatus
    { wsWorkflowId = wid,
      wsStatus = status,
      wsCompleted = (.progressCompleted) <$> progress,
      wsTotal = (.progressTotal) <$> progress
    }

listLimit :: Int64
listLimit = 200
