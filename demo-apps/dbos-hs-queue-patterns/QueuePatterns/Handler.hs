{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The queue-patterns demo's handlers: fair-queue submits (with the tenant
-- as partition key), rate-limited submits, and the per-tab list. Every
-- route's work happens here through 'RouteHandler'; the dispatch in
-- "QueuePatterns.Route" picks the runner.
module QueuePatterns.Handler
  ( getPatternsPage,
    getPatternRows,
    submitFair,
    submitRateLimited,
    submitDebounced,
  )
where

import Control.Monad.Except (ExceptT (..), runExceptT)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString.Lazy qualified as LBS
import Data.Int (Int64)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding (encodeUtf8)
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.Transact (DBOS, Debouncer (..), Duration, Enqueue (..), EngineOnly, Error, StartOptions (..), WorkflowId (..), WorkflowStatus, debounce, debouncerNew, decodeWorkflowValue, encodeWorkflowValue, enqueueWorkflow, enqueueNew, fetchWorkflowStatuses, getWorkflowEvent, listWorkflowIdsByName, millisDuration, newWorkflowKey, secondsDuration, startWorkflowRef, startOptionsDefault)
import Demo.Http (RouteHandler, throwRouteError)
import Network.HTTP.Types (status500)
import QueuePatterns.App (QueuePatternsApp (..))
import QueuePatterns.View (PatternRow (..), PatternsPage (..), PatternsTab (..))
import QueuePatterns.Workflows (debouncerQueueName, debouncerWorkflowName, fairManagerWorkflowName, partitionedQueueName, rateLimitedQueueName, rateLimitedWorkflowName, tenantEventKey)
import Prelude

-- * Pages and lists

getPatternsPage :: QueuePatternsApp -> PatternsTab -> RouteHandler PatternsPage
getPatternsPage app tab = do
  rows <- getPatternRows app tab
  pure PatternsPage {patternsTab = tab, patternsRows = rows, patternsStats = stats rows}

getPatternRows :: QueuePatternsApp -> PatternsTab -> RouteHandler [PatternRow]
getPatternRows app tab = liftIO $ do
  listed <- listWorkflowIdsByName app.qpDbos (tabWorkflow tab) listLimit
  let ids = either (const []) id listed
  statuses <- fetchWorkflowStatuses app.qpDbos ids
  traverse (readRow app.qpDbos) statuses

-- * Submits

-- | Enqueue the concurrency manager onto the partitioned queue with the
-- tenant as its partition key (the Python @SetEnqueueOptions@). The manager
-- then enforces the per-partition limit by routing through the
-- concurrency-limited queue.
submitFair :: QueuePatternsApp -> Text -> RouteHandler [PatternRow]
submitFair app tenant = do
  _ <- liftIO (runExceptT (startManager app tenant))
  getPatternRows app FairQueueTab

-- | Enqueue one rate-limited workflow.
submitRateLimited :: QueuePatternsApp -> RouteHandler [PatternRow]
submitRateLimited app = do
  _ <- liftIO (enqueueRateLimited app)
  getPatternRows app RateLimitedTab

-- | Debounce the debouncer workflow for a tenant: waits 5s past the last
-- input, then runs with the last input submitted.
submitDebounced :: QueuePatternsApp -> Text -> Text -> RouteHandler [PatternRow]
submitDebounced app tenant input = do
  outcome <- liftIO (debounce app.qpDbos app.qpDebouncer debouncerDef tenant debouncePeriod (Just (encodeWorkflowValue (tenant, input))))
  case outcome of
    Left err -> throwRouteError status500 (textBody ("debounce failed: " <> Text.pack (show err)))
    Right _ -> getPatternRows app DebouncerTab

-- * Helpers

startManager :: QueuePatternsApp -> Text -> ExceptT (Error EngineOnly) IO WorkflowId
startManager app tenant = do
  wid <- liftIO (WorkflowId . ("fair-" <>) . UUID.toText <$> UUID.V4.nextRandom)
  let input = Just (encodeWorkflowValue tenant)
      options = startOptionsDefault {startWorkflowId = Just wid, startQueue = Just ((enqueueNew partitionedQueueName) {partitionKey = Just tenant})}
  _ <- ExceptT (startWorkflowRef app.qpExec app.qpFairManager options input)
  pure wid

enqueueRateLimited :: QueuePatternsApp -> IO (Either (Error EngineOnly) ())
enqueueRateLimited app = do
  wid <- WorkflowId . ("ratelimit-" <>) . UUID.toText <$> UUID.V4.nextRandom
  enqueued <-
    enqueueWorkflow
      app.qpDbos
      (newWorkflowKey rateLimitedWorkflowName)
      wid
      Nothing
      rateLimitedQueueName
  pure (voidResult enqueued)
  where
    voidResult (Left err) = Left err
    voidResult (Right _) = Right ()

readRow :: DBOS IO -> (WorkflowId, WorkflowStatus) -> IO PatternRow
readRow dbos (WorkflowId wid, status) = do
  stored <- getWorkflowEvent dbos (WorkflowId wid) tenantEventKey (millisDuration 0)
  pure PatternRow
    { prWorkflowId = wid,
      prStatus = Text.pack (show status),
      prTenant = do
        recorded <- either (const Nothing) id stored
        either (const Nothing) Just (decodeWorkflowValue "result" (Just recorded))
    }

tabWorkflow :: PatternsTab -> Text
tabWorkflow FairQueueTab = fairManagerWorkflowName
tabWorkflow RateLimitedTab = rateLimitedWorkflowName
tabWorkflow DebouncerTab = debouncerWorkflowName

-- | The demo's debouncer: the debounced workflow runs on its own queue, with
-- no max-wait cap (the Python demo passes no timeout either).
debouncerDef :: Debouncer
debouncerDef = debouncerNew {debouncerQueueName = Just debouncerQueueName}

-- | How long the debouncer waits past the last input (the Python 5s period).
debouncePeriod :: Duration
debouncePeriod = secondsDuration 5

stats :: [PatternRow] -> (Int, Int, Int)
stats rows = (count "ENQUEUED", count "PENDING", count "SUCCESS")
  where
    count want = length [() | r <- rows, Text.toUpper r.prStatus == want]

textBody :: Text -> LBS.ByteString
textBody = LBS.fromStrict . encodeUtf8

listLimit :: Int64
listLimit = 200
