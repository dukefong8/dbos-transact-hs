{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | The queue-patterns demo's workflow bodies, ported from the Python
-- queue-patterns' @main.py@: fair queueing, rate limiting, and debouncing
-- (the debouncer rides the facade seam of @docs/adr/0028-transact-debouncer.md@).
--
-- Fair queueing pairs two queues: the handler enqueues a concurrency-manager
-- workflow onto the partitioned queue with the tenant as its partition key,
-- and the manager enqueues the real work onto the concurrency-limited queue
-- and awaits it. At most five workflows run concurrently, at most one per
-- tenant.
module QueuePatterns.Workflows
  ( -- * Names and keys
    fairManagerWorkflowName,
    fairWorkflowName,
    rateLimitedWorkflowName,
    debouncerWorkflowName,
    debouncerQueueName,
    concurrencyQueueName,
    partitionedQueueName,
    rateLimitedQueueName,
    tenantEventKey,
    debouncerInputKey,
    fairWorkMs,

    -- * Bodies
    fairQueueWorkflow,
    fairQueueConcurrencyManager,
    rateLimitedQueueWorkflow,
    debouncerWorkflow,
  )
where

import Control.Monad.Except (ExceptT (..), runExceptT)
import Data.Text (Text)
import DBOS.Transact (EngineOnly, Error, StartOptions (..), WorkflowCtx, WorkflowRef, awaitChild, queueRequestNew, millisDuration, setEvent, sleepStep, startChildWorkflow, startOptionsDefault)
import Prelude

-- * Names and keys (main.py)

-- | The per-tenant gate the handler enqueues (@fair_queue_concurrency_manager@).
fairManagerWorkflowName :: Text
fairManagerWorkflowName = "fair_queue_concurrency_manager"

-- | The fairly-queued work itself (@fair_queue_workflow@).
fairWorkflowName :: Text
fairWorkflowName = "fair_queue_workflow"

-- | The rate-limited work (@rate_limited_queue_workflow@).
rateLimitedWorkflowName :: Text
rateLimitedWorkflowName = "rate_limited_queue_workflow"

-- | The debounced work (@debouncer_workflow@): runs with the last input
-- submitted for its tenant, 5s after inputs go quiet.
debouncerWorkflowName :: Text
debouncerWorkflowName = "debouncer_workflow"

-- | At most five fairly-queued workflows run concurrently.
concurrencyQueueName :: Text
concurrencyQueueName = "concurrency-queue"

-- | One manager at a time per tenant.
partitionedQueueName :: Text
partitionedQueueName = "partitioned-queue"

-- | At most two workflows start per ten seconds.
rateLimitedQueueName :: Text
rateLimitedQueueName = "rate-limited-queue"

-- | The queue the debounced workflow runs on once released.
debouncerQueueName :: Text
debouncerQueueName = "debouncer-queue"

-- | The key the manager publishes its tenant under, so the list endpoint can
-- show it. The Python demo reads the row's @queue_partition_key@; the facade
-- exposes no row reads to demos, so the tenant travels as an event instead.
tenantEventKey :: Text
tenantEventKey = "tenant_id"

-- | The key the debounced workflow publishes the input it ran with under,
-- so the list endpoint can show last-input-wins (the Python demo reads the
-- row's input args; the facade exposes no row reads to demos, so the input
-- travels as an event beside the tenant).
debouncerInputKey :: Text
debouncerInputKey = "debouncer_input"

-- | How long the demo work sleeps (the Python @time.sleep(5)@), durable so a
-- restart resumes the wait rather than restarting it.
fairWorkMs :: Int
fairWorkMs = 5000

-- * Bodies

-- | Fairly-queued work: at most five run concurrently, at most one per
-- tenant (the concurrency manager enforces the second half).
fairQueueWorkflow ::
  forall exec.
  WorkflowCtx exec IO ->
  IO (Either (Error EngineOnly) Text)
fairQueueWorkflow wctx = runExceptT $ do
  ExceptT (sleepStep wctx (millisDuration (fromIntegral fairWorkMs)))
  pure "fair workflow completed"

-- | The concurrency manager: publish the tenant for the list endpoint, then
-- enqueue the real work on the non-partitioned queue and await its result to
-- enforce the global flow-control limit.
fairQueueConcurrencyManager ::
  forall exec.
  WorkflowRef IO EngineOnly ->
  Text ->
  WorkflowCtx exec IO ->
  IO (Either (Error EngineOnly) Text)
fairQueueConcurrencyManager fairRef tenant wctx = runExceptT $ do
  ExceptT (setEvent wctx tenantEventKey tenant)
  handle <- ExceptT (startChildWorkflow wctx fairRef (startOptionsDefault {startQueue = Just (queueRequestNew concurrencyQueueName)}) Nothing)
  _ <- ExceptT (awaitChild wctx handle)
  pure "manager completed"

-- | Rate-limited work: no more than two start per ten seconds.
rateLimitedQueueWorkflow ::
  forall exec.
  WorkflowCtx exec IO ->
  IO (Either (Error EngineOnly) Text)
rateLimitedQueueWorkflow wctx = runExceptT $ do
  ExceptT (sleepStep wctx (millisDuration (fromIntegral fairWorkMs)))
  pure "rate-limited workflow completed"

-- | Debounced work: executes with the last input submitted for its tenant.
-- Publishes the tenant and the input first so the list endpoint can show
-- them (the Python demo reads the row's input args; the facade exposes no
-- row reads, so both travel as events).
debouncerWorkflow ::
  forall exec.
  (Text, Text) ->
  WorkflowCtx exec IO ->
  IO (Either (Error EngineOnly) Text)
debouncerWorkflow (tenant, input) wctx = runExceptT $ do
  ExceptT (setEvent wctx tenantEventKey tenant)
  ExceptT (setEvent wctx debouncerInputKey input)
  ExceptT (sleepStep wctx (millisDuration (fromIntegral fairWorkMs)))
  pure "debounced workflow completed"
