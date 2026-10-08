{-# LANGUAGE OverloadedStrings #-}

-- | The starter's workflow bodies and the protocol constants the UI and the
-- handlers share (the Rust starter's @main.rs@ workflow section). Durations
-- are shorter than the Rust original so the E2E gate stays fast; every
-- behavior is the same.
module Starter.Workflows
  ( -- * Protocol constants
    stepDurationMs,
    orderStepMs,
    queueSleepMs,
    eventReadTimeoutMs,
    approvalTimeoutMs,
    stepsEventKey,
    orderKeys,
    approvalTopic,
    approvalWorkflowName,
    enqueuedWorkflowName,
    decisionEventKey,
    demoQueueName,
    defaultWorkerConcurrency,
    enqueueBatchSize,
    approvalListLimit,
    queueListLimit,

    -- * Bodies
    exampleWorkflow,
    orderWorkflow,
    approvalWorkflow,
    enqueuedWorkflow,
    StarterRefs (..),
    registerStarterWorkflows,
  )
where

import Control.Monad (void)
import Control.Monad.Class.MonadTimer (threadDelay)
import Control.Monad.Except (ExceptT (..), runExceptT)
import Data.Int (Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text, pack)
import Data.Word (Word64)
import DBOS.SystemDB (Topic (..), millisDuration)
import DBOS.Transact (DBOS, EngineOnly, Error, WorkflowCtx, WorkflowRef, newWorkflowKey, recv, registerWorkflow, registerWorkflowRef, runStep, setEvent, sleepStep)
import Prelude

-- * Durations and keys

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

-- * Bodies

exampleWorkflow :: forall exec. WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
exampleWorkflow wctx = runExceptT $ do
  ExceptT (stepSleep wctx "step_one" stepDurationMs)
  ExceptT (setEvent wctx stepsEventKey (1 :: Int))
  ExceptT (stepSleep wctx "step_two" stepDurationMs)
  ExceptT (setEvent wctx stepsEventKey (2 :: Int))
  ExceptT (stepSleep wctx "step_three" stepDurationMs)
  ExceptT (setEvent wctx stepsEventKey (3 :: Int))
  pure "Workflow completed"

orderWorkflow :: forall exec. WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
orderWorkflow wctx = runExceptT $ do
  mapM_ publish (zip [1 ..] orderKeys)
  pure "Order complete"
  where
    publish :: (Int, Text) -> ExceptT (Error EngineOnly) IO ()
    publish (stage, key) = do
      ExceptT (sleepStep wctx (millisDuration orderStepMs))
      ExceptT (setEvent wctx key (key <> " at step " <> pack (show stage)))

approvalWorkflow :: forall exec. WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
approvalWorkflow wctx = runExceptT $ do
  decision <- ExceptT (recv wctx (Just approvalTopic) (millisDuration approvalTimeoutMs))
  let outcome = fromMaybe "expired" decision
  ExceptT (setEvent wctx decisionEventKey outcome)
  pure outcome

enqueuedWorkflow :: forall exec. WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
enqueuedWorkflow wctx = runExceptT $ do
  ExceptT (sleepStep wctx (millisDuration queueSleepMs))
  pure "Enqueued workflow completed"

stepSleep :: WorkflowCtx exec IO -> Text -> Word64 -> IO (Either (Error EngineOnly) ())
stepSleep wctx name milliseconds =
  runStep wctx name (const (void (threadDelay (fromIntegral milliseconds * 1000))))

-- * Registration

registerStarterWorkflows :: DBOS IO -> IO (Either (Error EngineOnly) StarterRefs)
registerStarterWorkflows dbos = do
  example <- registerWorkflowRef dbos (newWorkflowKey "ExampleWorkflow") (\() -> exampleWorkflow)
  order <- registerWorkflowRef dbos (newWorkflowKey "OrderWorkflow") (\() -> orderWorkflow)
  approval <- registerWorkflowRef dbos (newWorkflowKey approvalWorkflowName) (\() -> approvalWorkflow)
  enqueued <- registerWorkflow dbos (newWorkflowKey enqueuedWorkflowName) (\() -> enqueuedWorkflow)
  pure (StarterRefs <$> example <*> order <*> approval <* enqueued)

-- | The refs the handlers start through. Starting by ref persists the
-- workflow row before the HTTP response returns (the oracle's
-- @SetWorkflowID@ shape), so a crash right after the response still
-- recovers the launch.
data StarterRefs = StarterRefs
  { starterExampleRef  :: WorkflowRef IO EngineOnly,
    starterOrderRef    :: WorkflowRef IO EngineOnly,
    starterApprovalRef :: WorkflowRef IO EngineOnly
  }
