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
    exampleWorkflowBody,
    orderWorkflowBody,
    approvalWorkflowBody,
    enqueuedWorkflowBody,
    StarterRefs (..),
    registerStarterWorkflows,
  )
where

import Data.Int (Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text, pack)
import Data.Word (Word64)
import Control.Monad.Class.MonadTimer (threadDelay)
import Prelude
import DBOS.SystemDB (Topic (..), millisDuration)
import DBOS.Transact (DBOS, EngineOnly, Error, WorkflowCtx, WorkflowRef, newWorkflowKey, recv, registerDBOSWorkflow, registerDBOSWorkflowRef, runWorkflowStep, setEvent, sleepWorkflowStep)

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

exampleWorkflowBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
exampleWorkflowBody () wctx = do
  first <- stepSleep wctx "step_one" stepDurationMs
  case first of
    Left err -> pure (Left err)
    Right () -> do
      published <- setEvent wctx stepsEventKey (1 :: Int)
      case published of
        Left err -> pure (Left err)
        Right () -> continue wctx
  where
    continue innerWctx = do
      second <- stepSleep innerWctx "step_two" stepDurationMs
      case second of
        Left err -> pure (Left err)
        Right () -> do
          published <- setEvent innerWctx stepsEventKey (2 :: Int)
          case published of
            Left err -> pure (Left err)
            Right () -> do
              third <- stepSleep innerWctx "step_three" stepDurationMs
              case third of
                Left err -> pure (Left err)
                Right () -> do
                  lastPublished <- setEvent innerWctx stepsEventKey (3 :: Int)
                  pure (lastPublished >> Right "Workflow completed")

orderWorkflowBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
orderWorkflowBody () wctx = do
  published <- mapM publish (zip [1 ..] orderKeys)
  pure (sequence_ published >> Right "Order complete")
  where
    publish (stage, key) = do
      slept <- sleepWorkflowStep wctx (millisDuration orderStepMs)
      case slept of
        Left err -> pure (Left err)
        Right () -> setEvent wctx key (key <> " at step " <> pack (show (stage :: Int)))

approvalWorkflowBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
approvalWorkflowBody () wctx = do
  decision <- recv wctx (Just approvalTopic) (millisDuration approvalTimeoutMs)
  case (decision :: Either (Error EngineOnly) (Maybe Text)) of
    Left err -> pure (Left err)
    Right stored -> do
      let outcome = fromMaybe "expired" stored
      published <- setEvent wctx decisionEventKey outcome
      pure (published >> Right outcome)

enqueuedWorkflowBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
enqueuedWorkflowBody () wctx = do
  slept <- sleepWorkflowStep wctx (millisDuration queueSleepMs)
  pure (slept >> Right "Enqueued workflow completed")

stepSleep :: WorkflowCtx exec IO -> Text -> Word64 -> IO (Either (Error EngineOnly) ())
stepSleep wctx name milliseconds =
  runWorkflowStep wctx name (const (threadDelay (fromIntegral milliseconds * 1000) >> pure ()))

-- * Registration

registerStarterWorkflows :: DBOS IO -> IO (Either (Error EngineOnly) StarterRefs)
registerStarterWorkflows dbos = do
  example <- registerDBOSWorkflowRef dbos (newWorkflowKey "ExampleWorkflow") exampleWorkflowBody
  order <- registerDBOSWorkflowRef dbos (newWorkflowKey "OrderWorkflow") orderWorkflowBody
  approval <- registerDBOSWorkflowRef dbos (newWorkflowKey approvalWorkflowName) approvalWorkflowBody
  enqueued <- registerDBOSWorkflow dbos (newWorkflowKey enqueuedWorkflowName) enqueuedWorkflowBody
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
