{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | The queue-worker demo's workflow bodies, ported from the Python
-- queue-worker's @worker.py@.
--
-- The background workflow runs a number of steps, periodically reporting its
-- progress as an event the server queries to render progress bars. Each loop
-- iteration is one step under the same name (the engine tells repetitions
-- apart by sequence, the way the widget store's dispatch ticks do), so a
-- crash restarts from the first untaken step rather than from zero.
module QueueWorker.Workflows
  ( -- * Names and keys
    workerWorkflowName,
    workerQueueName,
    progressEventKey,
    defaultNumSteps,

    -- * Progress payload
    Progress (..),

    -- * Bodies
    workerWorkflow,
  )
where

import Control.Monad.Class.MonadTimer (threadDelay)
import Control.Monad.Except (ExceptT (..), runExceptT)
import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, (.:), (.=))
import Data.Text (Text, pack)
import DBOS.Transact (EngineOnly, Error, StepCtx, WorkflowCtx, logInfo, runStep, setEvent)
import Prelude

-- * Names and keys (worker.py / server.py)

-- | The background workflow the server enqueues (@worker.py@).
workerWorkflowName :: Text
workerWorkflowName = "workflow"

-- | The queue the server submits workflows to and the worker drains.
workerQueueName :: Text
workerQueueName = "workflow-queue"

-- | The key the workflow publishes its progress under for the list endpoint.
progressEventKey :: Text
progressEventKey = "workflow_progress"

-- | How many steps an enqueued workflow runs (the Python server's
-- @num_steps = 10@).
defaultNumSteps :: Int
defaultNumSteps = 10

-- * Progress payload (server.py @WF_PROGRESS_KEY@)

-- | What the list endpoint reads: completed steps out of the total.
data Progress = Progress
  { progressCompleted :: Int,
    progressTotal     :: Int
  }
  deriving stock (Eq, Show)

instance ToJSON Progress where
  toJSON p = object ["steps_completed" .= p.progressCompleted, "num_steps" .= p.progressTotal]

instance FromJSON Progress where
  parseJSON = withObject "Progress" $ \o -> Progress <$> o .: "steps_completed" <*> o .: "num_steps"

-- * Bodies

-- | Run steps, reporting progress after each (the @workflow@). The @ExceptT@
-- do-block is the oracle's sequential flow: a failed step ends the workflow
-- and recovery resumes it.
workerWorkflow ::
  forall exec.
  Int ->
  WorkflowCtx exec IO ->
  IO (Either (Error EngineOnly) ())
workerWorkflow numSteps wctx = runExceptT $ do
  ExceptT (setEvent wctx progressEventKey (Progress 0 numSteps))
  go 0
  where
    go :: Int -> ExceptT (Error EngineOnly) IO ()
    go i
      | i >= numSteps = pure ()
      | otherwise = do
          ExceptT (runStep wctx "step" (runOne i))
          ExceptT (setEvent wctx progressEventKey (Progress (i + 1) numSteps))
          go (i + 1)
    runOne :: Int -> StepCtx exec IO -> IO ()
    runOne i sctx = do
      logInfo sctx ("Step " <> pack (show i) <> " completed!")
      threadDelay stepDelay

-- | One step's simulated work (the Python @time.sleep(1)@).
stepDelay :: Int
stepDelay = 1000000
