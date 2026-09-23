{-# LANGUAGE OverloadedRecordDot #-}

-- | In-memory model of the durable tables a simulation needs: step
-- checkpoints and workflow events. Backed by 'TVar's of 'Map's, so the same
-- code runs under @io-sim@ and @IO@ through the 'MonadSTM' interface. Key
-- scoping and first-write-wins recording mirror the Postgres contract; SQL
-- semantics (conflict clauses, locking reads) stay live-DB tested.
module DBOS.SimDB
  ( SimDB (..),
    newSimDB,
    simEventStore,
    simStepStore,
  )
where

import Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import DBOS.Transact
  ( EventStore (..),
    OperationCheckpoint (..),
    OperationCheckpointResult (..),
    OperationId (..),
    SerializedWorkflowValue,
    StepStore (..),
    WorkflowId (..),
  )
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)

data SimDB m = SimDB
  { simSteps :: TVar m (Map (Text, Int) OperationCheckpoint),
    simEvents :: TVar m (Map (Text, Text) SerializedWorkflowValue)
  }

newSimDB :: MonadSTM m => m (SimDB m)
newSimDB = SimDB <$> newTVarIO Map.empty <*> newTVarIO Map.empty

simStepStore :: MonadSTM m => SimDB m -> StepStore m
simStepStore db =
  StepStore
    { stepFetchResult = \workflowId operationId ->
        Map.lookup (stepKey workflowId operationId) <$> readTVarIO db.simSteps,
      -- First write wins, as @ON CONFLICT DO NOTHING@: a replay never
      -- overwrites the recorded output.
      stepRecordOutput = \workflowId operationId operationName output -> atomically $ do
        let checkpoint =
              OperationCheckpoint
                { checkpointOperationId = operationId,
                  checkpointOperationName = operationName,
                  checkpointStartedAt = Nothing,
                  checkpointCompletedAt = Nothing,
                  checkpointResult = CheckpointOutput output
                }
        steps <- readTVar db.simSteps
        writeTVar db.simSteps (Map.insertWith (\_ existing -> existing) (stepKey workflowId operationId) checkpoint steps)
    }

simEventStore :: MonadSTM m => SimDB m -> EventStore m
simEventStore db =
  EventStore
    { eventGet = \workflowId key ->
        Map.lookup (eventKey workflowId key) <$> readTVarIO db.simEvents,
      eventSet = \workflowId key value -> atomically $ do
        events <- readTVar db.simEvents
        writeTVar db.simEvents (Map.insert (eventKey workflowId key) value events)
    }

stepKey :: WorkflowId -> OperationId -> (Text, Int)
stepKey (WorkflowId workflowId) (OperationId operationId) = (workflowId, operationId)

eventKey :: WorkflowId -> Text -> (Text, Text)
eventKey (WorkflowId workflowId) key = (workflowId, key)
