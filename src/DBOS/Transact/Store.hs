{-# LANGUAGE OverloadedRecordDot #-}

-- | Durable-store seams (Rule 4: plain Haskell, no Bluefin imports). The
-- engine never touches storage directly: each durable interaction goes
-- through one of these records, polymorphic in the monad. Production
-- instantiates them over a Hasql pool ('postgresStepStore',
-- 'postgresEventStore'); simulations instantiate them over an in-memory
-- model, so the same bodies run headless and deterministically under
-- @io-sim@. Adding a seam here means adding a record, never a typeclass.
module DBOS.Transact.Store
  ( EventStore (..),
    StepStore (..),
  )
where

import DBOS.Transact.OperationCheckpointTypes
  ( OperationCheckpoint,
    OperationId,
    OperationName (..),
  )
import DBOS.Transact.WorkflowExecutionTypes
  ( SerializedWorkflowValue,
    WorkflowId,
  )
import Data.Text (Text)

-- | What 'runStep' needs from storage: fetch a recorded checkpoint, or
-- record a fresh output. The engine decides run-vs-replay; the store only
-- persists.
data StepStore m = StepStore
  { stepFetchResult :: WorkflowId -> OperationId -> m (Maybe OperationCheckpoint),
    stepRecordOutput :: WorkflowId -> OperationId -> OperationName -> SerializedWorkflowValue -> m ()
  }

-- | Workflow events by key: publish and read. Blocking reads compose on top
-- of repeated reads at the engine edge, so the store stays total.
data EventStore m = EventStore
  { eventGet :: WorkflowId -> Text -> m (Maybe SerializedWorkflowValue),
    eventSet :: WorkflowId -> Text -> SerializedWorkflowValue -> m ()
  }
