-- | Everything a handler needs: the launched instance and the manager
-- reference for fair-queue submits. Split out so 'QueuePatterns.Handler' and
-- the route dispatch can both see it without a cycle.
module QueuePatterns.App (QueuePatternsApp (..)) where

import DBOS.Transact (DBOS, EngineOnly, Executor, WorkflowRef)
import Prelude

data QueuePatternsApp = QueuePatternsApp
  { qpDbos       :: DBOS IO,
    qpExec       :: Executor IO,
    qpFairManager :: WorkflowRef IO EngineOnly,
    qpDebouncer  :: WorkflowRef IO EngineOnly
  }
