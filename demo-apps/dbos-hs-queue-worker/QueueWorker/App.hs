-- | Everything a handler needs: the launched instance and its executor.
-- Split out so 'QueueWorker.Handler' and the route dispatch can both see it
-- without a cycle.
module QueueWorker.App (QueueWorkerApp (..)) where

import DBOS.Transact (DBOS, Executor)
import Prelude

data QueueWorkerApp = QueueWorkerApp
  { qwDbos :: DBOS IO,
    qwExec :: Executor IO
  }
