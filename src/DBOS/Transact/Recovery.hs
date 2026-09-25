{-# LANGUAGE OverloadedStrings #-}

-- | Startup recovery sweep. Rust @recovery.rs@ expresses recovery as one
-- SystemDB call: only rows owned by this executor and stamped with this
-- application version are re-enqueued onto the internal queue.
module DBOS.Transact.Recovery (reenqueueForRecovery) where

import DBOS.Prelude
import Data.Text (Text)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Types (QueueName (..), WorkflowId, internalQueueName)
import DBOS.Transact.Connection (Connection (..), runSystemDB)
import DBOS.Transact.Error qualified as TransactError

reenqueueForRecovery :: Monad m => Connection m -> Text -> Text -> m (Either TransactError.Error [WorkflowId])
reenqueueForRecovery conn executorId applicationVersion = do
  let QueueName recoveryQueue = internalQueueName
  result <-
    runSystemDB conn.connSysdb (\db -> SystemDB.reenqueueForRecovery db [executorId] applicationVersion recoveryQueue)
  pure (either (Left . TransactError.ErrorSystemDatabase) Right result)
