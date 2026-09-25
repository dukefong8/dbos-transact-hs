{-# LANGUAGE OverloadedStrings #-}

-- | Workflow management operations on the launched instance. Transactional
-- validation and writes remain in the SystemDB class; this layer owns only
-- the launched-instance guard and engine error channel.
module DBOS.Transact.Management
  ( cancelWorkflows,
    resumeWorkflows,
    deleteWorkflows,
    forkWorkflows,
    forkFrom,
  )
where

import DBOS.Prelude
import Data.Text (Text)
import Data.Word (Word64)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Types (Fork, ForkOptions, ForkPoint, WorkflowId)
import DBOS.Transact.Connection (Connection (..), runSystemDB)
import DBOS.Transact.Error qualified as TransactError

cancelWorkflows :: Monad m => Connection m -> [WorkflowId] -> Bool -> m (Either TransactError.Error [WorkflowId])
cancelWorkflows conn workflowIds cancelChildren = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.cancelWorkflows db workflowIds cancelChildren Nothing)
  pure (either (Left . TransactError.ErrorSystemDatabase) Right result)

resumeWorkflows :: Monad m => Connection m -> [WorkflowId] -> Maybe Text -> m (Either TransactError.Error [WorkflowId])
resumeWorkflows conn workflowIds queueName = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.resumeWorkflows db workflowIds queueName Nothing)
  pure (either (Left . TransactError.ErrorSystemDatabase) Right result)

deleteWorkflows :: Monad m => Connection m -> [WorkflowId] -> Bool -> m (Either TransactError.Error Word64)
deleteWorkflows conn workflowIds deleteChildren = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.deleteWorkflows db workflowIds deleteChildren Nothing)
  pure (either (Left . TransactError.ErrorSystemDatabase) Right result)

forkWorkflows :: Monad m => Connection m -> [Fork] -> ForkOptions -> m (Either TransactError.Error [WorkflowId])
forkWorkflows conn forks options = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.forkWorkflows db forks options Nothing)
  pure (either (Left . TransactError.ErrorSystemDatabase) Right result)

forkFrom :: Monad m => Connection m -> [WorkflowId] -> ForkPoint -> ForkOptions -> m (Either TransactError.Error [WorkflowId])
forkFrom conn workflowIds point options = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.forkFrom db workflowIds point options Nothing)
  pure (either (Left . TransactError.ErrorSystemDatabase) Right result)
