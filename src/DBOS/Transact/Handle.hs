{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | A running or finished workflow, by id. Mirrors Rust @handle.rs@: every
-- reference agrees on the surface — the id, the result, the status — and
-- splits the implementation the same way. A handle to a workflow running in
-- this process awaits the running task directly; a handle to one running
-- elsewhere, or to one that finished before this process started, polls the
-- database. Dropping a handle stops watching, never the workflow.
--
-- The local-task await and the durable parent-side @DBOS.getResult@
-- checkpoint are L2 engine work (NOTE): this module polls the database,
-- which is the whole of the polling provenance and the whole of what
-- management and client surfaces hand back.
module DBOS.Transact.Handle
  ( -- * Handle
    WorkflowHandle (..),
    Provenance (..),
    pollingHandle,
    handleWorkflowId,
    handleStatus,
    handleResult,
  )
where

import DBOS.Prelude
import Data.Text (Text)
import Control.Monad.Class.MonadTime (MonadTime)
import Control.Monad.Class.MonadTimer (MonadDelay)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (AwaitedOutcome (..), Serialization (..), SerializedWorkflowValue (..), WorkflowId (..), WorkflowStatus)
import DBOS.Transact.Connection (Connection (..), runSystemDB)
import DBOS.Transact.Error qualified as TransactError

-- | Where this handle's result comes from.
data Provenance
  = -- | The workflow runs elsewhere, or already finished: the database is
    -- the only witness. Whether the minting call saw the row decides what
    -- an absent row means.
    Polling {fail_if_missing :: Bool}
  deriving stock (Eq, Show)

-- | A running or finished workflow, by id. Fields keep the Rust spelling:
-- @workflow_id@ names the workflow; the connection serves the reads and
-- carries the poll interval the handle watches at.
data WorkflowHandle m = WorkflowHandle
  { conn :: Connection m,
    workflow_id :: Text,
    provenance :: Provenance
  }

-- | A handle over a workflow some other execution owns. Takes a connection
-- rather than an executor, which is what lets a client hand one back.
pollingHandle :: Connection m -> Text -> Bool -> WorkflowHandle m
pollingHandle conn workflowId failIfMissing =
  WorkflowHandle
    { conn = conn,
      workflow_id = workflowId,
      provenance = Polling failIfMissing
    }

-- | The workflow's id.
handleWorkflowId :: WorkflowHandle m -> Text
handleWorkflowId handle = handle.workflow_id

-- | The workflow's status, as its row records it right now. An unknown id
-- reports the absence, because a single read has nothing to wait for.
handleStatus :: Monad m => WorkflowHandle m -> m (Either TransactError.Error (Maybe WorkflowStatus))
handleStatus handle = do
  result <- runSystemDB handle.conn.connSysdb (\db -> SystemDB.getWorkflow db (WorkflowId handle.workflow_id))
  pure $ case result of
    Left err -> Left (TransactError.ErrorSystemDatabase err)
    Right Nothing -> Right Nothing
    Right (Just record) -> Right (Just record.workflowRecordStatus)

-- | Waits for the workflow to finish and returns what it returned. Polls
-- the database: the local-task await is future engine work.
handleResult :: (MonadDelay m, MonadTime m) => WorkflowHandle m -> m (Either TransactError.Error (Maybe SerializedWorkflowValue))
handleResult handle = do
  awaited <-
    runSystemDB handle.conn.connSysdb (\db -> SystemDB.awaitWorkflowResult db (WorkflowId handle.workflow_id) handle.conn.connOutcomePollInterval failMissing)
  pure $ case awaited of
    Left err -> Left (TransactError.ErrorSystemDatabase err)
    Right (AwaitedSucceeded output serialization) ->
      Right (SerializedWorkflowValue <$> output <*> pure (Serialization <$> serialization))
    Right (AwaitedFailed message _) ->
      Left (TransactError.ErrorWorkflowFailed handle.workflow_id message)
    Right AwaitedCancelled ->
      Left
        ( TransactError.ErrorSystemDatabase
            (SystemDBError.WorkflowCancelled {workflowId = handle.workflow_id})
        )
    Right (AwaitedParked attempts) ->
      Left
        ( TransactError.ErrorSystemDatabase
            ( SystemDBError.ErrorMaxRecoveryAttemptsExceeded
                { workflowId = handle.workflow_id,
                  limit = attempts
                }
            )
        )
  where
    failMissing = case handle.provenance of
      Polling flag -> flag
