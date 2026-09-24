{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Internal workflow runner (Rule 4: plain Haskell, no Bluefin imports).
-- Runs a registered body and records its outcome: @SUCCESS@ with the output,
-- or @ERROR@ with the failure. A missing row is started first; an existing
-- row runs straight into the body, whose steps replay from their
-- checkpoints — which is the whole of crash recovery at this layer. A row
-- that already carries an outcome replays it without running the body, and a
-- row owned by another executor is left alone (@WorkflowClaimLost@):
-- re-running either would overwrite finished work or steal a live claim.
module DBOS.Transact.Workflow
  ( WorkflowRunError (..),
    runWorkflow,
  )
where

import Control.Concurrent.Async (AsyncCancelled (..))
import Control.Exception (AsyncException (..), SomeException, fromException, throwIO, try)
import DBOS.SystemDB.Error (Error (..))
import DBOS.SystemDB.Types (WorkflowStatus (..))
import DBOS.SystemDB.Postgres
  ( Pool,
    WorkflowStartDecision (..),
    fetchWorkflowExecutionRow,
    tryStartWorkflow,
    updateWorkflowOutcome,
  )
import DBOS.Transact.Codec (CodecError (..), decodeWorkflowValue, encodeWorkflowValue)
import DBOS.Transact.Registry (WorkflowRegistry, lookupWorkflow)
import DBOS.Transact.WorkflowExecutionParse (parseWorkflowExecution)
import DBOS.Transact.WorkflowExecutionTypes
  ( ApplicationVersion,
    ExecutorId,
    SerializedWorkflowValue (..),
    WorkflowExecution (..),
    WorkflowId,
    WorkflowName (..),
    WorkflowOutcome (..),
  )
import Data.Text (Text, pack)

data WorkflowRunError
  = WorkflowNotRegistered WorkflowName
  | WorkflowClaimLost WorkflowId
  | WorkflowBodyFailed Text
  deriving stock (Eq, Show)

runWorkflow ::
  Pool ->
  WorkflowRegistry ->
  WorkflowName ->
  WorkflowId ->
  Maybe SerializedWorkflowValue ->
  ExecutorId ->
  ApplicationVersion ->
  IO (Either WorkflowRunError SerializedWorkflowValue)
runWorkflow pool registry name workflowId input executorId applicationVersion =
  case lookupWorkflow name registry of
    Nothing -> pure (Left (WorkflowNotRegistered name))
    Just body -> do
      existing <- fetchWorkflowExecutionRow pool workflowId
      case existing of
        Nothing -> do
          decision <- tryStartWorkflow pool workflowId name input executorId applicationVersion
          case decision of
            StartWorkflow -> execute body
            AwaitWorkflow -> pure (Left (WorkflowClaimLost workflowId))
        Just row -> case parseWorkflowExecution row of
          -- Unreadable rows keep the legacy path: the body runs and its
          -- steps replay from whatever checkpoints decode.
          Left _ -> execute body
          Right execution -> case execution.workflowExecutionOutcome of
            Just (WorkflowSucceeded output) -> pure (Right output)
            Just (WorkflowFailed errValue) -> pure (Left (WorkflowBodyFailed (replayErrorText errValue)))
            _ -> case execution.workflowExecutionStatus of
              Cancelled -> pure (Left (WorkflowClaimLost workflowId))
              _ -> case execution.workflowExecutionExecutor of
                -- tryStart hands out ownership by writing our id; anything
                -- else is another runner's claim (or a queue row waiting for
                -- one) and running it here would steal it.
                Just owner | owner == executorId -> execute body
                _ -> pure (Left (WorkflowClaimLost workflowId))
  where
    execute body = do
      outcome <- try (body pool workflowId input)
      case outcome of
        Right output -> do
          updateWorkflowOutcome pool workflowId executorId Success (Just output) Nothing
          pure (Right output)
        -- Control and infrastructure errors are non-recorded: a cancelled
        -- workflow keeps its PENDING row for a later launch to recover, and
        -- a database outage must not become a permanent ERROR outcome. Only
        -- a failure of the body itself is recorded.
        Left failure
          | Just AsyncCancelled <- fromException failure -> throwIO failure
          | Just (_ :: AsyncException) <- fromException failure -> throwIO failure
          | Just (_ :: Error) <- fromException failure -> throwIO failure
          | otherwise -> do
              let complaint = encodeWorkflowValue (show (failure :: SomeException))
              updateWorkflowOutcome pool workflowId executorId Error Nothing (Just complaint)
              pure (Left (WorkflowBodyFailed (pack (show failure))))

-- | The error text a fresh failure reported, recovered from the stored
-- encoding so a replay reports the same string its first run did. A stored
-- value that no longer decodes falls back to its raw text rather than
-- failing the replay.
replayErrorText :: SerializedWorkflowValue -> Text
replayErrorText stored@(SerializedWorkflowValue errText _) =
  case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
    Right text -> text
    Left _ -> errText
