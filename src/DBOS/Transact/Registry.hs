{-# LANGUAGE OverloadedRecordDot #-}

-- | Internal workflow registry (Rule 4: plain Haskell, no Bluefin imports).
-- Mirrors @registry.rs@: registration turns a named body into a JSON-in /
-- JSON-out closure the executor can call knowing only a row. Bodies take the
-- pool explicitly — there is no ambient context — plus the workflow id their
-- steps run under and the stored input, if any.
module DBOS.Transact.Registry
  ( DuplicateWorkflowName (..),
    WorkflowBody,
    WorkflowRegistry,
    emptyRegistry,
    lookupWorkflow,
    registerWorkflow,
  )
where

import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import DBOS.SystemDB.Postgres (Pool)
import DBOS.Transact.WorkflowExecutionTypes
  ( SerializedWorkflowValue,
    WorkflowId,
    WorkflowName (..),
  )

type WorkflowBody =
  Pool ->
  WorkflowId ->
  Maybe SerializedWorkflowValue ->
  IO SerializedWorkflowValue

newtype WorkflowRegistry = WorkflowRegistry (Map WorkflowName WorkflowBody)

newtype DuplicateWorkflowName = DuplicateWorkflowName WorkflowName
  deriving stock (Eq, Show)

emptyRegistry :: WorkflowRegistry
emptyRegistry = WorkflowRegistry Map.empty

registerWorkflow ::
  WorkflowName ->
  WorkflowBody ->
  WorkflowRegistry ->
  Either DuplicateWorkflowName WorkflowRegistry
registerWorkflow name body (WorkflowRegistry entries) =
  case Map.lookup name entries of
    Just _ -> Left (DuplicateWorkflowName name)
    Nothing -> Right (WorkflowRegistry (Map.insert name body entries))

lookupWorkflow :: WorkflowName -> WorkflowRegistry -> Maybe WorkflowBody
lookupWorkflow name (WorkflowRegistry entries) = Map.lookup name entries
