{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Everything a durable function can fail with. Mirrors Rust
-- @error.rs@'s @Error@. Grows one variant per module as the engine port
-- reaches it; every variant that exists keeps the Rust name, spelling, and
-- payload. Only 'Config' is needed by the identity module, which is why it
-- is the first.
--
-- Serializability is load-bearing in the oracle (a failed step records the
-- error it failed with, and a replay gives back that error, not a
-- description of it). The Haskell port records errors as rendered text at
-- the step boundary, so the ADT here does not need Aeson instances yet; the
-- note stands so the day a variant is recorded whole, it gets them.
module DBOS.Transact.Error
  ( Error (..),
    renderTransactError,
  )
where

import DBOS.Prelude
import Data.Text (Text, pack)
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (Duration, durationAsMillis)

-- | Mirrors Rust @Error@, minus the generic application payload (the port
-- carries application failures as text until the typed-IO phase).
data Error
  = -- | The configuration could not be used: how the instance, the client or
    -- a queue was set up, not what a call was passed at the point it was
    -- made. Rust spells this @Error::Config@; the prefix is the documented
    -- collision deviation, because the @Config@ type owns that name here.
    ErrorConfig Text
  | -- | An operation requiring a running executor was attempted too early.
    ErrorNotLaunched {operation :: Text}
  | -- | A durable operation that allocates an id was called inside a step.
    InsideStep {operation :: Text}
  | -- | Registration happened after the launch snapshot was taken.
    ErrorAlreadyLaunched {operation :: Text}
  | -- | A workflow identity was registered more than once.
    ErrorAlreadyRegistered {key :: Text}
  | -- | A workflow input, result, or failure could not be encoded.
    ErrorSerialization {what :: Text, message :: Text}
  | -- | A serialized workflow input, result, or failure could not be decoded.
    ErrorDeserialization {what :: Text, message :: Text}
  | -- | The SystemDB rejected an engine operation.
    ErrorSystemDatabase SystemDBError.Error
  | -- | A previously recorded operation failed with an untyped message.
    StepFailed {step :: Text, message :: Text}
  | -- | A workflow key did not exist in the executor's immutable snapshot.
    ErrorWorkflowNotRegistered {key :: Text}
  | -- | The SystemDB did not grant this attempt the workflow execution claim.
    ErrorWorkflowClaimLost {workflowId :: Text}
  | -- | Shutdown cancelled the workflow while this caller waited for it. Its
    -- row stays @PENDING@, so a later executor recovers it: nothing was
    -- lost, this caller simply stopped being the one waiting. Mirrors Rust
    -- @Error::Interrupted@.
    Interrupted {workflowId :: Text}
  | -- | A prior execution recorded a workflow failure.
    ErrorWorkflowFailed {workflowId :: Text, message :: Text}
  | -- | An operation that only makes sense inside a workflow was called
    -- outside one.
    NotInWorkflow {operation :: Text}
  | -- | An instance method was called from inside a workflow another
    -- instance is running.
    WrongInstance {operation :: Text}
  | -- | A call was given an argument it cannot act on.
    InvalidArgument {operation :: Text, detail :: Text}
  | -- | A durable call was polled where its id means nothing.
    StepBuiltElsewhere {step :: Text, built :: Text, polled :: Text}
  | -- | One attempt exceeded its own timeout. The bound is per attempt,
    -- not per step. Mirrors Rust @Error::StepTimeout@.
    StepTimeout {step :: Text, timeout :: Duration}
  | -- | Every attempt failed. Carries all of them, oldest first. Mirrors
    -- Rust @Error::MaxStepRetriesExceeded@.
    MaxStepRetriesExceeded {step :: Text, attempts :: Int, errors :: [Error]}
  deriving stock (Eq, Show)

-- | The human-readable rendering, in the oracle's @Display@ shape.
renderTransactError :: Error -> Text
renderTransactError = \case
  ErrorConfig detail -> "invalid configuration: " <> detail
  ErrorNotLaunched operation -> "cannot " <> operation <> " before DBOS is launched"
  InsideStep operation -> operation <> " cannot be called from within a step"
  ErrorAlreadyLaunched operation -> "cannot " <> operation <> " after DBOS is launched"
  ErrorAlreadyRegistered key -> "a workflow is already registered as " <> key
  ErrorSerialization what message -> "could not serialize the workflow " <> what <> ": " <> message
  ErrorDeserialization what message -> "could not deserialize the workflow " <> what <> ": " <> message
  ErrorSystemDatabase err -> SystemDBError.renderError err
  StepFailed step message -> "the step " <> step <> " failed: " <> message
  ErrorWorkflowNotRegistered key -> "no workflow is registered as " <> key
  ErrorWorkflowClaimLost workflowId -> "the workflow " <> workflowId <> " is already being run elsewhere"
  Interrupted workflowId -> "the workflow " <> workflowId <> " was interrupted by shutdown and left PENDING"
  ErrorWorkflowFailed workflowId message -> "the workflow " <> workflowId <> " failed: " <> message
  NotInWorkflow operation -> operation <> " must be called from within a workflow"
  WrongInstance operation -> operation <> " was called on a different DBOS instance than the one running this workflow"
  InvalidArgument operation detail -> "invalid argument to " <> operation <> ": " <> detail
  StepBuiltElsewhere step built polled -> "step " <> step <> " was built " <> built <> " but polled " <> polled <> ": a step takes its id where it is built"
  StepTimeout step timeout -> "the step " <> step <> " exceeded its " <> pack (show (durationAsMillis timeout)) <> "ms timeout"
  MaxStepRetriesExceeded step attempts _ -> "the step " <> step <> " failed after " <> pack (show attempts) <> " attempts"
