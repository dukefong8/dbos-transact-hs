{-# LANGUAGE EmptyCase         #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Everything a durable function can fail with. Mirrors Rust
-- @error.rs@'s @Error<E>@: 'ErrorApplication' carries the application's own
-- failure, the rest are the engine's, and 'EngineOnly' is the uninhabited
-- channel for a workflow that declares none.
--
-- The engine channel is written @Error EngineOnly@: Rust's @Error<E =
-- EngineOnly>@ default has no Haskell counterpart, and ADR-0019 records
-- the alias that stood in while the channel was threaded.
module DBOS.Transact.Error
  ( Error (..),
    EngineOnly,
    DurableError,
    mapApplication,
    liftEngine,
    renderTransactError,
    controlOf,
    Failure (..),
    failureOf,
    failureError,
    encodeErrorText,
    decodeErrorText,
  )
where

import Data.Aeson (FromJSON (..), ToJSON (..), eitherDecodeStrict', encode, object, withObject, (.:), (.=))
import Data.Aeson.KeyMap qualified as KeyMap
import Data.ByteString.Lazy qualified as LBS
import Data.Text (Text, pack, unpack)
import Data.Text.Encoding (decodeUtf8, encodeUtf8)
import DBOS.Prelude
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (Duration, durationAsMillis)

-- | The error channel of a workflow that declares no failure of its own.
-- Rust's @EngineOnly@: uninhabited, so an engine error can be carried into
-- any channel by a total conversion rather than a panic waiting for an
-- input that cannot arrive.
data EngineOnly
  deriving stock (Eq, Show)

-- | What an application may fail with, inside 'ErrorApplication'. Rust's
-- @DurableError@: a recorded failure has to survive a column, so the
-- bounds are exactly the serializer's. A synonym rather than a class,
-- because the oracle's blanket impl means users write nothing — and here,
-- neither would they.
type DurableError e = (ToJSON e, FromJSON e)

-- | Mirrors Rust @Error@, with TypeScript consulted on naming. 'ErrorApplication' is the application's own
-- failure, held as itself rather than reduced to a description of one: the
-- run that fails and the replay that reads the row back both produce this
-- variant with an equal payload.
--
-- Constructors are bare unless the bare spelling is taken as a
-- constructor in the facade's scope: 'ErrorConfig' and
-- 'ErrorSerialization' collide with the 'Config' and 'Serialization'
-- constructors, so only those two stay prefixed. Collisions with the
-- sysdb error and event constructors are discriminated by the qualified
-- imports the tree already uses (ADR-0032).
data Error e
  = -- | A failure reported by the workflow or step body itself.
    ErrorApplication e
  | -- | The configuration could not be used: how the instance, the client or
    -- a queue was set up, not what a call was passed at the point it was
    -- made. Rust spells this @Error::Config@; the prefix is the documented
    -- collision deviation, because the @Config@ type owns that name here.
    ErrorConfig Text
  | -- | An operation requiring a running executor was attempted too early.
    NotLaunched {operation :: Text}
  | -- | A durable operation that allocates an id was called inside a step.
    InsideStep {operation :: Text}
  | -- | Registration happened after the launch snapshot was taken.
    AlreadyLaunched {operation :: Text}
  | -- | A workflow identity was registered more than once.
    AlreadyRegistered {key :: Text}
  | -- | A workflow input, result, or failure could not be encoded.
    ErrorSerialization {what :: Text, message :: Text}
  | -- | A serialized workflow input, result, or failure could not be decoded.
    Deserialization {what :: Text, message :: Text}
  | -- | The SystemDB rejected an engine operation.
    SystemDatabase SystemDBError.Error
  | -- | A previously recorded operation failed with an untyped message.
    StepFailed {step :: Text, message :: Text}
  | -- | A workflow key did not exist in the executor's immutable snapshot.
    NotRegistered {key :: Text}
  | -- | The SystemDB did not grant this attempt the workflow execution claim.
    WorkflowClaimLost {workflowId :: Text}
  | -- | Shutdown cancelled the workflow while this caller waited for it. Its
    -- row stays @PENDING@, so a later executor recovers it: nothing was
    -- lost, this caller simply stopped being the one waiting. Mirrors Rust
    -- @Error::Interrupted@.
    Interrupted {workflowId :: Text}
  | -- | A prior execution recorded a workflow failure.
    WorkflowFailed {workflowId :: Text, message :: Text}
  | -- | The workflow this caller was awaiting was cancelled — the child,
    -- not the caller. It is the awaited workflow's outcome, so it is
    -- recorded like one and a replay is told the same thing without
    -- waiting again. Mirrors Rust @Error::AwaitedWorkflowCancelled@.
    AwaitedWorkflowCancelled {workflowId :: Text}
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
    MaxStepRetriesExceeded {step :: Text, attempts :: Int, errors :: [Error e]}
  deriving stock (Eq, Show)

-- | Re-targets this error at another application channel. One match over
-- the engine's variants, kept in one place so 'liftEngine' shares the list
-- rather than carrying a copy; the nested errors of
-- 'MaxStepRetriesExceeded' recurse. (The oracle's blanket @impl<E> From<E>
-- for Error<E>@ is spelled with the constructor directly — 'Left .
-- ErrorApplication' — with no helper in between.)
mapApplication :: (e -> f) -> Error e -> Error f
mapApplication f = \case
  ErrorApplication err -> ErrorApplication (f err)
  ErrorConfig detail -> ErrorConfig detail
  NotLaunched operation -> NotLaunched operation
  InsideStep operation -> InsideStep operation
  AlreadyLaunched operation -> AlreadyLaunched operation
  AlreadyRegistered key -> AlreadyRegistered key
  ErrorSerialization what message -> ErrorSerialization what message
  Deserialization what message -> Deserialization what message
  SystemDatabase err -> SystemDatabase err
  StepFailed step message -> StepFailed step message
  NotRegistered key -> NotRegistered key
  WorkflowClaimLost workflowId -> WorkflowClaimLost workflowId
  Interrupted workflowId -> Interrupted workflowId
  WorkflowFailed workflowId message -> WorkflowFailed workflowId message
  AwaitedWorkflowCancelled workflowId -> AwaitedWorkflowCancelled workflowId
  NotInWorkflow operation -> NotInWorkflow operation
  WrongInstance operation -> WrongInstance operation
  InvalidArgument operation detail -> InvalidArgument operation detail
  StepBuiltElsewhere step built polled -> StepBuiltElsewhere step built polled
  StepTimeout step limit -> StepTimeout step limit
  MaxStepRetriesExceeded step attempts errors -> MaxStepRetriesExceeded step attempts (map (mapApplication f) errors)

-- | Rust @Error::lift@: an engine error carried into any workflow's
-- channel. Total because 'EngineOnly' is uninhabited.
liftEngine :: Error EngineOnly -> Error e
liftEngine = mapApplication absurd

absurd :: EngineOnly -> a
absurd impossible = case impossible of {}

-- | The human-readable rendering, in the oracle's @Display@ shape. An
-- application failure renders as itself; Rust delegates to the payload's
-- @Display@, and 'Show' is the port's closest.
renderTransactError :: Show e => Error e -> Text
renderTransactError = \case
  ErrorApplication err -> pack (show err)
  ErrorConfig detail -> "invalid configuration: " <> detail
  NotLaunched operation -> "cannot " <> operation <> " before DBOS is launched"
  InsideStep operation -> operation <> " cannot be called from within a step"
  AlreadyLaunched operation -> "cannot " <> operation <> " after DBOS is launched"
  AlreadyRegistered key -> "a workflow is already registered as " <> key
  ErrorSerialization what message -> "could not serialize the workflow " <> what <> ": " <> message
  Deserialization what message -> "could not deserialize the workflow " <> what <> ": " <> message
  SystemDatabase err -> SystemDBError.renderError err
  StepFailed step message -> "the step " <> step <> " failed: " <> message
  NotRegistered key -> "no workflow is registered as " <> key
  WorkflowClaimLost workflowId -> "the workflow " <> workflowId <> " is already being run elsewhere"
  Interrupted workflowId -> "the workflow " <> workflowId <> " was interrupted by shutdown and left PENDING"
  WorkflowFailed workflowId message -> "the workflow " <> workflowId <> " failed: " <> message
  AwaitedWorkflowCancelled workflowId -> "the workflow " <> workflowId <> " this one was awaiting was cancelled"
  NotInWorkflow operation -> operation <> " must be called from within a workflow"
  WrongInstance operation -> operation <> " was called on a different DBOS instance than the one running this workflow"
  InvalidArgument operation detail -> "invalid argument to " <> operation <> ": " <> detail
  StepBuiltElsewhere step built polled -> "step " <> step <> " was built " <> built <> " but polled " <> polled <> ": a step takes its id where it is built"
  StepTimeout step limit -> "the step " <> step <> " exceeded its " <> pack (show (durationAsMillis limit)) <> "ms timeout"
  MaxStepRetriesExceeded step attempts _ -> "the step " <> step <> " failed after " <> pack (show attempts) <> " attempts"

-- | Haskell best practice for the oracle's @Display@: the 'Exception'
-- mechanism carries the human rendering, so call sites use
-- 'displayException' instead of the bespoke render function (which stays
-- for the engine's existing uses).
instance (Show e, Typeable e) => Exception (Error e) where
  displayException = unpack . renderTransactError


-- | The engine-only channel holds no value, so it never encodes and never
-- parses: both instances are absurd for 'toJSON' and a refusal for
-- 'parseJSON', which is what a workflow that declares no failure deserves.
instance ToJSON EngineOnly where
  toJSON impossible = case impossible of {}

instance FromJSON EngineOnly where
  parseJSON _ = fail "the engine-only error channel holds no application failure"

-- | The recorded form of an error: the whole envelope, serialized, exactly
-- as the error column holds it and a replay reads it. Mirrors the serde
-- round-trip the oracle's test pins ("an application error is held as
-- itself, so it comes back as itself").
instance ToJSON e => ToJSON (Error e) where
  toJSON = \case
    ErrorApplication err -> tagged "ErrorApplication" (toJSON err)
    ErrorConfig detail -> tagged "ErrorConfig" (toJSON detail)
    NotLaunched operation -> tagged "NotLaunched" (object ["operation" .= operation])
    InsideStep operation -> tagged "InsideStep" (object ["operation" .= operation])
    AlreadyLaunched operation -> tagged "AlreadyLaunched" (object ["operation" .= operation])
    AlreadyRegistered key -> tagged "AlreadyRegistered" (object ["key" .= key])
    ErrorSerialization what message -> tagged "ErrorSerialization" (object ["what" .= what, "message" .= message])
    Deserialization what message -> tagged "Deserialization" (object ["what" .= what, "message" .= message])
    SystemDatabase err -> tagged "SystemDatabase" (object ["message" .= SystemDBError.renderError err])
    StepFailed step message -> tagged "StepFailed" (object ["step" .= step, "message" .= message])
    NotRegistered key -> tagged "NotRegistered" (object ["key" .= key])
    WorkflowClaimLost workflowId -> tagged "WorkflowClaimLost" (object ["workflowId" .= workflowId])
    Interrupted workflowId -> tagged "Interrupted" (object ["workflowId" .= workflowId])
    WorkflowFailed workflowId message -> tagged "WorkflowFailed" (object ["workflowId" .= workflowId, "message" .= message])
    AwaitedWorkflowCancelled workflowId -> tagged "AwaitedWorkflowCancelled" (object ["workflowId" .= workflowId])
    NotInWorkflow operation -> tagged "NotInWorkflow" (object ["operation" .= operation])
    WrongInstance operation -> tagged "WrongInstance" (object ["operation" .= operation])
    InvalidArgument operation detail -> tagged "InvalidArgument" (object ["operation" .= operation, "detail" .= detail])
    StepBuiltElsewhere step built polled -> tagged "StepBuiltElsewhere" (object ["step" .= step, "built" .= built, "polled" .= polled])
    StepTimeout step limit -> tagged "StepTimeout" (object ["step" .= step, "timeout" .= limit])
    MaxStepRetriesExceeded step attempts errors -> tagged "MaxStepRetriesExceeded" (object ["step" .= step, "attempts" .= attempts, "errors" .= errors])
    where
      tagged tag payload = object [tag .= payload]

-- | The decode side of the same round trip. Engine-only variants that never
-- reach a recorded row (the system-database failure is control) are refused
-- rather than guessed; a caller with a legacy or undecodable payload falls
-- back to 'WorkflowFailed', as the oracle's @decode(...).unwrap_or_else@
-- does.
instance FromJSON e => FromJSON (Error e) where
  parseJSON = withObject "Error" $ \fields -> case KeyMap.toList fields of
    [(tag, payload)] -> case tag of
      "ErrorApplication"         -> ErrorApplication <$> parseJSON payload
      "ErrorConfig"                -> ErrorConfig <$> parseJSON payload
      "NotLaunched"           -> withObject "NotLaunched" (\o -> NotLaunched <$> o .: "operation") payload
      "InsideStep"                 -> withObject "InsideStep" (\o -> InsideStep <$> o .: "operation") payload
      "AlreadyLaunched"       -> withObject "AlreadyLaunched" (\o -> AlreadyLaunched <$> o .: "operation") payload
      "AlreadyRegistered"     -> withObject "AlreadyRegistered" (\o -> AlreadyRegistered <$> o .: "key") payload
      "ErrorSerialization"         -> withObject "ErrorSerialization" (\o -> ErrorSerialization <$> o .: "what" <*> o .: "message") payload
      "Deserialization"       -> withObject "Deserialization" (\o -> Deserialization <$> o .: "what" <*> o .: "message") payload
      "SystemDatabase"        -> fail "a system-database failure is a control signal and is never decoded from an outcome"
      "StepFailed"                 -> withObject "StepFailed" (\o -> StepFailed <$> o .: "step" <*> o .: "message") payload
      "NotRegistered" -> withObject "NotRegistered" (\o -> NotRegistered <$> o .: "key") payload
      "WorkflowClaimLost"     -> withObject "WorkflowClaimLost" (\o -> WorkflowClaimLost <$> o .: "workflowId") payload
      "Interrupted"                -> withObject "Interrupted" (\o -> Interrupted <$> o .: "workflowId") payload
      "WorkflowFailed"        -> withObject "WorkflowFailed" (\o -> WorkflowFailed <$> o .: "workflowId" <*> o .: "message") payload
      "AwaitedWorkflowCancelled"   -> withObject "AwaitedWorkflowCancelled" (\o -> AwaitedWorkflowCancelled <$> o .: "workflowId") payload
      "NotInWorkflow"              -> withObject "NotInWorkflow" (\o -> NotInWorkflow <$> o .: "operation") payload
      "WrongInstance"              -> withObject "WrongInstance" (\o -> WrongInstance <$> o .: "operation") payload
      "InvalidArgument"            -> withObject "InvalidArgument" (\o -> InvalidArgument <$> o .: "operation" <*> o .: "detail") payload
      "StepBuiltElsewhere"         -> withObject "StepBuiltElsewhere" (\o -> StepBuiltElsewhere <$> o .: "step" <*> o .: "built" <*> o .: "polled") payload
      "StepTimeout"                -> withObject "StepTimeout" (\o -> StepTimeout <$> o .: "step" <*> o .: "timeout") payload
      "MaxStepRetriesExceeded"     -> withObject "MaxStepRetriesExceeded" (\o -> MaxStepRetriesExceeded <$> o .: "step" <*> o .: "attempts" <*> o .: "errors") payload
      _                            -> fail ("unknown error variant " <> show tag)
    _ -> fail "an error records exactly one tagged variant"

-- | The oracle's @Error::control@: a cancellation, an interruption and
-- every system-database failure are signals about the execution, not
-- statements about what the workflow computed. Everything else is an
-- outcome and gets recorded.
controlOf :: Error e -> Maybe (Error EngineOnly)
controlOf = \case
  SystemDatabase err -> Just (SystemDatabase err)
  Interrupted workflowId -> Just (Interrupted workflowId)
  _ -> Nothing

-- | How an erased workflow failed. Mirrors Rust @Failure@: the recorded
-- envelope, which a replay decodes, or an engine control signal, which is
-- never recorded as an outcome.
data Failure
  = FailureRecorded Text
  | FailureControl (Error EngineOnly)
  deriving stock (Eq, Show)

-- | Classify a typed failure for the erased boundary: control signals pass
-- through as engine errors, everything else is recorded as the serialized
-- envelope.
failureOf :: ToJSON e => Error e -> Failure
failureOf err = case controlOf err of
  Just engine -> FailureControl engine
  Nothing     -> FailureRecorded (encodeErrorText err)

-- | Decode a recorded failure back into a caller's channel. A payload that
-- cannot be decoded — a row written by older code — falls back to
-- 'WorkflowFailed' carrying the raw payload, as the oracle does.
failureError :: FromJSON e => Text -> Failure -> Error e
failureError workflowText failure = case failure of
  FailureControl engine -> liftEngine engine
  FailureRecorded payload -> case decodeErrorText payload of
    Right err -> err
    Left _    -> WorkflowFailed {workflowId = workflowText, message = payload}

-- | Encode an error as the error column holds it.
encodeErrorText :: ToJSON e => Error e -> Text
encodeErrorText = decodeUtf8 . LBS.toStrict . encode

-- | Decode an error column back into the caller's channel.
decodeErrorText :: FromJSON e => Text -> Either Text (Error e)
decodeErrorText = first pack . eitherDecodeStrict' . encodeUtf8
  where
    first f = either (Left . f) Right
