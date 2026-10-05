{-# LANGUAGE EmptyCase         #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Everything a durable function can fail with. Mirrors Rust
-- @error.rs@'s @Error<E>@: 'Application' carries the application's own
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
    application,
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
import Data.Text (Text, pack)
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

-- | What an application may fail with, inside 'Application'. Rust's
-- @DurableError@: a recorded failure has to survive a column, so the
-- bounds are exactly the serializer's. A synonym rather than a class,
-- because the oracle's blanket impl means users write nothing — and here,
-- neither would they.
type DurableError e = (ToJSON e, FromJSON e)

-- | Mirrors Rust @Error<E>@. 'Application' is the application's own
-- failure, held as itself rather than reduced to a description of one: the
-- run that fails and the replay that reads the row back both produce this
-- variant with an equal payload.
data Error e
  = -- | A failure reported by the workflow or step body itself.
    Application e
  | -- | The configuration could not be used: how the instance, the client or
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

-- | The oracle's blanket @impl<E> From<E> for Error<E>@: an application's
-- own failure, wrapped as itself, which is what lets a body's @?@ work.
application :: e -> Error e
application = Application

-- | Re-targets this error at another application channel. One match over
-- the engine's variants, kept in one place so 'liftEngine' shares the list
-- rather than carrying a copy; the nested errors of
-- 'MaxStepRetriesExceeded' recurse.
mapApplication :: (e -> f) -> Error e -> Error f
mapApplication f = \case
  Application err -> Application (f err)
  ErrorConfig detail -> ErrorConfig detail
  ErrorNotLaunched operation -> ErrorNotLaunched operation
  InsideStep operation -> InsideStep operation
  ErrorAlreadyLaunched operation -> ErrorAlreadyLaunched operation
  ErrorAlreadyRegistered key -> ErrorAlreadyRegistered key
  ErrorSerialization what message -> ErrorSerialization what message
  ErrorDeserialization what message -> ErrorDeserialization what message
  ErrorSystemDatabase err -> ErrorSystemDatabase err
  StepFailed step message -> StepFailed step message
  ErrorWorkflowNotRegistered key -> ErrorWorkflowNotRegistered key
  ErrorWorkflowClaimLost workflowId -> ErrorWorkflowClaimLost workflowId
  Interrupted workflowId -> Interrupted workflowId
  ErrorWorkflowFailed workflowId message -> ErrorWorkflowFailed workflowId message
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
  Application err -> pack (show err)
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
  AwaitedWorkflowCancelled workflowId -> "the workflow " <> workflowId <> " this one was awaiting was cancelled"
  NotInWorkflow operation -> operation <> " must be called from within a workflow"
  WrongInstance operation -> operation <> " was called on a different DBOS instance than the one running this workflow"
  InvalidArgument operation detail -> "invalid argument to " <> operation <> ": " <> detail
  StepBuiltElsewhere step built polled -> "step " <> step <> " was built " <> built <> " but polled " <> polled <> ": a step takes its id where it is built"
  StepTimeout step limit -> "the step " <> step <> " exceeded its " <> pack (show (durationAsMillis limit)) <> "ms timeout"
  MaxStepRetriesExceeded step attempts _ -> "the step " <> step <> " failed after " <> pack (show attempts) <> " attempts"


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
    Application err -> tagged "Application" (toJSON err)
    ErrorConfig detail -> tagged "ErrorConfig" (toJSON detail)
    ErrorNotLaunched operation -> tagged "ErrorNotLaunched" (object ["operation" .= operation])
    InsideStep operation -> tagged "InsideStep" (object ["operation" .= operation])
    ErrorAlreadyLaunched operation -> tagged "ErrorAlreadyLaunched" (object ["operation" .= operation])
    ErrorAlreadyRegistered key -> tagged "ErrorAlreadyRegistered" (object ["key" .= key])
    ErrorSerialization what message -> tagged "ErrorSerialization" (object ["what" .= what, "message" .= message])
    ErrorDeserialization what message -> tagged "ErrorDeserialization" (object ["what" .= what, "message" .= message])
    ErrorSystemDatabase err -> tagged "ErrorSystemDatabase" (object ["message" .= SystemDBError.renderError err])
    StepFailed step message -> tagged "StepFailed" (object ["step" .= step, "message" .= message])
    ErrorWorkflowNotRegistered key -> tagged "ErrorWorkflowNotRegistered" (object ["key" .= key])
    ErrorWorkflowClaimLost workflowId -> tagged "ErrorWorkflowClaimLost" (object ["workflowId" .= workflowId])
    Interrupted workflowId -> tagged "Interrupted" (object ["workflowId" .= workflowId])
    ErrorWorkflowFailed workflowId message -> tagged "ErrorWorkflowFailed" (object ["workflowId" .= workflowId, "message" .= message])
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
-- back to 'ErrorWorkflowFailed', as the oracle's @decode(...).unwrap_or_else@
-- does.
instance FromJSON e => FromJSON (Error e) where
  parseJSON = withObject "Error" $ \fields -> case KeyMap.toList fields of
    [(tag, payload)] -> case tag of
      "Application"                -> Application <$> parseJSON payload
      "ErrorConfig"                -> ErrorConfig <$> parseJSON payload
      "ErrorNotLaunched"           -> withObject "ErrorNotLaunched" (\o -> ErrorNotLaunched <$> o .: "operation") payload
      "InsideStep"                 -> withObject "InsideStep" (\o -> InsideStep <$> o .: "operation") payload
      "ErrorAlreadyLaunched"       -> withObject "ErrorAlreadyLaunched" (\o -> ErrorAlreadyLaunched <$> o .: "operation") payload
      "ErrorAlreadyRegistered"     -> withObject "ErrorAlreadyRegistered" (\o -> ErrorAlreadyRegistered <$> o .: "key") payload
      "ErrorSerialization"         -> withObject "ErrorSerialization" (\o -> ErrorSerialization <$> o .: "what" <*> o .: "message") payload
      "ErrorDeserialization"       -> withObject "ErrorDeserialization" (\o -> ErrorDeserialization <$> o .: "what" <*> o .: "message") payload
      "ErrorSystemDatabase"        -> fail "a system-database failure is a control signal and is never decoded from an outcome"
      "StepFailed"                 -> withObject "StepFailed" (\o -> StepFailed <$> o .: "step" <*> o .: "message") payload
      "ErrorWorkflowNotRegistered" -> withObject "ErrorWorkflowNotRegistered" (\o -> ErrorWorkflowNotRegistered <$> o .: "key") payload
      "ErrorWorkflowClaimLost"     -> withObject "ErrorWorkflowClaimLost" (\o -> ErrorWorkflowClaimLost <$> o .: "workflowId") payload
      "Interrupted"                -> withObject "Interrupted" (\o -> Interrupted <$> o .: "workflowId") payload
      "ErrorWorkflowFailed"        -> withObject "ErrorWorkflowFailed" (\o -> ErrorWorkflowFailed <$> o .: "workflowId" <*> o .: "message") payload
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
  ErrorSystemDatabase err -> Just (ErrorSystemDatabase err)
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
-- 'ErrorWorkflowFailed' carrying the raw payload, as the oracle does.
failureError :: FromJSON e => Text -> Failure -> Error e
failureError workflowText failure = case failure of
  FailureControl engine -> liftEngine engine
  FailureRecorded payload -> case decodeErrorText payload of
    Right err -> err
    Left _    -> ErrorWorkflowFailed {workflowId = workflowText, message = payload}

-- | Encode an error as the error column holds it.
encodeErrorText :: ToJSON e => Error e -> Text
encodeErrorText = decodeUtf8 . LBS.toStrict . encode

-- | Decode an error column back into the caller's channel.
decodeErrorText :: FromJSON e => Text -> Either Text (Error e)
decodeErrorText = first pack . eitherDecodeStrict' . encodeUtf8
  where
    first f = either (Left . f) Right
