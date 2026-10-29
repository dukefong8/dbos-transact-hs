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
    liftEngine,
    renderTransactError,
    controlOf,
    fromSystemDB,
    refusedArgument,
    startRefusal,
    refusedStart,
    fromRegistryError,
    fromSerializationError,
    Failure (..),
    failureOf,
    failureError,
    encodeErrorText,
    decodeErrorText,
    decodeRecordedError,
    recordedErrorView,
  )
where

import Data.Aeson (FromJSON (..), Result (..), ToJSON (..), Value (..), eitherDecodeStrict', encode, fromJSON, object, withObject, (.:), (.=))
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.ByteString.Lazy qualified as LBS
import Data.Int (Int64)
import Data.Text (pack, unpack)
import Data.Text.Encoding (decodeUtf8, encodeUtf8)
import Data.Void (Void, absurd)
import DBOS.Prelude
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (Duration, WorkflowKey (..), durationAsMillis, renderWorkflowKey)
import DBOS.Transact.Registry (RegistryError, decodeRegistryErrorPayload, encodeRegistryErrorPayload, renderRegistryError)
import DBOS.Transact.Registry qualified as Registry
import DBOS.Transact.Serialization (SerializationError, decodeSerializationError, encodeSerializationError, renderSerializationError)
import DBOS.Transact.Serialization qualified as Serialization

-- | The error channel of a workflow that declares no failure of its own.
-- Rust's @EngineOnly@: uninhabited, so an engine error can be carried into
-- any channel by a total conversion rather than a panic waiting for an
-- input that cannot arrive. A synonym for 'Void': aeson already ships both
-- 'Void' JSON instances with matching behavior, so no orphan instances.
type EngineOnly = Void

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
    AlreadyRegistered {key :: WorkflowKey}
  | -- | A workflow input, result, or failure could not be encoded.
    ErrorSerialization {what :: Text, message :: Text}
  | -- | A serialized workflow input, result, or failure could not be decoded.
    Deserialization {what :: Text, message :: Text}
  | -- | A row carries a serialization tag this build cannot read. Reported
    -- rather than guessed. It is an outcome like any other: Rust's
    -- @Error::control@ has no arm for it, so a step/event/stream read that
    -- meets one records it; only a workflow-input decode maps it to a control
    -- signal. Mirrors Rust @UnknownSerialization@.
    UnknownSerialization {name :: Text}
  | -- | The SystemDB rejected an engine operation.
    SystemDatabase SystemDBError.Error
  | -- | A previously recorded operation failed with an untyped message.
    StepFailed {step :: Text, message :: Text}
  | -- | A workflow key did not exist in the executor's immutable snapshot.
    NotRegistered {key :: WorkflowKey}
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
  | -- | The workflow this caller was awaiting was parked after too many
    -- recoveries. The handle's own verdict, not a system-database answer:
    -- the sysdb layer never constructs it. Mirrors Rust
    -- @Error::MaxRecoveryAttemptsExceeded@.
    ErrorMaxRecoveryAttemptsExceeded {workflowId :: Text, recoveryAttempts :: Int64}
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
  | -- | No stream value arrived before the per-value read timeout.
    StreamTimeout {streamWorkflowId :: Text, streamKey :: Text, streamTimeout :: Maybe Duration}
  | -- | Another durable call of the same workflow took the step position a
    -- patch was built at. Refused rather than recorded elsewhere: a patch
    -- fixes its position where it is built but occupies it only once it has
    -- read the history there, so a call built in between took the position
    -- first. A workflow's outcome, like 'StepBuiltElsewhere'. Mirrors Rust
    -- @Error::PatchNondeterminism@.
    PatchNondeterminism {workflowId :: Text, patch :: Text, stepId :: Int}
  | -- | Every attempt failed. Carries all of them, oldest first. Mirrors
    -- Rust @Error::MaxStepRetriesExceeded@.
    MaxStepRetriesExceeded {step :: Text, attempts :: Int, errors :: [Error e]}
  deriving stock (Eq, Show, Functor)

-- | Rust @Error::lift@: an engine error carried into any workflow's
-- channel. Total because 'EngineOnly' is uninhabited.
liftEngine :: Error EngineOnly -> Error e
liftEngine = fmap absurd

-- * Human rendering: prose per constructor, delegated to owner modules
-- where a fragment owns the variant.

-- | The human-readable rendering, in the oracle's @Display@ shape. An
-- application failure renders as itself; Rust delegates to the payload's
-- @Display@, and 'Show' is the port's closest.
renderTransactError :: Show e => Error e -> Text
renderTransactError = \case
  ErrorApplication err -> pack (show err)
  other -> renderErrorWithoutApplication other

-- | The prose every constructor but the application's own payload shares.
-- 'renderTransactError' adds the payload's 'Show'; the portable error
-- encoder uses this so it does not acquire a 'Show' constraint.
renderErrorWithoutApplication :: Error e -> Text
renderErrorWithoutApplication = \case
  ErrorApplication _ -> "the workflow failed with an application error"
  ErrorConfig detail -> "invalid configuration: " <> detail
  NotLaunched operation -> "cannot " <> operation <> " before DBOS is launched"
  InsideStep operation -> operation <> " cannot be called from within a step"
  AlreadyLaunched operation -> renderRegistryError (Registry.AlreadyLaunched operation)
  AlreadyRegistered key -> renderRegistryError (Registry.AlreadyRegistered key)
  ErrorSerialization what message -> renderSerializationError (Serialization.ErrorSerialization {what = what, message = message})
  Deserialization what message -> renderSerializationError (Serialization.Deserialization {what = what, message = message})
  UnknownSerialization {name} -> renderSerializationError (Serialization.UnknownSerialization {name = name})
  SystemDatabase err -> SystemDBError.renderError err
  StepFailed step message -> "the step " <> step <> " failed: " <> message
  NotRegistered key -> "no workflow is registered as " <> renderWorkflowKey key
  WorkflowClaimLost workflowId -> "the workflow " <> workflowId <> " is already being run elsewhere"
  Interrupted workflowId -> "the workflow " <> workflowId <> " was interrupted by shutdown and left PENDING"
  WorkflowFailed workflowId message -> "the workflow " <> workflowId <> " failed: " <> message
  AwaitedWorkflowCancelled workflowId -> "the workflow " <> workflowId <> " this one was awaiting was cancelled"
  ErrorMaxRecoveryAttemptsExceeded workflowId recoveryAttempts -> "the workflow " <> workflowId <> " exceeded " <> pack (show recoveryAttempts) <> " recovery attempts"
  NotInWorkflow operation -> operation <> " must be called from within a workflow"
  WrongInstance operation -> operation <> " was called on a different DBOS instance than the one running this workflow"
  InvalidArgument operation detail -> "invalid argument to " <> operation <> ": " <> detail
  StepBuiltElsewhere step built polled -> "step " <> step <> " was built " <> built <> " but polled " <> polled <> ": a step takes its id where it is built"
  StepTimeout step limit -> "the step " <> step <> " exceeded its " <> pack (show (durationAsMillis limit)) <> "ms timeout"
  StreamTimeout workflowText key limit -> "reading stream " <> key <> " from workflow " <> workflowText <> " timed out" <> maybe "" (\duration -> " after " <> pack (show (durationAsMillis duration)) <> "ms") limit
  PatchNondeterminism workflowId patch stepId -> "patch " <> patch <> " in workflow " <> workflowId <> " lost step " <> pack (show stepId) <> " to a call built before it was awaited: await a patch where it is written, from sequential workflow code"
  MaxStepRetriesExceeded step attempts _ -> "the step " <> step <> " failed after " <> pack (show attempts) <> " attempts"

-- | Haskell best practice for the oracle's @Display@: the 'Exception'
-- mechanism carries the human rendering, so call sites use
-- 'displayException' instead of the bespoke render function (which stays
-- for the engine's existing uses).
instance (Show e, Typeable e) => Exception (Error e) where
  displayException = unpack . renderTransactError


-- * Wire envelope: tags stay central, payloads are owned per fragment.

-- | The structured form of an error: the whole tagged envelope, which the
-- portable record carries in its @data@ field and a replay decodes. Mirrors
-- the serde round-trip the oracle's test pins ("an application error is held
-- as itself, so it comes back as itself").
instance ToJSON e => ToJSON (Error e) where
  toJSON = \case
    ErrorApplication err -> tagged "ErrorApplication" (toJSON err)
    ErrorConfig detail -> tagged "ErrorConfig" (toJSON detail)
    NotLaunched operation -> tagged "NotLaunched" (object ["operation" .= operation])
    InsideStep operation -> tagged "InsideStep" (object ["operation" .= operation])
    AlreadyLaunched operation -> tagged "AlreadyLaunched" (encodeRegistryErrorPayload (Registry.AlreadyLaunched operation))
    AlreadyRegistered key -> tagged "AlreadyRegistered" (encodeRegistryErrorPayload (Registry.AlreadyRegistered key))
    ErrorSerialization what message -> tagged "ErrorSerialization" (encodeSerializationError (Serialization.ErrorSerialization {what = what, message = message}))
    Deserialization what message -> tagged "Deserialization" (encodeSerializationError (Serialization.Deserialization {what = what, message = message}))
    UnknownSerialization {name} -> tagged "UnknownSerialization" (encodeSerializationError (Serialization.UnknownSerialization {name = name}))
    SystemDatabase err -> tagged "SystemDatabase" (toJSON err)
    StepFailed step message -> tagged "StepFailed" (object ["step" .= step, "message" .= message])
    NotRegistered key -> tagged "NotRegistered" (object ["key" .= key])
    WorkflowClaimLost workflowId -> tagged "WorkflowClaimLost" (object ["workflowId" .= workflowId])
    Interrupted workflowId -> tagged "Interrupted" (object ["workflowId" .= workflowId])
    WorkflowFailed workflowId message -> tagged "WorkflowFailed" (object ["workflowId" .= workflowId, "message" .= message])
    AwaitedWorkflowCancelled workflowId -> tagged "AwaitedWorkflowCancelled" (object ["workflowId" .= workflowId])
    -- Tag keeps the Rust spelling; the constructor carries the documented
    -- @Error@-prefix deviation (@WorkflowStatus@ owns the bare name).
    ErrorMaxRecoveryAttemptsExceeded workflowId recoveryAttempts -> tagged "MaxRecoveryAttemptsExceeded" (object ["workflowId" .= workflowId, "recoveryAttempts" .= recoveryAttempts])
    NotInWorkflow operation -> tagged "NotInWorkflow" (object ["operation" .= operation])
    WrongInstance operation -> tagged "WrongInstance" (object ["operation" .= operation])
    InvalidArgument operation detail -> tagged "InvalidArgument" (object ["operation" .= operation, "detail" .= detail])
    StepBuiltElsewhere step built polled -> tagged "StepBuiltElsewhere" (object ["step" .= step, "built" .= built, "polled" .= polled])
    StepTimeout step limit -> tagged "StepTimeout" (object ["step" .= step, "timeout" .= limit])
    StreamTimeout workflowText key limit -> tagged "StreamTimeout" (object ["workflowId" .= workflowText, "key" .= key, "timeout" .= limit])
    PatchNondeterminism workflowId patch stepId -> tagged "PatchNondeterminism" (object ["workflowId" .= workflowId, "patch" .= patch, "stepId" .= stepId])
    MaxStepRetriesExceeded step attempts errors -> tagged "MaxStepRetriesExceeded" (object ["step" .= step, "attempts" .= attempts, "errors" .= errors])
    where
      tagged tag payload = object [tag .= payload]

-- | The decode side of the same round trip. Legacy message-only database
-- errors remain undecodable; typed serde-compatible errors replay as their
-- original constructor. Other malformed payloads fall back to 'WorkflowFailed',
-- as the oracle's @decode(...).unwrap_or_else@ does.
instance FromJSON e => FromJSON (Error e) where
  parseJSON = withObject "Error" $ \fields -> case KeyMap.toList fields of
    [(tag, payload)] -> case tag of
      "ErrorApplication"            -> ErrorApplication <$> parseJSON payload
      "ErrorConfig"                 -> ErrorConfig <$> parseJSON payload
      "NotLaunched"                 -> withObject "NotLaunched" (\o -> NotLaunched <$> o .: "operation") payload
      "InsideStep"                  -> withObject "InsideStep" (\o -> InsideStep <$> o .: "operation") payload
      "AlreadyLaunched"             -> fromRegistryError <$> decodeRegistryErrorPayload "AlreadyLaunched" payload
      "AlreadyRegistered"           -> fromRegistryError <$> decodeRegistryErrorPayload "AlreadyRegistered" payload
      "ErrorSerialization"          -> fromSerializationError <$> decodeSerializationError "ErrorSerialization" payload
      "Deserialization"             -> fromSerializationError <$> decodeSerializationError "Deserialization" payload
      "UnknownSerialization"        -> fromSerializationError <$> decodeSerializationError "UnknownSerialization" payload
      "SystemDatabase"              -> SystemDatabase <$> parseJSON payload
      "StepFailed"                  -> withObject "StepFailed" (\o -> StepFailed <$> o .: "step" <*> o .: "message") payload
      "NotRegistered"               -> withObject "NotRegistered" (\o -> NotRegistered <$> o .: "key") payload
      "WorkflowClaimLost"           -> withObject "WorkflowClaimLost" (\o -> WorkflowClaimLost <$> o .: "workflowId") payload
      "Interrupted"                 -> withObject "Interrupted" (\o -> Interrupted <$> o .: "workflowId") payload
      "WorkflowFailed"              -> withObject "WorkflowFailed" (\o -> WorkflowFailed <$> o .: "workflowId" <*> o .: "message") payload
      "AwaitedWorkflowCancelled"    -> withObject "AwaitedWorkflowCancelled" (\o -> AwaitedWorkflowCancelled <$> o .: "workflowId") payload
      "MaxRecoveryAttemptsExceeded" -> withObject "MaxRecoveryAttemptsExceeded" (\o -> ErrorMaxRecoveryAttemptsExceeded <$> o .: "workflowId" <*> o .: "recoveryAttempts") payload
      "NotInWorkflow"               -> withObject "NotInWorkflow" (\o -> NotInWorkflow <$> o .: "operation") payload
      "WrongInstance"               -> withObject "WrongInstance" (\o -> WrongInstance <$> o .: "operation") payload
      "InvalidArgument"             -> withObject "InvalidArgument" (\o -> InvalidArgument <$> o .: "operation" <*> o .: "detail") payload
      "StepBuiltElsewhere"          -> withObject "StepBuiltElsewhere" (\o -> StepBuiltElsewhere <$> o .: "step" <*> o .: "built" <*> o .: "polled") payload
      "StepTimeout"                 -> withObject "StepTimeout" (\o -> StepTimeout <$> o .: "step" <*> o .: "timeout") payload
      "StreamTimeout"               -> withObject "StreamTimeout" (\o -> StreamTimeout <$> o .: "workflowId" <*> o .: "key" <*> o .: "timeout") payload
      "PatchNondeterminism"         -> withObject "PatchNondeterminism" (\o -> PatchNondeterminism <$> o .: "workflowId" <*> o .: "patch" <*> o .: "stepId") payload
      "MaxStepRetriesExceeded"      -> withObject "MaxStepRetriesExceeded" (\o -> MaxStepRetriesExceeded <$> o .: "step" <*> o .: "attempts" <*> o .: "errors") payload
      _                             -> fail ("unknown error variant " <> show tag)
    _ -> fail "an error records exactly one tagged variant"

-- * Classification: control signals pass through, everything else records.

-- | The oracle's @Error::control@: a cancellation, an interruption, and the
-- system-database failures that are signals about the execution rather than
-- answers a call gave are not the workflow's outcome. A typed refusal
-- ('ConflictingWorkflow', 'QueueDeduplicated', 'InvalidInput', …) is the
-- opposite case: its step records it, every replay gives it back, and a
-- workflow that returns one has failed with it — so it is recorded like any
-- step's error. The verdict lives on the sysdb error ('shouldRecord'),
-- exactly as Rust's @Error::should_record@ draws the same line.
controlOf :: Error e -> Maybe (Error EngineOnly)
controlOf = \case
  SystemDatabase err | not (SystemDBError.shouldRecord err) -> Just (SystemDatabase err)
  Interrupted workflowId -> Just (Interrupted workflowId)
  _ -> Nothing

-- | Lift a system-database failure into a caller's channel. An unknown tag is
-- the same variant on both sides, so a caller matching on it sees one shape
-- wherever the row was read; everything else stays 'SystemDatabase'. Mirrors
-- Rust @Error::from_sysdb@.
fromSystemDB :: SystemDBError.Error -> Error e
fromSystemDB = \case
  SystemDBError.UnknownSerialization {name} -> UnknownSerialization {name}
  err -> SystemDatabase err

-- | Lift a system-database refusal of a call's argument into the engine
-- channel: an 'InvalidInput' names its field the way the surface spells it
-- (@`delay` must fit in milliseconds as a 64-bit integer@), reported as
-- 'InvalidArgument' to the operation. Any other failure is not a refusal
-- and keeps its 'SystemDatabase' variant. Mirrors Rust
-- @Error::refused_argument@ (@from_refusal@ plus @refusal_detail@ folded
-- in): a surface refuses by asking the database's own check rather than
-- restating it, so the two cannot disagree about what is storable.
refusedArgument :: Text -> SystemDBError.Error -> Error e
refusedArgument operation = \case
  SystemDBError.InvalidInput {field, detail} -> InvalidArgument operation ("`" <> field <> "` " <> detail)
  err -> fromSystemDB err

-- | What a start that runs at once is refused as. A start onto a queue,
-- and with it every client enqueue, is refused as 'enqueueOperation'.
-- Mirrors Rust @START_OPERATION@.
startOperation :: Text
startOperation = "start a workflow"

-- | What a start onto a queue is refused as; see 'startOperation'.
-- Mirrors Rust @ENQUEUE_OPERATION@.
enqueueOperation :: Text
enqueueOperation = "enqueue a workflow"

-- | A start's refusal of one of its arguments: to 'enqueueOperation' when
-- it names a queue, with the queue in the detail because nothing else in
-- the refusal says which enqueue it was, and to 'startOperation' when it
-- does not. Mirrors Rust @start_refusal@.
startRefusal :: Maybe Text -> Text -> Error e
startRefusal queue detail = case queue of
  Just name -> InvalidArgument enqueueOperation ("queue `" <> name <> "`: " <> detail)
  Nothing -> InvalidArgument startOperation detail

-- | A start's error, with the system database's refusal of its arguments
-- reported as the start's own, through 'startRefusal'. Chiefly the
-- refusal only the insert can make: a delay that fits in milliseconds
-- but carries the release time past the largest instant, which depends
-- on the clock. A child start records that refusal at its parent's step
-- as the system database's error, so a replay reads it back in the same
-- form and this maps it the same way. Also every other value of the row
-- the caller supplied and the insert refuses, named as the start's API
-- spells it. Mirrors Rust @refused_start@.
refusedStart :: Maybe Text -> SystemDBError.Error -> Error e
refusedStart queue = \case
  SystemDBError.InvalidInput {field, detail} -> startRefusal queue (refusalDetail (apiField field) detail)
  err -> fromSystemDB err
  where
    -- A system-database refusal's field and detail as one phrase, for a
    -- message that names the call separately. Mirrors @refusal_detail@.
    refusalDetail field detail = "`" <> field <> "` " <> detail
    -- The name a start's API gives a column the system database refuses
    -- by name. Mirrors Rust @api_field@.
    apiField field = case field of
      "queue_name" -> "name"
      "queue_partition_key" -> "partition_key"
      "name" -> "workflow"
      "application_version" -> "app_version"
      other -> other

-- | Lift a registry refusal into a caller's channel. The registry owns the
-- refusal vocabulary; the envelope (tag, prose, wire) stays here so
-- recorded rows keep their shape. 'Datasource' maps through this too: its
-- freeze refusal is the same vocabulary, not a workflow registration.
fromRegistryError :: RegistryError -> Error e
fromRegistryError = \case
  Registry.AlreadyRegistered key -> AlreadyRegistered key
  Registry.AlreadyLaunched operation -> AlreadyLaunched operation

-- | Lift a codec outcome into a caller's channel. Codec construction sites
-- build the owned type and map through here, keeping one construction path.
fromSerializationError :: SerializationError -> Error e
fromSerializationError = \case
  Serialization.ErrorSerialization what message -> ErrorSerialization what message
  Serialization.Deserialization what message -> Deserialization what message
  Serialization.UnknownSerialization name -> UnknownSerialization name

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
-- 'WorkflowFailed'. Another SDK's portable record keeps its
-- @name: message@ text; a payload that is not an error this build can read
-- keeps the raw text.
failureError :: FromJSON e => Text -> Failure -> Error e
failureError workflowText failure = case failure of
  FailureControl engine   -> liftEngine engine
  FailureRecorded payload -> decodeRecordedError workflowText payload

-- | The recorded-failure fallback shared by 'failureError' and the handle
-- reads. A payload this caller's channel cannot decode still reports the
-- portable @name: message@ when it is another SDK's record, and the raw text
-- otherwise, instead of discarding the human message.
decodeRecordedError :: FromJSON e => Text -> Text -> Error e
decodeRecordedError workflowText payload =
  case decodeErrorText payload of
    Right err -> err
    Left _    -> WorkflowFailed {workflowId = workflowText, message = fallbackText payload}
  where
    fallbackText text = case jsonValue text of
      Just value -> case portableRecordOf value of
        Just (name, message, _) -> name <> ": " <> message
        Nothing                 -> text
      Nothing -> text

-- * Portable codec: the cross-SDK @{name, message, data}@ record.

-- | Encode an error as the error column holds it: the portable error record
-- other SDKs read (@{name, message, data}@), with the port's structured
-- @Error@ envelope in @data@ so a replay reconstructs the original
-- constructor. Python's portable errors carry @code@ too; the port has no
-- code to add.
encodeErrorText :: ToJSON e => Error e -> Text
encodeErrorText err =
  decodeUtf8 . LBS.toStrict . encode $
    object
      [ "name" .= portableErrorName err,
        "message" .= portableMessage err,
        "data" .= toJSON err
      ]

-- | The record's human message. An application failure uses the payload's
-- text when it is a JSON string — the common @ErrorApplication Text@ case —
-- and its JSON encoding otherwise; the wire encoder has no 'Show' constraint
-- to spend.
portableMessage :: ToJSON e => Error e -> Text
portableMessage err = case err of
  ErrorApplication value -> case toJSON value of
    String text -> text
    _           -> decodeUtf8 (LBS.toStrict (encode value))
  other -> renderErrorWithoutApplication other

-- | The constructor spelling used as the portable record's name. The tagged
-- envelope has exactly one key: the constructor.
portableErrorName :: ToJSON e => Error e -> Text
portableErrorName err = case toJSON err of
  Object fields -> case KeyMap.toList fields of
    [(tag, _)] -> Key.toText tag
    _          -> "Error"
  _ -> "Error"

-- | Decode an error column back into the caller's channel. A portable record
-- decodes through its @data@ payload; a record without a matching payload
-- (another language's) keeps its @name: message@ text. A legacy tagged
-- envelope decodes as itself.
decodeErrorText :: FromJSON e => Text -> Either Text (Error e)
decodeErrorText payload =
  case jsonValue payload of
    Nothing -> Left payload
    Just value -> case portableRecordOf value of
      Just (name, message, Just structured) ->
        case fromJSON structured of
          Success err -> Right err
          Error _     -> Left (name <> ": " <> message)
      Just (name, message, Nothing) -> Left (name <> ": " <> message)
      Nothing ->
        case fromJSON value of
          Success err   -> Right err
          Error message -> Left (pack message)

-- | The portable view of a recorded error for a reader with no error type of
-- its own (a step listing): the portable record's @name@, @message@ and
-- optional @data@, or a legacy tagged envelope decoded as 'Value' and rendered
-- through its own name and message. @Nothing@ when the payload is not an error
-- this build can read.
recordedErrorView :: Text -> Maybe (Text, Text, Maybe Value)
recordedErrorView text = do
  value <- jsonValue text
  case portableRecordOf value of
    Just (name, message, structured) -> Just (name, message, structured)
    Nothing -> case (fromJSON value :: Result (Error Value)) of
      Success err -> Just (portableErrorName err, portableMessage err, Just (toJSON err))
      Error _     -> Nothing

jsonValue :: Text -> Maybe Value
jsonValue text = case eitherDecodeStrict' (encodeUtf8 text) of
  Right value -> Just value
  Left _      -> Nothing

portableRecordOf :: Value -> Maybe (Text, Text, Maybe Value)
portableRecordOf (Object fields)
  | Just name <- KeyMap.lookup "name" fields,
    Just message <- KeyMap.lookup "message" fields =
      Just (textOf name, textOf message, KeyMap.lookup "data" fields)
  where
    textOf (String text) = text
    textOf value         = decodeUtf8 (LBS.toStrict (encode value))
portableRecordOf _ = Nothing
