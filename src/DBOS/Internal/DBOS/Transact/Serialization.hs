{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Internal portable-JSON codec for durable workflow values.
--
-- The tag this port writes is always @portable_json@: plain aeson JSON, the
-- cross-SDK wire format Python and TypeScript read. Workflow arguments use
-- the portable envelope @{positionalArgs, namedArgs}@; every other payload
-- (result, error, event, message, stream value) is a plain JSON value.
--
-- Reads dispatch on the stored tag: @portable_json@ is decoded as portable
-- (arguments unwrap the envelope), while NULL, the empty string, and the
-- legacy @rust_serde@ tag are decoded as native plain JSON. An unknown tag
-- is an error, never a guess.
--
-- Non-finite floats follow aeson, not Rust: 'Data.Aeson.toJSON' collapses
-- NaN to @null@ and spells ±Infinity as the strings @\"+inf\"@/@\"-inf\"@,
-- so the codec never sees a non-finite number and cannot refuse the write.
-- ADR-0039 records the deviation and the pinned read-back facts (Rust's
-- @\"NaN\"@/@\"Infinity\"@/@\"-Infinity\"@ names read as text here).
--
-- This module is implementation detail. The facade re-exports the
-- app-facing entry points ('encodeWorkflowInput', 'encodeWorkflowValue',
-- 'decodeWorkflowInput', 'decodeWorkflowValue', 'PortableArgs'); tag
-- inspection and 'encodeUnit' stay internal.
module DBOS.Transact.Serialization
  ( CodecError (..),
    codecErrorText,
    codecUnknownTag,
    SerializationError (..),
    renderSerializationError,
    encodeSerializationError,
    decodeSerializationError,
    PortableArgs (..),
    WorkflowInput (..),
    unWorkflowInput,
    decodeWorkflowInput,
    decodeWorkflowValue,
    encodeUnit,
    encodeWorkflowInput,
    encodeWorkflowValue,
    encodeAttributes,
  )
where

import DBOS.Prelude
import Data.Aeson (FromJSON, Result (..), ToJSON, Value (..), eitherDecodeStrict, encode, fromJSON, object, toJSON, withObject, (.:), (.=))
import Data.Aeson.Types (Parser)
import Data.Aeson.KeyMap qualified as KeyMap
import Data.ByteString.Lazy (toStrict)
import Data.Map.Strict (Map)
import Data.Text (pack)
import Data.Text.Encoding (decodeUtf8, encodeUtf8)
import Data.Vector qualified as Vector
import DBOS.SystemDB.Types (Serialization (..), SerializedWorkflowValue (..), portableJsonSerialization)

data CodecError
  = -- | The stored text is not JSON. Carries which half failed and the input.
    CodecNotJson {codecWhat :: Text, codecInput :: Text}
  | -- | The JSON parses but does not fit the expected shape.
    CodecTypeMismatch {codecWhat :: Text, codecMessage :: String}
  | -- | The row names a serialization this port cannot read.
    CodecUnknownSerialization {codecWhat :: Text, codecSerialization :: Text}
  deriving stock (Eq, Show)

-- | The one human rendering of a codec failure, shared by every decode site.
codecErrorText :: CodecError -> Text
codecErrorText = \case
  CodecNotJson _ input -> "invalid JSON: " <> input
  CodecTypeMismatch _ detail -> pack detail
  CodecUnknownSerialization _ tag -> "unknown serialization tag: " <> tag

-- | The tag when the failure is an encoding this build cannot read. Callers
-- map it to their own @UnknownSerialization@ variant; every other codec
-- failure is a real decode failure.
codecUnknownTag :: CodecError -> Maybe Text
codecUnknownTag = \case
  CodecUnknownSerialization _ tag -> Just tag
  _ -> Nothing

-- | A codec outcome as an engine error: what could not be encoded or
-- decoded, or the serialization tag this build cannot read. Owned here
-- (the codec already lives here) so the serialize/deserialize boundary
-- names its own failures; the central envelope maps each arm onto its wire
-- variant ('fromSerializationError'), keeping tags and prose byte-identical.
-- Constructor names stay verbatim per the one-type-one-name rule; central's
-- same-named constructors are resolved by qualification at the mapping
-- sites, never by renaming.
data SerializationError
  = ErrorSerialization {what :: Text, message :: Text}
  | Deserialization {what :: Text, message :: Text}
  | UnknownSerialization {name :: Text}
  deriving stock (Eq, Show)

-- | The outcome's prose, in the oracle's @Display@ shape.
renderSerializationError :: SerializationError -> Text
renderSerializationError = \case
  ErrorSerialization what message -> "could not serialize the workflow " <> what <> ": " <> message
  Deserialization what message -> "could not deserialize the workflow " <> what <> ": " <> message
  UnknownSerialization name -> "serialization " <> name <> " is not available"

-- | The tag's payload object, without the envelope. Tags themselves stay
-- central: the envelope owns tag spellings, the fragment owns payloads.
encodeSerializationError :: SerializationError -> Value
encodeSerializationError = \case
  ErrorSerialization what message -> object ["what" .= what, "message" .= message]
  Deserialization what message -> object ["what" .= what, "message" .= message]
  UnknownSerialization name -> object ["name" .= name]

-- | The payload back into the owned type. No control case exists for codec
-- outcomes, so there is no predicate arm: every codec failure records.
decodeSerializationError :: Text -> Value -> Parser SerializationError
decodeSerializationError "ErrorSerialization" payload = withObject "ErrorSerialization" (\o -> ErrorSerialization <$> o .: "what" <*> o .: "message") payload
decodeSerializationError "Deserialization" payload = withObject "Deserialization" (\o -> Deserialization <$> o .: "what" <*> o .: "message") payload
decodeSerializationError "UnknownSerialization" payload = withObject "UnknownSerialization" (\o -> UnknownSerialization <$> o .: "name") payload
decodeSerializationError tag _ = fail ("unknown serialization error variant " <> show tag)

-- | How a Haskell workflow argument maps onto the portable positional list.
-- Unit is the one special case: it means no arguments, not one @null@
-- argument. A JSON array (a tuple, or a single list argument) is read as
-- several positional arguments, matching the oracle's @positionalArgs@.
class PortableArgs a where
  portablePositionalArgs :: a -> [Value]

instance {-# OVERLAPPING #-} PortableArgs () where
  portablePositionalArgs () = []

instance {-# OVERLAPPABLE #-} ToJSON a => PortableArgs a where
  portablePositionalArgs value =
    case toJSON value of
      Array values -> Vector.toList values
      other -> [other]

-- | A workflow argument, already in the cross-SDK
-- @{positionalArgs, namedArgs}@ envelope. Only 'encodeWorkflowInput' builds
-- one, so a plain payload (an 'encodeWorkflowValue' result) cannot be passed
-- where an argument belongs: the compiler rejects it instead of a reader
-- failing to decode the row later.
newtype WorkflowInput = WorkflowInput SerializedWorkflowValue
  deriving stock (Eq, Show)

unWorkflowInput :: WorkflowInput -> SerializedWorkflowValue
unWorkflowInput (WorkflowInput raw) = raw

-- | Encode a workflow argument as the portable envelope, tagged
-- @portable_json@.
encodeWorkflowInput :: PortableArgs a => a -> WorkflowInput
encodeWorkflowInput value =
  WorkflowInput
    SerializedWorkflowValue
      { serializedText =
          decodeUtf8 . toStrict . encode $
            object
              [ "positionalArgs" .= portablePositionalArgs value,
                "namedArgs" .= (object [] :: Value)
              ],
        serializedSerialization = Just (Serialization portableJsonSerialization)
      }

-- | Encode an application value as plain portable JSON for durable storage.
-- This is every payload except a workflow argument; arguments use
-- 'encodeWorkflowInput' so readers in other SDKs receive the envelope.
-- Non-finite floats follow aeson (ADR-0039): NaN becomes @null@ and
-- ±Infinity become the strings @\"+inf\"@/@\"-inf\"@; no write is refused.
encodeWorkflowValue :: ToJSON a => a -> SerializedWorkflowValue
encodeWorkflowValue value =
  SerializedWorkflowValue
    { serializedText = decodeUtf8 (toStrict (encode value)),
      serializedSerialization = Just (Serialization portableJsonSerialization)
    }

-- | The stored form of a zero-argument workflow input: the empty envelope,
-- never an absence.
encodeUnit :: SerializedWorkflowValue
encodeUnit = unWorkflowInput (encodeWorkflowInput ())

-- | Caller-supplied attributes, stored as JSON on the row. Plain JSON,
-- never the configured serializer: the column is read by containment and by
-- every other implementation.
encodeAttributes :: Maybe (Map Text Value) -> Maybe Text
encodeAttributes attributes =
  decodeUtf8 . toStrict . encode <$> attributes

-- | Decode a stored workflow argument by the row's tag. Portable rows unwrap
-- the @{positionalArgs, namedArgs}@ envelope; native rows are the argument
-- itself. An absent input reads as JSON @null@.
decodeWorkflowInput :: FromJSON a => Text -> Maybe SerializedWorkflowValue -> Either CodecError a
decodeWorkflowInput what input = do
  encoding <- payloadEncodingOf what input
  parsed <- parseJson what (rawText input)
  shaped <- case encoding of
    NativePayload -> Right parsed
    PortablePayload -> unwrapEnvelope what parsed
  fromJson what shaped

-- | Decode a stored payload (result, error, event, message, stream value) by
-- the row's tag. Both portable and native payloads are plain JSON; the tag
-- only rejects formats this port cannot read. Rust's @\"NaN\"@/
-- @\"Infinity\"@/@\"-Infinity\"@ names are not aeson's spellings and decode
-- as text, never as a float (ADR-0039).
decodeWorkflowValue :: FromJSON a => Text -> Maybe SerializedWorkflowValue -> Either CodecError a
decodeWorkflowValue what input = do
  _ <- payloadEncodingOf what input
  parsed <- parseJson what (rawText input)
  fromJson what parsed

data PayloadEncoding
  = NativePayload
  | PortablePayload

payloadEncodingOf :: Text -> Maybe SerializedWorkflowValue -> Either CodecError PayloadEncoding
payloadEncodingOf what input =
  case tagOf input of
    Nothing -> Right NativePayload
    Just "" -> Right NativePayload
    Just "rust_serde" -> Right NativePayload
    Just tag
      | tag == portableJsonSerialization -> Right PortablePayload
      | otherwise -> Left (CodecUnknownSerialization what tag)

tagOf :: Maybe SerializedWorkflowValue -> Maybe Text
tagOf Nothing = Nothing
tagOf (Just value) =
  case value.serializedSerialization of
    Just (Serialization tag) -> Just tag
    Nothing -> Nothing

rawText :: Maybe SerializedWorkflowValue -> Text
rawText = maybe "null" (.serializedText)

parseJson :: Text -> Text -> Either CodecError Value
parseJson what text =
  case eitherDecodeStrict (encodeUtf8 text) of
    Left _ -> Left (CodecNotJson what text)
    Right value -> Right value

fromJson :: FromJSON a => Text -> Value -> Either CodecError a
fromJson what value =
  case fromJSON value of
    Error message -> Left (CodecTypeMismatch what message)
    Success decoded -> Right decoded

-- | The portable argument shapes: no arguments is @null@, named-only is the
-- named record, one positional is the value, several are a tuple (a JSON
-- array), and a mixed envelope is handed through whole so nothing is lost.
-- A missing or @null@ set is empty (Rust's @null_as_default@); a present set
-- of the wrong shape is malformed.
unwrapEnvelope :: Text -> Value -> Either CodecError Value
unwrapEnvelope _ Null = Right Null
unwrapEnvelope what (Object fields) =
  case (positionalArgs fields, namedArgs fields) of
    (Right positional, Right named) ->
      case (positional, named) of
        ([], namedValue) | isEmptyObject namedValue -> Right Null
        ([], namedValue) -> Right namedValue
        ([single], namedValue) | isEmptyObject namedValue -> Right single
        (several, namedValue) | isEmptyObject namedValue -> Right (toJSON several)
        _ -> Right (Object fields)
    _ -> Left (malformedEnvelope what)
  where
    -- Rust reads a missing or explicit @null@ field as empty
    -- (serialization.rs's @null_as_default@); a present field of the wrong
    -- shape is malformed.
    positionalArgs f = case KeyMap.lookup "positionalArgs" f of
      Nothing -> Right []
      Just Null -> Right []
      Just (Array args) -> Right (Vector.toList args)
      Just _ -> Left (malformedEnvelope what)
    namedArgs f = case KeyMap.lookup "namedArgs" f of
      Nothing -> Right (Object KeyMap.empty)
      Just Null -> Right (Object KeyMap.empty)
      Just value@(Object _) -> Right value
      Just _ -> Left (malformedEnvelope what)
unwrapEnvelope what _ = Left (malformedEnvelope what)

isEmptyObject :: Value -> Bool
isEmptyObject (Object fields) = KeyMap.null fields
isEmptyObject _ = False

malformedEnvelope :: Text -> CodecError
malformedEnvelope what =
  CodecTypeMismatch what "portable workflow input is not a {positionalArgs, namedArgs} envelope"
