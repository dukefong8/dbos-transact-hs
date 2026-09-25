{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Internal JSON codec for durable workflow values (Rule 4: plain Haskell,
-- no Bluefin imports). Mirrors @serialization.rs@: @()@ encodes as @"null"@
-- (aeson would give @[]@, so zero-argument workflows use 'encodeUnit'), an
-- absent value decodes as JSON @null@, and failures name the half that failed
-- (@argument@, @result@, @error@). The format tag is @rust_serde@: aeson
-- output is plain JSON, keeping Haskell-written rows readable by Python
-- DBOS. Verified against the oracle: no divergence (same tag, same
-- absent-as-null, same named halves).
module DBOS.Transact.Codec
  ( CodecError (..),
    decodeWorkflowValue,
    encodeUnit,
    encodeWorkflowValue,
    encodeAttributes,
  )
where

import DBOS.Prelude
import Data.Aeson (FromJSON, Result (..), ToJSON, Value, eitherDecodeStrict, encode, fromJSON)
import Data.ByteString.Lazy (toStrict)
import Data.Map.Strict (Map)
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8, encodeUtf8)
import DBOS.SystemDB.Types (Serialization (..), SerializedWorkflowValue (..))

data CodecError
  = -- | The stored text is not JSON. Carries which half failed and the input.
    CodecNotJson { codecWhat :: Text, codecInput :: Text }
  | -- | The JSON parses but does not fit the expected shape.
    CodecTypeMismatch { codecWhat :: Text, codecMessage :: String }
  deriving stock (Eq, Show)

-- | Encode an application value as JSON for durable storage.
encodeWorkflowValue :: ToJSON a => a -> SerializedWorkflowValue
encodeWorkflowValue value =
  SerializedWorkflowValue
    { serializedText = decodeUtf8 (toStrict (encode value)),
      serializedSerialization = Just (Serialization "rust_serde")
    }

-- | The stored form of a zero-argument workflow: @"null"@, never an absence.
encodeUnit :: SerializedWorkflowValue
encodeUnit =
  SerializedWorkflowValue
    { serializedText = "null",
      serializedSerialization = Just (Serialization "rust_serde")
    }

-- | Caller-supplied attributes, stored as JSON on the row. Plain JSON,
-- never the configured serializer: the column is read by containment and by
-- every other implementation.
encodeAttributes :: Maybe (Map Text Value) -> Maybe Text
encodeAttributes attributes =
  decodeUtf8 . toStrict . encode <$> attributes

-- | Decode a stored value, naming the half (@argument@, @result@,
-- @error@) on failure. An absent value reads as JSON @null@.
decodeWorkflowValue :: FromJSON a => Text -> Maybe SerializedWorkflowValue -> Either CodecError a
decodeWorkflowValue what input =
  case eitherDecodeStrict (encodeUtf8 rawText) of
    Left _ -> Left (CodecNotJson what rawText)
    Right raw -> case fromJSON raw of
      Error message -> Left (CodecTypeMismatch what message)
      Success value -> Right value
  where
    rawText = maybe "null" (.serializedText) input
