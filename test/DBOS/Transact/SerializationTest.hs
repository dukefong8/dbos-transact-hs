{-# LANGUAGE OverloadedStrings #-}

-- | 'DBOS.Transact.Serialization' against the portable-JSON contract: every
-- payload is tagged @portable_json@, workflow arguments use the
-- @{positionalArgs, namedArgs}@ envelope, legacy @rust_serde@/untagged rows
-- stay readable as native JSON, and unknown tags are refused.
module DBOS.Transact.SerializationTest
  ( tests,
  )
where

import DBOS.Prelude
import Data.Aeson (Value (..), eitherDecodeStrict, toJSON)
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Vector qualified as Vector
import DBOS.Transact (Serialization (..), SerializedWorkflowValue (..))
import DBOS.Transact.Error (Error (..), decodeErrorText, encodeErrorText)
import DBOS.Transact.Serialization
  ( CodecError (..),
    codecUnknownTag,
    decodeWorkflowInput,
    decodeWorkflowValue,
    encodeUnit,
    encodeWorkflowInput,
    encodeWorkflowValue,
    unWorkflowInput,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Transact Serialization"
    [ testCase "an absent value decodes as the unit" $ do
        (decodeWorkflowInput "argument" Nothing :: Either CodecError ()) @?= Right ()
        (decodeWorkflowInput "argument" Nothing :: Either CodecError (Maybe Word32)) @?= Right Nothing,
      testCase "a payload is plain JSON tagged portable_json" $ do
        let encoded = encodeWorkflowValue ((1 :: Word32, "two" :: Text))
        encoded.serializedText @?= "[1,\"two\"]"
        encoded.serializedSerialization @?= Just (Serialization "portable_json")
        (decodeWorkflowValue "result" (Just encoded) :: Either CodecError (Word32, Text)) @?= Right (1, "two"),
      testCase "a workflow argument is the portable envelope" $ do
        let encoded = unWorkflowInput (encodeWorkflowInput ((1 :: Word32, "two" :: Text)))
        encoded.serializedSerialization @?= Just (Serialization "portable_json")
        envelope encoded.serializedText @?= ([toJSON (1 :: Word32), toJSON ("two" :: Text)], Object KeyMap.empty)
        (decodeWorkflowInput "argument" (Just encoded) :: Either CodecError (Word32, Text)) @?= Right (1, "two"),
      testCase "missing or null envelope sets are empty, wrong shapes are malformed" $ do
        let portable text = SerializedWorkflowValue text (Just (Serialization "portable_json"))
        (decodeWorkflowInput "argument" (Just (portable "null")) :: Either CodecError ()) @?= Right ()
        (decodeWorkflowInput "argument" (Just (portable "{\"positionalArgs\":null,\"namedArgs\":null}")) :: Either CodecError ()) @?= Right ()
        (decodeWorkflowInput "argument" (Just (portable "{}")) :: Either CodecError ()) @?= Right ()
        (decodeWorkflowInput "argument" (Just (portable "{\"namedArgs\":{}}")) :: Either CodecError ()) @?= Right ()
        case decodeWorkflowInput "argument" (Just (portable "{\"positionalArgs\":1}")) :: Either CodecError () of
          Left (CodecTypeMismatch "argument" _) -> pure ()
          other -> fail ("expected a non-array positionalArgs to be malformed, got: " <> show other)
        case decodeWorkflowInput "argument" (Just (portable "{\"namedArgs\":[]}")) :: Either CodecError () of
          Left (CodecTypeMismatch "argument" _) -> pure ()
          other -> fail ("expected a non-object namedArgs to be malformed, got: " <> show other),
      testCase "aeson collapses NaN and spells the infinities" $ do
        toJSON (0 / 0 :: Double) @?= Null
        toJSON (1 / 0 :: Double) @?= String "+inf"
        toJSON (-1 / 0 :: Double) @?= String "-inf",
      testCase "a NaN payload is null and reads back only as a required float" $ do
        let encoded = encodeWorkflowValue (0 / 0 :: Double)
        encoded.serializedText @?= "null"
        case decodeWorkflowValue "result" (Just encoded) :: Either CodecError Double of
          Right nan -> assertBool "null decodes as NaN for a required Double" (isNaN nan)
          Left err -> fail ("expected null to decode as NaN, got: " <> show err)
        (decodeWorkflowValue "result" (Just encoded) :: Either CodecError (Maybe Double)) @?= Right Nothing,
      testCase "an infinity keeps aeson's spelling and reads back" $ do
        let positive = encodeWorkflowValue (1 / 0 :: Double)
            negative = encodeWorkflowValue (-1 / 0 :: Double)
        positive.serializedText @?= "\"+inf\""
        negative.serializedText @?= "\"-inf\""
        (decodeWorkflowValue "result" (Just positive) :: Either CodecError Double) @?= Right (1 / 0)
        (decodeWorkflowValue "result" (Just negative) :: Either CodecError Double) @?= Right (-1 / 0),
      testCase "Rust's non-finite names are strings here, not floats" $ do
        let rustNaN = SerializedWorkflowValue "\"NaN\"" (Just (Serialization "portable_json"))
        (decodeWorkflowValue "result" (Just rustNaN) :: Either CodecError Text) @?= Right "NaN"
        case decodeWorkflowValue "result" (Just rustNaN) :: Either CodecError Double of
          Left (CodecTypeMismatch "result" _) -> pure ()
          other -> fail ("expected Rust's NaN name to fail as a Double, got: " <> show other),
      testCase "a non-finite workflow argument keeps its JSON spelling" $ do
        let nanArg = unWorkflowInput (encodeWorkflowInput (0 / 0 :: Double))
            infArg = unWorkflowInput (encodeWorkflowInput (1 / 0 :: Double))
        envelope nanArg.serializedText @?= ([Null], Object KeyMap.empty)
        envelope infArg.serializedText @?= ([String "+inf"], Object KeyMap.empty),
      testCase "a non-finite field is null inside a record" $ do
        let encoded = encodeWorkflowValue ((0 / 0 :: Double, 1 :: Word32))
        encoded.serializedText @?= "[null,1]",
      testCase "the unit input is the empty envelope" $ do
        encodeUnit.serializedSerialization @?= Just (Serialization "portable_json")
        envelope encodeUnit.serializedText @?= ([], Object KeyMap.empty)
        (decodeWorkflowInput "argument" (Just encodeUnit) :: Either CodecError ()) @?= Right (),
      testCase "legacy native rows still decode" $ do
        let legacy = SerializedWorkflowValue "[1,\"two\"]" (Just (Serialization "rust_serde"))
        (decodeWorkflowInput "argument" (Just legacy) :: Either CodecError (Word32, Text)) @?= Right (1, "two")
        (decodeWorkflowValue "result" (Just legacy) :: Either CodecError (Word32, Text)) @?= Right (1, "two"),
      testCase "an unknown tag is refused, never guessed" $ do
        let unknown = SerializedWorkflowValue "1" (Just (Serialization "pickle"))
        (decodeWorkflowValue "result" (Just unknown) :: Either CodecError Int)
          @?= Left (CodecUnknownSerialization "result" "pickle")
        codecUnknownTag (CodecUnknownSerialization "result" "pickle") @?= Just "pickle"
        codecUnknownTag (CodecNotJson "result" "nope") @?= Nothing,
      testCase "a mismatch names the half that failed" $
        case decodeWorkflowValue "result" (Just (SerializedWorkflowValue "\"not a number\"" (Just (Serialization "portable_json")))) :: Either CodecError Word32 of
          Left (CodecTypeMismatch "result" _) -> pure ()
          other -> fail ("expected CodecTypeMismatch, got: " <> show other),
      testCase "rejects serialized text that is not JSON" $
        (decodeWorkflowValue "argument" (Just notJson) :: Either CodecError Value)
          @?= Left (CodecNotJson "argument" "not json"),
      testCase "an application error's portable message is its text" $ do
        let encoded = encodeErrorText (ErrorApplication ("boom" :: Text) :: Error Text)
        case eitherDecodeStrict (encodeUtf8 encoded) of
          Right (Object fields) -> KeyMap.lookup "message" fields @?= Just (String "boom")
          other -> fail ("expected a portable error object, got: " <> show other),
      testCase "an error round-trips through the portable record" $ do
        let err = ErrorApplication ("boom" :: Text) :: Error Text
        decodeErrorText (encodeErrorText err) @?= Right err
    ]

-- | The envelope's positional list and named object.
envelope :: Text -> ([Value], Value)
envelope text =
  case eitherDecodeStrict (encodeUtf8 text) of
    Right (Object fields) ->
      case (KeyMap.lookup "positionalArgs" fields, KeyMap.lookup "namedArgs" fields) of
        (Just (Array args), Just named) -> (Vector.toList args, named)
        other -> error ("not an envelope: " <> show other)
    other -> error ("not a JSON object: " <> show other)

notJson :: SerializedWorkflowValue
notJson =
  SerializedWorkflowValue
    { serializedText = "not json",
      serializedSerialization = Just (Serialization "portable_json")
    }
