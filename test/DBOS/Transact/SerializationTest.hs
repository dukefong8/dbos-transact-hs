{-# LANGUAGE OverloadedStrings #-}

-- | 'DBOS.Transact.Serialization' against the Rust @serialization.rs@ tests: an
-- absent value decodes as the unit, a value round trips, the unit encodes as
-- a value rather than an absence, and a mismatch names the half that failed.
module DBOS.Transact.SerializationTest
  ( tests,
  )
where

import DBOS.Prelude
import Data.Aeson (Value)
import DBOS.Transact
  ( CodecError (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    decodeWorkflowValue,
    encodeWorkflowValue,
  )
import DBOS.Transact.Serialization (encodeUnit)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Transact Serialization"
    [ testCase "an absent value decodes as the unit" $ do
        (decodeWorkflowValue "argument" Nothing :: Either CodecError ()) @?= Right ()
        (decodeWorkflowValue "argument" Nothing :: Either CodecError (Maybe Word32)) @?= Right Nothing,
      testCase "a value round trips" $ do
        let encoded = encodeWorkflowValue ((1 :: Word32, "two" :: Text))
        encoded.serializedText @?= "[1,\"two\"]"
        (decodeWorkflowValue "argument" (Just encoded) :: Either CodecError (Word32, Text)) @?= Right (1, "two"),
      testCase "the unit encodes as a value rather than an absence" $ do
        encodeUnit
          @?= SerializedWorkflowValue
            { serializedText = "null",
              serializedSerialization = Just (Serialization "rust_serde")
            },
      testCase "a mismatch names the half that failed" $
        case decodeWorkflowValue "result" (Just (SerializedWorkflowValue "\"not a number\"" (Just (Serialization "rust_serde")))) :: Either CodecError Word32 of
          Left (CodecTypeMismatch "result" _) -> pure ()
          other -> fail ("expected CodecTypeMismatch, got: " <> show other),
      testCase "rejects serialized text that is not JSON" $
        (decodeWorkflowValue "argument" (Just notJson) :: Either CodecError Value)
          @?= Left (CodecNotJson "argument" "not json")
    ]

notJson :: SerializedWorkflowValue
notJson =
  SerializedWorkflowValue
    { serializedText = "not json",
      serializedSerialization = Just (Serialization "rust_serde")
    }
