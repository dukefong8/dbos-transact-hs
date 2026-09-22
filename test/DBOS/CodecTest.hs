{-# LANGUAGE OverloadedStrings #-}

module DBOS.CodecTest
  ( tests,
  )
where

import Data.Aeson (Value, object, (.=))
import DBOS.Transact
  ( CodecError (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    decodeWorkflowValue,
    encodeUnit,
    encodeWorkflowValue,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), testCase)

tests :: TestTree
tests =
  testGroup
    "DBOS Codec"
    [ testCase "encodes zero-argument workflows as JSON null" $
        encodeUnit
          @?= SerializedWorkflowValue
            { serializedText = "null",
              serializedSerialization = Just (Serialization "json")
            },
      testCase "round-trips an application value through JSON" $
        decodeWorkflowValue "argument" (Just (encodeWorkflowValue (object ["ok" .= True])))
          @?= Right (object ["ok" .= True] :: Value),
      testCase "decodes an absent value as unit" $
        (decodeWorkflowValue "argument" Nothing :: Either CodecError ())
          @?= Right (),
      testCase "rejects serialized text that is not JSON" $
        ( decodeWorkflowValue "argument" (Just notJson) :: Either CodecError Value
          )
          @?= Left (CodecNotJson "argument" "not json"),
      testCase "rejects JSON of the wrong shape" $
        case decodeWorkflowValue "result" (Just stepOutput) :: Either CodecError Bool of
          Left (CodecTypeMismatch "result" _) -> pure ()
          other -> fail ("expected CodecTypeMismatch, got: " <> show other)
    ]

notJson :: SerializedWorkflowValue
notJson =
  SerializedWorkflowValue
    { serializedText = "not json",
      serializedSerialization = Just (Serialization "json")
    }

stepOutput :: SerializedWorkflowValue
stepOutput =
  SerializedWorkflowValue
    { serializedText = "{\"ok\":true}",
      serializedSerialization = Just (Serialization "json")
    }
