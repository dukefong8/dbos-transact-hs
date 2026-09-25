{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Mirrors the @config.rs@ test module, name for name.
module DBOS.Transact.ConfigTest (tests) where

import DBOS.Prelude
import DBOS.Transact
  ( Config (..),
    configNew,
    databaseUrlEnv,
    durationAsMillis,
    millisDuration,
    outcomePollInterval,
    secondsDuration,
    serializerName,
    validateConfig,
  )
import Data.Text qualified as Text
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Configuration"
    [ testCase "the serializer names itself as the column records it" $ do
        serializerName (configNew "app" "postgres://x").configSerializer @?= "rust_serde",
      testCase "validation reports a missing url by the name of the variable that sets it" $ do
        let config = configNew "app" ""
        case validateConfig config of
          Left err ->
            assertBool "names the variable" (databaseUrlEnv `Text.isInfixOf` Text.pack (show err))
          Right () -> assertBool "refused" False,
      testCase "a zero outcome poll interval is a busy loop and is refused" $ do
        let config interval = (configNew "app" "postgres://x") {configOutcomePollInterval = interval}
        assertEqual
          "the interval every implementation polls at"
          (durationAsMillis (outcomePollInterval (config Nothing)))
          1000
        assertEqual
          ""
          (durationAsMillis (outcomePollInterval (config (Just (millisDuration 250)))))
          250
        case validateConfig (config (Just (secondsDuration 0))) of
          Left err ->
            assertBool
              "unlike `notification_coalesce`, zero here is not a setting"
              ("outcome_poll_interval" `Text.isInfixOf` Text.pack (show err))
          Right () -> assertBool "refused" False
    ]
