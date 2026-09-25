{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The identity rules: who outranks whom, what a nameless process calls
-- itself, and the name rule every implementation shares. Every case mirrors
-- its Rust test name in @identity.rs@.
module DBOS.Transact.IdentityTest (tests) where

import DBOS.Prelude
import DBOS.Transact
  ( Config (..),
    Environment (..),
    Identity (..),
    appVersionEnv,
    cloudAppNameEnv,
    configNew,
    resolve,
    validateAppName,
  )
import Data.Text (Text)
import Data.Text qualified as Text
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Identity"
    [ testCase "the configuration outranks the environment off DBOS Cloud" $ do
        resolved <- expectResolved (resolve configured deployed)
        resolved.identityAppName @?= "config-app"
        resolved.identityAppVersion @?= "config-v1"
        resolved.identityExecutorId @?= "config-executor"
        resolved.identityAppId @?= "app-id-7",
      testCase "the environment fills in what the configuration leaves unset" $ do
        resolved <- expectResolved (resolve (configured {configAppVersion = Nothing, configExecutorId = Nothing}) deployed)
        resolved.identityAppVersion @?= "env-v9"
        resolved.identityExecutorId @?= "env-executor"
        resolved.identityAppName @?= "config-app",
      testCase "dbos cloud outranks the configuration" $ do
        resolved <- expectResolved (resolve configured deployed {environmentCloud = True})
        resolved.identityAppName @?= "env-app"
        resolved.identityAppVersion @?= "env-v9"
        resolved.identityExecutorId @?= "env-executor",
      testCase "an unnamed process is local" $ do
        resolved <- expectResolved (resolve (configured {configExecutorId = Nothing}) (deployed {environmentExecutorId = Nothing}))
        resolved.identityExecutorId @?= "local",
      testCase "a version nobody supplied is an error naming both ways to supply one" $ do
        case resolve (configured {configAppVersion = Nothing}) (deployed {environmentAppVersion = Nothing}) of
          Left err -> do
            let message = Text.pack (show err)
            assertBool "names the field" ("app_version" `Text.isInfixOf` message)
            assertBool "names the variable" (appVersionEnv `Text.isInfixOf` message)
          other -> fail ("expected a config error, got: " <> show other),
      testCase "dbos cloud without an application name says which variable is missing" $ do
        case resolve (configured {configAppName = ""}) (deployed {environmentCloud = True, environmentAppName = Nothing}) of
          Left err -> assertBool "names the variable" (cloudAppNameEnv `Text.isInfixOf` Text.pack (show err))
          other -> fail ("expected a config error, got: " <> show other),
      testCase "an app name is held to the rule every implementation shares" $ do
        mapM_ (\name -> assertBool (show name <> " should be accepted") (isRight (validateAppName name)))
          ["abc", "app_1", "a-b-c", Text.replicate 256 "a"]
        mapM_ (\name -> assertBool (show name <> " should be refused") (isLeft (validateAppName name)))
          ["", "ab", Text.replicate 257 "a", "Not A Name", "app!", "APP"],
      testCase "the resolved name is the one that is checked" $ do
        case resolve configured {configAppName = "Not A Name"} deployed of
          Left err -> assertBool "names the offending value" ("Not A Name" `Text.isInfixOf` Text.pack (show err))
          other -> fail ("expected a config error, got: " <> show other)
    ]

configured :: Config
configured =
  (configNew "config-app" "postgres://x")
    { configAppVersion = Just "config-v1",
      configExecutorId = Just "config-executor"
    }

deployed :: Environment
deployed =
  Environment
    { environmentCloud = False,
      environmentAppId = "app-id-7",
      environmentAppName = Just "env-app",
      environmentAppVersion = Just "env-v9",
      environmentExecutorId = Just "env-executor"
    }

expectResolved result = case result of
  Right identity -> pure identity
  Left err -> fail ("expected a resolved identity, got: " <> show err)

isRight :: Either a b -> Bool
isRight (Right _) = True
isRight _ = False

isLeft :: Either a b -> Bool
isLeft (Left _) = True
isLeft _ = False
