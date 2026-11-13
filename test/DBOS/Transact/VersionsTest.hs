{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | An application reading its version registry and promoting a version:
-- deploying registers each version, listing reads newest first, and
-- promoting an older one rolls the fleet back without changing what the
-- running executor runs. The version calls need a launched instance, as
-- every call that reaches the database does. Mirrors the oracle's
-- @versions.rs@.
module DBOS.Transact.VersionsTest (tests) where

import DBOS.Internal.Prelude
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.Transact
  ( Config (..),
    DBOS,
    Environment (..),
    Error (..),
    VersionInfo (..),
    configFromEnv,
    latestAppVersion,
    launchWithEnvironment,
    listAppVersions,
    newDBOS,
    promoteAppVersion,
    promoteAppVersionFor,
    shutdown,
  )
import DBOS.Internal.Transact.Identity (Identity (..))
import DBOS.Internal.Transact.Instance (Executor (..))
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

-- | This test's environment: nothing claimed from the process, so versions
-- register under the config's own application.
isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }

tests :: TestTree
tests =
  testGroup
    "Application versions"
    [ testCase "an application lists its versions and promotes an older one" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-versions-app-" <> Text.take 12 suffix
            v1 = "hs-l2-versions-v1-" <> suffix
            v2 = "hs-l2-versions-v2-" <> suffix
        config0 <- configFromEnv appName
        -- One executor per version, in order, so each registers its own.
        let deploy version = do
              let config = config0 {configAppVersion = Just version, configExecutorId = Just ("exec-" <> version)}
              bracket (newDBOS config) shutdown $ \dbos -> do
                launched <- launchWithEnvironment dbos isolatedEnvironment
                case launched of
                  Left err -> fail (show err)
                  Right _ -> pure ()
        _ <- deploy v1
        let config2 = config0 {configAppVersion = Just v2, configExecutorId = Just ("exec-" <> v2)}
        bracket (newDBOS config2) shutdown $ \dbos -> do
          exec <- launchWithEnvironment dbos isolatedEnvironment >>= either (fail . show) pure
          listed <- listAppVersions dbos
          case listed of
            Left err -> fail (show err)
            Right versions -> do
              -- Judge within our own application's rows: the shared
              -- database holds other applications' versions.
              let names = [name | VersionInfo {versionInfoAppName = owner, versionInfoName = name} <- versions, owner == Just appName]
              assertBool ("expected ours newest first, got: " <> show names) (take 2 names == [v2, v1])
          latest <- latestAppVersion dbos
          case latest of
            Right (Just row) -> row.versionInfoName @?= v2
            other -> fail ("expected v2 latest, got: " <> show other)
          promoted <- promoteAppVersion dbos v1
          case promoted of
            Left err -> fail (show err)
            Right () -> pure ()
          rolled <- latestAppVersion dbos
          case rolled of
            Right (Just row) -> row.versionInfoName @?= v1
            other -> fail ("expected the rollback latest, got: " <> show other)
          -- This executor still runs its own version.
          exec.identity.identityAppVersion @?= v2,
      testCase "the version calls are refused before launch" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-versions-unlaunched-" <> Text.take 12 suffix
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just "v1", configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          listed <- listAppVersions dbos
          case listed of
            Left NotLaunched {} -> pure ()
            other -> fail ("expected a not-launched refusal, got: " <> show other)
          latest <- latestAppVersion dbos
          case latest of
            Left NotLaunched {} -> pure ()
            other -> fail ("expected a not-launched refusal, got: " <> show other)
          promoted <- promoteAppVersion dbos "v1"
          case promoted of
            Left NotLaunched {} -> pure ()
            other -> fail ("expected a not-launched refusal, got: " <> show other)
          promotedFor <- promoteAppVersionFor dbos "v1" "versions-peer"
          case promotedFor of
            Left NotLaunched {} -> pure ()
            other -> fail ("expected a not-launched refusal, got: " <> show other)
    ]
