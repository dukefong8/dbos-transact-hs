{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Instance lifecycle behavior through the public DBOS facade.
module DBOS.Transact.InstanceTest (tests) where

import DBOS.Prelude
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (AwaitedOutcome (..))
import DBOS.Transact
  ( Config (..),
    Environment (..),
    Serializer (..),
    Ctx,
    WorkflowId (..),
    cancelWorkflows,
    configFromEnv,
    encodeWorkflowValue,
    enqueueDBOSWorkflow,
    isLaunched,
    launchWithEnvironment,
    newDBOS,
    newWorkflowKey,
    registerDBOSWorkflow,
    renderTransactError,
    shutdown,
    waitForWorkflow,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase)

tests :: TestTree
tests =
  testGroup
    "DBOS instance"
    [ testCase "an invalid config is refused before connecting" $ do
        let invalid =
              Config
                { configAppName = "hs-invalid",
                  configDatabaseUrl = "",
                  configMaxConnections = 10,
                  configSchema = "dbos",
                  configExecutorId = Nothing,
                  configAppVersion = Just "test-version",
                  configSerializer = RustSerde,
                  configUseListenNotify = True,
                  configMigrate = True,
                  configPollingConcurrency = Nothing,
                  configOutcomePollInterval = Nothing,
                  configListenQueues = Nothing,
                  configNotificationCoalesce = Nothing
                }
        dbos <- newDBOS invalid
        started <- launchWithEnvironment dbos isolatedEnvironment
        case started of
          Left err -> assertBool "names the missing database URL" ("database URL" `Text.isInfixOf` renderTransactError err)
          Right () -> fail "expected an empty database URL to be refused"
        assertEqual "failed launch does not install an executor" False =<< isLaunched dbos,
      testCase "launch installs an executor until idempotent shutdown" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-" <> Text.take 16 suffix
            appVersion = "hs-l2-version-" <> suffix
            executorId = "hs-l2-executor-" <> suffix
        base <- configFromEnv appName
        let configured = base {configAppVersion = Just appVersion, configExecutorId = Just executorId}
        dbos <- newDBOS configured
        let echoWorkflow :: Text -> Ctx IO -> IO (Either e Text)
            echoWorkflow message _ = pure (Right message)
        beforeLaunch <- registerDBOSWorkflow dbos (newWorkflowKey "greeting") echoWorkflow
        case beforeLaunch of
          Left err -> fail (Text.unpack (renderTransactError err))
          Right () -> pure ()
        assertEqual "new instance is unlaunched" False =<< isLaunched dbos
        started <- launchWithEnvironment dbos isolatedEnvironment
        case started of
          Left err -> fail (Text.unpack (renderTransactError err))
          Right () -> pure ()
        assertEqual "launch installs the executor" True =<< isLaunched dbos
        missing <- waitForWorkflow dbos (WorkflowId ("hs-l2-missing-" <> suffix))
        case missing of
          Left err -> assertBool "reports the missing workflow" ("no such workflow" `Text.isInfixOf` renderTransactError err)
          Right _ -> fail "expected waiting for a missing workflow to fail"
        afterLaunch <- registerDBOSWorkflow dbos (newWorkflowKey "late") echoWorkflow
        case afterLaunch of
          Left err -> assertBool "names the lifecycle boundary" ("after DBOS is launched" `Text.isInfixOf` renderTransactError err)
          Right () -> fail "expected registration after launch to be refused"
        shutdown dbos
        shutdown dbos
        assertEqual "shutdown is idempotent" False =<< isLaunched dbos,
      testCase "an enqueued workflow can be cancelled through the instance" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-cancel-" <> Text.take 12 suffix
            appVersion = "hs-l2-version-" <> suffix
            executorId = "hs-l2-executor-" <> suffix
            workflowId = WorkflowId ("hs-l2-cancel-wf-" <> suffix)
        base <- configFromEnv appName
        let configured = base {configAppVersion = Just appVersion, configExecutorId = Just executorId, configListenQueues = Just []}
            echoWorkflow :: Text -> Ctx IO -> IO (Either e Text)
            echoWorkflow message _ = pure (Right message)
        bracket (newDBOS configured) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos (newWorkflowKey "queued") echoWorkflow
          case registered of
            Left err -> fail (Text.unpack (renderTransactError err))
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (Text.unpack (renderTransactError err))
            Right () -> pure ()
          enqueued <-
            enqueueDBOSWorkflow
              dbos
              (newWorkflowKey "queued")
              workflowId
              (Just (encodeWorkflowValue ("hello" :: Text)))
              ("cancel-" <> suffix)
          case enqueued of
            Left err -> fail (Text.unpack (renderTransactError err))
            Right _ -> pure ()
          cancelled <- cancelWorkflows dbos [workflowId] False
          assertEqual "the selected workflow is cancelled" (Right [workflowId]) cancelled
          outcome <- waitForWorkflow dbos workflowId
          assertEqual "the waiter observes cancellation" (Right AwaitedCancelled) outcome
    ]

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }
