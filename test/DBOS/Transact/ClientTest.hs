{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Public-behavior tests for the @client.rs@ outside-in surface.
module DBOS.Transact.ClientTest (tests) where

import DBOS.Prelude
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (WorkflowId (..))
import DBOS.Transact
  ( Client,
    ClientConfig (..),
    CodecError,
    Config (..),
    DuplicationPolicy (..),
    Enqueue (..),
    EnqueueOptions (..),
    Environment (..),
    Error (..),
    QueueConflict (..),
    Ctx,
    clientConfigFromEnv,
    clientConfigNew,
    closeClient,
    configFromEnv,
    connectClient,
    decodeWorkflowValue,
    defaultQueueOptions,
    dequeueDBOSWorkflows,
    encodeWorkflowValue,
    enqueueClientWorkflowWith,
    enqueueNew,
    enqueueOptionsNew,
    enqueueOptionsOn,
    handleResult,
    handleWorkflowId,
    launchWithEnvironment,
    newDBOS,
    newWorkflowKey,
    registerDBOSWorkflow,
    registerQueue,
    retrieveClientWorkflow,
    runDBOSWorkflow,
    runWorkflowStep,
    shutdown,
    storedPriority,
    validateClientConfig,
    validateEnqueue,
    workflowStatusClient,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Client"
    [ testCase "connect reports a missing url by the name of the variable that sets it" $ do
        case validateClientConfig (clientConfigNew "") of
          Left err ->
            assertBool "names the variable" ("DBOS_DATABASE_URL" `Text.isInfixOf` Text.pack (show err))
          Right () -> fail "expected the empty url to be refused",
      testCase "a client enqueues by name and the app runs it" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-" <> Text.take 12 suffix
            appVersion = "hs-l2-client-version-" <> suffix
            executorId = "hs-l2-client-executor-" <> suffix
            queueName = "hs-l2-client-q-" <> Text.take 12 suffix
            key = newWorkflowKey "double"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {app_name = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            -- Pinned to the app's version: an unversioned row is only
            -- claimed by the latest registered version, and this shared
            -- database always has a newer nameless row.
            let options = (enqueueOptionsNew queueName) {app_version = Just appVersion}
            enqueued <- enqueueClientWorkflowWith client "double" options (Just (encodeWorkflowValue (21 :: Int)))
            case enqueued of
              Left err -> fail (show err)
              Right handle -> do
                _ <- dequeueDBOSWorkflows dbos
                result <- handleResult handle
                case result of
                  Right (Just stored) -> do
                    let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                    assertEqual "the app ran the client's enqueue" (Right 42) decoded
                  other -> fail (show other),
      testCase "an enqueue no queue could honour is refused before any write" $ do
        let both = (enqueueNew "q") {deduplication_id = Just "key", partition_key = Just "part"}
        case validateEnqueue both of
          Left err ->
            assertBool "names the queue" ("enqueue onto `q`" `Text.isInfixOf` Text.pack (show err))
          Right () -> fail "expected dedup plus partition to be refused"
        let noKey = (enqueueNew "q") {duplication_policy = ReturnExisting}
        case validateEnqueue noKey of
          Left _ -> pure ()
          Right () -> fail "expected return-existing without a key to be refused"
        let zero = (enqueueNew "q") {priority = Just 0}
        case validateEnqueue zero of
          Left _ -> pure ()
          Right () -> fail "expected priority zero to be refused"
        storedPriority (enqueueNew "q") @?= 0
        storedPriority ((enqueueNew "q") {priority = Just 3}) @?= 3,
      testCase "a second enqueue under a held key joins the holder" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-dup-" <> Text.take 12 suffix
            appVersion = "hs-l2-client-dup-version-" <> suffix
            executorId = "hs-l2-client-dup-executor-" <> suffix
            queueName = "hs-l2-client-dup-q-" <> Text.take 12 suffix
            key = newWorkflowKey "double"
            shape = (enqueueNew queueName) {deduplication_id = Just ("dup-" <> suffix), duplication_policy = ReturnExisting}
            options = (enqueueOptionsOn shape) {app_version = Just appVersion}
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {app_name = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            first <- enqueueClientWorkflowWith client "double" options (Just (encodeWorkflowValue (21 :: Int)))
            second <- enqueueClientWorkflowWith client "double" options (Just (encodeWorkflowValue (99 :: Int)))
            case (first, second) of
              (Right holder, Right joiner) -> do
                handleWorkflowId joiner @?= handleWorkflowId holder
                _ <- dequeueDBOSWorkflows dbos
                result <- handleResult joiner
                case result of
                  Right (Just stored) -> do
                    let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                    assertEqual "the joiner waits on the holder's run" (Right 42) decoded
                  other -> fail (show other)
              (Left err, _) -> fail ("the first enqueue failed: " <> show err)
              (_, Left err) -> fail ("the joining enqueue failed: " <> show err),
      testCase "a client retrieves by id and reads status" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-ret-" <> Text.take 12 suffix
            appVersion = "hs-l2-client-ret-version-" <> suffix
            executorId = "hs-l2-client-ret-executor-" <> suffix
            workflowText = "hs-l2-client-ret-id-" <> suffix
            key = newWorkflowKey "double"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runDBOSWorkflow dbos key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {app_name = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            let handle = retrieveClientWorkflow client workflowText
            handleWorkflowId handle @?= workflowText
            status <- workflowStatusClient client (WorkflowId workflowText)
            case status of
              Right (Just _) -> pure ()
              other -> fail (show other)
    ]

-- | Connects or fails the test with the engine error rendered.
connectOrFail :: ClientConfig -> IO (Client IO)
connectOrFail config = do
  connected <- connectClient config
  case connected of
    Left err -> fail (show err)
    Right client -> pure client

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }
