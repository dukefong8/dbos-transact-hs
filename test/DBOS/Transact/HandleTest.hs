{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Public-behavior tests for the @handle.rs@ workflow handle.
module DBOS.Transact.HandleTest (tests) where

import DBOS.Prelude
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (WorkflowId (..))
import DBOS.Transact
  ( CodecError,
    Config (..),
    Environment (..),
    Error (..),
    Ctx,
    configFromEnv,
    decodeWorkflowValue,
    deleteWorkflows,
    encodeWorkflowValue,
    handleResult,
    handleStatus,
    handleWorkflowId,
    launchWithEnvironment,
    newDBOS,
    newWorkflowKey,
    registerDBOSWorkflow,
    renderTransactError,
    retrieveWorkflow,
    runDBOSWorkflow,
    runWorkflowStep,
    shutdown,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertEqual, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Workflow handle"
    [ testCase "a retrieved handle names its workflow and reads its status" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-handle-" <> Text.take 12 suffix
            appVersion = "hs-l2-handle-version-" <> suffix
            executorId = "hs-l2-handle-executor-" <> suffix
            workflowText = "hs-l2-handle-id-" <> suffix
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
          retrieved <- retrieveWorkflow dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              handleWorkflowId handle @?= workflowText
              status <- handleStatus handle
              case status of
                Right (Just _) -> pure ()
                other -> fail (show other),
      testCase "a handle result adopts the recorded output" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-handle-res-" <> Text.take 12 suffix
            appVersion = "hs-l2-handle-res-version-" <> suffix
            executorId = "hs-l2-handle-res-executor-" <> suffix
            workflowText = "hs-l2-handle-res-id-" <> suffix
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
          retrieved <- retrieveWorkflow dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              result <- handleResult handle
              case result of
                Right (Just stored) -> do
                  let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                  assertEqual "handle adopts the recorded output" (Right 42) decoded
                other -> fail (show other),
      testCase "a handle result reports the error a failed run recorded" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-handle-fail-" <> Text.take 12 suffix
            workflowText = "hs-l2-handle-fail-id-" <> suffix
            key = newWorkflowKey "fails"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body _ _ = pure (Left (StepFailed "body" "boom"))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runDBOSWorkflow dbos key (WorkflowId workflowText) (Just (encodeWorkflowValue (1 :: Int)))
          case ran of
            Left _ -> pure ()
            Right other -> fail ("expected the run to fail, got: " <> show other)
          retrieved <- retrieveWorkflow dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              result <- handleResult handle
              case result of
                Left (ErrorWorkflowFailed failedId message) -> do
                  failedId @?= workflowText
                  -- The polling path decodes the recorded failure with the
                  -- same fidelity the local run reported: the whole rendered
                  -- error, not a fragment. (The oracle's typed variant —
                  -- fields and all — waits on the typed-IO phase; untyped
                  -- text is the whole channel here.)
                  message @?= renderTransactError (StepFailed "body" "boom")
                other -> fail ("expected the recorded failure, got: " <> show other),
      testCase "a handle over a deleted row reports its absence" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-handle-del-" <> Text.take 12 suffix
            workflowText = "hs-l2-handle-del-id-" <> suffix
            key = newWorkflowKey "delete-me"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body value _ = pure (Right (value + 1))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runDBOSWorkflow dbos key (WorkflowId workflowText) (Just (encodeWorkflowValue (1 :: Int)))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          deleted <- deleteWorkflows dbos [WorkflowId workflowText] True
          case deleted of
            Left err -> fail (show err)
            Right count -> count @?= 1
          retrieved <- retrieveWorkflow dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              status <- handleStatus handle
              status @?= Right Nothing,
      testCase "dropping a handle does not stop the workflow" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-handle-drop-" <> Text.take 12 suffix
            workflowText = "hs-l2-handle-drop-id-" <> suffix
            key = newWorkflowKey "double"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
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
          worker <- async (runDBOSWorkflow dbos key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int))))
          -- Retrieved and immediately dropped while the run is in flight.
          _ <- retrieveWorkflow dbos (WorkflowId workflowText)
          outcome <- wait worker
          case outcome of
            Left err -> fail (show err)
            Right _ -> pure ()
          retrieved <- retrieveWorkflow dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              result <- handleResult handle
              case result of
                Right (Just stored) -> do
                  let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                  assertEqual "a fresh handle reads the completed result" (Right 42) decoded
                other -> fail (show other)
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
