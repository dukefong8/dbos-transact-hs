{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Workflow deadlines against the live backend, ported from Rust
-- @tests/deadlines.rs@: a run inside its budget is unaffected, and one that
-- outlives it is cancelled durably and reports the cancellation.
module DBOS.Transact.DeadlinesTest (tests) where

import DBOS.Prelude
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (WorkflowId (..), millisDuration, secondsDuration)
import DBOS.SystemDB qualified as SystemDB
import DBOS.Transact
  ( CodecError,
    Config (..),
    Ctx,
    Environment (..),
    Error (..),
    RunOptions (..),
    Timeout (..),
    WorkflowStatus (..),
    configFromEnv,
    decodeWorkflowValue,
    encodeWorkflowValue,
    handleStatus,
    launchWithEnvironment,
    newDBOS,
    newWorkflowKey,
    registerDBOSWorkflowRef,
    retrieveWorkflow,
    runDBOSWorkflowRef,
    runOptionsDefault,
    shutdown,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertEqual, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Workflow deadlines"
    [ testCase "a workflow within its deadline is unaffected" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-within-deadline-" <> Text.take 12 suffix
            workflowText = "hs-l2-within-deadline-id-" <> suffix
            key = newWorkflowKey "quick"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: () -> Ctx IO -> IO (Either Error Int)
            body () _ = pure (Right 7)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRef dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <-
            runDBOSWorkflowRef
              dbos
              ref
              (runOptionsDefault {runWorkflowId = Just workflowText, runTimeout = Explicit (secondsDuration 30)})
              Nothing
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "a run inside its budget records its result" (Right 7) decoded
            other -> fail (show other)
          retrieved <- retrieveWorkflow dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              status <- handleStatus handle
              case status of
                Right (Just Success) -> pure ()
                other -> fail ("expected the row SUCCESS, got: " <> show other),
      testCase "a workflow past its deadline is cancelled" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-past-deadline-" <> Text.take 12 suffix
            workflowText = "hs-l2-past-deadline-id-" <> suffix
            key = newWorkflowKey "runs-forever"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: () -> Ctx IO -> IO (Either Error Int)
            body () _ = do
              threadDelay 30000000
              pure (Right 1)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRef dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <-
            runDBOSWorkflowRef
              dbos
              ref
              (runOptionsDefault {runWorkflowId = Just workflowText, runTimeout = Explicit (millisDuration 100)})
              Nothing
          case ran of
            Left (ErrorSystemDatabase (SystemDB.WorkflowCancelled {workflowId})) -> workflowId @?= workflowText
            other -> fail ("expected the cancellation, got: " <> show other)
          retrieved <- retrieveWorkflow dbos (WorkflowId workflowText)
          case retrieved of
            Left err -> fail (show err)
            Right handle -> do
              status <- handleStatus handle
              case status of
                Right (Just Cancelled) -> pure ()
                other -> fail ("expected the row CANCELLED, got: " <> show other)
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
