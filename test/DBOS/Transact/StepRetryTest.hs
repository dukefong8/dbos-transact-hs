{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The step retry seam, mirroring Rust @tests/retries.rs@ and the timeout
-- cases of @tests/timeouts.rs@: attempts, backoff, the retry predicate,
-- single-checkpoint replay, per-attempt timeouts, and the two engine errors
-- (@StepTimeout@, @MaxStepRetriesExceeded@). Live database.
module DBOS.Transact.StepRetryTest (tests) where

import DBOS.Prelude
import Colog.Core.Action (LogAction (..))
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (NewWorkflow (..), Submission (..), millisDuration, newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  ( Ctx,
    Error (..),
    StepOptions (..),
    runWorkflowStepWith,
    stepOptionsDefault,
  )
import DBOS.Transact.ContextTest (ctxOver)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Step retries"
    [ testCase "a step that fails twice succeeds on the third attempt" $ withWorkflow "retry-third" $ \backend workflowText -> do
        attempts <- newIORef (0 :: Int)
        context <- ctxOver backend workflowText
        let body :: Ctx IO -> IO (Either Error Int)
            body _ = do
              attempt <- readIORef attempts
              modifyIORef' attempts (+ 1)
              if attempt < 2
                then pure (Left (StepFailed "flaky" "boom"))
                else pure (Right (42 :: Int))
            options = stepOptionsDefault {max_attempts = 3, interval = millisDuration 1}
        outcome <- runWorkflowStepWith options context "flaky" body
        outcome @?= Right 42
        readIORef attempts >>= (@?= 3),
      testCase "exhausted retries carry every attempt's failure" $ withWorkflow "retry-exhausted" $ \backend workflowText -> do
        attempts <- newIORef (0 :: Int)
        context <- ctxOver backend workflowText
        let body :: Ctx IO -> IO (Either Error Int)
            body _ = do
              modifyIORef' attempts (+ 1)
              pure (Left (StepFailed "doomed" "boom"))
            options = stepOptionsDefault {max_attempts = 2, interval = millisDuration 1}
        outcome <- runWorkflowStepWith options context "doomed" body
        case outcome of
          Left MaxStepRetriesExceeded {step, attempts = made, errors} -> do
            step @?= "doomed"
            made @?= 2
            length errors @?= 2
          other -> fail ("expected MaxStepRetriesExceeded, got: " <> show other)
        readIORef attempts >>= (@?= 2),
      testCase "the default does not retry and does not wrap" $ withWorkflow "retry-default" $ \backend workflowText -> do
        attempts <- newIORef (0 :: Int)
        context <- ctxOver backend workflowText
        let body :: Ctx IO -> IO (Either Error Int)
            body _ = do
              modifyIORef' attempts (+ 1)
              pure (Left (StepFailed "plain" "boom"))
        outcome <- runWorkflowStepWith stepOptionsDefault context "plain" body
        outcome @?= Left (StepFailed "plain" "boom")
        readIORef attempts >>= (@?= 1),
      testCase "a retried step replays from its single checkpoint" $ withWorkflow "retry-replay" $ \backend workflowText -> do
        attempts <- newIORef (0 :: Int)
        firstContext <- ctxOver backend workflowText
        let body :: Ctx IO -> IO (Either Error Int)
            body _ = do
              attempt <- readIORef attempts
              modifyIORef' attempts (+ 1)
              if attempt < 1
                then pure (Left (StepFailed "flaky" "boom"))
                else pure (Right (7 :: Int))
            options = stepOptionsDefault {max_attempts = 3, interval = millisDuration 1}
        first <- runWorkflowStepWith options firstContext "flaky" body
        first @?= Right 7
        readIORef attempts >>= (@?= 2)
        replayContext <- ctxOver backend workflowText
        replayed <- runWorkflowStepWith options replayContext "flaky" body
        replayed @?= Right 7
        readIORef attempts >>= (@?= 2),
      testCase "a declined failure stops retrying immediately" $ withWorkflow "retry-declined" $ \backend workflowText -> do
        attempts <- newIORef (0 :: Int)
        context <- ctxOver backend workflowText
        let body :: Ctx IO -> IO (Either Error Int)
            body _ = do
              modifyIORef' attempts (+ 1)
              pure (Left (StepFailed "declined" "boom"))
            options =
              stepOptionsDefault
                { max_attempts = 3,
                  interval = millisDuration 1,
                  should_retry = Just (const False)
                }
        outcome <- runWorkflowStepWith options context "declined" body
        outcome @?= Left (StepFailed "declined" "boom")
        readIORef attempts >>= (@?= 1),
      testCase "declining mid-policy keeps the earlier failures" $ withWorkflow "retry-mid-decline" $ \backend workflowText -> do
        attempts <- newIORef (0 :: Int)
        context <- ctxOver backend workflowText
        let body :: Ctx IO -> IO (Either Error Int)
            body _ = do
              attempt <- readIORef attempts
              modifyIORef' attempts (+ 1)
              pure (Left (StepFailed "pick" (if attempt == 0 then "first" else "second")))
            declinesSecond err = case err of
              StepFailed _ message -> message /= "second"
              _ -> True
            options =
              stepOptionsDefault
                { max_attempts = 3,
                  interval = millisDuration 1,
                  should_retry = Just declinesSecond
                }
        outcome <- runWorkflowStepWith options context "pick" body
        case outcome of
          Left MaxStepRetriesExceeded {attempts = made, errors} -> do
            made @?= 2
            length errors @?= 2
            assertBool "the first failure is kept" (any (== StepFailed "pick" "first") errors)
            assertBool "the declining failure is kept" (any (== StepFailed "pick" "second") errors)
          other -> fail ("expected MaxStepRetriesExceeded, got: " <> show other)
        readIORef attempts >>= (@?= 2),
      testCase "a step that hangs is stopped at its timeout" $ withWorkflow "retry-timeout" $ \backend workflowText -> do
        context <- ctxOver backend workflowText
        let body :: Ctx IO -> IO (Either Error Int)
            body _ = threadDelay 50000 >> pure (Right (1 :: Int))
            options = (stepOptionsDefault :: StepOptions) {timeout = Just (millisDuration 5)}
        outcome <- runWorkflowStepWith options context "slow" body
        case outcome of
          Left StepTimeout {step} -> step @?= "slow"
          other -> fail ("expected StepTimeout, got: " <> show other),
      testCase "a step within its timeout is unaffected" $ withWorkflow "retry-within" $ \backend workflowText -> do
        context <- ctxOver backend workflowText
        let body :: Ctx IO -> IO (Either Error Int)
            body _ = pure (Right (9 :: Int))
            options = (stepOptionsDefault :: StepOptions) {timeout = Just (millisDuration 500)}
        outcome <- runWorkflowStepWith options context "quick" body
        outcome @?= Right 9
    ]

-- | Create the workflow row the step checkpoints against, then run the test
-- with its backend and workflow id.
withWorkflow :: Text -> (Postgres.PostgresSystemDB -> Text -> IO a) -> IO a
withWorkflow label action = do
  config <- Postgres.configFromEnv
  let logger = LogAction (const (pure ()))
  bracket (Postgres.acquirePostgresSystemDB config logger) Postgres.releasePostgresSystemDB $ \backend -> do
    Postgres.activatePostgresSystemDB backend
    freshId <- UUID.V4.nextRandom
    let workflowText = "hs-l2-" <> label <> "-" <> Text.pack (UUID.toString freshId)
        initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2StepRetryTest"}
    created <- SystemDB.initWorkflow backend initialWorkflow Nothing Fresh Nothing
    case created of
      Left err -> fail (show err)
      Right _ -> action backend workflowText
