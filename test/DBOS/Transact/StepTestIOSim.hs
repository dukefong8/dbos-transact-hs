{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.StepRetryTest' mirrored under IOSim: the same retry,
-- predicate and timeout cases, with the real ported runner
-- ('runWorkflowStepWith') and backoff/timeouts on virtual time. The only
-- mock is the 'IOSimSystemDB' backend; the mock's @checkStep@ records
-- nothing, so the replay case asserts that the body runs again where the
-- live test asserts the checkpoint.
module DBOS.Transact.StepTestIOSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, runSimOrThrow, runSimTrace, selectTraceEventsDynamic)
import Data.Text (Text)
import DBOS.SystemDB (millisDuration)
import DBOS.SystemDB.IOSim (simConnection)
import DBOS.Transact
  ( Ctx,
    Error (..),
    Identity (..),
    WorkflowEvent (..),
    StepOptions (..),
    newCtx,
    newWorkflowState,
    nextExecutionIdentity,
    runWorkflowStep,
    runWorkflowStepWith,
    simTracer,
    stepOptionsDefault,
    withTracer,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Step retries (IOSim)"
    [ testCase "a step that fails twice succeeds on the third attempt" $ do
        run thirdAttempt @?= (Right 42, 3),
      testCase "exhausted retries carry every attempt's failure" $ do
        case run exhausted of
          (Left MaxStepRetriesExceeded {step, attempts = made, errors}, attemptsMade) -> do
            step @?= "doomed"
            made @?= 2
            length errors @?= 2
            attemptsMade @?= 2
          other -> fail ("expected MaxStepRetriesExceeded, got: " <> show other),
      testCase "the default does not retry and does not wrap" $ do
        run defaultOnce @?= (Left (StepFailed "plain" "boom"), 1),
      testCase "a retried step replays from its single checkpoint" $ do
        -- The mock records nothing, so the second call runs the body again;
        -- the live test asserts the checkpoint here.
        run replayed @?= (Right 7, Right 7, 3),
      testCase "a declined failure stops retrying immediately" $ do
        run declined @?= (Left (StepFailed "declined" "boom"), 1),
      testCase "declining mid-policy keeps the earlier failures" $ do
        case run declinedMid of
          (Left MaxStepRetriesExceeded {attempts = made, errors}, attemptsMade) -> do
            made @?= 2
            length errors @?= 2
            assertBool "the first failure is kept" (any (== StepFailed "pick" "first") errors)
            assertBool "the declining failure is kept" (any (== StepFailed "pick" "second") errors)
            attemptsMade @?= 2
          other -> fail ("expected MaxStepRetriesExceeded, got: " <> show other),
      testCase "a step that hangs is stopped at its timeout" $ do
        case run timedOut of
          Left StepTimeout {step} -> step @?= "slow"
          other -> fail ("expected StepTimeout, got: " <> show other),
      testCase "a step within its timeout is unaffected" $ do
        run withinTimeout @?= Right 9,
      testCase "a step run announces through the context tracer" $ do
        let traced = selectTraceEventsDynamic (runSimTrace tracedRun) :: [WorkflowEvent]
        traced @?= [StepRunning "traced" 0]
    ]

-- * The mirrored cases

thirdAttempt :: IOSim s (Either Error Int, Int)
thirdAttempt = do
  attempts <- newTVarIO (0 :: Int)
  context <- simCtx
  let body _ = do
        attempt <- readTVarIO attempts
        atomically (modifyTVar attempts (+ 1))
        if attempt < 2
          then pure (Left (StepFailed "flaky" "boom"))
          else pure (Right (42 :: Int))
      options = stepOptionsDefault {max_attempts = 3, interval = millisDuration 1}
  outcome <- runWorkflowStepWith options context "flaky" body
  made <- readTVarIO attempts
  pure (outcome, made)

exhausted :: IOSim s (Either Error Int, Int)
exhausted = do
  attempts <- newTVarIO (0 :: Int)
  context <- simCtx
  let body _ = do
        atomically (modifyTVar attempts (+ 1))
        pure (Left (StepFailed "doomed" "boom"))
      options = stepOptionsDefault {max_attempts = 2, interval = millisDuration 1}
  outcome <- runWorkflowStepWith options context "doomed" body
  made <- readTVarIO attempts
  pure (outcome, made)

defaultOnce :: IOSim s (Either Error Int, Int)
defaultOnce = do
  attempts <- newTVarIO (0 :: Int)
  context <- simCtx
  let body _ = do
        atomically (modifyTVar attempts (+ 1))
        pure (Left (StepFailed "plain" "boom"))
  outcome <- runWorkflowStepWith stepOptionsDefault context "plain" body
  made <- readTVarIO attempts
  pure (outcome, made)

replayed :: IOSim s (Either Error Int, Either Error Int, Int)
replayed = do
  attempts <- newTVarIO (0 :: Int)
  context <- simCtx
  let body _ = do
        attempt <- readTVarIO attempts
        atomically (modifyTVar attempts (+ 1))
        if attempt < 1
          then pure (Left (StepFailed "flaky" "boom"))
          else pure (Right (7 :: Int))
      options = stepOptionsDefault {max_attempts = 3, interval = millisDuration 1}
  first <- runWorkflowStepWith options context "flaky" body
  second <- runWorkflowStepWith options context "flaky" body
  made <- readTVarIO attempts
  pure (first, second, made)

declined :: IOSim s (Either Error Int, Int)
declined = do
  attempts <- newTVarIO (0 :: Int)
  context <- simCtx
  let body _ = do
        atomically (modifyTVar attempts (+ 1))
        pure (Left (StepFailed "declined" "boom"))
      options =
        stepOptionsDefault
          { max_attempts = 3,
            interval = millisDuration 1,
            should_retry = Just (const False)
          }
  outcome <- runWorkflowStepWith options context "declined" body
  made <- readTVarIO attempts
  pure (outcome, made)

declinedMid :: IOSim s (Either Error Int, Int)
declinedMid = do
  attempts <- newTVarIO (0 :: Int)
  context <- simCtx
  let body _ = do
        attempt <- readTVarIO attempts
        atomically (modifyTVar attempts (+ 1))
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
  made <- readTVarIO attempts
  pure (outcome, made)

timedOut :: IOSim s (Either Error Int)
timedOut = do
  context <- simCtx
  let body _ = threadDelay 50000 >> pure (Right (1 :: Int))
      options = (stepOptionsDefault :: StepOptions) {timeout = Just (millisDuration 5)}
  runWorkflowStepWith options context "slow" body

withinTimeout :: IOSim s (Either Error Int)
withinTimeout = do
  context <- simCtx
  let body _ = pure (Right (9 :: Int))
      options = (stepOptionsDefault :: StepOptions) {timeout = Just (millisDuration 500)}
  runWorkflowStepWith options context "quick" body

-- * Helpers

run :: (forall s. IOSim s a) -> a
run = runSimOrThrow

tracedRun :: IOSim s (Either Error Int)
tracedRun = do
  context <- withTracer simTracer <$> simCtx
  runWorkflowStep context "traced" (const (pure (1 :: Int)))

simCtx :: IOSim s (Ctx (IOSim s))
simCtx = do
  conn <- simConnection
  identity <- nextExecutionIdentity conn
  state <- newWorkflowState "sim-step" Nothing identity
  newCtx conn simIdentity state

simIdentity :: Identity
simIdentity =
  Identity
    { identityAppName = "sim-app",
      identityAppVersion = "0.0.0",
      identityExecutorId = "sim-executor",
      identityAppId = ""
    }
