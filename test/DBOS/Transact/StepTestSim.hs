{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.StepRetryTest' mirrored under IOSim: the same retry,
-- predicate and timeout cases, with the real ported runner
-- ('runWorkflowStepWith') and backoff/timeouts on virtual time, each case
-- printing its sim's 'Say' trace inline so a plain @-- $> tasty@ run
-- shows announcements with no extra plumbing. Every case asserts its
-- behavior and its exact 'WorkflowEvent' trace — the typed asserts behind
-- the announcements the live tree writes through FastLogger. The replay
-- case runs over 'MemSystemDB', whose stateful steps let the second call
-- read the recorded checkpoint back genuinely (the stateless mock runs
-- the body again); the control-end and preemption emits have no sim case
-- (no sim backend fails a body with a control error or parks a row) and
-- are covered structurally against @step.rs@.
module DBOS.Transact.StepTestSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import Data.Text (Text)
import DBOS.SystemDB (millisDuration)
import DBOS.IOSimTracer (printSimTrace, runSimCase, simTracer)
import DBOS.SystemDB.IOSim (memConnectionOn, newMemDB, simConnectionWith)
import DBOS.Transact
  ( Ctx,
    EngineOnly,
    Error (..),
    Identity (..),
    StepOptions (..),
    WorkflowEvent (..),
    firstStepStatus,
    newCtx,
    newWorkflowState,
    nextExecutionIdentity,
    nextStepId,
    nextStepMarker,
    renderTransactError,
    runWorkflowStep,
    runWorkflowStepWith,
    stepOptionsDefault,
    withAttempt,
  )
import Test.Tasty (DependencyType (..), TestTree, dependentTestGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

simIdentity :: Identity
simIdentity =
  Identity
    { identityAppName = "sim-app",
      identityAppVersion = "0.0.0",
      identityExecutorId = "sim-executor",
      identityAppId = ""
    }

simCtx :: Text -> IOSim s (Ctx (IOSim s))
simCtx name = do
  conn <- simConnectionWith simTracer
  identity <- nextExecutionIdentity conn
  state <- newWorkflowState name Nothing identity
  newCtx conn simIdentity state

tests :: TestTree
tests =
  -- Sequential: cases announce through one shared stderr, so parallel
  -- 'printSimTrace' calls would interleave their lines mid-character.
  -- 'AllFinish' keeps every case running on a failure, in order.
  dependentTestGroup
    "Step retries (Sim)"
    AllFinish
    [ testCase "a step that fails twice succeeds on the third attempt" $ do
        ((outcome, made), tr) <- runSimCase thirdAttempt
        printSimTrace tr
        (outcome, made) @?= (Right 42, 3)
        traceEvents tr
          @?= [ StepRetrying "flaky" 0 1 3 1 (renderTransactError (StepFailed "flaky" "boom" :: (Error EngineOnly))),
                StepRetrying "flaky" 0 2 3 2 (renderTransactError (StepFailed "flaky" "boom" :: (Error EngineOnly))),
                StepOutputRecorded "flaky" 0
              ],
      testCase "exhausted retries carry every attempt's failure" $ do
        ((outcome, made), tr) <- runSimCase exhausted
        printSimTrace tr
        case (outcome, made) of
          (Left MaxStepRetriesExceeded {step, attempts, errors}, attemptsMade) -> do
            step @?= "doomed"
            attempts @?= 2
            length errors @?= 2
            attemptsMade @?= 2
          other -> fail ("expected MaxStepRetriesExceeded, got: " <> show other)
        -- The second attempt exhausts the policy, so only the first
        -- failure warns; the recorded error outcome still announces.
        traceEvents tr
          @?= [ StepRetrying "doomed" 0 1 2 1 (renderTransactError (StepFailed "doomed" "boom" :: (Error EngineOnly))),
                StepErrorRecorded "doomed" 0
              ],
      testCase "the default does not retry and does not wrap" $ do
        ((outcome, made), tr) <- runSimCase defaultOnce
        printSimTrace tr
        (outcome, made) @?= (Left (StepFailed "plain" "boom"), 1)
        traceEvents tr @?= [StepErrorRecorded "plain" 0],
      testCase "a retried step replays from its single checkpoint" $ do
        ((first, second, made), tr) <- runSimCase replayed
        printSimTrace tr
        -- The mock records nothing, so the live test asserts the
        -- checkpoint here; over stateful steps the replay is genuine and
        -- the body runs once, not twice.
        (first, second, made) @?= (Right 7, Right 7, 2)
        traceEvents tr
          @?= [ StepRetrying "flaky" 0 1 3 1 (renderTransactError (StepFailed "flaky" "boom" :: (Error EngineOnly))),
                StepOutputRecorded "flaky" 0,
                StepReplaying "flaky" 0
              ],
      testCase "a declined failure stops retrying immediately" $ do
        ((outcome, made), tr) <- runSimCase declined
        printSimTrace tr
        (outcome, made) @?= (Left (StepFailed "declined" "boom"), 1)
        traceEvents tr
          @?= [ StepDeclined "declined" 0 1 (renderTransactError (StepFailed "declined" "boom" :: (Error EngineOnly))),
                StepErrorRecorded "declined" 0
              ],
      testCase "declining mid-policy keeps the earlier failures" $ do
        ((outcome, made), tr) <- runSimCase declinedMid
        printSimTrace tr
        case (outcome, made) of
          (Left MaxStepRetriesExceeded {attempts, errors}, attemptsMade) -> do
            attempts @?= 2
            length errors @?= 2
            assertBool "the first failure is kept" (any (== StepFailed "pick" "first") errors)
            assertBool "the declining failure is kept" (any (== StepFailed "pick" "second") errors)
            attemptsMade @?= 2
          other -> fail ("expected MaxStepRetriesExceeded, got: " <> show other)
        traceEvents tr
          @?= [ StepRetrying "pick" 0 1 3 1 (renderTransactError (StepFailed "pick" "first" :: (Error EngineOnly))),
                StepDeclined "pick" 0 2 (renderTransactError (StepFailed "pick" "second" :: (Error EngineOnly))),
                StepErrorRecorded "pick" 0
              ],
      testCase "a step that hangs is stopped at its timeout" $ do
        (outcome, tr) <- runSimCase timedOut
        printSimTrace tr
        case outcome of
          Left StepTimeout {step} -> step @?= "slow"
          other -> fail ("expected StepTimeout, got: " <> show other)
        traceEvents tr @?= [StepAttemptTimedOut "slow" 0 5, StepErrorRecorded "slow" 0],
      testCase "a step within its timeout is unaffected" $ do
        (outcome, tr) <- runSimCase withinTimeout
        printSimTrace tr
        outcome @?= Right 9
        traceEvents tr @?= [StepOutputRecorded "quick" 0],
      testCase "a step run announces through the context tracer" $ do
        (outcome, tr) <- runSimCase tracedRun
        printSimTrace tr
        outcome @?= Right 1
        traceEvents tr @?= [StepRunning "traced" 0, StepOutputRecorded "traced" 0],
      testCase "a step inside a step runs plainly" $ do
        (outcome, tr) <- runSimCase plainRun
        printSimTrace tr
        outcome @?= Right 3
        traceEvents tr @?= [StepPlain "inner"]
    ]

traceEvents :: SimTrace a -> [WorkflowEvent]
traceEvents = selectTraceEventsDynamic

-- * The mirrored cases

thirdAttempt :: IOSim s (Either (Error EngineOnly) Int, Int)
thirdAttempt = do
  attempts <- newTVarIO (0 :: Int)
  context <- simCtx "sim-step"
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

exhausted :: IOSim s (Either (Error EngineOnly) Int, Int)
exhausted = do
  attempts <- newTVarIO (0 :: Int)
  context <- simCtx "sim-step"
  let body _ = do
        atomically (modifyTVar attempts (+ 1))
        pure (Left (StepFailed "doomed" "boom"))
      options = stepOptionsDefault {max_attempts = 2, interval = millisDuration 1}
  outcome <- runWorkflowStepWith options context "doomed" body
  made <- readTVarIO attempts
  pure (outcome, made)

defaultOnce :: IOSim s (Either (Error EngineOnly) Int, Int)
defaultOnce = do
  attempts <- newTVarIO (0 :: Int)
  context <- simCtx "sim-step"
  let body _ = do
        atomically (modifyTVar attempts (+ 1))
        pure (Left (StepFailed "plain" "boom"))
  outcome <- runWorkflowStepWith stepOptionsDefault context "plain" body
  made <- readTVarIO attempts
  pure (outcome, made)

replayed :: IOSim s (Either (Error EngineOnly) Int, Either (Error EngineOnly) Int, Int)
replayed = do
  mem <- newMemDB
  conn <- memConnectionOn mem simTracer
  attempts <- newTVarIO (0 :: Int)
  let runOnce = do
        identity <- nextExecutionIdentity conn
        state <- newWorkflowState "sim-step-replay" Nothing identity
        context <- newCtx conn simIdentity state
        let body _ = do
              attempt <- readTVarIO attempts
              atomically (modifyTVar attempts (+ 1))
              if attempt < 1
                then pure (Left (StepFailed "flaky" "boom"))
                else pure (Right (7 :: Int))
            options = stepOptionsDefault {max_attempts = 3, interval = millisDuration 1}
        runWorkflowStepWith options context "flaky" body
  first <- runOnce
  second <- runOnce
  made <- readTVarIO attempts
  pure (first, second, made)

declined :: IOSim s (Either (Error EngineOnly) Int, Int)
declined = do
  attempts <- newTVarIO (0 :: Int)
  context <- simCtx "sim-step"
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

declinedMid :: IOSim s (Either (Error EngineOnly) Int, Int)
declinedMid = do
  attempts <- newTVarIO (0 :: Int)
  context <- simCtx "sim-step"
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

timedOut :: IOSim s (Either (Error EngineOnly) Int)
timedOut = do
  context <- simCtx "sim-step"
  let body _ = threadDelay 50000 >> pure (Right (1 :: Int))
      options = (stepOptionsDefault :: StepOptions EngineOnly) {timeout = Just (millisDuration 5)}
  runWorkflowStepWith options context "slow" body

withinTimeout :: IOSim s (Either (Error EngineOnly) Int)
withinTimeout = do
  context <- simCtx "sim-step"
  let body _ = pure (Right (9 :: Int))
      options = (stepOptionsDefault :: StepOptions EngineOnly) {timeout = Just (millisDuration 500)}
  runWorkflowStepWith options context "quick" body

tracedRun :: IOSim s (Either (Error EngineOnly) Int)
tracedRun = do
  context <- simCtx "sim-step"
  runWorkflowStep context "traced" (const (pure (1 :: Int)))

plainRun :: IOSim s (Either (Error EngineOnly) Int)
plainRun = do
  context <- simCtx "sim-step-plain"
  marker <- nextStepMarker context
  withAttempt context marker (firstStepStatus 0) $ \inner ->
    runWorkflowStepWith stepOptionsDefault inner "inner" (const (pure (Right (3 :: Int))))
