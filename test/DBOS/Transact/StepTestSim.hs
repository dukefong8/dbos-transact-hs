{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | The step-runner cases under IOSim: 'runStep', nested and pending steps
-- through the workflow and step views, each case printing its sim's 'Say'
-- trace inline so a plain @-- $> tasty@ run shows announcements with no
-- extra plumbing. Every case asserts its behavior and its exact
-- 'WorkflowEvent' trace. The retry, predicate, timeout, preemption, and
-- cancellation cases moved to 'DBOS.Transact.StepRetryTestSim', sharing
-- scenarios and checks with 'DBOS.Transact.StepRetryTest'.
module DBOS.Transact.StepTestSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import Data.Text (Text)
import DBOS.IOSimTracer (printSimTrace, runSimCase, simTracer)
import DBOS.SystemDB.IOSim (simConnectionWith)
import DBOS.Transact
  ( EngineOnly,
    Error (..),
    Identity (..),
    StepStatus,
    PendingStep (..),
    WorkflowCtx,
    WorkflowEvent (..),
    WorkflowId (..),
    firstStepStatus,
    nextWorkflowMarker,
    pendingStep,
    runNestedStep,
    runStep,
    runStepWith,
    stepCtxStatus,
    stepOptionsDefault,
    withStep,
    withWorkflow,
  )
import DBOS.Transact.Checkpoint (pendingStepId)
import Test.Tasty (DependencyType (..), TestTree, dependentTestGroup)
import Test.Tasty.HUnit (testCase, (@?=))

simIdentity :: Identity
simIdentity =
  Identity
    { identityAppName = "sim-app",
      identityAppVersion = "0.0.0",
      identityExecutorId = "sim-executor",
      identityAppId = ""
    }

simRun :: Text -> (forall exec. WorkflowCtx exec (IOSim s) -> IOSim s a) -> IOSim s a
simRun name action = do
  conn <- simConnectionWith simTracer
  withWorkflow conn simIdentity (WorkflowId name) Nothing action

tests :: TestTree
tests =
  -- Sequential: cases announce through one shared stderr, so parallel
  -- 'printSimTrace' calls would interleave their lines mid-character.
  -- 'AllFinish' keeps every case running on a failure, in order.
  dependentTestGroup
    "Step runners (Sim)"
    AllFinish
    [       testCase "a step run announces through the context tracer" $ do
        (outcome, tr) <- runSimCase tracedRun
        printSimTrace tr
        outcome @?= Right 1
        traceEvents tr @?= [StepRunning "traced" 0, StepOutputRecorded "traced" 0],
      testCase "a step inside a step runs plainly" $ do
        (outcome, tr) <- runSimCase plainRun
        printSimTrace tr
        outcome @?= Right 3
        traceEvents tr @?= [StepPlain "inner"],
      testCase "a scoped step runs through the workflow view" $ do
        (outcome, tr) <- runSimCase scopedRun
        printSimTrace tr
        outcome @?= (Right 42, Just (firstStepStatus 0))
        traceEvents tr @?= [StepRunning "scoped" 0, StepOutputRecorded "scoped" 0],
      testCase "a nested step through the step view is plain" $ do
        (outcome, tr) <- runSimCase scopedNested
        printSimTrace tr
        outcome @?= Right 8
        traceEvents tr @?= [StepRunning "outer" 0, StepPlain "inner", StepOutputRecorded "outer" 0],
      testCase "a pending scoped step claims its id at build" $ do
        (outcome, tr) <- runSimCase scopedPending
        printSimTrace tr
        outcome @?= (Right 42, Just 0)
        -- The drive path announces the recorded output; the run-path
        -- StepRunning announce belongs to runStep, not to drives.
        traceEvents tr @?= [StepOutputRecorded "pending" 0]
    ]

traceEvents :: SimTrace a -> [WorkflowEvent]
traceEvents = selectTraceEventsDynamic

-- * Scoped-runner cases

-- | The workflow-scope runner over the sim backend: the body reads its
-- narrowed view's status, so the case proves the handoff as well as the
-- checkpoint.
scopedRun :: IOSim s (Either (Error EngineOnly) Int, Maybe StepStatus)
scopedRun = do
  conn <- simConnectionWith simTracer
  observed <- newTVarIO Nothing
  result <-
    withWorkflow conn simIdentity (WorkflowId "sim-step-scoped") Nothing $ \wctx ->
      runStep wctx "scoped" $ \s -> do
        atomically (writeTVar observed (stepCtxStatus s))
        pure 42
  seen <- readTVarIO observed
  pure (result, seen)

-- | The scoped pending pair: the id is claimed when the pending is built
-- and the drive records under it.
scopedPending :: IOSim s (Either (Error EngineOnly) Int, Maybe Int)
scopedPending = do
  conn <- simConnectionWith simTracer
  withWorkflow conn simIdentity (WorkflowId "sim-step-pending-scoped") Nothing $ \wctx -> do
    pending <-
      pendingStep wctx "pending" $ \_ ->
        pure (Right (42 :: Int))
    outcome <- pending.pendingRun
    pure (outcome, pendingStepId pending)

-- | The step-scope runner: the nested call is plain by construction.
scopedNested :: forall s. IOSim s (Either (Error EngineOnly) Int)
scopedNested = do
  conn <- simConnectionWith simTracer
  withWorkflow conn simIdentity (WorkflowId "sim-step-nested-scoped") Nothing $ \wctx ->
    runStep wctx "outer" $ \s -> do
      inner <- runNestedStep s "inner" (\_ -> pure (7 :: Int)) :: IOSim s (Either (Error EngineOnly) Int)
      case inner of
        Right n -> pure (n + 1)
        Left err -> error (show err)

-- * The mirrored cases

tracedRun :: IOSim s (Either (Error EngineOnly) Int)
tracedRun = do
  simRun "sim-step" $ \wctx -> runStep wctx "traced" (const (pure (1 :: Int)))

plainRun :: IOSim s (Either (Error EngineOnly) Int)
plainRun = do
  simRun "sim-step-plain" $ \wctx -> do
    marker <- nextWorkflowMarker wctx
    withStep wctx marker (firstStepStatus 0) $ \_stepped ->
      runStepWith stepOptionsDefault wctx "inner" (const (pure (Right (3 :: Int))))
