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
import DBOS.IOSimTracer (printSimTrace, runSimCase, simTracer)
import DBOS.SystemDB (StepRecord (..))
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.IOSim (memConnectionOn, newMemDB, simConnectionWith)
import DBOS.Transact.StepCases
  ( StepFixture (..),
    checkDurableSleep,
    checkNestedEnclosing,
    checkNestedPlain,
    checkNestedStepView,
    checkPendingScoped,
    checkRecordReplay,
    checkScopedView,
    checkTokenQuiet,
    scenarioDurableSleep,
    scenarioNestedEnclosing,
    scenarioNestedPlain,
    scenarioNestedStepView,
    scenarioPendingScoped,
    scenarioRecordReplay,
    scenarioScopedView,
    scenarioTokenQuiet,
  )
import DBOS.Transact
  ( EngineOnly,
    Error (..),
    WorkflowCtx,
    WorkflowId (..),
    runStep,
  )
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Step (WorkflowEvent (..))
import DBOS.Transact.Context (withWorkflow)
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

-- | One fixture per leaf over a fresh in-memory database: deterministic
-- names, no row creation (Mem records steps without workflow rows),
-- checkpoint reads through the memory backend, and the immediate
-- quiet-token sense (deterministic where there is no race to catch).
mkStepFixture :: forall s. IOSim s (StepFixture (IOSim s))
mkStepFixture = do
  mem <- newMemDB
  pure StepFixture
    { sfConnection = memConnectionOn mem simTracer,
      sfFreshId = \base -> pure (WorkflowId ("sim-step-" <> base)),
      sfIdentity = simIdentity,
      sfInitRow = \_ -> pure (),
      sfCheckStep = \wid step name -> do
        placed <- SystemDB.checkStep mem wid step name
        case placed of
          Right (Just _) -> pure True
          _ -> pure False,
      sfListStepNames = \wid -> do
        listed <- SystemDB.listSteps mem wid False Nothing Nothing Nothing
        case listed of
          Right rows -> pure (map (.stepRecordStepName) rows)
          Left err -> error (show err)
    }

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
      testCase "a recorded workflow step runs once and replays" $ do
        (outcome, tr) <- runSimCase (mkStepFixture >>= scenarioRecordReplay)
        printSimTrace tr
        either fail pure (checkRecordReplay outcome)
        traceEvents tr @?= [StepRunning "test_step" 0, StepOutputRecorded "test_step" 0, StepReplaying "test_step" 0],
      testCase "a step inside a step body runs plainly and takes no id" $ do
        (outcome, tr) <- runSimCase (mkStepFixture >>= scenarioNestedPlain)
        printSimTrace tr
        either fail pure (checkNestedPlain outcome)
        traceEvents tr @?= [StepRunning "outer" 0, StepPlain "inner", StepOutputRecorded "outer" 0],
      testCase "a scoped step runs once and replays through the workflow view" $ do
        (outcome, tr) <- runSimCase (mkStepFixture >>= scenarioScopedView)
        printSimTrace tr
        either fail pure (checkScopedView outcome)
        traceEvents tr @?= [StepRunning "scoped_step" 0, StepOutputRecorded "scoped_step" 0, StepReplaying "scoped_step" 0],
      testCase "a nested step through the step view is plain and takes no id" $ do
        (outcome, tr) <- runSimCase (mkStepFixture >>= scenarioNestedStepView)
        printSimTrace tr
        either fail pure (checkNestedStepView outcome)
        traceEvents tr @?= [StepRunning "outer" 0, StepPlain "inner", StepOutputRecorded "outer" 0],
      testCase "a pending scoped step claims its id at build and replays" $ do
        (outcome, tr) <- runSimCase (mkStepFixture >>= scenarioPendingScoped)
        printSimTrace tr
        either fail pure (checkPendingScoped outcome)
        traceEvents tr @?= [StepOutputRecorded "pending_step" 0, StepReplaying "pending_step" 0],
      testCase "durable sleep reuses its recorded wake time" $ do
        (outcome, tr) <- runSimCase (mkStepFixture >>= scenarioDurableSleep)
        printSimTrace tr
        either fail pure (checkDurableSleep outcome)
        traceEvents tr @?= [],
      testCase "a nested step reports the step that encloses it" $ do
        (outcome, tr) <- runSimCase (mkStepFixture >>= scenarioNestedEnclosing)
        printSimTrace tr
        either fail pure (checkNestedEnclosing outcome)
        traceEvents tr @?= [StepRunning "first" 0, StepOutputRecorded "first" 0, StepPlain "inner", StepRetrying "outer" 1 1 2 1 "the step outer failed: boom", StepPlain "inner", StepOutputRecorded "outer" 1],
      testCase "a cancellation token outside a step never fires" $ do
        (outcome, tr) <- runSimCase (mkStepFixture >>= scenarioTokenQuiet)
        printSimTrace tr
        either fail pure (checkTokenQuiet outcome)
        traceEvents tr @?= [StepAttemptTimedOut "times-out" 0 20, StepErrorRecorded "times-out" 0]
    ]

traceEvents :: SimTrace a -> [WorkflowEvent]
traceEvents = selectTraceEventsDynamic

-- | The workflow-scope runner over the sim backend, kept for the
-- tracer-demo leaf: it announces through the context tracer.
tracedRun :: IOSim s (Either (Error EngineOnly) Int)
tracedRun = do
  conn <- simConnectionWith simTracer
  withWorkflow conn simIdentity (WorkflowId "sim-step") Nothing $ \wctx ->
    runStep wctx "traced" (const (pure (1 :: Int)))
