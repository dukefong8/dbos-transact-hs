{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | The shared 'ContextTest' scenarios over 'MockSystemDB': the same cases
-- as the live tree, judged by the same checks, with the sim-only typed
-- trace assertions staying in this module. Nothing prints: 'simCase' runs
-- each leaf through 'runSimCase' and judges the trace by type.
module DBOS.Transact.ContextTestSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (simTracer)
import DBOS.SystemDB.IOSim (simConnectionWith)
import DBOS.Transact
  ( Identity (..),
    SysdbEvent (..),
    WorkflowEvent (..),
    runTracer,
  )
import DBOS.Transact.Context
  ( WorkflowCtx (wctxTracer),
    newWorkflowCtx,
    newWorkflowState,
    withTracer
  )
import DBOS.Transact.Connection (Connection)
import DBOS.Transact.Connection
  ( nextExecutionIdentity
  )
import DBOS.Transact.ContextTest
  ( Fixture (..),
    checkAttemptScope,
    checkAttemptTokens,
    checkConcurrentIsolation,
    checkCoopFlag,
    checkDeadline,
    checkDenseIds,
    checkExecCounters,
    checkFirstAttempt,
    checkForkCounter,
    checkNestedRunners,
    checkNestedScope,
    checkRaceCancelled,
    checkRaceCompletes,
    checkRerunIdentity,
    checkRetryAttempt,
    checkScopeStatus,
    checkSharedCounter,
    checkStateInterop,
    checkStepIds,
    checkStepView,
    checkThrowEscape,
    checkTokenFire,
    checkTokenOutsideStep,
    checkTravelsWith,
    checkWorkflowId,
    scenarioAttemptScope,
    scenarioAttemptTokens,
    scenarioConcurrentIsolation,
    scenarioCoopFlag,
    scenarioDeadline,
    scenarioDenseIds,
    scenarioExecCounters,
    scenarioRaceCancelled,
    scenarioRaceCompletes,
    scenarioFirstAttempt,
    scenarioForkCounter,
    scenarioNestedRunners,
    scenarioNestedScope,
    scenarioRerunIdentity,
    scenarioRetryAttempt,
    scenarioScopeStatus,
    scenarioSharedCounter,
    scenarioStateInterop,
    scenarioStepIds,
    scenarioStepView,
    scenarioThrowEscape,
    scenarioTokenFire,
    scenarioTokenOutsideStep,
    scenarioTravelsWith,
    scenarioWorkflowId,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, (@?=))

simIdentity :: Identity
simIdentity =
  Identity
    { identityAppName = "sim-app",
      identityAppVersion = "0.0.0",
      identityExecutorId = "sim-executor",
      identityAppId = ""
    }

-- | The sim backend wrapped in a connection carrying the io-sim tracer.
-- A local copy is deliberate: sibling sim trees repeat the builders they
-- need.
simConnection :: IOSim s (Connection (IOSim s))
simConnection = simConnectionWith simTracer

-- | Every case runs against a fresh mock connection; each use
-- instantiates the fixture at its own simulation.
simFixture :: forall s. Fixture (IOSim s)
simFixture =
  Fixture
    { fixtureMkCtx = \name -> do
        conn <- simConnection
        identity <- nextExecutionIdentity conn
        state <- newWorkflowState name Nothing identity
        newWorkflowCtx conn simIdentity state,
      fixtureMkConn = simConnection,
      fixtureIdentity = simIdentity,
      fixtureAppName = "sim-app"
    }

tests :: TestTree
tests =
  testGroup
    "Context (IOSim)"
    [ simCase (pure simFixture) "a context reads its workflow id" scenarioWorkflowId checkWorkflowId traceContextSilent,
      simCase (pure simFixture) "a workflow's step ids are zero based and allocated once" scenarioStepIds checkStepIds traceContextSilent,
      simCase (pure simFixture) "step ids stay dense while markers spend their own sequence" scenarioDenseIds checkDenseIds traceContextSilent,
      simCase (pure simFixture) "withAttempt scopes a step and leaves the outer scope alone" scenarioAttemptScope checkAttemptScope traceContextSilent,
      simCase (pure simFixture) "a scope reports its status and id" scenarioScopeStatus checkScopeStatus traceContextSilent,
      simCase (pure simFixture) "a first attempt reports its step, attempt 1 of 1" scenarioFirstAttempt checkFirstAttempt traceContextSilent,
      simCase (pure simFixture) "a retry keeps the step and moves the attempt" scenarioRetryAttempt checkRetryAttempt traceContextSilent,
      simCase (pure simFixture) "a fresh token is quiet until fired" scenarioTokenFire checkTokenFire traceContextSilent,
      simCase (pure simFixture) "each attempt watches a token of its own" scenarioAttemptTokens checkAttemptTokens traceContextSilent,
      simCase (pure simFixture) "a deadline rides the workflow state" scenarioDeadline checkDeadline traceContextSilent,
      simCase (pure simFixture) "two contexts over one workflow share its step counter" scenarioSharedCounter checkSharedCounter traceContextSilent,
      simCase (pure simFixture) "a re-run of one id is a different execution" scenarioRerunIdentity checkRerunIdentity traceContextSilent,
      simCase (pure simFixture) "the connection and identity travel with the context" scenarioTravelsWith checkTravelsWith traceContextSilent,
      simCase (pure simFixture) "nested runners isolate" scenarioNestedRunners checkNestedRunners traceContextSilent,
      simCase (pure simFixture) "state interop runs beside the context" scenarioStateInterop checkStateInterop traceContextSilent,
      simCase (pure simFixture) "a throw from an engine call reaches the caller" scenarioThrowEscape checkThrowEscape traceContextSilent,
      simCase (pure simFixture) "a cooperative flag cancels a wait promptly" scenarioCoopFlag checkCoopFlag traceContextSilent,
      simCase (pure simFixture) "a fork handed the context shares its counter" scenarioForkCounter checkForkCounter traceContextSilent,
      simCase (pure simFixture) "a nested scope reports the step that encloses it" scenarioNestedScope checkNestedScope traceContextSilent,
      simCase (pure simFixture) "a cancellation token outside a step never fires" scenarioTokenOutsideStep checkTokenOutsideStep traceContextSilent,
      simCase (pure simFixture) "concurrent contexts are isolated from each other" scenarioConcurrentIsolation checkConcurrentIsolation traceContextSilent,
      simCase (pure simFixture) "separate executions own independent step counters" scenarioExecCounters checkExecCounters traceContextSilent,
      simCase (pure simFixture) "a step view reads its status with the workflow id" scenarioStepView checkStepView traceContextSilent,
      simCase (pure simFixture) "raceCancel returns the value when the token stays quiet" scenarioRaceCompletes checkRaceCompletes traceContextSilent,
      simCase (pure simFixture) "raceCancel reports cancellation when the token has fired" scenarioRaceCancelled checkRaceCancelled traceContextSilent,
      simCase (pure simFixture) "a context announces through its tracer" (const demoTrace) checkCoopFlag traceAnnounce
    ]

-- | Context scenarios touch no engine paths, so the trace carries no
-- 'WorkflowEvent' at all.
traceContextSilent :: SimTrace a -> IO ()
traceContextSilent tr =
  assertBool "context must emit no workflow events" (null (selectTraceEventsDynamic tr :: [WorkflowEvent]))

-- | The sim-only structural announcement: the two hand-emitted events.
traceAnnounce :: SimTrace a -> IO ()
traceAnnounce tr = do
  selectTraceEventsDynamic tr @?= [StepRunning "demo" 0]
  selectTraceEventsDynamic tr @?= [SysdbRetryAttempt "demo-op" 1 0 "demo"]

demoTrace :: forall s. IOSim s ()
demoTrace = do
  ctx <- withTracer simTracer <$> simFixture.fixtureMkCtx "wf-1"
  runTracer ctx.wctxTracer (StepRunning "demo" 0)
  runTracer ctx.wctxTracer (SysdbRetryAttempt "demo-op" 1 0 "demo")
