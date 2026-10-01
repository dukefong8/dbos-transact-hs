{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | The shared 'ContextTest' scenarios over 'MockSystemDB': the same
-- cases as the live tree, with values asserted here — including the
-- 'IOSim' typed trace assertions, which stay in this module. Each case
-- prints its sim's 'Say' trace inline, so a plain @-- $> tasty@ run shows
-- traces with no extra plumbing.
module DBOS.Transact.ContextTestSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, selectTraceEventsDynamic)
import Data.Text (Text)
import DBOS.IOSimTracer (printSimTrace, runSimCase, simTracer, simTracerSay)
import DBOS.SystemDB.IOSim (simConnectionWith)
import DBOS.Transact
  ( Connection,
    Identity (..),
    SysdbEvent (..),
    WorkflowEvent (..),
    contextTracer,
    newCtx,
    newWorkflowState,
    nextExecutionIdentity,
    runTracer,
    withTracer,
  )
import DBOS.Transact.ContextTest
  ( Fixture (..),
    checkScopeStatus,
    checkThrowEscape,
    scenarioAttemptScope,
    scenarioAttemptTokens,
    scenarioConcurrentIsolation,
    scenarioCoopFlag,
    scenarioDeadline,
    scenarioDenseIds,
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
    scenarioThrowEscape,
    scenarioTokenFire,
    scenarioTokenOutsideStep,
    scenarioTravelsWith,
    scenarioWorkflowId,
  )
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
        newCtx conn simIdentity state,
      fixtureMkConn = simConnection,
      fixtureIdentity = simIdentity,
      fixtureAppName = "sim-app"
    }

tests :: TestTree
tests =
  -- Sequential: cases announce through one shared stderr, so parallel
  -- 'printSimTrace' calls would interleave their lines mid-character.
  -- 'AllFinish' keeps every case running on a failure, in order.
  dependentTestGroup
    "Context (IOSim)"
    AllFinish
    [ testCase "a context reads its workflow id" $ do
        (res, tr) <- runSimCase (scenarioWorkflowId simFixture)
        printSimTrace tr
        res @?= "wf-1",
      testCase "a workflow's step ids are zero based and allocated once" $ do
        (res, tr) <- runSimCase (scenarioStepIds simFixture)
        printSimTrace tr
        res @?= (0, 1, 2),
      testCase "step ids stay dense while markers spend their own sequence" $ do
        (res, tr) <- runSimCase (scenarioDenseIds simFixture)
        printSimTrace tr
        res @?= (0, 1, 2),
      testCase "withAttempt scopes a step and leaves the outer scope alone" $ do
        (res, tr) <- runSimCase (scenarioAttemptScope simFixture)
        printSimTrace tr
        res @?= (Nothing, Just 4, Nothing),
      testCase "a scope reports its status and id" $ do
        (res, tr) <- runSimCase (scenarioScopeStatus simFixture)
        printSimTrace tr
        either fail pure (checkScopeStatus res),
      testCase "a first attempt reports its step, attempt 1 of 1" $ do
        (res, tr) <- runSimCase (scenarioFirstAttempt simFixture)
        printSimTrace tr
        res @?= (3, 1, 1),
      testCase "a retry keeps the step and moves the attempt" $ do
        (res, tr) <- runSimCase (scenarioRetryAttempt simFixture)
        printSimTrace tr
        res @?= (3, 2, 1),
      testCase "a fresh token is quiet until fired" $ do
        (res, tr) <- runSimCase (scenarioTokenFire simFixture)
        printSimTrace tr
        res @?= (False, True),
      testCase "each attempt watches a token of its own" $ do
        (res, tr) <- runSimCase (scenarioAttemptTokens simFixture)
        printSimTrace tr
        res @?= (True, False),
      testCase "a deadline rides the workflow state" $ do
        (res, tr) <- runSimCase (scenarioDeadline simFixture)
        printSimTrace tr
        res @?= Nothing,
      testCase "two contexts over one workflow share its step counter" $ do
        (res, tr) <- runSimCase (scenarioSharedCounter simFixture)
        printSimTrace tr
        res @?= (0, 1),
      testCase "a re-run of one id is a different execution" $ do
        (res, tr) <- runSimCase (scenarioRerunIdentity simFixture)
        printSimTrace tr
        res @?= (True, False),
      testCase "the connection and identity travel with the context" $ do
        (res, tr) <- runSimCase (scenarioTravelsWith simFixture)
        printSimTrace tr
        res @?= (simFixture.fixtureIdentity, Just ("sim-app" :: Text)),
      testCase "nested runners isolate" $ do
        (res, tr) <- runSimCase (scenarioNestedRunners simFixture)
        printSimTrace tr
        res @?= ("wf-1", "wf-1", False),
      testCase "state interop runs beside the context" $ do
        (res, tr) <- runSimCase (scenarioStateInterop simFixture)
        printSimTrace tr
        res @?= "done",
      testCase "a throw from an engine call reaches the caller" $ do
        (res, tr) <- runSimCase (scenarioThrowEscape simFixture)
        printSimTrace tr
        either fail pure (checkThrowEscape res),
      testCase "a cooperative flag cancels a wait promptly" $ do
        (_, tr) <- runSimCase (scenarioCoopFlag simFixture)
        printSimTrace tr,
      testCase "a fork handed the context shares its counter" $ do
        (res, tr) <- runSimCase (scenarioForkCounter simFixture)
        printSimTrace tr
        res @?= (0, 1),
      testCase "a nested scope reports the step that encloses it" $ do
        (res, tr) <- runSimCase (scenarioNestedScope simFixture)
        printSimTrace tr
        res @?= (Nothing, Just 0, True),
      testCase "a cancellation token outside a step never fires" $ do
        (res, tr) <- runSimCase (scenarioTokenOutsideStep simFixture)
        printSimTrace tr
        res @?= False,
      testCase "concurrent contexts are isolated from each other" $ do
        (res, tr) <- runSimCase (scenarioConcurrentIsolation simFixture)
        printSimTrace tr
        res @?= ("a", "b"),
      testCase "a context announces through its tracer" $ do
        (_, tr) <- runSimCase demoTrace
        printSimTrace tr
        selectTraceEventsDynamic tr @?= [StepRunning "demo" 0]
        selectTraceEventsDynamic tr @?= [SysdbRetryAttempt "demo-op" 1 0 "demo"]
    ]
  where
    demoTrace :: forall s. IOSim s ()
    demoTrace = do
      ctx <- withTracer simTracerSay <$> simFixture.fixtureMkCtx "wf-1"
      runTracer (contextTracer ctx) (StepRunning "demo" 0)
      runTracer (contextTracer ctx) (SysdbRetryAttempt "demo-op" 1 0 "demo")
