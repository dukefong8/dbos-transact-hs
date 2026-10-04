{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.SleepTest' mirrored under IOSim over the mock backend:
-- the real 'sleepWorkflowStep'/'sleepPlain' with virtual time, each case
-- printing its sim's 'Say' trace inline so a plain @-- $> tasty@ run
-- shows announcements with no extra plumbing. The mock's @recordSleep@
-- answers a fixed wake time, so the replay case adopts it; every
-- checkpointed sleep announces the wake it waits until, and the in-step
-- sleep announces that it is not checkpointed — the same lines the live
-- tree writes through FastLogger. The free 'sleepPlain' takes no tracer
-- and stays quiet.
module DBOS.Transact.SleepTestSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim)
import Data.Text (Text)
import DBOS.IOSimTracer (printSimTrace, runSimCase, simTracer)
import DBOS.SystemDB (millisDuration)
import DBOS.SystemDB.IOSim (simConnectionWith)
import DBOS.Transact
  ( Identity (..),
    WorkflowCtx,
    WorkflowId (..),
    firstStepStatus,
    nextWorkflowMarker,
    nextWorkflowStepId,
    sleepPlain,
    sleepWorkflowStep,
    withStep,
    withWorkflow,
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
    "Durable sleep (Sim)"
    AllFinish
    [ testCase "a sleep waits and is checkpointed" $ do
        (outcome, tr) <- runSimCase $ simRun "sim-sleep" $ \wctx ->
          sleepWorkflowStep wctx (millisDuration 25)
        printSimTrace tr
        outcome @?= Right (),
      testCase "a replayed sleep does not start its clock again" $ do
        (outcome, tr) <- runSimCase $ simRun "sim-sleep-replay" $ \wctx -> do
          _ <- sleepWorkflowStep wctx (millisDuration 25)
          -- A much longer request still returns at the recorded wake time.
          sleepWorkflowStep wctx (millisDuration 60000)
        printSimTrace tr
        outcome @?= Right (),
      testCase "a sleep outside a workflow waits plainly" $ do
        (outcome, tr) <- runSimCase (sleepPlain (millisDuration 1))
        printSimTrace tr
        outcome @?= (),
      testCase "a sleep inside a step takes no id" $ do
        (outcome, tr) <- runSimCase $ simRun "sim-sleep-in-step" $ \wctx -> do
          marker <- nextWorkflowMarker wctx
          withStep wctx marker (firstStepStatus 0) $ \_stepped -> do
            before <- nextWorkflowStepId wctx
            slept <- sleepWorkflowStep wctx (millisDuration 5)
            after <- nextWorkflowStepId wctx
            pure (slept, before, after)
        printSimTrace tr
        case outcome of
          (slept, before, after) -> do
            slept @?= Right ()
            after @?= before + 1
    ]
