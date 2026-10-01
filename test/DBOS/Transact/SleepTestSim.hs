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
  ( Ctx,
    Identity (..),
    firstStepStatus,
    newCtx,
    newWorkflowState,
    nextExecutionIdentity,
    nextStepId,
    nextStepMarker,
    sleepPlain,
    sleepWorkflowStep,
    withAttempt,
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
    "Durable sleep (Sim)"
    AllFinish
    [ testCase "a sleep waits and is checkpointed" $ do
        (outcome, tr) <- runSimCase $ do
          context <- simCtx "sim-sleep"
          sleepWorkflowStep context (millisDuration 25)
        printSimTrace tr
        outcome @?= Right (),
      testCase "a replayed sleep does not start its clock again" $ do
        (outcome, tr) <- runSimCase $ do
          context <- simCtx "sim-sleep-replay"
          _ <- sleepWorkflowStep context (millisDuration 25)
          -- A much longer request still returns at the recorded wake time.
          sleepWorkflowStep context (millisDuration 60000)
        printSimTrace tr
        outcome @?= Right (),
      testCase "a sleep outside a workflow waits plainly" $ do
        (outcome, tr) <- runSimCase (sleepPlain (millisDuration 1))
        printSimTrace tr
        outcome @?= (),
      testCase "a sleep inside a step takes no id" $ do
        (outcome, tr) <- runSimCase $ do
          context <- simCtx "sim-sleep-in-step"
          marker <- nextStepMarker context
          withAttempt context marker (firstStepStatus 0) $ \inner -> do
            before <- nextStepId inner
            slept <- sleepWorkflowStep inner (millisDuration 5)
            after <- nextStepId inner
            pure (slept, before, after)
        printSimTrace tr
        case outcome of
          (slept, before, after) -> do
            slept @?= Right ()
            after @?= before + 1
    ]
