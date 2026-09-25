{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.SleepTest' mirrored under IOSim over the mock backend:
-- the real 'sleepWorkflowStep'/'sleepPlain' with virtual time. The mock's
-- @recordSleep@ answers a fixed wake time, so the replay case adopts it.
module DBOS.Transact.SleepTestIOSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, runSimOrThrow)
import DBOS.SystemDB (millisDuration)
import DBOS.SystemDB.IOSim (simConnection)
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
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Durable sleep (IOSim)"
    [ testCase "a sleep waits and is checkpointed" $ do
        run (do context <- simCtx; sleepWorkflowStep context (millisDuration 25)) @?= Right (),
      testCase "a replayed sleep does not start its clock again" $ do
        run
          ( do
              context <- simCtx
              _ <- sleepWorkflowStep context (millisDuration 25)
              -- A much longer request still returns at the recorded wake time.
              sleepWorkflowStep context (millisDuration 60000)
          )
          @?= Right (),
      testCase "a sleep outside a workflow waits plainly" $ do
        run (sleepPlain (millisDuration 1)) @?= (),
      testCase "a sleep inside a step takes no id" $ do
        let (outcome, before, after) = run
              ( do
                  context <- simCtx
                  marker <- nextStepMarker context
                  withAttempt context marker (firstStepStatus 0) $ \inner -> do
                    before <- nextStepId inner
                    outcome <- sleepWorkflowStep inner (millisDuration 5)
                    after <- nextStepId inner
                    pure (outcome, before, after)
              )
        outcome @?= Right ()
        after @?= before + 1
    ]

run :: (forall s. IOSim s a) -> a
run = runSimOrThrow

simCtx :: IOSim s (Ctx (IOSim s))
simCtx = do
  conn <- simConnection
  identity <- nextExecutionIdentity conn
  state <- newWorkflowState "sim-sleep" Nothing identity
  newCtx conn simIdentity state

simIdentity :: Identity
simIdentity =
  Identity
    { identityAppName = "sim-app",
      identityAppVersion = "0.0.0",
      identityExecutorId = "sim-executor",
      identityAppId = ""
    }
