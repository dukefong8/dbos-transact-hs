{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.SleepTest' mirrored under IOSim over the in-memory
-- backend: the real 'sleepStep'/'sleepPlain' with virtual time. Scenarios
-- and checks are shared; this module owns the sim factory and the
-- sim-only extras — the scheduler's own record (the fresh sleeps wait on
-- the sim clock) and the typed 'SleepEvent's (the replay waits nothing
-- after the recorded wake, the in-step sleep announces itself
-- uncheckpointed, the free sleep takes no tracer).
module DBOS.Transact.SleepTestSim (tests) where

import Control.Monad.IOSim (IOSim, SimEventType (..), SimTrace, selectTraceEventsDynamic, traceEvents)
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (simTracer)
import DBOS.Prelude
import Data.Text qualified as Text
import DBOS.SystemDB (Duration, NewWorkflow (..), Submission (..), WorkflowId (..), newWorkflow, sleepStepName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.IOSim (memConnectionOn, newMemDB, simIdentity)
import DBOS.Transact (SleepEvent (..), firstStepStatus, nextStepId, nextWorkflowMarker, sleepPlain, sleepStep, withStep, withWorkflow)
import DBOS.Transact.SleepCases
  ( SleepFixture (..),
    checkSleepCheckpoint,
    checkSleepInStep,
    checkSleepPlain,
    checkSleepReplay,
    scenarioSleepCheckpoint,
    scenarioSleepInStep,
    scenarioSleepPlain,
    scenarioSleepReplay,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool)

-- | One fixture per leaf over a fresh in-memory database: workflow ids are
-- minted per scenario and both runs of the replay case share the same store
-- in the same simulation, so the second run truly replays the first.
simSleepFixture :: forall s. IOSim s (SleepFixture (IOSim s))
simSleepFixture = do
  mem <- newMemDB
  fresh <- newTVarIO (0 :: Int)
  pure
    SleepFixture
      { sfFreshWorkflowId = do
          n <- atomically (readTVar fresh >>= \k -> writeTVar fresh (k + 1) >> pure k)
          let widText = "sim-sleep-" <> Text.pack (show n)
          created <- SystemDB.initWorkflow mem ((newWorkflow widText) {newWorkflowName = Just "SimSleepTest"}) Nothing Fresh Nothing
          case created of
            Left err -> error (show err)
            Right _ -> pure (WorkflowId widText),
        sfRunSleep = \wid duration -> do
          conn <- memConnectionOn mem simTracer
          withWorkflow conn simIdentity wid Nothing (\wctx -> sleepStep wctx duration),
        sfCheckSleep = \wid ->
          SystemDB.checkStep mem wid 0 sleepStepName >>= either (error . show) pure,
        sfPlainSleep = sleepPlain,
        sfRunInStep = \wid duration -> do
          conn <- memConnectionOn mem simTracer
          withWorkflow conn simIdentity wid Nothing $ \wctx -> do
            marker <- nextWorkflowMarker wctx
            withStep wctx marker (firstStepStatus 0) $ \_stepped -> do
              before <- nextStepId wctx
              slept <- sleepStep wctx duration
              after <- nextStepId wctx
              pure (slept, before, after)
      }

tests :: TestTree
tests =
  testGroup
    "Durable sleep (Sim)"
    [ simCase simSleepFixture "a sleep waits and is checkpointed" scenarioSleepCheckpoint checkSleepCheckpoint traceSleepCheckpoint,
      simCase simSleepFixture "a replayed sleep does not start its clock again" scenarioSleepReplay checkSleepReplay traceSleepReplay,
      simCase simSleepFixture "a sleep outside a workflow waits plainly" scenarioSleepPlain checkSleepPlain traceSleepPlain,
      simCase simSleepFixture "a sleep inside a step takes no id" scenarioSleepInStep checkSleepInStep traceSleepInStep
    ]

-- * Scheduler-event and typed-event assertions (sim-only)

eventTypes :: SimTrace a -> [SimEventType]
eventTypes = map (\(_, _, _, eventType) -> eventType) . traceEvents

isDelay :: SimEventType -> Bool
isDelay EventThreadDelay {} = True
isDelay _ = False

-- | The fresh sleep waits on the sim clock and schedules the full wait.
traceSleepCheckpoint :: SimTrace a -> IO ()
traceSleepCheckpoint tr = do
  assertBool "the fresh sleep must wait on the sim clock" (any isDelay (eventTypes tr))
  let remaining = [wake | SleepUntilWake _ wake <- selectTraceEventsDynamic tr]
  assertBool ("the fresh sleep must schedule its full wait, got: " <> show remaining) (remaining == [25])

-- | The replay adopts the recorded wake: the waits read 25ms then nothing.
traceSleepReplay :: SimTrace a -> IO ()
traceSleepReplay tr = do
  assertBool "the fresh sleep must wait on the sim clock" (any isDelay (eventTypes tr))
  let remaining = [wake | SleepUntilWake _ wake <- selectTraceEventsDynamic tr]
  assertBool ("the replay must wait nothing after the recorded wake, got: " <> show remaining) (remaining == [25, 0])

-- | The free sleep waits on the sim clock and takes no tracer.
traceSleepPlain :: SimTrace a -> IO ()
traceSleepPlain tr = do
  assertBool "the plain sleep must wait on the sim clock" (any isDelay (eventTypes tr))
  assertBool "the plain sleep takes no tracer" (null (selectTraceEventsDynamic tr :: [SleepEvent]))

-- | The in-step sleep waits on the sim clock and announces itself
-- uncheckpointed.
traceSleepInStep :: SimTrace a -> IO ()
traceSleepInStep tr = do
  assertBool "the in-step sleep must wait on the sim clock" (any isDelay (eventTypes tr))
  assertBool "the in-step sleep must announce itself uncheckpointed" (any isUncheckpointed (selectTraceEventsDynamic tr))
  where
    isUncheckpointed SleepUncheckpointed {} = True
    isUncheckpointed _ = False
