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
import DBOS.Internal.Prelude
import Data.Int (Int64)
import Data.Text qualified as Text
import DBOS.SystemDB (NewWorkflow (..), WorkflowId (..), WorkflowIdReusePolicy (..), WorkflowOwner (..), WorkflowRecord (workflowRecordCreatorXid), newWorkflow, sleepStepName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.IOSim (MemSystemDB, memConnectionOn, newMemDB, newMemDBSkewed, simIdentity)
import DBOS.Transact
  (
  sleepPlain,
  sleepStep,
  )
import DBOS.Internal.Transact.Sleep (SleepEvent (..))
import DBOS.Internal.Transact.Context (firstStepStatus, nextStepId, nextWorkflowMarker, peekStepId, withStep, withWorkflow)
import DBOS.Transact.SleepCases
  ( SleepFixture (..),
    checkSleepCheckpoint,
    checkSleepInStep,
    checkSleepPlain,
    checkSleepTimed,
    checkSleepRefused,
    checkSleepRefusedRecorded,
    checkSleepReplay,
    scenarioSleepCheckpoint,
    scenarioSleepInStep,
    scenarioSleepPlain,
    scenarioSleepTimed,
    scenarioSleepRefused,
    scenarioSleepRefusedRecorded,
    scenarioSleepReplay,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool)

-- | One fixture per leaf over a fresh in-memory database: workflow ids are
-- minted per scenario and both runs of the replay case share the same store
-- in the same simulation, so the second run truly replays the first.
simSleepFixture :: forall s. IOSim s (SleepFixture (IOSim s))
simSleepFixture = newMemDB >>= simSleepFixtureOn

simSleepFixtureSkewed :: forall s. Int64 -> IOSim s (SleepFixture (IOSim s))
simSleepFixtureSkewed skewMs = newMemDBSkewed skewMs >>= simSleepFixtureOn

simSleepFixtureOn :: forall s. MemSystemDB s -> IOSim s (SleepFixture (IOSim s))
simSleepFixtureOn mem = do
  fresh <- newTVarIO (0 :: Int)
  let -- Sleeps present the row's own creator token, as an execution built
      -- from the init result would.
      ownerFor widText = do
        fetched <- SystemDB.getWorkflow mem (WorkflowId widText)
        case fetched of
          Right (Just row) -> case row.workflowRecordCreatorXid of
            Just token -> pure (WorkflowOwner {ownerWorkflowId = WorkflowId widText, ownerToken = token})
            Nothing -> error "sleep fixture row has no creator token"
          _ -> error "sleep fixture built a context for a row that was never initialized"
  pure
    SleepFixture
      { sfFreshWorkflowId = do
          n <- atomically (readTVar fresh >>= \k -> writeTVar fresh (k + 1) >> pure k)
          let widText = "sim-sleep-" <> Text.pack (show n)
          created <- SystemDB.initWorkflow mem ((newWorkflow widText) {newWorkflowName = Just "SimSleepTest"}) Nothing WorkflowIdReturnExisting
          case created of
            Left err -> error (show err)
            Right _ -> pure (WorkflowId widText),
        sfRunSleep = \wid@(WorkflowId widText) duration -> do
          conn <- memConnectionOn mem simTracer
          owner <- ownerFor widText
          withWorkflow conn simIdentity wid Nothing owner (\wctx -> sleepStep wctx duration),
        sfCheckSleep = \wid ->
          SystemDB.checkStep mem wid 0 sleepStepName >>= either (error . show) pure,
        sfPlainSleep = sleepPlain,
        sfRefusedSleep = \wid@(WorkflowId widText) duration -> do
          conn <- memConnectionOn mem simTracer
          owner <- ownerFor widText
          withWorkflow conn simIdentity wid Nothing owner $ \wctx -> do
            refused <- sleepStep wctx duration
            next <- peekStepId wctx
            pure (refused, next),
        sfRunInStep = \wid@(WorkflowId widText) duration -> do
          conn <- memConnectionOn mem simTracer
          owner <- ownerFor widText
          withWorkflow conn simIdentity wid Nothing owner $ \wctx -> do
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
      simCase (simSleepFixtureSkewed 3600000) "a sleep waits its duration with the database clock an hour ahead" scenarioSleepTimed checkSleepTimed traceSleepTimed,
      simCase (simSleepFixtureSkewed (-3600000)) "a sleep waits its duration with the database clock an hour behind" scenarioSleepTimed checkSleepTimed traceSleepTimed,
      simCase simSleepFixture "a sleep inside a step takes no id" scenarioSleepInStep checkSleepInStep traceSleepInStep,
      simCase simSleepFixture "a sleep that cannot be recorded is refused before it takes an id" scenarioSleepRefused checkSleepRefused traceSleepRefused,
      simCase simSleepFixture "a refused sleep is recorded as the step error and replayed" scenarioSleepRefusedRecorded checkSleepRefusedRecorded traceSleepRefusedRecorded
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

-- | The skewed sleep schedules its full three seconds on the sim clock:
-- the wait is measured from the database's reading, so the hour of skew
-- never enters it.
traceSleepTimed :: SimTrace a -> IO ()
traceSleepTimed tr = do
  assertBool "the skewed sleep must wait on the sim clock" (any isDelay (eventTypes tr))
  let remaining = [wake | SleepUntilWake _ wake <- selectTraceEventsDynamic tr]
  assertBool ("the skewed sleep must schedule its full wait, got: " <> show remaining) (remaining == [3000])

-- | The in-step sleep waits on the sim clock and announces itself
-- uncheckpointed.
traceSleepInStep :: SimTrace a -> IO ()
traceSleepInStep tr = do
  assertBool "the in-step sleep must wait on the sim clock" (any isDelay (eventTypes tr))
  assertBool "the in-step sleep must announce itself uncheckpointed" (any isUncheckpointed (selectTraceEventsDynamic tr))
  where
    isUncheckpointed SleepUncheckpointed {} = True
    isUncheckpointed _ = False

-- | A refused sleep waits on nothing and announces no wake.
traceSleepRefused :: SimTrace a -> IO ()
traceSleepRefused tr = do
  assertBool "a refused sleep must wait on nothing" (null (filter isDelay (eventTypes tr)))
  assertBool "a refused sleep announces no wake" (null (selectTraceEventsDynamic tr :: [SleepEvent]))

-- | The recorded refusal waits on nothing after it: the primer waits,
-- and neither run announces a wake.
traceSleepRefusedRecorded :: SimTrace a -> IO ()
traceSleepRefusedRecorded tr = do
  assertBool "only the primer may wait on the sim clock" (length (filter isDelay (eventTypes tr)) == 1)
  assertBool "neither run announces a wake" (null (selectTraceEventsDynamic tr :: [SleepEvent]))
