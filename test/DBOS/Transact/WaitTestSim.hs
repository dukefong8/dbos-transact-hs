{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.WaitTest' mirrored under IOSim over the in-memory
-- backend: the checkpointed @DBOS.select_workflow@ step and the
-- uncheckpointed all-wait with real stored rows. Scenarios and checks are
-- shared; this module owns the sim factory and the sim-only extra — the
-- typed 'WaitEvent' record, which fires only when a replay adopts its
-- recorded winner.
module DBOS.Transact.WaitTestSim (tests) where

import DBOS.Transact.Wait (WaitEvent (..))
import DBOS.Transact.Context (withWorkflow)
import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (simTracer)
import DBOS.Prelude
import DBOS.SystemDB (NewWorkflow (..), Outcome (..), Submission (..), WorkflowId (..), newWorkflow, selectStepName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.IOSim (MemSystemDB, memConnectionOn, memLaunchOnWith, memSetApplication, newMemDB, simIdentity, simInstance)
import DBOS.Transact (shutdown, waitForFirstWorkflow, waitForWorkflow, waitForWorkflows)
import DBOS.Transact.WaitCases
  ( WaitExecFixture (..),
    WaitFixture (..),
    checkCancelled,
    checkEmptyAll,
    checkExecAllEmpty,
    checkExecAllSettled,
    checkExecFirstEmpty,
    checkExecFirstWinner,
    checkExecSingle,
    checkJoinLast,
    checkJoinSettled,
    checkRefusal,
    checkRepeated,
    checkReplayWinner,
    checkSelectFirst,
    checkSelectSettledFirst,
    checkWinnerLeft,
    scenarioCancelled,
    scenarioEmptyAll,
    scenarioExecAllEmpty,
    scenarioExecAllSettled,
    scenarioExecFirstEmpty,
    scenarioExecFirstWinner,
    scenarioExecSingle,
    scenarioJoinLast,
    scenarioJoinSettled,
    scenarioRefusal,
    scenarioRepeated,
    scenarioReplayWinner,
    scenarioSelectFirst,
    scenarioSelectSettledFirst,
    scenarioWinnerLeft,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool)

-- | One fixture per leaf over a fresh in-memory database: labeled workflow
-- ids are minted per scenario and every run — including replays over
-- changed sets — shares the same store in the same simulation.
simWaitFixture :: forall s. IOSim s (WaitFixture (IOSim s))
simWaitFixture = do
  mem <- newMemDB
  pure
    WaitFixture
      { wfFreshWorkflowId = \label -> do
          let widText = "sim-wait-" <> label
          created <- SystemDB.initWorkflow mem ((newWorkflow widText) {newWorkflowName = Just "SimWaitTest"}) Nothing Fresh Nothing
          case created of
            Left err -> error (show err)
            Right _ -> pure (WorkflowId widText),
        wfCtx = \wid action -> do
          conn <- memConnectionOn mem simTracer
          withWorkflow conn simIdentity wid Nothing action,
        wfSettle = \wid ->
          SystemDB.recordWorkflowOutcome mem wid (OutcomeOutput (Just "null")) >>= either (error . show) (const (pure ())),
        wfCancel = \wid ->
          SystemDB.cancelWorkflows mem [wid] False Nothing >>= either (error . show) (const (pure ())),
        wfCheckStep = \wid ->
          SystemDB.checkStep mem wid 0 selectStepName >>= either (error . show) pure
      }

-- | One launched instance per leaf over a fresh in-memory database: the
-- executor-level waits run through the launched connection, and the leaf
-- shuts the instance down inside the scenario.
simWaitExecFixture :: forall s. IOSim s (WaitExecFixture (IOSim s))
simWaitExecFixture = do
  mem <- newMemDB
  memSetApplication (Just "sim-app") mem
  dbos <- simInstance
  _ <- memLaunchOnWith mem simTracer dbos Nothing
  pure
    WaitExecFixture
      { weFreshWorkflowId = \label -> do
          let widText = "sim-waitexec-" <> label
          created <- SystemDB.initWorkflow mem ((newWorkflow widText) {newWorkflowName = Just "SimWaitExecTest"}) Nothing Fresh Nothing
          case created of
            Left err -> error (show err)
            Right _ -> pure (WorkflowId widText),
        weSettle = \wid ->
          SystemDB.recordWorkflowOutcome mem wid (OutcomeOutput (Just "null")) >>= either (error . show) (const (pure ())),
        weFirst = waitForFirstWorkflow dbos,
        weAll = waitForWorkflows dbos,
        weSingle = waitForWorkflow dbos,
        weShutdown = shutdown dbos
      }

tests :: TestTree
tests =
  testGroup
    "Waits (Sim)"
    [ simCase simWaitFixture "an empty first wait records its refusal and a replay reads it back" scenarioRefusal checkRefusal traceWaitSilent,
      simCase simWaitFixture "a recorded winner that left the set is refused" scenarioWinnerLeft checkWinnerLeft traceWaitSilent,
      simCase simWaitFixture "an all-wait completes over a settled workflow" scenarioJoinSettled checkJoinSettled traceWaitSilent,
      simCase simWaitFixture "select reports the first workflow to settle" scenarioSelectFirst checkSelectFirst traceWaitSilent,
      simCase simWaitFixture "a settled first id wins over a pending set" scenarioSelectSettledFirst checkSelectSettledFirst traceWaitSilent,
      simCase simWaitFixture "a replayed first-wait reads its recorded winner back" scenarioReplayWinner checkReplayWinner traceReplayWinner,
      simCase simWaitFixture "a cancelled workflow counts as settled" scenarioCancelled checkCancelled traceWaitSilent,
      simCase simWaitFixture "join returns when the last workflow settles" scenarioJoinLast checkJoinLast traceWaitSilent,
      simCase simWaitFixture "an empty all-wait is satisfied and takes no step" scenarioEmptyAll checkEmptyAll traceWaitSilent,
      simCase simWaitFixture "a repeated id is accepted by both waits" scenarioRepeated checkRepeated traceWaitSilent,
      simCase simWaitExecFixture "an executor-level first-wait over nothing is refused" scenarioExecFirstEmpty checkExecFirstEmpty traceWaitSilent,
      simCase simWaitExecFixture "an executor-level all-wait over nothing is satisfied" scenarioExecAllEmpty checkExecAllEmpty traceWaitSilent,
      simCase simWaitExecFixture "an executor-level first-wait reports the settled member" scenarioExecFirstWinner checkExecFirstWinner traceWaitSilent,
      simCase simWaitExecFixture "an executor-level all-wait over settled workflows completes" scenarioExecAllSettled checkExecAllSettled traceWaitSilent,
      simCase simWaitExecFixture "an executor-level single wait reports the settled outcome" scenarioExecSingle checkExecSingle traceWaitSilent
    ]

-- * Typed-event assertions (sim-only)

-- | Only a replayed winner announces: every other wait path is silent.
traceWaitSilent :: SimTrace a -> IO ()
traceWaitSilent tr =
  assertBool "no wait event may fire outside a replayed win" (null (selectTraceEventsDynamic tr :: [WaitEvent]))

-- | The replay adopts its recorded winner and says so.
traceReplayWinner :: SimTrace a -> IO ()
traceReplayWinner tr =
  assertBool "the replay must announce its recorded winner" (selectTraceEventsDynamic tr == [SelectWorkflowReplaying "sim-wait-replay-b"])
