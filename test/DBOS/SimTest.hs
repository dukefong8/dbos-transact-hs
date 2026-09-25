{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Headless mirrors of the starter acceptance flows: the same bodies run
-- against 'SimDB' under @io-sim@, with zero wall-clock time. Say-traces are
-- golden files, so any behavioral drift fails the suite exactly where the
-- transcript diverges.
module DBOS.SimTest
  ( tests,
  )
where

import DBOS.Prelude
import Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import Control.Monad.Class.MonadSay (MonadSay (..))
import Control.Monad.IOSim (IOSim, runSim, runSimTrace, selectTraceEventsSay)
import DBOS.SimDB (SimDB (..), newSimDB, simEventStore, simStepStore)
import DBOS.Transact
  ( EventStore (..),
    OperationId (..),
    OperationName (..),
    SerializedWorkflowValue,
    StepStore (..),
    WorkflowId (..),
    encodeWorkflowValue,
    runStep,
  )
import Data.ByteString.Lazy qualified as LBS
import Data.Text (pack)
import Data.Text.Encoding (encodeUtf8)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.Golden (goldenVsString)
import Test.Tasty.HUnit ((@?=), testCase)

tests :: TestTree
tests =
  testGroup
    "Simulation"
    [ goldenVsString
        "crash and resume replays recorded steps"
        "test/golden/sim-crash-recovery.txt"
        (pure (LBS.fromStrict (encodeUtf8 (pack (unlines (selectTraceEventsSay (runSimTrace sim))))))),
      testCase "simulated crash resumes with each step running once" $
        case runSim sim of
          Right result -> result @?= (3, Just (encodeWorkflowValue (3 :: Int)))
          Left _ -> fail "simulation failed"
    ]

-- | The starter's three-step workflow with progress events, against stores.
simBody ::
  (MonadSTM m, MonadSay m, MonadThrow m) =>
  StepStore m ->
  EventStore m ->
  StrictTVar IO m Int ->
  WorkflowId ->
  Maybe OperationId ->
  m ()
simBody steps events executions workflowId crashAfter = do
  crashBefore (OperationId 1)
  _ <- runStep steps workflowId (OperationId 1) (OperationName "step_one") (countedStep executions "step_one")
  (events.eventSet) workflowId "steps_event" (encodeWorkflowValue (1 :: Int))
  say "published steps_event=1"
  crashBefore (OperationId 2)
  _ <- runStep steps workflowId (OperationId 2) (OperationName "step_two") (countedStep executions "step_two")
  (events.eventSet) workflowId "steps_event" (encodeWorkflowValue (2 :: Int))
  say "published steps_event=2"
  crashBefore (OperationId 3)
  _ <- runStep steps workflowId (OperationId 3) (OperationName "step_three") (countedStep executions "step_three")
  (events.eventSet) workflowId "steps_event" (encodeWorkflowValue (3 :: Int))
  say "published steps_event=3"
  where
    crashBefore operationId =
      case crashAfter of
        Just crashId | crashId == operationId -> throwM SimCrash
        _ -> pure ()
    countedStep execCounter name = do
      atomically $ do
        count <- readTVar execCounter
        writeTVar execCounter (count + 1)
      say ("ran " <> name)
      pure (encodeWorkflowValue ())

data SimCrash = SimCrash
  deriving stock (Eq, Show)

instance Exception SimCrash

sim :: forall s. IOSim s (Int, Maybe SerializedWorkflowValue)
sim = do
  db <- newSimDB
  executions <- newTVarIO 0
  let steps = simStepStore db
      events = simEventStore db
      workflowId = WorkflowId "sim-wf-1"
  -- First attempt: step one finishes, then the process "crashes".
  crashed <- try (simBody steps events executions workflowId (Just (OperationId 2))) :: IOSim s (Either SimCrash ())
  case crashed of
    Left SimCrash -> pure ()
    Right () -> throwM (userError "the simulated crash never fired; this golden would test nothing")
  -- Restart: step one replays from its checkpoint instead of running again.
  simBody steps events executions workflowId Nothing
  count <- readTVarIO executions
  published <- (events.eventGet) workflowId "steps_event"
  pure (count, published)
