{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The queue claim's application scoping, deterministic under io-sim.
--
-- The live suite's queue flakes came from application-less queued rows:
-- per the oracle, an unclaimed row belongs to every application, so any
-- launched instance draining all queues (the default) could claim a
-- PostgresTest fixture before the test's own sweep, leaving the fixture
-- PENDING under a foreign executor. This tree reproduces that mechanism
-- deterministically over 'MemSystemDB' — which now mirrors the Postgres
-- claim's application filter — and pins the fix: a row that names its
-- application is only claimed by that application's listener.
--
-- The scheduling events ('SimEventType') are asserted directly: the two
-- racing sweeps are forked threads whose order is fixed by the
-- cooperative scheduler (io-sim appends forks to the runqueue in order
-- and never preempts), and no timer events are involved, so the
-- reproduction depends on the schedule alone.
module DBOS.Transact.QueueTestSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, SimEventType (..), selectTraceEvents)
import Data.Text (Text)
import DBOS.IOSimTracer (printSimTrace, runSimCase)
import DBOS.SystemDB
  ( Error,
    NewWorkflow (..),
    OnExistingQueue (..),
    QueueRecord,
    Submission (..),
    WorkflowId (..),
    WorkflowRecord (..),
    getQueue,
    getWorkflow,
    initWorkflow,
    newQueue,
    newWorkflow,
    startQueuedWorkflows,
    upsertQueue,
  )
import DBOS.SystemDB.IOSim (MemSystemDB, memSetApplication, newMemDBWithApplication)
import Test.Tasty (DependencyType (..), TestTree, dependentTestGroup)
import Test.Tasty.HUnit (testCase, (@?=))

-- * Staging

queueName :: Text
queueName = "q-scoped"

-- | One queue and one queued row in it. The row's application is what the
-- claim's filter reads: 'Nothing' is unclaimed (belongs to every
-- application), @Just name@ belongs to that application alone.
stageQueuedRow :: MemSystemDB s -> Text -> Maybe Text -> IOSim s QueueRecord
stageQueuedRow mem widText application = do
  _ <- upsertQueue mem (newQueue queueName) UpdateExisting
  _ <-
    initWorkflow
      mem
      ( (newWorkflow widText)
          { newWorkflowName = Just "fixture",
            newWorkflowQueueName = Just queueName,
            newWorkflowApplicationName = application
          }
      )
      Nothing
      Fresh
      Nothing
  record <- getQueue mem queueName
  case record of
    Right (Just queue) -> pure queue
    other -> error ("QueueTestSim: queue missing after staging: " <> show other)

-- | The claim a listener makes: the same engine call the supervisor's
-- poll makes for one queue.
claim :: MemSystemDB s -> QueueRecord -> Text -> IOSim s (Either Error [WorkflowId])
claim mem record executor = startQueuedWorkflows mem record executor "v1" Nothing 0 0

rowExecutor :: MemSystemDB s -> Text -> IOSim s (Maybe Text)
rowExecutor mem widText = do
  row <- getWorkflow mem (WorkflowId widText)
  pure $ case row of
    Right (Just record) -> record.workflowRecordExecutorId
    _ -> Nothing

-- * Scenarios

-- | Two listeners race for one unclaimed row. Both are forked before
-- either is awaited, so the cooperative scheduler runs them in fork
-- order; the first claims, the second sees nothing. Returns the two
-- results, the executor the row ended up stamped with, and the fork
-- order the trace must show.
scenarioRace :: IOSim s (Either Error [WorkflowId], Either Error [WorkflowId], Maybe Text, [ThreadId (IOSim s)])
scenarioRace = do
  mem <- newMemDBWithApplication (Just "foreign")
  record <- stageQueuedRow mem "race" Nothing
  first <- async $ do
    tid <- myThreadId
    claimed <- claim mem record "foreign"
    pure (tid, claimed)
  second <- async $ do
    tid <- myThreadId
    claimed <- claim mem record "owner"
    pure (tid, claimed)
  (firstTid, firstClaimed) <- wait first
  (secondTid, secondClaimed) <- wait second
  executor <- rowExecutor mem "race"
  pure (firstClaimed, secondClaimed, executor, [firstTid, secondTid])

-- | A row that names its application: the foreign listener's sweep skips
-- it, the owner's claims it.
scenarioScoped :: IOSim s (Either Error [WorkflowId], Either Error [WorkflowId], Maybe Text)
scenarioScoped = do
  mem <- newMemDBWithApplication (Just "foreign")
  record <- stageQueuedRow mem "scoped" (Just "owner")
  foreignClaim <- claim mem record "foreign"
  memSetApplication (Just "owner") mem
  owner <- claim mem record "owner"
  executor <- rowExecutor mem "scoped"
  pure (foreignClaim, owner, executor)

-- * Cases

tests :: TestTree
tests =
  dependentTestGroup
    "Queue claims (IOSim)"
    AllFinish
    [ testCase "two listeners race for an unclaimed row; the first claims it" $ do
        ((firstClaimed, secondClaimed, executor, forked), tr) <- runSimCase scenarioRace
        printSimTrace tr
        firstClaimed @?= Right [WorkflowId "race"]
        secondClaimed @?= Right []
        executor @?= Just "foreign"
        -- The fork order the scheduler used is the fork order we created,
        -- so the winner is fixed by the schedule, not by timing.
        selectTraceEvents (\_ -> \case EventThreadForked tid -> Just tid; _ -> Nothing) tr @?= forked
        -- No timer events: the race is decided by the cooperative
        -- schedule alone, which is what makes the reproduction
        -- deterministic.
        selectTraceEvents (\_ -> \case EventThreadDelay _ _ -> Just (); _ -> Nothing) tr @?= [],
      testCase "an application-scoped row is claimed only by its application" $ do
        ((foreignClaim, owner, executor), tr) <- runSimCase scenarioScoped
        printSimTrace tr
        foreignClaim @?= Right []
        owner @?= Right [WorkflowId "scoped"]
        executor @?= Just "owner"
    ]
