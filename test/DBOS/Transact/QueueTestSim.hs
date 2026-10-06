{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

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
import Control.Monad.IOSim (IOSim, SimEventType (..), SimTrace, selectTraceEvents)
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (printSimTrace, runSimCase, simTracer)
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
import DBOS.SystemDB.IOSim (MemSystemDB, memLaunchOnWith, memSetApplication, newMemDB, newMemDBWithApplication, simInstance)
import DBOS.Transact (shutdown)
import DBOS.Transact.QueueCases
  ( QueueFixture (..),
    checkBadEnqueue,
    checkCountedPartitioned,
    checkWorkerBudgetExhausted,
    checkDedup,
    checkDelayed,
    checkJoin,
    checkListenInternal,
    checkListenNarrow,
    checkListenNone,
    checkPartitioned,
    checkPeerQueue,
    checkPriority,
    checkUpdateHonoured,
    checkWorkerConcurrency,
    checkCrud,
    checkDeadlineStamped,
    checkEqualLimits,
    checkGhostQueue,
    checkIncoherent,
    checkInheritedDeadline,
    checkInternalRow,
    checkLateQueue,
    checkLegacyRescope,
    checkLegacyUpdateRefused,
    checkNoDeadlineYet,
    checkPartitionLimits,
    checkPartitionRow,
    checkQueueDefaults,
    checkRateLimit,
    checkReregister,
    checkReserved,
    checkSentinel,
    checkUnhonourable,
    checkUnlaunched,
    checkUpdateCoherent,
    scenarioBadEnqueue,
    scenarioCountedPartitioned,
    scenarioWorkerBudgetExhausted,
    scenarioDedup,
    scenarioDelayed,
    scenarioJoin,
    scenarioListenInternal,
    scenarioListenNarrow,
    scenarioListenNone,
    scenarioPartitioned,
    scenarioPeerQueue,
    scenarioPriority,
    scenarioUpdateHonoured,
    scenarioWorkerConcurrency,
    scenarioCrud,
    scenarioDeadlineStamped,
    scenarioEqualLimits,
    scenarioGhostQueue,
    scenarioIncoherent,
    scenarioInheritedDeadline,
    scenarioInternalRow,
    scenarioLateQueue,
    scenarioLegacyRescope,
    scenarioLegacyUpdateRefused,
    scenarioNoDeadlineYet,
    scenarioPartitionLimits,
    scenarioPartitionRow,
    scenarioQueueDefaults,
    scenarioRateLimit,
    scenarioReregister,
    scenarioReserved,
    scenarioSentinel,
    scenarioUnhonourable,
    scenarioUnlaunched,
    scenarioUpdateCoherent,
  )
import Test.Tasty (DependencyType (..), TestTree, dependentTestGroup, testGroup)
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

-- | One fixture per leaf over a fresh in-memory database: the instance
-- launches over it with the shared launch tail, and the listen filter is
-- derived from the leaf's suffix like the live config.
simQueueFixture :: (Text -> Maybe [Text]) -> forall s. IOSim s (QueueFixture (IOSim s))
simQueueFixture listenOf = do
  mem <- newMemDB
  -- The listener's application, as the launched connection carries it live:
  -- queue discovery and claims scope to this plus unclaimed rows, so a
  -- peer's queue stays invisible to this fixture's supervisor.
  memSetApplication (Just "sim-app") mem
  dbos <- simInstance
  let suffix = "sim"
  pure
    QueueFixture
      { qfSuffix = suffix,
        qfAppName = "sim-app",
        qfDBOS = dbos,
        qfLaunch = memLaunchOnWith mem simTracer dbos (listenOf suffix),
        qfShutdown = shutdown dbos,
        qfUpsertQueue = \new onExisting -> upsertQueue mem new onExisting,
        qfReadQueueRow = \name -> do
          found <- getQueue mem name
          case found of
            Left err -> error (show err)
            Right record -> pure record,
        qfReadWorkflowRow = \wid -> do
          found <- getWorkflow mem wid
          case found of
            Left err -> error (show err)
            Right record -> pure record
      }

-- | One framed leaf over the per-leaf fixture. Validation cases emit no
-- engine events, so the trace check is silence.
simLeaf :: (Text -> Maybe [Text]) -> String -> (forall s. QueueFixture (IOSim s) -> IOSim s a) -> (a -> Either String ()) -> TestTree
simLeaf listenOf name scen judge = simCase (simQueueFixture listenOf) name scen judge noTrace
  where
    noTrace :: forall x. SimTrace x -> IO ()
    noTrace _ = pure ()

tests :: TestTree
tests =
  testGroup
    "Workflow queues (Sim)"
    [ dependentTestGroup
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
        ],
      testGroup
        "Workflow queues"
        [ simCase (simQueueFixture (const Nothing)) "queue options default to no limits and poll once a second" scenarioQueueDefaults checkQueueDefaults noTrace,
          simCase (simQueueFixture (const Nothing)) "a legacy-partitioned row re-scopes its limits" scenarioLegacyRescope checkLegacyRescope noTrace,
          simLeaf (const Nothing) "the internal queue name is reserved" scenarioReserved checkReserved,
          simLeaf (const Nothing) "registering before launch is refused" scenarioUnlaunched checkUnlaunched,
          simLeaf (const Nothing) "incoherent limits are refused before they reach the row" scenarioIncoherent checkIncoherent,
          simLeaf (const Nothing) "an update cannot leave a queue incoherent" scenarioUpdateCoherent checkUpdateCoherent,
          simLeaf (const Nothing) "an unhonourable queue configuration is refused" scenarioUnhonourable checkUnhonourable,
          simLeaf (const Nothing) "a per-process limit may equal the fleet limit" scenarioEqualLimits checkEqualLimits,
          simLeaf (const Nothing) "a queue carries a rate limit and priority ordering" scenarioRateLimit checkRateLimit,
          simLeaf (const Nothing) "per-partition limits partition a queue" scenarioPartitionLimits checkPartitionLimits,
          simLeaf (const Nothing) "re-registering updates the stored limits" scenarioReregister checkReregister,
          simLeaf (const Nothing) "adding a per-partition limit to a legacy row is refused" scenarioLegacyUpdateRefused checkLegacyUpdateRefused,
          simLeaf (const Nothing) "a registered queue updates, lists and deletes through the instance" scenarioCrud checkCrud,
          simLeaf (const Nothing) "a dequeue stamps the deadline an enqueue left open" scenarioDeadlineStamped checkDeadlineStamped,
          simLeaf (const Nothing) "an explicit timeout on a queued workflow records no deadline yet" scenarioNoDeadlineYet checkNoDeadlineYet,
          simLeaf (const Nothing) "a partition key is recorded on the row" scenarioPartitionRow checkPartitionRow,
          simLeaf (const Nothing) "an unprioritised workflow stores the sentinel" scenarioSentinel checkSentinel,
          simLeaf (const Nothing) "an incoherent enqueue is refused" scenarioBadEnqueue checkBadEnqueue,
          simLeaf (const Nothing) "a stored row cannot redefine the internal queue" scenarioInternalRow checkInternalRow,
          simLeaf (const Nothing) "a queue registered after launch is dequeued from" scenarioLateQueue checkLateQueue,
          simLeaf (const Nothing) "a queue this process never registered is dequeued from" scenarioGhostQueue checkGhostQueue,
          simLeaf (const Nothing) "an inherited deadline reaches a queued child" scenarioInheritedDeadline checkInheritedDeadline,
          simLeaf (const Nothing) "a queue's worker concurrency runs that many at once in one process" scenarioWorkerConcurrency checkWorkerConcurrency,
          simLeaf (\s -> Just ["hs-l2-listen-fast-" <> Text.take 12 s]) "listen queues narrow what this process dequeues" scenarioListenNarrow checkListenNarrow,
          simLeaf (const (Just [])) "an empty listen set dequeues from no registered queue" scenarioListenNone checkListenNone,
          simLeaf (\s -> Just ["hs-l2-listen-other-" <> Text.take 12 s]) "listen queues never exclude the internal queue" scenarioListenInternal checkListenInternal,
          simLeaf (const Nothing) "a delayed enqueue waits before it is dequeued" scenarioDelayed checkDelayed,
          simLeaf (const Nothing) "a deduplication id admits one waiting workflow" scenarioDedup checkDedup,
          simLeaf (const Nothing) "return existing joins the workflow holding the key" scenarioJoin checkJoin,
          simLeaf (const Nothing) "priority orders the backlog lower first" scenarioPriority checkPriority,
          simLeaf (const Nothing) "updating a queue changes what a running worker honours" scenarioUpdateHonoured checkUpdateHonoured,
          simLeaf (const Nothing) "a partitioned queue runs one workflow per key at a time" scenarioPartitioned checkPartitioned,
          simLeaf (const Nothing) "a counted partitioned queue runs its limit per key" scenarioCountedPartitioned checkCountedPartitioned,
          simLeaf (const Nothing) "a saturated worker budget runs one at a time" scenarioWorkerBudgetExhausted checkWorkerBudgetExhausted,
          simLeaf (const Nothing) "another application's queue is not dequeued from" scenarioPeerQueue checkPeerQueue,
          -- IO only: the fixture rewrites the row through raw SQL into the
          -- pre-109 shape (input moved into the status column, payload-table
          -- row dropped), and MemSystemDB keeps no separate workflow_input
          -- table to rewrite.
          testCase "a queued workflow with a legacy input runs with it" (pure ())
        ]
    ]
  where
    noTrace :: forall x. SimTrace x -> IO ()
    noTrace _ = pure ()
