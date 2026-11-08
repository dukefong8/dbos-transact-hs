{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The SystemDB seam mirrored under IOSim against the mock backend: one
-- case per class method, asserting the canned answer 'MockSystemDB'
-- returns. The live SQL semantics stay in 'DBOS.SystemDB.PostgresTest';
-- this group proves every method is callable under IOSim and that the
-- engine's backend seam has an @IOSim@ instance.
module DBOS.SystemDB.IOSimTest (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, runSimOrThrow)
import Data.Map.Strict qualified as Map
import DBOS.SystemDB
  ( AppScope (..),
    AttributeUpdate (..),
    WakeTime (..),
    AwaitedOutcome (..),
    Change (..),
    Debounce (..),
    DebounceRequest (..),
    Dequeued (..),
    DequeueStop (..),
    EncodedValue (..),
    Error (..),
    EventRecord (..),
    Fork (..),
    ForkOptions (..),
    ForkPoint (..),
    InitWorkflowCaller (..),
    NewQueue (..),
    NewQueue (..),
    NewWorkflow (..),
    NotificationRecord (..),
    OnExistingQueue (..),
    Outcome (..),
    OutcomeWrite (..),
    QueueRecord (..),
    RenameBatching (..),
    RenameFrom (..),
    ScheduleRecord (..),
    ScheduleStatus (..),
    ScheduleUpdate (..),
    EncodedSendMessage (..),
    SerializedWorkflowValue (..),
    StepRecord (..),
    SysRewindOptions (..),
    SystemDB (..),
    Timestamp (..),
    Topic (..),
    VersionInfo (..),
    WorkflowOwner (..),
    WrittenBy (..),
    WorkflowDelay (..),
    WorkflowFilter (..),
    WorkflowId (..),
    WorkflowIdReusePolicy (..),
    WorkflowInitResult (..),
    WorkflowRecord (..),
    DatabaseTime (..),
    WorkflowStatus (..),
    defaultForkOptions,
    defaultQueueUpdate,
    defaultScheduleFilter,
    defaultScheduleUpdate,
    defaultWorkflowFilter,
    forkNew,
    message,
    millisDuration,
    newQueue,
    newSchedule,
    newWorkflow,
    newQualifiedWorkflowName,
    parseApplicationName,
    secondsDuration,
    spansPartitions,
    timestampFromEpochMs,
    zeroRowCounts)
import DBOS.SystemDB.IOSim (MemSystemDB (memRows, memSteps), MockSystemDB (..), memInitWorkflowAs, newMemDB)
import DBOS.Transact.WorkflowSimData (mockTimestamp)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "IOSim SystemDB (mirror)"
    [ workflowTests,
      waitTests,
      stepTests,
      eventMessageTests,
      versionTests,
      queueTests,
      scheduleTests,
      lifecycleTests,
      memTests
    ]

-- * Helpers

backend :: MockSystemDB
backend = MockSystemDB

run :: (forall s. IOSim s a) -> a
run = runSimOrThrow

-- | The same runner for test bodies written as IO do-blocks.
runMem :: (forall s. IOSim s a) -> IO a
runMem action = pure (runSimOrThrow action)

at :: Timestamp
at = timestampFromEpochMs 1000

-- * Workflows

workflowTests :: TestTree
workflowTests =
  testGroup
    "Workflows"
    [ testCase "initWorkflow answers with an executable pending row" $ do
        let result = run (initWorkflow backend (newWorkflow "wf-1") Nothing WorkflowIdReturnExisting)
        case result of
          Right initialized -> do
            initialized.initResultStatus @?= Pending
            initialized.initResultShouldExecute @?= True
          Left err -> fail (show err),
      testCase "getWorkflow answers by id" $ do
        let result = run (getWorkflow backend (WorkflowId "wf-1"))
        case result of
          Right (Just record) -> record.workflowRecordId @?= WorkflowId "wf-1"
          other -> fail (show other),
      testCase "getWorkflow reports an absent row as absence" $ do
        run (getWorkflow backend (WorkflowId "missing")) @?= Right Nothing,
      testCase "listWorkflows echoes the ids it is filtered by" $ do
        let filter = defaultWorkflowFilter {workflowFilterWorkflowIds = ["wf-1", "wf-2"]}
            result = run (listWorkflows backend filter Nothing)
        case result of
          Right records -> map (.workflowRecordId) records @?= [WorkflowId "wf-1", WorkflowId "wf-2"]
          Left err -> fail (show err),
      testCase "getWorkflowChildren answers a child" $ do
        run (getWorkflowChildren backend (WorkflowId "wf-1")) @?= Right [WorkflowId "mock-child"],
      testCase "recordWorkflowOutcome records once" $ do
        run (recordWorkflowOutcome backend (WorkflowId "wf-1") (OutcomeOutput (Just "\"mock\"")) "mock-token") @?= Right Recorded,
      testCase "setWorkflowDelay succeeds" $ do
        run (setWorkflowDelay backend (WorkflowId "wf-1") (DelayFor (secondsDuration 1)) Nothing) @?= Right (),
      testCase "clearQueueAssignment releases the claim" $ do
        run (clearQueueAssignment backend (WorkflowId "wf-1")) @?= Right True,
      testCase "updateWorkflowAttributes succeeds" $ do
        run (updateWorkflowAttributes backend (WorkflowId "wf-1") (Replace (Just "{}")) Nothing) @?= Right (),
      testCase "reenqueueForRecovery returns nothing to recover" $ do
        run (reenqueueForRecovery backend ["exec-1"] "app" "0.0.0") @?= Right [],
      testCase "transitionDelayedWorkflows moves nothing" $ do
        run (transitionDelayedWorkflows backend) @?= Right 0,
      testCase "cancelWorkflows echoes the ids it cancelled" $ do
        run (cancelWorkflows backend [WorkflowId "wf-1"] False Nothing Nothing) @?= Right [WorkflowId "wf-1"],
      testCase "resumeWorkflows echoes the ids it resumed" $ do
        run (resumeWorkflows backend [WorkflowId "wf-1"] Nothing Nothing Nothing) @?= Right [WorkflowId "wf-1"],
      testCase "deleteWorkflows counts the ids it deleted" $ do
        run (deleteWorkflows backend [WorkflowId "wf-1", WorkflowId "wf-2"] False Nothing) @?= Right 2,
      testCase "renameApplication reports zero row counts" $ do
        let target = either (error . show) id (parseApplicationName "target")
        run (renameApplication backend (RenameApplication "app") target Unbatched) @?= Right zeroRowCounts
    ]

-- * Waits

waitTests :: TestTree
waitTests =
  testGroup
    "Waits"
    [ testCase "awaitWorkflowResult answers a success" $ do
        run (awaitWorkflowResult backend (WorkflowId "wf-1") (secondsDuration 1) True) @?= Right (AwaitedSucceeded (Just "mock-output") (Just "portable_json")),
      testCase "awaitFirstWorkflowId answers the first id" $ do
        run (awaitFirstWorkflowId backend [WorkflowId "wf-1", WorkflowId "wf-2"] (secondsDuration 1)) @?= Right (WorkflowId "wf-1"),
      -- Placeholder pin: the mock answers every call canned, so the empty
      -- set gets the mock winner. The live refusal lives in
      -- 'DBOS.SystemDB.PostgresTest' and the mirror refusal in the shared
      -- executor-level wait cases (WaitCases).
      testCase "awaitFirstWorkflowId answers a placeholder for an empty set" $ do
        run (awaitFirstWorkflowId backend [] (secondsDuration 1)) @?= Right (WorkflowId "mock"),
      testCase "awaitWorkflowIds waits through" $ do
        run (awaitWorkflowIds backend [WorkflowId "wf-1"] (secondsDuration 1)) @?= Right ()
    ]

-- * Steps, children, sleeps

stepTests :: TestTree
stepTests =
  testGroup
    "Steps"
    [ testCase "checkStep reports no recorded step" $ do
        run (checkStep backend (WorkflowId "wf-1") 0 "step") @?= Right Nothing,
      testCase "recordStep succeeds" $ do
        run (recordStep backend (WorkflowId "wf-1") 0 "step" (OutcomeOutput (Just "\"v\"")) Nothing Nothing "mock-token") @?= Right (),
      testCase "listSteps answers the workflow's steps" $ do
        let result = run (listSteps backend (WorkflowId "wf-1") False Nothing Nothing Nothing)
        case result of
          Right steps -> map (.stepRecordStepName) steps @?= ["mock-step"]
          Left err -> fail (show err),
      testCase "recordSleep answers its wake time" $ do
        let woken = run (recordSleep backend (WorkflowId "wf-1") 0 (secondsDuration 1) "mock-token")
        case woken of
          Right wake -> wake.wakeAt @?= mockTimestamp
          other -> fail ("expected the mock wake time, got: " <> show other),
      testCase "checkChildResult uses the provided default" $ do
        run (checkChildResult backend (WorkflowId "parent") 0) @?= Right Nothing,
      testCase "recordChildWorkflow succeeds" $ do
        run (recordChildWorkflow backend (WorkflowId "parent") (WorkflowId "child") 0 "child-start" Nothing "mock-token") @?= Right (),
      testCase "recordChildResult succeeds" $ do
        run (recordChildResult backend (WorkflowId "parent") 0 (WorkflowId "child") (OutcomeOutput (Just "\"v\"")) Nothing Nothing "mock-token") @?= Right ()
    ]

-- * Events, notifications, messages

eventMessageTests :: TestTree
eventMessageTests =
  testGroup
    "Events and messages"
    [ testCase "setEvent succeeds" $ do
        run (setEvent backend (WorkflowId "wf-1") 0 "key" "\"v\"" Nothing "mock-token") @?= Right (),
      testCase "getEvent answers a value" $ do
        run (getEvent backend (WorkflowId "wf-1") "key" (secondsDuration 1) Nothing) @?= Right (Just (EncodedValue "mock-event" (Just "portable_json"))),
      testCase "getAllEvents answers the recorded events" $ do
        let result = run (getAllEvents backend (WorkflowId "wf-1"))
        case result of
          Right events -> map (.eventKey) events @?= ["mock-key"]
          Left err -> fail (show err),
      testCase "getAllNotifications answers the recorded messages" $ do
        let result = run (getAllNotifications backend (WorkflowId "wf-1"))
        case result of
          Right notifications -> map (.notificationRecordMessageUuid) notifications @?= ["mock-message-uuid"]
          Left err -> fail (show err),
      testCase "sendMessage succeeds" $ do
        run (sendMessage backend (message (WorkflowId "wf-1") (SerializedWorkflowValue "\"v\"" Nothing)) Nothing Nothing Nothing False) @?= Right (),
      testCase "sendMessages succeeds" $ do
        run (sendMessages backend [message (WorkflowId "wf-1") (SerializedWorkflowValue "\"v\"" Nothing)] Nothing Nothing Nothing False) @?= Right (),
      testCase "recv answers a message" $ do
        run (recv backend (WorkflowId "wf-1") 0 0 Nothing (secondsDuration 1) "mock-token") @?= Right (Just (EncodedValue "mock-message" (Just "portable_json")))
    ]

-- * Versions

versionTests :: TestTree
versionTests =
  testGroup
    "Versions"
    [ testCase "createApplicationVersion succeeds" $ do
        run (createApplicationVersion backend "0.0.0" Nothing) @?= Right (),
      testCase "listApplicationVersions answers the registered versions" $ do
        let result = run (listApplicationVersions backend)
        case result of
          Right versions -> map (.versionInfoName) versions @?= ["0.0.0"]
          Left err -> fail (show err),
      testCase "getLatestApplicationVersion answers a version" $ do
        let result = run (getLatestApplicationVersion backend Nothing)
        case result of
          Right (Just version) -> version.versionInfoId @?= "mock-version-id"
          other -> fail (show other),
      testCase "updateApplicationVersionTimestamp succeeds" $ do
        run (updateApplicationVersionTimestamp backend "0.0.0" at Nothing) @?= Right ()
    ]

-- * Queues

queueTests :: TestTree
queueTests =
  testGroup
    "Queues"
    [ testCase "upsertQueue reports a write" $ do
        run (upsertQueue backend (newQueue "q") UpdateExisting) @?= Right True,
      testCase "startQueuedWorkflows answers a claimed id" $ do
        fmap (.dequeuedWorkflowIds) (run (startQueuedWorkflows backend mockQueueRecord "exec" "0.0.0" Nothing 0 0 "tok-mock")) @?= Right [WorkflowId "mock-queued"],
      testCase "getQueuePartitions answers a partition" $ do
        run (getQueuePartitions backend "q") @?= Right ["mock-partition"],
      testCase "startQueuedPartitionedWorkflows answers a claimed id" $ do
        run (startQueuedPartitionedWorkflows backend mockQueueRecord "exec" "0.0.0" Nothing "tok-mock") @?= Right [WorkflowId "mock-partitioned"],
      testCase "getQueue answers the named queue" $ do
        let result = run (getQueue backend "q")
        case result of
          Right (Just record) -> record.queueRecordName @?= "q"
          other -> fail (show other),
      testCase "listQueues answers a queue" $ do
        let result = run (listQueues backend Unset)
        case result of
          Right records -> assertBool "a queue is listed" (not (null records))
          Left err -> fail (show err),
      testCase "updateQueue keeps the queue's name" $ do
        let result = run (updateQueue backend "q" defaultQueueUpdate (\_ _ -> Right ()))
        case result of
          Right record -> record.queueRecordName @?= "q"
          Left err -> fail (show err),
      testCase "debounceDelayedWorkflow answers a debounced holder" $ do
        run (debounceDelayedWorkflow backend debounceRequest Nothing) @?= Right (Debounced "mock-debounced"),
      testCase "getDeduplicationKeyHolder answers the holder" $ do
        run (getDeduplicationKeyHolder backend "q" "key") @?= Right (Just (WorkflowId "mock-holder")),
      testCase "deleteQueue succeeds" $ do
        run (deleteQueue backend "q") @?= Right ()
    ]
  where
    mockQueueRecord =
      QueueRecord
        { queueRecordName = "q",
          queueRecordConcurrency = Nothing,
          queueRecordWorkerConcurrency = Nothing,
          queueRecordRateLimit = Nothing,
          queueRecordPriorityEnabled = False,
          queueRecordPartitionQueue = False,
          queueRecordPartitionConcurrency = Nothing,
          queueRecordPartitionWorkerConcurrency = Nothing,
          queueRecordPartitionRateLimit = Nothing,
          queueRecordPollingInterval = secondsDuration 1,
          queueRecordAppName = Just "app"
        }
    debounceRequest =
      DebounceRequest
        { debounceRequestWorkflow = newQualifiedWorkflowName "wf",
          debounceRequestQueueName = "q",
          debounceRequestDeduplicationId = "key",
          debounceRequestPeriod = secondsDuration 1,
          debounceRequestInputs = Nothing,
          debounceRequestSerialization = Nothing,
          debounceRequestAppName = Nothing
        }

-- * Schedules

scheduleTests :: TestTree
scheduleTests =
  testGroup
    "Schedules"
    [ testCase "createSchedule succeeds" $ do
        run (createSchedule backend (newSchedule "s" "wf" "* * * * *") Nothing) @?= Right (),
      testCase "upsertSchedule succeeds" $ do
        run (upsertSchedule backend (newSchedule "s" "wf" "* * * * *") Nothing) @?= Right (),
      testCase "applySchedules succeeds" $ do
        run (applySchedules backend [newSchedule "s" "wf" "* * * * *"]) @?= Right (),
      testCase "getSchedule answers the named schedule" $ do
        let result = run (getSchedule backend "s" Nothing)
        case result of
          Right (Just record) -> record.scheduleRecordName @?= "s"
          other -> fail (show other),
      testCase "listSchedules answers a schedule" $ do
        let result = run (listSchedules backend defaultScheduleFilter Nothing)
        case result of
          Right records -> assertBool "a schedule is listed" (not (null records))
          Left err -> fail (show err),
      testCase "updateSchedule succeeds" $ do
        run (updateSchedule backend "s" defaultScheduleUpdate Nothing) @?= Right (),
      testCase "setScheduleStatus succeeds" $ do
        run (setScheduleStatus backend "s" Active Nothing) @?= Right (),
      testCase "updateScheduleLastFiredAt succeeds" $ do
        run (updateScheduleLastFiredAt backend "s" at) @?= Right (),
      testCase "deleteSchedule succeeds" $ do
        run (deleteSchedule backend "s" Nothing) @?= Right ()
    ]

-- * Forks and lifecycle

lifecycleTests :: TestTree
lifecycleTests =
  testGroup
    "Forks and lifecycle"
    [ testCase "forkWorkflows answers the forked ids" $ do
        run (forkWorkflows backend [forkNew "wf-1"] defaultForkOptions Nothing) @?= Right [WorkflowId "wf-1"],
      testCase "forkFrom answers the forked ids" $ do
        run (forkFrom backend [WorkflowId "wf-1"] (ForkStep 0) defaultForkOptions Nothing) @?= Right [WorkflowId "wf-1"],
      testCase "close runs under IOSim" $ do
        run (close backend) @?= ()
    ]

-- * MemSystemDB: the stateful sim backend's own subsystems

-- | The stateful 'MemSystemDB' is what the shared workflow scenarios run
-- over; these cases pin the subsystems step 4 implemented — queue claims
-- and their budgets, the delayed and recovery transitions, message
-- wakeups, events, schedules, and versions — so the sim semantics the
-- engine relies on are asserted here rather than assumed.
-- | Two clock readings five virtual seconds apart: the seam has to move
-- with the simulated clock rather than answer a constant.
simClockTwin :: forall s. MemSystemDB s -> IOSim s (DatabaseTime, DatabaseTime)
simClockTwin mem = do
  first <- orFailSim =<< databaseTime mem
  threadDelay 5000000
  later <- orFailSim =<< databaseTime mem
  pure (first, later)

memTests :: TestTree
memTests =
  testGroup
    "MemSystemDB (stateful)"
    [ testCase "a start runs only the row it created" $ do
        (firstExecute, firstCreator, secondExecute, owner) <- runMem $ do
          mem <- newMemDB
          first <- orFailSim =<< initWorkflow mem (newWorkflow "wf-c") Nothing WorkflowIdReturnExisting
          second <- orFailSim =<< initWorkflow mem (newWorkflow "wf-c") Nothing WorkflowIdReturnExisting
          row <- orJustSim "the created row" =<< orFailSim =<< getWorkflow mem (WorkflowId "wf-c")
          pure (first.initResultShouldExecute, first.initResultCreatorXid, second.initResultShouldExecute, row.workflowRecordOwnerXid)
        firstExecute @?= True
        secondExecute @?= False
        owner @?= Just firstCreator,
      -- Twin of the live debounced-conflict arms: a debounced insert that
      -- meets its key holder by id loses the key (the race answers as the
      -- lost deduplication would), while any other occupant is a
      -- conflict. A retry of the same insert then keeps the input a
      -- bounce replaced, as the write guards on a fresh insert.
      testCase "a debounced insert meeting its holder by id loses the key" $ do
        (lost, conflicted, kept) <- runMem $ do
          mem <- newMemDB
          let base =
                (newWorkflow "wf-b")
                  { newWorkflowName = Just "bouncer",
                    newWorkflowQueueName = Just "q",
                    newWorkflowDeduplicationId = Just "key",
                    newWorkflowDelay = Just (secondsDuration 60),
                    newWorkflowIsDebounced = True,
                    newWorkflowInput = Just "{\"n\":1}"
                  }
              bounce =
                DebounceRequest
                  { debounceRequestWorkflow = newQualifiedWorkflowName "bouncer",
                    debounceRequestQueueName = "q",
                    debounceRequestDeduplicationId = "key",
                    debounceRequestPeriod = secondsDuration 1,
                    debounceRequestInputs = Just "{\"n\":2}",
                    debounceRequestSerialization = Nothing,
                    debounceRequestAppName = Nothing
                  }
          _ <- orFailSim =<< initWorkflow mem base Nothing WorkflowIdReturnExisting
          lost <- initWorkflow mem base Nothing WorkflowIdReturnExisting
          conflicted <- initWorkflow mem (base {newWorkflowDeduplicationId = Just "other"}) Nothing WorkflowIdReturnExisting
          _ <- orFailSim =<< debounceDelayedWorkflow mem bounce Nothing
          -- A re-init under another token loses the key again; what matters
          -- is the input it must not touch.
          _ <- initWorkflow mem base Nothing WorkflowIdReturnExisting
          kept <- orJustSim "the bounced row" =<< orFailSim =<< getWorkflow mem (WorkflowId "wf-b")
          pure (lost, conflicted, kept.workflowRecordInput)
        case lost of
          Left (QueueDeduplicated {queueName = "q", deduplicationId = "key"}) -> pure ()
          other -> fail ("expected the holder's QueueDeduplicated, got: " <> show other)
        case conflicted of
          Left (ConflictingWorkflow {}) -> pure ()
          other -> fail ("expected ConflictingWorkflow, got: " <> show other)
        kept @?= Just "{\"n\":2}",
      -- Twin of the live rejecting-insert arms: under Reject a row another
      -- caller created is refused whatever its status, writing nothing; a
      -- retry staging a lost acknowledgement under the same token finds
      -- its own row, and the default joins.
      testCase "a rejecting insert refuses another caller but not its own retry" $ do
        (created, retried, refused, joined, kept) <- runMem $ do
          mem <- newMemDB
          let new =
                (newWorkflow "wf-taken")
                  { newWorkflowName = Just "charge",
                    newWorkflowInput = Just "\"first\""
                  }
          created <- memInitWorkflowAs mem new Nothing WorkflowIdReject "creator"
          retried <- memInitWorkflowAs mem new Nothing WorkflowIdReject "creator"
          refused <- memInitWorkflowAs mem (new {newWorkflowInput = Just "\"second\""}) Nothing WorkflowIdReject "rejecter"
          joined <- memInitWorkflowAs mem new Nothing WorkflowIdReturnExisting "joiner"
          kept <- orJustSim "the taken row" =<< orFailSim =<< getWorkflow mem (WorkflowId "wf-taken")
          pure (created, retried, refused, joined, kept.workflowRecordInput)
        case created of
          Right result -> result.initResultShouldExecute @?= True
          Left err -> fail ("expected the insert to create, got: " <> show err)
        case retried of
          Right result -> result.initResultShouldExecute @?= True
          Left err -> fail ("expected the retry to recognise its own row, got: " <> show err)
        case refused of
          Left (WorkflowIdInUse {workflowId = "wf-taken", status = "PENDING", existingName = Just "charge"}) -> pure ()
          other -> fail ("expected WorkflowIdInUse, got: " <> show other)
        case joined of
          Right result -> result.initResultShouldExecute @?= False
          Left err -> fail ("expected the join, got: " <> show err)
        kept @?= Just "\"first\"",
      -- Twin of the live rewound-adoption arm: a parent's own child that
      -- no step records is adopted, and the adoption writes the parent's
      -- record — so a later start of the same id at another step is a
      -- reuse. Without the record the second start would adopt again. The
      -- record delete is what a rewind of the parent leaves behind.
      testCase "a rewound child adoption writes the parent's record" $ do
        (adopted, retried, racing) <- runMem $ do
          mem <- newMemDB
          parent <- orFailSim =<< memInitWorkflowAs mem (newWorkflow "wf-mom") Nothing WorkflowIdReturnExisting "mom"
          let at step =
                Just
                  ( InitWorkflowCaller
                      { initCallerParentWorkflowId = WorkflowId "wf-mom",
                        initCallerStepId = step,
                        initCallerStepName = "child",
                        initCallerStartedAt = Timestamp 0,
                        initCallerToken = parent.initResultCreatorXid
                      }
                  )
              child = (newWorkflow "wf-kid") {newWorkflowName = Just "child"}
          _ <- orFailSim =<< memInitWorkflowAs mem child (at 1) WorkflowIdReject "first"
          atomically $ modifyTVar mem.memSteps (Map.filterWithKey (\(wid, _) _ -> wid /= "wf-mom"))
          adopted <- memInitWorkflowAs mem child (at 1) WorkflowIdReject "replay"
          retried <- memInitWorkflowAs mem child (at 1) WorkflowIdReject "replay"
          racing <- memInitWorkflowAs mem child (at 3) WorkflowIdReject "racing"
          pure (adopted, retried, racing)
        case adopted of
          Right result -> result.initResultShouldExecute @?= False
          Left err -> fail ("expected the replayed parent to adopt, got: " <> show err)
        case retried of
          Right result -> result.initResultShouldExecute @?= False
          Left err -> fail ("expected a retried adoption to adopt again, got: " <> show err)
        case racing of
          Left (WorkflowIdInUse {}) -> pure ()
          other -> fail ("expected the other-step start to be a reuse, got: " <> show other),
      -- Twin of the live stale-parent case: the refused start creates no
      -- child row.
      testCase "a stale parent starts nothing" $ do
        (refused, missing) <- runMem $ do
          mem <- newMemDB
          _ <- orFailSim =<< initWorkflow mem (newWorkflow "wf-parent") Nothing WorkflowIdReturnExisting
          let caller =
                InitWorkflowCaller
                  { initCallerParentWorkflowId = WorkflowId "wf-parent",
                    initCallerStepId = 1,
                    initCallerStepName = "ChildWorkflow",
                    initCallerStartedAt = Timestamp 0,
                    initCallerToken = "someone-else"
                  }
          refused <- initWorkflow mem (newWorkflow "wf-child") (Just caller) WorkflowIdReturnExisting
          missing <- orFailSim =<< getWorkflow mem (WorkflowId "wf-child")
          pure (refused, missing)
        case refused of
          Left (WorkflowConflict {}) -> pure ()
          other -> fail ("expected WorkflowConflict, got: " <> show other)
        missing @?= Nothing,
      -- The dispatch times a claimed row's deadline from the database's
      -- clock, so the sim seam must answer with the simulated clock, not a
      -- canned constant: advancing virtual time advances it.
      testCase "databaseTime follows the simulated clock" $ do
        (first, later) <- runMem (simClockTwin =<< newMemDB)
        assertBool
          ("expected the later reading to be later, got " <> show (first, later))
          (later.databaseTimeEpochMs > first.databaseTimeEpochMs && later.databaseTimeReadAt > first.databaseTimeReadAt),
      testCase "a queue claim flips an enqueued row and honors worker concurrency" $ do
        (first, second, row) <- runMem $ do
          mem <- newMemDB
          _ <- upsertQueue mem (newQueue "q") {newQueueWorkerConcurrency = Just 1} UpdateExisting
          _ <- initWorkflow mem ((newWorkflow "wf-q") {newWorkflowQueueName = Just "q"}) Nothing WorkflowIdReturnExisting
          queue <- orJustSim "the stored queue" =<< orFailSim =<< getQueue mem "q"
          first <- orFailSim =<< startQueuedWorkflows mem queue "exec" "v1" Nothing 0 0 "tok-first"
          second <- orFailSim =<< startQueuedWorkflows mem queue "exec" "v1" Nothing 0 0 "tok-second"
          row <- orJustSim "the claimed row" =<< orFailSim =<< getWorkflow mem (WorkflowId "wf-q")
          pure (first.dequeuedWorkflowIds, second.dequeuedWorkflowIds, row)
        first @?= [WorkflowId "wf-q"]
        second @?= []
        row.workflowRecordStatus @?= Pending
        row.workflowRecordOwnerXid @?= Just "tok-first",
      testCase "the pending count sees only the queue's running rows" $ do
        counted <- runMem $ do
          mem <- newMemDB
          _ <- upsertQueue mem (newQueue "orders") UpdateExisting
          _ <- upsertQueue mem (newQueue "invoices") UpdateExisting
          empty <- orFailSim =<< countPendingQueueWorkflows mem "orders"
          _ <- initWorkflow mem ((newWorkflow "wf-a") {newWorkflowQueueName = Just "orders", newWorkflowQueuePartitionKey = Just "alpha"}) Nothing WorkflowIdReturnExisting
          _ <- initWorkflow mem ((newWorkflow "wf-b") {newWorkflowQueueName = Just "orders", newWorkflowQueuePartitionKey = Just "beta"}) Nothing WorkflowIdReturnExisting
          _ <- initWorkflow mem ((newWorkflow "wf-c") {newWorkflowQueueName = Just "invoices"}) Nothing WorkflowIdReturnExisting
          _ <- initWorkflow mem ((newWorkflow "wf-w") {newWorkflowQueueName = Just "orders", newWorkflowQueuePartitionKey = Just "gamma"}) Nothing WorkflowIdReturnExisting
          orders <- orJustSim "the stored queue" =<< orFailSim =<< getQueue mem "orders"
          _ <- orFailSim =<< startQueuedWorkflows mem orders "exec" "v1" (Just "alpha") 0 0 "tok-a"
          _ <- orFailSim =<< startQueuedWorkflows mem orders "exec" "v1" (Just "beta") 0 0 "tok-b"
          full <- orFailSim =<< countPendingQueueWorkflows mem "orders"
          pure (empty, full)
        counted @?= (0, 2),
      testCase "a partition claim names a spent queue-wide concurrency limit" $ do
        stopped <- runMem $ do
          mem <- newMemDB
          _ <- upsertQueue mem ((newQueue "orders") {newQueueConcurrency = Just 1, newQueuePartitionConcurrency = Just 1}) UpdateExisting
          _ <- initWorkflow mem ((newWorkflow "wf-running") {newWorkflowQueueName = Just "orders", newWorkflowQueuePartitionKey = Just "alpha"}) Nothing WorkflowIdReturnExisting
          _ <- initWorkflow mem ((newWorkflow "wf-waiting") {newWorkflowQueueName = Just "orders", newWorkflowQueuePartitionKey = Just "beta"}) Nothing WorkflowIdReturnExisting
          queue <- orJustSim "the stored queue" =<< orFailSim =<< getQueue mem "orders"
          _ <- orFailSim =<< startQueuedWorkflows mem queue "exec" "v1" Nothing 0 0 "tok-run"
          orFailSim =<< startQueuedWorkflows mem queue "exec" "v1" (Just "beta") 0 0 "tok-c"
        stopped @?= Dequeued [] (Just DequeueStopConcurrency)
        spansPartitions DequeueStopConcurrency @?= True,
      -- IO only: a spent queue-wide rate limit names RateLimit. Mem keeps
      -- no dequeue history, so the rate stop never fires here; the live
      -- tree carries that twin.
      testCase "a full partition is named as that partition's limit alone" $ do
        stopped <- runMem $ do
          mem <- newMemDB
          _ <- upsertQueue mem ((newQueue "orders") {newQueueConcurrency = Just 10, newQueuePartitionConcurrency = Just 1}) UpdateExisting
          _ <- initWorkflow mem ((newWorkflow "wf-running") {newWorkflowQueueName = Just "orders", newWorkflowQueuePartitionKey = Just "alpha"}) Nothing WorkflowIdReturnExisting
          _ <- initWorkflow mem ((newWorkflow "wf-waiting") {newWorkflowQueueName = Just "orders", newWorkflowQueuePartitionKey = Just "alpha"}) Nothing WorkflowIdReturnExisting
          queue <- orJustSim "the stored queue" =<< orFailSim =<< getQueue mem "orders"
          _ <- orFailSim =<< startQueuedWorkflows mem queue "exec" "v1" Nothing 0 0 "tok-run"
          orFailSim =<< startQueuedWorkflows mem queue "exec" "v1" (Just "alpha") 0 0 "tok-c"
        stopped @?= Dequeued [] (Just DequeueStopPartitionConcurrency)
        spansPartitions DequeueStopPartitionConcurrency @?= False,
      testCase "a full worker is named as the stop" $ do
        stopped <- runMem $ do
          mem <- newMemDB
          _ <- upsertQueue mem ((newQueue "orders") {newQueueWorkerConcurrency = Just 1, newQueuePartitionConcurrency = Just 1}) UpdateExisting
          _ <- initWorkflow mem ((newWorkflow "wf-waiting") {newWorkflowQueueName = Just "orders", newWorkflowQueuePartitionKey = Just "alpha"}) Nothing WorkflowIdReturnExisting
          queue <- orJustSim "the stored queue" =<< orFailSim =<< getQueue mem "orders"
          orFailSim =<< startQueuedWorkflows mem queue "exec" "v1" (Just "alpha") 1 0 "tok-c"
        stopped @?= Dequeued [] (Just DequeueStopWorkerConcurrency)
        spansPartitions DequeueStopWorkerConcurrency @?= True,
      testCase "a delayed row whose instant passed transitions to enqueued" $ do
        (before, moved, after) <- runMem $ do
          mem <- newMemDB
          _ <- initWorkflow mem ((newWorkflow "wf-d") {newWorkflowQueueName = Just "q", newWorkflowDelay = Just (millisDuration 1)}) Nothing WorkflowIdReturnExisting
          _ <- setWorkflowDelay mem (WorkflowId "wf-d") (DelayUntil (Timestamp 0)) Nothing
          before <- orJustSim "the delayed row" =<< orFailSim =<< getWorkflow mem (WorkflowId "wf-d")
          moved <- orFailSim =<< transitionDelayedWorkflows mem
          after <- orJustSim "the transitioned row" =<< orFailSim =<< getWorkflow mem (WorkflowId "wf-d")
          pure (before, moved, after)
        before.workflowRecordStatus @?= Delayed
        moved @?= 1
        after.workflowRecordStatus @?= Enqueued,
      testCase "the timeout sweep cancels expired rows and spares live ones" $ do
        (swept, ended, kept) <- runMem $ do
          mem <- newMemDB
          _ <- initWorkflow mem ((newWorkflow "wf-expired") {newWorkflowDeadline = Just (timestampFromEpochMs 0)}) Nothing WorkflowIdReturnExisting
          _ <- initWorkflow mem ((newWorkflow "wf-live") {newWorkflowDeadline = Just (timestampFromEpochMs 4102444800000)}) Nothing WorkflowIdReturnExisting
          swept <- orFailSim =<< cancelTimedOutWorkflows mem 100
          ended <- orJustSim "the swept row" =<< orFailSim =<< getWorkflow mem (WorkflowId "wf-expired")
          kept <- orJustSim "the spared row" =<< orFailSim =<< getWorkflow mem (WorkflowId "wf-live")
          pure (swept, ended, kept)
        assertBool "the expired row swept" (WorkflowId "wf-expired" `elem` swept)
        assertBool "the live row spared" (WorkflowId "wf-live" `notElem` swept)
        ended.workflowRecordStatus @?= Cancelled
        kept.workflowRecordStatus @?= Pending,
      testCase "the timed-out cancel honors ownership" $ do
        (refused, held, taken, ended) <- runMem $ do
          mem <- newMemDB
          _ <- initWorkflow mem ((newWorkflow "wf-owned") {newWorkflowDeadline = Just (timestampFromEpochMs 0)}) Nothing WorkflowIdReturnExisting
          refused <- orFailSim =<< cancelIfTimedOut mem (WorkflowId "wf-owned") "someone-else"
          held <- orJustSim "the spared row" =<< orFailSim =<< getWorkflow mem (WorkflowId "wf-owned")
          owner <- orJustSim "the owner" held.workflowRecordOwnerXid
          taken <- orFailSim =<< cancelIfTimedOut mem (WorkflowId "wf-owned") owner
          ended <- orJustSim "the cancelled row" =<< orFailSim =<< getWorkflow mem (WorkflowId "wf-owned")
          pure (refused, held, taken, ended)
        refused @?= False
        held.workflowRecordStatus @?= Pending
        taken @?= True
        ended.workflowRecordStatus @?= Cancelled,
      -- Twins of the live self-target cases: a cancel or resume naming the
      -- caller is refused before anything runs, and a cascade reaching the
      -- caller names the tree it arrived through.
      testCase "a workflow cannot cancel itself" $ do
        (refused, survived) <- runMem $ do
          mem <- newMemDB
          _ <- orFailSim =<< initWorkflow mem (newWorkflow "wf-self") Nothing WorkflowIdReturnExisting
          refused <- cancelWorkflows mem [WorkflowId "wf-self"] False Nothing (Just "wf-self")
          survived <- orFailSim =<< getWorkflow mem (WorkflowId "wf-self")
          pure (refused, survived)
        case refused of
          Left (InvalidInput {}) -> pure ()
          other -> fail ("expected InvalidInput, got: " <> show other)
        case survived of
          Just row -> row.workflowRecordStatus @?= Pending
          other -> fail ("expected the row to survive, got: " <> show other),
      testCase "a workflow cannot resume itself" $ do
        (refused, stayed) <- runMem $ do
          mem <- newMemDB
          _ <- orFailSim =<< initWorkflow mem (newWorkflow "wf-self") Nothing WorkflowIdReturnExisting
          _ <- orFailSim =<< cancelWorkflows mem [WorkflowId "wf-self"] False Nothing Nothing
          refused <- resumeWorkflows mem [WorkflowId "wf-self"] (Just "q") Nothing (Just "wf-self")
          stayed <- orFailSim =<< getWorkflow mem (WorkflowId "wf-self")
          pure (refused, stayed)
        case refused of
          Left (InvalidInput {}) -> pure ()
          other -> fail ("expected InvalidInput, got: " <> show other)
        case stayed of
          Just row -> row.workflowRecordStatus @?= Cancelled
          other -> fail ("expected the row to stay cancelled, got: " <> show other),
      testCase "a cancel cascade reaching the caller names the tree" $ do
        (refused, survived) <- runMem $ do
          mem <- newMemDB
          parentCreated <- orFailSim =<< initWorkflow mem (newWorkflow "wf-root") Nothing WorkflowIdReturnExisting
          let caller =
                InitWorkflowCaller
                  { initCallerParentWorkflowId = WorkflowId "wf-root",
                    initCallerStepId = 1,
                    initCallerStepName = "ChildWorkflow",
                    initCallerStartedAt = Timestamp 0,
                    initCallerToken = parentCreated.initResultCreatorXid
                  }
          _ <- orFailSim =<< initWorkflow mem (newWorkflow "wf-child") (Just caller) WorkflowIdReturnExisting
          refused <- cancelWorkflows mem [WorkflowId "wf-root"] True Nothing (Just "wf-child")
          survived <- orFailSim =<< getWorkflow mem (WorkflowId "wf-root")
          pure (refused, survived)
        case refused of
          Left (InvalidInput {detail}) -> detail @?= "workflow wf-child cannot cancel the tree of wf-root, which contains it"
          other -> fail ("expected InvalidInput, got: " <> show other)
        case survived of
          Just row -> row.workflowRecordStatus @?= Pending
          other -> fail ("expected the row to survive, got: " <> show other),
      -- Twin of the live stale-token cases: a write presenting another
      -- execution's token is refused (steps) or adopts (outcomes), and the
      -- row is untouched.
      testCase "a stale token's step write is refused and writes nothing" $ do
        (refused, steps) <- runMem $ do
          mem <- newMemDB
          _ <- orFailSim =<< initWorkflow mem (newWorkflow "wf-stale") Nothing WorkflowIdReturnExisting
          refused <- recordStep mem (WorkflowId "wf-stale") 1 "DBOS.step" (OutcomeOutput (Just "spy")) Nothing Nothing "someone-else"
          steps <- orFailSim =<< listSteps mem (WorkflowId "wf-stale") True Nothing Nothing Nothing
          pure (refused, steps)
        case refused of
          Left (WorkflowConflict {}) -> pure ()
          other -> fail ("expected WorkflowConflict, got: " <> show other)
        steps @?= [],
      testCase "a stale token's outcome write adopts instead" $ do
        (adopted, row) <- runMem $ do
          mem <- newMemDB
          _ <- orFailSim =<< initWorkflow mem (newWorkflow "wf-stale") Nothing WorkflowIdReturnExisting
          adopted <- orFailSim =<< recordWorkflowOutcome mem (WorkflowId "wf-stale") (OutcomeOutput (Just "spy")) "someone-else"
          row <- orJustSim "the spared row" =<< orFailSim =<< getWorkflow mem (WorkflowId "wf-stale")
          pure (adopted, row)
        adopted @?= AlreadyFinished
        row.workflowRecordStatus @?= Pending
        row.workflowRecordOutput @?= Nothing,
      -- Twin of the live stale-delete case: the refused deleter rolls back,
      -- so both rows survive.
      testCase "a stale token's delete is refused and removes nothing" $ do
        (refused, targetRow) <- runMem $ do
          mem <- newMemDB
          _ <- orFailSim =<< initWorkflow mem (newWorkflow "wf-target") Nothing WorkflowIdReturnExisting
          _ <- orFailSim =<< initWorkflow mem (newWorkflow "wf-caller") Nothing WorkflowIdReturnExisting
          refused <-
            deleteWorkflows
              mem
              [WorkflowId "wf-target"]
              False
              (Just (WorkflowOwner {ownerWorkflowId = WorkflowId "wf-caller", ownerToken = "someone-else"}))
          targetRow <- orFailSim =<< getWorkflow mem (WorkflowId "wf-target")
          pure (refused, targetRow)
        case refused of
          Left (WorkflowConflict {}) -> pure ()
          other -> fail ("expected WorkflowConflict, got: " <> show other)
        case targetRow of
          Just _ -> pure ()
          other -> fail ("expected the row to survive, got: " <> show other),
      -- Twin of the live dead-letter case: the mem mirror enforces the
      -- same guard (only PENDING rows at or above the floor move).
      testCase "dead-letter parks only over-limit pending rows" $ do
        (parked, overRow, withinRow) <- runMem $ do
          mem <- newMemDB
          _ <- initWorkflow mem (newWorkflow "wf-over") Nothing WorkflowIdReturnExisting
          _ <- initWorkflow mem (newWorkflow "wf-within") Nothing WorkflowIdReturnExisting
          _ <- initWorkflow mem (newWorkflow "wf-settled") Nothing WorkflowIdReturnExisting
          let attempts widText count = Map.adjust (\row -> row {workflowRecordRecoveryAttempts = count}) widText
          atomically $
            modifyTVar
              mem.memRows
              ( attempts "wf-over" 150
                  . attempts "wf-within" 3
                  . attempts "wf-settled" 150
                  . Map.adjust (\row -> row {workflowRecordStatus = Success}) "wf-settled"
              )
          parked <- orFailSim =<< deadLetterWorkflows mem (map WorkflowId ["wf-over", "wf-within", "wf-settled"]) 100
          overRow <- orJustSim "the parked row" =<< orFailSim =<< getWorkflow mem (WorkflowId "wf-over")
          withinRow <- orJustSim "the spared row" =<< orFailSim =<< getWorkflow mem (WorkflowId "wf-within")
          pure (parked, overRow, withinRow)
        parked @?= [WorkflowId "wf-over"]
        overRow.workflowRecordStatus @?= MaxRecoveryAttemptsExceeded
        withinRow.workflowRecordStatus @?= Pending,
      testCase "recovery re-enqueues the named executor's pending rows" $ do
        (recovered, row) <- runMem $ do
          mem <- newMemDB
          _ <- initWorkflow mem ((newWorkflow "wf-r") {newWorkflowExecutorId = Just "exec-1", newWorkflowAppVersion = Just "v1"}) Nothing WorkflowIdReturnExisting
          recovered <- orFailSim =<< reenqueueForRecovery mem ["exec-1"] "v1" "recovery-q"
          row <- orJustSim "the recovered row" =<< orFailSim =<< getWorkflow mem (WorkflowId "wf-r")
          pure (recovered, row)
        recovered @?= [WorkflowId "wf-r"]
        row.workflowRecordStatus @?= Enqueued
        row.workflowRecordQueueName @?= Just "recovery-q",
      testCase "a send wakes a parked recv" $ do
        result <- runMem $ do
          mem <- newMemDB
          received <- newEmptyMVar
          created <- orFailSim =<< initWorkflow mem (newWorkflow "wf-m") Nothing WorkflowIdReturnExisting
          _ <- forkIO (recv mem (WorkflowId "wf-m") 0 0 (Just "topic") (secondsDuration 10) created.initResultCreatorXid >>= putMVar received)
          threadDelay 1000
          _ <- sendMessage mem (message (WorkflowId "wf-m") (SerializedWorkflowValue "\"v\"" Nothing)) {sendTopic = Just (Topic "topic")} (Just "portable_json") Nothing Nothing False
          takeMVar received
        result @?= Right (Just (EncodedValue "\"v\"" (Just "portable_json"))),
      -- Twin of the rival-take case: the loser of the first message waits
      -- for the next instead of recording an empty take, so both receives
      -- take distinct messages and neither step records an absence.
      testCase "a recv whose message a rival took waits for the next" $ do
        (first, second) <- runMem $ do
          mem <- newMemDB
          firstTaken <- newEmptyMVar
          secondTaken <- newEmptyMVar
          created <- orFailSim =<< initWorkflow mem (newWorkflow "wf-m") Nothing WorkflowIdReturnExisting
          let token = created.initResultCreatorXid
              receive step box =
                recv mem (WorkflowId "wf-m") step step (Just "topic") (secondsDuration 10) token >>= putMVar box
              post body =
                orFailSim
                  =<< sendMessage
                    mem
                    (message (WorkflowId "wf-m") (SerializedWorkflowValue body Nothing)) {sendTopic = Just (Topic "topic")}
                    (Just "portable_json")
                    Nothing
                    Nothing
                    False
          _ <- forkIO (receive 0 firstTaken)
          _ <- forkIO (receive 1 secondTaken)
          threadDelay 1000
          _ <- post "\"one\""
          threadDelay 1000
          _ <- post "\"two\""
          (,) <$> takeMVar firstTaken <*> takeMVar secondTaken
        let bodies = [body | Right (Just value) <- [first, second], let EncodedValue body _ = value]
        assertBool
          ("expected one take of each message, got: " <> show bodies)
          (length bodies == 2 && "\"one\"" `elem` bodies && "\"two\"" `elem` bodies),
      -- Twin of the live stale-send case: the refused delivery rolls back,
      -- so the owner's send still delivers.
      testCase "a stale token's send is refused and delivers nothing" $ do
        (refused, quiet, received) <- runMem $ do
          mem <- newMemDB
          senderCreated <- orFailSim =<< initWorkflow mem (newWorkflow "wf-sender") Nothing WorkflowIdReturnExisting
          destinationCreated <- orFailSim =<< initWorkflow mem (newWorkflow "wf-destination") Nothing WorkflowIdReturnExisting
          let stale = Just (WorkflowOwner {ownerWorkflowId = WorkflowId "wf-sender", ownerToken = "someone-else"})
              owned = Just (WorkflowOwner {ownerWorkflowId = WorkflowId "wf-sender", ownerToken = senderCreated.initResultCreatorXid})
              envelope body = message (WorkflowId "wf-destination") (SerializedWorkflowValue body Nothing)
          refused <- sendMessage mem (envelope "\"spy\"") Nothing Nothing stale False
          quiet <- orFailSim =<< getAllNotifications mem (WorkflowId "wf-destination")
          _ <- sendMessage mem (envelope "\"yes\"") Nothing Nothing owned False
          received <- recv mem (WorkflowId "wf-destination") 0 0 Nothing (millisDuration 5) destinationCreated.initResultCreatorXid
          pure (refused, quiet, received)
        case refused of
          Left (WorkflowConflict {}) -> pure ()
          other -> fail ("expected WorkflowConflict, got: " <> show other)
        quiet @?= []
        received @?= Right (Just (EncodedValue "\"yes\"" Nothing)),
      testCase "a recv with nothing parked records the timeout as absence" $ do
        result <- runMem $ do
          mem <- newMemDB
          created <- orFailSim =<< initWorkflow mem (newWorkflow "wf-m") Nothing WorkflowIdReturnExisting
          recv mem (WorkflowId "wf-m") 0 0 (Just "quiet") (millisDuration 5) created.initResultCreatorXid
        result @?= Right Nothing,
      -- Twin of the live stale-take case: the refused consume rolls back,
      -- so the owner's take still finds the parked message.
      testCase "a stale token's recv take is refused and the message stays parked" $ do
        (refused, taken) <- runMem $ do
          mem <- newMemDB
          created <- orFailSim =<< initWorkflow mem (newWorkflow "wf-m") Nothing WorkflowIdReturnExisting
          _ <- sendMessage mem (message (WorkflowId "wf-m") (SerializedWorkflowValue "\"held\"" Nothing)) {sendTopic = Just (Topic "topic")} (Just "portable_json") Nothing Nothing False
          refused <- recv mem (WorkflowId "wf-m") 0 0 (Just "topic") (millisDuration 5) "someone-else"
          taken <- recv mem (WorkflowId "wf-m") 0 0 (Just "topic") (millisDuration 5) created.initResultCreatorXid
          pure (refused, taken)
        case refused of
          Left (WorkflowConflict {}) -> pure ()
          other -> fail ("expected WorkflowConflict, got: " <> show other)
        taken @?= Right (Just (EncodedValue "\"held\"" (Just "portable_json"))),
      testCase "a stale token's recv absence is refused and records nothing" $ do
        -- Twin of the live stale-absence case: a stale execution is
        -- refused with nothing recorded, so the owner's later take still
        -- finds the message. Pins the end-to-end invariant; the refusal
        -- below fires at the sleep checkpoint, while the record path's
        -- own check closes the narrower take-to-record hand-off window.
        (refused, taken) <- runMem $ do
          mem <- newMemDB
          created <- orFailSim =<< initWorkflow mem (newWorkflow "wf-s") Nothing WorkflowIdReturnExisting
          refused <- recv mem (WorkflowId "wf-s") 0 0 (Just "topic") (millisDuration 0) "someone-else"
          _ <- sendMessage mem (message (WorkflowId "wf-s") (SerializedWorkflowValue "\"late\"" Nothing)) {sendTopic = Just (Topic "topic")} (Just "portable_json") Nothing Nothing False
          taken <- recv mem (WorkflowId "wf-s") 0 0 (Just "topic") (secondsDuration 10) created.initResultCreatorXid
          pure (refused, taken)
        case refused of
          Left (WorkflowConflict {}) -> pure ()
          other -> fail ("expected WorkflowConflict, got: " <> show other)
        taken @?= Right (Just (EncodedValue "\"late\"" (Just "portable_json"))),
      testCase "a rewind clears the finished row's outcome with its steps" $ do
        (before, rewound, row, steps) <- runMem $ do
          mem <- newMemDB
          created <- orFailSim =<< initWorkflow mem (newWorkflow "wf-rw") Nothing WorkflowIdReturnExisting
          _ <- orFailSim =<< recordStep mem (WorkflowId "wf-rw") 0 "DBOS.step" (OutcomeOutput (Just "\"one\"")) Nothing Nothing created.initResultCreatorXid
          _ <- orFailSim =<< recordWorkflowOutcome mem (WorkflowId "wf-rw") (OutcomeOutput (Just "\"done\"")) created.initResultCreatorXid
          started <- orJustSim "the finished row" =<< orFailSim =<< getWorkflow mem (WorkflowId "wf-rw")
          done <- orFailSim =<< rewindWorkflow mem (WorkflowId "wf-rw") 0 (SysRewindOptions Nothing Nothing Nothing) Nothing
          row <- orJustSim "the rewound row" =<< orFailSim =<< getWorkflow mem (WorkflowId "wf-rw")
          steps <- orFailSim =<< listSteps mem (WorkflowId "wf-rw") True Nothing Nothing Nothing
          pure (started, done, row, steps)
        before.workflowRecordStatus @?= Success
        before.workflowRecordOutput @?= Just "\"done\""
        rewound @?= ()
        row.workflowRecordStatus @?= Enqueued
        row.workflowRecordOutput @?= Nothing
        row.workflowRecordError @?= Nothing
        steps @?= [],
      testCase "an invalid fork is refused before anything runs" $ do
        (badOptions, badFork, badStep, badSource) <- runMem $ do
          mem <- newMemDB
          badOptions <- forkWorkflows mem [forkNew "wf-f"] (defaultForkOptions {forkOptionsQueueName = Just ""}) Nothing
          badFork <- forkWorkflows mem [Fork "" Nothing 0] defaultForkOptions Nothing
          badStep <- forkFrom mem [WorkflowId "wf-f"] (ForkStep (-1)) defaultForkOptions Nothing
          badSource <- forkFrom mem [WorkflowId ""] (ForkStep 0) defaultForkOptions Nothing
          pure (badOptions, badFork, badStep, badSource)
        badOptions @?= Left (InvalidInput "queue_name" "must be absent rather than empty")
        badFork @?= Left (InvalidInput "original_id" "must not be empty")
        badStep @?= Left (InvalidInput "start_step" "must not be negative")
        badSource @?= Left (InvalidInput "original_id" "must not be empty"),
      testCase "an event published after a getEvent parks is delivered" $ do
        result <- runMem $ do
          mem <- newMemDB
          received <- newEmptyMVar
          created <- orFailSim =<< initWorkflow mem (newWorkflow "wf-e") Nothing WorkflowIdReturnExisting
          _ <- forkIO (getEvent mem (WorkflowId "wf-e") "key" (secondsDuration 10) Nothing >>= putMVar received)
          threadDelay 1000
          _ <- setEvent mem (WorkflowId "wf-e") 0 "key" "\"v\"" (Just "portable_json") created.initResultCreatorXid
          takeMVar received
        result @?= Right (Just (EncodedValue "\"v\"" (Just "portable_json"))),
      -- Twin of the live stale-publish case: the refused publish rolls
      -- back, so the owner's publish lands and reads back.
      testCase "a stale token's publish is refused and publishes nothing" $ do
        (refused, found) <- runMem $ do
          mem <- newMemDB
          created <- orFailSim =<< initWorkflow mem (newWorkflow "wf-e") Nothing WorkflowIdReturnExisting
          refused <- setEvent mem (WorkflowId "wf-e") 0 "key" "\"spy\"" Nothing "someone-else"
          _ <- setEvent mem (WorkflowId "wf-e") 0 "key" "\"v\"" (Just "portable_json") created.initResultCreatorXid
          found <- getEvent mem (WorkflowId "wf-e") "key" (millisDuration 5) Nothing
          pure (refused, found)
        case refused of
          Left (WorkflowConflict {}) -> pure ()
          other -> fail ("expected WorkflowConflict, got: " <> show other)
        found @?= Right (Just (EncodedValue "\"v\"" (Just "portable_json"))),
      -- Twin of the live stale-stream case: the refused append rolls back,
      -- so the owner's write still lands at offset zero.
      testCase "a stale token's stream write is refused and appends nothing" $ do
        (refused, entries) <- runMem $ do
          mem <- newMemDB
          created <- orFailSim =<< initWorkflow mem (newWorkflow "wf-s") Nothing WorkflowIdReturnExisting
          refused <- writeStream mem (WorkflowId "wf-s") 0 "progress" "spy" Nothing Workflow "someone-else"
          _ <- writeStream mem (WorkflowId "wf-s") 0 "progress" "one" Nothing Workflow created.initResultCreatorXid
          entries <- orFailSim =<< getAllStreamEntries mem (WorkflowId "wf-s")
          pure (refused, entries)
        case refused of
          Left (WorkflowConflict {}) -> pure ()
          other -> fail ("expected WorkflowConflict, got: " <> show other)
        case refused of
          Left (WorkflowConflict {}) -> pure ()
          other -> fail ("expected WorkflowConflict, got: " <> show other)
        length entries @?= 1,
      -- The fence opens the write before the replay adopt, as the oracle
      -- orders it: a stale execution replaying a recorded publish is
      -- refused, not handed the recorded answer.
      testCase "a stale token's replayed publish is refused" $ do
        (refused, found) <- runMem $ do
          mem <- newMemDB
          created <- orFailSim =<< initWorkflow mem (newWorkflow "wf-er") Nothing WorkflowIdReturnExisting
          _ <- setEvent mem (WorkflowId "wf-er") 0 "key" "\"v\"" (Just "portable_json") created.initResultCreatorXid
          refused <- setEvent mem (WorkflowId "wf-er") 0 "key" "\"v\"" (Just "portable_json") "someone-else"
          found <- getEvent mem (WorkflowId "wf-er") "key" (millisDuration 5) Nothing
          pure (refused, found)
        case refused of
          Left (WorkflowConflict {}) -> pure ()
          other -> fail ("expected WorkflowConflict, got: " <> show other)
        found @?= Right (Just (EncodedValue "\"v\"" (Just "portable_json"))),
      testCase "a stale token's replayed stream write is refused" $ do
        (refused, entries) <- runMem $ do
          mem <- newMemDB
          created <- orFailSim =<< initWorkflow mem (newWorkflow "wf-sr") Nothing WorkflowIdReturnExisting
          _ <- writeStream mem (WorkflowId "wf-sr") 0 "progress" "one" Nothing Workflow created.initResultCreatorXid
          refused <- writeStream mem (WorkflowId "wf-sr") 0 "progress" "one" Nothing Workflow "someone-else"
          entries <- orFailSim =<< getAllStreamEntries mem (WorkflowId "wf-sr")
          pure (refused, entries)
        case refused of
          Left (WorkflowConflict {}) -> pure ()
          other -> fail ("expected WorkflowConflict, got: " <> show other)
        length entries @?= 1,
      -- A missing row is refused by the fence, as the SQL owner check
      -- refuses what no select finds.
      testCase "a stream write to a missing workflow is a conflict" $ do
        result <- runMem $ do
          mem <- newMemDB
          writeStream mem (WorkflowId "wf-missing") 0 "progress" "one" Nothing Workflow "any-token"
        case result of
          Left (WorkflowConflict {}) -> pure ()
          other -> fail ("expected WorkflowConflict, got: " <> show other),
      testCase "a schedule round-trips through create, update, and delete" $ do
        (created, found, paused, gone) <- runMem $ do
          mem <- newMemDB
          created <- createSchedule mem (newSchedule "s" "wf" "* * * * *") Nothing
          _ <- updateSchedule mem "s" defaultScheduleUpdate {scheduleUpdateExpression = Set "0 * * * *"} Nothing
          found <- orFailSim =<< getSchedule mem "s" Nothing
          _ <- setScheduleStatus mem "s" Paused Nothing
          paused <- orFailSim =<< getSchedule mem "s" Nothing
          _ <- deleteSchedule mem "s" Nothing
          gone <- orFailSim =<< getSchedule mem "s" Nothing
          pure (created, found, paused, gone)
        created @?= Right ()
        case found of
          Just record -> record.scheduleRecordExpression @?= "0 * * * *"
          Nothing -> fail "the schedule was not stored"
        case paused of
          Just record -> record.scheduleRecordStatus @?= Paused
          Nothing -> fail "the paused schedule vanished"
        gone @?= Nothing,
      testCase "the latest version is the newest registered" $ do
        result <- runMem $ do
          mem <- newMemDB
          _ <- createApplicationVersion mem "v1" Nothing
          _ <- updateApplicationVersionTimestamp mem "v1" (Timestamp 0) Nothing
          _ <- createApplicationVersion mem "v2" Nothing
          orFailSim =<< getLatestApplicationVersion mem Nothing
        case result of
          Just version -> version.versionInfoName @?= "v2"
          Nothing -> fail "no version was reported",
      testCase "awaitFirstWorkflowId settles on a recorded outcome" $ do
        result <- runMem $ do
          mem <- newMemDB
          created <- orFailSim =<< initWorkflow mem (newWorkflow "wf-a") Nothing WorkflowIdReturnExisting
          _ <- recordWorkflowOutcome mem (WorkflowId "wf-a") (OutcomeOutput (Just "\"v\"")) created.initResultCreatorXid
          awaitFirstWorkflowId mem [WorkflowId "wf-a"] (secondsDuration 1)
        result @?= Right (WorkflowId "wf-a")
    ]

-- | Unwrap a backend answer inside the simulation, failing the case with
-- the error's rendering.
orFailSim :: Show e => Either e a -> IOSim s a
orFailSim = either (throwIO . userError . show) pure

-- | Unwrap an optional value inside the simulation, failing the case with
-- the given description when it is absent.
orJustSim :: String -> Maybe a -> IOSim s a
orJustSim what = maybe (throwIO (userError ("missing: " <> what))) pure
