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
import Data.Text (Text)
import DBOS.SystemDB
  ( Applications (..),
    AwaitedOutcome (..),
    Debounce (..),
    DebounceRequest (..),
    EncodedValue (..),
    EventRecord (..),
    Fork (..),
    ForkOptions (..),
    ForkPoint (..),
    NewSchedule (..),
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
    SendMessage (..),
    SerializedWorkflowValue (..),
    StepRecord (..),
    StreamRead (..),
    StreamRecord (..),
    Submission (..),
    SystemDB (..),
    Timestamp,
    VersionInfo (..),
    WorkflowDelay (..),
    WorkflowFilter (..),
    WorkflowId (..),
    WorkflowInitResult (..),
    WorkflowRecord (..),
    WorkflowStatus (..),
    WrittenBy (..),
    defaultForkOptions,
    defaultQueueUpdate,
    defaultScheduleFilter,
    defaultScheduleUpdate,
    defaultWorkflowFilter,
    forkNew,
    message,
    newQueue,
    newSchedule,
    newWorkflow,
    secondsDuration,
    timestampFromEpochMs,
    zeroRowCounts,
  )
import DBOS.SystemDB.IOSim (MockSystemDB (..))
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
      streamTests,
      versionTests,
      queueTests,
      scheduleTests,
      lifecycleTests
    ]

-- * Helpers

backend :: MockSystemDB
backend = MockSystemDB

run :: (forall s. IOSim s a) -> a
run = runSimOrThrow

at :: Timestamp
at = timestampFromEpochMs 1000

-- * Workflows

workflowTests :: TestTree
workflowTests =
  testGroup
    "Workflows"
    [ testCase "initWorkflow answers with an executable pending row" $ do
        let result = run (initWorkflow backend (newWorkflow "wf-1") Nothing Fresh Nothing)
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
        run (recordWorkflowOutcome backend (WorkflowId "wf-1") (OutcomeOutput (Just "\"mock\""))) @?= Right Recorded,
      testCase "setWorkflowDelay succeeds" $ do
        run (setWorkflowDelay backend (WorkflowId "wf-1") (DelayFor (secondsDuration 1)) Nothing) @?= Right (),
      testCase "clearQueueAssignment releases the claim" $ do
        run (clearQueueAssignment backend (WorkflowId "wf-1")) @?= Right True,
      testCase "updateWorkflowAttributes succeeds" $ do
        run (updateWorkflowAttributes backend (WorkflowId "wf-1") (Just "{}") Nothing) @?= Right (),
      testCase "reenqueueForRecovery returns nothing to recover" $ do
        run (reenqueueForRecovery backend ["exec-1"] "app" "0.0.0") @?= Right [],
      testCase "transitionDelayedWorkflows moves nothing" $ do
        run (transitionDelayedWorkflows backend) @?= Right 0,
      testCase "cancelWorkflows echoes the ids it cancelled" $ do
        run (cancelWorkflows backend [WorkflowId "wf-1"] False Nothing) @?= Right [WorkflowId "wf-1"],
      testCase "resumeWorkflows echoes the ids it resumed" $ do
        run (resumeWorkflows backend [WorkflowId "wf-1"] Nothing Nothing) @?= Right [WorkflowId "wf-1"],
      testCase "deleteWorkflows counts the ids it deleted" $ do
        run (deleteWorkflows backend [WorkflowId "wf-1", WorkflowId "wf-2"] False Nothing) @?= Right 2,
      testCase "renameApplication reports zero row counts" $ do
        run (renameApplication backend (RenameApplication "app") "target" Unbatched) @?= Right zeroRowCounts
    ]

-- * Waits

waitTests :: TestTree
waitTests =
  testGroup
    "Waits"
    [ testCase "awaitWorkflowResult answers a success" $ do
        run (awaitWorkflowResult backend (WorkflowId "wf-1") (secondsDuration 1) True) @?= Right (AwaitedSucceeded (Just "mock-output") (Just "rust_serde")),
      testCase "awaitFirstWorkflowId answers the first id" $ do
        run (awaitFirstWorkflowId backend [WorkflowId "wf-1", WorkflowId "wf-2"] (secondsDuration 1)) @?= Right (WorkflowId "wf-1"),
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
        run (recordStep backend (WorkflowId "wf-1") 0 "step" (OutcomeOutput (Just "\"v\"")) Nothing Nothing) @?= Right (),
      testCase "listWorkflowSteps answers the workflow's steps" $ do
        let result = run (listWorkflowSteps backend (WorkflowId "wf-1") False Nothing Nothing Nothing)
        case result of
          Right steps -> map (.stepRecordStepName) steps @?= ["mock-step"]
          Left err -> fail (show err),
      testCase "recordSleep answers its wake time" $ do
        run (recordSleep backend (WorkflowId "wf-1") 0 (secondsDuration 1)) @?= Right at,
      testCase "checkChildResult uses the provided default" $ do
        run (checkChildResult backend (WorkflowId "parent") 0) @?= Right Nothing,
      testCase "recordChildWorkflow succeeds" $ do
        run (recordChildWorkflow backend (WorkflowId "parent") (WorkflowId "child") 0 "child-start" Nothing) @?= Right (),
      testCase "recordChildResult succeeds" $ do
        run (recordChildResult backend (WorkflowId "parent") 0 (WorkflowId "child") (OutcomeOutput (Just "\"v\"")) Nothing Nothing) @?= Right ()
    ]

-- * Events, notifications, messages

eventMessageTests :: TestTree
eventMessageTests =
  testGroup
    "Events and messages"
    [ testCase "setEvent succeeds" $ do
        run (setEvent backend (WorkflowId "wf-1") 0 "key" "\"v\"" Nothing) @?= Right (),
      testCase "getEvent answers a value" $ do
        run (getEvent backend (WorkflowId "wf-1") "key" (secondsDuration 1) Nothing) @?= Right (Just (EncodedValue "mock-event" (Just "rust_serde"))),
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
        run (sendMessage backend (message (WorkflowId "wf-1") (SerializedWorkflowValue "\"v\"" Nothing)) Nothing Nothing False) @?= Right (),
      testCase "sendMessages succeeds" $ do
        run (sendMessages backend [message (WorkflowId "wf-1") (SerializedWorkflowValue "\"v\"" Nothing)] Nothing Nothing False) @?= Right (),
      testCase "recv answers a message" $ do
        run (recv backend (WorkflowId "wf-1") 0 0 Nothing (secondsDuration 1)) @?= Right (Just (EncodedValue "mock-message" (Just "rust_serde")))
    ]

-- * Streams

streamTests :: TestTree
streamTests =
  testGroup
    "Streams"
    [ testCase "writeStream succeeds" $ do
        run (writeStream backend (WorkflowId "wf-1") 0 "key" "\"v\"" Nothing Workflow) @?= Right (),
      testCase "closeStream succeeds" $ do
        run (closeStream backend (WorkflowId "wf-1") 0 "key") @?= Right (),
      testCase "readStreamValue answers a value and the producer's status" $ do
        run (readStreamValue backend (WorkflowId "wf-1") "key" 0) @?= Right (StreamRead Pending (Just (EncodedValue "mock-stream" (Just "rust_serde")))),
      testCase "getAllStreamEntries answers the entries" $ do
        let result = run (getAllStreamEntries backend (WorkflowId "wf-1"))
        case result of
          Right entries -> map (.streamKey) entries @?= ["mock-key"]
          Left err -> fail (show err)
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
        run (startQueuedWorkflows backend mockQueueRecord "exec" "0.0.0" Nothing 0 0) @?= Right [WorkflowId "mock-queued"],
      testCase "getQueuePartitions answers a partition" $ do
        run (getQueuePartitions backend "q") @?= Right ["mock-partition"],
      testCase "startQueuedPartitionedWorkflows answers a claimed id" $ do
        run (startQueuedPartitionedWorkflows backend mockQueueRecord "exec" "0.0.0" Nothing) @?= Right [WorkflowId "mock-partitioned"],
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
          queueRecordApplicationName = Just "app"
        }
    debounceRequest =
      DebounceRequest
        { debounceRequestWorkflowName = "wf",
          debounceRequestClassName = Nothing,
          debounceRequestConfigName = Nothing,
          debounceRequestQueueName = "q",
          debounceRequestDeduplicationId = "key",
          debounceRequestDelayUntil = at,
          debounceRequestInputs = Nothing,
          debounceRequestSerialization = Nothing,
          debounceRequestApplicationName = Nothing
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
