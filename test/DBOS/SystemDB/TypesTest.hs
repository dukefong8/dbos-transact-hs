{-# LANGUAGE OverloadedStrings #-}

module DBOS.SystemDB.TypesTest
  ( tests,
  )
where

import DBOS.Prelude
import Data.Text qualified as Text
import Data.Time.Clock.System (SystemTime (..))
import DBOS.SystemDB (ApplicationRowCounts (..), Applications (..), Change (..), DebounceRequest (..), Error (..), Fork (..), ForkOptions (..), NewQueue (..), NewSchedule (..), NewWorkflow (..), Outcome (..), QueueRecord (..), QueueUpdate (..), RateLimit (..), RenameBatching (..), RenameFrom (..), ResolvedLimits (..), ScheduleFilter (..), ScheduleStatus (..), ScheduleUpdate (..), Submission (..), WorkflowDelay (..), WorkflowFilter (..), WorkflowRecord (..), addTimeout, applyQueueUpdate, cancelStepName, changeIsLeave, changeSet, claimsOwnership, closeStreamStepName, createScheduleStepName, debounceStepName, debounceValidate, defaultChange, defaultForkOptions, defaultQueueUpdate, defaultRenameBatchSize, defaultRenameBatching, defaultScheduleFilter, defaultScheduleUpdate, defaultWorkflowFilter, deleteScheduleStepName, deleteStepName, dequeueSweepCap, durationAsMillis, durationFromMs, durationFromSecs, durationSince, forkNew, forkOptionsValidate, forkValidate, forkStepName, getEventStepName, getResultStepName, getScheduleStepName, initialStatus, invalidInput, isQueueUpdateEmpty, isScheduleUpdateEmpty, isValidApplicationName, listSchedulesStepName, listStepsStepName, listWorkflowsStepName, newQueue, newSchedule, newWorkflow, outcomeStatus, parseScheduleStatus, pauseScheduleStepName, queueHasPartitionLimits, queueIsLegacyPartitioned, queueResolvedLimits, recvStepName, renameFromApplication, resolveWorkflowDelay, resolvedIsPartitioned, resumeScheduleStepName, resumeStepName, scheduleStatusText, secondsDuration, selectStepStepName, selectStepName, sendBulkStepName, sendStepName, setEventStepName, setWorkflowDelayStepName, sleepStepName, streamClosedSentinel, timestampFromEpochMs, timestampFromIso8601, timestampFromSystemTime, timestampNow, timestampToEpochMs, timestampToIso8601, timestampToSystemTime, updateScheduleStepName, updateWorkflowAttributesStepName, upsertScheduleStepName, validateNewWorkflow, writeStreamStepName, zeroRowCounts)
import DBOS.Transact
  (
  IdempotencyKey (..),
  SendMessage (..),
  Serialization (..),
  SerializedWorkflowValue (..),
  Topic (..),
  WorkflowId (..),
  WorkflowStatus (..),
  )
import DBOS.SystemDB.Types (MessageUUID (..), NotificationRow (..), notificationRowForMessage, nullTopicSentinel)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "SystemDB Types"
    [ timestampTests,
      durationTests,
      iso8601Tests,
      newWorkflowTests,
      workflowRecordTests,
      engineTests,
      queueTests,
      scheduleTests,
      filterTests,
      forkTests,
      debounceTests,
      outcomeTests,
      notificationTests
    ]

timestampTests :: TestTree
timestampTests =
  testGroup
    "Timestamp"
    [ testCase "wraps and unwraps stored epoch milliseconds" $
        timestampToEpochMs (timestampFromEpochMs 1700000000123)
          @?= 1700000000123,
      testCase "round-trips through SystemTime" $
        (timestampFromSystemTime =<< timestampToSystemTime (timestampFromEpochMs 1700000000123))
          @?= Just (timestampFromEpochMs 1700000000123),
      testCase "a pre-epoch instant does not convert to SystemTime" $
        timestampToSystemTime (timestampFromEpochMs (-1))
          @?= Nothing,
      testCase "an unstorable SystemTime is not a Timestamp" $ do
        timestampFromSystemTime (MkSystemTime (-1) 0)
          @?= Nothing
        timestampFromSystemTime (MkSystemTime (maxBound `div` 1000 + 1) 0)
          @?= Nothing,
      testCase "now never returns a pre-epoch instant" $ do
        now <- timestampNow
        assertBool "timestampToEpochMs now >= 0" (timestampToEpochMs now >= 0)
    ]

durationTests :: TestTree
durationTests =
  testGroup
    "Duration"
    [ testCase "reads a duration stored as integer milliseconds" $
        (durationAsMillis <$> durationFromMs 1500)
          @?= Just 1500,
      testCase "reads a duration stored as fractional seconds" $
        (durationAsMillis <$> durationFromSecs 1.5)
          @?= Just 1500,
      testCase "rejects values no valid duration column holds" $ do
        durationFromMs (-1) @?= Nothing
        (durationFromSecs <$> [-1.0, 0 / 0, 1 / 0, 1e300])
          @?= [Nothing, Nothing, Nothing, Nothing],
      testCase "both readers accept zero" $ do
        (durationAsMillis <$> durationFromMs 0) @?= Just 0
        (durationAsMillis <$> durationFromSecs 0.0) @?= Just 0
        (durationAsMillis <$> durationFromSecs (-0.0)) @?= Just 0,
      testCase "a deadline is a start plus a timeout" $ do
        timeout <- expectJust "durationFromSecs 30" (durationFromSecs 30)
        addTimeout (timestampFromEpochMs 1700000000000) timeout
          @?= Just (timestampFromEpochMs 1700000030000),
      testCase "how long after an earlier instant this instant is" $
        durationSince (timestampFromEpochMs 1700000030000) (timestampFromEpochMs 1700000000000)
          @?= durationFromSecs 30,
      testCase "going backwards is not a duration" $
        durationSince (timestampFromEpochMs 1700000000000) (timestampFromEpochMs 1700000030000)
          @?= Nothing
    ]

expectJust :: String -> Maybe a -> IO a
expectJust label = maybe (fail label) pure

iso8601Tests :: TestTree
iso8601Tests =
  testGroup
    "ISO-8601"
    [ testCase "an instant formats as ISO-8601" $
        (timestampToIso8601 . timestampFromEpochMs <$> [0, 1786492800000, 1786492800123, -1, -86400000])
          @?= [ "1970-01-01T00:00:00Z",
                "2026-08-12T00:00:00Z",
                "2026-08-12T00:00:00.123Z",
                "1969-12-31T23:59:59.999Z",
                "1969-12-31T00:00:00Z"
              ],
      testCase "an ISO-8601 instant parses from every implementation's format" $ do
        let expected = Just (timestampFromEpochMs 1786492800000)
        (timestampFromIso8601 <$> ["2026-08-12T00:00:00.000Z", "2026-08-12T00:00:00Z", "2026-08-12T00:00:00+00:00", "2026-08-12T00:00:00.000000000Z", "2026-08-12T01:30:00+01:30", "2026-08-11T19:00:00-05:00"])
          @?= [expected, expected, expected, expected, expected, expected]
        timestampFromIso8601 "2026-08-12T00:00:00.123+00:00"
          @?= Just (timestampFromEpochMs 1786492800123)
        (timestampToEpochMs <$> timestampFromIso8601 "2026-08-12T00:00:00.5Z")
          @?= Just 1786492800500
        (timestampToEpochMs <$> timestampFromIso8601 "2026-08-12T00:00:00.123999Z")
          @?= Just 1786492800123
        (timestampFromIso8601 <$> ["", "2026-08-12", "2026-08-12T00:00", "not-a-date-at-all", "2026-13-01T00:00:00Z", "2026-08-12T24:00:00Z", "2026-08-12T00:00:00", "2026-08-12T00:00:00+0130"])
          @?= [Nothing, Nothing, Nothing, Nothing, Nothing, Nothing, Nothing, Nothing],
      testCase "formatting and parsing are inverses" $
        (timestampFromIso8601 . timestampToIso8601 . timestampFromEpochMs <$> [0, -1, 1786492800000, 1709209845123, 951827445999, -2203977600000])
          @?= (Just . timestampFromEpochMs <$> [0, -1, 1786492800000, 1709209845123, 951827445999, -2203977600000])
    ]

newWorkflowTests :: TestTree
newWorkflowTests =
  testGroup
    "NewWorkflow"
    [ testCase "starts workflows without a queue as pending" $
        initialStatus (newWorkflow "wf")
          @?= Pending,
      testCase "starts queued workflows as enqueued" $
        initialStatus ((newWorkflow "wf") {newWorkflowQueueName = Just "q"})
          @?= Enqueued,
      testCase "starts delayed workflows on a queue as delayed" $ do
        delay <- expectJust "durationFromMs 1000" (durationFromMs 1000)
        initialStatus ((newWorkflow "wf") {newWorkflowQueueName = Just "q", newWorkflowDelay = Just delay})
          @?= Delayed,
      testCase "accepts a minimal workflow" $
        validateNewWorkflow (newWorkflow "wf")
          @?= Right (),
      testCase "rejects an empty workflow id" $
        validateNewWorkflow (newWorkflow "")
          @?= Left (invalidInput "workflow_id" "must not be empty"),
      testCase "rejects option fields that are empty rather than absent" $
        validateNewWorkflow ((newWorkflow "wf") {newWorkflowName = Just ""})
          @?= Left (invalidInput "name" "must be absent rather than empty"),
      testCase "rejects a zero delay and a zero timeout" $ do
        noWait <- expectJust "durationFromMs 0" (durationFromMs 0)
        validateNewWorkflow ((newWorkflow "wf") {newWorkflowDelay = Just noWait})
          @?= Left (invalidInput "delay" "must be a positive, non-zero duration")
        validateNewWorkflow ((newWorkflow "wf") {newWorkflowTimeout = Just noWait})
          @?= Left (invalidInput "timeout" "must be a positive, non-zero duration"),
      testCase "rejects attributes that are not a JSON object" $ do
        case validateNewWorkflow ((newWorkflow "wf") {newWorkflowAttributes = Just "[1, 2]"}) of
          Left (InvalidInput {field = "attributes"}) -> pure ()
          other -> fail ("expected InvalidInput attributes, got: " <> show other)
        validateNewWorkflow ((newWorkflow "wf") {newWorkflowAttributes = Just "{\"tenant\": \"acme\"}"})
          @?= Right ()
    ]

workflowRecordTests :: TestTree
workflowRecordTests =
  testGroup
    "WorkflowRecord"
    [ testCase "carries the stored columns" $
        let record =
              WorkflowRecord
                { workflowRecordId = WorkflowId "wf-1",
                  workflowRecordStatus = Enqueued,
                  workflowRecordName = Just "EnqueuedWorkflow",
                  workflowRecordClassName = Nothing,
                  workflowRecordConfigName = Nothing,
                  workflowRecordInput = Nothing,
                  workflowRecordOutput = Nothing,
                  workflowRecordError = Nothing,
                  workflowRecordSerialization = Just "json",
                  workflowRecordExecutorId = Nothing,
                  workflowRecordApplicationVersion = Just "v1",
                  workflowRecordRecoveryAttempts = 0,
                  workflowRecordQueueName = Just "demo-queue",
                  workflowRecordCreatedAt = timestampFromEpochMs 1700000000000,
                  workflowRecordUpdatedAt = timestampFromEpochMs 1700000001000,
                  workflowRecordStartedAt = Nothing,
                  workflowRecordCompletedAt = Nothing,
                  workflowRecordForkedFrom = Nothing,
                  workflowRecordParentWorkflowId = Nothing,
                  workflowRecordWasForkedFrom = False,
                  workflowRecordOwnerXid = Nothing,
                  workflowRecordApplicationId = Nothing,
                  workflowRecordAuthenticatedUser = Nothing,
                  workflowRecordAuthenticatedRoles = [],
                  workflowRecordAssumedRole = Nothing,
                  workflowRecordRequest = Nothing,
                  workflowRecordApplicationName = Nothing,
                  workflowRecordDeduplicationId = Nothing,
                  workflowRecordPriority = 0,
                  workflowRecordQueuePartitionKey = Nothing,
                  workflowRecordRateLimited = False,
                  workflowRecordScheduleName = Nothing,
                  workflowRecordTimeout = Nothing,
                  workflowRecordDeadline = Nothing,
                  workflowRecordDelayUntil = Nothing,
                  workflowRecordDebounceDeadline = Nothing,
                  workflowRecordIsDebounced = False,
                  workflowRecordAttributes = Nothing
                }
         in ( record.workflowRecordStatus,
              record.workflowRecordQueueName,
              record.workflowRecordPriority,
              record.workflowRecordCreatedAt
            )
              @?= (Enqueued, Just "demo-queue", 0, timestampFromEpochMs 1700000000000)
    ]

engineTests :: TestTree
engineTests =
  testGroup
    "Engine"
    [ testCase "pins cross-SDK step names" $
        [ sleepStepName,
          sendStepName,
          sendBulkStepName,
          writeStreamStepName,
          closeStreamStepName,
          setEventStepName,
          getEventStepName,
          getResultStepName,
          selectStepName,
          selectStepStepName,
          recvStepName,
          debounceStepName,
          cancelStepName,
          resumeStepName,
          deleteStepName,
          forkStepName,
          setWorkflowDelayStepName,
          updateWorkflowAttributesStepName,
          listWorkflowsStepName,
          listStepsStepName,
          createScheduleStepName,
          upsertScheduleStepName,
          getScheduleStepName,
          listSchedulesStepName,
          updateScheduleStepName,
          pauseScheduleStepName,
          resumeScheduleStepName,
          deleteScheduleStepName
        ]
          @?= [ "DBOS.sleep",
                "DBOS.send",
                "DBOS.sendBulk",
                "DBOS.writeStream",
                "DBOS.closeStream",
                "DBOS.setEvent",
                "DBOS.getEvent",
                "DBOS.getResult",
                "DBOS.selectWorkflow",
                "DBOS.selectStep",
                "DBOS.recv",
                "DBOS.debounceDelayedWorkflow",
                "DBOS.cancelWorkflow",
                "DBOS.resumeWorkflow",
                "DBOS.deleteWorkflow",
                "DBOS.forkWorkflow",
                "DBOS.setWorkflowDelay",
                "DBOS.updateWorkflowAttributes",
                "DBOS.listWorkflows",
                "DBOS.listWorkflowSteps",
                "DBOS.createSchedule",
                "DBOS.upsertSchedule",
                "DBOS.getSchedule",
                "DBOS.listSchedules",
                "DBOS.updateSchedule",
                "DBOS.pauseSchedule",
                "DBOS.resumeSchedule",
                "DBOS.deleteSchedule"
              ],
      testCase "pins the stream sentinel and sweep cap" $ do
        streamClosedSentinel @?= "__DBOS_STREAM_CLOSED__"
        dequeueSweepCap @?= 8192,
      testCase "a partial update leaves fields alone by default" $ do
        defaultChange @?= (Leave :: Change Int)
        changeSet (Set 3 :: Change Int) @?= Just 3
        changeSet (Leave :: Change Int) @?= Nothing
        changeIsLeave (Leave :: Change Int) @?= True
        changeIsLeave (Set 3 :: Change Int) @?= False,
      testCase "only recovery and dequeue claim ownership" $
        (claimsOwnership <$> [Fresh, Recovery, Dequeue])
          @?= [False, True, True],
      testCase "a relative delay resolves against now" $ do
        wait <- expectJust "durationFromMs 30000" (durationFromMs 30000)
        resolveWorkflowDelay (DelayFor wait) (timestampFromEpochMs 1700000000000)
          @?= Just (timestampFromEpochMs 1700000030000),
      testCase "an absolute delay is already resolved" $
        resolveWorkflowDelay (DelayUntil (timestampFromEpochMs 5)) (timestampFromEpochMs 1)
          @?= Just (timestampFromEpochMs 5),
      testCase "application names are short ASCII slugs" $ do
        (isValidApplicationName <$> ["api", "demo-queue", "a-b_c9"])
          @?= [True, True, True]
        (isValidApplicationName <$> ["", "ab", "ABC", "has space", "wörk", Text.replicate 257 "a"])
          @?= [False, False, False, False, False, False],
      testCase "a rename names its source application" $
        (renameFromApplication <$> [RenameApplication "a", RenameApplicationAndUnclaimed "a", RenameUnclaimed])
          @?= [Just "a", Just "a", Nothing],
      testCase "renames batch at ten thousand rows" $ do
        defaultRenameBatchSize @?= 10000
        defaultRenameBatching @?= Batched defaultRenameBatchSize,
      testCase "row counts start at zero" $
        zeroRowCounts
          @?= ApplicationRowCounts 0 0 0 0 0
    ]

queueTests :: TestTree
queueTests =
  testGroup
    "Queues"
    [ testCase "whole seconds are exact millisecond counts" $
        durationAsMillis (secondsDuration 30)
          @?= 30000,
      testCase "registers a queue with no limits polling once a second" $
        let queue = newQueue "q"
         in (queue.newQueueName, queue.newQueuePollingInterval, queue.newQueueConcurrency, queue.newQueuePriorityEnabled)
              @?= ("q", secondsDuration 1, Nothing, False),
      testCase "a legacy-partitioned row re-scopes its limits" $
        let legacy =
              QueueRecord
                { queueRecordName = "q",
                  queueRecordConcurrency = Just 3,
                  queueRecordWorkerConcurrency = Just 2,
                  queueRecordRateLimit = Just (RateLimit 10 (secondsDuration 60)),
                  queueRecordPriorityEnabled = False,
                  queueRecordPartitionQueue = True,
                  queueRecordPartitionConcurrency = Nothing,
                  queueRecordPartitionWorkerConcurrency = Nothing,
                  queueRecordPartitionRateLimit = Nothing,
                  queueRecordPollingInterval = secondsDuration 1,
                  queueRecordApplicationName = Nothing
                }
         in do
              queueHasPartitionLimits legacy @?= False
              queueIsLegacyPartitioned legacy @?= True
              queueResolvedLimits legacy
                @?= ResolvedLimits
                  { resolvedConcurrency = Nothing,
                    resolvedWorkerConcurrency = Nothing,
                    resolvedRateLimit = Nothing,
                    resolvedPartitionConcurrency = Just 3,
                    resolvedPartitionWorkerConcurrency = Just 2,
                    resolvedPartitionRateLimit = Just (RateLimit 10 (secondsDuration 60))
                  }
              resolvedIsPartitioned (queueResolvedLimits legacy) @?= True,
      testCase "a modern row keeps both scopes" $
        let modern =
              QueueRecord
                { queueRecordName = "q",
                  queueRecordConcurrency = Just 3,
                  queueRecordWorkerConcurrency = Nothing,
                  queueRecordRateLimit = Nothing,
                  queueRecordPriorityEnabled = True,
                  queueRecordPartitionQueue = False,
                  queueRecordPartitionConcurrency = Just 1,
                  queueRecordPartitionWorkerConcurrency = Nothing,
                  queueRecordPartitionRateLimit = Nothing,
                  queueRecordPollingInterval = secondsDuration 1,
                  queueRecordApplicationName = Nothing
                }
         in do
              queueHasPartitionLimits modern @?= True
              queueIsLegacyPartitioned modern @?= False
              queueResolvedLimits modern @?= ResolvedLimits (Just 3) Nothing Nothing (Just 1) Nothing Nothing,
      testCase "an empty update changes nothing" $ do
        isQueueUpdateEmpty defaultQueueUpdate @?= True
        isQueueUpdateEmpty (defaultQueueUpdate {queueUpdateConcurrency = Set Nothing}) @?= False,
      testCase "an update merges into the stored row" $
        let stored =
              QueueRecord
                { queueRecordName = "q",
                  queueRecordConcurrency = Just 3,
                  queueRecordWorkerConcurrency = Nothing,
                  queueRecordRateLimit = Nothing,
                  queueRecordPriorityEnabled = False,
                  queueRecordPartitionQueue = False,
                  queueRecordPartitionConcurrency = Nothing,
                  queueRecordPartitionWorkerConcurrency = Nothing,
                  queueRecordPartitionRateLimit = Nothing,
                  queueRecordPollingInterval = secondsDuration 1,
                  queueRecordApplicationName = Nothing
                }
            merged = applyQueueUpdate (defaultQueueUpdate {queueUpdateConcurrency = Set (Just 5)}) stored
         in (merged.queueRecordConcurrency, merged.queueRecordName)
              @?= (Just 5, "q"),
      testCase "the flag follows the limits the row will hold" $
        let stored =
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
                  queueRecordApplicationName = Nothing
                }
            moved = applyQueueUpdate (defaultQueueUpdate {queueUpdatePartitionConcurrency = Set (Just 1)}) stored
            pinned = applyQueueUpdate (defaultQueueUpdate {queueUpdatePartitionQueue = Set False, queueUpdatePartitionConcurrency = Set (Just 1)}) stored
         in do
              moved.queueRecordPartitionQueue @?= True
              pinned.queueRecordPartitionQueue @?= False
    ]

scheduleTests :: TestTree
scheduleTests =
  testGroup
    "Schedules"
    [ testCase "renders the stored status spellings" $
        (scheduleStatusText <$> [Active, Paused])
          @?= ["ACTIVE", "PAUSED"],
      testCase "rejects an unknown status spelling" $
        parseScheduleStatus "FIRING"
          @?= Nothing,
      testCase "registers an active schedule with a null context" $
        let schedule = newSchedule "s" "wf" "cron"
         in ( schedule.newScheduleName,
              schedule.newScheduleWorkflowName,
              schedule.newScheduleExpression,
              schedule.newScheduleStatus,
              schedule.newScheduleContext,
              schedule.newScheduleId,
              schedule.newScheduleAutomaticBackfill,
              schedule.newScheduleQueueName
            )
              @?= ("s", "wf", "cron", Active, "null", Nothing, False, Nothing),
      testCase "an empty schedule update changes nothing" $ do
        isScheduleUpdateEmpty defaultScheduleUpdate @?= True
        isScheduleUpdateEmpty (defaultScheduleUpdate {scheduleUpdateExpression = Set "cron"})
          @?= False
    ]

filterTests :: TestTree
filterTests =
  testGroup
    "Filters"
    [ testCase "an empty workflow filter narrows nothing but loads everything" $
        let narrowed = defaultWorkflowFilter
         in ( narrowed.workflowFilterWorkflowIds,
              narrowed.workflowFilterApplications,
              narrowed.workflowFilterLoadInput,
              narrowed.workflowFilterLoadOutput
            )
              @?= ([], Unset, True, True),
      testCase "an empty schedule filter narrows nothing" $
        defaultScheduleFilter.scheduleFilterStatuses
          @?= []
    ]

forkTests :: TestTree
forkTests =
  testGroup
    "Forks"
    [ testCase "a fork restarts from the beginning with a generated id" $
        let fork = forkNew "src"
         in (fork.forkSourceId, fork.forkForkedId, fork.forkStartStep)
              @?= ("src", Nothing, 0),
      testCase "rejects fork ids the schema cannot key on" $ do
        forkValidate (Fork "" Nothing 0)
          @?= Left (invalidInput "source_id" "must not be empty")
        forkValidate (Fork "s" (Just "") 0)
          @?= Left (invalidInput "forked_id" "must be absent rather than empty")
        forkValidate (Fork "s" Nothing (-1))
          @?= Left (invalidInput "start_step" "must not be negative")
        forkValidate (Fork "s" (Just "f") 2)
          @?= Right (),
      testCase "rejects fork options that would stamp or duplicate" $ do
        forkOptionsValidate (defaultForkOptions {forkOptionsApplicationVersion = Just ""})
          @?= Left (invalidInput "application_version" "must be absent rather than empty")
        noWait <- expectJust "durationFromMs 0" (durationFromMs 0)
        forkOptionsValidate (defaultForkOptions {forkOptionsTimeout = Just noWait})
          @?= Left (invalidInput "timeout" "must be absent rather than zero")
        forkOptionsValidate (defaultForkOptions {forkOptionsReplacementChildren = [("a", "b"), ("a", "c")]})
          @?= Left (invalidInput "replacement_children" "a is replaced more than once")
        forkOptionsValidate (defaultForkOptions {forkOptionsReplacementChildren = [("", "b")]})
          @?= Left (invalidInput "replacement_children" "a replaced child id must not be empty")
        forkOptionsValidate defaultForkOptions
          @?= Right ()
    ]

debounceTests :: TestTree
debounceTests =
  testGroup
    "Debounces"
    [ testCase "rejects bounces that could silently match nothing" $ do
        let request =
              DebounceRequest
                { debounceRequestWorkflowName = "wf",
                  debounceRequestClassName = Nothing,
                  debounceRequestConfigName = Nothing,
                  debounceRequestQueueName = "q",
                  debounceRequestDeduplicationId = "k",
                  debounceRequestDelayUntil = timestampFromEpochMs 1700000000000,
                  debounceRequestInputs = Nothing,
                  debounceRequestSerialization = Nothing,
                  debounceRequestApplicationName = Nothing
                }
        debounceValidate request
          @?= Right ()
        debounceValidate (request {debounceRequestWorkflowName = ""})
          @?= Left (invalidInput "workflow_name" "must not be empty")
        debounceValidate (request {debounceRequestClassName = Just ""})
          @?= Left (invalidInput "class_name" "must be absent rather than empty")
    ]

outcomeTests :: TestTree
outcomeTests =
  testGroup
    "Outcomes"
    [ testCase "an outcome settles the status" $ do
        outcomeStatus (OutcomeOutput Nothing) @?= Success
        outcomeStatus (OutcomeError "boom") @?= Error
    ]

-- | Python send mapping against the Rust @NULL_TOPIC@ contract
-- (@sysdb/mod.rs@, resolved @unwrap_or@ at the send call): an untopicked
-- message files under the sentinel, and both id branches scope per
-- recipient (@{key}::{destination}@, @postgres.rs@).
notificationTests :: TestTree
notificationTests =
  testGroup
    "Notifications"
    [ testCase "maps Python SendMessage without topic to notifications sentinel topic" $
        notificationRowForMessage
          (MessageUUID "generated-message-id")
          (SendMessage (WorkflowId "dest-wf") messageBody Nothing Nothing)
          @?= NotificationRow
            { notificationDestinationId = WorkflowId "dest-wf",
              notificationTopic = nullTopicSentinel,
              notificationMessage = messageBody,
              notificationMessageUUID = MessageUUID "generated-message-id::dest-wf",
              notificationConsumed = False
            },
      testCase "maps Python SendMessage topic into a notifications row" $
        notificationRowForMessage
          (MessageUUID "generated-message-id")
          (SendMessage (WorkflowId "dest-wf") messageBody (Just (Topic "testtopic")) Nothing)
          @?= NotificationRow
            { notificationDestinationId = WorkflowId "dest-wf",
              notificationTopic = "testtopic",
              notificationMessage = messageBody,
              notificationMessageUUID = MessageUUID "generated-message-id::dest-wf",
              notificationConsumed = False
            },
      testCase "scopes Python send idempotency keys by destination workflow" $
        ( notificationRowForMessage
            (MessageUUID "ignored-generated-id")
            ( SendMessage
                (WorkflowId "dest-wf")
                messageBody
                Nothing
                (Just (IdempotencyKey "idem-key"))
            )
          ).notificationMessageUUID
          @?= MessageUUID "idem-key::dest-wf",
      testCase "scopes generated fallback ids by destination workflow" $
        ( notificationRowForMessage
            (MessageUUID "generated-message-id")
            (SendMessage (WorkflowId "dest-wf") messageBody Nothing Nothing)
          ).notificationMessageUUID
          @?= MessageUUID "generated-message-id::dest-wf"
    ]

messageBody :: SerializedWorkflowValue
messageBody =
  SerializedWorkflowValue
    { serializedText = "\"hello\"",
      serializedSerialization = Just (Serialization "json")
    }
