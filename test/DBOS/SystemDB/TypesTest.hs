{-# LANGUAGE OverloadedStrings #-}

module DBOS.SystemDB.TypesTest
  ( tests,
  )
where

import DBOS.Internal.Prelude
import Data.Aeson qualified as Aeson
import Data.Int (Int64)
import Data.Text qualified as Text
import Data.Time.Clock.System (SystemTime (..))
import DBOS.SystemDB (ApplicationRowCounts (..), AppScope (..), Change (..), DebounceRequest (..), Error (..), Fork (..), ForkCallRefusal (..), ForkOptions (..), IdleTransactionTimeout (..), NewQueue (..), NewSchedule (..), NewWorkflow (..), Outcome (..), QualifiedWorkflowName, QueueRecord (..), QueueUpdate (..), RateLimit (..), RenameBatching (..), RenameFrom (..), ScheduleFilter (..), ScheduleStatus (..), ScheduleUpdate (..), WorkflowDelay (..), WorkflowFilter (..), WorkflowOwner (..), DatabaseTime (..), StepRecord (..), WorkflowRecord (..), addTimeout, applyQueueUpdate, cancelStepName, changeIsLeave, changeSet, closeStreamStepName, createScheduleStepName, debounceStepName, debounceValidate, defaultChange, defaultForkOptions, defaultQueueUpdate, defaultRenameBatchSize, defaultRenameBatching, defaultScheduleFilter, defaultScheduleUpdate, defaultWorkflowFilter, Duration, databaseTimeNow, databaseTimeReaches, deleteScheduleStepName, deleteStepName, dequeueSweepCap, durationAsMillis, durationFromMs, durationFromSecs, durationIsZero, durationSince, durationToMs, forkNew, forkOptionsValidate, forkValidate, forkStepName, getEventStepName, getResultStepName, getScheduleStepName, idleTimeoutDefault, idleTimeoutMax, idleTimeoutMillis, idleTimeoutOptionNames, idleTimeoutValidate, initialStatus, instanceQualifiedWorkflowName, invalidInput, isQueueUpdateEmpty, isScheduleUpdateEmpty, isStreamClosed, isValidApplicationName, listSchedulesStepName, listStepsStepName, listWorkflowsStepName, methodQualifiedWorkflowName, millisDuration, newQueue, newSchedule, newWorkflow, newQualifiedWorkflowName, outcomeStatus, parseApplicationName, parseScheduleStatus, pauseScheduleStepName, patchMarkerPrefix, portableJsonSerialization, qualifiedClassName, qualifiedConfigName, qualifiedName, qualifiedWorkflowNameFromRow, queueHasPartitionLimits, queueIsLegacyPartitioned, readStreamStepName, recvStepName, refuseInvalidForks, renameFromApplication, renderApplicationName, renderQualifiedWorkflowName, resumeScheduleStepName, resumeStepName, rewindStepName, saturateTimeout, scaleDuration, scheduleStatusText, secondsDuration, selectStepStepName, selectStepName, sendBulkStepName, sendStepName, setEventStepName, setWorkflowDelayStepName, sleepStepName, streamClosedSentinel, timestampFromEpochMs, timestampFromIso8601, timestampFromSystemTime, timestampNow, timestampToEpochMs, timestampToIso8601, timestampToSystemTime, updateScheduleStepName, updateWorkflowAttributesStepName, upsertScheduleStepName, urlSetsIdleTimeout, validateNewWorkflow, writeStreamStepName, zeroRowCounts)
import DBOS.Transact
  (
  IdempotencyKey (..),
  EncodedSendMessage (..),
  Serialization (..),
  SerializedWorkflowValue (..),
  Topic (..),
  WorkflowId (..),
  WorkflowStatus (..),
  )
import DBOS.Internal.SystemDB.Types (MessageUUID (..), NotificationRow (..), notificationRowForMessage, nullTopicSentinel)
import DBOS.Internal.Transact.Debouncer (debounceDedupKey)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "SystemDB Types"
    [ timestampTests,
      durationTests,
      databaseTimeTests,
      iso8601Tests,
      newWorkflowTests,
      qualifiedNameTests,
      workflowRecordTests,
      engineTests,
      queueTests,
      scheduleTests,
      filterTests,
      forkTests,
      debounceTests,
      outcomeTests,
      notificationTests,
      ownerTests,
      idleTimeoutTests
    ]

ownerTests :: TestTree
ownerTests =
  testGroup
    "Owner"
    [ testCase "an owner carries its row and token" $
        let owner = WorkflowOwner {ownerWorkflowId = WorkflowId "wf", ownerToken = "tok"}
         in (owner.ownerWorkflowId, owner.ownerToken) @?= (WorkflowId "wf", "tok")
    ]

-- | A workflow's qualified name holds one spelling of absence, displays
-- each part in its place, and reads a stored spelling back. Mirrors Rust
-- @a_name_holds_one_spelling_of_absence@,
-- @a_name_displays_each_part_in_its_place@ and
-- @a_deserialized_name_holds_one_spelling_of_absence@.
qualifiedNameTests :: TestTree
qualifiedNameTests =
  testGroup
    "QualifiedName"
    [ testCase "every way of building a name holds one spelling of absence" $ do
        let free = newQualifiedWorkflowName "f"
        methodQualifiedWorkflowName "f" "" @?= free
        instanceQualifiedWorkflowName "f" "" "" @?= free
        qualifiedWorkflowNameFromRow "f" (Just "") (Just "") @?= free
        instanceQualifiedWorkflowName "f" "C" "" @?= methodQualifiedWorkflowName "f" "C"
        let unclassed = instanceQualifiedWorkflowName "f" "" "eu"
        unclassed @?= qualifiedWorkflowNameFromRow "f" Nothing (Just "eu")
        qualifiedClassName unclassed @?= Nothing
        qualifiedConfigName unclassed @?= Just "eu",
      testCase "a name displays each part in its place" $ do
        renderQualifiedWorkflowName (newQualifiedWorkflowName "f") @?= "f"
        renderQualifiedWorkflowName (methodQualifiedWorkflowName "f" "C") @?= "f/C"
        renderQualifiedWorkflowName (instanceQualifiedWorkflowName "f" "C" "eu") @?= "f/C/eu"
        renderQualifiedWorkflowName (instanceQualifiedWorkflowName "f" "" "eu") @?= "f//eu",
      testCase "a deserialized name holds one spelling of absence" $ do
        (Aeson.fromJSON (Aeson.object ["name" Aeson..= ("f" :: Text), "class_name" Aeson..= ("" :: Text), "config_name" Aeson..= (Nothing :: Maybe Text)]) :: Aeson.Result QualifiedWorkflowName)
          @?= Aeson.Success (newQualifiedWorkflowName "f")
        (Aeson.fromJSON (Aeson.object ["name" Aeson..= ("f" :: Text)]) :: Aeson.Result QualifiedWorkflowName)
          @?= Aeson.Success (newQualifiedWorkflowName "f")
        (Aeson.fromJSON (Aeson.toJSON (instanceQualifiedWorkflowName "f" "C" "eu")) :: Aeson.Result QualifiedWorkflowName)
          @?= Aeson.Success (instanceQualifiedWorkflowName "f" "C" "eu")
    ]

idleTimeoutTests :: TestTree
idleTimeoutTests =
  testGroup
    "Idle transaction timeout"
    [ testCase "the default is sixty seconds and the max is i32 milliseconds" $ do
        durationAsMillis idleTimeoutDefault @?= 60000
        durationAsMillis idleTimeoutMax @?= 2147483647,
      testCase "millis resolves each variant" $ do
        idleTimeoutMillis Default @?= Just 60000
        idleTimeoutMillis (After (millisDuration 300)) @?= Just 300
        idleTimeoutMillis ServerSetting @?= Nothing
        case durationFromSecs 0.0005 of
          Nothing -> fail "expected a sub-millisecond duration"
          Just subMs -> idleTimeoutMillis (After subMs) @?= Just 1,
      testCase "validation refuses only what the server cannot take" $ do
        idleTimeoutValidate Default @?= Right ()
        idleTimeoutValidate ServerSetting @?= Right ()
        idleTimeoutValidate (After (millisDuration 300)) @?= Right ()
        case idleTimeoutValidate (After (millisDuration 0)) of
          Left (InvalidInput {}) -> pure ()
          other -> fail ("expected a zero After to be refused, got: " <> show other)
        case idleTimeoutValidate (After (secondsDuration 3000000)) of
          Left (InvalidInput {}) -> pure ()
          other -> fail ("expected an over-max After to be refused, got: " <> show other),
      testCase "option names read as the server reads them" $ do
        idleTimeoutOptionNames "-c a=1 -cB=2 --c-d=3 -c e\\ f=4" @?= ["a", "b", "c_d", "e f"]
        idleTimeoutOptionNames "-c application_name=idle_in_transaction_session_timeout" @?= ["application_name"]
        idleTimeoutOptionNames "-B 100 stray -c" @?= [],
      testCase "a URL sets the timeout only through its options" $ do
        urlSetsIdleTimeout "postgresql://h/db?options=-c%20idle_in_transaction_session_timeout%3D1234" Nothing @?= True
        urlSetsIdleTimeout "postgresql://h/db?options=-c%20IDLE%5FIN%5FTRANSACTION%5FSESSION%5FTIMEOUT%3D1234" Nothing @?= True
        urlSetsIdleTimeout "postgresql://h/db?options=--idle-in-transaction-session-timeout%3D1234" Nothing @?= True
        urlSetsIdleTimeout "postgresql://h/db?idle_in_transaction_session_timeout=1234" Nothing @?= False
        urlSetsIdleTimeout "postgresql://h/db?options=-c%20application_name%3Didle_in_transaction_session_timeout" Nothing @?= False
        urlSetsIdleTimeout "postgresql://h/db" Nothing @?= False
        urlSetsIdleTimeout "postgresql://h/db" (Just "-c idle_in_transaction_session_timeout=5") @?= True
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
      testCase "zero is exact, not the millisecond floor" $ do
        case durationFromSecs 0.0005 of
          Nothing -> fail "expected a positive sub-millisecond duration"
          Just subMs -> do
            durationIsZero subMs @?= False
            durationAsMillis subMs @?= 0
        durationIsZero (secondsDuration 0) @?= True,
      testCase "a negative duration is refused at every constructor" $ do
        -- Opaque by type: a negative span cannot be built, so it cannot
        -- reach a storage or deadline boundary either.
        case (Aeson.fromJSON (Aeson.Number (-5)) :: Aeson.Result Duration) of
          Aeson.Error _ -> pure ()
          other -> fail ("expected the JSON duration to be refused, got: " <> show other)
        durationFromMs (-5) @?= Nothing
        durationFromSecs (-0.005) @?= Nothing,
      testCase "scaling a duration refuses a negative or non-finite factor" $ do
        scaleDuration 2 (secondsDuration 1) @?= Just (secondsDuration 2)
        scaleDuration (-1) (secondsDuration 1) @?= Nothing
        scaleDuration (0 / 0) (secondsDuration 1) @?= Nothing
        scaleDuration (1 / 0) (secondsDuration 1) @?= Nothing,
      testCase "a deadline is a start plus a timeout" $ do
        timeout <- expectJust "durationFromSecs 30" (durationFromSecs 30)
        addTimeout (timestampFromEpochMs 1700000000000) timeout
          @?= Just (timestampFromEpochMs 1700000030000),
      testCase "how long after an earlier instant this instant is" $
        durationSince (timestampFromEpochMs 1700000030000) (timestampFromEpochMs 1700000000000)
          @?= durationFromSecs 30,
      testCase "going backwards is not a duration" $
        durationSince (timestampFromEpochMs 1700000000000) (timestampFromEpochMs 1700000030000)
          @?= Nothing,
      testCase "a storable duration converts to milliseconds" $ do
        wait <- expectJust "durationFromSecs 30" (durationFromSecs 30)
        durationToMs "timeout" wait @?= Right 30000,
      testCase "an unstorable duration names its field" $ do
        -- Mirrors Rust duration_to_ms: milliseconds past Int64 refuse,
        -- however the value was built.
        huge <- expectJust "durationFromSecs 1e16" (durationFromSecs 1e16)
        durationToMs "delay" huge
          @?= Left (invalidInput "delay" "must fit in milliseconds as a 64-bit integer"),
      testCase "a delay resolves against now and saturates at the end of time" $ do
        wait <- expectJust "durationFromSecs 30" (durationFromSecs 30)
        saturateTimeout (timestampFromEpochMs 1700000000000) wait
          @?= timestampFromEpochMs 1700000030000
        huge <- expectJust "durationFromSecs 1e16" (durationFromSecs 1e16)
        saturateTimeout (timestampFromEpochMs 1700000000000) huge
          @?= timestampFromEpochMs (maxBound :: Int64)
    ]

databaseTimeTests :: TestTree
databaseTimeTests =
  testGroup
    "DatabaseTime"
    [ testCase "a reading behind the instant waits the difference" $ do
        let clock = DatabaseTime {databaseTimeEpochMs = 1000, databaseTimeReadAt = 50}
        databaseTimeReaches clock (timestampFromEpochMs 2000) @?= 50 + 1000 * 1000000,
      testCase "an instant behind the reading waits at once" $ do
        let clock = DatabaseTime {databaseTimeEpochMs = 2000, databaseTimeReadAt = 50}
        databaseTimeReaches clock (timestampFromEpochMs 1000) @?= 50,
      testCase "extreme skew saturates instead of wrapping" $ do
        let low = DatabaseTime {databaseTimeEpochMs = maxBound, databaseTimeReadAt = maxBound}
        databaseTimeReaches low (timestampFromEpochMs minBound) @?= maxBound
        let high = DatabaseTime {databaseTimeEpochMs = minBound, databaseTimeReadAt = 0}
        databaseTimeReaches high (timestampFromEpochMs maxBound) @?= maxBound,
      testCase "now never wraps when the monotonic clock is behind the reading" $ do
        now <- databaseTimeNow (DatabaseTime {databaseTimeEpochMs = 1000, databaseTimeReadAt = maxBound})
        timestampToEpochMs now @?= 1000
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
      testCase "reads the RFC3339 spellings the oracle accepts" $ do
        let expected = Just (timestampFromEpochMs 1786492800000)
        (timestampFromIso8601 <$> ["2026-08-12t00:00:00Z", "2026-08-12T00:00:00z", "2026-08-12 00:00:00Z", "2026-08-12X00:00:00Z"])
          @?= replicate 4 expected
        timestampFromIso8601 "2026-08-12T00:00:00+01" @?= Nothing
        timestampFromIso8601 "2026-08-12T00:00:00+0130" @?= Nothing
        timestampFromIso8601 "2026-08-12T00:00:00" @?= Nothing,
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
      testCase "rejects an empty workflow name" $
        validateNewWorkflow ((newWorkflow "wf") {newWorkflowName = Just ""})
          @?= Left (invalidInput "name" "must not be empty"),
      testCase "rejects option fields that are empty rather than absent" $ do
        validateNewWorkflow ((newWorkflow "wf") {newWorkflowClassName = Just ""})
          @?= Left (invalidInput "class_name" "must be absent rather than empty")
        validateNewWorkflow ((newWorkflow "wf") {newWorkflowConfigName = Just ""})
          @?= Left (invalidInput "config_name" "must be absent rather than empty"),
      testCase "rejects a zero delay and a zero timeout" $ do
        noWait <- expectJust "durationFromMs 0" (durationFromMs 0)
        validateNewWorkflow ((newWorkflow "wf") {newWorkflowDelay = Just noWait})
          @?= Left (invalidInput "delay" "must be a positive, non-zero duration")
        validateNewWorkflow ((newWorkflow "wf") {newWorkflowTimeout = Just noWait})
          @?= Left (invalidInput "timeout" "must be a positive, non-zero duration")
        validateNewWorkflow ((newWorkflow "wf") {newWorkflowDebounceTimeout = Just noWait})
          @?= Left (invalidInput "debounce_timeout" "must be a positive, non-zero duration"),
      testCase "rejects a delay and a timeout too long to store" $ do
        -- Mirrors Rust an_oversized_workflow_duration_does_not_validate:
        -- 1e16 s constructs (below Word64) but its milliseconds exceed
        -- Int64. Debounce_timeout rides the same loop, last.
        huge <- expectJust "durationFromSecs 1e16" (durationFromSecs 1e16)
        validateNewWorkflow ((newWorkflow "wf") {newWorkflowDelay = Just huge})
          @?= Left (invalidInput "delay" "must fit in milliseconds as a 64-bit integer")
        validateNewWorkflow ((newWorkflow "wf") {newWorkflowTimeout = Just huge})
          @?= Left (invalidInput "timeout" "must fit in milliseconds as a 64-bit integer")
        validateNewWorkflow ((newWorkflow "wf") {newWorkflowDebounceTimeout = Just huge})
          @?= Left (invalidInput "debounce_timeout" "must fit in milliseconds as a 64-bit integer"),
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
                  workflowRecordAppVersion = Just "v1",
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
                  workflowRecordAppId = Nothing,
                  workflowRecordAuthenticatedUser = Nothing,
                  workflowRecordAuthenticatedRoles = [],
                  workflowRecordAssumedRole = Nothing,
                  workflowRecordRequest = Nothing,
                  workflowRecordAppName = Nothing,
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
              @?= (Enqueued, Just "demo-queue", 0, timestampFromEpochMs 1700000000000),
      testCase "a checkpoint written before a field existed still decodes" $ do
        let encoded =
              Aeson.encode
                ( Aeson.object
                    [ "workflowRecordId" Aeson..= ("wf-old" :: Text),
                      "workflowRecordStatus" Aeson..= ("PENDING" :: Text)
                    ]
                )
        case Aeson.eitherDecode encoded :: Either String WorkflowRecord of
          Left err -> fail ("expected a decode, got: " <> err)
          Right record -> do
            record.workflowRecordPriority @?= 0
            record.workflowRecordCreatedAt @?= timestampFromEpochMs 0
            record.workflowRecordStatus @?= Pending,
      testCase "a step checkpoint defaults its optional fields" $ do
        let encoded =
              Aeson.encode
                ( Aeson.object
                    [ "stepRecordWorkflowId" Aeson..= ("wf" :: Text),
                      "stepRecordStepId" Aeson..= (0 :: Int),
                      "stepRecordStepName" Aeson..= ("s" :: Text)
                    ]
                )
        case Aeson.eitherDecode encoded :: Either String StepRecord of
          Left err -> fail ("expected a decode, got: " <> err)
          Right record -> do
            record.stepRecordOutput @?= Nothing
            record.stepRecordChildWorkflowId @?= Nothing
    ]

engineTests :: TestTree
engineTests =
  testGroup
    "Engine"
    [ testCase "pins cross-SDK step names" $
        -- Mirrors Rust @the_step_names_are_pinned@: every recorded step
        -- name, spelled out. A replay compares these exactly, so renaming
        -- one strands every workflow recorded under the old name; this test
        -- is where such a rename has to be made on purpose.
        mapM_
          (\(constant, literal) -> constant @?= literal)
          [ (sleepStepName, "DBOS.sleep"),
            (patchMarkerPrefix, "DBOS.patch-"),
            (sendStepName, "DBOS.send"),
            (sendBulkStepName, "DBOS.sendBulk"),
            (writeStreamStepName, "DBOS.writeStream"),
            (closeStreamStepName, "DBOS.closeStream"),
            (readStreamStepName, "DBOS.readStream"),
            (setEventStepName, "DBOS.setEvent"),
            (getEventStepName, "DBOS.getEvent"),
            (getResultStepName, "DBOS.getResult"),
            (selectStepName, "DBOS.selectWorkflow"),
            (selectStepStepName, "DBOS.selectStep"),
            (recvStepName, "DBOS.recv"),
            (debounceStepName, "DBOS.debounceDelayedWorkflow"),
            (cancelStepName, "DBOS.cancelWorkflow"),
            (resumeStepName, "DBOS.resumeWorkflow"),
            (deleteStepName, "DBOS.deleteWorkflow"),
            (forkStepName, "DBOS.forkWorkflow"),
            (rewindStepName, "DBOS.rewindWorkflow"),
            (setWorkflowDelayStepName, "DBOS.setWorkflowDelay"),
            (updateWorkflowAttributesStepName, "DBOS.updateWorkflowAttributes"),
            (listWorkflowsStepName, "DBOS.listWorkflows"),
            (listStepsStepName, "DBOS.listWorkflowSteps"),
            (createScheduleStepName, "DBOS.createSchedule"),
            (upsertScheduleStepName, "DBOS.upsertSchedule"),
            (getScheduleStepName, "DBOS.getSchedule"),
            (listSchedulesStepName, "DBOS.listSchedules"),
            (updateScheduleStepName, "DBOS.updateSchedule"),
            (pauseScheduleStepName, "DBOS.pauseSchedule"),
            (resumeScheduleStepName, "DBOS.resumeSchedule"),
            (deleteScheduleStepName, "DBOS.deleteSchedule")
          ],
      testCase "pins the stream sentinel and sweep cap" $ do
        -- Quoted: the stored form is the JSON encoding, as Rust stores it.
        streamClosedSentinel @?= "\"__DBOS_STREAM_CLOSED__\""
        portableJsonSerialization @?= "portable_json"
        dequeueSweepCap @?= 8192,
      testCase "the close mark is the sentinel under its own label, nothing else" $ do
        isStreamClosed streamClosedSentinel (Just portableJsonSerialization) @?= True
        -- The pair travels together: the sentinel under another label is a
        -- value a producer wrote, not a close.
        isStreamClosed streamClosedSentinel (Just "rust_serde") @?= False
        isStreamClosed streamClosedSentinel Nothing @?= False
        isStreamClosed "\"v\"" (Just portableJsonSerialization) @?= False,
      testCase "a partial update leaves fields alone by default" $ do
        defaultChange @?= (Leave :: Change Int)
        changeSet (Set 3 :: Change Int) @?= Just 3
        changeSet (Leave :: Change Int) @?= Nothing
        changeIsLeave (Leave :: Change Int) @?= True
        changeIsLeave (Set 3 :: Change Int) @?= False,
      testCase "application names are short ASCII slugs" $ do
        (isValidApplicationName <$> ["api", "demo-queue", "a-b_c9"])
          @?= [True, True, True]
        (isValidApplicationName <$> ["", "ab", "ABC", "has space", "wörk", Text.replicate 257 "a"])
          @?= [False, False, False, False, False, False],
      testCase "a validated application name is the only way to build one" $ do
        -- Parsing is the constructor; rendering reads it back.
        (renderApplicationName <$> parseApplicationName "demo-queue") @?= Right "demo-queue"
        -- The refusal keeps the oracle's field and message verbatim.
        parseApplicationName "UPPER"
          @?= Left (invalidInput "new_name" "must be 3 to 30 characters of lowercase letters, digits, dashes and underscores"),
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
      testCase "a legacy-partitioned row keeps its column scopes" $
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
                  queueRecordAppName = Nothing
                }
         in do
              queueHasPartitionLimits legacy @?= False
              queueIsLegacyPartitioned legacy @?= True
              (legacy.queueRecordConcurrency, legacy.queueRecordWorkerConcurrency, legacy.queueRecordRateLimit)
                @?= (Just 3, Just 2, Just (RateLimit 10 (secondsDuration 60))),
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
                  queueRecordAppName = Nothing
                }
         in do
              queueHasPartitionLimits modern @?= True
              queueIsLegacyPartitioned modern @?= False
              (modern.queueRecordConcurrency, modern.queueRecordPartitionConcurrency) @?= (Just 3, Just 1),
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
                  queueRecordAppName = Nothing
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
                  queueRecordAppName = Nothing
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
              narrowed.workflowFilterAppScope,
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
          @?= Left (invalidInput "original_id" "must not be empty")
        forkValidate (Fork "s" (Just "") 0)
          @?= Left (invalidInput "forked_id" "must be absent rather than empty")
        forkValidate (Fork "s" Nothing (-1))
          @?= Left (invalidInput "start_step" "must not be negative")
        forkValidate (Fork "s" (Just "f") 2)
          @?= Right (),
      testCase "a batch refuses one forked id on two forks" $ do
        refuseInvalidForks [Fork "a" (Just "twice") 0, Fork "b" (Just "twice") 0] defaultForkOptions
          @?= Just (ForkBatchRefusal "`twice` is the forked id of more than one fork")
        refuseInvalidForks [Fork "a" (Just "once") 0, Fork "b" (Just "twice") 0] defaultForkOptions
          @?= Nothing,
      testCase "a batch refuses a forked id that is an original" $ do
        refuseInvalidForks [Fork "a" (Just "a") 0] defaultForkOptions
          @?= Just (ForkBatchRefusal "`a` is both a forked id and an original id")
        refuseInvalidForks [Fork "a" Nothing 0, Fork "b" (Just "a") 0] defaultForkOptions
          @?= Just (ForkBatchRefusal "`a` is both a forked id and an original id")
        refuseInvalidForks [Fork "a" (Just "") 0, Fork "b" (Just "") 0] defaultForkOptions
          @?= Just (ForkRefusal (Fork "a" (Just "") 0) "forked_id" "must be absent rather than empty"),
      testCase "a whole call refuses in the oracle's order: fork, clash, dup, options" $ do
        -- An invalid fork refuses before a duplicate id later in the batch.
        refuseInvalidForks [Fork "" (Just "twice") 0, Fork "b" (Just "twice") 0] defaultForkOptions
          @?= Just (ForkRefusal (Fork "" (Just "twice") 0) "original_id" "must not be empty")
        -- A clash refuses before a duplicate id later in the batch.
        refuseInvalidForks [Fork "a" (Just "a") 0, Fork "b" (Just "a") 0] defaultForkOptions
          @?= Just (ForkBatchRefusal "`a` is both a forked id and an original id")
        -- A duplicate refuses before bad options.
        refuseInvalidForks [Fork "a" (Just "twice") 0, Fork "b" (Just "twice") 0] (defaultForkOptions {forkOptionsAppVersion = Just ""})
          @?= Just (ForkBatchRefusal "`twice` is the forked id of more than one fork")
        -- A bad fork refuses before bad options.
        refuseInvalidForks [Fork "" Nothing 0] (defaultForkOptions {forkOptionsAppVersion = Just ""})
          @?= Just (ForkRefusal (Fork "" Nothing 0) "original_id" "must not be empty")
        refuseInvalidForks [Fork "a" (Just "once") 0] defaultForkOptions
          @?= Nothing,
      testCase "rejects fork options that would stamp or duplicate" $ do
        forkOptionsValidate (defaultForkOptions {forkOptionsAppVersion = Just ""})
          @?= Left (invalidInput "application_version" "must be absent rather than empty")
        noWait <- expectJust "durationFromMs 0" (durationFromMs 0)
        forkOptionsValidate (defaultForkOptions {forkOptionsTimeout = Just noWait})
          @?= Left (invalidInput "timeout" "must be absent rather than zero")
        forkOptionsValidate (defaultForkOptions {forkOptionsReplacementChildren = [("a", "b"), ("a", "c")]})
          @?= Left (invalidInput "replacement_children" "a is replaced more than once")
        forkOptionsValidate (defaultForkOptions {forkOptionsReplacementChildren = [("", "b")]})
          @?= Left (invalidInput "replacement_children" "a replaced child id must not be empty")
        forkOptionsValidate (defaultForkOptions {forkOptionsReplacementChildren = [("a", "")]})
          @?= Left (invalidInput "replacement_children" "the replacement for a must not be empty")
        case durationFromSecs 1e18 of
          Nothing -> fail "expected a storable-busting duration"
          Just huge ->
            forkOptionsValidate (defaultForkOptions {forkOptionsTimeout = Just huge})
              @?= Left (invalidInput "timeout" "must fit in milliseconds as a 64-bit integer")
        forkOptionsValidate defaultForkOptions
          @?= Right ()
    ]

debounceTests :: TestTree
debounceTests =
  testGroup
    "Debounces"
    [ testCase "rejects bounces that could silently match nothing" $ do
        wait <- expectJust "durationFromMs 1000" (durationFromMs 1000)
        let request =
              DebounceRequest
                { debounceRequestWorkflow = newQualifiedWorkflowName "wf",
                  debounceRequestQueueName = "q",
                  debounceRequestDeduplicationId = "k",
                  debounceRequestPeriod = wait,
                  debounceRequestInputs = Nothing,
                  debounceRequestSerialization = Nothing,
                  debounceRequestAppName = Nothing
                }
        debounceValidate request
          @?= Right ()
        debounceValidate (request {debounceRequestWorkflow = newQualifiedWorkflowName ""})
          @?= Left (invalidInput "workflow.name" "must not be empty")
        -- An empty class is absence, so it names the same workflow rather
        -- than a bounce that would match no row.
        debounceValidate (request {debounceRequestWorkflow = methodQualifiedWorkflowName "wf" ""})
          @?= Right (),
      testCase "rejects a bounce period that is zero or too long to store" $ do
        -- Mirrors Rust a_bounce_refuses_a_zero_or_unstorable_period: the
        -- period is held to the rule delay is.
        noWait <- expectJust "durationFromMs 0" (durationFromMs 0)
        wait <- expectJust "durationFromMs 1000" (durationFromMs 1000)
        huge <- expectJust "durationFromSecs 1e16" (durationFromSecs 1e16)
        let base =
              DebounceRequest
                { debounceRequestWorkflow = newQualifiedWorkflowName "wf",
                  debounceRequestQueueName = "q",
                  debounceRequestDeduplicationId = "k",
                  debounceRequestPeriod = wait,
                  debounceRequestInputs = Nothing,
                  debounceRequestSerialization = Nothing,
                  debounceRequestAppName = Nothing
                }
        debounceValidate (base {debounceRequestPeriod = noWait})
          @?= Left (invalidInput "period" "must be a positive, non-zero duration")
        debounceValidate (base {debounceRequestPeriod = huge})
          @?= Left (invalidInput "period" "must fit in milliseconds as a 64-bit integer"),
      testCase "each shape of name spells its own deduplication id" $
        -- Mirrors Rust @each_key_spells_its_deduplication_id@: the stored
        -- form a later debounce of the name must reproduce.
        [ debounceDedupKey (newQualifiedWorkflowName "f") "k",
          debounceDedupKey (methodQualifiedWorkflowName "f" "C") "k",
          debounceDedupKey (instanceQualifiedWorkflowName "f" "C" "eu") "k",
          debounceDedupKey (instanceQualifiedWorkflowName "f" "" "eu") "k"
        ]
          @?= ["f-k", "f/C-k", "f/C/eu-k", "f//eu-k"]
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
          (EncodedSendMessage (WorkflowId "dest-wf") messageBody Nothing Nothing)
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
          (EncodedSendMessage (WorkflowId "dest-wf") messageBody (Just (Topic "testtopic")) Nothing)
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
            ( EncodedSendMessage
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
            (EncodedSendMessage (WorkflowId "dest-wf") messageBody Nothing Nothing)
          ).notificationMessageUUID
          @?= MessageUUID "generated-message-id::dest-wf"
    ]

messageBody :: SerializedWorkflowValue
messageBody =
  SerializedWorkflowValue
    { serializedText = "\"hello\"",
      serializedSerialization = Just (Serialization "json")
    }
