{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Shared debouncer scenarios: one body per case, judged by one pure
-- check on each stack. The mechanism is the oracle's sysdb bounce
-- ('debounceDelayedWorkflow' onto a deduplication key), not a framework
-- workflow: the first call creates a DELAYED debounced row, later calls
-- extend its delay and replace its inputs, and every caller gets a handle
-- to the same user workflow id.
--
-- The live tree ('DBOS.Transact.DebouncerTest') runs them over Postgres
-- rows with real launches; the sim tree ('DBOS.Transact.DebouncerTestSim')
-- over the in-memory backend with the same engine calls.
module DBOS.Transact.DebouncerCases
  ( DebouncerFixture (..),
    driveQueue,
    scenarioFirstDebounceDelays,
    checkFirstDebounceDelays,
    scenarioSecondDebounceCoalesces,
    checkSecondDebounceCoalesces,
    scenarioForeignHolderRefused,
    checkForeignHolderRefused,
    scenarioInWorkflowDebounceRecordsStep,
    checkInWorkflowDebounceRecordsStep,
    scenarioTimeoutCapsExtension,
    checkTimeoutCapsExtension,
  )
where

import DBOS.Prelude
import DBOS.SystemDB (StepRecord (..), Timestamp (..), WorkflowId (..), WorkflowRecord (..), WorkflowStatus (..), debounceStepName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.Transact
import DBOS.Transact.Connection (SomeSystemDB, runSystemDB)
import DBOS.Transact.Instance (dequeueWorkflows)

-- | Domain operations each backend implements: the fixture launches over
-- its own queue set, registers the echo target, and observes rows and
-- runs. Scenarios only see these ops plus the real engine entries, never
-- SQL or maps directly.
data DebouncerFixture m = DebouncerFixture
  { dbDBOS :: DBOS m,
    dbExecutor :: Executor m,
    dbTargetRef :: WorkflowRef m EngineOnly,
    dbOtherRef :: WorkflowRef m EngineOnly,
    dbTargetQueue :: Text,
    dbForeignQueue :: Text,
    dbFreshTag :: Text -> m Text,
    dbFreshWid :: Text -> m WorkflowId,
    dbReadRow :: WorkflowId -> m (Maybe WorkflowRecord),
    dbListSteps :: WorkflowId -> m [StepRecord],
    dbUserRuns :: m Int,
    dbUserOutputs :: m [Text],
    dbAwaitUser :: WorkflowId -> m (Maybe WorkflowStatus)
  }

-- | Drives the executor's listened queues once, returning what was claimed.
-- Throws on a database failure; an empty claim is fine (the supervisor may
-- have taken the row first — claims are atomic, outcomes identical).
driveQueue :: (MonadMVar m, MonadFork m, MonadMask m, MonadTimer m, MonadTime m) => DBOS m -> m [WorkflowId]
driveQueue dbos = do
  driven <- dequeueWorkflows dbos
  case driven of
    Left err -> throwIO (userError (show err))
    Right wids -> pure wids

-- | A first debounce creates a DELAYED debounced row holding its key and
-- returns its id; the row runs once its delay passes with those inputs.
-- Returns the creation status, dedup key, debounced flag, deadline
-- presence, then the settled status, outputs, and run count.
scenarioFirstDebounceDelays ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, MonadCatch m) =>
  DebouncerFixture m ->
  m (Maybe WorkflowStatus, Maybe Text, Bool, Bool, Maybe WorkflowStatus, [Text], Int)
scenarioFirstDebounceDelays fx = do
  tag <- fx.dbFreshTag "debounce-first"
  let def = debouncerNew {debouncerQueueName = Just fx.dbTargetQueue}
  debounced <- debounce fx.dbDBOS fx.dbTargetRef def tag (secondsDuration 3) (Just (encodeWorkflowValue ("one" :: Text)))
  userWid <- case debounced of
    Right joined -> pure (WorkflowId joined.workflowId)
    Left err -> throwIO (userError ("expected the first debounce to create a row, got: " <> show err))
  row <- fx.dbReadRow userWid
  (atCreate, key, isDebounced, hasDeadline) <- case row of
    Just found -> pure (found.workflowRecordStatus, found.workflowRecordDeduplicationId, found.workflowRecordIsDebounced, found.workflowRecordDebounceDeadline /= Nothing)
    Nothing -> throwIO (userError "expected the debounced row")
  settled <- fx.dbAwaitUser userWid
  outputs <- fx.dbUserOutputs
  runs <- fx.dbUserRuns
  pure (Just atCreate, key, isDebounced, hasDeadline, settled, outputs, runs)

-- | The first debounce's row reads DELAYED with its key, then settles once
-- with those inputs after exactly one run.
checkFirstDebounceDelays :: (Maybe WorkflowStatus, Maybe Text, Bool, Bool, Maybe WorkflowStatus, [Text], Int) -> Either String ()
checkFirstDebounceDelays (atCreate, key, isDebounced, hasDeadline, settled, outputs, runs)
  | atCreate /= Just Delayed = Left ("expected the fresh row DELAYED, got: " <> show atCreate)
  | key == Nothing = Left "expected the row to hold its deduplication key"
  | not isDebounced = Left "expected the row marked debounced"
  | hasDeadline = Left "expected no deadline without a timeout"
  | settled /= Just Success = Left ("expected the debounced run SUCCESS, got: " <> show settled)
  | outputs /= ["one"] = Left ("expected the first inputs to run, got: " <> show outputs)
  | runs /= 1 = Left ("expected exactly one user run, got: " <> show runs)
  | otherwise = Right ()

-- | A second debounce inside the window extends the delay, replaces the
-- inputs, and returns the same user id; one run carries the last inputs.
-- Returns whether both handles agree, the delays, then the settled
-- status, outputs, and run count.
scenarioSecondDebounceCoalesces ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, MonadCatch m) =>
  DebouncerFixture m ->
  m (Bool, Maybe Timestamp, Maybe Timestamp, Maybe WorkflowStatus, [Text], Int)
scenarioSecondDebounceCoalesces fx = do
  tag <- fx.dbFreshTag "debounce-coalesce"
  let def = debouncerNew {debouncerQueueName = Just fx.dbTargetQueue}
      period = secondsDuration 4
  firstBounce <- debounce fx.dbDBOS fx.dbTargetRef def tag period (Just (encodeWorkflowValue ("one" :: Text)))
  wid1 <- case firstBounce of
    Right joined -> pure (WorkflowId joined.workflowId)
    Left err -> throwIO (userError ("expected the first debounce to create a row, got: " <> show err))
  delay1 <- readDelay wid1
  secondBounce <- debounce fx.dbDBOS fx.dbTargetRef def tag period (Just (encodeWorkflowValue ("two" :: Text)))
  wid2 <- case secondBounce of
    Right joined -> pure (WorkflowId joined.workflowId)
    Left err -> throwIO (userError ("expected the second debounce to join the row, got: " <> show err))
  delay2 <- readDelay wid2
  settled <- fx.dbAwaitUser wid2
  outputs <- fx.dbUserOutputs
  runs <- fx.dbUserRuns
  pure (wid1 == wid2, delay1, delay2, settled, outputs, runs)
  where
    readDelay wid = do
      row <- fx.dbReadRow wid
      case row of
        Just found -> pure found.workflowRecordDelayUntil
        Nothing -> throwIO (userError "expected the debounced row")

-- | Both calls name the same user workflow, the delay never moves
-- backwards, and one run carries the last inputs.
checkSecondDebounceCoalesces :: (Bool, Maybe Timestamp, Maybe Timestamp, Maybe WorkflowStatus, [Text], Int) -> Either String ()
checkSecondDebounceCoalesces (sameId, delay1, delay2, settled, outputs, runs)
  | not sameId = Left "expected both debounces to name the same user workflow"
  | delay2 < delay1 = Left ("expected the second bounce to extend the delay, got: " <> show (delay1, delay2))
  | settled /= Just Success = Left ("expected the debounced run SUCCESS, got: " <> show settled)
  | outputs /= ["two"] = Left ("expected the last inputs to run, got: " <> show outputs)
  | runs /= 1 = Left ("expected exactly one user run, got: " <> show runs)
  | otherwise = Right ()

-- | A foreign holder (a plain queued row on the same key) refuses the
-- debounce instead of joining it: no user row, no run. Returns the
-- refusal and the run count.
scenarioForeignHolderRefused ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, MonadCatch m) =>
  DebouncerFixture m ->
  m (Bool, Int)
scenarioForeignHolderRefused fx = do
  tag <- fx.dbFreshTag "debounce-foreign"
  let def = debouncerNew {debouncerQueueName = Just fx.dbForeignQueue}
      key = "echo-" <> tag
  planted <- startWorkflowRef fx.dbExecutor fx.dbOtherRef (startOptionsDefault {startQueue = Just ((enqueueNew fx.dbForeignQueue) {deduplicationId = Just key})}) Nothing
  _ <- case (planted :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) of
    Right _ -> pure ()
    Left err -> throwIO (userError ("expected the foreign row planted, got: " <> show err))
  debounced <- debounce fx.dbDBOS fx.dbTargetRef def tag (secondsDuration 3) (Just (encodeWorkflowValue ("one" :: Text)))
  refused <- case debounced of
    Left (ErrorSystemDatabase SystemDB.QueueDeduplicated {}) -> pure True
    Left err -> throwIO (userError ("expected a deduplication refusal, got: " <> show err))
    Right joined -> throwIO (userError ("expected no user row, got: " <> show joined.workflowId))
  runs <- fx.dbUserRuns
  pure (refused, runs)

-- | The foreign key refuses with a deduplication error and nothing runs.
checkForeignHolderRefused :: (Bool, Int) -> Either String ()
checkForeignHolderRefused (refused, runs)
  | not refused = Left "expected the foreign holder to refuse the debounce"
  | runs /= 0 = Left ("expected no user run, got: " <> show runs)
  | otherwise = Right ()

-- | Debouncing inside a workflow records a debounce step on the parent;
-- the user row still runs once with those inputs. Returns the parent
-- status, whether its steps name the bounce, then the user status,
-- outputs, and run count.
scenarioInWorkflowDebounceRecordsStep ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, MonadCatch m) =>
  DebouncerFixture m ->
  m (Maybe WorkflowStatus, Bool, Maybe WorkflowStatus, [Text], Int)
scenarioInWorkflowDebounceRecordsStep fx = do
  tag <- fx.dbFreshTag "debounce-in-workflow"
  parentWid <- fx.dbFreshWid "debounce-in-workflow"
  (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow fx.dbExecutor (newWorkflowKey "debounce-parent") parentWid (Just (encodeWorkflowValue tag))
  userText <- case ran of
    Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
      Right text -> pure text
      Left err -> throwIO (userError (show err))
    other -> throwIO (userError ("expected the parent to report its user id, got: " <> show other))
  let userWid = WorkflowId userText
  steps <- fx.dbListSteps parentWid
  row <- fx.dbReadRow parentWid
  userStatus <- fx.dbAwaitUser userWid
  outputs <- fx.dbUserOutputs
  runs <- fx.dbUserRuns
  let namesBounce = any (== debounceStepName) (map (.stepRecordStepName) steps)
  pure (fmap (.workflowRecordStatus) row, namesBounce, userStatus, outputs, runs)

-- | The parent records the bounce step and the user runs once.
checkInWorkflowDebounceRecordsStep :: (Maybe WorkflowStatus, Bool, Maybe WorkflowStatus, [Text], Int) -> Either String ()
checkInWorkflowDebounceRecordsStep (parentStatus, namesBounce, userStatus, outputs, runs)
  | parentStatus /= Just Success = Left ("expected the parent SUCCESS, got: " <> show parentStatus)
  | not namesBounce = Left "expected the parent steps to name the debounce step"
  | userStatus /= Just Success = Left ("expected the debounced run SUCCESS, got: " <> show userStatus)
  | outputs /= ["one"] = Left ("expected the bounced inputs to run, got: " <> show outputs)
  | runs /= 1 = Left ("expected exactly one user run, got: " <> show runs)
  | otherwise = Right ()

-- | A timeout caps how far a later bounce may extend the delay: past the
-- deadline the delay stops at the deadline itself. Runs on the unlistened
-- foreign queue so no worker can run the row between the bounces, and with
-- a period longer than the whole case so the row never comes due — the
-- supervisor's transition sweep releases due rows on every queue, which
-- would clear the key the second bounce must find.
-- Returns the stamped deadline and the second delay.
scenarioTimeoutCapsExtension ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, MonadDelay m, MonadCatch m) =>
  DebouncerFixture m ->
  m (Maybe Timestamp, Maybe Timestamp)
scenarioTimeoutCapsExtension fx = do
  tag <- fx.dbFreshTag "debounce-timeout"
  let def = debouncerNew {debouncerQueueName = Just fx.dbForeignQueue, debouncerTimeout = Just (secondsDuration 2)}
  firstBounce <- debounce fx.dbDBOS fx.dbTargetRef def tag (secondsDuration 5) (Just (encodeWorkflowValue ("one" :: Text)))
  wid <- case firstBounce of
    Right joined -> pure (WorkflowId joined.workflowId)
    Left err -> throwIO (userError ("expected the first debounce to create a row, got: " <> show err))
  threadDelay 1000000
  secondBounce <- debounce fx.dbDBOS fx.dbTargetRef def tag (secondsDuration 5) (Just (encodeWorkflowValue ("two" :: Text)))
  _ <- case secondBounce of
    Right joined | WorkflowId joined.workflowId == wid -> pure ()
    Right joined -> throwIO (userError ("expected the capped bounce to join, got: " <> show joined.workflowId))
    Left err -> throwIO (userError ("expected the capped bounce to join, got: " <> show err))
  row <- fx.dbReadRow wid
  case row of
    Just found -> pure (found.workflowRecordDebounceDeadline, found.workflowRecordDelayUntil)
    Nothing -> throwIO (userError "expected the debounced row")

-- | The deadline is stamped and the late bounce keeps its cap exactly.
checkTimeoutCapsExtension :: (Maybe Timestamp, Maybe Timestamp) -> Either String ()
checkTimeoutCapsExtension (deadline, delay2)
  | deadline == Nothing = Left "expected the first row to carry its deadline"
  | delay2 /= deadline = Left ("expected the late bounce capped at the deadline, got: " <> show (deadline, delay2))
  | otherwise = Right ()
