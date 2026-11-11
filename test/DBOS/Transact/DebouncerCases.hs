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
    scenarioDebouncedCapAtCreation,
    checkDebouncedCapAtCreation,
    scenarioPinnedIdNamesFreshRow,
    checkPinnedIdNamesFreshRow,
    scenarioPinnedIdReusedAcrossCycles,
    checkPinnedIdReusedAcrossCycles,
    scenarioDebounceDurationsRefused,
    checkDebounceDurationsRefused,
    scenarioDebouncerCannotAct,
    checkDebouncerCannotAct,
    scenarioCancelledDebounceFreesKey,
    checkCancelledDebounceFreesKey,
    scenarioDebounceInsideStepRefused,
    checkDebounceInsideStepRefused,
    scenarioInWorkflowBadPeriodRefused,
    checkInWorkflowBadPeriodRefused,
    scenarioKeyCollisionRefused,
    checkKeyCollisionRefused,
  )
where

import DBOS.Prelude
import Data.Text qualified as Text
import DBOS.SystemDB (Timestamp (..), WorkflowRecord (..), debounceStepName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.Transact (StepInfo (..), defaultListWorkflowStepsOptions)
import DBOS.Transact.Management (stepInfoFromRecord)
import DBOS.Transact.Serialization (decodeWorkflowValue, encodeWorkflowInput, CodecError (..))
import DBOS.Transact
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
    dbListSteps :: WorkflowId -> m [StepInfo],
    dbUserRuns :: m Int,
    dbUserOutputs :: m [Text],
    dbAwaitUser :: WorkflowId -> m (Maybe WorkflowStatus)
  }

-- | Drives the executor's listened queues once, returning what was claimed.
-- Throws on a database failure; an empty claim is fine (the supervisor may
-- have taken the row first — claims are atomic, outcomes identical).
driveQueue ::  (MonadAsync m, MonadMVar m, MonadFork m, MonadMask m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
           => DBOS m -> m [WorkflowId]
driveQueue dbos = do
  driven <- dequeueWorkflows dbos
  case driven of
    Left err -> throwIO (userError (show err))
    Right wids -> pure wids

-- | A first debounce creates a DELAYED debounced row holding its key and
-- returns its id; the row runs once its delay passes with those inputs.
-- Returns the creation status, dedup key, debounced flag, deadline
-- presence, then the settled status, outputs, and run count.
scenarioFirstDebounceDelays ::  forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                            => DebouncerFixture m ->
                               m (Maybe WorkflowStatus, Maybe Text, Bool, Bool, Maybe WorkflowStatus, [Text], Int)
scenarioFirstDebounceDelays fx = do
  tag <- fx.dbFreshTag "debounce-first"
  let def = debouncerNew {debouncerQueueName = Just fx.dbTargetQueue}
  debounced <- debounce fx.dbDBOS fx.dbTargetRef def tag (secondsDuration 3) (Just (encodeWorkflowInput ("one" :: Text)))
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

-- | A debounced creation with a delay longer than its timeout lands its
-- release on the deadline exactly: the init resolves both against one
-- clock reading and caps the release at the deadline. Returns the
-- release and the deadline side by side.
scenarioDebouncedCapAtCreation ::  forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                               => DebouncerFixture m ->
                                  m (Maybe Timestamp, Maybe Timestamp)
scenarioDebouncedCapAtCreation fx = do
  tag <- fx.dbFreshTag "debounce-capped"
  let def = debouncerNew {debouncerQueueName = Just fx.dbTargetQueue, debouncerDebounceTimeout = Just (secondsDuration 60)}
  created <- debounce fx.dbDBOS fx.dbTargetRef def tag (secondsDuration 3600) (Just (encodeWorkflowInput ("one" :: Text)))
  userWid <- case created of
    Right joined -> pure (WorkflowId joined.workflowId)
    Left err -> throwIO (userError ("expected the capped debounce to create a row, got: " <> show err))
  row <- fx.dbReadRow userWid
  case row of
    Just found -> pure (found.workflowRecordDelayUntil, found.workflowRecordDebounceDeadline)
    Nothing -> throwIO (userError "expected the debounced row")

-- | The release reads exactly the deadline: a 3600s delay under a 60s
-- timeout never escapes the deadline.
checkDebouncedCapAtCreation :: (Maybe Timestamp, Maybe Timestamp) -> Either String ()
checkDebouncedCapAtCreation (release, deadline)
  | deadline == Nothing = Left "expected the capped row to carry its deadline"
  | release /= deadline = Left ("expected the release capped at the deadline, got: " <> show (release, deadline))
  | otherwise = Right ()

-- | A pinned id names the fresh row: the call that enqueues with an id
-- the store does not know takes it, and the row reads DELAYED under it.
-- Returns whether the created id is the offered one, with the row status.
scenarioPinnedIdNamesFreshRow ::  forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                             => DebouncerFixture m ->
                                m (Bool, Maybe WorkflowStatus)
scenarioPinnedIdNamesFreshRow fx = do
  tag <- fx.dbFreshTag "debounce-pinned"
  offered <- fx.dbFreshWid "debounce-pinned-offered"
  let def = debouncerNew {debouncerQueueName = Just fx.dbTargetQueue}
      options = debounceOptionsDefault {debounceOptionsWorkflowId = Just offered}
  created <- debounceWith fx.dbDBOS fx.dbTargetRef def tag (secondsDuration 3) (Just (encodeWorkflowInput ("one" :: Text))) options
  case created of
    Left err -> throwIO (userError ("expected the pinned debounce to create a row, got: " <> show err))
    Right joined -> do
      row <- fx.dbReadRow (WorkflowId joined.workflowId)
      case row of
        Just found -> pure (WorkflowId joined.workflowId == offered, Just found.workflowRecordStatus)
        Nothing -> throwIO (userError "expected the pinned row")

-- | The created id is the offered one, and the row waits delayed under it.
checkPinnedIdNamesFreshRow :: (Bool, Maybe WorkflowStatus) -> Either String ()
checkPinnedIdNamesFreshRow (pinned, status) = do
  unless pinned $ Left "expected the fresh row to take the offered id"
  unless (status == Just Delayed) $ Left ("expected the pinned row DELAYED, got: " <> show status)

-- | A pinned id reused after its workflow ran is a conflict, not a join:
-- the first cycle runs to SUCCESS under the id, and the second call with
-- the same id is refused before anything is written. Mirrors Rust
-- @a_pinned_id_reused_after_its_workflow_ran_is_a_conflict_not_a_join@.
-- Returns the first cycle's terminal status plus the second verdict.
scenarioPinnedIdReusedAcrossCycles ::  forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                                   => DebouncerFixture m ->
                                      m (Maybe WorkflowStatus, Either (Error EngineOnly) ())
scenarioPinnedIdReusedAcrossCycles fx = do
  tag <- fx.dbFreshTag "debounce-reuse"
  offered <- fx.dbFreshWid "debounce-reuse-offered"
  let def = debouncerNew {debouncerQueueName = Just fx.dbTargetQueue}
      options = debounceOptionsDefault {debounceOptionsWorkflowId = Just offered}
  created <- debounceWith fx.dbDBOS fx.dbTargetRef def tag (secondsDuration 1) (Just (encodeWorkflowInput ("one" :: Text))) options
  case created of
    Left err -> throwIO (userError ("expected the first cycle to create a row, got: " <> show err))
    Right _ -> pure ()
  settled <- fx.dbAwaitUser offered
  reused <- debounceWith fx.dbDBOS fx.dbTargetRef def tag (secondsDuration 1) (Just (encodeWorkflowInput ("two" :: Text))) options
  pure (settled, void reused)

-- | The first cycle ran, and the reuse is refused as a conflicting
-- workflow rather than joined.
checkPinnedIdReusedAcrossCycles :: (Maybe WorkflowStatus, Either (Error EngineOnly) ()) -> Either String ()
checkPinnedIdReusedAcrossCycles (settled, verdict) = do
  unless (settled == Just Success) $ Left ("expected the first cycle SUCCESS, got: " <> show settled)
  case verdict of
    Left (SystemDatabase (SystemDBError.ConflictingWorkflow {})) -> Right ()
    other -> Left ("expected a ConflictingWorkflow refusal, got: " <> show other)

-- | A zero or unstorable period, and a zero timeout, are refused as
-- argument refusals to this operation before anything is written: no row
-- is created. Mirrors Rust @refuse_durations@. Returns the three
-- verdicts.
scenarioDebounceDurationsRefused ::  forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                                => DebouncerFixture m ->
                                   m ( Either (Error EngineOnly) (),
                                       Either (Error EngineOnly) (),
                                       Either (Error EngineOnly) ()
                                     )
scenarioDebounceDurationsRefused fx = do
  tag <- fx.dbFreshTag "debounce-refused"
  huge <- case SystemDB.durationFromSecs 1e16 of
    Nothing -> throwIO (userError "durationFromSecs 1e16 did not construct")
    Just duration -> pure duration
  let def = debouncerNew {debouncerQueueName = Just fx.dbTargetQueue}
      input = Just (encodeWorkflowInput ("one" :: Text))
  zeroPeriod <- debounce fx.dbDBOS fx.dbTargetRef def tag (millisDuration 0) input
  hugePeriod <- debounce fx.dbDBOS fx.dbTargetRef def tag huge input
  zeroTimeout <- debounce fx.dbDBOS fx.dbTargetRef (def {debouncerDebounceTimeout = Just (millisDuration 0)}) tag (secondsDuration 1) input
  pure (void zeroPeriod, void hugePeriod, void zeroTimeout)

-- | Each refusal names the operation and the offending field.
checkDebounceDurationsRefused :: ( Either (Error EngineOnly) (),
                                  Either (Error EngineOnly) (),
                                  Either (Error EngineOnly) ()
                                ) -> Either String ()
checkDebounceDurationsRefused (zeroPeriod, hugePeriod, zeroTimeout) = do
  case zeroPeriod of
    Left (InvalidArgument operation detail) -> do
      unless (operation == "debounce a workflow") $ Left ("expected the debounce operation, got: " <> show operation)
      unless (detail == "`period` must be a positive, non-zero duration") $ Left ("expected the period refusal, got: " <> show detail)
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  case hugePeriod of
    Left (InvalidArgument operation detail) -> do
      unless (operation == "debounce a workflow") $ Left ("expected the debounce operation, got: " <> show operation)
      unless (detail == "`period` must fit in milliseconds as a 64-bit integer") $ Left ("expected the period storability refusal, got: " <> show detail)
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  case zeroTimeout of
    Left (InvalidArgument operation detail) -> do
      unless (operation == "debounce a workflow") $ Left ("expected the debounce operation, got: " <> show operation)
      unless (detail == "`debounce_timeout` must be a positive, non-zero duration") $ Left ("expected the timeout refusal, got: " <> show detail)
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)

-- | What a debouncer cannot act on is refused, and nothing is written: a
-- zero budget, an empty queue and an empty app name. Mirrors the app half
-- of Rust @what_a_debouncer_cannot_act_on_is_refused@. Returns the three
-- verdicts.
scenarioDebouncerCannotAct ::  forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                           => DebouncerFixture m ->
                              m ( Either (Error EngineOnly) (),
                                  Either (Error EngineOnly) (),
                                  Either (Error EngineOnly) ()
                                )
scenarioDebouncerCannotAct fx = do
  tag <- fx.dbFreshTag "debounce-cannot"
  let base = debouncerNew {debouncerQueueName = Just fx.dbTargetQueue}
      input = Just (encodeWorkflowInput ("one" :: Text))
  zeroBudget <- debounce fx.dbDBOS fx.dbTargetRef (base {debouncerTimeout = Just (millisDuration 0)}) tag (secondsDuration 1) input
  emptyQueue <- debounce fx.dbDBOS fx.dbTargetRef (base {debouncerQueueName = Just ""}) tag (secondsDuration 1) input
  badApp <- debounce fx.dbDBOS fx.dbTargetRef (base {debouncerAppName = Just ""}) tag (secondsDuration 1) input
  pure (void zeroBudget, void emptyQueue, void badApp)

-- | Each refusal names its field: the budget on the enqueue's channel,
-- the queue and the app on the debounce's.
checkDebouncerCannotAct :: ( Either (Error EngineOnly) (),
                             Either (Error EngineOnly) (),
                             Either (Error EngineOnly) ()
                           ) -> Either String ()
checkDebouncerCannotAct (zeroBudget, emptyQueue, badApp) = do
  case zeroBudget of
    Left (InvalidArgument operation detail) -> do
      unless (operation == "enqueue a workflow") $ Left ("expected the enqueue operation, got: " <> show operation)
      unless ("`timeout`" `Text.isInfixOf` detail) $ Left ("expected the budget refusal, got: " <> show detail)
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  case emptyQueue of
    Left (InvalidArgument operation detail) -> do
      unless (operation == "debounce a workflow") $ Left ("expected the debounce operation, got: " <> show operation)
      unless ("queue_name" `Text.isInfixOf` detail) $ Left ("expected the queue refusal, got: " <> show detail)
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  case badApp of
    Left (InvalidArgument operation detail) -> do
      unless (operation == "debounce a workflow") $ Left ("expected the debounce operation, got: " <> show operation)
      unless ("`app_name`" `Text.isInfixOf` detail) $ Left ("expected the app refusal, got: " <> show detail)
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)

-- | A cancelled debounced row frees its key: cancelling the holder
-- clears the deduplication id, so the next debounce for the key starts a
-- fresh row instead of joining or refusing. Mirrors Rust
-- @a_cancelled_debounced_workflow_frees_its_key@. Returns the cancelled
-- status, the fresh status, whether the ids differ, and whether the fresh
-- row holds the key.
scenarioCancelledDebounceFreesKey ::  forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                                  => DebouncerFixture m ->
                                     m (Maybe WorkflowStatus, Maybe WorkflowStatus, Bool, Bool)
scenarioCancelledDebounceFreesKey fx = do
  tag <- fx.dbFreshTag "debounce-cancelled"
  let def = debouncerNew {debouncerQueueName = Just fx.dbTargetQueue}
  created <- debounce fx.dbDBOS fx.dbTargetRef def tag (secondsDuration 60) (Just (encodeWorkflowInput ("one" :: Text)))
  wid1 <- case created of
    Left err -> throwIO (userError ("expected the first debounce to create a row, got: " <> show err))
    Right joined -> pure (WorkflowId joined.workflowId)
  cancelled <- cancelWorkflow fx.dbDBOS wid1
  case cancelled of
    Left err -> throwIO (userError ("expected the cancel to land, got: " <> show err))
    Right () -> pure ()
  coincided <- debounce fx.dbDBOS fx.dbTargetRef def tag (secondsDuration 60) (Just (encodeWorkflowInput ("two" :: Text)))
  wid2 <- case coincided of
    Left err -> throwIO (userError ("expected the second debounce to start fresh, got: " <> show err))
    Right joined -> pure (WorkflowId joined.workflowId)
  row1 <- fx.dbReadRow wid1
  row2 <- fx.dbReadRow wid2
  case (row1, row2) of
    (Just cancelledRow, Just freshRow) ->
      pure
        ( Just cancelledRow.workflowRecordStatus,
          Just freshRow.workflowRecordStatus,
          wid1 /= wid2,
          freshRow.workflowRecordDeduplicationId /= Nothing
        )
    _ -> throwIO (userError "expected both the cancelled and the fresh row")

-- | The holder cancelled, the next debounce started fresh and delayed,
-- holding the key.
checkCancelledDebounceFreesKey :: (Maybe WorkflowStatus, Maybe WorkflowStatus, Bool, Bool) -> Either String ()
checkCancelledDebounceFreesKey (cancelled, fresh, differ, held)
  | cancelled /= Just Cancelled = Left ("expected the holder CANCELLED, got: " <> show cancelled)
  | fresh /= Just Delayed = Left ("expected the fresh row DELAYED, got: " <> show fresh)
  | not differ = Left "expected the fresh debounce to take a new id"
  | not held = Left "expected the fresh row to hold the key"
  | otherwise = Right ()

-- | A debounce inside a step is refused: steps record their own work,
-- and a debounce is a workflow-level call. Mirrors Rust
-- @a_debounce_inside_a_step_is_refused@. Returns the refused operation.
scenarioDebounceInsideStepRefused ::  forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                                 => DebouncerFixture m ->
                                    m Text
scenarioDebounceInsideStepRefused fx = do
  tag <- fx.dbFreshTag "debounce-in-step"
  parentWid <- fx.dbFreshWid "debounce-in-step"
  (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow fx.dbExecutor (newQualifiedWorkflowName "debounce-in-step-parent") parentWid (Just (encodeWorkflowInput tag))
  case ran of
    Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
      Right operation -> pure operation
      Left err -> throwIO (userError (show err))
    other -> throwIO (userError ("expected the parent to report the refusal, got: " <> show other))

-- | The refusal names the debouncing operation.
checkDebounceInsideStepRefused :: Text -> Either String ()
checkDebounceInsideStepRefused operation
  | operation == "debouncing a workflow" = Right ()
  | otherwise = Left ("expected the debouncing refusal, got: " <> show operation)

-- | A zero period inside a workflow is refused on the debounce's
-- channel: the period is policed by durations, not by the enqueue leg's
-- delay, as the oracle's @place@ validates the queue request without it.
-- Returns the refused operation and detail. Mirrors Rust @place@'s
-- validation order.
scenarioInWorkflowBadPeriodRefused ::  forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                                  => DebouncerFixture m ->
                                     m Text
scenarioInWorkflowBadPeriodRefused fx = do
  parentWid <- fx.dbFreshWid "debounce-bad-period"
  (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow fx.dbExecutor (newQualifiedWorkflowName "debounce-bad-period-parent") parentWid (Just (encodeWorkflowInput ("one" :: Text)))
  case ran of
    Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
      Right refusal -> pure refusal
      Left err -> throwIO (userError (show err))
    other -> throwIO (userError ("expected the parent to report the refusal, got: " <> show other))

-- | The refusal names the debounce operation and the period field — not
-- the enqueue leg's delay.
checkInWorkflowBadPeriodRefused :: Text -> Either String ()
checkInWorkflowBadPeriodRefused refusal = do
  unless ("debounce a workflow" `Text.isInfixOf` refusal) $ Left ("expected the debounce operation, got: " <> show refusal)
  unless ("`period`" `Text.isInfixOf` refusal) $ Left ("expected the period refusal, got: " <> show refusal)

-- | A key collision between unrelated workflows is refused, not
-- hijacked: a bounce whose deduplication id spells the same as another
-- workflow's key extends nothing, because the whole keys differ. Mirrors
-- Rust @a_key_collision_between_two_workflows_is_refused@ and pins the
-- collision refusal ADR-0042 relies on. Returns whether the bounce was
-- refused.
scenarioKeyCollisionRefused ::  forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                           => DebouncerFixture m ->
                              m Bool
scenarioKeyCollisionRefused fx = do
  tag <- fx.dbFreshTag "debounce-collision"
  let def = debouncerNew {debouncerQueueName = Just fx.dbTargetQueue}
      -- The target spells "echo-<tag>"; plant the other workflow under
      -- the identical deduplication id with a different key.
      key = "other-" <> tag
  planted <-
    startWorkflow
      fx.dbExecutor
      fx.dbOtherRef
      (startOptionsDefault {startQueue = Just (((queueRequestNew fx.dbTargetQueue) {deduplicationId = Just ("echo-" <> key)}) :: QueueRequest)})
      Nothing
  case (planted :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) of
    Left err -> throwIO (userError ("expected the colliding row planted, got: " <> show err))
    Right _ -> pure ()
  debounced <- debounce fx.dbDBOS fx.dbTargetRef def key (secondsDuration 3) (Just (encodeWorkflowInput ("one" :: Text)))
  case debounced of
    Left (SystemDatabase (SystemDBError.QueueDeduplicated {})) -> pure True
    Left err -> throwIO (userError ("expected a collision refusal, got: " <> show err))
    Right joined -> throwIO (userError ("expected no user row, got: " <> show joined.workflowId))

-- | The collision refused with a deduplication error.
checkKeyCollisionRefused :: Bool -> Either String ()
checkKeyCollisionRefused refused
  | refused = Right ()
  | otherwise = Left "expected the key collision to refuse the debounce"
-- inputs, and returns the same user id; one run carries the last inputs.
-- Returns whether both handles agree, the delays, then the settled
-- status, outputs, and run count.
scenarioSecondDebounceCoalesces ::  forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                                => DebouncerFixture m ->
                                   m (Bool, Maybe Timestamp, Maybe Timestamp, Maybe WorkflowStatus, [Text], Int)
scenarioSecondDebounceCoalesces fx = do
  tag <- fx.dbFreshTag "debounce-coalesce"
  let def = debouncerNew {debouncerQueueName = Just fx.dbTargetQueue}
      period = secondsDuration 4
  firstBounce <- debounce fx.dbDBOS fx.dbTargetRef def tag period (Just (encodeWorkflowInput ("one" :: Text)))
  wid1 <- case firstBounce of
    Right joined -> pure (WorkflowId joined.workflowId)
    Left err -> throwIO (userError ("expected the first debounce to create a row, got: " <> show err))
  delay1 <- readDelay wid1
  secondBounce <- debounce fx.dbDBOS fx.dbTargetRef def tag period (Just (encodeWorkflowInput ("two" :: Text)))
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
scenarioForeignHolderRefused ::  forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                             => DebouncerFixture m ->
                                m (Bool, Bool, Int)
scenarioForeignHolderRefused fx = do
  tag <- fx.dbFreshTag "debounce-foreign"
  let def = debouncerNew {debouncerQueueName = Just fx.dbForeignQueue}
      key = "echo-" <> tag
  planted <- startWorkflow fx.dbExecutor fx.dbOtherRef (startOptionsDefault {startQueue = Just (((queueRequestNew fx.dbForeignQueue) {deduplicationId = Just key}) :: QueueRequest)}) Nothing
  holderWid <- case (planted :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) of
    Right joined -> pure joined.workflowId
    Left err -> throwIO (userError ("expected the foreign row planted, got: " <> show err))
  debounced <- debounce fx.dbDBOS fx.dbTargetRef def tag (secondsDuration 3) (Just (encodeWorkflowInput ("one" :: Text)))
  named <- case debounced of
    Left (SystemDatabase (SystemDBError.QueueDeduplicated {SystemDBError.workflowId = holder})) -> pure holder
    Left err -> throwIO (userError ("expected a deduplication refusal, got: " <> show err))
    Right joined -> throwIO (userError ("expected no user row, got: " <> show joined.workflowId))
  runs <- fx.dbUserRuns
  pure (named == holderWid, True, runs)

-- | The foreign key refuses naming its holder, and nothing runs.
checkForeignHolderRefused :: (Bool, Bool, Int) -> Either String ()
checkForeignHolderRefused (named, refused, runs)
  | not refused = Left "expected the foreign holder to refuse the debounce"
  | not named = Left "expected the refusal to name the holder, not the pinned id"
  | runs /= 0 = Left ("expected no user run, got: " <> show runs)
  | otherwise = Right ()

-- | Debouncing inside a workflow records a debounce step on the parent;
-- the user row still runs once with those inputs. Returns the parent
-- status, whether its steps name the bounce, then the user status,
-- outputs, and run count.
scenarioInWorkflowDebounceRecordsStep ::  forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                                      => DebouncerFixture m ->
                                         m (Maybe WorkflowStatus, Bool, Maybe WorkflowStatus, [Text], Int)
scenarioInWorkflowDebounceRecordsStep fx = do
  tag <- fx.dbFreshTag "debounce-in-workflow"
  parentWid <- fx.dbFreshWid "debounce-in-workflow"
  (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow fx.dbExecutor (newQualifiedWorkflowName "debounce-parent") parentWid (Just (encodeWorkflowInput tag))
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
  let namesBounce = any (== debounceStepName) (map (.stepInfoName) steps)
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
scenarioTimeoutCapsExtension ::  forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                             => DebouncerFixture m ->
                                m (Maybe Timestamp, Maybe Timestamp)
scenarioTimeoutCapsExtension fx = do
  tag <- fx.dbFreshTag "debounce-timeout"
  let def = debouncerNew {debouncerQueueName = Just fx.dbForeignQueue, debouncerDebounceTimeout = Just (secondsDuration 2)}
  firstBounce <- debounce fx.dbDBOS fx.dbTargetRef def tag (secondsDuration 5) (Just (encodeWorkflowInput ("one" :: Text)))
  wid <- case firstBounce of
    Right joined -> pure (WorkflowId joined.workflowId)
    Left err -> throwIO (userError ("expected the first debounce to create a row, got: " <> show err))
  threadDelay 1000000
  secondBounce <- debounce fx.dbDBOS fx.dbTargetRef def tag (secondsDuration 5) (Just (encodeWorkflowInput ("two" :: Text)))
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
