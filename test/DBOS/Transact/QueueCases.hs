{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | Shared queue-registry scenarios: one body per case, judged by one pure
-- check on each stack. Slice 1 covers the validation tier — defaults, the
-- legacy re-scope, reservations, and limit coherence — which needs a
-- launched instance but no supervisor timing: nothing is enqueued, so the
-- claim path never runs and both stacks judge the same receipts and
-- refusals. Execution cases (fan-out, limits, recovery) follow in later
-- slices over the same fixture.
--
-- The live tree ('DBOS.Transact.QueueTest') runs them over Postgres rows
-- with real launches, the sim tree ('DBOS.Transact.QueueTestSim') over the
-- in-memory backend with the shared launch tail.
module DBOS.Transact.QueueCases
  ( QueueFixture (..),
    legacyPartitionedRecord,
    scenarioQueueDefaults,
    scenarioLegacyRescope,
    checkQueueDefaults,
    checkLegacyRescope,
    scenarioReserved,
    checkReserved,
    scenarioUnlaunched,
    checkUnlaunched,
    scenarioIncoherent,
    checkIncoherent,
    scenarioUpdateCoherent,
    checkUpdateCoherent,
    scenarioUnhonourable,
    checkUnhonourable,
    scenarioPausedZero,
    checkPausedZero,
    scenarioEqualLimits,
    checkEqualLimits,
    scenarioConcurrencySpellings,
    checkConcurrencySpellings,
    scenarioConcurrencyUpdate,
    checkConcurrencyUpdate,
    scenarioRateLimit,
    checkRateLimit,
    scenarioPartitionLimits,
    checkPartitionLimits,
    scenarioReregister,
    checkReregister,
    scenarioLegacyUpdateRefused,
    checkLegacyUpdateRefused,
    decodedInt,
    scenarioCrud,
    checkCrud,
    scenarioDeadlineStamped,
    checkDeadlineStamped,
    scenarioNoDeadlineYet,
    checkNoDeadlineYet,
    scenarioPartitionRow,
    checkPartitionRow,
    scenarioSentinel,
    checkSentinel,
    scenarioBadEnqueue,
    checkBadEnqueue,
    scenarioInternalRow,
    checkInternalRow,
    scenarioLateQueue,
    checkLateQueue,
    scenarioGhostQueue,
    checkGhostQueue,
    scenarioInheritedDeadline,
    checkInheritedDeadline,
    pollUntil,
    scenarioWorkerConcurrency,
    checkWorkerConcurrency,
    scenarioListenNarrow,
    checkListenNarrow,
    scenarioListenNone,
    checkListenNone,
    scenarioListenInternal,
    checkListenInternal,
    scenarioDelayed,
    checkDelayed,
    scenarioDedup,
    checkDedup,
    scenarioJoin,
    checkJoin,
    scenarioPriority,
    checkPriority,
    scenarioUpdateHonoured,
    checkUpdateHonoured,
    scenarioPartitioned,
    checkPartitioned,
    scenarioCountedPartitioned,
    checkCountedPartitioned,
    scenarioWalkSpentConcurrency,
    checkWalkSpentConcurrency,
    scenarioWalkSpentRateLimit,
    checkWalkSpentRateLimit,
    scenarioWalkFullPartition,
    checkWalkFullPartition,
    scenarioWalkSpentRoom,
    checkWalkSpentRoom,
    scenarioPeerQueue,
    checkPeerQueue,
    scenarioWorkerBudgetExhausted,
    checkWorkerBudgetExhausted,
    scenarioDispatchPark,
    checkDispatchPark,
  )
where

import Data.Int (Int64)
import Data.Map.Strict qualified as Map
import Data.Text qualified as Text
import DBOS.Prelude
import DBOS.SystemDB (AwaitedOutcome (..), Change (..), Dequeued (..), Duration, NewQueue (..), NewWorkflow (..), OnExistingQueue (..), QueueName (..), QueueRecord (..), RateLimit (..), Timestamp, WorkflowFilter (..), WorkflowId (..), WorkflowIdReusePolicy (..), WorkflowInitResult (..), WorkflowRecord (..), WorkflowStatus (..), defaultWorkflowFilter, internalQueueName, newQueue, newWorkflow, secondsDuration)
import DBOS.SystemDB qualified as SysDB
import DBOS.Transact.Connection (Connection (connSysdb), runSystemDB)
import DBOS.Transact.Dequeue (Polled (..), newRunning, pollOnce)
import DBOS.Transact.Identity (Identity (identityAppVersion))
import DBOS.Transact.Instance (Executor (conn, datasources, identity, workflowTasks, workflows))
import DBOS.Transact.Serialization (decodeWorkflowValue, encodeWorkflowInput, CodecError (..))
import DBOS.Transact
  (
    DBOS,
    DuplicationPolicy (..),
    EngineOnly,
    QueueRequest (..),
    Error (..),
    Executor,
    Queue (..),
    QueueChange (..),
    QueueConflict (..),
    QueueOptions (..),
    RunOptions (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    StartOptions (..),
    Timeout (..),
    WorkflowCtx,
    WorkflowHandle (workflowId),
    defaultQueueOptions,
    deleteQueue,
    enqueueWorkflow,
    queueRequestNew,
    handleResult,
    handleStatus,
    isLaunched,
    listQueues,
    listWorkflows,
    millisDuration,
    newQualifiedWorkflowName,
    queue,
    registerWorkflow,
    registerWorkflowRef,
    registerQueue,
    retrieveWorkflow,
    runWorkflow,
    runWorkflowRef,
    runOptionsDefault,
    startChildWorkflow,
    startWorkflow,
    startOptionsDefault,
    updateQueue,
    waitForWorkflow,
  )
import DBOS.Transact.Queue (defaultQueueChange, queueFromRecord, queueIsPartitioned)

-- | What a stack must provide: the leaf's suffix and application name,
-- the bracketed instance, launch/shutdown, a raw queue-row write (for
-- stored-row cases), and a raw queue-row read (for stored-flag asserts —
-- the facade read resolves limits, it does not expose the raw flag).
data QueueFixture m = QueueFixture
  { qfSuffix :: Text,
    qfAppName :: Text,
    qfDBOS :: DBOS m,
    qfLaunch :: m (Executor m),
    qfShutdown :: m (),
    qfUpsertQueue :: NewQueue -> OnExistingQueue -> m (Either SysDB.Error Bool),
    qfReadQueueRow :: Text -> m (Maybe QueueRecord),
    qfReadWorkflowRow :: WorkflowId -> m (Maybe WorkflowRecord),
    qfPlantAttempts :: WorkflowId -> Int64 -> m ()
  }

-- | The legacy row the re-scope case resolves: the deprecated partition
-- flag carries fleet limits, which come back as per-partition limits.
legacyPartitionedRecord :: QueueRecord
legacyPartitionedRecord =
  QueueRecord
    { queueRecordName = "legacy",
      queueRecordConcurrency = Just 8,
      queueRecordWorkerConcurrency = Just 3,
      queueRecordRateLimit = Nothing,
      queueRecordPriorityEnabled = True,
      queueRecordPartitionQueue = True,
      queueRecordPartitionConcurrency = Nothing,
      queueRecordPartitionWorkerConcurrency = Nothing,
      queueRecordPartitionRateLimit = Nothing,
      queueRecordPollingInterval = secondsDuration 1,
      queueRecordAppName = Just "app"
    }

-- | Fresh defaults admit everything and poll once a second.
scenarioQueueDefaults :: Applicative m => QueueFixture m -> m QueueOptions
scenarioQueueDefaults _ = pure defaultQueueOptions

-- | The deprecated flag re-scopes fleet limits as per-partition limits.
scenarioLegacyRescope :: Applicative m => QueueFixture m -> m Queue
scenarioLegacyRescope _ = pure (queueFromRecord legacyPartitionedRecord)

checkQueueDefaults :: QueueOptions -> Either String ()
checkQueueDefaults options = do
  unless (options.concurrency == Nothing) $ Left ("expected no fleet limit, got: " <> show options.concurrency)
  unless (options.workerConcurrency == Nothing) $ Left ("expected no worker limit, got: " <> show options.workerConcurrency)
  unless (options.pollingInterval == secondsDuration 1) $ Left "expected the one-second poll"
  unless (options.globalConcurrency == Nothing) $ Left ("expected no explicit fleet limit, got: " <> show options.globalConcurrency)

-- | The deprecated flag re-scopes fleet limits as per-partition limits.
checkLegacyRescope :: Queue -> Either String ()
checkLegacyRescope receipt = do
  unless (receipt.name == "legacy") $ Left ("expected the legacy name, got: " <> show receipt.name)
  unless (receipt.concurrency == Nothing) $ Left ("expected no fleet limit, got: " <> show receipt.concurrency)
  unless (receipt.workerConcurrency == Nothing) $ Left ("expected no worker limit, got: " <> show receipt.workerConcurrency)
  unless (receipt.partitionConcurrency == Just 8) $ Left ("expected the partition limit 8, got: " <> show receipt.partitionConcurrency)
  unless (receipt.partitionWorkerConcurrency == Just 3) $ Left ("expected the partition worker limit 3, got: " <> show receipt.partitionWorkerConcurrency)
  unless (queueIsPartitioned receipt) $ Left "expected the resolved receipt partitioned"

-- | The internal queue refuses every write with the reservation named.
scenarioReserved :: forall m. (MonadSTM m, MonadMVar m)
                 => QueueFixture m -> m (Either (Error EngineOnly) Queue, Either (Error EngineOnly) Queue, Either (Error EngineOnly) ())
scenarioReserved fx = do
  _ <- fx.qfLaunch
  let QueueName internalName = internalQueueName
  refusedRegister <- registerQueue fx.qfDBOS internalName defaultQueueOptions AlwaysUpdate
  refusedUpdate <- updateQueue fx.qfDBOS internalName defaultQueueChange
  refusedDelete <- deleteQueue fx.qfDBOS internalName
  fx.qfShutdown
  pure (refusedRegister, refusedUpdate, refusedDelete)

checkReserved :: (Either (Error EngineOnly) Queue, Either (Error EngineOnly) Queue, Either (Error EngineOnly) ()) -> Either String ()
checkReserved (refusedRegister, refusedUpdate, refusedDelete) = do
  case refusedRegister of
    Left (ErrorConfig message) ->
      unless ("reserved" `Text.isInfixOf` message) $ Left ("the registration refusal names nothing reserved, got: " <> show message)
    other -> Left ("expected a configuration refusal, got: " <> show other)
  case refusedUpdate of
    Left (ErrorConfig message) ->
      unless ("reserved" `Text.isInfixOf` message) $ Left ("the update refusal names nothing reserved, got: " <> show message)
    other -> Left ("expected a configuration refusal, got: " <> show other)
  case refusedDelete of
    Left (ErrorConfig message) ->
      unless ("reserved" `Text.isInfixOf` message) $ Left ("the delete refusal names nothing reserved, got: " <> show message)
    other -> Left ("expected a configuration refusal, got: " <> show other)

-- | Registering before launch is refused, never stored.
scenarioUnlaunched :: forall m. (MonadSTM m, MonadMVar m)
                   => QueueFixture m -> m (Either (Error EngineOnly) Queue)
scenarioUnlaunched fx = do
  let queueName = "hs-l2-queue-" <> Text.take 12 fx.qfSuffix
  refused <- registerQueue fx.qfDBOS queueName defaultQueueOptions AlwaysUpdate
  fx.qfShutdown
  pure refused

checkUnlaunched :: Either (Error EngineOnly) Queue -> Either String ()
checkUnlaunched refused = case refused of
  Left NotLaunched {} -> Right ()
  other -> Left ("expected a not-launched refusal, got: " <> show other)

-- | One incoherent table: what it is, the options, and the fragment the
-- refusal must carry.
incoherentTable :: [(Text, QueueOptions, Text)]
incoherentTable =
  [ ( "a negative fleet-wide limit",
      (defaultQueueOptions :: QueueOptions) {concurrency = Just (-1)},
      "`concurrency` must not be negative"
    ),
    ( "a negative per-process limit",
      (defaultQueueOptions :: QueueOptions) {workerConcurrency = Just (-1)},
      "`worker_concurrency` must not be negative"
    ),
    ( "a paused queue with a per-partition limit",
      (defaultQueueOptions :: QueueOptions) {concurrency = Just 0, partitionConcurrency = Just 1},
      "must be greater than or equal to `partition_concurrency`"
    ),
    ( "a rate limit admitting nothing",
      defaultQueueOptions {rateLimit = Just (RateLimit {rateLimitLimit = 0, rateLimitPeriod = secondsDuration 1})},
      "rate_limit.limit"
    ),
    ( "a rate limit over no window",
      (defaultQueueOptions :: QueueOptions) {rateLimit = Just (RateLimit {rateLimitLimit = 1, rateLimitPeriod = secondsDuration 0})},
      "rate_limit.period"
    ),
    ( "no workflows at all per partition",
      (defaultQueueOptions :: QueueOptions) {partitionConcurrency = Just 0},
      "partition_concurrency"
    ),
    ( "a partition allowed more than the whole queue",
      (defaultQueueOptions :: QueueOptions) {concurrency = Just 2, partitionConcurrency = Just 4},
      "must be greater than or equal to `partition_concurrency`"
    ),
    ( "a partition allowed more than the whole queue, explicit spelling",
      (defaultQueueOptions :: QueueOptions) {globalConcurrency = Just 2, partitionConcurrency = Just 4},
      "must be greater than or equal to `partition_concurrency`"
    ),
    ( "a partition worker allowed more than the whole queue",
      (defaultQueueOptions :: QueueOptions) {concurrency = Just 2, partitionWorkerConcurrency = Just 4},
      "must be greater than or equal to `partition_worker_concurrency`"
    )
  ]

-- | Every incoherent table entry is refused before it reaches the row,
-- and the refused registration writes nothing.
scenarioIncoherent :: forall m. (MonadSTM m, MonadMVar m)
                   => QueueFixture m -> m ([(Text, Text, Either (Error EngineOnly) Queue)], Maybe Queue, Either (Error EngineOnly) Queue)
scenarioIncoherent fx = do
  _ <- fx.qfLaunch
  let queueName = "hs-l2-checked-" <> Text.take 12 fx.qfSuffix
  refused <- mapM (\(what, options, fragment) -> (what,fragment,) <$> registerQueue fx.qfDBOS queueName options AlwaysUpdate) incoherentTable
  stored <- queue fx.qfDBOS queueName >>= either (error . show) pure
  accepted <-
    registerQueue
      fx.qfDBOS
      ("hs-l2-rated-q-" <> Text.take 12 fx.qfSuffix)
      ( defaultQueueOptions
          { rateLimit = Just (RateLimit {rateLimitLimit = 10, rateLimitPeriod = secondsDuration 60}),
            partitionRateLimit = Just (RateLimit {rateLimitLimit = 5, rateLimitPeriod = secondsDuration 1})
          }
      )
      AlwaysUpdate
  fx.qfShutdown
  pure (refused, stored, accepted)

checkIncoherent :: ([(Text, Text, Either (Error EngineOnly) Queue)], Maybe Queue, Either (Error EngineOnly) Queue) -> Either String ()
checkIncoherent (refused, stored, accepted) = do
  mapM_
    ( \(what, fragment, result) -> case result of
        Left (ErrorConfig message)
          | fragment `Text.isInfixOf` message -> Right ()
          | otherwise -> Left (Text.unpack what <> ": refusal misses " <> Text.unpack fragment <> ", got: " <> Text.unpack message)
        other -> Left (Text.unpack what <> " was accepted: " <> show other)
    )
    refused
  unless (stored == Nothing) $ Left ("a refused registration wrote a row anyway: " <> show stored)
  case accepted of
    Right _ -> Right ()
    other -> Left ("a shorter per-partition window should be honoured: " <> show other)

-- | An update is judged against the merged row: wrong beside the stored
-- concurrency is refused and leaves the row untouched, while raising both
-- together is coherent and accepted.
scenarioUpdateCoherent :: forall m. (MonadSTM m, MonadMVar m)
                       => QueueFixture m -> m (Either (Error EngineOnly) Queue, Maybe Queue, Either (Error EngineOnly) Queue)
scenarioUpdateCoherent fx = do
  _ <- fx.qfLaunch
  let queueName = "hs-l2-incoherent-q-" <> Text.take 12 fx.qfSuffix
      coherent = (defaultQueueOptions :: QueueOptions) {concurrency = Just 2, workerConcurrency = Just 2}
  _ <- registerQueue fx.qfDBOS queueName coherent AlwaysUpdate >>= either (error . show) pure
  refused <- updateQueue fx.qfDBOS queueName (defaultQueueChange {workerConcurrency = Set (Just 5)})
  stored <- queue fx.qfDBOS queueName >>= either (error . show) pure
  raised <- updateQueue fx.qfDBOS queueName (defaultQueueChange {concurrency = Set (Just 5), workerConcurrency = Set (Just 5)})
  fx.qfShutdown
  pure (refused, stored, raised)

checkUpdateCoherent :: (Either (Error EngineOnly) Queue, Maybe Queue, Either (Error EngineOnly) Queue) -> Either String ()
checkUpdateCoherent (refused, stored, raised) = do
  case refused of
    Left (ErrorConfig message) ->
      unless ("must be greater than or equal to" `Text.isInfixOf` message) $ Left ("the pair refusal names no bound, got: " <> show message)
    other -> Left ("expected a pair refusal, got: " <> show other)
  case stored of
    Just receipt -> unless (receipt.workerConcurrency == Just 2) $ Left ("the refused update touched the row: " <> show receipt.workerConcurrency)
    other -> Left ("expected the stored limits untouched, got: " <> show other)
  case raised of
    Right receipt -> unless (receipt.workerConcurrency == Just 5) $ Left ("the coherent raise landed nowhere: " <> show receipt.workerConcurrency)
    other -> Left ("expected the coherent raise accepted, got: " <> show other)

-- | The per-partition honour table: what it is, the options, and the
-- fragment the refusal must carry. Per-partition rates are never
-- cross-compared against the queue-wide rate (Rust queue.rs): a slower
-- per-partition rate over a longer window and a faster per-partition rate
-- alike stay honourable and are accepted.
unhonourableTable :: [(Text, QueueOptions, Text)]
unhonourableTable =
  [ ( "a per-partition rate limit over no window",
      defaultQueueOptions {partitionRateLimit = Just (RateLimit {rateLimitLimit = 1, rateLimitPeriod = secondsDuration 0})},
      "partition_rate_limit.period"
    ),
    ( "a partition's worker limit above the partition's own",
      defaultQueueOptions {partitionConcurrency = Just 2, partitionWorkerConcurrency = Just 4},
      "must be greater than or equal to `partition_worker_concurrency`"
    ),
    ( "a partition's worker limit above this process's own",
      (defaultQueueOptions :: QueueOptions) {workerConcurrency = Just 2, partitionWorkerConcurrency = Just 4},
      "must be greater than or equal to `partition_worker_concurrency`"
    )
  ]

scenarioUnhonourable :: forall m. (MonadSTM m, MonadMVar m)
                     => QueueFixture m -> m ([(Text, Text, Either (Error EngineOnly) Queue)], Maybe Queue, Either (Error EngineOnly) Queue, Either (Error EngineOnly) Queue)
scenarioUnhonourable fx = do
  _ <- fx.qfLaunch
  let queueName = "hs-l2-checked-" <> Text.take 12 fx.qfSuffix
  refused <- mapM (\(what, options, fragment) -> (what,fragment,) <$> registerQueue fx.qfDBOS queueName options UpdateIfLatestVersion) unhonourableTable
  stored <- queue fx.qfDBOS queueName >>= either (error . show) pure
  accepted <-
    registerQueue
      fx.qfDBOS
      queueName
      (defaultQueueOptions {rateLimit = Just (RateLimit {rateLimitLimit = 10, rateLimitPeriod = secondsDuration 1}), partitionRateLimit = Just (RateLimit {rateLimitLimit = 100, rateLimitPeriod = secondsDuration 60})})
      UpdateIfLatestVersion
  acceptedFast <-
    registerQueue
      fx.qfDBOS
      ("hs-l2-fast-q-" <> Text.take 12 fx.qfSuffix)
      (defaultQueueOptions {rateLimit = Just (RateLimit {rateLimitLimit = 10, rateLimitPeriod = secondsDuration 1}), partitionRateLimit = Just (RateLimit {rateLimitLimit = 100, rateLimitPeriod = secondsDuration 1})})
      UpdateIfLatestVersion
  fx.qfShutdown
  pure (refused, stored, accepted, acceptedFast)

checkUnhonourable :: ([(Text, Text, Either (Error EngineOnly) Queue)], Maybe Queue, Either (Error EngineOnly) Queue, Either (Error EngineOnly) Queue) -> Either String ()
checkUnhonourable (refused, stored, accepted, acceptedFast) = do
  mapM_
    ( \(what, fragment, result) -> case result of
        Left (ErrorConfig message)
          | fragment `Text.isInfixOf` message -> Right ()
          | otherwise -> Left (Text.unpack what <> ": refusal misses " <> Text.unpack fragment <> ", got: " <> Text.unpack message)
        other -> Left (Text.unpack what <> " was accepted: " <> show other)
    )
    refused
  unless (stored == Nothing) $ Left ("a refused registration wrote a row anyway: " <> show stored)
  case accepted of
    Right _ -> Right ()
    other -> Left ("a slower per-partition rate should be honoured: " <> show other)
  case acceptedFast of
    Right _ -> Right ()
    other -> Left ("a faster per-partition rate is never binding and should be honoured: " <> show other)

-- | A queue-wide limit of 0 pauses the queue until an update raises it,
-- for the fleet-wide limit and the per-process one alike, on an
-- unpartitioned queue and on a partitioned one. Mirrors the oracle's
-- @a_limit_of_zero_pauses_a_queue_until_it_is_raised@: registration and
-- an update take 0 and refuse -1; while the limit is 0 the workflows stay
-- ENQUEUED; once it is raised they run. A control queue with no limits is
-- registered beside each paused one and must finish a workflow before the
-- paused pair is checked: the sweep that starts the control queue's
-- worker starts the paused queue's too, so the paused queue has been
-- polled by then.
scenarioPausedZero :: forall m. (MonadMVar m, MonadSTM m, MonadFork m, MonadMask m, MonadDelay m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                   => QueueFixture m -> m [(Text, Text, Maybe WorkflowStatus, Maybe WorkflowStatus, Maybe Int, Either String Int, Either String Int)]
scenarioPausedZero fx = do
  let key = newQualifiedWorkflowName "paused"
      body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = pure (Right 1)
      setConcurrency limit = (defaultQueueChange :: QueueChange) {concurrency = Set limit}
      setWorkerConcurrency limit = (defaultQueueChange :: QueueChange) {workerConcurrency = Set limit}
      readConcurrency receipt = receipt.concurrency
      readWorkerConcurrency receipt = receipt.workerConcurrency
      perPartitionRate = RateLimit {rateLimitLimit = 100, rateLimitPeriod = secondsDuration 60}
      cases :: [(Text, Text, QueueOptions, Maybe Int -> QueueChange, Queue -> Maybe Int, Maybe Text)]
      cases =
        [ ( "concurrency",
            "unpartitioned",
            (defaultQueueOptions :: QueueOptions) {concurrency = Just 0},
            setConcurrency,
            readConcurrency,
            Nothing
          ),
          ( "worker_concurrency",
            "unpartitioned",
            (defaultQueueOptions :: QueueOptions) {workerConcurrency = Just 0},
            setWorkerConcurrency,
            readWorkerConcurrency,
            Nothing
          ),
          ( "concurrency",
            "partitioned",
            (defaultQueueOptions :: QueueOptions) {concurrency = Just 0, partitionRateLimit = Just perPartitionRate},
            setConcurrency,
            readConcurrency,
            Just "key"
          ),
          ( "worker_concurrency",
            "partitioned",
            (defaultQueueOptions :: QueueOptions) {workerConcurrency = Just 0, partitionConcurrency = Just 1},
            setWorkerConcurrency,
            readWorkerConcurrency,
            Just "key"
          )
        ]
  ref <- registerWorkflowRef fx.qfDBOS key body >>= orCrash
  exec <- fx.qfLaunch
  results <- mapM (runCase exec ref) cases
  fx.qfShutdown
  pure results
  where
    runCase exec ref (field, shape, options, change, readLimit, partitionKey) = do
      let queueName = "hs-l2-paused-" <> field <> "-" <> shape <> "-" <> Text.take 12 fx.qfSuffix
          control = queueName <> "-control"
          fast = (defaultQueueOptions :: QueueOptions) {pollingInterval = millisDuration 100}
          limited = (options :: QueueOptions) {pollingInterval = millisDuration 100}
          wid n = WorkflowId (queueName <> "-" <> Text.pack (show (n :: Int)))
          startOn queue wid' pk =
            startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just wid', startQueue = Just ((queueRequestNew queue) {partitionKey = pk})}) Nothing >>= orCrash
      _ <- registerQueue fx.qfDBOS queueName limited AlwaysUpdate >>= orCrash
      _ <- registerQueue fx.qfDBOS control fast AlwaysUpdate >>= orCrash
      refused <- updateQueue fx.qfDBOS queueName (change (Just (-1)))
      refusal <- case refused of
        Left (ErrorConfig message) -> pure message
        other -> error (Text.unpack queueName <> ": a negative limit was accepted: " <> show other)
      _ <- updateQueue fx.qfDBOS queueName (change (Just 0)) >>= orCrash
      first <- startOn queueName (wid 0) partitionKey
      second <- startOn queueName (wid 1) partitionKey
      controlled <- startOn control (WorkflowId (control <> "-0")) Nothing
      controlledOut <- timeout 15000000 (handleResult controlled) >>= maybe (error (Text.unpack queueName <> ": the control workflow never finished")) pure >>= orCrash
      case decodedInt (Right controlledOut) of
        Right 1 -> pure ()
        other -> error (Text.unpack queueName <> ": the control workflow did not return 1: " <> show other)
      -- The control finished, so the sweep has polled the paused queue;
      -- settle before asserting nothing moved.
      threadDelay 500000
      slow0 <- retrieveWorkflow fx.qfDBOS (wid 0) >>= orCrash >>= handleStatus >>= orCrash
      slow1 <- retrieveWorkflow fx.qfDBOS (wid 1) >>= orCrash >>= handleStatus >>= orCrash
      raised <- updateQueue fx.qfDBOS queueName (change (Just 2)) >>= orCrash
      out0raw <- timeout 15000000 (handleResult first) >>= maybe (error (Text.unpack queueName <> ": the raised queue never ran its workflows")) pure >>= orCrash
      out1raw <- timeout 15000000 (handleResult second) >>= maybe (error (Text.unpack queueName <> ": the raised queue never ran its workflows")) pure >>= orCrash
      let out0 = decodedInt (Right out0raw)
          out1 = decodedInt (Right out1raw)
      pure (field <> "-" <> shape, refusal, slow0, slow1, readLimit raised, out0, out1)

checkPausedZero :: [(Text, Text, Maybe WorkflowStatus, Maybe WorkflowStatus, Maybe Int, Either String Int, Either String Int)] -> Either String ()
checkPausedZero cases = mapM_ judge cases
  where
    judge (name, refusal, slow0, slow1, raised, out0, out1) = do
      let field = Text.takeWhile (/= '-') name
      unless (("`" <> field <> "` must not be negative") `Text.isInfixOf` refusal) $
        Left (Text.unpack name <> ": expected a refusal of the negative limit, got: " <> Text.unpack refusal)
      unless (slow0 == Just Enqueued && slow1 == Just Enqueued) $
        Left (Text.unpack name <> ": a queue paused at 0 dequeued a workflow: " <> show (slow0, slow1))
      unless (raised == Just 2) $ Left (Text.unpack name <> ": the raised limit was not stored: " <> show raised)
      unless (out0 == Right 1 && out1 == Right 1) $
        Left (Text.unpack name <> ": the raised queue did not run its workflows: " <> show (out0, out1))

-- | A per-process limit may equal the fleet limit it serves.
scenarioEqualLimits :: forall m. (MonadSTM m, MonadMVar m)
                    => QueueFixture m -> m (Either (Error EngineOnly) Queue)
scenarioEqualLimits fx = do
  _ <- fx.qfLaunch
  let queueName = "hs-l2-equal-q-" <> Text.take 12 fx.qfSuffix
  registered <-
    registerQueue
      fx.qfDBOS
      queueName
      (defaultQueueOptions {concurrency = Just 3, workerConcurrency = Just 3})
      UpdateIfLatestVersion
  fx.qfShutdown
  pure registered

checkEqualLimits :: Either (Error EngineOnly) Queue -> Either String ()
checkEqualLimits registered = case registered of
  Right receipt -> do
    unless (receipt.concurrency == Just 3) $ Left ("expected the fleet limit 3, got: " <> show receipt.concurrency)
    unless (receipt.workerConcurrency == Just 3) $ Left ("expected the worker limit 3, got: " <> show receipt.workerConcurrency)
  other -> Left ("expected the equal limits accepted, got: " <> show other)

-- | The fleet limit has two spellings, and setting both is refused (Python
-- _queue.py): the explicit spelling alone and the deprecated alias alone
-- each work, and each gets its own queue reporting the effective limit.
scenarioConcurrencySpellings :: forall m. (MonadSTM m, MonadMVar m)
                             => QueueFixture m -> m (Either (Error EngineOnly) Queue, Either (Error EngineOnly) Queue, Either (Error EngineOnly) Queue)
scenarioConcurrencySpellings fx = do
  _ <- fx.qfLaunch
  let queueTag = Text.take 12 fx.qfSuffix
  explicit <- registerQueue fx.qfDBOS ("hs-l2-explicit-q-" <> queueTag) (defaultQueueOptions {globalConcurrency = Just 3}) UpdateIfLatestVersion
  legacy <- registerQueue fx.qfDBOS ("hs-l2-legacy-q-" <> queueTag) (defaultQueueOptions {concurrency = Just 4}) UpdateIfLatestVersion
  both <- registerQueue fx.qfDBOS ("hs-l2-both-q-" <> queueTag) (defaultQueueOptions {globalConcurrency = Just 5, concurrency = Just 2}) UpdateIfLatestVersion
  fx.qfShutdown
  pure (explicit, legacy, both)

checkConcurrencySpellings :: (Either (Error EngineOnly) Queue, Either (Error EngineOnly) Queue, Either (Error EngineOnly) Queue) -> Either String ()
checkConcurrencySpellings (explicit, legacy, both) = do
  case explicit of
    Right receipt -> unless (receipt.concurrency == Just 3) $ Left ("expected the explicit limit 3, got: " <> show receipt.concurrency)
    other -> Left ("expected the explicit spelling accepted, got: " <> show other)
  case legacy of
    Right receipt -> unless (receipt.concurrency == Just 4) $ Left ("expected the alias limit 4, got: " <> show receipt.concurrency)
    other -> Left ("expected the deprecated alias accepted, got: " <> show other)
  case both of
    Left (ErrorConfig message)
      | "set only one of them" `Text.isInfixOf` message -> Right ()
      | otherwise -> Left ("the both-spellings refusal names no exclusivity, got: " <> Text.unpack message)
    other -> Left ("expected both spellings refused, got: " <> show other)

-- | Updates name one spelling at a time: migrating a queue from the alias
-- to the explicit spelling succeeds, while a single change naming both
-- spellings is refused and leaves the row untouched.
scenarioConcurrencyUpdate :: forall m. (MonadSTM m, MonadMVar m)
                          => QueueFixture m -> m (Either (Error EngineOnly) Queue, Either (Error EngineOnly) Queue, Maybe Queue)
scenarioConcurrencyUpdate fx = do
  _ <- fx.qfLaunch
  let queueName = "hs-l2-spell-q-" <> Text.take 12 fx.qfSuffix
  _ <- registerQueue fx.qfDBOS queueName (defaultQueueOptions {concurrency = Just 2}) AlwaysUpdate >>= either (error . show) pure
  migrated <- updateQueue fx.qfDBOS queueName (defaultQueueChange {globalConcurrency = Set (Just 5)})
  refused <- updateQueue fx.qfDBOS queueName (defaultQueueChange {concurrency = Set (Just 7), globalConcurrency = Set (Just 9)})
  stored <- queue fx.qfDBOS queueName >>= either (error . show) pure
  fx.qfShutdown
  pure (migrated, refused, stored)

checkConcurrencyUpdate :: (Either (Error EngineOnly) Queue, Either (Error EngineOnly) Queue, Maybe Queue) -> Either String ()
checkConcurrencyUpdate (migrated, refused, stored) = do
  case migrated of
    Right receipt -> unless (receipt.concurrency == Just 5) $ Left ("expected the migrated limit 5, got: " <> show receipt.concurrency)
    other -> Left ("expected the spelling migration accepted, got: " <> show other)
  case refused of
    Left (ErrorConfig message)
      | "set only one of them" `Text.isInfixOf` message -> Right ()
      | otherwise -> Left ("the both-spellings update refusal names no exclusivity, got: " <> Text.unpack message)
    other -> Left ("expected the both-spellings update refused, got: " <> show other)
  case stored of
    Just receipt -> unless (receipt.concurrency == Just 5) $ Left ("the refused update touched the row: " <> show receipt.concurrency)
    other -> Left ("expected the stored limit untouched, got: " <> show other)

-- | A queue carries a rate limit, and both clear at runtime like every
-- other limit. Registration and update always persist the legacy priority
-- column as true, as the Python oracle does: there is no priority option.
scenarioRateLimit :: forall m. (MonadSTM m, MonadMVar m)
                  => QueueFixture m -> m (Either (Error EngineOnly) Queue, Maybe QueueRecord, Either (Error EngineOnly) Queue, Maybe QueueRecord)
scenarioRateLimit fx = do
  _ <- fx.qfLaunch
  let queueName = "hs-l2-limited-q-" <> Text.take 12 fx.qfSuffix
  registered <-
    registerQueue
      fx.qfDBOS
      queueName
      (defaultQueueOptions {rateLimit = Just (RateLimit {rateLimitLimit = 5, rateLimitPeriod = secondsDuration 30})})
      UpdateIfLatestVersion
  stored <- fx.qfReadQueueRow queueName
  updated <- updateQueue fx.qfDBOS queueName (defaultQueueChange {rateLimit = Set Nothing})
  storedAgain <- fx.qfReadQueueRow queueName
  fx.qfShutdown
  pure (registered, stored, updated, storedAgain)

checkRateLimit :: (Either (Error EngineOnly) Queue, Maybe QueueRecord, Either (Error EngineOnly) Queue, Maybe QueueRecord) -> Either String ()
checkRateLimit (registered, stored, updated, storedAgain) = do
  case registered of
    Right receipt -> do
      unless (receipt.rateLimit == Just (RateLimit {rateLimitLimit = 5, rateLimitPeriod = secondsDuration 30})) $ Left ("expected the rate limit stored, got: " <> show receipt.rateLimit)
      unless (queueIsPartitioned receipt == False) $ Left "expected nothing partitioned"
    other -> Left ("expected the limited queue registered, got: " <> show other)
  case stored of
    Just record -> unless (record.queueRecordPriorityEnabled) $ Left "expected the legacy priority column persisted true"
    Nothing -> Left "expected the registered row"
  case updated of
    Right receipt -> do
      unless (receipt.rateLimit == Nothing) $ Left ("expected the rate limit cleared, got: " <> show receipt.rateLimit)
    other -> Left ("expected the limits cleared, got: " <> show other)
  case storedAgain of
    Just record -> unless (record.queueRecordPriorityEnabled) $ Left "expected the legacy priority column kept true"
    Nothing -> Left "expected the updated row"

-- | Per-partition limits partition the queue — the derived flag is
-- written — and clearing the last one un-partitions it, flag included.
scenarioPartitionLimits :: forall m. (MonadSTM m, MonadMVar m)
                        => QueueFixture m -> m (Either (Error EngineOnly) Queue, Maybe QueueRecord, Either (Error EngineOnly) Queue, Maybe QueueRecord)
scenarioPartitionLimits fx = do
  _ <- fx.qfLaunch
  let queueName = "hs-l2-sharded-" <> Text.take 12 fx.qfSuffix
  registered <-
    registerQueue
      fx.qfDBOS
      queueName
      (defaultQueueOptions {concurrency = Just 60, workerConcurrency = Just 10, partitionConcurrency = Just 4, partitionWorkerConcurrency = Just 2})
      UpdateIfLatestVersion
  stored <- fx.qfReadQueueRow queueName
  updated <- updateQueue fx.qfDBOS queueName (defaultQueueChange {partitionConcurrency = Set Nothing, partitionWorkerConcurrency = Set Nothing})
  storedAgain <- fx.qfReadQueueRow queueName
  fx.qfShutdown
  pure (registered, stored, updated, storedAgain)

checkPartitionLimits :: (Either (Error EngineOnly) Queue, Maybe QueueRecord, Either (Error EngineOnly) Queue, Maybe QueueRecord) -> Either String ()
checkPartitionLimits (registered, stored, updated, storedAgain) = do
  case registered of
    Right receipt -> do
      unless (queueIsPartitioned receipt) $ Left "expected a partition limit to partition it"
      unless (receipt.concurrency == Just 60) $ Left ("expected the fleet limit 60, got: " <> show receipt.concurrency)
      unless (receipt.partitionConcurrency == Just 4) $ Left ("expected the partition limit 4, got: " <> show receipt.partitionConcurrency)
      unless (receipt.partitionWorkerConcurrency == Just 2) $ Left ("expected the partition worker limit 2, got: " <> show receipt.partitionWorkerConcurrency)
    other -> Left ("expected the sharded queue registered, got: " <> show other)
  case stored of
    Just record -> unless (record.queueRecordPartitionQueue) $ Left "expected the derived flag written"
    other -> Left ("expected the queue row, got: " <> show other)
  case updated of
    Right receipt -> do
      unless (queueIsPartitioned receipt == False) $ Left "expected un-partitioned again"
      unless (receipt.concurrency == Just 60) $ Left ("expected the fleet limit kept, got: " <> show receipt.concurrency)
    other -> Left ("expected the partition limits cleared, got: " <> show other)
  case storedAgain of
    Just record -> unless (record.queueRecordPartitionQueue == False) $ Left "expected the flag to follow the limits back off"
    other -> Left ("expected the queue row, got: " <> show other)

-- | Re-registering updates the stored limits in place.
scenarioReregister :: forall m. (MonadSTM m, MonadMVar m)
                   => QueueFixture m -> m (Either (Error EngineOnly) Queue, Maybe Queue)
scenarioReregister fx = do
  _ <- fx.qfLaunch
  let queueName = "hs-l2-reregister-q-" <> Text.take 12 fx.qfSuffix
  _ <- registerQueue fx.qfDBOS queueName (defaultQueueOptions {concurrency = Just 3}) UpdateIfLatestVersion >>= either (error . show) pure
  second <- registerQueue fx.qfDBOS queueName (defaultQueueOptions {concurrency = Just 7}) UpdateIfLatestVersion
  stored <- queue fx.qfDBOS queueName >>= either (error . show) pure
  fx.qfShutdown
  pure (second, stored)

checkReregister :: (Either (Error EngineOnly) Queue, Maybe Queue) -> Either String ()
checkReregister (second, stored) = do
  case second of
    Right receipt -> unless (receipt.concurrency == Just 7) $ Left ("expected the re-registered limit 7, got: " <> show receipt.concurrency)
    other -> Left ("expected the re-registration accepted, got: " <> show other)
  case stored of
    Just receipt -> unless (receipt.concurrency == Just 7) $ Left ("expected the updated row, got: " <> show receipt.concurrency)
    other -> Left ("expected the updated row, got: " <> show other)

-- | A legacy partitioned row resolves through the facade, and adding a
-- per-partition limit to it is refused as the deprecated flag.
scenarioLegacyUpdateRefused :: forall m. (MonadSTM m, MonadMVar m)
                            => QueueFixture m -> m (Either SysDB.Error Bool, Maybe Queue, Either (Error EngineOnly) Queue)
scenarioLegacyUpdateRefused fx = do
  let queueName = "hs-l2-legacy-q-" <> Text.take 12 fx.qfSuffix
  written <- fx.qfUpsertQueue ((newQueue queueName) {newQueueConcurrency = Just 1, newQueueWorkerConcurrency = Just 1, newQueuePartitionQueue = True, newQueueAppName = Just fx.qfAppName}) UpdateExisting
  _ <- fx.qfLaunch
  stored <- queue fx.qfDBOS queueName >>= either (error . show) pure
  refused <- updateQueue fx.qfDBOS queueName (defaultQueueChange {partitionConcurrency = Set (Just 4)})
  fx.qfShutdown
  pure (written, stored, refused)

checkLegacyUpdateRefused :: (Either SysDB.Error Bool, Maybe Queue, Either (Error EngineOnly) Queue) -> Either String ()
checkLegacyUpdateRefused (written, stored, refused) = do
  case written of
    Right _ -> Right ()
    other -> Left ("could not write the legacy row: " <> show other)
  case stored of
    Just receipt -> do
      unless (queueIsPartitioned receipt) $ Left "expected the flag to re-scope the row"
      unless (receipt.partitionConcurrency == Just 1) $ Left ("expected the partition limit 1, got: " <> show receipt.partitionConcurrency)
      unless (receipt.partitionWorkerConcurrency == Just 1) $ Left ("expected the partition worker limit 1, got: " <> show receipt.partitionWorkerConcurrency)
      unless (receipt.concurrency == Nothing) $ Left ("expected no fleet limit, got: " <> show receipt.concurrency)
    other -> Left ("expected the legacy row, got: " <> show other)
  case refused of
    Left (ErrorConfig message) ->
      unless ("deprecated `partition_queue`" `Text.isInfixOf` message) $ Left ("the refusal names no deprecated flag, got: " <> show message)
    other -> Left ("expected the update refused, got: " <> show other)

-- * Execution and row reads (slice 2)

-- | Abort on an engine-channel failure, naming it. 'startWorkflow'
-- leaves its error channel free, so the call sites pin it here once
-- instead of annotating every start.
orCrash :: forall m a. Applicative m => Either (Error EngineOnly) a -> m a
orCrash = either (error . show) pure

-- | The recorded result a finished run reports, decoded as the body wrote
-- it. Anything else is a verdict, not a setup failure.
decodedInt :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue) -> Either String Int
decodedInt outcome = case outcome of
  Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
    Right value -> Right value
    Left err -> Left ("expected the result to decode, got: " <> show err)
  other -> Left ("expected a recorded result, got: " <> show other)

-- | The recorded name a finished run reports, decoded as the body wrote it.
decodedText :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue) -> Either String Text
decodedText outcome = case outcome of
  Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
    Right value -> Right value
    Left err -> Left ("expected the result to decode, got: " <> show err)
  other -> Left ("expected a recorded result, got: " <> show other)

-- | A queue registers through the instance, runs an enqueued workflow to
-- completion, replays it through a direct run, honours NeverUpdate, takes
-- an update, lists, reads back, deletes, and the launch reads back.
scenarioCrud :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
             => QueueFixture m -> m (Either (Error EngineOnly) Queue, WorkflowStatus, Either String Int, Either String Int, Either (Error EngineOnly) Queue, Either (Error EngineOnly) Queue, Bool, Maybe Queue, Either (Error EngineOnly) (), Bool)
scenarioCrud fx = do
  let key = newQualifiedWorkflowName "queued"
      body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body input _ = pure (Right input)
  _ <- registerWorkflow fx.qfDBOS key body >>= either (error . show) pure
  exec <- fx.qfLaunch
  let queueName = "hs-l2-queue-" <> Text.take 12 fx.qfSuffix
      options =
        QueueOptions
          { concurrency = Nothing,
            globalConcurrency = Nothing,
            workerConcurrency = Just 3,
            pollingInterval = secondsDuration 1,
            rateLimit = Nothing,
            partitionConcurrency = Nothing,
            partitionWorkerConcurrency = Nothing,
            partitionRateLimit = Nothing
          }
      workflowId = WorkflowId ("hs-l2-enqueue-" <> fx.qfSuffix)
      input = encodeWorkflowInput (7 :: Int)
  registered <- registerQueue fx.qfDBOS queueName options AlwaysUpdate
  enqueued <- enqueueWorkflow fx.qfDBOS key workflowId (Just input) queueName >>= either (error . show) pure
  waited <- waitForWorkflow fx.qfDBOS workflowId >>= either (error . show) pure
  let awaited = case waited of
        AwaitedSucceeded (Just output) serialization ->
          decodedInt (Right (Just (SerializedWorkflowValue output (Serialization <$> serialization))) :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
        other -> Left ("expected the queued run to succeed, got: " <> show other)
  rerun <- runWorkflow exec key workflowId (Just input)
  let replayed = decodedInt rerun
  leftAlone <- registerQueue fx.qfDBOS queueName defaultQueueOptions NeverUpdate
  let change =
        QueueChange
          { concurrency = Leave,
            globalConcurrency = Leave,
            workerConcurrency = Set (Just 2),
            pollingInterval = Leave,
            rateLimit = Leave,
            partitionConcurrency = Leave,
            partitionWorkerConcurrency = Leave,
            partitionRateLimit = Leave
          }
  updated <- updateQueue fx.qfDBOS queueName change
  listed <- listQueues fx.qfDBOS >>= either (error . show) pure
  fetched <- queue fx.qfDBOS queueName >>= either (error . show) pure
  removed <- deleteQueue fx.qfDBOS queueName
  launched <- isLaunched fx.qfDBOS
  fx.qfShutdown
  pure (registered, enqueued.initResultStatus, awaited, replayed, leftAlone, updated, queueName `elem` map (.name) listed, fetched, removed, launched)

checkCrud :: (Either (Error EngineOnly) Queue, WorkflowStatus, Either String Int, Either String Int, Either (Error EngineOnly) Queue, Either (Error EngineOnly) Queue, Bool, Maybe Queue, Either (Error EngineOnly) (), Bool) -> Either String ()
checkCrud (registered, enqueued, awaited, replayed, leftAlone, updated, listed, fetched, removed, launched) = do
  case registered of
    Right receipt -> unless (receipt.workerConcurrency == Just 3) $ Left ("expected the worker limit 3, got: " <> show receipt.workerConcurrency)
    other -> Left ("expected the queue registered, got: " <> show other)
  unless (enqueued == Enqueued) $ Left ("expected the enqueue admitted, got: " <> show enqueued)
  unless (awaited == Right 7) $ Left ("expected the supervisor to run 7, got: " <> show awaited)
  unless (replayed == Right 7) $ Left ("expected the direct run to replay 7, got: " <> show replayed)
  case leftAlone of
    Right receipt -> unless (receipt.workerConcurrency == Just 3) $ Left ("expected NeverUpdate to keep 3, got: " <> show receipt.workerConcurrency)
    other -> Left ("expected the re-registration to leave the row, got: " <> show other)
  case updated of
    Right receipt -> unless (receipt.workerConcurrency == Just 2) $ Left ("expected the update to land 2, got: " <> show receipt.workerConcurrency)
    other -> Left ("expected the limits updated, got: " <> show other)
  unless listed $ Left "expected the application's queue listed"
  case fetched of
    Just receipt -> unless (receipt.workerConcurrency == Just 2) $ Left ("expected the read to see 2, got: " <> show receipt.workerConcurrency)
    other -> Left ("expected the registered queue read back, got: " <> show other)
  unless (removed == Right ()) $ Left ("expected delete to succeed, got: " <> show removed)
  unless launched $ Left "expected shutdown to see the executor"

-- | A dequeue stamps the deadline the enqueue left open: the budget the
-- start carried is the timeout on the row, and the row carries an expiry
-- only after a worker takes it.
scenarioDeadlineStamped :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                        => QueueFixture m -> m (Either String Int, Maybe Duration, Maybe Timestamp)
scenarioDeadlineStamped fx = do
  let key = newQualifiedWorkflowName "budgeted"
      body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = pure (Right (1 :: Int))
  ref <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  exec <- fx.qfLaunch
  let queueName = "hs-l2-deadline-q-" <> Text.take 12 fx.qfSuffix
      workflowText = "budget-starts-on-dequeue-" <> fx.qfSuffix
  _ <- registerQueue fx.qfDBOS queueName defaultQueueOptions UpdateIfLatestVersion >>= either (error . show) pure
  let options =
        startOptionsDefault
          { startWorkflowId = Just (WorkflowId workflowText),
            startQueue = Just (queueRequestNew queueName),
            startTimeout = Explicit (secondsDuration 300)
          }
  started <- startWorkflow exec ref options Nothing >>= orCrash
  ran <- handleResult started
  let result = decodedInt ran
  found <- fx.qfReadWorkflowRow (WorkflowId workflowText) >>= maybe (error "expected the queued row") pure
  fx.qfShutdown
  pure (result, found.workflowRecordTimeout, found.workflowRecordDeadline)

checkDeadlineStamped :: (Either String Int, Maybe Duration, Maybe Timestamp) -> Either String ()
checkDeadlineStamped (result, stamped, deadline) = do
  unless (result == Right 1) $ Left ("expected the queued run to finish, got: " <> show result)
  unless (stamped == Just (secondsDuration 300)) $ Left ("expected the budget recorded, got: " <> show stamped)
  unless (deadline /= Nothing) $ Left "expected the dequeue to arm the expiry"

-- | The same budget with no worker behind it records no deadline yet: the
-- row stays as the enqueue left it.
scenarioNoDeadlineYet :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                      => QueueFixture m -> m (Maybe Duration, Maybe Timestamp)
scenarioNoDeadlineYet fx = do
  let key = newQualifiedWorkflowName "queued"
      body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = pure (Right (1 :: Int))
  ref <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  exec <- fx.qfLaunch
  let queueName = "hs-l2-unpolled-q-" <> Text.take 12 fx.qfSuffix
      workflowText = "queued-with-a-budget-" <> fx.qfSuffix
      options =
        startOptionsDefault
          { startWorkflowId = Just (WorkflowId workflowText),
            startQueue = Just (queueRequestNew queueName),
            startTimeout = Explicit (secondsDuration 300)
          }
  -- No register_queue anywhere: the row must stay as the enqueue left
  -- it, so the read follows the start at once.
  _ <- startWorkflow exec ref options Nothing >>= orCrash
  found <- fx.qfReadWorkflowRow (WorkflowId workflowText) >>= maybe (error "expected the queued row") pure
  fx.qfShutdown
  pure (found.workflowRecordTimeout, found.workflowRecordDeadline)

checkNoDeadlineYet :: (Maybe Duration, Maybe Timestamp) -> Either String ()
checkNoDeadlineYet (stamped, deadline) = do
  unless (stamped == Just (secondsDuration 300)) $ Left ("expected the budget recorded, got: " <> show stamped)
  unless (deadline == Nothing) $ Left "expected no expiry before a dequeue"

-- | The partition key and the priority the enqueue carried are recorded on
-- the row.
scenarioPartitionRow :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                     => QueueFixture m -> m (Maybe WorkflowRecord)
scenarioPartitionRow fx = do
  let key = newQualifiedWorkflowName "partitioned"
      body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = pure (Right (1 :: Int))
  ref <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  exec <- fx.qfLaunch
  let queueName = "hs-l2-partition-q-" <> Text.take 12 fx.qfSuffix
      workflowText = "sharded-" <> fx.qfSuffix
  _ <- registerQueue fx.qfDBOS queueName defaultQueueOptions UpdateIfLatestVersion >>= either (error . show) pure
  let options =
        startOptionsDefault
          { startWorkflowId = Just (WorkflowId workflowText),
            startQueue = Just ((queueRequestNew queueName) {partitionKey = Just "tenant-7", priority = Just 4})
          }
  _ <- startWorkflow exec ref options Nothing >>= orCrash
  found <- fx.qfReadWorkflowRow (WorkflowId workflowText)
  fx.qfShutdown
  pure found

checkPartitionRow :: Maybe WorkflowRecord -> Either String ()
checkPartitionRow found = case found of
  -- The queue name is scaffolding the scenario built, not engine behavior,
  -- so the check judges only what the enqueue recorded.
  Just row -> do
    unless (row.workflowRecordQueuePartitionKey == Just "tenant-7") $ Left ("expected the partition key recorded, got: " <> show row.workflowRecordQueuePartitionKey)
    unless (row.workflowRecordPriority == 4) $ Left ("expected the priority recorded, got: " <> show row.workflowRecordPriority)
  other -> Left ("expected the queued row, got: " <> show other)

-- | Without a priority the row stores the sentinel, and a delayed enqueue
-- carries neither a deduplication id nor a partition key.
scenarioSentinel :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                 => QueueFixture m -> m (Maybe WorkflowRecord)
scenarioSentinel fx = do
  let key = newQualifiedWorkflowName "plain"
      body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = pure (Right (1 :: Int))
  ref <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  exec <- fx.qfLaunch
  let queueName = "hs-l2-sentinel-q-" <> Text.take 12 fx.qfSuffix
      workflowText = "no-priority-" <> fx.qfSuffix
  _ <- registerQueue fx.qfDBOS queueName defaultQueueOptions UpdateIfLatestVersion >>= either (error . show) pure
  let options =
        startOptionsDefault
          { startWorkflowId = Just (WorkflowId workflowText),
            startQueue = Just ((queueRequestNew queueName) {delay = Just (secondsDuration 30)})
          }
  _ <- startWorkflow exec ref options Nothing >>= orCrash
  found <- fx.qfReadWorkflowRow (WorkflowId workflowText)
  fx.qfShutdown
  pure found

checkSentinel :: Maybe WorkflowRecord -> Either String ()
checkSentinel found = case found of
  Just row -> do
    unless (row.workflowRecordPriority == 0) $ Left ("expected the sentinel priority, got: " <> show row.workflowRecordPriority)
    unless (row.workflowRecordDeduplicationId == Nothing) $ Left ("expected no deduplication id, got: " <> show row.workflowRecordDeduplicationId)
    unless (row.workflowRecordQueuePartitionKey == Nothing) $ Left ("expected no partition key, got: " <> show row.workflowRecordQueuePartitionKey)
  other -> Left ("expected the queued row, got: " <> show other)

-- | Past what the priority column holds, the enqueue is refused before
-- anything is written: a bad enqueue costs a round trip, not a row.
scenarioBadEnqueue :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                   => QueueFixture m -> m (Either (Error EngineOnly) Text, [WorkflowRecord])
scenarioBadEnqueue fx = do
  let key = newQualifiedWorkflowName "checked"
      body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = pure (Right (1 :: Int))
  ref <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  exec <- fx.qfLaunch
  let queueName = "hs-l2-validation-q-" <> Text.take 12 fx.qfSuffix
  _ <- registerQueue fx.qfDBOS queueName defaultQueueOptions UpdateIfLatestVersion >>= either (error . show) pure
  -- Past what the priority column holds: i32 max plus one.
  let bad = (queueRequestNew queueName) {priority = Just 2147483648}
  started <- fmap (fmap (.workflowId)) (startWorkflow exec ref (startOptionsDefault {startQueue = Just bad}) Nothing)
  listed <- listWorkflows fx.qfDBOS (defaultWorkflowFilter {workflowFilterQueueNames = [queueName]}) >>= either (error . show) pure
  fx.qfShutdown
  pure (started, listed)

checkBadEnqueue :: (Either (Error EngineOnly) Text, [WorkflowRecord]) -> Either String ()
checkBadEnqueue (started, listed) = do
  case started of
    Left (InvalidArgument operation detail) -> do
      unless (operation == "enqueue a workflow") $ Left ("expected the enqueue operation, got: " <> show operation)
      unless ("`priority` must be at most 2147483647" `Text.isInfixOf` detail) $ Left ("expected the priority refusal, got: " <> show detail)
    Right workflowId -> Left ("expected a priority refusal, but the enqueue created: " <> show workflowId)
    other -> Left ("expected a priority refusal, got: " <> show other)
  case listed of
    [] -> pure ()
    other -> Left ("a refused enqueue wrote rows anyway: " <> show other)

-- | A stored internal-queue row cannot redefine the internal queue: the
-- engine ignores the stored limits and still runs the workflow.
scenarioInternalRow :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                    => QueueFixture m -> m (Either String Int)
scenarioInternalRow fx = do
  let key = newQualifiedWorkflowName "internal"
      body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = pure (Right (9 :: Int))
      QueueName internalName = internalQueueName
  ref <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  -- Ownerless on purpose: the internal row is a global singleton, so no
  -- single run can own it; leaving the owner null keeps the upsert
  -- repeatable while the stored 300s interval still proves the engine
  -- ignores it.
  _ <- fx.qfUpsertQueue ((newQueue internalName) {newQueuePollingInterval = secondsDuration 300, newQueueWorkerConcurrency = Just 1, newQueueAppName = Nothing}) UpdateExisting >>= either (error . show) pure
  exec <- fx.qfLaunch
  let workflowText = "on-the-internal-queue-" <> fx.qfSuffix
  started <- startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just (WorkflowId workflowText), startQueue = Just (queueRequestNew internalName)}) Nothing >>= orCrash
  ran <- handleResult started
  fx.qfShutdown
  pure (decodedInt ran)

checkInternalRow :: Either String Int -> Either String ()
checkInternalRow result = unless (result == Right 9) $ Left ("expected the internal queue to run 9, got: " <> show result)

-- | A queue registered after the launch is still dequeued from: the
-- supervisor picks up queues it did not start with.
scenarioLateQueue :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                  => QueueFixture m -> m (Either String Int)
scenarioLateQueue fx = do
  let key = newQualifiedWorkflowName "late"
      body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = pure (Right (1 :: Int))
  ref <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  exec <- fx.qfLaunch
  let queueName = "hs-l2-late-q-" <> Text.take 12 fx.qfSuffix
      workflowText = "late-run-" <> fx.qfSuffix
  _ <- registerQueue fx.qfDBOS queueName defaultQueueOptions UpdateIfLatestVersion >>= either (error . show) pure
  started <- startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just (WorkflowId workflowText), startQueue = Just (queueRequestNew queueName)}) Nothing >>= orCrash
  ran <- handleResult started
  fx.qfShutdown
  pure (decodedInt ran)

checkLateQueue :: Either String Int -> Either String ()
checkLateQueue result = unless (result == Right 1) $ Left ("expected the late queue to run 1, got: " <> show result)

-- | A queue this process never registered is still dequeued from: the
-- worker set comes from the table, never from what this instance
-- registered. The row is written straight to the database.
scenarioGhostQueue :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                   => QueueFixture m -> m (Either String Int)
scenarioGhostQueue fx = do
  let key = newQualifiedWorkflowName "ghost"
      body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = pure (Right (2 :: Int))
  ref <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  -- The row is written straight to the database: no register_queue
  -- anywhere on this process.
  _ <- fx.qfUpsertQueue ((newQueue ("hs-l2-ghost-q-" <> Text.take 12 fx.qfSuffix)) {newQueueAppName = Just fx.qfAppName}) UpdateExisting >>= either (error . show) pure
  exec <- fx.qfLaunch
  let queueName = "hs-l2-ghost-q-" <> Text.take 12 fx.qfSuffix
      workflowText = "ghost-run-" <> fx.qfSuffix
  started <- startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just (WorkflowId workflowText), startQueue = Just (queueRequestNew queueName)}) Nothing >>= orCrash
  ran <- handleResult started
  fx.qfShutdown
  pure (decodedInt ran)

checkGhostQueue :: Either String Int -> Either String ()
checkGhostQueue result = unless (result == Right 2) $ Left ("expected the ghost queue to run 2, got: " <> show result)

-- | An inherited deadline reaches a queued child: the enqueue copies the
-- parent's instant, and the child carries no timeout of its own. The child
-- sits on a queue nothing polls: the assertion is about what the enqueue
-- wrote, so the row has to stay as the enqueue left it.
scenarioInheritedDeadline :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                          => QueueFixture m -> m (Maybe Timestamp, Maybe Timestamp, Maybe Duration)
scenarioInheritedDeadline fx = do
  let childKey = newQualifiedWorkflowName "child"
      parentKey = newQualifiedWorkflowName "parent"
      queueName = "hs-l2-inherited-q-" <> Text.take 12 fx.qfSuffix
      parentText = "hs-l2-inherited-parent-" <> fx.qfSuffix
      childText = "hs-l2-inherited-child-" <> fx.qfSuffix
      childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      childBody () _ = pure (Right (0 :: Int))
  childRef <- registerWorkflowRef fx.qfDBOS childKey childBody >>= either (error . show) pure
  let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
      parentBody () wctx = do
        startedChild <-
          startChildWorkflow
            wctx
            childRef
            (startOptionsDefault {startWorkflowId = Just (WorkflowId childText), startQueue = Just (queueRequestNew queueName)})
            Nothing
        case startedChild of
          Left err -> pure (Left err)
          Right child -> pure (Right child.workflowId)
  parentRef <- registerWorkflowRef fx.qfDBOS parentKey parentBody >>= either (error . show) pure
  exec <- fx.qfLaunch
  _ <-
    runWorkflowRef
      exec
      parentRef
      (runOptionsDefault {runWorkflowId = Just (WorkflowId parentText), runTimeout = Explicit (secondsDuration 300)})
      (Just (encodeWorkflowInput ()))
    >>= either (error . show) pure
  parent <- fx.qfReadWorkflowRow (WorkflowId parentText) >>= maybe (error "expected the parent row") pure
  child <- fx.qfReadWorkflowRow (WorkflowId childText) >>= maybe (error "expected the child row") pure
  fx.qfShutdown
  pure (parent.workflowRecordDeadline, child.workflowRecordDeadline, child.workflowRecordTimeout)

checkInheritedDeadline :: (Maybe Timestamp, Maybe Timestamp, Maybe Duration) -> Either String ()
checkInheritedDeadline (parentDeadline, childDeadline, childTimeout) = do
  unless (childDeadline == parentDeadline) $ Left "expected the child to inherit the deadline"
  unless (childTimeout == Nothing) $ Left ("expected the child to carry no timeout, got: " <> show childTimeout)

-- * Concurrency, timing, and scoping (slice 3)

-- | Polls a condition until it holds or the budget runs out. Virtual time
-- under IOSim, wall clock on IO.
pollUntil :: MonadDelay m => Int -> m Bool -> m Bool
pollUntil remaining cond
  | remaining <= 0 = cond
  | otherwise = do
      ok <- cond
      if ok
        then pure True
        else threadDelay 100000 >> pollUntil (remaining - 100000) cond

-- | A queue's worker concurrency runs that many at once in one process:
-- three gated bodies, two running together, the peak never above the
-- budget.
scenarioWorkerConcurrency :: forall m. (MonadMVar m, MonadSTM m, MonadDelay m, MonadTime m)
                          => QueueFixture m -> m (Bool, Int)
scenarioWorkerConcurrency fx = do
  gate <- newTVarIO False
  active <- newTVarIO (0 :: Int)
  peak <- newTVarIO (0 :: Int)
  let key = newQualifiedWorkflowName "blocking"
      body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body input _ = do
        atomically $ do
          now <- readTVar active
          let running = now + 1
          writeTVar active running
          high <- readTVar peak
          when (running > high) (writeTVar peak running)
        atomically $ do
          open <- readTVar gate
          if open then pure () else retry
        atomically (modifyTVar active (subtract 1))
        pure (Right input)
  _ <- registerWorkflow fx.qfDBOS key body >>= either (error . show) pure
  _ <- fx.qfLaunch
  let queueName = "hs-l2-queueconc-" <> Text.take 12 fx.qfSuffix
      workflowIds = [WorkflowId ("hs-l2-queueconc-" <> fx.qfSuffix <> "-" <> Text.pack (show n)) | n <- [1 :: Int, 2, 3]]
      input = encodeWorkflowInput (7 :: Int)
  _ <- registerQueue fx.qfDBOS queueName (defaultQueueOptions {workerConcurrency = Just 2}) AlwaysUpdate >>= either (error . show) pure
  mapM_
    ( \workflowId -> do
        _ <- enqueueWorkflow fx.qfDBOS key workflowId (Just input) queueName >>= either (error . show) pure
        pure ()
    )
    workflowIds
  reachedTwo <- pollUntil (10 * 1000000) (atomically (readTVar peak) >>= \high -> pure (high >= 2))
  atomically (writeTVar gate True)
  mapM_
    ( \workflowId -> do
        waited <- waitForWorkflow fx.qfDBOS workflowId >>= either (error . show) pure
        case waited of
          AwaitedSucceeded _ _ -> pure ()
          other -> error ("expected the queued workflows to succeed, got: " <> show other)
    )
    workflowIds
  high <- readTVarIO peak
  fx.qfShutdown
  pure (reachedTwo, high)

checkWorkerConcurrency :: (Bool, Int) -> Either String ()
checkWorkerConcurrency (reachedTwo, high) = do
  unless reachedTwo $ Left "expected the worker concurrency to let two run at once"
  unless (high == 2) $ Left ("expected the local worker budget never exceeded, got: " <> show high)

-- | Listen queues narrow what this process dequeues: the listened queue
-- runs, the unlistened one stays ENQUEUED for a peer that does listen to
-- it. The leaf staffs the supervisor with @Just [fastQueue]@.
scenarioListenNarrow :: forall m. (MonadMVar m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                     => QueueFixture m -> m (Either String Int, Maybe WorkflowStatus)
scenarioListenNarrow fx = do
  let key = newQualifiedWorkflowName "either"
      body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body input _ = pure (Right input)
      fastQueue = "hs-l2-listen-fast-" <> Text.take 12 fx.qfSuffix
      slowQueue = "hs-l2-listen-slow-" <> Text.take 12 fx.qfSuffix
      fastText = "hs-l2-listen-fast-wf-" <> fx.qfSuffix
      slowText = "hs-l2-listen-slow-wf-" <> fx.qfSuffix
  _ <- registerWorkflow fx.qfDBOS key body >>= either (error . show) pure
  _ <- fx.qfLaunch
  mapM_
    ( \queueName -> do
        _ <- registerQueue fx.qfDBOS queueName defaultQueueOptions AlwaysUpdate >>= either (error . show) pure
        pure ()
    )
    [fastQueue, slowQueue]
  let enqueueOne wid input queueName = do
        _ <- enqueueWorkflow fx.qfDBOS key wid (Just (encodeWorkflowInput (input :: Int))) queueName >>= either (error . show) pure
        pure ()
  enqueueOne (WorkflowId fastText) 1 fastQueue
  enqueueOne (WorkflowId slowText) 2 slowQueue
  fasted <- timeout 15000000 (waitForWorkflow fx.qfDBOS (WorkflowId fastText)) >>= maybe (error "expected the listened workflow to run") pure >>= either (error . show) pure
  let fast = case fasted of
        AwaitedSucceeded (Just output) _ ->
          decodedInt (Right (Just (SerializedWorkflowValue output Nothing)) :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
        other -> Left ("expected the listened workflow to run, got: " <> show other)
  slow <- retrieveWorkflow fx.qfDBOS (WorkflowId slowText) >>= orCrash >>= handleStatus >>= orCrash
  fx.qfShutdown
  pure (fast, slow)

checkListenNarrow :: (Either String Int, Maybe WorkflowStatus) -> Either String ()
checkListenNarrow (fast, slow) = do
  unless (fast == Right 1) $ Left ("expected the listened queue to run, got: " <> show fast)
  unless (slow == Just Enqueued) $ Left ("expected the unlistened workflow ENQUEUED, got: " <> show slow)

-- | An empty listen set dequeues from no registered queue — but the
-- internal queue still runs, proving the loop is alive. The leaf staffs
-- the supervisor with @Just []@.
scenarioListenNone :: forall m. (MonadMVar m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                   => QueueFixture m -> m (Either String Int, Maybe WorkflowStatus)
scenarioListenNone fx = do
  let key = newQualifiedWorkflowName "nothing"
      body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body input _ = pure (Right input)
      ignoredQueue = "hs-l2-listen-ignored-" <> Text.take 12 fx.qfSuffix
      ignoredText = "hs-l2-listen-ignored-wf-" <> fx.qfSuffix
      internalText = "hs-l2-listen-internal-wf-" <> fx.qfSuffix
      QueueName internalName = internalQueueName
  _ <- registerWorkflow fx.qfDBOS key body >>= either (error . show) pure
  _ <- fx.qfLaunch
  _ <- registerQueue fx.qfDBOS ignoredQueue defaultQueueOptions AlwaysUpdate >>= either (error . show) pure
  let enqueueOne wid input queueName = do
        _ <- enqueueWorkflow fx.qfDBOS key wid (Just (encodeWorkflowInput (input :: Int))) queueName >>= either (error . show) pure
        pure ()
  enqueueOne (WorkflowId ignoredText) 1 ignoredQueue
  enqueueOne (WorkflowId internalText) 2 internalName
  -- The internal queue proves the loop is running at all, rather than the
  -- assertion below passing because nothing works.
  internaled <- timeout 15000000 (waitForWorkflow fx.qfDBOS (WorkflowId internalText)) >>= maybe (error "expected the internal workflow to run") pure >>= either (error . show) pure
  let internal = case internaled of
        AwaitedSucceeded (Just output) _ ->
          decodedInt (Right (Just (SerializedWorkflowValue output Nothing)) :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
        other -> Left ("expected the internal workflow to run, got: " <> show other)
  ignored <- retrieveWorkflow fx.qfDBOS (WorkflowId ignoredText) >>= orCrash >>= handleStatus >>= orCrash
  fx.qfShutdown
  pure (internal, ignored)

checkListenNone :: (Either String Int, Maybe WorkflowStatus) -> Either String ()
checkListenNone (internal, ignored) = do
  unless (internal == Right 2) $ Left ("expected the internal queue to run, got: " <> show internal)
  unless (ignored == Just Enqueued) $ Left ("expected the ignored workflow ENQUEUED, got: " <> show ignored)

-- | Listen queues never exclude the internal queue: under a filter naming
-- only another queue, the internal workflow still runs.
scenarioListenInternal :: forall m. (MonadMVar m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                       => QueueFixture m -> m (Either String Int)
scenarioListenInternal fx = do
  let key = newQualifiedWorkflowName "internal"
      body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body input _ = pure (Right input)
      internalText = "hs-l2-listen-int-wf-" <> fx.qfSuffix
      QueueName internalName = internalQueueName
  _ <- registerWorkflow fx.qfDBOS key body >>= either (error . show) pure
  _ <- fx.qfLaunch
  _ <- enqueueWorkflow fx.qfDBOS key (WorkflowId internalText) (Just (encodeWorkflowInput (4 :: Int))) internalName >>= either (error . show) pure
  internaled <- timeout 15000000 (waitForWorkflow fx.qfDBOS (WorkflowId internalText)) >>= maybe (error "expected the internal workflow to run") pure >>= either (error . show) pure
  fx.qfShutdown
  pure $ case internaled of
    AwaitedSucceeded (Just output) _ ->
      decodedInt (Right (Just (SerializedWorkflowValue output Nothing)) :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
    other -> Left ("expected the internal workflow to run under a filter, got: " <> show other)

checkListenInternal :: Either String Int -> Either String ()
checkListenInternal internal = unless (internal == Right 4) $ Left ("expected the internal queue to run 4, got: " <> show internal)

-- | A delayed enqueue waits before it is dequeued: comfortably inside the
-- delay and after several supervisor sweeps, no worker may have taken it.
scenarioDelayed :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                => QueueFixture m -> m (Maybe WorkflowStatus, Int, Either String Int)
scenarioDelayed fx = do
  ran <- newTVarIO (0 :: Int)
  let key = newQualifiedWorkflowName "delayed"
      body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = do
        atomically (modifyTVar ran (+ 1))
        pure (Right 7)
  ref <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  exec <- fx.qfLaunch
  let queueName = "hs-l2-delay-q-" <> Text.take 12 fx.qfSuffix
      workflowText = "held-back-" <> fx.qfSuffix
  _ <- registerQueue fx.qfDBOS queueName defaultQueueOptions UpdateIfLatestVersion >>= either (error . show) pure
  let options =
        startOptionsDefault
          { startWorkflowId = Just (WorkflowId workflowText),
            startQueue = Just ((queueRequestNew queueName) {delay = Just (secondsDuration 3)})
          }
  started <- startWorkflow exec ref options Nothing >>= orCrash
  status <- handleStatus started >>= orCrash
  threadDelay 1500000
  early <- readTVarIO ran
  finished <- handleResult started
  fx.qfShutdown
  pure (status, early, decodedInt finished)

checkDelayed :: (Maybe WorkflowStatus, Int, Either String Int) -> Either String ()
checkDelayed (status, early, finished) = do
  unless (status == Just Delayed) $ Left ("expected the delayed enqueue to wait, got: " <> show status)
  unless (early == 0) $ Left ("expected no run inside the delay, got: " <> show early)
  unless (finished == Right 7) $ Left ("expected the delayed workflow released, got: " <> show finished)

-- | A deduplication id admits one waiting workflow: the second enqueue is
-- refused naming the key, the first runs, and finishing releases the key.
scenarioDedup :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
              => QueueFixture m -> m (Text, Either String Int, Bool)
scenarioDedup fx = do
  let key = newQualifiedWorkflowName "deduped"
      body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = pure (Right (3 :: Int))
      queueName = "hs-l2-dedup-q-" <> Text.take 12 fx.qfSuffix
      firstText = "dedup-first-" <> fx.qfSuffix
      secondText = "dedup-second-" <> fx.qfSuffix
      thirdText = "dedup-third-" <> fx.qfSuffix
  ref <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  exec <- fx.qfLaunch
  _ <- registerQueue fx.qfDBOS queueName defaultQueueOptions UpdateIfLatestVersion >>= either (error . show) pure
  -- Delayed, so the first workflow is still holding the key when the
  -- second arrives.
  let held = (queueRequestNew queueName) {deduplicationId = Just "order-42", delay = Just (secondsDuration 3)}
  first <- startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just (WorkflowId firstText), startQueue = Just held}) Nothing >>= orCrash
  secondRun <- startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just (WorkflowId secondText), startQueue = Just held}) Nothing
  refusal <- case secondRun of
    Left err -> pure (Text.pack (displayException (err :: Error EngineOnly)))
    Right _ -> pure "a second workflow took a held deduplication key"
  firstResult <- handleResult first
  -- Finishing released the key, so the same one is enqueueable again.
  third <- startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just (WorkflowId thirdText), startQueue = Just ((queueRequestNew queueName) {deduplicationId = Just "order-42"})}) Nothing
  fx.qfShutdown
  pure (refusal, decodedInt firstResult, either (const False) (const True) third)

checkDedup :: (Text, Either String Int, Bool) -> Either String ()
checkDedup (refusal, first, third) = do
  unless ("order-42" `Text.isInfixOf` refusal) $ Left ("expected the refusal to name the key, got: " <> show refusal)
  unless (first == Right 3) $ Left ("expected the first workflow to run, got: " <> show first)
  unless third $ Left "expected the released key enqueueable again"

-- | ReturnExisting joins the workflow holding the key: the second handle
-- resolves to the first workflow, writes no row of its own, and both
-- handles read the one run.
scenarioJoin :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
             => QueueFixture m -> m (Text, Text, Maybe WorkflowRecord, Either String Int, Either String Int, Text, Text)
scenarioJoin fx = do
  let key = newQualifiedWorkflowName "deduped"
      body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = pure (Right (7 :: Int))
      queueName = "hs-l2-join-q-" <> Text.take 12 fx.qfSuffix
      firstText = "join-first-" <> fx.qfSuffix
      secondText = "join-second-" <> fx.qfSuffix
      thirdText = "join-third-" <> fx.qfSuffix
  ref <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  exec <- fx.qfLaunch
  _ <- registerQueue fx.qfDBOS queueName defaultQueueOptions UpdateIfLatestVersion >>= either (error . show) pure
  -- Delayed, so the holder is still waiting when the second caller
  -- arrives.
  let joining = (queueRequestNew queueName) {deduplicationId = Just "order-42", delay = Just (secondsDuration 3), duplicationPolicy = ReturnExisting}
  first <- startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just (WorkflowId firstText), startQueue = Just joining}) Nothing >>= orCrash
  second <- startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just (WorkflowId secondText), startQueue = Just joining}) Nothing >>= orCrash
  loser <- fx.qfReadWorkflowRow (WorkflowId secondText)
  firstResult <- handleResult first
  secondResult <- handleResult second
  -- The holder has finished, so the key is free and the same policy
  -- claims it rather than joining.
  third <- startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just (WorkflowId thirdText), startQueue = Just ((queueRequestNew queueName) {deduplicationId = Just "order-42", duplicationPolicy = ReturnExisting})}) Nothing >>= orCrash
  fx.qfShutdown
  pure (second.workflowId, firstText, loser, decodedInt firstResult, decodedInt secondResult, third.workflowId, thirdText)

checkJoin :: (Text, Text, Maybe WorkflowRecord, Either String Int, Either String Int, Text, Text) -> Either String ()
checkJoin (joined, firstId, loser, first, second, third, thirdId) = do
  unless (joined == firstId) $ Left ("expected the second enqueue to join the holder, got: " <> show joined)
  unless (loser == Nothing) $ Left ("expected the losing enqueue to write no row, got: " <> show loser)
  unless (first == Right 7 && second == Right 7) $ Left ("expected both handles to resolve 7, got: " <> show (first, second))
  unless (third == thirdId) $ Left ("expected the released key claimed under its own id, got: " <> show third)

-- | Priority orders the backlog lower first: enqueued before the queue is
-- registered — no worker exists for a queue with no row — the backlog
-- runs lowest-priority-number first once registration starts the worker.
scenarioPriority :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                 => QueueFixture m -> m [Text]
scenarioPriority fx = do
  order <- newTVarIO []
  let key = newQualifiedWorkflowName "ordered"
      body :: forall exec. Text -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
      body name _ = do
        atomically (modifyTVar order (++ [name]))
        pure (Right name)
      queueName = "hs-l2-priority-q-" <> Text.take 12 fx.qfSuffix
      submitted = [("low", Just 9), ("high", Just 1), ("none", Nothing), ("mid", Just 5)]
  ref <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  exec <- fx.qfLaunch
  -- Enqueued before the queue is registered, which is what holds the
  -- backlog back: no worker exists for a queue with no row.
  handles <- flip mapM submitted $ \(name, priority) -> do
    let workflowText = name <> "-" <> fx.qfSuffix
    startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just (WorkflowId workflowText), startQueue = Just ((queueRequestNew queueName) {priority = priority})}) (Just (encodeWorkflowInput name)) >>= orCrash
  -- The backlog is complete, so registering the queue is what starts its
  -- worker.
  _ <- registerQueue fx.qfDBOS queueName (defaultQueueOptions {workerConcurrency = Just 1}) UpdateIfLatestVersion >>= either (error . show) pure
  results <- mapM handleResult handles
  mapM_ (either (error . show) pure . decodedText) results
  fx.qfShutdown
  readTVarIO order

checkPriority :: [Text] -> Either String ()
checkPriority ran = unless (ran == ["none", "high", "mid", "low"]) $ Left ("expected the backlog lowest first, got: " <> show ran)

-- | Updating a queue changes what a running worker honours: one at a time
-- to begin with, three abreast after the update lands.
scenarioUpdateHonoured :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                       => QueueFixture m -> m (Int, Int)
scenarioUpdateHonoured fx = do
  active <- newTVarIO (0 :: Int)
  peak <- newTVarIO (0 :: Int)
  let key = newQualifiedWorkflowName "held"
      body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = do
        _ <- atomically $ do
          running <- readTVar active
          let running' = running + 1
          writeTVar active running'
          high <- readTVar peak
          when (running' > high) (writeTVar peak running')
          pure running'
        threadDelay 600000
        atomically (modifyTVar active (subtract 1))
        pure (Right 1)
      queueName = "hs-l2-update-q-" <> Text.take 12 fx.qfSuffix
  ref <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  exec <- fx.qfLaunch
  _ <- registerQueue fx.qfDBOS queueName (defaultQueueOptions {workerConcurrency = Just 1}) UpdateIfLatestVersion >>= either (error . show) pure
  handles <- flip mapM [0 .. 5 :: Int] $ \n -> do
    let workflowText = "fanned-" <> Text.pack (show n) <> "-" <> fx.qfSuffix
    startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just (WorkflowId workflowText), startQueue = Just (queueRequestNew queueName)}) Nothing >>= orCrash
  -- One at a time to begin with.
  threadDelay 1200000
  firstPeak <- readTVarIO peak
  _ <- updateQueue fx.qfDBOS queueName (defaultQueueChange {workerConcurrency = Set (Just 3)}) >>= either (error . show) pure
  results <- mapM handleResult handles
  mapM_ (either (error . show) pure) results
  lastPeak <- readTVarIO peak
  fx.qfShutdown
  pure (firstPeak, lastPeak)

checkUpdateHonoured :: (Int, Int) -> Either String ()
checkUpdateHonoured (firstPeak, lastPeak) = do
  unless (firstPeak == 1) $ Left ("expected one at a time first, got: " <> show firstPeak)
  unless (lastPeak == 3) $ Left ("expected three abreast after the update, got: " <> show lastPeak)

-- | A partitioned queue runs one workflow per key at a time while distinct
-- keys overlap.
scenarioPartitioned :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                    => QueueFixture m -> m (Int, Int)
scenarioPartitioned fx = do
  live <- newTVarIO Map.empty
  perKeyPeak <- newTVarIO (0 :: Int)
  overlapPeak <- newTVarIO (0 :: Int)
  let key = newQualifiedWorkflowName "sharded"
      body :: forall exec. Text -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
      body partition _ = do
        atomically $ do
          counts <- readTVar live
          let mine = 1 + Map.findWithDefault 0 partition counts
              counts' = Map.insert partition mine counts
          writeTVar live counts'
          high <- readTVar perKeyPeak
          when (mine > high) (writeTVar perKeyPeak mine)
          wide <- readTVar overlapPeak
          when (Map.size counts' > wide) (writeTVar overlapPeak (Map.size counts'))
        threadDelay 600000
        atomically $ do
          counts <- readTVar live
          case Map.lookup partition counts of
            Just 1 -> writeTVar live (Map.delete partition counts)
            Just n -> writeTVar live (Map.insert partition (n - 1) counts)
            Nothing -> pure ()
        pure (Right partition)
      queueName = "hs-l2-partitioned-q-" <> Text.take 12 fx.qfSuffix
  ref <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  exec <- fx.qfLaunch
  _ <- registerQueue fx.qfDBOS queueName (defaultQueueOptions {partitionConcurrency = Just 1}) UpdateIfLatestVersion >>= either (error . show) pure
  handles <- flip mapM ["tenant-a", "tenant-b"] $ \partition ->
    flip mapM [0 .. 1 :: Int] $ \n -> do
      let workflowText = partition <> "-" <> Text.pack (show n) <> "-" <> fx.qfSuffix
      startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just (WorkflowId workflowText), startQueue = Just ((queueRequestNew queueName) {partitionKey = Just partition})}) (Just (encodeWorkflowInput partition)) >>= orCrash
  results <- mapM (mapM handleResult) handles
  mapM_ (mapM_ (either (error . show) pure . decodedText)) results
  keyPeak <- readTVarIO perKeyPeak
  overlap <- readTVarIO overlapPeak
  fx.qfShutdown
  pure (keyPeak, overlap)

checkPartitioned :: (Int, Int) -> Either String ()
checkPartitioned (keyPeak, overlap) = do
  unless (keyPeak == 1) $ Left ("expected one per key, got: " <> show keyPeak)
  unless (overlap == 2) $ Left ("expected both keys overlapped, got: " <> show overlap)

-- | A counted partitioned queue runs its limit per key.
scenarioCountedPartitioned :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                           => QueueFixture m -> m Int
scenarioCountedPartitioned fx = do
  live <- newTVarIO Map.empty
  perKeyPeak <- newTVarIO (0 :: Int)
  let key = newQualifiedWorkflowName "sharded"
      body :: forall exec. Text -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
      body partition _ = do
        atomically $ do
          counts <- readTVar live
          let mine = 1 + Map.findWithDefault 0 partition counts
          writeTVar live (Map.insert partition mine counts)
          high <- readTVar perKeyPeak
          when (mine > high) (writeTVar perKeyPeak mine)
        threadDelay 600000
        atomically $ do
          counts <- readTVar live
          case Map.lookup partition counts of
            Just 1 -> writeTVar live (Map.delete partition counts)
            Just n -> writeTVar live (Map.insert partition (n - 1) counts)
            Nothing -> pure ()
        pure (Right partition)
      queueName = "hs-l2-counted-q-" <> Text.take 12 fx.qfSuffix
  ref <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  exec <- fx.qfLaunch
  _ <- registerQueue fx.qfDBOS queueName (defaultQueueOptions {partitionConcurrency = Just 2}) UpdateIfLatestVersion >>= either (error . show) pure
  handles <- flip mapM ["tenant-a", "tenant-b"] $ \partition ->
    flip mapM [0 .. 2 :: Int] $ \n -> do
      let workflowText = partition <> "-" <> Text.pack (show n) <> "-" <> fx.qfSuffix
      startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just (WorkflowId workflowText), startQueue = Just ((queueRequestNew queueName) {partitionKey = Just partition})}) (Just (encodeWorkflowInput partition)) >>= orCrash
  results <- mapM (mapM handleResult) handles
  mapM_ (mapM_ (either (error . show) pure)) results
  keyPeak <- readTVarIO perKeyPeak
  fx.qfShutdown
  pure keyPeak

checkCountedPartitioned :: Int -> Either String ()
checkCountedPartitioned keyPeak = unless (keyPeak == 2) $ Left ("expected two per key, got: " <> show keyPeak)

-- | The backlog one partition walk reads: a queue with one waiting row on
-- each of @count@ partitions, staged straight through the launched
-- executor's own connection — no workflow is registered, so whatever a
-- claim takes is skipped at dispatch and the walk's counts stand alone.
-- Staging runs through the executor (after its launch registers the
-- version) so the claims below read as the latest version.
stageWalkBacklog :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                 => Connection m -> Text -> NewQueue -> Int -> m QueueRecord
stageWalkBacklog conn suffix new count = do
  _ <- runSystemDB conn.connSysdb (\db -> SysDB.upsertQueue db new UpdateExisting) >>= either (error . show) pure
  let name = new.newQueueName
  mapM_
    ( \n -> do
        let wid = "walk-" <> Text.pack (show n) <> "-" <> suffix
            key = "p-" <> Text.pack (show n)
        runSystemDB conn.connSysdb (\db -> SysDB.initWorkflow db ((newWorkflow wid) {newWorkflowQueueName = Just name, newWorkflowQueuePartitionKey = Just key}) Nothing WorkflowIdReturnExisting) >>= either (error . show) pure
    )
    [0 .. count - 1]
  found <- runSystemDB conn.connSysdb (\db -> SysDB.getQueue db name) >>= either (error . show) pure
  maybe (error "walk queue missing after staging") pure found

-- | One row on @key@, claimed at once: a start from elsewhere that the
-- walk must reckon with. The claim presents the launched version, which
-- the launch registered as latest.
claimWalkRow :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
             => Connection m -> Identity -> Text -> Text -> Text -> m Dequeued
claimWalkRow conn identity queueName key suffix = do
  let wid = "walk-started-" <> key <> "-" <> suffix
  runSystemDB conn.connSysdb (\db -> SysDB.initWorkflow db ((newWorkflow wid) {newWorkflowQueueName = Just queueName, newWorkflowQueuePartitionKey = Just key}) Nothing WorkflowIdReturnExisting) >>= either (error . show) pure
  found <- runSystemDB conn.connSysdb (\db -> SysDB.getQueue db queueName) >>= either (error . show) pure
  record <- maybe (error "walk queue missing for the elsewhere start") pure found
  dequeued <- runSystemDB conn.connSysdb (\db -> SysDB.startQueuedWorkflows db record "exec" identity.identityAppVersion (Just key) 0 0 ("tok-" <> key)) >>= either (error . show) pure
  -- The elsewhere start takes exactly its row: anything else means the
  -- staging, not the walk, is broken, and the counts below would blame
  -- the wrong layer.
  case dequeued.dequeuedWorkflowIds of
    [_] -> pure dequeued
    ids -> error ("walk staging claimed " <> show (length ids) <> " rows, expected one")

-- | One poll of the named queue through the engine's own walk, with no
-- supervisor polling behind it (the leaf listens to nothing), so the
-- only claims are the walk's own.
pollWalk :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
         => QueueFixture m -> Executor m -> QueueRecord -> m Polled
pollWalk fx executor record = do
  running <- newRunning
  polled <- pollOnce executor.conn executor.identity executor.workflows executor.workflowTasks running record executor.datasources
  fx.qfShutdown
  pure polled

-- | A walk with the queue-wide concurrency spent visits no partition: the
-- room is read once up front, and none means the partitions are never
-- even listed.
scenarioWalkSpentConcurrency :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                             => QueueFixture m -> m Polled
scenarioWalkSpentConcurrency fx = do
  executor <- fx.qfLaunch
  let conn = executor.conn
      identity = executor.identity
      queueName = "hs-l2-walk-spent-q-" <> Text.take 12 fx.qfSuffix
  record <- stageWalkBacklog conn fx.qfSuffix ((newQueue queueName) {newQueueConcurrency = Just 1, newQueuePartitionConcurrency = Just 1}) 50
  -- A start from elsewhere spends the queue-wide room before the walk.
  _ <- claimWalkRow conn identity queueName "elsewhere" fx.qfSuffix
  pollWalk fx executor record

checkWalkSpentConcurrency :: Polled -> Either String ()
checkWalkSpentConcurrency polled = do
  unless (polled.polledClaims == 0) $ Left ("expected no partition visited, got: " <> show polled.polledClaims)
  unless (polled.polledContention == False) $ Left "expected no contention"
  unless (null polled.polledClaimed) $ Left ("expected nothing claimed, got: " <> show polled.polledClaimed)

-- | A spent queue-wide rate limit ends the walk on its first claim: the
-- pending count cannot see a rate limit, so one claim is paid to learn
-- it, and since it spans every partition the rest are not visited.
scenarioWalkSpentRateLimit :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                           => QueueFixture m -> m Polled
scenarioWalkSpentRateLimit fx = do
  executor <- fx.qfLaunch
  let conn = executor.conn
      identity = executor.identity
      queueName = "hs-l2-walk-rate-q-" <> Text.take 12 fx.qfSuffix
  record <- stageWalkBacklog conn fx.qfSuffix ((newQueue queueName) {newQueueRateLimit = Just (RateLimit {rateLimitLimit = 1, rateLimitPeriod = secondsDuration 3600}), newQueuePartitionConcurrency = Just 1}) 5
  -- The first claim fills the window; the walk's first claim finds it
  -- full and stops there.
  _ <- claimWalkRow conn identity queueName "p-0" fx.qfSuffix
  pollWalk fx executor record

checkWalkSpentRateLimit :: Polled -> Either String ()
checkWalkSpentRateLimit polled = do
  unless (polled.polledClaims == 1) $ Left ("expected one claim to learn the limit, got: " <> show polled.polledClaims)
  unless (null polled.polledClaimed) $ Left ("expected nothing claimed, got: " <> show polled.polledClaimed)

-- | A full partition does not end the walk: its own limit says nothing
-- about the next key, so the walk visits it and still claims every other
-- partition.
scenarioWalkFullPartition :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                          => QueueFixture m -> m Polled
scenarioWalkFullPartition fx = do
  executor <- fx.qfLaunch
  let conn = executor.conn
      identity = executor.identity
      queueName = "hs-l2-walk-full-q-" <> Text.take 12 fx.qfSuffix
  record <- stageWalkBacklog conn fx.qfSuffix ((newQueue queueName) {newQueueConcurrency = Just 10, newQueuePartitionConcurrency = Just 1}) 3
  -- One partition's concurrency is spent; the queue has room to spare.
  -- The elsewhere start fills p-0 either way: whichever of its two rows
  -- the claim takes, one runs and one stays waiting, so the full
  -- partition stays listed.
  _ <- claimWalkRow conn identity queueName "p-0" fx.qfSuffix
  pollWalk fx executor record

checkWalkFullPartition :: Polled -> Either String ()
checkWalkFullPartition polled = do
  unless (polled.polledClaims == 3) $ Left ("expected every partition visited, got: " <> show polled.polledClaims)
  unless (length polled.polledClaimed == 2) $ Left ("expected the two open partitions claimed, got: " <> show polled.polledClaimed)

-- | The walk stops once the queue-wide room it read up front is claimed.
scenarioWalkSpentRoom :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                      => QueueFixture m -> m Polled
scenarioWalkSpentRoom fx = do
  executor <- fx.qfLaunch
  let conn = executor.conn
      queueName = "hs-l2-walk-room-q-" <> Text.take 12 fx.qfSuffix
  record <- stageWalkBacklog conn fx.qfSuffix ((newQueue queueName) {newQueueConcurrency = Just 2, newQueuePartitionConcurrency = Just 1}) 3
  pollWalk fx executor record

checkWalkSpentRoom :: Polled -> Either String ()
checkWalkSpentRoom polled = do
  unless (polled.polledClaims == 2) $ Left ("expected the walk to stop after two claims, got: " <> show polled.polledClaims)
  unless (length polled.polledClaimed == 2) $ Left ("expected two rows claimed, got: " <> show polled.polledClaimed)

-- | Another application's queue is not dequeued from: several reconciles'
-- worth of waiting, the body never runs and the row stays ENQUEUED.
scenarioPeerQueue :: forall m. (MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                  => QueueFixture m -> m (Int, Maybe WorkflowStatus)
scenarioPeerQueue fx = do
  ran <- newTVarIO (0 :: Int)
  let key = newQualifiedWorkflowName "scoped"
      body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body () _ = do
        atomically (modifyTVar ran (+ 1))
        pure (Right 1)
      queueName = "belongs-to-a-peer-" <> Text.take 12 fx.qfSuffix
      peerName = "some-other-application-" <> Text.take 12 fx.qfSuffix
      workflowText = "enqueued-onto-a-peers-queue-" <> fx.qfSuffix
  ref <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  _ <- fx.qfUpsertQueue ((newQueue queueName) {newQueueAppName = Just peerName}) UpdateExisting >>= either (error . show) pure
  exec <- fx.qfLaunch
  _ <- startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just (WorkflowId workflowText), startQueue = Just (queueRequestNew queueName)}) Nothing >>= orCrash
  -- Several reconciles' worth: if this queue were going to enter the set,
  -- it would have.
  threadDelay 3000000
  early <- readTVarIO ran
  found <- fx.qfReadWorkflowRow (WorkflowId workflowText) >>= maybe (error "expected the queued row") pure
  fx.qfShutdown
  pure (early, Just found.workflowRecordStatus)

checkPeerQueue :: (Int, Maybe WorkflowStatus) -> Either String ()
checkPeerQueue (early, status) = do
  unless (early == 0) $ Left ("expected the peer queue untouched, got: " <> show early)
  unless (status == Just Enqueued) $ Left ("expected the workflow left ENQUEUED, got: " <> show status)

-- | A worker concurrency cap admits one run at a time: three rows wait on a
-- cap-one queue, and the supervisor runs them with at most one in flight
-- while every row eventually succeeds. A gate holds the runs until the peak
-- in-flight count pins the cap, and the settled statuses pin completion.
-- Returns the peak in-flight count with the settled statuses.
scenarioWorkerBudgetExhausted :: forall m. (MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m)
                              => QueueFixture m -> m (Int, [WorkflowStatus], [Maybe WorkflowStatus])
scenarioWorkerBudgetExhausted fx = do
  gate <- newTVarIO False
  active <- newTVarIO (0 :: Int)
  peak <- newTVarIO (0 :: Int)
  let key = newQualifiedWorkflowName "capped"
      body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body input _ = do
        atomically $ do
          now <- readTVar active
          let running = now + 1
          writeTVar active running
          high <- readTVar peak
          when (running > high) (writeTVar peak running)
        atomically $ do
          open <- readTVar gate
          if open then pure () else retry
        atomically (modifyTVar active (subtract 1))
        pure (Right input)
      queueName = "hs-l2-budget-q-" <> Text.take 12 fx.qfSuffix
      texts = ["hs-l2-budget-1-" <> fx.qfSuffix, "hs-l2-budget-2-" <> fx.qfSuffix, "hs-l2-budget-3-" <> fx.qfSuffix]
  _ <- registerWorkflow fx.qfDBOS key body >>= either (error . show) pure
  _ <- fx.qfLaunch
  _ <- registerQueue fx.qfDBOS queueName (defaultQueueOptions {workerConcurrency = Just 1}) AlwaysUpdate >>= either (error . show) pure
  mapM_
    ( \text -> do
        _ <- enqueueWorkflow fx.qfDBOS key (WorkflowId text) (Just (encodeWorkflowInput (1 :: Int))) queueName >>= either (error . show) pure
        pure ()
    )
    texts
  -- The supervisor claims the first row and parks it on the gate; the rest
  -- wait out the exhausted budget behind it.
  _ <- pollUntil (10 * 1000000) (atomically (readTVar active) >>= \running -> pure (running >= 1))
  waiting <- mapM (\text -> fx.qfReadWorkflowRow (WorkflowId text) >>= maybe (error "expected the queued row") (pure . (.workflowRecordStatus))) texts
  atomically (writeTVar gate True)
  _ <- pollUntil (30 * 1000000) (mapM (\text -> fx.qfReadWorkflowRow (WorkflowId text) >>= maybe (error "expected the queued row") (pure . (.workflowRecordStatus))) texts >>= \statuses -> pure (statuses == [Success, Success, Success]))
  high <- readTVarIO peak
  statuses <- mapM (\text -> fx.qfReadWorkflowRow (WorkflowId text) >>= maybe (error "expected the queued row") (pure . Just . (.workflowRecordStatus))) texts
  fx.qfShutdown
  pure (high, waiting, statuses)

checkWorkerBudgetExhausted :: (Int, [WorkflowStatus], [Maybe WorkflowStatus]) -> Either String ()
checkWorkerBudgetExhausted (high, waiting, statuses) = do
  unless (high == 1) $ Left ("expected at most one run in flight, got: " <> show high)
  unless (length waiting == 3 && length (filter (== Pending) waiting) == 1) $ Left ("expected one parked run behind two waiting rows, got: " <> show waiting)
  unless (statuses == [Just Success, Just Success, Just Success]) $ Left ("expected every row SUCCESS, got: " <> show statuses)

-- | A dispatch past the recovery budget parks the row without running
-- it: the attempts are spent behind the queue's back, so the claim that
-- takes the row counts one more and the dispatch parks it. Promoted from
-- the live-only twin; each stack plants the attempts its own way.
scenarioDispatchPark :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                     => QueueFixture m -> m (Int, Maybe WorkflowStatus, Int64)
scenarioDispatchPark fx = do
  runs <- newTVarIO (0 :: Int)
  let key = newQualifiedWorkflowName "never-runs"
      body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
      body () _ = atomically (modifyTVar runs (+ 1)) >> pure (Right ())
      queueName = "hs-l2-park-q-" <> Text.take 12 fx.qfSuffix
      workflowText = "hs-l2-park-run-" <> fx.qfSuffix
  _ <- registerWorkflowRef fx.qfDBOS key body >>= either (error . show) pure
  bracket fx.qfLaunch (\_ -> fx.qfShutdown) $ \_ -> do
    _ <- enqueueWorkflow fx.qfDBOS key (WorkflowId workflowText) (Just (encodeWorkflowInput ())) queueName >>= either (error . show) pure
    -- Spend the budget behind the queue's back: the claim counts one more,
    -- so the dispatch reads 151 against a limit of 100.
    fx.qfPlantAttempts (WorkflowId workflowText) 150
    _ <- registerQueue fx.qfDBOS queueName defaultQueueOptions UpdateIfLatestVersion >>= either (error . show) pure
    parkedRow <- pollUntil (30 * 1000000) (fx.qfReadWorkflowRow (WorkflowId workflowText) >>= \row -> pure (maybe False ((== MaxRecoveryAttemptsExceeded) . (.workflowRecordStatus)) row))
    unless parkedRow $ error "the row never parked"
    parked <- waitForWorkflow fx.qfDBOS (WorkflowId workflowText) >>= either (error . show) pure
    attempts <- case parked of
      AwaitedParked count -> pure count
      other -> error ("expected a parked outcome, got: " <> show other)
    status <- fx.qfReadWorkflowRow (WorkflowId workflowText) >>= \row -> pure ((.workflowRecordStatus) <$> row)
    ran <- readTVarIO runs
    pure (ran, status, attempts)

-- | The body never ran, the row parked, and the claim's count shows.
checkDispatchPark :: (Int, Maybe WorkflowStatus, Int64) -> Either String ()
checkDispatchPark (ran, status, attempts) = do
  unless (ran == 0) $ Left ("expected the body never to run, got: " <> show ran)
  unless (status == Just MaxRecoveryAttemptsExceeded) $ Left ("expected the row parked, got: " <> show status)
  unless (attempts == 151) $ Left ("expected the claim to count one more, got: " <> show attempts)
