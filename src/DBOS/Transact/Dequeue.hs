{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The Rust @dequeue.rs@ worker sweep: a supervisor that rebuilds the
-- queue set once a second, one tracked worker per queue whose interval
-- backs off under contention and scales back when clean, local running
-- tallies that bound worker concurrency without asking the database, and
-- concurrent dispatch of every claimed row.
module DBOS.Transact.Dequeue (dequeuePass, superviseForever) where

import DBOS.Prelude
import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM, StrictTVar, atomically, modifyTVar, newTVarIO, readTVar, readTVarIO, writeTVar)
import Control.Monad.Class.MonadFork (MonadFork)
import Control.Monad.Class.MonadThrow qualified as MThrow
import Control.Monad.Class.MonadTimer (MonadDelay, threadDelay)
import Control.Monad (forM_, unless, when)
import Control.Monad.Class.MonadThrow qualified as MThrow
import Colog.Core.Action (LogAction (..))
import Data.Bits (shiftL, shiftR, xor)
import Data.List (foldl')
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Word (Word32, Word64)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Error (BackendError (..), Error (..))
import DBOS.SystemDB.Types
  ( Applications (..),
    Duration (..),
    QueueName (..),
    QueueRecord (..),
    ResolvedLimits (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    WorkflowFilter (..),
    WorkflowId (..),
    WorkflowRecord (..),
    WorkflowStatus (..),
    defaultWorkflowFilter,
    internalQueueName,
    queueResolvedLimits,
    resolvedIsPartitioned,
    secondsDuration,
  )
import DBOS.Transact.Connection (Connection (..), runSystemDB)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Log (DbosLogMsg (..), DbosSeverity (..))
import DBOS.Transact.Registry (Snapshot, workflowKeyFromRow)
import DBOS.Transact.Workflow
  ( Tasks,
    spawnRegisteredWorkflowWithRow,
    spawnTracked,
    workflowNewWorkflow,
  )

-- | How often the supervisor rebuilds the queue set and transitions
-- delayed workflows.
supervisorInterval :: Duration
supervisorInterval = secondsDuration 1

-- | The ceiling a contended worker's polling interval backs off to.
maxPollingInterval :: Duration
maxPollingInterval = secondsDuration 120

-- | What a contended pass multiplies the polling interval by.
backoffFactor :: Double
backoffFactor = 2.0

-- | What a clean pass multiplies it by, walking it back towards the
-- queue's own interval.
scalebackFactor :: Double
scalebackFactor = 0.9

-- | The band every wait is jittered into. Not decorative: a fleet that
-- started together polls together forever without it.
jitterLow :: Double
jitterLow = 0.95

jitterHigh :: Double
jitterHigh = 1.05

-- | How many of a queue's workflows this process is currently running,
-- keyed by queue and by queue-and-partition, because the two worker
-- limits are enforced at different scopes and both are answered locally.
data Running m = Running
  { runningCounts :: StrictTVar m (Map Key Int)
  }

-- | What a local tally is kept under.
data Key = KeyQueue Text | KeyPartition Text Text
  deriving stock (Eq, Ord, Show)

-- | One workflow's place in the local tallies, released when its task
-- ends. A partition key counts it twice — once for the queue, once for
-- the partition — and the release drops both together.
data Slot m = Slot
  { slotRunning :: Running m,
    slotKeys :: [Key]
  }

newRunning :: MonadSTM m => m (Running m)
newRunning = Running <$> newTVarIO Map.empty

-- | Counts one workflow as running on a queue until the slot is released.
claimSlot :: MonadSTM m => Running m -> Text -> Maybe Text -> m (Slot m)
claimSlot running queue partition = do
  let keys = KeyQueue queue : maybe [] (\key -> [KeyPartition queue key]) partition
  atomically $ modifyTVar running.runningCounts (\counts -> foldl' (\acc key -> Map.insertWith (+) key 1 acc) counts keys)
  pure Slot {slotRunning = running, slotKeys = keys}

-- | Releases a slot, dropping keys at zero. A key at one is removed
-- rather than left at zero.
releaseSlot :: MonadSTM m => Slot m -> m ()
releaseSlot slot = atomically $ modifyTVar slot.slotRunning.runningCounts (\counts -> foldl' releaseOne counts slot.slotKeys)
  where
    releaseOne counts key = case Map.lookup key counts of
      Nothing -> counts
      Just count
        | count <= 1 -> Map.delete key counts
        | otherwise -> Map.insert key (count - 1) counts

runningCount :: MonadSTM m => Running m -> Text -> m Int
runningCount running queue = Map.findWithDefault 0 (KeyQueue queue) <$> readTVarIO running.runningCounts

runningPartitionCount :: MonadSTM m => Running m -> Text -> Text -> m Int
runningPartitionCount running queue partition =
  Map.findWithDefault 0 (KeyPartition queue partition) <$> readTVarIO running.runningCounts

-- | How many more workflows this process may start from a queue, or
-- 'Nothing' for unlimited. Local by construction: worker concurrency is
-- the limit a process can answer without asking the database. A
-- per-partition worker limit of zero pauses this worker outright.
workerBudget :: ResolvedLimits -> Int -> Maybe Int
workerBudget limits running
  | maybe False (<= 0) limits.resolvedPartitionWorkerConcurrency = Just 0
  | otherwise = (\cap -> max 0 (cap - running)) <$> limits.resolvedWorkerConcurrency

-- | One dequeue and the dispatch of whatever it claimed, reporting the
-- contended flag the caller backs off on.
pollOnce :: (MonadSTM m, MonadFork m, MThrow.MonadMask m, MonadTimer m, MonadTime m) => Connection m -> Identity -> Snapshot m -> Tasks m -> Running m -> QueueRecord -> LogAction m DbosLogMsg -> m (Bool, [WorkflowId])
pollOnce conn identity workflows tasks running queue logger = do
  if not (resolvedIsPartitioned limits)
    then do
      local <- runningCount running queueName
      claimed <- startClaim Nothing local 0
      case claimed of
        Left err -> do
          contended <- reportDequeueError logger err
          pure (contended, [])
        Right ids -> do
          dispatchClaimed conn identity workflows tasks running queue Nothing ids logger
          pure (False, ids)
    else do
      alreadyRunning <- runningCount running queueName
      case workerBudget limits alreadyRunning of
        Just 0 -> pure (False, [])
        _ | limits.resolvedPartitionConcurrency == Just 1
              && limits.resolvedConcurrency == Nothing
              && limits.resolvedRateLimit == Nothing
              && limits.resolvedPartitionRateLimit == Nothing -> do
              swept <-
                runSystemDB
                  conn.connSysdb
                  ( \db ->
                      SystemDB.startQueuedPartitionedWorkflows
                        db
                        queue
                        identity.identityExecutorId
                        identity.identityAppVersion
                        (fromIntegral <$> workerBudget limits alreadyRunning)
                  )
              case swept of
                Left err -> do
                  contended <- reportDequeueError logger err
                  pure (contended, [])
                Right ids -> do
                  -- The sweep returns one head per partition and does not
                  -- say which; each claimed row carries its own key, so the
                  -- tally is credited from the rows themselves.
                  dispatchClaimed conn identity workflows tasks running queue Nothing ids logger
                  pure (False, ids)
        _ -> do
          partitions <- runSystemDB conn.connSysdb (\db -> SystemDB.getQueuePartitions db queueName)
          case partitions of
            Left err -> do
              contended <- reportDequeueError logger err
              pure (contended, [])
            Right keys -> do
              seed <- conn.connEntropy
              walk (shuffled (fromIntegral seed) keys) alreadyRunning 0
              pure (False, [])
  where
    limits = queueResolvedLimits queue
    queueName = queue.queueRecordName
    startClaim partitionKey localRunning partitionLocalRunning =
      runSystemDB
        conn.connSysdb
        ( \db ->
            SystemDB.startQueuedWorkflows
              db
              queue
              identity.identityExecutorId
              identity.identityAppVersion
              partitionKey
              (fromIntegral localRunning)
              (fromIntegral partitionLocalRunning)
        )
    walk [] _ _ = pure ()
    walk (partition : rest) alreadyRunning claimedHere =
      case workerBudget limits (alreadyRunning + claimedHere) of
        Just 0 -> pure ()
        _ -> do
          local <- runningPartitionCount running queueName partition
          claimed <-
            runSystemDB
              conn.connSysdb
              ( \db ->
                  SystemDB.startQueuedWorkflows
                    db
                    queue
                    identity.identityExecutorId
                    identity.identityAppVersion
                    (Just partition)
                    (fromIntegral (alreadyRunning + claimedHere))
                    (fromIntegral local)
              )
          case claimed of
            Left err
              -- A peer holds this partition's rows. Skipping just this key
              -- is the point of walking them separately.
              | isContention err -> walk rest alreadyRunning claimedHere
              | otherwise -> do
                  _ <- reportDequeueError logger err
                  walk rest alreadyRunning claimedHere
            Right ids -> do
              dispatchClaimed conn identity workflows tasks running queue (Just partition) ids logger
              walk rest alreadyRunning (claimedHere + length ids)

-- | Turns a failed dequeue into the "was it contention" answer the caller
-- backs off on. A peer mid-dequeue is the system working, not a failure.
reportDequeueError :: Monad m => LogAction m DbosLogMsg -> SystemDB.Error -> m Bool
reportDequeueError logger err
  | isContention err = do
      unLogAction logger (DbosLogMsg DbosDebug "a peer is mid-dequeue; backing off" Nothing)
      pure True
  | otherwise = do
      unLogAction logger (DbosLogMsg DbosWarn ("could not dequeue from the queue: " <> SystemDB.renderError err) Nothing)
      pure False

-- | Whether a failed dequeue means a peer was mid-dequeue rather than
-- something being wrong. @55P03@ by code, not by class: a @NOWAIT@
-- conflict is @lock_not_available@ and never reaches the retry layer.
isContention :: SystemDB.Error -> Bool
isContention (Backend backend) = backend.backendSqlState == Just "55P03"
isContention _ = False

-- | Fisher-Yates, so a walk visits partitions in a different order each
-- poll: a worker whose budget runs out part way through would otherwise
-- always spend it on whichever keys sort first.
shuffled :: Word64 -> [Text] -> [Text]
shuffled seed keys = Map.elems (go (length keys - 1) (seed `xor` 1) initial)
  where
    initial = Map.fromList (zip [0 :: Int ..] keys)
    go i state current
      | i <= 0 = current
      | otherwise =
          let advanced = xorshift state
              j = fromIntegral (advanced `mod` fromIntegral (i + 1))
              left = current Map.! i
              right = current Map.! j
           in go (i - 1) advanced (Map.insert i right (Map.insert j left current))
    xorshift state =
      let first = state `xor` (state `shiftL` 13)
          second = first `xor` (first `shiftR` 7)
       in second `xor` (second `shiftL` 17)

-- | Reads the claimed workflows and starts each, crediting the local
-- tally before the dispatch so the next iteration's counts include it even
-- if this one is still starting. Walked in claim order, not in the order
-- the read came back, because claim order is what priority is for.
dispatchClaimed :: (MonadSTM m, MonadFork m, MThrow.MonadMask m, MonadTimer m, MonadTime m) => Connection m -> Identity -> Snapshot m -> Tasks m -> Running m -> QueueRecord -> Maybe Text -> [WorkflowId] -> LogAction m DbosLogMsg -> m ()
dispatchClaimed conn identity workflows tasks running queue partition claimed logger
  | null claimed = pure ()
  | otherwise = do
      fetched <-
        runSystemDB
          conn.connSysdb
          ( \db ->
              SystemDB.listWorkflows
                db
                (defaultWorkflowFilter {workflowFilterWorkflowIds = map (\(WorkflowId text) -> text) claimed, workflowFilterLoadOutput = False})
                Nothing
          )
      case fetched of
        Left err -> do
          -- The rows stay PENDING with this executor's id on them, which
          -- is what recovery is for.
          unLogAction logger (DbosLogMsg DbosWarn ("could not read the claimed workflows; they stay PENDING for recovery: " <> SystemDB.renderError err) Nothing)
        Right rows -> do
          when (length rows /= length claimed) $
            unLogAction logger (DbosLogMsg DbosWarn ("some claimed workflows have no row: claimed " <> showText (length claimed) <> ", found " <> showText (length rows)) Nothing)
          let byId = Map.fromList [(text, row) | row <- rows, let WorkflowId text = row.workflowRecordId]
          forM_ claimed $ \workflowId@(WorkflowId workflowText) -> case Map.lookup workflowText byId of
            Nothing -> pure ()
            Just row -> do
              let partition' = case partition of
                    Just key -> Just key
                    Nothing -> row.workflowRecordQueuePartitionKey
              slot <- claimSlot running queue.queueRecordName partition'
              case row.workflowRecordName of
                Nothing -> do
                  releaseSlot slot
                  unLogAction logger (DbosLogMsg DbosWarn ("the row names no workflow; skipped: " <> showText workflowId) Nothing)
                Just name -> do
                  let key = workflowKeyFromRow name row.workflowRecordClassName row.workflowRecordConfigName
                      input = (\raw -> SerializedWorkflowValue raw (Serialization <$> row.workflowRecordSerialization)) <$> row.workflowRecordInput
                      new = workflowNewWorkflow conn identity key workflowId input row.workflowRecordQueueName
                  spawned <-
                    spawnRegisteredWorkflowWithRow
                      tasks
                      (releaseSlot slot)
                      SystemDB.Dequeue
                      conn
                      identity
                      workflows
                      key
                      workflowId
                      input
                      new
                  case spawned of
                    Left err ->
                      unLogAction logger (DbosLogMsg DbosWarn ("could not start the dequeued workflow; it stays PENDING for recovery: " <> TransactError.renderTransactError err) Nothing)
                    Right _ -> pure ()

-- | Rebuilds and publishes the set of queues this process runs workers
-- for, returning their names. From the table, never from what this
-- instance registered; a transient read failure keeps the previous set
-- rather than emptying it.
refreshQueueSet :: MonadSTM m => Connection m -> Identity -> StrictTVar m (Map Text QueueRecord) -> StrictTVar m Bool -> LogAction m DbosLogMsg -> Maybe [Text] -> m [Text]
refreshQueueSet conn identity queues warnedInternal logger listenQueues = do
  listed <- runSystemDB conn.connSysdb (\db -> SystemDB.listQueues db Unset)
  case listed of
    Left err -> do
      unLogAction logger (DbosLogMsg DbosWarn ("could not list queues; keeping the current set: " <> SystemDB.renderError err) Nothing)
      Map.keys <$> readTVarIO queues
    Right records -> do
      warned <- readTVarIO warnedInternal
      let internalName = case internalQueueName of QueueName name -> name
          wanted record = case listenQueues of
            Nothing -> True
            Just names -> record.queueRecordName `elem` names
          step (current, warnedNow) record
            | record.queueRecordName == internalName = (current, warnedNow)
            | not (wanted record) = (current, warnedNow)
            | otherwise = (Map.insert record.queueRecordName record current, warnedNow)
          (current, _) = foldl' step (Map.fromList [(internalName, internalQueueRecord)], warned) records
          storedInternal = any (\record -> record.queueRecordName == internalName) records
      when (storedInternal && not warned) $ do
        atomically (writeTVar warnedInternal True)
        unLogAction logger (DbosLogMsg DbosWarn "the queues table holds a row for the engine's internal queue; its stored limits are ignored. Delete the row: it can only throttle `resume` and `fork`" Nothing)
      atomically (writeTVar queues current)
      pure (Map.keys current)

-- | The engine's own queue, which @resume@ and @fork@ put work on. No row,
-- no limits, and the default cadence.
internalQueueRecord :: QueueRecord
internalQueueRecord =
  QueueRecord
    { queueRecordName = case internalQueueName of QueueName name -> name,
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

-- | Polls one queue until its row leaves the published set, or shutdown
-- aborts the task. The interval is held across iterations, which is the
-- point: a contended queue stays backed off rather than rediscovering the
-- contention.
pollQueue :: (MonadSTM m, MonadDelay m, MonadFork m, MThrow.MonadMask m, MonadTimer m, MonadTime m) => Tasks m -> Connection m -> Identity -> Snapshot m -> StrictTVar m (Map Text QueueRecord) -> Running m -> Text -> LogAction m DbosLogMsg -> m ()
pollQueue tasks conn identity workflows queues running name logger = go (secondsDuration 1)
  where
    go interval = do
      current <- Map.lookup name <$> readTVarIO queues
      case current of
        Nothing -> unLogAction logger (DbosLogMsg DbosInfo "the queue is no longer registered; stopping its worker" Nothing)
        Just queue -> do
          -- The queue's own interval is the floor and the ceiling is
          -- derived from it; clamped rather than reset, so a backed-off
          -- worker keeps its backoff.
          let floorInterval = queue.queueRecordPollingInterval
              ceiling = max floorInterval maxPollingInterval
              clamped = max floorInterval (min ceiling interval)
          (contended, _) <- pollOnce conn identity workflows tasks running queue logger
          let next =
                if contended
                  then min (scaleDuration backoffFactor clamped) ceiling
                  else max (scaleDuration scalebackFactor clamped) floorInterval
          bits <- conn.connEntropy
          delayDuration (jitter bits next)
          go next

-- | The supervisor: transition, rebuild the set, spawn and reap one worker
-- per queue, and sleep a second.
superviseForever :: (MonadSTM m, MonadDelay m, MonadFork m, MThrow.MonadMask m, MonadTimer m, MonadTime m) => Tasks m -> Connection m -> Identity -> Snapshot m -> Maybe [Text] -> LogAction m DbosLogMsg -> m ()
superviseForever tasks conn identity workflows listenQueues logger = do
  queues <- newTVarIO Map.empty
  workers <- newTVarIO Map.empty
  warnedInternal <- newTVarIO False
  running <- newRunning
  loop queues workers running warnedInternal
  where
    loop queues workers running warnedInternal = do
      transitioned <- runSystemDB conn.connSysdb (\db -> SystemDB.transitionDelayedWorkflows db)
      case transitioned of
        Left err ->
          unLogAction logger (DbosLogMsg DbosWarn ("could not transition delayed workflows: " <> SystemDB.renderError err) Nothing)
        Right 0 -> pure ()
        Right moved -> unLogAction logger (DbosLogMsg DbosDebug ("delayed workflows are now enqueued: " <> showText moved) Nothing)
      names <- refreshQueueSet conn identity queues warnedInternal logger listenQueues
      current <- readTVarIO workers
      let desired = Set.fromList names
          kept = Map.filterWithKey (\queueName _ -> queueName `Set.member` desired) current
      atomically (writeTVar workers kept)
      forM_ names $ \name ->
        unless (Map.member name kept) $ do
          tid <-
            spawnTracked
              tasks
              ( MThrow.finally
                  (pollQueue tasks conn identity workflows queues running name logger)
                  (atomically (modifyTVar workers (Map.delete name)))
              )
          atomically (modifyTVar workers (Map.insert name tid))
      delayDuration supervisorInterval
      loop queues workers running warnedInternal

-- | One pass of the sweep outside the supervisor: a fresh tally, one poll
-- per queue, and the ids it claimed. Dispatch is concurrent; the caller
-- only learns what was claimed.
dequeuePass :: (MonadSTM m, MonadFork m, MThrow.MonadMask m, MonadTimer m, MonadTime m) => Tasks m -> Connection m -> Identity -> Snapshot m -> Maybe [Text] -> LogAction m DbosLogMsg -> m (Either TransactError.Error [WorkflowId])
dequeuePass tasks conn identity workflows listenQueues logger = do
  transitioned <- runSystemDB conn.connSysdb (\db -> SystemDB.transitionDelayedWorkflows db)
  case transitioned of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    Right _ -> do
      listed <- runSystemDB conn.connSysdb (\db -> SystemDB.listQueues db Unset)
      case listed of
        Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
        Right records -> do
          running <- newRunning
          let visible = filter (listensTo listenQueues) records
              queues = internalQueueRecord : visible
          claimedByQueue <- traverse (\queue -> pollOnce conn identity workflows tasks running queue logger) queues
          pure (Right (concatMap snd claimedByQueue))

listensTo :: Maybe [Text] -> QueueRecord -> Bool
listensTo Nothing _ = True
listensTo (Just names) record = record.queueRecordName `elem` names

-- | Spreads a wait over the dequeue jitter band, so a fleet that started
-- together stops polling in lockstep.
jitter :: Word32 -> Duration -> Duration
jitter bits (Duration interval) =
  let factor = jitterLow + (jitterHigh - jitterLow) * (fromIntegral bits / (fromIntegral (maxBound :: Word32) + 1))
   in Duration (interval * realToFrac factor)

scaleDuration :: Double -> Duration -> Duration
scaleDuration factor (Duration interval) = Duration (interval * realToFrac factor)

delayDuration :: MonadDelay m => Duration -> m ()
delayDuration (Duration interval) = threadDelay (round (interval * 1000000))

showText :: Show a => a -> Text
showText = Text.pack . show
