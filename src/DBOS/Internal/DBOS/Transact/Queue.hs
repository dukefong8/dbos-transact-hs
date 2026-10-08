{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The engine-facing queue configuration and receipt, one-to-one with Rust
-- @queue.rs@. SystemDB owns the durable QueueRecord and writes; this module
-- resolves legacy row semantics and translates user updates to that seam.
module DBOS.Transact.Queue
  ( Queue (..),
    QueueOptions (..),
    QueueChange (..),
    QueueConflict (..),
    defaultQueueOptions,
    defaultQueueChange,
    queueFromRecord,
    queueIsPartitioned,
    queueOptionsToNewQueue,
    queueChangeToUpdate,
    validateQueueOptions,
    registerQueue,
    queue,
    listQueues,
    updateQueue,
    deleteQueue,
  )
where

import DBOS.Prelude
import Data.Text qualified as Text
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Types
  ( Change (..),
    Applications (..),
    NewQueue (..),
    OnExistingQueue (..),
    QueueName (..),
    QueueRecord (..),
    VersionInfo (..),
    QueueUpdate (..),
    RateLimit (..),
    ResolvedLimits (..),
    Duration,
    defaultQueueUpdate,
    internalQueueName,
    newQueue,
    queueIsLegacyPartitioned,
    queueResolvedLimits,
    secondsDuration,
    durationAsMillis,
    durationIsZero,
  )
import DBOS.Transact.Connection (Connection (..), runSystemDB)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Instance (DBOS, Executor (..), requireExecutor)

-- | A registered queue as this process understands it. Limits are resolved
-- to their effective scope, not reported by raw database column. The
-- priority flag is gone (every queue dispatches in priority order, as both
-- SDK oracles hold); the record still carries the legacy column.
data Queue = Queue
  { name :: Text,
    concurrency :: Maybe Int,
    workerConcurrency :: Maybe Int,
    rateLimit :: Maybe RateLimit,
    partitionConcurrency :: Maybe Int,
    partitionWorkerConcurrency :: Maybe Int,
    partitionRateLimit :: Maybe RateLimit,
    pollingInterval :: Duration
  }
  deriving stock (Eq, Show)

-- | Queue limits as supplied at registration. Every default is unbounded,
-- with one-second polling. 'globalConcurrency' is the supported spelling
-- for the fleet limit; 'concurrency' is its deprecated alias (explicit
-- wins), retained because both SDK oracles keep it. There is no priority
-- option: every queue dispatches in priority order, so registration always
-- persists the legacy column as true.
data QueueOptions = QueueOptions
  { -- | Deprecated alias for 'globalConcurrency': explicit wins. Retained
    -- because both SDK oracles keep it. (A @DEPRECATED@ pragma would fire
    -- on the receipt's live field too, so the deprecation is documented,
    -- not pragma-enforced.)
    concurrency :: Maybe Int,
    globalConcurrency :: Maybe Int,
    workerConcurrency :: Maybe Int,
    pollingInterval :: Duration,
    rateLimit :: Maybe RateLimit,
    partitionConcurrency :: Maybe Int,
    partitionWorkerConcurrency :: Maybe Int,
    partitionRateLimit :: Maybe RateLimit
  }
  deriving stock (Eq, Show)

defaultQueueOptions :: QueueOptions
defaultQueueOptions =
  QueueOptions
    { concurrency = Nothing,
      globalConcurrency = Nothing,
      workerConcurrency = Nothing,
      pollingInterval = secondsDuration 1,
      rateLimit = Nothing,
      partitionConcurrency = Nothing,
      partitionWorkerConcurrency = Nothing,
      partitionRateLimit = Nothing
    }

-- | A partial change. 'Leave' differs from 'Set Nothing': only the latter
-- clears an optional limit. The concurrency pair follows the options
-- precedence: an explicit 'globalConcurrency' wins over 'concurrency'.
data QueueChange = QueueChange
  { -- | Deprecated alias for 'globalConcurrency', as on the options.
    concurrency :: Change (Maybe Int),
    globalConcurrency :: Change (Maybe Int),
    workerConcurrency :: Change (Maybe Int),
    pollingInterval :: Change Duration,
    rateLimit :: Change (Maybe RateLimit),
    partitionConcurrency :: Change (Maybe Int),
    partitionWorkerConcurrency :: Change (Maybe Int),
    partitionRateLimit :: Change (Maybe RateLimit)
  }
  deriving stock (Eq, Show)

defaultQueueChange :: QueueChange
defaultQueueChange =
  QueueChange
    { concurrency = Leave,
      globalConcurrency = Leave,
      workerConcurrency = Leave,
      pollingInterval = Leave,
      rateLimit = Leave,
      partitionConcurrency = Leave,
      partitionWorkerConcurrency = Leave,
      partitionRateLimit = Leave
    }

-- | What registering a queue that already exists does to the stored limits.
data QueueConflict
  = UpdateIfLatestVersion
  | AlwaysUpdate
  | NeverUpdate
  deriving stock (Eq, Show)

queueFromRecord :: QueueRecord -> Queue
queueFromRecord record =
  let limits = queueResolvedLimits record
   in Queue
        { name = record.queueRecordName,
          concurrency = limits.resolvedConcurrency,
          workerConcurrency = limits.resolvedWorkerConcurrency,
          rateLimit = limits.resolvedRateLimit,
          partitionConcurrency = limits.resolvedPartitionConcurrency,
          partitionWorkerConcurrency = limits.resolvedPartitionWorkerConcurrency,
          partitionRateLimit = limits.resolvedPartitionRateLimit,
          pollingInterval = record.queueRecordPollingInterval
        }

queueIsPartitioned :: Queue -> Bool
queueIsPartitioned receipt =
  case (receipt.partitionConcurrency, receipt.partitionWorkerConcurrency, receipt.partitionRateLimit) of
    (Nothing, Nothing, Nothing) -> False
    _ -> True

queueOptionsToNewQueue :: Text -> QueueOptions -> NewQueue
queueOptionsToNewQueue queueName options =
  (newQueue queueName)
    { newQueueConcurrency = effectiveConcurrency options,
      newQueueWorkerConcurrency = options.workerConcurrency,
      newQueueRateLimit = options.rateLimit,
      -- Legacy column, still read by other SDKs: every queue is a priority
      -- queue now, so registration always persists true, as Python does.
      newQueuePriorityEnabled = True,
      newQueuePartitionQueue = anyPresent options,
      newQueuePartitionConcurrency = options.partitionConcurrency,
      newQueuePartitionWorkerConcurrency = options.partitionWorkerConcurrency,
      newQueuePartitionRateLimit = options.partitionRateLimit,
      newQueuePollingInterval = options.pollingInterval
    }
  where
    anyPresent configured =
      case (configured.partitionConcurrency, configured.partitionWorkerConcurrency, configured.partitionRateLimit) of
        (Nothing, Nothing, Nothing) -> False
        _ -> True

-- | The fleet limit the options mean: the explicit spelling wins over the
-- deprecated alias, as both SDK oracles resolve it.
effectiveConcurrency :: QueueOptions -> Maybe Int
effectiveConcurrency options = options.globalConcurrency <|> options.concurrency

queueChangeToUpdate :: QueueChange -> Queue -> QueueUpdate
queueChangeToUpdate change current =
  let update = defaultQueueUpdate
      partitionTouched =
        change.partitionConcurrency /= Leave
          || change.partitionWorkerConcurrency /= Leave
          || change.partitionRateLimit /= Leave
      resultingPartitioned =
        maybeChanged change.partitionConcurrency current.partitionConcurrency /= Nothing
          || maybeChanged change.partitionWorkerConcurrency current.partitionWorkerConcurrency /= Nothing
          || maybeChanged change.partitionRateLimit current.partitionRateLimit /= Nothing
   in update
        { queueUpdateConcurrency = effectiveChange change,
          queueUpdateWorkerConcurrency = change.workerConcurrency,
          queueUpdateRateLimit = change.rateLimit,
          -- Legacy column, still read by other SDKs: every materialized
          -- update persists true, as Python's upsert does. This keeps an
          -- otherwise-empty change non-empty, so an empty update writes
          -- its (unchanged) row rather than skipping.
          queueUpdatePriorityEnabled = Set True,
          queueUpdatePartitionQueue = if partitionTouched then Set resultingPartitioned else Leave,
          queueUpdatePartitionConcurrency = change.partitionConcurrency,
          queueUpdatePartitionWorkerConcurrency = change.partitionWorkerConcurrency,
          queueUpdatePartitionRateLimit = change.partitionRateLimit,
          queueUpdatePollingInterval = change.pollingInterval
        }
  where
    maybeChanged (Set value) _ = value
    maybeChanged Leave oldValue = oldValue
    -- The fleet-limit change the update means: the explicit spelling wins
    -- over the deprecated alias, as both SDK oracles resolve it.
    effectiveChange change = case (change.globalConcurrency, change.concurrency) of
      (Set value, _) -> Set value
      (Leave, Set value) -> Set value
      (Leave, Leave) -> Leave

validateQueueOptions :: Text -> QueueOptions -> Either (TransactError.Error TransactError.EngineOnly) ()
validateQueueOptions queueName options =
  case firstInvalid of
    Nothing -> Right ()
    Just (field, detail) ->
      Left
        ( TransactError.ErrorConfig
            ("queue `" <> queueName <> "`: invalid " <> field <> ": " <> detail)
        )
  where
    -- Validation judges the effective fleet limit (explicit wins over the
    -- deprecated alias), never either spelling alone.
    fleet = effectiveConcurrency options
    firstInvalid =
      first
        [ positive "concurrency" fleet,
          positive "worker_concurrency" options.workerConcurrency,
          ordered "worker_concurrency" "concurrency" options.workerConcurrency fleet,
          nonzeroDuration "polling_interval" options.pollingInterval,
          rate "rate_limit" options.rateLimit,
          rate "partition_rate_limit" options.partitionRateLimit,
          positive "partition_concurrency" options.partitionConcurrency,
          positive "partition_worker_concurrency" options.partitionWorkerConcurrency,
          ordered "partition_worker_concurrency" "partition_concurrency" options.partitionWorkerConcurrency options.partitionConcurrency,
          ordered "partition_concurrency" "concurrency" options.partitionConcurrency fleet,
          ordered "partition_worker_concurrency" "worker_concurrency" options.partitionWorkerConcurrency options.workerConcurrency,
          partitionRate options.partitionRateLimit options.rateLimit
        ]
    positive field = \case
      Just value | value < 1 -> Just (field, "must be at least 1, got " <> Text.pack (show value))
      _ -> Nothing
    ordered field outer innerValue outerValue =
      case (innerValue, outerValue) of
        (Just inner, Just outer') | inner > outer' ->
          Just (field, "must not exceed `" <> outer <> "`, got " <> Text.pack (show inner) <> " and " <> Text.pack (show outer'))
        _ -> Nothing
    nonzeroDuration field duration
      | durationIsZero duration = Just (field, "cannot be zero")
      | otherwise = Nothing
    rate field = \case
      Nothing -> Nothing
      Just value
        | value.rateLimitLimit < 1 -> Just (field <> ".limit", "must be at least 1, got " <> Text.pack (show value.rateLimitLimit))
        | durationIsZero value.rateLimitPeriod -> Just (field <> ".period", "cannot be zero")
        | otherwise -> Nothing
    partitionRate Nothing _ = Nothing
    partitionRate _ Nothing = Nothing
    partitionRate (Just partition) (Just global)
      | toInteger partition.rateLimitLimit * durationAsMillis global.rateLimitPeriod
          > toInteger global.rateLimitLimit * durationAsMillis partition.rateLimitPeriod =
          Just ("partition_rate_limit", "must not exceed `rate_limit`")
      | otherwise = Nothing
    first = foldr (\candidate rest -> candidate <|> rest) Nothing
    (<|>) (Just value) _ = Just value
    (<|>) Nothing other = other

registerQueue :: (MonadMVar m)
              => DBOS m -> Text -> QueueOptions -> QueueConflict -> m (Either (TransactError.Error TransactError.EngineOnly) Queue)
registerQueue dbos queueName options conflict
  | Just refusal <- reserved queueName = pure (Left refusal)
  | otherwise = case validateQueueOptions queueName options of
    Left err -> pure (Left err)
    Right () -> do
      required <- requireExecutor dbos "register_queue"
      case required of
        Left err -> pure (Left err)
        Right executor -> do
          onExisting <- resolveConflict executor conflict
          case onExisting of
            Left err -> pure (Left err)
            Right policy -> do
              let new = (queueOptionsToNewQueue queueName options) {newQueueApplicationName = Just executor.identity.identityAppName}
              inserted <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.upsertQueue db new policy)
              case inserted of
                Left err -> pure (Left (TransactError.SystemDatabase err))
                Right _ -> queueRecord executor queueName

queue :: (MonadMVar m)
      => DBOS m -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe Queue))
queue dbos queueName = do
  required <- requireExecutor dbos "read a queue"
  case required of
    Left err -> pure (Left err)
    Right executor -> do
      result <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.getQueue db queueName)
      pure (fmap (fmap queueFromRecord) (either (Left . TransactError.SystemDatabase) Right result))

listQueues :: (MonadMVar m)
           => DBOS m -> m (Either (TransactError.Error TransactError.EngineOnly) [Queue])
listQueues dbos = do
  required <- requireExecutor dbos "list queues"
  case required of
    Left err -> pure (Left err)
    Right executor -> do
      result <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.listQueues db (Named [executor.identity.identityAppName]))
      pure (fmap (map queueFromRecord) (either (Left . TransactError.SystemDatabase) Right result))

updateQueue :: (MonadMVar m)
            => DBOS m -> Text -> QueueChange -> m (Either (TransactError.Error TransactError.EngineOnly) Queue)
updateQueue dbos queueName change = case reserved queueName of
  Just refusal -> pure (Left refusal)
  Nothing -> do
    required <- requireExecutor dbos "update a queue"
    case required of
      Left err -> pure (Left err)
      Right executor -> do
        stored <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.getQueue db queueName)
        case stored of
          Left err -> pure (Left (TransactError.SystemDatabase err))
          Right Nothing -> pure (Left (TransactError.ErrorConfig ("queue `" <> queueName <> "` is not registered")))
          Right (Just record) -> do
            -- A legacy-partitioned row cannot take a per-partition limit:
            -- its queue-wide limits already are its per-partition ones, so
            -- adding a second set would leave two answers in one row.
            -- Asked of the row as stored, not as merged: clearing the last
            -- partition limit leaves a legacy-looking row, and refusing
            -- that would make un-partitioning impossible.
            let touchesPartition =
                  change.partitionConcurrency /= Leave
                    || change.partitionWorkerConcurrency /= Leave
                    || change.partitionRateLimit /= Leave
            if touchesPartition && queueIsLegacyPartitioned record
              then
                pure
                  ( Left
                      ( TransactError.ErrorConfig
                          ( "queue `" <> queueName <> "`: this queue is registered with the deprecated `partition_queue` flag, under which its queue-wide limits already apply per partition; re-register it with the per-partition limits instead"
                          )
                      )
                  )
              else do
                let currentQueue = queueFromRecord record
                    desired = queueOptionsAfterChange change currentQueue
                case validateQueueOptions queueName desired of
                  Left err -> pure (Left err)
                  Right () -> do
                    let update = queueChangeToUpdate change currentQueue
                    updated <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.updateQueue db queueName update (\_ _ -> Right ()))
                    pure (queueFromRecord <$> either (Left . TransactError.SystemDatabase) Right updated)

deleteQueue :: (MonadMVar m)
            => DBOS m -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
deleteQueue dbos queueName = case reserved queueName of
  Just refusal -> pure (Left refusal)
  Nothing -> do
    required <- requireExecutor dbos "delete a queue"
    case required of
      Left err -> pure (Left err)
      Right executor -> do
        result <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.deleteQueue db queueName)
        pure (() <$ either (Left . TransactError.SystemDatabase) Right result)

-- | The engine's own queue is not one anybody registers, updates, or
-- deletes: it has no row and takes no limits. Mirrors the oracle's
-- reserved-name refusal, checked before anything else, with its message.
reserved :: Text -> Maybe (TransactError.Error TransactError.EngineOnly)
reserved queueName =
  case internalQueueName of
    QueueName internal
      | queueName == internal ->
          Just
            ( TransactError.ErrorConfig
                ("the queue name `" <> queueName <> "` is reserved for the engine's internal queue")
            )
      | otherwise -> Nothing

queueRecord :: (Monad m)
            => Executor m -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) Queue)
queueRecord executor queueName = do
  result <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.getQueue db queueName)
  pure $ case result of
    Left err -> Left (TransactError.SystemDatabase err)
    Right Nothing -> Left (TransactError.ErrorConfig ("queue `" <> queueName <> "` was not returned after registration"))
    Right (Just record) -> Right (queueFromRecord record)

resolveConflict :: (Monad m)
                => Executor m -> QueueConflict -> m (Either (TransactError.Error TransactError.EngineOnly) OnExistingQueue)
resolveConflict _ AlwaysUpdate = pure (Right UpdateExisting)
resolveConflict _ NeverUpdate = pure (Right LeaveExisting)
resolveConflict executor UpdateIfLatestVersion = do
  latest <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.getLatestApplicationVersion db (Just executor.identity.identityAppName))
  pure $ case latest of
    Left err -> Left (TransactError.SystemDatabase err)
    Right Nothing -> Right UpdateExisting
    Right (Just version)
      | version.versionInfoName == executor.identity.identityAppVersion -> Right UpdateExisting
      | otherwise -> Right LeaveExisting

queueOptionsAfterChange :: QueueChange -> Queue -> QueueOptions
queueOptionsAfterChange change current =
  QueueOptions
    { concurrency = apply change.concurrency current.concurrency,
      globalConcurrency = applyEffective change current.concurrency,
      workerConcurrency = apply change.workerConcurrency current.workerConcurrency,
      pollingInterval = apply change.pollingInterval current.pollingInterval,
      rateLimit = apply change.rateLimit current.rateLimit,
      partitionConcurrency = apply change.partitionConcurrency current.partitionConcurrency,
      partitionWorkerConcurrency = apply change.partitionWorkerConcurrency current.partitionWorkerConcurrency,
      partitionRateLimit = apply change.partitionRateLimit current.partitionRateLimit
    }
  where
    apply Leave value = value
    apply (Set value) _ = value
    -- The merged effective limit: a change on either spelling lands on the
    -- explicit one, so validation below judges what the write will store.
    applyEffective diff fallback = case (diff.globalConcurrency, diff.concurrency) of
      (Set value, _) -> value
      (Leave, Set value) -> value
      (Leave, Leave) -> fallback
