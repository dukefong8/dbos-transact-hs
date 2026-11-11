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
    reserved,
    queueOptionsAfterChange,
    changeBothSet,
    registerQueue,
    queue,
    listQueues,
    updateQueue,
    deleteQueue,
  )
where

import Data.Int (Int32)
import DBOS.Prelude
import Data.Text qualified as Text
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types
  ( Change (..),
    AppScope (..),
    NewQueue (..),
    OnExistingQueue (..),
    QueueName (..),
    QueueRecord (..),
    VersionInfo (..),
    QueueUpdate (..),
    RateLimit (..),
    Duration,
    defaultQueueUpdate,
    internalQueueName,
    newQueue,
    queueIsLegacyPartitioned,
    secondsDuration,
    durationIsZero,
  )
import DBOS.Transact.Connection (withConnection)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Instance (DBOS, Executor (..), withExecutor)

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
-- for the fleet limit; 'concurrency' is its deprecated alias, retained
-- because both SDK oracles keep it. The two spellings are mutually
-- exclusive (Python @_queue.py@: setting both is refused). There is no
-- priority option: every queue dispatches in priority order, so
-- registration always persists the legacy column as true. A queue-wide
-- limit of 0 pauses the queue — nothing is dequeued until an update
-- raises it — while the per-partition and rate limits stay at least 1:
-- a partition is paused by pausing its queue, not one key at a time.
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
-- clears an optional limit. The concurrency pair is mutually exclusive, as
-- on the options: naming both spellings in one change is refused.
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
  Queue
    { name = record.queueRecordName,
      concurrency = record.queueRecordConcurrency,
      workerConcurrency = record.queueRecordWorkerConcurrency,
      rateLimit = record.queueRecordRateLimit,
      partitionConcurrency = record.queueRecordPartitionConcurrency,
      partitionWorkerConcurrency = record.queueRecordPartitionWorkerConcurrency,
      partitionRateLimit = record.queueRecordPartitionRateLimit,
      pollingInterval = record.queueRecordPollingInterval
    }

-- | The registration view of a stored record, for the in-lock validator.
-- The record has one fleet-limit spelling, so it fills the supported one.
queueOptionsOfRecord :: QueueRecord -> QueueOptions
queueOptionsOfRecord record =
  let receipt = queueFromRecord record
   in defaultQueueOptions
        { globalConcurrency = receipt.concurrency,
          workerConcurrency = receipt.workerConcurrency,
          pollingInterval = receipt.pollingInterval,
          rateLimit = receipt.rateLimit,
          partitionConcurrency = receipt.partitionConcurrency,
          partitionWorkerConcurrency = receipt.partitionWorkerConcurrency,
          partitionRateLimit = receipt.partitionRateLimit
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

-- | The fleet limit the options mean. Only reached with at most one
-- spelling set (validation refuses both): the explicit spelling takes
-- precedence over the deprecated alias, as both SDK oracles resolve it.
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
    -- The fleet-limit change the update means. Only reached with at most
    -- one spelling set (the change check refuses both): the explicit
    -- spelling takes precedence over the deprecated alias.
    effectiveChange change = case (change.globalConcurrency, change.concurrency) of
      (Set value, _) -> Set value
      (Leave, Set value) -> Set value
      (Leave, Leave) -> Leave

-- | Python's both-spellings refusal detail (@_queue.py:148-151@), shared by
-- the registration check and the update change check.
bothSpellingsDetail :: Text
bothSpellingsDetail = "concurrency is deprecated in favor of global_concurrency; set only one of them"

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
        [ bothSpellings options.concurrency options.globalConcurrency,
          nonnegative "concurrency" fleet,
          nonnegative "worker_concurrency" options.workerConcurrency,
          ordered "worker_concurrency" "concurrency" options.workerConcurrency fleet,
          nonzeroDuration "polling_interval" options.pollingInterval,
          rate "rate_limit" options.rateLimit,
          rate "partition_rate_limit" options.partitionRateLimit,
          positive "partition_concurrency" options.partitionConcurrency,
          positive "partition_worker_concurrency" options.partitionWorkerConcurrency,
          ordered "partition_worker_concurrency" "partition_concurrency" options.partitionWorkerConcurrency options.partitionConcurrency,
          ordered "partition_concurrency" "concurrency" options.partitionConcurrency fleet,
          ordered "partition_worker_concurrency" "worker_concurrency" options.partitionWorkerConcurrency options.workerConcurrency,
          ordered "partition_worker_concurrency" "concurrency" options.partitionWorkerConcurrency fleet
        ]
    -- The fleet limit's two spellings are mutually exclusive (Python
    -- @_queue.py@: "set only one of them"); judged before any bound, so a
    -- doubly-spelled request reports exclusivity rather than a limit error.
    bothSpellings alias explicit = case (alias, explicit) of
      (Just _, Just _) -> Just ("concurrency", bothSpellingsDetail)
      _ -> Nothing
    positive field = \case
      Just value | value < 1 -> Just (field, "must be at least 1, got " <> Text.pack (show value))
      Just value | value > maxInt32 -> Just (field, fitsInt32Detail value)
      _ -> Nothing
    -- A queue-wide limit of 0 pauses the queue; only a negative one is
    -- refused. The per-partition and rate limits stay 'positive' above: a
    -- partition is paused by pausing its queue, not one key at a time.
    -- Mirrors the oracle's @validate_fields@; the field rides backticked
    -- in the detail so the refusal names the limit exactly.
    nonnegative field = \case
      Just value | value < 0 -> Just (field, "`" <> field <> "` must not be negative, got " <> Text.pack (show value))
      Just value | value > maxInt32 -> Just (field, fitsInt32Detail value)
      _ -> Nothing
    -- The limit columns are signed 32-bit integers. A wider value would
    -- truncate on write, so it is refused here, where Rust's i32 fields
    -- make it unrepresentable at the call.
    maxInt32 = fromIntegral (maxBound :: Int32)
    fitsInt32Detail value = "must fit in a signed 32-bit integer, got " <> Text.pack (show value)
    ordered field outer innerValue outerValue =
      case (innerValue, outerValue) of
        (Just inner, Just outer') | inner > outer' ->
          Just (field, "`" <> outer <> "` must be greater than or equal to `" <> field <> "`, got " <> Text.pack (show outer') <> " and " <> Text.pack (show inner))
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
      withExecutor dbos "register_queue" $ \executor -> do
        onExisting <- resolveConflict executor conflict
        case onExisting of
          Left err -> pure (Left err)
          Right policy -> do
            let new = (queueOptionsToNewQueue queueName options) {newQueueAppName = Just executor.identity.identityAppName}
            inserted <- withConnection executor.conn (\db -> SystemDB.upsertQueue db new policy)
            case inserted of
              Left err -> pure (Left err)
              Right (_, record) -> pure (Right (queueFromRecord record))

queue :: (MonadMVar m)
      => DBOS m -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe Queue))
queue dbos queueName = do
  withExecutor dbos "read a queue" $ \executor -> do
    result <- withConnection executor.conn (\db -> SystemDB.getQueue db queueName)
    pure (fmap (fmap queueFromRecord) result)

listQueues :: (MonadMVar m)
           => DBOS m -> m (Either (TransactError.Error TransactError.EngineOnly) [Queue])
listQueues dbos = do
  withExecutor dbos "list queues" $ \executor -> do
    result <- withConnection executor.conn (\db -> SystemDB.listQueues db (Named [executor.identity.identityAppName]))
    -- The engine's own queue is not one anybody registered: a stored row
    -- for it must not appear in the operator-visible list.
    let QueueName internalName = internalQueueName
    pure (fmap (map queueFromRecord . filter (\record -> record.queueRecordName /= internalName)) result)

-- | A change naming both fleet-limit spellings is refused with Python's
-- message, before the merge: the write would otherwise collapse the pair
-- onto the explicit spelling while validation judged the merged alias
-- value. 'Set' counts as naming even when it clears ('Set Nothing');
-- 'Leave' is absent.
changeBothSet :: Text -> QueueChange -> Maybe (TransactError.Error TransactError.EngineOnly)
changeBothSet queueName change = case (change.concurrency, change.globalConcurrency) of
  (Set _, Set _) ->
    Just
      ( TransactError.ErrorConfig
          ("queue `" <> queueName <> "`: invalid concurrency: " <> bothSpellingsDetail)
      )
  _ -> Nothing

updateQueue :: (MonadMVar m)
            => DBOS m -> Text -> QueueChange -> m (Either (TransactError.Error TransactError.EngineOnly) Queue)
updateQueue dbos queueName change
  | Just refusal <- reserved queueName = pure (Left refusal)
  | Just refusal <- changeBothSet queueName change = pure (Left refusal)
  | otherwise = do
    withExecutor dbos "update a queue" $ \executor -> do
      stored <- withConnection executor.conn (\db -> SystemDB.getQueue db queueName)
      case stored of
        Left err -> pure (Left err)
        Right Nothing -> pure (Left (TransactError.ErrorConfig ("queue `" <> queueName <> "` is not registered")))
        Right (Just record) -> do
          -- The stored flag is not a compatibility gate: every limit
          -- applies at the scope its column names, so a partition-touching
          -- update is ordinary, even on a row some older writer stamped
          -- with the deprecated flag.
          let currentQueue = queueFromRecord record
              desired = queueOptionsAfterChange change currentQueue
          case validateQueueOptions queueName desired of
            Left err -> pure (Left err)
            Right () -> do
              let update = queueChangeToUpdate change currentQueue
                  -- The seam locks the row, merges, then validates the
                  -- merged record inside that lock. The caller-side check
                  -- above is a fast path; this one is what keeps two
                  -- operators' interleaved limits coherent.
                  validateMerged _ merged =
                    case validateQueueOptions queueName (queueOptionsOfRecord merged) of
                      Right () -> Right ()
                      Left (TransactError.ErrorConfig detail) ->
                        Left (SystemDBError.InvalidInput {field = "queue", detail})
                      Left _ -> Right ()
              updated <- withConnection executor.conn (\db -> SystemDB.updateQueue db queueName update validateMerged)
              pure (queueFromRecord <$> updated)

deleteQueue :: (MonadMVar m)
            => DBOS m -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
deleteQueue dbos queueName = case reserved queueName of
  Just refusal -> pure (Left refusal)
  Nothing -> do
    withExecutor dbos "delete a queue" $ \executor -> do
      result <- withConnection executor.conn (\db -> SystemDB.deleteQueue db queueName)
      pure (() <$ result)

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

resolveConflict :: (Monad m)
                => Executor m -> QueueConflict -> m (Either (TransactError.Error TransactError.EngineOnly) OnExistingQueue)
resolveConflict _ AlwaysUpdate = pure (Right UpdateExisting)
resolveConflict _ NeverUpdate = pure (Right LeaveExisting)
resolveConflict executor UpdateIfLatestVersion = do
  latest <- withConnection executor.conn (\db -> SystemDB.getLatestApplicationVersion db (Just executor.identity.identityAppName))
  pure $ case latest of
    Left err -> Left err
    Right Nothing -> Right UpdateExisting
    Right (Just version)
      | version.versionInfoName == executor.identity.identityAppVersion -> Right UpdateExisting
      | otherwise -> Right LeaveExisting

queueOptionsAfterChange :: QueueChange -> Queue -> QueueOptions
queueOptionsAfterChange change current =
  QueueOptions
    { concurrency = mergedAlias,
      globalConcurrency = mergedExplicit,
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
    -- The merged spellings: a change touching either spelling supersedes
    -- the stored effective limit, and the untouched spelling clears, so the
    -- merged options name at most one spelling and validation below never
    -- mistakes a single-spelling update for a both-spellings request. A
    -- change naming both spellings keeps both, for the refusal below.
    (mergedAlias, mergedExplicit) = case (change.concurrency, change.globalConcurrency) of
      (Leave, Leave) -> (Nothing, current.concurrency)
      (Set alias, Leave) -> (alias, Nothing)
      (Leave, Set explicit) -> (Nothing, explicit)
      (Set alias, Set explicit) -> (alias, explicit)
