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
import Data.Text (Text)
import Data.Text qualified as Text
import Control.Concurrent.Class.MonadMVar (MonadMVar)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Types
  ( Change (..),
    Applications (..),
    NewQueue (..),
    OnExistingQueue (..),
    QueueRecord (..),
    QueueUpdate (..),
    RateLimit,
    ResolvedLimits (..),
    Duration,
    defaultQueueUpdate,
    newQueue,
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
-- to their effective scope, not reported by raw database column.
data Queue = Queue
  { name :: Text,
    concurrency :: Maybe Int,
    worker_concurrency :: Maybe Int,
    rate_limit :: Maybe RateLimit,
    priority_enabled :: Bool,
    partition_concurrency :: Maybe Int,
    partition_worker_concurrency :: Maybe Int,
    partition_rate_limit :: Maybe RateLimit,
    polling_interval :: Duration
  }
  deriving stock (Eq, Show)

-- | Queue limits as supplied at registration. Every default is unbounded,
-- with one-second polling.
data QueueOptions = QueueOptions
  { concurrency :: Maybe Int,
    worker_concurrency :: Maybe Int,
    polling_interval :: Duration,
    rate_limit :: Maybe RateLimit,
    priority_enabled :: Bool,
    partition_concurrency :: Maybe Int,
    partition_worker_concurrency :: Maybe Int,
    partition_rate_limit :: Maybe RateLimit
  }
  deriving stock (Eq, Show)

defaultQueueOptions :: QueueOptions
defaultQueueOptions =
  QueueOptions
    { concurrency = Nothing,
      worker_concurrency = Nothing,
      polling_interval = secondsDuration 1,
      rate_limit = Nothing,
      priority_enabled = False,
      partition_concurrency = Nothing,
      partition_worker_concurrency = Nothing,
      partition_rate_limit = Nothing
    }

-- | A partial change. 'Leave' differs from 'Set Nothing': only the latter
-- clears an optional limit.
data QueueChange = QueueChange
  { concurrency :: Change (Maybe Int),
    worker_concurrency :: Change (Maybe Int),
    polling_interval :: Change Duration,
    rate_limit :: Change (Maybe RateLimit),
    priority_enabled :: Change Bool,
    partition_concurrency :: Change (Maybe Int),
    partition_worker_concurrency :: Change (Maybe Int),
    partition_rate_limit :: Change (Maybe RateLimit)
  }
  deriving stock (Eq, Show)

defaultQueueChange :: QueueChange
defaultQueueChange =
  QueueChange
    { concurrency = Leave,
      worker_concurrency = Leave,
      polling_interval = Leave,
      rate_limit = Leave,
      priority_enabled = Leave,
      partition_concurrency = Leave,
      partition_worker_concurrency = Leave,
      partition_rate_limit = Leave
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
          worker_concurrency = limits.resolvedWorkerConcurrency,
          rate_limit = limits.resolvedRateLimit,
          priority_enabled = record.queueRecordPriorityEnabled,
          partition_concurrency = limits.resolvedPartitionConcurrency,
          partition_worker_concurrency = limits.resolvedPartitionWorkerConcurrency,
          partition_rate_limit = limits.resolvedPartitionRateLimit,
          polling_interval = record.queueRecordPollingInterval
        }

queueIsPartitioned :: Queue -> Bool
queueIsPartitioned receipt =
  case (receipt.partition_concurrency, receipt.partition_worker_concurrency, receipt.partition_rate_limit) of
    (Nothing, Nothing, Nothing) -> False
    _ -> True

queueOptionsToNewQueue :: Text -> QueueOptions -> NewQueue
queueOptionsToNewQueue queueName options =
  (newQueue queueName)
    { newQueueConcurrency = options.concurrency,
      newQueueWorkerConcurrency = options.worker_concurrency,
      newQueueRateLimit = options.rate_limit,
      newQueuePriorityEnabled = options.priority_enabled,
      newQueuePartitionQueue = anyPresent options,
      newQueuePartitionConcurrency = options.partition_concurrency,
      newQueuePartitionWorkerConcurrency = options.partition_worker_concurrency,
      newQueuePartitionRateLimit = options.partition_rate_limit,
      newQueuePollingInterval = options.polling_interval
    }
  where
    anyPresent configured =
      case (configured.partition_concurrency, configured.partition_worker_concurrency, configured.partition_rate_limit) of
        (Nothing, Nothing, Nothing) -> False
        _ -> True

queueChangeToUpdate :: QueueChange -> Queue -> QueueUpdate
queueChangeToUpdate change current =
  let update = defaultQueueUpdate
      partitionTouched =
        change.partition_concurrency /= Leave
          || change.partition_worker_concurrency /= Leave
          || change.partition_rate_limit /= Leave
      resultingPartitioned =
        maybeChanged change.partition_concurrency current.partition_concurrency /= Nothing
          || maybeChanged change.partition_worker_concurrency current.partition_worker_concurrency /= Nothing
          || maybeChanged change.partition_rate_limit current.partition_rate_limit /= Nothing
   in update
        { queueUpdateConcurrency = change.concurrency,
          queueUpdateWorkerConcurrency = change.worker_concurrency,
          queueUpdateRateLimit = change.rate_limit,
          queueUpdatePriorityEnabled = change.priority_enabled,
          queueUpdatePartitionQueue = if partitionTouched then Set resultingPartitioned else Leave,
          queueUpdatePartitionConcurrency = change.partition_concurrency,
          queueUpdatePartitionWorkerConcurrency = change.partition_worker_concurrency,
          queueUpdatePartitionRateLimit = change.partition_rate_limit,
          queueUpdatePollingInterval = change.polling_interval
        }
  where
    maybeChanged (Set value) _ = value
    maybeChanged Leave oldValue = oldValue

validateQueueOptions :: Text -> QueueOptions -> Either TransactError.Error ()
validateQueueOptions queueName options =
  case firstInvalid of
    Nothing -> Right ()
    Just (field, detail) ->
      Left
        ( TransactError.ErrorConfig
            ("queue `" <> queueName <> "`: invalid " <> field <> ": " <> detail)
        )
  where
    firstInvalid =
      first
        [ positive "concurrency" options.concurrency,
          positive "worker_concurrency" options.worker_concurrency,
          ordered "worker_concurrency" "concurrency" options.worker_concurrency options.concurrency,
          nonzeroDuration "polling_interval" options.polling_interval,
          rate "rate_limit" options.rate_limit,
          positive "partition_concurrency" options.partition_concurrency,
          positive "partition_worker_concurrency" options.partition_worker_concurrency,
          ordered "partition_worker_concurrency" "partition_concurrency" options.partition_worker_concurrency options.partition_concurrency,
          ordered "partition_concurrency" "concurrency" options.partition_concurrency options.concurrency,
          ordered "partition_worker_concurrency" "worker_concurrency" options.partition_worker_concurrency options.worker_concurrency,
          partitionRate options.partition_rate_limit options.rate_limit
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

registerQueue :: (MonadMVar m, Monad m) => DBOS m -> Text -> QueueOptions -> QueueConflict -> m (Either TransactError.Error Queue)
registerQueue dbos queueName options conflict =
  case validateQueueOptions queueName options of
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
                Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
                Right _ -> queueRecord executor queueName

queue :: (MonadMVar m, Monad m) => DBOS m -> Text -> m (Either TransactError.Error (Maybe Queue))
queue dbos queueName = do
  required <- requireExecutor dbos "read a queue"
  case required of
    Left err -> pure (Left err)
    Right executor -> do
      result <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.getQueue db queueName)
      pure (fmap (fmap queueFromRecord) (either (Left . TransactError.ErrorSystemDatabase) Right result))

listQueues :: (MonadMVar m, Monad m) => DBOS m -> m (Either TransactError.Error [Queue])
listQueues dbos = do
  required <- requireExecutor dbos "list queues"
  case required of
    Left err -> pure (Left err)
    Right executor -> do
      result <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.listQueues db (Named [executor.identity.identityAppName]))
      pure (fmap (map queueFromRecord) (either (Left . TransactError.ErrorSystemDatabase) Right result))

updateQueue :: (MonadMVar m, Monad m) => DBOS m -> Text -> QueueChange -> m (Either TransactError.Error Queue)
updateQueue dbos queueName change = do
  required <- requireExecutor dbos "update a queue"
  case required of
    Left err -> pure (Left err)
    Right executor -> do
      stored <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.getQueue db queueName)
      case stored of
        Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
        Right Nothing -> pure (Left (TransactError.ErrorConfig ("queue `" <> queueName <> "` is not registered")))
        Right (Just record) -> do
          let currentQueue = queueFromRecord record
              desired = queueOptionsAfterChange change currentQueue
          case validateQueueOptions queueName desired of
            Left err -> pure (Left err)
            Right () -> do
              let update = queueChangeToUpdate change currentQueue
              updated <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.updateQueue db queueName update (\_ _ -> Right ()))
              pure (queueFromRecord <$> either (Left . TransactError.ErrorSystemDatabase) Right updated)

deleteQueue :: (MonadMVar m, Monad m) => DBOS m -> Text -> m (Either TransactError.Error ())
deleteQueue dbos queueName = do
  required <- requireExecutor dbos "delete a queue"
  case required of
    Left err -> pure (Left err)
    Right executor -> do
      result <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.deleteQueue db queueName)
      pure (() <$ either (Left . TransactError.ErrorSystemDatabase) Right result)

queueRecord :: (MonadMVar m, Monad m) => Executor m -> Text -> m (Either TransactError.Error Queue)
queueRecord executor queueName = do
  result <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.getQueue db queueName)
  pure $ case result of
    Left err -> Left (TransactError.ErrorSystemDatabase err)
    Right Nothing -> Left (TransactError.ErrorConfig ("queue `" <> queueName <> "` was not returned after registration"))
    Right (Just record) -> Right (queueFromRecord record)

resolveConflict :: (MonadMVar m, Monad m) => Executor m -> QueueConflict -> m (Either TransactError.Error OnExistingQueue)
resolveConflict _ AlwaysUpdate = pure (Right UpdateExisting)
resolveConflict _ NeverUpdate = pure (Right LeaveExisting)
resolveConflict executor UpdateIfLatestVersion = do
  latest <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.getLatestApplicationVersion db (Just executor.identity.identityAppName))
  pure $ case latest of
    Left err -> Left (TransactError.ErrorSystemDatabase err)
    Right Nothing -> Right UpdateExisting
    Right (Just version)
      | version.versionInfoName == executor.identity.identityAppVersion -> Right UpdateExisting
      | otherwise -> Right LeaveExisting

queueOptionsAfterChange :: QueueChange -> Queue -> QueueOptions
queueOptionsAfterChange change current =
  QueueOptions
    { concurrency = apply change.concurrency current.concurrency,
      worker_concurrency = apply change.worker_concurrency current.worker_concurrency,
      polling_interval = apply change.polling_interval current.polling_interval,
      rate_limit = apply change.rate_limit current.rate_limit,
      priority_enabled = apply change.priority_enabled current.priority_enabled,
      partition_concurrency = apply change.partition_concurrency current.partition_concurrency,
      partition_worker_concurrency = apply change.partition_worker_concurrency current.partition_worker_concurrency,
      partition_rate_limit = apply change.partition_rate_limit current.partition_rate_limit
    }
  where
    apply Leave value = value
    apply (Set value) _ = value
