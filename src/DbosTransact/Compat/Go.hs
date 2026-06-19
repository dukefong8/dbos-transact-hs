{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module DbosTransact.Compat.Go
  ( DBOSContext
  , newDBOSContext
  , launch
  , shutdown
  , registerWorkflow
  , runWorkflow
  , runAsStep
  , runAsTxn
  , WorkflowOption(..)
  , StepOption(..)
  , WorkflowHandle(..)
  ) where

import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, readMVar)
import Control.Exception (SomeException, displayException, throwIO, try)
import Data.Foldable (traverse_)
import Control.Monad (void, when)
import Data.Aeson (FromJSON, Result(..), ToJSON, Value, fromJSON, toJSON)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import DbosTransact.Config
import DbosTransact.Effects (TransactionScope)
import DbosTransact.Error
import qualified DbosTransact.Step as Step
import qualified DbosTransact.Workflow as Safe
import GHC.Conc (TVar, atomically, newTVarIO, readTVar, readTVarIO, writeTVar)

newtype DBOSContext = DBOSContext InternalCompatContext

data InternalCompatContext = InternalCompatContext
  { compatConfig :: DBOSConfig
  , compatRuntime :: CompatRuntime
  , compatCurrentWorkflow :: Maybe Text
  }

data CompatRuntime = CompatRuntime
  { compatRegistry :: TVar (Map Text RegisteredCompatWorkflow)
  , compatStatuses :: TVar (Map Text Safe.WorkflowStatus)
  , compatResults :: TVar (Map Text (Either DBOSError Value))
  , compatNextWorkflowNumber :: TVar Int
  , compatStepResults :: TVar (Map (Text, Text) Value)
  , compatStepCounters :: TVar (Map Text Int)
  , compatInFlight :: TVar (Map Text (MVar ()))
  }

data RegisteredCompatWorkflow = RegisteredCompatWorkflow
  { registeredExecutor :: DBOSContext -> Value -> IO (Either DBOSError Value)
  }

newDBOSContext :: DBOSConfig -> IO DBOSContext
newDBOSContext config = do
  registry <- newTVarIO Map.empty
  statuses <- newTVarIO Map.empty
  results <- newTVarIO Map.empty
  nextWorkflowNumber <- newTVarIO 1
  stepResults <- newTVarIO Map.empty
  stepCounters <- newTVarIO Map.empty
  inFlight <- newTVarIO Map.empty
  let runtime = CompatRuntime
        { compatRegistry = registry
        , compatStatuses = statuses
        , compatResults = results
        , compatNextWorkflowNumber = nextWorkflowNumber
        , compatStepResults = stepResults
        , compatStepCounters = stepCounters
        , compatInFlight = inFlight
        }
  pure (DBOSContext (InternalCompatContext config runtime Nothing))

launch :: DBOSContext -> IO ()
launch _ = pure ()

shutdown :: DBOSContext -> NominalDiffTime -> IO ()
shutdown _ _ = pure ()

data WorkflowOption
  = WithWorkflowID Text
  | WithQueue Text
  | WithApplicationVersion Text
  | WithDeduplicationID Text
  | WithDeduplicationPolicy Safe.DeduplicationPolicy
  | WithPriority Int
  | WithQueuePartitionKey Text
  | WithDelay NominalDiffTime
  | WithAuthenticatedUser Text
  | WithAssumedRole Text
  | WithAuthenticatedRoles [Text]
  | WithPortableWorkflow
  deriving stock (Eq, Show)

data StepOption
  = WithStepName Text
  | WithStepMaxRetries Int
  | WithBackoffFactor Double
  | WithBaseInterval NominalDiffTime
  | WithMaxInterval NominalDiffTime
  | WithTxIsoLevel Step.IsoLevel
  deriving stock (Eq, Show)

data WorkflowHandle output = WorkflowHandle
  { getWorkflowID :: Text
  , getResult     :: IO (Either DBOSError output)
  , getStatus     :: IO (Either DBOSError Safe.WorkflowStatus)
  }

registerWorkflow
  :: (ToJSON input, FromJSON input, ToJSON output, FromJSON output)
  => DBOSContext
  -> Text
  -> (DBOSContext -> input -> IO output)
  -> IO ()
registerWorkflow (DBOSContext internal) name body = do
  inserted <- atomically $ do
    registry <- readTVar registryVar
    if Map.member name registry
      then pure False
      else do
        writeTVar registryVar (Map.insert name erasedWorkflow registry)
        pure True
  when (not inserted) $
    ioError (userError ("workflow already registered: " <> Text.unpack name))
  where
    runtime = compatRuntime internal
    registryVar = compatRegistry runtime
    erasedWorkflow = RegisteredCompatWorkflow $ \workflowCtx inputValue ->
      case fromJSON inputValue of
        Error err -> pure (Left (SerializationError (Text.pack err)))
        Success decodedInput -> Right . toJSON <$> body workflowCtx decodedInput

runWorkflow
  :: (ToJSON input, FromJSON output)
  => DBOSContext
  -> Text
  -> input
  -> [WorkflowOption]
  -> IO (WorkflowHandle output)
runWorkflow (DBOSContext internal) name input options =
  let runtime = compatRuntime internal in do
  registry <- readTVarIO (compatRegistry runtime)
  registered <- case Map.lookup name registry of
    Nothing -> ioError (userError ("workflow is not registered: " <> Text.unpack name))
    Just workflow -> pure workflow
  workflowID <- resolveWorkflowID runtime name options
  let workflowContext = DBOSContext internal { compatCurrentWorkflow = Just workflowID }

  -- Check for existing results or in-flight execution atomically.
  ourMVar <- newEmptyMVar
  claimResult <- atomically $ do
    results <- readTVar (compatResults runtime)
    inFlight <- readTVar (compatInFlight runtime)
    case Map.lookup workflowID results of
      Just storedResult
        | isErrorValue storedResult ->
            case Map.lookup workflowID inFlight of
              Just waitingMVar -> pure (Just (Right waitingMVar))
              Nothing -> do
                writeTVar (compatInFlight runtime) (Map.insert workflowID ourMVar inFlight)
                pure Nothing
        | otherwise -> pure (Just (Left storedResult))
      Nothing ->
        case Map.lookup workflowID inFlight of
          Just waitingMVar -> pure (Just (Right waitingMVar))
          Nothing -> do
            writeTVar (compatInFlight runtime) (Map.insert workflowID ourMVar inFlight)
            pure Nothing

  case claimResult of
    Just (Left storedResult) ->
      pure WorkflowHandle
        { getWorkflowID = workflowID
        , getResult = decodeStoredResult storedResult
        , getStatus = lookupStoredStatus runtime workflowID
        }
    Just (Right waitingMVar) -> do
      void (readMVar waitingMVar)
      existingResult <- readTVarIO (compatResults runtime)
      case Map.lookup workflowID existingResult of
        Just storedResult ->
          pure WorkflowHandle
            { getWorkflowID = workflowID
            , getResult = decodeStoredResult storedResult
            , getStatus = lookupStoredStatus runtime workflowID
            }
        Nothing -> executeAndStoreWorkflow runtime internal workflowID name input options workflowContext registered
    Nothing ->
      executeAndStoreWorkflow runtime internal workflowID name input options workflowContext registered

isErrorValue :: Either a b -> Bool
isErrorValue (Left _) = True
isErrorValue _ = False
executeAndStoreWorkflow
  :: (ToJSON input, FromJSON output)
  => CompatRuntime
  -> InternalCompatContext
  -> Text
  -> Text
  -> input
  -> [WorkflowOption]
  -> DBOSContext
  -> RegisteredCompatWorkflow
  -> IO (WorkflowHandle output)
executeAndStoreWorkflow runtime internal workflowID name input options workflowContext registered = do
  createdAt <- getCurrentTime
  let pendingStatus = mkStatus (compatConfig internal) createdAt createdAt workflowID name input options Safe.WorkflowPending Nothing Nothing
  atomically $ do
    statuses <- readTVar (compatStatuses runtime)
    writeTVar (compatStatuses runtime) (Map.insert workflowID pendingStatus statuses)
  attempt <- try (registeredExecutor registered workflowContext (toJSON input))
  completedAt <- getCurrentTime
  let finalResult = either (Left . WorkflowExecutionError . Text.pack . displayException) id (attempt :: Either SomeException (Either DBOSError Value))
      finalStatus = case finalResult of
        Right outputValue -> mkStatus (compatConfig internal) createdAt completedAt workflowID name input options Safe.WorkflowSuccess (Just outputValue) Nothing
        Left err -> mkStatus (compatConfig internal) createdAt completedAt workflowID name input options Safe.WorkflowError Nothing (Just (errorText err))
  mvarToSignal <- atomically $ do
    statuses <- readTVar (compatStatuses runtime)
    results <- readTVar (compatResults runtime)
    inFlight <- readTVar (compatInFlight runtime)
    writeTVar (compatStatuses runtime) (Map.insert workflowID finalStatus statuses)
    writeTVar (compatResults runtime) (Map.insert workflowID finalResult results)
    writeTVar (compatInFlight runtime) (Map.delete workflowID inFlight)
    pure (Map.lookup workflowID inFlight)
  traverse_ (`putMVar` ()) mvarToSignal
  pure WorkflowHandle
    { getWorkflowID = workflowID
    , getResult = decodeStoredResult finalResult
    , getStatus = lookupStoredStatus runtime workflowID
    }


runAsStep
  :: (ToJSON output, FromJSON output)
  => DBOSContext
  -> [StepOption]
  -> IO output
  -> IO output
runAsStep (DBOSContext internal) options action =
  case compatCurrentWorkflow internal of
    Nothing -> ioError (userError "runAsStep must be called inside a workflow")
    Just workflowID -> do
      let runtime = compatRuntime internal
      resolvedStepName <- resolveStepName runtime workflowID options
      stored <- readTVarIO (compatStepResults runtime)
      case Map.lookup (workflowID, resolvedStepName) stored of
        Just value -> decodeStepValue value
        Nothing -> do
          output <- executeStepWithRetries (stepMaxRetriesOption options) action
          atomically $ do
            records <- readTVar (compatStepResults runtime)
            writeTVar (compatStepResults runtime) (Map.insert (workflowID, resolvedStepName) (toJSON output) records)
          pure output

runAsTxn
  :: forall stmt output.
     (ToJSON output, FromJSON output)
  => DBOSContext
  -> [StepOption]
  -> (forall tx. TransactionScope stmt tx -> IO output)
  -> IO output
runAsTxn _ _ _ = error "DbosTransact.Compat.Go.runAsTxn: not implemented"

resolveWorkflowID :: CompatRuntime -> Text -> [WorkflowOption] -> IO Text
resolveWorkflowID runtime name options =
  case explicitWorkflowID options of
    Just workflowID -> pure workflowID
    Nothing -> atomically $ do
      next <- readTVar (compatNextWorkflowNumber runtime)
      writeTVar (compatNextWorkflowNumber runtime) (next + 1)
      pure (name <> "-" <> Text.pack (show next))

explicitWorkflowID :: [WorkflowOption] -> Maybe Text
explicitWorkflowID [] = Nothing
explicitWorkflowID (WithWorkflowID workflowID : _) = Just workflowID
explicitWorkflowID (_ : rest) = explicitWorkflowID rest

resolveStepName :: CompatRuntime -> Text -> [StepOption] -> IO Text
resolveStepName runtime workflowID options =
  case explicitStepName options of
    Just name -> pure name
    Nothing -> atomically $ do
      counters <- readTVar (compatStepCounters runtime)
      let next = Map.findWithDefault 1 workflowID counters
      writeTVar (compatStepCounters runtime) (Map.insert workflowID (next + 1) counters)
      pure ("step-" <> Text.pack (show next))

explicitStepName :: [StepOption] -> Maybe Text
explicitStepName [] = Nothing
explicitStepName (WithStepName name : _) = Just name
explicitStepName (_ : rest) = explicitStepName rest

stepMaxRetriesOption :: [StepOption] -> Int
stepMaxRetriesOption [] = 0
stepMaxRetriesOption (WithStepMaxRetries maxRetries : _) = max 0 maxRetries
stepMaxRetriesOption (_ : rest) = stepMaxRetriesOption rest

executeStepWithRetries :: Int -> IO output -> IO output
executeStepWithRetries retries action = go retries
  where
    go remaining = do
      result <- try action
      case result of
        Right output -> pure output
        Left err
          | remaining > 0 -> go (remaining - 1)
          | otherwise -> throwIO (err :: SomeException)

decodeStepValue :: FromJSON output => Value -> IO output
decodeStepValue value =
  case fromJSON value of
    Error err -> ioError (userError ("failed to decode recorded step output: " <> err))
    Success output -> pure output

mkStatus
  :: ToJSON input
  => DBOSConfig
  -> UTCTime
  -> UTCTime
  -> Text
  -> Text
  -> input
  -> [WorkflowOption]
  -> Safe.WorkflowStatusType
  -> Maybe Value
  -> Maybe Text
  -> Safe.WorkflowStatus
mkStatus config createdAt updatedAt workflowID name input options statusType outputValue errorValue = Safe.WorkflowStatus
  { Safe.statusWorkflowId = workflowID
  , Safe.statusType = statusType
  , Safe.statusName = name
  , Safe.statusInput = Just (toJSON input)
  , Safe.statusOutput = outputValue
  , Safe.statusError = errorValue
  , Safe.statusExecutorId = Nothing
  , Safe.statusApplicationVersion = optionApplicationVersion options
  , Safe.statusApplicationId = Just (dbosApplicationName config)
  , Safe.statusCreatedAt = createdAt
  , Safe.statusUpdatedAt = updatedAt
  , Safe.statusCompletedAt = if statusType == Safe.WorkflowPending then Nothing else Just updatedAt
  , Safe.statusRecoveryAttempts = 1
  , Safe.statusQueueName = optionQueue options
  , Safe.statusWorkflowTimeout = Nothing
  , Safe.statusWorkflowDeadline = Nothing
  , Safe.statusDeduplicationId = optionDeduplicationID options
  , Safe.statusPriority = optionPriority options
  , Safe.statusQueuePartitionKey = optionQueuePartitionKey options
  , Safe.statusParentWorkflowId = Nothing
  , Safe.statusClassName = Nothing
  , Safe.statusConfigName = Nothing
  , Safe.statusSerialization = "json"
  , Safe.statusDelayUntil = Nothing
  }

optionApplicationVersion :: [WorkflowOption] -> Maybe Text
optionApplicationVersion [] = Nothing
optionApplicationVersion (WithApplicationVersion version : _) = Just version
optionApplicationVersion (_ : rest) = optionApplicationVersion rest

optionQueue :: [WorkflowOption] -> Maybe Text
optionQueue [] = Nothing
optionQueue (WithQueue queue : _) = Just queue
optionQueue (_ : rest) = optionQueue rest

optionDeduplicationID :: [WorkflowOption] -> Maybe Text
optionDeduplicationID [] = Nothing
optionDeduplicationID (WithDeduplicationID deduplicationID : _) = Just deduplicationID
optionDeduplicationID (_ : rest) = optionDeduplicationID rest

optionQueuePartitionKey :: [WorkflowOption] -> Maybe Text
optionQueuePartitionKey [] = Nothing
optionQueuePartitionKey (WithQueuePartitionKey key : _) = Just key
optionQueuePartitionKey (_ : rest) = optionQueuePartitionKey rest

optionPriority :: [WorkflowOption] -> Int
optionPriority [] = 0
optionPriority (WithPriority priority : _) = priority
optionPriority (_ : rest) = optionPriority rest

lookupStoredStatus :: CompatRuntime -> Text -> IO (Either DBOSError Safe.WorkflowStatus)
lookupStoredStatus runtime workflowID = do
  statuses <- readTVarIO (compatStatuses runtime)
  pure $ maybe (Left (WorkflowNotFound workflowID)) Right (Map.lookup workflowID statuses)


decodeStoredResult :: FromJSON output => Either DBOSError Value -> IO (Either DBOSError output)
decodeStoredResult (Left err) = pure (Left err)
decodeStoredResult (Right value) =
  case fromJSON value of
    Error err -> pure (Left (SerializationError (Text.pack err)))
    Success output -> pure (Right output)

errorText :: DBOSError -> Text
errorText (WorkflowAlreadyExists text) = text
errorText (WorkflowNotFound text) = text
errorText (WorkflowNameCollision text) = text
errorText (WorkflowExecutionError text) = text
errorText StepOutsideWorkflow = "step outside workflow"
errorText (UnexpectedStepError text) = text
errorText (SerializationError text) = text
errorText (DatabaseError text) = text
errorText (NotImplemented text) = text
