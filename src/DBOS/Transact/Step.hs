{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Internal step runner (Rule 4: plain Haskell, no Bluefin imports).
-- Mirrors @step.rs@ for plain steps: run the body once and record its
-- output, or replay the recorded outcome without running the body.
-- Recorded failures come back as 'StepRecordedError'; child-workflow and
-- awaited checkpoints never occur for plain steps and are reported as
-- 'StepUnexpectedCheckpoint'. A throwing body propagates: the failure is
-- recorded at the workflow level by 'runWorkflow', never as a step error
-- row — step error rows only ever replay values 'recordOperationError'
-- wrote, so a step that throws re-runs on recovery by design.
-- The store is a parameter, so the same runner executes against Postgres in
-- production and against an in-memory model under @io-sim@.
module DBOS.Transact.Step
  ( StepError (..),
    runStep,
    runWorkflowStep,
    runWorkflowStepWith,
    stepOptionsDefault,
    stepBackoff,
    StepOptions (..),
    sleepStep,
  )
where

import DBOS.Prelude
import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Control.Monad.Class.MonadTime (MonadTime)
import Control.Monad.Class.MonadTimer (MonadDelay, MonadTimer, threadDelay, timeout)
import Data.Aeson (FromJSON, ToJSON)
import Data.Text (Text, pack, unpack)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Postgres
  ( Pool,
    fetchOperationCheckpoint,
    legacyRecordSleep,
  )
import DBOS.SystemDB.Types (Duration (..), Outcome (..), Serialization (..), SerializedWorkflowValue (..), StepRecord (..), StepTiming (..), WorkflowId (..), durationAsMillis, timestampFromEpochMs, timestampNow, timestampToEpochMs)
import DBOS.SystemDB.Types (secondsDuration, sleepStepName)
import DBOS.Transact.Codec (CodecError (..), decodeWorkflowValue, encodeWorkflowValue)
import DBOS.Transact.Config (serializerName)
import DBOS.Transact.Connection (Connection (..))
import DBOS.Transact.Context (Ctx, StepStatus (..), currentConnection, firstStepStatus, nextStepId, nextStepMarker, withAttempt, withSystemDB, workflowId)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.OperationCheckpointTypes
  ( OperationCheckpoint (..),
    OperationCheckpointResult (..),
    OperationId,
    OperationName (..),
  )
import DBOS.Transact.Store (StepStore (..))
import Data.Int (Int64)
import Data.Word (Word)

data StepError
  = StepRecordedError SerializedWorkflowValue
  | StepUnexpectedCheckpoint OperationCheckpoint
  | StepDatabaseError SystemDBError.Error
  | StepDecodeFailure CodecError
  | StepUnexpectedChildWorkflow WorkflowId
  | StepMissingRecordedOutput Int
  | StepInsideStep Text
  deriving stock (Eq, Show)

-- | Execute or replay one checkpointed operation through the SystemDB
-- class. The id is allocated from the execution's counter on the explicit
-- context; the body sees that id in the context it is handed only while it
-- runs. Body exceptions escape without a checkpoint, so shutdown and
-- infrastructure failures remain recoverable rather than being recorded as
-- application results.
runWorkflowStep :: (FromJSON value, ToJSON value, MonadSTM m, MonadTime m) => Ctx m -> Text -> (Ctx m -> m value) -> m (Either TransactError.Error value)
runWorkflowStep ctx name body = do
  let workflowId' = WorkflowId (workflowId ctx)
  stepId' <- nextStepId ctx
  checked <- withSystemDB ctx (\db -> SystemDB.checkStep db workflowId' stepId' name)
  case checked of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    Right (Just recorded) -> pure (replayWorkflowStep name stepId' recorded)
    Right Nothing -> do
      startedAt <- timestampNow
      marker <- nextStepMarker ctx
      value <- withAttempt ctx marker (firstStepStatus stepId') body
      completedAt <- timestampNow
      let encoded = encodeWorkflowValue value
          serialization = case encoded.serializedSerialization of
            Nothing -> Nothing
            Just (Serialization name') -> Just name'
      written <-
        withSystemDB
          ctx
          ( \db ->
              SystemDB.recordStep
                db
                workflowId'
                stepId'
                name
                (OutcomeOutput (Just encoded.serializedText))
                serialization
                (Just (StepTiming startedAt completedAt))
          )
      pure $ case written of
        Left err -> Left (TransactError.ErrorSystemDatabase err)
        Right () -> Right value

-- | The recorded outcome of a step, replayed without entering the body.
replayWorkflowStep :: FromJSON value => Text -> Int -> StepRecord -> Either TransactError.Error value
replayWorkflowStep name stepId record =
  case record.stepRecordChildWorkflowId of
    Just child -> Left (TransactError.StepFailed name ("unexpected child workflow checkpoint: " <> workflowIdText child))
    Nothing -> case record.stepRecordError of
      Just errorText -> Left (TransactError.StepFailed name errorText)
      Nothing -> case record.stepRecordOutput of
        Nothing -> Left (TransactError.StepFailed name ("recorded step " <> pack (show stepId) <> " has no output"))
        Just output ->
          case
              decodeWorkflowValue
                "result"
                (Just (SerializedWorkflowValue output (Serialization <$> record.stepRecordSerialization)))
            of
            Left err -> Left (TransactError.ErrorDeserialization "result" (codecMessage err))
            Right value -> Right value
  where
    workflowIdText (WorkflowId child) = child
    codecMessage err =
      case err of
        CodecNotJson _ input -> "invalid JSON: " <> input
        CodecTypeMismatch _ detail -> pack detail

-- | How a step retries, times out and decides. Mirrors Rust @StepOptions@.
-- Defaults are the three-way majority: one attempt (no retrying), a
-- one-second interval, a 2.0 rate, and a one-hour cap.
data StepOptions = StepOptions
  { max_attempts :: Int,
    interval :: Duration,
    backoff_rate :: Double,
    max_interval :: Duration,
    timeout :: Maybe Duration,
    preemptible :: Bool,
    should_retry :: Maybe (TransactError.Error -> Bool)
  }

-- | The defaults above: a plain step does not retry.
stepOptionsDefault :: StepOptions
stepOptionsDefault =
  StepOptions
    { max_attempts = 1,
      interval = secondsDuration 1,
      backoff_rate = 2.0,
      max_interval = secondsDuration 3600,
      timeout = Nothing,
      preemptible = False,
      should_retry = Nothing
    }

instance Show StepOptions where
  show options =
    "StepOptions {max_attempts = "
      <> show options.max_attempts
      <> ", interval = "
      <> show options.interval
      <> ", backoff_rate = "
      <> show options.backoff_rate
      <> ", max_interval = "
      <> show options.max_interval
      <> ", timeout = "
      <> show options.timeout
      <> ", preemptible = "
      <> show options.preemptible
      <> ", should_retry = "
      <> show (maybe False (const True) options.should_retry)
      <> "}"

-- | The wait before the attempt following @failures@ failures:
-- @interval * rate^failures@, capped, with a nonsensical rate saturating to
-- the cap rather than aborting. Mirrors @StepOptions::backoff@.
stepBackoff :: StepOptions -> Int -> Duration
stepBackoff options failures =
  let Duration interval = options.interval
      Duration cap = options.max_interval
      grown = realToFrac interval * (options.backoff_rate ** fromIntegral failures)
      capSeconds = realToFrac cap
   in if isNaN grown || isInfinite grown || grown < 0 || grown >= capSeconds
        then options.max_interval
        else Duration (realToFrac grown)

-- | 'runWorkflowStep' with the oracle's retry seam: the body returns its
-- failure as a value, the options decide whether it is worth retrying, each
-- attempt gets its own timeout, and the final outcome is checkpointed once.
-- A control error (a system-database failure, a cancellation) ends the step
-- without a checkpoint, so a cancelled or interrupted step re-runs rather
-- than replaying as permanently failed.
runWorkflowStepWith ::
  (FromJSON value, ToJSON value, MonadSTM m, MonadDelay m, MonadTimer m, MonadTime m) =>
  StepOptions ->
  Ctx m ->
  Text ->
  (Ctx m -> m (Either TransactError.Error value)) ->
  m (Either TransactError.Error value)
runWorkflowStepWith options ctx name body = do
  let workflowId' = WorkflowId (workflowId ctx)
  stepId' <- nextStepId ctx
  checked <- withSystemDB ctx (\db -> SystemDB.checkStep db workflowId' stepId' name)
  case checked of
    Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
    Right (Just recorded) -> pure (replayWorkflowStep name stepId' recorded)
    Right Nothing -> do
      startedAt <- timestampNow
      outcome <- attemptLoop stepId' 1 []
      completedAt <- timestampNow
      let timing = Just (StepTiming startedAt completedAt)
          serialization = Just (serializerName (currentConnection ctx).connSerializer)
      written <- case outcome of
        Right value -> do
          let encoded = encodeWorkflowValue value
              outputSerialization = case encoded.serializedSerialization of
                Nothing -> serialization
                Just (Serialization encodedName) -> Just encodedName
          withSystemDB
            ctx
            ( \db ->
                SystemDB.recordStep
                  db
                  workflowId'
                  stepId'
                  name
                  (OutcomeOutput (Just encoded.serializedText))
                  outputSerialization
                  timing
            )
        Left err ->
          withSystemDB
            ctx
            ( \db ->
                SystemDB.recordStep
                  db
                  workflowId'
                  stepId'
                  name
                  (OutcomeError (TransactError.renderTransactError err))
                  serialization
                  timing
            )
      pure $ case written of
        Left err -> Left (TransactError.ErrorSystemDatabase err)
        Right () -> outcome
  where
    attempts = max 1 options.max_attempts
    attemptLoop stepId' attempt failures = do
      marker <- nextStepMarker ctx
      let status =
            (firstStepStatus stepId')
              { current_attempt = fromIntegral attempt,
                max_attempts = fromIntegral attempts
              }
      result <-
        withAttempt ctx marker status $ \inner -> case options.timeout of
          Nothing -> body inner
          Just limit -> do
            timed <- timeout (durationMicros limit) (body inner)
            pure $ case timed of
              Nothing -> Left (TransactError.StepTimeout {step = name, timeout = limit})
              Just outcome -> outcome
      case result of
        Right value -> pure (Right value)
        Left err
          | isControlError err -> pure (Left err)
          | attempt >= attempts -> pure (Left (finalFailure attempt (failures <> [err])))
          | declined err -> pure (Left (finalFailure attempt (failures <> [err])))
          | otherwise -> do
              threadDelay (durationMicros (stepBackoff options (attempt - 1)))
              attemptLoop stepId' (attempt + 1) (failures <> [err])
    finalFailure attemptCount errs
      | null (init errs) = last errs
      | otherwise =
          TransactError.MaxStepRetriesExceeded
            { step = name,
              attempts = attemptCount,
              errors = errs
            }
    declined err = case options.should_retry of
      Just predicate -> not (predicate err)
      Nothing -> False
    isControlError err = case err of
      TransactError.ErrorSystemDatabase _ -> True
      _ -> False

durationMicros :: Duration -> Int
durationMicros (Duration interval) = round (interval * 1000000)

runStep ::
  Monad m =>
  StepStore m ->
  WorkflowId ->
  OperationId ->
  OperationName ->
  m SerializedWorkflowValue ->
  m (Either StepError SerializedWorkflowValue)
runStep store workflowId operationId operationName body = do
  checkpoint <- (store.stepFetchResult) workflowId operationId
  case checkpoint of
    Nothing -> do
      output <- body
      (store.stepRecordOutput) workflowId operationId operationName output
      pure (Right output)
    Just recorded -> pure (replayRecorded recorded)
  where
    replayRecorded recorded =
      case recorded.checkpointResult of
        CheckpointOutput output -> Right output
        CheckpointError err -> Left (StepRecordedError err)
        _ -> Left (StepUnexpectedCheckpoint recorded)

-- | Durable sleep: record the wake time, wait until it, and on replay wait
-- only what is left of the original wait. Mirrors @sleep.rs@: the recorded
-- wake time is the step output and @completed_at@ is stamped at the wake
-- time, so an hour's sleep reads as an hour rather than an instant. The step
-- name is fixed (@DBOS.sleep@), as in the oracle.
sleepStep ::
  Pool ->
  WorkflowId ->
  OperationId ->
  Duration ->
  IO ()
sleepStep pool workflowId operationId duration = do
  checkpoint <- fetchOperationCheckpoint pool workflowId operationId
  case checkpoint of
    Just recorded ->
      case recorded.checkpointResult of
        CheckpointOutput output -> waitUntilRecorded output
        _ -> pure ()
    Nothing -> do
      now <- timestampNow
      let durationMs = fromInteger (durationAsMillis duration)
          wakeAtMs = timestampToEpochMs now + durationMs
      legacyRecordSleep
        pool
        workflowId
        operationId
        sleepOperationName
        (SerializedWorkflowValue (pack (show wakeAtMs)) (Just (Serialization "portable_json")))
        now
        (timestampFromEpochMs wakeAtMs)
      threadDelay (millisToMicros durationMs)

-- | The step name the oracle records every durable sleep under.
sleepOperationName :: OperationName
sleepOperationName = OperationName sleepStepName

-- | A replayed sleep waits until the instant the first run recorded, which
-- may already have passed — then it waits not at all. The stored text is a
-- bare integer (@show@ on @Text@ would add quotes and never parse).
waitUntilRecorded :: SerializedWorkflowValue -> IO ()
waitUntilRecorded output = do
  now <- timestampNow
  case reads (unpack output.serializedText) of
    [(wakeAt, _)] -> threadDelay (millisToMicros (max 0 (wakeAt - timestampToEpochMs now)))
    _ -> pure ()

millisToMicros :: Int64 -> Int
millisToMicros ms = fromIntegral (ms * 1000)
