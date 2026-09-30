{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Internal step runner (Rule 4: plain Haskell, no Bluefin imports).
-- Mirrors @step.rs@ for plain steps: run the body once and record its
-- output, or replay the recorded outcome without running the body.
-- A throwing body propagates: the failure is recorded at the workflow
-- level by 'runWorkflow', never as a step error row — a step that throws
-- re-runs on recovery by design.
module DBOS.Transact.Step
  ( StepError (..),
    runWorkflowStep,
    runWorkflowStepWith,
    stepOptionsDefault,
    stepBackoff,
    StepOptions (..),
    ShouldRetry,
  )
where

import DBOS.Prelude
import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Control.Monad.Class.MonadTime (MonadTime)
import Control.Monad.Class.MonadTimer (MonadDelay, MonadTimer, threadDelay)
import Data.Aeson (FromJSON, ToJSON)
import Data.Text (Text, pack)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (Duration (..), Outcome (..), Serialization (..), SerializedWorkflowValue (..), StepRecord (..), StepTiming (..), WorkflowId (..), WorkflowRecord (..), WorkflowStatus (..), timestampNow)
import DBOS.SystemDB.Types (secondsDuration)
import DBOS.Tracer (WorkflowEvent (..), traceWith)
import DBOS.Transact.Serialization (CodecError (..), decodeWorkflowValue, encodeWorkflowValue)
import DBOS.Transact.Config (serializerName)
import DBOS.Transact.Connection (Connection (..))
import DBOS.Transact.Context (Ctx, StepStatus (..), cancellationToken, cancelToken, contextTracer, currentConnection, firstStepStatus, inStep, nextStepId, nextStepMarker, withAttempt, withSystemDB, workflowId)
import DBOS.Transact.Error qualified as TransactError
import GHC.Stack (HasCallStack)

data StepError
  = StepRecordedError SerializedWorkflowValue
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
--
-- Leaf rule: a call made inside a step body runs plainly — no id is
-- allocated and nothing is checkpointed — mirroring @StepDurability::Plain@.
--
-- Run and replay announcements go through the context's tracer, so the
-- same call sites log to FastLogger in production and to the io-sim trace
-- in simulations with no logger argument at all.
runWorkflowStep :: (FromJSON value, ToJSON value, MonadSTM m, MonadTime m, MonadCatch m, HasCallStack) => Ctx m -> Text -> (Ctx m -> m value) -> m (Either TransactError.Error value)
runWorkflowStep ctx name body
  | inStep ctx = do
      value <- body ctx
      pure (Right value)
  | otherwise = do
      let workflowId' = WorkflowId (workflowId ctx)
      stepId' <- nextStepId ctx
      -- 'started_at' covers the lookup round-trip, as in the oracle.
      startedAt <- timestampNow
      checked <- withSystemDB ctx (\db -> SystemDB.checkStep db workflowId' stepId' name)
      case checked of
        Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
        Right (Just recorded) -> do
          traceWith (contextTracer ctx) (StepReplaying name stepId')
          pure (replayWorkflowStep name stepId' recorded)
        Right Nothing -> do
          traceWith (contextTracer ctx) (StepRunning name stepId')
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
--
-- 'ShouldRetry' is Rust's @ShouldRetry@ predicate alias (minus the @Arc@:
-- predicates here are plain functions, shared by reference, not by atomics).
type ShouldRetry = TransactError.Error -> Bool

data StepOptions = StepOptions
  { max_attempts :: Int,
    interval :: Duration,
    backoff_rate :: Double,
    max_interval :: Duration,
    timeout :: Maybe Duration,
    preemptible :: Bool,
    should_retry :: Maybe ShouldRetry
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
--
-- Leaf rule, as in 'runWorkflowStep': inside a step body the call runs
-- plainly once, uncheckpointed.
runWorkflowStepWith ::
  (FromJSON value, ToJSON value, MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) =>
  StepOptions ->
  Ctx m ->
  Text ->
  (Ctx m -> m (Either TransactError.Error value)) ->
  m (Either TransactError.Error value)
runWorkflowStepWith options ctx name body
  | inStep ctx = body ctx
  | otherwise = do
      let workflowId' = WorkflowId (workflowId ctx)
      stepId' <- nextStepId ctx
      -- 'started_at' covers the lookup round-trip, as in the oracle.
      startedAt <- timestampNow
      checked <- withSystemDB ctx (\db -> SystemDB.checkStep db workflowId' stepId' name)
      case checked of
        Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
        Right (Just recorded) -> pure (replayWorkflowStep name stepId' recorded)
        Right Nothing -> do
          outcome <- attemptLoop workflowId' stepId' 1 []
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
    attemptLoop workflowId' stepId' attempt failures = do
      -- A preemptible step observes an externally cancelled workflow before
      -- every attempt and stops without checkpointing, as the oracle's
      -- preemption does. Plain steps never poll.
      preempted <- if options.preemptible then checkCancelled workflowId' else pure False
      if preempted
        then pure (Left (TransactError.ErrorSystemDatabase (SystemDBError.WorkflowCancelled {workflowId = workflowId ctx})))
        else do
          marker <- nextStepMarker ctx
          rest workflowId' stepId' attempt failures marker
    rest workflowId' stepId' attempt failures marker = do
      let status =
            (firstStepStatus stepId')
              { current_attempt = fromIntegral attempt,
                max_attempts = fromIntegral attempts
              }
      result <-
        withAttempt ctx marker status $ \inner -> case options.timeout of
          Nothing -> body inner
          Just limit -> do
            task <- async (body inner)
            finished <- race (threadDelay (durationMicros limit)) (wait task)
            case finished of
              -- The token fires before the body is dropped, as in the
              -- oracle: work watching it stops first, then the task dies.
              Left () -> do
                token <- cancellationToken inner
                cancelToken token
                cancel task
                pure (Left (TransactError.StepTimeout {step = name, timeout = limit}))
              Right value -> pure value
      case result of
        Right value -> pure (Right value)
        Left err
          | isControlError err -> pure (Left err)
          | attempt >= attempts -> pure (Left (finalFailure attempt (failures <> [err])))
          | declined err -> pure (Left (finalFailure attempt (failures <> [err])))
          | otherwise -> do
              threadDelay (durationMicros (stepBackoff options (attempt - 1)))
              attemptLoop workflowId' stepId' (attempt + 1) (failures <> [err])
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
    checkCancelled workflowId' = do
      found <- withSystemDB ctx (\db -> SystemDB.getWorkflow db workflowId')
      pure $ case found of
        Right (Just WorkflowRecord {workflowRecordStatus = status}) -> status == Cancelled
        _ -> False

durationMicros :: Duration -> Int
durationMicros (Duration interval) = round (interval * 1000000)
