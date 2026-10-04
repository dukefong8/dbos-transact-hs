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
    WorkflowEvent (..),
    runWorkflowStep,
    runNestedStep,
    runWorkflowStepWith,
    pendingWorkflowStep,
    pendingWorkflowStepWith,
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
import System.Log.FastLogger (ToLogStr (..))
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (Duration (..), Outcome (..), Serialization (..), SerializedWorkflowValue (..), StepRecord (..), StepTiming (..), WorkflowId (..), WorkflowRecord (..), WorkflowStatus (..), durationAsMillis, timestampNow)
import DBOS.SystemDB.Types (secondsDuration)
import DBOS.Tracer (LogEvent (..), LogSeverity (..), runTracer)
import DBOS.Transact.Serialization (CodecError (..), decodeWorkflowValue, encodeWorkflowValue)
import DBOS.Transact.Config (serializerName)
import DBOS.Transact.Connection (Connection (..))
import DBOS.Transact.Checkpoint (PendingStep (..), StepDurability (..), StepPlacement (..), checkHere, placeCall)
import DBOS.Transact.Context (Ctx, StepCtx, StepStatus (..), WorkflowCtx, cancellationToken, cancelToken, contextTracer, currentConnection, firstStepStatus, inStep, nextStepId, nextStepMarker, stepCtxAt, stepCtxTracer, withAttempt, withSystemDB, workflowCtxInner, workflowId)
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

-- | Step-run and workflow-execution events: the announcements
-- 'runWorkflowStep'/'runWorkflowStepWith' make across the retry seam —
-- plain runs, replays, control ends, declines, retries, recorded outcomes,
-- preemptions and timeouts — plus the execution announcements from
-- 'DBOS.Transact.Workflow' (joins, deadlines, outcomes). Mirrors @step.rs@
-- and @workflow.rs@: span fields ride on the constructors so FastLogger
-- lines carry the same @key=value@ pairs.
data WorkflowEvent
  = StepRunning { workflowStepName :: Text, workflowStepId :: Int }
  | StepReplaying { workflowStepName :: Text, workflowStepId :: Int }
  | StepPlain { plainStepName :: Text }
  | StepControlEnded { controlStepName :: Text, controlStepId :: Int, controlAttempt :: Int }
  | StepDeclined { declinedStepName :: Text, declinedStepId :: Int, declinedAttempt :: Int, declinedDetail :: Text }
  | StepRetrying { retryingStepName :: Text, retryingStepId :: Int, retryingAttempt :: Int, retryingAttempts :: Int, retryingBackoffMs :: Integer, retryingDetail :: Text }
  | StepOutputRecorded { recordedStepName :: Text, recordedStepId :: Int }
  | StepErrorRecorded { recordedStepName :: Text, recordedStepId :: Int }
  | StepPreempted { preemptedStepName :: Text, preemptedStepId :: Int }
  | StepAttemptTimedOut { timedOutStepName :: Text, timedOutStepId :: Int, timedOutAfterMs :: Integer }
  | WorkflowDedupJoined { workflowHolderId :: Text, workflowDeduplicationId :: Text }
  | WorkflowChildJoined { workflowParentId :: Text, workflowParentStepId :: Int, workflowJoinedChildId :: Text }
  | WorkflowEnqueued { workflowEnqueuedId :: Text, workflowEnqueuedQueue :: Text }
  | WorkflowAlreadyOwned { workflowOwnedId :: Text }
  | WorkflowDeadlineCancelled { workflowDeadlineId :: Text }
  | WorkflowDeadlineRaced { workflowRacedId :: Text }
  | WorkflowDeadlineRecordFailed { workflowDeadlineFailedId :: Text, workflowDeadlineDetail :: Text }
  | WorkflowControlEnded { workflowControlDetail :: Text }
  | WorkflowOutcomeRecordFailed { workflowOutcomeDetail :: Text }
  | WorkflowCompleted { workflowCompletedId :: Text }
  | WorkflowFailed { workflowFailedId :: Text }
  | WorkflowSuperseded { workflowSupersededId :: Text }
  | WorkflowPanicked { workflowPanickedId :: Text }
  deriving stock (Eq, Show)

instance LogEvent WorkflowEvent where
  eventSeverity StepRunning {}                  = SeverityDebug
  eventSeverity StepReplaying {}                = SeverityDebug
  eventSeverity StepPlain {}                    = SeverityDebug
  eventSeverity StepControlEnded {}             = SeverityDebug
  eventSeverity StepDeclined {}                 = SeverityDebug
  eventSeverity StepRetrying {}                 = SeverityWarning
  eventSeverity StepOutputRecorded {}           = SeverityDebug
  eventSeverity StepErrorRecorded {}            = SeverityDebug
  eventSeverity StepPreempted {}                = SeverityDebug
  eventSeverity StepAttemptTimedOut {}          = SeverityDebug
  eventSeverity WorkflowDedupJoined {}          = SeverityDebug
  eventSeverity WorkflowChildJoined {}          = SeverityDebug
  eventSeverity WorkflowEnqueued {}             = SeverityDebug
  eventSeverity WorkflowAlreadyOwned {}         = SeverityDebug
  eventSeverity WorkflowDeadlineCancelled {}    = SeverityInfo
  eventSeverity WorkflowDeadlineRaced {}        = SeverityWarning
  eventSeverity WorkflowDeadlineRecordFailed {} = SeverityError
  eventSeverity WorkflowControlEnded {}         = SeverityWarning
  eventSeverity WorkflowOutcomeRecordFailed {}  = SeverityWarning
  eventSeverity WorkflowCompleted {}            = SeverityDebug
  eventSeverity WorkflowFailed {}               = SeverityDebug
  eventSeverity WorkflowSuperseded {}           = SeverityWarning
  eventSeverity WorkflowPanicked {}             = SeverityError
  renderEvent (StepRunning name stepId') = "running step " <> name <> " (" <> showText stepId' <> ")"
  renderEvent (StepReplaying name stepId') = "replaying recorded step " <> name <> " (" <> showText stepId' <> ")"
  renderEvent (StepPlain name) =
    "the step body runs plainly step_name=" <> name
  renderEvent (StepControlEnded name stepId' attempt) =
    "a control signal ended the step; it is not retried and nothing is checkpointed step_id=" <> showText stepId' <> " step_name=" <> name <> " attempt=" <> showText attempt
  renderEvent (StepDeclined name stepId' attempt detail) =
    "the retry predicate declined this failure; the step is not retried step_id=" <> showText stepId' <> " step_name=" <> name <> " attempt=" <> showText attempt <> " error=" <> detail
  renderEvent (StepRetrying name stepId' attempt attempts backoffMs detail) =
    "the step failed and will be retried step_id=" <> showText stepId' <> " step_name=" <> name <> " attempt=" <> showText attempt <> " attempts=" <> showText attempts <> " backoff_ms=" <> showText backoffMs <> " error=" <> detail
  renderEvent (StepOutputRecorded name stepId') =
    "the step ran; its output is recorded step_id=" <> showText stepId' <> " step_name=" <> name
  renderEvent (StepErrorRecorded name stepId') =
    "the step failed; its error is recorded step_id=" <> showText stepId' <> " step_name=" <> name
  renderEvent (StepPreempted name stepId') =
    "the workflow was cancelled elsewhere; the step is preempted and records nothing step_name=" <> name <> " step_id=" <> showText stepId'
  renderEvent (StepAttemptTimedOut name stepId' afterMs) =
    "the step attempt exceeded its timeout and was stopped step_name=" <> name <> " step_id=" <> showText stepId' <> " timeout_ms=" <> showText afterMs
  renderEvent (WorkflowDedupJoined holderId deduplicationId) =
    "the deduplication key is held; the handle joins its holder workflow_id=" <> holderId <> " deduplication_id=" <> deduplicationId
  renderEvent (WorkflowChildJoined parentId parentStepId childId) =
    "the child workflow was already started; the handle joins it parent_workflow_id=" <> parentId <> " step_id=" <> showText parentStepId <> " workflow_id=" <> childId
  renderEvent (WorkflowEnqueued workflowId queue) =
    "the workflow is enqueued workflow_id=" <> workflowId <> " queue=" <> queue
  renderEvent (WorkflowAlreadyOwned workflowId) =
    "the workflow is already owned; the handle joins the existing run workflow_id=" <> workflowId
  renderEvent (WorkflowDeadlineCancelled workflowId) =
    "the workflow exceeded its deadline and is cancelled workflow_id=" <> workflowId
  renderEvent (WorkflowDeadlineRaced workflowId) =
    "the deadline fired on a workflow another execution had already finished; its recorded outcome stands workflow_id=" <> workflowId
  renderEvent (WorkflowDeadlineRecordFailed workflowId detail) =
    "could not record the deadline cancellation; the row stays PENDING for recovery workflow_id=" <> workflowId <> " error=" <> detail
  renderEvent (WorkflowControlEnded detail) =
    "a control signal ended this execution: it records no outcome of its own error=" <> detail
  renderEvent (WorkflowOutcomeRecordFailed detail) =
    "could not record the workflow's outcome: the row stays PENDING error=" <> detail
  renderEvent (WorkflowCompleted workflowId) =
    "the workflow completed; its output is recorded workflow_id=" <> workflowId
  renderEvent (WorkflowFailed workflowId) =
    "the workflow failed; its error is recorded workflow_id=" <> workflowId
  renderEvent (WorkflowSuperseded workflowId) =
    "another execution recorded this workflow's outcome first workflow_id=" <> workflowId
  renderEvent (WorkflowPanicked workflowId) =
    "the workflow body panicked: no outcome is recorded, and the row stays PENDING for a later executor to recover workflow_id=" <> workflowId

instance ToLogStr WorkflowEvent where
  toLogStr = toLogStr . renderLine

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
runWorkflowStep :: (FromJSON value, FromJSON e, ToJSON value, MonadSTM m, MonadTime m, MonadCatch m, HasCallStack) => WorkflowCtx exec m -> Text -> (StepCtx exec m -> m value) -> m (Either (TransactError.Error e) value)
runWorkflowStep wctx name body
  | inStep ctx = do
      runTracer (contextTracer ctx) (StepPlain name)
      value <- body (stepCtxAt wctx ctx)
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
          runTracer (contextTracer ctx) (StepReplaying name stepId')
          pure (replayWorkflowStep name stepId' recorded)
        Right Nothing -> do
          runTracer (contextTracer ctx) (StepRunning name stepId')
          marker <- nextStepMarker ctx
          value <- withAttempt ctx marker (firstStepStatus stepId') (\inner -> body (stepCtxAt wctx inner))
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
          case written of
            Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
            Right () -> do
              runTracer (contextTracer ctx) (StepOutputRecorded name stepId')
              pure (Right value)
  where
    ctx = workflowCtxInner wctx

-- | The step-scope entry: a call made inside a step body runs plainly —
-- no id is allocated and nothing is checkpointed — mirroring the leaf
-- rule. It cannot allocate even if asked: the narrowed view exposes no
-- allocator, and the allocating entry demands the workflow view this
-- scope does not hold.
runNestedStep ::
  Monad m =>
  StepCtx exec m ->
  Text ->
  (StepCtx exec m -> m value) ->
  m (Either (TransactError.Error e) value)
runNestedStep sctx name body = do
  runTracer (stepCtxTracer sctx) (StepPlain name)
  Right <$> body sctx

-- | The recorded outcome of a step, replayed without entering the body.
replayWorkflowStep :: (FromJSON value, FromJSON e) => Text -> Int -> StepRecord -> Either (TransactError.Error e) value
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
type ShouldRetry e = TransactError.Error e -> Bool

data StepOptions e = StepOptions
  { max_attempts :: Int,
    interval :: Duration,
    backoff_rate :: Double,
    max_interval :: Duration,
    timeout :: Maybe Duration,
    preemptible :: Bool,
    should_retry :: Maybe (ShouldRetry e)
  }

-- | The defaults above: a plain step does not retry.
stepOptionsDefault :: StepOptions e
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

instance Show (StepOptions e) where
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
stepBackoff :: StepOptions e -> Int -> Duration
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
  (FromJSON value, ToJSON value, FromJSON e, ToJSON e, Show e, MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) =>
  StepOptions e ->
  WorkflowCtx exec m ->
  Text ->
  (StepCtx exec m -> m (Either (TransactError.Error e) value)) ->
  m (Either (TransactError.Error e) value)
runWorkflowStepWith options wctx name body =
  placeCall ctx >>= \placement -> driveWorkflowStepWith options ctx name placement (\inner -> body (stepCtxAt wctx inner))
  where
    ctx = workflowCtxInner wctx

-- | A durable step built at its position and not yet run: the call claims
-- its id here, where it is written, and 'pendingRun' drives exactly what
-- 'driveWorkflowStepWith' does when the pending value is awaited or raced.
-- Mirrors Rust @PendingStep@ for steps; a pending value that is dropped
-- unconsumed still spent its id, which is what keeps a replay's numbering
-- stable.
pendingWorkflowStepWith ::
  (FromJSON value, ToJSON value, FromJSON e, ToJSON e, Show e, MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) =>
  StepOptions e ->
  WorkflowCtx exec m ->
  Text ->
  (StepCtx exec m -> m (Either (TransactError.Error e) value)) ->
  m (PendingStep exec m (Either (TransactError.Error e) value))
pendingWorkflowStepWith options wctx name body = do
  let ctx = workflowCtxInner wctx
  placement <- placeCall ctx
  pure
    PendingStep
      { name = name,
        placement = Just placement,
        pendingRun = driveWorkflowStepWith options ctx name placement (\inner -> body (stepCtxAt wctx inner))
      }

-- | 'pendingWorkflowStepWith' with the default options: a plain step.
pendingWorkflowStep ::
  (FromJSON value, ToJSON value, FromJSON e, ToJSON e, Show e, MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) =>
  WorkflowCtx exec m ->
  Text ->
  (StepCtx exec m -> m (Either (TransactError.Error e) value)) ->
  m (PendingStep exec m (Either (TransactError.Error e) value))
pendingWorkflowStep = pendingWorkflowStepWith stepOptionsDefault

-- | Drives a placed call: check where it stands, replay its recorded row,
-- or run and record it. Building and driving are separate so a race can
-- build every branch — claiming every id — before any branch runs.
driveWorkflowStepWith ::
  (FromJSON value, ToJSON value, FromJSON e, ToJSON e, Show e, MonadSTM m, MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m) =>
  StepOptions e ->
  Ctx m ->
  Text ->
  StepPlacement m ->
  (Ctx m -> m (Either (TransactError.Error e) value)) ->
  m (Either (TransactError.Error e) value)
driveWorkflowStepWith options ctx name placement body =
  case checkHere placement name (Just ctx) of
    Left err -> pure (Left err)
    Right DurabilityPlain -> do
      let inner = case placement of
            PlacementInsideStep built -> built
            _ -> ctx
      runTracer (contextTracer inner) (StepPlain name)
      body inner
    Right (DurabilityRecorded ctx' stepId') -> driveAt ctx' stepId'
  where
    driveAt ctx' stepId' = do
      let workflowId' = WorkflowId (workflowId ctx')
      -- 'started_at' covers the lookup round-trip, as in the oracle.
      startedAt <- timestampNow
      checked <- withSystemDB ctx' (\db -> SystemDB.checkStep db workflowId' stepId' name)
      case checked of
        Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
        Right (Just recorded) -> do
          runTracer (contextTracer ctx') (StepReplaying name stepId')
          pure (replayWorkflowStep name stepId' recorded)
        Right Nothing -> do
          outcome <- attemptLoop workflowId' stepId' 1 []
          -- A control signal — a cancellation, an interruption, a database
          -- failure — is not an outcome: the step returns it with the row
          -- untouched, so a resume runs it again rather than replaying a
          -- verdict it never reached.
          case outcome of
            Left err | isControlError err -> pure (Left err)
            _ -> do
              completedAt <- timestampNow
              let timing = Just (StepTiming startedAt completedAt)
                  serialization = Just (serializerName (currentConnection ctx').connSerializer)
              written <- case outcome of
                Right value -> do
                  let encoded = encodeWorkflowValue value
                      outputSerialization = case encoded.serializedSerialization of
                        Nothing -> serialization
                        Just (Serialization encodedName) -> Just encodedName
                  withSystemDB
                    ctx'
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
                    ctx'
                    ( \db ->
                        SystemDB.recordStep
                          db
                          workflowId'
                          stepId'
                          name
                          (OutcomeError (TransactError.encodeErrorText err))
                          serialization
                          timing
                    )
              case written of
                Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
                Right () -> case outcome of
                  Right _ -> do
                    runTracer (contextTracer ctx') (StepOutputRecorded name stepId')
                    pure outcome
                  Left _ -> do
                    runTracer (contextTracer ctx') (StepErrorRecorded name stepId')
                    pure outcome
      where
        attempts = max 1 options.max_attempts
        attemptLoop workflowId' stepId' attempt failures = do
          -- A preemptible step observes an externally cancelled workflow before
          -- every attempt and stops without checkpointing, as the oracle's
          -- preemption does. Plain steps never poll.
          preempted <- if options.preemptible then checkCancelled workflowId' else pure False
          if preempted
            then do
              runTracer (contextTracer ctx') (StepPreempted name stepId')
              pure (Left (TransactError.ErrorSystemDatabase (SystemDBError.WorkflowCancelled {workflowId = workflowId ctx'})))
            else do
              marker <- nextStepMarker ctx'
              rest workflowId' stepId' attempt failures marker
        rest workflowId' stepId' attempt failures marker = do
          let status =
                (firstStepStatus stepId')
                  { current_attempt = fromIntegral attempt,
                    max_attempts = fromIntegral attempts
                  }
          result <-
            withAttempt ctx' marker status $ \inner -> case options.timeout of
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
                    runTracer (contextTracer ctx') (StepAttemptTimedOut name stepId' (durationAsMillis limit))
                    pure (Left (TransactError.StepTimeout {step = name, timeout = limit}))
                  Right value -> pure value
          case result of
            Right value -> pure (Right value)
            Left err
              | isControlError err -> do
                  runTracer (contextTracer ctx') (StepControlEnded name stepId' attempt)
                  pure (Left err)
              | attempt >= attempts -> pure (Left (finalFailure attempt (failures <> [err])))
              | declined err -> do
                  runTracer (contextTracer ctx') (StepDeclined name stepId' attempt (TransactError.renderTransactError err))
                  pure (Left (finalFailure attempt (failures <> [err])))
              | otherwise -> do
                  let backoff = stepBackoff options (attempt - 1)
                  runTracer (contextTracer ctx') (StepRetrying name stepId' attempt attempts (durationAsMillis backoff) (TransactError.renderTransactError err))
                  threadDelay (durationMicros backoff)
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
        isControlError err = case TransactError.controlOf err of
          Just _ -> True
          Nothing -> False
        checkCancelled workflowId' = do
          found <- withSystemDB ctx' (\db -> SystemDB.getWorkflow db workflowId')
          pure $ case found of
            Right (Just WorkflowRecord {workflowRecordStatus = status}) -> status == Cancelled
            _ -> False

durationMicros :: Duration -> Int
durationMicros (Duration interval) = round (interval * 1000000)
