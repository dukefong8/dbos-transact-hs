{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | A running or finished workflow, by id. Mirrors Rust @handle.rs@: every
-- reference agrees on the surface — the id, the result, the status — and
-- splits the implementation the same way. A handle to a workflow running in
-- this process awaits the running task directly; a handle to one running
-- elsewhere, or to one that finished before this process started, polls the
-- database. Dropping a handle stops watching, never the workflow.
--
-- The local-task await and the durable parent-side @DBOS.getResult@
-- checkpoint are L2 engine work (NOTE): this module polls the database,
-- which is the whole of the polling provenance and the whole of what
-- management and client surfaces hand back.
module DBOS.Transact.Handle
  ( -- * Handle
    WorkflowHandle (..),
    Provenance (..),
    pollingHandle,
    localHandle,
    handleWorkflowId,
    handleStatus,
    handleResult,
    awaitChild,
    pendingAwait,
  )
where

import DBOS.Prelude
import Data.Aeson (FromJSON, ToJSON)
import Data.Int (Int64)
import Data.Text (Text)
import Data.Text qualified as Text
import Control.Monad.Class.MonadTime (MonadTime)
import Control.Monad.Class.MonadTimer (MonadDelay)
import Control.Monad.Class.MonadThrow qualified as MThrow
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (AwaitedOutcome (..), Outcome (..), Serialization (..), SerializedWorkflowValue (..), StepRecord (..), StepTiming (..), Timestamp, WorkflowId (..), WorkflowStatus, getResultStepName, timestampNow)
import DBOS.Transact.Checkpoint (PendingStep (..), StepDurability (..), StepPlacement (..), checkHere, placeCall)
import DBOS.Transact.Config (serializerName)
import DBOS.Transact.Connection (Connection (..), runSystemDB)
import DBOS.Transact.Context (Ctx, LocalTaskOutcome (..))
import DBOS.Transact.Context qualified as Context (workflowId)
import DBOS.Transact.Error qualified as TransactError

-- | Where this handle's result comes from. The local channel carries the
-- erased @Failure@, as the oracle's spawned task does: the application
-- error type was serialized at the registration boundary, and the awaiting
-- caller decodes it back into its own channel.
data Provenance m
  = -- | The workflow runs elsewhere, or already finished: the database is
    -- the only witness. Whether the minting call saw the row decides what
    -- an absent row means.
    Polling {fail_if_missing :: Bool}
  | -- | The workflow runs in this process: the spawned task's outcome is
    -- read directly, without touching its row. Mirrors Rust
    -- @Provenance::Local@.
    Local (StrictMVar m (LocalTaskOutcome (Either TransactError.Failure (Maybe SerializedWorkflowValue))))

-- | The provenance's label, as the oracle's @Debug@ prints it.
instance Show (Provenance m) where
  show Polling {} = "polling"
  show Local {} = "local"

-- | A running or finished workflow, by id. Fields keep the Rust spelling:
-- @workflow_id@ names the workflow; the connection serves the reads and
-- carries the poll interval the handle watches at.
data WorkflowHandle m e = WorkflowHandle
  { conn        :: Connection m,
    workflow_id :: Text,
    provenance  :: Provenance m
  }

instance Show (WorkflowHandle m e) where
  show handle = "WorkflowHandle " <> Text.unpack handle.workflow_id <> " " <> show handle.provenance

-- | A handle over a workflow some other execution owns. Takes a connection
-- rather than an executor, which is what lets a client hand one back.
pollingHandle :: Connection m -> Text -> Bool -> WorkflowHandle m e
pollingHandle conn workflowId failIfMissing =
  WorkflowHandle
    { conn = conn,
      workflow_id = workflowId,
      provenance = Polling failIfMissing
    }

-- | A handle over the task this process spawned. The channel is filled
-- while the task unwinds, so dropping the handle stops watching, never the
-- workflow, and an aborted task still answers its waiter.
localHandle :: Connection m -> Text -> StrictMVar m (LocalTaskOutcome (Either TransactError.Failure (Maybe SerializedWorkflowValue))) -> WorkflowHandle m e
localHandle conn workflowId channel =
  WorkflowHandle
    { conn = conn,
      workflow_id = workflowId,
      provenance = Local channel
    }

-- | The workflow's id.
handleWorkflowId :: WorkflowHandle m e -> Text
handleWorkflowId handle = handle.workflow_id

-- | The workflow's status, as its row records it right now. An unknown id
-- reports the absence, because a single read has nothing to wait for.
handleStatus :: Monad m => WorkflowHandle m e -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe WorkflowStatus))
handleStatus handle = do
  result <- runSystemDB handle.conn.connSysdb (\db -> SystemDB.getWorkflow db (WorkflowId handle.workflow_id))
  pure $ case result of
    Left err            -> Left (TransactError.ErrorSystemDatabase err)
    Right Nothing       -> Right Nothing
    Right (Just record) -> Right (Just record.workflowRecordStatus)

-- | Waits for the workflow to finish and returns what it returned. Polls
-- the database: the local-task await is future engine work. This is the
-- ctx-less polling face, for client and management handles; inside a
-- workflow body reach for 'awaitChild' instead, or the wait goes
-- unrecorded and a replay decides it again.
handleResult :: (MonadDelay m, MonadTime m, MonadMVar m, MThrow.MonadThrow m, FromJSON e) => WorkflowHandle m e -> m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue))
handleResult handle = do
  settled <- settleOutcome handle
  pure (settled >>= settledResult handle.workflow_id)

-- | Await a child workflow from inside a workflow body, recording the wait
-- as a @DBOS.getResult@ step. Mirrors the oracle's @handle.result()@ where
-- its ambient context is a running workflow: the id is claimed at the call
-- site, before anything can fail, so a replay rebuilds the same position;
-- a recorded row is adopted rather than waited for again; and a settled
-- outcome (success, failure, cancellation) is recorded while an
-- interruption, a substrate failure and a parked child are not.
--
-- Inside a step body nothing is allocated and nothing is recorded: the
-- enclosing step's checkpoint stands for the wait, as in the oracle's leaf
-- rule.
--
-- The handle must belong to this execution: a recorded row under the
-- claimed id that names a different workflow is refused, never adopted.
awaitChild ::
  (MonadDelay m, MonadTime m, MonadSTM m, MonadMVar m, MThrow.MonadThrow m, FromJSON e, ToJSON e) =>
  Ctx m ->
  WorkflowHandle m e ->
  m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue))
awaitChild ctx handle = placeCall ctx >>= driveAwait ctx handle

-- | An await built at its position and not yet run: the @DBOS.getResult@
-- step claims its id here, where the call is written, and 'pendingRun'
-- drives the wait when the pending value is awaited or raced. Mirrors the
-- oracle's @handle.result()@ building a @PendingStep@; a pending await the
-- race drops still spent its id, and recorded nothing.
pendingAwait ::
  (MonadDelay m, MonadTime m, MonadSTM m, MonadMVar m, MThrow.MonadThrow m, FromJSON e, ToJSON e) =>
  Ctx m ->
  WorkflowHandle m e ->
  m (PendingStep m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue)))
pendingAwait ctx handle = do
  placement <- placeCall ctx
  pure
    PendingStep
      { name = getResultStepName,
        placement = Just placement,
        pendingRun = driveAwait ctx handle placement
      }

-- | Drives a placed await: adopt a recorded row, or wait and record the
-- settled outcome under the claimed id. The placement decides, so a race
-- can build every branch before any of them waits.
driveAwait ::
  (MonadDelay m, MonadTime m, MonadSTM m, MonadMVar m, MThrow.MonadThrow m, FromJSON e, ToJSON e) =>
  Ctx m ->
  WorkflowHandle m e ->
  StepPlacement m ->
  m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue))
driveAwait ctx handle placement =
  case checkHere placement getResultStepName (Just ctx) of
    Left err -> pure (Left err)
    Right DurabilityPlain -> fmap (>>= settledResult handle.workflow_id) (settleOutcome handle)
    Right (DurabilityRecorded ctx' stepId') -> recordedAt ctx' stepId'
  where
    recordedAt ctx' stepId' = do
      let parent = WorkflowId (Context.workflowId ctx')
      checked <- runSystemDB handle.conn.connSysdb (\db -> SystemDB.checkChildResult db parent stepId')
      case checked of
        Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
        Right (Just recorded) -> pure (adoptRecordedAwait ctx' handle stepId' recorded)
        Right Nothing -> do
          startedAt <- timestampNow
          settled <- settleOutcome handle
          completedAt <- timestampNow
          case settled of
            Left err -> pure (Left err)
            Right outcome -> do
              written <- recordAwait ctx' handle stepId' startedAt completedAt outcome
              pure (written >> settledResult handle.workflow_id outcome)

-- | What an awaited workflow did, before it is mapped onto the error
-- channel. The oracle's @AwaitedOutcome@ minus the shapes its await never
-- lets out (a void success is a success).
data Settled e
  = SettledSucceeded (Maybe Text) (Maybe Text)
  | SettledFailed (TransactError.Error e)
  | SettledCancelled
  | SettledParked Int64
  deriving stock (Eq, Show)

-- | The one wait behind both faces: 'handleResult' and 'awaitChild'. A
-- local task answers from its own channel — no row is read, and only
-- shutdown produces a cancellation — while a polling handle reads the row
-- at the interval the connection carries.
settleOutcome :: (MonadDelay m, MonadTime m, MonadMVar m, MThrow.MonadThrow m, FromJSON e) => WorkflowHandle m e -> m (Either (TransactError.Error e) (Settled e))
settleOutcome handle = case handle.provenance of
  Local channel -> do
    outcome <- readMVar channel
    case outcome of
      LocalTaskCancelled -> pure (Left (TransactError.Interrupted {workflowId = handle.workflow_id}))
      LocalTaskPanic err -> MThrow.throwIO err
      LocalTaskValue value -> pure $ case value of
        Right Nothing -> Right (SettledSucceeded Nothing Nothing)
        Right (Just stored) ->
          Right
            ( SettledSucceeded
                (Just stored.serializedText)
                (case stored.serializedSerialization of
                  Just (Serialization serializationName') -> Just serializationName'
                  Nothing -> Nothing
                )
            )
        -- The erased failure decodes back into this caller's channel; a
        -- control signal is handed back unrecorded — except a durable
        -- cancellation, which a polling read would see as the child's
        -- status and report as an awaited cancellation.
        Left failure -> case TransactError.failureError handle.workflow_id failure of
          err | isCancellation err -> Right SettledCancelled
          err | Just _ <- TransactError.controlOf err -> Left err
          err -> Right (SettledFailed err)
  Polling failMissing -> do
    awaited <-
      runSystemDB handle.conn.connSysdb (\db -> SystemDB.awaitWorkflowResult db (WorkflowId handle.workflow_id) handle.conn.connOutcomePollInterval failMissing)
    pure $ case awaited of
      Left err -> Left (TransactError.ErrorSystemDatabase err)
      Right (AwaitedSucceeded output serialization) -> Right (SettledSucceeded output serialization)
      Right (AwaitedFailed message _) -> Right (SettledFailed (decodedRecordedError handle.workflow_id message))
      Right AwaitedCancelled -> Right SettledCancelled
      Right (AwaitedParked attempts) -> Right (SettledParked attempts)
  where
    isCancellation err = case err of
      TransactError.ErrorSystemDatabase SystemDBError.WorkflowCancelled {} -> True
      _ -> False

-- | Map a settled outcome onto the result channel, as both faces report it.
settledResult :: Text -> Settled e -> Either (TransactError.Error e) (Maybe SerializedWorkflowValue)
settledResult workflowText settled =
  case settled of
    SettledSucceeded output serialization ->
      Right (SerializedWorkflowValue <$> output <*> pure (Serialization <$> serialization))
    SettledFailed err -> Left err
    SettledCancelled -> Left (TransactError.AwaitedWorkflowCancelled {workflowId = workflowText})
    SettledParked attempts ->
      Left
        ( TransactError.ErrorSystemDatabase
            ( SystemDBError.ErrorMaxRecoveryAttemptsExceeded
                { workflowId = workflowText,
                  limit = attempts
                }
            )
        )

-- | Record a settled await under its claimed id, with the awaited
-- workflow's id in the step's @child_workflow_id@. A parked child and a
-- failed wait record nothing: freezing either into the parent's replay
-- would outlive its own truth.
recordAwait ::
  (MonadTime m, ToJSON e) =>
  Ctx m ->
  WorkflowHandle m e ->
  Int ->
  Timestamp ->
  Timestamp ->
  Settled e ->
  m (Either (TransactError.Error e) ())
recordAwait ctx handle stepId' startedAt completedAt settled =
  case settled of
    SettledSucceeded output _ -> record (OutcomeOutput output)
    SettledFailed err -> record (OutcomeError (TransactError.encodeErrorText err))
    SettledCancelled ->
      record
        ( OutcomeError
            ( TransactError.encodeErrorText
                (TransactError.AwaitedWorkflowCancelled {workflowId = handle.workflow_id} :: (TransactError.Error TransactError.EngineOnly))
            )
        )
    SettledParked _ -> pure (Right ())
  where
    parent = WorkflowId (Context.workflowId ctx)
    record outcome =
      fmap (either (Left . TransactError.ErrorSystemDatabase) Right) $
        runSystemDB handle.conn.connSysdb $ \db ->
          SystemDB.recordChildResult
            db
            parent
            stepId'
            (WorkflowId handle.workflow_id)
            outcome
            (Just (serializerName handle.conn.connSerializer))
            (Just (StepTiming startedAt completedAt))

-- | The replayed form of an await: adopt the recorded row, and only when
-- it names the workflow this handle stands for.
adoptRecordedAwait :: FromJSON e => Ctx m -> WorkflowHandle m e -> Int -> StepRecord -> Either (TransactError.Error e) (Maybe SerializedWorkflowValue)
adoptRecordedAwait ctx handle stepId' recorded =
  case recorded.stepRecordChildWorkflowId of
    Just (WorkflowId recordedChild)
      | recordedChild == handle.workflow_id ->
          case (recorded.stepRecordOutput, recorded.stepRecordError) of
            (Just output, _) ->
              Right (Just (SerializedWorkflowValue output (Serialization <$> recorded.stepRecordSerialization)))
            (Nothing, Just message) -> Left (decodedRecordedError handle.workflow_id message)
            (Nothing, Nothing) ->
              Left
                ( TransactError.StepFailed
                    getResultStepName
                    ("recorded await " <> showText stepId' <> " has no outcome")
                )
    mismatched ->
      Left
        ( TransactError.ErrorSystemDatabase
            ( SystemDBError.UnexpectedStep
                { workflowId = Context.workflowId ctx,
                  stepId = stepId',
                  expected = "an await of " <> handle.workflow_id,
                  recorded = case mismatched of
                    Just (WorkflowId other) -> "an await of " <> other
                    Nothing                 -> "an await of no workflow at all"
                }
            )
        )

-- | A recorded failure text decoded back into the caller's channel, with
-- the oracle's fallback for a payload that cannot be decoded (a row written
-- by older code): the workflow is reported failed with the raw text.
decodedRecordedError :: FromJSON e => Text -> Text -> TransactError.Error e
decodedRecordedError workflowText message =
  case TransactError.decodeErrorText message of
    Right err -> err
    Left _ -> TransactError.ErrorWorkflowFailed {workflowId = workflowText, message = message}
