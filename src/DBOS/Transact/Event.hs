{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Workflow-scoped event reads and writes. The SystemDB owns their
-- transactional checkpoint behavior; this module allocates operation ids
-- from the explicit context and decodes their serialized values.
module DBOS.Transact.Event
  ( setEvent,
    setEventScoped,
    getEvent,
    getEventScoped,
    pendingGetEvent,
    pendingGetEventScoped,
    pendingSetEvent,
    pendingSetEventScoped,
  )
where

import DBOS.Prelude
import Control.Concurrent.Class.MonadMVar (MonadMVar)
import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Control.Monad.Class.MonadTime (MonadTime)
import Control.Monad.Class.MonadTimer (MonadDelay)
import Data.Aeson (FromJSON, ToJSON)
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Types (Duration, EncodedValue (..), GetEventCaller (..), Serialization (..), SerializedWorkflowValue (..), WorkflowId (..), getEventStepName, setEventStepName)
import DBOS.Transact.Serialization (CodecError (..), decodeWorkflowValue, encodeWorkflowValue)
import DBOS.Transact.Checkpoint (PendingStep (..), StepDurability (..), StepPlacement (..), checkHere, placeCall, takenPlacement)
import DBOS.Transact.Connection (Connection (..), runSystemDB)
import DBOS.Transact.Context (Ctx, WorkflowCtx, insideAStep, nextStepId, withSystemDB, workflowId, workflowCtxInner)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Instance (DBOS, Executor (..), requireExecutor)

-- | Publish a value on the current workflow. A write is a checkpointed
-- operation and is refused from inside a step, where allocating another
-- operation id would shift replay order.
setEvent :: (ToJSON value, MonadSTM m) => Ctx m -> Text -> value -> m (Either (TransactError.Error TransactError.EngineOnly) ())
setEvent ctx key value = placeCall ctx >>= driveSetEvent ctx key value

-- | A publish built at its position and not yet run: the id is claimed at
-- the call so a replay rebuilds the same slot, and the write runs when the
-- pending value is awaited or raced.
pendingSetEvent :: forall exec value m. (ToJSON value, MonadSTM m) => Ctx m -> Text -> value -> m (PendingStep exec m (Either (TransactError.Error TransactError.EngineOnly) ()))
pendingSetEvent ctx key value = do
  placement <- placeCall ctx
  pure (PendingStep setEventStepName (Just placement) (driveSetEvent ctx key value placement))

-- | Drives a placed publish: refused from inside a step, otherwise the
-- checkpointed write under the claimed id.
driveSetEvent :: (ToJSON value, MonadSTM m) => Ctx m -> Text -> value -> StepPlacement m -> m (Either (TransactError.Error TransactError.EngineOnly) ())
driveSetEvent ctx key value placement =
  case checkHere placement setEventStepName (Just ctx) of
    Left err -> pure (Left err)
    -- No plain form exists: a publish built in a step can never run, so
    -- driving it reports the refusal the eager call would have raised.
    Right DurabilityPlain -> pure (Left (TransactError.InsideStep "set_event"))
    Right (DurabilityRecorded ctx' stepId') -> do
      let encoded = encodeWorkflowValue value
          serialization = case encoded.serializedSerialization of
            Nothing -> Nothing
            Just (Serialization name) -> Just name
          workflowText = workflowId ctx'
      written <-
        withSystemDB
          ctx'
          ( \db ->
              SystemDB.setEvent
                db
                (WorkflowId workflowText)
                stepId'
                key
                encoded.serializedText
                serialization
          )
      pure $ case written of
        Left err -> Left (TransactError.ErrorSystemDatabase err)
        Right () -> Right ()

-- | 'setEvent' over the scoped workflow view.
setEventScoped :: (ToJSON value, MonadSTM m) => WorkflowCtx exec m -> Text -> value -> m (Either (TransactError.Error TransactError.EngineOnly) ())
setEventScoped wctx key value = setEvent (workflowCtxInner wctx) key value

-- | 'pendingSetEvent' over the scoped workflow view.
pendingSetEventScoped :: (ToJSON value, MonadSTM m) => WorkflowCtx exec m -> Text -> value -> m (PendingStep exec m (Either (TransactError.Error TransactError.EngineOnly) ()))
pendingSetEventScoped wctx key value = pendingSetEvent (workflowCtxInner wctx) key value

-- | 'getEvent' over the scoped workflow view.
getEventScoped :: (FromJSON value, MonadSTM m, MonadTime m, MonadDelay m) => WorkflowCtx exec m -> WorkflowId -> Text -> Duration -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe value))
getEventScoped wctx destination key timeout = getEvent (workflowCtxInner wctx) destination key timeout

-- | 'pendingGetEvent' over the scoped workflow view: the named instance
-- still serves the read, the scope comes from the caller's execution.
pendingGetEventScoped ::
  (FromJSON value, MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m) =>
  WorkflowCtx exec m ->
  DBOS m ->
  WorkflowId ->
  Text ->
  Duration ->
  m (PendingStep exec m (Either (TransactError.Error c) (Maybe value)))
pendingGetEventScoped wctx dbos destination key timeout =
  pendingGetEvent dbos (workflowCtxInner wctx) destination key timeout

-- | Read an event of another workflow, waiting up to the polling duration.
-- The read belongs to the destination, the checkpoint to the caller. Outside
-- a step the read and its timeout each own an operation id, so replay
-- observes the same value (including absence) as the original execution.
-- Inside a step the enclosing step checkpoint stands for the read, so it
-- runs plainly. Mirrors Rust @get_event(workflow_id, key, timeout)@.
getEvent :: (FromJSON value, MonadSTM m, MonadTime m, MonadDelay m) => Ctx m -> WorkflowId -> Text -> Duration -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe value))
getEvent ctx destination key timeout = do
  let workflowText = workflowId ctx
  -- Inside a step the enclosing checkpoint stands for the read — through
  -- the handed context or a captured parent, read together.
  stepped <- insideAStep ctx
  caller <-
    if stepped
      then pure Nothing
      else do
        readStep <- nextStepId ctx
        timeoutStep <- nextStepId ctx
        pure
          ( Just
              GetEventCaller
                { getEventCallerWorkflowId = WorkflowId workflowText,
                  getEventCallerStepId = readStep,
                  getEventCallerTimeoutStepId = timeoutStep
                }
          )
  found <- withSystemDB ctx (\db -> SystemDB.getEvent db destination key timeout caller)
  pure (adoptEventValue found)

-- | The recorded form of a read answer: a system-database failure stays
-- engine-shaped, an absence stays an absence, and a present value decodes
-- back into the caller's channel. Shared by the eager read and the pending
-- drive, which differ only in whose connection serves them and which ids
-- the read was claimed under.
adoptEventValue :: FromJSON value => Either SystemDB.Error (Maybe EncodedValue) -> Either (TransactError.Error e) (Maybe value)
adoptEventValue found = case found of
  Left err -> Left (TransactError.ErrorSystemDatabase err)
  Right Nothing -> Right Nothing
  Right (Just encoded) ->
    case decodeWorkflowValue "event value" (Just (toStoredValue encoded)) of
      Left err -> Left (TransactError.ErrorDeserialization "event value" (codecMessage err))
      Right value -> Right (Just value)
  where
    codecMessage err =
      case err of
        CodecNotJson _ input -> "invalid JSON: " <> input
        CodecTypeMismatch _ detail -> Text.pack detail
    toStoredValue encoded =
      SerializedWorkflowValue
        { serializedText = encoded.encodedValue,
          serializedSerialization = Serialization <$> encoded.encodedSerialization
        }

-- | An event read built at its position and not yet run: what
-- @DBOS::get_event@ becomes when the call site holds a context. The
-- executor comes from the named instance and the step ids from the ambient
-- execution, so a read through another instance is refused before anything
-- is claimed or read; inside a step the read is plain, with nothing to
-- disagree about. A refusal is carried in the pending value, so the caller
-- keeps its single error channel. Mirrors Rust @DBOS::get_event@.
pendingGetEvent ::
  (FromJSON value, MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m) =>
  DBOS m ->
  Ctx m ->
  WorkflowId ->
  Text ->
  Duration ->
  m (PendingStep exec m (Either (TransactError.Error c) (Maybe value)))
pendingGetEvent dbos ctx destination key timeout = do
  running <- requireExecutor dbos "get_event"
  case running of
    Left err -> pure (PendingStep getEventStepName Nothing (pure (Left (TransactError.liftEngine err))))
    Right executor -> do
      placed <- takenPlacement executor.conn "get_event" ctx
      case placed of
        Left err -> pure (PendingStep getEventStepName Nothing (pure (Left err)))
        Right placement -> case placement of
          Recorded ctx' readStep -> do
            timeoutStep <- nextStepId ctx'
            let caller = Just (GetEventCaller (WorkflowId (workflowId ctx')) readStep timeoutStep)
            pure (PendingStep getEventStepName (Just placement) (driveGetEvent executor caller destination key timeout))
          _ -> pure (PendingStep getEventStepName (Just placement) (driveGetEvent executor Nothing destination key timeout))

-- | Drives a placed read: the recorded form of whatever the database
-- answers, decoded back into the caller's channel.
driveGetEvent ::
  (FromJSON value, MonadMVar m, MonadTime m, MonadDelay m) =>
  Executor m ->
  Maybe GetEventCaller ->
  WorkflowId ->
  Text ->
  Duration ->
  m (Either (TransactError.Error c) (Maybe value))
driveGetEvent executor caller destination key timeout = do
  found <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.getEvent db destination key timeout caller)
  pure (adoptEventValue found)
