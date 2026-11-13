{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | Workflow-scoped event reads and writes. The SystemDB owns their
-- transactional checkpoint behavior; this module allocates operation ids
-- from the explicit context and decodes their serialized values.
module DBOS.Internal.Transact.Event
  ( setEvent,
    getEvent,
    pendingGetEvent,
    pendingSetEvent,
  )
where

import Data.Aeson (FromJSON, ToJSON)
import Data.Text qualified as Text
import DBOS.Internal.Prelude
import DBOS.Internal.SystemDB.Class qualified as SystemDB
import DBOS.Internal.SystemDB.Error (Error (..))
import DBOS.Internal.SystemDB.Types (Duration, EncodedValue (..), GetEventCaller (..), Serialization (..), SerializedWorkflowValue (..), WorkflowId (..), getEventStepName, setEventStepName)
import DBOS.Internal.Transact.Checkpoint (PendingStep (..), StepDurability (..), StepPlacement (..), checkHere, placeCall, takenPlacement)
import DBOS.Internal.Transact.Connection (Connection (..), runSystemDB)
import DBOS.Internal.Transact.Context (WorkflowCtx, insideAStep, nextStepId, ownerToken, stepCtxBoundary, withSystemDB, workflowId)
import DBOS.Internal.Transact.Error qualified as TransactError
import DBOS.Internal.Transact.Instance (DBOS, Executor (..), requireExecutor)
import DBOS.Internal.Transact.Serialization (SerializationError, codecErrorText, codecUnknownTag, decodeWorkflowValue, encodeWorkflowValue)
import DBOS.Internal.Transact.Serialization qualified as Serialization

-- | Publish a value on the current workflow. A write is a checkpointed
-- operation and is refused from inside a step, where allocating another
-- operation id would shift replay order.
setEvent :: (ToJSON value, MonadSTM m)
         => WorkflowCtx exec m -> Text -> value -> m (Either (TransactError.Error TransactError.EngineOnly) ())
setEvent wctx key value =
  let encoded = forceEncoded value
   in encoded `seq` (placeCall wctx >>= driveSetEvent wctx key encoded)

-- | A publish built at its position and not yet run: the id is claimed at
-- the call so a replay rebuilds the same slot, and the write runs when the
-- pending value is awaited or raced.
pendingSetEvent :: (ToJSON value, MonadSTM m)
                => WorkflowCtx exec m -> Text -> value -> m (PendingStep exec m (Either (TransactError.Error TransactError.EngineOnly) ()))
pendingSetEvent wctx key value =
  let encoded = forceEncoded value
   in encoded `seq` do
        placement <- placeCall wctx
        pure (PendingStep setEventStepName (Just placement) (driveSetEvent wctx key encoded placement))

-- | Encode before the id is claimed: a value whose encoder throws must be
-- a publish that never happened, not one that spent a step id. Forcing the
-- text field forces the JSON encode.
forceEncoded :: ToJSON value => value -> SerializedWorkflowValue
forceEncoded value =
  let encoded = encodeWorkflowValue value
   in encoded.serializedText `seq` encoded

-- | Drives a placed publish: refused from inside a step, otherwise the
-- checkpointed write under the claimed id.
driveSetEvent :: MonadSTM m
              => WorkflowCtx exec m -> Text -> SerializedWorkflowValue -> StepPlacement exec m -> m (Either (TransactError.Error TransactError.EngineOnly) ())
driveSetEvent wctx key encoded placement =
  case checkHere placement setEventStepName (Just (stepCtxBoundary wctx)) of
    Left err -> pure (Left err)
    -- No plain form exists: a publish built in a step can never run, so
    -- driving it reports the refusal the eager call would have raised.
    Right DurabilityPlain -> pure (Left (TransactError.InsideStep "set_event"))
    Right (DurabilityRecorded wctx' stepId') -> do
      let serialization = case encoded.serializedSerialization of
            Nothing                   -> Nothing
            Just (Serialization name) -> Just name
          workflowText = workflowId wctx'
      written <-
        withSystemDB
          wctx'
          ( \db ->
              SystemDB.setEvent
                db
                (WorkflowId workflowText)
                stepId'
                key
                encoded.serializedText
                serialization
                (ownerToken wctx')
          )
      pure $ case written of
        Left err -> Left (TransactError.fromExecution err)
        Right () -> Right ()

-- | Read an event of another workflow, waiting up to the polling duration.
-- The read belongs to the destination, the checkpoint to the caller. Outside
-- a step the read and its timeout each own an operation id, so replay
-- observes the same value (including absence) as the original execution.
-- Inside a step the enclosing step checkpoint stands for the read, so it
-- runs plainly. Mirrors Rust @get_event(workflow_id, key, timeout)@.
getEvent :: (FromJSON value, MonadSTM m, MonadTime m, MonadDelay m)
         => WorkflowCtx exec m -> WorkflowId -> Text -> Duration -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe value))
getEvent wctx destination key timeout = do
  let workflowText = workflowId wctx
  -- Inside a step the enclosing checkpoint stands for the read — through
  -- the handed context or a captured parent, read together.
  stepped <- insideAStep wctx
  caller <-
    if stepped
      then pure Nothing
      else do
        readStep <- nextStepId wctx
        timeoutStep <- nextStepId wctx
        pure
          ( Just
              GetEventCaller
                { getEventCallerWorkflowId = WorkflowId workflowText,
                  getEventCallerStepId = readStep,
                  getEventCallerTimeoutStepId = timeoutStep,
                  getEventCallerToken = ownerToken wctx
                }
          )
  found <- withSystemDB wctx (\db -> SystemDB.getEvent db destination key timeout caller)
  pure (adoptEventValue found)

-- | The recorded form of a read answer: a system-database failure stays
-- engine-shaped, an absence stays an absence, and a present value decodes
-- back into the caller's channel. Shared by the eager read and the pending
-- drive, which differ only in whose connection serves them and which ids
-- the read was claimed under.
adoptEventValue :: FromJSON value
                => Either Error (Maybe EncodedValue) -> Either (TransactError.Error e) (Maybe value)
adoptEventValue found = case found of
  Left err -> Left (TransactError.fromExecution err)
  Right Nothing -> Right Nothing
  Right (Just encoded) ->
    case decodeWorkflowValue "event value" (Just (toStoredValue encoded)) of
      Left err -> Left (case codecUnknownTag err of
        Just tag -> TransactError.fromSerializationError (Serialization.UnknownSerialization {name = tag})
        Nothing  -> TransactError.fromSerializationError (Serialization.Deserialization "event value" (codecErrorText err)))
      Right value -> Right (Just value)
  where
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
pendingGetEvent :: (FromJSON value, MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m)
                => DBOS m ->
                   WorkflowCtx exec m ->
                   WorkflowId ->
                   Text ->
                   Duration ->
                   m (PendingStep exec m (Either (TransactError.Error c) (Maybe value)))
pendingGetEvent dbos wctx destination key timeout = do
  -- Stays on 'requireExecutor': the unlaunched branch answers a 'PendingStep'
  -- refusal, not a bare 'Left', so 'withExecutor' does not fit.
  running <- requireExecutor dbos "get_event"
  case running of
    Left err -> pure (PendingStep getEventStepName Nothing (pure (Left (TransactError.liftEngine err))))
    Right executor -> do
      placed <- takenPlacement executor.conn "get_event" wctx
      case placed of
        Left err -> pure (PendingStep getEventStepName Nothing (pure (Left err)))
        Right placement -> case placement of
          Recorded wctx' readStep -> do
            timeoutStep <- nextStepId wctx'
            let caller = Just (GetEventCaller (WorkflowId (workflowId wctx')) readStep timeoutStep (ownerToken wctx'))
            pure (PendingStep getEventStepName (Just placement) (driveGetEvent executor caller destination key timeout))
          _ -> pure (PendingStep getEventStepName (Just placement) (driveGetEvent executor Nothing destination key timeout))

-- | Drives a placed read: the recorded form of whatever the database
-- answers, decoded back into the caller's channel.
driveGetEvent :: (FromJSON value, MonadMVar m, MonadTime m, MonadDelay m)
              => Executor m ->
                 Maybe GetEventCaller ->
                 WorkflowId ->
                 Text ->
                 Duration ->
                 m (Either (TransactError.Error c) (Maybe value))
driveGetEvent executor caller destination key timeout = do
  found <- runSystemDB executor.conn.connSysdb (\db -> SystemDB.getEvent db destination key timeout caller)
  pure (adoptEventValue found)
