{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Workflow-scoped event reads and writes. The SystemDB owns their
-- transactional checkpoint behavior; this module allocates operation ids
-- from the explicit context and decodes their serialized values.
module DBOS.Transact.Event
  ( setEvent,
    getEvent,
  )
where

import DBOS.Prelude
import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Control.Monad.Class.MonadTime (MonadTime)
import Control.Monad.Class.MonadTimer (MonadDelay)
import Data.Aeson (FromJSON, ToJSON)
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Types (Duration, EncodedValue (..), GetEventCaller (..), Serialization (..), SerializedWorkflowValue (..), WorkflowId (..))
import DBOS.Transact.Codec (CodecError (..), decodeWorkflowValue, encodeWorkflowValue)
import DBOS.Transact.Context (Ctx, nextStepId, stepId, withSystemDB, workflowId)
import DBOS.Transact.Error qualified as TransactError

-- | Publish a value on the current workflow. A write is a checkpointed
-- operation and is refused from inside a step, where allocating another
-- operation id would shift replay order.
setEvent :: (ToJSON value, MonadSTM m) => Ctx m -> Text -> value -> m (Either TransactError.Error ())
setEvent ctx key value =
  case stepId ctx of
    Just _ -> pure (Left (TransactError.InsideStep "set_event"))
    Nothing -> do
      let encoded = encodeWorkflowValue value
          serialization = case encoded.serializedSerialization of
            Nothing -> Nothing
            Just (Serialization name) -> Just name
          workflowText = workflowId ctx
      stepId' <- nextStepId ctx
      written <-
        withSystemDB
          ctx
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

-- | Read an event of another workflow, waiting up to the polling duration.
-- The read belongs to the destination, the checkpoint to the caller. Outside
-- a step the read and its timeout each own an operation id, so replay
-- observes the same value (including absence) as the original execution.
-- Inside a step the enclosing step checkpoint stands for the read, so it
-- runs plainly. Mirrors Rust @get_event(workflow_id, key, timeout)@.
getEvent :: (FromJSON value, MonadSTM m, MonadTime m, MonadDelay m) => Ctx m -> WorkflowId -> Text -> Duration -> m (Either TransactError.Error (Maybe value))
getEvent ctx destination key timeout = do
  let workflowText = workflowId ctx
  caller <- case stepId ctx of
    Just _ -> pure Nothing
    Nothing -> do
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
  pure $ case found of
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
