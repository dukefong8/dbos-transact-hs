{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Workflow message send and receive. Payload encoding lives at this engine
-- boundary; Postgres owns the atomic delivery, checkpoint replay and
-- consuming read.
module DBOS.Transact.Message
  ( Message (..),
    Forks (..),
    SendOptions (..),
    sendOptionsDefault,
    SendBulkOptions (..),
    sendBulkOptionsDefault,
    send,
    sendWith,
    sendBulk,
    sendBulkWith,
    recv,
  )
where

import DBOS.Prelude
import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Control.Monad.Class.MonadTime (MonadTime)
import Control.Monad.Class.MonadTimer (MonadDelay)
import Data.Aeson (FromJSON, ToJSON)
import Data.Text qualified as Text
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Types (Duration, IdempotencyKey, SendMessage (..), Serialization (..), SerializedWorkflowValue (..), Topic (..), WorkflowId (..), sendBulkStepName)
import DBOS.Transact.Serialization (CodecError (..), decodeWorkflowValue, encodeWorkflowValue)
import DBOS.Transact.Context (Ctx, insideAStep, nextStepId, stepId, withSystemDB, workflowId)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Step (runWorkflowStepWith, stepOptionsDefault)

-- | One message in a bulk send: the destination and payload, with the topic
-- and idempotency key that vary per message travelling on the message rather
-- than on the call. Mirrors Rust @Message@.
data Message value = Message
  { messageDestinationId :: WorkflowId,
    messageValue :: value,
    messageTopic :: Maybe Topic,
    messageIdempotencyKey :: Maybe IdempotencyKey
  }

-- | Whether a message also reaches the workflows forked from its
-- destination. Mirrors Rust @Forks@; the constructor prefix is the port's
-- collision deviation.
data Forks
  = ForksSkip
  | ForksInclude
  deriving stock (Eq, Show)

-- | The per-call options of a single send. Mirrors Rust @SendOptions@.
data SendOptions = SendOptions
  { topic :: Maybe Topic,
    idempotency_key :: Maybe IdempotencyKey,
    forks :: Forks
  }
  deriving stock (Eq, Show)

sendOptionsDefault :: SendOptions
sendOptionsDefault = SendOptions {topic = Nothing, idempotency_key = Nothing, forks = ForksSkip}

-- | The per-call options of a bulk send: only what is uniform across the
-- batch. Mirrors Rust @SendBulkOptions@.
data SendBulkOptions = SendBulkOptions
  { forks :: Forks
  }
  deriving stock (Eq, Show)

sendBulkOptionsDefault :: SendBulkOptions
sendBulkOptionsDefault = SendBulkOptions {forks = ForksSkip}

-- | Send a typed message to another workflow. At a workflow boundary the
-- send is checkpointed under its own operation id; inside a step it is plain
-- and the enclosing step's checkpoint stands for the send.
send :: (ToJSON value, MonadSTM m) => Ctx m -> WorkflowId -> Maybe Topic -> Maybe IdempotencyKey -> value -> m (Either (TransactError.Error TransactError.EngineOnly) ())
send ctx destination topic idempotencyKey value =
  sendWith ctx destination value (sendOptionsDefault {topic = topic, idempotency_key = idempotencyKey})

-- | 'send' with the options rather than the defaults: a topic, an
-- idempotency key, or the fork fan-out. Mirrors Rust @send_with@.
sendWith :: (ToJSON value, MonadSTM m) => Ctx m -> WorkflowId -> value -> SendOptions -> m (Either (TransactError.Error TransactError.EngineOnly) ())
sendWith ctx destination value options = do
  let workflowText = workflowId ctx
      encoded = encodeWorkflowValue value
      serialization = case encoded.serializedSerialization of
        Nothing -> Nothing
        Just (Serialization name) -> Just name
      message =
        SendMessage
          { sendDestinationId = destination,
            sendMessageBody = encoded,
            sendTopic = options.topic,
            sendIdempotencyKey = options.idempotency_key
          }
      sendToForks = options.forks == ForksInclude
  -- Inside a step the enclosing checkpoint stands for the send — through
  -- the handed context or a captured parent, read together.
  stepped <- insideAStep ctx
  caller <-
    if stepped
      then pure Nothing
      else do
        stepId' <- nextStepId ctx
        pure (Just (WorkflowId workflowText, stepId'))
  written <- withSystemDB ctx (\db -> SystemDB.sendMessage db message serialization caller sendToForks)
  pure $ case written of
    Left err -> Left (TransactError.ErrorSystemDatabase err)
    Right () -> Right ()

-- | Send many messages in one transaction: all or none. The batch is
-- checkpointed as one @DBOS.sendBulk@ step however long it is, so a replay
-- sends none of it again; inside a step it sends plainly, the enclosing
-- step's checkpoint standing for the send. Payloads are encoded before the
-- step id is taken, so a message that cannot be encoded is a send that never
-- happened. Mirrors Rust @send_bulk@.
sendBulk :: (ToJSON value, MonadSTM m, MonadDelay m, MonadTimer m, MonadTime m, MonadAsync m, MonadCatch m) => Ctx m -> [Message value] -> m (Either (TransactError.Error TransactError.EngineOnly) ())
sendBulk ctx messages = sendBulkWith ctx messages sendBulkOptionsDefault

-- | 'sendBulk' with the options rather than the defaults: the fork
-- fan-out. Mirrors Rust @send_bulk_with@.
sendBulkWith :: (ToJSON value, MonadSTM m, MonadDelay m, MonadTimer m, MonadTime m, MonadAsync m, MonadCatch m) => Ctx m -> [Message value] -> SendBulkOptions -> m (Either (TransactError.Error TransactError.EngineOnly) ())
sendBulkWith ctx messages options = do
  let encoded = map encodeMessage messages
      serialization = case encoded of
        (message : _) -> case message.sendMessageBody.serializedSerialization of
          Nothing -> Nothing
          Just (Serialization name) -> Just name
        [] -> Nothing
  case stepId ctx of
    Just _ -> plainSend ctx encoded serialization Nothing
    Nothing -> runWorkflowStepWith stepOptionsDefault ctx sendBulkStepName $ \stepCtx -> do
      let caller = (\sid -> (WorkflowId (workflowId stepCtx), sid)) <$> stepId stepCtx
      plainSend stepCtx encoded serialization caller
  where
    encodeMessage message =
      let encodedValue = encodeWorkflowValue message.messageValue
       in SendMessage
            { sendDestinationId = message.messageDestinationId,
              sendMessageBody = encodedValue,
              sendTopic = message.messageTopic,
              sendIdempotencyKey = message.messageIdempotencyKey
            }
    plainSend stepCtx encodedMessages serialization caller = do
      written <- withSystemDB stepCtx (\db -> SystemDB.sendMessages db encodedMessages serialization caller (options.forks == ForksInclude))
      pure (either (Left . TransactError.ErrorSystemDatabase) Right written)

-- | Consume the oldest matching message, waiting up to the polling duration.
-- It is checkpointed as a read plus timeout so replay returns the same
-- message (or absence) instead of consuming another one. Receives from
-- inside a step are refused because the enclosing step cannot identify the
-- consumed message on a retry.
recv :: (FromJSON value, MonadSTM m, MonadTime m, MonadDelay m) => Ctx m -> Maybe Topic -> Duration -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe value))
recv ctx topic timeout = do
  -- Refused through the handed context or a captured parent alike: the
  -- enclosing step cannot identify the consumed message on a retry.
  stepped <- insideAStep ctx
  if stepped
    then pure (Left (TransactError.InsideStep "recv"))
    else do
      let workflowText = workflowId ctx
      stepId' <- nextStepId ctx
      timeoutStepId <- nextStepId ctx
      let workflowId' = WorkflowId workflowText
          topicText = case topic of
            Nothing -> Nothing
            Just (Topic name) -> Just name
      found <- withSystemDB ctx (\db -> SystemDB.recv db workflowId' stepId' timeoutStepId topicText timeout)
      pure $ case found of
        Left err -> Left (TransactError.ErrorSystemDatabase err)
        Right Nothing -> Right Nothing
        Right (Just encoded) ->
          case decodeWorkflowValue "message" (Just (storedValue encoded)) of
            Left err -> Left (TransactError.ErrorDeserialization "message" (codecMessage err))
            Right value -> Right (Just value)
  where
    codecMessage err =
      case err of
        CodecNotJson _ input -> "invalid JSON: " <> input
        CodecTypeMismatch _ detail -> Text.pack detail
    storedValue encoded =
      SerializedWorkflowValue
        { serializedText = encoded.encodedValue,
          serializedSerialization = Serialization <$> encoded.encodedSerialization
        }
