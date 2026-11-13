{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Workflow message send and receive. Payload encoding lives at this engine
-- boundary; Postgres owns the atomic delivery, checkpoint replay and
-- consuming read.
module DBOS.Internal.Transact.Message
  ( SendMessage (..),
    Forks (..),
    SendOptions (..),
    sendOptionsDefault,
    send,
    sendWith,
    sendBulk,
    sendBulkWith,
    recv,
  )
where

import DBOS.Internal.Prelude
import Data.Aeson (FromJSON, ToJSON)
import Data.Text qualified as Text
import DBOS.Internal.SystemDB.Class qualified as SystemDB
import DBOS.Internal.SystemDB.Types (Duration, EncodedValue (..), IdempotencyKey, EncodedSendMessage (..), Serialization (..), SerializedWorkflowValue (..), Topic (..), WorkflowId (..))
import DBOS.Internal.Transact.Serialization (codecErrorText, codecUnknownTag, decodeWorkflowValue, encodeWorkflowValue)
import DBOS.Internal.Transact.Context (WorkflowCtx, insideAStep, nextStepId, ownerToken, trySync, withSystemDB, workflowId, workflowOwner)
import DBOS.Internal.Transact.Error qualified as TransactError

-- | One message in a bulk send: the destination and payload, with the topic
-- and idempotency key that vary per message travelling on the message rather
-- than on the call. Mirrors Rust @SendMessage@.
data SendMessage value = SendMessage
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

-- | What a send does about forks: per-call behaviour shared by single
-- and bulk sends, while destinations, payloads, topics, and idempotency
-- keys travel on the messages. Mirrors Rust @SendOptions@.
data SendOptions = SendOptions
  { sendToForks :: Forks
  }
  deriving stock (Eq, Show)

sendOptionsDefault :: SendOptions
sendOptionsDefault = SendOptions {sendToForks = ForksSkip}

-- | Send a typed message to another workflow. At a workflow boundary the
-- send is checkpointed under its own operation id; inside a step it is plain
-- and the enclosing step's checkpoint stands for the send.
send :: (ToJSON value, MonadSTM m, MonadCatch m, MonadThread m)
     => WorkflowCtx exec m -> WorkflowId -> Maybe Topic -> Maybe IdempotencyKey -> value -> m (Either (TransactError.Error TransactError.EngineOnly) ())
send wctx destination topic idempotencyKey value =
  sendWith wctx (SendMessage destination value topic idempotencyKey) sendOptionsDefault

-- | 'send' with the options rather than the defaults: the fork fan-out.
-- The message carries its destination, payload, topic, and idempotency
-- key. Mirrors Rust @send_with@.
sendWith :: forall value m exec. (ToJSON value, MonadSTM m, MonadCatch m, MonadThread m)
         => WorkflowCtx exec m -> SendMessage value -> SendOptions -> m (Either (TransactError.Error TransactError.EngineOnly) ())
sendWith wctx message options = do
  let workflowText = workflowId wctx
      encoded = encodeWorkflowValue message.messageValue
      serialization = case encoded.serializedSerialization of
        Nothing -> Nothing
        Just (Serialization name) -> Just name
      encodedMessage =
        EncodedSendMessage
          { sendDestinationId = message.messageDestinationId,
            sendMessageBody = encoded,
            sendTopic = message.messageTopic,
            sendIdempotencyKey = message.messageIdempotencyKey
          }
      fanOut = options.sendToForks == ForksInclude
  -- Force the payload before any id is taken: a throwing ToJSON is a send
  -- that never happened, not an id spent on a message that cannot be
  -- written.
  forced <- trySync (pure $! Text.length encoded.serializedText) :: m (Either SomeException Int)
  case forced of
    Left exn -> pure (Left (TransactError.ErrorSerialization {what = "message", message = showText exn}))
    Right _ -> do
      -- Inside a step the enclosing checkpoint stands for the send — through
      -- the handed context or a captured parent, read together.
      stepped <- insideAStep wctx
      caller <-
        if stepped
          then pure Nothing
          else do
            stepId' <- nextStepId wctx
            pure (Just (WorkflowId workflowText, stepId'))
      -- Workflow and step sends alike present the sending execution; the
      -- client path below presents no owner and skips the fence.
      written <- withSystemDB wctx (\db -> SystemDB.sendMessage db encodedMessage serialization caller (Just (workflowOwner wctx)) fanOut)
      pure $ case written of
        Left err -> Left (TransactError.fromExecution err)
        Right () -> Right ()

-- | Send many messages in one transaction: all or none. The batch is
-- checkpointed as one @DBOS.sendBulk@ step however long it is, so a replay
-- sends none of it again; inside a step it sends plainly, the enclosing
-- step's checkpoint standing for the send. Payloads are encoded before the
-- step id is taken, so a message that cannot be encoded is a send that never
-- happened. The insert and the step record commit in the same system-database
-- transaction, so a crash cannot leave the batch durable with no step row.
-- Mirrors Rust @send_bulk@ (@deliver@ records the step last in the same
-- transaction).
sendBulk :: (ToJSON value, MonadSTM m, MonadCatch m, MonadThread m)
         => WorkflowCtx exec m -> [SendMessage value] -> m (Either (TransactError.Error TransactError.EngineOnly) ())
sendBulk wctx messages = sendBulkWith wctx messages sendOptionsDefault

-- | 'sendBulk' with the options rather than the defaults: the fork
-- fan-out. Mirrors Rust @send_bulk_with@.
sendBulkWith :: forall value m exec. (ToJSON value, MonadSTM m, MonadCatch m, MonadThread m)
             => WorkflowCtx exec m -> [SendMessage value] -> SendOptions -> m (Either (TransactError.Error TransactError.EngineOnly) ())
sendBulkWith wctx messages options = do
  let encoded = map encodeMessage messages
      serialization = case encoded of
        (message : _) -> case message.sendMessageBody.serializedSerialization of
          Nothing -> Nothing
          Just (Serialization name) -> Just name
        [] -> Nothing
  -- Force every payload before the step id is taken: a message that
  -- cannot be encoded is a batch that never happened.
  forced <- trySync (mapM_ (\m -> pure $! Text.length m.sendMessageBody.serializedText) encoded) :: m (Either SomeException ())
  case forced of
    Left exn -> pure (Left (TransactError.ErrorSerialization {what = "message", message = showText exn}))
    Right () -> do
      -- Inside a step the enclosing checkpoint stands for the send, so no
      -- id is taken and nothing is recorded — exactly like a single send.
      -- Otherwise the step id is the caller's position, and the
      -- system database inserts the batch and records the step atomically.
      stepped <- insideAStep wctx
      caller <-
        if stepped
          then pure Nothing
          else do
            stepId' <- nextStepId wctx
            pure (Just (WorkflowId (workflowId wctx), stepId'))
      written <- withSystemDB wctx (\db -> SystemDB.sendMessages db encoded serialization caller (Just (workflowOwner wctx)) (options.sendToForks == ForksInclude))
      pure (either (Left . TransactError.fromExecution) Right written)
  where
    encodeMessage message =
      let encodedValue = encodeWorkflowValue message.messageValue
       in EncodedSendMessage
            { sendDestinationId = message.messageDestinationId,
              sendMessageBody = encodedValue,
              sendTopic = message.messageTopic,
              sendIdempotencyKey = message.messageIdempotencyKey
            }

-- | Consume the oldest matching message, waiting up to the polling duration.
-- It is checkpointed as a read plus timeout so replay returns the same
-- message (or absence) instead of consuming another one. Two waits on one
-- topic both wake, and one message goes to one of them; the other waits out
-- its deadline and records the absence (a replay of the step it shares with
-- the winner adopts that winner's record instead). Receives from inside a
-- step are refused because the enclosing step cannot identify the consumed
-- message on a retry.
recv :: (FromJSON value, MonadSTM m, MonadTime m, MonadDelay m, MonadThread m)
     => WorkflowCtx exec m -> Maybe Topic -> Duration -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe value))
recv wctx topic timeout = do
  -- Refused through the handed context or a captured parent alike: the
  -- enclosing step cannot identify the consumed message on a retry.
  stepped <- insideAStep wctx
  if stepped
    then pure (Left (TransactError.InsideStep "recv"))
    else do
      let workflowText = workflowId wctx
      stepId' <- nextStepId wctx
      timeoutStepId <- nextStepId wctx
      let workflowId' = WorkflowId workflowText
          topicText = case topic of
            Nothing -> Nothing
            Just (Topic name) -> Just name
      found <- withSystemDB wctx (\db -> SystemDB.recv db workflowId' stepId' timeoutStepId topicText timeout (ownerToken wctx))
      pure $ case found of
        Left err -> Left (TransactError.fromExecution err)
        Right Nothing -> Right Nothing
        Right (Just encoded) ->
          case decodeWorkflowValue "message" (Just (storedValue encoded)) of
            Left err -> Left (case codecUnknownTag err of
              Just tag -> TransactError.UnknownSerialization {name = tag}
              Nothing -> TransactError.Deserialization "message" (codecErrorText err))
            Right value -> Right (Just value)
  where
    storedValue encoded =
      SerializedWorkflowValue
        { serializedText = encoded.encodedValue,
          serializedSerialization = Serialization <$> encoded.encodedSerialization
        }
