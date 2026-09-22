{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module DBOS.SystemDB.Types
  ( IdempotencyKey (..),
    MessageUUID (..),
    NotificationRow (..),
    QueueConflict (..),
    QueueName (..),
    SendMessage (..),
    Topic (..),
    internalQueueName,
    message,
    messageTo,
    messageUUIDForSend,
    notificationRowForMessage,
    nullTopicSentinel,
  )
where

import Data.Text (Text)
import DBOS.Transact.WorkflowExecutionTypes
  ( SerializedWorkflowValue,
    WorkflowId (..),
  )

newtype Topic = Topic Text
  deriving stock (Eq, Show)

newtype IdempotencyKey = IdempotencyKey Text
  deriving stock (Eq, Show)

newtype MessageUUID = MessageUUID Text
  deriving stock (Eq, Show)

newtype QueueName = QueueName Text
  deriving stock (Eq, Show)

-- | What a registration does when the queue row already exists. Mirrors the
-- oracle: 'UpdateIfLatestVersion' is an application's default,
-- 'AlwaysUpdate' an operator's, and 'NeverUpdate' leaves the row alone. With
-- no application versions registered yet, every application is the latest,
-- so 'UpdateIfLatestVersion' and 'AlwaysUpdate' coincide until version
-- registration is ported.
data QueueConflict
  = UpdateIfLatestVersion
  | AlwaysUpdate
  | NeverUpdate
  deriving stock (Eq, Show)

data SendMessage = SendMessage
  { sendDestinationId :: WorkflowId,
    sendMessageBody :: SerializedWorkflowValue,
    sendTopic :: Maybe Topic,
    sendIdempotencyKey :: Maybe IdempotencyKey
  }
  deriving stock (Eq, Show)

data NotificationRow = NotificationRow
  { notificationDestinationId :: WorkflowId,
    notificationTopic :: Text,
    notificationMessage :: SerializedWorkflowValue,
    notificationMessageUUID :: MessageUUID,
    notificationConsumed :: Bool
  }
  deriving stock (Eq, Show)

nullTopicSentinel :: Text
nullTopicSentinel = "__null__topic__"

-- | The queue abandoned work returns to. Mirrors @INTERNAL_QUEUE@: recovery
-- is a re-enqueue, and whichever executor next polls the queue runs it.
internalQueueName :: QueueName
internalQueueName = QueueName "_dbos_internal_queue"

-- | A message to a workflow on the default topic, mirroring @Message::new@.
message :: WorkflowId -> SerializedWorkflowValue -> SendMessage
message destination body =
  SendMessage
    { sendDestinationId = destination,
      sendMessageBody = body,
      sendTopic = Nothing,
      sendIdempotencyKey = Nothing
    }

-- | A message to a workflow on a named topic, mirroring
-- @Message { topic: Some(..), ..Message::new(..) }@.
messageTo :: WorkflowId -> Topic -> SerializedWorkflowValue -> SendMessage
messageTo destination topic body =
  (message destination body) {sendTopic = Just topic}

notificationRowForMessage :: MessageUUID -> SendMessage -> NotificationRow
notificationRowForMessage generatedUUID send =
  NotificationRow
    { notificationDestinationId = send.sendDestinationId,
      notificationTopic = maybe nullTopicSentinel (\(Topic topic) -> topic) (send.sendTopic),
      notificationMessage = send.sendMessageBody,
      notificationMessageUUID = messageUUIDForSend generatedUUID send,
      notificationConsumed = False
    }

-- | The stored id for a send: the idempotency key when one is given, the
-- generated fallback otherwise. Both branches are scoped per recipient: the
-- insert ends @ON CONFLICT (message_uuid) DO NOTHING@, so an unscoped
-- fallback shared across destinations would collide with itself and deliver
-- to one destination alone. Mirrors the oracle's @deliver@.
messageUUIDForSend :: MessageUUID -> SendMessage -> MessageUUID
messageUUIDForSend (MessageUUID fallback) send =
  let WorkflowId destination = send.sendDestinationId
   in case send.sendIdempotencyKey of
        Nothing -> MessageUUID (fallback <> "::" <> destination)
        Just (IdempotencyKey key) -> MessageUUID (key <> "::" <> destination)
