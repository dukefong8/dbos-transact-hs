{-# LANGUAGE OverloadedStrings #-}

module DBOS.SystemDB.Types
  ( IdempotencyKey (..),
    MessageUUID (..),
    NotificationRow (..),
    SendMessage (..),
    Topic (..),
    notificationRowForMessage,
    nullTopicSentinel,
  )
where

import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.Transact.WorkflowExecutionTypes
  ( SerializedWorkflowValue,
    WorkflowId (..),
  )

newtype Topic = Topic Text
  deriving (Eq, Show)

newtype IdempotencyKey = IdempotencyKey Text
  deriving (Eq, Show)

newtype MessageUUID = MessageUUID Text
  deriving (Eq, Show)

data SendMessage = SendMessage
  { sendDestinationId :: WorkflowId,
    sendMessageBody :: SerializedWorkflowValue,
    sendTopic :: Maybe Topic,
    sendIdempotencyKey :: Maybe IdempotencyKey
  }
  deriving (Eq, Show)

data NotificationRow = NotificationRow
  { notificationDestinationId :: WorkflowId,
    notificationTopic :: Text,
    notificationMessage :: SerializedWorkflowValue,
    notificationMessageUUID :: MessageUUID,
    notificationConsumed :: Bool
  }
  deriving (Eq, Show)

nullTopicSentinel :: Text
nullTopicSentinel = "__null__topic__"

notificationRowForMessage :: MessageUUID -> SendMessage -> NotificationRow
notificationRowForMessage generatedUUID message =
  NotificationRow
    { notificationDestinationId = sendDestinationId message,
      notificationTopic = maybe nullTopicSentinel (\(Topic topic) -> topic) (sendTopic message),
      notificationMessage = sendMessageBody message,
      notificationMessageUUID = messageUUIDForSend generatedUUID message,
      notificationConsumed = False
    }

messageUUIDForSend :: MessageUUID -> SendMessage -> MessageUUID
messageUUIDForSend generatedUUID message =
  case sendIdempotencyKey message of
    Nothing -> generatedUUID
    Just (IdempotencyKey key) ->
      let WorkflowId destination = sendDestinationId message
       in MessageUUID (Text.concat [key, "::", destination])
