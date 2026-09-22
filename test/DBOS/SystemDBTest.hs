{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module DBOS.SystemDBTest
  ( tests,
  )
where

import DBOS.Transact
  ( IdempotencyKey (..),
    MessageUUID (..),
    NotificationRow (..),
    SendMessage (..),
    SerializedWorkflowValue (..),
    Serialization (..),
    Topic (..),
    WorkflowId (..),
    notificationRowForMessage,
    nullTopicSentinel,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), testCase)

tests :: TestTree
tests =
  testGroup
    "System DB"
    [ testCase "maps Python SendMessage without topic to notifications sentinel topic" $
        notificationRowForMessage
          (MessageUUID "generated-message-id")
          (SendMessage (WorkflowId "dest-wf") messageBody Nothing Nothing)
          @?= NotificationRow
            { notificationDestinationId = WorkflowId "dest-wf",
              notificationTopic = nullTopicSentinel,
              notificationMessage = messageBody,
              notificationMessageUUID = MessageUUID "generated-message-id::dest-wf",
              notificationConsumed = False
            },
      testCase "maps Python SendMessage topic into a notifications row" $
        notificationRowForMessage
          (MessageUUID "generated-message-id")
          (SendMessage (WorkflowId "dest-wf") messageBody (Just (Topic "testtopic")) Nothing)
          @?= NotificationRow
            { notificationDestinationId = WorkflowId "dest-wf",
              notificationTopic = "testtopic",
              notificationMessage = messageBody,
              notificationMessageUUID = MessageUUID "generated-message-id::dest-wf",
              notificationConsumed = False
            },
      testCase "scopes Python send idempotency keys by destination workflow" $
        ( notificationRowForMessage
            (MessageUUID "ignored-generated-id")
            ( SendMessage
                (WorkflowId "dest-wf")
                messageBody
                Nothing
                (Just (IdempotencyKey "idem-key"))
            )
          ).notificationMessageUUID
          @?= MessageUUID "idem-key::dest-wf",
      testCase "scopes generated fallback ids by destination workflow" $
        ( notificationRowForMessage
            (MessageUUID "generated-message-id")
            (SendMessage (WorkflowId "dest-wf") messageBody Nothing Nothing)
          ).notificationMessageUUID
          @?= MessageUUID "generated-message-id::dest-wf"
    ]

messageBody :: SerializedWorkflowValue
messageBody =
  SerializedWorkflowValue
    { serializedText = "\"hello\"",
      serializedSerialization = Just (Serialization "json")
    }
