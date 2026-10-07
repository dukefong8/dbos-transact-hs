{-# LANGUAGE OverloadedStrings #-}

-- | Interaction-surface mock data for sim trees: the notification, event,
-- and stream answers plus the payload bodies mock receives and reads
-- serve. Owned here (not in the backend) so trees correlate through the
-- same values the backend serves. Duplicated across domains on purpose.
module DBOS.Transact.ContextSimData
  ( mockTimestamp,
    mockMessageBody,
    mockEventBody,
    mockStreamBody,
    mockNotification,
    mockEvent,
    mockStreamRecord,
  )
where

import DBOS.Prelude
import DBOS.SystemDB
  ( EventRecord (..),
    NotificationRecord (..),
    StreamRecord (..),
    Timestamp,
    timestampFromEpochMs,
  )

mockTimestamp :: Timestamp
mockTimestamp = timestampFromEpochMs 1000

-- | The message body mock receives answer with.
mockMessageBody :: Text
mockMessageBody = "mock-message"

-- | The event payload mock reads answer with.
mockEventBody :: Text
mockEventBody = "mock-event"

-- | The stream payload mock reads answer with.
mockStreamBody :: Text
mockStreamBody = "mock-stream"

mockEvent :: EventRecord
mockEvent = EventRecord {eventKey = "mock-key", eventValue = "\"mock\"", eventSerialization = Just "rust_serde"}

mockNotification :: NotificationRecord
mockNotification =
  NotificationRecord
    { notificationRecordMessageUuid = "mock-message-uuid",
      notificationRecordTopic = Nothing,
      notificationRecordMessage = "\"mock\"",
      notificationRecordSerialization = Just "rust_serde",
      notificationRecordCreatedAt = mockTimestamp,
      notificationRecordConsumed = False
    }

mockStreamRecord :: StreamRecord
mockStreamRecord =
  StreamRecord
    { streamKey = "mock-key",
      streamOffset = 0,
      streamValue = "\"mock\"",
      streamSerialization = Just "rust_serde",
      streamStepId = 0
    }
