{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Operator-surface mock data for sim trees: the queue, schedule, and
-- version answers plus the canned output payload every mock wait reads.
-- Owned here (not in the backend) so @ManagementTestSim@ correlates its
-- inputs and assertions through the same values the backend serves.
-- Duplicated across domains on purpose.
module DBOS.Transact.ManagementSimData
  ( mockTimestamp,
    mockOutput,
    mockSerialization,
    mockQueue,
    mockQueueName,
    mockQueuedId,
    mockPartition,
    mockPartitionedId,
    mockDebounced,
    mockSchedule,
    mockScheduleName,
    mockVersion,
  )
where

import DBOS.Prelude
import DBOS.SystemDB
  ( QueueRecord (..),
    ScheduleRecord (..),
    ScheduleStatus (..),
    Timestamp,
    VersionInfo (..),
    WorkflowId (..),
    secondsDuration,
    timestampFromEpochMs,
  )

mockTimestamp :: Timestamp
mockTimestamp = timestampFromEpochMs 1000

-- | The output payload mock waits and reads answer with.
mockOutput :: Text
mockOutput = "mock-output"

-- | The serialization mock payloads claim.
mockSerialization :: Text
mockSerialization = "rust_serde"

mockQueue :: Text -> QueueRecord
mockQueue name =
  QueueRecord
    { queueRecordName = name,
      queueRecordConcurrency = Nothing,
      queueRecordWorkerConcurrency = Nothing,
      queueRecordRateLimit = Nothing,
      queueRecordPriorityEnabled = False,
      queueRecordPartitionQueue = False,
      queueRecordPartitionConcurrency = Nothing,
      queueRecordPartitionWorkerConcurrency = Nothing,
      queueRecordPartitionRateLimit = Nothing,
      queueRecordPollingInterval = secondsDuration 1,
      queueRecordApplicationName = Just "mock-app"
    }

-- | The queue the stateless backend answers with.
mockQueueName :: Text
mockQueueName = "mock-queue"

-- | The id the stateless backend answers queued starts with.
mockQueuedId :: WorkflowId
mockQueuedId = WorkflowId "mock-queued"

-- | The partition the stateless backend answers with.
mockPartition :: Text
mockPartition = "mock-partition"

-- | The id the stateless backend answers partitioned starts with.
mockPartitionedId :: WorkflowId
mockPartitionedId = WorkflowId "mock-partitioned"

-- | The debounce token the stateless backend answers with.
mockDebounced :: Text
mockDebounced = "mock-debounced"

mockSchedule :: Text -> ScheduleRecord
mockSchedule name =
  ScheduleRecord
    { scheduleRecordId = "mock-schedule-id",
      scheduleRecordName = name,
      scheduleRecordWorkflowName = "mock-workflow",
      scheduleRecordWorkflowClassName = Nothing,
      scheduleRecordExpression = "* * * * *",
      scheduleRecordStatus = Active,
      scheduleRecordContext = "{}",
      scheduleRecordLastFiredAt = Nothing,
      scheduleRecordAutomaticBackfill = False,
      scheduleRecordCronTimezone = Nothing,
      scheduleRecordQueueName = Nothing,
      scheduleRecordApplicationName = Just "mock-app"
    }

-- | The schedule the stateless backend answers with.
mockScheduleName :: Text
mockScheduleName = "mock-schedule"

mockVersion :: VersionInfo
mockVersion =
  VersionInfo
    { versionInfoApplicationName = Just "mock-app",
      versionInfoId = "mock-version-id",
      versionInfoName = "0.0.0",
      versionInfoCreatedAt = mockTimestamp,
      versionInfoTimestamp = mockTimestamp
    }
