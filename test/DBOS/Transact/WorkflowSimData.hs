{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Workflow-lifecycle mock data for sim trees: the rows, steps, and init
-- answers the sim backend serves, owned here (not in the backend) so each
-- @TestSim@ tree can correlate its inputs and assertions through the same
-- constructors the backend reads. Duplicated across domains on purpose —
-- shared mock data would couple the trees through the backend.
module DBOS.Transact.WorkflowSimData
  ( mockTimestamp,
    mockInitResult,
    mockWorkflow,
    mockStep,
    mockChildId,
    mockHolderId,
  )
where

import DBOS.Prelude
import DBOS.SystemDB
  ( NewWorkflow (..),
    StepRecord (..),
    Timestamp,
    WorkflowId (..),
    WorkflowInitResult (..),
    WorkflowRecord (..),
    WorkflowStatus (..),
    timestampFromEpochMs,
  )

mockTimestamp :: Timestamp
mockTimestamp = timestampFromEpochMs 1000

mockInitResult :: NewWorkflow -> WorkflowInitResult
mockInitResult new =
  WorkflowInitResult
    { initResultStatus = Pending,
      initResultRecoveryAttempts = 0,
      initResultDeadline = new.newWorkflowDeadline,
      initResultSerialization = new.newWorkflowSerialization,
      initResultShouldExecute = True
    }

mockWorkflow :: WorkflowId -> WorkflowRecord
mockWorkflow wid =
  WorkflowRecord
    { workflowRecordId = wid,
      workflowRecordStatus = Pending,
      workflowRecordName = Just "mock-workflow",
      workflowRecordClassName = Nothing,
      workflowRecordConfigName = Nothing,
      workflowRecordInput = Just "null",
      workflowRecordOutput = Nothing,
      workflowRecordError = Nothing,
      workflowRecordSerialization = Just "rust_serde",
      workflowRecordExecutorId = Just "mock-executor",
      workflowRecordApplicationVersion = Just "0.0.0",
      workflowRecordRecoveryAttempts = 0,
      workflowRecordQueueName = Just "mock-queue",
      workflowRecordCreatedAt = mockTimestamp,
      workflowRecordUpdatedAt = mockTimestamp,
      workflowRecordStartedAt = Nothing,
      workflowRecordCompletedAt = Nothing,
      workflowRecordForkedFrom = Nothing,
      workflowRecordParentWorkflowId = Nothing,
      workflowRecordWasForkedFrom = False,
      workflowRecordOwnerXid = Nothing,
      workflowRecordApplicationId = Nothing,
      workflowRecordAuthenticatedUser = Nothing,
      workflowRecordAuthenticatedRoles = [],
      workflowRecordAssumedRole = Nothing,
      workflowRecordRequest = Nothing,
      workflowRecordApplicationName = Just "mock-app",
      workflowRecordDeduplicationId = Nothing,
      workflowRecordPriority = 0,
      workflowRecordQueuePartitionKey = Nothing,
      workflowRecordRateLimited = False,
      workflowRecordScheduleName = Nothing,
      workflowRecordTimeout = Nothing,
      workflowRecordDeadline = Nothing,
      workflowRecordDelayUntil = Nothing,
      workflowRecordDebounceDeadline = Nothing,
      workflowRecordIsDebounced = False,
      workflowRecordAttributes = Nothing
    }

mockStep :: WorkflowId -> Int -> Text -> StepRecord
mockStep wid stepId' name =
  StepRecord
    { stepRecordWorkflowId = wid,
      stepRecordStepId = stepId',
      stepRecordStepName = name,
      stepRecordOutput = Just "\"mock\"",
      stepRecordError = Nothing,
      stepRecordChildWorkflowId = Nothing,
      stepRecordSerialization = Just "rust_serde",
      stepRecordStartedAt = Just mockTimestamp,
      stepRecordCompletedAt = Just mockTimestamp
    }

-- | The child the stateless backend answers with.
mockChildId :: WorkflowId
mockChildId = WorkflowId "mock-child"

-- | The dedup holder the stateless backend answers with.
mockHolderId :: WorkflowId
mockHolderId = WorkflowId "mock-holder"
