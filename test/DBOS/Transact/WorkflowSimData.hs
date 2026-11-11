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
      initResultCreatorXid = "mock-creator",
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
      workflowRecordInput = Just "{\"positionalArgs\":[],\"namedArgs\":{}}",
      workflowRecordOutput = Nothing,
      workflowRecordError = Nothing,
      workflowRecordSerialization = Just "portable_json",
      workflowRecordExecutorId = Just "mock-executor",
      workflowRecordAppVersion = Just "0.0.0",
      workflowRecordRecoveryAttempts = 0,
      workflowRecordQueueName = Just "mock-queue",
      workflowRecordCreatedAt = mockTimestamp,
      workflowRecordUpdatedAt = mockTimestamp,
      workflowRecordStartedAt = Nothing,
      workflowRecordCompletedAt = Nothing,
      workflowRecordForkedFrom = Nothing,
      workflowRecordParentWorkflowId = Nothing,
      workflowRecordWasForkedFrom = False,
      -- The datasource fixture runs as 'testOwner', so its canned row must
      -- carry that execution's token for the ownership check to pass.
      workflowRecordOwnerXid = Just "test-owner",
      workflowRecordCreatorXid = Just "test-owner",
      workflowRecordAppId = Nothing,
      workflowRecordAuthenticatedUser = Nothing,
      workflowRecordAuthenticatedRoles = [],
      workflowRecordAssumedRole = Nothing,
      workflowRecordRequest = Nothing,
      workflowRecordAppName = Just "mock-app",
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
      stepRecordStepId = fromIntegral stepId',
      stepRecordStepName = name,
      stepRecordOutput = Just "\"mock\"",
      stepRecordError = Nothing,
      stepRecordChildWorkflowId = Nothing,
      stepRecordSerialization = Just "portable_json",
      stepRecordStartedAt = Just mockTimestamp,
      stepRecordCompletedAt = Just mockTimestamp
    }

-- | The child the stateless backend answers with.
mockChildId :: WorkflowId
mockChildId = WorkflowId "mock-child"

-- | The dedup holder the stateless backend answers with.
mockHolderId :: WorkflowId
mockHolderId = WorkflowId "mock-holder"
