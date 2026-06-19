{-# LANGUAGE OverloadedStrings #-}

module DbosTransact.Core.TransitionSpec
  ( tests
  ) where

import Data.Map.Strict qualified as Map
import Data.Time (UTCTime(..), fromGregorian, secondsToDiffTime)
import DbosTransact.Core.Model
import DbosTransact.Core.Transition (transition)
import DbosTransact.Error (DBOSError(..))
import DbosTransact.Workflow
import Hedgehog (Property, evalEither, property, (===))
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.Hedgehog (testProperty)

tests :: TestTree
tests = testGroup "Core.TransitionSpec"
  [ testProperty "terminal states stay terminal" prop_terminalStatesAreTerminal
  , testProperty "step replay name must match" prop_stepReplayNameMustMatch
  , testProperty "same workflow id does not start two bodies" prop_sameWorkflowIdDoesNotStartTwoBodies
  , testProperty "record step is idempotent for same payload" prop_recordStepIsIdempotentForSamePayload
  , testProperty "non-existent workflow operations fail" prop_nonExistentWorkflowFails
  , testProperty "completing terminal success is a no-op" prop_terminalSuccessNoOp
  ]

prop_terminalStatesAreTerminal :: Property
prop_terminalStatesAreTerminal = property $ do
  let wid = WorkflowId "wf-terminal"
      status = testStatus wid WorkflowError
      initial = emptyCoreState { csWorkflows = Map.singleton wid status }
  (afterComplete, completeEvents) <- evalEither $ transition initial (CompleteWorkflow wid "ignored")
  (afterCancel, cancelEvents) <- evalEither $ transition initial (CancelWorkflow wid)
  Map.lookup wid (csWorkflows afterComplete) === Just status
  Map.lookup wid (csWorkflows afterCancel) === Just status
  completeEvents === []
  cancelEvents === []

prop_stepReplayNameMustMatch :: Property
prop_stepReplayNameMustMatch = property $ do
  let wid = WorkflowId "wf-step-name"
      sid = StepId 1
      record = StepRecord
        { srName = "first"
        , srOutput = Just "payload"
        , srError = Nothing
        , srSerialization = "json"
        }
  (recorded, _) <- evalEither $ transition emptyCoreState (RecordStep wid sid record)
  transition recorded (CheckStep wid sid "second") === Left (UnexpectedStepError "step 1 for workflow wf-step-name was recorded as first, replayed as second")

prop_sameWorkflowIdDoesNotStartTwoBodies :: Property
prop_sameWorkflowIdDoesNotStartTwoBodies = property $ do
  let wid = WorkflowId "wf-once"
      first = testStatus wid WorkflowPending
      second = (testStatus wid WorkflowPending) { statusName = "second-body" }
  (started, firstEvents) <- evalEither $ transition emptyCoreState (StartWorkflow first)
  (again, secondEvents) <- evalEither $ transition started (StartWorkflow second)
  firstEvents === [WorkflowStarted wid]
  secondEvents === []
  Map.lookup wid (csWorkflows again) === Just first

prop_recordStepIsIdempotentForSamePayload :: Property
prop_recordStepIsIdempotentForSamePayload = property $ do
  let wid = WorkflowId "wf-step-idempotent"
      sid = StepId 2
      record = StepRecord
        { srName = "same"
        , srOutput = Just "payload"
        , srError = Nothing
        , srSerialization = "json"
        }
  (recorded, firstEvents) <- evalEither $ transition emptyCoreState (RecordStep wid sid record)
  (again, secondEvents) <- evalEither $ transition recorded (RecordStep wid sid record)
  csSteps recorded === csSteps again
  firstEvents === [StepRecorded wid sid]
  secondEvents === [StepReplayed wid sid]

testStatus :: WorkflowId -> WorkflowStatusType -> WorkflowStatus
testStatus (WorkflowId wid) statusType = WorkflowStatus
  { statusWorkflowId = wid
  , statusType = statusType
  , statusName = "test-workflow"
  , statusInput = Nothing
  , statusOutput = Nothing
  , statusError = Nothing
  , statusExecutorId = Nothing
  , statusApplicationVersion = Nothing
  , statusApplicationId = Nothing
  , statusCreatedAt = fixedTime
  , statusUpdatedAt = fixedTime
  , statusCompletedAt = Nothing
  , statusRecoveryAttempts = 0
  , statusQueueName = Nothing
  , statusWorkflowTimeout = Nothing
  , statusWorkflowDeadline = Nothing
  , statusDeduplicationId = Nothing
  , statusPriority = 0
  , statusQueuePartitionKey = Nothing
  , statusParentWorkflowId = Nothing
  , statusClassName = Nothing
  , statusConfigName = Nothing
  , statusSerialization = "json"
  , statusDelayUntil = Nothing
  }

fixedTime :: UTCTime
fixedTime = UTCTime (fromGregorian 2026 1 1) (secondsToDiffTime 0)

prop_nonExistentWorkflowFails :: Property
prop_nonExistentWorkflowFails = property $ do
  let wid = WorkflowId "wf-ghost"
  transition emptyCoreState (CompleteWorkflow wid "ignored") === Left (WorkflowNotFound "wf-ghost")
  transition emptyCoreState (FailWorkflow wid "ignored") === Left (WorkflowNotFound "wf-ghost")
  transition emptyCoreState (CancelWorkflow wid) === Left (WorkflowNotFound "wf-ghost")

prop_terminalSuccessNoOp :: Property
prop_terminalSuccessNoOp = property $ do
  let wid = WorkflowId "wf-terminal-success"
      status = testStatus wid WorkflowSuccess
      initial = emptyCoreState { csWorkflows = Map.singleton wid status }
  (afterComplete, completeEvents) <- evalEither $ transition initial (CompleteWorkflow wid "ignored")
  (afterFail, failEvents) <- evalEither $ transition initial (FailWorkflow wid "ignored")
  Map.lookup wid (csWorkflows afterComplete) === Just status
  Map.lookup wid (csWorkflows afterFail) === Just status
  completeEvents === []
  failEvents === []
