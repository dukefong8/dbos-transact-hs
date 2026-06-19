{-# LANGUAGE OverloadedStrings #-}

module DbosTransact.Core.Transition where

import Data.Aeson (Value(String))
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import DbosTransact.Core.Model
import DbosTransact.Error
import DbosTransact.Workflow

transition :: CoreState -> Command -> Either DBOSError (CoreState, [Event])
transition state command =
  case command of
    StartWorkflow status ->
      let wid = WorkflowId (statusWorkflowId status)
       in case Map.lookup wid (csWorkflows state) of
            Nothing -> Right (state { csWorkflows = Map.insert wid status (csWorkflows state) }, [WorkflowStarted wid])
            Just _existing -> Right (state, [])
    CompleteWorkflow wid output ->
      updateWorkflow wid [WorkflowCompleted wid] $ \status ->
        if workflowStatusIsTerminal (statusType status)
          then Nothing
          else Just status { statusType = WorkflowSuccess, statusOutput = Just (String output) }
    FailWorkflow wid err ->
      updateWorkflow wid [WorkflowFailed wid] $ \status ->
        if workflowStatusIsTerminal (statusType status)
          then Nothing
          else Just status { statusType = WorkflowError, statusError = Just err }
    CancelWorkflow wid ->
      updateWorkflow wid [WorkflowCancelledEvent wid] $ \status ->
        if workflowStatusIsTerminal (statusType status)
          then Nothing
          else Just status { statusType = WorkflowCancelled }
    BeginStep wid sid expectedName ->
      checkStepName state wid sid expectedName
    RecordStep wid sid record ->
      recordStep state wid sid record
    CheckStep wid sid expectedName ->
      checkStepName state wid sid expectedName
  where
    updateWorkflow wid events f =
      case Map.lookup wid (csWorkflows state) of
        Nothing -> Left (WorkflowNotFound (workflowIdText wid))
        Just status ->
          case f status of
            Nothing -> Right (state, [])
            Just updated -> Right (state { csWorkflows = Map.insert wid updated (csWorkflows state) }, events)

recordStep :: CoreState -> WorkflowId -> StepId -> StepRecord -> Either DBOSError (CoreState, [Event])
recordStep state wid sid record =
  case Map.lookup key (csSteps state) of
    Nothing -> Right (state { csSteps = Map.insert key record (csSteps state) }, [StepRecorded wid sid])
    Just existing
      | existing == record -> Right (state, [StepReplayed wid sid])
      | srName existing /= srName record -> Left (stepNameMismatch wid sid (srName existing) (srName record))
      | otherwise -> Left (UnexpectedStepError ("step " <> stepIdText sid <> " for workflow " <> workflowIdText wid <> " already has a different recorded payload"))
  where
    key = (wid, sid)

checkStepName :: CoreState -> WorkflowId -> StepId -> Text -> Either DBOSError (CoreState, [Event])
checkStepName state wid sid expectedName =
  case Map.lookup (wid, sid) (csSteps state) of
    Nothing -> Right (state, [])
    Just record
      | srName record == expectedName -> Right (state, [StepReplayed wid sid])
      | otherwise -> Left (stepNameMismatch wid sid (srName record) expectedName)

stepNameMismatch :: WorkflowId -> StepId -> Text -> Text -> DBOSError
stepNameMismatch wid sid recorded replayed =
  UnexpectedStepError
    ( "step "
        <> stepIdText sid
        <> " for workflow "
        <> workflowIdText wid
        <> " was recorded as "
        <> recorded
        <> ", replayed as "
        <> replayed
    )

workflowStatusIsTerminal :: WorkflowStatusType -> Bool
workflowStatusIsTerminal statusType =
  statusType `elem`
    [ WorkflowSuccess
    , WorkflowError
    , WorkflowCancelled
    , WorkflowMaxRecoveryAttemptsExceeded
    ]

workflowIdText :: WorkflowId -> Text
workflowIdText (WorkflowId wid) = wid

stepIdText :: StepId -> Text
stepIdText (StepId sid) = Text.pack (show sid)
