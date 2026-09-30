{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | In-workflow waits against the live backend, ported from Rust
-- @tests/waits.rs@: an empty first wait records its refusal and a replay
-- reads it back, and a recorded winner that left the set is refused.
module DBOS.Transact.WaitTest (tests) where

import DBOS.Prelude
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (NewWorkflow (..), Outcome (..), StepRecord (..), Submission (..), WorkflowId (..), newWorkflow, selectWorkflowStepName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact (Error (..), joinWorkflows, nullTracer, selectWorkflow)
import DBOS.Transact.ContextTest (ctxOver)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
  testGroup
    "In-workflow waits"
    [ testCase "an empty first wait records its refusal and a replay reads it back" $ withWorkflow getBackend "wait-refusal" $ \backend workflowText -> do
        context <- ctxOver backend nullTracer workflowText
        first <- selectWorkflow context []
        case first of
          Left (InvalidArgument operation detail) -> do
            operation @?= "select_workflow"
            detail @?= "no workflow ids to wait for"
          other -> fail (show other)
        recorded <- SystemDB.checkStep backend (WorkflowId workflowText) 0 selectWorkflowStepName
        case recorded of
          Right (Just record) -> assertBool "the refusal is recorded as a step error" (record.stepRecordError /= Nothing)
          other -> fail (show other)
        -- The replay looks at a set that now has an answer, and still reads
        -- the refusal back rather than deciding again. A fresh context is the
        -- replay's start: step ids are taken at the call in this port.
        replayContext <- ctxOver backend nullTracer workflowText
        replayed <- selectWorkflow replayContext [WorkflowId workflowText]
        case replayed of
          Left (InvalidArgument operation _) -> operation @?= "select_workflow"
          other -> fail (show other),
      testCase "a recorded winner that left the set is refused" $ withWorkflow getBackend "wait-winner" $ \backend workflowText -> do
        settled <- SystemDB.recordWorkflowOutcome backend (WorkflowId workflowText) (OutcomeOutput (Just "null"))
        case settled of
          Left err -> fail (show err)
          Right _ -> pure ()
        context <- ctxOver backend nullTracer workflowText
        first <- selectWorkflow context [WorkflowId workflowText]
        first @?= Right (WorkflowId workflowText)
        replayContext <- ctxOver backend nullTracer workflowText
        replayed <- selectWorkflow replayContext [WorkflowId "hs-wait-elsewhere"]
        case replayed of
          Left (ErrorSystemDatabase err) -> case err of
            SystemDB.UnexpectedStep {workflowId, stepId, recorded} -> do
              workflowId @?= workflowText
              stepId @?= 0
              assertBool "the recorded winner is named" (workflowText `Text.isInfixOf` recorded)
            other -> fail (show other)
          other -> fail (show other),
      testCase "an all-wait completes over a settled workflow" $ withWorkflow getBackend "wait-join" $ \backend workflowText -> do
        settled <- SystemDB.recordWorkflowOutcome backend (WorkflowId workflowText) (OutcomeOutput (Just "null"))
        case settled of
          Left err -> fail (show err)
          Right _ -> pure ()
        context <- ctxOver backend nullTracer workflowText
        outcome <- joinWorkflows context [WorkflowId workflowText]
        outcome @?= Right (),
      testCase "select reports the first workflow to settle" $ do
        backend <- getBackend
        freshId <- UUID.V4.nextRandom
        let firstText = "hs-l2-wait-first-a-" <> Text.pack (UUID.toString freshId)
            secondText = "hs-l2-wait-first-b-" <> Text.pack (UUID.toString freshId)
        let start text = do
              created <- SystemDB.initWorkflow backend ((newWorkflow text) {newWorkflowName = Just "L2WaitFirst"}) Nothing Fresh Nothing
              case created of
                Left err -> fail (show err)
                Right _ -> pure ()
        start firstText
        start secondText
        settled <- SystemDB.recordWorkflowOutcome backend (WorkflowId secondText) (OutcomeOutput (Just "null"))
        case settled of
          Left err -> fail (show err)
          Right _ -> pure ()
        context <- ctxOver backend nullTracer firstText
        first <- selectWorkflow context [WorkflowId firstText, WorkflowId secondText]
        first @?= Right (WorkflowId secondText)
        recorded <- SystemDB.checkStep backend (WorkflowId firstText) 0 selectWorkflowStepName
        case recorded of
          Right (Just record) -> case record.stepRecordOutput of
            Just _ -> pure ()
            Nothing -> fail "expected the winning select to checkpoint its output"
          other -> fail (show other),
      testCase "a cancelled workflow counts as settled" $ withWorkflow getBackend "wait-cancelled" $ \backend workflowText -> do
        freshId <- UUID.V4.nextRandom
        let otherText = "hs-l2-wait-cancelled-other-" <> Text.pack (UUID.toString freshId)
        created <- SystemDB.initWorkflow backend ((newWorkflow otherText) {newWorkflowName = Just "L2WaitCancelled"}) Nothing Fresh Nothing
        case created of
          Left err -> fail (show err)
          Right _ -> pure ()
        cancelled <- SystemDB.cancelWorkflows backend [WorkflowId otherText] False Nothing
        case cancelled of
          Left err -> fail (show err)
          Right _ -> pure ()
        context <- ctxOver backend nullTracer workflowText
        first <- selectWorkflow context [WorkflowId workflowText, WorkflowId otherText]
        first @?= Right (WorkflowId otherText),
      testCase "join returns when the last workflow settles" $ withWorkflow getBackend "wait-join-last" $ \backend workflowText -> do
        freshId <- UUID.V4.nextRandom
        let otherText = "hs-l2-wait-join-other-" <> Text.pack (UUID.toString freshId)
        created <- SystemDB.initWorkflow backend ((newWorkflow otherText) {newWorkflowName = Just "L2WaitJoinOther"}) Nothing Fresh Nothing
        case created of
          Left err -> fail (show err)
          Right _ -> pure ()
        let settle text = do
              settled <- SystemDB.recordWorkflowOutcome backend (WorkflowId text) (OutcomeOutput (Just "null"))
              case settled of
                Left err -> fail (show err)
                Right _ -> pure ()
        settle workflowText
        settle otherText
        context <- ctxOver backend nullTracer workflowText
        outcome <- joinWorkflows context [WorkflowId workflowText, WorkflowId otherText]
        outcome @?= Right (),
      testCase "an empty all-wait is satisfied and takes no step" $ withWorkflow getBackend "wait-empty-all" $ \backend workflowText -> do
        context <- ctxOver backend nullTracer workflowText
        outcome <- joinWorkflows context []
        outcome @?= Right ()
        free <- SystemDB.checkStep backend (WorkflowId workflowText) 0 selectWorkflowStepName
        case free of
          Right Nothing -> pure ()
          other -> fail (show other),
      testCase "a repeated id is accepted by both waits" $ withWorkflow getBackend "wait-repeated" $ \backend workflowText -> do
        settled <- SystemDB.recordWorkflowOutcome backend (WorkflowId workflowText) (OutcomeOutput (Just "null"))
        case settled of
          Left err -> fail (show err)
          Right _ -> pure ()
        context <- ctxOver backend nullTracer workflowText
        first <- selectWorkflow context [WorkflowId workflowText, WorkflowId workflowText]
        first @?= Right (WorkflowId workflowText)
        outcome <- joinWorkflows context [WorkflowId workflowText, WorkflowId workflowText]
        outcome @?= Right ()
    ]

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

withWorkflow :: IO Postgres.PostgresSystemDB -> Text.Text -> (Postgres.PostgresSystemDB -> Text.Text -> IO a) -> IO a
withWorkflow getBackend label action = do
  backend <- getBackend
  freshId <- UUID.V4.nextRandom
  let workflowText = "hs-l2-" <> label <> "-" <> Text.pack (UUID.toString freshId)
      initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2WaitTest"}
  created <- SystemDB.initWorkflow backend initialWorkflow Nothing Fresh Nothing
  case created of
    Left err -> fail (show err)
    Right _ -> action backend workflowText
