{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | In-workflow waits against the live backend, ported from Rust
-- @tests/waits.rs@: an empty first wait records its refusal and a replay
-- reads it back, and a recorded winner that left the set is refused.
module DBOS.Transact.WaitTest (tests) where

import DBOS.Prelude
import Colog.Core.Action (LogAction (..))
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (NewWorkflow (..), Outcome (..), StepRecord (..), Submission (..), WorkflowId (..), newWorkflow, selectWorkflowStepName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact (Error (..), joinWorkflows, selectWorkflow)
import DBOS.Transact.ContextTest (ctxOver)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "In-workflow waits"
    [ testCase "an empty first wait records its refusal and a replay reads it back" $ withWorkflow "wait-refusal" $ \backend workflowText -> do
        context <- ctxOver backend workflowText
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
        replayContext <- ctxOver backend workflowText
        replayed <- selectWorkflow replayContext [WorkflowId workflowText]
        case replayed of
          Left (InvalidArgument operation _) -> operation @?= "select_workflow"
          other -> fail (show other),
      testCase "a recorded winner that left the set is refused" $ withWorkflow "wait-winner" $ \backend workflowText -> do
        settled <- SystemDB.recordWorkflowOutcome backend (WorkflowId workflowText) (OutcomeOutput (Just "null"))
        case settled of
          Left err -> fail (show err)
          Right _ -> pure ()
        context <- ctxOver backend workflowText
        first <- selectWorkflow context [WorkflowId workflowText]
        first @?= Right (WorkflowId workflowText)
        replayContext <- ctxOver backend workflowText
        replayed <- selectWorkflow replayContext [WorkflowId "hs-wait-elsewhere"]
        case replayed of
          Left (ErrorSystemDatabase err) -> case err of
            SystemDB.UnexpectedStep {workflowId, stepId, recorded} -> do
              workflowId @?= workflowText
              stepId @?= 0
              assertBool "the recorded winner is named" (workflowText `Text.isInfixOf` recorded)
            other -> fail (show other)
          other -> fail (show other),
      testCase "an all-wait completes over a settled workflow" $ withWorkflow "wait-join" $ \backend workflowText -> do
        settled <- SystemDB.recordWorkflowOutcome backend (WorkflowId workflowText) (OutcomeOutput (Just "null"))
        case settled of
          Left err -> fail (show err)
          Right _ -> pure ()
        context <- ctxOver backend workflowText
        outcome <- joinWorkflows context [WorkflowId workflowText]
        outcome @?= Right ()
    ]

withWorkflow :: Text.Text -> (Postgres.PostgresSystemDB -> Text.Text -> IO a) -> IO a
withWorkflow label action = do
  config <- Postgres.configFromEnv
  let logger = LogAction (const (pure ()))
  bracket (Postgres.acquirePostgresSystemDB config logger) Postgres.releasePostgresSystemDB $ \backend -> do
    Postgres.activatePostgresSystemDB backend
    freshId <- UUID.V4.nextRandom
    let workflowText = "hs-l2-" <> label <> "-" <> Text.pack (UUID.toString freshId)
        initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2WaitTest"}
    created <- SystemDB.initWorkflow backend initialWorkflow Nothing Fresh Nothing
    case created of
      Left err -> fail (show err)
      Right _ -> action backend workflowText
