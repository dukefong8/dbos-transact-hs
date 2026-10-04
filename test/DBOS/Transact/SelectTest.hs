{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Public-behavior tests for the @select.rs@ checkpoint seam: what a
-- fresh select claims, what a recorded winner replays, and what a stale
-- winner earns. The race itself is exercised end-to-end in
-- 'DBOS.Transact.WorkflowTest'; here the check/record pair is driven
-- directly, with the branch set built by pushing pending identities.
module DBOS.Transact.SelectTest (tests) where

import DBOS.Prelude
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (NewWorkflow (..), StepRecord (..), WorkflowId (..), newWorkflow, selectStepStepName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  (
    EngineOnly, Branches,
    Error (..),
    PendingStep (..),
    Racing (..),
    nullTracer,
  )
import DBOS.Transact.Select
  ( checkSelect,
    controlError,
    newBranches,
    pushBranch,
    recordSelect,
  )
import DBOS.Transact.ContextTest (ctxOver)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

-- | One backend for the whole group: contexts build real connections over
-- it, and the check/record pairs write step rows against it.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | Two named branches that claim no ids: enough for the stale-winner
-- check, which reads only the identities.
-- | A fresh workflow row for a select to checkpoint against: the check
-- reads the row, so the row has to exist.
freshWorkflowRow :: Postgres.PostgresSystemDB -> Text -> IO Text
freshWorkflowRow backend label = do
  freshId <- UUID.V4.nextRandom
  let workflowText = "hs-l2-" <> label <> "-" <> Text.pack (UUID.toString freshId)
      initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "SelectTest"}
  created <- SystemDB.initWorkflow backend initialWorkflow Nothing SystemDB.Fresh Nothing
  case created of
    Left err -> fail (show err)
    Right _ -> pure workflowText

branches2 :: Branches
branches2 =
  pushBranch
    (PendingStep "charge" Nothing (pure (Right ()) :: IO (Either (Error EngineOnly) ())))
    (pushBranch (PendingStep "timeout" Nothing (pure (Right ()) :: IO (Either (Error EngineOnly) ()))) newBranches)

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    testGroup
      "Durable select"
      [ testCase "a fresh select claims its id and records a winner" $ do
          backend <- getBackend
          workflowText <- freshWorkflowRow backend "select-fresh"
          ctx <- ctxOver backend nullTracer workflowText
          checked <- checkSelect ctx branches2
          recording <- case checked of
            Right (Fresh recording') -> pure recording'
            Right (Replay _ _) -> fail "expected a fresh select"
            Left err -> fail ("expected a fresh select, got: " <> show err)
          recorded <- recordSelect recording 1
          case recorded of
            Left err -> fail (show err)
            Right () -> pure ()
          listed <- SystemDB.listWorkflowSteps backend (WorkflowId workflowText) True Nothing Nothing Nothing
          case listed of
            Right [StepRecord {stepRecordStepId = stepId, stepRecordStepName = name, stepRecordOutput = Just output}] -> do
              stepId @?= 0
              name @?= selectStepStepName
              output @?= "1"
            other -> fail ("expected the select's own row, got: " <> show other)
          -- A second pass over the same row, with a fresh counter: the
          -- recorded winner replays.
          replayCtx <- ctxOver backend nullTracer workflowText
          replay <- checkSelect replayCtx branches2
          case replay of
            Right (Replay winner _) -> winner @?= 1
            Right (Fresh _) -> fail "expected the recorded winner to replay"
            Left err -> fail ("expected a replay, got: " <> show err),
        testCase "a winner outside the branches that exist now is refused" $ do
          backend <- getBackend
          workflowText <- freshWorkflowRow backend "select-stale"
          ctx <- ctxOver backend nullTracer workflowText
          checked <- checkSelect ctx branches2
          recording <- case checked of
            Right (Fresh recording') -> pure recording'
            Right (Replay _ _) -> fail "expected a fresh select"
            Left err -> fail ("expected a fresh select, got: " <> show err)
          recorded <- recordSelect recording 5
          case recorded of
            Left err -> fail (show err)
            Right () -> pure ()
          replayCtx <- ctxOver backend nullTracer workflowText
          replay <- checkSelect replayCtx branches2
          case replay of
            Left (ErrorSystemDatabase (SystemDB.UnexpectedStep {stepId, expected, recorded = recordedText})) -> do
              stepId @?= 0
              assertBool ("names this run's shape in " <> Text.unpack expected) ("a select over 2 branches" `Text.isInfixOf` expected)
              assertBool ("and the stale winner in " <> Text.unpack recordedText) ("branch 5" `Text.isInfixOf` recordedText)
            other -> fail ("expected the stale-winner refusal, got: " <> show other),
        testCase "a control signal is not a race decision" $ do
          controlError (Left (Interrupted {workflowId = "wf"})) @?= Just (Interrupted {workflowId = "wf"})
          case controlError (Left (ErrorSystemDatabase (SystemDB.WorkflowCancelled {workflowId = "wf"}))) of
            Just _ -> pure ()
            Nothing -> fail "expected a cancellation to be control"
          controlError (Left (StepFailed "s" "boom")) @?= Nothing
          controlError (Left (AwaitedWorkflowCancelled {workflowId = "child"})) @?= Nothing
          controlError (Right (7 :: Int)) @?= Nothing
      ]
