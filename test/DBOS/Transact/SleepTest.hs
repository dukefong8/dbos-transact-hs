{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Durable sleep, mirroring Rust @tests/sleep.rs@: the wait is checkpointed,
-- a replay adopts the recorded wake time, and a sleep outside a workflow
-- waits plainly.
module DBOS.Transact.SleepTest (tests) where

import DBOS.Prelude
import Colog.Core.Action (LogAction (..))
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (NewWorkflow (..), StepRecord (..), Submission (..), WorkflowId (..), millisDuration, newWorkflow, sleepStepName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact (sleepPlain, sleepWorkflowStep)
import DBOS.Transact.ContextTest (ctxOver)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Durable sleep"
    [ testCase "a sleep waits and is checkpointed" $ withWorkflow "sleep-checkpoint" $ \backend workflowText -> do
        context <- ctxOver backend workflowText
        outcome <- sleepWorkflowStep context (millisDuration 25)
        outcome @?= Right ()
        checkpoint <- SystemDB.checkStep backend (WorkflowId workflowText) 0 sleepStepName
        case checkpoint of
          Right (Just record) -> do
            record.stepRecordStepName @?= sleepStepName
            assertBool "the sleep recorded a wake time" (record.stepRecordOutput /= Nothing)
          other -> fail ("expected a recorded sleep checkpoint, got: " <> show other),
      testCase "a replayed sleep does not start its clock again" $ withWorkflow "sleep-replay" $ \backend workflowText -> do
        firstContext <- ctxOver backend workflowText
        _ <- sleepWorkflowStep firstContext (millisDuration 25)
        before <- SystemDB.checkStep backend (WorkflowId workflowText) 0 sleepStepName
        replayContext <- ctxOver backend workflowText
        -- A much longer request still returns at the recorded wake time.
        replayed <- sleepWorkflowStep replayContext (millisDuration 60000)
        replayed @?= Right ()
        after <- SystemDB.checkStep backend (WorkflowId workflowText) 0 sleepStepName
        case (before, after) of
          (Right (Just first), Right (Just second)) -> do
            second.stepRecordCompletedAt @?= first.stepRecordCompletedAt
            second.stepRecordOutput @?= first.stepRecordOutput
          other -> fail ("expected the same recorded sleep, got: " <> show other),
      testCase "a sleep outside a workflow waits plainly" $ do
        outcome <- sleepPlain (millisDuration 1)
        outcome @?= ()
    ]

withWorkflow :: Text.Text -> (Postgres.PostgresSystemDB -> Text.Text -> IO a) -> IO a
withWorkflow label action = do
  config <- Postgres.configFromEnv
  let logger = LogAction (const (pure ()))
  bracket (Postgres.acquirePostgresSystemDB config logger) Postgres.releasePostgresSystemDB $ \backend -> do
    Postgres.activatePostgresSystemDB backend
    freshId <- UUID.V4.nextRandom
    let workflowText = "hs-l2-" <> label <> "-" <> Text.pack (UUID.toString freshId)
        initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2SleepTest"}
    created <- SystemDB.initWorkflow backend initialWorkflow Nothing Fresh Nothing
    case created of
      Left err -> fail (show err)
      Right _ -> action backend workflowText
