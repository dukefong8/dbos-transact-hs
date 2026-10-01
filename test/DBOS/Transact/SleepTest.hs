{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Durable sleep, mirroring Rust @tests/sleep.rs@: the wait is checkpointed,
-- a replay adopts the recorded wake time, and a sleep outside a workflow
-- waits plainly.
module DBOS.Transact.SleepTest (tests) where

import DBOS.Prelude
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (NewWorkflow (..), StepRecord (..), Submission (..), WorkflowId (..), millisDuration, newWorkflow, sleepStepName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact (acquireFastBackend, ioTracer, nullTracer, sleepPlain, sleepWorkflowStep)
import DBOS.Transact.ContextTest (ctxOver)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    testGroup
      "Durable sleep"
      [ testCase "a sleep waits and is checkpointed" $ do
          backend <- getBackend
          withWorkflow backend "sleep-checkpoint" $ \workflowText -> do
            context <- ctxOver backend nullTracer workflowText
            outcome <- sleepWorkflowStep context (millisDuration 25)
            outcome @?= Right ()
            checkpoint <- SystemDB.checkStep backend (WorkflowId workflowText) 0 sleepStepName
            case checkpoint of
              Right (Just record) -> do
                record.stepRecordStepName @?= sleepStepName
                assertBool "the sleep recorded a wake time" (record.stepRecordOutput /= Nothing)
              other -> fail ("expected a recorded sleep checkpoint, got: " <> show other),
        testCase "a replayed sleep does not start its clock again" $ do
          backend <- getBackend
          withWorkflow backend "sleep-replay" $ \workflowText -> do
            firstContext <- ctxOver backend nullTracer workflowText
            _ <- sleepWorkflowStep firstContext (millisDuration 25)
            before <- SystemDB.checkStep backend (WorkflowId workflowText) 0 sleepStepName
            -- The replay announces through FastLogger, so the run proves
            -- the trace seam as well as the wake it waits until.
            (logger, cleanup) <- acquireFastBackend
            replayContext <- ctxOver backend (ioTracer logger) workflowText
            -- A much longer request still returns at the recorded wake time.
            replayed <- sleepWorkflowStep replayContext (millisDuration 60000)
            cleanup
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

withWorkflow :: Postgres.PostgresSystemDB -> Text.Text -> (Text.Text -> IO a) -> IO a
withWorkflow backend label action = do
  freshId <- UUID.V4.nextRandom
  let workflowText = "hs-l2-" <> label <> "-" <> Text.pack (UUID.toString freshId)
      initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2SleepTest"}
  created <- SystemDB.initWorkflow backend initialWorkflow Nothing Fresh Nothing
  case created of
    Left err -> fail (show err)
    Right _ -> action workflowText
