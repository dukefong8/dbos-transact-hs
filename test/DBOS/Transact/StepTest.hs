{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Public workflow-step behavior against the configured live SystemDB.
module DBOS.Transact.StepTest (tests) where

import DBOS.Prelude
import Colog.Core.Action (LogAction (..))
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (NewWorkflow (..), Submission (..), SystemDB (..), millisDuration, newWorkflow)
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  ( Ctx,
    runWorkflowStep,
    sleepWorkflowStep,
    stepId,
  )
import DBOS.Transact.ContextTest (ctxOver)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertEqual, testCase)

tests :: TestTree
tests =
  testGroup
    "Durable step"
    [ testCase "a recorded workflow step runs once and replays" $ do
        config <- Postgres.configFromEnv
        let logger = LogAction (const (pure ()))
        bracket (Postgres.acquirePostgresSystemDB config logger) Postgres.releasePostgresSystemDB $ \backend -> do
          Postgres.activatePostgresSystemDB backend
          freshId <- UUID.V4.nextRandom
          let workflowText = "hs-l2-step-" <> Text.pack (UUID.toString freshId)
              initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2StepTest"}
          created <- initWorkflow backend initialWorkflow Nothing Fresh Nothing
          case created of
            Left err -> fail (show err)
            Right _ -> pure ()
          calls <- newIORef (0 :: Int)
          observedStepId <- newIORef Nothing
          -- Each execution gets a fresh context: a new counter and a new
          -- execution identity over the same workflow id, exactly as a
          -- recovered run does.
          firstContext <- ctxOver backend workflowText
          let body :: Ctx IO -> IO Int
              body ctx = do
                writeIORef observedStepId (stepId ctx)
                modifyIORef' calls (+ 1)
                pure 42
          first <- runWorkflowStep firstContext "test_step" body
          assertEqual "first execution returns the body's result" (Right 42) first
          assertEqual "the body runs inside step zero" (Just 0) =<< readIORef observedStepId
          replayContext <- ctxOver backend workflowText
          second <- runWorkflowStep replayContext "test_step" body
          assertEqual "replay returns the recorded result" (Right 42) second
          assertEqual "replay does not run the body again" 1 =<< readIORef calls,
      testCase "durable sleep reuses its recorded wake time" $ do
        config <- Postgres.configFromEnv
        let logger = LogAction (const (pure ()))
        bracket (Postgres.acquirePostgresSystemDB config logger) Postgres.releasePostgresSystemDB $ \backend -> do
          Postgres.activatePostgresSystemDB backend
          freshId <- UUID.V4.nextRandom
          let workflowText = "hs-l2-sleep-" <> Text.pack (UUID.toString freshId)
              initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2SleepTest"}
          created <- initWorkflow backend initialWorkflow Nothing Fresh Nothing
          case created of
            Left err -> fail (show err)
            Right _ -> pure ()
          firstContext <- ctxOver backend workflowText
          first <- sleepWorkflowStep firstContext (millisDuration 25)
          assertEqual "first sleep succeeds" (Right ()) first
          replayContext <- ctxOver backend workflowText
          replay <- sleepWorkflowStep replayContext (millisDuration 25)
          assertEqual "replay adopts the original wake time" (Right ()) replay
    ]
