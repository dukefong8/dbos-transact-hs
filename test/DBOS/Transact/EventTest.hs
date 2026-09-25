{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Event behavior through the workflow-facing API and live SystemDB.
module DBOS.Transact.EventTest (tests) where

import DBOS.Prelude
import Colog.Core.Action (LogAction (..))
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (NewWorkflow (..), StepRecord (..), Submission (..), getEventStepName, millisDuration, newWorkflow, sleepStepName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  ( Error (..),
    WorkflowId (..),
    firstStepStatus,
    getEvent,
    nextStepId,
    nextStepMarker,
    runWorkflowStepWith,
    setEvent,
    stepOptionsDefault,
    withAttempt,
  )
import DBOS.Transact.ContextTest (ctxOver)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Workflow events"
    [ testCase "a workflow can publish and replay an event" $ do
        config <- Postgres.configFromEnv
        let logger = LogAction (const (pure ()))
        bracket (Postgres.acquirePostgresSystemDB config logger) Postgres.releasePostgresSystemDB $ \backend -> do
          Postgres.activatePostgresSystemDB backend
          freshId <- UUID.V4.nextRandom
          let workflowText = "hs-l2-event-" <> Text.pack (UUID.toString freshId)
              initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2EventTest"}
          created <- SystemDB.initWorkflow backend initialWorkflow Nothing Fresh Nothing
          case created of
            Left err -> fail (show err)
            Right _ -> pure ()
          let action ctx = do
                published <- setEvent ctx "progress" ("ready" :: Text)
                case published of
                  Left err -> pure (Left err)
                  Right () -> getEvent ctx (WorkflowId workflowText) "progress" (millisDuration 100)
          firstContext <- ctxOver backend workflowText
          first <- action firstContext
          case first of
            Right (Just value) -> assertEqual "the event is visible to a workflow reader" ("ready" :: Text) value
            other -> fail (show other)
          replayContext <- ctxOver backend workflowText
          replay <- action replayContext
          case replay of
            Right (Just value) -> assertEqual "the event read replays the recorded value" ("ready" :: Text) value
            other -> fail (show other),
      testCase "a reading workflow is checkpointed and a reading step is not" $ do
        config <- Postgres.configFromEnv
        let logger = LogAction (const (pure ()))
        bracket (Postgres.acquirePostgresSystemDB config logger) Postgres.releasePostgresSystemDB $ \backend -> do
          Postgres.activatePostgresSystemDB backend
          freshId <- UUID.V4.nextRandom
          let prefix = "hs-l2-event-steps-" <> Text.pack (UUID.toString freshId)
              publisherText = prefix <> "-publisher"
              readerText = prefix <> "-reader"
              inStepText = prefix <> "-in-step"
              create workflowText workflowName =
                let row = (newWorkflow workflowText) {newWorkflowName = Just workflowName}
                 in SystemDB.initWorkflow backend row Nothing Fresh Nothing
          created <- sequence [create publisherText "L2EventPublisher", create readerText "L2EventReader", create inStepText "L2EventInStep"]
          case created of
            [Right _, Right _, Right _] -> pure ()
            other -> fail (show other)
          publisherContext <- ctxOver backend publisherText
          published <- setEvent publisherContext "answer" (42 :: Int)
          published @?= Right ()
          readerContext <- ctxOver backend readerText
          readOutside <- (getEvent readerContext (WorkflowId publisherText) "answer" (millisDuration 0) :: IO (Either Error (Maybe Int)))
          readOutside @?= Right (Just 42)
          outsideSteps <- stepNames backend readerText
          outsideSteps @?= [(0, getEventStepName), (1, sleepStepName)]
          inStepContext <- ctxOver backend inStepText
          readInside <- (runWorkflowStepWith stepOptionsDefault inStepContext "read" (\inner -> getEvent inner (WorkflowId publisherText) "answer" (millisDuration 0)) :: IO (Either Error (Maybe Int)))
          readInside @?= Right (Just 42)
          insideSteps <- stepNames backend inStepText
          insideSteps @?= [(0, "read")]
          -- The read inside the step left no id: the slot after the step is empty.
          spare <- SystemDB.checkStep backend (WorkflowId inStepText) 1 getEventStepName
          spare @?= Right Nothing,
      testCase "a refused set event spends no step id" $ do
        config <- Postgres.configFromEnv
        let logger = LogAction (const (pure ()))
        bracket (Postgres.acquirePostgresSystemDB config logger) Postgres.releasePostgresSystemDB $ \backend -> do
          Postgres.activatePostgresSystemDB backend
          freshId <- UUID.V4.nextRandom
          let workflowText = "hs-l2-event-refusal-" <> Text.pack (UUID.toString freshId)
              initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2EventRefusal"}
          created <- SystemDB.initWorkflow backend initialWorkflow Nothing Fresh Nothing
          case created of
            Left err -> fail (show err)
            Right _ -> pure ()
          context <- ctxOver backend workflowText
          marker <- nextStepMarker context
          (refused, before, after) <- withAttempt context marker (firstStepStatus 0) $ \inner -> do
            before <- nextStepId inner
            refused <- setEvent inner "progress" ("ready" :: Text)
            after <- nextStepId inner
            pure (refused, before, after)
          refused @?= Left (InsideStep "set_event")
          after @?= before + 1,
      testCase "a replayed set event does not republish" $ do
        config <- Postgres.configFromEnv
        let logger = LogAction (const (pure ()))
        bracket (Postgres.acquirePostgresSystemDB config logger) Postgres.releasePostgresSystemDB $ \backend -> do
          Postgres.activatePostgresSystemDB backend
          freshId <- UUID.V4.nextRandom
          let workflowText = "hs-l2-event-replay-" <> Text.pack (UUID.toString freshId)
              initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2EventReplay"}
          created <- SystemDB.initWorkflow backend initialWorkflow Nothing Fresh Nothing
          case created of
            Left err -> fail (show err)
            Right _ -> pure ()
          firstContext <- ctxOver backend workflowText
          first <- setEvent firstContext "progress" ("first" :: Text)
          first @?= Right ()
          -- A replay reaches the same slot with a different value and does not
          -- republish: the recorded step wins.
          replayContext <- ctxOver backend workflowText
          replayed <- setEvent replayContext "progress" ("second" :: Text)
          replayed @?= Right ()
          let readerText = workflowText <> "-reader"
          readerCreated <- SystemDB.initWorkflow backend ((newWorkflow readerText) {newWorkflowName = Just "L2EventReplayReader"}) Nothing Fresh Nothing
          case readerCreated of
            Left err -> fail (show err)
            Right _ -> pure ()
          readerContext <- ctxOver backend readerText
          readBack <- (getEvent readerContext (WorkflowId workflowText) "progress" (millisDuration 0) :: IO (Either Error (Maybe Text)))
          readBack @?= Right (Just "first")
    ]

stepNames :: Postgres.PostgresSystemDB -> Text -> IO [(Int, Text)]
stepNames backend workflowText = do
  rows <- SystemDB.listWorkflowSteps backend (WorkflowId workflowText) True Nothing Nothing Nothing
  pure $ case rows of
    Left _ -> []
    Right steps -> [(record.stepRecordStepId, record.stepRecordStepName) | record <- steps]
