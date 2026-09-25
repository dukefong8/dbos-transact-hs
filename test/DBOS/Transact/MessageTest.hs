{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Message send/receive behavior through the workflow API and live SystemDB.
module DBOS.Transact.MessageTest (tests) where

import DBOS.Prelude
import Colog.Core.Action (LogAction (..))
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (ForkOptions (..), ForkPoint (..), NewWorkflow (..), Outcome (..), Submission (..), millisDuration, newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  ( Forks (..),
    SendOptions (..),
    Topic (..),
    WorkflowId (..),
    recv,
    send,
    sendOptionsDefault,
    sendWith,
  )
import DBOS.Transact.ContextTest (ctxOver)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertEqual, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Workflow messages"
    [ testCase "a workflow send is delivered once and recv replays" $ do
        config <- Postgres.configFromEnv
        let logger = LogAction (const (pure ()))
        bracket (Postgres.acquirePostgresSystemDB config logger) Postgres.releasePostgresSystemDB $ \backend -> do
          Postgres.activatePostgresSystemDB backend
          freshId <- UUID.V4.nextRandom
          let prefix = "hs-l2-message-" <> Text.pack (UUID.toString freshId)
              sourceText = prefix <> "-source"
              destinationText = prefix <> "-destination"
              destinationId = WorkflowId destinationText
              create workflowText workflowName =
                let row = (newWorkflow workflowText) {newWorkflowName = Just workflowName}
                 in SystemDB.initWorkflow backend row Nothing Fresh Nothing
          sourceCreated <- create sourceText "L2MessageSource"
          destinationCreated <- create destinationText "L2MessageDestination"
          case (sourceCreated, destinationCreated) of
            (Right _, Right _) -> pure ()
            (Left err, _) -> fail (show err)
            (_, Left err) -> fail (show err)
          senderContext <- ctxOver backend sourceText
          let sendAction ctx = send ctx destinationId (Just (Topic "approval")) Nothing ("approved" :: Text)
          firstSend <- sendAction senderContext
          assertEqual "message send succeeds" (Right ()) firstSend
          receiverContext <- ctxOver backend destinationText
          let receiveAction ctx = recv ctx (Just (Topic "approval")) (millisDuration 100)
          firstReceive <- receiveAction receiverContext
          case firstReceive of
            Right (Just value) -> assertEqual "the addressed workflow receives the value" ("approved" :: Text) value
            other -> fail (show other)
          replaySenderContext <- ctxOver backend sourceText
          replaySend <- sendAction replaySenderContext
          assertEqual "send replay succeeds" (Right ()) replaySend
          replayReceiverContext <- ctxOver backend destinationText
          replayReceive <- receiveAction replayReceiverContext
          case replayReceive of
            Right (Just value) -> assertEqual "receive replay returns its recorded message" ("approved" :: Text) value
            other -> fail (show other),
      testCase "a send may fan out to the destination's forks" $ do
        config <- Postgres.configFromEnv
        let logger = LogAction (const (pure ()))
        bracket (Postgres.acquirePostgresSystemDB config logger) Postgres.releasePostgresSystemDB $ \backend -> do
          Postgres.activatePostgresSystemDB backend
          freshId <- UUID.V4.nextRandom
          let prefix = "hs-l2-forks-" <> Text.pack (UUID.toString freshId)
              originalText = prefix <> "-original"
              senderText = prefix <> "-sender"
              create workflowText workflowName =
                let row = (newWorkflow workflowText) {newWorkflowName = Just workflowName}
                 in SystemDB.initWorkflow backend row Nothing Fresh Nothing
          originalCreated <- create originalText "L2ForksOriginal"
          senderCreated <- create senderText "L2ForksSender"
          case (originalCreated, senderCreated) of
            (Right _, Right _) -> pure ()
            other -> fail (show other)
          settled <- SystemDB.recordWorkflowOutcome backend (WorkflowId originalText) (OutcomeOutput (Just "null"))
          case settled of
            Left err -> fail (show err)
            Right _ -> pure ()
          senderContext <- ctxOver backend senderText
          forked <-
            SystemDB.forkFrom
              backend
              [WorkflowId originalText]
              (ForkStep 0)
              ForkOptions
                { forkOptionsApplicationVersion = Nothing,
                  forkOptionsQueueName = Nothing,
                  forkOptionsQueuePartitionKey = Nothing,
                  forkOptionsTimeout = Nothing,
                  forkOptionsReplacementChildren = []
                }
              Nothing
          forkId <- case forked of
            Left err -> fail (show err)
            Right [fork] -> pure fork
            other -> fail (show other)
          -- The default addresses the destination alone.
          skipped <- sendWith senderContext (WorkflowId originalText) ("skipped" :: Text) sendOptionsDefault
          skipped @?= Right ()
          originalOnce <- notificationCount backend (WorkflowId originalText)
          forkOnce <- notificationCount backend forkId
          originalOnce @?= 1
          forkOnce @?= 0
          -- Asking for the fan-out reaches both.
          included <-
            sendWith
              senderContext
              (WorkflowId originalText)
              ("included" :: Text)
              sendOptionsDefault {forks = ForksInclude}
          included @?= Right ()
          originalTwice <- notificationCount backend (WorkflowId originalText)
          forkTwice <- notificationCount backend forkId
          originalTwice @?= 2
          forkTwice @?= 1
    ]

notificationCount :: Postgres.PostgresSystemDB -> WorkflowId -> IO Int
notificationCount backend workflowId = do
  found <- SystemDB.getAllNotifications backend workflowId
  pure (either (const 0) length found)

