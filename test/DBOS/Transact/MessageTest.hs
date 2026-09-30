{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Message send/receive behavior through the workflow API and live SystemDB.
module DBOS.Transact.MessageTest (tests) where

import DBOS.Prelude
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (ForkOptions (..), ForkPoint (..), NewWorkflow (..), Outcome (..), Submission (..), millisDuration, newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  ( Ctx,
    Error (..),
    Forks (..),
    SendOptions (..),
    Topic (..),
    WorkflowId (..),
    nullTracer,
    recv,
    runWorkflowStep,
    send,
    sendOptionsDefault,
    sendWith,
    workflowId,
  )
import DBOS.Transact.ContextTest (ctxOver)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertEqual, testCase, (@?=))

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
  testGroup
    "Workflow messages"
    [ testCase "a workflow send is delivered once and recv replays" $ do
        withSuiteBackend getBackend $ \backend -> do
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
          senderContext <- ctxOver backend nullTracer sourceText
          let sendAction ctx = send ctx destinationId (Just (Topic "approval")) Nothing ("approved" :: Text)
          firstSend <- sendAction senderContext
          assertEqual "message send succeeds" (Right ()) firstSend
          receiverContext <- ctxOver backend nullTracer destinationText
          let receiveAction ctx = recv ctx (Just (Topic "approval")) (millisDuration 100)
          firstReceive <- receiveAction receiverContext
          case firstReceive of
            Right (Just value) -> assertEqual "the addressed workflow receives the value" ("approved" :: Text) value
            other -> fail (show other)
          replaySenderContext <- ctxOver backend nullTracer sourceText
          replaySend <- sendAction replaySenderContext
          assertEqual "send replay succeeds" (Right ()) replaySend
          replayReceiverContext <- ctxOver backend nullTracer destinationText
          replayReceive <- receiveAction replayReceiverContext
          case replayReceive of
            Right (Just value) -> assertEqual "receive replay returns its recorded message" ("approved" :: Text) value
            other -> fail (show other),
      testCase "a send may fan out to the destination's forks" $ do
        withSuiteBackend getBackend $ \backend -> do
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
          senderContext <- ctxOver backend nullTracer senderText
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
          forkTwice @?= 1,
      testCase "a message from another workflow reaches its destination" $
        withPair getBackend "third-party" $ \backend sender receiver destination -> do
          sent <- send sender destination (Just (Topic "approval")) Nothing ("hello" :: Text)
          sent @?= Right ()
          received <- recv receiver (Just (Topic "approval")) (millisDuration 100)
          case received of
            Right (Just value) -> assertEqual "a third party's send is delivered" ("hello" :: Text) value
            other -> fail (show other),
      testCase "topics do not cross and absence is a value" $
        withPair getBackend "topics" $ \backend sender receiver destination -> do
          sent <- send sender destination (Just (Topic "a")) Nothing ("for-a" :: Text)
          sent @?= Right ()
          missed <- recv receiver (Just (Topic "b")) (millisDuration 100) :: IO (Either Error (Maybe Text))
          missed @?= Right Nothing
          found <- recv receiver (Just (Topic "a")) (millisDuration 100)
          case found of
            Right (Just value) -> assertEqual "the addressed topic delivers" ("for-a" :: Text) value
            other -> fail (show other),
      testCase "a replay takes the recorded message and sends once" $
        withPair getBackend "replay-takes" $ \backend sender receiver destination -> do
          firstSend <- send sender destination (Just (Topic "approval")) Nothing ("one" :: Text)
          firstSend @?= Right ()
          firstReceive <- recv receiver (Just (Topic "approval")) (millisDuration 100)
          case firstReceive of
            Right (Just value) -> assertEqual "first take reads the message" ("one" :: Text) value
            other -> fail (show other)
          secondSend <- send sender destination (Just (Topic "approval")) Nothing ("two" :: Text)
          secondSend @?= Right ()
          replayReceiver <- ctxOver backend nullTracer (workflowId receiver)
          replayed <- recv replayReceiver (Just (Topic "approval")) (millisDuration 100)
          case replayed of
            Right (Just value) -> assertEqual "replay returns the recorded message" ("one" :: Text) value
            other -> fail (show other)
          secondReceive <- recv receiver (Just (Topic "approval")) (millisDuration 100)
          case secondReceive of
            Right (Just value) -> assertEqual "the second message is still queued" ("two" :: Text) value
            other -> fail (show other)
          replaySender <- ctxOver backend nullTracer (workflowId sender)
          replaySend <- send replaySender destination (Just (Topic "approval")) Nothing ("one" :: Text)
          replaySend @?= Right ()
          total <- notificationCount backend destination
          total @?= 2,
      testCase "a step may send but may not receive" $
        withPair getBackend "step-send" $ \backend sender _ destination -> do
          senderContext <- ctxOver backend nullTracer (workflowId sender)
          outcome <- runWorkflowStep senderContext "probe" (probeSendRecv destination)
          outcome @?= Right "sent recv"
    ]

notificationCount :: Postgres.PostgresSystemDB -> WorkflowId -> IO Int
notificationCount backend destination = do
  found <- SystemDB.getAllNotifications backend destination
  pure (either (const 0) length found)

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | The suite backend behind the per-test @\backend ->@ shape, so call
-- sites keep their layout.
withSuiteBackend :: IO Postgres.PostgresSystemDB -> (Postgres.PostgresSystemDB -> IO a) -> IO a
withSuiteBackend getBackend action = getBackend >>= action

-- | A sender and receiver workflow over the suite backend: the message
-- tests' shared setup, with both contexts at step zero.
withPair :: IO Postgres.PostgresSystemDB -> Text -> (Postgres.PostgresSystemDB -> Ctx IO -> Ctx IO -> WorkflowId -> IO a) -> IO a
withPair getBackend label action = do
  backend <- getBackend
  freshId <- UUID.V4.nextRandom
  let prefix = "hs-l2-" <> label <> "-" <> Text.pack (UUID.toString freshId)
      sourceText = prefix <> "-source"
      destinationText = prefix <> "-destination"
      create workflowText workflowName =
        let row = (newWorkflow workflowText) {newWorkflowName = Just workflowName}
         in SystemDB.initWorkflow backend row Nothing Fresh Nothing
  sourceCreated <- create sourceText "L2MessageSource"
  destinationCreated <- create destinationText "L2MessageDestination"
  case (sourceCreated, destinationCreated) of
    (Right _, Right _) -> pure ()
    (Left err, _) -> fail (show err)
    (_, Left err) -> fail (show err)
  sender <- ctxOver backend nullTracer sourceText
  receiver <- ctxOver backend nullTracer destinationText
  action backend sender receiver (WorkflowId destinationText)

-- | Inside a step body a send succeeds and a receive is refused: the
-- oracle's leaf rule for messages, observed as one Text line.
probeSendRecv :: WorkflowId -> Ctx IO -> IO Text
probeSendRecv destination ctx = do
  sent <- send ctx destination (Just (Topic "approval")) Nothing ("ping" :: Text)
  received <- recv ctx (Just (Topic "approval")) (millisDuration 100) :: IO (Either Error (Maybe Text))
  pure (render sent received)
  where
    render (Right ()) (Left (InsideStep operation)) = "sent " <> operation
    render _ _ = "unexpected"

