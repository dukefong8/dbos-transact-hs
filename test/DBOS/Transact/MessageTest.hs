{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE RankNTypes #-}
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
  (
    EngineOnly,
    StepCtx,
    stepCtxWorkflow,
    Error (..),
    Forks (..),
    SendOptions (..),
    Topic (..),
    WorkflowId (..),
    Identity (..),
    WorkflowCtx,
    firstStepStatus,
    nextWorkflowMarker,
    nextWorkflowStepId,
    nullTracer,
    recv,
    runWorkflowStep,
    send,
    sendOptionsDefault,
    sendWith,
    withStep,
    withWorkflow,
    workflowCtxId,
  )
import DBOS.Transact.ContextTest (connOver)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertEqual, testCase, (@?=))

-- | The application identity the scoped message cases install.
messageTestIdentity :: Identity
messageTestIdentity =
  Identity
    { identityAppName = "test-app",
      identityAppVersion = "1.0.0",
      identityExecutorId = "test-executor",
      identityAppId = ""
    }

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
          senderConn <- connOver backend nullTracer
          let sendAction :: forall exec. WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
              sendAction wctx = send wctx destinationId (Just (Topic "approval")) Nothing ("approved" :: Text)
          firstSend <- withWorkflow senderConn messageTestIdentity (WorkflowId sourceText) Nothing sendAction
          assertEqual "message send succeeds" (Right ()) firstSend
          receiverConn <- connOver backend nullTracer
          let receiveAction :: forall exec. WorkflowCtx exec IO -> IO (Either (Error EngineOnly) (Maybe Text))
              receiveAction wctx = recv wctx (Just (Topic "approval")) (millisDuration 100)
          firstReceive <- withWorkflow receiverConn messageTestIdentity (WorkflowId destinationText) Nothing receiveAction
          case firstReceive of
            Right (Just value) -> assertEqual "the addressed workflow receives the value" ("approved" :: Text) value
            other -> fail (show other)
          replaySend <- withWorkflow senderConn messageTestIdentity (WorkflowId sourceText) Nothing sendAction
          assertEqual "send replay succeeds" (Right ()) replaySend
          replayReceive <- withWorkflow receiverConn messageTestIdentity (WorkflowId destinationText) Nothing receiveAction
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
          senderConn <- connOver backend nullTracer
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
          -- Both sends share one scope: separate scopes would restart the
          -- step counter and the second send would replay the first. The
          -- counts are read between the sends, while the scope is open.
          (skipped, originalOnce, forkOnce, included) <-
            withWorkflow senderConn messageTestIdentity (WorkflowId senderText) Nothing $ \senderContext -> do
              -- The default addresses the destination alone.
              skipped <- sendWith senderContext (WorkflowId originalText) ("skipped" :: Text) sendOptionsDefault
              originalOnce <- notificationCount backend (WorkflowId originalText)
              forkOnce <- notificationCount backend forkId
              -- Asking for the fan-out reaches both.
              included <-
                sendWith
                  senderContext
                  (WorkflowId originalText)
                  ("included" :: Text)
                  sendOptionsDefault {forks = ForksInclude}
              pure (skipped, originalOnce, forkOnce, included)
          skipped @?= Right ()
          originalOnce @?= 1
          forkOnce @?= 0
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
          missed <- recv receiver (Just (Topic "b")) (millisDuration 100) :: IO (Either (Error EngineOnly) (Maybe Text))
          missed @?= Right Nothing
          found <- recv receiver (Just (Topic "a")) (millisDuration 100)
          case found of
            Right (Just value) -> assertEqual "the addressed topic delivers" ("for-a" :: Text) value
            other -> fail (show other),
      testCase "a replay takes the recorded message and sends once" $
        withPair getBackend "replay-takes" $ \backend sender receiver destination -> do
          senderConn <- connOver backend nullTracer
          receiverConn <- connOver backend nullTracer
          firstSend <- send sender destination (Just (Topic "approval")) Nothing ("one" :: Text)
          firstSend @?= Right ()
          firstReceive <- recv receiver (Just (Topic "approval")) (millisDuration 100)
          case firstReceive of
            Right (Just value) -> assertEqual "first take reads the message" ("one" :: Text) value
            other -> fail (show other)
          secondSend <- send sender destination (Just (Topic "approval")) Nothing ("two" :: Text)
          secondSend @?= Right ()
          replayed <- withWorkflow receiverConn messageTestIdentity (WorkflowId (workflowCtxId receiver)) Nothing $ \replayReceiver ->
            recv replayReceiver (Just (Topic "approval")) (millisDuration 100)
          case replayed of
            Right (Just value) -> assertEqual "replay returns the recorded message" ("one" :: Text) value
            other -> fail (show other)
          secondReceive <- recv receiver (Just (Topic "approval")) (millisDuration 100)
          case secondReceive of
            Right (Just value) -> assertEqual "the second message is still queued" ("two" :: Text) value
            other -> fail (show other)
          replaySend <- withWorkflow senderConn messageTestIdentity (WorkflowId (workflowCtxId sender)) Nothing $ \replaySender ->
            send replaySender destination (Just (Topic "approval")) Nothing ("one" :: Text)
          replaySend @?= Right ()
          total <- notificationCount backend destination
          total @?= 2,
      testCase "a step may send but may not receive" $
        withPair getBackend "step-send" $ \backend sender _ destination -> do
          outcome <- (runWorkflowStep sender "probe" (\sctx -> probeSendRecv destination sctx) :: IO (Either (Error EngineOnly) Text))
          outcome @?= Right "sent recv",
      testCase "a send through a captured parent is plain and moves no id" $
        withPair getBackend "captured-send" $ \_backend sender _ destination -> do
          marker <- nextWorkflowMarker sender
          sent <- withStep sender marker (firstStepStatus 0) $ \_stepped ->
            send sender destination (Just (Topic "approval")) Nothing ("ping" :: Text)
          sent @?= Right ()
          counter <- nextWorkflowStepId sender
          counter @?= 0,
      testCase "a recv through a captured parent is refused" $
        withPair getBackend "captured-recv" $ \_backend sender _ _ -> do
          marker <- nextWorkflowMarker sender
          received <- withStep sender marker (firstStepStatus 0) $ \_stepped ->
            recv sender (Just (Topic "approval")) (millisDuration 100) :: IO (Either (Error EngineOnly) (Maybe Text))
          received @?= Left (InsideStep "recv")
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
withPair :: IO Postgres.PostgresSystemDB -> Text -> (forall execA execB. Postgres.PostgresSystemDB -> WorkflowCtx execA IO -> WorkflowCtx execB IO -> WorkflowId -> IO a) -> IO a
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
  senderConn <- connOver backend nullTracer
  receiverConn <- connOver backend nullTracer
  withWorkflow senderConn messageTestIdentity (WorkflowId sourceText) Nothing $ \sender ->
    withWorkflow receiverConn messageTestIdentity (WorkflowId destinationText) Nothing $ \receiver ->
      action backend sender receiver (WorkflowId destinationText)

-- | Inside a step body a send succeeds and a receive is refused: the
-- oracle's leaf rule for messages, observed as one Text line.
probeSendRecv :: WorkflowId -> StepCtx exec IO -> IO Text
probeSendRecv destination sctx = do
  let wctx = stepCtxWorkflow sctx
  sent <- send wctx destination (Just (Topic "approval")) Nothing ("ping" :: Text)
  received <- recv wctx (Just (Topic "approval")) (millisDuration 100) :: IO (Either (Error EngineOnly) (Maybe Text))
  pure (render sent received)
  where
    render (Right ()) (Left (InsideStep operation)) = "sent " <> operation
    render _ _ = "unexpected"

