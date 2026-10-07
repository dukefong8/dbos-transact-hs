{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Shared workflow-message scenarios: one body per case, judged by one
-- pure check on each stack. The fixture carries sender/destination pairs
-- over initialized rows, running sends and receives through a context,
-- forking, settling, and reading notification and step rows. The live tree
-- ('DBOS.Transact.MessageTest') runs them over Postgres rows, the sim
-- tree ('DBOS.Transact.MessageTestSim') over the in-memory backend —
-- whose notifier genuinely wakes receivers — and both prove the same
-- delivery record. Fresh pairs take a label so traces name their
-- workflows deterministically.
module DBOS.Transact.MessageCases
  ( MessageFixture (..),
    mfRun,
    mfFreshRun,
    probeSendRecv,
    scenarioSendDeliveredOnce,
    scenarioFanOut,
    scenarioThirdParty,
    scenarioTopics,
    scenarioReplayTakes,
    scenarioStepSend,
    scenarioCapturedSend,
    scenarioCapturedRecv,
    scenarioBulkSend,
    scenarioBulkEmpty,
    checkSendDeliveredOnce,
    checkFanOut,
    checkThirdParty,
    checkTopics,
    checkReplayTakes,
    checkStepSend,
    checkCapturedSend,
    checkCapturedRecv,
    checkBulkSend,
    checkBulkEmpty,
  )
where

import DBOS.Prelude
import DBOS.SystemDB (StepRecord (..), WorkflowId (..), sendBulkStepName)
import DBOS.Transact
  ( EngineOnly,
    Error (..),
    Forks (..),
    Message (..),
    SendOptions (..),
    Topic (..),
    WorkflowCtx,
    millisDuration,
    recv,
    runStep,
    send,
    sendBulk,
    sendOptionsDefault,
    sendWith,
  )
import DBOS.Transact.Context (firstStepStatus, nextStepId, nextWorkflowMarker, withStep)
import DBOS.Transact.Context (StepCtx (stepCtxWorkflow))

-- | What a stack must provide: labeled sender/destination pairs over
-- initialized rows, running sends and receives through a context for an
-- id, forking a workflow, settling a row, and reading notification and
-- step rows.
data MessageFixture m = MessageFixture
  { mfFreshPair :: Text -> m (WorkflowId, WorkflowId),
    mfCtx :: forall a. WorkflowId -> (forall exec. WorkflowCtx exec m -> m a) -> m a,
    mfFreshCtx :: forall a. WorkflowId -> (forall exec. WorkflowCtx exec m -> m a) -> m a,
    mfNotifyCount :: WorkflowId -> m Int,
    mfForkFrom :: WorkflowId -> m WorkflowId,
    mfSettle :: WorkflowId -> m (),
    mfCheckStep :: WorkflowId -> Text -> Int -> m (Maybe StepRecord)
  }

-- | Run with a fresh scope: the rank-2 field is read by pattern match
-- because record-dot has no 'HasField' instance for polymorphic fields.
-- | Run in the workflow's held scope: repeated calls advance the same
-- step counter, so consecutive sends and receives take consecutive ids.
-- The rank-2 field is read by pattern match because record-dot has no
-- 'HasField' instance for polymorphic fields.
mfRun :: MessageFixture m -> WorkflowId -> (forall exec. WorkflowCtx exec m -> m a) -> m a
mfRun (MessageFixture _ run _ _ _ _ _) = run

-- | Run in a fresh scope for the same workflow: the step counter restarts,
-- so a replay reads its recorded step back instead of advancing.
mfFreshRun :: MessageFixture m -> WorkflowId -> (forall exec. WorkflowCtx exec m -> m a) -> m a
mfFreshRun (MessageFixture _ _ fresh _ _ _ _) = fresh

-- | Inside a step body a send succeeds and a receive is refused: the
-- oracle's leaf rule for messages, observed as one Text line.
probeSendRecv :: forall m exec. (MonadSTM m, MonadTime m, MonadDelay m) => WorkflowId -> StepCtx exec m -> m Text
probeSendRecv destination sctx = do
  let wctx = sctx.stepCtxWorkflow
  sent <- send wctx destination (Just (Topic "approval")) Nothing ("ping" :: Text)
  received <- recv wctx (Just (Topic "approval")) (millisDuration 100) :: m (Either (Error EngineOnly) (Maybe Text))
  pure (render sent received)
  where
    render (Right ()) (Left (InsideStep operation)) = "sent " <> operation
    render _ _ = "unexpected"

-- | A workflow send is delivered once and recv replays: the replayed send
-- moves nothing again and the replayed receive reads its recording.
scenarioSendDeliveredOnce :: forall m. (MonadSTM m, MonadTime m, MonadDelay m) => MessageFixture m -> m (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text))
scenarioSendDeliveredOnce fx = do
  (source, destination) <- fx.mfFreshPair "deliver"
  firstSend <- mfRun fx source $ \wctx -> send wctx destination (Just (Topic "approval")) Nothing ("approved" :: Text)
  firstReceive <- mfRun fx destination $ \wctx -> recv wctx (Just (Topic "approval")) (millisDuration 100)
  replaySend <- mfFreshRun fx source $ \wctx -> send wctx destination (Just (Topic "approval")) Nothing ("approved" :: Text)
  replayReceive <- mfFreshRun fx destination $ \wctx -> recv wctx (Just (Topic "approval")) (millisDuration 100)
  pure (firstSend, firstReceive, replaySend, replayReceive)

-- | A send may fan out to the destination's forks: the default addresses
-- the destination alone, asking for the fan-out reaches both.
scenarioFanOut :: forall m. (MonadSTM m) => MessageFixture m -> m (Either (Error EngineOnly) (), Int, Int, Either (Error EngineOnly) (), Int, Int)
scenarioFanOut fx = do
  (original, sender) <- fx.mfFreshPair "fanout"
  fx.mfSettle original
  forkId <- fx.mfForkFrom original
  (skipped, originalOnce, forkOnce, included) <-
    mfRun fx sender $ \senderContext -> do
      skipped <- sendWith senderContext original ("skipped" :: Text) sendOptionsDefault
      originalOnce <- fx.mfNotifyCount original
      forkOnce <- fx.mfNotifyCount forkId
      included <- sendWith senderContext original ("included" :: Text) sendOptionsDefault {forks = ForksInclude}
      pure (skipped, originalOnce, forkOnce, included)
  originalTwice <- fx.mfNotifyCount original
  forkTwice <- fx.mfNotifyCount forkId
  pure (skipped, originalOnce, forkOnce, included, originalTwice, forkTwice)

-- | A message from another workflow reaches its destination.
scenarioThirdParty :: forall m. (MonadSTM m, MonadTime m, MonadDelay m) => MessageFixture m -> m (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text))
scenarioThirdParty fx = do
  (source, destination) <- fx.mfFreshPair "third-party"
  sent <- mfRun fx source $ \sender -> send sender destination (Just (Topic "approval")) Nothing ("hello" :: Text)
  received <- mfRun fx destination $ \receiver -> recv receiver (Just (Topic "approval")) (millisDuration 100)
  pure (sent, received)

-- | Topics do not cross and absence is a value: a missing topic reads
-- back empty, the addressed one delivers.
scenarioTopics :: forall m. (MonadSTM m, MonadTime m, MonadDelay m) => MessageFixture m -> m (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (Maybe Text))
scenarioTopics fx = do
  (source, destination) <- fx.mfFreshPair "topics"
  sent <- mfRun fx source $ \sender -> send sender destination (Just (Topic "a")) Nothing ("for-a" :: Text)
  missed <- mfRun fx destination $ \receiver -> recv receiver (Just (Topic "b")) (millisDuration 100)
  found <- mfRun fx destination $ \receiver -> recv receiver (Just (Topic "a")) (millisDuration 100)
  pure (sent, missed, found)

-- | A replay takes the recorded message and sends once: the replayed
-- receive reads the first message back, the live receive moves on, and
-- the replayed send moves nothing again.
scenarioReplayTakes :: forall m. (MonadSTM m, MonadTime m, MonadDelay m) => MessageFixture m -> m (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (), Int)
scenarioReplayTakes fx = do
  (source, destination) <- fx.mfFreshPair "replay-takes"
  firstSend <- mfRun fx source $ \sender -> send sender destination (Just (Topic "approval")) Nothing ("one" :: Text)
  firstReceive <- mfRun fx destination $ \receiver -> recv receiver (Just (Topic "approval")) (millisDuration 100)
  secondSend <- mfRun fx source $ \sender -> send sender destination (Just (Topic "approval")) Nothing ("two" :: Text)
  replayed <- mfFreshRun fx destination $ \replayReceiver -> recv replayReceiver (Just (Topic "approval")) (millisDuration 100)
  secondReceive <- mfRun fx destination $ \receiver -> recv receiver (Just (Topic "approval")) (millisDuration 100)
  replaySend <- mfFreshRun fx source $ \replaySender -> send replaySender destination (Just (Topic "approval")) Nothing ("one" :: Text)
  total <- fx.mfNotifyCount destination
  pure (firstSend, firstReceive, secondSend, replayed, secondReceive, replaySend, total)

-- | A step may send but may not receive.
scenarioStepSend :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => MessageFixture m -> m (Either (Error EngineOnly) Text)
scenarioStepSend fx = do
  (source, destination) <- fx.mfFreshPair "step-send"
  mfRun fx source $ \sender -> runStep sender "probe" (\sctx -> probeSendRecv destination sctx)

-- | A send through a captured parent is plain and moves no id.
scenarioCapturedSend :: forall m. (MonadSTM m, MonadCatch m) => MessageFixture m -> m (Either (Error EngineOnly) (), Int)
scenarioCapturedSend fx = do
  (source, destination) <- fx.mfFreshPair "captured-send"
  mfRun fx source $ \sender -> do
    marker <- nextWorkflowMarker sender
    sent <- withStep sender marker (firstStepStatus 0) $ \_stepped ->
      send sender destination (Just (Topic "approval")) Nothing ("ping" :: Text)
    counter <- nextStepId sender
    pure (sent, counter)

-- | A recv through a captured parent is refused.
scenarioCapturedRecv :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => MessageFixture m -> m (Either (Error EngineOnly) (Maybe Text))
scenarioCapturedRecv fx = do
  (source, _) <- fx.mfFreshPair "captured-recv"
  mfRun fx source $ \sender -> do
    marker <- nextWorkflowMarker sender
    withStep sender marker (firstStepStatus 0) $ \_stepped ->
      recv sender (Just (Topic "approval")) (millisDuration 100)

-- | A batch delivers every message and checkpoints once: both receives
-- read in order, two rows arrive, one bulk step stands for the batch.
scenarioBulkSend :: forall m. (MonadTime m, MonadCatch m, MonadAsync m, MonadTimer m) => MessageFixture m -> m (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (Maybe Text), Int, Maybe StepRecord)
scenarioBulkSend fx = do
  (source, destination) <- fx.mfFreshPair "bulk"
  sent <-
    mfRun fx source $ \sender ->
      sendBulk
        sender
        [ Message destination ("one" :: Text) (Just (Topic "bulk")) Nothing,
          Message destination ("two" :: Text) (Just (Topic "bulk")) Nothing
        ]
  (first, second) <-
    mfRun fx destination $ \receiver -> do
      first <- recv receiver (Just (Topic "bulk")) (millisDuration 100)
      second <- recv receiver (Just (Topic "bulk")) (millisDuration 100)
      pure (first, second)
  count <- fx.mfNotifyCount destination
  checkpoint <- fx.mfCheckStep source sendBulkStepName 0
  pure (sent, first, second, count, checkpoint)

-- | An empty bulk send still takes its step.
scenarioBulkEmpty :: forall m. (MonadTime m, MonadCatch m, MonadAsync m, MonadTimer m) => MessageFixture m -> m (Either (Error EngineOnly) (), Maybe StepRecord)
scenarioBulkEmpty fx = do
  (source, _) <- fx.mfFreshPair "bulk-empty"
  sent <- mfRun fx source $ \sender -> sendBulk sender ([] :: [Message Text])
  checkpoint <- fx.mfCheckStep source sendBulkStepName 0
  pure (sent, checkpoint)

-- * Checks

-- | The send succeeds, the addressed workflow receives the value, and
-- both replays agree.
checkSendDeliveredOnce :: (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text)) -> Either String ()
checkSendDeliveredOnce (firstSend, firstReceive, replaySend, replayReceive) = do
  unless (firstSend == Right ()) $ Left ("expected the send to succeed, got: " <> show firstSend)
  unless (firstReceive == Right (Just "approved")) $ Left ("expected the addressed value, got: " <> show firstReceive)
  unless (replaySend == Right ()) $ Left ("expected the replayed send to succeed, got: " <> show replaySend)
  unless (replayReceive == Right (Just "approved")) $ Left ("expected the replay to read its recording, got: " <> show replayReceive)

-- | The default reaches the destination alone (1/0); the fan-out reaches
-- both (2/1).
checkFanOut :: (Either (Error EngineOnly) (), Int, Int, Either (Error EngineOnly) (), Int, Int) -> Either String ()
checkFanOut (skipped, originalOnce, forkOnce, included, originalTwice, forkTwice) = do
  unless (skipped == Right ()) $ Left ("expected the default send to succeed, got: " <> show skipped)
  unless ((originalOnce, forkOnce) == (1, 0)) $ Left ("expected only the destination to hold the default, got: " <> show (originalOnce, forkOnce))
  unless (included == Right ()) $ Left ("expected the fan-out send to succeed, got: " <> show included)
  unless ((originalTwice, forkTwice) == (2, 1)) $ Left ("expected both to hold the fan-out, got: " <> show (originalTwice, forkTwice))

-- | A third party's send is delivered.
checkThirdParty :: (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text)) -> Either String ()
checkThirdParty (sent, received) = do
  unless (sent == Right ()) $ Left ("expected the send to succeed, got: " <> show sent)
  unless (received == Right (Just "hello")) $ Left ("expected the third party's value, got: " <> show received)

-- | The missing topic reads empty and the addressed topic delivers.
checkTopics :: (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (Maybe Text)) -> Either String ()
checkTopics (sent, missed, found) = do
  unless (sent == Right ()) $ Left ("expected the send to succeed, got: " <> show sent)
  unless (missed == Right Nothing) $ Left ("expected absence to read empty, got: " <> show missed)
  unless (found == Right (Just "for-a")) $ Left ("expected the addressed topic to deliver, got: " <> show found)

-- | The replay takes the recorded message, the live receive moves on, and
-- the replayed send moves nothing again.
checkReplayTakes :: (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (), Int) -> Either String ()
checkReplayTakes (firstSend, firstReceive, secondSend, replayed, secondReceive, replaySend, total) = do
  unless (firstSend == Right ()) $ Left ("expected the first send to succeed, got: " <> show firstSend)
  unless (firstReceive == Right (Just "one")) $ Left ("expected the first take to read one, got: " <> show firstReceive)
  unless (secondSend == Right ()) $ Left ("expected the second send to succeed, got: " <> show secondSend)
  unless (replayed == Right (Just "one")) $ Left ("expected the replay to return one, got: " <> show replayed)
  unless (secondReceive == Right (Just "two")) $ Left ("expected the second message to stay queued, got: " <> show secondReceive)
  unless (replaySend == Right ()) $ Left ("expected the replayed send to succeed, got: " <> show replaySend)
  unless (total == 2) $ Left ("expected two notification rows, got: " <> show total)

-- | The step body sends and the refused receive reads as one line.
checkStepSend :: Either (Error EngineOnly) Text -> Either String ()
checkStepSend outcome =
  unless (outcome == Right "sent recv") $ Left ("expected the probe line, got: " <> show outcome)

-- | The captured send succeeds and moves no id.
checkCapturedSend :: (Either (Error EngineOnly) (), Int) -> Either String ()
checkCapturedSend (sent, counter) = do
  unless (sent == Right ()) $ Left ("expected the captured send to succeed, got: " <> show sent)
  unless (counter == 0) $ Left ("expected no id to move, got: " <> show counter)

-- | The captured receive is refused.
checkCapturedRecv :: Either (Error EngineOnly) (Maybe Text) -> Either String ()
checkCapturedRecv received =
  unless (received == Left (InsideStep "recv")) $ Left ("expected the InsideStep refusal, got: " <> show received)

-- | Both messages arrive in order, two rows land, and one bulk step
-- stands for the batch.
checkBulkSend :: (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (Maybe Text), Int, Maybe StepRecord) -> Either String ()
checkBulkSend (sent, first, second, count, checkpoint) = do
  unless (sent == Right ()) $ Left ("expected the batch to send, got: " <> show sent)
  unless (first == Right (Just "one")) $ Left ("expected one first, got: " <> show first)
  unless (second == Right (Just "two")) $ Left ("expected two second, got: " <> show second)
  unless (count == 2) $ Left ("expected two notification rows, got: " <> show count)
  case checkpoint of
    Just _ -> pure ()
    Nothing -> Left "expected the batch to checkpoint once"

-- | The empty batch succeeds and still takes its step.
checkBulkEmpty :: (Either (Error EngineOnly) (), Maybe StepRecord) -> Either String ()
checkBulkEmpty (sent, checkpoint) = do
  unless (sent == Right ()) $ Left ("expected the empty batch to succeed, got: " <> show sent)
  case checkpoint of
    Just _ -> pure ()
    Nothing -> Left "expected the empty batch to take its step"
