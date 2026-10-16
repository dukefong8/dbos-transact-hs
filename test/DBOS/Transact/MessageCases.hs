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
    scenarioKeyedFanOut,
    scenarioResendDeduped,
    scenarioScopedReceive,
    scenarioThirdParty,
    scenarioTopics,
    scenarioReplayTakes,
    scenarioStepSend,
    scenarioRecvTimeoutSteps,
    scenarioSendNameMismatch,
    scenarioCapturedSend,
    scenarioCapturedRecv,
    scenarioBulkSend,
    scenarioBulkEmpty,
    scenarioBatchNotifyUnique,
    checkSendDeliveredOnce,
    checkFanOut,
    checkKeyedFanOut,
    checkResendDeduped,
    checkScopedReceive,
    checkThirdParty,
    checkTopics,
    checkReplayTakes,
    checkStepSend,
    checkRecvTimeoutSteps,
    checkSendNameMismatch,
    checkCapturedSend,
    checkCapturedRecv,
    checkBulkSend,
    checkBulkEmpty,
    checkBatchNotifyUnique,
  )
where

import DBOS.Prelude
import Data.List (nub)
import DBOS.SystemDB (StepRecord (..), WorkflowId (..), recvStepName, sendBulkStepName, sendStepName, sleepStepName)
import DBOS.SystemDB qualified as SysDB
import DBOS.Transact
  ( EngineOnly,
    Error (..),
    Forks (..),
    IdempotencyKey (..),
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
    mfCheckStep :: WorkflowId -> Text -> Int -> m (Maybe StepRecord),
    mfNotifyIds :: WorkflowId -> m [Text]
  }

-- | Run with a fresh scope: the rank-2 field is read by pattern match
-- because record-dot has no 'HasField' instance for polymorphic fields.
-- | Run in the workflow's held scope: repeated calls advance the same
-- step counter, so consecutive sends and receives take consecutive ids.
-- The rank-2 field is read by pattern match because record-dot has no
-- 'HasField' instance for polymorphic fields.
mfRun :: MessageFixture m -> WorkflowId -> (forall exec. WorkflowCtx exec m -> m a) -> m a
mfRun (MessageFixture _ run _ _ _ _ _ _) = run

-- | Run in a fresh scope for the same workflow: the step counter restarts,
-- so a replay reads its recorded step back instead of advancing.
mfFreshRun :: MessageFixture m -> WorkflowId -> (forall exec. WorkflowCtx exec m -> m a) -> m a
mfFreshRun (MessageFixture _ _ fresh _ _ _ _ _) = fresh

-- | Inside a step body a send succeeds and a receive is refused: the
-- oracle's leaf rule for messages, observed as one Text line.
probeSendRecv :: forall m exec. (MonadSTM m, MonadTime m, MonadDelay m)
              => WorkflowId -> StepCtx exec m -> m Text
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
scenarioSendDeliveredOnce :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
                          => MessageFixture m -> m (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text))
scenarioSendDeliveredOnce fx = do
  (source, destination) <- fx.mfFreshPair "deliver"
  firstSend <- mfRun fx source $ \wctx -> send wctx destination (Just (Topic "approval")) Nothing ("approved" :: Text)
  firstReceive <- mfRun fx destination $ \wctx -> recv wctx (Just (Topic "approval")) (millisDuration 100)
  replaySend <- mfFreshRun fx source $ \wctx -> send wctx destination (Just (Topic "approval")) Nothing ("approved" :: Text)
  replayReceive <- mfFreshRun fx destination $ \wctx -> recv wctx (Just (Topic "approval")) (millisDuration 100)
  pure (firstSend, firstReceive, replaySend, replayReceive)

-- | A send may fan out to the destination's forks: the default addresses
-- the destination alone, asking for the fan-out reaches both.
scenarioFanOut :: forall m. (MonadSTM m)
               => MessageFixture m -> m (Either (Error EngineOnly) (), Int, Int, Either (Error EngineOnly) (), Int, Int)
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
scenarioThirdParty :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
                   => MessageFixture m -> m (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text))
scenarioThirdParty fx = do
  (source, destination) <- fx.mfFreshPair "third-party"
  sent <- mfRun fx source $ \sender -> send sender destination (Just (Topic "approval")) Nothing ("hello" :: Text)
  received <- mfRun fx destination $ \receiver -> recv receiver (Just (Topic "approval")) (millisDuration 100)
  pure (sent, received)

-- | Topics do not cross and absence is a value: a missing topic reads
-- back empty, the addressed one delivers.
scenarioTopics :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
               => MessageFixture m -> m (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (Maybe Text))
scenarioTopics fx = do
  (source, destination) <- fx.mfFreshPair "topics"
  sent <- mfRun fx source $ \sender -> send sender destination (Just (Topic "a")) Nothing ("for-a" :: Text)
  missed <- mfRun fx destination $ \receiver -> recv receiver (Just (Topic "b")) (millisDuration 100)
  found <- mfRun fx destination $ \receiver -> recv receiver (Just (Topic "a")) (millisDuration 100)
  pure (sent, missed, found)

-- | A replay takes the recorded message and sends once: the replayed
-- receive reads the first message back, the live receive moves on, and
-- the replayed send moves nothing again.
scenarioReplayTakes :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
                    => MessageFixture m -> m (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (), Int)
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
scenarioStepSend :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m)
                 => MessageFixture m -> m (Either (Error EngineOnly) Text)
scenarioStepSend fx = do
  (source, destination) <- fx.mfFreshPair "step-send"
  mfRun fx source $ \sender -> runStep sender "probe" (\sctx -> probeSendRecv destination sctx)

-- | A send from a position that holds a step recorded under another name is
-- refused: the workflow's code changed at this position since the row was
-- written, so delivering would be a send the first run never made. Returns
-- the refusal and the messages the destination holds.
scenarioSendNameMismatch :: forall m. (MonadSTM m, MonadTime m, MonadCatch m)
                         => MessageFixture m -> m (Either (Error EngineOnly) (), Int)
scenarioSendNameMismatch fx = do
  (source, destination) <- fx.mfFreshPair "send-name-mismatch"
  -- The earlier run recorded a plain step at this position.
  _ <- mfFreshRun fx source $ \sender -> runStep sender "probe" (\_ -> pure ())
  -- The changed body sends from the same position.
  refused <- mfFreshRun fx source $ \sender -> send sender destination (Just (Topic "approval")) Nothing ("ping" :: Text)
  count <- fx.mfNotifyCount destination
  pure (refused, count)

-- | A timed-out recv records both its steps: the recv step with no output
-- (the absence) at the first id, and the deadline sleep step at the next.
-- Both oracles record the deadline unconditionally, so a replay that waited
-- has the same remainder left rather than the full duration. Returns the
-- recv verdict with the two steps' outputs and the sleep step's name.
scenarioRecvTimeoutSteps :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
                         => MessageFixture m -> m (Either (Error EngineOnly) (Maybe Text), Maybe (Maybe Text), Maybe (Maybe Text), Maybe Text)
scenarioRecvTimeoutSteps fx = do
  (source, _) <- fx.mfFreshPair "recv-timeout-steps"
  timedOut <- mfFreshRun fx source $ \receiver -> recv receiver (Just (Topic "quiet")) (millisDuration 5)
  recvStep <- fx.mfCheckStep source recvStepName 0
  sleepStep <- fx.mfCheckStep source sleepStepName 1
  pure (timedOut, (.stepRecordOutput) <$> recvStep, (.stepRecordOutput) <$> sleepStep, (.stepRecordStepName) <$> sleepStep)

-- | A send through a captured parent is plain and moves no id.
scenarioCapturedSend :: forall m. (MonadSTM m, MonadCatch m)
                     => MessageFixture m -> m (Either (Error EngineOnly) (), Int)
scenarioCapturedSend fx = do
  (source, destination) <- fx.mfFreshPair "captured-send"
  mfRun fx source $ \sender -> do
    marker <- nextWorkflowMarker sender
    sent <- withStep sender marker (firstStepStatus 0) $ \_stepped ->
      send sender destination (Just (Topic "approval")) Nothing ("ping" :: Text)
    counter <- nextStepId sender
    pure (sent, counter)

-- | A recv through a captured parent is refused.
scenarioCapturedRecv :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m)
                     => MessageFixture m -> m (Either (Error EngineOnly) (Maybe Text))
scenarioCapturedRecv fx = do
  (source, _) <- fx.mfFreshPair "captured-recv"
  mfRun fx source $ \sender -> do
    marker <- nextWorkflowMarker sender
    withStep sender marker (firstStepStatus 0) $ \_stepped ->
      recv sender (Just (Topic "approval")) (millisDuration 100)

-- | A batch delivers every message and checkpoints once: both receives
-- read in order, two rows arrive, one bulk step stands for the batch.
scenarioBulkSend :: forall m. (MonadTime m, MonadCatch m, MonadAsync m, MonadTimer m)
                 => MessageFixture m -> m (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (Maybe Text), Int, Maybe StepRecord)
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
scenarioBulkEmpty :: forall m. (MonadTime m, MonadCatch m, MonadAsync m, MonadTimer m)
                  => MessageFixture m -> m (Either (Error EngineOnly) (), Maybe StepRecord)
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

-- | A keyed send fans out like an unkeyed one: the duplicate-key refusal
-- applies to the caller's list, before fan-out, as both oracles check.
scenarioKeyedFanOut :: forall m. (MonadSTM m)
                    => MessageFixture m -> m (Either (Error EngineOnly) (), Int, Int)
scenarioKeyedFanOut fx = do
  (original, sender) <- fx.mfFreshPair "keyed-fanout"
  fx.mfSettle original
  forkId <- fx.mfForkFrom original
  included <-
    mfRun fx sender $ \senderContext ->
      sendWith senderContext original ("included" :: Text) (sendOptionsDefault {forks = ForksInclude, idempotencyKey = Just (IdempotencyKey "fanout-key")})
  originalTwice <- fx.mfNotifyCount original
  forkTwice <- fx.mfNotifyCount forkId
  pure (included, originalTwice, forkTwice)

checkKeyedFanOut :: (Either (Error EngineOnly) (), Int, Int) -> Either String ()
checkKeyedFanOut (included, originalTwice, forkTwice) = do
  unless (included == Right ()) $ Left ("expected the keyed fan-out send to succeed, got: " <> show included)
  unless ((originalTwice, forkTwice) == (1, 1)) $ Left ("expected both to hold the fan-out, got: " <> show (originalTwice, forkTwice))

-- | A resend under one key delivers once: the second send succeeds but moves
-- no new message, so the second receive times out.
scenarioResendDeduped :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
                      => MessageFixture m -> m (Either (Error EngineOnly) (), Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (Maybe Text))
scenarioResendDeduped fx = do
  (source, destination) <- fx.mfFreshPair "resend-dedup"
  firstSend <- mfRun fx source $ \wctx -> send wctx destination (Just (Topic "approval")) (Just (IdempotencyKey "webhook-1")) ("approved" :: Text)
  secondSend <- mfRun fx source $ \wctx -> send wctx destination (Just (Topic "approval")) (Just (IdempotencyKey "webhook-1")) ("approved" :: Text)
  firstReceive <- mfRun fx destination $ \wctx -> recv wctx (Just (Topic "approval")) (millisDuration 100)
  secondReceive <- mfRun fx destination $ \wctx -> recv wctx (Just (Topic "approval")) (millisDuration 100)
  pure (firstSend, secondSend, firstReceive, secondReceive)

checkResendDeduped :: (Either (Error EngineOnly) (), Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text), Either (Error EngineOnly) (Maybe Text)) -> Either String ()
checkResendDeduped (firstSend, secondSend, firstReceive, secondReceive) = do
  unless (firstSend == Right ()) $ Left ("expected the first send to succeed, got: " <> show firstSend)
  unless (secondSend == Right ()) $ Left ("expected the resend to succeed without delivering, got: " <> show secondSend)
  unless (firstReceive == Right (Just "approved")) $ Left ("expected the one delivery, got: " <> show firstReceive)
  unless (secondReceive == Right Nothing) $ Left ("expected the resend to leave nothing queued, got: " <> show secondReceive)

-- | Topics do not cross workflows: a message addressed elsewhere is not
-- readable here, even on a shared topic.
scenarioScopedReceive :: forall m. (MonadSTM m, MonadTime m, MonadDelay m)
                      => MessageFixture m -> m (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text))
scenarioScopedReceive fx = do
  (sourceA, destA) <- fx.mfFreshPair "scope-a"
  (_sourceB, destB) <- fx.mfFreshPair "scope-b"
  sent <- mfRun fx sourceA $ \wctx -> send wctx destA (Just (Topic "shared")) Nothing ("for-a" :: Text)
  missed <- mfRun fx destB $ \wctx -> recv wctx (Just (Topic "shared")) (millisDuration 100)
  pure (sent, missed)

checkScopedReceive :: (Either (Error EngineOnly) (), Either (Error EngineOnly) (Maybe Text)) -> Either String ()
checkScopedReceive (sent, missed) = do
  unless (sent == Right ()) $ Left ("expected the send to succeed, got: " <> show sent)
  unless (missed == Right Nothing) $ Left ("expected another workflow's message to stay unreadable, got: " <> show missed)

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

-- | The recv timed out with no output recorded, and the deadline sleep step
-- stands beside it carrying the wake instant.
checkRecvTimeoutSteps :: (Either (Error EngineOnly) (Maybe Text), Maybe (Maybe Text), Maybe (Maybe Text), Maybe Text) -> Either String ()
checkRecvTimeoutSteps (timedOut, recvOutput, sleepOutput, sleepName) = do
  unless (timedOut == Right Nothing) $ Left ("expected the timeout absence, got: " <> show timedOut)
  unless (recvOutput == Just Nothing) $ Left ("expected the recv step to record no output, got: " <> show recvOutput)
  unless (sleepName == Just "DBOS.sleep") $ Left ("expected the deadline step beside the recv, got: " <> show sleepName)
  case sleepOutput of
    Just (Just _) -> pure ()
    other -> Left ("expected the deadline step to carry its wake instant, got: " <> show other)

-- | The refusal names the recorded step, and nothing was delivered.
checkSendNameMismatch :: (Either (Error EngineOnly) (), Int) -> Either String ()
checkSendNameMismatch (refused, count) = do
  case refused of
    Left (SystemDatabase (SysDB.UnexpectedStep {stepId, expected, recorded})) -> do
      unless (stepId == 0) $ Left ("expected the first position, got: " <> show stepId)
      unless (expected == sendStepName) $ Left ("expected the send's name, got: " <> show expected)
      unless (recorded == "probe") $ Left ("expected the recorded name to be the plain step, got: " <> show recorded)
    other -> Left ("expected an UnexpectedStep refusal, got: " <> show other)
  unless (count == 0) $ Left ("expected nothing delivered, got: " <> show count)

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

-- | Two deliveries in one batch carry distinct notification ids: the stored
-- id is unique per row live (the message uuid), so the sim must not mint
-- one counter value twice.
scenarioBatchNotifyUnique :: forall m. (MonadTime m, MonadCatch m, MonadAsync m, MonadTimer m)
                          => MessageFixture m -> m (Either (Error EngineOnly) (), [Text])
scenarioBatchNotifyUnique fx = do
  (source, destination) <- fx.mfFreshPair "batch-notify-unique"
  sent <-
    mfRun fx source $ \sender ->
      sendBulk
        sender
        [ Message destination ("one" :: Text) (Just (Topic "approval")) Nothing,
          Message destination ("two" :: Text) (Just (Topic "approval")) Nothing
        ]
  ids <- fx.mfNotifyIds destination
  pure (sent, ids)

checkBatchNotifyUnique :: (Either (Error EngineOnly) (), [Text]) -> Either String ()
checkBatchNotifyUnique (sent, ids) = do
  unless (sent == Right ()) $ Left ("expected the batch to send, got: " <> show sent)
  unless (length ids == 2) $ Left ("expected two notification rows, got: " <> show ids)
  unless (nub ids == ids) $ Left ("expected distinct notification ids, got: " <> show ids)
