{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Shared workflow-facing stream scenarios. Both the live and simulated
-- trees run the same reader, error, and decode-continuation behavior.
module DBOS.Transact.StreamCases
  ( scenarioStreamReader,
    checkStreamReader,
    scenarioWrittenStream,
    checkWrittenStream,
    scenarioUnclosedStream,
    checkUnclosedStream,
    scenarioCloseStopsStream,
    checkCloseStopsStream,
    scenarioCrossPages,
    checkCrossPages,
    scenarioTimeoutAfterValue,
    checkTimeoutAfterValue,
    scenarioSingleStreamValue,
    checkSingleStreamValue,
    scenarioCancelledProducer,
    checkCancelledProducer,
    scenarioWriteInsideStep,
    checkWriteInsideStep,
    scenarioReplayRead,
    checkReplayRead,
    scenarioTimeoutReplay,
    checkTimeoutReplay,
    scenarioJoinedReaders,
    checkJoinedReaders,
    scenarioDroppedRead,
    checkDroppedRead,
    scenarioMissingWorkflowReplay,
    checkMissingWorkflowReplay,
    scenarioInvalidStreamRead,
    checkInvalidStreamRead,
    scenarioCancelledReader,
    checkCancelledReader,
    scenarioWaitingReadStepId,
    checkWaitingReadStepId,
    scenarioStreamReaderError,
    checkStreamReaderError,
    scenarioStreamDecodeFailure,
    checkStreamDecodeFailure
  )
where

import Control.Foldl qualified as Foldl
import Data.Aeson (FromJSON, ToJSON)
import Data.Vector.Fusion.Stream.Monadic qualified as Vector
import DBOS.Prelude
import DBOS.SystemDB
  ( Outcome (..),
    WorkflowId (..),
    closeStreamStepName,
    millisDuration,
    readStreamStepName,
    writeStreamStepName
  )
import DBOS.SystemDB qualified as SystemDB
import DBOS.Transact
  ( Error (..),
    EngineOnly,
    StreamReader,
    ReadStreamOptions (..),
    WorkflowCtx,
    closeStream,
    defaultReadStreamOptions,
    foldVectorStream,
    readStream,
    readStreamFrom,
    toVectorStream,
    writeStream,
    runStep
  )
import DBOS.Transact.Context (StepCtx (stepCtxWorkflow), withWorkflow)
import DBOS.Transact.EventCases (EventFixture (..))

-- | Write, close, and consume a stream through the public reader and FoldM
-- adapter; each value and the end marker become reader checkpoints.
scenarioStreamReader :: forall m. (MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m, MonadThrow m)
                     => EventFixture m -> m ([Text], [(Int, Text)])
scenarioStreamReader fx = do
  wid <- fx.efFreshId "stream-reader"
  let WorkflowId widText = wid
  fx.efInitRow widText "L2StreamReader"
  conn <- fx.efConn
  consumed <- withWorkflow conn fx.efIdentity wid Nothing $ \wctx -> do
    written <- writeStream wctx "progress" ("value" :: Text)
    case written of
      Left err -> pure (Left err)
      Right () -> do
        closed <- closeStream wctx "progress"
        case closed of
          Left err -> pure (Left err)
          Right () -> do
            reader <- readStream wctx wid "progress" defaultReadStreamOptions
            runExceptT (foldVectorStream (Foldl.generalize Foldl.list) (toVectorStream reader))
  values <- case consumed of
    Left err -> throwIO (userError (show err))
    Right found -> pure found
  names <- fx.efStepNames wid
  pure (values, names)

checkStreamReader :: ([Text], [(Int, Text)]) -> Either String ()
checkStreamReader = checkEq (["value"], [(0, writeStreamStepName), (1, closeStreamStepName), (2, readStreamStepName), (3, readStreamStepName)])

scenarioWrittenStream :: forall m. (MonadSTM m, MonadMVar m, MonadTime m, MonadDelay m, MonadThrow m)
                      => EventFixture m -> m ([Text], [Text], Bool)
scenarioWrittenStream fx = do
  producer <- fx.efFreshId "stream-written"
  consumer <- fx.efFreshId "stream-written-reader"
  let WorkflowId producerText = producer
      WorkflowId consumerText = consumer
  fx.efInitRow producerText "L2StreamWritten"
  fx.efInitRow consumerText "L2StreamWrittenReader"
  conn <- fx.efConn
  written <- withWorkflow conn fx.efIdentity producer Nothing $ \wctx -> do
    mapM_ (writeOrFail wctx "out") (["a", "b", "c"] :: [Text])
    closeOrFail wctx "out"
    pure (Right () :: Either (Error EngineOnly) ())
  either (throwIO . userError . show) pure written
  withWorkflow conn fx.efIdentity consumer Nothing $ \wctx -> do
    readAll <- readValues wctx producer 0 defaultReadStreamOptions
    readOffset <- readValues wctx producer 1 defaultReadStreamOptions
    reader <- (readStream wctx producer "out" defaultReadStreamOptions :: m (StreamReader (Error EngineOnly) m Text))
    _ <- runExceptT (foldVectorStream (Foldl.generalize Foldl.list) (toVectorStream reader))
    endedAgain <- runExceptT (foldVectorStream (Foldl.generalize Foldl.list) (toVectorStream reader))
    case (readAll, readOffset, endedAgain) of
      (Right allValues, Right offsetValues, Right noMore) -> pure (allValues, offsetValues, null noMore)
      other -> throwIO (userError ("stream read failed: " <> show other))

checkWrittenStream :: ([Text], [Text], Bool) -> Either String ()
checkWrittenStream = checkEq (["a", "b", "c"], ["b", "c"], True)

scenarioUnclosedStream :: (MonadSTM m, MonadMVar m, MonadTime m, MonadDelay m, MonadThrow m)
                       => EventFixture m -> m [Text]
scenarioUnclosedStream fx = do
  producer <- fx.efFreshId "stream-unclosed"
  consumer <- fx.efFreshId "stream-unclosed-reader"
  let WorkflowId producerText = producer
      WorkflowId consumerText = consumer
  fx.efInitRow producerText "L2StreamUnclosed"
  fx.efInitRow consumerText "L2StreamUnclosedReader"
  conn <- fx.efConn
  written <- withWorkflow conn fx.efIdentity producer Nothing $ \wctx -> do
    mapM_ (writeOrFail wctx "out") (["a", "b"] :: [Text])
    pure (Right () :: Either (Error EngineOnly) ())
  either (throwIO . userError . show) pure written
  fx.efFinishWorkflow producer (OutcomeOutput Nothing)
  result <- withWorkflow conn fx.efIdentity consumer Nothing $ \wctx -> readValues wctx producer 0 defaultReadStreamOptions
  either (throwIO . userError . show) pure result

checkUnclosedStream :: [Text] -> Either String ()
checkUnclosedStream = checkEq ["a", "b"]

scenarioCloseStopsStream :: (MonadSTM m, MonadMVar m, MonadTime m, MonadDelay m, MonadThrow m)
                        => EventFixture m -> m [Text]
scenarioCloseStopsStream fx = do
  wid <- fx.efFreshId "stream-close-stops"
  let WorkflowId widText = wid
  fx.efInitRow widText "L2StreamCloseStops"
  conn <- fx.efConn
  result <- withWorkflow conn fx.efIdentity wid Nothing $ \wctx -> do
    writeOrFail wctx "out" ("a" :: Text)
    closeOrFail wctx "out"
    writeOrFail wctx "out" ("after" :: Text)
    readValues wctx wid 0 defaultReadStreamOptions
  either (throwIO . userError . show) pure result

checkCloseStopsStream :: [Text] -> Either String ()
checkCloseStopsStream = checkEq ["a"]

scenarioCrossPages :: forall m. (MonadAsync m, MonadMVar m, MonadTime m, MonadDelay m, MonadThrow m)
                   => EventFixture m -> m ([Int], [Int], [Int])
scenarioCrossPages fx = do
  producer <- fx.efFreshId "stream-pages"
  prewrittenProducer <- fx.efFreshId "stream-pages-ready"
  consumer <- fx.efFreshId "stream-pages-reader"
  tailConsumer <- fx.efFreshId "stream-pages-tail-reader"
  prewrittenConsumer <- fx.efFreshId "stream-pages-ready-reader"
  let WorkflowId producerText = producer
      WorkflowId prewrittenProducerText = prewrittenProducer
      WorkflowId consumerText = consumer
      WorkflowId tailConsumerText = tailConsumer
      WorkflowId prewrittenConsumerText = prewrittenConsumer
      values = [0 .. 249] :: [Int]
  fx.efInitRow producerText "L2StreamPages"
  fx.efInitRow prewrittenProducerText "L2StreamPagesReady"
  fx.efInitRow consumerText "L2StreamPagesReader"
  fx.efInitRow tailConsumerText "L2StreamPagesTailReader"
  fx.efInitRow prewrittenConsumerText "L2StreamPagesReadyReader"
  conn <- fx.efConn
  ready <- withWorkflow conn fx.efIdentity prewrittenProducer Nothing $ \wctx -> do
    mapM_ (writeOrFail wctx "out") values
    closeOrFail wctx "out"
    pure (Right () :: Either (Error EngineOnly) ())
  either (throwIO . userError . show) pure ready
  fx.efFinishWorkflow prewrittenProducer (OutcomeOutput Nothing)
  reachedPage <- newEmptyMVar
  readerTask <- async $ withWorkflow conn fx.efIdentity consumer Nothing $ \wctx -> do
    reader <- readStream wctx producer "out" defaultReadStreamOptions :: m (StreamReader (Error EngineOnly) m Int)
    case toVectorStream reader of
      Vector.Stream next initial -> do
        let collect state found = do
              nextResult <- runExceptT (next state)
              case nextResult of
                Left err -> pure (Left err)
                Right Vector.Done -> pure (Right found)
                Right (Vector.Skip state') -> collect state' found
                Right (Vector.Yield value state') -> do
                  let found' = value : found
                  when (length found' == 150) (putMVar reachedPage ())
                  collect state' found'
        collect initial []
  written <- withWorkflow conn fx.efIdentity producer Nothing $ \wctx -> do
    mapM_ (writeOrFail wctx "out") (take 150 values)
    takeMVar reachedPage
    mapM_ (writeOrFail wctx "out") (drop 150 values)
    closeOrFail wctx "out"
    pure (Right () :: Either (Error EngineOnly) ())
  either (throwIO . userError . show) pure written
  fx.efFinishWorkflow producer (OutcomeOutput Nothing)
  allValues <- wait readerTask >>= either (throwIO . userError . show) (pure . reverse)
  tailValues <- withWorkflow conn fx.efIdentity tailConsumer Nothing $ \wctx -> readValues wctx producer 150 defaultReadStreamOptions
  prewrittenValues <- withWorkflow conn fx.efIdentity prewrittenConsumer Nothing $ \wctx -> readValues wctx prewrittenProducer 0 defaultReadStreamOptions
  case tailValues of
    Right tailRead -> case prewrittenValues of
      Right readyRead -> pure (allValues, tailRead, readyRead)
      Left err -> throwIO (userError ("prewritten stream page read failed: " <> show err))
    Left err -> throwIO (userError ("paged stream tail read failed: " <> show err))

checkCrossPages :: ([Int], [Int], [Int]) -> Either String ()
checkCrossPages (allValues, tailValues, prewrittenValues) = do
  checkEq [0 .. 249] allValues
  checkEq [150 .. 249] tailValues
  checkEq [0 .. 249] prewrittenValues

scenarioTimeoutAfterValue :: forall m. (MonadSTM m, MonadMVar m, MonadTime m, MonadDelay m, MonadThrow m)
                          => EventFixture m -> m (Maybe Text, Bool)
scenarioTimeoutAfterValue fx = do
  producer <- fx.efFreshId "stream-next-timeout"
  consumer <- fx.efFreshId "stream-next-timeout-reader"
  let WorkflowId producerText = producer
      WorkflowId consumerText = consumer
      options = defaultReadStreamOptions {timeout = Just (millisDuration 25), pollingInterval = Just (millisDuration 5)}
  fx.efInitRow producerText "L2StreamTimeout"
  fx.efInitRow consumerText "L2StreamTimeoutReader"
  conn <- fx.efConn
  written <- withWorkflow conn fx.efIdentity producer Nothing $ \wctx -> writeOrFail wctx "out" ("a" :: Text) >> pure (Right () :: Either (Error EngineOnly) ())
  either (throwIO . userError . show) pure written
  withWorkflow conn fx.efIdentity consumer Nothing $ \wctx -> do
    reader <- readStream wctx producer "out" options :: m (StreamReader (Error EngineOnly) m Text)
    case toVectorStream reader of
      Vector.Stream next initial -> do
        firstRead <- runExceptT (next initial)
        secondRead <- case firstRead of
          Right (Vector.Yield _ nextState) -> runExceptT (next nextState)
          _ -> pure (Right Vector.Done)
        pure
          ( case firstRead of Right (Vector.Yield value _) -> Just value; _ -> Nothing,
            case secondRead of Left (StreamTimeout {}) -> True; _ -> False
          )

checkTimeoutAfterValue :: (Maybe Text, Bool) -> Either String ()
checkTimeoutAfterValue = checkEq (Just "a", True)

-- | Pulling at individual offsets yields values in order, then the close
-- marker and any later offset both appear as end-of-stream.
scenarioSingleStreamValue :: forall m. (MonadAsync m, MonadMVar m, MonadTime m, MonadDelay m, MonadThrow m, MonadCatch m)
                          => EventFixture m -> m (Maybe Text, Maybe Text, Bool, Bool)
scenarioSingleStreamValue fx = do
  producer <- fx.efFreshId "stream-single-value"
  consumer <- fx.efFreshId "stream-single-value-reader"
  let WorkflowId producerText = producer
      WorkflowId consumerText = consumer
  fx.efInitRow producerText "L2StreamSingleValue"
  fx.efInitRow consumerText "L2StreamSingleValueReader"
  conn <- fx.efConn
  written <- (withWorkflow conn fx.efIdentity producer Nothing $ \wctx ->
    runStep wctx "initial" $ \sctx -> writeOrFail sctx.stepCtxWorkflow "out" ("a" :: Text)) :: m (Either (Error EngineOnly) ())
  either (throwIO . userError . show) pure written
  withWorkflow conn fx.efIdentity consumer Nothing $ \wctx -> do
    firstValue <- readStreamHead wctx producer 0 defaultReadStreamOptions
    secondTask <- async (readStreamHead wctx producer 1 defaultReadStreamOptions)
    -- Let the second pull enter its wait while the producer is still pending.
    threadDelay 20000
    appended <- (withWorkflow conn fx.efIdentity producer Nothing $ \producerCtx -> do
      initialStep <- runStep producerCtx "initial" (\_ -> pure ())
      case initialStep of
        Left err -> pure (Left err)
        Right () -> runStep producerCtx "append" $ \sctx -> do
          writeOrFail sctx.stepCtxWorkflow "out" ("b" :: Text)
          closeOrFail sctx.stepCtxWorkflow "out"
          pure ()) :: m (Either (Error EngineOnly) ())
    either (throwIO . userError . show) pure appended
    fx.efFinishWorkflow producer (OutcomeOutput Nothing)
    secondValue <- wait secondTask
    atClose <- readStreamHead wctx producer 2 defaultReadStreamOptions
    pastClose <- readStreamHead wctx producer 5 defaultReadStreamOptions
    case (firstValue, secondValue, atClose, pastClose) of
      (Right firstValue', Right secondValue', Right Nothing, Right Nothing) -> pure (firstValue', secondValue', True, True)
      other -> throwIO (userError ("single stream read failed: " <> show other))

checkSingleStreamValue :: (Maybe Text, Maybe Text, Bool, Bool) -> Either String ()
checkSingleStreamValue = checkEq (Just "a", Just "b", True, True)

-- | A cancelled producer's reader drains values already written, then ends.
scenarioCancelledProducer :: (MonadSTM m, MonadMVar m, MonadTime m, MonadDelay m, MonadThrow m)
                          => EventFixture m -> m [Text]
scenarioCancelledProducer fx = do
  producer <- fx.efFreshId "stream-cancelled-producer"
  consumer <- fx.efFreshId "stream-cancelled-reader"
  let WorkflowId producerText = producer
      WorkflowId consumerText = consumer
  fx.efInitRow producerText "L2StreamCancelledProducer"
  fx.efInitRow consumerText "L2StreamCancelledReader"
  conn <- fx.efConn
  written <- withWorkflow conn fx.efIdentity producer Nothing $ \wctx -> do
    mapM_ (writeOrFail wctx "out") (["a", "b"] :: [Text])
    pure (Right () :: Either (Error EngineOnly) ())
  either (throwIO . userError . show) pure written
  fx.efCancelWorkflow producer
  result <- withWorkflow conn fx.efIdentity consumer Nothing $ \wctx -> readValues wctx producer 0 defaultReadStreamOptions
  either (throwIO . userError . show) pure result

checkCancelledProducer :: [Text] -> Either String ()
checkCancelledProducer = checkEq ["a", "b"]

-- | Writes and close inside a step are plain; the outer step is the only
-- checkpoint the workflow records.
scenarioWriteInsideStep :: forall m. (MonadSTM m, MonadMVar m, MonadTime m, MonadDelay m, MonadCatch m)
                        => EventFixture m -> m ([Text], [(Int, Text)])
scenarioWriteInsideStep fx = do
  producer <- fx.efFreshId "stream-step-writes"
  consumer <- fx.efFreshId "stream-step-writes-reader"
  let WorkflowId producerText = producer
      WorkflowId consumerText = consumer
  fx.efInitRow producerText "L2StreamStepWrites"
  fx.efInitRow consumerText "L2StreamStepWritesReader"
  conn <- fx.efConn
  written <- (withWorkflow conn fx.efIdentity producer Nothing $ \wctx ->
    runStep wctx "produce" $ \sctx -> do
      writeOrFail sctx.stepCtxWorkflow "out" ("a" :: Text)
      writeOrFail sctx.stepCtxWorkflow "out" ("b" :: Text)
      closeOrFail sctx.stepCtxWorkflow "out"
      pure ()) :: m (Either (Error EngineOnly) ())
  either (throwIO . userError . show) pure written
  values <- withWorkflow conn fx.efIdentity consumer Nothing $ \wctx -> readValues wctx producer 0 defaultReadStreamOptions
  found <- either (throwIO . userError . show) pure values
  names <- fx.efStepNames producer
  pure (found, names)

checkWriteInsideStep :: ([Text], [(Int, Text)]) -> Either String ()
checkWriteInsideStep = checkEq (["a", "b"], [(0, "produce")])

-- | Replaying a consumer after deleting its producer returns the values and
-- close it checkpointed on the first read.
scenarioReplayRead :: (MonadSTM m, MonadMVar m, MonadTime m, MonadDelay m, MonadThrow m)
                   => EventFixture m -> m ([Text], [Text])
scenarioReplayRead fx = do
  producer <- fx.efFreshId "stream-replay-producer"
  consumer <- fx.efFreshId "stream-replay-consumer"
  let WorkflowId producerText = producer
      WorkflowId consumerText = consumer
  fx.efInitRow producerText "L2StreamReplayProducer"
  fx.efInitRow consumerText "L2StreamReplayConsumer"
  conn <- fx.efConn
  written <- withWorkflow conn fx.efIdentity producer Nothing $ \wctx -> do
    mapM_ (writeOrFail wctx "out") (["a", "b", "c"] :: [Text])
    closeOrFail wctx "out"
    pure (Right () :: Either (Error EngineOnly) ())
  either (throwIO . userError . show) pure written
  fx.efFinishWorkflow producer (OutcomeOutput Nothing)
  let readOnce = withWorkflow conn fx.efIdentity consumer Nothing $ \wctx -> readValues wctx producer 0 defaultReadStreamOptions
  firstRead <- readOnce >>= either (throwIO . userError . show) pure
  fx.efDeleteWorkflow producer
  replayed <- readOnce >>= either (throwIO . userError . show) pure
  pure (firstRead, replayed)

checkReplayRead :: ([Text], [Text]) -> Either String ()
checkReplayRead = checkEq (["a", "b", "c"], ["a", "b", "c"])

-- | A timed-out read replays the recorded timeout even after a value arrives.
scenarioTimeoutReplay :: forall m. (MonadSTM m, MonadMVar m, MonadTime m, MonadDelay m, MonadThrow m)
                      => EventFixture m -> m (Bool, Bool)
scenarioTimeoutReplay fx = do
  producer <- fx.efFreshId "stream-timeout-replay-producer"
  consumer <- fx.efFreshId "stream-timeout-replay-consumer"
  let WorkflowId producerText = producer
      WorkflowId consumerText = consumer
      options = defaultReadStreamOptions {timeout = Just (millisDuration 25), pollingInterval = Just (millisDuration 5)}
  fx.efInitRow producerText "L2StreamTimeoutReplayProducer"
  fx.efInitRow consumerText "L2StreamTimeoutReplayConsumer"
  conn <- fx.efConn
  let readTimedOut = withWorkflow conn fx.efIdentity consumer Nothing $ \wctx -> do
        answer <- readStreamHead wctx producer 0 options
        pure $ case answer of
          Left (StreamTimeout {}) -> True
          _ -> False
  firstWasTimeout <- readTimedOut
  written <- withWorkflow conn fx.efIdentity producer Nothing $ \wctx -> do
    writeOrFail wctx "out" ("late" :: Text)
    pure (Right () :: Either (Error EngineOnly) ())
  either (throwIO . userError . show) pure written
  replayed <- readTimedOut
  pure (firstWasTimeout, replayed)

checkTimeoutReplay :: (Bool, Bool) -> Either String ()
checkTimeoutReplay = checkEq (True, True)

-- | Two independent readers in one consumer workflow both see the first value.
scenarioJoinedReaders :: (MonadAsync m, MonadMVar m, MonadTime m, MonadDelay m, MonadThrow m)
                      => EventFixture m -> m (Maybe Text, Maybe Text)
scenarioJoinedReaders fx = do
  producer <- fx.efFreshId "stream-joined-producer"
  consumer <- fx.efFreshId "stream-joined-consumer"
  let WorkflowId producerText = producer
      WorkflowId consumerText = consumer
  fx.efInitRow producerText "L2StreamJoinedProducer"
  fx.efInitRow consumerText "L2StreamJoinedConsumer"
  conn <- fx.efConn
  written <- withWorkflow conn fx.efIdentity producer Nothing $ \wctx -> do
    writeOrFail wctx "out" ("a" :: Text)
    closeOrFail wctx "out"
    pure (Right () :: Either (Error EngineOnly) ())
  either (throwIO . userError . show) pure written
  fx.efFinishWorkflow producer (OutcomeOutput Nothing)
  withWorkflow conn fx.efIdentity consumer Nothing $ \wctx -> do
    (leftRead, rightRead) <- withAsync (readStreamHead wctx producer 0 defaultReadStreamOptions) $ \leftTask ->
      withAsync (readStreamHead wctx producer 0 defaultReadStreamOptions) $ \rightTask ->
        (,) <$> wait leftTask <*> wait rightTask
    case (leftRead, rightRead) of
      (Right one, Right two) -> pure (one, two)
      other -> throwIO (userError ("joined stream readers failed: " <> show other))

checkJoinedReaders :: (Maybe Text, Maybe Text) -> Either String ()
checkJoinedReaders = checkEq (Just "a", Just "a")

-- | Dropping a reader after a completed pull loses no later values; a new
-- reader at the next offset continues the stream whole.
scenarioDroppedRead :: (MonadAsync m, MonadSTM m, MonadMVar m, MonadTime m, MonadDelay m, MonadThrow m)
                    => EventFixture m -> m ([Text], [Text])
scenarioDroppedRead fx = do
  producer <- fx.efFreshId "stream-dropped-producer"
  consumer <- fx.efFreshId "stream-dropped-consumer"
  let WorkflowId producerText = producer
      WorkflowId consumerText = consumer
      values = ["0", "1", "2", "3", "4"] :: [Text]
  fx.efInitRow producerText "L2StreamDroppedProducer"
  fx.efInitRow consumerText "L2StreamDroppedConsumer"
  conn <- fx.efConn
  withWorkflow conn fx.efIdentity consumer Nothing $ \wctx -> do
    -- Abandon an in-flight pull while the producer is still pending. Its
    -- cancelled consumer must not advance the durable read offset.
    withAsync (readStreamHead wctx producer 0 defaultReadStreamOptions) $ \pendingRead -> do
      threadDelay 20000
      cancel pendingRead
    written <- withWorkflow conn fx.efIdentity producer Nothing $ \producerCtx -> do
      mapM_ (writeOrFail producerCtx "out") values
      closeOrFail producerCtx "out"
      pure (Right () :: Either (Error EngineOnly) ())
    either (throwIO . userError . show) pure written
    fx.efFinishWorkflow producer (OutcomeOutput Nothing)
    replayed <- readValues wctx producer 0 defaultReadStreamOptions
    case replayed of
      Right remaining -> pure ([], remaining)
      Left err -> throwIO (userError ("continued stream reader failed: " <> show err))

checkDroppedRead :: ([Text], [Text]) -> Either String ()
checkDroppedRead = checkEq ([], ["0", "1", "2", "3", "4"])

-- | A missing producer error is checkpointed, so creating that producer
-- later does not change the consumer's replayed result.
scenarioMissingWorkflowReplay :: forall m. (MonadSTM m, MonadMVar m, MonadTime m, MonadDelay m, MonadThrow m)
                              => EventFixture m -> m (Bool, Bool)
scenarioMissingWorkflowReplay fx = do
  consumer <- fx.efFreshId "stream-missing-replay-consumer"
  producer <- fx.efFreshId "stream-missing-replay-producer"
  let WorkflowId consumerText = consumer
      WorkflowId producerText = producer
  fx.efInitRow consumerText "L2StreamMissingReplayConsumer"
  conn <- fx.efConn
  let checkMissing = withWorkflow conn fx.efIdentity consumer Nothing $ \wctx -> do
        answer <- readStreamHead wctx producer 0 defaultReadStreamOptions
        pure $ case answer of
          Left (SystemDatabase (SystemDB.NonExistentWorkflow {})) -> True
          _ -> False
  firstMissing <- checkMissing
  fx.efInitRow producerText "L2StreamMissingReplayProducer"
  written <- withWorkflow conn fx.efIdentity producer Nothing $ \wctx -> do
    writeOrFail wctx "out" ("now-present" :: Text)
    closeOrFail wctx "out"
    pure (Right () :: Either (Error EngineOnly) ())
  either (throwIO . userError . show) pure written
  replayed <- checkMissing
  pure (firstMissing, replayed)

checkMissingWorkflowReplay :: (Bool, Bool) -> Either String ()
checkMissingWorkflowReplay = checkEq (True, True)

-- | Negative offsets and zero polling intervals are refused, and the reader
-- stays ended after its first invalid argument.
scenarioInvalidStreamRead :: forall m. (MonadSTM m, MonadMVar m, MonadTime m, MonadDelay m)
                          => EventFixture m -> m (Bool, Bool)
scenarioInvalidStreamRead fx = do
  producer <- fx.efFreshId "stream-invalid-producer"
  consumer <- fx.efFreshId "stream-invalid-consumer"
  let WorkflowId producerText = producer
      WorkflowId consumerText = consumer
      zeroPoll = defaultReadStreamOptions {pollingInterval = Just (millisDuration 0)}
  fx.efInitRow producerText "L2StreamInvalidProducer"
  fx.efInitRow consumerText "L2StreamInvalidConsumer"
  conn <- fx.efConn
  withWorkflow conn fx.efIdentity consumer Nothing $ \wctx -> do
    negative <- pullTwice wctx producer (-1) defaultReadStreamOptions
    zero <- pullTwice wctx producer 0 zeroPoll
    pure (negative, zero)
  where
    -- Both pulls go through one reader: the first reports the refusal, the
    -- second the end the refusal left behind.
    pullTwice :: WorkflowCtx exec m -> WorkflowId -> Int -> ReadStreamOptions -> m Bool
    pullTwice wctx' producer' offset options = do
      reader <- (readStreamFrom wctx' producer' "out" offset options :: m (StreamReader (Error EngineOnly) m Text))
      case toVectorStream reader of
        Vector.Stream next initial -> do
          firstPull <- runExceptT (next initial)
          secondPull <- runExceptT (next initial)
          pure (isInvalidArgument firstPull && isEnded secondPull)
    isEnded result = case result of
      Right Vector.Done -> True
      _ -> False
    isInvalidArgument result = case result of
      Left (InvalidArgument {}) -> True
      _ -> False

checkInvalidStreamRead :: (Bool, Bool) -> Either String ()
checkInvalidStreamRead = checkEq (True, True)

-- | A reader waiting on a pending producer notices cancellation of its own
-- workflow during the bounded polling interval.
scenarioCancelledReader :: forall m. (MonadAsync m, MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m, MonadThrow m)
                        => EventFixture m -> m Bool
scenarioCancelledReader fx = do
  producer <- fx.efFreshId "stream-cancelled-reader-producer"
  consumer <- fx.efFreshId "stream-cancelled-reader-consumer"
  let WorkflowId producerText = producer
      WorkflowId consumerText = consumer
  fx.efInitRow producerText "L2StreamCancelledReaderProducer"
  fx.efInitRow consumerText "L2StreamCancelledReaderConsumer"
  conn <- fx.efConn
  readTask <- async $ withWorkflow conn fx.efIdentity consumer Nothing $ \wctx ->
    readStreamHead wctx producer 0 defaultReadStreamOptions
  threadDelay 20000
  fx.efCancelWorkflow consumer
  result <- wait readTask
  pure $ case result of
    Left (SystemDatabase (SystemDB.WorkflowCancelled {})) -> True
    _ -> False

checkCancelledReader :: Bool -> Either String ()
checkCancelledReader = checkEq True

-- | A pull that waits owns one step id: the reader polls in slices while the
-- producer is pending, then delivers, ends, and the workflow's next step
-- follows at the next id. Mirrors Rust @StreamReader::next@ taking its id at
-- the call.
scenarioWaitingReadStepId :: forall m. (MonadAsync m, MonadMVar m, MonadTime m, MonadDelay m, MonadCatch m)
                            => EventFixture m -> m ([Text], [(Int, Text)])
scenarioWaitingReadStepId fx = do
  producer <- fx.efFreshId "stream-wait-step-id"
  consumer <- fx.efFreshId "stream-wait-step-id-reader"
  let WorkflowId producerText = producer
      WorkflowId consumerText = consumer
      options = defaultReadStreamOptions {pollingInterval = Just (millisDuration 20)}
  fx.efInitRow producerText "L2StreamWaitStepId"
  fx.efInitRow consumerText "L2StreamWaitStepIdReader"
  conn <- fx.efConn
  readTask <- async $ withWorkflow conn fx.efIdentity consumer Nothing $ \wctx -> do
    reader <- readStream wctx producer "out" options :: m (StreamReader (Error EngineOnly) m Text)
    found <- runExceptT (foldVectorStream (Foldl.generalize Foldl.list) (toVectorStream reader))
    case found of
      Left err -> pure (Left err)
      Right values -> do
        stepped <- runStep wctx "after" (\_ -> pure ())
        pure (fmap (values,) stepped)
  -- The producer stays pending past several polling slices, so the first
  -- pull retries well before anything is written. The margin is wide on
  -- purpose: a stall that lets the reader start late would weaken the
  -- assertion rather than fail it.
  threadDelay 200000
  written <- withWorkflow conn fx.efIdentity producer Nothing $ \wctx -> do
    writeOrFail wctx "out" ("a" :: Text)
    closeOrFail wctx "out"
    pure (Right () :: Either (Error EngineOnly) ())
  either (throwIO . userError . show) pure written
  fx.efFinishWorkflow producer (OutcomeOutput Nothing)
  (values, _) <- wait readTask >>= either (throwIO . userError . show) pure
  names <- fx.efStepNames consumer
  pure (values, names)

checkWaitingReadStepId :: ([Text], [(Int, Text)]) -> Either String ()
checkWaitingReadStepId = checkEq (["a"], [(0, readStreamStepName), (1, readStreamStepName), (2, "after")])

readStreamHead :: forall m exec. (MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m)
               => WorkflowCtx exec m -> WorkflowId -> Int -> ReadStreamOptions -> m (Either (Error EngineOnly) (Maybe Text))
readStreamHead wctx producer offset options = do
  reader <- readStreamFrom wctx producer "out" offset options :: m (StreamReader (Error EngineOnly) m Text)
  case toVectorStream reader of
    Vector.Stream next initial -> runExceptT $ do
      result <- next initial
      pure $ case result of
        Vector.Yield value _ -> Just value
        Vector.Skip _ -> Nothing
        Vector.Done -> Nothing

writeOrFail :: (ToJSON value, MonadSTM m, MonadThrow m) => WorkflowCtx exec m -> Text -> value -> m ()
writeOrFail wctx key value = writeStream wctx key value >>= either (throwIO . userError . show) pure

closeOrFail :: (MonadSTM m, MonadThrow m) => WorkflowCtx exec m -> Text -> m ()
closeOrFail wctx key = closeStream wctx key >>= either (throwIO . userError . show) pure

readValues :: (FromJSON value, MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m)
           => WorkflowCtx exec m -> WorkflowId -> Int -> ReadStreamOptions -> m (Either (Error EngineOnly) [value])
readValues wctx wid offset options = do
  reader <- readStreamFrom wctx wid "out" offset options
  runExceptT (foldVectorStream (Foldl.generalize Foldl.list) (toVectorStream reader))

-- | Missing producer errors propagate through the vector adapter.
scenarioStreamReaderError :: forall m. (MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m)
                          => EventFixture m -> m Bool
scenarioStreamReaderError fx = do
  wid <- fx.efFreshId "stream-reader-error"
  let WorkflowId widText = wid
      WorkflowId missing = WorkflowId (widText <> "-missing")
  fx.efInitRow widText "L2StreamReaderError"
  conn <- fx.efConn
  result <- withWorkflow conn fx.efIdentity wid Nothing $ \wctx -> do
    reader <- (readStream wctx (WorkflowId missing) "progress" defaultReadStreamOptions :: m (StreamReader (Error EngineOnly) m Text))
    runExceptT (foldVectorStream (Foldl.generalize Foldl.list) (toVectorStream reader))
  case result of
    Left (SystemDatabase (SystemDB.NonExistentWorkflow {})) -> pure True
    _ -> pure False

checkStreamReaderError :: Bool -> Either String ()
checkStreamReaderError = checkEq True

-- | A decode error consumes only its value; a subsequent read can still
-- yield the next value and then observe the close marker.
scenarioStreamDecodeFailure :: forall m. (MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m)
                            => EventFixture m -> m (Bool, Maybe Text, Bool)
scenarioStreamDecodeFailure fx = do
  wid <- fx.efFreshId "stream-decode-failure"
  let WorkflowId widText = wid
  fx.efInitRow widText "L2StreamDecodeFailure"
  conn <- fx.efConn
  withWorkflow conn fx.efIdentity wid Nothing $ \wctx -> do
    _ <- writeStream wctx "progress" (1 :: Int)
    _ <- writeStream wctx "progress" ("survivor" :: Text)
    _ <- closeStream wctx "progress"
    reader <- (readStream wctx wid "progress" defaultReadStreamOptions :: m (StreamReader (Error EngineOnly) m Text))
    case toVectorStream reader of
      Vector.Stream next initial -> do
        firstResult <- runExceptT (next initial)
        secondResult <- runExceptT (next initial)
        third <- case secondResult of
          Right (Vector.Yield _ nextState) -> runExceptT (next nextState)
          _ -> pure (Right Vector.Done)
        pure
          ( case firstResult of
              Left (Deserialization {}) -> True
              _ -> False,
            case secondResult of
              Right (Vector.Yield value _) -> Just value
              _ -> Nothing,
            case third of
              Right Vector.Done -> True
              _ -> False
          )

checkStreamDecodeFailure :: (Bool, Maybe Text, Bool) -> Either String ()
checkStreamDecodeFailure = checkEq (True, Just "survivor", True)

checkEq :: (Eq a, Show a) => a -> a -> Either String ()
checkEq expected actual
  | expected == actual = Right ()
  | otherwise = Left ("expected " <> show expected <> ", got " <> show actual)
