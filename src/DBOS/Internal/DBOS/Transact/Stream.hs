{-# LANGUAGE NoFieldSelectors  #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Monadic stream adapters used by the durable workflow stream reader.
module DBOS.Transact.Stream
  ( StreamReader,
    ReadStreamOptions (..),
    defaultReadStreamOptions,
    readStream,
    readStreamFrom,
    readStreamOutside,
    writeStream,
    closeStream,
    toVectorStream,
    foldVectorStream,
  )
where

import Control.Foldl qualified as Foldl
import Data.Aeson (FromJSON, ToJSON)
import Data.Text qualified as Text
import Data.Vector.Fusion.Stream.Monadic qualified as Vector
import DBOS.Prelude
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (Duration, EncodedValue (..), Outcome (..), Serialization (..), SerializedWorkflowValue (..), StepTiming (..), WorkflowId (..), WorkflowRecord (WorkflowRecord, workflowRecordStatus), WorkflowStatus (..), WrittenBy (..), addTimeout, durationAsMillis, millisDuration, readStreamStepName, secondsDuration, streamClosedSentinel, streamClosedSerialization, timestampNow)
import DBOS.SystemDB.Types qualified as Types
import DBOS.Transact.Checkpoint (StepPlacement (..), placeCall)
import DBOS.Transact.Connection (Connection (..), runSystemDB)
import DBOS.Transact.Context (WorkflowCtx, stepId, withSystemDB, workflowId)
import DBOS.Transact.Error (EngineOnly, Error (..), decodeErrorText, encodeErrorText)
import DBOS.Transact.Serialization (CodecError (..), decodeWorkflowValue, encodeWorkflowValue)

data ReadStreamOptions = ReadStreamOptions
  { pollingInterval :: Maybe Duration,
    timeout         :: Maybe Duration
  }
  deriving stock (Eq, Show)

defaultReadStreamOptions :: ReadStreamOptions
defaultReadStreamOptions = ReadStreamOptions {pollingInterval = Nothing, timeout = Nothing}

-- | A pull reader whose next action returns either an error or the next
-- value. The action owns its position, so the same reader can be pulled
-- repeatedly while its state advances.
newtype StreamReader e m a = StreamReader (m (Either e (Maybe a)))

data ReaderState = ReaderState
  { readerOffset    :: Int,
    readerEnded     :: Bool,
    readerReplaying :: Bool,
    readerBuffer    :: [EncodedValue]
  }

-- | Values fetched per blocking read; each is still recorded and delivered
-- one pull at a time under that pull's id. Mirrors Rust @STREAM_PAGE@.
streamPageSize :: Int
streamPageSize = 100

readStream :: (FromJSON value, MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m)
           => WorkflowCtx exec m -> WorkflowId -> Text -> ReadStreamOptions -> m (StreamReader (Error EngineOnly) m value)
readStream wctx wid key options = readStreamFrom wctx wid key 0 options

readStreamFrom :: (FromJSON value, MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m)
               => WorkflowCtx exec m -> WorkflowId -> Text -> Int -> ReadStreamOptions -> m (StreamReader (Error EngineOnly) m value)
readStreamFrom wctx wid key initialOffset options =
  readStreamWith (placeCall wctx) (withSystemDB wctx) (Just (WorkflowId (workflowId wctx))) wid key initialOffset options

-- | The reader a client opens: the same state machine, a
-- 'ClientConnection' placement (no counter to agree with, nothing is
-- recorded), the client's own connection for database access, and no
-- caller whose cancellation could end the wait. Mirrors Rust
-- @Client::read_stream@, which shares @StreamReader@ with the workflow
-- reader and differs only in what it passes as the source.
readStreamOutside :: (FromJSON value, MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m)
                  => Connection m -> WorkflowId -> Text -> Int -> ReadStreamOptions -> m (StreamReader (Error EngineOnly) m value)
readStreamOutside conn = readStreamWith (pure ClientConnection) (runSystemDB conn.connSysdb) Nothing

-- | The one reader both callers build: the placement a pull stands at, how
-- to reach the system database, and the caller whose cancellation ends a
-- wait ('Nothing' from a client). The rest of the pull — buffering,
-- replay, recording, the close sentinel — the placement itself decides.
readStreamWith :: (FromJSON value, MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m)
               => m (StepPlacement exec m)
               -> (forall a. (forall db. SystemDB.SystemDB db m => db -> m a) -> m a)
               -> Maybe WorkflowId
               -> WorkflowId -> Text -> Int -> ReadStreamOptions -> m (StreamReader (Error EngineOnly) m value)
readStreamWith place runDb caller wid key initialOffset options = do
  state <- newMVar ReaderState {readerOffset = initialOffset, readerEnded = False, readerReplaying = True, readerBuffer = []}
  pure (StreamReader (modifyMVar state (readNext wid key options)))
  where
    unwrapId :: WorkflowId -> Text
    unwrapId (WorkflowId text) = text

    readNext streamWorkflow streamKey options' current
      | current.readerEnded = pure (current, Right Nothing)
      | initialOffset < 0 = pure (current {readerEnded = True}, Left (InvalidArgument "read_stream" "the offset must not be negative"))
      | maybe False ((< 1) . durationAsMillis) options'.pollingInterval =
          pure (current {readerEnded = True}, Left (InvalidArgument "read_stream" "the polling interval must be at least 1ms"))
      | otherwise = do
          -- One pull owns one placement: the id is taken here, at the call,
          -- and every wait slice below reuses it, so a replay finds the row
          -- where this pull recorded it. Mirrors Rust @StreamReader::next@.
          placement <- place
          startedAt <- timestampNow
          drivePull placement startedAt current
      where
        -- One bounded wait slice under an already-placed pull: replay
        -- adoption, cancellation, the await, and the record all share the
        -- pull's step id. Every outcome ends the pull except the unrecorded
        -- wait, which loops with the same placement.
        drivePull placement startedAt waiting = do
          replay <- replayRead placement waiting
          case replay of
            Just (updated, answer) -> pure (updated, answer)
            Nothing -> do
              -- Ids are taken in order: the first slice with no record means
              -- no later slice of this pull has one either, so they skip the
              -- adoption check. Mirrors Rust @read_next@.
              let live = case placement of
                    Recorded _ _ -> waiting {readerReplaying = False}
                    _ -> waiting
              -- Buffered values deliver without another wait: the pull that
              -- fetched the page already spent its wait.
              case live.readerBuffer of
                buffered@(_ : _) -> deliverBuffered placement startedAt live buffered
                [] -> do
                  -- A workflow reader stops when its own workflow is
                  -- cancelled; a client has no caller, so nothing to check.
                  cancelled <- case caller of
                    Nothing -> pure Nothing
                    Just callerId -> do
                      found <- runDb (\db -> SystemDB.getWorkflow db callerId)
                      pure $ case found of
                        Left err -> Just (SystemDatabase err)
                        Right (Just WorkflowRecord {workflowRecordStatus = Cancelled}) ->
                          Just (SystemDatabase (SystemDBError.WorkflowCancelled {workflowId = unwrapId callerId}))
                        _ -> Nothing
                  case cancelled of
                    Just err -> pure (live {readerEnded = True}, Left err)
                    Nothing -> do
                      let configuredPolling = fromMaybe (secondsDuration 1) options'.pollingInterval
                      (polling, deadline) <- case options'.timeout of
                        Just limit -> pure (configuredPolling, Just (fromMaybe startedAt (addTimeout startedAt limit)))
                        -- No per-value timeout: wait in bounded slices so a
                        -- cancelled reader is noticed promptly. The slices share
                        -- this pull's placement and recorded start. The 100ms
                        -- cap is ours — the oracle has no such slice, since its
                        -- wait watches cancellation directly — bounding how
                        -- stale a cancel notice can be.
                        Nothing -> do
                          sliceStart <- timestampNow
                          let slice = millisDuration (fromIntegral (min 100 (durationAsMillis configuredPolling)))
                          pure (slice, Just (fromMaybe sliceStart (addTimeout sliceStart slice)))
                      awaited <- runDb (\db -> SystemDB.awaitStreamValues db streamWorkflow streamKey live.readerOffset streamPageSize deadline polling)
                      case awaited of
                        Left err -> do
                          let readError = SystemDatabase err
                          recorded <- recordRead placement startedAt (OutcomeError (encodeErrorText readError)) (Just "rust_serde")
                          pure (live {readerEnded = True}, either (Left . SystemDatabase) (const (Left readError)) recorded)
                        Right Types.TimedOut | isNothing options'.timeout -> drivePull placement startedAt live
                        Right Types.TimedOut -> do
                          -- The reader's own workflow names the timeout: a
                          -- workflow reader names itself, a client names the
                          -- stream's workflow as Rust's reader does.
                          let readerText = case caller of
                                Just (WorkflowId callerView) -> callerView
                                Nothing -> case streamWorkflow of WorkflowId streamView -> streamView
                              timedOut = StreamTimeout readerText streamKey options'.timeout
                          recorded <- recordRead placement startedAt (OutcomeError (encodeErrorText timedOut)) (Just "rust_serde")
                          pure (live {readerEnded = True}, either (Left . SystemDatabase) (const (Left timedOut)) recorded)
                        Right Types.Ended -> do
                          recorded <- recordRead placement startedAt (OutcomeOutput (Just streamClosedSentinel)) (Just streamClosedSerialization)
                          pure (live {readerEnded = True}, either (Left . SystemDatabase) (const (Right Nothing)) recorded)
                        Right (Types.Values entries) -> deliverBuffered placement startedAt live (fillBuffer entries)
        -- Deliver the buffered head under the pull's id: recorded before it
        -- is handed over, so a read dropped mid-record loses nothing and a
        -- replay decodes the recorded entry exactly as this does. Mirrors
        -- Rust @read_next@'s deliver half.
        deliverBuffered pullPlacement pullStartedAt st buffered = case buffered of
          -- Defensive: no backend sends an empty page (Rust @expect@s on
          -- one); wait again rather than guessing.
          [] -> drivePull pullPlacement pullStartedAt st
          entry : rest
            | isClose entry -> do
                recorded <- recordRead pullPlacement pullStartedAt (OutcomeOutput (Just entry.encodedValue)) entry.encodedSerialization
                pure (st {readerEnded = True, readerBuffer = []}, either (Left . SystemDatabase) (const (Right Nothing)) recorded)
            | otherwise -> do
                recorded <- recordRead pullPlacement pullStartedAt (OutcomeOutput (Just entry.encodedValue)) entry.encodedSerialization
                case recorded of
                  Left err -> pure (st {readerEnded = True}, Left (SystemDatabase err))
                  Right () ->
                    case decodeWorkflowValue "stream value" (Just (SerializedWorkflowValue entry.encodedValue (Serialization <$> entry.encodedSerialization))) of
                      Left codecError -> pure (st {readerBuffer = rest, readerOffset = st.readerOffset + 1}, Left (decodeFailure codecError))
                      Right value -> pure (st {readerBuffer = rest, readerOffset = st.readerOffset + 1}, Right (Just value))
        -- Page into the buffer, through the first close if one arrived:
        -- the close is acted on only when delivered, since a rewind can
        -- delete it and reopen the stream. Mirrors Rust's page buffering
        -- in @read_next@.
        fillBuffer :: [EncodedValue] -> [EncodedValue]
        fillBuffer entries = case break isClose entries of
          (values, []) -> values
          (values, close : _) -> values <> [close]
        replayRead placement currentState = case placement of
          Recorded readerWctx stepNo | currentState.readerReplaying -> do
            found <- withSystemDB readerWctx (\db -> SystemDB.checkStep db (WorkflowId (workflowId readerWctx)) stepNo readStreamStepName)
            case found of
              Left err -> pure (Just (currentState {readerEnded = True}, Left (SystemDatabase err)))
              Right Nothing -> pure Nothing
              Right (Just record) -> case record.stepRecordError of
                Just errorText -> case decodeErrorText errorText of
                  Right err -> pure (Just (currentState {readerEnded = True}, Left err))
                  Left _    -> pure (Just (currentState {readerEnded = True}, Left (StepFailed readStreamStepName errorText)))
                Nothing -> case record.stepRecordOutput of
                  Nothing -> pure (Just (currentState {readerEnded = True}, Left (StepFailed readStreamStepName "recorded stream read has no output")))
                  Just output
                    | isClose (EncodedValue output record.stepRecordSerialization) -> pure (Just (currentState {readerEnded = True}, Right Nothing))
                    | otherwise ->
                        case decodeWorkflowValue "stream value" (Just (SerializedWorkflowValue output (Serialization <$> record.stepRecordSerialization))) of
                          Left codecError -> pure (Just (currentState {readerOffset = currentState.readerOffset + 1}, Left (decodeFailure codecError)))
                          Right value     -> pure (Just (currentState {readerOffset = currentState.readerOffset + 1}, Right (Just value)))
          _ -> pure Nothing
        recordRead placement startedAt outcome serialization = case placement of
          Recorded readerWctx stepNo -> do
            completedAt <- timestampNow
            withSystemDB readerWctx (\db -> SystemDB.recordStep db (WorkflowId (workflowId readerWctx)) stepNo readStreamStepName outcome serialization (Just (StepTiming startedAt completedAt)))
          _ -> pure (Right ())
        isClose encoded = encoded.encodedValue == streamClosedSentinel && encoded.encodedSerialization == Just streamClosedSerialization
        decodeFailure codecError = case codecError of
          CodecNotJson _ input       -> Deserialization "stream value" ("invalid JSON: " <> input)
          CodecTypeMismatch _ detail -> Deserialization "stream value" (Text.pack detail)

writeStream :: (ToJSON value, MonadSTM m) => WorkflowCtx exec m -> Text -> value -> m (Either (Error EngineOnly) ())
writeStream wctx key value = do
  let encoded = encodeWorkflowValue value
  placement <- placeCall wctx
  let (stepNo, writtenBy) = writePosition placement
  written <- withSystemDB wctx $ \db ->
    SystemDB.writeStream db (WorkflowId (workflowId wctx)) stepNo key encoded.serializedText (unSerialization <$> encoded.serializedSerialization) writtenBy
  pure (either (Left . SystemDatabase) Right written)

closeStream :: MonadSTM m => WorkflowCtx exec m -> Text -> m (Either (Error EngineOnly) ())
closeStream wctx key = do
  placement <- placeCall wctx
  let (stepNo, writtenBy) = writePosition placement
  closed <- withSystemDB wctx $ \db -> SystemDB.closeStream db (WorkflowId (workflowId wctx)) stepNo key writtenBy
  pure (either (Left . SystemDatabase) Right closed)

-- | The row a stream write lands on: a workflow-level write carries its step
-- id and is replay-suppressed; a step-level write carries the enclosing
-- step's id and is plain. 'placeCall' serves only these two placements, so
-- the rest is defensive.
writePosition :: StepPlacement exec m -> (Int, WrittenBy)
writePosition = \case
  Recorded _ step -> (step, Workflow)
  PlacementInsideStep sctx -> (fromMaybe 0 (stepId sctx), Step)
  Outside -> (0, Step)
  ClientConnection -> (0, Step)

unSerialization :: Serialization -> Text
unSerialization (Serialization name) = name

-- | Adapt a reader to vector's monadic stream. A read error is raised in the
-- stream's 'ExceptT' effect, keeping it distinct from normal end-of-stream.
toVectorStream :: Monad m => StreamReader e m a -> Vector.Stream (ExceptT e m) a
toVectorStream (StreamReader next) = Vector.Stream step ()
  where
    step () = do
      ExceptT $ do
        answer <- next
        pure $ case answer of
          Left err           -> Left err
          Right Nothing      -> Right Vector.Done
          Right (Just value) -> Right (Vector.Yield value ())

-- | Run a monadic left fold over a vector stream.
foldVectorStream :: Monad m => Foldl.FoldM m a b -> Vector.Stream m a -> m b
foldVectorStream (Foldl.FoldM foldStep begin extract) (Vector.Stream streamStep initial) = do
  state <- begin
  go initial state
  where
    go streamState foldState = do
      result <- streamStep streamState
      case result of
        Vector.Done -> extract foldState
        Vector.Skip nextStreamState -> go nextStreamState foldState
        Vector.Yield value nextStreamState -> do
          nextFoldState <- foldStep foldState value
          go nextStreamState nextFoldState
