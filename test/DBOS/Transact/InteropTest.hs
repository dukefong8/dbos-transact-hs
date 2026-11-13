{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Cross-language interop fixtures ported from Python's
-- @tests/test_interop.py@ (and the TypeScript/Java golden corpus).
--
-- The canonical input is the seven positional values, in the same order, and
-- the golden strings are the ones Python inserts raw. The port writes
-- portable JSON, so these cases exercise the cross-SDK envelope, event,
-- stream, message and result shapes against rows another SDK produced.
--
-- The output/value assertions are structural: portable JSON is a value
-- contract, and aeson does not promise Python's key insertion order.
module DBOS.Transact.InteropTest (tests) where

import Data.Aeson (FromJSON (..), ToJSON (..), Value (..), eitherDecodeStrict', object, toJSON, withArray, withObject, (.=), (.:))
import Data.List (sort)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding (encodeUtf8)
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Data.Vector qualified as Vector
import DBOS.Internal.Prelude
import DBOS.SystemDB (AwaitedOutcome (..), Serialization (..), SerializedWorkflowValue (..), Topic (..), WorkflowId (..), WorkflowStatus (..))
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  ( DBOS,
    Config (..),
    EngineOnly,
    Executor,
    StartOptions (..),
    WorkflowCtx,
    WorkflowHandle,
    QualifiedWorkflowName,
    methodQualifiedWorkflowName,
    WorkflowRef,
    WorkflowFilter (..),
    WorkflowRecord (..),
    QueueConflict (..),
    configFromEnv,
    defaultQueueOptions,
    defaultWorkflowFilter,
    clientConfigFromEnv,
    clientGetEvent,
    clientWorkflowRecord,
    closeClient,
    connectClient,
    getAllEvents,
    getWorkflowRecord,
    handleResult,
    launch,
    listQueuedWorkflows,
    newDBOS,
    recv,
    registerQueue,
    registerWorkflowRef,
    retrieveWorkflow,
    secondsDuration,
    sendWorkflowMessage,
    setEvent,
    shutdown,
    startOptionsDefault,
    startWorkflow,
    waitForWorkflow,
    writeStream,
  )
import DBOS.Internal.Transact.Error (Error (..), decodeErrorText)
import DBOS.Internal.Transact.Instance (dequeueWorkflows)
import DBOS.Internal.Transact.Logger (nullTracer)
import DBOS.Internal.Transact.Serialization (CodecError, decodeWorkflowValue, encodeWorkflowInput, encodeWorkflowValue)
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

-- * The canonical corpus

-- | Python's @GOLDEN_INPUTS_JSON@: positional arguments only, no
-- @namedArgs@ — the minimal portable form.
goldenInputs :: Text
goldenInputs =
  "{\"positionalArgs\":[\"hello-interop\",42,\"2025-06-15T10:30:00.000Z\",\
  \[\"alpha\",\"beta\",\"gamma\"],\
  \{\"key1\":\"value1\",\"key2\":99,\"nested\":{\"deep\":true}},true,null]}"

goldenMessage :: Text
goldenMessage = "{\"sender\":\"test\",\"payload\":[1,2,3]}"

goldenKwargs :: Text
goldenKwargs = "{\"positionalArgs\":[],\"namedArgs\":{\"name\":\"test\",\"count\":42,\"tags\":[\"a\",\"b\"]}}"

canonicalText :: Text
canonicalText = "hello-interop"

canonicalNum :: Int
canonicalNum = 42

canonicalDt :: Text
canonicalDt = "2025-06-15T10:30:00.000Z"

canonicalItems :: [Text]
canonicalItems = ["alpha", "beta", "gamma"]

canonicalMeta :: Map Text Value
canonicalMeta =
  Map.fromList
    [ ("key1", String "value1"),
      ("key2", toJSON (99 :: Int)),
      ("nested", object ["deep" .= True])
    ]

canonicalFlag :: Bool
canonicalFlag = True

canonicalMessage :: Value
canonicalMessage = object ["sender" .= ("test" :: Text), "payload" .= ([1, 2, 3] :: [Int])]

expectedOutput :: Value
expectedOutput =
  object
    [ "echo_text" .= canonicalText,
      "echo_num" .= canonicalNum,
      "echo_dt" .= canonicalDt,
      "items_count" .= length canonicalItems,
      "meta_keys" .= sort (Map.keys canonicalMeta),
      "flag" .= canonicalFlag,
      "empty" .= Null,
      "received" .= canonicalMessage
    ]

expectedEvent :: Value
expectedEvent = object ["text" .= canonicalText, "num" .= canonicalNum, "flag" .= canonicalFlag]

expectedStream :: Value
expectedStream = object ["item" .= canonicalText]

-- | The seven positional arguments, in Python's order. 'ToJSON' emits an
-- array so 'encodeWorkflowInput' writes seven positionals, and 'FromJSON'
-- reads the seven-positional array back.
data CanonicalArgs = CanonicalArgs Text Int Text [Text] (Map Text Value) Bool (Maybe Value)

instance ToJSON CanonicalArgs where
  toJSON (CanonicalArgs text' num dt items meta flag empty) =
    toJSON [toJSON text', toJSON num, toJSON dt, toJSON items, toJSON meta, toJSON flag, toJSON empty]

instance FromJSON CanonicalArgs where
  parseJSON = withArray "CanonicalArgs" $ \values ->
    case Vector.toList values of
      [text', num, dt, items, meta, flag, empty] ->
        CanonicalArgs
          <$> parseJSON text'
          <*> parseJSON num
          <*> parseJSON dt
          <*> parseJSON items
          <*> parseJSON meta
          <*> parseJSON flag
          <*> parseJSON empty
      other -> fail ("expected seven canonical arguments, got " <> show (length other))

canonicalArgs :: CanonicalArgs
canonicalArgs =
  CanonicalArgs canonicalText canonicalNum canonicalDt canonicalItems canonicalMeta canonicalFlag Nothing

-- | Python's kwargs workflow input: no positional arguments and a named
-- object.
data KwargArgs = KwargArgs Text Int [Text]

instance FromJSON KwargArgs where
  parseJSON = withObject "KwargArgs" $ \fields ->
    KwargArgs <$> fields .: "name" <*> fields .: "count" <*> fields .: "tags"

-- * Bodies

canonicalBody :: forall exec. CanonicalArgs -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Value)
canonicalBody (CanonicalArgs text' num dt items meta flag empty) wctx = do
  published <- setEvent wctx "interop_status" expectedEvent
  case published of
    Left err -> pure (Left err)
    Right () -> do
      streamed <- writeStream wctx "interop_stream" expectedStream
      case streamed of
        Left err -> pure (Left err)
        Right () -> do
          received <- recv wctx (Just (Topic "interop_topic")) (secondsDuration 30)
          pure $ case received of
            Left err -> Left err
            Right message ->
              Right
                ( object
                    [ "echo_text" .= text',
                      "echo_num" .= num,
                      "echo_dt" .= dt,
                      "items_count" .= length items,
                      "meta_keys" .= sort (Map.keys meta),
                      "flag" .= flag,
                      "empty" .= maybe Null id empty,
                      "received" .= maybe Null id message
                    ]
                )

kwargBody :: forall exec. KwargArgs -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
kwargBody (KwargArgs name count tags) _ =
  pure (Right (name <> "-" <> showText count <> "-" <> Text.intercalate "," tags))

-- * Suite

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    testGroup
      "Interop (Python golden corpus)"
      [ testCase "a canonical run writes the portable envelope, event, stream, message and result" $ do
          withInteropInstance "canonical" $ \dbos exec suffix _ canonicalRef _ -> do
            let wid = WorkflowId ("hs-interop-canonical-" <> suffix)
            started <- (startWorkflow exec canonicalRef (startOptionsDefault {startWorkflowId = Just wid}) (Just (encodeWorkflowInput canonicalArgs)) :: IO (Either (Error EngineOnly) (WorkflowHandle IO EngineOnly)))
            handle <- either (fail . show) pure started
            sendWorkflowMessage dbos wid (Just (Topic "interop_topic")) Nothing (encodeWorkflowValue canonicalMessage)
              >>= either (fail . show) pure
            result <- timeout 30000000 (handleResult handle) >>= maybe (fail "canonical run timed out") pure >>= either (fail . show) pure
            decoded <- decodeResult result
            decoded @?= expectedOutput
            backend <- getBackend
            (tag, inputs, output) <- workflowRow backend wid
            tag @?= Just "portable_json"
            payload inputs @?= expectedInputEnvelope
            payload output @?= expectedOutput
            (eventTag, eventValue) <- eventRow backend wid
            eventTag @?= Just "portable_json"
            payload eventValue @?= expectedEvent
            (streamTag, streamValue) <- streamRow backend wid
            streamTag @?= Just "portable_json"
            payload streamValue @?= expectedStream
            (messageTag, messageValue) <- messageRow backend wid
            messageTag @?= Just "portable_json"
            payload messageValue @?= canonicalMessage
            events <- getAllEvents dbos wid >>= either (fail . show) pure
            events @?= Map.fromList [("interop_status", expectedEvent)]
            record <- getWorkflowRecord dbos wid >>= either (fail . show) pure
            fmap (.workflowRecordStatus) record @?= Just Success
            clientConfig <- clientConfigFromEnv
            client <- connectClient clientConfig >>= either (fail . show) pure
            eventValue <- (clientGetEvent client wid "interop_status" (secondsDuration 0) :: IO (Either (Error EngineOnly) (Maybe Value)))
            eventValue @?= Right (Just expectedEvent)
            clientRecord <- clientWorkflowRecord client wid >>= either (fail . show) pure
            fmap (.workflowRecordStatus) clientRecord @?= Just Success
            closeClient client,
        testCase "a raw Python canonical row runs in the port" $ do
          withInteropInstance "direct" $ \dbos _ suffix queueText _ _ -> do
            let wid = WorkflowId ("hs-interop-direct-" <> suffix)
                WorkflowId widText = wid
            backend <- getBackend
            insertWorkflow backend wid "canonicalWorkflow" "interop" queueText "ENQUEUED" goldenInputs
            insertNotification backend wid "interop_topic" goldenMessage
            queued <- listQueuedWorkflows dbos (defaultWorkflowFilter {workflowFilterWorkflowIds = [widText]}) >>= either (fail . show) pure
            -- The supervisor may have claimed it already; either is fine,
            -- this case is about the raw row running in the port.
            length queued @?= 1
            _ <- dequeueWorkflows dbos >>= either (fail . show) pure
            result <- waitOutcome dbos wid
            decoded <- decodeResult (Just result)
            decoded @?= expectedOutput
            (_, _, output) <- workflowRow backend wid
            payload output @?= expectedOutput,
        testCase "a raw Python namedArgs row runs in the port" $ do
          withInteropInstance "kwargs" $ \dbos _ suffix queueText _ _ -> do
            let wid = WorkflowId ("hs-interop-kwargs-" <> suffix)
            backend <- getBackend
            insertWorkflow backend wid "kwargWorkflow" "interop" queueText "ENQUEUED" goldenKwargs
            _ <- dequeueWorkflows dbos >>= either (fail . show) pure
            result <- waitOutcome dbos wid
            case decodeWorkflowValue "result" (Just result) :: Either CodecError Text of
              Left err -> fail (show err)
              Right text -> text @?= "test-42-a,b"
            (_, _, output) <- workflowRow backend wid
            payload output @?= String "test-42-a,b",
        testCase "a Python portable error row decodes to its message" $ do
          withInteropInstance "error" $ \dbos _ suffix _ _ _ -> do
            let wid = WorkflowId ("hs-interop-error-" <> suffix)
            backend <- getBackend
            insertWorkflow backend wid "canonicalWorkflow" "interop" "" "ERROR" "null"
            insertError backend wid "{\"name\":\"ValueError\",\"message\":\"boom\",\"code\":400,\"data\":{\"order\":\"A\"}}"
            record <- getWorkflowRecord dbos wid >>= either (fail . show) pure
            fmap (.workflowRecordStatus) record @?= Just Error
            case record >>= (.workflowRecordError) of
              Nothing -> fail "expected the error column to be populated"
              Just text -> (decodeErrorText text :: Either Text (Error Text)) @?= Left "ValueError: boom"
            -- The handle path uses the same decoder: it keeps the portable
            -- record's text instead of falling back to the raw JSON.
            retrieved <- retrieveWorkflow dbos wid >>= either (fail . show) pure
            awaited <- handleResult retrieved :: IO (Either (Error Text) (Maybe SerializedWorkflowValue))
            let WorkflowId widText = wid
            awaited @?= Left (WorkflowFailed {workflowId = widText, message = "ValueError: boom"})
      ]

-- * Instance and rows

-- | A launched instance with both canonical workflows registered and one
-- queue of its own; the action receives the queue name too.
withInteropInstance :: Text -> (DBOS IO -> Executor IO -> Text -> Text -> WorkflowRef IO EngineOnly -> WorkflowRef IO EngineOnly -> IO a) -> IO a
withInteropInstance label action = do
  fresh <- UUID.V4.nextRandom
  let suffix = Text.pack (UUID.toString fresh)
      queueText = "hs-interop-q-" <> label <> "-" <> Text.take 8 suffix
      appName = "hs-interop-" <> Text.take 12 suffix
  config0 <- configFromEnv appName
  let config =
        config0
          { configAppVersion = Just ("v-" <> suffix),
            configExecutorId = Just ("exec-" <> suffix),
            configListenQueues = Just [queueText]
          }
  bracket (newDBOS config) shutdown $ \dbos -> do
    canonicalRef <- registerOrFail dbos (methodQualifiedWorkflowName "canonicalWorkflow" "interop") canonicalBody
    kwargRef <- registerOrFail dbos (methodQualifiedWorkflowName "kwargWorkflow" "interop") kwargBody
    exec <- launch dbos >>= either (fail . show) pure
    _ <- registerQueue dbos queueText defaultQueueOptions AlwaysUpdate >>= either (fail . show) pure
    action dbos exec suffix queueText canonicalRef kwargRef

registerOrFail :: (FromJSON argument, ToJSON result)
               => DBOS IO -> QualifiedWorkflowName -> (forall exec. argument -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) result)) -> IO (WorkflowRef IO EngineOnly)
registerOrFail dbos key body = do
  registered <- registerWorkflowRef dbos key body
  either (fail . show) pure registered

-- | Await a queued golden row after the dequeue sweep claims it.
waitOutcome :: DBOS IO -> WorkflowId -> IO SerializedWorkflowValue
waitOutcome dbos wid = do
  waited <- timeout 30000000 (waitForWorkflow dbos wid) >>= maybe (fail "queued golden run timed out") pure
  case waited of
    Left err -> fail (show err)
    Right (AwaitedSucceeded (Just output) serialization) ->
      pure (SerializedWorkflowValue output (Serialization <$> serialization))
    Right other -> fail ("expected the queued golden run to succeed, got: " <> show other)

decodeResult :: Maybe SerializedWorkflowValue -> IO Value
decodeResult Nothing = fail "expected a result"
decodeResult (Just stored) =
  case decodeWorkflowValue "result" (Just stored) :: Either CodecError Value of
    Left err -> fail (show err)
    Right value -> pure value

-- * Raw rows

-- | The canonical input envelope, as the port writes it. Key order is not
-- part of the contract.
expectedInputEnvelope :: Value
expectedInputEnvelope =
  object
    [ "positionalArgs"
        .= [ toJSON canonicalText,
             toJSON canonicalNum,
             toJSON canonicalDt,
             toJSON canonicalItems,
             toJSON canonicalMeta,
             toJSON canonicalFlag,
             toJSON (Nothing :: Maybe Value)
           ],
      "namedArgs" .= object []
    ]

payload :: Maybe Text -> Value
payload Nothing = Null
payload (Just text) =
  case eitherDecodeStrict' (encodeUtf8 text) of
    Right value -> value
    Left err -> error ("expected JSON, got: " <> err <> " in " <> show text)

workflowRow :: Postgres.PostgresSystemDB -> WorkflowId -> IO (Maybe Text, Maybe Text, Maybe Text)
workflowRow backend (WorkflowId wid) = run backend "interop-workflow-row" $
  Session.statement wid $
    Statement.preparable
      "select ws.serialization, coalesce(wi.inputs, ws.inputs), wo.output \
      \from dbos.workflow_status ws \
      \left join dbos.workflow_input wi using (workflow_uuid) \
      \left join dbos.workflow_output wo using (workflow_uuid) \
      \where ws.workflow_uuid = $1"
      (Encoders.param (Encoders.nonNullable Encoders.text))
      ( Decoders.singleRow
          ( (,,)
              <$> Decoders.column (Decoders.nullable Decoders.text)
              <*> Decoders.column (Decoders.nullable Decoders.text)
              <*> Decoders.column (Decoders.nullable Decoders.text)
          )
      )

eventRow :: Postgres.PostgresSystemDB -> WorkflowId -> IO (Maybe Text, Maybe Text)
eventRow backend (WorkflowId wid) = run backend "interop-event-row" $
  Session.statement wid $
    Statement.preparable
      "select serialization, value from dbos.workflow_events where workflow_uuid = $1 and key = 'interop_status'"
      (Encoders.param (Encoders.nonNullable Encoders.text))
      ( Decoders.singleRow
          ( (,)
              <$> Decoders.column (Decoders.nullable Decoders.text)
              <*> Decoders.column (Decoders.nullable Decoders.text)
          )
      )

streamRow :: Postgres.PostgresSystemDB -> WorkflowId -> IO (Maybe Text, Maybe Text)
streamRow backend (WorkflowId wid) = run backend "interop-stream-row" $
  Session.statement wid $
    Statement.preparable
      "select serialization, value from dbos.streams where workflow_uuid = $1 and key = 'interop_stream'"
      (Encoders.param (Encoders.nonNullable Encoders.text))
      ( Decoders.singleRow
          ( (,)
              <$> Decoders.column (Decoders.nullable Decoders.text)
              <*> Decoders.column (Decoders.nullable Decoders.text)
          )
      )

messageRow :: Postgres.PostgresSystemDB -> WorkflowId -> IO (Maybe Text, Maybe Text)
messageRow backend (WorkflowId wid) = run backend "interop-message-row" $
  Session.statement wid $
    Statement.preparable
      "select serialization, message from dbos.notifications where destination_uuid = $1 and topic = 'interop_topic'"
      (Encoders.param (Encoders.nonNullable Encoders.text))
      ( Decoders.singleRow
          ( (,)
              <$> Decoders.column (Decoders.nullable Decoders.text)
              <*> Decoders.column (Decoders.nullable Decoders.text)
          )
      )

insertWorkflow :: Postgres.PostgresSystemDB -> WorkflowId -> Text -> Text -> Text -> Text -> Text -> IO ()
insertWorkflow backend (WorkflowId wid) name class' queue status inputs =
  void $ run backend "interop-insert-workflow" $
    Session.statement (wid, name, class', queue, status, inputs) $
      Statement.preparable
        "insert into dbos.workflow_status \
        \(workflow_uuid, name, class_name, queue_name, status, inputs, created_at, serialization) \
        \values ($1, $2, $3, $4, $5, $6, (extract(epoch from now()) * 1000)::bigint, 'portable_json')"
        ( contramap (\(a, _, _, _, _, _) -> a) (Encoders.param (Encoders.nonNullable Encoders.text))
            <> contramap (\(_, b, _, _, _, _) -> b) (Encoders.param (Encoders.nonNullable Encoders.text))
            <> contramap (\(_, _, c, _, _, _) -> c) (Encoders.param (Encoders.nonNullable Encoders.text))
            <> contramap (\(_, _, _, d, _, _) -> d) (Encoders.param (Encoders.nonNullable Encoders.text))
            <> contramap (\(_, _, _, _, e, _) -> e) (Encoders.param (Encoders.nonNullable Encoders.text))
            <> contramap (\(_, _, _, _, _, f) -> f) (Encoders.param (Encoders.nonNullable Encoders.text))
        )
        Decoders.noResult

insertNotification :: Postgres.PostgresSystemDB -> WorkflowId -> Text -> Text -> IO ()
insertNotification backend (WorkflowId wid) topic message =
  void $ run backend "interop-insert-notification" $
    Session.statement (wid, topic, message) $
      Statement.preparable
        "insert into dbos.notifications (destination_uuid, topic, message, serialization) values ($1, $2, $3, 'portable_json')"
        ( contramap (\(a, _, _) -> a) (Encoders.param (Encoders.nonNullable Encoders.text))
            <> contramap (\(_, b, _) -> b) (Encoders.param (Encoders.nonNullable Encoders.text))
            <> contramap (\(_, _, c) -> c) (Encoders.param (Encoders.nonNullable Encoders.text))
        )
        Decoders.noResult

insertError :: Postgres.PostgresSystemDB -> WorkflowId -> Text -> IO ()
insertError backend (WorkflowId wid) err =
  void $ run backend "interop-insert-error" $
    Session.statement (wid, err) $
      Statement.preparable
        "insert into dbos.workflow_output (workflow_uuid, error) values ($1, $2)"
        ( contramap fst (Encoders.param (Encoders.nonNullable Encoders.text))
            <> contramap snd (Encoders.param (Encoders.nonNullable Encoders.text))
        )
        Decoders.noResult

run :: Postgres.PostgresSystemDB -> Text -> Session.Session a -> IO a
run backend operation session = Postgres.runSession backend operation session >>= either (fail . show) pure

acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend
