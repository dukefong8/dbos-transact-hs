{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Public-behavior tests for the @client.rs@ outside-in surface.
module DBOS.Transact.ClientTest (tests) where

import DBOS.Prelude
import Data.Aeson (Value (..))
import Data.Int (Int64)
import Data.Map.Strict qualified as Map
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Data.Vector.Fusion.Stream.Monadic qualified as Vector
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import Hasql.Transaction qualified as Tx
import DBOS.SystemDB (Applications (..), QueueRecord (..), VersionInfo (..), WorkflowFilter (..), WorkflowId (..), WorkflowRecord (..), WorkflowStatus (..), defaultForkOptions, defaultWorkflowFilter, forkNew, getWorkflow, millisDuration, secondsDuration)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  (
    EngineOnly, Client,
    ClientConfig (..),
    Change (..),
    CodecError,
    Config (..),
    DBOS,
    Executor,
    DuplicationPolicy (..),
    Enqueue (..),
    EnqueueOptions (..),
    Environment (..),
    Error (..),
    IdempotencyKey (..),
    Queue (..),
    QueueChange (..),
    QueueConflict (..),
    QueueOptions (..),
    WorkflowCtx,
    SerializedWorkflowValue (..),
    SendMessage (..),
    StreamReader,
    Topic (..),
    clientCancelWorkflows,
    clientConfigFromEnv,
    clientConfigNew,
    clientDeleteWorkflows,
    clientDeleteQueue,
    clientForkWorkflows,
    clientGetEvent,
    clientLatestApplicationVersion,
    clientListApplicationVersions,
    clientListQueues,
    clientListQueuedWorkflows,
    clientReadStream,
    clientReadStreamFrom,
    clientRegisterQueue,
    clientRetrieveQueue,
    clientUpdateQueue,
    clientUpdateWorkflowAttributes,
    clientSetWorkflowDelay,
    clientWaitFirst,
    clientListWorkflows,
    clientPromoteVersion,
    clientResumeWorkflows,
    clientRewindWorkflows,
    clientSendMessage,
    clientSendMessages,
    clientSendMessageInTx,
    clientSendMessagesInTx,
    clientConfigFromEnv,
    clientConfigNew,
    closeClient,
    configFromEnv,
    connectClient,
    decodeWorkflowValue,
    defaultQueueChange,
    defaultQueueOptions,
    defaultReadStreamOptions,
    defaultRewindOptions,
    encodeWorkflowValue,
    clientEnqueueWorkflowInTx,
    clientEnqueueWorkflowWith,
    clientEnqueueWorkflow,
    enqueueNew,
    enqueueOptionsNew,
    enqueueOptionsOn,
    handleResult,
    launchWithEnvironment,
    newDBOS,
    newWorkflowKey,
    registerDataSource,
    registerWorkflow,
    registerQueue,
    clientRetrieveWorkflow,
    recv,
    runWorkflow,
    runStep,
    setEvent,
    shutdown,
    validateClientConfig,
    RewindOptions (rewindStartStep),
    WorkflowKey,
    clientWorkflowStatus,
    WorkflowHandle (workflowId),
    WorkflowDelay (..),
    recv,
    runStep,
    setEvent,
    toVectorStream,
    writeStream)
import DBOS.Transact.DatasourceCases (FakeDs (..), mkFakeDs)
import DBOS.Transact.Datasource (DataSource (..))
import DBOS.Transact.Datasource qualified as DSTx (Tx (Tx))
import DBOS.Transact.Logger (nullTracer)
import DBOS.Transact.Instance (dequeueWorkflows)
import DBOS.Transact.Workflow (storedPriority, validateEnqueue)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

-- | Launch over the isolated environment and hand back the executor.
launchClientExec :: DBOS IO -> Environment -> IO (Executor IO)
launchClientExec dbos env = do
  started <- launchWithEnvironment dbos env
  case started of
    Left err -> fail (show err)
    Right executor -> pure executor

tests :: TestTree
tests =
  withResource acquireSuiteBackend releaseSuiteBackend $ \getBackend ->
  testGroup
    "Client"
    [ testCase "connect reports a missing url by the name of the variable that sets it" $ do
        case validateClientConfig (clientConfigNew "") of
          Left err ->
            assertBool "names the variable" ("DBOS_DATABASE_URL" `Text.isInfixOf` Text.pack (show err))
          Right () -> fail "expected the empty url to be refused",
      testCase "a client enqueues by name and the app runs it" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-" <> Text.take 12 suffix
            appVersion = "hs-l2-client-version-" <> suffix
            executorId = "hs-l2-client-executor-" <> suffix
            queueName = "hs-l2-client-q-" <> Text.take 12 suffix
            key = newWorkflowKey "double"
        config0 <- configFromEnv appName
        -- The driven dequeue and the background supervisor sweep this case's
        -- queue only, not every fixture queue on the shared database.
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId, configListenQueues = Just [queueName]}
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            -- Pinned to the app's version: an unversioned row is only
            -- claimed by the latest registered version, and this shared
            -- database always has a newer nameless row.
            let options = (enqueueOptionsNew queueName) {appVersion = Just appVersion}
            enqueued <- clientEnqueueWorkflowWith client "double" options (Just (encodeWorkflowValue (21 :: Int)))
            case enqueued of
              Left err -> fail (show err)
              Right handle -> do
                _ <- dequeueWorkflows dbos
                result <- resultWf handle
                case result of
                  Right (Just stored) -> do
                    let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                    assertEqual "the app ran the client's enqueue" (Right 42) decoded
                  other -> fail (show other),
      testCase "an enqueue no queue could honour is refused before any write" $ do
        let both = (enqueueNew "q") {deduplicationId = Just "key", partitionKey = Just "part"}
        case validateEnqueue both of
          Left err ->
            assertBool "names the queue" ("enqueue onto `q`" `Text.isInfixOf` Text.pack (show err))
          Right () -> fail "expected dedup plus partition to be refused"
        let noKey = (enqueueNew "q") {duplicationPolicy = ReturnExisting}
        case validateEnqueue noKey of
          Left _ -> pure ()
          Right () -> fail "expected return-existing without a key to be refused"
        let zero = (enqueueNew "q") {priority = Just 0}
        case validateEnqueue zero of
          Left _ -> pure ()
          Right () -> fail "expected priority zero to be refused"
        storedPriority (enqueueNew "q") @?= 0
        storedPriority ((enqueueNew "q") {priority = Just 3}) @?= 3,
      testCase "a second enqueue under a held key joins the holder" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-dup-" <> Text.take 12 suffix
            appVersion = "hs-l2-client-dup-version-" <> suffix
            executorId = "hs-l2-client-dup-executor-" <> suffix
            queueName = "hs-l2-client-dup-q-" <> Text.take 12 suffix
            key = newWorkflowKey "double"
            shape = (enqueueNew queueName) {deduplicationId = Just ("dup-" <> suffix), duplicationPolicy = ReturnExisting}
            options = (enqueueOptionsOn shape) {appVersion = Just appVersion}
        config0 <- configFromEnv appName
        -- The driven dequeue and the background supervisor sweep this case's
        -- queue only, not every fixture queue on the shared database.
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId, configListenQueues = Just [queueName]}
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            first <- clientEnqueueWorkflowWith client "double" options (Just (encodeWorkflowValue (21 :: Int)))
            second <- clientEnqueueWorkflowWith client "double" options (Just (encodeWorkflowValue (99 :: Int)))
            case (first, second) of
              (Right holder, Right joiner) -> do
                joiner.workflowId @?= holder.workflowId
                _ <- dequeueWorkflows dbos
                result <- resultWf joiner
                case result of
                  Right (Just stored) -> do
                    let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                    assertEqual "the joiner waits on the holder's run" (Right 42) decoded
                  other -> fail (show other)
              (Left err, _) -> fail ("the first enqueue failed: " <> show err)
              (_, Left err) -> fail ("the joining enqueue failed: " <> show err),
      testCase "a client retrieves by id and reads status" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-ret-" <> Text.take 12 suffix
            appVersion = "hs-l2-client-ret-version-" <> suffix
            executorId = "hs-l2-client-ret-executor-" <> suffix
            workflowText = "hs-l2-client-ret-id-" <> suffix
            key = newWorkflowKey "double"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            let handle = clientRetrieveWorkflow client workflowText
            handle.workflowId @?= workflowText
            status <- clientWorkflowStatus client (WorkflowId workflowText)
            case status of
              Right (Just _) -> pure ()
              other -> fail (show other),
      testCase "an enqueue writes a row nothing here could run" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            queueName = "hs-l2-client-unrunnable-q-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-unrunnable-id-" <> suffix
        clientConfig0 <- clientConfigFromEnv
        bracket (connectOrFail clientConfig0) closeClient $ \client -> do
          enqueued <-
            clientEnqueueWorkflowWith
              client
              "no-such-function"
              (enqueueOptionsNew queueName) {workflowId = Just workflowText}
              Nothing
          case enqueued of
            Left err -> fail (show err)
            Right handle -> do
              handle.workflowId @?= workflowText
              status <- clientWorkflowStatus client (WorkflowId workflowText)
              case status of
                Right (Just _) -> pure ()
                other -> fail ("expected the unrunnable row to exist, got: " <> show other),
      testCase "every option reaches the row" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            queueName = "hs-l2-client-opts-q-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-opts-id-" <> suffix
            shape = (enqueueNew queueName) {deduplicationId = Just ("opts-" <> suffix), priority = Just 3, delay = Just (secondsDuration 60)}
            options =
              (enqueueOptionsOn shape)
                { workflowId = Just workflowText,
                  className = Just "cls",
                  configName = Just "cfg",
                  appVersion = Just "v1",
                  timeout = Just (secondsDuration 30),
                  attributes = Just (Map.fromList [("k", String "v")])
                }
        clientConfig0 <- clientConfigFromEnv
        bracket (connectOrFail clientConfig0) closeClient $ \client -> do
          enqueued <- clientEnqueueWorkflowWith client "double" options Nothing
          case enqueued of
            Left err -> fail (show err)
            Right _ -> pure ()
          row <- readRow getBackend (WorkflowId workflowText)
          case row of
            WorkflowRecord {workflowRecordName = name, workflowRecordQueueName = queue, workflowRecordPriority = priority, workflowRecordDeduplicationId = dedup, workflowRecordApplicationVersion = version, workflowRecordTimeout = budget, workflowRecordAttributes = attributes, workflowRecordStatus = status} -> do
              name @?= Just "double"
              queue @?= Just queueName
              priority @?= 3
              dedup @?= Just ("opts-" <> suffix)
              version @?= Just "v1"
              budget @?= Just (secondsDuration 30)
              assertBool "attributes are stored" (attributes /= Nothing)
              status @?= Delayed,
      testCase "a nameless client writes unclaimed rows" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            queueName = "hs-l2-client-nameless-q-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-nameless-id-" <> suffix
        clientConfig0 <- clientConfigFromEnv
        bracket (connectOrFail clientConfig0) closeClient $ \client -> do
          enqueued <- clientEnqueueWorkflowWith client "double" (enqueueOptionsNew queueName) {workflowId = Just workflowText} Nothing
          case enqueued of
            Left err -> fail (show err)
            Right _ -> pure ()
          row <- readRow getBackend (WorkflowId workflowText)
          case row of
            WorkflowRecord {workflowRecordApplicationName = application} ->
              application @?= Nothing,
      testCase "the workflow id is an idempotency key" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            queueName = "hs-l2-client-idkey-q-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-idkey-id-" <> suffix
        clientConfig0 <- clientConfigFromEnv
        bracket (connectOrFail clientConfig0) closeClient $ \client -> do
          first <- clientEnqueueWorkflowWith client "double" (enqueueOptionsNew queueName) {workflowId = Just workflowText} Nothing
          second <- clientEnqueueWorkflowWith client "double" (enqueueOptionsNew queueName) {workflowId = Just workflowText} Nothing
          case (first, second) of
            (Right holder, Right joiner) -> joiner.workflowId @?= holder.workflowId
            (Left err, _) -> fail ("the first enqueue failed: " <> show err)
            (_, Left err) -> fail ("the joining enqueue failed: " <> show err),
      testCase "closing is clean and the pool stays usable" $ do
        clientConfig0 <- clientConfigFromEnv
        client <- connectOrFail clientConfig0
        -- hasql-pool stays usable after release: close frees the connections
        -- without invalidating the client, so a later call reconnects.
        closeClient client
        closeClient client
        enqueued <- clientEnqueueWorkflowWith client "double" (enqueueOptionsNew "hs-l2-client-closed-q") Nothing
        case enqueued of
          Left err -> fail ("expected the pool to reconnect, got: " <> show err)
          Right _ -> pure (),
      testCase "connecting to an unknown schema is refused" $ do
        clientConfig0 <- clientConfigFromEnv
        let clientConfig = (clientConfig0 :: ClientConfig) {schema = "nosuchschema"}
        connected <- connectClient clientConfig
        case connected of
          Left _ -> pure ()
          Right client -> closeClient client >> fail "expected an unknown schema to be refused",
      testCase "a message waits for its destination" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-msg-" <> Text.take 12 suffix
            appVersion = "hs-l2-client-msg-version-" <> suffix
            executorId = "hs-l2-client-msg-executor-" <> suffix
            queueName = "hs-l2-client-msg-q-" <> Text.take 12 suffix
            key = newWorkflowKey "receiver"
            body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
            body () wctx = do
              received <- recv wctx (Just (Topic "ping")) (millisDuration 5000)
              case received of
                Left err -> pure (Left err)
                Right Nothing -> pure (Left (StepFailed "recv" "nothing arrived"))
                Right (Just value) -> pure (Right value)
        config0 <- configFromEnv appName
        -- The driven dequeue and the background supervisor sweep this case's
        -- queue only, not every fixture queue on the shared database.
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId, configListenQueues = Just [queueName]}
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          -- The queue needs its table row before anything can dequeue
          -- from it; an enqueue alone only names it on the workflow row.
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            let options = (enqueueOptionsNew queueName) {appVersion = Just appVersion}
            enqueued <- clientEnqueueWorkflowWith client "receiver" options Nothing
            handle <- case enqueued of
              Left err -> fail (show err)
              Right handle -> pure handle
            let enqueuedId = handle.workflowId
            sent <- clientSendMessage client (WorkflowId enqueuedId) (Just (Topic "ping")) Nothing (encodeWorkflowValue ("hello" :: Text))
            sent @?= Right ()
            _ <- dequeueWorkflows dbos
            result <- resultWf handle
            case result of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Text
                assertEqual "the outside send reaches the waiter" (Right "hello") decoded
              other -> fail (show other),
      testCase "a batch of messages lands together" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-batch-" <> Text.take 12 suffix
            appVersion = "hs-l2-client-batch-version-" <> suffix
            executorId = "hs-l2-client-batch-executor-" <> suffix
            queueName = "hs-l2-client-batch-q-" <> Text.take 12 suffix
            key = newWorkflowKey "batcher"
            body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
            body () wctx = do
              first <- recv wctx (Just (Topic "ping")) (millisDuration 5000)
              second <- recv wctx (Just (Topic "ping")) (millisDuration 5000)
              pure $ case (first, second) of
                -- Arrival order is not promised: one batch insert stamps
                -- every row with the same millisecond, so the oldest-first
                -- consume breaks ties arbitrarily. Assert the set that
                -- landed, as the oracle does (it counts the batch, never
                -- orders it).
                (Right (Just one), Right (Just two))
                  | one <= two -> Right (one <> "+" <> two)
                  | otherwise -> Right (two <> "+" <> one)
                (Left err, _) -> Left err
                (_, Left err) -> Left err
                _ -> Left (StepFailed "recv" "a message never arrived")
        config0 <- configFromEnv appName
        -- The driven dequeue and the background supervisor sweep this case's
        -- queue only, not every fixture queue on the shared database.
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId, configListenQueues = Just [queueName]}
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            let options = (enqueueOptionsNew queueName) {appVersion = Just appVersion}
            enqueued <- clientEnqueueWorkflowWith client "batcher" options Nothing
            batch <- case enqueued of
              Left err -> fail (show err)
              Right handle -> pure handle
            let enqueuedId = WorkflowId batch.workflowId
            sent <-
              clientSendMessages
                client
                [ SendMessage enqueuedId (encodeWorkflowValue ("one" :: Text)) (Just (Topic "ping")) Nothing,
                  SendMessage enqueuedId (encodeWorkflowValue ("two" :: Text)) (Just (Topic "ping")) Nothing
                ]
            sent @?= Right ()
            _ <- dequeueWorkflows dbos
            result <- resultWf batch
            case result of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Text
                assertEqual "the batch lands together" (Right "one+two") decoded
              other -> fail (show other),
      testCase "a client send joins the caller's transaction atomically" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-tx-" <> Text.take 12 suffix
            appVersion = "hs-l2-client-tx-version-" <> suffix
            executorId = "hs-l2-client-tx-executor-" <> suffix
            queueName = "hs-l2-client-tx-q-" <> Text.take 12 suffix
            table = "client_tx_probe_" <> Text.filter (/= '-') (Text.take 12 suffix)
            topic = Topic ("client-tx-" <> Text.take 12 suffix)
            key = newWorkflowKey "txreceiver"
            body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
            body () wctx = do
              found <- recv wctx (Just topic) (millisDuration 5000)
              pure $ case found of
                Right (Just value) -> Right value
                Right Nothing -> Left (StepFailed "recv" "a message never arrived")
                Left err -> Left err
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId, configListenQueues = Just [queueName]}
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            let options = (enqueueOptionsNew queueName) {appVersion = Just appVersion}
            enqueued <- clientEnqueueWorkflowWith client "txreceiver" options Nothing
            receiver <- case enqueued of
              Left err -> fail (show err)
              Right handle -> pure handle
            backend <- getBackend
            bracket
              (Postgres.runSession backend "client-tx-ddl" (Session.statement () (createProbeTable table)) >>= either (fail . show) pure)
              (\_ -> Postgres.runSession backend "client-tx-ddl" (Session.statement () (dropProbeTable table)) >> pure ())
              (\_ -> do
                fallback <- Text.pack . UUID.toString <$> UUID.V4.nextRandom
                composed <- clientSendMessagesInTx client "probe-commit" (do _ <- Tx.statement "row-1" (insertProbe table); Tx.statement "row-2" (insertProbe table)) [SendMessage (WorkflowId receiver.workflowId) (encodeWorkflowValue ("hello" :: Text)) (Just topic) Nothing] (Just "rust_serde") [fallback] False
                composed @?= Right ()
                counted <- Postgres.runSession backend "client-tx-probe" (Session.statement () (countProbe table))
                counted @?= Right 2
                _ <- dequeueWorkflows dbos
                result <- resultWf receiver
                case result of
                  Right (Just stored) -> do
                    let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Text
                    assertEqual "the committed send reaches the waiter" (Right "hello") decoded
                  other -> fail (show other)),
      testCase "a failing caller write aborts before any send lands" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-txrb-" <> Text.take 12 suffix
            table = "client_tx_probe_" <> Text.filter (/= '-') (Text.take 12 suffix)
            topicName = "client-txrb-" <> Text.take 12 suffix
            missingTable = "client_tx_missing_" <> Text.take 12 suffix
        clientConfig0 <- clientConfigFromEnv
        let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
        bracket (connectOrFail clientConfig) closeClient $ \client -> do
          backend <- getBackend
          bracket
            (Postgres.runSession backend "client-tx-ddl" (Session.statement () (createProbeTable table)) >>= either (fail . show) pure)
            (\_ -> Postgres.runSession backend "client-tx-ddl" (Session.statement () (dropProbeTable table)) >> pure ())
            (\_ -> do
              aborted <- clientSendMessagesInTx client "probe-rollback" (Tx.statement () (Statement.preparable ("SELECT * FROM " <> missingTable) Encoders.noParams Decoders.noResult)) [] Nothing [] False
              case aborted of
                Left _ -> pure ()
                Right _ -> fail "expected the failing statement to abort the transaction"
              counted <- Postgres.runSession backend "client-tx-probe" (Session.statement () (countProbe table))
              counted @?= Right 0
              notified <- Postgres.runSession backend "client-tx-probe" (Session.statement topicName countTopic)
              notified @?= Right 0),
      testCase "a duplicate key in one batch is refused before anything runs" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-txkey-" <> Text.take 12 suffix
            table = "client_tx_probe_" <> Text.filter (/= '-') (Text.take 12 suffix)
            topic = Topic ("client-txkey-" <> Text.take 12 suffix)
        clientConfig0 <- clientConfigFromEnv
        let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
        bracket (connectOrFail clientConfig) closeClient $ \client -> do
          backend <- getBackend
          bracket
            (Postgres.runSession backend "client-tx-ddl" (Session.statement () (createProbeTable table)) >>= either (fail . show) pure)
            (\_ -> Postgres.runSession backend "client-tx-ddl" (Session.statement () (dropProbeTable table)) >> pure ())
            (\_ -> do
              fallback <- Text.pack . UUID.toString <$> UUID.V4.nextRandom
              let keyed destination = SendMessage destination (encodeWorkflowValue ("hello" :: Text)) (Just topic) (Just (IdempotencyKey "dup-key"))
              refused <- clientSendMessagesInTx client "probe-validation" (Tx.statement "row-1" (insertProbe table)) [keyed (WorkflowId ("client-tx-a-" <> suffix)), keyed (WorkflowId ("client-tx-b-" <> suffix))] (Just "rust_serde") [fallback, fallback] False
              refused @?= Left (SystemDatabase (SystemDBError.InvalidInput {field = "idempotency_key", detail = "dup-key is used by more than one message"}))
              counted <- Postgres.runSession backend "client-tx-probe" (Session.statement () (countProbe table))
              counted @?= Right 0),
      testCase "a send to a missing workflow aborts the caller's writes" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-txfk-" <> Text.take 12 suffix
            table = "client_tx_probe_" <> Text.filter (/= '-') (Text.take 12 suffix)
            topic = Topic ("client-txfk-" <> Text.take 12 suffix)
        clientConfig0 <- clientConfigFromEnv
        let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
        bracket (connectOrFail clientConfig) closeClient $ \client -> do
          backend <- getBackend
          bracket
            (Postgres.runSession backend "client-tx-ddl" (Session.statement () (createProbeTable table)) >>= either (fail . show) pure)
            (\_ -> Postgres.runSession backend "client-tx-ddl" (Session.statement () (dropProbeTable table)) >> pure ())
            (\_ -> do
              fallback <- Text.pack . UUID.toString <$> UUID.V4.nextRandom
              aborted <- clientSendMessagesInTx client "probe-fk" (Tx.statement "row-1" (insertProbe table)) [SendMessage (WorkflowId ("client-tx-nowhere-" <> suffix)) (encodeWorkflowValue ("hello" :: Text)) (Just topic) Nothing] (Just "rust_serde") [fallback] False
              case aborted of
                Left _ -> pure ()
                Right _ -> fail "expected the foreign key to abort the transaction"
              counted <- Postgres.runSession backend "client-tx-probe" (Session.statement () (countProbe table))
              counted @?= Right 0),
      testCase "a keyed send fans out inside the caller's transaction" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-txfork-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-txfork-id-" <> suffix
            table = "client_tx_probe_" <> Text.filter (/= '-') (Text.take 12 suffix)
            topicName = "client-txfork-" <> Text.take 12 suffix
            topic = Topic topicName
            key = newWorkflowKey "forked"
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "noop" (const (pure (value * 2)))
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            forked <- clientForkWorkflows client [forkNew workflowText] defaultForkOptions
            case forked of
              Left err -> fail (show err)
              Right [forkedId] -> assertBool "the fork restarts under a new id" (forkedId /= WorkflowId workflowText)
              other -> fail ("expected exactly one fork, got: " <> show other)
            backend <- getBackend
            bracket
              (Postgres.runSession backend "client-tx-ddl" (Session.statement () (createProbeTable table)) >>= either (fail . show) pure)
              (\_ -> Postgres.runSession backend "client-tx-ddl" (Session.statement () (dropProbeTable table)) >> pure ())
              (\_ -> do
                fallback <- Text.pack . UUID.toString <$> UUID.V4.nextRandom
                composed <- clientSendMessageInTx client "probe-forks" (Tx.statement "row-1" (insertProbe table)) (SendMessage (WorkflowId workflowText) (encodeWorkflowValue ("hello" :: Text)) (Just topic) (Just (IdempotencyKey "fork-key"))) (Just "rust_serde") fallback True
                composed @?= Right ()
                notified <- Postgres.runSession backend "client-tx-probe" (Session.statement topicName countTopic)
                notified @?= Right 2),
      testCase "a client rewinds a finished workflow" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-rewind-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-rewind-id-" <> suffix
            key = newWorkflowKey "redoubled"
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "double" (const (pure (value * 2)))
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            rewound <- clientRewindWorkflows client (WorkflowId workflowText) (defaultRewindOptions {rewindStartStep = 0})
            case rewound of
              Right handle -> handle.workflowId @?= workflowText
              Left err -> fail (show err)
            backend <- getBackend
            found <- SystemDB.getWorkflow backend (WorkflowId workflowText)
            case found of
              Right (Just record) -> record.workflowRecordStatus @?= Enqueued
              other -> fail (show other),
      testCase "a client rewind keeps datasource checkpoints" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-rewind-ds-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-rewind-ds-id-" <> suffix
            key = newWorkflowKey "plain"
            body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
            body () _ = pure (Right ())
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          fake <- mkFakeDs
          registeredDs <- registerDataSource dbos fake.fakeSource
          case registeredDs of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowValue ()))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          -- A checkpoint written outside any run: the client rewind must
          -- leave datasource state alone, so rewound transactions are not
          -- re-executed through this path.
          wrote <- fake.fakeSource.dsRecordOutput (DSTx.Tx (\_ _ -> error "client rewind probe")) (WorkflowId workflowText) "probe" 0 "x"
          wrote @?= True
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            rewound <- clientRewindWorkflows client (WorkflowId workflowText) (defaultRewindOptions {rewindStartStep = 0})
            case rewound of
              Right handle -> handle.workflowId @?= workflowText
              Left err -> fail (show err)
            remaining <- Map.size <$> readTVarIO fake.fakeRows
            remaining @?= 1
            backend <- getBackend
            found <- SystemDB.getWorkflow backend (WorkflowId workflowText)
            case found of
              Right (Just record) -> record.workflowRecordStatus @?= Enqueued
              other -> fail (show other),
      testCase "a client reads a workflow's events" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-events-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-events-id-" <> suffix
            key = newWorkflowKey "greeter"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = do
              published <- setEvent wctx "greeting" ("hello" :: Text)
              case published of
                Left err -> pure (Left err)
                Right () -> runStep wctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            found <- clientGetEvent client (WorkflowId workflowText) "greeting" (millisDuration 100)
            case found of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "event" (Just stored) :: Either CodecError Text
                assertEqual "the client reads the published event" (Right "hello") decoded
              other -> fail (show other),
      testCase "a client cancels and deletes a workflow it did not start" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-cancel-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-cancel-id-" <> suffix
            key = newWorkflowKey "double"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            cancelled <- clientCancelWorkflows client [WorkflowId workflowText] False
            -- The run already finished: cancel moves nothing, overwrites
            -- nothing, and names nothing.
            cancelled @?= Right []
            deleted <- clientDeleteWorkflows client [WorkflowId workflowText] True
            deleted @?= Right 1,
      testCase "a client cancels and resumes a workflow it did not start" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-resume-" <> Text.take 12 suffix
            queueName = "hs-l2-client-resume-q-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-resume-id-" <> suffix
            key = newWorkflowKey "double"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            enqueued <- clientEnqueueWorkflowWith client "double" (enqueueOptionsNew queueName) {workflowId = Just workflowText} Nothing
            case enqueued of
              Left err -> fail (show err)
              Right _ -> pure ()
            cancelled <- clientCancelWorkflows client [WorkflowId workflowText] False
            cancelled @?= Right [WorkflowId workflowText]
            resumed <- clientResumeWorkflows client [WorkflowId workflowText] Nothing
            resumed @?= Right [WorkflowId workflowText],
      testCase "a client forks a workflow the application then runs" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-fork-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-fork-id-" <> suffix
            key = newWorkflowKey "double"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            forked <- clientForkWorkflows client [forkNew workflowText] defaultForkOptions
            case forked of
              Left err -> fail (show err)
              Right [forkedId] -> assertBool "the fork restarts under a new id" (forkedId /= WorkflowId workflowText)
              other -> fail ("expected exactly one fork, got: " <> show other),
      testCase "a client reads and promotes application versions" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-versions-" <> Text.take 12 suffix
            versionA = "hs-l2-client-version-a-" <> suffix
            versionB = "hs-l2-client-version-b-" <> suffix
        config0 <- configFromEnv appName
        let configA = config0 {configAppVersion = Just versionA, configExecutorId = Just ("exec-a-" <> suffix)}
            configB = config0 {configAppVersion = Just versionB, configExecutorId = Just ("exec-b-" <> suffix)}
        clientConfig0 <- clientConfigFromEnv
        let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
        bracket (connectOrFail clientConfig) closeClient $ \client -> do
          bracket (newDBOS configA) shutdown $ \dbosA -> do
            exec <- launchClientExec dbosA isolatedEnvironment
            listed <- clientListApplicationVersions client
            case listed of
              Left err -> fail (show err)
              Right versions -> do
                assertBool "the launched version is listed" (versionA `elem` [name | VersionInfo {versionInfoName = name} <- versions])
                -- Latest is judged within this application's own rows: the
                -- shared database holds other applications' and unclaimed
                -- rows (including far-future stamps), so a global latest
                -- names none of this test's business.
                assertAppLatest appName versionA versions
            bracket (newDBOS configB) shutdown $ \dbosB -> do
              exec <- launchClientExec dbosB isolatedEnvironment
              -- Settle past DB-vs-app clock skew before promoting: launches
              -- stamp versions with the database clock while promotion
              -- stamps with the application clock, and the two have been
              -- observed ~25ms apart (Docker VM drift), which a back-to-back
              -- promote can lose by. 100ms keeps the assertion about
              -- promotion, not about clock agreement.
              threadDelay 100000
              promoted <- clientPromoteVersion client versionA
              case promoted of
                Left err -> fail (show err)
                Right () -> pure ()
              rolled <- clientListApplicationVersions client
              case rolled of
                Left err -> fail (show err)
                Right versions -> assertAppLatest appName versionA versions,
      testCase "a client reads the latest version of its own application" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appA = "hs-l2-client-latest-a-" <> Text.take 12 suffix
            appB = "hs-l2-client-latest-b-" <> Text.take 12 suffix
            versionA = "hs-l2-client-latest-va-" <> suffix
            versionB = "hs-l2-client-latest-vb-" <> suffix
        config0 <- configFromEnv appA
        let configA = config0 {configAppVersion = Just versionA, configExecutorId = Just ("exec-latest-a-" <> suffix)}
        configB0 <- configFromEnv appB
        let configB = configB0 {configAppVersion = Just versionB, configExecutorId = Just ("exec-latest-b-" <> suffix)}
        clientConfig0 <- clientConfigFromEnv
        let clientConfigA = (clientConfig0 :: ClientConfig) {appName = Just appA}
            clientConfigB = (clientConfig0 :: ClientConfig) {appName = Just appB}
        bracket (newDBOS configA) shutdown $ \dbosA ->
          bracket (newDBOS configB) shutdown $ \dbosB -> do
            _ <- launchClientExec dbosA isolatedEnvironment
            -- B stamps later, so a global read would name it: the scoped
            -- read must still name each application's own version.
            threadDelay 100000
            _ <- launchClientExec dbosB isolatedEnvironment
            bracket (connectOrFail clientConfigA) closeClient $ \clientA -> do
              latestA <- clientLatestApplicationVersion clientA
              case latestA of
                Right (Just (VersionInfo {versionInfoName})) -> versionInfoName @?= versionA
                other -> fail ("expected A's own latest " <> Text.unpack versionA <> ", got: " <> show other)
            bracket (connectOrFail clientConfigB) closeClient $ \clientB -> do
              latestB <- clientLatestApplicationVersion clientB
              case latestB of
                Right (Just (VersionInfo {versionInfoName})) -> versionInfoName @?= versionB
                other -> fail ("expected B's own latest " <> Text.unpack versionB <> ", got: " <> show other),
      testCase "a client lists its own application's queues" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appA = "hs-l2-client-queues-a-" <> Text.take 12 suffix
            appB = "hs-l2-client-queues-b-" <> Text.take 12 suffix
            queueA = "hs-l2-client-qa-" <> suffix
            queueB = "hs-l2-client-qb-" <> suffix
        config0 <- configFromEnv appA
        let configA = config0 {configAppVersion = Just ("vqa-" <> suffix), configExecutorId = Just ("exec-qa-" <> suffix)}
        configB0 <- configFromEnv appB
        let configB = configB0 {configAppVersion = Just ("vqb-" <> suffix), configExecutorId = Just ("exec-qb-" <> suffix)}
        clientConfig0 <- clientConfigFromEnv
        let clientConfigA = (clientConfig0 :: ClientConfig) {appName = Just appA}
            clientConfigB = (clientConfig0 :: ClientConfig) {appName = Just appB}
        bracket (newDBOS configA) shutdown $ \dbosA ->
          bracket (newDBOS configB) shutdown $ \dbosB -> do
            _ <- launchClientExec dbosA isolatedEnvironment
            _ <- launchClientExec dbosB isolatedEnvironment
            regA <- registerQueue dbosA queueA defaultQueueOptions AlwaysUpdate
            case regA of
              Left err -> fail (show err)
              Right _ -> pure ()
            regB <- registerQueue dbosB queueB defaultQueueOptions AlwaysUpdate
            case regB of
              Left err -> fail (show err)
              Right _ -> pure ()
            bracket (connectOrFail clientConfigA) closeClient $ \clientA -> do
              listedA <- clientListQueues clientA
              case listedA of
                Right queues ->
                  let names = [name | Queue {name} <- queues]
                   in do
                        assertBool ("expected A's queue listed, got: " <> show names) (queueA `elem` names)
                        assertBool ("expected B's queue hidden, got: " <> show names) (queueB `notElem` names)
                Left err -> fail (show err)
            bracket (connectOrFail clientConfigB) closeClient $ \clientB -> do
              listedB <- clientListQueues clientB
              case listedB of
                Right queues ->
                  let names = [name | Queue {name} <- queues]
                   in do
                        assertBool ("expected B's queue listed, got: " <> show names) (queueB `elem` names)
                        assertBool ("expected A's queue hidden, got: " <> show names) (queueA `notElem` names)
                Left err -> fail (show err),
      testCase "a client lists workflows" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-list-" <> Text.take 12 suffix
            firstText = "hs-l2-client-list-1-" <> suffix
            secondText = "hs-l2-client-list-2-" <> suffix
            key = newWorkflowKey "double"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "double" (const (pure (value * 2)))
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          _ <- runWf exec key (WorkflowId firstText) (Just (encodeWorkflowValue (1 :: Int)))
          _ <- runWf exec key (WorkflowId secondText) (Just (encodeWorkflowValue (2 :: Int)))
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            listed <- clientListWorkflows client defaultWorkflowFilter
            case listed of
              Left err -> fail (show err)
              Right records -> do
                let ids = [wid | WorkflowRecord {workflowRecordId = wid} <- records]
                assertBool "both workflows are listed" (all (`elem` ids) [WorkflowId firstText, WorkflowId secondText]),
      testCase "a client enqueues without options and the row is unversioned" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-plain-" <> Text.take 12 suffix
            queueName = "hs-l2-client-plain-q-" <> Text.take 12 suffix
            key = newWorkflowKey "double"
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "double" (const (pure (value * 2)))
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            enqueued <- clientEnqueueWorkflow client "double" queueName (Just (encodeWorkflowValue (21 :: Int)))
            handle <- case enqueued of
              Left err -> fail (show err)
              Right handle -> pure handle
            -- No options names no version: the row is claimed only by the
            -- latest registered version.
            row <- readRow getBackend (WorkflowId handle.workflowId)
            row.workflowRecordApplicationVersion @?= Nothing,
      testCase "a client cancelling a missing workflow gets nothing back" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-cancel-missing-" <> Text.take 12 suffix
        clientConfig0 <- clientConfigFromEnv
        let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
        bracket (connectOrFail clientConfig) closeClient $ \client -> do
          cancelled <- clientCancelWorkflows client [WorkflowId "never-existed"] False
          cancelled @?= Right [],
      testCase "a client getEvent with no sender waits out its timeout" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-evt-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-evt-id-" <> suffix
            key = newWorkflowKey "quiet"
            body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
            body () _ = pure (Right ())
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          _ <- runWf exec key (WorkflowId workflowText) Nothing
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            waited <- clientGetEvent client (WorkflowId workflowText) "absent" (millisDuration 200)
            waited @?= Right Nothing,
      testCase "a client registers, reads, updates, and deletes a queue" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-qcrud-" <> Text.take 12 suffix
            queueName = "hs-l2-client-qcrud-q-" <> Text.take 12 suffix
        clientConfig0 <- clientConfigFromEnv
        let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
        bracket (connectOrFail clientConfig) closeClient $ \client -> do
          registered <- clientRegisterQueue client queueName (defaultQueueOptions {globalConcurrency = Just 3}) AlwaysUpdate
          receipt <- case registered of
            Left err -> fail (show err)
            Right receipt -> pure receipt
          receipt.concurrency @?= Just 3
          found <- clientRetrieveQueue client queueName
          found @?= Right (Just receipt)
          -- NeverUpdate leaves the stored limits standing.
          leftAlone <- clientRegisterQueue client queueName (defaultQueueOptions {globalConcurrency = Just 9}) NeverUpdate
          case leftAlone of
            Left err -> fail (show err)
            Right kept -> kept.concurrency @?= Just 3
          updated <- clientUpdateQueue client queueName (defaultQueueChange {workerConcurrency = Set (Just 2)})
          case updated of
            Left err -> fail (show err)
            Right receipt -> receipt.workerConcurrency @?= Just 2
          deleted <- clientDeleteQueue client queueName
          deleted @?= Right ()
          gone <- clientRetrieveQueue client queueName
          gone @?= Right Nothing,
      testCase "a client is refused the version-guarded conflict" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-qconf-" <> Text.take 12 suffix
            queueName = "hs-l2-client-qconf-q-" <> Text.take 12 suffix
        clientConfig0 <- clientConfigFromEnv
        let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
        bracket (connectOrFail clientConfig) closeClient $ \client -> do
          refused <- clientRegisterQueue client queueName defaultQueueOptions UpdateIfLatestVersion
          case refused of
            Left (ErrorConfig message)
              | "application version" `Text.isInfixOf` message -> pure ()
              | otherwise -> fail ("the conflict refusal names no version, got: " <> Text.unpack message)
            other -> fail ("expected the version-guarded conflict refused, got: " <> show other),
      testCase "a client cannot take the internal queue name" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-qres-" <> Text.take 12 suffix
        clientConfig0 <- clientConfigFromEnv
        let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
        bracket (connectOrFail clientConfig) closeClient $ \client -> do
          refused <- clientRegisterQueue client "_dbos_internal_queue" defaultQueueOptions AlwaysUpdate
          case refused of
            Left (ErrorConfig message)
              | "reserved" `Text.isInfixOf` message -> pure ()
              | otherwise -> fail ("the reserved refusal names no reservation, got: " <> Text.unpack message)
            other -> fail ("expected the reserved name refused, got: " <> show other),
      testCase "a client waits for the first of its handles" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-wait-" <> Text.take 12 suffix
            firstText = "hs-l2-client-wait-1-" <> suffix
            secondText = "hs-l2-client-wait-2-" <> suffix
            key = newWorkflowKey "double"
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "double" (const (pure (value * 2)))
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          _ <- runWf exec key (WorkflowId firstText) (Just (encodeWorkflowValue (1 :: Int)))
          _ <- runWf exec key (WorkflowId secondText) (Just (encodeWorkflowValue (2 :: Int)))
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            let first = clientRetrieveWorkflow client firstText
                second = clientRetrieveWorkflow client secondText
            won <- clientWaitFirst client [first, second]
            case won of
              Left err -> fail (show err)
              Right winner ->
                assertBool ("expected a settled handle back, got: " <> Text.unpack winner.workflowId) (winner.workflowId `elem` [firstText, secondText]),
      testCase "a client refuses an empty or duplicated wait" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-waitneg-" <> Text.take 12 suffix
        clientConfig0 <- clientConfigFromEnv
        let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
        bracket (connectOrFail clientConfig) closeClient $ \client -> do
          empty <- clientWaitFirst client []
          case empty of
            Left (InvalidArgument operation _)
              | operation == "wait_first" -> pure ()
              | otherwise -> fail ("the empty wait names the wrong operation, got: " <> show operation)
            other -> fail ("expected the empty wait refused, got: " <> show other)
          let probe = clientRetrieveWorkflow client "never-existed"
          duplicated <- clientWaitFirst client [probe, probe]
          case duplicated of
            Left (InvalidArgument _ detail)
              | "duplicate" `Text.isInfixOf` detail -> pure ()
              | otherwise -> fail ("the duplicated wait names no duplication, got: " <> Text.unpack detail)
            other -> fail ("expected the duplicated wait refused, got: " <> show other),
      testCase "a client replaces and clears workflow attributes" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-attrs-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-attrs-id-" <> suffix
            key = newWorkflowKey "double"
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "double" (const (pure (value * 2)))
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          _ <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowValue (1 :: Int)))
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
              filters = defaultWorkflowFilter {workflowFilterWorkflowIds = [workflowText]}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            replaced <- clientUpdateWorkflowAttributes client (WorkflowId workflowText) (Just "{\"team\":\"red\"}")
            replaced @?= Right ()
            listed <- clientListWorkflows client filters
            case listed of
              Left err -> fail (show err)
              -- jsonb normalizes the spacing on write; the pin holds the
              -- canonical form, not the literal sent.
              Right [record] -> record.workflowRecordAttributes @?= Just "{\"team\": \"red\"}"
              Right records -> fail ("expected one listed row, got: " <> show (length records))
            cleared <- clientUpdateWorkflowAttributes client (WorkflowId workflowText) Nothing
            cleared @?= Right ()
            relisted <- clientListWorkflows client filters
            case relisted of
              Left err -> fail (show err)
              Right [record] -> record.workflowRecordAttributes @?= Nothing
              Right records -> fail ("expected one relisted row, got: " <> show (length records)),
      testCase "a client moves a delayed workflow's release" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-delay-" <> Text.take 12 suffix
            queueName = "hs-l2-client-delay-q-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-delay-id-" <> suffix
            key = newWorkflowKey "double"
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "double" (const (pure (value * 2)))
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          _ <- launchClientExec dbos isolatedEnvironment
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            registeredQ <- clientRegisterQueue client queueName defaultQueueOptions AlwaysUpdate
            case registeredQ of
              Left err -> fail (show err)
              Right _ -> pure ()
            let options :: EnqueueOptions
                options = (enqueueOptionsOn ((enqueueNew queueName) {delay = Just (secondsDuration 3600)})) {workflowId = Just workflowText}
            enqueued <- clientEnqueueWorkflowWith client "double" options (Just (encodeWorkflowValue (1 :: Int)))
            case enqueued of
              Left err -> fail (show err)
              Right _ -> pure ()
            moved <- clientSetWorkflowDelay client (WorkflowId workflowText) (DelayFor (secondsDuration 1))
            moved @?= Right ()
            row <- readRow getBackend (WorkflowId workflowText)
            case row.workflowRecordDelayUntil of
              Just _ -> pure ()
              Nothing -> fail "expected the moved release stamped on the row",
      testCase "a client enqueue joins the caller's transaction atomically" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-enqtx-" <> Text.take 12 suffix
            queueName = "hs-l2-client-enqtx-q-" <> Text.take 12 suffix
            table = "client_enqtx_probe_" <> Text.filter (/= '-') (Text.take 12 suffix)
            key = newWorkflowKey "double"
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "double" (const (pure (value * 2)))
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            backend <- getBackend
            bracket
              (Postgres.runSession backend "client-enqtx-ddl" (Session.statement () (createProbeTable table)) >>= either (fail . show) pure)
              (\_ -> Postgres.runSession backend "client-enqtx-ddl" (Session.statement () (dropProbeTable table)) >> pure ())
              (\_ -> do
                enqueued <- clientEnqueueWorkflowInTx client "double" "enq-commit" (Tx.statement "row-1" (insertProbe table)) (enqueueOptionsNew queueName) (Just (encodeWorkflowValue (7 :: Int)))
                handle <- case enqueued of
                  Left err -> fail (show err)
                  Right handle -> pure handle
                counted <- Postgres.runSession backend "client-enqtx-probe" (Session.statement () (countProbe table))
                counted @?= Right 1
                row <- readRow getBackend (WorkflowId handle.workflowId)
                row.workflowRecordStatus @?= Enqueued),
      testCase "a repeated enqueue joins the existing row" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-enqtx2-" <> Text.take 12 suffix
            queueName = "hs-l2-client-enqtx2-q-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-enqtx2-id-" <> suffix
            table = "client_enqtx2_probe_" <> Text.filter (/= '-') (Text.take 12 suffix)
        clientConfig0 <- clientConfigFromEnv
        let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
            options :: EnqueueOptions
            options = (enqueueOptionsNew queueName) {workflowId = Just workflowText}
        bracket (connectOrFail clientConfig) closeClient $ \client -> do
          backend <- getBackend
          bracket
            (Postgres.runSession backend "client-enqtx-ddl" (Session.statement () (createProbeTable table)) >>= either (fail . show) pure)
            (\_ -> Postgres.runSession backend "client-enqtx-ddl" (Session.statement () (dropProbeTable table)) >> pure ())
            (\_ -> do
              first <- clientEnqueueWorkflowInTx client "double" "enq-first" (Tx.statement "row-1" (insertProbe table)) options Nothing
              case first of
                Left err -> fail (show err)
                Right _ -> pure ()
              second <- clientEnqueueWorkflowInTx client "double" "enq-repeat" (Tx.statement "row-2" (insertProbe table)) options Nothing
              case second of
                Left err -> fail (show err)
                Right _ -> pure ()
              counted <- Postgres.runSession backend "client-enqtx-probe" (Session.statement () (countProbe table))
              counted @?= Right 2),
      testCase "a conflicting enqueue condemns the caller's writes" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-enqtx3-" <> Text.take 12 suffix
            queueName = "hs-l2-client-enqtx3-q-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-enqtx3-id-" <> suffix
            table = "client_enqtx3_probe_" <> Text.filter (/= '-') (Text.take 12 suffix)
        clientConfig0 <- clientConfigFromEnv
        let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
            options :: EnqueueOptions
            options = (enqueueOptionsNew queueName) {workflowId = Just workflowText}
            rival :: EnqueueOptions
            rival = (enqueueOptionsNew queueName) {workflowId = Just workflowText}
        bracket (connectOrFail clientConfig) closeClient $ \client -> do
          backend <- getBackend
          bracket
            (Postgres.runSession backend "client-enqtx-ddl" (Session.statement () (createProbeTable table)) >>= either (fail . show) pure)
            (\_ -> Postgres.runSession backend "client-enqtx-ddl" (Session.statement () (dropProbeTable table)) >> pure ())
            (\_ -> do
              first <- clientEnqueueWorkflowInTx client "double" "enq-first" (Tx.statement "row-1" (insertProbe table)) options Nothing
              case first of
                Left err -> fail (show err)
                Right _ -> pure ()
              conflicted <- clientEnqueueWorkflowInTx client "rival" "enq-rival" (Tx.statement "row-2" (insertProbe table)) rival Nothing
              case conflicted of
                Left (SystemDatabase (SystemDBError.ConflictingWorkflow {})) -> pure ()
                other -> fail ("expected the rival enqueue condemned, got: " <> show other)
              counted <- Postgres.runSession backend "client-enqtx-probe" (Session.statement () (countProbe table))
              counted @?= Right 1),
      testCase "return-existing is refused before anything runs" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-enqtx4-" <> Text.take 12 suffix
            queueName = "hs-l2-client-enqtx4-q-" <> Text.take 12 suffix
            table = "client_enqtx4_probe_" <> Text.filter (/= '-') (Text.take 12 suffix)
        clientConfig0 <- clientConfigFromEnv
        let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
            options :: EnqueueOptions
            options = (enqueueOptionsNew queueName) {queue = (enqueueNew queueName) {duplicationPolicy = ReturnExisting, deduplicationId = Just "k"}}
        bracket (connectOrFail clientConfig) closeClient $ \client -> do
          backend <- getBackend
          bracket
            (Postgres.runSession backend "client-enqtx-ddl" (Session.statement () (createProbeTable table)) >>= either (fail . show) pure)
            (\_ -> Postgres.runSession backend "client-enqtx-ddl" (Session.statement () (dropProbeTable table)) >> pure ())
            (\_ -> do
              refused <- clientEnqueueWorkflowInTx client "double" "enq-join" (Tx.statement "row-1" (insertProbe table)) options Nothing
              case refused of
                Left (InvalidArgument operation _)
                  | operation == "enqueue in transaction" -> pure ()
                  | otherwise -> fail ("the join refusal names the wrong operation, got: " <> show operation)
                other -> fail ("expected return-existing refused, got: " <> show other)
              counted <- Postgres.runSession backend "client-enqtx-probe" (Session.statement () (countProbe table))
              counted @?= Right 0),
      testCase "a client lists queued workflows only" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-qlist-" <> Text.take 12 suffix
            queueName = "hs-l2-client-qlist-q-" <> Text.take 12 suffix
            plainText = "hs-l2-client-qlist-plain-" <> suffix
            queuedText = "hs-l2-client-qlist-queued-" <> suffix
            key = newWorkflowKey "double"
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "double" (const (pure (value * 2)))
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          _ <- runWf exec key (WorkflowId plainText) (Just (encodeWorkflowValue (1 :: Int)))
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            registeredQ <- clientRegisterQueue client queueName defaultQueueOptions AlwaysUpdate
            case registeredQ of
              Left err -> fail (show err)
              Right _ -> pure ()
            let options :: EnqueueOptions
                options = (enqueueOptionsNew queueName) {workflowId = Just queuedText}
            enqueued <- clientEnqueueWorkflowWith client "double" options (Just (encodeWorkflowValue (2 :: Int)))
            case enqueued of
              Left err -> fail (show err)
              Right handle -> handle.workflowId @?= queuedText
            queued <- clientListQueuedWorkflows client defaultWorkflowFilter
            case queued of
              Left err -> fail (show err)
              Right records -> do
                let ids = [wid | WorkflowRecord {workflowRecordId = wid} <- records]
                assertBool ("expected the queued row listed, got: " <> show ids) (WorkflowId queuedText `elem` ids)
                assertBool ("expected the plain row excluded, got: " <> show ids) (WorkflowId plainText `notElem` ids),
      testCase "a client reads a workflow's stream without checkpointing" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-stream-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-stream-id-" <> suffix
            key = newWorkflowKey "streamer"
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
            body value wctx = writeStream wctx "out" (show value)
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          _ <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowValue (5 :: Int)))
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            reader <- clientReadStream client (WorkflowId workflowText) "out" defaultReadStreamOptions :: IO (StreamReader (Error EngineOnly) IO Text)
            firstValue <- readHead reader
            -- The producer is terminal, so the read past its single value ends.
            secondValue <- readHead reader
            firstValue @?= Right (Just "5")
            secondValue @?= Right Nothing,
      testCase "a client read refuses a negative offset" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-streamneg-" <> Text.take 12 suffix
        clientConfig0 <- clientConfigFromEnv
        let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
        bracket (connectOrFail clientConfig) closeClient $ \client -> do
          reader <- clientReadStreamFrom client (WorkflowId ("hs-l2-client-streamneg-id-" <> suffix)) "out" (-1) defaultReadStreamOptions :: IO (StreamReader (Error EngineOnly) IO Text)
          refused <- readHead reader
          case refused of
            Left (InvalidArgument operation _)
              | operation == "read_stream" -> pure ()
              | otherwise -> fail ("the offset refusal names the wrong operation, got: " <> show operation)
            other -> fail ("expected the negative offset refused, got: " <> show other)
    ]

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through. Local
-- copies are deliberate: this module carries only the aliases it uses.
runWf :: Executor IO -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWf = runWorkflow

resultWf :: WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWf = handleResult

-- | Pull one value from a stream reader through its vector adapter.
readHead :: StreamReader (Error EngineOnly) IO Text -> IO (Either (Error EngineOnly) (Maybe Text))
readHead reader = case toVectorStream reader of
  Vector.Stream next initial -> do
    outcome <- runExceptT (next initial)
    pure $ case outcome of
      Right (Vector.Yield value _) -> Right (Just value)
      Right Vector.Done -> Right Nothing
      Right (Vector.Skip _) -> Right Nothing
      Left err -> Left err

-- | Connects or fails the test with the engine error rendered.
connectOrFail :: ClientConfig -> IO (Client IO)
connectOrFail config = do
  connected <- connectClient config
  case connected of
    Left err -> fail (show err)
    Right client -> pure client

-- | Scratch-table statements for transaction-atomicity probes: test-owned
-- DDL on the system database, created and dropped around each case.
createProbeTable :: Text -> Statement.Statement () ()
createProbeTable table =
  Statement.preparable
    ("CREATE TABLE " <> table <> " (v text NOT NULL)")
    Encoders.noParams
    Decoders.noResult

dropProbeTable :: Text -> Statement.Statement () ()
dropProbeTable table =
  Statement.preparable
    ("DROP TABLE " <> table)
    Encoders.noParams
    Decoders.noResult

insertProbe :: Text -> Statement.Statement Text ()
insertProbe table =
  Statement.preparable
    ("INSERT INTO " <> table <> " (v) VALUES ($1)")
    (Encoders.param (Encoders.nonNullable Encoders.text))
    Decoders.noResult

countProbe :: Text -> Statement.Statement () Int64
countProbe table =
  Statement.preparable
    ("SELECT COUNT(*) FROM " <> table)
    Encoders.noParams
    (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))

-- | Rows addressed to one topic. The suite runs the default schema, which
-- is also where the send insert writes.
countTopic :: Statement.Statement Text Int64
countTopic =
  Statement.preparable
    "SELECT COUNT(*) FROM dbos.notifications WHERE topic = $1"
    (Encoders.param (Encoders.nonNullable Encoders.text))
    (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted. The
-- clients and launched instances below keep their own pools: clients are
-- bracketed with closeClient, instances with shutdown.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | The queue-name prefix this suite's cases register under. No other live
-- suite registers under it, so the release can delete by prefix without
-- racing a running peer (tasty runs groups in parallel; names are unique
-- per case).
suiteQueuePrefix :: Text
suiteQueuePrefix = "hs-l2-client-"

-- | Delete this suite's fixture queues after the group finishes: the shared
-- database keeps a queue row per run, and every unscoped sweep pays one claim
-- query per row. Runs in the 'withResource' release, after every case, so it
-- never races a running case. Best-effort: a refusal is ignored rather than
-- failing the suite. Workflow rows are left alone; without their queue row
-- no sweep will ever enumerate them.
releaseSuiteBackend :: Postgres.PostgresSystemDB -> IO ()
releaseSuiteBackend backend = do
  listed <- SystemDB.listQueues backend Unset
  case listed of
    Left _ -> pure ()
    Right records -> mapM_ (\name -> SystemDB.deleteQueue backend name >> pure ()) names
      where
        names = [name | record <- records, let name = record.queueRecordName, Text.isPrefixOf suiteQueuePrefix name]
  Postgres.releasePostgresSystemDB backend

-- | One workflow row as stored: a reader over the suite backend.
readRow :: IO Postgres.PostgresSystemDB -> WorkflowId -> IO WorkflowRecord
readRow getBackend wid = do
  backend <- getBackend
  found <- getWorkflow backend wid
  case found of
    Right (Just record) -> pure record
    _ -> fail "expected the workflow row to exist"

-- | The named version carries the newest stamp among this application's
-- own rows. Scoped to the application because the shared database holds
-- other applications' and unclaimed rows beside it.
assertAppLatest :: Text -> Text -> [VersionInfo] -> IO ()
assertAppLatest appName versionName versions = do
  let ours =
        [ (name, stamp)
          | VersionInfo {versionInfoApplicationName = owner, versionInfoName = name, versionInfoTimestamp = stamp} <- versions,
            owner == Just appName
        ]
  case (lookup versionName ours, map snd ours) of
    (Just stamped, stamps) | not (null stamps) -> stamped @?= maximum stamps
    _ -> fail ("expected " <> Text.unpack versionName <> " among " <> show (map fst ours))

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }
