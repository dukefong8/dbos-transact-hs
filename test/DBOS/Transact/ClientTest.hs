{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Public-behavior tests for the @client.rs@ outside-in surface.
module DBOS.Transact.ClientTest (tests) where

import DBOS.Prelude
import Data.Aeson (Value (..))
import Data.Map.Strict qualified as Map
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (Applications (..), QueueRecord (..), VersionInfo (..), WorkflowId (..), WorkflowRecord (..), WorkflowStatus (..), defaultForkOptions, defaultWorkflowFilter, forkNew, getWorkflow, millisDuration, secondsDuration)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  (
    EngineOnly, Client,
    ClientConfig (..),
    CodecError,
    Config (..),
    DBOS,
    Executor,
    DuplicationPolicy (..),
    Enqueue (..),
    EnqueueOptions (..),
    Environment (..),
    Error (..),
    QueueConflict (..),
    WorkflowCtx,
    SerializedWorkflowValue (..),
    SendMessage (..),
    Topic (..),
    clientCancelWorkflows,
    clientConfigFromEnv,
    clientConfigNew,
    clientDeleteWorkflows,
    clientForkWorkflows,
    clientGetEvent,
    clientListApplicationVersions,
    clientListWorkflows,
    clientPromoteVersion,
    clientResumeWorkflows,
    clientSendMessage,
    clientSendMessages,
    clientConfigFromEnv,
    clientConfigNew,
    closeClient,
    configFromEnv,
    connectClient,
    decodeWorkflowValue,
    defaultQueueOptions,
    encodeWorkflowValue,
    enqueueClientWorkflowWith,
    enqueueClientWorkflow,
    enqueueNew,
    enqueueOptionsNew,
    enqueueOptionsOn,
    handleResult,
    launchWithEnvironment,
    newDBOS,
    newWorkflowKey,
    registerDBOSWorkflow,
    registerQueue,
    retrieveClientWorkflow,
    nullTracer,
    recv,
    runDBOSWorkflow,
    runStep,
    setEvent,
    shutdown,
    validateClientConfig,
    WorkflowKey,
    workflowStatusClient,
    WorkflowHandle (workflowId),
    recv,
    runStep,
    setEvent)
import DBOS.Transact.Instance (dequeueDBOSWorkflows)
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
          registered <- registerDBOSWorkflow dbos key body
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
            enqueued <- enqueueClientWorkflowWith client "double" options (Just (encodeWorkflowValue (21 :: Int)))
            case enqueued of
              Left err -> fail (show err)
              Right handle -> do
                _ <- dequeueDBOSWorkflows dbos
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
          registered <- registerDBOSWorkflow dbos key body
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
            first <- enqueueClientWorkflowWith client "double" options (Just (encodeWorkflowValue (21 :: Int)))
            second <- enqueueClientWorkflowWith client "double" options (Just (encodeWorkflowValue (99 :: Int)))
            case (first, second) of
              (Right holder, Right joiner) -> do
                joiner.workflowId @?= holder.workflowId
                _ <- dequeueDBOSWorkflows dbos
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
          registered <- registerDBOSWorkflow dbos key body
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
            let handle = retrieveClientWorkflow client workflowText
            handle.workflowId @?= workflowText
            status <- workflowStatusClient client (WorkflowId workflowText)
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
            enqueueClientWorkflowWith
              client
              "no-such-function"
              (enqueueOptionsNew queueName) {workflowId = Just workflowText}
              Nothing
          case enqueued of
            Left err -> fail (show err)
            Right handle -> do
              handle.workflowId @?= workflowText
              status <- workflowStatusClient client (WorkflowId workflowText)
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
          enqueued <- enqueueClientWorkflowWith client "double" options Nothing
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
          enqueued <- enqueueClientWorkflowWith client "double" (enqueueOptionsNew queueName) {workflowId = Just workflowText} Nothing
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
          first <- enqueueClientWorkflowWith client "double" (enqueueOptionsNew queueName) {workflowId = Just workflowText} Nothing
          second <- enqueueClientWorkflowWith client "double" (enqueueOptionsNew queueName) {workflowId = Just workflowText} Nothing
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
        enqueued <- enqueueClientWorkflowWith client "double" (enqueueOptionsNew "hs-l2-client-closed-q") Nothing
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
          registered <- registerDBOSWorkflow dbos key body
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
            enqueued <- enqueueClientWorkflowWith client "receiver" options Nothing
            handle <- case enqueued of
              Left err -> fail (show err)
              Right handle -> pure handle
            let enqueuedId = handle.workflowId
            sent <- clientSendMessage client (WorkflowId enqueuedId) (Just (Topic "ping")) Nothing (encodeWorkflowValue ("hello" :: Text))
            sent @?= Right ()
            _ <- dequeueDBOSWorkflows dbos
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
          registered <- registerDBOSWorkflow dbos key body
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
            enqueued <- enqueueClientWorkflowWith client "batcher" options Nothing
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
            _ <- dequeueDBOSWorkflows dbos
            result <- resultWf batch
            case result of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Text
                assertEqual "the batch lands together" (Right "one+two") decoded
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
          registered <- registerDBOSWorkflow dbos key body
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
          registered <- registerDBOSWorkflow dbos key body
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
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            enqueued <- enqueueClientWorkflowWith client "double" (enqueueOptionsNew queueName) {workflowId = Just workflowText} Nothing
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
          registered <- registerDBOSWorkflow dbos key body
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
          registered <- registerDBOSWorkflow dbos key body
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
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            enqueued <- enqueueClientWorkflow client "double" queueName (Just (encodeWorkflowValue (21 :: Int)))
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
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchClientExec dbos isolatedEnvironment
          _ <- runWf exec key (WorkflowId workflowText) Nothing
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            waited <- clientGetEvent client (WorkflowId workflowText) "absent" (millisDuration 200)
            waited @?= Right Nothing
    ]

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through. Local
-- copies are deliberate: this module carries only the aliases it uses.
runWf :: Executor IO -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWf = runDBOSWorkflow

resultWf :: WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWf = handleResult

-- | Connects or fails the test with the engine error rendered.
connectOrFail :: ClientConfig -> IO (Client IO)
connectOrFail config = do
  connected <- connectClient config
  case connected of
    Left err -> fail (show err)
    Right client -> pure client

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
