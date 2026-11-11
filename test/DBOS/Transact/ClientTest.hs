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
import Hasql.Pool qualified as Pool
import Hasql.Pool.Config qualified as PoolConfig
import Hasql.Connection.Settings qualified as ConnectionSettings
import Hasql.Session qualified as Session
import Pqi.Ffi qualified as Pqi
import Hasql.Statement qualified as Statement
import Hasql.Transaction qualified as Tx
import DBOS.SystemDB (AppScope (..), QueueRecord (..), VersionInfo (..), WorkflowFilter (..), WorkflowId (..), WorkflowIdReusePolicy (..), WorkflowRecord (..), WorkflowStatus (..), defaultForkOptions, defaultWorkflowFilter, durationFromSecs, forkNew, getWorkflow, millisDuration, secondsDuration)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact.Serialization (WorkflowInput, decodeWorkflowValue, encodeWorkflowValue, encodeWorkflowInput, CodecError (..))
import DBOS.Transact
  (
    EngineOnly, Client,
    ClientConfig (..),
    Change (..),
    Config (..),
    DBOS,
    Executor,
    DuplicationPolicy (..),
    QueueRequest (..),
    EnqueueOptions (..),
    Environment (..),
    Error (..),
    IdempotencyKey (..),
    IdleTransactionTimeout (..),
    Queue (..),
    QueueChange (..),
    QueueConflict (..),
    QueueOptions (..),
    WorkflowCtx,
    SerializedWorkflowValue (..),
    EncodedSendMessage (..),
    StreamReader,
    Topic (..),
    clientCancelWorkflows,
    clientConfigFromEnv,
    clientConfigNew,
    clientDeleteWorkflows,
    clientDeleteQueue,
    clientForkWorkflows,
    clientLatestAppVersion,
    clientListAppVersions,
    clientListQueues,
    clientListQueuedWorkflows,
    clientReadStream,
    clientReadStreamFrom,
    clientRegisterQueue,
    clientRetrieveQueue,
    clientUpdateQueue,
    clientUpdateWorkflowAttributes,
    clientMergeWorkflowAttributes,
    clientSetWorkflowDelay,
    clientWaitFirst,
    clientListWorkflows,
    clientPromoteVersion,
    clientPromoteVersionFor,
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
    DebounceOptions (..),
    DebouncerClient (..),
    debounceClient,
    debounceClientWith,
    debounceOptionsDefault,
    debouncerClientNew,
    defaultQueueChange,
    defaultQueueOptions,
    defaultReadStreamOptions,
    defaultRewindOptions,
    clientEnqueueWorkflowInTx,
    clientEnqueueWorkflowWith,
    clientEnqueueWorkflow,
    queueRequestNew,
    enqueueOptionsNew,
    handleResult,
    instanceQualifiedWorkflowName,
    launchWithEnvironment,
    methodQualifiedWorkflowName,
    newDBOS,
    newQualifiedWorkflowName,
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
    QualifiedWorkflowName,
    clientWorkflowStatus,
    clientWorkflowRecord,
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
import DBOS.Transact.Client (clientGetEventSerialized, queueRequest)
import DBOS.Transact.Workflow (Timeout (..), storedPriority, validateEnqueue, validateStart)
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
            assertBool "names the variable" ("DBOS_SYSTEM_DATABASE_URL" `Text.isInfixOf` Text.pack (show err))
          Right () -> fail "expected the empty url to be refused",
      testCase "the client's listen default matches Rust and the field stays inert" $
        (clientConfigNew "postgresql://x").useListenNotify @?= False,
      testCase "an idle timeout the server cannot take is refused" $ do
        let config timeout = (clientConfigNew "postgresql://x") {idleTransactionTimeout = timeout}
        validateClientConfig (config Default) @?= Right ()
        case validateClientConfig (config (After (millisDuration 0))) of
          Left err ->
            assertBool
              "names the field"
              ("idle_transaction_timeout" `Text.isInfixOf` Text.pack (show err))
          Right () -> fail "expected a zero After to be refused",
      testCase "a client enqueues by name and the app runs it" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-" <> Text.take 12 suffix
            appVersion = "hs-l2-client-version-" <> suffix
            executorId = "hs-l2-client-executor-" <> suffix
            queueName = "hs-l2-client-q-" <> Text.take 12 suffix
            key = newQualifiedWorkflowName "double"
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
            enqueued <- clientEnqueueWorkflowWith client (newQualifiedWorkflowName "double") options (Just (encodeWorkflowInput (21 :: Int)))
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
      testCase "a client debounce coalesces onto one delayed row" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-debounce-" <> Text.take 12 suffix
            appVersion = "hs-l2-client-debounce-version-" <> suffix
            executorId = "hs-l2-client-debounce-executor-" <> suffix
            queueName = "hs-l2-client-debounce-q-" <> Text.take 12 suffix
            key = newQualifiedWorkflowName "double"
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "double" (const (pure (value * 2)))
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
            let dc = (debouncerClientNew client key queueName) {debouncerClientAppVersion = Just appVersion}
            created <- debounceClient dc "user-7" (secondsDuration 3) (Just (encodeWorkflowInput (21 :: Int)))
            wid1 <- case created of
              Left err -> fail (show err)
              Right joined -> pure joined.workflowId
            found <- clientWorkflowRecord client (WorkflowId wid1)
            case found of
              Right (Just row) -> do
                row.workflowRecordStatus @?= Delayed
                assertBool "the row holds the key" (row.workflowRecordDeduplicationId /= Nothing)
                row.workflowRecordIsDebounced @?= True
              other -> fail ("expected the debounced row, got: " <> show other)
            coincided <- debounceClient dc "user-7" (secondsDuration 3) (Just (encodeWorkflowInput (22 :: Int)))
            wid2 <- case coincided of
              Left err -> fail (show err)
              Right joined -> pure joined.workflowId
            wid2 @?= wid1
            _ <- dequeueWorkflows dbos
            result <- resultWf (clientRetrieveWorkflow client wid1)
            case result of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "the last inputs won" (Right 44) decoded
              other -> fail (show other),
      testCase "a client debounce with a reused pinned id is a conflict" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-pinned-" <> Text.take 12 suffix
            appVersion = "hs-l2-client-pinned-version-" <> suffix
            executorId = "hs-l2-client-pinned-executor-" <> suffix
            queueName = "hs-l2-client-pinned-q-" <> Text.take 12 suffix
            key = newQualifiedWorkflowName "double"
            offered = WorkflowId ("hs-l2-client-pinned-id-" <> suffix)
            WorkflowId offeredText = offered
            options = debounceOptionsDefault {debounceOptionsWorkflowId = Just offered}
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = runStep wctx "double" (const (pure (value * 2)))
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
            let dc = (debouncerClientNew client key queueName) {debouncerClientAppVersion = Just appVersion}
            created <- debounceClientWith dc "user-9" (secondsDuration 1) (Just (encodeWorkflowInput (21 :: Int))) options
            wid1 <- case created of
              Left err -> fail (show err)
              Right joined -> pure joined.workflowId
            wid1 @?= offeredText
            _ <- dequeueWorkflows dbos
            ran <- resultWf (clientRetrieveWorkflow client wid1)
            case ran of
              Right (Just _) -> pure ()
              other -> fail ("expected the first cycle to run, got: " <> show other)
            reused <- debounceClientWith dc "user-9" (secondsDuration 1) (Just (encodeWorkflowInput (22 :: Int))) options
            case reused of
              Left (SystemDatabase (SystemDBError.ConflictingWorkflow {})) -> pure ()
              other -> fail ("expected a conflict, not a join, got: " <> show other),
      testCase "what a client debouncer cannot act on is refused" $ do
        -- Mirrors the client half of Rust @what_a_debouncer_cannot_act_on_is_refused@:
        -- a zero period, a zero budget, an empty queue and an empty app name
        -- are refused, and nothing is written.
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-debounce-refused-" <> Text.take 12 suffix
            queueName = "hs-l2-client-debounce-refused-q-" <> Text.take 12 suffix
            key = newQualifiedWorkflowName "double"
        config0 <- configFromEnv appName
        let config = config0 {configExecutorId = Just ("hs-l2-client-debounce-refused-exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \_dbos -> do
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            let dc = debouncerClientNew client key queueName
                refusedBy label action field = do
                  refused <- action
                  case refused of
                    Left (InvalidArgument _ detail) ->
                      assertBool (label <> " names " <> field) (Text.pack field `Text.isInfixOf` detail)
                    other -> fail ("expected an InvalidArgument refusal, got: " <> show other)
            refusedBy "period" (debounceClient dc "user-1" (millisDuration 0) Nothing) "period"
            refusedBy "budget" (debounceClient (dc {debouncerClientTimeout = Just (millisDuration 0)}) "user-1" (secondsDuration 1) Nothing) "timeout"
            refusedBy "queue" (debounceClient (debouncerClientNew client key "") "user-1" (secondsDuration 1) Nothing) "name"
            refusedBy "app" (debounceClient (dc {debouncerClientAppName = Just ""}) "user-1" (secondsDuration 1) Nothing) "app_name"
            queued <- clientListWorkflows client (defaultWorkflowFilter {workflowFilterQueueNames = [""]})
            case queued of
              Left err -> fail (show err)
              Right records -> records @?= [],
      testCase "a client debounce of a nameless workflow is refused" $ do
        -- Mirrors Rust @a_client_debounce_of_a_nameless_workflow_is_refused@:
        -- refused before anything bounces or writes.
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-debounce-nameless-" <> Text.take 12 suffix
            queueName = "hs-l2-client-debounce-nameless-q-" <> Text.take 12 suffix
        config0 <- configFromEnv appName
        let config = config0 {configExecutorId = Just ("hs-l2-client-debounce-nameless-exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \_dbos -> do
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            refused <- debounceClient (debouncerClientNew client (newQualifiedWorkflowName "") queueName) "user-1" (secondsDuration 1) Nothing
            case refused of
              Left (InvalidArgument operation detail) -> do
                operation @?= "enqueue a workflow"
                detail @?= ("queue `" <> queueName <> "`: `workflow` must not be empty")
              other -> fail ("expected an InvalidArgument refusal, got: " <> show other)
            listed <- clientListWorkflows client (defaultWorkflowFilter {workflowFilterNames = [""]})
            case listed of
              Left err -> fail (show err)
              Right records -> records @?= [],
      testCase "a config with no class debounces apart from the free function" $ do
        -- Mirrors Rust @a_config_with_no_class_debounces_apart_from_the_free_function@:
        -- its deduplication id keeps the config.
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-debounce-unclassed-" <> Text.take 12 suffix
            queueName = "hs-l2-client-debounce-unclassed-q-" <> Text.take 12 suffix
        config0 <- configFromEnv appName
        let config = config0 {configExecutorId = Just ("hs-l2-client-debounce-unclassed-exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \_dbos -> do
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            let free = debouncerClientNew client (newQualifiedWorkflowName "echo") queueName
                unclassed = debouncerClientNew client (instanceQualifiedWorkflowName "echo" "" "eu") queueName
            first <- debounceClient free "key" (secondsDuration 60) (Just (encodeWorkflowInput (1 :: Int)))
            second <- debounceClient unclassed "key" (secondsDuration 60) (Just (encodeWorkflowInput (2 :: Int)))
            case (first, second) of
              (Right held1, Right held2) -> do
                assertBool "separate debounces" (held1.workflowId /= held2.workflowId)
                found <- clientWorkflowRecord client (WorkflowId held2.workflowId)
                case found of
                  Right (Just row) -> do
                    row.workflowRecordClassName @?= Nothing
                    row.workflowRecordConfigName @?= Just "eu"
                    row.workflowRecordDeduplicationId @?= Just "echo//eu-key"
                  other -> fail ("expected the unclassed row, got: " <> show other)
              other -> fail ("expected two debounces, got: " <> show other),
      testCase "an enqueue asks for a queue and nothing else" $ do
        -- Mirrors Rust @an_enqueue_asks_for_a_queue_and_nothing_else@: the
        -- flat fields build the request a start would make.
        let options = enqueueOptionsNew "work"
        queueRequest options @?= queueRequestNew "work"
        options.workflowId @?= Nothing
        options.appVersion @?= Nothing
        options.timeout @?= Inherit
        options.duplicationPolicy @?= Reject,
      testCase "flat queue fields make the queue request" $ do
        -- Mirrors Rust @flat_queue_fields_make_the_queue_request@: the row
        -- and the shared checks see what the caller wrote.
        let options =
              ((enqueueOptionsNew "billing") {priority = Just 3, partitionKey = Just "tenant-7", workflowId = Just "order-42"} :: EnqueueOptions)
        queueRequest options
          @?= (((queueRequestNew "billing") {priority = Just 3, partitionKey = Just "tenant-7"}) :: QueueRequest),
      testCase "an enqueue no queue could honour is refused before any write" $ do
        -- A deduplication id beside a partition key is legal: the key is
        -- held across the whole queue, not within one partition.
        let both = (((queueRequestNew "q") {deduplicationId = Just "key", partitionKey = Just "part"}) :: QueueRequest)
        (validateEnqueue both :: Either (Error EngineOnly) ()) @?= Right ()
        let noKey = (((queueRequestNew "q") {duplicationPolicy = ReturnExisting}) :: QueueRequest)
        case (validateEnqueue noKey :: Either (Error EngineOnly) ()) of
          Left (InvalidArgument _ _) -> pure ()
          other -> fail ("expected return-existing without a key to be refused, got: " <> show other)
        let zero = (((queueRequestNew "q") {priority = Just 0}) :: QueueRequest)
        case (validateEnqueue zero :: Either (Error EngineOnly) ()) of
          Left (InvalidArgument _ _) -> pure ()
          other -> fail ("expected priority zero to be refused, got: " <> show other)
        storedPriority (queueRequestNew "q") @?= 0
        storedPriority (((queueRequestNew "q") {priority = Just 3}) :: QueueRequest) @?= 3,
      testCase "a start's durations are refused on the start's channel" $ do
        let huge = case durationFromSecs 1e18 of
              Nothing -> error "a storable-busting duration must build"
              Just duration -> duration
        -- Zero reads as zero at millisecond granularity on both layers,
        -- as the system database's own check does; one millisecond is
        -- the smallest valid duration.
        case (validateEnqueue (((queueRequestNew "q") {delay = Just (millisDuration 0)}) :: QueueRequest) :: Either (Error EngineOnly) ()) of
          Left (InvalidArgument operation detail) -> do
            operation @?= "enqueue a workflow"
            detail @?= "queue `q`: `delay` must be a positive, non-zero duration"
          other -> fail ("expected an InvalidArgument refusal, got: " <> show other)
        case (validateEnqueue (((queueRequestNew "q") {delay = Just huge}) :: QueueRequest) :: Either (Error EngineOnly) ()) of
          Left (InvalidArgument operation detail) -> do
            operation @?= "enqueue a workflow"
            detail @?= "queue `q`: `delay` must fit in milliseconds as a 64-bit integer"
          other -> fail ("expected an InvalidArgument refusal, got: " <> show other)
        case (validateStart (Just (queueRequestNew "q")) (Just huge) Nothing WorkflowIdReturnExisting :: Either (Error EngineOnly) ()) of
          Left (InvalidArgument operation detail) -> do
            operation @?= "enqueue a workflow"
            detail @?= "queue `q`: `timeout` must fit in milliseconds as a 64-bit integer"
          other -> fail ("expected an InvalidArgument refusal, got: " <> show other)
        case (validateStart Nothing (Just huge) Nothing WorkflowIdReturnExisting :: Either (Error EngineOnly) ()) of
          Left (InvalidArgument operation detail) -> do
            operation @?= "start a workflow"
            detail @?= "`timeout` must fit in milliseconds as a 64-bit integer"
          other -> fail ("expected an InvalidArgument refusal, got: " <> show other)
        validateStart (Just (queueRequestNew "q")) (Just (millisDuration 1)) Nothing WorkflowIdReturnExisting @?= (Right () :: Either (Error EngineOnly) ())
        validateStart Nothing Nothing Nothing WorkflowIdReturnExisting @?= (Right () :: Either (Error EngineOnly) ()),
      testCase "a reuse policy about no id is refused before any write" $
        -- Mirrors Rust @validate_start@: the policy is about an id the
        -- caller chose, so Reject without one is refused.
        validateStart (Just (queueRequestNew "q")) Nothing Nothing WorkflowIdReject
          @?= (Left (InvalidArgument "enqueue a workflow" "queue `q`: `WorkflowIdReusePolicy::Reject` needs a `workflow_id`: the policy is about an id the caller chose") :: Either (Error EngineOnly) ()),
      testCase "a start the database refuses reports as the start's own" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-refused-" <> Text.take 12 suffix
            queueName = "hs-l2-client-refused-q-" <> Text.take 12 suffix
        clientConfig0 <- clientConfigFromEnv
        let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
        bracket (connectOrFail clientConfig) closeClient $ \client -> do
          let enqueued shape = clientEnqueueWorkflowWith client (newQualifiedWorkflowName "refused") shape Nothing
              check shape expected = do
                refused <- enqueued shape
                case refused of
                  Left (InvalidArgument operation detail) -> do
                    operation @?= "enqueue a workflow"
                    detail @?= expected
                  other -> fail ("expected an InvalidArgument refusal, got: " <> show other)
          check
            (((enqueueOptionsNew queueName) {workflowId = Just ""}) :: EnqueueOptions)
            ("queue `" <> queueName <> "`: `workflow_id` must not be empty")
          check
            (((enqueueOptionsNew queueName) {deduplicationId = Just ""}) :: EnqueueOptions)
            ("queue `" <> queueName <> "`: `deduplication_id` must be absent rather than empty")
          check
            (((enqueueOptionsNew queueName) {partitionKey = Just ""}) :: EnqueueOptions)
            ("queue `" <> queueName <> "`: `partition_key` must be absent rather than empty")
          check
            (((enqueueOptionsNew queueName) {appVersion = Just ""}) :: EnqueueOptions)
            ("queue `" <> queueName <> "`: `app_version` must be absent rather than empty")
          -- An empty queue name is refused up front, naming the field the
          -- caller wrote. Mirrors the oracle's new table row.
          check
            (enqueueOptionsNew "")
            "`queue_name` must not be empty"
          -- A workflow with no name, which nothing could dequeue, is
          -- refused before anything is written. Mirrors Rust's nameless
          -- client enqueue refusal.
          nameless <- clientEnqueueWorkflowWith client (newQualifiedWorkflowName "") (enqueueOptionsNew queueName) Nothing
          case nameless of
            Left (InvalidArgument operation detail) -> do
              operation @?= "enqueue a workflow"
              detail @?= ("queue `" <> queueName <> "`: `workflow` must not be empty")
            other -> fail ("expected an InvalidArgument refusal, got: " <> show other)
          listed <- clientListWorkflows client (defaultWorkflowFilter {workflowFilterNames = [""]})
          case listed of
            Left err -> fail (show err)
            Right records -> records @?= [],
      testCase "a second enqueue under a held key joins the holder" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-dup-" <> Text.take 12 suffix
            appVersion = "hs-l2-client-dup-version-" <> suffix
            executorId = "hs-l2-client-dup-executor-" <> suffix
            queueName = "hs-l2-client-dup-q-" <> Text.take 12 suffix
            key = newQualifiedWorkflowName "double"
            options =
              ((((enqueueOptionsNew queueName) {deduplicationId = Just ("dup-" <> suffix), duplicationPolicy = ReturnExisting}) :: EnqueueOptions)
                {appVersion = Just appVersion} :: EnqueueOptions)
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
            first <- clientEnqueueWorkflowWith client (newQualifiedWorkflowName "double") options (Just (encodeWorkflowInput (21 :: Int)))
            second <- clientEnqueueWorkflowWith client (newQualifiedWorkflowName "double") options (Just (encodeWorkflowInput (99 :: Int)))
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
            key = newQualifiedWorkflowName "double"
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
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowInput (21 :: Int)))
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
              (newQualifiedWorkflowName "no-such-function")
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
            options =
              ((enqueueOptionsNew queueName)
                { deduplicationId = Just ("opts-" <> suffix),
                  priority = Just 3,
                  delay = Just (secondsDuration 60),
                  workflowId = Just workflowText,
                  appVersion = Just "v1",
                  timeout = Explicit (secondsDuration 30),
                  attributes = Just (Map.fromList [("k", String "v")])
                } :: EnqueueOptions)
        clientConfig0 <- clientConfigFromEnv
        bracket (connectOrFail clientConfig0) closeClient $ \client -> do
          enqueued <- clientEnqueueWorkflowWith client (methodQualifiedWorkflowName "double" "cls") options Nothing
          case enqueued of
            Left err -> fail (show err)
            Right _ -> pure ()
          row <- readRow getBackend (WorkflowId workflowText)
          case row of
            WorkflowRecord {workflowRecordName = name, workflowRecordClassName = className, workflowRecordConfigName = configName, workflowRecordQueueName = queue, workflowRecordPriority = priority, workflowRecordDeduplicationId = dedup, workflowRecordAppVersion = version, workflowRecordTimeout = budget, workflowRecordAttributes = attributes, workflowRecordStatus = status} -> do
              name @?= Just "double"
              className @?= Just "cls"
              configName @?= Nothing
              queue @?= Just queueName
              priority @?= 3
              dedup @?= Just ("opts-" <> suffix)
              version @?= Just "v1"
              budget @?= Just (secondsDuration 30)
              assertBool "attributes are stored" (attributes /= Nothing)
              status @?= Delayed,
      testCase "an enqueued name writes an empty class or config as null" $ do
        -- Mirrors Rust @an_enqueued_key_writes_an_empty_class_or_config_as_null@:
        -- absence has one spelling on the row.
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            queueName = "hs-l2-client-blank-q-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-blank-id-" <> suffix
        clientConfig0 <- clientConfigFromEnv
        bracket (connectOrFail clientConfig0) closeClient $ \client -> do
          enqueued <-
            clientEnqueueWorkflowWith
              client
              (instanceQualifiedWorkflowName "never_registered" "" "")
              ((enqueueOptionsNew queueName) {workflowId = Just workflowText})
              Nothing
          case enqueued of
            Left err -> fail (show err)
            Right _ -> pure ()
          row <- readRow getBackend (WorkflowId workflowText)
          row.workflowRecordClassName @?= Nothing
          row.workflowRecordConfigName @?= Nothing,
      testCase "a rejecting client enqueue refuses a taken id" $ do
        -- Mirrors Rust @a_rejecting_client_enqueue_refuses_a_taken_id@: a
        -- Reject enqueue whose id is taken runs nothing and writes nothing;
        -- the default joins the holder.
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            queueName = "hs-l2-client-reject-q-" <> Text.take 12 suffix
            takenText = "hs-l2-client-taken-id-" <> suffix
            options policy = (enqueueOptionsNew queueName) {workflowId = Just takenText, workflowIdReusePolicy = policy}
        clientConfig0 <- clientConfigFromEnv
        bracket (connectOrFail clientConfig0) closeClient $ \client -> do
          first <-
            clientEnqueueWorkflowWith
              client
              (newQualifiedWorkflowName "never_registered")
              (options WorkflowIdReject)
              (Just (encodeWorkflowInput (1 :: Int)))
          case first of
            Left err -> fail ("a fresh id is not in use, got: " <> show err)
            Right _ -> pure ()
          before <- readRow getBackend (WorkflowId takenText)
          refused <-
            clientEnqueueWorkflowWith
              client
              (newQualifiedWorkflowName "never_registered")
              (options WorkflowIdReject)
              (Just (encodeWorkflowInput (2 :: Int)))
          case refused of
            Left (SystemDatabase (SystemDBError.WorkflowIdInUse {workflowId = wid, status = st, existingName = name})) -> do
              wid @?= takenText
              st @?= "ENQUEUED"
              name @?= Just "never_registered"
            other -> fail ("expected WorkflowIdInUse, got: " <> show other)
          joined <-
            clientEnqueueWorkflowWith
              client
              (newQualifiedWorkflowName "never_registered")
              (options WorkflowIdReturnExisting)
              (Just (encodeWorkflowInput (3 :: Int)))
          case joined of
            Right handle -> handle.workflowId @?= takenText
            other -> fail ("expected the default join, got: " <> show other)
          after <- readRow getBackend (WorkflowId takenText)
          after.workflowRecordInput @?= before.workflowRecordInput
          after.workflowRecordUpdatedAt @?= before.workflowRecordUpdatedAt,
      testCase "a nameless client writes unclaimed rows, which the app claims when it runs them" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-adopt-" <> Text.take 12 suffix
            appVersion = "hs-l2-client-adopt-version-" <> suffix
            executorId = "hs-l2-client-adopt-executor-" <> suffix
            unpolledQueue = "hs-l2-client-nameless-q-" <> Text.take 12 suffix
            adoptedQueue = "hs-l2-client-adopt-q-" <> Text.take 12 suffix
            unpolledText = "hs-l2-client-nameless-id-" <> suffix
            adoptedText = "hs-l2-client-adopt-id-" <> suffix
            key = newQualifiedWorkflowName "adopt"
        config0 <- configFromEnv appName
        -- The driven dequeue and the background supervisor sweep this case's
        -- queue only, not every fixture queue on the shared database.
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId, configListenQueues = Just [adoptedQueue]}
            body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value _ = pure (Right value)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          _ <- launchClientExec dbos isolatedEnvironment
          queueRegistered <- registerQueue dbos adoptedQueue defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          clientConfig0 <- clientConfigFromEnv
          bracket (connectOrFail clientConfig0) closeClient $ \client -> do
            -- A queue nobody polls, so the row stays exactly as written.
            unpolled <-
              clientEnqueueWorkflowWith
                client
                (newQualifiedWorkflowName "adopt")
                (enqueueOptionsNew unpolledQueue) {workflowId = Just unpolledText}
                (Just (encodeWorkflowInput (7 :: Int)))
            case unpolled of
              Left err -> fail (show err)
              Right _ -> pure ()
            unclaimed <- readRow getBackend (WorkflowId unpolledText)
            unclaimed.workflowRecordAppName @?= Nothing
            -- On the queue the app polls, the row the client wrote unclaimed is
            -- claimed by the application that runs it, so the unclaimed
            -- partition drains on its own. Pinned to the app's version: an
            -- unversioned row is only claimed by the latest registered version,
            -- and this shared database always has a newer nameless row.
            adopted <-
              clientEnqueueWorkflowWith
                client
                (newQualifiedWorkflowName "adopt")
                ((enqueueOptionsNew adoptedQueue) {workflowId = Just adoptedText, appVersion = Just appVersion})
                (Just (encodeWorkflowInput (7 :: Int)))
            case adopted of
              Left err -> fail (show err)
              Right handle -> do
                _ <- dequeueWorkflows dbos
                result <- resultWf handle
                case result of
                  Right (Just stored) -> do
                    let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                    assertEqual "the app ran the client's enqueue" (Right 7) decoded
                  other -> fail (show other)
                claimed <- readRow getBackend (WorkflowId adoptedText)
                claimed.workflowRecordAppName @?= Just appName,
      testCase "the workflow id is an idempotency key" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            queueName = "hs-l2-client-idkey-q-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-idkey-id-" <> suffix
        clientConfig0 <- clientConfigFromEnv
        bracket (connectOrFail clientConfig0) closeClient $ \client -> do
          first <- clientEnqueueWorkflowWith client (newQualifiedWorkflowName "double") (enqueueOptionsNew queueName) {workflowId = Just workflowText} Nothing
          second <- clientEnqueueWorkflowWith client (newQualifiedWorkflowName "double") (enqueueOptionsNew queueName) {workflowId = Just workflowText} Nothing
          case (first, second) of
            (Right holder, Right joiner) -> joiner.workflowId @?= holder.workflowId
            (Left err, _) -> fail ("the first enqueue failed: " <> show err)
            (_, Left err) -> fail ("the joining enqueue failed: " <> show err),
      testCase "closing is clean and a closed client refuses further calls" $ do
        clientConfig0 <- clientConfigFromEnv
        client <- connectOrFail clientConfig0
        -- A closed client is settled, not reset: its pool is released and
        -- every later call is a permanent refusal, which is what ends a
        -- parked wait instead of reconnecting it. Closing twice is safe.
        closeClient client
        closeClient client
        enqueued <- clientEnqueueWorkflowWith client (newQualifiedWorkflowName "double") (enqueueOptionsNew "hs-l2-client-closed-q") Nothing
        case enqueued of
          Left (SystemDatabase (SystemDB.Backend err)) -> do
            err.backendKind @?= SystemDB.Permanent
            assertBool "the refusal names the closed handle" ("closed" `Text.isInfixOf` err.backendMessage)
          other -> fail ("expected a permanent closed-client refusal, got: " <> show other),
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
            key = newQualifiedWorkflowName "receiver"
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
            enqueued <- clientEnqueueWorkflowWith client (newQualifiedWorkflowName "receiver") options Nothing
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
            key = newQualifiedWorkflowName "batcher"
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
            enqueued <- clientEnqueueWorkflowWith client (newQualifiedWorkflowName "batcher") options Nothing
            batch <- case enqueued of
              Left err -> fail (show err)
              Right handle -> pure handle
            let enqueuedId = WorkflowId batch.workflowId
            sent <-
              clientSendMessages
                client
                [ EncodedSendMessage enqueuedId (encodeWorkflowValue ("one" :: Text)) (Just (Topic "ping")) Nothing,
                  EncodedSendMessage enqueuedId (encodeWorkflowValue ("two" :: Text)) (Just (Topic "ping")) Nothing
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
            key = newQualifiedWorkflowName "txreceiver"
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
            enqueued <- clientEnqueueWorkflowWith client (newQualifiedWorkflowName "txreceiver") options Nothing
            receiver <- case enqueued of
              Left err -> fail (show err)
              Right handle -> pure handle
            backend <- getBackend
            bracket
              (Postgres.runSession backend "client-tx-ddl" (Session.statement () (createProbeTable table)) >>= either (fail . show) pure)
              (\_ -> Postgres.runSession backend "client-tx-ddl" (Session.statement () (dropProbeTable table)) >> pure ())
              (\_ -> do
                fallback <- Text.pack . UUID.toString <$> UUID.V4.nextRandom
                composed <- clientSendMessagesInTx client "probe-commit" (do _ <- Tx.statement "row-1" (insertProbe table); Tx.statement "row-2" (insertProbe table)) [EncodedSendMessage (WorkflowId receiver.workflowId) (encodeWorkflowValue ("hello" :: Text)) (Just topic) Nothing] (Just "portable_json") [fallback] False
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
              let keyed destination = EncodedSendMessage destination (encodeWorkflowValue ("hello" :: Text)) (Just topic) (Just (IdempotencyKey "dup-key"))
              refused <- clientSendMessagesInTx client "probe-validation" (Tx.statement "row-1" (insertProbe table)) [keyed (WorkflowId ("client-tx-a-" <> suffix)), keyed (WorkflowId ("client-tx-b-" <> suffix))] (Just "portable_json") [fallback, fallback] False
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
              aborted <- clientSendMessagesInTx client "probe-fk" (Tx.statement "row-1" (insertProbe table)) [EncodedSendMessage (WorkflowId ("client-tx-nowhere-" <> suffix)) (encodeWorkflowValue ("hello" :: Text)) (Just topic) Nothing] (Just "portable_json") [fallback] False
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
            key = newQualifiedWorkflowName "forked"
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
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowInput (21 :: Int)))
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
                composed <- clientSendMessageInTx client "probe-forks" (Tx.statement "row-1" (insertProbe table)) (EncodedSendMessage (WorkflowId workflowText) (encodeWorkflowValue ("hello" :: Text)) (Just topic) (Just (IdempotencyKey "fork-key"))) (Just "portable_json") fallback True
                composed @?= Right ()
                notified <- Postgres.runSession backend "client-tx-probe" (Session.statement topicName countTopic)
                notified @?= Right 2),
      testCase "a client rewinds a finished workflow" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-rewind-" <> Text.take 12 suffix
            workflowText = "hs-l2-client-rewind-id-" <> suffix
            key = newQualifiedWorkflowName "redoubled"
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
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowInput (21 :: Int)))
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
            key = newQualifiedWorkflowName "plain"
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
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowInput ()))
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
            key = newQualifiedWorkflowName "greeter"
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
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowInput (21 :: Int)))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            found <- clientGetEventSerialized client (WorkflowId workflowText) "greeting" (millisDuration 100)
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
            key = newQualifiedWorkflowName "double"
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
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowInput (21 :: Int)))
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
            key = newQualifiedWorkflowName "double"
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
            enqueued <- clientEnqueueWorkflowWith client (newQualifiedWorkflowName "double") (enqueueOptionsNew queueName) {workflowId = Just workflowText} Nothing
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
            key = newQualifiedWorkflowName "double"
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
          ran <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowInput (21 :: Int)))
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
            listed <- clientListAppVersions client
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
              promoted <- clientPromoteVersion client versionA
              case promoted of
                Left err -> fail (show err)
                Right () -> pure ()
              rolled <- clientListAppVersions client
              case rolled of
                Left err -> fail (show err)
                Right versions -> assertAppLatest appName versionA versions,
      testCase "a client promotes a version for another application" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-for-" <> Text.take 12 suffix
            peerApp = "hs-l2-client-peer-" <> Text.take 12 suffix
            versionA = "hs-l2-client-for-va-" <> suffix
            versionP = "hs-l2-client-for-vp-" <> suffix
        config0 <- configFromEnv appName
        let configA = config0 {configAppVersion = Just versionA, configExecutorId = Just ("exec-for-a-" <> suffix)}
        configP0 <- configFromEnv peerApp
        let configP = configP0 {configAppVersion = Just versionP, configExecutorId = Just ("exec-for-p-" <> suffix)}
        clientConfig0 <- clientConfigFromEnv
        let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
            peerConfig = (clientConfig0 :: ClientConfig) {appName = Just peerApp}
        bracket (connectOrFail clientConfig) closeClient $ \client -> do
          bracket (connectOrFail peerConfig) closeClient $ \peer -> do
            bracket (newDBOS configA) shutdown $ \dbosA -> do
              _ <- launchClientExec dbosA isolatedEnvironment
              bracket (newDBOS configP) shutdown $ \dbosP -> do
                _ <- launchClientExec dbosP isolatedEnvironment
                -- A peer's version is refused unless the promotion names
                -- the peer: the stamp is what the peer's fleet is rolling
                -- on, so it is not this client's to move without saying so.
                misowned <- clientPromoteVersionFor client versionP appName
                case misowned of
                  Left (SystemDatabase (SystemDBError.RegisteredByAnother {})) -> pure ()
                  other -> fail ("expected a peer-ownership refusal, got: " <> show other)
                named <- clientPromoteVersionFor client versionP peerApp
                case named of
                  Left err -> fail (show err)
                  Right () -> pure ()
                peerLatest <- clientLatestAppVersion peer
                case peerLatest of
                  Right (Just row) -> row.versionInfoName @?= versionP
                  other -> fail ("expected the peer's promotion latest, got: " <> show other),
      testCase "a client promotes on the database clock" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-client-skew-" <> Text.take 12 suffix
        -- The database's clock an hour behind this process: a promotion
        -- stamped by this process's clock would land an hour ahead of the
        -- database's, so a version deployed after it would sort below it
        -- and not become the latest for an hour. Mirrors the oracle's
        -- skewed client promotion test.
        withSkewedClient getBackend (Just appName) (-3600) $ \(env, client) -> do
          let deploy version = do
                created <- SystemDB.createApplicationVersion env version Nothing
                case created of
                  Left err -> fail (show err)
                  Right () -> pure ()
              latest = do
                latest <- clientLatestAppVersion client
                case latest of
                  Right (Just row) -> pure row.versionInfoName
                  other -> fail ("expected a latest version, got: " <> show other)
          deploy ("v1-" <> suffix)
          deploy ("v2-" <> suffix)
          promoted <- clientPromoteVersion client ("v1-" <> suffix)
          case promoted of
            Left err -> fail (show err)
            Right () -> pure ()
          latest >>= (@?= ("v1-" <> suffix))
          deploy ("v3-" <> suffix)
          latest >>= (@?= ("v3-" <> suffix)),
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
              latestA <- clientLatestAppVersion clientA
              case latestA of
                Right (Just (VersionInfo {versionInfoName})) -> versionInfoName @?= versionA
                other -> fail ("expected A's own latest " <> Text.unpack versionA <> ", got: " <> show other)
            bracket (connectOrFail clientConfigB) closeClient $ \clientB -> do
              latestB <- clientLatestAppVersion clientB
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
            key = newQualifiedWorkflowName "double"
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
          _ <- runWf exec key (WorkflowId firstText) (Just (encodeWorkflowInput (1 :: Int)))
          _ <- runWf exec key (WorkflowId secondText) (Just (encodeWorkflowInput (2 :: Int)))
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
            key = newQualifiedWorkflowName "double"
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
            enqueued <- clientEnqueueWorkflow client (newQualifiedWorkflowName "double") queueName (Just (encodeWorkflowInput (21 :: Int)))
            handle <- case enqueued of
              Left err -> fail (show err)
              Right handle -> pure handle
            -- No options names no version: the row is claimed only by the
            -- latest registered version.
            row <- readRow getBackend (WorkflowId handle.workflowId)
            row.workflowRecordAppVersion @?= Nothing,
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
            key = newQualifiedWorkflowName "quiet"
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
            waited <- clientGetEventSerialized client (WorkflowId workflowText) "absent" (millisDuration 200)
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
            key = newQualifiedWorkflowName "double"
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
          _ <- runWf exec key (WorkflowId firstText) (Just (encodeWorkflowInput (1 :: Int)))
          _ <- runWf exec key (WorkflowId secondText) (Just (encodeWorkflowInput (2 :: Int)))
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
            key = newQualifiedWorkflowName "double"
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
          _ <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowInput (1 :: Int)))
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
            merged <- clientMergeWorkflowAttributes client (WorkflowId workflowText) "{\"tier\":\"silver\"}"
            merged @?= Right ()
            mergedListed <- clientListWorkflows client filters
            case mergedListed of
              Left err -> fail (show err)
              -- A merge keeps what is there: the team survives beside the
              -- new tier.
              Right [record] -> record.workflowRecordAttributes @?= Just "{\"team\": \"red\", \"tier\": \"silver\"}"
              Right records -> fail ("expected one merged row, got: " <> show (length records))
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
            key = newQualifiedWorkflowName "double"
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
                options = ((((enqueueOptionsNew queueName) {delay = Just (secondsDuration 3600)}) :: EnqueueOptions) {workflowId = Just workflowText} :: EnqueueOptions)
            enqueued <- clientEnqueueWorkflowWith client (newQualifiedWorkflowName "double") options (Just (encodeWorkflowInput (1 :: Int)))
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
            key = newQualifiedWorkflowName "double"
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
                enqueued <- clientEnqueueWorkflowInTx client (newQualifiedWorkflowName "double") "enq-commit" (Tx.statement "row-1" (insertProbe table)) (enqueueOptionsNew queueName) (Just (encodeWorkflowInput (7 :: Int)))
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
              first <- clientEnqueueWorkflowInTx client (newQualifiedWorkflowName "double") "enq-first" (Tx.statement "row-1" (insertProbe table)) options Nothing
              case first of
                Left err -> fail (show err)
                Right _ -> pure ()
              second <- clientEnqueueWorkflowInTx client (newQualifiedWorkflowName "double") "enq-repeat" (Tx.statement "row-2" (insertProbe table)) options Nothing
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
              first <- clientEnqueueWorkflowInTx client (newQualifiedWorkflowName "double") "enq-first" (Tx.statement "row-1" (insertProbe table)) options Nothing
              case first of
                Left err -> fail (show err)
                Right _ -> pure ()
              conflicted <- clientEnqueueWorkflowInTx client (newQualifiedWorkflowName "rival") "enq-rival" (Tx.statement "row-2" (insertProbe table)) rival Nothing
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
            options = ((enqueueOptionsNew queueName) {duplicationPolicy = ReturnExisting, deduplicationId = Just "k"} :: EnqueueOptions)
        bracket (connectOrFail clientConfig) closeClient $ \client -> do
          backend <- getBackend
          bracket
            (Postgres.runSession backend "client-enqtx-ddl" (Session.statement () (createProbeTable table)) >>= either (fail . show) pure)
            (\_ -> Postgres.runSession backend "client-enqtx-ddl" (Session.statement () (dropProbeTable table)) >> pure ())
            (\_ -> do
              refused <- clientEnqueueWorkflowInTx client (newQualifiedWorkflowName "double") "enq-join" (Tx.statement "row-1" (insertProbe table)) options Nothing
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
            key = newQualifiedWorkflowName "double"
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
          _ <- runWf exec key (WorkflowId plainText) (Just (encodeWorkflowInput (1 :: Int)))
          clientConfig0 <- clientConfigFromEnv
          let clientConfig = (clientConfig0 :: ClientConfig) {appName = Just appName}
          bracket (connectOrFail clientConfig) closeClient $ \client -> do
            registeredQ <- clientRegisterQueue client queueName defaultQueueOptions AlwaysUpdate
            case registeredQ of
              Left err -> fail (show err)
              Right _ -> pure ()
            let options :: EnqueueOptions
                options = (enqueueOptionsNew queueName) {workflowId = Just queuedText}
            enqueued <- clientEnqueueWorkflowWith client (newQualifiedWorkflowName "double") options (Just (encodeWorkflowInput (2 :: Int)))
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
            key = newQualifiedWorkflowName "streamer"
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
          _ <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowInput (5 :: Int)))
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
runWf :: Executor IO -> QualifiedWorkflowName -> WorkflowId -> Maybe WorkflowInput -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
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

-- | A skewed system-database backend beside a client over it, for tests
-- that time promotions against a database whose clock is not this
-- process's. The schema shadows @now()@ and @clock_timestamp()@ through a
-- private pool's search path (mirroring the clock-wait tests' backend, but
-- the client builds its own pool, so it connects through URL options
-- instead); both are torn down afterwards. The backend serves deploys and
-- reads, the client the calls under test.
-- | A skewed clock plus a private schema: connections through this pool
-- read the database's clock minus the skew. The env claims the skewed
-- application's name, so versions it deploys are that application's own:
-- an unclaimed deploy would be visible to every app-scoped latest read
-- on the shared database, and a concurrent case's read could name this
-- case's rows.
withSkewedClient :: IO Postgres.PostgresSystemDB -> Maybe Text -> Int -> ((Postgres.PostgresSystemDB, Client IO) -> IO a) -> IO a
withSkewedClient getBackend appName skewSeconds action = do
  base <- getBackend
  tag <- (Text.filter (/= '-') . Text.pack . UUID.toString) <$> UUID.V4.nextRandom
  let schema = "skew_" <> tag
      offset = Text.pack (show skewSeconds) <> " seconds"
      ddl =
        [ "CREATE SCHEMA " <> schema,
          "CREATE FUNCTION " <> schema <> ".now() RETURNS timestamptz LANGUAGE sql STABLE AS $$ SELECT pg_catalog.now() + interval '" <> offset <> "' $$",
          "CREATE FUNCTION " <> schema <> ".clock_timestamp() RETURNS timestamptz LANGUAGE sql VOLATILE AS $$ SELECT pg_catalog.clock_timestamp() + interval '" <> offset <> "' $$"
        ]
  mapM_
    ( \statement -> Postgres.runSession base "client-skew-ddl" (Session.script statement) >>= either (fail . show) pure
    )
    ddl
  clientConfig0 <- clientConfigFromEnv
  let skewedUrl = clientConfig0.databaseUrl <> "?options=-c%20search_path%3D" <> schema <> "%2Cpg_catalog%2Cpublic"
  pool <-
    Pool.acquire
      Pqi.adapter
      ( PoolConfig.settings
          [ PoolConfig.size 4,
            PoolConfig.acquisitionTimeout 10,
            PoolConfig.staticConnectionSettings (ConnectionSettings.connectionString skewedUrl),
            PoolConfig.initSession (Session.script ("SET search_path = " <> schema <> ", pg_catalog, public"))
          ]
      )
  bracket (Postgres.fromPool pool 4 (Postgres.defaultSettings {Postgres.settingsAppName = appName}) nullTracer) Postgres.releasePostgresSystemDB $ \env -> do
    let clientConfig = clientConfig0 {appName = appName, databaseUrl = skewedUrl}
    bracket (connectOrFail clientConfig) closeClient $ \client -> do
      result <- action (env, client)
      Postgres.runSession base "client-skew-ddl" (Session.script ("DROP SCHEMA " <> schema <> " CASCADE")) >>= either (fail . show) pure
      pure result

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
          | VersionInfo {versionInfoAppName = owner, versionInfoName = name, versionInfoTimestamp = stamp} <- versions,
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
