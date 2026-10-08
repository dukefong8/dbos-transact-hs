{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | The transactional outbox over live Postgres: Variant A (the atomic
-- workflow) and Variant B (the transactional enqueue) under all-or-nothing
-- commit and rollback, plus notification redrive — with real SQL through
-- the 'Tx' handle, real engine entries, and row-count observations. The app
-- tables live in the shared @outbox_store@ schema (created idempotently at
-- fixture setup, mirroring the demo's own schema); every case owns its rows
-- via a unique customer tag.
module DBOS.Transact.OutboxTest (tests) where

import DBOS.DualStack (liveCaseWith)
import DBOS.Prelude
import Data.Aeson qualified as Aeson
import Data.Int (Int32, Int64)
import Data.Text qualified as Text
import Data.ByteString.Lazy qualified as LBS
import Data.Text.Encoding (decodeUtf8)
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (WorkflowRecord (..))
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
import DBOS.Transact.Connection (SomeSystemDB (..))
import DBOS.Transact.Logger (SomeTracer (..), acquireLoggerBackend, ioTracer, nullTracer)
import DBOS.Transact.OutboxCases
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import Test.Tasty (TestTree, testGroup, withResource)

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    withResource acquireLoggerBackend snd $ \getLogger ->
      testGroup
        "Outbox transactions"
        [ liveCaseWith (withOutboxFixture getBackend (ioTracer . fst <$> getLogger)) "the atomic workflow commits order and notification together" scenarioAtomicCommit checkAtomicCommit,
          liveCaseWith (withOutboxFixture getBackend (ioTracer . fst <$> getLogger)) "the atomic workflow rolls back on an app throw, then resumes" scenarioAtomicRollback checkAtomicRollback,
          liveCaseWith (withOutboxFixture getBackend (ioTracer . fst <$> getLogger)) "the transactional enqueue commits order and enqueue together" scenarioEnqueueCommit checkEnqueueCommit,
          liveCaseWith (withOutboxFixture getBackend (ioTracer . fst <$> getLogger)) "the transactional enqueue rolls back on an app throw" scenarioEnqueueRollbackApp checkEnqueueRollbackApp,
          liveCaseWith (withOutboxFixture getBackend (ioTracer . fst <$> getLogger)) "the transactional enqueue rolls back on a database error" scenarioEnqueueRollbackDb checkEnqueueRollbackDb,
          liveCaseWith (withOutboxFixture getBackend (ioTracer . fst <$> getLogger)) "a failed atomic send redrives to exactly one notification" scenarioAtomicRedrive checkAtomicRedrive,
          liveCaseWith (withOutboxFixture getBackend (ioTracer . fst <$> getLogger)) "a failed enqueued notification redrives to exactly one notification" scenarioEnqueueRedrive checkEnqueueRedrive
        ]

acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | The shared app schema, created idempotently: mirrors the demo's own
-- schema (demo-apps/dbos-hs-outbox schema.sql), which the demo creates at
-- startup. Tests must not depend on the demo server having run.
ensureOutboxSchema :: Session.Session ()
ensureOutboxSchema = do
  Session.statement
    ()
    ( Statement.preparable
        "create schema if not exists outbox_store"
        Encoders.noParams
        Decoders.noResult
    )
  Session.statement
    ()
    ( Statement.preparable
        "create table if not exists outbox_store.orders (order_id serial primary key, customer text not null, item text not null, quantity integer not null, notification_status text not null default 'PENDING', created_at timestamp default now() not null)"
        Encoders.noParams
        Decoders.noResult
    )
  Session.statement
    ()
    ( Statement.preparable
        "create table if not exists outbox_store.transaction_completion (workflow_id text not null, step_name text not null, function_num int not null, output text, error text, primary key (workflow_id, function_num))"
        Encoders.noParams
        Decoders.noResult
    )

insertStmt :: Statement.Statement (Text, Text, Int32) Int32
insertStmt =
  Statement.preparable
    "insert into outbox_store.orders (customer, item, quantity) values ($1, $2, $3) returning order_id"
    ( contramap (\(c, _, _) -> c) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, i, _) -> i) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, q) -> q) (Encoders.param (Encoders.nonNullable Encoders.int4))
    )
    (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int4)))

markStmt :: Statement.Statement Int32 ()
markStmt =
  Statement.preparable
    "update outbox_store.orders set notification_status = 'SENT' where order_id = $1"
    (Encoders.param (Encoders.nonNullable Encoders.int4))
    Decoders.noResult

-- | The demo's enqueue statement plus the version/app stamps the demo now
-- carries (Variant B writes its row by hand, so the test copy stamps what
-- the engine stamps automatically).
enqueueStmt :: Statement.Statement (Text, Text, Text, Text, Text, Text, Text) (Maybe Text)
enqueueStmt =
  Statement.preparable
    "select dbos.enqueue_workflow(workflow_name => $3, queue_name => $4, positional_args => array[$5::json, $6::json, $7::json], app_version => $1, application_name => $2)"
    ( contramap (\(v, _, _, _, _, _, _) -> v) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, a, _, _, _, _, _) -> a) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, n, _, _, _, _) -> n) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, _, q, _, _, _) -> q) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, _, _, x, _, _) -> x) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, _, _, _, y, _) -> y) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, _, _, _, _, _, z) -> z) (Encoders.param (Encoders.nonNullable Encoders.text))
    )
    (Decoders.singleRow (Decoders.column (Decoders.nullable Decoders.text)))

missingTableStmt :: Statement.Statement () ()
missingTableStmt =
  Statement.preparable
    "select * from outbox_store.no_such_table_probe"
    Encoders.noParams
    Decoders.noResult

orderStatusStmt :: Statement.Statement Int32 (Maybe Text)
orderStatusStmt =
  Statement.preparable
    "select notification_status from outbox_store.orders where order_id = $1"
    (Encoders.param (Encoders.nonNullable Encoders.int4))
    (Decoders.singleRow (Decoders.column (Decoders.nullable Decoders.text)))

orderCountStmt :: Statement.Statement Text Int64
orderCountStmt =
  Statement.preparable
    "select count(*) from outbox_store.orders where customer = $1"
    (Encoders.param (Encoders.nonNullable Encoders.text))
    (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))

orderIdsStmt :: Statement.Statement Text [Int32]
orderIdsStmt =
  Statement.preparable
    "select order_id from outbox_store.orders where customer = $1 order by order_id"
    (Encoders.param (Encoders.nonNullable Encoders.text))
    (Decoders.rowList (Decoders.column (Decoders.nonNullable Decoders.int4)))

withOutboxFixture :: IO Postgres.PostgresSystemDB -> IO (SomeTracer IO) -> (OutboxFixture IO -> IO b) -> IO b
withOutboxFixture getBackend getTracer body = do
  fresh <- UUID.V4.nextRandom
  let suffix = Text.take 12 (Text.filter (/= '-') (Text.pack (UUID.toString fresh)))
      version = "hs-ob-version-" <> suffix
      appName = "hs-ob-" <> suffix
      queueName = "ob-notify-q-" <> suffix
  config0 <- configFromEnv appName
  let config =
        config0
          { configAppVersion = Just version,
            configExecutorId = Just ("hs-ob-executor-" <> suffix),
            -- Cases observe their own queue through the supervisor; listening
            -- to all (the default) would sweep other tests' queue fixtures.
            configListenQueues = Just [queueName]
          }
  pool <- acquireAppDataSourceInFromEnv "outbox_store" config0.configDatabaseUrl 2
  schemaed <- runAppSession pool ensureOutboxSchema
  case schemaed of
    Left err -> fail (show err)
    Right () -> pure ()
  dbos <- newDBOS config
  let ds = toDataSource pool
  _ <- registerDataSource dbos ds >>= either (fail . show) pure
  faultVar <- newTVarIO TxOk
  sendFailsVar <- newTVarIO (0 :: Int)
  sentVar <- newTVarIO (0 :: Int)
  backend <- getBackend
  tracer <- getTracer
  let atomicKey = newWorkflowKey "place_order"
      notifyKey = newWorkflowKey "send_notification_workflow"
      consumeTxFault = atomically $ do
        fault <- readTVar faultVar
        writeTVar faultVar TxOk
        pure fault
      consumeSendFail = atomically $ do
        n <- readTVar sendFailsVar
        if n > 0
          then writeTVar sendFailsVar (n - 1) >> pure True
          else pure False
      insertOp (Tx run) cust item qty = do
        oid32 <- run insertStmt (cust, item, fromIntegral qty)
        fault <- consumeTxFault
        case fault of
          TxOk -> pure (fromIntegral oid32)
          TxThrowApp -> throwIO (userError "outbox probe: app throw after insert")
          TxThrowDb -> run missingTableStmt () >> pure (fromIntegral oid32)
      enqueueOp (Tx run) oid cust = do
        let args = (version, appName, "send_notification_workflow", queueName, decodeUtf8 (LBS.toStrict (Aeson.encode oid)), decodeUtf8 (LBS.toStrict (Aeson.encode cust)), decodeUtf8 (LBS.toStrict (Aeson.encode ("Widget B" :: Text))))
        wid <- run enqueueStmt args
        case wid of
          Just widText -> pure (WorkflowId widText)
          Nothing -> throwIO (userError "outbox probe: enqueue returned no id")
      markOp (Tx run) oid = run markStmt (fromIntegral oid)
      atomicWf :: forall exec. (Text, Text, Int) -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
      atomicWf = atomicBody ds insertOp markOp (atomically (modifyTVar sentVar (+ 1))) consumeSendFail
      notifyWf :: forall exec. Envelope -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
      notifyWf = notifyBody ds markOp (atomically (modifyTVar sentVar (+ 1))) consumeSendFail
  atomicRef <- registerWorkflowRef dbos atomicKey atomicWf >>= either (fail . show) pure
  notifyRef <- registerWorkflowRef dbos notifyKey notifyWf >>= either (fail . show) pure
  exec <- launch dbos >>= either (fail . show) pure
  _ <- registerQueue dbos queueName defaultQueueOptions NeverUpdate >>= either (fail . show) pure
  let fx =
        OutboxFixture
          { obDBOS = dbos,
            obExecutor = exec,
            obDataSource = ds,
            obAtomicRef = atomicRef,
            obNotifyRef = notifyRef,
            obFreshTag = \prefix -> pure (prefix <> "-" <> suffix),
            obFreshWid = \prefix -> pure (WorkflowId ("hs-ob-" <> prefix <> "-" <> suffix)),
            obTxInsert = insertOp,
            obTxEnqueue = enqueueOp,
            obTxMarkSent = markOp,
            obNoteSent = atomically (modifyTVar sentVar (+ 1)),
            obArmTxThrow = atomically (writeTVar faultVar TxThrowApp),
            obArmTxDbError = atomically (writeTVar faultVar TxThrowDb),
            obArmSendFail = \n -> atomically (writeTVar sendFailsVar n),
            obConsumeSendFail = consumeSendFail,
            obConsumeTxFault = consumeTxFault,
            obRunAtomic = \wid cust item qty -> runWorkflow exec atomicKey wid (Just (encodeWorkflowValue (cust, item, qty))),
            obPlaceEnqueued = placeEnqueued ds insertOp enqueueOp,
            obOrderIds = \tag -> do
              found <- runAppSession pool (Session.statement tag orderIdsStmt)
              case found of
                Left err -> fail (show err)
                Right oids -> pure (map fromIntegral oids),
            obFindNotification = findNotification dbos,
            obAwaitNotification = \wid -> do
              _ <- driveQueue dbos
              settled <- waitForWorkflow dbos wid
              case settled of
                Left err -> throwIO (userError (show err))
                Right _  -> pure ()
              row <- readRowShared (SomeSystemDB backend) wid
              pure (fmap (.workflowRecordStatus) row),
            obResumeNotification = \wid -> do
              resumed <- resumeWorkflow dbos wid Nothing
              case resumed of
                Left err -> throwIO (userError (show err))
                Right _  -> pure ()
              _ <- driveQueue dbos
              settled <- waitForWorkflow dbos wid
              case settled of
                Left err -> throwIO (userError (show err))
                Right _  -> pure ()
              row <- readRowShared (SomeSystemDB backend) wid
              pure (fmap (.workflowRecordStatus) row),
            obResumeAtomic = \wid -> do
              resumed <- resumeWorkflow dbos wid Nothing
              case resumed of
                Left err -> throwIO (userError (show err))
                Right _  -> pure ()
              settled <- waitForWorkflow dbos wid
              case settled of
                Left err -> throwIO (userError (show err))
                Right _  -> pure ()
              row <- readRowShared (SomeSystemDB backend) wid
              pure (fmap (.workflowRecordStatus) row),
            obOrderStatus = \oid -> do
              found <- runAppSession pool (Session.statement (fromIntegral oid) orderStatusStmt)
              case found of
                Left err -> fail (show err)
                Right status -> pure status,
            obOrderCount = \tag -> do
              found <- runAppSession pool (Session.statement tag orderCountStmt)
              case found of
                Left err -> fail (show err)
                Right n -> pure (fromIntegral n),
            obSentCount = readTVarIO sentVar,
            obReadRow = readRowShared (SomeSystemDB backend),
            obSystemDB = SomeSystemDB backend
          }
  bracket (pure ()) (\_ -> shutdown dbos >> releaseAppDataSource pool) (\_ -> body fx)
