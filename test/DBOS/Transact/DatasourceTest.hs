{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Mirrors the datasource test modules of the oracles, name for name:
-- TypeScript @knex-datasource@ and Python @tests/test_datasource.py@.
-- Scenarios are written once against a fake 'DataSource' (STM maps, so
-- the same fake runs live and under IOSim) and asserted in the trees
-- here (live) and in 'DatasourceTestSim' (sim). Local copies of the
-- connection builders are deliberate, as in sibling sim trees.
module DBOS.Transact.DatasourceTest
  ( tests,
    DsFixture (..),
    FakeDs (..),
    mkFakeDs,
    scenarioBodyFailureRecorded,
    scenarioCommitReplay,
    scenarioDeleteCheckpoints,
    scenarioOwnershipMoved,
    scenarioPrecheckRetry,
    scenarioRunsOutside,
    scenarioErrorReplays,
    scenarioRetryThenSuccess,
    scenarioConflictAdopts,
    scenarioInStepRefused,
    scenarioCaptureRefused,
  )
where

import DBOS.Prelude
import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM, StrictTVar, atomically, modifyTVar, newTVarIO, readTVar, readTVarIO, writeTVar)
import Control.Monad.Class.MonadTime (MonadTime)
import Control.Monad.Class.MonadTimer (MonadDelay)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Int (Int64)
import Data.Text (Text)
import Data.Text qualified as Text
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import DBOS.SystemDB (BackendErrorKind (..))
import DBOS.SystemDB (BackendErrorKind (..), NewWorkflow (..), Submission (..), SystemDB (..), newWorkflow)
import DBOS.SystemDB qualified as SysDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  ( AppDataSource,
    BackendError (..),
    Connection,
    Ctx,
    DataSource (..),
    DBOS,
    EngineOnly,
    Error (..),
    Identity (..),
    IsolationLevel (..),
    Owner (..),
    RecordedOutcome (..),
    Serializer (..),
    SerializedWorkflowValue (..),
    SomeSystemDB (..),
    TransactionConfig (..),
    Tx (..),
    WorkflowId (..),
    acquireAppDataSource,
    acquireAppDataSourceIn,
    application,
    beginSql,
    clearDBOSCheckpoints,
    configNew,
    encodeErrorText,
    encodeWorkflowValue,
    firstStepStatus,
    launchOn,
    newConnection,
    newCtx,
    newDBOS,
    newWorkflowState,
    nextExecutionIdentity,
    nextStepMarker,
    nullTracer,
    registerDBOSDataSource,
    releaseAppDataSource,
    renderTransactError,
    runAppSession,
    runTransaction,
    runTransactionOutside,
    secondsDuration,
    toDataSource,
    transactionConfigDefault,
    uuidEntropy,
    uuidWorkflowId,
    verifyAppDataSource,
    withAttempt,
    withSystemDB,
  )
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

-- | How a tree instantiation builds its world: contexts over any backend
-- plus a fresh fake datasource per case (each case owns its rows).
data DsFixture m = DsFixture
  { dsFixtureMkCtx :: Text -> m (Ctx m),
    dsFixtureMkDs :: m (FakeDs m)
  }

-- | The STM-backed fake: checkpoint rows, body-run counter, injected
-- transient failures, and a one-shot simulated concurrent winner.
data FakeDs m = FakeDs
  { fakeSource :: DataSource m,
    fakeRows :: StrictTVar m (Map (Text, Int) RecordedOutcome),
    fakeNames :: StrictTVar m (Map (Text, Int) Text),
    fakeRuns :: StrictTVar m Int,
    fakeTransients :: StrictTVar m Int,
    fakeConflictOnce :: StrictTVar m Bool
  }

widText :: WorkflowId -> Text
widText (WorkflowId text) = text

transientError :: BackendError
transientError =
  BackendError
    { backendMessage = "serialization failure",
      backendSqlState = Just "40001",
      backendKind = Transient
    }

mkFakeDs :: MonadSTM m => m (FakeDs m)
mkFakeDs = do
  rows <- newTVarIO Map.empty
  names <- newTVarIO Map.empty
  runs <- newTVarIO 0
  transients <- newTVarIO 0
  conflict <- newTVarIO False
  let fakeTx = Tx (\_ _ -> error "FakeDs: statements unsupported in this fake")
      source =
        DataSource
          { dsName = "test-app-db",
            dsSchema = "dbos",
            dsCheck = \wid _name step -> do
              pending <- readTVarIO transients
              if pending > 0
                then atomically (modifyTVar transients (subtract 1)) >> pure (Left transientError)
                else Right . Map.lookup (widText wid, step) <$> readTVarIO rows,
            dsWithTransaction = \_isolation action -> do
              pending <- readTVarIO transients
              if pending > 0
                then atomically (modifyTVar transients (subtract 1)) >> pure (Left transientError)
                else Right <$> action fakeTx,
            dsRecordOutput = \_tx wid name step output -> atomically $ do
              existing <- readTVar rows
              priorNames <- readTVar names
              winner <- readTVar conflict
              case Map.lookup (widText wid, step) existing of
                Just _ -> pure False
                Nothing
                  | winner -> do
                      writeTVar conflict False
                      writeTVar rows (Map.insert (widText wid, step) (RecordedOutput (encodeWorkflowValue ("winner" :: Text)).serializedText) existing)
                      writeTVar names (Map.insert (widText wid, step) name priorNames)
                      pure False
                  | otherwise -> do
                      writeTVar rows (Map.insert (widText wid, step) (RecordedOutput output) existing)
                      writeTVar names (Map.insert (widText wid, step) name priorNames)
                      pure True,
            dsRecordError = \_tx wid name step message -> atomically $ do
              existing <- readTVar rows
              priorNames <- readTVar names
              case Map.lookup (widText wid, step) existing of
                Just _ -> pure False
                Nothing -> do
                  writeTVar rows (Map.insert (widText wid, step) (RecordedError message) existing)
                  writeTVar names (Map.insert (widText wid, step) name priorNames)
                  pure True,
            dsStepName = \wid step -> Right . Map.lookup (widText wid, step) <$> readTVarIO names,
            dsDeleteCheckpoints = \wid step -> atomically $ do
              existing <- readTVar rows
              writeTVar rows (Map.filterWithKey (\(w, s) _ -> not (w == widText wid && s >= step)) existing)
              pure (Right ())
          }
  pure (FakeDs source rows names runs transients conflict)

protoConfig :: TransactionConfig
protoConfig = TransactionConfig {txName = Just "proto_step", txIsolation = Just ReadCommitted}

-- | Python @test_sync_ds_records_and_replays@: a fresh execution replays
-- the recorded output without re-running the body.
scenarioCommitReplay :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> m (Either (Error EngineOnly) Text, Either (Error EngineOnly) Text, Int)
scenarioCommitReplay fx = do
  fake <- fx.dsFixtureMkDs
  let counted _ = atomically (modifyTVar fake.fakeRuns (+ 1)) >> pure (Right "v1")
  ctx1 <- fx.dsFixtureMkCtx "ds-wf-1"
  first <- runTransaction fake.fakeSource ctx1 protoConfig counted
  ctx2 <- fx.dsFixtureMkCtx "ds-wf-1"
  second <- runTransaction fake.fakeSource ctx2 protoConfig counted
  runs <- readTVarIO fake.fakeRuns
  pure (first, second, runs)

-- | Python @test_sync_ds_records_and_replays_errors@ (replay half): a
-- recorded failure decodes back to itself. The record half follows once
-- the body-failure channel lands.
scenarioErrorReplays :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> m (Either (Error Text) Text)
scenarioErrorReplays fx = do
  fake <- fx.dsFixtureMkDs
  atomically (writeTVar fake.fakeRows (Map.singleton ("ds-wf-2", 0) (RecordedError (encodeErrorText (application ("boom" :: Text) :: Error Text)))))
  ctx <- fx.dsFixtureMkCtx "ds-wf-2"
  runTransaction fake.fakeSource ctx protoConfig (\_ -> pure (Right "unused"))

-- | Python @test_sync_ds_retries_on_serialization_error@: two retriable
-- failures, then success, with the injections consumed.
scenarioRetryThenSuccess :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> m (Either (Error EngineOnly) Text, Int)
scenarioRetryThenSuccess fx = do
  fake <- fx.dsFixtureMkDs
  atomically (writeTVar fake.fakeTransients 2)
  ctx <- fx.dsFixtureMkCtx "ds-wf-3"
  result <- runTransaction fake.fakeSource ctx protoConfig (\_ -> pure (Right "v"))
  left <- readTVarIO fake.fakeTransients
  pure (result, left)

-- | Python @test_sync_ds_conflicts_when_duplicate_execution_wins@: a
-- concurrent winner committed first, so this execution adopts it.
scenarioConflictAdopts :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> m (Either (Error EngineOnly) Text)
scenarioConflictAdopts fx = do
  fake <- fx.dsFixtureMkDs
  atomically (writeTVar fake.fakeConflictOnce True)
  ctx <- fx.dsFixtureMkCtx "ds-wf-4"
  runTransaction fake.fakeSource ctx protoConfig (\_ -> pure (Right "loser"))

-- | A datasource call inside a step body is refused and records nothing
-- (the @InsideStep "transaction"@ shape of @Event.hs@).
scenarioInStepRefused :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> m (Either (Error EngineOnly) Text, Int)
scenarioInStepRefused fx = do
  fake <- fx.dsFixtureMkDs
  ctx <- fx.dsFixtureMkCtx "ds-wf-5"
  marker <- nextStepMarker ctx
  result <- withAttempt ctx marker (firstStepStatus 0) $ \inner ->
    runTransaction fake.fakeSource inner protoConfig (\_ -> pure (Right "x"))
  rows <- readTVarIO fake.fakeRows
  pure (result, Map.size rows)

-- | The captured-parent shape of the same leaf violation: the call reaches
-- through a context whose scope field predates the running body, so the
-- shared depth counter reports it. Refused with 'InsideStep' before
-- anything is written.
scenarioCaptureRefused :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> m (Either (Error EngineOnly) Text, Int)
scenarioCaptureRefused fx = do
  fake <- fx.dsFixtureMkDs
  ctx <- fx.dsFixtureMkCtx "ds-wf-5-captured"
  marker <- nextStepMarker ctx
  result <- withAttempt ctx marker (firstStepStatus 0) $ \_stepped ->
    runTransaction fake.fakeSource ctx protoConfig (\_ -> pure (Right "x"))
  rows <- readTVarIO fake.fakeRows
  pure (result, Map.size rows)

-- | The record half: a body failure records, and replay returns it
-- without re-running the body.
scenarioBodyFailureRecorded :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> m (Either (Error Text) Text, Either (Error Text) Text, Int)
scenarioBodyFailureRecorded fx = do
  fake <- fx.dsFixtureMkDs
  let failing _ = pure (Left (application "boom"))
      counted _ = atomically (modifyTVar fake.fakeRuns (+ 1)) >> pure (Right "v")
  ctx1 <- fx.dsFixtureMkCtx "ds-wf-6"
  first <- runTransaction fake.fakeSource ctx1 protoConfig failing
  ctx2 <- fx.dsFixtureMkCtx "ds-wf-6"
  second <- runTransaction fake.fakeSource ctx2 protoConfig counted
  runs <- readTVarIO fake.fakeRuns
  pure (first, second, runs)

-- | A transient pre-check read is retried, then the transaction runs.
scenarioPrecheckRetry :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> m (Either (Error EngineOnly) Text, Int)
scenarioPrecheckRetry fx = do
  fake <- fx.dsFixtureMkDs
  atomically (writeTVar fake.fakeTransients 1)
  ctx <- fx.dsFixtureMkCtx "ds-wf-8"
  result <- runTransaction fake.fakeSource ctx protoConfig (\_ -> pure (Right "v"))
  left <- readTVarIO fake.fakeTransients
  pure (result, left)

-- | Python @test_sync_ds_runs_outside_workflow@: outside any workflow the
-- body runs transactionally (transients retried) but checkpoints nothing.
-- Takes only the fake: no context exists out here.
scenarioRunsOutside :: (MonadSTM m, MonadDelay m, MonadCatch m) => m (FakeDs m) -> m (Either BackendError Text, Int, Int)
scenarioRunsOutside mkDs = do
  fake <- mkDs
  atomically (writeTVar fake.fakeTransients 1)
  result <- runTransactionOutside fake.fakeSource protoConfig (\_ -> atomically (modifyTVar fake.fakeRuns (+ 1)) >> pure "v")
  rows <- readTVarIO fake.fakeRows
  runs <- readTVarIO fake.fakeRuns
  pure (result, Map.size rows, runs)

-- | Python completion clearing (`delete_checkpoints` shape): deleting
-- from a step drops later checkpoints (earlier ones stay, replaying),
-- and deleting everything re-runs the body.
scenarioDeleteCheckpoints :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> m (Either (Error EngineOnly) Text, Either (Error EngineOnly) Text, Either (Error EngineOnly) Text, Either (Error EngineOnly) Text, Int)
scenarioDeleteCheckpoints fx = do
  fake <- fx.dsFixtureMkDs
  let counted _ = atomically (modifyTVar fake.fakeRuns (+ 1)) >> pure (Right "v")
      clean wid step = do
        cleared <- fake.fakeSource.dsDeleteCheckpoints (WorkflowId wid) step
        case cleared of
          Left err -> pure (Left (ErrorSystemDatabase (SysDB.Backend err)))
          Right () -> pure (Right "cleaned")
  ctx1 <- fx.dsFixtureMkCtx "ds-wf-9"
  first <- runTransaction fake.fakeSource ctx1 protoConfig counted
  second <- runTransaction fake.fakeSource ctx1 protoConfig counted
  _ <- clean "ds-wf-9" 1
  ctx2 <- fx.dsFixtureMkCtx "ds-wf-9"
  third <- runTransaction fake.fakeSource ctx2 protoConfig counted
  _ <- clean "ds-wf-9" 0
  ctx3 <- fx.dsFixtureMkCtx "ds-wf-9"
  fourth <- runTransaction fake.fakeSource ctx3 protoConfig counted
  runs <- readTVarIO fake.fakeRuns
  pure (first, second, third, fourth, runs)

-- | Python @test_sync_ds_rolls_back_once_ownership_moves@: the checkpoint
-- lost to an execution owned by another executor, so this execution stops
-- instead of adopting. Staged through the 'SystemDB' class so both stacks
-- run it: the row carries a foreign executor on live and sim alike.
scenarioOwnershipMoved :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> Text -> m (Either (Error EngineOnly) Text)
scenarioOwnershipMoved fx wfId = do
  fake <- fx.dsFixtureMkDs
  stage <- fx.dsFixtureMkCtx wfId
  started <- withSystemDB stage (\db -> initWorkflow db ((newWorkflow wfId) {newWorkflowExecutorId = Just "other-executor"}) Nothing Fresh Nothing)
  case started of
    Left err -> pure (Left (ErrorSystemDatabase err))
    Right _ -> do
      atomically (writeTVar fake.fakeConflictOnce True)
      ctx <- fx.dsFixtureMkCtx wfId
      runTransaction fake.fakeSource ctx protoConfig (\_ -> pure (Right "loser"))

-- | The registry's created-before-launch rule and completion clearing,
-- over an instance that never launches: registration is open, a duplicate
-- name is refused, and clearing empties the fake's rows. The transaction
-- runs over the suite's own connection; only registration and clearing go
-- through the instance.
scenarioRegistryLifecycle :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadMVar m) => DBOS m -> (Text -> m (Ctx m)) -> Text -> m (Either (Error EngineOnly) (), Either (Error EngineOnly) (), Int, Int)
scenarioRegistryLifecycle dbos mkCtx wid = do
  fake <- mkFakeDs
  first <- registerDBOSDataSource dbos fake.fakeSource
  duplicate <- registerDBOSDataSource dbos fake.fakeSource
  ctx <- mkCtx wid
  _ <- (runTransaction fake.fakeSource ctx protoConfig (\_ -> pure (Right ("v" :: Text))) :: m (Either (Error Text) Text))
  rowsBefore <- readTVarIO fake.fakeRows
  clearDBOSCheckpoints dbos (WorkflowId wid)
  rowsAfter <- readTVarIO fake.fakeRows
  pure (first, duplicate, Map.size rowsBefore, Map.size rowsAfter)

-- | A connection over the suite backend. The engine under test touches no
-- per-test tables — only their own workflow ids.
acquireDsBackend :: IO Postgres.PostgresSystemDB
acquireDsBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

dsTestIdentity :: Identity
dsTestIdentity =
  Identity
    { identityAppName = "test-app",
      identityAppVersion = "1.0.0",
      identityExecutorId = "test-executor",
      identityAppId = ""
    }

dsConnOver :: Postgres.PostgresSystemDB -> IO (Connection IO)
dsConnOver backend = do
  instanceId <- uuidWorkflowId
  newConnection
    (SomeSystemDB backend)
    RustSerde
    (Just "test-app")
    (secondsDuration 1)
    OwnerApplication
    instanceId
    uuidWorkflowId
    uuidEntropy
    nullTracer

dsCtxOver :: Postgres.PostgresSystemDB -> Text -> IO (Ctx IO)
dsCtxOver backend workflowText = do
  conn <- dsConnOver backend
  identity <- nextExecutionIdentity conn
  state <- newWorkflowState workflowText Nothing identity
  newCtx conn dsTestIdentity state

-- | An application pool over the same database the suite runs on. The
-- checkpoint table is never created (verify-only), so binding tests use
-- scratch tables they create and drop themselves.
acquireProbeApp :: IO AppDataSource
acquireProbeApp = do
  config <- Postgres.configFromEnv
  acquireAppDataSource config.configUrl 2

-- | Bracket an application pool around one case, releasing even on failure.
withProbeApp :: forall a. (AppDataSource -> IO a) -> IO a
withProbeApp action = do
  app <- acquireProbeApp
  outcome <- try (action app) :: IO (Either SomeException a)
  releaseAppDataSource app
  either throwIO pure outcome

-- | A scratch table name this case owns: prefix plus fresh hex.
freshProbeTable :: IO Text
freshProbeTable = ("ds_probe_" <>) . Text.filter (/= '-') <$> uuidWorkflowId

-- | Run one setup/teardown statement, failing the case on a database error.
execProbe :: AppDataSource -> Text -> IO ()
execProbe app sql = do
  result <- runAppSession app (Session.statement () (Statement.preparable sql Encoders.noParams Decoders.noResult))
  case result of
    Left err -> assertFailure ("probe setup failed: " <> show err)
    Right () -> pure ()

-- | Bracket a scratch table around one case, dropping even on failure.
withProbeTable :: forall a. AppDataSource -> (Text -> IO a) -> IO a
withProbeTable app action = do
  table <- freshProbeTable
  execProbe app ("CREATE TABLE " <> table <> " (wid text, bal int)")
  outcome <- try (action table) :: IO (Either SomeException a)
  execProbe app ("DROP TABLE " <> table)
  either throwIO pure outcome

-- | Row count of a scratch table.
countProbe :: AppDataSource -> Text -> IO Int64
countProbe app table = do
  result <-
    runAppSession
      app
      (Session.statement () (Statement.preparable ("SELECT COUNT(*) FROM " <> table) Encoders.noParams (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))))
  case result of
    Left err -> assertFailure ("probe read failed: " <> show err) >> pure 0
    Right count -> pure count

-- | The transaction bracket at a fixed result type. A pattern binding
-- does not generalize a RankN field, so this alias pins the instantiation
-- each probe body needs.
runProbeTx :: DataSource IO -> Maybe IsolationLevel -> (Tx IO -> IO ()) -> IO (Either BackendError ())
runProbeTx ds iso body =
  let DataSource {dsWithTransaction = withTx} = ds
   in withTx iso body

-- | A probe pool over a scratch schema this case owns, with the checkpoint
-- table the live delete path writes to. The suite's own schema never has one
-- (verify refuses), so the case creates and drops it.
withProbeCheckpointApp :: forall a. (AppDataSource -> IO a) -> IO a
withProbeCheckpointApp action = do
  config <- Postgres.configFromEnv
  schema <- ("ds_ckpt_" <>) . Text.filter (/= '-') <$> uuidWorkflowId
  app <- acquireAppDataSourceIn schema config.configUrl 2
  execProbe app ("CREATE SCHEMA " <> schema)
  execProbe app ("CREATE TABLE " <> schema <> ".transaction_completion (workflow_id text not null, step_name text not null, function_num int not null, output text, error text, primary key (workflow_id, function_num))")
  outcome <- try (action app) :: IO (Either SomeException a)
  execProbe app ("DROP SCHEMA " <> schema <> " CASCADE")
  releaseAppDataSource app
  either throwIO pure outcome

tests :: TestTree
tests =
  withResource acquireDsBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    testGroup
      "Datasource"
      [ testCase "a default config names nothing and takes the database default isolation" $ do
          transactionConfigDefault @?= TransactionConfig {txName = Nothing, txIsolation = Nothing},
        testCase "a transaction commits once and replays without re-running" $ do
          backend <- getBackend
          let fx = DsFixture (dsCtxOver backend) mkFakeDs
          (first, second, runs) <- scenarioCommitReplay fx
          first @?= Right "v1"
          second @?= Right "v1"
          runs @?= 1,
        testCase "a recorded failure decodes back to itself" $ do
          backend <- getBackend
          let fx = DsFixture (dsCtxOver backend) mkFakeDs
          result <- scenarioErrorReplays fx
          result @?= Left (application "boom"),
        testCase "a body failure records, and replay returns it without re-running" $ do
          backend <- getBackend
          let fx = DsFixture (dsCtxOver backend) mkFakeDs
          (first, second, runs) <- scenarioBodyFailureRecorded fx
          first @?= Left (application "boom")
          second @?= Left (application "boom")
          runs @?= 0,
        testCase "retriable failures are retried, then the body runs" $ do
          backend <- getBackend
          let fx = DsFixture (dsCtxOver backend) mkFakeDs
          (result, left) <- scenarioRetryThenSuccess fx
          result @?= Right "v"
          left @?= 0,
        testCase "a duplicate execution that won is adopted" $ do
          backend <- getBackend
          let fx = DsFixture (dsCtxOver backend) mkFakeDs
          result <- scenarioConflictAdopts fx
          result @?= Right "winner",
        testCase "a call inside a step is refused and records nothing" $ do
          backend <- getBackend
          let fx = DsFixture (dsCtxOver backend) mkFakeDs
          (result, rowCount) <- scenarioInStepRefused fx
          result @?= Left (InsideStep "transaction")
          rowCount @?= 0,
        testCase "a call through a captured parent is refused and records nothing" $ do
          backend <- getBackend
          let fx = DsFixture (dsCtxOver backend) mkFakeDs
          (result, rowCount) <- scenarioCaptureRefused fx
          result @?= Left (InsideStep "transaction")
          rowCount @?= 0,
        testCase "beginSql names every isolation level" $ do
          beginSql Nothing @?= "BEGIN"
          beginSql (Just ReadUncommitted) @?= "BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
          beginSql (Just ReadCommitted) @?= "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED"
          beginSql (Just RepeatableRead) @?= "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"
          beginSql (Just Serializable) @?= "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        -- IO only: needs a live pool; the sim has no SQL layer. No
        -- migration ever creates the table, so absence is the assertion.
        testCase "verify refuses a database without the checkpoint table" $ do
          withProbeApp $ \app -> do
            verified <- verifyAppDataSource app
            case verified of
              Left _ -> pure ()
              Right () -> assertFailure "expected refusal: nothing creates the checkpoint table",
        -- IO only: needs a live pool and scratch DDL; the sim has no SQL layer.
        testCase "a committed transaction leaves every row" $ do
          withProbeApp $ \app -> do
            withProbeTable app $ \table -> do
              let insert =
                    Statement.preparable
                      ("INSERT INTO " <> table <> " (wid, bal) VALUES ('a', 1)")
                      Encoders.noParams
                      Decoders.noResult
              result <- runProbeTx (toDataSource app) Nothing (\(Tx run) -> run insert () >> run insert ())
              case result of
                Left err -> assertFailure ("commit failed: " <> show err)
                Right () -> pure ()
              count <- countProbe app table
              count @?= 2,
        -- IO only: needs a live pool and scratch DDL; the sim has no SQL layer.
        testCase "a thrown body rolls everything back" $ do
          withProbeApp $ \app -> do
            withProbeTable app $ \table -> do
              let insert =
                    Statement.preparable
                      ("INSERT INTO " <> table <> " (wid, bal) VALUES ('a', 1)")
                      Encoders.noParams
                      Decoders.noResult
              attempted <- try (runProbeTx (toDataSource app) Nothing (\(Tx run) -> run insert () >> throwIO (userError "boom"))) :: IO (Either IOError (Either BackendError ()))
              case attempted of
                Left _ -> pure ()
                Right _ -> assertFailure "expected the body failure to escape"
              count <- countProbe app table
              count @?= 0,
        testCase "an ownership move stops the execution instead of adopting" $ do
          backend <- getBackend
          let fx = DsFixture (dsCtxOver backend) mkFakeDs
          wfId <- (("ds-own-" <>) . Text.filter (/= '-')) <$> uuidWorkflowId
          result <- scenarioOwnershipMoved fx wfId
          case result of
            Left err -> assertBool "names the owning executor" ("other-executor" `Text.isInfixOf` renderTransactError err)
            Right _ -> assertFailure "expected the ownership conflict to stop the execution",
        testCase "a transient pre-check read is retried, then the transaction runs" $ do
          backend <- getBackend
          let fx = DsFixture (dsCtxOver backend) mkFakeDs
          (result, left) <- scenarioPrecheckRetry fx
          result @?= Right "v"
          left @?= 0,
        testCase "outside a workflow the body runs transactionally and checkpoints nothing" $ do
          (result, rowCount, runs) <- scenarioRunsOutside mkFakeDs
          result @?= Right "v"
          rowCount @?= 0
          runs @?= 1,
        testCase "deleting from a step drops later checkpoints and re-runs" $ do
          backend <- getBackend
          let fx = DsFixture (dsCtxOver backend) mkFakeDs
          (first, second, third, fourth, runs) <- scenarioDeleteCheckpoints fx
          (first, second, third, fourth) @?= (Right "v", Right "v", Right "v", Right "v")
          runs @?= 3,
        -- IO only: the live binding's delete path, over a scratch schema so
        -- the case owns its checkpoint table.
        testCase "deleting from a step drops later checkpoints on the live datasource" $ do
          backend <- getBackend
          wfId <- (("ds-live-del-" <>) . Text.filter (/= '-')) <$> uuidWorkflowId
          withProbeCheckpointApp $ \app -> do
            let ds = toDataSource app
                expected = RecordedOutput (encodeWorkflowValue ("v" :: Text)).serializedText
            ctx <- dsCtxOver backend wfId
            first <- runTransaction ds ctx protoConfig (\_ -> pure (Right ("v" :: Text))) :: IO (Either (Error EngineOnly) Text)
            second <- runTransaction ds ctx protoConfig (\_ -> pure (Right ("v" :: Text))) :: IO (Either (Error EngineOnly) Text)
            (first, second) @?= (Right "v", Right "v")
            ds.dsCheck (WorkflowId wfId) "proto_step" 0 >>= (@?= Right (Just expected))
            ds.dsCheck (WorkflowId wfId) "proto_step" 1 >>= (@?= Right (Just expected))
            ds.dsDeleteCheckpoints (WorkflowId wfId) 1 >>= (@?= Right ())
            ds.dsCheck (WorkflowId wfId) "proto_step" 0 >>= (@?= Right (Just expected))
            ds.dsCheck (WorkflowId wfId) "proto_step" 1 >>= (@?= Right Nothing)
            ds.dsDeleteCheckpoints (WorkflowId wfId) 0 >>= (@?= Right ())
            ds.dsCheck (WorkflowId wfId) "proto_step" 0 >>= (@?= Right Nothing),
        -- IO only: the live binding, over a scratch checkpoint table.
        testCase "a transaction recorded under another name at the same step is refused" $ do
          backend <- getBackend
          wfId <- (("ds-live-name-" <>) . Text.filter (/= '-')) <$> uuidWorkflowId
          withProbeCheckpointApp $ \app -> do
            let ds = toDataSource app
                first = TransactionConfig {txName = Just "first_step", txIsolation = Just ReadCommitted}
                second = TransactionConfig {txName = Just "second_step", txIsolation = Just ReadCommitted}
            ctx1 <- dsCtxOver backend wfId
            written <- runTransaction ds ctx1 first (\_ -> pure (Right ("v" :: Text))) :: IO (Either (Error EngineOnly) Text)
            written @?= Right "v"
            -- A reordered or renamed body reaches the same step slot under a
            -- different name: replay must refuse instead of returning "v".
            ctx2 <- dsCtxOver backend wfId
            replayed <- runTransaction ds ctx2 second (\_ -> pure (Right ("changed" :: Text))) :: IO (Either (Error EngineOnly) Text)
            case replayed of
              Left (ErrorSystemDatabase (SysDB.UnexpectedStep {stepId = recordedStep, expected = want, recorded = got})) -> do
                recordedStep @?= 0
                want @?= "second_step"
                got @?= "first_step"
              other -> assertFailure ("expected the recorded name to be refused, got: " <> show other),
        testCase "the datasource registry refuses duplicates and clears checkpoints" $ do
          backend <- getBackend
          dbos <- newDBOS (configNew "ds-registry" "postgres://unused")
          (first, duplicate, rowsBefore, rowsAfter) <- scenarioRegistryLifecycle dbos (dsCtxOver backend) "ds-wf-registry"
          first @?= Right ()
          case duplicate of
            Left err -> assertBool "refusal names the datasource" ("datasource" `Text.isInfixOf` renderTransactError err)
            Right () -> assertFailure "expected the duplicate registration to be refused"
          rowsBefore @?= 1
          rowsAfter @?= 0
      ]
