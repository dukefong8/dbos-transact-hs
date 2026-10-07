{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Mirrors the datasource test modules of the oracles, name for name:
-- TypeScript @knex-datasource@ and Python @tests/test_datasource.py@.
-- Scenarios and checks live in 'DBOS.Transact.DatasourceCases' and run here
-- over Postgres system rows (and in 'DatasourceTestSim' over the mock or
-- in-memory backend). The probe cases are IO-only: they need a live pool
-- and scratch DDL, and the sim has no SQL layer.
module DBOS.Transact.DatasourceTest
  ( tests,
  )
where

import DBOS.DualStack (liveCase)
import DBOS.Prelude
import Data.Int (Int64)
import Data.Text qualified as Text
import DBOS.SystemDB qualified as SysDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  ( AppDataSource,
    DataSource (..),
    EngineOnly,
    Error (..),
    IsolationLevel (..),
    Serializer (..),
    TransactionConfig (..),
    Tx (..),
    WorkflowCtx,
    WorkflowId (..),
    acquireAppDataSource,
    acquireAppDataSourceIn,
    configNew,
    encodeWorkflowValue,
    newDBOS,
    nullTracer,
    releaseAppDataSource,
    runAppSession,
    runTxStep,
    secondsDuration,
    toDataSource,
    verifyAppDataSource,
  )
import DBOS.SystemDB.Error (BackendError)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Datasource (RecordedOutcome (..))
import DBOS.Transact.Context (withWorkflow)
import DBOS.Transact.Connection
  ( newConnection,
    uuidWorkflowId,
    Connection,
    Owner (..),
    SomeSystemDB (..)
  )
import DBOS.SystemDB.Retry (uuidEntropy)
import DBOS.Transact.DatasourceCases
  ( DsFixture (..),
    RegistryFixture (..),
    checkBeginSql,
    checkBodyFailureRecorded,
    checkCaptureRefused,
    checkCommitReplay,
    checkConflictAdopts,
    checkDefaultConfig,
    checkDeleteCheckpoints,
    checkErrorReplays,
    checkOwnershipMoved,
    checkPrecheckRetry,
    checkRegistryLifecycle,
    checkRetryThenSuccess,
    checkRunsOutside,
    mkFakeDs,
    protoConfig,
    scenarioBeginSql,
    scenarioBodyFailureRecorded,
    scenarioCaptureRefused,
    scenarioCommitReplay,
    scenarioConflictAdopts,
    scenarioDefaultConfig,
    scenarioDeleteCheckpoints,
    scenarioErrorReplays,
    scenarioOwnershipMoved,
    scenarioPrecheckRetry,
    scenarioRegistryLifecycle,
    scenarioRetryThenSuccess,
    scenarioRunsOutside,
  )
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertFailure, testCase, (@?=))

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

dsRunOver :: Postgres.PostgresSystemDB -> Text -> (forall exec. WorkflowCtx exec IO -> IO a) -> IO a
dsRunOver backend workflowText action = do
  conn <- dsConnOver backend
  withWorkflow conn dsTestIdentity (WorkflowId workflowText) Nothing action

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
    let leaf :: String -> (DsFixture IO -> IO a) -> (a -> Either String ()) -> TestTree
        leaf name scen check = liveCase (mkLiveFixture =<< getBackend) name scen check
        mkLiveFixture backend = pure (DsFixture (dsRunOver backend) mkFakeDs)
     in testGroup
          "Datasource"
          [ testCase "a default config names nothing and takes the database default isolation" (either fail pure (checkDefaultConfig scenarioDefaultConfig)),
            leaf "a transaction commits once and replays without re-running" scenarioCommitReplay checkCommitReplay,
            leaf "a recorded failure decodes back to itself" scenarioErrorReplays checkErrorReplays,
            leaf "a body failure records, and replay returns it without re-running" scenarioBodyFailureRecorded checkBodyFailureRecorded,
            leaf "retriable failures are retried, then the body runs" scenarioRetryThenSuccess checkRetryThenSuccess,
            leaf "a duplicate execution that won is adopted" scenarioConflictAdopts checkConflictAdopts,
            leaf "a call through a captured parent is refused and records nothing" scenarioCaptureRefused checkCaptureRefused,
            testCase "beginSql names every isolation level" (either fail pure (checkBeginSql scenarioBeginSql)),
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
            leaf "an ownership move stops the execution instead of adopting" (\fx -> (("ds-own-" <>) . Text.filter (/= '-')) <$> uuidWorkflowId >>= scenarioOwnershipMoved fx) checkOwnershipMoved,
            leaf "a transient pre-check read is retried, then the transaction runs" scenarioPrecheckRetry checkPrecheckRetry,
            leaf "outside a workflow the body runs transactionally and checkpoints nothing" (\_ -> scenarioRunsOutside mkFakeDs) checkRunsOutside,
            leaf "deleting from a step drops later checkpoints and re-runs" scenarioDeleteCheckpoints checkDeleteCheckpoints,
            -- IO only: the live binding's delete path, over a scratch schema so
            -- the case owns its checkpoint table.
            testCase "deleting from a step drops later checkpoints on the live datasource" $ do
              backend <- getBackend
              wfId <- (("ds-live-del-" <>) . Text.filter (/= '-')) <$> uuidWorkflowId
              withProbeCheckpointApp $ \app -> do
                let ds = toDataSource app
                    expected = RecordedOutput (encodeWorkflowValue ("v" :: Text)).serializedText
                (first, second) <-
                  dsRunOver backend wfId $ \wctx -> do
                    first <- runTxStep ds protoConfig wctx (\_ _ -> pure (Right ("v" :: Text))) :: IO (Either (Error EngineOnly) Text)
                    second <- runTxStep ds protoConfig wctx (\_ _ -> pure (Right ("v" :: Text))) :: IO (Either (Error EngineOnly) Text)
                    pure (first, second)
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
                written <- dsRunOver backend wfId $ \wctx ->
                  runTxStep ds first wctx (\_ _ -> pure (Right ("v" :: Text))) :: IO (Either (Error EngineOnly) Text)
                written @?= Right "v"
                -- A reordered or renamed body reaches the same step slot under a
                -- different name: replay must refuse instead of returning "v".
                replayed <- dsRunOver backend wfId $ \wctx ->
                  runTxStep ds second wctx (\_ _ -> pure (Right ("changed" :: Text))) :: IO (Either (Error EngineOnly) Text)
                case replayed of
                  Left (ErrorSystemDatabase (SysDB.UnexpectedStep {stepId = recordedStep, expected = want, recorded = got})) -> do
                    recordedStep @?= 0
                    want @?= "second_step"
                    got @?= "first_step"
                  other -> assertFailure ("expected the recorded name to be refused, got: " <> show other),
            liveCase
              (do
                backend <- getBackend
                dbos <- newDBOS (configNew "ds-registry" "postgres://unused")
                pure (RegistryFixture dbos (DsFixture (dsRunOver backend) mkFakeDs)))
              "the datasource registry refuses duplicates and clears checkpoints"
              (\(RegistryFixture dbos fx) -> scenarioRegistryLifecycle dbos fx "ds-wf-registry")
              checkRegistryLifecycle
          ]
