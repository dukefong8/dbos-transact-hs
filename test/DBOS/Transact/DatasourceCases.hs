{-# LANGUAGE OverloadedStrings #-}

-- | Shared datasource scenarios: one body per case, judged by one pure check
-- on each stack. The STM-backed fake ('FakeDs') runs live and under IOSim
-- alike — checkpoint rows, a body-run counter, injected transient failures,
-- and a one-shot simulated concurrent winner — and the fixture carries the
-- workflow-running capability, so scenarios only differ in idle backend.
-- The live tree ('DBOS.Transact.DatasourceTest') runs them over Postgres
-- system rows, the sim tree ('DBOS.Transact.DatasourceTestSim') over the
-- mock or in-memory backend, and both prove the same transactional record.
module DBOS.Transact.DatasourceCases
  ( DsFixture (..),
    RegistryFixture (..),
    FakeDs (..),
    mkFakeDs,
    runFixture,
    protoConfig,
    scenarioCommitReplay,
    scenarioErrorReplays,
    scenarioBodyFailureRecorded,
    scenarioRetryThenSuccess,
    scenarioConflictAdopts,
    scenarioCaptureRefused,
    scenarioPrecheckRetry,
    scenarioRunsOutside,
    scenarioDeleteCheckpoints,
    scenarioOwnershipMoved,
    scenarioRegistryLifecycle,
    scenarioDefaultConfig,
    scenarioBeginSql,
    checkCommitReplay,
    checkErrorReplays,
    checkBodyFailureRecorded,
    checkRetryThenSuccess,
    checkConflictAdopts,
    checkCaptureRefused,
    checkPrecheckRetry,
    checkRunsOutside,
    checkDeleteCheckpoints,
    checkOwnershipMoved,
    checkRegistryLifecycle,
    checkDefaultConfig,
    checkBeginSql,
  )
where

import DBOS.Prelude
import DBOS.Transact.Context (withSystemDB)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text qualified as Text
import DBOS.SystemDB (BackendErrorKind (..), NewWorkflow (..), Submission (..), SystemDB (..), newWorkflow)
import DBOS.SystemDB qualified as SysDB
import DBOS.Transact
  (
  DBOS,
  DataSource (..),
  EngineOnly,
  Error (..),
  IsolationLevel (..),
  SerializedWorkflowValue (..),
  TransactionConfig (..),
  Tx (..),
  WorkflowCtx,
  WorkflowId (..),
  application,
  encodeWorkflowValue,
  registerDataSource,
  runTxOutside,
  runTxStep,
  transactionConfigDefault,
  )
import DBOS.SystemDB.Error (BackendError (..))
import DBOS.Transact.Datasource (RecordedOutcome (..))
import DBOS.Transact.Datasource.Postgres (beginSql)
import DBOS.Transact.Instance (clearCheckpoints)
import DBOS.Transact.Error (encodeErrorText)
import DBOS.Transact.Context (firstStepStatus, nextWorkflowMarker, withStep)

-- | How a tree instantiation builds its world: contexts over any backend
-- plus a fresh fake datasource per case (each case owns its rows).
data DsFixture m = DsFixture
  { dsFixtureRun :: forall a. Text -> (forall exec. WorkflowCtx exec m -> m a) -> m a,
    dsFixtureMkDs :: m (FakeDs m)
  }

-- | A registry case's world: an unlaunched instance plus the datasource
-- fixture (registration is open before launch on both stacks).
data RegistryFixture m = RegistryFixture
  { rfDBOS :: DBOS m,
    rfDs :: DsFixture m
  }

-- | Run a case's transactions under a fresh scope: the rank-2 field is
-- read by pattern match because record-dot has no 'HasField' instance for
-- polymorphic fields.
runFixture :: DsFixture m -> Text -> (forall exec. WorkflowCtx exec m -> m a) -> m a
runFixture (DsFixture run _) = run

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
  let counted _ = atomically (modifyTVar fake.fakeRuns (+ 1)) >> pure (Right "v1" :: Either (Error EngineOnly) Text)
  first <- runFixture fx "ds-wf-1" $ \wctx -> runTxStep fake.fakeSource protoConfig wctx (\_ tx -> counted tx)
  second <- runFixture fx "ds-wf-1" $ \wctx -> runTxStep fake.fakeSource protoConfig wctx (\_ tx -> counted tx)
  runs <- readTVarIO fake.fakeRuns
  pure (first, second, runs)

-- | Python @test_sync_ds_records_and_replays_errors@ (replay half): a
-- recorded failure decodes back to itself. The record half follows once
-- the body-failure channel lands.
scenarioErrorReplays :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> m (Either (Error Text) Text)
scenarioErrorReplays fx = do
  fake <- fx.dsFixtureMkDs
  atomically (writeTVar fake.fakeRows (Map.singleton ("ds-wf-2", 0) (RecordedError (encodeErrorText (application ("boom" :: Text) :: Error Text)))))
  runFixture fx "ds-wf-2" $ \wctx -> runTxStep fake.fakeSource protoConfig wctx (\_ _ -> pure (Right ("unused" :: Text)))

-- | Python @test_sync_ds_retries_on_serialization_error@: two retriable
-- failures, then success, with the injections consumed.
scenarioRetryThenSuccess :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> m (Either (Error EngineOnly) Text, Int)
scenarioRetryThenSuccess fx = do
  fake <- fx.dsFixtureMkDs
  atomically (writeTVar fake.fakeTransients 2)
  result <- runFixture fx "ds-wf-3" $ \wctx -> runTxStep fake.fakeSource protoConfig wctx (\_ _ -> pure (Right ("v" :: Text)))
  left <- readTVarIO fake.fakeTransients
  pure (result, left)

-- | Python @test_sync_ds_conflicts_when_duplicate_execution_wins@: a
-- concurrent winner committed first, so this execution adopts it.
scenarioConflictAdopts :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> m (Either (Error EngineOnly) Text)
scenarioConflictAdopts fx = do
  fake <- fx.dsFixtureMkDs
  atomically (writeTVar fake.fakeConflictOnce True)
  runFixture fx "ds-wf-4" $ \wctx -> runTxStep fake.fakeSource protoConfig wctx (\_ _ -> pure (Right ("loser" :: Text)))

-- | The captured-parent shape of the same leaf violation: the call reaches
-- through a context whose scope field predates the running body, so the
-- shared depth counter reports it. Refused with 'InsideStep' before
-- anything is written.
scenarioCaptureRefused :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> m (Either (Error EngineOnly) Text, Int)
scenarioCaptureRefused fx = do
  fake <- fx.dsFixtureMkDs
  result <- runFixture fx "ds-wf-5-captured" $ \wctx -> do
    marker <- nextWorkflowMarker wctx
    withStep wctx marker (firstStepStatus 0) $ \_stepped ->
      runTxStep fake.fakeSource protoConfig wctx (\_ _ -> pure (Right ("x" :: Text)))
  rows <- readTVarIO fake.fakeRows
  pure (result, Map.size rows)

-- | The record half: a body failure records, and replay returns it
-- without re-running the body.
scenarioBodyFailureRecorded :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> m (Either (Error Text) Text, Either (Error Text) Text, Int)
scenarioBodyFailureRecorded fx = do
  fake <- fx.dsFixtureMkDs
  let failing _ = pure (Left (application ("boom" :: Text)))
      counted _ = atomically (modifyTVar fake.fakeRuns (+ 1)) >> pure (Right ("v" :: Text))
  first <- runFixture fx "ds-wf-6" $ \wctx -> runTxStep fake.fakeSource protoConfig wctx (\_ tx -> failing tx)
  second <- runFixture fx "ds-wf-6" $ \wctx -> runTxStep fake.fakeSource protoConfig wctx (\_ tx -> counted tx)
  runs <- readTVarIO fake.fakeRuns
  pure (first, second, runs)

-- | A transient pre-check read is retried, then the transaction runs.
scenarioPrecheckRetry :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> m (Either (Error EngineOnly) Text, Int)
scenarioPrecheckRetry fx = do
  fake <- fx.dsFixtureMkDs
  atomically (writeTVar fake.fakeTransients 1)
  result <- runFixture fx "ds-wf-8" $ \wctx -> runTxStep fake.fakeSource protoConfig wctx (\_ _ -> pure (Right ("v" :: Text)))
  left <- readTVarIO fake.fakeTransients
  pure (result, left)

-- | Python @test_sync_ds_runs_outside_workflow@: outside any workflow the
-- body runs transactionally (transients retried) but checkpoints nothing.
-- Takes only the fake: no context exists out here.
scenarioRunsOutside :: (MonadSTM m, MonadDelay m, MonadCatch m) => m (FakeDs m) -> m (Either BackendError Text, Int, Int)
scenarioRunsOutside mkDs = do
  fake <- mkDs
  atomically (writeTVar fake.fakeTransients 1)
  result <- runTxOutside fake.fakeSource protoConfig (\_ -> atomically (modifyTVar fake.fakeRuns (+ 1)) >> pure "v")
  rows <- readTVarIO fake.fakeRows
  runs <- readTVarIO fake.fakeRuns
  pure (result, Map.size rows, runs)

-- | Python completion clearing (`delete_checkpoints` shape): deleting
-- from a step drops later checkpoints (earlier ones stay, replaying),
-- and deleting everything re-runs the body.
scenarioDeleteCheckpoints :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> m (Either (Error EngineOnly) Text, Either (Error EngineOnly) Text, Either (Error EngineOnly) Text, Either (Error EngineOnly) Text, Int)
scenarioDeleteCheckpoints fx = do
  fake <- fx.dsFixtureMkDs
  let counted _ = atomically (modifyTVar fake.fakeRuns (+ 1)) >> pure (Right ("v" :: Text))
      clean wid step = do
        cleared <- fake.fakeSource.dsDeleteCheckpoints (WorkflowId wid) step
        case cleared of
          Left err -> pure (Left (ErrorSystemDatabase (SysDB.Backend err)))
          Right () -> pure (Right "cleaned")
  (first, second) <-
    runFixture fx "ds-wf-9" $ \wctx -> do
      first <- runTxStep fake.fakeSource protoConfig wctx (\_ tx -> counted tx)
      second <- runTxStep fake.fakeSource protoConfig wctx (\_ tx -> counted tx)
      pure (first, second)
  _ <- clean "ds-wf-9" 1
  third <- runFixture fx "ds-wf-9" $ \wctx -> runTxStep fake.fakeSource protoConfig wctx (\_ tx -> counted tx)
  _ <- clean "ds-wf-9" 0
  fourth <- runFixture fx "ds-wf-9" $ \wctx -> runTxStep fake.fakeSource protoConfig wctx (\_ tx -> counted tx)
  runs <- readTVarIO fake.fakeRuns
  pure (first, second, third, fourth, runs)

-- | Python @test_sync_ds_rolls_back_once_ownership_moves@: the checkpoint
-- lost to an execution owned by another executor, so this execution stops
-- instead of adopting. Staged through the 'SystemDB' class so both stacks
-- run it: the row carries a foreign executor on live and sim alike.
scenarioOwnershipMoved :: (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) => DsFixture m -> Text -> m (Either (Error EngineOnly) Text)
scenarioOwnershipMoved fx wfId = do
  fake <- fx.dsFixtureMkDs
  started <- runFixture fx wfId $ \wctx ->
    withSystemDB wctx (\db -> initWorkflow db ((newWorkflow wfId) {newWorkflowExecutorId = Just "other-executor"}) Nothing Fresh Nothing)

  case started of
    Left err -> pure (Left (ErrorSystemDatabase err))
    Right _ -> do
      atomically (writeTVar fake.fakeConflictOnce True)
      runFixture fx wfId $ \wctx -> runTxStep fake.fakeSource protoConfig wctx (\_ _ -> pure (Right ("loser" :: Text)))

-- | The registry's created-before-launch rule and completion clearing,
-- over an instance that never launches: registration is open, a duplicate
-- name is refused, and clearing empties the fake's rows.
scenarioRegistryLifecycle :: forall m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadMVar m) => DBOS m -> DsFixture m -> Text -> m (Either (Error EngineOnly) (), Either (Error EngineOnly) (), Int, Int)
scenarioRegistryLifecycle dbos fx wid = do
  fake <- fx.dsFixtureMkDs
  first <- registerDataSource dbos fake.fakeSource
  duplicate <- registerDataSource dbos fake.fakeSource
  _ <- runFixture fx wid (\wctx -> (runTxStep fake.fakeSource protoConfig wctx (\_ _ -> pure (Right ("v" :: Text))) :: m (Either (Error Text) Text)))
  rowsBefore <- readTVarIO fake.fakeRows
  clearCheckpoints dbos (WorkflowId wid)
  rowsAfter <- readTVarIO fake.fakeRows
  pure (first, duplicate, Map.size rowsBefore, Map.size rowsAfter)

-- | The default config names nothing and takes the database default
-- isolation. Pure: both stacks assert the same value.
scenarioDefaultConfig :: TransactionConfig
scenarioDefaultConfig = transactionConfigDefault

-- | 'beginSql' names every isolation level. Pure: both stacks assert the
-- same mapping.
scenarioBeginSql :: [(Maybe IsolationLevel, Text)]
scenarioBeginSql =
  [ (Nothing, beginSql Nothing),
    (Just ReadUncommitted, beginSql (Just ReadUncommitted)),
    (Just ReadCommitted, beginSql (Just ReadCommitted)),
    (Just RepeatableRead, beginSql (Just RepeatableRead)),
    (Just Serializable, beginSql (Just Serializable))
  ]

-- * Checks

-- | The first run commits, the replay returns the recording, and the body
-- ran exactly once.
checkCommitReplay :: (Either (Error EngineOnly) Text, Either (Error EngineOnly) Text, Int) -> Either String ()
checkCommitReplay (first, second, runs) = do
  unless (first == Right "v1") $ Left ("expected the first run to commit v1, got: " <> show first)
  unless (second == Right "v1") $ Left ("expected the replay to return v1, got: " <> show second)
  unless (runs == 1) $ Left ("expected the body to run once, got: " <> show runs)

-- | The recorded failure decodes back to itself.
checkErrorReplays :: Either (Error Text) Text -> Either String ()
checkErrorReplays result =
  unless (result == Left (application "boom")) $ Left ("expected the recorded failure, got: " <> show result)

-- | The body failure records, and the replay returns it without running
-- the body again.
checkBodyFailureRecorded :: (Either (Error Text) Text, Either (Error Text) Text, Int) -> Either String ()
checkBodyFailureRecorded (first, second, runs) = do
  unless (first == Left (application "boom")) $ Left ("expected the body failure, got: " <> show first)
  unless (second == Left (application "boom")) $ Left ("expected the replay to return the failure, got: " <> show second)
  unless (runs == 0) $ Left ("expected the body never to succeed, got: " <> show runs)

-- | Both retriable failures are consumed and the body runs once.
checkRetryThenSuccess :: (Either (Error EngineOnly) Text, Int) -> Either String ()
checkRetryThenSuccess (result, left) = do
  unless (result == Right "v") $ Left ("expected the retried transaction to succeed, got: " <> show result)
  unless (left == 0) $ Left ("expected the injections to be consumed, got: " <> show left)

-- | The concurrent winner's value is adopted.
checkConflictAdopts :: Either (Error EngineOnly) Text -> Either String ()
checkConflictAdopts result =
  unless (result == Right "winner") $ Left ("expected the adopted winner, got: " <> show result)

-- | The captured-parent call is refused and records nothing.
checkCaptureRefused :: (Either (Error EngineOnly) Text, Int) -> Either String ()
checkCaptureRefused (result, rowCount) = do
  unless (result == Left (InsideStep "transaction")) $ Left ("expected the InsideStep refusal, got: " <> show result)
  unless (rowCount == 0) $ Left ("expected no checkpoint rows, got: " <> show rowCount)

-- | The transient pre-check is consumed and the transaction runs.
checkPrecheckRetry :: (Either (Error EngineOnly) Text, Int) -> Either String ()
checkPrecheckRetry (result, left) = do
  unless (result == Right "v") $ Left ("expected the transaction to succeed, got: " <> show result)
  unless (left == 0) $ Left ("expected the injection to be consumed, got: " <> show left)

-- | Outside a workflow the body runs once, transactionally, and
-- checkpoints nothing.
checkRunsOutside :: (Either BackendError Text, Int, Int) -> Either String ()
checkRunsOutside (result, rowCount, runs) = do
  unless (result == Right "v") $ Left ("expected the outside body to succeed, got: " <> show result)
  unless (rowCount == 0) $ Left ("expected no checkpoint rows, got: " <> show rowCount)
  unless (runs == 1) $ Left ("expected the body to run once, got: " <> show runs)

-- | Partial clearing replays the survivors; full clearing re-runs.
checkDeleteCheckpoints :: (Either (Error EngineOnly) Text, Either (Error EngineOnly) Text, Either (Error EngineOnly) Text, Either (Error EngineOnly) Text, Int) -> Either String ()
checkDeleteCheckpoints (first, second, third, fourth, runs) = do
  unless ((first, second, third, fourth) == (Right "v", Right "v", Right "v", Right "v")) $
    Left "expected every read to return v"
  unless (runs == 3) $ Left ("expected three body runs, got: " <> show runs)

-- | The ownership move stops the execution, naming the owning executor.
checkOwnershipMoved :: Either (Error EngineOnly) Text -> Either String ()
checkOwnershipMoved result = case result of
  Left err
    | "other-executor" `Text.isInfixOf` Text.pack (displayException err) -> Right ()
    | otherwise -> Left ("expected the owning executor to be named, got: " <> displayException err)
  Right _ -> Left "expected the ownership conflict to stop the execution"

-- | Registration succeeds once, the duplicate names the datasource, and
-- clearing empties the rows.
checkRegistryLifecycle :: (Either (Error EngineOnly) (), Either (Error EngineOnly) (), Int, Int) -> Either String ()
checkRegistryLifecycle (first, duplicate, rowsBefore, rowsAfter) = do
  unless (first == Right ()) $ Left ("expected registration to succeed, got: " <> show first)
  case duplicate of
    Left err
      | "datasource" `Text.isInfixOf` Text.pack (displayException err) -> pure ()
      | otherwise -> Left ("expected the refusal to name the datasource, got: " <> displayException err)
    Right () -> Left "expected the duplicate registration to be refused"
  unless (rowsBefore == 1) $ Left ("expected one checkpoint row, got: " <> show rowsBefore)
  unless (rowsAfter == 0) $ Left ("expected clearing to empty the rows, got: " <> show rowsAfter)

-- | The default config names nothing and takes the database default.
checkDefaultConfig :: TransactionConfig -> Either String ()
checkDefaultConfig config =
  unless (config == TransactionConfig {txName = Nothing, txIsolation = Nothing}) $
    Left ("expected the empty default config, got: " <> show config)

-- | Every isolation level renders its SQL.
checkBeginSql :: [(Maybe IsolationLevel, Text)] -> Either String ()
checkBeginSql rendered =
  unless
    (rendered == [(Nothing, "BEGIN"),
                  (Just ReadUncommitted, "BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"),
                  (Just ReadCommitted, "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED"),
                  (Just RepeatableRead, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
                  (Just Serializable, "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE")])
    (Left ("expected every isolation level to render, got: " <> show rendered))
