{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Transactional steps: the application's writes and the step checkpoint
-- commit in one database transaction. A pre-check replays the recorded
-- outcome, one transaction holds the body writes and the checkpoint insert,
-- an already-recorded conflict adopts the winner, retriable failures loop
-- with backoff, and ownership conflicts are rethrown unrecorded.
--
-- The seam is a record of operations rather than a connection pool, because
-- the two things a transactional step needs from the database have different
-- scopes and a pool offers only one of them:
--
-- * Affinity-free reads. Checking a recorded outcome, reading the step name
--   a row holds, deleting checkpoints — each is a single self-contained
--   statement that any connection may serve, and each wants a pooled,
--   short-lived session. Nothing outside the transaction depends on which
--   connection served it.
--
-- * A transaction scope holding exactly one connection. The body and the
--   checkpoint must share a single commit, so one connection is held from
--   @BEGIN@ through the body to @COMMIT@, and rolled back on any escape.
--   The body is an effectful computation that issues statements and does
--   other work between them, so the scope cannot be a closed statement
--   script: it must outlive individual statements and keep the connection
--   pinned for their whole span. Every application statement and both
--   checkpoint writes therefore run on that held connection — never through
--   an ambient or ambient-resolved handle, which is why 'Tx' is threaded
--   explicitly into the body instead.
--
-- So a transactional-step backend supplies two shapes of operation:
-- single-statement reads that need no affinity, and one call that opens a
-- transaction, hands the body a 'Tx' pinned to the held connection, and
-- closes it. Which resources back those operations is the backend's
-- business. Keeping the record polymorphic in its monad also lets one
-- runner serve live backends and simulated ones unchanged.
module DBOS.Internal.Transact.Datasource
  ( -- * Configuration
    IsolationLevel (..),
    TransactionConfig (..),
    transactionConfigDefault,
    -- * Checkpoint shape
    RecordedOutcome (..),
    -- * Backend seam (record-of-functions, IOSim-fakeable)
    Tx (..),
    DataSource (..),
    -- * Runner (staged: signatures first, bodies next)
    runTxStep,
    runTxOutside,
    -- * Registry (per-instance list, frozen at launch)
    DataSourceRegistry,
    newDataSourceRegistry,
    registerDataSource,
    freezeDataSourceRegistry,
    thawDataSourceRegistry,
    snapshotDatasources,
    clearDatasourceCheckpoints,
    -- * Tracing
    TransactionEvent (..),
  )
where

import Data.Aeson (FromJSON, ToJSON)
import Data.Int (Int32)
import Data.Text (pack)
import Data.Text qualified as Text
import DBOS.Internal.Prelude
import DBOS.Internal.SystemDB.Class qualified as SystemDB
import DBOS.Internal.SystemDB.Error (BackendError (..), BackendErrorKind (..), renderError)
import DBOS.Internal.SystemDB.Error qualified as SystemDBError
import DBOS.Internal.SystemDB.Types (SerializedWorkflowValue (..), WorkflowId (..), WorkflowRecord (..), newQualifiedWorkflowName)
import DBOS.Internal.Transact.Context (StepCtx, WorkflowCtx (wctxIdentity, wctxTracer), firstStepStatus, insideAStep, nextStepId, nextWorkflowMarker, ownerToken, withStep, withSystemDB, workflowId)
import DBOS.Internal.Transact.Error (EngineOnly, Error (..), decodeErrorText, encodeErrorText, fromRegistryError, fromSerializationError)
import DBOS.Internal.Transact.Identity (Identity (..))
import DBOS.Internal.Transact.Logger (LogEvent (..), LogSeverity (..), SomeTracer, runTracer)
import DBOS.Internal.Transact.Registry (RegistryError)
import DBOS.Internal.Transact.Registry qualified as Registry
import DBOS.Internal.Transact.Serialization (SerializationError, codecErrorText, codecUnknownTag, decodeWorkflowValue, encodeWorkflowValue)
import DBOS.Internal.Transact.Serialization qualified as Serialization
import GHC.Stack (HasCallStack)
import Hasql.Statement qualified as Statement
import System.Log.FastLogger (ToLogStr (..))

-- | The isolation of the application transaction. The four levels are
-- TypeScript's @PGIsolationLevel@ set — the only oracle that names
-- @READ UNCOMMITTED@; Python's @IsolationLevel@ literal is the three-level
-- subset without it, and Rust v0.5.0 exposes no application-facing isolation
-- level at all. 'transactionConfigDefault' names 'Serializable', the Python
-- default; 'Nothing' is Python's explicit-none path, leaving the level to the
-- database (a bare @BEGIN@).
data IsolationLevel
  = ReadUncommitted
  | ReadCommitted
  | RepeatableRead
  | Serializable
  deriving stock (Eq, Show, Enum, Bounded)

-- | What a caller may say about a transactional step, beyond the body.
-- Minimal v1: a step name, an isolation level, and the read-only flag. A
-- read-only transaction runs @READ ONLY@ and records no checkpoint: reads
-- re-execute safely, so there is nothing to adopt. A volatile read is the
-- caller's hazard — with no record anywhere, every execution re-runs the
-- body and may observe new data. Mirrors TypeScript's @TransactionConfig@
-- (@{ isolationLevel, readOnly }@ — the only oracle that names the flag).
data TransactionConfig = TransactionConfig
  { txName      :: Maybe Text,
    txIsolation :: Maybe IsolationLevel,
    txReadOnly  :: Bool
  }
  deriving stock (Eq, Show)

-- | A transaction that names nothing: the step name falls back to the
-- function name at the call site, isolation to 'Serializable' like Python,
-- and read-write like every oracle's default.
transactionConfigDefault :: TransactionConfig
transactionConfigDefault =
  TransactionConfig
    { txName = Nothing,
      txIsolation = Just Serializable,
      txReadOnly = False
    }

-- | A recorded checkpoint. Mirrors the oracles' @{output}|{error}@ row,
-- plus the step name the oracles keep in @function_name@ so a reordered or
-- renamed step is refused instead of replayed (their
-- @DBOSUnexpectedStepError@ / @UnexpectedStep@).
data RecordedOutcome
  = RecordedOutput Text
  | RecordedError Text
  deriving stock (Eq, Show)

-- | The application transaction the body runs in. Carries the
-- statement runner the body executes its queries through: live it is
-- 'Tx.statement' inside the open transaction (so application writes and
-- the checkpoint share one commit); fakes that model no SQL stub it with
-- an error, and simulator application tests stub above this library.
-- Opaque in construction — minted by 'dsWithTransaction', never by
-- callers — where the oracles use ambient storage.
newtype Tx m = Tx { txStatement :: forall p r. Statement.Statement p r -> p -> m r }

-- | An application pool behind checkpoints. A record of functions — the
-- backend handle travels by argument and tests fake it over STM maps —
-- mirroring the @SomeSystemDB@ existential one layer down. 'dsCheck' reads
-- outside any transaction (the replay fast-path); 'dsWithTransaction' runs
-- one @Tx.Transaction@ holding the body; the record calls run inside that
-- transaction, so application writes and the checkpoint share one commit.
data DataSource m = DataSource
  { dsName              :: Text,
    dsSchema            :: Text,
    dsCheck             :: WorkflowId -> Text -> Int -> m (Either BackendError (Maybe RecordedOutcome)),
    dsWithTransaction   :: forall a. TransactionConfig -> (Tx m -> m a) -> m (Either BackendError a),
    -- | Checkpoint write inside the transaction: 'True' means the row was
    -- written, 'False' means another execution already holds it (adopt
    -- the recorded row — never sniff @23505@: a violation from the
    -- application's own tables is its failure, not a conflict).
    -- Transport failures throw rather than return 'Left', so a failed
    -- write always aborts the attempt it rode in on.
    dsRecordOutput      :: Tx m -> WorkflowId -> Text -> Int -> Text -> m Bool,
    -- | Failure checkpoint, same commit as the output write: a body
    -- failure records rather than escapes, so replay returns it as itself.
    dsRecordError       :: Tx m -> WorkflowId -> Text -> Int -> Text -> m Bool,
    -- | The step name a row holds: the replay name check, the transaction
    -- counterpart of @operation_outputs.function_name@.
    dsStepName          :: WorkflowId -> Int -> m (Either BackendError (Maybe Text)),
    -- | Delete checkpoints from a step onward: completion cleanup and
    -- rewind. Best effort like the oracle — a leftover row is harmless.
    dsDeleteCheckpoints :: WorkflowId -> Int32 -> m (Either BackendError ())
  }

-- | Transactional-step announcements: what the runner says as it checks,
-- replays, commits, and retries. Homed here — the emitter-adjacent leaf —
-- never in the tracer or the facade. Mirrors @WorkflowEvent@ severity
-- shapes: routine run/replay/record at debug, contention retry at warning.
data TransactionEvent
  = TransactionRunning { transactionWorkflowId :: Text, transactionStepName :: Text, transactionStepId :: Int }
  | TransactionReplaying { transactionWorkflowId :: Text, transactionStepName :: Text, transactionStepId :: Int }
  | TransactionOutputRecorded { transactionWorkflowId :: Text, transactionStepName :: Text, transactionStepId :: Int }
  | TransactionErrorRecorded { transactionWorkflowId :: Text, transactionStepName :: Text, transactionStepId :: Int }
  | TransactionConflictAdopted { transactionWorkflowId :: Text, transactionStepName :: Text, transactionStepId :: Int }
  | TransactionOwnershipLost { transactionWorkflowId :: Text, transactionOwner :: Text }
  | TransactionSerializationRetry { transactionWorkflowId :: Text, transactionStepName :: Text, transactionStepId :: Int, transactionAttempt :: Int, transactionBackoffMs :: Integer, transactionDetail :: Text }
  deriving stock (Eq, Show)

instance LogEvent TransactionEvent where
  eventSeverity TransactionRunning {}            = SeverityDebug
  eventSeverity TransactionReplaying {}          = SeverityDebug
  eventSeverity TransactionOutputRecorded {}     = SeverityDebug
  eventSeverity TransactionErrorRecorded {}      = SeverityDebug
  eventSeverity TransactionConflictAdopted {}    = SeverityDebug
  eventSeverity TransactionOwnershipLost {}      = SeverityWarning
  eventSeverity TransactionSerializationRetry {} = SeverityWarning
  renderEvent (TransactionRunning workflowText name stepId') =
    "running transaction step " <> name <> " (" <> showText stepId' <> ") workflow_id=" <> workflowText
  renderEvent (TransactionReplaying workflowText name stepId') =
    "replaying recorded transaction step " <> name <> " (" <> showText stepId' <> ") workflow_id=" <> workflowText
  renderEvent (TransactionOutputRecorded workflowText name stepId') =
    "the transaction committed; its output is recorded step_name=" <> name <> " step_id=" <> showText stepId' <> " workflow_id=" <> workflowText
  renderEvent (TransactionErrorRecorded workflowText name stepId') =
    "the transaction body failed; its error is recorded step_name=" <> name <> " step_id=" <> showText stepId' <> " workflow_id=" <> workflowText
  renderEvent (TransactionConflictAdopted workflowText name stepId') =
    "another execution recorded this step first; adopting its outcome step_name=" <> name <> " step_id=" <> showText stepId' <> " workflow_id=" <> workflowText
  renderEvent (TransactionOwnershipLost workflowText owner) =
    "the workflow is owned by another executor; stopping without recording workflow_id=" <> workflowText <> " owner=" <> owner
  renderEvent (TransactionSerializationRetry workflowText name stepId' attempt backoffMs detail) =
    "the transaction hit a retriable failure and will be retried step_name=" <> name <> " step_id=" <> showText stepId' <> " workflow_id=" <> workflowText <> " attempt=" <> showText attempt <> " backoff_ms=" <> showText backoffMs <> " error=" <> detail

instance ToLogStr TransactionEvent where
  toLogStr = toLogStr . renderLine

-- | Run a transactional step: the application writes and the checkpoint
-- share one commit on the application pool (ADR-0021). Pre-check replays;
-- one transaction holds the body and the checkpoint insert; a held row
-- rolls back and either adopts the recorded outcome or, when another
-- executor owns the workflow, stops without recording. Retriable failures
-- back off and retry; an in-step call is refused. Bodies report their
-- failure as a value (like 'runStepWith'); a body panic propagates
-- unrecorded.
--
-- The scoped entry hands the body the step view of the transaction's own
-- step beside the transaction handle, so step tables can be keyed by the
-- capability that scopes the call. Each attempt runs under 'withAttempt':
-- the body is a step like any other (nested durable calls are refused by
-- the leaf rule, the checkpoint id is visible, and a retry gets a fresh
-- marker and token).
runTxStep :: (FromJSON a, ToJSON a, FromJSON e, ToJSON e, MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, HasCallStack)
          => DataSource m -> TransactionConfig -> WorkflowCtx exec m -> (StepCtx exec m -> Tx m -> m (Either (Error e) a)) -> m (Either (Error e) a)
runTxStep ds config wctx body =
  runTxStepWith ds config wctx body

-- | The shared transaction path: the shaped body receives the attempt's
-- context (the scoped entry turns it into the step view) and the
-- transaction handle.
runTxStepWith :: (FromJSON a, ToJSON a, FromJSON e, ToJSON e, MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, HasCallStack)
              => DataSource m -> TransactionConfig -> WorkflowCtx exec m -> (StepCtx exec m -> Tx m -> m (Either (Error e) a)) -> m (Either (Error e) a)
runTxStepWith ds config wctx body = do
  -- Refused through the handed context or a captured parent alike: a
  -- transaction inside a step would checkpoint under the wrong id.
  stepped <- insideAStep wctx
  if stepped
    then pure (Left (InsideStep "transaction"))
    else do
      let stepName = fromMaybe "transaction" config.txName
      stepId <- nextStepId wctx
      let wid = WorkflowId (workflowId wctx)
          tracer = wctx.wctxTracer
          DataSource {dsStepName = nameAt} = ds
      runTracer tracer (TransactionRunning (workflowId wctx) stepName stepId)
      prechecked <- checkWithRetry ds tracer wid stepName stepId
      case prechecked of
        Left err -> pure (Left (controlErr err))
        Right (Just recorded) -> do
          -- A row can hold a different step: the body's step allocation is
          -- part of the workflow's durable state, and a reordered or renamed
          -- transaction must not replay another step's outcome.
          recordedName <- nameAt wid stepId
          case recordedName of
            Left err -> pure (Left (controlErr err))
            Right (Just other) | other /= stepName -> pure (Left (unexpectedTransaction wid stepName stepId other))
            _ -> do
              runTracer tracer (TransactionReplaying (workflowId wctx) stepName stepId)
              pure (replayRecorded stepName recorded)
        Right Nothing -> attemptTransaction ds wctx config body tracer wid stepName stepId 1 initialBackoffMs

-- | Pre-check with the oracle's retry: a transient read failure backs off
-- and retries ('_check_execution_with_retry'); anything else is control.
-- Attempt numbers restart here — the transaction loop counts its own.
checkWithRetry :: (MonadSTM m, MonadDelay m)
               => DataSource m -> SomeTracer m -> WorkflowId -> Text -> Int -> m (Either BackendError (Maybe RecordedOutcome))
checkWithRetry ds tracer wid stepName stepId = loop 1 initialBackoffMs
  where
    DataSource {dsCheck = checkStep} = ds
    WorkflowId widText = wid
    loop n waitMs = do
      found <- checkStep wid stepName stepId
      case found of
        Left err
          | isRetriable err -> do
              runTracer tracer (TransactionSerializationRetry widText stepName stepId n (round waitMs) err.backendMessage)
              threadDelay (round (waitMs * 1000))
              loop (n + 1) (min (waitMs * 1.5) maxBackoffMs)
          | otherwise -> pure (Left err)
        Right recorded -> pure (Right recorded)

-- | One attempt: body plus checkpoint insert in a single transaction. A
-- held checkpoint throws 'TxConflict' to roll the attempt's application
-- writes back; transport failures surface as 'Left' through the adapter.
attemptTransaction :: forall a e exec m. (FromJSON a, ToJSON a, FromJSON e, ToJSON e, MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, HasCallStack)
                   => DataSource m -> WorkflowCtx exec m -> TransactionConfig -> (StepCtx exec m -> Tx m -> m (Either (Error e) a)) -> SomeTracer m -> WorkflowId -> Text -> Int -> Int -> Double -> m (Either (Error e) a)
attemptTransaction ds wctx config body tracer wid stepName stepId n waitMs = do
  let DataSource {dsWithTransaction = withTx} = ds
      DataSource {dsRecordOutput = recordOutput} = ds
      DataSource {dsRecordError = recordError} = ds
      -- A read-only transaction records no checkpoint: reads re-execute
      -- safely, so there is nothing to adopt. The write is skipped, not
      -- refused — the database refuses actual writes itself.
      recordUnlessReadOnly record tx = if config.txReadOnly then pure True else record tx
  marker <- nextWorkflowMarker wctx
  outcome <-
    try (withTx config $ \tx -> do
      bodyOutcome <- withStep wctx marker (firstStepStatus stepId) (\sctx -> body sctx tx)
      case bodyOutcome of
        Left err -> do
          wrote <- recordUnlessReadOnly (\t -> recordError t wid stepName stepId (encodeErrorText err)) tx
          if wrote
            then do
              unless config.txReadOnly ensureOwned
              pure (Left err)
            else throwIO TxConflict
        Right value -> do
          wrote <- recordUnlessReadOnly (\t -> recordOutput t wid stepName stepId (encodeWorkflowValue value).serializedText) tx
          if wrote
            then do
              unless config.txReadOnly ensureOwned
              pure (Right value)
            else throwIO TxConflict)
  case outcome of
    Left (TxOwnerUnreadable err) -> pure (Left (SystemDatabase err))
    Left TxConflict -> adoptTransaction ds wctx tracer wid stepName stepId
    Right (Left err) -> handleBackend err
    -- A read-only attempt announces nothing: no checkpoint was written, so
    -- the recorded events would lie.
    Right (Right answer)
      | config.txReadOnly -> pure answer
      | otherwise -> do
          case answer of
            Left _  -> runTracer tracer (TransactionErrorRecorded widText stepName stepId)
            Right _ -> runTracer tracer (TransactionOutputRecorded widText stepName stepId)
          pure answer
  where
    WorkflowId widText = wid
    -- The checkpoint and the body's writes commit together, so ownership
    -- is settled before the commit: a loss rolls both back and lets the
    -- adopt path decide. A read that failed is not a loss — the adopt path
    -- would find our own checkpoint gone and call it a missing winner — so
    -- it rolls back and surfaces the system-database error, which the
    -- retry policy may treat as transient.
    ensureOwned = do
      ruling <- checkOwner wctx wid
      case ruling of
        Right Nothing             -> pure ()
        Right (Just _)            -> throwIO TxConflict
        Left (SystemDatabase err) -> throwIO (TxOwnerUnreadable err)
        -- 'checkOwner' wraps every failure in 'SystemDatabase'.
        Left _                    -> throwIO TxConflict
    handleBackend err
      | isRetriable err = do
          runTracer tracer (TransactionSerializationRetry widText stepName stepId n (round waitMs) err.backendMessage)
          threadDelay (round (waitMs * 1000))
          attemptTransaction ds wctx config body tracer wid stepName stepId (n + 1) (min (waitMs * 1.5) maxBackoffMs)
      | otherwise = pure (Left (controlErr err))

-- | Adopt the recorded outcome after losing the race — unless another
-- execution owns the workflow, in which case stop without recording so
-- the owner keeps it. The comparison is the per-execution owner token,
-- not the executor id: a recovery re-run on the same executor is a
-- different execution, and a same-executor race must stop rather than
-- adopt. A null row token is not this execution's either.
adoptTransaction :: (FromJSON a, FromJSON e, MonadSTM m, MonadDelay m)
                 => DataSource m -> WorkflowCtx exec m -> SomeTracer m -> WorkflowId -> Text -> Int -> m (Either (Error e) a)
adoptTransaction ds wctx tracer wid@(WorkflowId widText) stepName stepId = do
  ruling <- checkOwner wctx wid
  case ruling of
    Left err -> pure (Left err)
    Right (Just owner) -> do
      runTracer tracer (TransactionOwnershipLost widText owner)
      pure (Left (ownershipMoved wid owner))
    Right Nothing -> do
      rechecked <- checkWithRetry ds tracer wid stepName stepId
      case rechecked of
        Left err -> pure (Left (controlErr err))
        Right (Just recorded) -> do
          -- The pre-check compares the recorded name before the body runs;
          -- the row this conflict adopted appeared after that check, so
          -- compare it here too. A renamed transaction must not replay the
          -- other build's step.
          let DataSource {dsStepName = nameAt} = ds
          recordedName <- nameAt wid stepId
          case recordedName of
            Left err -> pure (Left (controlErr err))
            Right (Just other) | other /= stepName -> pure (Left (unexpectedTransaction wid stepName stepId other))
            _ -> do
              runTracer tracer (TransactionConflictAdopted widText stepName stepId)
              pure (replayRecorded stepName recorded)
        _ -> pure (Left (controlErr missingWinner))

-- | 'Nothing' means adopt; 'Just owner' names the foreign executor the
-- workflow row belongs to.
checkOwner :: Monad m => WorkflowCtx exec m -> WorkflowId -> m (Either (Error e) (Maybe Text))
checkOwner wctx wid = do
  found <- withSystemDB wctx (\db -> SystemDB.getWorkflow db wid)
  pure $ case found of
    Left err -> Left (SystemDatabase err)
    Right Nothing -> Right Nothing
    Right (Just row) -> case row.workflowRecordOwnerXid of
      -- The per-execution token, not the executor id: a recovery re-run
      -- on the same executor is a different execution and must not pass.
      Just kept | kept == ownerToken wctx -> Right Nothing
      _                                   -> Right (Just (fromMaybe "unknown" row.workflowRecordExecutorId))

-- | Another executor owns the workflow: stop without recording, so the
-- owner keeps it. A control signal, never an outcome.
ownershipMoved :: WorkflowId -> Text -> Error e
ownershipMoved (WorkflowId widText) owner =
  SystemDatabase
    ( SystemDBError.Backend
        ( BackendError
            { backendMessage = "workflow " <> widText <> " is owned by another execution on executor " <> owner,
              backendSqlState = Nothing,
              backendKind = Permanent
            }
        )
    )

-- | Internal: our checkpoint lost the race. Thrown inside the attempt
-- to roll its application writes back, caught by the attempt driver to
-- adopt the winner. Never escapes the runner and never reaches callers.
data TxConflict
  = -- | Our checkpoint lost the race.
    TxConflict
  | -- | The ownership fence could not be read. Distinct from a loss: the
    -- attempt is rolled back and the system-database error is returned, so
    -- a transient blip cannot surface as an adopted row that was never
    -- written.
    TxOwnerUnreadable SystemDBError.Error
  deriving stock (Show)

instance Exception TxConflict

-- | A registered checkpoint whose step name is not the call's: the
-- workflow changed shape between executions. Mirrors the oracle's
-- @UnexpectedStep@ and the name check @operation_outputs@ already applies.
unexpectedTransaction :: WorkflowId -> Text -> Int -> Text -> Error e
unexpectedTransaction (WorkflowId widText) expected stepId recorded =
  SystemDatabase
    ( SystemDBError.UnexpectedStep
        { workflowId = widText
        , stepId = stepId
        , expected = expected
        , recorded = recorded
        }
    )

-- | The recorded outcome of a transaction, replayed without entering the
-- body. Values decode as results; recorded failures decode back to
-- themselves (ADR-0019 envelope).
replayRecorded :: (FromJSON a, FromJSON e) => Text -> RecordedOutcome -> Either (Error e) a
replayRecorded stepName = \case
  RecordedOutput text -> case decodeWorkflowValue "result" (Just (SerializedWorkflowValue text Nothing)) of
    Left err    -> Left (case codecUnknownTag err of
      Just tag -> fromSerializationError (Serialization.UnknownSerialization {name = tag})
      Nothing  -> fromSerializationError (Serialization.Deserialization "result" (codecErrorText err)))
    Right value -> Right value
  RecordedError text -> case decodeErrorText text of
    Left _    -> Left (StepFailed stepName ("recorded transaction error is not decodable: " <> text))
    Right err -> Left err

-- | A backend failure as a control signal: recorded nowhere, so the row
-- stays pending and recovery re-runs. Mirrors the step runner's mapping.
controlErr :: BackendError -> Error e
controlErr err = SystemDatabase (SystemDBError.Backend err)

-- | Failures worth another attempt: the connection and transient kinds by
-- name, plus the two exact serialization-conflict codes for rows that carry
-- no kind. Class @40@ is not a retry set: @40002@ is a refusal and @40003@
-- may already have committed.
-- Matches the primary oracle's retry set (Rust retries conflict and
-- connection kinds; Python additionally retries idle-in-transaction
-- disconnects, which stay permanent here like Rust — decided 2026-10-14
-- under the oracle rule: Rust wins where both speak, and its classifier
-- leaves the 25 class permanent); the checkpoint dedup converges a
-- retried ambiguous commit to adoption, so retrying a dropped connection
-- is safe.
isRetriable :: BackendError -> Bool
isRetriable err = err.backendKind `elem` [Connection, Transient] || isSerializationClass
  where
    isSerializationClass = case err.backendSqlState of
      Just code -> code == "40001" || code == "40P01"
      Nothing   -> False

-- | A conflict with no recorded row to adopt: unreachable by construction
-- (the signal means a row was written), so control rather than data.
missingWinner :: BackendError
missingWinner =
  BackendError
    { backendMessage = "a conflicting execution held this step but recorded no outcome",
      backendSqlState = Nothing,
      backendKind = Permanent
    }

-- | First backoff between serialization retries, in milliseconds. The
-- Knex datasource's 1ms start, 1.5 rate, 2s cap.
initialBackoffMs :: Double
initialBackoffMs = 1.0

maxBackoffMs :: Double
maxBackoffMs = 2000.0
-- | Run a transaction outside any workflow: the body runs
-- transactionally with the same retry loop, but nothing is checkpointed
-- and nothing is announced — there is no execution to record against
-- (mirrors the oracle running plainly outside workflows). Backend
-- failures surface as 'Left'; anything else the body throws propagates.
runTxOutside :: (MonadDelay m, MonadCatch m)
             => DataSource m -> TransactionConfig -> (Tx m -> m a) -> m (Either BackendError a)
runTxOutside ds config body = loop (1 :: Int) initialBackoffMs
  where
    DataSource {dsWithTransaction = withTx} = ds
    loop n waitMs = do
      outcome <- try (withTx config body)
      case outcome of
        Left sysErr -> case sysErr of
          SystemDBError.Backend err -> handleBackend err
          _                         -> pure (Left (synthBackend sysErr))
        Right (Left err) -> handleBackend err
        Right (Right value) -> pure (Right value)
      where
        handleBackend err
          | isRetriable err = do
              threadDelay (round (waitMs * 1000))
              loop (n + 1) (min (waitMs * 1.5) maxBackoffMs)
          | otherwise = pure (Left err)
    synthBackend sysErr =
      BackendError
        { backendMessage = renderError sysErr,
          backendSqlState = Nothing,
          backendKind = Permanent
        }

-- | The per-instance datasource list, frozen at launch like the workflow
-- registry beside it. A registration after launch is refused — this is
-- the oracle's created-before-launch rule — and a failed launch or
-- shutdown thaws it again.
data DataSourceRegistry m = DataSourceRegistry
  { dsrSources :: StrictMVar m [DataSource m],
    dsrFrozen  :: StrictMVar m Bool
  }

newDataSourceRegistry :: MonadMVar m => m (DataSourceRegistry m)
newDataSourceRegistry = DataSourceRegistry <$> newMVar [] <*> newMVar False

-- | Register one datasource unless launch has frozen the registry or its
-- name is taken.
registerDataSource :: MonadMVar m
                   => DataSourceRegistry m -> DataSource m -> m (Either (Error EngineOnly) ())
registerDataSource registry source = do
  frozen <- readMVar registry.dsrFrozen
  if frozen
    then pure (Left (fromRegistryError (Registry.AlreadyLaunched "register_datasource")))
    else
      modifyMVar registry.dsrSources $ \sources ->
        case find ((== source.dsName) . (.dsName)) sources of
          Just _  -> pure (sources, Left (fromRegistryError (Registry.AlreadyRegistered (newQualifiedWorkflowName ("datasource " <> source.dsName)))))
          Nothing -> pure (source : sources, Right ())

-- | The registered datasources, oldest first.
snapshotDatasources :: MonadMVar m => DataSourceRegistry m -> m [DataSource m]
snapshotDatasources registry = reverse <$> readMVar registry.dsrSources

-- | Freeze registrations at launch, so a datasource created afterwards is
-- refused rather than silently unused.
freezeDataSourceRegistry :: MonadMVar m => DataSourceRegistry m -> m ()
freezeDataSourceRegistry registry = modifyMVar_ registry.dsrFrozen (const (pure True))

-- | Reopen registration after a failed launch or a shutdown.
thawDataSourceRegistry :: MonadMVar m => DataSourceRegistry m -> m ()
thawDataSourceRegistry registry = modifyMVar_ registry.dsrFrozen (const (pure False))

-- | Clear a finished workflow's checkpoints from every registered
-- datasource, best effort and silent: a leftover row is harmless, since a
-- later replay adopts from it or re-runs.
clearDatasourceCheckpoints :: forall m. (MonadMVar m, MonadCatch m)
                           => DataSourceRegistry m -> WorkflowId -> m ()
clearDatasourceCheckpoints registry wid = do
  sources <- snapshotDatasources registry
  mapM_
    ( \source -> do
        _ <- try (source.dsDeleteCheckpoints wid 0) :: m (Either SomeException (Either BackendError ()))
        pure ()
    )
    sources
