{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Internal workflow runner. Runs a registered body and records its outcome:
-- @SUCCESS@ with the output, or @ERROR@ with the failure. A missing row is
-- started first; an existing row runs straight into the body, whose steps
-- replay from their
-- checkpoints — which is the whole of crash recovery at this layer. A row
-- that already carries an outcome replays it without running the body, and a
-- row owned by another executor is left alone (@WorkflowClaimLost@):
-- re-running either would overwrite finished work or steal a live claim.
module DBOS.Transact.Workflow
  ( runRegisteredWorkflow,
    runRegisteredWorkflowWithRow,
    -- * Erased registration boundary (moved from Registry under ADR-0041;
    -- the map, the key, and the refusal vocabulary stay there)
    ErasedWorkflow (..),
    WorkflowRef (..),
    refName,
    registerWorkflowRef,
    registerTypedWorkflow,
    enqueueWorkflow,
    QueueRequest (..),
    DuplicationPolicy (..),
    EnqueueOptions (..),
    enqueueOptionsNew,
    queueRequest,
    refuseNamelessWorkflow,
    refuseEmptyQueueName,
    refuseEmptyAppVersion,
    validateEnqueueOptions,
    queueRequestNew,
    validateEnqueue,
    checkStartDuration,
    validateStart,
    storedPriority,
    Timeout (..),
    timeoutBudget,
    resolveTimeoutDeadline,
    RunOptions (..),
    StartOptions (..),
    runOptionsDefault,
    startOptionsDefault,
    runOptionsToStartOptions,
    childWorkflowId,
    -- * Starting a row, joining a held deduplication key (workflow.rs @init_or_join@)
    Submitted (..),
    initOrJoin,
    -- * A child start's recorded refusal (workflow.rs @check_child@)
    recordedStartErrorNames,
    shouldRecordStartError,
    isRecordedStartError,
    startWorkflow,
    runWorkflowRef,
    startChildWorkflow,
    startChildWorkflowAt,
    enqueueWorkflowByNameInWorkflow,
    maxRecoveryAttempts,
    workflowNewWorkflow,
    -- * Task ownership (workflow.rs @Tasks@)
    Role (..),
    Tasks,
    newTasks,
    spawnTracked,
    tasksSpawner,
    abortAll,
    waitAll,
    waitAllUntil,
    runningTasks,
    sleepUnlessStopping,
    isStopping,
    -- * Direct execution for the dequeue dispatch (workflow.rs dispatch
    -- runs from the claimed row, never re-submits through init)
    executeRegisteredWorkflow,
    deadlineOf,
    clearCheckpointsFor,
  )
where

import DBOS.Prelude
-- NOTE (deviation): the one base import outside the port's prelude. Mapping
-- the abort channel needs the identity of the exception 'killThread'
-- throws, and io-classes exposes no async-exception identity — exactly what
-- the oracle matches with @join.is_cancelled()@. Nothing here forks,
-- throws, or waits through base; all effects stay on io-classes.
import Control.Exception (AsyncException (..))
import Data.Aeson (FromJSON, ToJSON, Value (..), fromJSON)
import Data.Aeson qualified as Aeson
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Int (Int64)
import Data.Map.Strict (Map)
import Data.Text (pack)
import Data.Text qualified as Text
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Error (Error (..))
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (AwaitedOutcome (..), DatabaseTime (..), Duration, InitWorkflowCaller (..), NewWorkflow (..), Outcome (..), OutcomeWrite (..), Serialization (..), SerializedWorkflowValue (..), StepRecord (..), StepTiming (..), Timestamp, WorkflowId (..), WorkflowIdReusePolicy (..), WorkflowInitResult (..), QualifiedWorkflowName, qualifiedName, qualifiedClassName, qualifiedConfigName, WorkflowOwner (..), WorkflowRecord (workflowRecordStatus), WorkflowStatus (..), databaseTimeRead, databaseTimeReaches, durationIsZero, durationToMs, isTerminal, newWorkflow, renderQualifiedWorkflowName, timestampNow, timestampToEpochMs)
import DBOS.Transact.Config (serializerName)
import DBOS.Transact.Connection (Connection (..), generatedWorkflowId, runSystemDB)
import DBOS.Transact.Datasource (DataSourceRegistry, clearDatasourceCheckpoints)
import DBOS.Transact.Context (LocalTaskOutcome (..), TaskSpawner (..), WorkflowCtx (wctxConn, wctxIdentity, wctxSpawner), clearSpawnerCheckpoints, deadline, insideAStep, nextStepId, ownerToken, releaseStart, spawnLocal, startToken, waitForStarts, withWorkflow, withWorkflowTaskSpawner, workflowId)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Handle (WorkflowHandle (..), localHandle, pollingHandle)
import DBOS.Transact.Identity (Identity (..), refuseAppName)
import DBOS.Transact.Logger (runTracer)
import DBOS.Transact.Registry (Registry, Snapshot, lookupRegistryWorkflow, lookupSnapshotWorkflow, registryInstanceId)
import DBOS.Transact.Registry qualified as Registry (registerErasedWorkflow)
import DBOS.Transact.Serialization (CodecError (..), SerializationError, WorkflowInput (..), codecErrorText, codecUnknownTag, decodeWorkflowInput, decodeWorkflowValue, encodeAttributes, encodeWorkflowValue, unWorkflowInput)
import DBOS.Transact.Serialization qualified as Serialization
import DBOS.Transact.Step (WorkflowEvent (..))

-- | Attempts before a workflow is parked as
-- @MAX_RECOVERY_ATTEMPTS_EXCEEDED@. Mirrors workflow.rs.
maxRecoveryAttempts :: Int64
maxRecoveryAttempts = 100

-- | Start a registered workflow using the SystemDB class-backed engine.
-- The connection, resolved identity and task registry belong to the
-- executor; the body takes the explicit context it runs in.
runRegisteredWorkflow :: (MonadFork m, MonadMask m, MonadMVar m, MonadDelay m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m, FromJSON e)
                      => Tasks m -> Connection m -> Identity -> Snapshot m (ErasedWorkflow m) -> QualifiedWorkflowName -> WorkflowId -> Maybe SerializedWorkflowValue -> WorkflowIdReusePolicy -> DataSourceRegistry m -> m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue))
runRegisteredWorkflow tasks conn identity snapshot key workflowId input reusePolicy datasources = runRegisteredWorkflowWithRow tasks conn identity snapshot key workflowId input (workflowNewWorkflow conn identity key workflowId input Nothing) reusePolicy datasources

-- | The execute-or-adopt core both runners share: one init decides whether
-- this call owns the row. A fresh row (or a recovery/dequeue claim) runs
-- the body; an existing row awaits whoever owns it. Splitting record and
-- run across two inits would always read back a foreign owner — each init
-- mints its own — and poll a row nothing runs, so a run is one init, never
-- a start plus a second init.
--
-- The owned body is spawned, not run in place: the workflow's life is the
-- executor's, not the caller's, so dropping the caller must not stop the
-- run. The caller then waits on the spawned task, which is what makes a
-- shutdown that aborts it visible here as 'TransactError.Interrupted'
-- rather than as a hang.

runRegisteredWorkflowWithRow :: (MonadFork m, MonadMask m, MonadMVar m, MonadDelay m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m, FromJSON e)
                             => Tasks m -> Connection m -> Identity -> Snapshot m (ErasedWorkflow m) -> QualifiedWorkflowName -> WorkflowId -> Maybe SerializedWorkflowValue -> NewWorkflow -> WorkflowIdReusePolicy -> DataSourceRegistry m -> m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue))
runRegisteredWorkflowWithRow tasks conn identity snapshot key workflowId@(WorkflowId workflowText) input new reusePolicy datasources =
  case lookupSnapshotWorkflow key snapshot of
    Nothing -> pure (Left (TransactError.NotRegistered key))
    Just workflow -> do
      initialized <- runSystemDB conn.connSysdb (\db -> SystemDB.initWorkflow db new Nothing reusePolicy)
      case initialized of
        Left err -> pure (Left (TransactError.fromSystemDB err))
        Right result
          -- A queued row is not this call's to execute: wait for the queue
          -- worker (which holds the claim) exactly as a run of a foreign-owned
          -- row waits for its owner.
          | result.initResultStatus == Enqueued || result.initResultStatus == Delayed ->
              adoptRecordedOutcome conn workflowId
          -- A finished row is adopted, never re-run: a start reusing a
          -- finished id joins the recorded outcome (the oracle joins on
          -- creator mismatch the same way). Re-running would repeat the
          -- body behind an id whose history is closed. Parked rows ride
          -- along: exceeded recovery attempts never run again either.
          -- Announced like every other join, as the start path does.
          | isTerminal result.initResultStatus || result.initResultStatus == MaxRecoveryAttemptsExceeded -> do
              runTracer conn.connTracer (WorkflowAlreadyOwned workflowText)
              adoptRecordedOutcome conn workflowId
          | not result.initResultShouldExecute -> do
              runTracer conn.connTracer (WorkflowAlreadyOwned workflowText)
              adoptRecordedOutcome conn workflowId
          | otherwise -> do
              channel <-
                spawnLocal
                  (tasksSpawner tasks (clearCheckpointsFor datasources))
                  workflowText
                  ( \spawner ->
                      executeRegisteredWorkflow spawner conn identity workflowId workflow input (deadlineOf result.initResultDeadline result.initResultClock) Nothing (WorkflowOwner {ownerWorkflowId = workflowId, ownerToken = result.initResultCreatorXid})
                  )
              executed <- readMVar channel
              case executed of
                LocalTaskValue value -> case value of
                  Right output -> pure (Right output)
                  Left failure -> pure (Left (TransactError.failureError workflowText failure))
                -- Only shutdown aborts a workflow task, and it leaves the
                -- row PENDING on purpose.
                LocalTaskCancelled -> pure (Left (TransactError.Interrupted {workflowId = workflowText}))
                -- The body's own failure escapes to the waiter, as the
                -- oracle resumes the panic into its awaiting caller.
                LocalTaskPanic err -> throwIO err
-- | The spawner the engine injects into an execution's context, closing
-- over the executor's task registry: a child started from a body is
-- detached, counted, and abortable by shutdown, exactly like a directly
-- spawned execution. Self-referential on purpose — the capability an
-- execution hands its grandchildren is its own. The outcome lands in the
-- returned box while the task unwinds, so a handle awaiting a task the
-- shutdown aborted still learns 'LocalTaskCancelled' rather than hanging.
-- The clearance rides the same capability: a child the body starts inherits
-- its parent's spawner, so its checkpoints clear on completion exactly like
-- a top-level run's, with no registry channel on any body-level signature.
tasksSpawner :: (MonadFork m, MonadMask m, MonadSTM m, MonadMVar m)
             => Tasks m -> (Text -> m ()) -> TaskSpawner m
tasksSpawner tasks clear = spawner
  where
    spawner = TaskSpawner (\label action -> do
      channel <- newEmptyMVar
      spawned <- spawnTracked tasks label $ mask $ \restore -> do
        result <- try (restore (action spawner))
        -- The fill below is masked back over: only the body runs
        -- interruptibly, so a kill landing around the body still leaves the
        -- outcome in the box.
        putMVar channel $ case result of
          Right value -> LocalTaskValue value
          Left err -> case fromException err :: Maybe AsyncException of
            Just ThreadKilled -> LocalTaskCancelled
            _                 -> LocalTaskPanic err
      -- A refused arrival never forked, so the parent fills the
      -- cancellation itself: no child exists to do it, and the waiter must
      -- still learn the outcome rather than hang on an empty box.
      case spawned of
        Nothing -> putMVar channel LocalTaskCancelled
        Just _  -> pure ()
      pure channel
      ) clear

-- | The checkpoint clearance a spawner carries for one registry: drop the
-- named workflow's rows. Best effort and silent at the call site — the
-- registry call already swallows per-source failures, and the runner
-- swallows what the snapshot read itself could still throw.
clearCheckpointsFor :: (MonadMVar m, MonadCatch m) => DataSourceRegistry m -> Text -> m ()
clearCheckpointsFor registry widText = clearDatasourceCheckpoints registry (WorkflowId widText)

-- | Park-and-adopt: wait for whoever owns the row and report its recorded
-- outcome as an erased failure. Mirrors @Connection::adopt@: the recorded
-- outcome is the answer, so a superseded execution — or a caller joining an
-- owned row — does not return what it computed itself. The recorded failure
-- travels as the payload the row holds, and the caller's channel decides
-- how to decode it.
adoptRecordedFailure :: (MonadDelay m, MonadTime m)
                     => Connection m -> WorkflowId -> m (Either TransactError.Failure (Maybe SerializedWorkflowValue))
adoptRecordedFailure conn wid@(WorkflowId workflowText) = do
  awaited <- runSystemDB conn.connSysdb (\db -> SystemDB.awaitWorkflowResult db wid conn.connOutcomePollInterval True)
  pure $ case awaited of
    Left err -> Left (TransactError.FailureControl (TransactError.fromSystemDB err))
    Right (AwaitedSucceeded output serialization) ->
      Right (SerializedWorkflowValue <$> output <*> pure (Serialization <$> serialization))
    Right (AwaitedFailed message _) -> Left (TransactError.FailureRecorded message)
    -- The adopted row is this execution's own, so its cancellation is this
    -- workflow being cancelled, not an awaited child's outcome. Rust's
    -- @adopt@ draws the same line: @WorkflowCancelled@ here,
    -- @AwaitedWorkflowCancelled@ only in the handle's await.
    Right AwaitedCancelled ->
      Left
        ( TransactError.FailureControl
            ( TransactError.fromSystemDB
                (SystemDBError.WorkflowCancelled {workflowId = workflowText})
            )
        )
    Right (AwaitedParked attempts) ->
      -- The handle's own verdict, as Rust's @adopt@ constructs
      -- @Error::MaxRecoveryAttemptsExceeded@ directly: the sysdb layer
      -- never built one, so neither does this port.
      Left
        ( TransactError.FailureControl
            (TransactError.ErrorMaxRecoveryAttemptsExceeded {workflowId = workflowText, recoveryAttempts = attempts})
        )

-- | The typed face of 'adoptRecordedFailure': the recorded failure decoded
-- back into the caller's channel.
adoptRecordedOutcome :: (MonadDelay m, MonadTime m, FromJSON e)
                     => Connection m -> WorkflowId -> m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue))
adoptRecordedOutcome conn (WorkflowId workflowText) = do
  adopted <- adoptRecordedFailure conn (WorkflowId workflowText)
  pure (either (Left . TransactError.failureError workflowText) Right adopted)

-- | A superseded execution's answer: the row's, not the body's. The body
-- outlived the call that noticed the hand-off, and the workflow may have
-- been resumed and finished meanwhile, so the control failure it returns
-- may no longer be the outcome. Reading the row answers "cancelled" at
-- once while it still is, and otherwise gives this caller what every
-- other caller sees. A dequeued execution gives up its queue slot first,
-- as a timeout does: holding the only slot while polling for the new
-- owner's outcome would stall the worker that must run it. Mirrors Rust
-- @adopt_superseded@.
adoptSuperseded :: (MonadDelay m, MonadTime m)
                => Connection m -> WorkflowId -> Maybe (m ()) -> m (Either TransactError.Failure (Maybe SerializedWorkflowValue))
adoptSuperseded conn wid@(WorkflowId workflowText) releaseSlot = do
  maybe (pure ()) id releaseSlot
  found <- runSystemDB conn.connSysdb (\db -> SystemDB.getWorkflow db wid)
  case found of
    Right (Just row) | row.workflowRecordStatus == Cancelled ->
      pure (Left (TransactError.FailureControl (TransactError.WorkflowCancelled {workflowId = workflowText})))
    _ -> adoptRecordedFailure conn wid
-- | A workflow's deadline, and when it passes on this process's monotonic
-- clock. Mirrors Rust @workflow::Deadline@: @at@ is the instant the row
-- holds, which the context carries for children to inherit; @expires@ is
-- that instant measured from a reading of the database's clock, so the wait
-- does not depend on this host's wall clock agreeing with the database's.
data Deadline = Deadline
  { deadlineAt      :: Timestamp,
    deadlineExpires :: Word64
  }
  deriving stock (Eq, Show)

-- | The deadline @init_workflow@ reported, timed from the reading of the
-- database's clock that came back with it.
deadlineOf :: Maybe Timestamp -> DatabaseTime -> Maybe Deadline
deadlineOf at clock = (\instant -> Deadline instant (databaseTimeReaches clock instant)) <$> at

-- | Runs one execution of a registered body as its owner: a fresh execution
-- identity, the row's stored deadline, the body, and the outcome write. The
-- row's stored deadline rides the execution, so a recovered workflow keeps
-- what is left, and children inherit the instant rather than a fresh budget.
-- The spawner rides the context, so a child this body starts is detached
-- into the same registry and hands its own children the same capability.
-- @releaseSlot@ is the queue slot a dequeued execution holds: it is given up
-- before waiting for the timeout's row to settle, as Rust's @time_out@ drops
-- its slot first.
executeRegisteredWorkflow :: forall m. (MonadDelay m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m, MonadCatch m)
                          => TaskSpawner m -> Connection m -> Identity -> WorkflowId -> ErasedWorkflow m -> Maybe SerializedWorkflowValue -> Maybe Deadline -> Maybe (m ()) -> WorkflowOwner -> m (Either TransactError.Failure (Maybe SerializedWorkflowValue))
executeRegisteredWorkflow spawner conn identity workflowId@(WorkflowId workflowText) workflow input deadline releaseSlot owner = do
  attempted <- try (runBody)
  case attempted of
    -- Shutdown aborts the task: the row stays PENDING on purpose, with no
    -- panic line — a kill is not a bug. Anything else escaping the body is
    -- one, and the row it leaves behind is for recovery. Mirrors
    -- @PanicLog@: the guard fires on the unwind, never on a returned
    -- outcome.
    Left se -> case fromException se of
      Just ThreadKilled -> throwIO se
      _ -> do
        runTracer conn.connTracer (WorkflowPanicked workflowText)
        throwIO se
    Right value -> pure value
  where
    -- | Drops this execution's datasource checkpoints, best effort and
    -- silent, through the spawner's registry channel. A child this body
    -- starts runs under its parent's spawner, so its checkpoints clear on
    -- completion exactly like a top-level run's.
    --
    -- Only completions clear, never escapes: a panic before the app
    -- transaction committed already rolled its checkpoints back with it,
    -- so there is nothing to drop; a panic after it committed must keep
    -- them, or the redrive re-runs committed work instead of adopting it
    -- (pinned by the outbox redrive cases). Decided 2026-10-14 under the
    -- oracle rule: Python deletes on every escape, but has no counterpart
    -- test in the panic-post-commit shape, and deletion there breaks
    -- exactly-once adoption.
    clearExecution = do
      _ <- try (clearSpawnerCheckpoints spawner workflowText) :: m (Either SomeException ())
      pure ()
    -- The scope the body runs in: fresh counters, the row's deadline, and
    -- the executor's task spawner, so a converted body's scoped entries
    -- reach the same machinery the context-level ones did. The row's stored
    -- deadline is the run's budget: the body races the clock, and losing it
    -- cancels the workflow durably, as Rust's deadline does.
    runBody =
      withWorkflow conn identity workflowId ((.deadlineAt) <$> deadline) owner $ \wctx -> do
        let scoped = withWorkflowTaskSpawner spawner wctx
            ErasedWorkflow body = workflow
            -- **A start the body dropped may still be writing under this
            -- execution's token**, which the outcome write gives up. Its
            -- write would then be refused with nobody awaiting it, and the
            -- child would never exist, so the outcome waits for every start
            -- the body began. Inside the deadline, so a start that stalls
            -- past it cannot hold the workflow open: the run times out as
            -- any other would, and a start refused after that is refused
            -- for a cancelled parent.
            runWithStarts = do
              outcome <- body input scoped
              waitForStarts wctx
              pure outcome
        raced <- case deadline of
          Nothing -> Just <$> runWithStarts
          Just due -> do
            now <- getMonotonicTimeNSec
            let remainingNs = if due.deadlineExpires > now then due.deadlineExpires - now else 0
            -- io-classes' timer is microseconds, the monotonic reading is
            -- nanoseconds: round down, so a sub-microsecond remainder waits
            -- zero rather than a whole extra microsecond.
            timeout (fromIntegral (remainingNs `div` 1000)) runWithStarts
        case raced of
          Nothing -> timeOut
          Just outcome -> case outcome of
            Left failure
              -- A control signal is not the workflow's outcome: the execution
              -- records nothing of its own. Warned, because the caller may
              -- have dropped its future and this is the only evidence.
              -- A supersede instead adopts the row's outcome (see
              -- 'adoptSuperseded'): the body outlived the hand-off, so its
              -- control failure may no longer be the answer.
              | isControlFailure failure -> do
                  runTracer conn.connTracer (WorkflowControlEnded (TransactError.renderTransactError (failureControl failure)))
                  case failure of
                    TransactError.FailureControl (TransactError.SystemDatabase (SystemDBError.WorkflowConflict {workflowId = stopped}))
                      | stopped == workflowText -> adoptSuperseded conn workflowId releaseSlot
                    TransactError.FailureControl (TransactError.WorkflowCancelled {workflowId = stopped})
                      | stopped == workflowText -> adoptSuperseded conn workflowId releaseSlot
                    _ -> pure (Left failure)
              | otherwise -> do
                  -- A settled body holds no checkpoints: clear before the
                  -- outcome commits, so no observer ever sees a finished
                  -- row with rows still present — the oracle deletes in
                  -- its run wrapper for the same reason.
                  clearExecution
                  case failure of
                    TransactError.FailureRecorded payload -> recordOutcome (OutcomeError payload) (Left failure)
                    -- Unreachable by construction: only control failures are
                    -- `Control`, and those were handled above.
                    TransactError.FailureControl err      -> recordOutcome (OutcomeError (TransactError.encodeErrorText (TransactError.liftEngine err :: (TransactError.Error TransactError.EngineOnly)))) (Left failure)
            Right output -> clearExecution >> recordOutcome (OutcomeOutput ((.serializedText) <$> output)) (Right output)
    -- Records one outcome and reports what the write decided: recording a
    -- second outcome behind a finished row is a supersede, not a write, and
    -- the recorded outcome is the answer.
    recordOutcome outcome fallback = do
      saved <- runSystemDB conn.connSysdb (\db -> SystemDB.recordWorkflowOutcome db workflowId outcome owner.ownerToken)
      case saved of
        Left dbError -> do
          runTracer conn.connTracer (WorkflowOutcomeRecordFailed (TransactError.renderTransactError (TransactError.fromSystemDB dbError :: (TransactError.Error TransactError.EngineOnly))))
          pure (Left (TransactError.FailureControl (TransactError.fromSystemDB dbError)))
        Right Recorded -> case fallback of
          Right _ -> do
            runTracer conn.connTracer (WorkflowCompleted workflowText)
            pure fallback
          Left _ -> do
            runTracer conn.connTracer (WorkflowFailed workflowText)
            pure fallback
        Right AlreadyFinished -> do
          runTracer conn.connTracer (WorkflowSuperseded workflowText)
          adoptRecordedFailure conn workflowId
    isControlFailure failure = case failure of
      TransactError.FailureControl _  -> True
      TransactError.FailureRecorded _ -> False
    failureControl failure = case failure of
      TransactError.FailureControl err      -> err
      TransactError.FailureRecorded payload -> TransactError.WorkflowFailed {workflowId = workflowText, message = payload}
    -- Records the timeout of a workflow whose local timer fired, and settles
    -- on what the row holds. Cancelled only if the database's clock says the
    -- deadline has passed — the same test the timeout sweep makes — so a
    -- timer that fires a moment early, inside a reading's millisecond, cannot
    -- cancel a workflow early. The sweep is normally the writer; this keeps a
    -- timed-out run's own row settled even where the sweep does not reach it.
    -- Mirrors Rust's @time_out@.
    timeOut = do
      maybe (pure ()) id releaseSlot
      cancelled <- runSystemDB conn.connSysdb (\db -> SystemDB.cancelIfTimedOut db workflowId owner.ownerToken)
      case cancelled of
        Right True -> runTracer conn.connTracer (WorkflowDeadlineCancelled workflowText)
        Right False -> pure ()
        Left dbError ->
          runTracer conn.connTracer (WorkflowDeadlineRecordFailed workflowText (TransactError.renderTransactError (TransactError.fromSystemDB dbError :: (TransactError.Error TransactError.EngineOnly))))
      adoptRecordedFailure conn workflowId


-- | Create an enqueued workflow row without running its body. A queue worker
-- later claims it and invokes the registered body with the claimed row.
enqueueWorkflow :: Monad m
                => Connection m -> Identity -> QualifiedWorkflowName -> WorkflowId -> Maybe SerializedWorkflowValue -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) WorkflowInitResult)
enqueueWorkflow conn identity key workflowId input queueName = do
  initialized <- runSystemDB conn.connSysdb (\db -> SystemDB.initWorkflow db (workflowNewWorkflow conn identity key workflowId input (Just queueName)) Nothing WorkflowIdReturnExisting)
  pure (either (Left . TransactError.fromSystemDB) Right initialized)

workflowNewWorkflow :: Connection m -> Identity -> QualifiedWorkflowName -> WorkflowId -> Maybe SerializedWorkflowValue -> Maybe Text -> NewWorkflow
workflowNewWorkflow conn identity key (WorkflowId workflowText) input queueName =
  let serialization = case input >>= (.serializedSerialization) of
        Just (Serialization name) -> name
        Nothing                   -> serializerName conn.connSerializer
      workflowName = qualifiedName key
      className = qualifiedClassName key
      configName = qualifiedConfigName key
   in (newWorkflow workflowText)
        { newWorkflowName = Just workflowName,
          newWorkflowClassName = className,
          newWorkflowConfigName = configName,
          newWorkflowInput = (.serializedText) <$> input,
          newWorkflowSerialization = Just serialization,
          newWorkflowQueueName = queueName,
          newWorkflowExecutorId = Just identity.identityExecutorId,
          newWorkflowAppName = Just identity.identityAppName,
          newWorkflowAppVersion = Just identity.identityAppVersion,
          newWorkflowAppId = if Text.null identity.identityAppId then Nothing else Just identity.identityAppId
        }

-- | What an enqueue asks of its queue: deduplication, priority, partition,
-- delay, and — only for the debouncer's fresh creation — the debounce
-- creation flags. Mirrors Rust @QueueRequest@ in @workflow.rs@, which the
-- client's @EnqueueOptions@ builds internally and the runtime's start
-- options hold, so the two surfaces never spell the same settings
-- differently; the two debounce fields mirror TypeScript's
-- @EnqueueOptions@ (@isDebounced@, @debounceDeadlineEpochMS@), which only
-- the debounce names — the oracle's own comment says no other caller
-- does — so a plain enqueue stays non-debounced with no deadline.
data QueueRequest = QueueRequest
  { name              :: Text,
    deduplicationId   :: Maybe Text,
    priority          :: Maybe Word32,
    partitionKey      :: Maybe Text,
    delay             :: Maybe Duration,
    duplicationPolicy :: DuplicationPolicy,
    isDebounced       :: Bool,
    debounceTimeout   :: Maybe Duration,
    applicationName   :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | What an enqueue does when its 'deduplication_id' is already held.
data DuplicationPolicy
  = -- | Refuse the enqueue, reporting the collision. The default.
    Reject
  | -- | Hand back a handle to the workflow already holding the key.
    ReturnExisting
  deriving stock (Eq, Show)

-- | A plain enqueue onto a name, asking for nothing else.
queueRequestNew :: Text -> QueueRequest
queueRequestNew queueName =
  QueueRequest
    { name = queueName,
      deduplicationId = Nothing,
      priority = Nothing,
      partitionKey = Nothing,
      delay = Nothing,
      duplicationPolicy = Reject,
      isDebounced = False,
      debounceTimeout = Nothing,
      applicationName = Nothing
    }

-- | Rejects an enqueue no queue could honour. Only what the shape could not
-- rule out: nesting under the queue already makes a delay with no queue
-- unbuildable, so what is left is the deduplication/partition pair, the
-- policy without a key, the priority range, and the delay. Each is an
-- Rust @QueueRequest::validate@.
validateEnqueue :: QueueRequest -> Either (TransactError.Error e) ()
validateEnqueue queue
  -- The insert's refusal comes after a workflow caller's step id is
  -- taken, and is recorded there: refuse here instead. Mirrors Rust
  -- @QueueRequest::validate@.
  | Just "" <- queue.deduplicationId =
      refuse "`deduplication_id` must be absent rather than empty"
  | Just "" <- queue.partitionKey =
      refuse "`partition_key` must be absent rather than empty"
  | Just _ <- queue.deduplicationId,
    Just _ <- queue.partitionKey =
      refuse
        "`deduplication_id` and `partition_key` cannot both be set: a partitioned queue's dequeue and a deduplication key enforce different things"
  | queue.duplicationPolicy == ReturnExisting,
    Nothing <- queue.deduplicationId =
      refuse
        "`DuplicationPolicy::ReturnExisting` needs a `deduplication_id`: with no key there is no collision to resolve"
  | Just 0 <- queue.priority =
      refuse "`priority` must be at least 1; use `Nothing` for an unprioritised workflow"
  | Just limit <- queue.priority,
    limit > maxPriority =
      refuse ("`priority` must be at most " <> pack (show maxPriority) <> ", got " <> pack (show limit))
  | Just delay <- queue.delay = checkStartDuration (Just queue.name) "delay" delay
  | otherwise = Right ()
  where
    refuse detail = Left (TransactError.startRefusal (Just queue.name) detail)
    -- The column is a signed 32-bit integer; 'Nothing' is the only way to
    -- say unprioritised, because 0 is the stored sentinel for it.
    maxPriority = 2147483647 :: Word32

-- | Refuses a start duration on the start's channel, asking the
-- database's own check: a zero duration is refused before storability
-- is asked, and the queue rides along so the refusal names the enqueue.
-- Mirrors the duration half of Rust @validate_start@.
checkStartDuration :: Maybe Text -> Text -> Duration -> Either (TransactError.Error e) ()
checkStartDuration queueName field duration
  | durationIsZero duration =
      Left (TransactError.startRefusal queueName ("`" <> field <> "` must be a positive, non-zero duration"))
  | otherwise = case durationToMs field duration of
      Left (SystemDBError.InvalidInput _ detail) ->
        Left (TransactError.startRefusal queueName ("`" <> field <> "` " <> detail))
      Left err -> Left (TransactError.fromSystemDB err)
      Right _ -> Right ()

-- | The enqueue's own checks plus the timeout, refused as the start's
-- own: to 'enqueue a workflow' naming the queue when one is named, and
-- to 'start a workflow' otherwise. A reuse policy is about an id the
-- caller chose, so 'WorkflowIdReject' needs one: with none, a root start
-- has a fresh id nothing can have taken, and a child's is derived from
-- its parent's, an id the caller never asked to be new. The checks run
-- before the start takes a step id. Mirrors Rust @validate_start@.
validateStart :: Maybe QueueRequest -> Maybe Duration -> Maybe Text -> WorkflowIdReusePolicy -> Either (TransactError.Error e) ()
validateStart queue budget workflowId reusePolicy = do
  -- As the insert refuses it, but before a step id is taken: see
  -- 'validateEnqueue'. Mirrors Rust @validate_start@.
  case workflowId of
    Just "" ->
      Left
        ( TransactError.startRefusal
            ((.name) <$> queue)
            "`workflow_id` must not be empty"
        )
    _ -> pure ()
  case (reusePolicy, workflowId) of
    (WorkflowIdReject, Nothing) ->
      Left
        ( TransactError.startRefusal
            ((.name) <$> queue)
            "`WorkflowIdReusePolicy::Reject` needs a `workflow_id`: the policy is about an id the caller chose"
        )
    _ -> pure ()
  traverse_ validateEnqueue queue
  case budget of
    Nothing -> Right ()
    Just duration -> checkStartDuration ((.name) <$> queue) "timeout" duration

-- | The stored priority: the sentinel @0@ when unprioritised. Infallible
-- because 'validateEnqueue' has already ruled out what would not fit.
storedPriority :: QueueRequest -> Int
storedPriority queue = maybe 0 fromIntegral queue.priority

-- | How long a workflow may take. A budget, counted from now when explicit;
-- durable, and unlike a step timeout it cancels rather than fails: the
-- budget becomes a wall-clock deadline stored on the row, so it survives a
-- crash. A plain 'Maybe Duration' cannot spell the three states — a caller
-- who said nothing ('Inherit'), a caller who decided on no limit ('None'),
-- and a budget ('Explicit') — so this is an enum.
data Timeout
  = -- | Say nothing about the budget: a child takes its parent's deadline,
    -- a root runs unbounded. The default.
    Inherit
  | -- | Run for as long as it likes, even under a parent that has one.
    None
  | -- | A budget, counted from now. Replaces an inherited deadline rather
    -- than being bounded by it.
    Explicit Duration
  deriving stock (Eq, Show)

-- | The budget to record on the row, which only 'Explicit' has. Neither of
-- the others is a budget: 'Inherit' takes an instant from its parent and
-- 'None' takes nothing, and a row's timeout column is what a queue
-- recomputes a deadline from on dequeue.
timeoutBudget :: Timeout -> Maybe Duration
timeoutBudget timeout =
  case timeout of
    Explicit budget -> Just budget
    Inherit         -> Nothing
    None            -> Nothing

-- | The deadline to record on the row. Only an inherited deadline is an
-- instant this process writes: an explicit budget is recorded as a budget
-- and the insert turns it into a deadline against the database's clock (a
-- queued workflow's claim does the same), and 'None' records neither. An
-- inherited deadline is the parent's instant, shared with no signal passing
-- between them. Mirrors Rust's @deadline@ match in @create@.
resolveTimeoutDeadline :: Timeout -> Maybe Timestamp -> Maybe Timestamp
resolveTimeoutDeadline timeout parentDeadline =
  case timeout of
    Explicit _ -> Nothing
    None       -> Nothing
    Inherit    -> parentDeadline

-- | What a client may say about an enqueue, beyond the input. Flat, as
-- Python's and Java's @EnqueueOptions@ are: what to ask of the queue sits
-- beside everything else, so an enqueue by name is one type. A start says
-- the same queue-shaped things in a 'QueueRequest', under
-- 'StartOptions' 'startQueue', because there the queue is optional and
-- those settings exist only with one. Apart from the queue's own name,
-- the two spell each setting the same way, and are checked by the same
-- rules. Mirrors Rust @EnqueueOptions@.
data EnqueueOptions = EnqueueOptions
  { queueName :: Text,
    deduplicationId :: Maybe Text,
    duplicationPolicy :: DuplicationPolicy,
    priority :: Maybe Word32,
    partitionKey :: Maybe Text,
    delay :: Maybe Duration,
    workflowId :: Maybe Text,
    workflowIdReusePolicy :: WorkflowIdReusePolicy,
    appName :: Maybe Text,
    appVersion :: Maybe Text,
    timeout :: Timeout,
    attributes :: Maybe (Map Text Value)
  }
  deriving stock (Eq, Show)

-- | The refusal a client enqueue of a nameless workflow gets: nothing could
-- ever dequeue the row. A client takes the name from its caller; an
-- executor's starts take theirs from a registration, which refuses it.
-- Mirrors Rust @EnqueueOptions::validate@.
refuseNamelessWorkflow :: QualifiedWorkflowName -> Text -> Maybe (TransactError.Error e)
refuseNamelessWorkflow workflow queueName
  | Text.null (qualifiedName workflow) =
      Just
        ( TransactError.InvalidArgument
            "enqueue a workflow"
            ("queue `" <> queueName <> "`: `workflow` must not be empty")
        )
  | otherwise = Nothing

-- | Refuses an empty queue name up front, naming the field the caller
-- wrote — rather than leaving the insert to refuse the column under the
-- name a start's request gives it. Mirrors the @queue_name@ arm of Rust
-- @EnqueueOptions::validate@.
refuseEmptyQueueName :: Text -> Maybe (TransactError.Error e)
refuseEmptyQueueName queueName
  | Text.null queueName =
      Just (TransactError.InvalidArgument "enqueue a workflow" "`queue_name` must not be empty")
  | otherwise = Nothing

-- | Refuses an empty version up front: an empty string is not a missing
-- value ('Nothing' is), and the insert would refuse it only after a
-- workflow caller's step id is taken. Mirrors the @app_version@ arm of
-- Rust @EnqueueOptions::validate@.
refuseEmptyAppVersion :: Text -> Maybe Text -> Maybe (TransactError.Error e)
refuseEmptyAppVersion queueName appVersion
  | Just "" <- appVersion =
      Just (TransactError.InvalidArgument "enqueue a workflow" ("queue `" <> queueName <> "`: `app_version` must be absent rather than empty"))
  | otherwise = Nothing

-- | The whole client-side options check, in the oracle's order: the name,
-- the queue, the application, the version, then the start the row would
-- make. Both client entries share it, so a refusal never depends on which
-- one the caller reached. Mirrors Rust @EnqueueOptions::validate@.
validateEnqueueOptions :: QualifiedWorkflowName -> EnqueueOptions -> Either (TransactError.Error e) ()
validateEnqueueOptions workflow options = do
  case refuseNamelessWorkflow workflow options.queueName of
    Just err -> Left err
    Nothing -> case refuseEmptyQueueName options.queueName of
      Just err -> Left err
      Nothing -> case refuseAppName "enqueue a workflow" options.appName of
        Left err -> Left (TransactError.liftEngine err)
        Right () -> case refuseEmptyAppVersion options.queueName options.appVersion of
          Just err -> Left err
          Nothing -> validateStart (Just (queueRequest options)) (timeoutBudget options.timeout) options.workflowId options.workflowIdReusePolicy

-- | A plain enqueue onto a queue, asking for nothing else.
enqueueOptionsNew :: Text -> EnqueueOptions
enqueueOptionsNew queueName =
  let request = queueRequestNew queueName
   in EnqueueOptions
        { queueName = request.name,
          deduplicationId = request.deduplicationId,
          duplicationPolicy = request.duplicationPolicy,
          priority = request.priority,
          partitionKey = request.partitionKey,
          delay = request.delay,
          workflowId = Nothing,
          workflowIdReusePolicy = WorkflowIdReturnExisting,
          appName = Nothing,
          appVersion = Nothing,
          timeout = Inherit,
          attributes = Nothing
        }

-- | The queue-shaped half, as a start would say it, which is what the row
-- and the shared checks are written against. Mirrors Rust
-- @EnqueueOptions::queue_request@.
queueRequest :: EnqueueOptions -> QueueRequest
queueRequest options =
  (queueRequestNew options.queueName)
    { deduplicationId = options.deduplicationId,
      duplicationPolicy = options.duplicationPolicy,
      priority = options.priority,
      partitionKey = options.partitionKey,
      delay = options.delay
    }

-- | What a caller may say about a run, beyond the input. A workflow cannot
-- be both queued and waited for here, so there is no queue to name — see
-- 'StartOptions'.
data RunOptions = RunOptions
  { runWorkflowId :: Maybe WorkflowId,
    runWorkflowIdReusePolicy :: WorkflowIdReusePolicy,
    runTimeout    :: Timeout,
    runAttributes :: Maybe (Map Text Value)
  }
  deriving stock (Eq, Show)

-- | What a caller may say about a start, beyond the input. 'RunOptions'
-- plus a queue: a queued start is recorded, not run, so the handle polls.
-- Option fields carry @run@/@start@ prefixes: the facade already exports
-- 'EnqueueOptions' bare spellings, and one module cannot hold the names
-- twice.
data StartOptions = StartOptions
  { startWorkflowId :: Maybe WorkflowId,
    startWorkflowIdReusePolicy :: WorkflowIdReusePolicy,
    startTimeout    :: Timeout,
    startQueue      :: Maybe QueueRequest,
    startAttributes :: Maybe (Map Text Value)
  }
  deriving stock (Eq, Show)

-- | A run that names nothing: a generated id, the parent's budget when
-- there is one, no attributes.
runOptionsDefault :: RunOptions
runOptionsDefault =
  RunOptions
    { runWorkflowId = Nothing,
      runWorkflowIdReusePolicy = WorkflowIdReturnExisting,
      runTimeout = Inherit,
      runAttributes = Nothing
    }

-- | A start that names nothing: a generated id, the parent's budget when
-- there is one, no queue, no attributes.
startOptionsDefault :: StartOptions
startOptionsDefault =
  StartOptions
    { startWorkflowId = Nothing,
      startWorkflowIdReusePolicy = WorkflowIdReturnExisting,
      startTimeout = Inherit,
      startQueue = Nothing,
      startAttributes = Nothing
    }

-- | Every 'RunOptions' converts into 'StartOptions': a run is a start plus
-- an await.
runOptionsToStartOptions :: RunOptions -> StartOptions
runOptionsToStartOptions options =
  StartOptions
    { startWorkflowId = options.runWorkflowId,
      startWorkflowIdReusePolicy = options.runWorkflowIdReusePolicy,
      startTimeout = options.runTimeout,
      startQueue = Nothing,
      startAttributes = options.runAttributes
    }

-- | The id the child will have: the one the caller named, or one derived
-- from where the start stands. Derived rather than random so that a parent
-- recovered mid-run re-derives the same id, finds the child it already
-- started, and adopts it instead of starting a second one. An
-- application-assigned id wins over the derivation, in every reference.
childWorkflowId :: Maybe WorkflowId -> Maybe (Text, Int) -> Text -> Text
childWorkflowId chosen parent generated =
  case (chosen, parent) of
    (Just (WorkflowId offered), _)     -> offered
    (Nothing, Just (parentId, stepId)) -> parentId <> "-" <> pack (show stepId)
    (Nothing, Nothing)                 -> generated

-- | A start's answer: the row this call created, or the holder whose
-- deduplication key its insert lost to. Mirrors Rust @Submitted@.
data Submitted m e
  = SubmittedCreated WorkflowInitResult
  | SubmittedJoined (WorkflowHandle m e)

-- | One start's insert, settling a deduplication collision the way the
-- caller asked: a held key under 'ReturnExisting' hands back a handle to
-- whoever holds it, and anything else reports the collision. A key that
-- frees between the losing insert and the holder read is retried, as the
-- oracle does, rather than reported as a collision with a workflow that is
-- over. Shared by client and reference starts so the two surfaces never
-- diverge. Mirrors Rust @init_or_join@.
initOrJoin :: Monad m
           => Connection m
           -> NewWorkflow
           -> Maybe QueueRequest
           -> WorkflowIdReusePolicy
           -> Maybe InitWorkflowCaller
           -> m (Either Error (Submitted m e))
initOrJoin conn new queue reusePolicy caller = go
  where
    go = do
      initialized <-
        runSystemDB
          conn.connSysdb
          (\db -> SystemDB.initWorkflow db new caller reusePolicy)
      case initialized of
        Right result -> pure (Right (SubmittedCreated result))
        Left err -> case queue of
          Nothing    -> pure (Left err)
          Just shape -> joinHolder shape err
    -- The handle belongs to whoever holds the key, and the announcement
    -- names them both.
    joinHolder shape err =
      case (shape.duplicationPolicy, shape.deduplicationId, err) of
        (ReturnExisting, Just key, QueueDeduplicated {}) -> do
          holder <- runSystemDB conn.connSysdb (\db -> SystemDB.getDeduplicationKeyHolder db shape.name key)
          case holder of
            Right (Just (WorkflowId holderId)) -> do
              runTracer conn.connTracer (WorkflowDedupJoined holderId key)
              pure (Right (SubmittedJoined (pollingHandle conn holderId True)))
            Right Nothing  -> go
            Left lookupErr -> pure (Left lookupErr)
        _ -> pure (Left err)

-- | The recorded error variants a refused child start may hold. Mirrors
-- workflow.rs @RECORDED_START_ERRORS@: the replay returns only these, so a
-- row written by another step — or another SDK's step failure — stays
-- 'UnexpectedStep' rather than being mistaken for a start refusal.
recordedStartErrorNames :: [Text]
recordedStartErrorNames = ["QueueDeduplicated", "ConflictingWorkflow", "WorkflowIdInUse"]

-- | Whether a failed start is recorded at the parent's step: a refused
-- enqueue (another run holds the deduplication key) and an id already
-- taken by a different function are answers a replay must return, not
-- transient trouble to try again. Mirrors @should_record_start_error@.
shouldRecordStartError :: Error -> Bool
shouldRecordStartError err = case err of
  QueueDeduplicated {} -> True
  ConflictingWorkflow {} -> True
  WorkflowIdInUse {} -> True
  _ -> False

-- | Whether a recorded error value names one of the variants above, judged
-- by shape before the typed read: a start error this build cannot decode
-- must be reported as unreadable and name the step, not be mistaken for a
-- plain step's row. Mirrors @is_recorded_start_error@.
isRecordedStartError :: Value -> Bool
isRecordedStartError (Object fields) = case KeyMap.toList fields of
  [(name, _)] -> Key.toText name `elem` recordedStartErrorNames
  _           -> False
isRecordedStartError _ = False

-- | Replays a recorded child start. A recorded child id hands back a handle
-- to it; a recorded start refusal is returned as the same error without
-- starting again; a row with neither stays 'UnexpectedStep'. An unreadable
-- tag is 'UnknownSerialization'; a malformed or undecodable refusal names
-- the step in a 'Malformed' report. Mirrors @Parent::check_child@.
checkRecordedChild :: Monad m
                   => Connection m -> Text -> Int -> Text -> StepRecord
                   -> m (Either (TransactError.Error c) (WorkflowHandle m e))
checkRecordedChild conn parentText stepId stepName stored =
  case stored.stepRecordChildWorkflowId of
    Just (WorkflowId recordedChild) -> do
      runTracer conn.connTracer (WorkflowChildJoined parentText stepId recordedChild)
      pure (Right (pollingHandle conn recordedChild False))
    Nothing -> case stored.stepRecordError of
      Nothing -> pure (Left unexpected)
      Just encoded ->
        case decodeWorkflowValue "a child start's error" (Just (SerializedWorkflowValue encoded (Serialization <$> stored.stepRecordSerialization))) of
          Left codecErr
            | Just tag <- codecUnknownTag codecErr -> do
                runTracer conn.connTracer (StepValueUnreadable stepName stepId tag)
                pure (Left (TransactError.fromSystemDB (UnknownSerialization {name = tag})))
            | otherwise -> pure (Left (malformed (codecErrorText codecErr)))
          Right value
            | not (isRecordedStartError value) -> pure (Left unexpected)
            | otherwise -> case fromJSON value of
                Aeson.Error detail -> pure (Left (malformed (Text.pack detail)))
                Aeson.Success err -> do
                  runTracer conn.connTracer (WorkflowStartErrorReplayed parentText stepId (SystemDBError.renderError err))
                  pure (Left (TransactError.fromSystemDB err))
  where
    unexpected =
      TransactError.fromSystemDB
        ( UnexpectedStep
            { workflowId = parentText,
              stepId = stepId,
              expected = "a child workflow start of " <> stepName,
              recorded = "a plain step named " <> stepName
            }
        )
    malformed detail =
      TransactError.fromSystemDB
        ( Malformed
            ( "workflow " <> parentText <> " step " <> showText stepId <> " (" <> stepName
                <> ") has an error this build cannot read: " <> detail
            )
        )

-- | Records a refused child start at the parent's step, so a replay returns
-- the same error instead of trying the start again. The row carries no
-- child id; the error goes through the workflow's serializer as an
-- internal step's refusal does. A recording failure is returned in place of
-- the refusal, as in the oracle. Mirrors @Parent::record_start_error@.
recordStartError :: (Monad m, MonadTime m)
                 => Connection m -> Text -> Int -> Text -> Timestamp -> Text -> Error
                 -> m (TransactError.Error e)
recordStartError conn parentText stepId stepName startedAt token err = do
  completedAt <- timestampNow
  let encoded = encodeWorkflowValue err
      serialization = (\(Serialization tag) -> tag) <$> encoded.serializedSerialization
  written <-
    runSystemDB
      conn.connSysdb
      ( \db ->
          SystemDB.recordStep
            db
            (WorkflowId parentText)
            stepId
            stepName
            (OutcomeError encoded.serializedText)
            serialization
            (Just (StepTiming startedAt completedAt))
            token
      )
  pure $ case written of
    Left recordErr -> TransactError.fromSystemDB recordErr
    Right ()       -> TransactError.fromSystemDB err

-- | The debounce creation fields a start carries onto its row: the delay
-- capped at the timeout's deadline (as the oracle's executor caps its delay
-- at the debounce deadline), the debounced mark, the stamped deadline, and
-- the acting application. A plain enqueue contributes nothing beyond its
-- delay: no mark, no deadline, the identity's application.
debounceCreation :: Maybe QueueRequest -> (Maybe Duration, Bool, Maybe Duration, Maybe Text)
debounceCreation queue = case queue of
  -- The release cap lives in the init statement, which resolves the delay
  -- and the timeout against one database-clock reading: a Haskell-side
  -- minimum would split the reading the oracle keeps single.
  Just shape | shape.isDebounced ->
    ( shape.delay,
      True,
      shape.debounceTimeout,
      shape.applicationName
    )
  _ -> (queue >>= (.delay), False, Nothing, Nothing)

-- | Starts the referenced workflow durably and returns a handle to it,
-- without waiting. Called outside a workflow this starts a root; the
-- 'startChildWorkflow' form is what a body reaches for. If the id is
-- already owned the handle joins the existing run: the id is an
-- idempotency key. A row this call owns is spawned onto the executor's
-- tasks at once — the workflow's life is the executor's, so dropping the
-- handle must not stop the run — while a queued row is the supervisor's,
-- as the oracle returns a polling handle for enqueues.
startWorkflow :: (MonadFork m, MonadMask m, MonadMVar m, MonadDelay m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
              => Tasks m -> Connection m -> Identity -> Snapshot m (ErasedWorkflow m) -> WorkflowRef m e -> StartOptions -> Maybe SerializedWorkflowValue -> DataSourceRegistry m -> m (Either (TransactError.Error c) (WorkflowHandle m e))
startWorkflow tasks conn identity snapshot ref options input datasources =
  case validateStart options.startQueue (timeoutBudget options.startTimeout) ((\(WorkflowId offered) -> offered) <$> options.startWorkflowId) options.startWorkflowIdReusePolicy of
    Left err -> pure (Left err)
    Right _ -> do
      now <- timestampNow
      generated <- generatedWorkflowId conn
      let key = ref.refQualifiedName
          workflowText = maybe generated (\(WorkflowId offered) -> offered) options.startWorkflowId
          deadline' = resolveTimeoutDeadline options.startTimeout Nothing
          base = workflowNewWorkflow conn identity key (WorkflowId workflowText) input ((.name) <$> options.startQueue)
          (debouncedDelay, debounced, debouncedTimeout, debounceApp) = debounceCreation options.startQueue
          new =
            base
              { newWorkflowDeduplicationId = options.startQueue >>= (.deduplicationId),
                newWorkflowPriority = maybe 0 storedPriority options.startQueue,
                newWorkflowQueuePartitionKey = options.startQueue >>= (.partitionKey),
                newWorkflowDelay = debouncedDelay,
                newWorkflowIsDebounced = debounced,
                newWorkflowDebounceTimeout = debouncedTimeout,
                newWorkflowAppName = debounceApp <|> Just identity.identityAppName,
                -- A row written for another application carries no version: the
                -- runner resolves the target's latest, as the oracle's
                -- @for_another_app@ writes @application_version = None@.
                newWorkflowAppVersion = case debounceApp of
                  Just app | app /= identity.identityAppName -> Nothing
                  _ -> Just identity.identityAppVersion,
                newWorkflowTimeout = timeoutBudget options.startTimeout,
                newWorkflowDeadline = deadline',
                newWorkflowAttributes = encodeAttributes options.startAttributes
              }
      started <- initOrJoin conn new options.startQueue options.startWorkflowIdReusePolicy Nothing
      case started of
        Left err -> pure (Left (TransactError.refusedStart ((.name) <$> options.startQueue) err))
        -- A held deduplication key: the handle belongs to its holder.
        Right (SubmittedJoined holder) -> pure (Right holder)
        Right (SubmittedCreated result)
          -- A fresh start is not a dequeue, so it holds no queue's slot.
          | Just shape <- options.startQueue -> do
              runTracer conn.connTracer (WorkflowEnqueued workflowText shape.name)
              pure (Right (pollingHandle conn workflowText True))
          -- A queued row is the queue's to run. Only a dequeue claim moves it
          -- out of ENQUEUED, and an execution that never held the claim cannot
          -- record an outcome, so a plain start must join the row, not run it.
          | result.initResultStatus == Enqueued || result.initResultStatus == Delayed -> do
              runTracer conn.connTracer (WorkflowAlreadyOwned workflowText)
              pure (Right (pollingHandle conn workflowText True))
          -- Someone else owns this row: joining rather than erroring is
          -- what makes a retried request idempotent.
          | not result.initResultShouldExecute -> do
              runTracer conn.connTracer (WorkflowAlreadyOwned workflowText)
              pure (Right (pollingHandle conn workflowText True))
          -- A finished row is joined, never re-run: a start reusing a
          -- finished id joins the recorded outcome, as the run path does.
          | isTerminal result.initResultStatus || result.initResultStatus == MaxRecoveryAttemptsExceeded -> do
              runTracer conn.connTracer (WorkflowAlreadyOwned workflowText)
              pure (Right (pollingHandle conn workflowText True))
          | otherwise -> case lookupSnapshotWorkflow key snapshot of
              Nothing -> pure (Left (TransactError.NotRegistered key))
              Just workflow -> do
                channel <-
                  spawnLocal
                    (tasksSpawner tasks (clearCheckpointsFor datasources))
                    workflowText
                    ( \spawner ->
                        executeRegisteredWorkflow spawner conn identity (WorkflowId workflowText) workflow input (deadlineOf result.initResultDeadline result.initResultClock) Nothing (WorkflowOwner {ownerWorkflowId = WorkflowId workflowText, ownerToken = result.initResultCreatorXid})
                    )
                pure (Right (localHandle conn workflowText channel))

-- | A registered workflow, holding where it was registered and under which
-- identity. Mirrors Rust @WorkflowRef@: what registration returns and what
-- a call site holds. The registry stores only the erased body; the argument
-- and result types were checked once at registration rather than at every
-- invocation by string. The port holds no type parameters — application
-- values cross the boundary already serialized, as 'ErasedWorkflow' does —
-- and no 'DBOS' handle: drivers that need a launched executor take it
-- explicitly (see 'DBOS.Transact.Instance'). A launch does bind the
-- reference's registry to its connection ('bindRegistryInstance'), which
-- is what 'startChildWorkflow' compares against the
-- running context to refuse a child started through another instance
-- (ADR-0018, reversing the earlier "never pins an instance" note for
-- exactly that refusal).
data WorkflowRef m e = WorkflowRef
  { refRegistry :: Registry m (ErasedWorkflow m),
    refQualifiedName :: QualifiedWorkflowName
  }

instance Show (WorkflowRef m e) where
  show ref = "WorkflowRef " <> Text.unpack (renderQualifiedWorkflowName ref.refQualifiedName)

-- | The workflow's name: the bare name, not the full identity triple.
refName :: WorkflowRef m e -> Text
refName ref = qualifiedName ref.refQualifiedName

-- | The type-erased workflow body the engine resolves from a stored key.
-- Application values and the application error channel have already been
-- serialized by the registration boundary; the body takes the explicit
-- context it runs in. Mirrors the oracle's @ErasedWorkflow@, whose failure
-- channel is @Failure@ for the same reason: recovery and dequeue resolve
-- bodies by name and have no application error type to name.
newtype ErasedWorkflow m = ErasedWorkflow
  { -- | The stored body, run at whatever execution the caller's scope
    -- names: the rank-2 field is why converted and unconverted bodies can
    -- share one registry.
    runErasedWorkflow :: forall exec. Maybe SerializedWorkflowValue -> WorkflowCtx exec m -> m (Either TransactError.Failure (Maybe SerializedWorkflowValue))
  }

-- | The refusal a name nothing could be registered as gets: one that is
-- empty, or one with a config but no class, since a configured instance
-- belongs to a class. Either can be read from a row, which is why
-- 'QualifiedWorkflowName' holds them at all. Mirrors Rust
-- @refuse_unregistrable@.
refuseUnregistrable :: QualifiedWorkflowName -> Maybe (TransactError.Error e)
refuseUnregistrable name
  | Text.null (qualifiedName name) = Just (refuse "a workflow needs a name")
  | Just _ <- qualifiedConfigName name
  , Nothing <- qualifiedClassName name = Just (refuse "a configured instance needs a class")
  | otherwise = Nothing
  where
    refuse detail =
      TransactError.InvalidArgument
        "register a workflow"
        (detail <> ", got `" <> renderQualifiedWorkflowName name <> "`")

-- | Register a typed workflow and hand back a reference to it. Only before
-- launch snapshots the registry; a duplicate identity, or a name nothing
-- could be registered as, is refused.
--
-- Bodies take the scoped workflow view: the converted shape, where a body
-- can only reach the scoped entries. The old context-level entry was
-- deleted once every caller converted (C5b).
registerWorkflowRef :: (FromJSON argument, ToJSON result, ToJSON e, MonadMVar m)
                    => Registry m (ErasedWorkflow m) -> QualifiedWorkflowName -> (forall exec. argument -> WorkflowCtx exec m -> m (Either (TransactError.Error e) result)) -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowRef m e))
registerWorkflowRef registry key body = do
  registered <- registerTypedWorkflow registry key body
  pure ((\() -> WorkflowRef registry key) <$> registered)

-- | Register a typed workflow and erase its JSON input and output types at
-- the registry boundary. The stored representation is the same serialized
-- value used by workflow rows and operation checkpoints.
-- | Register a typed workflow whose body takes the scoped workflow view:
-- the only shape left, now that the engine has no context-level entries —
-- every durable call the body makes takes the view it was handed or one
-- derived from it. The type-erased form is the same either way, so the
-- registry seam is unchanged.
registerTypedWorkflow :: forall argument result e m. (FromJSON argument, ToJSON result, ToJSON e, MonadMVar m)
                      => Registry m (ErasedWorkflow m) -> QualifiedWorkflowName -> (forall exec. argument -> WorkflowCtx exec m -> m (Either (TransactError.Error e) result)) -> m (Either (TransactError.Error TransactError.EngineOnly) ())
registerTypedWorkflow registry key body =
  case refuseUnregistrable key of
    Just err -> pure (Left err)
    Nothing -> do
      registered <-
        Registry.registerErasedWorkflow registry key $ ErasedWorkflow $ \input wctx ->
          case decodeWorkflowInput "argument" input of
            Left err -> pure (Left (TransactError.FailureControl (TransactError.fromSerializationError (codecError "argument" err))))
            Right argument -> do
              result <- body argument wctx
              pure $ case result of
                Left err -> Left (TransactError.failureOf err)
                Right value -> Right (Just (encodeWorkflowValue value))
      pure (first TransactError.fromRegistryError registered)
  where
    codecError :: Text -> CodecError -> SerializationError
    codecError what err =
      case codecUnknownTag err of
        Just tag -> Serialization.UnknownSerialization {name = tag}
        Nothing -> case err of
          CodecNotJson _ input -> Serialization.Deserialization what input
          CodecTypeMismatch _ message -> Serialization.Deserialization what (Text.pack message)
          CodecUnknownSerialization _ tag -> Serialization.UnknownSerialization {name = tag}

-- | Runs the referenced workflow durably and waits for its result: one
-- init records the row with everything the options name, then the shared
-- execute-or-adopt core spawns the body when this call owns the row or
-- awaits whoever does. A run cannot be a start plus a second init — the
-- second init would always read back the start's foreign owner and poll a
-- row nothing runs — so the record and the decision are one upsert.
runWorkflowRef :: (MonadFork m, MonadMask m, MonadMVar m, MonadDelay m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m, FromJSON e)
               => Tasks m -> Connection m -> Identity -> Snapshot m (ErasedWorkflow m) -> WorkflowRef m e -> RunOptions -> Maybe SerializedWorkflowValue -> DataSourceRegistry m -> m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue))
runWorkflowRef tasks conn identity snapshot ref options input datasources = do
  now <- timestampNow
  generated <- generatedWorkflowId conn
  let key = ref.refQualifiedName
      workflowText = maybe generated (\(WorkflowId offered) -> offered) options.runWorkflowId
      new =
        (workflowNewWorkflow conn identity key (WorkflowId workflowText) input Nothing)
          { newWorkflowTimeout = timeoutBudget options.runTimeout,
            newWorkflowDeadline = resolveTimeoutDeadline options.runTimeout Nothing,
            newWorkflowAttributes = encodeAttributes options.runAttributes
          }
  runRegisteredWorkflowWithRow tasks conn identity snapshot key (WorkflowId workflowText) input new options.runWorkflowIdReusePolicy datasources

-- | What a child-creating call resolved before writing: the row's
-- naming, queueing, budget, version and refusal channel. Starts of
-- registered workflows and enqueues by name resolve differently and
-- write through one core, so a replay adopts the same record either
-- way. Mirrors Rust @OwnedStart@ (minus the encoding, which the facade
-- settled, and the debounce creation, which only starts carry).
data ChildLaunch m = ChildLaunch
  { launchKey          :: QualifiedWorkflowName,
    launchStepName   :: Text,
    launchQueue      :: Maybe QueueRequest,
    launchTimeout    :: Timeout,
    launchAppName    :: Maybe Text,
    launchAppVersion :: Maybe Text,
    launchReusePolicy :: WorkflowIdReusePolicy,
    launchAttributes :: Maybe Text,
    launchDebouncedDelay :: Maybe Duration,
    launchDebounced :: Bool,
    launchDebounceTimeout :: Maybe Duration,
    launchRegistry   :: Maybe (Registry m (ErasedWorkflow m))
  }

-- | The doing half of a child-creating call, detached when the context
-- carries a task registry (a test or client-side body has none and
-- writes record-only). The task holds the parent's starts place for as
-- long as it exists, so an aborted write cannot hold its parent open;
-- the place is released on every exit. Token and task are taken under
-- one mask: an async exception between them would leave a counted place
-- no task owns, and the parent's wait for its starts would never end.
-- The wait itself stays interruptible. Mirrors the oracle's detached
-- start, which reports @Interrupted@ when shutdown aborts it.
--
-- The label names the spawned task; 'interruptedId' is the child the
-- caller is waiting on and is what an aborted write reports, so a
-- caller can still look the row up by the id it was given.
detachChildWrite :: (MonadMVar m, MonadMask m, MonadDelay m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m, MonadCatch m)
                 => WorkflowCtx exec m -> Text -> Text -> m (Either (TransactError.Error c) (WorkflowHandle m e)) -> m (Either (TransactError.Error c) (WorkflowHandle m e))
detachChildWrite wctx label interruptedId write =
  case wctx.wctxSpawner of
    Nothing -> write
    Just spawner -> mask $ \_ -> do
      token <- startToken wctx
      channel <-
        spawnLocal spawner label (\_ -> write `finally` releaseStart token)
          `onException` releaseStart token
      -- Empty: the task is in flight and releases its own place. Full:
      -- either shutdown refused the spawn (the place has no task, so the
      -- caller releases it) or the task already finished (it did);
      -- 'releaseStart' is idempotent, so both settle the same way.
      running <- tryReadMVar channel
      case running of
        Nothing -> awaitWrite channel
        Just _ -> do
          releaseStart token
          awaitWrite channel
  where
    awaitWrite channel = do
      settled <- readMVar channel
      case settled of
        LocalTaskValue value -> pure value
        -- Only shutdown aborts a write task. Before its transaction
        -- commits nothing was written; after, the child exists and
        -- recovers and a replay joins it. Either way the caller's
        -- answer is we stopped waiting.
        LocalTaskCancelled -> pure (Left (TransactError.Interrupted {workflowId = interruptedId}))
        LocalTaskPanic err -> throwIO err

-- | Writes a child's row and faces the caller afterwards: the refusal
-- recorded at the parent's step when the insert cannot go, the launch
-- recorded against the parent otherwise, and a handle to what runs. A
-- replay finds the child it already started, or the refusal the call
-- recorded. Shared by registered starts and enqueues by name. Mirrors
-- the oracle's @create@ behind both @start_with@ and @DBOS::enqueue@.
writeChildLaunch :: (MonadMVar m, MonadMask m, MonadDelay m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m, MonadCatch m)
                 => WorkflowCtx exec m -> Int -> Timestamp -> Text -> Maybe SerializedWorkflowValue -> ChildLaunch m -> m (Either (TransactError.Error c) (WorkflowHandle m e))
writeChildLaunch wctx parentStepId now childText rawInput launch = do
  let parentText = workflowId wctx
      conn = wctx.wctxConn
      identity = wctx.wctxIdentity
      ChildLaunch {launchKey = key, launchStepName = name, launchQueue = queue} = launch
      -- A refused write is recorded at this step, so a replay of a
      -- parent that handled the refusal returns the same answer instead
      -- of starting the child for real once the key is free.
      recordRefusal refusal
        | shouldRecordStartError refusal =
            recordStartError conn parentText parentStepId name now (ownerToken wctx) refusal
        | otherwise = pure (TransactError.refusedStart ((.name) <$> queue) refusal)
  recorded <- runSystemDB conn.connSysdb (\db -> SystemDB.checkStep db (WorkflowId parentText) parentStepId name)
  case recorded of
    Left err -> pure (Left (TransactError.fromExecution err))
    -- The launch is recorded against the parent: a replay finds the
    -- child it already started, or the refusal the start recorded.
    Right (Just stored) -> checkRecordedChild conn parentText parentStepId name stored
    Right Nothing -> do
      let childDeadline = resolveTimeoutDeadline launch.launchTimeout (deadline wctx)
          base = workflowNewWorkflow conn identity key (WorkflowId childText) rawInput ((.name) <$> queue)
          new =
            base
              { newWorkflowDeduplicationId = queue >>= (.deduplicationId),
                newWorkflowPriority = maybe 0 storedPriority queue,
                newWorkflowQueuePartitionKey = queue >>= (.partitionKey),
                newWorkflowDelay = launch.launchDebouncedDelay,
                newWorkflowIsDebounced = launch.launchDebounced,
                newWorkflowDebounceTimeout = launch.launchDebounceTimeout,
                newWorkflowAppName = launch.launchAppName,
                newWorkflowAppVersion = launch.launchAppVersion,
                newWorkflowTimeout = timeoutBudget launch.launchTimeout,
                newWorkflowDeadline = childDeadline,
                newWorkflowAttributes = launch.launchAttributes
              }
          caller =
            InitWorkflowCaller
              { initCallerParentWorkflowId = WorkflowId parentText,
                initCallerStepId = parentStepId,
                initCallerStartedAt = now,
                initCallerStepName = name,
                initCallerToken = ownerToken wctx
              }
      started <- initOrJoin conn new queue launch.launchReusePolicy (Just caller)
      case started of
        Right (SubmittedCreated result) -> do
          -- The oracle announces the enqueue at debug whether the start
          -- came from outside or from a workflow body.
          case queue of
            Just shape -> runTracer conn.connTracer (WorkflowEnqueued childText shape.name)
            Nothing    -> pure ()
          spawned <- case (wctx.wctxSpawner, queue) of
            -- A fresh start is not a dequeue, so it holds no queue's
            -- slot; an owned-elsewhere row is already running somewhere.
            (Just spawner, Nothing)
              | result.initResultShouldExecute && not (isTerminal result.initResultStatus) && result.initResultStatus /= MaxRecoveryAttemptsExceeded && result.initResultStatus /= Enqueued && result.initResultStatus /= Delayed -> do
              resolved <- case launch.launchRegistry of
                -- By name: this process may not hold the code, so the
                -- child always runs elsewhere and polls. Unreachable
                -- with a queue, which an enqueue always names.
                Nothing -> pure Nothing
                Just registry -> lookupRegistryWorkflow key registry
              case resolved of
                Nothing | Just _ <- launch.launchRegistry -> pure (Left (TransactError.NotRegistered key))
                Nothing -> pure (Right (pollingHandle conn childText True))
                Just child -> do
                  channel <-
                    spawnLocal spawner childText $ \childSpawner ->
                      -- The child runs under its parent's spawner, so the
                      -- spawner's clearance drops the child's checkpoints
                      -- on completion exactly like a top-level run's.
                      executeRegisteredWorkflow childSpawner conn identity (WorkflowId childText) child rawInput (deadlineOf result.initResultDeadline result.initResultClock) Nothing (WorkflowOwner {ownerWorkflowId = WorkflowId childText, ownerToken = result.initResultCreatorXid})
                  pure (Right (localHandle conn childText channel))
            _ -> pure (Right (pollingHandle conn childText True))
          pure spawned
        Right (SubmittedJoined holder) -> do
          -- The mapping only, never the holder's own parent link:
          -- it travels on 'InitWorkflowCaller' and cannot be
          -- reached from here.
          mapped <- runSystemDB conn.connSysdb (\db -> SystemDB.recordChildWorkflow db (WorkflowId parentText) (WorkflowId holder.workflowId) parentStepId name (Just now) (ownerToken wctx))
          pure $ case mapped of
            Left recordErr -> Left (TransactError.fromExecution recordErr)
            Right _        -> Right holder
        Left err -> do
          surfaced <- recordRefusal err
          pure (Left surfaced)

-- | Starts the referenced workflow as a child of the running one and
-- returns a handle to it. The ambient context decides the parentage, so
-- there is no separate @start_child@: the child's id derives from the
-- parent's step counter unless the options assign one, the start is a
-- checkpoint of the parent (a replayed parent adopts the recorded child
-- without starting anything), and the child inherits the parent's deadline
-- unless it names a budget of its own. A child this call owns runs
-- detached at once — a replayed parent never reaches this branch, so the
-- start keeps its run-once shape across recovery — while a queued child is
-- the supervisor's and an owned-elsewhere child is already running.
--
-- **The writing half runs in a task, and this call awaits it.** A start
-- creates a durable object part-way through its own write, and abandoning
-- the caller does not undo a committed row — so a start dropped between
-- creating the child and recording its step could leave a row nothing
-- runs. Detached, the drop takes the wait and leaves the work: the child
-- is created, its start recorded, and it runs. The task holds the parent's
-- starts count (see 'Context.startToken') for as long as it exists, so a
-- parent that finishes first still waits for it before recording its
-- outcome.
--
-- Awaiting the handle is a second checkpoint the caller takes through
-- 'handleResult'; recording that wait against the parent is Select
-- follow-up. Starting from inside a step is 'InsideStep': a step is a
-- leaf, and an id-allocating call inside one would shift every later step
-- onto the wrong replay slot.
startChildWorkflow :: (MonadMVar m, MonadMask m, MonadDelay m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m, MonadCatch m)
                   => WorkflowCtx exec m -> WorkflowRef m e -> StartOptions -> Maybe WorkflowInput -> m (Either (TransactError.Error c) (WorkflowHandle m e))
startChildWorkflow = startChildWorkflowWith Nothing

-- | 'startChildWorkflow' at a step the caller has already claimed from
-- the counter. The debouncer claims its bounce and its fresh start
-- together, as the oracle's @place@ does, so a coalescing bounce still
-- spends the start's id and a later step lands where the oracle's would.
startChildWorkflowAt :: (MonadMVar m, MonadMask m, MonadDelay m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m, MonadCatch m)
                     => Int -> WorkflowCtx exec m -> WorkflowRef m e -> StartOptions -> Maybe WorkflowInput -> m (Either (TransactError.Error c) (WorkflowHandle m e))
startChildWorkflowAt = startChildWorkflowWith . Just

startChildWorkflowWith :: (MonadMVar m, MonadMask m, MonadDelay m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m, MonadCatch m)
                       => Maybe Int -> WorkflowCtx exec m -> WorkflowRef m e -> StartOptions -> Maybe WorkflowInput -> m (Either (TransactError.Error c) (WorkflowHandle m e))
startChildWorkflowWith placedStep wctx ref options input = do
  -- A start inside a step body is refused — and a start through a captured
  -- parent while a step body runs is the same leaf violation with a scope
  -- field predating the body, which the shared depth counter reports. Read
  -- together, as the oracle reads its ambient scope: refused before
  -- anything is written and before the counter moves.
  stepped <- insideAStep wctx
  if stepped
    then pure (Left (TransactError.InsideStep "starting a workflow"))
    else do
      -- A reference from another instance would take its id from this
      -- workflow's counter while the start record landed through the other
      -- instance's database, where the workflow that allocated it cannot
      -- see it. Refused before anything is written and before the counter
      -- moves, as the oracle's placement does.
      refInstance <- registryInstanceId (ref.refRegistry)
      let conn = wctx.wctxConn
      case refInstance of
        Nothing -> pure (Left (TransactError.NotLaunched {operation = "start a workflow"}))
        Just instanceId
          | instanceId /= conn.connInstanceId ->
              pure (Left (TransactError.WrongInstance {operation = "start a workflow"}))
          | otherwise -> case validateStart options.startQueue (timeoutBudget options.startTimeout) ((\(WorkflowId offered) -> offered) <$> options.startWorkflowId) options.startWorkflowIdReusePolicy of
              Left err -> pure (Left err)
              Right _ -> do
                -- Decided at the call, before anything is detached: the
                -- step the start occupies and the id it would create. An
                -- interrupted start still names the child in its answer,
                -- and a replay of this position derives the same id.
                parentStepId <- maybe (nextStepId wctx) pure placedStep
                now <- timestampNow
                generated <- generatedWorkflowId conn
                let parentText = workflowId wctx
                    childText = childWorkflowId options.startWorkflowId (Just (parentText, parentStepId)) generated
                    (debouncedDelay, debounced, debouncedTimeout, debounceApp) = debounceCreation options.startQueue
                    launch =
                      ChildLaunch
                        { launchKey = key,
                          launchStepName = name,
                          launchQueue = options.startQueue,
                          launchTimeout = options.startTimeout,
                          launchAppName = debounceApp <|> Just identity.identityAppName,
                          launchAppVersion = case debounceApp of
                            -- A row written for another application carries
                            -- no version: the runner resolves the target's
                            -- latest, as the oracle's @for_another_app@
                            -- writes @application_version = None@.
                            Just app | app /= identity.identityAppName -> Nothing
                            _ -> Just identity.identityAppVersion,
                          launchReusePolicy = options.startWorkflowIdReusePolicy,
                          launchAttributes = encodeAttributes options.startAttributes,
                          launchDebouncedDelay = debouncedDelay,
                          launchDebounced = debounced,
                          launchDebounceTimeout = debouncedTimeout,
                          launchRegistry = Just ref.refRegistry
                        }
                detachChildWrite wctx ("start " <> childText) childText (writeChildLaunch wctx parentStepId now childText rawInput launch)
  where
    key = ref.refQualifiedName
    name = refName ref
    conn = wctx.wctxConn
    identity = wctx.wctxIdentity
    rawInput = unWorkflowInput <$> input

enqueueWorkflowByNameInWorkflow :: (MonadMVar m, MonadMask m, MonadDelay m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m, MonadCatch m)
                             => WorkflowCtx exec m -> QualifiedWorkflowName -> EnqueueOptions -> Maybe WorkflowInput -> m (Either (TransactError.Error c) (WorkflowHandle m e))
enqueueWorkflowByNameInWorkflow wctx name options input = do
  -- An enqueue inside a step body is refused, as a start is: a step is
  -- a leaf, and an id-allocating call inside one would shift every later
  -- step onto the wrong replay slot. Everything refusable is refused
  -- before the counter moves, as the oracle reads its ambient scope.
  stepped <- insideAStep wctx
  if stepped
    then pure (Left (TransactError.InsideStep "enqueueing a workflow"))
    else case validateEnqueueOptions name options of
      Left err -> pure (Left err)
      Right _ -> do
        -- Decided at the call, before anything is detached: the step the
        -- enqueue occupies and the id it would create — the parent's
        -- counter, unless the caller pinned one. A replay of this
        -- position derives the same id.
        parentStepId <- nextStepId wctx
        now <- timestampNow
        generated <- generatedWorkflowId conn
        let parentText = workflowId wctx
            childText = childWorkflowId (WorkflowId <$> options.workflowId) (Just (parentText, parentStepId)) generated
            request = queueRequest options
            launch =
              ChildLaunch
                { launchKey = name,
                  launchStepName = qualifiedName name,
                  launchQueue = Just request,
                  launchTimeout = options.timeout,
                  -- The row belongs to this application unless the call
                  -- names another, and carries no version unless one is
                  -- named, so the application's latest version dequeues
                  -- it: this process may not hold the code, so its own
                  -- version says nothing about who can run it. Mirrors
                  -- Rust @DBOS::enqueue@.
                  launchAppName = options.appName <|> Just identity.identityAppName,
                  launchAppVersion = options.appVersion,
                  launchReusePolicy = options.workflowIdReusePolicy,
                  launchAttributes = encodeAttributes options.attributes,
                  launchDebouncedDelay = Nothing,
                  launchDebounced = False,
                  launchDebounceTimeout = Nothing,
                  launchRegistry = Nothing
                }
        detachChildWrite wctx ("enqueue " <> childText) childText (writeChildLaunch wctx parentStepId now childText rawInput launch)
  where
    conn = wctx.wctxConn
    identity = wctx.wctxIdentity
    rawInput = unWorkflowInput <$> input

-- | What a spawned task does for the executor, which decides when
-- shutdown stops it. One set per role, because shutdown treats them in
-- turn rather than all at once: the producers stop, the workflows drain,
-- and only then are the monitors stopped. Mirrors Rust @Role@.
data Role
  = -- | Starts workflows in this process, or enqueues them: the dequeue
    -- loop and the scheduler. Stopped first, so the set being drained
    -- cannot grow.
    Producer
  | -- | Runs a workflow, or a child's start on its parent's behalf. What
    -- shutdown drains.
    Workflow
  | -- | Watches workflows: the timeout sweep. Stopped after the drain,
    -- because a workflow still running may be waiting on it to record
    -- its timeout.
    Monitor
  deriving stock (Eq, Show)

-- | Abort handles to reach running tasks with, and a count of how many are
-- still alive to wait on. Mirrors Rust @Tasks@: nothing here holds a
-- joinable handle, because a caller that dropped its future is no longer
-- waiting though the workflow is still running — which is exactly the case
-- shutdown has to reach. The count stands in for joining, and is what lets
-- shutdown mean "quiet" rather than "told to stop".
--
-- One set per 'Role' rather than one set with a role on each task: the
-- set a spawn is tracked in carries its role, so 'spawnTracked' takes the
-- set and the label but no role.

data Tasks m = Tasks  { tasksState :: StrictTVar m (TaskState m),
    -- | Serializes a spawn's check-fork-register against the sweep's
    -- flag-snapshot, so no arrival can land between the count and the
    -- registry the kill list is read from. Held only across non-blocking
    -- steps (one STM commit, a fork, a kill), never across a wait.
    tasksLock                      :: StrictMVar m ()
  }

data TaskState m = TaskState
  { running  :: [(ThreadId m, Text)],
    -- | How many spawned tasks exist and have not departed.
    live     :: Int,
    -- | Set by 'abortAll'. An arrival past this point is refused outright
    -- (no fork, no count) rather than added to a list nothing will read
    -- again.
    closed   :: Bool,
    -- | Tasks that departed before the parent registered them: a fast body
    -- on a parallel scheduler can run to completion between the fork and
    -- the registration. The imminent registration consumes the entry
    -- instead of listing a dead thread, so a later sweep never kills the
    -- dead nor counts it as aborted.
    orphaned :: [ThreadId m]
  }

-- | An empty task set.
newTasks :: (MonadSTM m, MonadMVar m) => m (Tasks m)
newTasks = do
  state <- newTVarIO (TaskState [] 0 False [])
  lock <- newMVar ()
  pure (Tasks state lock)

-- | Spawns a task counted and reachable by shutdown. The check, the
-- count, the fork, and the registration all happen under the registry
-- lock, against which the sweep snapshots its kill list — so a shutdown
-- racing this one either refuses the arrival (closed: no fork, no count,
-- the caller fills the refusal itself) or finds the task in the registry
-- (open: the sweep's kill reaches it). Before the lock, an arrival that
-- landed between the count and the registration was waited on but never
-- killed: a finite body would run past shutdown and record an outcome
-- instead of staying @PENDING@, and an infinite supervisor or poll loop
-- would hold the count above zero forever and hang the sweep. A refused
-- arrival never forks, so no kill can land before a child that was never
-- born — which is what left refused outcome boxes empty before.
spawnTracked :: forall m. (MonadFork m, MonadMask m, MonadSTM m, MonadMVar m)
             => Tasks m -> Text -> m () -> m (Maybe (ThreadId m))
spawnTracked tasks label action = mask $ \restore ->
  -- One lock hold around check, count, fork, and registration: the sweep
  -- snapshots its kill list under the same lock, so an arrival is either
  -- refused before the flag flips or registered before the snapshot is
  -- taken. Nothing inside blocks (STM commits, a fork, a kill), so the
  -- hold is microseconds and cannot deadlock the sweep.
  withMVar tasks.tasksLock $ \_ -> do
    accepted <- atomically $ do
      st <- readTVar tasks.tasksState
      if st.closed
        then pure False
        else do
          writeTVar tasks.tasksState st { live = st.live + 1 }
          pure True
    if not accepted
      then pure Nothing
      else do
        tid <- forkIO $ do
          self <- myThreadId
          outcome <- try (restore action) :: m (Either SomeException ())
          departed tasks self
          either throwIO pure outcome
        -- Registration under the same hold: the flag cannot have flipped
        -- since the check above, so this always registers — but the child
        -- may already have departed on a parallel scheduler, in which case
        -- the entry 'departed' left behind is consumed instead of listing
        -- a dead thread. A shutdown past this point finds the task in its
        -- snapshot exactly when it is still alive.
        atomically $ do
          st <- readTVar tasks.tasksState
          if tid `elem` st.orphaned
            then writeTVar tasks.tasksState st { orphaned = mapMaybe (\t -> if t == tid then Nothing else Just t) st.orphaned }
            else writeTVar tasks.tasksState st { running = (tid, label) : st.running }
        pure (Just tid)

-- | Sets the closed flag, kills every registered task, and waits until the
-- count reaches zero. The flag flip and the kill-list snapshot hold the
-- registry lock, against which every spawn checks, forks, and registers —
-- so the sweep cannot miss a task it must wait for: each arrival is
-- either refused or in the list. Only the snapshot is serialized; the
-- kills and the wait run outside the lock. Returns the labels of the
-- tasks it cancelled, for shutdown to say what it stopped. Rows stay
-- @PENDING@: an aborted workflow is recovered by the next launch, which
-- is what makes shutdown safe rather than lossy.
abortAll :: (MonadFork m, MonadSTM m, MonadMVar m) => Tasks m -> m [Text]
abortAll tasks = do
  toAbort <- withMVar tasks.tasksLock $ \_ -> atomically $ do
    st <- readTVar tasks.tasksState
    writeTVar tasks.tasksState st { running = [], orphaned = [], closed = True }
    pure st.running
  mapM_ (killThread . fst) toAbort
  waitAll tasks
  pure (map snd toAbort)

-- | Waits until no task is left alive, counting any that arrive while it
-- waits. Nothing is cancelled: what is still running is left to
-- 'abortAll'.
waitAll :: (MonadSTM m) => Tasks m -> m ()
waitAll tasks = atomically $ do
  st <- readTVar tasks.tasksState
  unless (st.live == 0) retry

-- | 'waitAll', bounded by a monotonic-nanosecond deadline: reports whether
-- every task ended before it. Returns as soon as the last task ends, not
-- at the deadline. Nothing is cancelled, at the deadline or before it.
-- Raced rather than polled, so a task ending just before the deadline
-- still reports ended.
waitAllUntil :: (MonadSTM m, MonadAsync m, MonadDelay m, MonadMonotonicTimeNSec m) => Tasks m -> Word64 -> m Bool
waitAllUntil tasks deadline = do
  now <- getMonotonicTimeNSec
  if now >= deadline
    then (== 0) . (.live) <$> readTVarIO tasks.tasksState
    else do
      let micros = min (fromIntegral (maxBound :: Int)) ((deadline - now) `div` 1000)
      winner <- race (waitAll tasks) (threadDelay (fromIntegral micros))
      case winner of
        Left () -> pure True
        -- The timer won; re-read rather than reporting timed-out, since a
        -- task may have ended in the same instant.
        Right () -> (== 0) . (.live) <$> readTVarIO tasks.tasksState

-- | The labels of the tasks currently registered, for a shutdown that is
-- waiting on them to say what it is waiting on.
runningTasks :: (MonadSTM m) => Tasks m -> m [Text]
runningTasks tasks = map snd . (.running) <$> readTVarIO tasks.tasksState

-- | Sleeps up to the given microseconds between a producer loop's passes,
-- unless shutdown begins first. True when it slept the whole wait and the
-- loop should make another pass; False once shutdown has begun, and at
-- once rather than at the end of the sleep. The producers stop here,
-- between passes, rather than wherever an abort would land: a dequeue
-- whose claim has committed goes on to start what it claimed, so a
-- graceful shutdown drains those workflows instead of leaving them
-- PENDING and unstarted. Homed here because both the instance and the
-- than the reverse. Mirrors Rust @Executor::sleep_unless_stopping@: a
-- stop landing mid-sleep ends it at once, so unlike the oracle's biased
-- select either order at the same instant is benign — an extra pass only
-- runs work already claimed.
sleepUnlessStopping :: (MonadSTM m, MonadAsync m, MonadDelay m) => StrictTVar m Bool -> Int -> m Bool
sleepUnlessStopping stopping micros = do
  already <- readTVarIO stopping
  if already
    then pure False
    else do
      winner <- race waitStop (threadDelay micros)
      pure (either (const False) (const True) winner)
  where
    waitStop = atomically $ do
      stopNow <- readTVar stopping
      unless stopNow retry
-- | Whether shutdown has begun, for a producer about to start a pass.
-- Mirrors Rust @Executor::is_stopping@.
isStopping :: (MonadSTM m) => StrictTVar m Bool -> m Bool
isStopping = readTVarIO

-- | Records a departing task, saturating and deregistering: a double
-- departure must not undercount, because an undercount would let
-- 'abortAll' return early, and a finished task must not be killed again by
-- a later sweep or held in the registry forever. A departure the parent
-- has not registered yet (a fast body outrunning the fork) leaves an
-- orphan entry for the imminent registration to consume, rather than a
-- dead thread for a later sweep to kill and count. Past 'closed' no
-- registration is coming, so nothing is left behind; a repeat departure
-- (a kill racing a normal exit) only settles the count.
departed tasks tid = atomically $ do
  st <- readTVar tasks.tasksState
  if tid `elem` map fst st.running
    then writeTVar tasks.tasksState st { live = max 0 (st.live - 1), running = mapMaybe (\t -> if fst t == tid then Nothing else Just t) st.running }
    else if st.closed || tid `elem` st.orphaned
      then writeTVar tasks.tasksState st { live = max 0 (st.live - 1) }
      else writeTVar tasks.tasksState st { live = max 0 (st.live - 1), orphaned = tid : st.orphaned }
