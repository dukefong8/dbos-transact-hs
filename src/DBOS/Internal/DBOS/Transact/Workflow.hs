{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | Internal workflow runner (Rule 4: plain Haskell, no Bluefin imports).
-- Runs a registered body and records its outcome: @SUCCESS@ with the output,
-- or @ERROR@ with the failure. A missing row is started first; an existing
-- row runs straight into the body, whose steps replay from their
-- checkpoints — which is the whole of crash recovery at this layer. A row
-- that already carries an outcome replays it without running the body, and a
-- row owned by another executor is left alone (@WorkflowClaimLost@):
-- re-running either would overwrite finished work or steal a live claim.
module DBOS.Transact.Workflow
  ( runRegisteredWorkflow,
    runRegisteredWorkflowWithSubmission,
    runRegisteredWorkflowWithRow,
    enqueueWorkflow,
    Enqueue (..),
    DuplicationPolicy (..),
    enqueueNew,
    validateEnqueue,
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
    resolveEnqueueCollision,
    startWorkflowRef,
    runWorkflowRef,
    startChildWorkflow,
    maxRecoveryAttempts,
    spawnRegisteredWorkflowWithRow,
    workflowNewWorkflow,
    -- * Task ownership (workflow.rs @Tasks@)
    Tasks,
    newTasks,
    spawnTracked,
    tasksSpawner,
    abortAll,
  )
where

import DBOS.Prelude
-- NOTE (deviation): the one base import outside the port's prelude. Mapping
-- the abort channel needs the identity of the exception 'killThread'
-- throws, and io-classes exposes no async-exception identity — exactly what
-- the oracle matches with @join.is_cancelled()@. Nothing here forks,
-- throws, or waits through base; all effects stay on io-classes.
import Control.Exception (AsyncException (..))
import Control.Monad.Class.MonadThrow qualified as MThrow
import Data.Aeson (FromJSON, Value)
import Data.Int (Int64)
import Data.Map.Strict (Map)
import Data.Text (pack)
import Data.Text qualified as Text
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Error (Error (..))
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (AwaitedOutcome (..), Duration, InitWorkflowCaller (..), NewWorkflow (..), Outcome (..), OutcomeWrite (..), Serialization (..), SerializedWorkflowValue (..), StepRecord (..), Submission (..), Timestamp, WorkflowId (..), WorkflowInitResult (..), WorkflowStatus (..), addTimeout, newWorkflow, timestampNow, timestampToEpochMs)
import DBOS.Transact.Config (serializerName)
import DBOS.Transact.Connection (Connection (..), generatedWorkflowId, runSystemDB)
import DBOS.Transact.Context (LocalTaskOutcome (..), TaskSpawner (..), WorkflowCtx (wctxConn, wctxIdentity, wctxSpawner), deadline, insideAStep, nextStepId, spawnLocal, withWorkflow, withWorkflowTaskSpawner, workflowId)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Handle (WorkflowHandle (..), localHandle, pollingHandle)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Logger (runTracer)
import DBOS.Transact.Registry (ErasedWorkflow (..), Snapshot, WorkflowKey (..), WorkflowRef (refKey, refRegistry), lookupRegistryWorkflow, lookupSnapshotWorkflow, refName, registryInstanceId, renderWorkflowKey)
import DBOS.Transact.Serialization (encodeAttributes)
import DBOS.Transact.Step (WorkflowEvent (..))

-- | Attempts before a workflow is parked as
-- @MAX_RECOVERY_ATTEMPTS_EXCEEDED@. Mirrors workflow.rs.
maxRecoveryAttempts :: Int64
maxRecoveryAttempts = 100

-- | Start a registered workflow using the SystemDB class-backed engine.
-- The connection, resolved identity and task registry belong to the
-- executor; the body takes the explicit context it runs in.
runRegisteredWorkflow :: (MonadFork m, MThrow.MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, FromJSON e) => Tasks m -> Connection m -> Identity -> Snapshot m -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue))
runRegisteredWorkflow = runRegisteredWorkflowWithSubmission Fresh

runRegisteredWorkflowWithSubmission :: (MonadFork m, MThrow.MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, FromJSON e) => Submission -> Tasks m -> Connection m -> Identity -> Snapshot m -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue))
runRegisteredWorkflowWithSubmission submission tasks conn identity snapshot key workflowId input =
  runRegisteredWorkflowWithRow submission tasks conn identity snapshot key workflowId input (workflowNewWorkflow conn identity key workflowId input Nothing)

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
runRegisteredWorkflowWithRow :: (MonadFork m, MThrow.MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, FromJSON e) => Submission -> Tasks m -> Connection m -> Identity -> Snapshot m -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> NewWorkflow -> m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue))
runRegisteredWorkflowWithRow submission tasks conn identity snapshot key workflowId@(WorkflowId workflowText) input new =
  case lookupSnapshotWorkflow key snapshot of
    Nothing -> pure (Left (TransactError.ErrorWorkflowNotRegistered (renderWorkflowKey key)))
    Just workflow -> do
      initialized <- runSystemDB conn.connSysdb (\db -> SystemDB.initWorkflow db new (Just maxRecoveryAttempts) submission Nothing)
      case initialized of
        Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
        Right result
          | not result.initResultShouldExecute -> do
              runTracer conn.connTracer (WorkflowAlreadyOwned workflowText)
              adoptRecordedOutcome conn workflowId
          | otherwise -> do
              channel <-
                spawnLocal
                  (tasksSpawner tasks)
                  ( \spawner ->
                      executeRegisteredWorkflow spawner conn identity workflowId workflow input result.initResultDeadline
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
                LocalTaskPanic err -> MThrow.throwIO err
-- | The spawner the engine injects into an execution's context, closing
-- over the executor's task registry: a child started from a body is
-- detached, counted, and abortable by shutdown, exactly like a directly
-- spawned execution. Self-referential on purpose — the capability an
-- execution hands its grandchildren is its own. The outcome lands in the
-- returned box while the task unwinds, so a handle awaiting a task the
-- shutdown aborted still learns 'LocalTaskCancelled' rather than hanging.
tasksSpawner :: (MonadFork m, MThrow.MonadMask m, MonadSTM m, MonadMVar m) => Tasks m -> TaskSpawner m
tasksSpawner tasks = spawner
  where
    spawner = TaskSpawner $ \action -> do
      channel <- newEmptyMVar
      spawned <- spawnTracked tasks $ MThrow.mask $ \restore -> do
        result <- MThrow.try (restore (action spawner))
        -- The fill below is masked back over: only the body runs
        -- interruptibly, so a kill landing around the body still leaves the
        -- outcome in the box.
        putMVar channel $ case result of
          Right value -> LocalTaskValue value
          Left err -> case MThrow.fromException err :: Maybe AsyncException of
            Just ThreadKilled -> LocalTaskCancelled
            _                 -> LocalTaskPanic err
      -- A refused arrival never forked, so the parent fills the
      -- cancellation itself: no child exists to do it, and the waiter must
      -- still learn the outcome rather than hang on an empty box.
      case spawned of
        Nothing -> putMVar channel LocalTaskCancelled
        Just _  -> pure ()
      pure channel

-- | Park-and-adopt: wait for whoever owns the row and report its recorded
-- outcome as an erased failure. Mirrors @Connection::adopt@: the recorded
-- outcome is the answer, so a superseded execution — or a caller joining an
-- owned row — does not return what it computed itself. The recorded failure
-- travels as the payload the row holds, and the caller's channel decides
-- how to decode it.
adoptRecordedFailure :: (MonadDelay m, MonadTime m) => Connection m -> WorkflowId -> m (Either TransactError.Failure (Maybe SerializedWorkflowValue))
adoptRecordedFailure conn wid@(WorkflowId workflowText) = do
  awaited <- runSystemDB conn.connSysdb (\db -> SystemDB.awaitWorkflowResult db wid conn.connOutcomePollInterval True)
  pure $ case awaited of
    Left err -> Left (TransactError.FailureControl (TransactError.ErrorSystemDatabase err))
    Right (AwaitedSucceeded output serialization) ->
      Right (SerializedWorkflowValue <$> output <*> pure (Serialization <$> serialization))
    Right (AwaitedFailed message _) -> Left (TransactError.FailureRecorded message)
    Right AwaitedCancelled ->
      Left
        ( TransactError.FailureRecorded
            (TransactError.encodeErrorText (TransactError.AwaitedWorkflowCancelled {workflowId = workflowText} :: (TransactError.Error TransactError.EngineOnly)))
        )
    Right (AwaitedParked attempts) ->
      Left
        ( TransactError.FailureControl
            ( TransactError.ErrorSystemDatabase
                (SystemDBError.ErrorMaxRecoveryAttemptsExceeded {workflowId = workflowText, limit = attempts})
            )
        )

-- | The typed face of 'adoptRecordedFailure': the recorded failure decoded
-- back into the caller's channel.
adoptRecordedOutcome :: (MonadDelay m, MonadTime m, FromJSON e) => Connection m -> WorkflowId -> m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue))
adoptRecordedOutcome conn (WorkflowId workflowText) = do
  adopted <- adoptRecordedFailure conn (WorkflowId workflowText)
  pure (either (Left . TransactError.failureError workflowText) Right adopted)
-- | Runs one execution of a registered body as its owner: a fresh execution
-- identity, the row's stored deadline, the body, and the outcome write. The
-- row's stored deadline rides the execution, so a recovered workflow keeps
-- what is left, and children inherit the instant rather than a fresh budget.
-- The spawner rides the context, so a child this body starts is detached
-- into the same registry and hands its own children the same capability.
executeRegisteredWorkflow :: (MonadTimer m, MonadTime m, MThrow.MonadCatch m) => TaskSpawner m -> Connection m -> Identity -> WorkflowId -> ErasedWorkflow m -> Maybe SerializedWorkflowValue -> Maybe Timestamp -> m (Either TransactError.Failure (Maybe SerializedWorkflowValue))
executeRegisteredWorkflow spawner conn identity workflowId@(WorkflowId workflowText) workflow input deadline = do
  attempted <- MThrow.try (runBody)
  case attempted of
    -- Shutdown aborts the task: the row stays PENDING on purpose, with no
    -- panic line — a kill is not a bug. Anything else escaping the body is
    -- one, and the row it leaves behind is for recovery. Mirrors
    -- @PanicLog@: the guard fires on the unwind, never on a returned
    -- outcome.
    Left se -> case MThrow.fromException se of
      Just ThreadKilled -> MThrow.throwIO se
      _ -> do
        runTracer conn.connTracer (WorkflowPanicked workflowText)
        MThrow.throwIO se
    Right value -> pure value
  where
    -- The scope the body runs in: fresh counters, the row's deadline, and
    -- the executor's task spawner, so a converted body's scoped entries
    -- reach the same machinery the context-level ones did. The row's stored
    -- deadline is the run's budget: the body races the clock, and losing it
    -- cancels the workflow durably, as Rust's deadline does.
    runBody =
      withWorkflow conn identity workflowId deadline $ \wctx -> do
        let scoped = withWorkflowTaskSpawner spawner wctx
            ErasedWorkflow body = workflow
        raced <- case deadline of
          Nothing -> Just <$> body input scoped
          Just due -> do
            now <- timestampNow
            let remainingMillis = max 0 (timestampToEpochMs due - timestampToEpochMs now)
            timeout (fromIntegral remainingMillis * 1000) (body input scoped)
        case raced of
          Nothing -> deadlineLost
          Just outcome -> case outcome of
            Left failure
              -- A control signal is not the workflow's outcome: the execution
              -- records nothing of its own. Warned, because the caller may
              -- have dropped its future and this is the only evidence.
              | isControlFailure failure -> do
                  runTracer conn.connTracer (WorkflowControlEnded (TransactError.renderTransactError (failureControl failure)))
                  pure (Left failure)
              | otherwise -> case failure of
                  TransactError.FailureRecorded payload -> recordOutcome (OutcomeError payload) (Left failure)
                  -- Unreachable by construction: only control failures are
                  -- `Control`, and those were handled above.
                  TransactError.FailureControl err      -> recordOutcome (OutcomeError (TransactError.encodeErrorText (TransactError.liftEngine err :: (TransactError.Error TransactError.EngineOnly)))) (Left failure)
            Right output -> recordOutcome (OutcomeOutput ((.serializedText) <$> output)) (Right output)
    -- Records one outcome and reports what the write decided: recording a
    -- second outcome behind a finished row is a supersede, not a write, and
    -- the recorded outcome is the answer.
    recordOutcome outcome fallback = do
      saved <- runSystemDB conn.connSysdb (\db -> SystemDB.recordWorkflowOutcome db workflowId outcome)
      case saved of
        Left dbError -> do
          runTracer conn.connTracer (WorkflowOutcomeRecordFailed (TransactError.renderTransactError (TransactError.ErrorSystemDatabase dbError :: (TransactError.Error TransactError.EngineOnly))))
          pure (Left (TransactError.FailureControl (TransactError.ErrorSystemDatabase dbError)))
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
      TransactError.FailureRecorded payload -> TransactError.ErrorWorkflowFailed {workflowId = workflowText, message = payload}
    -- A durable cancellation, unless the row already reached an outcome while
    -- the race ran — then the recorded outcome is what the run reports, as
    -- Rust's deadline-loses-to-outcome rule does.
    deadlineLost = do
      runTracer conn.connTracer (WorkflowDeadlineCancelled workflowText)
      cancelled <- runSystemDB conn.connSysdb (\db -> SystemDB.cancelWorkflows db [workflowId] False Nothing)
      case cancelled of
        Left dbError -> do
          runTracer conn.connTracer (WorkflowDeadlineRecordFailed workflowText (TransactError.renderTransactError (TransactError.ErrorSystemDatabase dbError :: (TransactError.Error TransactError.EngineOnly))))
          pure (Left (TransactError.FailureControl (TransactError.ErrorSystemDatabase dbError)))
        Right [] -> do
          runTracer conn.connTracer (WorkflowDeadlineRaced workflowText)
          adoptRecordedFailure conn workflowId
        Right _ -> pure (Left (TransactError.FailureControl (TransactError.ErrorSystemDatabase (SystemDBError.WorkflowCancelled {workflowId = workflowText}))))

-- | Initializes a claimed row and spawns its body as a tracked task,
-- mirroring @dequeue.rs@'s @dispatch@: the claim already flipped the row to
-- @PENDING@, so the status it reports is checked once more and anything but
-- @PENDING@ is left alone. A parked row (the recovery cap is exceeded) is a
-- skip, not a fault. The release action runs when the spawned task ends, or
-- immediately on every path that does not spawn.
spawnRegisteredWorkflowWithRow :: (MonadFork m, MThrow.MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) => Tasks m -> m () -> Submission -> Connection m -> Identity -> Snapshot m -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> NewWorkflow -> m (Either (TransactError.Error TransactError.EngineOnly) (Maybe (ThreadId m)))
spawnRegisteredWorkflowWithRow tasks release submission conn identity snapshot key workflowId@(WorkflowId _) input new =
  case lookupSnapshotWorkflow key snapshot of
    Nothing -> release >> pure (Left (TransactError.ErrorWorkflowNotRegistered (renderWorkflowKey key)))
    Just workflow -> do
      initialized <- runSystemDB conn.connSysdb (\db -> SystemDB.initWorkflow db new (Just maxRecoveryAttempts) submission Nothing)
      case initialized of
        Left SystemDBError.ErrorMaxRecoveryAttemptsExceeded {} -> release >> pure (Right Nothing)
        Left err -> release >> pure (Left (TransactError.ErrorSystemDatabase err))
        Right result
          | result.initResultStatus /= Pending -> release >> pure (Right Nothing)
          | otherwise -> do
              spawned <-
                spawnTracked
                  tasks
                  ( MThrow.finally
                      (void (executeRegisteredWorkflow (tasksSpawner tasks) conn identity workflowId workflow input result.initResultDeadline))
                      release
                  )
              case spawned of
                -- Refused by a closed registry: no task exists to run the
                -- release, so it runs here — the "every path that does not
                -- spawn releases immediately" half of the contract above.
                Nothing  -> release >> pure (Right Nothing)
                Just tid -> pure (Right (Just tid))

-- | Create an enqueued workflow row without running its body. A queue worker
-- later claims it and invokes the registered body with the dequeue
-- submission.
enqueueWorkflow :: Monad m => Connection m -> Identity -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) WorkflowInitResult)
enqueueWorkflow conn identity key workflowId input queueName = do
  initialized <- runSystemDB conn.connSysdb (\db -> SystemDB.initWorkflow db (workflowNewWorkflow conn identity key workflowId input (Just queueName)) (Just maxRecoveryAttempts) Fresh Nothing)
  pure (either (Left . TransactError.ErrorSystemDatabase) Right initialized)

workflowNewWorkflow :: Connection m -> Identity -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> Maybe Text -> NewWorkflow
workflowNewWorkflow conn identity key (WorkflowId workflowText) input queueName =
  let serialization = case input >>= (.serializedSerialization) of
        Just (Serialization name) -> name
        Nothing                   -> serializerName conn.connSerializer
      (workflowName, className, configName) = case key of
        WorkflowKey name class' config' -> (name, class', config')
   in (newWorkflow workflowText)
        { newWorkflowName = Just workflowName,
          newWorkflowClassName = className,
          newWorkflowConfigName = configName,
          newWorkflowInput = (.serializedText) <$> input,
          newWorkflowSerialization = Just serialization,
          newWorkflowQueueName = queueName,
          newWorkflowExecutorId = Just identity.identityExecutorId,
          newWorkflowApplicationName = Just identity.identityAppName,
          newWorkflowApplicationVersion = Just identity.identityAppVersion,
          newWorkflowApplicationId = if Text.null identity.identityAppId then Nothing else Just identity.identityAppId
        }

-- | What an enqueue asks of its queue: deduplication, priority, partition,
-- delay, and — only for the debouncer's fresh creation — the debounce
-- creation flags. Mirrors Rust @Enqueue@ in @workflow.rs@, which the client's
-- @EnqueueOptions@ and the runtime's start options both build on so the two
-- surfaces never spell the same four things differently; the two debounce
-- fields mirror TypeScript's @EnqueueOptions@ (@isDebounced@,
-- @debounceDeadlineEpochMS@), which only the debounce names — the oracle's
-- own comment says no other caller does — so a plain enqueue stays
-- non-debounced with no deadline.
data Enqueue = Enqueue
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
enqueueNew :: Text -> Enqueue
enqueueNew queueName =
  Enqueue
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
-- policy without a key, and the priority range.
validateEnqueue :: Enqueue -> Either (TransactError.Error TransactError.EngineOnly) ()
validateEnqueue queue
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
  | otherwise = Right ()
  where
    refuse detail = Left (TransactError.ErrorConfig ("enqueue onto `" <> queue.name <> "`: " <> detail))
    -- The column is a signed 32-bit integer; 'Nothing' is the only way to
    -- say unprioritised, because 0 is the stored sentinel for it.
    maxPriority = 2147483647 :: Word32

-- | The stored priority: the sentinel @0@ when unprioritised. Infallible
-- because 'validateEnqueue' has already ruled out what would not fit.
storedPriority :: Enqueue -> Int
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

-- | The deadline to record on the row: what a direct start stamps now, and
-- what a queued start leaves null for the claim to fill in. An explicit
-- budget on a queued workflow records the budget and no deadline — the wait
-- in the queue is not part of it. An inherited deadline is the parent's
-- instant, shared with no signal passing between them.
resolveTimeoutDeadline :: Timeout -> Maybe Enqueue -> Maybe Timestamp -> Timestamp -> Maybe Timestamp
resolveTimeoutDeadline timeout queue parentDeadline now =
  case timeout of
    Explicit budget -> case queue of
      Just _  -> Nothing
      Nothing -> addTimeout now budget
    None -> Nothing
    Inherit -> parentDeadline

-- | What a caller may say about a run, beyond the input. A workflow cannot
-- be both queued and waited for here, so there is no queue to name — see
-- 'StartOptions'.
data RunOptions = RunOptions
  { runWorkflowId :: Maybe WorkflowId,
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
    startTimeout    :: Timeout,
    startQueue      :: Maybe Enqueue,
    startAttributes :: Maybe (Map Text Value)
  }
  deriving stock (Eq, Show)

-- | A run that names nothing: a generated id, the parent's budget when
-- there is one, no attributes.
runOptionsDefault :: RunOptions
runOptionsDefault =
  RunOptions
    { runWorkflowId = Nothing,
      runTimeout = Inherit,
      runAttributes = Nothing
    }

-- | A start that names nothing: a generated id, the parent's budget when
-- there is one, no queue, no attributes.
startOptionsDefault :: StartOptions
startOptionsDefault =
  StartOptions
    { startWorkflowId = Nothing,
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

-- | Settles a deduplication collision the way the caller asked: a held key
-- under 'ReturnExisting' hands back a handle to whoever holds it, and
-- anything else reports the collision. Shared by client and reference
-- enqueues so the two surfaces never diverge.
resolveEnqueueCollision :: Monad m => Connection m -> Enqueue -> Text -> Error -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
resolveEnqueueCollision conn shape _offeredId err =
  case (shape.duplicationPolicy, shape.deduplicationId) of
    (ReturnExisting, Just key) -> case err of
      QueueDeduplicated {} -> do
        holder <- runSystemDB conn.connSysdb (\db -> SystemDB.getDeduplicationKeyHolder db shape.name key)
        case holder of
          -- Mirrors the oracle's join: the handle belongs to whoever holds
          -- the key, and the announcement names them both.
          Right (Just (WorkflowId holderId)) -> do
            runTracer conn.connTracer (WorkflowDedupJoined holderId key)
            pure (Right (pollingHandle conn holderId True))
          Right Nothing -> pure (Left (TransactError.ErrorSystemDatabase err))
          Left lookupErr -> pure (Left (TransactError.ErrorSystemDatabase lookupErr))
      _ -> pure (Left (TransactError.ErrorSystemDatabase err))
    _ -> pure (Left (TransactError.ErrorSystemDatabase err))

-- | The debounce creation fields a start carries onto its row: the delay
-- capped at the timeout's deadline (as the oracle's executor caps its delay
-- at the debounce deadline), the debounced mark, the stamped deadline, and
-- the acting application. A plain enqueue contributes nothing beyond its
-- delay: no mark, no deadline, the identity's application.
debounceCreation :: Maybe Enqueue -> Timestamp -> (Maybe Duration, Bool, Maybe Timestamp, Maybe Text)
debounceCreation queue now = case queue of
  Just shape | shape.isDebounced ->
    ( cappedDelay shape.delay shape.debounceTimeout,
      True,
      shape.debounceTimeout >>= addTimeout now,
      shape.applicationName
    )
  _ -> (queue >>= (.delay), False, Nothing, Nothing)
  where
    -- The earlier of the period's wake and the timeout's deadline, as
    -- durations from the same now the deadline stamps above.
    cappedDelay delay timeout = case (delay, timeout) of
      (Just period, Just limit) -> Just (min period limit)
      (delayed, _)              -> delayed

-- | Starts the referenced workflow durably and returns a handle to it,
-- without waiting. Called outside a workflow this starts a root; the
-- 'startChildWorkflow' form is what a body reaches for. If the id is
-- already owned the handle joins the existing run: the id is an
-- idempotency key. A row this call owns is spawned onto the executor's
-- tasks at once — the workflow's life is the executor's, so dropping the
-- handle must not stop the run — while a queued row is the supervisor's,
-- as the oracle returns a polling handle for enqueues.
startWorkflowRef :: (MonadFork m, MThrow.MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) => Tasks m -> Connection m -> Identity -> Snapshot m -> WorkflowRef m e -> StartOptions -> Maybe SerializedWorkflowValue -> m (Either (TransactError.Error c) (WorkflowHandle m e))
startWorkflowRef tasks conn identity snapshot ref options input =
  case traverse validateEnqueue options.startQueue of
    Left err -> pure (Left (TransactError.liftEngine err))
    Right _ -> do
      now <- timestampNow
      generated <- generatedWorkflowId conn
      let key = ref.refKey
          workflowText = maybe generated (\(WorkflowId offered) -> offered) options.startWorkflowId
          deadline' = resolveTimeoutDeadline options.startTimeout options.startQueue Nothing now
          base = workflowNewWorkflow conn identity key (WorkflowId workflowText) input ((.name) <$> options.startQueue)
          (debouncedDelay, debounced, debouncedDeadline, debounceApp) = debounceCreation options.startQueue now
          new =
            base
              { newWorkflowDeduplicationId = options.startQueue >>= (.deduplicationId),
                newWorkflowPriority = maybe 0 storedPriority options.startQueue,
                newWorkflowQueuePartitionKey = options.startQueue >>= (.partitionKey),
                newWorkflowDelay = debouncedDelay,
                newWorkflowIsDebounced = debounced,
                newWorkflowDebounceDeadline = debouncedDeadline,
                newWorkflowApplicationName = debounceApp <|> Just identity.identityAppName,
                newWorkflowTimeout = timeoutBudget options.startTimeout,
                newWorkflowDeadline = deadline',
                newWorkflowAttributes = encodeAttributes options.startAttributes
              }
      initialized <- runSystemDB conn.connSysdb (\db -> SystemDB.initWorkflow db new (Just maxRecoveryAttempts) Fresh Nothing)
      case initialized of
        Right result
          -- A fresh start is not a dequeue, so it holds no queue's slot.
          | Just shape <- options.startQueue -> do
              runTracer conn.connTracer (WorkflowEnqueued workflowText shape.name)
              pure (Right (pollingHandle conn workflowText True))
          -- Someone else owns this row: joining rather than erroring is
          -- what makes a retried request idempotent.
          | not result.initResultShouldExecute -> do
              runTracer conn.connTracer (WorkflowAlreadyOwned workflowText)
              pure (Right (pollingHandle conn workflowText True))
          | otherwise -> case lookupSnapshotWorkflow key snapshot of
              Nothing -> pure (Left (TransactError.ErrorWorkflowNotRegistered (renderWorkflowKey key)))
              Just workflow -> do
                channel <-
                  spawnLocal
                    (tasksSpawner tasks)
                    ( \spawner ->
                        executeRegisteredWorkflow spawner conn identity (WorkflowId workflowText) workflow input result.initResultDeadline
                    )
                pure (Right (localHandle conn workflowText channel))
        Left err -> case options.startQueue of
          Just shape -> fmap (either (Left . TransactError.liftEngine) Right) (resolveEnqueueCollision conn shape workflowText err)
          Nothing    -> pure (Left (TransactError.ErrorSystemDatabase err))

-- | Runs the referenced workflow durably and waits for its result: one
-- init records the row with everything the options name, then the shared
-- execute-or-adopt core spawns the body when this call owns the row or
-- awaits whoever does. A run cannot be a start plus a second init — the
-- second init would always read back the start's foreign owner and poll a
-- row nothing runs — so the record and the decision are one upsert.
runWorkflowRef :: (MonadFork m, MThrow.MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, FromJSON e) => Tasks m -> Connection m -> Identity -> Snapshot m -> WorkflowRef m e -> RunOptions -> Maybe SerializedWorkflowValue -> m (Either (TransactError.Error e) (Maybe SerializedWorkflowValue))
runWorkflowRef tasks conn identity snapshot ref options input = do
  now <- timestampNow
  generated <- generatedWorkflowId conn
  let key = ref.refKey
      workflowText = maybe generated (\(WorkflowId offered) -> offered) options.runWorkflowId
      new =
        (workflowNewWorkflow conn identity key (WorkflowId workflowText) input Nothing)
          { newWorkflowTimeout = timeoutBudget options.runTimeout,
            newWorkflowDeadline = resolveTimeoutDeadline options.runTimeout Nothing Nothing now,
            newWorkflowAttributes = encodeAttributes options.runAttributes
          }
  runRegisteredWorkflowWithRow Fresh tasks conn identity snapshot key (WorkflowId workflowText) input new

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
-- Awaiting the handle is a second checkpoint the caller takes through
-- 'handleResult'; recording that wait against the parent is Select
-- follow-up. Starting from inside a step is 'InsideStep': a step is a
-- leaf, and an id-allocating call inside one would shift every later step
-- onto the wrong replay slot.
startChildWorkflow :: (MonadMVar m, MonadTimer m, MonadTime m, MThrow.MonadCatch m) => WorkflowCtx exec m -> WorkflowRef m e -> StartOptions -> Maybe SerializedWorkflowValue -> m (Either (TransactError.Error c) (WorkflowHandle m e))
startChildWorkflow wctx ref options input = do
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
        Nothing -> pure (Left (TransactError.ErrorNotLaunched {operation = "start a workflow"}))
        Just instanceId
          | instanceId /= conn.connInstanceId ->
              pure (Left (TransactError.WrongInstance {operation = "start a workflow"}))
          | otherwise -> case traverse validateEnqueue options.startQueue of
              Left err -> pure (Left (TransactError.liftEngine err))
              Right _ -> do
                parentStepId <- nextStepId wctx
                startChild parentStepId
  where
    key = ref.refKey
    name = refName ref
    conn = wctx.wctxConn
    identity = wctx.wctxIdentity
    startChild parentStepId = do
      now <- timestampNow
      generated <- generatedWorkflowId conn
      let parentText = workflowId wctx
          childText = childWorkflowId options.startWorkflowId (Just (parentText, parentStepId)) generated
      recorded <- runSystemDB conn.connSysdb (\db -> SystemDB.checkStep db (WorkflowId parentText) parentStepId name)
      case recorded of
        Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
        Right (Just stored) -> case stored.stepRecordChildWorkflowId of
          -- The launch is recorded against the parent: a replay finds the
          -- child it already started. The child's own row belongs to the
          -- earlier run, so an absent row stays "not yet".
          Just (WorkflowId recordedChild) -> do
            runTracer conn.connTracer (WorkflowChildJoined parentText parentStepId recordedChild)
            pure (Right (pollingHandle conn recordedChild False))
          Nothing ->
            pure
              ( Left
                  ( TransactError.ErrorSystemDatabase
                      ( UnexpectedStep
                          { workflowId = parentText,
                            stepId = parentStepId,
                            expected = "a child workflow start of " <> name,
                            recorded = "a plain step named " <> name
                          }
                      )
                  )
              )
        Right Nothing -> do
          let childDeadline = resolveTimeoutDeadline options.startTimeout options.startQueue (deadline wctx) now
              base = workflowNewWorkflow conn identity key (WorkflowId childText) input ((.name) <$> options.startQueue)
              (debouncedDelay, debounced, debouncedDeadline, debounceApp) = debounceCreation options.startQueue now
              new =
                base
                  { newWorkflowDeduplicationId = options.startQueue >>= (.deduplicationId),
                    newWorkflowPriority = maybe 0 storedPriority options.startQueue,
                    newWorkflowQueuePartitionKey = options.startQueue >>= (.partitionKey),
                    newWorkflowDelay = debouncedDelay,
                    newWorkflowIsDebounced = debounced,
                    newWorkflowDebounceDeadline = debouncedDeadline,
                    newWorkflowApplicationName = debounceApp <|> Just identity.identityAppName,
                    newWorkflowTimeout = timeoutBudget options.startTimeout,
                    newWorkflowDeadline = childDeadline,
                    newWorkflowAttributes = encodeAttributes options.startAttributes
                  }
              caller =
                InitWorkflowCaller
                  { initCallerParentWorkflowId = WorkflowId parentText,
                    initCallerStepId = parentStepId,
                    initCallerStartedAt = now,
                    initCallerStepName = name
                  }
          initialized <- runSystemDB conn.connSysdb (\db -> SystemDB.initWorkflow db new (Just maxRecoveryAttempts) Fresh (Just caller))
          case initialized of
            Right result -> do
              -- The oracle announces the enqueue at debug whether the start
              -- came from outside or from a workflow body.
              case options.startQueue of
                Just shape -> runTracer conn.connTracer (WorkflowEnqueued childText shape.name)
                Nothing    -> pure ()
              spawned <- case (wctx.wctxSpawner, options.startQueue) of
                -- A fresh start is not a dequeue, so it holds no queue's
                -- slot; an owned-elsewhere row is already running somewhere.
                (Just spawner, Nothing) | result.initResultShouldExecute -> do
                  resolved <- lookupRegistryWorkflow key (ref.refRegistry)
                  case resolved of
                    Nothing -> pure (Left (TransactError.ErrorWorkflowNotRegistered (renderWorkflowKey key)))
                    Just child -> do
                      channel <-
                        spawnLocal spawner $ \childSpawner ->
                          executeRegisteredWorkflow childSpawner conn identity (WorkflowId childText) child input result.initResultDeadline
                      pure (Right (localHandle conn childText channel))
                _ -> pure (Right (pollingHandle conn childText True))
              pure spawned
            Left err -> case options.startQueue of
              Just shape -> do
                joined <- resolveEnqueueCollision conn shape childText err
                case joined of
                  Right holder -> do
                    -- The mapping only, never the holder's own parent link:
                    -- it travels on 'InitWorkflowCaller' and cannot be
                    -- reached from here.
                    mapped <- runSystemDB conn.connSysdb (\db -> SystemDB.recordChildWorkflow db (WorkflowId parentText) (WorkflowId holder.workflowId) parentStepId name (Just now))
                    pure $ case mapped of
                      Left recordErr -> Left (TransactError.ErrorSystemDatabase recordErr)
                      Right _        -> Right holder
                  Left joinErr -> pure (Left (TransactError.liftEngine joinErr))
              Nothing -> pure (Left (TransactError.ErrorSystemDatabase err))

-- | Abort handles to reach running tasks with, and a count of how many are
-- still alive to wait on. Mirrors Rust @Tasks@: nothing here holds a
-- joinable handle, because a caller that dropped its future is no longer
-- waiting though the workflow is still running — which is exactly the case
-- shutdown has to reach. The count stands in for joining, and is what lets
-- shutdown mean "quiet" rather than "told to stop".

data Tasks m = Tasks  { tasksState :: StrictTVar m (TaskState m),
    -- | Serializes a spawn's check-fork-register against the sweep's
    -- flag-snapshot, so no arrival can land between the count and the
    -- registry the kill list is read from. Held only across non-blocking
    -- steps (one STM commit, a fork, a kill), never across a wait.
    tasksLock                      :: StrictMVar m ()
  }

data TaskState m = TaskState
  { running  :: [ThreadId m],
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
spawnTracked :: forall m. (MonadFork m, MThrow.MonadMask m, MonadSTM m, MonadMVar m) => Tasks m -> m () -> m (Maybe (ThreadId m))
spawnTracked tasks action = MThrow.mask $ \restore ->
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
          outcome <- MThrow.try (restore action) :: m (Either MThrow.SomeException ())
          departed tasks self
          either MThrow.throwIO pure outcome
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
            else writeTVar tasks.tasksState st { running = tid : st.running }
        pure (Just tid)

-- | Sets the closed flag, kills every registered task, and waits until the
-- count reaches zero. The flag flip and the kill-list snapshot hold the
-- registry lock, against which every spawn checks, forks, and registers —
-- so the sweep cannot miss a task it must wait for: each arrival is
-- either refused or in the list. Only the snapshot is serialized; the
-- kills and the wait run outside the lock. Rows stay @PENDING@: an aborted
-- workflow is recovered by the next launch, which is what makes shutdown
-- safe rather than lossy.
abortAll :: (MonadFork m, MonadSTM m, MonadMVar m) => Tasks m -> m Int
abortAll tasks = do
  toAbort <- withMVar tasks.tasksLock $ \_ -> atomically $ do
    st <- readTVar tasks.tasksState
    writeTVar tasks.tasksState st { running = [], orphaned = [], closed = True }
    pure st.running
  mapM_ killThread toAbort
  atomically $ do
    st <- readTVar tasks.tasksState
    if st.live == 0 then pure () else retry
  pure (length toAbort)

-- | Records a departing task, saturating and deregistering: a double
-- departure must not undercount, because an undercount would let
-- 'abortAll' return early, and a finished task must not be killed again by
-- a later sweep or held in the registry forever. A departure the parent
-- has not registered yet (a fast body outrunning the fork) leaves an
-- orphan entry for the imminent registration to consume, rather than a
-- dead thread for a later sweep to kill and count. Past 'closed' no
-- registration is coming, so nothing is left behind; a repeat departure
-- (a kill racing a normal exit) only settles the count.
departed :: (MonadFork m, MonadSTM m) => Tasks m -> ThreadId m -> m ()
departed tasks tid = atomically $ do
  st <- readTVar tasks.tasksState
  if tid `elem` st.running
    then writeTVar tasks.tasksState st { live = max 0 (st.live - 1), running = mapMaybe (\t -> if t == tid then Nothing else Just t) st.running }
    else if st.closed || tid `elem` st.orphaned
      then writeTVar tasks.tasksState st { live = max 0 (st.live - 1) }
      else writeTVar tasks.tasksState st { live = max 0 (st.live - 1), orphaned = tid : st.orphaned }
