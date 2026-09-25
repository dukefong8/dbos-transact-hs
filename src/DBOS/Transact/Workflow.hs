{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Internal workflow runner (Rule 4: plain Haskell, no Bluefin imports).
-- Runs a registered body and records its outcome: @SUCCESS@ with the output,
-- or @ERROR@ with the failure. A missing row is started first; an existing
-- row runs straight into the body, whose steps replay from their
-- checkpoints — which is the whole of crash recovery at this layer. A row
-- that already carries an outcome replays it without running the body, and a
-- row owned by another executor is left alone (@WorkflowClaimLost@):
-- re-running either would overwrite finished work or steal a live claim.
module DBOS.Transact.Workflow
  ( WorkflowRunError (..),
    runWorkflow,
    runRegisteredWorkflow,
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
    abortAll,
  )
where

import DBOS.Prelude
import Control.Monad (void)
import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM, StrictTVar, atomically, newTVarIO, readTVar, retry, writeTVar)
import Control.Monad.Class.MonadFork (MonadFork, ThreadId, forkIO, killThread, myThreadId)
import Control.Monad.Class.MonadTime (MonadTime)
import Control.Monad.Class.MonadTimer (MonadDelay, MonadTimer, timeout)
import Control.Monad.Class.MonadThrow qualified as MThrow
import Data.Int (Int64)
import Data.Maybe (fromMaybe)
import Data.Aeson (Value)
import Data.Map.Strict (Map)
import Data.Text qualified as Text
import Data.Word (Word32)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Error (Error (..))
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (ApplicationVersion, AwaitedOutcome (..), Duration, ExecutorId, InitWorkflowCaller (..), NewWorkflow (..), Outcome (..), Serialization (..), SerializedWorkflowValue (..), Submission (..), Timestamp, WorkflowId (..), WorkflowInitResult, WorkflowName (..), WorkflowStatus (..), addTimeout, newWorkflow, timestampNow, timestampToEpochMs)
import DBOS.SystemDB.Postgres
  ( Pool,
    WorkflowStartDecision (..),
    fetchWorkflowExecutionRow,
    tryStartWorkflow,
    updateWorkflowOutcome,
  )
import DBOS.Transact.Codec (CodecError (..), decodeWorkflowValue, encodeAttributes, encodeWorkflowValue)
import DBOS.Transact.Config (serializerName)
import DBOS.Transact.Connection (Connection (..), generatedWorkflowId, nextExecutionIdentity, runSystemDB)
import DBOS.Transact.Context (Ctx, currentConnection, currentIdentity, deadline, inStep, newCtx, newWorkflowState, nextStepId, workflowId)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Handle (WorkflowHandle (..), pollingHandle)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Registry (ErasedWorkflow, Snapshot, WorkflowKey (..), WorkflowRef, WorkflowRegistry, lookupSnapshotWorkflow, lookupWorkflow, refKey, refName, renderWorkflowKey)
import DBOS.Transact.WorkflowExecutionParse (parseWorkflowExecution)
import DBOS.Transact.WorkflowExecutionTypes
  ( WorkflowExecution (..),
    WorkflowOutcome (..),
  )
import Data.Text (Text, pack)

-- | Attempts before a workflow is parked as
-- @MAX_RECOVERY_ATTEMPTS_EXCEEDED@. Mirrors workflow.rs.
maxRecoveryAttempts :: Int64
maxRecoveryAttempts = 100

-- | Start a registered workflow using the SystemDB class-backed engine.
-- The connection and resolved identity belong to the executor; the body
-- takes the explicit context it runs in.
runRegisteredWorkflow :: (MonadSTM m, MonadDelay m, MonadTimer m, MonadTime m) => Connection m -> Identity -> Snapshot m -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> m (Either TransactError.Error (Maybe SerializedWorkflowValue))
runRegisteredWorkflow = runRegisteredWorkflowWithSubmission Fresh

runRegisteredWorkflowWithSubmission :: (MonadSTM m, MonadDelay m, MonadTimer m, MonadTime m) => Submission -> Connection m -> Identity -> Snapshot m -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> m (Either TransactError.Error (Maybe SerializedWorkflowValue))
runRegisteredWorkflowWithSubmission submission conn identity snapshot key workflowId input =
  runRegisteredWorkflowWithRow submission conn identity snapshot key workflowId input (workflowNewWorkflow conn identity key workflowId input Nothing)

-- | The execute-or-adopt core both runners share: one init decides whether
-- this call owns the row. A fresh row (or a recovery/dequeue claim) runs
-- the body; an existing row awaits whoever owns it. Splitting record and
-- run across two inits would always read back a foreign owner — each init
-- mints its own — and poll a row nothing runs, so a run is one init, never
-- a start plus a second init.
runRegisteredWorkflowWithRow :: (MonadSTM m, MonadDelay m, MonadTimer m, MonadTime m) => Submission -> Connection m -> Identity -> Snapshot m -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> NewWorkflow -> m (Either TransactError.Error (Maybe SerializedWorkflowValue))
runRegisteredWorkflowWithRow submission conn identity snapshot key workflowId@(WorkflowId workflowText) input new =
  case lookupSnapshotWorkflow key snapshot of
    Nothing -> pure (Left (TransactError.ErrorWorkflowNotRegistered (renderWorkflowKey key)))
    Just workflow -> do
      initialized <- runSystemDB conn.connSysdb (\db -> SystemDB.initWorkflow db new (Just maxRecoveryAttempts) submission Nothing)
      case initialized of
        Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
        Right result
          | not result.initResultShouldExecute ->
              awaitExisting
          | otherwise ->
              executeRegisteredWorkflow conn identity workflowId workflow input result.initResultDeadline
  where
    awaitExisting = do
      awaited <- runSystemDB conn.connSysdb (\db -> SystemDB.awaitWorkflowResult db workflowId conn.connOutcomePollInterval True)
      pure $ case awaited of
        Left err -> Left (TransactError.ErrorSystemDatabase err)
        Right (AwaitedSucceeded output serialization) ->
          Right (SerializedWorkflowValue <$> output <*> pure (Serialization <$> serialization))
        Right (AwaitedFailed message _) -> Left (TransactError.ErrorWorkflowFailed workflowText message)
        Right AwaitedCancelled ->
          Left
            ( TransactError.ErrorSystemDatabase
                (SystemDBError.WorkflowCancelled {workflowId = workflowText})
            )
        Right (AwaitedParked attempts) ->
          Left
            ( TransactError.ErrorSystemDatabase
                (SystemDBError.ErrorMaxRecoveryAttemptsExceeded {workflowId = workflowText, limit = attempts})
            )
-- | Runs one execution of a registered body as its owner: a fresh execution
-- identity, the row's stored deadline, the body, and the outcome write. The
-- row's stored deadline rides the execution, so a recovered workflow keeps
-- what is left, and children inherit the instant rather than a fresh budget.
executeRegisteredWorkflow :: (MonadSTM m, MonadDelay m, MonadTimer m, MonadTime m) => Connection m -> Identity -> WorkflowId -> ErasedWorkflow m -> Maybe SerializedWorkflowValue -> Maybe Timestamp -> m (Either TransactError.Error (Maybe SerializedWorkflowValue))
executeRegisteredWorkflow conn identity workflowId@(WorkflowId workflowText) workflow input deadline = do
  executionId <- nextExecutionIdentity conn
  state <- newWorkflowState workflowText deadline executionId
  ctx <- newCtx conn identity state
  -- The row's stored deadline is the run's budget: the body races the clock,
  -- and losing it cancels the workflow durably, as Rust's deadline does.
  raced <- case deadline of
    Nothing -> Just <$> workflow input ctx
    Just due -> do
      now <- timestampNow
      let remainingMillis = max 0 (timestampToEpochMs due - timestampToEpochMs now)
      timeout (fromIntegral remainingMillis * 1000) (workflow input ctx)
  case raced of
    Nothing -> deadlineLost
    Just outcome -> case outcome of
      Left err
        | isControlError err -> pure (Left err)
        | otherwise -> do
            saved <- runSystemDB conn.connSysdb (\db -> SystemDB.recordWorkflowOutcome db workflowId (OutcomeError (TransactError.renderTransactError err)))
            pure $ case saved of
              Left dbError -> Left (TransactError.ErrorSystemDatabase dbError)
              Right _ -> Left err
      Right output -> do
        let outputText = (.serializedText) <$> output
        saved <- runSystemDB conn.connSysdb (\db -> SystemDB.recordWorkflowOutcome db workflowId (OutcomeOutput outputText))
        pure $ case saved of
          Left err -> Left (TransactError.ErrorSystemDatabase err)
          Right _ -> Right output
  where
    isControlError err = case err of
      TransactError.ErrorSystemDatabase _ -> True
      _ -> False
    -- A durable cancellation, unless the row already reached an outcome while
    -- the race ran — then the recorded outcome is what the run reports, as
    -- Rust's deadline-loses-to-outcome rule does.
    deadlineLost = do
      cancelled <- runSystemDB conn.connSysdb (\db -> SystemDB.cancelWorkflows db [workflowId] False Nothing)
      case cancelled of
        Left dbError -> pure (Left (TransactError.ErrorSystemDatabase dbError))
        Right [] -> do
          awaited <- runSystemDB conn.connSysdb (\db -> SystemDB.awaitWorkflowResult db workflowId conn.connOutcomePollInterval True)
          pure $ case awaited of
            Left err -> Left (TransactError.ErrorSystemDatabase err)
            Right (AwaitedSucceeded output serialization) -> Right (SerializedWorkflowValue <$> output <*> pure (Serialization <$> serialization))
            Right (AwaitedFailed message _) -> Left (TransactError.ErrorWorkflowFailed workflowText message)
            Right AwaitedCancelled -> Left (TransactError.ErrorSystemDatabase (SystemDBError.WorkflowCancelled {workflowId = workflowText}))
            Right (AwaitedParked _) -> Left (TransactError.ErrorSystemDatabase (SystemDBError.WorkflowCancelled {workflowId = workflowText}))
        Right _ -> pure (Left (TransactError.ErrorSystemDatabase (SystemDBError.WorkflowCancelled {workflowId = workflowText})))

-- | Initializes a claimed row and spawns its body as a tracked task,
-- mirroring @dequeue.rs@'s @dispatch@: the claim already flipped the row to
-- @PENDING@, so the status it reports is checked once more and anything but
-- @PENDING@ is left alone. A parked row (the recovery cap is exceeded) is a
-- skip, not a fault. The release action runs when the spawned task ends, or
-- immediately on every path that does not spawn.
spawnRegisteredWorkflowWithRow :: (MonadFork m, MThrow.MonadMask m, MonadSTM m, MonadTimer m, MonadTime m) => Tasks m -> m () -> Submission -> Connection m -> Identity -> Snapshot m -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> NewWorkflow -> m (Either TransactError.Error (Maybe (ThreadId m)))
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
              tid <-
                spawnTracked
                  tasks
                  ( MThrow.finally
                      (void (executeRegisteredWorkflow conn identity workflowId workflow input result.initResultDeadline))
                      release
                  )
              pure (Right (Just tid))

-- | Create an enqueued workflow row without running its body. A queue worker
-- later claims it and invokes the registered body with the dequeue
-- submission.
enqueueWorkflow :: Monad m => Connection m -> Identity -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> Text -> m (Either TransactError.Error WorkflowInitResult)
enqueueWorkflow conn identity key workflowId input queueName = do
  initialized <- runSystemDB conn.connSysdb (\db -> SystemDB.initWorkflow db (workflowNewWorkflow conn identity key workflowId input (Just queueName)) (Just maxRecoveryAttempts) Fresh Nothing)
  pure (either (Left . TransactError.ErrorSystemDatabase) Right initialized)

workflowNewWorkflow :: Connection m -> Identity -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> Maybe Text -> NewWorkflow
workflowNewWorkflow conn identity key (WorkflowId workflowText) input queueName =
  let serialization = case input >>= (.serializedSerialization) of
        Just (Serialization name) -> name
        Nothing -> serializerName conn.connSerializer
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

data WorkflowRunError
  = WorkflowNotRegistered WorkflowName
  | WorkflowClaimLost WorkflowId
  | WorkflowBodyFailed Text
  deriving stock (Eq, Show)

runWorkflow ::
  Pool ->
  WorkflowRegistry ->
  WorkflowName ->
  WorkflowId ->
  Maybe SerializedWorkflowValue ->
  ExecutorId ->
  ApplicationVersion ->
  IO (Either WorkflowRunError SerializedWorkflowValue)
runWorkflow pool registry name workflowId input executorId applicationVersion =
  case lookupWorkflow name registry of
    Nothing -> pure (Left (WorkflowNotRegistered name))
    Just body -> do
      existing <- fetchWorkflowExecutionRow pool workflowId
      case existing of
        Nothing -> do
          decision <- tryStartWorkflow pool workflowId name input executorId applicationVersion
          case decision of
            StartWorkflow -> execute body
            AwaitWorkflow -> pure (Left (WorkflowClaimLost workflowId))
        Just row -> case parseWorkflowExecution row of
          -- Unreadable rows keep the legacy path: the body runs and its
          -- steps replay from whatever checkpoints decode.
          Left _ -> execute body
          Right execution -> case execution.workflowExecutionOutcome of
            Just (WorkflowSucceeded output) -> pure (Right output)
            Just (WorkflowFailed errValue) -> pure (Left (WorkflowBodyFailed (replayErrorText errValue)))
            _ -> case execution.workflowExecutionStatus of
              Cancelled -> pure (Left (WorkflowClaimLost workflowId))
              _ -> case execution.workflowExecutionExecutor of
                -- tryStart hands out ownership by writing our id; anything
                -- else is another runner's claim (or a queue row waiting for
                -- one) and running it here would steal it.
                Just owner | owner == executorId -> execute body
                _ -> pure (Left (WorkflowClaimLost workflowId))
  where
    execute body = do
      outcome <- try (body pool workflowId input)
      case outcome of
        Right output -> do
          updateWorkflowOutcome pool workflowId executorId Success (Just output) Nothing
          pure (Right output)
        -- Control and infrastructure errors are non-recorded: a cancelled
        -- workflow keeps its PENDING row for a later launch to recover, and
        -- a database outage must not become a permanent ERROR outcome. Only
        -- a failure of the body itself is recorded.
        Left failure
          | Just AsyncCancelled <- fromException failure -> throwIO failure
          | Just (_ :: Error) <- fromException failure -> throwIO failure
          | otherwise -> do
              let complaint = encodeWorkflowValue (show (failure :: MThrow.SomeException))
              updateWorkflowOutcome pool workflowId executorId Error Nothing (Just complaint)
              pure (Left (WorkflowBodyFailed (pack (show failure))))

-- | The error text a fresh failure reported, recovered from the stored
-- encoding so a replay reports the same string its first run did. A stored
-- value that no longer decodes falls back to its raw text rather than
-- failing the replay.
replayErrorText :: SerializedWorkflowValue -> Text
replayErrorText stored@(SerializedWorkflowValue errText _) =
  case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
    Right text -> text
    Left _ -> errText

-- | What an enqueue asks of its queue: deduplication, priority, partition,
-- delay. Mirrors Rust @Enqueue@ in @workflow.rs@, which the client's
-- @EnqueueOptions@ and the runtime's start options both build on so the two
-- surfaces never spell the same four things differently.
data Enqueue = Enqueue
  { name :: Text,
    deduplication_id :: Maybe Text,
    priority :: Maybe Word32,
    partition_key :: Maybe Text,
    delay :: Maybe Duration,
    duplication_policy :: DuplicationPolicy
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
      deduplication_id = Nothing,
      priority = Nothing,
      partition_key = Nothing,
      delay = Nothing,
      duplication_policy = Reject
    }

-- | Rejects an enqueue no queue could honour. Only what the shape could not
-- rule out: nesting under the queue already makes a delay with no queue
-- unbuildable, so what is left is the deduplication/partition pair, the
-- policy without a key, and the priority range.
validateEnqueue :: Enqueue -> Either TransactError.Error ()
validateEnqueue queue
  | Just _ <- queue.deduplication_id,
    Just _ <- queue.partition_key =
      refuse
        "`deduplication_id` and `partition_key` cannot both be set: a partitioned queue's dequeue and a deduplication key enforce different things"
  | queue.duplication_policy == ReturnExisting,
    Nothing <- queue.deduplication_id =
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
    Inherit -> Nothing
    None -> Nothing

-- | The deadline to record on the row: what a direct start stamps now, and
-- what a queued start leaves null for the claim to fill in. An explicit
-- budget on a queued workflow records the budget and no deadline — the wait
-- in the queue is not part of it. An inherited deadline is the parent's
-- instant, shared with no signal passing between them.
resolveTimeoutDeadline :: Timeout -> Maybe Enqueue -> Maybe Timestamp -> Timestamp -> Maybe Timestamp
resolveTimeoutDeadline timeout queue parentDeadline now =
  case timeout of
    Explicit budget -> case queue of
      Just _ -> Nothing
      Nothing -> addTimeout now budget
    None -> Nothing
    Inherit -> parentDeadline

-- | What a caller may say about a run, beyond the input. A workflow cannot
-- be both queued and waited for here, so there is no queue to name — see
-- 'StartOptions'.
data RunOptions = RunOptions
  { runWorkflowId :: Maybe Text,
    runTimeout :: Timeout,
    runAttributes :: Maybe (Map Text Value)
  }
  deriving stock (Eq, Show)

-- | What a caller may say about a start, beyond the input. 'RunOptions'
-- plus a queue: a queued start is recorded, not run, so the handle polls.
-- Option fields carry @run@/@start@ prefixes: the facade already exports
-- 'EnqueueOptions' bare spellings, and one module cannot hold the names
-- twice.
data StartOptions = StartOptions
  { startWorkflowId :: Maybe Text,
    startTimeout :: Timeout,
    startQueue :: Maybe Enqueue,
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
childWorkflowId :: Maybe Text -> Maybe (Text, Int) -> Text -> Text
childWorkflowId chosen parent generated =
  case (chosen, parent) of
    (Just offered, _) -> offered
    (Nothing, Just (parentId, stepId)) -> parentId <> "-" <> pack (show stepId)
    (Nothing, Nothing) -> generated

-- | Settles a deduplication collision the way the caller asked: a held key
-- under 'ReturnExisting' hands back a handle to whoever holds it, and
-- anything else reports the collision. Shared by client and reference
-- enqueues so the two surfaces never diverge.
resolveEnqueueCollision :: Monad m => Connection m -> Enqueue -> Text -> Error -> m (Either TransactError.Error (WorkflowHandle m))
resolveEnqueueCollision conn shape _offeredId err =
  case (shape.duplication_policy, shape.deduplication_id) of
    (ReturnExisting, Just key) -> case err of
      QueueDeduplicated {} -> do
        holder <- runSystemDB conn.connSysdb (\db -> SystemDB.getDeduplicationKeyHolder db shape.name key)
        pure $ case holder of
          Right (Just (WorkflowId holderId)) -> Right (pollingHandle conn holderId True)
          Right Nothing -> Left (TransactError.ErrorSystemDatabase err)
          Left lookupErr -> Left (TransactError.ErrorSystemDatabase lookupErr)
      _ -> pure (Left (TransactError.ErrorSystemDatabase err))
    _ -> pure (Left (TransactError.ErrorSystemDatabase err))

-- | Starts the referenced workflow durably and returns a handle to it,
-- without waiting. Called outside a workflow this starts a root; the
-- 'startChildWorkflow' form is what a body reaches for. If the id is
-- already owned the handle joins the existing run: the id is an
-- idempotency key. Nothing below spawns the row it describes — spawning is
-- the supervisor's or the caller's, which the task-ownership work closes.
startWorkflowRef :: (MonadSTM m, MonadDelay m, MonadTime m) => Connection m -> Identity -> WorkflowRef m -> StartOptions -> Maybe SerializedWorkflowValue -> m (Either TransactError.Error (WorkflowHandle m))
startWorkflowRef conn identity ref options input =
  case traverse validateEnqueue options.startQueue of
    Left err -> pure (Left err)
    Right _ -> do
      now <- timestampNow
      generated <- generatedWorkflowId conn
      let key = refKey ref
          workflowText = fromMaybe generated options.startWorkflowId
          deadline' = resolveTimeoutDeadline options.startTimeout options.startQueue Nothing now
          base = workflowNewWorkflow conn identity key (WorkflowId workflowText) input ((.name) <$> options.startQueue)
          new =
            base
              { newWorkflowDeduplicationId = options.startQueue >>= (.deduplication_id),
                newWorkflowPriority = maybe 0 storedPriority options.startQueue,
                newWorkflowQueuePartitionKey = options.startQueue >>= (.partition_key),
                newWorkflowDelay = options.startQueue >>= (.delay),
                newWorkflowTimeout = timeoutBudget options.startTimeout,
                newWorkflowDeadline = deadline',
                newWorkflowAttributes = encodeAttributes options.startAttributes
              }
      initialized <- runSystemDB conn.connSysdb (\db -> SystemDB.initWorkflow db new (Just maxRecoveryAttempts) Fresh Nothing)
      case initialized of
        Right _ -> pure (Right (pollingHandle conn workflowText True))
        Left err -> case options.startQueue of
          Just shape -> resolveEnqueueCollision conn shape workflowText err
          Nothing -> pure (Left (TransactError.ErrorSystemDatabase err))

-- | Runs the referenced workflow durably and waits for its result: one
-- init records the row with everything the options name, then the shared
-- execute-or-adopt core runs the body when this call owns the row or
-- awaits whoever does. A run cannot be a start plus a second init — the
-- second init would always read back the start's foreign owner and poll a
-- row nothing runs — so the record and the decision are one upsert.
runWorkflowRef :: (MonadSTM m, MonadDelay m, MonadTimer m, MonadTime m) => Connection m -> Identity -> Snapshot m -> WorkflowRef m -> RunOptions -> Maybe SerializedWorkflowValue -> m (Either TransactError.Error (Maybe SerializedWorkflowValue))
runWorkflowRef conn identity snapshot ref options input = do
  now <- timestampNow
  generated <- generatedWorkflowId conn
  let key = refKey ref
      workflowText = fromMaybe generated options.runWorkflowId
      new =
        (workflowNewWorkflow conn identity key (WorkflowId workflowText) input Nothing)
          { newWorkflowTimeout = timeoutBudget options.runTimeout,
            newWorkflowDeadline = resolveTimeoutDeadline options.runTimeout Nothing Nothing now,
            newWorkflowAttributes = encodeAttributes options.runAttributes
          }
  runRegisteredWorkflowWithRow Fresh conn identity snapshot key (WorkflowId workflowText) input new

-- | Starts the referenced workflow as a child of the running one and
-- returns a handle to it. The ambient context decides the parentage, so
-- there is no separate @start_child@: the child's id derives from the
-- parent's step counter unless the options assign one, the start is a
-- checkpoint of the parent (a replayed parent adopts the recorded child
-- without starting anything), and the child inherits the parent's deadline
-- unless it names a budget of its own. Awaiting the handle is a second
-- checkpoint the caller takes through 'handleResult'; recording that wait
-- against the parent is Select follow-up. Starting from inside a step is
-- 'InsideStep': a step is a leaf, and an id-allocating call inside one
-- would shift every later step onto the wrong replay slot.
startChildWorkflow :: (MonadSTM m, MonadDelay m, MonadTime m) => Ctx m -> WorkflowRef m -> StartOptions -> Maybe SerializedWorkflowValue -> m (Either TransactError.Error (WorkflowHandle m))
startChildWorkflow ctx ref options input =
  if inStep ctx
    then pure (Left (TransactError.InsideStep "starting a workflow"))
    else case traverse validateEnqueue options.startQueue of
      Left err -> pure (Left err)
      Right _ -> do
        parentStepId <- nextStepId ctx
        startChild parentStepId
  where
    key = refKey ref
    name = refName ref
    conn = currentConnection ctx
    identity = currentIdentity ctx
    startChild parentStepId = do
      now <- timestampNow
      generated <- generatedWorkflowId conn
      let parentText = workflowId ctx
          childText = childWorkflowId options.startWorkflowId (Just (parentText, parentStepId)) generated
      recorded <- runSystemDB conn.connSysdb (\db -> SystemDB.checkStep db (WorkflowId parentText) parentStepId name)
      case recorded of
        Left err -> pure (Left (TransactError.ErrorSystemDatabase err))
        Right (Just stored) -> case stored.stepRecordChildWorkflowId of
          -- The launch is recorded against the parent: a replay finds the
          -- child it already started. The child's own row belongs to the
          -- earlier run, so an absent row stays "not yet".
          Just (WorkflowId recordedChild) ->
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
          let childDeadline = resolveTimeoutDeadline options.startTimeout options.startQueue (deadline ctx) now
              base = workflowNewWorkflow conn identity key (WorkflowId childText) input ((.name) <$> options.startQueue)
              new =
                base
                  { newWorkflowDeduplicationId = options.startQueue >>= (.deduplication_id),
                    newWorkflowPriority = maybe 0 storedPriority options.startQueue,
                    newWorkflowQueuePartitionKey = options.startQueue >>= (.partition_key),
                    newWorkflowDelay = options.startQueue >>= (.delay),
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
            Right _ -> pure (Right (pollingHandle conn childText True))
            Left err -> case options.startQueue of
              Just shape -> do
                joined <- resolveEnqueueCollision conn shape childText err
                case joined of
                  Right holder -> do
                    -- The mapping only, never the holder's own parent link:
                    -- it travels on 'InitWorkflowCaller' and cannot be
                    -- reached from here.
                    mapped <- runSystemDB conn.connSysdb (\db -> SystemDB.recordChildWorkflow db (WorkflowId parentText) (WorkflowId holder.workflow_id) parentStepId name (Just now))
                    pure $ case mapped of
                      Left recordErr -> Left (TransactError.ErrorSystemDatabase recordErr)
                      Right _ -> Right holder
                  Left joinErr -> pure (Left joinErr)
              Nothing -> pure (Left (TransactError.ErrorSystemDatabase err))

-- | Abort handles to reach running tasks with, and a count of how many are
-- still alive to wait on. Mirrors Rust @Tasks@: nothing here holds a
-- joinable handle, because a caller that dropped its future is no longer
-- waiting though the workflow is still running — which is exactly the case
-- shutdown has to reach. The count stands in for joining, and is what lets
-- shutdown mean "quiet" rather than "told to stop".
data Tasks m = Tasks
  { tasksState :: StrictTVar m (TaskState m)
  }

data TaskState m = TaskState
  { running :: [ThreadId m],
    -- | How many spawned tasks exist and have not departed.
    live :: Int,
    -- | Set by 'abortAll'. A task registered after the sweep is cancelled
    -- on arrival rather than added to a list nothing will read again.
    closed :: Bool
  }

-- | An empty task set.
newTasks :: MonadSTM m => m (Tasks m)
newTasks = do
  state <- newTVarIO (TaskState [] 0 False)
  pure (Tasks state)

-- | Spawns a task counted and reachable by shutdown. The guard exists
-- before the fork, so a shutdown racing this one either kills the task
-- through the registry or waits for it through the count, and never misses
-- it through both.
spawnTracked :: forall m. (MonadFork m, MThrow.MonadMask m, MonadSTM m) => Tasks m -> m () -> m (ThreadId m)
spawnTracked tasks action = MThrow.mask $ \restore -> do
  arrived tasks
  tid <- forkIO $ do
    self <- myThreadId
    outcome <- MThrow.try (restore action) :: m (Either MThrow.SomeException ())
    departed tasks self
    either MThrow.throwIO pure outcome
  closedNow <- atomically $ do
    st <- readTVar tasks.tasksState
    if st.closed
      then pure True
      else do
        writeTVar tasks.tasksState st { running = tid : st.running }
        pure False
  if closedNow then killThread tid else pure ()
  pure tid

-- | Sets the closed flag, kills every registered task, and waits until the
-- count reaches zero. Rows stay @PENDING@: an aborted workflow is recovered
-- by the next launch, which is what makes shutdown safe rather than lossy.
abortAll :: (MonadFork m, MonadSTM m) => Tasks m -> m Int
abortAll tasks = do
  toAbort <- atomically $ do
    st <- readTVar tasks.tasksState
    writeTVar tasks.tasksState st { running = [], closed = True }
    pure st.running
  mapM_ killThread toAbort
  atomically $ do
    st <- readTVar tasks.tasksState
    if st.live == 0 then pure () else retry
  pure (length toAbort)

-- | Records a running task.
arrived :: MonadSTM m => Tasks m -> m ()
arrived tasks = atomically $ do
  st <- readTVar tasks.tasksState
  writeTVar tasks.tasksState st { live = st.live + 1 }

-- | Records a departing task, saturating and deregistering: a double
-- departure must not undercount, because an undercount would let
-- 'abortAll' return early, and a finished task must not be killed again by
-- a later sweep or held in the registry forever.
departed :: (MonadFork m, MonadSTM m) => Tasks m -> ThreadId m -> m ()
departed tasks tid = atomically $ do
  st <- readTVar tasks.tasksState
  writeTVar tasks.tasksState st { live = max 0 (st.live - 1), running = filter (/= tid) st.running }
