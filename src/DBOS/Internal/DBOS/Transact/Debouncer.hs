{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The debouncer: delay a workflow until its triggers go quiet. Mirrors
-- Rust @debouncer.rs@ for the engine surface (@Debouncer@,
-- @DebounceOptions@, the refusal chain, the coalesce loop); the queue,
-- timeout, and application it runs under follow the TypeScript @Debouncer@
-- the Python oracle implements, as Rust folds those into its start
-- options. The deduplication-id spelling still follows the TypeScript
-- oracle (class-prefixed); the Rust full-key spelling rides a later unit
-- with its ADR.
--
-- The mechanism is the oracle's sysdb bounce, not a framework workflow:
-- each call tries to extend the DELAYED debounced row holding the
-- deduplication key (@\<workflow\>-\<key\>@, class-prefixed for instance
-- workflows as the oracle prefixes its own); when the key is free the call
-- enqueues a fresh debounced row instead. A lost enqueue race loops back to
-- the bounce, as the oracle's does. Every caller gets a handle to the same
-- user workflow id.
module DBOS.Transact.Debouncer
  ( Debouncer (..),
    debouncerNew,
    DebounceOptions (..),
    debounceOptionsDefault,
    debounce,
    debounceWith,
    debounceInWorkflow,
    debounceOperation,
    DebouncerClient (..),
    debouncerClientNew,
    debounceClient,
    debounceClientWith,
  )
where

import DBOS.Prelude
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (Debounce (..), DebounceHolder (..), DebounceRequest (..), Duration, QueueName (..), Serialization (..), SerializedWorkflowValue (..), WorkflowId (..), WorkflowKey (..), durationIsZero, durationToMs, internalQueueName, renderWorkflowKey, workflowKeyFromRow)
import DBOS.Transact.Config (serializerName)
import DBOS.Transact.Client (Client (..), EnqueueOptions (..), clientEnqueueWorkflowWith)
import DBOS.Transact.Connection (Connection (..), generatedWorkflowId, withConnection)
import DBOS.Transact.Context (WorkflowCtx (wctxConn), insideAStep, nextStepId, workflowId)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Handle (WorkflowHandle (..), pollingHandle)
import DBOS.Transact.Instance (DBOS, Executor (..), withExecutor, startWorkflow)
import DBOS.Transact.Registry (registryInstanceId)
import DBOS.Transact.Serialization (WorkflowInput (..), unWorkflowInput)
import DBOS.Transact.Workflow (DuplicationPolicy (..), Enqueue (..), Timeout (..), StartOptions (..), WorkflowRef (..), childWorkflowId, enqueueNew, startChildWorkflow, startOptionsDefault, validateEnqueue)

-- | What to debounce: which queue the user workflow finally runs on, how
-- long to wait past the first trigger at most, and which application the
-- debounce acts for. Mirrors the TypeScript @Debouncer@ options: @queue@,
-- @debounceTimeoutMs@, and @applicationName@ — all optional. The debounced
-- workflow itself comes from the reference each call passes, as the
-- oracle's constructor takes its workflow.
data Debouncer = Debouncer
  { debouncerQueueName :: Maybe Text,
    debouncerTimeout :: Maybe Duration,
    debouncerAppName :: Maybe Text
  }
  deriving stock (Eq, Show)

-- | Debounce onto the internal queue, with no timeout, for the caller's
-- own application: the user workflow starts directly once its triggers go
-- quiet.
debouncerNew :: Debouncer
debouncerNew =
  Debouncer
    { debouncerQueueName = Nothing,
      debouncerTimeout = Nothing,
      debouncerAppName = Nothing
    }

-- | Per-call debounce options: only the id a fresh row may take. A call
-- that coalesces ignores it; an id naming an existing workflow is a
-- conflict, never a join. There is no stored counterpart on 'Debouncer'
-- to refuse (the oracle's @refuse_stored_workflow_id@ has no trigger
-- here), and the queue options a debounce owns are built internally, so
-- the owned-options refusal has no trigger either. Mirrors Rust
-- @DebounceOptions@.
data DebounceOptions = DebounceOptions
  { debounceOptionsWorkflowId :: Maybe WorkflowId
  }
  deriving stock (Eq, Show)

-- | No pinned id: the fresh row takes an engine-generated one.
debounceOptionsDefault :: DebounceOptions
debounceOptionsDefault = DebounceOptions {debounceOptionsWorkflowId = Nothing}

-- | The operation name every debounce refusal reports under. Mirrors Rust
-- @DEBOUNCE_OPERATION@.
debounceOperation :: Text
debounceOperation = "debounce a workflow"

-- | Debounce the referenced workflow: delay it until 'period' passes with
-- no new call for 'key', then run it with the last call's inputs. The first
-- call for a quiet key enqueues a fresh debounced row; concurrent calls
-- join it through the bounce, extending its delay and replacing its
-- inputs. Answers a handle to the user workflow.
debounce :: (MonadMVar m, MonadFork m, MonadMask m, MonadDelay m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
         => DBOS m ->
            WorkflowRef m TransactError.EngineOnly ->
            Debouncer ->
            Text ->
            Duration ->
            Maybe WorkflowInput ->
            m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
debounce dbos userRef def key period input =
  debounceWith dbos userRef def key period input debounceOptionsDefault

-- | Debounce with a per-call id: when the key is free and the id names no
-- workflow, the fresh row takes it; when it names one, the creation is a
-- conflict rather than a silent join, as the sysdb arm refuses. Mirrors
-- Rust @Debouncer::debounce_with@.
debounceWith :: (MonadMVar m, MonadFork m, MonadMask m, MonadDelay m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
             => DBOS m ->
                WorkflowRef m TransactError.EngineOnly ->
                Debouncer ->
                Text ->
                Duration ->
                Maybe WorkflowInput ->
                DebounceOptions ->
                m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
debounceWith dbos userRef def key period input options = do
  withExecutor dbos debounceOperation $ \exec -> case refuseDurations period def.debouncerTimeout of
    Left err -> pure (Left err)
    Right () -> do
      generated <- generatedWorkflowId exec.conn
      let offered = case options.debounceOptionsWorkflowId of
            Just (WorkflowId pinned) -> pinned
            Nothing -> generated
      bounceLoop exec offered Nothing
  where
    rawInput = unWorkflowInput <$> input
    bounceLoop exec pinned caller = do
      -- The period travels through: the write resolves it against the
      -- database clock the release sweep compares it with, as the
      -- oracle's @Bounce::request@ cannot fail.
      bounced <- withConnection exec.conn (\db -> SystemDB.debounceDelayedWorkflow db (bounceRequest exec period) caller)
      case bounced of
            Left err -> pure (Left err)
            Right (Debounced wid) -> pure (Right (pollingHandle exec.conn wid True))
            Right (DebounceHeld holder) -> case classifyBounce holder userRef.refKey (targetApp exec) of
              BounceRetry -> do
                threadDelay retryPauseMicros
                bounceLoop exec pinned caller
              BounceRaise holderId ->
                pure
                  ( Left
                      ( TransactError.fromSystemDB
                          (SystemDBError.QueueDeduplicated {workflowId = holderId, queueName = queueName, deduplicationId = dedupKey})
                      )
                  )
            Right DebounceUnheld -> do
              enqueued <- startWorkflow exec userRef (debouncedStartOptions def queueName dedupKey period (Just (WorkflowId pinned))) input
              case enqueued of
                -- A concurrent debounce grabbed the key between bounce and
                -- enqueue; loop to bounce that workflow instead.
                Left (TransactError.SystemDatabase (SystemDBError.QueueDeduplicated {})) -> bounceLoop exec pinned caller
                -- The fresh row is queued, so its handle is already a
                -- polling one; respelled to the caller's error type.
                other -> pure (adoptHandle exec <$> other)
    bounceRequest exec bouncePeriod =
      DebounceRequest
        { debounceRequestWorkflow = userRef.refKey,
          debounceRequestQueueName = queueName,
          debounceRequestDeduplicationId = dedupKey,
          debounceRequestPeriod = bouncePeriod,
          debounceRequestInputs = (.serializedText) <$> rawInput,
          debounceRequestSerialization = Just (inputSerialization exec.conn rawInput),
          debounceRequestAppName = targetApp exec
        }
    queueName = case def.debouncerQueueName of
      Just name -> name
      Nothing -> case internalQueueName of QueueName name -> name
    dedupKey = debounceDedupKey userRef.refKey key
    targetApp exec = def.debouncerAppName <|> exec.conn.connAppName

-- | Debounce from inside a running workflow: the bounce commits atomically
-- with its step checkpoint, as the oracle's @call_txn_as_step@ does, so a
-- crash can never commit one without the other. The fresh-enqueue leg
-- starts a child of the running workflow, which replays through its own
-- checkpoint; its id derives from the parent and the bounce's step, so a
-- replay re-enqueues the same id rather than a second row.
debounceInWorkflow :: (MonadMVar m, MonadDelay m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m, MonadCatch m)
                   => WorkflowCtx exec m ->
                      WorkflowRef m TransactError.EngineOnly ->
                      Debouncer ->
                      Text ->
                      Duration ->
                      Maybe WorkflowInput ->
                      m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
debounceInWorkflow wctx userRef def key period input = do
  stepped <- insideAStep wctx
  if stepped
    then pure (Left (TransactError.InsideStep "debouncing a workflow"))
    else do
      refInstance <- registryInstanceId userRef.refRegistry
      let conn = wctx.wctxConn
      case refInstance of
        Nothing -> pure (Left (TransactError.NotLaunched {operation = "debounce a workflow"}))
        Just instanceId
          | instanceId /= conn.connInstanceId ->
              pure (Left (TransactError.WrongInstance {operation = "debounce a workflow"}))
          | otherwise -> case refuseDurations period def.debouncerTimeout of
              Left err -> pure (Left err)
              Right () -> bounceLoop conn
  where
    rawInput = unWorkflowInput <$> input
    -- Each attempt takes its own step id and pinned id, as the oracle
    -- re-places both per attempt: a retry must bounce again, never
    -- replay the recorded outcome.
    bounceLoop conn = do
      stepId <- nextStepId wctx
      generated <- generatedWorkflowId conn
      let parentText = workflowId wctx
          pinned = childWorkflowId Nothing (Just (parentText, stepId)) generated
          caller = Just (WorkflowId parentText, stepId)
      bounced <- withConnection conn (\db -> SystemDB.debounceDelayedWorkflow db (bounceRequest conn period) caller)
      case bounced of
            Left err -> pure (Left err)
            Right (Debounced wid) -> pure (Right (pollingHandle conn wid True))
            Right (DebounceHeld holder) -> case classifyBounce holder userRef.refKey (targetApp conn) of
              BounceRetry -> do
                threadDelay retryPauseMicros
                bounceLoop conn
              BounceRaise holderId ->
                pure
                  ( Left
                      ( TransactError.fromSystemDB
                          (SystemDBError.QueueDeduplicated {workflowId = holderId, queueName = queueName, deduplicationId = dedupKey})
                      )
                  )
            Right DebounceUnheld -> do
              enqueued <- startChildWorkflow wctx userRef (debouncedStartOptions def queueName dedupKey period (Just (WorkflowId pinned))) input
              case enqueued of
                Left (TransactError.SystemDatabase (SystemDBError.QueueDeduplicated {})) -> bounceLoop conn
                other -> pure (adoptHandleConn conn <$> other)
    bounceRequest conn bouncePeriod =
      DebounceRequest
        { debounceRequestWorkflow = userRef.refKey,
          debounceRequestQueueName = queueName,
          debounceRequestDeduplicationId = dedupKey,
          debounceRequestPeriod = bouncePeriod,
          debounceRequestInputs = (.serializedText) <$> rawInput,
          debounceRequestSerialization = Just (inputSerialization conn rawInput),
          debounceRequestAppName = targetApp conn
        }
    queueName = case def.debouncerQueueName of
      Just name -> name
      Nothing -> case internalQueueName of QueueName name -> name
    dedupKey = debounceDedupKey userRef.refKey key
    targetApp conn = def.debouncerAppName <|> conn.connAppName

-- * Helpers

-- | A debouncer over a client connection: the same bounce loop with no
-- workflow steps and no placement — a client runs nothing, so there is
-- nothing to checkpoint. The workflow is named by key, as
-- @Client::enqueue@ names it; what every fresh row carries rides the
-- options, and the longest hold rides the debouncer. Mirrors Rust
-- @DebouncerClient@.
data DebouncerClient m = DebouncerClient
  { debouncerClient :: Client m,
    debouncerClientWorkflow :: WorkflowKey,
    debouncerClientOptions :: EnqueueOptions,
    debouncerClientTimeout :: Maybe Duration
  }

-- | Debounce through a client with the given options and no timeout.
debouncerClientNew :: Client m -> WorkflowKey -> EnqueueOptions -> DebouncerClient m
debouncerClientNew client workflow options =
  DebouncerClient
    { debouncerClient = client,
      debouncerClientWorkflow = workflow,
      debouncerClientOptions = options,
      debouncerClientTimeout = Nothing
    }

-- | Debounce a key through a client: quiet calls collapse onto one
-- delayed row running with the last call's inputs, as 'debounce' does
-- through an executor.
debounceClient :: (MonadSTM m, MonadDelay m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
               => DebouncerClient m ->
                  Text ->
                  Duration ->
                  Maybe WorkflowInput ->
                  m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
debounceClient dc key period input = debounceClientWith dc key period input debounceOptionsDefault

-- | 'debounceClient' with a per-call id, as 'debounceWith' is to
-- 'debounce'. Mirrors Rust @DebouncerClient::debounce_with@.
debounceClientWith :: (MonadSTM m, MonadDelay m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                   => DebouncerClient m ->
                      Text ->
                      Duration ->
                      Maybe WorkflowInput ->
                      DebounceOptions ->
                      m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
debounceClientWith dc key period input call = do
  case refuseClientOptions dc of
    Left err -> pure (Left err)
    Right () -> case refuseDurations period dc.debouncerClientTimeout of
      Left err -> pure (Left err)
      -- The enqueue leg would refuse these after the bounce already
      -- extended the holder and replaced its inputs; validate first, as
      -- the oracle validates its options before looping, reporting the
      -- same errors the leg would.
      Right () -> case validateEnqueue options.queue of
        Left err -> pure (Left err)
        Right () -> case traverse (durationToMs "timeout") options.timeout of
          Left sysErr -> pure (Left (TransactError.fromSystemDB sysErr))
          Right _ -> do
            generated <- generatedWorkflowId conn
            let offered = case call.debounceOptionsWorkflowId of
                  Just (WorkflowId pinned) -> pinned
                  Nothing -> generated
            bounceLoop offered
  where
    conn = dc.debouncerClient.conn
    options = dc.debouncerClientOptions
    dedupKey = debounceDedupKey dc.debouncerClientWorkflow key
    targetApp = options.appName <|> conn.connAppName
    rawInput = unWorkflowInput <$> input
    bounceLoop offered = do
      let request =
            DebounceRequest
              { debounceRequestWorkflow = dc.debouncerClientWorkflow,
                debounceRequestQueueName = options.queue.name,
                debounceRequestDeduplicationId = dedupKey,
                debounceRequestPeriod = period,
                debounceRequestInputs = (.serializedText) <$> rawInput,
                debounceRequestSerialization = Just (inputSerialization conn rawInput),
                debounceRequestAppName = targetApp
              }
      bounced <- withConnection conn (\db -> SystemDB.debounceDelayedWorkflow db request Nothing)
      case bounced of
        Left err -> pure (Left err)
        Right (Debounced wid) -> pure (Right (pollingHandle conn wid True))
        Right (DebounceHeld holder) -> case classifyBounce holder dc.debouncerClientWorkflow targetApp of
          BounceRetry -> do
            threadDelay retryPauseMicros
            bounceLoop offered
          BounceRaise holderId ->
            pure
              ( Left
                  ( TransactError.fromSystemDB
                      (SystemDBError.QueueDeduplicated {workflowId = holderId, queueName = options.queue.name, deduplicationId = dedupKey})
                  )
              )
        Right DebounceUnheld -> do
          let shape = options.queue {deduplicationId = Just dedupKey, delay = Just period, isDebounced = True, debounceTimeout = dc.debouncerClientTimeout}
              fresh = options {queue = shape, workflowId = Just offered, appName = targetApp}
          enqueued <- clientEnqueueWorkflowWith dc.debouncerClient dc.debouncerClientWorkflow fresh input
          case enqueued of
            -- A concurrent debounce grabbed the key between bounce and
            -- enqueue; loop to bounce that workflow instead.
            Left (TransactError.SystemDatabase (SystemDBError.QueueDeduplicated {})) -> bounceLoop offered
            other -> pure other

-- | Refuses a stored workflow id and the queue options a debounce owns,
-- as 'InvalidArgument' to this operation. A debouncer serves many keys
-- and many release cycles, so an id stored on the options would name
-- every fresh workflow it ever enqueues; the deduplication id, delay,
-- join policy, priority, and partition key are the debounce's own to
-- write. Mirrors Rust @refuse_stored_workflow_id@ plus
-- @refuse_owned_options@.
refuseClientOptions :: DebouncerClient m -> Either (TransactError.Error TransactError.EngineOnly) ()
refuseClientOptions dc = case dc.debouncerClientOptions.workflowId of
  Just _ ->
    Left
      ( TransactError.InvalidArgument
          debounceOperation
          "`options.workflow_id` cannot be set on a debouncer, which enqueues many workflows: pass the id for one call in `DebounceOptions.workflow_id`, to `debounceClientWith`"
      )
  Nothing -> refuseOwnedOptions dc.debouncerClientOptions.queue
  where
    refuseOwnedOptions queue
      | Just _ <- queue.deduplicationId = refuse "a `deduplication_id` cannot be set: the debounce key is the workflow's deduplication id"
      | Just _ <- queue.delay = refuse "a `delay` cannot be set: the debounce period is the workflow's delay"
      | queue.duplicationPolicy == ReturnExisting = refuse "`DuplicationPolicy::ReturnExisting` cannot be set: a debounce owns the workflow's deduplication"
      | Just _ <- queue.priority = refuse "a `priority` cannot be set: debounced workflows do not support priority"
      | Just _ <- queue.partitionKey = refuse "a `partition_key` cannot be set: debounced workflows do not support partition keys"
      | otherwise = Right ()
    refuse detail =
      Left (TransactError.InvalidArgument debounceOperation ("queue `" <> queueName <> "`: " <> detail))
      where
        queueName = dc.debouncerClientOptions.queue.name

-- | What a bounce that found no debounced row to extend means. Mirrors
-- the oracle's @classify@: a holder that is not debounced, one of a
-- different name, class, or configured instance (a key collision between
-- unrelated workflows never hijacks the wrong inputs), or one another
-- application owns is refused, naming the holder; a same-key debounced
-- holder that flipped out of DELAYED mid-bounce — or one with no owner to
-- tell — retries after a pause. An empty class or instance reads as
-- absent, as Rust normalizes rows other SDKs wrote, so a cross-SDK free
-- function still coalesces.
data BounceAction = BounceRetry | BounceRaise Text

-- | The pause before a retry bounce, so a holder between states is
-- re-read, not hammered. Mirrors Rust @RETRY_PAUSE@.
retryPauseMicros :: Int
retryPauseMicros = 100000

classifyBounce :: DebounceHolder -> WorkflowKey -> Maybe Text -> BounceAction
classifyBounce holder workflow targetApp
  | not holder.debounceHolderIsDebounced || not (sameWorkflow holder workflow) || anotherApp holder targetApp =
      BounceRaise holder.debounceHolderWorkflowId
  | otherwise = BounceRetry
  where
    sameWorkflow holder workflow = case holder.debounceHolderWorkflowName of
      Nothing -> False
      Just name ->
        workflowKeyFromRow name holder.debounceHolderClassName holder.debounceHolderConfigName == workflow
    anotherApp holder target = case (target, holder.debounceHolderAppName) of
      (Just waiting, Just owner) -> waiting /= owner
      _ -> False

-- | The fresh debounced enqueue: the period's delay, the debounced mark
-- with the timeout's deadline, and no inherited deadline — a debounce
-- delay can be long, so an inherited absolute deadline could expire before
-- the debounced workflow ever runs. Mirrors the oracle's enqueue leg. The
-- client leg builds the same columns on 'EnqueueOptions' rather than
-- sharing this: the two option types force the parallel construction.
debouncedStartOptions :: Debouncer -> Text -> Text -> Duration -> Maybe WorkflowId -> StartOptions
debouncedStartOptions def queueName dedupKey period offered =
  startOptionsDefault
    { startWorkflowId = offered,
      startTimeout = None,
      startQueue =
        Just
          ( (enqueueNew queueName)
              { deduplicationId = Just dedupKey,
                delay = Just period,
                isDebounced = True,
                debounceTimeout = def.debouncerTimeout,
                applicationName = def.debouncerAppName
              }
          )
    }

-- | A handle the enqueue leg returned, respelled to the caller's error
-- type. The fresh row is queued, so the handle is already a polling one;
-- only the phantom error type changes.
adoptHandle :: Executor m -> WorkflowHandle m TransactError.EngineOnly -> WorkflowHandle m e
adoptHandle exec handle = pollingHandle exec.conn handle.workflowId True

-- | 'adoptHandle' where only the connection is at hand.
adoptHandleConn :: Connection m -> WorkflowHandle m TransactError.EngineOnly -> WorkflowHandle m e
adoptHandleConn conn handle = pollingHandle conn handle.workflowId True

-- | Refuses a period or a debounce timeout of zero, or one too long to
-- store: a zero period would enqueue the workflow ready to run rather
-- than @DELAYED@, where no later bounce can find it. Reports through the
-- database's own check, so the two cannot disagree about what is
-- storable. Mirrors Rust @refuse_durations@.
refuseDurations :: Duration -> Maybe Duration -> Either (TransactError.Error TransactError.EngineOnly) ()
refuseDurations period cap = do
  refuseDuration "period" period
  case cap of
    Nothing -> Right ()
    Just limit -> refuseDuration "debounce_timeout" limit
  where
    refuseDuration field duration
      | durationIsZero duration =
          Left (TransactError.refusedArgument debounceOperation (SystemDBError.invalidInput field "must be a positive, non-zero duration"))
      | otherwise = case durationToMs field duration of
          Left sysErr -> Left (TransactError.refusedArgument debounceOperation sysErr)
          Right _ -> Right ()

-- | The deduplication key, spelled in one function: the workflow's full
-- key display, then the debounce key (@wf/Billing/eu-key@ for a
-- configured instance, @wf-key@ for a free function). A @/@-or-@-@
-- collision between unrelated workflows is refused by whole-key
-- comparison at classify, not escaped. Mirrors Rust @deduplication_id@.
debounceDedupKey :: WorkflowKey -> Text -> Text
debounceDedupKey workflow debKey = renderWorkflowKey workflow <> "-" <> debKey

-- | The stored wire for one pass-through input, or the connection's own
-- serialization when the input names none. Mirrors the start path, which
-- resolves the same default.
inputSerialization :: Connection m -> Maybe SerializedWorkflowValue -> Text
inputSerialization conn args = case args >>= (.serializedSerialization) of
  Just (Serialization name) -> name
  Nothing -> serializerName conn.connSerializer
