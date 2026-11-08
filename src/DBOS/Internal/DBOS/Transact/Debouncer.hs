{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The debouncer: delay a workflow until its triggers go quiet. Mirrors
-- Rust @debouncer.rs@ for the engine surface (@Debouncer@,
-- @DebounceOptions@, the refusal chain, the coalesce loop): the debouncer
-- holds only what a debounce uses — queue, budget, hold cap, app and
-- attributes — and names no workflow id, deduplication id, delay, join
-- policy, priority or partition key. The deduplication id spells the workflow's full name
-- (@name@, @name/class@, @name/class/config@, or @name//config@ for a
-- config with no class), as Rust's @deduplication_id@ does.
--
-- The mechanism is the oracle's sysdb bounce, not a framework workflow:
-- each call tries to extend the DELAYED debounced row holding the
-- deduplication id (@echo/Billing/eu-key@ for a configured instance,
-- @echo-key@ for a free function); when the key is free the call
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
    debounceDedupKey,
    classifyBounce,
  )
where

import DBOS.Prelude
import Data.Aeson (Value)
import Data.Map.Strict (Map)
import Data.Text qualified as Text
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (Debounce (..), DebounceHolder (..), DebounceRequest (..), Duration, QueueName (..), Serialization (..), SerializedWorkflowValue (..), WorkflowId (..), QualifiedWorkflowName, WorkflowIdReusePolicy (..), qualifiedName, qualifiedClassName, qualifiedConfigName, durationIsZero, durationToMs, internalQueueName)
import DBOS.Transact.Config (serializerName)
import DBOS.Transact.Client (Client (..), EnqueueOptions (..), clientEnqueueWorkflowWithDebounce, refuseNamelessWorkflow)
import DBOS.Transact.Connection (Connection (..), generatedWorkflowId, withConnection)
import DBOS.Transact.Context (WorkflowCtx (wctxConn), insideAStep, nextStepId, workflowId)
import DBOS.Transact.Identity (validateAppName)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Handle (WorkflowHandle (..), pollingHandle)
import DBOS.Transact.Instance (DBOS, Executor (..), withExecutor, startWorkflow)
import DBOS.Transact.Registry (registryInstanceId)
import DBOS.Transact.Serialization (WorkflowInput (..), unWorkflowInput)
import DBOS.Transact.Workflow (DuplicationPolicy (Reject), QueueRequest (..), Timeout (..), StartOptions (..), WorkflowRef (..), checkStartDuration, childWorkflowId, queueRequestNew, startChildWorkflow, startOptionsDefault, validateStart)

-- | What a debounce sets: the fields apply to every workflow a debounce
-- enqueues. They are the part of a start a debounce leaves to its caller:
-- the debounce writes the workflow's deduplication id (the key) and its
-- delay (the period) itself, and enqueues many workflows, so it takes no
-- workflow id but the one a single call names. 'debouncerQueueName' is the
-- queue the debounced workflow waits and runs on, and the scope of its key
-- ('Nothing' is the internal queue every app dequeues). 'debouncerTimeout'
-- is the workflow's budget once it runs, never a calling workflow's
-- deadline: a debounce can hold the workflow back for longer than the
-- caller lives. 'debouncerDebounceTimeout' caps the hold, counted from
-- the debounce that enqueued it. 'debouncerAttributes' go on the row of a
-- fresh workflow. Mirrors Rust @Debouncer@ (a per-call serialization
-- choice has no counterpart here: inputs carry their own tag and the
-- connection names the default, so the outcome is the same without the
-- field).
data Debouncer = Debouncer
  { debouncerQueueName :: Maybe Text,
    debouncerTimeout :: Maybe Duration,
    debouncerDebounceTimeout :: Maybe Duration,
    debouncerAppName :: Maybe Text,
    debouncerAttributes :: Maybe (Map Text Value)
  }
  deriving stock (Eq, Show)

-- | Debounce onto the internal queue, with no budget, no hold cap, for
-- the caller's own application and no attributes: the user workflow
-- starts directly once its triggers go quiet.
debouncerNew :: Debouncer
debouncerNew =
  Debouncer
    { debouncerQueueName = Nothing,
      debouncerTimeout = Nothing,
      debouncerDebounceTimeout = Nothing,
      debouncerAppName = Nothing,
      debouncerAttributes = Nothing
    }

-- | Per-call debounce options: only the id a fresh row may take. A call
-- that coalesces ignores it; an id naming an existing workflow is a
-- conflict, never a join. Mirrors Rust @DebounceOptions@.
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
  withExecutor dbos debounceOperation $ \exec ->
    -- The checks come first, so a refused debounce moves nothing: the
    -- budget the enqueue leg would refuse, then the durations, then the
    -- app name. Mirrors the oracle's validation order.
    case traverse_ (checkStartDuration (Just queueName) "timeout") def.debouncerTimeout of
      Left err -> pure (Left err)
      Right () -> case refuseDurations period def.debouncerDebounceTimeout of
        Left err -> pure (Left err)
        Right () -> case refuseDebounceAppName def.debouncerAppName of
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
            Left (TransactError.SystemDatabase sysErr) -> pure (Left (TransactError.refusedArgument debounceOperation sysErr))
            Left err -> pure (Left err)
            Right (Debounced wid) -> pure (Right (pollingHandle exec.conn wid True))
            Right (DebounceHeld holder) -> case classifyBounce holder userRef.refQualifiedName (targetApp exec) of
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
        { debounceRequestWorkflow = userRef.refQualifiedName,
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
    dedupKey = debounceDedupKey userRef.refQualifiedName key
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
          | otherwise -> case traverse_ (checkStartDuration (Just queueName) "timeout") def.debouncerTimeout of
              Left err -> pure (Left err)
              Right () -> case refuseDurations period def.debouncerDebounceTimeout of
                Left err -> pure (Left err)
                Right () -> case refuseDebounceAppName def.debouncerAppName of
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
            Left (TransactError.SystemDatabase sysErr) -> pure (Left (TransactError.refusedArgument debounceOperation sysErr))
            Left err -> pure (Left err)
            Right (Debounced wid) -> pure (Right (pollingHandle conn wid True))
            Right (DebounceHeld holder) -> case classifyBounce holder userRef.refQualifiedName (targetApp conn) of
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
        { debounceRequestWorkflow = userRef.refQualifiedName,
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
    dedupKey = debounceDedupKey userRef.refQualifiedName key
    targetApp conn = def.debouncerAppName <|> conn.connAppName

-- * Helpers

-- | A debouncer over a client connection: the same bounce loop with no
-- workflow steps and no placement — a client runs nothing, so there is
-- nothing to checkpoint. The workflow is named by key, as
-- @Client::enqueue@ names it; the other fields apply to every workflow a
-- debounce enqueues, as the same fields of 'EnqueueOptions' would to an
-- enqueue, and leave out what a debounce sets itself. Mirrors Rust
-- @DebouncerClient@.
data DebouncerClient m = DebouncerClient
  { debouncerClient :: Client m,
    debouncerClientWorkflow :: QualifiedWorkflowName,
    debouncerClientQueue :: Text,
    debouncerClientTimeout :: Maybe Duration,
    debouncerClientAppVersion :: Maybe Text,
    debouncerClientAppName :: Maybe Text,
    debouncerClientAttributes :: Maybe (Map Text Value),
    debouncerClientDebounceTimeout :: Maybe Duration
  }

-- | Debounce through a client on a queue, with no budget, for the
-- client's own application and no version: one call's workflow takes an
-- engine-generated id unless the call pins one.
debouncerClientNew :: Client m -> QualifiedWorkflowName -> Text -> DebouncerClient m
debouncerClientNew client workflow queueName =
  DebouncerClient
    { debouncerClient = client,
      debouncerClientWorkflow = workflow,
      debouncerClientQueue = queueName,
      debouncerClientTimeout = Nothing,
      debouncerClientAppVersion = Nothing,
      debouncerClientAppName = Nothing,
      debouncerClientAttributes = Nothing,
      debouncerClientDebounceTimeout = Nothing
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
  -- A client takes the name from its caller, so an empty one is refused
  -- before anything bounces or writes, as a client enqueue is; then the
  -- budget the enqueue leg would refuse, then the durations, then the app
  -- name. Mirrors Rust @DebouncerClient::debounce_with@'s order.
  case refuseNamelessWorkflow dc.debouncerClientWorkflow dc.debouncerClientQueue of
    Just err -> pure (Left err)
    Nothing ->
      case validateStart (Just (queueRequestNew dc.debouncerClientQueue)) dc.debouncerClientTimeout Nothing WorkflowIdReturnExisting of
        Left err -> pure (Left err)
        Right () -> case refuseDurations period dc.debouncerClientDebounceTimeout of
          Left err -> pure (Left err)
          Right () -> case refuseDebounceAppName dc.debouncerClientAppName of
            Left err -> pure (Left err)
            Right () -> do
              generated <- generatedWorkflowId conn
              let offered = case call.debounceOptionsWorkflowId of
                    Just (WorkflowId pinned) -> pinned
                    Nothing -> generated
              bounceLoop offered
  where
    conn = dc.debouncerClient.conn
    queueName = dc.debouncerClientQueue
    dedupKey = debounceDedupKey dc.debouncerClientWorkflow key
    targetApp = dc.debouncerClientAppName <|> conn.connAppName
    rawInput = unWorkflowInput <$> input
    bounceLoop offered = do
      let request =
            DebounceRequest
              { debounceRequestWorkflow = dc.debouncerClientWorkflow,
                debounceRequestQueueName = queueName,
                debounceRequestDeduplicationId = dedupKey,
                debounceRequestPeriod = period,
                debounceRequestInputs = (.serializedText) <$> rawInput,
                debounceRequestSerialization = Just (inputSerialization conn rawInput),
                debounceRequestAppName = targetApp
              }
      bounced <- withConnection conn (\db -> SystemDB.debounceDelayedWorkflow db request Nothing)
      case bounced of
        Left (TransactError.SystemDatabase sysErr) -> pure (Left (TransactError.refusedArgument debounceOperation sysErr))
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
                      (SystemDBError.QueueDeduplicated {workflowId = holderId, queueName = queueName, deduplicationId = dedupKey})
                  )
              )
        Right DebounceUnheld -> do
          let fresh =
                EnqueueOptions
                  { queueName = queueName,
                    deduplicationId = Just dedupKey,
                    duplicationPolicy = Reject,
                    priority = Nothing,
                    partitionKey = Nothing,
                    delay = Just period,
                    workflowId = Just offered,
                    workflowIdReusePolicy = WorkflowIdReturnExisting,
                    appName = targetApp,
                    appVersion = dc.debouncerClientAppVersion,
                    timeout = dc.debouncerClientTimeout,
                    attributes = dc.debouncerClientAttributes
                  }
          enqueued <- clientEnqueueWorkflowWithDebounce dc.debouncerClient dc.debouncerClientWorkflow fresh (Just dc.debouncerClientDebounceTimeout) input
          case enqueued of
            -- A concurrent debounce grabbed the key between bounce and
            -- enqueue; loop to bounce that workflow instead.
            Left (TransactError.SystemDatabase (SystemDBError.QueueDeduplicated {})) -> bounceLoop offered
            other -> pure other

-- | Refuses an empty or malformed app name as `app_name`, not as the
-- column it would fill: a row stamped with a name no other executor
-- spells the same way is a row nothing claims. Reuses the shared rule's
-- wording on this operation's channel. Mirrors Rust @refuse_app_name@.
refuseDebounceAppName :: Maybe Text -> Either (TransactError.Error TransactError.EngineOnly) ()
refuseDebounceAppName appName = case appName of
  Nothing -> Right ()
  Just name -> case validateAppName name of
    Right () -> Right ()
    Left (TransactError.ErrorConfig detail) -> Left (TransactError.InvalidArgument debounceOperation detail)
    Left err -> Left err

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

classifyBounce :: DebounceHolder -> QualifiedWorkflowName -> Maybe Text -> BounceAction
classifyBounce holder workflow targetApp
  | not holder.debounceHolderIsDebounced || not (sameWorkflow holder workflow) || anotherApp holder targetApp =
      BounceRaise holder.debounceHolderWorkflowId
  | otherwise = BounceRetry
  where
    sameWorkflow holder workflow =
      case holder.debounceHolderWorkflowName of
        Nothing -> False
        Just name ->
          name == qualifiedName workflow
            && presentPart holder.debounceHolderClassName == qualifiedClassName workflow
            && presentPart holder.debounceHolderConfigName == qualifiedConfigName workflow
    -- A stored '' is absent, as it is to a name built from a row.
    presentPart part = case part of
      Just text | Text.null text -> Nothing
      other -> other
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
      -- Never the caller's deadline: the debounce can hold the workflow
      -- back for longer than the caller lives, so a missing budget means
      -- none. Mirrors the oracle's @timeout.map_or(Timeout::None)@.
      startTimeout = maybe None Explicit def.debouncerTimeout,
      startAttributes = def.debouncerAttributes,
      startQueue =
        Just
          ( (queueRequestNew queueName)
              { deduplicationId = Just dedupKey,
                delay = Just period,
                isDebounced = True,
                debounceTimeout = def.debouncerDebounceTimeout,
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
      | otherwise = void (first (TransactError.refusedArgument debounceOperation) (durationToMs field duration))

-- | The deduplication key, spelled here and nowhere else from the
-- workflow's parts: @name@, @name/class@, @name/class/config@, or
-- @name//config@ for a config with no class, then the debounce key
-- (@wf/Billing/eu-key@ for a configured instance, @wf-key@ for a free
-- function). Spelled from the parts rather than the display, which is for
-- people to read. A @/@-or-@-@ collision between unrelated workflows is
-- refused by whole-name comparison at classify, not escaped. Mirrors Rust
-- @deduplication_id@.
debounceDedupKey :: QualifiedWorkflowName -> Text -> Text
debounceDedupKey workflow debKey =
  qualifiedName workflow <> qualifier <> "-" <> debKey
  where
    qualifier = case (qualifiedClassName workflow, qualifiedConfigName workflow) of
      (Nothing, Nothing) -> ""
      (Just className, Nothing) -> "/" <> className
      (Just className, Just configName) -> "/" <> className <> "/" <> configName
      (Nothing, Just configName) -> "//" <> configName

-- | The stored wire for one pass-through input, or the connection's own
-- serialization when the input names none. Mirrors the start path, which
-- resolves the same default.
inputSerialization :: Connection m -> Maybe SerializedWorkflowValue -> Text
inputSerialization conn args = case args >>= (.serializedSerialization) of
  Just (Serialization name) -> name
  Nothing -> serializerName conn.connSerializer
