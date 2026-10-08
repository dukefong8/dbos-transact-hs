{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The debouncer: delay a workflow until its triggers go quiet. There is no
-- Rust counterpart — Rust owns only the sysdb-level bounce primitives —
-- so the shape follows the TypeScript @Debouncer@ that the Python oracle
-- implements: @Debouncer(workflow, timeout, queue)@ plus
-- @debounce(key, period, inputs)@.
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
    debounce,
    debounceInWorkflow,
  )
where

import DBOS.Prelude
import Data.Text qualified as Text
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (Debounce (..), DebounceHolder (..), DebounceRequest (..), Duration, QueueName (..), Serialization (..), SerializedWorkflowValue (..), WorkflowId (..), addTimeout, durationAsMillis, internalQueueName, timestampNow)
import DBOS.Transact.Config (serializerName)
import DBOS.Transact.Connection (Connection (..), generatedWorkflowId, runSystemDB)
import DBOS.Transact.Context (WorkflowCtx (wctxConn), insideAStep, nextStepId, workflowId)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Handle (WorkflowHandle (..), pollingHandle)
import DBOS.Transact.Instance (DBOS, Executor (..), requireExecutor, startWorkflow)
import DBOS.Transact.Registry (WorkflowKey (..), WorkflowRef (..), refName, registryInstanceId)
import DBOS.Transact.Workflow (Enqueue (..), Timeout (..), StartOptions (..), childWorkflowId, enqueueNew, startChildWorkflow, startOptionsDefault)

-- | What to debounce: which queue the user workflow finally runs on, how
-- long to wait past the first trigger at most, and which application the
-- debounce acts for. Mirrors the TypeScript @Debouncer@ options: @queue@,
-- @debounceTimeoutMs@, and @applicationName@ — all optional. The debounced
-- workflow itself comes from the reference each call passes, as the
-- oracle's constructor takes its workflow.
data Debouncer = Debouncer
  { debouncerQueueName :: Maybe Text,
    debouncerTimeout :: Maybe Duration,
    debouncerApplicationName :: Maybe Text
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
      debouncerApplicationName = Nothing
    }

-- | Debounce the referenced workflow: delay it until 'period' passes with
-- no new call for 'key', then run it with the last call's inputs. The first
-- call for a quiet key enqueues a fresh debounced row; concurrent calls
-- join it through the bounce, extending its delay and replacing its
-- inputs. Answers a handle to the user workflow.
debounce :: (MonadMVar m, MonadFork m, MonadMask m, MonadTimer m, MonadTime m)
         => DBOS m ->
            WorkflowRef m TransactError.EngineOnly ->
            Debouncer ->
            Text ->
            Duration ->
            Maybe SerializedWorkflowValue ->
            m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowHandle m e))
debounce dbos userRef def key period input = do
  required <- requireExecutor dbos "debounce a workflow"
  case required of
    Left err -> pure (Left err)
    Right exec -> case checkPeriod period of
      Left err -> pure (Left err)
      Right () -> do
        pinned <- generatedWorkflowId exec.conn
        bounceLoop exec pinned Nothing
  where
    bounceLoop exec pinned caller = do
      now <- timestampNow
      case addTimeout now period of
        Nothing ->
          pure (Left (TransactError.InvalidArgument "debounce" "the debounce period does not resolve to a representable wake time"))
        Just delayUntil -> do
          bounced <- runSystemDB exec.conn.connSysdb (\db -> SystemDB.debounceDelayedWorkflow db (bounceRequest exec delayUntil) caller)
          case bounced of
            Left err -> pure (Left (TransactError.SystemDatabase err))
            Right (Debounced wid) -> pure (Right (pollingHandle exec.conn wid True))
            Right (DebounceHeld holder) -> case classifyBounce holder (refName userRef) (refClassName userRef) (targetApp exec) of
              BounceRetry -> bounceLoop exec pinned caller
              BounceRaise ->
                pure
                  ( Left
                      ( TransactError.SystemDatabase
                          (SystemDBError.QueueDeduplicated {workflowId = pinned, queueName = queueName, deduplicationId = dedupKey})
                      )
                  )
            Right DebounceUnheld -> do
              enqueued <- startWorkflow exec userRef (debounceOptions def queueName dedupKey period (Just (WorkflowId pinned))) input
              case enqueued of
                -- A concurrent debounce grabbed the key between bounce and
                -- enqueue; loop to bounce that workflow instead.
                Left (TransactError.SystemDatabase (SystemDBError.QueueDeduplicated {})) -> bounceLoop exec pinned caller
                -- The fresh row is queued, so its handle is already a
                -- polling one; respelled to the caller's error type.
                other -> pure (adoptHandle exec <$> other)
    bounceRequest exec delayUntil =
      DebounceRequest
        { debounceRequestWorkflowName = refName userRef,
          debounceRequestClassName = refClassName userRef,
          debounceRequestConfigName = refConfigName userRef,
          debounceRequestQueueName = queueName,
          debounceRequestDeduplicationId = dedupKey,
          debounceRequestDelayUntil = delayUntil,
          debounceRequestInputs = (.serializedText) <$> input,
          debounceRequestSerialization = Just (inputSerialization exec.conn input),
          debounceRequestApplicationName = targetApp exec
        }
    queueName = case def.debouncerQueueName of
      Just name -> name
      Nothing -> case internalQueueName of QueueName name -> name
    dedupKey = classPrefix userRef <> refName userRef <> "-" <> key
    targetApp exec = def.debouncerApplicationName <|> exec.conn.connAppName

-- | Debounce from inside a running workflow: the bounce commits atomically
-- with its step checkpoint, as the oracle's @call_txn_as_step@ does, so a
-- crash can never commit one without the other. The fresh-enqueue leg
-- starts a child of the running workflow, which replays through its own
-- checkpoint; its id derives from the parent and the bounce's step, so a
-- replay re-enqueues the same id rather than a second row.
debounceInWorkflow :: (MonadMVar m, MonadTimer m, MonadTime m, MonadCatch m)
                   => WorkflowCtx exec m ->
                      WorkflowRef m TransactError.EngineOnly ->
                      Debouncer ->
                      Text ->
                      Duration ->
                      Maybe SerializedWorkflowValue ->
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
          | otherwise -> case checkPeriod period of
              Left err -> pure (Left err)
              Right () -> do
                stepId <- nextStepId wctx
                generated <- generatedWorkflowId conn
                let parentText = workflowId wctx
                    pinned = childWorkflowId Nothing (Just (parentText, stepId)) generated
                    caller = Just (WorkflowId parentText, stepId)
                bounceLoop conn pinned caller
  where
    bounceLoop conn pinned caller = do
      now <- timestampNow
      case addTimeout now period of
        Nothing ->
          pure (Left (TransactError.InvalidArgument "debounce" "the debounce period does not resolve to a representable wake time"))
        Just delayUntil -> do
          bounced <- runSystemDB conn.connSysdb (\db -> SystemDB.debounceDelayedWorkflow db (bounceRequest conn delayUntil) caller)
          case bounced of
            Left err -> pure (Left (TransactError.SystemDatabase err))
            Right (Debounced wid) -> pure (Right (pollingHandle conn wid True))
            Right (DebounceHeld holder) -> case classifyBounce holder (refName userRef) (refClassName userRef) (targetApp conn) of
              BounceRetry -> bounceLoop conn pinned caller
              BounceRaise ->
                pure
                  ( Left
                      ( TransactError.SystemDatabase
                          (SystemDBError.QueueDeduplicated {workflowId = pinned, queueName = queueName, deduplicationId = dedupKey})
                      )
                  )
            Right DebounceUnheld -> do
              enqueued <- startChildWorkflow wctx userRef (debounceOptions def queueName dedupKey period (Just (WorkflowId pinned))) input
              case enqueued of
                Left (TransactError.SystemDatabase (SystemDBError.QueueDeduplicated {})) -> bounceLoop conn pinned caller
                other -> pure (adoptHandleConn conn <$> other)
    bounceRequest conn delayUntil =
      DebounceRequest
        { debounceRequestWorkflowName = refName userRef,
          debounceRequestClassName = refClassName userRef,
          debounceRequestConfigName = refConfigName userRef,
          debounceRequestQueueName = queueName,
          debounceRequestDeduplicationId = dedupKey,
          debounceRequestDelayUntil = delayUntil,
          debounceRequestInputs = (.serializedText) <$> input,
          debounceRequestSerialization = Just (inputSerialization conn input),
          debounceRequestApplicationName = targetApp conn
        }
    queueName = case def.debouncerQueueName of
      Just name -> name
      Nothing -> case internalQueueName of QueueName name -> name
    dedupKey = classPrefix userRef <> refName userRef <> "-" <> key
    targetApp conn = def.debouncerApplicationName <|> conn.connAppName

-- * Helpers

-- | What a bounce that found no debounced row to extend means. Mirrors the
-- oracle's @classifyBounce@: a foreign holder — non-debounced, a different
-- workflow, or another application — is a conflict to report; a same-name
-- debounced holder that flipped out of DELAYED mid-bounce is a rare race
-- to retry. An empty class reads as absent, as Rust normalizes rows other
-- SDKs wrote, so a cross-SDK free function still coalesces.
data BounceAction = BounceRetry | BounceRaise

classifyBounce :: DebounceHolder -> Text -> Maybe Text -> Maybe Text -> BounceAction
classifyBounce holder workflowName className targetApp
  | not holder.debounceHolderIsDebounced = BounceRaise
  | holder.debounceHolderWorkflowName /= Just workflowName = BounceRaise
  | present holder.debounceHolderClassName /= className = BounceRaise
  | targetApp /= Nothing && holder.debounceHolderApplicationName /= Nothing && holder.debounceHolderApplicationName /= targetApp = BounceRaise
  | otherwise = BounceRetry
  where
    present value = if value == Just "" then Nothing else value

-- | The fresh debounced enqueue: the period's delay, the debounced mark
-- with the timeout's deadline, and no inherited deadline — a debounce
-- delay can be long, so an inherited absolute deadline could expire before
-- the debounced workflow ever runs. Mirrors the oracle's enqueue leg.
debounceOptions :: Debouncer -> Text -> Text -> Duration -> Maybe WorkflowId -> StartOptions
debounceOptions def queueName dedupKey period offered =
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
                applicationName = def.debouncerApplicationName
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

-- | A non-positive period schedules nothing: the bounce would stamp a delay
-- already past. Mirrors the oracle's guard.
checkPeriod :: Duration -> Either (TransactError.Error TransactError.EngineOnly) ()
checkPeriod period
  | durationAsMillis period <= 0 =
      Left (TransactError.InvalidArgument "debounce" ("debouncePeriodMs must be positive, not " <> Text.pack (show (durationAsMillis period))))
  | otherwise = Right ()

-- | The deduplication key: the workflow name under the key, class-prefixed
-- for instance workflows. Free functions read as the Python oracle writes
-- them (@\<workflow\>-\<key\>@); instance workflows carry their class, as
-- the TypeScript oracle's @Class.workflow-key@ does.
classPrefix :: WorkflowRef m e -> Text
classPrefix userRef = case refClassName userRef of
  Just className -> className <> "."
  Nothing -> ""

-- | The workflow's class, if it is a method workflow.
refClassName :: WorkflowRef m e -> Maybe Text
refClassName userRef = case userRef.refKey of WorkflowKey _ className _ -> className

-- | The workflow's configured instance, if it has one.
refConfigName :: WorkflowRef m e -> Maybe Text
refConfigName userRef = case userRef.refKey of WorkflowKey _ _ configName -> configName

-- | The stored wire for one pass-through input, or the connection's own
-- serialization when the input names none. Mirrors the start path, which
-- resolves the same default.
inputSerialization :: Connection m -> Maybe SerializedWorkflowValue -> Text
inputSerialization conn args = case args >>= (.serializedSerialization) of
  Just (Serialization name) -> name
  Nothing -> serializerName conn.connSerializer
