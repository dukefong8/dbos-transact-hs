{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}

-- | The context a durable call runs in, threaded explicitly. Mirrors Rust
-- @context.rs@: the workflow it belongs to, the step attempt it is inside,
-- and the backend it reaches the system database through.
--
-- Deviations, all forced by the module graph:
--
-- * There is no ambient carrier and no body monad: every engine call takes
--   its 'Ctx' as an explicit argument, the way the oracle's free functions
--   read one from a task-local. A fork starts with no context unless it is
--   handed one, which is the oracle's non-crossing-spawn rule made
--   structural.
-- * 'Ctx' holds the 'Connection' and the resolved 'Identity' rather than an
--   executor record: @context.rs@ and @instance.rs@ are mutually dependent
--   (the executor owns a context, the context names an executor) and
--   Haskell modules cannot cycle. Everything an executor would give a body
--   — the backend, the serializer, the poll interval, the application
--   stamp — is present.
-- * Step markers are a per-workflow counter rather than a process-wide one:
--   a marker is only ever compared for equality, and every comparison also
--   holds the workflow id, so a counter scoped to the workflow is the same
--   contract without needing IO to mint one. The execution identity is the
--   same move: a counter on the 'Connection' rather than Rust's pointer.
module DBOS.Transact.Context
  ( -- * The context
    Ctx,
    newCtx,
    currentConnection,
    currentIdentity,

    -- * What outlives any one call
    WorkflowState,
    newWorkflowState,
    workflowId,
    deadline,
    nextStepId,
    stepDepth,
    executionIdentityOf,
    isSameExecution,

    -- * Step scopes
    StepScope,
    newStepScope,
    StepMarker (..),
    nextStepMarker,
    StepStatus (..),
    stepStatusId,
    stepStatusCurrentAttempt,
    stepStatusMaxAttempts,
    firstStepStatus,
    nextAttempt,
    stepId,
    stepMarker,
    stepStatus,
    inStep,
    insideAStep,
    withAttempt,
    cancellationToken,
    cancelToken,
    tokenCancelled,
    raceCancel,

    -- * The engine's task seam
    TaskSpawner (..),
    LocalTaskOutcome (..),
    withTaskSpawner,
    taskSpawner,
    spawnLocal,

    -- * The engine's tracer seam
    withTracer,
    contextTracer,

    -- * Backend access
    withSystemDB,
    -- * Scoped workflow contexts
    WorkflowCtx,
    StepCtx,
    withWorkflow,
    withStep,
    nextWorkflowStepId,
    nextWorkflowMarker,
    workflowCtxId,
    stepCtxId,
    stepCtxStatus,
    stepCtxAt,
    stepCtxInner,
    stepCtxTracer,
    stepCtxWorkflow,
    stepCtxCancellationToken,
    workflowCtxInner,
    withWorkflowTaskSpawner,
  )
where

import DBOS.Prelude
import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM, StrictTVar, atomically, modifyTVar, newTVarIO, readTVar, readTVarIO, writeTVar)
import Control.Monad.Class.MonadThrow qualified as MThrow
import Data.Kind (Type)
import Data.Text (Text)
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Types (Timestamp, WorkflowId, workflowIdText)
import DBOS.Tracer (SomeTracer)
import DBOS.Transact.Connection (Connection (..), ExecutionIdentity, nextExecutionIdentity, runSystemDB)
import DBOS.Transact.Identity (Identity)

-- | What a durable call knows about where it runs. The constructor is
-- private; 'newCtx' builds one and the readers below read it.
data Ctx m = Ctx
  { ctxConn     :: Connection m,
    ctxIdentity :: Identity,
    ctxWorkflow :: WorkflowState m,
    ctxStep     :: Maybe (StepScope m),
    ctxSpawner  :: Maybe (TaskSpawner m),
    ctxTracer   :: SomeTracer m
  }

-- | What a locally spawned, tracked task left behind: its value, the
-- cancellation shutdown performs, or the exception a panicking body threw.
-- Classifying at the spawn keeps the exception's identity — the one thing
-- io-classes cannot spell — in the module that already imports it, so a
-- handle only ever reads the outcome.
data LocalTaskOutcome a
  = LocalTaskValue a
  | LocalTaskCancelled
  | LocalTaskPanic MThrow.SomeException

-- | How a body reaches the task registry of the executor running it, so a
-- child it starts is detached, counted, and abortable by shutdown — the
-- oracle's @Arc&lt;Executor&gt;@ inside @Ctx@, narrowed to the one capability a
-- body needs. Injected by the engine when it builds the execution's
-- context ('withTaskSpawner'); a context built without one (a test, a
-- client-side body) has no spawner and child starts stay record-only.
--
-- Lives here rather than in @Workflow@ because @Workflow@ imports this
-- module: the type is the seam, the implementation that fills it is the
-- engine's.
data TaskSpawner m = TaskSpawner
  { spawnLocalTask :: forall a. (TaskSpawner m -> m a) -> m (StrictMVar m (LocalTaskOutcome a))
  }

-- | Spawns an action detached on the spawner's registry and hands back the
-- box its outcome lands in, filled while the task unwinds. The one call
-- site for the record's field, so callers never touch record-dot on a
-- function-typed field.
spawnLocal :: TaskSpawner m -> (TaskSpawner m -> m a) -> m (StrictMVar m (LocalTaskOutcome a))
spawnLocal (TaskSpawner spawn) = spawn

-- | Two contexts are equal when they are the same execution and the same
-- step body — identity, not structure, because the mutable refs inside
-- cannot be compared. This is what a placement refusal names.
instance Eq (Ctx m) where
  first == second =
    first.ctxWorkflow.executionIdentity == second.ctxWorkflow.executionIdentity
      && fmap (.scopeMarker) first.ctxStep == fmap (.scopeMarker) second.ctxStep

instance Show (Ctx m) where
  show ctx = "Ctx " <> show (workflowId ctx) <> " " <> show (stepId ctx)

-- | The parts of a workflow that outlive any one call within it. Mirrors
-- Rust @WorkflowState@: the id, the deadline the database holds, the step
-- id counter, the marker counter, and the execution's identity.
data WorkflowState m = WorkflowState
  { workflowId        :: Text,
    deadline          :: Maybe Timestamp,
    nextStepIdRef     :: StrictTVar m Int,
    nextMarkerRef     :: StrictTVar m Int,
    stepDepthRef      :: StrictTVar m Int,
    executionIdentity :: ExecutionIdentity
  }

-- | A workflow's state: the id, the deadline the row carries, and the
-- identity that tells a re-run of the same id apart from the run itself.
newWorkflowState :: MonadSTM m => Text -> Maybe Timestamp -> ExecutionIdentity -> m (WorkflowState m)
newWorkflowState workflowText deadlineAt identity = do
  stepRef <- newTVarIO 0
  markerRef <- newTVarIO 0
  depthRef <- newTVarIO 0
  pure
    WorkflowState
      { workflowId = workflowText,
        deadline = deadlineAt,
        nextStepIdRef = stepRef,
        nextMarkerRef = markerRef,
        stepDepthRef = depthRef,
        executionIdentity = identity
      }

-- | The context a body runs in: the connection, the resolved identity, and
-- the workflow state. No step scope: this is the workflow proper. The
-- tracer rides in on the connection, so every execution announces through
-- its owner's backend unless a test rebinds it with 'withTracer'.
newCtx :: MonadSTM m => Connection m -> Identity -> WorkflowState m -> m (Ctx m)
newCtx conn identity state =
  pure
    Ctx
      { ctxConn = conn,
        ctxIdentity = identity,
        ctxWorkflow = state,
        ctxStep = Nothing,
        ctxSpawner = Nothing,
        ctxTracer = conn.connTracer
      }

-- | Rebinds the context to carry the executor's task spawner. The engine
-- calls this when it builds an execution's context; rebind rather than
-- mutate, exactly like 'withAttempt'.
withTaskSpawner :: Ctx m -> TaskSpawner m -> Ctx m
withTaskSpawner ctx spawner = ctx {ctxSpawner = Just spawner}

-- | Rebinds the context to trace resource-lifetime events through the
-- given backend. The engine calls this when it builds an execution's
-- context; rebind rather than mutate, exactly like 'withTaskSpawner'.
withTracer :: SomeTracer m -> Ctx m -> Ctx m
withTracer tracer ctx = ctx {ctxTracer = tracer}

-- | The tracer this execution's resource-lifetime events go through: the
-- engine's FastLogger backend in production, the io-sim trace in
-- simulations, silence in tests that install nothing.
contextTracer :: Ctx m -> SomeTracer m
contextTracer ctx = ctx.ctxTracer

-- | The spawner this context was given, if the engine gave it one.
taskSpawner :: Ctx m -> Maybe (TaskSpawner m)
taskSpawner ctx = ctx.ctxSpawner

-- | The connection this call reaches the system database through.
currentConnection :: Ctx m -> Connection m
currentConnection ctx = ctx.ctxConn

-- | The resolved deployment identity this call's rows are stamped with.
currentIdentity :: Ctx m -> Identity
currentIdentity ctx = ctx.ctxIdentity

-- | The id of the workflow this call belongs to.
workflowId :: Ctx m -> Text
workflowId ctx = ctx.ctxWorkflow.workflowId

-- | When this workflow must stop, if it has a deadline at all.
deadline :: Ctx m -> Maybe Timestamp
deadline ctx = ctx.ctxWorkflow.deadline

-- | The identity of the execution this context belongs to. Pointer
-- identity in the oracle; here an opaque token minted once per execution.
executionIdentityOf :: Ctx m -> ExecutionIdentity
executionIdentityOf ctx = ctx.ctxWorkflow.executionIdentity

-- | Whether two contexts are the same execution of the same workflow.
isSameExecution :: Ctx m -> Ctx m -> Bool
isSameExecution first second =
  executionIdentityOf first == executionIdentityOf second

-- | Allocate the next zero-based step id in this workflow. Zero-based so
-- the first step is step 0, matching Go, TypeScript and Java.
nextStepId :: MonadSTM m => Ctx m -> m Int
nextStepId ctx = atomically $ do
    current <- readTVar ctx.ctxWorkflow.nextStepIdRef
    writeTVar ctx.ctxWorkflow.nextStepIdRef (current + 1)
    pure current

-- | How many step bodies deep this execution currently runs: zero at a
-- step boundary and outside workflows, one inside a step body, more under
-- nesting — sequential or concurrent, either way an allocation inside is
-- refused. Bumped by 'withAttempt' on entry and restored on every exit,
-- so a call reaching through a captured parent sees the running body
-- exactly like the handed view does.
stepDepth :: MonadSTM m => Ctx m -> m Int
stepDepth ctx = readTVarIO ctx.ctxWorkflow.stepDepthRef

-- | Which step body a context is inside. Opaque and equality-only; a fresh
-- value per attempt, so two bodies of one workflow cannot be confused.
newtype StepMarker = StepMarker Int
  deriving stock (Eq, Show)

-- | Mint a marker for a step body entering its scope.
nextStepMarker :: MonadSTM m => Ctx m -> m StepMarker
nextStepMarker ctx = do
  n <- atomically $ do
    current <- readTVar ctx.ctxWorkflow.nextMarkerRef
    writeTVar ctx.ctxWorkflow.nextMarkerRef (current + 1)
    pure current
  pure (StepMarker n)

-- | What a step body can learn about the attempt it is running as: the
-- step's ordinal position counting from zero, which attempt is running
-- counting from one, and how many attempts the policy allows in total.
data StepStatus = StepStatus
  { step_id         :: Int,
    current_attempt :: Word,
    max_attempts    :: Word
  }
  deriving stock (Eq, Show)

-- | The step's ordinal position in its workflow, counting from zero.
stepStatusId :: StepStatus -> Int
stepStatusId status = status.step_id

-- | Which attempt is running, counting from one.
stepStatusCurrentAttempt :: StepStatus -> Word
stepStatusCurrentAttempt status = status.current_attempt

-- | How many attempts the policy allows in total — a ceiling a retry
-- predicate may stop short of, not a promise.
stepStatusMaxAttempts :: StepStatus -> Word
stepStatusMaxAttempts status = status.max_attempts

-- | A first attempt at a step: attempt 1 of 1. A plain step honestly
-- reports itself this way; "does this step retry?" is @max_attempts > 1@.
firstStepStatus :: Int -> StepStatus
firstStepStatus stepId' =
  StepStatus
    { step_id = stepId',
      current_attempt = 1,
      max_attempts = 1
    }

-- | The following attempt at the same step: same id and cap, count moved.
nextAttempt :: StepStatus -> StepStatus
nextAttempt status =
  status {current_attempt = status.current_attempt + 1}

-- | What a context inside a step body knows: which body (for the leaf
-- rule), what it may report about its attempt, and the token that fires
-- when the attempt is abandoned.
data StepScope m = StepScope
  { scopeMarker       :: StepMarker,
    scopeStatus       :: StepStatus,
    scopeCancellation :: StrictTVar m Bool
  }

-- | A scope for one attempt, with a fresh cancellation token.
newStepScope :: MonadSTM m => StepMarker -> StepStatus -> m (StepScope m)
newStepScope marker status = do
  token <- newTVarIO False
  pure
    StepScope
      { scopeMarker = marker,
        scopeStatus = status,
        scopeCancellation = token
      }

-- | The id of the step body this context is inside, or 'Nothing' in the
-- workflow proper: between two steps a workflow is inside neither.
stepId :: Ctx m -> Maybe Int
stepId ctx = case ctx.ctxStep of
  Nothing    -> Nothing
  Just scope -> Just scope.scopeStatus.step_id

-- | Which step body this context is inside, if any. 'stepId' asks whether
-- there is one; this asks which, which is what tells two bodies of one
-- workflow apart.
stepMarker :: Ctx m -> Maybe StepMarker
stepMarker ctx = fmap (.scopeMarker) ctx.ctxStep

-- | What the step body may read about its own attempt, or 'Nothing'
-- outside one.
stepStatus :: Ctx m -> Maybe StepStatus
stepStatus ctx = case ctx.ctxStep of
  Nothing    -> Nothing
  Just scope -> Just scope.scopeStatus

-- | Whether this context is inside a step body. Per call, not per
-- workflow: a sibling step running concurrently has no bearing on it.
inStep :: Ctx m -> Bool
inStep ctx = case ctx.ctxStep of
  Nothing -> False
  Just _  -> True

-- | Whether this execution is inside a step body right now — through the
-- context in hand ('inStep') or through a captured parent while a body
-- runs (the shared depth counter). Every guard that refuses or degrades
-- inside a step reads this, never the scope field alone, so the
-- captured-parent shape gets the same verdict as the handed view.
insideAStep :: MonadSTM m => Ctx m -> m Bool
insideAStep ctx
  | inStep ctx = pure True
  | otherwise = (> 0) <$> stepDepth ctx

-- | Runs a body under a context that is 'inStep': this attempt's marker
-- and status, and a fresh cancellation flag. Rebinding rather than
-- mutating — the scope lives on the context handed to the body alone, so
-- it goes out of scope with the body however the body ends. Abandoning the
-- attempt (cancellation, timeout kill, dropped future) fires the token, as
-- the oracle's drop guard does; completing it leaves the token quiet.
withAttempt :: (MonadSTM m, MonadCatch m) => Ctx m -> StepMarker -> StepStatus -> (Ctx m -> m a) -> m a
withAttempt ctx marker status body = do
  scope <- newStepScope marker status
  atomically (modifyTVar ctx.ctxWorkflow.stepDepthRef (+ 1))
  -- Cancel and unbump in one transaction: the token fires and the depth
  -- restores together, never one without the other. Restored explicitly
  -- rather than through 'finally' so this keeps its 'MonadCatch'
  -- constraint — no 'MonadMask' cascade through the step API. The
  -- residual window (an async kill landing between the body's return and
  -- the restore below) leaks safe: a stuck depth degrades later calls to
  -- plain rather than corrupting any position.
  outcome <- body ctx {ctxStep = Just scope} `onException` atomically (writeTVar scope.scopeCancellation True >> modifyTVar ctx.ctxWorkflow.stepDepthRef (subtract 1))
  atomically (modifyTVar ctx.ctxWorkflow.stepDepthRef (subtract 1))
  pure outcome

-- | A token that fires when the step running here is abandoned. Outside a
-- step it never fires, so a body that is also called outside a workflow
-- needs no second path.
cancellationToken :: MonadSTM m => Ctx m -> m (StrictTVar m Bool)
cancellationToken ctx = case ctx.ctxStep of
  Just scope -> pure scope.scopeCancellation
  Nothing    -> newTVarIO False

-- | Run an action until it completes or this execution's token fires,
-- whichever comes first: 'Just' the value on completion, 'Nothing' on
-- cancellation. Outside a step the token never fires, so this is just the
-- action. Cooperative cancellation in one call — a step body that awaits
-- 'raceCancel' honors workflow cancellation and attempt timeouts without a
-- hand-polling loop, mirroring the skill's timeout-plus-abort-signal rule
-- ('step-timeouts.md') in polled-token form.
raceCancel :: (MonadSTM m, MonadAsync m) => Ctx m -> m a -> m (Maybe a)
raceCancel ctx action = do
  token <- cancellationToken ctx
  outcome <- race action (atomically (readTVar token >>= check))
  pure $ case outcome of
    Left value -> Just value
    Right () -> Nothing

-- | Fires a token: work watching it should stop.
cancelToken :: MonadSTM m => StrictTVar m Bool -> m ()
cancelToken token = atomically (writeTVar token True)

-- | Whether a token has fired.
tokenCancelled :: MonadSTM m => StrictTVar m Bool -> m Bool
tokenCancelled = readTVarIO

-- | Run a class method against this context's backend, passing the handle
-- explicitly. The one place the connection's existential is unpacked.
withSystemDB :: Monad m => Ctx m -> (forall db. SystemDB.SystemDB db m => db -> m a) -> m a
withSystemDB ctx action = runSystemDB ctx.ctxConn.connSysdb action

-- * Scoped workflow contexts: one execution's view and one attempt's view.
--
-- 'WorkflowCtx' owns its counters (step, marker, depth) through the shared
-- 'WorkflowState'; 'StepCtx' narrows it to a single attempt. Both are
-- built only by the runners below — constructors stay private — and both
-- are branded by execution, so a value from one run cannot be driven in
-- another. Readers expose ids, statuses, and counters; nothing exposes
-- the inner 'Ctx', so allocation stays on 'WorkflowCtx'.

-- | One execution's workflow context: the connection, identity, and fresh
-- workflow state a body runs with. Built only by 'withWorkflow', which
-- mints the state new — the inner context always starts outside any step.
data WorkflowCtx (exec :: Type) m = WorkflowCtx
  { workflowCtx :: Ctx m
  }

-- | One attempt's narrowed view: the execution it belongs to and the
-- attempt's scope. Built only by 'withStep'. Readers expose the id and
-- the status — never a counter and never the inner context, so only
-- 'WorkflowCtx' allocates.
data StepCtx (exec :: Type) m = StepCtx
  { stepCtxWorkflow :: WorkflowCtx exec m,
    stepCtxInner :: Ctx m
  }

-- | Run an execution's body under a fresh workflow context: a new
-- execution identity, fresh counters, and no step scope. The rank-2
-- continuation binds the execution scope — values built inside cannot
-- escape it, so one run's counters never leak into another's.
withWorkflow :: MonadSTM m => Connection m -> Identity -> WorkflowId -> Maybe Timestamp -> (forall exec. WorkflowCtx exec m -> m a) -> m a
withWorkflow conn identity wid deadline run = do
  execution <- nextExecutionIdentity conn
  state <- newWorkflowState (workflowIdText wid) deadline execution
  inner <- newCtx conn identity state
  run (WorkflowCtx inner)

-- | Run one attempt under the narrowed view. Delegates bump, restore, and
-- cancel to 'withAttempt', so the depth semantics stay in one place; what
-- differs is the handoff — a 'StepCtx', never a bare 'Ctx' another
-- allocator could spend.
withStep :: (MonadSTM m, MonadCatch m) => WorkflowCtx exec m -> StepMarker -> StepStatus -> (StepCtx exec m -> m a) -> m a
withStep wctx marker status body =
  withAttempt wctx.workflowCtx marker status $ \stepped ->
    body (StepCtx wctx stepped)

-- | Allocate the next step id in this execution. Only 'WorkflowCtx' can
-- spend the counter — the narrowed view exposes no allocator.
nextWorkflowStepId :: MonadSTM m => WorkflowCtx exec m -> m Int
nextWorkflowStepId wctx = nextStepId wctx.workflowCtx

-- | Mint the next attempt marker in this execution. Markers spend their
-- own sequence beside the step ids, as in the oracle.
nextWorkflowMarker :: MonadSTM m => WorkflowCtx exec m -> m StepMarker
nextWorkflowMarker wctx = nextStepMarker wctx.workflowCtx

-- | The id of the workflow this execution runs.
workflowCtxId :: WorkflowCtx exec m -> Text
workflowCtxId wctx = workflowId wctx.workflowCtx

-- | The id of the workflow this attempt belongs to.
stepCtxId :: StepCtx exec m -> Text
stepCtxId sctx = workflowId sctx.stepCtxInner

-- | The inner context behind a workflow view, for engine paths that
-- delegate to the context-level machinery.
workflowCtxInner :: WorkflowCtx exec m -> Ctx m
workflowCtxInner wctx = wctx.workflowCtx

-- | Rebind a workflow view's execution to a task spawner: what the run
-- path installs before handing the view to a body, so child starts reach
-- the executor's task registry.
withWorkflowTaskSpawner :: TaskSpawner m -> WorkflowCtx exec m -> WorkflowCtx exec m
withWorkflowTaskSpawner spawner wctx = wctx {workflowCtx = withTaskSpawner wctx.workflowCtx spawner}

-- | Rebuild the narrowed view around an attempt's inner context: what the
-- engine's step runner hands a body, built from the scope it is about to
-- run. Exported for the step seam; application code obtains views only
-- through 'withStep'.
stepCtxAt :: WorkflowCtx exec m -> Ctx m -> StepCtx exec m
stepCtxAt wctx inner = StepCtx wctx inner

-- | The workflow view this attempt belongs to. Reaching the parent's
-- operations from inside a step body is the captured-parent shape, which
-- the depth backstop reads together with the handed context — a call
-- through it degrades or refuses exactly as a call through the handed
-- context would. Same execution brand, so the type stays quiet and the
-- runtime keeps the verdict.
stepCtxWorkflow :: StepCtx exec m -> WorkflowCtx exec m
stepCtxWorkflow (StepCtx wctx _) = wctx

-- | The inner context behind a step view: the documented downgrade for
-- reader calls that have no scoped twin yet (a step body reads its
-- cancellation token, step id, or deadline through it). Widening is
-- explicit and greppable; the C5 pass twins the hot readers and deletes
-- these uses.
stepCtxInner :: StepCtx exec m -> Ctx m
stepCtxInner sctx = sctx.stepCtxInner

-- | The tracer behind a step view, for engine paths that must announce
-- through the view's execution without widening it.
stepCtxTracer :: StepCtx exec m -> SomeTracer m
stepCtxTracer sctx = contextTracer sctx.stepCtxInner

-- | The cancellation token behind a step view: the token that fires
-- when the step running here is abandoned. The view-taking twin of
-- 'cancellationToken', for step bodies that poll their own abandonment
-- without widening to the inner context.
stepCtxCancellationToken :: MonadSTM m => StepCtx exec m -> m (StrictTVar m Bool)
stepCtxCancellationToken sctx = cancellationToken (stepCtxInner sctx)

-- | What this attempt may read about itself, or 'Nothing' outside any
-- attempt — which a handed 'StepCtx' never is. Kept 'Maybe' like the
-- reader it projects; the narrowed view's guarantee is which values
-- exist, not totality.
stepCtxStatus :: StepCtx exec m -> Maybe StepStatus
stepCtxStatus sctx = stepStatus sctx.stepCtxInner
