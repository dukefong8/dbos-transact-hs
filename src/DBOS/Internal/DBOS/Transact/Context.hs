{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}

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
  ( -- * The context: one execution's view, one attempt's view
    -- Fields are exported for reads (dot / record syntax); construction
    -- still goes through 'withWorkflow' or the engine's 'newWorkflowCtx'.
    WorkflowCtx (wctxConn, wctxIdentity, wctxState, wctxSpawner, wctxTracer),
    -- 'stepCtxWorkflow' is exported for reads (the captured-parent shape);
    -- the attempt scope stays behind the derived readers below.
    StepCtx (stepCtxWorkflow),
    withWorkflow,
    withStep,
    newWorkflowCtx,
    stepCtxBoundary,

    -- * What outlives any one call
    WorkflowState,
    newWorkflowState,
    withTracer,
    withWorkflowTaskSpawner,
    deadline,
    stepDepth,
    isSameExecution,

    -- * Step scopes
    StepScope,
    newStepScope,
    StepMarker (..),
    StepStatus,
    stepStatusAt,
    stepStatusId,
    stepStatusCurrentAttempt,
    stepStatusMaxAttempts,
    firstStepStatus,
    nextAttempt,
    stepId,
    stepMarker,
    stepStatus,
    insideAStep,
    cancellationToken,
    cancelToken,
    tokenCancelled,
    raceCancel,

    -- * The engine's task seam
    TaskSpawner (..),
    LocalTaskOutcome (..),
    spawnLocal,

    -- * Backend access
    withSystemDB,

    -- * Execution readers
    nextStepId,
    nextWorkflowMarker,
    workflowId,
    stepCtxStatus,
    stepCtxCancellationToken,
  )
where

import DBOS.Prelude
import Data.Kind (Type)
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Types (Timestamp, WorkflowId (..))
import DBOS.Transact.Logger (LogCtx (..), SomeTracer)
import DBOS.Transact.Connection (Connection (..), ExecutionIdentity, nextExecutionIdentity, runSystemDB)
import DBOS.Transact.Identity (Identity)

-- | What a locally spawned, tracked task left behind: its value, the
-- cancellation shutdown performs, or the exception a panicking body threw.
-- Classifying at the spawn keeps the exception's identity — the one thing
-- io-classes cannot spell — in the module that already imports it, so a
-- handle only ever reads the outcome.
data LocalTaskOutcome a
  = LocalTaskValue a
  | LocalTaskCancelled
  | LocalTaskPanic SomeException

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

-- | Two views are equal when they are the same execution — identity, not
-- structure, because the mutable refs inside cannot be compared. This is
-- what a placement refusal names.
instance Eq (WorkflowCtx exec m) where
  first == second =
    first.wctxState.executionIdentity == second.wctxState.executionIdentity

instance Show (WorkflowCtx exec m) where
  show wctx = "WorkflowCtx " <> show (workflowId wctx)

-- | Two attempt views are equal when they are the same execution and the
-- same step body.
instance Eq (StepCtx exec m) where
  first == second =
    first.stepCtxWorkflow == second.stepCtxWorkflow
      && fmap (.scopeMarker) first.stepCtxScope == fmap (.scopeMarker) second.stepCtxScope

instance Show (StepCtx exec m) where
  show sctx = "StepCtx " <> show (workflowId sctx.stepCtxWorkflow) <> " " <> show (stepId sctx)

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
newWorkflowState :: MonadSTM m
                 => Text -> Maybe Timestamp -> ExecutionIdentity -> m (WorkflowState m)
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

-- | Rebinds a view to trace resource-lifetime events through the given
-- backend: the engine's FastLogger backend in production, the io-sim
-- trace in simulations, silence in tests that install nothing. Rebind
-- rather than mutate, exactly like 'withWorkflowTaskSpawner'.
withTracer :: SomeTracer m -> WorkflowCtx exec m -> WorkflowCtx exec m
withTracer tracer wctx = wctx {wctxTracer = tracer}

-- | When this workflow must stop, if it has a deadline at all. Deep
-- enough (a state field behind the view) to keep as a reader.
deadline :: WorkflowCtx exec m -> Maybe Timestamp
deadline wctx = wctx.wctxState.deadline

-- | Whether two views are the same execution of the same workflow:
-- pointer identity in the oracle, an opaque per-execution token here.
isSameExecution :: WorkflowCtx exec m -> WorkflowCtx exec m -> Bool
isSameExecution first second =
  first.wctxState.executionIdentity == second.wctxState.executionIdentity

-- | How many step bodies deep this execution currently runs: zero at a
-- step boundary and outside workflows, one inside a step body, more under
-- nesting — sequential or concurrent, either way an allocation inside is
-- refused. Bumped by 'withAttempt' on entry and restored on every exit,
-- so a call reaching through a captured parent sees the running body
-- exactly like the handed view does.
stepDepth :: MonadSTM m => WorkflowCtx exec m -> m Int
stepDepth wctx = readTVarIO wctx.wctxState.stepDepthRef

-- | Which step body a context is inside. Opaque and equality-only; a fresh
-- value per attempt, so two bodies of one workflow cannot be confused.
newtype StepMarker = StepMarker Int
  deriving stock (Eq, Show)


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

-- | A specific attempt at a step: its ordinal, which attempt, and the
-- ceiling the policy allows. The retry path builds each attempt's status
-- through this; 'firstStepStatus' is the common case.
stepStatusAt :: Int -> Word -> Word -> StepStatus
stepStatusAt stepId' attempt maxAttempts =
  StepStatus
    { step_id = stepId',
      current_attempt = attempt,
      max_attempts = maxAttempts
    }

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
stepId :: StepCtx exec m -> Maybe Int
stepId sctx = case sctx.stepCtxScope of
  Nothing    -> Nothing
  Just scope -> Just scope.scopeStatus.step_id

-- | Which step body this context is inside, if any. 'stepId' asks whether
-- there is one; this asks which, which is what tells two bodies of one
-- workflow apart.
stepMarker :: StepCtx exec m -> Maybe StepMarker
stepMarker sctx = fmap (.scopeMarker) sctx.stepCtxScope

-- | What the step body may read about its own attempt, or 'Nothing'
-- outside one.
stepStatus :: StepCtx exec m -> Maybe StepStatus
stepStatus sctx = case sctx.stepCtxScope of
  Nothing    -> Nothing
  Just scope -> Just scope.scopeStatus

-- | Whether this execution is inside a step body right now: the shared
-- depth counter is nonzero while a body runs. Every guard that refuses or
-- degrades inside a step reads this — a handed 'StepCtx' is statically
-- in-step and never reaches these guards; a call through a captured
-- parent view reads the running body through the counter. One predicate,
-- same verdict either way.
insideAStep :: MonadSTM m => WorkflowCtx exec m -> m Bool
insideAStep wctx = (> 0) <$> stepDepth wctx

-- | A token that fires when the step running here is abandoned. Outside a
-- step it never fires, so a body that is also called outside a workflow
-- needs no second path.
cancellationToken :: MonadSTM m => StepCtx exec m -> m (StrictTVar m Bool)
cancellationToken sctx = case sctx.stepCtxScope of
  Just scope -> pure scope.scopeCancellation
  Nothing    -> newTVarIO False

-- | Run an action until it completes or this execution's token fires,
-- whichever comes first: 'Just' the value on completion, 'Nothing' on
-- cancellation. Outside a step the token never fires, so this is just the
-- action. Cooperative cancellation in one call — a step body that awaits
-- 'raceCancel' honors workflow cancellation and attempt timeouts without a
-- hand-polling loop, mirroring the skill's timeout-plus-abort-signal rule
-- ('step-timeouts.md') in polled-token form.
raceCancel :: (MonadAsync m) => StepCtx exec m -> m a -> m (Maybe a)
raceCancel sctx action = do
  token <- cancellationToken sctx
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
withSystemDB ::  WorkflowCtx exec m -> (forall db. SystemDB.SystemDB db m => db -> m a) -> m a
withSystemDB wctx action = runSystemDB wctx.wctxConn.connSysdb action

-- * Scoped workflow contexts: one execution's view and one attempt's view.
--
-- 'WorkflowCtx' owns its counters (step, marker, depth) through the shared
-- 'WorkflowState'; 'StepCtx' narrows it to a single attempt. Both are
-- built only by the runners below — constructors stay private — and both
-- are branded by execution, so a value from one run cannot be driven in
-- another. Readers expose ids, statuses, and counters; nothing exposes
-- the inner 'Ctx', so allocation stays on 'WorkflowCtx'.

-- | One execution's workflow context: the connection, identity, and fresh
-- workflow state a body runs with, plus the task spawner the run path
-- installs and the tracer announcements go to. Built by 'withWorkflow',
-- which mints the state new — a fresh view always starts outside any step
-- — or by the engine's 'newWorkflowCtx'. Branded by execution, so one
-- run's counters never leak into another's; the fields are exported for
-- reads, not for construction.
data WorkflowCtx (exec :: Type) m = WorkflowCtx
  { wctxConn     :: Connection m,
    wctxIdentity :: Identity,
    wctxState    :: WorkflowState m,
    wctxSpawner  :: Maybe (TaskSpawner m),
    wctxTracer   :: SomeTracer m
  }

-- | One attempt's narrowed view: the execution it belongs to
-- ('stepCtxWorkflow', readable for the captured-parent shape) and the
-- attempt's scope (kept behind the derived readers: 'stepId',
-- 'stepMarker', 'stepStatus', 'stepCtxStatus',
-- 'stepCtxCancellationToken'). Built by 'withStep' and the engine's
-- drives; only 'WorkflowCtx' allocates and only the engine reaches the
-- database.
data StepCtx (exec :: Type) m = StepCtx
  { stepCtxWorkflow :: WorkflowCtx exec m,
    stepCtxScope    :: Maybe (StepScope m)
  }

-- | The context records are what the logger helpers accept: each view
-- carries its execution's tracer, so one set of helpers serves workflow
-- and step bodies alike. The instances live here — not in
-- "DBOS.Transact.Logger" — because the log module is the carrier a context
-- must import, and modules cannot cycle.
instance LogCtx (WorkflowCtx exec m) m where
  contextTracer ctx = ctx.wctxTracer

instance LogCtx (StepCtx exec m) m where
  contextTracer ctx = ctx.stepCtxWorkflow.wctxTracer

-- | Run an execution's body under a fresh workflow context: a new
-- execution identity, fresh counters, and no step scope. The rank-2
-- continuation binds the execution scope — values built inside cannot
-- escape it, so one run's counters never leak into another's.
withWorkflow :: MonadSTM m
             => Connection m -> Identity -> WorkflowId -> Maybe Timestamp -> (forall exec. WorkflowCtx exec m -> m a) -> m a
withWorkflow conn identity (WorkflowId widText) deadline run = do
  execution <- nextExecutionIdentity conn
  state <- newWorkflowState widText deadline execution
  run =<< newWorkflowCtx conn identity state

-- | Build one execution's workflow view around a state the caller already
-- holds: a fresh execution identity is minted by 'withWorkflow', which is
-- the only builder application code needs, because its rank-2 binder is
-- what keeps one run's brand from leaking into another. This seam exists
-- for engine paths and test fixtures that must hold a view outside a
-- continuation; it binds the brand at the call site exactly like the old
-- 'newCtx' did. Nothing here exposes allocation to a 'StepCtx' — the
-- narrowed view still owns no allocator.
newWorkflowCtx :: MonadSTM m
               => Connection m -> Identity -> WorkflowState m -> m (WorkflowCtx exec m)
newWorkflowCtx conn identity state =
  pure
    WorkflowCtx
      { wctxConn = conn,
        wctxIdentity = identity,
        wctxState = state,
        wctxSpawner = Nothing,
        wctxTracer = conn.connTracer
      }

-- | Run one attempt under the narrowed view. Bumps the shared depth on
-- entry and restores it on every exit; abandoning the attempt
-- (cancellation, timeout kill, dropped future) fires the token, as the
-- oracle's drop guard does, completing it leaves the token quiet. Cancel
-- and unbump in one transaction so the token fires and the depth restores
-- together, never one without the other. Restored explicitly rather than
-- through 'finally' so this keeps its 'MonadCatch' constraint — no
-- 'MonadMask' cascade through the step API. The residual window (an async
-- kill landing between the body's return and the restore below) leaks
-- safe: a stuck depth degrades later calls to plain rather than
-- corrupting any position. The scope lives on the view handed to the body
-- alone, so it goes out of scope with the body however the body ends.
withStep :: (MonadSTM m, MonadCatch m)
         => WorkflowCtx exec m -> StepMarker -> StepStatus -> (StepCtx exec m -> m a) -> m a
withStep wctx marker status body = do
  scope <- newStepScope marker status
  atomically (modifyTVar wctx.wctxState.stepDepthRef (+ 1))
  outcome <- body (StepCtx wctx (Just scope)) `onException` atomically (writeTVar scope.scopeCancellation True >> modifyTVar wctx.wctxState.stepDepthRef (subtract 1))
  atomically (modifyTVar wctx.wctxState.stepDepthRef (subtract 1))
  pure outcome

-- | Allocate the next step id in this execution. Only 'WorkflowCtx' can
-- spend the counter — the narrowed view exposes no allocator.
nextStepId :: MonadSTM m => WorkflowCtx exec m -> m Int
nextStepId wctx = atomically $ do
    current <- readTVar wctx.wctxState.nextStepIdRef
    writeTVar wctx.wctxState.nextStepIdRef (current + 1)
    pure current

-- | Mint the next attempt marker in this execution. Markers spend their
-- own sequence beside the step ids, as in the oracle.
nextWorkflowMarker :: MonadSTM m => WorkflowCtx exec m -> m StepMarker
nextWorkflowMarker wctx = do
  n <- atomically $ do
    current <- readTVar wctx.wctxState.nextMarkerRef
    writeTVar wctx.wctxState.nextMarkerRef (current + 1)
    pure current
  pure (StepMarker n)

-- | The id of the workflow this execution runs — the workflow's own id,
-- not the context's: a context's execution identity is the state's
-- @executionIdentity@. A step view reads the same id through its parent:
-- @workflowId sctx.stepCtxWorkflow@.
workflowId :: WorkflowCtx exec m -> Text
workflowId wctx = wctx.wctxState.workflowId

-- | Rebind a workflow view's execution to a task spawner: what the run
-- path installs before handing the view to a body, so child starts reach
-- the executor's task registry.
withWorkflowTaskSpawner :: TaskSpawner m -> WorkflowCtx exec m -> WorkflowCtx exec m
withWorkflowTaskSpawner spawner wctx = wctx {wctxSpawner = Just spawner}

-- | A step view with no scope: what a drive polling at a step boundary
-- hands where an attempt view is expected. Reads like the workflow proper
-- (no step id, a token that never fires), drives like its execution.
stepCtxBoundary :: WorkflowCtx exec m -> StepCtx exec m
stepCtxBoundary wctx = StepCtx wctx Nothing

-- | The cancellation token behind a step view: the token that fires
-- when the step running here is abandoned. The view-taking twin of
-- 'cancellationToken', for step bodies that poll their own abandonment
-- without widening to the inner context.
stepCtxCancellationToken :: MonadSTM m => StepCtx exec m -> m (StrictTVar m Bool)
stepCtxCancellationToken sctx = case sctx.stepCtxScope of
  Just scope -> pure scope.scopeCancellation
  Nothing -> newTVarIO False

-- | What this attempt may read about itself, or 'Nothing' outside any
-- attempt — which a handed 'StepCtx' never is. Kept 'Maybe' like the
-- reader it projects; the narrowed view's guarantee is which values
-- exist, not totality.
stepCtxStatus :: StepCtx exec m -> Maybe StepStatus
stepCtxStatus sctx = fmap (.scopeStatus) sctx.stepCtxScope
