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
    withAttempt,
    cancellationToken,
    cancelToken,
    tokenCancelled,

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
  )
where

import DBOS.Prelude
import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM, StrictTVar, atomically, newTVarIO, readTVar, readTVarIO, writeTVar)
import Control.Monad.Class.MonadThrow qualified as MThrow
import Data.Text (Text)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Types (Timestamp)
import DBOS.Tracer (SomeTracer)
import DBOS.Transact.Connection (Connection (..), ExecutionIdentity, runSystemDB)
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
    executionIdentity :: ExecutionIdentity
  }

-- | A workflow's state: the id, the deadline the row carries, and the
-- identity that tells a re-run of the same id apart from the run itself.
newWorkflowState :: MonadSTM m => Text -> Maybe Timestamp -> ExecutionIdentity -> m (WorkflowState m)
newWorkflowState workflowText deadlineAt identity = do
  stepRef <- newTVarIO 0
  markerRef <- newTVarIO 0
  pure
    WorkflowState
      { workflowId = workflowText,
        deadline = deadlineAt,
        nextStepIdRef = stepRef,
        nextMarkerRef = markerRef,
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

-- | Runs a body under a context that is 'inStep': this attempt's marker
-- and status, and a fresh cancellation flag. Rebinding rather than
-- mutating — the scope lives on the context handed to the body alone, so
-- it goes out of scope with the body however the body ends. Abandoning the
-- attempt (cancellation, timeout kill, dropped future) fires the token, as
-- the oracle's drop guard does; completing it leaves the token quiet.
withAttempt :: (MonadSTM m, MonadCatch m) => Ctx m -> StepMarker -> StepStatus -> (Ctx m -> m a) -> m a
withAttempt ctx marker status body = do
  scope <- newStepScope marker status
  body ctx {ctxStep = Just scope} `onException` cancelToken scope.scopeCancellation

-- | A token that fires when the step running here is abandoned. Outside a
-- step it never fires, so a body that is also called outside a workflow
-- needs no second path.
cancellationToken :: MonadSTM m => Ctx m -> m (StrictTVar m Bool)
cancellationToken ctx = case ctx.ctxStep of
  Just scope -> pure scope.scopeCancellation
  Nothing    -> newTVarIO False

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
