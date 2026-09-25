module DBOS.SystemDB
  ( -- * Backend seam (trait SystemDatabase)
    SystemDB (..),
    -- * Domain types (types.rs)
    module Types,
    -- * Error channel (error.rs)
    Error (..),
    BackendError (..),
    BackendErrorKind (..),
    invalidInput,
    renderError,
    sleepStepName,
    renderBackendError,
    -- * Retry (retry.rs)
    RetryPolicy (..),
    defaultRetryPolicy,
    shouldRetry,
    jitter,
    withRetry,
    uuidEntropy,
    -- * Wakeups (notify.rs)
    module DBOS.SystemDB.Notify,
    -- * Postgres notifier (postgres/notifier.rs)
    module DBOS.SystemDB.Postgres.Notifier,
  )
where

import DBOS.Prelude
import Control.Monad.Class.MonadTime (MonadTime)
import Control.Monad.Class.MonadTimer (MonadDelay)
import Data.Int (Int64)
import Data.Text (Text)
import Data.Word (Word64)
import DBOS.SystemDB.Notify
import DBOS.SystemDB.Postgres.Notifier
import DBOS.SystemDB.Error
  ( BackendError (..),
    BackendErrorKind (..),
    Error (..),
    invalidInput,
    renderBackendError,
    renderError,
  )
import DBOS.SystemDB.Retry
  ( RetryPolicy (..),
    defaultRetryPolicy,
    jitter,
    shouldRetry,
    uuidEntropy,
    withRetry,
  )
import DBOS.SystemDB.Types as Types

-- | The system database backend both the engine and the tests program
-- against. Mirrors Rust @trait SystemDatabase@ (the Rust spelling is kept
-- for the reference; the Haskell class keeps the established @SystemDB@
-- spelling): one method per trait item, snake_case folded to camelCase,
-- @&str@ as 'Text' ('WorkflowId' where the value is a workflow id),
-- @i32@\/@i64@\/@u64@ as 'Int'\/'Int64'\/'Word64', @Result<_, Error>@ as
-- @m (Either Error _)@ per Rule 3 — no @MonadThrow@ for domain errors.
--
-- No method mentions a driver type: every method takes the backend handle
-- explicitly as its first argument (@db@), exactly as the oracle's trait
-- owns its pool privately — there is no @ReaderT@ carrier. @db@ is the
-- handle type and @m@ is the effect monad, so the same methods run under
-- @IO@ in production and under @IOSim@ in tests. @close@ keeps the Rust
-- name. @checkChildResult@ is provided, not required: the oracle provides
-- it as @check_step@ under the stored @GET_RESULT@ contract, so a backend
-- has nothing of its own to add. The waits carry their own
-- 'MonadDelay'\/'MonadTime' constraints so the shared polling helpers run
-- under @IO@ and under @IOSim@ alike.
--
-- NOTE (ADR-0009, amended 2026-09-25): this class supersedes the plan's
-- record-only seam; the explicit @db@ argument supersedes the @ReaderT@
-- carrier. The class is a backend seam whose handle travels by argument,
-- not a monad transformer over the connection.
class Monad m => SystemDB db m where
  initWorkflow :: db -> NewWorkflow -> Maybe Int64 -> Submission -> Maybe InitWorkflowCaller -> m (Either Error WorkflowInitResult)
  getWorkflow :: db -> WorkflowId -> m (Either Error (Maybe WorkflowRecord))
  listWorkflows :: db -> WorkflowFilter -> Maybe (WorkflowId, Int) -> m (Either Error [WorkflowRecord])
  getWorkflowChildren :: db -> WorkflowId -> m (Either Error [WorkflowId])
  recordWorkflowOutcome :: db -> WorkflowId -> Outcome -> m (Either Error OutcomeWrite)
  awaitWorkflowResult :: (MonadDelay m, MonadTime m) => db -> WorkflowId -> Duration -> Bool -> m (Either Error AwaitedOutcome)
  awaitFirstWorkflowId :: (MonadDelay m, MonadTime m) => db -> [WorkflowId] -> Duration -> m (Either Error WorkflowId)
  awaitWorkflowIds :: (MonadDelay m, MonadTime m) => db -> [WorkflowId] -> Duration -> m (Either Error ())
  setWorkflowDelay :: db -> WorkflowId -> WorkflowDelay -> Maybe (WorkflowId, Int) -> m (Either Error ())
  clearQueueAssignment :: db -> WorkflowId -> m (Either Error Bool)
  updateWorkflowAttributes :: db -> WorkflowId -> Maybe Text -> Maybe (WorkflowId, Int) -> m (Either Error ())
  reenqueueForRecovery :: db -> [Text] -> Text -> Text -> m (Either Error [WorkflowId])
  transitionDelayedWorkflows :: db -> m (Either Error Word64)
  cancelWorkflows :: db -> [WorkflowId] -> Bool -> Maybe (WorkflowId, Int) -> m (Either Error [WorkflowId])
  resumeWorkflows :: db -> [WorkflowId] -> Maybe Text -> Maybe (WorkflowId, Int) -> m (Either Error [WorkflowId])
  deleteWorkflows :: db -> [WorkflowId] -> Bool -> Maybe (WorkflowId, Int) -> m (Either Error Word64)
  forkWorkflows :: db -> [Fork] -> ForkOptions -> Maybe (WorkflowId, Int) -> m (Either Error [WorkflowId])
  forkFrom :: db -> [WorkflowId] -> ForkPoint -> ForkOptions -> Maybe (WorkflowId, Int) -> m (Either Error [WorkflowId])
  sendMessage :: db -> SendMessage -> Maybe Text -> Maybe (WorkflowId, Int) -> Bool -> m (Either Error ())
  sendMessages :: db -> [SendMessage] -> Maybe Text -> Maybe (WorkflowId, Int) -> Bool -> m (Either Error ())
  recv :: (MonadDelay m, MonadTime m) => db -> WorkflowId -> Int -> Int -> Maybe Text -> Duration -> m (Either Error (Maybe EncodedValue))
  writeStream :: db -> WorkflowId -> Int -> Text -> Text -> Maybe Text -> WrittenBy -> m (Either Error ())
  closeStream :: db -> WorkflowId -> Int -> Text -> m (Either Error ())
  close :: db -> m ()
  checkStep :: db -> WorkflowId -> Int -> Text -> m (Either Error (Maybe StepRecord))
  recordStep :: db -> WorkflowId -> Int -> Text -> Outcome -> Maybe Text -> Maybe StepTiming -> m (Either Error ())
  listWorkflowSteps :: db -> WorkflowId -> Bool -> Maybe Int64 -> Maybe Int64 -> Maybe (WorkflowId, Int) -> m (Either Error [StepRecord])
  recordSleep :: db -> WorkflowId -> Int -> Duration -> m (Either Error Timestamp)
  setEvent :: db -> WorkflowId -> Int -> Text -> Text -> Maybe Text -> m (Either Error ())
  getEvent :: (MonadDelay m, MonadTime m) => db -> WorkflowId -> Text -> Duration -> Maybe GetEventCaller -> m (Either Error (Maybe EncodedValue))
  getAllNotifications :: db -> WorkflowId -> m (Either Error [NotificationRecord])
  getAllEvents :: db -> WorkflowId -> m (Either Error [EventRecord])
  readStreamValue :: (MonadDelay m, MonadTime m) => db -> WorkflowId -> Text -> Int -> m (Either Error StreamRead)
  getAllStreamEntries :: db -> WorkflowId -> m (Either Error [StreamRecord])
  createApplicationVersion :: db -> Text -> Maybe Text -> m (Either Error ())
  listApplicationVersions :: db -> m (Either Error [VersionInfo])
  getLatestApplicationVersion :: db -> Maybe Text -> m (Either Error (Maybe VersionInfo))
  updateApplicationVersionTimestamp :: db -> Text -> Timestamp -> Maybe Text -> m (Either Error ())
  upsertQueue :: db -> NewQueue -> OnExistingQueue -> m (Either Error Bool)
  startQueuedWorkflows :: db -> QueueRecord -> Text -> Text -> Maybe Text -> Int64 -> Int64 -> m (Either Error [WorkflowId])
  getQueuePartitions :: db -> Text -> m (Either Error [Text])
  startQueuedPartitionedWorkflows :: db -> QueueRecord -> Text -> Text -> Maybe Int64 -> m (Either Error [WorkflowId])
  getQueue :: db -> Text -> m (Either Error (Maybe QueueRecord))
  listQueues :: db -> Applications -> m (Either Error [QueueRecord])
  updateQueue :: db -> Text -> QueueUpdate -> (QueueRecord -> QueueRecord -> Either Error ()) -> m (Either Error QueueRecord)
  debounceDelayedWorkflow :: db -> DebounceRequest -> Maybe (WorkflowId, Int) -> m (Either Error Debounce)
  getDeduplicationKeyHolder :: db -> Text -> Text -> m (Either Error (Maybe WorkflowId))
  deleteQueue :: db -> Text -> m (Either Error ())
  createSchedule :: db -> NewSchedule -> Maybe (WorkflowId, Int) -> m (Either Error ())
  upsertSchedule :: db -> NewSchedule -> Maybe (WorkflowId, Int) -> m (Either Error ())
  applySchedules :: db -> [NewSchedule] -> m (Either Error ())
  getSchedule :: db -> Text -> Maybe (WorkflowId, Int) -> m (Either Error (Maybe ScheduleRecord))
  listSchedules :: db -> ScheduleFilter -> Maybe (WorkflowId, Int) -> m (Either Error [ScheduleRecord])
  updateSchedule :: db -> Text -> ScheduleUpdate -> Maybe (WorkflowId, Int) -> m (Either Error ())
  setScheduleStatus :: db -> Text -> ScheduleStatus -> Maybe (WorkflowId, Int) -> m (Either Error ())
  updateScheduleLastFiredAt :: db -> Text -> Timestamp -> m (Either Error ())
  deleteSchedule :: db -> Text -> Maybe (WorkflowId, Int) -> m (Either Error ())
  renameApplication :: db -> RenameFrom -> Text -> RenameBatching -> m (Either Error ApplicationRowCounts)
  recordChildWorkflow :: db -> WorkflowId -> WorkflowId -> Int -> Text -> Maybe Timestamp -> m (Either Error ())
  checkChildResult :: db -> WorkflowId -> Int -> m (Either Error (Maybe StepRecord))
  checkChildResult db parent step = checkStep db parent step getResultStepName
  recordChildResult :: db -> WorkflowId -> Int -> WorkflowId -> Outcome -> Maybe Text -> Maybe StepTiming -> m (Either Error ())
