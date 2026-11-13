-- | The system-database backend seam. This module defines the
-- @SystemDB@ class (Rust @trait SystemDatabase@); the client-facing
-- name 'DBOS.SystemDB' only re-exports it, so engine peers import the
-- class from here directly.
module DBOS.Internal.SystemDB.Class
  ( -- * Backend seam (trait SystemDatabase)
    SystemDB (..),
  )
where

import DBOS.Internal.Prelude
import Data.Int (Int64)
import DBOS.Internal.SystemDB.Error
  ( Error (..),
  )
import Data.Int (Int32)
import DBOS.Internal.SystemDB.Types as Types

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
  -- | The database's clock as of this call, paired with the moment the
  -- answer arrived: what a wait for a deadline the database set is measured
  -- from. Mirrors Rust @SystemDatabase::database_time@.
  databaseTime :: db -> m (Either Error DatabaseTime)
  -- | Starts a workflow row, joining the stored one under a repeated id.
  -- A debounced creation is never joined: an id that already names a row
  -- is a conflict (or the holder's key racing back to the bounce).
  -- @reusePolicy@ says what the insert does when the id already names
  -- another caller's workflow: join it, or refuse with 'WorkflowIdInUse'
  -- before writing anything. A retry that finds this call's own row after
  -- a lost commit acknowledgement is not a reuse. Mirrors Rust
  -- @SystemDatabase::init_workflow@.
  initWorkflow :: db -> NewWorkflow -> Maybe InitWorkflowCaller -> WorkflowIdReusePolicy -> m (Either Error WorkflowInitResult)
  getWorkflow :: db -> WorkflowId -> m (Either Error (Maybe WorkflowRecord))
  listWorkflows :: db -> WorkflowFilter -> Maybe (WorkflowId, Int) -> m (Either Error [WorkflowRecord])
  getWorkflowChildren :: db -> WorkflowId -> m (Either Error [WorkflowId])
  recordWorkflowOutcome :: db -> WorkflowId -> Outcome -> Text -> m (Either Error OutcomeWrite)
  awaitWorkflowResult :: (MonadDelay m, MonadTime m)
                      => db -> WorkflowId -> Duration -> Bool -> m (Either Error AwaitedOutcome)
  awaitFirstWorkflowId :: (MonadDelay m, MonadTime m)
                       => db -> [WorkflowId] -> Duration -> m (Either Error WorkflowId)
  awaitWorkflowIds :: (MonadDelay m, MonadTime m)
                   => db -> [WorkflowId] -> Duration -> m (Either Error ())
  setWorkflowDelay :: db -> WorkflowId -> WorkflowDelay -> Maybe (WorkflowId, Int) -> m (Either Error ())
  clearQueueAssignment :: db -> WorkflowId -> m (Either Error Bool)
  updateWorkflowAttributes :: db -> WorkflowId -> AttributeUpdate -> Maybe (WorkflowId, Int) -> m (Either Error ())
  reenqueueForRecovery :: db -> [Text] -> Text -> Text -> m (Either Error [WorkflowId])
  transitionDelayedWorkflows :: db -> m (Either Error Word64)
  cancelIfTimedOut :: db -> WorkflowId -> Text -> m (Either Error Bool)
  cancelTimedOutWorkflows :: db -> Int64 -> m (Either Error [WorkflowId])
  deadLetterWorkflows :: db -> [WorkflowId] -> Int64 -> m (Either Error [WorkflowId])
  -- 'fromWorkflow' names the calling workflow, when one calls: a call a
  -- workflow makes of itself is refused before anything runs. Client and
  -- engine paths pass 'Nothing'. Mirrors Rust @from_workflow@.
  -- Divergence note (2026-10-27): this refusal follows Rust (#115); the
  -- TypeScript and Python oracles allow a workflow to cancel itself. Rust
  -- wins by default where the oracles disagree, so the refusal stands.
  cancelWorkflows :: db -> [WorkflowId] -> Bool -> Maybe StepCaller -> Maybe Text -> m (Either Error [WorkflowId])
  resumeWorkflows :: db -> [WorkflowId] -> Maybe Text -> Maybe StepCaller -> Maybe Text -> m (Either Error [WorkflowId])
  -- The deleter presents its owner when it runs as a step: a stale
  -- execution deletes nothing. Client and engine paths pass 'Nothing'.
  -- One value, not a caller plus a separate token: the row refused for
  -- being the caller's own and the row the fence locks are then provably
  -- the same, and a mismatched pair cannot be written down. The optional
  -- step is the leaf rule: a delete inside a step body fences but records
  -- nothing, one the engine calls as a durable step does both.
  deleteWorkflows :: db -> [WorkflowId] -> Bool -> Maybe DeleteCaller -> m (Either Error Word64)
  -- Every management caller is a 'StepCaller': the execution, its step,
  -- and its owner token. The token fences the call before the work, so a
  -- hand-off or a cancelled caller refuses it. The caller check, the work,
  -- and the caller's step record share one transaction, and fork ids are
  -- minted before the retried region so a retry cannot fork twice.
  -- Debounce takes the same 'StepCaller' (its surface is live and follows
  -- Rust 0.6.0's owner fence, ADR-0028). Schedules still present a plain
  -- pair because that surface is deferred (ADR-0036), so they use the
  -- unfenced caller path.
  forkWorkflows :: db -> [Fork] -> ForkOptions -> Maybe StepCaller -> m (Either Error [WorkflowId])
  forkFrom :: db -> [WorkflowId] -> ForkPoint -> ForkOptions -> Maybe StepCaller -> m (Either Error [WorkflowId])
  rewindWorkflow :: db -> WorkflowId -> Int32 -> SysRewindOptions -> Maybe StepCaller -> m (Either Error ())
  -- The owner rides separately from the caller step: a workflow or step
  -- send presents it and is fenced on the sending execution; a client or
  -- engine send passes 'Nothing' and skips the fence. Mirrors Rust
  -- @send_message@'s @Option<WorkflowOwner>@.
  sendMessage :: db -> EncodedSendMessage -> Maybe Text -> Maybe (WorkflowId, Int) -> Maybe WorkflowOwner -> Bool -> m (Either Error ())
  sendMessages :: db -> [EncodedSendMessage] -> Maybe Text -> Maybe (WorkflowId, Int) -> Maybe WorkflowOwner -> Bool -> m (Either Error ())
  recv :: (MonadDelay m, MonadTime m)
       => db -> WorkflowId -> Int -> Int -> Maybe Text -> Duration -> Text -> m (Either Error (Maybe EncodedValue))
  writeStream :: db -> WorkflowId -> Int -> Text -> Text -> Maybe Text -> WrittenBy -> Text -> m (Either Error ())
  closeStream :: db -> WorkflowId -> Int -> Text -> WrittenBy -> Text -> m (Either Error ())
  close :: db -> m ()
  checkStep :: db -> WorkflowId -> Int -> Text -> m (Either Error (Maybe StepRecord))
  checkStepAtOrAfter :: db -> WorkflowId -> Int -> m (Either Error (Maybe (Int, Text)))
  recordStep :: db -> WorkflowId -> Int -> Text -> Outcome -> Maybe Text -> Maybe StepTiming -> Text -> m (Either Error ())
  listSteps :: db -> WorkflowId -> Bool -> Maybe Int64 -> Maybe Int64 -> Maybe (WorkflowId, Int) -> m (Either Error [StepRecord])
  recordSleep :: db -> WorkflowId -> Int -> Duration -> Text -> m (Either Error WakeTime)
  setEvent :: db -> WorkflowId -> Int -> Text -> Text -> Maybe Text -> Text -> m (Either Error ())
  getEvent :: (MonadDelay m, MonadTime m)
           => db -> WorkflowId -> Text -> Duration -> Maybe GetEventCaller -> m (Either Error (Maybe EncodedValue))
  getAllNotifications :: db -> WorkflowId -> m (Either Error [NotificationRecord])
  getAllEvents :: db -> WorkflowId -> m (Either Error [EventRecord])
  readStreamValues :: (MonadDelay m, MonadTime m)
                   => db -> WorkflowId -> Text -> Int -> Int -> m (Either Error StreamRead)
  awaitStreamValues :: (MonadDelay m, MonadTime m)
                    => db -> WorkflowId -> Text -> Int -> Int -> Maybe Timestamp -> Duration -> m (Either Error AwaitedStream)
  getAllStreamEntries :: db -> WorkflowId -> m (Either Error [StreamRecord])
  createApplicationVersion :: db -> Text -> Maybe Text -> m (Either Error ())
  listApplicationVersions :: db -> m (Either Error [VersionInfo])
  getLatestApplicationVersion :: db -> Maybe Text -> m (Either Error (Maybe VersionInfo))
  promoteApplicationVersion :: db -> Text -> Maybe Text -> m (Either Error ())
  -- | Whether the row was created, and the row as the write committed it.
  -- The row comes from the same transaction, so a peer's update cannot
  -- slip between the write and the receipt.
  upsertQueue :: db -> NewQueue -> OnExistingQueue -> m (Either Error (Bool, QueueRecord))
  startQueuedWorkflows :: db -> QueueRecord -> Text -> Text -> Maybe Text -> Int64 -> Int64 -> Text -> m (Either Error Dequeued)
  countPendingQueueWorkflows :: db -> Text -> m (Either Error Int64)
  getQueuePartitions :: db -> Text -> m (Either Error [Text])
  startQueuedPartitionedWorkflows :: db -> QueueRecord -> Text -> Text -> Maybe Int64 -> Text -> m (Either Error [WorkflowId])
  getQueue :: db -> Text -> m (Either Error (Maybe QueueRecord))
  listQueues :: db -> AppScope -> m (Either Error [QueueRecord])
  updateQueue :: db -> Text -> QueueUpdate -> (QueueRecord -> QueueRecord -> Either Error ()) -> m (Either Error QueueRecord)
  debounceDelayedWorkflow :: db -> DebounceRequest -> Maybe StepCaller -> m (Either Error Debounce)
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
  renameApplication :: db -> RenameFrom -> ApplicationName -> RenameBatching -> m (Either Error ApplicationRowCounts)
  recordChildWorkflow :: db -> WorkflowId -> WorkflowId -> Int -> Text -> Maybe Timestamp -> Text -> m (Either Error ())
  checkChildResult :: db -> WorkflowId -> Int -> m (Either Error (Maybe StepRecord))
  checkChildResult db parent step = checkStep db parent step getResultStepName
  recordChildResult :: db -> WorkflowId -> Int -> WorkflowId -> Outcome -> Maybe Text -> Maybe StepTiming -> Text -> m (Either Error ())
