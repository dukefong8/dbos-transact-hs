{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | Domain-event tracing over 'Control.Tracer.Tracer', with one backend
-- per runtime: production logs through FastLogger (the Rank-N shape — one
-- tracer value serves every 'ToLogStr' event type), simulations trace the
-- structured event through io-sim's @traceM@ and assert with
-- @selectTraceEventsDynamic@. Records hold one 'SomeTracer' — a GADT over
-- the Rank-N shape both backends share — so leaf functions stay agnostic
-- of the backend and of each other's event types: a general tracer zooms
-- to a domain event with 'contramap', exactly as @contra-tracer@
-- documents.
--
-- Rule 4: plain Haskell, no Bluefin imports. Rule 5: the tracer is passed
-- explicitly, never ambient. No @co-log@ import anywhere in this module.
module DBOS.Tracer
  ( -- * Universal carrier
    SomeTracer (..),
    traceWith,
    projectTracer,
    nullTracer,
    ioTracer,
    simTracer,

    -- * Narrow building blocks
    Tracer,
    mkTracer,
    contramap,
    TimedFastLogger,
    acquireFastBackend,
    fastLoggerTracer,

    -- * Events
    LogSeverity (..),
    showSeverity,
    LogEvent (..),
    EngineEvent (..),
    SysdbEvent (..),
    WorkflowEvent (..),
    QueueEvent (..),
    ManagementEvent (..),
  )
where

import Control.Monad.IOSim (IOSim, traceM)
import Control.Tracer (Tracer, contramap, mkTracer)
import Control.Tracer qualified as CT
import DBOS.Prelude
import Data.Text (Text, pack)
import Data.Typeable (Typeable)
import Data.Word (Word64)
import System.Log.FastLogger (LogType' (..), TimedFastLogger, ToLogStr (..), defaultBufSize, newTimeCache, newTimedFastLogger)

-- | Severity as data: which tag the rendered line carries. Mirrors the
-- four co-log levels the engine's call sites used, without the library.
data LogSeverity
  = SeverityDebug
  | SeverityInfo
  | SeverityWarning
  | SeverityError
  deriving stock (Eq, Show)

-- | The tag a line carries: @[Debug]@, @[Info]@, @[Warning]@, @[Error]@.
showSeverity :: LogSeverity -> Text
showSeverity SeverityDebug   = "[Debug]"
showSeverity SeverityInfo    = "[Info]"
showSeverity SeverityWarning = "[Warning]"
showSeverity SeverityError   = "[Error]"

-- | An event knows its severity and its line. The FastLogger backend
-- consumes events through 'ToLogStr', which delegates to this class, so
-- the severity rendering lives in exactly one place.
class LogEvent e where
  eventSeverity :: e -> LogSeverity
  renderEvent :: e -> Text

-- | Acquire the production backend: a one-second-cached timed stdout
-- logger plus its flush-and-release cleanup. The cleanup is the second
-- half of the pair, so launch brackets acquisition against shutdown with
-- no call-site changes.
acquireFastBackend :: IO (TimedFastLogger, IO ())
acquireFastBackend = do
  getTime <- newTimeCache "%Y-%m-%dT%H:%M:%S%z"
  newTimedFastLogger getTime (LogStdout defaultBufSize)

-- | The production tracer over any 'ToLogStr' event: the Rank-N shape —
-- one value serves every event type, each line carrying FastLogger's
-- cached timestamp plus the event's own rendering.
fastLoggerTracer :: ToLogStr e => TimedFastLogger -> Tracer IO e
fastLoggerTracer logger = mkTracer emit
  where
    emit event = logger (\time -> toLogStr time <> " " <> toLogStr event <> "\n")

-- | A backend tracer for any domain event: the Rank-N shape both
-- backends already have (FastLogger is polymorphic over 'ToLogStr',
-- io-sim's @traceM@ over 'Typeable'), wrapped so records hold one field
-- instead of one per domain. The GADT keeps the @forall@ on the
-- constructor, so holders need no Rank-N field types and — under
-- 'NoFieldSelectors' — callers consume it only through 'traceWith' and
-- 'projectTracer', never record-dot. Mirrors the 'SomeSystemDB'
-- existential one module over: the handle type is universal, so a record
-- does not name the backend and tests may substitute their own.
data SomeTracer m where
  SomeTracer :: (forall e. (LogEvent e, ToLogStr e, Typeable e) => Tracer m e) -> SomeTracer m

-- | Emit any domain event through a universal carrier: the sole
-- pattern-match site, so the existential is unpacked in exactly one
-- place, like 'runSystemDB' for the backend.
traceWith :: (LogEvent e, ToLogStr e, Typeable e, Monad m) => SomeTracer m -> e -> m ()
traceWith (SomeTracer narrow) event = CT.traceWith narrow event

-- | Project one typed view out of a universal carrier: the narrow
-- 'contra-tracer' rule (programs take as specific a tracer as possible)
-- at a seam entry, over a record that stores the general one.
projectTracer :: (LogEvent e, ToLogStr e, Typeable e) => SomeTracer m -> Tracer m e
projectTracer (SomeTracer narrow) = narrow

-- | A carrier that discards everything, in any event type and monad.
-- Used by clients (which run nothing) and as the default where no launch
-- has installed a real backend.
nullTracer :: Monad m => SomeTracer m
nullTracer = SomeTracer CT.nullTracer

-- | The production carrier over a launch's FastLogger backend: one value
-- serves every event type, each line carrying the cached timestamp plus
-- the event's own rendering.
ioTracer :: TimedFastLogger -> SomeTracer IO
ioTracer logger = SomeTracer (fastLoggerTracer logger)

-- | The simulation carrier traces the structured event itself through
-- io-sim's @traceM@, so simulation runs recover their traces by type
-- with 'selectTraceEventsDynamic' instead of matching strings.
simTracer :: SomeTracer (IOSim s)
simTracer = SomeTracer (mkTracer traceM)

-- | Engine-lifecycle events: instance launch and shutdown, client
-- connect and close, registry. Mirrors the @instance.rs@, @connection.rs@,
-- @client.rs@ and @registry.rs@ @tracing!@ calls; span fields ride on the
-- constructors so FastLogger lines carry the same @key=value@ pairs.
data EngineEvent
  = EngineLaunched { engineAppName :: Text, engineExecutorId :: Text, engineAppVersion :: Text }
  | EngineShutdown { engineShutdownAppName :: Text }
  | EngineVersionStale { engineVersion :: Text, engineLatestVersion :: Text }
  | EngineNoWorkflows
  | EngineRecovered { engineRecoveredCount :: Int }
  | EngineCancelledRunning { engineCancelledCount :: Int }
  deriving stock (Eq, Show)

instance LogEvent EngineEvent where
  eventSeverity EngineLaunched {} = SeverityInfo
  eventSeverity EngineShutdown {} = SeverityInfo
  eventSeverity EngineVersionStale {} = SeverityWarning
  eventSeverity EngineNoWorkflows = SeverityWarning
  eventSeverity (EngineRecovered 0) = SeverityDebug
  eventSeverity EngineRecovered {} = SeverityInfo
  eventSeverity EngineCancelledRunning {} = SeverityInfo
  renderEvent (EngineLaunched app exec ver) =
    "DBOS launched app_name=" <> app <> " executor_id=" <> exec <> " app_version=" <> ver
  renderEvent (EngineShutdown app) = "DBOS shut down app_name=" <> app
  renderEvent (EngineVersionStale ver latest) =
    "this executor is not running the latest registered application version: it will "
      <> "recover and dequeue only work stamped with its own version app_version="
      <> ver
      <> " latest_version="
      <> latest
  renderEvent EngineNoWorkflows =
    "no workflows are registered: this executor will recover nothing and dequeue nothing. Register before calling `launch`"
  renderEvent (EngineRecovered 0) =
    "no workflows to recover"
  renderEvent (EngineRecovered count) =
    "re-enqueued workflows a previous run left PENDING workflows=" <> showText count
  renderEvent (EngineCancelledRunning count) =
    "cancelled workflows still running; they stay PENDING cancelled=" <> showText count

instance ToLogStr EngineEvent where
  toLogStr event = toLogStr (showSeverity (eventSeverity event) <> " " <> renderEvent event)

-- | System-database events: retry attempts, backend warnings, notifier
-- lifecycle. Mirrors @sysdb/retry.rs@, the @postgres@ backend and
-- @sysdb/postgres/notifier.rs@.
data SysdbEvent
  = SysdbRetryAttempt { sysdbOperation :: Text, sysdbAttempt :: Integer, sysdbDelayMs :: Integer, sysdbDetail :: Text }
  | SysdbUnexpectedChannel { sysdbChannel :: Text }
  | SysdbQueueMismatch { sysdbWorkflowId :: Text }
  | SysdbNotifierStopped
  | SysdbPushFailed { sysdbPushChannel :: Text, sysdbPushCount :: Int, sysdbPushDetail :: Text }
  deriving stock (Eq, Show)

instance LogEvent SysdbEvent where
  eventSeverity SysdbRetryAttempt {}    = SeverityWarning
  eventSeverity SysdbUnexpectedChannel {} = SeverityWarning
  eventSeverity SysdbQueueMismatch {}     = SeverityWarning
  eventSeverity SysdbNotifierStopped    = SeverityDebug
  eventSeverity SysdbPushFailed {}      = SeverityWarning
  renderEvent (SysdbRetryAttempt operation attempt delayMs detail) =
    "system database operation failed; retrying operation="
      <> operation
      <> " attempt="
      <> showText attempt
      <> " delay_ms="
      <> showText delayMs
      <> " error="
      <> detail
  renderEvent (SysdbUnexpectedChannel channel) =
    "signalled on an unexpected channel channel=" <> channel
  renderEvent (SysdbQueueMismatch workflowId) =
    "workflow " <> workflowId <> " already exists on a different queue; the stored queue is kept"
  renderEvent SysdbNotifierStopped = "the notifier stopped"
  renderEvent (SysdbPushFailed channel count detail) =
    "could not push notifications; readers fall back to re-querying channel="
      <> channel
      <> " count="
      <> showText count
      <> " error="
      <> detail

instance ToLogStr SysdbEvent where
  toLogStr event = toLogStr (showSeverity (eventSeverity event) <> " " <> renderEvent event)

-- | Step-run events: the announcements 'runWorkflowStep' makes when it
-- runs a body versus replays its checkpoint. Mirrors @step.rs@.
data WorkflowEvent
  = StepRunning { workflowStepName :: Text, workflowStepId :: Int }
  | StepReplaying { workflowStepName :: Text, workflowStepId :: Int }
  deriving stock (Eq, Show)

instance LogEvent WorkflowEvent where
  eventSeverity StepRunning {}   = SeverityDebug
  eventSeverity StepReplaying {} = SeverityDebug
  renderEvent (StepRunning name stepId') = "running step " <> name <> " (" <> showText stepId' <> ")"
  renderEvent (StepReplaying name stepId') = "replaying recorded step " <> name <> " (" <> showText stepId' <> ")"

instance ToLogStr WorkflowEvent where
  toLogStr event = toLogStr (showSeverity (eventSeverity event) <> " " <> renderEvent event)

-- | Queue-worker events: the dequeue sweep, worker lifecycle, and
-- delayed-transition lifecycle. The rendered lines keep the legacy
-- message bodies, so operators see the same text with the severity now
-- carried by the event instead of the call site. Mirrors @dequeue.rs@.
data QueueEvent
  = QueueWorkerStopping { queueWorkerName :: Text }
  | DequeueBackoff
  | DequeueFailed { queueDetail :: Text }
  | ClaimedWorkflowsUnreadable { queueDetail :: Text }
  | ClaimedWorkflowsMissing { queueClaimed :: Int, queueFound :: Int }
  | DequeuedWorkflowFailed { queueDetail :: Text }
  | DequeuedRowSkipped { queueWorkflowId :: Text }
  | QueueListFailed { queueDetail :: Text }
  | InternalQueueLimitsIgnored
  | DelayedTransitionFailed { queueDetail :: Text }
  | DelayedWorkflowsEnqueued { queueMoved :: Word64 }
  deriving stock (Eq, Show)

instance LogEvent QueueEvent where
  eventSeverity QueueWorkerStopping {}      = SeverityInfo
  eventSeverity DequeueBackoff              = SeverityDebug
  eventSeverity DequeueFailed {}            = SeverityWarning
  eventSeverity ClaimedWorkflowsUnreadable {} = SeverityWarning
  eventSeverity ClaimedWorkflowsMissing {}  = SeverityWarning
  eventSeverity DequeuedWorkflowFailed {}   = SeverityWarning
  eventSeverity DequeuedRowSkipped {}       = SeverityWarning
  eventSeverity QueueListFailed {}          = SeverityWarning
  eventSeverity InternalQueueLimitsIgnored  = SeverityWarning
  eventSeverity DelayedTransitionFailed {}  = SeverityWarning
  eventSeverity DelayedWorkflowsEnqueued {} = SeverityDebug

  renderEvent (QueueWorkerStopping queue) = "the queue " <> queue <> " is no longer registered; stopping its worker"
  renderEvent DequeueBackoff = "a peer is mid-dequeue; backing off"
  renderEvent (DequeueFailed detail) = "could not dequeue from the queue: " <> detail
  renderEvent (ClaimedWorkflowsUnreadable detail) =
    "could not read the claimed workflows; they stay PENDING for recovery: " <> detail
  renderEvent (ClaimedWorkflowsMissing claimed found) =
    "some claimed workflows have no row: claimed " <> showText claimed <> ", found " <> showText found
  renderEvent (DequeuedWorkflowFailed detail) =
    "could not start the dequeued workflow; it stays PENDING for recovery: " <> detail
  renderEvent (DequeuedRowSkipped workflowText) =
    "the dequeued row " <> workflowText <> " names no workflow; skipped"
  renderEvent (QueueListFailed detail) = "could not list queues; keeping the current set: " <> detail
  renderEvent InternalQueueLimitsIgnored =
    "the queues table holds a row for the engine's internal queue; its stored limits are ignored. Delete the row: it can only throttle `resume` and `fork`"
  renderEvent (DelayedTransitionFailed detail) = "could not transition delayed workflows: " <> detail
  renderEvent (DelayedWorkflowsEnqueued moved) = "delayed workflows are now enqueued: " <> showText moved

instance ToLogStr QueueEvent where
  toLogStr event = toLogStr (showSeverity (eventSeverity event) <> " " <> renderEvent event)

-- | Operator-action events: the management surface's announcements.
-- Rendered lines keep the Rust @tracing!@ message bodies with their
-- @key=value@ span fields, so operators see the same text. Mirrors
-- @management.rs@'s @Connection@ impl: cancel guards on non-empty, delete
-- on a positive count, fork splits one id from many, the rest announce
-- unconditionally; the reads log nothing.
data ManagementEvent
  = WorkflowsCancelled { managementCancelled :: Int }
  | WorkflowsResumed { managementRequested :: Int, managementResumed :: Int }
  | WorkflowForked { managementForkedId :: Text }
  | WorkflowsForked { managementForked :: Int }
  | WorkflowsDeleted { managementDeleted :: Word64 }
  | WorkflowDelayMoveAsked { managementWorkflowId :: Text }
  | WorkflowAttributesReplaceAsked { managementWorkflowId :: Text }
  deriving stock (Eq, Show)

instance LogEvent ManagementEvent where
  eventSeverity WorkflowsCancelled {}         = SeverityInfo
  eventSeverity WorkflowsResumed {}           = SeverityInfo
  eventSeverity WorkflowForked {}             = SeverityInfo
  eventSeverity WorkflowsForked {}            = SeverityInfo
  eventSeverity WorkflowsDeleted {}           = SeverityInfo
  eventSeverity WorkflowDelayMoveAsked {}     = SeverityInfo
  eventSeverity WorkflowAttributesReplaceAsked {} = SeverityInfo
  renderEvent (WorkflowsCancelled cancelled) =
    "cancelled workflows cancelled=" <> showText cancelled
  renderEvent (WorkflowsResumed requested resumed) =
    "resumed workflows onto their queues requested=" <> showText requested <> " resumed=" <> showText resumed
  renderEvent (WorkflowForked forkedId) =
    "forked the workflow onto its queue forked_id=" <> forkedId
  renderEvent (WorkflowsForked count) =
    "forked workflows onto their queues count=" <> showText count
  renderEvent (WorkflowsDeleted deleted) =
    "deleted workflows deleted=" <> showText deleted
  renderEvent (WorkflowDelayMoveAsked workflowId) =
    "asked to move the workflow's release time workflow_id=" <> workflowId
  renderEvent (WorkflowAttributesReplaceAsked workflowId) =
    "asked to replace the workflow's attributes workflow_id=" <> workflowId

instance ToLogStr ManagementEvent where
  toLogStr event = toLogStr (showSeverity (eventSeverity event) <> " " <> renderEvent event)

-- | Render any 'Show' value as 'Text'. Local, as in @Dequeue@: the port
-- has no shared text-rendering home.
showText :: Show a => a -> Text
showText = pack . show
