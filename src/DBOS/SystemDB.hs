module DBOS.SystemDB
  ( -- * Pool and sessions
    Pool.Pool,
    Postgres.WorkflowStartDecision (..),
    Postgres.acquirePool,
    Postgres.releasePool,
    Postgres.runDb,
    Postgres.runDbOrFail,
    Postgres.fetchMigrationVersion,
    Postgres.fetchWorkflowExecutionRow,
    Postgres.fetchWorkflowStatus,
    Postgres.fetchOperationCheckpoint,
    Postgres.fetchNotification,
    Postgres.tryStartWorkflow,
    Postgres.updateWorkflowOutcome,
    Postgres.recordOperationOutput,
    Postgres.recordOperationError,
    Postgres.recordSleep,
    Postgres.setEvent,
    Postgres.getEvent,
    Postgres.getEventBlocking,
    Postgres.postgresEventStore,
    Postgres.postgresStepStore,
    Postgres.sendMessage,
    Postgres.sendMessages,
    Postgres.recvMessage,
    Postgres.listWorkflowIdsByName,
    Postgres.registerQueue,
    Postgres.releaseWorkflowClaim,
    Postgres.fetchQueueWorkerConcurrency,
    Postgres.updateQueueWorkerConcurrency,
    Postgres.enqueueWorkflow,
    Postgres.dequeueWorkflows,
    Postgres.fetchWorkflowStatuses,
    Postgres.reenqueueForRecovery,
    -- * Domain types (types.rs)
    module Types,
    -- * Error channel (error.rs)
    Error (..),
    BackendError (..),
    BackendErrorKind (..),
    invalidInput,
    renderError,
    renderBackendError,
    -- * Retry (retry.rs)
    RetryPolicy (..),
    defaultRetryPolicy,
    shouldRetry,
    jitter,
    withRetry,
    uuidEntropy,
  )
where

import DBOS.SystemDB.Error
  ( BackendError (..),
    BackendErrorKind (..),
    Error (..),
    invalidInput,
    renderBackendError,
    renderError,
  )
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.SystemDB.Retry
  ( RetryPolicy (..),
    defaultRetryPolicy,
    jitter,
    shouldRetry,
    uuidEntropy,
    withRetry,
  )
import DBOS.SystemDB.Types as Types
import Hasql.Pool qualified as Pool
