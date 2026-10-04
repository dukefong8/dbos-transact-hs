-- | The system-database seam: public client surface. The class lives
-- in 'DBOS.SystemDB.Class' and the domain modules beside it; this module
-- only re-exports them, so engine peers import the defining modules
-- directly and clients keep the stable name.
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

import DBOS.SystemDB.Class
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
