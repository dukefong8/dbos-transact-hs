-- | The system-database seam: public client surface. The class lives
-- in 'DBOS.Internal.SystemDB.Class' and the domain modules beside it; this module
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
    renderBackendError,
    shouldRecord,
    -- * Retry (retry.rs)
    RetryPolicy (..),
    defaultRetryPolicy,
    withoutConflictReplay,
    shouldRetry,
    jitter,
    withRetry,
    uuidEntropy,
    -- * Wakeups (notify.rs)
    module DBOS.Internal.SystemDB.Notify,
    -- * Postgres notifier (postgres/notifier.rs)
    module DBOS.Internal.SystemDB.Postgres.Notifier,
  )
where

import DBOS.Internal.SystemDB.Class
import DBOS.Internal.SystemDB.Notify
import DBOS.Internal.SystemDB.Postgres.Notifier
import DBOS.Internal.SystemDB.Error
  ( BackendError (..),
    BackendErrorKind (..),
    Error (..),
    invalidInput,
    renderBackendError,
    renderError,
    shouldRecord,
  )
import DBOS.Internal.SystemDB.Retry
  ( RetryPolicy (..),
    defaultRetryPolicy,
    jitter,
    shouldRetry,
    uuidEntropy,
    withRetry,
    withoutConflictReplay,
  )
import DBOS.Internal.SystemDB.Types as Types
