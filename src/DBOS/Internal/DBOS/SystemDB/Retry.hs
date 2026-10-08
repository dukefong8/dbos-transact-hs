{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | Retrying system database operations that failed for reasons that may
-- pass. Mirrors Rust @sysdb::retry@: classification lives with the backend
-- in 'BackendErrorKind', and this module only acts on the verdict.
--
-- The loop is monad-polymorphic — 'MonadDelay' for the wait, plus the
-- tracer and the entropy provider as parameters — so the same code runs
-- under 'IO' in production and under @IOSim@ in tests, where time is
-- virtual and the entropy word is fixed. It imports only its sibling
-- modules, so attempts are announced as 'SysdbEvent' through the caller's
-- carrier rather than the engine's message type.
module DBOS.SystemDB.Retry
  ( RetryPolicy (..),
    defaultRetryPolicy,
    shouldRetry,
    jitter,
    withRetry,
    uuidEntropy,
    SysdbEvent (..),
  )
where

import DBOS.Prelude
import Data.Text (Text)
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Data.Word (Word32)
import DBOS.SystemDB.Error (BackendError (..), BackendErrorKind (..), Error (..), renderError)
import DBOS.SystemDB.Types (Duration (..), durationAsMillis, secondsDuration)
import DBOS.Transact.Logger (LogEvent (..), LogSeverity (..), SomeTracer, runTracer)
import System.Log.FastLogger (ToLogStr (..))

-- | System-database events: retry attempts, backend warnings, notifier
-- lifecycle. Mirrors @sysdb/retry.rs@, the @postgres@ backend and
-- @sysdb/postgres/notifier.rs@. Owned here — the leaf every emitter
-- already imports — so the backend and the notifier announce through it
-- without a cycle.
data SysdbEvent
  = SysdbRetryAttempt { sysdbOperation :: Text, sysdbAttempt :: Integer, sysdbDelayMs :: Integer, sysdbDetail :: Text }
  | SysdbUnexpectedChannel { sysdbChannel :: Text }
  | SysdbQueueMismatch { sysdbWorkflowId :: Text }
  | SysdbNotifierStopped
  | SysdbPushFailed { sysdbPushChannel :: Text, sysdbPushCount :: Int, sysdbPushDetail :: Text }
  deriving stock (Eq, Show)

instance LogEvent SysdbEvent where
  eventSeverity SysdbRetryAttempt {}      = SeverityWarning
  eventSeverity SysdbUnexpectedChannel {} = SeverityWarning
  eventSeverity SysdbQueueMismatch {}     = SeverityWarning
  eventSeverity SysdbNotifierStopped      = SeverityDebug
  eventSeverity SysdbPushFailed {}        = SeverityWarning
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
  toLogStr = toLogStr . renderLine

-- | How long to wait between attempts, and what to give up on. The
-- defaults are Python's and Java's, which agree: one second, doubling to a
-- minute, no attempt limit, connection failures blocked on rather than
-- reported.
data RetryPolicy = RetryPolicy
  { retryPolicyInitialBackoff        :: Duration,
    retryPolicyMaxBackoff            :: Duration,
    retryPolicyRetryConnectionErrors :: Bool
  }
  deriving stock (Eq, Show)

-- | The shared defaults.
defaultRetryPolicy :: RetryPolicy
defaultRetryPolicy =
  RetryPolicy
    { retryPolicyInitialBackoff = secondsDuration 1,
      retryPolicyMaxBackoff = secondsDuration 60,
      retryPolicyRetryConnectionErrors = True
    }

-- | Whether this failure should be retried. Transient contention is
-- always retried, whatever the policy says; a connection failure is
-- retried unless the policy opts out; everything else — including answers
-- like 'ConflictingWorkflow' and 'MaxRecoveryAttemptsExceeded', which
-- would loop forever — is returned at once.
shouldRetry :: RetryPolicy -> Error -> Bool
shouldRetry policy err =
  case err of
    Backend backend -> case backend.backendKind of
      Transient  -> True
      Connection -> policy.retryPolicyRetryConnectionErrors
      Permanent  -> False
    _ -> False

-- | Spreads the backoff over @[0.5, 1.5)@ of its nominal value, from the
-- given entropy word: without it every process that lost the same database
-- comes back at the same instant. Pure, so a test can pin it; production
-- feeds it 'uuidEntropy'.
jitter :: Word32 -> Duration -> Duration
jitter bits (Duration backoff) =
  Duration (backoff * (0.5 + fromIntegral bits / 4294967296))

-- | Runs @work@ until it succeeds or fails for a reason that will not
-- pass. There is no attempt limit, matching every other implementation:
-- the bound in practice is the caller. The retried region runs more than
-- once, so nothing that identifies this attempt may be generated inside
-- it.
withRetry :: MonadDelay m
          => RetryPolicy ->
             Text ->
             SomeTracer m ->
             m Word32 ->
             m (Either Error a) ->
             m (Either Error a)
withRetry policy operation tracer nextEntropy work = go policy.retryPolicyInitialBackoff (0 :: Integer)
  where
    go backoff attempt = do
      result <- work
      case result of
        Right value -> pure (Right value)
        Left err
          | not (shouldRetry policy err) -> pure (Left err)
          | otherwise -> do
              bits <- nextEntropy
              let delay = jitter bits backoff
                  delayMs = durationAsMillis delay
                  nextAttempt = attempt + 1
              runTracer tracer (SysdbRetryAttempt operation nextAttempt delayMs (renderError err))
              threadDelay (fromInteger (delayMs * 1000))
              go (min (double backoff) policy.retryPolicyMaxBackoff) nextAttempt
    double (Duration backoff) = Duration (backoff * 2)

-- | Production entropy for 'jitter': the first word of a fresh v4 UUID,
-- the same source the oracle draws from.
uuidEntropy :: IO Word32
uuidEntropy = do
  uuid <- UUID.V4.nextRandom
  let (first, _, _, _) = UUID.toWords uuid
  pure first
