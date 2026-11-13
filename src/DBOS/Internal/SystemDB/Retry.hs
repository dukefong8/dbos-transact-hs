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
module DBOS.Internal.SystemDB.Retry
  ( RetryPolicy (..),
    defaultRetryPolicy,
    withoutConflictReplay,
    shouldRetry,
    jitter,
    withRetry,
    uuidEntropy,
    SysdbEvent (..),
  )
where

import DBOS.Internal.Prelude
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.Internal.SystemDB.Error (BackendError (..), BackendErrorKind (..), Error (..), renderError)
import DBOS.Internal.SystemDB.Types (Duration, durationAsMillis, millisDuration, scaleDuration, secondsDuration)
import DBOS.Internal.Transact.Logger (LogEvent (..), LogSeverity (..), SomeTracer, runTracer)
import System.Log.FastLogger (ToLogStr (..))

-- | System-database events: retry attempts, backend warnings, notifier
-- lifecycle. Mirrors @sysdb/retry.rs@, the @postgres@ backend and
-- @sysdb/postgres/notifier.rs@. Owned here — the leaf every emitter
-- already imports — so the backend and the notifier announce through it
-- without a cycle.
data SysdbEvent
  = SysdbRetryAttempt { sysdbOperation :: Text, sysdbAttempt :: !Integer, sysdbDelayMs :: Integer, sysdbDetail :: Text }
  | -- | A lost transaction conflict being replayed. Below the ceiling this
    -- is ordinary contention and reads at DEBUG; at the ceiling the call
    -- has been losing for seconds, so it reads at WARN like a connection
    -- retry. Mirrors the oracle's conflict branch.
    SysdbRetryConflict { sysdbOperation :: Text, sysdbAttempt :: !Integer, sysdbDelayMs :: Integer, sysdbDetail :: Text, sysdbLoud :: Bool }
  | SysdbUnexpectedChannel { sysdbChannel :: Text }
  | SysdbQueueMismatch { sysdbWorkflowId :: Text }
  | SysdbNotifierStopped
  | SysdbPushFailed { sysdbPushChannel :: Text, sysdbPushCount :: Int, sysdbPushDetail :: Text }
  | SysdbClockUnreadable { sysdbDetail :: Text }
  deriving stock (Eq, Show)

instance LogEvent SysdbEvent where
  eventSeverity SysdbRetryAttempt {}      = SeverityWarning
  eventSeverity SysdbRetryConflict {sysdbLoud = loud} = if loud then SeverityWarning else SeverityDebug
  eventSeverity SysdbUnexpectedChannel {} = SeverityWarning
  eventSeverity SysdbQueueMismatch {}     = SeverityWarning
  eventSeverity SysdbNotifierStopped      = SeverityDebug
  eventSeverity SysdbPushFailed {}        = SeverityWarning
  eventSeverity SysdbClockUnreadable {}   = SeverityWarning
  renderEvent (SysdbRetryAttempt operation attempt delayMs detail) =
    "system database operation failed; retrying operation="
      <> operation
      <> " attempt="
      <> showText attempt
      <> " delay_ms="
      <> showText delayMs
      <> " error="
      <> detail
  renderEvent (SysdbRetryConflict operation attempt delayMs detail loud) =
    (if loud then "system database operation failed; retrying " else "system database operation lost a transaction conflict; replaying ")
      <> "operation="
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
  renderEvent (SysdbClockUnreadable detail) =
    "could not read the database's clock; the claimed deadlines are timed from this host's error="
      <> detail

instance ToLogStr SysdbEvent where
  toLogStr = toLogStr . renderLine

-- | How long to wait between attempts, and what to give up on. The
-- defaults are Python's and Java's, which agree: one second, doubling to a
-- minute, no attempt limit, connection failures blocked on rather than
-- reported.
data RetryPolicy = RetryPolicy
  { retryPolicyInitialBackoff         :: Duration,
    retryPolicyMaxBackoff             :: Duration,
    retryPolicyRetryConnectionErrors  :: Bool,
    -- | Whether a transaction conflict is replayed rather than returned.
    -- A caller with its own answer to losing a race turns this off.
    retryPolicyRetryConflicts         :: Bool,
    retryPolicyConflictInitialBackoff :: Duration,
    retryPolicyConflictMaxBackoff     :: Duration
  }
  deriving stock (Eq, Show)

-- | The shared defaults.
defaultRetryPolicy :: RetryPolicy
defaultRetryPolicy =
  RetryPolicy
    { retryPolicyInitialBackoff = secondsDuration 1,
      retryPolicyMaxBackoff = secondsDuration 60,
      retryPolicyRetryConnectionErrors = True,
      retryPolicyRetryConflicts = True,
      retryPolicyConflictInitialBackoff = millisDuration 50,
      retryPolicyConflictMaxBackoff = secondsDuration 2
    }

-- | The policy a caller with its own answer to losing a race uses: the
-- first conflict reaches it instead of being slept off underneath it.
-- Connection retries keep whatever the policy says.
withoutConflictReplay :: RetryPolicy -> RetryPolicy
withoutConflictReplay policy =
  policy {retryPolicyRetryConflicts = False}

-- | Whether this failure should be retried. A transaction conflict follows
-- its own switch; a connection failure follows its own; everything else —
-- including answers like 'ConflictingWorkflow', which would loop forever —
-- is returned at once.
shouldRetry :: RetryPolicy -> Error -> Bool
shouldRetry policy err =
  case err of
    Backend backend -> case backend.backendKind of
      Transient  -> policy.retryPolicyRetryConflicts
      Connection -> policy.retryPolicyRetryConnectionErrors
      Permanent  -> False
    _ -> False

-- | Spreads the backoff over @[0.5, 1.5)@ of its nominal value, from the
-- given entropy word: without it every process that lost the same database
-- comes back at the same instant. Pure, so a test can pin it; production
-- feeds it 'uuidEntropy'.
jitter :: Word32 -> Duration -> Duration
jitter bits backoff =
  fromMaybe backoff (scaleDuration (0.5 + fromIntegral bits / 4294967296) backoff)

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
withRetry policy operation tracer nextEntropy work =
  go policy.retryPolicyInitialBackoff policy.retryPolicyConflictInitialBackoff (0 :: Integer)
  where
    go backoff conflictBackoff attempt = do
      result <- work
      case result of
        Right value -> pure (Right value)
        Left err
          | not (shouldRetry policy err) -> pure (Left err)
          | otherwise -> do
              bits <- nextEntropy
              let kind = case err of
                    Backend backend -> backend.backendKind
                    _               -> Permanent
                  (delay, nextBackoff, nextConflictBackoff, event) = case kind of
                    Transient ->
                      let loud = conflictBackoff >= policy.retryPolicyConflictMaxBackoff
                       in ( jitter bits conflictBackoff,
                            backoff,
                            min (double conflictBackoff) policy.retryPolicyConflictMaxBackoff,
                            \delayMs -> SysdbRetryConflict operation (attempt + 1) delayMs (renderError err) loud
                          )
                    _ ->
                      ( jitter bits backoff,
                        min (double backoff) policy.retryPolicyMaxBackoff,
                        conflictBackoff,
                        \delayMs -> SysdbRetryAttempt operation (attempt + 1) delayMs (renderError err)
                      )
                  delayMs = durationAsMillis delay
              runTracer tracer (event delayMs)
              threadDelay (fromInteger (delayMs * 1000))
              go nextBackoff nextConflictBackoff (attempt + 1)
    double backoff = fromMaybe backoff (scaleDuration 2 backoff)

-- | Production entropy for 'jitter': the first word of a fresh v4 UUID,
-- the same source the oracle draws from.
uuidEntropy :: IO Word32
uuidEntropy = do
  uuid <- UUID.V4.nextRandom
  let (first, _, _, _) = UUID.toWords uuid
  pure first
