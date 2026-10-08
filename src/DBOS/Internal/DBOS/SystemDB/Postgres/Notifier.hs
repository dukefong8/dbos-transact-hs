{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The writer's half of the wakeup path: what this process wrote, told to
-- everyone else. Mirrors Rust @sysdb::postgres::notifier@.
--
-- The notifications channel is fed by migration 1's trigger, so a @send@
-- needs nothing from this module. Events and streams have no trigger
-- (migrations 43 and 44 remove them), and this is what feeds their channels
-- instead: a trigger fires inside the writing transaction and its @NOTIFY@
-- serialises every notifying commit in the database against every other,
-- while pushing from the application moves that lock off the write path and
-- lets a batch of writes cost one notifying transaction instead of one each.
--
-- Nothing here is load-bearing: every wait re-queries on its own interval, so
-- with this module deleted the same values are delivered, just later. What it
-- buys is that a reader in another process hears about a value in
-- milliseconds rather than waiting out an interval — and a reader in /this/
-- process hears with no round trip at all, since 'signal' wakes the local
-- registry directly.
--
-- The queue is off until 'enable', and never on where there is nothing to
-- push to: nothing drains a queue no flush loop is reading, so accumulating
-- there would be an unbounded leak. The local wake is not gated on it — it
-- costs no database and is right on every backend.
module DBOS.SystemDB.Postgres.Notifier
  ( coalesceInterval,
    pushBatch,
    Notifier (..),
    notifierNew,
    enable,
    isPushing,
    signal,
    run,
    stop,
    flush,
  )
where

import DBOS.Prelude
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Functor.Contravariant (contramap)
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.SystemDB.Notify (Registry, keyFor, wake)
import DBOS.SystemDB.Retry (SysdbEvent (..))
import DBOS.SystemDB.Types (Duration, durationAsMillis, millisDuration)
import DBOS.Transact.Logger (SomeTracer, runTracer)
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Pool qualified as Pool
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement

-- | How long a payload waits for company before it is pushed. The whole point
-- is that it is not zero: a push per write would be one notifying transaction
-- per write, which is exactly the cost a database trigger has; ten
-- milliseconds of latency turns a burst of writes into one statement. Ten is
-- Go's @DefaultNotificationCoalesceInterval@, Python's
-- @notification_coalesce_sec@, TypeScript's @DEFAULT_NOTIFICATION_COALESCE_MS@
-- and Java's flush period.
coalesceInterval :: Duration
coalesceInterval = millisDuration 10

-- | One statement per channel per flush, however many payloads the batch
-- holds. @unnest@ is what makes that possible: the SQL text is the same for
-- one payload and a thousand, so the server can reuse the plan, and one round
-- trip takes the async-notify queue lock once. The one statement all four
-- references share character for character.
pushBatch :: Statement.Statement (Text, [Text]) ()
pushBatch =
  Statement.preparable
    "SELECT pg_notify($1, p) FROM unnest($2::text[]) AS p"
    ( contramap fst (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap snd (Encoders.param (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text))))
    )
    Decoders.noResult

-- | What this process has written and not yet told anyone else about. One set
-- per channel, so a batch that cannot be sent takes only its own channel down
-- with it — and a set rather than a list, because repeated writes to one key
-- between flushes are one thing to look at, not one each.
type Pending = Map Text (Set Text)

-- | The outbound half of the wakeup path.
data Notifier = Notifier
  { pool :: Pool.Pool,
    registry :: Registry,
    -- | The coalescing window, from the settings or 'coalesceInterval'.
    interval :: Duration,
    -- | Whether payloads are queued for other processes at all.
    pushing :: StrictTVar IO Bool,
    pending :: StrictTVar IO Pending,
    -- | Wakes the flush loop: for the payload that opens a batch, and for a
    -- stop. One signal for both because the loop's answer to either is to
    -- look at what it has; what distinguishes them is 'stopping'.
    woken :: StrictTVar IO Bool,
    -- | Whether the flush loop should make its last flush and return.
    stopping :: StrictTVar IO Bool,
    -- | Where the two Rust @tracing::warn!@s go. Not a Rust field: the port
    -- keeps tracing explicit, one universal carrier per backend.
    log :: SomeTracer IO
  }

-- | Mirrors Rust @Notifier::new@; the free-function spelling follows
-- 'DBOS.SystemDB.Postgres.configNew'.
notifierNew :: Pool.Pool -> Registry -> Maybe Duration -> SomeTracer IO -> IO Notifier
notifierNew pool registry interval tracer = do
  pushing <- newTVarIO False
  pending <- newTVarIO Map.empty
  woken <- newTVarIO False
  stopping <- newTVarIO False
  pure
    Notifier
      { pool = pool,
        registry = registry,
        interval = fromMaybe coalesceInterval interval,
        pushing = pushing,
        pending = pending,
        woken = woken,
        stopping = stopping,
        log = tracer
      }

-- | Starts queueing payloads for the other processes listening on the
-- channels. Separate from 'run' only because the two have different owners —
-- the caller does both at once, and one without the other is either a leak or
-- a silence.
enable :: Notifier -> IO ()
enable notifier = atomically (writeTVar notifier.pushing True)

-- | Whether payloads are being queued for other processes.
isPushing :: Notifier -> IO Bool
isPushing notifier = readTVarIO notifier.pushing

-- | Reports a row this process has just written.
--
-- Call it after the transaction commits, never before: a waiter woken by this
-- looks at the database, and one that looks before the row is visible finds
-- nothing and goes back to sleep until its own interval comes round — which
-- is the stall the wakeup existed to prevent. Waiters here are woken
-- immediately and with no round trip, whether or not anything is being
-- pushed.
signal :: Notifier -> Text -> Text -> Text -> IO ()
signal notifier channel workflowId key = do
  let payload = workflowId <> "::" <> key
  case keyFor channel payload of
    Nothing ->
      runTracer notifier.log (SysdbUnexpectedChannel channel)
    Just registryKey -> do
      wake notifier.registry registryKey
      pushing <- readTVarIO notifier.pushing
      when pushing $ do
        first <- atomically $ do
          pending' <- readTVar notifier.pending
          let batch = Map.findWithDefault Set.empty channel pending'
              pending'' = Map.insert channel (Set.insert payload batch) pending'
          writeTVar notifier.pending pending''
          pure (sum (map Set.size (Map.elems pending'')) == 1)
        -- Only the payload that opens a batch wakes the loop, and that is
        -- what makes the window a window: the rest ride along on the flush it
        -- already scheduled, rather than each cutting it short.
        when first (atomically (writeTVar notifier.woken True))

-- | Flushes what has been signalled, until 'stop', then once more.
--
-- Idle costs nothing: the loop waits for the first payload and then opens the
-- window, so it coalesces exactly the same writes as a ticker would, since
-- the window is measured from the first signal either way.
run :: Notifier -> IO ()
run notifier = do
  loop
  -- Whatever the window was still holding, so a value written just before a
  -- shutdown wakes readers elsewhere rather than leaving them to their
  -- interval.
  flush notifier
  runTracer notifier.log SysdbNotifierStopped
  where
    loop = do
      stopping <- readTVarIO notifier.stopping
      unless stopping $ do
        atomically $ do
          woken <- readTVar notifier.woken
          check woken
          writeTVar notifier.woken False
        stopping' <- readTVarIO notifier.stopping
        unless stopping' $ do
          -- The coalescing window: everything signalled during it goes out
          -- with the payload that opened it. Cut short only by a stop, which
          -- is why it is a timeout around the stop wait rather than a plain
          -- sleep.
          _ <- timeout (micros notifier.interval) (atomically (check =<< readTVar notifier.stopping))
          flush notifier
          loop

-- | Asks 'run' to make its final flush and return. The final flush is a
-- database write, so a caller closing a handle has to stop the notifier
-- /before/ closing the pool, not after.
stop :: Notifier -> IO ()
stop notifier = atomically $ do
  writeTVar notifier.stopping True
  writeTVar notifier.woken True

-- | Emits one notifying transaction per channel for everything queued since
-- the last flush.
flush :: Notifier -> IO ()
flush notifier = do
  batches <- atomically $ do
    queued <- readTVar notifier.pending
    writeTVar notifier.pending Map.empty
    pure [(channel, Set.toList batch) | (channel, batch) <- Map.toList queued, not (Set.null batch)]
  for_ batches $ \(channel, payloads) -> do
    sent <- Pool.use notifier.pool (Session.statement (channel, payloads) pushBatch)
    case sent of
      -- Dropped, never requeued: a payload the database will not take — one
      -- over @pg_notify@'s 8000-byte limit, say — would otherwise be retried
      -- forever and stall every later batch behind it. What is lost is an
      -- interval of latency for whoever was waiting, not the value. Not
      -- retried through the retry loop either, for the same reason: it has no
      -- attempt limit, so a channel that cannot be pushed would hold the loop
      -- rather than the queue.
      Left usage ->
        runTracer
          notifier.log
          ( SysdbPushFailed channel (length payloads) (Text.pack (show usage))
          )
      Right () -> pure ()

-- | The interval in microseconds, the unit 'threadDelay' takes.
micros :: Duration -> Int
micros = fromInteger . (* 1000) . durationAsMillis
