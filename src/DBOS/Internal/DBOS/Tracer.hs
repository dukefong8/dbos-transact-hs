{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}

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
-- One rendered line names its event: @[Debug] StepRunning: running step
-- double (3)@ — severity tag, data-constructor name, prose. The
-- constructor stands in for Rust's @tracing@ target; the IO backend
-- additionally prefixes FastLogger's cached time and the emitting
-- thread's id, itself cached per thread like the time. The IO backend
-- renders only events at or above its @TRACE_LEVEL@ floor (unset:
-- everything); lines below it are dropped before any formatting.
--
-- Rule 4: plain Haskell, no Bluefin imports. Rule 5: the tracer is passed
-- explicitly, never ambient. No @co-log@ import anywhere in this module.
module DBOS.Tracer
  ( -- * Universal carrier
    SomeTracer (..),
    runTracer,
    nullTracer,
    ioTracer,

    -- * Narrow building blocks
    Tracer,
    mkTracer,
    contramap,
    TimedFastLogger,
    LoggerBackend (..),
    newLoggerBackend,
    acquireLoggerBackend,
    ThreadIdCache,
    fastLoggerTracer,

    -- * Events
    LogSeverity (..),
    showSeverity,
    parseSeverity,
    LogEvent (..),
  )
where

import Control.Tracer (Tracer, contramap, mkTracer)
import Control.Tracer qualified as CT
import Data.Char (isSpace, toLower)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Typeable (Typeable)
import DBOS.Prelude
import System.Environment (lookupEnv)
import System.Log.FastLogger (LogStr, LogType' (..), TimedFastLogger, ToLogStr (..), defaultBufSize, newTimeCache, newTimedFastLogger)

-- | Severity as data: which tag the rendered line carries. Mirrors the
-- four co-log levels the engine's call sites used, without the library.
-- The constructor order is the severity order, so the derived 'Ord' is
-- what the backend's @TRACE_LEVEL@ threshold compares against.
data LogSeverity
  = SeverityDebug
  | SeverityInfo
  | SeverityWarning
  | SeverityError
  deriving stock (Eq, Ord, Show)

-- | The tag a line carries: @[Debug]@, @[Info]@, @[Warning]@, @[Error]@.
showSeverity :: LogSeverity -> Text
showSeverity SeverityDebug   = "[Debug]"
showSeverity SeverityInfo    = "[Info]"
showSeverity SeverityWarning = "[Warning]"
showSeverity SeverityError   = "[Error]"

-- | An event knows its severity, its constructor name, and its prose. The
-- FastLogger backend consumes events through 'ToLogStr', which delegates
-- to 'renderLine', so a line is shaped in exactly one place.
class LogEvent e where
  eventSeverity :: e -> LogSeverity

  -- | The prose half of the line: what happened, with every span field as
  -- a @key=value@ pair, matching the Rust @tracing!@ message bodies.
  renderEvent :: e -> Text

  -- | The event's data-constructor name, e.g. @StepRunning@. Defaults to
  -- the leading word of 'show', which spells a constructor before its
  -- fields — so an event type only derives 'Show', as every one already
  -- does, and a new constructor needs no clause anywhere.
  eventName :: e -> Text
  default eventName :: Show e => e -> Text
  eventName = Text.takeWhile (/= ' ') . showText

  -- | The whole rendered line: severity tag, constructor name, prose.
  -- Mirrors the shape of Rust's fmt layer (@LEVEL target: message@), with
  -- the constructor standing in for the target, so a log grep can key on
  -- the event it wants.
  renderLine :: e -> Text
  renderLine e = showSeverity (eventSeverity e) <> " " <> eventName e <> ": " <> renderEvent e

-- | Per-thread pre-formatted ids: the thread-id analogue of fast-logger's
-- time cache. Time is global, so one background thread keeps its shared
-- slot fresh; a thread's id is known only to the thread itself, so the
-- emitting thread is the updater — its first line formats the id, every
-- later line reads the same 'LogStr' back. The memo is exact: nothing
-- about a hit can be stale, and a hit is a hash-map lookup outside STM
-- instead of the 'show'+'Text.pack' the line used to pay for. A 'HashMap'
-- because the key hashes cheaply — @hashable@'s 'ThreadId' instance hashes
-- the thread's numeric id through a primop, no rendering — and 'Eq' still
-- compares the thread itself, so a collision can only cost a re-format.
--
-- One cache may serve every tracer in the process — a hit is a fact about
-- the thread, not about the backend. The table is capped at
-- 'threadIdCacheCapacity' threads; a full table resets (amortized one
-- reset per capacity inserts), and the threads it held simply re-format on
-- their next line. The cap also bounds memory: an entry keeps its
-- 'ThreadId' reachable, and a 'ThreadId' points at the thread's TSO —
-- measured at ~1 KB of dead-thread state per cached id — so a long-lived
-- engine never accumulates finished threads without limit.
newtype ThreadIdCache = ThreadIdCache (StrictTVar IO (HashMap (ThreadId IO) LogStr))

-- | How many threads the id cache remembers before it resets. Sized for
-- the engine's concurrent workflow threads; see 'ThreadIdCache' for what
-- the cap costs and buys.
threadIdCacheCapacity :: Int
threadIdCacheCapacity = 256

-- | A fresh, empty id cache: one per process is enough, one per launch is
-- the tidy default.
newThreadIdCache :: IO ThreadIdCache
newThreadIdCache = ThreadIdCache <$> newTVarIO HashMap.empty

-- | The emitting thread's id as a 'LogStr', formatted once per thread and
-- read back on every later line.
cachedThreadId :: ThreadIdCache -> ThreadId IO -> IO LogStr
cachedThreadId (ThreadIdCache table) self = do
  entries <- readTVarIO table
  case HashMap.lookup self entries of
    Just formatted -> pure formatted
    Nothing -> do
      let formatted = toLogStr (showText self)
      atomically (modifyTVar table (storing formatted))
      pure formatted
  where
    -- A full table resets instead of evicting: the thread that just paid
    -- for a format is the one that keeps its slot.
    storing formatted entries =
      if HashMap.size entries >= threadIdCacheCapacity
        then HashMap.singleton self formatted
        else HashMap.insert self formatted entries

-- | The production backend: FastLogger's timed logger plus the per-thread
-- id cache and the severity floor its lines need. Bundling them keeps the
-- tracer constructors pure — the Rank-N shape needs no monadic setup — and
-- gives the cache its lifetime, the way fast-logger's own logger sets
-- bundle their buffers.
data LoggerBackend = LoggerBackend
  { backendLogger :: TimedFastLogger,
    backendThreadIds :: ThreadIdCache,
    -- | The lowest severity the backend renders. An event below it is
    -- dropped before anything is formatted — the null path for a level
    -- nobody asked for — so @TRACE_LEVEL@ costs one 'eventSeverity'
    -- match per line and nothing else. Read once, at acquisition.
    backendMinSeverity :: LogSeverity
  }

-- | A backend over any 'TimedFastLogger', sharing the production line
-- shape: tests build theirs over a callback sink.
newLoggerBackend :: TimedFastLogger -> LogSeverity -> IO LoggerBackend
newLoggerBackend logger minSeverity = LoggerBackend logger <$> newThreadIdCache <*> pure minSeverity

-- | Acquire the production backend: a one-second-cached timed stderr
-- logger, its per-thread id cache, its @TRACE_LEVEL@ floor, and the
-- logger's flush-and-release cleanup. Stderr keeps log lines off the
-- test-result stream: stdout carries results, logs go beside them. The
-- cleanup is the second half of the pair, so launch brackets acquisition
-- against shutdown with no call-site changes.
acquireLoggerBackend :: IO (LoggerBackend, IO ())
acquireLoggerBackend = do
  getTime <- newTimeCache "%Y-%m-%dT%H:%M:%S%z"
  (logger, release) <- newTimedFastLogger getTime (LogStderr defaultBufSize)
  backend <- newLoggerBackend logger =<< minSeverityFromEnv
  pure (backend, release)

-- | The @TRACE_LEVEL@ floor: the lowest severity the backend renders.
-- Unset, blank, or unrecognised reads as 'SeverityDebug' — the backend
-- keeps logging everything rather than silently swallowing lines over a
-- typo. The environment is read once, at acquisition.
minSeverityFromEnv :: IO LogSeverity
minSeverityFromEnv = do
  raw <- lookupEnv "TRACE_LEVEL"
  pure $ case raw >>= parseSeverity of
    Just severity -> severity
    Nothing -> SeverityDebug

-- | The level names @TRACE_LEVEL@ accepts, case- and space-insensitive:
-- @debug@, @info@, @warning@ (or @warn@), @error@.
parseSeverity :: String -> Maybe LogSeverity
parseSeverity raw = case map toLower (filter (not . isSpace) raw) of
  "debug" -> Just SeverityDebug
  "info" -> Just SeverityInfo
  "warning" -> Just SeverityWarning
  "warn" -> Just SeverityWarning
  "error" -> Just SeverityError
  _ -> Nothing

-- | The production tracer over any 'ToLogStr' event: the Rank-N shape —
-- one value serves every event type, each line carrying FastLogger's
-- cached timestamp, the emitting thread's id, and the event's own
-- rendering. fast-logger has no thread notion to reuse — its callback
-- receives only the formatted time, and its one @myThreadId@ picks a
-- per-capability buffer — so the id is read here, the customization its
-- haddock recommends, through the cache that keeps its formatting off the
-- line path. An event below the backend's 'backendMinSeverity' is dropped
-- before any of that work — the null path for a level nobody asked for,
-- at the cost of one severity match. The workflow threads the engine
-- forks are the unit an operator debugs, so the line says which one spoke.
fastLoggerTracer :: (LogEvent e, ToLogStr e) => LoggerBackend -> Tracer IO e
fastLoggerTracer backend = mkTracer emit
  where
    emit event
      | eventSeverity event < backend.backendMinSeverity = pure ()
      | otherwise = do
          self <- myThreadId
          formatted <- cachedThreadId backend.backendThreadIds self
          backend.backendLogger
            ( \time ->
                toLogStr time <> " " <> formatted <> " " <> toLogStr event <> "\n"
            )

-- | A backend tracer for any domain event: the Rank-N shape both
-- backends already have (FastLogger is polymorphic over 'ToLogStr',
-- io-sim's @traceM@ over 'Typeable'), wrapped so records hold one field
-- instead of one per domain. The GADT keeps the @forall@ on the
-- constructor, so holders need no Rank-N field types and — under
-- 'NoFieldSelectors' — callers consume it only through 'runTracer', never
-- record-dot. Mirrors the 'SomeSystemDB' existential one module over: the
-- handle type is universal, so a record does not name the backend and
-- tests may substitute their own.
data SomeTracer m where
  SomeTracer :: (forall e. (LogEvent e, ToLogStr e, Typeable e) => Tracer m e) -> SomeTracer m

-- | Emit any domain event through a universal carrier: the sole
-- pattern-match site, so the existential is unpacked in exactly one
-- place, like 'runSystemDB' for the backend.
runTracer :: (LogEvent e, ToLogStr e, Typeable e, Monad m) => SomeTracer m -> e -> m ()
runTracer (SomeTracer tracer) = CT.traceWith tracer

-- | A carrier that discards everything, in any event type and monad.
-- Used by clients (which run nothing) and as the default where no launch
-- has installed a real backend.
nullTracer :: Monad m => SomeTracer m
nullTracer = SomeTracer CT.nullTracer

-- | The production carrier over a launch's FastLogger backend: one value
-- serves every event type, each line carrying the cached timestamp, the
-- emitting thread's id, and the event's own rendering.
ioTracer :: LoggerBackend -> SomeTracer IO
ioTracer backend = SomeTracer (fastLoggerTracer backend)
