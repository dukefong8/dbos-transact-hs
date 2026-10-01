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
    acquireFastBackend,
    fastLoggerTracer,

    -- * Events
    LogSeverity (..),
    showSeverity,
    LogEvent (..),
  )
where

import Control.Tracer (Tracer, contramap, mkTracer)
import Control.Tracer qualified as CT
import Data.Text (Text)
import Data.Typeable (Typeable)
import DBOS.Prelude
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

-- | Acquire the production backend: a one-second-cached timed stderr
-- logger plus its flush-and-release cleanup. Stderr keeps log lines off
-- the test-result stream: stdout carries results, logs go beside them.
-- The cleanup is the second half of the pair, so launch brackets
-- acquisition against shutdown with no call-site changes.
acquireFastBackend :: IO (TimedFastLogger, IO ())
acquireFastBackend = do
  getTime <- newTimeCache "%Y-%m-%dT%H:%M:%S%z"
  newTimedFastLogger getTime (LogStderr defaultBufSize)

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
-- serves every event type, each line carrying the cached timestamp plus
-- the event's own rendering.
ioTracer :: TimedFastLogger -> SomeTracer IO
ioTracer logger = SomeTracer (fastLoggerTracer logger)
