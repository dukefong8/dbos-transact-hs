{-# LANGUAGE OverloadedStrings #-}

module DBOS.TracerTest
  ( tests,
  )
where

import Control.Monad.IOSim (runSimTrace, selectTraceEventsDynamic, selectTraceEventsSay)
import Control.Tracer qualified as CT
import Data.ByteString.Char8 qualified as ByteString
import Data.IORef (IORef, atomicModifyIORef', modifyIORef', newIORef, readIORef)
import Data.List (isInfixOf)
import Data.Text (Text, pack)
import DBOS.IOSimTracer (simTracer)
import DBOS.Prelude
import DBOS.Transact (EngineEvent (..), LoggerBackend, LogEvent (..), LogSeverity (..), QueueEvent (..), SomeTracer (..), SysdbEvent (..), Tracer, WorkflowEvent (..), contramap, fastLoggerTracer, mkTracer, newLoggerBackend, nullTracer, parseSeverity, renderEvent, runTracer)
import System.Log.FastLogger (FormattedTime, LogStr, LogType' (..), ToLogStr, fromLogStr, newTimeCache, newTimedFastLogger, toLogStr)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

-- | A backend over a synchronous callback sink: the thread-id cases read
-- their lines back from the collector. LogCallback sinks on the spot, so a
-- line is readable as soon as the trace returns — no buffer to flush.
callbackBackend :: IO FormattedTime -> LogSeverity -> IORef [LogStr] -> IO (LoggerBackend, IO ())
callbackBackend getTime minSeverity collected = do
  (logger, release) <- newTimedFastLogger getTime (LogCallback (\line -> atomicModifyIORef' collected (\lines' -> (line : lines', ())) >> pure ()) (pure ()))
  backend <- newLoggerBackend logger minSeverity
  pure (backend, release)

-- | The lines a callback sink collected, in emission order.
renderedLines :: IORef [LogStr] -> IO [String]
renderedLines collected = map (ByteString.unpack . fromLogStr) . reverse <$> readIORef collected

tests :: TestTree
tests =
  testGroup
    "DBOS Tracer"
    [ testCase "step events render the legacy lines" $ do
        renderEvent (StepRunning "double" 3) @?= ("running step double (3)" :: Text),
      testCase "engine version staleness carries both versions" $ do
        renderEvent (EngineVersionStale "1.0.0" "9.9.9") @?= ("this executor is not running the latest registered application version: it will recover and dequeue only work stamped with its own version app_version=1.0.0 latest_version=9.9.9" :: Text),
      testCase "an empty recovery and a populated one render distinctly" $ do
        renderEvent (EngineRecovered 0) @?= ("no workflows to recover" :: Text)
        renderEvent (EngineRecovered 3) @?= ("re-enqueued workflows a previous run left PENDING workflows=3" :: Text),
      testCase "a skipped row names its workflow" $ do
        renderEvent (DequeuedRowSkipped "wf-9") @?= ("the dequeued row wf-9 names no workflow; skipped" :: Text),
      testCase "a contended dequeue announces its backoff" $ do
        renderEvent DequeueBackoff @?= ("a peer is mid-dequeue; backing off" :: Text),
      testCase "a slow pass reports its sweep, claims and elapsed time" $ do
        renderEvent (DequeuePassSlow 16050 1 98231) @?= ("a dequeue pass took longer than a second: queues 16050, claimed 1, elapsed_ms 98231" :: Text),
      testCase "a line names its event's constructor, nullary ones included" $ do
        eventName (StepRunning "double" 3) @?= ("StepRunning" :: Text)
        eventName DequeueBackoff @?= ("DequeueBackoff" :: Text),
      testCase "a line is severity, constructor, prose" $ do
        renderLine (StepRunning "double" 3) @?= ("[Debug] StepRunning: running step double (3)" :: Text),
      testCase "events carry their severity and name into LogStr" $ do
        ByteString.unpack (fromLogStr (toLogStr (StepRunning "double" 3))) @?= "[Debug] StepRunning: running step double (3)",
      testCase "a sim run carries the structured event and its line" $ do
        let traced = selectTraceEventsDynamic (runSimTrace (runTracer simTracer (StepRunning "double" 3)))
            said = selectTraceEventsSay (runSimTrace (runTracer simTracer (StepRunning "double" 3)))
        traced @?= [StepRunning "double" 3]
        said @?= ["[Debug] StepRunning: running step double (3)"],
      testCase "contramap zooms a general tracer to a domain event" $ do
        collected <- newIORef []
        let textTracer = mkTracer (\line -> modifyIORef' collected (line :)) :: Tracer IO Text
            eventTracer = contramap renderEvent textTracer
        CT.traceWith eventTracer DequeueBackoff
        messages <- reverse <$> readIORef collected
        messages @?= ["a peer is mid-dequeue; backing off"],
      testCase "null tracer discards events" $ do
        runTracer nullTracer (StepRunning "dropped" 0)
        pure (),
      testCase "null tracer forces nothing and renders nothing" $ do
        -- Contra-tracer's null tracer is a squelching arrow whose payload
        -- is discarded before the event is touched, and nothing in this
        -- module renders an event on the way in: a bottom event would
        -- throw if the null path even matched on it, and a bottom field
        -- would throw if anything rendered it.
        runTracer nullTracer (error "nullTracer forced the event" :: WorkflowEvent)
        runTracer nullTracer (StepRunning (error "nullTracer rendered the field") 1)
        pure (),
      testCase "one production tracer serves any event type" $ do
        getTime <- newTimeCache "%Y-%m-%dT%H:%M:%S%z"
        (logger, cleanup) <- newTimedFastLogger getTime LogNone
        backend <- newLoggerBackend logger SeverityDebug
        let tracer :: (LogEvent e, ToLogStr e) => Tracer IO e
            tracer = fastLoggerTracer backend
        CT.traceWith tracer (StepRunning "double" 3)
        CT.traceWith tracer (EngineLaunched "app" "exec" "1.0")
        cleanup,
      testCase "fast-logger lines carry the emitting thread's id" $ do
        getTime <- newTimeCache "%Y-%m-%dT%H:%M:%S%z"
        collected <- newIORef []
        (backend, release) <- callbackBackend getTime SeverityDebug collected
        let tracer :: Tracer IO WorkflowEvent
            tracer = fastLoggerTracer backend
        self <- myThreadId
        CT.traceWith tracer (StepRunning "double" 3)
        CT.traceWith tracer (StepRunning "triple" 4)
        release
        rendered <- renderedLines collected
        case rendered of
          [first, second] -> do
            assertBool ("the first line names the emitting thread " <> show self <> ": " <> first) (isInfixOf (show self) first)
            assertBool ("the second line names it too: " <> second) (isInfixOf (show self) second)
            assertBool ("the first line carries its event: " <> first) (isInfixOf "[Debug] StepRunning: running step double (3)" first)
            assertBool ("the second line carries its event: " <> second) (isInfixOf "[Debug] StepRunning: running step triple (4)" second)
          lines' -> assertFailure ("expected exactly two log lines, got: " <> show lines'),
      testCase "ids stay per-thread past the cache's capacity" $ do
        -- One line per thread, more threads than the cache holds: the
        -- table's reset path must not hand a thread another's id.
        getTime <- newTimeCache "%Y-%m-%dT%H:%M:%S%z"
        collected <- newIORef []
        emitted <- newIORef []
        (backend, release) <- callbackBackend getTime SeverityDebug collected
        let tracer :: Tracer IO WorkflowEvent
            tracer = fastLoggerTracer backend
        dones <- replicateM 300 newEmptyMVar
        forM_ (zip [1 :: Int ..] dones) $ \(index, done) -> do
          _ <-
            forkIO
              ( do
                  self <- myThreadId
                  atomicModifyIORef' emitted (\ids -> ((index, show self) : ids, ()))
                  CT.traceWith tracer (StepRunning (pack ("step-" <> show index)) 0)
                  putMVar done ()
              )
          pure ()
        mapM_ takeMVar dones
        release
        rendered <- renderedLines collected
        ids <- readIORef emitted
        forM_ ids $ \(index, threadText) -> do
          let mine = filter (isInfixOf ("running step step-" <> show index <> " (")) rendered
          case mine of
            [line] -> assertBool ("thread " <> threadText <> "'s line names it: " <> line) (isInfixOf (" " <> threadText <> " ") line)
            other -> assertFailure ("expected one line for step-" <> show index <> ", got: " <> show other),
      testCase "TRACE_LEVEL names parse case- and space-insensitively" $ do
        parseSeverity "debug" @?= Just SeverityDebug
        parseSeverity " INFO " @?= Just SeverityInfo
        parseSeverity "Warn" @?= Just SeverityWarning
        parseSeverity "error" @?= Just SeverityError
        parseSeverity "trace" @?= Nothing,
      testCase "severity orders from debug upward" $ do
        assertBool "debug below info" (SeverityDebug < SeverityInfo)
        assertBool "info below warning" (SeverityInfo < SeverityWarning)
        assertBool "warning below error" (SeverityWarning < SeverityError),
      testCase "events below the backend's floor are dropped before formatting" $ do
        getTime <- newTimeCache "%Y-%m-%dT%H:%M:%S%z"
        collected <- newIORef []
        (backend, release) <- callbackBackend getTime SeverityWarning collected
        let tracer :: Tracer IO WorkflowEvent
            tracer = fastLoggerTracer backend
        -- The dropped event carries a bottom field: letting it through to
        -- rendering would throw here instead of passing.
        CT.traceWith tracer (StepRunning "dropped" (error "a dropped line was rendered"))
        CT.traceWith tracer (StepRetrying "kept" 0 1 2 100 "boom")
        release
        rendered <- renderedLines collected
        length rendered @?= 1
        assertBool
          ("the kept line carries its event, and only it: " <> show rendered)
          (any (isInfixOf "[Warning] StepRetrying:") rendered),
      testCase "one universal tracer collects events of any domain" $ do
        collected <- newIORef []
        let backend = SomeTracer (mkTracer emit)
            emit :: LogEvent e => e -> IO ()
            emit e = modifyIORef' collected (renderEvent e :)
        runTracer backend (EngineLaunched "app" "exec" "1.0")
        runTracer backend (SysdbRetryAttempt "test-op" 1 1000 "boom")
        messages <- reverse <$> readIORef collected
        messages @?= ["DBOS launched app_name=app executor_id=exec app_version=1.0", "system database operation failed; retrying operation=test-op attempt=1 delay_ms=1000 error=boom"]
    ]
