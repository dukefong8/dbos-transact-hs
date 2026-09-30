{-# LANGUAGE OverloadedStrings #-}

module DBOS.TracerTest
  ( tests,
  )
where

import DBOS.Prelude
import DBOS.Transact (EngineEvent (..), LogEvent (..), QueueEvent (..), SomeTracer (..), SysdbEvent (..), Tracer, WorkflowEvent (..), contramap, fastLoggerTracer, mkTracer, nullTracer, renderEvent, simTracer, traceWith)
import Control.Monad.IOSim (runSimTrace, selectTraceEventsDynamic)
import Control.Tracer qualified as CT
import Control.Monad.IOSim (runSimTrace, selectTraceEventsDynamic)
import Data.ByteString.Char8 qualified as ByteString
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Text (Text)
import System.Log.FastLogger (LogType' (..), ToLogStr, fromLogStr, newTimeCache, newTimedFastLogger, toLogStr)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), testCase)

tests :: TestTree
tests =
  testGroup
    "DBOS Tracer"
    [       testCase "step events render the legacy lines" $ do
        renderEvent (StepRunning "double" 3) @?= ("running step double (3)" :: Text),
      testCase "engine version staleness carries both versions" $ do
        renderEvent (EngineVersionStale "1.0.0" "9.9.9") @?= ("this executor is not running the latest registered application version: it will recover and dequeue only work stamped with its own version app_version=1.0.0 latest_version=9.9.9" :: Text),
      testCase "an empty recovery and a populated one render distinctly" $ do
        renderEvent (EngineRecovered 0) @?= ("no workflows to recover" :: Text)
        renderEvent (EngineRecovered 3) @?= ("re-enqueued workflows a previous run left PENDING workflows=3" :: Text),
      testCase "a skipped row names its workflow" $ do
        renderEvent (DequeuedRowSkipped "wf-9") @?= ("the dequeued row wf-9 names no workflow; skipped" :: Text),
      testCase "events carry their severity into LogStr" $ do
        ByteString.unpack (fromLogStr (toLogStr (StepRunning "double" 3))) @?= "[Debug] running step double (3)",
      testCase "sim traces recover the structured event by type" $ do
        let traced = selectTraceEventsDynamic (runSimTrace (traceWith simTracer (StepRunning "double" 3)))
        traced @?= [StepRunning "double" 3],
      testCase "contramap zooms a general tracer to a domain event" $ do
        collected <- newIORef []
        let textTracer = mkTracer (\line -> modifyIORef' collected (line :)) :: Tracer IO Text
            eventTracer = contramap renderEvent textTracer
        CT.traceWith eventTracer DequeueBackoff
        messages <- reverse <$> readIORef collected
        messages @?= ["a peer is mid-dequeue; backing off"],
      testCase "null tracer discards events" $ do
        traceWith nullTracer (StepRunning "dropped" 0)
        pure (),
      testCase "one production tracer serves any ToLogStr event" $ do
        getTime <- newTimeCache "%Y-%m-%dT%H:%M:%S%z"
        (logger, cleanup) <- newTimedFastLogger getTime LogNone
        let tracer :: ToLogStr e => Tracer IO e
            tracer = fastLoggerTracer logger
        CT.traceWith tracer (StepRunning "double" 3)
        CT.traceWith tracer ("raw text" :: Text)
        cleanup,
      testCase "one universal tracer collects events of any domain" $ do
        collected <- newIORef []
        let backend = SomeTracer (mkTracer emit)
            emit :: LogEvent e => e -> IO ()
            emit e = modifyIORef' collected (renderEvent e :)
        traceWith backend (EngineLaunched "app" "exec" "1.0")
        traceWith backend (SysdbRetryAttempt "test-op" 1 1000 "boom")
        messages <- reverse <$> readIORef collected
        messages @?= ["DBOS launched app_name=app executor_id=exec app_version=1.0", "system database operation failed; retrying operation=test-op attempt=1 delay_ms=1000 error=boom"]
    ]
