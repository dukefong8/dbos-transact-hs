{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module DBOS.SystemDB.NotifierTest (tests) where

import DBOS.Prelude
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Control.Monad (replicateM_)
import DBOS.SystemDB
  ( Notifier (..),
    enable,
    eventsChannel,
    eventKey,
    flush,
    keyFor,
    newRegistry,
    notified,
    notifierNew,
    signal,
    streamsChannel,
    subscribe,
  )
import DBOS.SystemDB.Postgres
  ( PostgresSystemDB (..),
    acquirePostgresSystemDB,
    configFromEnv,
  )
import Data.List qualified as List
import Data.Map.Strict qualified as Map
import Data.Set qualified as Set
import Data.Text (Text)
import DBOS.Transact (LogEvent (..), SomeTracer (..), mkTracer, nullTracer)
import Hasql.Pool qualified as Pool
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (testCase, (@?=))

-- | The writer's half of the wakeup path: what this process wrote, told to
-- everyone else. Every case mirrors its Rust test name.
tests :: TestTree
tests =
  withResource acquireSuitePool Pool.release $ \getPool ->
  testGroup
    "Notifier"
    [ testCase "a key written repeatedly is pushed once" $ do
        withNotifier getPool $ \notifier -> do
          enable notifier
          replicateM_ 5 (signal notifier streamsChannel "wf" "progress")
          drained notifier >>= (@?= [(streamsChannel, ["wf::progress"])]),
      testCase "the two channels batch separately" $ do
        withNotifier getPool $ \notifier -> do
          enable notifier
          signal notifier eventsChannel "wf" "ready"
          signal notifier streamsChannel "wf" "progress"
          signal notifier eventsChannel "wf" "result"
          drained notifier
            >>= (@?= [ (streamsChannel, ["wf::progress"]),
                       (eventsChannel, ["wf::ready", "wf::result"])
                     ]),
      testCase "without the push a signal still wakes a local waiter" $ do
        withNotifier getPool $ \notifier -> do
          subscription <- subscribe notifier.registry (eventKey "wf" "ready")
          signal notifier eventsChannel "wf" "ready"
          drained notifier >>= (@?= [])
          waitFor (notified subscription),
      testCase "a flush pushes the queued payloads and drains them" $ do
        warnings <- newIORef []
        withNotifierLogging getPool (collectingTracer warnings) $ \notifier -> do
          enable notifier
          signal notifier eventsChannel "wf" "ready"
          flush notifier
          drained notifier >>= (@?= [])
          -- The push is dropped on failure, so a warning is the only trace a
          -- bad statement would leave; none means pg_notify ran clean.
          readIORef warnings >>= (@?= []),
      testCase "a signal on an unknown channel is announced" $ do
        events <- newIORef []
        withNotifierCollecting getPool events (\notifier -> signal notifier "bogus-channel" "wf" "ready")
        readIORef events >>= (@?= ["signalled on an unexpected channel channel=bogus-channel"]),
      testCase "the pushed payload is the key a waiter holds" $ do
        withNotifier getPool $ \notifier -> do
          enable notifier
          signal notifier eventsChannel "wf" "ready"
          batches <- drained notifier
          case batches of
            [(channel, payloads)] -> case payloads of
              [payload] -> keyFor channel payload @?= Just (eventKey "wf" "ready")
              other -> fail ("expected one payload, got: " <> show other)
            other -> fail ("expected one channel's batch, got: " <> show other)
    ]

-- | One pool for the whole group: pools bound connections, so sharing
-- bounds them no matter how many tests run or are interrupted. The
-- registry stays fresh per case, so wakes never cross tests. The backend's
-- retry and notifier warnings go nowhere: nullTracer.
acquireSuitePool :: IO Pool.Pool
acquireSuitePool = do
  config <- configFromEnv
  env <- acquirePostgresSystemDB config nullTracer
  pure env.psdbPool

-- | A notifier over the suite pool. Nothing here flushes, so the pool is
-- only held; the registry is fresh per case so wakes never cross tests,
-- and the notifier's warnings go nowhere: nullTracer.
withNotifier :: IO Pool.Pool -> (Notifier -> IO a) -> IO a
withNotifier getPool = withNotifierLogging getPool nullTracer

-- | A notifier whose warnings land in the given carrier, so a test can see
-- what the flush loop swallowed.
withNotifierLogging :: IO Pool.Pool -> SomeTracer IO -> (Notifier -> IO a) -> IO a
withNotifierLogging getPool tracer action = do
  pool <- getPool
  registry <- newRegistry
  notifier <- notifierNew pool registry Nothing tracer
  action notifier

-- | A carrier collecting rendered event lines: the Rank-N hole needs a
-- polymorphic emit, so the signature pins it explicitly.
collectingTracer :: IORef [Text] -> SomeTracer IO
collectingTracer ref = SomeTracer (mkTracer emit)
  where
    emit :: LogEvent e => e -> IO ()
    emit e = modifyIORef' ref (renderEvent e :)

-- | A notifier whose rendered event lines land in the given ref. Text, not
-- typed events: a concrete collector cannot fill the carrier's Rank-N
-- hole, so IO tests assert on lines and sim tests on types.
withNotifierCollecting :: IO Pool.Pool -> IORef [Text] -> (Notifier -> IO a) -> IO a
withNotifierCollecting getPool ref =
  withNotifierLogging getPool (collectingTracer ref)

-- | Drains what is queued, channels and payloads sorted, exactly as the Rust
-- test helper does.
drained :: Notifier -> IO [(Text, [Text])]
drained notifier = do
  queued <- atomically $ do
    queued <- readTVar notifier.pending
    writeTVar notifier.pending Map.empty
    pure queued
  pure (List.sort [(channel, List.sort (Set.toList batch)) | (channel, batch) <- Map.toList queued])

-- | A waiter that must be woken, not left waiting.
waitFor :: IO a -> IO a
waitFor action = do
  result <- timeout 5000000 action
  case result of
    Just value -> pure value
    Nothing -> fail "the waiter was never woken"
