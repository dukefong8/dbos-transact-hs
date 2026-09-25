{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module DBOS.SystemDB.NotifierTest (tests) where

import DBOS.Prelude
import Colog.Core.Action (LogAction (..))
import Data.IORef (modifyIORef', newIORef, readIORef)
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
import Hasql.Pool qualified as Pool
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

-- | The writer's half of the wakeup path: what this process wrote, told to
-- everyone else. Every case mirrors its Rust test name.
tests :: TestTree
tests =
  testGroup
    "Notifier"
    [ testCase "a key written repeatedly is pushed once" $ do
        withNotifier $ \notifier -> do
          enable notifier
          replicateM_ 5 (signal notifier streamsChannel "wf" "progress")
          drained notifier >>= (@?= [(streamsChannel, ["wf::progress"])]),
      testCase "the two channels batch separately" $ do
        withNotifier $ \notifier -> do
          enable notifier
          signal notifier eventsChannel "wf" "ready"
          signal notifier streamsChannel "wf" "progress"
          signal notifier eventsChannel "wf" "result"
          drained notifier
            >>= (@?= [ (streamsChannel, ["wf::progress"]),
                       (eventsChannel, ["wf::ready", "wf::result"])
                     ]),
      testCase "without the push a signal still wakes a local waiter" $ do
        withNotifier $ \notifier -> do
          subscription <- subscribe notifier.registry (eventKey "wf" "ready")
          signal notifier eventsChannel "wf" "ready"
          drained notifier >>= (@?= [])
          waitFor (notified subscription),
      testCase "a flush pushes the queued payloads and drains them" $ do
        warnings <- newIORef []
        withNotifierLogging (LogAction (\message -> modifyIORef' warnings (message :))) $ \notifier -> do
          enable notifier
          signal notifier eventsChannel "wf" "ready"
          flush notifier
          drained notifier >>= (@?= [])
          -- The push is dropped on failure, so a warning is the only trace a
          -- bad statement would leave; none means pg_notify ran clean.
          readIORef warnings >>= (@?= []),
      testCase "the pushed payload is the key a waiter holds" $ do
        withNotifier $ \notifier -> do
          enable notifier
          signal notifier eventsChannel "wf" "ready"
          batches <- drained notifier
          case batches of
            [(channel, payloads)] -> case payloads of
              [payload] -> keyFor channel payload @?= Just (eventKey "wf" "ready")
              other -> fail ("expected one payload, got: " <> show other)
            other -> fail ("expected one channel's batch, got: " <> show other)
    ]

-- | A notifier over a live pool. Nothing here flushes, so the pool is only
-- held; the registry is fresh per case so wakes never cross tests.
withNotifier :: (Notifier -> IO a) -> IO a
withNotifier = withNotifierLogging nullLogger

-- | A notifier whose warnings land in the given logger, so a test can see
-- what the flush loop swallowed.
withNotifierLogging :: LogAction IO Text -> (Notifier -> IO a) -> IO a
withNotifierLogging logger action = do
  config <- configFromEnv
  bracket (acquirePostgresSystemDB config nullLogger) (Pool.release . (.psdbPool)) $ \env -> do
    registry <- newRegistry
    notifier <- notifierNew env.psdbPool registry Nothing logger
    action notifier

-- | The notifier's retry warnings go nowhere in tests.
nullLogger :: LogAction IO Text
nullLogger = LogAction (const (pure ()))

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
