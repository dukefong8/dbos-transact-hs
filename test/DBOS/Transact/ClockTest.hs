{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Durable waits against a database whose clock is not this process's.
--
-- A sleep's wake time and a receive's or a read's deadline are recorded
-- once and replayed by whichever executor recovers the workflow, which may
-- be another host with another clock. So the instant is set on the
-- database's clock, and every wait for it is measured from a reading of
-- that clock. These tests run the waits over a backend whose connections
-- read the clock an hour off this process's: an executor waiting against
-- its own wall clock would sleep an hour too long, or not at all. Mirrors
-- the oracle's @clock.rs@.
module DBOS.Transact.ClockTest (tests) where

import DBOS.Internal.Prelude
import Data.Int (Int64)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Data.Word (Word64)
import DBOS.SystemDB
  ( GetEventCaller (..),
    Outcome (..),
    WorkflowId (..),
    WorkflowIdReusePolicy (..),
    WorkflowOwner (..),
    checkStep,
    getEvent,
    initWorkflow,
    millisDuration,
    newWorkflow,
    recordSleep,
    recv,
    sleepStepName,
    timestampNow,
    timestampToEpochMs,
  )
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact (sleepStep)
import DBOS.Internal.Transact.Context (withWorkflow)
import DBOS.Transact.ContextTest (connOver)
import DBOS.Internal.Transact.Identity (Identity (..))
import DBOS.Internal.Transact.Logger (nullTracer)
import Hasql.Connection.Settings qualified as ConnectionSettings
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Pool qualified as Pool
import Hasql.Pool.Config qualified as PoolConfig
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import Pqi.Ffi qualified as Pqi
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

-- | How long each replayed wait is set to last.
waitMillis :: Int64
waitMillis = 3000

-- | A backend over the suite database whose connections read a clock
-- @skewSeconds@ off this process's, tearing the skew down afterwards. A
-- private pool (so the search path touches no other test) whose
-- connections put the skew schema first: shadowing @now()@ and
-- @clock_timestamp()@ through @search_path@ rather than altering the
-- database, as the oracle's skewed database does. Mirrors the oracle's
-- @skewed_database@, minus the migrate step — the suite database is
-- already migrated, and Haskell never migrates.
withSkewedBackend :: IO Postgres.PostgresSystemDB -> Int -> (Postgres.PostgresSystemDB -> IO a) -> IO a
withSkewedBackend getBackend skewSeconds action = do
  base <- getBackend
  url <- (.configUrl) <$> Postgres.configFromEnv
  tag <- (Text.filter (/= '-') . Text.pack . UUID.toString) <$> UUID.V4.nextRandom
  let schema = "skew_" <> tag
      offset = Text.pack (show skewSeconds) <> " seconds"
      ddl =
        [ "CREATE SCHEMA " <> schema,
          "CREATE FUNCTION " <> schema <> ".now() RETURNS timestamptz LANGUAGE sql STABLE AS $$ SELECT pg_catalog.now() + interval '" <> offset <> "' $$",
          "CREATE FUNCTION " <> schema <> ".clock_timestamp() RETURNS timestamptz LANGUAGE sql VOLATILE AS $$ SELECT pg_catalog.clock_timestamp() + interval '" <> offset <> "' $$"
        ]
  mapM_
    ( \statement -> Postgres.runSession base "clock-skew-ddl" (Session.script statement) >>= either (fail . show) pure
    )
    ddl
  -- A connection opened from here on reads the offset clock; prove it
  -- before running anything timed, as the oracle proves its skew.
  pool <-
    Pool.acquire
      Pqi.adapter
      ( PoolConfig.settings
          [ PoolConfig.size 4,
            PoolConfig.acquisitionTimeout 10,
            PoolConfig.staticConnectionSettings (ConnectionSettings.connectionString url),
            PoolConfig.initSession (Session.script ("SET search_path = " <> schema <> ", pg_catalog, public"))
          ]
      )
  bracket (Postgres.fromPool pool 4 Postgres.defaultSettings nullTracer) Postgres.releasePostgresSystemDB $ \env -> do
    processNow <- timestampToEpochMs <$> timestampNow
    off <- (\now -> now - processNow) <$> dbNowMs env
    assertBool ("the database's clock is " <> show off <> "ms off, not " <> show (skewSeconds * 1000)) (abs (off - fromIntegral skewSeconds * 1000) < 5000)
    result <- action env
    Postgres.runSession base "clock-skew-ddl" (Session.script ("DROP SCHEMA " <> schema <> " CASCADE")) >>= either (fail . show) pure
    pure result

-- | The database's clock now, in epoch milliseconds, through the given
-- backend (whose clock may be skewed).
dbNowMs :: Postgres.PostgresSystemDB -> IO Int64
dbNowMs env = do
  now <-
    Postgres.runSession env "clock-skew-dbnow" $
      Session.statement () $
        Statement.preparable
          "SELECT (EXTRACT(epoch FROM clock_timestamp()) * 1000)::bigint"
          Encoders.noParams
          (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))
  case now of
    Left err -> fail (show err)
    Right epochMs -> pure epochMs

-- | Runs @wait@, asserting it took about 'waitMillis': the lower bound
-- catches a wait that ended before the deadline on the database's clock,
-- and the upper one a reading that fell behind the database's clock, say
-- by a wait for a lock. Mirrors the oracle's @waits_on_the_database_clock@.
waitsOnTheDatabaseClock :: IO a -> IO a
waitsOnTheDatabaseClock wait = do
  (answer, elapsed) <- boundedWait wait
  assertBool ("the wait ended after (ns): " <> show elapsed <> ", before the deadline on the database's clock") (elapsed >= 2500000000)
  assertBool ("the wait ended after (ns): " <> show elapsed <> ", well past the deadline on the database's clock") (elapsed < 6000000000)
  pure answer

-- | Runs @wait@ with a bound well short of an hour: a wait timed on this
-- process's clock instead of the database's fails here rather than hanging
-- the suite. Returns the answer with the nanoseconds it took on this
-- process's monotonic clock.
boundedWait :: IO a -> IO (a, Word64)
boundedWait wait = do
  before <- getMonotonicTimeNSec
  answer <- timeout 30000000 wait >>= maybe (fail "the wait went by this process's clock, an hour from the database's") pure
  after <- getMonotonicTimeNSec
  pure (answer, after - before)

-- | A fresh workflow row on the skewed backend with its owner token, as an
-- execution built from the init result would present it.
skewedRow :: Postgres.PostgresSystemDB -> Text -> IO (Text, Text)
skewedRow env prefix = do
  unique <- (Text.filter (/= '-') . Text.pack . UUID.toString) <$> UUID.V4.nextRandom
  let widText = prefix <> unique
  created <- SystemDB.initWorkflow env ((newWorkflow widText) {SystemDB.newWorkflowName = Just "ClockTest"}) Nothing WorkflowIdReturnExisting
  case created of
    Left err -> fail (show err)
    Right result -> pure (widText, result.initResultCreatorXid)

-- | Re-records the wake time at @step@ as 'waitMillis' from the database's
-- now, as an executor on the database's clock would have recorded it.
-- Mirrors the oracle's @rerecord_wake_time@.
rerecordWakeTime :: Postgres.PostgresSystemDB -> Text -> Int -> IO ()
rerecordWakeTime env widText step = do
  updated <-
    Postgres.runSession env "clock-skew-rerecord" $
      Session.statement (widText, fromIntegral step, waitMillis) $
        Statement.preparable
          "UPDATE dbos.operation_outputs SET output = ((EXTRACT(epoch FROM clock_timestamp()) * 1000)::bigint + $3)::text WHERE workflow_uuid = $1 AND function_id = $2"
          (contramap (\(wid, _, _) -> wid) (Encoders.param (Encoders.nonNullable Encoders.text)) <> contramap (\(_, stepId, _) -> stepId) (Encoders.param (Encoders.nonNullable Encoders.int4)) <> contramap (\(_, _, wait) -> wait) (Encoders.param (Encoders.nonNullable Encoders.int8)))
          Decoders.rowsAffected
  case updated of
    Right 1 -> pure ()
    other -> fail ("no wake time to re-record: " <> show other)

-- | The wake time recorded at @step@: what the first run set the wait on.
recordedWakeTime :: Postgres.PostgresSystemDB -> Text -> Int -> IO Int64
recordedWakeTime env widText step = do
  found <- SystemDB.checkStep env (WorkflowId widText) step sleepStepName
  case found of
    Right (Just record) -> case record.stepRecordOutput >>= (readMaybe . Text.unpack . Text.strip) of
      Just epochMs -> pure epochMs
      Nothing -> fail "no wake time was recorded"
    other -> fail ("no wake time was recorded: " <> show other)

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    testGroup
      "Database-clock waits"
      [ testCase "a first sleep is timed on the database clock ahead" $ firstSleep getBackend 3600,
        testCase "a first sleep is timed on the database clock behind" $ firstSleep getBackend (-3600),
        testCase "a first recv waits its timeout on the database clock ahead" $ firstRecv getBackend 3600,
        testCase "a first recv waits its timeout on the database clock behind" $ firstRecv getBackend (-3600),
        testCase "a first get_event waits its timeout on the database clock ahead" $ firstGetEvent getBackend 3600,
        testCase "a first get_event waits its timeout on the database clock behind" $ firstGetEvent getBackend (-3600),
        testCase "a replayed sleep ends on time with the database clock ahead" $ replayedSleep getBackend 3600,
        testCase "a replayed sleep ends on time with the database clock behind" $ replayedSleep getBackend (-3600),
        testCase "a replayed recv times out on time with the database clock ahead" $ replayedRecv getBackend 3600,
        testCase "a replayed recv times out on time with the database clock behind" $ replayedRecv getBackend (-3600),
        testCase "a replayed get_event times out on time with the database clock ahead" $ replayedGetEvent getBackend 3600,
        testCase "a replayed get_event times out on time with the database clock behind" $ replayedGetEvent getBackend (-3600)
      ]

acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | A first run sets its wake time on the database's clock and waits it
-- out: a short engine sleep whose recorded wake is the timeout past the
-- database's now. Mirrors the oracle's first-sleep test.
firstSleep :: IO Postgres.PostgresSystemDB -> Int -> IO ()
firstSleep getBackend skewSeconds = withSkewedBackend getBackend skewSeconds $ \env -> do
  (widText, token) <- skewedRow env "clock-first-sleeper-"
  before <- dbNowMs env
  conn <- connOver env nullTracer
  (slept, _) <-
    boundedWait $
      withWorkflow conn clockIdentity (WorkflowId widText) Nothing (WorkflowOwner {ownerWorkflowId = WorkflowId widText, ownerToken = token}) $ \wctx ->
        sleepStep wctx (millisDuration 10)
  case slept of
    Left err -> fail (show err)
    Right () -> pure ()
  recorded <- recordedWakeTime env widText 0
  after <- dbNowMs env
  assertBool ("recorded " <> show recorded <> ", not 10ms past the database's now (" <> show before <> "..=" <> show after <> ")") (recorded >= before + 10 && recorded <= after + 10)

-- | A first recv waits its timeout on the skewed clock and records the
-- absence at the database's deadline.
firstRecv :: IO Postgres.PostgresSystemDB -> Int -> IO ()
firstRecv getBackend skewSeconds = withSkewedBackend getBackend skewSeconds $ \env -> do
  (widText, token) <- skewedRow env "clock-first-receiver-"
  received <-
    waitsOnTheDatabaseClock $
      SystemDB.recv env (WorkflowId widText) 0 1 (Just "nothing-sent") (millisDuration 3000) token
  received @?= Right Nothing

-- | A first get_event waits its timeout on the skewed clock.
firstGetEvent :: IO Postgres.PostgresSystemDB -> Int -> IO ()
firstGetEvent getBackend skewSeconds = withSkewedBackend getBackend skewSeconds $ \env -> do
  (widText, token) <- skewedRow env "clock-first-reader-"
  let caller = GetEventCaller {getEventCallerWorkflowId = WorkflowId widText, getEventCallerStepId = 0, getEventCallerTimeoutStepId = 1, getEventCallerToken = token}
  read <-
    waitsOnTheDatabaseClock $
      SystemDB.getEvent env (WorkflowId widText) "unset" (millisDuration 3000) (Just caller)
  read @?= Right Nothing

-- | A replayed sleep ends at its wake time on the database's clock: a
-- short first sleep, its wake re-recorded, then the engine waits the
-- re-recorded instant out. Mirrors the oracle's replayed-sleep test.
replayedSleep :: IO Postgres.PostgresSystemDB -> Int -> IO ()
replayedSleep getBackend skewSeconds = withSkewedBackend getBackend skewSeconds $ \env -> do
  (widText, token) <- skewedRow env "clock-sleeper-"
  let owner = WorkflowOwner {ownerWorkflowId = WorkflowId widText, ownerToken = token}
  conn <- connOver env nullTracer
  first <-
    timeout 30000000 $
      withWorkflow conn clockIdentity (WorkflowId widText) Nothing owner $ \wctx ->
        sleepStep wctx (millisDuration 10)
  case first of
    Just (Right ()) -> pure ()
    other -> fail ("the first run waited on this process's clock, an hour from the database's: " <> show other)
  rerecordWakeTime env widText 0
  _ <-
    waitsOnTheDatabaseClock $
      withWorkflow conn clockIdentity (WorkflowId widText) Nothing owner $ \wctx ->
        sleepStep wctx (millisDuration 10)
  pure ()

-- | A receive replaying its deadline times out at it on the database's
-- clock: the deadline step recorded and re-recorded, then a 10ms recv
-- waits the re-recorded instant out. Mirrors the oracle's replayed-recv
-- test.
replayedRecv :: IO Postgres.PostgresSystemDB -> Int -> IO ()
replayedRecv getBackend skewSeconds = withSkewedBackend getBackend skewSeconds $ \env -> do
  (widText, token) <- skewedRow env "clock-receiver-"
  recordDeadline env widText token
  received <-
    waitsOnTheDatabaseClock $
      SystemDB.recv env (WorkflowId widText) 0 1 (Just "nothing-sent") (millisDuration 10) token
  received @?= Right Nothing

-- | A read of an event replaying its deadline times out at it on the
-- database's clock, as a receive does.
replayedGetEvent :: IO Postgres.PostgresSystemDB -> Int -> IO ()
replayedGetEvent getBackend skewSeconds = withSkewedBackend getBackend skewSeconds $ \env -> do
  (widText, token) <- skewedRow env "clock-reader-"
  recordDeadline env widText token
  let caller = GetEventCaller {getEventCallerWorkflowId = WorkflowId widText, getEventCallerStepId = 0, getEventCallerTimeoutStepId = 1, getEventCallerToken = token}
  read <-
    waitsOnTheDatabaseClock $
      SystemDB.getEvent env (WorkflowId widText) "unset" (millisDuration 10) (Just caller)
  read @?= Right Nothing

-- | Records a deadline at step 1, then re-records it 'waitMillis' from the
-- database's now, as an executor on the database's clock would have
-- recorded it before a recovery. Mirrors the oracle's @record_deadline@.
recordDeadline :: Postgres.PostgresSystemDB -> Text -> Text -> IO ()
recordDeadline env widText token = do
  recorded <- SystemDB.recordSleep env (WorkflowId widText) 1 (millisDuration 10) token
  case recorded of
    Left err -> fail (show err)
    Right _ -> pure ()
  rerecordWakeTime env widText 1

clockIdentity :: Identity
clockIdentity =
  Identity
    { identityAppName = "test-app",
      identityAppVersion = "1.0.0",
      identityExecutorId = "test-executor",
      identityAppId = ""
    }
