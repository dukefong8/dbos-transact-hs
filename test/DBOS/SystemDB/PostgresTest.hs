{-# LANGUAGE OverloadedStrings #-}

module DBOS.SystemDB.PostgresTest (tests, streamTests) where

import DBOS.Prelude
import Data.Functor.Contravariant (contramap)
import DBOS.SystemDB
  ( Applications (..),
    AwaitedOutcome (..),
    BackendError (..),
    BackendErrorKind (..),
    Error (..),
    Change (..),
    Debounce (..),
    DebounceRequest (..),
    NewWorkflow (..),
    NewQueue (..),
    NewSchedule (..),
    OnExistingQueue (..),
    Outcome (..),
    OutcomeWrite (..),
    RetryPolicy (..),
    EncodedValue (..),
    EventRecord (..),
    Fork (..),
    ForkPoint (..),
    forkNew,
    debounceDelayedWorkflow,
    getDeduplicationKeyHolder,
    getLatestApplicationVersion,
    invalidInput,
    isPushing,
    getQueue,
    getQueuePartitions,
    GetEventCaller (..),
    listQueues,
    newQueue,
    recordChildResult,
    recordChildWorkflow,
    startQueuedPartitionedWorkflows,
    startQueuedWorkflows,
    updateQueue,
    upsertQueue,
    deleteQueue,
    IdempotencyKey (..),
    InitWorkflowCaller (..),
    SendMessage (..),
    SerializedWorkflowValue (..),
    Serialization (..),
    NotificationRecord (..),
    Topic (..),
    VersionInfo (..),
    StepRecord (..),
    StepTiming (..),
    StreamRead (..),
    StreamRecord (..),
    Submission (..),
    SystemDB (..),
    WrittenBy (..),
    WorkflowDelay (..),
    WorkflowFilter (..),
    WorkflowId (..),
    WorkflowInitResult (..),
    WorkflowRecord (..),
    WorkflowStatus (..),
    ApplicationRowCounts (..),
    QueueRecord (..),
    QueueUpdate (..),
    RenameBatching (..),
    ScheduleFilter (..),
    ScheduleRecord (..),
    ScheduleStatus (..),
    ScheduleUpdate (..),
    defaultQueueUpdate,
    defaultScheduleFilter,
    defaultScheduleUpdate,
    RenameFrom (..),
    defaultForkOptions,
    defaultRetryPolicy,
    defaultWorkflowFilter,
    messageTo,
    newSchedule,
    millisDuration,
    newWorkflow,
    secondsDuration,
    sleepStepName,
    streamClosedSentinel,
    timestampFromEpochMs,
    timestampNow,
    timestampToEpochMs,
  )
import DBOS.SystemDB.Postgres
  ( Config (..),
    DbosMigration (..),
    PostgresSystemDB (..),
    Settings (..),
    acquirePostgresSystemDB,
    activatePostgresSystemDB,
    classifyUsageError,
    configFromEnv,
    configNew,
    defaultSettings,
    fromPool,
    isForeignKeyViolation,
    isTransportFailure,
    isUniqueViolation,
    migrationVersionSession,
    pollingLimit,
    releasePostgresSystemDB,
    runSession,
    verifySystemDatabase,
  )
import Data.Int (Int64)
import Data.List (sort)
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.Transact (nullTracer)
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Errors qualified as Errors
import Hasql.Pool qualified as Pool
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

-- | Backend tests for the @postgres.rs@ rewrite (Phase 7). Groups land
-- here one at a time, red-first: pure oracle ports
-- (@isTransportFailure@, @pollingLimit@) need no database; lifecycle and
-- method tests run against the live database from @DBOS_DATABASE_URL@.
-- Every ported case mirrors its Rust test name so the mapping is visible,
-- and step 3 of the loop re-checks each assertion against @postgres.rs@.
tests :: TestTree
tests =
  withResource acquireSuiteBackend releasePostgresSystemDB $ \getBackend ->
  testGroup
    "Postgres"
    [ transportTests,
      pollingTests,
      classifyTests,
      configTests,
      lifecycleTests getBackend,
      schemaTests getBackend,
      workflowTests getBackend,
      childrenTests getBackend,
      listTests getBackend,
      awaitTests getBackend,
      outcomeTests getBackend,
      initTests getBackend,
      recoveryTests getBackend,
      stepTests getBackend,
      sleepTests getBackend,
      eventTests getBackend,
      messageTests getBackend,
      lifecycleWriteTests getBackend,
      forkTests getBackend,
      renameTests getBackend,
      versionTests getBackend,
      queueTests getBackend,
      scheduleTests getBackend
    ]

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted. No
-- activation here, exactly as before: each case sees an acquired backend,
-- and settings variants wrap the same pool through 'fromPool'. The
-- backend's retry and notifier warnings go nowhere: nullTracer.
acquireSuiteBackend :: IO PostgresSystemDB
acquireSuiteBackend = do
  config <- configFromEnv
  acquirePostgresSystemDB config nullTracer

-- | A handle acting for a named application, over the suite pool.
withBackendAs :: IO PostgresSystemDB -> Maybe Text -> (PostgresSystemDB -> IO a) -> IO a
withBackendAs getBackend appName = withBackendSettings getBackend (defaultSettings {settingsApplicationName = appName})

-- | A handle with the given settings, over the suite pool: 'fromPool'
-- wraps the shared pool, so no case opens connections of its own.
withBackendSettings :: IO PostgresSystemDB -> Settings -> (PostgresSystemDB -> IO a) -> IO a
withBackendSettings getBackend settings action = do
  base <- getBackend
  env <- fromPool base.psdbPool 8 settings nullTracer
  action env

-- | Shared fixtures: every live case runs over the suite backend (nothing
-- is acquired per test) and owns its rows through fresh workflow ids, so
-- the group stays safe under tasty's parallel runner.
withBackend :: IO PostgresSystemDB -> (PostgresSystemDB -> IO a) -> IO a
withBackend getBackend action = getBackend >>= action

-- | A workflow id no other test can hold.
freshWorkflowId :: IO Text
freshWorkflowId = UUID.toText <$> UUID.V4.nextRandom

-- | A stored row: the @NOT NULL@ columns plus whatever the read tests need
-- to exercise. The @init_workflow@ port (P7.3) replaces this once it lands.
data FixtureRow = FixtureRow
  { fixtureId :: Text,
    fixtureStatus :: Text,
    fixtureName :: Maybe Text,
    fixtureParent :: Maybe Text,
    fixtureForkedFrom :: Maybe Text,
    fixtureApplication :: Maybe Text,
    fixtureQueue :: Maybe Text,
    fixtureCreatedAt :: Int64,
    fixtureInput :: Maybe Text,
    fixtureOutput :: Maybe Text,
    fixtureError :: Maybe Text,
    fixtureSerialization :: Maybe Text,
    fixtureDeduplicationId :: Maybe Text,
    fixturePartitionKey :: Maybe Text,
    fixtureIsDebounced :: Bool,
    fixtureExecutor :: Maybe Text,
    fixtureOwnerXid :: Maybe Text,
    fixtureRecoveryAttempts :: Maybe Int64,
    fixtureVersion :: Maybe Text,
    fixtureDelayUntil :: Maybe Int64
  }

-- | Everything but the id, ready to override per case.
defaultFixture :: Text -> FixtureRow
defaultFixture wid =
  FixtureRow
    { fixtureId = wid,
      fixtureStatus = "PENDING",
      fixtureName = Just "fixture",
      fixtureParent = Nothing,
      fixtureForkedFrom = Nothing,
      fixtureApplication = Nothing,
      fixtureQueue = Nothing,
      fixtureCreatedAt = 0,
      fixtureInput = Nothing,
      fixtureOutput = Nothing,
      fixtureError = Nothing,
      fixtureSerialization = Nothing,
      fixtureDeduplicationId = Nothing,
      fixturePartitionKey = Nothing,
      fixtureIsDebounced = False,
      fixtureExecutor = Nothing,
      fixtureOwnerXid = Nothing,
      fixtureRecoveryAttempts = Nothing,
      fixtureVersion = Nothing,
      fixtureDelayUntil = Nothing
    }

-- | A fixture that owns its rows: the application stamp keeps another
-- application's listener from claiming it. An unclaimed row belongs to
-- every application (the claim's filter), so a raw queued fixture can be
-- taken by any instance in the suite draining all queues; the suite's
-- own sweeps run with no application and still see every row.
ownedFixture :: Text -> FixtureRow
ownedFixture unique = (defaultFixture unique) {fixtureApplication = Just ("fixture-" <> unique)}

-- | Inserts a fixture row. The tests own their ids, so this never collides
-- with another test's rows.
insertFixture :: FixtureRow -> Session.Session ()
insertFixture row =
  Session.script
    ( "insert into dbos.workflow_status (workflow_uuid, status, name, created_at, updated_at, priority, was_forked_from, rate_limited, is_debounced, parent_workflow_id, forked_from, application_name, queue_name, inputs, output, error, serialization, deduplication_id, queue_partition_key, executor_id, owner_xid, recovery_attempts, application_version, delay_until_epoch_ms) values ('"
        <> row.fixtureId
        <> "', '"
        <> row.fixtureStatus
        <> "', "
        <> quoted row.fixtureName
        <> ", "
        <> Text.pack (show row.fixtureCreatedAt)
        <> ", "
        <> Text.pack (show row.fixtureCreatedAt)
        <> ", 0, false, false, "
        <> (if row.fixtureIsDebounced then "true" else "false")
        <> ", "
        <> quoted row.fixtureParent
        <> ", "
        <> quoted row.fixtureForkedFrom
        <> ", "
        <> quoted row.fixtureApplication
        <> ", "
        <> quoted row.fixtureQueue
        <> ", "
        <> quoted row.fixtureInput
        <> ", "
        <> quoted row.fixtureOutput
        <> ", "
        <> quoted row.fixtureError
        <> ", "
        <> quoted row.fixtureSerialization
        <> ", "
        <> quoted row.fixtureDeduplicationId
        <> ", "
        <> quoted row.fixturePartitionKey
        <> ", "
        <> quoted row.fixtureExecutor
        <> ", "
        <> quoted row.fixtureOwnerXid
        <> ", "
        <> maybe "0" (Text.pack . show) row.fixtureRecoveryAttempts
        <> ", "
        <> quoted row.fixtureVersion
        <> ", "
        <> maybe "null" (Text.pack . show) row.fixtureDelayUntil
        <> ")"
    )
  where
    quoted :: Maybe Text -> Text
    quoted = maybe "null" (\value -> "'" <> value <> "'")

-- | Seeds a finished row split across the two payload layouts: the status
-- row carries the legacy payloads, and @workflow_input@/@workflow_output@
-- carry the new ones. A missing new payload means no row there, the way a
-- row written before migration 109 looks. Mirrors the oracle's
-- payload-table seeds.
seedPayloadRow :: Text -> Text -> (Maybe Text, Maybe Text, Maybe Text) -> (Maybe Text, Maybe Text, Maybe Text) -> Session.Session ()
seedPayloadRow wid status (legacyInput, legacyOutput, legacyError) (newInput, newOutput, newError) =
  Session.script
    ( "insert into dbos.workflow_status (workflow_uuid, status, name, created_at, updated_at, priority, was_forked_from, rate_limited, is_debounced, recovery_attempts, inputs, output, error) values ('"
        <> wid
        <> "', '"
        <> status
        <> "', 'fixture', 0, 0, 0, false, false, false, 0, "
        <> lit legacyInput
        <> ", "
        <> lit legacyOutput
        <> ", "
        <> lit legacyError
        <> "); "
        <> "insert into dbos.workflow_input (workflow_uuid, inputs) values ('"
        <> wid
        <> "', "
        <> lit newInput
        <> ") on conflict (workflow_uuid) do nothing; "
        <> "insert into dbos.workflow_output (workflow_uuid, output, error) values ('"
        <> wid
        <> "', "
        <> lit newOutput
        <> ", "
        <> lit newError
        <> ") on conflict (workflow_uuid) do nothing"
    )
  where
    lit = maybe "null" (\value -> "'" <> value <> "'")

-- | The payload-table rows for one workflow: the input, then the output
-- and error. Raw SQL, the way the oracle's payload tests read them.
payloadRows :: PostgresSystemDB -> Text -> IO (Maybe Text, Maybe Text, Maybe Text)
payloadRows env wid = do
  input <- readInput
  (output, errorValue) <- readOutcome
  pure (input, output, errorValue)
  where
    readInput = do
      result <-
        runSession env "fixture" $
          Session.statement wid $
            Statement.preparable
              "select p.inputs from dbos.workflow_input p where p.workflow_uuid = $1"
              (Encoders.param (Encoders.nonNullable Encoders.text))
              (Decoders.rowMaybe (Decoders.column (Decoders.nullable Decoders.text)))
      case result of
        Left err -> fail ("payload read failed: " <> show err)
        Right Nothing -> pure Nothing
        Right (Just value) -> pure value
    readOutcome = do
      result <-
        runSession env "fixture" $
          Session.statement wid $
            Statement.preparable
              "select o.output, o.error from dbos.workflow_output o where o.workflow_uuid = $1"
              (Encoders.param (Encoders.nonNullable Encoders.text))
              (Decoders.rowMaybe ((,) <$> Decoders.column (Decoders.nullable Decoders.text) <*> Decoders.column (Decoders.nullable Decoders.text)))
      case result of
        Left err -> fail ("payload read failed: " <> show err)
        Right Nothing -> pure (Nothing, Nothing)
        Right (Just pair) -> pure pair

-- | The legacy @workflow_status@ payload columns for one workflow, the way
-- the oracle's payload tests read them.
legacyPayloads :: PostgresSystemDB -> Text -> IO (Maybe Text, Maybe Text, Maybe Text)
legacyPayloads env wid = do
  result <-
    runSession env "fixture" $
      Session.statement wid $
        Statement.preparable
          "select w.inputs, w.output, w.error from dbos.workflow_status w where w.workflow_uuid = $1"
          (Encoders.param (Encoders.nonNullable Encoders.text))
          (Decoders.rowMaybe ((,,) <$> Decoders.column (Decoders.nullable Decoders.text) <*> Decoders.column (Decoders.nullable Decoders.text) <*> Decoders.column (Decoders.nullable Decoders.text)))
  case result of
    Left err -> fail ("legacy payload read failed: " <> show err)
    Right Nothing -> pure (Nothing, Nothing, Nothing)
    Right (Just triple) -> pure triple

-- | How many rows one table holds for a workflow. The table name is a test
-- constant interpolated into the query, the way the fixture inserts are.
tableRowCount :: PostgresSystemDB -> Text -> Text -> IO Int64
tableRowCount env table wid = do
  result <-
    runSession env "fixture" $
      Session.statement wid $
        Statement.preparable
          ("select count(*) from dbos." <> table <> " where workflow_uuid = $1")
          (Encoders.param (Encoders.nonNullable Encoders.text))
          (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))
  case result of
    Left err -> fail ("row count failed: " <> show err)
    Right count -> pure count

-- | Deletes a fixture row by id, so a replay test can prove the answer came
-- from the recorded step and not from a second look at the row.
deleteFixture :: PostgresSystemDB -> Text -> IO ()
deleteFixture env wid = do
  result <- runSession env "fixture" (Session.script ("delete from dbos.workflow_status where workflow_uuid = '" <> wid <> "'"))
  case result of
    Left err -> fail ("fixture delete failed: " <> show err)
    Right () -> pure ()

-- | A fixture insert whose failure is the test's failure: a silent
-- collision (a shared queue or deduplication key) would otherwise look like
-- a missing row.
insertFixtureChecked :: PostgresSystemDB -> FixtureRow -> IO ()
insertFixtureChecked env row = do
  result <- runSession env "fixture" (insertFixture row)
  case result of
    Left err -> fail ("fixture insert failed: " <> show err)
    Right () -> pure ()

-- | Mirrors @postgres.rs@ @a_dead_socket_reads_as_a_transport_failure@: the
-- message CockroachDB attaches to @XXUUU@ when a client's connection dies
-- mid-read, which is what makes that code a connection failure rather than
-- an internal one.
transportTests :: TestTree
transportTests =
  testGroup
    "Transport failures"
    [ testCase "a dead socket reads as a transport failure" $ do
        assertBool "read i/o timeout" (isTransportFailure "read tcp 172.17.0.3:26257->172.17.0.1:37026: i/o timeout")
        assertBool "write broken pipe" (isTransportFailure "write tcp 172.17.0.3:26257->172.17.0.1:37026: broken pipe")
        assertBool "read reset by peer" (isTransportFailure "read tcp 10.0.0.2:26257->10.0.0.9:5432: connection reset by peer")
        assertBool "closed network connection" (isTransportFailure "use of closed network connection"),
      -- Mirrors @transport_text_matches_whatever_its_case@: case is the
      -- server's business, not ours.
      testCase "transport text matches whatever its case" $
        assertBool "I/O timeout uppercased" (isTransportFailure "read tcp: I/O timeout"),
      -- Mirrors @a_real_internal_error_is_not_a_transport_failure@: @XXUUU@
      -- is a catch-all, so everything else carrying it is a genuine
      -- internal error.
      testCase "a real internal error is not a transport failure" $ do
        assertBool "not a variable assignment" (not (isTransportFailure "internal error: expected LHS of assignment to be a variable"))
        assertBool "not a corrupted index" (not (isTransportFailure "index corrupted: duplicate key"))
        assertBool "not empty" (not (isTransportFailure ""))
    ]

-- | Mirrors @postgres.rs@ polling-cap tests. Rust's off value is
-- @tokio Semaphore::MAX_PERMITS@; the Haskell port's inexhaustible count is
-- 'maxBound', recorded as a deviation in ADR-0010's hub (typedSql/fixed
-- schema section covers identifier rendering; this one covers the counter).
pollingTests :: TestTree
pollingTests =
  testGroup
    "Polling cap"
    [ -- Mirrors @the_default_polling_cap_is_half_the_pool@.
      testCase "the default polling cap is half the pool" $ do
        pollingLimit Nothing 10 @?= 5
        pollingLimit Nothing 21 @?= 10,
      -- Mirrors @a_pool_too_small_to_halve_still_admits_one_poll@: half of
      -- one is zero, which as a cap would block every poll forever.
      testCase "a pool too small to halve still admits one poll" $ do
        pollingLimit Nothing 1 @?= 1
        pollingLimit Nothing 0 @?= 1,
      -- Mirrors @a_configured_polling_cap_is_taken_as_given@: above the pool
      -- size is the caller's business; neither reference rejects it either.
      testCase "a configured polling cap is taken as given" $ do
        pollingLimit (Just 3) 10 @?= 3
        pollingLimit (Just 100) 10 @?= 100,
      -- Mirrors @zero_switches_the_polling_cap_off@: off is a permit count
      -- nothing will exhaust, not an absent semaphore.
      testCase "zero switches the polling cap off" $
        pollingLimit (Just 0) 10 @?= maxBound
    ]

-- | The failure classification behind the retry loop. There are no Rust
-- unit tests for @classify@ itself, so these cases are verified against its
-- documented table (@postgres.rs@ classify docs) instead: @40@ transient,
-- @08@/@53@/@57@ connection, @XX@ by message, everything else permanent,
-- and no-SQLSTATE failures by driver variant.
classifyTests :: TestTree
classifyTests =
  testGroup
    "Classification"
    [ testCase "serialization and deadlock failures are transient" $ do
        kindOf (serverUsage "40001" "could not serialize access") @?= Transient
        kindOf (serverUsage "40P01" "deadlock detected") @?= Transient,
      testCase "connection-class codes are connection failures" $ do
        kindOf (serverUsage "08000" "connection exception") @?= Connection
        kindOf (serverUsage "53300" "too many connections") @?= Connection
        kindOf (serverUsage "57000" "cannot connect now") @?= Connection,
      testCase "XXUUU with a transport message is a connection failure" $
        kindOf (serverUsage "XXUUU" "read tcp 10.0.0.1:26257->10.0.0.2:5432: i/o timeout") @?= Connection,
      testCase "XXUUU with an internal message is permanent" $
        kindOf (serverUsage "XXUUU" "internal error: expected LHS of assignment to be a variable") @?= Permanent,
      testCase "anything else the server reports is permanent" $ do
        kindOf (serverUsage "23505" "duplicate key value") @?= Permanent
        kindOf (serverUsage "42P01" "relation does not exist") @?= Permanent,
      testCase "a lost connection never reached the database" $
        kindOf (Pool.SessionUsageError (Errors.ConnectionSessionError "connection reset")) @?= Connection,
      testCase "an acquisition timeout is a connection failure" $
        kindOf Pool.AcquisitionTimeoutUsageError @?= Connection,
      testCase "networking failures are connection failures" $
        kindOf (Pool.ConnectionUsageError (Errors.NetworkingConnectionError "timeout")) @?= Connection,
      testCase "authentication and compatibility failures are permanent" $ do
        kindOf (Pool.ConnectionUsageError (Errors.AuthenticationConnectionError "bad password")) @?= Permanent
        kindOf (Pool.ConnectionUsageError (Errors.CompatibilityConnectionError "old server")) @?= Permanent,
      testCase "driver and script failures are permanent" $ do
        kindOf (Pool.SessionUsageError (Errors.DriverSessionError "bug")) @?= Permanent
        kindOf (Pool.SessionUsageError (Errors.ScriptSessionError "select 1" (serverError "42601" "syntax error"))) @?= Permanent,
      testCase "the SQLSTATE and message survive classification" $
        case classifyUsageError (serverUsage "40001" "could not serialize access") of
          Backend backend -> do
            backend.backendSqlState @?= Just "40001"
            assertBool "message mentions the failure" (Text.isInfixOf "could not serialize access" backend.backendMessage)
          other -> fail ("expected Backend, got: " <> show other),
      testCase "unique and foreign-key violations read by code" $ do
        assertBool "23505 is a unique violation" (isUniqueViolation (sessionError "23505"))
        assertBool "23503 is not a unique violation" (not (isUniqueViolation (sessionError "23503")))
        assertBool "23503 is a foreign-key violation" (isForeignKeyViolation (sessionError "23503"))
        assertBool "23505 is not a foreign-key violation" (not (isForeignKeyViolation (sessionError "23505")))
        assertBool "no code is neither" (not (isUniqueViolation (Errors.ConnectionSessionError "down")) && not (isForeignKeyViolation (Errors.ConnectionSessionError "down")))
    ]
  where
    kindOf usage = case classifyUsageError usage of
      Backend backend -> backend.backendKind
      other -> error ("expected Backend, got: " <> show other)
    serverUsage code message =
      Pool.SessionUsageError (Errors.StatementSessionError 1 0 "select 1" [] True (Errors.ServerStatementError (serverError code message)))
    serverError code message = Errors.ServerError code message Nothing Nothing Nothing
    sessionError code = Errors.StatementSessionError 1 0 "select 1" [] True (Errors.ServerStatementError (serverError code "x"))

-- | 'Settings' and 'Config' carry the documented defaults: the @dbos@
-- schema, the shared retry policy, no executor or application identity, no
-- polling cap; 'configNew' sets the URL and ten connections. Mirrors Rust
-- @Settings::default@ and @Config::new@.
configTests :: TestTree
configTests =
  testGroup
    "Configuration"
    [ testCase "defaults match every implementation" $
        defaultSettings @?= Settings "dbos" defaultRetryPolicy Nothing Nothing Nothing Nothing,
      testCase "configNew takes the URL with shared defaults" $ do
        let config = configNew "postgresql://u:p@host:5432/db"
        config.configUrl @?= "postgresql://u:p@host:5432/db"
        config.configMaxConnections @?= 10
        config.configSettings @?= defaultSettings
    ]

-- | Lifecycle against the live database: acquire, verify, run, reject. The
-- tests own no rows (verify and the probe only read the migration table),
-- so they run safely alongside anything else.
lifecycleTests :: IO PostgresSystemDB -> TestTree
lifecycleTests getBackend =
  testGroup
    "Lifecycle"
    [ testCase "acquires and verifies the migrated database" $ do
        config <- configFromEnv
        bracket (acquirePostgresSystemDB config nullTracer) (Pool.release . (.psdbPool)) $ \env -> do
          verified <- verifySystemDatabase env
          verified @?= Right ()
          permits <- readTVarIO env.psdbPollingPermits
          permits @?= pollingLimit Nothing config.configMaxConnections,
      testCase "runs a session through the pool" $
        withBackend getBackend $ \env -> do
          result <- runSession env "probe" migrationVersionSession
          result @?= Right (Just (DbosMigration 114)),
      testCase "rejects a non-dbos schema" $
        withBackend getBackend $ \env -> do
          outcome <- try (fromPool env.psdbPool 8 (defaultSettings {settingsSchema = "other"}) nullTracer)
          case outcome of
            Left (InvalidInput {}) -> pure ()
            _ -> fail "expected InvalidInput",
      testCase "reports a dead database instead of hanging" $ do
        let offline =
              (configNew "postgresql://127.0.0.1:1/none")
                { configSettings = defaultSettings {settingsRetry = defaultRetryPolicy {retryPolicyRetryConnectionErrors = False}}
                }
        outcome <- try (acquirePostgresSystemDB offline nullTracer)
        case outcome of
          Left (Backend backend) -> backend.backendKind @?= Connection
          _ -> fail "expected Backend",
      testCase "activate then close stops the loop and releases the pool" $ do
        config <- configFromEnv
        env <- acquirePostgresSystemDB config nullTracer
        activatePostgresSystemDB env
        pushing <- isPushing env.psdbNotifier
        assertBool "pushing after activate" pushing
        _ <- close env
        pure ()
    ]

-- | Live schema contract, folded in from the parked @DBOS.SchemaTest@
-- (2026-09-29): the Python DBOS table set and the columns Haskell's pinned
-- queries touch, read from @information_schema@ rather than asserted
-- against copies of themselves. Owns no rows, so these run safely alongside
-- anything else; the migration-ceiling probe above pins the version this
-- contract was recorded against.
schemaTests :: IO PostgresSystemDB -> TestTree
schemaTests getBackend =
  testGroup
    "Schema"
    [ testCase "records the live Python DBOS table set used by Haskell tests" $
        withBackend getBackend $ \env -> do
          tables <- probeSchemaTables env
          assertBool ("contract tables present, got: " <> show tables) (all (`elem` tables) expectedTables),
      testCase "records the workflow_status columns used by Haskell tests" $
        withBackend getBackend $ \env -> do
          columns <- probeSchemaColumns env "workflow_status"
          assertBool "workflow_status contract columns are present" (all (`elem` columns) requiredWorkflowStatusColumns),
      testCase "records the operation_outputs columns used by Haskell tests" $
        withBackend getBackend $ \env -> do
          columns <- probeSchemaColumns env "operation_outputs"
          assertBool "operation_outputs contract columns are present" (all (`elem` columns) requiredOperationOutputsColumns),
      testCase "records the notifications columns used by Haskell tests" $
        withBackend getBackend $ \env -> do
          columns <- probeSchemaColumns env "notifications"
          assertBool "notifications contract columns are present" (all (`elem` columns) requiredNotificationsColumns),
      testCase "the migrated enqueue function writes inputs to the payload table" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          called <-
            runSession env "fixture" $
              Session.statement (unique, "q-" <> Text.take 8 unique, unique <> "-sql") $
                Statement.preparable
                  "select dbos.enqueue_workflow($1::text, $2::text, array['7'::json], '{}'::json, null, null, $3::text)"
                  ( contramap (\(name, _, _) -> name) (Encoders.param (Encoders.nonNullable Encoders.text))
                      <> contramap (\(_, queue, _) -> queue) (Encoders.param (Encoders.nonNullable Encoders.text))
                      <> contramap (\(_, _, wid) -> wid) (Encoders.param (Encoders.nonNullable Encoders.text))
                  )
                  (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.text)))
          case called of
            Left err -> fail ("enqueue function failed: " <> show err)
            Right wid -> do
              wid @?= unique <> "-sql"
              legacyPayloads env wid >>= (@?= (Nothing, Nothing, Nothing))
              (input, _, _) <- payloadRows env wid
              input @?= Just "{\"positionalArgs\" : [7], \"namedArgs\" : {}}"
    ]

-- | Base tables in the backend schema, names ascending.
probeSchemaTables :: PostgresSystemDB -> IO [Text]
probeSchemaTables env = do
  result <-
    runSession env "schema-tables" $
      Session.statement () $
        Statement.preparable
          "select table_name::text from information_schema.tables where table_schema = 'dbos' and table_type = 'BASE TABLE' order by table_name"
          Encoders.noParams
          (Decoders.rowList (Decoders.column (Decoders.nonNullable Decoders.text)))
  case result of
    Left err -> fail (show err)
    Right tables -> pure tables

-- | Column names of one backend table, ascending.
probeSchemaColumns :: PostgresSystemDB -> Text -> IO [Text]
probeSchemaColumns env table = do
  result <-
    runSession env "schema-columns" $
      Session.statement table $
        Statement.preparable
          "select column_name::text from information_schema.columns where table_schema = 'dbos' and table_name = $1 order by column_name"
          (Encoders.param (Encoders.nonNullable Encoders.text))
          (Decoders.rowList (Decoders.column (Decoders.nonNullable Decoders.text)))
  case result of
    Left err -> fail (show err)
    Right columns -> pure columns

expectedTables :: [Text]
expectedTables =
  [ "application_versions",
    "dbos_migrations",
    "event_dispatch_kv",
    "notifications",
    "operation_outputs",
    "queues",
    "streams",
    "workflow_events",
    "workflow_events_history",
    "workflow_input",
    "workflow_output",
    "workflow_schedules",
    "workflow_status"
  ]

requiredWorkflowStatusColumns :: [Text]
requiredWorkflowStatusColumns =
  [ "workflow_uuid",
    "status",
    "inputs",
    "serialization",
    "request",
    "attributes",
    "parent_workflow_id",
    "recovery_attempts",
    "forked_from"
  ]

requiredOperationOutputsColumns :: [Text]
requiredOperationOutputsColumns =
  [ "workflow_uuid",
    "function_id",
    "function_name",
    "child_workflow_id",
    "started_at_epoch_ms",
    "completed_at_epoch_ms",
    "serialization"
  ]

requiredNotificationsColumns :: [Text]
requiredNotificationsColumns =
  [ "destination_uuid",
    "topic",
    "message",
    "message_uuid",
    "serialization",
    "consumed"
  ]

-- | Reads against rows the tests own: each case inserts a fresh workflow id
-- through a raw fixture (the @init_workflow@ port lands in P7.3) and reads
-- it back through the class method. Mirrors @get_workflow@'s contract: a
-- stored row reads back whole, a missing id reads 'Nothing', and a row with
-- an unknown status is 'Malformed'.
workflowTests :: IO PostgresSystemDB -> TestTree
workflowTests getBackend =
  testGroup
    "Workflows"
    [ testCase "reads a stored workflow" $ do
        wid <- freshWorkflowId
        withBackend getBackend $ \env -> do
          fixture <- runSession env "fixture" (insertFixture (defaultFixture wid))
          fixture @?= Right ()
          found <- getWorkflow env (WorkflowId wid)
          case found of
            Left err -> fail ("expected record, got: " <> show err)
            Right Nothing -> fail "expected record, got Nothing"
            Right (Just record) -> do
              record.workflowRecordId @?= WorkflowId wid
              record.workflowRecordStatus @?= Pending
              record.workflowRecordName @?= Just "fixture"
              record.workflowRecordCreatedAt @?= timestampFromEpochMs 0
              record.workflowRecordUpdatedAt @?= timestampFromEpochMs 0
              record.workflowRecordRecoveryAttempts @?= 0
              record.workflowRecordPriority @?= 0
              record.workflowRecordWasForkedFrom @?= False
              record.workflowRecordRateLimited @?= False
              record.workflowRecordIsDebounced @?= False
              record.workflowRecordInput @?= Nothing,
      testCase "reads payloads from the payload tables, falling back to legacy columns" $ do
        base <- freshWorkflowId
        let newOnly = base <> "-new"
            legacyOk = base <> "-legacy-ok"
            legacyErr = base <> "-legacy-err"
            both = base <> "-both"
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (seedPayloadRow newOnly "SUCCESS" (Nothing, Nothing, Nothing) (Just "\"new in\"", Just "\"new out\"", Nothing))
          _ <- runSession env "fixture" (seedPayloadRow legacyOk "SUCCESS" (Just "\"old in\"", Just "\"old out\"", Nothing) (Nothing, Nothing, Nothing))
          _ <- runSession env "fixture" (seedPayloadRow legacyErr "ERROR" (Just "\"old in\"", Nothing, Just "\"old err\"") (Nothing, Nothing, Nothing))
          _ <- runSession env "fixture" (seedPayloadRow both "SUCCESS" (Just "\"legacy in\"", Just "\"legacy out\"", Nothing) (Just "\"new in\"", Just "\"new out\"", Nothing))
          checkPayloads env newOnly (Just "\"new in\"") (Just "\"new out\"") Nothing
          checkPayloads env legacyOk (Just "\"old in\"") (Just "\"old out\"") Nothing
          checkPayloads env legacyErr (Just "\"old in\"") Nothing (Just "\"old err\"")
          checkPayloads env both (Just "\"new in\"") (Just "\"new out\"") Nothing,
      testCase "returns Nothing for a missing id" $ do
        wid <- freshWorkflowId
        withBackend getBackend $ \env -> do
          found <- getWorkflow env (WorkflowId wid)
          found @?= Right Nothing,
      testCase "rejects a row with an unknown status" $ do
        wid <- freshWorkflowId
        withBackend getBackend $ \env -> do
          fixture <- runSession env "fixture" (insertFixture (defaultFixture wid) {fixtureStatus = "BOGUS"})
          fixture @?= Right ()
          found <- getWorkflow env (WorkflowId wid)
          case found of
            Left (Malformed _) -> pure ()
            _ -> fail "expected Malformed"
    ]
  where
    checkPayloads env wid input output errorValue = do
      found <- getWorkflow env (WorkflowId wid)
      case found of
        Left err -> fail ("expected record, got: " <> show err)
        Right Nothing -> fail "expected record, got Nothing"
        Right (Just record) -> do
          record.workflowRecordInput @?= input
          record.workflowRecordOutput @?= output
          record.workflowRecordError @?= errorValue

-- | Descendant walks over fixture trees: a root with two children, one of
-- which has a child of its own, plus a self-parented root (a workflow is
-- not its own descendant, and a cycle stops rather than loops). Mirrors
-- @get_workflow_children@: level-by-level breadth-first, deduplicated.
-- Within a level the database fixes no order, so assertions sort.
childrenTests :: IO PostgresSystemDB -> TestTree
childrenTests getBackend =
  testGroup
    "Children"
    [ testCase "finds every descendant at any depth" $ do
        root <- freshWorkflowId
        [child1, child2, grandchild] <- replicateM 3 freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture root))
          _ <- runSession env "fixture" (insertFixture (defaultFixture child1) {fixtureParent = Just root})
          _ <- runSession env "fixture" (insertFixture (defaultFixture child2) {fixtureParent = Just root})
          _ <- runSession env "fixture" (insertFixture (defaultFixture grandchild) {fixtureParent = Just child1})
          found <- getWorkflowChildren env (WorkflowId root)
          case found of
            Left err -> fail ("expected descendants, got: " <> show err)
            Right descendants -> sort (unId <$> descendants) @?= sort [child1, child2, grandchild],
      testCase "excludes the workflow itself" $ do
        root <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture root) {fixtureParent = Just root})
          found <- getWorkflowChildren env (WorkflowId root)
          found @?= Right [],
      testCase "returns empty for a leaf" $ do
        wid <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture wid))
          found <- getWorkflowChildren env (WorkflowId wid)
          found @?= Right []
    ]
  where
    unId (WorkflowId wid) = wid

-- | Reading workflows back through filters. Every case scopes its query to
-- its own rows (unique names, ids or prefixes), so the shared database
-- cannot contaminate an answer. Mirrors @list_workflows@ in @postgres.rs@:
-- ANDed guards, @LIKE ANY@ over escaped wildcards, application scoping, and
-- @created_at@ ordering that pages through a @limit@/@offset@.
listTests :: IO PostgresSystemDB -> TestTree
listTests getBackend =
  testGroup
    "Listing"
    [ testCase "narrows by id, name and status" $ do
        unique <- freshWorkflowId
        let ids = [unique <> "-1", unique <> "-2", unique <> "-3"]
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture (ids !! 0)) {fixtureName = Just (unique <> "-alpha"), fixtureStatus = "PENDING"})
          _ <- runSession env "fixture" (insertFixture (defaultFixture (ids !! 1)) {fixtureName = Just (unique <> "-beta"), fixtureStatus = "ENQUEUED"})
          _ <- runSession env "fixture" (insertFixture (defaultFixture (ids !! 2)) {fixtureName = Just (unique <> "-gamma"), fixtureStatus = "SUCCESS"})
          byId <- idsOf env (defaultWorkflowFilter {workflowFilterWorkflowIds = [ids !! 0]})
          byId @?= [ids !! 0]
          byName <- idsOf env (defaultWorkflowFilter {workflowFilterNames = [unique <> "-beta"]})
          byName @?= [ids !! 1]
          byStatus <- sort <$> idsOf env (defaultWorkflowFilter {workflowFilterNames = [unique <> "-beta", unique <> "-gamma"], workflowFilterStatus = [Success, Enqueued]})
          byStatus @?= sort [ids !! 1, ids !! 2],
      testCase "escapes LIKE wildcards in prefixes" $ do
        unique <- freshWorkflowId
        let literal = unique <> "_x"
            plain = unique <> "ax"
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture literal))
          _ <- runSession env "fixture" (insertFixture (defaultFixture plain))
          both <- sort <$> idsOf env (defaultWorkflowFilter {workflowFilterWorkflowIdPrefixes = [unique]})
          both @?= sort [literal, plain]
          literalOnly <- idsOf env (defaultWorkflowFilter {workflowFilterWorkflowIdPrefixes = [unique <> "_"]})
          literalOnly @?= [literal],
      testCase "orders by creation time in the asked direction" $ do
        unique <- freshWorkflowId
        let ids = [unique <> "-1", unique <> "-2", unique <> "-3"]
            stamp millis wid = (defaultFixture wid) {fixtureCreatedAt = millis}
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (stamp 2000 (ids !! 1)))
          _ <- runSession env "fixture" (insertFixture (stamp 3000 (ids !! 2)))
          _ <- runSession env "fixture" (insertFixture (stamp 1000 (ids !! 0)))
          let prefix = defaultWorkflowFilter {workflowFilterWorkflowIdPrefixes = [unique]}
          ascending <- idsOf env prefix
          ascending @?= ids
          descending <- idsOf env prefix {workflowFilterSortDesc = True}
          descending @?= reverse ids
          recent <- idsOf env prefix {workflowFilterCreatedAfter = Just (timestampFromEpochMs 2000)}
          recent @?= drop 1 ids,
      testCase "pages with limit and offset" $ do
        unique <- freshWorkflowId
        let ids = [unique <> "-1", unique <> "-2", unique <> "-3"]
            stamp millis wid = (defaultFixture wid) {fixtureCreatedAt = millis}
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (stamp 1000 (ids !! 0)))
          _ <- runSession env "fixture" (insertFixture (stamp 2000 (ids !! 1)))
          _ <- runSession env "fixture" (insertFixture (stamp 3000 (ids !! 2)))
          let prefix = defaultWorkflowFilter {workflowFilterWorkflowIdPrefixes = [unique]}
          firstPage <- idsOf env prefix {workflowFilterLimit = Just 1}
          firstPage @?= take 1 ids
          secondPage <- idsOf env prefix {workflowFilterLimit = Just 1, workflowFilterOffset = Just 1}
          secondPage @?= take 1 (drop 1 ids)
          tailPage <- idsOf env prefix {workflowFilterLimit = Just 2, workflowFilterOffset = Just 1}
          tailPage @?= drop 1 ids,
      testCase "declines payloads the filter does not ask for" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <-
            runSession
              env
              "fixture"
              ( insertFixture
                  (defaultFixture unique)
                    { fixtureInput = Just "in",
                      fixtureOutput = Just "out",
                      fixtureError = Just "boom",
                      fixtureSerialization = Just "json"
                    }
              )
          records <- listed env (defaultWorkflowFilter {workflowFilterWorkflowIds = [unique]})
          case records of
            [record] -> do
              record.workflowRecordInput @?= Just "in"
              record.workflowRecordOutput @?= Just "out"
              record.workflowRecordError @?= Just "boom"
              record.workflowRecordSerialization @?= Just "json"
            _ -> fail "expected one record"
          noInput <- listed env (defaultWorkflowFilter {workflowFilterWorkflowIds = [unique], workflowFilterLoadInput = False})
          case noInput of
            [record] -> do
              record.workflowRecordInput @?= Nothing
              record.workflowRecordOutput @?= Just "out"
            _ -> fail "expected one record"
          noOutput <- listed env (defaultWorkflowFilter {workflowFilterWorkflowIds = [unique], workflowFilterLoadOutput = False})
          case noOutput of
            [record] -> do
              record.workflowRecordInput @?= Just "in"
              record.workflowRecordOutput @?= Nothing
              record.workflowRecordError @?= Nothing
            _ -> fail "expected one record",
      testCase "lists payloads from the payload tables with legacy fallback" $ do
        base <- freshWorkflowId
        let newOnly = base <> "-new"
            legacyOk = base <> "-legacy-ok"
            both = base <> "-both"
            ids = [newOnly, legacyOk, both]
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (seedPayloadRow newOnly "SUCCESS" (Nothing, Nothing, Nothing) (Just "\"new in\"", Just "\"new out\"", Nothing))
          _ <- runSession env "fixture" (seedPayloadRow legacyOk "SUCCESS" (Just "\"old in\"", Just "\"old out\"", Nothing) (Nothing, Nothing, Nothing))
          _ <- runSession env "fixture" (seedPayloadRow both "SUCCESS" (Just "\"legacy in\"", Just "\"legacy out\"", Nothing) (Just "\"new in\"", Just "\"new out\"", Nothing))
          records <- listed env (defaultWorkflowFilter {workflowFilterWorkflowIds = ids})
          let payloadOf wid =
                [ (record.workflowRecordInput, record.workflowRecordOutput, record.workflowRecordError)
                  | record <- records,
                    record.workflowRecordId == WorkflowId wid
                ]
          payloadOf newOnly @?= [(Just "\"new in\"", Just "\"new out\"", Nothing)]
          payloadOf legacyOk @?= [(Just "\"old in\"", Just "\"old out\"", Nothing)]
          payloadOf both @?= [(Just "\"new in\"", Just "\"new out\"", Nothing)]
          declined <- listed env (defaultWorkflowFilter {workflowFilterWorkflowIds = ids, workflowFilterLoadInput = False, workflowFilterLoadOutput = False})
          forM_ declined $ \record -> do
            record.workflowRecordInput @?= Nothing
            record.workflowRecordOutput @?= Nothing
            record.workflowRecordError @?= Nothing,
      testCase "scopes applications: unset, named and any" $ do
        unique <- freshWorkflowId
        let claimed = unique <> "-claimed"
            unclaimed = unique <> "-unclaimed"
            mine = Just (unique <> "-app")
        withBackendAs getBackend Nothing $ \bare -> do
          _ <- runSession bare "fixture" (insertFixture (defaultFixture claimed) {fixtureApplication = mine})
          _ <- runSession bare "fixture" (insertFixture (defaultFixture unclaimed))
          named <- fromPool bare.psdbPool 8 (defaultSettings {settingsApplicationName = mine}) nullTracer
          let scoped env workflowFilter = idsOf env workflowFilter {workflowFilterWorkflowIds = [claimed, unclaimed]}
          -- A handle with no application of its own sees every application's
          -- rows rather than only the unclaimed ones.
          allRows <- scoped bare defaultWorkflowFilter
          sort allRows @?= sort [claimed, unclaimed]
          -- A named handle scopes to its name plus the unclaimed rows.
          own <- sort <$> scoped named defaultWorkflowFilter
          own @?= sort [claimed, unclaimed]
          other <- scoped named defaultWorkflowFilter {workflowFilterApplications = Named [unique <> "-elsewhere"]}
          other @?= [unclaimed]
          anyRows <- sort <$> scoped named defaultWorkflowFilter {workflowFilterApplications = Any}
          anyRows @?= sort [claimed, unclaimed],
      testCase "queues-only and parented flags narrow" $ do
        unique <- freshWorkflowId
        let queued = unique <> "-queued"
            plain = unique <> "-plain"
            rooted = unique <> "-rooted"
            unrooted = unique <> "-unrooted"
            ids = [queued, plain, rooted, unrooted]
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture queued) {fixtureQueue = Just "q"})
          _ <- runSession env "fixture" (insertFixture (defaultFixture plain))
          _ <- runSession env "fixture" (insertFixture (defaultFixture rooted) {fixtureParent = Just unique})
          _ <- runSession env "fixture" (insertFixture (defaultFixture unrooted))
          let scoped workflowFilter = idsOf env workflowFilter {workflowFilterWorkflowIds = ids}
          onlyQueued <- scoped defaultWorkflowFilter {workflowFilterQueuesOnly = True}
          onlyQueued @?= [queued]
          onlyParented <- sort <$> scoped defaultWorkflowFilter {workflowFilterHasParent = Just True}
          onlyParented @?= sort [rooted]
          onlyRoots <- sort <$> scoped defaultWorkflowFilter {workflowFilterHasParent = Just False}
          onlyRoots @?= sort [queued, plain, unrooted],
      testCase "forked flags narrow" $ do
        unique <- freshWorkflowId
        let forked = unique <> "-forked"
            plain = unique <> "-plain"
            ids = [forked, plain]
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture forked) {fixtureForkedFrom = Just unique})
          _ <- runSession env "fixture" (insertFixture (defaultFixture plain))
          let scoped workflowFilter = idsOf env workflowFilter {workflowFilterWorkflowIds = ids}
          onlyForked <- scoped defaultWorkflowFilter {workflowFilterIsFork = Just True}
          onlyForked @?= [forked]
          onlyRoots <- sort <$> scoped defaultWorkflowFilter {workflowFilterIsFork = Just False}
          onlyRoots @?= [plain]
    ]
  where
    listed env workflowFilter = do
      result <- listWorkflows env workflowFilter Nothing
      case result of
        Left err -> fail ("expected rows, got: " <> show err)
        Right records -> pure records
    idsOf env workflowFilter = map (unId . (.workflowRecordId)) <$> listed env workflowFilter
    unId (WorkflowId wid) = wid

-- | The blocking reads. Every case polls at 50ms so the tests stay fast, and
-- the ones that must observe a transition insert or update the row from a
-- concurrent thread after the wait has started — the real shape of a wait.
-- Mirrors @await_workflow_result@, @await_first_workflow_id@ and
-- @await_workflow_ids@: settled statuses report, unsettled poll, absence is
-- either an error or a wait depending on @fail_if_missing@.
awaitTests :: IO PostgresSystemDB -> TestTree
awaitTests getBackend =
  testGroup
    "Waits"
    [ testCase "a settled workflow reports how it did" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture unique) {fixtureStatus = "SUCCESS", fixtureOutput = Just "done", fixtureSerialization = Just "json"})
          settled <- await env unique
          settled @?= Right (AwaitedSucceeded (Just "done") (Just "json")),
      testCase "a failure reports its error" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture unique) {fixtureStatus = "ERROR", fixtureError = Just "boom"})
          settled <- await env unique
          settled @?= Right (AwaitedFailed "boom" Nothing),
      testCase "a failure with no error is malformed" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture unique) {fixtureStatus = "ERROR"})
          settled <- await env unique
          case settled of
            Left (Malformed _) -> pure ()
            _ -> fail "expected Malformed",
      testCase "settled payload-table and legacy rows report their outcome" $ do
        base <- freshWorkflowId
        let newOk = base <> "-new"
            legacyOk = base <> "-legacy-ok"
            legacyErr = base <> "-legacy-err"
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (seedPayloadRow newOk "SUCCESS" (Nothing, Nothing, Nothing) (Just "\"new in\"", Just "\"new out\"", Nothing))
          _ <- runSession env "fixture" (seedPayloadRow legacyOk "SUCCESS" (Just "\"old in\"", Just "\"old out\"", Nothing) (Nothing, Nothing, Nothing))
          _ <- runSession env "fixture" (seedPayloadRow legacyErr "ERROR" (Just "\"old in\"", Nothing, Just "\"old err\"") (Nothing, Nothing, Nothing))
          awaitedNew <- await env newOk
          awaitedNew @?= Right (AwaitedSucceeded (Just "\"new out\"") Nothing)
          awaitedLegacyOk <- await env legacyOk
          awaitedLegacyOk @?= Right (AwaitedSucceeded (Just "\"old out\"") Nothing)
          awaitedLegacyErr <- await env legacyErr
          awaitedLegacyErr @?= Right (AwaitedFailed "\"old err\"" Nothing),
      testCase "cancellation and parking are reported as values" $ do
        cancelled <- freshWorkflowId
        parked <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture cancelled) {fixtureStatus = "CANCELLED"})
          _ <- runSession env "fixture" (Session.script ("insert into dbos.workflow_status (workflow_uuid, status, name, created_at, updated_at, priority, was_forked_from, rate_limited, is_debounced, recovery_attempts) values ('" <> parked <> "', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED', 'fixture', 0, 0, 0, false, false, false, 7)"))
          cancelledOutcome <- await env cancelled
          cancelledOutcome @?= Right AwaitedCancelled
          parkedOutcome <- await env parked
          parkedOutcome @?= Right (AwaitedParked 7),
      testCase "a missing workflow fails fast when the caller says it saw the row" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          settled <- awaitWorkflowResult env (WorkflowId unique) (millisDuration 50) True
          case settled of
            Left (NonExistentWorkflow {workflowIds = [missing]}) -> missing @?= unique
            _ -> fail "expected NonExistentWorkflow",
      testCase "a wait polls until the row appears and settles" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          updater <- async $ do
            threadDelay 50000
            _ <- runSession env "fixture" (insertFixture (defaultFixture unique) {fixtureStatus = "SUCCESS", fixtureOutput = Just "late"})
            pure ()
          settled <- awaitWorkflowResult env (WorkflowId unique) (millisDuration 50) False
          wait updater
          settled @?= Right (AwaitedSucceeded (Just "late") Nothing),
      testCase "the first of a set reports whichever settled" $ do
        pending <- freshWorkflowId
        settling <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture pending) {fixtureStatus = "PENDING"})
          _ <- runSession env "fixture" (insertFixture (defaultFixture settling) {fixtureStatus = "ENQUEUED"})
          updater <- async $ do
            threadDelay 50000
            _ <- runSession env "fixture" (Session.script ("update dbos.workflow_status set status = 'SUCCESS' where workflow_uuid = '" <> settling <> "'"))
            pure ()
          winner <- awaitFirstWorkflowId env [WorkflowId pending, WorkflowId settling] (millisDuration 50)
          wait updater
          winner @?= Right (WorkflowId settling)
          empty <- awaitFirstWorkflowId env [] (millisDuration 50)
          case empty of
            Left (InvalidInput {}) -> pure ()
            _ -> fail "expected InvalidInput",
      testCase "waiting for every id returns once the last settles" $ do
        first <- freshWorkflowId
        second <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture first) {fixtureStatus = "SUCCESS"})
          _ <- runSession env "fixture" (insertFixture (defaultFixture second) {fixtureStatus = "PENDING"})
          updater <- async $ do
            threadDelay 50000
            _ <- runSession env "fixture" (Session.script ("update dbos.workflow_status set status = 'CANCELLED' where workflow_uuid = '" <> second <> "'"))
            pure ()
          -- A repeat is satisfied twice, and an empty wait is satisfied at once.
          done <- awaitWorkflowIds env [WorkflowId first, WorkflowId second, WorkflowId first] (millisDuration 50)
          wait updater
          done @?= Right ()
          empty <- awaitWorkflowIds env [] (millisDuration 50)
          empty @?= Right ()
    ]
  where
    await env unique = awaitWorkflowResult env (WorkflowId unique) (millisDuration 50) True

-- | Recording a terminal outcome. Mirrors @record_workflow_outcome@: the
-- @status = 'PENDING'@ gate is the whole mechanism — a superseded executor
-- updates nothing and learns it lost rather than clobbering the winner — and
-- finishing releases the deduplication key so it can be submitted again.
outcomeTests :: IO PostgresSystemDB -> TestTree
outcomeTests getBackend =
  testGroup
    "Outcomes"
    [ testCase "records an outcome once and frees the deduplication key" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture unique) {fixtureDeduplicationId = Just "key-1"})
          recorded <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeOutput (Just "done"))
          recorded @?= Right Recorded
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> do
              record.workflowRecordStatus @?= Success
              record.workflowRecordOutput @?= Just "done"
              record.workflowRecordError @?= Nothing
              record.workflowRecordDeduplicationId @?= Nothing
              assertBool "completed_at is stamped" (record.workflowRecordCompletedAt /= Nothing)
            other -> fail ("expected record, got: " <> show other),
      testCase "a second writer learns it lost" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture unique))
          first <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeOutput (Just "winner"))
          first @?= Right Recorded
          second <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeOutput (Just "loser"))
          second @?= Right AlreadyFinished
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> record.workflowRecordOutput @?= Just "winner"
            other -> fail ("expected record, got: " <> show other),
      testCase "a failure records its error and no output" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture unique))
          recorded <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeError "boom")
          recorded @?= Right Recorded
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> do
              record.workflowRecordStatus @?= Error
              record.workflowRecordError @?= Just "boom"
              record.workflowRecordOutput @?= Nothing
            other -> fail ("expected record, got: " <> show other),
      testCase "writes the outcome to the payload table, clearing legacy columns" $ do
        unique <- freshWorkflowId
        failed <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- initWorkflow env (newWorkflow unique) {newWorkflowInput = Just "\"in\""} Nothing Fresh Nothing
          recorded <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeOutput (Just "\"done\""))
          recorded @?= Right Recorded
          (input, output, errorValue) <- payloadRows env unique
          input @?= Just "\"in\""
          output @?= Just "\"done\""
          errorValue @?= Nothing
          legacyPayloads env unique >>= (@?= (Nothing, Nothing, Nothing))
          _ <- initWorkflow env (newWorkflow failed) {newWorkflowInput = Just "\"in\""} Nothing Fresh Nothing
          failedRecorded <- recordWorkflowOutcome env (WorkflowId failed) (OutcomeError "\"boom\"")
          failedRecorded @?= Right Recorded
          (_, failedOutput, failedError) <- payloadRows env failed
          failedOutput @?= Nothing
          failedError @?= Just "\"boom\""
          legacyPayloads env failed >>= (@?= (Nothing, Nothing, Nothing)),
      testCase "a refused outcome writes no payload" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- initWorkflow env (newWorkflow unique) {newWorkflowInput = Just "\"in\""} Nothing Fresh Nothing
          _ <- cancelWorkflows env [WorkflowId unique] False Nothing
          refused <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeOutput (Just "\"late\""))
          refused @?= Right AlreadyFinished
          (_, output, errorValue) <- payloadRows env unique
          output @?= Nothing
          errorValue @?= Nothing,
      testCase "a new outcome hides a legacy one" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <-
            runSession env "fixture" $
              Session.script ("insert into dbos.workflow_status (workflow_uuid, status, name, created_at, updated_at, priority, was_forked_from, rate_limited, is_debounced, recovery_attempts, error) values ('" <> unique <> "', 'PENDING', 'fixture', 0, 0, 0, false, false, false, 0, '\"first run failed\"')")
          recorded <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeOutput (Just "\"second run\""))
          recorded @?= Right Recorded
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> do
              record.workflowRecordOutput @?= Just "\"second run\""
              record.workflowRecordError @?= Nothing
            other -> fail ("expected record, got: " <> show other),
      testCase "no write path reaches the legacy payload columns" $ do
        base <- freshWorkflowId
        let direct = base <> "-direct"
            queued = base <> "-queued"
            parent = base <> "-parent"
            child = base <> "-child"
            failed = base <> "-failed"
            source = base <> "-source"
            forked = base <> "-forked"
        withBackend getBackend $ \env -> do
          _ <- initWorkflow env (newWorkflow direct) {newWorkflowInput = Just "\"1\""} Nothing Fresh Nothing
          _ <- recordWorkflowOutcome env (WorkflowId direct) (OutcomeOutput (Just "\"2\""))
          _ <- initWorkflow env (newWorkflow queued) {newWorkflowInput = Just "\"2\"", newWorkflowQueueName = Just ("q-" <> base)} Nothing Fresh Nothing
          _ <- recordWorkflowOutcome env (WorkflowId queued) (OutcomeOutput (Just "\"4\""))
          _ <- initWorkflow env (newWorkflow parent) Nothing Fresh Nothing
          startedAt <- timestampNow
          _ <-
            initWorkflow env (newWorkflow child) Nothing Fresh $
              Just
                ( InitWorkflowCaller
                    { initCallerParentWorkflowId = WorkflowId parent,
                      initCallerStepId = 1,
                      initCallerStepName = "ChildWorkflow",
                      initCallerStartedAt = startedAt
                    }
                )
          _ <- recordWorkflowOutcome env (WorkflowId child) (OutcomeOutput (Just "\"5\""))
          _ <- recordWorkflowOutcome env (WorkflowId parent) (OutcomeOutput (Just "\"10\""))
          _ <- initWorkflow env (newWorkflow failed) {newWorkflowInput = Just "\"f\""} Nothing Fresh Nothing
          _ <- recordWorkflowOutcome env (WorkflowId failed) (OutcomeError "\"boom\"")
          _ <- initWorkflow env (newWorkflow source) {newWorkflowInput = Just "\"s\""} Nothing Fresh Nothing
          _ <- recordWorkflowOutcome env (WorkflowId source) (OutcomeOutput (Just "\"s\""))
          _ <- forkWorkflows env [(forkNew source) {forkForkedId = Just forked}] defaultForkOptions Nothing
          _ <- recordWorkflowOutcome env (WorkflowId forked) (OutcomeOutput (Just "\"s\""))
          legacy <- legacyRows env (base <> "%")
          legacy @?= []
          missingInputs <- missingPayloadRows env (base <> "%") "workflow_input"
          missingInputs @?= []
          missingOutputs <- missingFinishedPayloadRows env (base <> "%")
          missingOutputs @?= []
    ]
  where
    legacyRows env pattern = do
      result <-
        runSession env "sweep" $
          Session.statement pattern $
            Statement.preparable
              "select s.workflow_uuid from dbos.workflow_status s where s.workflow_uuid like $1 and (s.inputs is not null or s.output is not null or s.error is not null)"
              (Encoders.param (Encoders.nonNullable Encoders.text))
              (Decoders.rowList (Decoders.column (Decoders.nonNullable Decoders.text)))
      case result of
        Left err -> fail ("legacy sweep failed: " <> show err)
        Right rows -> pure rows
    missingPayloadRows env pattern table = do
      result <-
        runSession env "sweep" $
          Session.statement pattern $
            Statement.preparable
              ("select s.workflow_uuid from dbos.workflow_status s where s.workflow_uuid like $1 and not exists (select 1 from dbos." <> table <> " p where p.workflow_uuid = s.workflow_uuid)")
              (Encoders.param (Encoders.nonNullable Encoders.text))
              (Decoders.rowList (Decoders.column (Decoders.nonNullable Decoders.text)))
      case result of
        Left err -> fail ("payload sweep failed: " <> show err)
        Right rows -> pure rows
    missingFinishedPayloadRows env pattern = do
      result <-
        runSession env "sweep" $
          Session.statement pattern $
            Statement.preparable
              "select s.workflow_uuid from dbos.workflow_status s where s.workflow_uuid like $1 and s.status in ('SUCCESS', 'ERROR') and not exists (select 1 from dbos.workflow_output o where o.workflow_uuid = s.workflow_uuid)"
              (Encoders.param (Encoders.nonNullable Encoders.text))
              (Decoders.rowList (Decoders.column (Decoders.nonNullable Decoders.text)))
      case result of
        Left err -> fail ("payload sweep failed: " <> show err)
        Right rows -> pure rows

-- | The queue registry: registration claims or inserts, reads scope the way
-- the oracle's do, and the deduplication key and partition reads answer from
-- the workflow rows. Mirrors @upsert_queue@, @get_queue@, @list_queues@,
-- @delete_queue@, @get_queue_partitions@ and @get_deduplication_key_holder@.
queueTests :: IO PostgresSystemDB -> TestTree
queueTests getBackend =
  testGroup
    "Queues"
    [ testCase "a queue round-trips through the registry" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          created <- upsertQueue env (newQueue name) UpdateExisting
          created @?= Right True
          found <- getQueue env name
          case found of
            Right (Just record) -> record.queueRecordName @?= name
            other -> fail ("expected the queue, got: " <> show other)
          _ <- deleteQueue env name
          gone <- getQueue env name
          gone @?= Right Nothing,
      testCase "re-registering a queue reports it existed" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          first <- upsertQueue env (newQueue name) UpdateExisting
          first @?= Right True
          second <- upsertQueue env (newQueue name) UpdateExisting
          second @?= Right False,
      testCase "listing queues scopes but reading one does not" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
            owner = "owner-" <> Text.take 8 unique
            stranger = "stranger-" <> Text.take 8 unique
        withBackendAs getBackend (Just owner) $ \env -> do
          _ <- upsertQueue env (newQueue name) UpdateExisting
          scoped <- listQueues env (Named [stranger])
          case scoped of
            Right records ->
              assertBool "another application's queue is not listed" (name `notElem` map (.queueRecordName) records)
            other' -> fail ("expected queues, got: " <> show other')
          mine <- listQueues env (Named [owner])
          case mine of
            Right records ->
              assertBool "the owning application's queue is listed" (name `elem` map (.queueRecordName) records)
            other' -> fail ("expected queues, got: " <> show other')
          found <- getQueue env name
          case found of
            Right (Just record) -> record.queueRecordName @?= name
            other' -> fail ("expected the queue, got: " <> show other'),
      testCase "the deduplication key holder is read by queue and key" $ do
        unique <- freshWorkflowId
        let queueName = "q-" <> Text.take 8 unique
            key = "dedup-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique) {fixtureQueue = Just queueName, fixtureDeduplicationId = Just key}
          holder <- getDeduplicationKeyHolder env queueName key
          holder @?= Right (Just (WorkflowId unique))
          absent <- getDeduplicationKeyHolder env queueName (key <> "-absent")
          absent @?= Right Nothing,
      testCase "queue partitions are the keys with work" $ do
        unique <- freshWorkflowId
        let queueName = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (ownedFixture unique) {fixtureQueue = Just queueName, fixtureStatus = "ENQUEUED", fixturePartitionKey = Just "p1"}
          _ <- insertFixtureChecked env (ownedFixture (unique <> "-2")) {fixtureQueue = Just queueName, fixtureStatus = "ENQUEUED", fixturePartitionKey = Just "p2"}
          _ <- insertFixtureChecked env (defaultFixture (unique <> "-3")) {fixtureQueue = Just queueName, fixtureStatus = "SUCCESS", fixturePartitionKey = Just "p3"}
          partitions <- getQueuePartitions env queueName
          partitions @?= Right ["p1", "p2"],
      testCase "an update changes only what it names" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- upsertQueue env (newQueue name) {newQueueConcurrency = Just 2, newQueueWorkerConcurrency = Just 1} UpdateExisting
          updated <- updateQueue env name (defaultQueueUpdate {queueUpdateConcurrency = Set (Just 4)}) (\_ _ -> Right ())
          case updated of
            Right record -> do
              record.queueRecordConcurrency @?= Just 4
              record.queueRecordWorkerConcurrency @?= Just 1
            other -> fail ("expected the updated queue, got: " <> show other),
      testCase "an empty update is a no-op" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- upsertQueue env (newQueue name) {newQueueConcurrency = Just 2} UpdateExisting
          stored <- updateQueue env name defaultQueueUpdate (\_ _ -> Left (invalidInput "validate" "must not be called"))
          case stored of
            Right record -> record.queueRecordConcurrency @?= Just 2
            other -> fail ("expected the stored queue, got: " <> show other),
      testCase "updating a queue that is not registered is refused" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          refused <- updateQueue env name (defaultQueueUpdate {queueUpdateConcurrency = Set (Just 4)}) (\_ _ -> Right ())
          case refused of
            Left (NotRegistered {}) -> pure ()
            other -> fail ("expected NotRegistered, got: " <> show other),
      testCase "a refused update writes nothing" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- upsertQueue env (newQueue name) {newQueueConcurrency = Just 2} UpdateExisting
          refused <- updateQueue env name (defaultQueueUpdate {queueUpdateConcurrency = Set (Just 9)}) (\_ _ -> Left (invalidInput "concurrency" "refused"))
          case refused of
            Left _ -> pure ()
            other -> fail ("expected a refusal, got: " <> show other)
          found <- getQueue env name
          case found of
            Right (Just record) -> record.queueRecordConcurrency @?= Just 2
            other -> fail ("expected the queue, got: " <> show other),
      testCase "worker concurrency bounds a dequeue" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- upsertQueue env (newQueue name) {newQueueWorkerConcurrency = Just 1} UpdateExisting
          _ <- insertFixtureChecked env (ownedFixture unique) {fixtureQueue = Just name, fixtureStatus = "ENQUEUED"}
          _ <- insertFixtureChecked env (ownedFixture (unique <> "-2")) {fixtureQueue = Just name, fixtureStatus = "ENQUEUED"}
          record <- registeredQueue env name
          version <- latestVersion env
          claimed <- startQueuedWorkflows env record "exec" version Nothing 0 0
          fmap length claimed @?= Right 1
          blocked <- startQueuedWorkflows env record "exec" version Nothing 1 0
          blocked @?= Right [],
      testCase "an empty partition key is refused" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- upsertQueue env (newQueue name) UpdateExisting
          record <- registeredQueue env name
          refused <- startQueuedWorkflows env record "exec" "v1" (Just "") 0 0
          case refused of
            Left (InvalidInput {}) -> pure ()
            other -> fail ("expected InvalidInput, got: " <> show other),
      testCase "the dequeue claim flips the row to pending" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- upsertQueue env (newQueue name) UpdateExisting
          _ <- insertFixtureChecked env (ownedFixture unique) {fixtureQueue = Just name, fixtureStatus = "ENQUEUED"}
          record <- registeredQueue env name
          version <- latestVersion env
          claimed <- startQueuedWorkflows env record "exec" version Nothing 0 0
          claimed @?= Right [WorkflowId unique]
          readBack <- getWorkflow env (WorkflowId unique)
          case readBack of
            Right (Just row) -> row.workflowRecordStatus @?= Pending
            other -> fail ("expected the workflow, got: " <> show other),
      testCase "a claim only sees its own application's rows" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
            foreignApp = "foreign-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- upsertQueue env (newQueue name) UpdateExisting
          version <- latestVersion env
          _ <- insertFixtureChecked env (ownedFixture unique) {fixtureQueue = Just name, fixtureStatus = "ENQUEUED", fixtureVersion = Just version}
          -- A listener of another application skips the scoped row: the
          -- claim's filter admits its own application's rows and
          -- unclaimed ones only.
          withBackendSettings getBackend (defaultSettings {settingsApplicationName = Just foreignApp}) $ \foreignEnv -> do
            foreignRecord <- registeredQueue foreignEnv name
            foreignClaim <- startQueuedWorkflows foreignEnv foreignRecord "foreign-exec" version Nothing 0 0
            foreignClaim @?= Right []
          -- The application the row names claims it.
          withBackendSettings getBackend (defaultSettings {settingsApplicationName = Just ("fixture-" <> unique)}) $ \ownerEnv -> do
            ownerRecord <- registeredQueue ownerEnv name
            ownerClaim <- startQueuedWorkflows ownerEnv ownerRecord "owner-exec" version Nothing 0 0
            ownerClaim @?= Right [WorkflowId unique],
      testCase "a partition sweep takes one head per partition" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- upsertQueue env (newQueue name) {newQueuePartitionConcurrency = Just 1} UpdateExisting
          _ <- insertFixtureChecked env (ownedFixture unique) {fixtureQueue = Just name, fixtureStatus = "ENQUEUED", fixturePartitionKey = Just "p1"}
          _ <- insertFixtureChecked env (ownedFixture (unique <> "-2")) {fixtureQueue = Just name, fixtureStatus = "ENQUEUED", fixturePartitionKey = Just "p2"}
          record <- registeredQueue env name
          version <- latestVersion env
          swept <- startQueuedPartitionedWorkflows env record "exec" version Nothing
          fmap length swept @?= Right 2
          again <- startQueuedPartitionedWorkflows env record "exec" version Nothing
          again @?= Right [],
      testCase "a bounce extends the delayed holder and an unmatched key is unheld" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
            key = "dedup-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (ownedFixture unique) {fixtureName = Just "bouncer", fixtureQueue = Just name, fixtureDeduplicationId = Just key, fixtureStatus = "DELAYED", fixtureIsDebounced = True, fixtureDelayUntil = Just 1000}
          bounced <- debounceDelayedWorkflow env (bounceRequest name key) Nothing
          bounced @?= Right (Debounced {debounceWorkflowId = unique})
          unheld <- debounceDelayedWorkflow env (bounceRequest name (key <> "-absent")) Nothing
          unheld @?= Right DebounceUnheld,
      testCase "a bounce writes the inputs to the payload table" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
            key = "dedup-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (ownedFixture unique) {fixtureName = Just "bouncer", fixtureQueue = Just name, fixtureDeduplicationId = Just key, fixtureStatus = "DELAYED", fixtureIsDebounced = True, fixtureDelayUntil = Just 1000}
          bounced <- debounceDelayedWorkflow env (bounceRequest name key) Nothing
          bounced @?= Right (Debounced {debounceWorkflowId = unique})
          (input, _, _) <- payloadRows env unique
          input @?= Just "{\"n\":2}"
          legacyPayloads env unique >>= (@?= (Nothing, Nothing, Nothing)),
      testCase "a bounce refuses an empty queue name" $ do
        withBackend getBackend $ \env -> do
          refused <- debounceDelayedWorkflow env (bounceRequest "" "dedup") Nothing
          case refused of
            Left (InvalidInput {}) -> pure ()
            other -> fail ("expected InvalidInput, got: " <> show other),
      testCase "recording the launch twice with the same child is idempotent" $ do
        parent <- freshWorkflowId
        child <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture parent)
          first <- recordChildWorkflow env (WorkflowId parent) (WorkflowId child) 1 "DBOS.child" Nothing
          first @?= Right ()
          again <- recordChildWorkflow env (WorkflowId parent) (WorkflowId child) 1 "DBOS.child" Nothing
          again @?= Right (),
      testCase "a different child at the same step is nondeterminism" $ do
        parent <- freshWorkflowId
        child <- freshWorkflowId
        rival <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture parent)
          _ <- recordChildWorkflow env (WorkflowId parent) (WorkflowId child) 1 "DBOS.child" Nothing
          refused <- recordChildWorkflow env (WorkflowId parent) (WorkflowId rival) 1 "DBOS.child" Nothing
          case refused of
            Left (StepAlreadyRecorded {}) -> pure ()
            other -> fail ("expected StepAlreadyRecorded, got: " <> show other),
      testCase "an empty child id is refused, not wedged" $ do
        parent <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture parent)
          refused <- recordChildWorkflow env (WorkflowId parent) (WorkflowId "") 1 "DBOS.child" Nothing
          case refused of
            Left (InvalidInput {}) -> pure ()
            other -> fail ("expected InvalidInput, got: " <> show other),
      testCase "a child result records under the result step name" $ do
        parent <- freshWorkflowId
        child <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture parent)
          recorded <- recordChildResult env (WorkflowId parent) 2 (WorkflowId child) (OutcomeOutput (Just "done")) Nothing Nothing
          recorded @?= Right ()
          found <- checkStep env (WorkflowId parent) 2 "DBOS.getResult"
          case found of
            Right (Just _) -> pure ()
            other -> fail ("expected the recorded step, got: " <> show other),
      testCase "starting a child records the parent link atomically" $ do
        parent <- freshWorkflowId
        child <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture parent)
          startedAt <- timestampNow
          let caller =
                InitWorkflowCaller
                  { initCallerParentWorkflowId = WorkflowId parent,
                    initCallerStepId = 1,
                    initCallerStepName = "ChildWorkflow",
                    initCallerStartedAt = startedAt
                  }
          started <- initWorkflow env (newWorkflow child) Nothing Fresh (Just caller)
          case started of
            Left err -> fail ("expected a child start, got: " <> show err)
            Right _ -> pure ()
          children <- getWorkflowChildren env (WorkflowId parent)
          children @?= Right [WorkflowId child]
          -- The parent step holds the same child: a rival link is refused.
          rival <- freshWorkflowId
          refused <- recordChildWorkflow env (WorkflowId parent) (WorkflowId rival) 1 "ChildWorkflow" Nothing
          case refused of
            Left (StepAlreadyRecorded {}) -> pure ()
            other -> fail ("expected StepAlreadyRecorded, got: " <> show other),
      testCase "a bounce inside a workflow is a step and replays" $ do
        parent <- freshWorkflowId
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
            key = "dedup-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture parent)
          _ <- insertFixtureChecked env (ownedFixture unique) {fixtureName = Just "bouncer", fixtureQueue = Just name, fixtureDeduplicationId = Just key, fixtureStatus = "DELAYED", fixtureIsDebounced = True, fixtureDelayUntil = Just 1000}
          first <- debounceDelayedWorkflow env (bounceRequest name key) (Just (WorkflowId parent, 7))
          first @?= Right (Debounced {debounceWorkflowId = unique})
          -- The row is gone, so a second run can only answer from the step.
          _ <- deleteFixture env unique
          replayed <- debounceDelayedWorkflow env (bounceRequest name key) (Just (WorkflowId parent, 7))
          replayed @?= Right (Debounced {debounceWorkflowId = unique})
    ]

-- | The registered record for a queue this test just created.
registeredQueue :: PostgresSystemDB -> Text -> IO QueueRecord
registeredQueue env name = do
  found <- getQueue env name
  case found of
    Right (Just record) -> pure record
    other -> fail ("expected the queue, got: " <> show other)

-- | The newest version this handle can see: passing it as the caller's
-- version makes the dequeue's version predicate admit unversioned rows.
latestVersion :: PostgresSystemDB -> IO Text
latestVersion env = do
  latest <- getLatestApplicationVersion env Nothing
  case latest of
    Right (Just info) -> pure info.versionInfoName
    other -> fail ("expected a latest version, got: " <> show other)

-- | A bounce request naming one delayed holder.
bounceRequest :: Text -> Text -> DebounceRequest
bounceRequest queueName key =
  DebounceRequest
    { debounceRequestWorkflowName = "bouncer",
      debounceRequestClassName = Nothing,
      debounceRequestConfigName = Nothing,
      debounceRequestQueueName = queueName,
      debounceRequestDeduplicationId = key,
      debounceRequestDelayUntil = timestampFromEpochMs 2000,
      debounceRequestInputs = Just "{\"n\":2}",
      debounceRequestSerialization = Nothing,
      debounceRequestApplicationName = Nothing
    }

-- | Application versions: registering one claims a nameless row or inserts
-- a new one, the listing and latest read the handle's own application plus
-- the unclaimed, and a timestamp update moves the latest. Mirrors
-- @create_application_version@, @list_application_versions@,
-- @get_latest_application_version@ and
-- @update_application_version_timestamp@.
versionTests :: IO PostgresSystemDB -> TestTree
versionTests getBackend =
  testGroup
    "Application versions"
    [ testCase "a registered version is listed and is the latest" $ do
        unique <- freshWorkflowId
        let versionName = "v-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          created <- createApplicationVersion env versionName Nothing
          created @?= Right ()
          versions <- listApplicationVersions env
          case versions of
            Right found -> assertBool "the version is listed" (versionName `elem` map (.versionInfoName) found)
            other -> fail ("expected versions, got: " <> show other)
          moved <- updateApplicationVersionTimestamp env versionName (timestampFromEpochMs 4102444800000) Nothing
          moved @?= Right ()
          -- The shared database holds other applications' versions, so the
          -- assertion is about our own row's timestamp rather than about
          -- which row is globally newest.
          refreshed <- listApplicationVersions env
          case refreshed of
            Right found -> do
              let ours = filter (\info -> info.versionInfoName == versionName) found
              map (.versionInfoTimestamp) ours @?= [timestampFromEpochMs 4102444800000]
            other -> fail ("expected versions, got: " <> show other)
          -- Restore a timestamp that can never be latest: a far-future
          -- stamp left behind would poison every scoped latest-version
          -- query on the shared database (and with it, every
          -- UpdateIfLatestVersion registration).
          restored <- updateApplicationVersionTimestamp env versionName (timestampFromEpochMs 0) Nothing
          restored @?= Right ()
          anyLatest <- getLatestApplicationVersion env Nothing
          case anyLatest of
            Right (Just _) -> pure ()
            other -> fail ("expected some latest, got: " <> show other),
      testCase "a version held by another application is reported" $ do
        unique <- freshWorkflowId
        let versionName = "v-" <> Text.take 8 unique
            otherApp = "other-" <> Text.take 8 unique
            mine = "mine-" <> Text.take 8 unique
        withBackendSettings getBackend (defaultSettings {settingsApplicationName = Just mine}) $ \env -> do
          _ <- createApplicationVersion env versionName (Just otherApp)
          refused <- createApplicationVersion env versionName Nothing
          case refused of
            Left (RegisteredByAnother {}) -> pure ()
            other -> fail ("expected RegisteredByAnother, got: " <> show other)
    ]

-- | Renaming an application's rows: in-flight rows in one transaction,
-- terminal rows and steps in batches, with the counts the caller gets back.
-- Mirrors @rename_application@ (and @rename_application_in_batches@).
renameTests :: IO PostgresSystemDB -> TestTree
renameTests getBackend =
  testGroup
    "Renames"
    [ testCase "renaming moves both in-flight and terminal rows and counts them" $ do
        unique <- freshWorkflowId
        let appName = "app-" <> Text.take 8 unique
            newName = "new-" <> Text.take 8 unique
            inFlight = unique <> "-live"
            settled = unique <> "-done"
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture inFlight) {fixtureApplication = Just appName}
          _ <- insertFixtureChecked env (defaultFixture settled) {fixtureApplication = Just appName, fixtureStatus = "SUCCESS"}
          counts <- renameApplication env (RenameApplication appName) newName Unbatched
          counts @?= Right (ApplicationRowCounts 0 0 0 2 0)
          live <- getWorkflow env (WorkflowId inFlight)
          case live of
            Right (Just record) -> record.workflowRecordApplicationName @?= Just newName
            other -> fail ("expected record, got: " <> show other)
          done <- getWorkflow env (WorkflowId settled)
          case done of
            Right (Just record) -> record.workflowRecordApplicationName @?= Just newName
            other -> fail ("expected record, got: " <> show other),
      testCase "renaming the unclaimed leaves other applications alone" $ do
        unique <- freshWorkflowId
        let newName = "new-" <> Text.take 8 unique
            unclaimed = unique <> "-unclaimed"
            owned = unique <> "-owned"
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unclaimed)
          _ <- insertFixtureChecked env (defaultFixture owned) {fixtureApplication = Just "other-app"}
          counts <- renameApplication env RenameUnclaimed newName Unbatched
          case counts of
            Right rowCounts -> assertBool "the unclaimed row moved" (rowCounts.rowCountWorkflows >= 1)
            other -> fail ("expected counts, got: " <> show other)
          kept <- getWorkflow env (WorkflowId owned)
          case kept of
            Right (Just record) -> record.workflowRecordApplicationName @?= Just "other-app"
            other -> fail ("expected record, got: " <> show other),
      testCase "a batched rename moves the same rows" $ do
        unique <- freshWorkflowId
        let appName = "app-" <> Text.take 8 unique
            newName = "new-" <> Text.take 8 unique
            ids = [unique <> "-" <> Text.pack (show n) | n <- [1 :: Int, 2, 3]]
        withBackend getBackend $ \env -> do
          _ <- traverse_ (\wid -> insertFixtureChecked env (defaultFixture wid) {fixtureApplication = Just appName, fixtureStatus = "SUCCESS"}) ids
          counts <- renameApplication env (RenameApplication appName) newName (Batched 2)
          case counts of
            Right rowCounts -> rowCounts.rowCountWorkflows @?= 3
            other -> fail ("expected counts, got: " <> show other),
      testCase "an invalid name and the same name are refused" $ do
        unique <- freshWorkflowId
        let appName = "app-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          invalid <- renameApplication env (RenameApplication appName) "UPPER" Unbatched
          case invalid of
            Left (InvalidInput {}) -> pure ()
            other -> fail ("expected InvalidInput, got: " <> show other)
          same <- renameApplication env (RenameApplication appName) appName Unbatched
          case same of
            Left (InvalidInput {}) -> pure ()
            other -> fail ("expected InvalidInput, got: " <> show other)
    ]

-- | Forks: a new workflow inheriting the source's identity, queued on the
-- internal queue, pointed back at its source — and, for a start step past
-- the beginning, the source's history copied forward up to that step.
-- Mirrors @fork_workflows@ and @fork_from@.
forkTests :: IO PostgresSystemDB -> TestTree
forkTests getBackend =
  testGroup
    "Forks"
    [ testCase "a fork inherits the source and copies its history to the start step" $ do
        source <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture source) {fixtureStatus = "SUCCESS"}
          _ <- setEvent env (WorkflowId source) 1 "progress" "halfway" Nothing
          _ <- recordStep env (WorkflowId source) 2 "DBOS.step" (OutcomeOutput (Just "two")) Nothing Nothing
          _ <- recordStep env (WorkflowId source) 3 "DBOS.step" (OutcomeOutput (Just "three")) Nothing Nothing
          forked <- forkWorkflows env [(forkNew source) {forkStartStep = 2}] defaultForkOptions Nothing
          case forked of
            Left err -> fail ("expected a fork, got: " <> show err)
            Right [WorkflowId forkedId] -> do
              found <- getWorkflow env (WorkflowId forkedId)
              case found of
                Right (Just record) -> do
                  record.workflowRecordStatus @?= Enqueued
                  record.workflowRecordForkedFrom @?= Just (WorkflowId source)
                  record.workflowRecordQueueName @?= Just "_dbos_internal_queue"
                other -> fail ("expected the fork, got: " <> show other)
              copied <- listSteps env (WorkflowId forkedId) True Nothing Nothing Nothing
              case copied of
                Right steps -> map (.stepRecordStepId) steps @?= [1]
                other -> fail ("expected copied steps, got: " <> show other)
              event <- getEvent env (WorkflowId forkedId) "progress" (millisDuration 0) Nothing
              event @?= Right (Just (EncodedValue "halfway" Nothing))
              marked <- getWorkflow env (WorkflowId source)
              case marked of
                Right (Just record) -> record.workflowRecordWasForkedFrom @?= True
                other -> fail ("expected the source, got: " <> show other)
            other -> fail ("expected one fork, got: " <> show other),
      testCase "a fork copies the input to the payload table" $ do
        base <- freshWorkflowId
        let srcNew = base <> "-src-new"
            srcOld = base <> "-src-old"
            forkNewId = base <> "-fork-new"
            forkOldId = base <> "-fork-old"
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (seedPayloadRow srcNew "SUCCESS" (Nothing, Nothing, Nothing) (Just "\"new in\"", Just "\"new out\"", Nothing))
          _ <- runSession env "fixture" (seedPayloadRow srcOld "SUCCESS" (Just "\"legacy in\"", Just "\"legacy out\"", Nothing) (Nothing, Nothing, Nothing))
          forked <-
            forkWorkflows
              env
              [ (forkNew srcNew) {forkForkedId = Just forkNewId},
                (forkNew srcOld) {forkForkedId = Just forkOldId}
              ]
              defaultForkOptions
              Nothing
          case forked of
            Left err -> fail ("expected forks, got: " <> show err)
            Right _ -> do
              (newInput, _, _) <- payloadRows env forkNewId
              newInput @?= Just "\"new in\""
              (oldInput, _, _) <- payloadRows env forkOldId
              oldInput @?= Just "\"legacy in\""
              legacyPayloads env forkNewId >>= (@?= (Nothing, Nothing, Nothing))
              legacyPayloads env forkOldId >>= (@?= (Nothing, Nothing, Nothing))
              found <- getWorkflow env (WorkflowId forkOldId)
              case found of
                Right (Just record) -> record.workflowRecordInput @?= Just "\"legacy in\""
                other -> fail ("expected the fork, got: " <> show other),
      testCase "forking from the last step copies everything before it" $ do
        source <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture source) {fixtureStatus = "SUCCESS"}
          _ <- recordStep env (WorkflowId source) 1 "DBOS.step" (OutcomeOutput (Just "one")) Nothing Nothing
          _ <- recordStep env (WorkflowId source) 2 "DBOS.step" (OutcomeOutput (Just "two")) Nothing Nothing
          forked <- forkFrom env [WorkflowId source] ForkLastStep defaultForkOptions Nothing
          case forked of
            Right [WorkflowId forkedId] -> do
              -- The fork restarts *at* the last step, so that step is the one
              -- it runs again and only the ones before it are copied.
              copied <- runReaderAt env forkedId
              copied @?= [1]
            other -> fail ("expected one fork, got: " <> show other),
      testCase "forking a missing source is reported" $ do
        missing <- freshWorkflowId
        withBackend getBackend $ \env -> do
          forked <- forkWorkflows env [forkNew missing] defaultForkOptions Nothing
          case forked of
            Left (NonExistentWorkflow {workflowIds = ids}) -> ids @?= [missing]
            other -> fail ("expected NonExistentWorkflow, got: " <> show other),
      testCase "a source with no steps has no fork point" $ do
        source <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture source)
          forked <- forkFrom env [WorkflowId source] ForkLastStep defaultForkOptions Nothing
          case forked of
            Left (NoForkPoint {workflowIds = ids}) -> ids @?= [source]
            other -> fail ("expected NoForkPoint, got: " <> show other)
    ]
  where
    runReaderAt env forkedId = do
      copied <- listSteps env (WorkflowId forkedId) True Nothing Nothing Nothing
      case copied of
        Right steps -> pure (map (.stepRecordStepId) steps)
        Left err -> fail ("expected copied steps, got: " <> show err)

-- | The workflow lifecycle writes: releasing delayed workflows, cancelling
-- (with and without children), resuming (reporting ids that do not exist),
-- and deleting. Mirrors @transition_delayed_workflows@,
-- @cancel_workflows@, @resume_workflows@ and @delete_workflows@.
lifecycleWriteTests :: IO PostgresSystemDB -> TestTree
lifecycleWriteTests getBackend =
  testGroup
    "Lifecycle writes"
    [ testCase "a due delayed workflow is released, an early one is not" $ do
        due <- freshWorkflowId
        early <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture due) {fixtureStatus = "DELAYED", fixtureDelayUntil = Just 1000}
          _ <- insertFixtureChecked env (defaultFixture early) {fixtureStatus = "DELAYED", fixtureDelayUntil = Just 4102444800000}
          moved <- transitionDelayedWorkflows env
          case moved of
            Left err -> fail ("expected a sweep, got: " <> show err)
            Right count -> assertBool "at least our row moved" (count >= 1)
          released <- getWorkflow env (WorkflowId due)
          case released of
            Right (Just record) -> record.workflowRecordStatus @?= Enqueued
            other -> fail ("expected record, got: " <> show other)
          kept <- getWorkflow env (WorkflowId early)
          case kept of
            Right (Just record) -> record.workflowRecordStatus @?= Delayed
            other -> fail ("expected record, got: " <> show other),
      testCase "cancelling stops at terminal rows and can take children" $ do
        unique <- freshWorkflowId
        let root = unique <> "-root"
            child = unique <> "-child"
            grandchild = unique <> "-grandchild"
            settled = unique <> "-settled"
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture root)
          _ <- insertFixtureChecked env (defaultFixture child) {fixtureParent = Just root}
          _ <- insertFixtureChecked env (defaultFixture grandchild) {fixtureParent = Just child}
          _ <- insertFixtureChecked env (defaultFixture settled) {fixtureStatus = "SUCCESS"}
          plain <- cancelWorkflows env [WorkflowId settled] False Nothing
          plain @?= Right []
          cancelled <- cancelWorkflows env [WorkflowId root] True Nothing
          case cancelled of
            Right ids -> sort (map unId ids) @?= sort [root, child, grandchild]
            other -> fail ("expected the tree, got: " <> show other)
          found <- getWorkflow env (WorkflowId child)
          case found of
            Right (Just record) -> record.workflowRecordStatus @?= Cancelled
            other -> fail ("expected record, got: " <> show other),
      testCase "resuming reports missing ids and resets the rest" $ do
        present <- freshWorkflowId
        missing <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture present) {fixtureStatus = "CANCELLED", fixtureRecoveryAttempts = Just 3}
          absent <- resumeWorkflows env [WorkflowId present, WorkflowId missing] (Just "q") Nothing
          case absent of
            Left (NonExistentWorkflow {workflowIds = ids}) -> ids @?= [missing]
            other -> fail ("expected the missing id, got: " <> show other)
          resumed <- resumeWorkflows env [WorkflowId present] (Just "q") Nothing
          case resumed of
            Right ids -> map unId ids @?= [present]
            other -> fail ("expected a resume, got: " <> show other)
          found <- getWorkflow env (WorkflowId present)
          case found of
            Right (Just record) -> do
              record.workflowRecordStatus @?= Enqueued
              record.workflowRecordRecoveryAttempts @?= 0
              record.workflowRecordQueueName @?= Just "q"
            other -> fail ("expected record, got: " <> show other),
      testCase "deleting removes the rows and optionally the children" $ do
        unique <- freshWorkflowId
        let root = unique <> "-root"
            child = unique <> "-child"
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture root)
          _ <- insertFixtureChecked env (defaultFixture child) {fixtureParent = Just root}
          deleted <- deleteWorkflows env [WorkflowId root] True Nothing
          deleted @?= Right 2
          gone <- getWorkflow env (WorkflowId child)
          gone @?= Right Nothing,
      testCase "deleting a workflow removes its payloads and steps" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- initWorkflow env (newWorkflow unique) {newWorkflowInput = Just "\"in\""} Nothing Fresh Nothing
          _ <- recordStep env (WorkflowId unique) 0 "charge" (OutcomeOutput (Just "1")) Nothing Nothing
          _ <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeOutput (Just "1"))
          deleted <- deleteWorkflows env [WorkflowId unique] False Nothing
          deleted @?= Right 1
          forM_ ["workflow_status", "workflow_input", "workflow_output", "operation_outputs"] $ \table -> do
            count <- tableRowCount env table unique
            count @?= 0
    ]
  where
    unId (WorkflowId widText) = widText

-- | Messages: a send delivers to a destination's topic and, from a
-- workflow, records the send as a step; a receive takes the oldest waiting
-- message and records the take in one commit, adopts a rival's record,
-- reports an empty topic as a recorded absence, and refuses a second
-- receiver on the same topic. Mirrors @send_messages@ and @recv@.
messageTests :: IO PostgresSystemDB -> TestTree
messageTests getBackend =
  testGroup
    "Messages"
    [ testCase "a message sent to a workflow can be received" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture destination)
          sent <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "yes" (Just (Serialization "json")))) (Just "json") Nothing False
          sent @?= Right ()
          received <- recv env (WorkflowId destination) 1 2 (Just "approval") (millisDuration 0)
          received @?= Right (Just (EncodedValue "yes" (Just "json"))),
      testCase "an empty topic times out as a recorded absence" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture destination)
          missed <- recv env (WorkflowId destination) 1 2 (Just "approval") (millisDuration 0)
          missed @?= Right Nothing
          step <- checkStep env (WorkflowId destination) 1 "DBOS.recv"
          case step of
            Right (Just recorded) -> recorded.stepRecordOutput @?= Nothing
            other -> fail ("expected the recorded receive, got: " <> show other),
      testCase "a replay returns the same message without taking another" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture destination)
          _ <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "first" Nothing)) Nothing Nothing False
          _ <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "second" Nothing)) Nothing Nothing False
          first <- recv env (WorkflowId destination) 1 2 (Just "approval") (millisDuration 0)
          first @?= Right (Just (EncodedValue "first" Nothing))
          replayed <- recv env (WorkflowId destination) 1 2 (Just "approval") (millisDuration 0)
          replayed @?= Right (Just (EncodedValue "first" Nothing))
          next <- recv env (WorkflowId destination) 3 4 (Just "approval") (millisDuration 0)
          next @?= Right (Just (EncodedValue "second" Nothing)),
      testCase "everything sent is reported, read or not" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture destination)
          _ <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "first" Nothing)) Nothing Nothing False
          _ <- recv env (WorkflowId destination) 1 2 (Just "approval") (millisDuration 0)
          reported <- getAllNotifications env (WorkflowId destination)
          case reported of
            Right [record] -> do
              record.notificationRecordTopic @?= Just "approval"
              record.notificationRecordConsumed @?= True
            other -> fail ("expected the consumed message, got: " <> show other),
      testCase "a second receiver on one topic is refused" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture destination)
          waiting <- async (recv env (WorkflowId destination) 1 2 (Just "approval") (millisDuration 500))
          threadDelay 100000
          refused <- recv env (WorkflowId destination) 3 4 (Just "approval") (millisDuration 500)
          case refused of
            Left (ConcurrentRecv {}) -> pure ()
            other -> fail ("expected ConcurrentRecv, got: " <> show other)
          _ <- wait waiting
          pure (),
      testCase "a send to forks reaches the fork tree" $ do
        source <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture source) {fixtureStatus = "SUCCESS"}
          forked <- forkWorkflows env [(forkNew source) {forkStartStep = 0}] defaultForkOptions Nothing
          case forked of
            Left err -> fail ("expected a fork, got: " <> show err)
            Right [WorkflowId forkedId] -> do
              sent <- sendMessage env (messageTo (WorkflowId source) (Topic "approval") (SerializedWorkflowValue "fan" Nothing)) Nothing Nothing True
              sent @?= Right ()
              toSource <- recv env (WorkflowId source) 1 2 (Just "approval") (millisDuration 0)
              toSource @?= Right (Just (EncodedValue "fan" Nothing))
              toFork <- recv env (WorkflowId forkedId) 1 2 (Just "approval") (millisDuration 0)
              toFork @?= Right (Just (EncodedValue "fan" Nothing))
            other -> fail ("expected one fork, got: " <> show other),
      testCase "a send to a missing workflow is reported" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          sent <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "yes" Nothing)) Nothing Nothing False
          case sent of
            Left (NonExistentWorkflow {workflowIds = missing}) -> missing @?= [destination]
            other -> fail ("expected NonExistentWorkflow, got: " <> show other),
      testCase "duplicate and empty idempotency keys are refused" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture destination)
          let keyed key = (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "yes" Nothing)) {sendIdempotencyKey = Just (IdempotencyKey key)}
          emptyKey <- sendMessage env (keyed "") Nothing Nothing False
          case emptyKey of
            Left (InvalidInput {}) -> pure ()
            other -> fail ("expected InvalidInput, got: " <> show other)
          duplicated <- sendMessages env [keyed "k", keyed "k"] Nothing Nothing False
          case duplicated of
            Left (InvalidInput {}) -> pure ()
            other -> fail ("expected InvalidInput, got: " <> show other)
    ]

-- | Events: publishing records the publish as a step (so a replay does not
-- republish), reading outside a workflow is just the poll, and reading
-- inside one records the answer — including the absence a timeout produced,
-- which a replay must not re-wait. Mirrors @set_event@ and @get_event@.
eventTests :: IO PostgresSystemDB -> TestTree
eventTests getBackend =
  testGroup
    "Events"
    [ testCase "a published event reads back with its format" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          published <- setEvent env (WorkflowId unique) 1 "progress" "halfway" (Just "json")
          published @?= Right ()
          found <- getEvent env (WorkflowId unique) "progress" (millisDuration 0) Nothing
          found @?= Right (Just (EncodedValue "halfway" (Just "json"))),
      testCase "an absent event times out as nothing" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          found <- getEvent env (WorkflowId unique) "never" (millisDuration 0) Nothing
          found @?= Right Nothing,
      testCase "a replayed publish does not republish" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          _ <- setEvent env (WorkflowId unique) 1 "progress" "first" Nothing
          again <- setEvent env (WorkflowId unique) 1 "progress" "second" Nothing
          again @?= Right ()
          found <- getEvent env (WorkflowId unique) "progress" (millisDuration 0) Nothing
          found @?= Right (Just (EncodedValue "first" Nothing)),
      testCase "a wait sees a value published while it waits" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          publisher <- async $ do
            threadDelay 50000
            _ <- setEvent env (WorkflowId unique) 1 "progress" "late" Nothing
            pure ()
          found <- getEvent env (WorkflowId unique) "progress" (secondsDuration 5) Nothing
          wait publisher
          found @?= Right (Just (EncodedValue "late" Nothing)),
      testCase "a caller's read records the answer and replays it" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          _ <- setEvent env (WorkflowId unique) 1 "progress" "first" Nothing
          let caller = GetEventCaller {getEventCallerWorkflowId = WorkflowId unique, getEventCallerStepId = 2, getEventCallerTimeoutStepId = 3}
          read1 <- getEvent env (WorkflowId unique) "progress" (millisDuration 0) (Just caller)
          read1 @?= Right (Just (EncodedValue "first" Nothing))
          _ <- setEvent env (WorkflowId unique) 4 "progress" "second" Nothing
          read2 <- getEvent env (WorkflowId unique) "progress" (millisDuration 0) (Just caller)
          read2 @?= Right (Just (EncodedValue "first" Nothing))
          step <- checkStep env (WorkflowId unique) 2 "DBOS.getEvent"
          case step of
            Right (Just recorded) -> recorded.stepRecordOutput @?= Just "first"
            other -> fail ("expected the recorded read, got: " <> show other)
          deadline <- checkStep env (WorkflowId unique) 3 sleepStepName
          case deadline of
            Right (Just recorded) -> assertBool "the deadline is registered" (recorded.stepRecordOutput /= Nothing)
            other -> fail ("expected the deadline step, got: " <> show other),
      testCase "a caller's timeout is a recorded answer" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          let caller = GetEventCaller {getEventCallerWorkflowId = WorkflowId unique, getEventCallerStepId = 2, getEventCallerTimeoutStepId = 3}
          missed <- getEvent env (WorkflowId unique) "never" (millisDuration 0) (Just caller)
          missed @?= Right Nothing
          _ <- setEvent env (WorkflowId unique) 1 "never" "too-late" Nothing
          replayed <- getEvent env (WorkflowId unique) "never" (millisDuration 0) (Just caller)
          replayed @?= Right Nothing,
      testCase "every published event is listed by key" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          _ <- setEvent env (WorkflowId unique) 1 "b-key" "two" Nothing
          _ <- setEvent env (WorkflowId unique) 2 "a-key" "one" Nothing
          listed <- getAllEvents env (WorkflowId unique)
          case listed of
            Right records -> map (.eventKey) records @?= ["a-key", "b-key"]
            other -> fail ("expected the events, got: " <> show other)
    ]

-- | Checkpointed sleep: the wake time is the step's output, so a replay
-- wakes at the original instant rather than sleeping again, and a rival's
-- recording is adopted rather than disagreed with. A durable sleep is
-- stamped complete at its wake time, so the step's duration is the sleep.
-- Mirrors @checkpoint_sleep@ with @SleepKind::Durable@.
sleepTests :: IO PostgresSystemDB -> TestTree
sleepTests getBackend =
  testGroup
    "Sleeps"
    [ testCase "a sleep records its wake time as a step" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          woke <- recordSleep env (WorkflowId unique) 1 (millisDuration 60000)
          case woke of
            Left err -> fail ("expected a wake time, got: " <> show err)
            Right wakeAt -> do
              assertBool "the wake time is in the future" (timestampToEpochMs wakeAt > 0)
              found <- checkStep env (WorkflowId unique) 1 sleepStepName
              case found of
                Right (Just step) -> do
                  step.stepRecordStepName @?= sleepStepName
                  step.stepRecordOutput @?= Just (Text.pack (show (timestampToEpochMs wakeAt)))
                  step.stepRecordSerialization @?= Just "portable_json"
                  step.stepRecordCompletedAt @?= Just wakeAt
                other -> fail ("expected the sleep step, got: " <> show other),
      testCase "a replay wakes at the original instant" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          first <- recordSleep env (WorkflowId unique) 1 (millisDuration 60000)
          again <- recordSleep env (WorkflowId unique) 1 (millisDuration 5000)
          again @?= first,
      testCase "a rival's recorded sleep is adopted" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          _ <- recordStep env (WorkflowId unique) 1 sleepStepName (OutcomeOutput (Just "12345")) (Just "portable_json") Nothing
          woke <- recordSleep env (WorkflowId unique) 1 (millisDuration 60000)
          woke @?= Right (timestampFromEpochMs 12345)
    ]

-- | DEFERRED (P7.4): the ported stream contract, kept compiled but not
-- wired into 'tests' — the stream methods are stubs until their turn, so
-- wiring these now would leave the watcher red. Enable by adding
-- @streamTests@ to 'tests' in the same change that implements
-- @writeStream@/@closeStream@/@readStreamValue@/@getAllStreamEntries@.
-- Semantics verified against @postgres.rs@: append-with-computed-offset
-- under a primary-key collision retry, the close sentinel recorded as a
-- close, replay suppressed only for workflow-level writes, and reads that
-- report the producer's status alongside an optional value.
streamTests :: TestTree
streamTests =
  withResource acquireSuiteBackend releasePostgresSystemDB $ \getBackend ->
  testGroup
    "Streams (deferred)"
    [ testCase "a workflow-written value reads back at its offset" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          written <- writeStream env (WorkflowId unique) 1 "progress" "one" (Just "json") Workflow
          written @?= Right ()
          found <- readStreamValue env (WorkflowId unique) "progress" 0
          found @?= Right (StreamRead Pending (Just (EncodedValue "one" (Just "json")))),
      testCase "an offset with no entry still reports the producer's status" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          found <- readStreamValue env (WorkflowId unique) "progress" 0
          found @?= Right (StreamRead Pending Nothing),
      testCase "a stream for a missing workflow is reported" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          found <- readStreamValue env (WorkflowId unique) "progress" 0
          case found of
            Left (NonExistentWorkflow {}) -> pure ()
            other -> fail ("expected NonExistentWorkflow, got: " <> show other),
      testCase "writes append in offset order" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          _ <- writeStream env (WorkflowId unique) 1 "progress" "one" Nothing Step
          _ <- writeStream env (WorkflowId unique) 2 "progress" "two" Nothing Step
          entries <- getAllStreamEntries env (WorkflowId unique)
          case entries of
            Right found ->
              map (\entry -> (entry.streamOffset, entry.streamValue)) found @?= [(0, "one"), (1, "two")]
            other -> fail ("expected entries, got: " <> show other),
      testCase "closing writes the sentinel" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          closed <- closeStream env (WorkflowId unique) 1 "progress"
          closed @?= Right ()
          found <- readStreamValue env (WorkflowId unique) "progress" 0
          case found of
            Right (StreamRead _ (Just value)) -> value.encodedValue @?= streamClosedSentinel
            other -> fail ("expected the sentinel, got: " <> show other),
      testCase "a replayed workflow-level write does not append twice" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          _ <- writeStream env (WorkflowId unique) 1 "progress" "one" Nothing Workflow
          _ <- writeStream env (WorkflowId unique) 1 "progress" "one" Nothing Workflow
          entries <- getAllStreamEntries env (WorkflowId unique)
          case entries of
            Right found -> length found @?= 1
            other -> fail ("expected entries, got: " <> show other)
    ]

-- | The replay gate: reading a recorded step back, recording one, and the
-- two ways a recording loses — a rival execution already wrote it, or the
-- position now holds a different step. Mirrors @check_step@ and
-- @record_step@.
stepTests :: IO PostgresSystemDB -> TestTree
stepTests getBackend =
  testGroup
    "Steps"
    [ testCase "a recorded step reads back whole" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          recorded <- recordStep env (WorkflowId unique) 1 "DBOS.step" (OutcomeOutput (Just "out")) (Just "json") Nothing
          recorded @?= Right ()
          found <- checkStep env (WorkflowId unique) 1 "DBOS.step"
          case found of
            Right (Just step) -> do
              step.stepRecordWorkflowId @?= WorkflowId unique
              step.stepRecordStepId @?= 1
              step.stepRecordStepName @?= "DBOS.step"
              step.stepRecordOutput @?= Just "out"
              step.stepRecordSerialization @?= Just "json"
            other -> fail ("expected a step, got: " <> show other),
      testCase "an unrecorded step reads as nothing" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          found <- checkStep env (WorkflowId unique) 1 "DBOS.step"
          found @?= Right Nothing,
      testCase "a missing workflow and a cancelled one are reported" $ do
        missing <- freshWorkflowId
        cancelled <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture cancelled) {fixtureStatus = "CANCELLED"}
          missingStep <- checkStep env (WorkflowId missing) 1 "DBOS.step"
          case missingStep of
            Left (NonExistentWorkflow {}) -> pure ()
            other -> fail ("expected NonExistentWorkflow, got: " <> show other)
          cancelledStep <- checkStep env (WorkflowId cancelled) 1 "DBOS.step"
          case cancelledStep of
            Left (WorkflowCancelled {}) -> pure ()
            other -> fail ("expected WorkflowCancelled, got: " <> show other),
      testCase "a renamed step at the same position is unexpected" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          _ <- recordStep env (WorkflowId unique) 1 "DBOS.old" (OutcomeOutput (Just "out")) Nothing Nothing
          found <- checkStep env (WorkflowId unique) 1 "DBOS.new"
          case found of
            Left (UnexpectedStep {}) -> pure ()
            other -> fail ("expected UnexpectedStep, got: " <> show other),
      testCase "a rival execution's step is not overwritten" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          _ <- recordStep env (WorkflowId unique) 1 "DBOS.step" (OutcomeOutput (Just "winner")) (Just "json") (Just (StepTiming (timestampFromEpochMs 100) (timestampFromEpochMs 200)))
          rival <- recordStep env (WorkflowId unique) 1 "DBOS.step" (OutcomeOutput (Just "loser")) (Just "json") (Just (StepTiming (timestampFromEpochMs 100) (timestampFromEpochMs 300)))
          case rival of
            Left (StepAlreadyRecorded {}) -> pure ()
            other -> fail ("expected StepAlreadyRecorded, got: " <> show other)
          found <- checkStep env (WorkflowId unique) 1 "DBOS.step"
          case found of
            Right (Just step) -> step.stepRecordOutput @?= Just "winner"
            other -> fail ("expected a step, got: " <> show other),
      testCase "winning the checkpoint re-stamps the executor" $ do
        unique <- freshWorkflowId
        withBackendSettings getBackend (defaultSettings {settingsExecutorId = Just "advancing"}) $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique) {fixtureExecutor = Just "stale"}
          _ <- recordStep env (WorkflowId unique) 1 "DBOS.step" (OutcomeOutput Nothing) Nothing Nothing
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> record.workflowRecordExecutorId @?= Just "advancing"
            other -> fail ("expected record, got: " <> show other)
    ]

-- | Delaying, claiming back, attribute updates and recovery sweeps: the
-- writes around a workflow's lifecycle. Mirrors @set_workflow_delay@,
-- @clear_queue_assignment@, @update_workflow_attributes@ and
-- @reenqueue_for_recovery@ — each is guarded on the status it may act from,
-- so a write that no longer applies changes nothing rather than lying.
recoveryTests :: IO PostgresSystemDB -> TestTree
recoveryTests getBackend =
  testGroup
    "Delay, claims and recovery"
    [ testCase "a delayed workflow's delay moves, a pending one's does not" $ do
        delayed <- freshWorkflowId
        pending <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture delayed) {fixtureStatus = "DELAYED", fixtureCreatedAt = 1000}
          _ <- insertFixtureChecked env (defaultFixture pending) {fixtureCreatedAt = 1000}
          moved <- setWorkflowDelay env (WorkflowId delayed) (DelayFor (secondsDuration 60)) Nothing
          moved @?= Right ()
          ignored <- setWorkflowDelay env (WorkflowId pending) (DelayFor (secondsDuration 60)) Nothing
          ignored @?= Right ()
          delayedRow <- getWorkflow env (WorkflowId delayed)
          case delayedRow of
            Right (Just record) -> assertBool "the delay moved out" (maybe False (> timestampFromEpochMs 1000000000000) record.workflowRecordDelayUntil)
            other -> fail ("expected record, got: " <> show other)
          pendingRow <- getWorkflow env (WorkflowId pending)
          case pendingRow of
            Right (Just record) -> record.workflowRecordDelayUntil @?= Nothing
            other -> fail ("expected record, got: " <> show other),
      testCase "a queued pending workflow returns to its queue" $ do
        queued <- freshWorkflowId
        plain <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture queued) {fixtureQueue = Just "q"}
          _ <- insertFixtureChecked env (defaultFixture plain)
          returned <- clearQueueAssignment env (WorkflowId queued)
          returned @?= Right True
          notReturned <- clearQueueAssignment env (WorkflowId plain)
          notReturned @?= Right False
          found <- getWorkflow env (WorkflowId queued)
          case found of
            Right (Just record) -> do
              record.workflowRecordStatus @?= Enqueued
              record.workflowRecordStartedAt @?= Nothing
            other -> fail ("expected record, got: " <> show other),
      testCase "attributes are validated and stored" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          stored <- updateWorkflowAttributes env (WorkflowId unique) (Just "{\"k\": \"v\"}") Nothing
          stored @?= Right ()
          rejected <- updateWorkflowAttributes env (WorkflowId unique) (Just "[1,2]") Nothing
          case rejected of
            Left (InvalidInput {}) -> pure ()
            other -> fail ("expected InvalidInput, got: " <> show other)
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> assertBool "the attribute is stored" (maybe False (Text.isInfixOf "k") record.workflowRecordAttributes)
            other -> fail ("expected record, got: " <> show other),
      testCase "a recovery sweep enqueues only the dead executor's rows" $ do
        unique <- freshWorkflowId
        dead <- freshWorkflowId
        alive <- freshWorkflowId
        let deadExecutor = "dead-" <> unique
            aliveExecutor = "alive-" <> unique
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture dead) {fixtureExecutor = Just deadExecutor, fixtureVersion = Just "v1"}
          _ <- insertFixtureChecked env (defaultFixture alive) {fixtureExecutor = Just aliveExecutor, fixtureVersion = Just "v1"}
          swept <- reenqueueForRecovery env [deadExecutor] "v1" "_dbos_internal_queue"
          case swept of
            Right [recovered] -> recovered @?= WorkflowId dead
            other -> fail ("expected one id, got: " <> show other)
          found <- getWorkflow env (WorkflowId dead)
          case found of
            Right (Just record) -> do
              record.workflowRecordStatus @?= Enqueued
              record.workflowRecordQueueName @?= Just "_dbos_internal_queue"
              record.workflowRecordStartedAt @?= Nothing
            other -> fail ("expected record, got: " <> show other)
          untouched <- getWorkflow env (WorkflowId alive)
          case untouched of
            Right (Just record) -> record.workflowRecordStatus @?= Pending
            other -> fail ("expected record, got: " <> show other)
          none <- reenqueueForRecovery env [] "v1" "_dbos_internal_queue"
          none @?= Right []
    ]

-- | Starting workflows. Mirrors @init_workflow@: the initial status is
-- derived, queued workflows are not counted or claimed, a duplicate fresh
-- start records without claiming another owner's row, a recovery claims and
-- re-stamps it, a spent budget parks the workflow (and commits that), and a
-- different function under the same id is a programming error. The caller
-- form (the atomic child start) lands with @recordChildWorkflow@ in P7.4.
initTests :: IO PostgresSystemDB -> TestTree
initTests getBackend =
  testGroup
    "Initialising"
    [ testCase "a fresh start is pending, claimed and counted once" $ do
        unique <- freshWorkflowId
        withBackendSettings getBackend (defaultSettings {settingsExecutorId = Just "exec-1"}) $ \env -> do
          started <- initWorkflow env (newWorkflow unique) {newWorkflowExecutorId = Just "exec-1"} Nothing Fresh Nothing
          case started of
            Left err -> fail ("expected a start, got: " <> show err)
            Right startedResult -> do
              startedResult.initResultStatus @?= Pending
              startedResult.initResultRecoveryAttempts @?= 1
              startedResult.initResultShouldExecute @?= True
              startedResult.initResultDeadline @?= Nothing
              startedResult.initResultSerialization @?= Nothing
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> do
              record.workflowRecordStatus @?= Pending
              record.workflowRecordExecutorId @?= Just "exec-1"
              record.workflowRecordRecoveryAttempts @?= 1
              assertBool "the owner is stamped" (record.workflowRecordOwnerXid /= Nothing)
              assertBool "creation is stamped" (record.workflowRecordCreatedAt /= timestampFromEpochMs 0)
            other -> fail ("expected record, got: " <> show other),
      testCase "a queued start is enqueued and not counted" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          started <- initWorkflow env (newWorkflow unique) {newWorkflowQueueName = Just "q"} Nothing Fresh Nothing
          case started of
            Left err -> fail ("expected a start, got: " <> show err)
            Right startedResult -> do
              startedResult.initResultStatus @?= Enqueued
              startedResult.initResultRecoveryAttempts @?= 0
              startedResult.initResultShouldExecute @?= True,
      testCase "a duplicate fresh start does not claim another owner's row" $ do
        unique <- freshWorkflowId
        withBackendSettings getBackend (defaultSettings {settingsExecutorId = Just "mine"}) $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique) {fixtureName = Nothing, fixtureExecutor = Just "other-exec", fixtureOwnerXid = Just "other-owner", fixtureRecoveryAttempts = Just 1}
          started <- initWorkflow env (newWorkflow unique) Nothing Fresh Nothing
          case started of
            Left err -> fail ("expected a start, got: " <> show err)
            Right startedResult -> do
              startedResult.initResultShouldExecute @?= False
              startedResult.initResultRecoveryAttempts @?= 1
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> record.workflowRecordExecutorId @?= Just "other-exec"
            other -> fail ("expected record, got: " <> show other),
      testCase "a recovery claims the row, re-stamps the executor and counts the attempt" $ do
        unique <- freshWorkflowId
        withBackendSettings getBackend (defaultSettings {settingsExecutorId = Just "mine"}) $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique) {fixtureName = Nothing, fixtureExecutor = Just "other-exec", fixtureOwnerXid = Just "other-owner", fixtureRecoveryAttempts = Just 1}
          started <- initWorkflow env (newWorkflow unique) {newWorkflowExecutorId = Just "mine"} Nothing Recovery Nothing
          case started of
            Left err -> fail ("expected a start, got: " <> show err)
            Right startedResult -> do
              startedResult.initResultShouldExecute @?= True
              startedResult.initResultRecoveryAttempts @?= 2
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> record.workflowRecordExecutorId @?= Just "mine"
            other -> fail ("expected record, got: " <> show other),
      testCase "a different function under the same id is a conflict" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture unique) {fixtureName = Just "alpha"})
          started <- initWorkflow env (newWorkflow unique) {newWorkflowName = Just "beta"} Nothing Fresh Nothing
          case started of
            Left (ConflictingWorkflow {}) -> pure ()
            _ -> fail "expected ConflictingWorkflow",
      testCase "the recovery budget parks the workflow and commits it" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique) {fixtureName = Nothing, fixtureOwnerXid = Just "other-owner", fixtureRecoveryAttempts = Just 3, fixtureQueue = Just ("q-" <> unique), fixtureDeduplicationId = Just ("key-" <> unique)}
          started <- initWorkflow env (newWorkflow unique) (Just 2) Recovery Nothing
          case started of
            Left (ErrorMaxRecoveryAttemptsExceeded {}) -> pure ()
            other -> fail ("expected ErrorMaxRecoveryAttemptsExceeded, got: " <> show other)
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> do
              record.workflowRecordStatus @?= MaxRecoveryAttemptsExceeded
              record.workflowRecordDeduplicationId @?= Nothing
              record.workflowRecordQueueName @?= Nothing
              assertBool "updated_at is stamped" (record.workflowRecordUpdatedAt /= timestampFromEpochMs 0)
              assertBool "completed_at is stamped" (record.workflowRecordCompletedAt /= Nothing)
            other -> fail ("expected record, got: " <> show other),
      testCase "a deduplication collision is reported" $ do
        unique <- freshWorkflowId
        first <- freshWorkflowId
        second <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture first) {fixtureQueue = Just ("q-" <> unique), fixtureDeduplicationId = Just ("key-" <> unique)}
          started <- initWorkflow env (newWorkflow second) {newWorkflowQueueName = Just ("q-" <> unique), newWorkflowDeduplicationId = Just ("key-" <> unique)} Nothing Fresh Nothing
          case started of
            Left (QueueDeduplicated {}) -> pure ()
            _ -> fail "expected QueueDeduplicated",
      testCase "writes the input to the payload table, not the status row" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          started <- initWorkflow env (newWorkflow unique) {newWorkflowInput = Just "\"in\""} Nothing Fresh Nothing
          case started of
            Left err -> fail ("expected a start, got: " <> show err)
            Right _ -> pure ()
          (input, _, _) <- payloadRows env unique
          input @?= Just "\"in\""
          legacyPayloads env unique >>= (@?= (Nothing, Nothing, Nothing)),
      testCase "a resubmission does not replace the input" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          first <- initWorkflow env (newWorkflow unique) {newWorkflowInput = Just "\"first\""} Nothing Fresh Nothing
          case first of
            Left err -> fail ("expected a start, got: " <> show err)
            Right _ -> pure ()
          again <- initWorkflow env (newWorkflow unique) {newWorkflowInput = Just "\"second\""} Nothing Fresh Nothing
          case again of
            Left err -> fail ("expected a start, got: " <> show err)
            Right _ -> pure ()
          (input, _, _) <- payloadRows env unique
          input @?= Just "\"first\"",
      testCase "a fresh start replaces an orphaned input" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <-
            runSession env "fixture" $
              Session.script ("insert into dbos.workflow_input (workflow_uuid, inputs) values ('" <> unique <> "', '\"stale\"')")
          started <- initWorkflow env (newWorkflow unique) {newWorkflowInput = Just "\"fresh\""} Nothing Fresh Nothing
          case started of
            Left err -> fail ("expected a start, got: " <> show err)
            Right _ -> pure ()
          (input, _, _) <- payloadRows env unique
          input @?= Just "\"fresh\""
    ]

-- | Schedules: the cron registry, ported from @postgres.rs@'s schedule
-- methods and the Rust integration tests in @tests/sysdb.rs@. Each case
-- names the Rust test it mirrors. The caller-driven replay cases
-- (@a_schedule_step_replays_rather_than_repeating@,
-- @pausing_and_resuming_are_distinct_steps@,
-- @every_schedule_write_replays_from_its_checkpoint@,
-- @a_failed_schedule_step_records_nothing@) are absent: caller
-- checkpointing is deferred port-wide, and the refusal stands in for them.
scheduleTests :: IO PostgresSystemDB -> TestTree
scheduleTests getBackend =
  testGroup
    "Schedules"
    [ -- Mirrors @a_schedule_round_trips@.
      testCase "a schedule round-trips through the database" $ do
        unique <- freshWorkflowId
        let name = "sched-" <> Text.take 8 unique
            scheduleId = "sch-" <> Text.take 8 unique
            firedAt = timestampFromEpochMs 1786492800000
            schedule =
              (newSchedule name "generate_report" "0 0 * * *")
                { newScheduleId = Just scheduleId,
                  newScheduleWorkflowClassName = Just "Reports",
                  newScheduleContext = "{\"tenant\":\"acme\"}",
                  newScheduleLastFiredAt = Just firedAt,
                  newScheduleAutomaticBackfill = True,
                  newScheduleCronTimezone = Just "Europe/London",
                  newScheduleQueueName = Just "reports"
                }
        withBackend getBackend $ \env -> do
          created <- createSchedule env schedule Nothing
          created @?= Right ()
          found <- getSchedule env name Nothing
          case found of
            Right (Just record) -> do
              record.scheduleRecordId @?= scheduleId
              record.scheduleRecordName @?= name
              record.scheduleRecordWorkflowName @?= "generate_report"
              record.scheduleRecordWorkflowClassName @?= Just "Reports"
              record.scheduleRecordExpression @?= "0 0 * * *"
              record.scheduleRecordStatus @?= Active
              record.scheduleRecordContext @?= "{\"tenant\":\"acme\"}"
              record.scheduleRecordLastFiredAt @?= Just firedAt
              record.scheduleRecordAutomaticBackfill @?= True
              record.scheduleRecordCronTimezone @?= Just "Europe/London"
              record.scheduleRecordQueueName @?= Just "reports"
              record.scheduleRecordApplicationName @?= Nothing
            other -> fail ("expected the schedule, got: " <> show other)
          missing <- getSchedule env (name <> "-absent") Nothing
          missing @?= Right Nothing
          -- The row is deliberately left behind as the psql evidence for the
          -- round trip; deleting is covered by the pause/resume/delete case.
          pure (),
      -- Mirrors @a_peers_last_fired_at_spelling_is_read_as_an_instant@.
      testCase "a peer's last-fired-at spelling is read as an instant" $ do
        unique <- freshWorkflowId
        let name = "sched-" <> Text.take 8 unique
            expected = timestampFromEpochMs 1786492800000
            spellings =
              [ "2026-08-12T00:00:00.000Z",
                "2026-08-12T00:00:00Z",
                "2026-08-12T00:00:00+00:00",
                "2026-08-12T00:00:00.000000000Z",
                "2026-08-12T01:30:00+01:30"
              ]
        withBackend getBackend $ \env -> do
          _ <- createSchedule env (newSchedule name "generate_report" "0 0 * * *") Nothing
          traverse_
            ( \stored -> do
                setScheduleLastFiredAtRaw env name stored
                found <- getSchedule env name Nothing
                case found of
                  Right (Just record) -> record.scheduleRecordLastFiredAt @?= Just expected
                  other -> fail ("expected the schedule after " <> show stored <> ", got: " <> show other)
            )
            spellings
          setScheduleLastFiredAtRaw env name "yesterday"
          malformed <- getSchedule env name Nothing
          case malformed of
            Left (Malformed {}) -> pure ()
            other -> fail ("expected Malformed, got: " <> show other)
          _ <- deleteSchedule env name Nothing
          pure (),
      -- Mirrors @a_schedule_keeps_its_identity_across_a_re_apply@.
      testCase "a schedule keeps its identity across a re-apply" $ do
        unique <- freshWorkflowId
        let name = "sched-" <> Text.take 8 unique
            firedAt = timestampFromEpochMs 1786492800000
        withBackend getBackend $ \env -> do
          _ <- createSchedule env (newSchedule name "generate_report" "0 0 * * *") Nothing
          first <- readSchedule env name
          assertBool "an id is generated" (not (Text.null first.scheduleRecordId))
          _ <- setScheduleStatus env name Paused Nothing
          _ <- updateScheduleLastFiredAt env name firedAt
          reapplied <-
            upsertSchedule env
                  (newSchedule name "generate_report" "*/5 * * * *") {newScheduleWorkflowClassName = Just "Reports"}
                  Nothing
              
          reapplied @?= Right ()
          after <- readSchedule env name
          after.scheduleRecordId @?= first.scheduleRecordId
          after.scheduleRecordExpression @?= "*/5 * * * *"
          after.scheduleRecordWorkflowClassName @?= Just "Reports"
          after.scheduleRecordStatus @?= Paused
          after.scheduleRecordLastFiredAt @?= Just firedAt
          _ <- deleteSchedule env name Nothing
          pure (),
      -- Mirrors @creating_a_schedule_twice_is_refused@.
      testCase "creating a schedule twice is refused" $ do
        unique <- freshWorkflowId
        let name = "sched-" <> Text.take 8 unique
            otherName = "sched-other-" <> Text.take 8 unique
            schedule = newSchedule name "generate_report" "0 0 * * *"
        withBackend getBackend $ \env -> do
          _ <- createSchedule env schedule Nothing
          twice <- createSchedule env schedule Nothing
          case twice of
            Left (AlreadyRegistered {kind = kind, name = taken}) -> do
              kind @?= "Schedule"
              taken @?= name
            other -> fail ("expected AlreadyRegistered, got: " <> show other)
          taken <- readSchedule env name
          idCollision <-
            createSchedule env (newSchedule otherName "sweep" "0 * * * *") {newScheduleId = Just taken.scheduleRecordId} Nothing
          case idCollision of
            Left (AlreadyRegistered {kind = kind}) -> kind @?= "Schedule id"
            other -> fail ("expected an id collision, got: " <> show other)
          _ <- upsertSchedule env schedule Nothing
          names <- scheduleNames env (defaultScheduleFilter {scheduleFilterNamePrefixes = [name], scheduleFilterApplications = Any})
          names @?= [name]
          _ <- deleteSchedule env name Nothing
          pure (),
      -- Mirrors @a_schedule_name_held_by_another_application_is_refused@.
      testCase "a schedule name held by another application is refused" $ do
        unique <- freshWorkflowId
        let alpha = "alpha-" <> Text.take 8 unique
            beta = "beta-" <> Text.take 8 unique
            name = "sched-" <> Text.take 8 unique
            unclaimed = "sched-unclaimed-" <> Text.take 8 unique
            schedule = newSchedule name "generate_report" "0 0 * * *"
        withBackendAs getBackend (Just alpha) $ \env -> do
          created <- createSchedule env schedule Nothing
          created @?= Right ()
        withBackendAs getBackend (Just beta) $ \env -> do
          refusedCreate <- createSchedule env schedule Nothing
          refusedHolder refusedCreate alpha
          refusedUpsert <- upsertSchedule env schedule Nothing
          refusedHolder refusedUpsert alpha
        -- An anonymous handle claims an unclaimed row rather than colliding with it.
        withBackend getBackend $ \env -> do
          created <- createSchedule env (newSchedule unclaimed "sweep" "0 * * * *") Nothing
          created @?= Right ()
        withBackendAs getBackend (Just beta) $ \env -> do
          _ <- upsertSchedule env (newSchedule unclaimed "sweep" "0 * * * *") Nothing
          claimed <- readSchedule env unclaimed
          claimed.scheduleRecordApplicationName @?= Just beta
        withBackend getBackend $ \env -> do
          _ <- deleteSchedule env name Nothing
          _ <- deleteSchedule env unclaimed Nothing
          pure (),
      -- Mirrors @applying_schedules_is_all_or_nothing@.
      testCase "applying schedules is all or nothing" $ do
        unique <- freshWorkflowId
        let prefix = "p-" <> Text.take 8 unique <> "/"
            nightly = prefix <> "nightly"
            hourly = prefix <> "hourly"
            weekly = prefix <> "weekly"
            pausedDecl = prefix <> "paused-decl"
            alpha = "alpha-" <> Text.take 8 unique
            beta = "beta-" <> Text.take 8 unique
            firedAt = timestampFromEpochMs 1786492800000
            -- Unset from beta's handle: alpha's nightly must not appear, nor
            -- beta's unclaimed peers.
            scoped = defaultScheduleFilter {scheduleFilterNamePrefixes = [prefix]}
        withBackendAs getBackend (Just alpha) $ \env -> do
          created <- createSchedule env (newSchedule nightly "generate_report" "0 0 * * *") Nothing
          created @?= Right ()
        withBackendAs getBackend (Just beta) $ \env -> do
          refused <-
            applySchedules env
                  [ newSchedule hourly "sweep" "0 * * * *",
                    newSchedule nightly "generate_report" "0 0 * * *"
                  ]
              
          case refused of
            Left (RegisteredByAnother {kind = kind, holder = holder}) -> do
              kind @?= "Schedule"
              holder @?= alpha
            other -> fail ("expected RegisteredByAnother, got: " <> show other)
          rolledBack <- getSchedule env hourly Nothing
          rolledBack @?= Right Nothing
          -- Without the collision, both land, and re-applying is a no-op.
          let declared = [newSchedule hourly "sweep" "0 * * * *", newSchedule weekly "archive" "0 0 * * 0"]
          applied <- applySchedules env declared
          applied @?= Right ()
          reapplied <- applySchedules env declared
          reapplied @?= Right ()
          names <- scheduleNames env scoped
          names @?= [hourly, weekly]
          nothing <- applySchedules env []
          nothing @?= Right ()
          -- A declaration's runtime state seeds a fresh row, and the conflict
          -- clause keeps the stored value on an existing one.
          let paused = (newSchedule pausedDecl "run" "0 0 * * *") {newScheduleStatus = Paused, newScheduleLastFiredAt = Just firedAt}
          seeded <- applySchedules env [paused]
          seeded @?= Right ()
          before <- readSchedule env pausedDecl
          before.scheduleRecordStatus @?= Paused
          before.scheduleRecordLastFiredAt @?= Just firedAt
          _ <- setScheduleStatus env pausedDecl Active Nothing
          _ <- applySchedules env [paused]
          after <- readSchedule env pausedDecl
          after.scheduleRecordStatus @?= Active
        withBackend getBackend $ \env -> do
          traverse_ (\scheduleName -> deleteSchedule env scheduleName Nothing) [nightly, hourly, weekly, pausedDecl]
          pure (),
      -- Mirrors @schedules_are_listed_by_status_workflow_and_prefix@.
      testCase "schedules are listed by status, workflow and prefix" $ do
        unique <- freshWorkflowId
        let base = "p-" <> Text.take 8 unique <> "/"
            reportNightly = base <> "report-nightly"
            reportWeekly = base <> "report-weekly"
            sweepHourly = base <> "sweep-hourly"
            wildcardOdd = base <> "100%-odd"
            scoped = defaultScheduleFilter {scheduleFilterNamePrefixes = [base], scheduleFilterApplications = Any}
        withBackend getBackend $ \env -> do
          forM_
            [ (reportNightly, "generate_report"),
              (reportWeekly, "generate_report"),
              (sweepHourly, "sweep"),
              (wildcardOdd, "sweep")
            ]
            ( \(name, workflowName) -> do
                created <- createSchedule env (newSchedule name workflowName "0 0 * * *") Nothing
                created @?= Right ()
            )
          _ <- setScheduleStatus env reportWeekly Paused Nothing
          allNames <- scheduleNames env scoped
          allNames @?= [wildcardOdd, reportNightly, reportWeekly, sweepHourly]
          paused <- scheduleNames env scoped {scheduleFilterStatuses = [Paused]}
          paused @?= [reportWeekly]
          sweeps <- scheduleNames env scoped {scheduleFilterWorkflowNames = ["sweep"]}
          sweeps @?= [wildcardOdd, sweepHourly]
          prefixed <- scheduleNames env scoped {scheduleFilterNamePrefixes = [base <> "report-", base <> "sweep-"]}
          prefixed @?= [reportNightly, reportWeekly, sweepHourly]
          -- `%` is a character in a name, not a wildcard: were it one, this would match everything.
          literalPercent <- scheduleNames env scoped {scheduleFilterNamePrefixes = [base <> "100%"]}
          literalPercent @?= [wildcardOdd]
          composed <- scheduleNames env scoped {scheduleFilterStatuses = [Active], scheduleFilterWorkflowNames = ["generate_report"]}
          composed @?= [reportNightly]
          traverse_ (\scheduleName -> deleteSchedule env scheduleName Nothing) [reportNightly, reportWeekly, sweepHourly, wildcardOdd]
          pure (),
      -- Mirrors @a_schedule_listing_defaults_to_its_own_application@.
      testCase "a schedule listing defaults to its own application" $ do
        unique <- freshWorkflowId
        let base = "p-" <> Text.take 8 unique <> "/"
            alphaJob = base <> "alpha-job"
            betaJob = base <> "beta-job"
            nobodyJob = base <> "nobody-job"
            alpha = "alpha-" <> Text.take 8 unique
            beta = "beta-" <> Text.take 8 unique
            scoped = defaultScheduleFilter {scheduleFilterNamePrefixes = [base]}
        withBackendAs getBackend (Just alpha) $ \env -> do
          created <- createSchedule env (newSchedule alphaJob "run" "0 0 * * *") Nothing
          created @?= Right ()
        withBackendAs getBackend (Just beta) $ \env -> do
          created <- createSchedule env (newSchedule betaJob "run" "0 0 * * *") Nothing
          created @?= Right ()
        withBackend getBackend $ \env -> do
          created <- createSchedule env (newSchedule nobodyJob "run" "0 0 * * *") Nothing
          created @?= Right ()
        withBackendAs getBackend (Just alpha) $ \env -> do
          own <- scheduleNames env scoped {scheduleFilterApplications = Unset}
          own @?= [alphaJob, nobodyJob]
          anyApp <- scheduleNames env scoped {scheduleFilterApplications = Any}
          anyApp @?= [alphaJob, betaJob, nobodyJob]
          named <- scheduleNames env scoped {scheduleFilterApplications = Named [beta]}
          named @?= [betaJob, nobodyJob]
          -- A schedule is addressed by name, so a read crosses applications.
          crossed <- getSchedule env betaJob Nothing
          case crossed of
            Right (Just record) -> record.scheduleRecordApplicationName @?= Just beta
            other -> fail ("expected beta's schedule, got: " <> show other)
        withBackend getBackend $ \env -> do
          unnamedOwn <- scheduleNames env scoped {scheduleFilterApplications = Unset}
          unnamedOwn @?= [alphaJob, betaJob, nobodyJob]
          traverse_ (\scheduleName -> deleteSchedule env scheduleName Nothing) [alphaJob, betaJob, nobodyJob]
          pure (),
      -- Mirrors @updating_a_schedule_touches_only_its_definition@.
      testCase "updating a schedule touches only its definition" $ do
        unique <- freshWorkflowId
        let name = "sched-" <> Text.take 8 unique
            firedAt = timestampFromEpochMs 1786492800000
        withBackend getBackend $ \env -> do
          _ <-
            createSchedule env
                  (newSchedule name "generate_report" "0 0 * * *") {newScheduleCronTimezone = Just "Europe/London", newScheduleQueueName = Just "reports"}
                  Nothing
              
          _ <- setScheduleStatus env name Paused Nothing
          _ <- updateScheduleLastFiredAt env name firedAt
          before <- readSchedule env name
          updated <-
            updateSchedule env
                  name
                  defaultScheduleUpdate
                    { scheduleUpdateExpression = Set "*/5 * * * *",
                      scheduleUpdateAutomaticBackfill = Set True,
                      -- Both nullable columns clear, which is why they are Change (Maybe _).
                      scheduleUpdateCronTimezone = Set Nothing,
                      scheduleUpdateQueueName = Set Nothing
                    }
                  Nothing
              
          updated @?= Right ()
          after <- readSchedule env name
          after.scheduleRecordExpression @?= "*/5 * * * *"
          after.scheduleRecordAutomaticBackfill @?= True
          after.scheduleRecordCronTimezone @?= Nothing
          after.scheduleRecordQueueName @?= Nothing
          after.scheduleRecordContext @?= before.scheduleRecordContext
          after.scheduleRecordId @?= before.scheduleRecordId
          after.scheduleRecordStatus @?= Paused
          after.scheduleRecordLastFiredAt @?= before.scheduleRecordLastFiredAt
          _ <- deleteSchedule env name Nothing
          pure (),
      -- Mirrors @addressing_a_missing_schedule_is_refused@.
      testCase "addressing a missing schedule is refused" $ do
        unique <- freshWorkflowId
        let ghost = "sched-ghost-" <> Text.take 8 unique
            name = "sched-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          updated <- updateSchedule env ghost (defaultScheduleUpdate {scheduleUpdateExpression = Set "0 0 * * *"}) Nothing
          refusedMissing updated ghost
          empty <- updateSchedule env ghost defaultScheduleUpdate Nothing
          refusedMissing empty ghost
          status <- setScheduleStatus env ghost Paused Nothing
          refusedMissing status ghost
          -- The two that race a concurrent delete stay silent.
          fired <- updateScheduleLastFiredAt env ghost (timestampFromEpochMs 1786492800000)
          fired @?= Right ()
          deleted <- deleteSchedule env ghost Nothing
          deleted @?= Right ()
          -- An empty update against a schedule that exists changes nothing and succeeds.
          _ <- createSchedule env (newSchedule name "generate_report" "0 0 * * *") Nothing
          before <- readSchedule env name
          noop <- updateSchedule env name defaultScheduleUpdate Nothing
          noop @?= Right ()
          after <- getSchedule env name Nothing
          after @?= Right (Just before)
          _ <- deleteSchedule env name Nothing
          pure (),
      -- Mirrors @a_schedule_pauses_resumes_and_deletes@.
      testCase "a schedule pauses resumes and deletes" $ do
        unique <- freshWorkflowId
        let name = "sched-" <> Text.take 8 unique
            firedAt = timestampFromEpochMs 1786492800000
        withBackend getBackend $ \env -> do
          _ <- createSchedule env (newSchedule name "generate_report" "0 0 * * *") Nothing
          _ <- updateScheduleLastFiredAt env name firedAt
          paused <- setScheduleStatus env name Paused Nothing
          paused @?= Right ()
          stored <- readSchedule env name
          stored.scheduleRecordStatus @?= Paused
          stored.scheduleRecordLastFiredAt @?= Just firedAt
          resumed <- setScheduleStatus env name Active Nothing
          resumed @?= Right ()
          active <- readSchedule env name
          active.scheduleRecordStatus @?= Active
          deleted <- deleteSchedule env name Nothing
          deleted @?= Right ()
          gone <- getSchedule env name Nothing
          gone @?= Right Nothing,
      testCase "pausing inside a workflow is a step and replays" $ do
        parent <- freshWorkflowId
        unique <- freshWorkflowId
        let name = "sched-" <> Text.take 8 unique
            caller = Just (WorkflowId parent, 5)
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture parent)
          _ <- createSchedule env (newSchedule name "generate_report" "0 0 * * *") Nothing
          paused <- setScheduleStatus env name Paused caller
          paused @?= Right ()
          stored <- readSchedule env name
          stored.scheduleRecordStatus @?= Paused
          -- The schedule is gone, so a second run can only answer from the step.
          _ <- deleteSchedule env name Nothing
          replayed <- setScheduleStatus env name Paused caller
          replayed @?= Right (),
      testCase "creating inside a workflow is a step and replays" $ do
        parent <- freshWorkflowId
        unique <- freshWorkflowId
        let name = "sched-" <> Text.take 8 unique
            caller = Just (WorkflowId parent, 6)
            schedule = newSchedule name "generate_report" "0 0 * * *"
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture parent)
          created <- createSchedule env schedule caller
          created @?= Right ()
          stored <- readSchedule env name
          stored.scheduleRecordName @?= name
          -- The schedule is gone, so a second run can only answer from the step.
          _ <- deleteSchedule env name Nothing
          replayed <- createSchedule env schedule caller
          replayed @?= Right (),
      testCase "reading inside a workflow is a step and replays" $ do
        parent <- freshWorkflowId
        unique <- freshWorkflowId
        let name = "sched-" <> Text.take 8 unique
            caller = Just (WorkflowId parent, 8)
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture parent)
          _ <- createSchedule env (newSchedule name "generate_report" "0 0 * * *") Nothing
          found <- getSchedule env name caller
          case found of
            Right (Just record) -> record.scheduleRecordName @?= name
            other -> fail ("expected the schedule, got: " <> show other)
          -- The schedule is gone, so a second run can only answer from the step.
          _ <- deleteSchedule env name Nothing
          replayed <- getSchedule env name caller
          case replayed of
            Right (Just record) -> record.scheduleRecordName @?= name
            other -> fail ("expected the replayed schedule, got: " <> show other),
      testCase "listing inside a workflow is a step and replays" $ do
        parent <- freshWorkflowId
        unique <- freshWorkflowId
        let name = "sched-" <> Text.take 8 unique
            caller = Just (WorkflowId parent, 9)
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture parent)
          _ <- createSchedule env (newSchedule name "generate_report" "0 0 * * *") Nothing
          listed <- listSchedules env defaultScheduleFilter caller
          case listed of
            Right records -> assertBool "the schedule is listed" (name `elem` map (.scheduleRecordName) records)
            other -> fail ("expected schedules, got: " <> show other)
          _ <- deleteSchedule env name Nothing
          replayed <- listSchedules env defaultScheduleFilter caller
          case replayed of
            Right records -> assertBool "the replay lists it" (name `elem` map (.scheduleRecordName) records)
            other -> fail ("expected the replayed schedules, got: " <> show other)
    ]

-- | The stored record for a schedule this test just created.
readSchedule :: PostgresSystemDB -> Text -> IO ScheduleRecord
readSchedule env name = do
  found <- getSchedule env name Nothing
  case found of
    Right (Just record) -> pure record
    other -> fail ("expected the schedule, got: " <> show other)

-- | The names a listing returns; the shape most schedule assertions take.
-- Mirrors the Rust helper of the same name.
scheduleNames :: PostgresSystemDB -> ScheduleFilter -> IO [Text]
scheduleNames env scheduleFilter = do
  listed <- listSchedules env scheduleFilter Nothing
  case listed of
    Right records -> pure (map (.scheduleRecordName) records)
    other -> fail ("expected schedules, got: " <> show other)

-- | A raw write of one schedule's @last_fired_at@, for the spellings another
-- implementation writes. The values are test literals, so the script is safe
-- to build by concatenation.
setScheduleLastFiredAtRaw :: PostgresSystemDB -> Text -> Text -> IO ()
setScheduleLastFiredAtRaw env name stored = do
  result <-
    runSession
      env
      "fixture"
      ( Session.script
          ( "update dbos.workflow_schedules set last_fired_at = '"
              <> stored
              <> "' where schedule_name = '"
              <> name
              <> "'"
          )
      )
  case result of
    Left err -> fail ("last_fired_at fixture failed: " <> show err)
    Right () -> pure ()

-- | A create or upsert refused because a peer holds the name.
refusedHolder :: Show a => Either Error a -> Text -> IO ()
refusedHolder result holder =
  case result of
    Left (RegisteredByAnother {kind = kind, holder = owner}) -> do
      kind @?= "Schedule"
      owner @?= holder
    other -> fail ("expected RegisteredByAnother held by " <> Text.unpack holder <> ", got: " <> show other)

-- | A write addressed to a name nothing holds.
refusedMissing :: Show a => Either Error a -> Text -> IO ()
refusedMissing result name =
  case result of
    Left (NotRegistered {kind = kind, name = missing}) -> do
      kind @?= "Schedule"
      missing @?= name
    other -> fail ("expected NotRegistered, got: " <> show other)

-- | A caller-bearing schedule method refuses before any database work,
-- naming the method.
expectCallerRefused :: Show a => Text -> Either Error a -> IO ()
expectCallerRefused method result =
  case result of
    Left (Malformed message) ->
      assertBool ("the refusal names " <> show method) (method `Text.isInfixOf` message)
    other -> fail ("expected a Malformed refusal naming " <> Text.unpack method <> ", got: " <> show other)

