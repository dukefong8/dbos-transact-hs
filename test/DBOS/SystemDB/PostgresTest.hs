{-# LANGUAGE OverloadedStrings #-}

module DBOS.SystemDB.PostgresTest (tests, streamTests) where

import DBOS.Prelude
import DBOS.SystemDB.Error (decodePortableError)
import DBOS.SystemDB
  ( AppScope (..),
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
    Dequeued (..),
    DequeueStop (..),
    countPendingQueueWorkflows,
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
    IdleTransactionTimeout (..),
    InitWorkflowCaller (..),
    EncodedSendMessage (..),
    SerializedWorkflowValue (..),
    Serialization (..),
    portableJsonSerialization,
    NotificationRecord (..),
    AwaitedStream (..),
    Topic (..),
    VersionInfo (..),
    StepRecord (..),
    StepTiming (..),
    DatabaseTime (..),
    StreamRead (..),
    StreamRecord (..),
    SysRewindOptions (..),
    SystemDB (..),
    WrittenBy (..),
    WorkflowDelay (..),
    WorkflowFilter (..),
    WorkflowId (..),
    WorkflowInitResult (..),
    WorkflowOwner (..),
    WorkflowRecord (..),
    WorkflowStatus (..),
    ApplicationRowCounts (..),
    ApplicationName,
    QueueRecord (..),
    QueueUpdate (..),
    RateLimit (..),
    RenameBatching (..),
    ScheduleFilter (..),
    ScheduleRecord (..),
    ScheduleStatus (..),
    ScheduleUpdate (..),
    defaultQueueUpdate,
    defaultScheduleFilter,
    defaultScheduleUpdate,
    durationFromSecs,
    RenameFrom (..),
    defaultForkOptions,
    defaultRetryPolicy,
    defaultWorkflowFilter,
    addTimeout,
    messageTo,
    newSchedule,
    millisDuration,
    newWorkflowKey,
    newWorkflow,
    parseApplicationName,
    secondsDuration,
    spansPartitions,
    sleepStepName,
    streamClosedSentinel,
    timestampFromEpochMs,
    timestampNow,
    timestampToEpochMs,
  )
import DBOS.SystemDB.Postgres.Statements qualified as Statements
import DBOS.SystemDB.Postgres.Backend qualified as Backend
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
    PollingConcurrency (PollingDefault, PollingUnlimited),
    pollingAtMost,
    pollingPermits,
    releasePostgresSystemDB,
    runSession,
    verifySystemDatabase,
  )
import Data.Int (Int64)
import Data.List (sort)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import DBOS.Transact.Logger (nullTracer)
import Data.UUID.V4 qualified as UUID.V4
import Hasql.Connection qualified as Connection
import Hasql.Connection.Settings qualified as ConnectionSettings
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Errors qualified as Errors
import Hasql.Pool qualified as Pool
import Hasql.Pool.Config qualified as PoolConfig
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import Hasql.Transaction qualified as Tx
import Pqi.Ffi qualified as Pqi
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
      testCase "a step listing without outputs leaves the payload columns out of the SQL" $ do
        let onSql = Statements.listStepsSql True
            offSql = Statements.listStepsSql False
        assertBool "the default reads both payload columns" ("output, error" `Text.isInfixOf` onSql)
        assertBool "the metadata listing does not select output" (not (", output" `Text.isInfixOf` offSql))
        assertBool "the metadata listing does not select error" (not (", error" `Text.isInfixOf` offSql))
        assertBool "the listing pages in step order" ("order by function_id" `Text.isInfixOf` offSql)
        assertBool "limit and offset are bound" ("limit $2 offset $3" `Text.isInfixOf` offSql),
      lifecycleTests getBackend,
      idleTimeoutTests getBackend,
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
withBackendAs getBackend appName = withBackendSettings getBackend (defaultSettings {settingsAppName = appName})

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

-- | The validated rename target the API takes. Fails the test when the name a
-- case built does not satisfy the oracle's rule; a case that means to pin that
-- refusal calls 'parseApplicationName' directly.
renameTarget :: Text -> IO ApplicationName
renameTarget name = either (fail . show) pure (parseApplicationName name)

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
    fixtureCreatorXid :: Maybe Text,
    fixtureRecoveryAttempts :: Maybe Int64,
    fixtureVersion :: Maybe Text,
    fixtureDelayUntil :: Maybe Int64
  }

-- | The token a fixture row is owned by: tests act as the execution that
-- created their rows, so every direct write presents it. Derived from the
-- id, which the suite already keeps unique per test.
fixtureOwner :: Text -> Text
fixtureOwner wid = "owner-" <> wid

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
      -- Tests own their rows: direct writes present this token, as the
      -- execution that created them would. Claims overwrite it, and
      -- cancel/resume/recovery clear it, so lifecycle assertions hold.
      fixtureOwnerXid = Just (fixtureOwner wid),
      fixtureCreatorXid = Just (fixtureOwner wid),
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
    ( "insert into dbos.workflow_status (workflow_uuid, status, name, created_at, updated_at, priority, was_forked_from, rate_limited, is_debounced, parent_workflow_id, forked_from, application_name, queue_name, inputs, output, error, serialization, deduplication_id, queue_partition_key, executor_id, owner_xid, creator_xid, recovery_attempts, application_version, delay_until_epoch_ms) values ('"
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
        <> quoted row.fixtureCreatorXid
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
-- payload-table seeds. The owner columns carry the test's token (legacy is
-- about the payload shape): setup steps record through the fence.
seedPayloadRow :: Text -> Text -> (Maybe Text, Maybe Text, Maybe Text) -> (Maybe Text, Maybe Text, Maybe Text) -> Session.Session ()
seedPayloadRow wid status (legacyInput, legacyOutput, legacyError) (newInput, newOutput, newError) =
  Session.script
    ( "insert into dbos.workflow_status (workflow_uuid, status, name, created_at, updated_at, priority, was_forked_from, rate_limited, is_debounced, recovery_attempts, inputs, output, error, owner_xid, creator_xid) values ('"
        <> wid
        <> "', '"
        <> status
        <> "', 'fixture', 0, 0, 0, false, false, false, 0, "
        <> lit legacyInput
        <> ", "
        <> lit legacyOutput
        <> ", "
        <> lit legacyError
        <> ", '" <> fixtureOwner wid <> "', '" <> fixtureOwner wid <> "'); "
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

-- | Initialize a row and hand back its creator token: a test acting as the
-- execution that created the row presents it on every direct write. Raw
-- fixture rows carry 'fixtureOwner' instead; never mix the two on one row.
initOwnedRow :: PostgresSystemDB -> NewWorkflow -> Maybe InitWorkflowCaller -> IO Text
initOwnedRow env new caller = do
  created <- initWorkflow env new caller
  case created of
    Left err -> fail ("fixture init failed: " <> show err)
    Right result -> pure result.initResultCreatorXid

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

-- | Mirrors @postgres.rs@ polling-cap tests against the 'pollingPermits'
-- method. Rust's off value is @tokio Semaphore::MAX_PERMITS@; the Haskell
-- port's inexhaustible count is 'maxBound'.
pollingTests :: TestTree
pollingTests =
  testGroup
    "Polling cap"
    [ -- Mirrors @the_default_polling_cap_is_half_the_pool@.
      testCase "the default polling cap is half the pool" $ do
        pollingPermits PollingDefault 10 @?= 5
        pollingPermits PollingDefault 21 @?= 10,
      -- Mirrors @a_pool_too_small_to_halve_still_admits_one_poll@: half of
      -- one is zero, which as a cap would block every poll forever.
      testCase "a pool too small to halve still admits one poll" $ do
        pollingPermits PollingDefault 1 @?= 1
        pollingPermits PollingDefault 0 @?= 1,
      -- Mirrors @a_configured_polling_cap_is_taken_as_given@: above the pool
      -- size is the caller's business; neither reference rejects it either.
      testCase "a configured polling cap is taken as given" $ do
        pollingPermits <$> pollingAtMost 3 <*> pure 10 @?= Just 3
        pollingPermits <$> pollingAtMost 100 <*> pure 10 @?= Just 100,
      -- A zero cap cannot be written: the only way to build the bounded
      -- case refuses zero, mirroring @NonZeroU32@.
      testCase "a zero polling cap is refused" $
        pollingAtMost 0 @?= Nothing,
      -- Mirrors @an_unlimited_polling_cap_never_runs_out@: off is a permit
      -- count nothing will exhaust, not an absent semaphore.
      testCase "an unlimited polling cap never runs out" $
        pollingPermits PollingUnlimited 10 @?= maxBound
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
      testCase "an idle session the server ended is a lost connection" $
        kindOf (serverUsage "25P03" "terminating connection due to idle-in-transaction timeout") @?= Connection,
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
        defaultSettings @?= Settings "dbos" defaultRetryPolicy Nothing Nothing PollingDefault Nothing,
      testCase "configNew takes the URL with shared defaults" $ do
        let config = configNew "postgresql://u:p@host:5432/db"
        config.configUrl @?= "postgresql://u:p@host:5432/db"
        config.configMaxConnections @?= 10
        config.configIdleTransactionTimeout @?= Default
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
          permits @?= pollingPermits PollingDefault (fromIntegral config.configMaxConnections),
      testCase "runs a session through the pool" $
        withBackend getBackend $ \env -> do
          result <- runSession env "probe" migrationVersionSession
          case result of
            Right (Just (DbosMigration version)) ->
              assertBool ("expected at least migration 125, got: " <> show version) (version >= 125)
            other -> fail ("expected an applied migration version, got: " <> show other),
      -- The dispatch times a claimed row's deadline from the database's
      -- clock, so the seam has to answer with it, not the host's.
      testCase "databaseTime reads the database's clock" $
        withBackend
          getBackend
          ( \env -> do
              read <- databaseTime env
              case read of
                Left err -> fail ("expected a clock reading, got: " <> show err)
                Right clock -> do
                  direct <-
                    runSession env "clock" $
                      Session.statement () $
                        Statement.preparable
                          "select (extract(epoch from clock_timestamp()) * 1000)::bigint"
                          Encoders.noParams
                          (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))
                  case direct of
                    Left err -> fail ("expected a clock reading, got: " <> show err)
                    Right epochMs ->
                      let seam = clock.databaseTimeEpochMs
                       in assertBool
                            ("expected the seam to agree with the statement, got " <> show seam <> " and " <> show epochMs)
                            (abs (seam - epochMs) < 2000)
          ),
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

-- | The idle-transaction timeout connect sets: each variant read back
-- from the server it was set on, so backend formatting differences
-- cannot hide a missed @SET@. Owns no rows and opens its own pools, so
-- these run safely alongside anything else.
idleTimeoutTests :: IO PostgresSystemDB -> TestTree
idleTimeoutTests getBackend =
  testGroup
    "Idle transaction timeout"
    [ testCase "connect sets sixty seconds by default" $ do
        baseConfig <- configFromEnv
        let base = baseConfig.configUrl
        shown <- connected base Default
        reference <- connected (withOptions base "options=-c%20idle_in_transaction_session_timeout%3D60000") ServerSetting
        shown @?= reference,
      testCase "connect sets an explicit timeout" $ do
        baseConfig <- configFromEnv
        let base = baseConfig.configUrl
        shown <- connected base (After (millisDuration 300))
        reference <- connected (withOptions base "options=-c%20idle_in_transaction_session_timeout%3D300") ServerSetting
        shown @?= reference,
      testCase "a positive timeout under a millisecond is a millisecond" $ do
        baseConfig <- configFromEnv
        let base = baseConfig.configUrl
        subMs <- case durationFromSecs 0.0005 of
          Nothing -> fail "expected a sub-millisecond duration"
          Just duration -> pure duration
        shown <- connected base (After subMs)
        reference <- connected (withOptions base "options=-c%20idle_in_transaction_session_timeout%3D1") ServerSetting
        shown @?= reference,
      testCase "ServerSetting leaves the server's setting" $ do
        baseConfig <- configFromEnv
        let base = baseConfig.configUrl
        shown <- connected base ServerSetting
        -- The server's own value, read over a raw connection that sets
        -- nothing: the suite pool carries the default sixty seconds.
        server <- rawShow base
        shown @?= server,
      testCase "the URL's own options keep theirs, however spelled" $ do
        baseConfig <- configFromEnv
        let base = baseConfig.configUrl
        owned <- connected (withOptions base "options=-c%20idle_in_transaction_session_timeout%3D1234") ServerSetting
        forM_
          [ "options=-c%20idle_in_transaction_session_timeout%3D1234",
            "options=-c%20IDLE%5FIN%5FTRANSACTION%5FSESSION%5FTIMEOUT%3D1234",
            "options=--idle-in-transaction-session-timeout%3D1234"
          ]
          ( \query -> do
              kept <- connected (withOptions base query) Default
              kept @?= owned
          ),
      testCase "a bare parameter the URL cannot carry is refused at once" $ do
        baseConfig <- configFromEnv
        let base = baseConfig.configUrl
        -- libpq rejects a bare server parameter the URL cannot carry, so
        -- there is nothing to connect with: without connection retries
        -- the refusal surfaces at once as a connection failure rather
        -- than blocking on a database that will never answer. The
        -- detection itself (the parameter sets nothing) is unit-proven;
        -- sqlx accepts this URL where libpq does not, so the oracle's
        -- twin connects here and ours cannot.
        outcome <-
          try
            ( acquirePostgresSystemDB
                ((configNew (withOptions base "idle_in_transaction_session_timeout=1234")) {configIdleTransactionTimeout = Default, configSettings = defaultSettings {settingsRetry = defaultRetryPolicy {retryPolicyRetryConnectionErrors = False}}})
                nullTracer
            )
        case outcome of
          Left (Backend backend) -> backend.backendKind @?= Connection
          _ -> fail "expected a refused connection",
      testCase "a value that merely names the parameter sets nothing" $ do
        baseConfig <- configFromEnv
        let base = baseConfig.configUrl
        sixty <- connected base Default
        set <- connected (withOptions base "options=-c%20application_name%3Didle_in_transaction_session_timeout") Default
        set @?= sixty,
      testCase "a pool the caller built is left alone" $ do
        baseConfig <- configFromEnv
        let base = baseConfig.configUrl
        server <- rawShow base
        -- A pool `connect` never saw, wrapped by `fromPool`: no SET runs
        -- on its connections, so it reads the server's own value rather
        -- than the sixty seconds a connected pool carries.
        bracket
          (Pool.acquire Pqi.adapter (PoolConfig.settings [PoolConfig.size 1, PoolConfig.staticConnectionSettings (ConnectionSettings.connectionString base)]))
          Pool.release
          $ \pool -> do
            env <- fromPool pool 1 defaultSettings nullTracer
            shown <- showIdleTimeout env
            shown @?= server,
      testCase "a bad timeout is refused before anything connects" $ do
        baseConfig <- configFromEnv
        let base = baseConfig.configUrl
        outcome <- try (acquirePostgresSystemDB ((configNew base) {configIdleTransactionTimeout = After (millisDuration 0)}) nullTracer)
        case outcome of
          Left (InvalidInput {}) -> pure ()
          _ -> fail "expected InvalidInput",
      testCase "the server ends a transaction left idle" $ do
        baseConfig <- configFromEnv
        let base = baseConfig.configUrl
        -- A session-level advisory lock stands in for a row lock: the
        -- server releases it with the session, and a second session can
        -- take it at once only if the first is gone. The dead session
        -- itself reports whatever the driver saw on next use (usually a
        -- closed connection, with no SQLSTATE to classify), so the twin
        -- asserts the failure plus the lock's release — the 25P03 code
        -- itself is pinned by the classifier unit twin. The key folds in
        -- a fresh id, so two suites sharing one server never share a
        -- lock.
        lockId <- freshWorkflowId
        let lockKey = Text.foldl' (\key char -> key * 31 + fromIntegral (fromEnum char)) 0 lockId
            lockSql = Text.pack (show lockKey)
        outcome <-
          bracket
            (Connection.acquire Pqi.adapter (ConnectionSettings.connectionString base) >>= either (fail . show) pure)
            Connection.release
            $ \idle -> do
              _ <- Connection.use idle (Session.script "set idle_in_transaction_session_timeout = 300") >>= either (fail . show) pure
              _ <- Connection.use idle (Session.script "begin") >>= either (fail . show) pure
              _ <- Connection.use idle (Session.script ("select pg_advisory_lock(" <> lockSql <> ")")) >>= either (fail . show) pure
              threadDelay 1000000
              Connection.use idle (Session.statement () Statements.showIdleTimeoutStatement)
        case outcome of
          Left _ -> pure ()
          Right _ -> fail "expected the idle session to be ended"
        bracket
          (Connection.acquire Pqi.adapter (ConnectionSettings.connectionString base) >>= either (fail . show) pure)
          Connection.release
          $ \fresh -> do
            taken <- Connection.use fresh (Session.statement lockKey Statements.tryAdvisoryLockStatement) >>= either (fail . show) pure
            taken @?= True
    ]
  where
    connected url timeout =
      bracket
        (acquirePostgresSystemDB ((configNew url) {configIdleTransactionTimeout = timeout}) nullTracer)
        releasePostgresSystemDB
        showIdleTimeout
    showIdleTimeout env = do
      result <- runSession env "fixture-show-idle-timeout" (Session.statement () Statements.showIdleTimeoutStatement)
      case result of
        Left err -> fail (show err)
        Right value -> pure value
    -- The server's own reading over a raw connection that sets nothing.
    rawShow url =
      bracket
        (Connection.acquire Pqi.adapter (ConnectionSettings.connectionString url) >>= either (fail . show) pure)
        Connection.release
        $ \raw -> Connection.use raw (Session.statement () Statements.showIdleTimeoutStatement) >>= either (fail . show) pure
    withOptions url query = url <> (if "?" `Text.isInfixOf` url then "&" else "?") <> query

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
      testCase "the stream and event notify triggers are gone" $
        withBackend getBackend $ \env -> do
          triggers <-
            probeSchemaNames
              env
              "schema-triggers"
              "select t.tgname::text from pg_trigger t join pg_class c on c.oid = t.tgrelid join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'dbos' and c.relname in ('streams', 'workflow_events') and not t.tgisinternal order by t.tgname"
          functions <-
            probeSchemaNames
              env
              "schema-functions"
              "select p.proname::text from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'dbos' and p.proname in ('streams_function', 'workflow_events_function') order by p.proname"
          assertBool ("streams/workflow_events triggers are gone, got: " <> show triggers) (null triggers)
          assertBool ("streams/workflow_events trigger functions are gone, got: " <> show functions) (null functions),
      testCase "the notifications notify trigger remains" $
        withBackend getBackend $ \env -> do
          triggers <-
            probeSchemaNames
              env
              "schema-notify-triggers"
              "select t.tgname::text from pg_trigger t join pg_class c on c.oid = t.tgrelid join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'dbos' and c.relname = 'notifications' and not t.tgisinternal order by t.tgname"
          functions <-
            probeSchemaNames
              env
              "schema-notify-functions"
              "select p.proname::text from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'dbos' and p.proname = 'notifications_function' order by p.proname"
          assertBool ("the notifications trigger remains, got: " <> show triggers) ("dbos_notifications_trigger" `elem` triggers)
          assertBool ("the notifications function remains, got: " <> show functions) ("notifications_function" `elem` functions),
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

-- | One text column from a schema-catalog query. Labels the session so a
-- failure names the probe that asked.
probeSchemaNames :: PostgresSystemDB -> Text -> Text -> IO [Text]
probeSchemaNames env label sql = do
  result <-
    runSession env label $
      Session.statement () $
        Statement.preparable sql Encoders.noParams (Decoders.rowList (Decoders.column (Decoders.nonNullable Decoders.text)))
  case result of
    Left err -> fail (show err)
    Right names -> pure names

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
    "consumed",
    "consumed_by_function_id"
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
      testCase "a rewind clears a legacy-only outcome with its steps" $ do
        base <- freshWorkflowId
        let legacyOnly = base <> "-legacy"
            newOnly = base <> "-new"
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (seedPayloadRow legacyOnly "SUCCESS" (Just "\"old in\"", Just "\"old out\"", Nothing) (Nothing, Nothing, Nothing))
          _ <- runSession env "fixture" (seedPayloadRow newOnly "SUCCESS" (Nothing, Nothing, Nothing) (Just "\"new in\"", Just "\"new out\"", Nothing))
          _ <- recordStep env (WorkflowId legacyOnly) 0 "DBOS.step" (OutcomeOutput (Just "one")) Nothing Nothing (fixtureOwner legacyOnly)
          _ <- recordStep env (WorkflowId newOnly) 0 "DBOS.step" (OutcomeOutput (Just "one")) Nothing Nothing (fixtureOwner newOnly)
          -- The outcomes stand before the rewind, so the cleared read below
          -- proves the transition rather than a vacuous absence.
          checkPayloads env legacyOnly (Just "\"old in\"") (Just "\"old out\"") Nothing
          checkPayloads env newOnly (Just "\"new in\"") (Just "\"new out\"") Nothing
          rewoundLegacy <- rewindWorkflow env (WorkflowId legacyOnly) 0 (SysRewindOptions Nothing Nothing Nothing) Nothing
          rewoundLegacy @?= Right ()
          rewoundNew <- rewindWorkflow env (WorkflowId newOnly) 0 (SysRewindOptions Nothing Nothing Nothing) Nothing
          rewoundNew @?= Right ()
          checkRewound env legacyOnly
          checkRewound env newOnly,
      testCase "a caller step replays by its tag and an unreadable tag is refused" $ do
        base <- freshWorkflowId
        let caller = base <> "-caller"
            target = base <> "-target"
            callerStep = 0
        withBackend getBackend $ \env -> do
          -- A fresh caller and an active target: the refusal is recordable,
          -- so the rewind writes the caller's step.
          _ <- runSession env "fixture" (seedPayloadRow caller "PENDING" (Nothing, Nothing, Nothing) (Nothing, Nothing, Nothing))
          _ <- runSession env "fixture" (seedPayloadRow target "ENQUEUED" (Nothing, Nothing, Nothing) (Nothing, Nothing, Nothing))
          first <- rewindWorkflow env (WorkflowId target) 0 (SysRewindOptions Nothing Nothing Nothing) (Just (WorkflowId caller, callerStep))
          first @?= Left (WorkflowNotRewindable {workflowId = target, status = "ENQUEUED"})
          steps <- listSteps env (WorkflowId caller) True Nothing Nothing Nothing
          case steps of
            Left err -> fail (show err)
            Right [record] -> do
              record.stepRecordSerialization @?= Just portableJsonSerialization
              case record.stepRecordError of
                Just text ->
                  decodePortableError text
                    @?= Right (WorkflowNotRewindable {workflowId = target, status = "ENQUEUED"})
                Nothing -> fail "expected the caller error to be recorded"
            other -> fail ("expected one caller step, got: " <> show other)
          -- The recorded portable error replays as itself.
          second <- rewindWorkflow env (WorkflowId target) 0 (SysRewindOptions Nothing Nothing Nothing) (Just (WorkflowId caller, callerStep))
          second @?= Left (WorkflowNotRewindable {workflowId = target, status = "ENQUEUED"})
          -- A tag this build cannot read is refused, never decoded.
          _ <- runSession env "fixture" (Session.script ("update dbos.operation_outputs set serialization = 'pickle' where workflow_uuid = '" <> caller <> "' and function_id = 0"))
          third <- rewindWorkflow env (WorkflowId target) 0 (SysRewindOptions Nothing Nothing Nothing) (Just (WorkflowId caller, callerStep))
          third @?= Left (UnknownSerialization {name = "pickle"}),
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
    -- | A rewound row is re-enqueued with no outcome standing in either
    -- layout and no steps: the legacy columns read null and the payload
    -- table reads back through them.
    checkRewound env wid = do
      found <- getWorkflow env (WorkflowId wid)
      case found of
        Left err -> fail ("expected the rewound row, got: " <> show err)
        Right Nothing -> fail "expected the rewound row, got Nothing"
        Right (Just record) -> do
          record.workflowRecordStatus @?= Enqueued
          record.workflowRecordOutput @?= Nothing
          record.workflowRecordError @?= Nothing
      steps <- listSteps env (WorkflowId wid) True Nothing Nothing Nothing
      (map (.stepRecordStepName) <$> steps) @?= Right []

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
          named <- fromPool bare.psdbPool 8 (defaultSettings {settingsAppName = mine}) nullTracer
          let scoped env workflowFilter = idsOf env workflowFilter {workflowFilterWorkflowIds = [claimed, unclaimed]}
          -- A handle with no application of its own sees every application's
          -- rows rather than only the unclaimed ones.
          allRows <- scoped bare defaultWorkflowFilter
          sort allRows @?= sort [claimed, unclaimed]
          -- A named handle scopes to its name plus the unclaimed rows.
          own <- sort <$> scoped named defaultWorkflowFilter
          own @?= sort [claimed, unclaimed]
          other <- scoped named defaultWorkflowFilter {workflowFilterAppScope = Named [unique <> "-elsewhere"]}
          other @?= [unclaimed]
          anyRows <- sort <$> scoped named defaultWorkflowFilter {workflowFilterAppScope = AnyApplication}
          anyRows @?= sort [claimed, unclaimed],
      testCase "an id is an address while a prefix stays a search" $ do
        unique <- freshWorkflowId
        let mineName = unique <> "-app"
            theirsName = unique <> "-theirs-app"
            mine = Just mineName
            owned = unique <> "-owned"
            peer = unique <> "-peer"
            nobody = unique <> "-nobody"
        withBackendAs getBackend Nothing $ \bare -> do
          _ <- runSession bare "fixture" (insertFixture (defaultFixture owned) {fixtureApplication = mine})
          _ <- runSession bare "fixture" (insertFixture (defaultFixture peer) {fixtureApplication = Just theirsName})
          _ <- runSession bare "fixture" (insertFixture (defaultFixture nobody))
          named <- fromPool bare.psdbPool 8 (defaultSettings {settingsAppName = mine}) nullTracer
          -- A prefix is a search, not an address, so it stays scoped to the
          -- caller's own rows plus the unclaimed ones.
          let stem = defaultWorkflowFilter {workflowFilterWorkflowIdPrefixes = [unique]}
          search <- sort <$> idsOf named stem
          search @?= sort [owned, nobody]
          -- An id is a global address, so a peer's workflow answers by id
          -- rather than pretending it does not exist.
          byId <- idsOf named defaultWorkflowFilter {workflowFilterWorkflowIds = [peer]}
          byId @?= [peer]
          -- Naming an application still narrows an id-keyed read.
          narrowed <- idsOf named defaultWorkflowFilter {workflowFilterWorkflowIds = [peer], workflowFilterAppScope = Named [mineName]}
          narrowed @?= [],
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
          recorded <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeOutput (Just "done")) (fixtureOwner unique)
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
          first <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeOutput (Just "winner")) (fixtureOwner unique)
          first @?= Right Recorded
          second <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeOutput (Just "loser")) (fixtureOwner unique)
          second @?= Right AlreadyFinished
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> record.workflowRecordOutput @?= Just "winner"
            other -> fail ("expected record, got: " <> show other),
      testCase "a failure records its error and no output" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture unique))
          recorded <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeError "boom") (fixtureOwner unique)
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
          creator <- initOwnedRow env ((newWorkflow unique) {newWorkflowInput = Just "\"in\""}) Nothing
          recorded <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeOutput (Just "\"done\"")) creator
          recorded @?= Right Recorded
          (input, output, errorValue) <- payloadRows env unique
          input @?= Just "\"in\""
          output @?= Just "\"done\""
          errorValue @?= Nothing
          legacyPayloads env unique >>= (@?= (Nothing, Nothing, Nothing))
          creator <- initOwnedRow env ((newWorkflow failed) {newWorkflowInput = Just "\"in\""}) Nothing
          failedRecorded <- recordWorkflowOutcome env (WorkflowId failed) (OutcomeError "\"boom\"") creator
          failedRecorded @?= Right Recorded
          (_, failedOutput, failedError) <- payloadRows env failed
          failedOutput @?= Nothing
          failedError @?= Just "\"boom\""
          legacyPayloads env failed >>= (@?= (Nothing, Nothing, Nothing)),
      testCase "a refused outcome writes no payload" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- initWorkflow env (newWorkflow unique) {newWorkflowInput = Just "\"in\""} Nothing
          _ <- cancelWorkflows env [WorkflowId unique] False Nothing Nothing
          refused <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeOutput (Just "\"late\"")) (fixtureOwner unique)
          refused @?= Right AlreadyFinished
          (_, output, errorValue) <- payloadRows env unique
          output @?= Nothing
          errorValue @?= Nothing,
      testCase "a new outcome hides a legacy one" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <-
            runSession env "fixture" $
              Session.script ("insert into dbos.workflow_status (workflow_uuid, status, name, created_at, updated_at, priority, was_forked_from, rate_limited, is_debounced, recovery_attempts, error, owner_xid, creator_xid) values ('" <> unique <> "', 'PENDING', 'fixture', 0, 0, 0, false, false, false, 0, '\"first run failed\"', '" <> fixtureOwner unique <> "', '" <> fixtureOwner unique <> "')")
          recorded <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeOutput (Just "\"second run\"")) (fixtureOwner unique)
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
          creatorDirect <- initOwnedRow env ((newWorkflow direct) {newWorkflowInput = Just "\"1\""}) Nothing
          _ <- recordWorkflowOutcome env (WorkflowId direct) (OutcomeOutput (Just "\"2\"")) creatorDirect
          creatorQueued <- initOwnedRow env ((newWorkflow queued) {newWorkflowInput = Just "\"2\"", newWorkflowQueueName = Just ("q-" <> base)}) Nothing
          _ <- recordWorkflowOutcome env (WorkflowId queued) (OutcomeOutput (Just "\"4\"")) creatorQueued
          creatorParent <- initOwnedRow env (newWorkflow parent) Nothing
          startedAt <- timestampNow
          creatorChild <-
            initOwnedRow env (newWorkflow child) $
              Just
                ( InitWorkflowCaller
                    { initCallerParentWorkflowId = WorkflowId parent,
                      initCallerStepId = 1,
                      initCallerStepName = "ChildWorkflow",
                      initCallerStartedAt = startedAt,
                      initCallerToken = creatorParent
                    }
                )
          _ <- recordWorkflowOutcome env (WorkflowId child) (OutcomeOutput (Just "\"5\"")) creatorChild
          _ <- recordWorkflowOutcome env (WorkflowId parent) (OutcomeOutput (Just "\"10\"")) creatorParent
          creatorFailed <- initOwnedRow env ((newWorkflow failed) {newWorkflowInput = Just "\"f\""}) Nothing
          _ <- recordWorkflowOutcome env (WorkflowId failed) (OutcomeError "\"boom\"") creatorFailed
          creatorSource <- initOwnedRow env ((newWorkflow source) {newWorkflowInput = Just "\"s\""}) Nothing
          _ <- recordWorkflowOutcome env (WorkflowId source) (OutcomeOutput (Just "\"s\"")) creatorSource
          _ <- forkWorkflows env [(forkNew source) {forkForkedId = Just forked}] defaultForkOptions Nothing
          -- Forks start ownerless; a claim would stamp them. Take the row
          -- for the test the way a claim would, so the outcome records.
          let creatorForked = fixtureOwner forked
          _ <- runSession env "fixture" (Session.script ("update dbos.workflow_status set owner_xid = '" <> creatorForked <> "', creator_xid = '" <> creatorForked <> "' where workflow_uuid = '" <> forked <> "'"))
          _ <- recordWorkflowOutcome env (WorkflowId forked) (OutcomeOutput (Just "\"s\"")) creatorForked
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
          claimed <- startQueuedWorkflows env record "exec" version Nothing 0 0 "tok-c"
          fmap (length . (.dequeuedWorkflowIds)) claimed @?= Right 1
          blocked <- startQueuedWorkflows env record "exec" version Nothing 1 0 "tok-c"
          fmap (.dequeuedWorkflowIds) blocked @?= Right [],
      testCase "an empty partition key is refused" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- upsertQueue env (newQueue name) UpdateExisting
          record <- registeredQueue env name
          refused <- startQueuedWorkflows env record "exec" "v1" (Just "") 0 0 "tok-c"
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
          claimed <- startQueuedWorkflows env record "exec" version Nothing 0 0 "tok-claim-0"
          fmap (.dequeuedWorkflowIds) claimed @?= Right [WorkflowId unique]
          readBack <- getWorkflow env (WorkflowId unique)
          case readBack of
            Right (Just row) -> row.workflowRecordStatus @?= Pending
            other -> fail ("expected the workflow, got: " <> show other),
      testCase "the dequeue claim stamps its token as the owner" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- upsertQueue env (newQueue name) UpdateExisting
          _ <- insertFixtureChecked env (ownedFixture unique) {fixtureQueue = Just name, fixtureStatus = "ENQUEUED"}
          record <- registeredQueue env name
          version <- latestVersion env
          -- One token per claim: the caller mints it, the claim writes it.
          claimed <- startQueuedWorkflows env record "exec" version Nothing 0 0 "tok-claim-1"
          fmap (.dequeuedWorkflowIds) claimed @?= Right [WorkflowId unique]
          readBack <- getWorkflow env (WorkflowId unique)
          case readBack of
            Right (Just row) -> row.workflowRecordOwnerXid @?= Just "tok-claim-1"
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
          withBackendSettings getBackend (defaultSettings {settingsAppName = Just foreignApp}) $ \foreignEnv -> do
            foreignRecord <- registeredQueue foreignEnv name
            foreignClaim <- startQueuedWorkflows foreignEnv foreignRecord "foreign-exec" version Nothing 0 0 "tok-c"
            fmap (.dequeuedWorkflowIds) foreignClaim @?= Right []
          -- The application the row names claims it.
          withBackendSettings getBackend (defaultSettings {settingsAppName = Just ("fixture-" <> unique)}) $ \ownerEnv -> do
            ownerRecord <- registeredQueue ownerEnv name
            ownerClaim <- startQueuedWorkflows ownerEnv ownerRecord "owner-exec" version Nothing 0 0 "tok-c"
            fmap (.dequeuedWorkflowIds) ownerClaim @?= Right [WorkflowId unique],
      testCase "the pending count sees only the queue's running rows" $ do
        unique <- freshWorkflowId
        let orders = "q-orders-" <> Text.take 8 unique
            invoices = "q-invoices-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- upsertQueue env (newQueue orders) UpdateExisting
          _ <- upsertQueue env (newQueue invoices) UpdateExisting
          empty <- countPendingQueueWorkflows env orders
          empty @?= Right 0
          _ <- insertFixtureChecked env (ownedFixture (unique <> "-a")) {fixtureQueue = Just orders, fixtureStatus = "PENDING", fixturePartitionKey = Just "alpha"}
          _ <- insertFixtureChecked env (ownedFixture (unique <> "-b")) {fixtureQueue = Just orders, fixtureStatus = "PENDING", fixturePartitionKey = Just "beta"}
          _ <- insertFixtureChecked env (ownedFixture (unique <> "-c")) {fixtureQueue = Just invoices, fixtureStatus = "PENDING"}
          _ <- insertFixtureChecked env (ownedFixture (unique <> "-w")) {fixtureQueue = Just orders, fixtureStatus = "ENQUEUED", fixturePartitionKey = Just "alpha"}
          counted <- countPendingQueueWorkflows env orders
          counted @?= Right 2,
      testCase "a partition claim names a spent queue-wide concurrency limit" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- upsertQueue env (newQueue name) {newQueueConcurrency = Just 1, newQueuePartitionConcurrency = Just 1} UpdateExisting
          _ <- insertFixtureChecked env (ownedFixture unique) {fixtureQueue = Just name, fixtureStatus = "PENDING", fixturePartitionKey = Just "alpha"}
          _ <- insertFixtureChecked env (ownedFixture (unique <> "-w")) {fixtureQueue = Just name, fixtureStatus = "ENQUEUED", fixturePartitionKey = Just "beta"}
          record <- registeredQueue env name
          version <- latestVersion env
          claimed <- startQueuedWorkflows env record "exec" version (Just "beta") 0 0 "tok-c"
          fmap (.dequeuedWorkflowIds) claimed @?= Right []
          fmap (.dequeuedStoppedBy) claimed @?= Right (Just DequeueStopConcurrency)
          spansPartitions DequeueStopConcurrency @?= True,
      testCase "a partition claim names a spent queue-wide rate limit" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- upsertQueue env (newQueue name) {newQueueRateLimit = Just (RateLimit {rateLimitLimit = 1, rateLimitPeriod = secondsDuration 3600}), newQueuePartitionConcurrency = Just 1} UpdateExisting
          _ <- insertFixtureChecked env (ownedFixture unique) {fixtureQueue = Just name, fixtureStatus = "ENQUEUED", fixturePartitionKey = Just "beta"}
          record <- registeredQueue env name
          version <- latestVersion env
          -- The first claim fills the window and marks the row it took;
          -- the second finds no room left.
          _ <- startQueuedWorkflows env record "exec" version (Just "beta") 0 0 "tok-first"
          claimed <- startQueuedWorkflows env record "exec" version (Just "beta") 0 0 "tok-second"
          fmap (.dequeuedWorkflowIds) claimed @?= Right []
          fmap (.dequeuedStoppedBy) claimed @?= Right (Just DequeueStopRateLimit)
          spansPartitions DequeueStopRateLimit @?= True,
      testCase "a full partition is named as that partition's limit alone" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- upsertQueue env (newQueue name) {newQueueConcurrency = Just 10, newQueuePartitionConcurrency = Just 1} UpdateExisting
          _ <- insertFixtureChecked env (ownedFixture unique) {fixtureQueue = Just name, fixtureStatus = "PENDING", fixturePartitionKey = Just "alpha"}
          _ <- insertFixtureChecked env (ownedFixture (unique <> "-w")) {fixtureQueue = Just name, fixtureStatus = "ENQUEUED", fixturePartitionKey = Just "alpha"}
          record <- registeredQueue env name
          version <- latestVersion env
          claimed <- startQueuedWorkflows env record "exec" version (Just "alpha") 0 0 "tok-c"
          fmap (.dequeuedWorkflowIds) claimed @?= Right []
          fmap (.dequeuedStoppedBy) claimed @?= Right (Just DequeueStopPartitionConcurrency)
          spansPartitions DequeueStopPartitionConcurrency @?= False,
      testCase "a full worker is named as the stop" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- upsertQueue env (newQueue name) {newQueueWorkerConcurrency = Just 1, newQueuePartitionConcurrency = Just 1} UpdateExisting
          _ <- insertFixtureChecked env (ownedFixture (unique <> "-w")) {fixtureQueue = Just name, fixtureStatus = "ENQUEUED", fixturePartitionKey = Just "alpha"}
          record <- registeredQueue env name
          version <- latestVersion env
          claimed <- startQueuedWorkflows env record "exec" version (Just "alpha") 1 0 "tok-c"
          fmap (.dequeuedWorkflowIds) claimed @?= Right []
          fmap (.dequeuedStoppedBy) claimed @?= Right (Just DequeueStopWorkerConcurrency)
          spansPartitions DequeueStopWorkerConcurrency @?= True,
      testCase "a partition sweep takes one head per partition" $ do
        unique <- freshWorkflowId
        let name = "q-" <> Text.take 8 unique
        withBackend getBackend $ \env -> do
          _ <- upsertQueue env (newQueue name) {newQueuePartitionConcurrency = Just 1} UpdateExisting
          _ <- insertFixtureChecked env (ownedFixture unique) {fixtureQueue = Just name, fixtureStatus = "ENQUEUED", fixturePartitionKey = Just "p1"}
          _ <- insertFixtureChecked env (ownedFixture (unique <> "-2")) {fixtureQueue = Just name, fixtureStatus = "ENQUEUED", fixturePartitionKey = Just "p2"}
          record <- registeredQueue env name
          version <- latestVersion env
          swept <- startQueuedPartitionedWorkflows env record "exec" version Nothing "tok-c"
          fmap length swept @?= Right 2
          again <- startQueuedPartitionedWorkflows env record "exec" version Nothing "tok-c"
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
      testCase "a retried insert keeps the input a bounce wrote since" $ do
        -- Mirrors Rust a_retried_insert_keeps_the_input_a_bounce_wrote_since:
        -- the same creator token finds its own row (a lost acknowledgement,
        -- staged through the init seam), and the retry must not undo the
        -- bounce's input replacement.
        unique <- freshWorkflowId
        let wid = "hs-l2-init-retry-" <> Text.take 8 unique
            name = "q-" <> Text.take 8 unique
            key = "dedup-" <> Text.take 8 unique
            creator = "twin-creator-" <> unique
            new input =
              (newWorkflow wid)
                { newWorkflowName = Just "bouncer",
                  newWorkflowQueueName = Just name,
                  newWorkflowDeduplicationId = Just key,
                  newWorkflowDelay = Just (secondsDuration 60),
                  newWorkflowIsDebounced = True,
                  newWorkflowInput = Just input
                }
            request =
              DebounceRequest
                { debounceRequestWorkflow = newWorkflowKey "bouncer",
                  debounceRequestQueueName = name,
                  debounceRequestDeduplicationId = key,
                  debounceRequestPeriod = secondsDuration 1,
                  debounceRequestInputs = Just "{\"n\":2}",
                  debounceRequestSerialization = Nothing,
                  debounceRequestAppName = Nothing
                }
        withBackend getBackend $ \env -> do
          now <- timestampNow
          first <- Backend.initWorkflowAs env (new "{\"n\":1}") creator now
          case first of
            Left err -> fail ("expected the first insert to create, got: " <> show err)
            Right created -> created.initResultShouldExecute @?= True
          bounced <- debounceDelayedWorkflow env request Nothing
          case bounced of
            Left err -> fail ("expected the bounce to extend, got: " <> show err)
            Right _ -> pure ()
          -- A visibly later stamp: real retries arrive after backoff, and
          -- the same-millisecond retry would read inserted and undo the
          -- bounce this test guards.
          later <- pure (timestampFromEpochMs (timestampToEpochMs now + 1000))
          retried <- Backend.initWorkflowAs env (new "{\"n\":1}") creator later
          case retried of
            Left err -> fail ("expected the retry to join its own row, got: " <> show err)
            Right joined -> joined.initResultShouldExecute @?= True
          (input, _, _) <- payloadRows env wid
          input @?= Just "{\"n\":2}",
      testCase "recording the launch twice with the same child is idempotent" $ do
        parent <- freshWorkflowId
        child <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture parent)
          first <- recordChildWorkflow env (WorkflowId parent) (WorkflowId child) 1 "DBOS.child" Nothing (fixtureOwner parent)
          first @?= Right ()
          again <- recordChildWorkflow env (WorkflowId parent) (WorkflowId child) 1 "DBOS.child" Nothing (fixtureOwner parent)
          again @?= Right (),
      testCase "a different child at the same step is nondeterminism" $ do
        parent <- freshWorkflowId
        child <- freshWorkflowId
        rival <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture parent)
          _ <- recordChildWorkflow env (WorkflowId parent) (WorkflowId child) 1 "DBOS.child" Nothing (fixtureOwner parent)
          refused <- recordChildWorkflow env (WorkflowId parent) (WorkflowId rival) 1 "DBOS.child" Nothing (fixtureOwner parent)
          case refused of
            Left (StepNondeterminism {}) -> pure ()
            other -> fail ("expected StepNondeterminism, got: " <> show other),
      testCase "an empty child id is refused, not wedged" $ do
        parent <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture parent)
          refused <- recordChildWorkflow env (WorkflowId parent) (WorkflowId "") 1 "DBOS.child" Nothing (fixtureOwner parent)
          case refused of
            Left (InvalidInput {}) -> pure ()
            other -> fail ("expected InvalidInput, got: " <> show other),
      testCase "a child result records under the result step name" $ do
        parent <- freshWorkflowId
        child <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture parent)
          recorded <- recordChildResult env (WorkflowId parent) 2 (WorkflowId child) (OutcomeOutput (Just "done")) Nothing Nothing (fixtureOwner parent)
          recorded @?= Right ()
          found <- checkStep env (WorkflowId parent) 2 "DBOS.get_result"
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
                    initCallerStartedAt = startedAt,
                    initCallerToken = fixtureOwner parent
                  }
          started <- initWorkflow env (newWorkflow child) (Just caller)
          case started of
            Left err -> fail ("expected a child start, got: " <> show err)
            Right _ -> pure ()
          children <- getWorkflowChildren env (WorkflowId parent)
          children @?= Right [WorkflowId child]
          -- The parent step holds the same child: a rival link is refused.
          rival <- freshWorkflowId
          refused <- recordChildWorkflow env (WorkflowId parent) (WorkflowId rival) 1 "ChildWorkflow" Nothing (fixtureOwner parent)
          case refused of
            Left (StepNondeterminism {}) -> pure ()
            other -> fail ("expected StepNondeterminism, got: " <> show other),
      testCase "a stale parent starts nothing" $ do
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
                    initCallerStartedAt = startedAt,
                    initCallerToken = "someone-else"
                  }
          refused <- initWorkflow env (newWorkflow child) (Just caller)
          case refused of
            Left (WorkflowConflict {}) -> pure ()
            other -> fail ("expected WorkflowConflict, got: " <> show other)
          missing <- getWorkflow env (WorkflowId child)
          missing @?= Right Nothing,
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
    { debounceRequestWorkflow = newWorkflowKey "bouncer",
      debounceRequestQueueName = queueName,
      debounceRequestDeduplicationId = key,
      debounceRequestPeriod = millisDuration 2000,
      debounceRequestInputs = Just "{\"n\":2}",
      debounceRequestSerialization = Nothing,
      debounceRequestAppName = Nothing
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
        withBackendSettings getBackend (defaultSettings {settingsAppName = Just mine}) $ \env -> do
          _ <- createApplicationVersion env versionName (Just otherApp)
          refused <- createApplicationVersion env versionName Nothing
          case refused of
            Left (RegisteredByAnother {}) -> pure ()
            other -> fail ("expected RegisteredByAnother, got: " <> show other),
      -- Twin of the oracle's per-owner latest-version case: with a peer's
      -- versions newer than ours, each application still sees its own
      -- latest (or the unclaimed one), and each probe reads one index
      -- entry rather than walking past the peer's history. Exact names
      -- are unassertable on a shared database (a concurrent case's
      -- rows, and its brief far-future stamps, win any newest-read they
      -- overlap), so the verdicts pin application membership instead:
      -- the probes must never leak a peer's rows, and an application
      -- with no rows of its own must still see the unclaimed ones. The
      -- planted rows carry ancient timestamps, so they never become
      -- anyone's latest and disturb no other case.
      testCase "each application sees its own and unclaimed versions" $ do
        unique <- freshWorkflowId
        let prefix = "slice6-" <> Text.take 8 unique
            ours = prefix <> "-ours"
            peer = prefix <> "-peer"
            ghost = prefix <> "-ghost"
            perOwner = 10000
            plant =
              Session.statement (prefix, perOwner) $
                Statement.preparable
                  "insert into dbos.application_versions (version_id, version_name, version_timestamp, created_at, application_name) \
                  \select $1 || '-v-' || g::text, $1 || '-v-' || g::text, g, g, \
                  \case when g <= $2 then $1 || '-ours' when g <= 2 * $2 then null else $1 || '-peer' end \
                  \from generate_series(1, 3 * $2) g"
                  (contramap fst (Encoders.param (Encoders.nonNullable Encoders.text)) <> contramap snd (Encoders.param (Encoders.nonNullable Encoders.int8)))
                  Decoders.rowsAffected
            explain query =
              Session.statement () $
                Statement.preparable
                  ("explain " <> query)
                  Encoders.noParams
                  (Decoders.rowList (Decoders.column (Decoders.nonNullable Decoders.text)))
            appOf info = info.versionInfoAppName
        withBackend getBackend $ \env -> do
          planted <- runSession env "plant versions" plant
          planted @?= Right (fromIntegral (3 * perOwner))
          -- An application with no rows of its own still sees the
          -- unclaimed ones, and only those.
          ghostLatest <- getLatestApplicationVersion env (Just ghost)
          case ghostLatest of
            Right (Just info) -> appOf info @?= Nothing
            other -> fail ("expected an unclaimed version, got: " <> show other)
          -- Our rows and the unclaimed ones are visible; the peer's never.
          oursLatest <- getLatestApplicationVersion env (Just ours)
          case oursLatest of
            Right (Just info) -> assertBool ("expected our rows only, got: " <> show info) (appOf info == Just ours || appOf info == Nothing)
            other -> fail ("expected our latest, got: " <> show other)
          peerLatest <- getLatestApplicationVersion env (Just peer)
          case peerLatest of
            Right (Just info) -> assertBool ("expected peer rows only, got: " <> show info) (appOf info == Just peer || appOf info == Nothing)
            other -> fail ("expected the peer latest, got: " <> show other)
          namedPlan <-
            runSession env "explain named" $
              explain "(select version_id, version_name, version_timestamp, created_at, application_name from dbos.application_versions where application_name = 'x' order by application_name, version_timestamp desc limit 1) union all (select version_id, version_name, version_timestamp, created_at, application_name from dbos.application_versions where application_name is null order by application_name, version_timestamp desc limit 1) order by version_timestamp desc limit 1"
          case namedPlan of
            Right rows -> assertBool ("expected the owner index in the plan, got: " <> show rows) (any (Text.isInfixOf "idx_application_versions_owner_timestamp") rows)
            other -> fail ("expected a plan, got: " <> show other)
          unnamedPlan <-
            runSession env "explain unnamed" $
              explain "select version_id, version_name, version_timestamp, created_at, application_name from dbos.application_versions order by version_timestamp desc limit 1"
          case unnamedPlan of
            Right rows -> assertBool ("expected the timestamp index in the plan, got: " <> show rows) (any (Text.isInfixOf "idx_application_versions_timestamp") rows)
            other -> fail ("expected a plan, got: " <> show other)
          -- Remove the planted rows: unique to this test, so no other case
          -- can miss them, and the shared table stays lean.
          removed <-
            runSession env "remove versions" $
              Session.statement prefix $
                Statement.preparable
                  "delete from dbos.application_versions where version_name like $1 || '%'"
                  (Encoders.param (Encoders.nonNullable Encoders.text))
                  Decoders.rowsAffected
          removed @?= Right (fromIntegral (3 * perOwner))
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
          target <- renameTarget newName
          counts <- renameApplication env (RenameApplication appName) target Unbatched
          counts @?= Right (ApplicationRowCounts 0 0 0 2 0)
          live <- getWorkflow env (WorkflowId inFlight)
          case live of
            Right (Just record) -> record.workflowRecordAppName @?= Just newName
            other -> fail ("expected record, got: " <> show other)
          done <- getWorkflow env (WorkflowId settled)
          case done of
            Right (Just record) -> record.workflowRecordAppName @?= Just newName
            other -> fail ("expected record, got: " <> show other),
      testCase "renaming the unclaimed leaves other applications alone" $ do
        unique <- freshWorkflowId
        let newName = "new-" <> Text.take 8 unique
            unclaimed = unique <> "-unclaimed"
            owned = unique <> "-owned"
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unclaimed)
          _ <- insertFixtureChecked env (defaultFixture owned) {fixtureApplication = Just "other-app"}
          unclaimedTarget <- renameTarget newName
          counts <- renameApplication env RenameUnclaimed unclaimedTarget Unbatched
          case counts of
            Right rowCounts -> assertBool "the unclaimed row moved" (rowCounts.rowCountWorkflows >= 1)
            other -> fail ("expected counts, got: " <> show other)
          kept <- getWorkflow env (WorkflowId owned)
          case kept of
            Right (Just record) -> record.workflowRecordAppName @?= Just "other-app"
            other -> fail ("expected record, got: " <> show other),
      testCase "a batched rename moves the same rows" $ do
        unique <- freshWorkflowId
        let appName = "app-" <> Text.take 8 unique
            newName = "new-" <> Text.take 8 unique
            ids = [unique <> "-" <> Text.pack (show n) | n <- [1 :: Int, 2, 3]]
        withBackend getBackend $ \env -> do
          _ <- traverse_ (\wid -> insertFixtureChecked env (defaultFixture wid) {fixtureApplication = Just appName, fixtureStatus = "SUCCESS"}) ids
          batchedTarget <- renameTarget newName
          counts <- renameApplication env (RenameApplication appName) batchedTarget (Batched 2)
          case counts of
            Right rowCounts -> rowCounts.rowCountWorkflows @?= 3
            other -> fail ("expected counts, got: " <> show other),
      testCase "an invalid name is refused where it is parsed, and the same one by the backend" $ do
        unique <- freshWorkflowId
        let appName = "app-" <> Text.take 8 unique
        parseApplicationName "UPPER"
          @?= Left (invalidInput "new_name" "must be 3 to 30 characters of lowercase letters, digits, dashes and underscores")
        withBackend getBackend $ \env -> do
          sameTarget <- renameTarget appName
          same <- renameApplication env (RenameApplication appName) sameTarget Unbatched
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
          _ <- setEvent env (WorkflowId source) 1 "progress" "halfway" Nothing (fixtureOwner source)
          _ <- recordStep env (WorkflowId source) 2 "DBOS.step" (OutcomeOutput (Just "two")) Nothing Nothing (fixtureOwner source)
          _ <- recordStep env (WorkflowId source) 3 "DBOS.step" (OutcomeOutput (Just "three")) Nothing Nothing (fixtureOwner source)
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
          _ <- recordStep env (WorkflowId source) 1 "DBOS.step" (OutcomeOutput (Just "one")) Nothing Nothing (fixtureOwner source)
          _ <- recordStep env (WorkflowId source) 2 "DBOS.step" (OutcomeOutput (Just "two")) Nothing Nothing (fixtureOwner source)
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
            other -> fail ("expected NoForkPoint, got: " <> show other),
      testCase "an invalid fork point is refused before anything runs" $ do
        source <- freshWorkflowId
        withBackend getBackend $ \env -> do
          negative <- forkFrom env [WorkflowId source] (ForkStep (-1)) defaultForkOptions Nothing
          case negative of
            Left (InvalidInput {field, detail}) -> (field, detail) @?= ("start_step", "must not be negative")
            other -> fail ("expected InvalidInput, got: " <> show other)
          empty <- forkFrom env [WorkflowId ""] (ForkStep 0) defaultForkOptions Nothing
          case empty of
            Left (InvalidInput {field, detail}) -> (field, detail) @?= ("source_id", "must not be empty")
            other -> fail ("expected InvalidInput, got: " <> show other)
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
      testCase "the timeout sweep cancels expired rows and spares live ones" $ do
        expired <- freshWorkflowId
        live <- freshWorkflowId
        let sweepApp = "sweep-app-" <> expired
        withBackend getBackend $ \env -> do
          let plant wid deadline =
                initWorkflow
                  env
                  ( (newWorkflow wid)
                      { newWorkflowDeadline = Just deadline,
                        newWorkflowAppName = Just sweepApp
                      }
                  )
                  Nothing
          _ <- plant expired (timestampFromEpochMs 1000)
          _ <- plant live (timestampFromEpochMs 4102444800000)
          -- Scoped to this test's application: the guard test below plants
          -- expired rows of its own in parallel, and an unscoped sweep
          -- would cancel them first (observed once as a `taken` miss).
          swept <- withBackendAs getBackend (Just sweepApp) $ \sweepEnv -> do
            result <- cancelTimedOutWorkflows sweepEnv 100
            case result of
              Left err -> fail ("expected a sweep, got: " <> show err)
              Right ids -> pure ids
          assertBool "the expired row swept" (WorkflowId expired `elem` swept)
          assertBool "the live row spared" (WorkflowId live `notElem` swept)
          ended <- getWorkflow env (WorkflowId expired)
          case ended of
            Right (Just record) -> record.workflowRecordStatus @?= Cancelled
            other -> fail ("expected record, got: " <> show other)
          keptLive <- getWorkflow env (WorkflowId live)
          case keptLive of
            Right (Just record) -> record.workflowRecordStatus @?= Pending
            other -> fail ("expected record, got: " <> show other),
      testCase "the timed-out cancel honors ownership" $ do
        owned <- freshWorkflowId
        withBackend getBackend $ \env -> do
          -- A private application scope: the sweep test in this group
          -- cancels expired rows of the shared app in parallel, and must
          -- never see this row (nor sweep it first). The single-row cancel
          -- takes no application filter, so the scope changes nothing it
          -- asserts.
          let scoped = (newWorkflow owned) {newWorkflowDeadline = Just (timestampFromEpochMs 1000), newWorkflowAppName = Just ("guard-app-" <> owned)}
          _ <- initWorkflow env scoped Nothing
          refused <- cancelIfTimedOut env (WorkflowId owned) "someone-else"
          refused @?= Right False
          held <- getWorkflow env (WorkflowId owned)
          owner <- case held of
            Right (Just record) -> do
              record.workflowRecordStatus @?= Pending
              case record.workflowRecordOwnerXid of
                Just token -> pure token
                Nothing -> fail "expected an owner on the row"
            other -> fail ("expected record, got: " <> show other)
          taken <- cancelIfTimedOut env (WorkflowId owned) owner
          taken @?= Right True
          ended <- getWorkflow env (WorkflowId owned)
          case ended of
            Right (Just record) -> record.workflowRecordStatus @?= Cancelled
            other -> fail ("expected record, got: " <> show other),
      testCase "dead-letter parks only over-limit pending rows" $ do
        over <- freshWorkflowId
        within <- freshWorkflowId
        settled <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env ((defaultFixture over) {fixtureRecoveryAttempts = Just 150})
          _ <- insertFixtureChecked env ((defaultFixture within) {fixtureRecoveryAttempts = Just 3})
          _ <- insertFixtureChecked env ((defaultFixture settled) {fixtureStatus = "SUCCESS", fixtureRecoveryAttempts = Just 150})
          parked <- deadLetterWorkflows env (map WorkflowId [over, within, settled]) 100
          parked @?= Right [WorkflowId over]
          parkedRow <- getWorkflow env (WorkflowId over)
          case parkedRow of
            Right (Just record) -> record.workflowRecordStatus @?= MaxRecoveryAttemptsExceeded
            other -> fail ("expected record, got: " <> show other)
          keptRow <- getWorkflow env (WorkflowId within)
          case keptRow of
            Right (Just record) -> record.workflowRecordStatus @?= Pending
            other -> fail ("expected record, got: " <> show other),
      testCase "cancelling stops at terminal rows and can take children" $ do
        unique <- freshWorkflowId
        let root = unique <> "-root"
            child = unique <> "-child"
            grandchild = unique <> "-grandchild"
            settled = unique <> "-settled"
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture root) {fixtureOwnerXid = Just "held-owner"}
          _ <- insertFixtureChecked env (defaultFixture child) {fixtureParent = Just root}
          _ <- insertFixtureChecked env (defaultFixture grandchild) {fixtureParent = Just child}
          _ <- insertFixtureChecked env (defaultFixture settled) {fixtureStatus = "SUCCESS"}
          plain <- cancelWorkflows env [WorkflowId settled] False Nothing Nothing
          plain @?= Right []
          cancelled <- cancelWorkflows env [WorkflowId root] True Nothing Nothing
          case cancelled of
            Right ids -> sort (map unId ids) @?= sort [root, child, grandchild]
            other -> fail ("expected the tree, got: " <> show other)
          found <- getWorkflow env (WorkflowId child)
          case found of
            Right (Just record) -> record.workflowRecordStatus @?= Cancelled
            other -> fail ("expected record, got: " <> show other)
          released <- getWorkflow env (WorkflowId root)
          case released of
            -- A cancelled row belongs to nobody: the execution that held
            -- it is refused at its next write.
            Right (Just record) -> record.workflowRecordOwnerXid @?= Nothing
            other -> fail ("expected record, got: " <> show other),
      testCase "a workflow cannot cancel itself" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          refused <- cancelWorkflows env [WorkflowId unique] False Nothing (Just unique)
          case refused of
            Left (InvalidInput {}) -> pure ()
            other -> fail ("expected InvalidInput, got: " <> show other)
          -- The refused cancel wrote nothing: the row still runs.
          survived <- getWorkflow env (WorkflowId unique)
          case survived of
            Right (Just record) -> record.workflowRecordStatus @?= Pending
            other -> fail ("expected the row to survive, got: " <> show other),
      testCase "a workflow cannot resume itself" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique) {fixtureStatus = "CANCELLED"}
          refused <- resumeWorkflows env [WorkflowId unique] (Just "q") Nothing (Just unique)
          case refused of
            Left (InvalidInput {}) -> pure ()
            other -> fail ("expected InvalidInput, got: " <> show other)
          stayed <- getWorkflow env (WorkflowId unique)
          case stayed of
            Right (Just record) -> record.workflowRecordStatus @?= Cancelled
            other -> fail ("expected the row to stay cancelled, got: " <> show other),
      testCase "a cancel cascade reaching the caller names the tree" $ do
        unique <- freshWorkflowId
        let root = unique <> "-root"
            child = unique <> "-child"
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture root)
          _ <- insertFixtureChecked env (defaultFixture child) {fixtureParent = Just root}
          refused <- cancelWorkflows env [WorkflowId root] True Nothing (Just child)
          case refused of
            Left (InvalidInput {detail}) -> detail @?= ("workflow " <> child <> " cannot cancel the tree of " <> root <> ", which contains it")
            other -> fail ("expected InvalidInput, got: " <> show other)
          survived <- getWorkflow env (WorkflowId root)
          case survived of
            Right (Just record) -> record.workflowRecordStatus @?= Pending
            other -> fail ("expected the row to survive, got: " <> show other),
      testCase "resuming reports missing ids and resets the rest" $ do
        present <- freshWorkflowId
        missing <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture present) {fixtureStatus = "CANCELLED", fixtureRecoveryAttempts = Just 3, fixtureOwnerXid = Just "held-owner"}
          absent <- resumeWorkflows env [WorkflowId present, WorkflowId missing] (Just "q") Nothing Nothing
          case absent of
            Left (NonExistentWorkflow {workflowIds = ids}) -> ids @?= [missing]
            other -> fail ("expected the missing id, got: " <> show other)
          resumed <- resumeWorkflows env [WorkflowId present] (Just "q") Nothing Nothing
          case resumed of
            Right ids -> map unId ids @?= [present]
            other -> fail ("expected a resume, got: " <> show other)
          found <- getWorkflow env (WorkflowId present)
          case found of
            Right (Just record) -> do
              record.workflowRecordStatus @?= Enqueued
              record.workflowRecordRecoveryAttempts @?= 0
              record.workflowRecordQueueName @?= Just "q"
              -- A resumed row belongs to nobody until the next claim takes
              -- it: an execution still running it is refused at its next
              -- write rather than running beside the recovery.
              record.workflowRecordOwnerXid @?= Nothing
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
          creator <- initOwnedRow env ((newWorkflow unique) {newWorkflowInput = Just "\"in\""}) Nothing
          _ <- recordStep env (WorkflowId unique) 0 "charge" (OutcomeOutput (Just "1")) Nothing Nothing creator
          _ <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeOutput (Just "1")) creator
          deleted <- deleteWorkflows env [WorkflowId unique] False Nothing
          deleted @?= Right 1
          forM_ ["workflow_status", "workflow_input", "workflow_output", "operation_outputs"] $ \table -> do
            count <- tableRowCount env table unique
            count @?= 0,
      testCase "a stale token's delete is refused and removes nothing" $ do
        target <- freshWorkflowId
        callerRow <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture target)
          _ <- insertFixtureChecked env (defaultFixture callerRow)
          refused <-
            deleteWorkflows
              env
              [WorkflowId target]
              False
              (Just (WorkflowOwner {ownerWorkflowId = WorkflowId callerRow, ownerToken = "someone-else"}))
          case refused of
            Left (WorkflowConflict {}) -> pure ()
            other -> fail ("expected WorkflowConflict, got: " <> show other)
          survived <- getWorkflow env (WorkflowId target)
          case survived of
            Right (Just _) -> pure ()
            other -> fail ("expected the row to survive, got: " <> show other)
    ]
  where
    unId (WorkflowId widText) = widText

-- | Messages: a send delivers to a destination's topic and, from a
-- workflow, records the send as a step; a receive takes the oldest waiting
-- message and records the take in one commit, adopts a rival's record,
-- reports an empty topic as a recorded absence, and lets two receivers on
-- one topic both wait for one message. Mirrors @send_messages@ and @recv@.
messageTests :: IO PostgresSystemDB -> TestTree
messageTests getBackend =
  testGroup
    "Messages"
    [ testCase "a stale token's send is refused and delivers nothing" $ do
        sender <- freshWorkflowId
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture sender)
          _ <- insertFixtureChecked env (defaultFixture destination)
          let stale = Just (WorkflowOwner {ownerWorkflowId = WorkflowId sender, ownerToken = "someone-else"})
              owned = Just (WorkflowOwner {ownerWorkflowId = WorkflowId sender, ownerToken = fixtureOwner sender})
          refused <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "spy" Nothing)) Nothing Nothing stale False
          case refused of
            Left (WorkflowConflict {}) -> pure ()
            other -> fail ("expected WorkflowConflict, got: " <> show other)
          quiet <- getAllNotifications env (WorkflowId destination)
          quiet @?= Right []
          -- The refused send rolled back: the owner's send delivers.
          _ <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "yes" Nothing)) Nothing Nothing owned False
          received <- recv env (WorkflowId destination) 1 2 (Just "approval") (millisDuration 0) (fixtureOwner destination)
          received @?= Right (Just (EncodedValue "yes" Nothing)),
      testCase "a message sent to a workflow can be received" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture destination)
          sent <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "yes" (Just (Serialization "json")))) (Just "json") Nothing Nothing False
          sent @?= Right ()
          received <- recv env (WorkflowId destination) 1 2 (Just "approval") (millisDuration 0) (fixtureOwner destination)
          received @?= Right (Just (EncodedValue "yes" (Just "json"))),
      testCase "an empty topic times out as a recorded absence" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture destination)
          missed <- recv env (WorkflowId destination) 1 2 (Just "approval") (millisDuration 0) (fixtureOwner destination)
          missed @?= Right Nothing
          step <- checkStep env (WorkflowId destination) 1 "DBOS.recv"
          case step of
            Right (Just recorded) -> recorded.stepRecordOutput @?= Nothing
            other -> fail ("expected the recorded receive, got: " <> show other),
      testCase "a replay returns the same message without taking another" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture destination)
          _ <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "first" Nothing)) Nothing Nothing Nothing False
          _ <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "second" Nothing)) Nothing Nothing Nothing False
          first <- recv env (WorkflowId destination) 1 2 (Just "approval") (millisDuration 0) (fixtureOwner destination)
          first @?= Right (Just (EncodedValue "first" Nothing))
          replayed <- recv env (WorkflowId destination) 1 2 (Just "approval") (millisDuration 0) (fixtureOwner destination)
          replayed @?= Right (Just (EncodedValue "first" Nothing))
          next <- recv env (WorkflowId destination) 3 4 (Just "approval") (millisDuration 0) (fixtureOwner destination)
          next @?= Right (Just (EncodedValue "second" Nothing)),
      testCase "everything sent is reported, read or not" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture destination)
          _ <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "first" Nothing)) Nothing Nothing Nothing False
          _ <- recv env (WorkflowId destination) 1 2 (Just "approval") (millisDuration 0) (fixtureOwner destination)
          reported <- getAllNotifications env (WorkflowId destination)
          case reported of
            Right [record] -> do
              record.notificationRecordTopic @?= Just "approval"
              record.notificationRecordConsumed @?= True
            other -> fail ("expected the consumed message, got: " <> show other),
      testCase "two receivers on one topic both wait and share one message" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture destination)
          first <- async (recv env (WorkflowId destination) 1 2 (Just "approval") (millisDuration 300) (fixtureOwner destination))
          second <- async (recv env (WorkflowId destination) 3 4 (Just "approval") (millisDuration 300) (fixtureOwner destination))
          threadDelay 100000
          _ <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "only" Nothing)) Nothing Nothing Nothing False
          answers <- traverse wait [first, second]
          case answers of
            [Right (Just value), Right Nothing] -> value @?= EncodedValue "only" Nothing
            [Right Nothing, Right (Just value)] -> value @?= EncodedValue "only" Nothing
            other -> fail ("expected one take and one recorded absence, got: " <> show other)
          reported <- getAllNotifications env (WorkflowId destination)
          case reported of
            Right records -> length (filter (.notificationRecordConsumed) records) @?= 1
            other -> fail ("expected the consumed message, got: " <> show other),
      testCase "a recv whose message a rival took waits for the next" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture destination)
          tookOne <- async (recv env (WorkflowId destination) 1 2 (Just "approval") (secondsDuration 5) (fixtureOwner destination))
          tookTwo <- async (recv env (WorkflowId destination) 3 4 (Just "approval") (secondsDuration 5) (fixtureOwner destination))
          threadDelay 100000
          _ <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "one" Nothing)) Nothing Nothing Nothing False
          threadDelay 100000
          _ <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "two" Nothing)) Nothing Nothing Nothing False
          answers <- traverse wait [tookOne, tookTwo]
          let bodies = [body | Right (Just value) <- answers, let EncodedValue body _ = value]
          assertBool ("expected one take of each message, got: " <> show answers) (length bodies == 2 && "one" `elem` bodies && "two" `elem` bodies)
          reported <- getAllNotifications env (WorkflowId destination)
          case reported of
            Right records -> length (filter (.notificationRecordConsumed) records) @?= 2
            other -> fail ("expected both messages consumed, got: " <> show other),
      testCase "an empty take records nothing" $ do
        -- The load-bearing semantic of the take-inside-the-wait-loop: a
        -- take that finds nothing writes no step, so a later take for
        -- the same step still waits rather than adopting an absence.
        -- Deterministic where the threaded twin cannot be: one take
        -- consumes, the next misses, and only the miss must leave no
        -- record.
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture destination)
          _ <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "one" Nothing)) Nothing Nothing Nothing False
          let runTake step = Backend.runTransaction env "fixture-take" $ do
                owned <- Statements.checkOwnerTx destination (fixtureOwner destination)
                case owned of
                  Left conflict -> Tx.condemn >> pure (Left conflict)
                  Right () ->
                    Right
                      <$> Statements.recvTx (Statements.RecvParams destination step "approval" 0 0)
          tookFirst <- runTake 1
          case tookFirst of
            Right (Right (Statements.RecvTook message)) -> message @?= Statements.EncodedValueRaw "one" Nothing
            other -> fail ("expected the first take to consume, got: " <> show other)
          missed <- runTake 2
          case missed of
            Right (Right Statements.RecvMissed) -> pure ()
            other -> fail ("expected the empty take to miss unrecorded, got: " <> show other)
          step <- checkStep env (WorkflowId destination) 2 "DBOS.recv"
          step @?= Right Nothing,
      testCase "a stale token's recv take is refused and the message stays parked" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture destination)
          _ <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "held" Nothing)) Nothing Nothing Nothing False
          refused <- recv env (WorkflowId destination) 1 2 (Just "approval") (millisDuration 0) "someone-else"
          case refused of
            Left (WorkflowConflict {}) -> pure ()
            other -> fail ("expected WorkflowConflict, got: " <> show other)
          -- The refused take rolled back: the owner's take still finds it.
          taken <- recv env (WorkflowId destination) 1 2 (Just "approval") (millisDuration 0) (fixtureOwner destination)
          taken @?= Right (Just (EncodedValue "held" Nothing)),
      testCase "a stale token's recv absence is refused and records nothing" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture destination)
          refused <- recv env (WorkflowId destination) 1 2 (Just "approval") (millisDuration 0) "someone-else"
          case refused of
            Left (WorkflowConflict {}) -> pure ()
            other -> fail ("expected WorkflowConflict, got: " <> show other)
          -- Nothing was recorded: no step answers a later read.
          step <- checkStep env (WorkflowId destination) 1 "DBOS.recv"
          step @?= Right Nothing,
      testCase "a send to forks reaches the fork tree" $ do
        source <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture source) {fixtureStatus = "SUCCESS"}
          forked <- forkWorkflows env [(forkNew source) {forkStartStep = 0}] defaultForkOptions Nothing
          case forked of
            Left err -> fail ("expected a fork, got: " <> show err)
            Right [WorkflowId forkedId] -> do
              sent <- sendMessage env (messageTo (WorkflowId source) (Topic "approval") (SerializedWorkflowValue "fan" Nothing)) Nothing Nothing Nothing True
              sent @?= Right ()
              toSource <- recv env (WorkflowId source) 1 2 (Just "approval") (millisDuration 0) (fixtureOwner source)
              toSource @?= Right (Just (EncodedValue "fan" Nothing))
              -- Forks start ownerless; a claim would stamp them. Take the
              -- row for the test the way a claim would, so the read records.
              _ <- runSession env "fixture" (Session.script ("update dbos.workflow_status set owner_xid = '" <> fixtureOwner forkedId <> "', creator_xid = '" <> fixtureOwner forkedId <> "' where workflow_uuid = '" <> forkedId <> "'"))
              toFork <- recv env (WorkflowId forkedId) 1 2 (Just "approval") (millisDuration 0) (fixtureOwner forkedId)
              toFork @?= Right (Just (EncodedValue "fan" Nothing))
            other -> fail ("expected one fork, got: " <> show other),
      testCase "a send to a missing workflow is reported" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          sent <- sendMessage env (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "yes" Nothing)) Nothing Nothing Nothing False
          case sent of
            Left (NonExistentWorkflow {workflowIds = missing}) -> missing @?= [destination]
            other -> fail ("expected NonExistentWorkflow, got: " <> show other),
      testCase "duplicate and empty idempotency keys are refused" $ do
        destination <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture destination)
          let keyed key = (messageTo (WorkflowId destination) (Topic "approval") (SerializedWorkflowValue "yes" Nothing)) {sendIdempotencyKey = Just (IdempotencyKey key)}
          emptyKey <- sendMessage env (keyed "") Nothing Nothing Nothing False
          case emptyKey of
            Left (InvalidInput {}) -> pure ()
            other -> fail ("expected InvalidInput, got: " <> show other)
          duplicated <- sendMessages env [keyed "k", keyed "k"] Nothing Nothing Nothing False
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
          published <- setEvent env (WorkflowId unique) 1 "progress" "halfway" (Just "json") (fixtureOwner unique)
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
          _ <- setEvent env (WorkflowId unique) 1 "progress" "first" Nothing (fixtureOwner unique)
          again <- setEvent env (WorkflowId unique) 1 "progress" "second" Nothing (fixtureOwner unique)
          again @?= Right ()
          found <- getEvent env (WorkflowId unique) "progress" (millisDuration 0) Nothing
          found @?= Right (Just (EncodedValue "first" Nothing)),
      testCase "a wait sees a value published while it waits" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          publisher <- async $ do
            threadDelay 50000
            _ <- setEvent env (WorkflowId unique) 1 "progress" "late" Nothing (fixtureOwner unique)
            pure ()
          found <- getEvent env (WorkflowId unique) "progress" (secondsDuration 5) Nothing
          wait publisher
          found @?= Right (Just (EncodedValue "late" Nothing)),
      testCase "a caller's read records the answer and replays it" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          _ <- setEvent env (WorkflowId unique) 1 "progress" "first" Nothing (fixtureOwner unique)
          let caller = GetEventCaller {getEventCallerWorkflowId = WorkflowId unique, getEventCallerStepId = 2, getEventCallerTimeoutStepId = 3, getEventCallerToken = fixtureOwner unique}
          read1 <- getEvent env (WorkflowId unique) "progress" (millisDuration 0) (Just caller)
          read1 @?= Right (Just (EncodedValue "first" Nothing))
          _ <- setEvent env (WorkflowId unique) 4 "progress" "second" Nothing (fixtureOwner unique)
          read2 <- getEvent env (WorkflowId unique) "progress" (millisDuration 0) (Just caller)
          read2 @?= Right (Just (EncodedValue "first" Nothing))
          step <- checkStep env (WorkflowId unique) 2 "DBOS.get_event"
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
          let caller = GetEventCaller {getEventCallerWorkflowId = WorkflowId unique, getEventCallerStepId = 2, getEventCallerTimeoutStepId = 3, getEventCallerToken = fixtureOwner unique}
          missed <- getEvent env (WorkflowId unique) "never" (millisDuration 0) (Just caller)
          missed @?= Right Nothing
          _ <- setEvent env (WorkflowId unique) 1 "never" "too-late" Nothing (fixtureOwner unique)
          replayed <- getEvent env (WorkflowId unique) "never" (millisDuration 0) (Just caller)
          replayed @?= Right Nothing,
      testCase "every published event is listed by key" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          _ <- setEvent env (WorkflowId unique) 1 "b-key" "two" Nothing (fixtureOwner unique)
          _ <- setEvent env (WorkflowId unique) 2 "a-key" "one" Nothing (fixtureOwner unique)
          listed <- getAllEvents env (WorkflowId unique)
          case listed of
            Right records -> map (.eventKey) records @?= ["a-key", "b-key"]
            other -> fail ("expected the events, got: " <> show other),
      testCase "a stale token's publish is refused and publishes nothing" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          refused <- setEvent env (WorkflowId unique) 1 "progress" "spy" Nothing "someone-else"
          case refused of
            Left (WorkflowConflict {}) -> pure ()
            other -> fail ("expected WorkflowConflict, got: " <> show other)
          -- The refused publish rolled back: the owner's publish lands and
          -- reads back.
          _ <- setEvent env (WorkflowId unique) 1 "progress" "halfway" Nothing (fixtureOwner unique)
          found <- getEvent env (WorkflowId unique) "progress" (millisDuration 0) Nothing
          found @?= Right (Just (EncodedValue "halfway" Nothing)),
      -- The fence opens the write before the replay adopt, as the oracle
      -- orders it: a stale execution replaying a recorded publish is
      -- refused, not handed the recorded answer.
      testCase "a stale token's replayed publish is refused" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          creator <- initOwnedRow env (newWorkflow unique) Nothing
          _ <- setEvent env (WorkflowId unique) 1 "progress" "halfway" Nothing creator
          refused <- setEvent env (WorkflowId unique) 1 "progress" "halfway" Nothing "someone-else"
          case refused of
            Left (WorkflowConflict {}) -> pure ()
            other -> fail ("expected WorkflowConflict, got: " <> show other)
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
          woke <- recordSleep env (WorkflowId unique) 1 (millisDuration 60000) (fixtureOwner unique)
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
          first <- recordSleep env (WorkflowId unique) 1 (millisDuration 60000) (fixtureOwner unique)
          again <- recordSleep env (WorkflowId unique) 1 (millisDuration 5000) (fixtureOwner unique)
          again @?= first,
      testCase "a rival's recorded sleep is adopted" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          _ <- recordStep env (WorkflowId unique) 1 sleepStepName (OutcomeOutput (Just "12345")) (Just "portable_json") Nothing (fixtureOwner unique)
          woke <- recordSleep env (WorkflowId unique) 1 (millisDuration 60000) (fixtureOwner unique)
          woke @?= Right (timestampFromEpochMs 12345)
    ]

-- | The ported stream contract, run against the live backend as part of the
-- standard Postgres test tree.
-- Semantics verified against @postgres.rs@: append-with-computed-offset
-- under a primary-key collision retry, the close sentinel recorded as a
-- close, replay suppressed only for workflow-level writes, and reads that
-- report the producer's status alongside an optional value.
streamTests :: TestTree
streamTests =
  withResource acquireSuiteBackend releasePostgresSystemDB $ \getBackend ->
  testGroup
    "Streams"
    [ testCase "a workflow-written value reads back at its offset" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          written <- writeStream env (WorkflowId unique) 1 "progress" "one" (Just "json") Workflow (fixtureOwner unique)
          written @?= Right ()
          found <- readStreamValues env (WorkflowId unique) "progress" 0 10
          found @?= Right (StreamRead Pending [EncodedValue "one" (Just "json")]),
      testCase "await returns an already-written page" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          _ <- writeStream env (WorkflowId unique) 1 "progress" "one" (Just "json") Step (fixtureOwner unique)
          awaited <- awaitStreamValues env (WorkflowId unique) "progress" 0 10 Nothing (millisDuration 5)
          awaited @?= Right (Values [EncodedValue "one" (Just "json")]),
      testCase "await times out while the producer is pending" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          now <- timestampNow
          let deadline = addTimeout now (millisDuration 5)
          awaited <- awaitStreamValues env (WorkflowId unique) "progress" 0 10 deadline (millisDuration 2)
          awaited @?= Right TimedOut,
      testCase "await ends after a terminal producer has no values" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          _ <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeOutput Nothing) (fixtureOwner unique)
          awaited <- awaitStreamValues env (WorkflowId unique) "progress" 0 10 Nothing (millisDuration 2)
          awaited @?= Right Ended,
      testCase "an offset with no entry still reports the producer's status" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          found <- readStreamValues env (WorkflowId unique) "progress" 0 10
          found @?= Right (StreamRead Pending []),
      testCase "a stream for a missing workflow is reported" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          found <- readStreamValues env (WorkflowId unique) "progress" 0 10
          case found of
            Left (NonExistentWorkflow {}) -> pure ()
            other -> fail ("expected NonExistentWorkflow, got: " <> show other),
      testCase "writes append in offset order" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          _ <- writeStream env (WorkflowId unique) 1 "progress" "one" Nothing Step (fixtureOwner unique)
          _ <- writeStream env (WorkflowId unique) 2 "progress" "two" Nothing Step (fixtureOwner unique)
          entries <- getAllStreamEntries env (WorkflowId unique)
          case entries of
            Right found ->
              map (\entry -> (entry.streamOffset, entry.streamValue)) found @?= [(0, "one"), (1, "two")]
            other -> fail ("expected entries, got: " <> show other),
      testCase "closing writes the sentinel" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          closed <- closeStream env (WorkflowId unique) 1 "progress" Workflow (fixtureOwner unique)
          closed @?= Right ()
          found <- readStreamValues env (WorkflowId unique) "progress" 0 10
          case found of
            Right (StreamRead _ [value]) -> value.encodedValue @?= streamClosedSentinel
            other -> fail ("expected the sentinel, got: " <> show other),
      testCase "a replayed workflow-level write does not append twice" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          _ <- writeStream env (WorkflowId unique) 1 "progress" "one" Nothing Workflow (fixtureOwner unique)
          _ <- writeStream env (WorkflowId unique) 1 "progress" "one" Nothing Workflow (fixtureOwner unique)
          entries <- getAllStreamEntries env (WorkflowId unique)
          case entries of
            Right found -> length found @?= 1
            other -> fail ("expected entries, got: " <> show other),
      testCase "a stale token's stream write is refused and appends nothing" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          refused <- writeStream env (WorkflowId unique) 1 "progress" "spy" Nothing Workflow "someone-else"
          case refused of
            Left (WorkflowConflict {}) -> pure ()
            other -> fail ("expected WorkflowConflict, got: " <> show other)
          -- The refused write rolled back: the owner's write still appends
          -- at offset zero.
          _ <- writeStream env (WorkflowId unique) 1 "progress" "one" Nothing Workflow (fixtureOwner unique)
          entries <- getAllStreamEntries env (WorkflowId unique)
          case entries of
            Right [entry] -> entry.streamOffset @?= 0
            other -> fail ("expected one entry at offset zero, got: " <> show other),
      -- The fence opens the write before the replay adopt, as the oracle
      -- orders it: a stale execution replaying a recorded append is refused
      -- rather than told the append already happened.
      testCase "a stale token's replayed stream write is refused" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          creator <- initOwnedRow env (newWorkflow unique) Nothing
          _ <- writeStream env (WorkflowId unique) 1 "progress" "one" Nothing Workflow creator
          refused <- writeStream env (WorkflowId unique) 1 "progress" "one" Nothing Workflow "someone-else"
          case refused of
            Left (WorkflowConflict {}) -> pure ()
            other -> fail ("expected WorkflowConflict, got: " <> show other)
          entries <- getAllStreamEntries env (WorkflowId unique)
          case entries of
            Right found -> length found @?= 1
            other -> fail ("expected one entry, got: " <> show other),
      -- A missing row is refused by the fence, as the oracle's owner check
      -- refuses what no select finds: the FK mapping below it never sees one.
      testCase "a stream write to a missing workflow is a conflict" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          refused <- writeStream env (WorkflowId unique) 1 "progress" "one" Nothing Workflow (fixtureOwner unique)
          case refused of
            Left (WorkflowConflict {}) -> pure ()
            other -> fail ("expected WorkflowConflict, got: " <> show other)
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
          recorded <- recordStep env (WorkflowId unique) 1 "DBOS.step" (OutcomeOutput (Just "out")) (Just "json") Nothing (fixtureOwner unique)
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
          _ <- recordStep env (WorkflowId unique) 1 "DBOS.old" (OutcomeOutput (Just "out")) Nothing Nothing (fixtureOwner unique)
          found <- checkStep env (WorkflowId unique) 1 "DBOS.new"
          case found of
            Left (UnexpectedStep {}) -> pure ()
            other -> fail ("expected UnexpectedStep, got: " <> show other),
      testCase "a rival execution's step is not overwritten" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          _ <- recordStep env (WorkflowId unique) 1 "DBOS.step" (OutcomeOutput (Just "winner")) (Just "json") (Just (StepTiming (timestampFromEpochMs 100) (timestampFromEpochMs 200))) (fixtureOwner unique)
          rival <- recordStep env (WorkflowId unique) 1 "DBOS.step" (OutcomeOutput (Just "loser")) (Just "json") (Just (StepTiming (timestampFromEpochMs 100) (timestampFromEpochMs 300))) (fixtureOwner unique)
          case rival of
            Left (StepNondeterminism {}) -> pure ()
            other -> fail ("expected StepNondeterminism, got: " <> show other)
          found <- checkStep env (WorkflowId unique) 1 "DBOS.step"
          case found of
            Right (Just step) -> step.stepRecordOutput @?= Just "winner"
            other -> fail ("expected a step, got: " <> show other),
      testCase "a stale token's step write is refused and writes nothing" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          refused <- recordStep env (WorkflowId unique) 1 "DBOS.step" (OutcomeOutput (Just "spy")) Nothing Nothing "someone-else"
          case refused of
            Left (WorkflowConflict {}) -> pure ()
            other -> fail ("expected WorkflowConflict, got: " <> show other)
          steps <- listSteps env (WorkflowId unique) True Nothing Nothing Nothing
          case steps of
            Right [] -> pure ()
            other -> fail ("expected no steps, got: " <> show other),
      testCase "a stale token's outcome write adopts instead" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique)
          adopted <- recordWorkflowOutcome env (WorkflowId unique) (OutcomeOutput (Just "spy")) "someone-else"
          adopted @?= Right AlreadyFinished
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> do
              record.workflowRecordStatus @?= Pending
              record.workflowRecordOutput @?= Nothing
            other -> fail ("expected record, got: " <> show other),
      testCase "winning the checkpoint re-stamps the executor" $ do
        unique <- freshWorkflowId
        withBackendSettings getBackend (defaultSettings {settingsExecutorId = Just "advancing"}) $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique) {fixtureExecutor = Just "stale"}
          _ <- recordStep env (WorkflowId unique) 1 "DBOS.step" (OutcomeOutput Nothing) Nothing Nothing (fixtureOwner unique)
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
      testCase "a recovery sweep enqueues only the dead executor's own rows" $ do
        unique <- freshWorkflowId
        dead <- freshWorkflowId
        alive <- freshWorkflowId
        peer <- freshWorkflowId
        legacy <- freshWorkflowId
        let deadExecutor = "dead-" <> unique
            aliveExecutor = "alive-" <> unique
            mine = "mine-" <> Text.take 8 unique
            theirs = "theirs-" <> Text.take 8 unique
        withBackendAs getBackend (Just mine) $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture dead) {fixtureExecutor = Just deadExecutor, fixtureVersion = Just "v1", fixtureApplication = Just mine}
          _ <- insertFixtureChecked env (defaultFixture alive) {fixtureExecutor = Just aliveExecutor, fixtureVersion = Just "v1", fixtureApplication = Just mine}
          -- Another application's row at the same executor and version, as
          -- recovery collides across applications by default; an unclaimed row
          -- belongs to every application.
          _ <- insertFixtureChecked env (defaultFixture peer) {fixtureExecutor = Just deadExecutor, fixtureVersion = Just "v1", fixtureApplication = Just theirs}
          _ <- insertFixtureChecked env (defaultFixture legacy) {fixtureExecutor = Just deadExecutor, fixtureVersion = Just "v1"}
          swept <- reenqueueForRecovery env [deadExecutor] "v1" "_dbos_internal_queue"
          case swept of
            Right recovered -> do
              length recovered @?= 2
              assertBool "our own row is recovered" (WorkflowId dead `elem` recovered)
              assertBool "the unclaimed row is recovered" (WorkflowId legacy `elem` recovered)
              assertBool "the peer's row is not ours to recover" (WorkflowId peer `notElem` recovered)
            other -> fail ("expected the owned and unclaimed rows, got: " <> show other)
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
          leftAlone <- getWorkflow env (WorkflowId peer)
          case leftAlone of
            Right (Just record) -> do
              record.workflowRecordStatus @?= Pending
              record.workflowRecordAppName @?= Just theirs
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
          started <- initWorkflow env (newWorkflow unique) {newWorkflowExecutorId = Just "exec-1"} Nothing
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
      testCase "a fresh start on another attempt's row joins without touching it" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique) {fixtureStatus = "PENDING", fixtureOwnerXid = Nothing, fixtureCreatorXid = Just "creator-a", fixtureInput = Just "{\"positionalArgs\":[21],\"namedArgs\":{}}", fixtureSerialization = Just "portable_json"}
          started <- initWorkflow env (newWorkflow unique) {newWorkflowName = Just "fixture", newWorkflowInput = Just "{\"positionalArgs\":[99],\"namedArgs\":{}}", newWorkflowSerialization = Just "portable_json"} Nothing
          case started of
            Left err -> fail ("expected a join, got: " <> show err)
            Right startedResult -> do
              -- Another caller created it, so this one joins whatever has
              -- become of the row — and must not touch its recorded input.
              startedResult.initResultShouldExecute @?= False
              startedResult.initResultSerialization @?= Just "portable_json"
          kept <- getWorkflow env (WorkflowId unique)
          case kept of
            Right (Just record) -> record.workflowRecordInput @?= Just "{\"positionalArgs\":[21],\"namedArgs\":{}}"
            other -> fail ("expected record, got: " <> show other),
      testCase "a fresh insert runs with the offered input" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          started <- initWorkflow env (newWorkflow unique) {newWorkflowInput = Just "{\"positionalArgs\":[1],\"namedArgs\":{}}", newWorkflowSerialization = Just "portable_json"} Nothing
          case started of
            Left err -> fail ("expected a start, got: " <> show err)
            Right startedResult -> do
              startedResult.initResultShouldExecute @?= True
              startedResult.initResultSerialization @?= Just "portable_json"
          kept <- getWorkflow env (WorkflowId unique)
          case kept of
            Right (Just record) -> record.workflowRecordInput @?= Just "{\"positionalArgs\":[1],\"namedArgs\":{}}"
            other -> fail ("expected record, got: " <> show other),
      testCase "a queued start is enqueued and not counted" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          started <- initWorkflow env (newWorkflow unique) {newWorkflowQueueName = Just "q"} Nothing
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
          started <- initWorkflow env (newWorkflow unique) Nothing
          case started of
            Left err -> fail ("expected a start, got: " <> show err)
            Right startedResult -> do
              startedResult.initResultShouldExecute @?= False
              startedResult.initResultRecoveryAttempts @?= 1
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> record.workflowRecordExecutorId @?= Just "other-exec"
            other -> fail ("expected record, got: " <> show other),
      testCase "a recovery start on another attempt's row joins without touching it" $ do
        unique <- freshWorkflowId
        withBackendSettings getBackend (defaultSettings {settingsExecutorId = Just "mine"}) $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique) {fixtureName = Nothing, fixtureExecutor = Just "other-exec", fixtureOwnerXid = Just "other-owner", fixtureCreatorXid = Just "other-owner", fixtureRecoveryAttempts = Just 1}
          started <- initWorkflow env (newWorkflow unique) {newWorkflowExecutorId = Just "mine"} Nothing
          case started of
            Left err -> fail ("expected a start, got: " <> show err)
            Right startedResult -> do
              -- Recovery runs through re-enqueue and claim, never through
              -- init: a start that did not create the row joins it and
              -- leaves its count and executor alone.
              startedResult.initResultShouldExecute @?= False
              startedResult.initResultRecoveryAttempts @?= 1
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> record.workflowRecordExecutorId @?= Just "other-exec"
            other -> fail ("expected record, got: " <> show other),
      testCase "a different function under the same id is a conflict" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- runSession env "fixture" (insertFixture (defaultFixture unique) {fixtureName = Just "alpha"})
          started <- initWorkflow env (newWorkflow unique) {newWorkflowName = Just "beta"} Nothing
          case started of
            Left (ConflictingWorkflow {}) -> pure ()
            _ -> fail "expected ConflictingWorkflow",
      testCase "a start runs only the row it created" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          -- A row another attempt created, owned by its creator: this
          -- start's fresh token never matches, so it joins.
          _ <- insertFixtureChecked env ((defaultFixture unique) {fixtureName = Nothing, fixtureCreatorXid = Just "creator-a", fixtureOwnerXid = Just "creator-a"})
          started <- initWorkflow env (newWorkflow unique) Nothing
          case started of
            Right result -> do
              result.initResultShouldExecute @?= False
              assertBool "the call mints its own creator token" (result.initResultCreatorXid /= "creator-a")
            other -> fail ("expected the row back unrun, got: " <> show other)
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> do
              record.workflowRecordStatus @?= Pending
              record.workflowRecordOwnerXid @?= Just "creator-a"
              record.workflowRecordRecoveryAttempts @?= 0
            other -> fail ("expected record, got: " <> show other),
      testCase "a start that created the row runs it" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          first <- initWorkflow env (newWorkflow unique) Nothing
          creator <- case first of
            Right result -> do
              result.initResultShouldExecute @?= True
              pure result.initResultCreatorXid
            other -> fail ("expected a fresh row to run, got: " <> show other)
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> do
              record.workflowRecordStatus @?= Pending
              -- A PENDING insert is owned by its creator from the start.
              record.workflowRecordOwnerXid @?= Just creator
            other -> fail ("expected record, got: " <> show other),
      testCase "an over-limit row starts unparked: only the dequeue dispatch parks" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture unique) {fixtureName = Nothing, fixtureOwnerXid = Just "other-owner", fixtureRecoveryAttempts = Just 150, fixtureQueue = Just ("q-" <> unique), fixtureDeduplicationId = Just ("key-" <> unique)}
          -- Recovery past the budget used to park here; since the claim
          -- counts attempts, only its dispatch can judge a spent budget.
          started <- initWorkflow env (newWorkflow unique) Nothing
          case started of
            Right result -> result.initResultStatus @?= Pending
            other -> fail ("expected the row back unparked, got: " <> show other)
          found <- getWorkflow env (WorkflowId unique)
          case found of
            Right (Just record) -> record.workflowRecordStatus @?= Pending
            other -> fail ("expected record, got: " <> show other),
      testCase "a deduplication collision is reported" $ do
        unique <- freshWorkflowId
        first <- freshWorkflowId
        second <- freshWorkflowId
        withBackend getBackend $ \env -> do
          _ <- insertFixtureChecked env (defaultFixture first) {fixtureQueue = Just ("q-" <> unique), fixtureDeduplicationId = Just ("key-" <> unique)}
          started <- initWorkflow env (newWorkflow second) {newWorkflowQueueName = Just ("q-" <> unique), newWorkflowDeduplicationId = Just ("key-" <> unique)} Nothing
          case started of
            Left (QueueDeduplicated {}) -> pure ()
            _ -> fail "expected QueueDeduplicated",
      testCase "writes the input to the payload table, not the status row" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          started <- initWorkflow env (newWorkflow unique) {newWorkflowInput = Just "\"in\""} Nothing
          case started of
            Left err -> fail ("expected a start, got: " <> show err)
            Right _ -> pure ()
          (input, _, _) <- payloadRows env unique
          input @?= Just "\"in\""
          legacyPayloads env unique >>= (@?= (Nothing, Nothing, Nothing)),
      testCase "a resubmission does not replace the input" $ do
        unique <- freshWorkflowId
        withBackend getBackend $ \env -> do
          first <- initWorkflow env (newWorkflow unique) {newWorkflowInput = Just "\"first\""} Nothing
          case first of
            Left err -> fail ("expected a start, got: " <> show err)
            Right _ -> pure ()
          again <- initWorkflow env (newWorkflow unique) {newWorkflowInput = Just "\"second\""} Nothing
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
          started <- initWorkflow env (newWorkflow unique) {newWorkflowInput = Just "\"fresh\""} Nothing
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
              record.scheduleRecordAppName @?= Nothing
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
          names <- scheduleNames env (defaultScheduleFilter {scheduleFilterNamePrefixes = [name], scheduleFilterAppScope = AnyApplication})
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
          claimed.scheduleRecordAppName @?= Just beta
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
            scoped = defaultScheduleFilter {scheduleFilterNamePrefixes = [base], scheduleFilterAppScope = AnyApplication}
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
          own <- scheduleNames env scoped {scheduleFilterAppScope = Unset}
          own @?= [alphaJob, nobodyJob]
          anyApp <- scheduleNames env scoped {scheduleFilterAppScope = AnyApplication}
          anyApp @?= [alphaJob, betaJob, nobodyJob]
          named <- scheduleNames env scoped {scheduleFilterAppScope = Named [beta]}
          named @?= [betaJob, nobodyJob]
          -- A schedule is addressed by name, so a read crosses applications.
          crossed <- getSchedule env betaJob Nothing
          case crossed of
            Right (Just record) -> record.scheduleRecordAppName @?= Just beta
            other -> fail ("expected beta's schedule, got: " <> show other)
        withBackend getBackend $ \env -> do
          unnamedOwn <- scheduleNames env scoped {scheduleFilterAppScope = Unset}
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

