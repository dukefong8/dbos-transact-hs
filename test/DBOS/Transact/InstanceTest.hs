{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Instance lifecycle behavior through the public DBOS facade.
module DBOS.Transact.InstanceTest (tests) where

import DBOS.Internal.Prelude
import Data.Int (Int64)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import DBOS.SystemDB (AwaitedOutcome (..), VersionInfo (..), WorkflowRecord (..), getWorkflow, listApplicationVersions)
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Internal.Transact.Serialization (CodecError (..), WorkflowInput, decodeWorkflowValue, encodeWorkflowInput)
import System.Mem (performGC)
import System.Mem.Weak (Weak, deRefWeak, mkWeakPtr)
import DBOS.Transact
  (
    EngineOnly, Config (..),
    DBOS,
    Executor,
    Environment (..),
    Error (..),
    IdleTransactionTimeout (..),
    PollingConcurrency (PollingDefault),
    Serializer (..),
    SerializedWorkflowValue (..),
    WorkflowCtx,
    WorkflowHandle,
    WorkflowId (..),
    QualifiedWorkflowName,
    cancelWorkflows,
    configFromEnv,
    enqueueWorkflow,
    isLaunched,
    launchWithEnvironment,
    newDBOS,
    newQualifiedWorkflowName,
    registerWorkflow,
    registerWorkflowRef,
    runWorkflow,
    shutdown,
    startChildWorkflow,
    startWorkflow,
    handleResult,
    waitForWorkflow,
  )
import DBOS.Internal.Transact.Logger (nullTracer)
import DBOS.Internal.Transact.Instance (dbosAppVersion, dbosExecutorId)
import DBOS.Internal.Transact.Workflow (WorkflowRef, refName, startOptionsDefault)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

-- | Launch over the isolated environment and hand back the executor.
launchInstanceExec :: DBOS IO -> Environment -> IO (Executor IO)
launchInstanceExec dbos env = do
  started <- launchWithEnvironment dbos env
  case started of
    Left err -> fail (displayException err)
    Right executor -> pure executor

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
  testGroup
    "DBOS instance"
    [ testCase "launching twice is a no-op, not an error" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-" <> Text.take 16 suffix
            appVersion = "hs-l2-version-" <> suffix
            executorId = "hs-l2-executor-" <> suffix
        base <- configFromEnv appName
        let configured = base {configAppVersion = Just appVersion, configExecutorId = Just executorId}
        dbos <- newDBOS configured
        first <- launchWithEnvironment dbos isolatedEnvironment
        case first of
          Left err -> fail (displayException err)
          Right _ -> pure ()
        second <- launchWithEnvironment dbos isolatedEnvironment
        case second of
          Left err -> fail ("a second launch should be a no-op, got: " <> displayException err)
          Right _ -> pure ()
        assertEqual "the executor survives the second launch" True =<< isLaunched dbos
        shutdown dbos
        assertEqual "shutdown still lands" False =<< isLaunched dbos,
      testCase "an instance outlives the executor it launched" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-" <> Text.take 16 suffix
            appVersion = "hs-l2-version-" <> suffix
            executorId = "hs-l2-executor-" <> suffix
        base <- configFromEnv appName
        let configured = base {configAppVersion = Just appVersion, configExecutorId = Just executorId}
        dbos <- newDBOS configured
        first <- launchWithEnvironment dbos isolatedEnvironment
        case first of
          Left err -> fail (displayException err)
          Right _ -> pure ()
        firstId <- dbosExecutorId dbos
        firstId @?= Right executorId
        shutdown dbos
        assertEqual "shutdown drops the executor" False =<< isLaunched dbos
        missing <- dbosAppVersion dbos
        missing @?= Left (NotLaunched "app_version")
        relaunched <- launchWithEnvironment dbos isolatedEnvironment
        case relaunched of
          Left err -> fail (displayException err)
          Right _ -> pure ()
        secondId <- dbosExecutorId dbos
        secondId @?= firstId
        shutdown dbos
        assertEqual "shutdown still lands" False =<< isLaunched dbos,
      testCase "an invalid config is refused before connecting" $ do
        let invalid =
              Config
                { configAppName = "hs-invalid",
                  configDatabaseUrl = "",
                  configMaxConnections = 10,
                  configSchema = "dbos",
                  configExecutorId = Nothing,
                  configAppVersion = Just "test-version",
                  configSerializer = PortableJson,
                  configUseListenNotify = True,
                  configMigrate = True,
                  configPollingConcurrency = PollingDefault,
                  configOutcomePollInterval = Nothing,
                  configListenQueues = Nothing,
                  configNotificationCoalesce = Nothing,
                  configIdleTransactionTimeout = Default
                }
        dbos <- newDBOS invalid
        started <- launchWithEnvironment dbos isolatedEnvironment
        case started of
          Left err -> assertBool "names the missing database URL" ("database URL" `Text.isInfixOf` Text.pack (displayException err))
          Right _ -> fail "expected an empty database URL to be refused"
        assertEqual "failed launch does not install an executor" False =<< isLaunched dbos,
      testCase "a failed launch leaves registration open" $ do
        base <- configFromEnv "ab"
        let configured = base {configAppVersion = Just "hs-l2-open-v1", configExecutorId = Just "hs-l2-open-exec"}
            echoWorkflow :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
            echoWorkflow message _ = pure (Right message)
        dbos <- newDBOS configured
        started <- launchWithEnvironment dbos isolatedEnvironment
        case started of
          Left _ -> pure ()
          Right _ -> fail "expected a short application name to be refused"
        reopened <- registerWorkflow dbos (newQualifiedWorkflowName "late") echoWorkflow
        case reopened of
          Left err -> fail (displayException err)
          Right () -> pure (),
      testCase "a failed launch can be followed by a good one" $ do
        bad <- configFromEnv "ab"
        good0 <- configFromEnv "hs-l2-retry"
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            badConfigured = bad {configAppVersion = Just "hs-l2-retry-bad-v1", configExecutorId = Just "hs-l2-retry-bad-exec"}
            goodConfigured = good0 {configAppVersion = Just ("hs-l2-retry-v-" <> suffix), configExecutorId = Just ("hs-l2-retry-exec-" <> suffix)}
        badDbos <- newDBOS badConfigured
        failed <- launchWithEnvironment badDbos isolatedEnvironment
        case failed of
          Left _ -> pure ()
          Right _ -> fail "expected a short application name to be refused"
        shutdown badDbos
        bracket (newDBOS goodConfigured) shutdown $ \goodDbos -> do
          retried <- launchWithEnvironment goodDbos isolatedEnvironment
          case retried of
            Left err -> fail (displayException err)
            Right _ -> pure ()
          assertEqual "the good launch installs its executor" True =<< isLaunched goodDbos,
      testCase "an invalid application name is refused at launch" $ do
        base <- configFromEnv "ab"
        let configured = base {configAppVersion = Just "hs-l2-badname-v1", configExecutorId = Just "hs-l2-badname-exec"}
        dbos <- newDBOS configured
        started <- launchWithEnvironment dbos isolatedEnvironment
        case started of
          Left err -> assertBool "names the short name rule" ("at least 3 characters" `Text.isInfixOf` Text.pack (displayException err))
          Right _ -> fail "expected a short application name to be refused"
        assertEqual "failed launch does not install an executor" False =<< isLaunched dbos,
      testCase "relaunching registers the same version once" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-re-" <> Text.take 16 suffix
            appVersion = "hs-l2-re-version-" <> suffix
            executorId = "hs-l2-re-executor-" <> suffix
        base <- configFromEnv appName
        let configured = base {configAppVersion = Just appVersion, configExecutorId = Just executorId}
        dbos <- newDBOS configured
        first <- launchWithEnvironment dbos isolatedEnvironment
        case first of
          Left err -> fail (displayException err)
          Right _ -> pure ()
        shutdown dbos
        second <- launchWithEnvironment dbos isolatedEnvironment
        case second of
          Left err -> fail (displayException err)
          Right _ -> pure ()
        shutdown dbos
        ours <- readAppVersions getBackend appName appVersion
        ours @?= [appVersion],
      testCase "explicit version and executor ids are used as given" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-ids-" <> Text.take 16 suffix
            appVersion = "hs-l2-ids-version-" <> suffix
            executorId = "hs-l2-ids-executor-" <> suffix
            workflowId = WorkflowId ("hs-l2-ids-wf-" <> suffix)
        base <- configFromEnv appName
        let configured = base {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            echoWorkflow :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
            echoWorkflow message _ = pure (Right message)
        dbos <- newDBOS configured
        registered <- registerWorkflow dbos (newQualifiedWorkflowName "greeting") echoWorkflow
        case registered of
          Left err -> fail (displayException err)
          Right () -> pure ()
        exec <- launchInstanceExec dbos isolatedEnvironment
        ran <- runWf exec (newQualifiedWorkflowName "greeting") workflowId (Just (encodeWorkflowInput ("hi" :: Text)))
        case ran of
          Left err -> fail (displayException err)
          Right _ -> pure ()
        shutdown dbos
        assertWorkflowExecutor getBackend workflowId executorId
        ours <- readAppVersions getBackend appName appVersion
        ours @?= [appVersion],
      testCase "two applications sharing a database own their own versions" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appA = "hs-l2-ten-a-" <> Text.take 12 suffix
            appB = "hs-l2-ten-b-" <> Text.take 12 suffix
            versionA = "hs-l2-ten-version-a-" <> suffix
            versionB = "hs-l2-ten-version-b-" <> suffix
        baseA <- configFromEnv appA
        baseB <- configFromEnv appB
        dbosA <- newDBOS (baseA {configAppVersion = Just versionA})
        launchedA <- launchWithEnvironment dbosA isolatedEnvironment
        case launchedA of
          Left err -> fail (displayException err)
          Right _ -> pure ()
        shutdown dbosA
        dbosB <- newDBOS (baseB {configAppVersion = Just versionB})
        launchedB <- launchWithEnvironment dbosB isolatedEnvironment
        case launchedB of
          Left err -> fail (displayException err)
          Right _ -> pure ()
        shutdown dbosB
        oursA <- readAppVersions getBackend appA versionA
        oursA @?= [versionA]
        oursB <- readAppVersions getBackend appB versionB
        oursB @?= [versionB],
      testCase "launch installs an executor until idempotent shutdown" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-" <> Text.take 16 suffix
            appVersion = "hs-l2-version-" <> suffix
            executorId = "hs-l2-executor-" <> suffix
        base <- configFromEnv appName
        let configured = base {configAppVersion = Just appVersion, configExecutorId = Just executorId}
        dbos <- newDBOS configured
        let echoWorkflow :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
            echoWorkflow message _ = pure (Right message)
        beforeLaunch <- registerWorkflow dbos (newQualifiedWorkflowName "greeting") echoWorkflow
        case beforeLaunch of
          Left err -> fail (displayException err)
          Right () -> pure ()
        assertEqual "new instance is unlaunched" False =<< isLaunched dbos
        started <- launchWithEnvironment dbos isolatedEnvironment
        case started of
          Left err -> fail (displayException err)
          Right _ -> pure ()
        assertEqual "launch installs the executor" True =<< isLaunched dbos
        missing <- waitForWorkflow dbos (WorkflowId ("hs-l2-missing-" <> suffix))
        case missing of
          Left err -> assertBool "reports the missing workflow" ("no such workflow" `Text.isInfixOf` Text.pack (displayException err))
          Right _ -> fail "expected waiting for a missing workflow to fail"
        afterLaunch <- registerWorkflow dbos (newQualifiedWorkflowName "late") echoWorkflow
        case afterLaunch of
          Left err -> assertBool "names the lifecycle boundary" ("after DBOS is launched" `Text.isInfixOf` Text.pack (displayException err))
          Right () -> fail "expected registration after launch to be refused"
        shutdown dbos
        shutdown dbos
        assertEqual "shutdown is idempotent" False =<< isLaunched dbos,
      testCase "an enqueued workflow can be cancelled through the instance" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-cancel-" <> Text.take 12 suffix
            appVersion = "hs-l2-version-" <> suffix
            executorId = "hs-l2-executor-" <> suffix
            workflowId = WorkflowId ("hs-l2-cancel-wf-" <> suffix)
        base <- configFromEnv appName
        let configured = base {configAppVersion = Just appVersion, configExecutorId = Just executorId, configListenQueues = Just []}
            echoWorkflow :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
            echoWorkflow message _ = pure (Right message)
        bracket (newDBOS configured) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos (newQualifiedWorkflowName "queued") echoWorkflow
          case registered of
            Left err -> fail (displayException err)
            Right () -> pure ()
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (displayException err)
            Right _ -> pure ()
          enqueued <-
            enqueueWorkflow
              dbos
              (newQualifiedWorkflowName "queued")
              workflowId
              (Just (encodeWorkflowInput ("hello" :: Text)))
              ("cancel-" <> suffix)
          case enqueued of
            Left err -> fail (displayException err)
            Right _ -> pure ()
          cancelled <- cancelWorkflows dbos [workflowId] False
          assertEqual "the selected workflow is cancelled" (Right [workflowId]) cancelled
          outcome <- waitForWorkflow dbos workflowId
          assertEqual "the waiter observes cancellation" (Right AwaitedCancelled) outcome,
      testCase "a launch that fails after connecting closes the database" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            tag = "dbos-launch-leak-probe-" <> Text.take 12 suffix
            contested = "contested-v1-" <> suffix
        holderBase <- configFromEnv ("hs-l2-version-holder-" <> Text.take 12 suffix)
        let holderConfig =
              holderBase
                { configAppVersion = Just contested,
                  configExecutorId = Just ("hs-l2-executor-" <> suffix)
                }
        bracket (newDBOS holderConfig) shutdown $ \holder -> do
          holderStarted <- launchWithEnvironment holder isolatedEnvironment
          case holderStarted of
            Left err -> fail (displayException err)
            Right _ -> pure ()
          -- A second application claiming the same version name: version
          -- registration is refused, which happens after the connect and
          -- before the executor exists.
          forM_ [1 .. 3 :: Int] $ \attempt -> do
            loserBase <- configFromEnv ("hs-l2-version-loser-" <> Text.take 12 suffix)
            let loserConfig =
                  loserBase
                    { configAppVersion = Just contested,
                      configExecutorId = Just ("hs-l2-executor-" <> suffix),
                      configDatabaseUrl = withAppName loserBase.configDatabaseUrl tag
                    }
            bracket (newDBOS loserConfig) shutdown $ \loser -> do
              loserStarted <- launchWithEnvironment loser isolatedEnvironment
              case loserStarted of
                Left (SystemDatabase _) -> pure ()
                Left other -> fail ("attempt " <> show attempt <> ": expected a system-database refusal, got: " <> show other)
                Right _ -> fail ("attempt " <> show attempt <> ": a conflicting launch succeeded")
          -- The server drops a session shortly after its client goes away,
          -- so this is a bounded wait rather than a single look. A leak
          -- never converges; a close does, immediately.
          backend <- getBackend
          drained <- pollUntil 30000000 $ do
            open <- countConnections backend tag
            pure (open == 0)
          assertBool "connections from three failed launches are still open" drained,
      testCase "launching resolves an identity" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-lifecycle-app-" <> Text.take 12 suffix
            appVersion = "hs-l2-lifecycle-v-" <> suffix
        base <- configFromEnv appName
        let configured = base {configAppVersion = Just appVersion}
        bracket (newDBOS configured) shutdown $ \dbos -> do
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (displayException err)
            Right _ -> pure ()
          assertEqual "launched" True =<< isLaunched dbos
          executorId <- dbosExecutorId dbos
          executorId @?= Right "local"
          version <- dbosAppVersion dbos
          version @?= Right appVersion,
      -- Rust #155: a reference never held the instance — it names the
      -- workflow and its registry, while the caller passes the executor
      -- explicitly — so a launched executor serves its refs with no
      -- handle to the instance left. Runs, starts, and a child started
      -- from a workflow body, all through the executor alone.
      testCase "a launched executor serves its refs with no handle to its instance" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-detached-app-" <> Text.take 12 suffix
            appVersion = "hs-l2-detached-v-" <> suffix
        base <- configFromEnv appName
        let configured = base {configAppVersion = Just appVersion, configExecutorId = Just ("hs-l2-detached-exec-" <> suffix)}
        dbos <- newDBOS configured
        let childBody :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            childBody n _ = pure (Right (n * 2))
        childRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "child") childBody >>= either (fail . show) pure
        let parentBody :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            parentBody n wctx = do
              started <- startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowInput n))
              case started of
                Left err -> pure (Left err)
                Right child -> do
                  awaited <- handleResult child
                  case awaited of
                    Left err -> pure (Left err)
                    Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                      Right doubled -> pure (Right doubled)
                      Left err -> throwIO (userError (show err))
                    Right Nothing -> throwIO (userError "the child returned no output")
        parentRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "parent") parentBody >>= either (fail . show) pure
        exec <- launchWithEnvironment dbos isolatedEnvironment >>= either (fail . show) pure
        -- From here the instance handle is never touched again until the
        -- final shutdown below: a direct run, a start, and a body that
        -- starts its own child.
        direct <- runWf exec (newQualifiedWorkflowName "child") (WorkflowId ("hs-l2-detached-direct-" <> suffix)) (Just (encodeWorkflowInput (21 :: Int)))
        case direct of
          Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
            Right n -> n @?= 42
            Left err -> fail (show err)
          other -> fail ("the direct run failed: " <> show other)
        started <- startWorkflow exec childRef startOptionsDefault (Just (encodeWorkflowInput (7 :: Int)))
        case (started :: Either (Error EngineOnly) (WorkflowHandle IO EngineOnly)) of
          Left err -> fail (show err)
          Right handle -> do
            awaited <- handleResult handle
            case awaited of
              Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                Right n -> n @?= 14
                Left err -> fail (show err)
              other -> fail ("the start failed: " <> show other)
        parented <- runWf exec (newQualifiedWorkflowName "parent") (WorkflowId ("hs-l2-detached-parent-" <> suffix)) (Just (encodeWorkflowInput (3 :: Int)))
        case parented of
          Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
            Right n -> n @?= 6
            Left err -> fail (show err)
          other -> fail ("the parent run failed: " <> show other)
        shutdown dbos
        assertEqual "shutdown still lands" False =<< isLaunched dbos,
      -- Rust #155's leak, which cannot happen here: the registry cycle
      -- (registry, closures, refs) never touches the instance, and GHC
      -- collects cycles, so a launched-then-shutdown instance is freed
      -- while its refs survive. Fails if anyone ever points a ref, a
      -- closure, or the executor back at the instance.
      testCase "a dropped instance is collected while its refs survive" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-leak-app-" <> Text.take 12 suffix
            appVersion = "hs-l2-leak-v-" <> suffix
        base <- configFromEnv appName
        let configured = base {configAppVersion = Just appVersion, configExecutorId = Just ("hs-l2-leak-exec-" <> suffix)}
            childBody :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            childBody n _ = pure (Right (n * 2))
        -- Built and dropped inside the helper, so no stack slot in this
        -- frame can retain the instance across the collection: only the
        -- weak pointer and the refs come back.
        (weak, parentRef, childRef) <- launchShutdownCollect configured childBody suffix
        -- The weak pointer must find nothing after a collection.
        performGC
        collected <- deRefWeak weak
        case collected of
          Nothing -> pure ()
          Just _ -> fail "the dropped instance survived collection"
        -- And the surviving refs still name their workflows.
        refName parentRef @?= "parent"
        refName childRef @?= "child"
    ]

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through. Local
-- copies are deliberate: this module carries only the aliases it uses.
runWf :: Executor IO -> QualifiedWorkflowName -> WorkflowId -> Maybe WorkflowInput -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWf = runWorkflow

-- | Launches an instance, runs one workflow through it, shuts it down,
-- and hands back only the weak pointer and the refs: by the time the
-- caller collects, no frame on its stack can still reach the instance or
-- its executor.
launchShutdownCollect :: Config -> (forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)) -> Text -> IO (Weak (DBOS IO), WorkflowRef IO EngineOnly, WorkflowRef IO EngineOnly)
launchShutdownCollect configured childBody suffix = do
  dbos <- newDBOS configured
  childRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "child") childBody >>= either (fail . show) pure
  -- The parent mentions the child ref, closing the registry cycle the
  -- oracle leaked through: registry, closure, ref, registry. The mention
  -- shapes the value, so the closure genuinely retains it rather than
  -- dropping it as dead code.
  let parentBody :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
      parentBody n _ = pure (Right (n + 1 + Text.length (refName childRef)))
  parentRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "parent") parentBody >>= either (fail . show) pure
  weak <- mkWeakPtr dbos Nothing
  exec <- launchWithEnvironment dbos isolatedEnvironment >>= either (fail . show) pure
  direct <- runWf exec (newQualifiedWorkflowName "child") (WorkflowId ("hs-l2-leak-direct-" <> suffix)) (Just (encodeWorkflowInput (1 :: Int)))
  case direct of
    Right (Just _) -> pure ()
    other -> fail ("the run failed: " <> show other)
  shutdown dbos
  pure (weak, parentRef, childRef)

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted.
-- The launched instances below keep their own pools: each needs a distinct
-- application identity.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | Tags a connection string so its backends can be told from every other
-- test's on the shared server.
withAppName :: Text -> Text -> Text
withAppName url tag
  | "?" `Text.isInfixOf` url = url <> "&application_name=" <> tag
  | otherwise = url <> "?application_name=" <> tag

-- | How many server backends currently carry the tag.
countConnections :: Postgres.PostgresSystemDB -> Text -> IO Int64
countConnections backend tag = do
  result <- Postgres.runSession backend "leak-probe" (Session.statement tag countStatement)
  case result of
    Left err -> fail (show err)
    Right open -> pure open
  where
    countStatement =
      Statement.preparable
        "select count(*) from pg_stat_activity where application_name = $1"
        (Encoders.param (Encoders.nonNullable Encoders.text))
        (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))

-- | Polls a condition until it holds or the budget runs out.
pollUntil :: Int -> IO Bool -> IO Bool
pollUntil remaining check
  | remaining <= 0 = check
  | otherwise = do
      ok <- check
      if ok
        then pure True
        else threadDelay 100000 >> pollUntil (remaining - 100000) check

-- | The executor id stamped on a workflow row must be the given one: a
-- reader over the suite backend.
assertWorkflowExecutor :: IO Postgres.PostgresSystemDB -> WorkflowId -> Text -> IO ()
assertWorkflowExecutor getBackend workflowId executorId = do
  backend <- getBackend
  found <- getWorkflow backend workflowId
  case found of
    Right (Just WorkflowRecord {workflowRecordExecutorId = stamped}) -> stamped @?= Just executorId
    _ -> fail "expected the workflow row to exist"

-- | Version ids registered for one application: a reader over the suite
-- backend, narrowed to a single version.
readAppVersions :: IO Postgres.PostgresSystemDB -> Text -> Text -> IO [Text]
readAppVersions getBackend appName appVersion = do
  backend <- getBackend
  listed <- listApplicationVersions backend
  case listed of
    Left err -> fail (show err)
    Right versions ->
      pure
        [ version.versionInfoName
          | version <- versions,
            version.versionInfoAppName == Just appName,
            version.versionInfoName == appVersion
        ]

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }
