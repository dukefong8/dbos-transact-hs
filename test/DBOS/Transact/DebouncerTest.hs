{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | The debouncer over live Postgres: sysdb-bounce coalescing with real
-- rows, real launches, and row-field observations. Every case owns its
-- rows via a unique tag and its queues via a unique suffix.
module DBOS.Transact.DebouncerTest (tests) where

import DBOS.DualStack (liveCaseWith)
import DBOS.Internal.Prelude
import Data.Int (Int64)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import Hasql.Connection qualified as Connection
import Hasql.Connection.Settings qualified as ConnectionSettings
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import Pqi.Ffi qualified as Pqi
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Internal.Transact.Serialization (encodeWorkflowInput)
import DBOS.Transact
import DBOS.Internal.Transact.Management (stepInfoFromRecord)
import DBOS.Internal.Transact.Connection (SomeSystemDB (..), runSystemDB)
import DBOS.Transact.DebouncerCases
import DBOS.Internal.Transact.Logger (SomeTracer (..), acquireLoggerBackend, ioTracer, nullTracer)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (testCase, (@?=))

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    withResource acquireLoggerBackend snd $ \getLogger ->
      testGroup
        "Debouncer"
        [ liveCaseWith (withDebouncerFixture getBackend (ioTracer . fst <$> getLogger)) "a first debounce creates a delayed debounced row" scenarioFirstDebounceDelays checkFirstDebounceDelays,
          liveCaseWith (withDebouncerFixture getBackend (ioTracer . fst <$> getLogger)) "a second debounce coalesces onto the same row" scenarioSecondDebounceCoalesces checkSecondDebounceCoalesces,
          liveCaseWith (withDebouncerFixture getBackend (ioTracer . fst <$> getLogger)) "a non-debounced holder refuses the debounce" scenarioForeignHolderRefused checkForeignHolderRefused,
          liveCaseWith (withDebouncerFixture getBackend (ioTracer . fst <$> getLogger)) "debouncing inside a workflow records a step" scenarioInWorkflowDebounceRecordsStep checkInWorkflowDebounceRecordsStep,
          liveCaseWith (withDebouncerFixture getBackend (ioTracer . fst <$> getLogger)) "a timeout caps the extension" scenarioTimeoutCapsExtension checkTimeoutCapsExtension,
          liveCaseWith (withDebouncerFixture getBackend (ioTracer . fst <$> getLogger)) "a debounced creation lands its release on the deadline" scenarioDebouncedCapAtCreation checkDebouncedCapAtCreation,
          liveCaseWith (withDebouncerFixture getBackend (ioTracer . fst <$> getLogger)) "a pinned id names the fresh row" scenarioPinnedIdNamesFreshRow checkPinnedIdNamesFreshRow,
          liveCaseWith (withDebouncerFixture getBackend (ioTracer . fst <$> getLogger)) "a pinned id reused across cycles is a conflict" scenarioPinnedIdReusedAcrossCycles checkPinnedIdReusedAcrossCycles,
          liveCaseWith (withDebouncerFixture getBackend (ioTracer . fst <$> getLogger)) "a zero or unstorable debounce duration is refused" scenarioDebounceDurationsRefused checkDebounceDurationsRefused,
          liveCaseWith (withDebouncerFixture getBackend (ioTracer . fst <$> getLogger)) "what a debouncer cannot act on is refused" scenarioDebouncerCannotAct checkDebouncerCannotAct,
          liveCaseWith (withDebouncerFixture getBackend (ioTracer . fst <$> getLogger)) "a cancelled debounce frees its key" scenarioCancelledDebounceFreesKey checkCancelledDebounceFreesKey,
          liveCaseWith (withDebouncerFixture getBackend (ioTracer . fst <$> getLogger)) "a debounce inside a step is refused" scenarioDebounceInsideStepRefused checkDebounceInsideStepRefused,
          liveCaseWith (withDebouncerFixture getBackend (ioTracer . fst <$> getLogger)) "a zero period inside a workflow is refused on the debounce channel" scenarioInWorkflowBadPeriodRefused checkInWorkflowBadPeriodRefused,
          liveCaseWith (withDebouncerFixture getBackend (ioTracer . fst <$> getLogger)) "a key collision between workflows is refused" scenarioKeyCollisionRefused checkKeyCollisionRefused,
          -- Live only: the case holds a table lock and polls pg_stat_activity,
          -- neither of which an IOSim mirror can do; it drives the same
          -- debounceInWorkflow and startChildWorkflow seams as its siblings.
          testCase "a debounce dropped while it enqueues still enqueues and records it" droppedDebounceStillEnqueues
        ]

acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

withDebouncerFixture :: IO Postgres.PostgresSystemDB -> IO (SomeTracer IO) -> (DebouncerFixture IO -> IO b) -> IO b
withDebouncerFixture getBackend getTracer body = do
  fresh <- UUID.V4.nextRandom
  let suffix = Text.take 12 (Text.filter (/= '-') (Text.pack (UUID.toString fresh)))
      version = "hs-db-version-" <> suffix
      appName = "hs-db-" <> suffix
      targetQueue = "db-target-q-" <> suffix
      foreignQueue = "db-foreign-q-" <> suffix
  config0 <- configFromEnv appName
  let config =
        config0
          { configAppVersion = Just version,
            configExecutorId = Just ("hs-db-executor-" <> suffix),
            -- Cases drive their own queue; listening to all (the
            -- default) would sweep other tests' queue fixtures.
            configListenQueues = Just [targetQueue]
          }
  dbos <- newDBOS config
  runsVar <- newTVarIO (0 :: Int)
  outputsVar <- newTVarIO ([] :: [Text])
  let echoBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
      echoBody input _ = do
        atomically (modifyTVar runsVar (+ 1))
        atomically (modifyTVar outputsVar (<> [input]))
        pure (Right input)
      otherBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
      otherBody _ _ = pure (Right "parked")
  targetRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "echo") echoBody >>= either (fail . show) pure
  otherRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "other") otherBody >>= either (fail . show) pure
  -- The in-workflow debounce's parent: registered before launch like every
  -- workflow, since recovery starts inside launch. Only the records-step
  -- case runs it, passing its tag as input.
  let parentBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
      parentBody tag wctx = do
        debounced <- debounceInWorkflow wctx targetRef (debouncerNew {debouncerQueueName = Just targetQueue}) tag (secondsDuration 3) (Just (encodeWorkflowInput ("one" :: Text)))
        case debounced of
          Right joined -> pure (Right joined.workflowId)
          Left err -> pure (Left err)
  _ <- registerWorkflow dbos (newQualifiedWorkflowName "debounce-parent") parentBody >>= either (fail . show) pure
  -- The in-step refusal's parent: its step calls the debounce, which the
  -- step boundary refuses. Only the refusal case runs it.
  let stepParentBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
      stepParentBody _ wctx = do
        outcome <- runStep wctx "attempt" $ \_ -> do
          debounced <- debounceInWorkflow wctx targetRef (debouncerNew {debouncerQueueName = Just targetQueue}) "in-step-key" (secondsDuration 3) Nothing
          case debounced of
            Left (InsideStep operation) -> pure operation
            Left err -> throwIO (userError (show err))
            Right joined -> pure ("unexpected-row:" <> joined.workflowId)
        case outcome of
          Right operation -> pure (Right operation)
          Left err -> pure (Left err)
  _ <- registerWorkflow dbos (newQualifiedWorkflowName "debounce-in-step-parent") stepParentBody >>= either (fail . show) pure
  -- The bad-period refusal's parent: debounces with a zero period, which
  -- durations refuse on the debounce's channel. Only the refusal case
  -- runs it.
  let badPeriodParentBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
      badPeriodParentBody _ wctx = do
        debounced <- debounceInWorkflow wctx targetRef (debouncerNew {debouncerQueueName = Just targetQueue}) "bad-period-key" (millisDuration 0) Nothing
        case debounced of
          Left (InvalidArgument operation detail) -> pure (Right (operation <> "|" <> detail))
          Left err -> throwIO (userError (show err))
          Right joined -> pure (Right ("unexpected-row:" <> joined.workflowId))
  _ <- registerWorkflow dbos (newQualifiedWorkflowName "debounce-bad-period-parent") badPeriodParentBody >>= either (fail . show) pure
  exec <- launch dbos >>= either (fail . show) pure
  _ <- registerQueue dbos targetQueue defaultQueueOptions NeverUpdate >>= either (fail . show) pure
  _ <- registerQueue dbos foreignQueue defaultQueueOptions NeverUpdate >>= either (fail . show) pure
  backend <- getBackend
  _ <- getTracer
  let fx =
        DebouncerFixture
          { dbDBOS = dbos,
            dbExecutor = exec,
            dbTargetRef = targetRef,
            dbOtherRef = otherRef,
            dbTargetQueue = targetQueue,
            dbForeignQueue = foreignQueue,
            dbFreshTag = \prefix -> pure (prefix <> "-" <> suffix),
            dbFreshWid = \prefix -> pure (WorkflowId ("hs-db-" <> prefix <> "-" <> suffix)),
            dbReadRow = \wid -> do
              found <- runSystemDB (SomeSystemDB backend) (\db -> SystemDB.getWorkflow db wid)
              case found of
                Left err -> throwIO (userError (show err))
                Right row -> pure row,
            dbListSteps = \wid -> do
              listed <- runSystemDB (SomeSystemDB backend) (\db -> SystemDB.listSteps db wid True Nothing Nothing Nothing)
              case listed of
                Left err -> throwIO (userError (show err))
                Right steps -> pure (map (stepInfoFromRecord defaultListWorkflowStepsOptions) steps),
            dbUserRuns = readTVarIO runsVar,
            dbUserOutputs = readTVarIO outputsVar,
            dbAwaitUser = \wid -> do
              _ <- driveQueue dbos
              settled <- waitForWorkflow dbos wid
              case settled of
                Left err -> throwIO (userError (show err))
                Right _ -> pure ()
              row <- runSystemDB (SomeSystemDB backend) (\db -> SystemDB.getWorkflow db wid)
              case row of
                Left err -> throwIO (userError (show err))
                Right found -> pure (fmap (.workflowRecordStatus) found)
          }
  bracket (pure ()) (\_ -> shutdown dbos) (\_ -> body fx)

-- * The dropped-debounce case (live only)

-- | A debounce dropped while its fresh enqueue writes still enqueues, and
-- records it at the parent's start step.
--
-- **The enqueue runs in a task of its own** (the writing half of
-- 'startChildWorkflow'), so a caller that stops waiting takes the wait and
-- leaves the work. Inline, the drop rolled the enqueue back, and the parent
-- finished without the workflow its debounce had begun to create.
--
-- Forced rather than raced: the case holds @workflow_input@ locked, which a
-- bounce that finds no holder never touches and the fresh enqueue's insert
-- waits on. The branch is dropped only once that insert is seen waiting, so
-- the enqueue has begun and cannot have finished.
--
-- **What this does not isolate is the parent waiting for the enqueue.** The
-- insert waits after the enqueue has locked the parent's row, so the
-- parent's outcome write would wait on that lock anyway. The starts token's
-- wait is covered at its own level: taking a token blocks 'waitForStarts'
-- until every taken token is released, which the 'starts token' ContextTest
-- case pins directly (a plain-child drop test cannot isolate it either —
-- every child start locks the parent row before its input insert, so the
-- same lock caveat applies).
droppedDebounceStillEnqueues :: IO ()
droppedDebounceStillEnqueues = do
  fresh <- UUID.V4.nextRandom
  let suffix = Text.take 12 (Text.filter (/= '-') (Text.pack (UUID.toString fresh)))
      appName = "hs-db-drop-" <> suffix
      queueName = "db-drop-q-" <> suffix
      parentText = "hs-db-drop-parent-" <> suffix
      parentId = WorkflowId parentText
      -- The debounce claims the bounce's step (0) and the start's (1)
      -- together, as the oracle's placement does, and the fresh row's id
      -- derives from the start's step.
      childId = WorkflowId (parentText <> "-1")
  config0 <- configFromEnv appName
  let config =
        config0
          { configAppVersion = Just ("hs-db-drop-version-" <> suffix),
            configExecutorId = Just ("hs-db-drop-executor-" <> suffix),
            configListenQueues = Just [queueName],
            -- Tag every connection this instance opens, so the lock poll
            -- sees this case's backends and no other test's.
            configDatabaseUrl = withAppName config0.configDatabaseUrl appName
          }
  lockTaken <- newEmptyMVar
  dropNow <- newEmptyMVar
  dbos <- newDBOS config
  let childBody :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
      childBody n _ = pure (Right n)
  childRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "child") childBody >>= either (fail . show) pure
  let parentBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
      parentBody _ wctx = do
        -- This workflow's own input row is written before the lock is
        -- taken; the case signals once it holds the table.
        takeMVar lockTaken
        finished <- newEmptyMVar
        worker <-
          forkIO $ do
            result <-
              debounceInWorkflow
                wctx
                childRef
                (debouncerNew {debouncerQueueName = Just queueName})
                "drop-key"
                (secondsDuration 3)
                (Just (encodeWorkflowInput (7 :: Int)))
            putMVar finished result
        takeMVar dropNow
        killThread worker
        early <- tryTakeMVar finished
        case early of
          -- The insert cannot have finished under the lock; a debounce that
          -- did means the case proved nothing.
          Just _ -> throwIO (userError "the debounce finished under the lock")
          Nothing -> pure (Right ())
  parentRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "parent") parentBody >>= either (fail . show) pure
  exec <- launch dbos >>= either (fail . show) pure
  _ <- registerQueue dbos queueName defaultQueueOptions NeverUpdate >>= either (fail . show) pure
  bracket (pure ()) (\_ -> shutdown dbos) $ \_ -> do
    started <-
      startWorkflow
        exec
        parentRef
        (startOptionsDefault {startWorkflowId = Just parentId})
        (Just (encodeWorkflowInput ()))
    handle <-
      case (started :: Either (Error EngineOnly) (WorkflowHandle IO EngineOnly)) of
        Left err -> fail (show err)
        Right startedHandle -> pure startedHandle
    let url = config.configDatabaseUrl
    bracket (acquireRaw url) Connection.release $ \lockConn ->
      bracket (acquireRaw url) Connection.release $ \pollConn -> do
        runScript lockConn "begin"
        runScript lockConn "lock table dbos.workflow_input in access exclusive mode"
        putMVar lockTaken ()
        waitForInputInsert pollConn appName
        putMVar dropNow ()
        runScript lockConn "rollback"
        settled <- handleResult handle
        case settled of
          Left err -> fail (show err)
          Right _ -> pure ()
    held <-
      getWorkflowRecord dbos childId >>= either (fail . show) pure
    case held of
      Nothing -> fail "the dropped debounce created no child"
      Just row -> do
        (row.workflowRecordStatus :: WorkflowStatus) @?= Delayed
        row.workflowRecordParentWorkflowId @?= Just parentId
    listed <- listWorkflowSteps dbos parentId >>= either (fail . show) pure
    let recorded = [(step.stepInfoId, step.stepInfoName, step.stepInfoChild) | step <- listed]
    recorded
      @?= [ (0, "DBOS.debounceDelayedWorkflow", Nothing),
            (1, "child", Just childId)
          ]

-- | Tags a connection string so its backends can be told from every other
-- test's on the shared server.
withAppName :: Text -> Text -> Text
withAppName url tag
  | "?" `Text.isInfixOf` url = url <> "&application_name=" <> tag
  | otherwise = url <> "?application_name=" <> tag

acquireRaw :: Text -> IO Connection.Connection
acquireRaw url =
  Connection.acquire Pqi.adapter (ConnectionSettings.connectionString url)
    >>= either (fail . show) pure

runScript :: Connection.Connection -> Text -> IO ()
runScript conn sql =
  Connection.use conn (Session.script sql) >>= either (fail . show) pure

-- | Waits until this case's instance has a backend waiting on the
-- @workflow_input@ relation lock — the fresh enqueue's insert. Reading
-- @pg_locks@ rather than the query text keeps it independent of how the
-- driver spells a prepared statement.
waitForInputInsert :: Connection.Connection -> Text -> IO ()
waitForInputInsert conn tag = go (0 :: Int)
  where
    go attempts = do
      waiting <- Connection.use conn (Session.statement tag inputLockWaitersStatement) >>= either (fail . show) pure
      if waiting > 0
        then pure ()
        else
          if attempts >= 1500
            then fail "the enqueue never reached its insert"
            else threadDelay 20000 >> go (attempts + 1)

inputLockWaitersStatement :: Statement.Statement Text Int64
inputLockWaitersStatement =
  Statement.preparable
    "select count(*) from pg_stat_activity a join pg_locks l on l.pid = a.pid \
    \where a.application_name = $1 and not l.granted \
    \and l.relation = 'dbos.workflow_input'::regclass"
    (Encoders.param (Encoders.nonNullable Encoders.text))
    (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))
