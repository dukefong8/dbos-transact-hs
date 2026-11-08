{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | The debouncer over live Postgres: sysdb-bounce coalescing with real
-- rows, real launches, and row-field observations. Every case owns its
-- rows via a unique tag and its queues via a unique suffix.
module DBOS.Transact.DebouncerTest (tests) where

import DBOS.DualStack (liveCaseWith)
import DBOS.Prelude
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact.Serialization (encodeWorkflowInput)
import DBOS.Transact
import DBOS.Transact.Management (stepInfoFromRecord)
import DBOS.Transact.Connection (SomeSystemDB (..), runSystemDB)
import DBOS.Transact.DebouncerCases
import DBOS.Transact.Logger (SomeTracer (..), acquireLoggerBackend, ioTracer, nullTracer)
import Test.Tasty (TestTree, testGroup, withResource)

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
          liveCaseWith (withDebouncerFixture getBackend (ioTracer . fst <$> getLogger)) "a key collision between workflows is refused" scenarioKeyCollisionRefused checkKeyCollisionRefused
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
