{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | The debouncer over the simulator: the same five scenarios, judged by
-- the same checks, over the in-memory backend with the same engine calls.
-- Traces pin the sim event record per case.
module DBOS.Transact.DebouncerTestSim (tests) where

import DBOS.DualStack (simCase)
import DBOS.Prelude
import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import Data.Text qualified as Text
import DBOS.IOSimTracer (simTracer)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.IOSim (memLaunchOnWith, newMemDB, simInstance)
import DBOS.Transact
import DBOS.Transact.Connection (SomeSystemDB (..), runSystemDB)
import DBOS.Transact.DebouncerCases
import DBOS.Transact.Recovery (EngineEvent (..))
import DBOS.Transact.Step (WorkflowEvent (..))
import Test.Tasty (DependencyType (..), TestTree, dependentTestGroup)
import Test.Tasty.HUnit ((@?=))

tests :: TestTree
tests =
  -- Sequential: cases announce through one shared stderr, so parallel
  -- 'printSimTrace' calls would interleave their lines mid-character.
  -- 'AllFinish' keeps every case running on a failure, in order.
  dependentTestGroup
    "Debouncer (Sim)"
    AllFinish
    [ simCase simDebouncerFixture "a first debounce creates a delayed debounced row" scenarioFirstDebounceDelays checkFirstDebounceDelays traceFirstDebounceDelays,
      simCase simDebouncerFixture "a second debounce coalesces onto the same row" scenarioSecondDebounceCoalesces checkSecondDebounceCoalesces traceSecondDebounceCoalesces,
      simCase simDebouncerFixture "a foreign holder refuses the debounce" scenarioForeignHolderRefused checkForeignHolderRefused traceForeignHolderRefused,
      simCase simDebouncerFixture "debouncing inside a workflow records a step" scenarioInWorkflowDebounceRecordsStep checkInWorkflowDebounceRecordsStep traceInWorkflowDebounceRecordsStep,
      simCase simDebouncerFixture "a timeout caps the extension" scenarioTimeoutCapsExtension checkTimeoutCapsExtension traceTimeoutCapsExtension
    ]

simDebouncerFixture :: forall s. IOSim s (DebouncerFixture (IOSim s))
simDebouncerFixture = do
  mem <- newMemDB
  dbos <- simInstance
  runsVar <- newTVarIO (0 :: Int)
  outputsVar <- newTVarIO ([] :: [Text])
  tagCounter <- newTVarIO (0 :: Int)
  let targetQueue = "db-target-q"
      foreignQueue = "db-foreign-q"
      echoBody :: forall exec. Text -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Text)
      echoBody input _ = do
        atomically (modifyTVar runsVar (+ 1))
        atomically (modifyTVar outputsVar (<> [input]))
        pure (Right input)
      otherBody :: forall exec. () -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Text)
      otherBody _ _ = pure (Right "parked")
      freshTag prefix = do
        n <- atomically $ do
          k <- readTVar tagCounter
          writeTVar tagCounter (k + 1)
          pure k
        pure (prefix <> "-sim-" <> Text.pack (show n))
  targetRef <- registerWorkflowRef dbos (newWorkflowKey "echo") echoBody >>= either (fail . show) pure
  otherRef <- registerWorkflowRef dbos (newWorkflowKey "other") otherBody >>= either (fail . show) pure
  -- The in-workflow debounce's parent: registered before launch like every
  -- workflow, since recovery starts inside launch. Only the records-step
  -- case runs it, passing its tag as input.
  let parentBody :: forall exec. Text -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Text)
      parentBody tag wctx = do
        debounced <- debounceInWorkflow wctx targetRef (debouncerNew {debouncerQueueName = Just targetQueue}) tag (secondsDuration 3) (Just (encodeWorkflowValue ("one" :: Text)))
        case debounced of
          Right joined -> pure (Right joined.workflowId)
          Left err -> pure (Left err)
  _ <- registerWorkflow dbos (newWorkflowKey "debounce-parent") parentBody >>= either (fail . show) pure
  exec <- memLaunchOnWith mem simTracer dbos (Just [targetQueue])
  _ <- registerQueue dbos targetQueue defaultQueueOptions NeverUpdate >>= either (fail . show) pure
  _ <- registerQueue dbos foreignQueue defaultQueueOptions NeverUpdate >>= either (fail . show) pure
  pure
    DebouncerFixture
      { dbDBOS = dbos,
        dbExecutor = exec,
        dbTargetRef = targetRef,
        dbOtherRef = otherRef,
        dbTargetQueue = targetQueue,
        dbForeignQueue = foreignQueue,
        dbFreshTag = freshTag,
        dbFreshWid = \prefix -> WorkflowId . (<> "-wid") <$> freshTag prefix,
        dbReadRow = \wid -> do
          found <- runSystemDB (SomeSystemDB mem) (\db -> SystemDB.getWorkflow db wid)
          case found of
            Left err -> throwIO (userError (show err))
            Right row -> pure row,
        dbListSteps = \wid -> do
          listed <- runSystemDB (SomeSystemDB mem) (\db -> SystemDB.listSteps db wid True Nothing Nothing Nothing)
          case listed of
            Left err -> throwIO (userError (show err))
            Right steps -> pure steps,
        dbUserRuns = readTVarIO runsVar,
        dbUserOutputs = readTVarIO outputsVar,
        dbAwaitUser = \wid -> do
          _ <- driveQueue dbos
          settled <- waitForWorkflow dbos wid
          case settled of
            Left err -> throwIO (userError (show err))
            Right _ -> pure ()
          row <- runSystemDB (SomeSystemDB mem) (\db -> SystemDB.getWorkflow db wid)
          case row of
            Left err -> throwIO (userError (show err))
            Right found -> pure (fmap (.workflowRecordStatus) found)
      }

-- * Typed event assertions (sim-only)

traceFirstDebounceDelays :: forall a. SimTrace a -> IO ()
traceFirstDebounceDelays tr = do
  -- The user id is engine-generated, so no workflow event pins it; the
  -- shared check asserts the row, outputs, and run count.
  selectTraceEventsDynamic tr @?= [EngineRecovered 0, EngineLaunched "sim-app" "sim-executor" "0.0.0"]

traceSecondDebounceCoalesces :: forall a. SimTrace a -> IO ()
traceSecondDebounceCoalesces tr = do
  selectTraceEventsDynamic tr @?= [EngineRecovered 0, EngineLaunched "sim-app" "sim-executor" "0.0.0"]

traceForeignHolderRefused :: forall a. SimTrace a -> IO ()
traceForeignHolderRefused tr = do
  -- The only row is the planted foreign holder (the first minted id);
  -- the refused debounce enqueues nothing.
  selectTraceEventsDynamic tr @?= [WorkflowEnqueued "sim-0" "db-foreign-q"]
  selectTraceEventsDynamic tr @?= [EngineRecovered 0, EngineLaunched "sim-app" "sim-executor" "0.0.0"]

traceInWorkflowDebounceRecordsStep :: forall a. SimTrace a -> IO ()
traceInWorkflowDebounceRecordsStep tr = do
  selectTraceEventsDynamic tr @?= [EngineRecovered 0, EngineLaunched "sim-app" "sim-executor" "0.0.0"]

traceTimeoutCapsExtension :: forall a. SimTrace a -> IO ()
traceTimeoutCapsExtension tr = do
  -- The fresh enqueue consumes the pinned id, the first minted in the
  -- run; the capped second bounce enqueues nothing.
  selectTraceEventsDynamic tr @?= [WorkflowEnqueued "sim-0" "db-foreign-q"]
  selectTraceEventsDynamic tr @?= [EngineRecovered 0, EngineLaunched "sim-app" "sim-executor" "0.0.0"]
