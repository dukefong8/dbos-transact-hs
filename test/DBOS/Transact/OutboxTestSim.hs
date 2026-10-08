{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | The transactional outbox over the simulator: the same seven scenarios,
-- judged by the same checks, over staged TVar maps with the same engine
-- calls. The fake datasource snapshots the whole store around each
-- transaction and restores it when the body throws — the documented
-- emulation of rollback (live Postgres rolls back for real); checkpoints
-- go through a real 'transaction_completion' map so replay adoption is
-- genuine. Traces pin the sim event record per case.
module DBOS.Transact.OutboxTestSim (tests) where

import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import Data.Aeson qualified as Aeson
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text qualified as Text
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (simTracer)
import DBOS.Prelude
import DBOS.SystemDB (NewWorkflow (..), Submission (..), WorkflowRecord (..), newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.Transact.Connection (SomeSystemDB (..), runSystemDB)
import DBOS.SystemDB.Error qualified as SysErr
import DBOS.SystemDB.IOSim (memLaunchOn, newMemDB)
import DBOS.Transact
import DBOS.Transact.Workflow (maxRecoveryAttempts)
import DBOS.Transact.Datasource (RecordedOutcome (..), TransactionEvent (..))
import DBOS.Transact.OutboxCases
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
    "Outbox transactions (Sim)"
    AllFinish
    [ simCase simOutboxFixture "the atomic workflow commits order and notification together" scenarioAtomicCommit checkAtomicCommit traceAtomicCommit,
      simCase simOutboxFixture "the atomic workflow rolls back on an app throw, then resumes" scenarioAtomicRollback checkAtomicRollback traceAtomicRollback,
      simCase simOutboxFixture "the transactional enqueue commits order and enqueue together" scenarioEnqueueCommit checkEnqueueCommit traceEnqueueCommit,
      simCase simOutboxFixture "the transactional enqueue rolls back on an app throw" scenarioEnqueueRollbackApp checkEnqueueRollbackApp traceEnqueueRollbackApp,
      simCase simOutboxFixture "the transactional enqueue rolls back on a database error" scenarioEnqueueRollbackDb checkEnqueueRollbackDb traceEnqueueRollbackDb,
      simCase simOutboxFixture "a failed atomic send redrives to exactly one notification" scenarioAtomicRedrive checkAtomicRedrive traceAtomicRedrive,
      simCase simOutboxFixture "a failed enqueued notification redrives to exactly one notification" scenarioEnqueueRedrive checkEnqueueRedrive traceEnqueueRedrive
    ]

-- * The sim store and fake datasource

data SimStore = SimStore
  { simOrders    :: Map Int (Text, Text, Int, Text),
    simNextId    :: Int,
    simCommits   :: Map (Text, Int) (Text, RecordedOutcome),
    simSent      :: Int
  }

newSimStore :: (MonadSTM m) => m (StrictTVar m SimStore)
newSimStore =
  newTVarIO
    SimStore
      { simOrders = Map.empty,
        simNextId = 1,
        simCommits = Map.empty,
        simSent = 0
      }

-- | Fault flags live outside the snapshot: live Postgres rolls back table
-- rows, never process memory, so an armed fault stays consumed across the
-- rollback. Keeping them in the store would resurrect the fault on every
-- restore (replacement re-throws unguarded; resume re-throws forever).
-- the attempt staged, mirroring a real rollback. Live Postgres rolls back
-- for real; the sim restores the snapshot — same observable atomicity.
mkSimDs :: (MonadSTM m, MonadCatch m) => StrictTVar m SimStore -> DataSource m
mkSimDs storeVar =
  DataSource
    { dsName = "ob-sim-db",
      dsSchema = "outbox_store",
      dsCheck = \(WorkflowId widText) _name step -> do
        store <- readTVarIO storeVar
        pure (Right (snd <$> Map.lookup (widText, step) store.simCommits)),
      dsWithTransaction = \_ action -> do
        snap <- readTVarIO storeVar
        outcome <- try (action (Tx (\_ _ -> error "outbox sim: statements run through fixture ops")))
        case outcome of
          -- Restore and rethrow: like the live bracket, a body failure
          -- aborts the attempt without recording. The engine loop above
          -- decides retry vs panic, identically on both stacks.
          Left (ex :: SomeException) -> atomically (writeTVar storeVar snap) >> throwIO ex
          Right v -> pure (Right v),
      dsRecordOutput = \(Tx _) (WorkflowId widText) name step text ->
        atomically $ do
          store <- readTVar storeVar
          if Map.member (widText, step) store.simCommits
            then pure False
            else do
              writeTVar storeVar (store {simCommits = Map.insert (widText, step) (name, RecordedOutput text) store.simCommits})
              pure True,
      dsRecordError = \(Tx _) (WorkflowId widText) name step text ->
        atomically $ do
          store <- readTVar storeVar
          if Map.member (widText, step) store.simCommits
            then pure False
            else do
              writeTVar storeVar (store {simCommits = Map.insert (widText, step) (name, RecordedError text) store.simCommits})
              pure True,
      dsStepName = \(WorkflowId widText) step -> do
        store <- readTVarIO storeVar
        pure (Right (fst <$> Map.lookup (widText, step) store.simCommits)),
      dsDeleteCheckpoints = \(WorkflowId widText) step -> do
        atomically (modifyTVar storeVar (\store -> store {simCommits = Map.filterWithKey (\(w, s) _ -> w /= widText || s < step) store.simCommits}))
        pure (Right ())
    }

-- * The sim fixture

simOutboxFixture :: forall s. IOSim s (OutboxFixture (IOSim s))
simOutboxFixture = do
  mem <- newMemDB
  dbos <- newDBOS (configNew "sim-ob-app" "") {configAppVersion = Just "sim-ob-version", configExecutorId = Just "sim-ob-executor"}
  storeVar <- newSimStore
  faultVar <- newTVarIO TxOk
  sendFailsVar <- newTVarIO (0 :: Int)
  tagCounter <- newTVarIO (0 :: Int)
  widCounter <- newTVarIO (0 :: Int)
  let ds = mkSimDs storeVar
      atomicKey = newWorkflowKey "place_order"
      notifyKey = newWorkflowKey "send_notification_workflow"
      consumeTxFault = atomically $ do
        fault <- readTVar faultVar
        writeTVar faultVar TxOk
        pure fault
      consumeSendFail = atomically $ do
        n <- readTVar sendFailsVar
        if n > 0
          then writeTVar sendFailsVar (n - 1) >> pure True
          else pure False
      insertOp _tx cust item qty = do
        oid <- atomically $ do
          store <- readTVar storeVar
          let oid = store.simNextId
          writeTVar storeVar (store {simOrders = Map.insert oid (cust, item, qty, "PENDING") store.simOrders, simNextId = oid + 1})
          pure oid
        -- Faults fire after the insert stages, so the rollback below drops
        -- a genuine partial commit (live Postgres rolls back for real).
        fault <- consumeTxFault
        case fault of
          TxThrowApp -> throwIO (userError "outbox probe: app throw after insert")
          -- A statement failure surfaces engine-typed (like the live
          -- txRunner's 'Backend' throw), so runTxOutside converts it to a
          -- Left value instead of propagating.
          TxThrowDb -> throwIO (SysErr.Backend (SysErr.BackendError "outbox sim: db failure after insert" Nothing SysErr.Permanent))
          TxOk -> pure oid
      enqueueOp _tx oid cust = do
        -- Inputs mirror the SQL path's stored shape (positionalArgs object),
        -- which 'matchCustomer' parses; rows carry the launch identity
        -- (sim-app/0.0.0), which the version-scoped claim requires.
        let widText = "sim-enq-" <> Text.pack (show oid)
            input = encodeWorkflowValue (Aeson.object ["positionalArgs" Aeson..= (oid, cust, ("Widget B" :: Text)), "namedArgs" Aeson..= Aeson.object []])
            row =
              (newWorkflow widText)
                { newWorkflowName = Just "send_notification_workflow",
                  newWorkflowInput = Just input.serializedText,
                  newWorkflowSerialization = Nothing,
                  newWorkflowQueueName = Just "ob-notify-q",
                  newWorkflowExecutorId = Nothing,
                  newWorkflowApplicationName = Just "sim-app",
                  newWorkflowApplicationVersion = Just "0.0.0"
                }
        created <- runSystemDB (SomeSystemDB mem) (\db -> SystemDB.initWorkflow db row (Just maxRecoveryAttempts) Fresh Nothing)
        case created of
          Left err -> throwIO (userError (show err))
          Right _ -> pure (WorkflowId widText)
      markOp _tx oid = atomically $ do
        store <- readTVar storeVar
        case Map.lookup oid store.simOrders of
          Just (cust, item, qty, _) -> writeTVar storeVar (store {simOrders = Map.insert oid (cust, item, qty, "SENT") store.simOrders})
          Nothing                   -> throwIO (userError "outbox sim: mark SENT on a missing order")
      noteSent = atomically (modifyTVar storeVar (\store -> store {simSent = store.simSent + 1}))
      atomicWf :: forall exec. (Text, Text, Int) -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
      atomicWf = atomicBody ds insertOp markOp noteSent consumeSendFail
      notifyWf :: forall exec. Envelope -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) ())
      notifyWf = notifyBody ds markOp noteSent consumeSendFail
  atomicRef <- registerWorkflowRef dbos atomicKey atomicWf >>= either (fail . show) pure
  notifyRef <- registerWorkflowRef dbos notifyKey notifyWf >>= either (fail . show) pure
  exec <- memLaunchOn mem simTracer dbos
  _ <- registerQueue dbos "ob-notify-q" defaultQueueOptions NeverUpdate >>= either (fail . show) pure
  pure
    OutboxFixture
      { obDBOS = dbos,
        obExecutor = exec,
        obDataSource = ds,
        obAtomicRef = atomicRef,
        obNotifyRef = notifyRef,
        obFreshTag = \prefix -> do
          n <- atomically $ do
            k <- readTVar tagCounter
            writeTVar tagCounter (k + 1)
            pure k
          pure (prefix <> "-sim-" <> Text.pack (show n)),
        obFreshWid = \prefix -> do
          n <- atomically $ do
            k <- readTVar widCounter
            writeTVar widCounter (k + 1)
            pure k
          pure (WorkflowId ("sim-ob-" <> prefix <> "-" <> Text.pack (show n))),
        obTxInsert = insertOp,
        obTxEnqueue = enqueueOp,
        obTxMarkSent = markOp,
        obNoteSent = noteSent,
        obArmTxThrow = atomically (writeTVar faultVar TxThrowApp),
        obArmTxDbError = atomically (writeTVar faultVar TxThrowDb),
        obArmSendFail = \n -> atomically (writeTVar sendFailsVar n),
        obConsumeSendFail = consumeSendFail,
        obConsumeTxFault = consumeTxFault,
        obRunAtomic = \wid cust item qty -> runWorkflow exec atomicKey wid (Just (encodeWorkflowValue (cust, item, qty))),
        obPlaceEnqueued = placeEnqueued ds insertOp enqueueOp,
        obOrderIds = \tag -> do
          store <- readTVarIO storeVar
          pure [oid | (oid, (cust, _, _, _)) <- Map.toList store.simOrders, cust == tag],
        obFindNotification = findNotification dbos,
        obAwaitNotification = \wid -> do
          _ <- driveQueue dbos
          settled <- waitForWorkflow dbos wid
          case settled of
            Left err -> throwIO (userError (show err))
            Right _  -> pure ()
          row <- readRowShared (SomeSystemDB mem) wid
          pure (fmap (.workflowRecordStatus) row),
        obResumeNotification = \wid -> do
          resumed <- resumeWorkflow dbos wid Nothing
          case resumed of
            Left err -> throwIO (userError (show err))
            Right _  -> pure ()
          _ <- driveQueue dbos
          settled <- waitForWorkflow dbos wid
          case settled of
            Left err -> throwIO (userError (show err))
            Right _  -> pure ()
          row <- readRowShared (SomeSystemDB mem) wid
          pure (fmap (.workflowRecordStatus) row),
        obResumeAtomic = \wid -> do
          resumed <- resumeWorkflow dbos wid Nothing
          case resumed of
            Left err -> throwIO (userError (show err))
            Right _  -> pure ()
          settled <- waitForWorkflow dbos wid
          case settled of
            Left err -> throwIO (userError (show err))
            Right _  -> pure ()
          row <- readRowShared (SomeSystemDB mem) wid
          pure (fmap (.workflowRecordStatus) row),
        obOrderStatus = \oid -> do
          store <- readTVarIO storeVar
          pure ((\(_, _, _, status) -> status) <$> Map.lookup oid store.simOrders),
        obOrderCount = \tag -> do
          store <- readTVarIO storeVar
          pure (length [() | (_, (cust, _, _, _)) <- Map.toList store.simOrders, cust == tag]),
        obSentCount = (.simSent) <$> readTVarIO storeVar,
        obReadRow = readRowShared (SomeSystemDB mem),
        obSystemDB = SomeSystemDB mem
      }

-- * Typed event assertions (sim-only)

traceAtomicCommit :: forall a. SimTrace a -> IO ()
traceAtomicCommit tr = do
  selectTraceEventsDynamic tr
    @?= [ TransactionRunning "sim-ob-atomic-commit-0" "insert_order" 0,
          TransactionOutputRecorded "sim-ob-atomic-commit-0" "insert_order" 0,
          TransactionRunning "sim-ob-atomic-commit-0" "update_notification_status" 2,
          TransactionOutputRecorded "sim-ob-atomic-commit-0" "update_notification_status" 2
        ]
  selectTraceEventsDynamic tr
    @?= [ StepRunning "send_notification" 1,
          StepOutputRecorded "send_notification" 1,
          WorkflowCompleted "sim-ob-atomic-commit-0"
        ]
  selectTraceEventsDynamic tr @?= [EngineRecovered 0, EngineLaunched "sim-app" "sim-executor" "0.0.0"]

traceAtomicRollback :: forall a. SimTrace a -> IO ()
traceAtomicRollback tr = do
  selectTraceEventsDynamic tr
    @?= [ TransactionRunning "sim-ob-atomic-rollback-0" "insert_order" 0,
          TransactionRunning "sim-ob-atomic-rollback-0" "insert_order" 0,
          TransactionOutputRecorded "sim-ob-atomic-rollback-0" "insert_order" 0,
          TransactionRunning "sim-ob-atomic-rollback-0" "update_notification_status" 2,
          TransactionOutputRecorded "sim-ob-atomic-rollback-0" "update_notification_status" 2
        ]
  selectTraceEventsDynamic tr
    @?= [ WorkflowPanicked "sim-ob-atomic-rollback-0",
          StepRunning "send_notification" 1,
          StepOutputRecorded "send_notification" 1,
          WorkflowCompleted "sim-ob-atomic-rollback-0"
        ]
  selectTraceEventsDynamic tr @?= [EngineRecovered 0, EngineLaunched "sim-app" "sim-executor" "0.0.0"]

traceEnqueueCommit :: forall a. SimTrace a -> IO ()
traceEnqueueCommit tr = do
  selectTraceEventsDynamic tr
    @?= [ TransactionRunning "sim-enq-1" "update_notification_status" 1,
          TransactionOutputRecorded "sim-enq-1" "update_notification_status" 1
        ]
  selectTraceEventsDynamic tr
    @?= [ StepRunning "send_notification" 0,
          StepOutputRecorded "send_notification" 0,
          WorkflowCompleted "sim-enq-1"
        ]
  selectTraceEventsDynamic tr @?= [EngineRecovered 0, EngineLaunched "sim-app" "sim-executor" "0.0.0"]

traceEnqueueRollbackApp :: forall a. SimTrace a -> IO ()
traceEnqueueRollbackApp tr = do
  -- The rolled-back placement emits nothing; the replacement notification
  -- runs to completion through the driven queue.
  selectTraceEventsDynamic tr
    @?= [ TransactionRunning "sim-enq-1" "update_notification_status" 1,
          TransactionOutputRecorded "sim-enq-1" "update_notification_status" 1
        ]
  selectTraceEventsDynamic tr
    @?= [ StepRunning "send_notification" 0,
          StepOutputRecorded "send_notification" 0,
          WorkflowCompleted "sim-enq-1"
        ]
  selectTraceEventsDynamic tr @?= [EngineRecovered 0, EngineLaunched "sim-app" "sim-executor" "0.0.0"]

traceEnqueueRollbackDb :: forall a. SimTrace a -> IO ()
traceEnqueueRollbackDb tr = do
  selectTraceEventsDynamic tr
    @?= [ TransactionRunning "sim-enq-1" "update_notification_status" 1,
          TransactionOutputRecorded "sim-enq-1" "update_notification_status" 1
        ]
  selectTraceEventsDynamic tr
    @?= [ StepRunning "send_notification" 0,
          StepOutputRecorded "send_notification" 0,
          WorkflowCompleted "sim-enq-1"
        ]
  selectTraceEventsDynamic tr @?= [EngineRecovered 0, EngineLaunched "sim-app" "sim-executor" "0.0.0"]

traceAtomicRedrive :: forall a. SimTrace a -> IO ()
traceAtomicRedrive tr = do
  selectTraceEventsDynamic tr
    @?= [ TransactionRunning "sim-ob-atomic-redrive-0" "insert_order" 0,
          TransactionOutputRecorded "sim-ob-atomic-redrive-0" "insert_order" 0,
          TransactionRunning "sim-ob-atomic-redrive-0" "insert_order" 0,
          TransactionReplaying "sim-ob-atomic-redrive-0" "insert_order" 0,
          TransactionRunning "sim-ob-atomic-redrive-0" "update_notification_status" 2,
          TransactionOutputRecorded "sim-ob-atomic-redrive-0" "update_notification_status" 2
        ]
  selectTraceEventsDynamic tr
    @?= [ StepRunning "send_notification" 1,
          WorkflowPanicked "sim-ob-atomic-redrive-0",
          StepRunning "send_notification" 1,
          StepOutputRecorded "send_notification" 1,
          WorkflowCompleted "sim-ob-atomic-redrive-0"
        ]
  selectTraceEventsDynamic tr @?= [EngineRecovered 0, EngineLaunched "sim-app" "sim-executor" "0.0.0"]

traceEnqueueRedrive :: forall a. SimTrace a -> IO ()
traceEnqueueRedrive tr = do
  selectTraceEventsDynamic tr
    @?= [ TransactionRunning "sim-enq-1" "update_notification_status" 1,
          TransactionOutputRecorded "sim-enq-1" "update_notification_status" 1
        ]
  selectTraceEventsDynamic tr
    @?= [ StepRunning "send_notification" 0,
          WorkflowPanicked "sim-enq-1",
          StepRunning "send_notification" 0,
          StepOutputRecorded "send_notification" 0,
          WorkflowCompleted "sim-enq-1"
        ]
  selectTraceEventsDynamic tr @?= [EngineRecovered 0, EngineLaunched "sim-app" "sim-executor" "0.0.0"]
