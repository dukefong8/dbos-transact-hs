{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Shared management scenarios: one body per case, judged by one pure
-- check on each stack, over the shared 'MgmtFixture'. The live tree
-- ('DBOS.Transact.ManagementTest') runs them over Postgres with full
-- launches; the sim tree ('DBOS.Transact.ManagementTestSim') over the
-- in-memory backend with the same engine calls, including the supervisor
-- passes that run resumed and forked rows. Engine errors throw (via
-- 'MonadThrow'), so both trees assert on plain values.
--
-- Bodies that capture per-case state (counters, gates) live as top-level
-- helpers taking that state explicitly: @MonoLocalBinds@ cannot
-- generalize a @let@-bound rank-2 body.
module DBOS.Transact.ManagementCases
  ( MgmtFixture (..),
    mkMgmtFixture,
    cancellableBody,
    treeChildBody,
    treeParentBody,
    scenarioUnlaunched,
    scenarioCancelMissing,
    scenarioResumeMissing,
    scenarioCancelResumeRun,
    scenarioResumeOntoQueue,
    scenarioCancelTree,
    scenarioDelete,
    scenarioRetrieve,
    scenarioListSteps,
    scenarioListStepsInWorkflow,
    scenarioSingular,
    scenarioForkFromBeginning,
    scenarioForkTakesIdAndQueue,
    scenarioForkFromStep,
    scenarioForkFromFailure,
    scenarioBulkCancelResume,
    scenarioBulkFork,
    scenarioForkRefusals,
    scenarioForkPartitioned,
    scenarioAttributes,
    scenarioAttributesInWorkflow,
    scenarioDelayRelease,
    scenarioDelayReleaseInWorkflow,
    scenarioRewindRunsAgain,
    scenarioRewindRefusals,
    scenarioRewindUndoCut,
    scenarioRewindKeepBelowCut,
    scenarioRewindQueueVersion,
    scenarioRewindInWorkflow,
    scenarioRewindCallerStep,
    scenarioRewindAdoptsChild,
    scenarioRewindRefusedTakesNoStep,
    scenarioRewindDropsDatasourceCheckpoints,
    scenarioCompletionClearsCheckpoints,
    scenarioChildClearsCheckpoints,
    checkUnlaunched,
    checkCancelMissing,
    checkResumeMissing,
    checkCancelResumeRun,
    checkResumeOntoQueue,
    checkCancelTree,
    checkDelete,
    checkRetrieve,
    checkListSteps,
    checkListStepsInWorkflow,
    checkSingular,
    checkForkFromBeginning,
    checkForkTakesIdAndQueue,
    checkForkFromStep,
    checkForkFromFailure,
    checkBulkCancelResume,
    checkBulkFork,
    checkForkRefusals,
    checkForkPartitioned,
    checkAttributes,
    checkAttributesInWorkflow,
    checkDelayRelease,
    checkDelayReleaseInWorkflow,
    checkRewindRunsAgain,
    checkRewindRefusals,
    checkRewindUndoCut,
    checkRewindKeepBelowCut,
    checkRewindQueueVersion,
    checkRewindInWorkflow,
    checkRewindCallerStep,
    checkRewindAdoptsChild,
    checkRewindRefusedTakesNoStep,
    checkRewindDropsDatasourceCheckpoints,
    checkCompletionClearsCheckpoints,
    checkChildClearsCheckpoints,
  )
where

import DBOS.Prelude
import Data.Int (Int64)
import Data.Map.Strict qualified as Map
import Data.Text qualified as Text
import GHC.Stack (HasCallStack)
import DBOS.SystemDB (EventRecord (..), Fork (..), ForkOptions (..), ForkPoint (..), NotificationRecord (..), QueueName (..), SerializedWorkflowValue (..), StreamRecord (..), SysRewindOptions (..), WorkflowDelay (..), WorkflowFilter (..), WorkflowId (..), WorkflowRecord (..), WorkflowStatus (..), defaultForkOptions, defaultWorkflowFilter, forkNew, internalQueueName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.Transact
  ( CodecError,
    Config (..),
    DBOS,
    Enqueue (..),
    EngineOnly,
    Error (..),
    Executor,
    Serializer (..),
    StartOptions (..),
    StepRecord (..),
    WorkflowCtx,
    WorkflowHandle (..),
    WorkflowKey,
    WorkflowRef,
    cancelWorkflow,
    cancelWorkflows,
    closeStream,
    decodeWorkflowValue,
    defaultRewindOptions,
    deleteWorkflow,
    deleteWorkflows,
    encodeWorkflowValue,
    enqueueWorkflow,
    enqueueNew,
    forkWorkflow,
    forkFrom,
    forkWorkflows,
    getWorkflowStatus,
    handleResult,
    handleStatus,
    millisDuration,
    newDBOS,
    newWorkflowKey,
    recv,
    registerDataSource,
    registerWorkflow,
    registerWorkflowRef,
    resumeWorkflow,
    resumeWorkflows,
    retrieveWorkflow,
    rewindWorkflow,
    rewindWorkflows,
    rewindWorkflowsInWorkflow,
    RewindOptions (..),
    runTxStep,
    runWorkflow,
    runStep,
    send,
    setEvent,
    setWorkflowDelay,
    setWorkflowDelayInWorkflow,
    Topic (..),
    updateWorkflowAttributes,
    updateWorkflowAttributesInWorkflow,
    listWorkflowStepsInWorkflow,
    listWorkflowSteps,
    listWorkflows,
    secondsDuration,
    shutdown,
    startChildWorkflow,
    startWorkflow,
    startOptionsDefault,
    waitForWorkflow,
    writeStream,
  )
import DBOS.Transact.DatasourceCases (FakeDs (..), mkFakeDs, protoConfig)
import DBOS.Transact.Datasource (DataSource (..), Tx (..))
import DBOS.Transact.Logger (SomeTracer (..))
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Instance (dequeueWorkflows, launchOnWithQueues)
import DBOS.Transact.Connection
  ( Connection,
    Owner (..),
    SomeSystemDB (..),
    newConnection,
    runSystemDB,
  )

-- | How a tree instantiation builds its world: fresh unlaunched instances
-- per call, the stack-specific full launch (version registration, recovery
-- of the executor's rows, supervisor fork), fresh id bases, row reads, and
-- a connection and identity for direct scopes. Live fills the rest with
-- Postgres and per-call UUIDs; the sim tree with 'MemSystemDB' and
-- deterministic names.
data MgmtFixture m = MgmtFixture
  { mfNewDBOS :: m (DBOS m),
    mfLaunch :: DBOS m -> m (Executor m),
    mfFreshBase :: m Text,
    mfReadRow :: WorkflowId -> m WorkflowRecord,
    mfConn :: m (Connection m),
    mfIdentity :: Identity,
    mfSystemDB :: SomeSystemDB m
  }

-- | One fixture builder over any backend: the tree passes its 'Config',
-- 'Identity', connection app name, id/entropy generators, plus its
-- 'SomeSystemDB' and 'SomeTracer'. Launches install the executor over an
-- explicitly built connection without forking a supervisor (like
-- 'WfFixture'): scenarios that need queued rows executed drive the
-- engine's dequeue entry themselves. Live passes Postgres + FastLogger;
-- sim passes 'MemSystemDB' + the sim carrier.
mkMgmtFixture :: forall m. (MonadMVar m, MonadSTM m, MonadThrow m)
              => Config ->
                 Identity ->
                 Text ->
                 m Text ->
                 m Word32 ->
                 SomeSystemDB m ->
                 SomeTracer m ->
                 m Text ->
                 m (MgmtFixture m)
mkMgmtFixture config identity connApp genId genEntropy sysdb tracer freshBase = do
  conn <- mkConn
  base <- freshBase
  pure
    MgmtFixture
      { mfNewDBOS = newDBOS config,
        -- The listen set is the internal queue only, so a driven dequeue
        -- sweeps one queue instead of every fixture queue the shared
        -- database has accumulated (EventCases scopes the same way).
        mfLaunch = \dbos -> do
          let QueueName internal = internalQueueName
          launchOnWithQueues dbos conn identity (Just [internal]),
        mfFreshBase = pure base,
        mfReadRow = \wid -> do
          found <- runSystemDB sysdb (\db -> SystemDB.getWorkflow db wid)
          case found of
            Right (Just record) -> pure record
            _ -> throwIO (userError "expected the workflow row to exist"),
        mfConn = pure conn,
        mfIdentity = identity,
        mfSystemDB = sysdb
      }
  where
    mkConn = do
      instanceId <- genId
      newConnection
        sysdb
        RustSerde
        (Just connApp)
        (secondsDuration 1)
        OwnerApplication
        instanceId
        genId
        genEntropy
        tracer

-- | Engine-only driver aliases: every scenario reads through these, so the
-- error channel pins to 'EngineOnly' once instead of at each call site.
-- Local copies are deliberate: this module carries only the aliases it uses.
runWf :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
      => Executor m -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWf = runWorkflow

startWfRef :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
           => Executor m -> WorkflowRef m EngineOnly -> StartOptions -> Maybe SerializedWorkflowValue -> m (Either (Error EngineOnly) (WorkflowHandle m EngineOnly))
startWfRef = startWorkflow

retrieveWf :: forall m. (MonadMVar m)
           => DBOS m -> WorkflowId -> m (Either (Error EngineOnly) (WorkflowHandle m EngineOnly))
retrieveWf = retrieveWorkflow

resultWf :: forall m. (MonadDelay m, MonadTime m, MonadMVar m, MonadThrow m)
         => WorkflowHandle m EngineOnly -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWf = handleResult

statusWf :: forall m. (MonadMVar m)
         => WorkflowHandle m EngineOnly -> m (Either (Error EngineOnly) (Maybe WorkflowStatus))
statusWf = handleStatus

listStepsWf :: forall m. (MonadMVar m)
            => DBOS m -> WorkflowId -> m (Either (Error EngineOnly) [StepRecord])
listStepsWf = listWorkflowSteps

-- | The cancellable body: counts its entries, then answers input plus five.
cancellableBody :: forall exec m. (MonadSTM m)
                => StrictTVar m Int ->
                   Int ->
                   WorkflowCtx exec m ->
                   m (Either (Error EngineOnly) Int)
cancellableBody ran input _ = do
  atomically (modifyTVar ran (+ 1))
  pure (Right (input + 5))

-- | The tree child: signals its start, then parks on the gate until the
-- cancel kills it.
treeChildBody :: forall exec m. (MonadMVar m)
              => StrictMVar m () ->
                 StrictMVar m () ->
                 () ->
                 WorkflowCtx exec m ->
                 m (Either (Error EngineOnly) Int)
treeChildBody started gate () _ = putMVar started () >> takeMVar gate >> pure (Right 1)

-- | The tree parent: starts the child, waits until it has begun (so the
-- cancel below cannot miss it), and hands back the child's id.
treeParentBody :: forall exec m. (MonadMVar m, MonadTimer m, MonadTime m, MonadCatch m)
               => WorkflowRef m EngineOnly ->
                  StrictMVar m () ->
                  () ->
                  WorkflowCtx exec m ->
                  m (Either (Error EngineOnly) Text)
treeParentBody childRef started () wctx = do
  startedChild <- startChildWorkflow wctx childRef startOptionsDefault Nothing
  case startedChild of
    Left err -> pure (Left err)
    Right handle -> takeMVar started >> pure (Right handle.workflowId)

-- | The management surface needs a launched instance. Returns whether the
-- unlaunched call was refused.
scenarioUnlaunched :: forall m. (MonadFork m, MonadMVar m, MonadSTM m, MonadThrow m)
                   => MgmtFixture m ->
                      m Bool
scenarioUnlaunched fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    refused <- cancelWorkflows dbos [WorkflowId "never-launched"] False
    listed <- listWorkflowSteps dbos (WorkflowId "never-launched")
    case (refused, listed) of
      (Left NotLaunched {}, Left NotLaunched {}) -> pure True
      other -> throwIO (userError ("expected not-launched refusals, got: " <> show other))

-- | Cancelling a workflow that does not exist is not an error.
scenarioCancelMissing :: forall m. (MonadFork m, MonadMVar m, MonadSTM m, MonadThrow m)
                      => MgmtFixture m ->
                         m [WorkflowId]
scenarioCancelMissing fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    _ <- fx.mfLaunch dbos
    cancelled <- cancelWorkflows dbos [WorkflowId "never-existed"] False
    case cancelled of
      Left err -> throwIO (userError (show err))
      Right ids -> pure ids

-- | Resuming a workflow that does not exist is an error. Returns the
-- missing ids the refusal names.
scenarioResumeMissing :: forall m. (MonadFork m, MonadMVar m, MonadSTM m, MonadThrow m)
                      => MgmtFixture m ->
                         m [Text]
scenarioResumeMissing fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    _ <- fx.mfLaunch dbos
    resumed <- resumeWorkflows dbos [WorkflowId "never-existed"] Nothing
    case resumed of
      Left (SystemDatabase (SystemDB.NonExistentWorkflow {workflowIds})) -> pure workflowIds
      other -> throwIO (userError ("expected a non-existent-workflow refusal, got: " <> show other))

-- | Cancelling makes a workflow terminal and leaves it resumable: cancel a
-- queued run before it starts, resume it, and watch it run exactly once.
-- Returns the cancelled ids, the terminal status, the pre-resume run
-- count, the resumed ids, the decoded result, and the final run count.
scenarioCancelResumeRun :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                        => MgmtFixture m ->
                           m (Text, [Text], Maybe WorkflowStatus, Int, [Text], Int, Int)
scenarioCancelResumeRun fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    ran <- newTVarIO (0 :: Int)
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "cancellable"
        workflowText = "hs-l2-mgmt-cancel-resume-" <> suffix
        wid = WorkflowId workflowText
    refE <- registerWorkflowRef dbos key (cancellableBody ran)
    ref <- case refE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.mfLaunch dbos
    started <-
      startWfRef
        exec
        ref
        (startOptionsDefault {startWorkflowId = Just wid, startQueue = Just (enqueueNew "no-runner-here")})
        (Just (encodeWorkflowValue (0 :: Int)))
    case started of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    cancelled <- cancelWorkflows dbos [wid] False
    cancelledIds <- case cancelled of
      Left err -> throwIO (userError (show err))
      Right ids -> pure [t | WorkflowId t <- ids]
    retrieved <- retrieveWf dbos wid
    status <- case retrieved of
      Left err -> throwIO (userError (show err))
      Right handle -> do
        status <- statusWf handle
        case status of
          Left err -> throwIO (userError (show err))
          Right mStatus -> pure mStatus
    before <- readTVarIO ran
    resumed <- resumeWorkflows dbos [wid] Nothing
    resumedIds <- case resumed of
      Left err -> throwIO (userError (show err))
      Right ids -> pure [t | WorkflowId t <- ids]
    -- Drive the dequeue entry synchronously: the resumed row runs now
    -- instead of waiting for a supervisor tick that bare launches never fork.
    drove <- dequeueWorkflows dbos
    case drove of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    waited <- waitForWorkflow dbos wid
    decoded <- case waited of
      Right (SystemDB.AwaitedSucceeded (Just output) _) ->
        case decodeWorkflowValue "result" (Just (SystemDB.SerializedWorkflowValue output Nothing)) :: Either CodecError Int of
          Right n -> pure n
          Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the resumed workflow's result, got: " <> show other))
    after <- readTVarIO ran
    pure (workflowText, cancelledIds, status, before, resumedIds, decoded, after)

-- | Resuming onto a named queue puts the workflow there... (first shape:
-- start on a runnerless queue, cancel, resume queueless, watch it run).
-- Returns the decoded result.
scenarioResumeOntoQueue :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                        => MgmtFixture m ->
                           m Int
scenarioResumeOntoQueue fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "resumable"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body input _ = pure (Right input)
        wid = WorkflowId ("hs-l2-mgmt-resume-queue-" <> suffix)
    refE <- registerWorkflowRef dbos key body
    ref <- case refE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.mfLaunch dbos
    started <-
      startWfRef
        exec
        ref
        (startOptionsDefault {startWorkflowId = Just wid, startQueue = Just (enqueueNew "no-runner-here")})
        (Just (encodeWorkflowValue (7 :: Int)))
    case started of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    _ <- cancelWorkflows dbos [wid] False
    resumed <- resumeWorkflows dbos [wid] Nothing
    case resumed of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    -- Drive the dequeue entry synchronously: the resumed row runs now
    -- instead of waiting for a supervisor tick that bare launches never fork.
    drove <- dequeueWorkflows dbos
    case drove of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    waited <- waitForWorkflow dbos wid
    case waited of
      Right (SystemDB.AwaitedSucceeded (Just output) _) ->
        case decodeWorkflowValue "result" (Just (SystemDB.SerializedWorkflowValue output Nothing)) :: Either CodecError Int of
          Right n -> pure n
          Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the queued resume to run, got: " <> show other))

-- | Cancelling a tree reaches the children. Returns the child's id, the
-- ids the tree cancel named, and the child's terminal status.
scenarioCancelTree :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                   => MgmtFixture m ->
                      m (Text, [Text], Maybe WorkflowStatus)
scenarioCancelTree fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    childStarted <- newEmptyMVar
    gate <- newEmptyMVar
    suffix <- fx.mfFreshBase
    let childKey = newWorkflowKey "tree-child"
        parentKey = newWorkflowKey "tree-parent"
        parentText = "hs-l2-mgmt-tree-parent-" <> suffix
        parentId = WorkflowId parentText
    childRefE <- registerWorkflowRef dbos childKey (treeChildBody childStarted gate)
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    parentReg <- registerWorkflow dbos parentKey (treeParentBody childRef childStarted)
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec parentKey parentId Nothing
    childText <- case ran of
      Left err -> throwIO (userError (show err))
      Right (Just output) ->
        case decodeWorkflowValue "result" (Just output) :: Either CodecError Text of
          Right t -> pure t
          Left err -> throwIO (userError (show err))
      Right Nothing -> throwIO (userError "the parent recorded no child")
    cancelled <- cancelWorkflows dbos [parentId] True
    cancelledIds <- case cancelled of
      Left err -> throwIO (userError (show err))
      Right ids -> pure [t | WorkflowId t <- ids]
    retrieved <- retrieveWf dbos (WorkflowId childText)
    status <- case retrieved of
      Left err -> throwIO (userError (show err))
      Right handle -> do
        status <- statusWf handle
        case status of
          Left err -> throwIO (userError (show err))
          Right mStatus -> pure mStatus
    pure (childText, cancelledIds, status)

-- | Deleting a workflow removes its row. Returns the deleted count and the
-- status a fresh handle reads.
scenarioDelete :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
               => MgmtFixture m ->
                  m (Word64, Maybe WorkflowStatus)
scenarioDelete fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "deletable"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body input wctx = runStep wctx "work" (const (pure input))
        wid = WorkflowId ("hs-l2-mgmt-delete-" <> suffix)
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowValue (1 :: Int)))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    deleted <- deleteWorkflows dbos [wid] True
    count <- case deleted of
      Left err -> throwIO (userError (show err))
      Right n -> pure n
    retrieved <- retrieveWf dbos wid
    case retrieved of
      Left err -> throwIO (userError (show err))
      Right handle -> do
        status <- statusWf handle
        case status of
          Left err -> throwIO (userError (show err))
          Right mStatus -> pure (count, mStatus)

-- | A workflow can be retrieved by id. Returns the status it reports and
-- the decoded result.
scenarioRetrieve :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                 => MgmtFixture m ->
                    m (Maybe WorkflowStatus, Int)
scenarioRetrieve fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "retrievable"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body input _ = pure (Right (input * 3))
        wid = WorkflowId ("hs-l2-mgmt-retrieve-" <> suffix)
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowValue (2 :: Int)))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    retrieved <- retrieveWf dbos wid
    case retrieved of
      Left err -> throwIO (userError (show err))
      Right handle -> do
        status <- statusWf handle
        mStatus <- case status of
          Left err -> throwIO (userError (show err))
          Right s -> pure s
        result <- resultWf handle
        decoded <- case result of
          Right (Just output) ->
            case decodeWorkflowValue "result" (Just output) :: Either CodecError Int of
              Right n -> pure n
              Left err -> throwIO (userError (show err))
          other -> throwIO (userError ("expected the retrieved result, got: " <> show other))
        pure (mStatus, decoded)

-- | A workflow's steps list in execution order with their zero-based ids,
-- and an id with no workflow behind it lists nothing: mirrors the Rust
-- @listing_a_workflows_steps_reports_them_in_execution_order@ case.
scenarioListSteps :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                  => MgmtFixture m ->
                     m ([Text], [Int], [Text])
scenarioListSteps fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "three-steps"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
        body () wctx = do
          one <- runStep wctx "one" (const (pure ()))
          case one of
            Left err -> pure (Left err)
            Right () -> do
              two <- runStep wctx "two" (const (pure ()))
              case two of
                Left err -> pure (Left err)
                Right () -> do
                  three <- runStep wctx "three" (const (pure ()))
                  pure (void three)
        wid = WorkflowId ("hs-l2-mgmt-liststeps-" <> suffix)
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowValue ()))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    listed <- listStepsWf dbos wid
    (names, ids) <- case listed of
      Left err -> throwIO (userError (show err))
      Right records ->
        pure
          ( [record.stepRecordStepName | record <- records],
            [record.stepRecordStepId | record <- records]
          )
    missing <- listStepsWf dbos (WorkflowId "never-existed")
    missingNames <- case missing of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepRecordStepName | record <- records]
    pure (names, ids, missingNames)

-- | The in-workflow listing is itself a step: it reads the steps recorded
-- before it and the engine records the snapshot after it, under the
-- cross-SDK step name.
scenarioListStepsInWorkflow :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                            => MgmtFixture m ->
                               m ([Text], [Text])
scenarioListStepsInWorkflow fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "self-lister"
        wid = WorkflowId ("hs-l2-mgmt-liststeps-in-" <> suffix)
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) [Text])
        body () wctx = do
          first <- runStep wctx "first" (const (pure ()))
          case first of
            Left err -> pure (Left err)
            Right () -> do
              listed <- listWorkflowStepsInWorkflow wctx wid
              pure (fmap (map (.stepRecordStepName)) listed)
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowValue ()))
    inside <- case ran of
      Left err -> throwIO (userError (show err))
      Right Nothing -> throwIO (userError "expected the workflow result")
      Right (Just output) ->
        case decodeWorkflowValue "result" (Just output) :: Either CodecError [Text] of
          Left err -> throwIO (userError (show err))
          Right names -> pure names
    outside <- listStepsWf dbos wid
    outsideNames <- case outside of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepRecordStepName | record <- records]
    pure (inside, outsideNames)

-- | The singular wrappers move one workflow each, per the Rust @DBOS@
-- singular surface: cancel (children skipped), resume (handle back),
-- delete, fork-from-the-top (handle back), and the singular status read.
scenarioSingular :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                 => MgmtFixture m ->
                    m (Maybe WorkflowStatus, Maybe WorkflowStatus, Maybe WorkflowStatus, Bool, Maybe WorkflowStatus)
scenarioSingular fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "singular"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body input _ = pure (Right input)
        wid :: Int -> WorkflowId
        wid n = WorkflowId ("hs-l2-mgmt-singular-" <> Text.pack (show n) <> "-" <> suffix)
    refE <- registerWorkflowRef dbos key body
    ref <- case refE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.mfLaunch dbos
    started1 <-
      startWfRef
        exec
        ref
        (startOptionsDefault {startWorkflowId = Just (wid 1), startQueue = Just (enqueueNew "no-runner-here")})
        (Just (encodeWorkflowValue (1 :: Int)))
    ran2 <- runWf exec key (wid 2) (Just (encodeWorkflowValue (2 :: Int)))
    ran3 <- runWf exec key (wid 3) (Just (encodeWorkflowValue (3 :: Int)))
    case (started1, ran2, ran3) of
      (Right _, Right _, Right _) -> pure ()
      other -> throwIO (userError ("expected one queued start and two runs, got: " <> show other))
    cancelled <- cancelWorkflow dbos (wid 1)
    case cancelled of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    afterCancel <- getWorkflowStatus dbos (wid 1)
    resumed <- resumeWorkflow dbos (wid 1) Nothing
    case resumed of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    afterResume <- getWorkflowStatus dbos (wid 1)
    deleted <- deleteWorkflow dbos (wid 2)
    case deleted of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    afterDelete <- getWorkflowStatus dbos (wid 2)
    forked <- forkWorkflow dbos (wid 3) Nothing defaultForkOptions
    (forkedDifferent, forkedStatus) <- case forked of
      Left err -> throwIO (userError (show err))
      Right handle -> do
        let forkedText = handle.workflowId
            sourceText = case wid 3 of WorkflowId text -> text
        status <- getWorkflowStatus dbos (WorkflowId forkedText)
        pure (forkedText /= sourceText, status)
    pure (afterCancel, afterResume, afterDelete, forkedDifferent, forkedStatus)

-- | The unlaunched call is refused.
checkUnlaunched :: Bool -> Either String ()
checkUnlaunched = checkEq True

-- | A missing row cancels to nothing.
checkCancelMissing :: [WorkflowId] -> Either String ()
checkCancelMissing = checkEq []

-- | The refusal names the missing id.
checkResumeMissing :: [Text] -> Either String ()
checkResumeMissing = checkEq ["never-existed"]

-- | The cancel reports the id it moved, the row is terminal, the cancelled
-- run never ran, the resume hands the id back, and the resumed run answers
-- once.
checkCancelResumeRun :: (Text, [Text], Maybe WorkflowStatus, Int, [Text], Int, Int) -> Either String ()
checkCancelResumeRun (widText, cancelled, status, before, resumed, decoded, after) = do
  checkEq [widText] cancelled
  checkEq (Just Cancelled) status
  checkEq 0 before
  checkEq [widText] resumed
  checkEq 5 decoded
  checkEq 1 after

-- | The queued resume runs the input.
checkResumeOntoQueue :: Int -> Either String ()
checkResumeOntoQueue = checkEq 7

-- | The tree cancel names the child, and the child is terminal.
checkCancelTree :: (Text, [Text], Maybe WorkflowStatus) -> Either String ()
checkCancelTree (childText, cancelled, status) = do
  checkEq True (childText `elem` cancelled)
  checkEq (Just Cancelled) status

-- | One row deleted; a fresh handle reads no row.
checkDelete :: (Word64, Maybe WorkflowStatus) -> Either String ()
checkDelete = checkEq (1, Nothing)

-- | The retrieved row reports success and its recorded output.
checkRetrieve :: (Maybe WorkflowStatus, Int) -> Either String ()
checkRetrieve = checkEq (Just Success, 6)

-- | Steps name and number in execution order; an unknown id lists empty.
checkListSteps :: ([Text], [Int], [Text]) -> Either String ()
checkListSteps (names, ids, missing) = do
  checkEq ["one", "two", "three"] names
  checkEq [0, 1, 2] ids
  checkEq [] missing

-- | The listing sees the steps before it, and is itself recorded after.
checkListStepsInWorkflow :: ([Text], [Text]) -> Either String ()
checkListStepsInWorkflow = checkEq (["first"], ["first", "DBOS.list_workflow_steps"])

-- | Cancel leaves it cancelled, resume re-enqueues it, delete removes the
-- row, and the fork is a new enqueued workflow.
checkSingular :: (Maybe WorkflowStatus, Maybe WorkflowStatus, Maybe WorkflowStatus, Bool, Maybe WorkflowStatus) -> Either String ()
checkSingular (afterCancel, afterResume, afterDelete, forkedDifferent, forkedStatus) = do
  checkEq (Just Cancelled) afterCancel
  checkEq (Just Enqueued) afterResume
  checkEq Nothing afterDelete
  checkEq True forkedDifferent
  checkEq (Just Enqueued) forkedStatus

-- | The forkable body: its first attempt fails, later attempts answer 8. The
-- counter is shared so both stacks can observe exactly-once retry semantics.
forkableBody :: forall exec m. (MonadSTM m)
             => StrictTVar m Int ->
                Int ->
                WorkflowCtx exec m ->
                m (Either (Error EngineOnly) Int)
forkableBody attempts _ _ = do
  attempt <- readTVarIO attempts
  atomically (modifyTVar attempts (+ 1))
  if attempt == 0
    then pure (Left (ErrorConfig "the first attempt fails"))
    else pure (Right 8)

-- | The staged body: records the names of the steps it actually runs.
stagedBody :: forall exec m. (MonadSTM m, MonadTime m, MonadCatch m)
           => StrictTVar m [Text] ->
              Int ->
              WorkflowCtx exec m ->
              m (Either (Error EngineOnly) Int)
stagedBody ran _ wctx = do
  outcomes <-
    traverse
      (\name -> runStep wctx name (const (atomically (modifyTVar ran (<> [name])) >> pure (0 :: Int))))
      ["one", "two", "three"]
  pure (fmap (const 0) (sequence outcomes))

-- | The plus-one body: answers its input plus one.
plusOneBody :: forall exec m. (Applicative m)
            => Int ->
               WorkflowCtx exec m ->
               m (Either (Error EngineOnly) Int)
plusOneBody input _ = pure (Right (input + 1))

-- | The echo body: answers its message.
echoTextBody :: forall exec m. (Applicative m)
             => Text ->
                WorkflowCtx exec m ->
                m (Either (Error EngineOnly) Text)
echoTextBody message _ = pure (Right message)

-- | The doubling body: answers twice its input.
doublingBody :: forall exec m. (Applicative m)
             => Int ->
                WorkflowCtx exec m ->
                m (Either (Error EngineOnly) Int)
doublingBody input _ = pure (Right (input * 2))

-- | The zero body: answers zero.
zeroBody :: forall exec m. (Applicative m)
         => () ->
            WorkflowCtx exec m ->
            m (Either (Error EngineOnly) Int)
zeroBody () _ = pure (Right 0)

-- | A fork from the beginning runs the source's workflow again under a new id
-- and succeeds on the retry. Returns the fork's id, the fork's decoded result,
-- and the source's attempt count.
scenarioForkFromBeginning :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                          => MgmtFixture m ->
                             m (Text, Int, Int)
scenarioForkFromBeginning fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    attempts <- newTVarIO (0 :: Int)
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "forkable"
        sourceText = "hs-l2-mgmt-fork-src-" <> suffix
        sourceId = WorkflowId sourceText
    refE <- registerWorkflowRef dbos key (forkableBody attempts)
    case refE of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    exec <- fx.mfLaunch dbos
    first <- runWf exec key sourceId (Just (encodeWorkflowValue (0 :: Int)))
    case first of
      Left _ -> pure ()
      Right _ -> throwIO (userError "the source was supposed to fail")
    forked <- forkWorkflows dbos [forkNew sourceText] defaultForkOptions
    forkedText <- case forked of
      Left err -> throwIO (userError (show err))
      Right [WorkflowId text] -> pure text
      other -> throwIO (userError ("expected exactly one fork, got: " <> show other))
    drove <- dequeueWorkflows dbos
    case drove of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    waited <- waitForWorkflow dbos (WorkflowId forkedText)
    decoded <- case waited of
      Right (SystemDB.AwaitedSucceeded (Just output) _) ->
        case decodeWorkflowValue "result" (Just (SystemDB.SerializedWorkflowValue output Nothing)) :: Either CodecError Int of
          Right n -> pure n
          Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the fork to run, got: " <> show other))
    count <- readTVarIO attempts
    pure (forkedText, decoded, count)

-- | A fork takes the id it is given. Returns the fork's id, the queue its row
-- carries, and the fork's decoded result.
scenarioForkTakesIdAndQueue :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                            => MgmtFixture m ->
                               m (Text, Maybe Text, Int)
scenarioForkTakesIdAndQueue fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "placed"
        sourceText = "hs-l2-mgmt-fork-placed-src-" <> suffix
        forkedText = "hs-l2-mgmt-fork-placed-" <> suffix
        sourceId = WorkflowId sourceText
    refE <- registerWorkflowRef dbos key plusOneBody
    case refE of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    exec <- fx.mfLaunch dbos
    _ <- runWf exec key sourceId (Just (encodeWorkflowValue (3 :: Int)))
    forked <-
      forkWorkflows
        dbos
        [(forkNew sourceText) {forkForkedId = Just forkedText}]
        defaultForkOptions
    case forked of
      Left err -> throwIO (userError (show err))
      Right [WorkflowId text] | text == forkedText -> pure ()
      other -> throwIO (userError ("expected the chosen fork id, got: " <> show other))
    drove <- dequeueWorkflows dbos
    case drove of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    waited <- waitForWorkflow dbos (WorkflowId forkedText)
    decoded <- case waited of
      Right (SystemDB.AwaitedSucceeded (Just output) _) ->
        case decodeWorkflowValue "result" (Just (SystemDB.SerializedWorkflowValue output Nothing)) :: Either CodecError Int of
          Right n -> pure n
          Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the placed fork to run, got: " <> show other))
    row <- fx.mfReadRow (WorkflowId forkedText)
    pure (forkedText, row.workflowRecordQueueName, decoded)

-- | Forking from a chosen step replays the steps below it: the fork runs only
-- the steps at or above the fork point. Returns the fork's step names.
scenarioForkFromStep :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                     => MgmtFixture m ->
                        m [Text]
scenarioForkFromStep fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    ran <- newTVarIO ([] :: [Text])
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "staged"
        sourceText = "hs-l2-mgmt-fork-step-src-" <> suffix
        sourceId = WorkflowId sourceText
    refE <- registerWorkflowRef dbos key (stagedBody ran)
    case refE of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    exec <- fx.mfLaunch dbos
    first <- runWf exec key sourceId (Just (encodeWorkflowValue (0 :: Int)))
    case first of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    below <- readTVarIO ran
    atomically (writeTVar ran [])
    forked <- forkFrom dbos [sourceId] (ForkStep 1) defaultForkOptions
    forkedText <- case forked of
      Left err -> throwIO (userError (show err))
      Right [WorkflowId text] -> pure text
      other -> throwIO (userError ("expected exactly one fork, got: " <> show other))
    drove <- dequeueWorkflows dbos
    case drove of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    waited <- waitForWorkflow dbos (WorkflowId forkedText)
    case waited of
      Right (SystemDB.AwaitedSucceeded _ _) -> pure ()
      other -> throwIO (userError ("expected the fork to run, got: " <> show other))
    after <- readTVarIO ran
    pure (below <> after)

-- | Forking from the last failure restarts at the failed step. The source
-- fails workflow-level before running step two, so no step recorded the
-- failure and both stacks fall back to the last recorded step (step zero):
-- the fork re-runs every step. Returns the source's first-failure step, the
-- fork's id, and the fork's decoded result.
scenarioForkFromFailure :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                        => MgmtFixture m ->
                           m (Text, Text, Int)
scenarioForkFromFailure fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    calls <- newTVarIO (0 :: Int)
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "flaky"
        sourceText = "hs-l2-mgmt-fork-fail-src-" <> suffix
        sourceId = WorkflowId sourceText
        flakyBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        flakyBody _ wctx = do
          first <- runStep wctx "one" (const (pure (0 :: Int)))
          case first of
            Left err -> pure (Left err)
            Right _ -> do
              attempt <- readTVarIO calls
              atomically (modifyTVar calls (+ 1))
              if attempt < 1
                then pure (Left (StepFailed "two" "boom"))
                else runStep wctx "two" (const (pure 99))
    refE <- registerWorkflowRef dbos key flakyBody
    case refE of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    exec <- fx.mfLaunch dbos
    first <- runWf exec key sourceId (Just (encodeWorkflowValue (0 :: Int)))
    failureStep <- case first of
      Left (StepFailed step _) -> pure step
      other -> throwIO (userError ("expected the step failure, got: " <> show other))
    forked <- forkFrom dbos [sourceId] ForkLastFailure defaultForkOptions
    forkedText <- case forked of
      Left err -> throwIO (userError (show err))
      Right [WorkflowId text] -> pure text
      other -> throwIO (userError ("expected exactly one fork, got: " <> show other))
    drove <- dequeueWorkflows dbos
    case drove of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    waited <- waitForWorkflow dbos (WorkflowId forkedText)
    decoded <- case waited of
      Right (SystemDB.AwaitedSucceeded (Just output) _) ->
        case decodeWorkflowValue "result" (Just (SystemDB.SerializedWorkflowValue output Nothing)) :: Either CodecError Int of
          Right n -> pure n
          Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the fork to recover, got: " <> show other))
    pure (failureStep, forkedText, decoded)

-- | The fork is a fresh id, ran the source's workflow again, and succeeded on
-- the second attempt.
checkForkFromBeginning :: (Text, Int, Int) -> Either String ()
checkForkFromBeginning (forkedText, decoded, count) = do
  checkEq 8 decoded
  checkEq 2 count
  checkEq True (not (Text.null forkedText))

-- | The fork carries the id it was given, restarts on the internal queue, and
-- runs the workflow's body.
checkForkTakesIdAndQueue :: (Text, Maybe Text, Int) -> Either String ()
checkForkTakesIdAndQueue (forkedText, queue, decoded) = do
  checkEq True (not (Text.null forkedText))
  checkEq (Just "_dbos_internal_queue") queue
  checkEq 4 decoded

-- | Bulk cancel and resume hand back every id, in any order: two queued runs
-- are cancelled and resumed, and both ids come back from each call. Returns
-- the two source texts with the cancelled and resumed ids.
scenarioBulkCancelResume :: forall m. (MonadFork m, MonadMVar m, MonadSTM m, MonadThrow m)
                         => MgmtFixture m ->
                            m (Text, Text, [Text], [Text])
scenarioBulkCancelResume fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "queued"
        firstText = "hs-l2-mgmt-bulk-1-" <> suffix
        secondText = "hs-l2-mgmt-bulk-2-" <> suffix
        first = WorkflowId firstText
        second = WorkflowId secondText
    regE <- registerWorkflow dbos key echoTextBody
    case regE of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    _ <- fx.mfLaunch dbos
    let enqueueOne wid = do
          enqueued <- enqueueWorkflow dbos key wid (Just (encodeWorkflowValue ("hello" :: Text))) ("bulk-" <> suffix)
          case enqueued of
            Left err -> throwIO (userError (show err))
            Right _ -> pure ()
    enqueueOne first
    enqueueOne second
    cancelled <- cancelWorkflows dbos [first, second] False
    cancelledIds <- case cancelled of
      Left err -> throwIO (userError (show err))
      Right ids -> pure [t | WorkflowId t <- ids]
    resumed <- resumeWorkflows dbos [first, second] Nothing
    resumedIds <- case resumed of
      Left err -> throwIO (userError (show err))
      Right ids -> pure [t | WorkflowId t <- ids]
    pure (firstText, secondText, cancelledIds, resumedIds)

-- | Bulk forking hands back one new id per source, in order: both sources run,
-- both fork, one drive runs both forks, and the i-th fork replays the i-th
-- source's input. Returns the source texts with the forked ids and decoded
-- results, in order.
scenarioBulkFork :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                 => MgmtFixture m ->
                    m (Text, Text, [Text], [Int])
scenarioBulkFork fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "doubling"
        firstText = "hs-l2-mgmt-bulk-fork-1-" <> suffix
        secondText = "hs-l2-mgmt-bulk-fork-2-" <> suffix
        first = WorkflowId firstText
        second = WorkflowId secondText
    regE <- registerWorkflow dbos key doublingBody
    case regE of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    exec <- fx.mfLaunch dbos
    let runOne wid input = do
          ran <- runWf exec key wid (Just (encodeWorkflowValue input))
          case ran of
            Right _ -> pure ()
            other -> throwIO (userError ("expected the source to run, got: " <> show other))
    runOne first (9 :: Int)
    runOne second (10 :: Int)
    forked <- forkWorkflows dbos [forkNew firstText, forkNew secondText] defaultForkOptions
    forkedTexts <- case forked of
      Left err -> throwIO (userError (show err))
      Right ids -> pure [t | WorkflowId t <- ids]
    drove <- dequeueWorkflows dbos
    case drove of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    let awaitOne (WorkflowId text) = do
          waited <- waitForWorkflow dbos (WorkflowId text)
          case waited of
            Right (SystemDB.AwaitedSucceeded (Just output) _) ->
              case decodeWorkflowValue "result" (Just (SystemDB.SerializedWorkflowValue output Nothing)) :: Either CodecError Int of
                Right n -> pure n
                Left err -> throwIO (userError (show err))
            other -> throwIO (userError ("expected the fork to run, got: " <> show other))
    decoded <- traverse awaitOne [WorkflowId t | t <- forkedTexts]
    pure (firstText, secondText, forkedTexts, decoded)

-- | A fork onto a partitioned queue carries the key it is given. Returns the
-- partition key on the fork's row.
scenarioForkPartitioned :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                        => MgmtFixture m ->
                           m (Maybe Text)
scenarioForkPartitioned fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "queued"
        sourceId = WorkflowId ("hs-l2-mgmt-fork-key-src-" <> suffix)
    regE <- registerWorkflow dbos key echoTextBody
    case regE of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    exec <- fx.mfLaunch dbos
    _ <- runWf exec key sourceId (Just (encodeWorkflowValue ("hello" :: Text)))
    forked <-
      forkFrom
        dbos
        [sourceId]
        (ForkStep 0)
        (defaultForkOptions {forkOptionsQueuePartitionKey = Just "pk-7"})
    forkedId <- case forked of
      Left err -> throwIO (userError (show err))
      Right [forkedId] -> pure forkedId
      other -> throwIO (userError ("expected exactly one fork, got: " <> show other))
    row <- fx.mfReadRow forkedId
    pure row.workflowRecordQueuePartitionKey

-- | Both cancelled and resumed ids come back, covering both sources.
checkBulkCancelResume :: (Text, Text, [Text], [Text]) -> Either String ()
checkBulkCancelResume (firstText, secondText, cancelled, resumed) = do
  checkEq 2 (length cancelled)
  checkEq True (all (`elem` cancelled) [firstText, secondText])
  checkEq 2 (length resumed)
  checkEq True (all (`elem` resumed) [firstText, secondText])

-- | One new id per source, each distinct from the sources and from each
-- other, with the i-th fork replaying the i-th source's input.
checkBulkFork :: (Text, Text, [Text], [Int]) -> Either String ()
checkBulkFork (firstText, secondText, forked, decoded) = do
  checkEq 2 (length forked)
  checkEq True (all (`notElem` [firstText, secondText]) forked)
  checkEq True (case forked of [a, b] -> a /= b; _ -> False)
  checkEq [18, 20] decoded

-- | Invalid plain forks are refused on the engine channel before anything
-- runs: an empty source, a negative fork step, and bad options each report
-- InvalidArgument, and no fork is written. Returns the three verdicts plus
-- whether the named fork row exists afterwards: the refusal precedes any
-- write, so the explicitly named fork id must stay absent. Empty inputs
-- are an immediate success with no validation, as the oracle returns @Ok@
-- before validating options: the last two verdicts must both be empty.
scenarioForkRefusals :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                     => MgmtFixture m -> m ( Either (Error EngineOnly) [WorkflowId],
                                            Either (Error EngineOnly) [WorkflowId],
                                            Either (Error EngineOnly) [WorkflowId],
                                            Either (Error EngineOnly) [WorkflowId],
                                            Either (Error EngineOnly) [WorkflowId],
                                            Bool
                                          )
scenarioForkRefusals fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let refusedText = "hs-l2-mgmt-fork-refused-" <> suffix
        badOptions = defaultForkOptions {forkOptionsQueueName = Just ""}
    _ <- fx.mfLaunch dbos
    emptySource <- forkWorkflows dbos [Fork "" Nothing 0] defaultForkOptions
    negativeStep <- forkFrom dbos [WorkflowId "hs-l2-mgmt-fork-negative"] (ForkStep (-1)) defaultForkOptions
    emptyQueue <- forkWorkflows dbos [Fork "hs-l2-mgmt-fork-source" (Just refusedText) 0] badOptions
    emptyForks <- forkWorkflows dbos [] badOptions
    emptyIds <- forkFrom dbos [] (ForkStep (-1)) badOptions
    found <- runSystemDB fx.mfSystemDB (\db -> SystemDB.getWorkflow db (WorkflowId refusedText))
    let written = case found of
          Right (Just _) -> True
          _ -> False
    pure (emptySource, negativeStep, emptyQueue, emptyForks, emptyIds, written)

-- | Each refusal names its argument on the engine channel, empty inputs
-- succeed without validation, and the named fork was never written.
checkForkRefusals :: ( Either (Error EngineOnly) [WorkflowId],
                       Either (Error EngineOnly) [WorkflowId],
                       Either (Error EngineOnly) [WorkflowId],
                       Either (Error EngineOnly) [WorkflowId],
                       Either (Error EngineOnly) [WorkflowId],
                       Bool
                     ) -> Either String ()
checkForkRefusals (emptySource, negativeStep, emptyQueue, emptyForks, emptyIds, written) = do
  case emptySource of
    Left (InvalidArgument operation detail) -> do
      checkEq "fork workflows" operation
      checkEq "source_id must not be empty" detail
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  case negativeStep of
    Left (InvalidArgument operation detail) -> do
      checkEq "fork from workflows" operation
      checkEq "start_step must not be negative" detail
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  case emptyQueue of
    Left (InvalidArgument operation detail) -> do
      checkEq "fork workflows" operation
      checkEq "queue_name must be absent rather than empty" detail
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  checkEq (Right []) emptyForks
  checkEq (Right []) emptyIds
  checkEq False written

-- | The fork's row carries the partition key it was given.
checkForkPartitioned :: Maybe Text -> Either String ()
checkForkPartitioned = checkEq (Just "pk-7")

-- | Attributes are replaced and can be searched: a full replacement matches a
-- one-key filter by containment, a narrower replacement drops the unseen key,
-- and clearing removes them. Returns the matching ids at each search and the
-- cleared row's attributes.
scenarioAttributes :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                   => MgmtFixture m ->
                      m (Text, [Text], [Text], Maybe Text)
scenarioAttributes fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "tagged"
        widText = "hs-l2-mgmt-attributes-" <> suffix
        wid = WorkflowId widText
        tenant = "acme-" <> suffix
        full = "{\"tenant\":\"" <> tenant <> "\",\"tier\":\"gold\"}"
        tenantOnly = "{\"tenant\":\"" <> tenant <> "\"}"
        tierOnly = "{\"tier\":\"gold\"}"
    regE <- registerWorkflow dbos key zeroBody
    case regE of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    exec <- fx.mfLaunch dbos
    _ <- runWf exec key wid Nothing
    updated <- updateWorkflowAttributes dbos wid (Just full)
    case updated of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    -- Containment, not equality: one key out of two matches.
    found <- listWorkflows dbos (defaultWorkflowFilter {workflowFilterAttributes = Just tenantOnly})
    foundIds <- case found of
      Left err -> throwIO (userError (show err))
      Right rows -> pure [t | WorkflowId t <- map (.workflowRecordId) rows]
    -- A replacement, not a merge: the key not sent again is gone.
    fewer <- updateWorkflowAttributes dbos wid (Just tenantOnly)
    case fewer of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    afterReplacement <- listWorkflows dbos (defaultWorkflowFilter {workflowFilterAttributes = Just tierOnly})
    afterIds <- case afterReplacement of
      Left err -> throwIO (userError (show err))
      Right rows -> pure [t | WorkflowId t <- map (.workflowRecordId) rows]
    -- And Nothing clears them.
    cleared <- updateWorkflowAttributes dbos wid Nothing
    case cleared of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    row <- fx.mfReadRow wid
    pure (widText, foundIds, afterIds, row.workflowRecordAttributes)

-- | The full replacement matches by containment, the narrower replacement
-- drops the tier, and clearing removes the attributes.
checkAttributes :: (Text, [Text], [Text], Maybe Text) -> Either String ()
checkAttributes (widText, found, after, cleared) = do
  checkEq [widText] found
  checkEq [] after
  checkEq Nothing cleared

-- | A workflow replacing another's attributes as its step: the target row
-- carries the replacement, and the caller's move took its framework step.
-- Returns the target's stored attributes, the attributes after clearing,
-- and the caller's recorded step names. The clearing leaves no searchable
-- residue: attribute searches are unscoped, so a standing row would leak
-- into other cases sharing the database.
scenarioAttributesInWorkflow :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                             => MgmtFixture m -> m (Maybe Text, Maybe Text, [Text])
scenarioAttributesInWorkflow fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let targetKey = newWorkflowKey "tagged-target"
        callerKey = newWorkflowKey "attributes-caller"
        targetText = "hs-l2-mgmt-attributes-target-" <> suffix
        callerText = "hs-l2-mgmt-attributes-caller-" <> suffix
        replacement = "{\"tenant\":\"acme-" <> suffix <> "\",\"tier\":\"gold\"}"
        callerBody :: forall exec. Text -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        callerBody target wctx = do
          replaced <- updateWorkflowAttributesInWorkflow wctx (WorkflowId target) (Just replacement)
          case replaced of
            Left err -> pure (Left err)
            Right () -> pure (Right target)
    targetRegistered <- registerWorkflow dbos targetKey zeroBody
    case targetRegistered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    callerRegistered <- registerWorkflow dbos callerKey callerBody
    case callerRegistered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ranTarget <- runWf exec targetKey (WorkflowId targetText) (Just (encodeWorkflowValue ()))
    case ranTarget of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    ranCaller <- runWf exec callerKey (WorkflowId callerText) (Just (encodeWorkflowValue targetText))
    decoded <- case ranCaller of
      Left err -> throwIO (userError (show err))
      Right Nothing -> throwIO (userError "expected the caller result")
      Right (Just output) ->
        case decodeWorkflowValue "result" (Just output) :: Either CodecError Text of
          Right value -> pure value
          Left err -> throwIO (userError (show err))
    unless (decoded == targetText) $ throwIO (userError ("expected the target id back, got: " <> show decoded))
    row <- fx.mfReadRow (WorkflowId targetText)
    listed <- listStepsWf dbos (WorkflowId callerText)
    names <- case listed of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepRecordStepName | record <- records]
    cleared <- updateWorkflowAttributes dbos (WorkflowId targetText) Nothing
    case cleared of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    clearedRow <- fx.mfReadRow (WorkflowId targetText)
    pure (row.workflowRecordAttributes, clearedRow.workflowRecordAttributes, names)

-- | The target carried the replacement, clearing removed it, and the
-- caller's move took its framework step.
checkAttributesInWorkflow :: (Maybe Text, Maybe Text, [Text]) -> Either String ()
checkAttributesInWorkflow (stored, cleared, names) = do
  -- The backend re-serializes the object, so the check is containment of
  -- both keys rather than the input's exact bytes.
  case stored of
    Just attributes
      | "tenant" `Text.isInfixOf` attributes && "gold" `Text.isInfixOf` attributes -> pure ()
      | otherwise -> Left ("expected the replacement attributes, got: " <> show stored)
    Nothing -> Left "expected the replacement attributes, got nothing"
  checkEq Nothing cleared
  checkEq ["DBOS.update_workflow_attributes"] names

-- | The workflow waited as delayed, then ran its body on release.
checkDelayRelease :: (Maybe WorkflowStatus, Int) -> Either String ()
checkDelayRelease = checkEq (Just Delayed, 0)

-- * Rewind

-- | Two steps and a run counter: the rewind target that proves steps at
-- and past the cut run again.
rewindCountBody :: forall exec m. (MonadSTM m, MonadTime m, MonadCatch m)
                => StrictTVar m Int ->
                   Int ->
                   WorkflowCtx exec m ->
                   m (Either (Error EngineOnly) Int)
rewindCountBody ran input wctx = do
  first <- runStep wctx "first" (const (pure input))
  case first of
    Left err -> pure (Left err)
    Right value -> do
      second <- runStep wctx "second" (const (pure (value + 1)))
      case second of
        Left err -> pure (Left err)
        Right done -> atomically (modifyTVar ran (+ 1)) >> pure (Right done)

-- | Sends, receives, publishes, and streams across three steps, addressed
-- to its own id (the input): the rewind target that proves every undo
-- branch. Step ids are fixed by the calls: send at 0, recv at 1 (timeout
-- at 2), publish at 3, send at 4, recv at 5 (timeout at 6), publish at 7,
-- stream writes at 8 and 9, a parked send at 10, the close at 11.
rewindUndoBody :: forall exec m. (MonadSTM m, MonadTime m, MonadDelay m)
               => Text ->
                  WorkflowCtx exec m ->
                  m (Either (Error EngineOnly) Text)
rewindUndoBody widText wctx = do
  let wid = WorkflowId widText
      topic = Topic "undo"
      expect step expected actual = pure $ case actual of
        Right (Just value) | value == expected -> Right ()
        _ -> Left (StepFailed ("expect-" <> Text.pack (show step)) ("took " <> Text.pack (show actual)))
  sent0 <- send wctx wid (Just topic) Nothing ("m0" :: Text)
  case sent0 of
    Left err -> pure (Left err)
    Right () -> do
      took0 <- recv wctx (Just topic) (millisDuration 5000) :: m (Either (Error EngineOnly) (Maybe Text))
      outcome0 <- expect (0 :: Int) "m0" took0
      case outcome0 of
        Left err -> pure (Left err)
        Right () -> do
          published0 <- setEvent wctx "k" ("v0" :: Text)
          case published0 of
            Left err -> pure (Left err)
            Right () -> do
              sent1 <- send wctx wid (Just topic) Nothing ("m1" :: Text)
              case sent1 of
                Left err -> pure (Left err)
                Right () -> do
                  took1 <- recv wctx (Just topic) (millisDuration 5000) :: m (Either (Error EngineOnly) (Maybe Text))
                  outcome1 <- expect (1 :: Int) "m1" took1
                  case outcome1 of
                    Left err -> pure (Left err)
                    Right () -> do
                      published1 <- setEvent wctx "k" ("v1" :: Text)
                      case published1 of
                        Left err -> pure (Left err)
                        Right () -> do
                          wroteA <- writeStream wctx "s" ("a" :: Text)
                          case wroteA of
                            Left err -> pure (Left err)
                            Right () -> do
                              wroteB <- writeStream wctx "s" ("b" :: Text)
                              case wroteB of
                                Left err -> pure (Left err)
                                Right () -> do
                                  sent2 <- send wctx wid (Just topic) Nothing ("m2" :: Text)
                                  case sent2 of
                                    Left err -> pure (Left err)
                                    Right () -> do
                                      closed <- closeStream wctx "s"
                                      case closed of
                                        Left err -> pure (Left err)
                                        Right () -> pure (Right "done")

-- | The datasource transaction a rewind drops checkpoints for.
rewindTxBody :: forall exec m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, HasCallStack)
             => FakeDs m ->
                () ->
                WorkflowCtx exec m ->
                m (Either (Error EngineOnly) Text)
rewindTxBody fake () wctx = do
  outcome <- runTxStep fake.fakeSource protoConfig wctx (\_ _ -> pure (Right ("v" :: Text)))
  case outcome of
    Left err -> pure (Left err)
    Right _ -> pure (Right "done")

-- | A transaction that reports its checkpoint count mid-run: the count
-- the completion clearing must later remove.
txCountBody :: forall exec m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, HasCallStack)
            => FakeDs m ->
               StrictTVar m Int ->
               () ->
               WorkflowCtx exec m ->
               m (Either (Error EngineOnly) Text)
txCountBody fake countVar () wctx = do
  outcome <- runTxStep fake.fakeSource protoConfig wctx (\_ _ -> pure (Right ("v" :: Text)))
  case outcome of
    Left err -> pure (Left err)
    Right _ -> do
      n <- Map.size <$> readTVarIO fake.fakeRows
      atomically (writeTVar countVar n)
      pure (Right "done")

-- | The adoption child: counts its runs and doubles its input.
adoptChildBody :: forall exec m. (MonadSTM m)
               => StrictTVar m Int ->
                  Int ->
                  WorkflowCtx exec m ->
                  m (Either (Error EngineOnly) Int)
adoptChildBody ran input _ = atomically (modifyTVar ran (+ 1)) >> pure (Right (input * 2))

-- | The adoption parent: counts its runs, starts the child, and awaits
-- its result.
adoptParentBody :: forall exec m. (MonadTime m, MonadMVar m, MonadCatch m, MonadTimer m)
                => WorkflowRef m EngineOnly ->
                   StrictTVar m Int ->
                   () ->
                   WorkflowCtx exec m ->
                   m (Either (Error EngineOnly) Int)
adoptParentBody childRef ran () wctx = do
  atomically (modifyTVar ran (+ 1))
  started <- startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowValue (21 :: Int)))
  case started of
    Left err -> pure (Left err)
    Right child -> do
      awaited <- handleResult child
      case awaited of
        Left err -> pure (Left err)
        Right (Just stored) ->
          pure $ case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
            Right value -> Right value
            Left err -> Left (StepFailed "parent" (Text.pack (show err)))
        Right Nothing -> pure (Left (StepFailed "parent" "no child output"))

-- | A refused rewind inside a workflow records nothing: the body hands
-- back the refusal's field.
rewindRefusedBody :: forall exec m. (MonadAsync m, MonadDelay m, MonadTime m, MonadCatch m)
                  => Text ->
                     WorkflowCtx exec m ->
                     m (Either (Error EngineOnly) Text)
rewindRefusedBody target wctx = do
  rewound <- rewindWorkflowsInWorkflow wctx (WorkflowId target) (defaultRewindOptions {rewindStartStep = -3})
  case rewound of
    Left (InvalidArgument _ detail) -> pure (Right detail)
    other -> pure (Left (StepFailed "rewind" (Text.pack (show other))))

-- | A finished workflow rewound to its second step runs again under the
-- same id: the row is re-enqueued with its counters cleared, the later
-- step is gone, and the driven pass re-executes it. Returns the row
-- status, queue, attempts, output, remaining step ids, the re-run output,
-- and the run count.
scenarioRewindRunsAgain :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                        => MgmtFixture m ->
                           m (WorkflowStatus, Maybe Text, Int64, Maybe Text, [Int], Int, Int)
scenarioRewindRunsAgain fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    ran <- newTVarIO (0 :: Int)
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "rewindable"
        wid = WorkflowId ("hs-l2-mgmt-rewind-runs-" <> suffix)
    registered <- registerWorkflow dbos key (rewindCountBody ran)
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    first <- runWf exec key wid (Just (encodeWorkflowValue (0 :: Int)))
    case first of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    rewound <- rewindWorkflows dbos wid (defaultRewindOptions {rewindStartStep = 1})
    case rewound of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    row <- fx.mfReadRow wid
    listed <- listStepsWf dbos wid
    stepIds <- case listed of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepRecordStepId | record <- records]
    drove <- dequeueWorkflows dbos
    case drove of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    waited <- waitForWorkflow dbos wid
    decoded <- case waited of
      Right (SystemDB.AwaitedSucceeded (Just output) _) ->
        case decodeWorkflowValue "result" (Just (SystemDB.SerializedWorkflowValue output Nothing)) :: Either CodecError Int of
          Right n -> pure n
          Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the rewound run to finish, got: " <> show other))
    after <- readTVarIO ran
    pure
      ( row.workflowRecordStatus,
        row.workflowRecordQueueName,
        row.workflowRecordRecoveryAttempts,
        row.workflowRecordOutput,
        stepIds,
        decoded,
        after
      )

-- | A delayed workflow can be released sooner: started an hour out it waits
-- as delayed; brought forward to now, the driven pass transitions and runs
-- it. Returns the waiting status and the decoded result.
scenarioDelayRelease :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                     => MgmtFixture m ->
                        m (Maybe WorkflowStatus, Int)
scenarioDelayRelease fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "delayable"
        wid = WorkflowId ("hs-l2-mgmt-delayed-" <> suffix)
        QueueName internal = internalQueueName
    refE <- registerWorkflowRef dbos key zeroBody
    ref <- case refE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.mfLaunch dbos
    started <-
      startWfRef
        exec
        ref
        (startOptionsDefault {startWorkflowId = Just wid, startQueue = Just ((enqueueNew internal) {delay = Just (secondsDuration 3600)})})
        Nothing
    case started of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    waiting <- fx.mfReadRow wid
    released <- setWorkflowDelay dbos wid (DelayFor (secondsDuration 0))
    case released of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    drove <- dequeueWorkflows dbos
    case drove of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    waited <- waitForWorkflow dbos wid
    decoded <- case waited of
      Right (SystemDB.AwaitedSucceeded (Just output) _) ->
        case decodeWorkflowValue "result" (Just (SystemDB.SerializedWorkflowValue output Nothing)) :: Either CodecError Int of
          Right n -> pure n
          Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the released run, got: " <> show other))
    pure (Just waiting.workflowRecordStatus, decoded)

-- | A workflow moving another's delay as its step releases the target: the
-- delayed start waits, the caller's step brings it forward, and the driven
-- pass transitions and runs it. Returns the caller's verdict, the target's
-- decoded output, and the caller's recorded step names.
scenarioDelayReleaseInWorkflow :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                               => MgmtFixture m -> m (Text, Int, [Text])
scenarioDelayReleaseInWorkflow fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let targetKey = newWorkflowKey "delayable-target"
        callerKey = newWorkflowKey "delay-caller"
        targetText = "hs-l2-mgmt-delay-target-" <> suffix
        callerText = "hs-l2-mgmt-delay-caller-" <> suffix
        QueueName internal = internalQueueName
        callerBody :: forall exec. Text -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        callerBody target wctx = do
          moved <- setWorkflowDelayInWorkflow wctx (WorkflowId target) (DelayFor (secondsDuration 0))
          case moved of
            Left err -> pure (Left err)
            Right () -> pure (Right target)
    targetRefE <- registerWorkflowRef dbos targetKey zeroBody
    targetRef <- case targetRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    callerRegistered <- registerWorkflow dbos callerKey callerBody
    case callerRegistered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    started <-
      startWfRef
        exec
        targetRef
        (startOptionsDefault {startWorkflowId = Just (WorkflowId targetText), startQueue = Just ((enqueueNew internal) {delay = Just (secondsDuration 3600)})})
        Nothing
    case started of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    ranCaller <- runWf exec callerKey (WorkflowId callerText) (Just (encodeWorkflowValue targetText))
    decoded <- case ranCaller of
      Left err -> throwIO (userError (show err))
      Right Nothing -> throwIO (userError "expected the caller result")
      Right (Just output) ->
        case decodeWorkflowValue "result" (Just output) :: Either CodecError Text of
          Right value -> pure value
          Left err -> throwIO (userError (show err))
    drove <- dequeueWorkflows dbos
    case drove of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    waited <- waitForWorkflow dbos (WorkflowId targetText)
    released <- case waited of
      Right (SystemDB.AwaitedSucceeded (Just output) _) ->
        case decodeWorkflowValue "result" (Just (SystemDB.SerializedWorkflowValue output Nothing)) :: Either CodecError Int of
          Right n -> pure n
          Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the released run, got: " <> show other))
    listed <- listStepsWf dbos (WorkflowId callerText)
    names <- case listed of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepRecordStepName | record <- records]
    pure (decoded, released, names)

-- | The caller hands back the target id, the target ran, and the caller's
-- move took its framework step.
checkDelayReleaseInWorkflow :: (Text, Int, [Text]) -> Either String ()
checkDelayReleaseInWorkflow (decoded, released, names) = do
  checkEq True ("hs-l2-mgmt-delay-target-" `Text.isPrefixOf` decoded)
  checkEq 0 released
  checkEq ["DBOS.set_workflow_delay"] names

-- | The fork ran only the steps at or above the fork point; the steps below
-- replayed from the copied history.
checkForkFromStep :: [Text] -> Either String ()
checkForkFromStep names =
  checkEq ["one", "two", "three", "two", "three"] names

-- | The fork restarted at the failed step and recovered.
checkForkFromFailure :: (Text, Text, Int) -> Either String ()
checkForkFromFailure (failureStep, forkedText, decoded) = do
  checkEq "two" failureStep
  checkEq 99 decoded
  checkEq True (not (Text.null forkedText))

-- | The text under a 'WorkflowId'.
widTextOf :: WorkflowId -> Text
widTextOf (WorkflowId widText) = widText

-- | Pure verdicts; both trees judge through these.
checkEq :: (Eq a, Show a) => a -> a -> Either String ()
checkEq expected actual
  | expected == actual = Right ()
  | otherwise = Left ("expected " <> show expected <> ", got " <> show actual)

-- | The finished run rewound to its second step runs again: the check
-- judges the reset row, the surviving step, and the re-run.
checkRewindRunsAgain :: (WorkflowStatus, Maybe Text, Int64, Maybe Text, [Int], Int, Int) -> Either String ()
checkRewindRunsAgain (status, queue, attempts, output, stepIds, decoded, after) = do
  checkEq Enqueued status
  let QueueName internal = internalQueueName
  checkEq (Just internal) queue
  checkEq 0 attempts
  checkEq Nothing output
  checkEq [0] stepIds
  checkEq 1 decoded
  checkEq 2 after

-- | Refusals name their cause and write nothing: a missing id, a negative
-- cut, an empty queue name, and a workflow still enqueued. Returns the
-- four verdicts with the active row's status and steps afterwards.
scenarioRewindRefusals :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                       => MgmtFixture m ->
                          m ( Either (Error EngineOnly) (),
                              Either (Error EngineOnly) (),
                              Either (Error EngineOnly) (),
                              Either (Error EngineOnly) (),
                              WorkflowStatus,
                              [Int]
                            )
scenarioRewindRefusals fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "rewindable"
        refKey = newWorkflowKey "rewindable-ref"
        finished = WorkflowId ("hs-l2-mgmt-rewind-done-" <> suffix)
        active = WorkflowId ("hs-l2-mgmt-rewind-active-" <> suffix)
    registered <- registerWorkflow dbos key zeroBody
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    refE <- registerWorkflowRef dbos refKey zeroBody
    ref <- case refE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key finished (Just (encodeWorkflowValue ()))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    missing <- rewindWorkflows dbos (WorkflowId "never-existed") (defaultRewindOptions {rewindStartStep = 0})
    negative <- rewindWorkflows dbos finished (defaultRewindOptions {rewindStartStep = -1})
    emptyQueue <- rewindWorkflows dbos finished (defaultRewindOptions {rewindQueueName = Just ""})
    started <-
      startWfRef
        exec
        ref
        (startOptionsDefault {startWorkflowId = Just active, startQueue = Just (enqueueNew "no-runner-here")})
        (Just (encodeWorkflowValue ()))
    case started of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    refused <- rewindWorkflows dbos active (defaultRewindOptions {rewindStartStep = 0})
    row <- fx.mfReadRow active
    listed <- listStepsWf dbos active
    stepIds <- case listed of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepRecordStepId | record <- records]
    pure (missing, negative, emptyQueue, refused, row.workflowRecordStatus, stepIds)

-- | Every refusal names its cause; the active row is untouched.
checkRewindRefusals :: ( Either (Error EngineOnly) (),
                         Either (Error EngineOnly) (),
                         Either (Error EngineOnly) (),
                         Either (Error EngineOnly) (),
                         WorkflowStatus,
                         [Int]
                       ) -> Either String ()
checkRewindRefusals (missing, negative, emptyQueue, refused, statusAfter, stepsAfter) = do
  case missing of
    Left (SystemDatabase SystemDB.NonExistentWorkflow {}) -> pure ()
    other -> Left ("expected a missing workflow refusal, got: " <> show other)
  case negative of
    Left (InvalidArgument op detail) -> do
      checkEq "rewind a workflow" op
      checkEq "start_step must not be negative, got -1" detail
    other -> Left ("expected a negative-cut refusal, got: " <> show other)
  case emptyQueue of
    Left (InvalidArgument op detail) -> do
      checkEq "rewind a workflow" op
      checkEq "queue_name must be absent rather than empty" detail
    other -> Left ("expected an empty-queue refusal, got: " <> show other)
  case refused of
    Left (SystemDatabase (SystemDB.WorkflowNotRewindable {status})) -> checkEq "ENQUEUED" status
    other -> Left ("expected an active-workflow refusal, got: " <> show other)
  checkEq Enqueued statusAfter
  checkEq [] stepsAfter

-- | Runs the undo fixture and rewinds it at the given cut, reading back
-- every undone surface: the surviving notification messages, the current
-- event value for the published key, the stream values in order, the
-- surviving step ids, and the row status.
undoAt :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
       => MgmtFixture m ->
          Int ->
          m ([Text], Maybe Text, [Text], [Int], WorkflowStatus)
undoAt fx cut = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "rewind-undo"
        widText = "hs-l2-mgmt-rewind-undo-" <> suffix
        wid = WorkflowId widText
    registered <- registerWorkflow dbos key rewindUndoBody
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowValue widText))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    rewound <- rewindWorkflows dbos wid (defaultRewindOptions {rewindStartStep = cut})
    case rewound of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    notified <- runSystemDB fx.mfSystemDB (\db -> SystemDB.getAllNotifications db wid)
    messages <- case notified of
      Left err -> throwIO (userError (show err))
      Right rows -> pure [row.notificationRecordMessage | row <- rows]
    published <- runSystemDB fx.mfSystemDB (\db -> SystemDB.getAllEvents db wid)
    eventHit <- case published of
      Left err -> throwIO (userError (show err))
      Right rows -> pure (lookup "k" [(row.eventKey, row.eventValue) | row <- rows])
    entries <- runSystemDB fx.mfSystemDB (\db -> SystemDB.getAllStreamEntries db wid)
    values <- case entries of
      Left err -> throwIO (userError (show err))
      Right rows -> pure [row.streamValue | row <- rows]
    listed <- listStepsWf dbos wid
    stepIds <- case listed of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepRecordStepId | record <- records]
    row <- fx.mfReadRow wid
    pure (messages, eventHit, values, stepIds, row.workflowRecordStatus)

-- | Rewound at the first step, everything the run did is undone: no
-- message survives, the published key is gone, the close is lifted while
-- the entries stay, and only the first step stands.
scenarioRewindUndoCut :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                      => MgmtFixture m -> m ([Text], Maybe Text, [Text], [Int], WorkflowStatus)
scenarioRewindUndoCut fx = undoAt fx 1

-- | The cut wipes the run's messages, restores the key, lifts the close,
-- and keeps the first step.
checkRewindUndoCut :: ([Text], Maybe Text, [Text], [Int], WorkflowStatus) -> Either String ()
checkRewindUndoCut (messages, eventHit, values, stepIds, status) = do
  checkEq [] messages
  checkEq Nothing eventHit
  checkEq (map (.serializedText) [encodeWorkflowValue ("a" :: Text), encodeWorkflowValue ("b" :: Text)]) values
  checkEq [0] stepIds
  checkEq Enqueued status

-- | Rewound mid-run, what fell below the cut survives: the message the
-- first recv took stays queued, the key restores to its first value, and
-- the later message and the parked send are gone.
scenarioRewindKeepBelowCut :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                           => MgmtFixture m -> m ([Text], Maybe Text, [Text], [Int], WorkflowStatus)
scenarioRewindKeepBelowCut fx = undoAt fx 5

-- | The kept message is the first one, the key holds its first value, the
-- entries stand without their close, and only pre-cut steps stand. Step
-- membership is asserted as a bound, not an exact list: a recv's timeout
-- step is recorded on every stack live but only in-workflow under sim, a
-- pre-existing engine divergence outside this slice.
checkRewindKeepBelowCut :: ([Text], Maybe Text, [Text], [Int], WorkflowStatus) -> Either String ()
checkRewindKeepBelowCut (messages, eventHit, values, stepIds, status) = do
  checkEq [(encodeWorkflowValue ("m0" :: Text)).serializedText] messages
  checkEq (Just (encodeWorkflowValue ("v0" :: Text)).serializedText) eventHit
  checkEq (map (.serializedText) [encodeWorkflowValue ("a" :: Text), encodeWorkflowValue ("b" :: Text)]) values
  checkEq True (0 `elem` stepIds)
  checkEq True (all (< 5) stepIds)
  checkEq Enqueued status

-- | The rewind names the queue and version the re-run gets: the row
-- carries them, the counters clear, and the history from the cut is gone.
scenarioRewindQueueVersion :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                           => MgmtFixture m -> m (WorkflowStatus, Maybe Text, Maybe Text, Int64, [Int])
scenarioRewindQueueVersion fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "rewindable-qv"
        wid = WorkflowId ("hs-l2-mgmt-rewind-qv-" <> suffix)
    registered <- registerWorkflow dbos key zeroBody
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowValue ()))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    rewound <- rewindWorkflow dbos wid (defaultRewindOptions {rewindStartStep = 0, rewindQueueName = Just "custom-q", rewindApplicationVersion = Just "v9"})
    case rewound of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    row <- fx.mfReadRow wid
    listed <- listStepsWf dbos wid
    stepIds <- case listed of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepRecordStepId | record <- records]
    pure
      ( row.workflowRecordStatus,
        row.workflowRecordQueueName,
        row.workflowRecordApplicationVersion,
        row.workflowRecordRecoveryAttempts,
        stepIds
      )

-- | The chosen queue and version land on the row; the cut clears all steps.
checkRewindQueueVersion :: (WorkflowStatus, Maybe Text, Maybe Text, Int64, [Int]) -> Either String ()
checkRewindQueueVersion (status, queue, version, attempts, stepIds) = do
  checkEq Enqueued status
  checkEq (Just "custom-q") queue
  checkEq (Just "v9") version
  checkEq 0 attempts
  checkEq [] stepIds

-- | A workflow rewinding another as its own step: the rewind runs once
-- under the caller's checkpoint. Returns the caller's decoded output, the
-- target's status, and the caller's recorded step names.
scenarioRewindInWorkflow :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                         => MgmtFixture m -> m (Text, WorkflowStatus, [Text])
scenarioRewindInWorkflow fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let targetKey = newWorkflowKey "rewind-target"
        callerKey = newWorkflowKey "rewind-caller"
        targetText = "hs-l2-mgmt-rewind-target-" <> suffix
        callerText = "hs-l2-mgmt-rewind-caller-" <> suffix
        callerBody :: forall exec. Text -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        callerBody target wctx = do
          rewound <- rewindWorkflowsInWorkflow wctx (WorkflowId target) (defaultRewindOptions {rewindStartStep = 0})
          case rewound of
            Left err -> pure (Left err)
            Right () -> pure (Right target)
    targetRegistered <- registerWorkflow dbos targetKey zeroBody
    case targetRegistered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    callerRegistered <- registerWorkflow dbos callerKey callerBody
    case callerRegistered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ranTarget <- runWf exec targetKey (WorkflowId targetText) (Just (encodeWorkflowValue ()))
    case ranTarget of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    ranCaller <- runWf exec callerKey (WorkflowId callerText) (Just (encodeWorkflowValue targetText))
    decoded <- case ranCaller of
      Left err -> throwIO (userError (show err))
      Right Nothing -> throwIO (userError "expected the caller result")
      Right (Just output) ->
        case decodeWorkflowValue "result" (Just output) :: Either CodecError Text of
          Right value -> pure value
          Left err -> throwIO (userError (show err))
    target <- fx.mfReadRow (WorkflowId targetText)
    listed <- listStepsWf dbos (WorkflowId callerText)
    names <- case listed of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepRecordStepName | record <- records]
    pure (decoded, target.workflowRecordStatus, names)

-- | The caller hands back the target id, the target is re-enqueued, and
-- the caller's rewind took its framework step.
checkRewindInWorkflow :: (Text, WorkflowStatus, [Text]) -> Either String ()
checkRewindInWorkflow (decoded, status, names) = do
  checkEq True ("hs-l2-mgmt-rewind-target-" `Text.isPrefixOf` decoded)
  checkEq Enqueued status
  checkEq ["DBOS.rewind_workflow"] names

-- | The executor rewind drops the datasource checkpoints from the cut on:
-- the fake holds one checkpoint before the rewind and none after, while
-- the row is re-enqueued. Returns the checkpoint count before and after
-- with the row status.
scenarioRewindDropsDatasourceCheckpoints :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                                         => MgmtFixture m -> m (Int, Int, WorkflowStatus)
scenarioRewindDropsDatasourceCheckpoints fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    fake <- mkFakeDs
    registeredDs <- registerDataSource dbos fake.fakeSource
    case registeredDs of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    let key = newWorkflowKey "rewind-tx"
        wid = WorkflowId ("hs-l2-mgmt-rewind-tx-" <> suffix)
    registered <- registerWorkflow dbos key zeroBody
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowValue ()))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    wrote <- fake.fakeSource.dsRecordOutput (Tx (\_ _ -> error "rewind probe: statements unsupported")) wid "probe" 0 "x"
    case wrote of
      True -> pure ()
      False -> throwIO (userError "expected the probe checkpoint to land")
    before <- Map.size <$> readTVarIO fake.fakeRows
    rewound <- rewindWorkflows dbos wid (defaultRewindOptions {rewindStartStep = 0})
    case rewound of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    after <- Map.size <$> readTVarIO fake.fakeRows
    row <- fx.mfReadRow wid
    pure (before, after, row.workflowRecordStatus)

-- | One checkpoint stood, none stands, and the row runs again.
checkRewindDropsDatasourceCheckpoints :: (Int, Int, WorkflowStatus) -> Either String ()
checkRewindDropsDatasourceCheckpoints (before, after, status) = do
  checkEq True (before > 0)
  checkEq 0 after
  checkEq Enqueued status

-- | Every completion clears the run's datasource checkpoints, however it
-- ran: the checkpoint stands mid-run and is gone after, on the direct
-- path and on the driven (dequeue) path alike. Mirrors Python's
-- @test_sync_ds_completion_clears_checkpoints@. Returns the mid-run and
-- post-run counts for each path.
scenarioCompletionClearsCheckpoints :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, HasCallStack)
                                    => MgmtFixture m -> m (Int, Int, Int, Int)
scenarioCompletionClearsCheckpoints fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    fake <- mkFakeDs
    registeredDs <- registerDataSource dbos fake.fakeSource
    case registeredDs of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    midDirect <- newTVarIO (0 :: Int)
    midDriven <- newTVarIO (0 :: Int)
    let directKey = newWorkflowKey "clearing-direct"
        drivenRefKey = newWorkflowKey "clearing-driven-ref"
        directWid = WorkflowId ("hs-l2-mgmt-clear-direct-" <> suffix)
        drivenWid = WorkflowId ("hs-l2-mgmt-clear-driven-" <> suffix)
        QueueName internal = internalQueueName
    directRegistered <- registerWorkflow dbos directKey (txCountBody fake midDirect)
    case directRegistered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    drivenRefE <- registerWorkflowRef dbos drivenRefKey (txCountBody fake midDriven)
    drivenRef <- case drivenRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.mfLaunch dbos
    ranDirect <- runWf exec directKey directWid (Just (encodeWorkflowValue ()))
    case ranDirect of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    mid1 <- readTVarIO midDirect
    post1 <- Map.size <$> readTVarIO fake.fakeRows
    started <-
      startWfRef
        exec
        drivenRef
        (startOptionsDefault {startWorkflowId = Just drivenWid, startQueue = Just (enqueueNew internal)})
        (Just (encodeWorkflowValue ()))
    case started of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    drove <- dequeueWorkflows dbos
    case drove of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    waited <- waitForWorkflow dbos drivenWid
    case waited of
      Right (SystemDB.AwaitedSucceeded _ _) -> pure ()
      other -> throwIO (userError ("expected the driven transaction to finish, got: " <> show other))
    mid2 <- readTVarIO midDriven
    post2 <- Map.size <$> readTVarIO fake.fakeRows
    pure (mid1, post1, mid2, post2)

-- | Both paths hold one checkpoint mid-run and none after.
checkCompletionClearsCheckpoints :: (Int, Int, Int, Int) -> Either String ()
checkCompletionClearsCheckpoints (mid1, post1, mid2, post2) = do
  checkEq (1, 0, 1, 0) (mid1, post1, mid2, post2)

-- | The parent of a child that runs a datasource transaction: the parent's
-- own checkpoint stands while the child runs, so the child's lands beside
-- it and the parent observes both once the child finishes.
childClearParentBody :: forall exec m. (MonadTime m, MonadMVar m, MonadCatch m, MonadTimer m, HasCallStack)
                     => FakeDs m ->
                        WorkflowRef m EngineOnly ->
                        StrictTVar m Int ->
                        () ->
                        WorkflowCtx exec m ->
                        m (Either (Error EngineOnly) Text)
childClearParentBody fake childRef midParent () wctx = do
  outcome <- runTxStep fake.fakeSource protoConfig wctx (\_ _ -> pure (Right ("p" :: Text)))
  case outcome of
    Left err -> pure (Left err)
    Right _ -> do
      started <- startChildWorkflow wctx childRef startOptionsDefault Nothing
      case started of
        Left err -> pure (Left err)
        Right child -> do
          awaited <- handleResult child
          case awaited of
            Left err -> pure (Left err)
            Right _ -> do
              n <- Map.size <$> readTVarIO fake.fakeRows
              atomically (writeTVar midParent n)
              pure (Right "done")

-- | An inline child clears its own datasource checkpoints when it finishes,
-- exactly like a top-level run: the child's checkpoint stands mid-run next
-- to the parent's, the child's completion drops its own before the parent
-- observes, and the parent's completion drops the last. Mirrors Python's
-- @_dropping_datasource_checkpoints@ covering the child execution paths.
-- Returns the checkpoint count inside the child, inside the parent once the
-- child finished, and after the parent finished.
scenarioChildClearsCheckpoints :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m, HasCallStack)
                                => MgmtFixture m -> m (Int, Int, Int)
scenarioChildClearsCheckpoints fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    fake <- mkFakeDs
    registeredDs <- registerDataSource dbos fake.fakeSource
    case registeredDs of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    midChild <- newTVarIO (0 :: Int)
    midParent <- newTVarIO (0 :: Int)
    let childKey = newWorkflowKey "clearing-child"
        parentKey = newWorkflowKey "clearing-parent"
        parentWid = WorkflowId ("hs-l2-mgmt-clear-child-" <> suffix)
    childRefE <- registerWorkflowRef dbos childKey (txCountBody fake midChild)
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    parentRegistered <- registerWorkflow dbos parentKey (childClearParentBody fake childRef midParent)
    case parentRegistered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec parentKey parentWid (Just (encodeWorkflowValue ()))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    mid1 <- readTVarIO midChild
    mid2 <- readTVarIO midParent
    post <- Map.size <$> readTVarIO fake.fakeRows
    pure (mid1, mid2, post)

-- | Both checkpoints stand mid-child-run; the child's own is gone once it
-- finishes; none stands after the parent.
checkChildClearsCheckpoints :: (Int, Int, Int) -> Either String ()
checkChildClearsCheckpoints (midChild, midParent, post) = do
  checkEq (2, 1, 0) (midChild, midParent, post)

-- | A direct system-database rewind naming its caller records the call:
-- the second identical call replays the step instead of rewinding again,
-- while invalid options are refused on the sysdb channel without
-- recording anything. Returns both verdicts with the recorded step
-- names, the row status, and whether the invalid call refused as
-- 'InvalidInput'. Exercises the caller branch the engine paths leave to
-- the backend, on both stacks.
scenarioRewindCallerStep :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                         => MgmtFixture m -> m (Bool, Bool, [Text], WorkflowStatus, Bool)
scenarioRewindCallerStep fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "rewind-caller-target"
        wid = WorkflowId ("hs-l2-mgmt-rewind-caller-" <> suffix)
    registered <- registerWorkflow dbos key zeroBody
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowValue ()))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    let direct = runSystemDB fx.mfSystemDB (\db -> SystemDB.rewindWorkflow db wid 0 (SysRewindOptions Nothing Nothing Nothing) (Just (wid, 0)))
    first <- direct
    second <- direct
    invalid <- runSystemDB fx.mfSystemDB (\db -> SystemDB.rewindWorkflow db wid (-1) (SysRewindOptions Nothing Nothing Nothing) (Just (wid, 1)))
    let invalidRefused = case invalid of
          Left (SystemDB.InvalidInput {}) -> True
          _ -> False
    listed <- listStepsWf dbos wid
    names <- case listed of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepRecordStepName | record <- records]
    row <- fx.mfReadRow wid
    pure (first == Right (), second == Right (), names, row.workflowRecordStatus, invalidRefused)

-- | Both calls succeed, one step records the call, the row is queued, and
-- the invalid call refused without recording a second step.
checkRewindCallerStep :: (Bool, Bool, [Text], WorkflowStatus, Bool) -> Either String ()
checkRewindCallerStep (first, second, names, status, invalidRefused) = do
  checkEq True first
  checkEq True second
  checkEq ["DBOS.rewind_workflow"] names
  checkEq Enqueued status
  checkEq True invalidRefused

-- | A rewound parent replays to its child and adopts the row already
-- there: the parent runs again, the child does not. Returns both decoded
-- outputs with the run counts.
scenarioRewindAdoptsChild :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                          => MgmtFixture m -> m (Int, Int, Int, Int)
scenarioRewindAdoptsChild fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    parentRan <- newTVarIO (0 :: Int)
    childRan <- newTVarIO (0 :: Int)
    let childKey = newWorkflowKey "adopted-child"
        parentKey = newWorkflowKey "adopting-parent"
        wid = WorkflowId ("hs-l2-mgmt-rewind-adopt-" <> suffix)
    childRefE <- registerWorkflowRef dbos childKey (adoptChildBody childRan)
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    parentRegistered <- registerWorkflow dbos parentKey (adoptParentBody childRef parentRan)
    case parentRegistered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    first <- runWf exec parentKey wid (Just (encodeWorkflowValue ()))
    firstDecoded <- case first of
      Left err -> throwIO (userError (show err))
      Right Nothing -> throwIO (userError "expected the parent result")
      Right (Just output) ->
        case decodeWorkflowValue "result" (Just output) :: Either CodecError Int of
          Right value -> pure value
          Left err -> throwIO (userError (show err))
    rewound <- rewindWorkflows dbos wid (defaultRewindOptions {rewindStartStep = 0})
    case rewound of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    drove <- dequeueWorkflows dbos
    case drove of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    waited <- waitForWorkflow dbos wid
    secondDecoded <- case waited of
      Right (SystemDB.AwaitedSucceeded (Just output) _) ->
        case decodeWorkflowValue "result" (Just (SystemDB.SerializedWorkflowValue output Nothing)) :: Either CodecError Int of
          Right n -> pure n
          Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the re-run to finish, got: " <> show other))
    parents <- readTVarIO parentRan
    children <- readTVarIO childRan
    pure (firstDecoded, secondDecoded, parents, children)

-- | Both runs return the doubled input; the parent ran twice, the child
-- once.
checkRewindAdoptsChild :: (Int, Int, Int, Int) -> Either String ()
checkRewindAdoptsChild (first, second, parents, children) = do
  checkEq 42 first
  checkEq 42 second
  checkEq 2 parents
  checkEq 1 children

-- | A refused rewind inside a workflow spends no step id: the caller
-- hands back the refusal and records nothing. Returns the refusal field
-- with the caller's recorded step names.
scenarioRewindRefusedTakesNoStep :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
                                 => MgmtFixture m -> m (Text, [Text])
scenarioRewindRefusedTakesNoStep fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let targetKey = newWorkflowKey "rewind-target"
        callerKey = newWorkflowKey "refused-caller"
        targetText = "hs-l2-mgmt-rewind-refused-tgt-" <> suffix
        callerText = "hs-l2-mgmt-rewind-refused-op-" <> suffix
    targetRegistered <- registerWorkflow dbos targetKey zeroBody
    case targetRegistered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    callerRegistered <- registerWorkflow dbos callerKey rewindRefusedBody
    case callerRegistered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ranTarget <- runWf exec targetKey (WorkflowId targetText) (Just (encodeWorkflowValue ()))
    case ranTarget of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    ranCaller <- runWf exec callerKey (WorkflowId callerText) (Just (encodeWorkflowValue targetText))
    field <- case ranCaller of
      Left err -> throwIO (userError (show err))
      Right Nothing -> throwIO (userError "expected the caller result")
      Right (Just output) ->
        case decodeWorkflowValue "result" (Just output) :: Either CodecError Text of
          Right value -> pure value
          Left err -> throwIO (userError (show err))
    listed <- listStepsWf dbos (WorkflowId callerText)
    names <- case listed of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepRecordStepName | record <- records]
    pure (field, names)

-- | The refusal names the cut problem, and the caller recorded no step.
checkRewindRefusedTakesNoStep :: (Text, [Text]) -> Either String ()
checkRewindRefusedTakesNoStep (detail, names) = do
  checkEq "start_step must not be negative, got -3" detail
  checkEq [] names
