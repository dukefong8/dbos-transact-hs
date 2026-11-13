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
    scenarioCancelledSeesOneError,
    scenarioResumeAfterCancelAdoptsOwner,
    scenarioSupervisorManagesFreely,
    scenarioStepManagementIsPlain,
    scenarioSwitchFormsReplay,
    scenarioResumeOntoQueue,
    scenarioCancelTree,
    scenarioDelete,
    scenarioRetrieve,
    scenarioListSteps,
    scenarioListStepsWithoutOutputs,
    scenarioListStepsPaging,
    scenarioListStepsInWorkflow,
    scenarioSingular,
    ForkReplacementFacts (..),
    scenarioForkFromBeginning,
    scenarioForkReplacesChild,
    scenarioForkTakesIdAndQueue,
    scenarioForkFromStep,
    scenarioForkFromFailure,
    scenarioBulkCancelResume,
    scenarioBulkFork,
    scenarioForkRefusals,
    scenarioForkBatchRefusals,
    scenarioStartRefusals,
    scenarioSelfCancelResume,
    scenarioSelfDelete,
    scenarioAncestorDelete,
    scenarioForkPartitioned,
    scenarioForkInheritsOwner,
    scenarioAttributes,
    scenarioAttributesInWorkflow,
    scenarioMergeAttributes,
    scenarioMissingAttributesReplay,
    scenarioDelayRelease,
    scenarioDelayReleaseInWorkflow,
    scenarioSetDelayRefusal,
    scenarioRewindRunsAgain,
    scenarioRewindRefusals,
    scenarioRewindUndoCut,
    scenarioRewindKeepBelowCut,
    scenarioRewindKeepStreamClose,
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
    checkCancelledSeesOneError,
    checkResumeAfterCancelAdoptsOwner,
    checkSupervisorManagesFreely,
    checkStepManagementIsPlain,
    checkSwitchFormsReplay,
    checkResumeOntoQueue,
    checkCancelTree,
    checkDelete,
    checkRetrieve,
    checkListSteps,
    checkListStepsWithoutOutputs,
    checkListStepsPaging,
    checkListStepsInWorkflow,
    checkSingular,
    checkForkFromBeginning,
    checkForkReplacesChild,
    checkForkTakesIdAndQueue,
    checkForkFromStep,
    checkForkFromFailure,
    checkBulkCancelResume,
    checkBulkFork,
    checkForkRefusals,
    checkForkBatchRefusals,
    checkStartRefusals,
    checkSelfCancelResume,
    checkSelfDelete,
    checkAncestorDelete,
    checkForkPartitioned,
    checkForkInheritsOwner,
    checkAttributes,
    checkAttributesInWorkflow,
    checkMergeAttributes,
    checkMissingAttributesReplay,
    checkDelayRelease,
    checkDelayReleaseInWorkflow,
    checkSetDelayRefusal,
    checkRewindRunsAgain,
    checkRewindRefusals,
    checkRewindUndoCut,
    checkRewindKeepBelowCut,
    checkRewindKeepStreamClose,
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

import DBOS.Internal.Prelude
import Data.Int (Int64)
import Data.List (sort)
import Data.Map.Strict qualified as Map
import Data.Text qualified as Text
import Data.Vector.Fusion.Stream.Monadic qualified as Vector
import GHC.Stack (HasCallStack)
import DBOS.SystemDB
  ( AppScope (..),
    EventRecord (..),
    Fork (..),
    ForkOptions (..),
    ForkPoint (..),
    NewWorkflow (..),
    NotificationRecord (..),
    QueueName (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    StepRecord (..),
    StreamRecord (..),
    SysRewindOptions (..),
    Timestamp (..),
    WorkflowDelay (..),
    WorkflowFilter (..),
    WorkflowId (..),
    WorkflowIdReusePolicy (..),
    WorkflowRecord (..),
    WorkflowStatus (..),
    cancelStepName,
    defaultForkOptions,
    defaultWorkflowFilter,
    deleteStepName,
    durationFromMs,
    durationFromSecs,
    forkNew,
    forkStepName,
    internalQueueName,
    listStepsStepName,
    listWorkflowsStepName,
    newWorkflow,
    resumeStepName,
    rewindStepName,
    setWorkflowDelayStepName,
    streamClosedSentinel,
    portableJsonSerialization,
    updateWorkflowAttributesStepName
  )
import DBOS.SystemDB qualified as SystemDB
import DBOS.Internal.Transact.Serialization
  ( WorkflowInput,
    decodeWorkflowValue,
    encodeWorkflowValue,
    encodeWorkflowInput,
    CodecError (..)
  )
import DBOS.Transact
  (
    Config (..),
    DBOS,
    QueueRequest (..),
    EngineOnly,
    Error (..),
    Executor,
    Serializer (..),
    StartOptions (..),
    StepInfo (..),
    Timeout (..),
    StepOutcome (..),
    StepOutput (..),
    StepFailure (..),
    PortableWorkflowError (..),
    WorkflowCtx,
    WorkflowHandle (workflowId),
    QualifiedWorkflowName,
    WorkflowRef,
    cancelWorkflow,
    cancelWorkflows,
    cancelWorkflowsInWorkflow,
    closeStream,
    defaultRewindOptions,
    deleteWorkflow,
    deleteWorkflows,
    deleteWorkflowsInWorkflow,
    enqueueWorkflow,
    queueRequestNew,
    forkWorkflow,
    forkFrom,
    forkWorkflows,
    getWorkflowStatus,
    getWorkflowRecord,
    handleResult,
    handleStatus,
    millisDuration,
    newDBOS,
    newQualifiedWorkflowName,
    recv,
    registerDataSource,
    registerWorkflow,
    registerWorkflowRef,
    resumeWorkflow,
    resumeWorkflows,
    resumeWorkflowsInWorkflow,
    retrieveWorkflow,
    rewindWorkflow,
    rewindWorkflows,
    rewindWorkflowsInWorkflow,
    RewindOptions (..),
    runTxStep,
    runWorkflow,
    runStep,
    runStepWith,
    awaitChild,
    stepOptionsDefault,
    StreamReader,
    cancelWorkflowInWorkflow,
    defaultReadStreamOptions,
    deleteWorkflowInWorkflow,
    forkWorkflowInWorkflow,
    forkWorkflowsInWorkflow,
    getEvent,
    patchStep,
    pendingSleep,
    readStreamFrom,
    resumeWorkflowInWorkflow,
    retrieveWorkflowInWorkflow,
    rewindWorkflowInWorkflow,
    selectStep,
    SelectArm (..),
    selectWorkflow,
    sleepStep,
    toVectorStream,
    send,
    setEvent,
    setWorkflowDelay,
    setWorkflowDelayInWorkflow,
    Topic (..),
    mergeWorkflowAttributes,
    mergeWorkflowAttributesInWorkflow,
    updateWorkflowAttributes,
    updateWorkflowAttributesInWorkflow,
    ListWorkflowStepsOptions (..),
    defaultListWorkflowStepsOptions,
    listWorkflows,
    listWorkflowSteps,
    listWorkflowStepsWith,
    listWorkflowStepsInWorkflow,
    listWorkflowsInWorkflow,
    secondsDuration,
    shutdown,
    startChildWorkflow,
    startWorkflow,
    startOptionsDefault,
    waitForWorkflow,
    writeStream,
  )
import DBOS.Transact.DatasourceCases (FakeDs (..), mkFakeDs, protoConfig)
import DBOS.Internal.Transact.Datasource (DataSource (..), Tx (..))
import DBOS.Internal.Transact.Logger (SomeTracer (..))
import DBOS.Internal.Transact.Identity (Identity (..))
import DBOS.Internal.Transact.Instance (dequeueWorkflows, launchOnWithQueues)
import DBOS.Internal.Transact.Connection
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
mkMgmtFixture ::  forall m. (MonadMVar m, MonadSTM m, MonadThrow m)
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
        PortableJson
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
runWf ::  forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
      => Executor m -> QualifiedWorkflowName -> WorkflowId -> Maybe WorkflowInput -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWf = runWorkflow

startWfRef ::  forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
           => Executor m -> WorkflowRef m EngineOnly -> StartOptions -> Maybe WorkflowInput -> m (Either (Error EngineOnly) (WorkflowHandle m EngineOnly))
startWfRef = startWorkflow

retrieveWf ::  forall m. (MonadMVar m)
           => DBOS m -> WorkflowId -> m (Either (Error EngineOnly) (WorkflowHandle m EngineOnly))
retrieveWf = retrieveWorkflow

resultWf ::  forall m. (MonadDelay m, MonadTime m, MonadMVar m, MonadThrow m)
         => WorkflowHandle m EngineOnly -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWf = handleResult

statusWf ::  forall m. (MonadMVar m)
         => WorkflowHandle m EngineOnly -> m (Either (Error EngineOnly) (Maybe WorkflowStatus))
statusWf = handleStatus

listStepsWf ::  forall m. (MonadMVar m)
            => DBOS m -> WorkflowId -> m (Either (Error EngineOnly) [StepInfo])
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
treeChildBody ::  forall exec m. (MonadMVar m)
              => StrictMVar m () ->
                 StrictMVar m () ->
                 () ->
                 WorkflowCtx exec m ->
                 m (Either (Error EngineOnly) Int)
treeChildBody started gate () _ = putMVar started () >> takeMVar gate >> pure (Right 1)

-- | The tree parent: starts the child, waits until it has begun (so the
-- cancel below cannot miss it), and hands back the child's id.
treeParentBody ::  forall exec m. (MonadAsync m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
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
scenarioUnlaunched :: forall m. (MonadCatch m, MonadMonotonicTimeNSec m, MonadDelay m, MonadAsync m, MonadFork m, MonadMVar m)
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
scenarioCancelMissing :: forall m. (MonadAsync m, MonadDelay m, MonadMonotonicTimeNSec m, MonadCatch m, MonadFork m, MonadMVar m)
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
scenarioResumeMissing :: forall m. (MonadAsync m, MonadDelay m, MonadMonotonicTimeNSec m, MonadCatch m, MonadFork m, MonadMVar m)
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
scenarioCancelResumeRun :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                        => MgmtFixture m ->
                           m (Text, [Text], Maybe WorkflowStatus, Int, [Text], Int, Int)
scenarioCancelResumeRun fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    ran <- newTVarIO (0 :: Int)
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "cancellable"
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
        (startOptionsDefault {startWorkflowId = Just wid, startQueue = Just (queueRequestNew "no-runner-here")})
        (Just (encodeWorkflowInput (0 :: Int)))
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
scenarioResumeOntoQueue :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                        => MgmtFixture m ->
                           m Int
scenarioResumeOntoQueue fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "resumable"
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
        (startOptionsDefault {startWorkflowId = Just wid, startQueue = Just (queueRequestNew "no-runner-here")})
        (Just (encodeWorkflowInput (7 :: Int)))
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
scenarioCancelTree :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                   => MgmtFixture m ->
                      m (Text, [Text], Maybe WorkflowStatus)
scenarioCancelTree fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    childStarted <- newEmptyMVar
    gate <- newEmptyMVar
    suffix <- fx.mfFreshBase
    let childKey = newQualifiedWorkflowName "tree-child"
        parentKey = newQualifiedWorkflowName "tree-parent"
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
scenarioDelete :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
               => MgmtFixture m ->
                  m (Word64, Maybe WorkflowStatus)
scenarioDelete fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "deletable"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body input wctx = runStep wctx "work" (const (pure input))
        wid = WorkflowId ("hs-l2-mgmt-delete-" <> suffix)
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowInput (1 :: Int)))
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
scenarioRetrieve :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                 => MgmtFixture m ->
                    m (Maybe WorkflowStatus, Int)
scenarioRetrieve fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "retrievable"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body input _ = pure (Right (input * 3))
        wid = WorkflowId ("hs-l2-mgmt-retrieve-" <> suffix)
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowInput (2 :: Int)))
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
scenarioListSteps :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                  => MgmtFixture m ->
                     m ([Text], [Int], [Text])
scenarioListSteps fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "three-steps"
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
    ran <- runWf exec key wid (Just (encodeWorkflowInput ()))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    listed <- listStepsWf dbos wid
    (names, ids) <- case listed of
      Left err -> throwIO (userError (show err))
      Right records ->
        pure
          ( [record.stepInfoName | record <- records],
            [record.stepInfoId | record <- records]
          )
    missing <- listStepsWf dbos (WorkflowId "never-existed")
    missingNames <- case missing of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepInfoName | record <- records]
    pure (names, ids, missingNames)

-- | A listing with 'loadOutput' off reads metadata only: both payload
-- columns come back empty on every step, while the default reads them.
scenarioListStepsWithoutOutputs :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                                => MgmtFixture m -> m ([StepInfo], [StepInfo])
scenarioListStepsWithoutOutputs fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "big-then-fails"
        wid = WorkflowId ("hs-l2-mgmt-steps-no-output-" <> suffix)
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
        body () wctx = do
          big <- runStepWith stepOptionsDefault wctx "big" (const (pure (Right ("big output" :: Text))))
          case big of
            Left err -> pure (Left err)
            Right _ -> do
              fails <- (runStepWith stepOptionsDefault wctx "fails" (const (pure (Left (StepFailed "fails" "declined")))) :: m (Either (Error EngineOnly) ()))
              pure (void fails)
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    _ <- runWf exec key wid (Just (encodeWorkflowInput ()))
    withOutputs <- listWorkflowStepsWith dbos wid defaultListWorkflowStepsOptions
    without <- listWorkflowStepsWith dbos wid defaultListWorkflowStepsOptions {loadOutput = False}
    case (withOutputs, without) of
      (Right withRows, Right withoutRows) -> pure (withRows, withoutRows)
      (Left err, _) -> throwIO (userError (show err))
      (_, Left err) -> throwIO (userError (show err))

-- | A listing pages with 'limit' and 'offset', in step order.
scenarioListStepsPaging :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                        => MgmtFixture m -> m [[Text]]
scenarioListStepsPaging fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "five-steps"
        wid = WorkflowId ("hs-l2-mgmt-steps-page-" <> suffix)
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
        body () wctx = do
          outcomes <- mapM (\name -> runStep wctx name (const (pure ()))) ["a", "b", "c", "d", "e"]
          pure (void (sequence outcomes))
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowInput ()))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    mapM
      ( \(limit', offset') -> do
          listed <- listWorkflowStepsWith dbos wid defaultListWorkflowStepsOptions {limit = limit', offset = offset'}
          case listed of
            Left err -> throwIO (userError (show err))
            Right records -> pure [record.stepInfoName | record <- records]
      )
      [ (Just 2, Nothing),
        (Just 2, Just 2),
        (Nothing, Just 3),
        (Just 10, Just 4),
        (Nothing, Just 5)
      ]

-- | The in-workflow listing is itself a step: it reads the steps recorded
-- before it and the engine records the snapshot after it, under the
-- cross-SDK step name. The snapshot is @(loadOutput, rows)@ so a replay can
-- re-project the typed view from the flag the first execution read.
scenarioListStepsInWorkflow :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                            => MgmtFixture m ->
                               m ([Text], [Text], Text)
scenarioListStepsInWorkflow fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "self-lister"
        wid = WorkflowId ("hs-l2-mgmt-liststeps-in-" <> suffix)
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) [Text])
        body () wctx = do
          first <- runStep wctx "first" (const (pure ()))
          case first of
            Left err -> pure (Left err)
            Right () -> do
              listed <- listWorkflowStepsInWorkflow wctx wid
              pure (fmap (map (.stepInfoName)) listed)
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowInput ()))
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
      Right records -> pure [record.stepInfoName | record <- records]
    recorded <-
      runSystemDB fx.mfSystemDB
        (\db -> SystemDB.listSteps db wid True Nothing Nothing Nothing)
    listingText <- case recorded of
      Left err -> throwIO (userError (show err))
      Right records -> case [text | StepRecord {stepRecordStepName = name, stepRecordOutput = Just text} <- records, name == "DBOS.listWorkflowSteps"] of
        [text] -> pure text
        other -> throwIO (userError ("expected one recorded listing, got: " <> show other))
    pure (inside, outsideNames, listingText)

-- | The singular wrappers move one workflow each, per the Rust @DBOS@
-- singular surface: cancel (children skipped), resume (handle back),
-- delete, fork-from-the-top (handle back), and the singular status read.
scenarioSingular :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                 => MgmtFixture m ->
                    m (Maybe WorkflowStatus, Maybe WorkflowStatus, Maybe WorkflowStatus, Bool, Maybe WorkflowStatus)
scenarioSingular fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "singular"
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
        (startOptionsDefault {startWorkflowId = Just (wid 1), startQueue = Just (queueRequestNew "no-runner-here")})
        (Just (encodeWorkflowInput (1 :: Int)))
    ran2 <- runWf exec key (wid 2) (Just (encodeWorkflowInput (2 :: Int)))
    ran3 <- runWf exec key (wid 3) (Just (encodeWorkflowInput (3 :: Int)))
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

-- | A cancelled workflow sees one error from every call: the step in
-- progress when its result is refused, and every kind of durable call
-- made after that. The body keeps going past each error to try the next
-- call, so the run ends cancelled and only the pre-cancel child start is
-- recorded. Mirrors Rust
-- @a_cancelled_workflow_sees_one_error_from_every_call@. Returns the
-- noted calls, the run's error, the row's status, and the recorded step
-- names.
scenarioCancelledSeesOneError :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                              => MgmtFixture m -> m ([(Text, Text)], Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowStatus, [Text])
scenarioCancelledSeesOneError fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let parentText = "hs-l2-mgmt-one-error-" <> suffix
        wid = WorkflowId parentText
    seen <- newTVarIO []
    entered <- newEmptyMVar
    released <- newEmptyMVar
    let note :: forall a. Text -> m (Either (Error EngineOnly) a) -> m ()
        note name action = do
          result <- action
          atomically (modifyTVar seen ((name, render result) :))
        render :: forall b. Either (Error EngineOnly) b -> Text
        render (Left (WorkflowCancelled {workflowId})) = "cancelled " <> workflowId
        render (Left err) = "other " <> showText err
        render (Right _) = "ok"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
        childBody _ _ = pure (Right ())
    childRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "one-error-child") childBody >>= either (throwIO . userError . show) pure
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
        parentBody _ wctx = do
          started <- startChildWorkflow wctx childRef startOptionsDefault Nothing
          firstChild <- case (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) of
            Left err -> throwIO (userError (show err))
            Right handle -> pure handle
          note "step in progress" (runStep wctx "slow" (\_ -> putMVar entered () >> takeMVar released >> pure (1 :: Int)))
          note "step" (runStep wctx "later" (\_ -> pure (2 :: Int)))
          note "sleep" (sleepStep wctx (millisDuration 10))
          note "set_event" (setEvent wctx "key" ("1" :: Text))
          note "get_event" (getEvent wctx wid "key" (millisDuration 10) :: m (Either (Error EngineOnly) (Maybe Text)))
          note "send" (send wctx wid (Just (Topic "t")) Nothing ("1" :: Text))
          note "recv" (recv wctx (Just (Topic "t")) (millisDuration 10) :: m (Either (Error EngineOnly) (Maybe Text)))
          note "write_stream" (writeStream wctx "key" ("1" :: Text))
          note "close_stream" (closeStream wctx "key")
          note "patch" (patchStep wctx "v2")
          note "cancel_workflow" (cancelWorkflowsInWorkflow wctx [WorkflowId ("other-" <> suffix)] False)
          note "start a child" (startChildWorkflow wctx childRef startOptionsDefault Nothing)
          note "await a child" (awaitChild wctx firstChild)
          reader <- readStreamFrom wctx wid "out" 0 defaultReadStreamOptions
          note "read stream" (pullHead reader)
          note "select_workflow" (selectWorkflow wctx [WorkflowId firstChild.workflowId])
          armAResult <- pendingSleep wctx (millisDuration 10)
          armBResult <- pendingSleep wctx (millisDuration 10)
          case (armAResult, armBResult) of
            (Right armA, Right armB) -> note "select_step" (selectStep wctx [SelectArm "raced-a" armA pure, SelectArm "raced-b" armB pure])
            _ -> throwIO (userError "pending sleeps were refused")
          pure (Right ())
        pullHead :: StreamReader (Error EngineOnly) m Text -> m (Either (Error EngineOnly) (Maybe Text))
        pullHead reader = case toVectorStream reader of
          Vector.Stream next initial -> runExceptT $ do
            result <- next initial
            pure $ case result of
              Vector.Yield value _ -> Just value
              _ -> Nothing
    parentRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "one-error") parentBody >>= either (throwIO . userError . show) pure
    exec <- fx.mfLaunch dbos
    started <- startWfRef exec parentRef (startOptionsDefault {startWorkflowId = Just wid}) Nothing
    handle <- case started of
      Left err -> throwIO (userError (show err))
      Right h -> pure h
    takeMVar entered
    cancelled <- cancelWorkflows dbos [wid] False
    case cancelled of
      Right [cancelledId] | cancelledId == wid -> pure ()
      other -> throwIO (userError ("expected the cancel to land, got: " <> show other))
    putMVar released ()
    outcome <- resultWf handle
    notes <- readTVarIO seen
    row <- fx.mfReadRow wid
    listed <- listStepsWf dbos wid
    steps <- case listed of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepInfoName | record <- records]
    pure (notes, outcome, Just row.workflowRecordStatus, steps)

-- | Every noted call saw the cancellation, the run reports its own
-- cancellation like the oracle's handle, the row reads CANCELLED, and
-- only the pre-cancel child start was recorded.
checkCancelledSeesOneError :: ([(Text, Text)], Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowStatus, [Text]) -> Either String ()
checkCancelledSeesOneError (notes, outcome, status, steps) = do
  checkEq 16 (length notes)
  forM_ notes $ \(call, saw) -> unless ("cancelled hs-l2-mgmt-one-error-" `Text.isPrefixOf` saw) (Left ("expected WorkflowCancelled from " <> show call <> ", got: " <> show saw))
  case outcome of
    Left (WorkflowCancelled {workflowId}) -> checkEq True ("hs-l2-mgmt-one-error-" `Text.isPrefixOf` workflowId)
    other -> Left ("expected the run cancelled, got: " <> show other)
  checkEq (Just Cancelled) status
  checkEq ["one-error-child"] steps

-- | A resume after the body hears of a cancel gives the owner's outcome:
-- the body outlives the call that told it, and by the time it returns
-- that error the workflow has been resumed and finished by a new owner.
-- The error is no longer the outcome, so the old execution's caller gets
-- what the owner recorded. Mirrors Rust
-- @a_resume_after_the_body_hears_of_a_cancel_gives_the_owners_outcome@.
-- Returns the stale handle's decoded outcome and the row's status.
scenarioResumeAfterCancelAdoptsOwner :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                                    => MgmtFixture m -> m (Int, Maybe WorkflowStatus)
scenarioResumeAfterCancelAdoptsOwner fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let parentText = "hs-l2-mgmt-heard-cancel-" <> suffix
        wid = WorkflowId parentText
    entered <- newTVarIO (0 :: Int)
    heard <- newTVarIO (0 :: Int)
    release <- newTVarIO False
    hold <- newTVarIO False
    let bump var = atomically (modifyTVar var (+ 1))
        waitFlag var = atomically (readTVar var >>= check)
        waitCount var n label = go (0 :: Int)
          where
            go attempts = do
              count <- readTVarIO var
              if count >= n
                then pure ()
                else
                  if attempts >= 600
                    then throwIO (userError label)
                    else threadDelay 100000 >> go (attempts + 1)
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body _ wctx = do
          outcome <- runStep wctx "first" (\_ -> bump entered >> waitFlag release >> pure (2 :: Int))
          case outcome of
            Right n -> pure (Right n)
            Left err -> bump heard >> waitFlag hold >> pure (Left err)
    ref <- registerWorkflowRef dbos (newQualifiedWorkflowName "heard") body >>= either (throwIO . userError . show) pure
    exec <- fx.mfLaunch dbos
    started <- startWfRef exec ref (startOptionsDefault {startWorkflowId = Just wid}) Nothing
    handle <- case started of
      Left err -> throwIO (userError (show err))
      Right h -> pure h
    waitCount entered 1 "the workflow never reached its gate"
    cancelled <- cancelWorkflows dbos [wid] False
    case cancelled of
      Right [cancelledId] | cancelledId == wid -> pure ()
      other -> throwIO (userError ("expected the cancel to land, got: " <> show other))
    atomically (writeTVar release True)
    waitCount heard 1 "the body never heard of the cancel"
    resumed <- resumeWorkflows dbos [wid] Nothing
    case resumed of
      Right [resumedId] | resumedId == wid -> pure ()
      other -> throwIO (userError ("expected the resume to land, got: " <> show other))
    -- Drive the dequeue entry synchronously: the resumed row runs now
    -- instead of waiting for a supervisor tick that bare launches never
    -- fork, as the cancel-resume case does.
    drove <- dequeueWorkflows dbos
    case drove of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    -- The resumed run replays nothing (the first run's step was refused)
    -- and finishes; only then is the old execution let go, so its error
    -- is no longer the outcome.
    awaitStatus dbos wid Success
    atomically (writeTVar hold True)
    outcome <- resultWf handle
    decoded <- case outcome of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the owner's outcome, got: " <> show other))
    row <- fx.mfReadRow wid
    pure (decoded, Just row.workflowRecordStatus)
  where
    awaitStatus dbos wid status = go (0 :: Int)
      where
        go attempts = do
          found <- getWorkflowRecord dbos wid >>= either (throwIO . userError . show) pure
          case found of
            Just row | row.workflowRecordStatus == status -> pure ()
            _ ->
              if attempts >= 600
                then throwIO (userError "the workflow never reached SUCCESS")
                else threadDelay 100000 >> go (attempts + 1)

-- | The stale handle reports the owner's outcome, and the row reads
-- SUCCESS.
checkResumeAfterCancelAdoptsOwner :: (Int, Maybe WorkflowStatus) -> Either String ()
checkResumeAfterCancelAdoptsOwner = checkEq (2, Just Success)

-- | Management calls inside a step body run plainly and record nothing
-- of their own, and a refused fork spends no step id: the operator
-- refuses a negative-step fork, then cancels and lists its parked target
-- from inside one @manage@ step. Returns the listed count, the target's
-- status, and the operator's recorded (id, name) steps. Mirrors Rust's
-- in-step test.
scenarioStepManagementIsPlain :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                              => MgmtFixture m -> m (Int, Maybe WorkflowStatus, [(Int, Text)])
scenarioStepManagementIsPlain fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let targetText = "hs-l2-mgmt-step-target-" <> suffix
        operatorText = "hs-l2-mgmt-step-operator-" <> suffix
        targetBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        targetBody _ _ = pure (Right 1)
        operatorBody :: forall exec. Text -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        operatorBody target wctx = do
          refused <- forkWorkflowInWorkflow wctx (WorkflowId target) (Just (ForkStep (-1))) defaultForkOptions
          case refused of
            Left _ -> pure ()
            Right _ -> throwIO (userError "the refused fork handed back a handle")
          listed <- runStep wctx "manage" $ \_ -> do
            _ <- cancelWorkflowInWorkflow wctx (WorkflowId target) >>= either (\e -> throwIO (userError (show e))) pure
            rows <- listWorkflowsInWorkflow wctx (defaultWorkflowFilter {workflowFilterWorkflowIds = [target]}) >>= either (\e -> throwIO (userError (show e))) pure
            pure (length rows)
          case listed of
            Left err -> pure (Left err)
            Right n -> pure (Right n)
    targetRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "step-target") targetBody >>= either (throwIO . userError . show) pure
    let operatorKey = newQualifiedWorkflowName "step-operator"
    registered <- registerWorkflow dbos operatorKey operatorBody
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    started <- startWfRef exec targetRef (startOptionsDefault {startWorkflowId = Just (WorkflowId targetText), startQueue = Just (queueRequestNew "nothing-polls-this")}) (Just (encodeWorkflowInput ()))
    case started of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    ran <- runWf exec operatorKey (WorkflowId operatorText) (Just (encodeWorkflowInput targetText))
    listed <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the operator's count, got: " <> show other))
    found <- getWorkflowRecord dbos (WorkflowId targetText) >>= either (throwIO . userError . show) pure
    steps <- listStepsWf dbos (WorkflowId operatorText) >>= either (throwIO . userError . show) pure
    pure (listed, (.workflowRecordStatus) <$> found, [(record.stepInfoId, record.stepInfoName) | record <- steps])

-- | The target is cancelled, the operator counted it, and the only
-- recorded step is the managing one: the refusal spent no id and the
-- in-step calls recorded nothing of their own.
checkStepManagementIsPlain :: (Int, Maybe WorkflowStatus, [(Int, Text)]) -> Either String ()
checkStepManagementIsPlain = checkEq (1, Just Cancelled, [(0, "manage")])

-- | A run recorded through the plural forms replays through the singular
-- ones, which is what sharing one step name buys: a body that drops its
-- bulk calls for the one-row forms recovers the executions the old code
-- started. The first instance records cancel, fork, list and list-steps
-- through the plurals; the second registers the same name with the
-- singulars and forks the first run past its last step, so the whole
-- body replays. Returns the recorded summary, the replay's, and the
-- source's forks. Mirrors Rust's switch-forms test; the oracle switches
-- from captured-handle methods, which the port's explicit context makes
-- unrepresentable, so the port switches from the plural free forms.
scenarioSwitchFormsReplay :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                          => MgmtFixture m -> m ((Text, Int, Int), (Text, Int, Int), [Text])
scenarioSwitchFormsReplay fx = do
  let sourceBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      sourceBody _ _ = pure (Right 5)
      firstBody :: forall exec. Text -> WorkflowCtx exec m -> m (Either (Error EngineOnly) (Text, Int, Int))
      firstBody source wctx = do
        _ <- orThrow "cancel" =<< cancelWorkflowsInWorkflow wctx [WorkflowId source] False
        forked <- orThrow "fork" =<< forkWorkflowsInWorkflow wctx [forkNew source] defaultForkOptions
        rows <- orThrow "list" =<< listWorkflowsInWorkflow wctx (defaultWorkflowFilter {workflowFilterWorkflowIds = [source]})
        steps <- orThrow "list-steps" =<< listWorkflowStepsInWorkflow wctx (WorkflowId source)
        case forked of
          [WorkflowId forkId] -> pure (Right (forkId, length rows, length steps))
          _ -> throwIO (userError ("expected one fork, got: " <> show forked))
      secondBody :: forall exec. Text -> WorkflowCtx exec m -> m (Either (Error EngineOnly) (Text, Int, Int))
      secondBody source wctx = do
        _ <- orThrow "cancel" =<< cancelWorkflowInWorkflow wctx (WorkflowId source)
        forked <- orThrow "fork" =<< forkWorkflowInWorkflow wctx (WorkflowId source) Nothing defaultForkOptions
        rows <- orThrow "list" =<< listWorkflowsInWorkflow wctx (defaultWorkflowFilter {workflowFilterWorkflowIds = [source]})
        steps <- orThrow "list-steps" =<< listWorkflowStepsInWorkflow wctx (WorkflowId source)
        pure (Right (forked.workflowId, length rows, length steps))
      orThrow :: forall a. Text -> Either (Error EngineOnly) a -> m a
      orThrow label = either (\e -> throwIO (userError (Text.unpack label <> ": " <> show e))) pure
      awaitSummary wid dbos = do
        handle <- retrieveWf dbos wid >>= either (throwIO . userError . show) pure
        outcome <- resultWf handle
        stored <- orThrow "operator" outcome
        case stored of
          Just value -> case decodeWorkflowValue "result" (Just value) :: Either CodecError (Text, Int, Int) of
            Right summary -> pure summary
            Left err -> throwIO (userError (show err))
          Nothing -> throwIO (userError "the operator handed back no summary")
      forksOf dbos wanted = do
        rows <- listWorkflows dbos (defaultWorkflowFilter {workflowFilterAppScope = Named [fx.mfIdentity.identityAppName], workflowFilterForkedFrom = [wanted]}) >>= either (throwIO . userError . show) pure
        pure (sort [t | WorkflowId t <- (.workflowRecordId) <$> rows])
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let source = "hs-l2-mgmt-switch-source-" <> suffix
        operator = WorkflowId ("hs-l2-mgmt-switching-" <> suffix)
        operatorKey = newQualifiedWorkflowName "switching"
    sourceRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "switch-target") sourceBody >>= either (throwIO . userError . show) pure
    registered <- registerWorkflow dbos operatorKey firstBody
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    started <- startWfRef exec sourceRef (startOptionsDefault {startWorkflowId = Just (WorkflowId source)}) (Just (encodeWorkflowInput ()))
    sourceHandle <- case started of
      Left err -> throwIO (userError (show err))
      Right handle -> pure handle
    _ <- awaitIntHandle sourceHandle
    ran <- runWf exec operatorKey operator (Just (encodeWorkflowInput source))
    recorded <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError (Text, Int, Int) of
        Right summary -> pure summary
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the method-form summary, got: " <> show other))
    listed <- listStepsWf dbos operator >>= either (throwIO . userError . show) pure
    bracket fx.mfNewDBOS shutdown $ \dbos2 -> do
      registered2 <- registerWorkflow dbos2 operatorKey secondBody
      case registered2 of
        Left err -> throwIO (userError (show err))
        Right () -> pure ()
      _ <- fx.mfLaunch dbos2
      -- Past the last step the plurals recorded, so the whole body
      -- replays through the singulars.
      replayedIds <- forkFrom dbos2 [operator] (ForkStep (fromIntegral (length listed))) defaultForkOptions >>= either (throwIO . userError . show) pure
      replayId <- case replayedIds of
        [rid] -> pure rid
        _ -> throwIO (userError ("expected one replayed operator, got: " <> show replayedIds))
      _ <- dequeueWorkflows dbos2 >>= either (throwIO . userError . show) pure
      replayed <- awaitSummary replayId dbos2
      forks <- forksOf dbos2 source
      -- Forced before the return: the rows escape runSimCase with it,
      -- and a one-element sort performs no comparison, so an unevaluated
      -- element would read the sim's rows outside it.
      length (map (\t -> t `seq` t) forks) `seq` pure (recorded, replayed, forks)
  where
    awaitIntHandle handle = do
      outcome <- resultWf handle
      case outcome of
        Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
          Right n -> pure n
          Left err -> throwIO (userError (show err))
        other -> throwIO (userError ("expected an output, got: " <> show other))

-- | The replay hands back what the recording saw, and the source was
-- forked exactly once: the singulars replayed the plurals' history.
checkSwitchFormsReplay :: ((Text, Int, Int), (Text, Int, Int), [Text]) -> Either String ()
checkSwitchFormsReplay (recorded, replayed, forks) = do
  checkEq recorded replayed
  let (forkId, _, _) = recorded
  checkEq True (not (Text.null forkId))
  checkEq [forkId] forks

-- | A supervisor whose closure captures no handle manages other
-- workflows through every free form: cancel, resume, fork, rewind,
-- delete, delay, tag, list and retrieve. Each records under its DBOS
-- method's step name, in the order it was made. Forking the supervisor
-- past its last step replays every call from its checkpoint: the replay
-- hands back what the first run saw, the rewound row is untouched, and
-- nothing forks again. Mirrors Rust's supervisor test. Awaiting
-- unfinished handles stays outside the body, as bare launches never fork
-- a supervisor tick, and direct row reads stay outside too, so the
-- replayed body is fully checkpointed.
-- Returns the first run's summary, the replay's, the targets' outputs,
-- the supervisor's recorded step names in order, and the replay-effect
-- probes.
scenarioSupervisorManagesFreely :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                                => MgmtFixture m -> m ((([Text], Text, Text, Text), (Int, Int, [Text], Int)), (([Text], Text, Text, Text), (Int, Int, [Text], Int)), Int, Int, Int, Int, Int, [Text], [Text], [Text], (WorkflowStatus, Timestamp), (WorkflowStatus, Timestamp), Int, Bool, Maybe Text)
scenarioSupervisorManagesFreely fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let t1 = WorkflowId ("hs-l2-mgmt-sup-victim-" <> suffix)
        t2 = WorkflowId ("hs-l2-mgmt-sup-forked-" <> suffix)
        t3 = WorkflowId ("hs-l2-mgmt-sup-rewound-" <> suffix)
        t4 = WorkflowId ("hs-l2-mgmt-sup-deleted-" <> suffix)
        t5 = WorkflowId ("hs-l2-mgmt-sup-tagged-" <> suffix)
        t6 = WorkflowId ("hs-l2-mgmt-sup-delayed-" <> suffix)
        sup = WorkflowId ("hs-l2-mgmt-supervisor-" <> suffix)
        WorkflowId t1text = t1
        WorkflowId t2text = t2
        WorkflowId t3text = t3
        WorkflowId t5text = t5
    entered <- newTVarIO (0 :: Int)
    released <- newTVarIO False
    rewindRuns <- newTVarIO (0 :: Int)
    let orThrow :: forall a. Text -> Either (Error EngineOnly) a -> m a
        orThrow label = either (\e -> throwIO (userError (Text.unpack label <> ": " <> show e))) pure
        waitCount var n label = go (0 :: Int)
          where
            go attempts = do
              count <- readTVarIO var
              if count >= n
                then pure ()
                else
                  if attempts >= 600
                    then throwIO (userError label)
                    else threadDelay 100000 >> go (attempts + 1)
        decodeStored :: Text -> Maybe SerializedWorkflowValue -> m Int
        decodeStored label = \case
          Just stored -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
            Right n -> pure n
            Left err -> throwIO (userError (Text.unpack label <> ": " <> show err))
          Nothing -> throwIO (userError (Text.unpack label <> ": no output"))
        victimBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        victimBody _ _ = do
          atomically (modifyTVar entered (+ 1))
          atomically (readTVar released >>= check)
          pure (Right 41)
        quickBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        quickBody _ _ = pure (Right 7)
        rewindBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        rewindBody _ wctx = do
          ticked <- runStep wctx "tick" (\_ -> atomically (modifyTVar rewindRuns (+ 1)) >> pure (9 :: Int))
          case ticked of
            Left err -> pure (Left err)
            Right n -> pure (Right n)
        serveBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        serveBody _ wctx = do
          served <- runStep wctx "serve" (\_ -> pure (7 :: Int))
          case served of
            Left err -> pure (Left err)
            Right n -> pure (Right n)
        superBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) (([Text], Text, Text, Text), (Int, Int, [Text], Int)))
        superBody _ wctx = do
          cancelled <- orThrow "cancel" =<< cancelWorkflowsInWorkflow wctx [t1] False
          resumedHandle <- orThrow "resume" =<< resumeWorkflowInWorkflow wctx t1 Nothing
          forkedHandle <- orThrow "fork" =<< forkWorkflowInWorkflow wctx t2 Nothing defaultForkOptions
          rewoundHandle <- orThrow "rewind" =<< rewindWorkflowInWorkflow wctx t3 (defaultRewindOptions {rewindStartStep = 0})
          deleted <- orThrow "delete" =<< deleteWorkflowsInWorkflow wctx [t4] False
          _ <- orThrow "delay" =<< setWorkflowDelayInWorkflow wctx t6 (DelayFor (secondsDuration 0))
          _ <- orThrow "tag" =<< updateWorkflowAttributesInWorkflow wctx t5 (Just "{\"a\":1}")
          _ <- orThrow "tag-merge" =<< mergeWorkflowAttributesInWorkflow wctx t5 "{\"b\":2}"
          listed <- orThrow "list" =<< listWorkflowsInWorkflow wctx (defaultWorkflowFilter {workflowFilterWorkflowIds = [t5text]})
          listedSteps <- orThrow "list-steps" =<< listWorkflowStepsInWorkflow wctx t5
          retrieved <- orThrow "retrieve" =<< awaitChild wctx (retrieveWorkflowInWorkflow wctx t5)
          retrievedOut <- decodeStored "retrieve" retrieved
          pure
            ( Right
                ( ([t | WorkflowId t <- cancelled], resumedHandle.workflowId, forkedHandle.workflowId, rewoundHandle.workflowId),
                  (fromIntegral deleted, length listed, [record.stepInfoName | record <- listedSteps], retrievedOut)
                )
            )
    victimRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "victim") victimBody >>= either (throwIO . userError . show) pure
    quickRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "quick") quickBody >>= either (throwIO . userError . show) pure
    rewindRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "rewindable") rewindBody >>= either (throwIO . userError . show) pure
    serveRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "serve") serveBody >>= either (throwIO . userError . show) pure
    _superRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "supervisor") superBody >>= either (throwIO . userError . show) pure
    exec <- fx.mfLaunch dbos
    let startTarget ref wid input = do
          started <- startWfRef exec ref (startOptionsDefault {startWorkflowId = Just wid}) input
          case started of
            Left err -> throwIO (userError (show err))
            Right handle -> pure handle
        awaitIntHandle handle = do
          outcome <- resultWf handle
          decodeStored "target" =<< orThrow "target" outcome
        awaitSummary wid = do
          handle <- retrieveWf dbos wid >>= either (throwIO . userError . show) pure
          outcome <- resultWf handle
          case outcome of
            Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError (([Text], Text, Text, Text), (Int, Int, [Text], Int)) of
              Right summary -> pure summary
              Left err -> throwIO (userError (show err))
            other -> throwIO (userError ("expected the supervisor's summary, got: " <> show other))
        runSupervisor = do
          ran <- runWf exec (newQualifiedWorkflowName "supervisor") sup (Just (encodeWorkflowInput ()))
          case ran of
            Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError (([Text], Text, Text, Text), (Int, Int, [Text], Int)) of
              Right summary -> pure summary
              Left err -> throwIO (userError (show err))
            other -> throwIO (userError ("expected the supervisor's summary, got: " <> show other))
        forksOf wanted = do
          -- Scoped to this fixture's app and filtered by forked_from in
          -- the query: a global list would also parse every other test's
          -- rows, including fixture litter with statuses the reader
          -- refuses.
          rows <- listWorkflows dbos (defaultWorkflowFilter {workflowFilterAppScope = Named [fx.mfIdentity.identityAppName], workflowFilterForkedFrom = [wanted]}) >>= either (throwIO . userError . show) pure
          pure (sort [t | WorkflowId t <- (.workflowRecordId) <$> rows])
        rowMark wid = do
          found <- fx.mfReadRow wid
          pure (found.workflowRecordStatus, found.workflowRecordUpdatedAt)
    h1 <- startTarget victimRef t1 (Just (encodeWorkflowInput ()))
    waitCount entered 1 "the victim never started"
    h2 <- startTarget quickRef t2 (Just (encodeWorkflowInput ()))
    h4 <- startTarget quickRef t4 (Just (encodeWorkflowInput ()))
    h5 <- startTarget serveRef t5 (Just (encodeWorkflowInput ()))
    h3 <- startTarget rewindRef t3 (Just (encodeWorkflowInput ()))
    _ <- awaitIntHandle h2
    _ <- awaitIntHandle h4
    _ <- awaitIntHandle h5
    _ <- awaitIntHandle h3
    let QueueName internal = internalQueueName
    h6 <- do
      started <- startWfRef exec quickRef (startOptionsDefault {startWorkflowId = Just t6, startQueue = Just ((queueRequestNew internal) {delay = Just (secondsDuration 3600)})}) (Just (encodeWorkflowInput ()))
      case started of
        Left err -> throwIO (userError (show err))
        Right handle -> pure handle
    firstSummary <- runSupervisor
    let ((cancelled, resumedId, forked1, rewoundId), (deleted, listedCount, listedSteps, retrievedOut)) = firstSummary
    unless (cancelled == [t1text] && resumedId == t1text) $
      throwIO (userError ("the cancel/resume did not name the victim: " <> show firstSummary))
    unless (not (Text.null forked1) && forked1 /= t2text) $
      throwIO (userError ("the fork handed back no new id: " <> show firstSummary))
    unless (rewoundId == t3text) $
      throwIO (userError ("the rewind did not name its target: " <> show firstSummary))
    unless (deleted == 1 && listedCount == 1 && listedSteps == ["serve"] && retrievedOut == 7) $
      throwIO (userError ("the supervisor saw what it should not have: " <> show firstSummary))
    -- The resumed, forked, rewound and released rows run now instead of
    -- waiting for a supervisor tick that bare launches never fork, as the
    -- cancel-resume case does.
    _ <- dequeueWorkflows dbos >>= either (throwIO . userError . show) pure
    atomically (writeTVar released True)
    t1out <- awaitIntHandle h1
    forkOut <- awaitIntHandle =<< (retrieveWf dbos (WorkflowId forked1) >>= either (throwIO . userError . show) pure)
    t6out <- awaitIntHandle h6
    t3out <- awaitIntHandle h3
    count3 <- readTVarIO rewindRuns
    taggedRow <- fx.mfReadRow t5
    deletedRow <- getWorkflowRecord dbos t4 >>= either (throwIO . userError . show) pure
    deletedSteps <- listStepsWf dbos t4 >>= either (throwIO . userError . show) pure
    let deletedGone = case deletedRow of
          Nothing -> null deletedSteps
          _ -> False
    listed <- listStepsWf dbos sup >>= either (throwIO . userError . show) pure
    let steps = [record.stepInfoName | record <- listed]
    forksBefore <- forksOf t2text
    length (map (\t -> t `seq` t) forksBefore) `seq` pure ()
    rowBefore <- rowMark t3
    -- Past the last step, so the fork replays every call from its
    -- checkpoint instead of running one again.
    replayedIds <- forkFrom dbos [sup] (ForkStep (fromIntegral (length steps))) defaultForkOptions >>= either (throwIO . userError . show) pure
    replayId <- case replayedIds of
      [rid] -> pure rid
      _ -> throwIO (userError ("expected one replayed supervisor, got: " <> show replayedIds))
    _ <- dequeueWorkflows dbos >>= either (throwIO . userError . show) pure
    replayedSummary <- awaitSummary replayId
    forksAfter <- forksOf t2text
    length (map (\t -> t `seq` t) forksAfter) `seq` pure ()
    rowAfter <- rowMark t3
    runsAfter <- readTVarIO rewindRuns
    pure (firstSummary, replayedSummary, t1out, forkOut, t6out, t3out, count3, steps, forksBefore, forksAfter, rowBefore, rowAfter, runsAfter, deletedGone, taggedRow.workflowRecordAttributes)

-- | The free forms manage, the replay is stable, and the recorded steps
-- carry the methods' names.
checkSupervisorManagesFreely :: ((([Text], Text, Text, Text), (Int, Int, [Text], Int)), (([Text], Text, Text, Text), (Int, Int, [Text], Int)), Int, Int, Int, Int, Int, [Text], [Text], [Text], (WorkflowStatus, Timestamp), (WorkflowStatus, Timestamp), Int, Bool, Maybe Text) -> Either String ()
checkSupervisorManagesFreely (first, replayed, t1out, forkOut, t6out, t3out, count3, steps, forksBefore, forksAfter, rowBefore, rowAfter, runsAfter, deletedGone, attrs) = do
  checkEq first replayed
  checkEq 41 t1out
  checkEq 7 forkOut
  checkEq 7 t6out
  checkEq 9 t3out
  checkEq 2 count3
  checkEq count3 runsAfter
  checkEq
    [ "DBOS.cancelWorkflow",
      "DBOS.resumeWorkflow",
      "DBOS.forkWorkflow",
      "DBOS.rewindWorkflow",
      "DBOS.deleteWorkflow",
      "DBOS.setWorkflowDelay",
      "DBOS.updateWorkflowAttributes",
      "DBOS.updateWorkflowAttributes",
      "DBOS.listWorkflows",
      "DBOS.listWorkflowSteps",
      "DBOS.getResult"
    ]
    steps
  checkEq forksBefore forksAfter
  checkEq True (not (null forksBefore))
  checkEq rowBefore rowAfter
  checkEq True deletedGone
  case attrs of
    Just merged
      | "a" `Text.isInfixOf` merged && "b" `Text.isInfixOf` merged -> pure ()
      | otherwise -> Left ("the tags did not merge: " <> show merged)
    Nothing -> Left "the tags did not land"

-- | The tree cancel names the child, and the child is terminal.
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

-- | The default reads both payload columns; the metadata listing reads
-- neither, while ids, names, timings and tags stay.
checkListStepsWithoutOutputs :: ([StepInfo], [StepInfo]) -> Either String ()
checkListStepsWithoutOutputs (withOutputs, without) = do
  checkEq ["big", "fails"] (map (\record -> record.stepInfoName) withOutputs)
  checkEq ["big", "fails"] (map (\record -> record.stepInfoName) without)
  case withOutputs of
    [big, fails] -> do
      case big.stepInfoOutcome of
        StepOutcomeSucceeded (PayloadDecoded _) -> pure ()
        other -> Left ("expected the big step output, got: " <> show other)
      case fails.stepInfoOutcome of
        StepOutcomeFailed _ -> pure ()
        other -> Left ("expected the failing step error, got: " <> show other)
    _ -> Left "expected two steps with payloads"
  mapM_
    ( \record -> do
        checkEq StepOutcomeNotLoaded record.stepInfoOutcome
        checkEq True (record.stepInfoStartedAt /= Nothing)
        checkEq True (record.stepInfoCompletedAt /= Nothing)
    )
    without

-- | One page per (limit, offset), in step order.
checkListStepsPaging :: [[Text]] -> Either String ()
checkListStepsPaging =
  checkEq
    [ ["a", "b"],
      ["c", "d"],
      ["d", "e"],
      ["e"],
      []
    ]

-- | The listing sees the steps before it, is itself recorded after, and the
-- recorded snapshot carries the @loadOutput@ the read used.
checkListStepsInWorkflow :: ([Text], [Text], Text) -> Either String ()
checkListStepsInWorkflow (inside, outside, recorded) = do
  checkEq ["first"] inside
  checkEq ["first", "DBOS.listWorkflowSteps"] outside
  checkEq True ("[true," `Text.isPrefixOf` recorded)

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
stagedBody :: forall exec m. (MonadSTM m, MonadTime m, MonadMask m, MonadThread m)
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
scenarioForkFromBeginning :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                          => MgmtFixture m ->
                             m (Text, Int, Int)
scenarioForkFromBeginning fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    attempts <- newTVarIO (0 :: Int)
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "forkable"
        sourceText = "hs-l2-mgmt-fork-src-" <> suffix
        sourceId = WorkflowId sourceText
    refE <- registerWorkflowRef dbos key (forkableBody attempts)
    case refE of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    exec <- fx.mfLaunch dbos
    first <- runWf exec key sourceId (Just (encodeWorkflowInput (0 :: Int)))
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

-- | What the replacement-child scenario observed: the replacement child's
-- result and the parent fork that adopted it, the batch's results, the
-- step and child run counts, whether the un-mapped forks failed with the
-- original child, the three refusals' shape, and the refused fork's row.
data ForkReplacementFacts = ForkReplacementFacts
  { forkReplacementResult    :: Int,
    forkAdoptedResult        :: Int,
    forkBatchResults         :: [Int],
    forkBeforeRuns           :: Int,
    forkChildRuns            :: Int,
    forkPlainFailedWithChild :: Bool,
    forkUnrelatedFailed      :: Bool,
    forkRefusalsWereEngine   :: [Bool],
    forkRefusedStatus        :: Maybe WorkflowStatus
  }

-- | A fork can adopt a replacement for a child that failed: re-run the
-- failed child, then fork the parent from the step that awaits it, mapping
-- the old child to the new one. Neither the parent's earlier step nor
-- either child runs again; a fork without the map (or naming a child the
-- parent never started) waits on the original and fails with it; a map
-- naming one child twice, or either side empty, is refused before anything
-- is written. Mirrors Rust's @a_fork_adopts_the_replacement_for_a_failed_child@.
scenarioForkReplacesChild :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                          => MgmtFixture m ->
                             m ForkReplacementFacts
scenarioForkReplacesChild fx =
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    broken <- newTVarIO True
    beforeCount <- newTVarIO (0 :: Int)
    childCount <- newTVarIO (0 :: Int)
    let childKey = newQualifiedWorkflowName "replace-child"
        parentKey = newQualifiedWorkflowName "replace-parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = do
          atomically (modifyTVar childCount (+ 1))
          isBroken <- readTVarIO broken
          pure (if isBroken then Left (ErrorConfig "the child is broken") else Right 42)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r  -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          before <- runStep wctx "before" (const (atomically (modifyTVar beforeCount (+ 1)) >> pure (0 :: Int)))
          case before of
            Left err -> pure (Left err)
            Right _ -> do
              (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
                startChildWorkflow wctx childRef startOptionsDefault Nothing
              case started of
                Left err -> pure (Left err)
                Right wfHandle -> do
                  awaited <- awaitChild wctx wfHandle
                  pure $ case awaited of
                    Left err -> Left err
                    Right (Just stored) ->
                      case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                        Right value -> Right (value + 1)
                        Left err    -> Left (StepFailed "parent" (Text.pack (show err)))
                    Right Nothing -> Left (StepFailed "parent" "no child output")
    registered <- registerWorkflow dbos parentKey parentBody
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    let parent1Text = "hs-l2-mgmt-fork-replace-parent-" <> suffix
        parent2Text = "hs-l2-mgmt-fork-replace-other-" <> suffix
        replacementText = "hs-l2-mgmt-fork-replace-child-" <> suffix
        adoptedText = "hs-l2-mgmt-fork-replace-adopted-" <> suffix
        batchOneText = "hs-l2-mgmt-fork-replace-batch-one-" <> suffix
        batchTwoText = "hs-l2-mgmt-fork-replace-batch-two-" <> suffix
        plainText = "hs-l2-mgmt-fork-replace-plain-" <> suffix
        unrelatedText = "hs-l2-mgmt-fork-replace-unrelated-" <> suffix
        refusedText = "hs-l2-mgmt-fork-replace-refused-" <> suffix
        originalChild = parent1Text <> "-1"
        otherChild = parent2Text <> "-1"
        drive = do
          drove <- dequeueWorkflows dbos
          case drove of
            Left err -> throwIO (userError (show err))
            Right _  -> pure ()
        awaitInt wid = do
          waited <- waitForWorkflow dbos (WorkflowId wid)
          pure $ case waited of
            Right (SystemDB.AwaitedSucceeded (Just output) _) ->
              either (Left . show) Right (decodeWorkflowValue "result" (Just (SystemDB.SerializedWorkflowValue output Nothing)) :: Either CodecError Int)
            other -> Left (show other)
        forkAt source forked step options = do
          made <-
            forkWorkflows
              dbos
              [(forkNew source) {forkForkedId = Just forked, forkStartStep = step}]
              options
          case made of
            Left err -> throwIO (userError (show err))
            Right _  -> drive
        forkBeginning source forked options = forkAt source forked 0 options
        forkBatch pairs options = do
          made <-
            forkWorkflows
              dbos
              [(forkNew source) {forkForkedId = Just forked, forkStartStep = 2} | (source, forked) <- pairs]
              options
          case made of
            Left err -> throwIO (userError (show err))
            Right _  -> drive
        failedWithChild wid = do
          waited <- waitForWorkflow dbos (WorkflowId wid)
          pure $ case waited of
            Right (SystemDB.AwaitedFailed {awaitedError = detail}) -> "broken" `Text.isInfixOf` detail
            _                                                      -> False
    expectBroken =<< runWf exec parentKey (WorkflowId parent1Text) Nothing
    expectBroken =<< runWf exec parentKey (WorkflowId parent2Text) Nothing
    atomically (writeTVar broken False)
    -- The replacement child: the failed child forked from the beginning
    -- with the id the parent's map will name.
    _ <- forkBeginning originalChild replacementText defaultForkOptions
    replacementResult' <- awaitInt replacementText
    replacement <- case replacementResult' of
      Right n -> pure n
      Left detail -> throwIO (userError ("the replacement failed: " <> detail))
    -- The parent, forked from the step that awaits the child: the copied
    -- start names the replacement, so the re-run await finishes with it.
    let adoption = defaultForkOptions {forkOptionsReplacementChildren = [(originalChild, replacementText)]}
    _ <- forkAt parent1Text adoptedText 2 adoption
    adoptedResult <- awaitInt adoptedText
    adopted <- case adoptedResult of
      Right n -> pure n
      Left detail -> throwIO (userError ("the adopting fork failed: " <> detail))
    -- One map for a whole batch: each parent adopts the replacement for
    -- its own child.
    let both =
          defaultForkOptions
            { forkOptionsReplacementChildren = [(originalChild, replacementText), (otherChild, replacementText)]
            }
    _ <- forkBatch [(parent1Text, batchOneText), (parent2Text, batchTwoText)] both
    batchResults' <- traverse awaitInt [batchOneText, batchTwoText]
    batch <- case sequence batchResults' of
      Right results -> pure results
      Left detail   -> throwIO (userError ("a fork in the batch failed: " <> detail))
    -- Without the map, and with a map naming a child never started, the
    -- fork waits on the original child and fails with it.
    _ <- forkAt parent1Text plainText 2 defaultForkOptions
    _ <- forkAt
      parent1Text
      unrelatedText
      2
      defaultForkOptions {forkOptionsReplacementChildren = [("never-started", replacementText)]}
    plainFailed <- failedWithChild plainText
    unrelatedFailed <- failedWithChild unrelatedText
    -- A child named twice, or an empty id on either side, is refused
    -- before anything is forked.
    refusals <-
      forM
        [ [(originalChild, replacementText), (originalChild, otherChild)],
          [("", replacementText)],
          [(originalChild, "")]
        ]
        $ \children -> do
          result <-
            forkWorkflows
              dbos
              [(forkNew parent1Text) {forkForkedId = Just refusedText}]
              defaultForkOptions {forkOptionsReplacementChildren = children}
          pure $ case result of
            Left (InvalidArgument _ detail) -> "replacement_children" `Text.isInfixOf` detail
            _                               -> False
    refusedStatus <- getWorkflowStatus dbos (WorkflowId refusedText)
    beforeRuns' <- readTVarIO beforeCount
    childRuns' <- readTVarIO childCount
    pure
      ForkReplacementFacts
        { forkReplacementResult = replacement,
          forkAdoptedResult = adopted,
          forkBatchResults = batch,
          forkBeforeRuns = beforeRuns',
          forkChildRuns = childRuns',
          forkPlainFailedWithChild = plainFailed,
          forkUnrelatedFailed = unrelatedFailed,
          forkRefusalsWereEngine = refusals,
          forkRefusedStatus = refusedStatus
        }
  where
    expectBroken result = case result of
      Left err | "broken" `Text.isInfixOf` Text.pack (show err) -> pure ()
      other -> throwIO (userError ("expected the broken child to fail the parent, got: " <> show other))

-- | A fork takes the id it is given. Returns the fork's id, the queue its row
-- carries, and the fork's decoded result.
scenarioForkTakesIdAndQueue :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                            => MgmtFixture m ->
                               m (Text, Maybe Text, Int)
scenarioForkTakesIdAndQueue fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "placed"
        sourceText = "hs-l2-mgmt-fork-placed-src-" <> suffix
        forkedText = "hs-l2-mgmt-fork-placed-" <> suffix
        sourceId = WorkflowId sourceText
    refE <- registerWorkflowRef dbos key plusOneBody
    case refE of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    exec <- fx.mfLaunch dbos
    _ <- runWf exec key sourceId (Just (encodeWorkflowInput (3 :: Int)))
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

-- | A workflow's row carries the application that ran it, and a fork's row
-- inherits that owner rather than starting unclaimed. Mirrors Python's
-- @test_steps_carry_owner_and_forks_inherit_it@ at the row level.
scenarioForkInheritsOwner :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                          => MgmtFixture m ->
                             m (Maybe Text, Maybe Text, Text)
scenarioForkInheritsOwner fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "owned"
        sourceText = "hs-l2-mgmt-fork-owner-src-" <> suffix
        forkedText = "hs-l2-mgmt-fork-owner-" <> suffix
        sourceId = WorkflowId sourceText
    refE <- registerWorkflowRef dbos key plusOneBody
    case refE of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key sourceId (Just (encodeWorkflowInput (3 :: Int)))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    parentRow <- fx.mfReadRow sourceId
    forked <-
      forkWorkflows
        dbos
        [(forkNew sourceText) {forkForkedId = Just forkedText}]
        defaultForkOptions
    case forked of
      Left err -> throwIO (userError (show err))
      Right [WorkflowId text] | text == forkedText -> pure ()
      other -> throwIO (userError ("expected the chosen fork id, got: " <> show other))
    forkRow <- fx.mfReadRow (WorkflowId forkedText)
    pure
      ( parentRow.workflowRecordAppName,
        forkRow.workflowRecordAppName,
        fx.mfIdentity.identityAppName
      )

-- | Forking from a chosen step replays the steps below it: the fork runs only
-- the steps at or above the fork point. Returns the fork's step names.
scenarioForkFromStep :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                     => MgmtFixture m ->
                        m [Text]
scenarioForkFromStep fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    ran <- newTVarIO ([] :: [Text])
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "staged"
        sourceText = "hs-l2-mgmt-fork-step-src-" <> suffix
        sourceId = WorkflowId sourceText
    refE <- registerWorkflowRef dbos key (stagedBody ran)
    case refE of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    exec <- fx.mfLaunch dbos
    first <- runWf exec key sourceId (Just (encodeWorkflowInput (0 :: Int)))
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
scenarioForkFromFailure :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                        => MgmtFixture m ->
                           m (Text, Text, Int)
scenarioForkFromFailure fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    calls <- newTVarIO (0 :: Int)
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "flaky"
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
    first <- runWf exec key sourceId (Just (encodeWorkflowInput (0 :: Int)))
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

-- | The replacement ran with the child's code, the adopting fork finished
-- with its result, the batch adopted both parents' children, and no parent
-- step or child ran again. The un-mapped forks waited on the original and
-- failed with it; the three bad maps were refused on the engine channel
-- with nothing written.
checkForkReplacesChild :: ForkReplacementFacts -> Either String ()
checkForkReplacesChild facts = do
  checkEq 42 facts.forkReplacementResult
  checkEq 43 facts.forkAdoptedResult
  checkEq [43, 43] facts.forkBatchResults
  checkEq 2 facts.forkBeforeRuns
  checkEq 3 facts.forkChildRuns
  checkEq True facts.forkPlainFailedWithChild
  checkEq True facts.forkUnrelatedFailed
  checkEq [True, True, True] facts.forkRefusalsWereEngine
  checkEq Nothing facts.forkRefusedStatus

-- | The fork carries the id it was given, restarts on the internal queue, and
-- runs the workflow's body.
checkForkTakesIdAndQueue :: (Text, Maybe Text, Int) -> Either String ()
checkForkTakesIdAndQueue (forkedText, queue, decoded) = do
  checkEq True (not (Text.null forkedText))
  checkEq (Just "_dbos_internal_queue") queue
  checkEq 4 decoded

-- | The source and its fork are both owned by the launching application, and
-- the fork inherits the owner instead of starting unclaimed.
checkForkInheritsOwner :: (Maybe Text, Maybe Text, Text) -> Either String ()
checkForkInheritsOwner (parentOwner, forkOwner, appName) = do
  checkEq (Just appName) parentOwner
  checkEq (Just appName) forkOwner
  checkEq parentOwner forkOwner

-- | Bulk cancel and resume hand back every id, in any order: two queued runs
-- are cancelled and resumed, and both ids come back from each call. Returns
-- the two source texts with the cancelled and resumed ids.
scenarioBulkCancelResume :: forall m. (MonadCatch m, MonadMonotonicTimeNSec m, MonadDelay m, MonadAsync m, MonadFork m, MonadMVar m)
                         => MgmtFixture m ->
                            m (Text, Text, [Text], [Text])
scenarioBulkCancelResume fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "queued"
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
          enqueued <- enqueueWorkflow dbos key wid (Just (encodeWorkflowInput ("hello" :: Text))) ("bulk-" <> suffix)
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
scenarioBulkFork :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                 => MgmtFixture m ->
                    m (Text, Text, [Text], [Int])
scenarioBulkFork fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "doubling"
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
          ran <- runWf exec key wid (Just (encodeWorkflowInput input))
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
scenarioForkPartitioned :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                        => MgmtFixture m ->
                           m (Maybe Text)
scenarioForkPartitioned fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "queued"
        sourceId = WorkflowId ("hs-l2-mgmt-fork-key-src-" <> suffix)
    regE <- registerWorkflow dbos key echoTextBody
    case regE of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    exec <- fx.mfLaunch dbos
    _ <- runWf exec key sourceId (Just (encodeWorkflowInput ("hello" :: Text)))
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
scenarioForkRefusals :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m)
                     => MgmtFixture m -> m ( Either (Error EngineOnly) [WorkflowId], Either (Error EngineOnly) [WorkflowId], Either (Error EngineOnly) [WorkflowId], Either (Error EngineOnly) [WorkflowId], Either (Error EngineOnly) [WorkflowId], Bool
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
      checkEq "fork a workflow" operation
      checkEq "Fork.original_id: must not be empty" detail
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  case negativeStep of
    Left (InvalidArgument operation detail) -> do
      checkEq "fork from workflows" operation
      checkEq "Fork.start_step of `hs-l2-mgmt-fork-negative`: must not be negative, got -1" detail
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  case emptyQueue of
    Left (InvalidArgument operation detail) -> do
      checkEq "fork a workflow" operation
      checkEq "ForkOptions.queue_name: must be absent rather than empty" detail
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  checkEq (Right []) emptyForks
  checkEq (Right []) emptyIds
  checkEq False written

-- | An invalid batch is refused up front with the fork named, and no
-- fork is written: one forked id on two forks, a forked id that is an
-- original, and a timeout too long to store. The refusal precedes any
-- read, so the sources need not exist; the pinned id's absence
-- afterwards proves nothing was written. Mirrors the oracle's bulk
-- refusal twin.
scenarioForkBatchRefusals :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m)
                          => MgmtFixture m -> m ( Either (Error EngineOnly) [WorkflowId], Either (Error EngineOnly) [WorkflowId], Either (Error EngineOnly) [WorkflowId], Text, Text, Bool
                                               )
scenarioForkBatchRefusals fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let first = "hs-l2-mgmt-fork-batch-a-" <> suffix
        second = "hs-l2-mgmt-fork-batch-b-" <> suffix
        duped = "hs-l2-mgmt-fork-dup-" <> suffix
    _ <- fx.mfLaunch dbos
    duplicated <- forkWorkflows dbos [Fork first (Just duped) 0, Fork second (Just duped) 0] defaultForkOptions
    collided <- forkWorkflows dbos [Fork first Nothing 0, Fork second (Just first) 0] defaultForkOptions
    huge <- case durationFromSecs 1e18 of
      Nothing -> error "a storable-busting duration must build"
      Just duration -> forkWorkflows dbos [Fork first Nothing 0] (defaultForkOptions {forkOptionsTimeout = Just duration})
    found <- runSystemDB fx.mfSystemDB (\db -> SystemDB.getWorkflow db (WorkflowId duped))
    let written = case found of
          Right (Just _) -> True
          _ -> False
    pure (duplicated, collided, huge, duped, first, written)

-- | Each refusal arrives on the engine channel naming the fork, and the
-- pinned fork was never written.
checkForkBatchRefusals :: ( Either (Error EngineOnly) [WorkflowId],
                            Either (Error EngineOnly) [WorkflowId],
                            Either (Error EngineOnly) [WorkflowId],
                            Text,
                            Text,
                            Bool
                          ) -> Either String ()
checkForkBatchRefusals (duplicated, collided, huge, duped, first, written) = do
  case duplicated of
    Left (InvalidArgument operation detail) -> do
      checkEq "fork a workflow" operation
      checkMentioned ("`" <> duped <> "`") detail
      checkMentioned "is the forked id of more than one fork" detail
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  case collided of
    Left (InvalidArgument operation detail) -> do
      checkEq "fork a workflow" operation
      checkMentioned ("`" <> first <> "`") detail
      checkMentioned "is both a forked id and an original id" detail
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  case huge of
    Left (InvalidArgument operation detail) -> do
      checkEq "fork a workflow" operation
      checkMentioned "ForkOptions.timeout: must fit in milliseconds as a 64-bit integer" detail
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  checkEq False written
  where
    checkMentioned needle detail =
      unless (needle `Text.isInfixOf` detail) $ Left ("expected " <> show needle <> " in " <> show detail)

-- | A start's durations are refused on the start's channel: a plain
-- start's timeout to "start a workflow", and a queued start's delay and
-- timeout to "enqueue a workflow" naming the queue. Nothing runs, so the
-- bodies never need registering.
scenarioStartRefusals :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                      => MgmtFixture m -> m ( Either (Error EngineOnly) (),
                                             Either (Error EngineOnly) (),
                                             Either (Error EngineOnly) ()
                                           )
scenarioStartRefusals fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName ("refused-start-" <> Text.take 12 suffix)
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
        body () _ = pure (Right ())
        queueName = "hs-l2-mgmt-start-refused-q-" <> Text.take 12 suffix
    ref <- registerWorkflowRef dbos key body >>= either (error . show) pure
    executor <- fx.mfLaunch dbos
    huge <- case durationFromSecs 1e18 of
      Nothing -> error "a storable-busting duration must build"
      Just duration -> pure duration
    plain <- startWfRef executor ref (startOptionsDefault {startTimeout = Explicit huge}) Nothing
    queued <- startWfRef executor ref (startOptionsDefault {startTimeout = Explicit huge, startQueue = Just (queueRequestNew queueName)}) Nothing
    delayed <- startWfRef executor ref (startOptionsDefault {startQueue = Just ((queueRequestNew queueName) {delay = Just huge})}) Nothing
    pure (void plain, void queued, void delayed)

-- | Each refusal arrives as InvalidArgument on the start's own operation,
-- naming the queue for queued starts.
checkStartRefusals :: ( Either (Error EngineOnly) (),
                        Either (Error EngineOnly) (),
                        Either (Error EngineOnly) ()
                      ) -> Either String ()
checkStartRefusals (plain, queued, delayed) = do
  case plain of
    Left (InvalidArgument operation detail) -> do
      checkEq "start a workflow" operation
      checkMentioned "`timeout` must fit in milliseconds as a 64-bit integer" detail
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  case queued of
    Left (InvalidArgument operation detail) -> do
      checkEq "enqueue a workflow" operation
      checkMentioned "`timeout` must fit in milliseconds as a 64-bit integer" detail
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  case delayed of
    Left (InvalidArgument operation detail) -> do
      checkEq "enqueue a workflow" operation
      checkMentioned "`delay` must fit in milliseconds as a 64-bit integer" detail
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  where
    checkMentioned needle detail =
      unless (needle `Text.isInfixOf` detail) $ Left ("expected " <> show needle <> " in " <> show detail)

-- | A workflow that cancels and resumes itself is refused both, and
-- its row still succeeds: the refusals name the calling workflow.
scenarioSelfCancelResume :: forall m. (MonadFork m, MonadAsync m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                         => MgmtFixture m -> m ((Text, Text), WorkflowStatus)
scenarioSelfCancelResume fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "self-targeting"
        wid = WorkflowId ("hs-l2-mgmt-self-target-" <> suffix)
        body :: forall exec. Text -> WorkflowCtx exec m -> m (Either (Error EngineOnly) (Text, Text))
        body ownId wctx = do
          cancelled <- cancelWorkflowsInWorkflow wctx [WorkflowId ownId] False
          let cancelledText = case cancelled of
                Left err -> Text.pack (show err)
                Right _ -> "cancelled"
          resumed <- resumeWorkflowsInWorkflow wctx [WorkflowId ownId] Nothing
          let resumedText = case resumed of
                Left err -> Text.pack (show err)
                Right _ -> "resumed"
          pure (Right (cancelledText, resumedText))
    ref <- registerWorkflowRef dbos key body >>= either (error . show) pure
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowInput ("hs-l2-mgmt-self-target-" <> suffix :: Text)))
    pair <- case ran of
      Left err -> error (show err)
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError (Text, Text) of
        Left err -> error (show err)
        Right texts -> pure texts
      Right Nothing -> error "the self-targeting workflow returned no result"
    row <- fx.mfReadRow wid
    pure (pair, row.workflowRecordStatus)

-- | Both refusals name the calling workflow, and its row succeeds.
checkSelfCancelResume :: ((Text, Text), WorkflowStatus) -> Either String ()
checkSelfCancelResume ((cancelled, resumed), status) = do
  unless ("cannot cancel itself" `Text.isInfixOf` cancelled) $
    Left ("expected the cancel refusal, got: " <> show cancelled)
  unless ("cannot resume itself" `Text.isInfixOf` resumed) $
    Left ("expected the resume refusal, got: " <> show resumed)
  checkEq Success status

-- | A workflow cannot delete itself: the refusal reaches the caller and
-- the row goes nowhere. Promoted from the live-only twin.
scenarioSelfDelete :: forall m. (MonadFork m, MonadAsync m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                   => MgmtFixture m -> m (Text, WorkflowStatus)
scenarioSelfDelete fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "self-deleter"
        wid = WorkflowId ("hs-l2-mgmt-self-delete-" <> suffix)
        body :: forall exec. Text -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        body ownId wctx = do
          deleted <- deleteWorkflowsInWorkflow wctx [WorkflowId ownId] False
          pure (Right (Text.pack (show deleted)))
    ref <- registerWorkflowRef dbos key body >>= either (error . show) pure
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowInput ("hs-l2-mgmt-self-delete-" <> suffix :: Text)))
    outcome <- case ran of
      Left err -> error (show err)
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
        Left err -> error (show err)
        Right text -> pure text
      Right Nothing -> error "the self-deleting workflow returned no result"
    row <- fx.mfReadRow wid
    pure (outcome, row.workflowRecordStatus)

-- | The refusal reaches the caller and the row goes nowhere.
checkSelfDelete :: (Text, WorkflowStatus) -> Either String ()
checkSelfDelete (outcome, status) = do
  unless ("cannot delete itself" `Text.isInfixOf` outcome) $
    Left ("expected the delete refusal, got: " <> show outcome)
  checkEq Success status

-- | A child cannot delete the ancestor tree it runs inside: the refusal
-- reaches the parent through the child's handle, and the ancestor row
-- goes nowhere. Promoted from the live-only twin.
scenarioAncestorDelete :: forall m. (MonadFork m, MonadAsync m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                       => MgmtFixture m -> m (Text, WorkflowStatus)
scenarioAncestorDelete fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let childKey = newQualifiedWorkflowName "deleting-child"
        parentKey = newQualifiedWorkflowName "deleted-parent"
        rootText = "hs-l2-mgmt-ancestor-" <> suffix
        childBody :: forall exec. Text -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        childBody root wctx = do
          outcome <- deleteWorkflowsInWorkflow wctx [WorkflowId root] True
          pure (Right (Text.pack (show outcome)))
    childRef <- registerWorkflowRef dbos childKey childBody >>= either (error . show) pure
    let parentBody :: forall exec. Text -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        parentBody root wctx = do
          started <- startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowInput root))
          case started of
            Left err -> pure (Left err)
            Right child -> do
              awaited <- resultWf child
              case awaited of
                Left err -> pure (Left err)
                Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
                  Left err -> error (show err)
                  Right text -> pure (Right text)
                Right Nothing -> error "the child returned no result"
    _ <- registerWorkflow dbos parentKey parentBody >>= either (error . show) pure
    exec <- fx.mfLaunch dbos
    ran <- runWf exec parentKey (WorkflowId rootText) (Just (encodeWorkflowInput (rootText :: Text)))
    outcome <- case ran of
      Left err -> error (show err)
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
        Left err -> error (show err)
        Right text -> pure text
      Right Nothing -> error "the parent returned no result"
    root <- fx.mfReadRow (WorkflowId rootText)
    pure (outcome, root.workflowRecordStatus)

-- | The refusal reaches the parent and the ancestor row goes nowhere.
checkAncestorDelete :: (Text, WorkflowStatus) -> Either String ()
checkAncestorDelete (outcome, status) = do
  unless ("cannot delete itself" `Text.isInfixOf` outcome) $
    Left ("expected the delete refusal, got: " <> show outcome)
  checkEq Success status
-- | The fork's row carries the partition key it was given.
checkForkPartitioned :: Maybe Text -> Either String ()
checkForkPartitioned = checkEq (Just "pk-7")

-- | Attributes are replaced and can be searched: a full replacement matches a
-- one-key filter by containment, a narrower replacement drops the unseen key,
-- and clearing removes them. Returns the matching ids at each search and the
-- cleared row's attributes.
scenarioAttributes :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                   => MgmtFixture m ->
                      m (Text, [Text], [Text], Maybe Text)
scenarioAttributes fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "tagged"
        widText = "hs-l2-mgmt-attributes-" <> suffix
        wid = WorkflowId widText
        tenant = "acme-" <> suffix
        -- The tier carries the suffix too: the live fixture's backend holds no
        -- application name, so an attribute search over the shared database is
        -- unscoped, and a literal tier would match a parallel case's row.
        tier = "gold-" <> suffix
        full = "{\"tenant\":\"" <> tenant <> "\",\"tier\":\"" <> tier <> "\"}"
        tenantOnly = "{\"tenant\":\"" <> tenant <> "\"}"
        tierOnly = "{\"tier\":\"" <> tier <> "\"}"
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
scenarioAttributesInWorkflow :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                             => MgmtFixture m -> m (Maybe Text, Maybe Text, [Text])
scenarioAttributesInWorkflow fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let targetKey = newQualifiedWorkflowName "tagged-target"
        callerKey = newQualifiedWorkflowName "attributes-caller"
        targetText = "hs-l2-mgmt-attributes-target-" <> suffix
        callerText = "hs-l2-mgmt-attributes-caller-" <> suffix
        replacement = "{\"tenant\":\"acme-" <> suffix <> "\",\"tier\":\"gold-" <> suffix <> "\"}"
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
    ranTarget <- runWf exec targetKey (WorkflowId targetText) (Just (encodeWorkflowInput ()))
    case ranTarget of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    ranCaller <- runWf exec callerKey (WorkflowId callerText) (Just (encodeWorkflowInput targetText))
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
      Right records -> pure [record.stepInfoName | record <- records]
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
  checkEq ["DBOS.updateWorkflowAttributes"] names

-- | A merge adds keys and keeps the rest, from outside a workflow and from
-- inside one, where it is a step recorded under the same name as a
-- replacement. Mirrors the oracle's
-- @attributes_are_merged_from_outside_and_inside_a_workflow@.
scenarioMergeAttributes :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                        => MgmtFixture m -> m (Maybe Text, [Text])
scenarioMergeAttributes fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let targetKey = newQualifiedWorkflowName "merge-target"
        mergerKey = newQualifiedWorkflowName "attributes-merger"
        targetText = "hs-l2-mgmt-merge-target-" <> suffix
        mergerText = "hs-l2-mgmt-merge-caller-" <> suffix
        tenant = "acme-" <> suffix
        tags = "{\"tenant\":\"" <> tenant <> "\",\"tier\":\"gold-" <> suffix <> "\"}"
        tier = "{\"tier\":\"silver-" <> suffix <> "\"}"
        mergerBody :: forall exec. Text -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        mergerBody target wctx = do
          merged <- mergeWorkflowAttributesInWorkflow wctx (WorkflowId target) "{\"reviewed\":true}"
          case merged of
            Left err -> pure (Left err)
            Right () -> pure (Right target)
    targetRegistered <- registerWorkflow dbos targetKey zeroBody
    case targetRegistered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    mergerRegistered <- registerWorkflow dbos mergerKey mergerBody
    case mergerRegistered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ranTarget <- runWf exec targetKey (WorkflowId targetText) (Just (encodeWorkflowInput ()))
    case ranTarget of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    mergedTags <- mergeWorkflowAttributes dbos (WorkflowId targetText) tags
    case mergedTags of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    mergedTier <- mergeWorkflowAttributes dbos (WorkflowId targetText) tier
    case mergedTier of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    ranMerger <- runWf exec mergerKey (WorkflowId mergerText) (Just (encodeWorkflowInput targetText))
    decoded <- case ranMerger of
      Left err -> throwIO (userError (show err))
      Right Nothing -> throwIO (userError "expected the merger result")
      Right (Just output) ->
        case decodeWorkflowValue "result" (Just output) :: Either CodecError Text of
          Right value -> pure value
          Left err -> throwIO (userError (show err))
    unless (decoded == targetText) $ throwIO (userError ("expected the target id back, got: " <> show decoded))
    row <- fx.mfReadRow (WorkflowId targetText)
    listed <- listStepsWf dbos (WorkflowId mergerText)
    names <- case listed of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepInfoName | record <- records]
    pure (row.workflowRecordAttributes, names)

-- | The merges land together: the tenant from the first, the silver tier
-- from the second, the reviewed flag from inside the workflow — and the
-- inside merge took the replacement's step name.
checkMergeAttributes :: (Maybe Text, [Text]) -> Either String ()
checkMergeAttributes (stored, names) = do
  case stored of
    Just attributes
      | "tenant" `Text.isInfixOf` attributes && "silver" `Text.isInfixOf` attributes && "reviewed" `Text.isInfixOf` attributes -> pure ()
      | otherwise -> Left ("expected the merged attributes, got: " <> show stored)
    Nothing -> Left "expected the merged attributes, got nothing"
  checkEq ["DBOS.updateWorkflowAttributes"] names

-- | An attribute update from inside a workflow on an id no workflow has is
-- refused, and the refusal is the step's recorded outcome: a replay
-- refuses again even once a workflow with that id exists. Mirrors the
-- oracle's @an_attribute_update_on_a_missing_workflow_is_refused_and_replayed@.
scenarioMissingAttributesReplay :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                                 => MgmtFixture m -> m (Bool, [(Text, Bool)], Bool, Maybe Text)
scenarioMissingAttributesReplay fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let targetKey = newQualifiedWorkflowName "missing-target"
        taggerKey = newQualifiedWorkflowName "attributes-tagger"
        missingText = "hs-l2-mgmt-missing-attributes-" <> suffix
        taggerText = "hs-l2-mgmt-attributes-tagger-" <> suffix
        taggerBody :: forall exec. Text -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Bool)
        taggerBody target wctx = do
          -- The refusal is the answer this workflow reports, not a failure
          -- of its own.
          merged <- mergeWorkflowAttributesInWorkflow wctx (WorkflowId target) "{\"tagged\":true}"
          case merged of
            Left (SystemDatabase (SystemDB.NonExistentWorkflow {})) -> pure (Right True)
            Left err -> pure (Left err)
            Right () -> pure (Right False)
    targetRegistered <- registerWorkflow dbos targetKey zeroBody
    case targetRegistered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    taggerRegistered <- registerWorkflow dbos taggerKey taggerBody
    case taggerRegistered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ranTagger <- runWf exec taggerKey (WorkflowId taggerText) (Just (encodeWorkflowInput missingText))
    refused <- case ranTagger of
      Left err -> throwIO (userError (show err))
      Right Nothing -> throwIO (userError "expected the tagger result")
      Right (Just output) ->
        case decodeWorkflowValue "result" (Just output) :: Either CodecError Bool of
          Right value -> pure value
          Left err -> throwIO (userError (show err))
    listed <- listStepsWf dbos (WorkflowId taggerText)
    steps <- case listed of
      Left err -> throwIO (userError (show err))
      Right records -> pure [(record.stepInfoName, isRecordedRefusal record.stepInfoOutcome) | record <- records]
    -- The id exists now, so only the record can answer a replay with the
    -- refusal.
    ranTarget <- runWf exec targetKey (WorkflowId missingText) (Just (encodeWorkflowInput ()))
    case ranTarget of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    forked <- forkWorkflows dbos [Fork taggerText Nothing 1] defaultForkOptions
    forkedText <- case forked of
      Left err -> throwIO (userError (show err))
      Right [WorkflowId text] -> pure text
      other -> throwIO (userError ("expected exactly one fork, got: " <> show other))
    drove <- dequeueWorkflows dbos
    case drove of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    waited <- waitForWorkflow dbos (WorkflowId forkedText)
    replayed <- case waited of
      Right (SystemDB.AwaitedSucceeded (Just output) _) ->
        case decodeWorkflowValue "result" (Just (SystemDB.SerializedWorkflowValue output Nothing)) :: Either CodecError Bool of
          Right value -> pure value
          Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the fork to replay the refusal, got: " <> show other))
    row <- fx.mfReadRow (WorkflowId missingText)
    pure (refused, steps, replayed, row.workflowRecordAttributes)
  where
    isRecordedRefusal outcome = case outcome of
      StepOutcomeFailed _ -> True
      _ -> False

-- | The first run refused, the refusal is the recorded step outcome, the
-- replay refused again without writing, and the target carries no
-- attributes.
checkMissingAttributesReplay :: (Bool, [(Text, Bool)], Bool, Maybe Text) -> Either String ()
checkMissingAttributesReplay (refused, steps, replayed, attributes) = do
  checkEq True refused
  checkEq [("DBOS.updateWorkflowAttributes", True)] steps
  checkEq True replayed
  checkEq Nothing attributes

-- | The workflow waited as delayed, then ran its body on release.
checkDelayRelease :: (Maybe WorkflowStatus, Int) -> Either String ()
checkDelayRelease = checkEq (Just Delayed, 0)

-- | A moved delay reports what the database's own check decides: a delay
-- too long to be milliseconds at all is refused as an argument refusal to
-- this operation, while one that fits is accepted — even the largest
-- storable milliseconds, which saturate at the end of time instead of
-- refusing, where the old code refused depending on the clock. A zero
-- delay on the queued row is accepted and changes nothing. Mirrors Rust
-- @a_client_lists_tags_and_delays_workflows@'s delay tail. Returns the
-- zero move, the refusal, the saturating move, and the row status after
-- all three.
scenarioSetDelayRefusal :: forall m. (MonadMonotonicTimeNSec m, MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m)
                        => MgmtFixture m ->
                           m (Either (Error EngineOnly) (), Either (Error EngineOnly) (), Either (Error EngineOnly) (), Maybe WorkflowStatus)
scenarioSetDelayRefusal fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    _ <- fx.mfLaunch dbos
    let wid = WorkflowId ("hs-l2-mgmt-set-delay-" <> suffix)
        WorkflowId widText = wid
        queued = (newWorkflow widText) {newWorkflowQueueName = Just "set-delay-q"}
    planted <- runSystemDB fx.mfSystemDB (\db -> SystemDB.initWorkflow db queued Nothing WorkflowIdReturnExisting)
    case planted of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    zero <- setWorkflowDelay dbos wid (DelayFor (secondsDuration 0))
    huge <- case durationFromSecs 1e16 of
      Nothing -> throwIO (userError "durationFromSecs 1e16 did not construct")
      Just duration -> pure duration
    refused <- setWorkflowDelay dbos wid (DelayFor huge)
    capped <- case durationFromMs maxBound of
      Nothing -> throwIO (userError "durationFromMs maxBound did not construct")
      Just duration -> pure duration
    saturated <- setWorkflowDelay dbos wid (DelayFor capped)
    row <- fx.mfReadRow wid
    pure (zero, refused, saturated, Just row.workflowRecordStatus)

-- | The zero move and the saturating move are accepted, the unstorable
-- delay is refused as an argument to this operation naming @`delay`@, and
-- the row is still queued.
checkSetDelayRefusal :: (Either (Error EngineOnly) (), Either (Error EngineOnly) (), Either (Error EngineOnly) (), Maybe WorkflowStatus) -> Either String ()
checkSetDelayRefusal (zero, refused, saturated, status) = do
  checkEq (Right ()) zero
  case refused of
    Left (InvalidArgument operation detail) -> do
      checkEq "delay a workflow" operation
      unless ("`delay`" `Text.isInfixOf` detail) $ Left ("expected the detail to name `delay`, got: " <> show detail)
    other -> Left ("expected an InvalidArgument refusal, got: " <> show other)
  checkEq (Right ()) saturated
  checkEq (Just Enqueued) status

-- * Rewind

-- | Two steps and a run counter: the rewind target that proves steps at
-- and past the cut run again.
rewindCountBody :: forall exec m. (MonadSTM m, MonadTime m, MonadMask m, MonadThread m)
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
rewindUndoBody :: forall exec m. (MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m, MonadThread m)
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
rewindTxBody :: forall exec m. (MonadSTM m, MonadTime m, MonadDelay m, MonadMask m, HasCallStack, MonadThread m)
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
txCountBody :: forall exec m. (MonadSTM m, MonadTime m, MonadDelay m, MonadMask m, HasCallStack, MonadThread m)
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
adoptParentBody ::  forall exec m. (MonadAsync m, MonadTime m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m)
                => WorkflowRef m EngineOnly ->
                   StrictTVar m Int ->
                   () ->
                   WorkflowCtx exec m ->
                   m (Either (Error EngineOnly) Int)
adoptParentBody childRef ran () wctx = do
  atomically (modifyTVar ran (+ 1))
  started <- startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowInput (21 :: Int)))
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
rewindRefusedBody :: forall exec m. (MonadAsync m, MonadDelay m)
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
scenarioRewindRunsAgain :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                        => MgmtFixture m ->
                           m (WorkflowStatus, Maybe Text, Int64, Maybe Text, [Int], Int, Int)
scenarioRewindRunsAgain fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    ran <- newTVarIO (0 :: Int)
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "rewindable"
        wid = WorkflowId ("hs-l2-mgmt-rewind-runs-" <> suffix)
    registered <- registerWorkflow dbos key (rewindCountBody ran)
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    first <- runWf exec key wid (Just (encodeWorkflowInput (0 :: Int)))
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
      Right records -> pure [record.stepInfoId | record <- records]
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
scenarioDelayRelease :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                     => MgmtFixture m ->
                        m (Maybe WorkflowStatus, Int)
scenarioDelayRelease fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "delayable"
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
        (startOptionsDefault {startWorkflowId = Just wid, startQueue = Just ((queueRequestNew internal) {delay = Just (secondsDuration 3600)})})
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
scenarioDelayReleaseInWorkflow :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                               => MgmtFixture m -> m (Text, Int, [Text])
scenarioDelayReleaseInWorkflow fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let targetKey = newQualifiedWorkflowName "delayable-target"
        callerKey = newQualifiedWorkflowName "delay-caller"
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
        (startOptionsDefault {startWorkflowId = Just (WorkflowId targetText), startQueue = Just ((queueRequestNew internal) {delay = Just (secondsDuration 3600)})})
        Nothing
    case started of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    ranCaller <- runWf exec callerKey (WorkflowId callerText) (Just (encodeWorkflowInput targetText))
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
      Right records -> pure [record.stepInfoName | record <- records]
    pure (decoded, released, names)

-- | The caller hands back the target id, the target ran, and the caller's
-- move took its framework step.
checkDelayReleaseInWorkflow :: (Text, Int, [Text]) -> Either String ()
checkDelayReleaseInWorkflow (decoded, released, names) = do
  checkEq True ("hs-l2-mgmt-delay-target-" `Text.isPrefixOf` decoded)
  checkEq 0 released
  checkEq ["DBOS.setWorkflowDelay"] names

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
scenarioRewindRefusals :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
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
    let key = newQualifiedWorkflowName "rewindable"
        refKey = newQualifiedWorkflowName "rewindable-ref"
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
    ran <- runWf exec key finished (Just (encodeWorkflowInput ()))
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
        (startOptionsDefault {startWorkflowId = Just active, startQueue = Just (queueRequestNew "no-runner-here")})
        (Just (encodeWorkflowInput ()))
    case started of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    refused <- rewindWorkflows dbos active (defaultRewindOptions {rewindStartStep = 0})
    row <- fx.mfReadRow active
    listed <- listStepsWf dbos active
    stepIds <- case listed of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepInfoId | record <- records]
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
undoAt :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
       => MgmtFixture m ->
          Int ->
          m ([Text], Maybe Text, [Text], [Int], WorkflowStatus)
undoAt fx cut = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "rewind-undo"
        widText = "hs-l2-mgmt-rewind-undo-" <> suffix
        wid = WorkflowId widText
    registered <- registerWorkflow dbos key rewindUndoBody
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowInput widText))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    rewound <- rewindWorkflows dbos wid (defaultRewindOptions {rewindStartStep = fromIntegral cut})
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
      Right records -> pure [record.stepInfoId | record <- records]
    row <- fx.mfReadRow wid
    pure (messages, eventHit, values, stepIds, row.workflowRecordStatus)

-- | Rewound at the first step, everything the run did is undone: no
-- message survives, the published key is gone, the close is lifted while
-- the entries stay, and only the first step stands.
scenarioRewindUndoCut :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
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
scenarioRewindKeepBelowCut :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
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

-- | A body that writes one stream entry and then closes the stream. The close
-- is a step, so whether its sentinel is the discarded run's to undo depends on
-- where the cut falls.
streamCloseBody :: forall exec m. (MonadSTM m, MonadThread m)
                => () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
streamCloseBody () wctx = do
  wrote <- writeStream wctx "out" ("v" :: Text)
  case wrote of
    Left err -> pure (Left err)
    Right () -> do
      closed <- closeStream wctx "out"
      pure $ case closed of
        Left err -> Left err
        Right () -> Right "done"

-- | Rewound above the close, the close's step survives the cut, so the
-- sentinel stands with the entry and a reader meeting it still ends. Mirrors
-- the third cut in Python's @test_rewind_reopens_a_closed_stream@.
--
-- IO only: the management slice skips its sim half by decision (2026-10-18),
-- so this case is wired into 'ManagementTest' alone.
scenarioRewindKeepStreamClose :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                              => MgmtFixture m -> m [Text]
scenarioRewindKeepStreamClose fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "rewind-stream-close"
        widText = "hs-l2-mgmt-rewind-stream-close-" <> suffix
        wid = WorkflowId widText
    registered <- registerWorkflow dbos key streamCloseBody
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowInput ()))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    -- Written (step 0) and closed (step 1) are both below the cut.
    rewound <- rewindWorkflows dbos wid (defaultRewindOptions {rewindStartStep = 2})
    case rewound of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    entries <- runSystemDB fx.mfSystemDB (\db -> SystemDB.getAllStreamEntries db wid)
    case entries of
      Left err -> throwIO (userError (show err))
      Right rows -> pure [row.streamValue | row <- rows]

-- | The entry the run wrote stands, and so does the close's sentinel: the cut
-- fell above the close, so nothing replays it.
checkRewindKeepStreamClose :: [Text] -> Either String ()
checkRewindKeepStreamClose values =
  checkEq
    ( map
        (.serializedText)
        [ encodeWorkflowValue ("v" :: Text),
          SerializedWorkflowValue
            { serializedText = streamClosedSentinel,
              serializedSerialization = Just (Serialization portableJsonSerialization)
            }
        ]
    )
    values

-- | The rewind names the queue, partition key and version the re-run
-- gets: the row carries them, the counters clear, and the history from
-- the cut is gone. Mirrors Python's
-- @test_rewind_onto_a_queue_with_a_partition_key@.
scenarioRewindQueueVersion :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                           => MgmtFixture m -> m (WorkflowStatus, Maybe Text, Maybe Text, Maybe Text, Int64, [Int])
scenarioRewindQueueVersion fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "rewindable-qv"
        wid = WorkflowId ("hs-l2-mgmt-rewind-qv-" <> suffix)
    registered <- registerWorkflow dbos key zeroBody
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowInput ()))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    rewound <- rewindWorkflow dbos wid (defaultRewindOptions {rewindStartStep = 0, rewindQueueName = Just "custom-q", rewindQueuePartitionKey = Just "pk-1", rewindAppVersion = Just "v9"})
    case rewound of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    row <- fx.mfReadRow wid
    listed <- listStepsWf dbos wid
    stepIds <- case listed of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepInfoId | record <- records]
    pure
      ( row.workflowRecordStatus,
        row.workflowRecordQueueName,
        row.workflowRecordQueuePartitionKey,
        row.workflowRecordAppVersion,
        row.workflowRecordRecoveryAttempts,
        stepIds
      )

-- | The chosen queue, partition key and version land on the row; the cut
-- clears all steps.
checkRewindQueueVersion :: (WorkflowStatus, Maybe Text, Maybe Text, Maybe Text, Int64, [Int]) -> Either String ()
checkRewindQueueVersion (status, queue, partitionKey, version, attempts, stepIds) = do
  checkEq Enqueued status
  checkEq (Just "custom-q") queue
  checkEq (Just "pk-1") partitionKey
  checkEq (Just "v9") version
  checkEq 0 attempts
  checkEq [] stepIds

-- | A workflow rewinding another as its own step: the rewind runs once
-- under the caller's checkpoint. Returns the caller's decoded output, the
-- target's status, and the caller's recorded step names.
scenarioRewindInWorkflow :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                         => MgmtFixture m -> m (Text, WorkflowStatus, [Text])
scenarioRewindInWorkflow fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let targetKey = newQualifiedWorkflowName "rewind-target"
        callerKey = newQualifiedWorkflowName "rewind-caller"
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
    ranTarget <- runWf exec targetKey (WorkflowId targetText) (Just (encodeWorkflowInput ()))
    case ranTarget of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    ranCaller <- runWf exec callerKey (WorkflowId callerText) (Just (encodeWorkflowInput targetText))
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
      Right records -> pure [record.stepInfoName | record <- records]
    pure (decoded, target.workflowRecordStatus, names)

-- | The caller hands back the target id, the target is re-enqueued, and
-- the caller's rewind took its framework step.
checkRewindInWorkflow :: (Text, WorkflowStatus, [Text]) -> Either String ()
checkRewindInWorkflow (decoded, status, names) = do
  checkEq True ("hs-l2-mgmt-rewind-target-" `Text.isPrefixOf` decoded)
  checkEq Enqueued status
  checkEq ["DBOS.rewindWorkflow"] names

-- | The executor rewind drops the datasource checkpoints from the cut on:
-- the fake holds one checkpoint before the rewind and none after, while
-- the row is re-enqueued. Returns the checkpoint count before and after
-- with the row status.
scenarioRewindDropsDatasourceCheckpoints :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                                         => MgmtFixture m -> m (Int, Int, WorkflowStatus)
scenarioRewindDropsDatasourceCheckpoints fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    fake <- mkFakeDs
    registeredDs <- registerDataSource dbos fake.fakeSource
    case registeredDs of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    let key = newQualifiedWorkflowName "rewind-tx"
        wid = WorkflowId ("hs-l2-mgmt-rewind-tx-" <> suffix)
    registered <- registerWorkflow dbos key zeroBody
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowInput ()))
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
scenarioCompletionClearsCheckpoints :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m, HasCallStack)
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
    let directKey = newQualifiedWorkflowName "clearing-direct"
        drivenRefKey = newQualifiedWorkflowName "clearing-driven-ref"
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
    ranDirect <- runWf exec directKey directWid (Just (encodeWorkflowInput ()))
    case ranDirect of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    mid1 <- readTVarIO midDirect
    post1 <- Map.size <$> readTVarIO fake.fakeRows
    started <-
      startWfRef
        exec
        drivenRef
        (startOptionsDefault {startWorkflowId = Just drivenWid, startQueue = Just (queueRequestNew internal)})
        (Just (encodeWorkflowInput ()))
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
childClearParentBody ::  forall exec m. (MonadAsync m, MonadTime m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, HasCallStack)
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
scenarioChildClearsCheckpoints :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m, HasCallStack)
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
    let childKey = newQualifiedWorkflowName "clearing-child"
        parentKey = newQualifiedWorkflowName "clearing-parent"
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
    ran <- runWf exec parentKey parentWid (Just (encodeWorkflowInput ()))
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
scenarioRewindCallerStep :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                         => MgmtFixture m -> m (Bool, Bool, [Text], WorkflowStatus, Bool)
scenarioRewindCallerStep fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newQualifiedWorkflowName "rewind-caller-target"
        wid = WorkflowId ("hs-l2-mgmt-rewind-caller-" <> suffix)
    registered <- registerWorkflow dbos key zeroBody
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.mfLaunch dbos
    ran <- runWf exec key wid (Just (encodeWorkflowInput ()))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    -- A caller whose row is still owned: the target itself is completed
    -- and has no owner left, so the caller branch needs its own row.
    callerSuffix <- fx.mfFreshBase
    let callerText = "hs-l2-mgmt-rewind-caller-row-" <> callerSuffix
    callerCreated <- runSystemDB fx.mfSystemDB (\db -> SystemDB.initWorkflow db (newWorkflow callerText) Nothing WorkflowIdReturnExisting)
    callerToken <- case callerCreated of
      Left err -> throwIO (userError (show err))
      Right created -> pure created.initResultCreatorXid
    let callerKey step = Just (SystemDB.StepCaller (SystemDB.WorkflowOwner {ownerWorkflowId = WorkflowId callerText, ownerToken = callerToken}) step)
        direct = runSystemDB fx.mfSystemDB (\db -> SystemDB.rewindWorkflow db wid 0 (SysRewindOptions Nothing Nothing Nothing) (callerKey 0))
    first <- direct
    second <- direct
    invalid <- runSystemDB fx.mfSystemDB (\db -> SystemDB.rewindWorkflow db wid (-1) (SysRewindOptions Nothing Nothing Nothing) (callerKey 1))
    let invalidRefused = case invalid of
          Left (SystemDB.InvalidInput {}) -> True
          _ -> False
    listed <- listStepsWf dbos (WorkflowId callerText)
    names <- case listed of
      Left err -> throwIO (userError (show err))
      Right records -> pure [record.stepInfoName | record <- records]
    row <- fx.mfReadRow wid
    pure (first == Right (), second == Right (), names, row.workflowRecordStatus, invalidRefused)

-- | Both calls succeed, one step records the call, the row is queued, and
-- the invalid call refused without recording a second step.
checkRewindCallerStep :: (Bool, Bool, [Text], WorkflowStatus, Bool) -> Either String ()
checkRewindCallerStep (first, second, names, status, invalidRefused) = do
  checkEq True first
  checkEq True second
  checkEq ["DBOS.rewindWorkflow"] names
  checkEq Enqueued status
  checkEq True invalidRefused

-- | A rewound parent replays to its child and adopts the row already
-- there: the parent runs again, the child does not. Returns both decoded
-- outputs with the run counts.
scenarioRewindAdoptsChild :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                          => MgmtFixture m -> m (Int, Int, Int, Int)
scenarioRewindAdoptsChild fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    parentRan <- newTVarIO (0 :: Int)
    childRan <- newTVarIO (0 :: Int)
    let childKey = newQualifiedWorkflowName "adopted-child"
        parentKey = newQualifiedWorkflowName "adopting-parent"
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
    first <- runWf exec parentKey wid (Just (encodeWorkflowInput ()))
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
scenarioRewindRefusedTakesNoStep :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                                 => MgmtFixture m -> m (Text, [Text])
scenarioRewindRefusedTakesNoStep fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let targetKey = newQualifiedWorkflowName "rewind-target"
        callerKey = newQualifiedWorkflowName "refused-caller"
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
    ranTarget <- runWf exec targetKey (WorkflowId targetText) (Just (encodeWorkflowInput ()))
    case ranTarget of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    ranCaller <- runWf exec callerKey (WorkflowId callerText) (Just (encodeWorkflowInput targetText))
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
      Right records -> pure [record.stepInfoName | record <- records]
    pure (field, names)

-- | The refusal names the cut problem, and the caller recorded no step.
checkRewindRefusedTakesNoStep :: (Text, [Text]) -> Either String ()
checkRewindRefusedTakesNoStep (detail, names) = do
  checkEq "start_step must not be negative, got -3" detail
  checkEq [] names
