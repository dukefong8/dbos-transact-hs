{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Shared workflow scenarios: one body per case, judged by one pure
-- check on each stack, over the shared 'WfFixture'. The live tree
-- ('DBOS.Transact.WorkflowTest') runs them over Postgres with real
-- launches; the sim tree ('DBOS.Transact.WorkflowTestSim') over the
-- in-memory backend with the same engine calls.
module DBOS.Transact.WorkflowCases
  ( timeoutOptionsCase,
    startErrorCase,
    WfFixture (..),
    scenarioRegisteredRecordsResult,
    scenarioRunRefRejectsWithoutId,
    JoinOutcome (..),
    scenarioJoinTakesId,
    scenarioFreshJoinPolls,
    scenarioQueuedStartPolls,
    scenarioNonFinite,
    scenarioAwaitRecorded,
    scenarioStaleAwaitRefused,
    scenarioAwaitInsideStep,
    scenarioChildIdsInBuildOrder,
    scenarioStepIdPairs,
    scenarioScopedBody,
    scenarioScopedSelect,
    scenarioSelectStepRaces,
    scenarioControlSelect,
    scenarioLosingTokenFired,
    scenarioCancelledChildAwaited,
    scenarioDeadlineInherited,
    scenarioChildBudgetWins,
    scenarioDeclinedDeadline,
    scenarioCascadeDeadline,
    scenarioCaptureChildRefused,
    scenarioChildInsideStepRefused,
    scenarioLiftChildError,
    scenarioUnawaitedChild,
    scenarioFanout,
    scenarioRootNoParent,
    scenarioPlainStepAtStart,
    scenarioDerivedChildAdopted,
    scenarioAssignedChildAdopted,
    scenarioZeroNoInput,
    scenarioRowBeforeBody,
    scenarioPanic,
    scenarioRetrieveBeforeLaunch,
    scenarioAppErrorRoundtrip,
    scenarioDbFailureNotOutcome,
    scenarioStepsTaken,
    scenarioShutdownCancels,
    scenarioDropFuture,
    scenarioAttributes,
    scenarioStepErrorRecorded,
    scenarioBudgetCancels,
    scenarioWrongInstance,
    scenarioJoinHeldKey,
    scenarioEnqueuedChildReplays,
    DedupStartOutcome (..),
    scenarioDedupStartRecorded,
    scenarioQueuedStartClaimedByNextPass,
    ConflictStartOutcome (..),
    scenarioConflictStartRecorded,
    scenarioRejectStartRecorded,
    scenarioEnqueueByNameIsChild,
    scenarioEnqueueByNameWritesRow,
    scenarioEnqueueByNameDedupReplays,
    scenarioEnqueueByNameDeadlines,
    scenarioEnqueueByNameRefusedNoStep,
    waitForRowShared,
    checkRegisteredResult,
    checkRunRefRejectsWithoutId,
    checkJoinTakesId,
    checkFreshJoinPolls,
    checkQueuedStartPolls,
    checkNonFinite,
    checkAwaitRecorded,
    checkStaleAwaitRefused,
    checkAwaitInsideStep,
    checkChildIdsInBuildOrder,
    checkStepIdPairs,
    checkScopedBody,
    checkScopedSelect,
    checkSelectStepRaces,
    checkControlSelect,
    checkLosingTokenFired,
    checkCancelledChildAwaited,
    checkDeadlineInherited,
    checkChildBudgetWins,
    checkDeclinedDeadline,
    checkCascadeDeadline,
    checkCaptureChildRefused,
    checkChildInsideStepRefused,
    checkLiftChildError,
    checkUnawaitedChild,
    checkFanout,
    checkRootNoParent,
    checkPlainStepAtStart,
    checkDerivedChildAdopted,
    checkAssignedChildAdopted,
    checkZeroNoInput,
    checkRowBeforeBody,
    checkPanic,
    checkRunBeforeLaunch,
    checkAppErrorRoundtrip,
    checkDbFailureNotOutcome,
    checkStepsTaken,
    checkShutdownCancels,
    checkDropFuture,
    checkAttributes,
    checkStepErrorRecorded,
    checkBudgetCancels,
    checkWrongInstance,
    checkJoinHeldKey,
    checkEnqueuedChildReplays,
    checkDedupStartRecorded,
    checkQueuedStartClaimedByNextPass,
    checkConflictStartRecorded,
    checkRejectStartRecorded,
    checkEnqueueByNameIsChild,
    checkEnqueueByNameWritesRow,
    checkEnqueueByNameDedupReplays,
    checkEnqueueByNameDeadlines,
    checkEnqueueByNameRefusedNoStep,
    TaskCase,
    taskAbortAllWaits,
    taskFinishedNotRegistered,
    taskRefusedAfterSweep,
    taskEmptySweep,
    taskEarlyFinishNotSwept,
    taskFinishRacingSweep,
    taskWaitAllUntilDeadline,
    taskRunningNamesTasks,
    checkNoMiscounts,
    mkWfFixture,
    Refused (..),
    GaveUp (..)
  )
where

import DBOS.Prelude
import Data.Aeson (FromJSON (..), ToJSON (..), Value (..), eitherDecodeStrict, object, toJSON, (.=))
import Data.Aeson.KeyMap qualified as KeyMap
import Data.List (sort)
import Data.Text.Encoding (encodeUtf8)
import Data.Int (Int64)
import Data.Map.Strict qualified as Map
import Data.Text qualified as Text
import DBOS.SystemDB
  ( AppScope (..),
    AwaitedOutcome (..),
    Duration,
    Fork (..),
    ForkPoint (..),
    NewWorkflow (..),
    QueueName (..),
    StepRecord (..),
    WorkflowId (..),
    WorkflowIdReusePolicy (..),
    WorkflowInitResult (initResultCreatorXid),
    WorkflowOwner (..),
    WorkflowRecord (..),
    Timestamp (..),
    addTimeout,
    defaultForkOptions,
    forkNew,
    getWorkflow,
    internalQueueName,
    listSteps,
    newWorkflow
  )
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Error qualified as SysErr
import DBOS.Transact.Serialization (decodeWorkflowValue, encodeWorkflowInput, CodecError (..))
import DBOS.Transact
  (
    EngineOnly,
    Config (..),
    Error (..),
    RunOptions (..),
    Serialization (..),
    Serializer (..),
    SelectArm (..),
    SerializedWorkflowValue (..),
    StartOptions (..),
    Timeout (..),
    DBOS,
    WorkflowCtx,
    stepCtxCancellationToken,
    Executor,
    QueueRequest (..),
    DuplicationPolicy (..),
    QueueConflict (..),
    WorkflowStatus (..),
    awaitChild,
    defaultQueueOptions,
    defaultWorkflowFilter,
    WorkflowFilter (..),
    deleteWorkflows,
    EnqueueOptions (..),
    enqueueOptionsNew,
    enqueueWorkflowByName,
    enqueueWorkflowByNameInWorkflow,
    forkFrom,
    listWorkflows,
    deleteWorkflow,
    queueRequestNew,
    forkWorkflow,
    forkWorkflows,
    handleResult,
    handleStatus,
    deleteQueue,
    newDBOS,
    newQualifiedWorkflowName,
    registerWorkflowRef,
    registerWorkflow,
    registerQueue,
    retrieveWorkflow,
    runWorkflow,
    runWorkflowRef,
    runOptionsDefault,
    pendingAwait,
    pendingStep,
    runStep,
    runStepWith,
    sleepStep,
    startChildWorkflow,
    millisDuration,
    secondsDuration,
    selectStep,
    shutdown,
    startWorkflow,
    startOptionsDefault,
    stepOptionsDefault,
    waitForWorkflow,
  )
import DBOS.Transact.Logger (SomeTracer (..))
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Handle (Provenance (..), WorkflowHandle (..))
import DBOS.Transact.Error (decodeErrorText)
import DBOS.Transact.Instance (dequeueWorkflows, launchOn, launchOnWithQueues)
import DBOS.Transact.Workflow (resolveTimeoutDeadline, runOptionsToStartOptions, timeoutBudget)
import DBOS.Transact.Context
  ( firstStepStatus,
    nextWorkflowMarker,
    testOwner,
    withStep,
    nextWorkflowMarker,
    withStep,
    withSystemDB,
    withWorkflow,
    tokenCancelled,
    workflowId
  )
import DBOS.Transact.Workflow
  ( abortAll,
    childWorkflowId,
    isRecordedStartError,
    newTasks,
    recordedStartErrorNames,
    runningTasks,
    shouldRecordStartError,
    spawnTracked,
    waitAllUntil
  )
import DBOS.Transact.Connection
  ( Connection,
    Owner (..),
    SomeSystemDB (..),
    newConnection,
    runSystemDB
  )
import Test.Tasty.HUnit ((@?=))

-- | Timeout, option, and child-id composition without a database: pure
-- functions both trees judge identically, so one shared body covers both
-- leaves.
timeoutOptionsCase :: IO ()
timeoutOptionsCase = do
  let budget = secondsDuration 60
      now = Timestamp 1000
  timeoutBudget Inherit @?= Nothing
  timeoutBudget None @?= Nothing
  timeoutBudget (Explicit budget) @?= Just budget
  resolveTimeoutDeadline (Explicit budget) Nothing @?= Nothing
  resolveTimeoutDeadline (Explicit budget) (Just now) @?= Nothing
  resolveTimeoutDeadline None (Just now) @?= Nothing
  resolveTimeoutDeadline Inherit (Just now) @?= Just now
  resolveTimeoutDeadline Inherit Nothing @?= Nothing
  runOptionsDefault @?= RunOptions Nothing WorkflowIdReturnExisting Inherit Nothing
  startOptionsDefault @?= StartOptions Nothing WorkflowIdReturnExisting Inherit Nothing Nothing
  runOptionsToStartOptions runOptionsDefault @?= startOptionsDefault
  childWorkflowId (Just (WorkflowId "chosen")) (Just ("parent", 3)) "generated" @?= "chosen"
  childWorkflowId Nothing (Just ("parent", 0)) "generated" @?= "parent-0"
  childWorkflowId Nothing (Just ("parent", 2)) "generated" @?= "parent-2"
  childWorkflowId Nothing Nothing "generated" @?= "generated"

-- | The recorded start-error surface without a database: the four refusal
-- variants are recorded, the names are the recorded ones, and a value is
-- judged by its single-key shape before the typed read — so a plain step's
-- error is never mistaken for a refusal.
startErrorCase :: IO ()
startErrorCase = do
  recordedStartErrorNames @?= ["QueueDeduplicated", "ConflictingWorkflow", "WorkflowIdInUse", "InvalidInput"]
  shouldRecordStartError (SysErr.QueueDeduplicated {workflowId = "w", queueName = "q", deduplicationId = "d"}) @?= True
  shouldRecordStartError (SysErr.ConflictingWorkflow {workflowId = "w", detail = "taken"}) @?= True
  shouldRecordStartError (SysErr.WorkflowIdInUse {workflowId = "w", status = "SUCCESS", existingName = Just "charge"}) @?= True
  shouldRecordStartError (SysErr.InvalidInput {field = "queue_name", detail = "must be absent rather than empty"}) @?= True
  shouldRecordStartError (SysErr.WorkflowCancelled {workflowId = "w"}) @?= False
  shouldRecordStartError (SysErr.UnexpectedStep {workflowId = "w", stepId = 0, expected = "a child start", recorded = "a plain step"}) @?= False
  isRecordedStartError (object ["QueueDeduplicated" .= True]) @?= True
  isRecordedStartError (object ["ConflictingWorkflow" .= True]) @?= True
  isRecordedStartError (object ["WorkflowIdInUse" .= True]) @?= True
  isRecordedStartError (object ["InvalidInput" .= True]) @?= True
  isRecordedStartError (object ["WorkflowCancelled" .= True]) @?= False
  isRecordedStartError (object ["QueueDeduplicated" .= True, "extra" .= True]) @?= False
  isRecordedStartError (String "not an object") @?= False

-- * Shared workflow fixture: one body over any backend.

-- | How a tree instantiation builds its world: a fresh unlaunched
-- instance, the stack-specific launch, a fresh workflow id, and
-- backend-agnostic row and step reads. The tracer arrives as a
-- parameter — FastLogger on IO, the sim carrier on IOSim — and both
-- launches go through it over an explicitly built connection, so the
-- scenario drives the same engine calls on both stacks. Live fills the
-- rest with Postgres and per-test UUIDs; the sim tree with
-- 'MemSystemDB' and deterministic ids.
data WfFixture m = WfFixture
  { wfNewDBOS :: m (DBOS m),
    wfLaunch :: DBOS m -> m (Executor m),
    -- | 'wfLaunch' with an explicit listen set: a queue-driven scenario
    -- narrows the sweep to its own queue. Without it every pass polls
    -- every queue row in the shared database.
    wfLaunchWithQueues :: Maybe [Text] -> DBOS m -> m (Executor m),
    wfFreshId :: Text -> m WorkflowId,
    wfReadRow :: WorkflowId -> m (Maybe WorkflowRecord),
    wfListSteps :: WorkflowId -> m [StepRecord],
    wfChildren :: WorkflowId -> m [WorkflowId],
    -- | A second, separately launchable instance over the same backend:
    -- its own connection, registry, and identity (executor and version
    -- suffixed), for the cross-instance refusals.
    wfSecondInstance :: m (DBOS m, m (Executor m)),
    -- | The fixture's connection and application identity, for scenarios
    -- that build a workflow scope directly instead of through a runner.
    wfConn :: m (Connection m),
    wfIdentity :: Identity,
    -- | The backend the tree passed in, for scenarios that seed or read
    -- durable state directly (e.g. planting a stale await).
    wfSystemDB :: SomeSystemDB m
  }

-- | A registered workflow starts and records its result: the smallest
-- proof of the 'WfFixture' plumbing. Returns the decoded result and the
-- stored row. Engine errors throw (via 'MonadThrow'), so both trees
-- assert on plain values.
scenarioRegisteredRecordsResult :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                                => WfFixture m ->
                                   m (Int, WorkflowRecord)
scenarioRegisteredRecordsResult fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newQualifiedWorkflowName "double"
        -- Converted body: registered through the scoped entry and using the
        -- scoped step runner. The body takes WorkflowCtx and every call
        -- it makes takes that view or one derived from it.
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body value wctx = runStep wctx "double" (const (pure (value * 2)))
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "wf-double"
    (result :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec key wid (Just (encodeWorkflowInput (21 :: Int)))
    decoded <- case result of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the doubled result, got: " <> show other))
    row <- fx.wfReadRow wid
    case row of
      Just found -> pure (decoded, found)
      Nothing -> throwIO (userError "expected exactly one successful row")

-- | A run whose reuse policy names an id it was not given is refused on
-- the start's channel before the generated id is used or the body runs.
scenarioRunRefRejectsWithoutId :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                               => WfFixture m ->
                                  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Bool)
scenarioRunRefRejectsWithoutId fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    ran <- newTVarIO False
    let key = newQualifiedWorkflowName "ref-reject"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
        body () _ = atomically (writeTVar ran True) >> pure (Right ())
    ref <- registerWorkflowRef dbos key body >>= either (throwIO . userError . show) pure
    exec <- fx.wfLaunch dbos
    result <- runWorkflowRef exec ref (runOptionsDefault {runWorkflowIdReusePolicy = WorkflowIdReject}) Nothing
    executed <- readTVarIO ran
    pure (result, executed)

-- | One fixture builder over any backend: the tree passes its
-- 'SomeSystemDB' and 'SomeTracer' in (plus config, identity, connection
-- app name, id naming, and id/entropy generators), and the connection,
-- the launch, and the row reads all go through them. Live passes
-- Postgres + FastLogger; sim passes 'MemSystemDB' + the sim carrier.
-- Backend construction lives here once; per-side factories supply only
-- the atoms.
mkWfFixture :: forall m. (MonadMVar m, MonadSTM m, MonadThrow m)
            => Config ->
               Identity ->
               Text ->
               (Text -> WorkflowId) ->
               m Text ->
               m Word32 ->
               SomeSystemDB m ->
               SomeTracer m ->
               m (WfFixture m)
mkWfFixture config identity connApp nameScheme genId genEntropy sysdb tracer = do
  let mkConn = do
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
  conn <- mkConn
  secondConn <- mkConn
  let config2 =
        config
          { configAppVersion = (<> "-other") <$> config.configAppVersion,
            configExecutorId = (<> "-other") <$> config.configExecutorId
          }
      identity2 =
        identity
          { identityAppVersion = identity.identityAppVersion <> "-other",
            identityExecutorId = identity.identityExecutorId <> "-other"
          }
  pure
    WfFixture
      { wfNewDBOS = newDBOS config,
        wfLaunch = \dbos -> launchOn dbos conn identity,
        wfLaunchWithQueues = \listen dbos -> launchOnWithQueues dbos conn identity listen,
        wfFreshId = pure . nameScheme,
        wfReadRow = \wid -> do
          found <- runSystemDB sysdb (\db -> getWorkflow db wid)
          case found of
            Left err -> throwIO (userError (show err))
            Right row -> pure row,
        wfListSteps = \wid -> do
          listed <- runSystemDB sysdb (\db -> listSteps db wid True Nothing Nothing Nothing)
          case listed of
            Left err -> throwIO (userError (show err))
            Right steps -> pure steps,
        wfChildren = \wid -> do
          children <- runSystemDB sysdb (\db -> SystemDB.getWorkflowChildren db wid)
          case children of
            Left err -> throwIO (userError (show err))
            Right ids -> pure ids,
        wfSecondInstance = do
          dbos2 <- newDBOS config2
          pure (dbos2, launchOn dbos2 secondConn identity2),
        wfConn = pure conn,
        wfIdentity = identity,
        wfSystemDB = sysdb
      }

-- | What a joining double-start observes: both callers' decoded results,
-- how many times the body entered, the stored row's status, both handles'
-- workflow ids, and the first handle's status while the run still owns it.
-- A record (not a tuple): seven fields stay readable at both call sites.
data JoinOutcome = JoinOutcome
  { joinFirst :: Int,
    joinSecond :: Int,
    joinEntered :: Int,
    joinRowStatus :: WorkflowStatus,
    joinFirstId :: Text,
    joinSecondId :: Text,
    joinFirstPending :: WorkflowStatus
  }
  deriving stock (Eq, Show)

-- | A started id joined by a second start: one id, one execution. The
-- body parks on an MVar gate so the second start lands while the first
-- run still owns the id; the gate release lets both handles resolve.
-- Result waits are bounded (virtual time in sim, fifteen seconds live),
-- so a lost wakeup fails the case instead of hanging the suite.
scenarioJoinTakesId :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                    => WfFixture m ->
                       m JoinOutcome
scenarioJoinTakesId fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    entered <- newTVarIO (0 :: Int)
    release <- newEmptyMVar
    let key = newQualifiedWorkflowName "slow"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body () _ = do
          atomically (modifyTVar entered (+ 1))
          takeMVar release
          pure (Right 7)
    refE <- registerWorkflowRef dbos key body
    ref <- case refE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "join-start"
    let WorkflowId widText = wid
        startOpts = startOptionsDefault {startWorkflowId = Just (WorkflowId widText)}
    (firstE :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <- startWorkflow exec ref startOpts Nothing
    firstHandle <- case firstE of
      Left err -> throwIO (userError (show err))
      Right h -> pure h
    pendingE <- handleStatus firstHandle
    firstPending <- case pendingE of
      Right (Just status) -> pure status
      other -> throwIO (userError ("expected the started row PENDING: " <> show other))
    (secondE :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <- startWorkflow exec ref startOpts Nothing
    secondHandle <- case secondE of
      Left err -> throwIO (userError (show err))
      Right h -> pure h
    putMVar release ()
    first <- awaitSettled firstHandle
    second <- awaitSettled secondHandle
    count <- readTVarIO entered
    row <- fx.wfReadRow wid
    case (first, second, row) of
      (Right (Just firstStored), Right (Just secondStored), Just found) -> do
        let firstDecoded = decodeWorkflowValue "result" (Just firstStored) :: Either CodecError Int
            secondDecoded = decodeWorkflowValue "result" (Just secondStored) :: Either CodecError Int
        case (firstDecoded, secondDecoded) of
          (Right a, Right b) ->
            pure
              JoinOutcome
                { joinFirst = a,
                  joinSecond = b,
                  joinEntered = count,
                  joinRowStatus = found.workflowRecordStatus,
                  joinFirstId = firstHandle.workflowId,
                  joinSecondId = secondHandle.workflowId,
                  joinFirstPending = firstPending
                }
          other -> throwIO (userError ("expected both handles to resolve: " <> show other))
      other -> throwIO (userError ("expected both handles and one row: " <> show other))

-- | A bounded wait for a handle to settle: virtual time in sim, fifteen
-- seconds live. A lost wakeup fails the case instead of hanging the
-- suite. Shared by the scenarios that resolve handles.
awaitSettled :: forall m. (MonadMask m, MonadMVar m, MonadTimer m, MonadTime m)
             => WorkflowHandle m EngineOnly ->
                m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
awaitSettled handle = do
  (settled :: Maybe (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))) <- timeout 15000000 (handleResult handle)
  case settled of
    Just r -> pure r
    Nothing -> throwIO (userError "the handle never settled")

-- | A fresh start is local, a join polls: the first start runs the body
-- in-process, while a second start of the same id and a retrieve observe
-- it through polling handles. Returns the three provenance labels and the
-- decoded result.
scenarioFreshJoinPolls :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                       => WfFixture m ->
                          m (Text, Text, Text, Int)
scenarioFreshJoinPolls fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    release <- newEmptyMVar
    let key = newQualifiedWorkflowName "quick"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body () _ = takeMVar release >> pure (Right 7)
    refE <- registerWorkflowRef dbos key body
    ref <- case refE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "local-id"
    let WorkflowId widText = wid
        startOpts = startOptionsDefault {startWorkflowId = Just (WorkflowId widText)}
        label :: WorkflowHandle m EngineOnly -> Text
        label (WorkflowHandle _ _ provenance') = case provenance' of
          Local _ -> "local"
          Polling {} -> "polling"
    (firstE :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <- startWorkflow exec ref startOpts Nothing
    firstHandle <- case firstE of
      Left err -> throwIO (userError (show err))
      Right h -> pure h
    (joinE :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <- startWorkflow exec ref startOpts Nothing
    joinHandle <- case joinE of
      Left err -> throwIO (userError (show err))
      Right h -> pure h
    (retrieveE :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <- retrieveWorkflow dbos wid
    retrieveHandle <- case retrieveE of
      Left err -> throwIO (userError (show err))
      Right h -> pure h
    putMVar release ()
    result <- awaitSettled firstHandle
    decoded <- case result of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the local await to resolve, got: " <> show other))
    pure (label firstHandle, label joinHandle, label retrieveHandle, decoded)

-- | A queued row belongs to its queue: a plain start of the same id joins it
-- through a polling handle and must not execute the body, because only a
-- dequeue claim can move the row out of ENQUEUED and record an outcome.
-- Returns the second handle's provenance, the row's status, and the steps the
-- body would have taken.
scenarioQueuedStartPolls :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                         => WfFixture m ->
                            m (Text, WorkflowStatus, [StepRecord])
scenarioQueuedStartPolls fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newQualifiedWorkflowName "queued-start"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body () wctx = do
          stepped <- runStep wctx "ran" (const (pure ()))
          case stepped of
            Left err   -> pure (Left err)
            Right ()   -> pure (Right 7)
    registered <- registerWorkflowRef dbos key body
    ref <- case registered of
      Left err -> throwIO (userError (show err))
      Right r  -> pure r
    -- Listen only to a queue nobody uses, so the queued row below is never
    -- swept by this instance's supervisor.
    exec <- fx.wfLaunchWithQueues (Just ["queued-start-unwatched"]) dbos
    wid <- fx.wfFreshId "queued-start"
    let WorkflowId widText = wid
        queueName = "queued-start-q-" <> widText
        enqueueOpts = startOptionsDefault {startWorkflowId = Just (WorkflowId widText), startQueue = Just (queueRequestNew queueName)}
        joinOpts = startOptionsDefault {startWorkflowId = Just (WorkflowId widText)}
        label :: WorkflowHandle m EngineOnly -> Text
        label (WorkflowHandle _ _ provenance') = case provenance' of
          Local _   -> "local"
          Polling{} -> "polling"
    (enqueuedE :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <- startWorkflow exec ref enqueueOpts Nothing
    _ <- case enqueuedE of
      Left err -> throwIO (userError (show err))
      Right _  -> pure ()
    (joinedE :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <- startWorkflow exec ref joinOpts Nothing
    joined <- case joinedE of
      Left err -> throwIO (userError (show err))
      Right h  -> pure h
    row <- fx.wfReadRow wid
    steps <- fx.wfListSteps wid
    pure (label joined, maybe Pending (\r -> r.workflowRecordStatus) row, steps)

-- | Non-finite floats through the engine: a @-Infinity@ argument, a step-like
-- result holding @+Infinity@ and @NaN@. Returns the decoded tuple plus the
-- stored input/output text so the caller can pin aeson's spelling.
scenarioNonFinite :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                  => WfFixture m ->
                     m ((Double, Double, Double), Maybe Text, Maybe Text)
scenarioNonFinite fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newQualifiedWorkflowName "extremes"
        body :: forall exec. Double -> WorkflowCtx exec m -> m (Either (Error EngineOnly) (Double, Double, Double))
        body low _ = pure (Right (low, 1 / 0, 0 / 0))
    registered <- registerWorkflowRef dbos key body
    ref <- case registered of
      Left err -> throwIO (userError (show err))
      Right r  -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "non-finite"
    (startedE :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
      startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just wid}) (Just (encodeWorkflowInput (-1 / 0 :: Double)))
    started <- case startedE of
      Left err -> throwIO (userError (show err))
      Right h  -> pure h
    settled <- awaitSettled started
    decoded <- case settled of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError (Double, Double, Double) of
        Right triple -> pure triple
        Left err     -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the non-finite run to settle, got: " <> show other))
    row <- fx.wfReadRow wid
    pure (decoded, (.workflowRecordInput) =<< row, (.workflowRecordOutput) =<< row)

-- | Awaiting a child is recorded as a step: the parent starts the child
-- and awaits it, so the parent's history holds the start and a
-- @DBOS.get_result@ checkpoint naming the child. Returns the decoded
-- result, the parent's steps, and the derived child id.
scenarioAwaitRecorded :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                      => WfFixture m ->
                         m (Int, [StepRecord], Text)
scenarioAwaitRecorded fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 99)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
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
                    Right value -> Right value
                    Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                Right Nothing -> Left (StepFailed "parent" "no child output")
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "await-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey wid Nothing
    decoded <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the awaited child, got: " <> show other))
    steps <- fx.wfListSteps wid
    pure (decoded, steps, childText)

-- | Three children started and then awaited: the starts claim ids in
-- build order and the awaits follow in the same order, so the history is
-- starts 0-2 then awaits 3-5. Returns the summed result, the parent's
-- steps, the parent id, and the first-built child's recorded output.
scenarioChildIdsInBuildOrder :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                             => WfFixture m ->
                                m (Int, [StepRecord], Text, Maybe Text)
scenarioChildIdsInBuildOrder fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "parent"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody n _ = pure (Right n)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    -- The oracle drives the built starts through `join!` backwards; the
    -- port claims and waits at the call, so call order is build order and
    -- the rows are the contract both pin.
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: [Either (Error EngineOnly) (WorkflowHandle m EngineOnly)]) <-
            mapM (\n -> startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowInput (n :: Int)))) [1, 2, 3]
          case sequence started of
            Left err -> pure (Left err)
            Right handles -> do
              awaited <- mapM (awaitChild wctx) handles
              case sequence awaited of
                Left err -> pure (Left err)
                Right outputs -> case mapM (decodeWorkflowValue "result") outputs of
                  Left _ -> pure (Left (StepFailed "parent" "bad child output"))
                  Right (numbers :: [Int]) -> pure (Right (sum numbers))
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "order-parent"
    let WorkflowId parentText = wid
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey wid Nothing
    decoded <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the summed children, got: " <> show other))
    steps <- fx.wfListSteps wid
    firstChild <- fx.wfReadRow (WorkflowId (parentText <> "-0"))
    pure (decoded, steps, parentText, firstChild >>= (.workflowRecordOutput))

-- | One row's status, for scenarios that report it bare.
rowStatus :: Maybe WorkflowRecord -> Maybe WorkflowStatus
rowStatus = fmap (.workflowRecordStatus)

-- | A child started through another instance is refused: the reference
-- was registered on a second instance, so the running instance refuses
-- to start it before anything is written. Returns the run, the derived
-- row, and the parent's steps.
scenarioWrongInstance :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                      => WfFixture m ->
                         m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, [StepRecord], Text)
scenarioWrongInstance fx =
  bracket fx.wfNewDBOS shutdown $ \owner -> do
    (other, launchOther) <- fx.wfSecondInstance
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 1)
    childRefE <- registerWorkflowRef other childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
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
                    Right value -> Right value
                    Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                Right Nothing -> Left (StepFailed "parent" "no child output")
    parentReg <- registerWorkflow owner parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    _ <- launchOther
    exec <- fx.wfLaunch owner
    wid <- fx.wfFreshId "wrong-instance-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey wid Nothing
    missing <- fx.wfReadRow (WorkflowId childText)
    steps <- fx.wfListSteps wid
    shutdown other
    pure (ran, missing, steps, childText)

-- | A row that must appear, observed by polling the fixture's own read:
-- the row is written before the body is entered, so its presence means
-- the run is gated, not merely started. Shared by the shutdown and
-- dropped-future scenarios.
waitForRowShared :: forall m. (MonadDelay m, MonadThrow m)
                 => (WorkflowId -> m (Maybe WorkflowRecord)) ->
                    WorkflowId ->
                    m ()
waitForRowShared readRow wid = go (200 :: Int)
  where
    go 0 = throwIO (userError "the workflow row never appeared")
    go n = do
      row <- readRow wid
      case row of
        Just _ -> pure ()
        Nothing -> threadDelay 50000 >> go (n - 1)

-- | A workflow records the steps it took: two steps compose and list in
-- order. Returns the run and the steps.
scenarioStepsTaken :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                   => WfFixture m ->
                      m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord])
scenarioStepsTaken fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newQualifiedWorkflowName "two-steps"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body value wctx = do
          first <- runStep wctx "one" (const (pure (value + 1)))
          case first of
            Left err -> pure (Left err)
            Right stepped -> runStep wctx "two" (const (pure (stepped * 2)))
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "steps-listed-id"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runWorkflow exec key wid (Just (encodeWorkflowInput (21 :: Int)))
    steps <- fx.wfListSteps wid
    pure (ran, steps)

-- | Shutdown cancels a running workflow and leaves it pending: the run
-- is gated when the executor shuts down, the caller is cancelled, and
-- the row stays @PENDING@ for the next launch to recover. Returns the
-- row's status before and after.
scenarioShutdownCancels :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                        => WfFixture m ->
                           m (Maybe WorkflowStatus, Maybe WorkflowStatus)
scenarioShutdownCancels fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    gate <- newEmptyMVar
    let key = newQualifiedWorkflowName "gated"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body () _ = takeMVar gate >> pure (Right 7)
    refE <- registerWorkflowRef dbos key body
    ref <- case refE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "shutdown-run-id"
    worker <-
      async
        ( runWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just wid}) Nothing ::
            m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
        )
    waitForRowShared fx.wfReadRow wid
    before <- rowStatus <$> fx.wfReadRow wid
    shutdown dbos
    cancel worker
    after <- rowStatus <$> fx.wfReadRow wid
    pure (before, after)

-- | Dropping the future does not stop the workflow: cancelling the
-- caller leaves the row pending and the body still gated; released, the
-- run finishes on its own. Returns the gated row's status and the
-- settled outcome.
scenarioDropFuture :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                   => WfFixture m ->
                      m (Maybe WorkflowStatus, Either (Error EngineOnly) AwaitedOutcome)
scenarioDropFuture fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    gate <- newEmptyMVar
    let key = newQualifiedWorkflowName "gated"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body () _ = takeMVar gate >> pure (Right 7)
    refE <- registerWorkflowRef dbos key body
    ref <- case refE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "drop-future-id"
    worker <-
      async
        ( runWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just wid}) Nothing ::
            m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
        )
    waitForRowShared fx.wfReadRow wid
    -- Dropping the waiter stops the watching, not the workflow: the run
    -- is detached onto the executor, so cancelling the caller leaves the
    -- row pending and the body still gated.
    cancel worker
    gated <- rowStatus <$> fx.wfReadRow wid
    putMVar gate ()
    settled <- waitForWorkflow dbos wid
    pure (gated, settled)

-- | A started workflow carries the attributes it was given: the run's
-- attributes land on the parent's row, and the child, naming nothing of
-- its own, inherits nothing. Returns the run, both rows, and the tenant.
scenarioAttributes :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                   => WfFixture m ->
                      m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord, Text)
scenarioAttributes fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "attributed"
        tenant = "acme-tenant"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody _ _ = pure (Right 9)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowInput (0 :: Int)))
          case started of
            Left err -> pure (Left err)
            Right handle -> do
              result <- awaitChild wctx handle
              pure $ case result of
                Left err -> Left err
                Right (Just stored) ->
                  case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                    Right n -> Right n
                    Left _ -> Left (StepFailed "parent" "bad child output")
                Right _ -> Left (StepFailed "parent" "no child output")
    parentRefE <- registerWorkflowRef dbos parentKey parentBody
    parentRef <- case parentRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "attributes-parent"
    let WorkflowId parentText = wid
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runWorkflowRef exec parentRef (runOptionsDefault {runWorkflowId = Just (WorkflowId parentText), runAttributes = Just (Map.singleton "tenant" (String tenant))}) Nothing
    parentRow <- fx.wfReadRow wid
    childRow <- fx.wfReadRow (WorkflowId (parentText <> "-0"))
    pure (ran, parentRow, childRow, tenant)

-- | A step error is recorded in its column: the step fails, the run
-- returns the step error, and the column holds the shortfall. Returns
-- the run and the parent's steps.
scenarioStepErrorRecorded :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                          => WfFixture m ->
                             m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord])
scenarioStepErrorRecorded fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newQualifiedWorkflowName "charger"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body () wctx = runStepWith stepOptionsDefault wctx "charge" (const (pure (Left (StepFailed "charge" "short by 12"))))
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "step-err-id"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec key wid Nothing
    steps <- fx.wfListSteps wid
    pure (ran, steps)

-- | An application error round-trips as itself: the body's own failure
-- comes back unchanged through the engine. Returns the run.
scenarioAppErrorRoundtrip :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                          => WfFixture m ->
                             m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
scenarioAppErrorRoundtrip fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newQualifiedWorkflowName "flaky"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body _ _ = pure (Left (StepFailed "flaky" "boom"))
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "app-err-id"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runWorkflow exec key wid (Just (encodeWorkflowInput (21 :: Int)))
    pure ran

-- | A database failure is not the workflow outcome: the backend error
-- comes back as itself and the row is left pending with no error
-- column, so a later recovery can retry. Returns the run and the row.
scenarioDbFailureNotOutcome :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                            => WfFixture m ->
                               m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord)
scenarioDbFailureNotOutcome fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newQualifiedWorkflowName "blips"
        backendErr =
          SystemDB.Backend
            ( SystemDB.BackendError
                { SystemDB.backendMessage = "connection reset by peer",
                  SystemDB.backendSqlState = Nothing,
                  SystemDB.backendKind = SystemDB.Connection
                }
            )
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
        body () _ = pure (Left (SystemDatabase backendErr))
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "blip-id"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec key wid Nothing
    row <- fx.wfReadRow wid
    pure (ran, row)

-- | A panicking workflow leaves its row pending: the body's exception
-- escapes the run as itself, and the row stays @PENDING@ with no error
-- column. Returns the escaped outcome and the row.
scenarioPanic :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
              => WfFixture m ->
                 m (Either SomeException (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)), Maybe WorkflowRecord)
scenarioPanic fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newQualifiedWorkflowName "explodes"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
        body () _ = throwIO (userError "boom")
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "panic-id"
    outcome <- try (runWorkflow exec key wid Nothing)
    row <- fx.wfReadRow wid
    pure (outcome, row)

-- | Retrieving before launch is refused: the unlaunched instance has no
-- executor, naming the call. Returns the refusal. (Running before launch
-- moved to the type level: the runner takes the launch-produced
-- 'Executor', so that call is unconstructible and this runtime surface is
-- the remaining not-launched refusal.)
scenarioRetrieveBeforeLaunch :: forall m. (MonadMVar m)
                             => WfFixture m ->
                                m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
scenarioRetrieveBeforeLaunch fx = do
  dbos <- fx.wfNewDBOS
  wid <- fx.wfFreshId "unlaunched-id"
  retrieved <- (retrieveWorkflow dbos wid :: m (Either (Error EngineOnly) (WorkflowHandle m EngineOnly)))
  pure $ case retrieved of
    Left err -> Left err
    Right _ -> Right Nothing

-- | A zero-argument workflow records no input: the row's input column
-- stays null however the workflow ran. Returns the run and its row.
scenarioZeroNoInput :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                    => WfFixture m ->
                       m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord)
scenarioZeroNoInput fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newQualifiedWorkflowName "zero"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
        body () _ = pure (Right ())
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "zero-id"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec key wid Nothing
    row <- fx.wfReadRow wid
    pure (ran, row)

-- | The row exists before the body starts: the body reads its own row
-- through the context's database and finds it. Returns the run.
scenarioRowBeforeBody :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                      => WfFixture m ->
                         m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
scenarioRowBeforeBody fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newQualifiedWorkflowName "sees-itself"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Bool)
        body () wctx = do
          row <- withSystemDB wctx (\db -> SystemDB.getWorkflow db (WorkflowId (workflowId wctx)))
          pure (Right (case row of Right (Just _) -> True; _ -> False))
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "row-id"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec key wid Nothing
    pure ran

-- | A start position holding a plain step is refused: the parent parks
-- before its first step id is allocated, a plain step is planted at the
-- start position, and the start then finds output where a child link
-- should be. The refusal happens early, so nothing was created to be
-- orphaned. Returns the run and the derived row.
scenarioPlainStepAtStart :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                         => WfFixture m ->
                            m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord)
scenarioPlainStepAtStart fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "waiter"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody _ _ = pure (Right 1)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    gate <- newEmptyMVar
    entered <- newEmptyMVar
    let parentBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody _ wctx = do
          putMVar entered ()
          takeMVar gate
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowInput (1 :: Int)))
          case started of
            Left err -> pure (Left err)
            Right _ -> pure (Right 0)
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "stale-parent"
    let WorkflowId parentText = wid
        derivedText = parentText <> "-0"
    worker <-
      async
        ( runWorkflow exec parentKey wid (Just (encodeWorkflowInput (0 :: Int))) ::
            m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
        )
    enteredOk <- timeout 15000000 (takeMVar entered)
    case enteredOk of
      Nothing -> throwIO (userError "the parent never reached its gate")
      Just () -> pure ()
    planted <-
      runSystemDB fx.wfSystemDB $ \db -> do
        -- The plant lands while the run owns the id, so it presents the
        -- row's own creator token, as the execution's write would.
        found <- SystemDB.getWorkflow db wid
        creator <- case found of
          Right (Just row) -> case row.workflowRecordCreatorXid of
            Just token -> pure token
            Nothing -> throwIO (userError "planted write needs the row's creator")
          _ -> throwIO (userError "planted write needs the row")
        SystemDB.recordStep db wid 0 "child" (SystemDB.OutcomeOutput (Just "1")) Nothing Nothing creator
    case planted of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    putMVar gate ()
    outcome <- timeout 15000000 (wait worker)
    ran <- case outcome of
      Just r -> pure r
      Nothing -> throwIO (userError "the plain-step refusal never returned")
    missing <- fx.wfReadRow (WorkflowId derivedText)
    pure (ran, missing)

-- | A parent starts a child under a derived id and replay adopts it: the
-- child start detaches the child onto the executor, which runs it; a
-- second run of the parent adopts the recorded child id instead of
-- starting another. Returns the child id, the (empty) recovery and
-- dequeue results, the settled child, and the replayed parent's run.
scenarioDerivedChildAdopted :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                            => WfFixture m ->
                               m (Text, [WorkflowId], Either (Error EngineOnly) [WorkflowId], Either (Error EngineOnly) AwaitedOutcome, Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
scenarioDerivedChildAdopted fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "double"
        parentKey = newQualifiedWorkflowName "parent"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody value wctx = runStep wctx "double" (const (pure (value * 2)))
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        parentBody _ wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowInput (21 :: Int)))
          pure ((.workflowId) <$> started)
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "child-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (first :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey wid (Just (encodeWorkflowInput (0 :: Int)))
    _ <- case first of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
        Right _ -> pure ()
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the parent's child id, got: " <> show other))
    -- The child start detaches the child onto the executor; the replay
    -- adopts the recorded id instead of starting another.
    settled <- waitForWorkflow dbos (WorkflowId childText)
    (replayed :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey wid (Just (encodeWorkflowInput (0 :: Int)))
    pure (childText, [], Right [], settled, replayed)

-- | The assigned-id variant: the child was started under a chosen id, so
-- it runs under that id and no derived row ever exists. Returns the
-- chosen id, the (empty) recovery and dequeue results, the settled
-- child, the replayed parent's run, and both rows.
scenarioAssignedChildAdopted :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                             => WfFixture m ->
                                m (Text, [WorkflowId], Either (Error EngineOnly) [WorkflowId], Either (Error EngineOnly) AwaitedOutcome, Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord)
scenarioAssignedChildAdopted fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "namer"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody _ _ = pure (Right 7)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    wid <- fx.wfFreshId "assigned-parent"
    let WorkflowId parentText = wid
        chosenText = parentText <> "-chosen"
        derivedText = parentText <> "-0"
        parentBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        parentBody _ wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef (startOptionsDefault {startWorkflowId = Just (WorkflowId chosenText)}) (Just (encodeWorkflowInput (21 :: Int)))
          pure ((.workflowId) <$> started)
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    (first :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey wid (Just (encodeWorkflowInput (0 :: Int)))
    _ <- case first of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
        Right _ -> pure ()
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the parent's child id, got: " <> show other))
    settled <- waitForWorkflow dbos (WorkflowId chosenText)
    (replayed :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey wid (Just (encodeWorkflowInput (0 :: Int)))
    chosenRow <- fx.wfReadRow (WorkflowId chosenText)
    derivedRow <- fx.wfReadRow (WorkflowId derivedText)
    pure (chosenText, [], Right [], settled, replayed, chosenRow, derivedRow)

-- | A workflow started outside a workflow has no parent: a root run
-- records no parent link. Returns the run and its row.
scenarioRootNoParent :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                     => WfFixture m ->
                        m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord)
scenarioRootNoParent fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newQualifiedWorkflowName "root"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body () _ = pure (Right 1)
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "root-id"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec key wid Nothing
    row <- fx.wfReadRow wid
    pure (ran, row)

-- | Children started in a loop run concurrently: all three start before
-- any await, so their sleeps overlap and the whole fan-out takes about
-- one child's delay rather than three. Returns the summed result, the
-- children, and the elapsed milliseconds — measured on the wall clock
-- live, on the virtual clock in sim, where a serialized run would still
-- show three delays.
scenarioFanout :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
               => WfFixture m ->
                  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [WorkflowId], Int64)
scenarioFanout fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "fan"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody n _ = threadDelay 400000 >> pure (Right n)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    -- Start all three first, then collect: awaiting inside the first loop
    -- would serialize them.
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: [Either (Error EngineOnly) (WorkflowHandle m EngineOnly)]) <-
            mapM (\n -> startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowInput (n :: Int)))) [0, 1, 2]
          case sequence started of
            Left err -> pure (Left err)
            Right handles -> do
              results <- mapM (awaitChild wctx) handles
              case sequence results of
                Left err -> pure (Left err)
                Right outputs -> case mapM (decodeWorkflowValue "result") outputs of
                  Left _ -> pure (Left (StepFailed "fan" "bad child output"))
                  Right (numbers :: [Int]) -> pure (Right (sum numbers))
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "fanout-parent"
    began <- SystemDB.timestampNow
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey wid Nothing
    ended <- SystemDB.timestampNow
    children <- fx.wfChildren wid
    let tookMs = SystemDB.timestampToEpochMs ended - SystemDB.timestampToEpochMs began
    pure (ran, children, tookMs)

-- | A child started and never awaited is still recorded: the handle is
-- dropped, but the start row is what makes a child adoptable, and the
-- detached child outlives the parent's interest and records its result.
-- Returns the parent's run, its steps, the child's awaited outcome, the
-- child row, the parent's children, and the parent id.
scenarioUnawaitedChild :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                       => WfFixture m ->
                          m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord], Either (Error EngineOnly) AwaitedOutcome, Maybe WorkflowRecord, [WorkflowId], Text)
scenarioUnawaitedChild fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "forgetful"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody value wctx = runStep wctx "double" (const (pure (value * 2)))
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
        parentBody _ wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowInput (21 :: Int)))
          case started of
            Left err -> pure (Left err)
            Right _ -> pure (Right ())
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "unawaited-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runWorkflow exec parentKey wid (Just (encodeWorkflowInput (0 :: Int)))
    steps <- fx.wfListSteps wid
    found <- waitForWorkflow dbos (WorkflowId childText)
    childRow <- fx.wfReadRow (WorkflowId childText)
    children <- fx.wfChildren wid
    pure (ran, steps, found, childRow, children, parentText)

-- | A child that fails differently is started through lift: the child's
-- own error channel crosses the boundary as itself on its row, and the
-- parent reports the refused child through its own channel. Returns the
-- parent's run and the child's row.
scenarioLiftChildError :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                       => WfFixture m ->
                          m (Either (Error GaveUp) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord)
scenarioLiftChildError fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let shipKey = newQualifiedWorkflowName "ship"
        billKey = newQualifiedWorkflowName "bill"
        shipBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error Refused) ())
        shipBody () _ = pure (Left (ErrorApplication Refused))
    shipRefE <- registerWorkflowRef dbos shipKey shipBody
    shipRef <- case shipRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let billBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error GaveUp) Bool)
        billBody () wctx = do
          (started :: Either (Error GaveUp) (WorkflowHandle m Refused)) <-
            startChildWorkflow wctx shipRef startOptionsDefault Nothing
          case started of
            Left err -> pure (Left err)
            Right handle -> do
              awaited <- awaitChild wctx handle
              let refusedChild = case awaited of
                    Left (ErrorApplication Refused) -> True
                    _ -> False
              marker <- nextWorkflowMarker wctx
              (refusedStart :: Either (Error GaveUp) ()) <-
                withStep wctx marker (firstStepStatus 2) $ \_sctx -> do
                  inside <- startChildWorkflow wctx shipRef startOptionsDefault Nothing
                  pure (case inside of
                    Left err -> Left err
                    Right _ -> Right ())
              pure $ case refusedStart of
                Left (InsideStep _) -> Right refusedChild
                Left err -> Left err
                Right () -> Right False
    billRefE <- registerWorkflowRef dbos billKey billBody
    billRef <- case billRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    billWid <- fx.wfFreshId "lift-parent"
    let WorkflowId billText = billWid
        shipText = billText <> "-0"
    ran <-
      runWorkflowRef exec billRef (runOptionsDefault {runWorkflowId = Just (WorkflowId billText)}) (Just (encodeWorkflowInput ()))
    childRow <- fx.wfReadRow (WorkflowId shipText)
    pure (ran, childRow)

-- | Starting a child through a captured parent while a step body runs is
-- refused, not recorded: the depth says what the context cannot, so the
-- engine refuses it with @InsideStep@ before anything is written and no
-- start row appears. Returns the run and the parent's steps.
scenarioCaptureChildRefused :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                            => WfFixture m ->
                               m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord])
scenarioCaptureChildRefused fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "double"
        parentKey = newQualifiedWorkflowName "badparent"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody value wctx = runStep wctx "double" (const (pure (value * 2)))
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let -- Converted body: the scoped shape with the captured-parent start.
        -- The step body captures the workflow view and starts through it —
        -- the same capture the context-level shape made — and the refusal
        -- still fires through the shared depth backstop.
        badBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        badBody _ wctx = do
          marker <- nextWorkflowMarker wctx
          outcome <- withStep wctx marker (firstStepStatus 0) (\_ -> startChildWorkflow wctx childRef startOptionsDefault Nothing)
          pure $ case outcome of
            Left err -> Left err
            Right handle -> Left (ErrorConfig ("started through a captured parent: " <> handle.workflowId))
    parentReg <- registerWorkflow dbos parentKey badBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "childleaf-captured-parent"
    (result :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey wid (Just (encodeWorkflowInput (0 :: Int)))
    steps <- fx.wfListSteps wid
    pure (result, steps)

-- | Starting a child inside a step is refused, not recorded: the leaf
-- start is attempted inside a step scope, so the engine refuses it with
-- @InsideStep@ and no start row appears. Returns the run and the
-- parent's steps.
scenarioChildInsideStepRefused :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                               => WfFixture m ->
                                  m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord])
scenarioChildInsideStepRefused fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "double"
        parentKey = newQualifiedWorkflowName "badparent"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody value wctx = runStep wctx "double" (const (pure (value * 2)))
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let badBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        badBody _ wctx = do
          marker <- nextWorkflowMarker wctx
          outcome <- withStep wctx marker (firstStepStatus 0) (\_sctx -> startChildWorkflow wctx childRef startOptionsDefault Nothing)
          pure $ case outcome of
            Left err -> Left err
            Right handle -> Left (ErrorConfig ("started inside a step: " <> handle.workflowId))
    parentReg <- registerWorkflow dbos parentKey badBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "childleaf-parent"
    (result :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey wid (Just (encodeWorkflowInput (0 :: Int)))
    steps <- fx.wfListSteps wid
    pure (result, steps)

-- | A parent and its child hit an inherited deadline independently: the
-- parent's budget expires while it awaits a sleeping child, which carries
-- the same deadline and cancels itself too. The interrupted await
-- checkpointed nothing — nobody answered it — so a resumed parent asks
-- the child's then-settled row again. Returns the parent's run, the
-- child's awaited outcome, and the parent's steps.
scenarioCascadeDeadline :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                        => WfFixture m ->
                           m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Either (Error EngineOnly) AwaitedOutcome, [StepRecord])
scenarioCascadeDeadline fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = threadDelay 30000000 >> pure (Right 1)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
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
                    Right value -> Right value
                    Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                Right Nothing -> Left (StepFailed "parent" "no child output")
    parentRefE <- registerWorkflowRef dbos parentKey parentBody
    parentRef <- case parentRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "cascade-deadline-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runWorkflowRef exec parentRef (runOptionsDefault {runWorkflowId = Just (WorkflowId parentText), runTimeout = Explicit (millisDuration 400)}) Nothing
    childOutcome <- waitForWorkflow dbos (WorkflowId childText)
    steps <- fx.wfListSteps wid
    pure (ran, childOutcome, steps)

-- | A child can decline the inherited deadline: two children under one
-- bounded parent — the first says nothing, the second declines — are
-- together the difference the timeout sum exists for. Returns the
-- parent's run and all three rows.
scenarioDeclinedDeadline :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                         => WfFixture m ->
                            m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord, Maybe WorkflowRecord)
scenarioDeclinedDeadline fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 1)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          let childPair opts = do
                (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
                  startChildWorkflow wctx childRef opts Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitChild wctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          first <- childPair startOptionsDefault
          second <- childPair (startOptionsDefault {startTimeout = None})
          pure $ case (first, second) of
            (Right x, Right y) -> Right (x + y)
            _ -> Left (StepFailed "parent" "a child failed")
    parentRefE <- registerWorkflowRef dbos parentKey parentBody
    parentRef <- case parentRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "decline-deadline-parent"
    let WorkflowId parentText = wid
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runWorkflowRef exec parentRef (runOptionsDefault {runWorkflowId = Just (WorkflowId parentText), runTimeout = Explicit (secondsDuration 300)}) Nothing
    parentRow <- fx.wfReadRow wid
    inheritedRow <- fx.wfReadRow (WorkflowId (parentText <> "-0"))
    detachedRow <- fx.wfReadRow (WorkflowId (parentText <> "-2"))
    pure (ran, parentRow, inheritedRow, detachedRow)

-- | A child's own timeout replaces the inherited deadline: given its own
-- budget, the child records that timeout and a deadline that outlives its
-- parent's instead of copying the parent's instant.
scenarioChildBudgetWins :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                        => WfFixture m ->
                           m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord)
scenarioChildBudgetWins fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 1)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef (startOptionsDefault {startTimeout = Explicit (secondsDuration 3600)}) Nothing
          case started of
            Left err -> pure (Left err)
            Right wfHandle -> do
              awaited <- awaitChild wctx wfHandle
              pure $ case awaited of
                Left err -> Left err
                Right (Just stored) ->
                  case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                    Right value -> Right value
                    Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                Right Nothing -> Left (StepFailed "parent" "no child output")
    parentRefE <- registerWorkflowRef dbos parentKey parentBody
    parentRef <- case parentRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "child-budget-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runWorkflowRef exec parentRef (runOptionsDefault {runWorkflowId = Just (WorkflowId parentText), runTimeout = Explicit (secondsDuration 60)}) Nothing
    parentRow <- fx.wfReadRow wid
    childRow <- fx.wfReadRow (WorkflowId childText)
    pure (ran, parentRow, childRow)

-- | A child inherits its parent's deadline: the parent's budget becomes
-- a wall-clock deadline stored on its row, and the child copies the same
-- instant instead of deriving a fresh budget. Returns the parent's run
-- and both rows.
scenarioDeadlineInherited :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                          => WfFixture m ->
                             m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord)
scenarioDeadlineInherited fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 1)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
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
                    Right value -> Right value
                    Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                Right Nothing -> Left (StepFailed "parent" "no child output")
    parentRefE <- registerWorkflowRef dbos parentKey parentBody
    parentRef <- case parentRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "inherit-deadline-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runWorkflowRef exec parentRef (runOptionsDefault {runWorkflowId = Just (WorkflowId parentText), runTimeout = Explicit (secondsDuration 300)}) Nothing
    parentRow <- fx.wfReadRow wid
    childRow <- fx.wfReadRow (WorkflowId childText)
    pure (ran, parentRow, childRow)

-- | A cancelled child is an awaited cancellation in the parent: the
-- child carries its own budget, so it cancels itself while the parent
-- waits — that is the awaited workflow's outcome, not the parent's own
-- cancellation. Returns the parent's run, its steps, both rows' statuses,
-- and the child id.
scenarioCancelledChildAwaited :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                              => WfFixture m ->
                                 m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord], Maybe WorkflowStatus, Maybe WorkflowStatus, Text)
scenarioCancelledChildAwaited fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = threadDelay 30000000 >> pure (Right 1)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef (startOptionsDefault {startTimeout = Explicit (millisDuration 300)}) Nothing
          case started of
            Left err -> pure (Left err)
            Right wfHandle -> do
              awaited <- awaitChild wctx wfHandle
              pure $ case awaited of
                Left err -> Left err
                Right (Just stored) ->
                  case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                    Right value -> Right value
                    Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                Right Nothing -> Left (StepFailed "parent" "no child output")
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "awaited-cancel-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    settled <-
      timeout 15000000
        ( runWorkflow exec parentKey wid Nothing ::
            m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
        )
    ran <- case settled of
      Just r -> pure r
      Nothing -> throwIO (userError "the awaited cancellation never returned")
    steps <- fx.wfListSteps wid
    parentRow <- fx.wfReadRow wid
    childRow <- fx.wfReadRow (WorkflowId childText)
    pure (ran, steps, (.workflowRecordStatus) <$> parentRow, (.workflowRecordStatus) <$> childRow, childText)

-- | A converted body: registered through the scoped entry, it runs with
-- the scoped step runner and the scoped sleep through the real run path —
-- the erased transition hands the body a WorkflowCtx. Returns the decoded
-- result and the recorded step names.
scenarioScopedBody :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                   => WfFixture m ->
                      m (Either (Error EngineOnly) Int, [(Int, Text)])
scenarioScopedBody fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newQualifiedWorkflowName "scoped-body"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body value wctx = do
          stepped <- runStep wctx "double" (\_ -> pure (value * 2))
          case stepped of
            Left err -> pure (Left err)
            Right doubled -> do
              slept <- sleepStep wctx (millisDuration 1)
              pure (doubled <$ slept)
    registered <- registerWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "scoped-body-wf"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <-
      runWorkflow exec key wid (Just (encodeWorkflowInput (21 :: Int)))
    decoded <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right value -> pure (Right value)
        Left err -> pure (Left (Deserialization "result" (Text.pack (show err))))
      Right Nothing -> pure (Right 0)
      Left err -> pure (Left err)
    steps <- fx.wfListSteps wid
    pure (decoded, map (\row -> (fromIntegral row.stepRecordStepId, row.stepRecordStepName)) steps)

-- | The converted body's value is the doubled input and its rows are the
-- step and the sleep, in order.
checkScopedBody :: (Either (Error EngineOnly) Int, [(Int, Text)]) -> Either String ()
checkScopedBody (outcome, steps)
  | outcome /= Right 42 = Left ("expected the doubled value, got: " <> show outcome)
  | steps /= [(0, "double"), (1, "DBOS.sleep")] = Left ("unexpected rows: " <> show steps)
  | otherwise = Right ()

-- | The scoped select: two pending steps built through the workflow view,
-- raced by 'selectStep' over the same view. The fast arm wins, the
-- select records its own position after both branch ids, and the loser
-- leaves no row. Returns the winner's value and the recorded steps.
scenarioScopedSelect :: forall m. (MonadMonotonicTimeNSec m, MonadAsync m, MonadDelay m, MonadFork m, MonadMask m, MonadMVar m, MonadTime m)
                     => WfFixture m ->
                        m (Either (Error EngineOnly) Int, [(Int, Text)])
scenarioScopedSelect fx = do
  bracket fx.wfNewDBOS shutdown $ \_dbos -> do
    wid <- fx.wfFreshId "scoped-select-parent"
    let WorkflowId widText = wid
    created <-
      runSystemDB fx.wfSystemDB $ \db ->
        SystemDB.initWorkflow db ((newWorkflow widText) {newWorkflowName = Just "L2ScopedSelect"}) Nothing WorkflowIdReturnExisting
    creator <- case created of
      Left err -> throwIO (userError (show err))
      Right result -> pure result.initResultCreatorXid
    conn <- fx.wfConn
    outcome <-
      withWorkflow conn fx.wfIdentity wid Nothing (WorkflowOwner {ownerWorkflowId = wid, ownerToken = creator}) $ \wctx -> do
        slow <- pendingStep wctx "slow" (\_ -> threadDelay 30000000 >> pure (Right (2 :: Int)))
        fast <- pendingStep wctx "fast" (\_ -> pure (Right (1 :: Int)))
        selectStep
          wctx
          [ SelectArm "slow" slow (\armOutcome -> pure (armOutcome >>= \value -> Right value)),
            SelectArm "fast" fast (\armOutcome -> pure (armOutcome >>= \value -> Right value))
          ]
    steps <- fx.wfListSteps wid
    pure (outcome, map (\row -> (fromIntegral row.stepRecordStepId, row.stepRecordStepName)) steps)

-- | The winner is the fast arm's value; the rows are the fast step under
-- its branch id and the select's own position after both branches.
checkScopedSelect :: (Either (Error EngineOnly) Int, [(Int, Text)]) -> Either String ()
checkScopedSelect (outcome, steps)
  | outcome /= Right 1 = Left ("expected the fast arm's value, got: " <> show outcome)
  | steps /= [(1, "fast"), (2, "DBOS.selectStep")] = Left ("unexpected rows: " <> show steps)
  | otherwise = Right ()

-- | A losing step has its cancellation token fired: the loser registers
-- a watcher on its token, then parks; the winner waits for that
-- registration, so dropping the loser must fire the token for work the
-- runtime cannot stop by dropping it. Returns the winner's value and
-- whether the loser's watcher observed the fire.
scenarioLosingTokenFired :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                         => WfFixture m ->
                            m (Int, Bool)
scenarioLosingTokenFired fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    released <- newEmptyMVar
    watching <- newEmptyMVar
    let parentKey = newQualifiedWorkflowName "parent"
        parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          slow <- pendingStep wctx "slow" $ \inner -> do
            token <- stepCtxCancellationToken inner
            _ <- async $ do
              let watch = do
                    cancelled <- tokenCancelled token
                    if cancelled then pure () else threadDelay 1000 >> watch
              watch
              putMVar released ()
            putMVar watching ()
            threadDelay 30000000
            pure (Right (2 :: Int))
          fast <- pendingStep wctx "fast" (\_ -> takeMVar watching >> pure (Right (1 :: Int)))
          selectStep
            wctx
            [ SelectArm "slow" slow (\outcome -> pure (outcome >>= \value -> Right value)),
              SelectArm "fast" fast (\outcome -> pure (outcome >>= \value -> Right value))
            ]
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "race-token-parent"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey wid Nothing
    decoded <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the fast step's value, got: " <> show other))
    fired <- timeout 15000000 (takeMVar released)
    pure (decoded, maybe False (const True) fired)

-- | A control signal winning a select records no winner: the arm whose
-- outcome is the interrupted error wins, no step row is written, and the
-- row stays @PENDING@. Returns the run's outcome, the parent's steps,
-- the row's status, and the parent id.
scenarioControlSelect :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                      => WfFixture m ->
                         m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord], Maybe WorkflowStatus, Text)
scenarioControlSelect fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let parentKey = newQualifiedWorkflowName "parent"
    wid <- fx.wfFreshId "race-control-parent"
    let WorkflowId parentText = wid
        parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          interrupted <- pendingStep wctx "interrupted" (\_ -> pure (Left (Interrupted {workflowId = parentText})))
          slow <- pendingStep wctx "slow" (\_ -> threadDelay 30000000 >> pure (Right (1 :: Int)))
          selectStep
            wctx
            [ SelectArm "interrupted" interrupted (\outcome -> pure (outcome >>= \value -> Right value)),
              SelectArm "slow" slow (\outcome -> pure (outcome >>= \value -> Right value))
            ]
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey wid Nothing
    steps <- fx.wfListSteps wid
    row <- fx.wfReadRow wid
    pure (ran, steps, (.workflowRecordStatus) <$> row, parentText)

-- | A select step races a never-finishing step against a child's result:
-- the await wins, and every branch is built before the race, so the ids
-- follow source order — the losing step claims 1 without a row, the
-- await 2, and the race itself 3. Returns the winner's value, the
-- parent's steps, and the derived child id.
scenarioSelectStepRaces :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                        => WfFixture m ->
                           m (Int, [StepRecord], Text)
scenarioSelectStepRaces fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 7)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    -- Never finishes, so the await wins however long the child's row
    -- takes to settle.
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault Nothing
          case started of
            Left err -> pure (Left err)
            Right childHandle -> do
              slow <- pendingStep wctx "slow" (\_ -> threadDelay 30000000 >> pure (Right (0 :: Int)))
              awaited <- pendingAwait wctx childHandle
              selectStep
                wctx
                [ SelectArm "slow" slow (\outcome -> pure (outcome >>= \value -> Right value)),
                  SelectArm "DBOS.getResult" awaited $ \outcome ->
                    pure $ case outcome of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left _ -> Left (StepFailed "parent" "bad child output")
                      Right Nothing -> Left (StepFailed "parent" "no child output")
                ]
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "race-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey wid Nothing
    decoded <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the race's winner, got: " <> show other))
    steps <- fx.wfListSteps wid
    pure (decoded, steps, childText)

-- | A run claims its start and its await together, so each await sits
-- immediately behind its own start and a replay rebuilds the same pairs
-- however the children interleave. Returns the summed result, the
-- parent's steps, and the parent id.
scenarioStepIdPairs :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                    => WfFixture m ->
                       m (Int, [StepRecord], Text)
scenarioStepIdPairs fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "parent"
        childBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody n _ = pure (Right n)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          let pair n = do
                (startedPair :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
                  startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowInput (n :: Int)))
                case startedPair of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitChild wctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          a <- pair 1
          b <- pair 2
          c <- pair 3
          pure $ case (a, b, c) of
            (Right x, Right y, Right z) -> Right (x + y + z)
            _ -> Left (StepFailed "parent" "a started child failed")
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "pairs-parent"
    let WorkflowId parentText = wid
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey wid Nothing
    decoded <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the summed children, got: " <> show other))
    steps <- fx.wfListSteps wid
    pure (decoded, steps, parentText)

-- | Awaiting a child inside a step is covered by that step: the parent
-- history holds the child start and the enclosing @collect@ step whose
-- output is the child's value — no @DBOS.get_result@ checkpoint of its
-- own. Returns the enclosing step's value, the parent's steps, and the
-- derived child id.
scenarioAwaitInsideStep :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                        => WfFixture m ->
                           m (Int, [StepRecord], Text)
scenarioAwaitInsideStep fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 41)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault Nothing
          case started of
            Left err -> pure (Left err)
            Right wfHandle ->
              runStepWith stepOptionsDefault wctx "collect" $ \_sctx -> do
                awaited <- awaitChild wctx wfHandle
                pure $ case awaited of
                  Left err -> Left err
                  Right (Just stored) ->
                    case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                      Right value -> Right value
                      Left err -> Left (StepFailed "collect" (Text.pack (show err)))
                  Right Nothing -> Left (StepFailed "collect" "no child output")
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "await-step-parent"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    (ran :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey wid Nothing
    decoded <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the enclosing step's value, got: " <> show other))
    steps <- fx.wfListSteps wid
    pure (decoded, steps, childText)

-- | An await recorded at the position the parent is about to reach,
-- naming a workflow that is not the one it holds a handle to, is refused
-- rather than consumed: the parent ends on the engine's
-- @UnexpectedStep@. The parent runs on a fork and parks on a gate after
-- its start step, so the planted row lands while the run owns the id —
-- the same sequence on both stacks (cooperative in sim). Returns the
-- settled run and the derived child id the refusal must name.
scenarioStaleAwaitRefused :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                          => WfFixture m ->
                             m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Text)
scenarioStaleAwaitRefused fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 1)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    gate <- newEmptyMVar
    entered <- newEmptyMVar
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef startOptionsDefault Nothing
          case started of
            Left err -> pure (Left err)
            Right wfHandle -> do
              putMVar entered ()
              takeMVar gate
              awaited <- awaitChild wctx wfHandle
              pure $ case awaited of
                Left err -> Left err
                Right (Just stored) ->
                  case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                    Right value -> Right value
                    Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                Right Nothing -> Left (StepFailed "parent" "no child output")
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "await-wrong"
    let WorkflowId parentText = wid
        childText = parentText <> "-0"
    worker <-
      async
        ( runWorkflow exec parentKey wid Nothing ::
            m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
        )
    enteredOk <- timeout 15000000 (takeMVar entered)
    case enteredOk of
      Nothing -> throwIO (userError "the parent never recorded its start")
      Just () -> pure ()
    planted <-
      runSystemDB fx.wfSystemDB $ \db -> do
        -- The plant lands while the run owns the id, so it presents the
        -- row's own creator token, as the execution's write would.
        found <- SystemDB.getWorkflow db wid
        creator <- case found of
          Right (Just row) -> case row.workflowRecordCreatorXid of
            Just token -> pure token
            Nothing -> throwIO (userError "planted write needs the row's creator")
          _ -> throwIO (userError "planted write needs the row")
        SystemDB.recordChildResult
          db
          wid
          1
          (WorkflowId "somebody-elses-workflow")
          (SystemDB.OutcomeOutput (Just "7"))
          Nothing
          Nothing
          creator
    case planted of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    putMVar gate ()
    outcome <- timeout 15000000 (wait worker)
    case outcome of
      Just ran -> pure (ran, childText)
      Nothing -> throwIO (userError "the stale-await refusal never returned")

-- | A child joining a held key is recorded as the workflow it joined: the
-- holder waits on its queue (a delay holds the key without running), the
-- parent starts a child that joins it, and the recorded start and await
-- both name the holder while no derived id ever exists. The queue is
-- driven explicitly — no supervisor runs on either stack — with the parent
-- run forked, the join observed, then passes until the parent settles.
scenarioJoinHeldKey :: forall m. (MonadMVar m, MonadFork m, MonadAsync m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                    => WfFixture m -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Text, Maybe WorkflowRecord, [StepRecord], [WorkflowId])
scenarioJoinHeldKey fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    holderWid <- fx.wfFreshId "join-holder"
    parentWid <- fx.wfFreshId "join-parent"
    let WorkflowId holderText = holderWid
        WorkflowId parentText = parentWid
        derivedWid = WorkflowId (parentText <> "-0")
        queueName = "join-q-" <> holderText
        dedupKey = "order-42-" <> holderText
        childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "joiner"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 9)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        parentBody () wctx = do
          started <- startChildWorkflow wctx childRef (startOptionsDefault {startQueue = Just (((queueRequestNew queueName) {deduplicationId = Just dedupKey, duplicationPolicy = ReturnExisting}) :: QueueRequest)}) Nothing
          case started of
            Left err -> pure (Left err)
            Right child -> do
              result <- awaitChild wctx child
              case result of
                Left err -> pure (Left err)
                Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                  Right n -> pure (Right n)
                  Left _ -> pure (Left (StepFailed "parent" "bad child output"))
                Right _ -> pure (Left (StepFailed "parent" "no child output"))
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunchWithQueues (Just [queueName]) dbos
    _ <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate >>= \r -> case r of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    -- The holder, enqueued before the parent runs and still waiting when
    -- the child starts: a delay holds the key without running.
    let holderQueue = ((queueRequestNew queueName) {deduplicationId = Just dedupKey, delay = Just (secondsDuration 3)} :: QueueRequest)
    holderStarted <- startWorkflow exec childRef (startOptionsDefault {startWorkflowId = Just holderWid, startQueue = Just holderQueue}) Nothing
    case holderStarted of
      Left err -> throwIO (userError (show (err :: Error EngineOnly)))
      Right _ -> pure ()
    worker <- async (runWorkflow exec parentKey parentWid Nothing)
    -- The join must find the holder waiting: observe the parent's start
    -- step before driving the queue.
    let awaitJoin = go (100 :: Int)
        go 0 = throwIO (userError "the parent never started its child")
        go n = do
          steps <- fx.wfListSteps parentWid
          if null steps then threadDelay 200000 >> go (n - 1) else pure ()
    awaitJoin
    -- Drive the queue until the parent settles: the delayed holder comes
    -- due, is claimed, runs, and the await resolves.
    let drive 0 = throwIO (userError "the joined parent never settled")
        drive n = do
          t0 <- getCurrentTime
          _ <- dequeueWorkflows dbos >>= \r -> case r of
            Left err -> throwIO (userError (show err))
            Right _ -> pure ()
          t1 <- getCurrentTime
          row <- fx.wfReadRow parentWid
          t2 <- getCurrentTime
          trace ("pass " <> show (100 - n) <> " dequeue=" <> show (diffUTCTime t1 t0) <> " read=" <> show (diffUTCTime t2 t1)) (pure ())
          case (.workflowRecordStatus) <$> row of
            Just Pending -> threadDelay 200000 >> drive (n - 1)
            _ -> pure ()
    drive (100 :: Int)
    -- Best-effort hygiene: the shared database keeps a queue row per run and
    -- every unscoped sweep pays one claim query per row. The queue is idle
    -- now (the drive settled its workflows); workflow rows are left alone.
    _ <- deleteQueue dbos queueName >>= \r -> case r of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    ran <- wait worker
    derived <- fx.wfReadRow derivedWid
    listed <- fx.wfListSteps parentWid
    children <- fx.wfChildren parentWid
    pure (ran, holderText, derived, listed, children)

-- | An in-workflow enqueue is a recorded child start that replays: the parent
-- enqueues a child onto a queue nothing drives, the start step names the
-- child and links it, and a second run under the same id adopts the recorded
-- child instead of enqueuing another.
scenarioEnqueuedChildReplays :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                             => WfFixture m ->
                                m (Text, Text, [StepRecord], [WorkflowId], Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
scenarioEnqueuedChildReplays fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "enqueuer"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 7)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    parentWid <- fx.wfFreshId "enqueued-child-parent"
    queueWid <- fx.wfFreshId "enqueued-child-q"
    let WorkflowId parentText = parentWid
        WorkflowId queueText = queueWid
        queueName = "enqueued-child-q-" <> queueText
        childText = parentText <> "-0"
        parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        parentBody () wctx = do
          (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
            startChildWorkflow wctx childRef (startOptionsDefault {startQueue = Just (queueRequestNew queueName)}) Nothing
          pure ((.workflowId) <$> started)
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    (first :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey parentWid Nothing
    _ <- case first of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
        Right got | got == childText -> pure ()
        Right got -> throwIO (userError ("expected the enqueued child id, got: " <> Text.unpack got))
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the parent's child id, got: " <> show other))
    -- The replay adopts the recorded start instead of enqueuing another.
    (replayed :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) <- runWorkflow exec parentKey parentWid Nothing
    listed <- fx.wfListSteps parentWid
    children <- fx.wfChildren parentWid
    pure (parentText, childText, listed, children, replayed)

-- | What a deduplicated child start's replay observed: the parent's first
-- and replayed outputs, the parent's listed steps, the re-run's output, and
-- the child rows the replay and the re-run did (or did not) start.
data DedupStartOutcome = DedupStartOutcome
  { dedupFirst       :: Text,
    dedupReplay      :: Text,
    dedupSteps       :: [StepRecord],
    dedupRerun       :: Text,
    dedupReplayChild :: Maybe WorkflowRecord,
    dedupRerunChild  :: Maybe WorkflowRecord
  }

-- | The window a live queue assertion races, made deterministic: a queued
-- start commits its row @ENQUEUED@, and the next dequeue pass claims it to
-- @PENDING@. Nothing in between is guaranteed — a supervisor tick landing
-- after the start and before the read is the same claim.
--
-- This is the mechanism behind the once-seen failure of
-- "a refused child start replays after the key is freed": that case starts a
-- queued child during a fork's body and then reads the row, so under the
-- live supervisor the row can already be @PENDING@. The body here blocks on
-- a gate it never opens before the assertion, so the claimed status is
-- @PENDING@ on every run rather than @SUCCESS@ or @PENDING@ by luck.
scenarioQueuedStartClaimedByNextPass
  :: forall m. (MonadAsync m, MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
  => WfFixture m -> m (Maybe WorkflowStatus, Maybe WorkflowStatus)
scenarioQueuedStartClaimedByNextPass fx =
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    wid <- fx.wfFreshId "claim-window"
    let WorkflowId text = wid
        queueName = "claim-window-q-" <> text
        key = newQualifiedWorkflowName "claim-window"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body () _ = do
          gate <- newEmptyMVar
          takeMVar gate >> pure (Right 1)
    ref <- registerWorkflowRef dbos key body >>= either (throwIO . userError . show) pure
    let QueueName internal = internalQueueName
    exec <- fx.wfLaunchWithQueues (Just [queueName, internal]) dbos
    _ <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate >>= either (throwIO . userError . show) pure
    (_started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <-
      startWorkflow
        exec
        ref
        (startOptionsDefault {startWorkflowId = Just wid, startQueue = Just (queueRequestNew queueName)})
        Nothing
    _ <- case _started of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    before <- fmap (.workflowRecordStatus) <$> fx.wfReadRow wid
    _ <- dequeueWorkflows dbos >>= either (throwIO . userError . show) pure
    after <- fmap (.workflowRecordStatus) <$> fx.wfReadRow wid
    _ <- deleteQueue dbos queueName >>= either (throwIO . userError . show) pure
    pure (before, after)

-- | The row is @ENQUEUED@ until the pass claims it, and @PENDING@ after:
-- the claim, not the start, is what makes the status a reader sees.
checkQueuedStartClaimedByNextPass :: (Maybe WorkflowStatus, Maybe WorkflowStatus) -> Either String ()
checkQueuedStartClaimedByNextPass (before, after) = do
  unless (before == Just Enqueued) $ Left ("expected the start to leave the row enqueued, got: " <> show before)
  unless (after == Just Pending) $ Left ("expected the next pass to claim the row, got: " <> show after)

-- | A deduplicated child start's refusal is recorded at the parent's step
-- and a replay returns it after the key is freed: the parent handles the
-- refusal and finishes; the holder then runs; a fork that copies the start
-- takes the same branch and starts no child, while a fork that runs the
-- start again goes ahead. Mirrors Rust's
-- @a_deduplicated_child_start_replays_its_error_after_the_key_is_freed@.
scenarioDedupStartRecorded :: forall m. (MonadMVar m, MonadFork m, MonadAsync m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                           => WfFixture m -> m DedupStartOutcome
scenarioDedupStartRecorded fx =
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    holderWid <- fx.wfFreshId "dedup-error-holder"
    parentWid <- fx.wfFreshId "dedup-error-parent"
    let WorkflowId holderText = holderWid
        WorkflowId parentText = parentWid
        queueName = "dedup-error-q-" <> holderText
        dedupKey = "order-7-" <> holderText
        childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "dedup-error-parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 9)
        dedupShape = ((queueRequestNew queueName) {deduplicationId = Just dedupKey, duplicationPolicy = Reject} :: QueueRequest)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        parentBody () wctx = do
          started <- startChildWorkflow wctx childRef (startOptionsDefault {startQueue = Just dedupShape}) Nothing
          pure $ case started of
            Left (SystemDatabase (SysErr.QueueDeduplicated {})) -> Right "deduplicated"
            Left err -> Left err
            Right _ -> Right "started"
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    let QueueName internal = internalQueueName
    exec <- fx.wfLaunchWithQueues (Just [queueName, internal]) dbos
    let runFork source forked point = do
          made <-
            forkWorkflows
              dbos
              [(forkNew source) {forkForkedId = Just forked, forkStartStep = point}]
              defaultForkOptions
          case made of
            Left err -> throwIO (userError (show err))
            Right _ -> pure ()
          _ <- dequeueWorkflows dbos >>= \r -> case r of
            Left err -> throwIO (userError (show err))
            Right _ -> pure ()
          handleE <- retrieveWorkflow dbos (WorkflowId forked)
          handle <- case handleE of
            Left err -> throwIO (userError (show err))
            Right h -> pure (h :: WorkflowHandle m EngineOnly)
          ran <- handleResult handle
          pure (forked, ran)
    -- Enqueued before its queue is registered: nothing polls it, so the
    -- holder waits and holds the key through the parent's first run.
    holderStarted <-
      startWorkflow
        exec
        childRef
        ( startOptionsDefault
            { startWorkflowId = Just holderWid,
              startQueue = Just (((queueRequestNew queueName) {deduplicationId = Just dedupKey}) :: QueueRequest)
            }
        )
        Nothing
    case holderStarted of
      Left err -> throwIO (userError (show (err :: Error EngineOnly)))
      Right _ -> pure ()
    first <- runWorkflow exec parentKey parentWid Nothing
    firstText <- decodeText "the parent's first run" (first :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
    steps <- fx.wfListSteps parentWid
    -- Registering the queue lets the holder run, and its finishing frees
    -- the key.
    _ <-
      registerQueue dbos queueName defaultQueueOptions AlwaysUpdate >>= \r -> case r of
        Left err -> throwIO (userError (show err))
        Right _ -> pure ()
    _ <- dequeueWorkflows dbos >>= \r -> case r of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    holderDone <- waitForWorkflow dbos holderWid
    case holderDone of
      Right (SystemDB.AwaitedSucceeded {}) -> pure ()
      other -> throwIO (userError ("the holder never ran, got: " <> show other))
    -- Copies of the recorded start replay the refusal.
    (replayId, replayRan) <- runFork parentText (parentText <> "-replay-fork") 1
    replayText <- decodeText "the replayed fork" replayRan
    replayChild <- lookupRow (WorkflowId (replayId <> "-0"))
    -- The start runs again with the key free.
    (rerunId, rerunRan) <- runFork parentText (parentText <> "-rerun-fork") 0
    rerunText <- decodeText "the re-run fork" rerunRan
    rerunChild <- lookupRow (WorkflowId (rerunId <> "-0"))
    _ <- deleteQueue dbos queueName >>= \r -> case r of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    pure
      DedupStartOutcome
        { dedupFirst = firstText,
          dedupReplay = replayText,
          dedupSteps = steps,
          dedupRerun = rerunText,
          dedupReplayChild = replayChild,
          dedupRerunChild = rerunChild
        }
  where
    decodeText what result = case result of
      Right (Just stored) -> case decodeWorkflowValue what (Just stored) :: Either CodecError Text of
        Right text -> pure text
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected " <> Text.unpack what <> ", got: " <> show other))
    lookupRow wid = do
      found <- runSystemDB fx.wfSystemDB (\db -> SystemDB.getWorkflow db wid)
      case found of
        Left err -> throwIO (userError (show err))
        Right row -> pure row

-- | What a conflicting child start's replay observed: the first run's and
-- the replay's rendered errors, the parent's listed steps, its terminal
-- status, and the row at the freed id after the replay.
data ConflictStartOutcome = ConflictStartOutcome
  { conflictFirst    :: Text,
    conflictReplayed :: Text,
    conflictSteps    :: [StepRecord],
    conflictStatus   :: Maybe WorkflowStatus,
    conflictChildRow :: Maybe WorkflowRecord
  }

-- | A child id taken by another function is recorded at the parent's step
-- too: the parent returns the error with @?@ and ends @ERROR@; once the
-- other row is deleted the same start would go ahead, so a replay that did
-- not read the record would succeed. Mirrors Rust's
-- @a_child_id_taken_by_another_function_replays_its_error_after_the_row_is_deleted@.
scenarioConflictStartRecorded :: forall m. (MonadMVar m, MonadFork m, MonadAsync m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                              => WfFixture m -> m ConflictStartOutcome
scenarioConflictStartRecorded fx =
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    takenWid <- fx.wfFreshId "taken-child"
    parentWid <- fx.wfFreshId "conflict-parent"
    let WorkflowId takenText = takenWid
        childKey = newQualifiedWorkflowName "child"
        squatterKey = newQualifiedWorkflowName "squatter"
        parentKey = newQualifiedWorkflowName "conflict-parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 9)
        squatterBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        squatterBody () _ = pure (Right 0)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    squatterRefE <- registerWorkflowRef dbos squatterKey squatterBody
    case squatterRefE of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        parentBody () wctx = do
          started <-
            startChildWorkflow
              wctx
              childRef
              (startOptionsDefault {startWorkflowId = Just (WorkflowId takenText)})
              Nothing
          pure $ case started of
            Left err -> Left err
            Right _ -> Right "started"
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    squatted <- runWorkflow exec squatterKey takenWid Nothing
    case (squatted :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) of
      Right _ -> pure ()
      other -> throwIO (userError ("the squatter was supposed to run, got: " <> show other))
    first <- runWorkflow exec parentKey parentWid Nothing
    firstShown <- case (first :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) of
      Left err -> pure (Text.pack (show err))
      Right _ -> throwIO (userError "expected the conflicting start to fail the parent")
    steps <- fx.wfListSteps parentWid
    parentRow <- fx.wfReadRow parentWid
    -- With the squatter gone the id is free, so only the record keeps the
    -- replay failing.
    _ <- deleteWorkflow dbos takenWid >>= \r -> case r of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    forked <- forkWorkflow dbos parentWid (Just (ForkStep 1)) defaultForkOptions
    handle <- case forked of
      Left err -> throwIO (userError (show (err :: Error EngineOnly)))
      Right h -> pure (h :: WorkflowHandle m EngineOnly)
    _ <- dequeueWorkflows dbos >>= \r -> case r of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    replayed <- handleResult handle
    replayedShown <- case replayed of
      Left err -> pure (Text.pack (show err))
      Right _ -> throwIO (userError "expected the replay to end with the error")
    childRow <- do
      found <- runSystemDB fx.wfSystemDB (\db -> SystemDB.getWorkflow db takenWid)
      case found of
        Left err -> throwIO (userError (show err))
        Right row -> pure row
    pure
      ConflictStartOutcome
        { conflictFirst = firstShown,
          conflictReplayed = replayedShown,
          conflictSteps = steps,
          conflictStatus = fmap (.workflowRecordStatus) parentRow,
          conflictChildRow = childRow
        }

-- | A millisecond budget against a second-long body: the run reports the
-- durable cancellation naming the workflow and the row reads CANCELLED.
-- Virtual time makes the second instant under IOSim.
scenarioBudgetCancels :: forall m. (MonadAsync m, MonadMVar m, MonadFork m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                      => WfFixture m -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowStatus)
scenarioBudgetCancels fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    let key = newQualifiedWorkflowName "slow"
        body :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body () _ = threadDelay 1000000 >> pure (Right 7)
    refE <- registerWorkflowRef dbos key body
    ref <- case refE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.wfLaunch dbos
    wid <- fx.wfFreshId "budget-id"
    ran <- runWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just wid, runTimeout = Explicit (millisDuration 1)}) Nothing
    row <- fx.wfReadRow wid
    pure (ran, (.workflowRecordStatus) <$> row)

-- * Shared checks and the live interpreter: every converted case is one
-- line per tree over a shared scenario and a shared pure check
-- (ContextTest's @checkScopeStatus@ pattern). The sim tree mirrors each
-- line with its own one-line interpreter over the sim fixture plus
-- @runSimCase@; the pair diff then proves the trees match
-- name-for-name. A single imported @TestTree@ cannot cover both
-- backends: tasty leaves are @IO@, while sim execution is rank-2
-- (@forall s. IOSim s a@) with trace printing on top.

-- | The shared verdicts both trees assert. Pure so either runner can own
-- the failure; messages match the assertions they replace.
checkRegisteredResult :: (Int, WorkflowRecord) -> Either String ()
checkRegisteredResult (n, row)
  | n /= 42 = Left ("expected the doubled result 42, got: " <> show n)
  | row.workflowRecordStatus /= Success = Left ("expected the row SUCCESS, got: " <> show row.workflowRecordStatus)
  | row.workflowRecordName /= Just "double" = Left ("expected the workflow name double, got: " <> show row.workflowRecordName)
  | row.workflowRecordOutput /= Just "42" = Left ("expected the output 42, got: " <> show row.workflowRecordOutput)
  | not (portableInputEnvelope [toJSON (21 :: Int)] row.workflowRecordInput) = Left ("expected the portable input envelope for 21, got: " <> show row.workflowRecordInput)
  | row.workflowRecordSerialization /= Just "portable_json" = Left ("expected portable_json, got: " <> show row.workflowRecordSerialization)
  | otherwise = Right ()

checkRunRefRejectsWithoutId :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Bool) -> Either String ()
checkRunRefRejectsWithoutId (result, ran) = do
  when ran (Left "the workflow body ran although the policy had no id")
  case result of
    Left (InvalidArgument _ detail)
      | "WorkflowIdReusePolicy::Reject" `Text.isInfixOf` detail -> Right ()
      | otherwise -> Left ("expected the Reject detail, got: " <> Text.unpack detail)
    other -> Left ("expected the Reject refusal, got: " <> show other)

-- | A workflow input is the portable envelope with these positional values
-- and no named ones.
portableInputEnvelope :: [Value] -> Maybe Text -> Bool
portableInputEnvelope positional (Just text) =
  case eitherDecodeStrict (encodeUtf8 text) of
    Right (Object fields) ->
      KeyMap.lookup "positionalArgs" fields == Just (toJSON positional)
        && KeyMap.lookup "namedArgs" fields == Just (Object KeyMap.empty)
    _ -> False
portableInputEnvelope _ Nothing = False

-- | One id, one execution: both callers read the run and the handles
-- name the same workflow.
checkJoinTakesId :: JoinOutcome -> Either String ()
checkJoinTakesId out
  | out.joinFirst /= 7 = Left ("expected the first caller to read 7, got: " <> show out.joinFirst)
  | out.joinSecond /= 7 = Left ("expected the joining caller to read 7, got: " <> show out.joinSecond)
  | out.joinEntered /= 1 = Left ("expected one execution, got: " <> show out.joinEntered)
  | out.joinRowStatus /= Success = Left ("expected the row SUCCESS, got: " <> show out.joinRowStatus)
  | out.joinFirstId /= out.joinSecondId = Left ("expected both handles to name one workflow, got: " <> show (out.joinFirstId, out.joinSecondId))
  | out.joinFirstPending /= Pending = Left ("expected the started row PENDING, got: " <> show out.joinFirstPending)
  | otherwise = Right ()

-- | A fresh start runs local; joins and retrieves poll.
checkFreshJoinPolls :: (Text, Text, Text, Int) -> Either String ()
checkFreshJoinPolls (firstLabel, joinLabel, retrieveLabel, n)
  | firstLabel /= "local" = Left ("expected a local handle for the fresh start, got: " <> show firstLabel)
  | joinLabel /= "polling" = Left ("expected a polling handle for the join, got: " <> show joinLabel)
  | retrieveLabel /= "polling" = Left ("expected a polling handle from retrieve, got: " <> show retrieveLabel)
  | n /= 7 = Left ("expected the local handle to read 7, got: " <> show n)
  | otherwise = Right ()

-- | A plain start of a queued row joins it: polling handle, row stays
-- ENQUEUED, and the body never ran a step.
checkQueuedStartPolls :: (Text, WorkflowStatus, [StepRecord]) -> Either String ()
checkQueuedStartPolls (joinLabel, status, steps)
  | joinLabel /= "polling" = Left ("expected a polling handle for a queued join, got: " <> show joinLabel)
  | status /= Enqueued = Left ("expected the queued row to stay ENQUEUED, got: " <> show status)
  | not (null steps) = Left ("expected the queued body not to run, got steps: " <> show (map (.stepRecordStepName) steps))
  | otherwise = Right ()

-- | The stored spelling is aeson's, not Rust's: @-inf@/@+inf@ as strings and
-- NaN as @null@; the decoded tuple still reads the infinities.
checkNonFinite :: ((Double, Double, Double), Maybe Text, Maybe Text) -> Either String ()
checkNonFinite ((low, high, nan), inputText, outputText)
  | not (isInfinite low) || low > 0 = Left ("expected the argument to read -Infinity, got: " <> show low)
  | not (isInfinite high) || high < 0 = Left ("expected the result to read +Infinity, got: " <> show high)
  | not (isNaN nan) = Left ("expected the result NaN to read back as NaN, got: " <> show nan)
  | Just text <- inputText, not ("\"-inf\"" `Text.isInfixOf` text) =
      Left ("expected the stored argument to spell -inf, got: " <> Text.unpack text)
  | Just text <- outputText,
    not (all (`Text.isInfixOf` text) ["\"-inf\"", "\"+inf\"", "null"]) =
      Left ("expected the stored result to spell -inf/+inf/null, got: " <> Text.unpack text)
  | otherwise = Right ()

-- | The parent history holds the start and its recorded await.
checkAwaitRecorded :: (Int, [StepRecord], Text) -> Either String ()
checkAwaitRecorded (n, steps, childText)
  | n /= 99 = Left ("expected the parent to read 99, got: " <> show n)
  | otherwise = case steps of
      [ StepRecord {stepRecordStepName = startName, stepRecordChildWorkflowId = Just (WorkflowId startedChild)},
        StepRecord
          { stepRecordStepName = awaitName,
            stepRecordOutput = Just awaitOutput,
            stepRecordChildWorkflowId = Just (WorkflowId awaitedChild)
          }
        ]
        | startName /= "child" -> Left ("expected the start step, got: " <> show startName)
        | startedChild /= childText -> Left ("expected the start to name the child, got: " <> show startedChild)
        | awaitName /= "DBOS.getResult" -> Left ("expected the recorded await, got: " <> show awaitName)
        | awaitOutput /= "99" -> Left ("expected the awaited output 99, got: " <> show awaitOutput)
        | awaitedChild /= childText -> Left ("expected the await to name the child, got: " <> show awaitedChild)
        | otherwise -> Right ()
      other -> Left ("expected the start and the recorded await, got: " <> show other)

-- | The refusal names the await it was reaching for and the outcome it
-- found instead.
checkStaleAwaitRefused :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Text) -> Either String ()
checkStaleAwaitRefused (ran, childText) = case ran of
  Left (SystemDatabase (SystemDB.UnexpectedStep {stepId, expected, recorded}))
    | stepId /= 1 -> Left ("expected the refusal at step 1, got: " <> show stepId)
    | not (childText `Text.isInfixOf` expected) ->
        Left ("expected the await of " <> Text.unpack childText <> " in " <> Text.unpack expected)
    | not ("somebody-elses-workflow" `Text.isInfixOf` recorded) ->
        Left ("expected the planted workflow in " <> Text.unpack recorded)
    | otherwise -> Right ()
  other -> Left ("expected the stale-await refusal, got: " <> show other)

-- | The enclosing step carries the child's value; no separate await
-- checkpoint exists beside it.
checkAwaitInsideStep :: (Int, [StepRecord], Text) -> Either String ()
checkAwaitInsideStep (n, steps, childText)
  | n /= 41 = Left ("expected the enclosing step to carry 41, got: " <> show n)
  | otherwise = case steps of
      [ StepRecord {stepRecordStepName = startName, stepRecordChildWorkflowId = Just (WorkflowId startedChild)},
        StepRecord {stepRecordStepName = collectName, stepRecordOutput = Just collectOutput}
        ]
        | startName /= "child" -> Left ("expected the start step, got: " <> show startName)
        | startedChild /= childText -> Left ("expected the start to name the child, got: " <> show startedChild)
        | collectName /= "collect" -> Left ("expected the enclosing step, got: " <> show collectName)
        | collectOutput /= "41" -> Left ("expected the enclosing step's output 41, got: " <> show collectOutput)
        | otherwise -> Right ()
      other -> Left ("expected the start and the enclosing step only, got: " <> show other)

-- | Starts claim ids in build order, then the awaits follow in the same
-- order; the first-built child is the one that returned 1.
checkChildIdsInBuildOrder :: (Int, [StepRecord], Text, Maybe Text) -> Either String ()
checkChildIdsInBuildOrder (n, steps, parentText, firstChildOutput)
  | n /= 6 = Left ("expected 1 + 2 + 3, got: " <> show n)
  | firstChildOutput /= Just "1" = Left ("expected the first-built child to have returned 1, got: " <> show firstChildOutput)
  | table /= expected = Left ("expected the three starts and their awaits, got: " <> show table)
  | otherwise = Right ()
  where
    table = map (\row -> (row.stepRecordStepId, row.stepRecordStepName, row.stepRecordChildWorkflowId)) steps
    child :: Int -> WorkflowId
    child k = WorkflowId (parentText <> "-" <> Text.pack (show k))
    expected =
      [ (0, "child", Just (child 0)),
        (1, "child", Just (child 1)),
        (2, "child", Just (child 2)),
        (3, "DBOS.getResult", Just (child 0)),
        (4, "DBOS.getResult", Just (child 1)),
        (5, "DBOS.getResult", Just (child 2))
      ]

-- | The refusal names the call; nothing was written for the refused
-- start, and the parent recorded no step.
checkWrongInstance :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, [StepRecord], Text) -> Either String ()
checkWrongInstance (ran, missing, steps, _childText) = case ran of
  Left (WrongInstance {operation})
    | not ("workflow" `Text.isInfixOf` operation) -> Left ("expected the call named in " <> Text.unpack operation)
    | missing /= Nothing -> Left ("expected no child written, got: " <> show missing)
    | not (null steps) -> Left ("expected no start on the parent, got: " <> show steps)
    | otherwise -> Right ()
  other -> Left ("expected a wrong-instance refusal, got: " <> show other)

-- | The parent reads the joined workflow's output, no derived id exists,
-- and the recorded start and await both name the holder while the holder
-- lists among no children.
checkJoinHeldKey :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Text, Maybe WorkflowRecord, [StepRecord], [WorkflowId]) -> Either String ()
checkJoinHeldKey (ran, holderText, derived, listed, children) = case ran of
  Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
    Right 9 -> do
      unless (derived == Nothing) $ Left ("expected no derived id, got: " <> show derived)
      case listed of
        [ StepRecord {stepRecordStepName = startName, stepRecordChildWorkflowId = Just (WorkflowId startedChild)},
          StepRecord
            { stepRecordStepName = awaitName,
              stepRecordOutput = Just awaitOutput,
              stepRecordChildWorkflowId = Just (WorkflowId awaitedChild)
            }
          ] -> do
            unless (startName == "child") $ Left ("expected the joining start, got: " <> show startName)
            unless (startedChild == holderText) $ Left ("expected the start to name the holder, got: " <> show startedChild)
            unless (awaitName == "DBOS.getResult") $ Left ("expected the recorded await, got: " <> show awaitName)
            unless (awaitOutput == "9") $ Left ("expected the joined output recorded, got: " <> show awaitOutput)
            unless (awaitedChild == holderText) $ Left ("expected the await to name the holder, got: " <> show awaitedChild)
            unless (children == []) $ Left ("expected the holder among no children, got: " <> show children)
        other -> Left ("expected the joining start and its recorded await, got: " <> show other)
    other -> Left ("expected the joined output 9, got: " <> show other)
  other -> Left ("expected the joined output, got: " <> show other)

-- | The enqueue recorded exactly one start step naming the child and linking
-- it; the replay adopted the recorded child, so exactly one child exists.
checkEnqueuedChildReplays :: (Text, Text, [StepRecord], [WorkflowId], Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) -> Either String ()
checkEnqueuedChildReplays (parentText, childText, listed, children, replayed)
  | childText /= parentText <> "-0" = Left ("expected the derived child id, got: " <> Text.unpack childText)
  | otherwise = case listed of
      [StepRecord {stepRecordStepName = startName, stepRecordChildWorkflowId = Just (WorkflowId startedChild)}] -> do
        unless (startName == "child") $ Left ("expected the enqueued start, got: " <> show startName)
        unless (startedChild == childText) $ Left ("expected the start to name the child, got: " <> show startedChild)
        unless (children == [WorkflowId childText]) $ Left ("expected exactly the enqueued child, got: " <> show children)
        case replayed of
          Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
            Right adopted
              | adopted == childText -> Right ()
              | otherwise -> Left ("expected the replay to adopt " <> Text.unpack childText <> ", got: " <> Text.unpack adopted)
            Left err -> Left ("expected the replayed parent's id, got: " <> show err)
          other -> Left ("expected the replay to adopt the recorded child, got: " <> show other)
      other -> Left ("expected the one recorded enqueue, got: " <> show other)

-- | The deduplicated start's refusal is recorded with no child, the replay
-- returns it after the key is freed, and the re-run starts the child.
checkDedupStartRecorded :: DedupStartOutcome -> Either String ()
checkDedupStartRecorded out
  | out.dedupFirst /= "deduplicated" =
      Left ("expected the handled refusal, got: " <> show out.dedupFirst)
  | out.dedupReplay /= out.dedupFirst =
      Left ("the replay took a different branch: " <> show (out.dedupFirst, out.dedupReplay))
  | out.dedupRerun /= "started" =
      Left ("expected the re-run to start the child, got: " <> show out.dedupRerun)
  | Just _ <- out.dedupReplayChild =
      Left ("the replay started a child: " <> show out.dedupReplayChild)
  | otherwise = case (out.dedupSteps, out.dedupRerunChild) of
      ([step], Just childRow)
        | step.stepRecordStepName /= "child" ->
            Left ("expected the child start's step, got: " <> show step.stepRecordStepName)
        | step.stepRecordChildWorkflowId /= Nothing ->
            Left ("the refusal recorded a child id: " <> show step.stepRecordChildWorkflowId)
        | step.stepRecordSerialization /= Just "portable_json" ->
            Left ("expected the refusal under the portable tag, got: " <> show step.stepRecordSerialization)
        | not (maybe False ("QueueDeduplicated" `Text.isInfixOf`) step.stepRecordError) ->
            Left ("expected the recorded QueueDeduplicated, got: " <> show step.stepRecordError)
        -- The child was started; its status is whatever the supervisor
        -- tick had driven it to when read (ENQUEUED unclaimed, PENDING
        -- claimed, SUCCESS run). Pinning one status overspecifies a
        -- moving row: the tick, not the start, decides what a reader
        -- sees, and all three prove the re-run started it.
        | childRow.workflowRecordStatus `notElem` [Enqueued, Pending, Success] ->
            Left ("expected the re-run's child started (ENQUEUED, PENDING, or SUCCESS), got: " <> show childRow.workflowRecordStatus)
        | otherwise -> Right ()
      (steps, childRow) ->
        Left ("expected one recorded refusal and the re-run's child, got: " <> show (steps, childRow))

-- | The conflicting start is recorded with no child: the parent ends
-- @ERROR@ with it, and the replay after the squatter's row is deleted ends
-- with the same rendered error instead of starting the child.
-- | A child start that refuses a taken id is recorded at the parent's step
-- too: the parent returns the error with @?@ and ends @ERROR@; once the
-- row holding the id is deleted the same start would go ahead, so only
-- the record keeps the replay failing. Mirrors Rust's
-- @a_rejected_child_start_replays_its_error_after_the_row_is_deleted@.
scenarioRejectStartRecorded :: forall m. (MonadMVar m, MonadFork m, MonadAsync m, MonadMask m, MonadTime m, MonadTimer m, MonadMonotonicTimeNSec m)
                            => WfFixture m -> m ConflictStartOutcome
scenarioRejectStartRecorded fx =
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    takenWid <- fx.wfFreshId "reused-child"
    parentWid <- fx.wfFreshId "reject-parent"
    let WorkflowId takenText = takenWid
        childKey = newQualifiedWorkflowName "child"
        parentKey = newQualifiedWorkflowName "reject-parent"
        childBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        childBody () _ = pure (Right 9)
    childRefE <- registerWorkflowRef dbos childKey childBody
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    let parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        parentBody () wctx = do
          started <-
            startChildWorkflow
              wctx
              childRef
              (startOptionsDefault {startWorkflowId = Just takenWid, startWorkflowIdReusePolicy = WorkflowIdReject})
              Nothing
          pure $ case started of
            Left err -> Left err
            Right _ -> Right "started"
    parentReg <- registerWorkflow dbos parentKey parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    -- The same function holds the id: a reuse even so.
    firstChild <- runWorkflow exec childKey takenWid Nothing
    case (firstChild :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) of
      Right _ -> pure ()
      other -> throwIO (userError ("the first child was supposed to run, got: " <> show other))
    first <- runWorkflow exec parentKey parentWid Nothing
    firstShown <- case (first :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) of
      Left err -> pure (Text.pack (show err))
      Right _ -> throwIO (userError "expected the refusing start to fail the parent")
    steps <- fx.wfListSteps parentWid
    parentRow <- fx.wfReadRow parentWid
    -- With the first child gone the id is free, so only the record keeps
    -- the replay failing.
    _ <- deleteWorkflow dbos takenWid >>= \r -> case r of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    forked <- forkWorkflow dbos parentWid (Just (ForkStep 1)) defaultForkOptions
    handle <- case forked of
      Left err -> throwIO (userError (show (err :: Error EngineOnly)))
      Right h -> pure (h :: WorkflowHandle m EngineOnly)
    _ <- dequeueWorkflows dbos >>= \r -> case r of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    replayed <- handleResult handle
    replayedShown <- case replayed of
      Left err -> pure (Text.pack (show err))
      Right _ -> throwIO (userError "expected the replay to end with the error")
    childRow <- do
      found <- runSystemDB fx.wfSystemDB (\db -> SystemDB.getWorkflow db takenWid)
      case found of
        Left err -> throwIO (userError (show err))
        Right row -> pure row
    pure
      ConflictStartOutcome
        { conflictFirst = firstShown,
          conflictReplayed = replayedShown,
          conflictSteps = steps,
          conflictStatus = fmap (.workflowRecordStatus) parentRow,
          conflictChildRow = childRow
        }

checkConflictStartRecorded :: ConflictStartOutcome -> Either String ()
checkConflictStartRecorded out
  | out.conflictFirst /= out.conflictReplayed =
      Left ("the replay ended differently: " <> show (out.conflictFirst, out.conflictReplayed))
  | not ("ConflictingWorkflow" `Text.isInfixOf` out.conflictFirst) =
      Left ("expected the conflicting start's error, got: " <> show out.conflictFirst)
  | out.conflictStatus /= Just Error =
      Left ("expected the parent ERROR, got: " <> show out.conflictStatus)
  | Just _ <- out.conflictChildRow =
      Left ("the replay started the child: " <> show out.conflictChildRow)
  | otherwise = case out.conflictSteps of
      [step]
        | step.stepRecordStepName /= "child" ->
            Left ("expected the child start's step, got: " <> show step.stepRecordStepName)
        | step.stepRecordChildWorkflowId /= Nothing ->
            Left ("the refusal recorded a child id: " <> show step.stepRecordChildWorkflowId)
        | step.stepRecordSerialization /= Just "portable_json" ->
            Left ("expected the refusal under the portable tag, got: " <> show step.stepRecordSerialization)
        | not (maybe False ("ConflictingWorkflow" `Text.isInfixOf`) step.stepRecordError) ->
            Left ("expected the recorded ConflictingWorkflow, got: " <> show step.stepRecordError)
        | otherwise -> Right ()
      other -> Left ("expected the one recorded refusal, got: " <> show other)

checkRejectStartRecorded :: ConflictStartOutcome -> Either String ()
checkRejectStartRecorded out
  | out.conflictFirst /= out.conflictReplayed =
      Left ("the replay ended differently: " <> show (out.conflictFirst, out.conflictReplayed))
  | not ("WorkflowIdInUse" `Text.isInfixOf` out.conflictFirst) =
      Left ("expected the refusing start's error, got: " <> show out.conflictFirst)
  | out.conflictStatus /= Just Error =
      Left ("expected the parent ERROR, got: " <> show out.conflictStatus)
  | Just _ <- out.conflictChildRow =
      Left ("the replay started the child: " <> show out.conflictChildRow)
  | otherwise = case out.conflictSteps of
      [step]
        | step.stepRecordStepName /= "child" ->
            Left ("expected the child start's step, got: " <> show step.stepRecordStepName)
        | step.stepRecordChildWorkflowId /= Nothing ->
            Left ("the refusal recorded a child id: " <> show step.stepRecordChildWorkflowId)
        | step.stepRecordSerialization /= Just "portable_json" ->
            Left ("expected the refusal under the portable tag, got: " <> show step.stepRecordSerialization)
        | not (maybe False ("WorkflowIdInUse" `Text.isInfixOf`) step.stepRecordError) ->
            Left ("expected the recorded WorkflowIdInUse, got: " <> show step.stepRecordError)
        | otherwise -> Right ()
      other -> Left ("expected the one recorded refusal, got: " <> show other)

-- | Two steps compose to 44 and list in order.
checkStepsTaken :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord]) -> Either String ()
checkStepsTaken (ran, steps) = case ran of
  Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
    Right 44 -> case map (.stepRecordStepName) steps of
      ["one", "two"] -> Right ()
      other -> Left ("expected two steps in order, got: " <> show other)
    other -> Left ("expected two steps to compose to 44, got: " <> show other)
  other -> Left ("expected the workflow to run, got: " <> show other)

-- | The row stayed @PENDING@ through the shutdown.
checkShutdownCancels :: (Maybe WorkflowStatus, Maybe WorkflowStatus) -> Either String ()
checkShutdownCancels (before, after)
  | before /= Just Pending = Left ("expected the gated row PENDING, got: " <> show before)
  | after /= Just Pending = Left ("expected the row PENDING after shutdown, got: " <> show after)
  | otherwise = Right ()

-- | The gated row stayed pending while the caller was gone, and the run
-- still finished once released.
checkDropFuture :: (Maybe WorkflowStatus, Either (Error EngineOnly) AwaitedOutcome) -> Either String ()
checkDropFuture (gated, settled)
  | gated /= Just Pending = Left ("expected the gated row PENDING, got: " <> show gated)
  | otherwise = case settled of
      Right (AwaitedSucceeded (Just output) serialization) -> do
        let stored = SerializedWorkflowValue output (Serialization <$> serialization)
            decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
        if decoded /= Right 7
          then Left ("expected the dropped run to record 7, got: " <> show decoded)
          else Right ()
      other -> Left ("expected the dropped run to finish, got: " <> show other)

-- | The parent carries the tenant; the child inherits nothing.
checkAttributes :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord, Text) -> Either String ()
checkAttributes (ran, parentRow, childRow, tenant)
  | Left err <- ran = Left ("expected the attributed run, got: " <> show err)
  | otherwise = case (parentRow, childRow) of
      (Just parent, Just child)
        | Just attributes <- parent.workflowRecordAttributes,
          tenant `Text.isInfixOf` attributes ->
            if child.workflowRecordAttributes /= Nothing
              then Left ("expected the child to inherit nothing, got: " <> show child.workflowRecordAttributes)
              else Right ()
        | otherwise -> Left ("expected the tenant in the parent's attributes, got: " <> show parent.workflowRecordAttributes)
      _ -> Left "expected both rows"

-- | The step error came back and its column holds the shortfall.
checkStepErrorRecorded :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord]) -> Either String ()
checkStepErrorRecorded (ran, steps) = case ran of
  Left (StepFailed step message)
    | step /= "charge" -> Left ("expected the failing step, got: " <> show step)
    | message /= "short by 12" -> Left ("expected the shortfall, got: " <> show message)
    | otherwise -> case steps of
        [StepRecord {stepRecordStepName = name, stepRecordError = Just recorded}]
          | name /= "charge" -> Left ("expected the failed step, got: " <> show name)
          | not ("short by 12" `Text.isInfixOf` recorded) -> Left ("expected the shortfall in " <> Text.unpack recorded)
          | otherwise -> Right ()
        other -> Left ("expected the failed step, got: " <> show other)
  other -> Left ("expected the step error back, got: " <> show other)

-- | The millisecond budget cancelled the run durably and the row reads
-- CANCELLED.
checkBudgetCancels :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowStatus) -> Either String ()
checkBudgetCancels (ran, status) = case ran of
  Left (WorkflowCancelled {}) ->
    unless (status == Just Cancelled) $ Left ("expected the row CANCELLED, got: " <> show status)
  other -> Left ("expected the durable cancellation, got: " <> show other)

-- | The body's own failure came back unchanged.
checkAppErrorRoundtrip :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue) -> Either String ()
checkAppErrorRoundtrip ran = case ran of
  Left (StepFailed step message)
    | step /= "flaky" -> Left ("expected the failing step name, got: " <> show step)
    | message /= "boom" -> Left ("expected the failing step message, got: " <> show message)
    | otherwise -> Right ()
  other -> Left ("expected the application error back, got: " <> show other)

-- | The backend failure came back as itself and the row stays pending
-- with no error column.
checkDbFailureNotOutcome :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord) -> Either String ()
checkDbFailureNotOutcome (ran, row) = case ran of
  Left (SystemDatabase _) -> case row of
    Just found
      | found.workflowRecordStatus /= Pending -> Left ("expected the row PENDING, got: " <> show found.workflowRecordStatus)
      | found.workflowRecordError /= Nothing -> Left ("expected no recorded error, got: " <> show found.workflowRecordError)
      | otherwise -> Right ()
    Nothing -> Left "expected the failing workflow's row"
  other -> Left ("expected the database failure back, got: " <> show other)

-- | The body's exception escaped and the row stays @PENDING@ with no
-- recorded error.
checkPanic :: (Either SomeException (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)), Maybe WorkflowRecord) -> Either String ()
checkPanic (outcome, row)
  | Right other <- outcome = Left ("expected the body's exception to escape, got: " <> show other)
  | otherwise = case row of
      Just found
        | found.workflowRecordStatus /= Pending -> Left ("expected the row PENDING, got: " <> show found.workflowRecordStatus)
        | found.workflowRecordError /= Nothing -> Left ("expected no recorded error, got: " <> show found.workflowRecordError)
        | otherwise -> Right ()
      Nothing -> Left "expected the panicking workflow's row"

-- | The unlaunched run was refused by name.
checkRunBeforeLaunch :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue) -> Either String ()
checkRunBeforeLaunch ran = case ran of
  Left NotLaunched {} -> Right ()
  other -> Left ("expected a not-launched refusal, got: " <> show other)

-- | The zero-argument run recorded no input.
checkZeroNoInput :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord) -> Either String ()
checkZeroNoInput (ran, row)
  | Left err <- ran = Left ("expected the workflow to run, got: " <> show err)
  | otherwise = case row of
      Just record
        | record.workflowRecordInput /= Nothing -> Left ("expected no recorded input, got: " <> show record.workflowRecordInput)
        | otherwise -> Right ()
      Nothing -> Left "expected the zero-argument row"

-- | The body saw its own row: the run decodes to True.
checkRowBeforeBody :: Either (Error EngineOnly) (Maybe SerializedWorkflowValue) -> Either String ()
checkRowBeforeBody ran = case ran of
  Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Bool of
    Right True -> Right ()
    other -> Left ("expected the body to find its own row, got: " <> show other)
  other -> Left ("expected the workflow to run, got: " <> show other)

-- | The refusal names the child start it wanted and the plain step it
-- found, and nothing was created to be orphaned.
checkPlainStepAtStart :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord) -> Either String ()
checkPlainStepAtStart (ran, missing) = case ran of
  Left (SystemDatabase (SystemDB.UnexpectedStep {stepId, expected, recorded}))
    | stepId /= 0 -> Left ("expected the refusal at step 0, got: " <> show stepId)
    | not ("child workflow start" `Text.isInfixOf` expected) -> Left ("expected the wanted start in " <> Text.unpack expected)
    | not ("plain step" `Text.isInfixOf` recorded) -> Left ("expected the plain step in " <> Text.unpack recorded)
    | missing /= Nothing -> Left ("expected nothing created to be orphaned, got: " <> show missing)
    | otherwise -> Right ()
  other -> Left ("expected the unexpected-step refusal, got: " <> show other)

-- | The child ran under its derived id and the replay adopted the
-- recorded child id rather than starting another.
checkDerivedChildAdopted :: (Text, [WorkflowId], Either (Error EngineOnly) [WorkflowId], Either (Error EngineOnly) AwaitedOutcome, Either (Error EngineOnly) (Maybe SerializedWorkflowValue)) -> Either String ()
checkDerivedChildAdopted (childText, recovered, dequeued, settled, replayed)
  -- The child start detaches the child onto the executor, so it may finish
  -- before the shutdown: recovery then finds nothing pending and the
  -- dequeue nothing enqueued. Either way the outcome and the adoption are
  -- the contract.
  | recovered /= [] && recovered /= [WorkflowId childText] = Left ("unexpected recovery result: " <> show recovered)
  | dequeued /= Right [] && dequeued /= Right [WorkflowId childText] = Left ("unexpected dequeue result: " <> show dequeued)
  | otherwise = case settled of
      Right (AwaitedSucceeded (Just raw) serialization) -> do
        let stored = SerializedWorkflowValue raw (Serialization <$> serialization)
            decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
        if decoded /= Right 42
          then Left ("expected recovery to run the recorded child, got: " <> show decoded)
          else case replayed of
            Right (Just stored') -> case decodeWorkflowValue "result" (Just stored') :: Either CodecError Text of
              Right adopted
                | adopted == childText -> Right ()
                | otherwise -> Left ("expected the replay to adopt " <> Text.unpack childText <> ", got: " <> Text.unpack adopted)
              Left err -> Left ("expected the replayed parent's id, got: " <> show err)
            other -> Left ("expected the replay to adopt the recorded child, got: " <> show other)
      other -> Left ("expected the recovered child to settle, got: " <> show other)

-- | The assigned child ran under its chosen id; the derived row never
-- existed and the replay adopted the chosen id.
checkAssignedChildAdopted :: (Text, [WorkflowId], Either (Error EngineOnly) [WorkflowId], Either (Error EngineOnly) AwaitedOutcome, Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord) -> Either String ()
checkAssignedChildAdopted (chosenText, recovered, dequeued, settled, replayed, chosenRow, derivedRow)
  | recovered /= [] && recovered /= [WorkflowId chosenText] = Left ("unexpected recovery result: " <> show recovered)
  | dequeued /= Right [] && dequeued /= Right [WorkflowId chosenText] = Left ("unexpected dequeue result: " <> show dequeued)
  | derivedRow /= Nothing = Left ("expected no derived row, got: " <> show derivedRow)
  | otherwise = case settled of
      Right (AwaitedSucceeded (Just raw) serialization) -> do
        let stored = SerializedWorkflowValue raw (Serialization <$> serialization)
            decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
        if decoded /= Right 7
          then Left ("expected the assigned child to record 7, got: " <> show decoded)
          else case chosenRow of
            Just row
              | row.workflowRecordParentWorkflowId /= Just (WorkflowId (Text.dropEnd (Text.length "-chosen") chosenText)) ->
                  Left ("expected the chosen row to link its parent, got: " <> show row.workflowRecordParentWorkflowId)
              | otherwise -> case replayed of
                  Right (Just stored') -> case decodeWorkflowValue "result" (Just stored') :: Either CodecError Text of
                    Right adopted
                      | adopted == chosenText -> Right ()
                      | otherwise -> Left ("expected the replay to adopt " <> Text.unpack chosenText <> ", got: " <> Text.unpack adopted)
                    Left err -> Left ("expected the replayed parent's id, got: " <> show err)
                  other -> Left ("expected the replay to adopt the recorded child, got: " <> show other)
            Nothing -> Left "expected the chosen row"
      other -> Left ("expected the recovered child to settle, got: " <> show other)

-- | The root ran and its row links no parent.
checkRootNoParent :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord) -> Either String ()
checkRootNoParent (ran, row)
  | Left err <- ran = Left ("expected the workflow to run, got: " <> show err)
  | otherwise = case row of
      Just found
        | found.workflowRecordParentWorkflowId /= Nothing ->
            Left ("expected no parent link, got: " <> show found.workflowRecordParentWorkflowId)
        | otherwise -> Right ()
      Nothing -> Left "expected the root row"

-- | The fan-out summed 0 + 1 + 2 with three children listed, all three
-- overlaps close to one delay rather than three.
checkFanout :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [WorkflowId], Int64) -> Either String ()
checkFanout (ran, children, tookMs)
  | Right (Just stored) <- ran, Right 3 == (decodeWorkflowValue "result" (Just stored) :: Either CodecError Int) =
      if length children /= 3
        then Left ("expected three children, got: " <> show children)
        else if tookMs >= 1200
          then Left ("three 400 ms children took " <> show tookMs <> " ms, which is serial rather than concurrent")
          else Right ()
  | otherwise = Left ("expected the fan-out total, got: " <> show ran)

-- | The parent ran and recorded the lone start; the abandoned child
-- still finished with 42, carries the parent link, and lists as the
-- parent's child.
checkUnawaitedChild ::
  (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord], Either (Error EngineOnly) AwaitedOutcome, Maybe WorkflowRecord, [WorkflowId], Text) ->
  Either String ()
checkUnawaitedChild (ran, steps, found, childRow, children, parentText)
  | Right _ <- ran = case steps of
      [StepRecord {stepRecordChildWorkflowId = Just (WorkflowId recorded)}]
        | recorded /= childText -> Left ("expected the lone start step to name " <> Text.unpack childText <> ", got: " <> show recorded)
        | otherwise -> case found of
            Right (AwaitedSucceeded (Just output) serialization) -> do
              let stored = SerializedWorkflowValue output (Serialization <$> serialization)
                  decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              if decoded /= Right 42
                then Left ("expected the abandoned child to record 42, got: " <> show decoded)
                else case childRow of
                  Just row
                    | row.workflowRecordParentWorkflowId /= Just (WorkflowId parentText) ->
                        Left ("expected the child to link its parent, got: " <> show row.workflowRecordParentWorkflowId)
                    | children /= [WorkflowId childText] ->
                        Left ("expected the child listed under its parent, got: " <> show children)
                    | otherwise -> Right ()
                  Nothing -> Left "expected the child row"
            other -> Left ("expected the abandoned child to finish, got: " <> show other)
      other -> Left ("expected the lone start step, got: " <> show other)
  | otherwise = Left ("expected the parent to run, got: " <> show ran)
  where
    childText = parentText <> "-0"

-- | The parent reported the refused child and the child's own error sits
-- in its column, in the child's channel rather than the parent's.
checkLiftChildError :: (Either (Error GaveUp) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord) -> Either String ()
checkLiftChildError (ran, childRow)
  | Right (Just stored) <- ran, Right True == (decodeWorkflowValue "result" (Just stored) :: Either CodecError Bool) =
      case childRow of
        Just row
          | row.workflowRecordStatus /= Error -> Left ("expected the child row Error, got: " <> show row.workflowRecordStatus)
          | otherwise -> case row.workflowRecordError of
              Just recorded -> case decodeErrorText recorded :: Either Text (Error Refused) of
                Right (ErrorApplication Refused) -> Right ()
                other -> Left ("expected the child's own error in the column, got: " <> show other)
              Nothing -> Left "the child recorded no error"
        Nothing -> Left "expected the child row"
  | otherwise = Left ("expected the parent to report the child's refusal, got: " <> show ran)

-- | The engine refused the in-step start and nothing was recorded.
checkChildInsideStepRefused :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord]) -> Either String ()
checkChildInsideStepRefused (result, steps)
  | Left (InsideStep operation) <- result = if operation == "starting a workflow"
      then if null steps
        then Right ()
        else Left ("expected no recorded start, got: " <> show steps)
      else Left ("expected the leaf refusal, got: " <> show operation)
  | otherwise = Left ("expected the leaf refusal, got: " <> show result)

-- | The captured-parent start is refused with the leaf error and records
-- nothing: same verdict as the in-step start, through the depth rather
-- than the scope field.
checkCaptureChildRefused :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord]) -> Either String ()
checkCaptureChildRefused (result, steps)
  | Left (InsideStep operation) <- result = if operation == "starting a workflow"
      then if null steps
        then Right ()
        else Left ("expected no recorded start, got: " <> show steps)
      else Left ("expected the leaf refusal, got: " <> show operation)
  | otherwise = Left ("expected the leaf refusal, got: " <> show result)

-- | The parent and the child each ended cancelled by the inherited
-- deadline, and the interrupted await left only the child start recorded.
checkCascadeDeadline :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Either (Error EngineOnly) AwaitedOutcome, [StepRecord]) -> Either String ()
checkCascadeDeadline (ran, childOutcome, steps)
  | not (parentCancelled ran) = Left ("expected the parent's own deadline cancellation, got: " <> show ran)
  | childOutcome /= Right AwaitedCancelled = Left ("expected the child cancelled independently, got: " <> show childOutcome)
  | map (.stepRecordStepName) steps /= ["child"] = Left ("expected the lone start only, got: " <> show steps)
  | otherwise = Right ()
  where
    parentCancelled result = case result of
      Left (WorkflowCancelled {}) -> True
      _ -> False

-- | Both children ran; the silent one inherited the parent's exact
-- instant, the declining one carries neither deadline nor timeout.
checkDeclinedDeadline :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord, Maybe WorkflowRecord) -> Either String ()
checkDeclinedDeadline (ran, parentRow, inheritedRow, detachedRow)
  | Right (Just stored) <- ran, Right 2 == (decodeWorkflowValue "result" (Just stored) :: Either CodecError Int) = case (parentRow, inheritedRow, detachedRow) of
      (Just parent, Just inheritedChild, Just detachedChild) -> case parent.workflowRecordDeadline of
        Nothing -> Left "the parent has no deadline"
        Just parentDeadline
          | inheritedChild.workflowRecordDeadline /= Just parentDeadline ->
              Left ("expected silence to inherit the parent's instant verbatim, got: " <> show inheritedChild.workflowRecordDeadline)
          | detachedChild.workflowRecordDeadline /= Nothing ->
              Left ("expected the declining child to carry no deadline, got: " <> show detachedChild.workflowRecordDeadline)
          | detachedChild.workflowRecordTimeout /= Nothing ->
              Left ("expected the declining child to carry no timeout, got: " <> show detachedChild.workflowRecordTimeout)
          | otherwise -> Right ()
      _ -> Left ("expected all three rows, got: " <> show (parentRow, inheritedRow, detachedRow))
  | otherwise = Left ("expected the parent's output of both children, got: " <> show ran)

-- | The child's own budget won: it records its own timeout and a
-- deadline that outlives its parent's.
checkChildBudgetWins :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord) -> Either String ()
checkChildBudgetWins (ran, parentRow, childRow)
  | Right _ <- ran = case (parentRow, childRow) of
      (Just parent, Just child) -> case (parent.workflowRecordDeadline, child.workflowRecordDeadline) of
        (Just parentDeadline, Just childDeadline)
          | SystemDB.timestampToEpochMs childDeadline <= SystemDB.timestampToEpochMs parentDeadline ->
              Left "expected the child's deadline to outlive its parent's"
          | child.workflowRecordTimeout /= Just (secondsDuration 3600) ->
              Left ("expected the child's own timeout, got: " <> show child.workflowRecordTimeout)
          | otherwise -> Right ()
        other -> Left ("expected both deadlines, got: " <> show other)
      _ -> Left ("expected both rows, got: " <> show (parentRow, childRow))
  | otherwise = Left ("expected the parent to run, got: " <> show ran)

-- | The child carries the parent's exact deadline instant, not a fresh
-- budget; only the parent's row records the timeout it came from.
checkDeadlineInherited :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), Maybe WorkflowRecord, Maybe WorkflowRecord) -> Either String ()
checkDeadlineInherited (ran, parentRow, childRow)
  | Right _ <- ran = case (parentRow, childRow) of
      (Just parent, Just child) -> case parent.workflowRecordDeadline of
        Nothing -> Left "the parent has no deadline"
        Just deadline'
          | child.workflowRecordDeadline /= Just deadline' -> Left ("expected the same instant, not a fresh budget, in " <> show child.workflowRecordDeadline)
          | child.workflowRecordTimeout /= Nothing -> Left ("expected the child to record no timeout, got: " <> show child.workflowRecordTimeout)
          | parent.workflowRecordTimeout /= Just (secondsDuration 300) -> Left ("expected the parent's own timeout, got: " <> show parent.workflowRecordTimeout)
          | otherwise -> Right ()
      _ -> Left ("expected both rows, got: " <> show (parentRow, childRow))
  | otherwise = Left ("expected the parent to run, got: " <> show ran)

-- | The parent ends on the child's cancellation, which is also what its
-- recorded await column holds; the parent row failed and the child row
-- is cancelled.
checkCancelledChildAwaited ::
  (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord], Maybe WorkflowStatus, Maybe WorkflowStatus, Text) ->
  Either String ()
checkCancelledChildAwaited (ran, steps, parentStatus, childStatus, childText)
  | not (namesChild ran) = Left ("expected an awaited cancellation of " <> Text.unpack childText <> ", got: " <> show ran)
  | not (recordsChild steps) = Left ("expected a recorded awaited cancellation of " <> Text.unpack childText <> ", got: " <> show steps)
  | parentStatus /= Just Error = Left ("expected the failed parent row, got: " <> show parentStatus)
  | childStatus /= Just Cancelled = Left ("expected the cancelled child row, got: " <> show childStatus)
  | otherwise = Right ()
  where
    namesChild result = case result of
      Left (AwaitedWorkflowCancelled {workflowId}) -> workflowId == childText
      _ -> False
    recordsChild rows = case [row | row <- rows, row.stepRecordStepName == "DBOS.getResult"] of
      [awaitRow] -> case awaitRow.stepRecordError of
        Just recorded -> case decodeErrorText recorded :: Either Text (Error EngineOnly) of
          Right (AwaitedWorkflowCancelled {workflowId}) -> workflowId == childText
          _ -> False
        Nothing -> False
      _ -> False

-- | The fast step won and the losing step's token fired for its watcher.
checkLosingTokenFired :: (Int, Bool) -> Either String ()
checkLosingTokenFired (n, fired)
  | n /= 1 = Left ("expected the fast step's 1, got: " <> show n)
  | not fired = Left "the losing step's token never fired"
  | otherwise = Right ()

-- | The interrupted arm won: the control signal comes back, no step row
-- exists, and the row stays @PENDING@ for a later recovery.
checkControlSelect :: (Either (Error EngineOnly) (Maybe SerializedWorkflowValue), [StepRecord], Maybe WorkflowStatus, Text) -> Either String ()
checkControlSelect (ran, steps, status, parentText)
  | Left (Interrupted {workflowId}) <- ran, workflowId == parentText = case steps of
      [] -> if status == Just Pending
        then Right ()
        else Left ("expected the parent row PENDING, got: " <> show status)
      other -> Left ("expected no step rows, got: " <> show other)
  | otherwise = Left ("expected the control signal back, got: " <> show ran)

-- | The await arm won and produced the child's value; the losing step
-- left no row, so the history is the start, the await, and the select.
checkSelectStepRaces :: (Int, [StepRecord], Text) -> Either String ()
checkSelectStepRaces (n, steps, childText)
  | n /= 7 = Left ("expected the await arm's 7, got: " <> show n)
  | table /= expected = Left ("expected the start, the await and the select, got: " <> show table)
  | otherwise = Right ()
  where
    table = map (\row -> (row.stepRecordStepId, row.stepRecordStepName, row.stepRecordChildWorkflowId)) steps
    expected =
      [ (0, "child", Just (WorkflowId childText)),
        -- Id 1 is the losing step, built and dropped without a row.
        (2, "DBOS.getResult", Just (WorkflowId childText)),
        (3, "DBOS.selectStep", Nothing)
      ]

-- | Each await sits immediately behind its own start: the pairs are
-- (0,1), (2,3), (4,5), naming the child derived at the start's position.
checkStepIdPairs :: (Int, [StepRecord], Text) -> Either String ()
checkStepIdPairs (n, steps, parentText)
  | n /= 6 = Left ("expected 1 + 2 + 3, got: " <> show n)
  | table /= expected = Left ("expected each await behind its own start, got: " <> show table)
  | otherwise = Right ()
  where
    table = map (\row -> (row.stepRecordStepId, row.stepRecordStepName, row.stepRecordChildWorkflowId)) steps
    child :: Int -> WorkflowId
    child k = WorkflowId (parentText <> "-" <> Text.pack (show k))
    expected =
      [ (0, "child", Just (child 0)),
        (1, "DBOS.getResult", Just (child 0)),
        (2, "child", Just (child 2)),
        (3, "DBOS.getResult", Just (child 2)),
        (4, "child", Just (child 4)),
        (5, "DBOS.getResult", Just (child 4))
      ]

-- | What every shared task case needs from io-classes: a constraint
-- synonym, not a class — the bodies stay ordinary functions, and the one
-- stack-specific operation arrives as an argument.
type TaskCase m =
  (MonadFork m, MonadMask m, MonadSTM m, MonadMVar m, MonadDelay m, MonadAsync m, MonadTimer m, MonadMonotonicTimeNSec m)

-- | No swept task may be miscounted as aborted: any nonzero sweep count
-- fails the case. Shared with the sim tree, which judges its leaves by
-- this same assertion.
checkNoMiscounts :: [Int] -> IO ()
checkNoMiscounts counts = case [count | count <- counts, count /= 0] of
  [] -> pure ()
  miscounts -> fail ("dead tasks swept as aborted: " <> show (length miscounts))

-- | Two parked tasks: the sweep must count both. The parks are never
-- waited out — the sweep kills the sleepers — but the margin is what keeps
-- a parent descheduled between the forks and the sweep from finding the
-- tasks finished on their own. The simulator is immune either way: time
-- advances only when nothing is runnable.
taskAbortAllWaits :: TaskCase m => m Int
taskAbortAllWaits = do
  tasks <- newTasks
  _ <- spawnTracked tasks "first" (threadDelay 10000000)
  _ <- spawnTracked tasks "second" (threadDelay 10000000)
  length <$> abortAll tasks

-- | A trivial body, awaited past its departure, leaves the sweep nothing
-- to kill.
taskFinishedNotRegistered :: (MonadFork m, MonadMask m, MonadSTM m, MonadMVar m)
                          => (ThreadId m -> m ()) ->
                             m Int
taskFinishedNotRegistered awaitDeparture = do
  tasks <- newTasks
  spawned <- spawnTracked tasks "brief" (pure ())
  mapM_ awaitDeparture spawned
  length <$> abortAll tasks

-- | The sweep closes the registry; the arrival that follows is refused and
-- must never run.
taskRefusedAfterSweep :: TaskCase m => m Bool
taskRefusedAfterSweep = do
  tasks <- newTasks
  _ <- abortAll tasks
  ran <- newTVarIO False
  _ <- spawnTracked tasks "refused" (threadDelay 1000 >> atomically (writeTVar ran True))
  threadDelay 5000
  readTVarIO ran

taskEmptySweep :: (MonadFork m, MonadSTM m, MonadMVar m) => m Int
taskEmptySweep = length <$> (newTasks >>= abortAll)

-- | A body that has already returned but has not yet deregistered when
-- the sweep looks must not be reported as cancelled. The child hands its
-- completion to the parent and returns; the sweep races the child's
-- deregistration. On the cooperative simulator the child deregisters
-- before yielding, so the sweep finds nothing; on the IO scheduler the
-- sweep can see the entry and kill a finished thread, and must subtract
-- what the departure recorded.
taskFinishRacingSweep :: TaskCase m => m Int
taskFinishRacingSweep = do
  tasks <- newTasks
  done <- newEmptyMVar
  _ <- spawnTracked tasks "brief" (putMVar done () >> pure ())
  takeMVar done
  length <$> abortAll tasks

-- | A trivial body can depart between the fork and the parent's
-- registration on a preemptive scheduler; the registration must consume
-- that early departure rather than list a dead thread, so a later sweep
-- finds nothing to kill and counts nothing aborted. Only the IO half can
-- reach that interleaving — the cooperative simulator never preempts a
-- forked child (io-sim's @Fork@ appends it to the runqueue and resumes the
-- parent), so there the case checks the ordinary path.
taskEarlyFinishNotSwept :: (MonadFork m, MonadMask m, MonadSTM m, MonadMVar m)
                        => (ThreadId m -> m ()) ->
                           m [Int]
taskEarlyFinishNotSwept awaitDeparture =
  mapM
    ( \_ -> do
        tasks <- newTasks
        spawned <- spawnTracked tasks "brief" (pure ())
        mapM_ awaitDeparture spawned
        length <$> abortAll tasks
    )
    [1 .. 200 :: Int]

-- | The bounded wait reports a set that empties in time as ended, and one
-- still held at the deadline as not — without cancelling anything either
-- way. The held task is parked on an MVar, so the deadline arm is
-- deterministic rather than a race with a sleeper.
taskWaitAllUntilDeadline :: TaskCase m => m (Bool, Bool)
taskWaitAllUntilDeadline = do
  tasks <- newTasks
  gate <- newEmptyMVar
  _ <- spawnTracked tasks "held" (takeMVar gate)
  now <- getMonotonicTimeNSec
  beforeRelease <- waitAllUntil tasks (now + 100000000)
  putMVar gate ()
  afterRelease <- waitAllUntil tasks (now + 5000000000)
  _ <- abortAll tasks
  pure (beforeRelease, afterRelease)

-- | The registry reports the labels spawns were tracked under. Sorted in
-- the body: registration prepends, so the raw order is an implementation
-- detail the trees must not pin.
taskRunningNamesTasks :: TaskCase m => m [Text]
taskRunningNamesTasks = do
  tasks <- newTasks
  _ <- spawnTracked tasks "alpha" (threadDelay 10000000)
  _ <- spawnTracked tasks "beta" (threadDelay 10000000)
  names <- sort <$> runningTasks tasks
  _ <- abortAll tasks
  pure names

-- | The test-local JSON channels the scenarios cross columns with: the
-- child's nullary refusal and the parent's own channel in the lift case.
data Refused = Refused
  deriving stock (Eq, Show)

instance ToJSON Refused where
  toJSON _ = object []

instance FromJSON Refused where
  parseJSON _ = pure Refused

data GaveUp = GaveUp
  deriving stock (Eq, Show)

instance ToJSON GaveUp where
  toJSON _ = object []

instance FromJSON GaveUp where
  parseJSON _ = pure GaveUp
-- | A workflow enqueued by name is the parent's child, and a replay
-- does not enqueue it again: the child's id joins the parent and the
-- step, the launch is recorded under the workflow's name, and forking
-- the parent past its step hands back the same handle while the
-- name-filtered listing still names one row. Mirrors Rust's by-name
-- child test; the worker name is scenario-unique, as listings are
-- global over the shared database.
-- | A workflow enqueued by name is the parent's child, and a replay
-- does not enqueue it again: the child's id joins the parent and the
-- step, the launch is recorded under the workflow's name, and forking
-- the parent past its step hands back the same handle while the
-- name-filtered listing still names one row. Mirrors Rust's by-name
-- child test, except the parent hands back the child's id and the
-- scenario awaits it outside: a queued child needs a driven pass, and
-- bare launches never fork a supervisor tick. The worker name is
-- scenario-unique, as listings are global over the shared database.
scenarioEnqueueByNameIsChild :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                             => WfFixture m -> m (Int, WorkflowStatus, Maybe Text, Text, Int, Text, Text, Int)
scenarioEnqueueByNameIsChild fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    fresh <- fx.wfFreshId "byname"
    let WorkflowId base = fresh
        workerName = "byname-worker"
        queueName = "byname-queue-" <> base
        parentWid = WorkflowId ("hs-l2-wf-byname-parent-" <> base)
        WorkflowId parentText = parentWid
        childWid = WorkflowId (parentText <> "-0")
        WorkflowId childText = childWid
        QueueName internal = internalQueueName
        workerBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        workerBody n _ = pure (Right (n * 2))
        parentBody :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        parentBody n wctx = do
          -- The fixture version is named so the shared database's sweep
          -- can claim the child: an unnamed row waits for the global
          -- latest, which another application's row always holds here.
          -- The unnamed shape is pinned by the row test instead.
          started <- enqueueWorkflowByNameInWorkflow wctx (newQualifiedWorkflowName workerName) (((enqueueOptionsNew queueName) {appVersion = Just fx.wfIdentity.identityAppVersion}) :: EnqueueOptions) (Just (encodeWorkflowInput n))
          pure $ case started of
            Left err -> Left err
            Right handle -> Right handle.workflowId
    _ <- registerWorkflowRef dbos (newQualifiedWorkflowName workerName) workerBody >>= either (throwIO . userError . show) pure
    parentReg <- registerWorkflow dbos (newQualifiedWorkflowName "byname-parent") parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunchWithQueues (Just [queueName, internal]) dbos
    _ <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate >>= either (throwIO . userError . show) pure
    ran <- (runWorkflow exec (newQualifiedWorkflowName "byname-parent") parentWid (Just (encodeWorkflowInput (21 :: Int))) :: m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
    firstChild <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
        Right childId -> pure childId
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the child's id, got: " <> show other))
    unless (firstChild == childText) $
      throwIO (userError ("the child id joins parent and step: " <> show firstChild))
    -- The queued child runs on a driven pass, here and under the sim.
    _ <- dequeueWorkflows dbos >>= either (throwIO . userError . show) pure
    childHandle <- (retrieveWorkflow dbos childWid :: m (Either (Error EngineOnly) (WorkflowHandle m EngineOnly))) >>= either (throwIO . userError . show) pure
    childOutcome <- (handleResult childHandle :: m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
    out <- case childOutcome of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> pure n
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the doubled output, got: " <> show other))
    child <- fx.wfReadRow childWid >>= maybe (throwIO (userError "expected the child row")) pure
    unless (child.workflowRecordParentWorkflowId == Just parentWid) $
      throwIO (userError ("the child names the wrong parent: " <> show child.workflowRecordParentWorkflowId))
    steps <- fx.wfListSteps parentWid
    (step0name, stepCount) <- case steps of
      first : _ -> pure (first.stepRecordStepName, length steps)
      [] -> throwIO (userError "expected the parent's steps")
    unless (step0name == workerName) $
      throwIO (userError ("the launch recorded under the wrong name: " <> show step0name))
    replayedIds <- forkFrom dbos [parentWid] (ForkStep 1) defaultForkOptions >>= either (throwIO . userError . show) pure
    replayId <- case replayedIds of
      [rid] -> pure rid
      _ -> throwIO (userError ("expected one replayed parent, got: " <> show replayedIds))
    _ <- dequeueWorkflows dbos >>= either (throwIO . userError . show) pure
    replayHandle <- (retrieveWorkflow dbos replayId :: m (Either (Error EngineOnly) (WorkflowHandle m EngineOnly))) >>= either (throwIO . userError . show) pure
    replayOutcome <- (handleResult replayHandle :: m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
    replayedChild <- case replayOutcome of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
        Right childId -> pure childId
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the replayed child id, got: " <> show other))
    -- Scoped to this fixture's app: names are global over the shared
    -- database, and other runs' rows would count otherwise.
    workers <- listWorkflows dbos (defaultWorkflowFilter {workflowFilterAppScope = Named [fx.wfIdentity.identityAppName], workflowFilterNames = [workerName]}) >>= either (throwIO . userError . show) pure
    pure (out, child.workflowRecordStatus, child.workflowRecordName, step0name, stepCount, firstChild, replayedChild, length workers)

-- | The doubled output, the child's row and the parent's step, the same
-- child id on replay, and the single worker row: no second enqueue.
checkEnqueueByNameIsChild :: (Int, WorkflowStatus, Maybe Text, Text, Int, Text, Text, Int) -> Either String ()
checkEnqueueByNameIsChild (out, status, name, step0name, stepCount, firstChild, replayedChild, workerCount)
  | out /= 42 = Left ("expected the doubled output, got: " <> show out)
  | status /= Success = Left ("expected the child SUCCESS, got: " <> show status)
  | name /= Just "byname-worker" = Left ("expected the worker name, got: " <> show name)
  | step0name /= "byname-worker" = Left ("expected the launch under the worker name, got: " <> show step0name)
  | stepCount /= 1 = Left ("expected the launch alone, got: " <> show stepCount)
  | replayedChild /= firstChild = Left ("expected the replay to hand back the same child, got: " <> show (firstChild, replayedChild))
  | workerCount /= 1 = Left ("expected one worker row, got: " <> show workerCount)
  | otherwise = Right ()

-- | An enqueue by name outside a workflow writes a client's row: the
-- executor's own, belonging to this application unless another is named,
-- and carrying no version unless one is named. Inside a step body it is
-- refused, as a start is, and an empty name is refused up front. Returns
-- the own row's status, name, version and parent, the other row's app
-- and version, the in-step refusal's operation, the empty refusal's
-- operation, and the operator's recorded steps. Mirrors Rust's row test.
scenarioEnqueueByNameWritesRow :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                               => WfFixture m -> m (WorkflowStatus, Maybe Text, Maybe Text, Maybe WorkflowId, Maybe Text, Maybe Text, Text, Text, [(Int, Text)])
scenarioEnqueueByNameWritesRow fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    fresh <- fx.wfFreshId "byname-row"
    let WorkflowId base = fresh
        queueName = "idle-queue-" <> base
        ownWid = WorkflowId ("hs-l2-wf-byname-own-" <> base)
        otherWid = WorkflowId ("hs-l2-wf-byname-other-" <> base)
        operatorWid = WorkflowId ("hs-l2-wf-byname-instep-" <> base)
        WorkflowId ownText = ownWid
        WorkflowId otherText = otherWid
        operatorBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        operatorBody () wctx = do
          refused <- runStep wctx "enqueues" $ \_ -> do
            outcome <- (enqueueWorkflowByNameInWorkflow wctx (newQualifiedWorkflowName "remote-row") (enqueueOptionsNew queueName) Nothing :: m (Either (Error EngineOnly) (WorkflowHandle m EngineOnly)))
            case outcome of
              Left (InsideStep operation) -> pure operation
              Left err -> throwIO (userError (show err))
              Right _ -> throwIO (userError "the in-step enqueue was not refused")
          case refused of
            Left err -> pure (Left err)
            Right operation -> pure (Right operation)
    operatorReg <- registerWorkflow dbos (newQualifiedWorkflowName "instep") operatorBody
    case operatorReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    _ <- enqueueWorkflowByName dbos (newQualifiedWorkflowName "remote-row") ((enqueueOptionsNew queueName) {workflowId = Just ownText}) Nothing >>= either (throwIO . userError . show) pure
    _ <- enqueueWorkflowByName dbos (newQualifiedWorkflowName "remote-row") (((enqueueOptionsNew queueName) {workflowId = Just otherText, appName = Just "by-name-other-app", appVersion = Just "v9"}) :: EnqueueOptions) Nothing >>= either (throwIO . userError . show) pure
    own <- fx.wfReadRow ownWid >>= maybe (throwIO (userError "expected the own row")) pure
    unless (own.workflowRecordAppName == Just fx.wfIdentity.identityAppName) $
      throwIO (userError ("the own row names the wrong app: " <> show own.workflowRecordAppName))
    other <- fx.wfReadRow otherWid >>= maybe (throwIO (userError "expected the other row")) pure
    ran <- (runWorkflow exec (newQualifiedWorkflowName "instep") operatorWid Nothing :: m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
    inStepOp <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
        Right operation -> pure operation
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the refusal's operation, got: " <> show other))
    emptyOutcome <- enqueueWorkflowByName dbos (newQualifiedWorkflowName "") (enqueueOptionsNew queueName) Nothing
    emptyOp <- case emptyOutcome of
      Left (InvalidArgument operation _) -> pure operation
      other -> throwIO (userError ("expected the empty refusal, got: " <> show other))
    steps <- fx.wfListSteps operatorWid
    pure (own.workflowRecordStatus, own.workflowRecordName, own.workflowRecordAppVersion, own.workflowRecordParentWorkflowId, other.workflowRecordAppName, other.workflowRecordAppVersion, inStepOp, emptyOp, [(fromIntegral record.stepRecordStepId, record.stepRecordStepName) | record <- steps])

-- | The client's row, the named app and version, the two refusals, and
-- the single managing step.
checkEnqueueByNameWritesRow :: (WorkflowStatus, Maybe Text, Maybe Text, Maybe WorkflowId, Maybe Text, Maybe Text, Text, Text, [(Int, Text)]) -> Either String ()
checkEnqueueByNameWritesRow (status, name, version, parent, otherApp, otherVersion, inStepOp, emptyOp, steps)
  | status /= Enqueued = Left ("expected the row ENQUEUED, got: " <> show status)
  | name /= Just "remote-row" = Left ("expected the remote name, got: " <> show name)
  | version /= Nothing = Left ("expected no version unless named, got: " <> show version)
  | parent /= Nothing = Left ("expected no parent outside a workflow, got: " <> show parent)
  | otherApp /= Just "by-name-other-app" = Left ("expected the named app, got: " <> show otherApp)
  | otherVersion /= Just "v9" = Left ("expected the named version, got: " <> show otherVersion)
  | inStepOp /= "enqueueing a workflow" = Left ("expected the in-step refusal, got: " <> show inStepOp)
  | emptyOp /= "enqueue a workflow" = Left ("expected the empty refusal, got: " <> show emptyOp)
  | steps /= [(0, "enqueues")] = Left ("expected the managing step alone, got: " <> show steps)
  | otherwise = Right ()

-- | A deduplicated enqueue by name replays the way it went: a join hands
-- back the holder, a refusal records and replays after the key is free,
-- and no replay enqueues again. Returns the two first answers, the two
-- replayed answers, and the worker-name row count. Mirrors Rust's dedup
-- test; the worker name is scenario-unique, as listings are global.
scenarioEnqueueByNameDedupReplays :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                                  => WfFixture m -> m (Text, Text, Text, Text, Int)
scenarioEnqueueByNameDedupReplays fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    fresh <- fx.wfFreshId "byname-dedup"
    let WorkflowId base = fresh
        workerName = "remote-dedup"
        queueName = "idle-dedup-queue-" <> base
        holderWid = WorkflowId ("hs-l2-wf-byname-holder-" <> base)
        joinsWid = WorkflowId ("hs-l2-wf-byname-joins-" <> base)
        refusedWid = WorkflowId ("hs-l2-wf-byname-refused-" <> base)
        WorkflowId holderText = holderWid
        QueueName internal = internalQueueName
        parentBody :: forall exec. Text -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
        parentBody policy wctx = do
          let duplication = if policy == ("join" :: Text) then ReturnExisting else Reject
          enqueued <- enqueueWorkflowByNameInWorkflow wctx (newQualifiedWorkflowName workerName) ((enqueueOptionsNew queueName) {deduplicationId = Just ("held-key-" <> base), duplicationPolicy = duplication} :: EnqueueOptions) Nothing
          pure $ case enqueued of
            Left (SystemDatabase (SysErr.QueueDeduplicated {})) -> Right "deduplicated"
            Left err -> Left err
            Right handle -> Right ("joined " <> handle.workflowId)
    parentReg <- registerWorkflow dbos (newQualifiedWorkflowName "dedup-parent") parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunchWithQueues (Just [internal]) dbos
    _ <- enqueueWorkflowByName dbos (newQualifiedWorkflowName workerName) (((enqueueOptionsNew queueName) {workflowId = Just holderText, deduplicationId = Just ("held-key-" <> base)}) :: EnqueueOptions) Nothing >>= either (throwIO . userError . show) pure
    let runParent wid policy = do
          ran <- (runWorkflow exec (newQualifiedWorkflowName "dedup-parent") wid (Just (encodeWorkflowInput policy)) :: m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
          case ran of
            Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
              Right answer -> pure answer
              Left err -> throwIO (userError (show err))
            other -> throwIO (userError ("expected the parent's answer, got: " <> show other))
        replayParent wid = do
          replayedIds <- forkFrom dbos [wid] (ForkStep 1) defaultForkOptions >>= either (throwIO . userError . show) pure
          replayId <- case replayedIds of
            [rid] -> pure rid
            _ -> throwIO (userError ("expected one replay, got: " <> show replayedIds))
          replayHandle <- (retrieveWorkflow dbos replayId :: m (Either (Error EngineOnly) (WorkflowHandle m EngineOnly))) >>= either (throwIO . userError . show) pure
          -- The forked replay runs on a driven pass, here and under the sim.
          _ <- dequeueWorkflows dbos >>= either (throwIO . userError . show) pure
          replayOutcome <- (handleResult replayHandle :: m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
          case replayOutcome of
            Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
              Right answer -> pure answer
              Left err -> throwIO (userError (show err))
            other -> throwIO (userError ("expected the replayed answer, got: " <> show other))
    joinText <- runParent joinsWid ("join" :: Text)
    unless (joinText == "joined " <> holderText) $
      throwIO (userError ("the join named the wrong holder: " <> show joinText))
    refusedText <- runParent refusedWid ("reject" :: Text)
    joinSteps <- fx.wfListSteps joinsWid
    case joinSteps of
      [only] -> unless (only.stepRecordChildWorkflowId == Just holderWid) $
        throwIO (userError ("the join recorded the wrong child: " <> show only.stepRecordChildWorkflowId))
      _ -> throwIO (userError ("expected the join's one step, got: " <> show (length joinSteps)))
    replayJoin <- replayParent joinsWid
    _ <- deleteWorkflows dbos [holderWid] False >>= either (throwIO . userError . show) pure
    replayRefused <- replayParent refusedWid
    remotes <- listWorkflows dbos (defaultWorkflowFilter {workflowFilterAppScope = Named [fx.wfIdentity.identityAppName], workflowFilterNames = [workerName]}) >>= either (throwIO . userError . show) pure
    pure (joinText, refusedText, replayJoin, replayRefused, length remotes)

-- | The join hands back the holder on both runs, the refusal replays
-- after the delete, and no replay enqueued again.
checkEnqueueByNameDedupReplays :: (Text, Text, Text, Text, Int) -> Either String ()
checkEnqueueByNameDedupReplays (joinText, refusedText, replayJoin, replayRefused, remoteCount)
  | not ("joined " `Text.isPrefixOf` joinText) = Left ("expected the join to name its holder, got: " <> show joinText)
  | refusedText /= "deduplicated" = Left ("expected the handled refusal, got: " <> show refusedText)
  | replayJoin /= joinText = Left ("expected the replay to join the same holder, got: " <> show (joinText, replayJoin))
  | replayRefused /= "deduplicated" = Left ("expected the replayed refusal, got: " <> show replayRefused)
  | remoteCount /= 0 = Left ("expected no second enqueue, got: " <> show remoteCount)
  | otherwise = Right ()

-- | An enqueue by name inherits its parent's deadline unless given a
-- budget of its own, and an unbounded one records neither. Returns the
-- parent's deadline, the three children's timeouts and deadlines.
-- Mirrors Rust's deadline test.
scenarioEnqueueByNameDeadlines :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                               => WfFixture m -> m (Maybe Timestamp, Maybe Timestamp, Maybe Duration, Maybe Timestamp, Maybe Duration, Maybe Timestamp)
scenarioEnqueueByNameDeadlines fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    fresh <- fx.wfFreshId "byname-deadline"
    let WorkflowId base = fresh
        queueName = "idle-deadline-queue-" <> base
        parentWid = WorkflowId ("hs-l2-wf-byname-deadline-parent-" <> base)
        WorkflowId parentText = parentWid
        QueueName internal = internalQueueName
        parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) ())
        parentBody () wctx = do
          let enqueue opts = do
                outcome <- (enqueueWorkflowByNameInWorkflow wctx (newQualifiedWorkflowName "remote-deadline") opts Nothing :: m (Either (Error EngineOnly) (WorkflowHandle m EngineOnly)))
                case outcome of
                  Left err -> pure (Left err)
                  Right _ -> pure (Right ())
          first <- enqueue (enqueueOptionsNew queueName)
          case first of
            Left err -> pure (Left err)
            Right () -> do
              second <- enqueue (((enqueueOptionsNew queueName) {timeout = Explicit (secondsDuration 3600)}) :: EnqueueOptions)
              case second of
                Left err -> pure (Left err)
                Right () -> do
                  third <- enqueue (((enqueueOptionsNew queueName) {timeout = None}) :: EnqueueOptions)
                  case third of
                    Left err -> pure (Left err)
                    Right () -> pure (Right ())
    parentRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "deadline-parent") parentBody >>= either (throwIO . userError . show) pure
    exec <- fx.wfLaunchWithQueues (Just [internal]) dbos
    (started :: Either (Error EngineOnly) (WorkflowHandle m EngineOnly)) <- startWorkflow exec parentRef (startOptionsDefault {startWorkflowId = Just parentWid, startTimeout = Explicit (secondsDuration 600)}) Nothing
    parentHandle <- case started of
      Left err -> throwIO (userError (show err))
      Right handle -> pure handle
    _ <- handleResult parentHandle >>= either (throwIO . userError . show) pure
    parent <- fx.wfReadRow parentWid >>= maybe (throwIO (userError "expected the parent row")) pure
    inherited <- fx.wfReadRow (WorkflowId (parentText <> "-0")) >>= maybe (throwIO (userError "expected the inherited row")) pure
    own <- fx.wfReadRow (WorkflowId (parentText <> "-1")) >>= maybe (throwIO (userError "expected the budgeted row")) pure
    unbounded <- fx.wfReadRow (WorkflowId (parentText <> "-2")) >>= maybe (throwIO (userError "expected the unbounded row")) pure
    pure (parent.workflowRecordDeadline, inherited.workflowRecordDeadline, own.workflowRecordTimeout, own.workflowRecordDeadline, unbounded.workflowRecordTimeout, unbounded.workflowRecordDeadline)

-- | The inherited deadline is the parent's, the budget records its own
-- timeout but no deadline yet (a claim stamps that, as the queue tests
-- pin), and the unbounded row records neither.
checkEnqueueByNameDeadlines :: (Maybe Timestamp, Maybe Timestamp, Maybe Duration, Maybe Timestamp, Maybe Duration, Maybe Timestamp) -> Either String ()
checkEnqueueByNameDeadlines (parentDl, inheritedDl, ownTo, ownDl, unboundedTo, unboundedDl)
  | parentDl == Nothing = Left "expected the parent to carry a deadline"
  | inheritedDl /= parentDl = Left ("expected the inherited deadline to match, got: " <> show (parentDl, inheritedDl))
  | ownTo /= Just (secondsDuration 3600) = Left ("expected the budgeted timeout, got: " <> show ownTo)
  | ownDl /= Nothing = Left ("expected no deadline until a claim stamps one, got: " <> show ownDl)
  | unboundedTo /= Nothing = Left ("expected no unbounded timeout, got: " <> show unboundedTo)
  | unboundedDl /= Nothing = Left ("expected no unbounded deadline, got: " <> show unboundedDl)
  | otherwise = Right ()

-- | Bad options are refused before the step id is taken: five invalid
-- argument details and one recorded step. Mirrors Rust's no-step test.
scenarioEnqueueByNameRefusedNoStep :: forall m. (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadMonotonicTimeNSec m, MonadTime m)
                                   => WfFixture m -> m ([Text], [(Int, Text)])
scenarioEnqueueByNameRefusedNoStep fx = do
  bracket fx.wfNewDBOS shutdown $ \dbos -> do
    fresh <- fx.wfFreshId "byname-refused"
    let WorkflowId base = fresh
        queueName = "idle-queue-" <> base
        parentWid = WorkflowId ("hs-l2-wf-byname-refusing-" <> base)
        badApp = ((enqueueOptionsNew queueName) {appName = Just "Not-An-App"} :: EnqueueOptions)
        emptyVersion = ((enqueueOptionsNew queueName) {appVersion = Just ""} :: EnqueueOptions)
        emptyId = ((enqueueOptionsNew queueName) {workflowId = Just ""} :: EnqueueOptions)
        emptyDedup = ((enqueueOptionsNew queueName) {deduplicationId = Just ""} :: EnqueueOptions)
        emptyPartition = ((enqueueOptionsNew queueName) {partitionKey = Just ""} :: EnqueueOptions)
        parentBody :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) [Text])
        parentBody () wctx = do
          let tryEnqueue options = do
                outcome <- (enqueueWorkflowByNameInWorkflow wctx (newQualifiedWorkflowName "remote-refused") options Nothing :: m (Either (Error EngineOnly) (WorkflowHandle m EngineOnly)))
                case outcome of
                  Left (InvalidArgument _ detail) -> pure detail
                  Left err -> throwIO (userError ("expected an InvalidArgument refusal, got: " <> show err))
                  Right _ -> throwIO (userError "the bad options were not refused")
          refusals <- traverse tryEnqueue [badApp, emptyVersion, emptyId, emptyDedup, emptyPartition]
          moved <- runStep wctx "after" (\_ -> pure ())
          case moved of
            Left err -> pure (Left err)
            Right _ -> pure (Right refusals)
    parentReg <- registerWorkflow dbos (newQualifiedWorkflowName "refusing-parent") parentBody
    case parentReg of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.wfLaunch dbos
    ran <- (runWorkflow exec (newQualifiedWorkflowName "refusing-parent") parentWid Nothing :: m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
    refusals <- case ran of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError [Text] of
        Right details -> pure details
        Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the refusals, got: " <> show other))
    steps <- fx.wfListSteps parentWid
    pure (refusals, [(fromIntegral record.stepRecordStepId, record.stepRecordStepName) | record <- steps])

-- | Each refusal names its field, and the only recorded step is the one
-- after them: no refusal spent an id.
checkEnqueueByNameRefusedNoStep :: ([Text], [(Int, Text)]) -> Either String ()
checkEnqueueByNameRefusedNoStep (refusals, steps)
  | length refusals /= 5 = Left ("expected five refusals, got: " <> show refusals)
  | not ("`app_name`" `Text.isInfixOf` (refusals !! 0)) = Left ("expected the app_name refusal, got: " <> show refusals)
  | not ("`app_version`" `Text.isInfixOf` (refusals !! 1)) = Left ("expected the app_version refusal, got: " <> show refusals)
  | not ("`workflow_id`" `Text.isInfixOf` (refusals !! 2)) = Left ("expected the workflow_id refusal, got: " <> show refusals)
  | not ("`deduplication_id`" `Text.isInfixOf` (refusals !! 3)) = Left ("expected the deduplication_id refusal, got: " <> show refusals)
  | not ("`partition_key`" `Text.isInfixOf` (refusals !! 4)) = Left ("expected the partition_key refusal, got: " <> show refusals)
  | steps /= [(0, "after")] = Left ("expected the one step after them, got: " <> show steps)
  | otherwise = Right ()
