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
    scenarioForkFromBeginning,
    scenarioForkTakesIdAndQueue,
    scenarioForkFromStep,
    scenarioForkFromFailure,
    scenarioBulkCancelResume,
    scenarioBulkFork,
    scenarioForkPartitioned,
    scenarioAttributes,
    scenarioDelayRelease,
    checkUnlaunched,
    checkCancelMissing,
    checkResumeMissing,
    checkCancelResumeRun,
    checkResumeOntoQueue,
    checkCancelTree,
    checkDelete,
    checkRetrieve,
    checkForkFromBeginning,
    checkForkTakesIdAndQueue,
    checkForkFromStep,
    checkForkFromFailure,
    checkBulkCancelResume,
    checkBulkFork,
    checkForkPartitioned,
    checkAttributes,
    checkDelayRelease,
  )
where

import DBOS.Prelude
import Data.Text qualified as Text
import DBOS.SystemDB (Fork (..), ForkOptions (..), ForkPoint (..), QueueName (..), SerializedWorkflowValue (..), WorkflowDelay (..), WorkflowFilter (..), WorkflowId (..), WorkflowRecord (..), WorkflowStatus (..), defaultForkOptions, defaultWorkflowFilter, forkNew, internalQueueName)
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
    SomeTracer (..),
    StartOptions (..),
    WorkflowCtx,
    WorkflowHandle (..),
    WorkflowKey,
    WorkflowRef,
    cancelWorkflows,
    decodeWorkflowValue,
    deleteWorkflows,
    encodeWorkflowValue,
    enqueueDBOSWorkflow,
    enqueueNew,
    forkFrom,
    forkWorkflows,
    handleResult,
    handleStatus,
    newDBOS,
    newWorkflowKey,
    registerDBOSWorkflow,
    registerDBOSWorkflowRef,
    resumeWorkflows,
    retrieveWorkflow,
    runDBOSWorkflow,
    runStep,
    setWorkflowDelay,
    updateWorkflowAttributes,
    listWorkflows,
    secondsDuration,
    shutdown,
    startChildWorkflow,
    startDBOSWorkflowRef,
    startOptionsDefault,
    waitForWorkflow,
  )
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Instance (dequeueDBOSWorkflows, launchOnWithQueues)
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
mkMgmtFixture ::
  forall m.
  (MonadMVar m, MonadSTM m, MonadThrow m) =>
  Config ->
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
runWf :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) => Executor m -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWf = runDBOSWorkflow

startWfRef :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) => Executor m -> WorkflowRef m EngineOnly -> StartOptions -> Maybe SerializedWorkflowValue -> m (Either (Error EngineOnly) (WorkflowHandle m EngineOnly))
startWfRef = startDBOSWorkflowRef

retrieveWf :: forall m. (MonadMVar m) => DBOS m -> WorkflowId -> m (Either (Error EngineOnly) (WorkflowHandle m EngineOnly))
retrieveWf = retrieveWorkflow

resultWf :: forall m. (MonadDelay m, MonadTime m, MonadMVar m, MonadThrow m) => WorkflowHandle m EngineOnly -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWf = handleResult

statusWf :: forall m. (MonadMVar m) => WorkflowHandle m EngineOnly -> m (Either (Error EngineOnly) (Maybe WorkflowStatus))
statusWf = handleStatus

-- | The cancellable body: counts its entries, then answers input plus five.
cancellableBody ::
  forall exec m.
  (MonadSTM m) =>
  StrictTVar m Int ->
  Int ->
  WorkflowCtx exec m ->
  m (Either (Error EngineOnly) Int)
cancellableBody ran input _ = do
  atomically (modifyTVar ran (+ 1))
  pure (Right (input + 5))

-- | The tree child: signals its start, then parks on the gate until the
-- cancel kills it.
treeChildBody ::
  forall exec m.
  (MonadMVar m) =>
  StrictMVar m () ->
  StrictMVar m () ->
  () ->
  WorkflowCtx exec m ->
  m (Either (Error EngineOnly) Int)
treeChildBody started gate () _ = putMVar started () >> takeMVar gate >> pure (Right 1)

-- | The tree parent: starts the child, waits until it has begun (so the
-- cancel below cannot miss it), and hands back the child's id.
treeParentBody ::
  forall exec m.
  (MonadMVar m, MonadTimer m, MonadTime m, MonadCatch m) =>
  WorkflowRef m EngineOnly ->
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
scenarioUnlaunched ::
  forall m.
  (MonadFork m, MonadMVar m, MonadSTM m, MonadThrow m) =>
  MgmtFixture m ->
  m Bool
scenarioUnlaunched fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    refused <- cancelWorkflows dbos [WorkflowId "never-launched"] False
    case refused of
      Left ErrorNotLaunched {} -> pure True
      other -> throwIO (userError ("expected a not-launched refusal, got: " <> show other))

-- | Cancelling a workflow that does not exist is not an error.
scenarioCancelMissing ::
  forall m.
  (MonadFork m, MonadMVar m, MonadSTM m, MonadThrow m) =>
  MgmtFixture m ->
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
scenarioResumeMissing ::
  forall m.
  (MonadFork m, MonadMVar m, MonadSTM m, MonadThrow m) =>
  MgmtFixture m ->
  m [Text]
scenarioResumeMissing fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    _ <- fx.mfLaunch dbos
    resumed <- resumeWorkflows dbos [WorkflowId "never-existed"] Nothing
    case resumed of
      Left (ErrorSystemDatabase (SystemDB.NonExistentWorkflow {workflowIds})) -> pure workflowIds
      other -> throwIO (userError ("expected a non-existent-workflow refusal, got: " <> show other))

-- | Cancelling makes a workflow terminal and leaves it resumable: cancel a
-- queued run before it starts, resume it, and watch it run exactly once.
-- Returns the cancelled ids, the terminal status, the pre-resume run
-- count, the resumed ids, the decoded result, and the final run count.
scenarioCancelResumeRun ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  MgmtFixture m ->
  m (Text, [Text], Maybe WorkflowStatus, Int, [Text], Int, Int)
scenarioCancelResumeRun fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    ran <- newTVarIO (0 :: Int)
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "cancellable"
        workflowText = "hs-l2-mgmt-cancel-resume-" <> suffix
        wid = WorkflowId workflowText
    refE <- registerDBOSWorkflowRef dbos key (cancellableBody ran)
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
    drove <- dequeueDBOSWorkflows dbos
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
scenarioResumeOntoQueue ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  MgmtFixture m ->
  m Int
scenarioResumeOntoQueue fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "resumable"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body input _ = pure (Right input)
        wid = WorkflowId ("hs-l2-mgmt-resume-queue-" <> suffix)
    refE <- registerDBOSWorkflowRef dbos key body
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
    drove <- dequeueDBOSWorkflows dbos
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
scenarioCancelTree ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  MgmtFixture m ->
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
    childRefE <- registerDBOSWorkflowRef dbos childKey (treeChildBody childStarted gate)
    childRef <- case childRefE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    parentReg <- registerDBOSWorkflow dbos parentKey (treeParentBody childRef childStarted)
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
scenarioDelete ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  MgmtFixture m ->
  m (Word64, Maybe WorkflowStatus)
scenarioDelete fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "deletable"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body input wctx = runStep wctx "work" (const (pure input))
        wid = WorkflowId ("hs-l2-mgmt-delete-" <> suffix)
    registered <- registerDBOSWorkflow dbos key body
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
scenarioRetrieve ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  MgmtFixture m ->
  m (Maybe WorkflowStatus, Int)
scenarioRetrieve fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "retrievable"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body input _ = pure (Right (input * 3))
        wid = WorkflowId ("hs-l2-mgmt-retrieve-" <> suffix)
    registered <- registerDBOSWorkflow dbos key body
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

-- | The forkable body: its first attempt fails, later attempts answer 8. The
-- counter is shared so both stacks can observe exactly-once retry semantics.
forkableBody ::
  forall exec m.
  (MonadSTM m) =>
  StrictTVar m Int ->
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
stagedBody ::
  forall exec m.
  (MonadSTM m, MonadTime m, MonadCatch m) =>
  StrictTVar m [Text] ->
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
plusOneBody ::
  forall exec m.
  (Applicative m) =>
  Int ->
  WorkflowCtx exec m ->
  m (Either (Error EngineOnly) Int)
plusOneBody input _ = pure (Right (input + 1))

-- | The echo body: answers its message.
echoTextBody ::
  forall exec m.
  (Applicative m) =>
  Text ->
  WorkflowCtx exec m ->
  m (Either (Error EngineOnly) Text)
echoTextBody message _ = pure (Right message)

-- | The doubling body: answers twice its input.
doublingBody ::
  forall exec m.
  (Applicative m) =>
  Int ->
  WorkflowCtx exec m ->
  m (Either (Error EngineOnly) Int)
doublingBody input _ = pure (Right (input * 2))

-- | The zero body: answers zero.
zeroBody ::
  forall exec m.
  (Applicative m) =>
  () ->
  WorkflowCtx exec m ->
  m (Either (Error EngineOnly) Int)
zeroBody () _ = pure (Right 0)

-- | A fork from the beginning runs the source's workflow again under a new id
-- and succeeds on the retry. Returns the fork's id, the fork's decoded result,
-- and the source's attempt count.
scenarioForkFromBeginning ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  MgmtFixture m ->
  m (Text, Int, Int)
scenarioForkFromBeginning fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    attempts <- newTVarIO (0 :: Int)
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "forkable"
        sourceText = "hs-l2-mgmt-fork-src-" <> suffix
        sourceId = WorkflowId sourceText
    refE <- registerDBOSWorkflowRef dbos key (forkableBody attempts)
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
    drove <- dequeueDBOSWorkflows dbos
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
scenarioForkTakesIdAndQueue ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  MgmtFixture m ->
  m (Text, Maybe Text, Int)
scenarioForkTakesIdAndQueue fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "placed"
        sourceText = "hs-l2-mgmt-fork-placed-src-" <> suffix
        forkedText = "hs-l2-mgmt-fork-placed-" <> suffix
        sourceId = WorkflowId sourceText
    refE <- registerDBOSWorkflowRef dbos key plusOneBody
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
    drove <- dequeueDBOSWorkflows dbos
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
scenarioForkFromStep ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  MgmtFixture m ->
  m [Text]
scenarioForkFromStep fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    ran <- newTVarIO ([] :: [Text])
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "staged"
        sourceText = "hs-l2-mgmt-fork-step-src-" <> suffix
        sourceId = WorkflowId sourceText
    refE <- registerDBOSWorkflowRef dbos key (stagedBody ran)
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
    drove <- dequeueDBOSWorkflows dbos
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
scenarioForkFromFailure ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  MgmtFixture m ->
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
    refE <- registerDBOSWorkflowRef dbos key flakyBody
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
    drove <- dequeueDBOSWorkflows dbos
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
scenarioBulkCancelResume ::
  forall m.
  (MonadFork m, MonadMVar m, MonadSTM m, MonadThrow m) =>
  MgmtFixture m ->
  m (Text, Text, [Text], [Text])
scenarioBulkCancelResume fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "queued"
        firstText = "hs-l2-mgmt-bulk-1-" <> suffix
        secondText = "hs-l2-mgmt-bulk-2-" <> suffix
        first = WorkflowId firstText
        second = WorkflowId secondText
    regE <- registerDBOSWorkflow dbos key echoTextBody
    case regE of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    _ <- fx.mfLaunch dbos
    let enqueueOne wid = do
          enqueued <- enqueueDBOSWorkflow dbos key wid (Just (encodeWorkflowValue ("hello" :: Text))) ("bulk-" <> suffix)
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
scenarioBulkFork ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  MgmtFixture m ->
  m (Text, Text, [Text], [Int])
scenarioBulkFork fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "doubling"
        firstText = "hs-l2-mgmt-bulk-fork-1-" <> suffix
        secondText = "hs-l2-mgmt-bulk-fork-2-" <> suffix
        first = WorkflowId firstText
        second = WorkflowId secondText
    regE <- registerDBOSWorkflow dbos key doublingBody
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
    drove <- dequeueDBOSWorkflows dbos
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
scenarioForkPartitioned ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  MgmtFixture m ->
  m (Maybe Text)
scenarioForkPartitioned fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "queued"
        sourceId = WorkflowId ("hs-l2-mgmt-fork-key-src-" <> suffix)
    regE <- registerDBOSWorkflow dbos key echoTextBody
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

-- | The fork's row carries the partition key it was given.
checkForkPartitioned :: Maybe Text -> Either String ()
checkForkPartitioned = checkEq (Just "pk-7")

-- | Attributes are replaced and can be searched: a full replacement matches a
-- one-key filter by containment, a narrower replacement drops the unseen key,
-- and clearing removes them. Returns the matching ids at each search and the
-- cleared row's attributes.
scenarioAttributes ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  MgmtFixture m ->
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
    regE <- registerDBOSWorkflow dbos key zeroBody
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

-- | The workflow waited as delayed, then ran its body on release.
checkDelayRelease :: (Maybe WorkflowStatus, Int) -> Either String ()
checkDelayRelease = checkEq (Just Delayed, 0)

-- | A delayed workflow can be released sooner: started an hour out it waits
-- as delayed; brought forward to now, the driven pass transitions and runs
-- it. Returns the waiting status and the decoded result.
scenarioDelayRelease ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  MgmtFixture m ->
  m (Maybe WorkflowStatus, Int)
scenarioDelayRelease fx = do
  bracket fx.mfNewDBOS shutdown $ \dbos -> do
    suffix <- fx.mfFreshBase
    let key = newWorkflowKey "delayable"
        wid = WorkflowId ("hs-l2-mgmt-delayed-" <> suffix)
        QueueName internal = internalQueueName
    refE <- registerDBOSWorkflowRef dbos key zeroBody
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
    drove <- dequeueDBOSWorkflows dbos
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
