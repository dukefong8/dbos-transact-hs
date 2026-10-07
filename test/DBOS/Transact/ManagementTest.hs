{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Engine-level management behavior, mirroring the Rust
-- @tests/management.rs@ cases the ported surface can express: cancel
-- (bulk, tree, missing), resume (missing, named queue, internal queue),
-- delete, fork (beginning, chosen step, chosen id and queue), and retrieve.
-- The backend halves of these calls live in 'DBOS.SystemDB.PostgresTest';
-- this group is the launched-instance behavior.
module DBOS.Transact.ManagementTest (tests) where

import DBOS.DualStack (liveCase)
import DBOS.Prelude
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB
  ( AwaitedOutcome (..),
    Fork (..),
    NewWorkflow (..),
    SerializedWorkflowValue (..),
    StepRecord (..),
    Submission (..),
    WorkflowFilter (..),
    WorkflowId (..),
    WorkflowRecord (..),
    WorkflowStatus (..),
    defaultForkOptions,
    defaultWorkflowFilter,
    forkNew,
    getWorkflow,
    newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  (
    EngineOnly,
    CodecError,
    Config (..),
    DBOS,
    Executor,
    WorkflowCtx,
    Environment (..),
    Error (..),
    QueueConflict (..),
    SomeTracer (..),
    StartOptions (..),
    WorkflowHandle (workflowId),
    WorkflowKey,
    WorkflowRef,
    acquireLoggerBackend,
    cancelWorkflowsInWorkflow,
    clientCancelWorkflows,
    clientConfigFromEnv,
    closeClient,
    configFromEnv,
    connectClient,
    decodeWorkflowValue,
    defaultQueueOptions,
    deleteWorkflowsInWorkflow,
    encodeWorkflowValue,
    enqueueDBOSWorkflow,
    forkWorkflowsInWorkflow,
    handleResult,
    handleStatus,
    ioTracer,
    launchWithEnvironment,
    listWorkflowsInWorkflow,
    newDBOS,
    newWorkflowKey,
    registerDBOSWorkflow,
    registerDBOSWorkflowRef,
    registerQueue,
    resumeWorkflows,
    resumeWorkflowsInWorkflow,
    retrieveWorkflow,
    nullTracer,
    runDBOSWorkflow,
    runStep,
    shutdown,
    startDBOSWorkflowRef,
    startOptionsDefault,
    waitForWorkflow,
    cancelWorkflowsInWorkflow,
    deleteWorkflowsInWorkflow,
    forkWorkflowsInWorkflow,
    resumeWorkflowsInWorkflow,
    startChildWorkflow)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Context (withWorkflow)
import DBOS.Transact.Connection (SomeSystemDB (..), uuidWorkflowId)
import DBOS.SystemDB.Retry (uuidEntropy)
import DBOS.Transact.ContextTest (connOver)
import DBOS.Transact.ManagementCases
  ( MgmtFixture (..),
    checkCancelMissing,
    checkCancelMissing,
    checkCancelResumeRun,
    checkCancelTree,
    checkDelete,
    checkForkFromBeginning,
    checkForkFromFailure,
    checkForkFromStep,
    checkForkTakesIdAndQueue,
    checkBulkCancelResume,
    checkBulkFork,
    checkForkPartitioned,
    checkAttributes,
    checkDelayRelease,
    checkDelete,
    checkResumeMissing,
    checkResumeOntoQueue,
    checkRetrieve,
    checkUnlaunched,
    mkMgmtFixture,
    mkMgmtFixture,
    scenarioCancelMissing,
    scenarioCancelResumeRun,
    scenarioCancelTree,
    scenarioDelete,
    scenarioForkFromBeginning,
    scenarioForkFromFailure,
    scenarioForkFromStep,
    scenarioForkTakesIdAndQueue,
    scenarioBulkCancelResume,
    scenarioBulkFork,
    scenarioForkPartitioned,
    scenarioAttributes,
    scenarioDelayRelease,
    scenarioDelete,
    scenarioResumeMissing,
    scenarioResumeOntoQueue,
    scenarioRetrieve,
    scenarioUnlaunched,
  )
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

-- | The application identity the scoped management cases install.
mgmtTestIdentity :: Identity
mgmtTestIdentity =
  Identity
    { identityAppName = "test-app",
      identityAppVersion = "1.0.0",
      identityExecutorId = "test-executor",
      identityAppId = ""
    }

tests :: TestTree
tests =
  withResource acquireSuiteBackend releaseSuiteBackend $ \getBackend ->
  withResource acquireLoggerBackend snd $ \getLogger ->
  testGroup
    "Workflow management"
    [ liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "the management surface needs a launched instance" scenarioUnlaunched checkUnlaunched,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "cancelling a workflow that does not exist is not an error" scenarioCancelMissing checkCancelMissing,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "resuming a workflow that does not exist is an error" scenarioResumeMissing checkResumeMissing,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "cancelling makes a workflow terminal and leaves it resumable" scenarioCancelResumeRun checkCancelResumeRun,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "resuming onto a named queue puts the workflow there" scenarioResumeOntoQueue checkResumeOntoQueue,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "cancelling a tree reaches the children" scenarioCancelTree checkCancelTree,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "deleting a workflow removes its row" scenarioDelete checkDelete,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a workflow can be retrieved by id" scenarioRetrieve checkRetrieve,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "forking from the beginning runs the workflow again under a new id" scenarioForkFromBeginning checkForkFromBeginning,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a fork takes the id it is given" scenarioForkTakesIdAndQueue checkForkTakesIdAndQueue,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "forking from a chosen step replays the steps below it" scenarioForkFromStep checkForkFromStep,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "bulk cancel and resume hand back every id" scenarioBulkCancelResume checkBulkCancelResume,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "bulk forking hands back one new id per source, in order" scenarioBulkFork checkBulkFork,
      testCase "resuming onto a named queue through a full launch puts the workflow there" $
        withInstance "mgmt-resume-queue" $ \dbos suffix -> do
          let key = newWorkflowKey "queued"
              wid = WorkflowId ("hs-l2-mgmt-resume-q-" <> suffix)
              echoWorkflow :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
              echoWorkflow message _ = pure (Right message)
          registered <- registerDBOSWorkflow dbos key echoWorkflow
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          enqueued <-
            enqueueDBOSWorkflow
              dbos
              key
              wid
              (Just (encodeWorkflowValue ("hello" :: Text)))
              ("from-" <> suffix)
          case enqueued of
            Left err -> fail (show err)
            Right _ -> pure ()
          resumed <- resumeWorkflows dbos [wid] (Just ("to-" <> suffix))
          resumed @?= Right [wid]
          WorkflowRecord {workflowRecordQueueName = queue} <- readRow getBackend wid
          queue @?= Just ("to-" <> suffix),
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a fork onto a partitioned queue carries the key it is given" scenarioForkPartitioned checkForkPartitioned,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "forking from the last failure restarts at the failed step" scenarioForkFromFailure checkForkFromFailure,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "attributes are replaced and can be searched" scenarioAttributes checkAttributes,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a delayed workflow can be released sooner" scenarioDelayRelease checkDelayRelease,
      testCase "a workflow cannot delete itself" $
        withInstance "mgmt-self-delete" $ \dbos suffix -> do
          let key = newWorkflowKey "self-deleter"
              wid = WorkflowId ("hs-l2-mgmt-self-" <> suffix)
              body :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
              body ownId wctx = do
                deleted <- deleteWorkflowsInWorkflow wctx [WorkflowId ownId] False
                pure (void deleted)
          registered <- registerDBOSWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          ran <-
            runWf
              exec
              key
              wid
              (Just (encodeWorkflowValue ("hs-l2-mgmt-self-" <> suffix :: Text)))
          case ran of
            Left err -> assertBool ("the refusal did not reach the caller: " <> show err) ("cannot delete itself" `Text.isInfixOf` Text.pack (show err))
            Right _ -> fail "the workflow deleted itself"
          -- The row went nowhere even though the delete was refused.
          _ <- readRow getBackend wid
          pure (),
      testCase "a workflow cannot delete an ancestors tree" $
        withInstance "mgmt-ancestor-delete" $ \dbos suffix -> do
          -- The child reports through a rendezvous because a refused
          -- delete records nothing: a polling handle would wait on a row
          -- that stays pending forever, where the oracle awaits the
          -- child's local task.
          observed <- newEmptyMVar
          let childKey = newWorkflowKey "deleting-child"
              parentKey = newWorkflowKey "deleted-parent"
              rootText = "hs-l2-mgmt-ancestor-" <> suffix
              childBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
              childBody root wctx = do
                outcome <- deleteWorkflowsInWorkflow wctx [WorkflowId root] True
                void (tryPutMVar observed outcome)
                pure (void outcome)
          childRef <- registerRefOrFail dbos childKey childBody
          let parentBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
              parentBody root wctx = do
                started <- startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowValue root))
                case started of
                  Left err -> pure (Left err)
                  Right _ -> void <$> takeMVar observed
          registered <- registerDBOSWorkflow dbos parentKey parentBody
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          ran <-
            runWf
              exec
              parentKey
              (WorkflowId rootText)
              (Just (encodeWorkflowValue (rootText :: Text)))
          case ran of
            Left err -> assertBool ("the refusal did not reach the caller: " <> show err) ("cannot delete itself" `Text.isInfixOf` Text.pack (show err))
            Right _ -> fail "the child deleted the tree it was in"
          -- Neither the ancestor nor the parent row went anywhere.
          _ <- readRow getBackend (WorkflowId rootText)
          pure (),
      testCase "management calls from inside a workflow are recorded as steps" $
        withInstance "mgmt-in-workflow" $ \dbos suffix -> do
          let targetKey = newWorkflowKey "target"
              operatorKey = newWorkflowKey "operator"
              targetText = "hs-l2-mgmt-op-target-" <> suffix
              operatorText = "hs-l2-mgmt-operator-" <> suffix
              -- A queue nothing polls, so the target sits there to be
              -- cancelled: named on the row, with no table row for any
              -- sweep to see.
              quietQueue = "hs-l2-mgmt-quiet-" <> suffix
              targetBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              targetBody () _ = pure (Right 1)
              operatorBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              operatorBody target wctx = do
                cancelled <- cancelWorkflowsInWorkflow wctx [WorkflowId target] False
                case cancelled of
                  Left err -> pure (Left err)
                  Right _ -> do
                    listed <-
                      listWorkflowsInWorkflow
                        wctx
                        (defaultWorkflowFilter {workflowFilterWorkflowIds = [target]})
                    pure (Right (length listed))
          registeredTarget <- registerDBOSWorkflow dbos targetKey targetBody
          case registeredTarget of
            Left err -> fail (show err)
            Right () -> pure ()
          registeredOperator <- registerDBOSWorkflow dbos operatorKey operatorBody
          case registeredOperator of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          enqueued <-
            enqueueDBOSWorkflow
              dbos
              targetKey
              (WorkflowId targetText)
              Nothing
              quietQueue
          case enqueued of
            Left err -> fail (show err)
            Right _ -> pure ()
          ran <-
            runWf
              exec
              operatorKey
              (WorkflowId operatorText)
              (Just (encodeWorkflowValue (targetText :: Text)))
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the listing saw the filtered workflow" (Right 1) decoded
            other -> fail (show other)
          target <- readRow getBackend (WorkflowId targetText)
          target.workflowRecordStatus @?= Cancelled
          backend <- getBackend
          listed <- SystemDB.listSteps backend (WorkflowId operatorText) False Nothing Nothing Nothing
          case listed of
            Right steps ->
              map (\StepRecord {stepRecordStepId = sid, stepRecordStepName = name} -> (sid, name)) steps
                @?= [(0, "DBOS.cancelWorkflow"), (1, "DBOS.listWorkflows")]
            other -> fail (show other),
      testCase "a replayed fork hands back the id it recorded" $
        withInstance "mgmt-fork-replay" $ \dbos suffix -> do
          -- A fork generates a new id, so a replay without the checkpoint
          -- would write a second fork: crash after the fork is recorded
          -- and let recovery prove the replay adopts it.
          let sourceKey = newWorkflowKey "forked-source"
              operatorKey = newWorkflowKey "forking-operator"
              sourceText = "hs-l2-mgmt-replay-src-" <> suffix
              operatorText = "hs-l2-mgmt-replay-op-" <> suffix
              sourceBody :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              sourceBody value wctx = runStep wctx "double" (const (pure (value * 2)))
          shouldCrash <- newIORef True
          seen <- newEmptyMVar
          let operatorBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
              operatorBody source wctx = do
                forked <- forkWorkflowsInWorkflow wctx [forkNew source] defaultForkOptions
                case forked of
                  Left err -> pure (Left err)
                  Right [WorkflowId fid] -> do
                    void (tryPutMVar seen (WorkflowId fid))
                    crash <- readIORef shouldCrash
                    if crash
                      then liftIO (ioError (userError "forked then crashed"))
                      else pure (Right fid)
                  Right other -> fail ("expected exactly one fork, got: " <> show other)
          sourceRegistered <- registerDBOSWorkflow dbos sourceKey sourceBody
          case sourceRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          operatorRegistered <- registerDBOSWorkflow dbos operatorKey operatorBody
          case operatorRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          ranSource <- runWf exec sourceKey (WorkflowId sourceText) (Just (encodeWorkflowValue (21 :: Int)))
          case ranSource of
            Right _ -> pure ()
            other -> fail ("expected the source to run, got: " <> show other)
          first <- try (runWf exec operatorKey (WorkflowId operatorText) (Just (encodeWorkflowValue (sourceText :: Text)))) :: IO (Either SomeException (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
          case first of
            Left exception -> assertBool "the crash escapes" ("forked then crashed" `Text.isInfixOf` Text.pack (show exception))
            Right other -> fail ("expected the crash, got: " <> show other)
          shutdown dbos
          writeIORef shouldCrash False
          _ <- launchWithEnvironment dbos isolatedEnvironment
          settled <- timeout 10000000 (waitForWorkflow dbos (WorkflowId operatorText))
          case settled of
            Just (Right (AwaitedSucceeded (Just output) _)) -> do
              let decoded = decodeWorkflowValue "result" (Just (SerializedWorkflowValue output Nothing)) :: Either CodecError Text
              recorded <- takeMVar seen
              case decoded of
                Right fid -> WorkflowId fid @?= recorded
                Left err -> fail (show err)
            other -> fail ("expected the recovery to finish, got: " <> show other)
          backend <- getBackend
          forks <- SystemDB.listWorkflows backend (defaultWorkflowFilter {workflowFilterForkedFrom = [sourceText]}) Nothing
          case forks of
            Right rows -> length rows @?= 1
            other -> fail ("expected exactly one fork row, got: " <> show other),
      testCase "resume puts a cancelled workflow on a queue that runs it" $
        withInstance "mgmt-resume-run" $ \dbos suffix -> do
          let targetKey = newWorkflowKey "resumable"
              operatorKey = newWorkflowKey "resumer"
              targetText = "hs-l2-mgmt-resume-target-" <> suffix
              operatorText = "hs-l2-mgmt-resumer-" <> suffix
              quietQueue = "hs-l2-mgmt-quiet-" <> suffix
              runQueue = "hs-l2-mgmt-run-" <> Text.take 12 suffix
              targetBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              targetBody () _ = pure (Right 1)
              operatorBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
              operatorBody target wctx = do
                cancelled <- cancelWorkflowsInWorkflow wctx [WorkflowId target] False
                case cancelled of
                  Left err -> pure (Left err)
                  Right _ -> do
                    resumed <- resumeWorkflowsInWorkflow wctx [WorkflowId target] (Just runQueue)
                    case resumed of
                      Left err -> pure (Left err)
                      Right _ -> pure (Right ())
          registeredTarget <- registerDBOSWorkflow dbos targetKey targetBody
          case registeredTarget of
            Left err -> fail (show err)
            Right () -> pure ()
          registeredOperator <- registerDBOSWorkflow dbos operatorKey operatorBody
          case registeredOperator of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          queueRegistered <- registerQueue dbos runQueue defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          enqueued <-
            enqueueDBOSWorkflow
              dbos
              targetKey
              (WorkflowId targetText)
              Nothing
              quietQueue
          case enqueued of
            Left err -> fail (show err)
            Right _ -> pure ()
          ran <-
            runWf
              exec
              operatorKey
              (WorkflowId operatorText)
              (Just (encodeWorkflowValue (targetText :: Text)))
          case ran of
            Right _ -> pure ()
            other -> fail ("expected the operator to run, got: " <> show other)
          settled <- timeout 15000000 (waitForWorkflow dbos (WorkflowId targetText))
          case settled of
            Just (Right (AwaitedSucceeded (Just output) _)) -> do
              let decoded = decodeWorkflowValue "result" (Just (SerializedWorkflowValue output Nothing)) :: Either CodecError Int
              assertEqual "the resumed workflow runs" (Right 1) decoded
            other -> fail ("expected the resumed run, got: " <> show other)
          backend <- getBackend
          listed <- SystemDB.listSteps backend (WorkflowId operatorText) False Nothing Nothing Nothing
          case listed of
            Right steps ->
              map (\StepRecord {stepRecordStepId = sid, stepRecordStepName = name} -> (sid, name)) steps
                @?= [(0, "DBOS.cancelWorkflow"), (1, "DBOS.resumeWorkflow")]
            other -> fail (show other),
      testCase "a clients management call inside a workflow is not a step" $
        withInstance "mgmt-client-call" $ \dbos suffix -> do
          -- A client has no step counter of its own to agree with the
          -- workflow's, so its calls run again on replay instead of
          -- checkpointing: by construction they cannot spend step ids.
          let operatorKey = newWorkflowKey "caller"
              operatorText = "hs-l2-mgmt-client-op-" <> suffix
          clientConfig <- clientConfigFromEnv
          bracket (connectClient clientConfig) (either (const (pure ())) closeClient) $ \connected ->
            case connected of
              Left err -> fail (show err)
              Right client -> do
                let body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
                    body () wctx = do
                      cancelled <- clientCancelWorkflows client [WorkflowId "never-existed"] False
                      case cancelled of
                        Left err -> pure (Left err)
                        Right _ -> do
                          probe <- runStep wctx "probe" (const (pure ()))
                          case probe of
                            Left err -> pure (Left err)
                            Right () -> pure (Right ())
                registered <- registerDBOSWorkflow dbos operatorKey body
                case registered of
                  Left err -> fail (show err)
                  Right () -> pure ()
                exec <- launchOrFail dbos
                ran <- runWf exec operatorKey (WorkflowId operatorText) Nothing
                case ran of
                  Right _ -> pure ()
                  other -> fail ("expected the operator to run, got: " <> show other)
                backend <- getBackend
                listed <- SystemDB.listSteps backend (WorkflowId operatorText) False Nothing Nothing Nothing
                case listed of
                  Right [StepRecord {stepRecordStepId = sid, stepRecordStepName = name}] ->
                    (sid, name) @?= (0, "probe")
                  other -> fail ("expected only the probe at step zero, got: " <> show other),
      testCase "a refused management call spends no step id" $ do
        backend <- getBackend
        fresh <- freshSuffix
        let workflowText = "hs-l2-mgmt-refusal-" <> fresh
            initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2MgmtRefusal"}
        created <- SystemDB.initWorkflow backend initialWorkflow Nothing Fresh Nothing
        case created of
          Left err -> fail (show err)
          Right _ -> pure ()
        timed <- getLogger
        conn <- connOver backend (ioTracer (fst timed))
        -- Both calls share one scope, so the probe taking step zero proves
        -- the refused fork spent nothing.
        (refused, probe) <-
          withWorkflow conn mgmtTestIdentity (WorkflowId workflowText) Nothing $ \wctx -> do
            -- An empty forked id must be absent rather than empty: refused
            -- before any id is taken.
            refused <- forkWorkflowsInWorkflow wctx [(forkNew "source") {forkForkedId = Just ""}] defaultForkOptions
            -- The probe that follows still takes step zero.
            probe <- (runStep wctx "probe" (const (pure ())) :: IO (Either (Error EngineOnly) ()))
            pure (refused, probe)
        case refused of
          Left (ErrorSystemDatabase (SystemDB.InvalidInput {})) -> pure ()
          other -> fail ("expected an argument refusal, got: " <> show other)
        case probe of
          Left err -> fail (show err)
          Right () -> pure ()
        listed <- SystemDB.listSteps backend (WorkflowId workflowText) False Nothing Nothing Nothing
        case listed of
          Right [StepRecord {stepRecordStepId = sid, stepRecordStepName = name}] ->
            (sid, name) @?= (0, "probe")
          other -> fail ("expected only the probe at step zero, got: " <> show other),
      testCase "an empty management batch records its step" $ do
        backend <- getBackend
        fresh <- freshSuffix
        let workflowText = "hs-l2-mgmt-empty-" <> fresh
            initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2MgmtEmpty"}
        created <- SystemDB.initWorkflow backend initialWorkflow Nothing Fresh Nothing
        case created of
          Left err -> fail (show err)
          Right _ -> pure ()
        timed <- getLogger
        conn <- connOver backend (ioTracer (fst timed))
        emptied <- withWorkflow conn mgmtTestIdentity (WorkflowId workflowText) Nothing $ \wctx ->
          forkWorkflowsInWorkflow wctx [] defaultForkOptions
        emptied @?= Right []
        listed <- SystemDB.listSteps backend (WorkflowId workflowText) False Nothing Nothing Nothing
        case listed of
          Right [StepRecord {stepRecordStepId = sid, stepRecordStepName = name}] ->
            (sid, name) @?= (0, "DBOS.forkWorkflow")
          other -> fail ("expected the empty batch at step zero, got: " <> show other)
    ]

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through: each
-- pins the error channel to 'EngineOnly' (the channel the test bodies
-- declare), so a call site outside an annotated body does not leave it
-- for the compiler to guess. Local copies are deliberate — this module
-- carries only the aliases it uses, and a sibling test module repeats
-- the ones it needs.
runWf :: Executor IO -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWf = runDBOSWorkflow

startWfRef :: Executor IO -> WorkflowRef IO EngineOnly -> StartOptions -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
startWfRef = startDBOSWorkflowRef

retrieveWf :: DBOS IO -> WorkflowId -> IO (Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
retrieveWf = retrieveWorkflow

resultWf :: WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWf = handleResult

statusWf :: WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe WorkflowStatus))
statusWf = handleStatus

-- * Helpers

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted. The
-- launched instances below keep their own pools: each needs a distinct
-- application identity.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | The queue-name prefixes this suite's legacy cases register. No other live
-- suite registers under them, so the release can delete by prefix without
-- racing a running peer (tasty runs groups in parallel; names are unique per
-- case). The framed 'ManagementCases' scenarios register no queue rows.
suiteQueuePrefixes :: [Text]
suiteQueuePrefixes = ["hs-l2-mgmt-fork-queue-", "hs-l2-mgmt-run-", "hs-l2-delayed-work-"]

-- | Delete this suite's fixture queues after the group finishes: the shared
-- database keeps a queue row per run, and every unscoped sweep pays one claim
-- query per row. Runs in the 'withResource' release, after every case, so it
-- never races a running case. Best-effort: a refusal is ignored rather than
-- failing the suite. Workflow rows are left alone; without their queue row
-- no sweep will ever enumerate them.
releaseSuiteBackend :: Postgres.PostgresSystemDB -> IO ()
releaseSuiteBackend backend = do
  listed <- SystemDB.listQueues backend SystemDB.Unset
  case listed of
    Left _ -> pure ()
    Right records -> mapM_ (\name -> SystemDB.deleteQueue backend name >> pure ()) names
      where
        names = [name | record <- records, let name = record.queueRecordName, any (`Text.isPrefixOf` name) suiteQueuePrefixes]
  Postgres.releasePostgresSystemDB backend

-- | The shared tree over a real backend: every test owns its rows via
-- fresh UUIDs (application, executor, workflow id). The tracer arrives
-- as a parameter — FastLogger here, the sim carrier in
-- 'DBOS.Transact.ManagementTestSim' — and launches go through it over an
-- explicitly built connection, so each scenario drives the same engine
-- calls on both stacks.
liveMgmtFixture :: IO Postgres.PostgresSystemDB -> IO (SomeTracer IO) -> IO (MgmtFixture IO)
liveMgmtFixture getBackend getTracer = do
  fresh <- UUID.V4.nextRandom
  let suffix = Text.pack (UUID.toString fresh)
      appName = "hs-l2-mgmt-" <> Text.take 12 suffix
      appVersion = "hs-l2-mgmt-version-" <> suffix
      executorId = "hs-l2-mgmt-executor-" <> suffix
  config0 <- configFromEnv appName
  let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
      identity =
        Identity
          { identityAppName = appName,
            identityAppVersion = appVersion,
            identityExecutorId = executorId,
            identityAppId = ""
          }
  backend <- getBackend
  tracer <- getTracer
  mkMgmtFixture
    config
    identity
    appName
    uuidWorkflowId
    uuidEntropy
    (SomeSystemDB backend)
    tracer
    (pure suffix)

-- | One workflow row as stored: a reader over the suite backend.
readRow :: IO Postgres.PostgresSystemDB -> WorkflowId -> IO WorkflowRecord
readRow getBackend wid = do
  backend <- getBackend
  found <- getWorkflow backend wid
  case found of
    Right (Just record) -> pure record
    _ -> fail "expected the workflow row to exist"

freshSuffix :: IO Text
freshSuffix = Text.pack . UUID.toString <$> UUID.V4.nextRandom

instanceFor :: Text -> IO (DBOS IO, Text)
instanceFor label = do
  suffix <- freshSuffix
  base <- configFromEnv (("hs-l2-" <> label <> "-") <> Text.take 16 suffix)
  let config = base {configAppVersion = Just ("hs-l2-version-" <> suffix), configExecutorId = Just ("hs-l2-executor-" <> suffix)}
  dbos <- newDBOS config
  pure (dbos, suffix)

-- | A launched-instance test owns its backend: shutdown runs even when
-- assertions fail, so repeated evals cannot exhaust Postgres connections.
withInstance :: Text -> (DBOS IO -> Text -> IO a) -> IO a
withInstance label action = bracket (instanceFor label) (shutdown . fst) (uncurry action)

launchOrFail :: DBOS IO -> IO (Executor IO)
launchOrFail dbos = do
  started <- launchWithEnvironment dbos isolatedEnvironment
  either (fail . show) pure started

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }

registerRefOrFail :: (FromJSON argument, ToJSON result) => DBOS IO -> WorkflowKey -> (forall exec. argument -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) result)) -> IO (WorkflowRef IO EngineOnly)
registerRefOrFail dbos key body = do
  registered <- registerDBOSWorkflowRef dbos key body
  either (fail . show) pure registered

retrieveOrFail :: DBOS IO -> WorkflowId -> IO (WorkflowHandle IO EngineOnly)
retrieveOrFail dbos workflowId = do
  retrieved <- retrieveWf dbos workflowId
  either (fail . show) pure retrieved

decodeResult :: Text -> Either CodecError Int
decodeResult output = decodeWorkflowValue "result" (Just (SerializedWorkflowValue output Nothing))

decodeSerializedResult :: SerializedWorkflowValue -> Either CodecError Int
decodeSerializedResult output = decodeWorkflowValue "result" (Just output)

decodeSerializedChildId :: SerializedWorkflowValue -> Either CodecError Text
decodeSerializedChildId output = decodeWorkflowValue "result" (Just output)

workflowTextOf :: WorkflowHandle IO EngineOnly -> Text
workflowTextOf handle = handle.workflowId
