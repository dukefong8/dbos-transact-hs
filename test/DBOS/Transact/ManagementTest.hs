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
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import DBOS.SystemDB
  ( Applications (..),
    AwaitedOutcome (..),
    Fork (..),
    NewWorkflow (..),
    QueueName (..),
    SerializedWorkflowValue (..),
    StepRecord (..),
    WorkflowFilter (..),
    WorkflowId (..),
    WorkflowInitResult (initResultCreatorXid),
    WorkflowOwner (..),
    WorkflowRecord (..),
    WorkflowStatus (..),
    defaultForkOptions,
    defaultWorkflowFilter,
    forkNew,
    getWorkflow,
    internalQueueName,
    newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact.Serialization (WorkflowInput, decodeWorkflowValue, encodeWorkflowValue, encodeWorkflowInput, CodecError (..))
import DBOS.Transact
  (
    EngineOnly,
    Config (..),
    DBOS,
    Executor,
    WorkflowCtx,
    Environment (..),
    Error (..),
    QueueConflict (..),
    StartOptions (..),
    StepInfo (..),
    StepOutcome (..),
    StepOutput (..),
    StepFailure (..),
    PortableWorkflowError (..),
    WorkflowHandle (workflowId),
    WorkflowKey,
    WorkflowRef,
    cancelWorkflowsInWorkflow,
    clientCancelWorkflows,
    clientConfigFromEnv,
    ListWorkflowStepsOptions (..),
    defaultListWorkflowStepsOptions,
    closeClient,
    configFromEnv,
    connectClient,
    defaultQueueOptions,
    defaultRewindOptions,
    deleteWorkflowsInWorkflow,
    enqueueWorkflow,
    forkWorkflows,
    forkWorkflowsInWorkflow,
    getWorkflowStatus,
    handleResult,
    handleStatus,
    launchWithEnvironment,
    listWorkflows,
    listWorkflowsInWorkflow,
    newDBOS,
    newWorkflowKey,
    registerWorkflow,
    registerWorkflowRef,
    registerQueue,
    resumeWorkflows,
    resumeWorkflowsInWorkflow,
    retrieveWorkflow,
    runWorkflow,
    runStep,
    shutdown,
    startWorkflow,
    startOptionsDefault,
    waitForWorkflow,
    cancelWorkflowsInWorkflow,
    deleteWorkflowsInWorkflow,
    forkWorkflowsInWorkflow,
    resumeWorkflowsInWorkflow,
    rewindWorkflowsInWorkflow,
    RewindOptions (rewindStartStep),
    startChildWorkflow)
import DBOS.Transact.Logger (SomeTracer (..), acquireLoggerBackend, ioTracer, nullTracer)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Instance (dequeueWorkflows, listWorkflowSteps)
import DBOS.Transact.Client (clientListWorkflowSteps, clientListWorkflowStepsWith)
import DBOS.Transact.Context (testOwner, withWorkflow)
import DBOS.Transact.Connection (SomeSystemDB (..), uuidWorkflowId)
import DBOS.SystemDB.Retry (uuidEntropy)
import DBOS.Transact.ContextTest (connOver)
import DBOS.Transact.Management (stepInfoFromRecord)
import DBOS.Transact.ManagementCases
  ( MgmtFixture (..),
    checkCancelMissing,
    checkCancelMissing,
    checkCancelResumeRun,
    checkCancelTree,
    checkDelete,
    checkForkFromBeginning,
    checkForkReplacesChild,
    checkForkFromFailure,
    checkForkFromStep,
    checkForkTakesIdAndQueue,
    checkBulkCancelResume,
    checkBulkFork,
    checkForkRefusals,
    checkForkPartitioned,
    checkForkInheritsOwner,
    checkAttributes,
    checkAttributesInWorkflow,
    checkDelayRelease,
    checkDelayReleaseInWorkflow,
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
    checkCompletionClearsCheckpoints,
    checkDelete,
    checkResumeMissing,
    checkResumeOntoQueue,
    checkRetrieve,
    checkListSteps,
    checkListStepsWithoutOutputs,
    checkListStepsPaging,
    checkListStepsInWorkflow,
    checkSingular,
    checkUnlaunched,
    mkMgmtFixture,
    mkMgmtFixture,
    scenarioCancelMissing,
    scenarioCancelResumeRun,
    scenarioCancelTree,
    scenarioDelete,
    scenarioForkFromBeginning,
    scenarioForkReplacesChild,
    scenarioForkFromFailure,
    scenarioForkFromStep,
    scenarioForkTakesIdAndQueue,
    scenarioBulkCancelResume,
    scenarioBulkFork,
    scenarioForkRefusals,
    scenarioForkPartitioned,
    scenarioForkInheritsOwner,
    scenarioAttributes,
    scenarioAttributesInWorkflow,
    scenarioDelayRelease,
    scenarioDelayReleaseInWorkflow,
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
    scenarioCompletionClearsCheckpoints,
    scenarioDelete,
    scenarioResumeMissing,
    scenarioResumeOntoQueue,
    scenarioRetrieve,
    scenarioListSteps,
    scenarioListStepsWithoutOutputs,
    scenarioListStepsPaging,
    scenarioListStepsInWorkflow,
    scenarioSingular,
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
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a workflow's steps list in execution order" scenarioListSteps checkListSteps,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a step listing can leave the outputs out" scenarioListStepsWithoutOutputs checkListStepsWithoutOutputs,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a step listing pages in step order" scenarioListStepsPaging checkListStepsPaging,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "an in-workflow step listing is itself a step" scenarioListStepsInWorkflow checkListStepsInWorkflow,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "the singular wrappers move one workflow each" scenarioSingular checkSingular,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "forking from the beginning runs the workflow again under a new id" scenarioForkFromBeginning checkForkFromBeginning,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a fork adopts the replacement for a failed child" scenarioForkReplacesChild checkForkReplacesChild,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a fork takes the id it is given" scenarioForkTakesIdAndQueue checkForkTakesIdAndQueue,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "forking from a chosen step replays the steps below it" scenarioForkFromStep checkForkFromStep,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a fork inherits the owner of the workflow it was forked from" scenarioForkInheritsOwner checkForkInheritsOwner,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "bulk cancel and resume hand back every id" scenarioBulkCancelResume checkBulkCancelResume,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a finished workflow rewound to its second step runs again under the same id" scenarioRewindRunsAgain checkRewindRunsAgain,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "rewind refusals name their cause and write nothing" scenarioRewindRefusals checkRewindRefusals,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a rewind at the first step undoes the run" scenarioRewindUndoCut checkRewindUndoCut,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a rewind keeps what fell below the cut" scenarioRewindKeepBelowCut checkRewindKeepBelowCut,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a rewind above a close keeps the close sentinel" scenarioRewindKeepStreamClose checkRewindKeepStreamClose,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a rewind names the queue, partition key and version the re-run gets" scenarioRewindQueueVersion checkRewindQueueVersion,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a workflow rewinding another as its step runs once" scenarioRewindInWorkflow checkRewindInWorkflow,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a direct rewind naming its caller records the call" scenarioRewindCallerStep checkRewindCallerStep,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a rewound parent replays to its child and adopts it" scenarioRewindAdoptsChild checkRewindAdoptsChild,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a refused rewind inside a workflow spends no step" scenarioRewindRefusedTakesNoStep checkRewindRefusedTakesNoStep,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "an executor rewind drops the datasource checkpoints" scenarioRewindDropsDatasourceCheckpoints checkRewindDropsDatasourceCheckpoints,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "every completion clears the run's datasource checkpoints" scenarioCompletionClearsCheckpoints checkCompletionClearsCheckpoints,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "an inline child clears its own datasource checkpoints on completion" scenarioChildClearsCheckpoints checkChildClearsCheckpoints,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "bulk forking hands back one new id per source, in order" scenarioBulkFork checkBulkFork,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "invalid forks are refused on the engine channel" scenarioForkRefusals checkForkRefusals,
      testCase "resuming onto a named queue through a full launch puts the workflow there" $
        withInstance "mgmt-resume-queue" $ \dbos suffix -> do
          let key = newWorkflowKey "queued"
              wid = WorkflowId ("hs-l2-mgmt-resume-q-" <> suffix)
              echoWorkflow :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
              echoWorkflow message _ = pure (Right message)
          registered <- registerWorkflow dbos key echoWorkflow
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          enqueued <-
            enqueueWorkflow
              dbos
              key
              wid
              (Just (encodeWorkflowInput ("hello" :: Text)))
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
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a workflow replacing another attributes as its step stores the replacement" scenarioAttributesInWorkflow checkAttributesInWorkflow,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a delayed workflow can be released sooner" scenarioDelayRelease checkDelayRelease,
      liveCase (liveMgmtFixture getBackend (ioTracer . fst <$> getLogger)) "a workflow moving another delay as its step releases the target" scenarioDelayReleaseInWorkflow checkDelayReleaseInWorkflow,
      testCase "a workflow cannot delete itself" $
        withInstance "mgmt-self-delete" $ \dbos suffix -> do
          let key = newWorkflowKey "self-deleter"
              wid = WorkflowId ("hs-l2-mgmt-self-" <> suffix)
              body :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
              body ownId wctx = do
                deleted <- deleteWorkflowsInWorkflow wctx [WorkflowId ownId] False
                pure (void deleted)
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          ran <-
            runWf
              exec
              key
              wid
              (Just (encodeWorkflowInput ("hs-l2-mgmt-self-" <> suffix :: Text)))
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
                started <- startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowInput root))
                case started of
                  Left err -> pure (Left err)
                  Right _ -> void <$> takeMVar observed
          registered <- registerWorkflow dbos parentKey parentBody
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          ran <-
            runWf
              exec
              parentKey
              (WorkflowId rootText)
              (Just (encodeWorkflowInput (rootText :: Text)))
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
          registeredTarget <- registerWorkflow dbos targetKey targetBody
          case registeredTarget of
            Left err -> fail (show err)
            Right () -> pure ()
          registeredOperator <- registerWorkflow dbos operatorKey operatorBody
          case registeredOperator of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          enqueued <-
            enqueueWorkflow
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
              (Just (encodeWorkflowInput (targetText :: Text)))
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
                @?= [(0, "DBOS.cancel_workflow"), (1, "DBOS.list_workflows")]
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
          sourceRegistered <- registerWorkflow dbos sourceKey sourceBody
          case sourceRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          operatorRegistered <- registerWorkflow dbos operatorKey operatorBody
          case operatorRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          ranSource <- runWf exec sourceKey (WorkflowId sourceText) (Just (encodeWorkflowInput (21 :: Int)))
          case ranSource of
            Right _ -> pure ()
            other -> fail ("expected the source to run, got: " <> show other)
          first <- try (runWf exec operatorKey (WorkflowId operatorText) (Just (encodeWorkflowInput (sourceText :: Text)))) :: IO (Either SomeException (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
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
      testCase "a recovered caller does not rewind its target again" $
        withInstance "mgmt-rewind-replay" $ \dbos suffix -> do
          -- A rewind commits under the same id, so a replay without the
          -- checkpoint would meet an ENQUEUED target and fail: crash after
          -- the call is recorded and let recovery prove the replay adopts
          -- it.
          let targetKey = newWorkflowKey "rewind-target"
              operatorKey = newWorkflowKey "rewinding-operator"
              targetText = "hs-l2-mgmt-rewind-replay-tgt-" <> suffix
              operatorText = "hs-l2-mgmt-rewind-replay-op-" <> suffix
              targetBody :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              targetBody value wctx = runStep wctx "double" (const (pure (value * 2)))
          shouldCrash <- newIORef True
          seen <- newEmptyMVar
          let operatorBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
              operatorBody target wctx = do
                rewound <- rewindWorkflowsInWorkflow wctx (WorkflowId target) (defaultRewindOptions {rewindStartStep = 0})
                case rewound of
                  Left err -> pure (Left err)
                  Right () -> do
                    void (tryPutMVar seen target)
                    crash <- readIORef shouldCrash
                    if crash
                      then liftIO (ioError (userError "rewound then crashed"))
                      else pure (Right target)
          targetRegistered <- registerWorkflow dbos targetKey targetBody
          case targetRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          operatorRegistered <- registerWorkflow dbos operatorKey operatorBody
          case operatorRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          ranTarget <- runWf exec targetKey (WorkflowId targetText) (Just (encodeWorkflowInput (21 :: Int)))
          case ranTarget of
            Right _ -> pure ()
            other -> fail ("expected the target to run, got: " <> show other)
          first <- try (runWf exec operatorKey (WorkflowId operatorText) (Just (encodeWorkflowInput (targetText :: Text)))) :: IO (Either SomeException (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
          case first of
            Left exception -> assertBool "the crash escapes" ("rewound then crashed" `Text.isInfixOf` Text.pack (show exception))
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
                Right tid -> tid @?= recorded
                Left err -> fail (show err)
            other -> fail ("expected the recovery to finish, got: " <> show other)
          listed <- listWorkflowSteps dbos (WorkflowId operatorText)
          case listed of
            Right records -> map (.stepInfoName) records @?= ["DBOS.rewind_workflow"]
            other -> fail ("expected one recorded rewind step, got: " <> show other),
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
          registeredTarget <- registerWorkflow dbos targetKey targetBody
          case registeredTarget of
            Left err -> fail (show err)
            Right () -> pure ()
          registeredOperator <- registerWorkflow dbos operatorKey operatorBody
          case registeredOperator of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          queueRegistered <- registerQueue dbos runQueue defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          enqueued <-
            enqueueWorkflow
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
              (Just (encodeWorkflowInput (targetText :: Text)))
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
                @?= [(0, "DBOS.cancel_workflow"), (1, "DBOS.resume_workflow")]
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
                registered <- registerWorkflow dbos operatorKey body
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
      testCase "a client lists a workflow's steps without recording one" $
        withInstance "mgmt-client-steps" $ \dbos suffix -> do
          let key = newWorkflowKey "client-listed"
              wid = WorkflowId ("hs-l2-mgmt-client-steps-" <> suffix)
              body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
              body () wctx = do
                one <- runStep wctx "one" (const (pure ()))
                case one of
                  Left err -> pure (Left err)
                  Right () -> do
                    two <- runStep wctx "two" (const (pure ()))
                    pure (void two)
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          ran <- runWf exec key wid (Just (encodeWorkflowInput ()))
          case ran of
            Right _ -> pure ()
            other -> fail ("expected the workflow to run, got: " <> show other)
          clientConfig <- clientConfigFromEnv
          bracket (connectClient clientConfig) (either (const (pure ())) closeClient) $ \connected ->
            case connected of
              Left err -> fail (show err)
              Right client -> do
                listed <- clientListWorkflowSteps client wid
                case listed of
                  Left err -> fail (show err)
                  Right records ->
                    [record.stepInfoName | record <- records] @?= ["one", "two"]
                paged <- clientListWorkflowStepsWith client wid defaultListWorkflowStepsOptions {limit = Just 1, offset = Just 1}
                case paged of
                  Left err -> fail (show err)
                  Right records ->
                    [record.stepInfoName | record <- records] @?= ["two"]
                without <- clientListWorkflowStepsWith client wid defaultListWorkflowStepsOptions {loadOutput = False}
                case without of
                  Left err -> fail (show err)
                  Right records ->
                    mapM_
                      (\record -> record.stepInfoOutcome @?= StepOutcomeNotLoaded)
                      records,
      testCase "a foreign step payload is an explicit undecodable outcome" $ do
        let base =
              StepRecord
                { stepRecordWorkflowId = WorkflowId "wf-foreign",
                  stepRecordStepId = 0,
                  stepRecordStepName = "foreign",
                  stepRecordOutput = Nothing,
                  stepRecordError = Nothing,
                  stepRecordChildWorkflowId = Nothing,
                  stepRecordSerialization = Just "py_pickle",
                  stepRecordStartedAt = Nothing,
                  stepRecordCompletedAt = Nothing
                }
            expectedOutput = CodecUnknownSerialization "step output" "py_pickle"
            expectedError = CodecUnknownSerialization "step error" "py_pickle"
        case stepInfoFromRecord defaultListWorkflowStepsOptions (base {stepRecordOutput = Just "gARLAy4="}) of
          StepInfo {stepInfoOutcome = StepOutcomeSucceeded (PayloadUndecodable found)} ->
            found @?= expectedOutput
          other -> fail ("expected an undecodable output outcome, got: " <> show other)
        case stepInfoFromRecord defaultListWorkflowStepsOptions (base {stepRecordError = Just "gARLAy4="}) of
          StepInfo {stepInfoOutcome = StepOutcomeFailed (FailureUndecodable found)} ->
            found @?= expectedError
          other -> fail ("expected an undecodable error outcome, got: " <> show other),
      testCase "an unfinished row and a legacy tagged error are distinct outcomes" $ do
        let base =
              StepRecord
                { stepRecordWorkflowId = WorkflowId "wf-outcomes",
                  stepRecordStepId = 0,
                  stepRecordStepName = "outcome",
                  stepRecordOutput = Nothing,
                  stepRecordError = Nothing,
                  stepRecordChildWorkflowId = Nothing,
                  stepRecordSerialization = Just "portable_json",
                  stepRecordStartedAt = Nothing,
                  stepRecordCompletedAt = Nothing
                }
            legacy = base {stepRecordError = Just "{\"WorkflowFailed\":{\"workflowId\":\"wf-outcomes\",\"message\":\"boom\"}}"}
        (stepInfoFromRecord defaultListWorkflowStepsOptions base).stepInfoOutcome
          @?= StepOutcomePending
        case stepInfoFromRecord defaultListWorkflowStepsOptions legacy of
          StepInfo {stepInfoOutcome = StepOutcomeFailed (FailurePortable PortableWorkflowError {portableWorkflowErrorName = name, portableWorkflowErrorMessage = message})} ->
            (name, message) @?= ("WorkflowFailed", "the workflow wf-outcomes failed: boom")
          other -> fail ("expected a legacy tagged error to project its name and message, got: " <> show other)
        (stepInfoFromRecord defaultListWorkflowStepsOptions {loadOutput = False} legacy).stepInfoOutcome
          @?= StepOutcomeNotLoaded,
      testCase "a refused management call spends no step id" $ do
        backend <- getBackend
        fresh <- freshSuffix
        let workflowText = "hs-l2-mgmt-refusal-" <> fresh
            initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2MgmtRefusal"}
        created <- SystemDB.initWorkflow backend initialWorkflow Nothing
        creator <- case created of
          Left err -> fail (show err)
          Right result -> pure result.initResultCreatorXid
        timed <- getLogger
        conn <- connOver backend (ioTracer (fst timed))
        -- Both calls share one scope, so the probe taking step zero proves
        -- the refused fork spent nothing.
        (refused, probe) <-
          withWorkflow conn mgmtTestIdentity (WorkflowId workflowText) Nothing (WorkflowOwner {ownerWorkflowId = WorkflowId workflowText, ownerToken = creator}) $ \wctx -> do
            -- An empty forked id must be absent rather than empty: refused
            -- before any id is taken.
            refused <- forkWorkflowsInWorkflow wctx [(forkNew "source") {forkForkedId = Just ""}] defaultForkOptions
            -- The probe that follows still takes step zero.
            probe <- (runStep wctx "probe" (const (pure ())) :: IO (Either (Error EngineOnly) ()))
            pure (refused, probe)
        case refused of
          Left (InvalidArgument _ _) -> pure ()
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
        created <- SystemDB.initWorkflow backend initialWorkflow Nothing
        creator <- case created of
          Left err -> fail (show err)
          Right result -> pure result.initResultCreatorXid
        timed <- getLogger
        conn <- connOver backend (ioTracer (fst timed))
        emptied <- withWorkflow conn mgmtTestIdentity (WorkflowId workflowText) Nothing (WorkflowOwner {ownerWorkflowId = WorkflowId workflowText, ownerToken = creator}) $ \wctx ->
          forkWorkflowsInWorkflow wctx [] defaultForkOptions
        emptied @?= Right []
        listed <- SystemDB.listSteps backend (WorkflowId workflowText) False Nothing Nothing Nothing
        case listed of
          Right [StepRecord {stepRecordStepId = sid, stepRecordStepName = name}] ->
            (sid, name) @?= (0, "DBOS.fork_workflow")
          other -> fail ("expected the empty batch at step zero, got: " <> show other),
      -- IO only: the assertion reads @operation_outputs.application_name@, a
      -- column no ported type carries (Rust's step listing omits it too), but
      -- which the cross-language schema contract another SDK's @step_owners@
      -- reads. The sim store mirrors the ported step type, so it cannot build
      -- the value without inventing a field.
      testCase "a workflow's step rows carry the owner and a fork's copies carry it too" $
        withInstance "mgmt-step-owner" $ \dbos suffix -> do
          let key = newWorkflowKey "owned-step"
              sourceText = "hs-l1-mgmt-step-owner-" <> suffix
              forkedText = "hs-l1-mgmt-step-owner-fork-" <> suffix
              appName = "hs-l2-mgmt-step-owner-" <> Text.take 16 suffix
              body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body () wctx = do
                recorded <- runStep wctx "a_step" (const (pure (8 :: Int)))
                pure (fmap (const 8) recorded)
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          ran <- runWf exec key (WorkflowId sourceText) (Just (encodeWorkflowInput ()))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          sourceOwners <- stepOwners getBackend (WorkflowId sourceText)
          sourceOwners @?= [Just appName]
          -- Forking from above the step copies its row: the copy inherits the
          -- source's owner rather than being re-stamped or left unclaimed.
          forked <-
            forkWorkflows
              dbos
              [(forkNew sourceText) {forkForkedId = Just forkedText, forkStartStep = 1}]
              defaultForkOptions
          case forked of
            Left err -> fail (show err)
            Right [WorkflowId text] -> text @?= forkedText
            other -> fail ("expected the chosen fork id, got: " <> show other)
          forkedOwners <- stepOwners getBackend (WorkflowId forkedText)
          forkedOwners @?= [Just appName],
      -- IO only: the peer's row belongs to a second application, which the sim
      -- fixture cannot build (its store keys rows by id alone, with no owner).
      testCase "an identity read reaches a peer's row while an unset list stays scoped" $
        withInstance "mgmt-read-scope" $ \dbosA suffix -> do
          let key = newWorkflowKey "peer-workflow"
              appAName = "hs-l2-mgmt-read-scope-" <> Text.take 16 suffix
              appBName = "hs-l2-mgmt-read-peer-" <> Text.take 16 suffix
              ownText = "hs-l1-mgmt-read-mine-" <> suffix
              peerText = "hs-l1-mgmt-read-peer-" <> suffix
              queueA = "hs-l1-mgmt-read-q-a-" <> Text.take 12 suffix
              queueB = "hs-l1-mgmt-read-q-b-" <> Text.take 12 suffix
              input = Just (encodeWorkflowInput (1 :: Int))
              body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body value _ = pure (Right value)
          registeredA <- registerWorkflow dbosA key body
          case registeredA of
            Left err -> fail (show err)
            Right () -> pure ()
          _ <- launchOrFail dbosA
          queuedA <- registerQueue dbosA queueA defaultQueueOptions AlwaysUpdate
          case queuedA of
            Left err -> fail (show err)
            Right _ -> pure ()
          own <- enqueueWorkflow dbosA key (WorkflowId ownText) input queueA
          case own of
            Left err -> fail (show err)
            Right _ -> pure ()
          -- The peer runs the same code under another application name. It is
          -- narrowed to its own queue so its sweep stays away from ours.
          baseB <- configFromEnv appBName
          let configB =
                baseB
                  { configAppVersion = Just ("hs-l2-peer-version-" <> suffix),
                    configExecutorId = Just ("hs-l2-peer-executor-" <> suffix),
                    configListenQueues = Just [queueB]
                  }
          bracket (newDBOS configB) shutdown $ \dbosB -> do
            registeredB <- registerWorkflow dbosB key body
            case registeredB of
              Left err -> fail (show err)
              Right () -> pure ()
            _ <- launchOrFail dbosB
            queuedB <- registerQueue dbosB queueB defaultQueueOptions AlwaysUpdate
            case queuedB of
              Left err -> fail (show err)
              Right _ -> pure ()
            peer <- enqueueWorkflow dbosB key (WorkflowId peerText) input queueB
            case peer of
              Left err -> fail (show err)
              Right _ -> pure ()
            -- An ID-keyed read is an address: it reaches the peer's row.
            byId <- listWorkflows dbosA (defaultWorkflowFilter {workflowFilterWorkflowIds = [peerText]})
            case byId of
              Left err -> fail (show err)
              Right [WorkflowRecord {workflowRecordId = foundId, workflowRecordApplicationName = owner}] -> do
                foundId @?= WorkflowId peerText
                owner @?= Just appBName
              other -> fail ("expected the peer's row, got: " <> show other)
            -- The unset filter stays scoped to this application's own rows.
            scoped <- listWorkflows dbosA (defaultWorkflowFilter {workflowFilterNames = ["peer-workflow"]})
            case scoped of
              Left err -> fail (show err)
              Right records -> do
                assertBool "our own row is listed" (WorkflowId ownText `elem` map (.workflowRecordId) records)
                assertBool "the peer's row is scoped out" (WorkflowId peerText `notElem` map (.workflowRecordId) records)
            -- Naming an application narrows even an ID-keyed read.
            narrowed <-
              listWorkflows
                dbosA
                ( defaultWorkflowFilter
                    { workflowFilterWorkflowIds = [peerText],
                      workflowFilterApplications = Named [appAName]
                    }
                )
            narrowed @?= Right []
            statusA <- getWorkflowStatus dbosA (WorkflowId peerText)
            assertBool "the status read reaches a peer's row" (statusA /= Nothing)
            retrieved <- retrieveWorkflow dbosA (WorkflowId peerText)
            case retrieved of
              Left err -> fail (show err)
              Right retrievedHandle -> do
                throughHandle <- handleStatus retrievedHandle
                case throughHandle of
                  Right (Just _) -> pure ()
                  other -> fail ("expected the peer's status through the handle, got: " <> show other),
      -- IO only: the peer's rows belong to a second application, which the sim
      -- fixture cannot build. Dequeue and recovery are the two ways a row gets
      -- picked up; both collide across applications by default, so only
      -- ownership separates them.
      testCase "a claim skips another application's queued rows" $
        withInstance "mgmt-claim-scope" $ \dbosA suffix -> do
          let key = newWorkflowKey "claimable"
              appBName = "hs-l2-mgmt-claim-peer-" <> Text.take 16 suffix
              sharedQueue = "hs-l1-mgmt-claim-shared-" <> Text.take 12 suffix
              peerQueue = "hs-l1-mgmt-claim-qb-" <> Text.take 12 suffix
              mineText = "hs-l1-mgmt-claim-mine-" <> suffix
              myInternalText = "hs-l1-mgmt-claim-mine-int-" <> suffix
              peerNamedText = "hs-l1-mgmt-claim-peer-" <> suffix
              peerInternalText = "hs-l1-mgmt-claim-peer-int-" <> suffix
              internalText = case internalQueueName of QueueName name -> name
              input = Just (encodeWorkflowInput (1 :: Int))
              body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body value _ = pure (Right value)
          registeredA <- registerWorkflow dbosA key body
          case registeredA of
            Left err -> fail (show err)
            Right () -> pure ()
          _ <- launchOrFail dbosA
          queuedA <- registerQueue dbosA sharedQueue defaultQueueOptions AlwaysUpdate
          case queuedA of
            Left err -> fail (show err)
            Right _ -> pure ()
          mineNamed <- enqueueWorkflow dbosA key (WorkflowId mineText) input sharedQueue
          case mineNamed of
            Left err -> fail (show err)
            Right _ -> pure ()
          mineInternal <- enqueueWorkflow dbosA key (WorkflowId myInternalText) input internalText
          case mineInternal of
            Left err -> fail (show err)
            Right _ -> pure ()
          baseB <- configFromEnv appBName
          let configB =
                baseB
                  { configAppVersion = Just ("hs-l2-peer-version-" <> suffix),
                    configExecutorId = Just ("hs-l2-peer-executor-" <> suffix),
                    configListenQueues = Just [peerQueue]
                  }
          bracket (newDBOS configB) shutdown $ \dbosB -> do
            registeredB <- registerWorkflow dbosB key body
            case registeredB of
              Left err -> fail (show err)
              Right () -> pure ()
            _ <- launchOrFail dbosB
            queuedB <- registerQueue dbosB peerQueue defaultQueueOptions AlwaysUpdate
            case queuedB of
              Left err -> fail (show err)
              Right _ -> pure ()
            -- The peer's rows name our registered queue and the internal one,
            -- which every application sweeps. A peer does not need the queue
            -- registered: the name is written on the row.
            peerNamed <- enqueueWorkflow dbosB key (WorkflowId peerNamedText) input sharedQueue
            case peerNamed of
              Left err -> fail (show err)
              Right _ -> pure ()
            peerInternal <- enqueueWorkflow dbosB key (WorkflowId peerInternalText) input internalText
            case peerInternal of
              Left err -> fail (show err)
              Right _ -> pure ()
            claimed <- dequeueWorkflows dbosA
            case claimed of
              Left err -> fail (show err)
              Right ids -> do
                assertBool "the peer's named row is not ours to claim" (WorkflowId peerNamedText `notElem` ids)
                assertBool "the peer's internal-queue row is not ours to claim" (WorkflowId peerInternalText `notElem` ids)
            -- Our own work on both queues still runs.
            forM_ [mineText, myInternalText] $ \wid -> do
              awaited <- waitForWorkflow dbosA (WorkflowId wid)
              case awaited of
                Right (SystemDB.AwaitedSucceeded _ _) -> pure ()
                other -> fail ("expected " <> Text.unpack wid <> " to run, got: " <> show other)
            -- The peer's named-queue row is untouched, still owned by the peer:
            -- nobody else could take it, and the peer does not poll that queue.
            peerRow <- readRow getBackend (WorkflowId peerNamedText)
            peerRow.workflowRecordStatus @?= Enqueued
            peerRow.workflowRecordApplicationName @?= Just appBName
    ]

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through: each
-- pins the error channel to 'EngineOnly' (the channel the test bodies
-- declare), so a call site outside an annotated body does not leave it
-- for the compiler to guess. Local copies are deliberate — this module
-- carries only the aliases it uses, and a sibling test module repeats
-- the ones it needs.
runWf :: Executor IO -> WorkflowKey -> WorkflowId -> Maybe WorkflowInput -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWf = runWorkflow

startWfRef :: Executor IO -> WorkflowRef IO EngineOnly -> StartOptions -> Maybe WorkflowInput -> IO (Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
startWfRef = startWorkflow

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
suiteQueuePrefixes = ["hs-l2-mgmt-fork-queue-", "hs-l2-mgmt-run-", "hs-l2-delayed-work-", "hs-l1-mgmt-"]

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

-- | The application that wrote each of a workflow's @operation_outputs@ rows,
-- in step order: the owner column, read raw. No ported step type carries it
-- (Rust's step listing omits it too), but the column is part of the
-- cross-language schema contract another SDK's @step_owners@ reads.
stepOwners :: IO Postgres.PostgresSystemDB -> WorkflowId -> IO [Maybe Text]
stepOwners getBackend (WorkflowId workflowText) = do
  backend <- getBackend
  found <-
    Postgres.runSession backend "step owners" $
      Session.statement workflowText $
        Statement.preparable
          "select application_name from dbos.operation_outputs where workflow_uuid = $1 order by function_id"
          (Encoders.param (Encoders.nonNullable Encoders.text))
          (Decoders.rowList (Decoders.column (Decoders.nullable Decoders.text)))
  either (fail . show) pure found

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

registerRefOrFail :: (FromJSON argument, ToJSON result)
                  => DBOS IO -> WorkflowKey -> (forall exec. argument -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) result)) -> IO (WorkflowRef IO EngineOnly)
registerRefOrFail dbos key body = do
  registered <- registerWorkflowRef dbos key body
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
