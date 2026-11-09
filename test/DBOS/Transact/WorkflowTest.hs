{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Workflow execution behavior through the class-backed engine
-- runner: the live tree. Shared scenario bodies and checks live in
-- 'DBOS.Transact.WorkflowCases'.
module DBOS.Transact.WorkflowTest (tests) where

import DBOS.DualStack (liveCase)
import DBOS.Prelude
import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, (.:), (.=))
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (AwaitedOutcome (..), DatabaseTime (..), NewWorkflow (..), StepRecord (..), WorkflowId (..), WorkflowIdReusePolicy (..), WorkflowInitResult (..), WorkflowRecord (..), getWorkflow, listSteps, newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact.Serialization (WorkflowInput, decodeWorkflowValue, encodeWorkflowInput, unWorkflowInput, CodecError (..))
import DBOS.Transact
  (
    EngineOnly,
    Config (..),
    Environment (..),
    Error (..),
    RunOptions (..),
    Serialization (..),
    SerializedWorkflowValue (..),
    StartOptions (..),
    WorkflowHandle,
    DBOS,
    WorkflowCtx,
    Executor,
    WorkflowStatus (..),
    awaitChild,
    cancelWorkflowsInWorkflow,
    configFromEnv,
    enqueueWorkflow,
    launchWithEnvironment,
    newDBOS,
    newQualifiedWorkflowName,
    registerWorkflowRef,
    registerWorkflow,
    QualifiedWorkflowName,
    WorkflowRef,
    runWorkflow,
    runWorkflowRef,
    runOptionsDefault,
    runStep,
    selectWorkflow,
    setEvent,
    startChildWorkflow,
    shutdown,
    startWorkflow,
    queueRequestNew,
    handleResult,
    startOptionsDefault,
    waitForWorkflow,
  )
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.Transact.Logger (SomeTracer (..), acquireLoggerBackend, ioTracer, nullTracer)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Error (decodeErrorText)
import DBOS.Transact.Workflow (abortAll, newTasks, tasksSpawner)
import DBOS.Transact.Connection (SomeSystemDB (..), uuidWorkflowId)
import DBOS.Transact.Context (spawnLocal)
import DBOS.SystemDB.Retry (uuidEntropy)
import GHC.Conc (ThreadStatus (..), threadStatus)
import Data.Int (Int64)
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))
import DBOS.Transact.WorkflowCases
  ( timeoutOptionsCase,
    startErrorCase,
    WfFixture (..),
    scenarioRegisteredRecordsResult,
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
    scenarioDedupStartRecorded,
    scenarioConflictStartRecorded,
    scenarioRejectStartRecorded,
    scenarioEnqueueByNameIsChild,
    scenarioEnqueueByNameWritesRow,
    scenarioEnqueueByNameDedupReplays,
    scenarioEnqueueByNameDeadlines,
    scenarioEnqueueByNameRefusedNoStep,
    checkRegisteredResult,
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
    checkConflictStartRecorded,
    checkRejectStartRecorded,
    checkEnqueueByNameIsChild,
    checkEnqueueByNameWritesRow,
    checkEnqueueByNameDedupReplays,
    checkEnqueueByNameDeadlines,
    checkEnqueueByNameRefusedNoStep,
    taskAbortAllWaits,
    taskFinishedNotRegistered,
    taskRefusedAfterSweep,
    taskEmptySweep,
    taskEarlyFinishNotSwept,
    taskWaitAllUntilDeadline,
    taskRunningNamesTasks,
    checkNoMiscounts,
    mkWfFixture
  )



tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
  withResource acquireLoggerBackend snd $ \getLogger ->
  testGroup
    "Workflow execution"
    [ liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a registered workflow starts and records its result" scenarioRegisteredRecordsResult checkRegisteredResult,
      -- IO only: crash-and-relaunch recovery sweep (MemSystemDB delegates
      -- reenqueueForRecovery to the canned mock; see ADR-0020).
      testCase "a recovery run replays completed steps after a body interruption" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-recovery-" <> Text.take 12 suffix
            appVersion = "hs-l2-recovery-version-" <> suffix
            executorId = "hs-l2-recovery-executor-" <> suffix
            workflowText = "hs-l2-recovery-workflow-" <> suffix
            workflowId = WorkflowId workflowText
            key = newQualifiedWorkflowName "recover"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
        shouldCrash <- newIORef True
        bodyCalls <- newIORef (0 :: Int)
        let body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value wctx = do
              completedStep <- runStep wctx "once" (const (modifyIORef' bodyCalls (+ 1) >> pure (value * 2)))
              case completedStep of
                Left err -> pure (Left err)
                Right result -> do
                  crash <- readIORef shouldCrash
                  if crash
                    then ioError (userError "interrupted after checkpoint")
                    else pure (Right result)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflow dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchExec dbos isolatedEnvironment
          first <- try (runWf exec key workflowId (Just (encodeWorkflowInput (21 :: Int)))) :: IO (Either SomeException (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
          _ <- case first of
            Left exception -> assertBool "body interruption escapes without a workflow outcome" ("interrupted after checkpoint" `Text.isInfixOf` Text.pack (show exception))
            Right result -> fail (show result)
          assertEqual "the step ran before interruption" 1 =<< readIORef bodyCalls
          shutdown dbos
          writeIORef shouldCrash False
          _ <- launchExec dbos isolatedEnvironment
          settled <- timeout 10000000 (waitForWorkflow dbos workflowId)
          case settled of
            Just (Right (AwaitedSucceeded (Just output) serialization)) -> do
              let stored = SerializedWorkflowValue output (Serialization <$> serialization)
                  decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "recovery completes the same workflow" (Right 42) decoded
            other -> fail (show other)
          assertEqual "the replay adopts the recorded step" 1 =<< readIORef bodyCalls
          adopted <- runWf exec key (WorkflowId workflowText) (Just (encodeWorkflowInput (21 :: Int)))
          case adopted of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "a duplicate run awaits and adopts the stored result" (Right 42) decoded
            other -> fail (show other),
      -- IO only: the 0.5 step-names replay (slice #154). The parent sets
      -- an event, starts and awaits a child, and cancels a workflow queued
      -- where nothing polls, then blocks at a gate; shutdown abandons it
      -- there. The history's names are overwritten with 0.5's literals, so
      -- the relaunch must replay every recorded step from its checkpoint —
      -- without starting the child again — and finish. A constant renamed
      -- away from 0.5's spelling fails the replay with UnexpectedStep, not
      -- just the pin test above. Mirrors the oracle's
      -- @a_history_under_the_0_5_step_names_replays@; the recorded step
      -- sequence is derived from our own engine (setEvent, child start
      -- under the child's name, getResult await, cancelWorkflow), not from
      -- the oracle's step ids.
      testCase "a history under the 0.5 step names replays" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-stepnames-" <> Text.take 12 suffix
            appVersion = "hs-l2-stepnames-version-" <> suffix
            executorId = "hs-l2-stepnames-executor-" <> suffix
            parentText = "hs-l2-stepnames-parent-" <> suffix
            targetText = "hs-l2-stepnames-target-" <> suffix
            childKey = newQualifiedWorkflowName "child"
            targetKey = newQualifiedWorkflowName "target"
            parentKey = newQualifiedWorkflowName "parent"
        reached <- newEmptyMVar
        release <- newEmptyMVar
        childRuns <- newIORef (0 :: Int)
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            childBody :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            childBody n _ = modifyIORef' childRuns (+ 1) >> pure (Right (n * 2))
            targetBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            targetBody () _ = pure (Right 1)
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRefE <- registerWorkflowRef dbos childKey childBody
          childRef <- case childRefE of
            Left err -> fail (show err)
            Right ref -> pure ref
          targetRegistered <- registerWorkflow dbos targetKey targetBody
          case targetRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          let parentBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              parentBody targetId wctx = do
                setR <- setEvent wctx "progress" (1 :: Int)
                case setR of
                  Left err -> pure (Left err)
                  Right () -> do
                    started <- startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowInput (21 :: Int)))
                    case started of
                      Left err -> pure (Left err)
                      Right handle -> do
                        awaited <- awaitChild wctx handle
                        case awaited of
                          Left err -> pure (Left err)
                          Right Nothing -> pure (Left (StepFailed "parent" "no child output"))
                          Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                            Left err -> pure (Left (StepFailed "parent" (Text.pack (show err))))
                            Right doubled -> do
                              cancelled <- cancelWorkflowsInWorkflow wctx [WorkflowId targetId] False
                              case cancelled of
                                Left err -> pure (Left err)
                                Right _ -> putMVar reached () >> takeMVar release >> pure (Right doubled)
          parentRegistered <- registerWorkflow dbos parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchExec dbos isolatedEnvironment
          -- The target sits on a queue nothing polls, so it is there to be cancelled.
          enqueued <- enqueueWorkflow dbos targetKey (WorkflowId targetText) Nothing "nothing-polls-this"
          case enqueued of
            Left err -> fail (show err)
            Right _ -> pure ()
          worker <- async (runWf exec parentKey (WorkflowId parentText) (Just (encodeWorkflowInput targetText)))
          entered <- timeout 15000000 (takeMVar reached)
          case entered of
            Nothing -> fail "the workflow never reached its gate"
            Just _ -> pure ()
          -- Abandoned at the gate: the steps above are recorded, the gate
          -- below is not.
          shutdown dbos
          cancel worker
          -- Write 0.5's names over whatever this build recorded.
          backend <- getBackend
          let expected = [(0, "DBOS.setEvent"), (1, "child"), (2, "DBOS.getResult"), (3, "DBOS.cancelWorkflow")]
          forM_ expected $ \(step, name) -> do
            updated <- Postgres.runSession backend "step-names-rewrite" (Session.statement (parentText, step, name) rewriteStepName)
            case updated of
              Right 1 -> pure ()
              other -> fail ("no step " <> show step <> " to rewrite: " <> show other)
          history <- listSteps backend (WorkflowId parentText) False Nothing Nothing Nothing
          case history of
            Left err -> fail (show err)
            Right found -> assertEqual "the history is 0.5's" expected (map (\record -> (record.stepRecordStepId, record.stepRecordStepName)) found)
          _ <- launchExec dbos isolatedEnvironment
          reentered <- timeout 15000000 (takeMVar reached)
          case reentered of
            Nothing -> fail "the recovered workflow never reached its gate again"
            Just _ -> pure ()
          putMVar release ()
          settled <- timeout 10000000 (waitForWorkflow dbos (WorkflowId parentText))
          case settled of
            Just (Right (AwaitedSucceeded (Just output) serialization)) -> do
              let stored = SerializedWorkflowValue output (Serialization <$> serialization)
                  decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "recovery completes with the doubled child result" (Right 42) decoded
            other -> fail (show other)
          assertEqual "the child ran once" 1 =<< readIORef childRuns
          replayed <- listSteps backend (WorkflowId parentText) False Nothing Nothing Nothing
          case replayed of
            Left err -> fail (show err)
            Right found -> assertEqual "the replay recorded nothing new" expected (map (\record -> (record.stepRecordStepId, record.stepRecordStepName)) found),
      -- IO only: same recovery sweep as above.
      testCase "an unregistered workflow is skipped and the rest recover" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-skip-" <> Text.take 12 suffix
            appVersion = "hs-l2-skip-version-" <> suffix
            -- One executor identity across both instances, as the oracle's
            -- default executor id gives both of its objects: recovery sweeps
            -- rows stamped with this executor's own id.
            execId = "hs-l2-skip-exec-" <> suffix
            firstExec = execId
            secondExec = execId
            ghostText = "hs-l2-skip-ghost-" <> suffix
            keeperText = "hs-l2-skip-keeper-" <> suffix
            ghostKey = newQualifiedWorkflowName "ghost"
            keeperKey = newQualifiedWorkflowName "keeper"
        gate <- newEmptyMVar
        enteredGhost <- newEmptyMVar
        enteredKeeper <- newEmptyMVar
        config0 <- configFromEnv appName
        let firstConfig = config0 {configAppVersion = Just appVersion, configExecutorId = Just firstExec}
            gated :: StrictMVar IO () -> forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
            gated entered () _ = putMVar entered () >> takeMVar gate >> pure (Right ())
        bracket (newDBOS firstConfig) shutdown $ \first -> do
          ghostRegistered <- registerWorkflowRef first ghostKey (gated enteredGhost)
          ghostRef <- case ghostRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          keeperRegistered <- registerWorkflowRef first keeperKey (gated enteredKeeper)
          keeperRef <- case keeperRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          execFirst <- launchExec first isolatedEnvironment
          -- Ghost first, so the sweep meets the skip before the recovery.
          ghostWorker <- async (runWfRef execFirst ghostRef (runOptionsDefault {runWorkflowId = Just (WorkflowId ghostText)}) Nothing)
          keeperWorker <- async (runWfRef execFirst keeperRef (runOptionsDefault {runWorkflowId = Just (WorkflowId keeperText)}) Nothing)
          entered <- timeout 15000000 (takeMVar enteredGhost >> takeMVar enteredKeeper)
          case entered of
            Nothing -> fail "the abandoned runs never started"
            Just _ -> pure ()
          -- Abandoned mid-run: both block at the gate until shutdown.
          shutdown first
          cancel ghostWorker
          cancel keeperWorker
        -- A new instance on the same database, with ghost's code removed.
        let secondConfig = config0 {configAppVersion = Just appVersion, configExecutorId = Just secondExec}
        bracket (newDBOS secondConfig) shutdown $ \second -> do
          keeperRegistered <- registerWorkflowRef second keeperKey (\() _ -> pure (Right ()) :: IO (Either (Error EngineOnly) ()))
          case keeperRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          _ <- launchExec second isolatedEnvironment
          settled <- timeout 10000000 (waitForWorkflow second (WorkflowId keeperText))
          case settled of
            Just (Right (AwaitedSucceeded _ _)) -> pure ()
            other -> fail ("expected the keeper to recover, got: " <> show other)
          ghostStatus <- readWorkflowStatus getBackend (WorkflowId ghostText)
          ghostStatus @?= Just Pending,
      testCase "timeouts, options, and child ids compose without a database" timeoutOptionsCase,
      testCase "only a child start's refusals are recorded and replay" startErrorCase,
      -- The double-click: the same id while the first run still owns it
      -- joins rather than failing — one id, one execution.
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "starting a taken id joins the existing run" scenarioJoinTakesId checkJoinTakesId,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a fresh start is local and a join polls" scenarioFreshJoinPolls checkFreshJoinPolls,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a queued row is joined, not run, by a plain start" scenarioQueuedStartPolls checkQueuedStartPolls,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "non-finite floats store aeson's spelling and read back" scenarioNonFinite checkNonFinite,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "awaiting a child is recorded as a step" scenarioAwaitRecorded checkAwaitRecorded,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a recorded await of another workflow is refused" scenarioStaleAwaitRefused checkStaleAwaitRefused,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "awaiting a child inside a step is covered by that step" scenarioAwaitInsideStep checkAwaitInsideStep,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "child starts and awaits keep their ids in build order" scenarioChildIdsInBuildOrder checkChildIdsInBuildOrder,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "runs claim their pairs of step ids adjacently" scenarioStepIdPairs checkStepIdPairs,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a select step races a step against a child's result" scenarioSelectStepRaces checkSelectStepRaces,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a scoped select races two pending steps" scenarioScopedSelect checkScopedSelect,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a converted body runs through the scoped entries" scenarioScopedBody checkScopedBody,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a control signal winning a select records no winner" scenarioControlSelect checkControlSelect,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a losing step has its cancellation token fired" scenarioLosingTokenFired checkLosingTokenFired,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a cancelled child is an awaited cancellation in the parent" scenarioCancelledChildAwaited checkCancelledChildAwaited,
      -- IO only: recorded-await replay across two launches (needs the
      -- recovery sweep).
      testCase "a replayed parent reads the recorded outcome rather than waiting again" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-replay-await-" <> Text.take 12 suffix
            appVersion = "hs-l2-replay-await-version-" <> suffix
            executorId = "hs-l2-replay-await-executor-" <> suffix
            parentText = "hs-l2-replay-await-parent-" <> suffix
            childText = parentText <> "-0"
            childKey = newQualifiedWorkflowName "child"
            parentKey = newQualifiedWorkflowName "parent"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
            childBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            childBody () _ = pure (Right 5)
        bracket (newDBOS config) shutdown $ \first -> do
          childRegistered <- registerWorkflowRef first childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          -- First process: the child finishes and the await is recorded,
          -- then the parent is killed before it can finish.
          let parentBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              parentBody () wctx = do
                started <- startChildWorkflow wctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWf wctx wfHandle
                    case awaited of
                      Left err -> pure (Left err)
                      Right (Just stored) -> do
                        threadDelay 30000000
                        pure $ case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> pure (Left (StepFailed "parent" "no child output"))
          parentRegistered <- registerWorkflowRef first parentKey parentBody
          parentRef <- case parentRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          execFirst <- launchExec first isolatedEnvironment
          _ <- startWfRef execFirst parentRef (startOptionsDefault {startWorkflowId = Just (WorkflowId parentText)}) Nothing
          reader <- getBackend
          let awaitRecorded = go (200 :: Int)
                where
                  go 0 = fail "the await was never recorded"
                  go n = do
                    rows <- listSteps reader (WorkflowId parentText) False Nothing Nothing Nothing
                    case rows of
                      Right found | length found >= 2 -> pure ()
                      _ -> threadDelay 50000 >> go (n - 1)
          awaitRecorded
        -- The child is gone. Only the parent's recorded copy of its result
        -- is left, so finishing is only possible from the recorded row.
        reader <- getBackend
        deleted <- SystemDB.deleteWorkflows reader [WorkflowId childText] False Nothing
        case deleted of
          Left err -> fail (show err)
          Right _ -> pure ()
        gone <- getWorkflow reader (WorkflowId childText)
        gone @?= Right Nothing
        -- Second process: recovery replays the parent, which must not go
        -- looking for the child.
        bracket (newDBOS config) shutdown $ \second -> do
          childRegistered <- registerWorkflowRef second childKey childBody
          childRef2 <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          let parentBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              parentBody () wctx = do
                started <- startChildWorkflow wctx childRef2 startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWf wctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          parentRegistered <- registerWorkflowRef second parentKey parentBody
          case parentRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          _ <- launchExec second isolatedEnvironment
          settled <- timeout 15000000 (waitForWorkflow second (WorkflowId parentText))
          case settled of
            Just (Right (AwaitedSucceeded (Just output) _)) -> do
              let stored = SerializedWorkflowValue output Nothing
                  decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the replayed parent read the recorded await" (Right 5) decoded
            other -> fail ("expected the replayed parent to finish, got: " <> show other),
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a child inherits its parent's deadline" scenarioDeadlineInherited checkDeadlineInherited,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a child's own timeout replaces the inherited deadline" scenarioChildBudgetWins checkChildBudgetWins,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a child can decline the inherited deadline" scenarioDeclinedDeadline checkDeclinedDeadline,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a parent and its child hit an inherited deadline independently" scenarioCascadeDeadline checkCascadeDeadline,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a parent starts a child under a derived id and replay adopts it" scenarioDerivedChildAdopted checkDerivedChildAdopted,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "starting a child inside a step is refused, not recorded" scenarioChildInsideStepRefused checkChildInsideStepRefused,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "starting a child through a captured parent is refused, not recorded" scenarioCaptureChildRefused checkCaptureChildRefused,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a child that fails differently is started through lift" scenarioLiftChildError checkLiftChildError,
      -- IO only: the body performs real IO (the foreign charge call),
      -- which the simulator cannot run.
      testCase "a foreign error is converted at the boundary" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-foreign-" <> Text.take 12 suffix
            payText = "hs-l2-foreign-pay-" <> suffix
            payKey = newQualifiedWorkflowName "pay"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          let payBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error PaymentError) ())
              payBody () _ = do
                charged <- charge
                pure $ case charged of
                  Left refused -> Left (ErrorApplication (Gateway (Text.pack (show refused))))
                  Right () -> Right ()
          payRegistered <- registerWorkflow dbos payKey payBody
          case payRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchExec dbos isolatedEnvironment
          ran <- runWorkflow exec payKey (WorkflowId payText) (Just (encodeWorkflowInput ()))
          case ran of
            Left (ErrorApplication (Gateway {reason})) -> reason @?= "the gateway refused the card"
            other -> fail ("expected the boundary conversion, got: " <> show other)
          reader <- getBackend
          payRow <- getWorkflow reader (WorkflowId payText)
          case payRow of
            Right (Just row) -> do
              row.workflowRecordStatus @?= Error
              case row.workflowRecordError of
                Just recorded -> case decodeErrorText recorded :: Either Text (Error PaymentError) of
                  Right (ErrorApplication (Gateway {reason})) -> reason @?= "the gateway refused the card"
                  other -> fail ("expected the encoded error in the column, got: " <> show other)
                Nothing -> fail "the pay workflow recorded no error"
            other -> fail ("expected the pay row, got: " <> show other),
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a child started and never awaited is still recorded" scenarioUnawaitedChild checkUnawaitedChild,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "children started in a loop run concurrently" scenarioFanout checkFanout,
      -- IO only: first-to-settle timing is wall-clock-bound.
      testCase "select reports the first workflow to settle, not the first started" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-first-" <> Text.take 12 suffix
            waiterText = "hs-l2-first-waiter-" <> suffix
            childKey = newQualifiedWorkflowName "blocked"
            childText n = "hs-l2-first-blocked-" <> Text.pack (show (n :: Int)) <> "-" <> suffix
        entered <- mapM (const newEmptyMVar) [0, 1, 2 :: Int]
        gates <- mapM (const newEmptyMVar) [0, 1, 2 :: Int]
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            childBody :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            childBody n _ = putMVar (entered !! n) () >> takeMVar (gates !! n) >> pure (Right n)
            readerBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
            readerBody _ wctx = do
              won <- selectWorkflow wctx (map (WorkflowId . childText) [0, 1, 2])
              pure $ case won of
                Left err -> Left err
                Right (WorkflowId winnerText) -> Right winnerText
        bracket (newDBOS config) shutdown $ \dbos -> do
          childRegistered <- registerWorkflowRef dbos childKey childBody
          childRef <- case childRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          readerRegistered <- registerWorkflowRef dbos (newQualifiedWorkflowName "reader") readerBody
          readerRef <- case readerRegistered of
            Left err -> fail (show err)
            Right ref -> pure ref
          exec <- launchExec dbos isolatedEnvironment
          mapM_
            ( \n -> do
                startedChild <-
                  startWfRef
                    exec
                    childRef
                    (startOptionsDefault {startWorkflowId = Just (WorkflowId (childText n))})
                    (Just (encodeWorkflowInput n))
                case startedChild of
                  Left err -> fail (show err)
                  Right _ -> pure ()
            )
            [0, 1, 2 :: Int]
          -- All three are at their gates, so nothing has settled and none
          -- can win by accident.
          mapM_ takeMVar entered
          startedReader <-
            startWfRef
              exec
              readerRef
              (startOptionsDefault {startWorkflowId = Just (WorkflowId waiterText)})
              (Just (encodeWorkflowInput ()))
          readerHandle <- case startedReader of
            Left err -> fail (show err)
            Right handle -> pure handle
          -- Released after the wait is in flight, so the wait genuinely
          -- waits rather than reading an already-settled row. Either way
          -- the middle one wins: the others never settle.
          threadDelay 200000
          -- The middle one, neither the first id passed nor the first
          -- started: the answer has to name it.
          putMVar (gates !! 1) ()
          decided <- timeout 15000000 (handleResult readerHandle)
          case decided of
            Just (Right (Just stored)) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
              Right winnerText -> winnerText @?= childText 1
              Left err -> fail (show err)
            other -> fail ("expected the middle workflow to win, got: " <> show other)
          mapM_ (`putMVar` ()) [gates !! 0, gates !! 2],
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "an assigned child id wins over the derived one" scenarioAssignedChildAdopted checkAssignedChildAdopted,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a workflow started outside a workflow has no parent" scenarioRootNoParent checkRootNoParent,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a start position holding a plain step is refused" scenarioPlainStepAtStart checkPlainStepAtStart,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a child started through another instance is refused" scenarioWrongInstance checkWrongInstance,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a child joining a held key is recorded as the workflow it joined" scenarioJoinHeldKey checkJoinHeldKey,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "an in-workflow enqueue is a recorded child start that replays" scenarioEnqueuedChildReplays checkEnqueuedChildReplays,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a refused child start replays after the key is freed" scenarioDedupStartRecorded checkDedupStartRecorded,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a taken child id replays after the row is deleted" scenarioConflictStartRecorded checkConflictStartRecorded,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a rejected child start replays after the row is deleted" scenarioRejectStartRecorded checkRejectStartRecorded,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a workflow enqueued by name is a child and a replay does not enqueue it again" scenarioEnqueueByNameIsChild checkEnqueueByNameIsChild,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "an enqueue by name writes a client's row and is refused inside a step" scenarioEnqueueByNameWritesRow checkEnqueueByNameWritesRow,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a deduplicated enqueue by name replays the way it went" scenarioEnqueueByNameDedupReplays checkEnqueueByNameDedupReplays,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "an enqueue by name inherits its parent's deadline" scenarioEnqueueByNameDeadlines checkEnqueueByNameDeadlines,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "bad enqueue options are refused before the step id is taken" scenarioEnqueueByNameRefusedNoStep checkEnqueueByNameRefusedNoStep,
      testCase "a rejecting start refuses a taken id and joins by default" $ do
        -- Mirrors Rust @a_rejecting_start_or_enqueue_refuses_a_taken_id@: a
        -- Reject start or enqueue whose id is taken runs nothing, even when
        -- the holder finished; the default joins it.
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-reuse-policy-" <> Text.take 12 suffix
            appVersion = "hs-l2-reuse-policy-version-" <> suffix
            executorId = "hs-l2-reuse-policy-executor-" <> suffix
            takenText = "hs-l2-reuse-policy-id-" <> suffix
            queueName = "hs-l2-reuse-policy-q-" <> Text.take 12 suffix
            key = newQualifiedWorkflowName "counted"
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
        runs <- newIORef (0 :: Int)
        let body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body value _ = modifyIORef' runs (+ 1) >> pure (Right value)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerWorkflowRef dbos key body
          counted <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          exec <- launchExec dbos isolatedEnvironment
          let rejecting queue =
                (startOptionsDefault {startWorkflowId = Just (WorkflowId takenText), startWorkflowIdReusePolicy = WorkflowIdReject}) {startQueue = queue}
          firstStarted <- startWfRef exec counted (rejecting Nothing) (Just (encodeWorkflowInput (1 :: Int)))
          firstHandle <- case firstStarted of
            Left err -> fail ("a fresh id is not in use, got: " <> show err)
            Right handle -> pure handle
          first <- handleResult firstHandle
          case first of
            Right (Just stored) ->
              case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                Right 1 -> pure ()
                other -> fail ("expected the first run's result, got: " <> show other)
            other -> fail ("expected the first run's result, got: " <> show other)
          let checkRefused queueName' queue = do
                refused <- startWfRef exec counted (rejecting queue) (Just (encodeWorkflowInput (2 :: Int)))
                case refused of
                  Left (SystemDatabase (SystemDBError.WorkflowIdInUse {workflowId = wid, status = st, existingName = name})) -> do
                    wid @?= takenText
                    st @?= "SUCCESS"
                    name @?= Just "counted"
                  other -> fail ("expected WorkflowIdInUse, got: " <> show other)
          checkRefused "plain" Nothing
          checkRefused "queued" (Just (queueRequestNew queueName))
          joined <- runWfRef exec counted (runOptionsDefault {runWorkflowId = Just (WorkflowId takenText)}) (Just (encodeWorkflowInput (3 :: Int)))
          case joined of
            Right (Just stored) ->
              case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                Right 1 -> pure ()
                other -> fail ("expected the joined result, got: " <> show other)
            other -> fail ("expected the joined result, got: " <> show other)
          let checkNeedsId queue = do
                refused <- startWfRef exec counted ((startOptionsDefault {startWorkflowIdReusePolicy = WorkflowIdReject}) {startQueue = queue}) (Just (encodeWorkflowInput (4 :: Int)))
                case refused of
                  Left (InvalidArgument _ detail) ->
                    assertBool "needs a workflow_id" ("`WorkflowIdReusePolicy::Reject` needs a `workflow_id`" `Text.isInfixOf` detail)
                  other -> fail ("expected an InvalidArgument refusal, got: " <> show other)
          checkNeedsId Nothing
          checkNeedsId (Just (queueRequestNew queueName))
          runsFinal <- readIORef runs
          runsFinal @?= 1,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a zero-argument workflow records no input" scenarioZeroNoInput checkZeroNoInput,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "the row exists before the body starts" scenarioRowBeforeBody checkRowBeforeBody,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a panicking workflow leaves its row pending" scenarioPanic checkPanic,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "retrieving before launch is refused" scenarioRetrieveBeforeLaunch checkRunBeforeLaunch,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "an application error round-trips as itself" scenarioAppErrorRoundtrip checkAppErrorRoundtrip,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a database failure is not the workflow outcome" scenarioDbFailureNotOutcome checkDbFailureNotOutcome,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a workflow records the steps it took" scenarioStepsTaken checkStepsTaken,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "shutdown cancels a running workflow and leaves it pending" scenarioShutdownCancels checkShutdownCancels,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "dropping the future does not stop the workflow" scenarioDropFuture checkDropFuture,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a budget cancels the workflow durably" scenarioBudgetCancels checkBudgetCancels,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a started workflow carries the attributes it was given" scenarioAttributes checkAttributes,
      liveCase (liveWfFixture getBackend (ioTracer . fst <$> getLogger)) "a step error is recorded in its column" scenarioStepErrorRecorded checkStepErrorRecorded,
      -- Sim only: typed trace assertions live only in sim.
      testCase "workflow announcements carry their counts and ids" (pure ()),
      tasksTests
    ]


-- | The shared tree over a real backend: every test owns its rows via
-- fresh UUIDs (application, executor, workflow id). The tracer arrives
-- as a parameter — FastLogger on IO, the sim carrier on IOSim — and the
-- launch goes through it over an explicitly built connection, so the
-- scenario drives the same launch call on both stacks. Production launch
-- (supervisor, prepare, version check) stays covered by the unconverted
-- live cases that still use @launchWithEnvironment@.
liveWfFixture :: IO Postgres.PostgresSystemDB -> IO (SomeTracer IO) -> IO (WfFixture IO)
liveWfFixture getBackend getTracer = do
  fresh <- UUID.V4.nextRandom
  let suffix = Text.pack (UUID.toString fresh)
      appName = "hs-l2-workflow-" <> Text.take 12 suffix
      appVersion = "hs-l2-version-" <> suffix
      executorId = "hs-l2-executor-" <> suffix
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
  mkWfFixture
    config
    identity
    -- The connection's application name is the workflow rows' own (the
    -- dequeue and recovery sweeps scope by it), not a separate test name.
    appName
    (\prefix -> WorkflowId ("hs-l2-" <> prefix <> "-" <> suffix))
    uuidWorkflowId
    uuidEntropy
    (SomeSystemDB backend)
    tracer


-- | Block until a spawned thread has finished. The registry's departure
-- hook runs before the thread ends, so waiting on the thread turns "has
-- this task departed?" from a bet on a delay — the bet a fixed sleep lost
-- under load — into an observation. Bounded like 'waitFor' in
-- "DBOS.Transact.ContextTest": a waiter that never wakes fails the case
-- instead of hanging the suite.
waitFinished :: ThreadId IO -> IO ()
waitFinished tid = do
  settled <- timeout 5000000 (pollFinished tid)
  case settled of
    Just () -> pure ()
    Nothing -> fail ("the spawned task never finished: " <> show tid)

-- | Poll a thread's status until it has ended.
pollFinished :: ThreadId IO -> IO ()
pollFinished tid = do
  status <- threadStatus tid
  case status of
    ThreadFinished -> pure ()
    ThreadDied -> pure ()
    _ -> threadDelay 500 >> pollFinished tid




-- | The oracle's @Tasks@ behaviour: one body per case, shared with the
-- sim tree — the IO leaves run here under real threads and real
-- preemption (where the fork/registration race bites); the IOSim leaves
-- run in "DBOS.Transact.WorkflowTestSim" under the cooperative scheduler
-- (where the same engine code path is deterministic). The checks live
-- beside each leaf, not in the bodies: @?=@ is 'IO'-only, so a body
-- polymorphic in the stack returns a value and each tree judges it by
-- the same assertion.
tasksTests :: TestTree
tasksTests =
  testGroup
    "Tasks"
    [ testCase "abortAll waits until every task has departed" (taskAbortAllWaits @IO >>= (@?= 2)),
      testCase "a task that finished on its own is not left in the registry" (taskFinishedNotRegistered @IO waitFinished >>= (@?= 0)),
      testCase "a task arriving after the sweep is aborted on arrival" (taskRefusedAfterSweep @IO >>= (@?= False)),
      testCase "an empty sweep returns at once" (taskEmptySweep @IO >>= (@?= 0)),
      -- IO only: real preemption, not cooperation.
      testCase "a spawn refused after abort fills its channel instead of hanging" $ do
        -- Closed-arrival kill lands before the child ever runs; each
        -- iteration must fill promptly, and the per-iteration bound turns
        -- a lost fill into a failure instead of a hung suite.
        filled <- mapM (\_ -> do
            tasks <- newTasks
            _ <- abortAll tasks
            channel <- spawnLocal (tasksSpawner tasks (\_ -> pure ())) "test" (\_ -> pure ())
            timeout 1000000 (readMVar channel)
          ) [1 .. 50 :: Int]
        case [() | Nothing <- filled] of
          [] -> pure ()
          missing -> fail ("refused spawns left channels empty: " <> show (length missing)),
      testCase "a task finishing before registration is not swept as aborted" (taskEarlyFinishNotSwept @IO waitFinished >>= checkNoMiscounts),
      testCase "a bounded wait ends at the deadline with the set still held" (taskWaitAllUntilDeadline @IO >>= (@?= (False, True))),
      testCase "the registry reports the labels spawns were tracked under" (taskRunningNamesTasks @IO >>= (@?= ["alpha", "beta"]))
    ]

-- | Launch over the isolated environment and hand back the executor:
-- the live cases' one-call form of @launchWithEnvironment@ plus unwrap.
launchExec :: DBOS IO -> Environment -> IO (Executor IO)
launchExec dbos env = do
  started <- launchWithEnvironment dbos env
  case started of
    Left err -> fail (show err)
    Right executor -> pure executor

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through: each
-- pins the error channel to 'EngineOnly' (the channel the test bodies
-- declare), so a call site outside an annotated body does not leave it
-- for the compiler to guess. Local copies are deliberate — this module
-- carries only the aliases it uses, and a sibling test module repeats
-- the ones it needs.
runWf :: Executor IO -> QualifiedWorkflowName -> WorkflowId -> Maybe WorkflowInput -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWf = runWorkflow

runWfRef :: Executor IO -> WorkflowRef IO EngineOnly -> RunOptions -> Maybe WorkflowInput -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWfRef = runWorkflowRef

startWfRef :: Executor IO -> WorkflowRef IO EngineOnly -> StartOptions -> Maybe WorkflowInput -> IO (Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
startWfRef = startWorkflow

awaitWf :: WorkflowCtx exec IO -> WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
awaitWf = awaitChild

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | The status a workflow row carries: a reader over the suite backend,
-- so assertions hold after shutdown. The launched instances below keep
-- their own pools: each needs a distinct application identity.
readWorkflowStatus :: IO Postgres.PostgresSystemDB -> WorkflowId -> IO (Maybe WorkflowStatus)
readWorkflowStatus getBackend wid = do
  backend <- getBackend
  found <- getWorkflow backend wid
  case found of
    Left err -> fail (show err)
    Right Nothing -> pure Nothing
    Right (Just WorkflowRecord {workflowRecordStatus = status}) -> pure (Just status)

-- | Overwrites one recorded step's name with a 0.5 literal: the replay
-- must adopt it, so a constant renamed away from 0.5's spelling fails
-- the replay itself. Asserts one row moved, as the oracle's rewrite does.
rewriteStepName :: Statement.Statement (Text, Int, Text) Int64
rewriteStepName =
  Statement.preparable sql encoder Decoders.rowsAffected
  where
    sql =
      "UPDATE dbos.operation_outputs SET function_name = $3 \
      \WHERE workflow_uuid = $1 AND function_id = $2"
    encoder =
      contramap (\(wid, _, _) -> wid) (Encoders.param (Encoders.nonNullable Encoders.text))
        <> contramap (\(_, step, _) -> fromIntegral step) (Encoders.param (Encoders.nonNullable Encoders.int4))
        <> contramap (\(_, _, name) -> name) (Encoders.param (Encoders.nonNullable Encoders.text))

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }

-- | Somebody else's error type: 'Show' and nothing more, so it cannot
-- cross a column and is converted at the boundary.
data GatewayRefused = GatewayRefused
  deriving stock (Eq)

instance Show GatewayRefused where
  show _ = "the gateway refused the card"

-- | The workflow's own error: serializable, and what the boundary
-- conversion lands in.
data PaymentError = Gateway {reason :: Text}
  deriving stock (Eq)

instance Show PaymentError where
  show (Gateway reason) = "charging failed: " <> Text.unpack reason

instance ToJSON PaymentError where
  toJSON (Gateway reason) = object ["Gateway" .= object ["reason" .= reason]]

instance FromJSON PaymentError where
  parseJSON = withObject "PaymentError" $ \o -> do
    gateway <- o .: "Gateway"
    Gateway <$> gateway .: "reason"

-- | A foreign call whose error has no serde counterpart.
charge :: IO (Either GatewayRefused ())
charge = pure (Left GatewayRefused)
