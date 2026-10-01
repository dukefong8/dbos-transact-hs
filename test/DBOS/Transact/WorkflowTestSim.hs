{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | 'DBOS.Transact.WorkflowTest' mirrored over simulated data: the same
-- scenarios and the same assertions as the live tree, with values asserted
-- here — including the 'IOSim' typed trace assertions, which stay in this
-- module. Each case prints its sim's 'Say' trace inline, so a plain
-- @-- $> tasty@ run shows the workflow announcements with no extra
-- plumbing. The memory backend ('MemSystemDB', fresh per case) records
-- rows, steps, and dedup holds for real, so joins, replays, and row reads
-- assert what live asserts; what needs a fleet stays live-only and says
-- so: recovery sweeps (including the recorded-await replay, which needs a
-- second process and a deleted child), unregistered skips, queued
-- supervision, and the fan-out/select timing.
module DBOS.Transact.WorkflowTestSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, selectTraceEventsDynamic)
import Data.Aeson (FromJSON (..), ToJSON (..), Value (..), object)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.SystemDB
  ( AwaitedOutcome (..),
    Outcome (..),
    StepRecord (..),
    Timestamp (..),
    WorkflowId (..),
    WorkflowRecord (..),
    WorkflowStatus (..),
    addTimeout,
    defaultWorkflowFilter,
    getWorkflow,
    listWorkflowSteps,
  )
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.IOSim (newMemDB, memLaunchOn, simInstance)
import DBOS.IOSimTracer (printSimTrace, runSimCase, simTracerSay)
import DBOS.Transact
  ( 
    application,
    decodeErrorText,
    EngineOnly,
    CodecError,
    Ctx,
    DBOS,
    DuplicationPolicy (..),
    Enqueue (..),
    Error (..),
    awaitChild,
    cancellationToken,
    RunOptions (..),
    Serialization (..),
    SelectArm (..),
    SerializedWorkflowValue (..),
    StartOptions (..),
    Provenance (..),
    WorkflowHandle (..),
    Timeout (..),
    WorkflowKey,
    WorkflowRef,
    WorkflowEvent (..),
    childWorkflowId,
    decodeWorkflowValue,
    encodeWorkflowValue,
    enqueueNew,
    firstStepStatus,
    handleResult,
    handleStatus,
    handleWorkflowId,
    millisDuration,
    newWorkflowKey,
    pendingAwait,
    pendingWorkflowStepWith,
    nextStepMarker,
    resolveTimeoutDeadline,
    registerDBOSWorkflow,
    registerDBOSWorkflowRef,
    retrieveWorkflow,
    runDBOSWorkflow,
    runDBOSWorkflowRef,
    runOptionsDefault,
    selectStep,
    runOptionsToStartOptions,
    runWorkflowStep,
    runWorkflowStepWith,
    secondsDuration,
    shutdown,
    startChildWorkflow,
    startDBOSWorkflowRef,
    startOptionsDefault,
    stepOptionsDefault,
    timeoutBudget,
    tokenCancelled,
    runTracer,
    waitForWorkflow,
    withAttempt,
    withSystemDB,
    workflowId,
  )
import Test.Tasty (DependencyType (..), TestTree, dependentTestGroup)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

tests :: TestTree
tests =
  -- Sequential: cases announce through one shared stderr, so parallel
  -- 'printSimTrace' calls would interleave their lines mid-character.
  -- 'AllFinish' keeps every case running on a failure, in order.
  dependentTestGroup
    "Workflow execution (Sim)"
    AllFinish
    [ testCase "a registered workflow starts and records its result" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let key = newWorkflowKey "double"
              body :: Int -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              body value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
          orFail =<< registerWfSim dbos key body
          memLaunchOn mem simTracerSay dbos
          result <- runWfSim dbos key (WorkflowId "sim-wf-double") (Just (encodeWorkflowValue (21 :: Int)))
          rows <- SystemDB.listWorkflows mem (defaultWorkflowFilter {SystemDB.workflowFilterWorkflowIds = ["sim-wf-double"]}) Nothing
          pure (result, rows)
        printSimTrace tr
        case outcome of
          (result, rows) -> do
            case result of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "typed result is stored" (Right 42) decoded
              other -> fail (show other)
            case rows of
              Right [row] -> do
                row.workflowRecordStatus @?= Success
                row.workflowRecordName @?= Just "double"
                row.workflowRecordOutput @?= Just "42"
                row.workflowRecordInput @?= Just "21"
                row.workflowRecordSerialization @?= Just "rust_serde"
              other -> fail ("expected exactly one successful row: " <> show other),
      testCase "timeouts, options, and child ids compose without a database" $ do
        let budget = secondsDuration 60
            now = Timestamp 1000
        timeoutBudget Inherit @?= Nothing
        timeoutBudget None @?= Nothing
        timeoutBudget (Explicit budget) @?= Just budget
        resolveTimeoutDeadline (Explicit budget) (Just (enqueueNew "q")) Nothing now @?= Nothing
        resolveTimeoutDeadline (Explicit budget) Nothing Nothing now @?= addTimeout now budget
        resolveTimeoutDeadline None Nothing (Just now) now @?= Nothing
        resolveTimeoutDeadline Inherit Nothing (Just now) now @?= Just now
        resolveTimeoutDeadline Inherit Nothing Nothing now @?= Nothing
        runOptionsDefault @?= RunOptions Nothing Inherit Nothing
        startOptionsDefault @?= StartOptions Nothing Inherit Nothing Nothing
        runOptionsToStartOptions runOptionsDefault @?= startOptionsDefault
        childWorkflowId (Just "chosen") (Just ("parent", 3)) "generated" @?= "chosen"
        childWorkflowId Nothing (Just ("parent", 0)) "generated" @?= "parent-0"
        childWorkflowId Nothing (Just ("parent", 2)) "generated" @?= "parent-2"
        childWorkflowId Nothing Nothing "generated" @?= "generated",
      testCase "starting a taken id joins the existing run" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          entered <- newTVarIO (0 :: Int)
          release <- newEmptyMVar
          let key = newWorkflowKey "slow"
              startWid = "sim-join-start"
              startOpts = startOptionsDefault {startWorkflowId = Just startWid}
              body () _ = do
                atomically (modifyTVar entered (+ 1))
                takeMVar release
                pure (Right 7)
          ref <- registerUnitRef dbos key body
          memLaunchOn mem simTracerSay dbos
          first <- startWfRefSim dbos ref startOpts Nothing
          second <- case first of
            Left err -> throwIO (userError (show err))
            Right firstHandle -> do
              joined <- startWfRefSim dbos ref startOpts Nothing
              case joined of
                Left err -> throwIO (userError (show err))
                Right secondHandle -> pure (firstHandle, secondHandle)
          putMVar release ()
          firstResult <- resultWfSim (fst second)
          secondResult <- resultWfSim (snd second)
          count <- readTVarIO entered
          row <- getWorkflow mem (WorkflowId startWid)
          pure (firstResult, secondResult, count, row)
        printSimTrace tr
        case outcome of
          (firstResult, secondResult, count, row) -> do
            case (firstResult, secondResult) of
              (Right (Just firstStored), Right (Just secondStored)) -> do
                let firstDecoded = decodeWorkflowValue "result" (Just firstStored) :: Either CodecError Int
                    secondDecoded = decodeWorkflowValue "result" (Just secondStored) :: Either CodecError Int
                assertEqual "the first caller reads the run" (Right 7) firstDecoded
                assertEqual "the joining caller reads the same run" (Right 7) secondDecoded
              other -> fail ("expected both handles to resolve: " <> show other)
            count @?= 1
            case row of
              Right (Just found) -> found.workflowRecordStatus @?= Success
              other -> fail ("expected exactly one successful row: " <> show other),
      testCase "a fresh start is local and a join polls" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          release <- newEmptyMVar
          let key = newWorkflowKey "quick"
              workflowText = "sim-local-id"
          ref <-
            registerUnitRef
              dbos
              key
              (\() _ -> takeMVar release >> pure (Right 7))
          memLaunchOn mem simTracerSay dbos
          let label (WorkflowHandle _ _ provenance') = case provenance' of
                Local _ -> "local"
                Polling {} -> "polling"
          firstStarted <- startWfRefSim dbos ref (startOptionsDefault {startWorkflowId = Just workflowText}) Nothing
          firstHandle <- orFail firstStarted
          let firstLabel = label firstHandle
          joined <- startWfRefSim dbos ref (startOptionsDefault {startWorkflowId = Just workflowText}) Nothing
          retrieved <- retrieveWfSim dbos (WorkflowId workflowText)
          let joinLabel = either (const "error") label joined
              retrieveLabel = either (const "error") label retrieved
          putMVar release ()
          result <- resultWfSim firstHandle
          pure (firstLabel, joinLabel, retrieveLabel, result)
        printSimTrace tr
        case outcome of
          (firstLabel, joinLabel, retrieveLabel, result) -> do
            firstLabel @?= "local"
            joinLabel @?= "polling"
            retrieveLabel @?= "polling"
            case result of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "the local handle reads the task's outcome" (Right 7) decoded
              other -> fail ("expected the local await to resolve, got: " <> show other),
      testCase "awaiting a child is recorded as a step" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "parent"
              parentText = "sim-await-parent"
              childText = parentText <> "-0"
              childBody :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody () _ = pure (Right 99)
          childRef <- registerUnitRef dbos childKey childBody
          let parentBody () ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWfSim ctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          orFail =<< registerWfSim dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos parentKey (WorkflowId parentText) Nothing
          listed <- SystemDB.listWorkflowSteps mem (WorkflowId parentText) True Nothing Nothing Nothing
          pure (ran, listed, childText)
        printSimTrace tr
        case outcome of
          (ran, listed, childIdText) -> do
            case ran of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "the parent reads the awaited child" (Right 99) decoded
              other -> fail (show other)
            case listed of
              Right
                [ StepRecord {stepRecordStepName = startName, stepRecordChildWorkflowId = Just (WorkflowId startedChild)},
                  StepRecord
                    { stepRecordStepName = awaitName,
                      stepRecordOutput = Just awaitOutput,
                      stepRecordChildWorkflowId = Just (WorkflowId awaitedChild)
                    }
                  ] -> do
                  startName @?= "child"
                  startedChild @?= childIdText
                  awaitName @?= "DBOS.getResult"
                  awaitOutput @?= "99"
                  awaitedChild @?= childIdText
              other -> fail ("expected the start and the recorded await, got: " <> show other),
      testCase "a recorded await of another workflow is refused" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "parent"
              parentText = "sim-await-wrong"
              childText = parentText <> "-0"
              childBody :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody () _ = pure (Right 1)
          childRef <- registerUnitRef dbos childKey childBody
          let parentBody () ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWfSim ctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          orFail =<< registerWfSim dbos parentKey parentBody
          -- An await recorded at the position this parent is about to reach,
          -- naming a workflow that is not the one it holds a handle to.
          orFailSys
            =<< SystemDB.recordChildResult
              mem
              (WorkflowId parentText)
              1
              (WorkflowId "somebody-elses-workflow")
              (OutcomeOutput (Just "7"))
              Nothing
              Nothing
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos parentKey (WorkflowId parentText) Nothing
          pure (ran, childText)
        printSimTrace tr
        case outcome of
          (ran, childIdText) -> do
            case ran of
              Left (ErrorSystemDatabase (SystemDB.UnexpectedStep {stepId, expected, recorded})) -> do
                stepId @?= 1
                assertBool ("says which workflow it was awaiting in " <> Text.unpack expected) (childIdText `Text.isInfixOf` expected)
                assertBool ("and whose outcome it found in " <> Text.unpack recorded) ("somebody-elses-workflow" `Text.isInfixOf` recorded)
              other -> fail ("expected the stale-await refusal, got: " <> show other),
      testCase "awaiting a child inside a step is covered by that step" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "parent"
              parentText = "sim-await-step-parent"
              childText = parentText <> "-0"
              childBody :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody () _ = pure (Right 41)
          childRef <- registerUnitRef dbos childKey childBody
          let parentBody () ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle ->
                    runWorkflowStepWith stepOptionsDefault ctx "collect" $ \inner -> do
                      awaited <- awaitWfSim inner wfHandle
                      pure $ case awaited of
                        Left err -> Left err
                        Right (Just stored) ->
                          case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                            Right value -> Right value
                            Left err -> Left (StepFailed "collect" (Text.pack (show err)))
                        Right Nothing -> Left (StepFailed "collect" "no child output")
          orFail =<< registerWfSim dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos parentKey (WorkflowId parentText) Nothing
          listed <- SystemDB.listWorkflowSteps mem (WorkflowId parentText) True Nothing Nothing Nothing
          pure (ran, listed, childText)
        printSimTrace tr
        case outcome of
          (ran, listed, childIdText) -> do
            case ran of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "the enclosing step carries the child's value" (Right 41) decoded
              other -> fail (show other)
            case listed of
              Right
                [ StepRecord {stepRecordStepName = startName, stepRecordChildWorkflowId = Just (WorkflowId startedChild)},
                  StepRecord {stepRecordStepName = collectName, stepRecordOutput = Just collectOutput}
                  ] -> do
                  startName @?= "child"
                  startedChild @?= childIdText
                  collectName @?= "collect"
                  collectOutput @?= "41"
              other -> fail ("expected the start and the enclosing step only, got: " <> show other),
      testCase "child starts and awaits keep their ids in build order" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "parent"
              parentText = "sim-order-parent"
              childBody :: Int -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody n _ = pure (Right n)
          childRef <- registerIntRef dbos childKey childBody
          let parentBody () ctx = do
                started <- mapM (\n -> startChildWorkflow ctx childRef startOptionsDefault (Just (encodeWorkflowValue (n :: Int)))) [1, 2, 3]
                case sequence started of
                  Left err -> pure (Left err)
                  Right handles -> do
                    awaited <- mapM (awaitWfSim ctx) handles
                    case sequence awaited of
                      Left err -> pure (Left err)
                      Right outputs -> case mapM (decodeWorkflowValue "result") outputs of
                        Left _ -> pure (Left (StepFailed "parent" "bad child output"))
                        Right (numbers :: [Int]) -> pure (Right (sum numbers))
          orFail =<< registerWfSim dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos parentKey (WorkflowId parentText) Nothing
          listed <- SystemDB.listWorkflowSteps mem (WorkflowId parentText) False Nothing Nothing Nothing
          firstChild <- getWorkflow mem (WorkflowId (parentText <> "-0"))
          pure (ran, listed, firstChild)
        printSimTrace tr
        case outcome of
          (ran, listed, firstChild) -> do
            case ran of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "1 + 2 + 3" (Right 6) decoded
              other -> fail (show other)
            case listed of
              Right rows -> do
                let table = map (\row -> (row.stepRecordStepId, row.stepRecordStepName, row.stepRecordChildWorkflowId)) rows
                table
                  @?= [ (0, "child", Just (WorkflowId "sim-order-parent-0")),
                        (1, "child", Just (WorkflowId "sim-order-parent-1")),
                        (2, "child", Just (WorkflowId "sim-order-parent-2")),
                        (3, "DBOS.getResult", Just (WorkflowId "sim-order-parent-0")),
                        (4, "DBOS.getResult", Just (WorkflowId "sim-order-parent-1")),
                        (5, "DBOS.getResult", Just (WorkflowId "sim-order-parent-2"))
                      ]
              other -> fail ("expected the three starts and their awaits, got: " <> show other)
            case firstChild of
              Right (Just row) -> row.workflowRecordOutput @?= Just "1"
              other -> fail ("expected the first-built child, got: " <> show other),
      testCase "runs claim their pairs of step ids adjacently" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "parent"
              parentText = "sim-pairs-parent"
              childBody :: Int -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody n _ = pure (Right n)
          childRef <- registerIntRef dbos childKey childBody
          let parentBody () ctx = do
                let pair n = do
                      startedPair <- startChildWorkflow ctx childRef startOptionsDefault (Just (encodeWorkflowValue (n :: Int)))
                      case startedPair of
                        Left err -> pure (Left err)
                        Right wfHandle -> do
                          awaited <- awaitWfSim ctx wfHandle
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
          orFail =<< registerWfSim dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos parentKey (WorkflowId parentText) Nothing
          listed <- SystemDB.listWorkflowSteps mem (WorkflowId parentText) False Nothing Nothing Nothing
          pure (ran, listed)
        printSimTrace tr
        case outcome of
          (ran, listed) -> do
            case ran of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "1 + 2 + 3" (Right 6) decoded
              other -> fail (show other)
            case listed of
              Right rows -> do
                let table = map (\row -> (row.stepRecordStepId, row.stepRecordStepName, row.stepRecordChildWorkflowId)) rows
                table
                  @?= [ (0, "child", Just (WorkflowId "sim-pairs-parent-0")),
                        (1, "DBOS.getResult", Just (WorkflowId "sim-pairs-parent-0")),
                        (2, "child", Just (WorkflowId "sim-pairs-parent-2")),
                        (3, "DBOS.getResult", Just (WorkflowId "sim-pairs-parent-2")),
                        (4, "child", Just (WorkflowId "sim-pairs-parent-4")),
                        (5, "DBOS.getResult", Just (WorkflowId "sim-pairs-parent-4"))
                      ]
              other -> fail ("expected each await behind its own start, got: " <> show other),
      testCase "a select step races a step against a child's result" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "parent"
              parentText = "sim-race-parent"
              childText = parentText <> "-0"
              childBody :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody () _ = pure (Right 7)
          childRef <- registerUnitRef dbos childKey childBody
          let parentBody () ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right childHandle -> do
                    slow <- pendingWorkflowStepWith stepOptionsDefault ctx "slow" (\_ -> threadDelay 30000000 >> pure (Right (0 :: Int)))
                    awaited <- pendingAwait ctx childHandle
                    selectStep
                      ctx
                      [ SelectArm "slow" slow (\slowOutcome -> pure (slowOutcome >>= \value -> Right value)),
                        SelectArm "DBOS.getResult" awaited $ \awaitOutcome ->
                          pure $ case awaitOutcome of
                            Left err -> Left err
                            Right (Just stored) ->
                              case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                                Right value -> Right value
                                Left _ -> Left (StepFailed "parent" "bad child output")
                            Right Nothing -> Left (StepFailed "parent" "no child output")
                      ]
          orFail =<< registerWfSim dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos parentKey (WorkflowId parentText) Nothing
          listed <- SystemDB.listWorkflowSteps mem (WorkflowId parentText) False Nothing Nothing Nothing
          pure (ran, listed)
        printSimTrace tr
        case outcome of
          (ran, listed) -> do
            case ran of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "the await won and its arm produced the answer" (Right 7) decoded
              other -> fail ("expected the race's winner, got: " <> show other)
            case listed of
              Right rows -> do
                let table = map (\row -> (row.stepRecordStepId, row.stepRecordStepName, row.stepRecordChildWorkflowId)) rows
                table
                  @?= [ (0, "child", Just (WorkflowId "sim-race-parent-0")),
                        (2, "DBOS.getResult", Just (WorkflowId "sim-race-parent-0")),
                        (3, "DBOS.selectStep", Nothing)
                      ]
              other -> fail ("expected the start, the await and the select, got: " <> show other),
      testCase "a control signal winning a select records no winner" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let parentKey = newWorkflowKey "parent"
              parentText = "sim-race-control-parent"
              parentBody () ctx = do
                interrupted <- pendingWorkflowStepWith stepOptionsDefault ctx "interrupted" (\_ -> pure (Left (Interrupted {workflowId = parentText})))
                slow <- pendingWorkflowStepWith stepOptionsDefault ctx "slow" (\_ -> threadDelay 30000000 >> pure (Right (1 :: Int)))
                selectStep
                  ctx
                  [ SelectArm "interrupted" interrupted (\armOutcome -> pure (armOutcome >>= \value -> Right value)),
                    SelectArm "slow" slow (\armOutcome -> pure (armOutcome >>= \value -> Right value))
                  ]
          orFail =<< registerWfSim dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos parentKey (WorkflowId parentText) Nothing
          listed <- SystemDB.listWorkflowSteps mem (WorkflowId parentText) False Nothing Nothing Nothing
          parentRow <- getWorkflow mem (WorkflowId parentText)
          pure (ran, listed, parentRow, parentText)
        printSimTrace tr
        case outcome of
          (ran, listed, parentRow, parentIdText) -> do
            case ran of
              Left (Interrupted {workflowId}) -> workflowId @?= parentIdText
              other -> fail ("expected the control signal back, got: " <> show other)
            listed @?= Right []
            case parentRow of
              Right (Just row) -> row.workflowRecordStatus @?= Pending
              other -> fail ("expected the parent row PENDING, got: " <> show other),
      testCase "a losing step has its cancellation token fired" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          released <- newEmptyMVar
          watching <- newEmptyMVar
          let parentKey = newWorkflowKey "parent"
              parentText = "sim-race-token-parent"
              parentBody () ctx = do
                slow <- pendingWorkflowStepWith stepOptionsDefault ctx "slow" $ \inner -> do
                  token <- cancellationToken inner
                  _ <- async $ do
                    let watch = do
                          cancelled <- tokenCancelled token
                          if cancelled then pure () else threadDelay 1000 >> watch
                    watch
                    putMVar released ()
                  putMVar watching ()
                  threadDelay 30000000
                  pure (Right (2 :: Int))
                fast <- pendingWorkflowStepWith stepOptionsDefault ctx "fast" (\_ -> takeMVar watching >> pure (Right (1 :: Int)))
                selectStep
                  ctx
                  [ SelectArm "slow" slow (\armOutcome -> pure (armOutcome >>= \value -> Right value)),
                    SelectArm "fast" fast (\armOutcome -> pure (armOutcome >>= \value -> Right value))
                  ]
          orFail =<< registerWfSim dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos parentKey (WorkflowId parentText) Nothing
          fired <- timeout 15000000 (takeMVar released)
          pure (ran, fired)
        printSimTrace tr
        case outcome of
          (ran, fired) -> do
            case ran of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "the fast step won" (Right 1) decoded
              other -> fail ("expected the fast step's value, got: " <> show other)
            case fired of
              Just _ -> pure ()
              Nothing -> fail "the losing step's token never fired",
      testCase "a cancelled child is an awaited cancellation in the parent" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "parent"
              parentText = "sim-awaited-cancel-parent"
              childText = parentText <> "-0"
              childBody :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody () _ = threadDelay 30000000 >> pure (Right 1)
          childRef <- registerUnitRef dbos childKey childBody
          let parentBody () ctx = do
                started <- startChildWorkflow ctx childRef (startOptionsDefault {startTimeout = Explicit (millisDuration 300)}) Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWfSim ctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          orFail =<< registerWfSim dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos parentKey (WorkflowId parentText) Nothing
          listed <- SystemDB.listWorkflowSteps mem (WorkflowId parentText) True Nothing Nothing Nothing
          parentRow <- getWorkflow mem (WorkflowId parentText)
          childRow <- getWorkflow mem (WorkflowId childText)
          pure (ran, listed, parentRow, childRow, childText)
        printSimTrace tr
        case outcome of
          (ran, listed, parentRow, childRow, childIdText) -> do
            case ran of
              Left (AwaitedWorkflowCancelled {workflowId}) -> workflowId @?= childIdText
              other -> fail ("expected an awaited cancellation, got: " <> show other)
            case listed of
              Right rows -> case [row | row <- rows, row.stepRecordStepName == "DBOS.getResult"] of
                [awaitRow] -> case awaitRow.stepRecordError of
                  Just recorded ->
                    case decodeErrorText recorded :: Either Text (Error EngineOnly) of
                      Right (AwaitedWorkflowCancelled {workflowId}) -> workflowId @?= childIdText
                      other -> fail ("expected a recorded awaited cancellation, got: " <> show other)
                  Nothing -> fail "the await recorded no error"
                other -> fail ("expected one recorded await, got: " <> show other)
              other -> fail ("expected the parent's steps, got: " <> show other)
            case parentRow of
              Right (Just row) -> row.workflowRecordStatus @?= Error
              other -> fail ("expected the failed parent row, got: " <> show other)
            case childRow of
              Right (Just row) -> row.workflowRecordStatus @?= Cancelled
              other -> fail ("expected the cancelled child row, got: " <> show other),
      testCase "a child inherits its parent's deadline" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "parent"
              parentText = "sim-inherit-deadline-parent"
              childText = parentText <> "-0"
              childBody :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody () _ = pure (Right 1)
          childRef <- registerUnitRef dbos childKey childBody
          let parentBody () ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWfSim ctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          parentRef <- registerUnitRef dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          ran <- runWfRefSim dbos parentRef (runOptionsDefault {runWorkflowId = Just parentText, runTimeout = Explicit (secondsDuration 300)}) Nothing
          parentRow <- getWorkflow mem (WorkflowId parentText)
          childRow <- getWorkflow mem (WorkflowId childText)
          pure (ran, parentRow, childRow)
        printSimTrace tr
        case outcome of
          (ran, parentRow, childRow) -> do
            case ran of
              Right _ -> pure ()
              other -> fail ("expected the parent to run, got: " <> show other)
            case (parentRow, childRow) of
              (Right (Just parent), Right (Just child)) -> do
                parentDeadline <- case parent.workflowRecordDeadline of
                  Just deadline' -> pure deadline'
                  Nothing -> fail "the parent has no deadline"
                assertEqual "the same instant, not a fresh budget" (Just parentDeadline) child.workflowRecordDeadline
                child.workflowRecordTimeout @?= Nothing
                parent.workflowRecordTimeout @?= Just (secondsDuration 300)
              other -> fail ("expected both rows, got: " <> show other),
      testCase "a child's own timeout replaces the inherited deadline" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "parent"
              parentText = "sim-child-budget-parent"
              childText = parentText <> "-0"
              childBody :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody () _ = pure (Right 1)
          childRef <- registerUnitRef dbos childKey childBody
          let parentBody () ctx = do
                started <- startChildWorkflow ctx childRef (startOptionsDefault {startTimeout = Explicit (secondsDuration 3600)}) Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWfSim ctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          parentRef <- registerUnitRef dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          ran <- runWfRefSim dbos parentRef (runOptionsDefault {runWorkflowId = Just parentText, runTimeout = Explicit (secondsDuration 60)}) Nothing
          parentRow <- getWorkflow mem (WorkflowId parentText)
          childRow <- getWorkflow mem (WorkflowId childText)
          pure (ran, parentRow, childRow)
        printSimTrace tr
        case outcome of
          (ran, parentRow, childRow) -> do
            case ran of
              Right _ -> pure ()
              other -> fail ("expected the parent to run, got: " <> show other)
            case (parentRow, childRow) of
              (Right (Just parent), Right (Just child)) -> do
                parentDeadline <- case parent.workflowRecordDeadline of
                  Just deadline' -> pure deadline'
                  Nothing -> fail "the parent has no deadline"
                childDeadline <- case child.workflowRecordDeadline of
                  Just deadline' -> pure deadline'
                  Nothing -> fail "the child has no deadline"
                assertBool "the child's own timeout won: it outlives its parent" (SystemDB.timestampToEpochMs childDeadline > SystemDB.timestampToEpochMs parentDeadline)
                child.workflowRecordTimeout @?= Just (secondsDuration 3600)
              other -> fail ("expected both rows, got: " <> show other),
      testCase "a child can decline the inherited deadline" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "parent"
              parentText = "sim-decline-deadline-parent"
              childBody :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody () _ = pure (Right 1)
          childRef <- registerUnitRef dbos childKey childBody
          let parentBody () ctx = do
                let childPair opts = do
                      started <- startChildWorkflow ctx childRef opts Nothing
                      case started of
                        Left err -> pure (Left err)
                        Right wfHandle -> do
                          awaited <- awaitWfSim ctx wfHandle
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
          parentRef <- registerUnitRef dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          ran <- runWfRefSim dbos parentRef (runOptionsDefault {runWorkflowId = Just parentText, runTimeout = Explicit (secondsDuration 300)}) Nothing
          parentRow <- getWorkflow mem (WorkflowId parentText)
          inheritedRow <- getWorkflow mem (WorkflowId (parentText <> "-0"))
          detachedRow <- getWorkflow mem (WorkflowId (parentText <> "-2"))
          pure (ran, parentRow, inheritedRow, detachedRow)
        printSimTrace tr
        case outcome of
          (ran, parentRow, inheritedRow, detachedRow) -> do
            case ran of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "both children ran" (Right 2) decoded
              other -> fail ("expected the parent's output, got: " <> show other)
            case (parentRow, inheritedRow, detachedRow) of
              (Right (Just parent), Right (Just inheritedChild), Right (Just detachedChild)) -> do
                parentDeadline <- case parent.workflowRecordDeadline of
                  Just deadline' -> pure deadline'
                  Nothing -> fail "the parent has no deadline"
                assertEqual "silence inherits the parent's instant verbatim" (Just parentDeadline) inheritedChild.workflowRecordDeadline
                detachedChild.workflowRecordDeadline @?= Nothing
                detachedChild.workflowRecordTimeout @?= Nothing
              other -> fail ("expected all three rows, got: " <> show other),
      testCase "a parent and its child hit an inherited deadline independently" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "parent"
              parentText = "sim-cascade-deadline-parent"
              childText = parentText <> "-0"
              childBody :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody () _ = threadDelay 30000000 >> pure (Right 1)
          childRef <- registerUnitRef dbos childKey childBody
          let parentBody () ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWfSim ctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          parentRef <- registerUnitRef dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          ran <- runWfRefSim dbos parentRef (runOptionsDefault {runWorkflowId = Just parentText, runTimeout = Explicit (millisDuration 400)}) Nothing
          childSettled <- waitForWorkflow dbos (WorkflowId childText)
          listed <- SystemDB.listWorkflowSteps mem (WorkflowId parentText) False Nothing Nothing Nothing
          pure (ran, childSettled, listed)
        printSimTrace tr
        case outcome of
          (ran, childSettled, listed) -> do
            case ran of
              Left (ErrorSystemDatabase (SystemDB.WorkflowCancelled {})) -> pure ()
              other -> fail ("expected the parent's own deadline cancellation, got: " <> show other)
            case childSettled of
              Right AwaitedCancelled -> pure ()
              other -> fail ("expected the child cancelled independently, got: " <> show other)
            case listed of
              Right [StepRecord {stepRecordStepName = name}] -> name @?= "child"
              other -> fail ("expected the lone start only, got: " <> show other),
      testCase "a parent starts a child under a derived id and replay adopts it" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          childRan <- newTVarIO (0 :: Int)
          let childKey = newWorkflowKey "double"
              parentKey = newWorkflowKey "parent"
              parentText = "sim-child-parent"
              childBody value ctx = do
                atomically (modifyTVar childRan (+ 1))
                runWorkflowStep ctx "double" (const (pure (value * 2)))
          childRef <- registerIntRef dbos childKey childBody
          let parentBody (_ :: Int) ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault (Just (encodeWorkflowValue (21 :: Int)))
                pure (handleWorkflowId <$> started)
          orFail =<< registerWfSim dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          first <- runWfSim dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int)))
          childId <- case first of
            Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Text of
              Right cid -> pure cid
              Left err -> throwIO (userError (show err))
            other -> throwIO (userError (show other))
          -- The child runs through its own call, then a handle adopts it.
          childRanNow <- runWfSim dbos childKey (WorkflowId childId) (Just (encodeWorkflowValue (21 :: Int)))
          retrieved <- retrieveWfSim dbos (WorkflowId childId)
          result <- case retrieved of
            Left err -> throwIO (userError (show err))
            Right handle -> resultWfSim handle
          runs <- readTVarIO childRan
          -- A second run of the parent adopts the recorded child id,
          -- starting nothing new.
          replayed <- runWfSim dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int)))
          runsAfter <- readTVarIO childRan
          pure (childId, childRanNow, result, runs, replayed, runsAfter)
        printSimTrace tr
        case outcome of
          (childId, childRanNow, result, runs, replayed, runsAfter) -> do
            childId @?= "sim-child-parent-0"
            case childRanNow of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "the child records its result" (Right 42) decoded
              other -> fail (show other)
            case result of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "a handle adopts the outcome" (Right 42) decoded
              other -> fail (show other)
            runs @?= 1
            case replayed of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Text
                assertEqual "replay adopts the recorded child, starting none" (Right childId) decoded
              other -> fail (show other)
            runsAfter @?= 1,
      testCase "starting a child inside a step is refused, not recorded" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "double"
              parentKey = newWorkflowKey "badparent"
              parentText = "sim-childleaf-parent"
              childBody :: Int -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
          childRef <- registerIntRef dbos childKey childBody
          let badBody (_ :: Int) ctx = do
                marker <- nextStepMarker ctx
                outcome <- withAttempt ctx marker (firstStepStatus 0) (\inner -> startChildWorkflow inner childRef startOptionsDefault Nothing)
                pure (case outcome of
                  Left err -> Left err
                  Right handle -> Left (ErrorConfig ("started inside a step: " <> handleWorkflowId handle)) :: Either (Error EngineOnly) Text)
          orFail =<< registerWfSim dbos parentKey badBody
          memLaunchOn mem simTracerSay dbos
          result <- runWfSim dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int)))
          listed <- SystemDB.listWorkflowSteps mem (WorkflowId parentText) False Nothing Nothing Nothing
          pure (result, listed)
        printSimTrace tr
        case outcome of
          (result, listed) -> do
            case result of
              Left (InsideStep operation) -> operation @?= "starting a workflow"
              other -> fail ("expected the leaf refusal, got: " <> show other)
            case listed of
              Right [] -> pure ()
              other -> fail ("expected no recorded start, got: " <> show other),
      testCase "a child that fails differently is started through lift" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let shipKey = newWorkflowKey "ship"
              billKey = newWorkflowKey "bill"
              billText = "sim-lift-parent"
              shipText = billText <> "-0"
              shipBody :: () -> Ctx (IOSim s) -> IOSim s (Either (Error Refused) ())
              shipBody () _ = pure (Left (application Refused))
          shipRef <- orFail =<< registerRefOf @Refused dbos shipKey shipBody
          let billBody () ctx = do
                started <- startChildWorkflow ctx shipRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right handle -> do
                    awaited <- awaitChild ctx handle
                    let refusedChild = case awaited of
                          Left (Application Refused) -> True
                          _ -> False
                    marker <- nextStepMarker ctx
                    refusedStart <- withAttempt ctx marker (firstStepStatus 2) $ \inner -> do
                      inside <- startChildWorkflow inner shipRef startOptionsDefault Nothing
                      pure (case inside of
                        Left err -> Left err
                        Right _ -> Right ())
                    pure $ case refusedStart of
                      Left (InsideStep _) -> Right refusedChild
                      Left err -> Left err
                      Right () -> Right False
          billRef <- orFail =<< registerRefOf @GaveUp dbos billKey billBody
          memLaunchOn mem simTracerSay dbos
          ran <- runDBOSWorkflowRef dbos billRef (runOptionsDefault {runWorkflowId = Just billText}) (Just (encodeWorkflowValue ()))
          childRow <- getWorkflow mem (WorkflowId shipText)
          pure (ran, childRow)
        printSimTrace tr
        case outcome of
          (ran, childRow) -> do
            case ran of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Bool
                decoded @?= Right True
              other -> fail ("expected the parent to report the child's refusal, got: " <> show other)
            case childRow of
              Right (Just row) -> do
                row.workflowRecordStatus @?= Error
                case row.workflowRecordError of
                  Just recorded -> case decodeErrorText recorded :: Either Text (Error Refused) of
                    Right (Application Refused) -> pure ()
                    other -> fail ("expected the child's own error in the column, got: " <> show other)
                  Nothing -> fail "the child recorded no error"
              other -> fail ("expected the child row, got: " <> show other),
      testCase "a child started and never awaited is still recorded" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "forgetful"
              parentText = "sim-unawaited-parent"
              childText = parentText <> "-0"
              childBody :: Int -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
          childRef <- registerIntRef dbos childKey childBody
          let parentBody (_ :: Int) ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault (Just (encodeWorkflowValue (21 :: Int)))
                case started of
                  Left err -> pure (Left err)
                  Right _ -> pure (Right ())
          orFail =<< registerWfSim dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int)))
          listed <- SystemDB.listWorkflowSteps mem (WorkflowId parentText) False Nothing Nothing Nothing
          -- The child outlives the parent's interest: the start detached
          -- it, so the parent returning does not stop it.
          found <- waitForWorkflow dbos (WorkflowId childText)
          childRow <- getWorkflow mem (WorkflowId childText)
          children <- SystemDB.getWorkflowChildren mem (WorkflowId parentText)
          pure (ran, listed, found, childRow, children)
        printSimTrace tr
        case outcome of
          (ran, listed, found, childRow, children) -> do
            case ran of
              Right _ -> pure ()
              other -> fail ("expected the parent to run, got: " <> show other)
            case listed of
              Right [StepRecord {stepRecordChildWorkflowId = Just (WorkflowId recorded)}] ->
                recorded @?= "sim-unawaited-parent-0"
              other -> fail ("expected the lone start step, got: " <> show other)
            case found of
              Right (AwaitedSucceeded (Just output) serialization) -> do
                let stored = SerializedWorkflowValue output (Serialization <$> serialization)
                    decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "the abandoned child still records its result" (Right 42) decoded
              other -> fail ("expected the abandoned child to finish, got: " <> show other)
            case childRow of
              Right (Just row) -> row.workflowRecordParentWorkflowId @?= Just (WorkflowId "sim-unawaited-parent")
              other -> fail ("expected the child row, got: " <> show other)
            children @?= Right [WorkflowId "sim-unawaited-parent-0"],
      testCase "children started in a loop run concurrently" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "fan"
              parentText = "sim-fanout-parent"
              childBody :: Int -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody n _ = threadDelay 1000 >> pure (Right n)
          childRef <- registerIntRef dbos childKey childBody
          let parentBody () ctx = do
                started <- mapM (\n -> startChildWorkflow ctx childRef startOptionsDefault (Just (encodeWorkflowValue (n :: Int)))) [0, 1, 2]
                case sequence started of
                  Left err -> pure (Left err)
                  Right handles -> do
                    results <- mapM (awaitWfSim ctx) handles
                    case sequence results of
                      Left err -> pure (Left err)
                      Right outputs -> case mapM (decodeWorkflowValue "result") outputs of
                        Left _ -> pure (Left (StepFailed "fan" "bad child output"))
                        Right (numbers :: [Int]) -> pure (Right (sum numbers))
          orFail =<< registerWfSim dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos parentKey (WorkflowId parentText) Nothing
          children <- SystemDB.getWorkflowChildren mem (WorkflowId parentText)
          pure (ran, children)
        printSimTrace tr
        -- No timing assert: virtual time settles instantly, so elapsed
        -- time cannot tell concurrent from serial here; the sum and the
        -- three children do.
        case outcome of
          (ran, children) -> do
            case ran of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "0 + 1 + 2" (Right 3) decoded
              other -> fail ("expected the fan-out total, got: " <> show other)
            case children of
              Right ids -> length ids @?= 3
              other -> fail ("expected three children, got: " <> show other),
      testCase "an assigned child id wins over the derived one" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "namer"
              parentText = "sim-assigned-parent"
              chosenText = "sim-assigned-chosen"
              derivedText = parentText <> "-0"
              childBody :: Int -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody _ _ = pure (Right 7)
          childRef <- registerIntRef dbos childKey childBody
          let parentBody (_ :: Int) ctx = do
                started <- startChildWorkflow ctx childRef (startOptionsDefault {startWorkflowId = Just chosenText}) (Just (encodeWorkflowValue (21 :: Int)))
                pure (handleWorkflowId <$> started)
          orFail =<< registerWfSim dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          first <- runWfSim dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int)))
          chosen <- getWorkflow mem (WorkflowId chosenText)
          derived <- getWorkflow mem (WorkflowId derivedText)
          childRan <- runWfSim dbos childKey (WorkflowId chosenText) (Just (encodeWorkflowValue (21 :: Int)))
          pure (first, chosen, derived, childRan)
        printSimTrace tr
        case outcome of
          (first, chosen, derived, childRan) -> do
            case first of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Text
                decoded @?= Right "sim-assigned-chosen"
              other -> fail (show other)
            case chosen of
              Right (Just row) -> row.workflowRecordParentWorkflowId @?= Just (WorkflowId "sim-assigned-parent")
              other -> fail ("expected the assigned row, got: " <> show other)
            derived @?= Right Nothing
            case childRan of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "the assigned child records its result" (Right 7) decoded
              other -> fail (show other),
      testCase "a workflow started outside a workflow has no parent" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let key = newWorkflowKey "root"
              workflowText = "sim-root-id"
              body :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              body () _ = pure (Right 1)
          orFail =<< registerWfSim dbos key body
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos key (WorkflowId workflowText) Nothing
          row <- getWorkflow mem (WorkflowId workflowText)
          pure (ran, row)
        printSimTrace tr
        case outcome of
          (ran, row) -> do
            case ran of
              Right _ -> pure ()
              other -> fail ("expected the workflow to run, got: " <> show other)
            case row of
              Right (Just found) -> found.workflowRecordParentWorkflowId @?= Nothing
              other -> fail ("expected the root row, got: " <> show other),
      testCase "a start position holding a plain step is refused" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "waiter"
              parentText = "sim-stale-parent"
              derivedText = parentText <> "-0"
              childBody :: Int -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody _ _ = pure (Right 1)
          childRef <- registerIntRef dbos childKey childBody
          let parentBody (_ :: Int) ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault (Just (encodeWorkflowValue (1 :: Int)))
                case started of
                  Left err -> pure (Left err)
                  Right _ -> pure (Right (0 :: Int))
          orFail =<< registerWfSim dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          -- A plain step planted at the start position before the body
          -- runs: the start finds output where a child link should be.
          orFailSys =<< SystemDB.recordStep mem (WorkflowId parentText) 0 "child" (SystemDB.OutcomeOutput (Just "1")) Nothing Nothing
          result <- runWfSim dbos parentKey (WorkflowId parentText) (Just (encodeWorkflowValue (0 :: Int)))
          missing <- getWorkflow mem (WorkflowId derivedText)
          pure (result, missing)
        printSimTrace tr
        case outcome of
          (result, missing) -> do
            case result of
              Left (ErrorSystemDatabase (SystemDB.UnexpectedStep {stepId, expected, recorded})) -> do
                stepId @?= 0
                assertBool "says what it wanted" ("child workflow start" `Text.isInfixOf` expected)
                assertBool "and what it found" ("plain step" `Text.isInfixOf` recorded)
              other -> fail ("expected the unexpected-step refusal, got: " <> show other)
            missing @?= Right Nothing,
      testCase "a child started through another instance is refused" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          other <- simInstance
          owner <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "parent"
              parentText = "sim-wrong-instance-parent"
              childText = parentText <> "-0"
              childBody :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody () _ = pure (Right 1)
          childRef <- registerUnitRef other childKey childBody
          let parentBody () ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right wfHandle -> do
                    awaited <- awaitWfSim ctx wfHandle
                    pure $ case awaited of
                      Left err -> Left err
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right value -> Right value
                          Left err -> Left (StepFailed "parent" (Text.pack (show err)))
                      Right Nothing -> Left (StepFailed "parent" "no child output")
          orFail =<< registerWfSim owner parentKey parentBody
          memLaunchOn mem simTracerSay other
          memLaunchOn mem simTracerSay owner
          ran <- runWfSim owner parentKey (WorkflowId parentText) Nothing
          missing <- getWorkflow mem (WorkflowId childText)
          listed <- SystemDB.listWorkflowSteps mem (WorkflowId parentText) False Nothing Nothing Nothing
          pure (ran, missing, listed)
        printSimTrace tr
        case outcome of
          (ran, missing, listed) -> do
            case ran of
              Left (WrongInstance {operation}) -> assertBool ("names the call in " <> Text.unpack operation) ("workflow" `Text.isInfixOf` operation)
              other -> fail ("expected a wrong-instance refusal, got: " <> show other)
            missing @?= Right Nothing
            listed @?= Right [],
      testCase "a child joining a held key is recorded as the workflow it joined" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "joiner"
              queueName = "sim-join-q"
              dedupKey = "order-42"
              holderText = "sim-join-holder"
              parentText = "sim-join-parent"
              derivedText = parentText <> "-0"
              joinQueue =
                (enqueueNew queueName)
                  { deduplication_id = Just dedupKey,
                    duplication_policy = ReturnExisting
                  }
              childBody :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody () _ = pure (Right 9)
          childRef <- registerUnitRef dbos childKey childBody
          let parentBody () ctx = do
                started <- startChildWorkflow ctx childRef (startOptionsDefault {startQueue = Just joinQueue}) Nothing
                case started of
                  Left err -> pure (Left err)
                  Right handle -> do
                    result <- awaitWfSim ctx handle
                    case result of
                      Left err -> pure (Left err)
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right n -> pure (Right n)
                          Left _ -> pure (Left (StepFailed "parent" "bad child output"))
                      Right _ -> pure (Left (StepFailed "parent" "no child output"))
          orFail =<< registerWfSim dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          -- The holder parks on its queue with the key held; nothing runs
          -- it here, so the test stages what the queue runner would do and
          -- records its completion directly. (Live parks it on a delay
          -- instead and the supervisor runs it; the asserted join is the
          -- same.)
          holderStarted <-
            startWfRefSim
              dbos
              childRef
              (startOptionsDefault {startWorkflowId = Just holderText, startQueue = Just (enqueueNew queueName) {deduplication_id = Just dedupKey}})
              Nothing
          case holderStarted of
            Left err -> throwIO (userError (show err))
            Right _ -> pure ()
          orFailSys =<< SystemDB.recordWorkflowOutcome mem (WorkflowId holderText) (OutcomeOutput (Just "9"))
          outcome <- runWfSim dbos parentKey (WorkflowId parentText) Nothing
          derived <- getWorkflow mem (WorkflowId derivedText)
          listed <- SystemDB.listWorkflowSteps mem (WorkflowId parentText) False Nothing Nothing Nothing
          children <- SystemDB.getWorkflowChildren mem (WorkflowId parentText)
          pure (outcome, derived, listed, children)
        printSimTrace tr
        case outcome of
          (outcome, derived, listed, children) -> do
            case outcome of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "the parent reads the joined workflow's output" (Right 9) decoded
              other -> fail ("expected the joined output, got: " <> show other)
            derived @?= Right Nothing
            case listed of
              Right
                [ StepRecord {stepRecordStepName = startName, stepRecordChildWorkflowId = Just (WorkflowId startedChild)},
                  StepRecord
                    { stepRecordStepName = awaitName,
                      stepRecordOutput = Just awaitOutput,
                      stepRecordChildWorkflowId = Just (WorkflowId awaitedChild)
                    }
                  ] -> do
                  startName @?= "child"
                  startedChild @?= "sim-join-holder"
                  awaitName @?= "DBOS.getResult"
                  awaitOutput @?= "9"
                  awaitedChild @?= "sim-join-holder"
              other -> fail ("expected the joining start and its recorded await, got: " <> show other)
            children @?= Right [],
      testCase "a zero-argument workflow records no input" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let key = newWorkflowKey "zero"
              workflowText = "sim-zero-id"
              body :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) ())
              body () _ = pure (Right ())
          orFail =<< registerWfSim dbos key body
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos key (WorkflowId workflowText) Nothing
          row <- SystemDB.getWorkflow mem (WorkflowId workflowText)
          pure (ran, row)
        printSimTrace tr
        case outcome of
          (ran, row) -> do
            case ran of
              Right _ -> pure ()
              other -> fail ("expected the workflow to run, got: " <> show other)
            case row of
              Right (Just record) -> record.workflowRecordInput @?= Nothing
              other -> fail (show other),
      testCase "the row exists before the body starts" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let key = newWorkflowKey "sees-itself"
              workflowText = "sim-row-id"
              body :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Bool)
              body () ctx = do
                row <- withSystemDB ctx (\db -> SystemDB.getWorkflow db (WorkflowId (workflowId ctx)))
                pure (Right (case row of Right (Just _) -> True; _ -> False))
          orFail =<< registerWfSim dbos key body
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos key (WorkflowId workflowText) Nothing
          pure ran
        printSimTrace tr
        case outcome of
          Right (Just stored) -> do
            let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Bool
            assertEqual "the body found its own row" (Right True) decoded
          other -> fail (show other),
      testCase "a panicking workflow leaves its row pending" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let key = newWorkflowKey "explodes"
              workflowText = "sim-panic-id"
              body :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) ())
              body () _ = throwIO (userError "boom")
          orFail =<< registerWfSim dbos key body
          memLaunchOn mem simTracerSay dbos
          outcome <- try (runWfSim dbos key (WorkflowId workflowText) Nothing)
          row <- getWorkflow mem (WorkflowId workflowText)
          pure (outcome, row)
        printSimTrace tr
        case outcome of
          (outcome, row) -> do
            case (outcome :: Either SomeException (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))) of
              Left _ -> pure ()
              Right other -> fail ("expected the body's exception to escape, got: " <> show other)
            case row of
              Right (Just found) -> do
                found.workflowRecordStatus @?= Pending
                found.workflowRecordError @?= Nothing
              other -> fail ("expected the row PENDING, got: " <> show other),
      testCase "running before launch is refused" $ do
        (ran, tr) <- runSimCase $ do
          dbos <- simInstance
          let key = newWorkflowKey "double"
              body :: Int -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              body value ctx = runWorkflowStep ctx "double" (const (pure (value * 2)))
          orFail =<< registerWfSim dbos key body
          runWfSim dbos key (WorkflowId "sim-unlaunched-id") (Just (encodeWorkflowValue (21 :: Int)))
        printSimTrace tr
        case ran of
          Left ErrorNotLaunched {} -> pure ()
          other -> fail ("expected a not-launched refusal, got: " <> show other),
      testCase "an application error round-trips as itself" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let key = newWorkflowKey "flaky"
              workflowText = "sim-app-err-id"
              body :: Int -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              body _ _ = pure (Left (StepFailed "flaky" "boom"))
          orFail =<< registerWfSim dbos key body
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          pure ran
        printSimTrace tr
        case outcome of
          Left (StepFailed step message) -> do
            step @?= "flaky"
            message @?= "boom"
          other -> fail ("expected the application error back, got: " <> show other),
      testCase "a database failure is not the workflow outcome" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let key = newWorkflowKey "blips"
              workflowText = "sim-blip-id"
              backendErr =
                SystemDB.Backend
                  ( SystemDB.BackendError
                      { SystemDB.backendMessage = "connection reset by peer",
                        SystemDB.backendSqlState = Nothing,
                        SystemDB.backendKind = SystemDB.Connection
                      }
                  )
              body :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) ())
              body () _ = pure (Left (ErrorSystemDatabase backendErr))
          orFail =<< registerWfSim dbos key body
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos key (WorkflowId workflowText) Nothing
          row <- getWorkflow mem (WorkflowId workflowText)
          pure (ran, row)
        printSimTrace tr
        case outcome of
          (ran, row) -> do
            case ran of
              Left (ErrorSystemDatabase _) -> pure ()
              other -> fail ("expected the database failure back, got: " <> show other)
            case row of
              Right (Just found) -> do
                found.workflowRecordStatus @?= Pending
                found.workflowRecordError @?= Nothing
              other -> fail ("expected the row left pending with no error, got: " <> show other),
      testCase "a workflow records the steps it took" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let key = newWorkflowKey "two-steps"
              workflowText = "sim-steps-listed-id"
              body :: Int -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              body value ctx = do
                first <- runWorkflowStep ctx "one" (const (pure (value + 1)))
                case first of
                  Left err -> pure (Left err)
                  Right stepped -> runWorkflowStep ctx "two" (const (pure (stepped * 2)))
          orFail =<< registerWfSim dbos key body
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos key (WorkflowId workflowText) (Just (encodeWorkflowValue (21 :: Int)))
          listed <- listWorkflowSteps mem (WorkflowId workflowText) False Nothing Nothing Nothing
          pure (ran, listed)
        printSimTrace tr
        case outcome of
          (ran, listed) -> do
            case ran of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "two steps compose" (Right 44) decoded
              other -> fail (show other)
            case listed of
              Right [StepRecord {stepRecordStepName = first}, StepRecord {stepRecordStepName = second}] ->
                [first, second] @?= ["one", "two"]
              other -> fail ("expected two steps in order, got: " <> show other),
      testCase "shutdown cancels a running workflow and leaves it pending" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          gate <- newEmptyMVar
          let key = newWorkflowKey "gated"
              workflowText = "sim-shutdown-run-id"
              body () _ = takeMVar gate >> pure (Right 7)
          ref <- registerUnitRef dbos key body
          memLaunchOn mem simTracerSay dbos
          worker <- forkIO (runWfRefSim dbos ref (runOptionsDefault {runWorkflowId = Just workflowText}) Nothing >> pure ())
          -- The row is written before the body is entered, so its
          -- presence means the run is gated, not merely started.
          status <- waitForRow dbos (WorkflowId workflowText)
          shutdown dbos
          killThread worker
          final <- getWorkflow mem (WorkflowId workflowText)
          pure (status, final)
        printSimTrace tr
        case outcome of
          (status, final) -> do
            status @?= Pending
            case final of
              Right (Just row) -> row.workflowRecordStatus @?= Pending
              other -> fail ("expected the row PENDING, got: " <> show other),
      testCase "dropping the future does not stop the workflow" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          gate <- newEmptyMVar
          let key = newWorkflowKey "gated"
              workflowText = "sim-drop-future-id"
              body () _ = takeMVar gate >> pure (Right 7)
          ref <- registerUnitRef dbos key body
          memLaunchOn mem simTracerSay dbos
          -- The start spawns the body and returns at once; dropping its
          -- handle stops nothing. A waiter is forked and killed to mirror
          -- the live cancel, then the run is awaited directly.
          _started <- startWfRefSim dbos ref (startOptionsDefault {startWorkflowId = Just workflowText}) Nothing
          waiter <- forkIO (waitForWorkflow dbos (WorkflowId workflowText) >> pure ())
          -- Dropping the waiter stops the watching, not the workflow: the
          -- run is detached onto the executor, so killing the waiter
          -- leaves the row pending and the body still gated.
          killThread waiter
          gated <- getWorkflow mem (WorkflowId workflowText)
          -- Released, the run finishes on its own — no recovery needed.
          putMVar gate ()
          settled <- waitForWorkflow dbos (WorkflowId workflowText)
          pure (gated, settled)
        printSimTrace tr
        case outcome of
          (gated, settled) -> do
            -- While gated, the row is pending: killing the waiter stopped
            -- the watching, not the workflow.
            case gated of
              Right (Just found) -> found.workflowRecordStatus @?= Pending
              other -> fail ("expected the gated row PENDING, got: " <> show other)
            case settled of
              Right (AwaitedSucceeded (Just output) serialization) -> do
                let stored = SerializedWorkflowValue output (Serialization <$> serialization)
                    decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
                assertEqual "the dropped run still records its result" (Right 7) decoded
              other -> fail ("expected the dropped run to finish, got: " <> show other),
      testCase "a budget cancels the workflow durably" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let key = newWorkflowKey "slow"
              workflowText = "sim-budget-id"
              body :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              body () _ = threadDelay 1000000 >> pure (Right 7)
          ref <- registerUnitRef dbos key body
          memLaunchOn mem simTracerSay dbos
          -- A millisecond budget against a second-long body: the clock
          -- wins on virtual time, deterministically.
          ran <-
            runWfRefSim
              dbos
              ref
              (runOptionsDefault {runWorkflowId = Just workflowText, runTimeout = Explicit (millisDuration 1)})
              Nothing
          row <- getWorkflow mem (WorkflowId workflowText)
          pure (ran, row)
        printSimTrace tr
        case outcome of
          (ran, row) -> do
            case ran of
              Left (ErrorSystemDatabase (SystemDB.WorkflowCancelled {})) -> pure ()
              other -> fail ("expected the durable cancellation, got: " <> show other)
            case row of
              Right (Just found) -> found.workflowRecordStatus @?= Cancelled
              other -> fail ("expected the row CANCELLED, got: " <> show other),
      testCase "a started workflow carries the attributes it was given" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let childKey = newWorkflowKey "child"
              parentKey = newWorkflowKey "attributed"
              parentText = "sim-attributes-parent"
              tenant = "acme-sim"
              childBody :: Int -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              childBody _ _ = pure (Right 9)
          childRef <- registerIntRef dbos childKey childBody
          let parentBody () ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault (Just (encodeWorkflowValue (0 :: Int)))
                case started of
                  Left err -> pure (Left err)
                  Right handle -> do
                    result <- awaitWfSim ctx handle
                    case result of
                      Left err -> pure (Left err)
                      Right (Just stored) ->
                        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right n -> pure (Right n)
                          Left _ -> pure (Left (StepFailed "parent" "bad child output"))
                      Right _ -> pure (Left (StepFailed "parent" "no child output"))
          parentRef <- registerUnitRef dbos parentKey parentBody
          memLaunchOn mem simTracerSay dbos
          ran <-
            runWfRefSim
              dbos
              parentRef
              (runOptionsDefault {runWorkflowId = Just parentText, runAttributes = Just (Map.singleton "tenant" (String tenant))})
              Nothing
          parentRow <- getWorkflow mem (WorkflowId parentText)
          childRow <- getWorkflow mem (WorkflowId (parentText <> "-0"))
          pure (ran, parentRow, childRow)
        printSimTrace tr
        case outcome of
          (ran, parentRow, childRow) -> do
            case ran of
              Right _ -> pure ()
              other -> fail ("expected the attributed run, got: " <> show other)
            case parentRow of
              Right (Just row) -> case row.workflowRecordAttributes of
                Just attributes -> assertBool ("expected the tenant in " <> Text.unpack attributes) ("acme-sim" `Text.isInfixOf` attributes)
                Nothing -> fail "the parent row carries no attributes"
              other -> fail ("expected the parent row, got: " <> show other)
            case childRow of
              Right (Just row) -> row.workflowRecordAttributes @?= Nothing
              other -> fail ("expected the child row, got: " <> show other),
      testCase "a step error is recorded in its column" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          dbos <- simInstance
          let key = newWorkflowKey "charger"
              workflowText = "sim-step-err-id"
              body :: () -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
              body () ctx = runWorkflowStepWith stepOptionsDefault ctx "charge" (const (pure (Left (StepFailed "charge" "short by 12"))))
          orFail =<< registerWfSim dbos key body
          memLaunchOn mem simTracerSay dbos
          ran <- runWfSim dbos key (WorkflowId workflowText) Nothing
          -- Payloads loaded: the flag gates output AND error together,
          -- as the oracle's @step_payloads@ does (the memory backend
          -- always returns full records).
          listed <- listWorkflowSteps mem (WorkflowId workflowText) True Nothing Nothing Nothing
          pure (ran, listed)
        printSimTrace tr
        case outcome of
          (ran, listed) -> do
            case ran of
              Left (StepFailed step message) -> do
                step @?= "charge"
                message @?= "short by 12"
              other -> fail ("expected the step error back, got: " <> show other)
            case listed of
              Right [StepRecord {stepRecordStepName = name, stepRecordError = Just recorded}] -> do
                name @?= "charge"
                assertBool ("expected the shortfall in " <> Text.unpack recorded) ("short by 12" `Text.isInfixOf` recorded)
              other -> fail ("expected the failed step, got: " <> show other),
      testCase "workflow announcements carry their counts and ids" $ do
        (_, tr) <- runSimCase demoTrace
        printSimTrace tr
        selectTraceEventsDynamic tr
          @?= [ WorkflowEnqueued "sim-wf-enqueued" "sim-queue",
                WorkflowAlreadyOwned "sim-wf-owned",
                WorkflowChildJoined "sim-parent" 0 "sim-child",
                WorkflowDedupJoined "sim-holder" "sim-key",
                WorkflowDeadlineRaced "sim-wf-raced",
                WorkflowOutcomeRecordFailed "sim-detail",
                WorkflowSuperseded "sim-wf-first",
                WorkflowControlEnded "sim-control"
              ]
    ]

-- | The announcement shapes no staged case reaches: a superseded write,
-- a raced deadline, and a failed outcome write need races and faults the
-- sim does not stage; the rest ride the cases above. Hand-emitted through
-- the say-carrier, asserted by type.
demoTrace :: forall s. IOSim s ()
demoTrace = do
  runTracer simTracerSay (WorkflowEnqueued "sim-wf-enqueued" "sim-queue")
  runTracer simTracerSay (WorkflowAlreadyOwned "sim-wf-owned")
  runTracer simTracerSay (WorkflowChildJoined "sim-parent" 0 "sim-child")
  runTracer simTracerSay (WorkflowDedupJoined "sim-holder" "sim-key")
  runTracer simTracerSay (WorkflowDeadlineRaced "sim-wf-raced")
  runTracer simTracerSay (WorkflowOutcomeRecordFailed "sim-detail")
  runTracer simTracerSay (WorkflowSuperseded "sim-wf-first")
  runTracer simTracerSay (WorkflowControlEnded "sim-control")

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through. Local
-- copies are deliberate: this module carries only the aliases it uses.
runWfSim :: DBOS (IOSim s) -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> IOSim s (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWfSim = runDBOSWorkflow

runWfRefSim :: DBOS (IOSim s) -> WorkflowRef (IOSim s) EngineOnly -> RunOptions -> Maybe SerializedWorkflowValue -> IOSim s (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWfRefSim = runDBOSWorkflowRef

startWfRefSim :: DBOS (IOSim s) -> WorkflowRef (IOSim s) EngineOnly -> StartOptions -> Maybe SerializedWorkflowValue -> IOSim s (Either (Error EngineOnly) (WorkflowHandle (IOSim s) EngineOnly))
startWfRefSim = startDBOSWorkflowRef

retrieveWfSim :: DBOS (IOSim s) -> WorkflowId -> IOSim s (Either (Error EngineOnly) (WorkflowHandle (IOSim s) EngineOnly))
retrieveWfSim = retrieveWorkflow

awaitWfSim :: Ctx (IOSim s) -> WorkflowHandle (IOSim s) EngineOnly -> IOSim s (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
awaitWfSim = awaitChild

resultWfSim :: WorkflowHandle (IOSim s) EngineOnly -> IOSim s (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWfSim = handleResult

statusWfSim :: WorkflowHandle (IOSim s) EngineOnly -> IOSim s (Either (Error EngineOnly) (Maybe WorkflowStatus))
statusWfSim = handleStatus

-- | The sim-side registration aliases: a locally defined body has no
-- signature, so the channel's @e@ stays ambiguous; these pin it while
-- leaving @s@ universally quantified.
registerWfSim :: (FromJSON a, ToJSON r) => DBOS (IOSim s) -> WorkflowKey -> (a -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) r)) -> IOSim s (Either (Error EngineOnly) ())
registerWfSim = registerDBOSWorkflow

registerWfRefSim :: (FromJSON a, ToJSON r) => DBOS (IOSim s) -> WorkflowKey -> (a -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) r)) -> IOSim s (Either (Error EngineOnly) (WorkflowRef (IOSim s) EngineOnly))
registerWfRefSim = registerDBOSWorkflowRef

-- * Helpers

-- | A started reader for inspecting a row's status: polls until the row
-- appears, so background starts are observed rather than raced. Virtual
-- time makes the settling instant.
waitForRow :: DBOS (IOSim s) -> WorkflowId -> IOSim s WorkflowStatus
waitForRow dbos wid = go (20 :: Int)
  where
    go 0 = throwIO (userError "the workflow row never appeared")
    go n = do
      retrieved <- retrieveWfSim dbos wid
      case retrieved of
        Left err -> throwIO (userError (show err))
        Right handle -> do
          status <- statusWfSim handle
          case status of
            Left err -> throwIO (userError (show err))
            Right (Just found) -> pure found
            Right Nothing -> threadDelay 1000 >> go (n - 1)

-- | Register a @() -> Int@ body under IOSim, pinning the JSON types the
-- polymorphic registration cannot infer from a local binding.
registerUnitRef :: DBOS (IOSim s) -> WorkflowKey -> (() -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)) -> IOSim s (WorkflowRef (IOSim s) EngineOnly)
registerUnitRef dbos key body = orFail =<< registerWfRefSim dbos key body

-- | Register an @Int -> Int@ body under IOSim, pinning the JSON types the
-- polymorphic registration cannot infer from a local binding.
registerIntRef :: DBOS (IOSim s) -> WorkflowKey -> (Int -> Ctx (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)) -> IOSim s (WorkflowRef (IOSim s) EngineOnly)
registerIntRef dbos key body = orFail =<< registerWfRefSim dbos key body

-- | Register a body at its own error channel, leaving @s@ and @e@ to the
-- call site: the polymorphic registration cannot infer them from a local
-- binding, and a locally written channel is the point of the lift case.
registerRefOf :: forall e s a r. (FromJSON a, ToJSON r, ToJSON e) => DBOS (IOSim s) -> WorkflowKey -> (a -> Ctx (IOSim s) -> IOSim s (Either (Error e) r)) -> IOSim s (Either (Error EngineOnly) (WorkflowRef (IOSim s) e))
registerRefOf = registerDBOSWorkflowRef

orFail :: Either (Error EngineOnly) a -> IOSim s a
orFail result = case result of
  Left err -> throwIO (userError (show err))
  Right value -> pure value

orFailSys :: Either SystemDB.Error a -> IOSim s a
orFailSys result = case result of
  Left err -> throwIO (userError (show err))
  Right value -> pure value

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
