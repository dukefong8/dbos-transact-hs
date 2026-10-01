{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Public workflow-step behavior against the configured live SystemDB.
module DBOS.Transact.StepTest (tests) where

import DBOS.Prelude
import Data.Aeson (FromJSON, ToJSON)
import Data.Text (Text)
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (NewWorkflow (..), Submission (..), SystemDB (..), millisDuration, newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  (
    EngineOnly,
    Ctx,
    Error (..),
    StepOptions (..),
    StepStatus (..),
    WorkflowId (..),
    acquireLoggerBackend,
    cancellationToken,
    ioTracer,
    nullTracer,
    runWorkflowStep,
    runWorkflowStepWith,
    sleepWorkflowStep,
    stepId,
    stepStatus,
    stepStatusCurrentAttempt,
    stepStatusId,
    stepStatusMaxAttempts,
    stepOptionsDefault,
  )
import DBOS.Transact.ContextTest (ctxOver)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase)

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    testGroup
      "Durable step"
      [ testCase "a recorded workflow step runs once and replays" $ do
          backend <- getBackend
          freshId <- UUID.V4.nextRandom
          let workflowText = "hs-l2-step-" <> Text.pack (UUID.toString freshId)
              initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2StepTest"}
          created <- initWorkflow backend initialWorkflow Nothing Fresh Nothing
          case created of
            Left err -> fail (show err)
            Right _ -> pure ()
          calls <- newIORef (0 :: Int)
          observedStepId <- newIORef Nothing
          -- Each execution gets a fresh context: a new counter and a new
          -- execution identity over the same workflow id, exactly as a
          -- recovered run does.
          firstContext <- ctxOver backend nullTracer workflowText
          let body :: Ctx IO -> IO Int
              body ctx = do
                writeIORef observedStepId (stepId ctx)
                modifyIORef' calls (+ 1)
                pure 42
          first <- runStep firstContext "test_step" body
          assertEqual "first execution returns the body's result" (Right 42) first
          assertEqual "the body runs inside step zero" (Just 0) =<< readIORef observedStepId
          replayContext <- ctxOver backend nullTracer workflowText
          second <- runStep replayContext "test_step" body
          assertEqual "replay returns the recorded result" (Right 42) second
          assertEqual "replay does not run the body again" 1 =<< readIORef calls,
      testCase "a step inside a step body runs plainly and takes no id" $ do
        backend <- getBackend
        freshId <- UUID.V4.nextRandom
        let workflowText = "hs-l2-step-nested-" <> Text.pack (UUID.toString freshId)
            initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2StepNestedTest"}
        created <- initWorkflow backend initialWorkflow Nothing Fresh Nothing
        case created of
          Left err -> fail (show err)
          Right _ -> pure ()
        -- The nested run announces through FastLogger, so the run proves
        -- the trace seam as well as the checkpoint it skips.
        (logger, cleanup) <- acquireLoggerBackend
        firstContext <- ctxOver backend (ioTracer logger) workflowText
        let innerBody :: Ctx IO -> IO Int
            innerBody _ = pure 7
            outerBody :: Ctx IO -> IO Int
            outerBody ctx = do
              inner <- runStep ctx "inner" innerBody
              case inner of
                Right n -> pure (n + 1)
                Left err -> fail (show err)
        outer <- runStep firstContext "outer" outerBody
        cleanup
        assertEqual "the outer body sees the inner result" (Right 8) outer
        placed <- checkStep backend (WorkflowId workflowText) 0 "outer"
        case placed of
          Right (Just _) -> pure ()
          _ -> fail "expected a checkpoint for the outer step"
        free <- checkStep backend (WorkflowId workflowText) 1 "inner"
        case free of
          Right Nothing -> pure ()
          _ -> fail "expected no checkpoint for the inner call",
      testCase "durable sleep reuses its recorded wake time" $ do
        backend <- getBackend
        freshId <- UUID.V4.nextRandom
        let workflowText = "hs-l2-sleep-" <> Text.pack (UUID.toString freshId)
            initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2SleepTest"}
        created <- initWorkflow backend initialWorkflow Nothing Fresh Nothing
        case created of
          Left err -> fail (show err)
          Right _ -> pure ()
        firstContext <- ctxOver backend nullTracer workflowText
        first <- sleepWorkflowStep firstContext (millisDuration 25)
        assertEqual "first sleep succeeds" (Right ()) first
        replayContext <- ctxOver backend nullTracer workflowText
        replay <- sleepWorkflowStep replayContext (millisDuration 25)
        assertEqual "replay adopts the original wake time" (Right ()) replay,
      testCase "a nested step reports the step that encloses it" $ do
        backend <- getBackend
        freshId <- UUID.V4.nextRandom
        let workflowText = "hs-l2-step-nested-status-" <> Text.pack (UUID.toString freshId)
            initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2StepNestedStatusTest"}
        created <- initWorkflow backend initialWorkflow Nothing Fresh Nothing
        case created of
          Left err -> fail (show err)
          Right _ -> pure ()
        firstContext <- ctxOver backend nullTracer workflowText
        seen <- newIORef ([] :: [(Maybe StepStatus, Maybe StepStatus, Maybe Int)])
        attempts <- newIORef (0 :: Int)
        first <- runStep firstContext "first" (\_ -> pure ())
        assertEqual "the leading step runs" (Right ()) first
        let outerBody :: Ctx IO -> IO (Either (Error EngineOnly) ())
            outerBody ctx = do
              let outer = stepStatus ctx
              inner <- runStep ctx "inner" (\innerCtx -> do
                let innerStatus = stepStatus innerCtx
                    innerId = stepId innerCtx
                modifyIORef' seen (++ [(outer, innerStatus, innerId)])
                pure ())
              case inner of
                Left err -> pure (Left err)
                Right () -> do
                  attempt <- readIORef attempts
                  modifyIORef' attempts (+ 1)
                  if attempt == 0
                    then pure (Left (StepFailed "outer" "boom"))
                    else pure (Right ())
            options = stepOptionsDefault {max_attempts = 2, interval = millisDuration 1}
        outcome <- runWorkflowStepWith options firstContext "outer" outerBody
        assertEqual "the outer step succeeds on its retry" (Right ()) outcome
        assertEqual "the outer body ran twice" 2 =<< readIORef attempts
        observed <- readIORef seen
        assertEqual "the nested body ran once per attempt" 2 (length observed)
        case observed of
          [(outer1, inner1, id1), (outer2, inner2, id2)] -> do
            assertEqual "the nested body takes no id of its own" (Just 1) id1
            assertEqual "the retry keeps the enclosing id" (Just 1) id2
            assertEqual "a nested step reports the enclosing status whole" outer1 inner1
            assertEqual "the retry reports the enclosing status whole" outer2 inner2
            case (outer1, outer2) of
              (Just firstStatus, Just secondStatus) -> do
                assertEqual "the enclosing id holds still" 1 (stepStatusId firstStatus)
                assertEqual "the retry keeps the enclosing id" 1 (stepStatusId secondStatus)
                assertEqual "the first attempt counts from one" 1 (stepStatusCurrentAttempt firstStatus)
                assertEqual "the second attempt moves" 2 (stepStatusCurrentAttempt secondStatus)
                assertEqual "both attempts share the cap" 2 (stepStatusMaxAttempts firstStatus)
                assertEqual "the cap holds still" 2 (stepStatusMaxAttempts secondStatus)
              _ -> fail ("expected enclosing statuses, got: " <> show observed)
          _ -> fail ("expected two nested observations, got: " <> show observed)
        listed <- SystemDB.listWorkflowSteps backend (WorkflowId workflowText) False Nothing Nothing Nothing
        case listed of
          Right rows -> assertEqual "the nested call writes no checkpoint" ["first", "outer"] (map (.stepRecordStepName) rows)
          Left err -> fail ("expected step rows, got: " <> show err),
      testCase "a cancellation token outside a step never fires" $ do
        backend <- getBackend
        freshId <- UUID.V4.nextRandom
        let workflowText = "hs-l2-step-quiet-token-" <> Text.pack (UUID.toString freshId)
            initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2StepQuietTokenTest"}
        created <- initWorkflow backend initialWorkflow Nothing Fresh Nothing
        case created of
          Left err -> fail (show err)
          Right _ -> pure ()
        ctx <- ctxOver backend nullTracer workflowText
        outside <- cancellationToken ctx
        assertBool "outside a workflow there is no attempt to abandon" =<< stayedQuiet outside
        body <- cancellationToken ctx
        let hanging :: Ctx IO -> IO (Either (Error EngineOnly) ())
            hanging _ = threadDelay 1000000 >> pure (Right ())
            options = (stepOptionsDefault :: StepOptions EngineOnly) {timeout = Just (millisDuration 20)}
        abandoned <- runWorkflowStepWith options ctx "times-out" hanging
        case abandoned of
          Left StepTimeout {} -> pure ()
          other -> fail ("expected the step to blow its deadline, got: " <> show other)
        assertBool "the workflow body's token outlives its abandoned step unfired" =<< stayedQuiet body
    ]

-- | Whether a token stays quiet for 50ms: the claim is about the whole
-- life of the token, not the instant of the call, so a token about to
-- fire would have fired within the wait. Mirrors the oracle's
-- @stayed_quiet@ over @CancellationToken::cancelled@.
stayedQuiet :: StrictTVar IO Bool -> IO Bool
stayedQuiet token = do
  result <- timeout 50000 (atomically (readTVar token >>= check))
  pure (result == Nothing)

-- | The simple step runner at the engine-only channel: top-level test
-- calls do not sit in an annotated body, so the channel needs pinning.
runStep :: (FromJSON value, ToJSON value) => Ctx IO -> Text -> (Ctx IO -> IO value) -> IO (Either (Error EngineOnly) value)
runStep ctx name body = runWorkflowStep ctx name body
