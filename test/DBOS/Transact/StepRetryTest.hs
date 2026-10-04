{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The step retry seam, mirroring Rust @tests/retries.rs@ and the timeout
-- cases of @tests/timeouts.rs@: attempts, backoff, the retry predicate,
-- single-checkpoint replay, per-attempt timeouts, and the two engine errors
-- (@StepTimeout@, @MaxStepRetriesExceeded@). Live database.
module DBOS.Transact.StepRetryTest (tests) where

import DBOS.Prelude
import Data.Aeson (FromJSON, ToJSON)
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (NewWorkflow (..), StepRecord (..), Submission (..), WorkflowId (..), millisDuration, newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  ( runWorkflowStep,
    EngineOnly,
    Identity (..),
    SomeTracer,
    StepCtx,
    WorkflowCtx,
    withWorkflow,
    runWorkflowStepWith,
    Error (..),
    StepOptions (..),
    acquireLoggerBackend,
    stepCtxCancellationToken,
    ioTracer,
    stepOptionsDefault,
    nullTracer,
    tokenCancelled,
  )
import DBOS.Transact.ContextTest (connOver)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
  testGroup
    "Step retries"
    [ testCase "a step that fails twice succeeds on the third attempt" $ withBackendWorkflow getBackend "retry-third" $ \backend workflowText -> do
        attempts <- newIORef (0 :: Int)
        -- Retries announce through FastLogger, so the run proves the
        -- trace seam as well as the attempts it makes.
        (logger, cleanup) <- acquireLoggerBackend
        let body :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body _ = do
              attempt <- readIORef attempts
              modifyIORef' attempts (+ 1)
              if attempt < 2
                then pure (Left (StepFailed "flaky" "boom"))
                else pure (Right (42 :: Int))
            options = stepOptionsDefault {max_attempts = 3, interval = millisDuration 1}
        outcome <- withScopedContext backend (ioTracer logger) workflowText $ \wctx ->
          runWorkflowStepWith options wctx "flaky" body
        cleanup
        outcome @?= Right 42
        readIORef attempts >>= (@?= 3),
      testCase "exhausted retries carry every attempt's failure" $ withBackendWorkflow getBackend "retry-exhausted" $ \backend workflowText -> do
        attempts <- newIORef (0 :: Int)
        let body :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body _ = do
              modifyIORef' attempts (+ 1)
              pure (Left (StepFailed "doomed" "boom"))
            options = stepOptionsDefault {max_attempts = 2, interval = millisDuration 1}
        outcome <- withScopedContext backend nullTracer workflowText $ \wctx ->
          runWorkflowStepWith options wctx "doomed" body
        case outcome of
          Left MaxStepRetriesExceeded {step, attempts = made, errors} -> do
            step @?= "doomed"
            made @?= 2
            length errors @?= 2
          other -> fail ("expected MaxStepRetriesExceeded, got: " <> show other)
        readIORef attempts >>= (@?= 2),
      testCase "the default does not retry and does not wrap" $ withBackendWorkflow getBackend "retry-default" $ \backend workflowText -> do
        attempts <- newIORef (0 :: Int)
        let body :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body _ = do
              modifyIORef' attempts (+ 1)
              pure (Left (StepFailed "plain" "boom"))
        outcome <- withScopedContext backend nullTracer workflowText $ \wctx ->
          runWorkflowStepWith stepOptionsDefault wctx "plain" body
        outcome @?= Left (StepFailed "plain" "boom")
        readIORef attempts >>= (@?= 1),
      testCase "a retried step replays from its single checkpoint" $ withBackendWorkflow getBackend "retry-replay" $ \backend workflowText -> do
        attempts <- newIORef (0 :: Int)

        let body :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body _ = do
              attempt <- readIORef attempts
              modifyIORef' attempts (+ 1)
              if attempt < 1
                then pure (Left (StepFailed "flaky" "boom"))
                else pure (Right (7 :: Int))
            options = stepOptionsDefault {max_attempts = 3, interval = millisDuration 1}
        first <- withScopedContext backend nullTracer workflowText $ \wctx ->
          runWorkflowStepWith options wctx "flaky" body
        first @?= Right 7
        readIORef attempts >>= (@?= 2)
        -- The replay announces through FastLogger, so the run proves the
        -- trace seam as well as the checkpoint it reads back.
        (logger, cleanup) <- acquireLoggerBackend

        replayed <- withScopedContext backend (ioTracer logger) workflowText $ \wctx ->
          runWorkflowStepWith options wctx "flaky" body
        cleanup
        replayed @?= Right 7
        readIORef attempts >>= (@?= 2),
      testCase "a declined failure stops retrying immediately" $ withBackendWorkflow getBackend "retry-declined" $ \backend workflowText -> do
        attempts <- newIORef (0 :: Int)
        (logger, cleanup) <- acquireLoggerBackend
        let body :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body _ = do
              modifyIORef' attempts (+ 1)
              pure (Left (StepFailed "declined" "boom"))
            options =
              stepOptionsDefault
                { max_attempts = 3,
                  interval = millisDuration 1,
                  should_retry = Just (const False)
                }
        outcome <- withScopedContext backend (ioTracer logger) workflowText $ \wctx ->
          runWorkflowStepWith options wctx "declined" body
        cleanup
        outcome @?= Left (StepFailed "declined" "boom")
        readIORef attempts >>= (@?= 1),
      testCase "declining mid-policy keeps the earlier failures" $ withBackendWorkflow getBackend "retry-mid-decline" $ \backend workflowText -> do
        attempts <- newIORef (0 :: Int)
        let body :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body _ = do
              attempt <- readIORef attempts
              modifyIORef' attempts (+ 1)
              pure (Left (StepFailed "pick" (if attempt == 0 then "first" else "second")))
            declinesSecond err = case err of
              StepFailed _ message -> message /= "second"
              _ -> True
            options =
              stepOptionsDefault
                { max_attempts = 3,
                  interval = millisDuration 1,
                  should_retry = Just declinesSecond
                }
        outcome <- withScopedContext backend nullTracer workflowText $ \wctx ->
          runWorkflowStepWith options wctx "pick" body
        case outcome of
          Left MaxStepRetriesExceeded {attempts = made, errors} -> do
            made @?= 2
            length errors @?= 2
            assertBool "the first failure is kept" (any (== StepFailed "pick" "first") errors)
            assertBool "the declining failure is kept" (any (== StepFailed "pick" "second") errors)
          other -> fail ("expected MaxStepRetriesExceeded, got: " <> show other)
        readIORef attempts >>= (@?= 2),
      testCase "a step that hangs is stopped at its timeout" $ withBackendWorkflow getBackend "retry-timeout" $ \backend workflowText -> do
        (logger, cleanup) <- acquireLoggerBackend
        let body :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body _ = threadDelay 50000 >> pure (Right (1 :: Int))
            options = (stepOptionsDefault :: StepOptions EngineOnly) {timeout = Just (millisDuration 5)}
        outcome <- withScopedContext backend (ioTracer logger) workflowText $ \wctx ->
          runWorkflowStepWith options wctx "slow" body
        cleanup
        case outcome of
          Left StepTimeout {step} -> step @?= "slow"
          other -> fail ("expected StepTimeout, got: " <> show other),
      testCase "a step within its timeout is unaffected" $ withBackendWorkflow getBackend "retry-within" $ \backend workflowText -> do
        let body :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body _ = pure (Right (9 :: Int))
            options = (stepOptionsDefault :: StepOptions EngineOnly) {timeout = Just (millisDuration 500)}
        outcome <- withScopedContext backend nullTracer workflowText $ \wctx ->
          runWorkflowStepWith options wctx "quick" body
        outcome @?= Right 9,
      testCase "a timed-out body stops rather than continuing" $ withBackendWorkflow getBackend "retry-stops" $ \backend workflowText -> do
        ran <- newIORef False
        let body :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body _ = threadDelay 100000 >> writeIORef ran True >> pure (Right (1 :: Int))
            options = (stepOptionsDefault :: StepOptions EngineOnly) {timeout = Just (millisDuration 5)}
        outcome <- withScopedContext backend nullTracer workflowText $ \wctx ->
          runWorkflowStepWith options wctx "slow" body
        case outcome of
          Left StepTimeout {} -> pure ()
          other -> fail ("expected StepTimeout, got: " <> show other)
        threadDelay 150000
        readIORef ran >>= (@?= False),
      testCase "a timed-out attempt is retried with a fresh timeout" $ withBackendWorkflow getBackend "retry-fresh" $ \backend workflowText -> do
        attempts <- newIORef (0 :: Int)
        let body :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body _ = do
              attempt <- readIORef attempts
              modifyIORef' attempts (+ 1)
              if attempt < 2
                then threadDelay 100000 >> pure (Right (1 :: Int))
                else pure (Right (42 :: Int))
            options = stepOptionsDefault {max_attempts = 3, interval = millisDuration 1, timeout = Just (millisDuration 20)}
        outcome <- withScopedContext backend nullTracer workflowText $ \wctx ->
          runWorkflowStepWith options wctx "flaky" body
        outcome @?= Right 42
        readIORef attempts >>= (@?= 3),
      testCase "every attempt timing out reports each timeout" $ withBackendWorkflow getBackend "retry-all-timeout" $ \backend workflowText -> do
        let body :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body _ = threadDelay 100000 >> pure (Right (1 :: Int))
            options = stepOptionsDefault {max_attempts = 2, interval = millisDuration 1, timeout = Just (millisDuration 10)}
            isTimeout StepTimeout {} = True
            isTimeout _ = False
        outcome <- withScopedContext backend nullTracer workflowText $ \wctx ->
          runWorkflowStepWith options wctx "slow" body
        case outcome of
          Left MaxStepRetriesExceeded {attempts = made, errors} -> do
            made @?= 2
            length errors @?= 2
            assertBool "every error is a timeout" (all isTimeout errors)
          other -> fail ("expected MaxStepRetriesExceeded, got: " <> show other),
      testCase "a plain step is not preemptible" $ withBackendWorkflow getBackend "retry-plain" $ \backend workflowText -> do
        gate <- newEmptyMVar
        let body :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body _ = takeMVar gate >> pure (Right (7 :: Int))
        worker <-
          async
            ( withScopedContext backend nullTracer workflowText $ \wctx ->
                runWorkflowStepWith stepOptionsDefault wctx "plain" body
            )
        threadDelay 200000
        cancelled <- SystemDB.cancelWorkflows backend [WorkflowId workflowText] False Nothing
        case cancelled of
          Left err -> fail (show err)
          Right _ -> pure ()
        putMVar gate ()
        outcome <- wait worker
        outcome @?= Right 7,
      testCase "a completed step leaves its token alone" $ withBackendWorkflow getBackend "retry-quiet-token" $ \backend workflowText -> do
        seen <- newIORef True
        let body :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body ctx = do
              token <- stepCtxCancellationToken ctx
              fired <- tokenCancelled token
              writeIORef seen fired
              pure (Right (1 :: Int))
        outcome <- withScopedContext backend nullTracer workflowText $ \wctx ->
          runWorkflowStepWith stepOptionsDefault wctx "quiet" body
        outcome @?= Right 1
        readIORef seen >>= (@?= False),
      testCase "the cancellation token fires before the body is dropped" $ withBackendWorkflow getBackend "retry-token-first" $ \backend workflowText -> do
        gate <- newEmptyMVar
        probe <- newIORef (pure False)
        let body :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body ctx = do
              token <- stepCtxCancellationToken ctx
              writeIORef probe (tokenCancelled token)
              takeMVar gate >> pure (Right (1 :: Int))
            options = (stepOptionsDefault :: StepOptions EngineOnly) {timeout = Just (millisDuration 50)}
        outcome <- withScopedContext backend nullTracer workflowText $ \wctx ->
          runWorkflowStepWith options wctx "slow" body
        case outcome of
          Left StepTimeout {} -> pure ()
          other -> fail ("expected StepTimeout, got: " <> show other)
        probeAction <- readIORef probe
        fired <- probeAction
        fired @?= True,
      testCase "a dropped step fires its cancellation token" $ withBackendWorkflow getBackend "retry-token-drop" $ \backend workflowText -> do
        gate <- newEmptyMVar
        started <- newEmptyMVar
        probe <- newIORef (pure False)
        let body :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body ctx = do
              token <- stepCtxCancellationToken ctx
              writeIORef probe (tokenCancelled token)
              putMVar started ()
              takeMVar gate >> pure (Right (1 :: Int))
        worker <-
          async
            ( withScopedContext backend nullTracer workflowText $ \wctx ->
                runWorkflowStepWith stepOptionsDefault wctx "dropped" body
            )
        takeMVar started
        cancel worker
        probeAction <- readIORef probe
        fired <- probeAction
        fired @?= True,
      testCase "a preemptible step stops and records no outcome" $ withBackendWorkflow getBackend "retry-preempt" $ \backend workflowText -> do
        gate <- newEmptyMVar
        started <- newEmptyMVar
        (logger, cleanup) <- acquireLoggerBackend
        let body :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body _ = putMVar started () >> takeMVar gate >> pure (Right (7 :: Int))
            options = stepOptionsDefault {preemptible = True, max_attempts = 3, interval = millisDuration 1, timeout = Just (millisDuration 50)}
        worker <-
          async
            ( withScopedContext backend (ioTracer logger) workflowText $ \wctx ->
                runWorkflowStepWith options wctx "preemptible" body
            )
        takeMVar started
        cancelled <- SystemDB.cancelWorkflows backend [WorkflowId workflowText] False Nothing
        case cancelled of
          Left err -> fail (show err)
          Right _ -> pure ()
        outcome <- wait worker
        cleanup
        case outcome of
          Left (ErrorSystemDatabase SystemDB.WorkflowCancelled {}) -> pure ()
          other -> fail ("expected WorkflowCancelled, got: " <> show other)
        listed <- SystemDB.listWorkflowSteps backend (WorkflowId workflowText) False Nothing Nothing Nothing
        -- A preempted step was interrupted, not wrong, so it records no
        -- outcome at all and a resume runs it again.
        listed @?= Right []
    ]

-- | Create the workflow row the step checkpoints against, then run the test
-- with its backend and workflow id.
-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | A scoped context over the backend: what the converted step cases run
-- their steps through.
withScopedContext :: Postgres.PostgresSystemDB -> SomeTracer IO -> Text -> (forall exec. WorkflowCtx exec IO -> IO a) -> IO a
withScopedContext backend tracer workflowText action = do
  conn <- connOver backend tracer
  withWorkflow conn stepTestIdentity (WorkflowId workflowText) Nothing action

stepTestIdentity :: Identity
stepTestIdentity =
  Identity
    { identityAppName = "test-app",
      identityAppVersion = "1.0.0",
      identityExecutorId = "test-executor",
      identityAppId = ""
    }

withBackendWorkflow :: IO Postgres.PostgresSystemDB -> Text -> (Postgres.PostgresSystemDB -> Text -> IO a) -> IO a
withBackendWorkflow getBackend label action = do
  backend <- getBackend
  freshId <- UUID.V4.nextRandom
  let workflowText = "hs-l2-" <> label <> "-" <> Text.pack (UUID.toString freshId)
      initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2StepRetryTest"}
  created <- SystemDB.initWorkflow backend initialWorkflow Nothing Fresh Nothing
  case created of
    Left err -> fail (show err)
    Right _ -> action backend workflowText

-- | The simple step runner at the engine-only channel: top-level test
-- calls do not sit in an annotated body, so the channel needs pinning.
runStep :: (FromJSON value, ToJSON value) => WorkflowCtx exec IO -> Text -> (StepCtx exec IO -> IO value) -> IO (Either (Error EngineOnly) value)
runStep wctx name body = runWorkflowStep wctx name body
