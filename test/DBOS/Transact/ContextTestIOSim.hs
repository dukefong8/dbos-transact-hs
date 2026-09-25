{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.ContextTest' mirrored under IOSim over the mock backend:
-- the same 19 cases, with IO-only plumbing (IORef, IO MVar, real
-- @threadDelay@, 'Control.Exception.try') replaced by the io-classes
-- equivalents the sim provides. The contexts run over 'simConnection', so
-- the mirror also proves the mock 'SystemDB' instance carries a context.
module DBOS.Transact.ContextTestIOSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, runSimOrThrow)
import Data.List (isInfixOf)
import Data.Text (Text)
import Control.Monad.Class.MonadThrow qualified as MThrow
import DBOS.SystemDB.IOSim (simConnection)
import DBOS.Transact
  ( Connection (..),
    Ctx,
    Identity (..),
    cancelToken,
    cancellationToken,
    currentConnection,
    currentIdentity,
    deadline,
    firstStepStatus,
    inStep,
    isSameExecution,
    newCtx,
    newWorkflowState,
    nextAttempt,
    nextExecutionIdentity,
    nextStepId,
    nextStepMarker,
    stepId,
    stepStatus,
    stepStatusCurrentAttempt,
    stepStatusId,
    stepStatusMaxAttempts,
    tokenCancelled,
    withAttempt,
    workflowId,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Context (IOSim)"
    [ testCase "a context reads its workflow id" $ do
        result <- run (workflowId <$> simCtx)
        result @?= "wf-1",
      testCase "a workflow's step ids are zero based and allocated once" $ do
        result <- run $ do
          ctx <- simCtx
          (,,) <$> nextStepId ctx <*> nextStepId ctx <*> nextStepId ctx
        result @?= (0, 1, 2),
      testCase "step ids stay dense while markers spend their own sequence" $ do
        result <- run $ do
          ctx <- simCtx
          first <- nextStepId ctx
          _ <- nextStepMarker ctx
          second <- nextStepId ctx
          _ <- nextStepMarker ctx
          third <- nextStepId ctx
          pure (first, second, third)
        result @?= (0, 1, 2),
      testCase "withAttempt scopes a step and leaves the outer scope alone" $ do
        result <- run $ do
          ctx <- simCtx
          innerMarker <- nextStepMarker ctx
          let outside = stepId ctx
          inner <- withAttempt ctx innerMarker (firstStepStatus 4) (pure . stepId)
          let after = stepId ctx
          pure (outside, inner, after)
        result @?= (Nothing, Just 4, Nothing),
      testCase "a scope reports its status and id" $ do
        result <- run $ do
          ctx <- simCtx
          marker <- nextStepMarker ctx
          let proper = stepStatus ctx
              properFlag = inStep ctx
          scoped <-
            withAttempt ctx marker (firstStepStatus 3) $ \stepped ->
              pure (stepStatus stepped, stepId stepped, inStep stepped)
          pure (proper, properFlag, scoped)
        case result of
          (Nothing, False, (Just status, Just 3, True)) -> do
            stepStatusId status @?= 3
            stepStatusCurrentAttempt status @?= 1
          other -> fail ("expected proper Nothing and scoped status: " <> show other),
      testCase "a first attempt reports its step, attempt 1 of 1" $ do
        let status = firstStepStatus 3
        stepStatusId status @?= 3
        stepStatusCurrentAttempt status @?= 1
        stepStatusMaxAttempts status @?= 1,
      testCase "a retry keeps the step and moves the attempt" $ do
        let second = nextAttempt (firstStepStatus 3)
        stepStatusId second @?= 3
        stepStatusCurrentAttempt second @?= 2
        stepStatusMaxAttempts second @?= 1,
      testCase "a fresh token is quiet until fired" $ do
        result <- run $ do
          ctx <- simCtx
          token <- cancellationToken ctx
          quiet <- tokenCancelled token
          cancelToken token
          fired <- tokenCancelled token
          pure (quiet, fired)
        result @?= (False, True),
      testCase "each attempt watches a token of its own" $ do
        result <- run $ do
          ctx <- simCtx
          firstMarker <- nextStepMarker ctx
          secondMarker <- nextStepMarker ctx
          first <- withAttempt ctx firstMarker (firstStepStatus 0) cancellationToken
          second <- withAttempt ctx secondMarker (firstStepStatus 1) cancellationToken
          cancelToken first
          firstFired <- tokenCancelled first
          secondFired <- tokenCancelled second
          pure (firstFired, secondFired)
        result @?= (True, False),
      testCase "a deadline rides the workflow state" $ do
        result <- run (deadline <$> simCtx)
        result @?= Nothing,
      testCase "two contexts over one workflow share its step counter" $ do
        result <- run $ do
          conn <- simConnection
          identity <- nextExecutionIdentity conn
          state <- newWorkflowState "wf-1" Nothing identity
          first <- newCtx conn simIdentity state
          second <- newCtx conn simIdentity state
          (,) <$> nextStepId first <*> nextStepId second
        result @?= (0, 1),
      testCase "a re-run of one id is a different execution" $ do
        result <- run $ do
          conn <- simConnection
          firstId <- nextExecutionIdentity conn
          secondId <- nextExecutionIdentity conn
          firstState <- newWorkflowState "wf-1" Nothing firstId
          secondState <- newWorkflowState "wf-1" Nothing secondId
          first <- newCtx conn simIdentity firstState
          second <- newCtx conn simIdentity secondState
          pure (isSameExecution first first, isSameExecution first second)
        result @?= (True, False),
      testCase "the connection and identity travel with the context" $ do
        result <- run $ do
          ctx <- simCtx
          pure (currentIdentity ctx, (currentConnection ctx).connAppName)
        case result of
          (identity, appName) -> do
            identity @?= simIdentity
            appName @?= Just ("sim-app" :: Text),
      testCase "nested runners isolate" $ do
        result <- run $ do
          conn <- simConnection
          outerId <- nextExecutionIdentity conn
          innerId <- nextExecutionIdentity conn
          outerState <- newWorkflowState "wf-1" Nothing outerId
          innerState <- newWorkflowState "wf-1" Nothing innerId
          outer <- newCtx conn simIdentity outerState
          inner <- newCtx conn simIdentity innerState
          pure (workflowId outer, workflowId inner, isSameExecution outer inner)
        result @?= ("wf-1", "wf-1", False),
      testCase "state interop runs beside the context" $ do
        result <- run $ do
          ctx <- simCtx
          ref <- newTVarIO ("" :: Text)
          atomically (writeTVar ref "done")
          _ <- pure ctx
          readTVarIO ref
        result @?= "done",
      testCase "a throw from an engine call reaches the caller" $ do
        outcome <- run (throwFromCtx simCtx)
        case outcome of
          Left err -> assertBool "the original throw escapes" ("boom" `isInfixOf` show err)
          Right _ -> fail "expected the throw to escape",
      testCase "a cooperative flag cancels a wait promptly" $ do
        run $ do
          stop <- newTVarIO False
          done <- newEmptyMVar
          _ <- forkIO (waitForFlag stop done)
          threadDelay 200000
          atomically (writeTVar stop True)
          waitFor (takeMVar done),
      testCase "a fork handed the context shares its counter" $ do
        result <- run $ do
          ctx <- simCtx
          first <- nextStepId ctx
          seen <- newEmptyMVar
          _ <- forkIO (nextStepId ctx >>= putMVar seen)
          second <- waitFor (takeMVar seen)
          pure (first, second)
        result @?= (0, 1),
      testCase "concurrent contexts are isolated from each other" $ do
        result <- run $ do
          first <- newEmptyMVar
          second <- newEmptyMVar
          let child name box = do
                conn <- simConnection
                identity <- nextExecutionIdentity conn
                state <- newWorkflowState name Nothing identity
                ctx <- newCtx conn simIdentity state
                putMVar box (workflowId ctx)
          a <- async (child "a" first)
          b <- async (child "b" second)
          wait a
          wait b
          (,) <$> takeMVar first <*> takeMVar second
        result @?= ("a", "b")
    ]

-- * Helpers

throwFromCtx :: IOSim s (Ctx (IOSim s)) -> IOSim s (Either MThrow.SomeException Int)
throwFromCtx makeCtx = do
  ctx <- makeCtx
  MThrow.try (nextStepId ctx >> MThrow.throwIO (userError "boom") >> pure 0)

run :: (forall s. IOSim s a) -> IO a
run action = pure (runSimOrThrow action)

simIdentity :: Identity
simIdentity =
  Identity
    { identityAppName = "sim-app",
      identityAppVersion = "0.0.0",
      identityExecutorId = "sim-executor",
      identityAppId = ""
    }

simCtx :: IOSim s (Ctx (IOSim s))
simCtx = do
  conn <- simConnection
  identity <- nextExecutionIdentity conn
  state <- newWorkflowState "wf-1" Nothing identity
  newCtx conn simIdentity state

-- | A wait that polls a cooperative flag instead of sleeping through it.
waitForFlag :: (MonadSTM m, MonadMVar m, MonadDelay m) => StrictTVar m Bool -> StrictMVar m () -> m ()
waitForFlag stop done = do
  flag <- readTVarIO stop
  if flag
    then putMVar done ()
    else threadDelay 50000 >> waitForFlag stop done

-- | A result that must arrive, not a wait that may hang the suite. Under
-- IOSim the timeout is virtual, so a hung wait fails instantly.
waitFor :: (MonadTimer m, MonadThrow m) => m a -> m a
waitFor action = do
  result <- timeout 5000000 action
  case result of
    Just value -> pure value
    Nothing -> throwIO (userError "timed out waiting for a result")
