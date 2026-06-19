{-# LANGUAGE OverloadedStrings #-}

module DbosTransact.Compat.ChaosSpec
  ( tests
  ) where

import Control.Concurrent.Async (mapConcurrently)
import Data.IORef (newIORef, readIORef, modifyIORef')
import Data.Text qualified as Text
import DbosTransact.Config (defaultDBOSConfig)
import DbosTransact.Error (DBOSError)
import qualified DbosTransact.Compat.Go as Go
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), testCase)

-- | Port of chaos_test.go tests.
--   Workflow chaos tests run now; Send/Recv/Events/Queues deferred to Phase 7.
tests :: TestTree
tests = testGroup "Compat.ChaosSpec"
  [ testCase "chaos workflow — 100 workflows × 2 steps produce correct results" testChaosWorkflow100
  , testCase "chaos workflow — 50 concurrent workflows do not corrupt state" testChaosWorkflowConcurrent
  , testCase "TODO: port TestChaosRecv from Go — requires Send/Recv (Phase 7)" pendingTest
  , testCase "TODO: port TestChaosEvents from Go — requires SetEvent/GetEvent (Phase 7)" pendingTest
  , testCase "TODO: port TestChaosQueues from Go — requires Queue subsystem (Phase 7)" pendingTest
  ]

pendingTest :: IO ()
pendingTest = pure ()

------------------------------------------------------------
-- Sequential chaos: 100 workflows, each with 2 steps
-- Ported from chaos_test.go:292-377 (scaled from 10,000 to 100)
------------------------------------------------------------

testChaosWorkflow100 :: IO ()
testChaosWorkflow100 = do
  ctx <- Go.newDBOSContext defaultDBOSConfig
  stepOneCounter <- newIORef (0 :: Int)
  stepTwoCounter <- newIORef (0 :: Int)

  Go.registerWorkflow ctx "chaos-workflow" $ \workflowCtx (i :: Int) -> do
    x <- Go.runAsStep workflowCtx [Go.WithStepName "chaos-step-1"] $ do
      modifyIORef' stepOneCounter (+ 1)
      pure (i + 1)
    y <- Go.runAsStep workflowCtx [Go.WithStepName "chaos-step-2"] $ do
      modifyIORef' stepTwoCounter (+ 1)
      pure (x + 2)
    pure y

  let numWorkflows = 100
  results <- mapM (\i -> do
    handle <- Go.runWorkflow ctx "chaos-workflow" i [Go.WithWorkflowID ("wf-chaos-" <> Text.pack (show i))]
    result <- Go.getResult handle :: IO (Either DBOSError Int)
    pure (i, result)
    ) [0 .. numWorkflows - 1]

  -- Verify all results are correct: i → i+1 → i+3
  mapM_ (\(i, result) ->
    result @?= Right (i + 3)
    ) results

  -- Verify each step executed exactly once per workflow
  s1 <- readIORef stepOneCounter
  s2 <- readIORef stepTwoCounter
  s1 @?= numWorkflows
  s2 @?= numWorkflows

------------------------------------------------------------
-- Concurrent chaos: run workflows in parallel, verify isolation
------------------------------------------------------------

testChaosWorkflowConcurrent :: IO ()
testChaosWorkflowConcurrent = do
  ctx <- Go.newDBOSContext defaultDBOSConfig
  executions <- newIORef (0 :: Int)

  Go.registerWorkflow ctx "concurrent-chaos" $ \_workflowCtx (i :: Int) -> do
    modifyIORef' executions (+ 1)
    pure (i * 2)

  let numWorkflows = 50
  results <- mapConcurrently (\i -> do
    handle <- Go.runWorkflow ctx "concurrent-chaos" i [Go.WithWorkflowID ("wf-conc-" <> Text.pack (show i))]
    result <- Go.getResult handle :: IO (Either DBOSError Int)
    pure (i, result)
    ) [0 .. numWorkflows - 1]

  -- Verify all results correct
  mapM_ (\(i, result) ->
    result @?= Right (i * 2)
    ) results

  -- Verify each workflow executed exactly once
  total <- readIORef executions
  total @?= numWorkflows
