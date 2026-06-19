{-# LANGUAGE OverloadedStrings #-}

module DbosTransact.Compat.IdempotencySpec
  ( tests
  ) where

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Data.Either (isLeft)
import Data.IORef (newIORef, readIORef, writeIORef, modifyIORef')
import Data.Text (Text)
import DbosTransact.Config (defaultDBOSConfig)
import DbosTransact.Error (DBOSError)
import DbosTransact.Workflow (WorkflowStatus(..))
import qualified DbosTransact.Compat.Go as Go
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), assertBool, testCase)

tests :: TestTree
tests = testGroup "Compat.IdempotencySpec"
  [ testCase "same workflow ID returns same result without rerunning body" testSameWorkflowIdReturnsExistingResult
  , testCase "concurrent same workflow ID executes one body" testNoConcurrentSameIDBody
  , testCase "recovery reuses recorded step outputs and completes later steps" testRecoveryReusesStepOutputs
  ]

testSameWorkflowIdReturnsExistingResult :: IO ()
testSameWorkflowIdReturnsExistingResult = do
  ctx <- Go.newDBOSContext defaultDBOSConfig
  executions <- newIORef (0 :: Int)
  Go.registerWorkflow ctx "idempotent" $ \_ (input :: Text) -> do
    modifyIORef' executions (+ 1)
    pure ("hello " <> input)
  first <- Go.runWorkflow ctx "idempotent" ("dbos" :: Text) [Go.WithWorkflowID "wf-idempotent"]
  second <- Go.runWorkflow ctx "idempotent" ("ignored" :: Text) [Go.WithWorkflowID "wf-idempotent"]
  Go.getWorkflowID first @?= "wf-idempotent"
  Go.getWorkflowID second @?= "wf-idempotent"
  Go.getResult first >>= (@?= Right ("hello dbos" :: Text))
  Go.getResult second >>= (@?= Right ("hello dbos" :: Text))
  readIORef executions >>= (@?= 1)

testNoConcurrentSameIDBody :: IO ()
testNoConcurrentSameIDBody = do
  ctx <- Go.newDBOSContext defaultDBOSConfig
  executions <- newIORef (0 :: Int)
  started <- newEmptyMVar
  firstHandleVar <- newEmptyMVar
  Go.registerWorkflow ctx "one-active" $ \_ (_input :: ()) -> do
    modifyIORef' executions (+ 1)
    putMVar started ()
    pure ("done" :: Text)
  _ <- forkIO $ Go.runWorkflow ctx "one-active" () [Go.WithWorkflowID "wf-one-active"] >>= putMVar firstHandleVar
  takeMVar started
  second <- Go.runWorkflow ctx "one-active" () [Go.WithWorkflowID "wf-one-active"]
  first <- takeMVar firstHandleVar
  Go.getResult first >>= (@?= Right ("done" :: Text))
  Go.getResult second >>= (@?= Right ("done" :: Text))
  readIORef executions >>= (@?= 1)
  status <- either (error . show) pure =<< Go.getStatus second
  statusRecoveryAttempts status @?= 1

testRecoveryReusesStepOutputs :: IO ()
testRecoveryReusesStepOutputs = do
  ctx <- Go.newDBOSContext defaultDBOSConfig
  firstAttempt <- newIORef True
  firstStepExecutions <- newIORef (0 :: Int)
  secondStepExecutions <- newIORef (0 :: Int)
  Go.registerWorkflow ctx "recoverable" $ \workflowCtx (_input :: ()) -> do
    firstValue <- Go.runAsStep workflowCtx [Go.WithStepName "first"] $ do
      modifyIORef' firstStepExecutions (+ 1)
      pure (10 :: Int)
    shouldFail <- readIORef firstAttempt
    if shouldFail
      then do
        writeIORef firstAttempt False
        ioError (userError "crash after first step")
      else do
        secondValue <- Go.runAsStep workflowCtx [Go.WithStepName "second"] $ do
          modifyIORef' secondStepExecutions (+ 1)
          pure (32 :: Int)
        pure (firstValue + secondValue)
  failed <- Go.runWorkflow ctx "recoverable" () [Go.WithWorkflowID "wf-recoverable"]
  failedResult <- Go.getResult failed :: IO (Either DBOSError Int)
  assertBool "first attempt should fail" (isLeft failedResult)
  recovered <- Go.runWorkflow ctx "recoverable" () [Go.WithWorkflowID "wf-recoverable"]
  Go.getResult recovered >>= (@?= Right (42 :: Int))
  readIORef firstStepExecutions >>= (@?= 1)
  readIORef secondStepExecutions >>= (@?= 1)
