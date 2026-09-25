{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | In-workflow waits mirrored under IOSim: the checkpointed
-- @DBOS.selectWorkflow@ step and the uncheckpointed all-wait, over the mock
-- backend. The mock answers the first id of the set, and its @checkStep@ is
-- empty, so each call takes its step and records.
module DBOS.Transact.WaitTestIOSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, runSimOrThrow)
import DBOS.SystemDB (WorkflowId (..))
import DBOS.SystemDB.IOSim (simConnection)
import DBOS.Transact
  ( Ctx,
    Error (..),
    Identity (..),
    joinWorkflows,
    newCtx,
    newWorkflowState,
    nextExecutionIdentity,
    selectWorkflow,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "In-workflow waits (IOSim)"
    [ testCase "a first-wait answers with the first id and takes its step" $ do
        outcome <- run (do context <- simCtx; selectWorkflow context [WorkflowId "first", WorkflowId "second"])
        outcome @?= Right (WorkflowId "first"),
      testCase "a first-wait over nothing is refused" $ do
        outcome <- run (do context <- simCtx; selectWorkflow context [])
        case outcome of
          Left (InvalidArgument operation detail) -> do
            operation @?= "select_workflow"
            detail @?= "no workflow ids to wait for"
          other -> fail (show other),
      testCase "an all-wait completes without a step" $ do
        outcome <- run (do context <- simCtx; joinWorkflows context [WorkflowId "a", WorkflowId "b"])
        outcome @?= Right ()
    ]

run :: (forall s. IOSim s a) -> IO a
run action = pure (runSimOrThrow action)

simCtx :: IOSim s (Ctx (IOSim s))
simCtx = do
  conn <- simConnection
  identity <- nextExecutionIdentity conn
  state <- newWorkflowState "sim-wait" Nothing identity
  newCtx conn simIdentity state

simIdentity :: Identity
simIdentity =
  Identity
    { identityAppName = "sim-app",
      identityAppVersion = "0.0.0",
      identityExecutorId = "sim-executor",
      identityAppId = ""
    }
