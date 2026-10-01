{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.WaitTest' mirrored under IOSim: the checkpointed
-- @DBOS.selectWorkflow@ step and the uncheckpointed all-wait, each case
-- printing its sim's 'Say' trace inline so a plain @-- $> tasty@ run
-- shows announcements with no extra plumbing. The mock answers the first
-- id of the set, and its @checkStep@ is empty, so the fresh calls take
-- their step and record; the replay case runs over 'MemSystemDB', whose
-- stateful steps let the second call read the recorded winner back and
-- announce it, the same line the live tree writes through FastLogger.
-- The all-wait takes no placement and stays quiet, as in the oracle.
module DBOS.Transact.WaitTestSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim)
import Data.Text (Text)
import DBOS.IOSimTracer (printSimTrace, runSimCase, simTracerSay)
import DBOS.SystemDB (WorkflowId (..))
import DBOS.SystemDB.IOSim (memConnectionOn, newMemDB, simConnectionWith)
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
import Test.Tasty (DependencyType (..), TestTree, dependentTestGroup)
import Test.Tasty.HUnit (testCase, (@?=))

simIdentity :: Identity
simIdentity =
  Identity
    { identityAppName = "sim-app",
      identityAppVersion = "0.0.0",
      identityExecutorId = "sim-executor",
      identityAppId = ""
    }

tests :: TestTree
tests =
  -- Sequential: cases announce through one shared stderr, so parallel
  -- 'printSimTrace' calls would interleave their lines mid-character.
  -- 'AllFinish' keeps every case running on a failure, in order.
  dependentTestGroup
    "In-workflow waits (Sim)"
    AllFinish
    [ testCase "a first-wait answers with the first id and takes its step" $ do
        (outcome, tr) <- runSimCase $ do
          context <- simCtx "sim-wait"
          selectWorkflow context [WorkflowId "first", WorkflowId "second"]
        printSimTrace tr
        outcome @?= Right (WorkflowId "first"),
      testCase "a first-wait over nothing is refused" $ do
        (outcome, tr) <- runSimCase $ do
          context <- simCtx "sim-wait-empty"
          selectWorkflow context []
        printSimTrace tr
        case outcome of
          Left (InvalidArgument operation detail) -> do
            operation @?= "select_workflow"
            detail @?= "no workflow ids to wait for"
          other -> fail (show other),
      testCase "a replayed first-wait reads its recorded winner back" $ do
        (outcome, tr) <- runSimCase $ do
          mem <- newMemDB
          conn <- memConnectionOn mem simTracerSay
          let runOnce = do
                identity <- nextExecutionIdentity conn
                state <- newWorkflowState "sim-wait-replay" Nothing identity
                context <- newCtx conn simIdentity state
                selectWorkflow context [WorkflowId "first", WorkflowId "second"]
          first <- runOnce
          second <- runOnce
          pure (first, second)
        printSimTrace tr
        -- The mock's first-id answer is what gets recorded, so the replay
        -- reads the same winner back and announces it.
        outcome @?= (Right (WorkflowId "first"), Right (WorkflowId "first")),
      testCase "an all-wait completes without a step" $ do
        (outcome, tr) <- runSimCase $ do
          context <- simCtx "sim-wait-join"
          joinWorkflows context [WorkflowId "a", WorkflowId "b"]
        printSimTrace tr
        outcome @?= Right ()
    ]
  where
    simCtx :: Text -> IOSim s (Ctx (IOSim s))
    simCtx name = do
      conn <- simConnectionWith simTracerSay
      identity <- nextExecutionIdentity conn
      state <- newWorkflowState name Nothing identity
      newCtx conn simIdentity state
