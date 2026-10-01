{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.HandleTest' mirrored under IOSim over the mock
-- backend: the real handle code over canned rows, each case printing its
-- sim's 'Say' trace inline so a plain @-- $> tasty@ run shows
-- announcements with no extra plumbing. The mock answers @Just@ for
-- every id but @"missing"@, so the deleted-row case is mirrored as that
-- id; the failed-run and handle-drop cases stay live-only (the mock
-- await always succeeds, and there are no real tasks to outlive a
-- handle). The oracle has exactly one @handle.rs@ trace site (the
-- child-await finding an already-recorded outcome, debug): our replay
-- join announces the same moment through @WorkflowChildJoined@ and the
-- already-finished start through @WorkflowSuperseded@, so no
-- handle-domain event is missing — the pane stays quiet here by
-- fidelity. The cases run on the say-carrier regardless, so any future
-- handle announcement prints with no test change.
module DBOS.Transact.HandleTestSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim)
import Data.Text (Text)
import DBOS.IOSimTracer (printSimTrace, runSimCase, simTracer)
import DBOS.SystemDB (SerializedWorkflowValue (..), WorkflowId (..), WorkflowStatus (..))
import DBOS.SystemDB.IOSim (simDBOSWith)
import DBOS.Transact (
    EngineOnly,DBOS, Error, WorkflowHandle, handleResult, handleStatus, handleWorkflowId, retrieveWorkflow)
import Test.Tasty (DependencyType (..), TestTree, dependentTestGroup)
import Test.Tasty.HUnit (testCase, (@?=))

simSayDBOS :: IOSim s (DBOS (IOSim s))
simSayDBOS = simDBOSWith simTracer

tests :: TestTree
tests =
  -- Sequential: cases announce through one shared stderr, so parallel
  -- 'printSimTrace' calls would interleave their lines mid-character.
  -- 'AllFinish' keeps every case running on a failure, in order.
  dependentTestGroup
    "Workflow handle (Sim)"
    AllFinish
    [ testCase "a retrieved handle names its workflow and reads its status" $ do
        (outcome, tr) <- runSimCase (retrieveAndStatus "sim-handle")
        printSimTrace tr
        case outcome of
          Left err -> fail (show err)
          Right (named, status) -> do
            named @?= "sim-handle"
            case status of
              Right (Just Pending) -> pure ()
              other -> fail (show other),
      testCase "a handle result adopts the recorded output" $ do
        (outcome, tr) <- runSimCase (retrieveAndResult "sim-handle")
        printSimTrace tr
        case outcome of
          Right (Just SerializedWorkflowValue {serializedText = storedText}) -> storedText @?= "mock-output"
          other -> fail (show other),
      testCase "a handle over a deleted row reports its absence" $ do
        (outcome, tr) <- runSimCase (retrieveAndCheck "missing")
        printSimTrace tr
        outcome @?= Right Nothing
    ]

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree below reads through. Local
-- copies are deliberate: this module carries only the aliases it uses.
retrieveWfSim :: DBOS (IOSim s) -> WorkflowId -> IOSim s (Either (Error EngineOnly) (WorkflowHandle (IOSim s) EngineOnly))
retrieveWfSim = retrieveWorkflow

resultWfSim :: WorkflowHandle (IOSim s) EngineOnly -> IOSim s (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWfSim = handleResult

statusWfSim :: WorkflowHandle (IOSim s) EngineOnly -> IOSim s (Either (Error EngineOnly) (Maybe WorkflowStatus))
statusWfSim = handleStatus

retrieveAndStatus :: Text -> IOSim s (Either (Error EngineOnly) (Text, Either (Error EngineOnly) (Maybe WorkflowStatus)))
retrieveAndStatus wid = do
  dbos <- simSayDBOS
  retrieved <- retrieveWfSim dbos (WorkflowId wid)
  case retrieved of
    Left err -> pure (Left err)
    Right handle -> do
      status <- statusWfSim handle
      pure (Right (handleWorkflowId handle, status))

retrieveAndResult :: Text -> IOSim s (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
retrieveAndResult wid = do
  dbos <- simSayDBOS
  retrieved <- retrieveWfSim dbos (WorkflowId wid)
  case retrieved of
    Left err -> pure (Left err)
    Right handle -> resultWfSim handle

retrieveAndCheck :: Text -> IOSim s (Either (Error EngineOnly) (Maybe WorkflowStatus))
retrieveAndCheck wid = do
  dbos <- simSayDBOS
  retrieved <- retrieveWfSim dbos (WorkflowId wid)
  case retrieved of
    Left err -> pure (Left err)
    Right handle -> statusWfSim handle
