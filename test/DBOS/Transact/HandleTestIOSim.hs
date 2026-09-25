{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.HandleTest' mirrored under IOSim over the mock backend:
-- the real handle code over canned rows. The mock answers @Just@ for every
-- id but @\"missing\"@, so the deleted-row case is mirrored as that id.
module DBOS.Transact.HandleTestIOSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, runSimOrThrow)
import Data.Text (Text)
import DBOS.SystemDB (SerializedWorkflowValue (..), WorkflowId (..), WorkflowStatus (..))
import DBOS.SystemDB.IOSim (simDBOS)
import DBOS.Transact (Error, handleResult, handleStatus, handleWorkflowId, retrieveWorkflow)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Workflow handle (IOSim)"
    [ testCase "a retrieved handle names its workflow and reads its status" $ do
        outcome <- run (retrieveAndStatus "sim-handle")
        case outcome of
          Left err -> fail (show err)
          Right (named, status) -> do
            named @?= "sim-handle"
            case status of
              Right (Just Pending) -> pure ()
              other -> fail (show other),
      testCase "a handle result adopts the recorded output" $ do
        outcome <- run (retrieveAndResult "sim-handle")
        case outcome of
          Right (Just SerializedWorkflowValue {serializedText = storedText}) -> storedText @?= "mock-output"
          other -> fail (show other),
      testCase "a handle over a deleted row reports its absence" $ do
        outcome <- run (retrieveAndCheck "missing")
        outcome @?= Right Nothing
    ]

retrieveAndStatus :: Text -> IOSim s (Either Error (Text, Either Error (Maybe WorkflowStatus)))
retrieveAndStatus wid = do
  dbos <- simDBOS
  retrieved <- retrieveWorkflow dbos (WorkflowId wid)
  case retrieved of
    Left err -> pure (Left err)
    Right handle -> do
      status <- handleStatus handle
      pure (Right (handleWorkflowId handle, status))

retrieveAndResult :: Text -> IOSim s (Either Error (Maybe SerializedWorkflowValue))
retrieveAndResult wid = do
  dbos <- simDBOS
  retrieved <- retrieveWorkflow dbos (WorkflowId wid)
  case retrieved of
    Left err -> pure (Left err)
    Right handle -> handleResult handle

retrieveAndCheck :: Text -> IOSim s (Either Error (Maybe WorkflowStatus))
retrieveAndCheck wid = do
  dbos <- simDBOS
  retrieved <- retrieveWorkflow dbos (WorkflowId wid)
  case retrieved of
    Left err -> pure (Left err)
    Right handle -> handleStatus handle

run :: (forall s. IOSim s a) -> IO a
run action = pure (runSimOrThrow action)
