{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | Bulk message send mirrored under IOSim: the checkpointed
-- @DBOS.sendBulk@ step over the mock backend, whose @sendMessages@ accepts
-- every batch.
module DBOS.Transact.MessageTestIOSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, runSimOrThrow)
import DBOS.SystemDB (WorkflowId (..))
import DBOS.SystemDB.IOSim (simConnection)
import DBOS.Transact
  ( Ctx,
    Identity (..),
    Message (..),
    newCtx,
    newWorkflowState,
    nextExecutionIdentity,
    sendBulk,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Bulk message send (IOSim)"
    [ testCase "a bulk send checkpoints once and delivers the batch" $ do
        outcome <-
          run
            ( do
                context <- simCtx
                sendBulk
                  context
                  [ Message (WorkflowId "first") (1 :: Int) Nothing Nothing,
                    Message (WorkflowId "second") (2 :: Int) Nothing Nothing
                  ]
            )
        outcome @?= Right (),
      testCase "an empty bulk send still takes its step" $ do
        outcome <- run (do context <- simCtx; sendBulk context ([] :: [Message Int]))
        outcome @?= Right ()
    ]

run :: (forall s. IOSim s a) -> IO a
run action = pure (runSimOrThrow action)

simCtx :: IOSim s (Ctx (IOSim s))
simCtx = do
  conn <- simConnection
  identity <- nextExecutionIdentity conn
  state <- newWorkflowState "sim-bulk" Nothing identity
  newCtx conn simIdentity state

simIdentity :: Identity
simIdentity =
  Identity
    { identityAppName = "sim-app",
      identityAppVersion = "0.0.0",
      identityExecutorId = "sim-executor",
      identityAppId = ""
    }
