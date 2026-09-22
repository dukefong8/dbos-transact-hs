{-# LANGUAGE OverloadedStrings #-}

module DBOS.LogTest
  ( tests,
  )
where

import Colog.Core.Action (LogAction (..))
import DBOS.Transact (DbosLogMsg (..), DbosSeverity (..), WorkflowId (..), nullLogAction, withStdoutLogger)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=), testCase)

tests :: TestTree
tests =
  testGroup
    "DBOS Log"
    [ testCase "null logger discards messages" $ do
        unLogAction nullLogAction infoMessage
        unLogAction nullLogAction errorMessage
        pure (),
      testCase "stdout logger runs its action and cleans up" $ do
        result <- withStdoutLogger (\_ -> pure (42 :: Int))
        result @?= 42
    ]

infoMessage :: DbosLogMsg
infoMessage =
  DbosLogMsg
    { logSeverity = DbosInfo,
      logMessage = "getWorkflowExecution",
      logWorkflow = Just (WorkflowId "wf-1")
    }

errorMessage :: DbosLogMsg
errorMessage =
  DbosLogMsg
    { logSeverity = DbosError,
      logMessage = "the workflow is parked",
      logWorkflow = Nothing
    }
