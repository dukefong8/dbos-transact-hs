{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | The shared 'DatasourceTest' scenarios over 'MockSystemDB': the same
-- cases as the live tree, with values asserted here. Each case prints its
-- sim's 'Say' trace inline, so a plain @-- $> tasty@ run shows traces
-- with no extra plumbing.
module DBOS.Transact.DatasourceTestSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, selectTraceEventsDynamic)
import DBOS.IOSimTracer (printSimTrace, runSimCase, simTracer)
import DBOS.SystemDB.IOSim (MemSystemDB, memConnectionOn, newMemDB, simConnectionWith)
import DBOS.Transact
  ( Ctx,
    Error (..),
    Identity (..),
    TransactionEvent (..),
    application,
    newCtx,
    newWorkflowState,
    nextExecutionIdentity,
    renderTransactError,
  )
import DBOS.Transact.DatasourceTest
  ( DsFixture (..),
    mkFakeDs,
    scenarioBodyFailureRecorded,
    scenarioCommitReplay,
    scenarioConflictAdopts,
    scenarioDeleteCheckpoints,
    scenarioErrorReplays,
    scenarioInStepRefused,
    scenarioOwnershipMoved,
    scenarioPrecheckRetry,
    scenarioRetryThenSuccess,
    scenarioRunsOutside,
  )
import Data.Text (Text)
import Data.Text qualified as Text
import Test.Tasty (DependencyType (..), TestTree, dependentTestGroup)
import Test.Tasty.HUnit (assertBool, assertFailure, testCase, (@?=))

simDsIdentity :: Identity
simDsIdentity =
  Identity
    { identityAppName = "sim-app",
      identityAppVersion = "0.0.0",
      identityExecutorId = "sim-executor",
      identityAppId = ""
    }

-- | Every case runs against a fresh mock connection and a fresh fake
-- datasource; each use instantiates the fixture at its own simulation.
simDsFixture :: forall s. DsFixture (IOSim s)
simDsFixture =
  DsFixture
    { dsFixtureMkCtx = dsSimCtx,
      dsFixtureMkDs = mkFakeDs
    }

dsSimCtx :: Text -> IOSim s (Ctx (IOSim s))
dsSimCtx name = do
  conn <- simConnectionWith simTracer
  identity <- nextExecutionIdentity conn
  state <- newWorkflowState name Nothing identity
  newCtx conn simDsIdentity state

-- | A mem-backed fixture for cases that stage system rows: 'MockSystemDB'
-- answers canned rows, so only 'MemSystemDB' can hold a staged owner.
memDsFixture :: MemSystemDB s -> DsFixture (IOSim s)
memDsFixture mem =
  DsFixture
    { dsFixtureMkCtx = \name -> do
        conn <- memConnectionOn mem simTracer
        identity <- nextExecutionIdentity conn
        state <- newWorkflowState name Nothing identity
        newCtx conn simDsIdentity state,
      dsFixtureMkDs = mkFakeDs
    }

tests :: TestTree
tests =
  -- Sequential: cases announce through one shared stderr, so parallel
  -- 'printSimTrace' calls would interleave their lines mid-character.
  -- 'AllFinish' keeps every case running on a failure, in order.
  dependentTestGroup
    "Datasource (IOSim)"
    AllFinish
    [ testCase "a transaction commits once and replays without re-running" $ do
        (res, tr) <- runSimCase (scenarioCommitReplay simDsFixture)
        printSimTrace tr
        res @?= (Right "v1", Right "v1", 1)
        (selectTraceEventsDynamic tr :: [TransactionEvent])
          @?= [ TransactionRunning "ds-wf-1" "proto_step" 0,
                TransactionOutputRecorded "ds-wf-1" "proto_step" 0,
                TransactionRunning "ds-wf-1" "proto_step" 0,
                TransactionReplaying "ds-wf-1" "proto_step" 0
              ],
      testCase "a recorded failure decodes back to itself" $ do
        (res, tr) <- runSimCase (scenarioErrorReplays simDsFixture)
        printSimTrace tr
        res @?= Left (application "boom")
        (selectTraceEventsDynamic tr :: [TransactionEvent])
          @?= [ TransactionRunning "ds-wf-2" "proto_step" 0,
                TransactionReplaying "ds-wf-2" "proto_step" 0
              ],
      testCase "a body failure records, and replay returns it without re-running" $ do
        (res, tr) <- runSimCase (scenarioBodyFailureRecorded simDsFixture)
        printSimTrace tr
        res @?= (Left (application "boom"), Left (application "boom"), 0)
        (selectTraceEventsDynamic tr :: [TransactionEvent])
          @?= [ TransactionRunning "ds-wf-6" "proto_step" 0,
                TransactionErrorRecorded "ds-wf-6" "proto_step" 0,
                TransactionRunning "ds-wf-6" "proto_step" 0,
                TransactionReplaying "ds-wf-6" "proto_step" 0
              ],
      testCase "retriable failures are retried, then the body runs" $ do
        (res, tr) <- runSimCase (scenarioRetryThenSuccess simDsFixture)
        printSimTrace tr
        res @?= (Right "v", 0)
        (selectTraceEventsDynamic tr :: [TransactionEvent])
          @?= [ TransactionRunning "ds-wf-3" "proto_step" 0,
                TransactionSerializationRetry "ds-wf-3" "proto_step" 0 1 1 "serialization failure",
                TransactionSerializationRetry "ds-wf-3" "proto_step" 0 2 2 "serialization failure",
                TransactionOutputRecorded "ds-wf-3" "proto_step" 0
              ],
      testCase "a duplicate execution that won is adopted" $ do
        (res, tr) <- runSimCase (scenarioConflictAdopts simDsFixture)
        printSimTrace tr
        res @?= Right "winner"
        (selectTraceEventsDynamic tr :: [TransactionEvent])
          @?= [ TransactionRunning "ds-wf-4" "proto_step" 0,
                TransactionConflictAdopted "ds-wf-4" "proto_step" 0
              ],
      testCase "a call inside a step is refused and records nothing" $ do
        (res, tr) <- runSimCase (scenarioInStepRefused simDsFixture)
        printSimTrace tr
        res @?= (Left (InsideStep "transaction"), 0)
        (selectTraceEventsDynamic tr :: [TransactionEvent]) @?= [],
      testCase "an ownership move stops the execution instead of adopting" $ do
        (res, tr) <- runSimCase $ do
          mem <- newMemDB
          scenarioOwnershipMoved (memDsFixture mem) "ds-own-sim"
        printSimTrace tr
        case res of
          Left err -> assertBool "names the owning executor" ("other-executor" `Text.isInfixOf` renderTransactError err)
          Right _ -> assertFailure "expected the ownership conflict to stop the execution"
        -- The event names the ruling executor, not the owner token.
        (selectTraceEventsDynamic tr :: [TransactionEvent])
          @?= [ TransactionRunning "ds-own-sim" "proto_step" 0,
                TransactionOwnershipLost "ds-own-sim" "other-executor"
              ],
      testCase "a transient pre-check read is retried, then the transaction runs" $ do
        (res, tr) <- runSimCase (scenarioPrecheckRetry simDsFixture)
        printSimTrace tr
        res @?= (Right "v", 0)
        (selectTraceEventsDynamic tr :: [TransactionEvent])
          @?= [ TransactionRunning "ds-wf-8" "proto_step" 0,
                TransactionSerializationRetry "ds-wf-8" "proto_step" 0 1 1 "serialization failure",
                TransactionOutputRecorded "ds-wf-8" "proto_step" 0
              ],
      testCase "outside a workflow the body runs transactionally and checkpoints nothing" $ do
        (res, tr) <- runSimCase (scenarioRunsOutside mkFakeDs)
        printSimTrace tr
        res @?= (Right "v", 0, 1)
        (selectTraceEventsDynamic tr :: [TransactionEvent]) @?= [],
      testCase "deleting from a step drops later checkpoints and re-runs" $ do
        (res, tr) <- runSimCase (scenarioDeleteCheckpoints simDsFixture)
        printSimTrace tr
        res @?= (Right "v", Right "v", Right "v", Right "v", 3)
        (selectTraceEventsDynamic tr :: [TransactionEvent])
          @?= [ TransactionRunning "ds-wf-9" "proto_step" 0,
                TransactionOutputRecorded "ds-wf-9" "proto_step" 0,
                TransactionRunning "ds-wf-9" "proto_step" 1,
                TransactionOutputRecorded "ds-wf-9" "proto_step" 1,
                TransactionRunning "ds-wf-9" "proto_step" 0,
                TransactionReplaying "ds-wf-9" "proto_step" 0,
                TransactionRunning "ds-wf-9" "proto_step" 0,
                TransactionOutputRecorded "ds-wf-9" "proto_step" 0
              ]
    ]
