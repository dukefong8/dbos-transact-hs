{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | The shared 'DatasourceTest' scenarios over the mock backend (and the
-- in-memory backend where rows must be staged): the same cases as the live
-- tree, judged by the same checks. The sim-only extra is the typed
-- 'TransactionEvent' record per case — the trace speaks through types, not
-- lines.
module DBOS.Transact.DatasourceTestSim (tests) where

import DBOS.DualStack (simCase)
import DBOS.Prelude
import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import DBOS.IOSimTracer (simTracer)
import DBOS.SystemDB.IOSim (MemSystemDB, memConnectionOn, newMemDB, simConnectionWith, simInstance)
import DBOS.Transact
  (
  WorkflowCtx,
  WorkflowId (..),
  )
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Datasource (TransactionEvent (..))
import DBOS.Transact.Step (WorkflowEvent (..))
import DBOS.Transact.Context (withWorkflow)
import DBOS.Transact.DatasourceCases
  ( DsFixture (..),
    RegistryFixture (..),
    checkBeginSql,
    checkBodyFailureRecorded,
    checkCaptureRefused,
    checkCommitReplay,
    checkConflictAdopts,
    checkDefaultConfig,
    checkDeleteCheckpoints,
    checkErrorReplays,
    checkNestedInTx,
    checkOutsideReexecutes,
    checkOwnershipMoved,
    checkPrecheckRetry,
    checkRegistryLifecycle,
    checkRetryThenSuccess,
    checkRunsOutside,
    mkFakeDs,
    scenarioBeginSql,
    scenarioBodyFailureRecorded,
    scenarioCaptureRefused,
    scenarioCommitReplay,
    scenarioConflictAdopts,
    scenarioDefaultConfig,
    scenarioDeleteCheckpoints,
    scenarioErrorReplays,
    scenarioNestedInTx,
    scenarioOutsideReexecutes,
    scenarioOwnershipMoved,
    scenarioPrecheckRetry,
    scenarioRegistryLifecycle,
    scenarioRetryThenSuccess,
    scenarioRunsOutside,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

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
    { dsFixtureRun = dsSimRun,
      dsFixtureMkDs = mkFakeDs
    }

dsSimRun :: Text -> (forall exec. WorkflowCtx exec (IOSim s) -> IOSim s a) -> IOSim s a
dsSimRun name action = do
  conn <- simConnectionWith simTracer
  withWorkflow conn simDsIdentity (WorkflowId name) Nothing action

-- | A mem-backed fixture for cases that stage system rows: 'MockSystemDB'
-- answers canned rows, so only 'MemSystemDB' can hold a staged owner.
memDsFixture :: MemSystemDB s -> DsFixture (IOSim s)
memDsFixture mem =
  DsFixture
    { dsFixtureRun = \name action -> do
        conn <- memConnectionOn mem simTracer
        withWorkflow conn simDsIdentity (WorkflowId name) Nothing action,
      dsFixtureMkDs = mkFakeDs
    }

tests :: TestTree
tests =
  testGroup
    "Datasource (IOSim)"
    [ testCase "a default config names nothing and takes the database default isolation" (either fail pure (checkDefaultConfig scenarioDefaultConfig)),
      simCase (pure simDsFixture) "a transaction commits once and replays without re-running" scenarioCommitReplay checkCommitReplay traceCommitReplay,
      simCase (pure simDsFixture) "a recorded failure decodes back to itself" scenarioErrorReplays checkErrorReplays traceErrorReplays,
      simCase (pure simDsFixture) "a body failure records, and replay returns it without re-running" scenarioBodyFailureRecorded checkBodyFailureRecorded traceBodyFailureRecorded,
      simCase (pure simDsFixture) "retriable failures are retried, then the body runs" scenarioRetryThenSuccess checkRetryThenSuccess traceRetryThenSuccess,
      simCase (pure simDsFixture) "a duplicate execution that won is adopted" scenarioConflictAdopts checkConflictAdopts traceConflictAdopts,
      simCase (pure simDsFixture) "a call through a captured parent is refused and records nothing" scenarioCaptureRefused checkCaptureRefused traceCaptureRefused,
      simCase (pure simDsFixture) "a nested step inside a transaction runs plainly and records once" scenarioNestedInTx checkNestedInTx traceNestedInTx,
      testCase "beginSql names every isolation level" (either fail pure (checkBeginSql scenarioBeginSql)),
      simCase (memDsFixture <$> newMemDB) "an ownership move stops the execution instead of adopting" (\fx -> scenarioOwnershipMoved fx "ds-own-sim") checkOwnershipMoved traceOwnershipMoved,
      simCase (pure simDsFixture) "a transient pre-check read is retried, then the transaction runs" scenarioPrecheckRetry checkPrecheckRetry tracePrecheckRetry,
      simCase (pure simDsFixture) "outside a workflow the body runs transactionally and checkpoints nothing" (\_ -> scenarioRunsOutside mkFakeDs) checkRunsOutside traceRunsOutside,
      simCase (pure simDsFixture) "an unrecorded transaction re-runs on every execution" scenarioOutsideReexecutes checkOutsideReexecutes traceOutsideReexecutes,
      simCase (pure simDsFixture) "deleting from a step drops later checkpoints and re-runs" scenarioDeleteCheckpoints checkDeleteCheckpoints traceDeleteCheckpoints,
      simCase (RegistryFixture <$> simInstance <*> (memDsFixture <$> newMemDB)) "the datasource registry refuses duplicates and clears checkpoints" (\(RegistryFixture dbos fx) -> scenarioRegistryLifecycle dbos fx "ds-wf-registry") checkRegistryLifecycle traceRegistryLifecycle
    ]

-- * Typed-event assertions (sim-only): the exact 'TransactionEvent'
-- record each case must emit.

traceCommitReplay :: SimTrace a -> IO ()
traceCommitReplay tr =
  (selectTraceEventsDynamic tr :: [TransactionEvent])
    @?= [ TransactionRunning "ds-wf-1" "proto_step" 0,
          TransactionOutputRecorded "ds-wf-1" "proto_step" 0,
          TransactionRunning "ds-wf-1" "proto_step" 0,
          TransactionReplaying "ds-wf-1" "proto_step" 0
        ]

traceErrorReplays :: SimTrace a -> IO ()
traceErrorReplays tr =
  (selectTraceEventsDynamic tr :: [TransactionEvent])
    @?= [ TransactionRunning "ds-wf-2" "proto_step" 0,
          TransactionReplaying "ds-wf-2" "proto_step" 0
        ]

traceBodyFailureRecorded :: SimTrace a -> IO ()
traceBodyFailureRecorded tr =
  (selectTraceEventsDynamic tr :: [TransactionEvent])
    @?= [ TransactionRunning "ds-wf-6" "proto_step" 0,
          TransactionErrorRecorded "ds-wf-6" "proto_step" 0,
          TransactionRunning "ds-wf-6" "proto_step" 0,
          TransactionReplaying "ds-wf-6" "proto_step" 0
        ]

traceRetryThenSuccess :: SimTrace a -> IO ()
traceRetryThenSuccess tr =
  (selectTraceEventsDynamic tr :: [TransactionEvent])
    @?= [ TransactionRunning "ds-wf-3" "proto_step" 0,
          TransactionSerializationRetry "ds-wf-3" "proto_step" 0 1 1 "serialization failure",
          TransactionSerializationRetry "ds-wf-3" "proto_step" 0 2 2 "serialization failure",
          TransactionOutputRecorded "ds-wf-3" "proto_step" 0
        ]

traceConflictAdopts :: SimTrace a -> IO ()
traceConflictAdopts tr =
  (selectTraceEventsDynamic tr :: [TransactionEvent])
    @?= [ TransactionRunning "ds-wf-4" "proto_step" 0,
          TransactionConflictAdopted "ds-wf-4" "proto_step" 0
        ]

traceCaptureRefused :: SimTrace a -> IO ()
traceCaptureRefused tr =
  (selectTraceEventsDynamic tr :: [TransactionEvent]) @?= []

-- The tx checkpoints once; the inner call announces its plain run beside
-- it — the cross-domain trace both halves of the case own.
traceNestedInTx :: SimTrace a -> IO ()
traceNestedInTx tr = do
  (selectTraceEventsDynamic tr :: [TransactionEvent])
    @?= [ TransactionRunning "ds-wf-nested-tx" "proto_step" 0,
          TransactionOutputRecorded "ds-wf-nested-tx" "proto_step" 0
        ]
  (selectTraceEventsDynamic tr :: [WorkflowEvent]) @?= [StepPlain "inner"]

tracePrecheckRetry :: SimTrace a -> IO ()
tracePrecheckRetry tr =
  (selectTraceEventsDynamic tr :: [TransactionEvent])
    @?= [ TransactionRunning "ds-wf-8" "proto_step" 0,
          TransactionSerializationRetry "ds-wf-8" "proto_step" 0 1 1 "serialization failure",
          TransactionOutputRecorded "ds-wf-8" "proto_step" 0
        ]

traceRunsOutside :: SimTrace a -> IO ()
traceRunsOutside tr =
  (selectTraceEventsDynamic tr :: [TransactionEvent]) @?= []

-- Neither execution announces: unrecorded runs leave no trace either.
traceOutsideReexecutes :: SimTrace a -> IO ()
traceOutsideReexecutes tr =
  (selectTraceEventsDynamic tr :: [TransactionEvent]) @?= []

traceDeleteCheckpoints :: SimTrace a -> IO ()
traceDeleteCheckpoints tr =
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

traceOwnershipMoved :: SimTrace a -> IO ()
traceOwnershipMoved tr =
  (selectTraceEventsDynamic tr :: [TransactionEvent])
    @?= [ TransactionRunning "ds-own-sim" "proto_step" 0,
          TransactionOwnershipLost "ds-own-sim" "other-executor"
        ]

traceRegistryLifecycle :: SimTrace a -> IO ()
traceRegistryLifecycle tr =
  (selectTraceEventsDynamic tr :: [TransactionEvent])
    @?= [ TransactionRunning "ds-wf-registry" "proto_step" 0,
          TransactionOutputRecorded "ds-wf-registry" "proto_step" 0
        ]
