module Main (main, tasty, simTests) where

import DBOS.Prelude
-- PARKED (not in the test-suite build while the Postgres backend is
-- rewritten; re-enable in dbos-transact-hs.cabal first):
-- Codec, Log, SystemDB.
-- Deleted 2026-09-25: SystemDBHasqlTest, TransactTest (Bluefin seam, ADR-0012).
-- Deleted 2026-09-29: SchemaTest, folded live into SystemDB.PostgresTest
-- as schemaTests.
-- Disabled: ClientTest (wedges the watcher eval; skipped by user).
-- Re-enabled 2026-09-29: StarterTest + live ManagementTest, both green in
-- the watcher eval (50/50, 16/16).
import DBOS.StarterTest qualified as Starter
import DBOS.SystemDB.ErrorTest qualified as SystemDBError
import DBOS.SystemDB.IOSimTest qualified as SystemDBIOSim
import DBOS.SystemDB.NotifierTest qualified as SystemDBNotifier
import DBOS.SystemDB.NotifyTest qualified as SystemDBNotify
import DBOS.SystemDB.PostgresTest qualified as SystemDBPostgres
import DBOS.SystemDB.RetryTest qualified as SystemDBRetry
import DBOS.SystemDB.TypesTest qualified as SystemDBTypes
import DBOS.TracerTest qualified as Tracer
import DBOS.Transact.CheckpointTest qualified as TransactCheckpoint
import DBOS.Transact.ClientTest qualified as TransactClient
import DBOS.Transact.ConfigTest qualified as TransactConfig
import DBOS.Transact.ContextTest qualified as TransactContext
import DBOS.Transact.ContextTestSim qualified as TransactContextSim
import DBOS.Transact.DeadlinesTest qualified as TransactDeadlines
import DBOS.Transact.EventTest qualified as TransactEvent
import DBOS.Transact.HandleTest qualified as TransactHandle
import DBOS.Transact.HandleTestIOSim qualified as TransactHandleIOSim
import DBOS.Transact.IdentityTest qualified as TransactIdentity
import DBOS.Transact.InstanceTest qualified as TransactInstance
import DBOS.Transact.ManagementTest qualified as TransactManagement
import DBOS.Transact.ManagementTestSim qualified as TransactManagementSim
import DBOS.Transact.MessageTest qualified as TransactMessage
import DBOS.Transact.MessageTestIOSim qualified as TransactMessageIOSim
import DBOS.Transact.QueueTest qualified as TransactQueue
import DBOS.Transact.RegistryTest qualified as TransactRegistry
import DBOS.Transact.SerializationTest qualified as TransactSerialization
import DBOS.Transact.SimTest qualified as TransactSim
import DBOS.Transact.SleepTest qualified as TransactSleep
import DBOS.Transact.SleepTestIOSim qualified as TransactSleepIOSim
import DBOS.Transact.StepRetryTest qualified as TransactStepRetry
import DBOS.Transact.StepTest qualified as TransactStep
import DBOS.Transact.StepTestIOSim qualified as TransactStepIOSim
import DBOS.Transact.WaitTest qualified as TransactWait
import DBOS.Transact.WaitTestIOSim qualified as TransactWaitIOSim
import DBOS.Transact.WorkflowTest qualified as TransactWorkflow
import System.IO.Silently (capture)
import Test.Tasty (TestTree, defaultIngredients, defaultMain, testGroup)
import Test.Tasty.Ingredients (tryIngredients)
import Test.Tasty.Options (OptionSet)

--- $> tasty DBOS.StarterTest.tests
--- $> tasty DBOS.SystemDB.ErrorTest.tests
--- $> tasty DBOS.SystemDB.IOSimTest.tests
--- $> tasty DBOS.SystemDB.NotifierTest.tests
--- $> tasty DBOS.SystemDB.NotifyTest.tests
--- $> tasty DBOS.SystemDB.PostgresTest.tests
--- $> tasty DBOS.SystemDB.RetryTest.tests
--- $> tasty DBOS.SystemDB.TypesTest.tests
--- $> tasty DBOS.TracerTest.tests
--- $> tasty DBOS.Transact.CheckpointTest.tests
--- $> tasty DBOS.Transact.ClientTest.tests
--- $> tasty DBOS.Transact.ConfigTest.tests
--- $> tasty DBOS.Transact.ContextTest.tests
--- $> tasty TransactContextSim.tests
--- $> tasty TransactHandleIOSim.tests
--- $> tasty DBOS.Transact.DeadlinesTest.tests
--- $> tasty DBOS.Transact.EventTest.tests
--- $> tasty DBOS.Transact.HandleTest.tests
--- $> tasty DBOS.Transact.HandleTestIOSim.tests
--- $> tasty DBOS.Transact.IdentityTest.tests
--- $> tasty DBOS.Transact.InstanceTest.tests
--- $> tasty DBOS.Transact.ManagementTest.tests
-- $> tasty TransactManagementSim.tests
--- $> tasty DBOS.Transact.MessageTest.tests
--- $> tasty DBOS.Transact.MessageTestIOSim.tests
--- $> tasty DBOS.Transact.QueueTest.tests
--- $> tasty DBOS.Transact.RegistryTest.tests
--- $> tasty DBOS.Transact.SerializationTest.tests
--- $> tasty DBOS.Transact.SimTest.tests
--- $> tasty DBOS.Transact.SleepTest.tests
--- $> tasty DBOS.Transact.SleepTestIOSim.tests
--- $> tasty DBOS.Transact.StepRetryTest.tests
--- $> tasty DBOS.Transact.StepTest.tests
--- $> tasty DBOS.Transact.StepTestIOSim.tests
--- $> tasty DBOS.Transact.WaitTest.tests
--- $> tasty DBOS.Transact.WaitTestIOSim.tests
--- $> tasty DBOS.Transact.WorkflowTest.tests
main :: IO ()
main = defaultMain tests

-- | Entry point for ghciwatch `--enable-eval` reloads. Runs the enabled
-- group on stdout and mirrors the captured output into @ghcid.txt@ ahead of
-- ghciwatch's own @--error-file@ writes, so the file reports every outcome
-- (the console additionally keeps progress and reload lines). Unlike
-- 'defaultMain', it never calls 'exitWith' — an uncaught @ExitSuccess@
-- inside an eval just prints @*** Exception: ExitSuccess@ — and never
-- installs signal handlers in the GHCi session.
tasty :: TestTree -> IO ()
tasty tree = do
  old <- readGhcid
  (output, _ok) <- capture (runTree tree)
  putStr output
  writeFile "ghcid.txt" (output ++ old)
  where
    runTree t =
      case tryIngredients defaultIngredients (mempty :: OptionSet) t of
        Nothing       -> pure False
        Just runTests -> runTests
    readGhcid = do
      content <- try (readFile "ghcid.txt") :: IO (Either IOError String)
      case content of
        Left _     -> pure ""
        Right text -> evaluate (length text) >> pure text

tests :: TestTree
tests =
  testGroup
    "dbos-transact-hs"
    [ Starter.tests
    , SystemDBError.tests
    , SystemDBIOSim.tests
    , SystemDBNotifier.tests
    , SystemDBNotify.tests
    , SystemDBPostgres.tests
    , SystemDBRetry.tests
    , SystemDBTypes.tests
    , Tracer.tests
    , TransactCheckpoint.tests
    , TransactClient.tests
    , TransactConfig.tests
    , TransactContext.tests
    , TransactDeadlines.tests
    , TransactEvent.tests
    , TransactHandle.tests
    , TransactHandleIOSim.tests
    , TransactIdentity.tests
    , TransactInstance.tests
    , TransactManagement.tests
    , TransactMessage.tests
    , TransactMessageIOSim.tests
    , TransactQueue.tests
    , TransactRegistry.tests
    , TransactSerialization.tests
    , TransactSim.tests
    , TransactSleep.tests
    , TransactSleepIOSim.tests
    , TransactStep.tests
    , TransactStepIOSim.tests
    , TransactStepRetry.tests
    , TransactWait.tests
    , TransactWaitIOSim.tests
    , TransactWorkflow.tests
    ]

-- | Sim-backed trees for the watcher eval only: never add these to 'tests'
-- ('main' runs live backends only). Referenced here so the qualified import
-- above resolves the @-- $>@ toggle without going redundant.
simTests :: [TestTree]
simTests =
  [ TransactContextSim.tests,
    TransactManagementSim.tests
  ]
