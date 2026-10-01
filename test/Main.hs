module Main (main, tasty, simTests) where

import DBOS.Prelude
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
import DBOS.Transact.ErrorTest qualified as TransactError
import DBOS.Transact.EventTest qualified as TransactEvent
import DBOS.Transact.HandleTest qualified as TransactHandle
import DBOS.Transact.HandleTestSim qualified as TransactHandleSim
import DBOS.Transact.IdentityTest qualified as TransactIdentity
import DBOS.Transact.InstanceTest qualified as TransactInstance
import DBOS.Transact.ManagementTest qualified as TransactManagement
import DBOS.Transact.ManagementTestSim qualified as TransactManagementSim
import DBOS.Transact.MessageTest qualified as TransactMessage
import DBOS.Transact.MessageTestSim qualified as TransactMessageSim
import DBOS.Transact.QueueTest qualified as TransactQueue
import DBOS.Transact.RegistryTest qualified as TransactRegistry
import DBOS.Transact.SelectTest qualified as TransactSelect
import DBOS.Transact.SerializationTest qualified as TransactSerialization
import DBOS.Transact.SimTest qualified as TransactSim
import DBOS.Transact.SleepTest qualified as TransactSleep
import DBOS.Transact.SleepTestSim qualified as TransactSleepSim
import DBOS.Transact.StepRetryTest qualified as TransactStepRetry
import DBOS.Transact.StepTest qualified as TransactStep
import DBOS.Transact.StepTestSim qualified as TransactStepSim
import DBOS.Transact.WaitTest qualified as TransactWait
import DBOS.Transact.WaitTestSim qualified as TransactWaitSim
import DBOS.Transact.WorkflowTest qualified as TransactWorkflow
import DBOS.Transact.WorkflowTestSim qualified as TransactWorkflowSim
import System.IO.Silently (capture)
import Test.Tasty (TestTree, defaultIngredients, defaultMain, testGroup)
import Test.Tasty.Ingredients (tryIngredients)
import Test.Tasty.Options (OptionSet)

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
--- $> tasty TransactHandleSim.tests
--- $> tasty DBOS.Transact.DeadlinesTest.tests
--- $> tasty DBOS.Transact.ErrorTest.tests
--- $> tasty DBOS.Transact.EventTest.tests
--- $> tasty DBOS.Transact.HandleTest.tests
--- $> tasty DBOS.Transact.IdentityTest.tests
--- $> tasty DBOS.Transact.InstanceTest.tests
--- $> tasty DBOS.Transact.ManagementTest.tests
--- $> tasty TransactManagementSim.tests
--- $> tasty DBOS.Transact.MessageTest.tests
--- $> tasty TransactMessageSim.tests
--- $> tasty DBOS.Transact.QueueTest.tests
--- $> tasty DBOS.Transact.RegistryTest.tests
--- $> tasty DBOS.Transact.SelectTest.tests
--- $> tasty DBOS.Transact.SerializationTest.tests
--- $> tasty DBOS.Transact.SimTest.tests
--- $> tasty DBOS.Transact.SleepTest.tests
--- $> tasty TransactSleepSim.tests
--- $> tasty DBOS.Transact.StepRetryTest.tests
--- $> tasty DBOS.Transact.StepTest.tests
--- $> tasty TransactStepSim.tests
--- $> tasty DBOS.Transact.WaitTest.tests
--- $> tasty TransactWaitSim.tests
-- $> tasty DBOS.Transact.WorkflowTest.tests
--- $> tasty TransactWorkflowSim.tests
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
    [ SystemDBError.tests
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
    , TransactError.tests
    , TransactEvent.tests
    , TransactHandle.tests
    , TransactIdentity.tests
    , TransactInstance.tests
    , TransactManagement.tests
    , TransactMessage.tests
    , TransactQueue.tests
    , TransactRegistry.tests
    , TransactSelect.tests
    , TransactSerialization.tests
    , TransactSleep.tests
    , TransactStep.tests
    , TransactStepRetry.tests
    , TransactWait.tests
    , TransactWorkflow.tests
    ]

-- | Sim-backed trees for the watcher eval only: never add these to 'tests'
-- ('main' runs live backends only). Referenced here so the qualified import
-- above resolves the @-- $>@ toggle without going redundant.
simTests :: [TestTree]
simTests =
  [ TransactSim.tests
  , TransactContextSim.tests
  , TransactHandleSim.tests
  , TransactManagementSim.tests
  , TransactMessageSim.tests
  , TransactSleepSim.tests
  , TransactStepSim.tests
  , TransactWaitSim.tests
  , TransactWorkflowSim.tests
  ]
