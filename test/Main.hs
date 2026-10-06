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
import DBOS.Transact.CheckpointTest qualified as CheckpointTest
import DBOS.Transact.CheckpointTestSim qualified as CheckpointSim
import DBOS.Transact.ClientTest qualified as ClientTest
import DBOS.Transact.ConfigTest qualified as ConfigTest
import DBOS.Transact.ContextTest qualified as ContextTest
import DBOS.Transact.ContextTestSim qualified as ContextSim
import DBOS.Transact.DatasourceTest qualified as DatasourceTest
import DBOS.Transact.DatasourceTestSim qualified as DatasourceSim
import DBOS.Transact.QueueTestSim qualified as QueueSim
import DBOS.Transact.WidgetSim qualified as WidgetSim
import DBOS.Transact.WidgetTest qualified as WidgetTest
import DBOS.Transact.DeadlinesTest qualified as DeadlinesTest
import DBOS.Transact.DeadlinesTestSim qualified as DeadlinesSim
import DBOS.Transact.ErrorTest qualified as ErrorTest
import DBOS.Transact.EventTest qualified as EventTest
import DBOS.Transact.EventTestSim qualified as EventSim
import DBOS.Transact.HandleTest qualified as HandleTest
import DBOS.Transact.HandleTestSim qualified as HandleSim
import DBOS.Transact.IdentityTest qualified as IdentityTest
import DBOS.Transact.InstanceTest qualified as InstanceTest
import DBOS.Transact.ManagementTest qualified as ManagementTest
import DBOS.Transact.ManagementTestSim qualified as ManagementSim
import DBOS.Transact.MessageTest qualified as MessageTest
import DBOS.Transact.MessageTestSim qualified as MessageSim
import DBOS.Transact.QueueTest qualified as QueueTest
import DBOS.Transact.RegistryTest qualified as RegistryTest
import DBOS.Transact.SelectTest qualified as SelectTest
import DBOS.Transact.SelectTestSim qualified as SelectSim
import DBOS.Transact.SerializationTest qualified as SerializationTest
import DBOS.Transact.SimTest qualified as SimTest
import DBOS.Transact.SleepTest qualified as SleepTest
import DBOS.Transact.SleepTestSim qualified as SleepSim
import DBOS.Transact.StepRetryTest qualified as StepRetryTest
import DBOS.Transact.StepRetryTestSim qualified as StepRetrySim
import DBOS.Transact.StepTest qualified as StepTest
import DBOS.Transact.StepTestSim qualified as StepSim
import DBOS.Transact.WaitTest qualified as WaitTest
import DBOS.Transact.WaitTestSim qualified as WaitSim
import DBOS.Transact.WorkflowTest qualified as WorkflowTest
import DBOS.Transact.WorkflowTestSim qualified as WorkflowSim
import System.IO.Silently (capture)
import Test.Tasty (TestTree, defaultIngredients, defaultMain, testGroup)
import Test.Tasty.Ingredients (tryIngredients)
import Test.Tasty.Options (OptionSet)

--- $> tasty SystemDBError.tests
--- $> tasty SystemDBIOSim.tests
--- $> tasty SystemDBNotifier.tests
--- $> tasty SystemDBNotify.tests
--- $> tasty SystemDBPostgres.tests
--- $> tasty SystemDBRetry.tests
--- $> tasty SystemDBTypes.tests
--- $> tasty Tracer.tests
--- $> tasty CheckpointTest.tests
--- $> tasty CheckpointSim.tests
--- $> tasty ClientTest.tests
--- $> tasty ConfigTest.tests
--- $> tasty ContextTest.tests
--- $> tasty ContextSim.tests
--- $> tasty DatasourceTest.tests
--- $> tasty DatasourceSim.tests
-- $> tasty QueueSim.tests
--- $> tasty WidgetSim.tests
--- $> tasty WidgetTest.tests
--- $> tasty DeadlinesTest.tests
--- $> tasty DeadlinesSim.tests
--- $> tasty ErrorTest.tests
--- $> tasty EventTest.tests
--- $> tasty EventSim.tests
--- $> tasty HandleTest.tests
--- $> tasty HandleSim.tests
--- $> tasty IdentityTest.tests
--- $> tasty InstanceTest.tests
--- $> tasty ManagementTest.tests
--- $> tasty ManagementSim.tests
--- $> tasty MessageTest.tests
--- $> tasty MessageSim.tests
-- $> tasty QueueTest.tests
--- $> tasty RegistryTest.tests
--- $> tasty SelectTest.tests
--- $> tasty SelectSim.tests
--- $> tasty SerializationTest.tests
--- $> tasty SimTest.tests
--- $> tasty SleepTest.tests
--- $> tasty SleepSim.tests
--- $> tasty StepRetryTest.tests
--- $> tasty StepRetrySim.tests
--- $> tasty StepTest.tests
--- $> tasty StepSim.tests
--- $> tasty WaitTest.tests
--- $> tasty WaitSim.tests
--- $> tasty WorkflowTest.tests
--- $> tasty WorkflowSim.tests
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
    , CheckpointTest.tests
    , CheckpointSim.tests
    , ClientTest.tests
    , ConfigTest.tests
    , ContextTest.tests
    , DatasourceTest.tests
    , WidgetTest.tests
    , DeadlinesTest.tests
    , ErrorTest.tests
    , EventTest.tests
    , HandleTest.tests
    , IdentityTest.tests
    , InstanceTest.tests
    , ManagementTest.tests
    , MessageTest.tests
    , QueueTest.tests
    , RegistryTest.tests
    , SelectTest.tests
    , SerializationTest.tests
    , SleepTest.tests
    , StepTest.tests
    , StepRetryTest.tests
    , StepRetrySim.tests
    , WaitTest.tests
    , WorkflowTest.tests
    ]

-- | Sim-backed trees for the watcher eval only: never add these to 'tests'
-- ('main' runs live backends only). Referenced here so the qualified import
-- above resolves the @-- $>@ toggle without going redundant.
simTests :: [TestTree]
simTests =
  [ SimTest.tests
  , ContextSim.tests
  , EventSim.tests
  , HandleSim.tests
  , ManagementSim.tests
  , MessageSim.tests
  , SleepSim.tests
  , StepSim.tests
  , WaitSim.tests
  , DatasourceSim.tests
  , QueueSim.tests
  , WidgetSim.tests
  , WorkflowSim.tests
  , SelectSim.tests
  , DeadlinesSim.tests
  ]
