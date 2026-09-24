module Main (main, tasty) where

import Control.Exception (evaluate, try)
import DBOS.CodecTest qualified as Codec
import DBOS.LogTest qualified as Log
import DBOS.SchemaTest qualified as Schema
import DBOS.SimTest qualified as Sim
import DBOS.StarterTest qualified as Starter
import DBOS.SystemDB.ErrorTest qualified as SystemDBError
import DBOS.SystemDB.RetryTest qualified as SystemDBRetry
import DBOS.SystemDB.TypesTest qualified as SystemDBTypes
import DBOS.SystemDBHasqlTest qualified as SystemDBHasql
import DBOS.SystemDBTest qualified as SystemDB
import DBOS.TransactTest qualified as Transact
import Test.Tasty (TestTree, defaultIngredients, defaultMain, testGroup)
import Test.Tasty.Ingredients (tryIngredients)
import System.IO.Silently (capture)
import Test.Tasty.Options (OptionSet)

--- $> tasty tests
-- Toggle: one dash enables, three dashes disables (`--- $>`).
-- Needs-live-DB groups marked *.
--- $> tasty DBOS.TransactTest.tests
--- $> tasty DBOS.CodecTest.tests
--- $> tasty DBOS.LogTest.tests
--- $> tasty DBOS.SimTest.tests
--- $> tasty DBOS.StarterTest.tests -- *
--- $> tasty DBOS.SystemDBTest.tests
-- $> tasty DBOS.SystemDB.TypesTest.tests
--- $> tasty DBOS.SystemDB.ErrorTest.tests
--- $> tasty DBOS.SystemDB.RetryTest.tests
--- $> tasty DBOS.SystemDBHasqlTest.tests -- *
--- $> tasty DBOS.SchemaTest.tests -- *
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
        Nothing -> pure False
        Just runTests -> runTests
    readGhcid = do
      content <- try (readFile "ghcid.txt") :: IO (Either IOError String)
      case content of
        Left _ -> pure ""
        Right text -> evaluate (length text) >> pure text

tests :: TestTree
tests =
  testGroup
    "dbos-transact-hs"
    [ Transact.tests,
      Codec.tests,
      Log.tests,
      Sim.tests,
      Starter.tests,
      SystemDB.tests,
      SystemDBTypes.tests,
      SystemDBError.tests,
      SystemDBRetry.tests,
      SystemDBHasql.tests,
      Schema.tests
    ]
