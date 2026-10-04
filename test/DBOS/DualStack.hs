{-# LANGUAGE RankNTypes #-}

-- | The dual-stack test frame: one runner helper per backend for leaves that
-- share a scenario body and a pure check.
--
-- The shared scenario carries the polymorphic effect constraints; the runner
-- carries the top-level interpretation — IO with Tasty on the live side,
-- @IOSim@ with a typed trace assertion on the sim side. Fixtures stay
-- domain-local; only the runners are shared.
module DBOS.DualStack
  ( liveCase,
    liveCaseWith,
    simCase,
  )
where

import Control.Monad.IOSim (IOSim, SimTrace)
import DBOS.IOSimTracer (runSimCase)
import Prelude
import Test.Tasty (TestTree)
import Test.Tasty.HUnit (testCase)

-- | One live leaf over a shared-resource fixture: build the fixture per leaf
-- from the group's resource, drive the shared scenario, judge by the shared
-- check. The leaf does not release the resource.
liveCase :: IO fixture -> String -> (fixture -> IO a) -> (a -> Either String ()) -> TestTree
liveCase buildFixture name scen check =
  testCase name (buildFixture >>= scen >>= either fail pure . check)

-- | One live leaf over a per-case fixture: acquire and release (bracket)
-- around the leaf, drive the shared scenario, judge by the shared check.
liveCaseWith :: (forall b. (fixture -> IO b) -> IO b) -> String -> (fixture -> IO a) -> (a -> Either String ()) -> TestTree
liveCaseWith withFixture name scen check =
  testCase name (withFixture scen >>= either fail pure . check)

-- | One sim leaf: build the sim fixture, drive the shared scenario through
-- @runSimCase@, judge the value by the shared check and the trace by the
-- caller's assertions. Nothing prints from here: the watcher stays quiet and
-- the trace speaks through types, not lines. The mirror of 'liveCase'.
simCase ::
  (forall s. IOSim s (fixture (IOSim s))) ->
  String ->
  (forall s. fixture (IOSim s) -> IOSim s a) ->
  (a -> Either String ()) ->
  (forall x. SimTrace x -> IO ()) ->
  TestTree
simCase simFixture name scen check traceCheck = testCase name $ do
  (out, tr) <- runSimCase (simFixture >>= scen)
  either fail pure (check out)
  traceCheck tr
