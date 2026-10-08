{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Public-behavior tests for the @select.rs@ checkpoint seam: what a
-- fresh select claims, what a recorded winner replays, and what a stale
-- winner earns. Scenarios and checks live in
-- 'DBOS.Transact.SelectCases' and run here over Postgres rows (and in
-- 'DBOS.Transact.SelectTestSim' over the in-memory backend). The race
-- itself is exercised end-to-end in 'DBOS.Transact.WorkflowTest'.
module DBOS.Transact.SelectTest (tests) where

import DBOS.DualStack (liveCase)
import DBOS.Prelude
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (NewWorkflow (..), Submission (..), WorkflowId (..), newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact.ContextTest (ctxOver)
import DBOS.Transact.Logger (nullTracer)
import DBOS.Transact.SelectCases
  ( SelectFixture (..),
    checkControlError,
    checkFreshWinner,
    checkStaleWinner,
    scenarioControlError,
    scenarioFreshWinner,
    scenarioStaleWinner,
  )
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (testCase)

-- | One backend for the whole group: contexts build real connections over
-- it, and the check/record pairs write step rows against it.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | One fixture per leaf: a fresh workflow row for the select to
-- checkpoint against (the check reads the row, so the row has to
-- exist), contexts over the suite backend, and the select step rows.
mkSelectFixture :: Postgres.PostgresSystemDB -> IO (SelectFixture IO)
mkSelectFixture backend = do
  freshId <- UUID.V4.nextRandom
  let workflowText = "hs-l2-select-" <> Text.pack (UUID.toString freshId)
  created <- SystemDB.initWorkflow backend ((newWorkflow workflowText) {newWorkflowName = Just "SelectTest"}) Nothing Fresh Nothing
  case created of
    Left err -> fail (show err)
    Right _ ->
      pure
        SelectFixture
          { scfRun = \action -> ctxOver backend nullTracer workflowText >>= action,
            scfListSteps = SystemDB.listSteps backend (WorkflowId workflowText) True Nothing Nothing Nothing >>= either (fail . show) pure
          }

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    testGroup
      "Durable select"
      [ liveCase (mkSelectFixture =<< getBackend) "a fresh select claims its id and records a winner" scenarioFreshWinner checkFreshWinner,
        liveCase (mkSelectFixture =<< getBackend) "a winner outside the branches that exist now is refused" scenarioStaleWinner checkStaleWinner,
        testCase "a control signal is not a race decision" (either fail pure (checkControlError scenarioControlError))
      ]
