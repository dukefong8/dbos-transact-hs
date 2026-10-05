{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Workflow deadlines against the live backend, ported from Rust
-- @tests/deadlines.rs@: a run inside its budget is unaffected, and one
-- that outlives it is cancelled durably and reports the cancellation.
-- Scenarios and checks live in 'DBOS.Transact.DeadlinesCases' and run
-- here over Postgres rows with real launches (and in
-- 'DBOS.Transact.DeadlinesTestSim' over the in-memory backend with the
-- shared launch tail).
module DBOS.Transact.DeadlinesTest (tests) where

import DBOS.DualStack (liveCaseWith)
import DBOS.Prelude
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (WorkflowId (..), WorkflowRecord (..))
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  ( Config (..),
    DBOS,
    Environment (..),
    Executor,
    configFromEnv,
    launchWithEnvironment,
    newDBOS,
    nullTracer,
    shutdown,
  )
import DBOS.Transact.DeadlinesCases
  ( DeadlinesFixture (..),
    checkBeatenDeadline,
    checkKeptDeadline,
    checkPastDeadline,
    checkShutdownPending,
    checkWithinDeadline,
    scenarioBeatenDeadline,
    scenarioKeptDeadline,
    scenarioPastDeadline,
    scenarioShutdownPending,
    scenarioWithinDeadline,
  )
import Test.Tasty (TestTree, testGroup, withResource)

-- | Launch over the isolated environment and hand back the executor.
launchDeadlinesExec :: DBOS IO -> Environment -> IO (Executor IO)
launchDeadlinesExec dbos env = do
  started <- launchWithEnvironment dbos env
  case started of
    Left err -> fail (show err)
    Right executor -> pure executor

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted. The
-- launched instances below keep their own pools: each needs a distinct
-- application identity, minted per leaf.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | One fixture per leaf: a fresh instance over a fresh identity (bracketed
-- around the leaf), launched on demand per scenario, with row reads over
-- the suite backend so assertions hold after shutdown.
mkDeadlinesFixture :: Postgres.PostgresSystemDB -> (DeadlinesFixture IO -> IO a) -> IO a
mkDeadlinesFixture backend run = do
  fresh <- UUID.V4.nextRandom
  let suffix = Text.pack (UUID.toString fresh)
  config0 <- configFromEnv ("hs-l2-deadline-" <> Text.take 12 suffix)
  let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
  bracket (newDBOS config) shutdown $ \dbos ->
    run
      DeadlinesFixture
        { dfSetup = do
            widFresh <- UUID.V4.nextRandom
            let workflowText = "hs-l2-deadline-id-" <> Text.pack (UUID.toString widFresh)
            pure (dbos, WorkflowId workflowText),
          dfLaunch = \_ -> launchDeadlinesExec dbos isolatedEnvironment,
          dfReadStatus = \wid -> do
            found <- SystemDB.getWorkflow backend wid
            case found of
              Left err -> fail (show err)
              Right Nothing -> pure Nothing
              Right (Just record) -> pure (Just record.workflowRecordStatus),
          dfReadDeadline = \wid -> do
            found <- SystemDB.getWorkflow backend wid
            case found of
              Left err -> fail (show err)
              Right Nothing -> pure Nothing
              Right (Just record) -> pure record.workflowRecordDeadline
        }

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    let leaf :: String -> (DeadlinesFixture IO -> IO a) -> (a -> Either String ()) -> TestTree
        leaf name scen check = liveCaseWith (\run -> getBackend >>= \backend -> mkDeadlinesFixture backend run) name scen check
     in testGroup
          "Workflow deadlines"
          [ leaf "a workflow within its deadline is unaffected" scenarioWithinDeadline checkWithinDeadline,
            leaf "a workflow past its deadline is cancelled" scenarioPastDeadline checkPastDeadline,
            leaf "a recovered workflow keeps the deadline it already had" scenarioKeptDeadline checkKeptDeadline,
            leaf "shutdown does not durably cancel a workflow that has a deadline" scenarioShutdownPending checkShutdownPending,
            leaf "a deadline that loses to a recorded outcome reports that outcome" scenarioBeatenDeadline checkBeatenDeadline
          ]
