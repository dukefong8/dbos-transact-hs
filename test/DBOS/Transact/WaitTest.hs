{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | In-workflow waits against the live backend, ported from Rust
-- @tests/waits.rs@: an empty first wait records its refusal and a replay
-- reads it back, a recorded winner that left the set is refused, and a
-- replayed first wait reads its recorded winner back. The executor-level
-- waits run through a launched instance over the same rows. Scenarios and
-- checks live in 'DBOS.Transact.WaitCases' and run here over Postgres rows
-- (and in 'DBOS.Transact.WaitTestSim' over the in-memory backend).
module DBOS.Transact.WaitTest (tests) where

import DBOS.DualStack (liveCaseWith)
import DBOS.Prelude
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (NewWorkflow (..), Outcome (..), Submission (..), WorkflowId (..), newWorkflow, selectStepName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact.Logger (acquireLoggerBackend, ioTracer, nullTracer)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact (Config (..), DBOS, Environment (..), configFromEnv, launchWithEnvironment, newDBOS, shutdown, waitForFirstWorkflow, waitForWorkflow, waitForWorkflows)
import DBOS.Transact.Context (withWorkflow)
import DBOS.Transact.ContextTest (connOver)
import DBOS.Transact.WaitCases
  ( WaitFixture (..),
    checkCancelled,
    checkEmptyAll,
    checkExecAllEmpty,
    checkExecAllSettled,
    checkExecFirstEmpty,
    checkExecFirstWinner,
    checkExecSingle,
    checkJoinLast,
    checkJoinSettled,
    checkRefusal,
    checkRepeated,
    checkReplayWinner,
    checkSelectFirst,
    checkSelectSettledFirst,
    checkWinnerLeft,
    scenarioCancelled,
    scenarioEmptyAll,
    scenarioExecAllEmpty,
    scenarioExecAllSettled,
    scenarioExecFirstEmpty,
    scenarioExecFirstWinner,
    scenarioExecSingle,
    scenarioJoinLast,
    scenarioJoinSettled,
    scenarioRefusal,
    scenarioRepeated,
    scenarioReplayWinner,
    scenarioSelectFirst,
    scenarioSelectSettledFirst,
    scenarioWinnerLeft,
    WaitExecFixture (..),
  )
import Test.Tasty (TestTree, testGroup, withResource)

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

waitTestIdentity :: Identity
waitTestIdentity =
  Identity
    { identityAppName = "test-app",
      identityAppVersion = "1.0.0",
      identityExecutorId = "test-executor",
      identityAppId = ""
    }

-- | One fixture per leaf over the suite backend: labeled fresh ids per
-- scenario, every run announcing through the leaf's FastLogger backend
-- (so the replay run proves the trace seam as well as the winner it
-- reads back).
mkWaitFixture :: Postgres.PostgresSystemDB -> (WaitFixture IO -> IO a) -> IO a
mkWaitFixture backend run = do
  (logger, cleanup) <- acquireLoggerBackend
  let fixture =
        WaitFixture
          { wfFreshWorkflowId = \label -> do
              freshId <- UUID.V4.nextRandom
              let workflowText = "hs-l2-wait-" <> label <> "-" <> Text.pack (UUID.toString freshId)
              created <- SystemDB.initWorkflow backend ((newWorkflow workflowText) {newWorkflowName = Just "L2WaitTest"}) Nothing Fresh Nothing
              case created of
                Left err -> fail (show err)
                Right _ -> pure (WorkflowId workflowText),
            wfCtx = \wid action -> do
              conn <- connOver backend (ioTracer logger)
              withWorkflow conn waitTestIdentity wid Nothing action,
            wfSettle = \wid ->
              SystemDB.recordWorkflowOutcome backend wid (OutcomeOutput (Just "null")) >>= either (fail . show) (const (pure ())),
            wfCancel = \wid ->
              SystemDB.cancelWorkflows backend [wid] False Nothing >>= either (fail . show) (const (pure ())),
            wfCheckStep = \wid ->
              SystemDB.checkStep backend wid 0 selectStepName >>= either (fail . show) pure
          }
  run fixture `finally` cleanup

-- | One launched instance per leaf for the executor-level waits: a fresh
-- identity per scenario, bracketed around the leaf like the queue
-- fixture, with workflow rows over the suite backend.
isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }

mkWaitExecFixture :: Postgres.PostgresSystemDB -> (WaitExecFixture IO -> IO a) -> IO a
mkWaitExecFixture backend run = do
  fresh <- UUID.V4.nextRandom
  let suffix = Text.pack (UUID.toString fresh)
      appName = "hs-l2-waitexec-" <> Text.take 12 suffix
  config0 <- configFromEnv appName
  let config =
        config0
          { configAppVersion = Just ("v-" <> suffix),
            configExecutorId = Just ("exec-" <> suffix)
          }
  bracket (newDBOS config) shutdown $ \dbos -> do
    started <- launchWithEnvironment dbos isolatedEnvironment
    case started of
      Left err -> fail (show err)
      Right _ ->
        run
          WaitExecFixture
            { weFreshWorkflowId = \label -> do
                freshId <- UUID.V4.nextRandom
                let workflowText = "hs-l2-waitexec-" <> label <> "-" <> Text.pack (UUID.toString freshId)
                created <- SystemDB.initWorkflow backend ((newWorkflow workflowText) {newWorkflowName = Just "L2WaitExecTest"}) Nothing Fresh Nothing
                case created of
                  Left err -> fail (show err)
                  Right _ -> pure (WorkflowId workflowText),
              weSettle = \wid ->
                SystemDB.recordWorkflowOutcome backend wid (OutcomeOutput (Just "null")) >>= either (fail . show) (const (pure ())),
              weFirst = waitForFirstWorkflow dbos,
              weAll = waitForWorkflows dbos,
              weSingle = waitForWorkflow dbos,
              weShutdown = shutdown dbos
            }

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    let leaf :: String -> (WaitFixture IO -> IO a) -> (a -> Either String ()) -> TestTree
        leaf name scen check = liveCaseWith (\run -> getBackend >>= \backend -> mkWaitFixture backend run) name scen check
        execLeaf :: String -> (WaitExecFixture IO -> IO a) -> (a -> Either String ()) -> TestTree
        execLeaf name scen check = liveCaseWith (\run -> getBackend >>= \backend -> mkWaitExecFixture backend run) name scen check
     in testGroup
          "Waits"
          [ leaf "an empty first wait records its refusal and a replay reads it back" scenarioRefusal checkRefusal,
            leaf "a recorded winner that left the set is refused" scenarioWinnerLeft checkWinnerLeft,
            leaf "an all-wait completes over a settled workflow" scenarioJoinSettled checkJoinSettled,
            leaf "select reports the first workflow to settle" scenarioSelectFirst checkSelectFirst,
            leaf "a settled first id wins over a pending set" scenarioSelectSettledFirst checkSelectSettledFirst,
            leaf "a replayed first-wait reads its recorded winner back" scenarioReplayWinner checkReplayWinner,
            leaf "a cancelled workflow counts as settled" scenarioCancelled checkCancelled,
            leaf "join returns when the last workflow settles" scenarioJoinLast checkJoinLast,
            leaf "an empty all-wait is satisfied and takes no step" scenarioEmptyAll checkEmptyAll,
            leaf "a repeated id is accepted by both waits" scenarioRepeated checkRepeated,
            execLeaf "an executor-level first-wait over nothing is refused" scenarioExecFirstEmpty checkExecFirstEmpty,
            execLeaf "an executor-level all-wait over nothing is satisfied" scenarioExecAllEmpty checkExecAllEmpty,
            execLeaf "an executor-level first-wait reports the settled member" scenarioExecFirstWinner checkExecFirstWinner,
            execLeaf "an executor-level all-wait over settled workflows completes" scenarioExecAllSettled checkExecAllSettled,
            execLeaf "an executor-level single wait reports the settled outcome" scenarioExecSingle checkExecSingle
          ]
