{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The step retry seam, mirroring Rust @tests/retries.rs@ and the timeout
-- cases of @tests/timeouts.rs@: attempts, backoff, the retry predicate,
-- single-checkpoint replay, per-attempt timeouts, and the two engine errors
-- (@StepTimeout@, @MaxStepRetriesExceeded@). Scenarios and checks live in
-- 'DBOS.Transact.StepRetryCases' and run here over Postgres rows (and in
-- 'DBOS.Transact.StepRetryTestSim' over the in-memory backend). Live
-- database.
module DBOS.Transact.StepRetryTest (tests) where

import DBOS.DualStack (liveCaseWith)
import DBOS.Prelude
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (NewWorkflow (..), Submission (..), WorkflowId (..), newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact.Logger (acquireLoggerBackend, ioTracer, nullTracer)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Context (withWorkflow)
import DBOS.Transact.ContextTest (connOver)
import DBOS.Transact.StepRetryCases
  ( StepRetryFixture (..),
    checkPlainNotPreemptible,
    checkPreemptible,
    checkRetryDeclined,
    checkRetryDefault,
    checkRetryExhausted,
    checkRetryMidDecline,
    checkRetryReplay,
    checkRetryThird,
    checkStepTimeout,
    checkStepWithinTimeout,
    checkTimeoutAllTimeout,
    checkTimeoutFreshRetry,
    checkTimeoutStopsBody,
    checkTokenDrop,
    checkTokenFirst,
    checkTokenQuiet,
    scenarioPlainNotPreemptible,
    scenarioPreemptible,
    scenarioRetryDeclined,
    scenarioRetryDefault,
    scenarioRetryExhausted,
    scenarioRetryMidDecline,
    scenarioRetryReplay,
    scenarioRetryThird,
    scenarioStepTimeout,
    scenarioStepWithinTimeout,
    scenarioTimeoutAllTimeout,
    scenarioTimeoutFreshRetry,
    scenarioTimeoutStopsBody,
    scenarioTokenDrop,
    scenarioTokenFirst,
    scenarioTokenQuiet,
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

stepTestIdentity :: Identity
stepTestIdentity =
  Identity
    { identityAppName = "test-app",
      identityAppVersion = "1.0.0",
      identityExecutorId = "test-executor",
      identityAppId = ""
    }

-- | One fixture per leaf over the suite backend: a fresh workflow id per
-- scenario, every run announcing through the leaf's FastLogger backend (so
-- the retry runs prove the trace seam as well as the attempts they make).
mkStepFixture :: Postgres.PostgresSystemDB -> (StepRetryFixture IO -> IO a) -> IO a
mkStepFixture backend run = do
  (logger, cleanup) <- acquireLoggerBackend
  let fixture =
        StepRetryFixture
          { srfFreshWorkflowId = do
              freshId <- UUID.V4.nextRandom
              let workflowText = "hs-l2-retry-" <> Text.pack (UUID.toString freshId)
              created <- SystemDB.initWorkflow backend ((newWorkflow workflowText) {newWorkflowName = Just "L2StepRetryTest"}) Nothing Fresh Nothing
              case created of
                Left err -> fail (show err)
                Right _ -> pure (WorkflowId workflowText),
            srfRun = \wid action -> do
              conn <- connOver backend (ioTracer logger)
              withWorkflow conn stepTestIdentity wid Nothing action,
            srfCancel = \wids ->
              SystemDB.cancelWorkflows backend wids False Nothing >>= either (fail . show) (const (pure ())),
            srfListSteps = \wid ->
              SystemDB.listSteps backend wid False Nothing Nothing Nothing >>= either (fail . show) pure
          }
  run fixture `finally` cleanup

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    let leaf :: String -> (StepRetryFixture IO -> IO a) -> (a -> Either String ()) -> TestTree
        leaf name scen check = liveCaseWith (\run -> getBackend >>= \backend -> mkStepFixture backend run) name scen check
     in testGroup
          "Step retries"
          [ leaf "a step that fails twice succeeds on the third attempt" scenarioRetryThird checkRetryThird,
            leaf "exhausted retries carry every attempt's failure" scenarioRetryExhausted checkRetryExhausted,
            leaf "the default does not retry and does not wrap" scenarioRetryDefault checkRetryDefault,
            leaf "a retried step replays from its single checkpoint" scenarioRetryReplay checkRetryReplay,
            leaf "a declined failure stops retrying immediately" scenarioRetryDeclined checkRetryDeclined,
            leaf "declining mid-policy keeps the earlier failures" scenarioRetryMidDecline checkRetryMidDecline,
            leaf "a step that hangs is stopped at its timeout" scenarioStepTimeout checkStepTimeout,
            leaf "a step within its timeout is unaffected" scenarioStepWithinTimeout checkStepWithinTimeout,
            leaf "a timed-out body stops rather than continuing" scenarioTimeoutStopsBody checkTimeoutStopsBody,
            leaf "a timed-out attempt is retried with a fresh timeout" scenarioTimeoutFreshRetry checkTimeoutFreshRetry,
            leaf "every attempt timing out reports each timeout" scenarioTimeoutAllTimeout checkTimeoutAllTimeout,
            leaf "a plain step is not preemptible" scenarioPlainNotPreemptible checkPlainNotPreemptible,
            leaf "a completed step leaves its token alone" scenarioTokenQuiet checkTokenQuiet,
            leaf "the cancellation token fires before the body is dropped" scenarioTokenFirst checkTokenFirst,
            leaf "a dropped step fires its cancellation token" scenarioTokenDrop checkTokenDrop,
            leaf "a preemptible step stops and records no outcome" scenarioPreemptible checkPreemptible
          ]
