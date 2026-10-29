{-# LANGUAGE OverloadedStrings #-}

-- | Durable sleep, mirroring Rust @tests/sleep.rs@: the wait is checkpointed,
-- a replay adopts the recorded wake time, and a sleep outside a workflow
-- waits plainly. Scenarios and checks are shared with the sim tree
-- ('DBOS.Transact.SleepTestSim'); this module owns the Postgres factory.
module DBOS.Transact.SleepTest (tests) where

import DBOS.DualStack (liveCaseWith)
import DBOS.Prelude
import DBOS.Transact.SleepCases
  ( SleepFixture (..),
    checkSleepCheckpoint,
    checkSleepInStep,
    checkSleepPlain,
    checkSleepRefused,
    checkSleepRefusedRecorded,
    checkSleepReplay,
    scenarioSleepCheckpoint,
    scenarioSleepInStep,
    scenarioSleepPlain,
    scenarioSleepRefused,
    scenarioSleepRefusedRecorded,
    scenarioSleepReplay,
  )
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (NewWorkflow (..), WorkflowId (..), WorkflowOwner (..), WorkflowRecord (workflowRecordCreatorXid), newWorkflow, sleepStepName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  (
  sleepPlain,
  sleepStep,
  )
import DBOS.Transact.Logger (acquireLoggerBackend, ioTracer, nullTracer)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Context (firstStepStatus, nextStepId, nextWorkflowMarker, peekStepId, withStep, withWorkflow)
import DBOS.Transact.ContextTest (connOver)
import Test.Tasty (TestTree, testGroup, withResource)

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    let leaf :: String -> (SleepFixture IO -> IO a) -> (a -> Either String ()) -> TestTree
        leaf = liveCaseWith (\run -> getBackend >>= \backend -> mkSleepFixture backend run)
     in testGroup
          "Durable sleep"
          [ leaf "a sleep waits and is checkpointed" scenarioSleepCheckpoint checkSleepCheckpoint,
            leaf "a replayed sleep does not start its clock again" scenarioSleepReplay checkSleepReplay,
            leaf "a sleep outside a workflow waits plainly" scenarioSleepPlain checkSleepPlain,
            leaf "a sleep inside a step takes no id" scenarioSleepInStep checkSleepInStep,
            leaf "a sleep that cannot be recorded is refused before it takes an id" scenarioSleepRefused checkSleepRefused,
            leaf "a refused sleep is recorded as the step error and replayed" scenarioSleepRefusedRecorded checkSleepRefusedRecorded
          ]

-- | One fixture per leaf over the suite backend: a fresh workflow id per
-- scenario, every run announcing through the leaf's FastLogger backend (so
-- the replay run proves the trace seam as well as the wake it waits until).
mkSleepFixture :: Postgres.PostgresSystemDB -> (SleepFixture IO -> IO a) -> IO a
mkSleepFixture backend run = do
  (logger, cleanup) <- acquireLoggerBackend
  let -- Sleeps present the row's own creator token, as an execution built
      -- from the init result would.
      ownerFor workflowText = do
        fetched <- SystemDB.getWorkflow backend (WorkflowId workflowText)
        case fetched of
          Right (Just row) -> case row.workflowRecordCreatorXid of
            Just token -> pure (WorkflowOwner {ownerWorkflowId = WorkflowId workflowText, ownerToken = token})
            Nothing -> fail "sleep fixture row has no creator token"
          _ -> fail "sleep fixture built a context for a row that was never initialized"
      fixture =
        SleepFixture
          { sfFreshWorkflowId = do
              freshId <- UUID.V4.nextRandom
              let workflowText = "hs-l2-sleep-" <> Text.pack (UUID.toString freshId)
              created <- SystemDB.initWorkflow backend ((newWorkflow workflowText) {newWorkflowName = Just "L2SleepTest"}) Nothing
              case created of
                Left err -> fail (show err)
                Right _ -> pure (WorkflowId workflowText),
            sfRunSleep = \wid@(WorkflowId widText) duration -> do
              conn <- connOver backend (ioTracer logger)
              owner <- ownerFor widText
              withWorkflow conn sleepTestIdentity wid Nothing owner (\wctx -> sleepStep wctx duration),
            sfCheckSleep = \wid ->
              SystemDB.checkStep backend wid 0 sleepStepName >>= either (fail . show) pure,
            sfPlainSleep = sleepPlain,
            sfRefusedSleep = \wid@(WorkflowId widText) duration -> do
              conn <- connOver backend (ioTracer logger)
              owner <- ownerFor widText
              withWorkflow conn sleepTestIdentity wid Nothing owner $ \wctx -> do
                refused <- sleepStep wctx duration
                next <- peekStepId wctx
                pure (refused, next),
            sfRunInStep = \wid@(WorkflowId widText) duration -> do
              conn <- connOver backend (ioTracer logger)
              owner <- ownerFor widText
              withWorkflow conn sleepTestIdentity wid Nothing owner $ \wctx -> do
                marker <- nextWorkflowMarker wctx
                withStep wctx marker (firstStepStatus 0) $ \_stepped -> do
                  before <- nextStepId wctx
                  slept <- sleepStep wctx duration
                  after <- nextStepId wctx
                  pure (slept, before, after)
          }
  run fixture `finally` cleanup

-- | The application identity the scoped sleep cases install.
sleepTestIdentity :: Identity
sleepTestIdentity =
  Identity
    { identityAppName = "test-app",
      identityAppVersion = "1.0.0",
      identityExecutorId = "test-executor",
      identityAppId = ""
    }
