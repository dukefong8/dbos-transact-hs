{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Event behavior through the workflow-facing API and live SystemDB: the
-- shared 'EventCases' scenarios over a real 'PostgresSystemDB' with a
-- FastLogger tracer, for @main@.
module DBOS.Transact.EventTest (tests) where

import DBOS.DualStack (liveCase)
import DBOS.Prelude
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (WorkflowId (..))
import DBOS.SystemDB.Postgres (PostgresSystemDB)
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  ( Config (..),
    SomeTracer (..),
    acquireLoggerBackend,
    configFromEnv,
    ioTracer,
    nullTracer,
  )
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Connection (SomeSystemDB (..), uuidWorkflowId)
import DBOS.SystemDB.Retry (uuidEntropy)
import DBOS.Transact.EventCases
  ( EventFixture (..),
    checkCapturedRead,
    checkCheckpointedRead,
    checkOutOfOrderIds,
    checkPublishReplay,
    checkRecoveryKeepsFirst,
    checkRefusedSet,
    checkReplayNoRepublish,
    checkWrongInstance,
    mkEventFixture,
    scenarioCapturedRead,
    scenarioCheckpointedRead,
    scenarioOutOfOrderIds,
    scenarioPublishReplay,
    scenarioRecoveryKeepsFirst,
    scenarioRefusedSet,
    scenarioReplayNoRepublish,
    scenarioWrongInstance,
  )
import Test.Tasty (TestTree, testGroup, withResource)

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted. The
-- recovery case below keeps its own launched instance: it needs a distinct
-- application identity per execution.
acquireSuiteBackend :: IO PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | The shared tree over a real backend: every test owns its rows via
-- fresh UUIDs (application, executor, workflow id). The tracer arrives
-- as a parameter — FastLogger here, the sim carrier in
-- 'DBOS.Transact.EventTestSim' — and launches go through it over an
-- explicitly built connection, so each scenario drives the same engine
-- calls on both stacks.
liveEventFixture :: IO PostgresSystemDB -> IO (SomeTracer IO) -> IO (EventFixture IO)
liveEventFixture getBackend getTracer = do
  fresh <- UUID.V4.nextRandom
  let suffix = Text.pack (UUID.toString fresh)
      appName = "hs-l2-event-" <> Text.take 12 suffix
      otherName = "hs-l2-event-other-" <> Text.take 12 suffix
  config0 <- configFromEnv appName
  otherConfig0 <- configFromEnv otherName
  let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
      otherConfig = otherConfig0 {configAppVersion = Just ("other-v-" <> suffix), configExecutorId = Just ("other-exec-" <> suffix)}
      identity =
        Identity
          { identityAppName = appName,
            identityAppVersion = "v-" <> suffix,
            identityExecutorId = "exec-" <> suffix,
            identityAppId = ""
          }
      otherIdentity =
        Identity
          { identityAppName = otherName,
            identityAppVersion = "other-v-" <> suffix,
            identityExecutorId = "other-exec-" <> suffix,
            identityAppId = ""
          }
  backend <- getBackend
  tracer <- getTracer
  mkEventFixture
    config
    otherConfig
    identity
    otherIdentity
    appName
    (\prefix -> WorkflowId ("hs-l2-" <> prefix <> "-" <> suffix))
    uuidWorkflowId
    uuidEntropy
    (SomeSystemDB backend)
    tracer

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    withResource acquireLoggerBackend snd $ \getLogger ->
      testGroup
        "Workflow events"
        [ liveCase (liveEventFixture getBackend (ioTracer . fst <$> getLogger)) "a workflow can publish and replay an event" scenarioPublishReplay checkPublishReplay,
          liveCase (liveEventFixture getBackend (ioTracer . fst <$> getLogger)) "a reading workflow is checkpointed and a reading step is not" scenarioCheckpointedRead checkCheckpointedRead,
          liveCase (liveEventFixture getBackend (ioTracer . fst <$> getLogger)) "a refused set event spends no step id" scenarioRefusedSet checkRefusedSet,
          liveCase (liveEventFixture getBackend (ioTracer . fst <$> getLogger)) "a getEvent through a captured parent is plain and moves no ids" scenarioCapturedRead checkCapturedRead,
          liveCase (liveEventFixture getBackend (ioTracer . fst <$> getLogger)) "a replayed set event does not republish" scenarioReplayNoRepublish checkReplayNoRepublish,
          liveCase (liveEventFixture getBackend (ioTracer . fst <$> getLogger)) "progress events survive recovery without republishing" scenarioRecoveryKeepsFirst checkRecoveryKeepsFirst,
          liveCase (liveEventFixture getBackend (ioTracer . fst <$> getLogger)) "reading through another instance from inside a workflow is refused" scenarioWrongInstance checkWrongInstance,
          liveCase (liveEventFixture getBackend (ioTracer . fst <$> getLogger)) "library calls driven out of build order keep the ids they were built with" scenarioOutOfOrderIds checkOutOfOrderIds
        ]
