{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Public-behavior tests for the @handle.rs@ workflow handle: the shared
-- 'HandleCases' scenarios over a real 'PostgresSystemDB' with a FastLogger
-- tracer, for @main@.
module DBOS.Transact.HandleTest (tests) where

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
    configFromEnv,
  )
import DBOS.Transact.Logger (SomeTracer (..), acquireLoggerBackend, ioTracer, nullTracer)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Connection (SomeSystemDB (..), uuidWorkflowId)
import DBOS.SystemDB.Retry (uuidEntropy)
import DBOS.Transact.HandleCases
  ( HandleFixture (..),
    checkDeletedAbsent,
    checkDropHandle,
    checkFailError,
    checkResultAdopts,
    checkRetrieveStatus,
    checkScopedAwait,
    mkHandleFixture,
    scenarioDeletedAbsent,
    scenarioDropHandle,
    scenarioFailError,
    scenarioResultAdopts,
    scenarioRetrieveStatus,
    scenarioScopedAwait,
  )
import Test.Tasty (TestTree, testGroup, withResource)

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run.
acquireSuiteBackend :: IO PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | The shared tree over a real backend: every test owns its rows via
-- fresh UUIDs (application, executor, workflow id). The tracer arrives
-- as a parameter — FastLogger here, the sim carrier in
-- 'DBOS.Transact.HandleTestSim' — and the launch goes through it over an
-- explicitly built connection, so the scenario drives the same launch
-- call on both stacks.
liveHandleFixture :: IO PostgresSystemDB -> IO (SomeTracer IO) -> IO (HandleFixture IO)
liveHandleFixture getBackend getTracer = do
  fresh <- UUID.V4.nextRandom
  let suffix = Text.pack (UUID.toString fresh)
      appName = "hs-l2-handle-" <> Text.take 12 suffix
      appVersion = "hs-l2-handle-version-" <> suffix
      executorId = "hs-l2-handle-executor-" <> suffix
  config0 <- configFromEnv appName
  let config = config0 {configAppVersion = Just appVersion, configExecutorId = Just executorId}
      identity =
        Identity
          { identityAppName = appName,
            identityAppVersion = appVersion,
            identityExecutorId = executorId,
            identityAppId = ""
          }
  backend <- getBackend
  tracer <- getTracer
  mkHandleFixture
    config
    identity
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
        "Workflow handle"
        [ liveCase (liveHandleFixture getBackend (ioTracer . fst <$> getLogger)) "a retrieved handle names its workflow and reads its status" scenarioRetrieveStatus checkRetrieveStatus,
          liveCase (liveHandleFixture getBackend (ioTracer . fst <$> getLogger)) "a handle result adopts the recorded output" scenarioResultAdopts checkResultAdopts,
          liveCase (liveHandleFixture getBackend (ioTracer . fst <$> getLogger)) "a handle result reports the error a failed run recorded" scenarioFailError checkFailError,
          liveCase (liveHandleFixture getBackend (ioTracer . fst <$> getLogger)) "a handle over a deleted row reports its absence" scenarioDeletedAbsent checkDeletedAbsent,
          liveCase (liveHandleFixture getBackend (ioTracer . fst <$> getLogger)) "dropping a handle does not stop the workflow" scenarioDropHandle checkDropHandle,
          liveCase (liveHandleFixture getBackend (ioTracer . fst <$> getLogger)) "a scoped await records the child's result under the parent" scenarioScopedAwait checkScopedAwait
        ]
