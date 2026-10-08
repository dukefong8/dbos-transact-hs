{-# LANGUAGE OverloadedStrings #-}

-- | Public-behavior tests for the @checkpoint.rs@ placement seam.
-- Scenarios and checks live in 'DBOS.Transact.CheckpointCases' and run here
-- over Postgres-backed contexts (and in
-- 'DBOS.Transact.CheckpointTestSim' over the in-memory backend); placement
-- checks never reach the database on either stack.
module DBOS.Transact.CheckpointTest (tests) where

import DBOS.DualStack (liveCase)
import DBOS.Prelude
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact (Serializer (..), secondsDuration)
import DBOS.Transact.Logger (nullTracer)
import DBOS.Transact.Checkpoint (takenPlacement)
import DBOS.Transact.Connection
  ( Owner (..),
    SomeSystemDB (..),
    newConnection,
    uuidWorkflowId
  )
import DBOS.SystemDB.Retry (uuidEntropy)
import DBOS.Transact.CheckpointCases
  ( CheckpointFixture (..),
    checkBoundaryRecords,
    checkCapturedParent,
    checkClientPlain,
    checkInStepOwnBody,
    checkLeafRule,
    checkOutside,
    checkPlacementNames,
    checkRecordedDurable,
    checkRecordedRefused,
    checkSiblingRefused,
    checkTakenOther,
    scenarioBoundaryRecords,
    scenarioCapturedParent,
    scenarioClientPlain,
    scenarioInStepOwnBody,
    scenarioLeafRule,
    scenarioOutside,
    scenarioPlacementNames,
    scenarioRecordedDurable,
    scenarioRecordedRefused,
    scenarioSiblingRefused,
    scenarioTakenOther,
  )
import DBOS.Transact.ContextTest (ctxOver)
import Test.Tasty (TestTree, testGroup, withResource)

-- | One backend for the whole group: contexts build real connections
-- over it, though placement checks never reach the database.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | One fixture per leaf: contexts over the suite backend, and the
-- taken-placement probe over a second connection.
mkCheckpointFixture :: Postgres.PostgresSystemDB -> CheckpointFixture IO
mkCheckpointFixture backend =
  CheckpointFixture
    { ccfWithCtx = \run -> ctxOver backend nullTracer "wf-1" >>= run,
      ccfTakenOther = \ctx -> do
        otherId <- uuidWorkflowId
        otherConn <-
          newConnection
            (SomeSystemDB backend)
            RustSerde
            (Just "test-app")
            (secondsDuration 1)
            OwnerApplication
            otherId
            uuidWorkflowId
            uuidEntropy
            nullTracer
        takenPlacement otherConn "get_event" ctx
    }

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    let leaf :: String -> (CheckpointFixture IO -> IO a) -> (a -> Either String ()) -> TestTree
        leaf name scen check = liveCase (mkCheckpointFixture <$> getBackend) name scen check
     in testGroup
          "Checkpoint placement"
          [ leaf "outside a workflow takes no id and records nothing" scenarioOutside checkOutside,
            leaf "at a step boundary the call records under the allocated id" scenarioBoundaryRecords checkBoundaryRecords,
            leaf "a call built through a captured parent while a step body runs is plain" scenarioCapturedParent checkCapturedParent,
            leaf "a taken placement through a captured parent under another connection is plain" scenarioTakenOther checkTakenOther,
            leaf "inside a step body the call is plain by the leaf rule" scenarioLeafRule checkLeafRule,
            leaf "a recorded call polled at its boundary stays durable" scenarioRecordedDurable checkRecordedDurable,
            leaf "a recorded call carried into a step is refused" scenarioRecordedRefused checkRecordedRefused,
            leaf "a client's call stays plain wherever it is driven" scenarioClientPlain checkClientPlain,
            leaf "an in-step call polled in its own body stays plain" scenarioInStepOwnBody checkInStepOwnBody,
            leaf "an in-step call carried to a sibling body is refused" scenarioSiblingRefused checkSiblingRefused,
            leaf "only outside has no workflow around it" scenarioPlacementNames checkPlacementNames
          ]
