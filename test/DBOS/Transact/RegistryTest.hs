{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Public-behavior tests for the @registry.rs@ identity seam. Scenarios
-- and checks live in 'DBOS.Transact.RegistryCases' and run here over
-- Postgres connections (and in 'DBOS.Transact.RegistryTestSim' over the
-- mock backend). Registry checks never reach the database, but invocation
-- runs through a workflow view, which needs a real connection on this
-- stack.
module DBOS.Transact.RegistryTest (tests) where

import DBOS.DualStack (liveCase)
import DBOS.Prelude
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact.ContextTest (connOver)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Logger (nullTracer)
import DBOS.Transact.RegistryCases
  ( RegistryFixture (..),
    checkDuplicateRefRefused,
    checkDuplicateRefused,
    checkEmptyNamesResolve,
    checkErasedRoundTrip,
    checkInstanceDistinct,
    checkKeyTriple,
    checkMalformedReported,
    checkMethodDistinct,
    checkReferenceIdentity,
    checkSnapshotReopen,
    checkZeroArgCall,
    scenarioDuplicateRefRefused,
    scenarioDuplicateRefused,
    scenarioEmptyNamesResolve,
    scenarioErasedRoundTrip,
    scenarioInstanceDistinct,
    scenarioKeyTriple,
    scenarioMalformedReported,
    scenarioMethodDistinct,
    scenarioReferenceIdentity,
    scenarioSnapshotReopen,
    scenarioZeroArgCall,
  )
import Test.Tasty (TestTree, testGroup, withResource)

-- | The application identity the scoped registry probes install.
registryLiveIdentity :: Identity
registryLiveIdentity =
  Identity
    { identityAppName = "test-app",
      identityAppVersion = "1.0.0",
      identityExecutorId = "test-executor",
      identityAppId = ""
    }

-- | One backend for the whole group: invocations build real connections
-- over it, though registry checks never reach the database.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | Every case runs against a fresh connection over the suite backend.
liveFixture :: Postgres.PostgresSystemDB -> RegistryFixture IO
liveFixture backend =
  RegistryFixture
    { rfConn = connOver backend nullTracer,
      rfIdentity = registryLiveIdentity
    }

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    testGroup
      "Workflow registry"
      [ liveCase (liveFixture <$> getBackend) "a key displays as the triple the references spell" scenarioKeyTriple checkKeyTriple,
        liveCase (liveFixture <$> getBackend) "one identity registers once" scenarioDuplicateRefused checkDuplicateRefused,
        liveCase (liveFixture <$> getBackend) "a snapshot closes the registry and releasing it reopens" scenarioSnapshotReopen checkSnapshotReopen,
        liveCase (liveFixture <$> getBackend) "a name and an instance of it are different identities" scenarioInstanceDistinct checkInstanceDistinct,
        liveCase (liveFixture <$> getBackend) "empty class and config names resolve like nulls" scenarioEmptyNamesResolve checkEmptyNamesResolve,
        liveCase (liveFixture <$> getBackend) "an erased workflow round-trips through json" scenarioErasedRoundTrip checkErasedRoundTrip,
        liveCase (liveFixture <$> getBackend) "a malformed argument is reported rather than panicking" scenarioMalformedReported checkMalformedReported,
        liveCase (liveFixture <$> getBackend) "a class-bound key with no instance is its own identity" scenarioMethodDistinct checkMethodDistinct,
        liveCase (liveFixture <$> getBackend) "a reference holds the identity it registered under" scenarioReferenceIdentity checkReferenceIdentity,
        liveCase (liveFixture <$> getBackend) "a reference to a duplicate identity is refused" scenarioDuplicateRefRefused checkDuplicateRefRefused,
        liveCase (liveFixture <$> getBackend) "a zero argument workflow is called with no input at all" scenarioZeroArgCall checkZeroArgCall
      ]
