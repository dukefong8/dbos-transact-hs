{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | The shared 'RegistryTest' scenarios over 'MockSystemDB': the same
-- cases as the live tree, judged by the same checks, with the sim-only
-- typed trace assertions staying in this module. Registry work is
-- STM-only — registration, snapshot, lookup, invocation through a
-- workflow view, and the codec boundary all run without touching the
-- database — so every leaf also pins the trace silent: the sim tree
-- validates the same deep seams as the live tree, not just its values.
module DBOS.Transact.RegistryTestSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (simTracer)
import DBOS.SystemDB.IOSim (simConnectionWith)
import DBOS.Transact.Connection (Connection)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.RegistryCases
  ( RegistryFixture (..),
    checkDuplicateRefRefused,
    checkDuplicateRefused,
    checkEmptyNamesResolve,
    checkErasedRoundTrip,
    checkInstanceDistinct,
    checkKeyTriple,
    checkMalformedReported,
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
    scenarioReferenceIdentity,
    scenarioSnapshotReopen,
    scenarioZeroArgCall,
  )
import DBOS.Transact.Step (WorkflowEvent (..))
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=))

simIdentity :: Identity
simIdentity =
  Identity
    { identityAppName = "sim-app",
      identityAppVersion = "0.0.0",
      identityExecutorId = "sim-executor",
      identityAppId = ""
    }

-- | The sim backend wrapped in a connection carrying the io-sim tracer.
-- A local copy is deliberate: sibling sim trees repeat the builders they
-- need.
simConnection :: IOSim s (Connection (IOSim s))
simConnection = simConnectionWith simTracer

-- | Every case runs against a fresh mock connection; each use
-- instantiates the fixture at its own simulation.
simFixture :: forall s. IOSim s (RegistryFixture (IOSim s))
simFixture =
  pure
    ( RegistryFixture
        { rfConn = simConnection,
          rfIdentity = simIdentity
        }
    )

-- | Registry work announces nothing: no launch, no steps, no outcomes.
traceSilent :: forall x. SimTrace x -> IO ()
traceSilent tr = selectTraceEventsDynamic tr @?= ([] :: [WorkflowEvent])

tests :: TestTree
tests =
  testGroup
    "Workflow registry (Sim)"
    [ simCase simFixture "a key displays as the triple the references spell" scenarioKeyTriple checkKeyTriple traceSilent,
      simCase simFixture "one identity registers once" scenarioDuplicateRefused checkDuplicateRefused traceSilent,
      simCase simFixture "a snapshot closes the registry and releasing it reopens" scenarioSnapshotReopen checkSnapshotReopen traceSilent,
      simCase simFixture "a name and an instance of it are different identities" scenarioInstanceDistinct checkInstanceDistinct traceSilent,
      simCase simFixture "empty class and config names resolve like nulls" scenarioEmptyNamesResolve checkEmptyNamesResolve traceSilent,
      simCase simFixture "an erased workflow round-trips through json" scenarioErasedRoundTrip checkErasedRoundTrip traceSilent,
      simCase simFixture "a malformed argument is reported rather than panicking" scenarioMalformedReported checkMalformedReported traceSilent,
      simCase simFixture "a reference holds the identity it registered under" scenarioReferenceIdentity checkReferenceIdentity traceSilent,
      simCase simFixture "a reference to a duplicate identity is refused" scenarioDuplicateRefRefused checkDuplicateRefRefused traceSilent,
      simCase simFixture "a zero argument workflow is called with no input at all" scenarioZeroArgCall checkZeroArgCall traceSilent
    ]
