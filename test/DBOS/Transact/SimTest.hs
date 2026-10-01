{-# LANGUAGE OverloadedStrings #-}

-- | Compile-and-run proof that the engine instantiates under @IOSim@ with
-- the sim backend stub. Running 'simDBOS' builds the sim connection (which
-- needs @instance SystemDB MockSystemDB (IOSim s)@), a real 'Tasks' set,
-- and the handle's strict MVars — all inside @IOSim@. No backend method is
-- called, so the @undefined@ methods are never reached.
--
-- Infrastructure, not a domain sim tree: it touches no database and owns
-- no fixture rows, so it correctly stays in @main@ while the @*Sim@
-- behavior mirrors stay eval-only.
module DBOS.Transact.SimTest (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, runSimOrThrow)
import DBOS.IOSimTracer (simTracer)
import DBOS.SystemDB.IOSim (simDBOSWith)
import DBOS.Transact (DBOS)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase)

-- | A @DBOS (IOSim s)@ already launched over the mock backend, carrying
-- the io-sim tracer. A local copy is deliberate: sibling sim trees repeat
-- the builders they need.
simDBOS :: IOSim s (DBOS (IOSim s))
simDBOS = simDBOSWith simTracer

tests :: TestTree
tests =
  testGroup
    "Sim backend"
    [ testCase "a DBOS handle instantiates under IOSim with the sim backend" $
        assertBool "simDBOS runs" (runSimOrThrow (simDBOS >> pure True))
    ]
