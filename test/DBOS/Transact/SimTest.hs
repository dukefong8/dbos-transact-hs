{-# LANGUAGE OverloadedStrings #-}

-- | Compile-and-run proof that the engine instantiates under @IOSim@ with
-- the sim backend stub. Running 'simDBOS' builds the sim connection (which
-- needs @instance SystemDB IOSimSystemDB (IOSim s)@), a real 'Tasks' set,
-- and the handle's strict MVars — all inside @IOSim@. No backend method is
-- called, so the @undefined@ methods are never reached.
module DBOS.Transact.SimTest (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (runSimOrThrow)
import DBOS.SystemDB.IOSim (simDBOS)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase)

tests :: TestTree
tests =
  testGroup
    "Sim backend"
    [ testCase "a DBOS handle instantiates under IOSim with the sim backend" $
        assertBool "simDBOS runs" (runSimOrThrow (simDBOS >> pure True))
    ]
