{-# LANGUAGE OverloadedStrings #-}

-- | MUST FAIL: an IO-only step body riding the shared path into the
-- simulator. The body compiles fine at IO, but the workflow that uses
-- it cannot instantiate under IOSim — so anything the sim runs is
-- ambient-IO-free by construction, and anything else never reaches it.
module Main (main) where

import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Control.Monad.IOSim (IOSim, runSim)
import Data.Text (Text)
import Data.Unique (hashUnique, newUnique)
import Ops.Model
import System.Exit (exitFailure)

-- Compiles at IO: uniqueness is a legitimate effect there.
ioOnlyCents :: Ops exec IO -> StepCtx exec IO -> IO Int
ioOnlyCents ops s = do
  unique <- newUnique
  cents <- opRandomCents ops s
  pure (cents + hashUnique unique `mod` 100)

-- The shared path instantiates at IOSim: the IO-only body cannot follow.
simDummyOps :: Ops exec (IOSim s)
simDummyOps = Ops
  { opReadInvoice = \_ -> pure 0
  , opFetchPrice  = \_ _ -> pure 0
  , opRandomCents = \_ -> pure 0
  , opNow         = \_ -> pure 0
  }

scenario :: IOSim s Int
scenario =
  withFreshWorkflow simDummyOps "wf" $ \w ->
    withStep w "s" $ \s ->
      ioOnlyCents simDummyOps s

main :: IO ()
main = case runSim scenario of
  Left failure -> print failure >> exitFailure
  Right n -> print n
