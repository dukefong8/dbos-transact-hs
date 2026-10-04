{-# LANGUAGE OverloadedStrings #-}

-- | WITNESS CONTROL for neg-raw-io: MUST BUILD CLEAN. The IO-only body
-- made polymorphic over the interpreter — the shape a shared workflow
-- must have. Compiles at IO and at IOSim alike.
module Main (main) where

import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Control.Monad.IOSim (IOSim, runSim)
import Ops.Model
import System.Exit (exitFailure)

-- Polymorphic over m: no IO-only operations, so the sim can instantiate it.
sharedCents :: MonadSTM m => Ops exec m -> StepCtx exec m -> m Int
sharedCents ops s = do
  cents <- opRandomCents ops s
  pure (cents + 1)

dummyOps :: Ops exec IO
dummyOps = Ops
  { opReadInvoice = \_ -> pure 0
  , opFetchPrice  = \_ _ -> pure 0
  , opRandomCents = \_ -> pure 7
  , opNow         = \_ -> pure 0
  }

simDummyOps :: Ops exec (IOSim s)
simDummyOps = Ops
  { opReadInvoice = \_ -> pure 0
  , opFetchPrice  = \_ _ -> pure 0
  , opRandomCents = \_ -> pure 7
  , opNow         = \_ -> pure 0
  }

scenario :: IOSim s Int
scenario =
  withFreshWorkflow simDummyOps "wf" $ \w ->
    withStep w "s" $ \s ->
      sharedCents simDummyOps s

main :: IO ()
main = case runSim scenario of
  Left failure -> print failure >> exitFailure
  Right n -> print n
