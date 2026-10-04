{-# LANGUAGE OverloadedStrings #-}

-- | WITNESS CONTROL for neg-step-cross-stack: MUST BUILD CLEAN. The sim
-- scope inside the sim flow — the legal pairing.
module Main (main) where

import Control.Monad.IOSim (IOSim, runSim)
import Data.Text (Text)
import Store.Model
import System.Exit (exitFailure)

simWctx :: WorkflowCtx (IOSim s)
simWctx = error "compile probe only"

scenario :: IOSim s (Either Text OrderId)
scenario =
  placeOrder simWctx "widget" 3

main :: IO ()
main = case runSim scenario of
  Left failure -> print failure >> exitFailure
  Right outcome -> print outcome
