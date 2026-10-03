{-# LANGUAGE OverloadedStrings #-}

-- | MUST FAIL: spending an IO-backed workflow scope inside the
-- simulator. The step table rides the workflow context, so contexts —
-- not bare records — are what cross (or refuse to cross) interpreters.
module Main (main) where

import Control.Monad.IOSim (IOSim, runSim)
import Data.Text (Text)
import Store.Model
import System.Exit (exitFailure)

-- Compile probes only: never run (-fno-code), so unimplemented is fine.
ioWctx :: WorkflowCtx IO
ioWctx = error "compile probe only"

simWctx :: WorkflowCtx (IOSim s)
simWctx = error "compile probe only"

scenario :: IOSim s (Either Text OrderId)
scenario = do
  _ <- placeOrder simWctx "widget" 3 -- pins the channel, valid use
  placeOrder ioWctx "widget" 3 -- MUST FAIL: IO scope in an IOSim flow

main :: IO ()
main = case runSim scenario of
  Left failure -> print failure >> exitFailure
  Right outcome -> print outcome
