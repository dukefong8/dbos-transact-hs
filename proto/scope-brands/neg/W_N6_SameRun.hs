{-# LANGUAGE OverloadedStrings #-}

-- | WITNESS CONTROL for neg-n6-cross-run: MUST BUILD CLEAN. The handle
-- used inside the run that minted it.
module Main (main) where

import Control.Monad.IOSim (IOSim, runSim)
import Data.Text (Text)
import Scope.Model

mintIt :: DBOS (IOSim s) -> Text -> IOSim s (WHandle (IOSim s) ())
mintIt dbos wid = mintHandle dbos wid

main :: IO ()
main =
  print (runSim (newDBOS "a" >>= \dbos -> mintIt dbos "wf-1" >>= \h -> pure (handleId h)))
