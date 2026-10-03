{-# LANGUAGE OverloadedStrings #-}

-- | MUST FAIL: using a handle from one sim run inside another. With `inst`
-- gone, the refusal is io-sim's own run tag — same composition claim,
-- one tag fewer.
module Main (main) where

import Control.Monad.IOSim (IOSim, runSim)
import Data.Text (Text)
import Scope.Model

mintIt :: DBOS (IOSim s) -> Text -> IOSim s (WHandle (IOSim s) ())
mintIt dbos wid = mintHandle dbos wid

main :: IO ()
main = do
  let Right h1 = runSim (newDBOS "a" >>= \dbos -> mintIt dbos "wf-1")
  print (runSim (pure (handleId h1)))
