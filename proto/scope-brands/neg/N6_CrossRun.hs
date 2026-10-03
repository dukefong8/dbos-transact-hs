{-# LANGUAGE OverloadedStrings #-}

-- | MUST FAIL: using a handle from one sim run inside another. The 'let'
-- forces the escape to be about the two scope tags (the instance tag and
-- io-sim's own run tag) rather than about runSim's Either wrapper.
module Main (main) where

import Control.Monad.IOSim (IOSim, runSim)
import Data.Text (Text)
import Scope.Model

mintIt :: DBOS i (IOSim s) -> Text -> IOSim s (WHandle i (IOSim s) ())
mintIt dbos wid = mintHandle dbos wid

main :: IO ()
main = do
  let Right h1 = runSim (withDBOS "a" $ \dbos -> mintIt dbos "wf-1")
  print (runSim (withDBOS "b" $ \_ -> pure (handleId h1)))
