{-# LANGUAGE OverloadedStrings #-}

-- | MUST FAIL: a handle smuggled out of its instance region. The rigid
-- @inst@ cannot be named outside the continuation (ST-style escape).
module Main (main) where

import Data.Text (Text)
import Scope.Model

mintIt :: DBOS i IO -> Text -> IO (WHandle i IO ())
mintIt dbos wid = mintHandle dbos wid

main :: IO ()
main = do
  h <- withDBOS "a" $ \dbos -> mintIt dbos "wf-1"
  print (handleId h)
