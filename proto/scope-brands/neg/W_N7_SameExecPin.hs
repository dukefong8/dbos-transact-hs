{-# LANGUAGE OverloadedStrings #-}

-- | WITNESS CONTROL for neg-n7-pin-cross-exec: MUST BUILD CLEAN. The pin
-- checked out and used inside one execution.
module Main (main) where

import Scope.Model

main :: IO ()
main = do
  dbos <- newDBOS "app"
  pool <- newPool 1
  withWorkflow dbos "w1" $ \c1 -> do
    Right pin <- checkout pool c1
    _ <- useIn c1 pin pool
    pure ()
