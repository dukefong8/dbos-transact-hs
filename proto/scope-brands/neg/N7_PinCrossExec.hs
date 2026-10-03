{-# LANGUAGE OverloadedStrings #-}

-- | MUST FAIL: using a pin checked out in one execution from another
-- execution. Connection affinity without execution affinity is how app
-- writes land on a foreign transaction.
module Main (main) where

import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Data.Text (Text)
import Scope.Model

main :: IO ()
main = do
  dbos <- newDBOS "app"
  pool <- newPool 1
  withWorkflow dbos "w1" $ \c1 -> do
    Right pin <- checkout pool c1
    withWorkflow dbos "w2" $ \c2 -> do
      _ <- useIn c2 pin pool
      pure ()
