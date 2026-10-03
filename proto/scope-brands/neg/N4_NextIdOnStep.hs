{-# LANGUAGE OverloadedStrings #-}

-- | MUST FAIL: allocating a step id through the narrowed step view. The
-- counter belongs to the workflow context alone.
module Main (main) where

import Scope.Model

main :: IO ()
main = do
  dbos <- newDBOS "a"
  withWorkflow dbos "wf" $ \ctx ->
    withStep ctx "s" $ \step -> do
      _ <- nextStepId step
      pure ()
