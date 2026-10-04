{-# LANGUAGE OverloadedStrings #-}

-- | WITNESS CONTROL for neg-n4-nextid-on-step: MUST BUILD CLEAN. The id
-- allocated through the workflow view while the step scope is live.
module Main (main) where

import Scope.Model

main :: IO ()
main = do
  dbos <- newDBOS "a"
  withWorkflow dbos "wf" $ \ctx ->
    withStep ctx "s" $ \_step -> do
      _ <- nextStepId ctx
      pure ()
