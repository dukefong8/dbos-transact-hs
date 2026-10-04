{-# LANGUAGE OverloadedStrings #-}

-- | WITNESS CONTROL for neg-n2-step-start: MUST BUILD CLEAN. The same
-- file with the one illegal token fixed — the child start made with the
-- workflow view. If this stops compiling, the negative is vacuous.
module Main (main) where

import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Data.Text (Text)
import Scope.Model

startIt :: MonadSTM m => WorkflowCtx exec m -> WRef m () -> m (Either Text (WHandle m ()))
startIt ctx ref = startChild ctx ref "opts"

main :: IO ()
main = do
  dbos <- newDBOS "a"
  withWorkflow dbos "wf" $ \ctx ->
    withStep ctx "s" $ \_step -> do
      ref <- register dbos "worker"
      _ <- startIt ctx ref -- the legal view, same call
      pure ()
