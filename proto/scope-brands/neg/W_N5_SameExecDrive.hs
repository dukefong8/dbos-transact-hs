{-# LANGUAGE OverloadedStrings #-}

-- | WITNESS CONTROL for neg-n5-cross-exec-drive: MUST BUILD CLEAN. The
-- await driven from the execution it was placed in — the legal pairing.
module Main (main) where

import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Data.Text (Text)
import Scope.Model

startIt :: MonadSTM m => WorkflowCtx exec m -> WRef m () -> m (Either Text (WHandle m ()))
startIt ctx ref = startChild ctx ref "opts"

driveIt :: WorkflowCtx exec IO -> Pending exec IO Text -> IO (Either Text Text)
driveIt ctx p = drive ctx p

main :: IO ()
main = do
  dbos <- newDBOS "a"
  ref <- register dbos "worker"
  withWorkflow dbos "p1" $ \w1 -> do
    Right h <- startIt w1 ref
    p <- placeAwait w1 h
    _ <- driveIt w1 p
    pure ()
