{-# LANGUAGE OverloadedStrings #-}

-- | MUST FAIL: driving a placed await from a different execution of the
-- same instance. The cross-execution half of placement, as a type error
-- (plus the runtime token backstop in 'drive' for smuggled values).
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
    withWorkflow dbos "p2" $ \w2 -> do
      _ <- driveIt w2 p
      pure ()
