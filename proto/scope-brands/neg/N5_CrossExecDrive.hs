{-# LANGUAGE OverloadedStrings #-}

-- | MUST FAIL: driving a placed await from a different execution of the
-- same instance. The cross-execution half of placement, as a type error
-- (plus the runtime token backstop in 'drive' for smuggled values).
module Main (main) where

import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Data.Text (Text)
import Scope.Model

startIt :: MonadSTM m => WCtx i x m -> WRef i m () -> m (WHandle i m ())
startIt ctx ref = startChild ctx ref "opts"

driveIt :: WCtx i x IO -> Pending i x IO Text -> IO (Either Text Text)
driveIt ctx p = drive ctx p

main :: IO ()
main = withInstance "a" $ \dbos -> do
  ref <- register dbos "worker"
  withExecution dbos "p1" $ \w1 -> do
    h <- startIt w1 ref
    p <- placeAwait w1 h
    withExecution dbos "p2" $ \w2 -> do
      _ <- driveIt w2 p
      pure ()
