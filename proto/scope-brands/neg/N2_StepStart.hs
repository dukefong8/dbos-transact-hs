{-# LANGUAGE OverloadedStrings #-}

-- | MUST FAIL: starting a child with the narrowed step view. The natural
-- in-step shape (use what the engine handed the body) is rejected; the
-- leaf rule becomes a compile error instead of runtime InsideStep.
module Main (main) where

import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Scope.Model

startIt :: MonadSTM m => WCtx i x m -> WRef i m () -> m (WHandle i m ())
startIt ctx ref = startChild ctx ref "opts"

main :: IO ()
main = withInstance "a" $ \dbos ->
  withExecution dbos "wf" $ \ctx ->
    runStep ctx "s" $ \sctx -> do
      ref <- register dbos "worker"
      _ <- startIt ctx ref -- pins the error channel, valid use
      _ <- startChild sctx ref "sneaky"
      pure ()
