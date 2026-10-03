{-# LANGUAGE OverloadedStrings #-}

-- | MUST FAIL: starting a child with the narrowed step view. The natural
-- in-step shape (use what the engine handed the body) is rejected; the
-- leaf rule becomes a compile error instead of runtime InsideStep.
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
    withStep ctx "s" $ \step -> do
      ref <- register dbos "worker"
      _ <- startIt ctx ref -- pins the error channel, valid use
      _ <- startChild step ref "sneaky"
      pure ()
