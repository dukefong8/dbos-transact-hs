{-# LANGUAGE OverloadedStrings #-}

-- | MUST FAIL: a ref from instance A used with a context from instance B.
-- The analogue of the runtime WrongInstance refusal, as a type error.
module Main (main) where

import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Data.Text (Text)
import Scope.Model

startIt :: MonadSTM m => WorkflowCtx inst exec m -> WRef inst m () -> m (Either Text (WHandle inst m ()))
startIt ctx ref = startChild ctx ref "opts"

main :: IO ()
main = withDBOS "a" $ \dba ->
  withDBOS "b" $ \dbb -> do
    ref <- register dba "worker"
    withWorkflow dbb "wf" $ \ctxb -> do
      _ <- startIt ctxb ref
      pure ()
