{-# LANGUAGE OverloadedStrings #-}

-- | MUST FAIL: invoking an operation with the workflow view. Holding the
-- record at workflow scope is fine (to thread into helpers that run
-- steps); spending it there is rejected — every op demands the step
-- view, so non-determinism cannot happen outside a step.
module Main (main) where

import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Data.Text (Text)
import Ops.Model

dummyOps :: Ops exec IO
dummyOps = Ops
  { opReadInvoice = \_ -> pure 0
  , opFetchPrice  = \_ _ -> pure 0
  , opRandomCents = \_ -> pure 0
  , opNow         = \_ -> pure 0
  }

useIt :: MonadSTM m => WorkflowCtx exec m -> m Int
useIt wctx =
  withStep wctx "ok" $ \s -> do
    _ <- opFetchPrice (stepOps s) s "ok" -- pins the channel, valid use
    opFetchPrice (wfOps wctx) wctx "sneaky"

main :: IO ()
main = withFreshWorkflow dummyOps "wf" useIt >>= print
