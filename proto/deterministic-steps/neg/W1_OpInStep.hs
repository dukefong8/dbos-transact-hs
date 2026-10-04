{-# LANGUAGE OverloadedStrings #-}

-- | WITNESS CONTROL for neg-op-in-workflow: MUST BUILD CLEAN. The same
-- file with the one illegal token fixed — the op invoked with the step
-- view it demands. If this stops compiling, the negative is vacuous.
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
  withStep wctx "ok" $ \s ->
    opFetchPrice (stepOps s) s "ok"

main :: IO ()
main = withFreshWorkflow dummyOps "wf" useIt >>= print
