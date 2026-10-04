-- | G2 negative: a nested step is an op on the step view. Passing the
-- workflow view (which stands one level up) must not typecheck.
-- Twin: @w-nested-on-step.hs@.
{-# LANGUAGE OverloadedStrings #-}

module NegNestedOnWorkflow where

import Control.Monad (void)
import DBOS.Prelude
import DBOS.Transact (EngineOnly, Error, WorkflowCtx, runNestedStep)

bad :: WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
bad wctx = runNestedStep wctx "inner" (\_ -> pure (0 :: Int))
