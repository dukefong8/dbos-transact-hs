-- | G3 witness for @neg-alloc-on-step.hs@: the allocator on the view that
-- owns the counter. Must build clean.
{-# LANGUAGE OverloadedStrings #-}

module WAllocOnWorkflow where

import DBOS.Transact (WorkflowCtx, nextStepId)

good :: WorkflowCtx exec IO -> IO Int
good wctx = nextStepId wctx
