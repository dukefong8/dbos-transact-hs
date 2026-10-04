-- | G2 negative: step ids are allocated by the workflow view alone. The
-- narrowed view exposes no allocator. Twin: @w-alloc-on-workflow.hs@.
{-# LANGUAGE OverloadedStrings #-}

module NegAllocOnStep where

import DBOS.Transact (StepCtx, nextWorkflowStepId)

bad :: StepCtx exec IO -> IO Int
bad sctx = nextWorkflowStepId sctx
