-- | G2 negative: a child start needs the workflow view. The narrowed
-- 'StepCtx' owns no allocator and cannot spend the parent's counter, so the
-- call must not typecheck. Twin: @w-child-start-workflow.hs@.
{-# LANGUAGE OverloadedStrings #-}

module NegChildStartStep where

import Control.Monad (void)
import DBOS.Prelude
import DBOS.Transact (StepCtx, WorkflowRef, startChildWorkflow, startOptionsDefault)

bad :: StepCtx exec IO -> WorkflowRef IO e -> IO ()
bad sctx ref = void (startChildWorkflow sctx ref startOptionsDefault Nothing)
