-- | G3 witness for @neg-child-start-step.hs@: the same call with the one
-- legal token — the 'WorkflowCtx' that owns the counter. Must build clean.
{-# LANGUAGE OverloadedStrings #-}

module WChildStartWorkflow where

import Control.Monad (void)
import DBOS.Prelude
import DBOS.Transact (WorkflowCtx, WorkflowRef, startChildWorkflow, startOptionsDefault)

good :: WorkflowCtx exec IO -> WorkflowRef IO e -> IO ()
good wctx ref = void (startChildWorkflow wctx ref startOptionsDefault Nothing)
