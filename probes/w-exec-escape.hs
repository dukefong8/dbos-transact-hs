-- | G3 witness for @neg-exec-escape.hs@: spending the view inside the
-- continuation and returning a plain value builds clean.
{-# LANGUAGE OverloadedStrings #-}

module WExecEscape where

import Control.Monad (void)
import DBOS.Prelude
import DBOS.Transact (Identity, WorkflowCtx, WorkflowId, withWorkflow, workflowCtxId)
import DBOS.Transact.Connection (Connection)

good :: Connection IO -> Identity -> WorkflowId -> IO ()
good conn ident wid = void (withWorkflow conn ident wid Nothing (pure . workflowCtxId))
