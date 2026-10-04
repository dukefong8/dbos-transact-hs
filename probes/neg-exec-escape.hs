-- | G2 negative: the rank-2 binder keeps one run's counters from leaking
-- into another — returning the workflow view out of 'withWorkflow' must
-- not typecheck. Twin: @w-exec-escape.hs@.
{-# LANGUAGE OverloadedStrings #-}

module NegExecEscape where

import DBOS.Prelude
import DBOS.Transact (Identity, WorkflowCtx, WorkflowId, withWorkflow)
import DBOS.Transact.Connection (Connection)

bad :: Connection IO -> Identity -> WorkflowId -> IO (WorkflowCtx exec IO)
bad conn ident wid = withWorkflow conn ident wid Nothing pure
