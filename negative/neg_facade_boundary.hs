-- Regression corpus (ADR-0029): facade confinement. App code cannot reach
-- the parent WorkflowCtx from a step — only internals see it. Must FAIL
-- with the not-exported error (GHC reports it twice: the import and the
-- use site — same documented cause).
module NegFacadeBoundary where

import DBOS.Transact (stepCtxWorkflow)

f = stepCtxWorkflow
