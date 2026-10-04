-- | G2 negative: a race's arms carry the execution brand of the pending
-- steps they were built from; racing them under a different execution's
-- view must not typecheck. Twin: @w-cross-exec-arms.hs@.
{-# LANGUAGE OverloadedStrings #-}

module NegCrossExecArms where

import Control.Monad (void)
import DBOS.Prelude
import DBOS.Transact (SelectArm, WorkflowCtx, selectStep)

bad :: WorkflowCtx exec1 IO -> WorkflowCtx exec2 IO -> [SelectArm exec2 IO r] -> IO ()
bad wctx _ arms = void (selectStep wctx arms)
