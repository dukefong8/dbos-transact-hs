-- | G3 witness for @neg-cross-exec-arms.hs@: the same race with both halves
-- under one execution brand. Must build clean.
{-# LANGUAGE OverloadedStrings #-}

module WCrossExecArms where

import Control.Monad (void)
import DBOS.Prelude
import DBOS.Transact (SelectArm, WorkflowCtx, selectStep)

good :: WorkflowCtx exec IO -> [SelectArm exec IO r] -> IO ()
good wctx arms = void (selectStep wctx arms)
