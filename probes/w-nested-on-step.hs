-- | G3 witness for @neg-nested-on-workflow.hs@: the same call with the one
-- legal token — the 'StepCtx' the body receives. Must build clean.
{-# LANGUAGE OverloadedStrings #-}

module WNestedOnStep where

import Control.Monad (void)
import DBOS.Prelude
import DBOS.Transact (EngineOnly, Error, StepCtx, runNestedStep)

good :: StepCtx exec IO -> IO (Either (Error EngineOnly) Int)
good sctx = runNestedStep sctx "inner" (\_ -> pure (0 :: Int))
