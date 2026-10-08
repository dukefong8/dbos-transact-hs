{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- Witness twin (ADR-0029) for neg_nesting.hs: two levels of nesting stay
-- StepCtx-threaded throughout — no WorkflowCtx ever appears. Must build clean.
module WNesting where

import Data.Text (Text)
import DBOS.Transact

goodNestStaysStep :: forall exec. StepCtx exec IO -> IO (Either (Error EngineOnly) Text)
goodNestStaysStep sctx =
  runNestedStep sctx "a" $ \i1 -> do
    (inner :: Either (Error EngineOnly) Text) <-
      runNestedStep i1 "b" $ \_ ->
        pure "deep"
    pure (either (const "inner-failed") id inner)
