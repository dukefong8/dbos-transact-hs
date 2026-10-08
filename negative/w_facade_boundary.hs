{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- Witness twin (ADR-0029) for neg_facade_boundary.hs: a name the facade does
-- export imports fine. Must build clean.
module WFacadeBoundary where

import Data.Text (Text)
import DBOS.Transact (WorkflowCtx, StepCtx, Error (..), EngineOnly, runStep)

f :: forall exec. WorkflowCtx exec IO -> Text -> (StepCtx exec IO -> IO Text) -> IO (Either (Error EngineOnly) Text)
f = runStep
