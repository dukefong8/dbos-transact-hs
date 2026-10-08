{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- Witness twin (ADR-0029) for neg_counter.hs: the same six reads with the
-- legal WorkflowCtx. Must build clean. Imports internals: corpus code.
module WCounter where

import DBOS.Transact
import DBOS.Transact.Context (insideAStep, nextStepId, nextWorkflowMarker, withStep, withSystemDB)
import DBOS.Transact.Checkpoint (placeCall)

witnessCounter :: forall exec. WorkflowCtx exec IO -> IO ()
witnessCounter wctx = do
  _ <- nextStepId wctx
  _ <- nextWorkflowMarker wctx
  _ <- placeCall wctx
  _ <- insideAStep wctx
  _ <- withStep wctx undefined undefined (\_ -> pure ())
  _ <- (withSystemDB wctx undefined :: IO ())
  pure ()
