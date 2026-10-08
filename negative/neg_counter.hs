{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- Regression corpus (ADR-0029): the id counter and placement machinery are
-- reachable only through WorkflowCtx, so parallel starts claim in program
-- order with no StepCtx path to an id. All six BAD bindings must FAIL with
-- the StepCtx mismatch (GHC-83865). The witness twin is w_counter.hs.
-- Imports internals: corpus code, not app code.
module NegCounter where

import DBOS.Transact
import DBOS.Transact.Context (insideAStep, nextStepId, nextWorkflowMarker, withStep, withSystemDB)
import DBOS.Transact.Checkpoint (placeCall)

-- BAD: claiming a step id from a step view.
badNextStepId :: forall exec. WorkflowCtx exec IO -> IO ()
badNextStepId wctx = do
  _ <- runStep wctx "probe" $ \sctx -> do
    _ <- nextStepId sctx
    pure ()
  pure ()

-- BAD: minting a marker from a step view.
badNextMarker :: forall exec. WorkflowCtx exec IO -> IO ()
badNextMarker wctx = do
  _ <- runStep wctx "probe" $ \sctx -> do
    _ <- nextWorkflowMarker sctx
    pure ()
  pure ()

-- BAD: placing a call from a step view.
badPlaceCall :: forall exec. WorkflowCtx exec IO -> IO ()
badPlaceCall wctx = do
  _ <- runStep wctx "probe" $ \sctx -> do
    _ <- placeCall sctx
    pure ()
  pure ()

-- BAD: reading depth from a step view.
badInsideAStep :: forall exec. WorkflowCtx exec IO -> IO ()
badInsideAStep wctx = do
  _ <- runStep wctx "probe" $ \sctx -> do
    _ <- insideAStep sctx
    pure ()
  pure ()

-- BAD: opening a step scope from a step view.
badWithStep :: forall exec. WorkflowCtx exec IO -> IO ()
badWithStep wctx = do
  _ <- runStep wctx "probe" $ \sctx -> do
    _ <- withStep sctx undefined undefined (\_ -> pure ())
    pure ()
  pure ()

-- BAD: reaching the system database scope from a step view.
badWithSystemDB :: forall exec. WorkflowCtx exec IO -> IO ()
badWithSystemDB wctx = do
  _ <- runStep wctx "probe" $ \sctx -> do
    _ <- (withSystemDB sctx undefined :: IO ())
    pure ()
  pure ()
