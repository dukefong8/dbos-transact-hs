{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- Regression corpus (ADR-0029) for "cannot call, start, or enqueue workflows
-- from within steps": the three BAD bindings must FAIL with the
-- StepCtx-vs-WorkflowCtx mismatch (GHC-83865). The GOOD boundary controls
-- live in w_call_start_enqueue.hs. runNestedStep is deliberately absent: it
-- is the sanctioned StepCtx entry, covered by neg_nesting.hs.
module NegCallStartEnqueue where

import DBOS.Transact

childBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
childBody () _ = pure (Right 7)

-- CALL: invoking a workflow body inline with a step context.
badInlineCall :: forall exec. WorkflowCtx exec IO -> IO ()
badInlineCall wctx = do
  _ <- runStep wctx "probe" $ \sctx -> childBody () sctx
  pure ()

-- START: the ctx-threaded start from a step body.
badStartInStep ::
  forall exec. WorkflowRef IO () -> WorkflowCtx exec IO -> IO ()
badStartInStep ref wctx = do
  _ <- runStep wctx "probe" $ \sctx -> do
    _ <- (startChildWorkflow sctx ref undefined Nothing :: IO (Either (Error EngineOnly) (WorkflowHandle IO ())))
    pure ()
  pure ()

-- ENQUEUE: the ctx-threaded enqueue (start with a queue) from a step body.
badEnqueueInStep ::
  forall exec. WorkflowRef IO () -> WorkflowCtx exec IO -> IO ()
badEnqueueInStep ref wctx = do
  _ <- runStep wctx "probe" $ \sctx -> do
    _ <- (startChildWorkflow sctx ref (startOptionsDefault {startQueue = Just (enqueueNew "q")}) Nothing :: IO (Either (Error EngineOnly) (WorkflowHandle IO ())))
    pure ()
  pure ()
