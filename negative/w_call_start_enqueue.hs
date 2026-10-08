{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- Witness twin (ADR-0029) for neg_call_start_enqueue.hs: the outside forms on
-- captured values compile — the accepted instance-surface limitation (R1),
-- not ctx entries. Must build clean.
module WCallStartEnqueue where

import DBOS.Transact

-- GOOD (boundary): calling through a captured executor compiles — outside form.
goodOutsideCall ::
  forall exec. Executor IO -> WorkflowKey -> WorkflowId -> WorkflowCtx exec IO -> IO ()
goodOutsideCall exec key wid wctx = do
  _ <- runStep wctx "probe" $ \_sctx -> do
    _ <- (runWorkflow exec key wid Nothing :: IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
    pure ()
  pure ()

-- GOOD (boundary): starting through a captured executor compiles — outside form.
goodOutsideStart ::
  forall exec. Executor IO -> WorkflowRef IO () -> WorkflowCtx exec IO -> IO ()
goodOutsideStart exec ref wctx = do
  _ <- runStep wctx "probe" $ \_sctx -> do
    _ <- (startWorkflow exec ref undefined Nothing :: IO (Either (Error EngineOnly) (WorkflowHandle IO ())))
    pure ()
  pure ()

-- GOOD (boundary): enqueueing through a captured DBOS compiles — outside form.
goodOutsideEnqueue ::
  forall exec. DBOS IO -> WorkflowKey -> WorkflowId -> WorkflowCtx exec IO -> IO ()
goodOutsideEnqueue dbos key wid wctx = do
  _ <- runStep wctx "probe" $ \_sctx -> do
    _ <- (fmap (const ()) <$> enqueueWorkflow dbos key wid Nothing "q" :: IO (Either (Error EngineOnly) ()))
    pure ()
  pure ()
