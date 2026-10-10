{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- Regression corpus (ADR-0029): a step can only nest into the calling step's
-- execution. The five BAD bindings must FAIL with a ctx mismatch
-- (GHC-83865): three keep recording entries out of step views, the fourth
-- keeps the step-view entry out of the workflow view (no StepCtx in hand),
-- and the fifth keeps transactions out of step views. The GOOD two-level
-- nesting lives in w_nesting.hs; the runtime half (nested calls run plain,
-- no id moves) is pinned by `scenarioNestedPlain` / `scenarioNestedStepView`
-- in StepTest + StepTestSim and `scenarioNestedInTx` in DatasourceTest +
-- DatasourceTestSim.
module NegNesting where

import Data.Text (Text)
import DBOS.Transact

-- BAD: a fresh recorded step from a step view.
badFreshStep :: forall exec. WorkflowCtx exec IO -> IO ()
badFreshStep wctx = do
  _ <- runStep wctx "outer" $ \sctx -> do
    _ <- (runStep sctx "fresh" (\_ -> pure "x") :: IO (Either (Error EngineOnly) Text))
    pure ()
  pure ()

-- BAD: a recorded step from a nested depth.
badNestedEscape :: forall exec. WorkflowCtx exec IO -> IO ()
badNestedEscape wctx = do
  _ <- runStep wctx "outer" $ \sctx -> do
    _ <- (runNestedStep sctx "inner" $ \inner -> do
      _ <- (runStep inner "escape" (\_ -> pure "x") :: IO (Either (Error EngineOnly) Text))
      pure "still-step") :: IO (Either (Error EngineOnly) Text)
    pure ()
  pure ()

-- BAD: a workflow start from a nested depth.
badNestedStart :: forall exec. WorkflowRef IO () -> WorkflowCtx exec IO -> IO ()
badNestedStart ref wctx = do
  _ <- runStep wctx "outer" $ \sctx -> do
    _ <- (runNestedStep sctx "inner" $ \inner -> do
      _ <- (startChildWorkflow inner ref undefined Nothing :: IO (Either (Error EngineOnly) (WorkflowHandle IO ())))
      pure "started") :: IO (Either (Error EngineOnly) Text)
    pure ()
  pure ()

-- BAD: the step-view entry from a workflow view. A workflow body holds no
-- StepCtx, so the nested entry is unreachable there — the mirror of the
-- three cases above.
badWorkflowNests :: forall exec. WorkflowCtx exec IO -> IO ()
badWorkflowNests wctx = do
  _ <- (runNestedStep wctx "inner" (\_ -> pure "x") :: IO (Either (Error EngineOnly) Text))
  pure ()

-- BAD: a transaction from a step view. Locks the matrix's `runTxStep` row
-- the way the first case locks `runStep`.
badTxFromStep :: forall exec. WorkflowCtx exec IO -> IO ()
badTxFromStep wctx = do
  _ <- runStep wctx "outer" $ \sctx -> do
    _ <- (runTxStep undefined undefined sctx (\_ _ -> pure (Right "x")) :: IO (Either (Error EngineOnly) Text))
    pure ()
  pure ()
