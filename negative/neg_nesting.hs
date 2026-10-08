{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- Regression corpus (ADR-0029): a step can only nest into the calling step's
-- execution. The three BAD bindings must FAIL with the StepCtx mismatch
-- (GHC-83865). The GOOD two-level nesting lives in w_nesting.hs; the runtime
-- half (nested calls run plain, no id moves) is pinned by
-- `scenarioNestedPlain` / `scenarioNestedStepView` in StepTest + StepTestSim.
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
