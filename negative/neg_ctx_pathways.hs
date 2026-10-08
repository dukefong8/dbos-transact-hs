{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- Regression corpus (ADR-0029): every WorkflowCtx-taking facade entry misused
-- with a StepCtx. Expected class: GHC-83865 StepCtx-vs-WorkflowCtx mismatch,
-- one per statement below (29). The witness twin is w_ctx_pathways.hs.
module NegCtxPathways where

import Data.Text (Text)
import Data.Word (Word64)
import DBOS.Transact

badAll :: forall exec. WorkflowCtx exec IO -> IO ()
badAll wctx = do
  _ <- runStep wctx "probe" $ \sctx -> do
    _ <- (startChildWorkflow sctx undefined undefined Nothing :: IO (Either (Error EngineOnly) (WorkflowHandle IO ())))
    _ <- (runTxStep undefined undefined sctx undefined :: IO (Either (Error EngineOnly) ()))
    _ <- (runStep sctx "nested" (\_ -> pure "x") :: IO (Either (Error EngineOnly) Text))
    _ <- (runStepWith (undefined :: StepOptions EngineOnly) sctx "x" (\_ -> pure (Right "x")) :: IO (Either (Error EngineOnly) Text))
    _ <- (pendingStep sctx "x" (\_ -> pure (Right "x")) :: IO (PendingStep exec IO (Either (Error EngineOnly) Text)))
    _ <- (pendingStepWith (undefined :: StepOptions EngineOnly) sctx "x" (\_ -> pure (Right "x")) :: IO (PendingStep exec IO (Either (Error EngineOnly) Text)))
    _ <- (sleepStep sctx undefined :: IO (Either (Error EngineOnly) ()))
    _ <- (pendingSleep sctx undefined :: IO (PendingStep exec IO (Either (Error EngineOnly) ())))
    _ <- (selectStep sctx (undefined :: [SelectArm exec IO Text]) :: IO (Either (Error EngineOnly) Text))
    _ <- (awaitChild sctx (undefined :: WorkflowHandle IO EngineOnly) :: IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
    _ <- (pendingAwait sctx (undefined :: WorkflowHandle IO EngineOnly) :: IO (PendingStep exec IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))))
    _ <- (setEvent sctx "k" "v" :: IO (Either (Error EngineOnly) ()))
    _ <- (pendingSetEvent sctx "k" "v" :: IO (PendingStep exec IO (Either (Error EngineOnly) ())))
    _ <- (getEvent sctx undefined undefined undefined :: IO (Either (Error EngineOnly) (Maybe Text)))
    _ <- (send sctx undefined undefined undefined "x" :: IO (Either (Error EngineOnly) ()))
    _ <- (sendWith sctx undefined "x" undefined :: IO (Either (Error EngineOnly) ()))
    _ <- (sendBulk sctx (undefined :: [Message Text]) :: IO (Either (Error EngineOnly) ()))
    _ <- (sendBulkWith sctx (undefined :: [Message Text]) undefined :: IO (Either (Error EngineOnly) ()))
    _ <- (recv sctx undefined undefined :: IO (Either (Error EngineOnly) (Maybe Text)))
    _ <- (selectWorkflow sctx [] :: IO (Either (Error EngineOnly) WorkflowId))
    _ <- (joinWorkflows sctx [] :: IO (Either (Error EngineOnly) ()))
    _ <- (cancelWorkflowsInWorkflow sctx [] True :: IO (Either (Error EngineOnly) [WorkflowId]))
    _ <- (resumeWorkflowsInWorkflow sctx [] undefined :: IO (Either (Error EngineOnly) [WorkflowId]))
    _ <- (deleteWorkflowsInWorkflow sctx [] True :: IO (Either (Error EngineOnly) Word64))
    _ <- (forkWorkflowsInWorkflow sctx undefined undefined :: IO (Either (Error EngineOnly) [WorkflowId]))
    _ <- (forkFromInWorkflow sctx undefined undefined undefined :: IO (Either (Error EngineOnly) [WorkflowId]))
    _ <- (listWorkflowStepsInWorkflow sctx undefined :: IO (Either (Error EngineOnly) [StepRecord]))
    _ <- (either (const 0) length <$> listWorkflowsInWorkflow sctx undefined :: IO Int)
    let _ = workflowId sctx
    pure ()
  pure ()
