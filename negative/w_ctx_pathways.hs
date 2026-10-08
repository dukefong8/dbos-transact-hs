{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- Witness twin (ADR-0029) for neg_ctx_pathways.hs: the same 29 entries with
-- the legal WorkflowCtx. Must build clean — if it does not, the negative is
-- suspect and the gate is red.
module WCtxPathways where

import Data.Text (Text)
import Data.Word (Word64)
import DBOS.Transact

witness :: forall exec. WorkflowCtx exec IO -> IO ()
witness wctx = do
  _ <- (startChildWorkflow wctx undefined undefined Nothing :: IO (Either (Error EngineOnly) (WorkflowHandle IO ())))
  _ <- (runTxStep undefined undefined wctx undefined :: IO (Either (Error EngineOnly) ()))
  _ <- (runStep wctx "ok" (\_ -> pure ("x" :: Text)) :: IO (Either (Error EngineOnly) Text))
  _ <- (runStepWith (undefined :: StepOptions EngineOnly) wctx "x" (\_ -> pure (Right ("x" :: Text))) :: IO (Either (Error EngineOnly) Text))
  _ <- (pendingStep wctx "x" (\_ -> pure (Right ("x" :: Text))) :: IO (PendingStep exec IO (Either (Error EngineOnly) Text)))
  _ <- (pendingStepWith (undefined :: StepOptions EngineOnly) wctx "x" (\_ -> pure (Right ("x" :: Text))) :: IO (PendingStep exec IO (Either (Error EngineOnly) Text)))
  _ <- (sleepStep wctx undefined :: IO (Either (Error EngineOnly) ()))
  _ <- (pendingSleep wctx undefined :: IO (PendingStep exec IO (Either (Error EngineOnly) ())))
  _ <- (selectStep wctx (undefined :: [SelectArm exec IO Text]) :: IO (Either (Error EngineOnly) Text))
  _ <- (awaitChild wctx (undefined :: WorkflowHandle IO EngineOnly) :: IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
  _ <- (pendingAwait wctx (undefined :: WorkflowHandle IO EngineOnly) :: IO (PendingStep exec IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))))
  _ <- (setEvent wctx ("k" :: Text) ("v" :: Text) :: IO (Either (Error EngineOnly) ()))
  _ <- (pendingSetEvent wctx ("k" :: Text) ("v" :: Text) :: IO (PendingStep exec IO (Either (Error EngineOnly) ())))
  _ <- (getEvent wctx undefined undefined undefined :: IO (Either (Error EngineOnly) (Maybe Text)))
  _ <- (send wctx undefined undefined undefined ("x" :: Text) :: IO (Either (Error EngineOnly) ()))
  _ <- (sendWith wctx undefined ("x" :: Text) undefined :: IO (Either (Error EngineOnly) ()))
  _ <- (sendBulk wctx (undefined :: [Message Text]) :: IO (Either (Error EngineOnly) ()))
  _ <- (sendBulkWith wctx (undefined :: [Message Text]) undefined :: IO (Either (Error EngineOnly) ()))
  _ <- (recv wctx undefined undefined :: IO (Either (Error EngineOnly) (Maybe Text)))
  _ <- (selectWorkflow wctx [] :: IO (Either (Error EngineOnly) WorkflowId))
  _ <- (joinWorkflows wctx [] :: IO (Either (Error EngineOnly) ()))
  _ <- (cancelWorkflowsInWorkflow wctx [] True :: IO (Either (Error EngineOnly) [WorkflowId]))
  _ <- (resumeWorkflowsInWorkflow wctx [] undefined :: IO (Either (Error EngineOnly) [WorkflowId]))
  _ <- (deleteWorkflowsInWorkflow wctx [] True :: IO (Either (Error EngineOnly) Word64))
  _ <- (forkWorkflowsInWorkflow wctx undefined undefined :: IO (Either (Error EngineOnly) [WorkflowId]))
  _ <- (forkFromInWorkflow wctx undefined undefined undefined :: IO (Either (Error EngineOnly) [WorkflowId]))
  _ <- (listWorkflowStepsInWorkflow wctx undefined :: IO (Either (Error EngineOnly) [StepRecord]))
  _ <- (either (const 0) length <$> listWorkflowsInWorkflow wctx undefined :: IO Int)
  let _ = workflowId wctx
  pure ()
