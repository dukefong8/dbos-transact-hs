{-# LANGUAGE OverloadedStrings #-}

-- | THROWAWAY positive flow under IOSim: two instance objects in one
-- simulation plus a virtual-thread rendezvous. Proves the execution tag
-- nests inside the sim tag with determinism kept (no IO, no real
-- threads).
module Main (main) where

import Control.Monad.Class.MonadFork (forkIO)
import Control.Concurrent.Class.MonadMVar.Strict
  ( newEmptyMVar
  , putMVar
  , takeMVar
  )
import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Control.Monad.IOSim (IOSim, runSim)
import Data.Text (Text)
import qualified Data.Text as Text
import Scope.Model

startIt :: MonadSTM m => WorkflowCtx exec m -> WRef m () -> m (Either Text (WHandle m ()))
startIt ctx ref = startChild ctx ref "opts"

expectRight :: Either Text a -> IOSim s a
expectRight = either (error . Text.unpack) pure

main :: IO ()
main = print (runSim scenario)

scenario :: IOSim s (Text, Text, Text)
scenario = do
  dba <- newDBOS "a"
  dbb <- newDBOS "b"
  ra <- register dba "w"
  rb <- register dbb "w"
  box <- newEmptyMVar
  _ <- forkIO (withWorkflow dba "wf-a" $ \ctx -> do
    h <- expectRight =<< startIt ctx ra
    putMVar box (handleId h))
  withWorkflow dbb "wf-b" $ \ctx -> do
    h <- expectRight =<< startIt ctx rb
    a <- takeMVar box
    pure (a, handleId h, ctxWorkflowId ctx)
