{-# LANGUAGE OverloadedStrings #-}

-- | THROWAWAY positive flow under IOSim: two instances in one simulation
-- plus a virtual-thread rendezvous. Proves the instance tag nests inside
-- the sim tag with determinism kept (no IO, no real threads).
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
import Scope.Model

startIt :: MonadSTM m => WCtx i x m -> WRef i m () -> m (WHandle i m ())
startIt ctx ref = startChild ctx ref "opts"

main :: IO ()
main = print (runSim scenario)

scenario :: IOSim s (Text, Text, Text)
scenario = withInstance "a" $ \dba ->
  withInstance "b" $ \dbb -> do
    ra <- register dba "w"
    rb <- register dbb "w"
    box <- newEmptyMVar
    _ <- forkIO (withExecution dba "wf-a" $ \ctx -> do
      h <- startIt ctx ra
      putMVar box (handleId h))
    withExecution dbb "wf-b" $ \ctx -> do
      h <- startIt ctx rb
      a <- takeMVar box
      pure (a, handleId h, ctxWorkflowId ctx)
