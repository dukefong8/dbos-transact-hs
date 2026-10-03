{-# LANGUAGE OverloadedStrings #-}

-- | THROWAWAY positive flow under IO: full lifecycle in two nested
-- instance regions, proving counter independence and downhill inference
-- (no annotations beyond the helper's pinned error channel).
module Main (main) where

import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.IO as TIO
import Scope.Model

-- Pins the phantom error channel once, so call sites never annotate it.
startIt :: MonadSTM m => WorkflowCtx inst exec m -> WRef inst m () -> m (Either Text (WHandle inst m ()))
startIt ctx ref = startChild ctx ref "opts"

expectRight :: Either Text a -> IO a
expectRight = either (error . Text.unpack) pure

tpack :: Show a => a -> Text
tpack = Text.pack . show

main :: IO ()
main = withDBOS "a" $ \dba ->
  withDBOS "b" $ \dbb -> do
    ra <- register dba "worker"
    rb <- register dbb "worker"
    withWorkflow dba "wf-a" $ \ctxa -> do
      TIO.putStrLn ("a: workflow " <> ctxWorkflowId ctxa)
      s0 <- nextStepId ctxa
      TIO.putStrLn ("a: first step id " <> tpack s0)
      ha <- expectRight =<< startIt ctxa ra
      TIO.putStrLn ("a: child " <> handleId ha)
      withStep ctxa "charge" $ \step -> do
        TIO.putStrLn ("a: in step, workflow " <> sctxWorkflowId step)
        p <- placeAwait ctxa ha
        driven <- drive ctxa p
        TIO.putStrLn ("a: drive " <> tpack driven)
      hb <- mintHandle dba "legacy-id"
      TIO.putStrLn ("a: minted " <> handleId hb)
    withWorkflow dbb "wf-b" $ \ctxb -> do
      hb <- expectRight =<< startIt ctxb rb
      TIO.putStrLn ("b: child " <> handleId hb <> " (own counter: also -0)")
