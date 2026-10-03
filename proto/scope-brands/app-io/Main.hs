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
startIt :: MonadSTM m => WCtx i x m -> WRef i m () -> m (WHandle i m ())
startIt ctx ref = startChild ctx ref "opts"

tpack :: Show a => a -> Text
tpack = Text.pack . show

main :: IO ()
main = withInstance "a" $ \dba ->
  withInstance "b" $ \dbb -> do
    ra <- register dba "worker"
    rb <- register dbb "worker"
    withExecution dba "wf-a" $ \ctxa -> do
      TIO.putStrLn ("a: workflow " <> ctxWorkflowId ctxa)
      s0 <- nextStepId ctxa
      TIO.putStrLn ("a: first step id " <> tpack s0)
      ha <- startIt ctxa ra
      TIO.putStrLn ("a: child " <> handleId ha)
      runStep ctxa "charge" $ \sctx -> do
        TIO.putStrLn ("a: in step, workflow " <> sctxWorkflowId sctx)
        p <- placeAwait ctxa ha
        driven <- drive ctxa p
        TIO.putStrLn ("a: drive " <> tpack driven)
      hb <- mintHandle dba "legacy-id"
      TIO.putStrLn ("a: minted " <> handleId hb)
    withExecution dbb "wf-b" $ \ctxb -> do
      hb <- startIt ctxb rb
      TIO.putStrLn ("b: child " <> handleId hb <> " (own counter: also -0)")
