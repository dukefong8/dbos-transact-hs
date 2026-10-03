{-# LANGUAGE OverloadedStrings #-}

-- | MUST BUILD: the documented residual hole. A step body that reaches
-- past its narrowed view and spends the *captured parent* context still
-- compiles — Haskell closures capture, and no type split can take the
-- parent value out of the body's lexical scope.
--
-- What the split buys anyway: the natural shape (use the handed 'SCtx')
-- is rejected (see N2/N4), and every engine-generated call site uses the
-- handed view. The runtime InsideStep check stays as the backstop for
-- this deliberate-capture shape — exactly the §11 "keep runtime"
-- direction, now with a smaller reachable surface.
module Main (main) where

import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Data.Text (Text)
import Scope.Model

startIt :: (MonadSTM m, Functor m) => WCtx i x m -> WRef i m () -> m Text
startIt ctx ref = handleId <$> startChild ctx ref "opts"

main :: IO ()
main = withInstance "a" $ \dbos ->
  withExecution dbos "parent" $ \wctx -> do
    ref <- register dbos "worker"
    _ <- startIt wctx ref
    runStep wctx "s" $ \_sctx -> startIt wctx ref >> pure ()
