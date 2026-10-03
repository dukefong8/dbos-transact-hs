{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | THROWAWAY runtime checks: cross-instance refusal (oracle parity) plus
-- the depth backstop (capture refused, restoration on success and throw,
-- per-attempt markers and tokens).
module Main (main) where

import Control.Exception (SomeException)
import Control.Monad.Class.MonadThrow qualified as MThrow
import Control.Concurrent.Class.MonadSTM.Strict (MonadSTM)
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.IO as TIO
import Scope.Model
import System.IO.Error (userError)

startIt :: MonadSTM m => WorkflowCtx exec m -> WRef m () -> m (Either Text (WHandle m ()))
startIt ctx ref = startChild ctx ref "opts"

isInsideRefusal :: Either Text a -> Bool
isInsideRefusal (Left e) = "InsideStep" `Text.isPrefixOf` e
isInsideRefusal _ = False

isWrongInstance :: Either Text a -> Bool
isWrongInstance (Left e) = "WrongInstance" `Text.isPrefixOf` e
isWrongInstance _ = False

main :: IO ()
main = do
  -- B0: cross-instance start is refused at runtime (oracle parity: the
  -- scenario the old N1 checked at compile time under `inst`).
  dba <- newDBOS "a"
  dbb <- newDBOS "b"
  refA <- register dba "worker"
  withWorkflow dbb "wf-b" $ \ctxb -> do
    r0 <- startChild ctxb refA "cross"
    TIO.putStrLn $
      if isWrongInstance r0 then "backstop: cross-instance refused"
      else "backstop: cross-instance NOT refused (HOLE)"
  dbos <- newDBOS "app"
  ref <- register dbos "worker"
  withWorkflow dbos "wf" $ \wctx -> do
    -- B1: a child start through the captured parent, inside a step body:
    -- compiles, but the depth counter refuses it.
    r1 <- withStep wctx "s" $ \_step -> startIt wctx ref
    TIO.putStrLn $
      if isInsideRefusal r1 then "backstop: capture-start refused"
      else "backstop: capture-start NOT refused (HOLE)"
    -- B2: same for a bare position claim.
    r2 <- withStep wctx "s" $ \_step -> placeCall wctx "test-call"
    TIO.putStrLn $
      if isInsideRefusal r2 then "backstop: capture-place refused"
      else "backstop: capture-place NOT refused (HOLE)"
    -- B3: the success path restores depth: allocating after a step works,
    -- and the counter did not move under the refused attempts above.
    _ <- withStep wctx "s" $ \sctx -> pure (sctxWorkflowId sctx)
    r3 <- startIt wctx ref
    case r3 of
      Right h -> TIO.putStrLn ("backstop: depth restored after success (" <> handleId h <> ")")
      Left e -> TIO.putStrLn ("backstop: depth NOT restored: " <> e)
    -- B4: a throwing body restores depth too (finally, not onException).
    _ <- MThrow.try (withStep wctx "s" $ \_ -> MThrow.throwIO (userError "boom"))
      :: IO (Either SomeException Text)
    r4 <- startIt wctx ref
    case r4 of
      Right h -> TIO.putStrLn ("backstop: depth restored after throw (" <> handleId h <> ")")
      Left e -> TIO.putStrLn ("backstop: depth NOT restored after throw: " <> e)
    -- B5: markers are per attempt (fresh scope each entry).
    m1 <- withStep wctx "s" $ \s -> pure (stepMarkerOf s)
    m2 <- withStep wctx "s" $ \s -> pure (stepMarkerOf s)
    TIO.putStrLn $
      if m1 /= m2 then "backstop: markers distinct"
      else "backstop: markers NOT distinct (HOLE)"
    -- B6: cancellation tokens are per attempt (fresh, unfired).
    c1 <- withStep wctx "s" $ \s -> cancelStep s >> stepCancelled s
    c2 <- withStep wctx "s" $ \s -> stepCancelled s
    TIO.putStrLn $
      if c1 && not c2 then "backstop: tokens per-attempt"
      else "backstop: tokens NOT per-attempt (HOLE)"
