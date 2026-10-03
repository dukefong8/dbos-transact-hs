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
import Control.Monad.Class.MonadFork (forkIO)
import Control.Concurrent.Class.MonadMVar.Strict
  ( newEmptyMVar
  , putMVar
  , takeMVar
  )
import Control.Monad.IOSim (IOSim, runSim)

startIt :: MonadSTM m => WorkflowCtx exec m -> WRef m () -> m (Either Text (WHandle m ()))
startIt ctx ref = startChild ctx ref "opts"

isInsideRefusal :: Either Text a -> Bool
isInsideRefusal (Left e) = "InsideStep" `Text.isPrefixOf` e
isInsideRefusal _ = False

isWrongInstance :: Either Text a -> Bool
isWrongInstance (Left e) = "WrongInstance" `Text.isPrefixOf` e
isWrongInstance _ = False

mustRight :: Either Text a -> IOSim s a
mustRight = either (error . Text.unpack) pure

tpack :: Show a => a -> Text
tpack = Text.pack . show

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
  poolA <- newPool 2
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
    -- B7: pinning inside a step body is refused (uniform depth rule:
    -- checkout, like allocation, needs depth 0).
    r7 <- withStep wctx "s" $ \_ -> checkout poolA wctx
    TIO.putStrLn $
      if isInsideRefusal r7 then "backstop: in-step pin refused"
      else "backstop: in-step pin NOT refused (HOLE)"
    -- B10: use after release is refused (generation backstop).
    Right pin10 <- checkout poolA wctx
    releasePin pin10 poolA
    u10 <- useIn wctx pin10 poolA
    TIO.putStrLn $
      case u10 of
        Left _ -> "backstop: use-after-release refused"
        Right _ -> "backstop: use-after-release NOT refused (HOLE)"
    -- B11: the pin is released even when the use throws (bracket shape).
    Right pin11 <- checkout poolA wctx
    MThrow.catch
      (MThrow.finally
        (do _ <- useIn wctx pin11 poolA
            MThrow.throwIO (userError "pin-boom")
            pure ())
        (releasePin pin11 poolA))
      ((\_ -> pure ()) :: SomeException -> IO ())
    r11 <- checkout poolA wctx
    TIO.putStrLn $
      case r11 of
        Right _ -> "backstop: pin released on throw"
        Left _ -> "backstop: pin NOT released on throw (HOLE)"
    -- B9: the raw path bypasses the cap (today's per-attempt raw acquire).
    pool9 <- newPool 2
    _ <- rawAcquire pool9
    _ <- rawAcquire pool9
    _ <- rawAcquire pool9
    hw9 <- poolHighWater pool9
    TIO.putStrLn $
      if hw9 == 3 then "backstop: raw path exceeds"
      else "backstop: raw path capped?! (unexpected)"
    rawRelease pool9
    rawRelease pool9
    rawRelease pool9
    -- B8: under contention the pool waits instead of opening more. Fully
    -- deterministic: both holders are observed holding before the third
    -- is released to proceed, so interleavings cannot move the mark.
    TIO.putStrLn "backstop: pool race (sim):"
    case runSim simRace of
      Right (during, final, d1, d2, d3) -> do
        TIO.putStrLn ("  high-water under contention: " <> tpack during <> ", final: " <> tpack final)
        TIO.putStrLn $
          if during == 2 && final == 2 && all (== "ok") [d1, d2, d3] then "backstop: pool cap respected"
          else "backstop: pool cap NOT respected (HOLE)"
      Left _ -> TIO.putStrLn "backstop: pool race FAILED (sim error)"

-- | Three racers, pool of two: the third waits for a release instead of
-- opening a third connection. Gate MVars force the overlap
-- deterministically: both holders are observed holding before anyone is
-- released, so no schedule can dodge the contention.
simRace :: IOSim s (Int, Int, Text, Text, Text)
simRace = do
  pool <- newPool 2
  dba <- newDBOS "sim"
  arrived1 <- newEmptyMVar
  arrived2 <- newEmptyMVar
  arrived3 <- newEmptyMVar
  trying <- newEmptyMVar
  go <- newEmptyMVar
  done <- newEmptyMVar
  let holder arrived = withWorkflow dba "w" $ \ctx -> do
        pin <- mustRight =<< checkout pool ctx
        putMVar arrived ()
        takeMVar go
        u <- useIn ctx pin pool
        releasePin pin pool
        putMVar done (either (const "use-failed") (const "ok") u)
  _ <- forkIO (holder arrived1)
  _ <- forkIO (holder arrived2)
  _ <- forkIO (putMVar trying () >> holder arrived3)
  takeMVar arrived1
  takeMVar arrived2
  hwDuring <- poolHighWater pool
  takeMVar trying
  putMVar go ()
  putMVar go ()
  takeMVar arrived3
  putMVar go ()
  d1 <- takeMVar done
  d2 <- takeMVar done
  d3 <- takeMVar done
  hwFinal <- poolHighWater pool
  pure (hwDuring, hwFinal, d1, d2, d3)
