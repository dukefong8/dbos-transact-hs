{-# LANGUAGE OverloadedStrings #-}

-- | WITNESS CONTROL for neg-cross-exec: MUST BUILD CLEAN. The op record
-- and the scope from the same execution — the legal pairing.
module Main (main) where

import Ops.Model

dummyOps :: Ops exec IO
dummyOps = Ops
  { opReadInvoice = \_ -> pure 0
  , opFetchPrice  = \_ _ -> pure 0
  , opRandomCents = \_ -> pure 0
  , opNow         = \_ -> pure 0
  }

main :: IO ()
main = do
  j1 <- newJournal
  j2 <- newJournal
  withWorkflow j1 dummyOps "a" $ \w1 ->
    withStep w1 "s" $ \s1 ->
      withWorkflow j2 dummyOps "b" $ \_w2 -> do
        _ <- opFetchPrice (stepOps s1) s1 "x"
        pure () :: IO ()
