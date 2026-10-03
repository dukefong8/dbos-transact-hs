{-# LANGUAGE OverloadedStrings #-}

-- | MUST FAIL: spending one execution's record against another
-- execution's scope. The phantom brand threads through the operations
-- record, so mixing executions is rejected even though each value is
-- used with the right shape. (Merely mentioning one execution's values
-- inside another scope is fine when nothing branded crosses — brands
-- track executions, the depth counter tracks dynamic step scope.)
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
      withWorkflow j2 dummyOps "b" $ \w2 ->
        withStep w2 "t" $ \s2 -> do
          _ <- opFetchPrice (stepOps s1) s2 "x"
          pure () :: IO ()
