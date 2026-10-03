{-# LANGUAGE OverloadedStrings #-}

-- | THROWAWAY positive flow under IOSim: the same demo workflow against
-- canned handlers (fixed answers, no IO, virtual scheduling). The
-- workflow source is shared with the IO run — only the installed record
-- differs, which is the whole swappability claim.
module Main (main) where

import Control.Concurrent.Class.MonadSTM.Strict (StrictTVar, newTVarIO)
import Control.Monad.IOSim (IOSim, runSim)
import Data.Text (Text)
import qualified Data.Text as Text
import Ops.Model
import System.Exit (exitFailure)

-- | The sim handler: canned answers standing in for the world. Same
-- values the shipped stubs carry, so the runs agree on every recorded
-- slot; nothing here can observe wall-clock or process uniqueness.
simOps :: StrictTVar (IOSim s) Int -> Ops exec (IOSim s)
simOps counter = Ops
  { opReadInvoice = \_ -> bump counter >> pure 4200
  , opFetchPrice  = \_ _ -> bump counter >> pure 1500
  , opRandomCents = \_ -> bump counter >> pure 7
  , opNow         = \_ -> bump counter >> pure 1700000000
  }

stepLines :: Receipt -> [Text]
stepLines receipt =
  [ "STEP read-invoice=" <> Text.pack (show (receiptInvoice receipt))
  , "STEP fetch-price=" <> Text.pack (show (receiptPrice receipt))
  , "STEP lane-step=" <> receiptLane receipt
  ]

outcomeLine :: Receipt -> Text
outcomeLine receipt =
  "OUTCOME total=" <> Text.pack (show (receiptTotal receipt))
    <> " lane=" <> receiptLane receipt

nondetLine :: Receipt -> Text
nondetLine receipt =
  "NONDET cents=" <> Text.pack (show (receiptCents receipt))
    <> " at=" <> Text.pack (show (receiptAt receipt))

scenario :: IOSim s [Text]
scenario = do
  counter <- newTVarIO 0
  journal <- newJournal
  first <- withWorkflow journal (simOps counter) "order" orderWorkflow
  calls <- readCounter counter
  -- Same journal, exploding handler: any effect re-execution fails loudly.
  second <- withWorkflow journal explodingOps "order" orderWorkflow
  callsAfter <- readCounter counter
  pure $
    stepLines first
      <> [ outcomeLine first
         , nondetLine first
         , "CALLS n=" <> Text.pack (show calls)
         , "REPLAY-" <> outcomeLine second
         , "REPLAY-MATCH " <> Text.pack (show (first == second))
         , "REPLAY-CALLS n=" <> Text.pack (show callsAfter)
         ]

main :: IO ()
main = case runSim scenario of
  Left failure -> print failure >> exitFailure
  Right logLines -> mapM_ (putStrLn . Text.unpack) logLines
