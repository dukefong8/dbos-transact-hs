{-# LANGUAGE OverloadedStrings #-}

-- | THROWAWAY positive flow under IO: the demo workflow against real
-- handlers (real file reads, real clock, real uniqueness), then the same
-- journal replayed against the exploding handler. Proves record-once,
-- replay-without-effects on the live interpreter.
module Main (main) where

import Control.Concurrent.Class.MonadSTM.Strict (StrictTVar, newTVarIO)
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.IO as TextIO
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Unique (hashUnique, newUnique)
import Ops.Model
import Text.Read (readMaybe)

-- | The IO handler: every boundary a workflow must not touch directly,
-- each call counted.
ioOps :: StrictTVar IO Int -> Ops exec IO
ioOps counter = Ops
  { opReadInvoice = \_ -> bump counter >> readAmount "data/INVOICE.txt"
  , opFetchPrice  = \_ _ -> bump counter >> readAmount "data/PRICE.txt"
  , opRandomCents = \_ -> bump counter >> randomCents
  , opNow         = \_ -> bump counter >> round <$> getPOSIXTime
  }

randomCents :: IO Int
randomCents = do
  n <- abs . hashUnique <$> newUnique
  pure (n `mod` 100)

readAmount :: FilePath -> IO Int
readAmount path = do
  raw <- TextIO.readFile path
  case readMaybe (Text.unpack (Text.strip raw)) of
    Just n  -> pure n
    Nothing -> fail ("unparseable stub amount in " <> path)

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

main :: IO ()
main = do
  counter <- newTVarIO 0
  journal <- newJournal
  first <- withWorkflow journal (ioOps counter) "order" orderWorkflow
  mapM_ (putStrLn . Text.unpack) (stepLines first)
  putStrLn (Text.unpack (outcomeLine first))
  putStrLn (Text.unpack (nondetLine first))
  calls <- readCounter counter
  putStrLn ("CALLS n=" <> show calls)
  -- Same journal, exploding handler: any effect re-execution fails loudly.
  second <- withWorkflow journal explodingOps "order" orderWorkflow
  putStrLn ("REPLAY-" <> Text.unpack (outcomeLine second))
  putStrLn ("REPLAY-MATCH " <> show (first == second))
  callsAfter <- readCounter counter
  putStrLn ("REPLAY-CALLS n=" <> show callsAfter)
