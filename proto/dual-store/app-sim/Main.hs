{-# LANGUAGE OverloadedStrings #-}

-- | THROWAWAY STM handler: each step is one 'atomically' block over
-- in-memory tables standing in for the Postgres schema. Same step
-- meanings, same atomicity unit, deterministic scheduling — the sim
-- verifies the workflow's invariants without touching a database.
module Main (main) where

import Control.Concurrent.Class.MonadSTM.Strict
  ( MonadSTM (..)
  , StrictTVar
  , atomically
  , newTVarIO
  , readTVar
  , readTVarIO
  , throwSTM
  , writeTVar
  )
import Control.Monad.IOSim (IOSim, runSim)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import qualified Data.Text as Text
import Store.Model
import System.Exit (exitFailure)

-- | The sim schema: inventory and orders mirror the Postgres tables,
-- the audit log mirrors the audits table, counters mint ids.
data SimTables s = SimTables
  { simStock  :: StrictTVar (IOSim s) (Map Text Int)
  , simOrders :: StrictTVar (IOSim s) (Map Int (Text, Int))
  , simAudits :: StrictTVar (IOSim s) (Map Int Text)
  , simNextId :: StrictTVar (IOSim s) Int
  }

seedTables :: IOSim s (SimTables s)
seedTables = do
  stock <- newTVarIO (Map.singleton "widget" 10)
  orders <- newTVarIO Map.empty
  audits <- newTVarIO Map.empty
  nextId <- newTVarIO 1
  pure (SimTables stock orders audits nextId)

reseedTables :: SimTables s -> IOSim s ()
reseedTables tbls = atomically $ do
  writeTVar (simStock tbls) (Map.singleton "widget" 10)
  writeTVar (simOrders tbls) Map.empty
  writeTVar (simAudits tbls) Map.empty
  writeTVar (simNextId tbls) 1

-- | The STM step table: each field is one atomic block, so a step's
-- effects land or vanish together — the same unit the Postgres side
-- gets from its held transaction.
simSteps :: SimTables s -> StepOps (IOSim s)
simSteps tbls = StepOps
  { stepReserve = \item qty -> atomically $ do
      inv <- readTVar (simStock tbls)
      case Map.lookup item inv of
        Just n | n >= qty -> do
          oid <- mintOrder tbls item qty
          writeTVar (simStock tbls) (Map.insert item (n - qty) inv)
          _ <- audit tbls ("reserved:" <> item)
          pure (Right (OrderId oid))
        _ -> pure (Left "insufficient-stock")
  , stepBomb = \item qty -> atomically $ do
      _ <- mintOrder tbls item qty
      _ <- audit tbls ("bomb:" <> item)
      throwSTM (userError "boom")
  , stepTrace = \note value -> atomically $ do
      _ <- audit tbls note
      pure value
  , stepStock = \item ->
      Map.findWithDefault (error "missing inventory row") item <$> readTVarIO (simStock tbls)
  , stepOrderCount = Map.size <$> readTVarIO (simOrders tbls)
  , stepAuditCount = Map.size <$> readTVarIO (simAudits tbls)
  }

mintOrder :: SimTables s -> Text -> Int -> STM (IOSim s) Int
mintOrder tbls item qty = do
  oid <- readTVar (simNextId tbls)
  writeTVar (simNextId tbls) (oid + 1)
  orders <- readTVar (simOrders tbls)
  writeTVar (simOrders tbls) (Map.insert oid (item, qty) orders)
  pure oid

audit :: SimTables s -> Text -> STM (IOSim s) ()
audit tbls note = do
  aid <- readTVar (simNextId tbls)
  writeTVar (simNextId tbls) (aid + 1)
  audits <- readTVar (simAudits tbls)
  writeTVar (simAudits tbls) (Map.insert aid note audits)

tally :: [Either Text OrderId] -> (Int, Int)
tally outcomes = (length [() | Right _ <- outcomes], length [() | Left _ <- outcomes])

tshow :: Show a => a -> Text
tshow = Text.pack . show

scenario :: IOSim s [Text]
scenario = do
  tbls <- seedTables
  withWorkflow (simSteps tbls) "widget-flow" $ \wctx -> do
    (refused, stock1, orders1, audits1) <- probeInsufficient wctx "widget" 99
    reseedTables tbls
    (threw, stock2, orders2, audits2) <- probeRollback wctx "widget" 3
    reseedTables tbls
    outcomes <- raceOrders wctx 8 "widget" 3
    let (winners, losers) = tally outcomes
        ops = wfSteps wctx
    stock3 <- stepStock ops "widget"
    orders3 <- stepOrderCount ops
    audits3 <- stepAuditCount ops
    pure
      [ "INSUFFICIENT-OK refused=" <> tshow refused <> " stock=" <> tshow stock1 <> " orders=" <> tshow orders1 <> " audits=" <> tshow audits1
      , "ROLLBACK-OK threw=" <> tshow threw <> " stock=" <> tshow stock2 <> " orders=" <> tshow orders2 <> " audits=" <> tshow audits2
      , "RACE winners=" <> tshow winners <> " losers=" <> tshow losers <> " stock=" <> tshow stock3 <> " orders=" <> tshow orders3 <> " audits=" <> tshow audits3
      ]

main :: IO ()
main = case runSim scenario of
  Left failure -> print failure >> exitFailure
  Right logLines -> mapM_ (putStrLn . Text.unpack) logLines
