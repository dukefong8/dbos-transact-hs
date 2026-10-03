{-# LANGUAGE OverloadedStrings #-}

-- | THROWAWAY Postgres handler: each step is one transaction on a held
-- connection (BEGIN/body/COMMIT, ROLLBACK on throw), mirroring the real
-- datasource backend's per-attempt connection. Setup and reseed run on a
-- scratch schema this case owns.
module Main (main) where

import Control.Exception (SomeException, bracket, throwIO, try)
import Data.Functor.Contravariant (contramap)
import Data.Int (Int32, Int64)
import Data.Text (Text)
import qualified Data.Text as Text
import Hasql.Connection qualified as Connection
import Hasql.Connection.Settings qualified as ConnSettings
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import Store.Model
import System.Environment (lookupEnv)

-- | The Postgres step table: every field opens, commits, or rolls back
-- its own transaction. Multi-statement steps (reserve, bomb) are atomic
-- because every statement rides one held connection.
pgSteps :: ConnSettings.Settings -> StepOps IO
pgSteps settings = StepOps
  { stepReserve = \item qty -> withTx settings $ \conn -> do
      leftover <- run conn (Session.statement (fromIntegral qty, item) reserveStmt)
      case leftover of
        Nothing -> pure (Left "insufficient-stock")
        Just _ -> do
          oid <- run conn (Session.statement (item, fromIntegral qty) insertStmt)
          _ <- run conn (Session.statement ("reserved:" <> item) auditStmt)
          pure (Right (OrderId (fromIntegral oid)))
  , stepBomb = \item qty -> withTx settings $ \conn -> do
      _ <- run conn (Session.statement (item, fromIntegral qty) insertStmt)
      _ <- run conn (Session.statement ("bomb:" <> item) auditStmt)
      throwIO (userError "boom")
  , stepTrace = \note value -> withTx settings $ \conn -> do
      _ <- run conn (Session.statement note auditStmt)
      pure value
  , stepStock = \item -> withConn settings $ \conn -> do
      found <- run conn (Session.statement item stockStmt)
      case found of
        Just n  -> pure (fromIntegral n)
        Nothing -> fail ("missing inventory row for " <> Text.unpack item)
  , stepOrderCount = withConn settings $ \conn ->
      fromIntegral <$> run conn (Session.statement () countOrdersStmt)
  , stepAuditCount = withConn settings $ \conn ->
      fromIntegral <$> run conn (Session.statement () countAuditsStmt)
  }

-- | One transaction around a body on a fresh raw connection: the probe
-- version of the backend's per-attempt pinning (no pool checkout exists,
-- so each scope holds its own connection).
withTx :: ConnSettings.Settings -> (Connection.Connection -> IO a) -> IO a
withTx settings body = bracket (acquireConn settings) Connection.release $ \conn -> do
  run conn (Session.script "BEGIN")
  outcome <- try (body conn)
  case outcome of
    Left err -> run conn (Session.script "ROLLBACK") >> throwIO (err :: SomeException)
    Right value -> run conn (Session.script "COMMIT") >> pure value

withConn :: ConnSettings.Settings -> (Connection.Connection -> IO a) -> IO a
withConn settings body = bracket (acquireConn settings) Connection.release body

acquireConn :: ConnSettings.Settings -> IO Connection.Connection
acquireConn settings =
  Connection.acquire settings >>= either (throwIO . userError . show) pure

run :: Connection.Connection -> Session.Session a -> IO a
run conn session =
  Connection.use conn session >>= either (throwIO . userError . show) pure

reserveStmt :: Statement.Statement (Int32, Text) (Maybe Int32)
reserveStmt =
  Statement.preparable
    "UPDATE proto_dual.inventory SET stock = stock - $1 WHERE item = $2 AND stock >= $1 RETURNING stock"
    (contramap fst (Encoders.param (Encoders.nonNullable Encoders.int4))
      <> contramap snd (Encoders.param (Encoders.nonNullable Encoders.text)))
    (Decoders.rowMaybe (Decoders.column (Decoders.nonNullable Decoders.int4)))

insertStmt :: Statement.Statement (Text, Int32) Int64
insertStmt =
  Statement.preparable
    "INSERT INTO proto_dual.orders (item, qty) VALUES ($1, $2) RETURNING id"
    (contramap fst (Encoders.param (Encoders.nonNullable Encoders.text))
      <> contramap snd (Encoders.param (Encoders.nonNullable Encoders.int4)))
    (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))

auditStmt :: Statement.Statement Text ()
auditStmt =
  Statement.preparable
    "INSERT INTO proto_dual.audits (note) VALUES ($1)"
    (Encoders.param (Encoders.nonNullable Encoders.text))
    Decoders.noResult

stockStmt :: Statement.Statement Text (Maybe Int32)
stockStmt =
  Statement.preparable
    "SELECT stock FROM proto_dual.inventory WHERE item = $1"
    (Encoders.param (Encoders.nonNullable Encoders.text))
    (Decoders.rowMaybe (Decoders.column (Decoders.nonNullable Decoders.int4)))

countOrdersStmt :: Statement.Statement () Int64
countOrdersStmt =
  Statement.preparable
    "SELECT COUNT(*) FROM proto_dual.orders"
    Encoders.noParams
    (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))

countAuditsStmt :: Statement.Statement () Int64
countAuditsStmt =
  Statement.preparable
    "SELECT COUNT(*) FROM proto_dual.audits"
    Encoders.noParams
    (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))

setupReseed :: ConnSettings.Settings -> IO ()
setupReseed settings = withConn settings $ \conn -> do
  run conn (Session.script "DROP SCHEMA IF EXISTS proto_dual CASCADE")
  run conn (Session.script "CREATE SCHEMA proto_dual")
  run conn (Session.script "CREATE TABLE proto_dual.inventory (item text primary key, stock integer not null)")
  run conn (Session.script "CREATE TABLE proto_dual.orders (id bigserial primary key, item text not null, qty integer not null)")
  run conn (Session.script "CREATE TABLE proto_dual.audits (id bigserial primary key, note text not null)")
  reseed settings

reseed :: ConnSettings.Settings -> IO ()
reseed settings = withConn settings $ \conn -> do
  run conn (Session.script "DELETE FROM proto_dual.audits")
  run conn (Session.script "DELETE FROM proto_dual.orders")
  run conn (Session.script "UPDATE proto_dual.inventory SET stock = 10 WHERE item = 'widget'")
  run conn (Session.script "INSERT INTO proto_dual.inventory (item, stock) SELECT 'widget', 10 WHERE NOT EXISTS (SELECT 1 FROM proto_dual.inventory WHERE item = 'widget')")

teardown :: ConnSettings.Settings -> IO ()
teardown settings = withConn settings $ \conn ->
  run conn (Session.script "DROP SCHEMA proto_dual CASCADE")

tally :: [Either Text OrderId] -> (Int, Int)
tally outcomes = (length [() | Right _ <- outcomes], length [() | Left _ <- outcomes])

main :: IO ()
main = do
  url <- lookupEnv "DBOS_DATABASE_URL"
  case url of
    Nothing -> fail "DBOS_DATABASE_URL is not set (the IO probe needs live Postgres)"
    Just raw -> do
      let settings = ConnSettings.connectionString (Text.pack raw)
      bracket (setupReseed settings) (\_ -> teardown settings) $ \_ -> do
        withWorkflow (pgSteps settings) "widget-flow" $ \wctx -> do
          (refused, stock1, orders1, audits1) <- probeInsufficient wctx "widget" 99
          putStrLn ("INSUFFICIENT-OK refused=" <> show refused <> " stock=" <> show stock1 <> " orders=" <> show orders1 <> " audits=" <> show audits1)
          reseed settings
          (threw, stock2, orders2, audits2) <- probeRollback wctx "widget" 3
          putStrLn ("ROLLBACK-OK threw=" <> show threw <> " stock=" <> show stock2 <> " orders=" <> show orders2 <> " audits=" <> show audits2)
          reseed settings
          outcomes <- raceOrders wctx 8 "widget" 3
          let (winners, losers) = tally outcomes
          stock3 <- stepStock (wfSteps wctx) "widget"
          orders3 <- stepOrderCount (wfSteps wctx)
          audits3 <- stepAuditCount (wfSteps wctx)
          putStrLn ("RACE winners=" <> show winners <> " losers=" <> show losers <> " stock=" <> show stock3 <> " orders=" <> show orders3 <> " audits=" <> show audits3)
