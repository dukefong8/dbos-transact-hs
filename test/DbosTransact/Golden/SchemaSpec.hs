{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module DbosTransact.Golden.SchemaSpec
  ( tests
  ) where

import Control.Exception (bracket)
import Data.ByteString.Lazy qualified as LBS
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text.Encoding
import Data.Vector qualified as Vector
import DbosTransact.SystemDB.Schema
import Hasql.Connection qualified as Connection
import Hasql.Connection.Settings qualified as Settings
import Hasql.Session qualified as Session
import Hasql.Statement (Statement)
import Hasql.TH (vectorStatement)
import System.Environment (lookupEnv)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.Golden (goldenVsString)
import Test.Tasty.HUnit (testCase)

tests :: TestTree
tests = testGroup "Golden.SchemaSpec"
  [ goldenVsString
      "rendered migration 1 SQL matches golden"
      "test/DbosTransact/Golden/migration1.sql"
      (pure (renderMigrationBS (SchemaName "dbos") migration1))

  , goldenVsString
      "dbos schema tables match golden"
      "test/DbosTransact/Golden/dbos_tables.txt"
      renderDbosTablesBS

  , goldenVsString
      "workflow_status columns match golden"
      "test/DbosTransact/Golden/workflow_status_columns.txt"
      renderWorkflowStatusColumnsBS

  , testCase "migration validates in fresh PostgreSQL schema" $ do
      tables <- runHasqlValidation
      expectTables tables
  ]

------------------------------------------------------------
-- Golden renderers
------------------------------------------------------------

renderMigrationBS :: SchemaName -> Migration -> LBS.ByteString
renderMigrationBS schemaName =
  LBS.fromStrict . Text.Encoding.encodeUtf8 . renderMigration schemaName

renderDbosTablesBS :: IO LBS.ByteString
renderDbosTablesBS = do
  tables <- queryDbosTables
  pure (LBS.fromStrict (Text.Encoding.encodeUtf8 (textLines tables)))

renderWorkflowStatusColumnsBS :: IO LBS.ByteString
renderWorkflowStatusColumnsBS = do
  columns <- queryWorkflowStatusColumns
  pure (LBS.fromStrict (Text.Encoding.encodeUtf8 (textLines columns)))

textLines :: Vector.Vector Text -> Text
textLines = Text.intercalate "\n" . Vector.toList

------------------------------------------------------------
-- PostgreSQL queries
------------------------------------------------------------

queryDbosTables :: IO (Vector.Vector Text)
queryDbosTables = withConnection $ \conn ->
  use conn (Session.statement "dbos" selectTablesStatement)

queryWorkflowStatusColumns :: IO (Vector.Vector Text)
queryWorkflowStatusColumns = withConnection $ \conn ->
  use conn (Session.statement "workflow_status" selectColumnsStatement)

selectTablesStatement :: Statement Text (Vector.Vector Text)
selectTablesStatement =
  [vectorStatement|
    select table_name :: text
    from information_schema.tables
    where table_schema = $1 :: text
      and table_type = 'BASE TABLE'
    order by table_name
  |]

selectColumnsStatement :: Statement Text (Vector.Vector Text)
selectColumnsStatement =
  [vectorStatement|
    select column_name :: text
    from information_schema.columns
    where table_schema = 'dbos'
      and table_name = $1 :: text
    order by ordinal_position
  |]

------------------------------------------------------------
-- Temp-schema migration validation
------------------------------------------------------------

runHasqlValidation :: IO (Vector.Vector Text)
runHasqlValidation = withConnection $ \conn -> do
  let schemaName = "dbos_hs_validation"
  voidUse conn $ Session.script ("DROP SCHEMA IF EXISTS " <> schemaName <> " CASCADE;")
  voidUse conn $ Session.script ("CREATE SCHEMA " <> schemaName <> ";")
  voidUse conn $ Session.script (renderMigration (SchemaName schemaName) migration1)
  tables <- use conn $ Session.statement schemaName selectTablesStatement
  voidUse conn $ Session.script ("DROP SCHEMA " <> schemaName <> " CASCADE;")
  pure tables

------------------------------------------------------------
-- hasql helpers
------------------------------------------------------------

use :: Connection.Connection -> Session.Session a -> IO a
use conn session = do
  result <- Connection.use conn session
  case result of
    Left err -> error (show err)
    Right a  -> pure a

voidUse :: Connection.Connection -> Session.Session () -> IO ()
voidUse conn = use conn

------------------------------------------------------------
-- Connection helper
------------------------------------------------------------

withConnection :: (Connection.Connection -> IO a) -> IO a
withConnection action = do
  settings <- testSettings
  bracket (Connection.acquire settings) releaseConnection $ \case
    Left err -> error ("failed to connect to PostgreSQL: " <> show err)
    Right conn -> action conn

testSettings :: IO Settings.Settings
testSettings = do
  host <- textEnvDefault "DBOS_TEST_PGHOST" "localhost"
  user <- textEnvDefault "DBOS_TEST_PGUSER" "postgres"
  database <- textEnvDefault "DBOS_TEST_PGDATABASE" "dbos_starter_clojure"
  password <- textEnvDefault "PGPASSWORD" "dbos"
  pure $
    Settings.host host
      <> Settings.user user
      <> Settings.dbname database
      <> Settings.password password

releaseConnection :: Either err Connection.Connection -> IO ()
releaseConnection = either (const (pure ())) Connection.release

textEnvDefault :: String -> Text -> IO Text
textEnvDefault name fallback = do
  value <- lookupEnv name
  pure (maybe fallback Text.pack value)

------------------------------------------------------------
-- Expectations
------------------------------------------------------------

expectTables :: Vector.Vector Text -> IO ()
expectTables tables = do
  let names = Vector.toList tables
  required `expectEach` names
  where
    required =
      [ "event_dispatch_kv"
      , "notifications"
      , "operation_outputs"
      , "streams"
      , "workflow_events"
      , "workflow_status"
      ]

expectEach :: (Eq a, Show a, Foldable t) => t a -> [a] -> IO ()
expected `expectEach` actual =
  mapM_ (\x -> case x `Prelude.elem` actual of
    True  -> pure ()
    False -> error ("expected element not found: " <> show x)
  ) expected
