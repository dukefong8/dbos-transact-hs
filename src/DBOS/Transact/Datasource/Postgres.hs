{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | Application-pool binding for transactional steps (Rule 4: plain
-- Haskell, no Bluefin imports). The live 'DataSource' behind
-- 'DBOS.Transact.Datasource': a hasql pool for affinity-free sessions
-- (pre-checks, verification) plus one raw connection per transaction,
-- driven with explicit @BEGIN@/@COMMIT@/@ROLLBACK@ so an @IO@ body
-- sequenced between statements shares the commit (ADR-0021 addendum —
-- @runTransactionAt@ only accepts closed @Tx.Transaction@ bodies, which
-- an @IO@ body cannot join). Checkpoint statements are hand-written
-- 'Statement' values over the fixed @dbos@ schema (ADR-0011: typedSql
-- sessions do not compose, and here there is no transaction to compose
-- into — each statement runs via 'txStatement' on the held connection).
-- Verify-only: tables are checked, never migrated (ADR-0004).
module DBOS.Transact.Datasource.Postgres
  ( AppDataSource,
    acquireAppDataSource,
    acquireAppDataSourceIn,
    releaseAppDataSource,
    verifyAppDataSource,
    runAppSession,
    toDataSource,
    beginSql,
  )
where

import DBOS.Prelude
import Control.Monad.Class.MonadThrow qualified as MThrow
import Data.Int (Int32)
import Data.Maybe (isJust)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Char (isAsciiLower, isAsciiUpper, isDigit)
import Hasql.Connection qualified as Connection
import Hasql.Connection.Settings qualified as ConnSettings
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.Pool qualified as Pool
import Hasql.Pool.Config qualified as PoolConfig
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import Data.Functor.Contravariant (contramap)
import DBOS.SystemDB.Error (BackendError (..), BackendErrorKind (..), Error (..), invalidInput, renderError)
import DBOS.SystemDB.Postgres qualified as SystemPostgres (classifyUsageError)
import DBOS.Transact.Datasource (DataSource (..), IsolationLevel (..), RecordedOutcome (..), Tx (..))
import DBOS.SystemDB.Types (WorkflowId (..))

-- | An application database behind a transactional-step datasource: a
-- pool for sessions plus the settings to open per-transaction
-- connections from, and the schema holding @transaction_completion@
-- (default @dbos@, the oracle's @schemaName@ parameter). Checkpoint SQL
-- interpolates the validated schema.
data AppDataSource = AppDataSource
  { appSessionPool :: Pool.Pool,
    appSettings :: ConnSettings.Settings,
    appSchema :: Text
  }

-- | Open the pool over the default @dbos@ checkpoint schema. Never
-- migrates; 'verifyAppDataSource' gates serving.
acquireAppDataSource :: Text -> Int -> IO AppDataSource
acquireAppDataSource = acquireAppDataSourceIn "dbos"

-- | Open the pool over a named checkpoint schema, the oracle's
-- @schemaName@ parameter. The name is validated and quoted before it
-- reaches SQL; anything but @[A-Za-z_][A-Za-z0-9_]*@ is refused the way
-- the system backend refuses a schema.
acquireAppDataSourceIn :: Text -> Text -> Int -> IO AppDataSource
acquireAppDataSourceIn schema url maxConnections = do
  if validSchemaName schema
    then pure ()
    else MThrow.throwIO (invalidInput "schema" ("not a usable schema name: " <> schema))
  let settings = ConnSettings.connectionString url
  pool <-
    Pool.acquire
      ( PoolConfig.settings
          [ PoolConfig.size (fromIntegral maxConnections),
            PoolConfig.acquisitionTimeout 10,
            PoolConfig.agingTimeout 1800,
            PoolConfig.idlenessTimeout 1800,
            PoolConfig.staticConnectionSettings settings
          ]
      )
  pure (AppDataSource pool settings schema)

-- | A schema name that is safe to interpolate as a quoted identifier.
validSchemaName :: Text -> Bool
validSchemaName name =
  not (Text.null name)
    && (isAsciiLower first || isAsciiUpper first || first == '_')
    && Text.all (\c -> c == '_' || isAsciiLower c || isAsciiUpper c || isDigit c) name
  where
    first = Text.head name

-- | A validated schema name, double-quoted for SQL.
quoteIdent :: Text -> Text
quoteIdent name = "\"" <> Text.replace "\"" "\"\"" name <> "\""

-- | Release the pool.
releaseAppDataSource :: AppDataSource -> IO ()
releaseAppDataSource app = Pool.release app.appSessionPool

-- | The @BEGIN@ opening a transaction at the requested isolation, or a
-- bare @BEGIN@ for the database default.
beginSql :: Maybe IsolationLevel -> Text
beginSql isolation =
  case isolation of
    Nothing -> "BEGIN"
    Just level -> "BEGIN TRANSACTION ISOLATION LEVEL " <> levelSql level
  where
    levelSql ReadUncommitted = "READ UNCOMMITTED"
    levelSql ReadCommitted = "READ COMMITTED"
    levelSql RepeatableRead = "REPEATABLE READ"
    levelSql Serializable = "SERIALIZABLE"

-- | One session through the pool, outside any transaction: pre-checks,
-- verification probes, and reads the tests own. Checkpoint writes never
-- go through here — they ride the held connection inside
-- 'dsWithTransaction'.
runAppSession :: AppDataSource -> Session.Session a -> IO (Either BackendError a)
runAppSession app session = do
  result <- Pool.use app.appSessionPool session
  pure (either (Left . sessionErr) Right result)

-- | Failures through the single funnel the system backend uses.
sessionErr :: Pool.UsageError -> BackendError
sessionErr usage = case SystemPostgres.classifyUsageError usage of
  Backend err -> err
  other -> BackendError {backendMessage = renderError other, backendSqlState = Nothing, backendKind = Permanent}

-- | The checkpoint table exists. Verify-only: a missing table is a
-- permanent backend error, never a migration.
verifyAppDataSource :: AppDataSource -> IO (Either BackendError ())
verifyAppDataSource app = do
  found <- runAppSession app (existsSession app.appSchema)
  pure $ case found of
    Left err -> Left err
    Right False ->
      Left
        ( BackendError
            { backendMessage = "application database has no " <> app.appSchema <> ".transaction_completion table",
              backendSqlState = Nothing,
              backendKind = Permanent
            }
        )
    Right True -> Right ()
  where
    existsSession :: Text -> Session.Session Bool
    existsSession schema =
      Session.statement schema $
        Statement.preparable
          "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = $1 AND tablename = 'transaction_completion')"
          (Encoders.param (Encoders.nonNullable Encoders.text))
          (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.bool)))

-- | The checkpoint lookup: recorded output and error, if any, from the
-- given schema.
checkTxStatement :: Text -> Statement.Statement (Text, Int32) (Maybe (Maybe Text, Maybe Text))
checkTxStatement schema =
  Statement.preparable
    ("SELECT output, error FROM " <> quoteIdent schema <> ".transaction_completion WHERE workflow_id = $1 AND function_num = $2")
    (contramap fst (Encoders.param (Encoders.nonNullable Encoders.text))
      <> contramap snd (Encoders.param (Encoders.nonNullable Encoders.int4)))
    (Decoders.rowMaybe ((,) <$> Decoders.column (Decoders.nullable Decoders.text) <*> Decoders.column (Decoders.nullable Decoders.text)))

-- | The checkpoint write: a held row reports absence (adopt it), never a
-- unique violation (a violation from the application's own tables is its
-- failure, not a conflict).
recordTxStatement :: Text -> Statement.Statement (Text, Int32, Text, Bool) Bool
recordTxStatement schema =
  Statement.preparable
    ("INSERT INTO " <> quoteIdent schema <> ".transaction_completion (workflow_id, function_num, output, error) VALUES ($1, $2, CASE WHEN $4 THEN NULL ELSE $3 END, CASE WHEN $4 THEN $3 ELSE NULL END) ON CONFLICT (workflow_id, function_num) DO NOTHING RETURNING TRUE")
    (contramap (\(a, _, _, _) -> a) (Encoders.param (Encoders.nonNullable Encoders.text))
      <> contramap (\(_, b, _, _) -> b) (Encoders.param (Encoders.nonNullable Encoders.int4))
      <> contramap (\(_, _, c, _) -> c) (Encoders.param (Encoders.nonNullable Encoders.text))
      <> contramap (\(_, _, _, d) -> d) (Encoders.param (Encoders.nonNullable Encoders.bool)))
    (isJust <$> Decoders.rowMaybe (Decoders.column (Decoders.nonNullable Decoders.bool)))

-- | Run one session on the held connection, throwing backend failures so
-- the bracket rolls the attempt back.
txRunner :: Connection.Connection -> Statement.Statement params result -> params -> IO result
txRunner conn stmt params = do
  ran <- Connection.use conn (Session.statement params stmt)
  case ran of
    Left se -> MThrow.throwIO (Backend (sessionErr (Pool.SessionUsageError se)))
    Right value -> pure value

-- | The live 'DataSource': pre-checks through the pool, transactions on
-- per-attempt raw connections, checkpoints through the held runner.
toDataSource :: AppDataSource -> DataSource IO
toDataSource app =
  DataSource
    { dsName = "app-db",
      dsSchema = app.appSchema,
      dsCheck = \(WorkflowId widText) step -> do
        found <- runAppSession app (Session.statement (widText, fromIntegral step) (checkTxStatement app.appSchema))
        pure $ case found of
          Left err -> Left err
          Right Nothing -> Right Nothing
          Right (Just (output, recordedError)) -> case recordedError of
            Just message -> Right (Just (RecordedError message))
            Nothing -> case output of
              Just text -> Right (Just (RecordedOutput text))
              Nothing -> Right Nothing,
      dsWithTransaction = \isolation action ->
        MThrow.bracket acquireConn Connection.release $ \conn -> do
          began <- Connection.use conn (Session.script (beginSql isolation))
          case began of
            Left se -> pure (Left (sessionErr (Pool.SessionUsageError se)))
            Right () -> do
              outcome <- MThrow.try (action (Tx (txRunner conn)))
              case outcome of
                Left sysErr -> rollbackQuiet conn >> pure (Left (unwrapBackend sysErr))
                Right value -> do
                  done <- Connection.use conn (Session.script "COMMIT")
                  case done of
                    Left se -> pure (Left (sessionErr (Pool.SessionUsageError se)))
                    Right () -> pure (Right value),
      dsRecordOutput = \(Tx run) (WorkflowId widText) step text ->
        run (recordTxStatement app.appSchema) (widText, fromIntegral step, text, False),
      dsRecordError = \(Tx run) (WorkflowId widText) step text ->
        run (recordTxStatement app.appSchema) (widText, fromIntegral step, text, True)
    }
  where
    acquireConn :: IO Connection.Connection
    acquireConn = do
      acquired <- Connection.acquire app.appSettings
      case acquired of
        Left ce ->
          MThrow.throwIO
            ( Backend
                ( BackendError
                    { backendMessage = showText ce,
                      backendSqlState = Nothing,
                      backendKind = Connection
                    }
                )
            )
        Right conn -> pure conn
    rollbackQuiet :: Connection.Connection -> IO ()
    rollbackQuiet conn = do
      _ <- Connection.use conn (Session.script "ROLLBACK")
      pure ()
    unwrapBackend :: Error -> BackendError
    unwrapBackend err = case err of
      Backend backend -> backend
      _ -> BackendError {backendMessage = renderError err, backendSqlState = Nothing, backendKind = Permanent}
