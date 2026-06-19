{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE QuantifiedConstraints #-}

module DbosTransact.SystemDB.Postgres
  ( PostgresError
  , OperationResult(..)
  , runMigrations
  , insertWorkflowStatus
  , updateWorkflowOutcome
  , awaitWorkflowResult
  , checkOperationExecution
  , recordOperationResult
  , queryTx
  , commandTx
  ) where
import Bluefin.Eff (Eff, type (<:))

import Control.Exception (bracket)
import Control.Monad (void)
import Data.Aeson (Value, decode, encode)
import Data.ByteString.Lazy qualified as LBS
import Data.Foldable (traverse_)
import Data.Int (Int32)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text.Encoding
import DbosTransact.Effects (TransactionScope)
import DbosTransact.SystemDB.Schema (SchemaName(..), allMigrations, renderMigration)
import DbosTransact.Workflow (WorkflowStatus(..), WorkflowStatusType(..))
import Hasql.Connection qualified as Connection
import Hasql.Connection.Settings qualified as Settings
import Hasql.Session qualified as Session
import Hasql.Statement (Statement)
import Hasql.TH (maybeStatement, resultlessStatement, singletonStatement)

type PostgresError = Text

data OperationResult
  = OperationSucceeded Value
  | OperationFailed Text
  deriving stock (Eq, Show)

runMigrations :: Settings.Settings -> SchemaName -> IO (Either PostgresError ())
runMigrations settings schemaName@(SchemaName schemaText) =
  runHasql settings $ do
    Session.script ("CREATE SCHEMA IF NOT EXISTS " <> schemaText <> ";")
    traverse_ (Session.script . renderMigration schemaName) allMigrations

insertWorkflowStatus :: Settings.Settings -> SchemaName -> WorkflowStatus -> IO (Either PostgresError ())
insertWorkflowStatus settings schemaName status =
  runInSchema settings schemaName $ do
    setSearchPath schemaName
    Session.statement params insertWorkflowStatusStatement
  where
    params =
      ( statusWorkflowId status
      , statusTypeToText (statusType status)
      , statusName status
      , valueText <$> statusInput status
      , statusQueueName status
      , fromIntegral (statusPriority status) :: Int32
      )

updateWorkflowOutcome :: Settings.Settings -> SchemaName -> Text -> Either Text Value -> IO (Either PostgresError ())
updateWorkflowOutcome settings schemaName workflowID outcome =
  runInSchema settings schemaName $ do
    setSearchPath schemaName
    Session.statement params updateWorkflowOutcomeStatement
  where
    params = case outcome of
      Right output -> (workflowID, "SUCCESS", Just (valueText output), Nothing)
      Left err -> (workflowID, "ERROR", Nothing, Just err)

awaitWorkflowResult :: Settings.Settings -> SchemaName -> Text -> IO (Either PostgresError (Maybe (Text, Maybe Text, Maybe Text)))
awaitWorkflowResult settings schemaName workflowID =
  runInSchema settings schemaName $ do
    setSearchPath schemaName
    Session.statement workflowID selectWorkflowResultStatement

checkOperationExecution :: Settings.Settings -> SchemaName -> Text -> Int32 -> Text -> IO (Either PostgresError (Maybe OperationResult))
checkOperationExecution settings schemaName workflowID functionID expectedName = do
  rowResult <- runInSchema settings schemaName $ do
    setSearchPath schemaName
    Session.statement (workflowID, functionID) selectOperationStatement
  pure $ do
    row <- rowResult
    case row of
      Nothing -> Right Nothing
      Just (storedName, outputText, errorText)
        | storedName /= expectedName -> Left ("operation name mismatch: expected " <> expectedName <> ", saw " <> storedName)
        | Just err <- errorText -> Right (Just (OperationFailed err))
        | Just output <- outputText ->
            case decodeValueText output of
              Just value -> Right (Just (OperationSucceeded value))
              Nothing -> Left ("failed to decode operation output for workflow " <> workflowID)
        | otherwise -> Right Nothing

recordOperationResult :: Settings.Settings -> SchemaName -> Text -> Int32 -> Text -> Either Text Value -> IO (Either PostgresError ())
recordOperationResult settings schemaName workflowID functionID functionName outcome =
  runInSchema settings schemaName $ do
    setSearchPath schemaName
    Session.statement params recordOperationResultStatement
  where
    params = case outcome of
      Right output -> (workflowID, functionID, functionName, Just (valueText output), Nothing)
      Left err -> (workflowID, functionID, functionName, Nothing, Just err)

queryTx :: forall stmt tx es params result. (tx <: es, stmt ~ Statement)
  => TransactionScope stmt tx -> Statement params result -> params -> Eff es result
queryTx = error "DbosTransact.SystemDB.Postgres.queryTx: use TransactionScope directly with hasql Session"

commandTx :: forall stmt tx es params. (tx <: es, stmt ~ Statement)
  => TransactionScope stmt tx -> Statement params () -> params -> Eff es ()
commandTx = error "DbosTransact.SystemDB.Postgres.commandTx: use TransactionScope directly with hasql Session"

runInSchema :: Settings.Settings -> SchemaName -> Session.Session a -> IO (Either PostgresError a)
runInSchema settings _schemaName session = runHasql settings session

runHasql :: Settings.Settings -> Session.Session a -> IO (Either PostgresError a)
runHasql settings session =
  bracket (Connection.acquire settings) releaseConnection $ \case
    Left err -> pure (Left (Text.pack (show err)))
    Right connection -> do
      result <- Connection.use connection session
      pure (either (Left . Text.pack . show) Right result)

releaseConnection :: Either err Connection.Connection -> IO ()
releaseConnection = either (const (pure ())) Connection.release

setSearchPath :: SchemaName -> Session.Session ()
setSearchPath (SchemaName schemaName) =
  void (Session.statement schemaName setSearchPathStatement)


statusTypeToText :: WorkflowStatusType -> Text
statusTypeToText WorkflowPending = "PENDING"
statusTypeToText WorkflowEnqueued = "ENQUEUED"
statusTypeToText WorkflowDelayed = "PENDING"
statusTypeToText WorkflowSuccess = "SUCCESS"
statusTypeToText WorkflowError = "ERROR"
statusTypeToText WorkflowCancelled = "CANCELLED"
statusTypeToText WorkflowMaxRecoveryAttemptsExceeded = "MAX_RECOVERY_ATTEMPTS_EXCEEDED"

valueText :: Value -> Text
valueText = Text.Encoding.decodeUtf8 . LBS.toStrict . encode

decodeValueText :: Text -> Maybe Value
decodeValueText = decode . LBS.fromStrict . Text.Encoding.encodeUtf8

setSearchPathStatement :: Statement Text Text
setSearchPathStatement =
  [singletonStatement|
    select set_config('search_path', $1 :: text, false) :: text
  |]

insertWorkflowStatusStatement :: Statement (Text, Text, Text, Maybe Text, Maybe Text, Int32) ()
insertWorkflowStatusStatement =
  [resultlessStatement|
    insert into workflow_status (workflow_uuid, status, name, inputs, queue_name, priority)
    values ($1 :: text, $2 :: text, $3 :: text, $4 :: text?, $5 :: text?, $6 :: int4)
    on conflict (workflow_uuid) do nothing
  |]

updateWorkflowOutcomeStatement :: Statement (Text, Text, Maybe Text, Maybe Text) ()
updateWorkflowOutcomeStatement =
  [resultlessStatement|
    update workflow_status
    set status = $2 :: text,
        output = $3 :: text?,
        error = $4 :: text?
    where workflow_uuid = $1 :: text
  |]

selectWorkflowResultStatement :: Statement Text (Maybe (Text, Maybe Text, Maybe Text))
selectWorkflowResultStatement =
  [maybeStatement|
    select status :: text, output :: text?, error :: text?
    from workflow_status
    where workflow_uuid = $1 :: text
  |]

selectOperationStatement :: Statement (Text, Int32) (Maybe (Text, Maybe Text, Maybe Text))
selectOperationStatement =
  [maybeStatement|
    select function_name :: text, output :: text?, error :: text?
    from operation_outputs
    where workflow_uuid = $1 :: text
      and function_id = $2 :: int4
  |]

recordOperationResultStatement :: Statement (Text, Int32, Text, Maybe Text, Maybe Text) ()
recordOperationResultStatement =
  [resultlessStatement|
    insert into operation_outputs (workflow_uuid, function_id, function_name, output, error)
    values ($1 :: text, $2 :: int4, $3 :: text, $4 :: text?, $5 :: text?)
    on conflict (workflow_uuid, function_id) do update set
      function_name = excluded.function_name,
      output = excluded.output,
      error = excluded.error
  |]
