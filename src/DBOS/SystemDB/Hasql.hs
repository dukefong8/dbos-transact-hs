{-# LANGUAGE OverloadedStrings #-}

module DBOS.SystemDB.Hasql
  ( Pool.Pool,
    acquirePool,
    fetchNotification,
    fetchOperationCheckpoint,
    fetchWorkflowExecutionRow,
    fetchWorkflowStatus,
    recordOperationOutput,
    releasePool,
    runDb,
    runDbOrFail,
    tryStartWorkflow,
    updateWorkflowOutcome,
    WorkflowStartDecision (..),
  )
where

import Data.Text (Text)
import Data.Text qualified as Text
import Data.Word (Word16)
import DBOS.SystemDB.Queries
  ( fetchNotificationSession,
    fetchOperationCheckpointSession,
    fetchWorkflowExecutionRowSession,
    fetchWorkflowStatusSession,
    recordOperationOutputSession,
    tryStartWorkflowSession,
    updateWorkflowOutcomeSession,
  )
import DBOS.SystemDB.Types (MessageUUID, NotificationRow)
import DBOS.Transact.OperationCheckpointTypes
  ( OperationCheckpoint,
    OperationId,
    OperationName,
  )
import DBOS.Transact.WorkflowExecutionStatus (WorkflowStatus)
import DBOS.Transact.WorkflowExecutionTypes
  ( SerializedWorkflowValue,
    WorkflowExecutionRow,
    WorkflowId,
    WorkflowName,
  )
import Hasql.Connection.Settings qualified as Connection
import Hasql.Pool qualified as Pool
import Hasql.Pool.Config qualified as PoolConfig
import Hasql.Session (Session)
import System.Environment (lookupEnv)
import Text.Read (readMaybe)

data WorkflowStartDecision
  = StartWorkflow
  | AwaitWorkflow
  deriving (Eq, Show)

acquirePool :: IO Pool.Pool
acquirePool = do
  settings <- getConnectionSettings
  Pool.acquire
    ( PoolConfig.settings
        [ PoolConfig.size 8,
          PoolConfig.acquisitionTimeout 10,
          PoolConfig.agingTimeout 1800,
          PoolConfig.idlenessTimeout 1800,
          PoolConfig.staticConnectionSettings settings
        ]
    )

releasePool :: Pool.Pool -> IO ()
releasePool =
  Pool.release

runDb :: Pool.Pool -> Session a -> IO (Either Pool.UsageError a)
runDb =
  Pool.use

runDbOrFail :: Pool.Pool -> Session a -> IO a
runDbOrFail pool session = do
  result <- runDb pool session
  case result of
    Left err -> fail (show err)
    Right value -> pure value

fetchWorkflowExecutionRow ::
  Pool.Pool ->
  WorkflowId ->
  IO (Maybe WorkflowExecutionRow)
fetchWorkflowExecutionRow pool workflowId =
  runDbOrFail pool (fetchWorkflowExecutionRowSession workflowId)

fetchWorkflowStatus ::
  Pool.Pool ->
  WorkflowId ->
  IO (Maybe WorkflowStatus)
fetchWorkflowStatus pool workflowId =
  runDbOrFail pool (fetchWorkflowStatusSession workflowId)

fetchOperationCheckpoint ::
  Pool.Pool ->
  WorkflowId ->
  OperationId ->
  IO (Maybe OperationCheckpoint)
fetchOperationCheckpoint pool workflowId operationId =
  runDbOrFail pool (fetchOperationCheckpointSession workflowId operationId)

fetchNotification ::
  Pool.Pool ->
  MessageUUID ->
  IO (Maybe NotificationRow)
fetchNotification pool messageUUID =
  runDbOrFail pool (fetchNotificationSession messageUUID)

tryStartWorkflow ::
  Pool.Pool ->
  WorkflowId ->
  WorkflowName ->
  IO WorkflowStartDecision
tryStartWorkflow pool workflowId workflowName = do
  started <- runDbOrFail pool (tryStartWorkflowSession workflowId workflowName)
  pure $
    if started
      then StartWorkflow
      else AwaitWorkflow

updateWorkflowOutcome ::
  Pool.Pool ->
  WorkflowId ->
  WorkflowStatus ->
  Maybe SerializedWorkflowValue ->
  Maybe SerializedWorkflowValue ->
  IO ()
updateWorkflowOutcome pool workflowId status output errorValue =
  runDbOrFail pool (updateWorkflowOutcomeSession workflowId status output errorValue)

recordOperationOutput ::
  Pool.Pool ->
  WorkflowId ->
  OperationId ->
  OperationName ->
  SerializedWorkflowValue ->
  IO ()
recordOperationOutput pool workflowId operationId operationName output =
  runDbOrFail pool (recordOperationOutputSession workflowId operationId operationName output)

getConnectionSettings :: IO Connection.Settings
getConnectionSettings = do
  databaseURL <- lookupEnv "DBOS_DATABASE_URL"
  case databaseURL of
    Just url -> pure (Connection.connectionString (toText url))
    Nothing -> getConnectionSettingsFromPGEnv

getConnectionSettingsFromPGEnv :: IO Connection.Settings
getConnectionSettingsFromPGEnv = do
  host <- lookupEnv "PGHOST"
  port <- lookupEnv "PGPORT"
  dbname <- lookupEnv "PGDATABASE"
  user <- lookupEnv "PGUSER"
  password <- lookupEnv "PGPASSWORD"
  pure $
    mconcat
      [ Connection.hostAndPort
          (maybe "127.0.0.1" toText host)
          (maybe 5432 parsePort port),
        Connection.dbname (maybe "dbos" toText dbname),
        Connection.user (maybe "postgres" toText user),
        Connection.password (maybe "pgpasswd" toText password)
      ]

parsePort :: String -> Word16
parsePort raw =
  maybe 5432 fromIntegral (readMaybe raw :: Maybe Int)

toText :: String -> Text
toText =
  Text.pack
