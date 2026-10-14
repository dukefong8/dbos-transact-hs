{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | What can go wrong talking to the system database. Mirrors Rust
-- @sysdb::error@: shared by the retry layer, the input validators and any
-- second backend, and deliberately backend-agnostic.
module DBOS.SystemDB.Error
  ( Error (..),
    BackendError (..),
    BackendErrorKind (..),
    invalidInput,
    renderError,
    renderBackendError,
  )
where

import DBOS.Prelude
import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, withText, (.:), (.=))
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Int (Int64)
import Data.Text qualified as Text

-- | What went wrong talking to the system database. Names match the Rust
    -- @Error@ variants exactly, and the id payloads are plain 'Text' — the
    -- oracle's fields are @String@, so this module stays a leaf exactly as
    -- @error.rs@ is. Its serde-compatible instances let failed operations
    -- replay this typed error from their checkpoint.
data Error
  = Backend BackendError
  | Malformed Text
  | ConflictingWorkflow {workflowId :: Text, detail :: Text}
  | ConcurrentRecv {workflowId :: Text, topic :: Maybe Text}
  | InvalidInput {field :: Text, detail :: Text}
  | QueueDeduplicated {workflowId :: Text, queueName :: Text, deduplicationId :: Text}
  | WorkflowCancelled {workflowId :: Text}
  | UnexpectedStep {workflowId :: Text, stepId :: Int, expected :: Text, recorded :: Text}
  | StepAlreadyRecorded {workflowId :: Text, stepId :: Int}
  | NoForkPoint {workflowIds :: [Text], stepName :: Maybe Text}
  | NonExistentWorkflow {workflowIds :: [Text]}
  | ErrorMaxRecoveryAttemptsExceeded {workflowId :: Text, limit :: Int64}
  | AlreadyRegistered {kind :: Text, name :: Text}
  | NotRegistered {kind :: Text, name :: Text}
  | RegisteredByAnother {kind :: Text, name :: Text, holder :: Text, claimant :: Maybe Text}
  deriving stock (Eq, Show)

-- | A failure the database or its driver reported. Mirrors Rust
-- @BackendError@.
data BackendError = BackendError
  { backendMessage :: Text,
    backendSqlState :: Maybe Text,
    backendKind :: BackendErrorKind
  }
  deriving stock (Eq, Show)

-- NOTE (deviation): the parked-workflow variant is spelled
-- @ErrorMaxRecoveryAttemptsExceeded@ rather than the Rust
-- @MaxRecoveryAttemptsExceeded@, because 'DBOS.SystemDB.WorkflowStatus'
-- carries the same word from @types.rs@ and Haskell modules share one
-- constructor namespace. Documented per the AGENTS HARD RULE's
-- constructor-prefix escape hatch.

-- | Whether a backend failure is worth asking again about. Classification
-- is the backend's job, not the retry loop's. Mirrors Rust
-- @BackendErrorKind@.
data BackendErrorKind
  = Connection
  | Transient
  | Permanent
  deriving stock (Eq, Show)

instance ToJSON Error where
  toJSON = \case
    Backend err -> tagged "Backend" (toJSON err)
    Malformed message -> tagged "Malformed" (toJSON message)
    ConflictingWorkflow {workflowId, detail} -> tagged "ConflictingWorkflow" (object ["workflow_id" .= workflowId, "detail" .= detail])
    ConcurrentRecv {workflowId, topic} -> tagged "ConcurrentRecv" (object ["workflow_id" .= workflowId, "topic" .= topic])
    InvalidInput {field, detail} -> tagged "InvalidInput" (object ["field" .= field, "detail" .= detail])
    QueueDeduplicated {workflowId, queueName, deduplicationId} -> tagged "QueueDeduplicated" (object ["workflow_id" .= workflowId, "queue_name" .= queueName, "deduplication_id" .= deduplicationId])
    WorkflowCancelled {workflowId} -> tagged "WorkflowCancelled" (object ["workflow_id" .= workflowId])
    UnexpectedStep {workflowId, stepId, expected, recorded} -> tagged "UnexpectedStep" (object ["workflow_id" .= workflowId, "step_id" .= stepId, "expected" .= expected, "recorded" .= recorded])
    StepAlreadyRecorded {workflowId, stepId} -> tagged "StepAlreadyRecorded" (object ["workflow_id" .= workflowId, "step_id" .= stepId])
    NoForkPoint {workflowIds, stepName} -> tagged "NoForkPoint" (object ["workflow_ids" .= workflowIds, "step_name" .= stepName])
    NonExistentWorkflow {workflowIds} -> tagged "NonExistentWorkflow" (object ["workflow_ids" .= workflowIds])
    -- Tag keeps the Rust spelling; see the decode side.
    ErrorMaxRecoveryAttemptsExceeded {workflowId, limit} -> tagged "MaxRecoveryAttemptsExceeded" (object ["workflow_id" .= workflowId, "limit" .= limit])
    AlreadyRegistered {kind, name} -> tagged "AlreadyRegistered" (object ["kind" .= kind, "name" .= name])
    NotRegistered {kind, name} -> tagged "NotRegistered" (object ["kind" .= kind, "name" .= name])
    RegisteredByAnother {kind, name, holder, claimant} -> tagged "RegisteredByAnother" (object ["kind" .= kind, "name" .= name, "holder" .= holder, "claimant" .= claimant])
    where
      tagged name payload = object [name .= payload]

instance FromJSON Error where
  parseJSON = withObject "SystemDB.Error" $ \fields -> case KeyMap.toList fields of
    [(tag, payload)] -> case Key.toText tag of
      "Backend" -> Backend <$> parseJSON payload
      "Malformed" -> Malformed <$> parseJSON payload
      "ConflictingWorkflow" -> withObject "ConflictingWorkflow" (\o -> ConflictingWorkflow <$> o .: "workflow_id" <*> o .: "detail") payload
      "ConcurrentRecv" -> withObject "ConcurrentRecv" (\o -> ConcurrentRecv <$> o .: "workflow_id" <*> o .: "topic") payload
      "InvalidInput" -> withObject "InvalidInput" (\o -> InvalidInput <$> o .: "field" <*> o .: "detail") payload
      "QueueDeduplicated" -> withObject "QueueDeduplicated" (\o -> QueueDeduplicated <$> o .: "workflow_id" <*> o .: "queue_name" <*> o .: "deduplication_id") payload
      "WorkflowCancelled" -> withObject "WorkflowCancelled" (\o -> WorkflowCancelled <$> o .: "workflow_id") payload
      "UnexpectedStep" -> withObject "UnexpectedStep" (\o -> UnexpectedStep <$> o .: "workflow_id" <*> o .: "step_id" <*> o .: "expected" <*> o .: "recorded") payload
      "StepAlreadyRecorded" -> withObject "StepAlreadyRecorded" (\o -> StepAlreadyRecorded <$> o .: "workflow_id" <*> o .: "step_id") payload
      "NoForkPoint" -> withObject "NoForkPoint" (\o -> NoForkPoint <$> o .: "workflow_ids" <*> o .: "step_name") payload
      "NonExistentWorkflow" -> withObject "NonExistentWorkflow" (\o -> NonExistentWorkflow <$> o .: "workflow_ids") payload
      -- Tag keeps the Rust spelling: the oracle names this variant
      -- @MaxRecoveryAttemptsExceeded@, and the port's @Error@ prefix is
      -- collision avoidance only (constructors, never wire names).
      "MaxRecoveryAttemptsExceeded" -> withObject "MaxRecoveryAttemptsExceeded" (\o -> ErrorMaxRecoveryAttemptsExceeded <$> o .: "workflow_id" <*> o .: "limit") payload
      "AlreadyRegistered" -> withObject "AlreadyRegistered" (\o -> AlreadyRegistered <$> o .: "kind" <*> o .: "name") payload
      "NotRegistered" -> withObject "NotRegistered" (\o -> NotRegistered <$> o .: "kind" <*> o .: "name") payload
      "RegisteredByAnother" -> withObject "RegisteredByAnother" (\o -> RegisteredByAnother <$> o .: "kind" <*> o .: "name" <*> o .: "holder" <*> o .: "claimant") payload
      other -> fail ("unknown system database error variant " <> Text.unpack other)
    _ -> fail "a system database error must have one tagged variant"

instance ToJSON BackendError where
  toJSON BackendError {backendMessage, backendSqlState, backendKind} =
    object ["message" .= backendMessage, "sqlstate" .= backendSqlState, "kind" .= backendKind]

instance FromJSON BackendError where
  parseJSON = withObject "BackendError" $ \fields ->
    BackendError <$> fields .: "message" <*> fields .: "sqlstate" <*> fields .: "kind"

instance ToJSON BackendErrorKind where
  -- Tags keep the Rust spelling: the oracle names this variant @Conflict@,
  -- where the port's @Transient@ says what it means for retry.
  toJSON Connection = toJSON ("Connection" :: Text)
  toJSON Transient = toJSON ("Conflict" :: Text)
  toJSON Permanent = toJSON ("Permanent" :: Text)

instance FromJSON BackendErrorKind where
  parseJSON = withText "BackendErrorKind" $ \case
    "Connection" -> pure Connection
    "Conflict" -> pure Transient
    "Permanent" -> pure Permanent
    _ -> fail "unknown backend error kind"

instance Exception Error

-- | A caller supplied a value the layer will not store: which field, and
-- what was wrong with it. Mirrors the oracle's @Error::InvalidInput@
-- construction sites.
invalidInput :: Text -> Text -> Error
invalidInput fieldName detailText = InvalidInput {field = fieldName, detail = detailText}

-- | The human message. Mirrors Rust @Error@'s @Display@: the remedy is in
-- the message where no code can act on it.
renderError :: Error -> Text
renderError err =
  case err of
    Backend backend -> "system database error: " <> renderBackendError backend
    Malformed message -> "unexpected value in the system database: " <> message
    ConflictingWorkflow {workflowId = wid, detail = why} ->
      "workflow " <> wid <> " already exists: " <> why
    ConcurrentRecv {workflowId = wid, topic = Nothing} ->
      "workflow " <> wid <> " is already receiving"
    ConcurrentRecv {workflowId = wid, topic = Just name} ->
      "workflow " <> wid <> " is already receiving on topic " <> name
    InvalidInput {field = fieldName, detail = why} ->
      "invalid " <> fieldName <> ": " <> why
    QueueDeduplicated {workflowId = wid, queueName = queue, deduplicationId = key} ->
      "workflow " <> wid <> " (queue: " <> queue <> ", deduplication id: " <> key <> ") is already enqueued"
    WorkflowCancelled {workflowId = wid} ->
      "workflow " <> wid <> " is cancelled"
    UnexpectedStep {workflowId = wid, stepId = position, expected = want, recorded = got} ->
      "workflow " <> wid <> " step " <> Text.pack (show position) <> " was recorded as \"" <> got <> "\", but \"" <> want <> "\" was expected"
    StepAlreadyRecorded {workflowId = wid, stepId = position} ->
      "workflow " <> wid <> " step " <> Text.pack (show position) <> " was already recorded by another execution"
    NoForkPoint {workflowIds = ids, stepName = Nothing} ->
      "no steps in workflows " <> Text.intercalate ", " ids
    NoForkPoint {workflowIds = ids, stepName = Just name} ->
      "no step named " <> name <> " in workflows " <> Text.intercalate ", " ids
    NonExistentWorkflow {workflowIds = ids} ->
      "no such workflow: " <> Text.intercalate ", " ids
    ErrorMaxRecoveryAttemptsExceeded {workflowId = wid, limit = count} ->
      "workflow " <> wid <> " exceeded " <> Text.pack (show count) <> " recovery attempts"
    AlreadyRegistered {kind = what, name = taken} ->
      what <> " \"" <> taken <> "\" is already registered"
    NotRegistered {kind = what, name = missing} ->
      what <> " \"" <> missing <> "\" is not registered"
    RegisteredByAnother {kind = what, name = taken, holder = owner, claimant = Nothing} ->
      what <> " \"" <> taken <> "\" is already registered by application \"" <> owner <> "\" in this system database, and " <> lower <> " names must be unique across the applications sharing one"
      where lower = Text.toLower what
    RegisteredByAnother {kind = what, name = taken, holder = owner, claimant = Just requester} ->
      what <> " \"" <> taken <> "\" is already registered by application \"" <> owner <> "\" in this system database, and " <> lower <> " names must be unique across the applications sharing one: either give \"" <> requester <> "\" a different " <> lower <> " name, or, if \"" <> owner <> "\" was renamed to \"" <> requester <> "\", move its rows first"
      where lower = Text.toLower what
-- | The driver message, with the SQLSTATE when the database (not the
-- connection) failed. Mirrors @BackendError@'s @Display@.
renderBackendError :: BackendError -> Text
renderBackendError backend =
  case backend.backendSqlState of
    Just code -> backend.backendMessage <> " (" <> code <> ")"
    Nothing   -> backend.backendMessage
