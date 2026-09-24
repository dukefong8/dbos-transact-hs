{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | What can go wrong talking to the system database. Mirrors Rust
-- @sysdb::error@: shared by the retry layer, the input validators and any
-- second backend, and deliberately backend-agnostic. Plain Haskell, no
-- Bluefin imports.
module DBOS.SystemDB.Error
  ( Error (..),
    BackendError (..),
    BackendErrorKind (..),
    invalidInput,
    renderError,
    renderBackendError,
  )
where

import Control.Exception (Exception)
import Data.Int (Int64)
import Data.Text (Text)
import Data.Text qualified as Text

-- | What went wrong talking to the system database. Names match the Rust
-- @Error@ variants exactly, and the id payloads are plain 'Text' — the
-- oracle's fields are @String@, so this module stays a leaf exactly as
-- @error.rs@ is. Serializable
-- in the oracle (a failed step's error replays from the column); that
-- encoding lands with the checkpoint path, so there are no JSON instances
-- yet.
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
