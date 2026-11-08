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
    encodePortableError,
    decodePortableError,
    shouldRecord,
  )
where

import DBOS.Prelude
import Data.Aeson (FromJSON (..), Result (..), ToJSON (..), Value (..), eitherDecodeStrict', encode, fromJSON, object, withObject, withText, (.:), (.=))
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.ByteString.Lazy qualified as LBS
import Data.Int (Int64)
import Data.Text qualified as Text
import Data.Text.Encoding (decodeUtf8, encodeUtf8)

-- | What went wrong talking to the system database. Names match the Rust
    -- @Error@ variants exactly, and the id payloads are plain 'Text' — the
    -- oracle's fields are @String@, so this module stays a leaf exactly as
    -- @error.rs@ is. Its serde-compatible instances let failed operations
    -- replay this typed error from their checkpoint.
data Error
  = Backend BackendError
  | Malformed Text
  | -- | A recorded value carries a tag this process cannot read. Reported,
    -- never guessed: decoding bytes as another encoding would hand a replay
    -- a value the first run never saw. Mirrors Rust @UnknownSerialization@.
    UnknownSerialization {name :: Text}
  | ConflictingWorkflow {workflowId :: Text, detail :: Text}
  | -- | A workflow with this id already exists, and the caller asked for
    -- a new one with 'Reject'. Raised whatever the existing workflow's
    -- status, finished included, and before anything is written. An
    -- answer rather than a failure, so it is never retried. Mirrors Rust
    -- @Error::WorkflowIdInUse@. The name rides as 'existingName': 'name' is
    -- taken in this module by 'Text' fields, and under
    -- @DuplicateRecordFields@ a shared name must share its type. The wire
    -- key stays @"name"@, so the recorded shape matches the oracle.
    -- The status rides as text, as
    -- 'WorkflowNotRewindable' carries it: the oracle holds a typed
    -- @WorkflowStatus@, but this module is the leaf @Types@ imports, so a
    -- typed field would cycle. The wire shape is identical — a status
    -- serializes as its text either way.
    WorkflowIdInUse {workflowId :: Text, status :: Text, existingName :: Maybe Text}
  | InvalidInput {field :: Text, detail :: Text}
  | QueueDeduplicated {workflowId :: Text, queueName :: Text, deduplicationId :: Text}
  | WorkflowCancelled {workflowId :: Text}
  | UnexpectedStep {workflowId :: Text, stepId :: Int, expected :: Text, recorded :: Text}
  | StepNondeterminism {workflowId :: Text, stepId :: Int}
  | WorkflowConflict {workflowId :: Text}
  | NoForkPoint {workflowIds :: [Text], stepName :: Maybe Text}
  | NonExistentWorkflow {workflowIds :: [Text]}
  | WorkflowNotRewindable {workflowId :: Text, status :: Text}
  | RewindInterrupted {workflowId :: Text, status :: Text}
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
    UnknownSerialization {name} -> tagged "UnknownSerialization" (object ["name" .= name])
    ConflictingWorkflow {workflowId, detail} -> tagged "ConflictingWorkflow" (object ["workflow_id" .= workflowId, "detail" .= detail])
    WorkflowIdInUse {workflowId, status, existingName} -> tagged "WorkflowIdInUse" (object ["workflow_id" .= workflowId, "status" .= status, "name" .= existingName])
    InvalidInput {field, detail} -> tagged "InvalidInput" (object ["field" .= field, "detail" .= detail])
    QueueDeduplicated {workflowId, queueName, deduplicationId} -> tagged "QueueDeduplicated" (object ["workflow_id" .= workflowId, "queue_name" .= queueName, "deduplication_id" .= deduplicationId])
    WorkflowCancelled {workflowId} -> tagged "WorkflowCancelled" (object ["workflow_id" .= workflowId])
    UnexpectedStep {workflowId, stepId, expected, recorded} -> tagged "UnexpectedStep" (object ["workflow_id" .= workflowId, "step_id" .= stepId, "expected" .= expected, "recorded" .= recorded])
    StepNondeterminism {workflowId, stepId} -> tagged "StepNondeterminism" (object ["workflow_id" .= workflowId, "step_id" .= stepId])
    WorkflowConflict {workflowId} -> tagged "WorkflowConflict" (object ["workflow_id" .= workflowId])
    NoForkPoint {workflowIds, stepName} -> tagged "NoForkPoint" (object ["workflow_ids" .= workflowIds, "step_name" .= stepName])
    NonExistentWorkflow {workflowIds} -> tagged "NonExistentWorkflow" (object ["workflow_ids" .= workflowIds])
    WorkflowNotRewindable {workflowId, status} -> tagged "WorkflowNotRewindable" (object ["workflow_id" .= workflowId, "status" .= status])
    RewindInterrupted {workflowId, status} -> tagged "RewindInterrupted" (object ["workflow_id" .= workflowId, "status" .= status])
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
      "UnknownSerialization" -> withObject "UnknownSerialization" (\o -> UnknownSerialization <$> o .: "name") payload
      "ConflictingWorkflow" -> withObject "ConflictingWorkflow" (\o -> ConflictingWorkflow <$> o .: "workflow_id" <*> o .: "detail") payload
      "WorkflowIdInUse" -> withObject "WorkflowIdInUse" (\o -> WorkflowIdInUse <$> o .: "workflow_id" <*> o .: "status" <*> o .: "name") payload
      "InvalidInput" -> withObject "InvalidInput" (\o -> InvalidInput <$> o .: "field" <*> o .: "detail") payload
      "QueueDeduplicated" -> withObject "QueueDeduplicated" (\o -> QueueDeduplicated <$> o .: "workflow_id" <*> o .: "queue_name" <*> o .: "deduplication_id") payload
      "WorkflowCancelled" -> withObject "WorkflowCancelled" (\o -> WorkflowCancelled <$> o .: "workflow_id") payload
      "UnexpectedStep" -> withObject "UnexpectedStep" (\o -> UnexpectedStep <$> o .: "workflow_id" <*> o .: "step_id" <*> o .: "expected" <*> o .: "recorded") payload
      "StepNondeterminism" -> withObject "StepNondeterminism" (\o -> StepNondeterminism <$> o .: "workflow_id" <*> o .: "step_id") payload
      "WorkflowConflict" -> withObject "WorkflowConflict" (\o -> WorkflowConflict <$> o .: "workflow_id") payload
      "NoForkPoint" -> withObject "NoForkPoint" (\o -> NoForkPoint <$> o .: "workflow_ids" <*> o .: "step_name") payload
      "NonExistentWorkflow" -> withObject "NonExistentWorkflow" (\o -> NonExistentWorkflow <$> o .: "workflow_ids") payload
      "WorkflowNotRewindable" -> withObject "WorkflowNotRewindable" (\o -> WorkflowNotRewindable <$> o .: "workflow_id" <*> o .: "status") payload
      "RewindInterrupted" -> withObject "RewindInterrupted" (\o -> RewindInterrupted <$> o .: "workflow_id" <*> o .: "status") payload
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
    UnknownSerialization {name = tag} -> "serialization " <> tag <> " is not available"
    ConflictingWorkflow {workflowId = wid, detail = why} ->
      "workflow " <> wid <> " already exists: " <> why
    WorkflowIdInUse {workflowId = wid, status = current, existingName = who} ->
      "workflow id " <> wid <> " is already in use, by " <> maybe "a workflow" ("workflow " <>) who <> " (" <> current <> ")"
    InvalidInput {field = fieldName, detail = why} ->
      "invalid " <> fieldName <> ": " <> why
    QueueDeduplicated {workflowId = wid, queueName = queue, deduplicationId = key} ->
      "workflow " <> wid <> " (queue: " <> queue <> ", deduplication id: " <> key <> ") is already enqueued"
    WorkflowCancelled {workflowId = wid} ->
      "workflow " <> wid <> " is cancelled"
    UnexpectedStep {workflowId = wid, stepId = position, expected = want, recorded = got} ->
      "workflow " <> wid <> " step " <> Text.pack (show position) <> " was recorded as \"" <> got <> "\", but \"" <> want <> "\" was expected"
    StepNondeterminism {workflowId = wid, stepId = position} ->
      "step " <> Text.pack (show position) <> " of workflow " <> wid <> " was recorded twice by the same execution with different results; check that the workflow is deterministic"
    WorkflowConflict {workflowId = wid} ->
      "workflow " <> wid <> " is no longer owned by this execution"
    NoForkPoint {workflowIds = ids, stepName = Nothing} ->
      "no steps in workflows " <> Text.intercalate ", " ids
    NoForkPoint {workflowIds = ids, stepName = Just name} ->
      "no step named " <> name <> " in workflows " <> Text.intercalate ", " ids
    NonExistentWorkflow {workflowIds = ids} ->
      "no such workflow: " <> Text.intercalate ", " ids
    WorkflowNotRewindable {workflowId = wid, status = current} ->
      "cannot rewind workflow " <> wid <> " (" <> current <> "): it is running or waiting to run, so cancel it first"
    RewindInterrupted {workflowId = wid, status = before} ->
      "workflow " <> wid <> " changed status from " <> before <> " while being rewound; retry the rewind"
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

-- | The portable error record other SDKs read: @{name, message, data}@. The
-- sysdb layer writes it for a recorded caller outcome, which has no @Error e@
-- channel to reach the engine codec.
encodePortableError :: Error -> Text
encodePortableError err =
  decodeUtf8 . LBS.toStrict . encode $
    object
      [ "name" .= portableErrorName err,
        "message" .= renderError err,
        "data" .= toJSON err
      ]

portableErrorName :: Error -> Text
portableErrorName err = case toJSON err of
  Object fields -> case KeyMap.toList fields of
    [(tag, _)] -> Key.toText tag
    _ -> "Error"
  _ -> "Error"

-- | Read the portable record back. A foreign record without a payload this
-- build can decode keeps its @name: message@ text; a bare tagged envelope
-- (a legacy row) still decodes as itself.
decodePortableError :: Text -> Either Text Error
decodePortableError text =
  case eitherDecodeStrict' (encodeUtf8 text) of
    Left message -> Left (Text.pack message)
    Right (Object fields)
      | Just name <- KeyMap.lookup "name" fields,
        Just message <- KeyMap.lookup "message" fields ->
          case KeyMap.lookup "data" fields of
            Just structured -> case fromJSON structured of
              Success err -> Right err
              Error _ -> Left (textOf name <> ": " <> textOf message)
            Nothing -> Left (textOf name <> ": " <> textOf message)
    Right value -> case fromJSON value of
      Success err -> Right err
      Error message -> Left (Text.pack message)
  where
    textOf (String value) = value
    textOf value = decodeUtf8 (LBS.toStrict (encode value))

-- | Whether a failure is an answer worth recording against a caller's
-- step: re-asking in the same state gets the same reply, so a replay
-- stays on the branch the original run took. Control failures —
-- transport, values this build cannot read, signals about the calling
-- execution — end the run rather than answer the call, and are never
-- recorded. Exhaustive by construction (no wildcard arm), so a new
-- variant must be placed on one side or the other. Mirrors Rust
-- @Error::should_record@.
shouldRecord :: Error -> Bool
shouldRecord err = case err of
  Backend _ -> False
  Malformed _ -> False
  UnknownSerialization {} -> False
  ConflictingWorkflow {} -> True
  WorkflowIdInUse {} -> True
  InvalidInput {} -> True
  QueueDeduplicated {} -> True
  WorkflowCancelled {} -> False
  UnexpectedStep {} -> False
  StepNondeterminism {} -> False
  WorkflowConflict {} -> False
  NoForkPoint {} -> True
  NonExistentWorkflow {} -> True
  WorkflowNotRewindable {} -> True
  RewindInterrupted {} -> True
  AlreadyRegistered {} -> True
  NotRegistered {} -> True
  RegisteredByAnother {} -> True
