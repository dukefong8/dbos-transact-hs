{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Internal workflow registry. Mirrors @registry.rs@: registration turns a
-- named body into a JSON-in / JSON-out closure the executor can call knowing
-- only a row. This module owns the workflow identity ('WorkflowKey') and the
-- registration refusal vocabulary ('RegistryError'), and the dumb
-- key-to-body map between them. Bodies take the pool explicitly — there is
-- no ambient context — plus the workflow id their steps run under and the
-- stored input, if any.
--
-- The map is generic over the stored body so this module never names the
-- engine's error channel: the erased bodies live in
-- @DBOS.Transact.Workflow@, which every consumer already imports and which
-- imports none of them back. That shape is what lets the key live here
-- (ADR-0041, superseding ADR-0040's leaf): @Registry@ imports only
-- 'Prelude', Aeson, and containers, so the central error module can import
-- the key back without closing a cycle.
module DBOS.Transact.Registry
  ( WorkflowKey (..),
    newWorkflowKey,
    methodWorkflowKey,
    instanceWorkflowKey,
    workflowKeyFromRow,
    renderWorkflowKey,
    RegistryError (..),
    renderRegistryError,
    encodeRegistryErrorPayload,
    decodeRegistryErrorPayload,
    Registry,
    Snapshot,
    newRegistry,
    bindRegistryInstance,
    registryInstanceId,
    registerErasedWorkflow,
    snapshotRegistry,
    thawRegistry,
    lookupSnapshotWorkflow,
    lookupRegistryWorkflow,
    snapshotSize,
  )
where

import DBOS.Prelude
import Data.Aeson (FromJSON (..), ToJSON (..), Value, object, withObject, (.:), (.:?), (.!=), (.=))
import Data.Aeson.Types (Parser)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text qualified as Text

-- | The identity stored in @workflow_status@: a workflow name, optionally
-- qualified by its class and configured instance. Mirrors @WorkflowKey@ in
-- Rust @registry.rs@. The database treats NULL and the empty string as the
-- same absence at this registration boundary.
data WorkflowKey = WorkflowKey
  { name :: Text,
    className :: Maybe Text,
    configName :: Maybe Text
  }
  deriving stock (Eq, Ord, Show)

-- | A free function's workflow identity.
newWorkflowKey :: Text -> WorkflowKey
newWorkflowKey workflowName = WorkflowKey workflowName Nothing Nothing

-- | A class-bound workflow's identity, with no configured instance.
methodWorkflowKey :: Text -> Text -> WorkflowKey
methodWorkflowKey workflowName className = WorkflowKey workflowName (Just className) Nothing

-- | A method workflow's configured identity.
instanceWorkflowKey :: Text -> Text -> Text -> WorkflowKey
instanceWorkflowKey workflowName className configName =
  WorkflowKey workflowName (Just className) (Just configName)

-- | Rebuild an identity from a status row. Rust normalizes empty class and
-- config names to absence so rows written by Java and Python resolve alike.
workflowKeyFromRow :: Text -> Maybe Text -> Maybe Text -> WorkflowKey
workflowKeyFromRow name className configName =
  WorkflowKey name (nonEmpty className) (nonEmpty configName)
  where
    nonEmpty = (>>= \value -> if value == "" then Nothing else Just value)

-- | The identity's stable external spelling.
renderWorkflowKey :: WorkflowKey -> Text
renderWorkflowKey (WorkflowKey name className configName) =
  case (className, configName) of
    (Just className', Just configName') -> name <> "/" <> className' <> "/" <> configName'
    (Just className', Nothing) -> name <> "/" <> className'
    _ -> name

-- | Rust's derived serde shape (@name@/@class_name@/@config_name@), so a
-- recorded error carrying a key decodes back to the key on any SDK.
-- Optional fields accept absence like Rust's derived @Deserialize@ does.
instance ToJSON WorkflowKey where
  toJSON (WorkflowKey name className configName) =
    object ["name" .= name, "class_name" .= className, "config_name" .= configName]

instance FromJSON WorkflowKey where
  parseJSON = withObject "WorkflowKey" $ \fields ->
    WorkflowKey <$> fields .: "name" <*> fields .:? "class_name" .!= Nothing <*> fields .:? "config_name" .!= Nothing

-- | A registration refusal: the identity was already taken, or registration
-- happened after the launch snapshot was taken. Owned here (both sites
-- construct it) so the registry never names the engine error channel; the
-- central envelope maps each arm onto its wire variant
-- ('fromRegistryError'), keeping tags and prose byte-identical. The second
-- registry ('Datasource') reuses the same vocabulary for its own freeze
-- refusal rather than minting a parallel one.
data RegistryError
  = AlreadyRegistered {key :: WorkflowKey}
  | AlreadyLaunched {operation :: Text}
  deriving stock (Eq, Show)

-- | The refusal's prose, in the oracle's @Display@ shape. The engine's
-- existing render path delegates here for these two arms.
renderRegistryError :: RegistryError -> Text
renderRegistryError = \case
  AlreadyRegistered key -> "a workflow is already registered as " <> renderWorkflowKey key
  AlreadyLaunched operation -> "cannot " <> operation <> " after DBOS is launched"

-- | The exception mechanism carries the human rendering, matching the
-- central envelope's instance for these variants.
instance Exception RegistryError where
  displayException = Text.unpack . renderRegistryError

-- | The refusal's payload object, without the envelope. Tags stay central:
-- the envelope owns tag spellings, the fragment owns payloads.
encodeRegistryErrorPayload :: RegistryError -> Value
encodeRegistryErrorPayload = \case
  AlreadyRegistered key -> object ["key" .= key]
  AlreadyLaunched operation -> object ["operation" .= operation]

decodeRegistryErrorPayload :: Text -> Value -> Parser RegistryError
decodeRegistryErrorPayload "AlreadyRegistered" payload = withObject "AlreadyRegistered" (\o -> AlreadyRegistered <$> o .: "key") payload
decodeRegistryErrorPayload "AlreadyLaunched" payload = withObject "AlreadyLaunched" (\o -> AlreadyLaunched <$> o .: "operation") payload
decodeRegistryErrorPayload tag _ = fail ("unknown registry error variant " <> show tag)

-- | The mutable set of registrations. Its lock protects both the map and
-- whether launch has frozen it, so insertion cannot slip past a snapshot.
-- Generic over the stored body: instantiations live with the erasure
-- boundary, never here.
newtype Registry m b = Registry (StrictMVar m (RegistryState m b))

data RegistryState m b = RegistryState (Map WorkflowKey b) Bool (Maybe Text)

-- | The immutable set held by one launched executor.
newtype Snapshot m b = Snapshot (Map WorkflowKey b)

newRegistry :: MonadMVar m => m (Registry m b)
newRegistry = Registry <$> newMVar (RegistryState Map.empty False Nothing)

-- | Binds the connection a launch installed to this registry. A reference
-- minted by this registry belongs to that connection, which is what a
-- child start compares against the running context to refuse
-- @WrongInstance@; before a launch the registry names no connection, so a
-- reference to it is @NotLaunched@.
bindRegistryInstance :: MonadMVar m => Registry m b -> Text -> m ()
bindRegistryInstance (Registry stateVar) instanceId =
  modifyMVar_ stateVar $ \(RegistryState workflows frozen _) ->
    pure (RegistryState workflows frozen (Just instanceId))

-- | The connection this registry was launched over, if it has been.
registryInstanceId :: MonadMVar m => Registry m b -> m (Maybe Text)
registryInstanceId (Registry stateVar) =
  readMVar stateVar >>= \(RegistryState _ _ instanceId) -> pure instanceId

-- | Add a type-erased workflow unless the full identity is already present
-- or launch has taken its snapshot.
registerErasedWorkflow :: MonadMVar m
                       => Registry m b -> WorkflowKey -> b -> m (Either RegistryError ())
registerErasedWorkflow (Registry stateVar) key workflow =
  modifyMVar stateVar $ \state@(RegistryState workflows frozen instanceId) ->
    if frozen
      then pure (state, Left (AlreadyLaunched "register_workflow"))
      else
        case Map.lookup key workflows of
          Just _ -> pure (state, Left (AlreadyRegistered key))
          Nothing -> pure (RegistryState (Map.insert key workflow workflows) False instanceId, Right ())

-- | Freeze registrations and take an immutable snapshot atomically.
snapshotRegistry :: MonadMVar m => Registry m b -> m (Snapshot m b)
snapshotRegistry (Registry stateVar) =
  modifyMVar stateVar $ \(RegistryState workflows _ instanceId) ->
    pure (RegistryState workflows True instanceId, Snapshot workflows)

-- | Reopen the registry after a failed launch or executor shutdown.
thawRegistry :: MonadMVar m => Registry m b -> m ()
thawRegistry (Registry stateVar) =
  modifyMVar_ stateVar $ \(RegistryState workflows _ instanceId) ->
    pure (RegistryState workflows False instanceId)

lookupSnapshotWorkflow :: WorkflowKey -> Snapshot m b -> Maybe b
lookupSnapshotWorkflow key (Snapshot workflows) = Map.lookup key workflows

-- | The body registered under a key, read from the live registry rather
-- than from a launch snapshot. Equivalent to the snapshot for as long as
-- an executor holds it — launch freezes registration — and it is what a
-- call site holding only a 'WorkflowRef' can reach.
lookupRegistryWorkflow :: MonadMVar m => WorkflowKey -> Registry m b -> m (Maybe b)
lookupRegistryWorkflow key registry = case registry of
  Registry mvar -> do
    RegistryState workflows _ _ <- readMVar mvar
    pure (Map.lookup key workflows)

snapshotSize :: Snapshot m b -> Int
snapshotSize (Snapshot workflows) = Map.size workflows
