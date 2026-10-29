{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Internal workflow registry. Mirrors @registry.rs@: registration turns a
-- named body into a JSON-in / JSON-out closure the executor can call knowing
-- only a row. This module owns the registration refusal vocabulary
-- ('RegistryError') and the dumb key-to-body map; the workflow identity
-- ('WorkflowKey') lives in 'DBOS.SystemDB.Types', where the oracle moved it
-- in #135 so the system-database layer names keys without depending on the
-- engine (ADR-0041 amendment). Bodies take the pool explicitly — there is
-- no ambient context — plus the workflow id their steps run under and the
-- stored input, if any.
--
-- The map is generic over the stored body so this module never names the
-- engine's error channel: the erased bodies live in
-- @DBOS.Transact.Workflow@, which every consumer already imports and which
-- imports none of them back.
module DBOS.Transact.Registry
  ( RegistryError (..),
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
import DBOS.SystemDB.Types (WorkflowKey (..), renderWorkflowKey)
import Data.Aeson (FromJSON (..), ToJSON (..), Value, object, withObject, (.:), (.:?), (.!=), (.=))
import Data.Aeson.Types (Parser)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text qualified as Text

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
