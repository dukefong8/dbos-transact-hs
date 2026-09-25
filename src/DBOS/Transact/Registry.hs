{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Internal workflow registry (Rule 4: plain Haskell, no Bluefin imports).
-- Mirrors @registry.rs@: registration turns a named body into a JSON-in /
-- JSON-out closure the executor can call knowing only a row. Bodies take the
-- pool explicitly — there is no ambient context — plus the workflow id their
-- steps run under and the stored input, if any.
module DBOS.Transact.Registry
  ( WorkflowKey (..),
    newWorkflowKey,
    instanceWorkflowKey,
    workflowKeyFromRow,
    renderWorkflowKey,
    WorkflowRef (..),
    refKey,
    refName,
    registerWorkflowRef,
    ErasedWorkflow,
    Registry,
    Snapshot,
    newRegistry,
    registerTypedWorkflow,
    registerErasedWorkflow,
    snapshotRegistry,
    thawRegistry,
    lookupSnapshotWorkflow,
    snapshotSize,
    DuplicateWorkflowName (..),
    WorkflowBody,
    WorkflowRegistry,
    emptyRegistry,
    lookupWorkflow,
    registerWorkflow,
  )
where

import DBOS.Prelude
import Control.Concurrent.Class.MonadMVar (MonadMVar)
import Control.Concurrent.Class.MonadMVar.Strict (StrictMVar, modifyMVar, modifyMVar_, newMVar)
import Data.Aeson (FromJSON, ToJSON)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.SystemDB.Postgres (Pool)
import DBOS.SystemDB.Types (Serialization (..), SerializedWorkflowValue, WorkflowId, WorkflowName (..))
import DBOS.Transact.Codec (CodecError (..), decodeWorkflowValue, encodeWorkflowValue)
import DBOS.Transact.Context (Ctx)
import DBOS.Transact.Error qualified as TransactError

-- | The identity stored in @workflow_status@: a workflow name, optionally
-- qualified by its class and configured instance. Mirrors @WorkflowKey@ in
-- Rust @registry.rs@. The database treats NULL and the empty string as the
-- same absence at this registration boundary.
data WorkflowKey = WorkflowKey
  { name :: Text,
    class_name :: Maybe Text,
    config_name :: Maybe Text
  }
  deriving stock (Eq, Ord, Show)

-- | A free function's workflow identity.
newWorkflowKey :: Text -> WorkflowKey
newWorkflowKey workflowName = WorkflowKey workflowName Nothing Nothing

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

-- | A registered workflow, holding where it was registered and under which
-- identity. Mirrors Rust @WorkflowRef@: what registration returns and what
-- a call site holds. The registry stores only the erased body; the argument
-- and result types were checked once at registration rather than at every
-- invocation by string. The port holds no type parameters — application
-- values cross the boundary already serialized, as 'ErasedWorkflow' does —
-- and no 'DBOS' handle: drivers that need a launched executor take it
-- explicitly (see 'DBOS.Transact.Instance'), so capturing a reference never
-- pins an instance.
data WorkflowRef m = WorkflowRef
  { refRegistry :: Registry m,
    refKey :: WorkflowKey
  }

instance Show (WorkflowRef m) where
  show ref = "WorkflowRef " <> Text.unpack (renderWorkflowKey ref.refKey)

-- | The identity this workflow was registered under.
refKey :: WorkflowRef m -> WorkflowKey
refKey ref = ref.refKey

-- | The workflow's name: the bare name, not the full identity triple.
refName :: WorkflowRef m -> Text
refName ref = case ref.refKey of WorkflowKey name _ _ -> name

-- | Register a typed workflow and hand back a reference to it. Only before
-- launch snapshots the registry; a duplicate identity is refused.
registerWorkflowRef :: (FromJSON argument, ToJSON result, MonadMVar m) => Registry m -> WorkflowKey -> (argument -> Ctx m -> m (Either TransactError.Error result)) -> m (Either TransactError.Error (WorkflowRef m))
registerWorkflowRef registry key body = do
  registered <- registerTypedWorkflow registry key body
  pure (WorkflowRef registry key <$ registered)

-- | The type-erased workflow body the engine resolves from a stored key.
-- Application values have already been serialized by the registration
-- boundary; the body takes the explicit context it runs in.
type ErasedWorkflow m = Maybe SerializedWorkflowValue -> Ctx m -> m (Either TransactError.Error (Maybe SerializedWorkflowValue))

-- | Register a typed workflow and erase its JSON input and output types at
-- the registry boundary. The stored representation is the same serialized
-- value used by workflow rows and operation checkpoints.
registerTypedWorkflow :: (FromJSON argument, ToJSON result, MonadMVar m) => Registry m -> WorkflowKey -> (argument -> Ctx m -> m (Either TransactError.Error result)) -> m (Either TransactError.Error ())
registerTypedWorkflow registry key body =
  registerErasedWorkflow registry key $ \input ctx ->
    case decodeWorkflowValue "argument" input of
      Left err -> pure (Left (codecError "argument" err))
      Right argument -> do
        result <- body argument ctx
        pure (fmap (Just . encodeWorkflowValue) result)
  where
    codecError what err =
      case err of
        CodecNotJson _ input -> TransactError.ErrorDeserialization what input
        CodecTypeMismatch _ message -> TransactError.ErrorDeserialization what (Text.pack message)

-- | The mutable set of registrations. Its lock protects both the map and
-- whether launch has frozen it, so insertion cannot slip past a snapshot.
newtype Registry m = Registry (StrictMVar m (RegistryState m))

data RegistryState m = RegistryState (Map WorkflowKey (ErasedWorkflow m)) Bool

-- | The immutable set held by one launched executor.
newtype Snapshot m = Snapshot (Map WorkflowKey (ErasedWorkflow m))

newRegistry :: MonadMVar m => m (Registry m)
newRegistry = Registry <$> newMVar (RegistryState Map.empty False)

-- | Add a type-erased workflow unless the full identity is already present
-- or launch has taken its snapshot.
registerErasedWorkflow :: MonadMVar m => Registry m -> WorkflowKey -> ErasedWorkflow m -> m (Either TransactError.Error ())
registerErasedWorkflow (Registry stateVar) key workflow =
  modifyMVar stateVar $ \state@(RegistryState workflows frozen) ->
    if frozen
      then pure (state, Left (TransactError.ErrorAlreadyLaunched "register_workflow"))
      else
        case Map.lookup key workflows of
          Just _ -> pure (state, Left (TransactError.ErrorAlreadyRegistered (renderWorkflowKey key)))
          Nothing -> pure (RegistryState (Map.insert key workflow workflows) False, Right ())

-- | Freeze registrations and take an immutable snapshot atomically.
snapshotRegistry :: MonadMVar m => Registry m -> m (Snapshot m)
snapshotRegistry (Registry stateVar) =
  modifyMVar stateVar $ \(RegistryState workflows _) ->
    pure (RegistryState workflows True, Snapshot workflows)

-- | Reopen the registry after a failed launch or executor shutdown.
thawRegistry :: MonadMVar m => Registry m -> m ()
thawRegistry (Registry stateVar) =
  modifyMVar_ stateVar $ \(RegistryState workflows _) ->
    pure (RegistryState workflows False)

lookupSnapshotWorkflow :: WorkflowKey -> Snapshot m -> Maybe (ErasedWorkflow m)
lookupSnapshotWorkflow key (Snapshot workflows) = Map.lookup key workflows

snapshotSize :: Snapshot m -> Int
snapshotSize (Snapshot workflows) = Map.size workflows

type WorkflowBody =
  Pool ->
  WorkflowId ->
  Maybe SerializedWorkflowValue ->
  IO SerializedWorkflowValue

newtype WorkflowRegistry = WorkflowRegistry (Map WorkflowName WorkflowBody)

newtype DuplicateWorkflowName = DuplicateWorkflowName WorkflowName
  deriving stock (Eq, Show)

emptyRegistry :: WorkflowRegistry
emptyRegistry = WorkflowRegistry Map.empty

registerWorkflow ::
  WorkflowName ->
  WorkflowBody ->
  WorkflowRegistry ->
  Either DuplicateWorkflowName WorkflowRegistry
registerWorkflow name body (WorkflowRegistry entries) =
  case Map.lookup name entries of
    Just _ -> Left (DuplicateWorkflowName name)
    Nothing -> Right (WorkflowRegistry (Map.insert name body entries))

lookupWorkflow :: WorkflowName -> WorkflowRegistry -> Maybe WorkflowBody
lookupWorkflow name (WorkflowRegistry entries) = Map.lookup name entries
