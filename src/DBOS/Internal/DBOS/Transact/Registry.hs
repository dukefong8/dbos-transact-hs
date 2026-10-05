{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE ScopedTypeVariables #-}
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
    refName,
    registerWorkflowRef,
    ErasedWorkflow (..),
    Registry,
    Snapshot,
    newRegistry,
    bindRegistryInstance,
    registryInstanceId,
    registerTypedWorkflow,
    registerErasedWorkflow,
    snapshotRegistry,
    thawRegistry,
    lookupSnapshotWorkflow,
    lookupRegistryWorkflow,
    snapshotSize,
  )
where

import DBOS.Prelude
import Data.Aeson (FromJSON, ToJSON)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import DBOS.SystemDB.Types (Serialization (..), SerializedWorkflowValue, WorkflowId, WorkflowName (..))
import DBOS.Transact.Serialization (CodecError (..), decodeWorkflowValue, encodeWorkflowValue)
import DBOS.Transact.Context (WorkflowCtx)
import DBOS.Transact.Error qualified as TransactError

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
-- explicitly (see 'DBOS.Transact.Instance'). A launch does bind the
-- reference's registry to its connection ('bindRegistryInstance'), which
-- is what 'DBOS.Transact.Workflow.startChildWorkflow' compares against the
-- running context to refuse a child started through another instance
-- (ADR-0018, reversing the earlier "never pins an instance" note for
-- exactly that refusal).
data WorkflowRef m e = WorkflowRef
  { refRegistry :: Registry m,
    refKey :: WorkflowKey
  }

instance Show (WorkflowRef m e) where
  show ref = "WorkflowRef " <> Text.unpack (renderWorkflowKey ref.refKey)

-- | The workflow's name: the bare name, not the full identity triple.
refName :: WorkflowRef m e -> Text
refName ref = case ref.refKey of WorkflowKey name _ _ -> name

-- | Register a typed workflow and hand back a reference to it. Only before
-- launch snapshots the registry; a duplicate identity is refused.
--
-- Bodies take the scoped workflow view: the converted shape, where a body
-- can only reach the scoped entries. The old context-level entry was
-- deleted once every caller converted (C5b).
registerWorkflowRef :: (FromJSON argument, ToJSON result, ToJSON e, MonadMVar m) => Registry m -> WorkflowKey -> (forall exec. argument -> WorkflowCtx exec m -> m (Either (TransactError.Error e) result)) -> m (Either (TransactError.Error TransactError.EngineOnly) (WorkflowRef m e))
registerWorkflowRef registry key body = do
  registered <- registerTypedWorkflow registry key body
  pure ((\() -> WorkflowRef registry key) <$> registered)

-- | The type-erased workflow body the engine resolves from a stored key.
-- Application values and the application error channel have already been
-- serialized by the registration boundary; the body takes the explicit
-- context it runs in. Mirrors the oracle's @ErasedWorkflow@, whose failure
-- channel is @Failure@ for the same reason: recovery and dequeue resolve
-- bodies by name and have no application error type to name.
newtype ErasedWorkflow m = ErasedWorkflow
  { -- | The stored body, run at whatever execution the caller's scope
    -- names: the rank-2 field is why converted and unconverted bodies can
    -- share one registry.
    runErasedWorkflow :: forall exec. Maybe SerializedWorkflowValue -> WorkflowCtx exec m -> m (Either TransactError.Failure (Maybe SerializedWorkflowValue))
  }

-- | Register a typed workflow and erase its JSON input and output types at
-- the registry boundary. The stored representation is the same serialized
-- value used by workflow rows and operation checkpoints.
-- | Register a typed workflow whose body takes the scoped workflow view:
-- the only shape left, now that the engine has no context-level entries —
-- every durable call the body makes takes the view it was handed or one
-- derived from it. The type-erased form is the same either way, so the
-- registry seam is unchanged.
registerTypedWorkflow :: forall argument result e m. (FromJSON argument, ToJSON result, ToJSON e, MonadMVar m) => Registry m -> WorkflowKey -> (forall exec. argument -> WorkflowCtx exec m -> m (Either (TransactError.Error e) result)) -> m (Either (TransactError.Error TransactError.EngineOnly) ())
registerTypedWorkflow registry key body =
  registerErasedWorkflow registry key $ ErasedWorkflow $ \input wctx ->
    case decodeWorkflowValue "argument" input of
      Left err -> pure (Left (TransactError.failureOf (codecError "argument" err)))
      Right argument -> do
        result <- body argument wctx
        pure $ case result of
          Left err -> Left (TransactError.failureOf err)
          Right value -> Right (Just (encodeWorkflowValue value))
  where
    codecError :: Text -> CodecError -> TransactError.Error e
    codecError what err =
      case err of
        CodecNotJson _ input -> TransactError.ErrorDeserialization what input
        CodecTypeMismatch _ message -> TransactError.ErrorDeserialization what (Text.pack message)

-- | The mutable set of registrations. Its lock protects both the map and
-- whether launch has frozen it, so insertion cannot slip past a snapshot.
newtype Registry m = Registry (StrictMVar m (RegistryState m))

data RegistryState m = RegistryState (Map WorkflowKey (ErasedWorkflow m)) Bool (Maybe Text)

-- | The immutable set held by one launched executor.
newtype Snapshot m = Snapshot (Map WorkflowKey (ErasedWorkflow m))

newRegistry :: MonadMVar m => m (Registry m)
newRegistry = Registry <$> newMVar (RegistryState Map.empty False Nothing)

-- | Binds the connection a launch installed to this registry. A reference
-- minted by this registry belongs to that connection, which is what a
-- child start compares against the running context to refuse
-- @WrongInstance@; before a launch the registry names no connection, so a
-- reference to it is @NotLaunched@.
bindRegistryInstance :: MonadMVar m => Registry m -> Text -> m ()
bindRegistryInstance (Registry stateVar) instanceId =
  modifyMVar_ stateVar $ \(RegistryState workflows frozen _) ->
    pure (RegistryState workflows frozen (Just instanceId))

-- | The connection this registry was launched over, if it has been.
registryInstanceId :: MonadMVar m => Registry m -> m (Maybe Text)
registryInstanceId (Registry stateVar) =
  readMVar stateVar >>= \(RegistryState _ _ instanceId) -> pure instanceId

-- | Add a type-erased workflow unless the full identity is already present
-- or launch has taken its snapshot.
registerErasedWorkflow :: MonadMVar m => Registry m -> WorkflowKey -> ErasedWorkflow m -> m (Either (TransactError.Error TransactError.EngineOnly) ())
registerErasedWorkflow (Registry stateVar) key workflow =
  modifyMVar stateVar $ \state@(RegistryState workflows frozen instanceId) ->
    if frozen
      then pure (state, Left (TransactError.ErrorAlreadyLaunched "register_workflow"))
      else
        case Map.lookup key workflows of
          Just _ -> pure (state, Left (TransactError.ErrorAlreadyRegistered (renderWorkflowKey key)))
          Nothing -> pure (RegistryState (Map.insert key workflow workflows) False instanceId, Right ())

-- | Freeze registrations and take an immutable snapshot atomically.
snapshotRegistry :: MonadMVar m => Registry m -> m (Snapshot m)
snapshotRegistry (Registry stateVar) =
  modifyMVar stateVar $ \(RegistryState workflows _ instanceId) ->
    pure (RegistryState workflows True instanceId, Snapshot workflows)

-- | Reopen the registry after a failed launch or executor shutdown.
thawRegistry :: MonadMVar m => Registry m -> m ()
thawRegistry (Registry stateVar) =
  modifyMVar_ stateVar $ \(RegistryState workflows _ instanceId) ->
    pure (RegistryState workflows False instanceId)

lookupSnapshotWorkflow :: WorkflowKey -> Snapshot m -> Maybe (ErasedWorkflow m)
lookupSnapshotWorkflow key (Snapshot workflows) = Map.lookup key workflows

-- | The body registered under a key, read from the live registry rather
-- than from a launch snapshot. Equivalent to the snapshot for as long as
-- an executor holds it — launch freezes registration — and it is what a
-- call site holding only a 'WorkflowRef' can reach.
lookupRegistryWorkflow :: MonadMVar m => WorkflowKey -> Registry m -> m (Maybe (ErasedWorkflow m))
lookupRegistryWorkflow key registry = case registry of
  Registry mvar -> do
    RegistryState workflows _ _ <- readMVar mvar
    pure (Map.lookup key workflows)

snapshotSize :: Snapshot m -> Int
snapshotSize (Snapshot workflows) = Map.size workflows
