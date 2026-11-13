{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Public-behavior scenarios for the @registry.rs@ identity seam: what a
-- key displays, what duplicate registration refuses, what a snapshot
-- closes and a thaw reopens, and what a registered body computes through
-- a workflow view — including the malformed-input control refusal. Every
-- scenario is polymorphic over the backing monad so the live tree (over
-- Postgres connections) and the sim tree (over the mock backend) drive
-- the same seam: registration, snapshot, lookup, invocation, and the
-- codec boundary all run on both stacks.
module DBOS.Transact.RegistryCases
  ( RegistryFixture (..),
    checkDuplicateRefused,
    checkDuplicateRefRefused,
    checkEmptyNamesResolve,
    checkErasedRoundTrip,
    checkInstanceDistinct,
    checkKeyTriple,
    checkMalformedReported,
    checkMethodDistinct,
    checkReferenceIdentity,
    checkSnapshotReopen,
    checkUnregistrableRefused,
    checkZeroArgCall,
    scenarioDuplicateRefused,
    scenarioDuplicateRefRefused,
    scenarioEmptyNamesResolve,
    scenarioErasedRoundTrip,
    scenarioInstanceDistinct,
    scenarioKeyTriple,
    scenarioMalformedReported,
    scenarioMethodDistinct,
    scenarioReferenceIdentity,
    scenarioSnapshotReopen,
    scenarioUnregistrableRefused,
    scenarioZeroArgCall,
  )
where

import DBOS.Internal.Prelude
import Data.Text qualified as Text
import DBOS.SystemDB (WorkflowId (..))
import DBOS.Internal.SystemDB.Types (QualifiedWorkflowName, instanceQualifiedWorkflowName, methodQualifiedWorkflowName, renderQualifiedWorkflowName, qualifiedWorkflowNameFromRow)
import DBOS.Transact (EngineOnly, Error, SerializedWorkflowValue (..), WorkflowCtx, newQualifiedWorkflowName)
import DBOS.Internal.Transact.Connection (Connection)
import DBOS.Internal.Transact.Context (testOwner, withWorkflow)
import DBOS.Internal.Transact.Error (Error (..), Failure (..))
import DBOS.Internal.Transact.Identity (Identity)
import DBOS.Internal.Transact.Registry
  ( lookupSnapshotWorkflow,
    newRegistry,
    registerErasedWorkflow,
    snapshotRegistry,
    snapshotSize,
    thawRegistry,
  )
import DBOS.Internal.Transact.Workflow
  ( ErasedWorkflow (..),
    WorkflowRef (refQualifiedName),
    refName,
    registerTypedWorkflow,
    registerWorkflowRef,
  )
import DBOS.Internal.Transact.Serialization (decodeWorkflowValue, encodeWorkflowInput, unWorkflowInput)

-- | How a tree instantiation builds bodies' world: connections over any
-- backend and the identity they carry. Registry checks never reach the
-- database, but invocation runs through a workflow view, which needs a
-- connection on either stack.
data RegistryFixture m = RegistryFixture
  { rfConn :: m (Connection m),
    rfIdentity :: Identity
  }

-- | A key displays as the triple the references spell.
scenarioKeyTriple :: forall m. Monad m => RegistryFixture m -> m Text
scenarioKeyTriple _ = pure (renderQualifiedWorkflowName (newQualifiedWorkflowName "checkout"))

checkKeyTriple :: Text -> Either String ()
checkKeyTriple rendered
  | rendered == "checkout" = Right ()
  | otherwise = Left ("expected the bare key, got: " <> Text.unpack rendered)

-- | One identity registers once; the second registration names the key it
-- refused.
scenarioDuplicateRefused :: forall m. MonadMVar m => RegistryFixture m -> m (Either String Text)
scenarioDuplicateRefused _ = do
  registry <- newRegistry
  let key = newQualifiedWorkflowName "same"
      body :: ErasedWorkflow m
      body = ErasedWorkflow (\_ _ -> pure (Right Nothing))
  first <- registerErasedWorkflow registry key body
  case first of
    Left err -> pure (Left ("expected the first registration to succeed, got: " <> displayException err))
    Right () -> do
      second <- registerErasedWorkflow registry key body
      case second of
        Left err -> pure (Right (Text.pack (displayException err)))
        Right () -> pure (Left "expected duplicate registration to be refused")

checkDuplicateRefused :: Either String Text -> Either String ()
checkDuplicateRefused result = case result of
  Right rendered
    | rendered == "a workflow is already registered as same" -> Right ()
    | otherwise -> Left ("expected the duplicate refusal, got: " <> Text.unpack rendered)
  Left problem -> Left problem

-- | A snapshot closes the registry and releasing it reopens: the size, the
-- refusal, and the held workflow, then a clean registration after the
-- thaw.
scenarioSnapshotReopen :: forall m. MonadMVar m => RegistryFixture m -> m (Either String (Int, Text, Bool))
scenarioSnapshotReopen _ = do
  registry <- newRegistry
  let key = newQualifiedWorkflowName "before"
      body :: ErasedWorkflow m
      body = ErasedWorkflow (\_ _ -> pure (Right Nothing))
  _ <- registerErasedWorkflow registry key body
  snapshot <- snapshotRegistry registry
  refused <- registerErasedWorkflow registry (newQualifiedWorkflowName "after") body
  case refused of
    Left err -> do
      thawRegistry registry
      reopened <- registerErasedWorkflow registry (newQualifiedWorkflowName "after") body
      case reopened of
        Left reopenErr -> pure (Left ("expected registration after thaw to succeed, got: " <> displayException reopenErr))
        Right () ->
          pure (Right (snapshotSize snapshot, Text.pack (displayException err), maybe False (const True) (lookupSnapshotWorkflow key snapshot)))
    Right () -> pure (Left "expected registration after snapshot to be refused")

checkSnapshotReopen :: Either String (Int, Text, Bool) -> Either String ()
checkSnapshotReopen result = case result of
  Right (size, rendered, held)
    | size /= 1 -> Left ("expected one snapshotted workflow, got: " <> show size)
    | rendered /= "cannot register_workflow after DBOS is launched" -> Left ("expected the frozen refusal, got: " <> Text.unpack rendered)
    | not held -> Left "expected the snapshot to hold the registered workflow"
    | otherwise -> Right ()
  Left problem -> Left problem

-- | A name and an instance of it are different identities.
scenarioInstanceDistinct :: forall m. Monad m => RegistryFixture m -> m (Bool, Text)
scenarioInstanceDistinct _ = do
  let free = newQualifiedWorkflowName "shared"
      method = instanceQualifiedWorkflowName "shared" "Checkout" "eu"
  pure (free /= method, renderQualifiedWorkflowName method)

checkInstanceDistinct :: (Bool, Text) -> Either String ()
checkInstanceDistinct (distinct, rendered)
  | not distinct = Left "expected the triple to differ from the bare name"
  | rendered /= "shared/Checkout/eu" = Left ("expected the qualified spelling, got: " <> Text.unpack rendered)
  | otherwise = Right ()

-- | A class-bound key with no instance is its own identity: it differs
-- from the bare name and from the configured instance, and renders the
-- pair. Mirrors @QualifiedWorkflowName::method@.
scenarioMethodDistinct :: forall m. Monad m => RegistryFixture m -> m (Bool, Bool, Text)
scenarioMethodDistinct _ = do
  let free = newQualifiedWorkflowName "shared"
      method = methodQualifiedWorkflowName "shared" "Checkout"
      configured = instanceQualifiedWorkflowName "shared" "Checkout" "eu"
  pure (free /= method, method /= configured, renderQualifiedWorkflowName method)

checkMethodDistinct :: (Bool, Bool, Text) -> Either String ()
checkMethodDistinct (fromFree, fromConfigured, rendered)
  | not fromFree = Left "expected the method key to differ from the bare name"
  | not fromConfigured = Left "expected the method key to differ from the configured instance"
  | rendered /= "shared/Checkout" = Left ("expected the pair spelling, got: " <> Text.unpack rendered)
  | otherwise = Right ()

-- | Empty class and config names resolve like nulls.
scenarioEmptyNamesResolve :: forall m. Monad m => RegistryFixture m -> m Bool
scenarioEmptyNamesResolve _ =
  pure (qualifiedWorkflowNameFromRow "checkout" (Just "") (Just "") == newQualifiedWorkflowName "checkout")

checkEmptyNamesResolve :: Bool -> Either String ()
checkEmptyNamesResolve resolved
  | resolved = Right ()
  | otherwise = Left "expected empty names to resolve like nulls"

-- | An erased workflow round-trips through json: registered typed,
-- invoked through a workflow view with an encoded argument, decoded back.
scenarioErasedRoundTrip :: forall m. (MonadMVar m, MonadSTM m) => RegistryFixture m -> m (Either String Int)
scenarioErasedRoundTrip fixture = do
  registry <- newRegistry
  let key = newQualifiedWorkflowName "double"
      double :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      double value _ = pure (Right (value * 2))
  registered <- registerTypedWorkflow registry key double
  case registered of
    Left err -> pure (Left ("expected registration to succeed, got: " <> displayException err))
    Right () -> do
      snapshot <- snapshotRegistry registry
      case lookupSnapshotWorkflow key snapshot of
        Nothing -> pure (Left "expected the registered workflow in the snapshot")
        Just (ErasedWorkflow body) -> do
          conn <- fixture.rfConn
          outcome <-
            withWorkflow conn fixture.rfIdentity (WorkflowId "wf-1") Nothing (testOwner (WorkflowId "wf-1")) $ \wctx ->
              body (Just (unWorkflowInput (encodeWorkflowInput (21 :: Int)))) wctx
          case outcome of
            Right (Just output) -> case decodeWorkflowValue "result" (Just output) of
              Right doubled -> pure (Right doubled)
              Left err -> pure (Left ("expected a decodable output, got: " <> show err))
            Right Nothing -> pure (Left "expected an output, got none")
            Left failure -> pure (Left ("expected the body to succeed, got: " <> show failure))

checkErasedRoundTrip :: Either String Int -> Either String ()
checkErasedRoundTrip result = case result of
  Right 42 -> Right ()
  Right doubled -> Left ("expected the doubled input, got: " <> show doubled)
  Left problem -> Left problem

-- | A malformed argument is reported rather than panicking: the input
-- decode failure is control, not recorded.
scenarioMalformedReported :: forall m. (MonadMVar m, MonadSTM m) => RegistryFixture m -> m (Either String Text)
scenarioMalformedReported fixture = do
  registry <- newRegistry
  let key = newQualifiedWorkflowName "double"
      double :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      double value _ = pure (Right (value * 2))
  registered <- registerTypedWorkflow registry key double
  case registered of
    Left err -> pure (Left ("expected registration to succeed, got: " <> displayException err))
    Right () -> do
      snapshot <- snapshotRegistry registry
      case lookupSnapshotWorkflow key snapshot of
        Nothing -> pure (Left "expected the registered workflow in the snapshot")
        Just (ErasedWorkflow body) -> do
          conn <- fixture.rfConn
          outcome <-
            withWorkflow conn fixture.rfIdentity (WorkflowId "wf-1") Nothing (testOwner (WorkflowId "wf-1")) $ \wctx ->
              body (Just (SerializedWorkflowValue "\"not a number\"" Nothing)) wctx
          case outcome of
            Left (FailureControl (Deserialization {what})) -> pure (Right what)
            Left (FailureControl _) -> pure (Left "expected the argument decode failure")
            Left (FailureRecorded _) -> pure (Left "expected an input decode failure to be control, not recorded")
            Right _ -> pure (Left "expected malformed input to fail decoding")

checkMalformedReported :: Either String Text -> Either String ()
checkMalformedReported result = case result of
  Right "argument" -> Right ()
  Right what -> Left ("expected the failure to name the argument, got: " <> Text.unpack what)
  Left problem -> Left problem

-- | A reference holds the identity it registered under.
scenarioReferenceIdentity :: forall m. MonadMVar m => RegistryFixture m -> m (Either String (Bool, Text))
scenarioReferenceIdentity _ = do
  registry <- newRegistry
  let key = instanceQualifiedWorkflowName "checkout" "Checkout" "eu"
      body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body value _ = pure (Right (value * 2))
  registered <- registerWorkflowRef registry key body
  case registered of
    Left err -> pure (Left ("expected registration to succeed, got: " <> displayException err))
    Right ref -> pure (Right (ref.refQualifiedName == key, refName ref))

checkReferenceIdentity :: Either String (Bool, Text) -> Either String ()
checkReferenceIdentity result = case result of
  Right (same, name)
    | not same -> Left "expected the reference to hold its key"
    | name /= "checkout" -> Left ("expected the bare name, got: " <> Text.unpack name)
    | otherwise -> Right ()
  Left problem -> Left problem

-- | A reference to a duplicate identity is refused, naming the key.
scenarioDuplicateRefRefused :: forall m. MonadMVar m => RegistryFixture m -> m (Either String Text)
scenarioDuplicateRefRefused _ = do
  registry <- newRegistry
  let key = newQualifiedWorkflowName "same"
      body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body value _ = pure (Right value)
  first <- registerWorkflowRef registry key body
  case first of
    Left err -> pure (Left ("expected the first registration to succeed, got: " <> displayException err))
    Right _ -> do
      second <- registerWorkflowRef registry key body
      case second of
        Left err -> pure (Right (Text.pack (displayException err)))
        Right _ -> pure (Left "expected duplicate registration to be refused")

checkDuplicateRefRefused :: Either String Text -> Either String ()
checkDuplicateRefRefused result = case result of
  Right rendered
    | rendered == "a workflow is already registered as same" -> Right ()
    | otherwise -> Left ("expected the duplicate refusal, got: " <> Text.unpack rendered)
  Left problem -> Left problem

-- | An empty name, or a config with no class, names nothing a workflow
-- could be registered as, and registers nothing. Mirrors Rust
-- @a_key_naming_nothing_is_refused@.
scenarioUnregistrableRefused :: forall m. MonadMVar m => RegistryFixture m -> m (Either String (Text, Text, Int))
scenarioUnregistrableRefused _ = do
  registry <- newRegistry
  let body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body value _ = pure (Right value)
  emptyName <- registerWorkflowRef registry (newQualifiedWorkflowName "") body
  unclassed <- registerWorkflowRef registry (instanceQualifiedWorkflowName "unclassed" "" "eu") body
  snapshot <- snapshotRegistry registry
  case (emptyName, unclassed) of
    (Left (InvalidArgument _ emptyDetail), Left (InvalidArgument _ classDetail)) ->
      pure (Right (emptyDetail, classDetail, snapshotSize snapshot))
    (Left err, _) -> pure (Left ("expected the empty name refused as an argument, got: " <> displayException err))
    (_, Left err) -> pure (Left ("expected the classless config refused as an argument, got: " <> displayException err))
    _ -> pure (Left "expected names for nothing to be refused")

checkUnregistrableRefused :: Either String (Text, Text, Int) -> Either String ()
checkUnregistrableRefused result = case result of
  Right (emptyDetail, classDetail, held)
    | not ("needs a name" `Text.isInfixOf` emptyDetail) -> Left ("expected the nameless refusal, got: " <> Text.unpack emptyDetail)
    | not ("needs a class" `Text.isInfixOf` classDetail) -> Left ("expected the classless refusal, got: " <> Text.unpack classDetail)
    | held /= 0 -> Left ("expected nothing registered, got: " <> show held)
    | otherwise -> Right ()
  Left problem -> Left problem

-- | A zero argument workflow is called with no input at all.
scenarioZeroArgCall :: forall m. (MonadMVar m, MonadSTM m) => RegistryFixture m -> m (Either String Text)
scenarioZeroArgCall fixture = do
  registry <- newRegistry
  let key = newQualifiedWorkflowName "nothing"
      nothing :: forall exec. () -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Text)
      nothing () _ = pure (Right "nothing")
  registered <- registerTypedWorkflow registry key nothing
  case registered of
    Left err -> pure (Left ("expected registration to succeed, got: " <> displayException err))
    Right () -> do
      snapshot <- snapshotRegistry registry
      case lookupSnapshotWorkflow key snapshot of
        Nothing -> pure (Left "expected the registered workflow in the snapshot")
        Just (ErasedWorkflow body) -> do
          conn <- fixture.rfConn
          outcome <-
            withWorkflow conn fixture.rfIdentity (WorkflowId "wf-1") Nothing (testOwner (WorkflowId "wf-1")) $ \wctx ->
              body Nothing wctx
          case outcome of
            Right (Just output) -> case decodeWorkflowValue "result" (Just output) of
              Right text -> pure (Right text)
              Left err -> pure (Left ("expected a decodable output, got: " <> show err))
            Right Nothing -> pure (Left "expected an output, got none")
            Left failure -> pure (Left ("expected the body to succeed, got: " <> show failure))

checkZeroArgCall :: Either String Text -> Either String ()
checkZeroArgCall result = case result of
  Right "nothing" -> Right ()
  Right text -> Left ("expected the unit body to answer, got: " <> Text.unpack text)
  Left problem -> Left problem
