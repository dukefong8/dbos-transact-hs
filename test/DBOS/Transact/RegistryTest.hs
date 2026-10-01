{-# LANGUAGE OverloadedStrings #-}

-- | Public-behavior tests for the @registry.rs@ identity seam.
module DBOS.Transact.RegistryTest (tests) where

import DBOS.Prelude
import DBOS.Transact
  (
    EngineOnly, CodecError,
    Ctx,
    ErasedWorkflow,
    Error,
    Failure (..),
    SerializedWorkflowValue (..),
    decodeWorkflowValue,
    encodeWorkflowValue,
    instanceWorkflowKey,
    lookupSnapshotWorkflow,
    newRegistry,
    newWorkflowKey,
    nullTracer,
    refKey,
    refName,
    registerTypedWorkflow,
    registerErasedWorkflow,
    registerWorkflowRef,
    renderTransactError,
    renderWorkflowKey,
    snapshotRegistry,
    snapshotSize,
    thawRegistry,
    workflowKeyFromRow,
  )
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact.ContextTest (ctxOver)
import Data.Text (Text)
import Data.Text qualified as Text
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

-- | One backend for the whole group: contexts build real connections
-- over it, though registry checks never reach the database.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    testGroup
      "Workflow registry"
      [ testCase "a key displays as the triple the references spell" $ do
        renderWorkflowKey (newWorkflowKey "checkout") @?= "checkout",
      testCase "one identity registers once" $ do
        registry <- newRegistry
        let key = newWorkflowKey "same"
            body :: ErasedWorkflow IO
            body _ _ = pure (Right Nothing)
        first <- registerErasedWorkflow registry key body
        case first of
          Right () -> pure ()
          Left err -> fail (show err)
        second <- registerErasedWorkflow registry key body
        case second of
          Left err -> renderTransactError err @?= "a workflow is already registered as same"
          Right () -> fail "expected duplicate registration to be refused",
      testCase "a snapshot closes the registry and releasing it reopens" $ do
        registry <- newRegistry
        let key = newWorkflowKey "before"
            body :: ErasedWorkflow IO
            body _ _ = pure (Right Nothing)
        _ <- registerErasedWorkflow registry key body
        snapshot <- snapshotRegistry registry
        snapshotSize snapshot @?= 1
        refused <- registerErasedWorkflow registry (newWorkflowKey "after") body
        case refused of
          Left err -> renderTransactError err @?= "cannot register_workflow after DBOS is launched"
          Right () -> fail "expected registration after snapshot to be refused"
        assertBool "the snapshot holds the registered workflow" (maybe False (const True) (lookupSnapshotWorkflow key snapshot))
        thawRegistry registry
        reopened <- registerErasedWorkflow registry (newWorkflowKey "after") body
        case reopened of
          Right () -> pure ()
          Left err -> fail (show err),
      testCase "a name and an instance of it are different identities" $ do
        let free = newWorkflowKey "shared"
            method = instanceWorkflowKey "shared" "Checkout" "eu"
        assertBool "the triple is distinct" (free /= method)
        renderWorkflowKey method @?= "shared/Checkout/eu",
      testCase "empty class and config names resolve like nulls" $ do
        workflowKeyFromRow "checkout" (Just "") (Just "") @?= newWorkflowKey "checkout",
      testCase "an erased workflow round-trips through json" $ do
        registry <- newRegistry
        let key = newWorkflowKey "double"
            double :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            double value _ = pure (Right (value * 2))
        registered <- registerTypedWorkflow registry key double
        case registered of
          Left err -> fail (show err)
          Right () -> pure ()
        snapshot <- snapshotRegistry registry
        workflow <- maybe (fail "registered workflow missing from snapshot") pure (lookupSnapshotWorkflow key snapshot)
        backend <- getBackend
        ctx <- ctxOver backend nullTracer "wf-1"
        result <- workflow (Just (encodeWorkflowValue (21 :: Int))) ctx
        case result of
          Right (Just output) -> decodeWorkflowValue "result" (Just output) @?= Right (42 :: Int)
          other -> fail (show other),
      testCase "a malformed argument is reported rather than panicking" $ do
        registry <- newRegistry
        let key = newWorkflowKey "double"
            double :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            double value _ = pure (Right (value * 2))
        registered <- registerTypedWorkflow registry key double
        case registered of
          Left err -> fail (show err)
          Right () -> pure ()
        snapshot <- snapshotRegistry registry
        workflow <- maybe (fail "registered workflow missing from snapshot") pure (lookupSnapshotWorkflow key snapshot)
        backend <- getBackend
        ctx <- ctxOver backend nullTracer "wf-1"
        result <- workflow (Just (SerializedWorkflowValue "\"not a number\"" Nothing)) ctx
        case result of
          Left (FailureRecorded payload) -> assertBool "names the argument" ("argument" `Text.isInfixOf` payload)
          Left (FailureControl _) -> fail "expected a recorded argument failure"
          Right _ -> fail "expected malformed input to fail decoding",
      testCase "a reference holds the identity it registered under" $ do
        registry <- newRegistry
        let key = instanceWorkflowKey "checkout" "Checkout" "eu"
            body :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            body value _ = pure (Right (value * 2))
        registered <- registerWorkflowRef registry key body
        case registered of
          Left err -> fail (show err)
          Right ref -> do
            refKey ref @?= key
            refName ref @?= "checkout",
      testCase "a reference to a duplicate identity is refused" $ do
        registry <- newRegistry
        let key = newWorkflowKey "same"
            body :: Int -> Ctx IO -> IO (Either (Error EngineOnly) Int)
            body value _ = pure (Right value)
        first <- registerWorkflowRef registry key body
        case first of
          Left err -> fail (show err)
          Right _ -> pure ()
        second <- registerWorkflowRef registry key body
        case second of
          Left err -> renderTransactError err @?= "a workflow is already registered as same"
          Right _ -> fail "expected duplicate registration to be refused",
      testCase "a zero argument workflow is called with no input at all" $ do
        registry <- newRegistry
        let key = newWorkflowKey "nothing"
            nothing :: () -> Ctx IO -> IO (Either (Error EngineOnly) Text)
            nothing () _ = pure (Right "nothing")
        registered <- registerTypedWorkflow registry key nothing
        case registered of
          Left err -> fail (show err)
          Right () -> pure ()
        snapshot <- snapshotRegistry registry
        workflow <- maybe (fail "registered workflow missing from snapshot") pure (lookupSnapshotWorkflow key snapshot)
        backend <- getBackend
        ctx <- ctxOver backend nullTracer "wf-1"
        result <- workflow Nothing ctx
        case result of
          Right (Just output) -> do
            let decoded = decodeWorkflowValue "result" (Just output) :: Either CodecError Text
            decoded @?= Right "nothing"
          other -> fail (show other)
    ]
