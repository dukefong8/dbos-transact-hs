{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Shared handle scenarios: one body per case, judged by one pure check on
-- each stack, over the shared 'HandleFixture'. The live tree
-- ('DBOS.Transact.HandleTest') runs them over Postgres with real launches;
-- the sim tree ('DBOS.Transact.HandleTestSim') over the in-memory backend
-- with the same engine calls. Engine errors throw (via 'MonadThrow'), so
-- both trees assert on plain values.
module DBOS.Transact.HandleCases
  ( HandleFixture (..),
    mkHandleFixture,
    scenarioRetrieveStatus,
    scenarioResultAdopts,
    scenarioFailError,
    scenarioDeletedAbsent,
    scenarioDropHandle,
    scenarioScopedAwait,
    checkRetrieveStatus,
    checkResultAdopts,
    checkFailError,
    checkDeletedAbsent,
    checkDropHandle,
    checkScopedAwait,
  )
where

import DBOS.Prelude
import Data.Text (Text)
import Data.Word (Word32, Word64)
import DBOS.SystemDB (NewWorkflow (..), SerializedWorkflowValue (..), Submission (..), WorkflowId (..), WorkflowStatus (..), getResultStepName, initWorkflow, listSteps, newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.Transact
  ( CodecError,
    Config (..),
    DBOS,
    EngineOnly,
    Error (..),
    Executor,
    Identity (..),
    Serializer (..),
    SomeTracer (..),
    WorkflowCtx,
    WorkflowHandle (..),
    WorkflowKey,
    awaitChild,
    decodeWorkflowValue,
    deleteWorkflows,
    encodeWorkflowValue,
    handleResult,
    handleStatus,
    launchOn,
    newDBOS,
    newWorkflowKey,
    registerDBOSWorkflow,
    retrieveWorkflow,
    runDBOSWorkflow,
    runStep,
    secondsDuration,
    shutdown,
    withWorkflow,
  )
import DBOS.Transact.Connection
  ( Connection,
    Owner (..),
    SomeSystemDB (..),
    newConnection,
    nextExecutionIdentity,
    runSystemDB,
  )

-- | How a tree instantiation builds its world: a fresh unlaunched instance,
-- the stack-specific launch, fresh workflow ids, a connection and identity
-- for parent-side scopes, and backend reads for seeding and step inspection.
-- Live fills the rest with Postgres and per-test UUIDs; the sim tree with
-- 'MemSystemDB' and deterministic ids.
data HandleFixture m = HandleFixture
  { hfNewDBOS :: m (DBOS m),
    hfLaunch :: DBOS m -> m (Executor m),
    hfFreshId :: Text -> m WorkflowId,
    hfConn :: m (Connection m),
    hfIdentity :: Identity,
    hfInitRow :: WorkflowId -> m (),
    hfListStepNames :: WorkflowId -> m [Text]
  }

-- | One fixture builder over any backend: the tree passes its 'Config',
-- 'Identity', connection app name, id naming, and id/entropy generators,
-- plus its 'SomeSystemDB' and 'SomeTracer'. Live passes Postgres +
-- FastLogger; sim passes 'MemSystemDB' + the sim carrier.
mkHandleFixture ::
  forall m.
  (MonadMVar m, MonadSTM m, MonadThrow m) =>
  Config ->
  Identity ->
  Text ->
  (Text -> WorkflowId) ->
  m Text ->
  m Word32 ->
  SomeSystemDB m ->
  SomeTracer m ->
  m (HandleFixture m)
mkHandleFixture config identity connApp nameScheme genId genEntropy sysdb tracer = do
  conn <- mkConn
  pure
    HandleFixture
      { hfNewDBOS = newDBOS config,
        hfLaunch = \dbos -> launchOn dbos conn identity,
        hfFreshId = pure . nameScheme,
        hfConn = pure conn,
        hfIdentity = identity,
        hfInitRow = \wid ->
          let WorkflowId widText = wid
              row = (newWorkflow widText) {newWorkflowName = Just "L2HandleAwaiter"}
           in do
                created <- runSystemDB sysdb (\db -> initWorkflow db row Nothing Fresh Nothing)
                case created of
                  Left err -> throwIO (userError (show err))
                  Right _ -> pure (),
        hfListStepNames = \wid -> do
          listed <- runSystemDB sysdb (\db -> listSteps db wid True Nothing Nothing Nothing)
          case listed of
            Left err -> throwIO (userError (show err))
            Right rows -> pure (map (.stepRecordStepName) rows)
      }
  where
    mkConn = do
      instanceId <- genId
      newConnection
        sysdb
        RustSerde
        (Just connApp)
        (secondsDuration 1)
        OwnerApplication
        instanceId
        genId
        genEntropy
        tracer

-- | Register the doubling body both retrieval cases run.
registerDouble :: forall m. (MonadMVar m, MonadSTM m, MonadCatch m, MonadTime m) => DBOS m -> m ()
registerDouble dbos = do
  let key = newWorkflowKey "double"
      body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
      body value wctx = runStep wctx "double" (const (pure (value * 2)))
  registered <- registerDBOSWorkflow dbos key body
  case registered of
    Left err -> throwIO (userError (show err))
    Right () -> pure ()

-- | Engine-only driver aliases: every scenario reads through these, so the
-- error channel pins to 'EngineOnly' once instead of at each call site.
-- Local copies are deliberate: this module carries only the aliases it uses.
runWf :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) => Executor m -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWf = runDBOSWorkflow

retrieveWf :: forall m. (MonadMVar m) => DBOS m -> WorkflowId -> m (Either (Error EngineOnly) (WorkflowHandle m EngineOnly))
retrieveWf = retrieveWorkflow

resultWf :: forall m. (MonadDelay m, MonadTime m, MonadMVar m, MonadThrow m) => WorkflowHandle m EngineOnly -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWf = handleResult

statusWf :: forall m. (MonadMVar m) => WorkflowHandle m EngineOnly -> m (Either (Error EngineOnly) (Maybe WorkflowStatus))
statusWf = handleStatus

-- | A retrieved handle names its workflow and reads its status. Returns the
-- requested id, the handle's id, and the status it reports.
scenarioRetrieveStatus ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadSTM m, MonadTimer m, MonadTime m, MonadDelay m) =>
  HandleFixture m ->
  m (Text, Text, Maybe WorkflowStatus)
scenarioRetrieveStatus fx = do
  bracket fx.hfNewDBOS shutdown $ \dbos -> do
    registerDouble dbos
    exec <- fx.hfLaunch dbos
    wid <- fx.hfFreshId "handle-id"
    let WorkflowId widText = wid
        key = newWorkflowKey "double"
    ran <- runWf exec key wid (Just (encodeWorkflowValue (21 :: Int)))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    retrieved <- retrieveWf dbos wid
    case retrieved of
      Left err -> throwIO (userError (show err))
      Right handle -> do
        status <- statusWf handle
        case status of
          Left err -> throwIO (userError (show err))
          Right mStatus -> pure (widText, handle.workflowId, mStatus)

-- | A handle result adopts the recorded output. Returns the decoded value.
scenarioResultAdopts ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadSTM m, MonadTimer m, MonadTime m, MonadDelay m) =>
  HandleFixture m ->
  m Int
scenarioResultAdopts fx = do
  bracket fx.hfNewDBOS shutdown $ \dbos -> do
    registerDouble dbos
    exec <- fx.hfLaunch dbos
    wid <- fx.hfFreshId "handle-res-id"
    let key = newWorkflowKey "double"
    ran <- runWf exec key wid (Just (encodeWorkflowValue (21 :: Int)))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    retrieved <- retrieveWf dbos wid
    case retrieved of
      Left err -> throwIO (userError (show err))
      Right handle -> do
        result <- resultWf handle
        case result of
          Right (Just stored) ->
            case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
              Right n -> pure n
              Left err -> throwIO (userError (show err))
          other -> throwIO (userError ("expected the recorded output, got: " <> show other))

-- | A handle result reports the error a failed run recorded. Returns the
-- failure's step and message fields.
scenarioFailError ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadSTM m, MonadTimer m, MonadTime m, MonadDelay m) =>
  HandleFixture m ->
  m (Text, Text)
scenarioFailError fx = do
  bracket fx.hfNewDBOS shutdown $ \dbos -> do
    let key = newWorkflowKey "fails"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body _ _ = pure (Left (StepFailed "body" "boom"))
    registered <- registerDBOSWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.hfLaunch dbos
    wid <- fx.hfFreshId "handle-fail-id"
    ran <- runWf exec key wid (Just (encodeWorkflowValue (1 :: Int)))
    case ran of
      Left _ -> pure ()
      Right other -> throwIO (userError ("expected the run to fail, got: " <> show other))
    retrieved <- retrieveWf dbos wid
    case retrieved of
      Left err -> throwIO (userError (show err))
      Right handle -> do
        result <- resultWf handle
        case result of
          Left (StepFailed {step, message}) -> pure (step, message)
          other -> throwIO (userError ("expected the recorded failure, got: " <> show other))

-- | A handle over a deleted row reports its absence. Returns the deleted
-- count and the status the fresh handle reads.
scenarioDeletedAbsent ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadSTM m, MonadTimer m, MonadTime m, MonadDelay m) =>
  HandleFixture m ->
  m (Word64, Maybe WorkflowStatus)
scenarioDeletedAbsent fx = do
  bracket fx.hfNewDBOS shutdown $ \dbos -> do
    let key = newWorkflowKey "delete-me"
        body :: forall exec. Int -> WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
        body value _ = pure (Right (value + 1))
    registered <- registerDBOSWorkflow dbos key body
    case registered of
      Left err -> throwIO (userError (show err))
      Right () -> pure ()
    exec <- fx.hfLaunch dbos
    wid <- fx.hfFreshId "handle-del-id"
    ran <- runWf exec key wid (Just (encodeWorkflowValue (1 :: Int)))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    deleted <- deleteWorkflows dbos [wid] True
    count <- case deleted of
      Left err -> throwIO (userError (show err))
      Right n -> pure n
    retrieved <- retrieveWf dbos wid
    case retrieved of
      Left err -> throwIO (userError (show err))
      Right handle -> do
        status <- statusWf handle
        case status of
          Left err -> throwIO (userError (show err))
          Right mStatus -> pure (count, mStatus)

-- | Dropping a handle does not stop the workflow: retrieved and immediately
-- dropped while the run is in flight, a fresh handle still reads the
-- completed result. Returns the decoded value.
scenarioDropHandle ::
  forall m.
  (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadSTM m, MonadTimer m, MonadTime m, MonadDelay m) =>
  HandleFixture m ->
  m Int
scenarioDropHandle fx = do
  bracket fx.hfNewDBOS shutdown $ \dbos -> do
    registerDouble dbos
    exec <- fx.hfLaunch dbos
    wid <- fx.hfFreshId "handle-drop-id"
    let key = newWorkflowKey "double"
    worker <- async (runWf exec key wid (Just (encodeWorkflowValue (21 :: Int))))
    -- Retrieved and immediately dropped while the run is in flight.
    _ <- retrieveWf dbos wid
    outcome <- wait worker
    case outcome of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    retrieved <- retrieveWf dbos wid
    case retrieved of
      Left err -> throwIO (userError (show err))
      Right handle -> do
        result <- resultWf handle
        case result of
          Right (Just stored) ->
            case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
              Right n -> pure n
              Left err -> throwIO (userError (show err))
          other -> throwIO (userError ("expected the completed result, got: " <> show other))

-- | A scoped await records the child's result under the parent. Returns the
-- decoded value and the parent's recorded step names.
scenarioScopedAwait ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadSTM m, MonadTimer m, MonadTime m, MonadDelay m) =>
  HandleFixture m ->
  m (Int, [Text])
scenarioScopedAwait fx = do
  parentWid <- fx.hfFreshId "handle-await-parent"
  childWid <- fx.hfFreshId "handle-await-child"
  fx.hfInitRow parentWid
  bracket fx.hfNewDBOS shutdown $ \dbos -> do
    registerDouble dbos
    exec <- fx.hfLaunch dbos
    let key = newWorkflowKey "double"
    ran <- runWf exec key childWid (Just (encodeWorkflowValue (21 :: Int)))
    case ran of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    retrieved <- retrieveWf dbos childWid
    case retrieved of
      Left err -> throwIO (userError (show err))
      Right handle -> do
        conn <- fx.hfConn
        awaited <- withWorkflow conn fx.hfIdentity parentWid Nothing $ \wctx ->
          awaitChild wctx handle
        decoded <- case awaited of
          Right (Just stored) ->
            case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
              Right n -> pure n
              Left err -> throwIO (userError (show err))
          other -> throwIO (userError ("expected the awaited output, got: " <> show other))
        names <- fx.hfListStepNames parentWid
        pure (decoded, names)

-- | The handle names the requested workflow and the row reads SUCCESS.
checkRetrieveStatus :: (Text, Text, Maybe WorkflowStatus) -> Either String ()
checkRetrieveStatus (requested, named, status) =
  checkEq (requested, Just Success) (named, status)

-- | The handle adopts the recorded output.
checkResultAdopts :: Int -> Either String ()
checkResultAdopts = checkEq 42

-- | The failure comes back as itself, fields and all.
checkFailError :: (Text, Text) -> Either String ()
checkFailError = checkEq ("body", "boom")

-- | One row deleted; the fresh handle reads no row.
checkDeletedAbsent :: (Word64, Maybe WorkflowStatus) -> Either String ()
checkDeletedAbsent = checkEq (1, Nothing)

-- | A fresh handle reads the completed result.
checkDropHandle :: Int -> Either String ()
checkDropHandle = checkEq 42

-- | The scoped await adopts the recorded output and records one
-- @DBOS.getResult@ step under the parent.
checkScopedAwait :: (Int, [Text]) -> Either String ()
checkScopedAwait = checkEq (42, [getResultStepName])

-- | Pure verdicts; both trees judge through these.
checkEq :: (Eq a, Show a) => a -> a -> Either String ()
checkEq expected actual
  | expected == actual = Right ()
  | otherwise = Left ("expected " <> show expected <> ", got " <> show actual)
