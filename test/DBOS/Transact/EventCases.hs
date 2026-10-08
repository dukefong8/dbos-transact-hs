{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Shared event scenarios: one body per case, judged by one pure check on
-- each stack, over the shared 'EventFixture'. The live tree
-- ('DBOS.Transact.EventTest') runs them over Postgres with real launches;
-- the sim tree ('DBOS.Transact.EventTestSim') over the in-memory backend
-- with the same engine calls — including the crash-and-relaunch recovery,
-- which replays the recorded publish instead of republishing it. Engine
-- errors throw (via 'MonadThrow'), so both trees assert on plain values.
--
-- Bodies that capture per-case state (MVars, a second instance) live as
-- top-level helpers taking that state explicitly: @MonoLocalBinds@ cannot
-- generalize a @let@-bound rank-2 body.
module DBOS.Transact.EventCases
  ( EventFixture (..),
    mkEventFixture,
    progressBody,
    readerBody,
    inStepReaderBody,
    outOfOrderBody,
    scenarioPublishReplay,
    scenarioCheckpointedRead,
    scenarioRefusedSet,
    scenarioCapturedRead,
    scenarioReplayNoRepublish,
    scenarioRecoveryKeepsFirst,
    scenarioWrongInstance,
    scenarioOutOfOrderIds,
    checkPublishReplay,
    checkCheckpointedRead,
    checkRefusedSet,
    checkCapturedRead,
    checkReplayNoRepublish,
    checkRecoveryKeepsFirst,
    checkWrongInstance,
    checkOutOfOrderIds,
    waitForEvent,
  )
where

import DBOS.Prelude
import DBOS.SystemDB (NewWorkflow (..), QueueName (..), SerializedWorkflowValue (..), Submission (..), WorkflowId (..), getEventStepName, initWorkflow, internalQueueName, listSteps, newWorkflow, secondsDuration, setEventStepName, sleepStepName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.Transact
  ( CodecError,
    Config (..),
    DBOS,
    EngineOnly,
    Error (..),
    Executor,
    RunOptions (..),
    Serializer (..),
    WorkflowCtx,
    WorkflowRef,
    decodeWorkflowValue,
    encodeWorkflowValue,
    getEvent,
    getWorkflowEvent,
    millisDuration,
    newDBOS,
    newWorkflowKey,
    pendingGetEvent,
    pendingSetEvent,
    pendingSleep,
    pendingStep,
    registerWorkflowRef,
    runWorkflowRef,
    runOptionsDefault,
    runStep,
    runStepWith,
    setEvent,
    shutdown,
    stepOptionsDefault,
  )
import DBOS.Transact.Logger (SomeTracer (..))
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.Checkpoint (PendingStep (..))
import DBOS.Transact.Instance (dequeueWorkflows, launchExecutor, launchOn, launchOnWithQueues)
import DBOS.Transact.Context (firstStepStatus, nextStepId, nextWorkflowMarker, withStep, withWorkflow)
import DBOS.Transact.Checkpoint (pendingStepId)
import DBOS.Transact.Connection
  ( Connection,
    Owner (..),
    SomeSystemDB (..),
    newConnection,
    runSystemDB,
  )
import DBOS.Transact.Context (StepCtx (stepCtxWorkflow))

-- | How a tree instantiation builds its world: fresh unlaunched instances
-- (plus a second one for the cross-instance refusal), the stack-specific
-- launches, fresh workflow ids, a connection and identity for direct
-- scopes, and backend reads for seeding and step inspection. Live fills
-- the rest with Postgres and per-test UUIDs; the sim tree with
-- 'MemSystemDB' and deterministic ids.
data EventFixture m = EventFixture
  { efNewDBOS :: m (DBOS m),
    efNewOtherDBOS :: m (DBOS m),
    efLaunch :: DBOS m -> m (Executor m),
    efLaunchOther :: DBOS m -> m (Executor m),
    efRelaunch :: DBOS m -> m (Executor m),
    efFreshId :: Text -> m WorkflowId,
    efConn :: m (Connection m),
    efIdentity :: Identity,
    efInitRow :: Text -> Text -> m (),
    efStepNames :: WorkflowId -> m [(Int, Text)],
    efStepAbsent :: WorkflowId -> Int -> Text -> m Bool
  }

-- | One fixture builder over any backend: the tree passes its 'Config's,
-- 'Identity's, connection app name, id naming, and id/entropy generators,
-- plus its 'SomeSystemDB' and 'SomeTracer'. Live passes Postgres +
-- FastLogger; sim passes 'MemSystemDB' + the sim carrier.
mkEventFixture ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  Config ->
  Config ->
  Identity ->
  Identity ->
  Text ->
  (Text -> WorkflowId) ->
  m Text ->
  m Word32 ->
  SomeSystemDB m ->
  SomeTracer m ->
  m (EventFixture m)
mkEventFixture config otherConfig identity otherIdentity connApp nameScheme genId genEntropy sysdb tracer = do
  conn <- mkConn
  otherConn <- mkConn
  pure
    EventFixture
      { efNewDBOS = newDBOS config,
        efNewOtherDBOS = newDBOS otherConfig,
        efLaunch = \dbos -> launchOn dbos conn identity,
        efLaunchOther = \dbos -> launchOn dbos otherConn otherIdentity,
        -- Relaunch through the full tail: version registration, recovery
        -- of this executor's pending rows, the launch announcement, and a
        -- fresh supervisor — what a crash restart runs. The listen set is
        -- the internal queue only, so neither the supervisor nor the driven
        -- pass below sweeps foreign queue rows in the shared database. A
        -- bare 'efLaunch' installs the executor but recovers nothing, so a
        -- joined pending run would wait forever.
        efRelaunch = \dbos -> do
          let QueueName internal = internalQueueName
          exec <- launchOnWithQueues dbos conn identity (Just [internal])
          launchExecutor dbos exec >>= either (throwIO . userError . show) pure,
        efFreshId = pure . nameScheme,
        efConn = pure conn,
        efIdentity = identity,
        efInitRow = \widText name -> do
          created <- runSystemDB sysdb (\db -> initWorkflow db ((newWorkflow widText) {newWorkflowName = Just name}) Nothing Fresh Nothing)
          case created of
            Left err -> throwIO (userError (show err))
            Right _ -> pure (),
        efStepNames = \wid -> do
          listed <- runSystemDB sysdb (\db -> listSteps db wid True Nothing Nothing Nothing)
          case listed of
            Left err -> throwIO (userError (show err))
            Right rows -> pure [(row.stepRecordStepId, row.stepRecordStepName) | row <- rows],
        efStepAbsent = \wid step name -> do
          found <- runSystemDB sysdb (\db -> SystemDB.checkStep db wid step name)
          case found of
            Right Nothing -> pure True
            _ -> pure False
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

-- | Engine-only driver alias: the recovery and order cases read through
-- this, so the error channel pins to 'EngineOnly' once.
runRef :: forall m. (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) => Executor m -> WorkflowRef m EngineOnly -> RunOptions -> Maybe SerializedWorkflowValue -> m (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runRef = runWorkflowRef

-- | A wait that polls for a published event instead of sleeping through it.
-- Under IOSim the timeout is virtual, so a hung wait fails fast; live it
-- throws after fifty polls, which tasty reports as a failure.
waitForEvent :: (MonadMVar m, MonadDelay m, MonadTime m, MonadThrow m) => DBOS m -> WorkflowId -> Text -> m ()
waitForEvent dbos wid key = go (50 :: Int)
  where
    go 0 = throwIO (userError "the workflow never published an event")
    go n = do
      found <- getWorkflowEvent dbos wid key (millisDuration 0)
      case found of
        Right (Just _) -> pure ()
        _ -> threadDelay 200000 >> go (n - 1)

-- | The recovery body: publishes the offered value (or "republished" when
-- the offer is already taken), then parks until released.
progressBody ::
  forall exec m.
  (MonadMVar m, MonadSTM m) =>
  StrictMVar m Text ->
  StrictMVar m () ->
  () ->
  WorkflowCtx exec m ->
  m (Either (Error EngineOnly) Int)
progressBody offer release () wctx = do
  proposal <- tryTakeMVar offer
  published <- setEvent wctx "progress" (maybe "republished" id proposal)
  case published of
    Left err -> pure (Left err)
    Right () -> takeMVar release >> pure (Right 7)

-- | Reads through the other instance: refused, never combined.
readerBody ::
  forall exec m.
  (MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m) =>
  DBOS m ->
  () ->
  WorkflowCtx exec m ->
  m (Either (Error EngineOnly) (Maybe Int))
readerBody other () wctx = do
  built <- pendingGetEvent other wctx (WorkflowId "wf-1") "answer" (millisDuration 0)
  built.pendingRun

-- | The same read from inside a step: plain, nothing checkpointed.
inStepReaderBody ::
  forall exec m.
  (MonadMVar m, MonadSTM m, MonadTime m, MonadDelay m, MonadCatch m) =>
  DBOS m ->
  () ->
  WorkflowCtx exec m ->
  m (Either (Error EngineOnly) (Maybe Int))
inStepReaderBody other () wctx = do
  stepped <- runStep wctx "read" $ \inner -> do
    built <- pendingGetEvent other inner.stepCtxWorkflow (WorkflowId "wf-1") "answer" (millisDuration 0)
    built.pendingRun
  pure $ case stepped of
    Left err -> Left err
    Right read -> read

-- | Builds a sleep, a set, a get, and a step, then drives them out of
-- build order. The ids stay in build order regardless.
outOfOrderBody ::
  forall exec m.
  (MonadMVar m, MonadAsync m, MonadTime m, MonadDelay m, MonadCatch m) =>
  DBOS m ->
  () ->
  WorkflowCtx exec m ->
  m (Either (Error EngineOnly) ())
outOfOrderBody dbos () wctx = do
  a <- pendingSleep wctx (millisDuration 1)
  b <- pendingSetEvent wctx "b" (1 :: Int)
  c <- (pendingGetEvent dbos wctx (WorkflowId "no-such-workflow") "nothing" (millisDuration 0) :: m (PendingStep exec m (Either (Error EngineOnly) (Maybe Int))))
  d <- (pendingStep wctx "after" (\_ -> pure (Right (1 :: Int))) :: m (PendingStep exec m (Either (Error EngineOnly) Int)))
  idsOk <- case (pendingStepId a, pendingStepId b, pendingStepId c, pendingStepId d) of
    (Just 0, Just 1, Just 2, Just 4) -> pure True
    _ -> pure False
  if not idsOk
    then pure (Left (StepFailed "joins" "ids were taken out of source order"))
    else do
      dResult <- d.pendingRun
      cResult <- c.pendingRun
      bResult <- b.pendingRun
      aResult <- a.pendingRun
      pure $ case (dResult, cResult, bResult, aResult) of
        (Right _, Right Nothing, Right _, Right _) -> Right ()
        _ -> Left (StepFailed "joins" "a branch answered wrong")

-- | A workflow can publish and replay an event. Returns the first read
-- and the replay's read.
scenarioPublishReplay ::
  forall m.
  (MonadSTM m, MonadTime m, MonadDelay m, MonadThrow m) =>
  EventFixture m ->
  m (Maybe Text, Maybe Text)
scenarioPublishReplay fx = do
  wid <- fx.efFreshId "event"
  let WorkflowId widText = wid
  fx.efInitRow widText "L2EventTest"
  let action :: forall exec. WorkflowCtx exec m -> m (Either (Error EngineOnly) (Maybe Text))
      action wctx = do
        published <- setEvent wctx "progress" ("ready" :: Text)
        case published of
          Left err -> pure (Left err)
          Right () -> getEvent wctx wid "progress" (millisDuration 100)
  conn <- fx.efConn
  first <- withWorkflow conn fx.efIdentity wid Nothing action
  firstRead <- case first of
    Left err -> throwIO (userError (show err))
    Right read -> pure read
  replay <- withWorkflow conn fx.efIdentity wid Nothing action
  replayRead <- case replay of
    Left err -> throwIO (userError (show err))
    Right read -> pure read
  pure (firstRead, replayRead)

-- | A reading workflow is checkpointed and a reading step is not. Returns
-- the outside read with its steps, and the inside read with its steps
-- plus whether the slot after the step stayed empty.
scenarioCheckpointedRead ::
  forall m.
  (MonadAsync m, MonadCatch m, MonadTime m, MonadDelay m) =>
  EventFixture m ->
  m ((Maybe Int, [(Int, Text)]), (Maybe Int, [(Int, Text)], Bool))
scenarioCheckpointedRead fx = do
  wid <- fx.efFreshId "event-steps"
  let WorkflowId prefix = wid
      publisher = WorkflowId (prefix <> "-publisher")
      reader = WorkflowId (prefix <> "-reader")
      inStepper = WorkflowId (prefix <> "-in-step")
      create w name = do
        let WorkflowId t = w
        fx.efInitRow t name
  create publisher "L2EventPublisher"
  create reader "L2EventReader"
  create inStepper "L2EventInStep"
  conn <- fx.efConn
  published <- withWorkflow conn fx.efIdentity publisher Nothing $ \wctx ->
    setEvent wctx "answer" (42 :: Int)
  case published of
    Left err -> throwIO (userError (show err))
    Right () -> pure ()
  outside <- withWorkflow conn fx.efIdentity reader Nothing $ \wctx ->
    (getEvent wctx publisher "answer" (millisDuration 0) :: m (Either (Error EngineOnly) (Maybe Int)))
  outsideRead <- case outside of
    Left err -> throwIO (userError (show err))
    Right read -> pure read
  outsideSteps <- fx.efStepNames reader
  inside <- withWorkflow conn fx.efIdentity inStepper Nothing $ \wctx ->
    (runStepWith stepOptionsDefault wctx "read" (\sctx -> getEvent sctx.stepCtxWorkflow publisher "answer" (millisDuration 0)) :: m (Either (Error EngineOnly) (Maybe Int)))
  insideRead <- case inside of
    Left err -> throwIO (userError (show err))
    Right read -> pure read
  insideSteps <- fx.efStepNames inStepper
  spare <- fx.efStepAbsent inStepper 1 getEventStepName
  pure ((outsideRead, outsideSteps), (insideRead, insideSteps, spare))

-- | A refused set event spends no step id. Returns the refusal's operation
-- and the step counter before and after.
scenarioRefusedSet ::
  forall m.
  (MonadSTM m, MonadCatch m) =>
  EventFixture m ->
  m (Text, Int, Int)
scenarioRefusedSet fx = do
  wid <- fx.efFreshId "event-refusal"
  let WorkflowId widText = wid
  fx.efInitRow widText "L2EventRefusal"
  conn <- fx.efConn
  (refused, before, after) <-
    withWorkflow conn fx.efIdentity wid Nothing $ \wctx -> do
      marker <- nextWorkflowMarker wctx
      withStep wctx marker (firstStepStatus 0) $ \_sctx -> do
        before <- nextStepId wctx
        refused <- setEvent wctx "progress" ("ready" :: Text)
        after <- nextStepId wctx
        pure (refused, before, after)
  op <- case refused of
    Left (InsideStep operation) -> pure operation
    other -> throwIO (userError ("expected an in-step refusal, got: " <> show other))
  pure (op, before, after)

-- | A getEvent through a captured parent is plain and moves no ids.
-- Returns the read and the step counter before and after.
scenarioCapturedRead ::
  forall m.
  (MonadSTM m, MonadCatch m, MonadTime m, MonadDelay m) =>
  EventFixture m ->
  m (Maybe Int, Int, Int)
scenarioCapturedRead fx = do
  wid <- fx.efFreshId "event-captured"
  let WorkflowId prefix = wid
      publisher = WorkflowId (prefix <> "-publisher")
      reader = WorkflowId (prefix <> "-reader")
      create w name = do
        let WorkflowId t = w
        fx.efInitRow t name
  create publisher "L2EventPublisher"
  create reader "L2EventReader"
  conn <- fx.efConn
  published <- withWorkflow conn fx.efIdentity publisher Nothing $ \wctx ->
    setEvent wctx "answer" (42 :: Int)
  case published of
    Left err -> throwIO (userError (show err))
    Right () -> pure ()
  (readCaptured, before, after) <-
    withWorkflow conn fx.efIdentity reader Nothing $ \wctx -> do
      marker <- nextWorkflowMarker wctx
      withStep wctx marker (firstStepStatus 0) $ \_sctx -> do
        before <- nextStepId wctx
        readCaptured <- (getEvent wctx publisher "answer" (millisDuration 0) :: m (Either (Error EngineOnly) (Maybe Int)))
        after <- nextStepId wctx
        pure (readCaptured, before, after)
  read <- case readCaptured of
    Left err -> throwIO (userError (show err))
    Right value -> pure value
  pure (read, before, after)

-- | A replayed set event does not republish. Returns what the reader sees.
scenarioReplayNoRepublish ::
  forall m.
  (MonadSTM m, MonadTime m, MonadDelay m, MonadThrow m) =>
  EventFixture m ->
  m (Maybe Text)
scenarioReplayNoRepublish fx = do
  wid <- fx.efFreshId "event-replay"
  let WorkflowId widText = wid
      reader = WorkflowId (widText <> "-reader")
  fx.efInitRow widText "L2EventReplay"
  conn <- fx.efConn
  first <- withWorkflow conn fx.efIdentity wid Nothing $ \wctx ->
    setEvent wctx "progress" ("first" :: Text)
  case first of
    Left err -> throwIO (userError (show err))
    Right () -> pure ()
  replayed <- withWorkflow conn fx.efIdentity wid Nothing $ \wctx ->
    setEvent wctx "progress" ("second" :: Text)
  case replayed of
    Left err -> throwIO (userError (show err))
    Right () -> pure ()
  let WorkflowId readerText = reader
  fx.efInitRow readerText "L2EventReplayReader"
  readBack <- withWorkflow conn fx.efIdentity reader Nothing $ \wctx ->
    (getEvent wctx wid "progress" (millisDuration 0) :: m (Either (Error EngineOnly) (Maybe Text)))
  case readBack of
    Left err -> throwIO (userError (show err))
    Right value -> pure value

-- | Progress events survive recovery without republishing. Returns the
-- recovered result and the kept event value.
scenarioRecoveryKeepsFirst ::
  forall m.
  (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  EventFixture m ->
  m (Int, Text)
scenarioRecoveryKeepsFirst fx = do
  bracket fx.efNewDBOS shutdown $ \dbos -> do
    offer <- newEmptyMVar
    release <- newEmptyMVar
    putMVar offer ("first" :: Text)
    refE <- registerWorkflowRef dbos (newWorkflowKey "progress") (progressBody offer release)
    ref <- case refE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.efLaunch dbos
    wid <- fx.efFreshId "event-recovery-id"
    worker <- async (runRef exec ref (runOptionsDefault {runWorkflowId = Just wid}) Nothing)
    waitForEvent dbos wid "progress"
    shutdown dbos
    cancel worker
    putMVar release ()
    exec2 <- fx.efRelaunch dbos
    -- Drive the supervisor's pass synchronously instead of waiting for its
    -- tick: one pass claims the recovered row and spawns its rerun, and the
    -- join below waits for that rerun rather than for a poll interval.
    -- (One pass, not a loop: the recovery holds exactly one row.)
    drove <- dequeueWorkflows dbos
    case drove of
      Left err -> throwIO (userError (show err))
      Right _ -> pure ()
    ran <- runRef exec2 ref (runOptionsDefault {runWorkflowId = Just wid}) Nothing
    decoded <- case ran of
      Right (Just stored) ->
        case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
          Right n -> pure n
          Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the recovered result, got: " <> show other))
    final <- getWorkflowEvent dbos wid "progress" (millisDuration 100)
    kept <- case final of
      Right (Just stored) ->
        case decodeWorkflowValue "event" (Just stored) :: Either CodecError Text of
          Right value -> pure value
          Left err -> throwIO (userError (show err))
      other -> throwIO (userError ("expected the kept event, got: " <> show other))
    pure (decoded, kept)

-- | Reading through another instance from inside a workflow is refused,
-- while the in-step read stays plain. Returns whether the refusal fired
-- and what the plain read found.
scenarioWrongInstance ::
  forall m.
  (MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  EventFixture m ->
  m (Bool, Maybe Int)
scenarioWrongInstance fx = do
  bracket fx.efNewOtherDBOS shutdown $ \other ->
    bracket fx.efNewDBOS shutdown $ \owner -> do
      readerE <- registerWorkflowRef owner (newWorkflowKey "reads_through_other") (readerBody other)
      readerRef <- case readerE of
        Left err -> throwIO (userError (show err))
        Right ref -> pure ref
      inStepE <- registerWorkflowRef owner (newWorkflowKey "reads_in_step") (inStepReaderBody other)
      inStepRef <- case inStepE of
        Left err -> throwIO (userError (show err))
        Right ref -> pure ref
      _ <- fx.efLaunchOther other
      execOwner <- fx.efLaunch owner
      wrongWid <- fx.efFreshId "event-wrong-id"
      inStepWid <- fx.efFreshId "event-in-step-id"
      ran <- runRef execOwner readerRef (runOptionsDefault {runWorkflowId = Just wrongWid}) (Just (encodeWorkflowValue ()))
      refused <- case ran of
        Left (WrongInstance _) -> pure True
        otherOutcome -> throwIO (userError ("expected a wrong-instance refusal, got: " <> show otherOutcome))
      ranInStep <- runRef execOwner inStepRef (runOptionsDefault {runWorkflowId = Just inStepWid}) (Just (encodeWorkflowValue ()))
      plain <- case ranInStep of
        Right (Just stored) ->
          case decodeWorkflowValue "result" (Just stored) :: Either CodecError (Maybe Int) of
            Right value -> pure value
            Left err -> throwIO (userError (show err))
        otherOutcome -> throwIO (userError ("expected a plain read of nothing, got: " <> show otherOutcome))
      pure (refused, plain)

-- | Library calls driven out of build order keep the ids they were built
-- with. Returns the recorded steps.
scenarioOutOfOrderIds ::
  forall m.
  (MonadAsync m, MonadFork m, MonadMask m, MonadMVar m, MonadTimer m, MonadTime m) =>
  EventFixture m ->
  m [(Int, Text)]
scenarioOutOfOrderIds fx = do
  bracket fx.efNewDBOS shutdown $ \dbos -> do
    refE <- registerWorkflowRef dbos (newWorkflowKey "joins") (outOfOrderBody dbos)
    ref <- case refE of
      Left err -> throwIO (userError (show err))
      Right r -> pure r
    exec <- fx.efLaunch dbos
    wid <- fx.efFreshId "joins-out-of-order"
    ran <- runRef exec ref (runOptionsDefault {runWorkflowId = Just wid}) (Just (encodeWorkflowValue ()))
    case ran of
      Right _ -> pure ()
      other -> throwIO (userError ("the workflow failed: " <> show other))
    fx.efStepNames wid

-- | The event is visible to a workflow reader, and the replay reads the
-- recorded value.
checkPublishReplay :: (Maybe Text, Maybe Text) -> Either String ()
checkPublishReplay = checkEq (Just ("ready" :: Text), Just ("ready" :: Text))

-- | The outside read checkpoints its get and sleep; the in-step read
-- records only its step and leaves the slot after empty.
checkCheckpointedRead :: ((Maybe Int, [(Int, Text)]), (Maybe Int, [(Int, Text)], Bool)) -> Either String ()
checkCheckpointedRead ((outsideRead, outsideSteps), (insideRead, insideSteps, spare)) = do
  checkEq (Just 42, [(0, getEventStepName), (1, sleepStepName)]) (outsideRead, outsideSteps)
  checkEq (Just 42, [(0, ("read" :: Text))]) (insideRead, insideSteps)
  checkEq True spare

-- | The refusal names the set, and the counter moved only for the probe.
checkRefusedSet :: (Text, Int, Int) -> Either String ()
checkRefusedSet (op, before, after) = do
  checkEq ("set_event" :: Text) op
  checkEq (before + 1) after

-- | The captured read sees the value and moves no ids beyond the probe.
checkCapturedRead :: (Maybe Int, Int, Int) -> Either String ()
checkCapturedRead (read, before, after) = do
  checkEq (Just 42) read
  checkEq (before + 1) after

-- | The reader sees the first published value, never the replay's.
checkReplayNoRepublish :: Maybe Text -> Either String ()
checkReplayNoRepublish = checkEq (Just ("first" :: Text))

-- | The recovered run records its result and keeps the first value.
checkRecoveryKeepsFirst :: (Int, Text) -> Either String ()
checkRecoveryKeepsFirst = checkEq (7, ("first" :: Text))

-- | The cross-instance read is refused; the in-step read finds nothing.
checkWrongInstance :: (Bool, Maybe Int) -> Either String ()
checkWrongInstance = checkEq (True, Nothing)

-- | The recorded steps keep build order despite out-of-order driving.
checkOutOfOrderIds :: [(Int, Text)] -> Either String ()
checkOutOfOrderIds = checkEq [(0, sleepStepName), (1, setEventStepName), (2, getEventStepName), (3, sleepStepName), (4, ("after" :: Text))]

-- | Pure verdicts; both trees judge through these.
checkEq :: (Eq a, Show a) => a -> a -> Either String ()
checkEq expected actual
  | expected == actual = Right ()
  | otherwise = Left ("expected " <> show expected <> ", got " <> show actual)
