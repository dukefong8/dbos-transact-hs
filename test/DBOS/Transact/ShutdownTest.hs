{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Shutting down while workflows run, over live Postgres: a shutdown
-- with a workflow-completion timeout stops the producers, waits up to
-- the timeout for the workflows already running, and cancels what is
-- left. The zero timeout — cancel at once — is covered where the rest of
-- the abandonment tests live. Every case owns its rows via a unique tag
-- and its queues via a unique suffix. Live only: the cases race real
-- threads against shutdown sleeps, which no IOSim mirror can stage.
module DBOS.Transact.ShutdownTest (tests) where

import DBOS.Prelude
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (WorkflowRecord (..))
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
import DBOS.Transact.Connection (SomeSystemDB (..), runSystemDB)
import DBOS.Transact.Logger (nullTracer)
import DBOS.Transact.Serialization (CodecError (..), decodeWorkflowValue, encodeWorkflowInput)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestTree
tests = testGroup "Shutdown" [testCase "a draining shutdown lets a running workflow finish" drainingLetsRunningFinish, testCase "nothing is dequeued once shutdown begins" nothingDequeuedOnceStopping, testCase "a workflow that outlasts the timeout is cancelled and recovered" outlastedIsCancelledAndRecovered, testCase "dropping the shutdown future does not cut the shutdown short" droppedShutdownStillFinishes, testCase "a wait after shutdown fails permanently instead of polling" postCloseWaitFailsPermanently]

-- | How long a case waits for a workflow it expects to settle. Nothing
-- legitimate takes this long, so a hang fails instead of holding the
-- suite. Mirrors the oracle's DEADLINE.
deadlineMicros :: Int
deadlineMicros = 60000000

newTag :: IO Text
newTag = Text.take 12 . Text.filter (/= '-') . Text.pack . UUID.toString <$> UUID.V4.nextRandom

draining :: Duration -> ShutdownOptions
draining timeout = ShutdownOptions {shutdownWorkflowCompletionTimeout = timeout}

-- | Builds an instance over a fresh application and queue set, torn
-- down by the caller. Registration happens inside, before the test
-- launches: launch freezes the registry.
withDBOS :: Text -> Maybe [Text] -> (DBOS IO -> IO a) -> IO a
withDBOS suffix listenQueues body = do
  let appName = "hs-shutdown-" <> suffix
  config0 <- configFromEnv appName
  let config =
        config0
          { configAppVersion = Just ("hs-shutdown-version-" <> suffix),
            configExecutorId = Just ("hs-shutdown-executor-" <> suffix),
            configListenQueues = listenQueues
          }
  dbos <- newDBOS config
  bracket (pure ()) (\_ -> shutdown dbos) (\_ -> body dbos)

-- | Polls a workflow's row until it carries the expected status.
awaitStatus :: DBOS IO -> WorkflowId -> WorkflowStatus -> IO ()
awaitStatus dbos wid status = go (0 :: Int)
  where
    go attempts = do
      found <- getWorkflowRecord dbos wid >>= either (fail . show) pure
      case found of
        Just row | row.workflowRecordStatus == status -> pure ()
        _ ->
          if attempts >= 600
            then fail ("the workflow never reached " <> show status <> ": " <> show wid)
            else threadDelay 100000 >> go (attempts + 1)

-- | Reads a workflow's row behind the instance's back, for assertions
-- after shutdown has taken the executor: instance calls would answer
-- NotLaunched. Mirrors the oracle's reader.
readRow :: WorkflowId -> IO (Maybe WorkflowRecord)
readRow wid = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  bracket (pure backend) Postgres.releasePostgresSystemDB $ \env ->
    runSystemDB (SomeSystemDB env) (\db -> SystemDB.getWorkflow db wid) >>= either (fail . show) pure

-- | A workflow mid-flight when shutdown begins is let finish: it
-- checkpoints its next step, starts a child, and records its outcome,
-- all before the system database closes. The instance stays launched
-- while it waits, and shutdown does not return until the workflow has
-- finished.
drainingLetsRunningFinish :: IO ()
drainingLetsRunningFinish = do
  suffix <- newTag
  reachedGate <- newEmptyMVar
  releaseGate <- newEmptyMVar
  withDBOS suffix Nothing $ \dbos -> do
    let childBody :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
        childBody n _ = pure (Right (n * 2))
    childRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "child") childBody >>= either (fail . show) pure
    let parentBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
        parentBody _ wctx = do
          before <- runStep wctx "before" (\_ -> pure (1 :: Int))
          case before of
            Left err -> pure (Left err)
            Right _ -> do
              putMVar reachedGate ()
              takeMVar releaseGate
              -- After shutdown began: the checkpoint and the child's start
              -- both still need the system database, and the child starts
              -- here rather than dequeued.
              after <- runStep wctx "after" (\_ -> pure (2 :: Int))
              case after of
                Left err -> pure (Left err)
                Right n -> do
                  started <- startChildWorkflow wctx childRef startOptionsDefault (Just (encodeWorkflowInput n))
                  case started of
                    Left err -> pure (Left err)
                    Right child -> do
                      awaited <- handleResult child
                      case awaited of
                        Left err -> pure (Left err)
                        Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
                          Right doubled -> pure (Right doubled)
                          Left err -> throwIO (userError (show err))
                        Right Nothing -> throwIO (userError "the child returned no output")
    parentRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "parent") parentBody >>= either (fail . show) pure
    exec <- launch dbos >>= either (fail . show) pure
    let parentId = WorkflowId ("hs-shutdown-drained-" <> suffix)
        childId = WorkflowId ("hs-shutdown-drained-" <> suffix <> "-2")
    startedParent <-
      startWorkflow exec parentRef (startOptionsDefault {startWorkflowId = Just parentId}) (Just (encodeWorkflowInput ()))
    handle <- case (startedParent :: Either (Error EngineOnly) (WorkflowHandle IO EngineOnly)) of
      Left err -> fail (show err)
      Right h -> pure h
    finished <- newEmptyMVar
    _ <- forkIO (handleResult handle >>= putMVar finished)
    _ <- timeout deadlineMicros (takeMVar reachedGate) >>= maybe (fail "the workflow never reached its gate") pure
    startedAt <- getMonotonicTimeNSec
    shuttingDown <- newEmptyMVar
    _ <- forkIO (shutdownWith dbos (draining (secondsDuration 30)) >>= putMVar shuttingDown)
    early <- timeout 500000 (takeMVar shuttingDown)
    case early of
      Just _ -> fail "shutdown returned while a workflow was still running"
      Nothing -> pure ()
    launched <- isLaunched dbos
    unless launched (fail "the instance stays launched while it waits")
    putMVar releaseGate ()
    _ <- timeout deadlineMicros (takeMVar shuttingDown) >>= maybe (fail "shutdown never returned") pure
    finishedAt <- getMonotonicTimeNSec
    unless (finishedAt - startedAt < 30000000000) (fail "shutdown waited out its timeout rather than returning when the workflow finished")
    launchedAfter <- isLaunched dbos
    when launchedAfter (fail "the instance is shut down after the drain")
    outcome <- timeout deadlineMicros (takeMVar finished) >>= maybe (fail "the drained workflow never reported") pure
    case outcome of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right total -> total @?= 4
        Left err -> fail (show err)
      other -> fail ("the drained workflow failed: " <> show other)
    parentRow <- readRow parentId
    case parentRow of
      Just row -> row.workflowRecordStatus @?= Success
      Nothing -> fail "the drained workflow has no row"
    childRow <- readRow childId
    case childRow of
      Just row -> row.workflowRecordStatus @?= Success
      Nothing -> fail "the drained child has no row"

-- | A handle kept across shutdown fails at once instead of polling a
-- released pool: the closed handle is a permanent backend error, which is
-- the port's @PoolClosed => Permanent@. The workflow parks, so its row
-- stays PENDING and a live poll would never settle.
postCloseWaitFailsPermanently :: IO ()
postCloseWaitFailsPermanently = do
  suffix <- newTag
  withDBOS suffix Nothing $ \dbos -> do
    let parked :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
        parked _ _ = threadDelay 30000000 >> pure (Right 0)
    ref <- registerWorkflowRef dbos (newQualifiedWorkflowName "parked") parked >>= either (fail . show) pure
    exec <- launch dbos >>= either (fail . show) pure
    let widText = "hs-shutdown-closed-" <> suffix
    started <- startWorkflow exec ref (startOptionsDefault {startWorkflowId = Just (WorkflowId widText)}) (Just (encodeWorkflowInput ()))
    _ <- either (fail . show) pure (started :: Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
    handle <- retrieveWorkflow dbos (WorkflowId widText) >>= either (fail . show) pure
    shutdown dbos
    result <- timeout 5000000 (handleResult (handle :: WorkflowHandle IO EngineOnly)) >>= maybe (fail "a post-shutdown wait hung") pure
    case result of
      Left (SystemDatabase (SystemDB.Backend err)) -> do
        err.backendKind @?= SystemDB.Permanent
        assertBool "the refusal names the closed handle" ("closed" `Text.isInfixOf` err.backendMessage)
      other -> fail ("expected a permanent closed-handle refusal, got: " <> show other)

-- | Once shutdown has begun, a workflow put on a queue stays there: the
-- dequeue loop has stopped, though the instance is still launched and the
-- enqueue itself succeeds. The queue's worker runs before shutdown
-- begins, so both halves of the loop stop here.
nothingDequeuedOnceStopping :: IO ()
nothingDequeuedOnceStopping = do
  suffix <- newTag
  let queueName = "hs-shutdown-drain-q-" <> suffix
  reachedGate <- newEmptyMVar
  releaseGate <- newEmptyMVar
  runsVar <- newTVarIO (0 :: Int)
  withDBOS suffix (Just [queueName]) $ \dbos -> do
    let holderBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
        holderBody _ _ = do
          putMVar reachedGate ()
          takeMVar releaseGate
          pure (Right ())
        queuedBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
        queuedBody _ _ = do
          atomically (modifyTVar runsVar (+ 1))
          pure (Right ())
    holderRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "holder") holderBody >>= either (fail . show) pure
    queuedRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "queued") queuedBody >>= either (fail . show) pure
    exec <- launch dbos >>= either (fail . show) pure
    _ <- registerQueue dbos queueName defaultQueueOptions NeverUpdate >>= either (fail . show) pure
    -- One run through the queue, so its worker exists by the time
    -- shutdown begins.
    startedWarm <-
      startWorkflow exec queuedRef (startOptionsDefault {startQueue = Just (queueRequestNew queueName)}) (Just (encodeWorkflowInput ()))
    warm <- case (startedWarm :: Either (Error EngineOnly) (WorkflowHandle IO EngineOnly)) of
      Left err -> fail (show err)
      Right h -> pure h
    _ <- timeout deadlineMicros (handleResult warm) >>= maybe (fail "the warm-up run never finished") pure
    readTVarIO runsVar >>= (@?= 1)
    -- Something for the shutdown to wait on, so it is still draining when
    -- the enqueue lands.
    holding <- newEmptyMVar
    _ <- forkIO $ do
      startedHolding <- startWorkflow exec holderRef startOptionsDefault (Just (encodeWorkflowInput ()))
      putMVar holding (startedHolding :: Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
    _ <- timeout deadlineMicros (takeMVar reachedGate) >>= maybe (fail "the holder never reached its gate") pure
    shuttingDown <- newEmptyMVar
    _ <- forkIO (shutdownWith dbos (draining (secondsDuration 30)) >>= putMVar shuttingDown)
    -- Long enough for a dequeue already in flight when shutdown began to
    -- have finished.
    threadDelay 500000
    let enqueuedId = WorkflowId ("hs-shutdown-enqueued-" <> suffix)
    startedEnqueued <-
      startWorkflow
        exec
        queuedRef
        (startOptionsDefault {startWorkflowId = Just enqueuedId, startQueue = Just (queueRequestNew queueName)})
        (Just (encodeWorkflowInput ()))
    case (startedEnqueued :: Either (Error EngineOnly) (WorkflowHandle IO EngineOnly)) of
      Left err -> fail (show err)
      Right _ -> pure ()
    -- Several of the queue's polling intervals: a dequeue loop still
    -- running would have taken it.
    threadDelay 3000000
    enqueuedRow <- getWorkflowRecord dbos enqueuedId >>= either (fail . show) pure
    case enqueuedRow of
      Just row -> row.workflowRecordStatus @?= Enqueued
      Nothing -> fail "the enqueue wrote no row"
    putMVar releaseGate ()
    _ <- timeout deadlineMicros (takeMVar shuttingDown) >>= maybe (fail "shutdown never returned") pure
    _ <- timeout deadlineMicros (takeMVar holding) >>= maybe (fail "the holder never finished") pure
    readTVarIO runsVar >>= (@?= 1)
    settledRow <- readRow enqueuedId
    case settledRow of
      Just row -> row.workflowRecordStatus @?= Enqueued
      Nothing -> fail "the enqueued row is gone"

-- | A workflow still running at the deadline is cancelled there and left
-- PENDING, and a relaunch recovers it from its last checkpoint.
outlastedIsCancelledAndRecovered :: IO ()
outlastedIsCancelledAndRecovered = do
  suffix <- newTag
  reachedGate <- newEmptyMVar
  releaseGate <- newEmptyMVar
  stepsVar <- newTVarIO (0 :: Int)
  withDBOS suffix Nothing $ \dbos -> do
    let slowBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
        slowBody _ wctx = do
          counted <- runStep wctx "counted" $ \_ -> do
            atomically (modifyTVar stepsVar (+ 1))
            pure ()
          case counted of
            Left err -> pure (Left err)
            Right () -> do
              putMVar reachedGate ()
              takeMVar releaseGate
              pure (Right "finished after recovery")
    slowRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "slow") slowBody >>= either (fail . show) pure
    exec <- launch dbos >>= either (fail . show) pure
    let wid = WorkflowId ("hs-shutdown-outlasts-" <> suffix)
    startedSlow <-
      startWorkflow exec slowRef (startOptionsDefault {startWorkflowId = Just wid}) (Just (encodeWorkflowInput ()))
    handle <- case (startedSlow :: Either (Error EngineOnly) (WorkflowHandle IO EngineOnly)) of
      Left err -> fail (show err)
      Right h -> pure h
    running <- newEmptyMVar
    _ <- forkIO (handleResult handle >>= putMVar running)
    _ <- timeout deadlineMicros (takeMVar reachedGate) >>= maybe (fail "the workflow never reached its gate") pure
    startedAt <- getMonotonicTimeNSec
    _ <- timeout deadlineMicros (shutdownWith dbos (draining (millisDuration 500))) >>= maybe (fail "shutdown never returned") pure
    finishedAt <- getMonotonicTimeNSec
    -- A millisecond of tolerance: the deadline math truncates to whole
    -- microseconds, so a wait that lands a fraction early is still the
    -- deadline being honoured, not an early cancel.
    unless (finishedAt - startedAt >= 499000000) (fail "shutdown cancelled the workflow before its deadline")
    outcome <- timeout deadlineMicros (takeMVar running) >>= maybe (fail "the run never reported") pure
    case outcome of
      Left (Interrupted {}) -> pure ()
      other -> fail ("expected the run interrupted, got: " <> show other)
    cancelledRow <- readRow wid
    case cancelledRow of
      Just row -> row.workflowRecordStatus @?= Pending
      Nothing -> fail "the cancelled workflow has no row"
    _ <- launch dbos >>= either (fail . show) pure
    _ <- timeout deadlineMicros (takeMVar reachedGate) >>= maybe (fail "the recovered workflow never reached its gate again") pure
    putMVar releaseGate ()
    awaitStatus dbos wid Success
    readTVarIO stepsVar >>= (@?= 1)

-- | A caller that stops waiting for shutdown does not stop the shutdown:
-- the drain goes on, the workflow finishes, and the instance ends up shut
-- down rather than half-stopped.
droppedShutdownStillFinishes :: IO ()
droppedShutdownStillFinishes = do
  suffix <- newTag
  reachedGate <- newEmptyMVar
  releaseGate <- newEmptyMVar
  withDBOS suffix Nothing $ \dbos -> do
    let heldBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
        heldBody _ _ = do
          putMVar reachedGate ()
          takeMVar releaseGate
          pure (Right 7)
    heldRef <- registerWorkflowRef dbos (newQualifiedWorkflowName "held") heldBody >>= either (fail . show) pure
    exec <- launch dbos >>= either (fail . show) pure
    startedHeld <-
      startWorkflow exec heldRef startOptionsDefault (Just (encodeWorkflowInput ()))
    handle <- case (startedHeld :: Either (Error EngineOnly) (WorkflowHandle IO EngineOnly)) of
      Left err -> fail (show err)
      Right h -> pure h
    running <- newEmptyMVar
    _ <- forkIO (handleResult handle >>= putMVar running)
    _ <- timeout deadlineMicros (takeMVar reachedGate) >>= maybe (fail "the workflow never reached its gate") pure
    cutShort <- timeout 500000 (shutdownWith dbos (draining (secondsDuration 30)))
    case cutShort of
      Nothing -> pure ()
      Just _ -> fail "shutdown returned while a workflow was still running"
    putMVar releaseGate ()
    output <- timeout deadlineMicros (takeMVar running) >>= maybe (fail "the workflow was cut short with the caller's wait") pure
    case output of
      Right (Just stored) -> case decodeWorkflowValue "result" (Just stored) :: Either CodecError Int of
        Right n -> n @?= 7
        Left err -> fail (show err)
      other -> fail ("the held workflow failed: " <> show other)
    -- Without calling shutdown again: the detached teardown must bring
    -- the instance down on its own. The withDBOS bracket's shutdown
    -- then finds nothing installed and returns at once.
    let go attempts = do
          launched <- isLaunched dbos
          if not launched
            then pure ()
            else
              if attempts >= 1200
                then fail "the instance never shut down after its drain"
                else threadDelay 50000 >> go (attempts + 1)
    go (0 :: Int)
