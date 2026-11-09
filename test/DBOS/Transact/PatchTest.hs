{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | Durable code patches, mirroring Rust @tests/patch.rs@: refusals,
-- races, markers, replays, old paths, and the version progression.
-- Scenarios and checks are shared with the sim tree
-- ('DBOS.Transact.PatchTestSim'); this module owns the Postgres factory.
module DBOS.Transact.PatchTest (tests) where

import DBOS.DualStack (liveCaseWith)
import DBOS.Prelude
import DBOS.Transact.PatchCases
  ( PatchFixture (..),
    PatchLastObservation (..),
    PatchOutOfOrderObservation (..),
    PatchTwoObservation (..),
    checkDeprecatedOnOldHistory,
    checkLastCall,
    checkOldMeetsMarker,
    checkOutOfOrder,
    checkPatchInStep,
    checkProgression,
    checkRecoveredTakesOldPath,
    checkPlantedGap,
    checkTwoPatches,
    deprecatedBody,
    lastBody,
    oldBody,
    scenarioDeprecatedOnOldHistory,
    scenarioLastCall,
    scenarioOldMeetsMarker,
    scenarioOutOfOrderDeprecate,
    scenarioOutOfOrderPatch,
    scenarioPatchInStep,
    scenarioProgression,
    scenarioRecoveredTakesOldPath,
    scenarioPlantedGap,
    scenarioTwoPatches,
    twoBody,
    v1Body,
    v2Body,
    v3Body,
    v4Body,
    v5Body,
  )
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (AwaitedOutcome (..), NewWorkflow (..), Outcome (OutcomeOutput), Serialization (..), SerializedWorkflowValue (..), StepRecord (stepRecordStepName), StepTiming (..), WorkflowId (..), WorkflowIdReusePolicy (..), WorkflowOwner (..), getEventStepName, newWorkflow, patchMarkerName, recvStepName, secondsDuration, sleepStepName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  (
    Config (..),
    configFromEnv,
    deprecatePatchStep,
    getEvent,
    launchWithEnvironment,
    newDBOS,
    newQualifiedWorkflowName,
    patchStep,
    pendingDeprecatePatch,
    pendingPatch,
    recv,
    registerWorkflowRef,
    runStep,
    sendWorkflowMessage,
    setEvent,
    shutdown,
    StartOptions (..),
    startOptionsDefault,
    startWorkflow,
    waitForWorkflow,
  )
import DBOS.Transact.Checkpoint (PendingStep (..))
import DBOS.Transact.Context (WorkflowCtx, withWorkflow)
import DBOS.Transact.Error (EngineOnly, Error (..))
import DBOS.Transact.Logger (acquireLoggerBackend, ioTracer, nullTracer)
import DBOS.Transact.Identity (Environment (..), Identity (..))
import DBOS.Transact.Serialization (CodecError (..), WorkflowInput, decodeWorkflowValue, encodeWorkflowInput, encodeWorkflowValue)
import DBOS.Transact.ContextTest (connOver)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (testCase)

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    let leaf :: String -> (PatchFixture IO -> IO a) -> (a -> Either String ()) -> TestTree
        leaf = liveCaseWith (\run -> getBackend >>= \backend -> mkPatchFixture backend run)
     in testGroup
          "Durable patches"
          [ leaf "a patch inside a step is refused" scenarioPatchInStep checkPatchInStep,
            leaf "a patch whose position another call took is refused" scenarioOutOfOrderPatch checkOutOfOrder,
            leaf "a deprecated patch whose position another call took is refused" scenarioOutOfOrderDeprecate checkOutOfOrder,
            leaf "two patches in a row record a marker each" scenarioTwoPatches checkTwoPatches,
            leaf "a patch as the last call replays" scenarioLastCall checkLastCall,
            leaf "code that no longer knows a marker finds an unexpected step" scenarioOldMeetsMarker checkOldMeetsMarker,
            leaf "an execution recovered under patched code takes the old path" scenarioRecoveredTakesOldPath checkRecoveredTakesOldPath,
            leaf "a patch in a gap below a recorded step takes the old path" scenarioPlantedGap checkPlantedGap,
            leaf "a deprecated patch meeting old code finds an unexpected step" scenarioDeprecatedOnOldHistory checkDeprecatedOnOldHistory,
            leaf "a patch is added nested deprecated and removed" scenarioProgression checkProgression,
            -- IO only: abandoning mid-wait leaves a gap the patched
            -- recovery must read as the old code's. Crash-and-relaunch
            -- cannot run on the sim (no supervisor to abandon or recover
            -- through), as the existing recovery test notes.
            testCase "a patch before a waiting receive takes the old path on recovery" $ gapRecoveryCase getBackend GapRecv,
            testCase "a patch before a waiting get_event takes the old path on recovery" $ gapRecoveryCase getBackend GapGetEvent
          ]

-- | Which blocking wait the old code abandons.
data GapWait = GapRecv | GapGetEvent

-- | A receive (or event read) with a timeout records its timeout's sleep,
-- at the position after its own, before it waits, so an execution
-- abandoned while waiting has nothing at the wait's position and a step
-- past it. A patch added before the wait must take the old path on
-- recovery rather than record its marker in the gap, which would move the
-- wait onto its own timeout's position. Mirrors the oracle's gap recovery
-- tests: abandon the old code mid-wait, recover under patched code, finish
-- with a send (or a publish), then run fresh and take the patch.
gapRecoveryCase :: IO Postgres.PostgresSystemDB -> GapWait -> IO ()
gapRecoveryCase getBackend wait = do
  fresh <- UUID.V4.nextRandom
  let suffix = Text.pack (UUID.toString fresh)
      appName = "hs-l2-patch-gap-" <> Text.take 12 suffix
      key = newQualifiedWorkflowName "gap-workflow"
      pubKey = newQualifiedWorkflowName "gap-publisher"
      wid = WorkflowId ("hs-l2-patch-gap-run-" <> suffix)
      newWid = WorkflowId ("hs-l2-patch-gap-new-" <> suffix)
      pubWid = WorkflowId ("hs-l2-patch-gap-publisher-" <> suffix)
      oldBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
      oldBody () wctx = do
        a <- runStep wctx "step_one" (const (pure (1 :: Int)))
        case a of
          Left err -> pure (Left err)
          Right n -> waitBody wctx n
      patchedBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
      patchedBody () wctx = do
        a <- runStep wctx "step_one" (const (pure (1 :: Int)))
        case a of
          Left err -> pure (Left err)
          Right n -> do
            patched <- patchStep wctx "v2"
            case patched of
              Left err -> pure (Left err)
              Right isNew -> do
                waited <- waitBody wctx n
                case waited of
                  Left err -> pure (Left err)
                  Right total -> pure (Right (if isNew then total + 100 else total))
      waitBody :: forall exec. WorkflowCtx exec IO -> Int -> IO (Either (Error EngineOnly) Int)
      waitBody wctx n = case wait of
        GapRecv -> do
          b <- recv wctx Nothing (secondsDuration 10) :: IO (Either (Error EngineOnly) (Maybe Int))
          case b of
            Left err -> pure (Left err)
            Right maybeB -> pure (Right (n + fromMaybe 0 maybeB))
        GapGetEvent -> do
          b <- getEvent wctx pubWid "b" (secondsDuration 10) :: IO (Either (Error EngineOnly) (Maybe Int))
          case b of
            Left err -> pure (Left err)
            Right maybeB -> pure (Right (n + fromMaybe 0 maybeB))
      pubBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
      pubBody () wctx = do
        published <- setEvent wctx "b" (2 :: Int)
        case published of
          Left err -> pure (Left err)
          Right () -> pure (Right ())
  config0 <- configFromEnv appName
  -- One executor identity across both instances: the recovery sweep
  -- re-enqueues rows stamped with the launching executor's own id, so the
  -- patched instance only recovers the abandoned run as its own, as the
  -- oracle's default executor id gives both of its objects.
  let executorId = "exec-gap-" <> suffix
      configA = config0 {configAppVersion = Just ("v1-" <> suffix), configExecutorId = Just executorId}
      configB = config0 {configAppVersion = Just ("v1-" <> suffix), configExecutorId = Just executorId}
  -- The old code, abandoned while its wait blocks: start it, wait until
  -- the timeout sleep lands past the gap, then shut down mid-wait.
  bracket (newDBOS configA) shutdown $ \dbosA -> do
    refA <- registerWorkflowRef dbosA key oldBody >>= either (fail . show) pure
    execA <- launchWithEnvironment dbosA patchIsolatedEnvironment >>= either (fail . show) pure
    _ <- startWorkflow execA refA (startOptionsDefault {startWorkflowId = Just wid}) (Just (encodeWorkflowInput ()))
    awaitStep getBackend wid 2 sleepStepName
    shutdown dbosA
  -- The patched code recovers it: the gap reads as the old code's, so no
  -- marker lands and the wait re-runs in the gap.
  bracket (newDBOS configB) shutdown $ \dbosB -> do
    refB <- registerWorkflowRef dbosB key patchedBody >>= either (fail . show) pure
    pubRef <- case wait of
      GapRecv -> pure Nothing
      GapGetEvent -> Just <$> (registerWorkflowRef dbosB pubKey pubBody >>= either (fail . show) pure)
    execB <- launchWithEnvironment dbosB patchIsolatedEnvironment >>= either (fail . show) pure
    case (wait, pubRef) of
      (GapRecv, _) -> do
        sent <- sendWorkflowMessage dbosB wid Nothing Nothing (encodeWorkflowValue (2 :: Int))
        case sent of
          Left err -> fail (show err)
          Right () -> pure ()
      (GapGetEvent, Just publisher) -> do
        _ <- startWorkflow execB publisher (startOptionsDefault {startWorkflowId = Just pubWid}) (Just (encodeWorkflowInput ()))
        published <- timeout 15000000 (waitForWorkflow dbosB pubWid)
        case published of
          Just (Right _) -> pure ()
          other -> fail ("expected the publisher to finish, got: " <> show other)
      (GapGetEvent, Nothing) -> fail "the publisher was never registered"
    settled <- timeout 30000000 (waitForWorkflow dbosB wid)
    case settled of
      Just (Right (AwaitedSucceeded (Just output) serialization)) -> do
        let decoded = decodeWorkflowValue "result" (Just (SerializedWorkflowValue output (Serialization <$> serialization))) :: Either CodecError Int
        assertEqual "the recovery takes the old path" (Right 3) decoded
      other -> fail ("expected the recovered run to finish, got: " <> show other)
    names <- stepNames getBackend wid
    assertEqual "no marker: the old code had got past the patch's position" ["step_one", waitStepName, sleepStepName] names
    -- A new execution takes the patch, recording its marker before the wait.
    -- A fresh message feeds a fresh receive: the recovered run consumed
    -- its own, and the standing publication feeds fresh reads. Either way
    -- the run starts before it is awaited.
    case wait of
      GapRecv -> do
        _ <- startWorkflow execB refB (startOptionsDefault {startWorkflowId = Just newWid}) (Just (encodeWorkflowInput ()))
        sendFirst <- sendWorkflowMessage dbosB newWid Nothing Nothing (encodeWorkflowValue (2 :: Int))
        case sendFirst of
          Left err -> fail (show err)
          Right () -> pure ()
      GapGetEvent -> do
        _ <- startWorkflow execB refB (startOptionsDefault {startWorkflowId = Just newWid}) (Just (encodeWorkflowInput ()))
        pure ()
    newSettled <- timeout 30000000 (waitForWorkflow dbosB newWid)
    case newSettled of
      Just (Right (AwaitedSucceeded (Just output) serialization)) -> do
        let decoded = decodeWorkflowValue "result" (Just (SerializedWorkflowValue output (Serialization <$> serialization))) :: Either CodecError Int
        assertEqual "the new execution takes the new path" (Right 103) decoded
      other -> fail ("expected the new run to finish, got: " <> show other)
    newNames <- stepNames getBackend newWid
    assertEqual "the marker lands before the wait" ["step_one", patchMarkerName "v2", waitStepName, sleepStepName] newNames
  where
    waitStepName = case wait of
      GapRecv -> recvStepName
      GapGetEvent -> getEventStepName
    assertEqual :: (Eq a, Show a) => String -> a -> a -> IO ()
    assertEqual message expected actual
      | expected == actual = pure ()
      | otherwise = fail (message <> ": expected " <> show expected <> ", got " <> show actual)

-- | Polls until the timeout sleep lands past the gap: the abandoned wait
-- recorded it before blocking, so the position below is a gap.
awaitStep :: IO Postgres.PostgresSystemDB -> WorkflowId -> Int -> Text -> IO ()
awaitStep getBackend wid stepId stepName = go (200 :: Int)
  where
    go 0 = fail "the wait never recorded its timeout step"
    go n = do
      backend <- getBackend
      found <- SystemDB.checkStep backend wid stepId stepName
      case found of
        Right (Just _) -> pure ()
        _ -> threadDelay 50000 >> go (n - 1)

-- | A workflow's recorded step names in position order.
stepNames :: IO Postgres.PostgresSystemDB -> WorkflowId -> IO [Text]
stepNames getBackend wid = do
  backend <- getBackend
  found <- SystemDB.listSteps backend wid False Nothing Nothing Nothing
  case found of
    Left err -> fail (show err)
    Right records -> pure (map (.stepRecordStepName) records)

-- | This test's environment: nothing claimed from the process.
patchIsolatedEnvironment :: Environment
patchIsolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }

-- | One fixture per leaf over the suite backend: a fresh workflow id per
-- scenario, every run announcing through the leaf's FastLogger backend.
mkPatchFixture :: Postgres.PostgresSystemDB -> (PatchFixture IO -> IO a) -> IO a
mkPatchFixture backend run = do
  (logger, cleanup) <- acquireLoggerBackend
  let -- Patches present the row's own creator token, as an execution built
      -- from the init result would.
      ownerFor workflowText = do
        fetched <- SystemDB.getWorkflow backend (WorkflowId workflowText)
        case fetched of
          Right (Just row) -> case row.workflowRecordCreatorXid of
            Just token -> pure (WorkflowOwner {ownerWorkflowId = WorkflowId workflowText, ownerToken = token})
            Nothing -> fail "patch fixture row has no creator token"
          _ -> fail "patch fixture built a context for a row that was never initialized"
      readName wid pos expected = do
        found <- SystemDB.checkStep backend wid pos expected >>= either (fail . show) pure
        pure ((.stepRecordStepName) <$> found)
      runOutOfOrder ::
        (forall exec. WorkflowCtx exec IO -> Text -> IO (PendingStep exec IO (Either (Error EngineOnly) Bool))) ->
        WorkflowId ->
        IO PatchOutOfOrderObservation
      runOutOfOrder mkPending wid@(WorkflowId widText) = do
        conn <- connOver backend (ioTracer logger)
        owner <- ownerFor widText
        outcome <- withWorkflow conn patchTestIdentity wid Nothing owner $ \wctx -> do
          pending <- mkPending wctx "v2"
          stepped <- runStep wctx "step_two" $ \_ -> pure (2 :: Int)
          case stepped of
            Left err -> pure (Left err)
            Right _ -> pending.pendingRun
        first <- SystemDB.checkStep backend wid 0 "step_two" >>= either (fail . show) pure
        second <- SystemDB.checkStep backend wid 1 (patchMarkerName "v2") >>= either (fail . show) pure
        pure (PatchOutOfOrderObservation widText outcome ((.stepRecordStepName) <$> first) ((.stepRecordStepName) <$> second))
      fixture =
        PatchFixture
          { pfFreshWorkflowId = do
              freshId <- UUID.V4.nextRandom
              let workflowText = "hs-l2-patch-" <> Text.pack (UUID.toString freshId)
              created <- SystemDB.initWorkflow backend ((newWorkflow workflowText) {newWorkflowName = Just "L2PatchTest"}) Nothing WorkflowIdReturnExisting
              case created of
                Left err -> fail (show err)
                Right _ -> pure (WorkflowId workflowText),
            pfPatchInStep = \wid@(WorkflowId widText) -> do
              conn <- connOver backend (ioTracer logger)
              owner <- ownerFor widText
              withWorkflow conn patchTestIdentity wid Nothing owner $ \wctx ->
                runStep wctx "probe" $ \_ -> do
                  patched <- patchStep wctx "v2"
                  deprecated <- deprecatePatchStep wctx "v2"
                  pure
                    ( patched == Left (InsideStep {operation = "patch"})
                        && deprecated == Left (InsideStep {operation = "deprecate_patch"})
                    ),
            pfOutOfOrderPatch = runOutOfOrder pendingPatch,
            pfOutOfOrderDeprecate = runOutOfOrder pendingDeprecatePatch,
            pfRunTwo = \wid@(WorkflowId widText) -> do
              conn <- connOver backend (ioTracer logger)
              owner <- ownerFor widText
              outcome <- withWorkflow conn patchTestIdentity wid Nothing owner twoBody
              found <-
                traverse
                  (\(pos, expected) -> readName wid pos expected)
                  [(0, patchMarkerName "first"), (1, patchMarkerName "second"), (2, "step_two")]
              pure (outcome, [name | Just name <- found]),
            pfRunLast = \wid@(WorkflowId widText) -> do
              conn <- connOver backend (ioTracer logger)
              owner <- ownerFor widText
              outcome <- withWorkflow conn patchTestIdentity wid Nothing owner lastBody
              found <-
                traverse
                  (\(pos, expected) -> readName wid pos expected)
                  [(0, "step_one"), (1, patchMarkerName "tail")]
              pure (outcome, [name | Just name <- found]),
            pfPlant = \wid@(WorkflowId widText) pos name output -> do
              owner <- ownerFor widText
              token <- case owner of WorkflowOwner {ownerToken = token} -> pure token
              now <- SystemDB.timestampNow
              SystemDB.recordStep backend wid pos name (OutcomeOutput output) Nothing (Just (StepTiming {stepTimingStartedAt = now, stepTimingCompletedAt = now})) token
                >>= either (fail . show) pure,
            pfRunBody = \body wid@(WorkflowId widText) -> do
              conn <- connOver backend (ioTracer logger)
              owner <- ownerFor widText
              withWorkflow conn patchTestIdentity wid Nothing owner body,
            pfReadNames = \wid pairs -> do
              traverse (\(pos, expected) -> readName wid pos expected) pairs,
            pfReadStep = \wid pos name -> do
              SystemDB.checkStep backend wid pos name >>= either (fail . show) pure
          }
  run fixture `finally` cleanup

-- | The application identity the scoped patch cases install.
patchTestIdentity :: Identity
patchTestIdentity =
  Identity
    { identityAppName = "test-app",
      identityAppVersion = "1.0.0",
      identityExecutorId = "patch-test-executor",
      identityAppId = "patch-test-app"
    }
