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
import DBOS.SystemDB (NewWorkflow (..), Outcome (OutcomeOutput), StepRecord (stepRecordStepName), StepTiming (..), WorkflowId (..), WorkflowIdReusePolicy (..), WorkflowOwner (..), newWorkflow, patchMarkerName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  (
    deprecatePatchStep,
    patchStep,
    pendingDeprecatePatch,
    pendingPatch,
    runStep,
  )
import DBOS.Transact.Checkpoint (PendingStep (..))
import DBOS.Transact.Context (WorkflowCtx, withWorkflow)
import DBOS.Transact.Error (EngineOnly, Error (..))
import DBOS.Transact.Logger (acquireLoggerBackend, ioTracer, nullTracer)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.ContextTest (connOver)
import Test.Tasty (TestTree, testGroup, withResource)

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
            leaf "a deprecated patch meeting old code finds an unexpected step" scenarioDeprecatedOnOldHistory checkDeprecatedOnOldHistory,
            leaf "a patch is added nested deprecated and removed" scenarioProgression checkProgression
          ]

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
