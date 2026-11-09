{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | 'DBOS.Transact.PatchTest' mirrored under IOSim over the in-memory
-- backend: the real 'patchStep'/'deprecatePatchStep' against mem rows.
-- Scenarios and checks are shared; this module owns the sim factory.
module DBOS.Transact.PatchTestSim (tests) where

import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (simTracer)
import DBOS.Prelude
import Data.Text qualified as Text
import DBOS.SystemDB (NewWorkflow (..), Outcome (OutcomeOutput), StepRecord (stepRecordStepName), StepTiming (..), WorkflowId (..), WorkflowIdReusePolicy (..), WorkflowOwner (..), WorkflowRecord (workflowRecordCreatorXid), newWorkflow, patchMarkerName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.IOSim (memConnectionOn, newMemDB, simIdentity)
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
import DBOS.Transact.Patch (PatchEvent (..))
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, (@?=))

-- | One fixture per leaf over a fresh in-memory database: workflow ids are
-- minted per scenario over the same store.
simPatchFixture :: forall s. IOSim s (PatchFixture (IOSim s))
simPatchFixture = do
  mem <- newMemDB
  fresh <- newTVarIO (0 :: Int)
  let -- Patches present the row's own creator token, as an execution built
      -- from the init result would.
      ownerFor widText = do
        fetched <- SystemDB.getWorkflow mem (WorkflowId widText)
        case fetched of
          Right (Just row) -> case row.workflowRecordCreatorXid of
            Just token -> pure (WorkflowOwner {ownerWorkflowId = WorkflowId widText, ownerToken = token})
            Nothing -> error "patch fixture row has no creator token"
          _ -> error "patch fixture built a context for a row that was never initialized"
      readName wid pos expected = do
        found <- SystemDB.checkStep mem wid pos expected >>= either (error . show) pure
        pure ((.stepRecordStepName) <$> found)
      runOutOfOrder ::
        (forall exec. WorkflowCtx exec (IOSim s) -> Text -> IOSim s (PendingStep exec (IOSim s) (Either (Error EngineOnly) Bool))) ->
        WorkflowId ->
        IOSim s PatchOutOfOrderObservation
      runOutOfOrder mkPending wid@(WorkflowId widText) = do
        conn <- memConnectionOn mem simTracer
        owner <- ownerFor widText
        outcome <- withWorkflow conn simIdentity wid Nothing owner $ \wctx -> do
          pending <- mkPending wctx "v2"
          stepped <- runStep wctx "step_two" $ \_ -> pure (2 :: Int)
          case stepped of
            Left err -> pure (Left err)
            Right _ -> pending.pendingRun
        first <- SystemDB.checkStep mem wid 0 "step_two" >>= either (error . show) pure
        second <- SystemDB.checkStep mem wid 1 (patchMarkerName "v2") >>= either (error . show) pure
        pure (PatchOutOfOrderObservation widText outcome ((.stepRecordStepName) <$> first) ((.stepRecordStepName) <$> second))
  pure
    PatchFixture
      { pfFreshWorkflowId = do
          n <- atomically (readTVar fresh >>= \k -> writeTVar fresh (k + 1) >> pure k)
          let widText = "sim-patch-" <> Text.pack (show n)
          created <- SystemDB.initWorkflow mem ((newWorkflow widText) {newWorkflowName = Just "SimPatchTest"}) Nothing WorkflowIdReturnExisting
          case created of
            Left err -> error (show err)
            Right _ -> pure (WorkflowId widText),
        pfPatchInStep = \wid@(WorkflowId widText) -> do
          conn <- memConnectionOn mem simTracer
          owner <- ownerFor widText
          withWorkflow conn simIdentity wid Nothing owner $ \wctx ->
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
          conn <- memConnectionOn mem simTracer
          owner <- ownerFor widText
          outcome <- withWorkflow conn simIdentity wid Nothing owner twoBody
          found <-
            traverse
              (\(pos, expected) -> readName wid pos expected)
              [(0, patchMarkerName "first"), (1, patchMarkerName "second"), (2, "step_two")]
          pure (outcome, [name | Just name <- found]),
        pfRunLast = \wid@(WorkflowId widText) -> do
          conn <- memConnectionOn mem simTracer
          owner <- ownerFor widText
          outcome <- withWorkflow conn simIdentity wid Nothing owner lastBody
          found <-
            traverse
              (\(pos, expected) -> readName wid pos expected)
              [(0, "step_one"), (1, patchMarkerName "tail")]
          pure (outcome, [name | Just name <- found]),
        pfPlant = \wid@(WorkflowId widText) pos name output -> do
          owner <- ownerFor widText
          token <- case owner of WorkflowOwner {ownerToken = token} -> pure token
          now <- SystemDB.timestampNow
          SystemDB.recordStep mem wid pos name (OutcomeOutput output) Nothing (Just (StepTiming {stepTimingStartedAt = now, stepTimingCompletedAt = now})) token
            >>= either (error . show) pure,
        pfRunBody = \body wid@(WorkflowId widText) -> do
          conn <- memConnectionOn mem simTracer
          owner <- ownerFor widText
          withWorkflow conn simIdentity wid Nothing owner body,
        pfReadNames = \wid pairs -> do
          traverse (\(pos, expected) -> readName wid pos expected) pairs,
        pfReadStep = \wid pos name -> do
          SystemDB.checkStep mem wid pos name >>= either (error . show) pure
      }
tests :: TestTree
tests =
  testGroup
    "Durable patches (Sim)"
    [ simCase simPatchFixture "a patch inside a step is refused" scenarioPatchInStep checkPatchInStep traceNoPatchEvents,
      simCase simPatchFixture "a patch whose position another call took is refused" scenarioOutOfOrderPatch checkOutOfOrder traceNoPatchEvents,
      simCase simPatchFixture "a deprecated patch whose position another call took is refused" scenarioOutOfOrderDeprecate checkOutOfOrder traceNoPatchEvents,
      simCase simPatchFixture "two patches in a row record a marker each" scenarioTwoPatches checkTwoPatches traceTwoMarkers,
      simCase simPatchFixture "a patch as the last call replays" scenarioLastCall checkLastCall traceLastMarker,
      simCase simPatchFixture "code that no longer knows a marker finds an unexpected step" scenarioOldMeetsMarker checkOldMeetsMarker traceNoPatchEvents,
      simCase simPatchFixture "an execution recovered under patched code takes the old path" scenarioRecoveredTakesOldPath checkRecoveredTakesOldPath traceRecovered,
      simCase simPatchFixture "a patch in a gap below a recorded step takes the old path" scenarioPlantedGap checkPlantedGap tracePlantedGap,
      simCase simPatchFixture "a deprecated patch meeting old code finds an unexpected step" scenarioDeprecatedOnOldHistory checkDeprecatedOnOldHistory traceNoPatchEvents,
      simCase simPatchFixture "a patch is added nested deprecated and removed" scenarioProgression checkProgression traceProgression
    ]

-- | Twins that write no marker announce no patch event at all: refusals
-- take no id, out-of-order races write nothing, and the steps still say
-- their own lines.
traceNoPatchEvents :: forall x. SimTrace x -> IO ()
traceNoPatchEvents tr = selectTraceEventsDynamic tr @?= ([] :: [PatchEvent])

-- | Marker writes announce themselves; the replay writes nothing new.
-- The fresh run records both markers, the replay replays both.
traceTwoMarkers :: forall x. SimTrace x -> IO ()
traceTwoMarkers tr =
  selectTraceEventsDynamic tr
    @?= [ PatchMarkerRecorded 0 "first",
          PatchMarkerRecorded 1 "second",
          PatchMarkerReplayed 0 "first",
          PatchMarkerReplayed 1 "second"
        ]

-- | The last call's marker records once and replays once.
traceLastMarker :: forall x. SimTrace x -> IO ()
traceLastMarker tr =
  selectTraceEventsDynamic tr
    @?= [PatchMarkerRecorded 1 "tail", PatchMarkerReplayed 1 "tail"]

-- | The recovered run takes the old path without recording: the planted
-- history replays, nothing new lands.
traceRecovered :: forall x. SimTrace x -> IO ()
traceRecovered tr =
  assertBool "the old path records no marker" (null [() | PatchMarkerRecorded {} <- selectTraceEventsDynamic tr :: [PatchEvent]])

-- | The gap takes the old path with its own announcement: the marker is
-- never recorded, and the step past the gap stands.
tracePlantedGap :: forall x. SimTrace x -> IO ()
tracePlantedGap tr =
  selectTraceEventsDynamic tr @?= [PatchGapOldPathTaken 0 "v2"]

-- | Markers record once each and replay without rewriting: the v1
-- history takes the old path, v2 records and replays, v3 records, the
-- planted v2 history declines v3 then replays v2, and the deprecation and
-- removal write nothing.
traceProgression :: forall x. SimTrace x -> IO ()
traceProgression tr =
  selectTraceEventsDynamic tr
    @?= [ PatchOldPathTaken 0 "v2",
          PatchMarkerRecorded 0 "v2",
          PatchMarkerReplayed 0 "v2",
          PatchMarkerRecorded 0 "v3",
          PatchOldPathTaken 0 "v3",
          PatchMarkerReplayed 0 "v2"
        ]
