{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Shared patch scenarios: one body per case, judged by one pure check on
-- each stack. The fixture carries every stack-specific operation (workflow
-- creation, running a patch), so the scenarios below are plain compositions
-- with no effect constraints of their own — the live tree runs them over
-- Postgres, the sim tree over the in-memory backend, and both prove the
-- same record.
module DBOS.Transact.PatchCases
  ( PatchFixture (..),
    PatchOutOfOrderObservation (..),
    PatchTwoObservation (..),
    PatchLastObservation (..),
    scenarioPatchInStep,
    checkPatchInStep,
    scenarioOutOfOrderPatch,
    scenarioOutOfOrderDeprecate,
    checkOutOfOrder,
    scenarioTwoPatches,
    checkTwoPatches,
    scenarioLastCall,
    checkLastCall,
    scenarioOldMeetsMarker,
    checkOldMeetsMarker,
    scenarioRecoveredTakesOldPath,
    scenarioPlantedGap,
    checkRecoveredTakesOldPath,
    checkPlantedGap,
    scenarioDeprecatedOnOldHistory,
    checkDeprecatedOnOldHistory,
    scenarioProgression,
    checkProgression,
    twoBody,
    lastBody,
    oldBody,
    v1Body,
    v2Body,
    v3Body,
    v4Body,
    v5Body,
    deprecatedBody,
  )
where

import DBOS.Internal.Prelude
import DBOS.SystemDB (StepRecord (..), WorkflowId, patchMarkerName)
import DBOS.Internal.SystemDB.Error qualified as SystemDBError
import DBOS.Transact (EngineOnly, Error (..), deprecatePatchStep, patchStep, runStep)
import DBOS.Internal.Transact.Context (WorkflowCtx)

-- | What a stack must provide to run the shared patch scenarios: fresh
-- workflow ids, the patch-inside-a-step probe, the out-of-order probe in
-- both marker modes, and single runs of the two-patch and last-call bodies
-- with the names on their rows.
data PatchFixture m = PatchFixture
  { pfFreshWorkflowId :: m WorkflowId,
    pfPatchInStep :: WorkflowId -> m (Either (Error EngineOnly) Bool),
    pfOutOfOrderPatch :: WorkflowId -> m PatchOutOfOrderObservation,
    pfOutOfOrderDeprecate :: WorkflowId -> m PatchOutOfOrderObservation,
    pfRunTwo :: WorkflowId -> m (Either (Error EngineOnly) (Bool, Bool, Int), [Text]),
    pfRunLast :: WorkflowId -> m (Either (Error EngineOnly) Int, [Text]),
    pfPlant :: WorkflowId -> Int -> Text -> Maybe Text -> m (),
    pfRunBody :: forall result. (forall exec. WorkflowCtx exec m -> m (Either (Error EngineOnly) result)) -> WorkflowId -> m (Either (Error EngineOnly) result),
    pfReadNames :: WorkflowId -> [(Int, Text)] -> m [Maybe Text],
    pfReadStep :: WorkflowId -> Int -> Text -> m (Maybe StepRecord)
  }

-- | What a raced patch left behind: the workflow it ran in, the refusal,
-- and the step names at positions zero and one. Positions are dense from
-- zero, so the two reads are exhaustive: the step at zero, no marker
-- anywhere.
data PatchOutOfOrderObservation = PatchOutOfOrderObservation
  { pooWorkflow :: Text,
    pooOutcome :: Either (Error EngineOnly) Bool,
    pooFirst :: Maybe Text,
    pooSecond :: Maybe Text
  }

-- | Both patch calls refuse inside a step body, naming their own operation.
-- Mirrors Rust @a_patch_inside_a_step_is_refused@.
scenarioPatchInStep :: Monad m => PatchFixture m -> m (Either (Error EngineOnly) Bool)
scenarioPatchInStep fx = do
  wid <- fx.pfFreshWorkflowId
  fx.pfPatchInStep wid

-- | The probe workflow succeeds and both calls were refused as in-step.
checkPatchInStep :: Either (Error EngineOnly) Bool -> Either String ()
checkPatchInStep result = case result of
  Right True -> Right ()
  Right False -> Left "both patch calls inside a step must be refused with their own operation"
  Left err -> Left ("the probe workflow must succeed, got: " <> show err)

-- | A patch built before a step takes its position loses it: the step
-- records, the marker never lands over it. Mirrors Rust
-- @a_patch_whose_position_another_call_took_is_refused@.
scenarioOutOfOrderPatch :: Monad m => PatchFixture m -> m PatchOutOfOrderObservation
scenarioOutOfOrderPatch fx = do
  wid <- fx.pfFreshWorkflowId
  fx.pfOutOfOrderPatch wid

-- | The same race in deprecation mode: nothing would record anyway, but the
-- moved position still refuses. Mirrors Rust
-- @a_deprecated_patch_whose_position_another_call_took_is_refused@.
scenarioOutOfOrderDeprecate :: Monad m => PatchFixture m -> m PatchOutOfOrderObservation
scenarioOutOfOrderDeprecate fx = do
  wid <- fx.pfFreshWorkflowId
  fx.pfOutOfOrderDeprecate wid

-- | The refusal names the workflow, the patch, and the lost position zero;
-- the step kept position zero and no marker row exists at one.
checkOutOfOrder :: PatchOutOfOrderObservation -> Either String ()
checkOutOfOrder obs = do
  case obs.pooOutcome of
    Left (PatchNondeterminism {workflowId, patch, stepId})
      | workflowId == obs.pooWorkflow
      , patch == "v2"
      , stepId == 0 -> pure ()
    Left other -> Left ("expected the refusal to name the workflow, patch v2, and position 0, got: " <> show other)
    Right _ -> Left "expected the patch to be refused"
  unless (obs.pooFirst == Just "step_two") $
    Left ("expected the step to keep position zero, got: " <> show obs.pooFirst)
  unless (obs.pooSecond == Nothing) $
    Left ("expected no marker row at position one, got: " <> show obs.pooSecond)

-- | The two-patch body: two markers, then the step. New executions take
-- both new paths.
twoBody :: (MonadSTM m, MonadTime m, MonadCatch m) => WorkflowCtx exec m -> m (Either (Error EngineOnly) (Bool, Bool, Int))
twoBody wctx = do
  a <- patchStep wctx "first"
  b <- patchStep wctx "second"
  case (a, b) of
    (Right pa, Right pb) -> do
      stepped <- runStep wctx "step_two" $ \_ -> pure (2 :: Int)
      case stepped of
        Right n -> pure (Right (pa, pb, n))
        Left err -> pure (Left err)
    (Left err, _) -> pure (Left err)
    (_, Left err) -> pure (Left err)

-- | The last-call body: a step, then the patch at the end.
lastBody :: (MonadSTM m, MonadTime m, MonadCatch m) => WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
lastBody wctx = do
  a <- runStep wctx "step_one" $ \_ -> pure (1 :: Int)
  case a of
    Left err -> pure (Left err)
    Right n -> do
      patched <- patchStep wctx "tail"
      case patched of
        Right True -> pure (Right (n + 10))
        Right False -> pure (Right n)
        Left err -> pure (Left err)

-- | What two runs of each body left: both outcomes, both name lists, and
-- the fresh marker row for the shape check.
data PatchTwoObservation = PatchTwoObservation
  { ptoFirst :: Either (Error EngineOnly) (Bool, Bool, Int),
    ptoFirstNames :: [Text],
    ptoSecond :: Either (Error EngineOnly) (Bool, Bool, Int),
    ptoSecondNames :: [Text],
    ptoMarker :: Maybe StepRecord
  }

data PatchLastObservation = PatchLastObservation
  { ploFirst :: Either (Error EngineOnly) Int,
    ploFirstNames :: [Text],
    ploSecond :: Either (Error EngineOnly) Int,
    ploSecondNames :: [Text]
  }

-- | Two patches in a row record a marker each, and the replay honours both
-- markers and records the step where it was. Mirrors Rust
-- @two_patches_in_a_row_record_a_marker_each@ without its fork tail: both
-- backends copy step rows name-blind, so replay adoption here plus the
-- fork suites cover it.
scenarioTwoPatches :: Monad m => PatchFixture m -> m PatchTwoObservation
scenarioTwoPatches fx = do
  wid <- fx.pfFreshWorkflowId
  (first, firstNames) <- fx.pfRunTwo wid
  (second, secondNames) <- fx.pfRunTwo wid
  marker <- fx.pfReadStep wid 0 (patchMarkerName "first")
  pure (PatchTwoObservation first firstNames second secondNames marker)

-- | Fresh and replayed runs agree, and both rows hold the two markers
-- followed by the step.
checkTwoPatches :: PatchTwoObservation -> Either String ()
checkTwoPatches obs = do
  let expected = [patchMarkerName "first", patchMarkerName "second", "step_two"]
  case obs.ptoFirst of
    Right (True, True, 2) -> pure ()
    other -> Left ("expected the fresh run to take both new paths, got: " <> show other)
  unless (obs.ptoFirstNames == expected) $
    Left ("expected two markers and the step, got: " <> show obs.ptoFirstNames)
  case obs.ptoSecond of
    Right (True, True, 2) -> pure ()
    other -> Left ("expected the replay to take both new paths, got: " <> show other)
  unless (obs.ptoSecondNames == expected) $
    Left ("expected the replay to keep the rows, got: " <> show obs.ptoSecondNames)
  -- The marker is a name and nothing else: no output, no error, no
  -- serialization — but stamped like any checkpoint.
  case obs.ptoMarker of
    Just marker
      | marker.stepRecordOutput == Nothing
      , marker.stepRecordError == Nothing
      , marker.stepRecordSerialization == Nothing
      , marker.stepRecordCompletedAt /= Nothing -> pure ()
      | otherwise -> Left ("expected a bare stamped marker, got: " <> show marker)
    Nothing -> Left "expected the fresh marker row"

-- | A patch as the last call replays: the step, then the marker, twice.
-- Mirrors Rust @a_patch_as_the_last_call_replays@ without its fork tail
-- (same composition argument as above).
scenarioLastCall :: Monad m => PatchFixture m -> m PatchLastObservation
scenarioLastCall fx = do
  wid <- fx.pfFreshWorkflowId
  (first, firstNames) <- fx.pfRunLast wid
  (second, secondNames) <- fx.pfRunLast wid
  pure (PatchLastObservation first firstNames second secondNames)

-- | Both runs answer eleven, and both rows hold the step and the marker.
checkLastCall :: PatchLastObservation -> Either String ()
checkLastCall obs = do
  let expected = ["step_one", patchMarkerName "tail"]
  case obs.ploFirst of
    Right 11 -> pure ()
    other -> Left ("expected eleven the first time, got: " <> show other)
  unless (obs.ploFirstNames == expected) $
    Left ("expected the step and the marker, got: " <> show obs.ploFirstNames)
  case obs.ploSecond of
    Right 11 -> pure ()
    other -> Left ("expected eleven on replay, got: " <> show other)
  unless (obs.ploSecondNames == expected) $
    Left ("expected the replay to keep the rows, got: " <> show obs.ploSecondNames)

-- | The old code: one step, no patch knowledge.
oldBody :: (MonadSTM m, MonadTime m, MonadCatch m) => WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
oldBody wctx = runStep wctx "step_two" $ \_ -> pure (2 :: Int)

-- | The code as first deployed: two steps, no patch.
v1Body :: (MonadSTM m, MonadTime m, MonadCatch m) => WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
v1Body wctx = do
  a <- runStep wctx "step_one" $ \_ -> pure (1 :: Int)
  case a of
    Left err -> pure (Left err)
    Right n -> do
      b <- runStep wctx "step_two" $ \_ -> pure (2 :: Int)
      case b of
        Left err -> pure (Left err)
        Right m -> pure (Right (n + m))

-- | The first step changed, behind a patch.
v2Body :: (MonadSTM m, MonadTime m, MonadCatch m) => WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
v2Body wctx = do
  patched <- patchStep wctx "v2"
  case patched of
    Left err -> pure (Left err)
    Right True -> runStep wctx "step_three" $ \_ -> pure (3 :: Int)
    Right False -> runStep wctx "step_one" $ \_ -> pure (1 :: Int)
  >>= \a -> case a of
    Left err -> pure (Left err)
    Right n -> do
      b <- runStep wctx "step_two" $ \_ -> pure (2 :: Int)
      case b of
        Left err -> pure (Left err)
        Right m -> pure (Right (n + m))

-- | The first step changed again, behind a second patch tested before
-- the first.
v3Body :: (MonadSTM m, MonadTime m, MonadCatch m) => WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
v3Body wctx = do
  first <- patchStep wctx "v3"
  a <- case first of
    Left err -> pure (Left err)
    Right True -> runStep wctx "step_two" $ \_ -> pure (2 :: Int)
    Right False -> do
      second <- patchStep wctx "v2"
      case second of
        Left err -> pure (Left err)
        Right True -> runStep wctx "step_three" $ \_ -> pure (3 :: Int)
        Right False -> runStep wctx "step_one" $ \_ -> pure (1 :: Int)
  case a of
    Left err -> pure (Left err)
    Right n -> do
      b <- runStep wctx "step_two" $ \_ -> pure (2 :: Int)
      case b of
        Left err -> pure (Left err)
        Right m -> pure (Right (n + m))

-- | Only v3's code is left, and its patch is deprecated. Returns whether
-- the deprecation held with the answer, since a silent False would still
-- sum to four with no markers.
v4Body :: (MonadSTM m, MonadTime m, MonadCatch m) => WorkflowCtx exec m -> m (Either (Error EngineOnly) (Bool, Int))
v4Body wctx = do
  deprecated <- deprecatePatchStep wctx "v3"
  case deprecated of
    Left err -> pure (Left err)
    Right held -> do
      a <- runStep wctx "step_two" $ \_ -> pure (2 :: Int)
      case a of
        Left err -> pure (Left err)
        Right n -> do
          b <- runStep wctx "step_two" $ \_ -> pure (2 :: Int)
          case b of
            Left err -> pure (Left err)
            Right m -> pure (Right (held, n + m))

-- | The deprecated patch is gone: two steps, no patches.
v5Body :: (MonadSTM m, MonadTime m, MonadCatch m) => WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
v5Body wctx = do
  a <- runStep wctx "step_two" $ \_ -> pure (2 :: Int)
  case a of
    Left err -> pure (Left err)
    Right n -> do
      b <- runStep wctx "step_two" $ \_ -> pure (2 :: Int)
      case b of
        Left err -> pure (Left err)
        Right m -> pure (Right (n + m))

-- | The deprecated patch meeting old code: honoured past the marker
-- search, then its first step meets the old history.
deprecatedBody :: (MonadSTM m, MonadTime m, MonadCatch m) => WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
deprecatedBody wctx = do
  deprecated <- deprecatePatchStep wctx "v2"
  case deprecated of
    Left err -> pure (Left err)
    Right _ -> do
      a <- runStep wctx "step_three" $ \_ -> pure (3 :: Int)
      case a of
        Left err -> pure (Left err)
        Right n -> do
          b <- runStep wctx "step_two" $ \_ -> pure (2 :: Int)
          case b of
            Left err -> pure (Left err)
            Right m -> pure (Right (n + m))

-- | Code that no longer knows a marker meets the execution that took it:
-- the old step fails against the marker row. Both planted halves in one
-- twin, mirroring the oracle's two halves.
scenarioOldMeetsMarker :: (MonadSTM m, MonadTime m, MonadCatch m) => PatchFixture m -> m (Either (Error EngineOnly) Int, Either (Error EngineOnly) Int)
scenarioOldMeetsMarker fx = do
  let PatchFixture {pfRunBody = runBody} = fx
  firstWid <- fx.pfFreshWorkflowId
  fx.pfPlant firstWid 0 (patchMarkerName "v2") Nothing
  first <- runBody oldBody firstWid
  secondWid <- fx.pfFreshWorkflowId
  fx.pfPlant secondWid 0 (patchMarkerName "v3") Nothing
  second <- runBody oldBody secondWid
  pure (first, second)

-- | Both halves fail with the unexpected-step triple naming the old step
-- and the marker it met.
checkOldMeetsMarker :: (Either (Error EngineOnly) Int, Either (Error EngineOnly) Int) -> Either String ()
checkOldMeetsMarker (first, second) = do
  case first of
    Left (SystemDatabase (SystemDBError.UnexpectedStep {stepId, expected, recorded}))
      | stepId == 0, expected == "step_two", recorded == patchMarkerName "v2" -> pure ()
      | otherwise -> Left ("expected the v2 triple, got: " <> show first)
    other -> Left ("expected an unexpected step for v2, got: " <> show other)
  case second of
    Left (SystemDatabase (SystemDBError.UnexpectedStep {workflowId = _, stepId, expected, recorded}))
      | stepId == 0, expected == "step_two", recorded == patchMarkerName "v3" -> pure ()
      | otherwise -> Left ("expected the v3 triple, got: " <> show second)
    other -> Left ("expected an unexpected step for v3, got: " <> show other)

-- | An execution recovered under patched code takes the old path: v1
-- history planted, v2 body runs the old branch to three. Mirrors Rust
-- @an_execution_recovered_under_patched_code_takes_the_old_path@ without
-- its crash machinery, which the recovery suite covers.
scenarioRecoveredTakesOldPath :: (MonadSTM m, MonadTime m, MonadCatch m) => PatchFixture m -> m (Either (Error EngineOnly) Int, [Maybe Text])
scenarioRecoveredTakesOldPath fx = do
  let PatchFixture {pfRunBody = runBody} = fx
  wid <- fx.pfFreshWorkflowId
  fx.pfPlant wid 0 "step_one" (Just "1")
  outcome <- runBody v2Body wid
  names <- fx.pfReadNames wid [(0, "step_one"), (1, "step_two")]
  pure (outcome, names)

-- | The old path answers three, and the rows hold the old steps.
checkRecoveredTakesOldPath :: (Either (Error EngineOnly) Int, [Maybe Text]) -> Either String ()
checkRecoveredTakesOldPath (outcome, names) = do
  case outcome of
    Right 3 -> pure ()
    other -> Left ("expected the old path to answer three, got: " <> show other)
  unless (names == [Just "step_one", Just "step_two"]) $
    Left ("expected the old steps, got: " <> show names)

-- | A gap below a recorded step is the old code's, however it was left:
-- concurrent calls of one execution can finish out of order, so position
-- 1 can be recorded while 0 is not. A patch at 0 takes the old path
-- there, and records nothing in the gap. Mirrors Rust
-- @a_patch_in_a_gap_below_a_recorded_step_takes_the_old_path@.
scenarioPlantedGap :: forall m. (MonadSTM m, MonadTime m) => PatchFixture m -> m (Either (Error EngineOnly) Int, [Maybe Text])
scenarioPlantedGap fx = do
  let PatchFixture {pfRunBody = runBody} = fx
  wid <- fx.pfFreshWorkflowId
  fx.pfPlant wid 1 "step_two" (Just "2")
  outcome <- runBody gapBody wid
  names <- fx.pfReadNames wid [(0, patchMarkerName "v2"), (1, "step_two")]
  pure (outcome, names)
  where
    gapBody :: forall exec. WorkflowCtx exec m -> m (Either (Error EngineOnly) Int)
    gapBody wctx = do
      patched <- patchStep wctx "v2"
      case patched of
        Left err -> pure (Left err)
        Right True -> pure (Right 1)
        Right False -> pure (Right 0)

-- | The old path answers zero, and no marker lands in the gap while the
-- planted step stands past it.
checkPlantedGap :: (Either (Error EngineOnly) Int, [Maybe Text]) -> Either String ()
checkPlantedGap (outcome, names) = do
  case outcome of
    Right 0 -> pure ()
    other -> Left ("expected the old path to answer zero, got: " <> show other)
  unless (names == [Nothing, Just "step_two"]) $
    Left ("expected no marker in the gap, got: " <> show names)

-- | A deprecated patch meeting old code: honoured past the marker search,
-- then its first step fails against the old history. Mirrors Rust
-- @a_deprecated_patch_meeting_the_old_code_finds_an_unexpected_step@.
scenarioDeprecatedOnOldHistory :: (MonadSTM m, MonadTime m, MonadCatch m) => PatchFixture m -> m (Either (Error EngineOnly) Int)
scenarioDeprecatedOnOldHistory fx = do
  let PatchFixture {pfRunBody = runBody} = fx
  wid <- fx.pfFreshWorkflowId
  fx.pfPlant wid 0 "step_one" (Just "1")
  fx.pfPlant wid 1 "step_two" (Just "2")
  runBody deprecatedBody wid

-- | The failure names position zero, the step it wanted, and the old step.
checkDeprecatedOnOldHistory :: Either (Error EngineOnly) Int -> Either String ()
checkDeprecatedOnOldHistory result = case result of
  Left (SystemDatabase (SystemDBError.UnexpectedStep {stepId, expected, recorded}))
    | stepId == 0, expected == "step_three", recorded == "step_one" -> Right ()
    | otherwise -> Left ("expected the deprecated triple, got: " <> show result)
  other -> Left ("expected an unexpected step, got: " <> show other)

-- | The full progression across versions and replays, without forks: v1
-- runs, v2 replays the old path on v1 history and the new path fresh, v3
-- nests over v2, a planted v2 history replays onto v2's path under v3,
-- v4 deprecates, v5 drops the patch. Mirrors Rust
-- @a_patch_is_added_nested_deprecated_and_removed@ without its fork tails:
-- forks copy step rows name-blind on both backends, so the sequential
-- replays here plus the fork suites cover them.
scenarioProgression :: (MonadSTM m, MonadTime m, MonadCatch m) => PatchFixture m -> m (Int, Int, Int, Int, Int, Int, (Bool, Int), Int)
scenarioProgression fx = do
  let PatchFixture {pfRunBody = runBody} = fx
  v1Wid <- fx.pfFreshWorkflowId
  v1 <- runBody v1Body v1Wid
  v1OldPath <- runBody v2Body v1Wid
  v2Wid <- fx.pfFreshWorkflowId
  v2 <- runBody v2Body v2Wid
  v2Replay <- runBody v2Body v2Wid
  v3Wid <- fx.pfFreshWorkflowId
  v3 <- runBody v3Body v3Wid
  plantedWid <- fx.pfFreshWorkflowId
  fx.pfPlant plantedWid 0 (patchMarkerName "v2") Nothing
  fx.pfPlant plantedWid 1 "step_three" (Just "3")
  fx.pfPlant plantedWid 2 "step_two" (Just "2")
  v2ReplayPlanted <- runBody v3Body plantedWid
  v4Wid <- fx.pfFreshWorkflowId
  v4 <- runBody v4Body v4Wid
  v5Wid <- fx.pfFreshWorkflowId
  v5 <- runBody v5Body v5Wid
  let justRight :: MonadThrow m => Either (Error EngineOnly) result -> m result
      justRight result = case result of
        Right n -> pure n
        Left err -> throwIO (userError ("a progression run must succeed, got: " <> show err))
  v1' <- justRight v1
  v1OldPath' <- justRight v1OldPath
  v2' <- justRight v2
  v2Replay' <- justRight v2Replay
  v3' <- justRight v3
  v2ReplayPlanted' <- justRight v2ReplayPlanted
  (v4Held, v4') <- justRight v4
  v5' <- justRight v5
  pure (v1', v1OldPath', v2', v2Replay', v3', v2ReplayPlanted', (v4Held, v4'), v5')

-- | Three, three, five, five, four, five on the planted v2 history, four.
checkProgression :: (Int, Int, Int, Int, Int, Int, (Bool, Int), Int) -> Either String ()
checkProgression (v1, v1OldPath, v2, v2Replay, v3, v2ReplayPlanted, (v4Held, v4), v5) = do
  unless (v1 == 3) $ Left ("expected v1 to answer three, got: " <> show v1)
  unless (v1OldPath == 3) $ Left ("expected v2 on v1 history to take the old path, got: " <> show v1OldPath)
  unless (v2 == 5) $ Left ("expected v2 to answer five, got: " <> show v2)
  unless (v2Replay == 5) $ Left ("expected the v2 replay to answer five, got: " <> show v2Replay)
  unless (v3 == 4) $ Left ("expected v3 to answer four, got: " <> show v3)
  unless (v2ReplayPlanted == 5) $ Left ("expected v3 on planted v2 history to replay v2's path, got: " <> show v2ReplayPlanted)
  unless (v4Held && v4 == 4) $ Left ("expected the deprecation to hold and answer four, got: " <> show (v4Held, v4))
  unless (v5 == 4) $ Left ("expected v5 to answer four, got: " <> show v5)
