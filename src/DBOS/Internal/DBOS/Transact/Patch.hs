{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Durable code patches, one-to-one with Rust @patch.rs@: changing a
-- workflow's code while executions of the old code are still in flight.
--
-- A replay follows the step history its first execution recorded, position
-- by position, so new code asks for a step where the history may hold a
-- different one. A patch is a recorded fork: an execution reaching it for
-- the first time records a marker (@DBOS.patch-<name>@) at the next step
-- position and takes the new path (@True@); one whose history already holds
-- the marker replays the new path; one whose history holds some other step
-- there was started by the old code, takes the old path (@False@), and
-- records nothing. 'deprecatePatchStep' honours recorded markers without
-- recording new ones, for when no old execution can run again.
--
-- Positions follow the PendingStep shape: building peeks the next step id
-- without taking it ('peekStepId'), and resolving reads the history at the
-- peeked position, then takes the id only if still next ('claimStepId', one
-- compare-and-swap). A call built in between took the position first, and
-- the patch fails with 'PatchNondeterminism' rather than record elsewhere.
-- Placement is checked at drive time ('checkHere'), as for every durable
-- call, so a carried patch is refused with 'StepBuiltElsewhere'.
module DBOS.Transact.Patch
  ( patchStep,
    deprecatePatchStep,
    pendingPatch,
    pendingDeprecatePatch,
    PatchEvent (..),
  )
where

import DBOS.Prelude
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Error qualified as SystemDBError
import DBOS.SystemDB.Types (Outcome (..), StepTiming (..), WorkflowId (..), patchMarkerName, timestampNow)
import DBOS.Transact.Checkpoint (PendingStep (..), StepDurability (..), StepPlacement (..), checkHere)
import DBOS.Transact.Context (WorkflowCtx (wctxTracer), claimStepId, insideAStep, ownerToken, peekStepId, stepCtxBoundary, withSystemDB, workflowId)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Logger (LogEvent (..), LogSeverity (..), runTracer)
import System.Log.FastLogger (ToLogStr (..))

-- | Announcements from the patch paths, homed here with their owner.
-- Mirrors @patch.rs@'s debug sites: a recorded marker, a replayed one, and
-- an old execution taking the old path.
data PatchEvent
  = PatchMarkerRecorded {patchStepId :: Int, patchMarker :: Text}
  | PatchMarkerReplayed {patchStepId :: Int, patchMarker :: Text}
  | PatchOldPathTaken {patchStepId :: Int, patchMarker :: Text}
  deriving stock (Eq, Show)

instance LogEvent PatchEvent where
  eventSeverity _ = SeverityDebug
  renderEvent (PatchMarkerRecorded stepId' marker) =
    "recorded a patch marker step_id=" <> showText stepId' <> " patch=" <> marker
  renderEvent (PatchMarkerReplayed stepId' marker) =
    "replaying a patch marker step_id=" <> showText stepId' <> " patch=" <> marker
  renderEvent (PatchOldPathTaken stepId' marker) =
    "an execution from before the patch: taking the old path step_id=" <> showText stepId' <> " patch=" <> marker

instance ToLogStr PatchEvent where
  toLogStr = toLogStr . renderLine

-- | What a patch point does about its marker. Mirrors Rust @Marker@.
data Marker
  = -- | 'patchStep': record one where the history has nothing yet.
    Write
  | -- | 'deprecatePatchStep': move past one already recorded, never record one.
    Honour
  deriving stock (Eq, Show)

-- | What the history holds at the position a patch point looked at.
-- Mirrors Rust @Found@.
data Found
  = FoundNothing
  | FoundThisMarker
  | FoundOtherStep
  deriving stock (Eq, Show)

-- | A patch built at its position and not yet resolved: the id is peeked at
-- the call so a replay rebuilds the same slot, and the read-claim-write
-- runs when the pending value is awaited or raced.
pendingPatch :: (MonadSTM m, MonadTime m) => WorkflowCtx exec m -> Text -> m (PendingStep exec m (Either (TransactError.Error TransactError.EngineOnly) Bool))
pendingPatch wctx name = pendingPatchWith wctx "patch" name Write

-- | A deprecation built at its position and not yet resolved. Honours
-- recorded markers without recording new ones.
pendingDeprecatePatch :: (MonadSTM m, MonadTime m) => WorkflowCtx exec m -> Text -> m (PendingStep exec m (Either (TransactError.Error TransactError.EngineOnly) Bool))
pendingDeprecatePatch wctx name = pendingPatchWith wctx "deprecate_patch" name Honour

-- | 'pendingPatch' now: build and resolve in one call.
patchStep :: (MonadSTM m, MonadTime m) => WorkflowCtx exec m -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) Bool)
patchStep wctx name = do
  pending <- pendingPatch wctx name
  pending.pendingRun

-- | 'pendingDeprecatePatch' now: build and resolve in one call.
deprecatePatchStep :: (MonadSTM m, MonadTime m) => WorkflowCtx exec m -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) Bool)
deprecatePatchStep wctx name = do
  pending <- pendingDeprecatePatch wctx name
  pending.pendingRun

-- | What building a patch does: refuse a place with no history to look at,
-- and peek the next position without taking it. A build inside a step body
-- never reaches the counter: it carries no position anywhere, and the drive
-- hands back the refusal it stored.
pendingPatchWith :: (MonadSTM m, MonadTime m) => WorkflowCtx exec m -> Text -> Text -> Marker -> m (PendingStep exec m (Either (TransactError.Error TransactError.EngineOnly) Bool))
pendingPatchWith wctx operation name marker = do
  stepped <- insideAStep wctx
  if stepped
    then pure (PendingStep (patchMarkerName name) Nothing (pure (Left (TransactError.InsideStep {operation = operation}))))
    else do
      peeked <- peekStepId wctx
      pure (PendingStep (patchMarkerName name) (Just (Recorded wctx peeked)) (drivePatch wctx name marker (Recorded wctx peeked)))

-- | Drives a placed patch: the placement check at await time, then the
-- read-claim-write once the position is confirmed still ours.
drivePatch :: (MonadSTM m, MonadTime m) => WorkflowCtx exec m -> Text -> Marker -> StepPlacement exec m -> m (Either (TransactError.Error TransactError.EngineOnly) Bool)
drivePatch wctx name marker placement =
  case checkHere placement (patchMarkerName name) (Just (stepCtxBoundary wctx)) of
    Left err -> pure (Left err)
    Right (DurabilityRecorded wctx' stepId') -> resolvePatch wctx' name marker stepId'
    Right DurabilityPlain ->
      -- Unreachable through our builds, which produce only Recorded
      -- placements or fail before placing, and checkHere never answers
      -- Plain for a Recorded one. Kept for exhaustiveness; reports the
      -- current position.
      do
        current <- peekStepId wctx
        pure (Left (TransactError.PatchNondeterminism {workflowId = workflowId wctx, patch = name, stepId = current}))

-- | What awaiting a patch does: read, claim, write. The read means
-- something only if nothing took the position while it was out: a claim
-- failure, or a moved peek on a non-occupying path, is 'PatchNondeterminism'.
resolvePatch :: (MonadSTM m, MonadTime m) => WorkflowCtx exec m -> Text -> Marker -> Int -> m (Either (TransactError.Error TransactError.EngineOnly) Bool)
resolvePatch wctx name marker stepId = do
  let wid = WorkflowId (workflowId wctx)
      markerName = patchMarkerName name
      raced = TransactError.PatchNondeterminism {workflowId = workflowId wctx, patch = name, stepId = stepId}
  found <- withSystemDB wctx (\db -> SystemDB.checkStep db wid stepId markerName)
  found' <- case found of
    Left err -> case err of
      -- The determinism check's refusal is this call's answer: a row
      -- recorded under another name is the old code's step.
      SystemDBError.UnexpectedStep {} -> pure (Right FoundOtherStep)
      _ -> pure (Left (TransactError.fromSystemDB err))
    Right (Just _) -> pure (Right FoundThisMarker)
    Right Nothing -> pure (Right FoundNothing)
  case found' of
    Left err -> pure (Left err)
    Right found'' -> do
      let occupy = case (found'', marker) of
            (FoundThisMarker, _) -> True
            (FoundNothing, Write) -> True
            (FoundOtherStep, _) -> False
            (FoundNothing, Honour) -> False
      if occupy
        then do
          claimed <- claimStepId wctx stepId
          if claimed
            then settleMarker wctx name marker wid markerName stepId found''
            else pure (Left raced)
        else do
          current <- peekStepId wctx
          if current == stepId
            then settleMarker wctx name marker wid markerName stepId found''
            else pure (Left raced)
  where
    settleMarker wctx' name' marker' wid' markerName' stepId' found'' =
      case (found'', marker') of
        (FoundNothing, Write) -> do
          -- Held in a variable, so a retry of the write repeats the same
          -- completion time and is recognised as this caller's own.
          now <- timestampNow
          recorded <-
            withSystemDB wctx' (\db -> SystemDB.recordStep db wid' stepId' markerName' (OutcomeOutput Nothing) Nothing (Just (StepTiming {stepTimingStartedAt = now, stepTimingCompletedAt = now})) (ownerToken wctx'))
          case recorded of
            Left err -> pure (Left (TransactError.fromSystemDB err))
            Right () -> do
              runTracer wctx'.wctxTracer (PatchMarkerRecorded stepId' name')
              pure (Right True)
        (FoundThisMarker, _) -> do
          runTracer wctx'.wctxTracer (PatchMarkerReplayed stepId' name')
          pure (Right True)
        (FoundOtherStep, Write) -> do
          runTracer wctx'.wctxTracer (PatchOldPathTaken stepId' name')
          pure (Right False)
        (FoundNothing, Honour) -> pure (Right True)
        (FoundOtherStep, Honour) -> pure (Right True)
