{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Shared checkpoint-placement scenarios: one body per case, judged by one
-- pure check on each stack. Placement is pure logic over workflow contexts
-- ('placementHere', 'placeCall', 'checkHere', ...), so scenarios reduce
-- every engine comparison to stack-free facts (booleans, texts, error
-- literals) inside the scenario — the same comparisons run live and under
-- IOSim, and the checks judge facts. The live tree
-- ('DBOS.Transact.CheckpointTest') builds contexts over Postgres rows, the
-- sim tree ('DBOS.Transact.CheckpointTestSim') over the in-memory backend;
-- placement checks never reach the database on either stack.
module DBOS.Transact.CheckpointCases
  ( CheckpointFixture (..),
    withCtxOf,
    takenOther,
    scenarioOutside,
    scenarioBoundaryRecords,
    scenarioCapturedParent,
    scenarioTakenOther,
    scenarioLeafRule,
    scenarioRecordedDurable,
    scenarioRecordedRefused,
    scenarioClientPlain,
    scenarioInStepOwnBody,
    scenarioSiblingRefused,
    scenarioPlacementNames,
    checkOutside,
    checkBoundaryRecords,
    checkCapturedParent,
    checkTakenOther,
    checkLeafRule,
    checkRecordedDurable,
    checkRecordedRefused,
    checkClientPlain,
    checkInStepOwnBody,
    checkSiblingRefused,
    checkPlacementNames,
  )
where

import DBOS.Prelude
import DBOS.Transact
  ( EngineOnly,
    Error (..),
    WorkflowCtx,
  )
import DBOS.Transact.Context (firstStepStatus, nextWorkflowMarker, withStep)
import DBOS.Transact.Checkpoint
  ( PendingStep (..),
    StepDurability (..),
    StepPlacement (..),
    checkHere,
    describePlacement,
    insideAWorkflow,
    pendingStepId,
    placeCall,
    placementAt,
    placementHere,
    placementStepId,
    placementWhereabouts,
  )
import DBOS.Transact.Context (stepCtxBoundary)

-- | What a stack must provide: a workflow context to place calls through,
-- and the taken-placement probe over a second connection.
data CheckpointFixture m = CheckpointFixture
  { ccfWithCtx :: forall a. (forall exec. WorkflowCtx exec m -> m a) -> m a,
    ccfTakenOther :: forall exec. WorkflowCtx exec m -> m (Either (Error EngineOnly) (StepPlacement exec m))
  }

-- | Run with a fresh context: the rank-2 field is read by pattern match
-- because record-dot has no 'HasField' instance for polymorphic fields.
withCtxOf :: CheckpointFixture m -> (forall exec. WorkflowCtx exec m -> m a) -> m a
withCtxOf (CheckpointFixture run _) = run

-- | The taken-placement probe over the fixture's other connection.
takenOther :: CheckpointFixture m -> WorkflowCtx exec m -> m (Either (Error EngineOnly) (StepPlacement exec m))
takenOther (CheckpointFixture _ taken) = taken

-- | Pin the error channel when comparing a placement probe: the 'e' of
-- 'checkHere' floats free in shared code (no call site names it), so these
-- helpers name 'EngineOnly' once instead of annotating every scenario.
isDurabilityPlain :: Either (Error EngineOnly) (StepDurability exec m) -> Bool
isDurabilityPlain (Right DurabilityPlain) = True
isDurabilityPlain _ = False

isDurableRecorded :: WorkflowCtx exec m -> Int -> Either (Error EngineOnly) (StepDurability exec m) -> Bool
isDurableRecorded ctx stepId result = result == Right (DurabilityRecorded ctx stepId)

isElsewhereRefused :: Text -> Text -> Text -> Either (Error EngineOnly) b -> Bool
isElsewhereRefused step built polled (Left (StepBuiltElsewhere {step = s, built = b, polled = p})) =
  s == step && b == built && p == polled
isElsewhereRefused _ _ _ _ = False

-- | Outside a workflow takes no id and records nothing.
scenarioOutside :: forall m. Monad m => CheckpointFixture m -> m (Bool, Bool, Bool)
scenarioOutside _ = do
  let placement = placementHere Nothing 0
  pure
    ( placement == Outside,
      placementStepId placement == Nothing,
      pendingStepId (PendingStep "DBOS.sleep" (Just placement) (pure () :: m ())) == Nothing
    )

-- | At a step boundary the call records under the allocated id.
scenarioBoundaryRecords :: forall m. (MonadSTM m) => CheckpointFixture m -> m (Bool, Bool, Bool)
scenarioBoundaryRecords fx = withCtxOf fx $ \ctx -> do
  let placement = placementAt (stepCtxBoundary ctx) 0
  pure
    ( placement == Recorded ctx 0,
      placementStepId placement == Just 0,
      pendingStepId (PendingStep "checkout" (Just placement) (pure () :: m ())) == Just 0
    )

-- | A call built through a captured parent while a step body runs is plain.
scenarioCapturedParent :: forall m. (MonadSTM m, MonadCatch m) => CheckpointFixture m -> m Bool
scenarioCapturedParent fx = withCtxOf fx $ \ctx -> do
  marker <- nextWorkflowMarker ctx
  withStep ctx marker (firstStepStatus 0) $ \_sctx -> do
    placement <- placeCall ctx
    pure (placement == PlacementInsideStep (stepCtxBoundary ctx))

-- | A taken placement through a captured parent under another connection
-- is plain.
scenarioTakenOther :: forall m. (MonadSTM m, MonadCatch m) => CheckpointFixture m -> m Bool
scenarioTakenOther fx = withCtxOf fx $ \ctx -> do
  marker <- nextWorkflowMarker ctx
  withStep ctx marker (firstStepStatus 0) $ \_sctx -> do
    placed <- takenOther fx ctx
    pure (placed == Right (PlacementInsideStep (stepCtxBoundary ctx)))

-- | Inside a step body the call is plain by the leaf rule.
scenarioLeafRule :: forall m. (MonadSTM m, MonadCatch m) => CheckpointFixture m -> m (Bool, Bool)
scenarioLeafRule fx = withCtxOf fx $ \ctx -> do
  marker <- nextWorkflowMarker ctx
  withStep ctx marker (firstStepStatus 3) $ \sctx -> do
    let placement = placementAt sctx 1
    pure
      ( placement == PlacementInsideStep sctx,
        placementStepId placement == Nothing
      )

-- | A recorded call polled at its boundary stays durable.
scenarioRecordedDurable :: forall m. (MonadSTM m) => CheckpointFixture m -> m Bool
scenarioRecordedDurable fx = withCtxOf fx $ \ctx -> do
  pure (isDurableRecorded ctx 0 (checkHere (Recorded ctx 0) "checkout" (Just (stepCtxBoundary ctx))))

-- | A recorded call carried into a step is refused.
scenarioRecordedRefused :: forall m. (MonadSTM m, MonadCatch m) => CheckpointFixture m -> m Bool
scenarioRecordedRefused fx = withCtxOf fx $ \ctx -> do
  marker <- nextWorkflowMarker ctx
  withStep ctx marker (firstStepStatus 0) $ \sctx ->
    pure
      (isElsewhereRefused "checkout" "in workflow wf-1" "inside a step of workflow wf-1" (checkHere (Recorded ctx 0) "checkout" (Just sctx)))

-- | A client's call stays plain wherever it is driven.
scenarioClientPlain :: forall m. (MonadSTM m) => CheckpointFixture m -> m (Bool, Bool)
scenarioClientPlain fx = withCtxOf fx $ \ctx -> do
  pure
    ( isDurabilityPlain (checkHere ClientConnection "DBOS.cancel" Nothing),
      isDurabilityPlain (checkHere ClientConnection "DBOS.cancel" (Just (stepCtxBoundary ctx)))
    )

-- | An in-step call polled in its own body stays plain.
scenarioInStepOwnBody :: forall m. (MonadSTM m, MonadCatch m) => CheckpointFixture m -> m Bool
scenarioInStepOwnBody fx = withCtxOf fx $ \ctx -> do
  marker <- nextWorkflowMarker ctx
  withStep ctx marker (firstStepStatus 3) $ \sctx ->
    pure (isDurabilityPlain (checkHere (PlacementInsideStep sctx) "checkout" (Just sctx)))

-- | An in-step call carried to a sibling body is refused.
scenarioSiblingRefused :: forall m. (MonadSTM m, MonadCatch m) => CheckpointFixture m -> m Bool
scenarioSiblingRefused fx = withCtxOf fx $ \ctx -> do
  firstMarker <- nextWorkflowMarker ctx
  secondMarker <- nextWorkflowMarker ctx
  withStep ctx firstMarker (firstStepStatus 3) $ \first ->
    withStep ctx secondMarker (firstStepStatus 3) $ \second ->
      pure
        (isElsewhereRefused "checkout" "inside a step of workflow wf-1" "inside a different step of workflow wf-1" (checkHere (PlacementInsideStep first) "checkout" (Just second)))

-- | Placement predicates and descriptions read the same everywhere.
scenarioPlacementNames :: forall m. (MonadSTM m) => CheckpointFixture m -> m (Bool, Bool, Bool, Text, Text, Text)
scenarioPlacementNames fx = withCtxOf fx $ \ctx -> do
  pure
    ( insideAWorkflow Outside == False,
      insideAWorkflow ClientConnection == True,
      insideAWorkflow (Recorded ctx 0) == True,
      describePlacement Nothing,
      describePlacement (Just (stepCtxBoundary ctx)),
      placementWhereabouts ClientConnection
    )

-- * Checks

-- | Outside takes no id and records nothing.
checkOutside :: (Bool, Bool, Bool) -> Either String ()
checkOutside obs =
  unless (obs == (True, True, True)) $ Left "expected outside to take no id and record nothing"

-- | The boundary call records under the allocated id.
checkBoundaryRecords :: (Bool, Bool, Bool) -> Either String ()
checkBoundaryRecords obs =
  unless (obs == (True, True, True)) $ Left "expected the boundary call to record under id 0"

-- | The captured-parent call is plain.
checkCapturedParent :: Bool -> Either String ()
checkCapturedParent plain =
  unless plain $ Left "expected the captured-parent call to be plain"

-- | The taken placement under another connection is plain.
checkTakenOther :: Bool -> Either String ()
checkTakenOther plain =
  unless plain $ Left "expected the taken placement to be plain"

-- | The leaf-rule call is plain and takes no id.
checkLeafRule :: (Bool, Bool) -> Either String ()
checkLeafRule obs =
  unless (obs == (True, True)) $ Left "expected the leaf-rule call to be plain with no id"

-- | The recorded call stays durable at its boundary.
checkRecordedDurable :: Bool -> Either String ()
checkRecordedDurable durable =
  unless durable $ Left "expected the recorded call to stay durable"

-- | The carried call is refused with the elsewhere error.
checkRecordedRefused :: Bool -> Either String ()
checkRecordedRefused refused =
  unless refused $ Left "expected the carried call to be refused"

-- | The client's call stays plain.
checkClientPlain :: (Bool, Bool) -> Either String ()
checkClientPlain obs =
  unless (obs == (True, True)) $ Left "expected the client's call to stay plain"

-- | The in-step call stays plain in its own body.
checkInStepOwnBody :: Bool -> Either String ()
checkInStepOwnBody plain =
  unless plain $ Left "expected the in-step call to stay plain"

-- | The sibling-carried call is refused.
checkSiblingRefused :: Bool -> Either String ()
checkSiblingRefused refused =
  unless refused $ Left "expected the sibling-carried call to be refused"

-- | Predicates and descriptions match the oracle's words.
checkPlacementNames :: (Bool, Bool, Bool, Text, Text, Text) -> Either String ()
checkPlacementNames obs =
  unless
    ( obs
        == ( True,
             True,
             True,
             "outside a workflow",
             "in workflow wf-1",
             "on a client's connection"
           )
    )
    $ Left "expected the placement predicates and descriptions to match"
