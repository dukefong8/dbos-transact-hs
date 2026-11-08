{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

-- | Workflow management operations on the launched instance. Transactional
-- validation and writes remain in the SystemDB class; this layer owns only
-- the launched-instance guard and engine error channel.
module DBOS.Transact.Management
  ( ManagementEvent (..),
    cancelWorkflows,
    cancelWorkflowsInWorkflow,
    resumeWorkflows,
    resumeWorkflowsInWorkflow,
    deleteWorkflows,
    deleteWorkflowsWithCaller,
    deleteWorkflowsInWorkflow,
    forkWorkflows,
    forkWorkflowsInWorkflow,
    updateWorkflowAttributesInWorkflow,
    mergeWorkflowAttributesInWorkflow,
    forkFrom,
    forkFromInWorkflow,
    setWorkflowDelay,
    setWorkflowDelayInWorkflow,
    checkRewindOptions,
    checkForkArgs,
    checkForkFromArgs,
    rewindWorkflows,
    rewindWorkflowsInWorkflow,
    updateWorkflowAttributes,
    mergeWorkflowAttributes,
    listWorkflows,
    ListWorkflowStepsOptions (..),
    defaultListWorkflowStepsOptions,
    StepInfo (..),
    StepOutcome (..),
    StepOutput (..),
    StepFailure (..),
    PortableWorkflowError (..),
    stepInfoFromRecord,
    listWorkflowSteps,
    listWorkflowStepsWith,
    listWorkflowStepsInWorkflow,
    listWorkflowStepsWithInWorkflow,
    listWorkflowStepsSerialized,
    listWorkflowStepsSerializedWith,
    listWorkflowStepsSerializedInWorkflow,
    listWorkflowStepsSerializedWithInWorkflow,
    listWorkflowsInWorkflow,
  )
where

import Data.Aeson (Value (..), eitherDecodeStrict')
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Int (Int64)
import Data.Text.Encoding (encodeUtf8)
import DBOS.Prelude
import DBOS.SystemDB.Class qualified as SystemDB
import DBOS.SystemDB.Types (AttributeUpdate (..), Fork (..), ForkCallRefusal (..), ForkOptions, ForkPoint (..), QueueName (..), RewindOptions (..), Serialization (..), SerializedWorkflowValue (..), StepRecord (..), Timestamp, WorkflowDelay (..), WorkflowFilter, WorkflowId (..), WorkflowOwner, WorkflowRecord, cancelStepName, deleteStepName, forkOptionProblems, forkProblems, forkStepName, internalQueueName, listStepsStepName, listWorkflowsStepName, refuseInvalidForks, resumeStepName, rewindOptionProblems, rewindStepName, setWorkflowDelayStepName, sysRewindOptions, updateWorkflowAttributesStepName, validateAttributes, validateAttributeUpdate)
import DBOS.Transact.Connection (Connection (..), runSystemDB, withConnection)
import DBOS.Transact.Context (StepCtx (stepCtxWorkflow), WorkflowCtx (wctxConn), stepCtxStatus, workflowId, workflowOwner)
import DBOS.Transact.Error qualified as TransactError
import DBOS.Transact.Logger (LogEvent (..), LogSeverity (..), runTracer)
import DBOS.Transact.Serialization (CodecError (..), decodeWorkflowValue)
import DBOS.Transact.Step (runStepWith, stepOptionsDefault)
import System.Log.FastLogger (ToLogStr (..))

-- | Operator-action events: the management surface's announcements.
-- Rendered lines keep the Rust @tracing!@ message bodies with their
-- @key=value@ span fields, so operators see the same text. Mirrors
-- @management.rs@'s @Connection@ impl: cancel guards on non-empty, delete
-- on a positive count, fork splits one id from many, the rest announce
-- unconditionally; the reads log nothing.
data ManagementEvent
  = WorkflowsCancelled { managementCancelled :: Int }
  | WorkflowsResumed { managementRequested :: Int, managementResumed :: Int }
  | WorkflowForked { managementForkedId :: Text }
  | WorkflowsForked { managementForked :: Int }
  | WorkflowRewound { managementRewoundId :: Text, managementRewindCut :: Int, managementRewindQueue :: Text }
  | WorkflowsDeleted { managementDeleted :: Word64 }
  | WorkflowDelayMoveAsked { managementWorkflowId :: Text }
  | WorkflowAttributesReplaceAsked { managementWorkflowId :: Text }
  | WorkflowAttributesMergeAsked { managementWorkflowId :: Text }
  deriving stock (Eq, Show)

instance LogEvent ManagementEvent where
  eventSeverity WorkflowsCancelled {}             = SeverityInfo
  eventSeverity WorkflowsResumed {}               = SeverityInfo
  eventSeverity WorkflowForked {}                 = SeverityInfo
  eventSeverity WorkflowsForked {}                = SeverityInfo
  eventSeverity WorkflowRewound {}                = SeverityInfo
  eventSeverity WorkflowsDeleted {}               = SeverityInfo
  eventSeverity WorkflowDelayMoveAsked {}         = SeverityInfo
  eventSeverity WorkflowAttributesReplaceAsked {} = SeverityInfo
  eventSeverity WorkflowAttributesMergeAsked {} = SeverityInfo
  renderEvent (WorkflowsCancelled cancelled) =
    "cancelled workflows cancelled=" <> showText cancelled
  renderEvent (WorkflowsResumed requested resumed) =
    "resumed workflows onto their queues requested=" <> showText requested <> " resumed=" <> showText resumed
  renderEvent (WorkflowForked forkedId) =
    "forked the workflow onto its queue forked_id=" <> forkedId
  renderEvent (WorkflowsForked count) =
    "forked workflows onto their queues count=" <> showText count
  renderEvent (WorkflowRewound rewoundId cut queue) =
    "rewound the workflow onto its queue workflow_id=" <> rewoundId <> " start_step=" <> showText cut <> " queue=" <> queue
  renderEvent (WorkflowsDeleted deleted) =
    "deleted workflows deleted=" <> showText deleted
  renderEvent (WorkflowDelayMoveAsked workflowId) =
    "asked to move the workflow's release time workflow_id=" <> workflowId
  renderEvent (WorkflowAttributesReplaceAsked workflowId) =
    "asked to replace the workflow's attributes workflow_id=" <> workflowId
  renderEvent (WorkflowAttributesMergeAsked workflowId) =
    "asked to merge into the workflow's attributes workflow_id=" <> workflowId

instance ToLogStr ManagementEvent where
  toLogStr = toLogStr . renderLine

cancelWorkflows :: Monad m
                => Connection m -> [WorkflowId] -> Bool -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
cancelWorkflows conn workflowIds cancelChildren fromWorkflow = do
  result <- withConnection conn (\db -> SystemDB.cancelWorkflows db workflowIds cancelChildren Nothing fromWorkflow)
  case result of
    Left err -> pure (Left err)
    -- Mirrors @cancel_all@: what moved, not what was asked for — silence
    -- when nothing moved.
    Right cancelled -> do
      if null cancelled
        then pure ()
        else runTracer conn.connTracer (WorkflowsCancelled (length cancelled))
      pure (Right cancelled)

-- | Cancels workflows as a step of the calling workflow: the call takes a
-- step id, runs once, and replays its recorded ids. Same leaf rule as
-- every in-workflow call — inside a step body it runs plainly.
cancelWorkflowsInWorkflow :: (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m)
                          => WorkflowCtx exec m -> [WorkflowId] -> Bool -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
cancelWorkflowsInWorkflow wctx workflowIds cancelChildren =
  runStepWith
    stepOptionsDefault
    wctx
    cancelStepName
    (\_ -> cancelWorkflows conn workflowIds cancelChildren (Just (workflowId wctx)))
  where
    conn = wctx.wctxConn

resumeWorkflows :: Monad m
                => Connection m -> [WorkflowId] -> Maybe Text -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
resumeWorkflows conn workflowIds queueName fromWorkflow = do
  result <- withConnection conn (\db -> SystemDB.resumeWorkflows db workflowIds queueName Nothing fromWorkflow)
  case result of
    Left err -> pure (Left err)
    -- Mirrors @resume_all@: what moved against what was asked — an id that
    -- had already finished still counts as requested, so this announces
    -- unconditionally.
    Right resumed -> do
      runTracer conn.connTracer (WorkflowsResumed (length workflowIds) (length resumed))
      pure (Right resumed)

-- | Resumes workflows as a step of the calling workflow. Same leaf rule
-- as every in-workflow call.
resumeWorkflowsInWorkflow :: (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m)
                          => WorkflowCtx exec m -> [WorkflowId] -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
resumeWorkflowsInWorkflow wctx workflowIds queueName =
  runStepWith
    stepOptionsDefault
    wctx
    resumeStepName
    (\_ -> resumeWorkflows conn workflowIds queueName (Just (workflowId wctx)))
  where
    conn = wctx.wctxConn

deleteWorkflows :: Monad m
                => Connection m -> [WorkflowId] -> Bool -> m (Either (TransactError.Error TransactError.EngineOnly) Word64)
deleteWorkflows conn workflowIds deleteChildren =
  deleteWorkflowsWithCaller conn workflowIds deleteChildren Nothing

-- | Deletes workflows naming the calling workflow's owner, so the backend
-- refuses a target set containing the caller itself and fences the removal
-- on the caller's token. The engine's in-workflow entry passes the owner of
-- the context the call was checkpointed under.
deleteWorkflowsWithCaller :: Monad m
                          => Connection m -> [WorkflowId] -> Bool -> Maybe WorkflowOwner -> m (Either (TransactError.Error TransactError.EngineOnly) Word64)
deleteWorkflowsWithCaller conn workflowIds deleteChildren owner = do
  result <- withConnection conn (\db -> SystemDB.deleteWorkflows db workflowIds deleteChildren owner)
  case result of
    Left err -> pure (Left err)
    -- Mirrors @delete_all@: silence when no rows went.
    Right deleted -> do
      if deleted == 0
        then pure ()
        else runTracer conn.connTracer (WorkflowsDeleted deleted)
      pure (Right deleted)

-- | Deletes workflows as a step of the calling workflow: the call takes a
-- step id, runs once, and replays its recorded count. A refusal (the
-- target set holds the caller) carries no checkpoint — a control error,
-- like every system-database failure — so a replay refuses again rather
-- than replaying a delete. Inside a step body the call runs plainly with
-- no caller, by the leaf rule.
deleteWorkflowsInWorkflow :: (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m)
                          => WorkflowCtx exec m -> [WorkflowId] -> Bool -> m (Either (TransactError.Error TransactError.EngineOnly) Word64)
deleteWorkflowsInWorkflow wctx workflowIds deleteChildren =
  runStepWith
    stepOptionsDefault
    wctx
    deleteStepName
    ( \sctx -> case stepCtxStatus sctx of
        Just _ ->
          deleteWorkflowsWithCaller
            conn
            workflowIds
            deleteChildren
            (Just (workflowOwner sctx.stepCtxWorkflow))
        Nothing -> deleteWorkflows conn workflowIds deleteChildren
    )
  where
    conn = wctx.wctxConn

forkWorkflows :: Monad m
              => Connection m -> [Fork] -> ForkOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
forkWorkflows conn forks options = case checkForkArgs forks options of
  Left err -> pure (Left err)
  Right () -> do
    result <- withConnection conn (\db -> SystemDB.forkWorkflows db forks options Nothing)
    case result of
      Left err -> pure (Left err)
      Right forked -> do
        announceFork conn forked
        pure (Right forked)

-- | The engine-layer fork validation: the oracle's whole-call check, which
-- walks each fork in turn — its own rules, then whether its pinned id is
-- an original, then whether an earlier fork already pinned it — and the
-- options last, reported as an engine refusal (Rust @InvalidArgument@)
-- rather than a database one. A batch arm's detail is already a complete
-- sentence naming the id; per-fork and option problems report as field
-- problems. Runs before anything runs on the launched-instance entries,
-- plain and in-workflow; the client calls the backends directly and
-- reports their sysdb refusal. An empty fork list is an immediate
-- success with no validation, as the oracle returns @Ok@ before
-- validating options.
checkForkArgs :: [Fork] -> ForkOptions -> Either (TransactError.Error e) ()
checkForkArgs [] _ = Right ()
checkForkArgs forks options = case refuseInvalidForks forks options of
  Just (ForkBatchRefusal detail) -> Left (TransactError.InvalidArgument TransactError.forkOperation detail)
  Just (ForkRefusal fork field detail) -> Left (TransactError.InvalidArgument TransactError.forkOperation (TransactError.refusedFork fork field detail))
  Just (ForkOptionsRefusal field detail) -> Left (TransactError.InvalidArgument TransactError.forkOperation (TransactError.refusedForkOptions field detail))
  Nothing -> Right ()

-- | The engine-layer fork-from validation: options plus the per-source
-- checks the oracle makes by validating a synthesized fork per id — three
-- of the four points resolve inside the transaction, so only the stated
-- step and the ids can be checked up front. Same channel as every engine
-- refusal. An empty id list is an immediate success with no validation,
-- matching the oracle and the backends.
checkForkFromArgs :: [WorkflowId] -> ForkPoint -> ForkOptions -> Either (TransactError.Error e) ()
checkForkFromArgs [] _ _ = Right ()
checkForkFromArgs workflowIds point options = case perFork of
  (fork, field, detail) : _ -> Left (TransactError.InvalidArgument "fork from workflows" (TransactError.refusedFork fork field detail))
  [] -> case forkOptionProblems options of
    Just (field, detail) -> Left (TransactError.InvalidArgument "fork from workflows" (TransactError.refusedForkOptions field detail))
    Nothing -> Right ()
  where
    givenStep = case point of
      ForkStep step -> step
      _             -> 0
    synthesized (WorkflowId source) = Fork source Nothing givenStep
    perFork = [(fork, field, detail) | fork <- map synthesized workflowIds, Just (field, detail) <- [forkProblems fork]]

-- | Forks workflows as a step of the calling workflow: a fork generates a
-- new id, so a replay without this checkpoint would write a second fork
-- under a second id. Arguments are refused before the step id is taken,
-- so a refused call spends nothing. Same leaf rule as every in-workflow
-- call.
forkWorkflowsInWorkflow :: (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m)
                        => WorkflowCtx exec m -> [Fork] -> ForkOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
forkWorkflowsInWorkflow wctx forks options =
  case checkForkArgs forks options of
    Left err -> pure (Left err)
    Right () ->
      runStepWith
        stepOptionsDefault
        wctx
        forkStepName
        (\_ -> forkWorkflows conn forks options)
  where
    conn = wctx.wctxConn

forkFrom :: Monad m
         => Connection m -> [WorkflowId] -> ForkPoint -> ForkOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
forkFrom conn workflowIds point options = case checkForkFromArgs workflowIds point options of
  Left err -> pure (Left err)
  Right () -> do
    result <- withConnection conn (\db -> SystemDB.forkFrom db workflowIds point options Nothing)
    case result of
      Left err -> pure (Left err)
      Right forked -> do
        announceFork conn forked
        pure (Right forked)

-- | The fork announcement shared by both fork paths, mirroring
-- @fork_all@: one id names its fork, any other count — including zero —
-- reports the batch size.
announceFork :: Monad m => Connection m -> [WorkflowId] -> m ()
announceFork conn forked = case forked of
  [WorkflowId only] -> runTracer conn.connTracer (WorkflowForked only)
  many              -> runTracer conn.connTracer (WorkflowsForked (length many))

-- | The engine-layer options check: the same rules as the system
-- database, reported as an engine refusal (Rust @InvalidArgument@) rather
-- than a database one. Runs before any step id moves or anything runs, on
-- every engine entry point; the backends keep their own sysdb refusal as
-- defense in depth.
checkRewindOptions :: RewindOptions -> Either (TransactError.Error e) ()
checkRewindOptions options = case rewindOptionProblems options of
  Nothing              -> Right ()
  Just (field, detail) -> Left (TransactError.InvalidArgument "rewind a workflow" (field <> " " <> detail))

-- | Rewinds a workflow as the operator asked: options are refused before
-- anything runs, and the datasource checkpoints are dropped by the
-- instance layer first (see 'DBOS.Transact.Instance.rewindWorkflows') —
-- this call owns only the system-database rewind. Mirrors Rust
-- @Connection::rewind@.
rewindWorkflows :: Monad m
                => Connection m -> WorkflowId -> RewindOptions -> m (Either (TransactError.Error TransactError.EngineOnly) ())
rewindWorkflows conn target options = case checkRewindOptions options of
  Left err -> pure (Left err)
  Right () -> do
    result <- withConnection conn (\db -> SystemDB.rewindWorkflow db target options.rewindStartStep (sysRewindOptions options) Nothing)
    case result of
      Left err -> pure (Left err)
      Right () -> do
        let WorkflowId widText = target
            QueueName queue = fromMaybe internalQueueName (QueueName <$> options.rewindQueueName)
        runTracer conn.connTracer (WorkflowRewound widText options.rewindStartStep queue)
        pure (Right ())

-- | Rewinds a workflow as a step of the calling workflow: the engine
-- step owns the checkpoint, so the system-database call runs without a
-- caller of its own — recording in both places would collide on the step
-- id, the same reason forks pass no caller. A recovered caller replays
-- the recorded step rather than rewinding the target a second time.
-- Arguments are refused before the step id is taken. Same leaf rule as
-- every in-workflow call.
rewindWorkflowsInWorkflow :: (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m)
                          => WorkflowCtx exec m -> WorkflowId -> RewindOptions -> m (Either (TransactError.Error TransactError.EngineOnly) ())
rewindWorkflowsInWorkflow wctx target options =
  case checkRewindOptions options of
    Left err -> pure (Left err)
    Right () ->
      runStepWith
        stepOptionsDefault
        wctx
        rewindStepName
        (\_ -> rewindWorkflows conn target options)
  where
    conn = wctx.wctxConn

-- | Forks from a point as a step of the calling workflow. Arguments are
-- refused before the step id is taken. Same leaf rule as every
-- in-workflow call.
forkFromInWorkflow :: (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m)
                   => WorkflowCtx exec m -> [WorkflowId] -> ForkPoint -> ForkOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowId])
forkFromInWorkflow wctx workflowIds point options =
  case checkForkFromArgs workflowIds point options of
    Left err -> pure (Left err)
    Right () ->
      runStepWith
        stepOptionsDefault
        wctx
        forkStepName
        (\_ -> forkFrom conn workflowIds point options)
  where
    conn = wctx.wctxConn

-- | Moves a delayed workflow's release: brought forward to now, the
-- supervisor releases it on its next pass. Mirrors Rust
-- @Connection::set_workflow_delay@: phrased as the request, not the
-- effect — the row may already be released, so this announces
-- unconditionally. A refusal of the delay reports as an argument refusal
-- to this operation, every other failure on the database channel — the
-- @map_err(refused_argument)@, so the surface never restates what the
-- database's own check decides.
setWorkflowDelay :: Monad m
                 => Connection m -> WorkflowId -> WorkflowDelay -> m (Either (TransactError.Error TransactError.EngineOnly) ())
setWorkflowDelay conn workflowId delay = do
  result <- runSystemDB conn.connSysdb (\db -> SystemDB.setWorkflowDelay db workflowId delay Nothing)
  TransactError.refusing (TransactError.refusedArgument delayOperation) result $ \() -> do
    let WorkflowId workflowText = workflowId
    runTracer conn.connTracer (WorkflowDelayMoveAsked workflowText)
    pure (Right ())

-- | The operation name a refused delay reports under. Mirrors Rust
-- @DELAY_OPERATION@.
delayOperation :: Text
delayOperation = "delay a workflow"

-- | Moves a delayed workflow's release as a step of the calling workflow.
-- Same leaf rule as every in-workflow call.
setWorkflowDelayInWorkflow :: (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m)
                           => WorkflowCtx exec m -> WorkflowId -> WorkflowDelay -> m (Either (TransactError.Error TransactError.EngineOnly) ())
setWorkflowDelayInWorkflow wctx workflowId delay =
  runStepWith
    stepOptionsDefault
    wctx
    setWorkflowDelayStepName
    (\_ -> setWorkflowDelay conn workflowId delay)
  where
    conn = wctx.wctxConn

-- | Replaces the attributes attached to a workflow, or clears them when
-- given 'Nothing'. Mirrors Rust @DBOS::update_workflow_attributes@ outside
-- a workflow, where the call is plain: the encoding happens before any id
-- could be taken, and the backend replaces rather than merges. An id no
-- workflow has is refused with 'NonExistentWorkflow', rather than reported
-- as done with nothing written.
updateWorkflowAttributes :: Monad m
                         => Connection m -> WorkflowId -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
updateWorkflowAttributes conn workflowId attributes =
  updateWorkflowAttributesAs conn workflowId (Replace attributes) WorkflowAttributesReplaceAsked

-- | Merges keys into a workflow's attributes, in one statement. A key
-- already there takes the new value and the others stay as they are; a key
-- set to @null@ is stored as @null@, not removed. Attributes that are
-- absent, or are not an object, count as an empty object, so an empty map
-- leaves @{}@ where there were none. Atomic, so two merges of different
-- keys both land. Mirrors Rust @DBOS::merge_workflow_attributes@ outside
-- a workflow. An id no workflow has is refused, as by
-- 'updateWorkflowAttributes'.
mergeWorkflowAttributes :: Monad m
                        => Connection m -> WorkflowId -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
mergeWorkflowAttributes conn workflowId attributes =
  updateWorkflowAttributesAs conn workflowId (Merge attributes) WorkflowAttributesMergeAsked

-- | The shared write behind the replace and the merge: the backend judges
-- the update and refuses a missing id, and the announcement is phrased as
-- the request, not the effect — a replay reads back the recorded outcome
-- and writes nothing, so it announces "asked to" again from the one call.
updateWorkflowAttributesAs :: Monad m
                           => Connection m -> WorkflowId -> AttributeUpdate -> (Text -> ManagementEvent) -> m (Either (TransactError.Error TransactError.EngineOnly) ())
updateWorkflowAttributesAs conn workflowId update asked = do
  result <- withConnection conn (\db -> SystemDB.updateWorkflowAttributes db workflowId update Nothing)
  case result of
    Left err -> pure (Left err)
    Right () -> do
      let WorkflowId workflowText = workflowId
      runTracer conn.connTracer (asked workflowText)
      pure (Right ())

-- | Replaces a workflow's attributes as a step of the calling workflow, or
-- clears them when given 'Nothing'. The encoding happens before the step id
-- is taken — attributes that cannot be encoded are a call that never
-- happens — as the oracle encodes before it places. Same leaf rule as every
-- in-workflow call.
updateWorkflowAttributesInWorkflow :: (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m)
                                   => WorkflowCtx exec m -> WorkflowId -> Maybe Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
updateWorkflowAttributesInWorkflow wctx workflowId attributes =
  case validateAttributes attributes of
    Left err -> pure (Left (TransactError.fromSystemDB err))
    Right () ->
      runStepWith
        stepOptionsDefault
        wctx
        updateWorkflowAttributesStepName
        (\_ -> updateWorkflowAttributes conn workflowId attributes)
  where
    conn = wctx.wctxConn

-- | Merges keys into a workflow's attributes as a step of the calling
-- workflow. Recorded under the same step name as a replacement, as the
-- oracle records both under one. The encoding happens before the step id
-- is taken, as above.
mergeWorkflowAttributesInWorkflow :: (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m)
                                  => WorkflowCtx exec m -> WorkflowId -> Text -> m (Either (TransactError.Error TransactError.EngineOnly) ())
mergeWorkflowAttributesInWorkflow wctx workflowId attributes =
  case validateAttributeUpdate (Merge attributes) of
    Left err -> pure (Left (TransactError.fromSystemDB err))
    Right () ->
      runStepWith
        stepOptionsDefault
        wctx
        updateWorkflowAttributesStepName
        (\_ -> mergeWorkflowAttributes conn workflowId attributes)
  where
    conn = wctx.wctxConn

-- | Reads the workflows matching a filter. Mirrors Rust
-- @DBOS::list_workflows@ outside a workflow, where the call is plain: every
-- filter is one @WHERE@ clause, so the default returns the whole table.
listWorkflows :: Monad m
              => Connection m -> WorkflowFilter -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowRecord])
listWorkflows conn filters = do
  result <- withConnection conn (\db -> SystemDB.listWorkflows db filters Nothing)
  pure result

-- | What a step listing may leave out or page. Mirrors Rust
-- @ListWorkflowStepsOptions@. With 'loadOutput' off the payload columns are
-- not read at all, so every step reports 'StepOutcomeNotLoaded' whatever it
-- recorded.
data ListWorkflowStepsOptions = ListWorkflowStepsOptions
  { loadOutput :: Bool,
    limit      :: Maybe Int64,
    offset     :: Maybe Int64
  }
  deriving stock (Eq, Show)

-- | Read every step with its payload: the listing's historical default.
defaultListWorkflowStepsOptions :: ListWorkflowStepsOptions
defaultListWorkflowStepsOptions =
  ListWorkflowStepsOptions
    { loadOutput = True,
      limit = Nothing,
      offset = Nothing
    }

-- | A portable error record as another SDK writes it. Kept separate from the
-- port's typed 'TransactError.Error' because a client listing has no workflow
-- error type to decode into.
data PortableWorkflowError = PortableWorkflowError
  { portableWorkflowErrorName    :: Text,
    portableWorkflowErrorMessage :: Text,
    portableWorkflowErrorCode    :: Maybe Value,
    portableWorkflowErrorData    :: Maybe Value
  }
  deriving stock (Eq, Show)

-- | What a step's output column says. The states are distinct: the step
-- recorded none, it decoded, or it exists but this build cannot read it.
data StepOutput
  = PayloadAbsent
  | PayloadDecoded Value
  | PayloadUndecodable CodecError
  deriving stock (Eq, Show)

-- | What a step's error column says. A portable record survives structured;
-- a native/unknown tag or a malformed body is reported, never guessed.
data StepFailure
  = FailurePortable PortableWorkflowError
  | FailureUndecodable CodecError
  deriving stock (Eq, Show)

data StepOutcome
  = -- | With 'loadOutput' off the payload columns were never read, so no
    -- outcome is known.
    StepOutcomeNotLoaded
  | -- | The row is not finished: neither an error nor an output is recorded
    -- and the completed-at column is still empty (a foreign in-flight step).
    StepOutcomePending
  | StepOutcomeSucceeded StepOutput
  | StepOutcomeFailed StepFailure
  deriving stock (Eq, Show)

-- | The typed view of a 'StepRecord': the Rust row stays the storage shape;
-- this is the decoded API shape.
data StepInfo = StepInfo
  { stepInfoWorkflowId   :: WorkflowId,
    stepInfoId           :: Int,
    stepInfoName         :: Text,
    stepInfoOutcome      :: StepOutcome,
    stepInfoChild        :: Maybe WorkflowId,
    stepInfoStartedAt    :: Maybe Timestamp,
    stepInfoCompletedAt  :: Maybe Timestamp
  }
  deriving stock (Eq, Show)

-- | Project a raw Rust-shaped row into the typed view. Error takes precedence
-- when a malformed row carries both columns; the schema's own outcome is
-- one-or-the-other.
stepInfoFromRecord :: ListWorkflowStepsOptions -> StepRecord -> StepInfo
stepInfoFromRecord options record =
  StepInfo
    { stepInfoWorkflowId = record.stepRecordWorkflowId,
      stepInfoId = record.stepRecordStepId,
      stepInfoName = record.stepRecordStepName,
      stepInfoOutcome = outcome,
      stepInfoChild = record.stepRecordChildWorkflowId,
      stepInfoStartedAt = record.stepRecordStartedAt,
      stepInfoCompletedAt = record.stepRecordCompletedAt
    }
  where
    outcome
      | not options.loadOutput = StepOutcomeNotLoaded
      | Just errorText <- record.stepRecordError = StepOutcomeFailed (decodeStepFailure record.stepRecordSerialization errorText)
      | Just outputText <- record.stepRecordOutput = StepOutcomeSucceeded (decodeStepOutput record.stepRecordSerialization outputText)
      | Nothing <- record.stepRecordCompletedAt = StepOutcomePending
      | otherwise = StepOutcomeSucceeded PayloadAbsent

decodeStepOutput :: Maybe Text -> Text -> StepOutput
decodeStepOutput tag text =
  case decodeWorkflowValue "step output" (Just (SerializedWorkflowValue text (Serialization <$> tag))) of
    Right value -> PayloadDecoded value
    Left err -> PayloadUndecodable err

decodeStepFailure :: Maybe Text -> Text -> StepFailure
decodeStepFailure tag text
  | Just name <- tag, name `notElem` ["", "rust_serde", "portable_json"] =
      FailureUndecodable (CodecUnknownSerialization "step error" name)
  | otherwise =
      case decodePortableWorkflowError text of
        Right err -> FailurePortable err
        -- A legacy tagged envelope or another SDK's record still carries its
        -- name and message, even when this port has no matching error type.
        Left codec -> case TransactError.recordedErrorView text of
          Just (name, message, structured) ->
            FailurePortable
              PortableWorkflowError
                { portableWorkflowErrorName = name,
                  portableWorkflowErrorMessage = message,
                  portableWorkflowErrorCode = Nothing,
                  portableWorkflowErrorData = structured
                }
          Nothing -> FailureUndecodable codec

decodePortableWorkflowError :: Text -> Either CodecError PortableWorkflowError
decodePortableWorkflowError text =
  case eitherDecodeStrict' (encodeUtf8 text) of
    Left _ -> Left (CodecNotJson "step error" text)
    Right (Object fields) ->
      case (KeyMap.lookup "name" fields, KeyMap.lookup "message" fields) of
        (Just (String name), Just (String message)) ->
          Right
            PortableWorkflowError
              { portableWorkflowErrorName = name,
                portableWorkflowErrorMessage = message,
                portableWorkflowErrorCode = KeyMap.lookup "code" fields,
                portableWorkflowErrorData = KeyMap.lookup "data" fields
              }
        _ -> Left (CodecTypeMismatch "step error" "the portable error needs string name and message")
    Right _ -> Left (CodecTypeMismatch "step error" "the portable error must be an object")

-- | Reads one workflow's steps in execution order with their outputs and
-- errors decoded. The raw Rust-shaped listing remains available internally as
-- 'listWorkflowStepsSerialized'.
listWorkflowSteps :: Monad m
                  => Connection m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) [StepInfo])
listWorkflowSteps conn workflowId =
  listWorkflowStepsWith conn workflowId defaultListWorkflowStepsOptions

-- | 'listWorkflowSteps' with the listing's options.
listWorkflowStepsWith :: Monad m
                      => Connection m -> WorkflowId -> ListWorkflowStepsOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [StepInfo])
listWorkflowStepsWith conn workflowId options = do
  listed <- listWorkflowStepsSerializedWith conn workflowId options
  pure (map (stepInfoFromRecord options) <$> listed)

-- | The raw Rust-shaped listing, outputs and errors encoded with their tag.
listWorkflowStepsSerialized :: Monad m
                            => Connection m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) [StepRecord])
listWorkflowStepsSerialized conn workflowId =
  listWorkflowStepsSerializedWith conn workflowId defaultListWorkflowStepsOptions

-- | The raw listing with options.
listWorkflowStepsSerializedWith :: Monad m
                                => Connection m -> WorkflowId -> ListWorkflowStepsOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [StepRecord])
listWorkflowStepsSerializedWith conn workflowId options = do
  result <- withConnection conn (\db -> SystemDB.listSteps db workflowId options.loadOutput options.limit options.offset Nothing)
  pure result

-- | Lists a workflow's steps as a step of the calling workflow, under the
-- cross-SDK name, so a replayed listing reads the snapshot the first
-- execution saw. The checkpoint records the row list together with the
-- 'loadOutput' flag it was read with, so a replay re-projects the typed view
-- from the bytes the first execution actually read rather than from the
-- replayed call's options.
listWorkflowStepsWithInWorkflow :: (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m)
                                => WorkflowCtx exec m -> WorkflowId -> ListWorkflowStepsOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [StepInfo])
listWorkflowStepsWithInWorkflow wctx workflowId options = do
  snapshot <- listWorkflowStepsSnapshotInWorkflow wctx workflowId options
  pure (fmap (\(loaded, rows) -> map (stepInfoFromRecord options {loadOutput = loaded}) rows) snapshot)

-- | 'listWorkflowStepsWithInWorkflow' with the default options.
listWorkflowStepsInWorkflow :: (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m)
                            => WorkflowCtx exec m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) [StepInfo])
listWorkflowStepsInWorkflow wctx workflowId =
  listWorkflowStepsWithInWorkflow wctx workflowId defaultListWorkflowStepsOptions

-- | The raw in-workflow listing: the checkpoint records the Rust-shaped step
-- rows and the flag they were read with.
listWorkflowStepsSerializedWithInWorkflow :: (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m)
                                         => WorkflowCtx exec m -> WorkflowId -> ListWorkflowStepsOptions -> m (Either (TransactError.Error TransactError.EngineOnly) [StepRecord])
listWorkflowStepsSerializedWithInWorkflow wctx workflowId options =
  fmap (fmap snd) (listWorkflowStepsSnapshotInWorkflow wctx workflowId options)

-- | The raw in-workflow listing with default options.
listWorkflowStepsSerializedInWorkflow :: (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m)
                                     => WorkflowCtx exec m -> WorkflowId -> m (Either (TransactError.Error TransactError.EngineOnly) [StepRecord])
listWorkflowStepsSerializedInWorkflow wctx workflowId =
  listWorkflowStepsSerializedWithInWorkflow wctx workflowId defaultListWorkflowStepsOptions

-- | The checkpointed listing step: records @(loadOutput, rows)@ so a replay
-- re-projects the typed view from the bytes the first execution read.
listWorkflowStepsSnapshotInWorkflow :: (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m)
                                    => WorkflowCtx exec m -> WorkflowId -> ListWorkflowStepsOptions -> m (Either (TransactError.Error TransactError.EngineOnly) (Bool, [StepRecord]))
listWorkflowStepsSnapshotInWorkflow wctx workflowId options =
  runStepWith
    stepOptionsDefault
    wctx
    listStepsStepName
    (\_ -> fmap (fmap (\rows -> (options.loadOutput, rows))) (listWorkflowStepsSerializedWith conn workflowId options))
  where
    conn = wctx.wctxConn

-- | Lists workflows as a step of the calling workflow, under the
-- cross-SDK name, so a step listing reads the same whichever SDK wrote
-- it. Same leaf rule as every in-workflow call.
listWorkflowsInWorkflow :: (MonadDelay m, MonadTime m, MonadAsync m, MonadCatch m)
                        => WorkflowCtx exec m -> WorkflowFilter -> m (Either (TransactError.Error TransactError.EngineOnly) [WorkflowRecord])
listWorkflowsInWorkflow wctx filters =
  runStepWith
    stepOptionsDefault
    wctx
    listWorkflowsStepName
    (\_ -> listWorkflows conn filters)
  where
    conn = wctx.wctxConn
