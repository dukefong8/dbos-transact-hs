{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.ManagementTest' mirrored under IOSim over the mock
-- backend: the same call sequences, with the answers the stateless mock
-- returns, each case printing its sim's 'Say' trace inline so a plain
-- @-- $> tasty@ run shows the management announcements with no extra
-- plumbing. Where a live assertion depends on database state (a cancelled
-- row reading back as @CANCELLED@, a deleted row reading back absent, a
-- fork actually running), the mirror asserts the mock's canned answer and
-- says so; the live semantics stay in 'DBOS.Transact.ManagementTest'.
-- Management calls flow through the say-carrier installed at launch, so
-- the 'ManagementEvent' lines below are the same announcements the live
-- tree writes through FastLogger.
module DBOS.Transact.ManagementTestSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, selectTraceEventsDynamic)
import Data.Aeson (FromJSON, ToJSON)
import Data.Either (isLeft, isRight)
import Data.Text (Text)
import DBOS.SystemDB
  ( AwaitedOutcome (..),
    Fork (..),
    ForkOptions (..),
    ForkPoint (..),
    SerializedWorkflowValue (..),
    WorkflowId (..),
    WorkflowStatus (..),
    defaultForkOptions,
    forkNew,
  )
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.IOSim (simDBOSWith, simInstance, simLaunchWith)
import DBOS.IOSimTracer (printSimTrace, runSimCase, simTracer)
import DBOS.Transact.ManagementSimData (mockOutput, mockSerialization)
import DBOS.Transact
  (
    EngineOnly,
    CodecError,
    Ctx,
    WorkflowCtx,
    workflowCtxInner,
    DBOS,
    Executor,
    Error (..),
    ManagementEvent (..),
    QueueConflict (..),
    Serialization (..),
    StartOptions (..),
    WorkflowHandle,
    WorkflowKey,
    WorkflowRef,
    cancelWorkflows,
    decodeWorkflowValue,
    defaultQueueOptions,
    deleteWorkflows,
    encodeWorkflowValue,
    enqueueNew,
    forkFrom,
    forkWorkflows,
    handleResult,
    handleStatus,
    handleWorkflowId,
    newWorkflowKey,
    registerDBOSWorkflow,
    registerDBOSWorkflowScoped,
    registerDBOSWorkflowRef,
    registerDBOSWorkflowRefScoped,
    registerQueue,
    resumeWorkflows,
    retrieveWorkflow,
    runDBOSWorkflow,
    runWorkflowStep,
    startChildWorkflow,
    startDBOSWorkflowRef,
    startOptionsDefault,
    runTracer,
    waitForWorkflow,
  )
import Test.Tasty (DependencyType (..), TestTree, dependentTestGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestTree
tests =
  -- Sequential: cases announce through one shared stderr, so parallel
  -- 'printSimTrace' calls would interleave their lines mid-character.
  -- 'AllFinish' keeps every case running on a failure, in order.
  dependentTestGroup
    "Workflow management (Sim)"
    AllFinish
    [ testCase "the management surface needs a launched instance" $ do
        (refused, tr) <- runSimCase $ do
          dbos <- simInstance
          cancelWorkflows dbos [WorkflowId "never-launched"] False
        printSimTrace tr
        case refused of
          Left ErrorNotLaunched {} -> pure ()
          other -> fail ("expected a not-launched refusal, got: " <> show other),
      testCase "cancelling a workflow that does not exist is not an error" $ do
        (cancelled, tr) <- runSimCase $ do
          dbos <- simSayDBOS
          cancelWorkflows dbos [WorkflowId "never-existed"] False
        printSimTrace tr
        cancelled @?= Right [],
      testCase "resuming a workflow that does not exist is an error" $ do
        (resumed, tr) <- runSimCase $ do
          dbos <- simSayDBOS
          resumeWorkflows dbos [WorkflowId "never-existed"] Nothing
        printSimTrace tr
        case resumed of
          Left (ErrorSystemDatabase (SystemDB.NonExistentWorkflow {workflowIds})) ->
            workflowIds @?= ["never-existed"]
          other -> fail ("expected a non-existent-workflow refusal, got: " <> show other),
      testCase "cancelling makes a workflow terminal and leaves it resumable" $ do
        (outcome, tr) <- runSimCase $ do
          dbos <- simInstance
          ran <- newTVarIO (0 :: Int)
          let key = newWorkflowKey "cancellable"
              workflowText = "sim-mgmt-cancel-resume"
              workflowId = WorkflowId workflowText
          ref <- registerIntRef dbos key (countingBody ran)
          exec <- simLaunchWith simTracer dbos
          _ <-
            startWfRefSim
              exec
              ref
              (startOptionsDefault {startWorkflowId = Just workflowText, startQueue = Just (enqueueNew "no-runner-here")})
              (Just (encodeWorkflowValue (0 :: Int)))
          cancelled <- cancelWorkflows dbos [workflowId] False
          handle <- orFail =<< retrieveWfSim dbos workflowId
          status <- statusWfSim handle
          resumed <- resumeWorkflows dbos [workflowId] Nothing
          waited <- waitForWorkflow dbos workflowId
          count <- readTVarIO ran
          pure (cancelled, status, resumed, waited, count)
        printSimTrace tr
        case outcome of
          (cancelled, status, resumed, waited, count) -> do
            cancelled @?= Right [WorkflowId "sim-mgmt-cancel-resume"]
            -- The mock is stateless: the live test reads CANCELLED here.
            status @?= Right (Just Pending)
            resumed @?= Right [WorkflowId "sim-mgmt-cancel-resume"]
            waited @?= Right (AwaitedSucceeded (Just mockOutput) (Just mockSerialization))
            count @?= 0,
      testCase "resuming onto a named queue puts the workflow there" $ do
        (outcome, tr) <- runSimCase $ do
          dbos <- simInstance
          let key = newWorkflowKey "resumable"
              body = echoIntBody
              workflowText = "sim-mgmt-resume-queue"
              workflowId = WorkflowId workflowText
              queueName = "sim-mgmt-queue"
          ref <- registerIntRef dbos key body
          exec <- simLaunchWith simTracer dbos
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          _ <-
            startWfRefSim
              exec
              ref
              (startOptionsDefault {startWorkflowId = Just workflowText, startQueue = Just (enqueueNew "no-runner-here")})
              (Just (encodeWorkflowValue (7 :: Int)))
          _ <- cancelWorkflows dbos [workflowId] False
          resumed <- resumeWorkflows dbos [workflowId] (Just queueName)
          waited <- waitForWorkflow dbos workflowId
          pure (queueRegistered, resumed, waited)
        printSimTrace tr
        case outcome of
          (queueRegistered, resumed, waited) -> do
            assertBool "the queue registered" (isRight queueRegistered)
            resumed @?= Right [WorkflowId "sim-mgmt-resume-queue"]
            waited @?= Right (AwaitedSucceeded (Just mockOutput) (Just mockSerialization)),
      testCase "cancelling a tree reaches the children" $ do
        (outcome, tr) <- runSimCase $ do
          dbos <- simInstance
          let childKey = newWorkflowKey "tree-child"
              parentKey = newWorkflowKey "tree-parent"
              childBody input _ = pure (Right input)
              parentText = "sim-mgmt-tree-parent"
              parentId = WorkflowId parentText
          childRef <- registerIntRef dbos childKey childBody
          _ <- registerTextWorkflow dbos parentKey (treeParentBody childRef)
          exec <- simLaunchWith simTracer dbos
          ran <- runWfSim exec parentKey parentId (Just (encodeWorkflowValue (0 :: Int)))
          childId <- case ran of
            Left err -> throwIO (userError (show err))
            Right (Just output) -> either (throwIO . userError . show) pure (decodeChildId output)
            Right Nothing -> throwIO (userError "the parent recorded no child")
          cancelled <- cancelWorkflows dbos [parentId] True
          handle <- orFail =<< retrieveWfSim dbos childId
          status <- statusWfSim handle
          pure (cancelled, childId, status)
        printSimTrace tr
        case outcome of
          (cancelled, childId, status) -> do
            -- The mock echoes the named id and does not know the tree.
            cancelled @?= Right [WorkflowId "sim-mgmt-tree-parent"]
            assertBool "the child has an id" (childId /= WorkflowId "sim-mgmt-tree-parent")
            status @?= Right (Just Pending),
      testCase "deleting a workflow removes its row" $ do
        (outcome, tr) <- runSimCase $ do
          dbos <- simInstance
          let key = newWorkflowKey "deletable"
              body = echoIntBody
              workflowText = "sim-mgmt-delete"
              workflowId = WorkflowId workflowText
          _ <- registerIntWorkflow dbos key body
          exec <- simLaunchWith simTracer dbos
          _ <- runWfSim exec key workflowId (Just (encodeWorkflowValue (1 :: Int)))
          deleted <- deleteWorkflows dbos [workflowId] True
          handle <- orFail =<< retrieveWfSim dbos workflowId
          status <- statusWfSim handle
          pure (deleted, status)
        printSimTrace tr
        case outcome of
          (deleted, status) -> do
            deleted @?= Right 1
            -- The mock is stateless: the live test reads absence here.
            status @?= Right (Just Pending),
      testCase "a workflow can be retrieved by id" $ do
        (outcome, tr) <- runSimCase $ do
          dbos <- simInstance
          let key = newWorkflowKey "retrievable"
              body = tripleIntBody
              workflowText = "sim-mgmt-retrieve"
              workflowId = WorkflowId workflowText
          _ <- registerIntWorkflow dbos key body
          exec <- simLaunchWith simTracer dbos
          _ <- runWfSim exec key workflowId (Just (encodeWorkflowValue (2 :: Int)))
          handle <- orFail =<< retrieveWfSim dbos workflowId
          status <- statusWfSim handle
          result <- resultWfSim handle
          pure (status, result)
        printSimTrace tr
        case outcome of
          (status, result) -> do
            status @?= Right (Just Pending)
            result @?= Right (Just (SerializedWorkflowValue mockOutput (Just (Serialization mockSerialization)))),
      testCase "forking from the beginning runs the workflow again under a new id" $ do
        (outcome, tr) <- runSimCase $ do
          dbos <- simInstance
          attempts <- newTVarIO (0 :: Int)
          let key = newWorkflowKey "forkable"
              sourceText = "sim-mgmt-fork-source"
              sourceId = WorkflowId sourceText
          _ <- registerIntWorkflow dbos key (forkableBody attempts)
          exec <- simLaunchWith simTracer dbos
          first <- runWfSim exec key sourceId (Just (encodeWorkflowValue (0 :: Int)))
          forked <- forkWorkflows dbos [forkNew sourceText] defaultForkOptions
          waited <- waitForWorkflow dbos sourceId
          count <- readTVarIO attempts
          pure (first, forked, waited, count)
        printSimTrace tr
        case outcome of
          (first, forked, waited, count) -> do
            assertBool "the source was supposed to fail" (isLeft first)
            -- The mock echoes each source id as its fork id.
            forked @?= Right [WorkflowId "sim-mgmt-fork-source"]
            waited @?= Right (AwaitedSucceeded (Just mockOutput) (Just mockSerialization))
            count @?= 1,
      testCase "a fork takes the id and queue it is given" $ do
        (outcome, tr) <- runSimCase $ do
          dbos <- simInstance
          let key = newWorkflowKey "placed"
              body = echoIntBody
              sourceText = "sim-mgmt-fork-placed-source"
              forkedText = "sim-mgmt-fork-placed-fork"
              queueName = "sim-mgmt-fork-queue"
              sourceId = WorkflowId sourceText
          _ <- registerIntWorkflow dbos key body
          exec <- simLaunchWith simTracer dbos
          _ <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          _ <- runWfSim exec key sourceId (Just (encodeWorkflowValue (4 :: Int)))
          forked <-
            forkWorkflows
              dbos
              [(forkNew sourceText) {forkForkedId = Just forkedText}]
              (defaultForkOptions {forkOptionsQueueName = Just queueName})
          waited <- waitForWorkflow dbos sourceId
          pure (forked, waited)
        printSimTrace tr
        case outcome of
          (forked, waited) -> do
            -- The mock echoes source ids and ignores the chosen one.
            forked @?= Right [WorkflowId "sim-mgmt-fork-placed-source"]
            waited @?= Right (AwaitedSucceeded (Just mockOutput) (Just mockSerialization)),
      testCase "forking from a chosen step replays the steps below it" $ do
        (outcome, tr) <- runSimCase $ do
          dbos <- simInstance
          ran <- newTVarIO ([] :: [Text])
          let key = newWorkflowKey "staged"
              sourceText = "sim-mgmt-fork-step-source"
              sourceId = WorkflowId sourceText
          _ <- registerIntWorkflow dbos key (stagedBody ran)
          exec <- simLaunchWith simTracer dbos
          first <- runWfSim exec key sourceId (Just (encodeWorkflowValue (0 :: Int)))
          forked <- forkFrom dbos [sourceId] (ForkStep 1) defaultForkOptions
          waited <- waitForWorkflow dbos sourceId
          names <- readTVarIO ran
          pure (first, forked, waited, names)
        printSimTrace tr
        case outcome of
          (first, forked, waited, names) -> do
            assertBool "the source ran" (isRight first)
            forked @?= Right [WorkflowId "sim-mgmt-fork-step-source"]
            waited @?= Right (AwaitedSucceeded (Just mockOutput) (Just mockSerialization))
            -- The mock records no history, so nothing replays.
            names @?= ["one", "two", "three"],
      testCase "management announces through its tracer" $ do
        (_, tr) <- runSimCase demoTrace
        printSimTrace tr
        selectTraceEventsDynamic tr
          @?= [ WorkflowsCancelled 2,
                WorkflowsResumed 3 2,
                WorkflowForked "sim-mgmt-fork-1",
                WorkflowsForked 0,
                WorkflowsDeleted 1,
                WorkflowDelayMoveAsked "sim-mgmt-delayed",
                WorkflowAttributesReplaceAsked "sim-mgmt-attributed"
              ]
    ]

-- | One of every management announcement, through the say-carrier: the
-- sim half of the live FastLogger lines.
demoTrace :: forall s. IOSim s ()
demoTrace = do
  runTracer simTracer (WorkflowsCancelled 2)
  runTracer simTracer (WorkflowsResumed 3 2)
  runTracer simTracer (WorkflowForked "sim-mgmt-fork-1")
  runTracer simTracer (WorkflowsForked 0)
  runTracer simTracer (WorkflowsDeleted 1)
  runTracer simTracer (WorkflowDelayMoveAsked "sim-mgmt-delayed")
  runTracer simTracer (WorkflowAttributesReplaceAsked "sim-mgmt-attributed")

-- | The sim case bodies, top-level so their rank-2 signatures can name
-- the simulation: captured state and refs arrive as parameters.
echoIntBody :: forall exec s. Int -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
echoIntBody input _ = pure (Right input)

tripleIntBody :: forall exec s. Int -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
tripleIntBody input _ = pure (Right (input * 3))

countingBody :: forall s. StrictTVar (IOSim s) Int -> forall exec. Int -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
countingBody ran input _ = do
  atomically (modifyTVar ran (+ 1))
  pure (Right (input + 5))

forkableBody :: forall s. StrictTVar (IOSim s) Int -> forall exec. Int -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
forkableBody attempts _ _ = do
  attempt <- readTVarIO attempts
  atomically (modifyTVar attempts (+ 1))
  if attempt == 0
    then pure (Left (ErrorConfig "the first attempt fails"))
    else pure (Right 8)

stagedBody :: forall s. StrictTVar (IOSim s) [Text] -> forall exec. Int -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)
stagedBody ran _ wctx = do
  let ctx = workflowCtxInner wctx
  outcomes <-
    mapM
      (\name -> runWorkflowStep ctx name (const (atomically (modifyTVar ran (<> [name])) >> pure (0 :: Int))))
      ["one", "two", "three"]
  pure (fmap (const 0) (sequence outcomes))

treeParentBody :: forall s. WorkflowRef (IOSim s) EngineOnly -> forall exec. Int -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Text)
treeParentBody childRef _ wctx = do
  let ctx = workflowCtxInner wctx
  started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
  pure (fmap handleWorkflowId started)

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through. Local
-- copies are deliberate: this module carries only the aliases it uses.
runWfSim :: Executor (IOSim s) -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> IOSim s (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWfSim = runDBOSWorkflow

startWfRefSim :: Executor (IOSim s) -> WorkflowRef (IOSim s) EngineOnly -> StartOptions -> Maybe SerializedWorkflowValue -> IOSim s (Either (Error EngineOnly) (WorkflowHandle (IOSim s) EngineOnly))
startWfRefSim = startDBOSWorkflowRef

retrieveWfSim :: DBOS (IOSim s) -> WorkflowId -> IOSim s (Either (Error EngineOnly) (WorkflowHandle (IOSim s) EngineOnly))
retrieveWfSim = retrieveWorkflow

resultWfSim :: WorkflowHandle (IOSim s) EngineOnly -> IOSim s (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWfSim = handleResult

statusWfSim :: WorkflowHandle (IOSim s) EngineOnly -> IOSim s (Either (Error EngineOnly) (Maybe WorkflowStatus))
statusWfSim = handleStatus

-- | The sim-side registration aliases: a locally defined body has no
-- signature, so the channel's @e@ stays ambiguous; these pin it while
-- leaving @s@ universally quantified.
registerWfSim :: (FromJSON a, ToJSON r) => DBOS (IOSim s) -> WorkflowKey -> (forall exec. a -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) r)) -> IOSim s (Either (Error EngineOnly) ())
registerWfSim = registerDBOSWorkflowScoped

registerWfRefSim :: (FromJSON a, ToJSON r) => DBOS (IOSim s) -> WorkflowKey -> (forall exec. a -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) r)) -> IOSim s (Either (Error EngineOnly) (WorkflowRef (IOSim s) EngineOnly))
registerWfRefSim = registerDBOSWorkflowRefScoped

-- * Helpers

-- | A launched sim instance whose engine calls announce through the
-- say-carrier: the cases' 'ManagementEvent' lines print inline.
simSayDBOS :: IOSim s (DBOS (IOSim s))
simSayDBOS = simDBOSWith simTracer

-- | Register an @Int -> Int@ body under IOSim, pinning the JSON types the
-- polymorphic registration cannot infer from a local binding.
registerIntRef :: DBOS (IOSim s) -> WorkflowKey -> (forall exec. Int -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)) -> IOSim s (WorkflowRef (IOSim s) EngineOnly)
registerIntRef dbos key body = orFail =<< registerWfRefSim dbos key body

registerIntWorkflow :: DBOS (IOSim s) -> WorkflowKey -> (forall exec. Int -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Int)) -> IOSim s ()
registerIntWorkflow dbos key body = orFail =<< registerWfSim dbos key body

registerTextWorkflow :: DBOS (IOSim s) -> WorkflowKey -> (forall exec. Int -> WorkflowCtx exec (IOSim s) -> IOSim s (Either (Error EngineOnly) Text)) -> IOSim s ()
registerTextWorkflow dbos key body = orFail =<< registerWfSim dbos key body

orFail :: Either (Error EngineOnly) a -> IOSim s a
orFail result = case result of
  Left err -> throwIO (userError (show err))
  Right value -> pure value

decodeChildId :: SerializedWorkflowValue -> Either CodecError WorkflowId
decodeChildId output = WorkflowId <$> decodeWorkflowValue "result" (Just output)
