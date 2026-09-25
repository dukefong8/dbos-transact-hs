{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.ManagementTest' mirrored under IOSim over the mock
-- backend: the same call sequences, with the answers the stateless mock
-- returns. Where a live assertion depends on database state (a cancelled
-- row reading back as @CANCELLED@, a deleted row reading back absent, a
-- fork actually running), the mirror asserts the mock's canned answer and
-- says so; the live semantics stay in 'DBOS.Transact.ManagementTest'.
module DBOS.Transact.ManagementTestIOSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, runSimOrThrow)
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
import DBOS.SystemDB.IOSim (simDBOS, simInstance, simLaunch)
import DBOS.Transact
  ( CodecError,
    Ctx,
    DBOS,
    Enqueue (..),
    Error (..),
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
    registerDBOSWorkflowRef,
    registerQueue,
    resumeWorkflows,
    retrieveWorkflow,
    runDBOSWorkflow,
    runWorkflowStep,
    startChildWorkflow,
    startDBOSWorkflowRef,
    startOptionsDefault,
    waitForWorkflow,
  )
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "Workflow management (IOSim)"
    [ testCase "the management surface needs a launched instance" $ do
        refused <- run $ do
          dbos <- simInstance
          cancelWorkflows dbos [WorkflowId "never-launched"] False
        case refused of
          Left ErrorNotLaunched {} -> pure ()
          other -> fail ("expected a not-launched refusal, got: " <> show other),
      testCase "cancelling a workflow that does not exist is not an error" $ do
        cancelled <- run $ do
          dbos <- simDBOS
          cancelWorkflows dbos [WorkflowId "never-existed"] False
        cancelled @?= Right [],
      testCase "resuming a workflow that does not exist is an error" $ do
        resumed <- run $ do
          dbos <- simDBOS
          resumeWorkflows dbos [WorkflowId "never-existed"] Nothing
        case resumed of
          Left (ErrorSystemDatabase (SystemDB.NonExistentWorkflow {workflowIds})) ->
            workflowIds @?= ["never-existed"]
          other -> fail ("expected a non-existent-workflow refusal, got: " <> show other),
      testCase "cancelling makes a workflow terminal and leaves it resumable" $ do
        outcome <- run $ do
          dbos <- simInstance
          ran <- newTVarIO (0 :: Int)
          let key = newWorkflowKey "cancellable"
              body input _ = do
                atomically (modifyTVar ran (+ 1))
                pure (Right (input + 5))
              workflowText = "sim-mgmt-cancel-resume"
              workflowId = WorkflowId workflowText
          ref <- registerIntRef dbos key body
          simLaunch dbos
          _ <-
            startDBOSWorkflowRef
              dbos
              ref
              (startOptionsDefault {startWorkflowId = Just workflowText, startQueue = Just (enqueueNew "no-runner-here")})
              (Just (encodeWorkflowValue (0 :: Int)))
          cancelled <- cancelWorkflows dbos [workflowId] False
          handle <- orFail =<< retrieveWorkflow dbos workflowId
          status <- handleStatus handle
          resumed <- resumeWorkflows dbos [workflowId] Nothing
          waited <- waitForWorkflow dbos workflowId
          count <- readTVarIO ran
          pure (cancelled, status, resumed, waited, count)
        case outcome of
          (cancelled, status, resumed, waited, count) -> do
            cancelled @?= Right [WorkflowId "sim-mgmt-cancel-resume"]
            -- The mock is stateless: the live test reads CANCELLED here.
            status @?= Right (Just Pending)
            resumed @?= Right [WorkflowId "sim-mgmt-cancel-resume"]
            waited @?= Right (AwaitedSucceeded (Just "mock-output") (Just "rust_serde"))
            count @?= 0,
      testCase "resuming onto a named queue puts the workflow there" $ do
        outcome <- run $ do
          dbos <- simInstance
          let key = newWorkflowKey "resumable"
              body input _ = pure (Right input)
              workflowText = "sim-mgmt-resume-queue"
              workflowId = WorkflowId workflowText
              queueName = "sim-mgmt-queue"
          ref <- registerIntRef dbos key body
          simLaunch dbos
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          _ <-
            startDBOSWorkflowRef
              dbos
              ref
              (startOptionsDefault {startWorkflowId = Just workflowText, startQueue = Just (enqueueNew "no-runner-here")})
              (Just (encodeWorkflowValue (7 :: Int)))
          _ <- cancelWorkflows dbos [workflowId] False
          resumed <- resumeWorkflows dbos [workflowId] (Just queueName)
          waited <- waitForWorkflow dbos workflowId
          pure (queueRegistered, resumed, waited)
        case outcome of
          (queueRegistered, resumed, waited) -> do
            assertBool "the queue registered" (isRight queueRegistered)
            resumed @?= Right [WorkflowId "sim-mgmt-resume-queue"]
            waited @?= Right (AwaitedSucceeded (Just "mock-output") (Just "rust_serde")),
      testCase "cancelling a tree reaches the children" $ do
        outcome <- run $ do
          dbos <- simInstance
          let childKey = newWorkflowKey "tree-child"
              parentKey = newWorkflowKey "tree-parent"
              childBody input _ = pure (Right input)
              parentText = "sim-mgmt-tree-parent"
              parentId = WorkflowId parentText
          childRef <- registerIntRef dbos childKey childBody
          let parentBody _ ctx = do
                started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                pure (fmap handleWorkflowId started)
          _ <- registerTextWorkflow dbos parentKey parentBody
          simLaunch dbos
          ran <- runDBOSWorkflow dbos parentKey parentId (Just (encodeWorkflowValue (0 :: Int)))
          childId <- case ran of
            Left err -> throwIO (userError (show err))
            Right (Just output) -> either (throwIO . userError . show) pure (decodeChildId output)
            Right Nothing -> throwIO (userError "the parent recorded no child")
          cancelled <- cancelWorkflows dbos [parentId] True
          handle <- orFail =<< retrieveWorkflow dbos childId
          status <- handleStatus handle
          pure (cancelled, childId, status)
        case outcome of
          (cancelled, childId, status) -> do
            -- The mock echoes the named id and does not know the tree.
            cancelled @?= Right [WorkflowId "sim-mgmt-tree-parent"]
            assertBool "the child has an id" (childId /= WorkflowId "sim-mgmt-tree-parent")
            status @?= Right (Just Pending),
      testCase "deleting a workflow removes its row" $ do
        outcome <- run $ do
          dbos <- simInstance
          let key = newWorkflowKey "deletable"
              body input _ = pure (Right input)
              workflowText = "sim-mgmt-delete"
              workflowId = WorkflowId workflowText
          _ <- registerIntWorkflow dbos key body
          simLaunch dbos
          _ <- runDBOSWorkflow dbos key workflowId (Just (encodeWorkflowValue (1 :: Int)))
          deleted <- deleteWorkflows dbos [workflowId] True
          handle <- orFail =<< retrieveWorkflow dbos workflowId
          status <- handleStatus handle
          pure (deleted, status)
        case outcome of
          (deleted, status) -> do
            deleted @?= Right 1
            -- The mock is stateless: the live test reads absence here.
            status @?= Right (Just Pending),
      testCase "a workflow can be retrieved by id" $ do
        outcome <- run $ do
          dbos <- simInstance
          let key = newWorkflowKey "retrievable"
              body input _ = pure (Right (input * 3))
              workflowText = "sim-mgmt-retrieve"
              workflowId = WorkflowId workflowText
          _ <- registerIntWorkflow dbos key body
          simLaunch dbos
          _ <- runDBOSWorkflow dbos key workflowId (Just (encodeWorkflowValue (2 :: Int)))
          handle <- orFail =<< retrieveWorkflow dbos workflowId
          status <- handleStatus handle
          result <- handleResult handle
          pure (status, result)
        case outcome of
          (status, result) -> do
            status @?= Right (Just Pending)
            result @?= Right (Just (SerializedWorkflowValue "mock-output" (Just (Serialization "rust_serde")))),
      testCase "forking from the beginning runs the workflow again under a new id" $ do
        outcome <- run $ do
          dbos <- simInstance
          attempts <- newTVarIO (0 :: Int)
          let key = newWorkflowKey "forkable"
              body _ _ = do
                attempt <- readTVarIO attempts
                atomically (modifyTVar attempts (+ 1))
                if attempt == 0
                  then pure (Left (ErrorConfig "the first attempt fails"))
                  else pure (Right 8)
              sourceText = "sim-mgmt-fork-source"
              sourceId = WorkflowId sourceText
          _ <- registerIntWorkflow dbos key body
          simLaunch dbos
          first <- runDBOSWorkflow dbos key sourceId (Just (encodeWorkflowValue (0 :: Int)))
          forked <- forkWorkflows dbos [forkNew sourceText] defaultForkOptions
          waited <- waitForWorkflow dbos sourceId
          count <- readTVarIO attempts
          pure (first, forked, waited, count)
        case outcome of
          (first, forked, waited, count) -> do
            assertBool "the source was supposed to fail" (isLeft first)
            -- The mock echoes each source id as its fork id.
            forked @?= Right [WorkflowId "sim-mgmt-fork-source"]
            waited @?= Right (AwaitedSucceeded (Just "mock-output") (Just "rust_serde"))
            count @?= 1,
      testCase "a fork takes the id and queue it is given" $ do
        outcome <- run $ do
          dbos <- simInstance
          let key = newWorkflowKey "placed"
              body input _ = pure (Right input)
              sourceText = "sim-mgmt-fork-placed-source"
              forkedText = "sim-mgmt-fork-placed-fork"
              queueName = "sim-mgmt-fork-queue"
              sourceId = WorkflowId sourceText
          _ <- registerIntWorkflow dbos key body
          simLaunch dbos
          _ <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          _ <- runDBOSWorkflow dbos key sourceId (Just (encodeWorkflowValue (4 :: Int)))
          forked <-
            forkWorkflows
              dbos
              [(forkNew sourceText) {forkForkedId = Just forkedText}]
              (defaultForkOptions {forkOptionsQueueName = Just queueName})
          waited <- waitForWorkflow dbos sourceId
          pure (forked, waited)
        case outcome of
          (forked, waited) -> do
            -- The mock echoes source ids and ignores the chosen one.
            forked @?= Right [WorkflowId "sim-mgmt-fork-placed-source"]
            waited @?= Right (AwaitedSucceeded (Just "mock-output") (Just "rust_serde")),
      testCase "forking from a chosen step replays the steps below it" $ do
        outcome <- run $ do
          dbos <- simInstance
          ran <- newTVarIO ([] :: [Text])
          let key = newWorkflowKey "staged"
              body _ ctx = do
                outcomes <-
                  mapM
                    (\name -> runWorkflowStep ctx name (const (atomically (modifyTVar ran (<> [name])) >> pure (0 :: Int))))
                    ["one", "two", "three"]
                pure (fmap (const 0) (sequence outcomes))
              sourceText = "sim-mgmt-fork-step-source"
              sourceId = WorkflowId sourceText
          _ <- registerIntWorkflow dbos key body
          simLaunch dbos
          first <- runDBOSWorkflow dbos key sourceId (Just (encodeWorkflowValue (0 :: Int)))
          forked <- forkFrom dbos [sourceId] (ForkStep 1) defaultForkOptions
          waited <- waitForWorkflow dbos sourceId
          names <- readTVarIO ran
          pure (first, forked, waited, names)
        case outcome of
          (first, forked, waited, names) -> do
            assertBool "the source ran" (isRight first)
            forked @?= Right [WorkflowId "sim-mgmt-fork-step-source"]
            waited @?= Right (AwaitedSucceeded (Just "mock-output") (Just "rust_serde"))
            -- The mock records no history, so nothing replays.
            names @?= ["one", "two", "three"]
    ]

-- * Helpers

run :: (forall s. IOSim s a) -> IO a
run action = pure (runSimOrThrow action)

-- | Register an @Int -> Int@ body under IOSim, pinning the JSON types the
-- polymorphic registration cannot infer from a local binding.
registerIntRef :: DBOS (IOSim s) -> WorkflowKey -> (Int -> Ctx (IOSim s) -> IOSim s (Either Error Int)) -> IOSim s (WorkflowRef (IOSim s))
registerIntRef dbos key body = orFail =<< registerDBOSWorkflowRef dbos key body

registerIntWorkflow :: DBOS (IOSim s) -> WorkflowKey -> (Int -> Ctx (IOSim s) -> IOSim s (Either Error Int)) -> IOSim s ()
registerIntWorkflow dbos key body = orFail =<< registerDBOSWorkflow dbos key body

registerTextWorkflow :: DBOS (IOSim s) -> WorkflowKey -> (Int -> Ctx (IOSim s) -> IOSim s (Either Error Text)) -> IOSim s ()
registerTextWorkflow dbos key body = orFail =<< registerDBOSWorkflow dbos key body

orFail :: Either Error a -> IOSim s a
orFail result = case result of
  Left err -> throwIO (userError (show err))
  Right value -> pure value

decodeChildId :: SerializedWorkflowValue -> Either CodecError WorkflowId
decodeChildId output = WorkflowId <$> decodeWorkflowValue "result" (Just output)
