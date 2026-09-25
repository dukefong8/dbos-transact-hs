{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Engine-level management behavior, mirroring the Rust
-- @tests/management.rs@ cases the ported surface can express: cancel
-- (bulk, tree, missing), resume (missing, named queue, internal queue),
-- delete, fork (beginning, chosen step, chosen id and queue), and retrieve.
-- The backend halves of these calls live in 'DBOS.SystemDB.PostgresTest';
-- this group is the launched-instance behavior.
module DBOS.Transact.ManagementTest (tests) where

import DBOS.Prelude
import Data.Aeson (FromJSON, ToJSON)
import Data.Either (isLeft)
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
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
import DBOS.Transact
  ( CodecError,
    Config (..),
    DBOS,
    Ctx,
    Enqueue (..),
    Environment (..),
    Error (..),
    QueueConflict (..),
    StartOptions (..),
    WorkflowHandle,
    WorkflowKey,
    WorkflowRef,
    cancelWorkflows,
    configFromEnv,
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
    launchWithEnvironment,
    newDBOS,
    newWorkflowKey,
    registerDBOSWorkflow,
    registerDBOSWorkflowRef,
    registerQueue,
    resumeWorkflows,
    retrieveWorkflow,
    runDBOSWorkflow,
    runWorkflowStep,
    shutdown,
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
    "Workflow management"
    [ testCase "the management surface needs a launched instance" $ do
        suffix <- freshSuffix
        base <- configFromEnv ("hs-l2-mgmt-unlaunched-" <> Text.take 16 suffix)
        dbos <- newDBOS base
        refused <- cancelWorkflows dbos [WorkflowId "never-launched"] False
        case refused of
          Left ErrorNotLaunched {} -> pure ()
          other -> fail ("expected a not-launched refusal, got: " <> show other),
      testCase "cancelling a workflow that does not exist is not an error" $ do
        (dbos, _suffix) <- instanceFor "mgmt-cancel-missing"
        launchOrFail dbos
        cancelled <- cancelWorkflows dbos [WorkflowId "never-existed"] False
        assertEqual "a missing row cancels to nothing" (Right []) cancelled,
      testCase "resuming a workflow that does not exist is an error" $ do
        (dbos, _suffix) <- instanceFor "mgmt-resume-missing"
        launchOrFail dbos
        resumed <- resumeWorkflows dbos [WorkflowId "never-existed"] Nothing
        case resumed of
          Left (ErrorSystemDatabase (SystemDB.NonExistentWorkflow {workflowIds})) ->
            workflowIds @?= ["never-existed"]
          other -> fail ("expected a non-existent-workflow refusal, got: " <> show other),
      testCase "cancelling makes a workflow terminal and leaves it resumable" $ do
        (dbos, suffix) <- instanceFor "mgmt-cancel-resume"
        ran <- newIORef (0 :: Int)
        let key = newWorkflowKey "cancellable"
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body input _ = do
              modifyIORef' ran (+ 1)
              pure (Right (input + 5))
            workflowText = "hs-l2-mgmt-cancel-resume-" <> suffix
            workflowId = WorkflowId workflowText
        ref <- registerRefOrFail dbos key body
        launchOrFail dbos
        started <-
          startDBOSWorkflowRef
            dbos
            ref
            (startOptionsDefault {startWorkflowId = Just workflowText, startQueue = Just (enqueueNew "no-runner-here")})
            (Just (encodeWorkflowValue (0 :: Int)))
        case started of
          Left err -> fail (show err)
          Right _ -> pure ()
        cancelled <- cancelWorkflows dbos [workflowId] False
        assertEqual "the cancel reports the id it moved" (Right [workflowId]) cancelled
        handle <- retrieveOrFail dbos workflowId
        status <- handleStatus handle
        assertEqual "the row is terminal" (Right (Just Cancelled)) status
        assertEqual "a cancelled workflow did not run" 0 =<< readIORef ran
        resumed <- resumeWorkflows dbos [workflowId] Nothing
        case resumed of
          Left err -> fail (show err)
          Right ids -> ids @?= [workflowId]
        waited <- waitForWorkflow dbos workflowId
        case waited of
          Right (AwaitedSucceeded (Just output) _) -> decodeResult output @?= Right (5 :: Int)
          other -> fail ("expected the resumed workflow's result, got: " <> show other)
        assertEqual "the resumed workflow ran once" 1 =<< readIORef ran,
      testCase "resuming onto a named queue puts the workflow there" $ do
        (dbos, suffix) <- instanceFor "mgmt-resume-queue"
        let queueName = "hs-l2-mgmt-queue-" <> suffix
            key = newWorkflowKey "resumable"
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body input _ = pure (Right input)
            workflowText = "hs-l2-mgmt-resume-queue-" <> suffix
            workflowId = WorkflowId workflowText
        ref <- registerRefOrFail dbos key body
        launchOrFail dbos
        queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
        case queueRegistered of
          Left err -> fail (show err)
          Right _ -> pure ()
        started <-
          startDBOSWorkflowRef
            dbos
            ref
            (startOptionsDefault {startWorkflowId = Just workflowText, startQueue = Just (enqueueNew "no-runner-here")})
            (Just (encodeWorkflowValue (7 :: Int)))
        case started of
          Left err -> fail (show err)
          Right _ -> pure ()
        _ <- cancelWorkflows dbos [workflowId] False
        resumed <- resumeWorkflows dbos [workflowId] (Just queueName)
        case resumed of
          Left err -> fail (show err)
          Right _ -> pure ()
        waited <- waitForWorkflow dbos workflowId
        case waited of
          Right (AwaitedSucceeded (Just output) _) -> decodeResult output @?= Right (7 :: Int)
          other -> fail ("expected the queued resume to run, got: " <> show other),
      testCase "cancelling a tree reaches the children" $ do
        (dbos, suffix) <- instanceFor "mgmt-cancel-tree"
        let childKey = newWorkflowKey "tree-child"
            parentKey = newWorkflowKey "tree-parent"
            childBody :: Int -> Ctx IO -> IO (Either Error Int)
            childBody input _ = pure (Right input)
            parentText = "hs-l2-mgmt-tree-parent-" <> suffix
            parentId = WorkflowId parentText
        childRef <- registerRefOrFail dbos childKey childBody
        let parentBody :: Int -> Ctx IO -> IO (Either Error Text)
            parentBody _ ctx = do
              started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
              pure $ case started of
                Left err -> Left err
                Right handle -> Right (workflowTextOf handle)
        registered <- registerDBOSWorkflow dbos parentKey parentBody
        case registered of
          Left err -> fail (show err)
          Right () -> pure ()
        launchOrFail dbos
        ran <- runDBOSWorkflow dbos parentKey parentId (Just (encodeWorkflowValue (0 :: Int)))
        childId <- case ran of
          Left err -> fail (show err)
          Right (Just output) -> case decodeSerializedChildId output of
            Left err -> fail (show err)
            Right childText -> pure (WorkflowId childText)
          Right Nothing -> fail "the parent recorded no child"
        cancelled <- cancelWorkflows dbos [parentId] True
        case cancelled of
          Left err -> fail (show err)
          Right ids -> assertBool "the tree cancel names the child" (childId `elem` ids)
        handle <- retrieveOrFail dbos childId
        status <- handleStatus handle
        assertEqual "the child is terminal" (Right (Just Cancelled)) status,
      testCase "deleting a workflow removes its row" $ do
        (dbos, suffix) <- instanceFor "mgmt-delete"
        let key = newWorkflowKey "deletable"
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body input ctx = runWorkflowStep ctx "work" (const (pure input))
            workflowText = "hs-l2-mgmt-delete-" <> suffix
            workflowId = WorkflowId workflowText
        registered <- registerDBOSWorkflow dbos key body
        case registered of
          Left err -> fail (show err)
          Right () -> pure ()
        launchOrFail dbos
        ran <- runDBOSWorkflow dbos key workflowId (Just (encodeWorkflowValue (1 :: Int)))
        case ran of
          Left err -> fail (show err)
          Right _ -> pure ()
        deleted <- deleteWorkflows dbos [workflowId] True
        assertEqual "one row was deleted" (Right 1) deleted
        handle <- retrieveOrFail dbos workflowId
        status <- handleStatus handle
        assertEqual "the row is gone" (Right Nothing) status,
      testCase "a workflow can be retrieved by id" $ do
        (dbos, suffix) <- instanceFor "mgmt-retrieve"
        let key = newWorkflowKey "retrievable"
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body input _ = pure (Right (input * 3))
            workflowText = "hs-l2-mgmt-retrieve-" <> suffix
            workflowId = WorkflowId workflowText
        registered <- registerDBOSWorkflow dbos key body
        case registered of
          Left err -> fail (show err)
          Right () -> pure ()
        launchOrFail dbos
        ran <- runDBOSWorkflow dbos key workflowId (Just (encodeWorkflowValue (2 :: Int)))
        case ran of
          Left err -> fail (show err)
          Right _ -> pure ()
        handle <- retrieveOrFail dbos workflowId
        status <- handleStatus handle
        assertEqual "the retrieved row reports success" (Right (Just Success)) status
        result <- handleResult handle
        case result of
          Right (Just output) -> decodeSerializedResult output @?= Right (6 :: Int)
          other -> fail ("expected the retrieved result, got: " <> show other),
      testCase "forking from the beginning runs the workflow again under a new id" $ do
        (dbos, suffix) <- instanceFor "mgmt-fork-beginning"
        attempts <- newIORef (0 :: Int)
        let key = newWorkflowKey "forkable"
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body _ _ = do
              attempt <- readIORef attempts
              modifyIORef' attempts (+ 1)
              if attempt == 0
                then pure (Left (ErrorConfig "the first attempt fails"))
                else pure (Right 8)
            sourceText = "hs-l2-mgmt-fork-source-" <> suffix
            sourceId = WorkflowId sourceText
        registered <- registerDBOSWorkflow dbos key body
        case registered of
          Left err -> fail (show err)
          Right () -> pure ()
        launchOrFail dbos
        first <- runDBOSWorkflow dbos key sourceId (Just (encodeWorkflowValue (0 :: Int)))
        assertBool "the source was supposed to fail" (isLeft first)
        forked <- forkWorkflows dbos [forkNew sourceText] defaultForkOptions
        forkedIds <- case forked of
          Left err -> fail (show err)
          Right ids -> pure ids
        case forkedIds of
          [forkedId] -> do
            assertBool "the fork got its own id" (forkedId /= sourceId)
            waited <- waitForWorkflow dbos forkedId
            case waited of
              Right (AwaitedSucceeded (Just output) _) -> decodeResult output @?= Right (8 :: Int)
              other -> fail ("expected the fork to run, got: " <> show other)
          other -> fail ("expected exactly one fork, got: " <> show other)
        sourceHandle <- retrieveOrFail dbos sourceId
        sourceStatus <- handleStatus sourceHandle
        assertEqual "the source keeps its outcome" (Right (Just Error)) sourceStatus,
      testCase "a fork takes the id and queue it is given" $ do
        (dbos, suffix) <- instanceFor "mgmt-fork-placed"
        let key = newWorkflowKey "placed"
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body input _ = pure (Right input)
            sourceText = "hs-l2-mgmt-fork-placed-source-" <> suffix
            forkedText = "hs-l2-mgmt-fork-placed-fork-" <> suffix
            queueName = "hs-l2-mgmt-fork-queue-" <> suffix
            sourceId = WorkflowId sourceText
        registered <- registerDBOSWorkflow dbos key body
        case registered of
          Left err -> fail (show err)
          Right () -> pure ()
        launchOrFail dbos
        queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
        case queueRegistered of
          Left err -> fail (show err)
          Right _ -> pure ()
        ran <- runDBOSWorkflow dbos key sourceId (Just (encodeWorkflowValue (4 :: Int)))
        case ran of
          Left err -> fail (show err)
          Right _ -> pure ()
        forked <-
          forkWorkflows
            dbos
            [(forkNew sourceText) {forkForkedId = Just forkedText}]
            (defaultForkOptions {forkOptionsQueueName = Just queueName})
        case forked of
          Left err -> fail (show err)
          Right ids -> ids @?= [WorkflowId forkedText]
        waited <- waitForWorkflow dbos (WorkflowId forkedText)
        case waited of
          Right (AwaitedSucceeded (Just output) _) -> decodeResult output @?= Right (4 :: Int)
          other -> fail ("expected the placed fork to run, got: " <> show other),
      testCase "forking from a chosen step replays the steps below it" $ do
        (dbos, suffix) <- instanceFor "mgmt-fork-step"
        ran <- newIORef ([] :: [Text])
        let key = newWorkflowKey "staged"
            body :: Int -> Ctx IO -> IO (Either Error Int)
            body _ ctx = do
              outcomes <-
                mapM
                  (\name -> runWorkflowStep ctx name (const (modifyIORef' ran (<> [name]) >> pure (0 :: Int))))
                  ["one", "two", "three"]
              pure (fmap (const 0) (sequence outcomes))
            sourceText = "hs-l2-mgmt-fork-step-source-" <> suffix
            sourceId = WorkflowId sourceText
        registered <- registerDBOSWorkflow dbos key body
        case registered of
          Left err -> fail (show err)
          Right () -> pure ()
        launchOrFail dbos
        first <- runDBOSWorkflow dbos key sourceId (Just (encodeWorkflowValue (0 :: Int)))
        case first of
          Left err -> fail (show err)
          Right _ -> pure ()
        readIORef ran >>= (@?= ["one", "two", "three"])
        writeIORef ran []
        forked <- forkFrom dbos [sourceId] (ForkStep 1) defaultForkOptions
        forkedIds <- case forked of
          Left err -> fail (show err)
          Right ids -> pure ids
        case forkedIds of
          [forkedId] -> do
            waited <- waitForWorkflow dbos forkedId
            case waited of
              Right (AwaitedSucceeded _ _) -> pure ()
              other -> fail ("expected the fork to run, got: " <> show other)
            readIORef ran >>= (@?= ["two", "three"])
          other -> fail ("expected exactly one fork, got: " <> show other)
    ]

-- * Helpers

freshSuffix :: IO Text
freshSuffix = Text.pack . UUID.toString <$> UUID.V4.nextRandom

instanceFor :: Text -> IO (DBOS IO, Text)
instanceFor label = do
  suffix <- freshSuffix
  base <- configFromEnv (("hs-l2-" <> label <> "-") <> Text.take 16 suffix)
  let config = base {configAppVersion = Just ("hs-l2-version-" <> suffix), configExecutorId = Just ("hs-l2-executor-" <> suffix)}
  dbos <- newDBOS config
  pure (dbos, suffix)

launchOrFail :: DBOS IO -> IO ()
launchOrFail dbos = do
  started <- launchWithEnvironment dbos isolatedEnvironment
  either (fail . show) pure started

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }

registerRefOrFail :: (FromJSON argument, ToJSON result) => DBOS IO -> WorkflowKey -> (argument -> Ctx IO -> IO (Either Error result)) -> IO (WorkflowRef IO)
registerRefOrFail dbos key body = do
  registered <- registerDBOSWorkflowRef dbos key body
  either (fail . show) pure registered

retrieveOrFail :: DBOS IO -> WorkflowId -> IO (WorkflowHandle IO)
retrieveOrFail dbos workflowId = do
  retrieved <- retrieveWorkflow dbos workflowId
  either (fail . show) pure retrieved

decodeResult :: Text -> Either CodecError Int
decodeResult output = decodeWorkflowValue "result" (Just (SerializedWorkflowValue output Nothing))

decodeSerializedResult :: SerializedWorkflowValue -> Either CodecError Int
decodeSerializedResult output = decodeWorkflowValue "result" (Just output)

decodeSerializedChildId :: SerializedWorkflowValue -> Either CodecError Text
decodeSerializedChildId output = decodeWorkflowValue "result" (Just output)

workflowTextOf :: WorkflowHandle IO -> Text
workflowTextOf handle = handleWorkflowId handle
