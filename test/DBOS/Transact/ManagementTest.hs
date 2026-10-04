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
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
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
    NewWorkflow (..),
    SerializedWorkflowValue (..),
    StepRecord (..),
    Submission (..),
    WorkflowDelay (..),
    WorkflowFilter (..),
    WorkflowId (..),
    WorkflowRecord (..),
    WorkflowStatus (..),
    defaultForkOptions,
    defaultWorkflowFilter,
    forkNew,
    getWorkflow,
    newWorkflow,
    secondsDuration,
  )
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  (
    EngineOnly,
    CodecError,
    Client (..),
    ClientConfig (..),
    Config (..),
    DBOS,
    Executor,
    Ctx,
    WorkflowCtx,
    workflowCtxInner,
    Enqueue (..),
    Environment (..),
    Error (..),
    QueueConflict (..),
    StartOptions (..),
    WorkflowHandle,
    WorkflowKey,
    WorkflowRef,
    acquireLoggerBackend,
    cancelWorkflows,
    cancelWorkflowsInWorkflow,
    clientCancelWorkflows,
    clientConfigFromEnv,
    closeClient,
    configFromEnv,
    connectClient,
    decodeWorkflowValue,
    defaultQueueOptions,
    deleteWorkflows,
    deleteWorkflowsInWorkflow,
    encodeWorkflowValue,
    enqueueDBOSWorkflow,
    enqueueNew,
    forkFrom,
    forkFromInWorkflow,
    forkWorkflows,
    forkWorkflowsInWorkflow,
    handleResult,
    handleStatus,
    handleWorkflowId,
    ioTracer,
    launchWithEnvironment,
    listWorkflows,
    listWorkflowsInWorkflow,
    newDBOS,
    newWorkflowKey,
    registerDBOSWorkflow,
    registerDBOSWorkflowScoped,
    registerDBOSWorkflowRef,
    registerDBOSWorkflowRefScoped,
    registerQueue,
    resumeWorkflows,
    resumeWorkflowsInWorkflow,
    retrieveWorkflow,
    nullTracer,
    runDBOSWorkflow,
    runWorkflowStep,
    setWorkflowDelay,
    shutdown,
    startChildWorkflow,
    updateWorkflowAttributes,
    startDBOSWorkflowRef,
    startOptionsDefault,
    waitForWorkflow,
  )
import DBOS.Transact.ContextTest (ctxOver)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
  withResource acquireLoggerBackend snd $ \getLogger ->
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
      testCase "cancelling a workflow that does not exist is not an error" $
        withInstance "mgmt-cancel-missing" $ \dbos _suffix -> do
          exec <- launchOrFail dbos
          cancelled <- cancelWorkflows dbos [WorkflowId "never-existed"] False
          assertEqual "a missing row cancels to nothing" (Right []) cancelled,
      testCase "resuming a workflow that does not exist is an error" $
        withInstance "mgmt-resume-missing" $ \dbos _suffix -> do
          exec <- launchOrFail dbos
          resumed <- resumeWorkflows dbos [WorkflowId "never-existed"] Nothing
          case resumed of
            Left (ErrorSystemDatabase (SystemDB.NonExistentWorkflow {workflowIds})) ->
              workflowIds @?= ["never-existed"]
            other -> fail ("expected a non-existent-workflow refusal, got: " <> show other),
      testCase "cancelling makes a workflow terminal and leaves it resumable" $
        withInstance "mgmt-cancel-resume" $ \dbos suffix -> do
          ran <- newIORef (0 :: Int)
          let key = newWorkflowKey "cancellable"
              body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body input _ = do
                modifyIORef' ran (+ 1)
                pure (Right (input + 5))
              workflowText = "hs-l2-mgmt-cancel-resume-" <> suffix
              workflowId = WorkflowId workflowText
          ref <- registerRefOrFail dbos key body
          exec <- launchOrFail dbos
          started <-
            startWfRef
              exec
              ref
              (startOptionsDefault {startWorkflowId = Just workflowText, startQueue = Just (enqueueNew "no-runner-here")})
              (Just (encodeWorkflowValue (0 :: Int)))
          case started of
            Left err -> fail (show err)
            Right _ -> pure ()
          cancelled <- cancelWorkflows dbos [workflowId] False
          assertEqual "the cancel reports the id it moved" (Right [workflowId]) cancelled
          handle <- retrieveOrFail dbos workflowId
          status <- statusWf handle
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
      testCase "resuming onto a named queue puts the workflow there" $
        withInstance "mgmt-resume-queue" $ \dbos suffix -> do
          let queueName = "hs-l2-mgmt-queue-" <> suffix
              key = newWorkflowKey "resumable"
              body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body input _ = pure (Right input)
              workflowText = "hs-l2-mgmt-resume-queue-" <> suffix
              workflowId = WorkflowId workflowText
          ref <- registerRefOrFail dbos key body
          exec <- launchOrFail dbos
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          started <-
            startWfRef
              exec
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
      testCase "cancelling a tree reaches the children" $
        withInstance "mgmt-cancel-tree" $ \dbos suffix -> do
          -- Both ends wait: the child blocks on the gate, so it is
          -- PENDING (not finished) when the cancel arrives — a detached
          -- child that already finished could no longer be cancelled.
          childStarted <- newEmptyMVar
          gate <- newEmptyMVar
          let childKey = newWorkflowKey "tree-child"
              parentKey = newWorkflowKey "tree-parent"
              childBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              childBody () _ = putMVar childStarted () >> takeMVar gate >> pure (Right 1)
              parentText = "hs-l2-mgmt-tree-parent-" <> suffix
              parentId = WorkflowId parentText
          childRef <- registerRefOrFail dbos childKey childBody
          let parentBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
              parentBody () wctx = do
                let ctx = workflowCtxInner wctx
                started <- startChildWorkflow ctx childRef startOptionsDefault Nothing
                case started of
                  Left err -> pure (Left err)
                  Right handle -> do
                    -- The child exists and has begun before the parent
                    -- returns, so the cancel below cannot miss it.
                    takeMVar childStarted
                    pure (Right (workflowTextOf handle))
          registered <- registerDBOSWorkflowScoped dbos parentKey parentBody
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          ran <- runWf exec parentKey parentId Nothing
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
          status <- statusWf handle
          assertEqual "the child is terminal" (Right (Just Cancelled)) status,
      testCase "deleting a workflow removes its row" $
        withInstance "mgmt-delete" $ \dbos suffix -> do
          let key = newWorkflowKey "deletable"
              body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body input wctx = let ctx = workflowCtxInner wctx in runWorkflowStep ctx "work" (const (pure input))
              workflowText = "hs-l2-mgmt-delete-" <> suffix
              workflowId = WorkflowId workflowText
          registered <- registerDBOSWorkflowScoped dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          ran <- runWf exec key workflowId (Just (encodeWorkflowValue (1 :: Int)))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          deleted <- deleteWorkflows dbos [workflowId] True
          assertEqual "one row was deleted" (Right 1) deleted
          handle <- retrieveOrFail dbos workflowId
          status <- statusWf handle
          assertEqual "the row is gone" (Right Nothing) status,
      testCase "a workflow can be retrieved by id" $
        withInstance "mgmt-retrieve" $ \dbos suffix -> do
          let key = newWorkflowKey "retrievable"
              body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body input _ = pure (Right (input * 3))
              workflowText = "hs-l2-mgmt-retrieve-" <> suffix
              workflowId = WorkflowId workflowText
          registered <- registerDBOSWorkflowScoped dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          ran <- runWf exec key workflowId (Just (encodeWorkflowValue (2 :: Int)))
          case ran of
            Left err -> fail (show err)
            Right _ -> pure ()
          handle <- retrieveOrFail dbos workflowId
          status <- statusWf handle
          assertEqual "the retrieved row reports success" (Right (Just Success)) status
          result <- resultWf handle
          case result of
            Right (Just output) -> decodeSerializedResult output @?= Right (6 :: Int)
            other -> fail ("expected the retrieved result, got: " <> show other),
      testCase "forking from the beginning runs the workflow again under a new id" $
        withInstance "mgmt-fork-beginning" $ \dbos suffix -> do
          attempts <- newIORef (0 :: Int)
          let key = newWorkflowKey "forkable"
              body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body _ _ = do
                attempt <- readIORef attempts
                modifyIORef' attempts (+ 1)
                if attempt == 0
                  then pure (Left (ErrorConfig "the first attempt fails"))
                  else pure (Right 8)
              sourceText = "hs-l2-mgmt-fork-source-" <> suffix
              sourceId = WorkflowId sourceText
          registered <- registerDBOSWorkflowScoped dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          first <- runWf exec key sourceId (Just (encodeWorkflowValue (0 :: Int)))
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
          sourceStatus <- statusWf sourceHandle
          assertEqual "the source keeps its outcome" (Right (Just Error)) sourceStatus,
      testCase "a fork takes the id and queue it is given" $
        withInstance "mgmt-fork-placed" $ \dbos suffix -> do
          let key = newWorkflowKey "placed"
              body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body input _ = pure (Right input)
              sourceText = "hs-l2-mgmt-fork-placed-source-" <> suffix
              forkedText = "hs-l2-mgmt-fork-placed-fork-" <> suffix
              queueName = "hs-l2-mgmt-fork-queue-" <> suffix
              sourceId = WorkflowId sourceText
          registered <- registerDBOSWorkflowScoped dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          ran <- runWf exec key sourceId (Just (encodeWorkflowValue (4 :: Int)))
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
      testCase "forking from a chosen step replays the steps below it" $
        withInstance "mgmt-fork-step" $ \dbos suffix -> do
          ran <- newIORef ([] :: [Text])
          let key = newWorkflowKey "staged"
              body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body _ wctx = do
                let ctx = workflowCtxInner wctx
                outcomes <-
                  mapM
                    (\name -> runWorkflowStep ctx name (const (modifyIORef' ran (<> [name]) >> pure (0 :: Int))))
                    ["one", "two", "three"]
                pure (fmap (const 0) (sequence outcomes))
              sourceText = "hs-l2-mgmt-fork-step-source-" <> suffix
              sourceId = WorkflowId sourceText
          registered <- registerDBOSWorkflowScoped dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          first <- runWf exec key sourceId (Just (encodeWorkflowValue (0 :: Int)))
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
            other -> fail ("expected exactly one fork, got: " <> show other),
      testCase "bulk cancel and resume hand back every id" $
        withInstance "mgmt-bulk" $ \dbos suffix -> do
          let key = newWorkflowKey "queued"
              echoWorkflow :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
              echoWorkflow message _ = pure (Right message)
          registered <- registerDBOSWorkflowScoped dbos key echoWorkflow
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          let first = WorkflowId ("hs-l2-mgmt-bulk-1-" <> suffix)
              second = WorkflowId ("hs-l2-mgmt-bulk-2-" <> suffix)
              enqueueOne wid = do
                enqueued <-
                  enqueueDBOSWorkflow
                    dbos
                    key
                    wid
                    (Just (encodeWorkflowValue ("hello" :: Text)))
                    ("bulk-" <> suffix)
                case enqueued of
                  Left err -> fail (show err)
                  Right _ -> pure ()
          mapM_ enqueueOne [first, second]
          cancelled <- cancelWorkflows dbos [first, second] False
          case cancelled of
            Left err -> fail (show err)
            Right ids -> do
              length ids @?= 2
              assertBool "both cancelled ids come back" (all (`elem` ids) [first, second])
          resumed <- resumeWorkflows dbos [first, second] Nothing
          case resumed of
            Left err -> fail (show err)
            Right ids -> do
              length ids @?= 2
              assertBool "both resumed ids come back" (all (`elem` ids) [first, second]),
      testCase "bulk forking hands back one new id per source, in order" $
        withInstance "mgmt-bulk-fork" $ \dbos suffix -> do
          let key = newWorkflowKey "doubling"
              body :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body input _ = pure (Right (input * 2))
              first = WorkflowId ("hs-l2-mgmt-bulk-fork-1-" <> suffix)
              second = WorkflowId ("hs-l2-mgmt-bulk-fork-2-" <> suffix)
          registered <- registerDBOSWorkflowScoped dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          let runOne wid input = do
                ran <- runWf exec key wid (Just (encodeWorkflowValue input))
                case ran of
                  Right _ -> pure ()
                  other -> fail ("expected the source to run, got: " <> show other)
          runOne first (9 :: Int)
          runOne second (10 :: Int)
          forked <- forkWorkflows dbos [(forkNew ("hs-l2-mgmt-bulk-fork-1-" <> suffix)), (forkNew ("hs-l2-mgmt-bulk-fork-2-" <> suffix))] defaultForkOptions
          case forked of
            Left err -> fail (show err)
            Right [forkedFirst, forkedSecond] -> do
              assertBool "a fork reuses neither source id" (all (`notElem` [first, second]) [forkedFirst, forkedSecond])
              assertBool "the two forks differ" (forkedFirst /= forkedSecond)
              let awaitOne fwid expected = do
                    waited <- waitForWorkflow dbos fwid
                    case waited of
                      Right (AwaitedSucceeded (Just output) _) -> decodeResult output @?= Right expected
                      other -> fail ("expected the fork to run, got: " <> show other)
              -- Positional: the i-th fork replays the i-th source's input.
              awaitOne forkedFirst (18 :: Int)
              awaitOne forkedSecond (20 :: Int)
            other -> fail ("expected one fork per source, got: " <> show other),
      testCase "resuming onto a named queue puts the workflow there" $
        withInstance "mgmt-resume-queue" $ \dbos suffix -> do
          let key = newWorkflowKey "queued"
              wid = WorkflowId ("hs-l2-mgmt-resume-q-" <> suffix)
              echoWorkflow :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
              echoWorkflow message _ = pure (Right message)
          registered <- registerDBOSWorkflowScoped dbos key echoWorkflow
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          enqueued <-
            enqueueDBOSWorkflow
              dbos
              key
              wid
              (Just (encodeWorkflowValue ("hello" :: Text)))
              ("from-" <> suffix)
          case enqueued of
            Left err -> fail (show err)
            Right _ -> pure ()
          resumed <- resumeWorkflows dbos [wid] (Just ("to-" <> suffix))
          resumed @?= Right [wid]
          WorkflowRecord {workflowRecordQueueName = queue} <- readRow getBackend wid
          queue @?= Just ("to-" <> suffix),
      testCase "a fork onto a partitioned queue carries the key it is given" $
        withInstance "mgmt-fork-key" $ \dbos suffix -> do
          let key = newWorkflowKey "queued"
              sourceId = WorkflowId ("hs-l2-mgmt-fork-key-src-" <> suffix)
              echoWorkflow :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
              echoWorkflow message _ = pure (Right message)
          registered <- registerDBOSWorkflowScoped dbos key echoWorkflow
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          enqueued <-
            enqueueDBOSWorkflow
              dbos
              key
              sourceId
              (Just (encodeWorkflowValue ("hello" :: Text)))
              ("keyed-" <> suffix)
          case enqueued of
            Left err -> fail (show err)
            Right _ -> pure ()
          forked <-
            forkFrom
              dbos
              [sourceId]
              (ForkStep 0)
              (defaultForkOptions {forkOptionsQueuePartitionKey = Just "pk-7"})
          forkedId <- case forked of
            Left err -> fail (show err)
            Right [forkedId] -> pure forkedId
            other -> fail ("expected exactly one fork, got: " <> show other)
          WorkflowRecord {workflowRecordQueuePartitionKey = partition} <- readRow getBackend forkedId
          partition @?= Just "pk-7",
      testCase "forking from the last failure restarts at the failed step" $
        withInstance "mgmt-fork-failure" $ \dbos suffix -> do
          calls <- newIORef (0 :: Int)
          let key = newWorkflowKey "flaky"
              sourceId = WorkflowId ("hs-l2-mgmt-fork-fail-src-" <> suffix)
              flakyBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              flakyBody _ wctx = do
                let ctx = workflowCtxInner wctx
                first <- runWorkflowStep ctx "one" (const (pure (0 :: Int)))
                case first of
                  Left err -> pure (Left err)
                  Right _ -> do
                    attempt <- readIORef calls
                    modifyIORef' calls (+ 1)
                    if attempt < 1
                      then pure (Left (StepFailed "two" "boom"))
                      else runWorkflowStep ctx "two" (const (pure 99))
          registered <- registerDBOSWorkflowScoped dbos key flakyBody
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          first <- runWf exec key sourceId (Just (encodeWorkflowValue ("hi" :: Text)))
          case first of
            Left (StepFailed step _) -> step @?= "two"
            other -> fail ("expected the step failure, got: " <> show other)
          forked <- forkFrom dbos [sourceId] ForkLastFailure defaultForkOptions
          forkedId <- case forked of
            Left err -> fail (show err)
            Right [forkedId] -> pure forkedId
            other -> fail ("expected exactly one fork, got: " <> show other)
          assertBool "the fork restarts under a new id" (forkedId /= sourceId)
          waited <- waitForWorkflow dbos forkedId
          case waited of
            Right (AwaitedSucceeded (Just output) _) -> decodeResult output @?= Right (99 :: Int)
            other -> fail ("expected the fork to recover, got: " <> show other),
      testCase "attributes are replaced and can be searched" $
        withInstance "mgmt-attributes" $ \dbos suffix -> do
          let key = newWorkflowKey "tagged"
              body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body () _ = pure (Right 0)
              wid = WorkflowId ("hs-l2-mgmt-attributes-" <> suffix)
              tenant = "acme-" <> Text.take 12 suffix
              full = "{\"tenant\":\"" <> tenant <> "\",\"tier\":\"gold\"}"
              tenantOnly = "{\"tenant\":\"" <> tenant <> "\"}"
              tierOnly = "{\"tier\":\"gold\"}"
          registered <- registerDBOSWorkflowScoped dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          ran <- runWf exec key wid Nothing
          case ran of
            Right _ -> pure ()
            other -> fail ("expected the workflow to run, got: " <> show other)
          updated <- updateWorkflowAttributes dbos wid (Just full)
          case updated of
            Left err -> fail (show err)
            Right () -> pure ()
          -- Containment, not equality: one key out of two matches.
          found <- listWorkflows dbos (defaultWorkflowFilter {workflowFilterAttributes = Just tenantOnly})
          case found of
            Right rows -> map (.workflowRecordId) rows @?= [wid]
            other -> fail ("expected the tagged workflow, got: " <> show other)
          -- A replacement, not a merge: the key not sent again is gone.
          fewer <- updateWorkflowAttributes dbos wid (Just tenantOnly)
          case fewer of
            Left err -> fail (show err)
            Right () -> pure ()
          afterReplacement <- listWorkflows dbos (defaultWorkflowFilter {workflowFilterAttributes = Just tierOnly})
          afterReplacement @?= Right []
          -- And None clears them.
          cleared <- updateWorkflowAttributes dbos wid Nothing
          case cleared of
            Left err -> fail (show err)
            Right () -> pure ()
          row <- readRow getBackend wid
          row.workflowRecordAttributes @?= Nothing,
      testCase "a workflow cannot delete itself" $
        withInstance "mgmt-self-delete" $ \dbos suffix -> do
          let key = newWorkflowKey "self-deleter"
              wid = WorkflowId ("hs-l2-mgmt-self-" <> suffix)
              body :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
              body ownId wctx = do
                let ctx = workflowCtxInner wctx
                deleted <- deleteWorkflowsInWorkflow ctx [WorkflowId ownId] False
                pure (void deleted)
          registered <- registerDBOSWorkflowScoped dbos key body
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          ran <-
            runWf
              exec
              key
              wid
              (Just (encodeWorkflowValue ("hs-l2-mgmt-self-" <> suffix :: Text)))
          case ran of
            Left err -> assertBool ("the refusal did not reach the caller: " <> show err) ("cannot delete itself" `Text.isInfixOf` Text.pack (show err))
            Right _ -> fail "the workflow deleted itself"
          -- The row went nowhere even though the delete was refused.
          _ <- readRow getBackend wid
          pure (),
      testCase "a workflow cannot delete an ancestors tree" $
        withInstance "mgmt-ancestor-delete" $ \dbos suffix -> do
          -- The child reports through a rendezvous because a refused
          -- delete records nothing: a polling handle would wait on a row
          -- that stays pending forever, where the oracle awaits the
          -- child's local task.
          observed <- newEmptyMVar
          let childKey = newWorkflowKey "deleting-child"
              parentKey = newWorkflowKey "deleted-parent"
              rootText = "hs-l2-mgmt-ancestor-" <> suffix
              childBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
              childBody root wctx = do
                let ctx = workflowCtxInner wctx
                outcome <- deleteWorkflowsInWorkflow ctx [WorkflowId root] True
                void (tryPutMVar observed outcome)
                pure (void outcome)
          childRef <- registerRefOrFail dbos childKey childBody
          let parentBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
              parentBody root wctx = do
                let ctx = workflowCtxInner wctx
                started <- startChildWorkflow ctx childRef startOptionsDefault (Just (encodeWorkflowValue root))
                case started of
                  Left err -> pure (Left err)
                  Right _ -> void <$> takeMVar observed
          registered <- registerDBOSWorkflowScoped dbos parentKey parentBody
          case registered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          ran <-
            runWf
              exec
              parentKey
              (WorkflowId rootText)
              (Just (encodeWorkflowValue (rootText :: Text)))
          case ran of
            Left err -> assertBool ("the refusal did not reach the caller: " <> show err) ("cannot delete itself" `Text.isInfixOf` Text.pack (show err))
            Right _ -> fail "the child deleted the tree it was in"
          -- Neither the ancestor nor the parent row went anywhere.
          _ <- readRow getBackend (WorkflowId rootText)
          pure (),
      testCase "management calls from inside a workflow are recorded as steps" $
        withInstance "mgmt-in-workflow" $ \dbos suffix -> do
          let targetKey = newWorkflowKey "target"
              operatorKey = newWorkflowKey "operator"
              targetText = "hs-l2-mgmt-op-target-" <> suffix
              operatorText = "hs-l2-mgmt-operator-" <> suffix
              -- A queue nothing polls, so the target sits there to be
              -- cancelled: named on the row, with no table row for any
              -- sweep to see.
              quietQueue = "hs-l2-mgmt-quiet-" <> suffix
              targetBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              targetBody () _ = pure (Right 1)
              operatorBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              operatorBody target wctx = do
                let ctx = workflowCtxInner wctx
                cancelled <- cancelWorkflowsInWorkflow ctx [WorkflowId target] False
                case cancelled of
                  Left err -> pure (Left err)
                  Right _ -> do
                    listed <-
                      listWorkflowsInWorkflow
                        ctx
                        (defaultWorkflowFilter {workflowFilterWorkflowIds = [target]})
                    pure (Right (length listed))
          registeredTarget <- registerDBOSWorkflowScoped dbos targetKey targetBody
          case registeredTarget of
            Left err -> fail (show err)
            Right () -> pure ()
          registeredOperator <- registerDBOSWorkflowScoped dbos operatorKey operatorBody
          case registeredOperator of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          enqueued <-
            enqueueDBOSWorkflow
              dbos
              targetKey
              (WorkflowId targetText)
              Nothing
              quietQueue
          case enqueued of
            Left err -> fail (show err)
            Right _ -> pure ()
          ran <-
            runWf
              exec
              operatorKey
              (WorkflowId operatorText)
              (Just (encodeWorkflowValue (targetText :: Text)))
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the listing saw the filtered workflow" (Right 1) decoded
            other -> fail (show other)
          target <- readRow getBackend (WorkflowId targetText)
          target.workflowRecordStatus @?= Cancelled
          backend <- getBackend
          listed <- SystemDB.listWorkflowSteps backend (WorkflowId operatorText) False Nothing Nothing Nothing
          case listed of
            Right steps ->
              map (\StepRecord {stepRecordStepId = sid, stepRecordStepName = name} -> (sid, name)) steps
                @?= [(0, "DBOS.cancelWorkflow"), (1, "DBOS.listWorkflows")]
            other -> fail (show other),
      testCase "a replayed fork hands back the id it recorded" $
        withInstance "mgmt-fork-replay" $ \dbos suffix -> do
          -- A fork generates a new id, so a replay without the checkpoint
          -- would write a second fork: crash after the fork is recorded
          -- and let recovery prove the replay adopts it.
          let sourceKey = newWorkflowKey "forked-source"
              operatorKey = newWorkflowKey "forking-operator"
              sourceText = "hs-l2-mgmt-replay-src-" <> suffix
              operatorText = "hs-l2-mgmt-replay-op-" <> suffix
              sourceBody :: forall exec. Int -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              sourceBody value wctx = let ctx = workflowCtxInner wctx in runWorkflowStep ctx "double" (const (pure (value * 2)))
          shouldCrash <- newIORef True
          seen <- newEmptyMVar
          let operatorBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Text)
              operatorBody source wctx = do
                let ctx = workflowCtxInner wctx
                forked <- forkWorkflowsInWorkflow ctx [forkNew source] defaultForkOptions
                case forked of
                  Left err -> pure (Left err)
                  Right [WorkflowId fid] -> do
                    void (tryPutMVar seen (WorkflowId fid))
                    crash <- readIORef shouldCrash
                    if crash
                      then liftIO (ioError (userError "forked then crashed"))
                      else pure (Right fid)
                  Right other -> fail ("expected exactly one fork, got: " <> show other)
          sourceRegistered <- registerDBOSWorkflowScoped dbos sourceKey sourceBody
          case sourceRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          operatorRegistered <- registerDBOSWorkflowScoped dbos operatorKey operatorBody
          case operatorRegistered of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          ranSource <- runWf exec sourceKey (WorkflowId sourceText) (Just (encodeWorkflowValue (21 :: Int)))
          case ranSource of
            Right _ -> pure ()
            other -> fail ("expected the source to run, got: " <> show other)
          first <- try (runWf exec operatorKey (WorkflowId operatorText) (Just (encodeWorkflowValue (sourceText :: Text)))) :: IO (Either SomeException (Either (Error EngineOnly) (Maybe SerializedWorkflowValue)))
          case first of
            Left exception -> assertBool "the crash escapes" ("forked then crashed" `Text.isInfixOf` Text.pack (show exception))
            Right other -> fail ("expected the crash, got: " <> show other)
          shutdown dbos
          writeIORef shouldCrash False
          _ <- launchWithEnvironment dbos isolatedEnvironment
          settled <- timeout 10000000 (waitForWorkflow dbos (WorkflowId operatorText))
          case settled of
            Just (Right (AwaitedSucceeded (Just output) _)) -> do
              let decoded = decodeWorkflowValue "result" (Just (SerializedWorkflowValue output Nothing)) :: Either CodecError Text
              recorded <- takeMVar seen
              case decoded of
                Right fid -> WorkflowId fid @?= recorded
                Left err -> fail (show err)
            other -> fail ("expected the recovery to finish, got: " <> show other)
          backend <- getBackend
          forks <- SystemDB.listWorkflows backend (defaultWorkflowFilter {workflowFilterForkedFrom = [sourceText]}) Nothing
          case forks of
            Right rows -> length rows @?= 1
            other -> fail ("expected exactly one fork row, got: " <> show other),
      testCase "resume puts a cancelled workflow on a queue that runs it" $
        withInstance "mgmt-resume-run" $ \dbos suffix -> do
          let targetKey = newWorkflowKey "resumable"
              operatorKey = newWorkflowKey "resumer"
              targetText = "hs-l2-mgmt-resume-target-" <> suffix
              operatorText = "hs-l2-mgmt-resumer-" <> suffix
              quietQueue = "hs-l2-mgmt-quiet-" <> suffix
              runQueue = "hs-l2-mgmt-run-" <> Text.take 12 suffix
              targetBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              targetBody () _ = pure (Right 1)
              operatorBody :: forall exec. Text -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
              operatorBody target wctx = do
                let ctx = workflowCtxInner wctx
                cancelled <- cancelWorkflowsInWorkflow ctx [WorkflowId target] False
                case cancelled of
                  Left err -> pure (Left err)
                  Right _ -> do
                    resumed <- resumeWorkflowsInWorkflow ctx [WorkflowId target] (Just runQueue)
                    case resumed of
                      Left err -> pure (Left err)
                      Right _ -> pure (Right ())
          registeredTarget <- registerDBOSWorkflowScoped dbos targetKey targetBody
          case registeredTarget of
            Left err -> fail (show err)
            Right () -> pure ()
          registeredOperator <- registerDBOSWorkflowScoped dbos operatorKey operatorBody
          case registeredOperator of
            Left err -> fail (show err)
            Right () -> pure ()
          exec <- launchOrFail dbos
          queueRegistered <- registerQueue dbos runQueue defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          enqueued <-
            enqueueDBOSWorkflow
              dbos
              targetKey
              (WorkflowId targetText)
              Nothing
              quietQueue
          case enqueued of
            Left err -> fail (show err)
            Right _ -> pure ()
          ran <-
            runWf
              exec
              operatorKey
              (WorkflowId operatorText)
              (Just (encodeWorkflowValue (targetText :: Text)))
          case ran of
            Right _ -> pure ()
            other -> fail ("expected the operator to run, got: " <> show other)
          settled <- timeout 15000000 (waitForWorkflow dbos (WorkflowId targetText))
          case settled of
            Just (Right (AwaitedSucceeded (Just output) _)) -> do
              let decoded = decodeWorkflowValue "result" (Just (SerializedWorkflowValue output Nothing)) :: Either CodecError Int
              assertEqual "the resumed workflow runs" (Right 1) decoded
            other -> fail ("expected the resumed run, got: " <> show other)
          backend <- getBackend
          listed <- SystemDB.listWorkflowSteps backend (WorkflowId operatorText) False Nothing Nothing Nothing
          case listed of
            Right steps ->
              map (\StepRecord {stepRecordStepId = sid, stepRecordStepName = name} -> (sid, name)) steps
                @?= [(0, "DBOS.cancelWorkflow"), (1, "DBOS.resumeWorkflow")]
            other -> fail (show other),
      testCase "a clients management call inside a workflow is not a step" $
        withInstance "mgmt-client-call" $ \dbos suffix -> do
          -- A client has no step counter of its own to agree with the
          -- workflow's, so its calls run again on replay instead of
          -- checkpointing: by construction they cannot spend step ids.
          let operatorKey = newWorkflowKey "caller"
              operatorText = "hs-l2-mgmt-client-op-" <> suffix
          clientConfig <- clientConfigFromEnv
          bracket (connectClient clientConfig) (either (const (pure ())) closeClient) $ \connected ->
            case connected of
              Left err -> fail (show err)
              Right client -> do
                let body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
                    body () wctx = do
                      let ctx = workflowCtxInner wctx
                      cancelled <- clientCancelWorkflows client [WorkflowId "never-existed"] False
                      case cancelled of
                        Left err -> pure (Left err)
                        Right _ -> do
                          probe <- runWorkflowStep ctx "probe" (const (pure ()))
                          case probe of
                            Left err -> pure (Left err)
                            Right () -> pure (Right ())
                registered <- registerDBOSWorkflowScoped dbos operatorKey body
                case registered of
                  Left err -> fail (show err)
                  Right () -> pure ()
                exec <- launchOrFail dbos
                ran <- runWf exec operatorKey (WorkflowId operatorText) Nothing
                case ran of
                  Right _ -> pure ()
                  other -> fail ("expected the operator to run, got: " <> show other)
                backend <- getBackend
                listed <- SystemDB.listWorkflowSteps backend (WorkflowId operatorText) False Nothing Nothing Nothing
                case listed of
                  Right [StepRecord {stepRecordStepId = sid, stepRecordStepName = name}] ->
                    (sid, name) @?= (0, "probe")
                  other -> fail ("expected only the probe at step zero, got: " <> show other),
      testCase "a refused management call spends no step id" $ do
        backend <- getBackend
        fresh <- freshSuffix
        let workflowText = "hs-l2-mgmt-refusal-" <> fresh
            initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2MgmtRefusal"}
        created <- SystemDB.initWorkflow backend initialWorkflow Nothing Fresh Nothing
        case created of
          Left err -> fail (show err)
          Right _ -> pure ()
        timed <- getLogger
        context <- ctxOver backend (ioTracer (fst timed)) workflowText
        -- An empty forked id must be absent rather than empty: refused
        -- before any id is taken.
        refused <- forkWorkflowsInWorkflow context [(forkNew "source") {forkForkedId = Just ""}] defaultForkOptions
        case refused of
          Left (ErrorSystemDatabase (SystemDB.InvalidInput {})) -> pure ()
          other -> fail ("expected an argument refusal, got: " <> show other)
        -- The probe that follows still takes step zero.
        probe <- (runWorkflowStep context "probe" (const (pure ())) :: IO (Either (Error EngineOnly) ()))
        case probe of
          Left err -> fail (show err)
          Right () -> pure ()
        listed <- SystemDB.listWorkflowSteps backend (WorkflowId workflowText) False Nothing Nothing Nothing
        case listed of
          Right [StepRecord {stepRecordStepId = sid, stepRecordStepName = name}] ->
            (sid, name) @?= (0, "probe")
          other -> fail ("expected only the probe at step zero, got: " <> show other),
      testCase "an empty management batch records its step" $ do
        backend <- getBackend
        fresh <- freshSuffix
        let workflowText = "hs-l2-mgmt-empty-" <> fresh
            initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2MgmtEmpty"}
        created <- SystemDB.initWorkflow backend initialWorkflow Nothing Fresh Nothing
        case created of
          Left err -> fail (show err)
          Right _ -> pure ()
        timed <- getLogger
        context <- ctxOver backend (ioTracer (fst timed)) workflowText
        emptied <- forkWorkflowsInWorkflow context [] defaultForkOptions
        emptied @?= Right []
        listed <- SystemDB.listWorkflowSteps backend (WorkflowId workflowText) False Nothing Nothing Nothing
        case listed of
          Right [StepRecord {stepRecordStepId = sid, stepRecordStepName = name}] ->
            (sid, name) @?= (0, "DBOS.forkWorkflow")
          other -> fail ("expected the empty batch at step zero, got: " <> show other),
      testCase "a delayed workflow can be released sooner" $
        withInstance "mgmt-delay-release" $ \dbos suffix -> do
          -- Far enough out that the test is not racing the supervisor.
          let key = newWorkflowKey "delayable"
              queueName = "hs-l2-delayed-work-" <> Text.take 12 suffix
              wid = WorkflowId ("hs-l2-delayed-" <> suffix)
              body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
              body () _ = pure (Right 2)
          registered <- registerDBOSWorkflowRefScoped dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          exec <- launchOrFail dbos
          queueRegistered <- registerQueue dbos queueName defaultQueueOptions AlwaysUpdate
          case queueRegistered of
            Left err -> fail (show err)
            Right _ -> pure ()
          started <-
            startWfRef
              exec
              ref
              (startOptionsDefault {startWorkflowId = Just ("hs-l2-delayed-" <> suffix), startQueue = Just ((enqueueNew queueName) {delay = Just (secondsDuration 3600)})})
              Nothing
          case started of
            Left err -> fail (show err)
            Right _ -> pure ()
          waiting <- readRow getBackend wid
          waiting.workflowRecordStatus @?= Delayed
          -- Bring it forward to now, and the supervisor releases it on
          -- its next pass.
          released <- setWorkflowDelay dbos wid (DelayFor (secondsDuration 0))
          case released of
            Left err -> fail (show err)
            Right () -> pure ()
          settled <- timeout 15000000 (waitForWorkflow dbos wid)
          case settled of
            Just (Right (AwaitedSucceeded (Just output) _)) -> do
              let decoded = decodeWorkflowValue "result" (Just (SerializedWorkflowValue output Nothing)) :: Either CodecError Int
              assertEqual "the released workflow runs" (Right 2) decoded
            other -> fail ("expected the released run, got: " <> show other)
    ]

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through: each
-- pins the error channel to 'EngineOnly' (the channel the test bodies
-- declare), so a call site outside an annotated body does not leave it
-- for the compiler to guess. Local copies are deliberate — this module
-- carries only the aliases it uses, and a sibling test module repeats
-- the ones it needs.
runWf :: Executor IO -> WorkflowKey -> WorkflowId -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWf = runDBOSWorkflow

startWfRef :: Executor IO -> WorkflowRef IO EngineOnly -> StartOptions -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
startWfRef = startDBOSWorkflowRef

retrieveWf :: DBOS IO -> WorkflowId -> IO (Either (Error EngineOnly) (WorkflowHandle IO EngineOnly))
retrieveWf = retrieveWorkflow

resultWf :: WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
resultWf = handleResult

statusWf :: WorkflowHandle IO EngineOnly -> IO (Either (Error EngineOnly) (Maybe WorkflowStatus))
statusWf = handleStatus

-- * Helpers

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted. The
-- launched instances below keep their own pools: each needs a distinct
-- application identity.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | One workflow row as stored: a reader over the suite backend.
readRow :: IO Postgres.PostgresSystemDB -> WorkflowId -> IO WorkflowRecord
readRow getBackend wid = do
  backend <- getBackend
  found <- getWorkflow backend wid
  case found of
    Right (Just record) -> pure record
    _ -> fail "expected the workflow row to exist"

freshSuffix :: IO Text
freshSuffix = Text.pack . UUID.toString <$> UUID.V4.nextRandom

instanceFor :: Text -> IO (DBOS IO, Text)
instanceFor label = do
  suffix <- freshSuffix
  base <- configFromEnv (("hs-l2-" <> label <> "-") <> Text.take 16 suffix)
  let config = base {configAppVersion = Just ("hs-l2-version-" <> suffix), configExecutorId = Just ("hs-l2-executor-" <> suffix)}
  dbos <- newDBOS config
  pure (dbos, suffix)

-- | A launched-instance test owns its backend: shutdown runs even when
-- assertions fail, so repeated evals cannot exhaust Postgres connections.
withInstance :: Text -> (DBOS IO -> Text -> IO a) -> IO a
withInstance label action = bracket (instanceFor label) (shutdown . fst) (uncurry action)

launchOrFail :: DBOS IO -> IO (Executor IO)
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

registerRefOrFail :: (FromJSON argument, ToJSON result) => DBOS IO -> WorkflowKey -> (forall exec. argument -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) result)) -> IO (WorkflowRef IO EngineOnly)
registerRefOrFail dbos key body = do
  registered <- registerDBOSWorkflowRefScoped dbos key body
  either (fail . show) pure registered

retrieveOrFail :: DBOS IO -> WorkflowId -> IO (WorkflowHandle IO EngineOnly)
retrieveOrFail dbos workflowId = do
  retrieved <- retrieveWf dbos workflowId
  either (fail . show) pure retrieved

decodeResult :: Text -> Either CodecError Int
decodeResult output = decodeWorkflowValue "result" (Just (SerializedWorkflowValue output Nothing))

decodeSerializedResult :: SerializedWorkflowValue -> Either CodecError Int
decodeSerializedResult output = decodeWorkflowValue "result" (Just output)

decodeSerializedChildId :: SerializedWorkflowValue -> Either CodecError Text
decodeSerializedChildId output = decodeWorkflowValue "result" (Just output)

workflowTextOf :: WorkflowHandle IO EngineOnly -> Text
workflowTextOf handle = handleWorkflowId handle
