{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Event behavior through the workflow-facing API and live SystemDB.
module DBOS.Transact.EventTest (tests) where

import DBOS.Prelude
import Data.Text (Text)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (NewWorkflow (..), StepRecord (..), Submission (..), getEventStepName, millisDuration, newWorkflow, sleepStepName)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  (
    EngineOnly, CodecError,
    Config (..),
    WorkflowCtx,
    Identity (..),
    withWorkflow,
    withStep,
    DBOS,
    Executor,
    Environment (..),
    Error (..),
    PendingStep (..),
    RunOptions (..),
    SerializedWorkflowValue (..),
    WorkflowId (..),
    WorkflowRef,
    configFromEnv,
    decodeWorkflowValue,
    encodeWorkflowValue,
    firstStepStatus,
    getEvent,
    getWorkflowEvent,
    launchWithEnvironment,
    newDBOS,
    newWorkflowKey,
    nextWorkflowMarker,
    nextWorkflowStepId,
    nullTracer,
    pendingGetEvent,
    pendingSetEvent,
    pendingSleep,
    pendingWorkflowStep,
    registerDBOSWorkflowRef,
    runDBOSWorkflowRef,
    runOptionsDefault,
    runWorkflowStep,
    runWorkflowStepWith,
    setEvent,
    shutdown,
    stepOptionsDefault,
    getEvent,
    pendingSetEvent,
    pendingSleep,
    pendingWorkflowStep,
    runWorkflowStep,
    setEvent,
  )
import DBOS.Transact.Checkpoint (pendingStepId)
import DBOS.Transact.Context (StepCtx (stepCtxWorkflow))
import DBOS.Transact.ContextTest (connOver)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

-- | Launch over the isolated environment and hand back the executor.
launchEventExec :: DBOS IO -> Environment -> IO (Executor IO)
launchEventExec dbos env = do
  started <- launchWithEnvironment dbos env
  case started of
    Left err -> fail (show err)
    Right executor -> pure executor

-- | The application identity the scoped event cases install.
eventTestIdentity :: Identity
eventTestIdentity =
  Identity
    { identityAppName = "test-app",
      identityAppVersion = "1.0.0",
      identityExecutorId = "test-executor",
      identityAppId = ""
    }

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
  testGroup
    "Workflow events"
    [ testCase "a workflow can publish and replay an event" $ do
        withSuiteBackend getBackend $ \backend -> do
          freshId <- UUID.V4.nextRandom
          let workflowText = "hs-l2-event-" <> Text.pack (UUID.toString freshId)
              initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2EventTest"}
          created <- SystemDB.initWorkflow backend initialWorkflow Nothing Fresh Nothing
          case created of
            Left err -> fail (show err)
            Right _ -> pure ()
          let action :: forall exec. WorkflowCtx exec IO -> IO (Either (Error EngineOnly) (Maybe Text))
              action wctx = do
                published <- setEvent wctx "progress" ("ready" :: Text)
                case published of
                  Left err -> pure (Left err)
                  Right () -> getEvent wctx (WorkflowId workflowText) "progress" (millisDuration 100)
          conn <- connOver backend nullTracer
          first <- withWorkflow conn eventTestIdentity (WorkflowId workflowText) Nothing action
          case first of
            Right (Just value) -> assertEqual "the event is visible to a workflow reader" ("ready" :: Text) value
            other -> fail (show other)
          replayConn <- connOver backend nullTracer
          replay <- withWorkflow replayConn eventTestIdentity (WorkflowId workflowText) Nothing action
          case replay of
            Right (Just value) -> assertEqual "the event read replays the recorded value" ("ready" :: Text) value
            other -> fail (show other),
      testCase "a reading workflow is checkpointed and a reading step is not" $ do
        withSuiteBackend getBackend $ \backend -> do
          freshId <- UUID.V4.nextRandom
          let prefix = "hs-l2-event-steps-" <> Text.pack (UUID.toString freshId)
              publisherText = prefix <> "-publisher"
              readerText = prefix <> "-reader"
              inStepText = prefix <> "-in-step"
              create workflowText workflowName =
                let row = (newWorkflow workflowText) {newWorkflowName = Just workflowName}
                 in SystemDB.initWorkflow backend row Nothing Fresh Nothing
          created <- sequence [create publisherText "L2EventPublisher", create readerText "L2EventReader", create inStepText "L2EventInStep"]
          case created of
            [Right _, Right _, Right _] -> pure ()
            other -> fail (show other)
          publisherConn <- connOver backend nullTracer
          published <- withWorkflow publisherConn eventTestIdentity (WorkflowId publisherText) Nothing $ \wctx ->
            setEvent wctx "answer" (42 :: Int)
          published @?= Right ()
          readerConn <- connOver backend nullTracer
          readOutside <- (withWorkflow readerConn eventTestIdentity (WorkflowId readerText) Nothing $ \wctx ->
            getEvent wctx (WorkflowId publisherText) "answer" (millisDuration 0) :: IO (Either (Error EngineOnly) (Maybe Int)))
          readOutside @?= Right (Just 42)
          outsideSteps <- stepNames backend readerText
          outsideSteps @?= [(0, getEventStepName), (1, sleepStepName)]
          inStepConn <- connOver backend nullTracer
          readInside <- (withWorkflow inStepConn eventTestIdentity (WorkflowId inStepText) Nothing $ \wctx ->
            runWorkflowStepWith stepOptionsDefault wctx "read" (\sctx -> getEvent sctx.stepCtxWorkflow (WorkflowId publisherText) "answer" (millisDuration 0)) :: IO (Either (Error EngineOnly) (Maybe Int)))
          readInside @?= Right (Just 42)
          insideSteps <- stepNames backend inStepText
          insideSteps @?= [(0, "read")]
          -- The read inside the step left no id: the slot after the step is empty.
          spare <- SystemDB.checkStep backend (WorkflowId inStepText) 1 getEventStepName
          spare @?= Right Nothing,
      testCase "a refused set event spends no step id" $ do
        withSuiteBackend getBackend $ \backend -> do
          freshId <- UUID.V4.nextRandom
          let workflowText = "hs-l2-event-refusal-" <> Text.pack (UUID.toString freshId)
              initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2EventRefusal"}
          created <- SystemDB.initWorkflow backend initialWorkflow Nothing Fresh Nothing
          case created of
            Left err -> fail (show err)
            Right _ -> pure ()
          conn <- connOver backend nullTracer
          (refused, before, after) <-
            withWorkflow conn eventTestIdentity (WorkflowId workflowText) Nothing $ \wctx -> do
              marker <- nextWorkflowMarker wctx
              withStep wctx marker (firstStepStatus 0) $ \_sctx -> do
                before <- nextWorkflowStepId wctx
                refused <- setEvent wctx "progress" ("ready" :: Text)
                after <- nextWorkflowStepId wctx
                pure (refused, before, after)
          refused @?= Left (InsideStep "set_event")
          after @?= before + 1,
      testCase "a getEvent through a captured parent is plain and moves no ids" $ do
        withSuiteBackend getBackend $ \backend -> do
          freshId <- UUID.V4.nextRandom
          let prefix = "hs-l2-event-captured-" <> Text.pack (UUID.toString freshId)
              publisherText = prefix <> "-publisher"
              readerText = prefix <> "-reader"
              create workflowText workflowName =
                let row = (newWorkflow workflowText) {newWorkflowName = Just workflowName}
                 in SystemDB.initWorkflow backend row Nothing Fresh Nothing
          created <- sequence [create publisherText "L2EventPublisher", create readerText "L2EventReader"]
          case created of
            [Right _, Right _] -> pure ()
            other -> fail (show other)
          publisherConn <- connOver backend nullTracer
          published <- withWorkflow publisherConn eventTestIdentity (WorkflowId publisherText) Nothing $ \wctx ->
            setEvent wctx "answer" (42 :: Int)
          published @?= Right ()
          readerConn <- connOver backend nullTracer
          (readCaptured, before, after) <-
            withWorkflow readerConn eventTestIdentity (WorkflowId readerText) Nothing $ \wctx -> do
              marker <- nextWorkflowMarker wctx
              withStep wctx marker (firstStepStatus 0) $ \_sctx -> do
                before <- nextWorkflowStepId wctx
                readCaptured <- getEvent wctx (WorkflowId publisherText) "answer" (millisDuration 0) :: IO (Either (Error EngineOnly) (Maybe Int))
                after <- nextWorkflowStepId wctx
                pure (readCaptured, before, after)
          readCaptured @?= Right (Just 42)
          -- The probe's own counter read moves one; the plain read moves none.
          after @?= before + 1,
      testCase "a replayed set event does not republish" $ do
        withSuiteBackend getBackend $ \backend -> do
          freshId <- UUID.V4.nextRandom
          let workflowText = "hs-l2-event-replay-" <> Text.pack (UUID.toString freshId)
              initialWorkflow = (newWorkflow workflowText) {newWorkflowName = Just "L2EventReplay"}
          created <- SystemDB.initWorkflow backend initialWorkflow Nothing Fresh Nothing
          case created of
            Left err -> fail (show err)
            Right _ -> pure ()
          firstConn <- connOver backend nullTracer
          first <- withWorkflow firstConn eventTestIdentity (WorkflowId workflowText) Nothing $ \wctx ->
            setEvent wctx "progress" ("first" :: Text)
          first @?= Right ()
          -- A replay reaches the same slot with a different value and does not
          -- republish: the recorded step wins.
          replayConn <- connOver backend nullTracer
          replayed <- withWorkflow replayConn eventTestIdentity (WorkflowId workflowText) Nothing $ \wctx ->
            setEvent wctx "progress" ("second" :: Text)
          replayed @?= Right ()
          let readerText = workflowText <> "-reader"
          readerCreated <- SystemDB.initWorkflow backend ((newWorkflow readerText) {newWorkflowName = Just "L2EventReplayReader"}) Nothing Fresh Nothing
          case readerCreated of
            Left err -> fail (show err)
            Right _ -> pure ()
          readerConn <- connOver backend nullTracer
          readBack <- (withWorkflow readerConn eventTestIdentity (WorkflowId readerText) Nothing $ \wctx ->
            getEvent wctx (WorkflowId workflowText) "progress" (millisDuration 0) :: IO (Either (Error EngineOnly) (Maybe Text)))
          readBack @?= Right (Just "first"),
      testCase "progress events survive recovery without republishing" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-event-recovery-" <> Text.take 12 suffix
            workflowText = "hs-l2-event-recovery-id-" <> suffix
            key = newWorkflowKey "progress"
        offer <- newEmptyMVar
        release <- newEmptyMVar
        putMVar offer ("first" :: Text)
        config0 <- configFromEnv appName
        let config = config0 {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
            -- The second execution finds the offer taken: only a replayed
            -- set step keeps the published value at "first".
            body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) Int)
            body () wctx = do
              proposal <- tryTakeMVar offer
              published <- setEvent wctx "progress" (maybe "republished" id proposal)
              case published of
                Left err -> pure (Left err)
                Right () -> takeMVar release >> pure (Right 7)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRef dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          exec <- launchEventExec dbos isolatedEnvironment
          worker <- async (runWfRef exec ref (runOptionsDefault {runWorkflowId = Just workflowText}) Nothing)
          waitForPublish dbos (WorkflowId workflowText)
          shutdown dbos
          cancel worker
          putMVar release ()
          _ <- launchEventExec dbos isolatedEnvironment
          ran <- runWfRef exec ref (runOptionsDefault {runWorkflowId = Just workflowText}) Nothing
          case ran of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError Int
              assertEqual "the recovered run records its result" (Right 7) decoded
            other -> fail (show other)
          final <- getWorkflowEvent dbos (WorkflowId workflowText) "progress" (millisDuration 100)
          case final of
            Right (Just stored) -> do
              let decoded = decodeWorkflowValue "event" (Just stored) :: Either CodecError Text
              assertEqual "recovery kept the first published value" (Right "first") decoded
            other -> fail (show other),
      testCase "reading through another instance from inside a workflow is refused" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            otherName = "hs-l2-event-other-" <> Text.take 12 suffix
            ownerName = "hs-l2-event-owner-" <> Text.take 12 suffix
            readerKey = newWorkflowKey "reads_through_other"
            inStepKey = newWorkflowKey "reads_in_step"
        otherConfig0 <- configFromEnv otherName
        ownerConfig0 <- configFromEnv ownerName
        let otherConfig = otherConfig0 {configAppVersion = Just ("other-v-" <> suffix), configExecutorId = Just ("other-exec-" <> suffix)}
            ownerConfig = ownerConfig0 {configAppVersion = Just ("owner-v-" <> suffix), configExecutorId = Just ("owner-exec-" <> suffix)}
        bracket (newDBOS otherConfig) shutdown $ \other ->
          bracket (newDBOS ownerConfig) shutdown $ \owner -> do
            let body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) (Maybe Int))
                body () wctx = do
                  built <- pendingGetEvent other wctx (WorkflowId "wf-1") "answer" (millisDuration 0)
                  built.pendingRun
            ownerRegistered <- registerDBOSWorkflowRef owner readerKey body
            readerRef <- case ownerRegistered of
              Left err  -> fail (show err)
              Right ref -> pure ref
            -- From inside a step the read is plain: nothing is checkpointed,
            -- so the two halves are never combined and there is nothing to
            -- refuse.
            let inStepBody :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) (Maybe Int))
                inStepBody () wctx = do
                  stepped <- runWorkflowStep wctx "read" $ \inner -> do
                    built <- pendingGetEvent other inner.stepCtxWorkflow (WorkflowId "wf-1") "answer" (millisDuration 0)
                    built.pendingRun
                  pure $ case stepped of
                    Left err  -> Left err
                    Right read -> read
            inStepRegistered <- registerDBOSWorkflowRef owner inStepKey inStepBody
            inStepRef <- case inStepRegistered of
              Left err  -> fail (show err)
              Right ref -> pure ref
            execOther <- launchEventExec other isolatedEnvironment
            execOwner <- launchEventExec owner isolatedEnvironment
            ran <- runDBOSWorkflowRef execOwner readerRef runOptionsDefault (Just (encodeWorkflowValue ()))
            case ran of
              Left (WrongInstance _) -> pure ()
              other                  -> fail ("expected a wrong-instance refusal, got: " <> show other)
            ranInStep <- runDBOSWorkflowRef execOwner inStepRef runOptionsDefault (Just (encodeWorkflowValue ()))
            case ranInStep of
              Right (Just stored) -> do
                let decoded = decodeWorkflowValue "result" (Just stored) :: Either CodecError (Maybe Int)
                decoded @?= Right Nothing
              other -> fail ("expected a plain read of nothing, got: " <> show other),
      testCase "library calls driven out of build order keep the ids they were built with" $ do
        fresh <- UUID.V4.nextRandom
        let suffix = Text.pack (UUID.toString fresh)
            appName = "hs-l2-joins-out-of-order-app-" <> Text.take 12 suffix
            workflowText = "joins-out-of-order-" <> suffix
            key = newWorkflowKey "joins"
        base <- configFromEnv appName
        let config = base {configAppVersion = Just ("v-" <> suffix), configExecutorId = Just ("exec-" <> suffix)}
        bracket (newDBOS config) shutdown $ \dbos -> do
          let body :: forall exec. () -> WorkflowCtx exec IO -> IO (Either (Error EngineOnly) ())
              body () wctx = do
                -- Built a, b, c, d: the order their ids come from the
                -- counter in, and the order a replay builds them in again.
                a <- pendingSleep wctx (millisDuration 1)
                b <- pendingSetEvent wctx "b" (1 :: Int)
                c <- (pendingGetEvent dbos wctx (WorkflowId "no-such-workflow") "nothing" (millisDuration 0) :: IO (PendingStep exec IO (Either (Error EngineOnly) (Maybe Int))))
                d <- (pendingWorkflowStep wctx "after" (\_ -> pure (Right (1 :: Int))) :: IO (PendingStep exec IO (Either (Error EngineOnly) Int)))
                idsOk <- case (pendingStepId a, pendingStepId b, pendingStepId c, pendingStepId d) of
                  (Just 0, Just 1, Just 2, Just 4) -> pure True
                  _                                -> pure False
                if not idsOk
                  then pure (Left (StepFailed "joins" "ids were taken out of source order"))
                  else do
                    -- ...and driven d, c, b, a.
                    dResult <- d.pendingRun
                    cResult <- c.pendingRun
                    bResult <- b.pendingRun
                    aResult <- a.pendingRun
                    pure $ case (dResult, cResult, bResult, aResult) of
                      (Right _, Right Nothing, Right _, Right _) -> Right ()
                      _ -> Left (StepFailed "joins" "a branch answered wrong")
          registered <- registerDBOSWorkflowRef dbos key body
          ref <- case registered of
            Left err  -> fail (show err)
            Right ref -> pure ref
          exec <- launchEventExec dbos isolatedEnvironment
          ran <- runDBOSWorkflowRef exec ref (runOptionsDefault {runWorkflowId = Just workflowText}) (Just (encodeWorkflowValue ()))
          case ran of
            Right _ -> pure ()
            other   -> fail ("the workflow failed: " <> show other)
          reader <- getBackend
          listed <- SystemDB.listWorkflowSteps reader (WorkflowId workflowText) True Nothing Nothing Nothing
          case listed of
            Right rows -> do
              let recorded = map (\row -> (row.stepRecordStepId, row.stepRecordStepName)) rows
              recorded @?= [(0, "DBOS.sleep"), (1, "DBOS.setEvent"), (2, "DBOS.getEvent"), (3, "DBOS.sleep"), (4, "after")]
            other -> fail ("expected the recorded steps, got: " <> show other)
    ]

-- * Engine-only driver aliases

-- | The engine-only driver aliases the tree above reads through. Local
-- copies are deliberate: this module carries only the aliases it uses.
runWfRef :: Executor IO -> WorkflowRef IO EngineOnly -> RunOptions -> Maybe SerializedWorkflowValue -> IO (Either (Error EngineOnly) (Maybe SerializedWorkflowValue))
runWfRef = runDBOSWorkflowRef

-- | Wait until a workflow has published its event: the row appears before
-- a blocked body proceeds, so a bounded poll always terminates.
waitForPublish :: DBOS IO -> WorkflowId -> IO ()
waitForPublish dbos workflowId = go (50 :: Int)
  where
    go 0 = fail "the workflow never published an event"
    go n = do
      found <- getWorkflowEvent dbos workflowId "progress" (millisDuration 0)
      case found of
        Right (Just _) -> pure ()
        _ -> threadDelay 200000 >> go (n - 1)

isolatedEnvironment :: Environment
isolatedEnvironment =
  Environment
    { environmentCloud = False,
      environmentAppId = "",
      environmentAppName = Nothing,
      environmentAppVersion = Nothing,
      environmentExecutorId = Nothing
    }

stepNames :: Postgres.PostgresSystemDB -> Text -> IO [(Int, Text)]
stepNames backend workflowText = do
  rows <- SystemDB.listWorkflowSteps backend (WorkflowId workflowText) True Nothing Nothing Nothing
  pure $ case rows of
    Left _ -> []
    Right steps -> [(record.stepRecordStepId, record.stepRecordStepName) | record <- steps]

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted. The
-- recovery case below keeps its own launched instance: it needs a distinct
-- application identity per execution.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

-- | The suite backend behind the per-test @\backend ->@ shape, so call
-- sites keep their layout.
withSuiteBackend :: IO Postgres.PostgresSystemDB -> (Postgres.PostgresSystemDB -> IO a) -> IO a
withSuiteBackend getBackend action = getBackend >>= action
