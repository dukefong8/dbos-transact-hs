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
  ( CodecError,
    Config (..),
    Ctx,
    DBOS,
    Environment (..),
    Error (..),
    RunOptions (..),
    WorkflowId (..),
    configFromEnv,
    decodeWorkflowValue,
    firstStepStatus,
    getEvent,
    getWorkflowEvent,
    launchWithEnvironment,
    newDBOS,
    newWorkflowKey,
    nextStepId,
    nextStepMarker,
    nullTracer,
    registerDBOSWorkflowRef,
    runDBOSWorkflowRef,
    runOptionsDefault,
    runWorkflowStepWith,
    setEvent,
    shutdown,
    stepOptionsDefault,
    withAttempt,
  )
import DBOS.Transact.ContextTest (ctxOver)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))

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
          let action ctx = do
                published <- setEvent ctx "progress" ("ready" :: Text)
                case published of
                  Left err -> pure (Left err)
                  Right () -> getEvent ctx (WorkflowId workflowText) "progress" (millisDuration 100)
          firstContext <- ctxOver backend nullTracer workflowText
          first <- action firstContext
          case first of
            Right (Just value) -> assertEqual "the event is visible to a workflow reader" ("ready" :: Text) value
            other -> fail (show other)
          replayContext <- ctxOver backend nullTracer workflowText
          replay <- action replayContext
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
          publisherContext <- ctxOver backend nullTracer publisherText
          published <- setEvent publisherContext "answer" (42 :: Int)
          published @?= Right ()
          readerContext <- ctxOver backend nullTracer readerText
          readOutside <- (getEvent readerContext (WorkflowId publisherText) "answer" (millisDuration 0) :: IO (Either Error (Maybe Int)))
          readOutside @?= Right (Just 42)
          outsideSteps <- stepNames backend readerText
          outsideSteps @?= [(0, getEventStepName), (1, sleepStepName)]
          inStepContext <- ctxOver backend nullTracer inStepText
          readInside <- (runWorkflowStepWith stepOptionsDefault inStepContext "read" (\inner -> getEvent inner (WorkflowId publisherText) "answer" (millisDuration 0)) :: IO (Either Error (Maybe Int)))
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
          context <- ctxOver backend nullTracer workflowText
          marker <- nextStepMarker context
          (refused, before, after) <- withAttempt context marker (firstStepStatus 0) $ \inner -> do
            before <- nextStepId inner
            refused <- setEvent inner "progress" ("ready" :: Text)
            after <- nextStepId inner
            pure (refused, before, after)
          refused @?= Left (InsideStep "set_event")
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
          firstContext <- ctxOver backend nullTracer workflowText
          first <- setEvent firstContext "progress" ("first" :: Text)
          first @?= Right ()
          -- A replay reaches the same slot with a different value and does not
          -- republish: the recorded step wins.
          replayContext <- ctxOver backend nullTracer workflowText
          replayed <- setEvent replayContext "progress" ("second" :: Text)
          replayed @?= Right ()
          let readerText = workflowText <> "-reader"
          readerCreated <- SystemDB.initWorkflow backend ((newWorkflow readerText) {newWorkflowName = Just "L2EventReplayReader"}) Nothing Fresh Nothing
          case readerCreated of
            Left err -> fail (show err)
            Right _ -> pure ()
          readerContext <- ctxOver backend nullTracer readerText
          readBack <- (getEvent readerContext (WorkflowId workflowText) "progress" (millisDuration 0) :: IO (Either Error (Maybe Text)))
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
            body :: () -> Ctx IO -> IO (Either Error Int)
            body () ctx = do
              proposal <- tryTakeMVar offer
              published <- setEvent ctx "progress" (maybe "republished" id proposal)
              case published of
                Left err -> pure (Left err)
                Right () -> takeMVar release >> pure (Right 7)
        bracket (newDBOS config) shutdown $ \dbos -> do
          registered <- registerDBOSWorkflowRef dbos key body
          ref <- case registered of
            Left err -> fail (show err)
            Right ref -> pure ref
          started <- launchWithEnvironment dbos isolatedEnvironment
          case started of
            Left err -> fail (show err)
            Right () -> pure ()
          worker <- async (runDBOSWorkflowRef dbos ref (runOptionsDefault {runWorkflowId = Just workflowText}) Nothing)
          waitForPublish dbos (WorkflowId workflowText)
          shutdown dbos
          cancel worker
          putMVar release ()
          relaunched <- launchWithEnvironment dbos isolatedEnvironment
          case relaunched of
            Left err -> fail (show err)
            Right () -> pure ()
          ran <- runDBOSWorkflowRef dbos ref (runOptionsDefault {runWorkflowId = Just workflowText}) Nothing
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
            other -> fail (show other)
    ]

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
