{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Public workflow-step behavior against the configured live SystemDB.
module DBOS.Transact.StepTest (tests) where

import DBOS.Prelude
import Data.Aeson (FromJSON, ToJSON)
import Data.Text qualified as Text
import Data.UUID qualified as UUID
import Data.UUID.V4 qualified as UUID.V4
import DBOS.SystemDB (NewWorkflow (..), Submission (..), SystemDB (..), newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.Postgres qualified as Postgres
import DBOS.Transact
  (
    EngineOnly,
    Error (..),
    StepCtx,
    WorkflowCtx,
    WorkflowId (..)
  )
import DBOS.Transact.Logger (nullTracer)
import DBOS.Transact.Identity (Identity (..))
import DBOS.Transact.StepCases
  ( StepFixture (..),
    checkDurableSleep,
    checkNestedEnclosing,
    checkNestedPlain,
    checkNestedStepView,
    checkNestedWorkflowId,
    checkSmuggledPlain,
    checkPendingScoped,
    checkRecordReplay,
    checkScopedView,
    checkTokenQuiet,
    scenarioDurableSleep,
    scenarioNestedEnclosing,
    scenarioNestedPlain,
    scenarioNestedStepView,
    scenarioNestedWorkflowId,
    scenarioSmuggledPlain,
    scenarioPendingScoped,
    scenarioRecordReplay,
    scenarioScopedView,
    scenarioTokenQuiet,
  )
import DBOS.Transact qualified as Transact
import DBOS.Transact.ContextTest (connOver)
import Test.Tasty (TestTree, testGroup, withResource)
import Test.Tasty.HUnit (testCase)

-- | The application identity the scoped runner installs: the app
-- identity, not the execution identity the state mints internally.
scopedTestIdentity :: Identity
scopedTestIdentity =
  Identity
    { identityAppName = "test-app",
      identityAppVersion = "1.0.0",
      identityExecutorId = "test-executor",
      identityAppId = ""
    }

-- | One backend for the whole group: pools are per-backend, so sharing
-- bounds connections no matter how many tests run or are interrupted.
acquireSuiteBackend :: IO Postgres.PostgresSystemDB
acquireSuiteBackend = do
  config <- Postgres.configFromEnv
  backend <- Postgres.acquirePostgresSystemDB config nullTracer
  Postgres.activatePostgresSystemDB backend
  pure backend

tests :: TestTree
tests =
  withResource acquireSuiteBackend Postgres.releasePostgresSystemDB $ \getBackend ->
    testGroup
      "Durable step"
      [ testCase "a recorded workflow step runs once and replays" $ do
          fixture <- mkStepFixture getBackend
          out <- scenarioRecordReplay fixture
          either fail pure (checkRecordReplay out),
      testCase "a step inside a step body runs plainly and takes no id" $ do
          fixture <- mkStepFixture getBackend
          out <- scenarioNestedPlain fixture
          either fail pure (checkNestedPlain out),
      testCase "a scoped step runs once and replays through the workflow view" $ do
          fixture <- mkStepFixture getBackend
          out <- scenarioScopedView fixture
          either fail pure (checkScopedView out),
      testCase "a nested step through the step view is plain and takes no id" $ do
          fixture <- mkStepFixture getBackend
          out <- scenarioNestedStepView fixture
          either fail pure (checkNestedStepView out),
      testCase "a nested step returns its workflow's id and records only the outer step" $ do
          fixture <- mkStepFixture getBackend
          out <- scenarioNestedWorkflowId fixture
          either fail pure (checkNestedWorkflowId out),
      testCase "a recorded step through a captured parent goes plain and takes no id" $ do
          fixture <- mkStepFixture getBackend
          out <- scenarioSmuggledPlain fixture
          either fail pure (checkSmuggledPlain out),
      testCase "a pending scoped step claims its id at build and replays" $ do
          fixture <- mkStepFixture getBackend
          out <- scenarioPendingScoped fixture
          either fail pure (checkPendingScoped out),
      testCase "durable sleep reuses its recorded wake time" $ do
          fixture <- mkStepFixture getBackend
          out <- scenarioDurableSleep fixture
          either fail pure (checkDurableSleep out),
      testCase "a nested step reports the step that encloses it" $ do
          fixture <- mkStepFixture getBackend
          out <- scenarioNestedEnclosing fixture
          either fail pure (checkNestedEnclosing out),
      testCase "a cancellation token outside a step never fires" $ do
          fixture <- mkStepFixture getBackend
          out <- scenarioTokenQuiet fixture
          either fail pure (checkTokenQuiet out)
    ]

-- | One fixture per leaf over the shared backend: fresh UUIDs per case, row
-- creation plus checkpoint reads through the backend, and the wall-clock
-- quiet-token sense.
mkStepFixture :: IO Postgres.PostgresSystemDB -> IO (StepFixture IO)
mkStepFixture getBackend = pure StepFixture
  { sfConnection = getBackend >>= \backend -> connOver backend nullTracer,
    sfFreshId = \base -> do
      freshId <- UUID.V4.nextRandom
      pure (WorkflowId ("hs-l2-step-" <> base <> "-" <> Text.pack (UUID.toString freshId))),
    sfIdentity = scopedTestIdentity,
    sfInitRow = \wid@(WorkflowId widText) -> do
      backend <- getBackend
      created <- initWorkflow backend (newWorkflow widText) {newWorkflowName = Just "L2StepCases"} Nothing Fresh Nothing
      case created of
        Left err -> throwIO (userError (show err))
        Right _ -> pure (),
    sfCheckStep = \wid step name -> do
      backend <- getBackend
      placed <- checkStep backend wid step name
      case placed of
        Right (Just _) -> pure True
        _ -> pure False,
    sfListStepNames = \wid -> do
      backend <- getBackend
      listed <- SystemDB.listSteps backend wid False Nothing Nothing Nothing
      case listed of
        Right rows -> pure (map (.stepRecordStepName) rows)
        Left err -> throwIO (userError (show err))
  }

-- | Whether a token stays quiet for 50ms: the claim is about the whole
-- life of the token, not the instant of the call, so a token about to
-- fire would have fired within the wait. Mirrors the oracle's
-- @stayed_quiet@ over @CancellationToken::cancelled@.
stayedQuiet :: StrictTVar IO Bool -> IO Bool
stayedQuiet token = do
  result <- timeout 50000 (atomically (readTVar token >>= check))
  pure (result == Nothing)

-- | The simple step runner at the engine-only channel: top-level test
-- calls do not sit in an annotated body, so the channel needs pinning.
runStep :: (FromJSON value, ToJSON value)
        => WorkflowCtx exec IO -> Text -> (StepCtx exec IO -> IO value) -> IO (Either (Error EngineOnly) value)
runStep wctx name body = Transact.runStep wctx name body
