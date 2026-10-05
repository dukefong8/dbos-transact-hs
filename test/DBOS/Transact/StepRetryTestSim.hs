{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | 'DBOS.Transact.StepRetryTest' mirrored under IOSim over the in-memory
-- backend: the real ported runner ('runStepWith') with backoff, timeouts,
-- preemption, and cancellation on virtual time. Scenarios and checks are
-- shared; this module owns the sim factory and the sim-only extra — the
-- exact 'WorkflowEvent' record per case, the typed asserts behind the
-- announcements the live tree writes through FastLogger.
module DBOS.Transact.StepRetryTestSim (tests) where

import Control.Monad.IOSim (IOSim, SimTrace, selectTraceEventsDynamic)
import DBOS.DualStack (simCase)
import DBOS.IOSimTracer (simTracer)
import DBOS.Prelude
import DBOS.SystemDB (NewWorkflow (..), Submission (..), WorkflowId (..), millisDuration, newWorkflow)
import DBOS.SystemDB qualified as SystemDB
import DBOS.SystemDB.IOSim (memConnectionOn, newMemDB)
import DBOS.Transact
  ( Error (..),
    EngineOnly,
    Identity (..),
    WorkflowEvent (..),
    renderTransactError,
    WorkflowCtx,
    withWorkflow,
  )
import DBOS.Transact.StepRetryCases
  ( StepRetryFixture (..),
    checkPlainNotPreemptible,
    checkPreemptible,
    checkRetryDeclined,
    checkRetryDefault,
    checkRetryExhausted,
    checkRetryMidDecline,
    checkRetryReplay,
    checkRetryThird,
    checkStepTimeout,
    checkStepWithinTimeout,
    checkTimeoutAllTimeout,
    checkTimeoutFreshRetry,
    checkTimeoutStopsBody,
    checkTokenDrop,
    checkTokenFirst,
    checkTokenQuiet,
    scenarioPlainNotPreemptible,
    scenarioPreemptible,
    scenarioRetryDeclined,
    scenarioRetryDefault,
    scenarioRetryExhausted,
    scenarioRetryMidDecline,
    scenarioRetryReplay,
    scenarioRetryThird,
    scenarioStepTimeout,
    scenarioStepWithinTimeout,
    scenarioTimeoutAllTimeout,
    scenarioTimeoutFreshRetry,
    scenarioTimeoutStopsBody,
    scenarioTokenDrop,
    scenarioTokenFirst,
    scenarioTokenQuiet,
  )
import Data.Text qualified as Text
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit ((@?=))

simStepIdentity :: Identity
simStepIdentity =
  Identity
    { identityAppName = "sim-app",
      identityAppVersion = "0.0.0",
      identityExecutorId = "sim-executor",
      identityAppId = ""
    }

-- | One fixture per leaf over a fresh in-memory database: workflow ids are
-- minted per scenario and every run — including replays, cancellations,
-- and preemptions — shares the same store in the same simulation.
simStepFixture :: forall s. IOSim s (StepRetryFixture (IOSim s))
simStepFixture = do
  mem <- newMemDB
  fresh <- newTVarIO (0 :: Int)
  pure
    StepRetryFixture
      { srfFreshWorkflowId = do
          n <- atomically (readTVar fresh >>= \k -> writeTVar fresh (k + 1) >> pure k)
          let widText = "sim-step-" <> Text.pack (show n)
          created <- SystemDB.initWorkflow mem ((newWorkflow widText) {newWorkflowName = Just "SimStepRetryTest"}) Nothing Fresh Nothing
          case created of
            Left err -> error (show err)
            Right _ -> pure (WorkflowId widText),
        srfRun = \wid action -> do
          conn <- memConnectionOn mem simTracer
          withWorkflow conn simStepIdentity wid Nothing action,
        srfCancel = \wids ->
          SystemDB.cancelWorkflows mem wids False Nothing >>= either (error . show) (const (pure ())),
        srfListSteps = \wid ->
          SystemDB.listSteps mem wid False Nothing Nothing Nothing >>= either (error . show) pure
      }

tests :: TestTree
tests =
  testGroup
    "Step retries (Sim)"
    [ simCase simStepFixture "a step that fails twice succeeds on the third attempt" scenarioRetryThird checkRetryThird traceRetryThird,
      simCase simStepFixture "exhausted retries carry every attempt's failure" scenarioRetryExhausted checkRetryExhausted traceRetryExhausted,
      simCase simStepFixture "the default does not retry and does not wrap" scenarioRetryDefault checkRetryDefault traceRetryDefault,
      simCase simStepFixture "a retried step replays from its single checkpoint" scenarioRetryReplay checkRetryReplay traceRetryReplay,
      simCase simStepFixture "a declined failure stops retrying immediately" scenarioRetryDeclined checkRetryDeclined traceRetryDeclined,
      simCase simStepFixture "declining mid-policy keeps the earlier failures" scenarioRetryMidDecline checkRetryMidDecline traceRetryMidDecline,
      simCase simStepFixture "a step that hangs is stopped at its timeout" scenarioStepTimeout checkStepTimeout traceStepTimeout,
      simCase simStepFixture "a step within its timeout is unaffected" scenarioStepWithinTimeout checkStepWithinTimeout traceStepWithinTimeout,
      simCase simStepFixture "a timed-out body stops rather than continuing" scenarioTimeoutStopsBody checkTimeoutStopsBody traceTimeoutStopsBody,
      simCase simStepFixture "a timed-out attempt is retried with a fresh timeout" scenarioTimeoutFreshRetry checkTimeoutFreshRetry traceTimeoutFreshRetry,
      simCase simStepFixture "every attempt timing out reports each timeout" scenarioTimeoutAllTimeout checkTimeoutAllTimeout traceTimeoutAllTimeout,
      simCase simStepFixture "a plain step is not preemptible" scenarioPlainNotPreemptible checkPlainNotPreemptible tracePlainNotPreemptible,
      simCase simStepFixture "a completed step leaves its token alone" scenarioTokenQuiet checkTokenQuiet traceTokenQuiet,
      simCase simStepFixture "the cancellation token fires before the body is dropped" scenarioTokenFirst checkTokenFirst traceTokenFirst,
      simCase simStepFixture "a dropped step fires its cancellation token" scenarioTokenDrop checkTokenDrop traceTokenDrop,
      simCase simStepFixture "a preemptible step stops and records no outcome" scenarioPreemptible checkPreemptible tracePreemptible
    ]

-- * Typed-event assertions (sim-only): the exact 'WorkflowEvent' record
-- each case must emit. Retry details are computed with 'renderTransactError'
-- over the same error values, so the asserts pin the sequence, not the
-- rendering.

traceEvents :: SimTrace a -> [WorkflowEvent]
traceEvents = selectTraceEventsDynamic

traceRetryThird :: SimTrace a -> IO ()
traceRetryThird tr =
  traceEvents tr
    @?= [ StepRetrying "flaky" 0 1 3 1 (renderTransactError (StepFailed "flaky" "boom" :: (Error EngineOnly))),
          StepRetrying "flaky" 0 2 3 2 (renderTransactError (StepFailed "flaky" "boom" :: (Error EngineOnly))),
          StepOutputRecorded "flaky" 0
        ]

traceRetryExhausted :: SimTrace a -> IO ()
traceRetryExhausted tr =
  traceEvents tr
    @?= [ StepRetrying "doomed" 0 1 2 1 (renderTransactError (StepFailed "doomed" "boom" :: (Error EngineOnly))),
          StepErrorRecorded "doomed" 0
        ]

traceRetryDefault :: SimTrace a -> IO ()
traceRetryDefault tr =
  traceEvents tr @?= [StepErrorRecorded "plain" 0]

traceRetryReplay :: SimTrace a -> IO ()
traceRetryReplay tr =
  traceEvents tr
    @?= [ StepRetrying "flaky" 0 1 3 1 (renderTransactError (StepFailed "flaky" "boom" :: (Error EngineOnly))),
          StepOutputRecorded "flaky" 0,
          StepReplaying "flaky" 0
        ]

traceRetryDeclined :: SimTrace a -> IO ()
traceRetryDeclined tr =
  traceEvents tr
    @?= [ StepDeclined "declined" 0 1 (renderTransactError (StepFailed "declined" "boom" :: (Error EngineOnly))),
          StepErrorRecorded "declined" 0
        ]

traceRetryMidDecline :: SimTrace a -> IO ()
traceRetryMidDecline tr =
  traceEvents tr
    @?= [ StepRetrying "pick" 0 1 3 1 (renderTransactError (StepFailed "pick" "first" :: (Error EngineOnly))),
          StepDeclined "pick" 0 2 (renderTransactError (StepFailed "pick" "second" :: (Error EngineOnly))),
          StepErrorRecorded "pick" 0
        ]

traceStepTimeout :: SimTrace a -> IO ()
traceStepTimeout tr =
  traceEvents tr @?= [StepAttemptTimedOut "slow" 0 5, StepErrorRecorded "slow" 0]

traceStepWithinTimeout :: SimTrace a -> IO ()
traceStepWithinTimeout tr =
  traceEvents tr @?= [StepOutputRecorded "quick" 0]

-- | Two hangs, each announced with its timeout, retried with a fresh one.
traceTimeoutFreshRetry :: SimTrace a -> IO ()
traceTimeoutFreshRetry tr =
  traceEvents tr
    @?= [ StepAttemptTimedOut "flaky" 0 20,
          StepRetrying "flaky" 0 1 3 1 (renderTransactError (StepTimeout "flaky" (millisDuration 20) :: (Error EngineOnly))),
          StepAttemptTimedOut "flaky" 0 20,
          StepRetrying "flaky" 0 2 3 2 (renderTransactError (StepTimeout "flaky" (millisDuration 20) :: (Error EngineOnly))),
          StepOutputRecorded "flaky" 0
        ]

traceTimeoutAllTimeout :: SimTrace a -> IO ()
traceTimeoutAllTimeout tr =
  traceEvents tr
    @?= [ StepAttemptTimedOut "slow" 0 10,
          StepRetrying "slow" 0 1 2 1 (renderTransactError (StepTimeout "slow" (millisDuration 10) :: (Error EngineOnly))),
          StepAttemptTimedOut "slow" 0 10,
          StepErrorRecorded "slow" 0
        ]

traceTimeoutStopsBody :: SimTrace a -> IO ()
traceTimeoutStopsBody tr =
  traceEvents tr @?= [StepAttemptTimedOut "slow" 0 5, StepErrorRecorded "slow" 0]

tracePlainNotPreemptible :: SimTrace a -> IO ()
-- | The plain path never announces a start: only the recorded output.
tracePlainNotPreemptible tr =
  traceEvents tr @?= [StepOutputRecorded "plain" 0]

traceTokenQuiet :: SimTrace a -> IO ()
traceTokenQuiet tr =
  traceEvents tr @?= [StepOutputRecorded "quiet" 0]

traceTokenFirst :: SimTrace a -> IO ()
traceTokenFirst tr =
  traceEvents tr @?= [StepAttemptTimedOut "slow" 0 50, StepErrorRecorded "slow" 0]

-- | The dropped worker is killed before it announces anything.
traceTokenDrop :: SimTrace a -> IO ()
traceTokenDrop tr =
  traceEvents tr @?= []

-- | The 50ms timeout fires while the body is parked, the retry starts,
-- and the cancellation preempts it: interruption announced, nothing
-- recorded.
tracePreemptible :: SimTrace a -> IO ()
tracePreemptible tr =
  traceEvents tr
    @?= [ StepAttemptTimedOut "preemptible" 0 50,
          StepRetrying "preemptible" 0 1 3 1 (renderTransactError (StepTimeout "preemptible" (millisDuration 50) :: (Error EngineOnly))),
          StepPreempted "preemptible" 0
        ]
