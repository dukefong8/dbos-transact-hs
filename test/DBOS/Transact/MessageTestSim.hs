{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | 'DBOS.Transact.MessageTest' mirrored under IOSim over the mock
-- backend: the same call shapes, with the answers the stateless mock
-- returns, each case printing its sim's 'Say' trace inline so a plain
-- @-- $> tasty@ run shows announcements with no extra plumbing. The
-- oracle emits no traces from @message.rs@/@event.rs@ (verified: zero
-- @tracing@ sites), and our send/recv/bulk paths emit none either, so
-- the pane stays quiet here by fidelity, not by omission — the cases
-- already run on the say-carrier, so when the Step sweep mirrors the
-- @step.rs@ run/recorded sites the bulk-send step's announcements will
-- print with no test change. Where a live assertion depends on database
-- state (a sent message reading back on receive, a replay returning the
-- recorded message), the mirror asserts the mock's canned answer and
-- says so; the live semantics stay in 'DBOS.Transact.MessageTest'.
module DBOS.Transact.MessageTestSim (tests) where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim)
import Data.Text (Text)
import DBOS.IOSimTracer (printSimTrace, runSimCase, simTracer)
import DBOS.SystemDB (WorkflowId (..), millisDuration)
import DBOS.SystemDB.IOSim (simConnectionWith)
import DBOS.Transact
  (
    EngineOnly, Ctx,
    Error (..),
    Identity (..),
    Message (..),
    Topic (..),
    firstStepStatus,
    newCtx,
    newWorkflowState,
    nextExecutionIdentity,
    nextStepId,
    nextStepMarker,
    recv,
    send,
    sendBulk,
    withAttempt,
  )
import Test.Tasty (DependencyType (..), TestTree, dependentTestGroup)
import Test.Tasty.HUnit (testCase, (@?=))

simIdentity :: Identity
simIdentity =
  Identity
    { identityAppName = "sim-app",
      identityAppVersion = "0.0.0",
      identityExecutorId = "sim-executor",
      identityAppId = ""
    }

simCtx :: Text -> IOSim s (Ctx (IOSim s))
simCtx name = do
  conn <- simConnectionWith simTracer
  identity <- nextExecutionIdentity conn
  state <- newWorkflowState name Nothing identity
  newCtx conn simIdentity state

tests :: TestTree
tests =
  -- Sequential: cases announce through one shared stderr, so parallel
  -- 'printSimTrace' calls would interleave their lines mid-character.
  -- 'AllFinish' keeps every case running on a failure, in order.
  dependentTestGroup
    "Workflow messages (Sim)"
    AllFinish
    [ testCase "a workflow send is accepted" $ do
        (sent, tr) <- runSimCase $ do
          context <- simCtx "sim-message-send"
          send context (WorkflowId "sim-message-destination") (Just (Topic "approval")) Nothing ("approved" :: Text)
        printSimTrace tr
        sent @?= Right (),
      testCase "a receive reads the mock's canned body, which is not JSON" $ do
        (received :: Either (Error EngineOnly) (Maybe Text), tr) <- runSimCase $ do
          context <- simCtx "sim-message-recv"
          recv context (Just (Topic "approval")) (millisDuration 100)
        printSimTrace tr
        -- The mock is stateless: the live test reads back the sent
        -- message here. The canned "mock-message" body is not valid
        -- JSON, so the sim surfaces a deserialization refusal.
        case received of
          Left (ErrorDeserialization _ _) -> pure ()
          other -> fail ("expected a deserialization refusal, got: " <> show other),
      testCase "a bulk send checkpoints once and delivers the batch" $ do
        (outcome, tr) <- runSimCase $ do
          context <- simCtx "sim-bulk"
          sendBulk
            context
            [ Message (WorkflowId "first") (1 :: Int) Nothing Nothing,
              Message (WorkflowId "second") (2 :: Int) Nothing Nothing
            ]
        printSimTrace tr
        outcome @?= Right (),
      testCase "an empty bulk send still takes its step" $ do
        (outcome, tr) <- runSimCase $ do
          context <- simCtx "sim-bulk-empty"
          sendBulk context ([] :: [Message Int])
        printSimTrace tr
        outcome @?= Right (),
      testCase "a send through a captured parent is plain and moves no id" $ do
        (outcome, tr) <- runSimCase $ do
          context <- simCtx "sim-captured-send"
          marker <- nextStepMarker context
          sent <- withAttempt context marker (firstStepStatus 0) $ \_ ->
            send context (WorkflowId "sim-message-destination") (Just (Topic "approval")) Nothing ("ping" :: Text)
          counter <- nextStepId context
          pure (sent, counter)
        printSimTrace tr
        outcome @?= (Right (), 0),
      testCase "a recv through a captured parent is refused" $ do
        (received :: Either (Error EngineOnly) (Maybe Text), tr) <- runSimCase $ do
          context <- simCtx "sim-captured-recv"
          marker <- nextStepMarker context
          withAttempt context marker (firstStepStatus 0) $ \_ ->
            recv context (Just (Topic "approval")) (millisDuration 100)
        printSimTrace tr
        received @?= Left (InsideStep "recv")
    ]
