{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The retry loop under both runners: 'IO' with real time, and @IOSim@
-- with virtual time — which is what the 'MonadDelay' constraint buys.
module DBOS.SystemDB.RetryTest
  ( tests,
  )
where

import DBOS.Prelude
import Control.Monad.IOSim (IOSim, runSim, runSimTrace, selectTraceEventsDynamic)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Text (Text)
import Data.Word (Word32)
import DBOS.SystemDB
  ( BackendError (..),
    BackendErrorKind (..),
    Error (..),
    RetryPolicy (..),
    defaultRetryPolicy,
    durationAsMillis,
    durationFromMs,
    jitter,
    renderError,
    shouldRetry,
    withRetry,
  )
import DBOS.Transact (SysdbEvent (..), WorkflowId (..), nullTracer, simTracer)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

tests :: TestTree
tests =
  testGroup
    "SystemDB Retry"
    [ classificationTests,
      jitterTests,
      ioTests,
      simTests
    ]

classificationTests :: TestTree
classificationTests =
  testGroup
    "Classification"
    [ testCase "retries the failures that may pass, and nothing else" $
        (shouldRetry defaultRetryPolicy <$> [transientError, permanentError, connectionError, semanticError])
          @?= [True, False, True, False],
      testCase "opting out covers connection failures only" $ do
        let policy = defaultRetryPolicy {retryPolicyRetryConnectionErrors = False}
        (shouldRetry policy <$> [connectionError, transientError])
          @?= [False, True]
    ]

jitterTests :: TestTree
jitterTests =
  testGroup
    "Jitter"
    [ testCase "jitter stays within [0.5, 1.5) of the backoff" $ do
        backoff <- expectJust "durationFromMs 1000" (durationFromMs 1000)
        let lcg :: Word32 -> Word32
            lcg n = n * 1664525 + 1013904223
            delays = durationAsMillis . (\bits -> jitter bits backoff) <$> take 1000 (iterate lcg 42)
        assertBool
          ("delays out of band: " <> show (minimum delays) <> ".." <> show (maximum delays))
          (all (\ms -> ms >= 500 && ms < 1500) delays)
    ]

ioTests :: TestTree
ioTests =
  testGroup
    "IO"
    [ testCase "a transient failure is retried until it succeeds" $ do
        policy <- fastPolicy
        calls <- newIORef (0 :: Int)
        result <- withRetry policy "test" nullTracer (pure 0) (failUntil calls 3 transientError)
        result @?= Right 3
        readIORef calls >>= (@?= 3),
      testCase "a permanent failure is returned at once" $ do
        policy <- fastPolicy
        calls <- newIORef (0 :: Int)
        _ <- withRetry policy "test" nullTracer (pure 0) (alwaysFail calls permanentError)
        readIORef calls >>= (@?= 1),
      testCase "a semantic error is not retried" $ do
        policy <- fastPolicy
        calls <- newIORef (0 :: Int)
        _ <- withRetry policy "test" nullTracer (pure 0) (alwaysFail calls semanticError)
        readIORef calls >>= (@?= 1),
      testCase "opting out covers connection errors but not contention" $ do
        policy <- (\p -> p {retryPolicyRetryConnectionErrors = False}) <$> fastPolicy
        connectionCalls <- newIORef (0 :: Int)
        _ <- withRetry policy "test" nullTracer (pure 0) (alwaysFail connectionCalls connectionError)
        readIORef connectionCalls >>= (@?= 1)
        transientCalls <- newIORef (0 :: Int)
        result <- withRetry policy "test" nullTracer (pure 0) (failUntil transientCalls 2 transientError)
        result @?= Right 2
        readIORef transientCalls >>= (@?= 2)
    ]

simTests :: TestTree
simTests =
  testGroup
    "IOSim"
    [ testCase "the same loop retries under virtual time" $ do
        case runSim simAction of
          Left _ -> fail "the simulation itself failed"
          Right (Left err) -> fail ("retry returned an error: " <> show err)
          Right (Right attempts) -> attempts @?= 3
        let traced = selectTraceEventsDynamic (runSimTrace simAction) :: [SysdbEvent]
        traced @?= [ SysdbRetryAttempt "test" 1 500 (renderError transientError),
                     SysdbRetryAttempt "test" 2 1000 (renderError transientError)
                   ]
    ]

-- | The same retry loop under @IOSim@: a StrictTVar IO counts attempts, the
-- sim tracer records the structured attempts, and time is virtual.
simAction :: IOSim s (Either Error Int)
simAction = do
  counter <- newTVarIO (0 :: Int)
  let work = do
        attempts <- readTVarIO counter
        atomically (writeTVar counter (attempts + 1))
        pure (if attempts + 1 < 3 then Left transientError else Right (attempts + 1))
  withRetry defaultRetryPolicy "test" simTracer (pure 0) work

-- | A policy with small backoffs, so real waits stay milliseconds.
fastPolicy :: IO RetryPolicy
fastPolicy = do
  backoff <- expectJust "durationFromMs 1" (durationFromMs 1)
  ceiling' <- expectJust "durationFromMs 4" (durationFromMs 4)
  pure defaultRetryPolicy {retryPolicyInitialBackoff = backoff, retryPolicyMaxBackoff = ceiling'}

failUntil :: IORef Int -> Int -> Error -> IO (Either Error Int)
failUntil calls threshold err = do
  attempt <- atomicModifyIORef' calls (\n -> (n + 1, n + 1))
  pure (if attempt < threshold then Left err else Right attempt)

alwaysFail :: IORef Int -> Error -> IO (Either Error Int)
alwaysFail calls err = do
  _ <- atomicModifyIORef' calls (\n -> (n + 1, n + 1))
  pure (Left err)

transientError :: Error
transientError = Backend (BackendError "boom" Nothing Transient)

permanentError :: Error
permanentError = Backend (BackendError "boom" Nothing Permanent)

connectionError :: Error
connectionError = Backend (BackendError "boom" Nothing Connection)

semanticError :: Error
semanticError = ConflictingWorkflow {workflowId = "wf-1", detail = "different function"}

expectJust :: String -> Maybe a -> IO a
expectJust label = maybe (fail label) pure
