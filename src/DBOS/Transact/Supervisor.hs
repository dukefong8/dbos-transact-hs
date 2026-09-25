{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Internal queue supervisor (Rule 4: plain Haskell, no Bluefin imports).
-- Rounds of claim-and-dispatch over named queues until the executor closes.
-- Dispatched workflows are tracked by the executor, so shutdown reaches
-- them; the loop itself is owned by the application, which cancels it. A
-- failing queue is logged and skipped so one bad queue cannot stop the
-- round; cancellation still interrupts the loop at once.
module DBOS.Transact.Supervisor
  ( superviseForever,
  )
where

import DBOS.Prelude
import Colog.Core.Action (LogAction (..))
import Control.Monad (unless, when)
import DBOS.SystemDB.Types (Duration (..), durationAsMillis)
import DBOS.SystemDB.Types (QueueName (..))
import DBOS.Transact.Executor (Executor (..), dequeuePass)
import DBOS.Transact.Log (DbosLogMsg (..), DbosSeverity (..))
import Data.Functor (void)

superviseForever :: Executor -> [QueueName] -> Duration -> IO ()
superviseForever executor queues duration = loop
  where
    loop = do
      closed <- readTVarIO executor.executorClosed
      unless closed $ do
        mapM_ pollQueue queues
        threadDelay (fromInteger (durationAsMillis duration * 1000))
        loop
    -- The close flag is re-checked per queue so a shutdown landing mid-round
    -- stops claiming instead of parking rows PENDING that need recovery.
    pollQueue queue = do
      closed <- readTVarIO executor.executorClosed
      when (not closed) $ do
        outcome <- try (void (dequeuePass executor queue)) :: IO (Either SomeException ())
        case outcome of
          Right _ -> pure ()
          Left failure
            -- Control errors interrupt like anywhere else: swallowing
            -- ThreadKilled/UserInterrupt here would delay shutdown a full
            -- interval per queue.
            | Just AsyncCancelled <- fromException failure -> throwIO failure
            | otherwise -> do
                let QueueName name = queue
                unLogAction
                  executor.executorLogger
                  (DbosLogMsg DbosWarn ("supervisor skipped a failing queue: " <> name) Nothing)
