{-# LANGUAGE BlockArguments    #-}
{-# LANGUAGE GHC2024           #-}
{-# LANGUAGE OverloadedStrings #-}

module DBOS.Logger
  ( logDebug
  , logInfo
  , logError
  , cleanupLogger
  ) where

import Colog.Core.Action (LogAction (..), cmap)
import Colog.Message qualified as Colog
import Colog.Monad (LoggerT, usingLoggerT)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.Text (Text)
import GHC.Stack (HasCallStack, withFrozenCallStack)
import System.IO.Unsafe (unsafePerformIO)
import System.Log.FastLogger (FastLogger, LogStr, LogType' (LogStdout), defaultBufSize, newFastLogger, toLogStr)

fastLogger :: (FastLogger, IO ())
fastLogger = unsafePerformIO $ newFastLogger (LogStdout defaultBufSize)
{-# NOINLINE fastLogger #-}

loggerAction :: MonadIO m => FastLogger -> LogAction m LogStr
loggerAction logger' = LogAction $ \logStr -> liftIO $ logger' logStr

fmtLogStr :: Colog.Message -> LogStr
fmtLogStr = toLogStr . (<> "\n") . Colog.fmtMessage

logger :: MonadIO m => LoggerT Colog.Message m a -> m a
logger = usingLoggerT $ cmap fmtLogStr (loggerAction (fst fastLogger))

cleanupLogger :: IO ()
cleanupLogger = snd fastLogger

logDebug :: HasCallStack => Text -> IO ()
logDebug msg = withFrozenCallStack $ logger $ Colog.logDebug msg

logInfo :: HasCallStack => Text -> IO ()
logInfo msg = withFrozenCallStack $ logger $ Colog.logInfo msg

logError :: HasCallStack => Text -> IO ()
logError msg = withFrozenCallStack $ logger $ Colog.logError msg
