{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Internal structured logging (Rule 4: plain Haskell, no Bluefin imports;
-- Rule 5: explicit 'LogAction', never ambient). 'DbosLogMsg' mirrors Rust's
-- @tracing@ fields (severity plus the workflow a message belongs to) instead
-- of free text. Production instantiates the action once at launch with
-- 'withStdoutLogger'; simulations instantiate it over @say@ and assert with
-- @selectTraceEventsSay@.
module DBOS.Transact.Log
  ( DbosLogMsg (..),
    DbosSeverity (..),
    nullLogAction,
    withStdoutLogger,
  )
where

import DBOS.Prelude
import Colog.Core.Action (LogAction (..))
import Data.Text (Text)
import DBOS.SystemDB.Types (WorkflowId (..))
import System.Log.FastLogger (LogType' (LogStdout), defaultBufSize, newFastLogger, toLogStr)

data DbosSeverity
  = DbosDebug
  | DbosInfo
  | DbosWarn
  | DbosError
  deriving stock (Eq, Show)

-- | A structured log message: severity plus the workflow it belongs to.
data DbosLogMsg = DbosLogMsg
  { logSeverity :: DbosSeverity,
    logMessage :: Text,
    logWorkflow :: Maybe WorkflowId
  }
  deriving stock (Eq, Show)

-- | A logger that discards everything. Used by pure tests and as the default
-- where no launch has installed a real backend.
nullLogAction :: Applicative m => LogAction m DbosLogMsg
nullLogAction = LogAction (\_ -> pure ())

-- | Run an action with a buffered-stdout logger, cleaning the backend up
-- afterwards. Call once at launch and thread the action down explicitly.
withStdoutLogger :: (LogAction IO DbosLogMsg -> IO a) -> IO a
withStdoutLogger use =
  bracket
    (newFastLogger (LogStdout defaultBufSize))
    (\(_, cleanup) -> cleanup)
    (\(logger, _) -> use (LogAction (\message -> logger (formatLogMessage message))))
  where
    formatLogMessage message =
      toLogStr
        ( "["
            <> shownSeverity
            <> "]"
            <> workflowSuffix
            <> " "
            <> message.logMessage
            <> "\n"
        )
      where
        shownSeverity = case message.logSeverity of
          DbosDebug -> "DEBUG" :: Text
          DbosInfo -> "INFO"
          DbosWarn -> "WARN"
          DbosError -> "ERROR"
        workflowSuffix = case message.logWorkflow of
          Nothing -> ""
          Just (WorkflowId workflowId) -> " " <> workflowId
