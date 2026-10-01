{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}

-- | Sim tracing tooling for test trees (no src counterpart): the engine's
-- @traceM@-only carrier, the say-carrier that also prints, and the case
-- runner. Production code never traces through io-sim — the sim backend
-- takes its tracer as a parameter — so these live in test, with the trees
-- that use them.
module DBOS.IOSimTracer
  ( simTracer,
    simTracerSay,
    runSimCase,
    printSimTrace,
  )
where

import Control.Monad.Class.MonadSay (say)
import Control.Monad.IOSim (IOSim, SimTrace, runSim, runSimTrace, selectTraceEventsSay, traceM)
import Control.Tracer (mkTracer)
import Data.Text (unpack)
import Data.Typeable (Typeable)
import DBOS.Prelude
import DBOS.Transact (LogEvent (..), SomeTracer (..), showSeverity)
import System.IO (hPutStrLn, stderr)

-- | The simulation carrier traces the structured event itself through
-- io-sim's @traceM@, so simulation runs recover their traces by type
-- with 'selectTraceEventsDynamic' instead of matching strings.
simTracer :: SomeTracer (IOSim s)
simTracer = SomeTracer (mkTracer traceM)

-- | Sim carrier that ALSO says each rendered line: typed assertions keep
-- working through 'selectTraceEventsDynamic' while eval runs can print
-- the same events with 'printTraceEventsSay'.
simTracerSay :: SomeTracer (IOSim s)
simTracerSay = SomeTracer (mkTracer emit)
  where
    emit :: (LogEvent e, Typeable e) => e -> IOSim s ()
    emit event = traceM event >> say (unpack (showSeverity (eventSeverity event) <> " " <> renderEvent event))

-- | Print a sim's 'Say' trace to the console's stderr: stderr bypasses
-- the 'tasty' stdout capture, so announcement lines show on the watcher
-- pane instead of accumulating in @ghcid.txt@.
printSimTrace :: SimTrace a -> IO ()
printSimTrace trace = mapM_ (hPutStrLn stderr) (selectTraceEventsSay trace)

-- | Run one sim case to a value plus its trace. Deterministic sims make
-- the double execution agree.
runSimCase :: (forall s. IOSim s a) -> IO (a, SimTrace a)
runSimCase sim =
  case runSim sim of
    Left failure -> fail ("simulation failed: " <> show failure)
    Right result -> pure (result, runSimTrace sim)

