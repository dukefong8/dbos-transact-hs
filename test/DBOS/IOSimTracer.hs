{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}

-- | Sim tracing tooling for test trees (no src counterpart): the sim
-- carrier — the structured event plus its rendered line, one event per
-- call — and the case runner and printer over it. Production code never
-- traces through io-sim — the sim backend takes its tracer as a parameter
-- — so these live in test, with the trees that use them.
module DBOS.IOSimTracer
  ( simTracer,
    runSimCase,
    printSimTrace,
  )
where

import Control.Monad.Class.MonadSay (say)
import Control.Monad.IOSim (IOSim, SimTrace, runSim, runSimTrace, selectTraceEventsSay, traceM)
import Control.Tracer (mkTracer)
import Data.Text (unpack)
import DBOS.Prelude
import DBOS.Transact.Logger (LogEvent (..), SomeTracer (..))
import System.IO (hPutStrLn, stderr)

-- | The simulation carrier does both halves of a sim run: it traces the
-- structured event itself through io-sim's @traceM@, so cases assert on
-- events by type with 'selectTraceEventsDynamic' instead of matching
-- strings, and it says the rendered line, so 'printSimTrace' can show the
-- announcement inline while the typed assertions keep working. Tracing
-- and saying share the one call, which is why there is no quiet variant:
-- a sim that never prints simply never reads the say half back.
simTracer :: SomeTracer (IOSim s)
simTracer = SomeTracer (mkTracer emit)
  where
    emit :: (LogEvent e, Typeable e) => e -> IOSim s ()
    emit event = Control.Monad.IOSim.traceM event >> say (unpack (renderLine event))

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

