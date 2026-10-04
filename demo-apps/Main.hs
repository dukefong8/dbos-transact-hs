{-# LANGUAGE OverloadedStrings #-}

-- | One server for both demo apps, mounted under URL prefixes the way the
-- Playground/hs @Site@ mounts its stacks: @/starter@ (the starter's four
-- tabs) and @/widget-store@ (the storefront).
module Main (main) where

import Data.Maybe (fromMaybe)
import Control.Monad.Class.MonadFork (myThreadId, throwTo)
import Prelude
import Network.HTTP.Types (status200, status404)
import Network.Wai (Application, pathInfo, responseLBS)
import Network.Wai.Handler.Warp qualified as Warp
import Starter.Run qualified as Starter
import System.Environment (lookupEnv)
import System.Exit (ExitCode (..))
import System.Posix.Signals (Handler (..), installHandler, sigINT, sigTERM)
import Text.Read (readMaybe)
import WidgetStore.Run qualified as WidgetStore

main :: IO ()
main = do
  port <- fromMaybe 8080 . (>>= readMaybe) <$> lookupEnv "PORT"
  (starterApp, stopStarter) <- Starter.start
  (widgetApp, stopWidget) <- WidgetStore.start
  mainThread <- myThreadId
  -- exitSuccess only terminates the calling thread: run from the signal
  -- handler it kills just the handler and the process lingers in warp.
  -- Throwing to the main thread ends the process with a quiet code 0.
  let stop = do
        sequence_ (reverse [stopStarter, stopWidget])
        throwTo mainThread ExitSuccess
  _ <- installHandler sigINT (CatchOnce stop) Nothing
  _ <- installHandler sigTERM (CatchOnce stop) Nothing
  putStrLn ("Demo apps on http://localhost:" <> show port <> " (/starter, /widget-store)")
  Warp.runSettings
    (Warp.setHost "127.0.0.1" (Warp.setPort port Warp.defaultSettings))
    (site starterApp widgetApp)

-- | Prefix dispatch, the Playground/hs Site shape: each app's route table
-- spells its own prefix (a quasiquoter cannot concatenate), so the request
-- travels through untouched.
site :: Application -> Application -> Application
site starter widget req respond =
  case pathInfo req of
    []                   -> respond indexResponse
    ("starter" : _)      -> starter req respond
    ("widget-store" : _) -> widget req respond
    _                    -> respond (responseLBS status404 [] "Not Found")
  where
    indexResponse =
      responseLBS
        status200
        [("Content-Type", "text/html; charset=utf-8")]
        "<!doctype html><title>DBOS Haskell demos</title><ul><li><a href=\"/starter/\">starter</a></li><li><a href=\"/widget-store/\">widget store</a></li></ul>"
