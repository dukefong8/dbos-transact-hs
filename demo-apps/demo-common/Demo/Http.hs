{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Shared web-layer helpers for the demo apps, mirroring the
-- Playground/hs Todo app's @Service.Http@: the route-handler monad and its
-- error type, the @runView@\/@runJson@\/@runText@ adapters, the html
-- response rendering that belongs with the runner rather than a view, and
-- the two request helpers both surfaces need ('isHtmx', 'postedParam').
--
-- 'runAppOr500' is @runDbOr500@'s counterpart over the datasource pool: a
-- failed application session becomes a 500, the same way the Todo pool's
-- failures do.
module Demo.Http
  ( RouteHandler,
    RouteError (..),
    runRouteHandler,
    throwRouteError,
    runAppOr500,
    errorResponse,
    htmlResponse,
    viewResponse,
    textResponse,
    runView,
    runJson,
    runText,
    runRespond,
    runEmpty,
    runViewTriggering,
    isHtmx,
    postedParam,
    pageShell,
  )
where

import Control.Monad.Except (ExceptT, runExceptT, throwError)
import Control.Monad.IO.Class (MonadIO (..))
import DBOS.Prelude
import DBOS.Transact (AppDataSource, runAppSession)
import Data.Aeson (ToJSON, encode)
import Data.ByteString.Lazy qualified as LBS
import Data.Text (Text)
import Demo.Htmx (hsx)
import Data.Text.Encoding (decodeUtf8', encodeUtf8)
import Hasql.Session qualified as Session
import Lucid (Html)
import Lucid qualified (renderBS)
import Network.HTTP.Types (Status, hContentType, status200, status500)
import Network.HTTP.Types.URI (parseQuery)
import Network.Wai (Application, Request, Response, queryString, requestHeaders, responseLBS, strictRequestBody)

-- | A handler failure as a status and a body, mirroring the Todo app's
-- @RouteError@.
data RouteError = RouteError Status LBS.ByteString
  deriving stock (Show)

newtype RouteHandler a = RouteHandler (ExceptT RouteError IO a)
  deriving newtype (Functor, Applicative, Monad, MonadIO)

runRouteHandler :: RouteHandler a -> IO (Either RouteError a)
runRouteHandler (RouteHandler action) = runExceptT action

throwRouteError :: Status -> LBS.ByteString -> RouteHandler a
throwRouteError status body = RouteHandler (throwError (RouteError status body))

-- | Run one application-pool session, mapping a failed session onto a 500.
runAppOr500 :: AppDataSource -> Session.Session a -> RouteHandler a
runAppOr500 app session = do
  result <- liftIO (runAppSession app session)
  case result of
    Left err -> throwRouteError status500 (LBS.fromStrict (encodeUtf8 (showText err)))
    Right value -> pure value

errorResponse :: RouteError -> Response
errorResponse (RouteError status body) =
  responseLBS status [(hContentType, "text/plain; charset=utf-8")] body

-- | The one place 'Html' becomes bytes.
htmlResponse :: Status -> Html () -> Response
htmlResponse status body =
  responseLBS status [(hContentType, "text/html; charset=utf-8")] (Lucid.renderBS body)

-- | 'htmlResponse' at status 200, for handlers that render a view result.
viewResponse :: (a -> Html ()) -> a -> Response
viewResponse renderHtml value = htmlResponse status200 (renderHtml value)

textResponse :: Status -> Text -> Response
textResponse status body =
  responseLBS status [(hContentType, "text/plain; charset=utf-8")] (LBS.fromStrict (encodeUtf8 body))

-- * Runners (the Application adapters the dispatch picks per route)

runView :: (a -> Html ()) -> RouteHandler a -> Application
runView renderHtml action _req respond = do
  result <- runRouteHandler action
  respond case result of
    Left err -> errorResponse err
    Right value -> viewResponse renderHtml value

runJson :: ToJSON a => RouteHandler a -> Application
runJson action _req respond = do
  result <- runRouteHandler action
  respond case result of
    Left err -> errorResponse err
    Right value -> responseLBS status200 [(hContentType, "application/json")] (encode value)

runText :: RouteHandler Text -> Application
runText action _req respond = do
  result <- runRouteHandler action
  respond case result of
    Left err -> errorResponse err
    Right value -> textResponse status200 value

runRespond :: (a -> Response) -> RouteHandler a -> Application
runRespond toResponse action _req respond = do
  result <- runRouteHandler action
  respond case result of
    Left err -> errorResponse err
    Right value -> toResponse value

runEmpty :: RouteHandler () -> Application
runEmpty action = runRespond (const (htmlResponse status200 mempty)) action

-- | 'runView' with an @HX-Trigger@ header: the page's script listens for the
-- named event to update the code-panel highlight without a fetch loop.
runViewTriggering :: (a -> LBS.ByteString) -> (a -> Html ()) -> RouteHandler a -> Application
runViewTriggering trigger renderHtml action _req respond = do
  result <- runRouteHandler action
  respond case result of
    Left err -> errorResponse err
    Right value ->
      responseLBS
        status200
        [ (hContentType, "text/html; charset=utf-8"),
          ("HX-Trigger", LBS.toStrict (trigger value))
        ]
        (Lucid.renderBS (renderHtml value))

-- | The common document shell every demo renders into, the Playground/hs
-- shape: a custom head plus the body, with htmx v4 loaded once so both apps'
-- fragments ride the same client.
pageShell :: Html () -> Html () -> Html ()
pageShell customHead body =
  [hsx|
    <!DOCTYPE html>
    <html lang="en">
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <script defer src="https://cdn.jsdelivr.net/npm/htmx.org@next/dist/htmax.min.js"></script>
        {customHead}
      </head>
      <body>
        {body}
      </body>
    </html>
  |]

-- | Whether this request came from htmx (its fragment surface).
isHtmx :: Request -> Bool
isHtmx request = any ((== "HX-Request") . fst) (requestHeaders request)

-- | A value posted by an htmx control: the query string first (where a
-- rendered link or hx-post URL carries it), then an urlencoded form body
-- (what hx-include produces). The API surface reads its JSON body instead.
postedParam :: Request -> Text -> IO (Maybe Text)
postedParam request key = do
  let fromQuery = lookup key (decodedPairs (queryString request))
  case fromQuery of
    Just value -> pure (Just value)
    Nothing -> do
      body <- strictRequestBody request
      pure (lookup key (decodedPairs (parseQuery (LBS.toStrict body))))
  where
    decodedPairs pairs =
      [ (keyText, valueText)
        | (rawKey, rawValue) <- pairs,
          Right keyText <- [decodeUtf8' rawKey],
          Just raw <- [rawValue],
          Right valueText <- [decodeUtf8' raw]
      ]
