{-# LANGUAGE OverloadedStrings #-}

-- | The hsx quasiquoter for this app: 'IHP.HSX.Lucid2.QQ.customHsx' with the
-- htmx v4 attribute allowlist, copied from the Playground/hs template
-- (~/dev/Playground/hs/src/Htmx/QQ.hs) so markup is checked against the same
-- vocabulary the rest of the htmx stack uses.
module Demo.Htmx (hsx) where

import Prelude
import Data.Set qualified as Set
import IHP.HSX.Lucid2.QQ (customHsx)
import IHP.HSX.Parser (HsxSettings (..))
import Language.Haskell.TH.Quote (QuasiQuoter)

-- Attribute allowlist sources (htmx v4):
--   https://four.htmx.org/reference
--   https://four.htmx.org/extensions
--
-- NOTE: hx-on / hx-on:* and hx-live / hx-live:* are intentionally absent here.
-- Both families are open-ended, so they are accepted by prefix in
-- IHP.HSX.Parser.hsxAttributeName
-- (~/dev/ihp/ihp-hsx/parser/IHP/HSX/Parser.hs).
hsx :: QuasiQuoter
hsx =
  customHsx
    ( HsxSettings
        { checkMarkup = True,
          additionalTagNames = Set.empty,
          additionalAttributeNames =
            Set.fromList
              [ "hx-action",
                "hx-alpine-compat",
                "hx-boost",
                "hx-boost:inherited",
                "hx-browser-indicator",
                "hx-config",
                "hx-confirm",
                "hx-csp",
                "hx-delete",
                "hx-disable",
                "hx-download",
                "hx-encoding",
                "hx-ext",
                "hx-get",
                "hx-headers",
                "hx-head",
                "hx-history-cache",
                "hx-history-elt",
                "hx-ignore",
                "hx-include",
                "hx-indicator",
                "hx-method",
                "hx-morph-skip",
                "hx-morph-skip-children",
                "hx-multipart",
                "hx-multipart:close",
                "hx-multipart:connect",
                "hx-nonce",
                "hx-optimistic",
                "hx-patch",
                "hx-pending",
                "hx-post",
                "hx-preload",
                "hx-preload:inherited",
                "hx-preserve",
                "hx-prompt",
                "hx-ptag",
                "hx-push-url",
                "hx-put",
                "hx-query",
                "hx-replace-url",
                "hx-select",
                "hx-select-oob",
                "hx-sse",
                "hx-sse:close",
                "hx-sse:connect",
                "hx-status",
                "hx-swap",
                "hx-swap:inherited",
                "hx-swap-oob",
                "hx-sync",
                "hx-target",
                "hx-targets",
                "hx-targets:inherited",
                "hx-trigger",
                "hx-upsert",
                "hx-validate",
                "hx-vals",
                "hx-ws",
                "hx-ws:connect",
                "hx-ws:send"
              ]
        }
    )
