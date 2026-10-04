{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

-- | The starter page's static assets, generated from the Rust starter's
-- @app.html@ (see the header comment on "Starter.View"): the CSS block and
-- the four code panels. Each panel is the real source of the definition it
-- documents, extracted at compile time by "Starter.CodePanel" — never a
-- hand-written copy, so the page cannot show code the repository does not
-- contain.
module Starter.Assets
  ( starterCss,
    codePanelWorkflows,
    codePanelQueues,
    codePanelEvents,
    codePanelMessages,
  )
where

import Data.Text (Text)
import Data.Text qualified as Text
import Prelude
import Starter.CodePanel (fragment, panel, panelBlock, panelGap)

starterCss :: Text
starterCss = Text.pack "\n      /* Color tokens from the DBOS Cloud Console theme (src/theme.ts). */\n      :root {\n        --ink-bg: #16181d;\n        --ink-border: #3a3d44;\n        --fg: #d8d9dc;\n        --muted: #a3a5aa;\n\n        --splice: #c8f62e;\n        --yellow: #ecfaa0;\n        --cyan: #44e3ee;\n        --lily: #9ab5e5;\n        --lily-light: #ced7f8;\n\n        --code-bg: #0e1014;\n        --code-fg: #f8f8f8;\n        --syntax-string: #ecc48d;\n        --syntax-keyword: #c792ea;\n        --syntax-type: #82aaff;\n\n        --status-error: #dc5848;\n        --status-warning: #e5a82f;\n        --status-success: #56a64b;\n\n        --dbos-gradient: linear-gradient(90deg, var(--splice), var(--yellow) 33%, var(--cyan) 66%, var(--lily-light));\n        /* \"Flashy\" olive-green glow (#678406), faded into the surface. */\n        --flashy-bg: radial-gradient(ellipse 50% 100% at 50% 130%, rgba(103, 132, 6, 0.9), var(--ink-bg));\n        --flashy-bg-hover: radial-gradient(ellipse 50% 100% at 50% 110%, rgba(103, 132, 6, 0.9), var(--ink-bg));\n\n        --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;\n      }\n\n      * {\n        box-sizing: border-box;\n      }\n      html,\n      body {\n        margin: 0;\n        padding: 0;\n      }\n      body {\n        font-family: \"Open Sans\", system-ui, -apple-system, sans-serif;\n        color: var(--fg);\n        background-color: var(--ink-bg);\n        min-height: 100vh;\n        position: relative;\n        -webkit-font-smoothing: antialiased;\n      }\n      /* Subtle brand glow at the bottom, echoing the console's \"flashy\" radial. */\n      body::before {\n        content: \"\";\n        position: fixed;\n        inset: 0;\n        z-index: 0;\n        pointer-events: none;\n        background: radial-gradient(ellipse 60% 45% at 50% 118%, rgba(103, 132, 6, 0.16), transparent 72%);\n      }\n\n      .wrap {\n        position: relative;\n        z-index: 1;\n        max-width: 1040px;\n        margin: 0 auto;\n        padding: 56px 24px 64px;\n      }\n\n      /* ---- header ---- */\n      .brand {\n        display: flex;\n        align-items: center;\n        gap: 14px;\n        margin-bottom: 30px;\n      }\n      .brand .logo-img {\n        display: block;\n        height: 32px;\n        width: auto;\n        border-radius: 4px;\n        border: 1px solid var(--ink-border);\n      }\n\n      h1 {\n        font-size: clamp(28px, 4.6vw, 44px);\n        line-height: 1.12;\n        font-weight: 600;\n        letter-spacing: -0.6px;\n        margin: 0 0 16px;\n        max-width: 760px;\n      }\n      h1 .grad {\n        background: var(--dbos-gradient);\n        -webkit-background-clip: text;\n        background-clip: text;\n        -webkit-text-fill-color: transparent;\n      }\n      .lede {\n        color: var(--muted);\n        font-size: 15.5px;\n        line-height: 1.7;\n        max-width: 640px;\n        margin: 0 0 10px;\n      }\n      .lede strong {\n        color: var(--fg);\n        font-weight: 600;\n      }\n\n      /* ---- reconnecting toast ---- */\n      #reconnecting {\n        position: fixed;\n        top: 20px;\n        left: 50%;\n        transform: translateX(-50%) translateY(-200%);\n        opacity: 0;\n        pointer-events: none;\n        z-index: 50;\n        display: flex;\n        align-items: center;\n        gap: 10px;\n        background: rgba(229, 168, 47, 0.12);\n        border: 1px solid rgba(229, 168, 47, 0.45);\n        color: var(--status-warning);\n        padding: 9px 18px;\n        border-radius: 4px;\n        font-size: 13.5px;\n        font-weight: 600;\n        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.5);\n        transition: transform 0.4s cubic-bezier(0.16, 1, 0.3, 1), opacity 0.3s ease;\n      }\n      #reconnecting.show {\n        transform: translateX(-50%) translateY(0);\n        opacity: 1;\n        pointer-events: auto;\n      }\n      .spinner {\n        width: 15px;\n        height: 15px;\n        border-radius: 50%;\n        border: 2px solid rgba(229, 168, 47, 0.3);\n        border-top-color: var(--status-warning);\n        animation: spin 0.8s linear infinite;\n      }\n      @keyframes spin {\n        to { transform: rotate(360deg); }\n      }\n\n      /* ---- tabs ---- */\n      .tabs {\n        display: flex;\n        gap: 0;\n        border-bottom: 1px solid var(--ink-border);\n        margin-bottom: 34px;\n      }\n      .tab-btn {\n        background: none;\n        border: none;\n        border-bottom: 2px solid transparent;\n        margin-bottom: -1px;\n        font-family: inherit;\n        font-size: 0.8125rem;\n        font-weight: 600;\n        text-transform: uppercase;\n        letter-spacing: 0.08em;\n        color: var(--muted);\n        padding: 10px 20px;\n      }\n      .tab-btn.active {\n        color: var(--splice);\n        border-bottom-color: var(--splice);\n      }\n\n      /* A tab that is not the active one is not on the page.\n         The page shipped with a single tab, so this class was decorative until there were two. */\n      .tab-content {\n        display: none;\n      }\n      .tab-content.active {\n        display: block;\n      }\n\n      /* ---- layout grid ---- */\n      .grid {\n        display: grid;\n        grid-template-columns: 1.15fr 0.85fr;\n        gap: 20px;\n      }\n      @media (max-width: 860px) {\n        .grid { grid-template-columns: 1fr; }\n      }\n\n      /* outlined surfaces, flat (matches MuiPaper variant=\"outlined\") */\n      .card {\n        background: var(--ink-bg);\n        border: 1px solid var(--ink-border);\n        border-radius: 4px;\n      }\n\n      /* ---- code editor window ---- */\n      .editor {\n        overflow: hidden;\n      }\n      .editor .chrome {\n        display: flex;\n        align-items: center;\n        gap: 8px;\n        padding: 12px 16px;\n        border-bottom: 1px solid var(--ink-border);\n        background: rgba(255, 255, 255, 0.015);\n      }\n      .dot {\n        width: 11px;\n        height: 11px;\n        border-radius: 50%;\n        opacity: 0.85;\n      }\n      .dot.r { background: var(--status-error); }\n      .dot.y { background: var(--status-warning); }\n      .dot.g { background: var(--status-success); }\n      .editor .filename {\n        margin-left: 10px;\n        font-family: var(--mono);\n        font-size: 12.5px;\n        color: var(--muted);\n      }\n      pre.code {\n        margin: 0;\n        padding: 18px 20px 20px;\n        background: var(--code-bg);\n        font-family: var(--mono);\n        font-size: 13px;\n        line-height: 1.7;\n        overflow-x: auto;\n        color: var(--code-fg);\n      }\n      .code-line {\n        display: block;\n        white-space: pre;\n      }\n      .indent { padding-left: 1.6em; }\n      .keyword { color: var(--syntax-keyword); }\n      .function { color: var(--syntax-type); }\n      .string { color: var(--syntax-string); }\n      .num-lit { color: var(--cyan); }\n      .punct { color: var(--muted); }\n\n      .code-block-step {\n        display: block;\n        border-radius: 3px;\n        padding: 2px 4px;\n        margin: 2px -4px;\n        transition: background 0.35s, box-shadow 0.35s;\n      }\n      .step-highlight {\n        background: rgba(200, 246, 46, 0.12);\n        box-shadow: inset 3px 0 0 var(--splice);\n      }\n\n      /* ---- controls panel ---- */\n      .controls {\n        padding: 22px;\n        display: flex;\n        flex-direction: column;\n        gap: 16px;\n      }\n      .controls h2 {\n        margin: 0;\n        font-size: 0.65rem;\n        font-weight: 700;\n        color: var(--muted);\n        text-transform: uppercase;\n        letter-spacing: 0.12em;\n      }\n\n      /* ---- buttons (mirror MUI \"flashy\" / \"basic\" variants) ---- */\n      .btn {\n        position: relative;\n        cursor: pointer;\n        font-family: inherit;\n        font-size: 0.8125rem;\n        font-weight: 600;\n        text-transform: uppercase;\n        letter-spacing: 0.04em;\n        padding: 9px 20px;\n        display: flex;\n        align-items: center;\n        justify-content: center;\n        gap: 10px;\n        color: var(--fg);\n        background: var(--ink-bg);\n      }\n      .btn:active { transform: translateY(1px); }\n      .btn svg { width: 16px; height: 16px; }\n\n      /* flashy = primary launch action */\n      .btn-flashy {\n        z-index: 0;\n        border: 1px solid transparent;\n        border-image: var(--dbos-gradient);\n        border-image-slice: 1;\n        background: var(--flashy-bg);\n        transition: all 0.4s ease;\n      }\n      .btn-flashy:hover {\n        background-image: var(--flashy-bg-hover);\n      }\n      .btn-flashy:hover::before {\n        content: \"\";\n        position: absolute;\n        inset: 0;\n        z-index: -2;\n        background: var(--dbos-gradient);\n        filter: blur(10px);\n      }\n      .btn-flashy:hover::after {\n        content: \"\";\n        position: absolute;\n        inset: 0;\n        z-index: -1;\n        background: inherit;\n        border: inherit;\n      }\n\n      /* basic, danger-tinted = crash action */\n      .btn-danger {\n        border: 1px solid rgba(220, 88, 72, 0.45);\n        border-radius: 2px;\n        color: var(--status-error);\n        transition: border-color 0.4s, background-color 0.4s;\n      }\n      .btn-danger:hover:not(:disabled) {\n        border-color: var(--status-error);\n        background-color: rgba(220, 88, 72, 0.08);\n      }\n\n      .btn:disabled {\n        opacity: 0.4;\n        cursor: not-allowed;\n      }\n\n      .divider {\n        height: 1px;\n        background: var(--ink-border);\n        margin: 2px 0;\n      }\n\n      /* ---- progress timeline ---- */\n      .timeline {\n        display: flex;\n        flex-direction: column;\n        gap: 2px;\n      }\n      .step {\n        position: relative;\n        display: flex;\n        align-items: center;\n        gap: 14px;\n        padding: 10px 4px;\n      }\n      .step:not(:last-child) .node::after {\n        content: \"\";\n        position: absolute;\n        left: 50%;\n        top: 33px;\n        transform: translateX(-50%);\n        width: 2px;\n        height: 26px;\n        background: var(--ink-border);\n        transition: background 0.4s;\n      }\n      .step.complete .node::after {\n        background: var(--status-success);\n      }\n      .node {\n        position: relative;\n        flex: 0 0 32px;\n        width: 32px;\n        height: 32px;\n        border-radius: 50%;\n        display: grid;\n        place-items: center;\n        font-size: 13px;\n        font-weight: 700;\n        font-family: var(--mono);\n        border: 2px solid var(--ink-border);\n        color: var(--muted);\n        background: rgba(255, 255, 255, 0.02);\n        transition: all 0.35s;\n      }\n      .node .num { transition: opacity 0.2s; }\n      .node .check {\n        position: absolute;\n        opacity: 0;\n        transform: scale(0.5);\n        transition: all 0.3s;\n        width: 15px;\n        height: 15px;\n      }\n      .step-label {\n        display: flex;\n        flex-direction: column;\n        gap: 1px;\n      }\n      .step-title {\n        font-size: 13.5px;\n        font-weight: 600;\n        font-family: var(--mono);\n        color: var(--fg);\n      }\n      .step-state {\n        font-size: 12.5px;\n        color: var(--muted);\n      }\n\n      /* pending */\n      .step.pending .node { border-color: var(--ink-border); }\n      /* active */\n      .step.active .node {\n        border-color: var(--cyan);\n        color: var(--cyan);\n        box-shadow: 0 0 0 4px rgba(68, 227, 238, 0.14);\n        animation: pulse 1.6s ease-in-out infinite;\n      }\n      .step.active .step-state { color: var(--cyan); }\n      /* Animated \"...\" trail on the in-progress / disconnected step. */\n      .step.active .step-state::after,\n      .step.recover .step-state::after {\n        content: \"\";\n        animation: dots 1.4s steps(1) infinite;\n      }\n      @keyframes pulse {\n        0%, 100% { box-shadow: 0 0 0 4px rgba(68, 227, 238, 0.12); }\n        50% { box-shadow: 0 0 0 8px rgba(68, 227, 238, 0.03); }\n      }\n      @keyframes dots {\n        0% { content: \"\"; }\n        25% { content: \".\"; }\n        50% { content: \"..\"; }\n        75% { content: \"...\"; }\n      }\n      /* complete */\n      .step.complete .node {\n        background: var(--status-success);\n        border-color: transparent;\n        color: var(--ink-bg);\n      }\n      .step.complete .node .num { opacity: 0; }\n      .step.complete .node .check { opacity: 1; transform: scale(1); color: var(--ink-bg); }\n      .step.complete .step-state { color: var(--status-success); }\n      /* recover / disconnected */\n      .step.recover .node {\n        border-color: var(--status-error);\n        color: var(--status-error);\n        box-shadow: 0 0 0 4px rgba(220, 88, 72, 0.15);\n        animation: pulse-red 1.4s ease-in-out infinite;\n      }\n      .step.recover .step-state { color: var(--status-error); }\n      @keyframes pulse-red {\n        0%, 100% { box-shadow: 0 0 0 4px rgba(220, 88, 72, 0.12); }\n        50% { box-shadow: 0 0 0 8px rgba(220, 88, 72, 0.03); }\n      }\n\n      .footer {\n        margin-top: 40px;\n        color: var(--muted);\n        font-size: 14px;\n        line-height: 1.6;\n      }\n      .footer .line {\n        display: block;\n      }\n      .footer a {\n        color: var(--lily);\n        text-decoration: none;\n        font-weight: 600;\n      }\n      .footer a:hover {\n        color: var(--lily-light);\n        text-decoration: underline;\n      }\n      /* ---- workflow counts (Queues tab) ---- */\n      .wf-counts {\n        display: flex;\n        flex-direction: column;\n        gap: 6px;\n      }\n      .wf-count-row {\n        display: flex;\n        align-items: center;\n        justify-content: space-between;\n        font-family: var(--mono);\n        font-size: 13px;\n      }\n      .wf-count-label {\n        color: var(--muted);\n        font-weight: 600;\n        letter-spacing: 0.05em;\n      }\n      .wf-count-value {\n        font-weight: 700;\n        padding: 2px 8px;\n        border-radius: 2px;\n      }\n      .wf-count-value.pending {\n        color: var(--cyan);\n        background: rgba(68, 227, 238, 0.1);\n      }\n      .wf-count-value.success {\n        color: var(--status-success);\n        background: rgba(86, 166, 75, 0.1);\n      }\n      .wf-count-value.enqueued {\n        color: var(--lily);\n        background: rgba(154, 181, 229, 0.12);\n      }\n      .wf-count-value.other {\n        color: var(--muted);\n        background: rgba(163, 165, 170, 0.08);\n      }\n      .concurrency-row {\n        display: flex;\n        gap: 8px;\n        align-items: stretch;\n      }\n      .concurrency-input {\n        background: var(--code-bg);\n        border: 1px solid var(--ink-border);\n        border-radius: 2px;\n        color: var(--code-fg);\n        font-family: var(--mono);\n        font-size: 13px;\n        padding: 8px 12px;\n        outline: none;\n        transition: border-color 0.2s;\n        min-width: 0;\n        max-width: 80px;\n      }\n      .concurrency-input:focus {\n        border-color: var(--muted);\n      }\n      /* ---- events + messages tabs ---- */\n      .wf-count-value.waiting {\n        color: var(--status-warning);\n        background: rgba(229, 168, 47, 0.12);\n      }\n      .wf-count-value.rejected {\n        color: var(--status-error);\n        background: rgba(220, 88, 72, 0.12);\n      }\n      .kv-value {\n        font-family: var(--mono);\n        font-size: 12.5px;\n        color: var(--fg);\n        max-width: 55%;\n        overflow: hidden;\n        text-overflow: ellipsis;\n        white-space: nowrap;\n      }\n      .btn-mini {\n        font-size: 0.6875rem;\n        padding: 4px 10px;\n        border: 1px solid var(--ink-border);\n        text-transform: uppercase;\n        letter-spacing: 0.04em;\n        font-weight: 600;\n        font-family: inherit;\n        cursor: pointer;\n        color: var(--fg);\n        background: var(--ink-bg);\n      }\n      .btn-mini:hover { border-color: var(--muted); }\n      .btn-mini:active { transform: translateY(1px); }\n      .btn-mini.reject { color: var(--status-error); }\n      .row-actions { display: flex; gap: 6px; }\n      .read-result {\n        font-family: var(--mono);\n        font-size: 12.5px;\n        color: var(--muted);\n        min-height: 18px;\n      }\n      .read-result .found { color: var(--status-success); }\n      .read-result .missing { color: var(--status-warning); }\n      .key-picker { display: flex; gap: 6px; flex-wrap: wrap; }\n      .key-picker .btn-mini.active {\n        border-color: var(--cyan);\n        color: var(--cyan);\n      }\n\n      .concurrency-row .btn {\n        white-space: nowrap;\n        flex-shrink: 0;\n      }\n\n    "

-- | The workflows tab: the example workflow the Launch button runs, plus the
-- shared step body the timeline lights up as each step runs.
codePanelWorkflows :: Text
codePanelWorkflows =
  Text.pack
    ( $( panel
           "dbos-hs-starter/Starter/Workflows.hs"
           "exampleWorkflow"
           [ ("workflow-step-1", "\"step_one\""),
             ("workflow-step-2", "\"step_two\""),
             ("workflow-step-3", "\"step_three\"")
           ]
       )
        <> panelGap
        <> $( panelBlock
               "dbos-hs-starter/Starter/Workflows.hs"
               "stepSleep"
               ["code-block-step", "step-1", "step-2", "step-3"]
               []
           )
    )

-- | The queues tab: the enqueued workflow the queue runners drain, plus its
-- registration and one enqueue call from the app.
codePanelQueues :: Text
codePanelQueues =
  Text.pack
    ( $( panel
           "dbos-hs-starter/Starter/Workflows.hs"
           "enqueuedWorkflow"
           [ ("queue-line-1", "sleepStep"),
             ("queue-line-2", "Enqueued workflow completed")
           ]
       )
        <> panelGap
        <> $( fragment
               "dbos-hs-starter/Starter/Run.hs"
               ["registeredQueue <-", "NeverUpdate"]
               []
           )
        <> panelGap
        <> $( fragment
               "dbos-hs-starter/Starter/Handler.hs"
               ["enqueueDBOSWorkflow app.staDbos"]
               []
           )
    )

-- | The events tab: the order workflow whose steps publish events, plus the
-- waiting read the Read button makes from outside it.
codePanelEvents :: Text
codePanelEvents =
  Text.pack
    ( $( panel
           "dbos-hs-starter/Starter/Workflows.hs"
           "orderWorkflow"
           [ ("event-line-1", "sleepStep"),
             ("event-line-2", "setEvent wctx key")
           ]
       )
        <> panelGap
        <> $( fragment
               "dbos-hs-starter/Starter/Handler.hs"
               ["getWorkflowEvent app.staDbos workflowId key (millisDuration eventReadTimeoutMs)"]
               []
           )
    )

-- | The messages tab: the approval workflow that waits on a message, plus the
-- single send and the bulk send the buttons make.
codePanelMessages :: Text
codePanelMessages =
  Text.pack
    ( $( panel
           "dbos-hs-starter/Starter/Workflows.hs"
           "approvalWorkflow"
           [ ("message-line-1", "decision <- ExceptT (recv"),
             ("message-line-2", "recv wctx (Just approvalTopic)")
           ]
       )
        <> panelGap
        <> $( fragment
               "dbos-hs-starter/Starter/Handler.hs"
               ["sendWorkflowMessage app.staDbos"]
               []
           )
        <> panelGap
        <> $( fragment
               "dbos-hs-starter/Starter/Handler.hs"
               ["( sendWorkflowMessages", ")"]
               []
           )
    )
