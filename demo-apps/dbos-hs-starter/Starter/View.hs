{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}

-- | The starter page, re-rendered through hsx and driven by htmx. The look
-- is the Rust starter's page byte-for-byte (CSS and the four code panels are
-- extracted from @page.html@ by "Starter.Assets"); the tab markup stays
-- client-switched like the original, while every dynamic region is a server
-- fragment with its own htmx polling trigger and every action posts through
-- htmx. Highlighting the code panels rides the responses' @HX-Trigger@
-- events, so no fetch loop owns any state.
module Starter.View
  ( -- * View models
    WorkflowProgress (..),
    QueueStatus (..),
    EventKey (..),
    EventsStatus (..),
    ReadResult (..),
    ApprovalRow (..),
    PageView (..),

    -- * Views
    pageView,
    timelineView,
    queueCountsView,
    eventKeysView,
    readResultView,
    approvalRowsView,

    -- * Highlight payloads (carried by HX-Trigger)
    workflowHighlightStep,
    queueIsRunning,
    eventsArePublishing,
    messagesAreWaiting,
  )
where

import Data.Aeson (ToJSON (..), object, (.=))
import Data.Aeson.Key (fromText)
import Data.List qualified as List
import Data.Text (Text)
import Data.Text qualified as Text
import Prelude
import Demo.Htmx (hsx)
import Demo.Http (pageShell)
import Lucid
import Starter.Assets (codePanelEvents, codePanelMessages, codePanelQueues, codePanelWorkflows, starterCss)

-- * View models

-- | Where "Starter.Route" mounts this app's routes and where the views'
-- htmx URLs point. The route table spells the same prefix (a quasiquoter
-- cannot concatenate).
starterMountPath :: Text
starterMountPath = "/starter"

-- | The workflows tab's timeline. @wpTaskId = Nothing@ is a page that has not
-- started a run yet; @wpFinished@ stops the self-poll.
data WorkflowProgress = WorkflowProgress
  { wpTaskId   :: Maybe Text,
    wpLastStep :: Int,
    wpFinished :: Bool
  }
  deriving stock (Eq, Show)

data QueueStatus = QueueStatus
  { qsWorkerConcurrency :: Int,
    qsCounts            :: [(Text, Int)]
  }
  deriving stock (Eq, Show)

data EventKey = EventKey
  { ekKey   :: Text,
    ekValue :: Maybe Text
  }
  deriving stock (Eq, Show)

data EventsStatus = EventsStatus
  { esWorkflowId :: Maybe Text,
    esKeys       :: [EventKey]
  }
  deriving stock (Eq, Show)

data ReadResult = ReadResult
  { rrKey      :: Text,
    rrValue    :: Maybe Text,
    rrWaitedMs :: Int
  }
  deriving stock (Eq, Show)

data ApprovalRow = ApprovalRow
  { arWorkflowId :: Text,
    arDecision   :: Maybe Text
  }
  deriving stock (Eq, Show)

data PageView = PageView
  { pvTab       :: Text,
    pvQueue     :: QueueStatus,
    pvEvents    :: EventsStatus,
    pvProgress  :: WorkflowProgress,
    pvApprovals :: [ApprovalRow]
  }
  deriving stock (Eq, Show)

-- The JSON surface's shapes, byte-compatible with the Rust starter's.
instance ToJSON QueueStatus where
  toJSON status =
    object
      [ "worker_concurrency" .= status.qsWorkerConcurrency,
        "workflow_counts" .= object [fromText name .= count | (name, count) <- status.qsCounts]
      ]

instance ToJSON EventKey where
  toJSON key = object ["key" .= key.ekKey, "value" .= key.ekValue]

instance ToJSON EventsStatus where
  toJSON status = object ["workflow_id" .= status.esWorkflowId, "keys" .= status.esKeys]

instance ToJSON ReadResult where
  toJSON result =
    object
      [ "key" .= result.rrKey,
        "value" .= result.rrValue,
        "waited_ms" .= result.rrWaitedMs
      ]

instance ToJSON ApprovalRow where
  toJSON row = object ["workflow_id" .= row.arWorkflowId, "decision" .= row.arDecision]

-- * Highlight payloads

workflowHighlightStep :: WorkflowProgress -> Int
workflowHighlightStep progress
  | progress.wpFinished = 0
  | otherwise = progress.wpLastStep + 1

queueIsRunning :: QueueStatus -> Bool
queueIsRunning status = any (\(name, count) -> name == "PENDING" && count >= 1) status.qsCounts

eventsArePublishing :: EventsStatus -> Bool
eventsArePublishing status = any (\key -> key.ekValue == Nothing) status.esKeys

messagesAreWaiting :: [ApprovalRow] -> Bool
messagesAreWaiting rows = any (\row -> row.arDecision == Nothing) rows

-- * Page

pageView :: PageView -> Html ()
pageView page =
  pageShell starterHead
    [hsx|
      <div id="mount" data-path={starterMountPath} style="display:none"></div>
      <div id="reconnecting">
        <div class="spinner"></div>
        <span>Application offline — reconnecting…</span>
      </div>

      <div class="wrap">
        <div class="brand">
          <img
            class="logo-img"
            src="https://dbos-blog-posts.s3.us-west-1.amazonaws.com/logos/white_logotype%2Bblack_bg_h1000px.png"
            alt="DBOS"
          />
        </div>

        <h1>Build reliable software <span class="grad">effortlessly</span></h1>
        <div class="tabs">
          {tabButton page "workflows" "Workflows"}
          {tabButton page "queues" "Queues"}
          {tabButton page "events" "Events"}
          {tabButton page "messages" "Messages"}
        </div>

        {workflowsTabView page}
        {queuesTabView page}
        {eventsTabView page}
        {messagesTabView page}
      </div>

      {pageScript}
    |]

tabClasses :: PageView -> Text -> Text
tabClasses page name = if page.pvTab == name then "tab-content active" else "tab-content"

tabButton :: PageView -> Text -> Text -> Html ()
tabButton page name label =
  [hsx|
    <button class={classes} onclick={"switchTab('" <> name <> "', this)"}>{label}</button>
  |]
  where
    classes :: Text
    classes = if page.pvTab == name then "tab-btn active" else "tab-btn"

-- | The starter's head, rendered into the common 'Demo.Http.pageShell'.
starterHead :: Html ()
starterHead =
  [hsx|
    <title>Welcome to DBOS!</title>
    <link rel="icon" href="https://dbos-blog-posts.s3.us-west-1.amazonaws.com/live-demo/favicon.ico" type="image/x-icon">
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
  |]
    <> toHtmlRaw ("<style>" <> starterCss <> "</style>")

-- * Tabs

workflowsTabView :: PageView -> Html ()
workflowsTabView page =
  [hsx|
    <div id="tab-workflows" class={classes}>
      <p class="lede">
        DBOS adds <strong>durable execution</strong> to your applications, checkpointing their workflows and steps to Postgres.
        If your apps ever crash, DBOS recovers them from their last completed steps.
      </p>
      <p class="lede">
        <strong>Try it:</strong> launch a workflow, then crash this app.
        When you restart, the workflow always resumes from exactly where it left off.
      </p>
      <div class="grid">
        {codeEditor codePanelWorkflows}
        <div class="card controls">
          <h2>Run the demo</h2>
          <button onclick="startBackgroundJob()" class="btn btn-flashy">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <polygon points="5 3 19 12 5 21 5 3" fill="currentColor" stroke="none"></polygon>
            </svg>
            Launch durable workflow
          </button>
          <button onclick="crashApp()" id="crash-button" class="btn btn-danger">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path>
              <line x1="12" y1="9" x2="12" y2="13"></line>
              <line x1="12" y1="17" x2="12.01" y2="17"></line>
            </svg>
            Crash the application
          </button>

          <div class="divider"></div>

          {timelineView page.pvProgress}
        </div>
      </div>

      <p class="footer">
        <span class="line">
          Check out the
          <a href="https://docs.dbos.dev/" target="_blank">DBOS docs</a>
          to learn how to build with DBOS.
        </span>
      </p>
    </div>
  |]
  where
    classes :: Text
    classes = tabClasses page "workflows"

queuesTabView :: PageView -> Html ()
queuesTabView page =
  [hsx|
    <div id="tab-queues" class={classes}>
      <p class="lede">
        DBOS <strong>queues</strong> let you run workflow fan-outs and control concurrency.
        The <code>worker_concurrency</code> parameter controls how many workflows one process may run simultaneously.
      </p>
      <p class="lede">
        <strong>Try it:</strong> press "Enqueue 5 workflows" repeatedly to put workflows on the queue.
        Three run at a time and the rest wait &mdash; then change the limit and press Apply, without restarting the app.
      </p>
      <div class="grid">
        {codeEditor codePanelQueues}
        <div class="card controls">
          <h2>Queue</h2>

          <div class="concurrency-row">
            <input id="concurrency-input" class="concurrency-input" type="number" min="1" value={Text.pack (show page.pvQueue.qsWorkerConcurrency)}>
            <span style="font-size:12.5px;color:var(--muted);align-self:center;white-space:nowrap">worker_concurrency</span>
            <button
              hx-post={starterMountPath <> "/queue/concurrency"}
              hx-include="#concurrency-input"
              hx-target="#queue-wf-counts"
              hx-swap="outerHTML"
              class="btn btn-flashy"
              style="margin-left:auto"
            >Apply</button>
          </div>

          <button hx-post={starterMountPath <> "/queue/enqueue"} hx-target="#queue-wf-counts" hx-swap="outerHTML" class="btn btn-flashy">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <line x1="12" y1="5" x2="12" y2="19"></line>
              <polyline points="19 12 12 19 5 12"></polyline>
            </svg>
            Enqueue 5 workflows
          </button>

          <div class="divider"></div>

          <h2>Workflows enqueued since startup:</h2>
          {queueCountsView page.pvQueue}
        </div>
      </div>

      <p class="footer">
        <span class="line">
          DBOS queues offer many additional controls for rate limiting, deduplication, debouncing, partitioning, prioritization and more.
          See the <a href="https://docs.dbos.dev/" target="_blank">DBOS docs</a> for details.
        </span>
      </p>
    </div>
  |]
  where
    classes :: Text
    classes = tabClasses page "queues"

eventsTabView :: PageView -> Html ()
eventsTabView page =
  [hsx|
    <div id="tab-events" class={classes}>
      <p class="lede">
        A workflow publishes <strong>events</strong> &mdash; named values anyone can read by key, from
        outside the workflow, in another process, or after it has finished.
      </p>
      <p class="lede">
        <strong>Try it:</strong> start an order, then immediately press Read on <code>shipped</code>.
        The request blocks until the workflow gets there and publishes it &mdash; no polling, and nothing
        held open on the workflow's side.
      </p>
      <div class="grid">
        {codeEditor codePanelEvents}
        <div class="card controls">
          <h2>Order</h2>
          <button hx-post={starterMountPath <> "/events/start"} hx-target="#event-keys" hx-swap="outerHTML" class="btn btn-flashy">Start an order</button>

          <div class="divider"></div>

          <h2>Published so far:</h2>
          {eventKeysView page.pvEvents}

          <div class="divider"></div>

          <h2>Read one key, waiting up to 12s:</h2>
          <div class="key-picker" id="event-key-picker">
            <button class="btn-mini" onclick="pickKey('accepted', this)">accepted</button>
            <button class="btn-mini" onclick="pickKey('charged', this)">charged</button>
            <button class="btn-mini active" onclick="pickKey('shipped', this)">shipped</button>
          </div>
          <button onclick="readEvent()" class="btn btn-flashy">Read</button>
          <div id="event-read-result" class="read-result"></div>
        </div>
      </div>

      <p class="footer">
        <span class="line">
          Events are one of several ways a workflow talks to the outside world. See the
          <a href="https://docs.dbos.dev/" target="_blank">DBOS docs</a> for streams and messages.
        </span>
      </p>
    </div>
  |]
  where
    classes :: Text
    classes = tabClasses page "events"

messagesTabView :: PageView -> Html ()
messagesTabView page =
  [hsx|
    <div id="tab-messages" class={classes}>
      <p class="lede">
        A workflow can stop and <strong>wait to be told something</strong>. <code>recv</code> parks it
        durably &mdash; nothing of it stays in memory, and a message sent later, by any process, wakes it.
      </p>
      <p class="lede">
        <strong>Try it:</strong> start a few approval requests and watch them wait. Approve one, or
        approve them all at once &mdash; <code>send_bulk</code> is a single transaction, so nobody is
        approved unless everybody is. You can also crash the app on the Workflows tab first: the
        requests are still waiting when it comes back.
      </p>
      <div class="grid">
        {codeEditor codePanelMessages}
        <div class="card controls">
          <h2>Approval requests</h2>
          <button hx-post={starterMountPath <> "/messages/start"} hx-target="#approval-rows" hx-swap="outerHTML" class="btn btn-flashy">Request an approval</button>

          <div class="concurrency-row">
            <button hx-post={starterMountPath <> "/messages/respond-all?decision=approved"} hx-target="#approval-rows" hx-swap="outerHTML" class="btn btn-flashy" style="flex:1">Approve all</button>
            <button hx-post={starterMountPath <> "/messages/respond-all?decision=rejected"} hx-target="#approval-rows" hx-swap="outerHTML" class="btn btn-danger" style="flex:1">Reject all</button>
          </div>

          <div class="divider"></div>

          <h2>Requests since startup:</h2>
          {approvalRowsView page.pvApprovals}
        </div>
      </div>

      <p class="footer">
        <span class="line">
          A message outlives the process that waits for it, so the sender and the receiver never have to
          be running at the same time. See the <a href="https://docs.dbos.dev/" target="_blank">DBOS docs</a>.
        </span>
      </p>
    </div>
  |]
  where
    classes :: Text
    classes = tabClasses page "messages"

codeEditor :: Text -> Html ()
codeEditor panel =
  [hsx|
    <div class="card editor">
      <div class="chrome">
        <span class="dot r"></span><span class="dot y"></span><span class="dot g"></span>
        <span class="filename">{filename}</span>
      </div>
      <pre class="code">{toHtmlRaw panel}</pre>
    </div>
  |]
  where
    filename = "Main.hs" :: Text

-- * Dynamic fragments

timelineView :: WorkflowProgress -> Html ()
timelineView progress
  | Just task <- progress.wpTaskId, not progress.wpFinished =
      [hsx|
        <div
          id="status"
          class="timeline"
          hx-get={starterMountPath <> "/last_step/" <> task}
          hx-trigger="every 700ms"
          hx-swap="outerHTML"
        >
          {rows}
        </div>
      |]
  | otherwise =
      [hsx|
        <div id="status" class="timeline">{rows}</div>
      |]
  where
    rows :: Html ()
    rows = mapM_ stepRow [1 :: Int, 2, 3]
    stepRow number =
      [hsx|
        <div class={"step " <> stepState number}>
          <div class="node">
            <span class="num">{Text.pack (show number)}</span>
            <svg class="check" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>
          </div>
          <div class="step-label">
            <span class="step-title">{stepTitle number}</span>
            <span class="step-state">{stepStateText number}</span>
          </div>
        </div>
      |]
    stepTitle :: Int -> Text
    stepTitle 1 = "step_one"
    stepTitle 2 = "step_two"
    stepTitle _ = "step_three"
    stepState :: Int -> Text
    stepState number
      | progress.wpFinished = "complete"
      | number < expected = "complete"
      | number == expected = "active"
      | otherwise = "pending"
    stepStateText :: Int -> Text
    stepStateText number
      | stepState number == "complete" = "Completed"
      | stepState number == "active" = "Executing"
      | otherwise = "Waiting"
    expected = progress.wpLastStep + 1

queueCountsView :: QueueStatus -> Html ()
queueCountsView status =
  [hsx|
    <div
      id="queue-wf-counts"
      class="wf-counts"
      hx-get={starterMountPath <> "/queue/status"}
      hx-trigger="every 800ms [activeTab()==='queues']"
      hx-swap="outerHTML"
    >
      {rows}
    </div>
  |]
  where
    rows :: Html ()
    rows
      | null ordered = [hsx|<div class="wf-count-row"><span class="wf-count-label">NONE YET</span></div>|]
      | otherwise =
          mapM_
            ( \(name, count) ->
                [hsx|
                  <div class="wf-count-row">
                    <span class="wf-count-label">{name}</span>
                    <span class={"wf-count-value " <> statusClass name}>{Text.pack (show count)}</span>
                  </div>
                |]
            )
            ordered
    ordered :: [(Text, Int)]
    ordered = List.sortOn fst status.qsCounts

statusClass :: Text -> Text
statusClass name
  | name == "ENQUEUED" = "enqueued"
  | name == "PENDING" = "pending"
  | name == "SUCCESS" = "success"
  | otherwise = "other"

eventKeysView :: EventsStatus -> Html ()
eventKeysView status =
  [hsx|
    <div
      id="event-keys"
      class="wf-counts"
      hx-get={starterMountPath <> "/events/status"}
      hx-trigger="every 800ms [activeTab()==='events']"
      hx-swap="outerHTML"
    >
      {rows}
    </div>
  |]
  where
    rows :: Html ()
    rows =
      case status.esWorkflowId of
        Nothing ->
          [hsx|<div class="wf-count-row"><span class="wf-count-label">NO ORDER YET</span></div>|]
        Just _ ->
          mapM_ keyRow status.esKeys
    keyRow :: EventKey -> Html ()
    keyRow key =
      [hsx|
        <div class="wf-count-row">
          <span class="wf-count-label">{Text.toUpper key.ekKey}</span>
          {badge key}
        </div>
      |]
    badge :: EventKey -> Html ()
    badge key = case key.ekValue of
      Just value -> [hsx|<span class="kv-value">{value}</span>|]
      Nothing    -> [hsx|<span class="wf-count-value waiting">WAITING</span>|]

readResultView :: ReadResult -> Html ()
readResultView result =
  [hsx|
    <div id="event-read-result" class="read-result">
      {body}
    </div>
  |]
  where
    seconds = Text.pack (show (fromIntegral result.rrWaitedMs / 1000 :: Double))
    body :: Html ()
    body = case result.rrValue of
      Nothing    -> [hsx|<span class="missing">not published</span> after {seconds}s|]
      Just value -> [hsx|<span class="found">{value}</span> after {seconds}s|]

approvalRowsView :: [ApprovalRow] -> Html ()
approvalRowsView rows =
  [hsx|
    <div
      id="approval-rows"
      class="wf-counts"
      hx-get={starterMountPath <> "/messages/status"}
      hx-trigger="every 800ms [activeTab()==='messages']"
      hx-swap="outerHTML"
    >
      {rendered}
    </div>
  |]
  where
    rendered :: Html ()
    rendered
      | null rows = [hsx|<div class="wf-count-row"><span class="wf-count-label">NONE YET</span></div>|]
      | otherwise = mapM_ rowView rows
    rowView :: ApprovalRow -> Html ()
    rowView row =
      [hsx|
        <div class="wf-count-row">
          <span class="wf-count-label">{shortId row.arWorkflowId}</span>
          {right row}
        </div>
      |]
    right :: ApprovalRow -> Html ()
    right row = case row.arDecision of
      Just decision ->
        [hsx|<span class={"wf-count-value " <> decisionClass decision}>{Text.toUpper decision}</span>|]
      Nothing ->
        [hsx|
          <span class="row-actions">
            <button
              class="btn-mini"
              hx-post={starterMountPath <> "/messages/respond?workflow_id=" <> row.arWorkflowId <> "&decision=approved"}
              hx-target="#approval-rows"
              hx-swap="outerHTML"
            >Approve</button>
            <button
              class="btn-mini reject"
              hx-post={starterMountPath <> "/messages/respond?workflow_id=" <> row.arWorkflowId <> "&decision=rejected"}
              hx-target="#approval-rows"
              hx-swap="outerHTML"
            >Reject</button>
          </span>
        |]
    shortId = Text.take 8

decisionClass :: Text -> Text
decisionClass decision
  | decision == "approved" = "success"
  | decision == "rejected" = "rejected"
  | otherwise = "other"

-- * Page script

pageScript :: Html ()
pageScript =
  [hsx|
    <script>
      const MOUNT = (document.getElementById("mount") || { dataset: {} }).dataset.path || "";

      function switchTab(name, button) {
        document.querySelectorAll(".tab-content").forEach((el) => el.classList.remove("active"));
        document.querySelectorAll(".tab-btn").forEach((el) => el.classList.remove("active"));
        document.getElementById("tab-" + name).classList.add("active");
        button.classList.add("active");
      }

      function activeTab() {
        const active = document.querySelector(".tab-content.active");
        return active ? active.id.replace("tab-", "") : "workflows";
      }

      let selectedKey = "shipped";

      function pickKey(key, button) {
        selectedKey = key;
        document.querySelectorAll("#event-key-picker .btn-mini").forEach((el) => el.classList.remove("active"));
        button.classList.add("active");
      }

      function generateRandomString() {
        const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        return Array.from(crypto.getRandomValues(new Uint8Array(6)))
          .map((x) => chars[x % chars.length])
          .join("");
      }

      function startBackgroundJob() {
        const id = generateRandomString();
        const params = new URLSearchParams(window.location.search);
        params.set("id", id);
        params.set("tab", "workflows");
        window.history.replaceState({}, "", `${window.location.pathname}?${params.toString()}`);
        htmx.ajax("POST", MOUNT + "/workflow/" + encodeURIComponent(id), { target: "#status", swap: "outerHTML" });
        htmx.ajax("GET", MOUNT + "/last_step/" + encodeURIComponent(id), { target: "#status", swap: "outerHTML" });
        document.getElementById("crash-button").disabled = false;
      }

      function crashApp() {
        htmx.ajax("POST", MOUNT + "/crash", { swap: "none" }).catch(() => {});
      }

      function readEvent() {
        const el = document.getElementById("event-read-result");
        el.innerHTML = "waiting for <strong>" + selectedKey + "</strong>…";
        htmx.ajax("POST", MOUNT + "/events/read?key=" + encodeURIComponent(selectedKey), { target: "#event-read-result", swap: "outerHTML" });
      }

      function highlightCode(step) {
        document.querySelectorAll(".step-highlight").forEach((el) => el.classList.remove("step-highlight"));
        if (step >= 1 && step <= 3) {
          const stepBlock = document.querySelector(`.step-${step}`);
          if (stepBlock) stepBlock.classList.add("step-highlight");
          const workflowLine = document.querySelector(`.workflow-step-${step}`);
          if (workflowLine) workflowLine.classList.add("step-highlight");
        }
      }

      function setQueueActive(running) {
        document.querySelectorAll(".queue-line-1, .queue-line-2").forEach((line) => line.classList.toggle("active", running));
      }

      function setEventPublishing(publishing) {
        document.querySelectorAll(".event-line-1, .event-line-2").forEach((line) => line.classList.toggle("step-highlight", publishing));
      }

      function setMessageWaiting(waiting) {
        document.querySelectorAll(".message-line-1, .message-line-2").forEach((line) => line.classList.toggle("step-highlight", waiting));
      }

      document.body.addEventListener("workflowHighlight", (event) => highlightCode(event.detail.step));
      document.body.addEventListener("queueActive", (event) => setQueueActive(event.detail.running));
      document.body.addEventListener("eventPublishing", (event) => setEventPublishing(event.detail.publishing));
      document.body.addEventListener("messageWaiting", (event) => setMessageWaiting(event.detail.waiting));

      document.body.addEventListener("htmx:error", () => {
        document.getElementById("reconnecting").classList.add("show");
        // Mirror the Rust page: the running row goes red until the next
        // successful poll swaps the fragment back to its server state.
        const active = document.querySelector("#status .step.active");
        if (active) {
          active.classList.remove("active");
          active.classList.add("recover");
          const state = active.querySelector(".step-state");
          if (state) state.textContent = "Disconnected";
        }
      });
      document.body.addEventListener("htmx:after:request", (event) => {
        const response = event.detail && event.detail.ctx && event.detail.ctx.response;
        if (response && response.status && response.status < 400) {
          document.getElementById("reconnecting").classList.remove("show");
        }
      });
    </script>
  |]
