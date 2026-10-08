{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}

-- | The queue-patterns demo's views, rendered with hsx and driven by htmx v4.
-- The markup keeps the Python React frontend's shape — one tab per pattern,
-- a submit card, workflow rows with tenant badges and stats polling every
-- two seconds — as server-rendered fragments.
module QueuePatterns.View
  ( -- * View models
    PatternsTab (..),
    PatternsPage (..),
    PatternRow (..),

    -- * Views
    pageView,
    workflowsList,
  )
where

import Data.Aeson (ToJSON (..), object, (.=))
import Data.Text (Text)
import Data.Text qualified as Text
import Demo.Htmx (hsx)
import Demo.Http (pageShell)
import Lucid
import Prelude

-- | Where 'QueuePatterns.Route'-style mounting puts this app's routes and
-- where the views' htmx URLs point.
patternsMountPath :: Text
patternsMountPath = "/queue-patterns"

-- | Which pattern tab is showing.
data PatternsTab
  = FairQueueTab
  | RateLimitedTab
  | DebouncerTab
  deriving stock (Eq, Show)

-- | What the index page renders: the active tab and its rows.
data PatternsPage = PatternsPage
  { patternsTab   :: PatternsTab,
    patternsRows  :: [PatternRow],
    patternsStats :: (Int, Int, Int)
  }
  deriving stock (Eq, Show)

-- | One row of a pattern's workflow list (the Python @WorkflowStatus@
-- without its @start_time@: the facade exposes no row timestamps to demos,
-- so rows carry id, status and tenant only).
data PatternRow = PatternRow
  { prWorkflowId :: Text,
    prStatus     :: Text,
    prTenant     :: Maybe Text
  }
  deriving stock (Eq, Show)

instance ToJSON PatternRow where
  toJSON r =
    object
      [ "workflow_id" .= r.prWorkflowId,
        "workflow_status" .= r.prStatus,
        "tenant_id" .= r.prTenant
      ]

-- * Page

pageView :: PatternsPage -> Html ()
pageView page =
  let rows = page.patternsRows
      (enqueued, pending, completed) = page.patternsStats
   in patternsPage [hsx|
    <header class="bg-white shadow-sm border-b border-gray-200">
      <div class="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="flex justify-between items-center py-6">
          <h1 class="text-2xl font-bold text-gray-900">DBOS Queue Patterns</h1>
          <nav class="flex gap-2">
            <a href={patternsMountPath <> "/?tab=fair-queue"} class={tabClass (page.patternsTab == FairQueueTab)}>Fair Queue</a>
            <a href={patternsMountPath <> "/?tab=rate-limited"} class={tabClass (page.patternsTab == RateLimitedTab)}>Rate Limited</a>
            <a href={patternsMountPath <> "/?tab=debouncer"} class={tabClass (page.patternsTab == DebouncerTab)}>Debouncer</a>
          </nav>
        </div>
      </div>
    </header>

    <main class="flex-1">
      <div class="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {submitCard page.patternsTab}
        <div class="bg-white rounded-2xl shadow-xl overflow-hidden card-hover mt-8">
          <div class="bg-gradient-to-r from-blue-500 to-blue-600 p-6 text-white">
            <h2 class="text-2xl font-bold">Queued Workflows</h2>
          </div>
          <div class="p-6">
            <div class="flex gap-6 mb-4 text-sm text-gray-600">
              <div><span class="font-bold text-gray-900">{Text.pack (show enqueued)}</span> Enqueued</div>
              <div><span class="font-bold text-gray-900">{Text.pack (show pending)}</span> Pending</div>
              <div><span class="font-bold text-gray-900">{Text.pack (show completed)}</span> Completed</div>
            </div>
            <div
              id="workflows-container"
              hx-get={patternsMountPath <> "/workflows?tab=" <> tabName page.patternsTab}
              hx-trigger="every 2s"
              hx-swap="innerHTML"
            >
              {workflowsList rows}
            </div>
          </div>
        </div>
      </div>
    </main>
  |]

-- | The submit card per tab: fair takes a tenant, rate-limited takes none,
-- debouncer explains the deferral.
submitCard :: PatternsTab -> Html ()
submitCard FairQueueTab =
  [hsx|
    <div class="bg-white rounded-2xl shadow-xl overflow-hidden card-hover">
      <div class="p-6">
        <h2 class="text-xl font-bold mb-2">Submit Workflow</h2>
        <p class="text-sm text-gray-600 mb-4">Fair queueing ensures at most 5 workflows run concurrently, with max 1 per tenant.</p>
        <form hx-post={patternsMountPath <> "/workflows/fair_queue"} hx-target="#workflows-container" hx-swap="innerHTML">
          <div class="flex gap-3">
            <input name="tenant_id" type="text" required placeholder="Enter tenant identifier..." class="flex-1 px-3 py-2 border border-gray-200 rounded-lg text-sm" />
            <button type="submit" class="px-5 py-2 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg text-sm">Queue Workflow</button>
          </div>
        </form>
      </div>
    </div>
  |]
submitCard RateLimitedTab =
  [hsx|
    <div class="bg-white rounded-2xl shadow-xl overflow-hidden card-hover">
      <div class="p-6">
        <h2 class="text-xl font-bold mb-2">Submit Workflow</h2>
        <p class="text-sm text-gray-600 mb-4">Rate limiting ensures no more than 2 workflows start per 10 seconds.</p>
        <button
          hx-post={patternsMountPath <> "/workflows/rate_limited_queue"}
          hx-target="#workflows-container"
          hx-swap="innerHTML"
          class="px-5 py-2 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg text-sm"
        >
          Queue Workflow
        </button>
      </div>
    </div>
  |]
submitCard DebouncerTab =
  [hsx|
    <div class="bg-white rounded-2xl shadow-xl overflow-hidden card-hover">
      <div class="p-6">
        <h2 class="text-xl font-bold mb-2">Submit Workflow</h2>
        <p class="text-sm text-gray-600 mb-4">
          Debouncing waits 5 seconds after the last input before starting the workflow for each tenant.
        </p>
        <form
          hx-post={patternsMountPath <> "/workflows/debouncer"}
          hx-target="#workflows-container"
          hx-swap="innerHTML"
        >
          <div class="flex flex-col gap-3">
            <input name="tenant_id" type="text" required placeholder="Enter tenant identifier..." class="px-3 py-2 border border-gray-200 rounded-lg text-sm" />
            <input name="input" type="text" required placeholder="Enter input value..." class="px-3 py-2 border border-gray-200 rounded-lg text-sm" />
            <button type="submit" class="self-start px-5 py-2 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg text-sm">
              Trigger Debounce
            </button>
          </div>
        </form>
      </div>
    </div>
  |]

tabName :: PatternsTab -> Text
tabName FairQueueTab = "fair-queue"
tabName RateLimitedTab = "rate-limited"
tabName DebouncerTab = "debouncer"

tabClass :: Bool -> Text
tabClass True = "px-3 py-1 rounded-lg text-sm font-semibold bg-blue-600 text-white"
tabClass False = "px-3 py-1 rounded-lg text-sm font-semibold bg-gray-100 text-gray-700"

-- | The demo head: Tailwind plus the shared card/badge CSS.
patternsHead :: Html ()
patternsHead =
  [hsx|
    <title>DBOS Queue Patterns</title>
    <link
      rel="icon"
      type="image/x-icon"
      href="https://dbos-blog-posts.s3.us-west-1.amazonaws.com/live-demo/favicon.ico"
    />
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
      .card-hover { transition: all 0.3s ease; }
      .card-hover:hover { transform: translateY(-2px); }
      .status-badge { display: inline-flex; align-items: center; padding: 0.25rem 0.75rem; border-radius: 999px; font-size: 0.75rem; font-weight: 600; }
      .status-success { background: #dcfce7; color: #16a34a; }
      .status-enqueued { background: #e0e7ff; color: #3730a3; }
      .status-pending { background: #fef9c3; color: #92400e; }
      .status-error { background: #fee2e2; color: #991b1b; }
      .tenant-badge { display: inline-flex; align-items: center; padding: 0.15rem 0.5rem; border-radius: 999px; font-size: 0.75rem; background: #f3f4f6; color: #4b5563; }
    </style>
  |]

patternsPage :: Html () -> Html ()
patternsPage body =
  pageShell patternsHead
    [hsx|
      <div class="min-h-screen bg-gray-50 font-sans">
        {body}
      </div>
    |]

-- * Rows fragment

workflowsList :: [PatternRow] -> Html ()
workflowsList [] =
  [hsx|
    <div class="text-center py-12">
      <h3 class="text-lg text-gray-500">No workflows yet</h3>
      <p class="text-sm text-gray-400 mt-1">Submit a workflow to get started</p>
    </div>
  |]
workflowsList rows =
  [hsx|
    <div class="space-y-3">
      {mapM_ patternRow rows}
    </div>
  |]

patternRow :: PatternRow -> Html ()
patternRow r =
  [hsx|
    <div class="flex justify-between items-center bg-gray-50 border border-gray-200 rounded-lg p-4">
      <div>
        <div class="font-mono text-sm text-gray-900">{r.prWorkflowId}</div>
        <div class="flex gap-2 mt-1">
          {tenantBadge}
        </div>
      </div>
      <span class={statusClass}>{r.prStatus}</span>
    </div>
  |]
  where
    tenantBadge = case r.prTenant of
      Just t -> [hsx|<span class="tenant-badge">{t}</span>|]
      Nothing -> mempty
    statusClass = "status-badge " <> statusKind r.prStatus :: Text
    -- Compared case-insensitively: the engine renders @Success@ while the
    -- oracle renders @SUCCESS@.
    statusKind s = case Text.toUpper s of
      "SUCCESS" -> "status-success" :: Text
      "ENQUEUED" -> "status-enqueued"
      "PENDING" -> "status-pending"
      _ -> "status-error"
