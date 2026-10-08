{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}

-- | The queue-worker demo's views, rendered with hsx and driven by htmx v4.
-- The markup keeps the Python React frontend's shape — an enqueue button and
-- workflow cards with progress bars polling every second — as
-- server-rendered fragments: POST answers the refreshed list into
-- @#workflows-container@, and the list polls itself.
module QueueWorker.View
  ( -- * View models
    QueueWorkerPage (..),
    WorkflowStatus (..),

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

-- | Where 'QueueWorker.Route'-style mounting puts this app's routes and where
-- the views' htmx URLs point.
queueWorkerMountPath :: Text
queueWorkerMountPath = "/queue-worker"

-- | What the index page renders: the workflows the list polls from.
newtype QueueWorkerPage = QueueWorkerPage
  { queueWorkerPageWorkflows :: [WorkflowStatus]
  }
  deriving stock (Eq, Show)

-- | One row of the workflows list (the Python @WorkflowStatus@ model).
data WorkflowStatus = WorkflowStatus
  { wsWorkflowId :: Text,
    wsStatus     :: Text,
    wsCompleted  :: Maybe Int,
    wsTotal      :: Maybe Int
  }
  deriving stock (Eq, Show)

instance ToJSON WorkflowStatus where
  toJSON s =
    object
      [ "workflow_id" .= s.wsWorkflowId,
        "workflow_status" .= s.wsStatus,
        "steps_completed" .= s.wsCompleted,
        "num_steps" .= s.wsTotal
      ]

-- * Page

pageView :: QueueWorkerPage -> Html ()
pageView page =
  let workflows = page.queueWorkerPageWorkflows
   in workerPage [hsx|
    <header class="bg-white shadow-sm border-b border-gray-200">
      <div class="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="flex justify-between items-center py-6">
          <h1 class="text-2xl font-bold text-gray-900">Queue Worker Demo</h1>
          <img
            src="https://dbos-blog-posts.s3.us-west-1.amazonaws.com/logos/black_logotype%2Btransparent_bg_h4000px.png"
            alt="DBOS Logo"
            class="h-8 opacity-70"
          />
        </div>
      </div>
    </header>

    <main class="flex-1">
      <div class="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div class="bg-white rounded-2xl shadow-xl overflow-hidden card-hover">
          <div class="p-6">
            <button
              hx-post={queueWorkerMountPath <> "/workflows"}
              hx-target="#workflows-container"
              hx-swap="innerHTML"
              class="px-5 py-2 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg text-sm"
            >
              Enqueue Workflow
            </button>
          </div>
        </div>

        <div class="bg-white rounded-2xl shadow-xl overflow-hidden card-hover mt-8">
          <div class="bg-gradient-to-r from-blue-500 to-blue-600 p-6 text-white">
            <h2 class="text-2xl font-bold">Workflows</h2>
          </div>
          <div
            id="workflows-container"
            class="p-6"
            hx-get={queueWorkerMountPath <> "/workflows"}
            hx-trigger="every 1s"
            hx-swap="innerHTML"
          >
            {workflowsList workflows}
          </div>
        </div>
      </div>
    </main>
  |]

-- | The demo head: Tailwind plus the shared card/progress CSS.
workerHead :: Html ()
workerHead =
  [hsx|
    <title>DBOS Queue Worker</title>
    <link
      rel="icon"
      type="image/x-icon"
      href="https://dbos-blog-posts.s3.us-west-1.amazonaws.com/live-demo/favicon.ico"
    />
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
      .card-hover { transition: all 0.3s ease; }
      .card-hover:hover { transform: translateY(-2px); }
      .progress-container { background: #e5e7eb; border-radius: 999px; height: 8px; overflow: hidden; }
      .progress-bar { background: linear-gradient(90deg, #10b981, #059669); height: 8px; border-radius: 999px; transition: width 0.3s ease; }
      .status-badge { display: inline-block; padding: 0.15rem 0.5rem; border-radius: 999px; font-size: 0.75rem; font-weight: 600; color: #fff; }
    </style>
  |]

workerPage :: Html () -> Html ()
workerPage body =
  pageShell workerHead
    [hsx|
      <div class="min-h-screen bg-gray-50 font-sans">
        {body}
      </div>
    |]

-- * Workflows list fragment

workflowsList :: [WorkflowStatus] -> Html ()
workflowsList [] =
  [hsx|
    <div class="text-center py-12">
      <p class="text-gray-500 text-lg">No workflows yet</p>
    </div>
  |]
workflowsList workflows =
  [hsx|
    <div class="space-y-3">
      {mapM_ workflowCard workflows}
    </div>
  |]

workflowCard :: WorkflowStatus -> Html ()
workflowCard s =
  [hsx|
    <div class="bg-gray-50 border border-gray-200 rounded-lg p-4">
      <div class="flex justify-between items-center">
        <span class="status-badge" style={badgeStyle}>{s.wsStatus}</span>
        <span class="text-gray-500 text-sm">{Text.take 8 s.wsWorkflowId}…</span>
      </div>
      {progressBlock}
    </div>
  |]
  where
    badgeStyle = "background: " <> statusColor s.wsStatus :: Text
    progressBlock = case (s.wsCompleted, s.wsTotal) of
      (Just completed, Just total) ->
        [hsx|
          <div class="progress-container mt-3">
            <div class="progress-bar" style={progressStyle completed total}></div>
          </div>
          <div class="text-sm text-gray-600 mt-1">{Text.pack (show completed) <> " / " <> Text.pack (show total) <> " steps"}</div>
        |]
      _ -> mempty
    progressStyle completed total
      | total <= 0 = "width: 0%" :: Text
      | otherwise = "width: " <> Text.pack (show (completed * 100 `div` total)) <> "%"

-- | The Python frontend's status colors, spelled the same. Compared
-- case-insensitively: the engine renders @Success@/@Pending@ while the
-- oracle renders @SUCCESS@/@PENDING@.
statusColor :: Text -> Text
statusColor status
  | upper == "SUCCESS" = "#10b981"
  | upper == "PENDING" = "#f59e0b"
  | upper == "ERROR" = "#ef4444"
  | otherwise = "#6b7280"
  where
    upper = Text.toUpper status
