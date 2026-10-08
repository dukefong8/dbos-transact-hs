{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}

-- | The outbox demo's views, rendered with hsx and driven by htmx v4 (the
-- Playground/hs stack). The markup keeps the Python static page's shape —
-- a place-order form, an orders table with SENT/PENDING badges polling every
-- two seconds — while the React-style client fetch loop becomes
-- server-rendered fragments: both POSTs answer the refreshed orders list
-- into @#orders-container@, and the list polls itself.
module Outbox.View
  ( -- * View models
    OutboxPage (..),

    -- * Views
    pageView,
    ordersList,
  )
where

import Data.Text (Text)
import Data.Text qualified as Text
import Demo.Htmx (hsx)
import Demo.Http (pageShell)
import Lucid
import Outbox.Store (OutboxOrder (..))
import Prelude

-- | Where 'Outbox.Route'-style mounting puts this app's routes and where the
-- views' htmx URLs point.
outboxMountPath :: Text
outboxMountPath = "/outbox"

-- | What the index page renders: the orders the list polls from.
newtype OutboxPage = OutboxPage
  { outboxPageOrders :: [OutboxOrder]
  }
  deriving stock (Eq, Show)

-- * Page

pageView :: OutboxPage -> Html ()
pageView page =
  let orders = page.outboxPageOrders
   in outboxPage [hsx|
    <header class="bg-white shadow-sm border-b border-gray-200">
      <div class="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="flex justify-between items-center py-6">
          <h1 class="text-2xl font-bold text-gray-900">Transactional Outbox</h1>
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
          <div class="bg-gradient-to-r from-blue-500 to-blue-600 p-6 text-white">
            <h2 class="text-2xl font-bold">Place Order · atomic workflow</h2>
            <p class="text-blue-100 mt-1 text-sm">
              One workflow inserts the order and sends its notification, atomically.
            </p>
          </div>
          <div class="p-6">
            <form
              hx-post={outboxMountPath <> "/orders"}
              hx-target="#orders-container"
              hx-swap="innerHTML"
            >
              <div class="flex gap-3 flex-wrap">
                <div class="flex flex-col flex-1 min-w-30">
                  <label class="text-xs font-semibold text-gray-600 mb-1">Customer</label>
                  <input name="customer" type="text" required placeholder="Alice" class="px-3 py-2 border border-gray-200 rounded-lg text-sm" />
                </div>
                <div class="flex flex-col flex-1 min-w-30">
                  <label class="text-xs font-semibold text-gray-600 mb-1">Item</label>
                  <input name="item" type="text" required placeholder="Widget A" class="px-3 py-2 border border-gray-200 rounded-lg text-sm" />
                </div>
                <div class="flex flex-col w-24">
                  <label class="text-xs font-semibold text-gray-600 mb-1">Qty</label>
                  <input name="quantity" type="number" min="1" value="1" required class="px-3 py-2 border border-gray-200 rounded-lg text-sm" />
                </div>
                <button type="submit" class="self-end px-5 py-2 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg text-sm">
                  Place Order
                </button>
              </div>
            </form>
          </div>
        </div>

        <div class="bg-white rounded-2xl shadow-xl overflow-hidden card-hover mt-8">
          <div class="bg-gradient-to-r from-emerald-500 to-emerald-600 p-6 text-white">
            <h2 class="text-2xl font-bold">Place Order · transactional enqueue</h2>
            <p class="text-emerald-100 mt-1 text-sm">
              The order row and the enqueued notification workflow commit together.
            </p>
          </div>
          <div class="p-6">
            <form
              hx-post={outboxMountPath <> "/enqueue-orders"}
              hx-target="#orders-container"
              hx-swap="innerHTML"
            >
              <div class="flex gap-3 flex-wrap">
                <div class="flex flex-col flex-1 min-w-30">
                  <label class="text-xs font-semibold text-gray-600 mb-1">Customer</label>
                  <input name="customer" type="text" required placeholder="Bob" class="px-3 py-2 border border-gray-200 rounded-lg text-sm" />
                </div>
                <div class="flex flex-col flex-1 min-w-30">
                  <label class="text-xs font-semibold text-gray-600 mb-1">Item</label>
                  <input name="item" type="text" required placeholder="Widget B" class="px-3 py-2 border border-gray-200 rounded-lg text-sm" />
                </div>
                <div class="flex flex-col w-24">
                  <label class="text-xs font-semibold text-gray-600 mb-1">Qty</label>
                  <input name="quantity" type="number" min="1" value="1" required class="px-3 py-2 border border-gray-200 rounded-lg text-sm" />
                </div>
                <button type="submit" class="self-end px-5 py-2 bg-emerald-600 hover:bg-emerald-700 text-white font-semibold rounded-lg text-sm">
                  Enqueue Order
                </button>
              </div>
            </form>
          </div>
        </div>

        <div class="bg-white rounded-2xl shadow-xl overflow-hidden card-hover mt-8">
          <div class="bg-gradient-to-r from-slate-500 to-slate-600 p-6 text-white">
            <h2 class="text-2xl font-bold">Orders</h2>
          </div>
          <div
            id="orders-container"
            class="p-6"
            hx-get={outboxMountPath <> "/orders"}
            hx-trigger="every 2s"
            hx-swap="innerHTML"
          >
            {ordersList orders}
          </div>
        </div>
      </div>
    </main>
  |]

-- | The outbox head: Tailwind plus the shared card/badge CSS.
outboxHead :: Html ()
outboxHead =
  [hsx|
    <title>DBOS Transactional Outbox</title>
    <link
      rel="icon"
      type="image/x-icon"
      href="https://dbos-blog-posts.s3.us-west-1.amazonaws.com/live-demo/favicon.ico"
    />
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
      .card-hover { transition: all 0.3s ease; }
      .card-hover:hover { transform: translateY(-2px); }
      .badge { display: inline-block; padding: 0.15rem 0.5rem; border-radius: 999px; font-size: 0.75rem; font-weight: 600; }
      .badge-sent { background: #dcfce7; color: #16a34a; }
      .badge-pending { background: #fef9c3; color: #ca8a04; }
    </style>
  |]

outboxPage :: Html () -> Html ()
outboxPage body =
  pageShell outboxHead
    [hsx|
      <div class="min-h-screen bg-gray-50 font-sans">
        {body}
      </div>
    |]

-- * Orders list fragment

ordersList :: [OutboxOrder] -> Html ()
ordersList [] =
  [hsx|
    <div class="text-center py-12">
      <p class="text-gray-500 text-lg">No orders yet</p>
      <p class="text-gray-400 text-sm mt-2">Place one above!</p>
    </div>
  |]
ordersList orders =
  [hsx|
    <table class="w-full text-sm">
      <thead>
        <tr>
          <th class="text-left text-xs font-semibold uppercase tracking-wide text-gray-600 px-3 py-2 border-b-2 border-gray-200">ID</th>
          <th class="text-left text-xs font-semibold uppercase tracking-wide text-gray-600 px-3 py-2 border-b-2 border-gray-200">Customer</th>
          <th class="text-left text-xs font-semibold uppercase tracking-wide text-gray-600 px-3 py-2 border-b-2 border-gray-200">Item</th>
          <th class="text-left text-xs font-semibold uppercase tracking-wide text-gray-600 px-3 py-2 border-b-2 border-gray-200">Qty</th>
          <th class="text-left text-xs font-semibold uppercase tracking-wide text-gray-600 px-3 py-2 border-b-2 border-gray-200">Notification</th>
          <th class="text-left text-xs font-semibold uppercase tracking-wide text-gray-600 px-3 py-2 border-b-2 border-gray-200">Created</th>
        </tr>
      </thead>
      <tbody>
        {mapM_ orderRow orders}
      </tbody>
    </table>
  |]

orderRow :: OutboxOrder -> Html ()
orderRow order =
  [hsx|
    <tr>
      <td class="px-3 py-2 border-b border-gray-100">{Text.pack (show order.outboxOrderId)}</td>
      <td class="px-3 py-2 border-b border-gray-100">{order.outboxCustomer}</td>
      <td class="px-3 py-2 border-b border-gray-100">{order.outboxItem}</td>
      <td class="px-3 py-2 border-b border-gray-100">{Text.pack (show order.outboxQuantity)}</td>
      <td class="px-3 py-2 border-b border-gray-100"><span class={badgeClass}>{order.outboxNotifStatus}</span></td>
      <td class="px-3 py-2 border-b border-gray-100">{order.outboxCreatedAt}</td>
    </tr>
  |]
  where
    badgeClass
      | order.outboxNotifStatus == "SENT" = "badge badge-sent" :: Text
      | otherwise = "badge badge-pending"
