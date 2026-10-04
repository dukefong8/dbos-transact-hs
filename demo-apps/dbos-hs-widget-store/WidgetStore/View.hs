{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}

-- | The storefront's views, rendered with hsx and driven by htmx v4
-- (the Playground/hs stack). The markup keeps the Rust port's
-- @html/app.html@ Tailwind classes, palettes, animations and custom CSS —
-- same header, three-column card layout, badges and progress bars — while
-- the Alpine state machine becomes server-rendered fragments: Buy posts
-- @\/checkout\/{key}@ and swaps the payment panel into @#store-panel@, the
-- payment buttons post the webhook, the status panel polls itself until the
-- order is terminal, and the orders list polls every second.
module WidgetStore.View
  ( -- * View models
    StorePage (..),
    StorePanel (..),

    -- * Views
    pageView,
    storePanelView,
    paymentPanel,
    orderStatusPanel,
    checkoutErrorPanel,
    ordersList,
    statusName,
    statusBadgeClass,
  )
where

import Data.Text (Text)
import Data.Text qualified as Text
import Prelude
import Demo.Htmx (hsx)
import Demo.Http (pageShell)
import Lucid
import WidgetStore.Store

-- | Where 'Starter.Route'-style mounting puts this app's routes and where
-- the views' htmx URLs point. The route table in "WidgetStore.Route" spells
-- the same prefix (a quasiquoter cannot concatenate).
widgetMountPath :: Text
widgetMountPath = "/widget-store"

-- | What the index page renders: the product, the orders list, and the fresh
-- idempotency key the Buy button posts under.
data StorePage = StorePage
  { storePageKey     :: Text,
    storePageProduct :: Product,
    storePageOrders  :: [Order]
  }
  deriving stock (Eq, Show)

-- | The middle panel's store view.
data StorePanel = StorePanel
  { storePanelKey     :: Text,
    storePanelProduct :: Product
  }
  deriving stock (Eq, Show)

-- * Status vocabulary

statusName :: Int -> Text
statusName status
  | status == orderStatusPending = "PENDING"
  | status == orderStatusDispatched = "DISPATCHED"
  | status == orderStatusPaid = "PAID"
  | status == orderStatusCancelled = "CANCELLED"
  | otherwise = "UNKNOWN"

statusBadgeClass :: Int -> Text
statusBadgeClass status = "status-" <> Text.toLower (statusName status)

progressWidth :: Int -> Text
progressWidth remaining = Text.pack (show (max 0 (100 - remaining * 10))) <> "%"

isTerminalStatus :: Int -> Bool
isTerminalStatus status = status == orderStatusDispatched || status == orderStatusCancelled

-- * Page

pageView :: StorePage -> Html ()
pageView page =
  let storeKey = page.storePageKey
      storeProduct = page.storePageProduct
      orders = page.storePageOrders
      -- Built outside the quote: hsx has no case for record construction
      -- in a splice (IHP.HSX.HsExpToTH.toFieldExp is undefined).
      panel = StorePanel {storePanelKey = storeKey, storePanelProduct = storeProduct}
   in widgetPage [hsx|
    <header class="bg-white shadow-sm border-b border-gray-200">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="flex justify-between items-center py-6">
          <div class="flex items-center space-x-4">
            <div class="w-10 h-10 bg-gradient-to-r from-primary-500 to-primary-600 rounded-lg flex items-center justify-center">
              <svg class="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 11V7a4 4 0 00-8 0v4M5 9h14l1 12H4L5 9z"></path>
              </svg>
            </div>
            <h1 class="text-2xl font-bold text-gray-900">Widget Store</h1>
          </div>
          <img
            src="https://dbos-blog-posts.s3.us-west-1.amazonaws.com/logos/black_logotype%2Btransparent_bg_h4000px.png"
            alt="DBOS Logo"
            class="h-8 opacity-70"
          />
        </div>
      </div>
    </header>

    <main class="flex-1">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-8">
          <div class="lg:col-span-1">
            <div class="bg-white rounded-2xl shadow-xl overflow-hidden card-hover">
              <div class="bg-gradient-to-r from-blue-500 to-blue-600 p-6 text-white">
                <div class="flex items-center space-x-3">
                  <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path>
                  </svg>
                  <h2 class="text-2xl font-bold">Recent Orders</h2>
                </div>
              </div>
              <div
                id="orders-panel"
                class="p-6"
                hx-get={widgetMountPath <> "/orders"}
                hx-trigger="load, every 1s"
                hx-swap="innerHTML"
              >
                {ordersList orders}
              </div>
            </div>
          </div>

          <div class="lg:col-span-1">
            <div class="bg-white rounded-2xl shadow-xl overflow-hidden card-hover">
              <div class="bg-gradient-to-r from-primary-500 to-primary-600 p-6 text-white text-center">
                <div class="flex items-center justify-center space-x-3">
                  <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 11V7a4 4 0 00-8 0v4M5 9h14l1 12H4L5 9z"></path>
                  </svg>
                  <h2 class="text-2xl font-bold">Widget Store</h2>
                </div>
              </div>
              <div class="p-6" id="store-panel">
                {storePanelView panel}
              </div>
            </div>
          </div>

          <div class="lg:col-span-1">
            <div class="bg-white rounded-2xl shadow-xl overflow-hidden card-hover">
              <div class="bg-gradient-to-r from-red-500 to-red-600 p-6 text-white">
                <div class="flex items-center space-x-3">
                  <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"></path>
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"></path>
                  </svg>
                  <h2 class="text-2xl font-bold">Server Tools</h2>
                </div>
              </div>
              <div class="p-6">
                <div class="text-center space-y-4">
                  <div class="flex justify-center">
                    <div class="w-16 h-16 bg-red-100 rounded-full flex items-center justify-center">
                      <svg class="w-8 h-8 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path>
                      </svg>
                    </div>
                  </div>
                  <h4 class="text-lg font-bold text-gray-900">Crash Simulation</h4>
                  <p class="text-gray-600 text-sm">
                    Click the button below to instantly crash the application and observe
                    how it recovers and continues processing from the exact point of failure.
                  </p>
                  <button
                    class="w-full bg-gradient-to-r from-red-500 to-red-600 hover:from-red-600 hover:to-red-700 text-white font-bold py-4 px-6 rounded-xl transition-all duration-300 transform hover:scale-105 shadow-lg hover:shadow-xl"
                    hx-post={widgetMountPath <> "/crash_application"}
                    hx-target="#crash-result"
                    hx-swap="innerHTML"
                  >
                    <svg class="w-5 h-5 mr-2 inline-block" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path>
                    </svg>
                    Crash the Application
                  </button>
                  <div id="crash-result" class="text-sm text-gray-500"></div>
                </div>

                <div class="bg-blue-50 border border-blue-200 rounded-lg p-4">
                  <div class="flex items-start space-x-3">
                    <svg class="w-5 h-5 text-blue-600 mt-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                    </svg>
                    <div>
                      <h4 class="font-bold text-blue-800">How it Works</h4>
                      <p class="text-blue-700 mt-1 text-sm">
                        DBOS uses durable execution to ensure that workflows survive failures.
                        When you crash the app, it will restart and continue any interrupted operations.
                      </p>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div class="mt-12 text-center">
          <div class="bg-gradient-to-r from-primary-500 to-primary-600 rounded-2xl p-8 text-white">
            <h3 class="text-2xl font-bold mb-4">Build Your Own Crash-Proof Applications</h3>
            <p class="text-primary-100 mb-6">
              Learn how to create resilient applications that can survive any failure with DBOS
            </p>
            <a
              href="https://docs.dbos.dev/"
              target="_blank"
              rel="noopener noreferrer"
              class="inline-flex items-center bg-white text-primary-600 font-bold py-3 px-6 rounded-lg hover:bg-gray-100 transition-colors duration-200"
            >
              <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.746 0 3.332.477 4.5 1.253v13C19.832 18.477 18.246 18 16.5 18c-1.746 0-3.332.477-4.5 1.253"></path>
              </svg>
              Read the Programming Guide
            </a>
          </div>
        </div>
      </div>
    </main>
  |]

-- | The widget store's head, rendered into the common 'pageShell'
-- ('Demo.Http'), and the body wrapper with the connection-lost alert wired
-- to htmx's error events.
widgetHead :: Html ()
widgetHead =
  [hsx|
    <title>DBOS Widget Store</title>
        <link
          rel="icon"
          type="image/x-icon"
          href="https://dbos-blog-posts.s3.us-west-1.amazonaws.com/live-demo/favicon.ico"
        />
        <script src="https://cdn.tailwindcss.com"></script>
        <script>
          tailwind.config = {
            theme: {
              extend: {
                colors: {
                  primary: {
                    50: '#f0f9ff',
                    100: '#e0f2fe',
                    200: '#bae6fd',
                    300: '#7dd3fc',
                    400: '#38bdf8',
                    500: '#0ea5e9',
                    600: '#0284c7',
                    700: '#0369a1',
                    800: '#075985',
                    900: '#0c4a6e',
                  },
                  secondary: {
                    50: '#f8fafc',
                    100: '#f1f5f9',
                    200: '#e2e8f0',
                    300: '#cbd5e1',
                    400: '#94a3b8',
                    500: '#64748b',
                    600: '#475569',
                    700: '#334155',
                    800: '#1e293b',
                    900: '#0f172a',
                  }
                },
                animation: {
                  'fade-in': 'fadeIn 0.5s ease-in-out',
                  'slide-up': 'slideUp 0.3s ease-out',
                  'pulse-slow': 'pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite',
                  'bounce-subtle': 'bounceSubtle 1s ease-in-out infinite',
                },
                keyframes: {
                  fadeIn: {
                    '0%': { opacity: '0' },
                    '100%': { opacity: '1' }
                  },
                  slideUp: {
                    '0%': { transform: 'translateY(10px)', opacity: '0' },
                    '100%': { transform: 'translateY(0)', opacity: '1' }
                  },
                  bounceSubtle: {
                    '0%, 100%': { transform: 'translateY(0)' },
                    '50%': { transform: 'translateY(-5px)' }
                  }
                }
              }
            }
          }
        </script>
        <style>
          @keyframes rotation {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
          }

          .spinner {
            width: 24px;
            height: 24px;
            border-radius: 50%;
            display: inline-block;
            border-top: 3px solid #fff;
            border-right: 3px solid transparent;
            box-sizing: border-box;
            animation: rotation 1s linear infinite;
          }

          .glass-effect {
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.2);
          }

          .progress-bar {
            background: linear-gradient(90deg, #10b981, #059669);
            transition: width 0.3s ease;
          }

          .widget-glow {
            box-shadow: 0 0 20px rgba(16, 185, 129, 0.3);
          }

          .gradient-bg {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          }

          .card-hover {
            transition: all 0.3s ease;
          }

          .card-hover:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04);
          }

          .status-badge {
            display: inline-flex;
            align-items: center;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.025em;
          }

          .status-pending {
            background-color: #fef3c7;
            color: #92400e;
          }

          .status-paid {
            background-color: #d1fae5;
            color: #065f46;
          }

          .status-dispatched {
            background-color: #e0e7ff;
            color: #3730a3;
          }

          .status-cancelled {
            background-color: #fee2e2;
            color: #991b1b;
          }

          .modern-scrollbar::-webkit-scrollbar {
            width: 8px;
          }

          .modern-scrollbar::-webkit-scrollbar-track {
            background: #f1f5f9;
            border-radius: 4px;
          }

          .modern-scrollbar::-webkit-scrollbar-thumb {
            background: linear-gradient(135deg, #3b82f6, #1d4ed8);
            border-radius: 4px;
            transition: background 0.3s ease;
          }

          .modern-scrollbar::-webkit-scrollbar-thumb:hover {
            background: linear-gradient(135deg, #2563eb, #1e40af);
          }

          .modern-scrollbar {
            scrollbar-width: thin;
            scrollbar-color: #3b82f6 #f1f5f9;
          }
        </style>
  |]

widgetPage :: Html () -> Html ()
widgetPage body =
  pageShell widgetHead
    [hsx|
      <div class="min-h-screen bg-gradient-to-br from-secondary-50 to-primary-50 font-sans">
        <div
          id="connection-lost"
          role="alert"
          aria-live="assertive"
          class="hidden fixed top-4 left-4 right-4 z-50 justify-center items-center text-center bg-red-500 text-white p-4 rounded-lg shadow-lg"
        >
          <div class="flex items-center space-x-3">
            <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
              <path fill-rule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clip-rule="evenodd"></path>
            </svg>
            <span>Connection to server lost. Reconnecting...</span>
            <span class="spinner ml-2"></span>
          </div>
        </div>
        <script>
          document.body.addEventListener('htmx:error', function () {
            var alertBox = document.getElementById('connection-lost');
            if (alertBox) { alertBox.classList.remove('hidden'); alertBox.classList.add('flex'); }
          });
          document.body.addEventListener('htmx:after:request', function (event) {
            var response = event.detail && event.detail.ctx && event.detail.ctx.response;
            if (response && response.status && response.status < 400) {
              var alertBox = document.getElementById('connection-lost');
              if (alertBox) { alertBox.classList.add('hidden'); alertBox.classList.remove('flex'); }
            }
          });
        </script>
        {body}
      </div>
    |]

-- * Middle panel: store, payment, status

-- | The store view: product card with the Rust buttons, now htmx.
storePanelView :: StorePanel -> Html ()
storePanelView panel =
  let storeKey = panel.storePanelKey
      storeProduct = panel.storePanelProduct
   in [hsx|
    <div class="text-center space-y-6">
      <div class="relative">
        <img
          src="https://dbos-blog-posts.s3.us-west-1.amazonaws.com/live-demo/widget-small.webp"
          alt="Widget Image"
          class="w-40 h-40 object-cover rounded-2xl mx-auto shadow-lg widget-glow"
        />
        <div class="absolute -top-2 -right-2 bg-green-500 text-white px-3 py-1 rounded-full text-sm font-bold animate-bounce-subtle">
          In Stock
        </div>
      </div>

      <div class="space-y-4">
        <h3 class="text-xl font-bold text-gray-900">{storeProduct.productName}</h3>
        <p class="text-gray-600">{storeProduct.productDescription}</p>

        <div class="flex justify-center items-center space-x-4">
          <div class="text-2xl font-bold text-primary-600">
            ${Text.pack (show storeProduct.productPrice)}
          </div>
          <div class="flex items-center space-x-2">
            <div class="text-sm text-gray-500">
              Only <span class="font-bold text-red-600">{Text.pack (show storeProduct.productInventory)}</span> left!
            </div>
            <button
              hx-post={widgetMountPath <> "/restock"}
              hx-target="#store-panel"
              hx-swap="innerHTML"
              class="bg-green-500 hover:bg-green-600 text-white font-medium py-1 px-2 rounded text-xs transition-colors duration-200 flex items-center space-x-1"
              title="Restock inventory"
            >
              <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path>
              </svg>
              <span>Restock</span>
            </button>
          </div>
        </div>
      </div>

      <button
        hx-post={widgetMountPath <> "/checkout/" <> storeKey}
        hx-target="#store-panel"
        hx-swap="innerHTML"
        disabled={storeProduct.productInventory <= 0}
        class="w-full bg-gradient-to-r from-green-500 to-green-600 hover:from-green-600 hover:to-green-700 disabled:from-gray-400 disabled:to-gray-500 text-white font-bold py-4 px-6 rounded-xl text-lg transition-all duration-300 transform hover:scale-105 shadow-lg hover:shadow-xl"
      >
        <svg class="w-5 h-5 mr-2 inline-block" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 3h2l.4 2M7 13h10l4-8H5.4M7 13L5.4 5M7 13l-2.293 2.293c-.63.63-.184 1.707.707 1.707H17m0 0a2 2 0 100 4 2 2 0 000-4zm-8 2a2 2 0 11-4 0 2 2 0 014 0z"></path>
        </svg>
        Buy Now
      </button>
    </div>
  |]

-- | The payment view: the checkout is parked on its @recv@ while this is
-- shown; confirming or cancelling posts the webhook.
paymentPanel :: Text -> Html ()
paymentPanel paymentId =
  [hsx|
    <div class="text-center space-y-6">
      <div class="w-20 h-20 bg-gradient-to-r from-blue-100 to-green-100 rounded-full flex items-center justify-center mx-auto shadow-lg">
        <svg class="w-10 h-10 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 10h18M7 15h1m4 0h1m-7 4h12a3 3 0 003-3V8a3 3 0 00-3-3H6a3 3 0 00-3 3v8a3 3 0 003 3z"></path>
        </svg>
      </div>
      <h3 class="text-xl font-bold text-gray-900">Payment Confirmation</h3>
      <p class="text-gray-600">Please confirm your payment to complete the order</p>

      <div class="space-y-3">
        <button
          hx-post={widgetMountPath <> "/payment_webhook/" <> paymentId <> "/paid"}
          hx-target="#store-panel"
          hx-swap="innerHTML"
          class="w-full bg-green-600 hover:bg-green-700 text-white font-bold py-3 px-6 rounded-lg transition-colors duration-200 flex items-center justify-center"
        >
          <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path>
          </svg>
          Confirm Payment
        </button>
        <button
          hx-post={widgetMountPath <> "/payment_webhook/" <> paymentId <> "/failed"}
          hx-target="#store-panel"
          hx-swap="innerHTML"
          class="w-full bg-red-600 hover:bg-red-700 text-white font-bold py-3 px-6 rounded-lg transition-colors duration-200 flex items-center justify-center"
        >
          <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
          </svg>
          Cancel Payment
        </button>
      </div>
      <p class="text-xs text-gray-400">Payment id {paymentId}</p>
    </div>
  |]

-- | The order status view. While the order is not terminal the panel polls
-- itself every second; the terminal render carries no trigger, so polling
-- stops on its own.
orderStatusPanel :: Order -> Html ()
orderStatusPanel order
  | isTerminalStatus order.orderStatus = [hsx|<div class="text-center space-y-6">{orderStatusBody order}</div>|]
  | otherwise =
      [hsx|
        <div
          class="text-center space-y-6"
          hx-get={widgetMountPath <> "/order/" <> Text.pack (show order.orderId)}
          hx-trigger="every 1s"
          hx-swap="outerHTML"
        >
          {orderStatusBody order}
        </div>
      |]

orderStatusBody :: Order -> Html ()
orderStatusBody order =
  [hsx|
    <div class="w-16 h-16 bg-blue-100 rounded-full flex items-center justify-center mx-auto">
      <svg class="w-8 h-8 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path>
      </svg>
    </div>
    <h3 class="text-xl font-bold text-gray-900">Order Status</h3>

    <div class="bg-gray-50 rounded-lg p-4 space-y-3">
      <div class="flex justify-between items-center">
        <span class="font-medium text-gray-700">Order ID:</span>
        <span class="font-bold text-gray-900">{Text.pack (show order.orderId)}</span>
      </div>
      <div class="flex justify-between items-center">
        <span class="font-medium text-gray-700">Status:</span>
        <span class={statusBadgeClass order.orderStatus}>{statusName order.orderStatus}</span>
      </div>
      <div class="flex justify-between items-center">
        <span class="font-medium text-gray-700">Last Updated:</span>
        <span class="text-gray-600 text-sm">{order.orderLastUpdateTime}</span>
      </div>
    </div>

    <button
      hx-get={widgetMountPath <> "/store"}
      hx-target="#store-panel"
      hx-swap="innerHTML"
      class="w-full bg-primary-600 hover:bg-primary-700 text-white font-bold py-3 px-6 rounded-lg transition-colors duration-200"
    >
      Continue Shopping
    </button>
  |]

-- | What a failed checkout leaves in the middle panel: the Rust storefront
-- logs the failure; the htmx surface shows it and offers a retry with a
-- fresh idempotency key.
checkoutErrorPanel :: Text -> Html ()
checkoutErrorPanel message =
  [hsx|
    <div class="text-center space-y-6">
      <div class="w-20 h-20 bg-red-100 rounded-full flex items-center justify-center mx-auto">
        <svg class="w-10 h-10 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path>
        </svg>
      </div>
      <h3 class="text-xl font-bold text-gray-900">Checkout Failed</h3>
      <p class="text-gray-600">{message}</p>
      <button
        hx-get={widgetMountPath <> "/store"}
        hx-target="#store-panel"
        hx-swap="innerHTML"
        class="w-full bg-primary-600 hover:bg-primary-700 text-white font-bold py-3 px-6 rounded-lg transition-colors duration-200"
      >
        Try Again
      </button>
    </div>
  |]

-- * Left panel: the orders list fragment

ordersList :: [Order] -> Html ()
ordersList [] =
  [hsx|
    <div class="text-center py-12">
      <svg class="w-16 h-16 text-gray-400 mx-auto mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path>
      </svg>
      <p class="text-gray-500 text-lg">No orders yet</p>
      <p class="text-gray-400 text-sm mt-2">Orders will appear here as they're placed</p>
    </div>
  |]
ordersList orders =
  [hsx|
    <div class="space-y-3 max-h-96 overflow-y-auto modern-scrollbar">
      {mapM_ orderItem (reverse orders)}
    </div>
  |]

orderItem :: Order -> Html ()
orderItem order =
  [hsx|
    <div class="bg-gray-50 border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow duration-200">
      <div class="flex justify-between items-start">
        <div>
          <div class="font-bold text-gray-900">Order #{Text.pack (show order.orderId)}</div>
          <div class={badgeClasses}>{statusName order.orderStatus}</div>
        </div>
        <div class="text-right">
          {progressBlock}
        </div>
      </div>
    </div>
  |]
  where
    badgeClasses = "status-badge mt-1 " <> statusBadgeClass order.orderStatus
    progressBlock
      | order.orderStatus == orderStatusPaid =
          [hsx|
            <div class="text-sm text-gray-600">
              <div>Progress to dispatch:</div>
              <div class="w-24 bg-gray-200 rounded-full h-2 mt-1">
                <div class="progress-bar h-2 rounded-full" style={progressStyle}></div>
              </div>
            </div>
          |]
      | otherwise = mempty
    progressStyle = "width: " <> progressWidth order.orderProgressRemaining
