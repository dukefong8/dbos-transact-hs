# Widget step tables (A3 sketch, STM side)

Status: landed (D2, 2026-10-04) — tables + STM handlers + engine
integration on both stacks. Store shape unchanged (`wsInventory`,
`wsOrders :: Map Int (Int, Int)`, `wsNextOrder`); only bodies migrated,
never tables.

Status codes (verify against `WidgetTest` assertions during migration):
`0` open, `2` paid, `-1` refused/cancelled, `1` dispatched; progress `3→0`.

## 1. Tables (per workflow, StepCtx-keyed)

```haskell
newtype OrderId = OrderId Int deriving stock (Eq, Show)

data CheckoutSteps exec m = CheckoutSteps
  { coCreate    :: StepCtx exec m -> m OrderId
  , coReserve   :: StepCtx exec m -> m Bool
  , coUndo      :: StepCtx exec m -> m ()
  , coSetStatus :: StepCtx exec m -> OrderId -> Int -> m ()
  }

data DispatchSteps exec m = DispatchSteps
  { doTick      :: StepCtx exec m -> OrderId -> m ()
  , doSetStatus :: StepCtx exec m -> OrderId -> Int -> m ()
  }
```

Shared-field convention: `setStatus` is repeated in both tables.
Rule: repeat when shared by ≤2 tables (records are cheap, no coupling);
embed a sub-record once ≥3 tables need the same subgroup. No inheritance.

## 2. STM handlers (single atomically per op)

```haskell
stmCheckoutSteps :: WidgetStore (IOSim s) -> CheckoutSteps exec (IOSim s)
stmCheckoutSteps store = CheckoutSteps
  { coCreate = \_ -> atomically $ do
      oid <- readTVar store.wsNextOrder
      writeTVar store.wsNextOrder (oid + 1)
      modifyTVar store.wsOrders (Map.insert oid (0, 3))
      pure (OrderId oid)
  , coReserve = \_ -> atomically $ do
      stock <- readTVar store.wsInventory
      if stock > 0
        then writeTVar store.wsInventory (stock - 1) >> pure True
        else pure False
  , coUndo = \_ -> atomically (modifyTVar store.wsInventory (+ 1))
  , coSetStatus = \_ (OrderId oid) status -> atomically $
      modifyTVar store.wsOrders (Map.adjust (\(_, progress) -> (status, progress)) oid)
  }

stmDispatchSteps :: WidgetStore (IOSim s) -> DispatchSteps exec (IOSim s)
stmDispatchSteps store = DispatchSteps
  { doTick = \_ (OrderId oid) -> atomically $
      modifyTVar store.wsOrders $
        Map.adjust
          (\(status, progress) -> if progress <= 1 then (1, 0) else (status, progress - 1))
          oid
  , doSetStatus = \_ (OrderId oid) status -> atomically $
      modifyTVar store.wsOrders (Map.adjust (\(_, progress) -> (status, progress)) oid)
  }
```

Notes:

- The `StepCtx` is capability-only (`\_`): callers must hold step scope;
  implementations don't read it. Same shape as the dual-store probe.
- Fidelity fixes vs today's fakes: `createOrder`/`reserveInventory`
  split `readTVarIO` from their write block — two concurrent checkouts
  can mint one id / oversell one unit. Single-`atomically` closes both;
  the race test (Phase D) pins it.
- `OrderId` is bound at the ops boundary; TVar keys stay `Int`
  (no table migration).

## 3. Failing variant (deterministic seed-8)

```haskell
failingCheckoutSteps :: StrictTVar (IOSim s) Int -> WidgetStore (IOSim s) -> CheckoutSteps exec (IOSim s)
-- counts table calls; the 3rd (the paid write) aborts via throwSTM:
-- nothing recorded, checkout stops before the dispatch child.
```

`throwSTM` inside the block aborts it whole (the paid mark vanishes with
the throw — the correct shape of "cannot commit"). Replaces
`failingWidgetDs`'s `BackendError` injection with a domain-level refusal.

## 4. Sim sequencing: direct tests now, engine integration at C-phase

Direct handler tests need no engine: build a store + `withWorkflow` over
`simConnectionWith`, drive table ops through `withStep`, assert TVar
state (create mints 1,2,3; race keeps stock ≥ 0; bomb leaves state
untouched; failing variant stops before dispatch). These land pre-rewire.

Engine integration landed (D2): `runTransactionScoped` hands the body the
step view beside the transaction handle, so the workflows spend the tables
directly and the stubs are deleted. The `Tx`-ignoring fakes
(`createOrder`, `reserveInventory`, …) and the `BackendError`-injection
datasource (`failingWidgetDs`) are retired; the seed-8 case runs the
domain-level `failingCheckoutSteps` over the plain fake datasource.

## 5. Live contract (landed as specified)

`runTransactionScoped` supplies **both** scope and connection to its body —
`(StepCtx exec m -> Tx m -> m …)` — so Postgres tables close over the
per-step `Tx` exactly where the STM tables close over nothing:

```haskell
-- live call shape (WidgetTest):
runTransactionScoped ds wctx cfg (\sctx tx -> coReserve (pgCheckoutSteps tables tx) s)
```

Table types (`CheckoutSteps`/`DispatchSteps`/`OrderId`) are shared across stacks
(dup'd between the sim and live trees per the test convention); only the
builders differ (store-closed vs Tx-closed).

Until then PG tables keep today's `Tx`-only shape; table-type sharing
across stacks arrives with the bridge, not before. Do not fake it with
dummy `Tx` in new code.

## 6. Migration checklist

- [x] Tables + STM handlers + failing variant land (this sketch).
- [x] Direct handler tests: mint sequence, oversell race, bomb rollback,
      failing stops-before-dispatch, status codes vs `WidgetTest`
      assertions (`(1,1,0)` dispatched, `(1,-1,3)` refused).
- [x] C-phase: Step slice accepts `StepCtx` in runners; runTxStep
      supplies scope+connection.
- [x] Flip `WidgetSim` call sites; delete the `Tx`-ignoring fakes.
- [x] PG tables + live flip; one mixed live+canned test (D2, 2026-10-04:
      `pgCheckoutSteps`/`pgDispatchSteps` close over the held `Tx`;
      `failingPgCheckoutSteps` refuses the paid mark; the canned-fail case
      asserts no `order_id`, order `(1,0,3)`, inventory `4`, and the
      workflow row stays `PENDING` with no dispatch child).

## Dual-stack conversion (2026-10-04)

The widget step tables are exercised by one shared scenario set
(`test/DBOS/Transact/WidgetCases.hs`) under both interpretations: the live
tree runs them over Postgres with `pgCheckoutSteps`/`pgDispatchSteps` and the
real datasource, the sim tree under IOSim with the STM handlers and a fake
datasource. The fakes now model the checkpoint PK for real — an
insert-or-`False` map keyed by (workflow, step id) — and the shared checks
assert **one commit per step name** (and byte-identical effects across a
crash-and-relaunch), so the positive cases prove the transactional step's
once-and-only-once guarantee rather than at-least-once presence. The runner
helpers live in `test/DBOS/DualStack.hs`; the plan and outcome are recorded in
`docs/dual-stack-concurrency-todo.md` (Step 7).
