# Widget step tables (A3 sketch, STM side)

Status: sketch — tables + STM handlers specified exactly; direct handler
tests can land pre-rewire; engine integration waits on the C-phase bridge
(see §5). Store shape unchanged (`wsInventory`, `wsOrders :: Map Int
(Int, Int)`, `wsNextOrder`); only bodies migrate, never tables.

Status codes (verify against `WidgetTest` assertions during migration):
`0` open, `2` paid, `-1` refused/cancelled, `1` dispatched; progress `3→0`.

## 1. Tables (per workflow, StepCtx-keyed)

```haskell
newtype OrderId = OrderId Int deriving stock (Eq, Show)

data CheckoutOps exec m = CheckoutOps
  { coCreate    :: StepCtx exec m -> m OrderId
  , coReserve   :: StepCtx exec m -> m Bool
  , coUndo      :: StepCtx exec m -> m ()
  , coSetStatus :: StepCtx exec m -> OrderId -> Int -> m ()
  }

data DispatchOps exec m = DispatchOps
  { doTick      :: StepCtx exec m -> OrderId -> m ()
  , doSetStatus :: StepCtx exec m -> OrderId -> Int -> m ()
  }
```

Shared-field convention: `setStatus` is repeated in both tables.
Rule: repeat when shared by ≤2 tables (records are cheap, no coupling);
embed a sub-record once ≥3 tables need the same subgroup. No inheritance.

## 2. STM handlers (single atomically per op)

```haskell
stmCheckoutOps :: WidgetStore (IOSim s) -> CheckoutOps exec (IOSim s)
stmCheckoutOps store = CheckoutOps
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

stmDispatchOps :: WidgetStore (IOSim s) -> DispatchOps exec (IOSim s)
stmDispatchOps store = DispatchOps
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
failingCheckoutOps :: StrictTVar (IOSim s) Int -> WidgetStore (IOSim s) -> CheckoutOps exec (IOSim s)
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

Engine integration (replay rows, stub retirement) waits on the C-phase
Step slice: `Ctx`-holding bodies have no `StepCtx` to spend, and
`runTransaction` bodies receive `Tx` but no scope — there is no bridge
until runners accept `StepCtx`. The stubs (`createOrder`,
`reserveInventory`, … taking ignored `Tx`) retire when call sites flip;
until then both coexist without conflict (different names, no shared code).

## 5. Live contract (for C-phase, specified not designed)

Post-rewire `runTransaction` (or its transactional-step successor)
supplies **both** scope and connection to its body —
`(StepCtx exec m -> Tx m -> m …)` shape to be fixed in the Step slice —
so Postgres tables close over the per-step `Tx` exactly where the STM
tables close over nothing:

```haskell
-- live call shape (target; engine API pending):
runTransaction ds ctx cfg (\tx s -> coReserve (pgCheckoutOps tx) s ...)
```

Until then PG tables keep today's `Tx`-only shape; table-type sharing
across stacks arrives with the bridge, not before. Do not fake it with
dummy `Tx` in new code.

## 6. Migration checklist

- [ ] Tables + STM handlers + failing variant land (this sketch).
- [ ] Direct handler tests: mint sequence, oversell race, bomb rollback,
      failing stops-before-dispatch, status codes vs `WidgetTest`
      assertions (`(1,1,0)` dispatched, `(1,-1,3)` refused).
- [ ] C-phase: Step slice accepts `StepCtx` in runners; runTransaction
      supplies scope+connection.
- [ ] Flip `WidgetSim` call sites; delete the `Tx`-ignoring fakes.
- [ ] PG tables + live flip; one mixed live+canned test.
