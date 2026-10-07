---
title: Testing DBOS Applications
impact: LOW-MEDIUM
impactDescription: Live plus simulated coverage of the same bodies
tags: testing, tasty, iosim
---

## Testing DBOS Applications

Tasty; every test owns its rows — fresh UUIDs, unique workflow/queue names, per-test executor ids. Two trees drive the same bodies:

- Live: launch on the real database, assert app tables *and* `dbos.workflow_status` / `dbos.operation_outputs`; cover replay (crash then relaunch) and refusal paths.
- IOSim: run the same bodies over STM-backed handlers and assert exact event traces via `printSimTrace`.

```haskell
-- Live leaf owns its workflow id; sim leaf asserts the trace.
```

Run one IO/Sim pair at a time in the watcher (`make dev` owns `ghcid.txt`); `cabal test all` only when the watcher is idle. Demos: stop the server before running tests (its queue supervisor claims fixtures).

Difference from TS: TS uses Jest with mocks. Haskell uses Tasty live + IOSim sim mirrors; tests import internals directly for fixtures (`DBOS.Transact.Context`, `Checkpoint`), app code imports only `DBOS.Transact`.
