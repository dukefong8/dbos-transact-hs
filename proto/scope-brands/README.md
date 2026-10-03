# dbos-scope-proto — THROWAWAY, not production

Prototype for the §11 phantom-brand direction
(`docs/ownership-lifetimes-isomorphism.md` §11), descoped per decision:
`inst` dropped (the DBOS object is singleton-like per process, so
cross-instance mixing stays a runtime refusal per the oracle), keeping
execution-scope (`exec`) branding, the `WorkflowCtx`/`StepCtx` split,
the scope-depth backstop — and phase 3's scoped pool pinning. Plain
phantoms throughout, not the Bluefin dependency.

Lives on the **proto/phantom-brands throwaway branch only**; main keeps
nothing but the validated decision. Findings consolidated in
`docs/scoped-workflow-capabilities.md` (same branch).

## Questions under test

1. Does `exec` branding make cross-execution pending-driving a compile
   error?
2. Does the context split make step-body child-starts and counter
   allocation compile errors for the natural code shape?
3. Does cross-instance refusal still fire at runtime (oracle parity)?
4. Does it all compose with `m` polymorphic over `IO` and `IOSim s`?
5. Does it infer without annotation burden?
6. Phase 2 (depth): can a scope-depth counter refuse allocations that
   arrive while a step body runs — including through a captured parent
   context?
7. Phase 3 (pinning): can a step-scoped region pin one pool connection
   for an attempt's lifetime — bounded (waits past capacity instead of
   opening more), refused inside step bodies (uniform depth rule),
   unusable across executions (exec brand) and after release
   (generation backstop)?

## Run

```sh
./run.sh
```

- builds the model + the three positive flows, runs them (2 checks);
- typechecks each `neg/N*.hs` expecting **failure** (5 checks: N2, N4,
  N5, N6, N7);
- runs `proto-backstop`, asserting runtime behaviour (12 checks:
  B0–B11).

19 checks total.

## Layout

- `src/Scope/Model.hs` — the analogues (private constructors, explicit
  exports = the privacy boundary). `WorkflowCtx` owns the step/marker
  counters plus the scope-depth counter; `StepCtx` is the narrowed
  per-attempt view (marker, status, fresh token); `placeCall`
  depth-checks allocation; `withStep` bumps/restores depth under
  `finally`; `startChild` checks the registry's bound instance id first
  (runtime `WrongInstance`), then derives `parent-step` ids; `Pool`
  models a bounded pool (blocking STM take, per-id generations,
  high-water mark) with `checkout` (depth-checked, exec-branded),
  `releasePin` (invalidating), `useIn` (generation-checked), and the
  `rawAcquire` contrast path that bypasses the cap.
- `app-io/Main.hs` — positive flow under `IO`, incl. two instance
  objects side by side proving counter independence.
- `app-sim/Main.hs` — positive flow under `runSim`: two instance
  objects in one simulation + virtual-thread rendezvous (tag nests in
  the sim tag, determinism kept).
- `app-backstop/Main.hs` — runtime checks (B0–B11), incl. the
  deterministic three-racer pool race under `runSim`.
- `neg/` — negative compile tests (N2, N4, N5, N6, N7).

## Notes

- The real tree's HLS config may flag these files (no component owns
  them); cosmetic, branch-local. `cabal build` at the repo root ignores
  this directory (own `cabal.project`, not in any root `hs-source-dirs`).
- `Text` ids stay bare on purpose: names must escape scopes; only the
  capabilities interpreting them are branded (the `DbEff e` vs `DbHandle`
  split from the Bluefin research).
- Connection/registry ids derive from the given name (the real tree
  mints a UUID per connection) — tests use distinct names per object.
- `placeAwait` does not degrade inside steps here; the real `awaitChild`
  degrades to unrecorded via the leaf rule — out of focus for this
  prototype, which tests allocation refusal.
- Pin at *step* scope (`withStep` lifetime == transaction lifetime), not
  workflow scope: a workflow-level pin would hold a pool slot across
  blocking waits and hand replayed steps a connection they must not
  touch. The model enforces nothing here — it is a policy note for the
  real migration (pin in the transactional runner, inside the attempt).
- Whether hasql-pool exposes single-connection checkout was not
  established (the API survey timed out); the prototype models pool
  semantics abstractly. If it doesn't, the fallback is today's raw
  `Connection.acquire` + exec-branded handle — same escape safety, pool
  limits unenforced (the B9 contrast quantifies exactly what that gives
  up).
