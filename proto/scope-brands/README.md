# dbos-scope-proto — THROWAWAY, not production

Prototype for the §11 phantom-brand direction
(`docs/ownership-lifetimes-isomorphism.md` §11), descoped per decision:
`inst` dropped (the DBOS object is singleton-like per process, so
cross-instance mixing stays a runtime refusal per the oracle), keeping
execution-scope (`exec`) branding, the `WorkflowCtx`/`StepCtx` split, and
the scope-depth backstop — plain phantoms, not the Bluefin dependency.

Lives on the **proto/phantom-brands throwaway branch only**; main keeps
nothing but the validated decision. Findings consolidated in
`docs/scoped-workflow-capabilities.md` (same branch).

## Question under test

Does execution-scope (`exec`) branding plus the context split:
1. make cross-execution pending-driving a compile error,
2. make step-body child-starts and counter allocation compile errors for
   the natural code shape,
3. refuse cross-instance starts at runtime with `WrongInstance` (oracle
   parity — the old compile-time N1, now a runtime backstop check),
4. compose with `m` polymorphic over `IO` and `IOSim s`,
5. infer without annotation burden?

Plus the depth question: can a scope-depth counter in shared
per-execution state refuse allocations that arrive while a step body is
running — including through a captured parent context — so the residual
capture hole is closed at runtime while the natural shape is closed at
compile time?

## Run

```sh
./run.sh
```

- builds the model + the three positive flows, runs them (2 checks);
- typechecks each `neg/N*.hs` expecting **failure** (4 checks: N2, N4,
  N5, N6);
- runs `proto-backstop`, asserting cross-instance refusal plus the depth
  behaviour (7 checks: B0–B6).

13 checks total.

## Layout

- `src/Scope/Model.hs` — the analogues (private constructors, explicit
  exports = the privacy boundary). `WorkflowCtx` owns the step/marker
  counters plus the scope-depth counter; `StepCtx` is the narrowed
  per-attempt view (marker, status, fresh token); `placeCall`
  depth-checks allocation; `withStep` bumps/restores depth under
  `finally`; `startChild` checks the registry's bound instance id first
  (runtime `WrongInstance`, ADR-0018 parity), then routes through
  `placeCall`; `drive` keeps the execution-token backstop.
- `app-io/Main.hs` — positive flow under `IO`, incl. two instance
  objects side by side proving counter independence.
- `app-sim/Main.hs` — positive flow under `runSim`: two instance
  objects in one simulation + virtual-thread rendezvous (tag nests in
  the sim tag, determinism kept).
- `app-backstop/Main.hs` — runtime checks (B0–B6).
- `neg/` — negative compile tests (N2, N4, N5, N6).

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
