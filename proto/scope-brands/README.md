# dbos-scope-proto — THROWAWAY, not production

Prototype for the §11 phantom-brand direction
(`docs/ownership-lifetimes-isomorphism.md` §11:
`Ctx s m`, `WorkflowRef s m`, `WorkflowHandle s m`, rank-2 binders at the
seam — plain phantoms, not the Bluefin dependency).

Lives on the **proto/phantom-brands throwaway branch only**; main keeps
nothing but the validated decision. Findings consolidated in
`docs/scoped-workflow-capabilities.md` (same branch).

## Question under test

Does instance-scope (`inst`) + execution-scope (`exec`) branding:
1. make cross-instance ref/ctx mixing a compile error (R9 cross-instance),
2. make cross-execution pending-driving a compile error (R9/checkHere half),
3. make step-body child-starts a compile error for the natural code shape,
4. compose with `m` polymorphic over `IO` and `IOSim s` (two tags nesting),
5. infer without annotation burden?

Phase 2 adds the depth question: can a scope-depth counter in shared
per-execution state refuse allocations that arrive while a step body is
running — including through a captured parent context — so the residual
capture hole is closed at runtime while the natural shape is closed at
compile time?

## Run

```sh
./run.sh
```

- builds the model + the three positive flows, runs them;
- typechecks each `neg/N*.hs` expecting **failure** (the enforcement);
- runs `proto-backstop`, asserting the capture shape is **refused** at
  runtime, depth restores after success and after a throw, markers are
  per-attempt, and tokens are per-attempt.

## Layout

- `src/Scope/Model.hs` — the analogues (private constructors, explicit
  exports = the privacy boundary). `WorkflowCtx` owns the step/marker counters
  plus the scope-depth counter; `StepCtx` is the narrowed per-attempt view
  (marker, status, fresh token); `placeCall` depth-checks allocation;
  `withStep` bumps/restores depth under `finally`; `drive` keeps the
  execution-token backstop.
- `app-io/Main.hs` — positive flow under `IO`, incl. two nested
  instances proving counter independence.
- `app-sim/Main.hs` — positive flow under `runSim`: two instances in one
  simulation + virtual-thread rendezvous (tags nest, determinism kept).
- `app-backstop/Main.hs` — runtime refusal + restoration checks (B1–B6).
- `neg/` — negative compile tests (N1–N6).

## Notes

- The real tree's HLS config may flag these files (no component owns
  them); cosmetic, branch-local. `cabal build` at the repo root ignores
  this directory (own `cabal.project`, not in any root `hs-source-dirs`).
- `Text` ids stay bare on purpose: names must escape scopes; only the
  capabilities interpreting them are branded (the `DbEff e` vs `DbHandle`
  split from the Bluefin research).
- `placeAwait` does not degrade inside steps here; the real `awaitChild`
  degrades to unrecorded via the leaf rule — out of focus for this
  prototype, which tests allocation refusal.
