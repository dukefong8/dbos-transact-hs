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

## Run

```sh
./run.sh
```

- builds the model + the two positive flows, runs them;
- typechecks each `neg/N*.hs` expecting **failure** (the enforcement);
- typechecks `neg/Residual_Capture.hs` expecting **success** (the
  documented residual hole: a step body capturing the *parent* context
  still compiles — closures capture; the type split only rejects the
  natural shape that uses the handed `SCtx`).

## Layout

- `src/Scope/Model.hs` — the analogues (private constructors, explicit
  exports = the privacy boundary).
- `app-io/Main.hs` — positive flow under `IO`, incl. two nested
  instances proving counter independence.
- `app-sim/Main.hs` — positive flow under `runSim`: two instances in one
  simulation + virtual-thread rendezvous (tags nest, determinism kept).
- `neg/` — negative compile tests + the residual-hole exhibit.

## Notes

- The real tree's HLS config may flag these files (no component owns
  them); cosmetic, branch-local. `cabal build` at the repo root ignores
  this directory (not in any `hs-source-dirs`).
- `Text` ids stay bare on purpose: names must escape scopes; only the
  capabilities interpreting them are branded (the `DbEff e` vs `DbHandle`
  split from the Bluefin research).
