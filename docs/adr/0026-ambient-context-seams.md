# ADR-0026: Canonical ambient context (`ImplicitParams`) on user-facing seams

Date: 2026-10-06. Status: accepted. Supersedes the opt-in rule of
ADR-0025 (explicit default stays for non-user-facing plumbing).

## Context

ADR-0025 sanctioned `ImplicitParams` as a user-space opt-in: bodies bind
`let ?wctx = wctx` themselves. That leaves every seam signature explicit
and every body doing its own binding — the repetition moves but does not
disappear. Probed facts (`docs/monadreader-seam-research.md`):

- mtl `MonadReader` over a shared monad cannot serve per-execution envs
  (fundep conflict, compile-probed).
- Name+type-keyed implicit params have no fundep, preserve the `exec`
  brand exactly (cross-brand use fails), and behave identically under
  IOSim (purely constraint-based).
- 14 `src` modules take `WorkflowCtx exec m` / `StepCtx exec m`
  parameters on user-facing seams.

## Decision

The engine binds and reads; user code carries only constraints:

1. Every user-facing `WorkflowCtx exec m ->` / `StepCtx exec m ->`
   parameter becomes a `(?wctx :: …)` / `(?sctx :: …)` constraint.
   Registration bodies become
   `forall exec. a -> ((?wctx :: WorkflowCtx exec m) => m (Either e r))`;
   step bodies likewise with `?sctx`.
2. Binding sites live in `dbos` code only: `executeRegisteredWorkflow`
   binds `?wctx` around each body run; `runStep`/`runStepWith`/
   `pendingStep`/`withStep` bind `?sctx` around each step body. User
   bodies never write a binding.
3. Engine-internal plumbing that never runs user code (supervisor,
   workers, backend calls, launch/shutdown, client surface,
   `DBOS m`/`Connection m` threading) keeps explicit parameters.
4. `ErasedWorkflow`'s rank-2 field carries the constraint; each run
   instantiates it under its own `withWorkflow`-minted brand.
5. This changes exported signatures (not just the export list): a
   minor-version-scale API change, authorized here, migrated
   mechanically per the rollout plan.

## Consequences

- One binding per site; nested scopes re-bind explicitly with a comment.
  Shadowing review becomes part of the slice checklist.
- New probe pair (`neg-ambient-cross-exec` + witness): a `BrandB`-bound
  `?wctx` used where `BrandA` is demanded must fail; same-brand use
  builds clean. `run.sh` + `docs/invariant-gates.md` §5 grow two lines.
- Rollout in `docs/implicit-wctx-ambient-plan.md` (v2 below); gates are
  the S8/S9 set.
- Revisit trigger: evidence that deferred higher-order errors cost more
  than the parameters they replace — measured, not argued.
