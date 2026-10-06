# ADR-0025: ImplicitParams ambient context as sanctioned opt-in for user bodies

Date: 2026-10-06. Status: accepted.

## Context

User-written workflow and step bodies receive `WorkflowCtx`/`StepCtx`
explicitly and repeat it at every seam call (`runStep wctx …`, `setEvent
wctx …`). Three ambient mechanisms were evaluated
(`docs/monadreader-seam-research.md`, with compile probes):

- mtl `MonadReader (WorkflowCtx exec m)`: unimplementable — fundep
  `m -> r` rejects per-execution envs over one monad (probe fails with
  `Functional dependencies conflict`).
- Per-execution `ReaderT`: compiles and simulates, but wraps explicitly
  at every run — explicit threading with extra steps. A runnable
  Starter-shaped prototype ran green; net ergonomic gain ≈ one repeated
  parameter per helper.
- `ImplicitParams` (`?wctx`): no instances, no fundep, brand preserved
  (same-brand use compiles, cross-brand use fails with `Couldn't match
  type 'BrandB' with 'BrandA'`), identical under IOSim by construction.

## Decision

Explicit context remains the default everywhere, matching the oracle's
shape. `ImplicitParams` is the sanctioned opt-in for *user bodies only*,
under four rules:

1. Boundaries stay explicit. Registration, launch, start, and await
   signatures are unchanged; a body binds `let ?wctx = wctx` from its
   explicitly-received context at the top and never anywhere else.
2. One binding site per body. Nested scopes re-bind explicitly rather
   than shadowing silently.
3. No stored constrained actions. A value capturing a `?wctx` constraint
   must not cross a rank-2 boundary (registry bodies, queued
   continuations) — run it inside the binding scope.
4. No engine or export changes. Seams keep their signatures; no blessed
   `*R` helpers enter `DBOS.Transact`. Lifted call shapes live in user
   code (demo apps first).

## Consequences

- The Starter rewrite follows `docs/implicit-wctx-ambient-plan.md`;
  the widget store follows only after the starter proves out.
- Revisit triggers (any one): measured demand across several bodies for
  shared lifted helpers (promote to demo-common, never the facade);
  evidence that shadowing/deferred errors cost more than the parameter
  they replace; an oracle-side ambient precedent to mirror.
- Independent of the ST-substance engine track: branding `WorkflowState`,
  `StepScope`, and handed-out cells changes what the `?wctx` constraint
  *carries*, never whether bodies may use it.
