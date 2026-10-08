# Plan: canonical ambient context on user-facing seams (ADR-0026)

> STOPPED 2026-10-06 after Phase-1 Context/Checkpoint edits: all code
> reverted to `348fe64`, ADR-0026 kept. Resume here if revived.
> Closed 2026-10-07 (`docs/start-enqueue-escape-todo.md` R1): accept-and-record; do not resume.
> reverted to `348fe64`, ADR-0026 kept. Resume here if revived.

Supersedes the opt-in rollout: the seams themselves change, so every
consumer migrates. Goal: user workflow/step bodies carry only
`?wctx`/`?sctx` constraints; the engine binds at each run, reads at
each seam. Boundaries that never took a ctx (`DBOS m`, `Connection m`,
handles, ids, outputs) are untouched.

Non-goals: behavior change of any kind (same calls, same order, same
rows/traces); duration or protocol changes; new exports beyond
signature evolution; Mem/sim backend changes (constraints behave
identically there).

## Phase 0 — storage spike (done 2026-10-06)

Proved with plain `ghc`, no deps (`Spike.hs`, scratch-only): an
existential box holding `forall exec. ((?ctx :: Ctx exec) => IO Int)`,
run through a rank-2 binder supplying value and rigid brand together,
compiles with nothing but one `let`. No redesign needed.

Prove rank-2 + constraint storage compiles before touching the tree
(scratch, plain `ghc`, no deps): an existential box holding
`forall exec. ((?ctx :: Ctx exec) => IO Int)`, run through a rank-2
`withCtx`-shaped binder that supplies value and rigid brand together.
If the unpacking needs annotations past one `let`, the `ErasedWorkflow`
shape must be redesigned first — stop and re-scope.

## Phase 1 — engine seams (14 modules, bottom-up)

Order matters (callers adapt to callees): Context readers
(`workflowId`, `deadline`, `stepId`, `stepStatus`, …) → Step family
(`runStep`, `runStepWith`, `runNestedStep`, `pendingStep`, `withStep`)
→ Sleep/Event/Message → Management in-workflow variants → Datasource
Tx → Select/Checkpoint/Wait/Handle → Registry/Workflow run paths
(the two binding sites). Supervisor/worker/backend/launch/client paths
keep explicit parameters.

Each module: change signatures, bind `?wctx`/`?sctx` exactly where a
user body is invoked, compile against the existing callers before
moving on (watcher pair on an untouched suite stays green throughout).

## Phase 2 — test tree

`*Cases.hs` scenarios drop ctx parameters for constraints; fixtures
keep building views explicitly (`withWorkflow`/`newWorkflowCtx` are
scope creators, not seams). Dual-stack pairs re-verify per converted
slice with flip guards as usual. No check changes expected — same calls,
same values.

## Phase 3 — demo apps

Starter bodies, then widget, per the (unchanged) per-body verification:
build, boot, drive, psql-compare outputs and step/event rows.
Durations and CodePanel needles untouched.

## Phase 4 — gates

Full gate set: `cabal test all`, `make db-migrate`, psql mirror, Rust
suites, `/review`. (The planned ambient-cross-exec probe pair died with
`probes/`; brand-confusion coverage, if wanted, belongs in the dual-stack
trees as refusal-shaped cases.)

## Risks and mitigations

- Shadowing (`?wctx` re-bound invisibly): single binder per engine
  site; user bodies must not bind (review rule, probe-backed).
- Deferred errors at stored constrained actions: Phase 0 spike retires
  this before any migration.
- `MUST RULE` tension (export-signature churn): authorized by ADR-0026
  as a version-scale API change; mechanical, no semantic drift by
  construction (same underlying calls).
- Scope creep into plumbing: the ADR's rule 3 is the fence — if a
  function never runs user code, its parameters stay explicit.
