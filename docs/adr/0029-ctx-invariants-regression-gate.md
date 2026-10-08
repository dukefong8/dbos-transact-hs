# ADR-0029: Step-context invariants and their negative-compile regression gate

Date: 2026-10-07. Status: accepted.

## Context

Four ad-hoc proof batches established the workflow/step separation as
type-level fact, each verified with `ghc -fno-code` against the built tree:

1. All 29 `WorkflowCtx`-taking facade entries reject a `StepCtx`
   (steps, sleep, select, await, start, transactions, events, messages,
   waits, all seven `*InWorkflow` management entries, `workflowId`).
2. No call/start/enqueue from steps through any ctx entry (inline body call,
   `startChildWorkflow`, queued `startChildWorkflow`); the `DBOS`/`Executor`
   outside forms compile by design (accepted limitation, R1 of
   `docs/start-enqueue-escape-todo.md`).
3. Nesting stays `StepCtx`-threaded: no fresh recorded step and no start is
   reachable from a step view, at any depth; `runNestedStep` is the single
   sanctioned `StepCtx` entry.
4. Confinement and counters: `stepCtxWorkflow` is not facade-exported (app
   code cannot reach a parent context at all), and the id counter, markers,
   placement, depth reads, scope opening, and the system-DB scope
   (`nextStepId`, `nextWorkflowMarker`, `placeCall`, `insideAStep`,
   `withStep`, `withSystemDB`) are `WorkflowCtx`-only.

In the same sweep `PendingStep` became a facade export, type-only
(constructors stay in the internal `Checkpoint` module), matching Rust's
`pub struct PendingStep` (`checkpoint.rs:136`) — the engine already handed
the type out from five exported functions without letting callers name it.

Policy is `docs/invariant-gates.md` (G1 positive, G2 compile-time negative
with error-class match, G3 witness twin, G4 runtime negative). The probes-era
`neg-*`/`w-*` practice was removed with `probes/` on 2026-10-06; this ADR
re-establishes it as a Make-driven corpus instead of a directory the build
sees.

## Decision

The invariants below hold, each with a negative, a witness, and (where a
runtime half exists) the tree cases that pin it. The corpus lives in
`negative/`: `neg_*.hs` must FAIL with the stated error class, `w_*.hs` must
BUILD clean. The directory sits outside every cabal stanza and outside the
ghciwatch globs, so no corpus file ever enters a build or a reload.

| # | Invariant | Class | Negative (must fail) | Witness (must build) | Runtime half |
|---|---|---|---|---|---|
| I1 | Every `WorkflowCtx` entry rejects a `StepCtx` (29 sites) | compile (`Couldn't match … WorkflowCtx … StepCtx`) | `neg_ctx_pathways.hs` | `w_ctx_pathways.hs` | slice 2–5 refusal cases, live+sim |
| I2 | No call/start/enqueue from steps via ctx entries (3 sites) | compile (same class) | `neg_call_start_enqueue.hs` | `w_call_start_enqueue.hs` (outside forms compile — the R1 boundary) | `scenarioCaptureChildRefused`, `scenarioEnqueuedChildReplays` |
| I3 | Nesting cannot reach a recording entry (3 sites) | compile (same class) | `neg_nesting.hs` | `w_nesting.hs` (two-level `StepCtx` threading) | `scenarioNestedPlain`, `scenarioNestedStepView` |
| I4 | `stepCtxWorkflow` stays out of the facade | compile (`does not export`) | `neg_facade_boundary.hs` | `w_facade_boundary.hs` | n/a (unreachable by construction) |
| I5 | Counter/placement/scope machinery is `WorkflowCtx`-only (6 sites) | compile (same class) | `neg_counter.hs` | `w_counter.hs` | `scenarioChildIdsInBuildOrder`, `scenarioStepIdPairs` (order pins) |
| I6 | `PendingStep` exported abstractly | construction | — (facade diff) | the corpus itself names the type through the facade | n/a |

Gate: `make neg` (Makefile) typechecks every corpus file with
`ghc -fno-code` via `cabal exec`: negatives must fail matching
`Couldn't match|does not export`, witnesses must exit 0. Either failure is
red and blocks `/review`.

Future-change rules:

- A new `WorkflowCtx`-taking facade entry extends `neg_ctx_pathways.hs` and
  `w_ctx_pathways.hs` in the same slice; a new `StepCtx`-taking entry is a
  design change and needs its own ADR note justifying why it cannot record.
- Removing a facade export that the corpus names (or hiding `PendingStep`
  again) must update the corpus first — the gate fails otherwise.
- A new invariant follows `docs/invariant-gates.md` §4 with a `neg_`/`w_`
  pair from day one; runtime halves keep their G4 tree cases.

## Consequences

- Corpus: ten files under `negative/`, plus the `neg` Make target. No CI
  exists in this repo; the gate runs locally and joins the pre-`/review`
  set (`make neg`, idle `cabal test all`, oracle spot-check).
- `make neg` depends on `build` (the `cabal exec` environment needs built
  package DBs) and needs no database (no codegen, no rows).
- If a negative starts failing for the wrong reason (e.g. an ambiguity the
  mismatch used to mask), the witness goes red first per the G3 rule — fix
  the witness's annotations, not the invariant.
