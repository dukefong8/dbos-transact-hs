# Deterministic workflows via record-of-functions steps

THROWAWAY prototype (branch `proto/phantom-brands` only). Run: `sh run.sh`
(12 checks: IO run, sim run, IO/Sim agreement, replay determinism ×2
interpreters, 3 must-fail compile tests).

## Claim

Non-determinism lives in steps behind one `Ops` record (operations);
workflows are pure orchestration over recorded step results. The same
workflow source runs under IO and under IOSim by swapping only the
installed record — and the types make the skill's Incorrect shapes
unwritable.

## Incorrect vs Correct (skill mapping)

| Skill rule | Incorrect (here: compile error) | Correct (here) |
|---|---|---|
| External call in workflow (`step-basics.md`) | `opFetchPrice (wfOps wctx) wctx "x"` — op demands `StepCtx`, workflow holds `WorkflowCtx` (`neg-op-in-workflow`) | call inside `withStep` via `stepOps s` |
| `Math.random`/`Date.now` in body (`workflow-determinism.md`) | ambient `newUnique`/`getCurrentTime` in a shared body — the sim instantiation fails (`neg-raw-io`) | `opRandomCents`/`opNow` in a step; journal replays recorded values |
| Branching on non-determinism | — | `orderWorkflow` branches on recorded `total`; replay takes the same lane with the handler uncalled (`REPLAY-MATCH True`, `REPLAY-CALLS n=4`) |
| Cross-run contamination | mixing execution A's record with execution B's scope (`neg-cross-exec`) | rank-2 `exec` on scopes and record alike |

Deliberate non-goal found while probing: merely *mentioning* one
execution's values inside another scope compiles when nothing branded
crosses (result is plain `m a`). Brands track executions; the runtime
depth counter tracks dynamic step scope. Both layers stay.

## Operations vs handlers (Bluefin mapping)

The shape is Bluefin's `DB.hs` handle pattern with `m` instead of `Eff`:

| Bluefin | Here |
|---|---|
| handle (`DBHandle e`) | `StepCtx exec m` — the capability each call demands |
| operation (`dbQuery h …`) | op selector (`opFetchPrice ops s …`) |
| handler binding (interpretation) | record construction (`ioOps` / `simOps` / counting wrappers) |
| interpreter swap | installing a different record per `withWorkflow` call |

Records, not a typeclass: two handlers coexist in one process (live +
counting, prod + canned), which classes make second-class. This matches
the repo convention (records-of-functions for fakes; no new ambient
machinery in internals).

## Carrier decision: the contexts carry the record

`WorkflowCtx` is the intake (the runner installs the app's
execution-independent `forall exec. Ops exec m` at construction);
`StepCtx` is the use point (bodies read `stepOps`). Considered and
rejected:

- **Explicit parameter everywhere**: works, but every internal step call
  site grows an argument (the engine already threads ctx everywhere —
  carrying rides for free).
- **Ambient (constraint/reader)**: new ambient machinery against the
  repo guardrails, and untestable-by-construction loses to first-class
  records.
- **`Connection`-carried**: not exec-branded; ops are environment,
  installed per execution.

Belt and suspenders, deliberately redundant: the carrier answers *how
bodies obtain ops*; the per-call `StepCtx` parameter answers *where
they may invoke*. A record without StepCtx-keying could be invoked from
workflow scope by anyone holding it (`workflowOps` exists precisely so
helpers can thread it into steps they run — holding is fine, spending
is scoped).

## What types enforce vs what stays discipline

Enforced: ops only in steps (per-call `StepCtx`), sim-runnable bodies
off ambient IO (no `MonadIO` under `IOSim` — instantiation fails),
cross-execution use (brand threads through the record), replay
stability (journal hit-before-run; `explodingOps` proves zero handler
calls on the second run).

Not enforced: a body at concrete `IO` can still use ambient effects —
nothing forbids `IO`. The teeth are social plus structural: bodies
shared with the sim *must* stay polymorphic, and the sim is where every
step behavior is pinned (dual-stack convention). Same position as the
Rust oracle.

## Integration path (engine rewire)

1. `WorkflowCtx` gains the installed record field; `withWorkflow` and
   run paths take the app's `forall exec. Ops exec m` (from the DBOS
   handle, beside datasources).
2. `withStep` moves journal + record into `StepCtx`; op results keep the
   engine's `FromJSON`/`ToJSON` boundary (this probe's `Dynamic`
   journal stands in for `SerializedWorkflowValue` rows).
3. Retry/timeout/deadline behavior stays in `StepOptions` + `raceCancel`
   — orthogonal to who provides the effects.
4. Step bodies become `Ops`-plus-`StepCtx` readers; registry bodies stay
   `WorkflowCtx`-scoped and can thread the record into step-running
   helpers, never invoke it.

Open: per-step op subsetting (a step declaring which ops it needs —
narrower records per domain); op-level idempotency keys reusing the
step id; where the IO handler's clients (http manager, fs root) are
configured (launch config vs datasource-style registration).
