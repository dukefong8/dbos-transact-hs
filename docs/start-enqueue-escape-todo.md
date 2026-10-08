# In-workflow start/enqueue and the captured-instance escape — TODO plan

Date: 2026-10-07.
Oracle: Rust `crates/dbos/src/workflow.rs` (one `PendingStep` start/enqueue,
step name = child's bare name, `operation_outputs.child_workflow_id` at the
parent's step), `management.rs:375-908`; TS rule
`workflow-constraints.md` ("do not call, start, or enqueue workflows from
within steps").
Sources re-read: `docs/agent-skill-api-review.md:34-60` (the recorded
"engine rewire"), `src/DBOS/Internal/DBOS/Transact/Workflow.hs:633-745`,
`Instance.hs:222-254`, `Checkpoint.hs:115-126`.

Status: **re-evaluation complete — plan is docs + verification, not a
rewire.** The natural-shape work the recorded fix asked for is already in
the tree; the residual escape cannot be closed by that fix.

## 1. Recorded claim vs. tree facts

| Recorded in the API review (2026-10-03) | Tree today | Verdict |
|---|---|---|
| "`enqueueWorkflow` / `startWorkflowRef` via `currentConnection` or captured `DBOS`/`Client` — HOLE: reachable from a step body with no guard" | Natural ctx path: `startChildWorkflow :: WorkflowCtx exec m -> …` (`Workflow.hs:633`) — a step body holds only `StepCtx`, so it is a type error. Captured parent ctx while a step runs → `insideAStep` depth refusal (`Workflow.hs:640-642`). Captured `DBOS`/`Executor`/`Client` (the outside forms, `Instance.hs:222-254`) → still unguarded. `currentConnection` no longer exists in `src` — only this doc names it. | **Open only on the instance surface.** The row's fix premise is stale. |
| "Fix: engine rewire — enqueue/start signatures take `WorkflowCtx exec m` (refuse when depth > 0, covering captured parents) and no overload takes `StepCtx`" | The ctx routes already take `WorkflowCtx`; the outside forms must stay for handlers, clients, and tests, and cannot be depth-checked: depth lives in `WorkflowState`, reachable only through a ctx. | **Recorded fix cannot close the hole it targets.** Converting the outside forms would delete the outside API; keeping them keeps the escape. |
| "calling enqueue from a step body then becomes a compile error" | True for the ctx form; false for captured `DBOS`/`Executor`/`Client`. | **Overclaim** — narrow to the ctx-routed surface. |
| (Earlier draft claim: in-workflow enqueue is missing) | `startChildWorkflow` + `options.startQueue` is the durable in-workflow enqueue: validates the `Enqueue` shape, builds the child row on the queue (dedup/priority/partition/delay), records the parent step under the child's bare name (`Workflow.hs:655-745`), links `child_workflow_id`. Covered by `scenarioJoinHeldKey` (`WorkflowCases.hs:2052-2095`). | **Not a gap.** No `enqueueWorkflowInWorkflow` needed; that would be an alias. |

Root cause of the residual: explicitness. TS/Rust can refuse a start from a
step body anywhere because their context is ambient (`AsyncLocalStorage` /
`tokio::task_local!`); Haskell's depth is per-execution state behind the
ctx, so a captured instance value carries no way to ask "am I inside a
step?".

## 2. What remains

- **G1 — residual escape (decision, not a port).** A step body (or any
  code) holding a captured `DBOS`/`Executor`/`Client` can start/enqueue
  without a checkpoint or child link, and a retried step body repeats it.
  Options:
  - (a) accept and record: the sanctioned ctx path exists; the instance
    forms are outside-only by discipline. One paragraph in the review doc
    + skill; no test change.
  - (b) revive ADR-0026 (canonical ambient context): with an ambient
    execution, the outside forms could consult `insideAStep` at runtime,
    closing the runtime half. Multi-phase signature migration; stopped
    once already (`docs/implicit-wctx-ambient-plan.md`).
  - (c) narrow what bodies can capture: not expressible; any value can be
    closed over.
- **G2 — doc drift.** `docs/agent-skill-api-review.md:48` and the
  "engine rewire" paragraph (lines 52-60) state a fix that is already
  satisfied on the ctx side and impossible on the instance side.
- **G3 — skill guidance.** `references/workflow-constraints.md` should
  show the sanctioned in-workflow start/enqueue
  (`startChildWorkflow … {startQueue = …}`) and mark the
  `DBOS`/`Executor` forms outside-only.
- **G4 — coverage confirmation.** Verify the sanctioned path asserts the
  recorded step row and `child_workflow_id` on disk (live psql), and that
  a parent replay reads the recorded start back rather than enqueuing
  again.

## 3. TODO

- [x] **V1 — replay + row evidence for the sanctioned enqueue** (TDD,
      framed): extend `scenarioJoinHeldKey` (or add a sibling) to re-run
      the parent under the same id after the first settle and assert the
      child is not created twice; assert the parent's step row name =
      child bare name and `child_workflow_id` set (both stacks). psql:
      `operation_outputs` row + `workflows` rows for parent/child.
      Gates: watcher Workflow pair, `cabal test … $2 == "Workflow"` (or the
      suite's group), `cargo test -p dbos --test children` read-only.
      Done 2026-10-07: `scenarioEnqueuedChildReplays` green on both stacks
      (Workflow pair Sim 55 + Live 55); engine fix `WorkflowEnqueued` on the
      ctx-threaded enqueue (`Workflow.hs`, matching Rust `workflow.rs`
      debug); psql shows parent SUCCESS + child ENQUEUED with one
      `child`-named step row carrying `child_workflow_id` after replay;
      `cabal test all` 703 green; oracle `children` suite 26 green.
- [x] **V2 — capture-refusal pin** (confirm-by-reference): if no test
      asserts `startChildWorkflow` through a captured parent inside a step
      body returns `InsideStep`, add one framed case; otherwise cite the
      slice-2 case and close this item.
      Done 2026-10-07 by citation, no new test: `scenarioCaptureChildRefused`
      + `checkCaptureChildRefused` (`WorkflowCases.hs:1311/2645`) assert
      `Left (InsideStep "starting a workflow")` with no recorded steps,
      green in both trees.
- [x] **D1 — rewrite the review row + section**: state the two parts —
      (i) natural shape compile-blocked, ctx-capture depth-refused;
      (ii) captured instance values remain an accepted/open limitation,
      with the reason (no ambient depth) and the closure options (ADR-0026
      or nothing). Drop the stale `currentConnection` mention.
- [x] **D2 — skill refs**: `workflow-constraints.md` gains the sanctioned
      in-workflow enqueue form and the outside-only note;
      `queue-basics.md` cross-references it if it shows `enqueueWorkflow`.
- [x] **D3 — audit artifact**: the `*InWorkflow` notes become
      "explicit-ctx rendering of Rust's single `PendingStep` entry (same
      wire name)"; add the enqueue path to the `enqueueWorkflow` row note.
- [x] **R1 — disposition decision (G1)**: pick (a) accept-and-record or
      (b) ADR-0026 revival. If (a): one dated accepted-limitation note in
      the review doc, no pin test (the port's policy is demote, never
      silent — a doc note is the demotion). If (b): open the ambient plan
      silent — a doc note is the demotion). If (b): open the ambient plan
      as its own tracker, not this one.
      Done 2026-10-07: (a) accept-and-record. Dated note in the review doc
      (§2 Decision R1); ADR-0026 status closed; ambient plan marked do-not-resume.

## 4. Out of scope

- No `enqueueWorkflowInWorkflow` alias — `startChildWorkflow` + `startQueue`
  is the oracle shape (one start entry with queue options).
- No change to the outside `DBOS`/`Executor`/`Client` signatures for
  outside callers.
- No ambient migration here; that is ADR-0026's own plan if revived.
