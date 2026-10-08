# Invariant gates: how a new invariant is validated

Applies to every slice of the scoped-capabilities epic (plan Phase 10 /
Rule 9, `TODO.md`). The rule: an invariant claim is proven twice — once
by a gate that would fail if the invariant were violated, and once by a
control that would fail if the gate were vacuous. Claims without both
halves are unproven, not "probably fine".

## 1. The four gates per invariant

| # | Gate | Passes when | Catches |
|---|---|---|---|
| G1 | **Positive path** | the legal use compiles and runs (live + sim where the domain has both) | over-refusal: the invariant rejects legal code |
| G2 | **Compile-time negative** | a must-fail build is rejected, matched to the *expected error class* (grep on `Couldn't match`/`rigid`/`No instance`), not merely "some error" | type-level holes (misuse that compiles) |
| G3 | **Witness control** (paired with G2) | a must-pass twin that is the negative with the one illegal token swapped for the legal one — **it must build clean** | vacuous negatives: a broken negative fails for reasons unrelated to the invariant |
| G4 | **Runtime negative** | the misuse that types cannot reach is refused with the exact ADT shape (`InsideStep "…"`, `WrongInstance "…"`) **and** leaves no trace: counter unmoved, no rows, no trace events | capture defeats: the residual runtime half |

Live-DB invariants add the standing gates on top: `make db-migrate`,
psql row verification against `$DBOS_DATABASE_URL`, read-only oracle
spot-check. Durable-state assertions are part of G4's "no trace" half.

## 2. Why the witness control is mandatory (G3)

A must-fail test can pass for the wrong reason: a typo, a missing import,
a stale module path. The witness is the negative file with exactly one
token changed — e.g. `opFetchPrice (wfOps wctx) wctx` → `opFetchPrice
(stepOps s) s`, `startChild step ref` → `startChild ctx ref`,
`placeOrder ioWctx` inside a sim → `placeOrder simWctx`. If the witness
does not build clean, the negative is suspect and the gate is red until
it is fixed.

Status: the three protos ship the grep half plus witness twins (`W_*.hs`
per negative, built first, must succeed). The real tree carried the same
practice as `-fno-code` probes until their removal (2026-10-06); the
policy below stands as the record of how they ran.

## 3. Showing old runtime negative vs. new compile-time check

The migration question is never "does it still fail?" but **"which class
of failure, and did the other class regress?"** The display is a table
per invariant with three rows: what the negative looks like, what
happens today, and what the gate asserts.

| Invariant | Old runtime negative (pre-design) | New verdict | Evidence |
|---|---|---|---|
| Start/enqueue a step | no step refs existed; a workflow ref could not name one | **compile error** (no term exists) | type-level by construction; no test needed — recorded, not gated |
| Start a child with the narrowed view | `InsideStep` at run time (worked, with an id risk) | **compile error** for the direct shape | proto negative + witness (`neg-n2-step-start` + twin) |
| Start a child through a captured parent | ran, child started with id `…-0` | **runtime `InsideStep`**, refused before any write | live+sim `scenarioCaptureChildRefused`, counter unmoved |
| Ops invoked at workflow scope | n/a (did not exist) | **compile error** (`StepCtx` vs `WorkflowCtx` mismatch) | proto negative + witness (`neg-op-in-workflow` + twin) |
| IO step table inside IOSim | n/a | **compile error** (`IO` vs `IOSim s`) | proto negative + witness (`neg-step-cross-stack` + twin) |
| Next id from a step view | allocator reachable | **compile error** (no allocator on `StepCtx`) | proto negative + witness (`neg-n4-nextid-on-step` + twin) |
| Cross-execution context/record use | silently ran | **compile error** (`exec1` vs `exec`) | proto negative + witness (`neg-cross-exec`/`n5`/`n6`/`n7` + twins) |
| Send/recv/getEvent/transaction in-step | silently proceeded (captured-parent shape) | **runtime** degrade/refuse with `InsideStep` | live+sim captured-parent cases, counter unmoved (slices 4–5) |
| Blank workflow id | accepted (empty `Text`) | **runtime** `Maybe` at `mkWorkflowId` (hiding evaluated, rejected as 45-site churn) | `TypesTest` `WorkflowId` group |

Reading rule: an invariant that *stays* runtime keeps its G4 gate and
says why types cannot reach it (capture, dynamic scope, database rows).
An invariant that *moves* to compile-time keeps a G2+G3 gate; the old
runtime test is either deleted (now unreachable by construction — say so
in the commit) or demoted to a witness that proves the legal shape still
works. Deleting a runtime negative without a compile-time replacement is
a hole, not a cleanup.

## 4. Per-slice checklist (copy into each slice)

1. [ ] G1 positive: legal use green on live and sim (or "live only"
       with the reason, per the dual-stack convention).
2. [ ] G2 negative: must-fail build, error-class matched.
3. [ ] G3 witness: must-pass twin of G2, builds clean.
4. [ ] G4 runtime (if the invariant has a runtime half): exact refusal
       ADT + no-trace assertions (counter, rows, trace events).
5. [ ] If an old runtime test moved class: record delete-or-demote in
       the commit message; no silent disappearance.
6. [ ] Standing gates: `make db-migrate`, watcher pair, idle `cabal
       test`, psql mirror, oracle spot-check.

## 5. Where this is enforced

- Protos: `proto/*/run.sh` gains witness builds (fail the run if a
  witness does not compile) — the probes become the executable spec of
  this policy.
- Tree slices: negative compile tests live beside the probe or as
  `-fno-code` build probes where the invariant touches the real tree
  (e.g. StepCtx-keyed ops post-rewire); witness twins accompany each.
  Landed-then-removed 2026-10-06 with `probes/` (had `neg-*`/`w-*` pairs
  for child start, allocation, nested steps, and cross-exec races).
- Runtime halves: the existing live+sim trees; no-trace assertions are
  strengthened wherever a negative currently asserts only the refusal
  shape.
- Regression corpus (ADR-0029): `negative/neg_*.hs` must fail with the
  expected error class, `negative/w_*.hs` must build clean; `make neg`
  runs both halves and blocks `/review` on red.
  strengthened wherever a negative currently asserts only the refusal
  shape.
