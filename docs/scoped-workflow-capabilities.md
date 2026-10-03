# Scoped workflow capabilities: phantom brands — research + prototype

**Status (2026-10-03, descoped).** Validated on a throwaway prototype
(`proto/scope-brands/`, branch `proto/phantom-brands`): 13/13 checks green —
valid flows run under `IO` and `IOSim`, four misuse shapes are compile
errors, cross-instance refusal is asserted at runtime (oracle parity),
inference needs no annotations beyond one pinned helper. Since the last
revision: `inst` dropped (the DBOS object is singleton-like per process);
what remains is `exec` branding, the context split, and the depth backstop.
This document consolidates the four research rounds behind it and records
what the prototype proved, what it refuted, and what remains open.

**Relationship to prior art.** This direction is *decided*, not proposed:
`docs/ownership-lifetimes-isomorphism.md` §11 records phantom brands
(`Ctx s m`, `WorkflowRef s m`, `WorkflowHandle s m`, rank-2 binders at the
seam, explicitly not-Bluefin-the-dependency), following the §9 addendum
(ADR-0012 removed Bluefin/`WorkflowM`; ambient escape is closed by
construction) and ADR-0018/0019 (instance identity, typed errors). What is
new here: (1) executable evidence the §11 shape typechecks, infers, and
composes with `IOSim`; (2) a tested extension beyond §11 — the
`WorkflowCtx`/`StepCtx` split — with a residual hole documented, which
forces a decision §11 left open (see §7).

## 1. Bluefin: patterns in, dependency out

Primary sources: `~/dev/Bluefin` `bluefin-examples/.../MonadError.hs`
(`DslBuilderEff` bridge), `.../DB.hs` (dynamic-effect record `DbEff`,
`useImplIn`-delimited scope), `bluefin/src/Bluefin/Compound.hs`
(single-`e` compound handles, `mapHandle`), `.../Stream/InsideAndOut.hs`
(`useImplWithin` narrowing). Full survey in
`docs/bluefin-research-context.md`.

Mechanisms that transfer, dependency-free:

- **Rank-2 scope discipline.** `runX :: (forall e. H e -> Eff (e :& es) r) -> …`
  becomes `withWorkflow :: DBOS m -> Text -> (forall exec. WorkflowCtx exec m -> m a) -> m a`.
  The ST-region trick; no `Eff` required.
  becomes `withDBOS :: … -> (forall inst. DBOS inst m -> m a) -> m a`.
  The ST-region trick; no `Eff` required.
- **Scoped capabilities vs unscoped names.** `DB.hs` scopes `DbEff e`
  while `DbHandle` (a `String` newtype) travels bare. Same split:
  `Connection`/`Ref`/`Handle`/`Ctx` branded; `Text` ids bare (durable
  names must escape every scope by design — dequeue claims, crash
  recovery, cross-SDK rows).
- **Narrowing.** `useImplWithin` becomes the `WorkflowCtx` → `StepCtx` handoff
  in `runStep`-equivalents: the step body receives the narrowed view.
- **`DslBuilderEff` as the only sanctioned Bluefin touch:** prototyping an
  `Eff` flavor at the outer edge without rewriting internals. Not used;
  kept as the escape hatch if ergonomics ever need testing.

What does *not* transfer (weighted in §4 of the research): `Eff` itself
(`Env -> IO a` — forces `m ~ IO`, kills the IOSim dual stack),
`State`/`Modify` (the step counter is 3 lines of STM; Bluefin state loses
atomic composition, `retry`-blocking, and virtualization),
`Ask`/`Throw`/streams (same arity or speculative, all requiring `Eff`).

## 2. Rust gaps the prototype closes (R-matrix refs)

Against `crates/dbos/src` (`connection.rs`, `context.rs`, `handle.rs`,
`registry.rs`, `checkpoint.rs`, `instance.rs`), full matrix in the
parent doc §10:

- **R9 cross-instance** (`Arc::ptr_eq` + `Owner` downgrade, runtime):
  stays runtime, by decision — the DBOS object is singleton-like per
  process, so the `inst` brand did not earn its CPS keep. The prototype
  asserts the refusal instead (B0: ref from A started under B →
  `WrongInstance`, before anything allocates, ADR-0018 order). The
  `Owner`-flag design stays untouched.
- **R9/checkHere cross-execution** (ambient `Ctx::current()`, runtime):
  closed at compile time. N5 drives a `Pending` built in execution 1
  from execution 2 → `Couldn't match type ‘exec1’ with ‘exec’`. The
  prototype also carries the runtime token backstop in `drive`
  (execution counter per instance), mirroring per-poll placement checks
  for smuggled values.
- **R3 call ownership, partial.** `PendingStep<'a>`'s borrow becomes the
  `exec` parameter (stronger: no moves-within-lifetime); `#[must_use]`
  and `result(self)` have no plain-Haskell counterparts (needs
  `LinearTypes`; double-await is benign — the row is the truth).
- **R1/R5/R8, not addressed.** `P`/`R` erasure stays (cross-SDK rows force
  runtime decode regardless); must-use stays discipline + id-density
  tests, per the parent doc §3.
- **Rust's documented `Arc`-cycle leak** (`register_workflow` docs) dissolves
  under GC; the remaining discipline (use the given ctx) is convention in
  both languages, with §3's split shrinking its surface.

## 3. IOSim / io-classes interaction

- **One tag plus the sim tag.** io-sim's `s` (run scope) already flows
  through `m` (`MemSystemDB s`, `StrictTVar (IOSim s)`); cross-*run*
  confusion is ill-typed today. `exec` adds cross-*execution*-within-a-run.
  N6 proves they nest: a handle carrying both tags escapes neither
  `runSim`'s nor `withWorkflow`'s `forall`.
- **Zero backend churn.** `SystemDB` class, Postgres sessions, both sim
  fakes, the existential, `runSystemDB` — untouched; scope lives above
  the seam by construction (ADR-0006 holds).
- **No new constraints.** Phantoms never appear in constraint heads; `IO`
  and `IOSim s` satisfy the same io-classes set. Runtime-invisible
  (erasure): traces, goldens, determinism, and the ADR-0020 mirror are
  unaffected.
- **No CPS regions.** `newDBOS` stays a plain constructor (unscoped
  bundle, as today); only executions are region-bound (`withWorkflow`),
  because only executions need fresh scopes — parent→child handle flow is
  what forced regions, and handles are per-execution values. No bracket
  conversion, no existential-unpacking question. Lifetimes: `exec`
  prevents *confusion* across executions, not *use-after-close* (threads
  can smuggle past region end — the ST+`forkIO` caveat; easier to honor
  under IOSim than IO).
- **Inference rule (validated).** Scoped constructors take scoped parents
  (executor→state→ctx, connection→handle); only region entry binds. The
  positives compiled with one pinned helper (`startIt`, fixing the
  phantom error channel to `()`); no other annotations anywhere.

## 4. Cost/benefit (from the weighting round)

| Option | Benefit | Cost | Verdict |
|---|---|---|---|
| Phantoms (`exec`, one region binder) | Closes cross-exec + natural-shape leaf rule + capture backstop; zero deps; erasure-clean | Moderate mechanical churn (Ctx split, depth counter, refusal-test rework; no CPS, no facade lifecycle change) | **Do this** |
| Bluefin `Eff` + handlers | Native rank-2 scope, standard idiom | IO-only (kills sim strategy), new dep, ADR-0006 overturn, full rewrite | No |
| `State`/`Ask`/`Throw`/streams | Nominal | `Eff` + lost STM virtues | No — STM wins |
| `DslBuilderEff` bridge | Contained ergonomics probe | Days, scratch-only | Prototype-only |

## 5. Prototype report (`proto/scope-brands/`, 13/13 green, descoped)

- **Model** (`src/Scope/Model.hs`, real io-classes constraints): unscoped
  `DBOS`/`Connection`/`Registry`/`WRef`/`WHandle` (plain `newDBOS`
  constructor); `WorkflowCtx`/`StepCtx`/`Pending` over `exec` only;
  `withWorkflow` binder; `nextStepId` on `WorkflowCtx` only; `startChild`
  checks the registry's bound instance id first (runtime `WrongInstance`,
  ADR-0018 order), then derives `parent-step` ids through the
  depth-checked `placeCall`; `mintHandle` as the bare-id introduction
  gate; `drive` with the token backstop. Private constructors, explicit
  exports = the privacy boundary.
- **Positives.** `proto-io`: two instance objects side by side, child
  derivation, step narrowing, same-exec drive, bare-id minting, counter
  independence. `proto-sim`: two instance objects + virtual-thread
  rendezvous in one `runSim`, deterministic.
- **Negatives (all rejected with the intended error).** N2 step-body
  child-start (`StepCtx` vs `WorkflowCtx`); N4 counter on the narrowed
  view; N5 cross-execution drive (`exec1` vs `exec`); N6 cross-run escape
  (sim tag refuses). Retired with `inst`: N1/N3 (nothing to check; B0
  covers the runtime refusal).
- **Backstop (`proto-backstop`, B0–B6).** Cross-instance refused (B0,
  oracle parity); capture-start and capture-place refused (B1/B2); depth
  restored after success (`wf-0` — refused attempts spend no counter
  positions) and after a throw (B3/B4); markers distinct (B5); tokens
  per-attempt and initially unfired (B6).
- **Incidental findings.** Pure constructors need `Applicative m`
  (invisible in the real tree's full constraint tuples); io-sim's
  `runSim` returns `Either Failure a` (N6 first drafted against the old
  pure shape); fork is `Control.Monad.Class.MonadFork`, not
  `Control.Concurrent.Class.MonadFork`; scratch projects need
  `ImportQualifiedPost`/`DerivingStrategies` stated (the real tree
  inherits both from cabal defaults); connection/registry ids deriving
  from a name is adequate for a prototype but the tree mints UUIDs.

## 6. Recommendation and migration order

Land compiler-guided (every error is local): (1) `exec` +
`WorkflowCtx`/`StepCtx` split, engine entry points flipped — signatures
first (tree stays green throughout phase 1); (2) depth counter +
`placeCall` refusal + `withStep` restore, `startChild` keeps its runtime
instance check (already there today); (3) facade + tests + demo-apps
call-site updates; (4) rework the refusal tests (`InsideStep` paths
become unrepresentable through typed calls and move to backstop-style
runtime assertions; `WrongInstance` tests stay as-is). No `newDBOS`
change, no CPS conversion, no bracket churn. The `ghciwatch` loop holds
throughout; only phases 2–3 are red, each sized for one sitting.

## 7. Open decisions

1. **`InsideStep`: type-error, runtime, or both? RESOLVED by phase 2 — both, with evidence.** The split rejects the natural shape at compile time (N2/N4); the depth counter refuses the capture shape at runtime (B1/B2) and restores under `finally` on success and on throw (B3/B4). §11's "keep runtime" is reinterpreted as "keep as backstop" with a strictly smaller reachable surface; the residual exhibit retired into `proto-backstop`.
2. **Binder naming: value-named, decided (amended: `withDBOS` dropped with #1).** `withWorkflow` / `withStep` — each named after what the continuation receives (workflow context, step view). Scope param (`exec`) unchanged; placement (`executeRegisteredWorkflow`, step bodies) unchanged.
3. **`Tasks` stays unbranded** (per §11: ownership semantics already
   right). No prototype coverage; no change proposed.

## 8. Planned-change summary (real-tree migration)

| # | Current API | Issue | New API | What it fixes (prototype evidence) |
|---|---|---|---|---|
| 1 | `newDBOS :: m (DBOS m)`; unscoped everything; cross-instance refused at runtime (`WrongInstance`, ADR-0018) | R9 cross-instance (singleton-like process; wiring-error class) | **DROPPED** — stays exactly as today (oracle parity); B0 asserts the refusal | — |
| 2 | No execution scope in types; `ExecutionIdentity` compared at runtime; placed values flow across executions; `StepBuiltElsewhere` runtime-only | R9/checkHere cross-execution half runtime-only | `withWorkflow` binding `exec`; `PendingStep` carries `exec`; drive demands same `exec`, token backstop kept | cross-execution driving ill-typed (N5); smuggled values refused at runtime |
| 3 | Single `Ctx m`; `nextStepId`/`startChildWorkflow` take any `Ctx`; leaf rule is runtime `inStep` → `InsideStep` | step bodies can allocate ids and start children — the natural shape compiles | `WorkflowCtx` (owns counter) + `StepCtx` (narrowed view); allocators and child-start take `WorkflowCtx`; bodies receive `StepCtx` | natural misuse rejected at compile time (N2/N4) |
| 4 | No depth tracking; `withAttempt` only rebinds | captured parent context bypasses the leaf rule silently (proven by exhibit) | depth counter in shared per-execution state; `placeCall` refuses at depth > 0; `withStep` restores under `finally`; fresh marker + token per attempt | capture refused at runtime (B1/B2); no lockout on success/throw (B3/B4); per-attempt freshness (B5/B6) |
| 5 | `retrieveWorkflow` mints handles from bare ids, unscoped→unscoped | the membrane between durable names and live capabilities is unnamed | named introduction gates (`DBOS inst m -> Text -> WorkflowHandle inst m e`) | all unscoped→scoped conversions flow through named functions |
| 6 | `newDBOS` / `newCtx`-style builders / `withAttempt` | names say neither what's provided nor what's scoped | value-named binders `withWorkflow` / `withStep` (`withDBOS` dropped with #1) | scope introduction visible at every call site |
| 7 | One reader set on `Ctx` (`workflowId`, `stepId`, `stepStatus`, …) | one type serves two nesting levels | readers split per context type | level confusion visible in signatures |

Deliberately unchanged: SystemDB seam and both backends, wire format and
codecs, traces, the `Owner` flag (oracle-decided), `P`/`R` erasure,
must-use discipline, `Tasks` branding.
