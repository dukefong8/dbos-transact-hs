# Scoped workflow capabilities: phantom brands — research + prototype

**Status (2026-10-03).** Validated on a throwaway prototype
(`proto/scope-brands/`, branch `proto/phantom-brands`): 9/9 checks green —
valid flows run under `IO` and `IOSim`, all six misuse shapes are compile
errors, inference needs no annotations beyond one pinned helper. This
document consolidates the four research rounds behind it and records what
the prototype proved, what it refuted, and what remains open.

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
  becomes `withInstance :: … -> (forall inst. DBOS inst m -> m a) -> m a`.
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
  closed at compile time. N1 mixes a ref from instance A with a context
  from B → `Couldn't match type ‘inst1’ with ‘inst’`. The `Owner`-flag
  design stays (the oracle deliberately rejected two types,
  `connection.rs:65-67`); only the comparison moves from `Text` equality
  to skolem inequality at typed call sites. Runtime checks remain for
  untyped management paths minting from bare ids.
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

- **Two tags, complementary.** io-sim's `s` (run scope) already flows
  through `m` (`MemSystemDB s`, `StrictTVar (IOSim s)`); cross-*run*
  confusion is ill-typed today. `inst` adds cross-*instance*-within-a-run,
  exactly the staged-sim-test scenario. N6 proves they nest: a handle
  carrying both tags escapes neither `runSim`'s nor `withInstance`'s
  `forall`.
- **Zero backend churn.** `SystemDB` class, Postgres sessions, both sim
  fakes, the existential, `runSystemDB` — untouched; scope lives above
  the seam by construction (ADR-0006 holds).
- **No new constraints.** Phantoms never appear in constraint heads; `IO`
  and `IOSim s` satisfy the same io-classes set. Runtime-invisible
  (erasure): traces, goldens, determinism, and the ADR-0020 mirror are
  unaffected.
- **The price is CPS.** `newDBOS :: m (DBOS m)` must become region-bound
  (`withInstance`), since per-execution scoping breaks parent→child
  handle flow. Every `bracket (newDBOS …)` site converts; existential
  unpacking was rejected (fresh skolem per match = silent identity
  forks). `inst` prevents *confusion*, not *use-after-close* (threads
  can smuggle past region end — the ST+`forkIO` caveat; easier to honor
  under IOSim than IO).
- **Inference rule (validated).** Scoped constructors take scoped parents
  (executor→state→ctx, connection→handle); only region entry binds. The
  positives compiled with one pinned helper (`startIt`, fixing the
  phantom error channel to `()`); no other annotations anywhere.

## 4. Cost/benefit (from the weighting round)

| Option | Benefit | Cost | Verdict |
|---|---|---|---|
| Phantoms (`inst`/`exec`, CPS regions) | Closes R9 + cross-exec + natural-shape leaf rule; zero deps; erasure-clean | Wide mechanical churn (facade signatures, `withDBOS`, test rewrites, refusal-test rework) | **Do this** |
| Bluefin `Eff` + handlers | Native rank-2 scope, standard idiom | IO-only (kills sim strategy), new dep, ADR-0006 overturn, full rewrite | No |
| `State`/`Ask`/`Throw`/streams | Nominal | `Eff` + lost STM virtues | No — STM wins |
| `DslBuilderEff` bridge | Contained ergonomics probe | Days, scratch-only | Prototype-only |

## 5. Prototype report (`proto/scope-brands/`, 14/14 green)

- **Model** (`src/Scope/Model.hs`, ~200 lines, real io-classes
  constraints): `DBOS`/`Connection`/`Registry`/`WRef`/`WHandle` over
  `inst`; `WorkflowCtx`/`StepCtx`/`Pending` over `inst`+`exec`; `withInstance` /
  `withExecution` binders; `nextStepId` on `WorkflowCtx` only; `startChild`
  derives `parent-step` ids; `mintHandle` as the bare-id introduction
  gate; `drive` with the token backstop. Private constructors, explicit
  exports = the privacy boundary.
- **Positives.** `proto-io`: nested instances, child derivation
  (`wf-a-1`), step narrowing, same-exec drive
  (`Right "outcome:wf-a-1"`), bare-id minting, counter independence
  (`wf-b-0`). `proto-sim`: two instances + virtual-thread rendezvous in
  one `runSim` — `("wf-a-0","wf-b-0","wf-b")`, deterministic.
- **Negatives (all rejected with the intended error).** N1 cross-instance
  (`inst1` vs `inst`); N2 step-body child-start (`StepCtx` vs `WorkflowCtx`); N3
  region escape ("would escape its scope", textbook skolem text); N4
  counter on the narrowed view; N5 cross-execution drive (`exec1` vs
  `exec`); N6 cross-run escape (both tags refuse).
- **Phase 2: scope-depth backstop.** `WorkflowCtx` owns a depth counter in shared per-execution state alongside the step/marker counters; `placeCall` refuses while depth > 0; `withAttempt` bumps/restores depth under `finally` (the real tree's `MThrow.finally` pattern) with a fresh marker and token per attempt; `startChild` routes through `placeCall`. `proto-backstop` asserts all six: capture-start refused, capture-place refused, depth restored after success (`wf-0` — refused attempts spend no counter positions), depth restored after a throw, markers distinct across attempts, tokens per-attempt and initially unfired.
  closures capture, so the split alone cannot remove a value from lexical scope — which is why the backstop above exists. The former `Residual_Capture` exhibit retired into `proto-backstop` B1/B2.
- **Incidental findings.** Pure constructors need `Applicative m` (invisible in the real tree's full constraint tuples); io-sim's `runSim` returns `Either Failure a` (N6 first drafted against the old pure shape); fork is `Control.Monad.Class.MonadFork`, not `Control.Concurrent.Class.MonadFork`; scratch projects need `ImportQualifiedPost`/`DerivingStrategies` stated (the real tree inherits both from cabal defaults).

## 6. Recommendation and migration order

Land phantoms per layer, compiler-guided (every error is local): (1)
`inst` on core types + `withInstance` beside `newDBOS`, engine
internals signatures-only (tree stays green); (2) `exec` + `WorkflowCtx`/`StepCtx`
split, engine entry points flipped; (3) facade + tests + demo-apps;
(4) remove `newDBOS`, rework the two refusal-test families. The
`ghciwatch` loop holds throughout; only phases 2–3 are red, each
sized for one sitting.

## 7. Open decisions

1. **`InsideStep`: type-error, runtime, or both? RESOLVED by phase 2 — both, with evidence.** The split rejects the natural shape at compile time (N2/N4); the depth counter refuses the capture shape at runtime (B1/B2) and restores under `finally` on success and on throw (B3/B4). §11's "keep runtime" is reinterpreted as "keep as backstop" with a strictly smaller reachable surface; the residual exhibit retired into `proto-backstop`.
2. **Binder naming/placement.** Prototype uses `withInstance` (per §11)
   at the `DBOS` level and `withExecution` at `executeRegisteredWorkflow`.
   Confirm against the facade naming pass.
3. **`Tasks` stays unbranded** (per §11: ownership semantics already
   right). No prototype coverage; no change proposed.
