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
- **Narrowing.** `useImplWithin` becomes the `WorkflowCtx` → `SCtx` handoff
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

## 5. Prototype report (`proto/scope-brands/`, 9/9 green)

- **Model** (`src/Scope/Model.hs`, ~200 lines, real io-classes
  constraints): `DBOS`/`Connection`/`Registry`/`WRef`/`WHandle` over
  `inst`; `WCtx`/`SCtx`/`Pending` over `inst`+`exec`; `withInstance` /
  `withExecution` binders; `nextStepId` on `WCtx` only; `startChild`
  derives `parent-step` ids; `mintHandle` as the bare-id introduction
  gate; `drive` with the token backstop. Private constructors, explicit
  exports = the privacy boundary.
- **Positives.** `proto-io`: nested instances, child derivation
  (`wf-a-1`), step narrowing, same-exec drive
  (`Right "outcome:wf-a-1"`), bare-id minting, counter independence
  (`wf-b-0`). `proto-sim`: two instances + virtual-thread rendezvous in
  one `runSim` — `("wf-a-0","wf-b-0","wf-b")`, deterministic.
- **Negatives (all rejected with the intended error).** N1 cross-instance
  (`inst1` vs `inst`); N2 step-body child-start (`SCtx` vs `WCtx`); N3
  region escape ("would escape its scope", textbook skolem text); N4
  counter on the narrowed view; N5 cross-execution drive (`exec1` vs
  `exec`); N6 cross-run escape (both tags refuse).
- **Residual hole (exhibit builds, as designed).** `Residual_Capture`:
  a step body capturing the *parent* `WCtx` starts children fine —
  closures capture; no type split removes a value from lexical scope.
  The split rejects the natural shape (use the handed `SCtx`); the
  capture shape needs the runtime `InsideStep` backstop. This is the
  finding that forces §7's decision.
- **Incidental findings.** Pure constructors need `Applicative m`
  (invisible in the real tree's full constraint tuples); io-sim's
  `runSim` returns `Either Failure a` (N6 first drafted against the old
  pure shape); fork is `Control.Monad.Class.MonadFork`, not
  `Control.Concurrent.Class.MonadFork`.

## 6. Recommendation and migration order

Land phantoms per layer, compiler-guided (every error is local): (1)
`inst` on core types + `withInstance` beside `newDBOS`, engine
internals signatures-only (tree stays green); (2) `exec` + `WCtx`/`SCtx`
split, engine entry points flipped; (3) facade + tests + demo-apps;
(4) remove `newDBOS`, rework the two refusal-test families. The
`ghciwatch` loop holds throughout; only phases 2–3 are red, each
sized for one sitting.

## 7. Open decisions

1. **`InsideStep`: type-error, runtime, or both?** §11 says keep it
   runtime; the split makes the natural shape a compile error while
   `Residual_Capture` proves capture still compiles. Recommendation:
   both — types for the natural shape, runtime backstop for capture
   (reconciles §11 with the prototype; needs explicit sign-off because
   it reinterprets "keep runtime" as "keep as backstop").
2. **Binder naming/placement.** Prototype uses `withInstance` (per §11)
   at the `DBOS` level and `withExecution` at `executeRegisteredWorkflow`.
   Confirm against the facade naming pass.
3. **`Tasks` stays unbranded** (per §11: ownership semantics already
   right). No prototype coverage; no change proposed.
