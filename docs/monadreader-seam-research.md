# Should the Transact seam thread Context implicitly via MonadReader?

**Question:** should `DBOS.Transact` pass its Context (connection / executor /
identity — the values threaded explicitly through `DBOS m` / `WorkflowCtx exec m`
today) implicitly via `MonadReader` instead of explicit parameters?

**Status:** research only. No source or test files changed, no builds run.

**Rust oracle:** out of scope by decision. Rust has no Reader-equivalent concern:
`Ctx` there is a clonable, explicitly threaded value with no lexical scope of its
own (`docs/adr/0012-explicit-connection-ctx-threading.md:3`). Nothing below needs
the oracle.

## Current state

Threading is explicit end to end, by recorded decision:

- Workflow bodies are `argument -> WorkflowCtx exec m -> m result`
  (`src/DBOS/Internal/DBOS/Transact/Instance.hs:128`), step bodies
  `StepCtx exec m -> m value` (`src/DBOS/Internal/DBOS/Transact/Step.hs:169`).
- Engine entries take context pieces as arguments: `runStep wctx name body`
  (`Step.hs:169`), `runNestedStep sctx name body` (`Step.hs:221-227`),
  `runTxStep ds config wctx body` (`Datasource.hs:192`),
  `startChildWorkflow wctx ref options input` (`Workflow.hs:636`),
  `runRegisteredWorkflow tasks conn identity snapshot …` (`Workflow.hs:86`),
  `runWorkflow executor …` / `cancelWorkflows dbos …` (`Instance.hs:216,288`).
- The backend seam is explicit-handle too: every `SystemDB` method takes `db`
  first; "there is no `ReaderT` carrier" (`src/DBOS/Internal/DBOS/SystemDB/Class.hs:43-57`).
- The tracer is explicit the same way: `SomeTracer m` rides `Connection`
  (`Connection.hs:109`) / `WorkflowCtx` (`Context.hs:365`), emission only through
  `runTracer` (AGENTS.md tracing rule).
- History: ADR-0009's `ReaderT` backend is retired, ADR-0006's Bluefin seam is
  retired, ADR-0012 mandates explicit `Connection`/`Ctx` threading. Bluefin is no
  longer in `dbos-transact-hs.cabal` build-deps (verified: no `bluefin` edge).

## (a) What would become implicit, and what stays explicit

Candidates for the implicit set: the `wctx`/`sctx` argument of every scoped
entry (`runStep`, `runStepWith`, `runTxStep`, `startChildWorkflow`, `selectStep`,
`send`/`recv`, `getEvent`/`setEvent`, `sleepStep`, `waitForWorkflow`, …) and the
`dbos`/`executor` argument of the `Instance`/`Client` entries. Two boundaries:

1. `DBOS m` actions only (a `ReaderT Executor`-style app layer); workflow bodies
   keep taking `WorkflowCtx` explicitly.
2. Workflow bodies too (a `ReaderT WorkflowCtx`-style engine layer); or both.

Must stay explicit regardless: the `db` backend handle (backend seam, Class.hs
note above); `DataSource m` + `Tx m` in `runTxStep` (connection-affinity pinning
lives on that path, `scoped-workflow-capabilities.md` §8 row 8); plain config
data (`StartOptions`, `TransactionConfig` — ambient buys nothing); and the
`WorkflowCtx` vs `StepCtx` split itself (see (c)).

## (b) The io-classes gap — the load-bearing fact

**There is no simulatable `MonadReader`.** Verified against primary sources:

- mtl-2.3.1 `Control/Monad/Reader/Class.hs`:
  `class Monad m => MonadReader r m | m -> r`, `MINIMAL (ask | reader), local`,
  with instances for `(->) r`, `ReaderT r m`, CPS `RWST`.
- transformers-0.6.2.0 `Control/Monad/Trans/Reader.hs:127`:
  `newtype ReaderT r m a = ReaderT { runReaderT :: r -> m a }`;
  `ask = ReaderT return` (`:244-245`), `local = withReaderT` (`:251-257`).
- io-classes-1.11.0.0: **zero** occurrences of `MonadReader` in the `io-classes`,
  `si-timers`, `strict-stm`, and `mtl` sublibrary sources (`grep -c = 0`).
  io-sim-1.11.0.0 `src`: **zero** occurrences. No `MonadReader r (IOSim s)`
  instance exists anywhere in the dependency closure.
- The lifting that *does* exist runs the other way: io-classes classes have
  `ReaderT` instances (`MonadMVar.hs:159-189`, `MonadThrow.hs:286-291`, plus the
  `io-classes:mtl` orphan-`Trans` modules). That is, `ReaderT` preserves the
  simulatability of its base monad — but `MonadReader` itself abstracts over
  nothing simulatable.

Options, assessed against ADR-0020 (sim must drive the same engine functions,
`docs/adr/0020-…md:5`):

1. **mtl `MonadReader` + per-stack `ReaderT` newtypes.** Mechanically possible
   only with one env type per monad (fundep `m -> r` — see (c)), and the sim and
   live trees would run in *different* monads (`ReaderT E IO` vs
   `ReaderT E' IOSim`). ADR-0020 tolerates different runners/backend/scheduler,
   so this is not a direct violation — but every `*Cases` body and both fixture
   types would churn (see (e)), and the suite gains a whole new mismatch class
   (right function, wrong env) that explicit parameters make visible at the call
   site.
2. **Repo-local reader class** (e.g. `HasWorkflowCtx`). Directly against the
   typeclass convention ("concrete modules now… test fakes use
   records-of-functions", AGENTS.md) with no second-backend justification, and it
   duplicates mtl for no expressive gain.
3. **Effectful read via existing `Ask`.** Unavailable: Bluefin was removed
   (ADR-0012), `Eff` is IO-based (`newtype Eff es a = UnsafeMkEff (IO a)` per
   `docs/bluefin-research-context.md:13-14`; "forces `m ~ IO`, kills the IOSim
   dual stack", `scoped-workflow-capabilities.md:51-55`). Re-adding a dependency
   to recover what explicit arguments already do.
4. **Don't.** Zero churn, zero new constraints, both stacks keep working.

## (c) Rank-2 `forall exec` brands vs an ambient reader

The phantom-brand scheme (`docs/scoped-workflow-capabilities.md` §§2-3,5)
isolates executions by putting the brand on *values*: `WorkflowCtx exec m`,
`StepCtx exec m`, `SelectArm exec`, cross-execution refusal at compile time
(N5: `Couldn't match type 'exec1' with 'exec'`). The permanent probes pin this:
`probes/neg-exec-escape.hs` (rank-2 binder traps the view), `neg-cross-exec-arms.hs`,
`neg-alloc-on-step.hs` (`nextStepId` takes `WorkflowCtx` only),
`neg-child-start-step.hs`, `neg-nested-on-workflow.hs` — each must fail with
`Couldn't match` (`probes/run.sh:20`).

An ambient reader is structurally hostile to this:

- Fundep `m -> r` fixes **one** env type per monad, but one `m` routinely hosts
  several live executions (parent + detached child, select arms built under
  different views). A single `MonadReader (WorkflowCtx exec m) m` instance cannot
  serve two `exec`s; the way out is an unbranded/existential env — which deletes
  exactly the distinction every probe above pins, demoting cross-execution
  confusion from compile-time refusal back to the runtime token backstop.
- `ask :: m r` hides provenance: the brand scheme works because misuse is
  ill-typed *where the value flows*; `ask` makes the value's origin invisible at
  every call site. `nextStepId`-on-`StepCtx` stays rejected only if the ambient
  env is the workflow view — but then step bodies cannot `ask` their own attempt
  scope without a second reader, which the fundep forbids in the same stack
  without newtypes.
- The scoped doc notes "Phantoms never appear in constraint heads"
  (§3); a `MonadReader` constraint puts exec-bearing types in constraint heads
  via the env. **Verdict on (c): ambient reader weakens brand isolation.**

## (d) Interaction with Bluefin `Ask`

Overlap in purpose, conflict in mechanism, unavailable in practice.
Bluefin 0.9.1.0: `Bluefin.Reader` is a deprecated shim over `Bluefin.Internal`;
the canonical `Bluefin.Capability.Ask` exports `Ask/runAsk/ask/asks/local` with
`runAsk :: r -> (forall e. Ask r e -> Eff (e :& es) a) -> Eff es a`
(`docs/bluefin-research-context.md:414-423`, shim exports read in the 0.9.1.0
tarball). `Ask`'s handle is *explicit and rank-2-scoped* — i.e. the same shape as
today's explicit `WorkflowCtx` plus escape prevention — while `MonadReader` is
ambient with no escape prevention (`ask` results can be stashed anywhere; cf. the
fork-fragility catalogue, `bluefin-research-context.md` §3). Adopting
`MonadReader` would be strictly weaker scoping than the already-rejected Bluefin
layer, and would require un-removing a dependency against ADR-0012's IOSim
rationale. **Conflict, not complement.**

## (e) Effect on dual-stack fixtures and the per-slice acceptance rule

Fixtures are already "ambient per tree, explicit per call": `MgmtFixture m`
(`test/DBOS/Transact/ManagementCases.hs:128-136`) and `Fixture m`
(`test/DBOS/Transact/ContextTest.hs:201-206`) bundle builders
(`mfNewDBOS`, `mfLaunch`, `mfConn`, `fixtureMkCtx`, …) filled per stack
(Postgres+UUIDs+FastLogger vs `MemSystemDB`+counters+`simTracer`), while shared
scenario bodies take what they need as arguments and drive identical engine
entries on both stacks (`test/DBOS/DualStack.hs:3-9,40-50`).

A Reader layer breaks the sharing, not the naming: a scenario would become a
`ReaderT E IO a` term on one stack and a `ReaderT E' (IOSim s) a` term on the
other — no single polymorphic body, since `Connection IO` and
`Connection (IOSim s)` live in different `m`s and the env types differ with
them. Case names could stay identical, but the ADR-0020 acceptance criterion
("deleting a sim half removes no engine function call", ADR-0020:31) would have
to be re-established by inspection per case instead of by construction through
one shared body. `local`-based overrides would also replace the current explicit
rebinds (`withTracer`, `newWorkflowCtx` at `ContextTest.hs:174-179`). **Cost:
churn in every `*Cases` module + both fixture types, for weaker guarantees.**

## (f) Alternatives

| Option | Cost | Verdict |
|---|---|---|
| Status quo (explicit) | none | **recommended** — matches oracle shape (ADR-0012), both stacks green |
| App-level `ReaderT` over the facade (e.g. `ReaderT Executor IO`) | zero library change — apps can do this today; facade already pins `IO` (`Instance.hs` launch/shutdown) | allowed, not a library question |
| Bluefin handle only | re-add dep, `Eff` is IO-only, overturn ADR-0012 | no |
| Effect-row (`effectful`/`fused-effects`/`polysemy`) | new deps, per-stack interpreters, against dependency + io-classes conventions | no |

## Open uncertainties

1. No app-side ergonomics survey: how many `wctx`/`executor` call sites the
   demos carry, i.e. how much pain explicit threading actually causes. Uncounted.
2. No `ghci` instance-resolution probe (read-only task): the fundep-collision
   and missing-`IOSim`-instance claims are by source inspection, not by a
   typechecker witness.
3. Bluefin 0.9 `Internal` signatures cited via the repo's Hackage-derived doc +
   0.9.1.0 shim exports; `Bluefin.Internal` itself was not read verbatim.
4. Config-adjacent paths (`pendingStepWith`/`StepOptions`, `runTxOutside`'s
   `Tx`-only body at `Datasource.hs:409-410`) not individually audited — they
   take no workflow context today, so they are unaffected either way.

## Recommendation

**Reject** ambient `MonadReader` at the engine seam. It buys call-site brevity
against four recorded losses: no simulatable class exists (b); it collides with
the `exec`-brand isolation the probes pin (c); its Bluefin cousin was removed
for IOSim incompatibility (d); and it dissolves the shared-body structure the
dual-stack acceptance rule rests on (e). If explicit threading ever hurts enough
to revisit, the experiment to run first is app-level `ReaderT` in `demo-apps`
— it needs no library change and touches none of (b)-(e).

## Addendum 2026-10-06 — the `forall exec` reader probe

Proposed shape: `MonadReader (WorkflowCtx exec m) m` for every `exec`. A
scratch probe (two phantom brands, one instance each over `IO`) fails to
compile under mtl's fundep `m -> r`:

    Functional dependencies conflict between instance declarations:
      instance MonadReader (Ctx BrandA) IO
      instance MonadReader (Ctx BrandB) IO

One env per monad is all the class allows, so per-execution environments
cannot share `IO` (or one `IOSim s`). The workarounds confirm the cost
rather than avoiding it: a fundep-free custom class compiles but makes
every read ambiguous; an existential box compiles but erases the brand the
five `neg-*` probes pin; per-execution `ReaderT` compiles and simulates
(io-classes ships the trans instances) but wraps explicitly at every run —
explicit threading with extra steps. Ambient reader survives only above
the `exec` boundary (one app handle per `runReaderT` per case), never
inside workflow bodies.

## Addendum 2026-10-06 — app-level ReaderT prototype (Starter exampleWorkflow)

Ran it, for real, against the test database
(scratch project outside the repo; `cabal repl` + `:load`; marked
`proto-readert-*` ids, deleted afterwards, zero rows left):
`AppEnv { appDBOS, appExec :: Maybe (Executor IO), appLabel }` over mtl
`ReaderT`, with the Starter `exampleWorkflow` body copied verbatim
(explicit `wctx`, short sleeps) running to `"Workflow completed"` and
`cleanup=Right 1`.

What it proves: the viable shape works end to end — `regExample`,
`launchApp`, and `runExample` read the handle from the env, bodies are
untouched, and the post-launch executor scopes in through `local`, so
unlaunched code cannot name an executor it does not have.

What it costs: the `Maybe` executor (or two env types) is load-bearing
complexity the explicit style does not have; every `liftIO` boundary
still needs its error channel pinned; `NoFieldSelectors` forces
pattern-match reads on the env. Net ergonomic gain over explicit
threading: about one repeated parameter per helper.

Incidental finding: the full launch forked a supervisor that claimed 11
foreign stranded rows (`EngineCancelledRunning cancelled=11`), left
`PENDING` at shutdown for recovery to requeue. Orthogonal to ReaderT
(explicit style does the same), but a live demonstration of why
unscoped launches stay out of shared-database tests.

Verdict stands, sharpened: keep explicit threading in demo-apps; the
ambient pattern functions but pays for itself only if app-level helpers
multiply well beyond today's handful.

## Addendum 2026-10-06 — ImplicitParams with branded scope: it works

Probed minimal (scratch, plain `ghc`, no dependencies): `needA ::
(?ctx :: Ctx BrandA) => Int` with `let ?ctx = Ctx :: Ctx BrandA`
compiles clean; the same use under `Ctx BrandB` fails with
`Couldn't match type 'BrandB' with 'BrandA'` arising from the
implicit-parameter constraints. No instances, no fundep conflict (name+type
keying, not class resolution), brand preserved exactly, and purely
constraint-based so IOSim behavior is identical by construction.

Remaining objections are idiom, not feasibility: innermost-lexical
resolution is silent on shadowing; higher-order uses defer errors to use
sites; GHC-only and unidiomatic against the repo's explicit-records
culture. Like per-execution `ReaderT`, it needs no library change — a
body binds `let ?wctx = wctx` from its explicitly-received context.
Cheapest of the three ambient mechanisms if one is ever wanted; default
stays explicit.

## Addendum 2026-10-06 — where the brand is actually bound

`run*Workflow`/`run*Step` do not scope `exec` the way `runST` scopes `s`:
they consume an already-built `WorkflowCtx exec m` and bind nothing. The
only ST-like boundary is `withWorkflow`, whose rank-2 continuation
(`forall exec. WorkflowCtx exec m -> m a`, `Context.hs:384`) mints a fresh
brand per run — and the engine run path goes through it
(`Workflow.hs:226`). `Executor m` never mentions `exec`, so executors are
freely shared across runs. The hole is `newWorkflowCtx`
(`Context.hs:398`): it returns `m (WorkflowCtx exec m)` with the brand
bound at the call site, for engine paths and fixtures that must hold a
view outside a continuation.

For the prototype this changes nothing and clarifies everything: the
ambient `ReaderT` close-over inherits exactly the provenance of the ctx
it closes over. Registered bodies receive a `withWorkflow`-minted ctx per
run, so per-execution `ReaderT` stays sound — no stronger and no weaker
than the explicit passing it replaces. Close it over a `newWorkflowCtx`
view shared across runs and the ambient env is shared too, identically
to an explicitly passed ctx. No new hazard either way; the fundep probe
above stands as stated.

## Addendum 2026-10-06 — the brand guards the box, not the contents

Correction to the paragraph above: the rank-2 boundary is ST-shaped but
not ST-substantive, and a two-sided probe proves it. `WorkflowState m`
(`nextStepIdRef`, `nextMarkerRef`, `stepDepthRef`), `Connection m`,
`Identity`, the spawner, and the tracer never mention `exec` — unlike
`STRef s`, no mutable cell is indexed by the brand.

- Wrapper escape is rejected (`make probes`, `neg-exec-escape.hs`):
  `Couldn't match type 'exec2' with 'exec1'`. Two ctx values cannot mix.
- State extraction compiles clean (scratch `LeakState.hs`, same
  toolchain): returning `wctx.wctxState` out of the `withWorkflow`
  continuation is accepted — `IO (WorkflowState IO)` mentions no brand.

So per-run isolation substantively rides freshly-minted TVars
(`nextExecutionIdentity` + `newWorkflowState` per `withWorkflow` call),
not the brand. The brand's actual teeth: it stops two ctx *values* from
mixing in one scope. Anything inside a continuation — or handed a
`newWorkflowCtx` view — can legally share the brand-free contents across
runs; the engine just never does. The prototype inherits exactly that:
sound on minted contexts, shared on shared views, identical to explicit
passing in both cases.
