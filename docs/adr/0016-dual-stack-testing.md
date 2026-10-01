# Dual-stack testing: live and sim trees per domain, with a three-leg port gate

Each ported domain ships two test trees over the same scenarios: a live tree (`DBOS.Transact.<Domain>Test`: real `PostgresSystemDB`, FastLogger tracer, runs under `main`) and a sim tree (`DBOS.Transact.<Domain>Sim`: `IOSimSystemDB`, sim carrier, watcher-eval only). The sim tree is the mirror, not a second suite: same cases, mock answers where the mock is stateless (said aloud in the assertion), plus the typed trace assertions, which live only in sim.

```haskell
-- The sim carrier traces the structured event and says its rendered line:
-- typed assertions keep working through 'selectTraceEventsDynamic' while
-- eval runs print the same lines.
simTracer :: SomeTracer (IOSim s)
simTracer = SomeTracer (mkTracer emit)
  where
    emit event = traceM event >> say (unpack (renderLine event))
```

Rules (worked examples: `ContextTest`/`ContextTestSim`, `ManagementTest`/`ManagementTestSim`):
- `cabal test` / `defaultMain` run live backends only — NEVER a `*Sim.tests` in `main`. The sim import stays used via an exported `simTests :: [TestTree]` registry so `-Wunused-imports`/`-Wunused-top-binds` stay silent.
- Exactly one `-- $>` toggle enabled in `test/Main.hs`, always a `*Sim.tests` for the tasty run (alias form — the eval scope is `Main`'s imports, so full paths do not resolve).
- Sim cases run through `runSimCase` (value + `SimTrace`) and `printTraceEventsSay` per case, so a plain `-- $> tasty` shows announcements with no runner plumbing. Live cases assert behavior only; emissions are verified by eyeballing stdout.
- Sim launches carry the sim carrier (`simDBOSWith`/`simLaunchWith` over `simConnectionWith`, all `simTracer` specializations), so engine calls announce inline per case.
- Suffix is `Sim`, not `IOSim` (`ManagementTestIOSim` renamed in the port).

## Addendum: one sim carrier (2026-10-01)

`simTracerSay` is gone; `simTracer` does both halves. The split bought nothing: tracing the structured event and saying the rendered line are one call site's job, every `*Sim` tree passed the say-carrier anyway (the `traceM`-only variant had no tree-level user), and a quiet sim simply never reads the say half back with `printSimTrace`. The pair the trees actually compose is `simTracer` + `printSimTrace`: the carrier records the event for assertions and its line for the pane, the printer shows the lines on stderr. Rendering at trace time rather than print time is deliberate — `printSimTrace` prints a `SimTrace` of one concrete case, and `selectTraceEventsDynamic` would have to name each domain's type to render it, a list that rots the day a domain is added; the carrier is generic over `LogEvent e` and needs no list.

## Three-leg gate: run all three when porting each module

1. **Sim leg** — watcher eval on the domain's `*Sim.tests`: green plus the announcement lines inline.
2. **Live leg** — `cabal test test --test-option='--pattern' --test-option='$2 == "<Group>"'` (tasty `$n` fields are 1-indexed path components; `$0 ~ /.../` does not parse): green, and the FastLogger lines on stdout match the new events' bodies, fields, and guard semantics (e.g. no `cancelled=0`, `count=0` on empty fork batches).
3. **Oracle leg** — `cargo test -p dbos --test <suite>` from `~/dev/dbos-transact-rust`, read-only: green, plus a structural trace comparison. The Rust integration tests install no collector (`tracing_subscriber` appears nowhere in the workspace), so `tracing::info!` is a no-op there — compare our rendered lines against the format strings and span fields at the oracle call sites, citing file and line.

Recorded 2026-09-30.

## Addendum: what makes a mirror valid (2026-10-01)

ADR-0020 fixes the rule this dual-tree structure assumes: a `*Sim` case is valid only if its sim half runs the same top-level engine functions as the live half, differing only in backend and scheduler. A staged effect, a re-encoded call sequence, a hand-emitted event, or a test-side scheduling stand-in is a defect — it tests the mock, not the engine — and a case the simulator cannot run (preemption-dependent) is marked IO-only with its reason rather than dropped. The audit and the build plan live in `docs/dual-stack-concurrency-todo.md`.
