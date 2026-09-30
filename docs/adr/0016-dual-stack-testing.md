# Dual-stack testing: live and sim trees per domain, with a three-leg port gate

Each ported domain ships two test trees over the same scenarios: a live tree (`DBOS.Transact.<Domain>Test`: real `PostgresSystemDB`, FastLogger tracer, runs under `main`) and a sim tree (`DBOS.Transact.<Domain>Sim`: `IOSimSystemDB`, say-carrier, watcher-eval only). The sim tree is the mirror, not a second suite: same cases, mock answers where the mock is stateless (said aloud in the assertion), plus the typed trace assertions, which live only in sim.

```haskell
-- Sim carrier that ALSO says each rendered line: typed assertions keep
-- working through 'selectTraceEventsDynamic' while eval runs print.
simTracerSay :: SomeTracer (IOSim s)
simTracerSay = SomeTracer (mkTracer emit)
  where
    emit event = traceM event >> say (unpack (showSeverity (eventSeverity event) <> " " <> renderEvent event))
```

Rules (worked examples: `ContextTest`/`ContextTestSim`, `ManagementTest`/`ManagementTestSim`):
- `cabal test` / `defaultMain` run live backends only — NEVER a `*Sim.tests` in `main`. The sim import stays used via an exported `simTests :: [TestTree]` registry so `-Wunused-imports`/`-Wunused-top-binds` stay silent.
- Exactly one `-- $>` toggle enabled in `test/Main.hs`, always a `*Sim.tests` for the tasty run (alias form — the eval scope is `Main`'s imports, so full paths do not resolve).
- Sim cases run through `runSimCase` (value + `SimTrace`) and `printTraceEventsSay` per case, so a plain `-- $> tasty` shows announcements with no runner plumbing. Live cases assert behavior only; emissions are verified by eyeballing stdout.
- Sim launches carry the say-carrier (`simDBOSWith`/`simLaunchWith` over `simConnectionWith`; the plain `sim*` helpers stay `simTracer` specializations), so engine calls announce inline per case.
- Suffix is `Sim`, not `IOSim` (`ManagementTestIOSim` renamed in the port).

## Three-leg gate: run all three when porting each module

1. **Sim leg** — watcher eval on the domain's `*Sim.tests`: green plus the announcement lines inline.
2. **Live leg** — `cabal test test --test-option='--pattern' --test-option='$2 == "<Group>"'` (tasty `$n` fields are 1-indexed path components; `$0 ~ /.../` does not parse): green, and the FastLogger lines on stdout match the new events' bodies, fields, and guard semantics (e.g. no `cancelled=0`, `count=0` on empty fork batches).
3. **Oracle leg** — `cargo test -p dbos --test <suite>` from `~/dev/dbos-transact-rust`, read-only: green, plus a structural trace comparison. The Rust integration tests install no collector (`tracing_subscriber` appears nowhere in the workspace), so `tracing::info!` is a no-op there — compare our rendered lines against the format strings and span fields at the oracle call sites, citing file and line.

Recorded 2026-09-30.
