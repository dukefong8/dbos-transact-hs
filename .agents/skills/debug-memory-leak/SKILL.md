---
name: debug-memory-leak
description: Diagnose memory growth in this repo's Haskell processes. Use when residency climbs across a suite run, a process is OOM-killed, or the box runs out of memory during builds/tests. Covers the env-vs-code triage, the RTS measurement loop, heap/eventlog profiling, ghc-debug, NoThunks, and Valgrind.
license: MIT
metadata:
  author: dbos
  version: "1.0.0"
  organization: DBOS
  date: October 2026
  abstract: Feedback-loop-first memory diagnosis for the Haskell DBOS port: rule out environment pressure, measure the suite binary with RTS stats, escalate to heap/eventlog profiles, then pinpoint with ghc-debug/NoThunks/Valgrind.
---

# Debugging Memory Leaks

Symptom first, tools second. Most "leaks" on a dev box are environment
pressure (browsers, harness servers, parallel builds), not the code. This
skill orders the work so the cheap exoneration comes before the expensive
profiling. It pairs with the `diagnosing-bugs` loop discipline: no
hypothesis without a red-capable measurement.

## 0. Whose memory? (triage, minutes)

Before touching the code, name the growing process:

- `ps aux -m | head -12` — top residents by %MEM. Browsers, `opencode
  serve`, OrbStack, and parallel `opencode` sessions routinely outweigh
  everything Haskell here.
- `vm_stat | head -8` — free pages × 16KB. Under ~500MB free with a
  nonzero `Pages throttled`, any large build can tip the box over.
- `uptime` — if the box never rebooted, a "restart" was the harness or
  service layer, not the OS. Do not chase an OOM the kernel never saw.

If no Haskell process is growing, stop: the leak is environmental. Record
the top residents and move on.

## 1. The measurement loop (the gate for everything below)

Build the standalone suite binary exactly as the repo gates prescribe —
`cabal build all` resolves since the hasql-2 migration, but the standalone
binary stays the faster loop for `--pattern` selection:

```sh
ghc --make -outputdir /tmp/leakbuild -isrc:test:src/DBOS/Internal \
  -XGHC2024 -XDuplicateRecordFields -XNoFieldSelectors -XNoImplicitPrelude \
  -XOverloadedLabels -XOverloadedRecordDot -XRecordWildCards \
  -threaded -rtsopts test/Main.hs -o /tmp/leak-test
```

Rules, all load-bearing:

- Build outputs live under `/tmp`, never in the tree. Never run two
  builds at once (separate `-outputdir`s), and never run two live suites
  concurrently (shared fixture rows deadlock).
- Always cap: `+RTS -M4G -RTS`. An uncapped run that eats the box proves
  nothing and kills the harness.
- **RTS flag placement**: runtime flags go *inside* the `+RTS … -RTS`
  block. `/tmp/leak-test +RTS -M4G -s -RTS` measures; a `-s` after the
  closing `-RTS` is parsed by tasty as a test flag and fails with
  `Invalid option`.
- Red-capable loop: `/tmp/leak-test +RTS -M4G -s -RTS`, asserting max
  residency against the standing baseline (~160MB peak for the full
  suite). Green = exonerated; red = escalate.
- Tighten by bisection: `--pattern '$2 == "<Group>"'` narrows to one
  domain (tasty `$n` fields are 1-indexed). A group whose residency
  dwarfs the rest names the subsystem; a flat profile across groups
  means systemic retention (logger buffers, pools, registries).

## 2. Heap profiles (what is retained)

Stage two when `-s` shows growth. `-hT` works on a standard build;
`-hy`/`-hd`/`-hc` (by type / constructor / cost center) need a profiling
build (`-prof -fprof-auto`). Convert with `hp2ps` or a visualizer.

```sh
/tmp/leak-test --pattern '$2 == "<Group>"' +RTS -M4G -hT -RTS
```

Read the curve, not just the peak: a staircase climbing per test is a
cross-test retainer (trace handles, `TVar` maps held past the case,
registry entries); a plateau that never drops is a one-time hold (config,
connection pools sized at launch). Sim backends are per-case fresh
(`newMemDB`), so growth across sim cases means a reference escapes the
case — the defect class to name, as with the `memDbNow`
self-recursion blowup (a clock helper rewriting its own body spun
virtual time forever under a retained trace; prevention: re-grep an
introduced identifier for self-reference after every batch rewrite).

## 3. Eventlog (leak vs fragmentation vs off-heap)

When the heap profile does not explain the RSS, the growth may not be
Haskell heap at all. Build with `-eventlog`, run with `+RTS -l`, and open
the result in `eventlog2html`: live bytes vs total heap, GC phases,
mutator time, scheduler activity. A flat live-bytes curve beside climbing
RSS means fragmentation or off-heap/C-library retention (database
drivers, TLS, compression) — no amount of strictness annotations will fix
it; look at pool sizing and foreign resource lifetimes instead.

## 4. Pinpointing (exact retainer)

- **ghc-debug**: connect to the running process over a socket
  (`withGhcDebug`), then use retainer analysis, heap history, and stack
  decoding (`ghc-debug-brick` TUI) to walk the exact thunk chain. The
  tool for "what points at this closure".
- **NoThunks**: assert at runtime that structures hold no unexpected
  thunks. The natural homes here are `WorkflowState` fields, `MemSystemDB`
  maps, and any accumulator threaded through a retry/poll loop. A
  `NoThunks` failure *is* the regression test for a laziness leak.

## 5. Low level (instruction/cache blowups)

For suspected unforced-tuple or go-loop explosions: compile with `-g`
(DWARF) and run under Cachegrind/Valgrind per the Tweag profiling guide.
Instruction/write explosions point at accumulators that never force —
fix with strictness (`!`, `StrictTVar` writes already strict here), not
with restructuring.

## 6. Cleanup

- Delete `.hp`, `.eventlog`, and `ps` outputs when done (they are large;
  they never enter the tree — scratch lives under `/tmp` or the
  operator's own space, never the repo).
- Record the outcome where the next debugger looks: the retainer (or the
  exoneration with numbers) goes in `TODO.md` beside the other dated
  notes, and a standing guard (cap, baseline assertion, or `NoThunks`
  check) lands in the suite if one earned its keep.
