# TODO — GAPS.md sections B, C, D

Date: 2026-10-28. Each item states its done gate. The full gate per
unit is watcher sims green, binary live green, oracle suite read-only
green, psql recency where rows are written. Commits need an explicit
go. Section A (scheduler engine) and E (closed) are not tracked here.

## B. Unported commits

- [x] #131 app spelling: rename public `application` names to `app`.
  Done 2026-10-28: `Applications` to `AppScope`, two client functions,
  all ported record fields; wire keys, columns, kept seam types, and
  kept sysdb functions byte-identical (90 wire strings before and
  after). First sweep attempt destroyed whitespace tree-wide and was
  fully reverted; redone with a gap-preserving script, single file
  verified before batch. Gates: watcher green, full 937/937, psql 501
  rows, cabal test target linked.
- [x] #118 send unification: split per-message from per-call options,
  delete `SendBulkOptions`, rename `forks` to `sendToForks`, sysdb
  `SendMessage` to `EncodedSendMessage`, engine `Message` to
  `SendMessage`, `sendWith` takes the message. Done 2026-10-28.
  Serialization option skipped per design; client signatures kept.
  Oracle's new test is borrow-check-only, no Haskell counterpart.
  Gates: watcher green, Message/Client 49/49, full 937/937, psql 501
  rows, oracle client 33/33 plus messages 9/9, cabal build all plus
  explicit test target linked.
- [x] #126 partition early stop: pre-count queue-wide PENDING, skip the
  listing, name the limit in the result. Done 2026-10-28: `Dequeued` plus
  `DequeueStop` (constructor prefixes are the collision deviation),
  `spansPartitions`, claim names the first spent limit in oracle order,
  `countPendingQueueWorkflows` shares the claim's count statement,
  `pollOnce` returns `Polled` with claim counts, walk reads room once,
  skips listing when full, stops on room claimed and on spanning stops.
  Walk staging runs through the launched executor's connection (a
  suite-backend claim reads an unregistered version and takes nothing).
  Rate twins live-only: mem keeps no dequeue history. Walk twins
  red-proven by revert. Gates: watcher green, Queue 44/44 live, full
  950/950, psql 565 rows, oracle queues 39/39 plus sysdb 272/272,
  cabal build all plus explicit test target linked.
- [x] #113 idle transaction timeout: per-server connect options,
  connect-time validation, `IdleTimeout` setting. Done 2026-10-28:
  `IdleTransactionTimeout` (`Default`/`ServerSetting`/`After`, verbatim —
  no collisions), millis mapping with sub-ms floor, exact-zero
  validation, `idleTimeoutOptionNames` plus URL/PGOPTIONS detection, pool
  `initSession` SET unless the URL owns it, `fromPool` untouched,
  25P03 classified Connection, timeout fields plus validation on Transact
  `Config` and `ClientConfig`. Runtime proof live-only (mem has no server
  to end sessions; oracle's proof is PG-only); pure logic twinned in
  TypesTest (watcher plus binary). Two port divergences recorded in-test:
  zero is exact (port `durationIsZero` is millis-based), and a bare URL
  parameter is refused by libpq where sqlx accepts it. Gates: watcher
  green, idle group 15/15 live, full 968/968, psql 565 rows, oracle
  config-unit 12/12 plus client 33/33 plus sysdb 272/272, cabal build all
  plus explicit test target linked.
- [x] #127 receive race: move the take inside the wait loop,
  owner check first. Done 2026-10-28: take-tx adopts/records-or-misses
  with Missed rolling back unrecorded; Backend loops poll plus take and
  records empty only at or past the deadline; Mem mirrors with an
  atomic take and name-filtered adopt. Twins: sysdb empty-take
  (red-proven by probe), threaded loser-waits convergence (both
  stacks), Mem rival twin. Gates: Postgres 187/187, full 937/937,
  oracle recv 9/9 read-only.

## C. Partial ports

- [x] Move the self-target validator to Types. Done 2026-10-28: the
  validator moved verbatim (smart constructor, same refusal text) and
  both backends call it from Types. Residual: the sim still imports
  `CallerStep (..)` from the Postgres session module for its caller
  verification mirror. Moving that codec machinery is separate scope.
- [x] Fork batch refusals in the shared helpers: duplicate pinned ids
  and oversized timeouts refuse up front naming the fork. Done
  2026-10-29: `forkBatchProblems` in Types (dup forked ids, forked/id
  collision with the id named, empty ids left to per-fork rules),
  refused through engine `checkForkArgs` (detail verbatim, as the
  oracle's `refuse()` vs `refused_fork()` split) and both sysdb backends
  (as `InvalidInput`), plus the oversized timeout bound in
  `forkOptionProblems` via `durationToMs`; `runFork` unified on
  `durationToMs`. Per-fork/options message formats unchanged, so no
  existing twin churn. Gates: watcher green, Management 58/58 live,
  shared batch scenario both stacks (red-proven by stub), TypesTest unit
  twins, full 973/973, psql 565 rows, oracle management 41/41,
  cabal build all plus explicit test target linked. Review follow-up
  2026-10-29: engine now walks the oracle's order via `refuseInvalidForks`
  (per-fork, clash, dup per fork in turn, options last; batch details
  backticked as the oracle spells them), sysdb batch checks removed (the
  oracle's `fork_workflows` runs only option plus per-fork checks),
  `forkOptionProblems` total on timeout failure, mem delete drops the
  subtree, mem sleep success records the serialization and names ids on
  replay failure. Gates re-run: watcher green incl. new order unit twin,
  full 980/980, psql 568 rows, oracle management 41/41 plus queues 39/39,
  cabal build all plus explicit test target linked.
- [x] Re-channel enqueue and start refusals to InvalidArgument with the
  oracle operation strings; Config stays for setup failures. Done
  2026-10-29: `validateEnqueue` refuses as InvalidArgument "enqueue a
  workflow" naming the queue (plus the missing delay checks), new
  `validateStart` covers the timeout on both operations, and
  `refusedStart` maps sysdb InvalidInput with the `api_field` spellings
  at every init path (engine starts, child starts, both client
  enqueues); `runFork`'s duplicate bound check unified on
  `durationToMs`. Per-fork/queue-registration formats unchanged. Live
  caught a twin bug: sub-ms reads as zero on both layers (millis
  granularity, matching sysdb), so the accept-case pins 1ms. Gates:
  watcher green, Client/Management/Queue groups live, full 976/976,
  psql 565 rows, oracle lib-workflow 22/22 plus client 33/33, cabal
  build all plus explicit test target linked.
- [x] Sleep surface: record the refusal as the step error, look up the
  recorded step before refusing, pre-check before the step id. Done
  2026-10-29: `durationToMs` pre-check before `placeCall` in `sleepStep`
  and `pendingSleep` (signature widens to Either), `sleepPlain` refuses
  likewise; checkpoint looks up first, durable past-max wake records the
  refusal as the step error (JSON codec round-trip) and replays it,
  deadlines saturate; `recordSleep` mapping moves from `fromSystemDB` to
  `refusedArgument "sleep"`. Both record paths red-proven in isolation.
  Gates: watcher green, Sleep 6/6 live, full 978/978, psql 567 rows,
  oracle sleep 4/4, cabal build all plus explicit test target linked.
- [x] Shared self cancel and self resume scenarios; promote the
  live-only delete cases to shared twins. Done 2026-10-29: new shared
  cancel/resume scenario (refusal texts reach the caller, row succeeds),
  promoted self-delete and ancestor-delete scenarios (rendezvous
  replaced by child-handle wait), plus the missing mem self-delete
  refusal with descendant walk that the promotion exposed (sim
  deadlocked deleting its own row). Gates: watcher green, Management
  groups both stacks, full 979/979, psql 568 rows, oracle management
  41/41, cabal build all plus explicit test target linked.
- [x] Dispatch-park sim twin plus the ADR list entry, or a recorded
  reason. Done 2026-10-29: promoted the live-only case to a shared
  `scenarioDispatchPark` (runs 0, parked status, claim counts one more
  to 151) with a `qfPlantAttempts` fixture op — SQL update live, direct
  mem row write on sim. No ADR entry needed: the case runs on sim, so
  the IO-only marker is gone with it. Gates: watcher green, Queue
  groups both stacks, full 979/979, psql 681 rows, oracle queues 39/39,
  cabal build all plus explicit test target linked.

## D. Debouncer twins

- [ ] Tag-replacement assertion inside the existing coalesce twins
  (row tag replaced alongside input), both stacks. Only portable JSON
  decodes, so no cross-codec twin is possible; the assertion pins the
  write path, not a second codec.
- [ ] Period lower bound twin from row start stamps against
  scenario-measured virtual time, both stacks.
- [ ] Unclaimed-holder plus collision twin at Mem level (plant rows,
  bounce directly, no fixture change), plus the engine collision twin
  already landed stays green.
- [ ] Release-reuse twin with one blocking body registered in both
  fixtures (MVar precedent exists).
- [ ] Timeout caps under a stream, start stamp bounded by deadline plus
  one sweep tick plus slack, both stacks; client cap through the live
  client with generous bounds mirroring the oracle.
- [ ] Concurrent race split: sim convergence twin (one row, one run,
  never which input) plus live N-thread twin.
- [ ] Cross-app two-runner twin. Blocked: no harness in either stack.
  Spike the two-app fixture (sim) or two-launch orchestration (live)
  first, twin after.
