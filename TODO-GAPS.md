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
- [ ] Fork batch refusals in the shared helpers: duplicate pinned ids
  and oversized timeouts refuse up front naming the fork. Gate: unit
  twins plus Management fork group, oracle fork tests.
- [ ] Re-channel enqueue and start refusals to InvalidArgument with the
  oracle operation strings; Config stays for setup failures. Gate:
  twins asserting the channel in Client and Management groups, oracle
  validation tests.
- [ ] Sleep surface: record the refusal as the step error, look up the
  recorded step before refusing, pre-check before the step id. Gate:
  sim twins for all three plus Sleep group live, oracle sleep tests.
- [ ] Shared self cancel and self resume scenarios; promote the
  live-only delete cases to shared twins. Gate: Management groups both
  stacks.
- [ ] Dispatch-park sim twin plus the ADR list entry, or a recorded
  reason. Gate: Queue groups both stacks.

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
