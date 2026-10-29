# Coverage gaps: Rust oracle vs Haskell port

Date: 2026-10-28. Oracle pinned at `b1b2b7d` (#135). Every item below is
verified against the tree, not assumed. Done means the three-leg gate:
watcher sims green, binary live green, oracle suite read-only green,
plus psql recency where rows are written. Nothing here commits without
an explicit go.

## A. Missing engine module

- **Scheduler engine (oracle `cron.rs`, `schedule.rs`, `scheduler.rs`).**
  Only schedule CRUD exists at the sysdb layer. No cron parser, no
  schedule runner, no supervision. Fix: port the three modules as one
  unit (parser, then runner, then supervision), with live twins driving
  a short cron. Largest item in this file.

## B. Unported oracle commits

- **#113 idle transaction timeout.** A stalled mid-transaction process
  holds row locks and a pool connection indefinitely. Documented as
  Later in TODO.md. Fix: split connect options per server parsing,
  pre-touch connect-time validation, `IdleTimeout` setting type.
- **#118 send unification.** Haskell keeps `SendOptions` with topic and
  idempotency key plus a separate `SendBulkOptions`. Engine behavior is
  identical; only option spelling drifts. Fix: split per-message
  (`SendMessage` plus builders) from per-call options, delete
  `SendBulkOptions`, rename `forks` to `send_to_forks`.
- **#126 partition-walk early stop.** The walk visits every partition and
  returns ids without limit naming. Same rows claimed, extra empty
  claim transactions per poll. Perf only. Fix: queue-wide PENDING
  pre-count, listing skip, limit-naming result.
- **#127 receive race on owner lock.** Take and record-timeout are split,
  so two receives of one topic can both see one message and the loser
  records empty early. Documented as Later in TODO.md. Fix: move the
  take inside the wait loop, owner check first.
- **#131 app spelling rename.** Public API still says `application`
  (`Applications`, `ApplicationVersion`, list calls). Schema unchanged
  either way. Fix: mechanical public-name rename keeping serialized
  names.

## C. Partial ports to complete

- **#119 fork batch refusals.** Duplicate pinned ids in one batch and
  oversized fork timeouts fail late at the database instead of refusing
  up front naming the fork. Fix: port the batch checks into the shared
  fork helpers so both layers agree.
- **#124 start and enqueue channel.** `validateEnqueue` refuses on
  Config and the client maps to SystemDatabase where the oracle demands
  InvalidArgument. Fix: re-channel argument refusals with the oracle
  operation strings; keep Config for genuine setup failures.
- **#120 sleep surface.** Unstorable wake times go unrecorded, lookup
  runs after refusal instead of before, and the plain path never
  pre-checks. Fix: record the refusal as the step error, look up the
  recorded step first, pre-check before the step id is taken.
- **Self-target engine twins.** Sysdb twins exist on both stacks but no
  shared scenario covers cancel or resume naming the calling workflow.
  Fix: add both scenarios; promote the live-only delete cases to shared
  twins.
- **Dispatch-park sim twin.** The live case exists but is marked IO-only
  without an ADR entry, although Mem can plant the attempts directly.
  Fix: add the sim twin and the list entry, or record why not.
- **Validator home.** The self-target check lives in the Postgres
  session module while every sibling pure validator lives in Types,
  forcing the sim backend to import a Postgres module. Fix: move the
  three-line function to Types.

## D. Debouncer twin gaps (oracle #135 suite)

- **Period lower bound.** Assert the run lands no earlier than the last
  call plus the period, using row start stamps against scenario-measured
  virtual time. No fixture change needed.
- **Timeout caps under a stream.** Steady calls must run once near the
  timeout with the last input. Bound the start stamp by deadline plus
  one sweep tick plus slack.
- **Release reuse.** A blocking body holds the row running while the key
  clears and a fresh debounce starts a second row. Needs one blocking
  body registered in both fixtures, the same surgery the in-step parent
  went through. MVar blocking has sim precedent.
- **Concurrent race.** Split: sim convergence twin over caller threads
  (one row, one run, no input assertion), live N-thread twin for real
  overlap. Never assert which input wins.
- **Cross-app.** The unclaimed-holder plus collision case stages at Mem
  level today with no fixture change. The full two-runner case waits on
  a harness neither stack has.
- **Portable tag replacement.** Portable JSON is the only decoding
  codec, so extend the existing coalesce twins with a tag-replacement
  assertion instead of a new scenario.
- **Configured instance.** No test binds an instance anywhere. Spike the
  harness first, twin after.

## E. Closed, no port needed

- **#122 in-flight child starts.** All three `initOrJoin` call sites run
  inline in the calling task. No detached write exists for a parent to
  outrun, so the lost-child race cannot occur.
- **#125 and #130 unencodable outcomes.** The encoder is infallible, so
  neither refusal can trigger. Cross-SDK rows carrying such outcomes
  still read back.
