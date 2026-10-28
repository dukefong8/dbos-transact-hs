# TODO — scoped workflow capabilities + step tables

Branch `proto/phantom-brands` (cut from main-line work; main untouched).
Date: 2026-10-04 (serialization slices added 2026-10-18; status synced
2026-10-19). Gate state: full suite **830/830**, psql mirror green,
migration ceiling 121. Design record: `.lavish/rust-port-plan.html` (Phase 10,
Rule 9, TODO 12–16), `docs/scoped-workflow-capabilities.md` (§1–§10),
`docs/agent-skill-api-review.md` (per-rule verdicts),
`proto/deterministic-steps/README.md`, `proto/dual-store/README.md`.

## Rust serialization stack — parity slices → Python interop (2026-10-18)

Oracle: `~/dev/dbos-transact-rust` @ `0f1dd66` (main) plus branch
`origin/refuse-non-finite-floats` (`fa48de8`…`e2fed10`, **unmerged upstream** —
re-pin the branch hash before starting S7). Port position: Rust #93/#94 landed
(`c84b4a1`). Ported 2026-10-19: S1's read dispatch and init-row input, S2 in the
simplified ADR-0038 form, S3 in full, S4–S7, and phase P's golden corpus plus
the Python read-back. Remaining: none in this stack.

Slice state (2026-10-19):

| Slice | State |
|---|---|
| S1 | done — dispatch, init-row input, schedule context untagged |
| S2 | done (simplified per ADR-0038) |
| S3 | done — portable caller error record, tag-dispatched replay |
| S4 | done (load_output + limit/offset on instance, in-workflow, client) |
| S5 | done (client listen default false, field inert) |
| S6 | done — 43/44 re-vendored; schema probe and oracle green |
| S7 | done — ADR-0039 option A; aeson facts pinned |
| P | done — P2 golden corpus, P1/P3 Python read-back |

Goal: port these commits for **Rust parity**, then prove **Haskell↔Python
interop** on one database (phase **P**) — the payload formats are the
cross-SDK contract, so parity without a cross-read is not evidence.

Order: **S1 → S2** (S2 unblocks phase P); **S3** after S1; **S4/S5/S6**
independent; **S7** after S1+S2; **P** after S2 and again after S7.

Standing gates, every slice:

- `make db-migrate` first (the Rust runner owns migrations; idempotent, and
  the ceiling must not move — 121 today).
- One IO/Sim pair toggled in `test/Main.hs` (`-- $>`); read `ghcid.txt` after
  every reload, never sleep first; sim trees that `printSimTrace` need
  `dependentTestGroup … AllFinish`; IO-only cases get the `-- IO only:` line
  and an ADR-0020 running-list entry.
- Watcher pair green → `cabal test test --test-option='--pattern'
  --test-option='$2 == "<Group>"'` → `cabal test all` only while the watcher
  is idle (`cabal build all` does not compile tests).
- Oracle leg: `cargo test -p dbos --test <suite>` read-only from the Rust
  checkout; never copy code, never chase main.
- psql against `$DBOS_DATABASE_URL` for the rows the slice writes and reads
  (a green `cabal test` is not the final gate).
- Facade discipline: every client-facing name lands in `DBOS.Transact`; every
  name the facade imports stays exported.
- Docs at slice end: `.lavish/rust-port-plan.html` delta, `CHANGELOG.md`, and
  `docs/cross-language-schema-interop.md` whenever the contract moves.

### Two receivers on one topic (Rust #111 `0da511d`, 2026-10-21)

- `ConcurrentRecv` is gone from `DBOS.SystemDB.Error` (JSON, render,
  `shouldRecord`), the exclusive registry entry point is gone from
  `DBOS.SystemDB.Notify`, and `recv` subscribes shared: both waiters wake,
  the consuming `UPDATE` gives the message to one, and the other records
  the absence at its deadline.
- `Postgres.Messages` replaces the refusal case with a two-receiver case
  (one message, one `Just`, one consumed notification); `NotifyTest` keeps
  only the shared-registry cases; the audit doc is corrected.
- Oracle leg green with OrbStack's socket
  (`DOCKER_HOST=unix://$HOME/.orbstack/run/docker.sock`): `--test sysdb`
  245/245 and `--test messages` 8/8, including
  `two_receivers_in_one_process_share_one_message`.

### Deadline slice (Rust #109 `7c879f3`, #110 `c1627c6`, 2026-10-26)

Ported in one slice (adopted an uncommitted #110 draft, then #109 on top):

- #110 threads a `DatabaseTime` through `WorkflowInitResult`, makes the
  insert compute a direct start's deadline from its timeout (returning
  `clock_timestamp()` with it), stops the Haskell side stamping one, and
  converts `Deadline` to a monotonic expiry for the body timer. The fired
  timer cancels only through `cancelIfTimedOut` (the sweep's own test) and
  adopts the row; an adopted `AwaitedCancelled` is the execution's own
  cancellation.
- #109 adds the per-executor timeout sweep (`DBOS.Transact.TimeoutSweep`:
  batch 1000, 1s idle cadence, full batch repasses at once, shutdown-aborted
  beside the supervisor), vendors migrations 122 AND 123
  (`122_add_workflow_status_deadline_index.sql`,
  `123_add_workflow_status_creator_xid.sql`) into `rust-migrate`.
- The oracle's shadowing-schema skew fixture has a shared-DB-safe
  counterpart: `memClockSkewMs` skews every mem clock read through
  `memDbNow`, and the ±60s dual-stack skew cases assert a 500ms budget runs
  ~500ms virtual then cancels. Live runs the same shape unskewed.
- Draft repairs: fresh sim inserts execute again (the unification through
  `memInitResult` had wedged every fresh sim start), mock backend gained
  the cancel methods plus a clock.
- Gates: Deadlines pair 8/8 live + 10/10 sim; Workflow execution 61/61
  live; Postgres 166/166 live; mirror 72/72; full live suite 855/855 in
  19s; every run under 160MB peak, serial through standalone binaries
  (no watcher, no cabal).
  One unrelated `Application versions` timestamp failure seen once in an
  earlier Postgres run, not reproduced since.
  NOT run: full `cabal test` (blocked on the sibling `hasql` drift), the
  sim trees outside Deadlines/mirror (per directive, sim suites are not
  gates), and the oracle cargo suites.
- Review fixes (same slice, all green): `cancelIfTimedOut` presents the row
  owner (`initResultOwnerXid`, `IS NOT DISTINCT FROM`) so only an execution
  that still owns the row cancels it; finished and parked ids adopt instead
  of re-running on the run, start, and child paths; guard tests pin the
  owner predicate live and sim; a dual-stack finished-id test pins
  adopt-without-rerun by trace. Open alongside: dequeue claims do not rotate
  the owner token the way the oracle's one-token-per-claim does, so the
  guard is fail-closed but rarely refuses — claim rotation belongs with
  #117 fencing.
- Memory incident, root-caused and fixed: a 50GB sim blowup came from a
  self-recursive `memDbNow` (the clock-sweep batch rewrote the helper's own
  body), spinning virtual time forever under a retained trace. Prefix probes
  isolated it to `initWorkflow`; the mirror group passes 72/72 at 80MB
  peak. Prevention: re-grep an introduced identifier for self-reference
  after every batch rewrite.

### Naming hygiene (Rust #129 `defd947`, #128 `29326be`, 2026-10-26)

- #129 (`queue` → `queue_name` on fork/rewind/resume options, schedule
  `name()` → `schedule_name()`) needed nothing: the port already names
  these fields `QueueName`, holds records (not names) in start options,
  reports `"queue_name"` in fork validation, and has no `ResumeOptions`
  record or schedule getter to rename. Verified by reading, no test
  changes.
- #128 (one `portable_json` tag) removed the `streamClosedSerialization`
  alias; all writers and the recognizer now name
  `portableJsonSerialization` directly. Mechanical, same string on the
  wire.
- Gates: Stream pair 28/28 live + 24/24 sim (watcher and standalone
  binaries agree), full-tree build clean.

### Polling-cap enum (Rust #114 `48789db`, 2026-10-26)

- `PollingConcurrency` ADT (`Default`/`Unlimited`/`AtMost`) with
  `pollingPermits` replaces `pollingLimit`; a zero cap cannot be written
  (opaque constructor + `pollingAtMost :: Word32 -> Maybe`,
  mirroring `NonZeroU32`). Settings, Config, and ClientConfig fields go
  from `Maybe` to the ADT with `PollingDefault` defaults; the type rides
  the Transact facade. Unlimited keeps the `maxBound` deviation.
- Tests mirror the oracle's enum unit tests plus a zero-refusal case.
- Slice 3 split: #123 (remove the sysdb `MaxRecoveryAttemptsExceeded`
  error) is a relocation, not a deletion — parking must move from init to
  the dequeue claim/dispatch path — so it follows as its own slice.
- Gates: polling tests 5/5; Postgres group 167/167 twice. Two
  single-occurrence parallel flakes seen once each in untouched code
  (versions timestamp, queue concurrency), never reproduced.

### Park relocation (Rust #123 `bd712ee`, 2026-10-26)

- Parking moved from init to the dequeue dispatch, as the oracle did:
  `initWorkflow` drops its limit argument everywhere (class, Postgres
  backend, Mem/Mock, 50+ call sites swept and re-verified), `finishInit`
  loses the spent-budget branch, and the sysdb
  `ErrorMaxRecoveryAttemptsExceeded` variant goes with its Display, JSON,
  and `shouldRecord` arms (`parkWorkflowSession` removed; the guarded
  `deadLetterWorkflows` statement is the only park write).
- Dispatch mirrors `dequeue.rs`: a three-arm `Admission` ADT
  (`Run`/`NotPending`/`Park`) over the row read after the claim, with the
  status checked first; `Park` dead-letters with the row's own count and
  warns (`DequeuedBudgetSpent`), `NotPending` warns (`DequeuedRowSettled`).
  `maxRecoveryAttempts` stays as the shared constant (never a per-call
  argument — a lone port that parks rows its peers leave alone is the
  worse divergence).
- Adopt and the handle's settled-parked path construct the engine error
  directly, as Rust's `adopt` always did: new
  `TransactError.ErrorMaxRecoveryAttemptsExceeded` (tag stays
  `MaxRecoveryAttemptsExceeded`; the `Error` prefix is the documented
  facade collision deviation, `WorkflowStatus` owning the bare name).
- Placement note: the check lives in `dispatchClaimed`, not
  `spawnRegisteredWorkflowWithRow` — single caller, row and `QueueEvent`
  both in scope, exact oracle file location.
- Gates: sysdb dead-letter live + mem twin; admission unit mirror;
  dispatch-park engine test (row parked, outcome `AwaitedParked 151`,
  body run count 0); render/round-trip cases for the new error and
  events.

### Sim-only watcher + pre-existing trace drift (2026-10-27)

- `test/Main.hs` now enables the 21 sim groups and nothing live, so the
  watcher is fast and never contends with `cabal test` on fixture rows.
- Enabling everything surfaced 4 sim failures that reproduce on the clean
  tree (proven via stash): stale expectations, not regressions.
- Three replay-adopt traces missed a trailing `WorkflowAlreadyOwned`.
  Root cause: slice 1 (`fd9a3a2`) added a silent terminal arm to the run
  path while its start-path twin announces. Restored the emission on the
  run path (matching event prose: "the handle joins the existing run");
  no expectation edits needed there.
- `progress events survive recovery` counted 2 shutdown-cancelled tasks;
  the timeout sweep (also slice 1) is a third legitimate tracked task on
  the relaunch tail. Expectation updated 2 → 3 with the inventory noted.
- The restored emission propagated to one more trace that had pinned the
  silence (`restarting a finished id adopts`); updated with its comment.

### Dual-binary 40P01 (deferred until caught up to Rust, 2026-10-27)

- Standing rule stays: never run two live suite binaries concurrently
  (AGENTS.md rule 8). Triage so far (2026-10-27, no commit):
  - The reported `40P01` did NOT reproduce in 2 dual full-suite runs with
    `log_lock_waits` on: zero server deadlocks, zero lock waits, zero
    sysdb retries. What reproduced instead is cross-binary *progress*
    failure — one binary slow (307 s) with 300 s tasty timeouts on queue
    tests whose rows sat `ENQUEUED`, unlocked, app/version-matching, and
    one stale queue-limits read (`NeverUpdate` returned `Nothing` after
    `AlwaysUpdate` wrote 3; `LeaveExisting` is `do nothing`, names verified
    unique, so no writer found yet).
  - Structural facts established: workflow UUIDs are random per run and
    engines use app-scoped backends, so same-tuple writes across binaries
    are nearly impossible except via NULL-app rows (only cross-visible
    population, ~222 rows, mostly old litter) and the constant
    `_dbos_internal_queue` row. Claim/supervisor scoping is app-narrowed
    (verified in code + DB). Stuck-row trap confirmed by reading only:
    unregistered-dispatch releases the worker slot but never resets the
    row (Dequeue.hs:467-470) — the oracle documents this as known upstream
    hole #24, so the port is faithful here.
  - Load-bearing gaps regardless of trigger: `withRetry` is unbounded
    (Retry.hs:136-151, no attempt cap) so any persistent Transient failure
    hangs instead of failing; ~22k `PENDING`+owned litter rows accumulate
    from interrupted runs (partly ours, from killed runs).
  - Cleanup done: `log_lock_waits` reset to off (verified). Server logs
    (`docker logs postgres`) DO show deadlock DETAIL when it fires — use
    them on the next repro. Repro so far: dual full suites hang 2/2;
    single-test dual loops pass 3/3; legacy-input test hung solo 1/10
    (321 s, then passed — needs explanation).
  - Next steps on revisit: (1) catch a hang live with pg wait-event +
    ungranted-lock sampling (missed twice); (2) explain the CRUD
    NULL-limits read (xmin-level check: overwrite vs delete+recreate);
    (3) decide on bounded retries + unclaim-on-unrunnable-dispatch (needs
    ADR — diverges from the oracle's known hole).

### Registry dual-stack (2026-10-27)

- `RegistryTest` split into `RegistryCases` (10 shared scenarios + pure
  checks, polymorphic over the backend monad) with `RegistryTest` reduced
  to live leaves over the suite backend and new `RegistryTestSim` running
  the identical scenarios over the mock backend (cabal + Main wired).
- Sim validates the deep seams, not just values: registration, snapshot,
  lookup, invocation through a workflow view, and the codec/control
  boundary all execute on both stacks; every sim leaf additionally pins
  its trace silent. Gates: 10/10 live (standalone pattern run) + 10/10
  sim (watcher).

### Read-only transactions (ADR-0021 deferred item, TS oracle, 2026-10-27)
- `TransactionConfig` gains `txReadOnly :: Bool` (default `False`;
  mirrors TS `{ isolationLevel, readOnly }`, the only oracle naming the
  flag). `dsWithTransaction` takes the config; Postgres `BEGIN` emits
  `READ ONLY`; the engine skips checkpoint writes and recorded
  announcements for read-only attempts while the database refuses actual
  writes. No app row ever exists, so every execution re-runs the body —
  safe for reads, volatile reads are the caller's hazard (on the type).
- `pendingTransaction` evaluated and left deferred: no oracle footprint
  and no composition site (select arms are `PendingStep`s by construction;
  no nesting support), so tracking would be speculative. ADR-0021 updated.
- Gates: shared no-record + re-run-replay cases and pure `beginSql` table
  green both stacks (watcher); live write-refusal twin green (standalone
  pattern run).

### Slice 4: owner tokens — init rewrite + dispatch bypass (Rust #117 pars 1–2)

- `creator_xid` (migration 123 already applied; column present, nullable):
  init mints one token per call, writes it always, writes it as `owner_xid`
  only for `PENDING` inserts; conflict arm is a no-op returning the row
  (no attempt/executor touch, no recovery bump — the claim counts, nowhere
  else). `should_execute` compares stored creator to the call's token;
  input written only then; `existing_input` goes.
- `init_workflow` drops `Submission` (oracle deleted it); `WorkflowInitResult`
  drops `initResultExistingInput`, gains `initResultCreatorXid` (renamed
  from `initResultOwnerXid` — the execution token); `Submission` type goes
  if unused; `claimsOwnership`/`initParamIncrement` pruned.
- Claims take one caller-minted token per call into `owner_xid`
  (`start_queued_workflows` family + partitioned); recovery re-enqueue and
  cancel/resume/timeout writes clear `owner_xid` (audit current state).
- Dispatch stops re-submitting through init: `dispatchClaimed` takes the
  claim token, runs from the claimed row directly (admission already
  decides), passes the token to the execution; `spawnRegisteredWorkflowWithRow`
  deleted (was single-called). Required together — creator-semantics init
  would refuse every claimed row.
- Floor 121 → 123 (engine reads/writes `creator_xid` now).
- Gates: creator live tests + mem twin green; claim-token live + mem
  tests green; cancel/resume clearing tests green; floor probe green;
  Postgres group 171/171 + Queue group 40/40 with the bypass live
  (standalone pattern runs); full suite 867/867 (18s, 160MB).
- Flake (root-caused 2026-10-27): `a refused child start replays after the
  key is freed` failed once full-suite (re-run child read as PENDING). The
  live case starts a queued child during a fork's body and then reads the
  row, so a supervisor tick landing in between claims it — the claim, not
  the start, decides the status a reader sees. Now pinned deterministically
  by a sim-only case ("a queued start is pending once the next pass claims
  it"): the queued start reads ENQUEUED, one driven dequeue pass claims it,
  and the next read is PENDING with the body still blocked at shutdown.
  The live case keeps its ENQUEUED assertion; its remaining exposure is the
  same one-second tick, not a product race.
  Third datapoint (2026-10-27, unit-3a full run): same case failed as
  `expected the re-run's child ENQUEUED, got: Success` — the tick claimed
  AND ran the trivial child before the read. Same race one stage later,
  under documented load (`DequeuePassSlow: elapsed_ms 3880`, 2987 queues
  polled per pass — shared-DB hygiene drift, pre-existing). Green in
  isolation. Unit 3a is behavior-neutral on this path (same token text
  through new wrappers); not a regression.
- Review fix (2026-10-27): the dispatch bypass had dropped the database
  clock and timed every claimed deadline from the host's, a regression
  against the database-clock deadline slices. `SystemDB` gains
  `databaseTime` (oracle `database_time`), read **once per batch and only
  when some claimed row has a deadline**; a failed read warns
  (`SysdbClockUnreadable`) and falls back to the host's, as
  `dequeue.rs` `timing_clock` does. Twins: live "databaseTime reads the
  database's clock" + sim "databaseTime follows the simulated clock".
- Review fix 2 (2026-10-27): three comments still spoke the deleted `Submission` vocabulary (`Backend.hs` init-params header, `Statements.hs` input-upsert guard, `Workflow.hs` enqueue header). Reworded to current concepts (caller's status decisions / attempt / claimed row); the `Types.hs` deletion note stays as the record.
- Slice 5 (separate): write fencing (`WorkflowOwner`, `check_owner_on`,
  `WorkflowConflict`, per-domain, `StepNondeterminism`, outcome token). Design: Rust-mirror (required token vs optional caller) over uniform-Maybe — a missed `Nothing` must fail to compile, never silently unfence. Three-oracle verdicts recorded 2026-10-27: stale bodies start and park at first write (all three agree); three-way send split; get_event target+optional-caller vs recv required identity; 25P03+60s idle timeout unanimous (slice 10); #115 self-refusal is Rust-vs-both divergence (dated note at port time). #115 folds into slice 5 (its in-step extension lives in #117).
  - Unit 1 (done): `WorkflowOwner` type, `WorkflowConflict` (shouldRecord False), `StepAlreadyRecorded` renamed to `StepNondeterminism` with oracle Display strings. Gates: Error 26/26, Types 58/58.
  - Unit 2 (done): `checkOwnerTx` helper (SELECT owner_xid FOR NO KEY UPDATE; missing/cleared/mismatch all refuse); `cancelIfTimedOut` cut over to required token, `Right False` preserved on refusal. Gates: Postgres 172/172, sim twin green.
  - Unit 3a (done, full suite 874/875 + 1 known tick-flake at Success stage, green in isolation): token rides `WorkflowState` (reader `workflowOwner`, `withWorkflow`/`executeRegisteredWorkflow` take it); fixtures hold real creator tokens where rows exist (Message/Select/Step/Event/Stream/WorkflowCases/Wait/Sleep/StepRetry/Management/Handle), oracle-mirrored `testOwner` elsewhere. Fidelity fix: sim StepFixture now really initializes rows (it documented recording steps without rows — impossible under a fence).
  - Unit 3b (done, full suite 878/879 + 1 known tick-flake at Success stage, green in isolation): required-token writes fenced — `recordStep`/`recordChildResult` (check+insert+compare in one tx, condemn on conflict), `recordWorkflowOutcome` (owner predicate + clear, unowned adopts), `recordSleep` (threads through fenced step), `recordChildWorkflow`, `recv`/`setEvent`/`writeStream`/`closeStream` signatures threaded (main-tx checks: unit 4), `GetEventCaller` carries its token (no `getEvent` sig change). Mem mirrors judge the same verdicts (incl. new status guard on outcome); Mock stays canned. Fixture rule: raw-SQL rows are stamped deterministic owner+creator (`fixtureOwner`), init-created rows present `initOwnedRow` creators, plants read the row's creator. Twins: stale-step refused + writes nothing, stale-outcome adopts (live + Mem).
  - Unit-3b fallout triaged (all green after): legacy-payload tests needed real creators (`initOwnedRow` helper; forked row claim-stamped); `SelectTest` live fixture threads the init creator; plain-step case reshaped to the oracle's cancel-during-step verdict (refused + records nothing, both stacks); `WaitCases` selects run under a pending waiter scope (settled rows are ownerless — the 120s hang was a winnerless wait, not a deadlock); `StepRetryCases`/`StreamCases`/`EventCases` fixtures return owners. Vacuous passes eliminated (legacy-payload delete test, payload-routing test now write for real).
  - Unit-3b gates (2026-10-27): watcher sims green (`All good (122 modules)`, stale-step/outcome + waiter + select twins OK); standalone binary 878/879 in ~20s (sole failure the known tick-flake, 5th datapoint, isolation-green); psql recency proof 476 `workflow_status` rows (237 SUCCESS / 141 PENDING / 50 ENQUEUED / 27 ERROR / 18 CANCELLED / 2 DELAYED / 1 PARKED) + 42 notifications; suite peak residency 19MB (`+RTS -s`, no leak); oracle `cargo test -p dbos --test ownership` 7/7 read-only — incl. `a_step_that_outlives_a_cancel_records_nothing`, the structural counterpart of the reshaped plain-step case.
  - Unit 4a (done, binary 884/885 + flake 6th-datapoint isolation-green): direct-tx checks on recv-consume, getEvent record, setEvent publish, stream append (Backend opens each tx with `checkOwnerTx`, `join`-flattened continuations; Mem mirrors judge the same, incl. missing-row refusal on streams); twins recv-take-retained / publish-rolled-back / offset-zero (live + Mem, sims green pre-binary).
  - Unit 4b (done): send `Maybe WorkflowOwner` split — Class/Backend/Mem/Mock arities, engine workflow+step sends present `Just (workflowOwner …)`, Client/Instance pass `Nothing`; Mem judges the check inside its delivery snapshot with the tuple-match verdict; live+Mem stale-send twins (refused + delivers nothing, owner's send lands).
  - Unit 4c (done): `deleteWorkflows` owner check via `deleteWorkflowsWithCaller` (caller+owner pair gates a `checkOwnerTx` inside the delete tx; self-delete check first); Backend/Mem/Mock/Client arities; live+Mem stale-delete twins (refused + row survives). Other mgmt optionals deferred with dated note — sole reachable Just-caller path.
  - Unit 4d (done): init-caller fence — `InitWorkflowCaller.initCallerToken` (engine presents `ownerToken wctx`), Backend opens the child-start tx with `checkOwnerTx` on the parent, Mem judges the same; live+Mem stale-parent twins (refused + no child row).
  - Unit 4e (#115, done): self-target refusal — `Statements.refuseSelfTarget` shared helper (direct name check, `InvalidInput`), Backend cancel (direct + cascade-with-root-naming via `rootContaining`, pre-write) and resume (direct, pre-write), Class `fromWorkflow` params, Management `cancel/resumeWorkflows` + InWorkflow `Just (workflowId wctx)`, Mem mirrors (`memRootContaining`), Mock arities, Client/Instance `Nothing`; live+Mem twins (self-cancel / self-resume / cascade-names-tree). Divergence note (2026-10-27, Class.hs): Rust refuses, TS/Python allow — Rust wins by default.
  - Unit 4e gates (2026-10-27): watcher sims green (`All good (122 modules)`, all three Mem refusal twins OK); standalone binary 896/897 in ~19s (sole failure the known tick-flake, 7th datapoint, isolation-green); three live refusal twins green in binary (live + Mem each); psql recency proof 952 `workflow_status` rows (474 SUCCESS / 281 PENDING / 103 ENQUEUED / 54 ERROR / 38 CANCELLED / 2 MAX_RECOVERY) + 87 notifications; oracle `cargo test -p dbos --test ownership` 7/7 read-only. Note: binary built before two comment-only Class.hs edits (divergence + deferral notes); behavior-identical, watcher green on the final tree.
  - Review fix 1 (2026-10-27, root cause): `deleteWorkflows` took the caller id and the fence token as two independent arguments, so the row the self-delete guard named and the row the fence locked could disagree (an unfenced delete when the pair was inconsistent, and `(Just caller, Nothing)` skipped the fence). The duplicate source of one fact was the defect, not a missing guard: the Class signature is now `… -> Bool -> Maybe WorkflowOwner -> …`, one value naming both the refused row and the token, and the unused caller step id is gone (Management, Client, Mem, Mock, tests follow). Red proof: the signature change plus `ghcid.txt` naming every site that had to change (Backend body, Management ×3, Client, Mem/Mock instances, PostgresTest ×3, IOSimTest ×2, EventCases, WorkflowTest) before the fix; green after.
  - Review fix 2 (2026-10-27, root cause): the sim's `setEvent`/`writeStream` judged the replay adopt *before* the ownership fence, while live and the oracle (`set_event`, `write_stream`) fence first — same input, different verdict, and a stale execution was handed the recorded answer in the sim only. Both Mem mirrors now open with the check and keep it in the same transaction as the write. Red proof: two new Mem cases (`a stale token's replayed publish/stream write is refused`) failed with `Right ()` before the fix and pass after; live twins added to pin the same verdict on both stacks, plus `a stream write to a missing workflow is a conflict` (live + Mem) pinning the verdict the fence now decides, with the FK mapping kept as the oracle keeps it and noted as defensive. During the fix the watcher also caught two orphaned bodies left by long-block edits (duplicate appends, `["a","a","b","b"]`); both removed and the stream sims are green again.
  - Review-fix gates (2026-10-27): watcher green (`All good (122 modules)`, 89/89 Mem tests, both replay refusals + missing-row pin OK); standalone binary 902/903 in ~20s, sole failure the known tick-flake (8th datapoint, isolation-green); the six new/changed live twins re-run individually green on both stacks (replayed publish, replayed stream write, missing-row stream conflict, stale delete, delete payloads, self-delete); psql recency proof 482 `workflow_status` rows (238 SUCCESS / 145 PENDING / 49 ENQUEUED / 29 ERROR / 18 CANCELLED / 2 DELAYED / 1 MAX_RECOVERY) + 44 notifications.
  - Review-fix follow-ups (2026-10-27, findings 3-7): stale `setEvent` fence comment deleted (it claimed the token was inert); the duplicated `InitWorkflowCaller` record-update block in `startChildWorkflow` collapsed to one construction; IOSim's duplicated `WorkflowInitResult` import item and doubled `rows <- readTVarIO` in Mem `resumeWorkflows` removed; `WorkflowConflict` added to the `sysdb/error.rs` inventory in `docs/rust-oracle-seam-audit.md` (17 Rust variants + the Haskell-only `MaxRecoveryAttemptsExceeded`); Mem `recordSleep` gained the live checkpoint's `StepNondeterminism` adopt arm so the check-then-record race judges the same verdict in both stacks.
  - Slice gate (2026-10-27, final): watcher green (89/89); binary 902/903 in ~20s, sole failure the known tick-flake (9th datapoint, isolation-green); psql recency proof 957 `workflow_status` rows (471 SUCCESS / 288 PENDING / 101 ENQUEUED / 56 ERROR / 37 CANCELLED / 2 MAX_RECOVERY / 2 DELAYED) + 88 notifications.
  `WorkflowConflict`, per-domain, `StepNondeterminism`, outcome token).
  Later: #127 receivers, #113 idle timeout.

### Pending oracle commits (reviewed 2026-10-28, unported, in this order)

- Slice 6 (#106 `739f985`, version-index lookups, done): vendor migrations 124/125 + `highest_version(SOURCES)` (ported the oracle's computed ceiling, not a bump — listing a file raises it), `make db-migrate` 123→125, ceiling probe 123→125, `latestVersionNameStatement` retired for split `latestVersionNamedStatement`/`latestVersionUnnamedStatement` (UNION ALL owned+unclaimed probes / single timestamp probe) with `latestVersionNameTx` for the two dequeue checks and rewritten `latestApplicationVersionSession`; factored `versionRowDecoder`. Mem untouched (same verdicts, no indexes). Twins: live "each application sees its own and unclaimed versions" (30k planted rows in oracle thirds, app-membership assertions — exact names are unassertable on a shared DB — plus EXPLAIN pins for both index names, rows deleted after).
- Slice 6 gates (2026-10-28): watcher sims green (89/89); new test green in isolation; versions+client groups 5/5 ×5; full binary 904/904 once, then 903/904 + 902/904 with the client-latest far-future flake (see below); psql recency proof 3481 `workflow_status` rows (1720 SUCCESS / 997 PENDING / 430 ENQUEUED / 189 ERROR / 134 CANCELLED / 7 MAX_RECOVERY / 4 DELAYED) + 338 notifications; oracle `cargo test -p dbos --test migrations` 30/30 read-only.
- Slice-6 fallout, both exonerated: (1) first version of the new test asserted exact latest names and failed on concurrent rows — redesigned to app-membership + EXPLAIN pins with ancient stamps and end-delete (zero blast radius: rows never win newest-reads); red proof for the EXPLAIN discriminator via psql (old shape walks 9973 rows past peers, never touches the owner index). (2) client-latest (`a client reads the latest version of its own application`) failed 2/4 full runs on the far-future window of the neighbouring versions test — stash A/B (baseline binary, no slice-6 tree) fails identically, so pre-existing, not slice fallout. Isolation-green 4/4.
- Slice 7 (#133 `99677cc`, WorkflowKey naming): `methodWorkflowKey` constructor; `EnqueueOptions` loses `className`/`configName`, `clientEnqueueWorkflow*` takes `WorkflowKey`; engine `AlreadyRegistered`/`NotRegistered` carry `WorkflowKey` (render + JSON). Compiler-driven sweep.
- Slice 8 (#132 `3abe8a5`, patch/deprecate_patch): new `Patch` module (PendingStep-shaped, per-poll placement checks), `PATCH_PREFIX` in step_names, peek+CAS over `nextStepIdRef`, marker via fenced `recordStep`, `PatchNondeterminism` engine error; live+Mem twins of the 668-line oracle test. Biggest of the three; shares slice 5's fenced `recordStep`.

### Child-start replay + fork replacement (Rust #108 `2540e18`, #107 `547eb9f`, 2026-10-20)

Ported in one slice:

- A child start's `QueueDeduplicated`/`ConflictingWorkflow` refusal is
  recorded at the parent's step (`recordStartError`, through the workflow
  serializer) with no child id; a replay's `checkRecordedChild` returns the
  recorded error without trying the start again. The recorded value is told
  apart from a plain step's error by its single-key shape
  (`isRecordedStartError` over `recordedStartErrorNames`), so an unreadable
  start error reports itself, naming the step, instead of `UnexpectedStep`.
  `initOrJoin` ports the oracle's `init_or_join`: a `ReturnExisting`
  collision joins the holder; a holder that finished between the losing
  insert and the read is retried (the oracle's `None => continue`), not
  recorded as a refusal; and a holder-lookup failure stays on the sysdb
  channel, so only a genuine refusal is recorded.
- `controlOf` consults `shouldRecord`: typed system-database refusals are
  outcomes (the parent that returns one ends `ERROR`), while execution
  signals and unreadable/substrate failures stay control. Rust's
  `Error::control` had this gate; the port was missing it, and the new
  conflict test exposed it.
- Fork refuses an empty `replacement_children` list in `forkOptionProblems`
  (engine and sysdb), and the in-memory backend applies the replacement map
  to its copied steps.

Evidence: shared live/sim `scenarioDedupStartRecorded` and
`scenarioConflictStartRecorded` (with typed trace assertions on the sim),
the pure `startErrorCase`, `TypesTest`, and the live/sim fork-replacement
case; `Workflow execution` 61/61 live, `WorkflowSim` 60/60, full suite
852/852, `cabal build all` green, `make db-migrate` 123→123. psql:
`workflow_status` shows the conflict parent and its replay `ERROR`, the
dedup parent/replay `SUCCESS` and the re-run child `ENQUEUED`;
`operation_outputs` step 0 holds `{"QueueDeduplicated":…}` on the parent and
its replay fork, and the re-run fork's step 0 names its child id. The
client's fork surface is a pass-through to the engine
(`clientForkWorkflows … Management.forkWorkflows`), so the oracle's
client-side replacement assertion is not duplicated; the engine cases own
the semantics.

Oracle leg green with OrbStack's socket
(`DOCKER_HOST=unix://$HOME/.orbstack/run/docker.sock`):
`cargo test -p dbos --test children` 28/28, plus the slice's other touched
suites `--test workflows` 13/13 and `--test management` 34/34.

### S1 — Read every payload by its row's `serialization` tag (Rust #95, `52d0f9b`)

Rust: `PayloadEncoding` + `PayloadEncoding::of` (NULL/`""`/`rust_serde` →
native; `portable_json` → portable; anything else → `UnknownSerialization`,
never a guess); `decode_tagged` at every read; a workflow's outcome carries
its format; recovery and dequeue run a row under the row's tag; a fresh start that finds an existing row **no
attempt owns** runs the row's own input *and* tag (it reads them in the
`init_workflow` transaction); a schedule's untagged context stays native.
Behavior-neutral: Rust still writes only `rust_serde`.

Port status (2026-10-19): dispatch landed with S2 under ADR-0038. The codec
reads NULL/`""`/`rust_serde` as native and `portable_json` as portable, and
every decode site passes the row's tag (`Registry`, `Step`, `Select`,
`Datasource`, `Event`, `Stream`, `Message`, `Wait`). `UnknownSerialization`
landed with S3 on both channels (`SystemDB.Error` and `Transact.Error e`,
mapped 1:1 by `fromSystemDB`, control on both). The fresh-start-on-an-existing
row case is done: `init_workflow` carries `initResultExistingInput`
(`Maybe (Maybe Text)`, read in the claiming transaction) and the three start
paths run `runInputAfterInit` — the stored input and tag when the init found
an unowned existing row, otherwise the caller's. The schedule context is
untagged by construction: `ScheduleRecord.scheduleRecordContext` is the row's
raw JSON text and is never dispatched through the payload tag (the schedule
tests round-trip it), so there is no tag to read. S1 is complete.

- Do: add `PayloadEncoding` + `payloadEncodingOf`/`decodeTagged` to
  `DBOS.Transact.Serialization`; dispatch at every decode site: `Registry.hs:142`
  (argument), `Handle.hs:229-243` (result), `Step.hs:239` (replay),
  `Select.hs:139`, `Datasource.hs:393`, `Event.hs:117`, `Stream.hs:193,219`,
  `Message.hs:180`, `Wait.hs:78`; recovery/dequeue under the row's tag
  (`Recovery.hs`/`Dequeue.hs`); the init-row case above (`Workflow.hs:107-130`
  currently runs the caller's `input`, not the stored row's — verify and fix);
  add `UnknownSerialization` to `SystemDB.Error` and `Transact.Error e` with
  the boundary mapping; on a recorded step error only *undecodable bytes*
  degrade to `StepFailed` (Rust's review fix). Done 2026-10-19: dispatch,
  `UnknownSerialization`, and the init-row input (`initResultExistingInput` +
  `runInputAfterInit`).
- Tests: port `crates/dbos/tests/serialization.rs` (187 lines) and the
  `sysdb.rs` tag cases (49). Owners: `SerializationTest.hs`, `TypesTest.hs`,
  plus the per-domain suites for input/output/event/message/stream/await
  reads. Pin the two fixed `portable_json` writers on the read side (sleep
  wake time `Backend.hs:1455`, the stream-close sentinel `Types.hs:739`).
- Validate: watcher pair for the read-path group; the oracle suite above;
  psql `select workflow_uuid, serialization from dbos.workflow_status …` for
  an untagged/empty-tag fixture written raw; a fork onto a row tagged
  `pickle` expects `UnknownSerialization` (engine channel, not a retry).
- Docs: correct `Serialization.hs`'s header claim ("Haskell-written rows
  readable by Python DBOS … no divergence") — Python refuses `rust_serde`
  (`_serialization.py:270-289`); the interop doc gains the tag-dispatch table.

### S2 — `portable_json` (Rust #97, `1cb3983`)

Portable JSON is the cross-SDK wire format and it is plain aeson JSON — no new
codec, no byte-exactness machinery. Python reads it (`portable_json`,
`_serialization.py`), so the contract is the JSON shape and phase P proves it.

Port status (2026-10-19): done in the simplified form ADR-0038 records —
every payload is tagged `portable_json`, workflow arguments use the envelope,
errors use the portable record, the codec is internal to
`DBOS.Transact.Serialization`, and there is no `SerializationFormat` surface
and no serializer selection. The original S2 plan below is historical.

- Workflow input is the envelope `{"positionalArgs": […], "namedArgs": {…}}`.
  Reads: one positional → the input value, named-only → a record, several
  positionals → a tuple; `()` is no arguments, a JSON `null` is one `None`.
- Every other payload (result, error, event, message, stream value) is a plain
  JSON value tagged `portable_json`; an error written `{name, message, code,
  data}` reads back structured when the reader's error type matches, else as a
  message-bearing portable error.
- `SerializationFormat {Default, Portable, Native}` per registration, per
  enqueue, and per event/message/stream write; `Default` defers: a start takes
  its registration's choice, an in-workflow write the workflow's, a client its
  configured serializer.
- Do: `SerializationFormat` in `Transact.Serialization`, carried through
  `Registry.hs`, `RunOptions`/`StartOptions`/`EnqueueOptions` and the write
  calls; envelope + error encode/decode; `clientEnqueueWorkflowPortable`
  writes a raw envelope for a callee in another language; `Handle` decodes by
  the recorded format; scheduled runs take the registration's format (hook it
  in the schedule sysdb layer although the engine exposure stays deferred per
  ADR-0036); a portable error variant in `Transact.Error`.
- Tests: new `PortableCases.hs` + live/sim pair — envelope both directions,
  the read shapes above, unit-is-none, a portable child's error replayed by
  its parent, the same value written/read under both formats.
- Validate: psql the stored `workflow_input.inputs` and
  `workflow_status.serialization` on a portable fixture (the envelope is the
  contract; phase P1 confirms Python reads it); oracle suite `portable`.
- Docs: `docs/cross-language-schema-interop.md` gains the envelope + error
  contract; gap 7 in `docs/python-test-gap-analysis.md` is unblocked here.

### S3 — Record a management result under the tag its bytes are in (Rust #104, `0f1dd66`)

Rust: a management call's result was recorded by the sysdb itself — JSON bytes
tagged `portable_json` whatever the row said — so the tag promised more than
the bytes delivered and replay ignored it. The recording now carries the
format its bytes are actually in, replay decodes by the tag, and an unreadable
tag is `UnknownSerialization` mapped back to the engine error (a control error
would make the workflow retry until recovery parked it). Sleep's wake time and
the stream-close sentinel keep their fixed portable tag.

Port status (2026-10-19): done. The sysdb layer gained
`UnknownSerialization` (render `serialization N is not available`,
`shouldRecord` false) plus `encodePortableError`/`decodePortableError`; the
engine carries the same variant, `fromSystemDB` maps it 1:1, and `controlOf`
treats it as control. `recordCallerOutcome` writes the portable record
`{name,message,data}` under `portable_json`; `checkCallerStep` reads
`stepCheckSerialization` and dispatches before replaying (NULL/`""`/
`rust_serde` native, `portable_json` portable, anything else refused). The sim
mirror (`verifyCallerStep`) does the same. Pinned by `PostgresTest` (portable
write, replay, and a `pickle` tag refused), a pure `ManagementTestSim` tag
case, and the `ErrorTest`/`SerializationTest` taxonomy cases; oracle
`management` green.

- Do: tag those sites with the format their bytes are in (the port has one
  codec today, so `rust_serde`), record a refusal under the same tag, decode
  replay by the tag, map an unreadable tag to the engine-level
  `UnknownSerialization`, and keep the two fixed portable writers (sleep's
  wake time `Backend.hs:1455`, the stream-close sentinel `Types.hs:739`).
- Tests: a management result round-trips through replay; a row tagged `pickle`
  reports `UnknownSerialization` on the engine channel. Owner:
  `ManagementTest` (+ its sim mirror where the case is dual-stack).
- Validate: oracle `cargo test -p dbos --test management`; watcher
  `ManagementTest`/`ManagementTestSim`; psql the
  `operation_outputs.serialization` of a management checkpoint.
- Depends on S1.

### S4 — Step listing: `load_output` and paging (Rust #102, `9c05128`)

Rust: `ListWorkflowStepsOptions {load_output = true, limit, offset}` on
`list_workflow_steps_with` (DBOS and Client); `list_workflow_steps` keeps
loading everything; outputs off means the payload columns are left out of the
query (a failed step then looks like a successful one — documented); the
statement moves into a named query so a unit test checks the SQL actually
sent.

Port status (2026-10-19): done. `ListWorkflowStepsOptions {loadOutput, limit,
offset}` (default reads everything) rides `listWorkflowStepsWith`,
`listWorkflowStepsWithInWorkflow`, and `clientListWorkflowStepsWith`; the
existing `listWorkflowSteps`/`clientListWorkflowSteps` delegate with the
default. The statement now picks one of two SQL variants (`listStepsSql`):
the metadata variant never selects `output`/`error` (the decoder fills both
`Nothing`), and `limit $2 offset $3` pages in step order. Pinned by a pure
SQL test, shared live/sim scenarios (outputs left out; five-step paging) and
the client surface in `ManagementTest`; oracle `management` 33/33. The port
reports `StepOutcomeNotLoaded` for the outputs-off read (the oracle's silent
success is documented there), distinguishes an in-flight row as
`StepOutcomePending`, and replays the in-workflow listing from a recorded
`(loadOutput, [StepRecord])` snapshot.

- Do: `ListWorkflowStepsOptions` (+ `defaultListWorkflowStepsOptions`),
  `listWorkflowStepsWith`/`listWorkflowStepsWithInWorkflow` and
  `clientListWorkflowStepsWith`; facade exports; a statement unit test
  asserting the rendered SQL leaves the payload columns out and binds
  `LIMIT`/`OFFSET`.
- Tests: port `management.rs`'s two cases — outputs left out, and paging five
  steps on both surfaces.
- Validate: oracle `cargo test -p dbos --test management`; watcher
  `ManagementTest`/`ManagementTestSim`; psql `operation_outputs` for the paged
  fixture.

### S5 — Client `use_listen_notify` defaults to false (Rust #101, `0b2adef`)

Rust: `ClientConfig::new` sets `use_listen_notify = false` (with the listener
on against executors that don't push, `get_event` waits out the 60-second
fallback); the field's docs say when to turn it on; the state-based tests
replace the LISTEN-counting test.

Port status (2026-10-19): done. `clientConfigNew` sets `False` and the field
is documented at its site as accepted-but-inert (the port polls; there is no
listener to turn on). `ClientTest` pins the default. No behavior test exists
to write — the port cannot observe a listener it does not have — so the pin is
the contract.

- Do: default `False`; document the field at its site (port polls regardless;
  the field is accepted for parity). No behavior test exists to write: the
  port cannot observe a listener it does not have — record `-- N/A:` with the
  reason in `ClientTest` rather than inventing one.
- Validate: oracle `cargo test -p dbos --test client`; `cabal test all`;
  pin the default in `ConfigTest`/`ClientTest`.

### S6 — Migrations 43/44 apply on any Postgres (Rust #100, `2ef88ea`) + re-vendor

Rust: 43/44 drop the streams/workflow_events triggers but applied only when
notify was on, so a database migrated with notify on and later taken past 44
by a notify-off process kept both triggers with the migrations recorded as
applied. Both now apply on every PostgreSQL database (`IF EXISTS` makes them
no-ops where the triggers never existed; CockroachDB never had them);
databases already past 44 are not repaired. The migration docs and the
notify-off regression test now check the two trigger *functions* as well.

Port status: `rust-migrate/src/migrations/mod.rs` now lists 43/44 as
`Applies::Postgres`, and both SQL files carry the "applies on every
PostgreSQL database" header (re-vendored 2026-10-19 from `2ef88ea`). The
ceiling stays 121. Validated the same day: `PostgresTest` pins the absence of
both triggers/functions and the survival of the notifications pair,
`make db-migrate` is 121→121 twice, oracle
`cargo test -p dbos --test migrations` is 27/27, psql shows only
`dbos_notifications_trigger`/`notifications_function`, and the full suite is
836/836.

- Do: re-vendor the two SQL files and the `mod.rs` entries from `0f1dd66`
  (`WhenNotify` → `Postgres`), plus any runner doc the commit touched; ceiling
  stays 121.
- Tests: a schema probe that both triggers and both functions are absent
  after migration (`pg_trigger`/`pg_proc`), and `make db-migrate` twice
  (idempotent).
- Validate: oracle `cargo test -p dbos --test migrations`; `make db-migrate`;
  psql probes.

### S7 — Non-finite floats (branch `fa48de8`…`e2fed10`)

Rust: `encode` refuses a non-finite f32/f64 anywhere in a value (portable and
native alike at first), then `rust_serde` alone round-trips them as the JSON
strings `"NaN"`/`"Infinity"`/`"-Infinity"` (a lenient deserializer turns a
name into a float only where a float is asked for — a `String` field holding
`"NaN"` stays a string; buffering targets like `Value`/`untagged`/`flatten`
see strings), `portable_json` keeps refusing (the SDKs agree on no form);
float map keys and serde_json's arbitrary-precision numbers are read;
non-`encode` value paths (portable error fields, DBOS's own error variants, a
schedule's context) go through the same float rule because a `Value` cannot
hold a non-finite.

**Measured port reality (aeson 2.3.2.0, checked 2026-10-18):**
`encode (0/0 :: Double)` = `null` (the value is already gone),
`encode (1/0)` = `"+inf"`, `encode (-1/0)` = `"-inf"`, and reading back gives
`Right Infinity` for both spellings but `Left` for `"NaN"`, `"Infinity"`,
`"-Infinity"`. Aeson collapses NaN inside `toJSON`, before any port code or
serializer sees the value, and there is no `serialize_f64`-style hook to
refuse it. Re-checked 2026-10-19: `fromJSON Null :: Result Double` is
`Success NaN`, while `Result (Maybe Double)` is `Success Nothing` — the
optional path cannot distinguish stored `null` from NaN.

Port status (2026-10-19): decided as option A (ADR-0039). `SerializationTest`
pins the aeson facts (NaN→`null`, ±Inf→`"+inf"`/`"-inf"`, a required `Double`
reads `null` as NaN, `Maybe Double` reads `Nothing`, Rust's names are text) and
the envelope/record shapes; the shared `scenarioNonFinite` writes a
`-Infinity` argument and a `(+Inf, NaN)` result through the engine on both
stacks, and the live row's stored text is `{"positionalArgs":["-inf"],
"namedArgs":{}}` / `["-inf","+inf",null]`. Oracle branch `e2fed10`
`serialization` is 5/5; full suite 843/843.

- Decision (ADR-0039, accepted 2026-10-19): (A) keep `ToJSON`, document the
  deviation table, and pin the observable facts — NaN writes `null`, a
  required `Double` reads `NaN`, `Maybe Double` reads `Nothing`, ±Inf use
  aeson's spelling, Rust's names are unreadable — **chosen**; (B) replace
  `ToJSON` with the port's own payload class carrying a failure channel was
  rejected as app-wide churn with no portable-format gain.
- Do under (A): doc notes at `encodeWorkflowValue`/`decodeWorkflowValue`; **no**
  text-level rewrite of the names (it cannot tell a float from a `String` field
  holding the same text — the exact confusion Rust's typed deserializer
  avoids — so record why it is declined); the schedule context and both error
  paths documented as `Value`-limited; the `record_step_output` comment
  corrected as Rust corrected it.
- Tests: port the halves of `crates/dbos/tests/serialization.rs` (65 new
  lines) that hold under the chosen option; pin the aeson facts above so the
  deviation is visible rather than silent.
- Validate: oracle `cargo test -p dbos --test serialization` on the branch;
  psql the stored text of a NaN/±Inf payload; phase **P3** measures the
  cross-SDK consequence (Python reads `"+inf"` as a string where Rust refuses
  the write).
- `185fdca` (`FloatVisitor` naming) and `18659e7` (envelope probes moved into
  `serialization/shape.rs`) are oracle-internal refactors — no port work;
  note them as N/A with the reason.

### P — Haskell↔Python interop validation (after S2; repeat after S7)

Harness lives **outside the repo** (e.g. `~/.local/share/dbos-interop/`),
Python from `~/dev/dbos-transact-py`, one `DBOS_DATABASE_URL` for both sides;
Python's `DBOSClient` never migrates (`_client.py:157`), so the port's ceiling
stays authoritative.

- **P1 — Haskell writes, Python reads.** Run a portable workflow from Haskell
  (input with positional + named args, an event, a stream value, a message, a
  result); a Python script reads it back (`client.retrieve_workflow`,
  `get_event`, `read_stream`, `list_workflow_steps(load_output=...)`) and
  asserts the envelope shape and values. Failure modes to watch: the port must
  write `portable_json`, not `rust_serde` — Python refuses the latter
  (`TypeError: Serialization rust_serde is not available`).
- **P2 — Python writes, Haskell reads.** A Python script enqueues a portable
  workflow (positional + named args) and sets an event/stream; the port claims
  and runs it under the same app name/version. Plus an **in-repo** golden-row
  test: raw-SQL insert of Python's `GOLDEN_INPUTS_JSON`
  (`tests/test_interop.py:31-35`) tagged `portable_json`, and the port runs
  it — no Python needed at test time, so the pin survives in CI.
  (2026-10-19: the in-repo half landed as `test/DBOS/Transact/InteropTest.hs`:
  a canonical portable write plus raw Python canonical/namedArgs rows, with
  the missing-`namedArgs` tolerance fix. The out-of-process Python script
  stays under P1.)
- **P3 — negatives and non-finite pins.** Python refuses `rust_serde` rows (so
  native rows are SDK-local by contract); a Haskell portable ±Inf payload is
  read by Python as the string `"+inf"` (S7's measured consequence) — capture
  it as evidence for the S7 decision.
- Evidence per run: the psql row dump (`inputs`, `output`, `serialization`),
  the Python assertions, the port's log lines. Nothing beyond the golden-row
  test is committed; `docs/cross-language-schema-interop.md` records the
  contract and the ceiling (still 114 there — correct it in S1/S2).

Port status (2026-10-19): P2's in-repo half is `InteropTest`; P1/P3 ran
out-of-process with Python's `.venv` and `DBOSClient` against the same
`DBOS_DATABASE_URL`: the Haskell canonical row read back its result dict,
`interop_status` event, `interop_stream` value, and a two-step workflow's
portable outputs (`one`=22, `two`=44). The non-finite row read back as
`["-inf","+inf",null]` with both infinities as Python `str` (P3). The harness
lives outside the repo.

### Interop seam audit (2026-10-19, Python skill as oracle)

Ran against the shared `dbos` schema (123) with a Haskell GHCi executor and a
Python `DBOS`/`DBOSClient` in IPython; the scratch harness lives outside the
repo. The Python skill references (`advanced-serialization`,
`advanced-shared-database`, `client-enqueue`, `comm-*`,
`workflow-introspection`, `workflow-control`) are the seam list.

Closed in this sweep:

- **Facade surface.** `DBOS.Transact` now exports `getAllEvents`,
  `getWorkflowRecord`, `clientWorkflowRecord`, `listQueuedWorkflows`,
  `WorkflowRecord (..)`, `WorkflowFilter (..)`, `defaultWorkflowFilter`,
  `Applications (..)`, `Timestamp` (`timestampFromEpochMs`/`timestampToEpochMs`),
  `VersionInfo (..)`, `RateLimit (..)`, and the codec entry points (`CodecError (..)`, `encodeWorkflowInput`,
  `encodeWorkflowValue`, `decodeWorkflowValue`); `getAllEvents`,
  `getWorkflowRecord`, and `listQueuedWorkflows` are pinned in
  `InteropTest`, the types by use in the facade.
- **Portable error message.** Closed: `ErrorApplication Text` now writes the
  payload text as `message` (`"boom"`, not `"\"boom\""`), so Python's
  `PortableWorkflowError.message` reads `boom`; pinned by `SerializationTest`
  and a Python read-back of a Haskell error row.
- **Typed reads preferred.** Closed: all public step listings return the
  typed `StepInfo` outcome ADT; the tag-preserving `StepRecord` loops are
  internal for backend/engine/tag tests. The management/client tests assert
  `StepInfo` (names, ids, not-loaded, decoded, portable failure) and a pure
  conversion test pins the `py_pickle` output/error as explicit undecodable
  outcomes. `clientGetEvent` and instance `getWorkflowEvent` are typed; their
  `...Serialized` forms are internal.
- **Haskell → Python.** Canonical result/event/stream/steps, the non-finite
  strings, a raw Python error row (`name`/`message`/`data`), and a Python
  `DBOSClient.enqueue` of a **Haskell-owned** workflow
  (`application_name="repl-hs"`, `serialization_type=PORTABLE`, queue
  `repl-q`) which the Haskell executor dequeued and ran; Python read
  `{'from':'haskell','n':7,'ok':True}`.
- **Python → Haskell.** A portable Python workflow's result read by
  `clientRetrieveWorkflow` + `handleResult`, status by `clientWorkflowStatus`,
  event by `clientGetEvent` (raw `SerializedWorkflowValue`, decoded with
  `decodeWorkflowValue`), and steps by `clientListWorkflowSteps`.
- **Demo boundary.** `demo-apps` no longer depends on
  `dbos-transact-hs:dbos-transact-internals`; every demo import is
  `DBOS.Transact`. The sublibrary stays `visibility: private`, is still a
  test-suite dependency, and `cabal build exe:demo-apps` is green.

Open bugs/gaps:

- **Schedules.** Python's `create_schedule`/`list_schedules`/`get_schedule`/
  `pause`/`resume`/`delete`/`apply_schedules`/`backfill`/`trigger` have no
  Haskell client/engine/facade twin (ADR-0036 defers exposure). Schedule
  contexts are not interoperable: the port stores raw JSON text, while
  Python's scheduler deserializes every shared `workflow_schedules` row with
  its own tagged serializer and crashes on a Haskell row (`binascii.Error:
  Incorrect padding`). Clear/migrate those rows before launching Python, or
  port the schedule context format.
- **Step payload asymmetry.** Python step outputs stay native (`py_pickle`)
  even inside a `PORTABLE` workflow — observed `py_step` output `gARLAy4=`
  with `stepRecordSerialization = Just "py_pickle"` — while the port writes
  portable step outputs. Python reads Haskell steps (P1); Haskell cannot
  replay/fork a Python workflow's native steps. All public listings return
  the typed `StepInfo`; the raw `[StepRecord]` is internal. The outcome ADT
  separates not-loaded, absent, decoded, and undecodable payloads, and a
  portable vs undecodable error. The Rust/TS/Python/Haskell field mapping is
  in `docs/cross-language-schema-interop.md` § “Step records vs StepInfo”.
- **`rename_application`.** The sysdb method exists, but there is no client or
  facade wrapper; Python's `DBOSClient.rename_application` ownership transfer
  (and `adopt_unclaimed_rows`) has no interop surface here.
- **In-app cross-application enqueue.** `enqueueWorkflow` takes a
  `WorkflowRef` and stamps the executor's own application; Python's
  `enqueue_workflow_with_options` accepts `application_name`/`app_version`.
  Only `DBOSClient` exposes the cross-application options today.
- **`read_stream_offset`.** Python's one-value wait at an offset has no
  direct twin; `readStreamFrom`/`clientReadStreamFrom` are readers from an
  offset.
- **Error data.** A foreign portable error's `data` decodes into the caller's
  `Error e` only when the shape matches; otherwise `decodeErrorText` falls
  back to `name: message`. Structured cross-SDK error payloads are therefore
  best-effort by design.

### Deviations to record while porting

- S1: `UnknownSerialization` never guesses; NULL/empty tag = native (pre-tag
  rows); the `Serialization.hs` "readable by Python" claim is wrong today.
  2026-10-19: the header now states the portable/legacy/unknown rule, and the
  engine channel treats the variant as an outcome — only the registry's
  workflow-input decode maps it to control (`FailureControl`).
- 2026-10-19: a plain start of an existing `ENQUEUED`/`DELAYED` row joins it
  with a polling handle; only a dequeue claim executes it, because an
  unclaimed execution cannot record an outcome. Pinned by
  `scenarioQueuedStartPolls` on both stacks.
- S2 (2026-10-19): superseded by ADR-0038 — no `SerializationFormat`; the
  envelope + portable error record are the public contract. The codec module
  stays internal; the facade re-exports its entry points for demo apps
  (ADR-0038 addendum), and demo-apps no longer depend on the private
  internals sublibrary.
- S3: a management checkpoint's tag decides its reader.
  2026-10-19: landed with the listing replay snapshot; an unreadable tag is an
  outcome in the listing (`PayloadUndecodable`/`FailureUndecodable`) and a
  control signal only for a workflow input.
- S7: the aeson deviation table above — ADR required whichever option lands.
- Out of scope (2026-10-18): Rust #99's custom serializer — Python interop
  rides `portable_json` and no application here defines its own codec, so the
  port stays `Serializer = RustSerde`.

## Constraint-audit actions (2026-10-16)

Audit of the Python skill's Lifecycle/CRITICAL, Workflow/CRITICAL,
Step/HIGH, Queue/HIGH constraints against the port; tables in
`.lavish/dbos-python-constraint-audit.html`. Tree audited: `ad906e0` +
the 19-file uncommitted stack. Fix in this order, red-first where a pin
holds the wrong behavior:

- [x] B1 queue over-validation: drop `partitionRate`'s cross-check in
      `validateQueueOptions` (`Queue.hs:252, 271-277`); Rust compares
      concurrency only (`queue.rs:455-461`). Repoint the pins at
      `QueueCases.hs:270` and `:349` to acceptance.
      Done 2026-10-16, uncommitted — plus the missing fourth bound
      (`partition_worker_concurrency <= concurrency`, both oracles refuse;
      new `incoherentTable` row pins it).
- [x] B2 both concurrency spellings: refuse `concurrency` +
      `globalConcurrency` both-set with Python's message
      (`_queue.py:148-151`); flip `scenarioConcurrencySpellings`
      (`QueueCases.hs:414-441`).
      Done 2026-10-16, uncommitted — including the `queueOptionsAfterChange`
      canonicalization (merged options name one spelling) and the
      change-level both-set refusal, with the `scenarioConcurrencyUpdate`
      migration case.
- [x] B3 empty first-wait mirror: `MemSystemDB.awaitFirstWorkflowId []`
      refuses like live (`Backend.hs:2596`) instead of hanging
      (`IOSim.hs:1250`); annotate the mock placeholder pin
      (`IOSim.hs:186`, `IOSimTest.hs:161`); add the shared
      executor-level `waitForFirstWorkflow` empty-set case (also
      closes G5's wait coverage).
      Done 2026-10-16, uncommitted — three shared executor-level cases
      (first-empty refusal on the `SystemDatabase`/`InvalidInput` channel,
      all-empty satisfied, settled-member winner) in `WaitCases`, live+sim.
- [x] G1 `Duration` abstract from `DBOS.Transact` (`Transact.hs:188`);
      keep `millisDuration`/`secondsDuration`, internal `(..)` stays.
      Done 2026-10-16, uncommitted — the constructor is no longer exported
      from the facade; no consumer used it raw.
- [x] G5 executor-level wait coverage: shared first-wait winner,
      empty-set refusal (paired with B3), and wait-all empty-is-satisfied
      cases. Done 2026-10-16, uncommitted — five shared
      `WaitExecFixture` cases (first empty refused on the
      `SystemDatabase`/`InvalidInput` channel, all-empty satisfied,
      settled-member first winner, all-settled completes, single wait
      reports the settled outcome), live+sim.
- [x] Notes (dated, no behavior) — recorded 2026-10-17, nothing changed:
  - G4 reserved queue names. The port refuses only the exact
    `_dbos_internal_queue` (`Queue.hs:366-374`), matching Rust
    (`sysdb/mod.rs:21`, `queue.rs:657`); Python reserves the whole `_dbos_`
    prefix (`_queue.py:58`). Rust speaks and wins: exact-name refusal stays.
  - G6 `observability_query_timeout_sec`: Python-only, no Rust counterpart.
    The port's listing statements carry no statement timeout, as Rust's do —
    deferred to the management/listing slice.
  - G6 graceful `destroy`: Python waits for completion under a timeout; the
    port's `shutdown` aborts tasks immediately and waits for unwinding
    (`Instance.hs:214-237`), leaving mid-run workflows PENDING for the next
    launch to recover. Deliberate contract, kept.
  - G6 schedules: the sysdb layer is complete (`createSchedule` …
    `deleteSchedule`, `applySchedules`, CallerStep-recorded, live+sim tested)
    but nothing exposes it — no engine form of Rust's `DBOS::create_schedule`
    / `apply_schedules` (`schedule.rs:434-489`), none of Python's client
    schedule methods (`_client.py:1440-1787`). Rust speaks, so this is an
    unimplemented surface rather than a divergence: own slice, next epic.
  - G6 `rename_application` (`_client.py:1756`): Python-only, no Rust
    counterpart. Deferred with that slice.
  - G6 `set_latest_application_version`: already present as
    `clientPromoteVersion` — the same stamp-through-owner-resolution body as
    Rust (`client.rs:616`), scoped to the client's own application because
    the backend fills the claimant when the name is `Nothing`
    (`Backend.hs:3211-3223`). Missing only Rust's `_for` override
    (`client.rs:648`); deferred with the client slice.
  - G7 fixed: the `IsolationLevel` comment now names its provenance
    (TypeScript's four-level `PGIsolationLevel` — the only oracle naming
    `READ UNCOMMITTED`; Python's three-level literal with `Serializable` the
    default; Rust v0.5.0 has no application-facing level) and states that
    `Nothing` is Python's explicit-none path (a bare `BEGIN`). ADR-0021's
    `ReadCommitted` default corrected in place to `Serializable`, dated.
- [x] G3 config: `configUseListenNotify`/`configMigrate`
      (`Config.hs:70-71`) are read nowhere — now documented as deliberately
      inert at the `Config` declaration (2026-10-17): the port polls, so there
      is no LISTEN/NOTIFY path to switch on, and migrations are out-of-band
      (`make db-migrate`, ADR-0004) with launch verifying the schema instead
      (`acquirePostgresSystemDB` -> `verifySystemDatabase`, ceiling `121`).
      The fields stay for one-to-one Rust `Config` fidelity; the backend's own
      `Config` already declares the same pair away (`Backend.hs:932-936`).
- [x] G2 client slices: queue CRUD + `set_*`, `wait_first`,
      `update_workflow_attributes`, `set_workflow_delay`,
      `enqueue`/`enqueue_in_transaction`, `list_queued_workflows`,
      `read_stream`; client `register_queue` defaults `AlwaysUpdate`
      and refuses `UpdateIfLatestVersion` with `ErrorConfig`
      (`queue.rs:640-660`). Done 2026-10-16, uncommitted —
      `clientRegisterQueue`/`RetrieveQueue`/`UpdateQueue`/`DeleteQueue`
      (reusing the engine's guards + validators), `clientWaitFirst`
      (Python's non-empty/no-duplicate refusal), `clientUpdateWorkflowAttributes`,
      `clientSetWorkflowDelay`, `clientListQueuedWorkflows`,
      `clientReadStream`/`clientReadStreamFrom` (the shared reader
      generalized to a `ClientConnection` placement via
      `readStreamOutside`), `clientEnqueueWorkflowInTx` (+ Postgres
      `enqueueInTx`). The G6 items this left open are recorded in the notes
      entry above (schedules need their own slice; the version writer already
      exists as `clientPromoteVersion`).
- [x] Stack-review follow-ups: R1 empty fork input returns `Ok` with no
      validation on every layer now (engine early-`Right`, sim null guards
      matching the Postgres guards; shared refusal scenario pins both
      empty verdicts plus the two zero-count announcements in the sim
      trace) — done 2026-10-16; R2 comment narrowed to
      launched-instance entries (client forks call the backends directly)
      — done 2026-10-16; R3 `coghcid.sh` and the `proto/queue-proofs` probe
      moved to the gitignored `.tmp/` (2026-10-17) — nothing in the tree
      referenced either, and `.gitignore` now carries `.tmp/`.
- [x] Close-out: full `cabal test all` + direct psql mirror against
      `$DBOS_DATABASE_URL` with the watcher idle, `make db-migrate` 121->121 —
      green 2026-10-17 and landed as `d33f436` (committed directly at the
      user's request; `/review` not run on that slice).

## Ground-truth verification (2026-10-17)

Re-checked the "open item" list against the code rather than the docs. Three
recorded follow-ups were stale and are corrected in place; one stands:

- ADR-0021's deferred list named `deleteCheckpoints` for rewind/completion — it
  landed (`Datasource.hs:159`; `Datasource/Postgres.hs:205-211`; rewind
  `Instance.hs:408`; completion `Workflow.hs:249-250,282,288`). Still deferred:
  `readOnly` fast-path, `pendingTransaction` (re-checked 2026-10-27: the
  widget track composed checkout/dispatch live+sim without needing either;
  ADR trigger unfired). ADR-0021 line 52 corrected.
- Completion clearing covers the child workflow and the fire-and-forget start
  too: the clearance rides the task spawner (`Workflow.hs:121,352,670`; a child
  runs under its parent's spawner, `:818-822`), while kills and control ends
  keep rows on purpose (`:241-248`). Superseded by `ad906e0`;
  `docs/transactional-step-todo.md` follow-up closed.
- The widget store drops its rows on completion as Python does; the
  exactly-once instrument reads an append-only commit log
  (`WidgetCases.hs:584-602`), same `ad906e0` — `docs/widget-store-port-todo.md`
  corrected.
- Still open by design (accepted limitation, R1): the captured-instance escape.
  A step body holding a captured `DBOS`/`Executor`/`Client` can start/enqueue
  with no `insideAStep` guard (`Instance.hs:252-255,266-279`,
  `Client.hs:311-373`). Re-checked 2026-10-27: no step-context threading
  landed since; closing it still needs the phantom-brand direction, so it
  stays an accepted limitation, not a blocker.

## Debouncer + changelog deferrals, re-checked (2026-10-17)

The two items picked off the open list were already landed; only the records
were stale:

- **Queue-patterns debouncer (ADR-0027) — done.** The facade exposes
  `DBOS.Transact.Debouncer` (`Debouncer`, `debouncerNew`, `debounce`,
  `debounceInWorkflow`), the demo wires `Route.DebouncerSubmitAction` ->
  `Handler.submitDebounced` -> `debounce`, and the dual-stack suite is
  `DebouncerTest`/`DebouncerTestSim` (ADR-0028, 5 live cases). ADR-0027 is
  marked superseded (dated note); the stale "debouncing is deferred" comments
  in `QueuePatterns/Workflows.hs` and `View.hs` and the "message-based"
  wording in the demo README are corrected.
- **CHANGELOG "Deferred, recorded" list — mostly stale (dated correction
  added).** `fetchOperationCheckpoint` and the `OperationCheckpointStore` seam
  no longer exist; the bulk status read reports `Malformed` instead of failing
  the batch; `enqueueWorkflow` carries inputs and the acting application
  (name/version/id, `Workflow.hs` `workflowNewWorkflow`); polling reads retry
  through `withRetry` (`Backend.hs:2450-2457`); and payload bodies are opaque
  cross-language by the interop contract, so the `portable_json` sleep tag
  being unread is by design.
- **Closed:** the last decode-crash item — the *unused* singular
  `fetchWorkflowStatus`/`fetchWorkflowStatusSession` (`error . show` on an
  unknown status spelling; no callers in `src`, `test`, or `demo-apps`; no Rust
  counterpart — Rust's `get_workflow` maps to the class's `getWorkflow`) is
  removed from `Backend.hs`, and the plan's out-of-scope decode note is folded
  closed. Verified `cabal build all` + `cabal test all` + the psql mirror.

## Locked decisions (do not relitigate without new evidence)

1. Phantom brands over Bluefin-the-dependency (`Eff` is `Env -> IO a`,
   kills the IOSim dual stack); Bluefin patterns only.
2. Mechanism before surface: depth counter, then the `Ctx` split, then
   the engine rewire.
3. Calls degrade to plain, starts refuse (`InsideStep`) — oracle-faithful.
4. `withAttempt` restores via `onException` + explicit success unbump
   (keeps `MonadCatch`, no `MonadMask` cascade; residual window leaks safe).
5. One shared `insideAStep` predicate (scope field OR depth) read by every
   guard: `placeCall`, `takenPlacement`, `startChildWorkflow`, `send`,
   `recv`, eager `getEvent`, `runTxStep`.
6. `WorkflowCtx`/`StepCtx` run parallel-track until the rewire; `Ctx`
   stays until the last module flips.
7. App-level StepCtx-keyed domain records; engine core stays ops-free.
   Registration closes over tables, helpers take them explicitly, step
   bodies capture + receive `StepCtx`. Per-workflow tables (no shared
   kitchen-sink record); runners never see them.
8. Runners take `Executor m` (Phase B); scope binder keeps narrow
   `Connection`/`Identity` (launch-free for unit tests).
9. `withWorkflow` takes `WorkflowId` (+ `workflowIdText` projector);
   smart-constructor hiding rejected (45-site churn for one blank check).
10. Enforcement ledger: compile-time where types reach; runtime refusal
    with matchable ADTs where capture defeats types; documented
    discipline where Haskell cannot reach (determinism, raw pool).
11. Probes before tree; throwaway packages under `proto/`, never merged.
12. TDD/watcher process per AGENTS.md: one eval pair, `make db-migrate`
    first, no `cabal test` overlapping evals, psql verify per gate.

## Phase A — design consolidation (THIS FILE + plan doc)

- [x] A1: fold deltas (plan HTML Phase 10 / Rule 9 / TODO 12–16 / sidebar).
- [x] A2: record decisions 1–11 (this section).
- [x] A3: widget table sketch (`docs/widget-step-tables.md`):
      `CheckoutSteps`/`DispatchSteps` (the actual workflow pair), `setStatus`
      repeated per the ≤2 rule, STM handlers with split-race fixes,
      failing variant, live contract specified for C-phase.

## Phase B — Executor runner slice — DONE (ac51207)

- [x] B1–B5 landed: `datasources` field; launch paths return `Executor`;
      three runners take it (`ErrorNotLaunched` unconstructible there);
      facade exports `Executor` abstract; every test file and both demo
      apps thread the executor; the run-before-launch case retargeted to
      retrieve (compile-time half recorded).
- [x] B6 gate: 644/644, psql 3/3, sim trees re-run one pair at a time
      (WorkflowSim 52/52, ManagementSim 12/12, WidgetSim 3/3).
- Remainder (NOT in slice): enqueue/queue/management/dequeue/getters/
  `pendingGetEvent` stay on `DBOS`+`requireExecutor`.

## Phase C — engine rewire (module order fixed)

Step → Handle → Select/Event/Sleep → Workflow → Management/Datasource/rest.
Per module: signatures → call sites → live+sim → gate. `Ctx` stays until
the last module flips.

- [x] C1a scoped step runners (7384031): `runStepScoped`
      (workflow view, allocates, hands `StepCtx`) and `runNestedStep`
      (step view, plain, no allocation possible); context seam readers
      `stepCtxAt`/`stepCtxTracer`/`workflowCtxInner`; live+sim tests with
      exact trace asserts; 646/646.
- [x] C2a scoped pending pair (0bc1852): `pendingStepWithScoped`/
      `pendingStepScoped`/`driveStepWithScoped`;
      `awaitChildScoped`/`pendingAwaitScoped`; live+sim tests (StepSim
      pins the drive trace: `StepOutputRecorded` only).
- [x] C2b scoped sleep/event/select (0adf014): `sleepStepScoped`/
      `pendingSleepScoped`, `setEventScoped`/`pendingSetEventScoped`/
      `getEventScoped`/`pendingGetEventScoped`, `selectStepScoped`; the
      shared `scenarioScopedSelect` races two pending steps (rows
      `[(1,"fast"),(2,"DBOS.selectStep")]`, sim trace pinned); WfFixture
      gains `wfConn`/`wfIdentity` for scope-building scenarios; 649/649.
- [x] C2c `PendingStep`/`SelectArm`/`Winner` carry `exec` (814b6cb):
      scoped producers tie the brand to their view, so a cross-execution
      arm is a compile error (verified with a temporary in-tree negative:
      "Couldn't match type 'exec1' with 'exec'"); old producers keep the
      parameter call-site-quantified for unconverted call sites.
- [x] C4a scoped registry bodies (34c406f): `ErasedWorkflow` is a rank-2
      newtype; the run path builds the scope with `withWorkflow` and
      installs the task spawner; `registerTypedWorkflowScoped` /
      `registerWorkflowRefScoped` (+ Instance twins) register converted
      bodies; old entries wrap with an explicit downgrade so converted and
      unconverted bodies share one registry. `WorkflowCtx`/`StepCtx` pin
      `(exec :: Type)`. Mem backend's `recordSleep` now composes
      check+record like Postgres.
- [x] C4b scoped entries for the rest of the body surface (429e8c8):
      `startChildWorkflowScoped`, `sendScoped`/`sendWithScoped`/
      `sendBulkScoped`/`sendBulkWithScoped`/`recvScoped`,
      `runTransactionScoped`, six management `...Scoped` wrappers. Two
      bodies converted as the pattern (the capture-refusal body now uses
      `withStep` + `startChildWorkflowScoped` and still refuses).
- [x] C4c bulk (2455671): every registered body across ClientTest,
      DeadlinesTest, EventTest, InstanceTest, ManagementTest, QueueTest,
      RegistryTest, WidgetTest, WorkflowTest and the sim trees takes
      `WorkflowCtx` and registers through the scoped entries. Each body
      keeps one explicit `let ctx = workflowCtxInner wctx` downgrade;
      local body-forwarding helpers are retyped rank-2; sim bodies that
      capture state/refs move to top-level helpers (forall s in argument
      position, forall exec in result position — MonoLocalBinds cannot
      generalize let-bound bodies to rank-2). Gates: Workflow pair 54/54,
      ManagementSim 12/12, Widget pair 4/7, full suite 650/650, psql 3/3.
- [x] C1c step-body flip (fa5768f): StepRetryTest/StepTest/EventTest/
      MessageTest/WidgetSim/WidgetTest/DatasourceTest step bodies take
      `StepCtx`; runner call sites go through runStep(With)Scoped;
      nested shapes use `runNestedStep`; the Datasource fixture is
      `dsFixtureRun` (scope runner) instead of a Ctx builder. In-step
      downgrades for refusal tests stay explicit via `stepCtxInner`.
      Gates: full suite 650/650, DatasourceSim 11/11, WidgetSim 7/7.
- [x] C5b registration + twin-swap (4eb040f, 2785fee): every body's
      alias deleted and calls moved to the scoped twins; the old
      registration entries (`registerTypedWorkflow`, `registerWorkflowRef`,
      `registerDBOSWorkflow`, `registerDBOSWorkflowRef`) and their facade
      exports deleted; demo apps converted and registered scoped. Gates:
      650/650, sim trees individually green, demo apps load clean.
- [x] C5c raw start/enqueue paths (50d8eb1): the facade no longer exports
      enqueueWorkflow/startWorkflowRef/runWorkflowRef/startChildWorkflow/
      runRegisteredWorkflow*; starts and enqueues reach application code
      only through the Executor/DBOS wrappers or the WorkflowCtx scoped
      entries. WorkflowTest's two in-step negatives moved to the captured
      shape; the direct in-step shape has no facade term.
- [x] C5c probes: permanent `probes/` pairs + `make probes` (four
      negatives matched to their error class, four witness twins clean):
      child start, allocation, nested step, cross-exec race.
- [x] C5c inner accessors (superseded twice): `workflowCtxInner`/
      `stepCtxInner` LEFT the facade in the C5a trim, and the C5 close
      (106e872) deleted them outright — views own their fields now, so
      there is no inner context to reach. The backstop tests construct
      illegal shapes through the view vocabulary itself
      (`stepCtxBoundary`, `stepCtxWorkflow`, captured parents), and the
      exec-brand probes pin the shapes that must not typecheck.
- [x] C5a op-pair promotion: every `*Scoped` operation takes the plain
      oracle name (`send`, `recv`, `setEvent`, `getEvent`, `runTxStep`,
      `runStep(With)`, `sleepStep`, `awaitChild`,
      `selectStep`, `startChildWorkflow`, the pendings, the six management
      `...InWorkflow`s, the `register*` entries); the raw-context workers
      are inlined or module-private (`selectStepOn`, `runTxStepWith`,
      the `drive*` drivers). Direct-shape backstop tests move to the
      captured shape with identical verdicts — except place-first ops,
      where capture now degrades to plain per the new `checkHere` arm
      (deviation 6); the transaction direct-shape case deletes
      (compile-time by construction, captured twin keeps G4). New:
      `stepCtxWorkflow` projector (same brand; backstop still applies).
      Gates: full suite 650/650, every converted sim re-run pair-green,
      probes 8/8.
- [x] C5a reader twins — decided minimal (338bb97 follow-up): exactly one
      twin, `stepCtxCancellationToken`, the `stepCtxX` mirror of the raw
      stem. Rationale: it is the only reader with app-body demand (token
      polling is contract-level); observation/scaffolding readers
      (`stepId`/`stepStatus`/`withSystemDB`/`workflowId`, 7 test-only
      sites) keep the documented downgrade; engine-only readers need
      nothing. Rejected: full twin set (shallow), `stepCtxCancelToken`
      (verb-stem collision with the firing function), `cancellingToken`
      (near-miss with the raw reader), typeclass overloading. 4 body
      sites swapped; StepTest's 2 workflow-scope checks stay downgraded
      by design (documented asymmetry). Gates: 650/650, probes 8/8.
- [x] C5b remainder (finished 2026-10-04, 106e872): `data Ctx` deleted —
      `WorkflowCtx`/`StepCtx` own every field (conn/identity/state/spawner/
      tracer; scope), `withStep` holds the bump/restore/cancel trio, the
      ten `workflowCtxInner` downgradelets are gone with the accessor, the
      `currentConnection` hole is closed (`workflowConnection` is
      engine-internal and takes a view), and the exec-brand `-fno-code`
      probe pair landed (`neg-exec-escape`/`w-exec-escape`, probes 10/10).
      Engines and tests build views; `newWorkflowCtx` replaces `newCtx` as
      the documented fixture seam.
- [x] C2 Handle — executed as C2a + C5a: awaits over scoped views
      (`awaitChild`/`pendingAwait` take the plain oracle names).
- [x] C3 Select/Event/Sleep — executed as C2b + C5a: scoped arms/reads/
      sleeps under the plain names.
- [x] C4 Workflow execute path — executed as C4a/b/c + C5c + C5a:
      `startChildWorkflow` takes `WorkflowCtx` (the typed surface is the
      `WorkflowRef m e`-taking entry); enqueue/start are `WorkflowCtx`-only
      with no `StepCtx` overload.
- [x] C5 Management/Datasource/rest + final `Ctx` removal (106e872): the
      six management `...InWorkflow`s, `runTxStep`, waits, selects,
      events, messages, sleeps, steps, handles, and checkpoints all take
      views; `stepCtxBoundary` is the boundary-shaped attempt view drives
      hand where a step view is expected. Gates: full suite 650/650,
      probes 10/10, migrate 114→114, psql mirror green.

## Phase D — StepOps widget pilot (`docs/widget-step-tables.md`)

- [x] D0 sketch: `CheckoutSteps`/`DispatchSteps` (StepCtx-keyed, `OrderId`
      boundary, `setStatus` repeated per the ≤2 rule), STM handlers
      (single-`atomically`, split-race fixes), failing variant, status
      codes, live contract specified for C-phase.
- [x] D1 tables + STM handlers + failing variant + direct handler tests
      (ca71aac): mint sequence, oversell race (one winner, stock 0),
      failing third call aborts whole, status codes `(1,0)` matching the
      live assertions; tables built inside the scope continuation so the
      phantom unifies; watcher Widget pair green (live 4, sim 7).
- [x] D2 engine integration (1f9d630 sim + live half): the
      `runTransactionScoped` bridge hands bodies `(StepCtx, Tx)`;
      `WidgetSim` bodies spend `stmCheckoutSteps`/`stmDispatchSteps`,
      `Tx`-ignoring fakes and `failingWidgetDs` retired; live bodies spend
      `pgCheckoutSteps`/`pgDispatchSteps` closing over the held `Tx`; the mixed
      live+canned case (`failingPgCheckoutSteps` refuses the paid mark)
      asserts no `order_id`, order `(1,0,3)`, inventory `4`, PENDING row
      with no dispatch child. Gates: full suite 651/651, Widget pair 5/7.
- [x] D3 follow-up domains — RETIRED 2026-10-27: the pilot proved the
      pattern and no follow-up domain requested it; keeping an open box for
      hypothetical next-domain work added review noise. The pattern lives in
      `docs/widget-step-tables.md` and `WidgetCases.hs` if a domain ever
      needs it.

## Standing gates (every slice)

1. `make db-migrate` first (idempotent, verify ceiling).
2. Watcher: sim-only eval; read `ghcid.txt` after reload.
3. Full suite via the standalone binary, freely alongside the watcher (the
   sim eval holds no fixture rows).
4. psql mirror against `$DBOS_DATABASE_URL` (NOT the default `dbos` DB
   unless the env says so).
5. Read-only Rust oracle spot-checks (`~/dev/dbos-transact-rust`).
6. Slices land on `/review`, possibly batched, with gate numbers in the message.

## Probe index (reference only, never merge)

| Probe | Runner | Claim | Gate |
|---|---|---|---|
| `proto/scope-brands/run.sh` | 19/19 | brands + backstop + pool pinning | compile-fail negatives + runtime marks |
| `proto/deterministic-steps/run.sh` | 12/12 | Ops carrier + journal replay, IO vs IOSim | agreement diff + exploding-handler replay + 3 negatives |
| `proto/dual-store/run.sh` | 7/7 | Postgres-txn vs STM step table | byte-identical invariant lines + 1 negative |

## Known asymmetries (documented, not hidden)

- STM serializes; Postgres is read-committed — op safety comes from
  single-statement check-and-decrement; ops stay coarse-grained.
- Body-atomicity across several steps exists on IO (held connection) but
  has no STM counterpart for `m`-typed bodies; the shared unit is one step.
- Brands track executions, not lexical nesting; the depth counter tracks
  dynamic step scope. Both layers stay.
- Record-access conventions landed 2026-10-04: dot-first reads (preference
  order in `AGENTS.md`), `RecordWildCards` a cabal default, pass-through
  accessors deleted, `StepStatus` oracle-shaped; public fields are
  camelCase (ADR-0023) while SQL/Aeson/config keys and log prose keep
  Rust spelling.
- `hasql` pinned to the repo's 2.x range in probes (`>=2.0.1 && <2.1`, matching `ihp-typed-sql`; 2.x moved `acquire` behind an explicit `Pqi.adapter`).
