# Cross-language interop: the shared `dbos` schema

Durable note for ADR-0001/0007. The Postgres `dbos` system schema is a
cross-SDK contract, not this port's private storage: a workflow written by one
SDK and resumed by another shares the database. This records what actually
crosses, the minimum common set every corpus must create, and the known gaps at
this port's migration ceiling.

Sources: the Rust corpus this port vendors (`rust-migrate/migrations/*.sql`,
`src/migrations/mod.rs`), Python `dbos/_migration.py`, TypeScript
`src/sysdb_migrations/internal/migrations.ts`, and this port's queries in
`src/DBOS/SystemDB/Postgres.hs`.

## The numbering contract

Every SDK defines `SHARED_MIGRATION_BASE = 100`: *"From here up, a version
number is a cross-SDK agreement by construction: 100 means the same DDL in
Rust, Python and TypeScript because all three define it identically"*
(`migrations.rs`). Below 100 the corpora agree by porting, not by agreement.

| SDK | ceiling | note |
|---|---|---|
| Rust | **125** | the corpus this port vendors; ADR-0007 |
| Haskell (this port) | **125** | `rust-migrate/` vendors the Rust corpus |
| Python | **132** | its corpus continues past the Rust 125; the numbering contract covers 100–125 |
| TypeScript | **130** | same contract; its corpus also continues past 125 |

A database migrated by a *newer* SDK is still readable by the port for the
columns the port uses; the shared additions are 115–125, and anything past
125 is outside the Rust corpus this port vendors.

## The wire format (values that must match exactly)

These are what one language reads out of another's rows.

1. **Status strings** — strict, enum-for-enum: `PENDING`, `SUCCESS`, `ERROR`,
   `MAX_RECOVERY_ATTEMPTS_EXCEEDED`, `CANCELLED`, `ENQUEUED`, `DELAYED`. The
   same literals appear in partial-index predicates, so a mismatch breaks
   dequeue/recovery queries, not just decoding. An unrecognised spelling is
   reported rather than guessed (`DBOS.Internal.SystemDB.Types`), because a row written
   by a newer SDK is a real possibility.
2. **`serialization` tag** — names the payload format (`portable_json`,
   `py_pickle`, `rust_serde`). Payload bodies are **opaque** to other SDKs;
   only the tag and the envelope shape cross. Rust states the model: workflows
   cross languages *by enqueue, not by execution*. **This port writes
   `portable_json` on every payload** and reads by the row's tag: NULL, the
   empty string, and the legacy `rust_serde` tag are native plain JSON;
   `portable_json` is the portable form; any other tag is refused
   (`CodecUnknownSerialization`), never guessed (ADR-0038).
3. **Enqueue input envelope** — the one cross-language payload: `inputs` is
   `{"positionalArgs": [...], "namedArgs": {...}}`. The port writes that
   envelope itself (`encodeWorkflowInput`), not only through
   `dbos.enqueue_workflow`, and its engine unwraps it before the body runs:
   zero positionals is `()`, one is the value, several are a tuple, named-only
   is the record.
4. **Portable error encoding** — `{name, message, code, data}`
   (`PortableWorkflowError`). The port writes `{name, message, data}` (it has
   no error codes) and keeps its structured `Error` envelope in `data`, so a
   Haskell replay reconstructs the constructor; a foreign record without a
   matching payload keeps its `name: message` text.
5. **Timestamps** — BIGINT epoch **milliseconds** (`created_at`,
   `updated_at`, `started_at_epoch_ms`, `completed_at_epoch_ms`,
   `delay_until_epoch_ms`, `workflow_deadline_epoch_ms`).
6. **Sentinels** — `__null__topic__` for an untyped message topic; empty
   string (not NULL) for an absent `function_name`/`queue_name`.

## Minimum common set for a shared database

Tables every corpus must create: `dbos_migrations`, `workflow_status`,
`operation_outputs`, `notifications`, `workflow_events`,
`workflow_events_history`, `streams`, `queues`, `application_versions`,
`workflow_schedules`. (`event_dispatch_kv` exists in all four but no core
engine reads it — it must exist for the schema to match, and is not
load-bearing for interop.)

Load-bearing columns:

- `workflow_status` — lifecycle plus the **queue polling** surface: `status`,
  `queue_name`, `priority`, `queue_partition_key`, `started_at_epoch_ms`,
  `delay_until_epoch_ms`, `deduplication_id`; and the recovery/ownership scope
  `application_version`, `application_name`, `executor_id`,
  `recovery_attempts`; plus `name, inputs, output, error, serialization,
  parent_workflow_id, forked_from, was_forked_from, owner_xid,
  workflow_timeout_ms, workflow_deadline_epoch_ms`.
- `operation_outputs` — `workflow_uuid, function_id, function_name, output,
  error, serialization, child_workflow_id, started_at_epoch_ms,
  completed_at_epoch_ms, application_name`. The step key is
  `(workflow_uuid, function_id)`, and `function_name` is what makes a reordered
  step a loud failure rather than a silent replay (see ADR-0021 addendum).
- `notifications` — `message_uuid, destination_uuid, topic, message,
  created_at_epoch_ms, serialization, consumed`.
- `workflow_events` / `workflow_events_history` / `streams` —
  `(workflow_uuid, key, value, serialization)` and their `function_id` /
  `offset` keys.
- `queues`, `application_versions` — registration and version scoping.

## Step records vs StepInfo

The row shape and the decoded API view are not the same type. Rust carries a
`StepRecord`; TypeScript and Python expose a decoded `StepInfo`. Haskell keeps
the Rust-shaped `StepRecord` for the instance/in-workflow listing and offers a
decoded client view:

| Semantic | Rust `StepRecord` / Haskell `StepRecord` | TS `StepInfo` | Python `StepInfo` |
|---|---|---|---|
| Step position | `step_id` / `stepRecordStepId` (`function_id`) | `functionID` | `function_id` |
| Recorded name | `step_name` / `stepRecordStepName` (`function_name`) | `name` | `function_name` |
| Output | `output` / `stepRecordOutput` — encoded `Option<String>` | `output` — decoded `unknown` | `output` — decoded `Any` |
| Error | `error` / `stepRecordError` — encoded `Option<String>` | `error` — decoded `Error \| null` | `error` — decoded `Exception` |
| Child workflow | `child_workflow_id` / `stepRecordChildWorkflowId` | `childWorkflowID` | `child_workflow_id` |
| Serialization tag | `serialization` / `stepRecordSerialization` | absent (consumed by the decoder) | absent (consumed by the decoder) |
| Started | `started_at` / `stepRecordStartedAt` | `startedAtEpochMs` | `started_at_epoch_ms` |
| Completed | `completed_at` / `stepRecordCompletedAt` | `completedAtEpochMs` | `completed_at_epoch_ms` |
| Workflow id | `workflow_id` / `stepRecordWorkflowId` | absent (listing is scoped to one workflow) | absent |

Haskell keeps the Rust row for storage and exposes the typed view on every
public listing:

| Haskell `StepInfo` | Projected from `StepRecord` |
|---|---|
| `stepInfoWorkflowId` | `stepRecordWorkflowId` |
| `stepInfoId` | `stepRecordStepId` |
| `stepInfoName` | `stepRecordStepName` |
| `stepInfoOutcome` | `stepRecordOutput` / `stepRecordError` / `stepRecordSerialization`, plus the listing's `loadOutput` (or the recorded flag inside a workflow) |
| `stepInfoChild` | `stepRecordChildWorkflowId` |
| `stepInfoStartedAt` | `stepRecordStartedAt` |
| `stepInfoCompletedAt` | `stepRecordCompletedAt` |

```haskell
data StepOutcome
  = StepOutcomeNotLoaded     -- loadOutput = False: the payload columns were not read
  | StepOutcomePending       -- no error, no output, completed_at still NULL (in flight)
  | StepOutcomeSucceeded StepOutput
  | StepOutcomeFailed StepFailure

data StepOutput
  = PayloadAbsent            -- loaded, no output
  | PayloadDecoded Value     -- decoded by the row's tag
  | PayloadUndecodable CodecError

data StepFailure
  = FailurePortable PortableWorkflowError  -- name/message/code/data
  | FailureUndecodable CodecError
```

The raw `StepRecord` listing remains an internal seam for tests and tag
inspection. A native/foreign step (`py_pickle`) becomes an undecodable
outcome, not a listing failure, preserving TS/Python tolerance. Missing
workflows list as an empty list here and in Rust; TS returns `undefined`.

Inside a workflow, `listWorkflowStepsInWorkflow` checkpoints under
`DBOS.listWorkflowSteps`. The port's checkpoint records
`(loadOutput, [StepRecord])` and re-projects the typed view from the recorded
flag, so its own replay is exact. This is a shape divergence with the other
SDKs, whose checkpoint for that step is a bare row array: a mixed-version
replay of this one step is not shape-compatible (intra-port replay is).

## Known divergences

Inert for this port (verified: zero references in `src/` and `test/`):

- `workflow_status.creator_xid` — Python/TS workflow-id-reuse policy.
- `notifications.consumed_by_function_id` — Python/TS rewind bookkeeping; the
  port sets `consumed` only.
- Non-finite floats follow aeson, not Rust (ADR-0039): NaN is stored as
  `null` and ±Infinity as the strings `"+inf"`/`"-inf"`; Rust's branch stores
  `"NaN"`/`"Infinity"`/`"-Infinity"`. A required `Double` reads `null` back as
  NaN while `Maybe Double` reads nothing; Python sees `"+inf"` as a string. No
  portable form is agreed.
- Rust still writes only `rust_serde`; this port writes `portable_json`
  everywhere (ADR-0038). The port reads both, so it reads Rust's native rows
  and Python/TypeScript portable rows.
- LISTEN/NOTIFY trigger gating.
- `transaction_completion` / `datasource_outputs` — datasource bookkeeping,
  outside the system schema.

**Migrations 121–125 (shared with the Rust corpus):**

- 121 adds `notifications.consumed_by_function_id`; 122 adds the
  unsettled-deadline partial index; 123 adds `workflow_status.creator_xid`;
  124 and 125 add the `application_versions` timestamp and
  `(application_name, version_timestamp DESC)` indexes. The 109–121 payload
  tables (`workflow_input` / `workflow_output`) arrived earlier and are
  shared too: the port creates, writes, and reads them exactly as the other
  SDKs do (ported 2026-10-03 from Rust #77, itself verbatim from Python).
- The port does not reference `creator_xid` or `consumed_by_function_id`
  (Python/TS bookkeeping, listed under the divergences above), but its
  corpus now *creates* both columns.
- A shared database may be at **125** or ahead. The port's corpus ceiling is
  125, it never migrates, and launch accepts a database **ahead** of that
  ceiling (matching Rust's runner tolerance); a database behind 125 is
  refused. Verified 2026-11-11: the full suite passes at 125 and
  `make db-migrate` reports `125→125`.

**Schedule contexts are not interoperable yet:** the port stores
`ScheduleRecord.scheduleRecordContext` as raw JSON text, while Python's
scheduler deserializes every row in a shared `workflow_schedules` table with
its own tagged serializer and fails on a Haskell row. Schedule exposure is
deferred (ADR-0036); do not share schedule rows across SDKs until the context
format is ported.

## When this changes

The migration-ceiling probe asserts the live database is at **at least 125**
(`test/DBOS/SystemDB/PostgresTest.hs`); a higher value means a newer SDK
migrated the shared database, and the pinned columns above must be re-verified
against that corpus. Re-check by diffing `information_schema` between a database
migrated by each SDK, as this note did.
