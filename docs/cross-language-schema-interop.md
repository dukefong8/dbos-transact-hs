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
| Rust / Haskell (this port) | **114** | the port vendors the Rust corpus; ADR-0007 |
| Python | **123** | 115–123 add columns Rust has not created |
| TypeScript | **123** | same shared numbers as Python |

A database migrated by a *newer* SDK is still readable by the port for the
columns the port uses; the gap is the 115–123 additions below.

## The wire format (values that must match exactly)

These are what one language reads out of another's rows.

1. **Status strings** — strict, enum-for-enum: `PENDING`, `SUCCESS`, `ERROR`,
   `MAX_RECOVERY_ATTEMPTS_EXCEEDED`, `CANCELLED`, `ENQUEUED`, `DELAYED`. The
   same literals appear in partial-index predicates, so a mismatch breaks
   dequeue/recovery queries, not just decoding. An unrecognised spelling is
   reported rather than guessed (`DBOS.SystemDB.Types`), because a row written
   by a newer SDK is a real possibility.
2. **`serialization` tag** — names the payload format (`portable_json`,
   `py_pickle`, `rust_serde`). Payload bodies are **opaque** to other SDKs;
   only the tag and the envelope shape cross. Rust states the model: workflows
   cross languages *by enqueue, not by execution*.
3. **Enqueue input envelope** — the one cross-language payload: `inputs` is
   `{"positionalArgs": [...], "namedArgs": {...}}`.
4. **Portable error encoding** — `{name, message, code, data}`
   (`PortableWorkflowError`).
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

## Known divergences

Inert for this port (verified: zero references in `src/` and `test/`):

- `workflow_status.creator_xid` — Python/TS workflow-id-reuse policy.
- `notifications.consumed_by_function_id` — Python/TS rewind bookkeeping; the
  port sets `consumed` only.
- Per-SDK serialization tags and LISTEN/NOTIFY trigger gating.
- `transaction_completion` / `datasource_outputs` — datasource bookkeeping,
  outside the system schema.

**The gap to watch — migrations 115–123 (Python/TS only):**

- `operation_outputs.retention_timestamp` and later columns Rust has not
  created. The 109–114 payload tables (`workflow_input` / `workflow_output`)
  are now shared: the port creates, writes, and reads them exactly as the
  other SDKs do (ported 2026-10-03 from Rust #77, itself verbatim from
  Python).

## When this changes

The migration-ceiling test pins 114 (`test/DBOS/SystemDB/PostgresTest.hs`); a
higher value means the Rust corpus moved and the pinned columns above must be
re-verified. Re-check by diffing `information_schema` between a database
migrated by each SDK, as this note did.
