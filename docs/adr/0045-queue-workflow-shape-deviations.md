# ADR-0045: Queue and workflow type-shape deviations (Y2/Y4)

Date: 2026-11-13 (oracle-validation findings Y2, Y4; Rust 0.6 oracle).

## Context

The findings list six shape deviations with no Haskell counterpart (or an
extra field) in the Rust 0.6 oracle. Each was checked against its readers
before deciding parity against record.

## Decision

Record all six as deviations with per-item justification. None changes
wire format or stored rows, and none hides behavior a caller needs.

### Y2 — `WorkflowRecord.creatorXid` (extra field, serialized)

Kept. The oracle reads the creator through separate init-row queries
(`stored_creator`, `init_workflow` returning `creator_xid`); the port's
`getWorkflow`/`listWorkflows` return the domain record directly, and the
readers of the field need the token from that same fetch:

- engine/sim fixtures derive the execution owner token from the fetched
  row (`ownerFor` in every `*Test(Sim).hs` fixture);
- the sim backend stores a per-row creator for `shouldExecute`,
  debounce-holder, and reuse-refusal checks, mirroring the SQL init's
  gates.

Removing it would thread a second lookup through every fixture and the
sim backend for zero behavior change, and the checkpoint JSON stays
intra-port exact (cross-SDK listing replay is already out per the Y3
record). Revisit if fixtures and the sim stop needing a per-row
creator.

### Y4a — `SysRewindOptions` (no oracle counterpart)

Kept. It mirrors the oracle trait's parameter split — cut taken
separately, remaining options as a record (`sysdb/mod.rs`
`rewind_workflow`) — while the engine `RewindOptions` keeps its 0.5
cut-inclusive shape on the app-facing surface. Shared validation
(`rewindFieldsValid`) keeps both error channels identical. Full 0.6
alignment (dropping the cut from engine `RewindOptions`) is a
client-visible break and needs its own ADR.

### Y4b — `DeleteCaller` (no oracle counterpart)

Kept. The oracle spells absence/recorded as `Option<StepCaller>`; the
port additionally models fenced-but-unrecorded
(`DeleteCaller owner Nothing`), which is what the in-step leaf rule
passes (`Management.hs` `deleteWorkflowsInWorkflow`): the fence still
applies, nothing is recorded. Collapsing it would delete the leaf-rule
shape.

### Y4c — `NotificationRow` (no oracle counterpart)

Kept. It is the named decode target for the consume path
(`decodeNotificationRow`, take transaction) and the mem backend's
stored shape — ordinary port seam work like the other `*Raw`/`*Record`
types, not a domain-type deviation. The oracle reads the same columns
through anonymous rows.

### Y4d — `clearQueueAssignment` (no oracle counterpart)

Kept as a class seam. Both backends implement it and both test trees
pin it (release returns the row to its queue). No engine path calls
it; recovery re-enqueues through its own sweep. If an engine path ever
needs claim release, it should call this seam rather than new SQL.

### Y4e — `StepCaller` drops the codec (and the old flatten claim)

Partly stale finding: the type nests `WorkflowOwner`
(`stepCallerOwner`) — only the codec is dropped, which the single-codec
port needs nowhere (`portable_json` everywhere, ADR-0038). The oracle's
`owner()` constructor maps to reading `stepCallerOwner` directly.
Kept as is.

## Consequences

- No wire, row, or API change in this slice.
- Y2 and Y4 in `docs/oracle-validation-findings-2026-11-12.md` are
  closed by this record. Each item names its revisit trigger above.
