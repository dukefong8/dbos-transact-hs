# ADR-0042: Debounce deduplication-id respelling (Rust full-key shape)

Date: 2026-10-28 (S10 engine U4d, Rust #135).

## Context

The debounce deduplication id is `<workflow>-<key>`, class-prefixed for
instance workflows (`Billing.eu-key`), following the TypeScript oracle
(ADR-0028). Rust #135 spells it in one function as
`{workflow}-{key}` where `{workflow}` is the key's full display
(`wf/Billing/eu`, `wf` for a free function), and refuses `/`-or-`-`
collisions between unrelated workflows by whole-key comparison at
`classify` rather than escaping.

## Decision

Adopt the Rust spelling: `debounceDedupKey` renders
`renderQualifiedWorkflowName workflow <> "-" <> debKey`, shared by the engine and
client loops. Whole-key comparison (name, class, and configured
instance, empty as absent) already landed with the classify port, so the
collision refusal the spelling relies on is in place.

## Consequences

- Wire break, recorded rather than migrated: rows written under the old
  spelling will not match bounces under the new one, and vice versa. The
  database holds no such rows at adoption (clean-slate development
  database; debounced rows are transient by nature — every one resolves
  within its timeout), so no migration or dual-read is provided. A
  deployment with live debounced rows across the upgrade would see those
  keys orphaned until their delays elapse and fresh debounces start new
  rows.
- Cross-SDK note: the spelling now matches Rust exactly (`wf/Billing/eu-key`),
  so a key debounced from Rust coalesces with one debounced from here.
- No twin pins the literal spelling (checks assert presence and
  coalescing, never the text), so the change is covered by the existing
  debounce suites running green on both stacks.

## Note 2026-11-08 (Rust #136)

Rust #136 spells the id from the name's parts rather than through
`Display` (which is now for people to read), keeping one definition for
the stored form. `debounceDedupKey` follows: it matches on
`(qualifiedClassName, qualifiedConfigName)` directly instead of rendering
first. The output is unchanged in all four arms
(`f-k`, `f/C-k`, `f/C/eu-k`, `f//eu-k`), so the wire-break accounting
above still holds, and a unit test now pins the literal spelling per arm
(`each shape of name spells its own deduplication id` in
`SystemDB.TypesTest`, mirroring Rust
`each_key_spells_its_deduplication_id`).
