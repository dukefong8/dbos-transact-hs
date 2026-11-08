# ADR-0041: Per-domain engine-error fragments under a thin central envelope

Date: 2026-10-28. Status: proposed.

## Context

`DBOS.Transact.Error` (`src/DBOS/Internal/DBOS/Transact/Error.hs:69`) holds all
24 constructors of the ported `Error<E>` in one ADT: the application channel
plus 23 engine variants spanning five owners (setup, registry, codec, step,
lifecycle), the recursive `MaxStepRetriesExceeded`, and the `SystemDatabase`
layer wrapper. Every consumer matches all 24 arms to touch one layer
(`mapApplication`, render, JSON, `controlOf` each re-list the whole type), and
no owner module can name its own errors. The tree's own precedent is the
opposite: per-domain event ADTs are homed with their owners (`WorkflowEvent`
in `Step`, `QueueEvent` in `Dequeue`, `SysdbEvent` in `Retry`). Layer
modularity exists (`SystemDB.Error` + `shouldRecord` vs `Transact.Error` +
`controlOf`, mirroring `sysdb/error.rs` vs `error.rs`); intra-`Transact`
modularity does not.

This ADR supersedes ADR-0040: once `Registry` drops its engine-`Error`
import (below), the import cycle that forced the `QualifiedWorkflowName` leaf is gone,
so the key merges back into `Registry` and the leaf file is deleted (history
in git). ADR-0040 stays on record as the superseded rationale.

## Decision

Split the engine side into per-owner fragments under a thin central
envelope that keeps the exact wire shape:

- `DBOS.Transact.Registry` (homed, not a leaf): `QualifiedWorkflowName` (type, smart
  constructors, `qualifiedWorkflowNameFromRow`, render, Rust-shaped JSON with `.:?`
  absence per slice-7 review) and `RegistryError` (`AlreadyRegistered`,
  `AlreadyLaunched`). Constructor and field names stay verbatim per the
  one-type-one-name rule; the resulting ambiguity with the central envelope's
  same-named constructors is resolved by module qualification at the mapping
  sites (the tree's records rule for collisions), never by renaming.
  `Registry`/`Snapshot` go generic over the stored body (`Registry m b`),
  and the module imports only `Prelude`, Aeson, and containers — that
  residue is what lets central import the key back without a cycle.
- `DBOS.Transact.Workflow` (homed, not a leaf): `ErasedWorkflow` (concrete,
  `Failure` outcome), `WorkflowRef` (phantom channel kept), `refName`,
  `registerWorkflowRef`, `registerTypedWorkflow`, and the codec-error
  helper. Every consumer (`Instance`, `Dequeue`, `Debouncer`, tests) already
  imports `Workflow`, and `Workflow` imports none of them back, so this adds
  zero new module edges — verified against the import graph. Cohesion cost,
  recorded honestly: registry concepts now live in the largest engine
  module. A dedicated leaf was proven acyclic too, but new modules are
  banned by operator constraint (2026-10-28); if that lifts, the leaf is the
  first thing to revisit.
- `DBOS.Transact.Serialization` (homed, not a leaf): `ErrorSerialization`,
  `Deserialization`, `UnknownSerialization` as `SerializationError`, beside
  the existing `CodecError`.
- `DBOS.Transact.Error.Setup` / `.Step` / `.Lifecycle` do not happen under
  the no-new-modules constraint, and owner-homing cycles (each owner imports
  central broadly), so those constructors stay central — and their logic
  stays central too. Partial per-owner functions (`Error e -> Maybe Text`)
  would relocate the same arms behind an extra layer without reducing
  their count, against reader-load and laziness; classification already
  distributes where it matters (`shouldRecordStartError` lives in
  `Workflow`). Revisit with real fragment types if the constraint lifts.
- Central `DBOS.Transact.Error` keeps all constructors flat, plus the
  `Exception` instance, the tagged JSON envelope, `Failure` classification,
  `fromSystemDB`/`fromRegistryError`, and re-exports. `EngineOnly` becomes
  a synonym for `Data.Void.Void` (aeson 2.3.2.0 ships both `Void` JSON
  instances with matching behavior; base ships the rest; signatures keep
  parsing). The decode-failure message changes from the custom refusal to
  aeson's `Cannot parse Void`; it surfaces only on rows that cannot exist
  for `EngineOnly` workflows, still falling back to `WorkflowFailed`.
  `mapApplication` is deleted for the derived `Functor` instance;
  call sites become `fmap`. `liftEngine = fmap absurd`.

Mapping between owned types and the envelope goes through named functions
(`fromRegistryError`, and a codec-side mapper in S2), never pattern
synonyms, so sweeps keep finding real sites and readers see one obvious
path.

Each fragment owns three aspects per constructor: the payload codec, the
prose renderer, and the control/record predicate arm (a `shouldRecord`-style
`Bool` predicate with no wildcard, so central `controlOf` delegates; the
`SystemDatabase` non-recorded arm stays central beside `shouldRecord`). The
canonical human rendering is the existing `Exception` / `displayException`
instance; `renderTransactError` stays as the engine's entry point and
delegates per fragment, with the `Show` constraint isolated to the
`Application` arm as today. Payload codecs may move to `Generic` with
`sumEncoding = TaggedObject` only where derivation is byte-identical to
today's hand-rolled objects — the round-trip tests decide, and mismatches
like `StreamTimeout` (fields `streamWorkflowId`/`streamKey`/`streamTimeout`
against wire `workflowId`/`key`/`timeout`) stay hand-written. Field types
follow the same rule: `workflowId` may become the `WorkflowId` newtype (its
JSON instances already encode as a plain string), while
`operation`/`step`/`detail` stay `Text`. Tag spellings are frozen, including
the documented deviations (`MaxRecoveryAttemptsExceeded` tag,
`Error`/`ErrorSerialization` prefixes per ADR-0032) — a replay must decode
old rows.

## Why this shape and not the alternatives

- **Central hosts the erasure boundary** (imports `Context`): disproven by
  execution. The import-closure script finds
  `Connection → Config → Error → Context → Connection`. Any central
  `→ Context` edge closes that cycle.
- **Fragments homed in owner modules**: the ADR-0040 cycle recurs per
  fragment, since `Registry`, `Workflow`, `Step`, and `Instance` import
  central broadly.
- **`Workflow` hosts the boundary types**: chosen. Mis-homes registry
  concepts into the largest engine module, but adds zero new module edges
  (every consumer already imports `Workflow`, which imports none of them
  back) and needs no new files. A dedicated leaf was proven acyclic too;
  revisit it first if the no-new-modules constraint lifts.
- **`Instance` hosts the boundary types**: `Workflow` needs `WorkflowRef`
  and `Instance` already imports `Workflow` — a cycle the other way.
- **Fully independent per-module errors, no central sum**: rows carry one
  `data` envelope read cross-SDK; without a central sum there is no single
  serializable type at the storage boundary.
- **Pattern synonyms to avoid touching use sites**: rejected on
  grep-ability and reader load; the sweep rules below depend on finding
  real sites.
- **Renaming fragment constructors to dodge ambiguity** (e.g. avoiding a
  second `AlreadyRegistered`): rejected by the verbatim-name rule;
  module qualification is the tree's own collision style.

## Consequences

- Recorded deviation: Rust `registry.rs` maps to `Registry` plus the
  erasure boundary homed in `Workflow`; Rust `error.rs` maps to central
  `Error` plus the `Registry`-homed and `Serialization`-homed fragments.
  No constructor changes owner without amending this ADR.
- Amendment 2026-10-28 (S10-U1): the oracle itself moved `QualifiedWorkflowName`
  (`registry.rs` → `sysdb/types.rs` in #135, so the sysdb layer names keys
  without depending on the engine), and the port follows: `QualifiedWorkflowName`
  (type, smart constructors, `qualifiedWorkflowNameFromRow`, render, JSON,
  `workflowRecordQualifiedName`) now lives in `DBOS.SystemDB.Types`. `RegistryError`
  stays homed in `Registry` and names the imported key. Double-move
  history: ADR-0040 leaf → this ADR's Registry merge → this move. The move
  is DAG-safe: `Types` imports only Prelude/Aeson/containers/Set/
  `SystemDB.Error`/time (the key needs Aeson+Text), and no module under
  `DBOS.SystemDB.*` imports `Registry` or the engine error channel — only
  the sanctioned `Transact.Logger` seam — so the new `Registry→Types` and
  `Transact.Error→Types` edges close no cycle. Facade export list
  unchanged (import source lines only).
- Every new engine variant in a homed fragment touches two places (fragment
  type plus central mapping arm). The compiler enforces both. Variants in
  the central remainder touch one place plus delegated logic arms.
- Facade discipline holds: the export list is unchanged (moved names only
  change their import source line); fragment and sum types are re-exported
  beside `Error`.
- Sweeps follow the tree rules: word-boundary, counted per file, verified
  by re-grep; string literals are never touched (wire tags share
  constructors' spellings — the `listStepsStepName` trap); after any sweep,
  `grep -rn '"DBOS\.'` is compared against the oracle constants, then
  `cabal build all` plus the suite.

## Migration plan (verifiable units, wire frozen throughout)

- **Phase 0 — done:** slice-7 review findings (FromJSON `.:?` + `.!=
  Nothing`; ADR-0040 facade sentence). Round-trip tests pin the wire.
- **Phase S1 — structural cut (this slice):** generic `Registry`, key merge
  (leaf deleted), `RegistryError`, erasure boundary moved to `Workflow`,
  `Instance` wrappers delegate to it, central gains only the
  import swap plus `fromRegistryError`. Constructors, tags, and prose
  unchanged. Gate: watcher green.
- **Phase S2 — homed types plus central hygiene:** `SerializationError`
  homing with a central codec-side mapper; `Void` synonym for `EngineOnly`;
  `mapApplication` deletion for derived `Functor`; render/JSON/predicate
  logic delegated to owner functions (no type moves, no use-site rewiring
  beyond the codec trio). Gate: watcher green plus `Error` round-trips.
- **Phase S3 — gates:** watcher sims; binary patterns for
  `Error`/`Registry`/`Client`; full binary plus psql recency proof;
  `Error`/`Registry`/`Client`; full binary plus psql recency proof;
  oracle `client` suite read-only; `cabal build all` (covers the demos
  exe). Shared scenarios stay live+sim twins; any new live twin owns its
  rows per the suite rules. Fold the delta into `.lavish/rust-port-plan.html`
  and land only on `/review`.

Each phase is type-first (new ADT lands first, `ghcid.txt` names every
site), no refactor while red.
