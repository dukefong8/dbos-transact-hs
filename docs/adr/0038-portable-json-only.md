# ADR-0038: Portable JSON is the port's only payload encoding

Date: 2026-10-19. Status: accepted.

## Context

The port wrote plain aeson JSON tagged `rust_serde` and exposed
`encodeWorkflowValue`/`decodeWorkflowValue` (with `CodecError`) through the
public `DBOS.Transact` facade. Python refuses `rust_serde` rows outright
(`TypeError: Serialization rust_serde is not available`), so a workflow or
step written by this port could not be read by Python, and its workflow
arguments never used the cross-SDK envelope. The TypeScript implementation
shows the contract the tag promises: `DBOSPortableJSON` writes plain JSON
tagged `portable_json`, workflow arguments are
`{positionalArgs, namedArgs}`, workflow errors are `{name, message, code,
data}`, and reads dispatch on the row's tag (`src/serialization.ts`).

`TODO.md` S2 proposed a per-registration `SerializationFormat
{Default, Portable, Native}` plus a public facade surface and a separate
portable client-enqueue entrypoint. That is the oracle-shaped, configurable
design, but it grows the public API for a port that has exactly one codec and
no application-defined serializer.

## Decision

The port has one payload encoding, and it writes it everywhere:

- **Tag.** Every payload column (workflow input/result/error, step
  output/error, event, message, stream value, sleep wake time, stream-close
  sentinel, management checkpoint) is tagged `portable_json`. The shared
  constant is `portableJsonSerialization` in `DBOS.SystemDB.Types`.
- **Workflow arguments.** `encodeWorkflowInput` writes the portable envelope
  `{positionalArgs, namedArgs}`; `decodeWorkflowInput` unwraps it. No
  arguments (`()`) is the empty positional list, one positional is the value,
  several are a tuple (a JSON array), named-only is the record, and a mixed
  envelope is passed through whole. A JSON array argument (a tuple or a list)
  is written as several positionals, matching the oracle's argument list.
- **Other payloads.** `encodeWorkflowValue` writes plain JSON; results,
  events, messages and stream values are not enveloped.
- **Errors.** `encodeErrorText` writes the portable record
  `{name, message, data}` (`code` is omitted: the port has no codes) and puts
  the port's structured `Error` envelope in `data`, so a replay reconstructs
  the original constructor. A foreign record without a matching `data` payload
  keeps its `name: message` text.
- **Reads dispatch.** NULL, the empty string and the legacy `rust_serde` tag
  decode as native plain JSON (old rows stay readable). `portable_json` decodes
  as portable (arguments unwrap the envelope). Any other tag is
  `CodecUnknownSerialization`: an unreadable encoding is reported, never
  guessed.
- **No format selection.** There is no `SerializationFormat`, no per-call
  format argument, and no separate portable enqueue. `Config.Serializer` keeps
  one constructor, `PortableJson`. The oracle's native/per-format machinery is
  not ported.
- **Codec access is the facade's.** `DBOS.Transact.Serialization` holds the
  codec (`CodecError`, `encodeWorkflowInput`, `encodeWorkflowValue`,
  `decodeWorkflowInput`, `decodeWorkflowValue`, `decodeErrorText`,
  `PortableArgs`, `WorkflowInput`, `encodeUnit`, `encodeAttributes`). The
  facade re-exports the app-facing entry points (`CodecError`,
  `encodeWorkflowInput`, `encodeWorkflowValue`, `decodeWorkflowInput`,
  `decodeWorkflowValue`, `decodeErrorText`, `PortableArgs`, and the opaque
  `WorkflowInput`), so a body or a client can build and read a workflow
  argument without reaching into the internals sublibrary. Tag inspection and
  `encodeUnit` stay internal.

## Consequences

- Python and TypeScript can read Haskell-written inputs, results, events,
  messages and error records, and this port reads their legacy/native rows.
  Byte-identical native (`rust_serde`) output is deliberately gone; Rust still
  writes only `rust_serde`, so this is a behavioral deviation, recorded here.
- The outbox demo's enqueued consumer now takes the unwrapped triple: the
  engine, not the body, decodes `positionalArgs`. The SQL function already
  wrote the envelope, so the stored row is unchanged.
- A single list-valued argument is indistinguishable from several positional
  arguments; the port writes both as a JSON array. A mixed
  positional+named envelope is handed to the body as the whole object so
  nothing is dropped.
- Application errors have no `Show` constraint on the wire path; their
  portable `message` is the payload's text when that is a JSON string, and
  its JSON encoding otherwise, while `data` keeps the structured value
  (2026-10-19: Python reads `ErrorApplication "boom"` as message `boom`, not
  `"\"boom\""`).
- The sysdb-only rewind caller step is now recorded as the same portable error
  record and read back by its tag (`checkCallerStep`); an unreadable tag is
  reported as `UnknownSerialization` on both channels. The sysdb refusal is
  never recorded (`should_record` is false for it); at the engine channel it
  is an ordinary outcome like any other, and only a workflow-input decode maps
  it to control (the registry boundary).
- The client-facing `Serializer` name now renders `portable_json`; a
  `Config` built with the old constructor name does not compile (one
  constructor, renamed, recorded here).
- Still open from `TODO.md`: non-finite floats (S7) are unchanged.

## Addendum (2026-10-19): facade access for demo apps

The port's demo package depends only on the public library, so the facade now
re-exports the codec entry points the demos use —
`encodeWorkflowInput`, `encodeWorkflowValue`, `decodeWorkflowValue` and
`CodecError` — alongside the already-public `Serialization`,
`SerializedWorkflowValue` and `Serializer`. The
`DBOS.Transact.Serialization` module itself remains in the
`dbos-transact-internals` sublibrary, which stays `visibility: private`:
the public library's facade and the package's test suite may depend on it,
demo apps may not. `cabal build exe:demo-apps` is the gate that keeps demo
imports on the public surface.

## Addendum (2026-10-19): review fixes on the portable-JSON slice

- **Input decode is control.** A recorded body's input decode failure is a
  `FailureControl` at the registry boundary (`registerTypedWorkflow`), as
  Rust's `registry.rs` maps it — the row is not failed with a recorded
  `Deserialization`/`UnknownSerialization`.
- **Unknown tags are outcomes.** `controlOf` has no
  `UnknownSerialization` arm; a step/event/stream/step-listing read with an
  unreadable tag is reported in the outcome (recorded) while a workflow-input
  decode maps it to control. `failureError` and the handle reads share
  `decodeRecordedError`, which keeps a foreign portable `name: message` and
  falls back to the raw payload only when the bytes are not an error this
  build can read.
- **`WorkflowInput` is opaque.** `encodeWorkflowInput` returns a
  `WorkflowInput`, and every public start/enqueue/child/debounce/client
  signature takes `Maybe WorkflowInput`, so a plain
  `encodeWorkflowValue` payload cannot be passed where an argument belongs.
  The `Workflow`/`Instance` internals still carry raw
  `SerializedWorkflowValue`; the conversion happens at the boundary.
- **Step listing outcomes are total.** `StepOutcomeNotLoaded` (payloads not
  read), `StepOutcomePending` (no error, no output, no completion time) and
  the succeeded/failed arms replace the old `PayloadNotLoaded`; a legacy or
  foreign error record still yields its `name`/`message` before the listing
  reports undecodable.
- **In-workflow listing replays its own snapshot.** The
  `DBOS.listWorkflowSteps` checkpoint records `(loadOutput, [StepRecord])`, so
  a replay re-projects the typed view from the bytes the first execution read
  rather than from the replayed call's options. Cross-SDK note: another
  SDK's checkpoint for that step is a bare row array; a mixed-version replay
  of this one step is not shape-compatible. The port's own replay is exact.
- **Envelope null tolerance.** `unwrapEnvelope` treats a missing or `null`
  `positionalArgs`/`namedArgs` as empty (Rust's `null_as_default`) and a
  stored `"null"` input as the empty envelope; a present field of the wrong
  shape is still malformed.
