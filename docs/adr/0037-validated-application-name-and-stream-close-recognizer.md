# ADR-0037: Validated `ApplicationName` and one stream-close recognizer

Date: 2026-10-18. Status: accepted.

## Context

Two invariants the port currently enforces at run time are better carried by
types, and one type-level candidate is deliberately *not* taken. This ADR
records all three calls so the boundaries are not rediscovered later.

**1. The rename target.** Rust validates an application name in exactly one
place: `rename_application` refuses `new_name` that fails
`is_valid_application_name` (`sysdb/postgres.rs:7338`, refusing with
`InvalidInput { field: "new_name", detail: "must be 3 to 30 characters of
lowercase letters, digits, dashes and underscores" }`). Everywhere else an
application name is an unchecked `&str`: the identity/settings name, an enqueue
target, a queue or schedule owner, and a listing filter all pass through
unvalidated, and stored rows are read back unvalidated.

The port mirrors that (`Backend.hs` `renameApplication` guard,
`isValidApplicationName` at `DBOS.SystemDB.Types`), so the refusal is a tested
run-time behavior on the `InvalidInput` channel
(`PostgresTest` "an invalid name and the same name are refused").

**2. The stream-close mark.** A closed stream is a row whose
`(value, serialization)` pair is exactly
`(streamClosedSentinel, streamClosedSerialization)`. Rust names the test once
(`sysdb/mod.rs:49`, `is_stream_closed`) and the port re-derives it by hand at
each site: `Stream.hs`'s local `isClose` (three uses, one of them on a
`StepRecord`'s recorded output), `IOSim.hs`'s step-name choice and its rewind
predicate, and `Statements.hs` `rewindDeleteStreamCloseStatement`, whose two
bound parameters must agree with the predicate.

**3. Listing filters.** `Applications = Unset | AnyApplication | Named [Text]`
also represents `Named []`, which every reader silently reads as "unscoped"
(`Backend.hs:1246-1248,1983,3348`, `IOSim.hs:1204`). A `NonEmpty` payload would
delete that hidden state and the four collapses that re-decide it.
**Deferred** (see Consequences) — not taken with this ADR.

## Decision

**1.** Add an opaque `ApplicationName` to `DBOS.SystemDB.Types`, constructible
only by `parseApplicationName :: Text -> Either Error ApplicationName` (the
oracle's rule, refusing on the same `new_name` field and message), read with
`renderApplicationName`. `renameApplication` takes it instead of `Text` on the
`SystemDB` class, both backends and the mock, so the run-time guard is deleted:
a rename target that cannot be stored cannot be built. `isValidApplicationName`
stays exported as the predicate the parser mirrors.

**Deliberately not applied** to the identity/settings name, an enqueue target,
a queue or schedule owner, or a listing filter. Rust does not validate those,
so typing them would refuse values the oracle accepts — the port would be
inventing a refusal rather than removing one, and would reject the unvalidated
names real deployments already carry.

**2.** Add `isStreamClosed :: Text -> Maybe Text -> Bool` beside the two
constants, ported name-for-name from Rust's `is_stream_closed`, and route every
recognition site through it: `Stream.hs` (both the pull reader and the recorded
replay), `IOSim.hs` (the step name chosen for a write, and the rewind
predicate) and `Backend.hs` (the step name chosen for a write). The pair has
one recognizer, so a mismatch between value and serialization is decided in one
place instead of five.

A sum type cannot cross this seam: the mark is two schema columns, and the
`SystemDB` write method takes the pair positionally (as Rust's trait method
does), so the alternatives are `writeStream`/`closeStream` at the workflow level
(already distinct) and one recognizer at the row level (this).

**3.** No change to `Applications` in this ADR.

## Consequences

- `renameApplication`'s signature changes on the `SystemDB` class, so the
  `InvalidInput` for a bad target is no longer reachable through it: the
  refusal moves to `parseApplicationName` at the caller. The refusal itself is
  unchanged in field and message, and it is what the tests now pin.
- The sysdb no longer refuses; a caller that wants the old behavior parses
  first. Rust keeps its check because its type is `String`; the port's type
  makes the check unreachable, which is the point.
- `Applications`' hidden `Named []` state and the four collapse sites stand as
  a known wart. Taking it later is a self-contained change: `Named (NonEmpty
  Text)` at the declaration plus the four collapses and the filter tests. It is
  a payload narrowing of a ported constructor, so it needs its own ADR when it
  lands.
- Adding a validated name type elsewhere (queue ownership, schedule ownership,
  enqueue targets) reopens this ADR's "deliberately not applied" clause and
  needs the oracle to justify it.
