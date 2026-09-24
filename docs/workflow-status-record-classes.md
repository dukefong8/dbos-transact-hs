# WorkflowStatus / WorkflowRecord Typeclasses

Recommendation for which Haskell types/typeclasses the port of Rust
`WorkflowStatus` and `WorkflowRecord` should carry. Sources read directly;
every claim cites `file:line`. No source files were changed.

## Sources

- Oracle: `/Users/duke/dev/dbos-transact-rust/crates/dbos/src/sysdb/types.rs`
- Port: `src/DBOS/Transact/WorkflowExecutionStatus.hs`,
  `src/DBOS/Transact/WorkflowExecutionTypes.hs`
- Rules: `AGENTS.md`, `dbos-transact-hs.cabal` (`commons` build-depends),
  `docs/tdd-workflow.md` (package rule)
- Related port code: `src/DBOS/Transact/Codec.hs`,
  `src/DBOS/Transact/WorkflowExecutionParse.hs`,
  `src/DBOS/SystemDB/Types.hs`, `src/DBOS/SystemDB/Postgres.hs`,
  `test/DBOS/TransactTest.hs`

## What the oracle carries

`WorkflowStatus` derives `Debug, Clone, Copy, PartialEq, Eq, Hash,
Serialize, Deserialize` with `#[serde(rename_all = "SCREAMING_SNAKE_CASE")]`
(`types.rs:205-206`). The serde spellings are a wire format shared with every
other implementation, and a checkpointed listing must replay as what `as_str`
wrote (`types.rs:195-203`). Hand impls: `as_str` (`types.rs:226-236`),
`parse -> Option` returning `None` for unknown rather than guessing
(`types.rs:243-254`), `is_terminal` true only for
Success/Error/Cancelled (`types.rs:257-262`), `Display` delegating to
`as_str` (`types.rs:265-269`). `MaxRecoveryAttemptsExceeded` is deliberately
*not* terminal — parked, resumable (`types.rs:827-828`; same point in
`AwaitedOutcome` docs, `types.rs:1123-1127`).

`WorkflowRecord` derives `Debug, Clone, PartialEq, Eq, Serialize,
Deserialize` (`types.rs:296`) — note: no `Hash`, no `Copy`, no `Default`.
Serialize exists because a listing taken inside a workflow is checkpointed and
replays from what it recorded (`types.rs:282-285`). Every field except the two
the schema declares `NOT NULL` and this layer always writes (`workflow_id`,
`status`) carries `#[serde(default)]`, so a checkpoint written by an older
build stays readable after a column is added; without it the replay fails as
`Error::Malformed`, non-retryable, on the replay path (`types.rs:286-295`).
Unknown fields need nothing (serde ignores them by default). `StepRecord`
repeats the pattern with narrower exemptions: the primary key and the replayed
name do *not* default, since that would pass an unreadable record off as step
0 with no name (`types.rs:1026-1041`).

Constraining neighbours: `NewWorkflow` (not `WorkflowRecord`) is what callers
build — `status` and `recovery_attempts` are derived, `initial_status` follows
from queue/delay (`types.rs:461`, `types.rs:606-617`); `Outcome::status`
maps Output→Success, Error→Error, so recording an outcome settles the status
(`types.rs:1093-1100`).

## What the port carries today

`WorkflowStatus` is a 7-constructor ADT deriving `stock (Eq, Show)`
(`WorkflowExecutionStatus.hs:12-20`), with `parseWorkflowStatus :: Text ->
Either WorkflowStatusDecodeError WorkflowStatus` and
`UnknownWorkflowStatus Text` (`WorkflowExecutionStatus.hs:22-36`) — the
per-domain `Either` ADT the error rule asks for (`AGENTS.md:69`). There is no
`Ord`, `Enum`/`Bounded`, `Hashable`, `ToJSON`/`FromJSON`, `isTerminal`, or
same-module render function; the render lives across the codebase boundary as
`workflowStatusText` in `Postgres.hs:1074-1083`.

`WorkflowExecution` (13 fields) and `WorkflowExecutionRow` (14 fields, status
as `Text`) derive `stock (Eq, Show)` throughout
(`WorkflowExecutionTypes.hs:51-84`); `WorkflowName` alone also derives `Ord`
(`WorkflowExecutionTypes.hs:25`). No JSON instances exist for any domain type:
the only `ToJSON`/`FromJSON` in `src/` are constraints on the generic
app-value helpers in `Codec.hs:32,49`, with failures reported through the
`CodecError` ADT (`Codec.hs:24-29`). Terminality knowledge is inline, not
named: `parseNonCancelledWorkflowOutcome` lists non-terminals
`[Pending, Enqueued, Delayed]` (`WorkflowExecutionParse.hs:93`). Tests
enumerate all seven variants twice via hand-maintained literal lists
(`TransactTest.hs:60-69`, `TransactTest.hs:209-218`).

## Per-type tables

### `WorkflowStatus` (Haskell `DBOS.Transact.WorkflowExecutionStatus`)

| Rust capability | Haskell equivalent | Verdict |
|---|---|---|
| `PartialEq + Eq` (`types.rs:205`) | `deriving stock Eq` — already have (`WorkflowExecutionStatus.hs:20`) | Adopt (keep) |
| `Debug` (`types.rs:205`) | derived `Show` — already have; keep for diagnostics only | Adopt (keep), with the `Display` caveat below |
| `Display` → `as_str` (`types.rs:265-269`, `226-236`) | hand-written `WorkflowStatus -> Text` in the *same* module | Adopt: move `workflowStatusText` (`Postgres.hs:1074-1083`) next to the type; derived `Show` must never be the wire spelling |
| `parse -> Option` (`types.rs:243-254`) | `parseWorkflowStatus` + `UnknownWorkflowStatus` — already have (`WorkflowExecutionStatus.hs:26-36`) | Adopt (keep); this is the reader, not `Read` |
| `is_terminal` (`types.rs:257-262`) | `isTerminal :: WorkflowStatus -> Bool`, false for `MaxRecoveryAttemptsExceeded` | Adopt: two inline framings exist today (`WorkflowExecutionParse.hs:93`); name it once |
| `Clone + Copy` (`types.rs:205`) | nothing | No equivalent needed — memory management, not semantics |
| `Hash` (`types.rs:205`) | `Hashable` instance | Defer: `hashable` is a hidden, non-direct dep (probe below); no `HashMap`/`HashSet` use site exists |
| `Serialize + Deserialize`, SCREAMING (`types.rs:205-206`) | manual `ToJSON`/`FromJSON` via the parse/render pair | Defer: no checkpoint-JSON path for statuses exists (only generic constraints in `Codec.hs:32,49`); `aeson` is already a direct dep (`dbos-transact-hs.cabal:13`) so this needs no new package when the slice arrives |
| `PartialOrd + Ord` (absent — `types.rs:205` lacks them) | `deriving Ord` | Reject: meaningless ordering; also Haskell constructor order (`WorkflowExecutionStatus.hs:12-19`) differs from Rust declaration order (`types.rs:208-221`), so any derived ordering would be arbitrary |
| `Read` (no Rust counterpart) | derived `Read` | Reject: would accept constructor spellings (`Pending`), not the wire spellings (`PENDING`) |
| `Enum + Bounded` (no Rust counterpart) | `deriving stock (Enum, Bounded)`; `[minBound..maxBound]` replaces the literal lists (`TransactTest.hs:60-69`, `209-218`) | Defer: free from `base` (probe below) but no production code enumerates statuses; `fromEnum` values must never be persisted (constructor order differs from Rust — see above) |

### `WorkflowRecord` (Haskell `WorkflowExecution` / `WorkflowExecutionRow`)

| Rust capability | Haskell equivalent | Verdict |
|---|---|---|
| `Debug + PartialEq + Eq` (`types.rs:296`) | `deriving stock (Eq, Show)` — already have (`WorkflowExecutionTypes.hs:66,84`) | Adopt (keep) |
| `Clone` (`types.rs:296`) | nothing | No equivalent needed |
| `Serialize + Deserialize` with per-field `default` (`types.rs:286-296`) | manual instances; required only for the NOT NULL-always-written pair (`workflow_id`, `status`), `(.:?)` + `.!=` everywhere else; unknown fields already ignored by aeson | Defer: no checkpoint-JSON path for rows today; when added, the checkpoint-compat rule transfers verbatim — a field added later must default or old checkpoints become unreadable on replay |
| `Hash`, `Copy`, `Default` (all absent — `types.rs:296` has none) | `Hashable`, strictness/`Default` conveniences | Reject `Default` (Rust keeps `Default` on `NewWorkflow`, `types.rs:461`, not on the record); defer `Hashable` with the status |
| `Ord` (absent) | `deriving Ord` | Reject, as for the status |
| Field-name mapping (Rust fields are `snake_case`, `types.rs:297-439`) | explicit `fieldLabelModifier` or manual instances | Defer with the instances; Haskell record fields are camelCase under `NoFieldSelectors` (`AGENTS.md:72-79`), so generic derivation without a modifier would produce the wrong wire keys |
| `NewWorkflow.initial_status` constraint (`types.rs:606-617`) | compute initial status from queue/delay at the insert path | Adopt when the insert slice lands: callers must never supply `status` directly |
| `Outcome.status` settling (`types.rs:1093-1100`) | outcome→status function beside `WorkflowOutcome` | Adopt with the outcome slice; note Haskell `WorkflowOutcome` already has a third variant, `WorkflowCancelled (`WorkflowExecutionTypes.hs:45-49`), mirroring the `AwaitedOutcome` read side (`types.rs:1145`) rather than the two-variant write side |

## Explicit treatments

- **SCREAMING_SNAKE_CASE spellings**: keep the existing manual
  `parseWorkflowStatus`/`UnknownWorkflowStatus` pattern
  (`WorkflowExecutionStatus.hs:22-36`), which follows the per-domain error-ADT
  rule (`AGENTS.md:69`). Aeson's generic machinery could reproduce the
  spellings, but the manual pair is one case-expression each way, already
  tested against the pinned spellings (`TransactTest.hs:59-71`, mirroring
  `types.rs:799-810`), and a `FromJSON` failure cannot carry the
  `UnknownWorkflowStatus` payload the boundary reports today
  (`Postgres.hs:960`, `WorkflowExecutionParse.hs:36`). Any future
  `ToJSON`/`FromJSON` should be thin manual wrappers over the parse/render
  pair, living in the plain-Haskell layer per the two-layer rule
  (`AGENTS.md:61,68`).
- **`#[serde(default)]`-per-field rule**: implies that any Haskell JSON
  instances for record types must make every later-added field optional with a
  default, requiring only the NOT NULL-always-written pair — the Haskell
  analogue of `workflow_id`/`status` (`types.rs:289-290`). The current
  `WorkflowExecution` slice (nearly all-`Maybe`,
  `WorkflowExecutionTypes.hs:51-66`) already has that shape by accident of
  being partial; a full port must keep it deliberately.
- **`Copy`/`Clone`**: no Haskell equivalent needed (see tables).
- **`Hash`/`Hashable`**: `hashable-1.5.1.0` is present but hidden — importing
  it fails without exposing the package (probe below). Per the package rule,
  adding an already-present hidden package to the cabal file is allowed
  (`docs/tdd-workflow.md:31-40`), so this is cheap when a use site appears;
  until then, defer. Note the oracle itself derives `Hash` only on
  `Timestamp`/`WorkflowStatus`, not on `WorkflowRecord` (`types.rs:296`).
- **`is_terminal`**: adopt as `isTerminal`, documenting the parked-not-terminal
  case. The two current inline framings agree today but encode it differently
  (terminal-set vs non-terminal-list); the named function pins the
  `MaxRecoveryAttemptsExceeded → False` verdict from `types.rs:820-829`.
- **`Display` vs derived `Show`**: follow the `Timestamp` precedent just set
  in this repo — `deriving stock (Eq, Ord)` plus a hand-written
  `instance Show` rendering `<ms>ms` (`SystemDB/Types.hs:138-143`, mirroring
  `types.rs:170-174`). For `WorkflowStatus` that means: keep derived `Show`
  for diagnostics, add the SCREAMING render as an ordinary function (moved
  in-module), never derive wire output.
- **`Enum`/`Bounded` for the 7 variants**: nothing enumerates statuses today
  except the two test literal lists (`TransactTest.hs:60-69`, `209-218`) and
  the `elem` check (`WorkflowExecutionParse.hs:93`); a codebase grep for
  `minBound|fromEnum|toEnum` finds only `Int64` bounds in `SystemDB/Types.hs`,
  no status enumeration. Defer until a second production enumeration site
  appears.
- **Deriving strategy**: every clause carries an explicit `stock`/`newtype`
  strategy (`AGENTS.md:71`); any `ToJSON`/`FromJSON` via `Generic` would be
  `deriving stock (Generic)` + `deriving anyclass (...)` — but manual
  instances are preferred here (see SCREAMING treatment).
- **Exports**: explicit export lists grouped by concept (`AGENTS.md:81`) —
  `isTerminal`/render additions extend the existing group in
  `WorkflowExecutionStatus.hs:4-7`.

## Open questions

- Whether the eventual checkpoint-JSON slice covers `WorkflowStatus` alone or
  full `WorkflowRecord` rows (Rust needs both: `types.rs:200-202` for the
  status inside listings, `types.rs:282-285` for the rows). Could not verify:
  no Haskell checkpoint path writes JSON today.
- Whether `WorkflowExecutionRow.rowWorkflowStatus :: Text`
  (`WorkflowExecutionTypes.hs:70`) should become a typed `WorkflowStatus`
  with `Text` kept only at the SQL decode edge (`Postgres.hs:960`), matching
  Rust's `status: WorkflowStatus` (`types.rs:301`). Likely yes; left open
  because row-decode call sites were not fully surveyed.
- Whether `WorkflowName`'s lone `Ord` (`WorkflowExecutionTypes.hs:25`) is
  intentional or drift; the oracle orders nothing but timestamps. Left open
  as out of scope.
- Whether strictness classes (`NFData`, or `NoThunks` — `nothunks` is already
  a test dep, `dbos-transact-hs.cabal:87`) are wanted for 30+-field records.
  No use site today; defer.

## Verification runs

From workdir `/Users/duke/dev/dbos-transact-hs`:

1. `ghci -e 'import Data.Hashable (Hashable)'` → `Could not load module
   'Data.Hashable'. It is a member of the hidden package
   'hashable-1.5.1.0'` — confirms `Hashable` needs a new direct dep
   (`dbos-transact-hs.cabal:12-41` has no `hashable`); defer.
2. `ghci -e 'import Control.DeepSeq (NFData)'` → same hidden-package error
   for `deepseq-1.5.1.0` — confirms `NFData` likewise needs a new direct dep;
   defer.
3. `ghci -e ':info Enum'` → `Enum` resolves from `base` — confirms
   `Enum`/`Bounded` are free whenever the enumeration slice wants them.
4. Codebase grep for `ToJSON|FromJSON` in `src/` finds only the generic
   constraints in `Codec.hs:18,32,49` and zero domain instances — confirms
   there is no existing instance pattern to follow and nothing to stay
   consistent with beyond the manual-parse style.
