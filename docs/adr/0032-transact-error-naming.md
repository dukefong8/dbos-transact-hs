# ADR-0032: Transact.Error uniform bare constructors

Date: 2026-10-08. A rename under the HARD RULES, using TypeScript as
oracle alongside Rust.

## Context

`Transact.Error` mixed `Error-`-prefixed and bare constructors with no
inferable rule. TypeScript names every error uniformly (`DBOS*Error`);
Rust's variants are bare (`NotLaunched`, `AlreadyRegistered`,
`NotRegistered`, …). Collisions decide the shape: Haskell types and
constructors are separate namespaces, so only constructor–constructor
collisions force a prefix — discriminated by module qualification (the
tree already imports the sysdb error qualified).

## Decision

- Eight constructors go bare, converging on Rust's names: `NotLaunched`,
  `AlreadyLaunched`, `AlreadyRegistered`, `Deserialization`,
  `NotRegistered`, `WorkflowClaimLost`, `WorkflowFailed`,
  `SystemDatabase`. The application's own failure is `ErrorApplication`,
  not bare `Application`: the bare spelling reads as the application
  itself rather than its failure (and collides in prose with WAI's
  `Application` across the demos) — the one deliberate exception to the
  rule below.
- Two keep their prefix: `ErrorConfig` and `ErrorSerialization`. Their
  bare spellings exist as constructors exported from the facade itself
  (`Config(..)`, `Serialization(..)`), where no qualification can
  discriminate for an unqualified importer.
- Collisions with the sysdb (`AlreadyRegistered`, `NotRegistered`) and
  Step event (`WorkflowFailed`) constructors are discriminated by the
  existing qualified-import convention (`SystemDB.*`, `Transact.*`).
- The lift helper is gone: `application` was vague and collided with app
  modules' own bindings; its `applicationError` rename survived one
  session before removal. Bodies spell the oracle's blanket `From<E>`
  with the constructor directly (`Left . ErrorApplication`). (2026-10-08
  follow-up.)
- Clean break: `ToJSON`/`FromJSON` move together, no compat shims. Old
  recorded rows with old tags do not replay.
- Exhaustiveness: every TS error situation already has a named HS path
  (table below); no new constructors. Same-id-different-content starts
  join in HS where TS throws `DBOSConflictingWorkflowError` — deliberate
  (idempotent retries/recovery depend on joining). Streams, query
  timeouts, and nondeterminism errors have no HS counterpart (no
  streams).

| TypeScript | Haskell path |
|---|---|
| `DBOSExecutorNotInitializedError` | `NotLaunched` |
| `DBOSInitializationError` | `ErrorConfig` |
| `DBOSNotRegisteredError` | `NotRegistered` |
| `DBOSNonExistentWorkflowError` | sysdb `NonExistentWorkflow` |
| `DBOSConflictingRegistrationError` | `AlreadyRegistered` |
| `DBOSMaxStepRetriesError` | `MaxStepRetriesExceeded` |
| `DBOSStepTimeoutError` | `StepTimeout` |
| `DBOSUnexpectedStepError` | sysdb `UnexpectedStep` |
| `DBOSQueueDuplicatedError` | sysdb `QueueDeduplicated` |
| `DBOSAwaitedWorkflowCancelledError` / `DBOSWorkflowCancelledError` | `AwaitedWorkflowCancelled` |
| `DBOSAwaitedWorkflowExceededMaxRecoveryAttempts` / `DBOSMaxRecoveryAttemptsExceededError` | sysdb `ErrorMaxRecoveryAttemptsExceeded` (wrapped) |
| `DBOSInvalidQueuePriorityError` | `ErrorConfig` via `validateEnqueue`/`validateQueueOptions` |
| `DBOSInvalidWorkflowInputError` | `ErrorSerialization` / `Deserialization` |
| `DBOSWorkflowConflictError` (lost ownership) | `ErrorWorkflowClaimLost` → `WorkflowClaimLost` |
| `DBOSConflictingWorkflowError` (same id, different content) | join by design — no error (divergence, see above) |
| `DBOSInvalidWorkflowTransitionError` (context misuse) | `NotInWorkflow` / `InsideStep` / `InvalidArgument` |
| stream / query / nondeterminism errors | no counterpart (no streams) |

## Consequences

- Rule, stated once on the type: *prefixed iff the bare spelling is
  taken as a constructor in the facade's scope*.
- `ErrorTest` pins the new tags; the full suite compiler-checks every
  match site. Skill refs and the parity audit move in the sweep; dated
  records keep old names as evidence.
