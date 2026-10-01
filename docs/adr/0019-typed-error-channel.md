# Typed error channel: `Error e` / `Application e` / `EngineOnly`, with serialized recorded failures

Rust's `Error<E = EngineOnly>` is generic over the application's error type
(`error.rs:100`): `Application(E)` wraps the app's own failure, `EngineOnly` is
the uninhabited default for workflows that declare none, a blanket
`impl<E> From<E> for Error<E>` is what lets `?` lift an app error, and
`map_application` re-targets a whole error at another channel (recursing into
`MaxStepRetriesExceeded`'s nested errors). Recorded failures are the
serialized `Error<E>` — "the run that fails and the replay that reads the row
back both produce this variant with an equal payload, which is the property the
whole generic parameter exists for" (`error.rs:104-107`). The port collapsed all
of that into one untyped ADT and rendered failures to text, with the erasure
documented in `Registry.hs` and `Error.hs`. Decided 2026-09-30: port the full
shape ("Full typed-error parameterization"), with recorded payloads serialized
typed. This is the typed-error epic; `children.rs:1894` (lift) and
`workflows.rs:222` (foreign error at the boundary) are its last cases.

Decision:

- `Error e` mirrors `Error<E>` variant for variant. `Application e` is the one
  new variant; the engine variants keep their current Haskell spellings (the
  documented `Error*` prefix deviations for name collisions). `EngineOnly` is
  an empty data type with absurd `Eq`/`Show` deriving and, at the recording
  stage, absurd Aeson instances.
- The constraint analogue of `DurableError` is a synonym, not a class:
  `type DurableError e = (ToJSON e, FromJSON e)` — Rust's blanket impl means
  users write nothing, and a synonym gives the same property with no methods.
  `Show e` is carried only where rendering needs it (Rust's `Display`).
- `application :: e -> Error e` is the blanket `From`. `mapApplication ::
  (e -> f) -> Error e -> Error f` is `map_application`, sharing the one match.
  `liftEngine :: Error EngineOnly -> Error e` is `Error::lift`, total because
  `EngineOnly` is uninhabited.
- Recorded failures hold the serialized `Error e` (Aeson JSON), and replay
  decodes it back: the failing run and the replay return the same error value.
  This changes what the `error` columns hold (today: rendered text), and the
  untyped-text assertions in tests migrate with it.

Staging (each stage ends green; recorded so future turns can resume):

1. **Core type + migration alias.** `ErrorOf e` with `Application`, `EngineOnly`,
   `application`/`mapApplication`/`liftEngine`, `renderTransactError ::
   Show e => ErrorOf e -> Text`; `type Error = ErrorOf EngineOnly` as a
   temporary alias so the ~228 existing sites compile unchanged (import lists
   change from `Error (..)` to `ErrorOf (..), Error`). The ADR records that the
   end state names the parameterized type `Error` and deletes the alias.
2. **Thread `e`.** Registrations, `WorkflowRef m e`, bodies
   (`argument -> Ctx m -> m (Either (Error e) result)`), handles, and every
   engine signature polymorphic in `e`; `DurableError e` where values cross
   the serialization boundary. Tests move to `Error EngineOnly` explicitly.
3. **Serialized recorded failures.** Encode `Error e` into step/workflow error
   columns; decode on replay; migrate the error-column assertions.
4. **Close the epic.** Land lift (`children.rs:1894`) and foreign-error
   (`workflows.rs:222`) on the typed API; delete the alias and rename
   `ErrorOf` to `Error`; fold the deltas into the ownership map and plan.

Consequences:

- Recorded error payloads change shape (JSON of `Error e` rather than rendered
  text), which is also what Python and Rust record; the Python-schema boundary
  improves rather than erodes.
- `Error` no longer derives `Eq`/`Show` unconditionally; it derives with the
  payload's instances, and `EngineOnly` supplies absurd ones.
- Until stage 4, the exported name `Error` is the alias and the ported type is
  `ErrorOf` — a knowingly temporary naming deviation, solely to keep the tree
  green between stages. The HARD RULES' one-name-one-type mapping holds at the
  end state, which this ADR pins.

Recorded 2026-09-30.

## Landed (2026-09-30)

All four stages are in: `Error e` is the type's name (no alias), the channel
is threaded through `Registry`/`Handle`/`Step`/`Workflow`/`Instance`/`Client`,
recorded failures are the serialized envelope (psql shows
`{"AwaitedWorkflowCancelled":{...}}`, `{"Application":{"Gateway":{...}}}`), and
replay decodes them back as themselves. Lift and foreign-error landed with
`children.rs:1894` / `workflows.rs:222` as live+sim cases, both oracle-checked.

Two port-only notes:

- **The start's channel is a type variable, not a marker.** Rust's
  `PendingStart<R, E, C = E>` defaults the reported channel to the child's and
  `.lift::<C2>()` re-declares it. The port's `startChildWorkflow` /
  `startWorkflowRef` / `startDBOSWorkflowRef` instead report in an
  unconstrained `c` (the start can only fail in engine terms, so `c` is
  phantom): the common case infers `c = e`, and the lift case — a parent whose
  channel differs from its child's — typechecks by inference. Same semantics,
  no marker value.
- **`Error EngineOnly` is written out where Rust's default parameter would
  apply.** Haskell has no defaulted type parameters, so the engine channel is
  explicit at every engine-only site; `EngineAliases`/`IOSimTracer` carry the
  driver aliases that pin it for tests.

`EngineOnly`'s absurd `ToJSON` encodes nothing (unreachable) and its
`FromJSON` refuses: a payload that claims an application failure in the
engine-only channel is a decoding bug, not data. The variant JSON is
hand-written tagged objects, single-field records included — a bare-field
`toJSON` emitted strings and was caught by the sim legs.
