# `SomeTracer` removal research: can the GADT carrier be replaced?

**Question.** Can the `SomeTracer m` wrapper be removed from this port, and if so, what is
the cheapest faithful replacement? (Research only; no repo source was modified. Prototypes
lived under `/var/folders/fk/w27km5892sb7vbt454rpq7b00000gn/T/opencode/sometracer-proto/`.)

**2026-10-07 note.** The module moved and was merged: `DBOS.Tracer` is now
`DBOS.Transact.Logger` (the former `DBOS.Transact.Log` merged in; the test module is
`DBOS.LoggerTest`). Paths below are updated to the new locations; line numbers remain those of
the research snapshot.

**Method / environment.** GHC 9.12.4, cabal-install 3.16.1.0, the repo's own package
environment via `cabal exec -- ghc`, with the cabal defaults replayed as flags
(`-XGHC2024 -XDuplicateRecordFields -XNoFieldSelectors -XOverloadedLabels
-XOverloadedRecordDot -Wall`, from `dbos-transact-hs.cabal:7-13`). Resolved versions from
`dist-newstyle/cache/plan.json`: `contra-tracer 0.2.1.1`, `io-sim 1.11.0.0`,
`io-classes 1.11.0.0`. Prototype modules import only `Control.Tracer`, `Control.Monad.IOSim`
and fast-logger — not `DBOS.*` — so the temporarily-red test component (`TDD Loop`, AGENTS.md)
was never built. Every result below is a command actually run; exact commands are in §7.

---

## 1. What `SomeTracer` does today

```haskell
-- src/DBOS/Transact/Logger.hs:96-97
data SomeTracer m where
  SomeTracer :: (forall e. (LogEvent e, ToLogStr e, Typeable e) => Tracer m e) -> SomeTracer m
```

- `runTracer` is the sole eliminator: `runTracer (SomeTracer tracer) = CT.traceWith tracer`
  (`src/DBOS/Transact/Logger.hs:102-103`); emission happens only through it (ADR-0017:5, plan Rule 5:
  `.lavish/rust-port-plan.html:329`).
- Backends: `nullTracer` (`src/DBOS/Transact/Logger.hs:108-109`), `ioTracer` over FastLogger
  (`src/DBOS/Transact/Logger.hs:114-115`), `fastLoggerTracer` (`:82-85`); test-owned sim carriers in
  `test/DBOS/IOSimTracer.hs:31-41`.
- The GADT is *the* mechanism that lets one stored value serve many unrelated event types:
  the existential erases the event type at the field, and the rank-N argument keeps the
  `LogEvent`/`ToLogStr`/`Typeable` dictionaries for each concrete event.

### 1.1 Every holder/storage site

| Holder | Location | Why it needs an erased event type |
|---|---|---|
| `Connection.connTracer` | `src/DBOS/Transact/Connection.hs:109`, constructed by `newConnection` (`:117-132`) and `forApplication` (`:151`) | The single engine-half carrier (ADR-0015:15). Emitters are in different modules with different event ADTs: `Recovery` (EngineEvent, `Recovery.hs:67`), `Management` (ManagementEvent, `Management.hs:90,113,142,207,208,237`), `Workflow` (WorkflowEvent, `Workflow.hs:121,225,246,261,265,268,271,283,287,290,542,583,588,679`), `Dequeue` (QueueEvent, `Dequeue.hs:203,229,241,288,353,356,368,387,399,415,447,478,480`), `Instance` (EngineEvent, `Instance.hs:216,218,328,501`). The record cannot name one event type. 39 dot-reads of `conn.connTracer` in src (count in §6). |
| `Ctx.ctxTracer` | `src/DBOS/Transact/Context.hs:97`; inherited at `newCtx` (`:181`), rebound by `withTracer` (`:193-194`), read by `contextTracer` (`:199-200`) | Per-execution override so sims/tests can substitute a carrier (ADR-0017:7); consumed by `Step` (WorkflowEvent, `Step.hs:175,187,190,214,371,383,432,435,446,470,477,481,485` — 13 `runTracer` sites), `Sleep` (`Sleep.hs:65,75`), `Wait` (`Wait.hs:88`). Same multi-domain problem as the connection. |
| `PostgresSystemDB.psdbLog` | `src/DBOS/SystemDB/Postgres.hs:1165`; built by `fromPool` (`:1171`), `acquirePostgresSystemDB` (`:1196`), `withPostgresSystemDB` (`:1241`) | The `SystemDB` class methods receive only `db` (plus params), so the handle is the only place the retry seam can find a tracer: `withRetry env.psdbRetry operation env.psdbLog uuidEntropy` (`:1278, :1689, :2543`) and the `SysdbQueueMismatch` warning (`:1530-1533`). |
| `Notifier.log` | `src/DBOS/SystemDB/Postgres/Notifier.hs:108`; built by `notifierNew` (`:113`) | Notifier methods receive only the notifier (`signal`, `run`); emits SysdbEvent (`:155`, `:183`, `:230`). |
| Function parameters (no record) | `withRetry` (`src/DBOS/SystemDB/Retry.hs:126-133`, emits at `:147`), `reportDequeueError` (`src/DBOS/Transact/Dequeue.hs:296-303`), `startExecutor` (`src/DBOS/Transact/Instance.hs:465`, emits `:468`), `newConnection`/`forApplication`/`fromPool`/`acquirePostgresSystemDB`/`withPostgresSystemDB`/`notifierNew` above | Threading the launch's single carrier through constructors; each emitter passes events of its own domain. |
| Test holders | `IOSimTracer.simTracer/simTracerSay` (`test/DBOS/IOSimTracer.hs:31-41`), `collectingTracer` (`test/DBOS/SystemDB/NotifierTest.hs:119-120`), `nullLogger` (`:130`; `test/DBOS/SystemDB/PostgresTest.hs:194`), signatures in `test/DBOS/Transact/ContextTest.hs:109,134,142`, `test/DBOS/SystemDB/IOSim.hs:195,261,267,297,314,320` | Same erasure need; the sim carrier must accept every domain event yet still trace concrete types. |

Checked and **not** holders (so removal does not touch them): `DBOS` (`Instance.hs:82-87`, no
tracer field) and `Executor` (`Instance.hs:95-102`) — `Executor` keeps only
`releaseTracer :: m ()`; readers go through `executor.conn.connTracer` (`Instance.hs:216,218,328,501`).
This matches ADR-0015:16-17 ("`DBOS.dbos_logger` and `Executor.tracer` deleted"; `releaseTracer`
stays because it is a lifecycle action, not data).

### 1.2 Typed sim traces: exactly what depends on the erasure

`selectTraceEventsDynamic :: forall a b. Typeable b => SimTrace a -> [b]` recovers the
*concrete* dynamic a `traceM` stored (`io-sim-1.11.0.0 Control/Monad/IOSim.hs:214-219`;
`traceM :: Typeable a => a -> IOSim s ()`, `Control/Monad/IOSim/Types.hs:148`). The current
carrier preserves that because `simTracer = SomeTracer (mkTracer traceM)` instantiates
`traceM` at each concrete event type, which the GADT's `Typeable e` dictionary permits.
Call sites and recovered types:

| Test site | Recovers |
|---|---|
| `test/DBOS/Transact/ContextTestSim.hs:174` / `:175` | `WorkflowEvent` (`[StepRunning "demo" 0]`) / `SysdbEvent` (`[SysdbRetryAttempt …]`) |
| `test/DBOS/Transact/StepTestSim.hs:143,148,153,158` (via `traceEvents :: SimTrace a -> [WorkflowEvent]`, `:161-162`) | `WorkflowEvent` |
| `test/DBOS/Transact/WorkflowTestSim.hs:1626` | `WorkflowEvent` (8 constructors) |
| `test/DBOS/Transact/ManagementTestSim.hs:326` | `ManagementEvent` (7 constructors) |
| `test/DBOS/SystemDB/RetryTest.hs:111` | `SysdbEvent` |
| `test/DBOS/LoggerTest.hs:36` | `WorkflowEvent` (`[StepRunning "double" 3]`) |

`test/DBOS/Transact/StepTestSim.hs` and the other Sim trees pass `simTracerSay`/`simTracer`
into builders (`simConnectionWith`, `simLaunchWith`, `memLaunchOn`, …) 82 times across 10
test files (grep `simTracerSay|simTracer` excluding `IOSimTracer.hs`). Any replacement must
keep this exact recovery.

---

## 2. Candidate 1 — direct Rank-N tracer, no wrapper at all

The field holds the rank-N value itself; `SomeTracer` is deleted (or kept as a rank-N
synonym). GHC's guide confirms a forall type is legal as a field type
(§6.4.20 `rank_polymorphism`, "As the argument of a constructor, or type of a field, in a
data type declaration"). On GHC 9.12.4 this needs no explicit `RankNTypes` pragma under
`-XGHC2024` (prototype `RankNGHC2024.hs` compiled).

**Prototype (the PASS variant), `RankNFieldAccess.hs`:**

```haskell
data Holder m = Holder
  { hTracer :: forall e. (LogEvent e, ToLogStr e, Typeable e) => Tracer m e }

runTracer :: (LogEvent e, ToLogStr e, Typeable e, Monad m)
          => (forall x. (LogEvent x, ToLogStr x, Typeable x) => Tracer m x) -> e -> m ()
runTracer t e = CT.traceWith t e

holderTracer :: Holder m
             -> (forall e. (LogEvent e, ToLogStr e, Typeable e) => Tracer m e)
holderTracer (Holder t) = t

useAccessor :: (LogEvent e, ToLogStr e, Typeable e, Monad m) => Holder m -> e -> m ()
useAccessor h e = runTracer (holderTracer h) e        -- PASS

simBackend :: [EventA]
simBackend = selectTraceEventsDynamic
  (runSimTrace (runTracer (holderTracer simHolder) (EventA "x"))) :: [EventA]   -- PASS
```

**Observed: PASS** — compiles clean, including the `IOSim` use with no explicit
`forall s` body, because the accessor returns a polytype and `runTracer` takes a rank-N
argument (the canonical `foo id` shape; GHC's QuickLook handles `f (g x)`). Construction
(`Holder { hTracer = mkTracer traceM }`), record update (`h { hTracer = t }`), and passing a
concrete backend through a higher-rank parameter also compile.

**What FAILS: reading the field with record dot.**

```haskell
useDirect h e = CT.traceWith h.hTracer e
-- error: [GHC-39999] Could not deduce ‘HasField "hTracer" (Holder m) (Tracer m e)’
```

This is a hard, documented GHC restriction, not a style preference: with
`OverloadedRecordDot`, `a.b` desugars to `GHC.Records.getField` (`GHC User's Guide §6.5.10`),
and §6.5.9.1 says verbatim: *"If a record field has a polymorphic type (and hence the
selector function is higher-rank), the corresponding `HasField` constraint will not be
solved, because doing so would violate the functional dependency on `HasField` and/or
require impredicativity"*, and *"we do not permit users to define their own instances
either"*. Adding `-XImpredicativeTypes` does **not** change the error (re-run observed).
This is fatal for this repo's mandated style: `NoFieldSelectors` + `OverloadedRecordDot` are
cabal defaults (`dbos-transact-hs.cabal:10,13`) and AGENTS.md says "DO read with record-dot"
and "DON'T call bare field selectors as functions" (they do not exist under
`NoFieldSelectors`). Reads would have to go through pattern-match accessors.

**What also FAILS: the type-synonym spelling as a first-class value.**

```haskell
type SomeTracer m = forall e. (LogEvent e, ToLogStr e, Typeable e) => Tracer m e
runTracer :: (LogEvent e, ToLogStr e, Typeable e, Monad m) => SomeTracer m -> e -> m ()
runTracer t e = CT.traceWith t e            -- PASS (predicative instantiation)
holderList :: [SomeTracer IO]               -- FAIL
holderList = [mkTracer (\_ -> pure ())]
-- error: [GHC-91510] Illegal polymorphic type: forall e. … Tracer IO e
--        In the expansion of type synonym ‘SomeTracer’
--        Perhaps you intended to use the ‘ImpredicativeTypes’ extension
```

Signatures written with the synonym *do* compile (so the name could be kept), but the
carrier stops being a first-class value: the current GADT `SomeTracer m` is a monomorphic
box that can sit in any container (`[SomeTracer m]`, `Maybe (SomeTracer m)`, tuples),
whereas the rank-N synonym cannot (GHC-91510; the rank-polymorphism guide's warning that
"GHC will not instantiate type variables to a polymorphic type" applies).

**Cost.** ~46 record-dot reads of the tracer fields must become accessor calls or pattern
matches: 39 `conn.connTracer` + 4 `env.psdbLog` + 3 `notifier.log` (counts in §6). Plus one
accessor per holder (`connTracerOf`, `psdbLogOf`, `notifierLogOf`, `ctxTracerOf`; `contextTracer`
already is one, so its 16 call sites survive). `runTracer`'s signature changes to the rank-N
spelling, and `withTracer`/`reportDequeueError`/`withRetry`/constructor params take the
rank-N type, but if the synonym name is kept the 33 signature lines spelled `SomeTracer` stay
textually valid. Sim tests keep working through the accessor shape.

---

## 3. Candidate 2 — existential **event**, monomorphic tracer (the cheap one)

Move erasure from the tracer to the event: `SomeTracer` becomes a synonym for a plain
`Tracer` whose input is an existential event. This is the same information (existential over
the *value* instead of the *tracer*), but no higher-rank tracer wrapper is needed anywhere.

**Prototype `SomeEventSyn.hs` (runtime-verified):**

```haskell
data SomeEvent where
  SomeEvent :: (LogEvent e, ToLogStr e, Typeable e) => e -> SomeEvent

type SomeTracer m = Tracer m SomeEvent

runTracer :: (LogEvent e, ToLogStr e, Typeable e, Monad m) => SomeTracer m -> e -> m ()
runTracer tracer e = CT.traceWith tracer (SomeEvent e)

nullTracer :: Monad m => SomeTracer m
nullTracer = CT.nullTracer

-- FastLogger: unwrap inside mkTracer (contramap SomeEvent does NOT typecheck, see below).
ioTracer logger = mkTracer $ \(SomeEvent e) -> CT.traceWith (fastLoggerTracer logger) e

-- Sim: unwrap and trace the CONCRETE event, preserving typed recovery.
simTracer :: SomeTracer (IOSim s)
simTracer = mkTracer (\(SomeEvent e) -> traceM e)

simTracerSay :: SomeTracer (IOSim s)
simTracerSay = mkTracer (\(SomeEvent e) -> traceM e >> say (… eventSeverity e <> " " <> renderEvent e …))

-- Holders: monomorphic field, record-dot read works.
data Holder m = Holder { hTracer :: SomeTracer m }
holderRun h e = runTracer h.hTracer e        -- PASS
```

**Observed compile: PASS** (clean under `-Wall`, no GADT/rank-N complications in holder
code). **Observed runtime output** (built with `-main-is SomeEventSyn`, run):

```
[EventA "alpha"]                          -- single-type typed recovery
[EventA "alpha",EventA "beta"]            -- two constructors of one sum type
[EventB 7]                                -- second type from the same tracer, no cross-talk
[EventA "said"]                           -- say-carrier still recovers by type
["collected","b=9"]                       -- IO collecting tracer through the existential
[EventA "via-holder"]                     -- holder + record-dot path
[]                                        -- TRAP: tracer that traces SomeEvent itself
null ok
```

The last line is the one faithfulness trap to engineer against: if `simTracer` traced the
`SomeEvent` wrapper (`mkTracer traceM` at `SomeEvent`) instead of unwrapping it, the stored
`Dynamic` is `SomeEvent` and every existing `selectTraceEventsDynamic … :: [WorkflowEvent]`
assertion silently returns `[]`. With unwrapping it recovers exactly the concrete domain
types listed in §1.2, in order.

**Construction detail:** `contramap SomeEvent (fastLoggerTracer logger)` does **not**
typecheck — `contramap` demands an unconstrained `b -> SomeEvent` and the constructor needs
`(LogEvent b, ToLogStr b, Typeable b)`:

```
error: [GHC-39999] No instance for ‘LogEvent SomeEvent’ arising from a use of ‘SomeEvent’
```

The `mkTracer $ \(SomeEvent e) -> …` lambda form (captured dictionaries in scope) is the
working spelling; a `ToLogStr SomeEvent` instance is possible but unnecessary.

**Does it satisfy "one launch-owned tracer field per holder"?** Yes. Each holder still stores
exactly one `Tracer m SomeEvent` value; `Connection`, `Ctx`, `PostgresSystemDB`, `Notifier`
keep one field each, and all 33 `SomeTracer` signature lines can stay as-is if the synonym is
kept. `SomeTracer m` also becomes an ordinary `Tracer`, so contra-tracer's `Semigroup`/`Monoid`
(`contra-tracer-0.2.1.1 Control/Tracer.hs:182-190`) now apply to it — the current GADT has no
instances.

**Cost: the smallest of all candidates.** Bodies only:
`DBOS/Transact/Logger.hs:96-115` (type + 3 backends), `test/DBOS/IOSimTracer.hs:31-41`,
`test/DBOS/LoggerTest.hs:56-64`, `test/DBOS/SystemDB/NotifierTest.hs:119-120`, plus export
lists `src/DBOS/Transact/Logger.hs:19` and `src/DBOS/Transact.hs:59` (`SomeTracer (..)` → `SomeTracer`,
add `SomeEvent (..)`) and imports at `test/DBOS/LoggerTest.hs:15`,
`test/DBOS/SystemDB/NotifierTest.hs:32`, `test/DBOS/Transact/ContextTest.hs:58`,
`test/DBOS/SystemDB/IOSim.hs:80`. All 10 sim-tree files and all 82 `simTracer*` uses are
untouched; all `selectTraceEventsDynamic` assertions are untouched. If the alias name is
deleted instead, add ~33 signature edits — the name is the cheap lever, not a semantic one.

---

## 4. Candidate 3 — `newtype SomeTracer m = SomeTracer (forall e. …)`

Identical payload, `data` → `newtype`. **Observed: PASS** (prototype `NewtypeWrap.hs`
compiles). This is a rename of the same encoding, not a removal: nothing about the erasure or
the rank-N argument changes, and `SomeTracer (..)` imports/pattern matches stay. No reason to
do it unless a coercion identity is wanted (none is used today).

---

## 5. Candidate 4 — contravariant composition (`Divisible` / `divide`)

**Observed: FAIL.** `contra-tracer` defines only `Contravariant` (`:173`), `Semigroup`
(`:182`) and `Monoid` (`:188`) for `Tracer`; no `Divisible`, no `Decidable` (source grep of
`contra-tracer-0.2.1.1/src/Control/Tracer.hs`). The prototype:

```haskell
divideTracers ta tb = divide id ta tb
-- error: [GHC-39999] No instance for ‘Divisible (Tracer IO)’
```

Even with an orphan instance, `divide` composes tracers over a *product* input
(`c -> (a, b)`); a single holder that must serve every domain needs a sum/union
(`Decidable.choose` over `Either`, also not instantiated). Building that union by hand gives
a **closed** sum of every event ADT: every emitting module would import the union owner, and
the union grows with each new domain — precisely the coupling the one-carrier design exists
to avoid (ADR-0015:15). The open version of that sum is candidate 2's `SomeEvent`. This
candidate neither removes the erasure nor satisfies the one-field constraint more cheaply.

---

## 6. Churn accounting (grep counts, 2026-10-01)

| Metric | Count |
|---|---|
| `conn.connTracer` record-dot reads in src | 39 |
| `env.psdbLog` dot reads / `notifier.log` dot reads | 4 / 3 |
| `contextTracer ctx*` calls (survive every candidate) | 16 |
| Lines containing `SomeTracer` in src+test | 58 (`rg -c SomeTracer src test` summed) |
| Signature lines `:: … SomeTracer` | 33 |
| True construction sites (`SomeTracer (…)` or backend wrappers to rewrite under C2) | `src/DBOS/Transact/Logger.hs:108-109,114-115`; `test/DBOS/IOSimTracer.hs:32,38`; `test/DBOS/LoggerTest.hs:58`; `test/DBOS/SystemDB/NotifierTest.hs:120` |
| `simTracer*` uses in test files | 82 across 10 files (unchanged under C2) |
| `SomeTracer (..)` import/export items that would warn on a synonym (`-Wdodgy-imports`/`-Wdodgy-exports`, observed) | `src/DBOS/Transact/Logger.hs:19`, `src/DBOS/Transact.hs:59`, `test/DBOS/LoggerTest.hs:15`, `test/DBOS/SystemDB/NotifierTest.hs:32`, `test/DBOS/Transact/ContextTest.hs:58`, `test/DBOS/SystemDB/IOSim.hs:80` |

## 7. Exact commands run

```sh
# compile-only prototypes (from /Users/duke/dev/dbos-transact-hs)
cabal exec -- ghc -fno-code -XGHC2024 -XDuplicateRecordFields -XNoFieldSelectors \
  -XOverloadedLabels -XOverloadedRecordDot -Wall <proto>.hs
# multi-module prototypes (FacadeSyn/FacadeImport)
cabal exec -- ghc -fno-code -i<protodir> -XGHC2024 … -Wall <protodir>/FacadeImport.hs
# runnable candidate-2 check (exit code 0, output in §3)
cabal exec -- ghc -XGHC2024 … -XScopedTypeVariables -Wall -main-is SomeEventSyn \
  -outputdir <out> -o <bin> <protodir>/SomeEventSyn.hs && <bin>
```

Scratch modules: `RankNField.hs`, `RankNFieldPat.hs`, `RankNFieldPat2.hs`, `RankNFieldPat3.hs`,
`RankNFieldSel.hs`, `RankNFieldAccess.hs`, `RankNGHC2024.hs`, `TypeSyn.hs`, `TypeSynList.hs`,
`FacadeSyn.hs`, `FacadeImport.hs`, `SomeEventSyn.hs`, `NewtypeWrap.hs`, `Divisible.hs`,
`ExportSyn.hs`.

## 8. Recommendation

**Remove the GADT wrapper; keep the name as a type synonym over the existential-event
encoding (candidate 2).**

- Candidate 2 is the cheapest faithful replacement: 5 bodies + 6 import/export items
  (§3/§6); zero changes to the 82 sim-tracer uses and to all typed `selectTraceEventsDynamic`
  assertions, which were run and verified to recover `[EventA]`/`[EventB]`/mixed lists
  exactly as today. The sim builders must unwrap (`\(SomeEvent e) -> traceM e`); tracing the
  wrapper silently breaks §1.2 (observed `[]`).
- Candidate 1 is the only encoding that *deletes* the carrier type, and it compiles — but
  only via pattern-match accessors: record-dot reads of a higher-rank field are
  unsolvable by GHC (`HasField`, GHC guide §6.5.9.1; unaffected by `ImpredicativeTypes`) and
  the repo mandates record-dot with `NoFieldSelectors` (`dbos-transact-hs.cabal:10,13`,
  AGENTS.md). It also loses first-class storage (`[SomeTracer m]`, GHC-91510). Not cheapest.
- Candidate 3 is a rename, candidate 4 has no `Divisible (Tracer m)` instance and needs a
  closed union — reject both.
- Paper trail required by the HARD RULES: ADR-0015:3-8 (the GADT definition) and ADR-0017:5
  (`runTracer`) need a dated amendment; `.lavish/rust-port-plan.html:329` and AGENTS.md:85
  ("`SomeTracer` GADT over contra-tracer's Rank-N shape") need the new spelling. Emission
  discipline ("only `runTracer`") is unchanged.

## Sources

Repo (file:line): `src/DBOS/Transact/Logger.hs:19,25-30,96-97,102-103,108-109,114-115` ·
`src/DBOS/Transact/Connection.hs:109,117,151` · `src/DBOS/Transact/Context.hs:97,181,193-200` ·
`src/DBOS/SystemDB/Postgres.hs:1165,1171,1196,1241,1278,1530-1533,1689,2543` ·
`src/DBOS/SystemDB/Postgres/Notifier.hs:108,113,155,183,230` ·
`src/DBOS/SystemDB/Retry.hs:126-133,147` · `src/DBOS/Transact/Instance.hs:82-102,216,218,328,462,465,468,501` ·
`src/DBOS/Transact/Dequeue.hs:296-303` · `src/DBOS/Transact/Recovery.hs:67` ·
`src/DBOS/Transact/Management.hs:90,113,142,207,208,237` ·
`src/DBOS/Transact/Workflow.hs:121,225,246,261,265,268,271,283,287,290,542,583,588,679` ·
`src/DBOS/Transact/Step.hs:175,187,190,214,371,383,432,435,446,470,477,481,485` ·
`src/DBOS/Transact/Sleep.hs:65,75` · `src/DBOS/Transact/Wait.hs:88` · `src/DBOS/Transact.hs:59,366` ·
`dbos-transact-hs.cabal:7-13,21,34` · `AGENTS.md:85` · `.lavish/rust-port-plan.html:329` ·
`docs/adr/0014-tracer-over-contra-tracer.md:3,5,7` · `docs/adr/0015-universal-tracer-no-co-log.md:3-21` ·
`docs/adr/0016-dual-stack-testing.md:3,24` · `docs/adr/0017-tracer-runner-event-homing-test-sim.md:5,9` ·
`docs/io-sim-simulations.md:4-11,54-63,105-107`; tests: `test/DBOS/IOSimTracer.hs:31-41` ·
`test/DBOS/LoggerTest.hs:36,56-64` · `test/DBOS/Transact/ContextTestSim.hs:174-181` ·
`test/DBOS/Transact/StepTestSim.hs:143-162` · `test/DBOS/Transact/WorkflowTestSim.hs:1626` ·
`test/DBOS/Transact/ManagementTestSim.hs:326` · `test/DBOS/SystemDB/RetryTest.hs:111` ·
`test/DBOS/SystemDB/NotifierTest.hs:107,119-130` · `test/DBOS/SystemDB/PostgresTest.hs:194` ·
`test/DBOS/SystemDB/IOSim.hs:195,261,267,297,314,320` · `test/DBOS/Transact/ContextTest.hs:109,134,142`.

Packages: `contra-tracer 0.2.1.1` `Control/Tracer.hs:171` (`newtype Tracer m a = Tracer { runTracer :: TracerA m a () }`),
`:194` (`mkTracer`), `:199` (`traceWith`), `:173,182,188` (only Contravariant/Semigroup/Monoid);
`io-sim 1.11.0.0` `Control/Monad/IOSim.hs:214` (`selectTraceEventsDynamic :: forall a b. Typeable b => SimTrace a -> [b]`),
`Control/Monad/IOSim/Types.hs:148` (`traceM :: Typeable a => a -> IOSim s ()`); resolved versions from
`dist-newstyle/cache/plan.json`.

Docs: GHC User's Guide §6.4.20 Arbitrary-rank polymorphism
(https://downloads.haskell.org/ghc/latest/docs/users_guide/exts/rank_polymorphism.html —
rank-N legal as field type; "GHC will not instantiate type variables to a polymorphic type");
§6.5.9.1 Solving `HasField` constraints
(https://downloads.haskell.org/ghc/latest/docs/users_guide/exts/hasfield.html — higher-rank
fields do not give rise to `HasField` solutions, and user instances are prohibited);
§6.5.10 Overloaded record dot
(https://downloads.haskell.org/ghc/latest/docs/users_guide/exts/overloaded_record_dot.html —
`.` desugars to `getField`/`HasField`); §6.4.7 Existentially quantified data constructors and
§6.4.9 GADTs (https://downloads.haskell.org/ghc/latest/docs/users_guide/exts/existential_quantification.html,
…/gadt.html); §6.4.21 Impredicative polymorphism
(https://downloads.haskell.org/ghc/latest/docs/users_guide/exts/impredicative_types.html).
contra-tracer Hackage: https://hackage.haskell.org/package/contra-tracer-0.2.1.1/docs/Control-Tracer.html.
io-sim Hackage: https://hackage.haskell.org/package/io-sim-1.11.0.0/docs/Control-Monad-IOSim.html.

Recorded 2026-10-01.
