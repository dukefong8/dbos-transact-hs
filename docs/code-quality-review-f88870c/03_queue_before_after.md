# 03 — Queue before/after: the pre-read is a port-only artifact

Files: `src/DBOS/Internal/Transact/Queue.hs` (`registerQueue` `:347-376`,
`updateQueue` `:409-428`, `warnIfPartitioned` `:435-439`),
`src/DBOS/Internal/Transact/Client.hs` (`clientRegisterQueue` `:610-643`,
`clientUpdateQueue` `:657-678`),
`docs/adr/0044-queue-before-after-collapse.md`.

## What the commit added

Registration now has to warn when a write turns an unpartitioned queue into a
partitioned one, because rows already enqueued without a key will never be
claimed again. `warnIfPartitioned tracer before after` needs the stored row
*before* the write, so both registration paths were given a pre-read:

- engine: `stored <- withConnection … getQueue`, then `upsertQueue`, then
  `case previous of Just before -> warn…; Nothing -> pure ()`
  (`Queue.hs:365-376`);
- client: the same read/upsert/warn with a different error-mapping shape
  (`Client.hs:633-643`).

The two update paths already had a pre-read for the same reason
(`Queue.hs:416-428`, `Client.hs:664-678`). So the sequence
**read stored → write → warn on the (before, after) transition** now exists four
times, and the engine registration copy is a six-level nest:

```
registerQueue
  → case validateQueueOptions
    → withExecutor
      → case resolveConflict
        → case stored            -- the pre-read
          → case upsertQueue
            → case previous of   -- only to fire a tracing event
```

That innermost `case` exists solely to decide whether to emit one warning line.
It is feature logic (a transition announcement) bolted into the middle of an
already busy registration flow — precisely the "ad-hoc branch inserted into an
unrelated path" shape the review bar treats as a design problem, not a nit.

## The deeper problem: ADR-0044's premise is now false

ADR-0044 records the decision to collapse Rust's `UpsertedQueue { before, after }`
and `UpdatedQueue { before, after }` into `(Bool, QueueRecord)` and
`QueueRecord`, justified by: *"no caller reads a post-call `before` row, so the
types would be structure nothing consumes."*

This commit makes that statement false. The partition warning **is** a caller
reading the post-call `before` row. The pre-read is the workaround for the
missing field, and it is a worse one:

1. **It is racy, and the code says so.** `Queue.hs:360-364` documents that "a
   peer racing between the read and the write can misplace the warning". The
   oracle has no such window: Rust reads `before` from the row the upsert itself
   locked and returns it (`sysdb/postgres.rs` `upsert_queue`; consumed at
   `queue.rs:753-760` and `queue.rs:861`, `updated.before.is_partitioned()`).
2. **It costs a second round-trip on every registration and update** — another
   connection acquisition plus a `select` whose result the write's own locked
   read already produced.
3. **The collapsed type is worse than the oracle's on both axes.** It carries a
   `Bool` that *no* call site reads — `Queue.hs:372` and `Client.hs:639/640/643`
   all discard it with `_` — and it omits the `before` row that every warning
   site now needs. So the collapse removed one piece of unused data and forced
   four call sites to reconstruct the missing piece racily.

That also means the repo's own HARD RULE ("one Rust type maps to exactly one
Haskell type, by name") is currently violated *for a reason that no longer
holds*. ADR-0044 cannot simply be amended to note the new reader; it should be
**superseded**, because its evidence section is contradicted by the code.

## Remedy A (preferred): restore the oracle pair, delete the pre-reads

The backend already holds both rows under the lock, so this costs no SQL:

- `upsertQueue` (`Backend.hs:3916-3957`) has the row at `:3947` for the
  update/leave branches and nothing for the created branch:
  `UpsertedQueue { before = Nothing, after = record }` at `:3942`,
  `before = Just record` at `:3952` and `:3957`.
- `updateQueue` (`:4105-4124`) has the before row at `:4110` and the written row
  at `:4120`: `UpdatedQueue { before = record, after = written }`.

Then each of the four call sites becomes straight-line:

```haskell
upserted <- withConnection executor.conn (\db -> SystemDB.upsertQueue db new policy)
case upserted of
  Left err -> pure (Left err)
  Right result -> do
    traverse_ (warnIfPartitioned executor.conn.connTracer `flip` result.upsertedAfter) result.upsertedBefore
    pure (Right (queueFromRecord result.upsertedAfter))
```

What disappears: two pre-read statements, two connection acquisitions per
registration, the six-level nest, the two duplicated race paragraphs, the
dead `Bool`, and the four-way duplication (there is nothing left to duplicate —
the transition is read off the write's own result). `created` becomes
`Base.isNothing . (.upsertedBefore)` if anything wants it, and nothing does.

What must be preserved: `updateQueue`'s validator still receives `(before,
merged)` under the lock (`validateMergedQueue`, `Queue.hs:444-451`), which is the
lock-scoped judgment ADR-0044 correctly defended. Returning the pair does not
weaken it.

## Remedy B (fallback, if the pair is refused for another reason)

If changing the class return types is genuinely out of scope, the duplication
still has a canonical home: one helper in `Queue`, the module both callers
already import and which already owns `warnIfPartitioned`:

```haskell
registerQueueWarned :: Monad m => Connection m -> NewQueue -> OnExistingQueue
                    -> m (Either Error QueueRecord)
```

It does read → upsert → warn and returns the record. Engine registration keeps
its `resolveConflict` step and calls it; the client path calls it directly. That
removes the six-level nest and three of the four copies, but it **keeps the
race** and therefore still needs the caveat comment — which is exactly why it is
second choice.

## Verification

- `test/DBOS/Transact/QueueTestSim.hs:296` asserts the `QueuePartitioned` event
  reaches a sim tracer on a re-register that partitions
  (`scenarioReregisterPartitioned`); the live twin asserts the stored partitioned
  row (`QueueCases.hs`, `checkReregisterPartitioned`).
- `test/DBOS/Transact/LoggerTest.hs:62` pins the rendered message text; the
  event's wording does not change here.
- Re-registration conflict semantics (`AlwaysUpdate` / `NeverUpdate` /
  `UpdateIfLatestVersion`), the reserved internal queue name, and the
  both-spellings refusal are untouched by either remedy.
- The sim and mock backends return the collapsed shapes today
  (`test/DBOS/SystemDB/IOSim.hs`); they must move in the same commit as the class
  change, and the ADR-0044 note in their comments goes with it.

Documentation obligation: whatever is chosen, ADR-0044's evidence bullet for
"engine `registerQueue` / client `clientRegisterQueue` use only the created flag
and the `after` row" must be rewritten or the ADR superseded, because this
commit's own warning contradicts it. Leaving it as-is means the next reader
trusts a rationale the code has already outgrown.
