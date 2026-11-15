# 02 — The two inside-transaction send pipelines in `Backend`

File: `src/DBOS/Internal/SystemDB/Postgres/Backend.hs` (4702 lines, +73/−90)
Regions: `deliverTo` at `:2549-2618`, `sendBatchInTx` at `:2654-2703`,
`sendInternal` at `:2534-2541`.

## What the commit fixed, and what it stopped short of

The old code walked forks in a separate transaction and then inserted in another,
which meant a fork created between the two walks could be missed entirely. The
commit deleted the outside-transaction `expandForks`/`descendantForks` pair and
moved the walk inside `deliverTo`'s transaction (`descendantForksTx`, `:2624-2634`).
That is the correct fix, and the comment at `:2543-2548` now states the rule.

But the module still holds **two implementations of the same
inside-transaction pipeline**, and the commit left both in place:

- `deliverTo` (`:2549-2618`) inlines the expansion, the four parameter
  projections, and the `SendMessagesParams` record.
- `sendBatchInTx` (`:2654-2703`) already had `pairMessages` / `expandPaired` /
  `insertExpanded` doing the same thing, one call deeper.

They differ in exactly three ways, and none of them is a reason for two bodies:

| aspect | `deliverTo` | `sendBatchInTx` |
|---|---|---|
| step record | `sendStep <$> caller` (`:2585-2595`) | `Nothing` (`:2701`) |
| return value | `(destination, topic)` pairs for waking waiters (`:2606`) | `()` |
| fallbacks | generated in IO before the transaction (`:2551`) | passed in by the caller |

Everything else is copy-paste, measured on the current file:

| duplicated piece | `deliverTo` | `sendBatchInTx` |
|---|---|---|
| fork expansion comprehension | `:2572-2577` | `:2680-2685` |
| `sendParamsDestinations` | `:2578` | `:2693` |
| `sendParamsTopics` | `:2579` | `:2694` |
| `sendParamsPayloads` | `:2580` | `:2695` |
| `sendParamsMessageIds` + `messageUUIDForSend` | `:2581-2584` | `:2696-2699` |
| duplicate-key rule (comment + `validateSendKeys`) | `:2537-2539` | `:2664-2669` |
| `SendMessagesParams` assembly | `:2596-2604` | `:2692-2702` |

That is roughly 28 duplicated lines, two copies of the same list comprehension,
and two copies of the same justification comment. Worse, the atomicity rule —
walk, insert, and step record are one commit — is stated in one place (`:2543`)
and re-derived in another (`:2673`), so the next change to the recipient set has
two places to remember.

## The code-judo move

One `Tx`-level batch function that owns the whole inside-transaction pipeline,
plus one pure projector:

```haskell
-- | Deliver a batch inside the caller's transaction: the fork walk, the
-- insert, and the optional step record are one commit, so a fork created
-- concurrently with a send is either walked with it or created after it.
-- Returns the (destination, topic) pairs the same-process wake hull needs.
deliverBatch :: Maybe Text -> [EncodedSendMessage] -> [Text]
             -> Maybe Statements.SendStep -> Bool
             -> Tx.Transaction (Either Error [(Text, Text)])

sendParamsOf :: Maybe Text -> Maybe Statements.SendStep
             -> [(EncodedSendMessage, Text)] -> Statements.SendMessagesParams
```

`deliverBatch` is `pairMessages` → `expandPaired` → `insertExpanded`, hoisted out
of `sendBatchInTx`'s `where` and turned into one flat body; `sendParamsOf` is the
four projections lifted verbatim from `:2596-2604`. Then:

- `deliverTo` becomes: generate fallbacks and timing in IO (unchanged), open the
  transaction, run the ownership check (`Statements.checkOwnerTx`, `:2560-2566`),
  call `deliverBatch … (sendStepFor caller) sendToForks`, then map the returned
  pairs into `wake` calls and the `foreignKeyError` mapping. Its 70 lines drop to
  roughly 35, and the expansion/params block disappears from the IO layer.
- `sendMessageInTx` / `sendBatchInTx` become `deliverBatch serialization messages
  fallbacks Nothing sendToForks`, matched for `Left` → `Tx.condemn`.
- `sendInternal`'s separate `validateSendKeys` guard (`:2537-2541`) goes away: the
  rule now lives once, in `pairMessages`, and a refusal inside the transaction
  condemns a transaction that wrote nothing, which is exactly what the client
  path already does. The empty-batch early return at `:2535` stays, because it
  is an IO-level short circuit that avoids opening a transaction at all.

This is not a refactor that moves complexity around. It **deletes one of two
pipelines**, deletes one of two copies of the atomicity comment, and puts the
"walk and write in one transaction" invariant in exactly one function.

## Two things not to "simplify" while doing it

- **Fallbacks and `StepTiming` are computed in IO before the transaction**
  (`:2551-2552`). That mirrors the oracle: Rust fixes
  `fallback_ids` and `StepTiming { started_at: now(), completed_at: now() }`
  immediately before opening its transaction
  (`crates/dbos/src/sysdb/postgres.rs:8323-8332`), because a retry after a lost
  commit acknowledgement must not generate fresh ids. Moving them inside the
  transaction would be a behavior change.
- **The ownership check stays inside the transaction.** It is a write fence
  (`checkOwnerTx`), not a pre-flight read; hoisting it would reopen the window
  the fence exists to close.

## Verification

The behavior is covered end to end already; this change moves no wire bytes and
no SQL.

- `test/DBOS/Transact/MessageCases.hs` (send / sendBulk, including the
  replay-adopts-the-batch case and the `DBOS.sendBulk` step name).
- The client in-transaction paths through `withClientTransaction`
  (`clientSendMessagesInTx`).
- Sim twins under `MessageTestSim` / the mock backend's `sendMessages`, which
  exercise the same `validateSendKeys` rule (`test/DBOS/SystemDB/IOSim.hs:2509`).
- Oracle cross-check: `cargo test -p dbos --test messages` in
  `~/dev/dbos-transact-rust` remains the behavioral reference for the single
  `deliver` shape this collapses to.

Ordering note: land this after finding 01 (Context) so each commit is
independently green, and run the watcher sims plus the live message group.
