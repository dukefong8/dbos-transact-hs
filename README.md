# DBOS Transact Haskell

An unofficial Haskell port of the official [DBOS Transact](https://github.com/dbos-inc/dbos-transact-rust) library, bringing lightweight, durable execution and transactional workflows to Haskell on top of PostgreSQL.

---

## 1. What is this project?

`dbos-transact-hs` is an unofficial Haskell port of the official GitHub DBOS Rust library ([`dbos-transact-rust`](https://github.com/dbos-inc/dbos-transact-rust)).

DBOS provides lightweight durable workflows built directly on PostgreSQL. It allows you to write long-lived, highly reliable code that survives crashes, restarts, and network failures without losing state or duplicating work:

- **Durable Checkpoints:** As your workflow runs, DBOS checkpoints the completion of each step in Postgres.
- **Automatic Recovery:** When a process crashes or is restarted, DBOS automatically restores execution state from checkpoints and continues from the last completed step as if nothing happened.
- **Zero Orchestrator Daemon:** DBOS is just a library. There are no external orchestrators, sidecars, or brokers to operate—Postgres is the only dependency.

### Transactional Step Support

A major enhancement over the current `dbos-transact-rust` (as noted in its [widget-store README.md](https://github.com/dbos-inc/dbos-transact-rust/blob/main/demo-apps/dbos-rust-widget-store/README.md#a-note-on-transactional-steps)) is full support for **transactional steps** (`DBOS.Transact.Datasource`).

In the `dbos-transact-hs` [widget-store demo code](demo-apps/dbos-hs-widget-store/WidgetStore/Workflows.hs):

- **Single Commit:** Application database writes and the workflow step checkpoint commit together in a single atomic database transaction.
- **Exactly-Once Semantics:** The step body and its checkpoint succeed or fail together. If a transaction conflicts or fails, it automatically rolls back cleanly; on replay, the recorded outcome is returned without re-executing the transaction body.
- **Automatic Serialization Retry:** Transient transaction conflicts (e.g. Postgres `40001` serialization failures) are automatically caught and retried with backoff.

---

## 2. How to run the demo app

```bash
make db-migrate
make widget-db
cabal run demo-apps
```

- Starter demo: <http://localhost:8080/starter/>
- Widget store demo: <http://localhost:8080/widget-store/>

---

## 3. How to use it as a Cabal library

```bash
git clone https://github.com/dukefong8/dbos-transact-hs.git
```

```cabal
-- cabal.project
packages:
  .
  ../dbos-transact-hs/
```

```cabal
-- your-app.cabal
build-depends:
    base >= 4.21,
    dbos-transact-hs,
    text,
    aeson
```

All core types and operations are exported from `DBOS.Transact`.

See demo apps for code examples:

- `demo-apps/dbos-hs-starter/`: Workflows, steps, durable sleeps, event communication, and queues.
- `demo-apps/dbos-hs-widget-store/`: Transactional steps (`runTxStep`) committing application writes and checkpoints together.

---

## 4. How it was developed

- Most code was written by an autonomous coding agent using non-frontier AI models (mostly Tier 3 "flash" models).
- Harnessing Haskell strengths to steer design and verification:
  - MTL-style polymorphic backends ([`io-classes`](https://hackage.haskell.org/package/io-classes))
  - Dual-stack `cabal test` running against live backends and IOSim ([`io-sim`](https://hackage.haskell.org/package/io-sim))
  - Advanced type/effect compile-time invariants harnessing coding agent
  - Integrated `ghciwatch` fast reload feedback loop with coding agent
- Development commands:
  - `make env`: Provision the local GHC package environment (`.ghc.environment.*`) for GHCi and tooling.
  - `make dev`: Launch `ghciwatch` for fast reloads and eval-based test feedback on file changes.
