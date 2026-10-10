# dbos-transact-hs

> **Note:** `CLAUDE.md` is a symlink to this file when present.

## Overview

Haskell port of the DBOS durable-workflow SDK. Use this skill when writing Haskell code with `DBOS.Transact`, creating workflows and steps, using queues, using `Client` from external applications, or building applications that need to be resilient to failures.

Import only from `DBOS.Transact` in app code; anything else is engine-internal (private `dbos-transact-internals` lib).

## Structure

```
.agents/skills/dbos-transact-hs/
  SKILL.md       # Main skill file - read this first
  AGENTS.md      # This navigation guide
  references/    # Detailed reference files (implemented subset only)
```

## Usage

1. Read `SKILL.md` for the main skill instructions
2. Browse `references/` for detailed documentation on specific topics
3. Reference files are loaded on-demand - read only what you need

## Reference Categories

| Priority | Category | Impact | Prefix |
|----------|----------|--------|--------|
| 1 | Lifecycle | CRITICAL | `lifecycle-` |
| 2 | Workflow | CRITICAL | `workflow-` |
| 3 | Step | HIGH | `step-` |
| 4 | Queue | HIGH | `queue-` |
| 5 | Communication | MEDIUM | `comm-` |
| 6 | Pattern | MEDIUM | `pattern-` |
| 7 | Testing | LOW-MEDIUM | `test-` |
| 8 | Client | MEDIUM | `client-` |
| 9 | Advanced | LOW | `advanced-` |

Reference files are named `{prefix}-{topic}.md` (e.g. `step-retries.md`).

Only the implemented subset is present. TS-only topics (`lifecycle-express`, `pattern-classes`, `pattern-scheduled`, `comm-streaming`, `advanced-patching`, `advanced-upgrading`) are intentionally absent. Where the Rust oracle differs from TS (datasource naming, queue defaults, fork/rewind availability), the Haskell reference states the Haskell behavior and names the difference.

## Available References

**Lifecycle** (`lifecycle-`):
- `references/lifecycle-config.md`

**Workflow** (`workflow-`):
- `references/workflow-background.md`
- `references/workflow-constraints.md`
- `references/workflow-control.md`
- `references/workflow-determinism.md`
- `references/workflow-introspection.md`
- `references/workflow-timeout.md`

**Step** (`step-`):
- `references/step-basics.md`
- `references/step-nesting.md`
- `references/step-retries.md`
- `references/step-timeouts.md`
- `references/step-transactions.md`

**Queue** (`queue-`):
- `references/queue-basics.md`
- `references/queue-concurrency.md`
- `references/queue-deduplication.md`
- `references/queue-delay.md`
- `references/queue-listening.md`
- `references/queue-management.md`
- `references/queue-partitioning.md`
- `references/queue-rate-limiting.md`

**Communication** (`comm-`):
- `references/comm-events.md`
- `references/comm-messages.md`

**Pattern** (`pattern-`):
- `references/pattern-debouncing.md`
- `references/pattern-idempotency.md`
- `references/pattern-sleep.md`

**Testing** (`test-`):
- `references/test-setup.md`

**Client** (`client-`):
- `references/client-enqueue.md`
- `references/client-setup.md`

**Advanced** (`advanced-`):
- `references/advanced-serialization.md`
- `references/advanced-shared-database.md`
- `references/advanced-versioning.md`
