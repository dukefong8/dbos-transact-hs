# AGENTS.md

Repo guide for DBOS Haskell.

## Structure

- `src/DBOS/Transact.hs` is the public DBOS transaction facade.
- `src/DBOS/<Domain>/*` holds internal parsers, replay logic, and types.
- `test/DBOS/<DomainTest>.hs` holds Tasty specs.
- `docs/` holds durable engineering notes, workflow guidance, research context, and ADRs.
- `docs/adr/` records architectural decisions.
- `.lavish/` holds visual review artifacts; keep them consistent with committed code and mark proposal-only content clearly.
- `CONTEXT.md` is the glossary for domain language only.

## TDD Loop

1. Run or watch `make dev` first.
2. Monitor `.ghcid.txt` and fix compiler errors before widening the slice.
3. Use `ghci -e ':hoogle ...'` and `ghci -e ':browse ...'` before adding any new dependency.
4. Write one public Tasty test at a time.
5. Implement the smallest code that passes that test.
6. Run `cabal test` after each green compiler cycle.

Use Neovim LSP document symbols to inspect module structure and exported surfaces before changing a module layout.

## End-to-End Verification

For DB-backed behavior, a green `cabal test` is not the final gate. After every successful `cabal test`, run a direct `psql` query against the local Postgres `dbos` database and verify the rows the tests are expected to create or read.

Use this session's mirror-workflow gate as the default shape:

```sql
select 'workflow_status' as table_name, workflow_uuid as id, status, name
from dbos.workflow_status
where workflow_uuid in ('hs-wf-1', 'hs-op-wf', 'hs-notification-wf', 'hs-simple-wf')
union all
select 'operation_outputs', workflow_uuid || ':' || function_id::text, null, function_name
from dbos.operation_outputs
where workflow_uuid in ('hs-op-wf', 'hs-simple-wf')
union all
select 'notifications', message_uuid, null, topic
from dbos.notifications
where message_uuid = 'hs-message-1'
order by table_name, id;
```

Expected rows include the mirrored simple workflow status row (`hs-simple-wf`, `SUCCESS`, `TryConcExec.testConcWorkflow`) and its operation output row (`hs-simple-wf:1`, `TryConcExec.testConcStep`). Adjust the IDs only when the test fixture intentionally changes.

## Guardrails

- Keep tests on public behavior, not implementation details.
- Do not refactor while red.
- Do not add schema migrations in Haskell.
- Keep Python DBOS schema compatibility as the boundary.
- Use Bluefin 0.7 scoped capabilities with `IOE` for effectful DBOS code; do not add DBOS-specific `Handle` records.
- Use the existing tmux `make env` / `ghciwatch` pane; do not start duplicate watchers.
