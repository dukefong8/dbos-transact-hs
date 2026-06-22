# Changelog

## 0.1.0.0

- Replaced the initial scaffold with the `DBOS.*` module tree.
- Added the `DBOS.Transact` public facade for workflow execution parsing, operation checkpoint parsing/replay, notification value types, and Bluefin-scoped store capabilities.
- Added the `DBOS.SystemDB` Hasql-backed facade for Python-compatible `workflow_status`, `operation_outputs`, and `notifications` access.
- Added Tasty coverage for schema records, row parsing, scoped capability replay, live Hasql reads/writes, and the mirrored sync simple workflow.
