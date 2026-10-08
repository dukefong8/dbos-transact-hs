# ADR-0031: `startWorkflowRef` renamed to `startWorkflow`

Date: 2026-10-08. A rename under the HARD RULES: the split itself
(executor-side vs `startChildWorkflow`) stands — only the redundant
suffix goes.

## Context

Both start entries take a `WorkflowRef`. The `Ref` suffix is systematic
(`runWorkflow` takes a key, `runWorkflowRef` takes a ref) but
information-free on the start pair: starting by bare name does not exist,
so there is no key-taking `startWorkflow` to disambiguate from. The
oracles call the root start `startWorkflow` / `start_workflow`
(TS `DBOS.startWorkflow`, Python `DBOS.start_workflow`, Rust
`client.start_workflow`).

## Decision

Rename the facade entry `startWorkflowRef` to `startWorkflow`
(word-boundary sweep; `startWorkflowId` and `runWorkflowRef` are
untouched). The starter demo's route handler of the same name becomes
`startWorkflowRoute` — the collision the sweep found.

## Consequences

- Facade, skill refs, and parity audit move in the sweep; dated ADRs and
  plans keep the old name as evidence.
- No semantic change: same argument order, same root-start behavior.
