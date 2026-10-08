---
title: Workflow Constraints
impact: CRITICAL
impactDescription: Rules the type system and runtime enforce
tags: workflow, constraints, determinism
---

## Workflow Constraints

- Register workflows and datasources before `launch`; registering after is refused (created-before-launch rule).
- Bodies must be deterministic — same inputs plus same step outputs must produce the same step sequence.
- Do not call, start, or enqueue workflows from within step bodies.
- Start or enqueue from a workflow body only via `startChildWorkflow` (with `startQueue` to enqueue): the start is a recorded child step that replays. `enqueueWorkflow` / `startWorkflow` are outside-only; never capture instance values into a step body.
- Do not use uncontrolled concurrency to start workflows — use `startWorkflow` or queues.
- Do not mutate globals from bodies; keep checkpoint payloads JSON-serializable and plain (`Int`, `Text`).
- Steps are only durable when called from a workflow; outside, they run as plain calls.

Violations either fail the type checker (nested `runTxStep`), degrade silently (`runStep` in a step), or break replay (renamed steps).
