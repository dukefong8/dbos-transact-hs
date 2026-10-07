---
title: Workflow Messages
impact: MEDIUM
impactDescription: Direct durable messaging between workflows
tags: communication, messages
---

## Workflow Messages

`send` delivers a durable message to a workflow id on an optional `Topic`; `recv` exclusively receives on a topic with a timeout (`Nothing` on timeout). Messages survive crashes; consumed messages are checkpointed.

```haskell
_ <- send wctx destination (Just (Topic "approval")) Nothing (encodeWorkflowValue payload)
m <- recv wctx (Just (Topic "approval")) (secondsDuration 60)
```

Bulk variants: `sendBulk` / `sendBulkWith` with `SendBulkOptions`. From outside use `sendWorkflowMessage` / `sendWorkflowMessages` (executor) or `clientSendMessage(s)` (client). `IdempotencyKey` dedups sends.

No streaming in this port: TS `comm-streaming` (`readStream`) has no Haskell equivalent.
