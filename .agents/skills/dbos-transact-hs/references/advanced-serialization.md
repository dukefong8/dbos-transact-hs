---
title: Serialization of Durable Values
impact: LOW
impactDescription: Inputs, outputs, events, and messages must round-trip
tags: advanced, serialization, codec
---

## Serialization of Durable Values

`encodeWorkflowValue` / `decodeWorkflowValue` convert Haskell values to the durable codec (`Serialization`, `SerializedWorkflowValue`). Step inputs/outputs, workflow inputs/results, event and message payloads all flow through it.

```haskell
let stored = encodeWorkflowValue (orderId :: Int)
case decodeWorkflowValue stored :: Either CodecError Int of
  Right v -> pure v
  Left e  -> fail (show e)
```

Keep payloads plain (`Int`, `Text`, records of plain fields); unwrap newtypes at the edge. `configSerializer` selects the wire format (default matches the Rust oracle). `CodecError` on decode means the stored bytes do not match the expected type — a schema change, not a transient failure.

Difference from TS: TS defaults to SuperJSON with custom serializers. Haskell defaults to the Rust-compatible serde JSON; same interop contract (`docs/cross-language-schema-interop.md`).
