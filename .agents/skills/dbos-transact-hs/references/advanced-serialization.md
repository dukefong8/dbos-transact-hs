---
title: Serialization of Durable Values
impact: LOW
impactDescription: Inputs, outputs, events, and messages must round-trip
tags: advanced, serialization, codec
---

## Serialization of Durable Values

The durable codec is **portable JSON**, tagged `portable_json` (ADR-0038).
It is plain JSON, not SuperJSON and not Rust serde:

- **Workflow arguments** use the cross-SDK envelope
  `{"positionalArgs": [...], "namedArgs": {...}}` (`encodeWorkflowInput`).
  The engine unwraps it before the body runs: `()` is no arguments, one
  positional is the value, several are a tuple, named-only is the record.
- **Every other payload** (result, event, message, stream value) is a plain
  JSON value (`encodeWorkflowValue`).
- **Errors** are `{name, message, data}`; the structured `Error` envelope
  stays in `data`, so a replay reconstructs the original constructor.
- **Reads dispatch on the row's tag**: NULL, `""`, and legacy `rust_serde`
  decode as native plain JSON; an unknown tag is refused, never guessed.

The facade re-exports `encodeWorkflowInput`, `decodeWorkflowInput`,
`encodeWorkflowValue`, `decodeWorkflowValue`,
and `CodecError` (ADR-0038 addendum);
`SerializedWorkflowValue` is the wire type those functions exchange. The
lower-level `DBOS.Transact.Serialization` module stays internal, and its
extra helper (`encodeUnit`) is not part of the app
surface:

```haskell
import DBOS.Transact (CodecError, SerializedWorkflowValue (..), decodeWorkflowValue, encodeWorkflowInput)

let input = encodeWorkflowInput (orderId :: Int)          -- workflow argument
case decodeWorkflowValue "result" stored :: Either CodecError Int of
  Right v -> pure v
  Left e  -> fail (show e)
```

Keep payloads plain (`Int`, `Text`, records of plain fields); unwrap newtypes
at the edge. A `CodecError` on decode means the stored bytes do not match the
expected type or the tag is unreadable — a schema change, not a transient
failure. `Config.Serializer` has one value, `PortableJson`; there is no
serializer selection.

Difference from TS: TypeScript defaults to SuperJSON (`js_superjson`) with
custom serializers and chooses portable per call. This port always writes
portable JSON, so Python and TypeScript can read its rows; see
`docs/cross-language-schema-interop.md` and ADR-0038.
