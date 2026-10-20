# ADR-0039: Non-finite floats keep `ToJSON` and pin aeson's deviation

Date: 2026-10-19. Status: accepted.

## Context

Rust's `refuse-non-finite-floats` branch (`origin/refuse-non-finite-floats`,
tip `e2fed10`) makes every encode path refuse a non-finite `f32`/`f64` in a
`Value`; only `rust_serde` round-trips them, as the JSON strings `"NaN"`,
`"Infinity"` and `"-Infinity"`. `portable_json` refuses them because the other
SDKs agree there is no cross-language form. The branch also reads float map
keys and serde_json's arbitrary-precision numbers, and routes the schedule
context and the engine's own error records through the same rule (a
`serde_json::Value` cannot hold a non-finite float).

The port cannot reproduce that under aeson 2.3.2.0:

- `toJSON (0/0 :: Double)` is already `Null`; the value is gone before the
  codec sees it.
- `encode (1/0 :: Double)` is the JSON string `"+inf"` and `-1/0` is
  `"-inf"`; aeson reads both back as `Infinity`.
- aeson does not read `"NaN"`, `"Infinity"` or `"-Infinity"` as a `Double`.
- `ToJSON` has no failure channel and no `serialize_f64`-style hook, so a
  refusal cannot be added without replacing the class every application type
  uses.

## Decision

Keep `ToJSON` and pin the observable deviation instead of reimplementing the
Rust refusal. This is option A from `TODO.md` S7.

- **No text rewrite.** A `Text` field holding `"+inf"` must stay a string;
  rewriting the spelling at the JSON level cannot distinguish it from a float,
  which is exactly the typed distinction the Rust deserializer preserves.
- **No custom payload class.** Option B would replace `ToJSON` with a
  port-owned class carrying a failure channel, churning every application
  type's instances for a behavior the portable format still refuses. It buys
  no cross-SDK capability.
- **Documented and pinned.** `SerializationTest` pins the aeson facts and the
  codec consequences; `DBOS.Transact.Serialization` names the deviation and
  points here.
- **Value-limited paths inherit the rule.** A schedule's context, the portable
  error record and the engine's own error data are `Value`s, so a non-finite
  float is `Null`/`"+inf"`/`"-inf"` there too; none is refused.

## Consequences

| Value | Port writes | Port reads back | Rust branch writes |
|---|---|---|---|
| `NaN` (`0/0`) | `null` | required `Double` reads `NaN` (aeson maps `null` to NaN); `Maybe Double` is `Nothing` | `"NaN"` |
| `+Infinity` (`1/0`) | `"+inf"` | `Infinity` | `"Infinity"` |
| `-Infinity` (`-1/0`) | `"-inf"` | `-Infinity` | `"-Infinity"` |

Rust's three names are unreadable as a `Double` here; a `Text` field keeps
whatever spelling it was given. A portable payload carrying a NaN is stored as
`null`, and aeson maps `null` back to `NaN` for a required `Double` while
`Maybe Double` reads `Nothing` — so the optional path loses the distinction.
Python reads `"+inf"` as a string rather than a float — phase P3 records that
cross-SDK consequence. Float map keys and
serde_json's arbitrary-precision numbers are not ported: aeson's `Map` has no
`Double` key instance and its `Scientific` already handles oversized numbers;
both are noted here rather than forced into a `Value` rewrite.

## Alternatives

- **B — port-owned payload class with a failure channel.** Rejected: every
  application type pays a new instance, and portable JSON still has no
  non-finite form, so the only gain is refusing a write the port can already
  see as `null`.
- **Text-level rewriting of the names.** Rejected: it cannot tell a float from
  a `String` field containing the same text.
