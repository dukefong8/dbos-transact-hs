# ADR-0034: Single-value stream read omitted

Date: 2026-10-14

## Context

The Rust oracle has two stream-read entry points (`crates/dbos/src/stream.rs`):
**`StreamReader::next`**, which delivers each value as a `DBOS.read_stream`
step, and **`read_stream_value`**, which reads the one value at an offset as
a `DBOS.read_stream_value` step (a `StreamTimeout` if the stream ends first).
The two reads record the same rows under different names on purpose, so a
workflow changed from one to the other is caught on replay as
`UnexpectedStep` rather than handed a value the other read recorded.
An end before the offset is a recorded end plus a raised timeout, not a
clean end.

Python agrees on the semantics but not the name (`dbos/_core.py`):
**`read_stream_offset`** is a thin layer over the same value generator under
`DBOS.readStreamOffset`, and raises `DBOSStreamTimeoutError` when the stream
ends before the offset — after the generator has recorded the end. Its
generator also reserves one step per delivered value before the wait, with a
per-value deadline, matching the Rust shape. There is therefore no single
cross-SDK single-read name to conform to (`DBOS.read_stream_value` vs
`DBOS.readStreamOffset`).

The Haskell port exposes only the pull reader (`readStream` /
`readStreamFrom`, adapted by `toVectorStream` / `foldVectorStream`). A
single-offset read is expressed as a one-shot reader at that offset — the
shared test helper `readStreamHead` (`test/DBOS/Transact/StreamCases.hs`)
shows the pattern — recorded under `DBOS.read_stream`.

## Decision

Deliberately do not port `read_stream_value` or the `DBOS.read_stream_value`
step name. One reader API means one recorded name, so the cross-name replay
hazard the oracle's second name guards against cannot arise inside the port:
there is no second spelling to switch to. The reader covers the single-offset
use case (`readStreamFrom` at the offset, first pull), including resume-where-
a-reader-left-off.

## Consequences

- A workflow recorded by the Rust oracle with `DBOS.read_stream_value` rows
  (or by Python with `DBOS.readStreamOffset` rows)
  will not have those rows adopted here (name mismatch on replay) — the same
  standing as any other unported API, and acceptable while no workflow
  crosses implementations by replay (workflows cross only by enqueue).
- To port it later, add a `readStreamValue` facade entry plus
  `readStreamValueStepName = "DBOS.read_stream_value"` recorded verbatim,
  reusing the existing page-of-one sysdb path, with live/sim cases mirroring
  the oracle's single-value and recorded-end tests.
