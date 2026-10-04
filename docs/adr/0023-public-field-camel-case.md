# ADR-0023: Public record fields are camelCase (Haskell spelling, Rust domain name)

Date: 2026-10-04. Branch: `proto/phantom-brands`. Amends the
`AGENTS.md` hard rule that previously kept Rust field spelling
verbatim.

## Context

The port kept Rust's snake_case field names verbatim
(`workflow_id`, `worker_concurrency`, `max_attempts`, …). That was the
original reading of the Rust-fidelity rule: the *name* travelled with
the field. In practice the spelling fought Haskell convention at every
dot-read and record pattern, and dot accessibility is now load-bearing:
records are read with `OverloadedRecordDot` and record patterns, and
accessor functions survive only where a field is hidden or a read is
derived (the record-access preference order, `AGENTS.md`). A snake_case
field invites one more accessor (`handleWorkflowId`,
`stepStatusId`-style) or reads as a wart (`status.step_id`).

The domain vocabulary is what must stay verbatim; the spelling
convention is Haskell's.

## Decision

1. **Rule.** Types, constructors, and fields keep the Rust DBOS
   *domain name* verbatim, spelled per Haskell convention: PascalCase
   types and constructors, camelCase fields and values. No prefixes, no
   renames for taste, no field splitting. This is the amended hard
   rule in `AGENTS.md`.
2. **Public surface first.** The fields reached through the facade
   (`DBOS.Transact` / `DBOS.SystemDB` re-exports) convert in this
   slice: `Queue`/`QueueOptions`/`QueueChange` (7 each), `StepOptions`
   (4), `WorkflowKey` (2), `Enqueue` (3), `SendOptions` (1),
   `ClientConfig` (7), `EnqueueOptions` (5), `WorkflowHandle`
   (`workflow_id` → `workflowId`, landed with the seam refactor). The
   record-access rule applies after conversion: dot reads, dot
   sections, puns, record patterns.
3. **Internal records convert as touched.** Hidden and internals-only
   records (`StepStatus.step_id`, `DBOS.dbos_*`, `Executor.listen_queues`,
   the `*RowRaw`/`*Params` statement shapes) keep Rust spelling until a
   slice touches them; conversions are recorded per slice, never done
   as a blind sweep.
4. **Wire formats do not move.** SQL text (column names), Aeson keys,
   environment/config key strings, and log prose (`workflow_id=…`)
   are not Haskell identifiers and keep their exact spelling.

## Consequences

- Dot reads and patterns spell fields the Haskell way; hidden state
  stays behind derived readers (`stepStatusId`-style) or is exposed for
  reads when it is a plain value.
- No accessor is added to bridge a spelling mismatch — a snake_case
  field name is the deviation, not an accessor's justification.
- Rust citations in comments keep the Rust spelling (`@workflow_id@`);
  code identifiers do not.
- The oracle comparison stays structural: field *names* are the
  composed word from Rust, only the case convention differs, so
  cross-referencing a Rust field remains mechanical.
