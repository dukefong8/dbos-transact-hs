# Tracer runner, event homing, and test-owned sim (supersedes ADR-0015's catalog placement)

Three renames/relocations, all behavior-preserving (full sequential gate green throughout):

- `traceWith` is `runTracer`; `projectTracer` is deleted (zero uses — every consumer goes through the carrier, so nothing needs the narrow form).
- The five event ADTs move out of `DBOS.Tracer` to the module that owns their vocabulary: `EngineEvent` in `Transact.Recovery`, `SysdbEvent` in `SystemDB.Retry`, `WorkflowEvent` in `Transact.Step`, `QueueEvent` in `Transact.Dequeue`, `ManagementEvent` in `Transact.Management`. Placement follows the import graph, not the names: `Workflow.hs` and `Queue.hs` as owners would cycle (`Step` is imported for steps, `Instance` imports `Dequeue` through `Queue`), so the emitter-adjacent leaf owns each type. `DBOS.Tracer` keeps the carrier, backends, and `LogEvent`/`LogSeverity`; `showText` moves to `DBOS.Prelude`. Facade export names are unchanged.
- `DBOS.SystemDB.IOSim` moves to `test/` (`MockSystemDB`, renamed from `IOSimSystemDB`; `MemSystemDB` unchanged). Test code cannot see src-hidden modules, so `src` keeps one backend-agnostic builder — `launchOn :: DBOS m -> Connection m -> Identity -> m ()` in `Instance.hs`, taking the `SomeSystemDB`-wrapped backend the test builds via the facade. Mock constructors split per domain (`Context/Management/WorkflowSimData`, dup'd on purpose); the backend and the `TestSim` trees import them, never the reverse. Sim tracing consolidates in `test/DBOS/IOSimTracer.hs` (`simTracer`, `runSimCase`, `printSimTrace`, default builders).

Stream routing: FastLogger writes stderr (stdout carries results); sim announcements print to the watcher pane's stderr via `printSimTrace`, bypassing the `tasty` stdout capture, so `ghcid.txt` holds progress and results only.

Recorded 2026-09-30.
