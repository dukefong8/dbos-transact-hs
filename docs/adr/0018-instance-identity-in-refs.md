# Instance identity in workflow references (reverses the Registry "never pins an instance" note)

Rust's `StepPlacement::of` compares `Arc::ptr_eq(ctx.executor().connection(), conn)`
and raises `Error::WrongInstance` when a start would take its step id from one
workflow's counter while the record lands through another instance's database
(`workflow.rs:908-913`). The port could not express that: `WorkflowRef` held only
its `Registry` and no instance identity, documented on purpose at the time
(`Registry.hs`: "capturing a reference never pins an instance"). The oracle case
`children.rs:1637` (`a_child_started_through_another_instance_is_refused`) needs
the refusal, decided 2026-09-30 ("carry instance identity").

Decision, in three parts:

- `Connection` carries `connInstanceId :: Text`, minted once per connection at
  `newConnection` — the analogue of the Rust `Arc<Connection>` pointer. Production
  draws it from the connection's UUID generator (`forApplication`, `Client`);
  the sim helpers mint deterministic ids, with the `MemSystemDB` counter living
  in the shared data so two launches over one mem database are still two
  instances.
- A launch binds the reference's registry to the connection it installed
  (`bindRegistryInstance`, called from both launch paths). Registration before a
  launch leaves the registry unbound.
- `startChildWorkflow` reads the reference's bound instance and refuses before
  anything is written and before the counter moves: `ErrorNotLaunched` when the
  reference's instance was never launched (the oracle's
  `self.dbos().executor(..)?`), `WrongInstance {operation = "start a workflow"}`
  when it names another connection. `InStep` still comes first, because a call
  inside a step checkpoints nothing whoever the connection belongs to.

Consequences:

- Reversal of the recorded note: a reference's child starts are scoped to the
  instance it was registered on. Top-level `startWorkflowRef`/`runWorkflowRef`
  keep taking the caller's connection explicitly and do not compare (the oracle's
  top-level run executes on the reference's own instance; the port has no such
  path, and the oracle has no case for it).
- The refusal is runtime, like the oracle's; the ownership map's committed
  phantom-brand direction (`Ctx s m`/`WorkflowRef s m`) would lift it to types
  later.
- `simConnectionWith`'s Mock counter is per call, so two Mock connections made
  side by side share an instance id; trees that stage the refusal use
  `MemSystemDB`, whose counter is shared. Documented at the helper.
- Found while landing this: the production `launchWithEnvironment` path never
  bound the registry (only `launchOn`, the test path, did), so every live child
  start refused with `ErrorNotLaunched`. Both paths bind now.

Evidence: live + sim case `a child started through another instance is refused`
(oracle `children.rs:1637` green); full suite 622/622 parallel; psql shows zero
child rows and every refused parent with no step rows.

Recorded 2026-09-30.
