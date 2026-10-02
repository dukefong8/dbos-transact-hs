# Widget store lifecycle evaluation

Date: 2026-10-02 · checks: **27/27 passed**

The Python run was a scripted scenario (crash after two dispatch ticks); the Haskell run drove the UI through a browser (same phases). The driver scripts were throwaway and live outside the repo.

## Lifecycle mirror

| lifecycle phase | Python (snooped events) | Haskell (production tracer) |
| --- | --- | --- |
| app launch | `app.session_start`, `app.launch.complete` | `EngineLaunched` |
| crash recovery scan | `recovery.scan` (`get_pending_workflows`), `recovery.start` | `EngineRecovered` (`workflows=N`) |
| checkout steps | `step.call create_order / reserve_inventory / update_order_status` | `TransactionRunning create_order / reserve_inventory / mark_order_paid` |
| transaction commit | `checkpoint.record` (same tx as the app write) | `TransactionOutputRecorded` |
| payment exchange | `event.set payment_id`, `event.recv.wait/return`, `event.send`, `event.get order_id` | `workflow_events` rows + `notifications` (no per-event tracer line today) |
| dispatch ticks | `step.call update_order_progress`, `sleep.call` | `TransactionRunning update_order_progress`, `SleepUntilWake` |
| replay after recovery | `step.replay` (outer checkpoint), `checkpoint.hit` (datasource pre-check) | `TransactionReplaying` |
| completion | `workflow.complete` | `WorkflowCompleted` |
| durable checkpoint rows | `dbos.datasource_outputs` (dropped on completion) | `widget_store.transaction_completion` (kept; clearing started workflows is a recorded follow-up) |

## Checks

- [x] **python.checkout.workflow** — starts=['checkout_workflow', 'dispatch_order_workflow', 'dispatch_order_workflow']
- [x] **python.checkout.steps** — steps={'create_order': 1, 'reserve_inventory': 1, 'update_order_status': 1, 'update_order_progress': 12}
- [x] **python.dispatch.ticks** — calls=12 executed=13 replayed=0
- [x] **python.checkpoint.write** — records=13 (each executed step writes one row in its transaction)
- [x] **python.messaging** — sets=['payment_id', 'order_id'] sends=1 recv=1 gets=2
- [x] **python.client.start** — client starts=['py-e2e-4', 'py-e2e-4-7']
- [x] **python.crash.recovery** — recovery scans=[0, 1] (pending ids found: [[], ['py-e2e-4-7']])
- [x] **python.crash.replay** — replayed outer checkpoints=5 (pre-crash dispatched ticks=2)
- [x] **python.crash.sessions** — sessions=2 (pre-crash and post-restart)
- [x] **python.crash.resume** — sleep calls=13 (the dispatch loop ran 10 ticks total)
- [x] **python.workflow.complete** — completes=[('checkout_workflow', 'None'), ('dispatch_order_workflow', 'None')]
- [x] **python.checkpoints.peak** — datasource_outputs peak=9 (sampled at 0.5s; the datasource drops the rows when the workflow completes)
- [x] **haskell.launch** — launches=2
- [x] **haskell.checkout.steps** — runs={'create_order': 1, 'reserve_inventory': 1, 'mark_order_paid': 1, 'update_order_progress': 11}
- [x] **haskell.dispatch.ticks** — distinct tick step ids=10 (runs=11 replays=2)
- [x] **haskell.checkpoint.write** — recorded=12
- [x] **haskell.workflow.ids** — completed=['e4400070-36d0-4ebb-b7f5-e4bdb17f2c87', 'e4400070-36d0-4ebb-b7f5-e4bdb17f2c87-6']
- [x] **haskell.crash.recovery** — recovered workflows=1 replayed ticks=2
- [x] **haskell.crash.resume** — sleep steps=12
- [x] **hs.db.order.final** — order 5 rows=[['1', '0']] (DISPATCHED with no progress left)
- [x] **hs.db.workflow_status** — rows=[['e4400070-36d0-4ebb-b7f5-e4bdb17f2c87', 'SUCCESS', 'CheckoutWorkflow'], ['e4400070-36d0-4ebb-b7f5-e4bdb17f2c87-6', 'SUCCESS', 'DispatchOrderWorkflow']]
- [x] **hs.db.checkpoints** — final checkpoint rows=13 (kept)
- [x] **hs.crash.reconnected** — reconnected=[{'ts': 65524.7228, 'wall': '14:25:24', 'app': 'hs', 'phase': 'reconnected', 'reconnected': True}]
- [x] **py.db.order.final** — order 7 rows=[['1', '0']] (DISPATCHED with no progress left)
- [x] **py.db.workflow_status** — rows=[['py-e2e-4', 'SUCCESS', 'checkout_workflow'], ['py-e2e-4-7', 'SUCCESS', 'dispatch_order_workflow']]
- [x] **py.db.checkpoints** — peak during run=9, final=0 (the datasource drops them on completion)
- [x] **py.crash.reconnected** — reconnected=[{'ts': 4.721, 'wall': '14:24:20', 'app': 'py', 'phase': 'reconnected', 'reconnected': True}, {'ts': 4.7685, 'wall': '14:28:04', 'app': 'py', 'phase': 'reconnected', 'reconnected': True}]

## Verdict

The Haskell production tracer mirrors the snooped Python lifecycle at every phase: the same workflow/step/transaction sequence for checkout and dispatch, the same one-transaction checkpoint writes, and the same crash/recovery semantics (recovery scan of the PENDING row, replay of the finished steps, resume of the rest, no duplicate inventory decrement). The two documented differences are storage details, not behavior: the checkpoint table name (`widget_store.transaction_completion` vs `dbos.datasource_outputs`), and the Python datasource dropping a workflow's checkpoint rows on completion while the port keeps them (clearing started workflows is a recorded follow-up).

## Python lifecycle events (snooped)

```json
{"ts": 0.0, "wall": "14:27:48.344", "event": "app.session_start", "pid": 32801}
{"ts": 0.0006, "wall": "14:27:48.344", "event": "app.launch.start"}
{"ts": 0.0365, "wall": "14:27:48.380", "event": "recovery.scan", "count": 0, "workflow_ids": [], "executor_id": "local", "app_version": "0.1.0"}
{"ts": 0.0367, "wall": "14:27:48.381", "event": "recovery.start", "count": 0, "workflow_ids": []}
{"ts": 0.037, "wall": "14:27:48.381", "event": "recovery.complete", "count": 0}
{"ts": 0.0376, "wall": "14:27:48.381", "event": "app.launch.complete"}
{"ts": 11.6792, "wall": "14:28:00.023", "event": "workflow.started_by_client", "workflow_id": "py-e2e-4", "name": "checkout_workflow"}
{"ts": 11.6792, "wall": "14:28:00.023", "event": "workflow.start", "workflow_id": "py-e2e-4", "name": "checkout_workflow", "app_version": null, "recovered": false}
{"ts": 11.6836, "wall": "14:28:00.027", "event": "step.call", "name": "create_order", "args": "()"}
{"ts": 11.6889, "wall": "14:28:00.033", "event": "step.fresh", "workflow_id": "py-e2e-4", "step_id": 1, "name": "create_order"}
{"ts": 11.6925, "wall": "14:28:00.036", "event": "checkpoint.miss", "workflow_id": "py-e2e-4", "step_id": 1}
{"ts": 11.696, "wall": "14:28:00.040", "event": "checkpoint.record", "workflow_id": "py-e2e-4", "step_id": 1, "is_error": false}
{"ts": 11.7172, "wall": "14:28:00.061", "event": "step.return", "name": "create_order", "result": "7", "elapsed_ms": 34}
{"ts": 11.7181, "wall": "14:28:00.062", "event": "step.call", "name": "reserve_inventory", "args": "()"}
{"ts": 11.7214, "wall": "14:28:00.065", "event": "step.fresh", "workflow_id": "py-e2e-4", "step_id": 2, "name": "reserve_inventory"}
{"ts": 11.7227, "wall": "14:28:00.066", "event": "checkpoint.miss", "workflow_id": "py-e2e-4", "step_id": 2}
{"ts": 11.7254, "wall": "14:28:00.069", "event": "checkpoint.record", "workflow_id": "py-e2e-4", "step_id": 2, "is_error": false}
{"ts": 11.739, "wall": "14:28:00.083", "event": "step.return", "name": "reserve_inventory", "result": "True", "elapsed_ms": 21}
{"ts": 11.7484, "wall": "14:28:00.092", "event": "event.set", "key": "payment_id", "value": "'py-e2e-4'"}
{"ts": 11.7485, "wall": "14:28:00.092", "event": "event.recv.wait", "topic": "payment_status"}
{"ts": 11.7517, "wall": "14:28:00.095", "event": "step.fresh", "workflow_id": "py-e2e-4", "step_id": 4, "name": "DBOS.recv"}
{"ts": 11.7552, "wall": "14:28:00.099", "event": "step.fresh", "workflow_id": "py-e2e-4", "step_id": 5, "name": "DBOS.sleep"}
{"ts": 11.7639, "wall": "14:28:00.108", "event": "event.get", "workflow_id": "py-e2e-4", "key": "payment_id", "value": "'py-e2e-4'"}
{"ts": 11.7775, "wall": "14:28:00.121", "event": "event.send", "destination": "py-e2e-4", "topic": "payment_status", "message": "'paid'"}
{"ts": 11.7841, "wall": "14:28:00.128", "event": "event.recv.return", "topic": "payment_status", "value": "'paid'"}
{"ts": 11.7848, "wall": "14:28:00.129", "event": "step.call", "name": "update_order_status", "args": "()"}
{"ts": 11.7873, "wall": "14:28:00.131", "event": "step.fresh", "workflow_id": "py-e2e-4", "step_id": 6, "name": "update_order_status"}
{"ts": 11.7884, "wall": "14:28:00.132", "event": "checkpoint.miss", "workflow_id": "py-e2e-4", "step_id": 6}
{"ts": 11.7899, "wall": "14:28:00.134", "event": "checkpoint.record", "workflow_id": "py-e2e-4", "step_id": 6, "is_error": false}
{"ts": 11.8015, "wall": "14:28:00.145", "event": "step.return", "name": "update_order_status", "result": "None", "elapsed_ms": 17}
{"ts": 11.8037, "wall": "14:28:00.147", "event": "step.fresh", "workflow_id": "py-e2e-4", "step_id": 7, "name": "dispatch_order_workflow"}
{"ts": 11.8119, "wall": "14:28:00.156", "event": "workflow.start", "workflow_id": "py-e2e-4-7", "name": "dispatch_order_workflow", "app_version": null, "recovered": false}
{"ts": 11.8119, "wall": "14:28:00.156", "event": "workflow.started_by_client", "workflow_id": "py-e2e-4-7", "name": "dispatch_order_workflow"}
{"ts": 11.8121, "wall": "14:28:00.156", "event": "sleep.call", "seconds": 1}
{"ts": 11.8154, "wall": "14:28:00.159", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 1, "name": "DBOS.sleep"}
{"ts": 11.8236, "wall": "14:28:00.167", "event": "event.set", "key": "order_id", "value": "'7'"}
{"ts": 11.8344, "wall": "14:28:00.178", "event": "event.get", "workflow_id": "py-e2e-4", "key": "order_id", "value": "'7'"}
{"ts": 11.8351, "wall": "14:28:00.179", "event": "workflow.complete", "workflow_id": "py-e2e-4", "name": "checkout_workflow", "result": "None", "elapsed_ms": 156}
{"ts": 12.8205, "wall": "14:28:01.164", "event": "sleep.return", "seconds": 1}
{"ts": 12.8235, "wall": "14:28:01.167", "event": "step.call", "name": "update_order_progress", "args": "()"}
{"ts": 12.8335, "wall": "14:28:01.177", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 2, "name": "update_order_progress"}
{"ts": 12.837, "wall": "14:28:01.181", "event": "checkpoint.miss", "workflow_id": "py-e2e-4-7", "step_id": 2}
{"ts": 12.8438, "wall": "14:28:01.188", "event": "checkpoint.record", "workflow_id": "py-e2e-4-7", "step_id": 2, "is_error": false}
{"ts": 12.8743, "wall": "14:28:01.218", "event": "step.return", "name": "update_order_progress", "result": "None", "elapsed_ms": 50}
{"ts": 12.8754, "wall": "14:28:01.219", "event": "sleep.call", "seconds": 1}
{"ts": 12.8819, "wall": "14:28:01.226", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 3, "name": "DBOS.sleep"}
{"ts": 13.8854, "wall": "14:28:02.229", "event": "sleep.return", "seconds": 1}
{"ts": 13.8873, "wall": "14:28:02.231", "event": "step.call", "name": "update_order_progress", "args": "()"}
{"ts": 13.8953, "wall": "14:28:02.239", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 4, "name": "update_order_progress"}
{"ts": 13.8991, "wall": "14:28:02.243", "event": "checkpoint.miss", "workflow_id": "py-e2e-4-7", "step_id": 4}
{"ts": 13.9029, "wall": "14:28:02.247", "event": "checkpoint.record", "workflow_id": "py-e2e-4-7", "step_id": 4, "is_error": false}
{"ts": 13.9314, "wall": "14:28:02.275", "event": "step.return", "name": "update_order_progress", "result": "None", "elapsed_ms": 44}
{"ts": 13.9323, "wall": "14:28:02.276", "event": "sleep.call", "seconds": 1}
{"ts": 13.936, "wall": "14:28:02.280", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 5, "name": "DBOS.sleep"}
{"ts": 0.0, "wall": "14:28:04.526", "event": "app.session_start", "pid": 32806}
{"ts": 0.0005, "wall": "14:28:04.526", "event": "app.launch.start"}
{"ts": 0.0358, "wall": "14:28:04.562", "event": "recovery.scan", "count": 1, "workflow_ids": ["py-e2e-4-7"], "executor_id": "local", "app_version": "0.1.0"}
{"ts": 0.036, "wall": "14:28:04.562", "event": "recovery.start", "count": 1, "workflow_ids": ["py-e2e-4-7"]}
{"ts": 0.0369, "wall": "14:28:04.563", "event": "app.launch.complete"}
{"ts": 0.0614, "wall": "14:28:04.587", "event": "recovery.complete", "count": 1}
{"ts": 1.0672, "wall": "14:28:05.593", "event": "workflow.start", "workflow_id": "py-e2e-4-7", "name": "dispatch_order_workflow", "app_version": null, "recovered": false}
{"ts": 1.0688, "wall": "14:28:05.595", "event": "sleep.call", "seconds": 1}
{"ts": 1.0743, "wall": "14:28:05.600", "event": "step.replay", "workflow_id": "py-e2e-4-7", "step_id": 1, "name": "DBOS.sleep"}
{"ts": 1.0749, "wall": "14:28:05.601", "event": "sleep.return", "seconds": 1}
{"ts": 1.0778, "wall": "14:28:05.604", "event": "step.call", "name": "update_order_progress", "args": "()"}
{"ts": 1.0875, "wall": "14:28:05.613", "event": "step.replay", "workflow_id": "py-e2e-4-7", "step_id": 2, "name": "update_order_progress"}
{"ts": 1.0881, "wall": "14:28:05.614", "event": "step.return", "name": "update_order_progress", "result": "None", "elapsed_ms": 10}
{"ts": 1.0886, "wall": "14:28:05.615", "event": "sleep.call", "seconds": 1}
{"ts": 1.0927, "wall": "14:28:05.619", "event": "step.replay", "workflow_id": "py-e2e-4-7", "step_id": 3, "name": "DBOS.sleep"}
{"ts": 1.0932, "wall": "14:28:05.619", "event": "sleep.return", "seconds": 1}
{"ts": 1.0944, "wall": "14:28:05.620", "event": "step.call", "name": "update_order_progress", "args": "()"}
{"ts": 1.1012, "wall": "14:28:05.627", "event": "step.replay", "workflow_id": "py-e2e-4-7", "step_id": 4, "name": "update_order_progress"}
{"ts": 1.1017, "wall": "14:28:05.628", "event": "step.return", "name": "update_order_progress", "result": "None", "elapsed_ms": 7}
{"ts": 1.1025, "wall": "14:28:05.628", "event": "sleep.call", "seconds": 1}
{"ts": 1.1063, "wall": "14:28:05.632", "event": "step.replay", "workflow_id": "py-e2e-4-7", "step_id": 5, "name": "DBOS.sleep"}
{"ts": 1.1067, "wall": "14:28:05.633", "event": "sleep.return", "seconds": 1}
{"ts": 1.1077, "wall": "14:28:05.634", "event": "step.call", "name": "update_order_progress", "args": "()"}
{"ts": 1.1155, "wall": "14:28:05.641", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 6, "name": "update_order_progress"}
{"ts": 1.1215, "wall": "14:28:05.647", "event": "checkpoint.miss", "workflow_id": "py-e2e-4-7", "step_id": 6}
{"ts": 1.1261, "wall": "14:28:05.652", "event": "checkpoint.record", "workflow_id": "py-e2e-4-7", "step_id": 6, "is_error": false}
{"ts": 1.1568, "wall": "14:28:05.683", "event": "step.return", "name": "update_order_progress", "result": "None", "elapsed_ms": 49}
{"ts": 1.1577, "wall": "14:28:05.684", "event": "sleep.call", "seconds": 1}
{"ts": 1.1624, "wall": "14:28:05.688", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 7, "name": "DBOS.sleep"}
{"ts": 2.1677, "wall": "14:28:06.694", "event": "sleep.return", "seconds": 1}
{"ts": 2.1701, "wall": "14:28:06.696", "event": "step.call", "name": "update_order_progress", "args": "()"}
{"ts": 2.1784, "wall": "14:28:06.704", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 8, "name": "update_order_progress"}
{"ts": 2.1842, "wall": "14:28:06.710", "event": "checkpoint.miss", "workflow_id": "py-e2e-4-7", "step_id": 8}
{"ts": 2.188, "wall": "14:28:06.714", "event": "checkpoint.record", "workflow_id": "py-e2e-4-7", "step_id": 8, "is_error": false}
{"ts": 2.2135, "wall": "14:28:06.739", "event": "step.return", "name": "update_order_progress", "result": "None", "elapsed_ms": 43}
{"ts": 2.2147, "wall": "14:28:06.741", "event": "sleep.call", "seconds": 1}
{"ts": 2.2199, "wall": "14:28:06.746", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 9, "name": "DBOS.sleep"}
{"ts": 3.2253, "wall": "14:28:07.751", "event": "sleep.return", "seconds": 1}
{"ts": 3.2278, "wall": "14:28:07.754", "event": "step.call", "name": "update_order_progress", "args": "()"}
{"ts": 3.2348, "wall": "14:28:07.761", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 10, "name": "update_order_progress"}
{"ts": 3.2381, "wall": "14:28:07.764", "event": "checkpoint.miss", "workflow_id": "py-e2e-4-7", "step_id": 10}
{"ts": 3.2426, "wall": "14:28:07.769", "event": "checkpoint.record", "workflow_id": "py-e2e-4-7", "step_id": 10, "is_error": false}
{"ts": 3.2688, "wall": "14:28:07.795", "event": "step.return", "name": "update_order_progress", "result": "None", "elapsed_ms": 41}
{"ts": 3.2696, "wall": "14:28:07.795", "event": "sleep.call", "seconds": 1}
{"ts": 3.2723, "wall": "14:28:07.798", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 11, "name": "DBOS.sleep"}
{"ts": 4.2773, "wall": "14:28:08.803", "event": "sleep.return", "seconds": 1}
{"ts": 4.279, "wall": "14:28:08.805", "event": "step.call", "name": "update_order_progress", "args": "()"}
{"ts": 4.2865, "wall": "14:28:08.812", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 12, "name": "update_order_progress"}
{"ts": 4.2911, "wall": "14:28:08.817", "event": "checkpoint.miss", "workflow_id": "py-e2e-4-7", "step_id": 12}
{"ts": 4.2947, "wall": "14:28:08.821", "event": "checkpoint.record", "workflow_id": "py-e2e-4-7", "step_id": 12, "is_error": false}
{"ts": 4.3258, "wall": "14:28:08.852", "event": "step.return", "name": "update_order_progress", "result": "None", "elapsed_ms": 47}
{"ts": 4.3276, "wall": "14:28:08.854", "event": "sleep.call", "seconds": 1}
{"ts": 4.3325, "wall": "14:28:08.858", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 13, "name": "DBOS.sleep"}
{"ts": 5.3344, "wall": "14:28:09.860", "event": "sleep.return", "seconds": 1}
{"ts": 5.3368, "wall": "14:28:09.863", "event": "step.call", "name": "update_order_progress", "args": "()"}
{"ts": 5.3436, "wall": "14:28:09.869", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 14, "name": "update_order_progress"}
{"ts": 5.3472, "wall": "14:28:09.873", "event": "checkpoint.miss", "workflow_id": "py-e2e-4-7", "step_id": 14}
{"ts": 5.351, "wall": "14:28:09.877", "event": "checkpoint.record", "workflow_id": "py-e2e-4-7", "step_id": 14, "is_error": false}
{"ts": 5.3782, "wall": "14:28:09.904", "event": "step.return", "name": "update_order_progress", "result": "None", "elapsed_ms": 41}
{"ts": 5.3796, "wall": "14:28:09.906", "event": "sleep.call", "seconds": 1}
{"ts": 5.3841, "wall": "14:28:09.910", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 15, "name": "DBOS.sleep"}
{"ts": 6.3865, "wall": "14:28:10.912", "event": "sleep.return", "seconds": 1}
{"ts": 6.3884, "wall": "14:28:10.914", "event": "step.call", "name": "update_order_progress", "args": "()"}
{"ts": 6.3976, "wall": "14:28:10.924", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 16, "name": "update_order_progress"}
{"ts": 6.4008, "wall": "14:28:10.927", "event": "checkpoint.miss", "workflow_id": "py-e2e-4-7", "step_id": 16}
{"ts": 6.4042, "wall": "14:28:10.930", "event": "checkpoint.record", "workflow_id": "py-e2e-4-7", "step_id": 16, "is_error": false}
{"ts": 6.4329, "wall": "14:28:10.959", "event": "step.return", "name": "update_order_progress", "result": "None", "elapsed_ms": 44}
{"ts": 6.4368, "wall": "14:28:10.963", "event": "sleep.call", "seconds": 1}
{"ts": 6.441, "wall": "14:28:10.967", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 17, "name": "DBOS.sleep"}
{"ts": 7.4444, "wall": "14:28:11.970", "event": "sleep.return", "seconds": 1}
{"ts": 7.4465, "wall": "14:28:11.972", "event": "step.call", "name": "update_order_progress", "args": "()"}
{"ts": 7.4539, "wall": "14:28:11.980", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 18, "name": "update_order_progress"}
{"ts": 7.4575, "wall": "14:28:11.983", "event": "checkpoint.miss", "workflow_id": "py-e2e-4-7", "step_id": 18}
{"ts": 7.4618, "wall": "14:28:11.988", "event": "checkpoint.record", "workflow_id": "py-e2e-4-7", "step_id": 18, "is_error": false}
{"ts": 7.4892, "wall": "14:28:12.015", "event": "step.return", "name": "update_order_progress", "result": "None", "elapsed_ms": 43}
{"ts": 7.4931, "wall": "14:28:12.019", "event": "sleep.call", "seconds": 1}
{"ts": 7.4979, "wall": "14:28:12.024", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 19, "name": "DBOS.sleep"}
{"ts": 8.5027, "wall": "14:28:13.029", "event": "sleep.return", "seconds": 1}
{"ts": 8.5047, "wall": "14:28:13.031", "event": "step.call", "name": "update_order_progress", "args": "()"}
{"ts": 8.5112, "wall": "14:28:13.037", "event": "step.fresh", "workflow_id": "py-e2e-4-7", "step_id": 20, "name": "update_order_progress"}
{"ts": 8.5152, "wall": "14:28:13.041", "event": "checkpoint.miss", "workflow_id": "py-e2e-4-7", "step_id": 20}
{"ts": 8.522, "wall": "14:28:13.048", "event": "checkpoint.record", "workflow_id": "py-e2e-4-7", "step_id": 20, "is_error": false}
{"ts": 8.5502, "wall": "14:28:13.076", "event": "step.return", "name": "update_order_progress", "result": "None", "elapsed_ms": 45}
{"ts": 8.5704, "wall": "14:28:13.096", "event": "workflow.complete", "workflow_id": "py-e2e-4-7", "name": "dispatch_order_workflow", "result": "None", "elapsed_ms": 7502}
```

## Haskell tracer events

```
{"kind": "EngineRecovered", "detail": "no workflows to recover"}
{"kind": "EngineLaunched", "detail": "DBOS launched app_name=dbos-hs-widget-store executor_id=local app_version=hs-widget-store-0.1.0"}
{"kind": "TransactionRunning", "detail": "running transaction step create_order (0)", "name": "create_order", "step_id": 0}
{"kind": "TransactionOutputRecorded", "detail": "the transaction committed; its output is recorded step_name=create_order step_id=0", "name": "create_order", "step_id": 0}
{"kind": "TransactionRunning", "detail": "running transaction step reserve_inventory (1)", "name": "reserve_inventory", "step_id": 1}
{"kind": "TransactionOutputRecorded", "detail": "the transaction committed; its output is recorded step_name=reserve_inventory step_id=1", "name": "reserve_inventory", "step_id": 1}
{"kind": "TransactionRunning", "detail": "running transaction step mark_order_paid (5)", "name": "mark_order_paid", "step_id": 5}
{"kind": "TransactionOutputRecorded", "detail": "the transaction committed; its output is recorded step_name=mark_order_paid step_id=5", "name": "mark_order_paid", "step_id": 5}
{"kind": "SleepUntilWake", "detail": "sleeping until the recorded wake time step_id=0 remaining_ms=993", "step_id": 0}
{"kind": "WorkflowCompleted", "detail": "the workflow completed; its output is recorded workflow_id=e4400070-36d0-4ebb-b7f5-e4bdb17f2c87", "workflow_id": "e4400070-36d0-4ebb-b7f5-e4bdb17f2c87"}
{"kind": "TransactionRunning", "detail": "running transaction step update_order_progress (1)", "name": "update_order_progress", "step_id": 1}
{"kind": "TransactionOutputRecorded", "detail": "the transaction committed; its output is recorded step_name=update_order_progress step_id=1", "name": "update_order_progress", "step_id": 1}
{"kind": "SleepUntilWake", "detail": "sleeping until the recorded wake time step_id=2 remaining_ms=994", "step_id": 2}
{"kind": "EngineRecovered", "detail": "re-enqueued workflows a previous run left PENDING workflows=1", "workflows": 1}
{"kind": "EngineLaunched", "detail": "DBOS launched app_name=dbos-hs-widget-store executor_id=local app_version=hs-widget-store-0.1.0"}
{"kind": "SleepUntilWake", "detail": "sleeping until the recorded wake time step_id=0 remaining_ms=0", "step_id": 0}
{"kind": "TransactionRunning", "detail": "running transaction step update_order_progress (1)", "name": "update_order_progress", "step_id": 1}
{"kind": "TransactionReplaying", "detail": "replaying recorded transaction step update_order_progress (1)", "name": "update_order_progress", "step_id": 1}
{"kind": "SleepUntilWake", "detail": "sleeping until the recorded wake time step_id=2 remaining_ms=0", "step_id": 2}
{"kind": "TransactionRunning", "detail": "running transaction step update_order_progress (3)", "name": "update_order_progress", "step_id": 3}
{"kind": "TransactionReplaying", "detail": "replaying recorded transaction step update_order_progress (3)", "name": "update_order_progress", "step_id": 3}
{"kind": "SleepUntilWake", "detail": "sleeping until the recorded wake time step_id=4 remaining_ms=0", "step_id": 4}
{"kind": "TransactionRunning", "detail": "running transaction step update_order_progress (5)", "name": "update_order_progress", "step_id": 5}
{"kind": "TransactionOutputRecorded", "detail": "the transaction committed; its output is recorded step_name=update_order_progress step_id=5", "name": "update_order_progress", "step_id": 5}
{"kind": "SleepUntilWake", "detail": "sleeping until the recorded wake time step_id=6 remaining_ms=997", "step_id": 6}
{"kind": "TransactionRunning", "detail": "running transaction step update_order_progress (7)", "name": "update_order_progress", "step_id": 7}
{"kind": "TransactionOutputRecorded", "detail": "the transaction committed; its output is recorded step_name=update_order_progress step_id=7", "name": "update_order_progress", "step_id": 7}
{"kind": "SleepUntilWake", "detail": "sleeping until the recorded wake time step_id=8 remaining_ms=995", "step_id": 8}
{"kind": "TransactionRunning", "detail": "running transaction step update_order_progress (9)", "name": "update_order_progress", "step_id": 9}
{"kind": "TransactionOutputRecorded", "detail": "the transaction committed; its output is recorded step_name=update_order_progress step_id=9", "name": "update_order_progress", "step_id": 9}
{"kind": "SleepUntilWake", "detail": "sleeping until the recorded wake time step_id=10 remaining_ms=993", "step_id": 10}
{"kind": "TransactionRunning", "detail": "running transaction step update_order_progress (11)", "name": "update_order_progress", "step_id": 11}
{"kind": "TransactionOutputRecorded", "detail": "the transaction committed; its output is recorded step_name=update_order_progress step_id=11", "name": "update_order_progress", "step_id": 11}
{"kind": "SleepUntilWake", "detail": "sleeping until the recorded wake time step_id=12 remaining_ms=993", "step_id": 12}
{"kind": "TransactionRunning", "detail": "running transaction step update_order_progress (13)", "name": "update_order_progress", "step_id": 13}
{"kind": "TransactionOutputRecorded", "detail": "the transaction committed; its output is recorded step_name=update_order_progress step_id=13", "name": "update_order_progress", "step_id": 13}
{"kind": "SleepUntilWake", "detail": "sleeping until the recorded wake time step_id=14 remaining_ms=995", "step_id": 14}
{"kind": "TransactionRunning", "detail": "running transaction step update_order_progress (15)", "name": "update_order_progress", "step_id": 15}
{"kind": "TransactionOutputRecorded", "detail": "the transaction committed; its output is recorded step_name=update_order_progress step_id=15", "name": "update_order_progress", "step_id": 15}
{"kind": "SleepUntilWake", "detail": "sleeping until the recorded wake time step_id=16 remaining_ms=994", "step_id": 16}
{"kind": "TransactionRunning", "detail": "running transaction step update_order_progress (17)", "name": "update_order_progress", "step_id": 17}
{"kind": "TransactionOutputRecorded", "detail": "the transaction committed; its output is recorded step_name=update_order_progress step_id=17", "name": "update_order_progress", "step_id": 17}
{"kind": "SleepUntilWake", "detail": "sleeping until the recorded wake time step_id=18 remaining_ms=991", "step_id": 18}
{"kind": "TransactionRunning", "detail": "running transaction step update_order_progress (19)", "name": "update_order_progress", "step_id": 19}
{"kind": "TransactionOutputRecorded", "detail": "the transaction committed; its output is recorded step_name=update_order_progress step_id=19", "name": "update_order_progress", "step_id": 19}
{"kind": "WorkflowCompleted", "detail": "the workflow completed; its output is recorded workflow_id=e4400070-36d0-4ebb-b7f5-e4bdb17f2c87-6", "workflow_id": "e4400070-36d0-4ebb-b7f5-e4bdb17f2c87-6"}
```
