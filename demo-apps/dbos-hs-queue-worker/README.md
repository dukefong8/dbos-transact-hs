# DBOS Haskell Queue Worker

The Python queue-worker (`~/dev/dbos-demo-apps/python/queue-worker`) rebuilt
as a **single process** on the Haskell queue engine and the IHP web stack:
the handlers enqueue through the executor while the same process's dequeue
loop drains `workflow-queue`, the way the starter's queue tab already works
(the Python demo splits this across `server.py` + `worker.py` sharing one
queue).

The background `workflow` runs 10 steps, publishing a `workflow_progress`
event (`steps_completed` / `num_steps`) after each; the list endpoint reads
each workflow's latest event with a zero-timeout poll, so rows for
not-yet-started workflows simply show no progress.

## Run

```sh
PORT=8090 cabal run exe:demo-apps
# http://localhost:8090/queue-worker/
```

## HTTP surface

`POST /workflows` enqueues one 10-step workflow;
`GET /workflows` lists `{workflow_id, workflow_status, steps_completed,
num_steps}` newest-first. A request carrying `HX-Request` gets the rendered
cards (polled every 1s, progress bars included) instead of JSON.
