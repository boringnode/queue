# `QueueManager.destroy()` Refuses to Run Under a Running Worker

## Bug Fix

`QueueManager.destroy()` called while a Worker ran in the same process closed the Adapter of the
Worker and resolved. The Worker then logged `Worker cycle failed: Connection is closed` on every
cycle and never ran another job.

`QueueManager.destroy()` now throws the new `E_DESTROY_WHILE_WORKER_RUNNING` error, and changes
nothing, from the moment `worker.start()` is called until the Worker has stopped. Await
`worker.stop()` (or `worker.start()`) before calling it.

## Breaking Changes

Code that calls `QueueManager.destroy()` while a Worker runs in the same process now gets
`E_DESTROY_WHILE_WORKER_RUNNING`. Stop the Worker first.
