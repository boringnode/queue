# `QueueManager.init()` Refuses to Run Under a Running Worker

## Bug Fix

`QueueManager.init()` destroys the Adapters of the previous configuration. Called while a Worker ran
in the same process, for example to dispatch jobs from the Worker process, it destroyed the Adapter
of the Worker: the Worker then failed every cycle (`Connection is closed` with Redis) and never ran
another job.

`QueueManager.init()` now throws the new `E_INIT_WHILE_WORKER_RUNNING` error, and changes nothing,
from the moment `worker.start()` is called until the Worker has stopped. The Worker initializes
QueueManager with its config: dispatch jobs without calling `init()`. Once the Worker has stopped,
`init()` works again.

## Breaking Changes

Code that calls `QueueManager.init()` while a Worker runs in the same process now gets
`E_INIT_WHILE_WORKER_RUNNING` instead of silently breaking the Worker. Remove that call.
