# `worker.start()` Resolves After the Worker Stopped

## Bug Fix

`await worker.start()` resolved as soon as the first running job finished after SIGTERM, SIGINT, or
`worker.stop()`, while the stop was still waiting for the other jobs. Code after it, such as
`await QueueManager.destroy()`, closed the Adapter connections under jobs that still had to be
finalized.

`start()` now resolves once the stop has finalized every job, as its documentation says.

Stopping a Worker does not close the Adapter connections, which keep the process alive. The README
now shows calling `await QueueManager.destroy()` after `await worker.start()` so the process exits.
