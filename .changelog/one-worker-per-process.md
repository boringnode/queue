# One Worker per Process

## Bug Fix

Starting a second Worker in a process where another one was running re-initialized the
`QueueManager`, which destroyed the Adapters of the first Worker. The first Worker then produced
`error` cycles forever, without saying why.

`worker.start()` now throws the new `E_WORKER_ALREADY_RUNNING` error when another Worker is running
in the process, before touching the `QueueManager`. The first Worker keeps working. A Worker holds
the process from `start()` until its `stop()` completes, after its last job is finalized; then
another Worker can start.

Run one Worker per process. A Worker listens on one Adapter: to process the queues of several
Adapters, start one process per Adapter.

## Breaking Changes

A second `worker.start()` while another Worker runs in the same process now rejects with
`E_WORKER_ALREADY_RUNNING` instead of starting.
