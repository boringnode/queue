# A Timed Out Handler Keeps Its Worker Slot

## Bug Fix

A timeout does not stop a handler: it aborts `this.signal` and the worker moves on. Until now, the
worker also freed the slot right away, while a handler that ignored the signal kept running. As a
result:

- the worker ran more handlers than its `concurrency`;
- `worker.stop()` could return while such a handler still ran;
- with the Sync adapter, a retry could start while the timed out attempt still ran.

The job is still failed or retried as soon as the timeout fires. Its handler now keeps its slot
until it returns: the worker does not start another job in its place, and `worker.stop()` waits for
it. The Sync adapter waits for it before running the retry or returning.

## Breaking Changes

`worker.stop()` now waits for timed out handlers to return. A handler that ignores `this.signal`
and never returns blocks it, as a handler without a timeout already did. Make long operations stop
when `this.signal` aborts.

Outside the Sync adapter, a retried job goes back to the queue right away: another worker, or
another slot of the same worker, can run it while the timed out handler has not returned yet.
