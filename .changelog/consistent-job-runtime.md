# Consistent Job Dispatch and Execution

## Improvements

All job dispatch paths now apply the same routing and job options. This includes `dispatch()`,
`dispatchMany()`, manual schedule triggers, and schedules claimed by workers.

The routing order is now consistent across these paths:

1. Fluent overrides such as `.toQueue()` and `.with()`
2. Static `Job.options`
3. The Adapter configured for the selected queue
4. The queue manager's default Adapter

Queue, Adapter, priority, custom job name, creation timestamp, and schedule provenance are therefore
preserved consistently regardless of how a job is dispatched. Static job options are resolved when
the fluent builder runs, so changes made between builder creation and execution are applied.

The Sync adapter and Worker execution paths now also share the same job lifecycle behavior,
including context construction, dependency injection through `jobFactory`, execution wrappers,
timeouts, retries, failed hooks, and tracing.

## Upgrade Notes

A job routed to a queue with `queues.<name>.adapter` now uses that Adapter when neither `.with()` nor
`Job.options.adapter` selects another one. Previously, some dispatch paths could incorrectly fall
back to the queue manager's default Adapter. Verify that a Worker is running for every Adapter used
by queue configuration.

Explicit fluent options continue to take precedence over static job options.
