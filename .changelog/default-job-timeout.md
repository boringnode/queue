# Timeouts From `defaultJobOptions` Apply

## Bug Fix

`timeout` and `failOnTimeout` were typed and accepted in `defaultJobOptions`, globally and per
queue, but ignored: only the job's own options and `worker.timeout` were applied. A job expected to
be bounded by these defaults could run forever, and a timed-out job retried even with
`failOnTimeout: true` in the defaults.

They now follow the same precedence as the retention options: the job's options, then the queue's
`defaultJobOptions`, then the global `defaultJobOptions`, then `worker.timeout` for the timeout.

## Breaking Changes

If your config sets `timeout` or `failOnTimeout` in `defaultJobOptions`, they now apply: jobs
without their own `timeout` can time out, and timed-out jobs can fail without retrying. Remove these
settings to keep the previous behavior.

A `timeout` in `defaultJobOptions` now takes precedence over `worker.timeout`.
