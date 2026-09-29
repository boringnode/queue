# Worker Logs Failed Cycles

## Bug Fix

When a Worker cycle failed, for example because the database was unreachable, the Worker waited 5
seconds and tried again, but it only reported the error with `NODE_DEBUG=boringnode:queue`. A Worker
could fail every cycle, and process no Job, without writing anything to the logs.

The Worker now logs each failed cycle at the `error` level through the configured logger, with the
error in `err` and the Worker id in `workerId`.
