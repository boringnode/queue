# Worker Logs and Retries Failed Finalizations

## Bug Fix

When `completeJob()`, `failJob()`, or `retryJob()` rejected, for example on a lost connection or a
MySQL deadlock between a completion and another worker's lease renewal, the Worker logged nothing:
the job stayed active and ran again once recovered as stalled. An error thrown by a `failed()` hook
was lost the same way, as was the failure of a job whose own options are invalid, such as a
`timeout` of `1.5`.

The Worker now:

- retries a finalization up to 3 times when the database rolled it back as a deadlock victim
  (MySQL `ER_LOCK_DEADLOCK`, PostgreSQL `40P01` and `40001`);
- logs at the `error` level a finalization that still fails, an error thrown by `failed()`, and a
  job that fails to initialize, with the error in `err` and `jobId`, `queue`, and `workerId`;
- logs at the `warn` level a lease renewal that fails, instead of only with
  `NODE_DEBUG=boringnode:queue`.
