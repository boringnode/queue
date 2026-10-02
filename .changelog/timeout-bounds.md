# `timeout: 0` Disables the Timeout, Invalid Timeouts Are Rejected

## Bug Fix

Timeouts outside the range Node timers support no longer fire after 1 ms:

- a timeout longer than 2147483647 ms (about 24.8 days, for example `'30d'`) expired the job after
  1 ms;
- a negative or fractional timeout (such as `1.5`), or one beyond about 49.7 days, made
  `AbortSignal.timeout()` throw `ERR_OUT_OF_RANGE`, which counted as a job failure and was retried.

They now throw `E_INVALID_TIMEOUT`, which explains the valid range. `QueueManager.init()` checks
`worker.timeout` and the `timeout` of every `defaultJobOptions`. A job whose own `timeout` is
invalid fails with this error before it runs, without being retried.

## Breaking Changes

`timeout: 0` now means no timeout, as in Laravel and Node's own timeouts. It used to time the job
out right away. Since `defaultJobOptions` and `worker.timeout` apply to every job, `timeout: 0` on a
job or a queue is the way to opt out of them.

A config with an out-of-range timeout, which used to start, now fails at `QueueManager.init()`.
