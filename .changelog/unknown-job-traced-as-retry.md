# A Job With an Unknown Class Is Traced as a Retry

## Bug Fix

When a worker put a job whose class it did not know back in the queue, the execute diagnostics
channel reported the attempt with `status: 'failed'`, and the OpenTelemetry span ended with an
ERROR status, as if the job had failed for good.

Such an attempt is now reported like any retry: `status: 'retrying'` with `nextRetryAt`, and an OK
span with a `messaging.retry` event. The `E_JOB_NOT_FOUND` error is still attached. The attempt that
reaches `worker.unknownJobRetries` and fails the job is still reported as `failed`.
