# Jobs With an Unknown Class Go Back to the Queue

## Bug Fix

When a worker took a job whose class it did not know (`E_JOB_NOT_FOUND`), the job failed for good,
without retry or `failed()` hook, and was deleted with the default retention. During a rolling
deploy, an old worker could take a job dispatched by new code, and the job was lost.

The worker now puts such a job back in the queue, to run again 30 seconds later, and logs a
warning. It does so while the job has fewer attempts than `worker.unknownJobRetries` (10 by
default), then the job fails for good, as before. The limit counts all the attempts of the job,
not only these returns: a new job gets up to 10 returns (about 5 minutes), but a job that already
used ordinary retries gets fewer. Each return also counts as an attempt, so a worker that knows the
class later sees a higher `this.context.attempt`, and the job has fewer ordinary retries left.

Only a job name with no registered class goes back to the queue. Other errors while loading or
creating a job still fail it right away, since retrying them changes nothing: a failed hot-reload
import, or a constructor or `jobFactory` that throws, even with `E_JOB_NOT_FOUND` for another
job. A stalled job whose class is unknown also fails for good.

## Breaking Changes

A job with an unknown class is retried instead of failing at once. Set
`worker.unknownJobRetries: 0` to keep the previous behavior. An invalid value (negative or not an
integer) makes `new Worker()` throw `E_CONFIGURATION_ERROR`.
