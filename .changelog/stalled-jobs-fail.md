# Stalled Jobs Fail Instead of Disappearing

## Bug Fix

A job that stalls more than `maxStalledCount` times now fails like any other permanent failure. It
used to be deleted by every adapter: it ended up in no state at all, `failed()` never ran, and the
retention settings were ignored. Two hard crashes in a row on the same job were enough to lose it
without a trace.

The worker that detects the stalled job now:

- calls `failed()` with the new `E_JOB_STALLED` error, with the job payload and context available;
- finalizes the job with its `removeOnFail` retention, so it stays in the failed history when
  retention keeps failed jobs.

With the default retention, failed jobs are removed, so the job still leaves the queue, but only
after `failed()` has run.

The failure runs like a job execution: it takes a worker slot while `failed()` runs, the heartbeat
keeps the job's lease, and `worker.stop()` waits for it. A worker only takes as many of these jobs as
it has free slots; the others stay stalled until a worker has room for them.

If the worker crashes before failing the job, the job stalls again and the next recovery picks it
up. In that rare case, `failed()` can run twice.

## Breaking Changes

Custom adapters must update `recoverStalledJobs()`. It takes a fourth argument, `maxExceeded`, and
returns a `StalledJobsRecovery` object instead of a number:

```typescript
interface StalledJobsRecovery {
  recovered: number
  exceeded: AcquiredJob[]
}
```

Jobs that exceed `maxStalledCount` must no longer be removed. Reassign up to `maxExceeded` of them
atomically to the calling worker (the id set with `setWorkerId()`), with `acquiredAt` set to now, and
return them in `exceeded`. Leave the others untouched. The worker then fails the returned jobs with
`failJob()`.
