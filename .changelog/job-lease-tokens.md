# A Stale Execution Can No Longer Finalize a Job

## Bug Fix

`completeJob()`, `failJob()`, and `retryJob()` only checked that a job was active, not who held it.
An execution that outlived `stalledThreshold`, for example a CPU-bound handler blocking the event
loop, could finalize the job after it had been recovered and acquired again:

- a late completion removed the job while the new execution was still running;
- a late retry put it back in the queue, so it ran a third time.

This also happened with a single worker, which recovers its own stalled jobs and acquires them
again.

Each acquisition now gets a lease token. `popFrom()` and `recoverStalledJobs()` return it in
`AcquiredJob.leaseToken`, and the adapter stores it with the active job. `completeJob()`,
`failJob()`, `retryJob()`, and `renewJobs()` change nothing when the token no longer matches: only
the latest execution can finalize the job. The earlier execution still runs to the end, with its
side effects and hooks.

The Knex `retryJob()` also rewrote the job without checking its status between its read and its
update. The update now checks the lease again.

A worker running several executions of the same job, after acquiring it again, now tracks each one:
before, the second execution replaced the first in the worker pool, which could exceed the
concurrency limit.

## Breaking Changes

Custom adapters must implement the new contract:

```typescript
interface AcquiredJob extends JobData {
  acquiredAt: number
  leaseToken: string
}

type JobLease = Pick<AcquiredJob, 'id' | 'leaseToken'>

completeJob(job: JobLease, queue: string, removeOnComplete?: JobRetention): Promise<boolean>
failJob(job: JobLease, queue: string, error?: Error, removeOnFail?: JobRetention): Promise<boolean>
retryJob(job: JobLease, queue: string, retryAt?: Date): Promise<boolean>
renewJobs(queue: string, jobs: JobLease[]): Promise<number>
```

Generate a new token for each acquisition, including the jobs `recoverStalledJobs()` hands back in
`exceeded`. The three finalization methods return `false` when the lease was lost and nothing
changed.

Code that calls these methods directly must pass the acquired job instead of its id.

In the SQL adapters, the `worker_id` column now holds the lease token (`<worker id>:<uuid>`) of an
active job. It still starts with the worker id. No schema change is needed.

Workers of the previous version do not check leases: until they are all stopped, they can still
finalize a job that was acquired again elsewhere.
