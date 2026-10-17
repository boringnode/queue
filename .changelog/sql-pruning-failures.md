# SQL Retention Pruning Failures No Longer Get Lost

## Bug Fix

The Knex and Kysely adapters commit the status of a completed or failed job before they prune the
history of its queue (`count` and `age` of `removeOnComplete` / `removeOnFail`). When the pruning
failed, `completeJob()` or `failJob()` rejected although the job was already finalized. On a
deadlock, the worker retried the whole call, which found the job no longer active: the pruning
never ran, and nothing was logged.

A pruning that loses a deadlock (MySQL `ER_LOCK_DEADLOCK`, PostgreSQL `40P01` and `40001`) now runs
again inside the adapter, up to 3 times. Any other pruning error is logged at the `error` level and
no longer fails the finalization: the next finalization on the queue prunes again.

The adapters log through the QueueManager logger when created with `knex()` or `kysely()`. When
constructed directly, `new KnexAdapter()` and `new KyselyAdapter()` accept a `logger` option and
default to the console.
