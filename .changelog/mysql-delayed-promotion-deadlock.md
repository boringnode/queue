# MySQL Workers No Longer Pause on Delayed Job Promotion Deadlocks

## Bug Fix

On MySQL, `popFrom()` first moves the due delayed jobs of the queue to pending. Two workers doing it
at once could deadlock, and InnoDB aborted one of them with `ER_LOCK_DEADLOCK`: `popFrom()`
rejected, and the worker logged a failed cycle and took no job for 5 seconds.

The Knex and Kysely adapters now treat a deadlock during this promotion as "promote on the next
poll": the promotion is rolled back, the jobs stay delayed, and `popFrom()` goes on to acquire a job.
