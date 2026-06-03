# Job Deduplication

## New Feature

Jobs can now be deduplicated with the new `.dedup()` dispatcher method.

Use `.dedup({ id })` to skip duplicate jobs while an existing job with the same deduplication key is still present. Deduplication keys are automatically prefixed with the job name, so different job classes can safely reuse the same user-provided ID.

The API also supports TTL-based modes:

- `.dedup({ id, ttl })` skips duplicates within a time window.
- `.dedup({ id, ttl, extend: true })` refreshes the deduplication window when a duplicate is dispatched.
- `.dedup({ id, ttl, replace: true })` replaces the payload of an existing pending or delayed job.
- `.dedup({ id, ttl, extend: true, replace: true })` can be used as a debounce-style dispatch.

Dispatch results now include a `deduped` outcome when deduplication is used: `added`, `skipped`, `replaced`, or `extended`.

## Upgrade Notes

Deduplication is supported by the Redis and Knex adapters. The Sync adapter still runs every dispatch inline and does not apply deduplication.

Knex users must run the queue schema setup after upgrading so the new deduplication columns and indexes are created.

Deduplication only applies to single-job dispatch. Batch dispatch and scheduled jobs do not support deduplication.

The user-provided deduplication ID must be 400 characters or fewer, and the final `<jobName>::<id>` key must be 510 characters or fewer.
