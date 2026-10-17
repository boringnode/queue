# SQLite Workers and Dispatchers in Separate Processes

## Bug Fix

On SQLite, the Knex and Kysely adapters acquired jobs, promoted delayed jobs, recovered stalled jobs,
claimed schedules, and dispatched `.dedup()` jobs in transactions that read before they write.
SQLite starts such a transaction with a deferred `BEGIN`: when another process wrote between the
read and the first write, it failed at once with `SQLITE_BUSY`, without waiting for the busy
timeout. A single worker next to a dispatcher process hit it: each failure cost a failed cycle and a
5-second pause. Two processes dispatching `.dedup()` with the same id got a rejection instead of
`skipped`.

These transactions now take the write lock first, as `BEGIN IMMEDIATE` does, so they wait for the
busy timeout. The README documents how to run SQLite with several processes.
