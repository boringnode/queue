# SQL Schedule Dates Stored as Epoch Milliseconds

## Bug Fix

The Knex and Kysely schedules tables now store their dates (`from_date`, `to_date`, `next_run_at`,
`last_run_at`, `created_at`) as epoch milliseconds in `bigint` columns, like the jobs table.

They used SQL date columns, which stored a local time without a time zone in some setups:

- Kysely on PostgreSQL (`timestamp` columns);
- Kysely and Knex on MySQL, where `mysql2` sends dates in the local time of the process by default.

A process in another time zone than the one that wrote a schedule read its dates shifted by the
difference. For example, a schedule written in Paris for 00:30 UTC was read as 02:30 UTC by a process
in UTC. Schedules could fire hours early or late, and DST changes shifted them on a single host.
Epoch milliseconds do not depend on any time zone. On MySQL, they also lift the `timestamp` limits of
year 2038 and whole seconds.

## Breaking Changes

Existing schedules tables must be migrated. Stop every process running the previous version, run the
new `migrateScheduleDates()` method of the schema service once, then start the new version:

```typescript
// Knex
await new KnexQueueSchemaService(connection).migrateScheduleDates('queue_schedules', {
  timezone: 'Europe/Paris',
})

// Kysely
await new KyselyQueueSchemaService(db, { dialect: 'postgres' }).migrateScheduleDates(
  'queue_schedules',
  { timezone: 'Europe/Paris' }
)
```

`timezone` is the IANA time zone the database driver wrote dates in with the previous version, used
for the dates stored without a time zone (Kysely on PostgreSQL and MySQL, Knex on MySQL). It is the
time zone of the process, unless the driver was configured otherwise: with the mysql2
`timezone: 'Z'` option, pass `'UTC'`. It defaults to the time zone of the process running the
migration.

On MySQL, dates are also converted through the session time zone of the connection. If the previous
version used a session time zone other than the one of the migration connection, pass it as
`databaseTimeZone`, such as `'+02:00'`.

The Kysely schema service also works inside the transaction of Kysely's `Migrator`. Both schema
services find the table the way the adapter's queries do: through `withSchema()` for Kysely and
through the PostgreSQL `search_path`.

The migration is idempotent and keeps custom columns. MySQL cannot change a schema inside a
transaction: if a run fails there, fix the cause and run it again, it resumes where it stopped.

`adapter.migrate()` on the Knex and Kysely adapters now throws an error that points to
`migrateScheduleDates()` while the schedules table is not migrated.

In AdonisJS, run it from a new migration file.

For Kysely users who type their database with `QueueScheduleTable`, the date columns of that
interface are now numeric instead of `Date`.
