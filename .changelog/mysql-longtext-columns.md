# Large Payloads on MySQL

## Bug Fix

On MySQL, the Knex and Kysely schema services created the job payload (`data`), the error message
(`error`), and the schedule payload (`payload`) as `TEXT` columns, which stop at 64 KB:

- in strict mode, the MySQL 8 default, dispatching a job or schedule with a larger payload failed
  with `Data too long for column`, and so did failing a job with a longer error message;
- in non-strict mode, MySQL cut the payload silently. The job could no longer be read, and since it
  stayed first in the queue, every later `popFrom()` failed as well: the queue was blocked.

New tables use `LONGTEXT` on MySQL, like Laravel's queue tables. PostgreSQL and SQLite are not
affected: their `TEXT` type has no such limit.

## Upgrade Notes

Convert existing MySQL tables once with the new `migrateTextColumns()` method of the schema service:

```typescript
await schemaService.migrateTextColumns({
  jobsTable: 'queue_jobs',
  schedulesTable: 'queue_schedules',
})
```

It keeps the nullability, collation, and comment of each column, skips columns already converted and
missing tables, and does nothing on PostgreSQL and SQLite. MySQL rebuilds each table for this change
and blocks writes to it while the copy runs. Tables that are not converted keep working for payloads
under 64 KB.
