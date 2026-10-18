# Large Batch Dispatches on SQL Adapters

## Bug Fix

The Knex and Kysely adapters inserted a whole `dispatchMany()` batch in one statement, which failed
past the database limits and inserted nothing:

- SQLite with Knex: above 500 jobs (`too many terms in compound SELECT`);
- SQLite: above 6,553 jobs (`too many SQL variables`);
- PostgreSQL: above 13,107 jobs (65,535 bound parameters).

The error also carried the whole SQL statement, with every payload.

They now insert batches in chunks of 500 jobs within one transaction: a batch is still inserted
entirely or not at all. An adapter created on an application transaction inserts the chunks in that
transaction. When a chunk fails there, the driver error propagates unchanged and the application
rolls the transaction back: PostgreSQL refuses to commit it anyway, while on MySQL and SQLite a
commit would keep the chunks inserted before the failure.
