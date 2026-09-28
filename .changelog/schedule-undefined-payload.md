# Consistent Schedule Payload Without a Value

## Bug Fix

A schedule created or updated without a payload now behaves the same on every adapter: its payload
is stored and returned as `{}`, the value the Redis adapter already returned in 0.7.

Before this fix, the Knex and Kysely adapters rejected such a schedule with a `NOT NULL` constraint
error, and the fake adapter returned `undefined`.

Payloads with a value, including empty arrays, are stored unchanged.
