# Indexed Redis Schedule Claims

## Performance Improvement

The Redis adapter now maintains a `schedules::due` sorted-set index scored by each schedule's
`next_run_at`. Claiming the next due schedule uses this index instead of scanning every stored
schedule, so polling no longer grows linearly with the total schedule count.

Schedule hashes remain the source of truth. Creating, updating, pausing, resuming, deleting, and
claiming schedules maintain the derived index, while claiming repairs stale entries when the hash
and index disagree.

Schedule hash and index writes are atomic, and index consistency is preserved across concurrent
lifecycle changes. Cron finalization now rejects stale calculations when a schedule is deleted or
reconfigured while its next run is being calculated. Paused schedules retain their next run but stay
out of the due index until resumed. Claiming also discards malformed due scores instead of allowing
one corrupt entry to block later schedules.

## Bug Fix

Schedule hashes moved from `schedules::<id>` to `schedules::data::<id>`. They used to share a level
with the `schedules::index` and `schedules::due` keys, so a schedule with the id `index` or `due`
collided with an index and failed with `WRONGTYPE`. Any schedule id is now safe.

## Upgrade Notes

This change requires an explicit migration for existing Redis schedules. The `Adapter` contract now
includes an idempotent `migrate()` method; built-in adapters without migrations implement it as a
no-op, and custom adapters must implement it as well.

Stop every process running 0.7, then run the migration once, before starting workers or any process
that creates or updates schedules:

```typescript
await QueueManager.init(config)
await QueueManager.use('redis').migrate()
```

The migration moves every schedule to its new key and rebuilds the due index. Existing Redis
schedules are not visible to the new version until it has completed. It scans all schedules and
should remain an explicit deployment step rather than run in the worker polling loop.

The migration cannot be rolled back by downgrading: 0.7 processes do not see migrated schedules. Do
not mix 0.7 and newer processes during the deployment.

If a destination key already holds something other than the schedule being moved, the migration
stops without changing anything and its error names that key. This can happen when a 0.7 cron
schedule was deleted while its next run was being calculated. Remove or rename the key, then run the
migration again.
