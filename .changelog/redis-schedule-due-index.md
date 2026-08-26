# Indexed Redis Schedule Claims

## Performance Improvement

The Redis adapter now maintains a `schedules::due` sorted-set index scored by each schedule's
`next_run_at`. Claiming the next due schedule uses this index instead of scanning every stored
schedule, so polling no longer grows linearly with the total schedule count.

Schedule hashes remain the source of truth. Creating, updating, pausing, resuming, deleting, and
claiming schedules maintain the derived index, while claiming repairs stale entries when the hash
and index disagree.

## Upgrade Notes

This change requires an explicit migration for existing Redis schedules. The `Adapter` contract now
includes an idempotent `migrate()` method; built-in adapters without migrations implement it as a
no-op, and custom adapters must implement it as well.

Run the migration once during deployment, before starting workers or any process that creates or
updates schedules:

```typescript
await QueueManager.init(config)
await QueueManager.use('redis').migrate()
```

Existing Redis schedules will not fire through the new index until the migration has completed.
The migration scans all schedules and should remain an explicit deployment step rather than run in
the worker polling loop.
