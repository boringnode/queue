# Redis Dedup Keys Cleanup

## Bug Fix

Redis dedup keys are now removed when their Job is deleted: on completion or failure without
retention, when history pruning removes the Job, and when a stalled Job fails permanently. A Job kept
in history still holds its dedup key until it is pruned.

The cleanup scripts ignored the ioredis `keyPrefix`, which the `redis()` factory sets to
`boringnode::queue::` by default. Each dedup key without a `ttl` was left in Redis forever.

Deduplication itself was not affected: a dedup key pointing to a missing Job is treated as free.

## Upgrade Notes

Keys leaked before this fix are not removed automatically. Keys with a `ttl` expire on their own.
Keys without a `ttl` match `<keyPrefix>jobs::<queue>::dedup::*` and can be deleted once the Job they
point to no longer exists.
