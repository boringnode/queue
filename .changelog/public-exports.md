# Public Exports Cleanup

## New Exports

- `JobDispatcher`, the class `Job.dispatch()` returns, is exported from the package root, like
  `JobBatchDispatcher`.
- `DedupOutcome` and `PushResult` are exported from `@boringnode/queue/types`. Custom adapters need
  `PushResult` for `push()` and `pushOn()`.
- `RedisConfig` and `KnexConfig`, the config types `redis()` and `knex()` accept, are exported from
  their drivers.
