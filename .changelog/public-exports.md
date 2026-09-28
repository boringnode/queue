# Public Exports Cleanup

## New Exports

- `JobDispatcher`, the class `Job.dispatch()` returns, is exported from the package root, like
  `JobBatchDispatcher`.
- `DedupOutcome` and `PushResult` are exported from `@boringnode/queue/types`. Custom adapters need
  `PushResult` for `push()` and `pushOn()`.
- `RedisConfig` and `KnexConfig`, the config types `redis()` and `knex()` accept, are exported from
  their drivers.

## Breaking Changes

### Only adapters are exported under `./drivers`

The `./drivers/*` and `./contracts/*` wildcards also exposed internal modules, such as
`@boringnode/queue/drivers/redis_scripts` and `@boringnode/queue/drivers/redis_job_storage`. The
package now exports these paths only:

- `@boringnode/queue/drivers/fake_adapter`
- `@boringnode/queue/drivers/knex_adapter`
- `@boringnode/queue/drivers/kysely_adapter`
- `@boringnode/queue/drivers/redis_adapter`
- `@boringnode/queue/drivers/sync_adapter`
- `@boringnode/queue/contracts/adapter`

Importing any other path under `./drivers` or `./contracts` now fails with
`ERR_PACKAGE_PATH_NOT_EXPORTED`.

### `errors.E_NO_JOBS_FOUND` is removed

It was never thrown: when job discovery finds no files, `QueueManager` logs a warning instead. Code
that referenced `errors.E_NO_JOBS_FOUND` must drop the reference.
