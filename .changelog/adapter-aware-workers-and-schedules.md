# Adapter-Aware Workers and Schedules

## New Features

### Select an Adapter per Worker

Workers can now listen on a specific registered Adapter with `worker.adapter`. When omitted, the
worker continues to use the queue manager's default Adapter.

```typescript
const config = {
  default: 'redis',
  adapters: {
    redis: redis(redisConfig),
    database: knex(databaseConfig),
  },
  worker: {
    adapter: 'database',
    concurrency: 5,
  },
}

const worker = new Worker(config)
await worker.start(['default', 'emails'])
```

This makes it possible to run separate workers for queues stored by different Adapters.

### Store and Access Schedules on a Specific Adapter

Schedules can now select their owning Adapter with `.with()`:

```typescript
await CleanupJob.schedule({ days: 30 }).id('daily-cleanup').with('redis').cron('0 0 * * *')
```

`Schedule.find()` and `Schedule.list()` accept an Adapter selector when accessing schedules outside
the default Adapter:

```typescript
const schedule = await Schedule.find('daily-cleanup', { adapter: 'redis' })
const schedules = await Schedule.list({ status: 'active' }, { adapter: 'redis' })
```

A returned `Schedule` retains the selected Adapter for subsequent `pause()`, `resume()`, `delete()`,
and `trigger()` calls. Jobs dispatched by a schedule stay on the Adapter that owns that schedule.

### Identify Jobs Dispatched by a Schedule

Scheduled jobs now include their originating schedule ID in `JobData.scheduleId`. Jobs can access it
while executing through `this.context.scheduleId`:

```typescript
async execute() {
  console.log(this.context.scheduleId)
}
```

The value is `undefined` for jobs that were not dispatched by a schedule.

## Upgrade Notes

Start a Worker for every Adapter that owns schedules. A Worker only claims schedules and jobs from
its configured Adapter.

When a schedule does not call `.with()`, its Adapter is resolved from the job's `adapter` option,
then from the Adapter configured for the job's queue, and finally from the queue manager default.
An explicit `.with()` always takes precedence.
