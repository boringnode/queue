# @boringnode/queue

<div align="center">

[![typescript-image]][typescript-url]
[![gh-workflow-image]][gh-workflow-url]
[![npm-image]][npm-url]
[![npm-download-image]][npm-download-url]
[![license-image]][license-url]

</div>

A simple and efficient queue system for Node.js applications. Built for simplicity and ease of use, `@boringnode/queue` allows you to dispatch background jobs and process them asynchronously with support for multiple queue adapters.

## Installation

```bash
npm install @boringnode/queue
```

## Features

- **Multiple Queue Adapters**: Redis, Knex (PostgreSQL, MySQL, SQLite), and Sync
- **Type-Safe Jobs**: TypeScript classes with typed payloads
- **Delayed Jobs**: Schedule jobs to run after a delay
- **Priority Queues**: Process high-priority jobs first
- **Bulk Dispatch**: Efficiently dispatch thousands of jobs at once
- **Job Grouping**: Organize related jobs for monitoring
- **Retry with Backoff**: Exponential, linear, or fixed backoff strategies
- **Job Timeout**: Fail or retry jobs that exceed a time limit
- **Job History**: Retain completed/failed jobs for debugging
- **Scheduled Jobs**: Cron or interval-based recurring jobs
- **Auto-Discovery**: Automatically register jobs from specified locations

## Quick Start

### 1. Define a Job

```typescript
import { Job } from '@boringnode/queue'
import type { JobOptions } from '@boringnode/queue/types'

interface SendEmailPayload {
  to: string
}

export default class SendEmailJob extends Job<SendEmailPayload> {
  static options: JobOptions = {
    queue: 'email',
  }

  async execute(): Promise<void> {
    console.log(`Sending email to: ${this.payload.to}`)
  }
}
```

> [!NOTE]
> The job name defaults to the class name (`SendEmailJob`). You can override it with `name: 'CustomName'` in options.

> [!WARNING]
> If you minify your code in production, class names may be mangled. Always specify `name` explicitly in your job options.

### 2. Configure the Queue Manager

```typescript
import { QueueManager } from '@boringnode/queue'
import { redis } from '@boringnode/queue/drivers/redis_adapter'

await QueueManager.init({
  default: 'redis',
  adapters: {
    redis: redis({ host: 'localhost', port: 6379 }),
  },
  locations: ['./app/jobs/**/*.ts'],
})
```

### 3. Dispatch Jobs

```typescript
// Simple dispatch
await SendEmailJob.dispatch({ to: 'user@example.com' })

// With options
await SendEmailJob.dispatch({ to: 'user@example.com' })
  .toQueue('high-priority')
  .priority(1)
  .in('5m')
```

### 4. Start a Worker

```typescript
import { Worker } from '@boringnode/queue'

const worker = new Worker(config)
await worker.start(['default', 'email'])
```

## Bulk Dispatch

Efficiently dispatch thousands of jobs in a single batch operation:

```typescript
const { jobIds } = await SendEmailJob.dispatchMany([
  { to: 'user1@example.com' },
  { to: 'user2@example.com' },
  { to: 'user3@example.com' },
])
  .group('newsletter-jan-2025')
  .toQueue('emails')
  .priority(3)

console.log(`Dispatched ${jobIds.length} jobs`)
```

This uses Redis MULTI/EXEC or SQL batch insert for optimal performance.

## Job Grouping

Organize related jobs together for monitoring and filtering:

```typescript
// Group newsletter jobs
await SendEmailJob.dispatch({ to: 'user@example.com' }).group('newsletter-jan-2025')

// Group with bulk dispatch
await SendEmailJob.dispatchMany(recipients).group('newsletter-jan-2025')
```

The `groupId` is stored with job data and accessible via `job.data.groupId`.

## Job History & Retention

Keep completed and failed jobs for debugging:

```typescript
export default class ImportantJob extends Job<Payload> {
  static options: JobOptions = {
    // Keep last 1000 completed jobs
    removeOnComplete: { count: 1000 },

    // Keep failed jobs for 7 days
    removeOnFail: { age: '7d' },
  }
}
```

<details>
<summary><strong>Retention options</strong></summary>

| Value                       | Behavior           |
| --------------------------- | ------------------ |
| `true` (default)            | Remove immediately |
| `false`                     | Keep forever       |
| `{ count: n }`              | Keep last n jobs   |
| `{ age: '7d' }`             | Keep for duration  |
| `{ count: 100, age: '1d' }` | Both limits apply  |

Query job history:

```typescript
const job = await adapter.getJob('job-id', 'queue-name')
console.log(job.status) // 'completed' | 'failed'
console.log(job.finishedAt) // timestamp
console.log(job.error) // error message (if failed)
```

</details>

## Adapters

### Redis (recommended for production)

```typescript
import { redis } from '@boringnode/queue/drivers/redis_adapter'

// With options
const adapter = redis({ host: 'localhost', port: 6379 })

// With existing ioredis instance
import { Redis } from 'ioredis'
const connection = new Redis({ host: 'localhost' })
const adapter = redis(connection)
```

### Knex (PostgreSQL, MySQL, SQLite)

```typescript
import { knex } from '@boringnode/queue/drivers/knex_adapter'

const adapter = knex({
  client: 'pg',
  connection: { host: 'localhost', database: 'myapp' },
})
```

<details>
<summary><strong>More Knex examples</strong></summary>

```typescript
// With existing Knex instance
import Knex from 'knex'
const connection = Knex({ client: 'pg', connection: '...' })
const adapter = knex(connection)

// Custom table name
const adapter = knex(config, 'custom_jobs_table')
```

</details>

<details>
<summary><strong>Database setup with QueueSchemaService</strong></summary>

The Knex adapter requires tables to be created before use. Use `QueueSchemaService` to create them:

```typescript
import { QueueSchemaService } from '@boringnode/queue'
import Knex from 'knex'

const connection = Knex({ client: 'pg', connection: '...' })
const schemaService = new QueueSchemaService(connection)

// Create tables with default names
await schemaService.createJobsTable()
await schemaService.createSchedulesTable()

// Or extend with custom columns
await schemaService.createJobsTable('queue_jobs', (table) => {
  table.string('tenant_id', 255).nullable()
})
```

**AdonisJS migration example:**

```typescript
import { BaseSchema } from '@adonisjs/lucid/schema'
import { QueueSchemaService } from '@boringnode/queue'

export default class extends BaseSchema {
  async up() {
    const schemaService = new QueueSchemaService(this.db.connection().getWriteClient())
    await schemaService.createJobsTable()
    await schemaService.createSchedulesTable()
  }

  async down() {
    const schemaService = new QueueSchemaService(this.db.connection().getWriteClient())
    await schemaService.dropSchedulesTable()
    await schemaService.dropJobsTable()
  }
}
```

</details>

### Fake (testing + assertions)

```typescript
import { QueueManager } from '@boringnode/queue'
import { redis } from '@boringnode/queue/drivers/redis_adapter'

await QueueManager.init({
  default: 'redis',
  adapters: {
    redis: redis({ host: 'localhost' }),
  },
  locations: ['./app/jobs/**/*.ts'],
})

// The `using` keyword automatically restores the real adapters when
// the variable goes out of scope (at the end of the test function).
using fake = QueueManager.fake()

await SendEmailJob.dispatch({ to: 'user@example.com' })

fake.assertPushed(SendEmailJob)
fake.assertPushed(SendEmailJob, {
  queue: 'default',
  payload: (payload) => payload.to === 'user@example.com',
})
fake.assertPushedCount(1)
```

You can also call `QueueManager.restore()` manually if you need more control over when the real adapters are restored.

### Sync (for testing)

```typescript
import { sync } from '@boringnode/queue/drivers/sync_adapter'

const adapter = sync() // Jobs execute immediately
```

Use the `sync` adapter for tests and lightweight local development only.

- `await MyJob.dispatch(payload).run()` waits for the job to fully finish.
- Retries are executed inline, not by a background worker.
- If you configure backoff, the adapter will `sleep` between attempts.
- This means the caller can stay blocked for the full retry duration.

Example: with `maxRetries: 3` and an exponential backoff of `1s`, `2s`, `4s`,
the request or command that dispatched the job can stay busy for about 7 seconds
before the job exhausts its retries and runs `failed()`.

## Job Options

```typescript
export default class MyJob extends Job<Payload> {
  static options: JobOptions = {
    queue: 'email', // Queue name (default: 'default')
    priority: 1, // Lower = higher priority (default: 5)
    maxRetries: 3, // Retry attempts before failing
    timeout: '30s', // Max execution time
    failOnTimeout: true, // Fail permanently on timeout (default: retry)
    removeOnComplete: { count: 100 }, // Keep last 100 completed
    removeOnFail: { age: '7d' }, // Keep failed for 7 days
  }
}
```

## Delayed Jobs

```typescript
await SendEmailJob.dispatch(payload).in('30s') // 30 seconds
await SendEmailJob.dispatch(payload).in('5m') // 5 minutes
await SendEmailJob.dispatch(payload).in('2h') // 2 hours
await SendEmailJob.dispatch(payload).in('1d') // 1 day
```

## Retry & Backoff

```typescript
import { exponentialBackoff } from '@boringnode/queue'

export default class ReliableJob extends Job<Payload> {
  static options: JobOptions = {
    maxRetries: 5,
    retry: {
      backoff: () =>
        exponentialBackoff({
          baseDelay: '1s',
          maxDelay: '1m',
          multiplier: 2,
          jitter: true,
        }),
    },
  }
}
```

`maxRetries` can be defined directly on the job options, and `retry.backoff`
controls the delay between attempts.

> With the `sync` adapter, these delays happen inline in the caller via
> `sleep`. If a job fails repeatedly, `dispatch().run()` will take as long as
> the total backoff duration. Use a worker-backed adapter when you do not want
> retries to slow down the request/command that dispatched the job.

<details>
<summary><strong>Available strategies</strong></summary>

```typescript
import { exponentialBackoff, linearBackoff, fixedBackoff } from '@boringnode/queue'

// Exponential: 1s, 2s, 4s, 8s...
exponentialBackoff({ baseDelay: '1s', maxDelay: '1m', multiplier: 2 })

// Linear: 1s, 2s, 3s, 4s...
linearBackoff({ baseDelay: '1s', maxDelay: '30s', multiplier: 1 })

// Fixed: 5s, 5s, 5s...
fixedBackoff({ baseDelay: '5s', jitter: true })
```

</details>

## Job Timeout

```typescript
export default class LongRunningJob extends Job<Payload> {
  static options: JobOptions = {
    timeout: '30s',
    failOnTimeout: false, // Will retry (default)
  }

  async execute(): Promise<void> {
    for (const item of this.payload.items) {
      // Check abort signal for graceful timeout handling
      if (this.signal?.aborted) {
        throw new Error('Job timed out')
      }
      await this.processItem(item)
    }
  }
}
```

## Job Context

Access execution metadata via `this.context`:

```typescript
async execute(): Promise<void> {
  console.log(this.context.jobId)       // Unique job ID
  console.log(this.context.attempt)     // 1, 2, 3...
  console.log(this.context.queue)       // Queue name
  console.log(this.context.priority)    // Priority value
  console.log(this.context.acquiredAt)  // When acquired
  console.log(this.context.stalledCount) // Stall recoveries
}
```

## Scheduled Jobs

Run jobs on a recurring basis:

```typescript
// Every 10 seconds
await MetricsJob.schedule({ endpoint: '/health' }).every('10s')

// Cron schedule
await CleanupJob.schedule({ days: 30 })
  .id('daily-cleanup')
  .cron('0 0 * * *') // Midnight daily
  .timezone('Europe/Paris')
```

<details>
<summary><strong>Schedule management</strong></summary>

```typescript
import { Schedule } from '@boringnode/queue'

// Find and manage
const schedule = await Schedule.find('daily-cleanup')
await schedule.pause()
await schedule.resume()
await schedule.trigger() // Run now
await schedule.delete()

// List schedules
const all = await Schedule.list()
const active = await Schedule.list({ status: 'active' })
```

**Schedule options:**

| Method              | Description                       |
| ------------------- | --------------------------------- |
| `.id(string)`       | Unique identifier                 |
| `.every(duration)`  | Fixed interval ('5s', '1m', '1h') |
| `.cron(expression)` | Cron schedule                     |
| `.timezone(tz)`     | Timezone (default: 'UTC')         |
| `.from(date)`       | Start boundary                    |
| `.to(date)`         | End boundary                      |
| `.limit(n)`         | Maximum runs                      |

</details>

## Dependency Injection

Integrate with IoC containers:

```typescript
await QueueManager.init({
  // ...
  jobFactory: async (JobClass) => {
    return app.container.make(JobClass)
  },
})
```

<details>
<summary><strong>Example with injected services</strong></summary>

```typescript
export default class SendEmailJob extends Job<SendEmailPayload> {
  constructor(
    private mailer: MailerService,
    private logger: Logger
  ) {
    super()
  }

  async execute(): Promise<void> {
    this.logger.info(`Sending email to ${this.payload.to}`)
    await this.mailer.send(this.payload)
  }
}
```

</details>

## Worker Configuration

```typescript
const config = {
  worker: {
    concurrency: 5, // Parallel jobs
    idleDelay: '2s', // Poll interval when idle
    timeout: '1m', // Default job timeout
    stalledThreshold: '30s', // When to consider job stalled
    stalledInterval: '30s', // How often to check
    maxStalledCount: 1, // Max recoveries before failing
    gracefulShutdown: true, // Wait for jobs on SIGTERM
  },
}
```

## Logging

```typescript
import { pino } from 'pino'

await QueueManager.init({
  // ...
  logger: pino(),
})
```

## OpenTelemetry Instrumentation (experimental)

> [!WARNING]
> The OpenTelemetry instrumentation is experimental and its API may change in future releases.

`@boringnode/queue` ships with built-in OpenTelemetry instrumentation that creates **PRODUCER** spans for job dispatch and **CONSUMER** spans for job execution, following [OTel messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/).

### Quick Setup

```typescript
import { QueueInstrumentation } from '@boringnode/queue/otel'
import * as boringqueue from '@boringnode/queue'

const instrumentation = new QueueInstrumentation({
  messagingSystem: 'boringqueue', // default
  executionSpanLinkMode: 'link',  // or 'parent'
})

instrumentation.enable()
instrumentation.manuallyRegister(boringqueue)
```

The instrumentation patches `QueueManager.init()` to automatically inject its wrappers — no config changes needed in your queue setup.

### Span Attributes

The instrumentation uses standard [OTel messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/) where they map cleanly, plus a few queue-specific custom attributes.

| Attribute                       | Kind    | Description                                |
| ------------------------------- | ------- | ------------------------------------------ |
| `messaging.system`              | Semconv | `'boringqueue'` (configurable)             |
| `messaging.operation.name`      | Semconv | `'publish'` or `'process'`                 |
| `messaging.destination.name`    | Semconv | Queue name                                 |
| `messaging.message.id`          | Semconv | Job ID for single-message spans            |
| `messaging.batch.message_count` | Semconv | Number of jobs in a batch dispatch         |
| `messaging.message.retry.count` | Custom  | Retry count (0-based) for a job attempt    |
| `messaging.job.name`            | Custom  | Job class name (e.g. `SendEmailJob`)       |
| `messaging.job.status`          | Custom  | `'completed'`, `'failed'`, or `'retrying'` |
| `messaging.job.group_id`        | Custom  | Queue-specific group identifier            |
| `messaging.job.priority`        | Custom  | Queue-specific job priority                |
| `messaging.job.delay_ms`        | Custom  | Delay before the job becomes available     |
| `messaging.job.queue_time_ms`   | Custom  | Time spent waiting in queue before processing |

### Trace Context Propagation

The instrumentation automatically propagates trace context from dispatch to execution:

- **Link mode** (default): Each job execution is an independent trace, linked to the dispatch span
- **Parent mode**: Job execution is a child of the dispatch span (same trace)

Child spans created inside `execute()` (DB queries, HTTP calls, etc.) are automatically parented to the job consumer span.

### diagnostics_channel

Raw telemetry events are available via `diagnostics_channel` for custom subscribers:

```typescript
import { tracingChannels } from '@boringnode/queue'

const { executeChannel } = tracingChannels

executeChannel.subscribe({
  start() {},
  end() {},
  asyncStart() {},
  asyncEnd(message) {
    console.log(`Job ${message.job.name} ${message.status} in ${message.duration}ms`)
  },
  error() {},
})
```

## Benchmarks

Performance comparison with BullMQ (5ms simulated work per job):

| Jobs | Concurrency | @boringnode/queue | BullMQ | Diff        |
| ---- | ----------- | ----------------- | ------ | ----------- |
| 1000 | 5           | 1096ms            | 1116ms | 1.8% faster |
| 1000 | 10          | 565ms             | 579ms  | 2.4% faster |
| 100K | 10          | 56.2s             | 57.5s  | 2.1% faster |
| 100K | 20          | 29.1s             | 29.6s  | 1.7% faster |

```bash
npm run benchmark -- --realistic
```

[gh-workflow-image]: https://img.shields.io/github/actions/workflow/status/boringnode/queue/checks.yml?branch=main&style=for-the-badge
[gh-workflow-url]: https://github.com/boringnode/queue/actions/workflows/checks.yml
[npm-image]: https://img.shields.io/npm/v/@boringnode/queue.svg?style=for-the-badge&logo=npm
[npm-url]: https://www.npmjs.com/package/@boringnode/queue
[npm-download-image]: https://img.shields.io/npm/dm/@boringnode/queue?style=for-the-badge
[npm-download-url]: https://www.npmjs.com/package/@boringnode/queue
[typescript-image]: https://img.shields.io/badge/Typescript-294E80.svg?style=for-the-badge&logo=typescript
[typescript-url]: https://www.typescriptlang.org
[license-image]: https://img.shields.io/npm/l/@boringnode/queue?color=blueviolet&style=for-the-badge
[license-url]: LICENSE.md
