import Knex from 'knex'
import { test } from '@japa/runner'
import { Redis } from 'ioredis'
import { MemoryAdapter } from './_mocks/memory_adapter.js'
import { redis, RedisAdapter } from '../src/drivers/redis_adapter.js'
import { KnexAdapter } from '../src/drivers/knex_adapter.js'
import { KnexQueueSchemaService } from '../src/services/knex_queue_schema.js'
import { registerDriverTestSuite } from './_utils/register_driver_test_suite.js'
import { withRedisWriteSpy } from './_utils/with_redis_write_spy.js'
import { withKnexQuerySpy } from './_utils/with_knex_query_spy.js'

const KEY_PREFIX = 'boringnode::queue::test::'

test('redis factory should reuse a connection from another ioredis package copy', async ({
  assert,
}) => {
  class ForeignRedis {
    readonly lazyConnect = true
    readonly enableOfflineQueue = false
    readonly maxRetriesPerRequest = 0
    lastKey?: string

    defineCommand() {}

    async zcard(key: string) {
      this.lastKey = key
      return 42
    }
  }

  const connection = new ForeignRedis()
  const adapter = redis(connection as unknown as Redis)()

  assert.equal(await adapter.size(), 42)
  assert.equal(connection.lastKey, 'jobs::default::pending')
})

test.group('Adapter | Memory', (group) => {
  let adapter: MemoryAdapter

  group.each.teardown(async () => {
    await adapter?.destroy()
  })

  registerDriverTestSuite({
    test,
    createAdapter: () => {
      adapter = new MemoryAdapter()
      return adapter
    },
    supportsConcurrency: false,
  })
})

test.group('Adapter | Redis', (group) => {
  let connection: Redis

  group.each.setup(async () => {
    connection = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
      keyPrefix: KEY_PREFIX,
      db: 15, // Use db 15 for tests so we can safely flush it
    })

    // Flush before test
    await connection.flushdb()

    return async () => {
      await connection.quit()
    }
  })

  registerDriverTestSuite({
    test,
    createAdapter: () => new RedisAdapter(connection),
  })

  test('listSchedules should use bounded network round-trips as schedule count grows', async ({
    assert,
  }) => {
    const adapter = new RedisAdapter(connection)

    for (let i = 0; i < 50; i++) {
      await adapter.upsertSchedule({
        id: `perf-schedule-${i}`,
        name: 'PerfJob',
        payload: { i },
        everyMs: 60_000,
        timezone: 'UTC',
      })
    }

    const { result: schedules, writes } = await withRedisWriteSpy({
      connection,
      run: () => adapter.listSchedules(),
    })

    assert.lengthOf(schedules, 50)
    assert.isAtMost(
      writes,
      4,
      `Expected bounded write count with pipelining, got ${writes} writes for 50 schedules`
    )
  })

  test('claimDueSchedule should use bounded network round-trips when many schedules are not due', async ({
    assert,
  }) => {
    const adapter = new RedisAdapter(connection)
    const futureRunAt = new Date(Date.now() + 60_000)

    for (let i = 0; i < 50; i++) {
      const id = `future-schedule-${i}`

      await adapter.upsertSchedule({
        id,
        name: 'FutureJob',
        payload: { i },
        everyMs: 60_000,
        timezone: 'UTC',
      })
      await adapter.updateSchedule(id, { nextRunAt: futureRunAt })
    }

    const { result: claimed, writes } = await withRedisWriteSpy({
      connection,
      run: () => adapter.claimDueSchedule(),
    })

    assert.isNull(claimed)
    assert.isAtMost(
      writes,
      2,
      `Expected bounded claim writes, got ${writes} writes for 50 future schedules`
    )
  })

  test('deleteSchedule should not leave ghost index under write-failure chaos', async ({
    assert,
  }) => {
    const adapter = new RedisAdapter(connection)
    const id = 'chaos-delete-schedule'

    await adapter.upsertSchedule({
      id,
      name: 'ChaosJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
    })

    const { writes } = await withRedisWriteSpy({
      connection,
      run: () => adapter.deleteSchedule(id),
      onWrite: (writeCount) => {
        if (writeCount === 2) {
          throw new Error('chaos: second network write blocked')
        }
      },
    })

    const scheduleExists = await connection.exists(`schedules::data::${id}`)
    const indexContains = await connection.sismember('schedules::index', id)

    assert.equal(scheduleExists, 0)
    assert.equal(indexContains, 0)
    assert.equal(
      writes,
      1,
      'deleteSchedule should be emitted in a single write window to avoid partial state'
    )
  })

  test('completeJob should not delete a newer TTL dedup lock when Redis keyPrefix is disabled', async ({
    assert,
  }) => {
    const redisOptions = {
      host: process.env.REDIS_HOST || 'localhost',
      port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
      db: 15,
      keyPrefix: '',
    }
    const inspectorConnection = new Redis(redisOptions)
    const adapter = redis(redisOptions)()
    const queue = 'raw-ttl-clean-queue'
    const dedupId = 'TestJob::raw-ttl-clean-1'
    const dedupKey = `jobs::${queue}::dedup::${dedupId}`

    await connection.flushdb()

    try {
      await adapter.pushOn(queue, {
        id: 'raw-ttl-clean-uuid-1',
        name: 'TestJob',
        payload: { n: 1 },
        attempts: 0,
        dedup: { id: dedupId, ttl: 80 },
      })

      const first = await adapter.popFrom(queue)
      assert.equal(first!.id, 'raw-ttl-clean-uuid-1')

      await new Promise((r) => setTimeout(r, 150))

      const second = await adapter.pushOn(queue, {
        id: 'raw-ttl-clean-uuid-2',
        name: 'TestJob',
        payload: { n: 2 },
        attempts: 0,
        dedup: { id: dedupId, ttl: 10_000 },
      })
      assert.equal(second && typeof second === 'object' && second.outcome, 'added')
      assert.equal(await inspectorConnection.get(dedupKey), 'raw-ttl-clean-uuid-2')

      await adapter.completeJob(first!, queue, true)

      assert.equal(await inspectorConnection.get(dedupKey), 'raw-ttl-clean-uuid-2')

      const third = await adapter.pushOn(queue, {
        id: 'raw-ttl-clean-uuid-3',
        name: 'TestJob',
        payload: { n: 3 },
        attempts: 0,
        dedup: { id: dedupId, ttl: 10_000 },
      })

      assert.equal(third && typeof third === 'object' && third.outcome, 'skipped')
      assert.equal(third && typeof third === 'object' && third.jobId, 'raw-ttl-clean-uuid-2')
    } finally {
      await connection.flushdb()
      await adapter.destroy()
      await inspectorConnection.quit()
    }
  })

  test('history pruning should not delete a newer dedup lock when Redis keyPrefix is disabled', async ({
    assert,
  }) => {
    const redisOptions = {
      host: process.env.REDIS_HOST || 'localhost',
      port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
      db: 15,
      keyPrefix: '',
    }
    const inspectorConnection = new Redis(redisOptions)
    const adapter = redis(redisOptions)()
    const queue = 'raw-finalize-prune-queue'
    const dedupId = 'TestJob::raw-finalize-prune-1'
    const dedupKey = `jobs::${queue}::dedup::${dedupId}`

    await connection.flushdb()

    try {
      await adapter.pushOn(queue, {
        id: 'raw-finalize-prune-uuid-1',
        name: 'TestJob',
        payload: { n: 1 },
        attempts: 0,
        dedup: { id: dedupId, ttl: 80 },
      })

      const first = await adapter.popFrom(queue)
      assert.equal(first!.id, 'raw-finalize-prune-uuid-1')

      await adapter.completeJob(first!, queue, { count: 1 })

      await new Promise((r) => setTimeout(r, 150))

      const second = await adapter.pushOn(queue, {
        id: 'raw-finalize-prune-uuid-2',
        name: 'TestJob',
        payload: { n: 2 },
        attempts: 0,
        dedup: { id: dedupId },
      })
      assert.equal(second && typeof second === 'object' && second.outcome, 'added')
      assert.equal(await inspectorConnection.get(dedupKey), 'raw-finalize-prune-uuid-2')

      const popped = await adapter.popFrom(queue)
      assert.equal(popped!.id, 'raw-finalize-prune-uuid-2')

      await adapter.completeJob(popped!, queue, { count: 1 })

      assert.equal(await inspectorConnection.get(dedupKey), 'raw-finalize-prune-uuid-2')

      const third = await adapter.pushOn(queue, {
        id: 'raw-finalize-prune-uuid-3',
        name: 'TestJob',
        payload: { n: 3 },
        attempts: 0,
        dedup: { id: dedupId },
      })

      assert.equal(third && typeof third === 'object' && third.outcome, 'skipped')
      assert.equal(third && typeof third === 'object' && third.jobId, 'raw-finalize-prune-uuid-2')
    } finally {
      await connection.flushdb()
      await adapter.destroy()
      await inspectorConnection.quit()
    }
  })

  test('recoverStalledJobs should not delete a newer dedup lock when Redis keyPrefix is disabled', async ({
    assert,
  }) => {
    const redisOptions = {
      host: process.env.REDIS_HOST || 'localhost',
      port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
      db: 15,
      keyPrefix: '',
    }
    const inspectorConnection = new Redis(redisOptions)
    const adapter = redis(redisOptions)()
    const queue = 'raw-stall-dedup-queue'
    const dedupId = 'TestJob::raw-stall-dedup-1'
    const dedupKey = `jobs::${queue}::dedup::${dedupId}`

    await connection.flushdb()

    try {
      await adapter.pushOn(queue, {
        id: 'raw-stall-dedup-uuid-1',
        name: 'TestJob',
        payload: { n: 1 },
        attempts: 0,
        stalledCount: 0,
        dedup: { id: dedupId, ttl: 80 },
      })

      const first = await adapter.popFrom(queue)
      assert.equal(first!.id, 'raw-stall-dedup-uuid-1')

      await new Promise((r) => setTimeout(r, 150))

      const second = await adapter.pushOn(queue, {
        id: 'raw-stall-dedup-uuid-2',
        name: 'TestJob',
        payload: { n: 2 },
        attempts: 0,
        dedup: { id: dedupId },
      })
      assert.equal(second && typeof second === 'object' && second.outcome, 'added')
      assert.equal(await inspectorConnection.get(dedupKey), 'raw-stall-dedup-uuid-2')

      // First job still active + stalled. With maxStalledCount=0 it is handed back to be failed.
      const { recovered, exceeded } = await adapter.recoverStalledJobs(queue, 10, 0, 100)
      assert.equal(recovered, 0)
      assert.lengthOf(exceeded, 1)
      await adapter.failJob(exceeded[0], queue, new Error('stalled'))

      assert.equal(await inspectorConnection.get(dedupKey), 'raw-stall-dedup-uuid-2')

      const third = await adapter.pushOn(queue, {
        id: 'raw-stall-dedup-uuid-3',
        name: 'TestJob',
        payload: { n: 3 },
        attempts: 0,
        dedup: { id: dedupId },
      })

      assert.equal(third && typeof third === 'object' && third.outcome, 'skipped')
      assert.equal(third && typeof third === 'object' && third.jobId, 'raw-stall-dedup-uuid-2')
    } finally {
      await connection.flushdb()
      await adapter.destroy()
      await inspectorConnection.quit()
    }
  })

  test('dedup keys are removed when the connection uses a key prefix', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)
    const push = (queue: string, id: string) =>
      adapter.pushOn(queue, {
        id,
        name: 'TestJob',
        payload: {},
        attempts: 0,
        dedup: { id },
      })
    const dedupExists = (queue: string, id: string) =>
      connection.exists(`jobs::${queue}::dedup::${id}`)

    await push('prefix-complete', 'completed-job')
    const completed = await adapter.popFrom('prefix-complete')
    await adapter.completeJob(completed!, 'prefix-complete')
    assert.equal(await dedupExists('prefix-complete', 'completed-job'), 0)

    await push('prefix-fail', 'failed-job')
    const failed = await adapter.popFrom('prefix-fail')
    await adapter.failJob(failed!, 'prefix-fail', new Error('boom'))
    assert.equal(await dedupExists('prefix-fail', 'failed-job'), 0)

    await push('prefix-prune', 'pruned-job')
    const pruned = await adapter.popFrom('prefix-prune')
    await adapter.completeJob(pruned!, 'prefix-prune', { count: 1 })
    // History is ordered by completion time; keep both completions in distinct milliseconds.
    await new Promise((resolve) => setTimeout(resolve, 2))
    await push('prefix-prune', 'kept-job')
    const kept = await adapter.popFrom('prefix-prune')
    await adapter.completeJob(kept!, 'prefix-prune', { count: 1 })
    assert.equal(await dedupExists('prefix-prune', 'pruned-job'), 0)
    assert.equal(await dedupExists('prefix-prune', 'kept-job'), 1)

    await push('prefix-stalled', 'stalled-job')
    await adapter.popFrom('prefix-stalled')
    await new Promise((resolve) => setTimeout(resolve, 5))
    const { exceeded } = await adapter.recoverStalledJobs('prefix-stalled', 0, 0, 100)
    await adapter.failJob(exceeded[0], 'prefix-stalled', new Error('stalled'))
    assert.equal(await dedupExists('prefix-stalled', 'stalled-job'), 0)
  })

  test('completeJob does not delete a newer dedup lock when the connection uses a key prefix', async ({
    assert,
  }) => {
    const adapter = new RedisAdapter(connection)
    const queue = 'prefixed-ttl-clean-queue'
    const dedupId = 'TestJob::prefixed-ttl-clean-1'
    const dedupKey = `jobs::${queue}::dedup::${dedupId}`

    await adapter.pushOn(queue, {
      id: 'prefixed-ttl-clean-uuid-1',
      name: 'TestJob',
      payload: { n: 1 },
      attempts: 0,
      dedup: { id: dedupId, ttl: 80 },
    })
    const first = await adapter.popFrom(queue)

    // The first lock expires while its job is still running, so a second job takes the id.
    await new Promise((resolve) => setTimeout(resolve, 150))
    await adapter.pushOn(queue, {
      id: 'prefixed-ttl-clean-uuid-2',
      name: 'TestJob',
      payload: { n: 2 },
      attempts: 0,
      dedup: { id: dedupId, ttl: 10_000 },
    })

    await adapter.completeJob(first!, queue)

    assert.equal(await connection.get(dedupKey), 'prefixed-ttl-clean-uuid-2')
  })

  test('dedup replace should return skipped when stored job_data is malformed JSON', async ({
    assert,
  }) => {
    const adapter = new RedisAdapter(connection)
    const queue = 'malformed-dedup-queue'
    const dataKey = `jobs::${queue}::data`

    await adapter.pushOn(queue, {
      id: 'malformed-uuid-1',
      name: 'TestJob',
      payload: { version: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::malformed-1', ttl: 10_000, replace: true },
    })

    await connection.hset(dataKey, 'malformed-uuid-1', '{not valid json')

    const second = await adapter.pushOn(queue, {
      id: 'malformed-uuid-2',
      name: 'TestJob',
      payload: { version: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::malformed-1', ttl: 10_000, replace: true },
    })
    assert.equal(second && typeof second === 'object' && second.outcome, 'skipped')
    assert.equal(second && typeof second === 'object' && second.jobId, 'malformed-uuid-1')

    const stored = await connection.hget(dataKey, 'malformed-uuid-1')
    assert.equal(stored, '{not valid json')
  })

  test('dedup: orphan dedup pointer is reclaimed when job data is missing', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)
    const queue = 'orphan-dedup-queue'
    const dataKey = `jobs::${queue}::data`
    const dedupKey = `jobs::${queue}::dedup::TestJob::orphan-1`

    await adapter.pushOn(queue, {
      id: 'orphan-uuid-1',
      name: 'TestJob',
      payload: { version: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::orphan-1' },
    })

    // Simulate the pointer outliving the job data (e.g. an external pruner
    // removes the hash entry and pending ZSET entry while the dedup key has
    // not expired yet). The dedup pointer remains pointing at a vanished id.
    const pendingKey = `jobs::${queue}::pending`
    await connection.hdel(dataKey, 'orphan-uuid-1')
    await connection.zrem(pendingKey, 'orphan-uuid-1')

    const before = await connection.get(dedupKey)
    assert.equal(before, 'orphan-uuid-1', 'dedup pointer should still reference the orphaned id')

    // A fresh dispatch should treat the orphan pointer as reclaimable and add
    // a new job, repointing the dedup key to the new winner.
    const second = await adapter.pushOn(queue, {
      id: 'orphan-uuid-2',
      name: 'TestJob',
      payload: { version: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::orphan-1' },
    })

    assert.equal(second && typeof second === 'object' && second.outcome, 'added')
    assert.equal(second && typeof second === 'object' && second.jobId, 'orphan-uuid-2')

    const after = await connection.get(dedupKey)
    assert.equal(after, 'orphan-uuid-2', 'dedup pointer should be reclaimed for the new job')

    const size = await adapter.sizeOf(queue)
    assert.equal(size, 1)
  })

  test('retryJob should keep retrying legacy jobs without Redis metadata', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)
    adapter.setWorkerId('worker-1')
    const queue = 'legacy-retry-empty-array-payload-queue'

    await adapter.pushOn(queue, {
      id: 'legacy-retry-empty-array-uuid-1',
      name: 'TestJob',
      payload: {
        empty: [],
      },
      attempts: 2,
    })

    const first = await adapter.popFrom(queue)
    await adapter.retryJob(first!, queue)

    const retried = await adapter.popFrom(queue)

    assert.deepEqual(retried!.payload, {
      empty: [],
    })
    assert.equal(retried!.attempts, 3)
  })

  test('retained Redis jobs keep metadata-backed payload overrides', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)
    adapter.setWorkerId('worker-1')
    const queue = 'metadata-retained-replace-queue'
    const metadataKey = `jobs::${queue}::metadata`

    await adapter.pushOn(queue, {
      id: 'metadata-retained-uuid-1',
      name: 'TestJob',
      payload: { version: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::metadata-retained-1', ttl: 10_000, replace: true },
    })

    await adapter.pushOn(queue, {
      id: 'metadata-retained-uuid-2',
      name: 'TestJob',
      payload: {
        version: 2,
        empty: [],
      },
      attempts: 0,
      dedup: { id: 'TestJob::metadata-retained-1', ttl: 10_000, replace: true },
    })

    assert.exists(await connection.hget(metadataKey, 'metadata-retained-uuid-1'))

    const job = await adapter.popFrom(queue)
    await adapter.completeJob(job!, queue, false)

    const record = await adapter.getJob('metadata-retained-uuid-1', queue)

    assert.exists(await connection.hget(metadataKey, 'metadata-retained-uuid-1'))
    assert.deepEqual(record!.data.payload, {
      version: 2,
      empty: [],
    })
  })

  test('Redis metadata is removed when a job is completed without retention', async ({
    assert,
  }) => {
    const adapter = new RedisAdapter(connection)
    adapter.setWorkerId('worker-1')
    const queue = 'metadata-clean-complete-queue'
    const metadataKey = `jobs::${queue}::metadata`

    await adapter.pushOn(queue, {
      id: 'metadata-clean-uuid-1',
      name: 'TestJob',
      payload: { version: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::metadata-clean-1', ttl: 10_000, replace: true },
    })

    await adapter.pushOn(queue, {
      id: 'metadata-clean-uuid-2',
      name: 'TestJob',
      payload: { version: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::metadata-clean-1', ttl: 10_000, replace: true },
    })

    assert.exists(await connection.hget(metadataKey, 'metadata-clean-uuid-1'))

    const job = await adapter.popFrom(queue)
    await adapter.completeJob(job!, queue, true)

    assert.isNull(await connection.hget(metadataKey, 'metadata-clean-uuid-1'))
  })

  test('Redis metadata is removed when retained history pruning removes a job', async ({
    assert,
  }) => {
    const adapter = new RedisAdapter(connection)
    adapter.setWorkerId('worker-1')
    const queue = 'metadata-prune-history-queue'
    const metadataKey = `jobs::${queue}::metadata`

    await adapter.pushOn(queue, {
      id: 'metadata-prune-uuid-1',
      name: 'TestJob',
      payload: { version: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::metadata-prune-1', ttl: 10_000, replace: true },
    })

    await adapter.pushOn(queue, {
      id: 'metadata-prune-uuid-2',
      name: 'TestJob',
      payload: { version: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::metadata-prune-1', ttl: 10_000, replace: true },
    })

    assert.exists(await connection.hget(metadataKey, 'metadata-prune-uuid-1'))

    const first = await adapter.popFrom(queue)
    await adapter.completeJob(first!, queue, { count: 1 })

    await new Promise((resolve) => setTimeout(resolve, 5))

    await adapter.pushOn(queue, {
      id: 'metadata-prune-uuid-3',
      name: 'TestJob',
      payload: { version: 3 },
      attempts: 0,
      dedup: { id: 'TestJob::metadata-prune-2', ttl: 10_000, replace: true },
    })

    await adapter.pushOn(queue, {
      id: 'metadata-prune-uuid-4',
      name: 'TestJob',
      payload: { version: 4 },
      attempts: 0,
      dedup: { id: 'TestJob::metadata-prune-2', ttl: 10_000, replace: true },
    })

    assert.exists(await connection.hget(metadataKey, 'metadata-prune-uuid-3'))

    const second = await adapter.popFrom(queue)
    await adapter.completeJob(second!, queue, { count: 1 })

    assert.isNull(await connection.hget(metadataKey, 'metadata-prune-uuid-1'))
    assert.exists(await connection.hget(metadataKey, 'metadata-prune-uuid-3'))
  })

  test('Redis metadata is removed when a permanently stalled job is failed', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)
    adapter.setWorkerId('worker-1')
    const queue = 'metadata-stalled-clean-queue'
    const metadataKey = `jobs::${queue}::metadata`

    await adapter.pushOn(queue, {
      id: 'metadata-stalled-uuid-1',
      name: 'TestJob',
      payload: { version: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::metadata-stalled-1', ttl: 10_000, replace: true },
    })

    await adapter.pushOn(queue, {
      id: 'metadata-stalled-uuid-2',
      name: 'TestJob',
      payload: { version: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::metadata-stalled-1', ttl: 10_000, replace: true },
    })

    assert.exists(await connection.hget(metadataKey, 'metadata-stalled-uuid-1'))

    await adapter.popFrom(queue)
    await new Promise((resolve) => setTimeout(resolve, 20))

    const { recovered, exceeded } = await adapter.recoverStalledJobs(queue, 10, 0, 100)
    assert.equal(recovered, 0)
    assert.lengthOf(exceeded, 1)

    await adapter.failJob(exceeded[0], queue, new Error('stalled'))

    assert.isNull(await connection.hget(metadataKey, 'metadata-stalled-uuid-1'))
    assert.isNull(await adapter.getJob('metadata-stalled-uuid-1', queue))
  })

  test('migrate makes pre-existing schedules claimable from the due index', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)

    // Simulate pre-upgrade schedule data: write hash + index directly, skip ZSET
    const id = 'pre-existing-schedule'
    const pastRunAt = (Date.now() - 5_000).toString()
    await connection
      .multi()
      .hset(`schedules::${id}`, {
        id,
        name: 'LegacyJob',
        payload: '{}',
        status: 'active',
        every_ms: '60000',
        timezone: 'UTC',
        next_run_at: pastRunAt,
        last_run_at: '',
        run_count: '0',
        created_at: Date.now().toString(),
      })
      .sadd('schedules::index', id)
      .exec()

    // Without backfill, ZSET has no entry so claim returns null
    const beforeBackfill = await adapter.claimDueSchedule()
    assert.isNull(beforeBackfill)

    await adapter.migrate()

    const score = await connection.zscore('schedules::due', id)
    assert.equal(Number(score), Number(pastRunAt))

    const afterBackfill = await adapter.claimDueSchedule()
    assert.isNotNull(afterBackfill)
    assert.equal(afterBackfill!.id, id)
  })

  test('migrate moves 0.7 schedule hashes, including one whose id is due', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)
    const pastRunAt = (Date.now() - 5_000).toString()
    const legacySchedule = (id: string) => ({
      id,
      name: 'LegacyJob',
      payload: JSON.stringify({ id }),
      status: 'active',
      every_ms: '60000',
      timezone: 'UTC',
      next_run_at: pastRunAt,
      run_count: '0',
      created_at: Date.now().toString(),
    })

    // In 0.7, the hash of a schedule with the id `due` lives at the due index key.
    await connection
      .multi()
      .hset('schedules::due', legacySchedule('due'))
      .hset('schedules::legacy', legacySchedule('legacy'))
      .sadd('schedules::index', 'due', 'legacy')
      .exec()

    await adapter.migrate()

    assert.equal(await connection.exists('schedules::legacy'), 0)
    assert.equal(await connection.type('schedules::due'), 'zset')
    assert.deepEqual((await adapter.getSchedule('due'))!.payload, { id: 'due' })
    assert.deepEqual((await adapter.getSchedule('legacy'))!.payload, { id: 'legacy' })
    assert.sameMembers(
      [(await adapter.claimDueSchedule())!.id, (await adapter.claimDueSchedule())!.id],
      ['due', 'legacy']
    )
  })

  test('migrate keeps the current hash when a legacy copy also exists', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)
    const id = 'migrated-twice'

    await adapter.upsertSchedule({
      id,
      name: 'CurrentJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
    })
    await connection.hset(`schedules::${id}`, { id, name: 'LegacyJob', status: 'active' })

    await adapter.migrate()

    assert.equal(await connection.exists(`schedules::${id}`), 0)
    assert.equal((await adapter.getSchedule(id))!.name, 'CurrentJob')
  })

  test('migrate handles a 0.7 id whose legacy key is the new key of another id', async ({
    assert,
  }) => {
    const adapter = new RedisAdapter(connection)

    // The 0.7 hash of `data::x` is stored at `schedules::data::x`, the new key of `x`.
    await connection
      .multi()
      .hset('schedules::x', { id: 'x', name: 'XJob', status: 'active' })
      .hset('schedules::data::x', { id: 'data::x', name: 'DataXJob', status: 'active' })
      .sadd('schedules::index', 'x', 'data::x')
      .exec()

    await adapter.migrate()

    assert.equal((await adapter.getSchedule('x'))!.name, 'XJob')
    assert.equal((await adapter.getSchedule('data::x'))!.name, 'DataXJob')
  })

  test('migrate aborts without changes when a destination belongs to another key', async ({
    assert,
  }) => {
    const adapter = new RedisAdapter(connection)

    // 0.7 cron finalization recreates `schedules::data::foo` with only `next_run_at` when
    // the schedule `data::foo` is deleted while it is being finalized. That key is not
    // indexed and is the new key of the schedule `foo`.
    await connection
      .multi()
      .hset('schedules::foo', { id: 'foo', name: 'FooJob', status: 'active' })
      .hset('schedules::data::foo', { next_run_at: Date.now().toString() })
      .sadd('schedules::index', 'foo')
      .exec()

    await assert.rejects(() => adapter.migrate(), /Cannot migrate schedule "foo"/)
    assert.equal(await connection.hget('schedules::foo', 'name'), 'FooJob')

    await connection.del('schedules::data::foo')
    await adapter.migrate()

    assert.equal((await adapter.getSchedule('foo'))!.name, 'FooJob')
  })

  test('schedules named after index keys do not collide with them', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)
    const ids = ['due', 'index', 'regular']
    const dueAt = new Date(Date.now() - 1_000)

    for (const id of ids) {
      await adapter.upsertSchedule({
        id,
        name: 'TestJob',
        payload: { id },
        everyMs: 60_000,
        timezone: 'UTC',
      })
      await adapter.updateSchedule(id, { nextRunAt: dueAt })
    }

    assert.sameMembers(
      (await adapter.listSchedules()).map((schedule) => schedule.id),
      ids
    )

    const claimed: string[] = []
    for (const _ of ids) {
      claimed.push((await adapter.claimDueSchedule())!.id)
    }

    assert.sameMembers(claimed, ids)
    assert.deepEqual((await adapter.getSchedule('index'))!.payload, { id: 'index' })
  })

  test('migrate is idempotent', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)
    const nextRunAt = Date.now() + 30_000

    await adapter.upsertSchedule({
      id: 'idempotent-schedule',
      name: 'TestJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
    })
    await adapter.updateSchedule('idempotent-schedule', {
      nextRunAt: new Date(nextRunAt),
    })

    await connection
      .multi()
      .del('schedules::due')
      .zadd('schedules::due', Date.now() - 10_000, 'orphaned-schedule')
      .exec()

    await adapter.migrate()
    const firstMembers = await connection.zrange('schedules::due', 0, -1, 'WITHSCORES')

    await adapter.migrate()
    const secondMembers = await connection.zrange('schedules::due', 0, -1, 'WITHSCORES')

    assert.deepEqual(firstMembers, ['idempotent-schedule', nextRunAt.toString()])
    assert.deepEqual(secondMembers, firstMembers)
  })

  test('migrate runs in one atomic Redis command', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)
    const id = 'atomic-backfill-schedule'

    await connection
      .multi()
      .hset(`schedules::data::${id}`, {
        id,
        status: 'active',
        next_run_at: (Date.now() + 30_000).toString(),
      })
      .sadd('schedules::index', id)
      .exec()

    const { writes } = await withRedisWriteSpy({
      connection,
      run: () => adapter.migrate(),
    })

    assert.equal(writes, 1)
  })

  test('concurrent migration and schedule writes leave the due index canonical', async ({
    assert,
    cleanup,
  }) => {
    const secondConnection = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
      keyPrefix: KEY_PREFIX,
      db: 15,
    })
    cleanup(async () => {
      await secondConnection.quit()
    })

    const adapter = new RedisAdapter(connection)
    const secondAdapter = new RedisAdapter(secondConnection)

    for (let i = 0; i < 20; i++) {
      const id = `migration-write-schedule-${i}`
      const nextRunAt = Date.now() + 30_000 + i

      await Promise.all([
        adapter.migrate(),
        secondAdapter.upsertSchedule({
          id,
          name: 'MigrationWriteJob',
          payload: {},
          everyMs: 60_000,
          timezone: 'UTC',
        }),
      ])
      await Promise.all([
        adapter.migrate(),
        secondAdapter.updateSchedule(id, { nextRunAt: new Date(nextRunAt) }),
      ])

      assert.equal(Number(await connection.zscore('schedules::due', id)), nextRunAt)
    }
  })

  test('stale ZSET score is self-healed during claim', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)
    const id = 'stale-score-schedule'
    const futureRunAt = Date.now() + 60_000

    await adapter.upsertSchedule({
      id,
      name: 'StaleJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
    })
    await adapter.updateSchedule(id, { nextRunAt: new Date(futureRunAt) })

    // Corrupt the ZSET score to a past value while hash still says future
    await connection.zadd('schedules::due', Date.now() - 10_000, id)

    const claimed = await adapter.claimDueSchedule()
    assert.isNull(claimed, 'should not claim when hash says schedule is not due yet')

    // ZSET score should have been repaired to match the hash
    const repairedScore = await connection.zscore('schedules::due', id)
    assert.equal(Number(repairedScore), futureRunAt)
  })

  test('schedule lifecycle keeps the due index aligned with canonical state', async ({
    assert,
  }) => {
    const adapter = new RedisAdapter(connection)
    const id = 'lifecycle-schedule'
    const firstRunAt = Date.now() + 30_000
    const pausedRunAt = firstRunAt + 30_000

    await adapter.upsertSchedule({
      id,
      name: 'LifecycleJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
    })
    await adapter.updateSchedule(id, { nextRunAt: new Date(firstRunAt) })
    assert.equal(Number(await connection.zscore('schedules::due', id)), firstRunAt)

    await adapter.updateSchedule(id, {
      status: 'paused',
      nextRunAt: new Date(pausedRunAt),
    })
    assert.isNull(await connection.zscore('schedules::due', id))

    await adapter.updateSchedule(id, { nextRunAt: new Date(pausedRunAt + 30_000) })
    assert.isNull(await connection.zscore('schedules::due', id))

    await adapter.updateSchedule(id, { status: 'active' })
    assert.equal(Number(await connection.zscore('schedules::due', id)), pausedRunAt + 30_000)

    await adapter.updateSchedule(id, { nextRunAt: null })
    assert.isNull(await connection.zscore('schedules::due', id))

    await adapter.updateSchedule(id, { nextRunAt: new Date(firstRunAt) })
    await adapter.deleteSchedule(id)
    assert.isNull(await connection.zscore('schedules::due', id))
  })

  test('schedule mutations update canonical state and its index in one command', async ({
    assert,
  }) => {
    const adapter = new RedisAdapter(connection)
    const id = 'atomic-schedule-mutation'

    const { writes: upsertWrites } = await withRedisWriteSpy({
      connection,
      run: () =>
        adapter.upsertSchedule({
          id,
          name: 'AtomicJob',
          payload: {},
          everyMs: 60_000,
          timezone: 'UTC',
        }),
    })
    assert.equal(upsertWrites, 1)

    const { writes: updateWrites } = await withRedisWriteSpy({
      connection,
      run: () => adapter.updateSchedule(id, { nextRunAt: new Date(Date.now() + 30_000) }),
    })
    assert.equal(updateWrites, 1)
  })

  test('concurrent resume and next-run updates leave the due index canonical', async ({
    assert,
    cleanup,
  }) => {
    const secondConnection = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
      keyPrefix: KEY_PREFIX,
      db: 15,
    })
    cleanup(async () => {
      await secondConnection.quit()
    })

    const adapter = new RedisAdapter(connection)
    const secondAdapter = new RedisAdapter(secondConnection)
    const id = 'concurrent-resume-schedule'

    await adapter.upsertSchedule({
      id,
      name: 'ConcurrentJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
    })

    for (let i = 0; i < 20; i++) {
      const nextRunAt = Date.now() + 30_000 + i
      await adapter.updateSchedule(id, { status: 'paused' })

      await Promise.all([
        adapter.updateSchedule(id, { nextRunAt: new Date(nextRunAt) }),
        secondAdapter.updateSchedule(id, { status: 'active' }),
      ])

      const schedule = await adapter.getSchedule(id)
      assert.equal(schedule!.status, 'active')
      assert.equal(Number(await connection.zscore('schedules::due', id)), nextRunAt)
    }
  })

  test('concurrent upsert and next-run updates leave the due index canonical', async ({
    assert,
    cleanup,
  }) => {
    const secondConnection = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
      keyPrefix: KEY_PREFIX,
      db: 15,
    })
    cleanup(async () => {
      await secondConnection.quit()
    })

    const adapter = new RedisAdapter(connection)
    const secondAdapter = new RedisAdapter(secondConnection)
    const id = 'concurrent-upsert-schedule'

    await adapter.upsertSchedule({
      id,
      name: 'ConcurrentJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
    })

    for (let i = 0; i < 20; i++) {
      const nextRunAt = Date.now() + 60_000 + i
      await adapter.updateSchedule(id, { status: 'paused' })

      await Promise.all([
        adapter.updateSchedule(id, { nextRunAt: new Date(nextRunAt) }),
        secondAdapter.upsertSchedule({
          id,
          name: 'ConcurrentJob',
          payload: { iteration: i },
          everyMs: 60_000,
          timezone: 'UTC',
        }),
      ])

      const schedule = await adapter.getSchedule(id)
      assert.equal(schedule!.status, 'active')
      assert.equal(schedule!.nextRunAt!.getTime(), nextRunAt)
      assert.equal(Number(await connection.zscore('schedules::due', id)), nextRunAt)
    }
  })

  test('interval claims update the due index from the canonical hash', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)
    const id = 'interval-index-schedule'

    await adapter.upsertSchedule({
      id,
      name: 'IntervalJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
    })
    await adapter.updateSchedule(id, { nextRunAt: new Date(Date.now() - 1_000) })

    assert.equal((await adapter.claimDueSchedule())?.id, id)

    const schedule = await adapter.getSchedule(id)
    const score = await connection.zscore('schedules::due', id)
    assert.isNotNull(schedule!.nextRunAt)
    assert.equal(Number(score), schedule!.nextRunAt!.getTime())
  })

  test('cron claims update the due index from the canonical hash', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)
    const id = 'cron-index-schedule'

    await adapter.upsertSchedule({
      id,
      name: 'CronJob',
      payload: {},
      cronExpression: '* * * * *',
      timezone: 'UTC',
    })
    await adapter.updateSchedule(id, { nextRunAt: new Date(Date.now() - 1_000) })

    assert.equal((await adapter.claimDueSchedule())?.id, id)

    const schedule = await adapter.getSchedule(id)
    const score = await connection.zscore('schedules::due', id)
    assert.isNotNull(schedule!.nextRunAt)
    assert.equal(Number(score), schedule!.nextRunAt!.getTime())
  })

  test('cron finalization does not undo a concurrent pause or delete', async ({
    assert,
    cleanup,
  }) => {
    const secondConnection = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
      keyPrefix: KEY_PREFIX,
      db: 15,
    })
    cleanup(async () => {
      await secondConnection.quit()
    })

    const adapter = new RedisAdapter(connection)
    const secondAdapter = new RedisAdapter(secondConnection)

    for (const mutation of ['pause', 'delete'] as const) {
      const id = `cron-finalize-${mutation}`
      await adapter.upsertSchedule({
        id,
        name: 'CronFinalizeJob',
        payload: {},
        cronExpression: '* * * * *',
        timezone: 'UTC',
      })
      await adapter.updateSchedule(id, { nextRunAt: new Date(Date.now() - 1_000) })

      const originalEval = connection.eval.bind(connection)
      let releaseClaim!: () => void
      let claimReturned!: () => void
      const claimReleased = new Promise<void>((resolve) => {
        releaseClaim = resolve
      })
      const claimHasReturned = new Promise<void>((resolve) => {
        claimReturned = resolve
      })
      let gateNextEval = true

      connection.eval = (async (...args: Parameters<typeof connection.eval>) => {
        const result = await originalEval(...args)
        if (gateNextEval) {
          gateNextEval = false
          claimReturned()
          await claimReleased
        }
        return result
      }) as typeof connection.eval

      const claim = adapter.claimDueSchedule()
      await claimHasReturned

      if (mutation === 'pause') {
        await secondAdapter.updateSchedule(id, { status: 'paused' })
      } else {
        await secondAdapter.deleteSchedule(id)
      }

      releaseClaim()
      await claim
      connection.eval = originalEval

      if (mutation === 'pause') {
        assert.equal((await adapter.getSchedule(id))!.status, 'paused')
      } else {
        assert.isNull(await adapter.getSchedule(id))
      }
      assert.isNull(await connection.zscore('schedules::due', id))

      if (mutation === 'pause') {
        const schedule = await adapter.getSchedule(id)
        assert.isNotNull(schedule!.nextRunAt)

        await secondAdapter.updateSchedule(id, { status: 'active' })
        assert.equal(
          Number(await connection.zscore('schedules::due', id)),
          schedule!.nextRunAt!.getTime()
        )

        const originalNow = Date.now
        Date.now = () => schedule!.nextRunAt!.getTime()
        try {
          assert.equal((await adapter.claimDueSchedule())?.id, id)
        } finally {
          Date.now = originalNow
        }
      }
    }
  })

  test('cron finalization does not apply a calculation from superseded configuration', async ({
    assert,
    cleanup,
  }) => {
    const secondConnection = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
      keyPrefix: KEY_PREFIX,
      db: 15,
    })
    cleanup(async () => {
      await secondConnection.quit()
    })

    const adapter = new RedisAdapter(connection)
    const secondAdapter = new RedisAdapter(secondConnection)
    const id = 'cron-finalize-superseded-config'

    await adapter.upsertSchedule({
      id,
      name: 'OriginalCronJob',
      payload: {},
      cronExpression: '0 9 * * *',
      timezone: 'UTC',
    })
    await adapter.updateSchedule(id, { nextRunAt: new Date(Date.now() - 1_000) })

    const originalEval = connection.eval.bind(connection)
    let releaseClaim!: () => void
    let claimReturned!: () => void
    const claimReleased = new Promise<void>((resolve) => {
      releaseClaim = resolve
    })
    const claimHasReturned = new Promise<void>((resolve) => {
      claimReturned = resolve
    })
    let gateNextEval = true

    connection.eval = (async (...args: Parameters<typeof connection.eval>) => {
      const result = await originalEval(...args)
      if (gateNextEval) {
        gateNextEval = false
        claimReturned()
        await claimReleased
      }
      return result
    }) as typeof connection.eval

    const claim = adapter.claimDueSchedule()
    await claimHasReturned
    await secondAdapter.upsertSchedule({
      id,
      name: 'UpdatedCronJob',
      payload: {},
      cronExpression: '0 9 * * *',
      timezone: 'America/New_York',
    })

    releaseClaim()
    await claim
    connection.eval = originalEval

    const schedule = await adapter.getSchedule(id)
    assert.equal(schedule!.name, 'UpdatedCronJob')
    assert.equal(schedule!.timezone, 'America/New_York')
    assert.isNull(schedule!.nextRunAt)
    assert.isNull(await connection.zscore('schedules::due', id))
  })

  test('cron finalization survives a concurrent runtime metadata update', async ({
    assert,
    cleanup,
  }) => {
    const secondConnection = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
      keyPrefix: KEY_PREFIX,
      db: 15,
    })
    cleanup(async () => {
      await secondConnection.quit()
    })

    const adapter = new RedisAdapter(connection)
    const secondAdapter = new RedisAdapter(secondConnection)
    const id = 'cron-finalize-concurrent-trigger'

    await adapter.upsertSchedule({
      id,
      name: 'CronTriggerJob',
      payload: {},
      cronExpression: '* * * * *',
      timezone: 'UTC',
    })
    await adapter.updateSchedule(id, { nextRunAt: new Date(Date.now() - 1_000) })

    const originalEval = connection.eval.bind(connection)
    let releaseClaim!: () => void
    let claimReturned!: () => void
    const claimReleased = new Promise<void>((resolve) => {
      releaseClaim = resolve
    })
    const claimHasReturned = new Promise<void>((resolve) => {
      claimReturned = resolve
    })
    let gateNextEval = true

    connection.eval = (async (...args: Parameters<typeof connection.eval>) => {
      const result = await originalEval(...args)
      if (gateNextEval) {
        gateNextEval = false
        claimReturned()
        await claimReleased
      }
      return result
    }) as typeof connection.eval

    const claim = adapter.claimDueSchedule()
    await claimHasReturned
    await secondAdapter.updateSchedule(id, {
      runCount: 2,
      lastRunAt: new Date(),
    })

    releaseClaim()
    await claim
    connection.eval = originalEval

    const schedule = await adapter.getSchedule(id)
    assert.equal(schedule!.runCount, 2)
    assert.isNotNull(schedule!.nextRunAt)
    assert.equal(
      Number(await connection.zscore('schedules::due', id)),
      schedule!.nextRunAt!.getTime()
    )
  })

  test('cron finalization does not undo a cleared next run', async ({ assert, cleanup }) => {
    const secondConnection = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
      keyPrefix: KEY_PREFIX,
      db: 15,
    })
    cleanup(async () => {
      await secondConnection.quit()
    })

    const adapter = new RedisAdapter(connection)
    const secondAdapter = new RedisAdapter(secondConnection)
    const id = 'cron-finalize-cleared-next-run'

    await adapter.upsertSchedule({
      id,
      name: 'CronClearedJob',
      payload: {},
      cronExpression: '* * * * *',
      timezone: 'UTC',
    })
    await adapter.updateSchedule(id, { nextRunAt: new Date(Date.now() - 1_000) })

    const originalEval = connection.eval.bind(connection)
    let releaseClaim!: () => void
    let claimReturned!: () => void
    const claimReleased = new Promise<void>((resolve) => {
      releaseClaim = resolve
    })
    const claimHasReturned = new Promise<void>((resolve) => {
      claimReturned = resolve
    })
    let gateNextEval = true

    connection.eval = (async (...args: Parameters<typeof connection.eval>) => {
      const result = await originalEval(...args)
      if (gateNextEval) {
        gateNextEval = false
        claimReturned()
        await claimReleased
      }
      return result
    }) as typeof connection.eval

    const claim = adapter.claimDueSchedule()
    await claimHasReturned
    await secondAdapter.updateSchedule(id, { nextRunAt: null })

    releaseClaim()
    await claim
    connection.eval = originalEval

    const schedule = await adapter.getSchedule(id)
    assert.isNull(schedule!.nextRunAt)
    assert.isNull(await connection.zscore('schedules::due', id))
  })

  test('stale cron finalization cannot modify a recreated schedule claim', async ({
    assert,
    cleanup,
  }) => {
    const secondConnection = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
      keyPrefix: KEY_PREFIX,
      db: 15,
    })
    cleanup(async () => {
      await secondConnection.quit()
    })

    const adapter = new RedisAdapter(connection)
    const secondAdapter = new RedisAdapter(secondConnection)
    const id = 'cron-finalize-recreated-claim'
    const cronExpression = '0 9 * * *'

    await adapter.upsertSchedule({
      id,
      name: 'OriginalCronJob',
      payload: {},
      cronExpression,
      timezone: 'UTC',
    })
    await adapter.updateSchedule(id, { nextRunAt: new Date(Date.now() - 1_000) })

    const originalEval = connection.eval.bind(connection)
    let releaseFirstClaim!: () => void
    let firstClaimReturned!: () => void
    const firstClaimReleased = new Promise<void>((resolve) => {
      releaseFirstClaim = resolve
    })
    const firstClaimHasReturned = new Promise<void>((resolve) => {
      firstClaimReturned = resolve
    })
    let gateFirstEval = true

    connection.eval = (async (...args: Parameters<typeof connection.eval>) => {
      const result = await originalEval(...args)
      if (gateFirstEval) {
        gateFirstEval = false
        firstClaimReturned()
        await firstClaimReleased
      }
      return result
    }) as typeof connection.eval

    const firstClaim = adapter.claimDueSchedule()
    await firstClaimHasReturned

    await secondAdapter.deleteSchedule(id)
    await secondAdapter.upsertSchedule({
      id,
      name: 'RecreatedCronJob',
      payload: {},
      cronExpression,
      timezone: 'America/New_York',
    })
    await secondAdapter.updateSchedule(id, { nextRunAt: new Date(Date.now() - 1_000) })

    const originalSecondEval = secondConnection.eval.bind(secondConnection)
    let releaseSecondClaim!: () => void
    let secondClaimReturned!: () => void
    const secondClaimReleased = new Promise<void>((resolve) => {
      releaseSecondClaim = resolve
    })
    const secondClaimHasReturned = new Promise<void>((resolve) => {
      secondClaimReturned = resolve
    })
    let gateSecondEval = true

    secondConnection.eval = (async (...args: Parameters<typeof secondConnection.eval>) => {
      const result = await originalSecondEval(...args)
      if (gateSecondEval) {
        gateSecondEval = false
        secondClaimReturned()
        await secondClaimReleased
      }
      return result
    }) as typeof secondConnection.eval

    const secondClaim = secondAdapter.claimDueSchedule()
    await secondClaimHasReturned

    releaseFirstClaim()
    await firstClaim

    const awaitingSecondFinalization = await adapter.getSchedule(id)
    assert.equal(awaitingSecondFinalization!.name, 'RecreatedCronJob')
    assert.equal(awaitingSecondFinalization!.timezone, 'America/New_York')
    assert.isNull(awaitingSecondFinalization!.nextRunAt)
    assert.isNull(await connection.zscore('schedules::due', id))

    releaseSecondClaim()
    await secondClaim
    connection.eval = originalEval
    secondConnection.eval = originalSecondEval

    const finalized = await adapter.getSchedule(id)
    assert.isNotNull(finalized!.nextRunAt)
    assert.equal(
      Number(await connection.zscore('schedules::due', id)),
      finalized!.nextRunAt!.getTime()
    )
  })

  test('updating a deleted schedule does not recreate or index it', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)
    const id = 'update-after-delete'

    await adapter.upsertSchedule({
      id,
      name: 'DeletedScheduleJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
    })
    await adapter.deleteSchedule(id)

    await adapter.updateSchedule(id, {
      status: 'active',
      nextRunAt: new Date(Date.now() - 1_000),
      runCount: 0,
    })

    assert.isNull(await adapter.getSchedule(id))
    assert.isNull(await connection.zscore('schedules::due', id))
    assert.isNull(await adapter.claimDueSchedule())
  })

  test('claim removes a malformed due score and continues to a valid schedule', async ({
    assert,
  }) => {
    const adapter = new RedisAdapter(connection)
    const malformedId = 'malformed-next-run-at'
    const validId = 'valid-after-malformed'

    await connection
      .multi()
      .hset(`schedules::data::${malformedId}`, {
        id: malformedId,
        status: 'active',
        next_run_at: 'not-a-number',
      })
      .sadd('schedules::index', malformedId)
      .zadd('schedules::due', Date.now() - 2_000, malformedId)
      .exec()

    await adapter.upsertSchedule({
      id: validId,
      name: 'ValidJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
    })
    await adapter.updateSchedule(validId, { nextRunAt: new Date(Date.now() - 1_000) })

    assert.equal((await adapter.claimDueSchedule())!.id, validId)
    assert.isNull(await connection.zscore('schedules::due', malformedId))
  })

  test('exhausted schedules are removed from the due index', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)

    for (const config of [
      { id: 'limited-interval', everyMs: 60_000, limit: 1 },
      { id: 'limited-cron', cronExpression: '* * * * *', limit: 1 },
      { id: 'ended-interval', everyMs: 60_000, to: new Date(Date.now() + 1_000) },
    ]) {
      await adapter.upsertSchedule({
        ...config,
        name: 'ExhaustedJob',
        payload: {},
        timezone: 'UTC',
      })
      await adapter.updateSchedule(config.id, { nextRunAt: new Date(Date.now() - 1_000) })

      assert.equal((await adapter.claimDueSchedule())?.id, config.id)
      assert.isNull((await adapter.getSchedule(config.id))!.nextRunAt)
      assert.isNull(await connection.zscore('schedules::due', config.id))
    }
  })

  test('claim removes a due index member whose canonical hash is missing', async ({ assert }) => {
    const adapter = new RedisAdapter(connection)
    await connection.zadd('schedules::due', Date.now() - 1_000, 'missing-schedule')

    assert.isNull(await adapter.claimDueSchedule())
    assert.isNull(await connection.zscore('schedules::due', 'missing-schedule'))
  })
})

test.group('Adapter | Knex (SQLite)', (group) => {
  let connection: ReturnType<typeof Knex>
  let adapter: KnexAdapter

  group.each.setup(async () => {
    // Each test gets a fresh in-memory database, so no cleanup needed
    connection = Knex({
      client: 'better-sqlite3',
      connection: {
        filename: ':memory:',
      },
      useNullAsDefault: true,
    })

    // Create tables via KnexQueueSchemaService
    const schemaService = new KnexQueueSchemaService(connection)
    await schemaService.createJobsTable()
    await schemaService.createSchedulesTable()

    return async () => {
      await adapter?.destroy()
      await connection.destroy()
    }
  })

  registerDriverTestSuite({
    test,
    createAdapter: () => {
      adapter = new KnexAdapter({ connection })
      return adapter
    },
  })

  test('listSchedules should execute a single SQL query in Knex adapter', async ({ assert }) => {
    const knexAdapter = new KnexAdapter({ connection })

    for (let i = 0; i < 20; i++) {
      await knexAdapter.upsertSchedule({
        id: `knex-list-${i}`,
        name: 'KnexPerfJob',
        payload: { i },
        everyMs: 60_000,
        timezone: 'UTC',
      })
    }

    const { result: schedules, queries } = await withKnexQuerySpy({
      connection,
      run: () => knexAdapter.listSchedules(),
    })
    assert.lengthOf(schedules, 20)

    const scheduleSelectQueries = queries.filter(
      (sql) => sql.includes('select') && sql.includes('queue_schedules')
    )

    assert.lengthOf(
      scheduleSelectQueries,
      1,
      `Expected a single schedule SELECT query, got ${scheduleSelectQueries.length}`
    )
  })

  test('deleteSchedule should execute a single SQL query in Knex adapter', async ({ assert }) => {
    const knexAdapter = new KnexAdapter({ connection })
    const id = 'knex-delete-atomicity'

    await knexAdapter.upsertSchedule({
      id,
      name: 'KnexDeleteJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
    })

    const { queries } = await withKnexQuerySpy({
      connection,
      run: () => knexAdapter.deleteSchedule(id),
    })

    const schedule = await knexAdapter.getSchedule(id)
    assert.isNull(schedule)

    const scheduleDeleteQueries = queries.filter(
      (sql) => sql.includes('delete') && sql.includes('queue_schedules')
    )

    assert.lengthOf(
      scheduleDeleteQueries,
      1,
      `Expected a single schedule DELETE query, got ${scheduleDeleteQueries.length}`
    )
  })
})

test.group('Adapter | Knex (PostgreSQL)', (group) => {
  let connection: ReturnType<typeof Knex>
  let adapter: KnexAdapter
  let schemaService: KnexQueueSchemaService
  const tableName = 'queue_jobs_test'
  const schedulesTableName = 'queue_schedules_test'

  group.each.setup(async () => {
    connection = Knex({
      client: 'pg',
      connection: {
        host: process.env.PG_HOST || 'localhost',
        port: Number.parseInt(process.env.PG_PORT || '5432', 10),
        user: process.env.PG_USER || 'postgres',
        password: process.env.PG_PASSWORD || 'postgres',
        database: process.env.PG_DATABASE || 'queue_test',
      },
    })

    schemaService = new KnexQueueSchemaService(connection)

    // Clean up tables before each test
    await schemaService.dropJobsTable(tableName)
    await schemaService.dropSchedulesTable(schedulesTableName)

    // Create tables
    await schemaService.createJobsTable(tableName)
    await schemaService.createSchedulesTable(schedulesTableName)

    return async () => {
      await adapter?.destroy()
      await schemaService.dropJobsTable(tableName)
      await schemaService.dropSchedulesTable(schedulesTableName)
      await connection.destroy()
    }
  })

  registerDriverTestSuite({
    test,
    createAdapter: () => {
      adapter = new KnexAdapter({ connection, tableName, schedulesTableName })
      return adapter
    },
  })

  test('listSchedules should execute a single SQL query in Knex PostgreSQL adapter', async ({
    assert,
  }) => {
    const knexAdapter = new KnexAdapter({ connection, tableName, schedulesTableName })

    for (let i = 0; i < 20; i++) {
      await knexAdapter.upsertSchedule({
        id: `pg-list-${i}`,
        name: 'PgPerfJob',
        payload: { i },
        everyMs: 60_000,
        timezone: 'UTC',
      })
    }

    const { result: schedules, queries } = await withKnexQuerySpy({
      connection,
      run: () => knexAdapter.listSchedules(),
    })
    assert.lengthOf(schedules, 20)

    const scheduleSelectQueries = queries.filter(
      (sql) => sql.includes('select') && sql.includes(schedulesTableName)
    )

    assert.lengthOf(
      scheduleSelectQueries,
      1,
      `Expected a single schedule SELECT query, got ${scheduleSelectQueries.length}`
    )
  })

  test('deleteSchedule should execute a single SQL query in Knex PostgreSQL adapter', async ({
    assert,
  }) => {
    const knexAdapter = new KnexAdapter({ connection, tableName, schedulesTableName })
    const id = 'pg-delete-atomicity'

    await knexAdapter.upsertSchedule({
      id,
      name: 'PgDeleteJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
    })

    const { queries } = await withKnexQuerySpy({
      connection,
      run: () => knexAdapter.deleteSchedule(id),
    })

    const schedule = await knexAdapter.getSchedule(id)
    assert.isNull(schedule)

    const scheduleDeleteQueries = queries.filter(
      (sql) => sql.includes('delete') && sql.includes(schedulesTableName)
    )

    assert.lengthOf(
      scheduleDeleteQueries,
      1,
      `Expected a single schedule DELETE query, got ${scheduleDeleteQueries.length}`
    )
  })

  test('concurrent dedup pushes should not both insert when no existing row is lockable', async ({
    assert,
  }) => {
    const dedupId = 'TestJob::pg-concurrent-missing-row'
    const barrierFunction = 'queue_jobs_test_dedup_insert_barrier'
    const barrierTrigger = 'queue_jobs_test_dedup_insert_barrier_trigger'

    await connection.raw(`
      CREATE OR REPLACE FUNCTION ${barrierFunction}()
      RETURNS trigger AS $$
      DECLARE
        attempts integer := 0;
      BEGIN
        IF NEW.dedup_id = '${dedupId}' THEN
          IF pg_try_advisory_lock(90312001) THEN
            LOOP
              EXIT WHEN NOT pg_try_advisory_lock(90312002);
              PERFORM pg_advisory_unlock(90312002);
              attempts := attempts + 1;
              IF attempts > 1000 THEN
                RAISE EXCEPTION 'timed out waiting for concurrent insert';
              END IF;
              PERFORM pg_sleep(0.01);
            END LOOP;
          ELSE
            PERFORM pg_advisory_lock(90312002);
          END IF;
        END IF;

        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql
    `)

    await connection.raw(`
      CREATE TRIGGER ${barrierTrigger}
      BEFORE INSERT ON ${tableName}
      FOR EACH ROW
      EXECUTE FUNCTION ${barrierFunction}()
    `)

    const createConnection = () =>
      Knex({
        client: 'pg',
        connection: {
          host: process.env.PG_HOST || 'localhost',
          port: Number.parseInt(process.env.PG_PORT || '5432', 10),
          user: process.env.PG_USER || 'postgres',
          password: process.env.PG_PASSWORD || 'postgres',
          database: process.env.PG_DATABASE || 'queue_test',
        },
        pool: { min: 1, max: 1 },
      })

    const connectionA = createConnection()
    const connectionB = createConnection()
    const adapterA = new KnexAdapter({ connection: connectionA, tableName, schedulesTableName })
    const adapterB = new KnexAdapter({ connection: connectionB, tableName, schedulesTableName })

    try {
      const results = await Promise.all([
        adapterA.pushOn('pg-dedup-race-queue', {
          id: 'pg-dedup-race-uuid-1',
          name: 'TestJob',
          payload: { n: 1 },
          attempts: 0,
          dedup: { id: dedupId },
        }),
        adapterB.pushOn('pg-dedup-race-queue', {
          id: 'pg-dedup-race-uuid-2',
          name: 'TestJob',
          payload: { n: 2 },
          attempts: 0,
          dedup: { id: dedupId },
        }),
      ])

      const outcomes = results.map((result) =>
        result && typeof result === 'object' ? result.outcome : undefined
      )
      assert.equal(outcomes.filter((outcome) => outcome === 'added').length, 1)
      assert.equal(outcomes.filter((outcome) => outcome === 'skipped').length, 1)

      const count = await connection(tableName)
        .where('queue', 'pg-dedup-race-queue')
        .where('dedup_id', dedupId)
        .count<{ total: string }[]>('* as total')
        .first()

      assert.equal(Number(count?.total), 1)
    } finally {
      await adapterA.destroy()
      await adapterB.destroy()
      await connectionA.destroy()
      await connectionB.destroy()
      await connection.raw(`DROP TRIGGER IF EXISTS ${barrierTrigger} ON ${tableName}`)
      await connection.raw(`DROP FUNCTION IF EXISTS ${barrierFunction}()`)
    }
  })

  test('retryJob should not violate dedup unique index after active TTL expires', async ({
    assert,
  }) => {
    const knexAdapter = new KnexAdapter({ connection, tableName, schedulesTableName })
    knexAdapter.setWorkerId('worker-1')

    const queue = 'pg-expired-active-retry-dedup-queue'
    const dedupId = 'TestJob::pg-expired-active-retry'

    await knexAdapter.pushOn(queue, {
      id: 'pg-expired-active-retry-uuid-1',
      name: 'TestJob',
      payload: { n: 1 },
      attempts: 0,
      dedup: { id: dedupId, ttl: 30 },
    })

    const first = await knexAdapter.popFrom(queue)
    assert.equal(first!.id, 'pg-expired-active-retry-uuid-1')

    await new Promise((resolve) => setTimeout(resolve, 50))

    const second = await knexAdapter.pushOn(queue, {
      id: 'pg-expired-active-retry-uuid-2',
      name: 'TestJob',
      payload: { n: 2 },
      attempts: 0,
      dedup: { id: dedupId, ttl: 30 },
    })

    assert.equal(second && typeof second === 'object' && second.outcome, 'added')

    await knexAdapter.retryJob(first!, queue)

    const availableJobs = [await knexAdapter.popFrom(queue), await knexAdapter.popFrom(queue)]
    const availableIds = availableJobs.map((job) => job?.id).sort()

    assert.deepEqual(availableIds, [
      'pg-expired-active-retry-uuid-1',
      'pg-expired-active-retry-uuid-2',
    ])
  })

  test('recoverStalledJobs should not violate dedup unique index after active TTL expires', async ({
    assert,
  }) => {
    const knexAdapter = new KnexAdapter({ connection, tableName, schedulesTableName })
    knexAdapter.setWorkerId('worker-1')

    const queue = 'pg-expired-active-stalled-dedup-queue'
    const dedupId = 'TestJob::pg-expired-active-stalled'

    await knexAdapter.pushOn(queue, {
      id: 'pg-expired-active-stalled-uuid-1',
      name: 'TestJob',
      payload: { n: 1 },
      attempts: 0,
      dedup: { id: dedupId, ttl: 30 },
    })

    const first = await knexAdapter.popFrom(queue)
    assert.equal(first!.id, 'pg-expired-active-stalled-uuid-1')

    await new Promise((resolve) => setTimeout(resolve, 50))

    const second = await knexAdapter.pushOn(queue, {
      id: 'pg-expired-active-stalled-uuid-2',
      name: 'TestJob',
      payload: { n: 2 },
      attempts: 0,
      dedup: { id: dedupId, ttl: 30 },
    })

    assert.equal(second && typeof second === 'object' && second.outcome, 'added')

    const { recovered } = await knexAdapter.recoverStalledJobs(queue, 1, 1, 100)
    assert.equal(recovered, 1)

    const availableJobs = [await knexAdapter.popFrom(queue), await knexAdapter.popFrom(queue)]
    const availableIds = availableJobs.map((job) => job?.id).sort()

    assert.deepEqual(availableIds, [
      'pg-expired-active-stalled-uuid-1',
      'pg-expired-active-stalled-uuid-2',
    ])
  })
})
