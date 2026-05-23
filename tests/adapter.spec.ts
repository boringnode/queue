import Knex from 'knex'
import { test } from '@japa/runner'
import { Redis } from 'ioredis'
import { MemoryAdapter } from './_mocks/memory_adapter.js'
import { redis, RedisAdapter } from '../src/drivers/redis_adapter.js'
import { KnexAdapter } from '../src/drivers/knex_adapter.js'
import { QueueSchemaService } from '../src/services/queue_schema.js'
import { registerDriverTestSuite } from './_utils/register_driver_test_suite.js'
import { withRedisWriteSpy } from './_utils/with_redis_write_spy.js'
import { withKnexQuerySpy } from './_utils/with_knex_query_spy.js'

const KEY_PREFIX = 'boringnode::queue::test::'

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

    const scheduleExists = await connection.exists(`schedules::${id}`)
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

      await adapter.completeJob(first!.id, queue, true)

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

      await adapter.completeJob(first!.id, queue, { count: 1 })

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

      await adapter.completeJob(popped!.id, queue, { count: 1 })

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

      // First job still active + stalled. With maxStalledCount=0 it fails permanently.
      const recovered = await adapter.recoverStalledJobs(queue, 10, 0)
      assert.equal(recovered, 0)

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

    // Create tables via QueueSchemaService
    const schemaService = new QueueSchemaService(connection)
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
  let schemaService: QueueSchemaService
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

    schemaService = new QueueSchemaService(connection)

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

    await knexAdapter.retryJob(first!.id, queue)

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

    const recovered = await knexAdapter.recoverStalledJobs(queue, 1, 1)
    assert.equal(recovered, 1)

    const availableJobs = [await knexAdapter.popFrom(queue), await knexAdapter.popFrom(queue)]
    const availableIds = availableJobs.map((job) => job?.id).sort()

    assert.deepEqual(availableIds, [
      'pg-expired-active-stalled-uuid-1',
      'pg-expired-active-stalled-uuid-2',
    ])
  })
})
