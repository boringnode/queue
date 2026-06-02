import Knex from 'knex'
import { test } from '@japa/runner'
import { Redis } from 'ioredis'
import { MemoryAdapter } from './_mocks/memory_adapter.js'
import { RedisAdapter } from '../src/drivers/redis_adapter.js'
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

    // Warm the due-index backfill so it doesn't count against the spy
    await adapter.claimDueSchedule()

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
})
