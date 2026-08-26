import Database from 'better-sqlite3'
import { Kysely, MysqlDialect, PostgresDialect, SqliteDialect } from 'kysely'
import { createPool } from 'mysql2'
import { Pool } from 'pg'
import { test } from '@japa/runner'
import {
  KyselyAdapter,
  KyselyQueueSchemaService,
  type QueueDatabase,
} from '../src/drivers/kysely_adapter.js'
import { registerDriverTestSuite } from './_utils/register_driver_test_suite.js'

test.group('Adapter | Kysely (SQLite)', (group) => {
  let connection: Kysely<QueueDatabase>
  let adapter: KyselyAdapter<QueueDatabase>

  group.each.setup(async () => {
    connection = new Kysely<QueueDatabase>({
      dialect: new SqliteDialect({ database: new Database(':memory:') }),
    })

    const schema = new KyselyQueueSchemaService(connection, { dialect: 'sqlite' })
    await schema.createJobsTable()
    await schema.createSchedulesTable()

    return async () => {
      await adapter?.destroy()
      await connection.destroy()
    }
  })

  registerDriverTestSuite({
    test,
    createAdapter: () => {
      adapter = new KyselyAdapter({ connection, dialect: 'sqlite' })
      return adapter
    },
  })

  test('concurrent workers should not acquire the same job', async ({ assert }) => {
    const firstWorker = new KyselyAdapter({ connection, dialect: 'sqlite' })
    const secondWorker = new KyselyAdapter({ connection, dialect: 'sqlite' })
    firstWorker.setWorkerId('worker-1')
    secondWorker.setWorkerId('worker-2')

    await firstWorker.pushOn('concurrent-pop-queue', {
      id: 'only-job',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const jobs = await Promise.all([
      firstWorker.popFrom('concurrent-pop-queue'),
      secondWorker.popFrom('concurrent-pop-queue'),
    ])

    assert.equal(jobs.filter((job) => job?.id === 'only-job').length, 1)
  })

  test('concurrent deduplicated dispatches should atomically share one slot', async ({
    assert,
  }) => {
    const firstDispatcher = new KyselyAdapter({ connection, dialect: 'sqlite' })
    const secondDispatcher = new KyselyAdapter({ connection, dialect: 'sqlite' })

    const results = await Promise.all([
      firstDispatcher.pushOn('dedup-race-queue', {
        id: 'dedup-race-1',
        name: 'TestJob',
        payload: { version: 1 },
        attempts: 0,
        dedup: { id: 'TestJob::dedup-race' },
      }),
      secondDispatcher.pushOn('dedup-race-queue', {
        id: 'dedup-race-2',
        name: 'TestJob',
        payload: { version: 2 },
        attempts: 0,
        dedup: { id: 'TestJob::dedup-race' },
      }),
    ])

    assert.equal(results.filter((result) => result?.outcome === 'added').length, 1)
    assert.equal(results.filter((result) => result?.outcome === 'skipped').length, 1)
    assert.equal(await firstDispatcher.sizeOf('dedup-race-queue'), 1)
  })
})

test.group('Adapter | Kysely (PostgreSQL)', (group) => {
  const tableName = 'kysely_queue_jobs_test'
  const schedulesTableName = 'kysely_queue_schedules_test'
  let connection: Kysely<QueueDatabase>
  let adapter: KyselyAdapter<QueueDatabase>
  let schema: KyselyQueueSchemaService<QueueDatabase>

  group.each.setup(async () => {
    connection = new Kysely<QueueDatabase>({
      dialect: new PostgresDialect({
        pool: new Pool({
          host: process.env.PG_HOST || 'localhost',
          port: Number.parseInt(process.env.PG_PORT || '5432', 10),
          user: process.env.PG_USER || 'postgres',
          password: process.env.PG_PASSWORD || 'postgres',
          database: process.env.PG_DATABASE || 'queue_test',
        }),
      }),
    })
    schema = new KyselyQueueSchemaService(connection, { dialect: 'postgres' })

    await schema.dropJobsTable(tableName)
    await schema.dropSchedulesTable(schedulesTableName)
    await schema.createJobsTable(tableName)
    await schema.createSchedulesTable(schedulesTableName)

    return async () => {
      await adapter?.destroy()
      await schema.dropJobsTable(tableName)
      await schema.dropSchedulesTable(schedulesTableName)
      await connection.destroy()
    }
  })

  registerDriverTestSuite({
    test,
    createAdapter: () => {
      adapter = new KyselyAdapter({
        connection,
        dialect: 'postgres',
        tableName,
        schedulesTableName,
      })
      return adapter
    },
  })
})

test.group('Adapter | Kysely (MySQL)', (group) => {
  const tableName = 'kysely_mysql_queue_jobs_test'
  const schedulesTableName = 'kysely_mysql_queue_schedules_test'
  let connection: Kysely<QueueDatabase>
  let adapter: KyselyAdapter<QueueDatabase>
  let schema: KyselyQueueSchemaService<QueueDatabase>

  group.each.setup(async () => {
    connection = new Kysely<QueueDatabase>({
      dialect: new MysqlDialect({
        pool: createPool({
          host: process.env.MYSQL_HOST || 'localhost',
          port: Number.parseInt(process.env.MYSQL_PORT || '3307', 10),
          user: process.env.MYSQL_USER || 'root',
          password: process.env.MYSQL_PASSWORD || 'mysql',
          database: process.env.MYSQL_DATABASE || 'queue_test',
        }),
      }),
    })
    schema = new KyselyQueueSchemaService(connection, { dialect: 'mysql' })

    await schema.dropJobsTable(tableName)
    await schema.dropSchedulesTable(schedulesTableName)
    await schema.createJobsTable(tableName)
    await schema.createSchedulesTable(schedulesTableName)

    return async () => {
      await adapter?.destroy()
      await schema.dropJobsTable(tableName)
      await schema.dropSchedulesTable(schedulesTableName)
      await connection.destroy()
    }
  })

  registerDriverTestSuite({
    test,
    supportsAtomicDedup: false,
    createAdapter: () => {
      adapter = new KyselyAdapter({
        connection,
        dialect: 'mysql',
        tableName,
        schedulesTableName,
      })
      return adapter
    },
  })

  test('addDedupColumns should be idempotent', async () => {
    await schema.addDedupColumns(tableName)
    await schema.addDedupColumns(tableName)
  })
})
