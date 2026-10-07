import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import Database from 'better-sqlite3'
import { Kysely, MysqlDialect, PostgresDialect, SqliteDialect } from 'kysely'
import { createPool } from 'mysql2'
import { Pool } from 'pg'
import { test } from '@japa/runner'
import {
  KyselyAdapter,
  KyselyQueueSchemaService,
  type QueueDatabase,
  type QueueJobTable,
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

  test('addDedupColumns should leave a dedup slot to its latest job', async ({ assert }) => {
    const jobs = connection.withTables<Record<typeof tableName, QueueJobTable>>()
    const job = (id: string, dedupId: string, dedupAt: number) => ({
      id,
      queue: 'default',
      status: 'completed' as const,
      data: '{}',
      dedup_id: dedupId,
      dedup_at: dedupAt,
      dedup_ttl: 10,
    })

    // A jobs table as the previous versions left it: no unique index, and
    // expired owners that still hold their dedup id.
    await connection.schema.dropIndex(`${tableName}_dedup_uidx`).on(tableName).execute()
    await connection.schema
      .createIndex(`${tableName}_queue_dedup_idx`)
      .on(tableName)
      .columns(['queue', 'dedup_id'])
      .execute()
    await jobs
      .insertInto(tableName)
      .values([job('older', 'shared', 1_000), job('newer', 'shared', 2_000)])
      .execute()

    await schema.addDedupColumns(tableName)
    await schema.addDedupColumns(tableName)

    assert.deepEqual(
      await jobs.selectFrom(tableName).select(['id', 'dedup_id']).orderBy('id').execute(),
      [
        { id: 'newer', dedup_id: 'shared' },
        { id: 'older', dedup_id: null },
      ]
    )
    await assert.rejects(() =>
      jobs
        .insertInto(tableName)
        .values(job('duplicate', 'shared', 3_000))
        .execute()
    )
  })
})

test.group('KyselyAdapter | SQLite file, two connections', () => {
  test('concurrent upserts from two connections all succeed', async ({ assert, cleanup }) => {
    const directory = await mkdtemp(join(tmpdir(), 'queue-sqlite-'))
    const connect = () => {
      const database = new Database(join(directory, 'queue.sqlite'))
      database.pragma('journal_mode = WAL')
      return new Kysely<QueueDatabase>({ dialect: new SqliteDialect({ database }) })
    }
    const first = connect()
    const second = connect()
    cleanup(async () => {
      await first.destroy()
      await second.destroy()
      await rm(directory, { recursive: true, force: true })
    })

    const schedulesTableName = 'queue_schedules'
    await new KyselyQueueSchemaService(first, { dialect: 'sqlite' }).createSchedulesTable(
      schedulesTableName
    )
    const adapters = [first, second].map(
      (connection) => new KyselyAdapter({ connection, dialect: 'sqlite', schedulesTableName })
    )

    for (let round = 0; round < 5; round++) {
      const id = `two-connections-${round}`
      const base = Date.now() + 60_000

      // Workers on two connections define the same schedule, then two timings.
      const results = await Promise.allSettled([
        ...adapters.map((adapter) =>
          adapter.upsertSchedule({
            id,
            name: 'TestJob',
            payload: {},
            everyMs: 60_000,
            timezone: 'UTC',
            nextRunAt: new Date(base + 60_000),
          })
        ),
        ...adapters.map((adapter, index) =>
          adapter.upsertSchedule({
            id,
            name: 'TestJob',
            payload: {},
            everyMs: (index + 2) * 60_000,
            timezone: 'UTC',
            nextRunAt: new Date(base + (index + 2) * 60_000),
          })
        ),
      ])

      assert.deepEqual(
        results.filter((result) => result.status === 'rejected'),
        []
      )
      const schedule = await adapters[0].getSchedule(id)
      assert.equal(schedule!.nextRunAt!.getTime() - base, schedule!.everyMs)
    }
  })
})
