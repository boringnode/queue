import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import Database from 'better-sqlite3'
import { Kysely, MysqlDialect, PostgresDialect, SqliteDialect, sql } from 'kysely'
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
import {
  pgDeleteFailureSql,
  registerPruneFailureSuite,
} from './_utils/register_prune_failure_suite.js'
import { startSqliteWriteContender } from './_utils/sqlite_write_contender.js'
import { pauseKyselyDeletes } from './_utils/pause_deletes.js'
import { registerRetentionRaceSuite } from './_utils/register_retention_race_suite.js'

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
    rejectsDuplicateJobIds: true,
    inTransaction: (operation) =>
      connection
        .transaction()
        .execute((trx) =>
          operation(new KyselyAdapter<QueueDatabase>({ connection: trx, dialect: 'sqlite' }))
        ),
    createAdapter: () => {
      adapter = new KyselyAdapter({ connection, dialect: 'sqlite' })
      return adapter
    },
  })

  test('dedup should reject a job id that is already stored', async ({ assert }) => {
    adapter = new KyselyAdapter({ connection, dialect: 'sqlite' })
    const job = { id: 'same-id', name: 'TestJob', payload: {}, attempts: 0 }

    await adapter.pushOn('default', { ...job, dedup: { id: 'first' } })
    // The primary key violation itself, rather than a dispatch that runs again
    const error = await adapter.pushOn('default', { ...job, dedup: { id: 'second' } }).then(
      () => null,
      (rejection: { code?: string }) => rejection
    )
    assert.equal(error?.code, 'SQLITE_CONSTRAINT_PRIMARYKEY')
    assert.equal(await adapter.sizeOf('default'), 1)
  })

  test('dedup should respect a case-insensitive column collation', async ({ assert }) => {
    await sql`DROP INDEX queue_jobs_dedup_uidx`.execute(connection)
    await sql`ALTER TABLE queue_jobs DROP COLUMN dedup_id`.execute(connection)
    await sql`ALTER TABLE queue_jobs ADD COLUMN dedup_id TEXT COLLATE NOCASE`.execute(connection)
    await sql`CREATE UNIQUE INDEX queue_jobs_dedup_uidx ON queue_jobs (queue, dedup_id)`.execute(
      connection
    )
    adapter = new KyselyAdapter({ connection, dialect: 'sqlite' })

    const owner = await adapter.pushOn('default', {
      id: 'owner',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      dedup: { id: 'Order-A' },
    })
    const duplicate = await adapter.pushOn('default', {
      id: 'duplicate',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      dedup: { id: 'order-a' },
    })

    assert.deepEqual(owner, { outcome: 'added', jobId: 'owner' })
    assert.deepEqual(duplicate, { outcome: 'skipped', jobId: 'owner' })
    assert.equal(await adapter.sizeOf('default'), 1)
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
    rejectsDuplicateJobIds: true,
    inTransaction: (operation) =>
      connection.transaction().execute((trx) =>
        operation(
          new KyselyAdapter<QueueDatabase>({
            connection: trx,
            dialect: 'postgres',
            tableName,
            schedulesTableName,
          })
        )
      ),
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

  registerPruneFailureSuite({
    test,
    createAdapter: (logger) => {
      adapter = new KyselyAdapter({
        connection,
        dialect: 'postgres',
        tableName,
        schedulesTableName,
        logger,
      })
      return adapter
    },
    failDeletes: async (sqlstate, count) => {
      for (const statement of pgDeleteFailureSql(tableName, sqlstate, count)) {
        await sql.raw(statement).execute(connection)
      }
    },
    inTransaction: (logger, operation) =>
      connection.transaction().execute((trx) =>
        operation(
          new KyselyAdapter<QueueDatabase>({
            connection: trx,
            dialect: 'postgres',
            tableName,
            schedulesTableName,
            logger,
          })
        )
      ),
  })
})

test.group('Adapter | Kysely (MySQL)', (group) => {
  const tableName = 'kysely_mysql_queue_jobs_test'
  const schedulesTableName = 'kysely_mysql_queue_schedules_test'
  let connection: Kysely<QueueDatabase>
  let adapter: KyselyAdapter<QueueDatabase>
  let schema: KyselyQueueSchemaService<QueueDatabase>

  const createDialect = () =>
    new MysqlDialect({
      pool: createPool({
        host: process.env.MYSQL_HOST || 'localhost',
        port: Number.parseInt(process.env.MYSQL_PORT || '3307', 10),
        user: process.env.MYSQL_USER || 'root',
        password: process.env.MYSQL_PASSWORD || 'mysql',
        database: process.env.MYSQL_DATABASE || 'queue_test',
      }),
    })

  group.each.setup(async () => {
    connection = new Kysely<QueueDatabase>({ dialect: createDialect() })
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
    rejectsDuplicateJobIds: true,
    inTransaction: (operation) =>
      connection.transaction().execute((trx) =>
        operation(
          new KyselyAdapter<QueueDatabase>({
            connection: trx,
            dialect: 'mysql',
            tableName,
            schedulesTableName,
          })
        )
      ),
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

  registerRetentionRaceSuite({
    test,
    manyConsumers: true,
    createAdapters: async (beforeDelete, cleanup) => {
      const first = new Kysely<QueueDatabase>({
        dialect: pauseKyselyDeletes(createDialect(), beforeDelete),
      })
      const second = new Kysely<QueueDatabase>({ dialect: createDialect() })
      cleanup(async () => {
        await first.destroy()
        await second.destroy()
      })
      return [
        new KyselyAdapter({ connection: first, dialect: 'mysql', tableName }),
        new KyselyAdapter({ connection: second, dialect: 'mysql', tableName }),
      ]
    },
  })
})

test.group('KyselyAdapter | SQLite file, two connections', () => {
  const connectTwice = async (
    cleanup: (handler: () => Promise<void>) => void,
    beforeDelete?: () => Promise<void>
  ) => {
    const directory = await mkdtemp(join(tmpdir(), 'queue-sqlite-'))
    const filename = join(directory, 'queue.sqlite')
    const connect = (pause?: () => Promise<void>) => {
      const database = new Database(filename)
      database.pragma('journal_mode = WAL')
      const dialect = new SqliteDialect({ database })
      return new Kysely<QueueDatabase>({
        dialect: pause ? pauseKyselyDeletes(dialect, pause) : dialect,
      })
    }
    const first = connect(beforeDelete)
    const second = connect()
    cleanup(async () => {
      await first.destroy()
      await second.destroy()
      await rm(directory, { recursive: true, force: true })
    })
    return Object.assign([first, second] as const, { filename })
  }

  test('popFrom and dedup pushes wait for a write from another process', async ({
    assert,
    cleanup,
  }) => {
    const connections = await connectTwice(cleanup)
    await new KyselyQueueSchemaService(connections[0], { dialect: 'sqlite' }).createJobsTable()
    const adapter = new KyselyAdapter({ connection: connections[0], dialect: 'sqlite' })
    adapter.setWorkerId('worker-1')

    const stopContender = await startSqliteWriteContender(connections.filename)
    const outcomes: string[] = []
    try {
      for (let i = 0; i < 20; i++) {
        const job = { id: `job-${i}`, name: 'TestJob', payload: {}, attempts: 0 }
        for (let dispatch = 0; dispatch < 2; dispatch++) {
          const result = await adapter.pushOn('test-queue', { ...job, dedup: { id: `dedup-${i}` } })
          outcomes.push(result?.outcome ?? 'none')
        }
        await adapter.pushLaterOn('test-queue', { ...job, id: `delayed-${i}` }, 0)
        assert.isNotNull(await adapter.popFrom('test-queue'))
      }
    } finally {
      await stopContender()
    }

    assert.deepEqual(outcomes, Array.from({ length: 20 }, () => ['added', 'skipped']).flat())
  })

  registerRetentionRaceSuite({
    test,
    createAdapters: async (beforeDelete, cleanup) => {
      const [first, second] = await connectTwice(cleanup, beforeDelete)
      await new KyselyQueueSchemaService(first, { dialect: 'sqlite' }).createJobsTable()
      return [
        new KyselyAdapter({ connection: first, dialect: 'sqlite' }),
        new KyselyAdapter({ connection: second, dialect: 'sqlite' }),
      ]
    },
  })

  test('concurrent upserts from two connections all succeed', async ({ assert, cleanup }) => {
    const [first, second] = await connectTwice(cleanup)

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
