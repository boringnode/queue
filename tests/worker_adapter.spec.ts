import Knex from 'knex'
import { test } from '@japa/runner'
import { Redis } from 'ioredis'
import { MemoryAdapter } from './_mocks/memory_adapter.js'
import { RedisAdapter } from '../src/drivers/redis_adapter.js'
import { KnexAdapter } from '../src/drivers/knex_adapter.js'
import { KnexQueueSchemaService } from '../src/services/knex_queue_schema.js'
import { registerWorkerRetryTestSuite } from './_utils/register_worker_retry_suite.js'

const KEY_PREFIX = 'boringnode::queue::worker-test::'

test.group('Worker Adapter | Memory', (group) => {
  let adapter: MemoryAdapter

  group.each.teardown(async () => {
    await adapter?.destroy()
  })

  registerWorkerRetryTestSuite({
    test,
    createAdapter: () => {
      adapter = new MemoryAdapter()
      return adapter
    },
  })
})

test.group('Worker Adapter | Redis', (group) => {
  let connection: Redis

  group.each.setup(async () => {
    connection = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
      keyPrefix: KEY_PREFIX,
      db: 14,
    })

    await connection.flushdb()

    return async () => {
      await connection.quit()
    }
  })

  registerWorkerRetryTestSuite({
    test,
    createAdapter: () => new RedisAdapter(connection),
  })
})

test.group('Worker Adapter | Knex (SQLite)', (group) => {
  let connection: ReturnType<typeof Knex>
  let adapter: KnexAdapter

  group.each.setup(async () => {
    connection = Knex({
      client: 'better-sqlite3',
      connection: {
        filename: ':memory:',
      },
      useNullAsDefault: true,
    })

    const schemaService = new KnexQueueSchemaService(connection)
    await schemaService.createJobsTable()
    await schemaService.createSchedulesTable()

    return async () => {
      await adapter?.destroy()
      await connection.destroy()
    }
  })

  registerWorkerRetryTestSuite({
    test,
    createAdapter: () => {
      adapter = new KnexAdapter({ connection })
      return adapter
    },
  })
})

test.group('Worker Adapter | Knex (PostgreSQL)', (group) => {
  let connection: ReturnType<typeof Knex>
  let adapter: KnexAdapter
  let schemaService: KnexQueueSchemaService
  const tableName = 'queue_jobs_worker_test'
  const schedulesTableName = 'queue_schedules_worker_test'

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
    await schemaService.dropJobsTable(tableName)
    await schemaService.dropSchedulesTable(schedulesTableName)
    await schemaService.createJobsTable(tableName)
    await schemaService.createSchedulesTable(schedulesTableName)

    return async () => {
      await adapter?.destroy()
      await schemaService.dropJobsTable(tableName)
      await schemaService.dropSchedulesTable(schedulesTableName)
      await connection.destroy()
    }
  })

  registerWorkerRetryTestSuite({
    test,
    createAdapter: () => {
      adapter = new KnexAdapter({ connection, tableName, schedulesTableName })
      return adapter
    },
  })
})
