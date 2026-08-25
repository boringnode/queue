import Knex from 'knex'
import { test } from '@japa/runner'
import { Redis } from 'ioredis'
import { RedisAdapter } from '../src/drivers/redis_adapter.js'
import { KnexAdapter } from '../src/drivers/knex_adapter.js'
import { KnexQueueSchemaService } from '../src/services/knex_queue_schema.js'
import { registerWorkerConcurrencyTestSuite } from './_utils/register_worker_concurrency_suite.js'

const KEY_PREFIX = 'boringnode::queue::concurrency-test::'

test.group('Worker Concurrency | Redis', (group) => {
  let connection: Redis

  group.each.setup(() => {
    connection = new Redis({
      host: process.env.REDIS_HOST || 'localhost',
      port: Number.parseInt(process.env.REDIS_PORT || '6379', 10),
      keyPrefix: KEY_PREFIX,
      db: 0,
    })

    return async () => {
      const keys = await connection.keys(`${KEY_PREFIX}*`)
      if (keys.length > 0) {
        const keysWithoutPrefix = keys.map((k) => k.replace(KEY_PREFIX, ''))
        await connection.del(...keysWithoutPrefix)
      }
      await connection.quit()
    }
  })

  registerWorkerConcurrencyTestSuite({
    test,
    createAdapter: () => new RedisAdapter(connection),
  })
})

test.group('Worker Concurrency | Knex (PostgreSQL)', (group) => {
  let connection: ReturnType<typeof Knex>
  let adapter: KnexAdapter
  let schemaService: KnexQueueSchemaService
  const tableName = 'queue_jobs_concurrency_test'
  const schedulesTableName = 'queue_schedules_concurrency_test'

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

    // Drop both tables to ensure clean state
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

  registerWorkerConcurrencyTestSuite({
    test,
    createAdapter: () => {
      adapter = new KnexAdapter({ connection, tableName, schedulesTableName })
      return adapter
    },
  })
})
