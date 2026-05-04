import type { Knex } from 'knex'

export class QueueSchemaService {
  #connection: Knex

  constructor(connection: Knex) {
    this.#connection = connection
  }

  /**
   * Creates the jobs table with the default schema.
   * The optional callback allows adding custom columns.
   */
  async createJobsTable(
    tableName: string = 'queue_jobs',
    extend?: (table: Knex.CreateTableBuilder) => void
  ): Promise<void> {
    await this.#connection.schema.createTable(tableName, (table) => {
      table.string('id', 255).notNullable()
      table.string('queue', 255).notNullable()
      table.enu('status', ['pending', 'active', 'delayed', 'completed', 'failed']).notNullable()
      table.text('data').notNullable()
      table.bigint('score').unsigned().nullable()
      table.string('worker_id', 255).nullable()
      table.bigint('acquired_at').unsigned().nullable()
      table.bigint('execute_at').unsigned().nullable()
      table.bigint('finished_at').unsigned().nullable()
      table.text('error').nullable()
      table.string('dedup_id', 510).nullable()
      table.bigint('dedup_at').unsigned().nullable()
      table.bigint('dedup_ttl').unsigned().nullable()
      table.primary(['id', 'queue'])
      table.index(['queue', 'status', 'score'])
      table.index(['queue', 'status', 'execute_at'])
      table.index(['queue', 'status', 'finished_at'])
      table.index(['queue', 'dedup_id'])

      extend?.(table)
    })

    await this.#createDedupActiveUniqueIndex(tableName)
  }

  /**
   * Idempotent migration: adds dedup columns (dedup_id, dedup_at, dedup_ttl)
   * and a (queue, dedup_id) index to an existing jobs table.
   *
   * Safe to run multiple times. Uses hasColumn checks so it won't fail on re-runs.
   * For large Postgres tables, consider pausing workers during the run.
   */
  async addDedupColumns(tableName: string = 'queue_jobs'): Promise<void> {
    const hasDedupId = await this.#connection.schema.hasColumn(tableName, 'dedup_id')
    const hasDedupAt = await this.#connection.schema.hasColumn(tableName, 'dedup_at')
    const hasDedupTtl = await this.#connection.schema.hasColumn(tableName, 'dedup_ttl')

    if (!hasDedupId || !hasDedupAt || !hasDedupTtl) {
      await this.#connection.schema.alterTable(tableName, (table) => {
        if (!hasDedupId) table.string('dedup_id', 510).nullable()
        if (!hasDedupAt) table.bigint('dedup_at').unsigned().nullable()
        if (!hasDedupTtl) table.bigint('dedup_ttl').unsigned().nullable()
      })
    }

    if (!hasDedupId) {
      await this.#connection.schema.alterTable(tableName, (table) => {
        table.index(['queue', 'dedup_id'])
      })
    }

    await this.#createDedupActiveUniqueIndex(tableName)
  }

  /**
   * Partial unique index on (queue, dedup_id) for active dedup slots.
   * Prevents two concurrent inserts with the same dedup_id from both succeeding.
   * Only PG and SQLite support partial unique indexes; MySQL is skipped.
   */
  async #createDedupActiveUniqueIndex(tableName: string): Promise<void> {
    const client = this.#connection.client.config.client
    if (client !== 'pg' && client !== 'better-sqlite3' && client !== 'sqlite3') return

    const indexName = `${tableName}_dedup_active_uidx`
    await this.#connection.raw(
      `CREATE UNIQUE INDEX IF NOT EXISTS ?? ON ?? ("queue", "dedup_id") ` +
        `WHERE "dedup_id" IS NOT NULL AND "status" IN ('pending', 'delayed')`,
      [indexName, tableName]
    )
  }

  /**
   * Creates the schedules table with the default schema.
   * The optional callback allows adding custom columns.
   */
  async createSchedulesTable(
    tableName: string = 'queue_schedules',
    extend?: (table: Knex.CreateTableBuilder) => void
  ): Promise<void> {
    await this.#connection.schema.createTable(tableName, (table) => {
      table.string('id', 255).primary()
      table.string('status', 50).notNullable().defaultTo('active')
      table.string('name', 255).notNullable()
      table.text('payload').notNullable()
      table.string('cron_expression', 255).nullable()
      table.bigint('every_ms').unsigned().nullable()
      table.string('timezone', 100).notNullable().defaultTo('UTC')
      table.timestamp('from_date').nullable()
      table.timestamp('to_date').nullable()
      table.integer('run_limit').unsigned().nullable()
      table.integer('run_count').unsigned().notNullable().defaultTo(0)
      table.timestamp('next_run_at').nullable()
      table.timestamp('last_run_at').nullable()
      table.timestamp('created_at').notNullable().defaultTo(this.#connection.fn.now())
      table.index(['status', 'next_run_at'])

      extend?.(table)
    })
  }

  /**
   * Drops the jobs table if it exists.
   */
  async dropJobsTable(tableName: string = 'queue_jobs'): Promise<void> {
    await this.#connection.schema.dropTableIfExists(tableName)
  }

  /**
   * Drops the schedules table if it exists.
   */
  async dropSchedulesTable(tableName: string = 'queue_schedules'): Promise<void> {
    await this.#connection.schema.dropTableIfExists(tableName)
  }
}
