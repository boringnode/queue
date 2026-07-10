import { sql, type Kysely } from 'kysely'

export type KyselyDialect = 'postgres' | 'mysql' | 'sqlite'

export interface KyselyQueueSchemaOptions {
  dialect: KyselyDialect
}

/**
 * Creates and removes the queue tables using Kysely's schema builder.
 *
 * The service never owns or destroys the supplied Kysely connection.
 */
export class KyselyQueueSchemaService<DB> {
  readonly #connection: Kysely<DB>
  readonly #dialect: KyselyDialect

  constructor(connection: Kysely<DB>, options: KyselyQueueSchemaOptions) {
    this.#connection = connection
    this.#dialect = options.dialect
  }

  async createJobsTable(tableName: string = 'queue_jobs'): Promise<void> {
    await this.#connection.schema
      .createTable(tableName)
      .addColumn('id', 'varchar(255)', (column) => column.notNull())
      .addColumn('queue', 'varchar(255)', (column) => column.notNull())
      .addColumn('status', 'varchar(20)', (column) => column.notNull())
      .addColumn('data', 'text', (column) => column.notNull())
      .addColumn('score', 'bigint')
      .addColumn('worker_id', 'varchar(255)')
      .addColumn('acquired_at', 'bigint')
      .addColumn('execute_at', 'bigint')
      .addColumn('finished_at', 'bigint')
      .addColumn('error', 'text')
      .addColumn('dedup_id', 'varchar(510)')
      .addColumn('dedup_at', 'bigint')
      .addColumn('dedup_ttl', 'bigint')
      .addPrimaryKeyConstraint(`${tableName}_primary`, ['id', 'queue'])
      .execute()

    await this.#createJobsIndexes(tableName)
    await this.#createDedupActiveUniqueIndex(tableName)
  }

  async addDedupColumns(tableName: string = 'queue_jobs'): Promise<void> {
    const table = (await this.#connection.introspection.getTables()).find(
      (candidate) => candidate.name === tableName
    )

    if (!table) {
      throw new Error(`Queue jobs table "${tableName}" does not exist`)
    }

    const columns = new Set(table.columns.map((column) => column.name))

    if (!columns.has('dedup_id')) {
      await this.#connection.schema
        .alterTable(tableName)
        .addColumn('dedup_id', 'varchar(510)')
        .execute()
    }
    if (!columns.has('dedup_at')) {
      await this.#connection.schema.alterTable(tableName).addColumn('dedup_at', 'bigint').execute()
    }
    if (!columns.has('dedup_ttl')) {
      await this.#connection.schema.alterTable(tableName).addColumn('dedup_ttl', 'bigint').execute()
    }

    const index = this.#connection.schema
      .createIndex(`${tableName}_queue_dedup_idx`)
      .on(tableName)
      .columns(['queue', 'dedup_id'])

    if (this.#dialect === 'mysql') {
      try {
        await index.execute()
      } catch (error) {
        if (!this.#isDuplicateIndexError(error)) throw error
      }
    } else {
      await index.ifNotExists().execute()
    }
    await this.#createDedupActiveUniqueIndex(tableName)
  }

  async createSchedulesTable(tableName: string = 'queue_schedules'): Promise<void> {
    await this.#connection.schema
      .createTable(tableName)
      .addColumn('id', 'varchar(255)', (column) => column.primaryKey())
      .addColumn('status', 'varchar(50)', (column) => column.notNull().defaultTo('active'))
      .addColumn('name', 'varchar(255)', (column) => column.notNull())
      .addColumn('payload', 'text', (column) => column.notNull())
      .addColumn('cron_expression', 'varchar(255)')
      .addColumn('every_ms', 'bigint')
      .addColumn('timezone', 'varchar(100)', (column) => column.notNull().defaultTo('UTC'))
      .addColumn('from_date', 'timestamp')
      .addColumn('to_date', 'timestamp')
      .addColumn('run_limit', 'integer')
      .addColumn('run_count', 'integer', (column) => column.notNull().defaultTo(0))
      .addColumn('next_run_at', 'timestamp')
      .addColumn('last_run_at', 'timestamp')
      .addColumn('created_at', 'timestamp', (column) =>
        column.notNull().defaultTo(sql`CURRENT_TIMESTAMP`)
      )
      .execute()

    await this.#connection.schema
      .createIndex(`${tableName}_status_next_run_idx`)
      .on(tableName)
      .columns(['status', 'next_run_at'])
      .execute()
  }

  async dropJobsTable(tableName: string = 'queue_jobs'): Promise<void> {
    await this.#connection.schema.dropTable(tableName).ifExists().execute()
  }

  async dropSchedulesTable(tableName: string = 'queue_schedules'): Promise<void> {
    await this.#connection.schema.dropTable(tableName).ifExists().execute()
  }

  async #createJobsIndexes(tableName: string): Promise<void> {
    const indexes: [string, string[]][] = [
      ['status_score', ['queue', 'status', 'score']],
      ['status_execute', ['queue', 'status', 'execute_at']],
      ['status_finished', ['queue', 'status', 'finished_at']],
      ['queue_dedup', ['queue', 'dedup_id']],
    ]

    for (const [suffix, columns] of indexes) {
      await this.#connection.schema
        .createIndex(`${tableName}_${suffix}_idx`)
        .on(tableName)
        .columns(columns)
        .execute()
    }
  }

  async #createDedupActiveUniqueIndex(tableName: string): Promise<void> {
    if (this.#dialect === 'mysql') return

    await this.#connection.schema
      .createIndex(`${tableName}_dedup_active_uidx`)
      .ifNotExists()
      .unique()
      .on(tableName)
      .columns(['queue', 'dedup_id'])
      .where(sql<boolean>`dedup_id is not null and status in ('pending', 'delayed')`)
      .execute()
  }

  #isDuplicateIndexError(error: unknown): boolean {
    if (!error || typeof error !== 'object') return false
    const candidate = error as { code?: string; errno?: number }
    return candidate.code === 'ER_DUP_KEYNAME' || candidate.errno === 1061
  }
}
