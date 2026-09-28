import { sql, type Kysely, type Transaction } from 'kysely'
import {
  assertTimeZone,
  epochColumn,
  legacyScheduleDateToEpoch,
  processTimeZone,
  scheduleDatesMigrationState,
  type ScheduleDateColumn,
  type ScheduleDatesMigrationOptions,
} from './schedule_dates.js'

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
      // Dates are epoch milliseconds, so they do not depend on any time zone.
      .addColumn('from_date', 'bigint')
      .addColumn('to_date', 'bigint')
      .addColumn('run_limit', 'integer')
      .addColumn('run_count', 'integer', (column) => column.notNull().defaultTo(0))
      .addColumn('next_run_at', 'bigint')
      .addColumn('last_run_at', 'bigint')
      .addColumn('created_at', 'bigint', (column) => column.notNull())
      .execute()

    await this.#connection.schema
      .createIndex(`${tableName}_status_next_run_idx`)
      .on(tableName)
      .columns(['status', 'next_run_at'])
      .execute()
  }

  /**
   * Migration for schedules tables created before 0.8, which stored their
   * dates in SQL date columns. Converts the dates to epoch milliseconds
   * (`bigint`), the format of the columns created by `createSchedulesTable()`.
   *
   * On PostgreSQL and MySQL, dates were stored without a time zone, in the time
   * zone the driver used: pass it as `options.timezone` (it defaults to the time
   * zone of the current process). SQLite dates are converted exactly.
   *
   * Stop every process running the previous version first. The migration is
   * idempotent. MySQL cannot change a schema inside a transaction: if a run
   * fails there, run it again, it resumes where it stopped.
   */
  async migrateScheduleDates(
    tableName: string = 'queue_schedules',
    options: ScheduleDatesMigrationOptions = {}
  ): Promise<void> {
    const columnTypes = await this.#columnTypes(tableName)

    if (!columnTypes) {
      throw new Error(`Queue schedules table "${tableName}" does not exist`)
    }

    const state = scheduleDatesMigrationState(columnTypes)

    if (state.migrated) {
      await this.#createNextRunIndex(this.#connection, tableName)
      return
    }

    const writerTimeZone = options.timezone ?? processTimeZone()
    assertTimeZone(writerTimeZone)

    const migrate = async (trx: Transaction<DB>) => {
      // Convert every value before the first schema change, so an unreadable
      // value fails the migration without leaving the table half migrated.
      const rows = state.legacy.length
        ? await this.#withDatabaseTimeZone(trx, options.databaseTimeZone, async () => {
            // The query builder applies withSchema(), unlike raw SQL table references.
            return (await (trx as unknown as Kysely<Record<string, Record<string, unknown>>>)
              .selectFrom(tableName)
              .select([
                sql.ref<string>('id').as('id'),
                ...state.legacy.map((name) => this.#legacyDateAsText(name).as(name)),
              ])
              .execute()) as Array<Record<string, string | number | null>>
          })
        : []
      // SQLite has no time zone: its CURRENT_TIMESTAMP default is UTC.
      const timeZone = this.#dialect === 'sqlite' ? 'UTC' : writerTimeZone
      const updates = rows.map((row) => ({
        id: row.id,
        values: Object.fromEntries(
          state.legacy.map((name) => [
            epochColumn(name),
            legacyScheduleDateToEpoch(row[name], timeZone),
          ])
        ),
      }))

      await this.#dropNextRunIndexes(trx, tableName)

      for (const name of state.legacy) {
        if (state.withEpochColumn.includes(name)) continue
        await trx.schema.alterTable(tableName).addColumn(epochColumn(name), 'bigint').execute()
      }

      for (const { id, values } of updates) {
        await (trx as unknown as Kysely<Record<string, Record<string, unknown>>>)
          .updateTable(tableName)
          .set(values)
          .where('id', '=', id)
          .execute()
      }

      for (const name of state.legacy) {
        await trx.schema.alterTable(tableName).dropColumn(name).execute()
      }

      for (const name of new Set([...state.legacy, ...state.withEpochColumn])) {
        await trx.schema.alterTable(tableName).renameColumn(epochColumn(name), name).execute()
      }

      await this.#createNextRunIndex(trx, tableName)
    }

    // Kysely's Migrator already runs each migration inside a transaction.
    if (this.#connection.isTransaction) {
      await migrate(this.#connection as Transaction<DB>)
    } else {
      await this.#connection.transaction().execute(migrate)
    }
  }

  /** Whether the schedules table still uses the date columns of 0.7. */
  async needsScheduleDatesMigration(tableName: string = 'queue_schedules'): Promise<boolean> {
    const columnTypes = await this.#columnTypes(tableName)
    return columnTypes !== undefined && !scheduleDatesMigrationState(columnTypes).migrated
  }

  /**
   * Column types of the table the connection targets with `tableName`, or
   * undefined when it does not exist. The table is resolved like the adapter's
   * queries resolve it: with withSchema() and the PostgreSQL search_path.
   */
  async #columnTypes(tableName: string): Promise<Map<string, string> | undefined> {
    const rows =
      this.#dialect === 'postgres'
        ? await this.#postgresColumns(tableName)
        : this.#dialect === 'mysql'
          ? await this.#mysqlColumns(tableName)
          : await this.#sqliteColumns(tableName)

    return rows.length > 0 ? new Map(rows.map(({ name, type }) => [name, type])) : undefined
  }

  /** The table reference as the connection renders it in a query, with withSchema() applied. */
  #renderedTable(tableName: string): string {
    const { sql: compiled } = (this.#connection as unknown as Kysely<any>)
      .selectFrom(tableName)
      .selectAll()
      .compile()

    return compiled.replace(/^select \* from /, '')
  }

  async #postgresColumns(tableName: string) {
    // to_regclass resolves the reference like any query would, search_path included.
    const result = await sql<{ name: string; type: string }>`
      select a.attname as ${sql.ref('name')}, format_type(a.atttypid, a.atttypmod) as ${sql.ref('type')}
      from pg_attribute a
      where a.attrelid = to_regclass(${this.#renderedTable(tableName)})
        and a.attnum > 0 and not a.attisdropped
    `.execute(this.#connection)

    return result.rows
  }

  /** Schema (null for the current database) and name of the table, with withSchema() applied. */
  #mysqlTable(tableName: string): [string | null, string] {
    const identifiers = [...this.#renderedTable(tableName).matchAll(/`((?:[^`]|``)*)`/g)].map(
      ([, identifier]) => identifier.replaceAll('``', '`')
    )
    return identifiers.length > 1 ? [identifiers[0], identifiers[1]] : [null, identifiers[0]]
  }

  async #mysqlColumns(tableName: string) {
    const [schema, name] = this.#mysqlTable(tableName)

    const result = await sql<{ name: string; type: string }>`
      select column_name as ${sql.ref('name')}, data_type as ${sql.ref('type')}
      from information_schema.columns
      where table_schema = coalesce(${schema}, database()) and table_name = ${name}
    `.execute(this.#connection)

    return result.rows
  }

  async #sqliteColumns(tableName: string) {
    const table = (await this.#connection.introspection.getTables()).find(
      ({ name }) => name === tableName
    )

    return table?.columns.map(({ name, dataType }) => ({ name, type: dataType })) ?? []
  }

  /**
   * Run `callback` with the MySQL session time zone set to `timeZone`, then
   * restore it. MySQL converts stored dates through the session time zone.
   */
  async #withDatabaseTimeZone<T>(
    trx: Transaction<DB>,
    timeZone: string | undefined,
    callback: () => Promise<T>
  ): Promise<T> {
    if (this.#dialect !== 'mysql' || timeZone === undefined) return callback()

    const current = await sql<{
      timeZone: string
    }>`select @@session.time_zone as ${sql.ref('timeZone')}`.execute(trx)
    await sql`set time_zone = ${timeZone}`.execute(trx)

    try {
      return await callback()
    } finally {
      await sql`set time_zone = ${current.rows[0].timeZone}`.execute(trx)
    }
  }

  /**
   * Indexes of the resolved table on exactly (status, next_run_at), whatever
   * their name. The name is not enough: an index with the same name can
   * belong to a table of another schema.
   */
  async #nextRunIndexes(
    connection: Kysely<DB>,
    tableName: string
  ): Promise<Array<{ schema: string | null; name: string }>> {
    if (this.#dialect === 'postgres') {
      const result = await sql<{ schema: string; name: string }>`
        select n.nspname as ${sql.ref('schema')}, c.relname as ${sql.ref('name')}
        from pg_index i
        join pg_class c on c.oid = i.indexrelid
        join pg_namespace n on n.oid = c.relnamespace
        where i.indrelid = to_regclass(${this.#renderedTable(tableName)})
          and array(
            select a.attname::text
            from unnest(i.indkey) with ordinality as k(attnum, position)
            join pg_attribute a on a.attrelid = i.indrelid and a.attnum = k.attnum
            order by k.position
          ) = array['status', 'next_run_at']
      `.execute(connection)
      return result.rows
    }

    if (this.#dialect === 'mysql') {
      const [schema, name] = this.#mysqlTable(tableName)
      const result = await sql<{ name: string }>`
        select index_name as ${sql.ref('name')}
        from information_schema.statistics
        where table_schema = coalesce(${schema}, database()) and table_name = ${name}
        group by index_name
        having group_concat(column_name order by seq_in_index) = 'status,next_run_at'
      `.execute(connection)
      return result.rows.map(({ name: indexName }) => ({ schema: null, name: indexName }))
    }

    const result = await sql<{ name: string }>`
      select il.name as ${sql.ref('name')}
      from pragma_index_list(${tableName}) il
      where (
        select group_concat(ii.name)
        from (select name from pragma_index_info(il.name) order by seqno) ii
      ) = 'status,next_run_at'
    `.execute(connection)
    return result.rows.map(({ name }) => ({ schema: null, name }))
  }

  async #dropNextRunIndexes(trx: Transaction<DB>, tableName: string): Promise<void> {
    for (const index of await this.#nextRunIndexes(trx, tableName)) {
      if (this.#dialect === 'postgres') {
        await sql`drop index ${sql.id(index.schema!, index.name)}`.execute(trx)
      } else if (this.#dialect === 'mysql') {
        await trx.schema.dropIndex(index.name).on(tableName).execute()
      } else {
        await sql`drop index ${sql.id(index.name)}`.execute(trx)
      }
    }
  }

  async #createNextRunIndex(connection: Kysely<DB>, tableName: string): Promise<void> {
    if ((await this.#nextRunIndexes(connection, tableName)).length > 0) return

    // An index name cannot be schema-qualified: PostgreSQL creates the index in
    // the schema of its table.
    const bareTableName = tableName.split('.').at(-1)!
    await connection.schema
      .createIndex(`${bareTableName}_status_next_run_idx`)
      .on(tableName)
      .columns(['status', 'next_run_at'])
      .execute()
  }

  /** Select a legacy date column in a form `legacyScheduleDateToEpoch` can read. */
  #legacyDateAsText(name: ScheduleDateColumn) {
    if (this.#dialect === 'postgres') return sql<string>`${sql.ref(name)}::text`
    if (this.#dialect === 'mysql') return sql<string>`CAST(${sql.ref(name)} AS CHAR)`
    return sql<string>`CAST(${sql.ref(name)} AS TEXT)`
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
