import type { Knex } from 'knex'
import {
  assertTimeZone,
  epochColumn,
  legacyScheduleRowToEpochs,
  processTimeZone,
  scheduleDatesMigrationState,
  type ScheduleDateColumn,
  type ScheduleDatesMigrationOptions,
} from './schedule_dates.js'
import { QUEUE_TEXT_COLUMNS, type TextColumnsMigrationOptions } from './text_columns.js'

interface MysqlTextColumn {
  name: string
  nullable: string
  collation: string | null
  comment: string
}

export class KnexQueueSchemaService {
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
      // LONGTEXT on MySQL, whose TEXT stops at 64 KB; TEXT elsewhere.
      table.text('data', 'longtext').notNullable()
      table.bigint('score').unsigned().nullable()
      table.string('worker_id', 255).nullable()
      table.bigint('acquired_at').unsigned().nullable()
      table.bigint('execute_at').unsigned().nullable()
      table.bigint('finished_at').unsigned().nullable()
      table.text('error', 'longtext').nullable()
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
      table.text('payload', 'longtext').notNullable()
      table.string('cron_expression', 255).nullable()
      table.bigint('every_ms').unsigned().nullable()
      table.string('timezone', 100).notNullable().defaultTo('UTC')
      // Dates are epoch milliseconds, so they do not depend on any time zone.
      table.bigint('from_date').nullable()
      table.bigint('to_date').nullable()
      table.integer('run_limit').unsigned().nullable()
      table.integer('run_count').unsigned().notNullable().defaultTo(0)
      table.bigint('next_run_at').nullable()
      table.bigint('last_run_at').nullable()
      table.bigint('created_at').notNullable()
      table.index(['status', 'next_run_at'])

      extend?.(table)
    })
  }

  /**
   * Migration for MySQL tables created before 0.8, whose TEXT columns stop at
   * 64 KB: a larger payload or error message failed to insert, or was cut in
   * non-strict mode. Converts `data`, `error` and `payload` to LONGTEXT and
   * keeps their nullability. Does nothing on other databases, on columns
   * already converted, or on a missing table.
   */
  async migrateTextColumns(options: TextColumnsMigrationOptions = {}): Promise<void> {
    if (this.#dialect() !== 'mysql') return

    const tables = [
      [options.jobsTable ?? 'queue_jobs', QUEUE_TEXT_COLUMNS.jobs],
      [options.schedulesTable ?? 'queue_schedules', QUEUE_TEXT_COLUMNS.schedules],
    ] as const

    for (const [tableName, columns] of tables) {
      const [schema, name] = tableName.includes('.') ? tableName.split('.', 2) : [null, tableName]
      const [rows] = await this.#connection.raw(
        `select column_name as name, is_nullable as nullable,
           collation_name as collation, column_comment as comment
         from information_schema.columns
         where table_schema = coalesce(?, database()) and table_name = ?
           and column_name in (${columns.map(() => '?').join(', ')})
           and data_type <> 'longtext'`,
        [schema, name, ...columns]
      )
      if (rows.length === 0) continue

      // MODIFY replaces the whole column definition: carry over what it would reset.
      // The collation and comment are bound: Knex does not escape backslashes in comments.
      const clauses: string[] = []
      const bindings: string[] = [tableName]
      for (const row of rows as MysqlTextColumn[]) {
        let clause = `modify ?? longtext ${row.nullable === 'YES' ? 'null' : 'not null'}`
        bindings.push(row.name)
        if (row.collation) {
          clause += ' collate ?'
          bindings.push(row.collation)
        }
        if (row.comment) {
          clause += ' comment ?'
          bindings.push(row.comment)
        }
        clauses.push(clause)
      }

      await this.#connection.raw(`alter table ?? ${clauses.join(', ')}`, bindings)
    }
  }

  /**
   * Migration for schedules tables created before 0.8, which stored their
   * dates in SQL date columns. Converts the dates to epoch milliseconds
   * (`bigint`), the format of the columns created by `createSchedulesTable()`.
   *
   * On MySQL, dates written by the adapter were stored without a time zone, in
   * the time zone the driver used: pass it as `options.timezone` (it defaults
   * to the time zone of the current process). PostgreSQL and SQLite dates are
   * converted exactly.
   *
   * Stop every process running the previous version first. The migration is
   * idempotent. MySQL cannot change a schema inside a transaction: if a run
   * fails there, run it again, it resumes where it stopped.
   */
  async migrateScheduleDates(
    tableName: string = 'queue_schedules',
    options: ScheduleDatesMigrationOptions = {}
  ): Promise<void> {
    const dialect = this.#dialect()
    const columnTypes = await this.#columnTypes(tableName)

    if (!columnTypes) {
      throw new Error(`Queue schedules table "${tableName}" does not exist`)
    }

    const state = scheduleDatesMigrationState(columnTypes)

    if (state.migrated) {
      await this.#createNextRunIndex(this.#connection, dialect, tableName)
      return
    }

    const writerTimeZone = options.timezone ?? processTimeZone()
    assertTimeZone(writerTimeZone)

    await this.#connection.transaction(async (trx) => {
      // Convert every value before the first schema change, so an unreadable
      // value fails the migration without leaving the table half migrated.
      const rows = state.legacy.length
        ? await this.#withDatabaseTimeZone(trx, dialect, options.databaseTimeZone, async () => {
            return (await trx(tableName).select(
              'id',
              ...state.legacy.map((name) => this.#legacyDateAsText(dialect, name))
            )) as Array<Record<string, string | number | null>>
          })
        : []
      // SQLite has no time zone: its CURRENT_TIMESTAMP default is UTC.
      const timeZone = dialect === 'sqlite' ? 'UTC' : writerTimeZone
      const updates = rows.map((row) => ({
        id: row.id,
        values: legacyScheduleRowToEpochs(row, state.legacy, timeZone),
      }))

      await this.#dropNextRunIndexes(trx, dialect, tableName)

      const missingEpochColumns = state.legacy.filter(
        (name) => !state.withEpochColumn.includes(name)
      )
      if (missingEpochColumns.length > 0) {
        await trx.schema.alterTable(tableName, (table) => {
          for (const name of missingEpochColumns) {
            // SQLite cannot add NOT NULL to an existing column, only to a new
            // one with a default. Every row gets its value below.
            if (name === 'created_at' && dialect === 'sqlite') {
              table.bigint(epochColumn(name)).notNullable().defaultTo(0)
            } else {
              table.bigint(epochColumn(name)).nullable()
            }
          }
        })
      }

      for (const { id, values } of updates) {
        await trx(tableName).where('id', id).update(values)
      }

      // created_at is required, as in a new table. Before the legacy columns
      // are dropped, so a failure on MySQL leaves a table the next run resumes.
      const epochColumns = [...new Set([...state.legacy, ...state.withEpochColumn])]
      if (epochColumns.includes('created_at') && dialect !== 'sqlite') {
        await trx.schema.alterTable(tableName, (table) => {
          if (dialect === 'pg') {
            table.dropNullable(epochColumn('created_at'))
          } else {
            table.bigint(epochColumn('created_at')).notNullable().alter()
          }
        })
      }

      if (state.legacy.length > 0) {
        await trx.schema.alterTable(tableName, (table) => {
          table.dropColumns(...state.legacy)
        })
      }

      await trx.schema.alterTable(tableName, (table) => {
        for (const name of epochColumns) table.renameColumn(epochColumn(name), name)
      })

      await this.#createNextRunIndex(trx, dialect, tableName)
    })
  }

  /** Whether the schedules table still uses the date columns of 0.7. */
  async needsScheduleDatesMigration(tableName: string = 'queue_schedules'): Promise<boolean> {
    const columnTypes = await this.#columnTypes(tableName)
    return columnTypes !== undefined && !scheduleDatesMigrationState(columnTypes).migrated
  }

  /**
   * Column types of the table the connection targets with `tableName`, or
   * undefined when it does not exist. On PostgreSQL, the table is resolved like
   * the adapter's queries resolve it, through the search_path: Knex's
   * hasTable() and columnInfo() only look in current_schema().
   */
  async #columnTypes(tableName: string): Promise<Map<string, string> | undefined> {
    if (this.#dialect() === 'pg') {
      const rendered = this.#renderedTable(tableName)
      const { rows } = await this.#connection.raw(
        `select a.attname as name, format_type(a.atttypid, a.atttypmod) as type
         from pg_attribute a
         where a.attrelid = to_regclass(?) and a.attnum > 0 and not a.attisdropped`,
        [rendered]
      )
      return rows.length > 0
        ? new Map(rows.map((row: { name: string; type: string }) => [row.name, row.type]))
        : undefined
    }

    if (!(await this.#connection.schema.hasTable(tableName))) return undefined

    const columns = await this.#connection(tableName).columnInfo()
    return new Map(Object.entries(columns).map(([name, info]) => [name, info.type]))
  }

  /**
   * Run `callback` with the MySQL session time zone set to `timeZone`, then
   * restore it. MySQL converts stored dates through the session time zone.
   */
  async #withDatabaseTimeZone<T>(
    trx: Knex.Transaction,
    dialect: 'pg' | 'mysql' | 'sqlite',
    timeZone: string | undefined,
    callback: () => Promise<T>
  ): Promise<T> {
    if (dialect !== 'mysql' || timeZone === undefined) return callback()

    const [[current]] = await trx.raw('select @@session.time_zone as time_zone')
    await trx.raw('set time_zone = ?', [timeZone])

    try {
      return await callback()
    } finally {
      await trx.raw('set time_zone = ?', [current.time_zone])
    }
  }

  /**
   * Indexes of the resolved table on exactly (status, next_run_at), whatever
   * their name. The name is not enough: an index with the same name can
   * belong to a table of another schema.
   */
  async #nextRunIndexes(
    connection: Knex,
    dialect: 'pg' | 'mysql' | 'sqlite',
    tableName: string
  ): Promise<Array<{ schema: string | null; name: string }>> {
    if (dialect === 'pg') {
      const { rows } = await connection.raw(
        `select n.nspname as schema, c.relname as name
         from pg_index i
         join pg_class c on c.oid = i.indexrelid
         join pg_namespace n on n.oid = c.relnamespace
         where i.indrelid = to_regclass(?)
           and array(
             select a.attname::text
             from unnest(i.indkey) with ordinality as k(attnum, position)
             join pg_attribute a on a.attrelid = i.indrelid and a.attnum = k.attnum
             order by k.position
           ) = array['status', 'next_run_at']`,
        [this.#renderedTable(tableName)]
      )
      return rows
    }

    if (dialect === 'mysql') {
      const [schema, name] = tableName.includes('.') ? tableName.split('.', 2) : [null, tableName]
      const [rows] = await connection.raw(
        `select index_name as name
         from information_schema.statistics
         where table_schema = coalesce(?, database()) and table_name = ?
         group by index_name
         having group_concat(column_name order by seq_in_index) = 'status,next_run_at'`,
        [schema, name]
      )
      return rows.map((row: { name: string }) => ({ schema: null, name: row.name }))
    }

    const rows = await connection.raw(
      `select il.name as name
       from pragma_index_list(?) il
       where (
         select group_concat(ii.name)
         from (select name from pragma_index_info(il.name) order by seqno) ii
       ) = 'status,next_run_at'`,
      [tableName]
    )
    return rows.map((row: { name: string }) => ({ schema: null, name: row.name }))
  }

  async #dropNextRunIndexes(
    trx: Knex.Transaction,
    dialect: 'pg' | 'mysql' | 'sqlite',
    tableName: string
  ): Promise<void> {
    for (const index of await this.#nextRunIndexes(trx, dialect, tableName)) {
      if (dialect === 'pg') {
        await trx.raw('DROP INDEX ??.??', [index.schema, index.name])
      } else if (dialect === 'mysql') {
        await trx.raw('DROP INDEX ?? ON ??', [index.name, tableName])
      } else {
        await trx.raw('DROP INDEX ??', [index.name])
      }
    }
  }

  async #createNextRunIndex(
    connection: Knex,
    dialect: 'pg' | 'mysql' | 'sqlite',
    tableName: string
  ): Promise<void> {
    if ((await this.#nextRunIndexes(connection, dialect, tableName)).length > 0) return

    // An index name cannot be schema-qualified: PostgreSQL creates the index in
    // the schema of its table.
    const bareTableName = tableName.split('.').at(-1)!
    await connection.raw('CREATE INDEX ?? ON ?? (??, ??)', [
      `${bareTableName}_status_next_run_at_index`,
      tableName,
      'status',
      'next_run_at',
    ])
  }

  /** The table reference as Knex renders it, such as `"tenant"."queue_schedules"`. */
  #renderedTable(tableName: string): string {
    return this.#connection.raw('??', [tableName]).toQuery()
  }

  /**
   * Select a legacy date column in a form `legacyScheduleDateToEpoch` can read.
   */
  #legacyDateAsText(dialect: 'pg' | 'mysql' | 'sqlite', name: ScheduleDateColumn): Knex.Raw {
    if (dialect === 'pg') return this.#connection.raw('??::text as ??', [name, name])

    if (dialect === 'mysql') {
      // created_at was generated by the MySQL server in the session time zone,
      // so the server converts it exactly.
      if (name === 'created_at') {
        return this.#connection.raw('ROUND(UNIX_TIMESTAMP(??) * 1000) as ??', [name, name])
      }
      return this.#connection.raw('CAST(?? AS CHAR) as ??', [name, name])
    }

    return this.#connection.raw('CAST(?? AS TEXT) as ??', [name, name])
  }

  #dialect(): 'pg' | 'mysql' | 'sqlite' {
    const dialect = this.#connection.client.dialect as string
    if (dialect === 'postgresql') return 'pg'
    if (dialect === 'mysql' || dialect === 'mysql2') return 'mysql'
    return 'sqlite'
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
