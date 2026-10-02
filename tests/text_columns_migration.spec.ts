import Database from 'better-sqlite3'
import Knex, { type Knex as KnexType } from 'knex'
import { Kysely, MysqlDialect, PostgresDialect, SqliteDialect, sql } from 'kysely'
import { createPool } from 'mysql2'
import { Pool } from 'pg'
import { test } from '@japa/runner'
import { KnexAdapter, KnexQueueSchemaService } from '../src/drivers/knex_adapter.js'
import { KyselyAdapter, KyselyQueueSchemaService } from '../src/drivers/kysely_adapter.js'
import type { Adapter } from '../src/contracts/adapter.js'

type Dialect = 'sqlite' | 'postgres' | 'mysql'

const JOBS_TABLE = 'legacy_text_queue_jobs'
const SCHEDULES_TABLE = 'legacy_text_queue_schedules'
const BIG_PAYLOAD = { blob: 'x'.repeat(70_000) }

const pgConfig = () => ({
  host: process.env.PG_HOST || 'localhost',
  port: Number.parseInt(process.env.PG_PORT || '5432', 10),
  user: process.env.PG_USER || 'postgres',
  password: process.env.PG_PASSWORD || 'postgres',
  database: process.env.PG_DATABASE || 'queue_test',
})

const mysqlConfig = () => ({
  host: process.env.MYSQL_HOST || 'localhost',
  port: Number.parseInt(process.env.MYSQL_PORT || '3307', 10),
  user: process.env.MYSQL_USER || 'root',
  password: process.env.MYSQL_PASSWORD || 'mysql',
  database: process.env.MYSQL_DATABASE || 'queue_test',
})

/** Column type and nullability of the text columns, read from the database catalog. */
async function textColumns(
  query: (text: string) => Promise<Array<{ name: string; type: string; nullable: string }>>,
  dialect: Dialect
) {
  if (dialect === 'sqlite') {
    const rows = [
      ...(await query(
        `select name, type, case "notnull" when 1 then 'NO' else 'YES' end as nullable from pragma_table_info('${JOBS_TABLE}')`
      )),
      ...(await query(
        `select name, type, case "notnull" when 1 then 'NO' else 'YES' end as nullable from pragma_table_info('${SCHEDULES_TABLE}')`
      )),
    ]
    return new Map(
      rows
        .filter((row) => ['data', 'error', 'payload'].includes(row.name))
        .map((row) => [row.name, `${row.type.toLowerCase()} ${row.nullable}`])
    )
  }

  const schema = dialect === 'postgres' ? 'current_schema()' : 'database()'
  const rows = await query(
    `select column_name as name, data_type as type, is_nullable as nullable
     from information_schema.columns
     where table_schema = ${schema} and table_name in ('${JOBS_TABLE}', '${SCHEDULES_TABLE}')
       and column_name in ('data', 'error', 'payload')`
  )
  return new Map(rows.map((row) => [row.name, `${row.type.toLowerCase()} ${row.nullable}`]))
}

const NEW_TABLE_COLUMNS = (dialect: Dialect) => {
  const type = dialect === 'mysql' ? 'longtext' : 'text'
  return { data: `${type} NO`, error: `${type} YES`, payload: `${type} NO` }
}

/** The adapter can store and read back a payload larger than 64 KB. */
async function assertLargePayloads(assert: any, adapter: Adapter) {
  adapter.setWorkerId('worker-1')
  await adapter.pushOn('default', { id: 'big', name: 'Big', payload: BIG_PAYLOAD, attempts: 0 })
  assert.deepEqual((await adapter.popFrom('default'))!.payload, BIG_PAYLOAD)

  await adapter.upsertSchedule({
    id: 'big',
    name: 'Big',
    payload: BIG_PAYLOAD,
    everyMs: 60_000,
    timezone: 'UTC',
  })
  assert.deepEqual((await adapter.getSchedule('big'))!.payload, BIG_PAYLOAD)
}

for (const dialect of ['sqlite', 'postgres', 'mysql'] as const) {
  test.group(`Text columns migration | Knex (${dialect})`, (group) => {
    let connection: KnexType
    let newTableColumns: Record<string, string>
    const query = async (text: string) => {
      const result = await connection.raw(text)
      return dialect === 'postgres' ? result.rows : dialect === 'mysql' ? result[0] : result
    }

    group.each.setup(async () => {
      connection =
        dialect === 'postgres'
          ? Knex({ client: 'pg', connection: pgConfig() })
          : dialect === 'mysql'
            ? Knex({ client: 'mysql2', connection: mysqlConfig() })
            : Knex({
                client: 'better-sqlite3',
                connection: { filename: ':memory:' },
                useNullAsDefault: true,
              })

      const schema = new KnexQueueSchemaService(connection)
      await schema.dropJobsTable(JOBS_TABLE)
      await schema.dropSchedulesTable(SCHEDULES_TABLE)
      await schema.createJobsTable(JOBS_TABLE)
      await schema.createSchedulesTable(SCHEDULES_TABLE)
      newTableColumns = Object.fromEntries(await textColumns(query, dialect))

      if (dialect === 'mysql') {
        // The columns created by KnexQueueSchemaService in 0.7.
        await connection.raw(`alter table ?? modify data text not null, modify error text null`, [
          JOBS_TABLE,
        ])
        await connection.raw(`alter table ?? modify payload text not null`, [SCHEDULES_TABLE])
      }

      return async () => {
        await schema.dropJobsTable(JOBS_TABLE)
        await schema.dropSchedulesTable(SCHEDULES_TABLE)
        await connection.destroy()
      }
    })

    const migrate = () =>
      new KnexQueueSchemaService(connection).migrateTextColumns({
        jobsTable: JOBS_TABLE,
        schedulesTable: SCHEDULES_TABLE,
      })
    const adapter = () =>
      new KnexAdapter({ connection, tableName: JOBS_TABLE, schedulesTableName: SCHEDULES_TABLE })

    test('converts the text columns and keeps their nullability', async ({ assert }) => {
      const before = await textColumns(query, dialect)

      await migrate()
      await migrate()

      const after = await textColumns(query, dialect)
      if (dialect === 'mysql') {
        assert.deepEqual(Object.fromEntries(after), {
          data: 'longtext NO',
          error: 'longtext YES',
          payload: 'longtext NO',
        })
      } else {
        assert.deepEqual(after, before)
      }

      await assertLargePayloads(assert, adapter())
    })

    test('creates new tables with the right text type', ({ assert }) => {
      assert.deepEqual(newTableColumns, NEW_TABLE_COLUMNS(dialect))
    })

    if (dialect === 'mysql') {
      test('keeps the collation and comment of a column', async ({ assert }) => {
        await connection.raw(
          `alter table ?? modify data text character set utf8mb4 collate utf8mb4_bin not null comment ?, modify error text null comment ?`,
          [JOBS_TABLE, 'job payload', 'C:\\new\\table\\']
        )

        await migrate()

        const [rows] = await connection.raw(
          `select column_name as name, data_type as type, collation_name as collation,
             column_comment as comment
           from information_schema.columns
           where table_schema = database() and table_name = ? and column_name in ('data', 'error')
           order by column_name`,
          [JOBS_TABLE]
        )
        assert.deepEqual(
          rows.map(({ name, type, collation, comment }: any) => ({
            name,
            type,
            collation,
            comment,
          })),
          [
            { name: 'data', type: 'longtext', collation: 'utf8mb4_bin', comment: 'job payload' },
            {
              name: 'error',
              type: 'longtext',
              collation: rows[1].collation,
              comment: 'C:\\new\\table\\',
            },
          ]
        )
      })
    }

    test('skips a missing schedules table', async ({ assert }) => {
      await new KnexQueueSchemaService(connection).dropSchedulesTable(SCHEDULES_TABLE)

      await migrate()

      const columns = await textColumns(query, dialect)
      assert.equal(columns.get('data'), dialect === 'mysql' ? 'longtext NO' : columns.get('data'))
    })
  })

  test.group(`Text columns migration | Kysely (${dialect})`, (group) => {
    let connection: Kysely<any>
    let newTableColumns: Record<string, string>
    const query = async (text: string) => (await sql.raw<any>(text).execute(connection)).rows

    group.each.setup(async () => {
      connection = new Kysely({
        dialect:
          dialect === 'postgres'
            ? new PostgresDialect({ pool: new Pool(pgConfig()) })
            : dialect === 'mysql'
              ? new MysqlDialect({ pool: createPool(mysqlConfig()) })
              : new SqliteDialect({ database: new Database(':memory:') }),
      })

      const schema = new KyselyQueueSchemaService(connection, { dialect })
      await schema.dropJobsTable(JOBS_TABLE)
      await schema.dropSchedulesTable(SCHEDULES_TABLE)
      await schema.createJobsTable(JOBS_TABLE)
      await schema.createSchedulesTable(SCHEDULES_TABLE)
      newTableColumns = Object.fromEntries(await textColumns(query, dialect))

      if (dialect === 'mysql') {
        // The columns created by KyselyQueueSchemaService in 0.7.
        await sql`alter table ${sql.table(JOBS_TABLE)} modify data text not null, modify error text null`.execute(
          connection
        )
        await sql`alter table ${sql.table(SCHEDULES_TABLE)} modify payload text not null`.execute(
          connection
        )
      }

      return async () => {
        await schema.dropJobsTable(JOBS_TABLE)
        await schema.dropSchedulesTable(SCHEDULES_TABLE)
        await connection.destroy()
      }
    })

    const migrate = () =>
      new KyselyQueueSchemaService(connection, { dialect }).migrateTextColumns({
        jobsTable: JOBS_TABLE,
        schedulesTable: SCHEDULES_TABLE,
      })
    const adapter = () =>
      new KyselyAdapter({
        connection,
        dialect,
        tableName: JOBS_TABLE,
        schedulesTableName: SCHEDULES_TABLE,
      })

    test('converts the text columns and keeps their nullability', async ({ assert }) => {
      const before = await textColumns(query, dialect)

      await migrate()
      await migrate()

      const after = await textColumns(query, dialect)
      if (dialect === 'mysql') {
        assert.deepEqual(Object.fromEntries(after), {
          data: 'longtext NO',
          error: 'longtext YES',
          payload: 'longtext NO',
        })
      } else {
        assert.deepEqual(after, before)
      }

      await assertLargePayloads(assert, adapter())
    })

    test('creates new tables with the right text type', ({ assert }) => {
      assert.deepEqual(newTableColumns, NEW_TABLE_COLUMNS(dialect))
    })

    if (dialect === 'mysql') {
      test('keeps the collation and comment of a column', async ({ assert }) => {
        await sql`alter table ${sql.table(JOBS_TABLE)} modify data text character set utf8mb4 collate utf8mb4_bin not null comment 'job payload', modify error text null comment ${'C:\\new\\table\\'}`.execute(
          connection
        )

        await migrate()

        const { rows } = await sql<{
          name: string
          type: string
          collation: string
          comment: string
        }>`
          select column_name as name, data_type as type, collation_name as collation,
            column_comment as comment
          from information_schema.columns
          where table_schema = database() and table_name = ${JOBS_TABLE}
            and column_name in ('data', 'error')
          order by column_name
        `.execute(connection)
        assert.deepEqual(
          rows.map(({ name, type, collation, comment }) => ({ name, type, collation, comment })),
          [
            { name: 'data', type: 'longtext', collation: 'utf8mb4_bin', comment: 'job payload' },
            {
              name: 'error',
              type: 'longtext',
              collation: rows[1].collation,
              comment: 'C:\\new\\table\\',
            },
          ]
        )
      })
    }

    test('skips a missing schedules table', async ({ assert }) => {
      await new KyselyQueueSchemaService(connection, { dialect }).dropSchedulesTable(
        SCHEDULES_TABLE
      )

      await migrate()

      const columns = await textColumns(query, dialect)
      assert.equal(columns.get('data'), dialect === 'mysql' ? 'longtext NO' : columns.get('data'))
    })
  })
}
