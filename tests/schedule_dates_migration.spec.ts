import Database from 'better-sqlite3'
import Knex, { type Knex as KnexType } from 'knex'
import { Kysely, MysqlDialect, PostgresDialect, SqliteDialect, sql } from 'kysely'
import { Migrator } from 'kysely/migration'
import { createPool } from 'mysql2'
import { Pool } from 'pg'
import { test } from '@japa/runner'
import { KnexAdapter, KnexQueueSchemaService } from '../src/drivers/knex_adapter.js'
import { KyselyAdapter, KyselyQueueSchemaService } from '../src/drivers/kysely_adapter.js'
import type { Adapter } from '../src/contracts/adapter.js'

type Dialect = 'sqlite' | 'postgres' | 'mysql'

const TABLE = 'legacy_queue_schedules'
const WRITER_TIME_ZONE = 'Europe/Paris'

// 02:30 in Paris on 2026-10-25 happens twice (DST fall-back); 0.7 wrote the first one.
const NEXT_RUN_AT = '2026-10-25T00:30:00.000Z'
const NEXT_RUN_AT_PARIS = '2026-10-25 02:30:00'
const LAST_RUN_AT = '2026-07-01T10:00:00.000Z'
const LAST_RUN_AT_PARIS = '2026-07-01 12:00:00'
const CREATED_AT = '2026-06-01T08:00:00.000Z'
const CREATED_AT_PARIS = '2026-06-01 10:00:00'

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

function createKnex(dialect: Dialect): KnexType {
  if (dialect === 'postgres') return Knex({ client: 'pg', connection: pgConfig() })
  if (dialect === 'mysql') return Knex({ client: 'mysql2', connection: mysqlConfig() })
  return Knex({
    client: 'better-sqlite3',
    connection: { filename: ':memory:' },
    useNullAsDefault: true,
  })
}

function createKysely(dialect: Dialect): Kysely<any> {
  if (dialect === 'postgres') {
    return new Kysely({ dialect: new PostgresDialect({ pool: new Pool(pgConfig()) }) })
  }
  if (dialect === 'mysql') {
    return new Kysely({ dialect: new MysqlDialect({ pool: createPool(mysqlConfig()) }) })
  }
  return new Kysely({ dialect: new SqliteDialect({ database: new Database(':memory:') }) })
}

/**
 * Asserts the schedule written by the legacy table setup survived the migration,
 * then checks the adapter keeps working on the migrated table.
 */
async function assertMigratedSchedules(
  assert: any,
  adapter: Adapter,
  createdAt: { exact: string } | { near: number }
) {
  const legacy = await adapter.getSchedule('legacy')

  assert.equal(legacy!.name, 'LegacyJob')
  assert.equal(legacy!.nextRunAt!.toISOString(), NEXT_RUN_AT)
  assert.equal(legacy!.lastRunAt!.toISOString(), LAST_RUN_AT)
  assert.isNull(legacy!.from)
  assert.isNull(legacy!.to)
  assert.equal(legacy!.runCount, 3)

  if ('exact' in createdAt) {
    assert.equal(legacy!.createdAt.toISOString(), createdAt.exact)
  } else {
    assert.closeTo(legacy!.createdAt.getTime(), createdAt.near, 60_000)
  }

  // The adapter can create, schedule, and claim on the migrated table. The
  // legacy schedule is due from NEXT_RUN_AT on: pause it, so the claim only
  // sees the new one whatever the date.
  await adapter.updateSchedule('legacy', { status: 'paused' })
  await adapter.upsertSchedule({
    id: 'after-migration',
    name: 'NewJob',
    payload: {},
    everyMs: 60_000,
    timezone: 'UTC',
    to: new Date('2100-01-01T00:00:00.000Z'),
  })
  await adapter.updateSchedule('after-migration', { nextRunAt: new Date(Date.now() - 1_000) })

  const claimed = await adapter.claimDueSchedule()
  assert.equal(claimed!.id, 'after-migration')
  assert.equal(claimed!.to!.toISOString(), '2100-01-01T00:00:00.000Z')
}

/**
 * Asserts every schedule operation fails with the migration message on a 0.7
 * table, then that the same adapter works once the table is migrated.
 */
async function assertScheduleOperationsWaitForMigration(
  assert: any,
  adapter: Adapter,
  migrate: () => Promise<void>
) {
  const operations = {
    upsertSchedule: () =>
      adapter.upsertSchedule({
        id: 'new',
        name: 'NewJob',
        payload: {},
        everyMs: 60_000,
        timezone: 'UTC',
        nextRunAt: new Date(),
      }),
    getSchedule: () => adapter.getSchedule('legacy'),
    listSchedules: () => adapter.listSchedules(),
    updateSchedule: () => adapter.updateSchedule('legacy', { status: 'paused' }),
    deleteSchedule: () => adapter.deleteSchedule('legacy'),
    claimDueSchedule: () => adapter.claimDueSchedule(),
  }

  for (const [name, operation] of Object.entries(operations)) {
    const error = await operation().then(
      () => null,
      (reason: Error) => reason
    )
    assert.match(
      error?.message ?? '',
      /migrateScheduleDates/,
      `${name} should wait for the migration`
    )
  }

  await migrate()

  // The legacy schedule is due from NEXT_RUN_AT on: pause it, so nothing is
  // claimable whatever the date.
  assert.equal((await adapter.getSchedule('legacy'))!.status, 'active')
  await adapter.updateSchedule('legacy', { status: 'paused' })
  assert.isNull(await adapter.claimDueSchedule())
}

for (const dialect of ['sqlite', 'postgres', 'mysql'] as const) {
  test.group(`Schedule dates migration | Knex (${dialect})`, (group) => {
    let connection: KnexType

    /** Creates the schedules table KnexQueueSchemaService created in 0.7, plus a custom column. */
    async function createLegacyTable(schema: KnexType.SchemaBuilder) {
      await schema.createTable(TABLE, (table) => {
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
        table.timestamp('created_at').notNullable().defaultTo(connection.fn.now())
        table.index(['status', 'next_run_at'])
        table.string('tenant', 50).nullable()
      })
    }

    group.each.setup(async () => {
      connection = createKnex(dialect)
      await connection.schema.dropTableIfExists(TABLE)
      await createLegacyTable(connection.schema)

      return async () => {
        await connection.schema.dropTableIfExists(TABLE)
        await connection.destroy()
      }
    })

    /** Inserts the schedule the 0.7 Knex adapter would have stored. */
    async function insertLegacySchedule(tableName: string = TABLE) {
      // Exact instants on PostgreSQL (timestamptz) and SQLite (epoch ms),
      // writer-local times on MySQL. created_at is generated by the database.
      const [nextRunAt, lastRunAt] =
        dialect === 'postgres'
          ? [new Date(NEXT_RUN_AT), new Date(LAST_RUN_AT)]
          : dialect === 'sqlite'
            ? [Date.parse(NEXT_RUN_AT), Date.parse(LAST_RUN_AT)]
            : [NEXT_RUN_AT_PARIS, LAST_RUN_AT_PARIS]

      await connection(tableName).insert({
        id: 'legacy',
        name: 'LegacyJob',
        payload: '{}',
        every_ms: 60_000,
        run_count: 3,
        next_run_at: nextRunAt,
        last_run_at: lastRunAt,
        tenant: 'acme',
      })

      return { near: Date.now() }
    }

    const adapter = () => new KnexAdapter({ connection, schedulesTableName: TABLE })

    test('converts the 0.7 dates to epoch milliseconds', async ({ assert }) => {
      const createdAt = await insertLegacySchedule()

      const schema = new KnexQueueSchemaService(connection)
      await schema.migrateScheduleDates(TABLE, { timezone: WRITER_TIME_ZONE })

      const column = await connection(TABLE).columnInfo('next_run_at')
      assert.match(column.type, /int/i)
      assert.isFalse((await connection(TABLE).columnInfo('created_at')).nullable)
      assert.equal((await connection(TABLE).where('id', 'legacy').first()).tenant, 'acme')

      await assertMigratedSchedules(assert, adapter(), createdAt)
    })

    test('rejects an invalid time zone before changing the table', async ({ assert }) => {
      await insertLegacySchedule()
      const schema = new KnexQueueSchemaService(connection)

      await assert.rejects(() => schema.migrateScheduleDates(TABLE, { timezone: 'Europe/Pari' }))
      assert.notMatch((await connection(TABLE).columnInfo('next_run_at')).type, /int/i)
      assert.notProperty(await connection(TABLE).columnInfo(), 'next_run_at__epoch')
    })

    test('resumes a migration that stopped after its first schema changes', async ({ assert }) => {
      const createdAt = await insertLegacySchedule()

      // MySQL commits each schema change: a failed run can leave these behind.
      await connection.schema.alterTable(TABLE, (table) => {
        table.dropIndex(['status', 'next_run_at'])
        for (const name of ['from_date', 'to_date', 'next_run_at', 'last_run_at', 'created_at']) {
          table.bigint(`${name}__epoch`).nullable()
        }
      })

      await new KnexQueueSchemaService(connection).migrateScheduleDates(TABLE, {
        timezone: WRITER_TIME_ZONE,
      })

      // SQLite migrates in one transaction, so it never resumes from nullable epoch columns.
      if (dialect !== 'sqlite') {
        assert.isFalse((await connection(TABLE).columnInfo('created_at')).nullable)
      }
      await assertMigratedSchedules(assert, adapter(), createdAt)
    })

    if (dialect === 'mysql') {
      test('reads dates through the session time zone of the previous version', async ({
        assert,
      }) => {
        // The previous version wrote through sessions in +02:00; this migration runs in UTC.
        await connection.transaction(async (trx) => {
          await trx.raw("set time_zone = '+02:00'")
          await trx(TABLE).insert({
            id: 'legacy',
            name: 'LegacyJob',
            payload: '{}',
            every_ms: 60_000,
            run_count: 3,
            next_run_at: NEXT_RUN_AT_PARIS,
            last_run_at: LAST_RUN_AT_PARIS,
          })
          await trx.raw("set time_zone = '+00:00'")
        })

        await new KnexQueueSchemaService(connection).migrateScheduleDates(TABLE, {
          timezone: WRITER_TIME_ZONE,
          databaseTimeZone: '+02:00',
        })

        const legacy = await adapter().getSchedule('legacy')
        assert.equal(legacy!.nextRunAt!.toISOString(), NEXT_RUN_AT)
        assert.equal(legacy!.lastRunAt!.toISOString(), LAST_RUN_AT)
      })

      test('reads the MySQL zero date as no date', async ({ assert }) => {
        // Non-strict mode stores the zero date for a date it cannot store.
        await connection.transaction(async (trx) => {
          const [[current]] = await trx.raw('select @@session.sql_mode as sql_mode')
          await trx.raw("set session sql_mode = ''")
          await trx(TABLE).insert({
            id: 'legacy',
            name: 'LegacyJob',
            payload: '{}',
            every_ms: 60_000,
            next_run_at: '0000-00-00 00:00:00',
            last_run_at: 'not a date',
          })
          await trx.raw('set session sql_mode = ?', [current.sql_mode])
        })

        await new KnexQueueSchemaService(connection).migrateScheduleDates(TABLE, {
          timezone: WRITER_TIME_ZONE,
        })

        const legacy = await adapter().getSchedule('legacy')
        assert.isNull(legacy!.nextRunAt)
        assert.isNull(legacy!.lastRunAt)
      })

      test('rejects a zero created_at before changing the table', async ({ assert }) => {
        await connection.transaction(async (trx) => {
          const [[current]] = await trx.raw('select @@session.sql_mode as sql_mode')
          await trx.raw("set session sql_mode = ''")
          await trx(TABLE).insert({
            id: 'legacy',
            name: 'LegacyJob',
            payload: '{}',
            every_ms: 60_000,
            created_at: '0000-00-00 00:00:00',
          })
          await trx.raw('set session sql_mode = ?', [current.sql_mode])
        })

        await assert.rejects(
          () =>
            new KnexQueueSchemaService(connection).migrateScheduleDates(TABLE, {
              timezone: WRITER_TIME_ZONE,
            }),
          /Cannot migrate schedule "legacy": its created_at is empty/
        )
        assert.match((await connection(TABLE).columnInfo('created_at')).type, /timestamp/i)
      })
    }

    if (dialect === 'postgres') {
      test('reads dates whatever the DateStyle and TimeZone of the session', async ({
        assert,
        cleanup,
      }) => {
        const sessionConnection = Knex({
          client: 'pg',
          connection: { ...pgConfig(), options: '-c DateStyle=SQL,DMY -c TimeZone=Europe/Paris' },
        })
        cleanup(() => sessionConnection.destroy())
        const createdAt = await insertLegacySchedule()
        // Paris used a local mean time offset of +00:09:21 in 1900.
        await connection(TABLE).update({ from_date: new Date('1900-01-01T00:00:00.000Z') })

        // Inside an outer transaction, as in an AdonisJS migration, which keeps
        // the settings the migration changes until it ends.
        const settings = await sessionConnection.transaction(async (trx) => {
          await new KnexQueueSchemaService(trx).migrateScheduleDates(TABLE, {
            timezone: WRITER_TIME_ZONE,
          })
          const { rows } = await trx.raw(
            "select current_setting('DateStyle') as date_style, current_setting('TimeZone') as time_zone"
          )
          return rows[0]
        })
        assert.deepEqual(settings, { date_style: 'SQL, DMY', time_zone: 'Europe/Paris' })
        const legacy = await adapter().getSchedule('legacy')
        assert.equal(legacy!.from!.toISOString(), '1900-01-01T00:00:00.000Z')
        await connection(TABLE).update({ from_date: null })
        await assertMigratedSchedules(assert, adapter(), createdAt)
      })
    }

    if (dialect === 'postgres') {
      test('migrates a schema-qualified table', async ({ assert, cleanup }) => {
        await connection.raw('create schema if not exists review_tenant')
        cleanup(async () => {
          await connection.raw('drop schema if exists review_tenant cascade')
        })
        const qualifiedTable = `review_tenant.${TABLE}`
        await createLegacyTable(connection.schema.withSchema('review_tenant'))
        const createdAt = await insertLegacySchedule(qualifiedTable)

        await new KnexQueueSchemaService(connection).migrateScheduleDates(qualifiedTable, {
          timezone: WRITER_TIME_ZONE,
        })

        await assertMigratedSchedules(
          assert,
          new KnexAdapter({ connection, schedulesTableName: qualifiedTable }),
          createdAt
        )
        const { rows } = await connection.raw(
          "select 1 from pg_indexes where schemaname = 'review_tenant' and tablename = ? and indexdef like '%(status, next_run_at)%'",
          [TABLE]
        )
        assert.lengthOf(rows, 1)
      })

      test('keeps an index with the same name on a table of another schema', async ({
        assert,
        cleanup,
      }) => {
        // The target is public, first in the search_path comes review_tenant, which holds
        // another table with an index named like the one of the target.
        const indexName = `${TABLE}_status_next_run_at_index`
        await connection.raw('create schema if not exists review_tenant')
        await connection.raw('create table review_tenant.other (status text, next_run_at bigint)')
        await connection.raw('create index ?? on review_tenant.other (status)', [indexName])
        const searchPathConnection = Knex({
          client: 'pg',
          connection: pgConfig(),
          searchPath: ['review_tenant', 'public'],
        })
        cleanup(async () => {
          await searchPathConnection.destroy()
          await connection.raw('drop schema if exists review_tenant cascade')
        })
        const createdAt = await insertLegacySchedule()

        await new KnexQueueSchemaService(searchPathConnection).migrateScheduleDates(TABLE, {
          timezone: WRITER_TIME_ZONE,
        })

        await assertMigratedSchedules(
          assert,
          new KnexAdapter({ connection, schedulesTableName: TABLE }),
          createdAt
        )
        const { rows } = await connection.raw(
          "select 1 from pg_indexes where schemaname = 'review_tenant' and indexname = ?",
          [indexName]
        )
        assert.lengthOf(rows, 1)
      })
    }

    if (dialect === 'postgres') {
      test('migrates the table found through the search_path', async ({ assert, cleanup }) => {
        // The first schema of the search_path does not hold the table: public does.
        await connection.raw('create schema if not exists review_tenant')
        const searchPathConnection = Knex({
          client: 'pg',
          connection: pgConfig(),
          searchPath: ['review_tenant', 'public'],
        })
        cleanup(async () => {
          await searchPathConnection.destroy()
          await connection.raw('drop schema if exists review_tenant cascade')
        })
        const createdAt = await insertLegacySchedule()

        const schema = new KnexQueueSchemaService(searchPathConnection)
        assert.isTrue(await schema.needsScheduleDatesMigration(TABLE))
        await schema.migrateScheduleDates(TABLE, { timezone: WRITER_TIME_ZONE })

        await assertMigratedSchedules(
          assert,
          new KnexAdapter({ connection: searchPathConnection, schedulesTableName: TABLE }),
          createdAt
        )
      })
    }

    test('adapter.migrate() rejects when the table cannot be inspected', async ({ assert }) => {
      const destroyed = createKnex(dialect)
      await destroyed.destroy()

      await assert.rejects(() =>
        new KnexAdapter({ connection: destroyed, schedulesTableName: TABLE }).migrate()
      )
    })

    test('adapter.migrate() fails until the schedules table is migrated', async ({ assert }) => {
      await assert.rejects(() => adapter().migrate(), /migrateScheduleDates/)

      await new KnexQueueSchemaService(connection).migrateScheduleDates(TABLE, {
        timezone: WRITER_TIME_ZONE,
      })

      await adapter().migrate()
    })

    test('schedule operations fail until the schedules table is migrated', async ({ assert }) => {
      await insertLegacySchedule()

      await assertScheduleOperationsWaitForMigration(assert, adapter(), () =>
        new KnexQueueSchemaService(connection).migrateScheduleDates(TABLE, {
          timezone: WRITER_TIME_ZONE,
        })
      )
    })

    test('does nothing on a table that is already migrated', async ({ assert }) => {
      const schema = new KnexQueueSchemaService(connection)
      await schema.migrateScheduleDates(TABLE, { timezone: WRITER_TIME_ZONE })
      await schema.migrateScheduleDates(TABLE, { timezone: WRITER_TIME_ZONE })

      const column = await connection(TABLE).columnInfo('next_run_at')
      assert.match(column.type, /int/i)
    })
  })

  test.group(`Schedule dates migration | Kysely (${dialect})`, (group) => {
    let connection: Kysely<any>

    /** Creates the schedules table KyselyQueueSchemaService created in 0.7. */
    async function createLegacyTable(db: Kysely<any>) {
      await db.schema
        .createTable(TABLE)
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
      await db.schema
        .createIndex(`${TABLE}_status_next_run_idx`)
        .on(TABLE)
        .columns(['status', 'next_run_at'])
        .execute()
    }

    group.each.setup(async () => {
      connection = createKysely(dialect)
      await connection.schema.dropTable(TABLE).ifExists().execute()
      await createLegacyTable(connection)

      return async () => {
        await connection.schema.dropTable(TABLE).ifExists().execute()
        await connection.destroy()
      }
    })

    /** Inserts the schedule the 0.7 Kysely adapter would have stored. */
    async function insertLegacySchedule(db: Kysely<any> = connection) {
      // ISO strings on SQLite, writer-local times on PostgreSQL and MySQL
      // (created_at included).
      const values =
        dialect === 'sqlite'
          ? { next_run_at: NEXT_RUN_AT, last_run_at: LAST_RUN_AT, created_at: CREATED_AT }
          : {
              next_run_at: NEXT_RUN_AT_PARIS,
              last_run_at: LAST_RUN_AT_PARIS,
              created_at: CREATED_AT_PARIS,
            }

      await db
        .insertInto(TABLE)
        .values({
          id: 'legacy',
          name: 'LegacyJob',
          payload: '{}',
          every_ms: 60_000,
          run_count: 3,
          ...values,
        })
        .execute()

      return { exact: CREATED_AT }
    }

    const adapter = () => new KyselyAdapter({ connection, dialect, schedulesTableName: TABLE })
    const columnTypes = async () => {
      const table = (await connection.introspection.getTables()).find(({ name }) => name === TABLE)
      return new Map(table!.columns.map(({ name, dataType }) => [name, dataType]))
    }
    const isNullable = async (column: string) => {
      const table = (await connection.introspection.getTables()).find(({ name }) => name === TABLE)
      return table!.columns.find(({ name }) => name === column)!.isNullable
    }

    test('converts the 0.7 dates to epoch milliseconds', async ({ assert }) => {
      const createdAt = await insertLegacySchedule()

      const schema = new KyselyQueueSchemaService(connection, { dialect })
      await schema.migrateScheduleDates(TABLE, { timezone: WRITER_TIME_ZONE })

      assert.match((await columnTypes()).get('next_run_at')!, /int/i)
      assert.isFalse(await isNullable('created_at'))
      await assertMigratedSchedules(assert, adapter(), createdAt)
    })

    test('rejects an invalid time zone before changing the table', async ({ assert }) => {
      await insertLegacySchedule()
      const schema = new KyselyQueueSchemaService(connection, { dialect })

      await assert.rejects(() => schema.migrateScheduleDates(TABLE, { timezone: 'Europe/Pari' }))
      const types = await columnTypes()
      assert.notMatch(types.get('next_run_at')!, /int/i)
      assert.isFalse(types.has('next_run_at__epoch'))
    })

    test('resumes a migration that stopped after its first schema changes', async ({ assert }) => {
      const createdAt = await insertLegacySchedule()

      // MySQL commits each schema change: a failed run can leave these behind.
      const dropIndex = connection.schema.dropIndex(`${TABLE}_status_next_run_idx`)
      await (dialect === 'mysql' ? dropIndex.on(TABLE) : dropIndex).execute()
      for (const name of ['from_date', 'to_date', 'next_run_at', 'last_run_at', 'created_at']) {
        await connection.schema.alterTable(TABLE).addColumn(`${name}__epoch`, 'bigint').execute()
      }

      await new KyselyQueueSchemaService(connection, { dialect }).migrateScheduleDates(TABLE, {
        timezone: WRITER_TIME_ZONE,
      })

      // SQLite migrates in one transaction, so it never resumes from nullable epoch columns.
      if (dialect !== 'sqlite') {
        assert.isFalse(await isNullable('created_at'))
      }
      await assertMigratedSchedules(assert, adapter(), createdAt)
    })

    if (dialect === 'mysql') {
      test('reads dates through the session time zone of the previous version', async ({
        assert,
      }) => {
        // The previous version wrote through sessions in +02:00; this migration runs in UTC.
        await connection.connection().execute(async (db) => {
          await sql`set time_zone = '+02:00'`.execute(db)
          await insertLegacySchedule(db)
          await sql`set time_zone = '+00:00'`.execute(db)
        })

        await new KyselyQueueSchemaService(connection, { dialect }).migrateScheduleDates(TABLE, {
          timezone: WRITER_TIME_ZONE,
          databaseTimeZone: '+02:00',
        })

        await assertMigratedSchedules(assert, adapter(), { exact: CREATED_AT })
      })

      test('reads the MySQL zero date as no date', async ({ assert }) => {
        // Non-strict mode stores the zero date for a date it cannot store.
        await connection.connection().execute(async (db) => {
          const current = await sql<{
            sqlMode: string
          }>`select @@session.sql_mode as ${sql.ref('sqlMode')}`.execute(db)
          await sql`set session sql_mode = ''`.execute(db)
          await db
            .insertInto(TABLE)
            .values({
              id: 'legacy',
              name: 'LegacyJob',
              payload: '{}',
              every_ms: 60_000,
              next_run_at: '0000-00-00 00:00:00',
              last_run_at: 'not a date',
            })
            .execute()
          await sql`set session sql_mode = ${current.rows[0].sqlMode}`.execute(db)
        })

        await new KyselyQueueSchemaService(connection, { dialect }).migrateScheduleDates(TABLE, {
          timezone: WRITER_TIME_ZONE,
        })

        const legacy = await adapter().getSchedule('legacy')
        assert.isNull(legacy!.nextRunAt)
        assert.isNull(legacy!.lastRunAt)
      })

      test('rejects a zero created_at before changing the table', async ({ assert }) => {
        await connection.connection().execute(async (db) => {
          const current = await sql<{
            sqlMode: string
          }>`select @@session.sql_mode as ${sql.ref('sqlMode')}`.execute(db)
          await sql`set session sql_mode = ''`.execute(db)
          await db
            .insertInto(TABLE)
            .values({
              id: 'legacy',
              name: 'LegacyJob',
              payload: '{}',
              every_ms: 60_000,
              created_at: '0000-00-00 00:00:00',
            })
            .execute()
          await sql`set session sql_mode = ${current.rows[0].sqlMode}`.execute(db)
        })

        await assert.rejects(
          () =>
            new KyselyQueueSchemaService(connection, { dialect }).migrateScheduleDates(TABLE, {
              timezone: WRITER_TIME_ZONE,
            }),
          /Cannot migrate schedule "legacy": its created_at is empty/
        )
        assert.match((await columnTypes()).get('created_at')!, /timestamp/i)
      })
    }

    if (dialect === 'sqlite') {
      test('keeps a date in the year 0', async ({ assert }) => {
        // SQLite stores the text it is given; the migration reads it as UTC.
        await insertLegacySchedule()
        await connection
          .updateTable(TABLE)
          .set({ next_run_at: '0000-06-01 12:00:00' })
          .where('id', '=', 'legacy')
          .execute()

        await new KyselyQueueSchemaService(connection, { dialect }).migrateScheduleDates(TABLE, {
          timezone: WRITER_TIME_ZONE,
        })

        const legacy = await adapter().getSchedule('legacy')
        assert.equal(legacy!.nextRunAt!.toISOString(), '0000-06-01T12:00:00.000Z')
      })
    }

    if (dialect === 'postgres') {
      test('migrates the table of the targeted schema only', async ({ assert, cleanup }) => {
        // A migrated table with the same name lives in another schema, listed before `public`.
        await sql`create schema if not exists aaa_queue_other`.execute(connection)
        cleanup(async () => {
          await sql`drop schema if exists aaa_queue_other cascade`.execute(connection)
        })
        const otherTable = `aaa_queue_other.${TABLE}`
        await new KyselyQueueSchemaService(connection, { dialect }).createSchedulesTable(otherTable)
        const createdAt = await insertLegacySchedule()

        const schema = new KyselyQueueSchemaService(connection, { dialect })
        assert.isTrue(await schema.needsScheduleDatesMigration(TABLE))
        assert.isFalse(await schema.needsScheduleDatesMigration(otherTable))

        await schema.migrateScheduleDates(TABLE, { timezone: WRITER_TIME_ZONE })

        await assertMigratedSchedules(assert, adapter(), createdAt)
      })
    }

    if (dialect === 'postgres') {
      test('reads dates whatever the DateStyle of the session', async ({ assert, cleanup }) => {
        const sessionConnection = new Kysely<any>({
          dialect: new PostgresDialect({
            pool: new Pool({
              ...pgConfig(),
              options: '-c DateStyle=SQL,DMY -c TimeZone=Europe/Paris',
            }),
          }),
        })
        cleanup(() => sessionConnection.destroy())
        const createdAt = await insertLegacySchedule()

        // Inside an outer transaction, as in the Kysely Migrator, which keeps
        // the settings the migration changes until it ends.
        const settings = await sessionConnection.transaction().execute(async (trx) => {
          await new KyselyQueueSchemaService(trx, { dialect }).migrateScheduleDates(TABLE, {
            timezone: WRITER_TIME_ZONE,
          })
          const { rows } = await sql<{ date_style: string; time_zone: string }>`
            select current_setting('DateStyle') as date_style, current_setting('TimeZone') as time_zone
          `.execute(trx)
          return rows[0]
        })
        assert.deepEqual(settings, { date_style: 'SQL, DMY', time_zone: 'Europe/Paris' })
        await assertMigratedSchedules(assert, adapter(), createdAt)
      })
    }

    if (dialect === 'postgres') {
      test('migrates a schema-qualified table', async ({ assert, cleanup }) => {
        await sql`create schema if not exists review_tenant`.execute(connection)
        cleanup(async () => {
          await sql`drop schema if exists review_tenant cascade`.execute(connection)
        })
        const qualifiedTable = `review_tenant.${TABLE}`
        const tenant = connection.withSchema('review_tenant')
        await createLegacyTable(tenant)
        const createdAt = await insertLegacySchedule(tenant)

        await new KyselyQueueSchemaService(connection, { dialect }).migrateScheduleDates(
          qualifiedTable,
          { timezone: WRITER_TIME_ZONE }
        )

        await assertMigratedSchedules(
          assert,
          new KyselyAdapter({ connection, dialect, schedulesTableName: qualifiedTable }),
          createdAt
        )
        const { rows } = await sql`
          select 1 from pg_indexes
          where schemaname = 'review_tenant' and tablename = ${TABLE}
            and indexdef like '%(status, next_run_at)%'
        `.execute(connection)
        assert.lengthOf(rows, 1)
      })

      test('keeps an index with the same name on a table of another schema', async ({
        assert,
        cleanup,
      }) => {
        // The target is public, first in the search_path comes review_tenant, which holds
        // another table with an index named like the one of the target.
        const indexName = `${TABLE}_status_next_run_idx`
        await sql`create schema if not exists review_tenant`.execute(connection)
        await sql`create table review_tenant.other (status text, next_run_at bigint)`.execute(
          connection
        )
        await sql`create index ${sql.id(indexName)} on review_tenant.other (status)`.execute(
          connection
        )
        const searchPathConnection = new Kysely<any>({
          dialect: new PostgresDialect({
            pool: new Pool({ ...pgConfig(), options: '-c search_path=review_tenant,public' }),
          }),
        })
        cleanup(async () => {
          await searchPathConnection.destroy()
          await sql`drop schema if exists review_tenant cascade`.execute(connection)
        })
        const createdAt = await insertLegacySchedule()

        await new KyselyQueueSchemaService(searchPathConnection, {
          dialect,
        }).migrateScheduleDates(TABLE, { timezone: WRITER_TIME_ZONE })

        await assertMigratedSchedules(assert, adapter(), createdAt)
        const { rows } = await sql`
          select 1 from pg_indexes where schemaname = 'review_tenant' and indexname = ${indexName}
        `.execute(connection)
        assert.lengthOf(rows, 1)
      })

      test('migrates the table targeted through withSchema()', async ({ assert, cleanup }) => {
        await sql`create schema if not exists review_tenant`.execute(connection)
        cleanup(async () => {
          await sql`drop schema if exists review_tenant cascade`.execute(connection)
        })
        const tenant = connection.withSchema('review_tenant')
        await createLegacyTable(tenant)
        const createdAt = await insertLegacySchedule(tenant)

        const schema = new KyselyQueueSchemaService(tenant, { dialect })
        assert.isTrue(await schema.needsScheduleDatesMigration(TABLE))
        await schema.migrateScheduleDates(TABLE, { timezone: WRITER_TIME_ZONE })

        await assertMigratedSchedules(
          assert,
          new KyselyAdapter({ connection: tenant, dialect, schedulesTableName: TABLE }),
          createdAt
        )
        // The table of the default schema was not touched.
        assert.isTrue(
          await new KyselyQueueSchemaService(connection, { dialect }).needsScheduleDatesMigration(
            TABLE
          )
        )
      })

      test('migrates the table found through the search_path', async ({ assert, cleanup }) => {
        // The first schema of the search_path does not hold the table: public does.
        await sql`create schema if not exists review_tenant`.execute(connection)
        const searchPathConnection = new Kysely<any>({
          dialect: new PostgresDialect({
            pool: new Pool({ ...pgConfig(), options: '-c search_path=review_tenant,public' }),
          }),
        })
        cleanup(async () => {
          await searchPathConnection.destroy()
          await sql`drop schema if exists review_tenant cascade`.execute(connection)
        })
        const createdAt = await insertLegacySchedule()

        const schema = new KyselyQueueSchemaService(searchPathConnection, { dialect })
        assert.isTrue(await schema.needsScheduleDatesMigration(TABLE))
        await assert.rejects(
          () =>
            new KyselyAdapter({
              connection: searchPathConnection,
              dialect,
              schedulesTableName: TABLE,
            }).migrate(),
          /migrateScheduleDates/
        )
        await schema.migrateScheduleDates(TABLE, { timezone: WRITER_TIME_ZONE })

        await assertMigratedSchedules(
          assert,
          new KyselyAdapter({
            connection: searchPathConnection,
            dialect,
            schedulesTableName: TABLE,
          }),
          createdAt
        )
      })
    }

    test('runs inside the transaction of the Kysely Migrator', async ({ assert, cleanup }) => {
      const createdAt = await insertLegacySchedule()
      const migrator = new Migrator({
        db: connection,
        migrationTableName: 'legacy_queue_kysely_migration',
        migrationLockTableName: 'legacy_queue_kysely_migration_lock',
        provider: {
          getMigrations: async () => ({
            '0001_migrate_schedule_dates': {
              up: (db: Kysely<any>) =>
                new KyselyQueueSchemaService(db, { dialect }).migrateScheduleDates(TABLE, {
                  timezone: WRITER_TIME_ZONE,
                }),
            },
          }),
        },
      })
      cleanup(async () => {
        await connection.schema.dropTable('legacy_queue_kysely_migration').ifExists().execute()
        await connection.schema.dropTable('legacy_queue_kysely_migration_lock').ifExists().execute()
      })

      const { error } = await migrator.migrateToLatest()
      assert.isUndefined(error)

      await assertMigratedSchedules(assert, adapter(), createdAt)
    })

    test('adapter.migrate() rejects when the table cannot be inspected', async ({ assert }) => {
      const unavailable = new Kysely<any>({
        dialect: new SqliteDialect({
          database: async () => {
            throw new Error('database unavailable')
          },
        }),
      })

      await assert.rejects(
        () =>
          new KyselyAdapter({
            connection: unavailable,
            dialect,
            schedulesTableName: TABLE,
          }).migrate(),
        /database unavailable/
      )
    })

    test('adapter.migrate() fails until the schedules table is migrated', async ({ assert }) => {
      await assert.rejects(() => adapter().migrate(), /migrateScheduleDates/)

      await new KyselyQueueSchemaService(connection, { dialect }).migrateScheduleDates(TABLE, {
        timezone: WRITER_TIME_ZONE,
      })

      await adapter().migrate()
    })

    test('schedule operations fail until the schedules table is migrated', async ({ assert }) => {
      await insertLegacySchedule()

      await assertScheduleOperationsWaitForMigration(assert, adapter(), () =>
        new KyselyQueueSchemaService(connection, { dialect }).migrateScheduleDates(TABLE, {
          timezone: WRITER_TIME_ZONE,
        })
      )
    })

    test('does nothing on a table that is already migrated', async ({ assert }) => {
      const schema = new KyselyQueueSchemaService(connection, { dialect })
      await schema.migrateScheduleDates(TABLE, { timezone: WRITER_TIME_ZONE })
      await schema.migrateScheduleDates(TABLE, { timezone: WRITER_TIME_ZONE })

      assert.match((await columnTypes()).get('next_run_at')!, /int/i)
    })
  })
}
