import type { Knex } from 'knex'
import type { DatabaseConnection, Dialect } from 'kysely'

type BeforeDelete = () => Promise<void>

const isDelete = (sql: string) => /^\s*delete\b/i.test(sql)

/** Makes a Knex instance await `beforeDelete` before it sends each DELETE. */
export function pauseKnexDeletes(connection: Knex, beforeDelete: BeforeDelete): Knex {
  const client = connection.client as { _query(connection: unknown, query: { sql: string }): any }
  const query = client._query.bind(client)
  client._query = async (raw, obj) => {
    if (isDelete(obj.sql)) await beforeDelete()
    return query(raw, obj)
  }
  return connection
}

/** Wraps a Kysely dialect so that it awaits `beforeDelete` before it sends each DELETE. */
export function pauseKyselyDeletes(dialect: Dialect, beforeDelete: BeforeDelete): Dialect {
  return {
    createAdapter: () => dialect.createAdapter(),
    createIntrospector: (db) => dialect.createIntrospector(db),
    createQueryCompiler: () => dialect.createQueryCompiler(),
    createDriver: () => {
      const driver = dialect.createDriver()
      const paused = new WeakSet<DatabaseConnection>()
      const acquireConnection = driver.acquireConnection.bind(driver)

      driver.acquireConnection = async () => {
        const connection = await acquireConnection()
        if (paused.has(connection)) return connection

        paused.add(connection)
        const executeQuery = connection.executeQuery.bind(connection)
        connection.executeQuery = async (query, options) => {
          if (isDelete(query.sql)) await beforeDelete()
          return executeQuery(query, options)
        }
        return connection
      }
      return driver
    },
  }
}
