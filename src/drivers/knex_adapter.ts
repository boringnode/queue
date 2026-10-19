import { createHash, randomUUID } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import KnexPkg from 'knex'
import type { Knex } from 'knex'
import type {
  Adapter,
  AcquiredJob,
  JobLease,
  PushResult,
  StalledJobsRecovery,
  WorkWatcher,
} from '../contracts/adapter.js'
import type {
  AdapterFactoryContext,
  DedupOutcome,
  JobData,
  JobRecord,
  JobRetention,
  JobStatus,
  ScheduleConfig,
  ScheduleData,
  ScheduleListOptions,
} from '../types/main.js'
import debug from '../debug.js'
import { consoleLogger, type Logger } from '../logger.js'
import { DEFAULT_PRIORITY, SQL_INSERT_CHUNK_SIZE, SQL_PRUNE_BATCH_SIZE } from '../constants.js'
import {
  calculateScore,
  createLeaseToken,
  epochToDate,
  isDeadlockError,
  retryOnDeadlock,
  resolveRetention,
  resolveSchedulePayload,
} from '../utils.js'

import { KnexQueueSchemaService } from '../services/knex_queue_schema.js'
import { scheduleDatesMigrationRequiredMessage } from '../services/schedule_dates.js'
import { SCHEDULE_TIMING_COLUMNS } from '../services/schedule_timing.js'
import {
  dispatchWakeMessage,
  encodeWakeMessage,
  startHeartbeat,
  subscriptionHeartbeat,
  type WakeListener,
  type WakeMessage,
} from './wake_messages.js'
import { WorkerWakeup } from './worker_wakeup.js'

export { KnexQueueSchemaService } from '../services/knex_queue_schema.js'
export type { ScheduleDatesMigrationOptions } from '../services/schedule_dates.js'
export type { TextColumnsMigrationOptions } from '../services/text_columns.js'

export interface KnexAdapterOptions {
  connection: Knex
  tableName?: string
  schedulesTableName?: string
  ownsConnection?: boolean
  /** Receives the retention prunings that fail. Defaults to the console. */
  logger?: Logger
  /**
   * PostgreSQL only. Wake workers through LISTEN/NOTIFY instead of polling.
   * Defaults to false. Enable it on every process that dispatches jobs, not
   * only on workers: a job dispatched without it wakes no worker.
   *
   * Each worker holds one more connection, and every NOTIFY reaches every
   * worker: suited to a few workers with a high concurrency. Not supported
   * behind a pooler in transaction mode, such as PgBouncer.
   */
  notifications?: boolean
}

/**
 * The part of a node-postgres client the LISTEN connection uses.
 */
interface PgListenClient {
  connect(): Promise<void>
  end(): Promise<void>
  query(sql: string, values?: unknown[]): Promise<{ rows: DbRow[] }>
  on(
    event: 'notification',
    listener: (message: { channel: string; payload?: string }) => void
  ): void
  on(event: 'error' | 'end', listener: (error?: Error) => void): void
  removeAllListeners(): void
  /** The socket of node-postgres, to destroy a connection that does not end. */
  connection?: { stream?: { destroy(): void } }
}

/**
 * The channel of a jobs table. PostgreSQL rejects a channel name over 63
 * bytes in pg_notify: a longer table name is hashed.
 */
function wakeChannelName(table: string): string {
  if (Buffer.byteLength(table) <= 63) return table
  return `queue_jobs_${createHash('sha256').update(table).digest('hex').slice(0, 40)}`
}

/**
 * End a LISTEN or probe client. A graceful end waits for the server, which a
 * stalled connection never answers: its socket is destroyed after a while.
 */
async function endListenClient(client: PgListenClient): Promise<void> {
  client.removeAllListeners()
  client.on('error', () => {})
  const timer = setTimeout(
    () => client.connection?.stream?.destroy(),
    subscriptionHeartbeat.timeout
  )
  await client.end().catch(() => {})
  clearTimeout(timer)
}

/** The message of a job of `queue` ready now, or due at `at`. */
function jobMessage(queue: string, at?: number): WakeMessage {
  return at === undefined ? { ready: [queue] } : { due: { [queue]: at } }
}

const LISTEN_RECONNECT_DELAY = 1000
const LISTEN_PROBE_TIMEOUT = 2000
const LISTEN_MAX_RECONNECT_DELAY = 30_000
/**
 * Every NOTIFY reaches every LISTEN connection of the database: outside of
 * transactions, a queue gets at most one signal per interval per adapter.
 */
const SIGNAL_INTERVAL = 50

/**
 * A watcher and the next schedule run it was told about.
 */
interface ScheduleWatcher {
  onWake: WakeListener
  at: number | null
}

/** Signals that wait for the next NOTIFY of an adapter. */
interface PendingSignals {
  ready: Set<string>
  /** The earliest due time by queue. */
  due: Map<string, number>
  sent: Promise<void>
}

/**
 * A Knex instance, or the config to create one.
 */
export type KnexConfig = Knex | Knex.Config

type DbRow = Record<string, unknown>

interface ScheduleRow extends DbRow {
  id: string
  name: string
  payload: unknown
  cron_expression: string | null
  every_ms: number | string | null
  timezone: string | null
  from_date: number | string | null
  to_date: number | string | null
  run_limit: number | string | null
  run_count: number | string | null
  next_run_at: number | string | null
  last_run_at: number | string | null
  status: string
  created_at: number | string | null
}

/**
 * Options of the `knex()` factory.
 */
export type KnexFactoryOptions = Pick<
  KnexAdapterOptions,
  'tableName' | 'schedulesTableName' | 'notifications'
>

/**
 * Create a new Knex adapter factory.
 * Accepts either a Knex instance or a Knex configuration object.
 *
 * When passing a config object, the adapter will create and manage
 * the connection lifecycle (closing it on destroy).
 *
 * When passing a Knex instance, the caller is responsible for
 * managing the connection lifecycle.
 *
 * ```ts
 * knex(config)
 * knex(config, 'custom_jobs_table')
 * knex(config, { tableName: 'custom_jobs_table', notifications: true })
 * ```
 */
export function knex(config: KnexConfig, options?: string | KnexFactoryOptions) {
  const resolved = typeof options === 'string' ? { tableName: options } : options

  return (context?: AdapterFactoryContext) => {
    const isKnexInstance = typeof config === 'function'
    const connection = isKnexInstance ? config : KnexPkg(config)
    return new KnexAdapter({
      ...resolved,
      connection,
      ownsConnection: !isKnexInstance,
      logger: context?.logger,
    })
  }
}

/**
 * Knex adapter for the queue system.
 * Stores jobs in a SQL database using Knex.
 */
export class KnexAdapter implements Adapter {
  readonly #connection: Knex
  readonly #jobsTable: string
  readonly #schedulesTable: string
  readonly #ownsConnection: boolean
  readonly #logger: Logger
  /** Set on PostgreSQL with notifications enabled: the jobs table names the channel. */
  readonly #wakeChannel?: string
  /** Signals asked for since the last NOTIFY: see #signal. */
  #pendingSignals?: PendingSignals
  /** Watchers of this adapter: see claimDueSchedule. */
  readonly #scheduleWatchers = new Set<ScheduleWatcher>()
  #workerId: string = ''
  #scheduleDatesCheck: Promise<void> | undefined

  /** Defined on PostgreSQL with notifications enabled. See `Adapter.watch`. */
  readonly watch?: (queues: readonly string[], signal?: AbortSignal) => Promise<WorkWatcher>

  constructor(config: KnexAdapterOptions) {
    this.#connection = config.connection
    this.#jobsTable = config.tableName ?? 'queue_jobs'
    this.#schedulesTable = config.schedulesTableName ?? 'queue_schedules'
    this.#ownsConnection = config.ownsConnection ?? false
    this.#logger = config.logger ?? consoleLogger

    if (this.#connection.client.dialect === 'postgresql' && config.notifications === true) {
      this.#wakeChannel = wakeChannelName(this.#jobsTable)
      this.watch = async (queues, signal) => {
        const wakeup = new WorkerWakeup()
        const close = await this.#listen(queues, (at) => wakeup.notify(at), signal)
        return {
          reset: () => wakeup.reset(),
          wait: (maxDelay, abort, cooldown) => wakeup.wait(maxDelay, abort, cooldown),
          close,
        }
      }
    }
  }

  /**
   * NOTIFY the workers. On a transaction, PostgreSQL delivers the
   * notification on commit, and drops it on rollback.
   */
  async #notify(connection: Knex, message: WakeMessage): Promise<void> {
    if (!this.#wakeChannel) return

    const query = connection.raw('select pg_notify(?, ?)', [
      this.#wakeChannel,
      encodeWakeMessage(message),
    ])

    // A failed statement aborts the transaction: let the application see it.
    if (connection.isTransaction) {
      await query
      return
    }

    // The job is stored: a lost signal only delays it until the next poll.
    try {
      await query
    } catch (error) {
      debug('failed to notify workers: %O', error)
    }
  }

  /**
   * Signal work of `queue`: ready now, or due at `at`.
   *
   * In a transaction, the NOTIFY joins it. Otherwise, the work is stored and
   * the signal is sent in the background: PostgreSQL serializes the commits
   * that NOTIFY, and delivers each NOTIFY to every LISTEN connection. An
   * adapter sends at most one NOTIFY per SIGNAL_INTERVAL, for all its queues;
   * the signals asked for meanwhile join the next one, with the earliest due
   * time of each queue. That is enough: promoting a delayed job announces the
   * next one. `destroy()` waits for them.
   */
  async #signal(queue: string, at?: number): Promise<void> {
    if (!this.#wakeChannel) return
    const ready = at === undefined || at <= Date.now()
    if (this.#connection.isTransaction) {
      return this.#notify(this.#connection, jobMessage(queue, ready ? undefined : at))
    }

    const pending = this.#pendingSignals ?? {
      ready: new Set<string>(),
      due: new Map<string, number>(),
      sent: Promise.resolve(),
    }
    if (ready) {
      pending.ready.add(queue)
    } else {
      pending.due.set(queue, Math.min(pending.due.get(queue) ?? Infinity, at!))
    }
    if (this.#pendingSignals) return

    this.#pendingSignals = pending
    pending.sent = (async () => {
      while (pending.ready.size > 0 || pending.due.size > 0) {
        const message: WakeMessage = {}
        if (pending.ready.size > 0) message.ready = [...pending.ready]
        if (pending.due.size > 0) message.due = Object.fromEntries(pending.due)
        pending.ready.clear()
        pending.due.clear()
        await this.#notify(this.#connection, message)
        await sleep(SIGNAL_INTERVAL)
      }
      this.#pendingSignals = undefined
    })()
  }

  /**
   * Insert a job row and signal it. In a transaction, the insert and the
   * NOTIFY run in one statement, so the signal costs no round trip.
   */
  async #insertJob(row: Record<string, unknown>, queue: string, at?: number): Promise<void> {
    const insert = this.#connection(this.#jobsTable).insert(row)
    if (!this.#wakeChannel || !this.#connection.isTransaction) {
      await insert
      await this.#signal(queue, at)
      return
    }

    await this.#connection.raw('with inserted as (?) select pg_notify(?, ?)', [
      insert,
      this.#wakeChannel,
      encodeWakeMessage(jobMessage(queue, at)),
    ])
  }

  /** Tell watchers the next schedule run, read in the same transaction. */
  async #notifySchedules(connection: Knex): Promise<void> {
    if (!this.#wakeChannel) return
    const at = await this.#nextScheduleAt(connection)
    if (at !== null) await this.#notify(connection, { schedule: at })
  }

  /**
   * LISTEN on a connection outside of the pool, held for the whole
   * subscription. A lost connection is replaced with a growing delay.
   *
   * The client is created from the driver of Knex rather than with
   * acquireRawConnection, which only returns it once connected: ending the
   * subscription must be able to abort a connection still in its handshake.
   */
  async #listen(
    queues: readonly string[],
    onWake: WakeListener,
    signal?: AbortSignal
  ): Promise<() => Promise<void>> {
    const channel = this.#wakeChannel!
    const knexClient = this.#connection.client
    const watcher: ScheduleWatcher = { onWake, at: null }
    let client: PgListenClient | undefined
    let closed = false
    let reconnectTimer: NodeJS.Timeout | undefined
    let reconnectDelay = LISTEN_RECONNECT_DELAY
    let listening = false
    // A client ended in its handshake never settles connect(): closing does.
    const closing = Promise.withResolvers<never>()
    closing.promise.catch(() => {})

    const release = async () => {
      const current = client
      client = undefined
      if (!current) return
      await endListenClient(current)
    }

    // A connection config provider is called again once its settings expire,
    // as the pool of Knex does, for rotated credentials for instance.
    let providedSettings: object | undefined
    let settingsExpired: (() => boolean) | null = null
    const createClient = async (): Promise<PgListenClient> => {
      const provider = knexClient.connectionConfigProvider
      if (!provider) return new knexClient.driver.Client(knexClient.connectionSettings)

      if (!providedSettings || settingsExpired?.()) {
        const { expirationChecker, ...settings } = await provider()
        providedSettings = settings
        settingsExpired = expirationChecker ?? null
      }
      return new knexClient.driver.Client(providedSettings)
    }

    // Signals sent before listening, or while disconnected, are lost. The
    // LISTEN connection reads them: the pool keeps a connection it adds.
    const announce = async (connection: PgListenClient) => {
      const read = async (query: Knex.QueryBuilder) => {
        const { sql, bindings } = query.toSQL().toNative()
        return (await connection.query(sql, [...bindings])).rows
      }
      onWake(Date.now())
      // One per queue: promoting a job only announces the next one of its queue.
      const rows = await read(
        this.#connection(this.#jobsTable)
          .whereIn('queue', [...queues])
          .where('status', 'delayed')
          .groupBy('queue')
          .min('execute_at as at')
      )
      for (const row of rows) {
        if (row.at !== null) onWake(Number(row.at))
      }
      const [next] = await read(this.#nextScheduleQuery(this.#connection))
      watcher.at = next?.at === null || next?.at === undefined ? null : Number(next.at)
      if (watcher.at !== null) onWake(watcher.at)
    }

    const onLost = (error?: Error) => {
      listening = false
      if (closed || reconnectTimer) return
      this.#logger.error({ err: error }, 'Lost the LISTEN connection, reconnecting')
      void release()
      reconnectTimer = setTimeout(() => {
        reconnectTimer = undefined
        if (closed) return
        connect().then(
          () => (reconnectDelay = LISTEN_RECONNECT_DELAY),
          (connectError) => {
            reconnectDelay = Math.min(reconnectDelay * 2, LISTEN_MAX_RECONNECT_DELAY)
            onLost(connectError)
          }
        )
      }, reconnectDelay)
    }

    // A pooler in transaction mode, such as PgBouncer, accepts LISTEN but
    // never delivers: check that a NOTIFY sent by another connection, outside
    // of the pool, arrives.
    let probe: { payload: string; received: PromiseWithResolvers<void> } | undefined
    const verify = async () => {
      probe = {
        payload: encodeWakeMessage({ probe: randomUUID() }),
        received: Promise.withResolvers(),
      }
      const { received, payload } = probe
      const sender = await createClient()
      // An error event without a listener would crash the process.
      const failed = Promise.withResolvers<never>()
      failed.promise.catch(() => {})
      sender.on('error', (error) => failed.reject(error))
      let timer: NodeJS.Timeout | undefined
      const timeout = new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () =>
            reject(
              new Error(
                'LISTEN received no notification within 2s. Behind a pooler in transaction ' +
                  'mode, such as PgBouncer, do not enable notifications'
              )
            ),
          LISTEN_PROBE_TIMEOUT
        )
      })
      // The timeout covers the connection of the sender too: it may stall.
      const exchange = (async () => {
        await sender.connect()
        await sender.query('select pg_notify($1, $2)', [channel, payload])
        await received.promise
      })()
      exchange.catch(() => {})
      try {
        await Promise.race([exchange, failed.promise, timeout, closing.promise])
      } finally {
        clearTimeout(timer)
        probe = undefined
        void endListenClient(sender)
      }
    }

    const connect = async () => {
      // Set when the caller gave up on this attempt: a late client stays unused.
      let abandoned = false
      const start = async () => {
        // A connection config provider may look settings up: it may stall too.
        const connection = await createClient()
        if (abandoned) return
        client = connection
        connection.on('error', onLost)
        connection.on('end', () => onLost())
        connection.on('notification', (message) => {
          if (message.channel !== channel || !message.payload) return
          if (message.payload === probe?.payload) return probe.received.resolve()
          dispatchWakeMessage(message.payload, queues, onWake, (at) => {
            watcher.at = at
            onWake(at)
          })
        })
        await connection.connect()
        // Announcements read unqualified tables, as the pool does.
        await knexClient.setSchemaSearchPath(connection)
        await connection.query(`LISTEN "${channel.replaceAll('"', '""')}"`)
        if (closed) return
        await verify()
        listening = true
        await announce(connection)
      }
      // A connection may stall before the heartbeat watches it: give up on
      // it, so that the caller falls back to polling or reconnects.
      let timer: NodeJS.Timeout | undefined
      const stalled = new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new Error('The LISTEN connection did not start in time')),
          LISTEN_PROBE_TIMEOUT + subscriptionHeartbeat.timeout
        )
      })
      const starting = start()
      starting.catch(() => {})
      try {
        await Promise.race([starting, stalled, closing.promise])
      } catch (error) {
        abandoned = true
        throw error
      } finally {
        clearTimeout(timer)
      }
      if (closed) return release()
    }
    const stopHeartbeat = startHeartbeat(
      () => (listening ? client?.query('select 1') : undefined),
      () => onLost(new Error('The LISTEN connection did not answer a ping'))
    )

    signal?.throwIfAborted()
    this.#scheduleWatchers.add(watcher)
    const close = async () => {
      closed = true
      closing.reject(new Error('The LISTEN subscription is closed'))
      stopHeartbeat()
      clearTimeout(reconnectTimer)
      signal?.removeEventListener('abort', abort)
      this.#scheduleWatchers.delete(watcher)
      await release()
    }
    // Ends the first connection, even in its handshake.
    const abort = () => void close()
    signal?.addEventListener('abort', abort, { once: true })

    try {
      await connect()
      signal?.throwIfAborted()
    } catch (error) {
      // The connection may have failed with an error event: no reconnection.
      await close()
      throw error
    }

    return close
  }

  async #announceNextSchedule(watcher: ScheduleWatcher, onlyFuture = false): Promise<void> {
    watcher.at = await this.#nextScheduleAt()
    if (watcher.at === null) return
    if (onlyFuture && watcher.at <= Date.now()) return
    watcher.onWake(watcher.at)
  }

  /**
   * Next run of a schedule that claimDueSchedule can claim: a schedule past
   * its run limit or end date stays active but never runs again.
   */
  async #nextScheduleAt(connection: Knex = this.#connection): Promise<number | null> {
    const row = await this.#nextScheduleQuery(connection)
    return row?.at === null || row?.at === undefined ? null : Number(row.at)
  }

  #nextScheduleQuery(connection: Knex) {
    return connection(this.#schedulesTable)
      .where('status', 'active')
      .whereNotNull('next_run_at')
      .where((builder) => {
        builder.whereNull('run_limit').orWhereRaw('run_count < run_limit')
      })
      .where((builder) => {
        builder
          .whereNull('to_date')
          .orWhere((range) =>
            range
              .where('to_date', '>=', Date.now())
              .whereRaw('?? >= ??', ['to_date', 'next_run_at'])
          )
      })
      .min('next_run_at as at')
      .first()
  }

  setWorkerId(workerId: string): void {
    this.#workerId = workerId
  }

  async destroy(): Promise<void> {
    await this.#pendingSignals?.sent
    if (this.#ownsConnection) {
      await this.#connection.destroy()
    }
  }

  /**
   * Schema changes of SQL tables belong to the application's migrations, run
   * through KnexQueueSchemaService. This only fails fast when the schedules
   * table still needs one.
   */
  async migrate(): Promise<void> {
    const schema = new KnexQueueSchemaService(this.#connection)

    if (await schema.needsScheduleDatesMigration(this.#schedulesTable)) {
      throw new Error(
        scheduleDatesMigrationRequiredMessage(this.#schedulesTable, 'KnexQueueSchemaService')
      )
    }
  }

  /**
   * Runs the migrate() check before the first schedule operation. The 0.8
   * queries fail on the date columns of a table not migrated yet on
   * PostgreSQL, and misread them on MySQL and SQLite. A failed check runs
   * again on the next operation, so a migration run meanwhile is picked up.
   */
  #assertScheduleDatesMigrated(): Promise<void> {
    this.#scheduleDatesCheck ??= this.migrate().catch((error) => {
      this.#scheduleDatesCheck = undefined
      throw error
    })
    return this.#scheduleDatesCheck
  }

  async pop(): Promise<AcquiredJob | null> {
    return this.popFrom('default')
  }

  async popFrom(queue: string): Promise<AcquiredJob | null> {
    const now = Date.now()

    // First, move ready delayed jobs to pending
    await this.#processDelayedJobs(queue, now)

    // Use a transaction to atomically pop a job
    return this.#claimTransaction(this.#jobsTable, async (trx) => {
      // Build the query for highest priority job (lowest score)
      let query = trx(this.#jobsTable)
        .where('queue', queue)
        .where('status', 'pending')
        .orderBy('score', 'asc')

      if (this.#supportsSkipLocked()) {
        query = query.forUpdate().skipLocked()
      }

      const job = await query.first()

      if (!job) {
        return null
      }

      // Update job to active status
      // For SQLite (no SKIP LOCKED), add status='pending' guard to prevent double-claim
      const updateQuery = trx(this.#jobsTable).where('id', job.id).where('queue', queue)

      if (!this.#supportsSkipLocked()) {
        updateQuery.where('status', 'pending')
      }

      // worker_id holds the lease token of the acquisition.
      const leaseToken = createLeaseToken(this.#workerId)
      const updated = await updateQuery.update({
        status: 'active',
        worker_id: leaseToken,
        acquired_at: now,
      })

      // Another worker already claimed this job
      if (updated === 0) {
        return null
      }

      const jobData: JobData = JSON.parse(job.data)

      return {
        ...jobData,
        acquiredAt: now,
        leaseToken,
      }
    })
  }

  /**
   * Check if the database supports FOR UPDATE SKIP LOCKED.
   * PostgreSQL 9.5+, MySQL 8.0+, and MariaDB 10.6+ support it.
   * SQLite does not, but it's single-writer so it doesn't need it.
   */
  #supportsSkipLocked(): boolean {
    const client = this.#connection.client.config.client
    return client === 'pg' || client === 'mysql' || client === 'mysql2' || client === 'mariadb'
  }

  #isMysql(): boolean {
    const client = this.#connection.client.config.client
    return client === 'mysql' || client === 'mysql2' || client === 'mariadb'
  }

  /**
   * Transaction for a SELECT ... FOR UPDATE SKIP LOCKED claim followed by an
   * UPDATE of the claimed rows.
   *
   * Under the MySQL default, REPEATABLE READ, the locking read also locks the
   * gap before every index entry it scans, skipped rows included. The claim
   * UPDATE then moves the row to another status, which inserts its new index
   * entry into a gap that a concurrent claim holds: two workers acquiring jobs
   * wait on each other and InnoDB aborts one with ER_LOCK_DEADLOCK. READ
   * COMMITTED only locks the claimed rows, as PostgreSQL does by default.
   */
  #claimTransaction<T>(table: string, callback: (trx: Knex.Transaction) => Promise<T>): Promise<T> {
    if (!this.#isMysql()) return this.#writeTransaction(table, callback)
    return this.#writeTransaction(table, callback, { isolationLevel: 'read committed' })
  }

  /**
   * Transaction that reads rows of `table`, then writes.
   *
   * Knex starts a SQLite transaction with a deferred BEGIN, which takes the
   * write lock at the first write. When another connection, such as a
   * dispatcher process, writes between the read and that write, SQLite fails
   * the transaction with SQLITE_BUSY at once, without waiting for the busy
   * timeout. Writing first takes the write lock up front, as BEGIN IMMEDIATE
   * does: the transaction waits for the busy timeout instead.
   */
  #writeTransaction<T>(
    table: string,
    callback: (trx: Knex.Transaction) => Promise<T>,
    config?: Knex.TransactionConfig
  ): Promise<T> {
    return this.#connection.transaction(async (trx) => {
      await this.#lockForWrite(trx, table)
      return callback(trx)
    }, config)
  }

  /** Take the SQLite write lock up front: see `#writeTransaction()`. */
  async #lockForWrite(trx: Knex.Transaction, table: string): Promise<void> {
    if (this.#isSqlite()) await trx.raw('update ?? set id = id where 0', [table])
  }

  #isSqlite(): boolean {
    const client = String(this.#connection.client.config.client)
    return client.includes('sqlite')
  }

  /**
   * Move the due delayed jobs of a queue to pending. On MySQL, the updates of
   * two workers promoting at once can deadlock. The promotion is rolled back
   * and changes nothing: the jobs stay delayed and the next poll promotes
   * them, so the claim goes on instead of failing the worker cycle.
   */
  async #processDelayedJobs(queue: string, now: number): Promise<void> {
    try {
      await this.#promoteDelayedJobs(queue, now)
    } catch (error) {
      if (!isDeadlockError(error)) throw error
      debug('delayed job promotion on queue %s deadlocked, retrying on the next poll', queue)
    }
  }

  async #promoteDelayedJobs(queue: string, now: number): Promise<void> {
    // Use a transaction with row locking to prevent race conditions
    await this.#claimTransaction(this.#jobsTable, async (trx) => {
      let query = trx(this.#jobsTable)
        .where('queue', queue)
        .where('status', 'delayed')
        .where('execute_at', '<=', now)
        .select('id', 'data')

      if (this.#supportsSkipLocked()) {
        query = query.forUpdate().skipLocked()
      }

      const delayedJobs = await query

      if (delayedJobs.length === 0) return

      // Move them to pending
      for (const job of delayedJobs) {
        const jobData: JobData = JSON.parse(job.data)
        const priority = jobData.priority ?? DEFAULT_PRIORITY
        const score = calculateScore(priority, now)

        await trx(this.#jobsTable).where('id', job.id).where('queue', queue).update({
          status: 'pending',
          score,
          execute_at: null,
        })
      }

      // Workers only know the next delayed job: announce the one after it.
      // Due rows left are being promoted by concurrent transactions.
      if (this.#wakeChannel) {
        const next = await trx(this.#jobsTable)
          .where('queue', queue)
          .where('status', 'delayed')
          .where('execute_at', '>', now)
          .min('execute_at as at')
          .first()
        if (next?.at !== null && next?.at !== undefined) {
          await this.#notify(trx, jobMessage(queue, Number(next.at)))
        }
      }
    })
  }

  async completeJob(
    job: JobLease,
    queue: string,
    removeOnComplete?: JobRetention
  ): Promise<boolean> {
    const { keep, maxAge, maxCount } = resolveRetention(removeOnComplete)

    if (!keep) {
      const deleted = await this.#leasedJob(job, queue).delete()
      return deleted > 0
    }

    const now = Date.now()

    const updated = await this.#leasedJob(job, queue).update({
      status: 'completed',
      worker_id: null,
      acquired_at: null,
      finished_at: now,
    })

    if (!updated) {
      return false
    }

    await this.#pruneHistoryAfterFinalization(queue, 'completed', maxAge, maxCount, now)
    return true
  }

  async failJob(
    job: JobLease,
    queue: string,
    error?: Error,
    removeOnFail?: JobRetention
  ): Promise<boolean> {
    const { keep, maxAge, maxCount } = resolveRetention(removeOnFail)

    if (!keep) {
      const deleted = await this.#leasedJob(job, queue).delete()
      return deleted > 0
    }

    const now = Date.now()

    const updated = await this.#leasedJob(job, queue).update({
      status: 'failed',
      worker_id: null,
      acquired_at: null,
      finished_at: now,
      error: error?.message || null,
    })

    if (!updated) {
      return false
    }

    await this.#pruneHistoryAfterFinalization(queue, 'failed', maxAge, maxCount, now)
    return true
  }

  /**
   * Query on a job still active under the given lease token.
   */
  #leasedJob(job: JobLease, queue: string) {
    return this.#connection(this.#jobsTable)
      .where('id', job.id)
      .where('queue', queue)
      .where('status', 'active')
      .where('worker_id', job.leaseToken)
  }

  async getJob(jobId: string, queue: string): Promise<JobRecord | null> {
    const row = await this.#connection(this.#jobsTable)
      .where('id', jobId)
      .where('queue', queue)
      .first()

    if (!row) {
      return null
    }

    const jobData: JobData = JSON.parse(row.data)

    return {
      status: row.status as JobStatus,
      data: jobData,
      finishedAt: row.finished_at ? Number(row.finished_at) : undefined,
      error: row.error || undefined,
    }
  }

  /**
   * Prune the history of a queue after a finalization, which is committed
   * already: a failed pruning must neither undo it nor fail the call. A
   * deadlock runs the pruning again, a bounded number of times. Another
   * error is logged, and the next finalization on the queue prunes again.
   *
   * On a transaction of the application, nothing is committed yet, and a
   * failed statement can abort the whole transaction: the error goes to the
   * caller, without a retry.
   */
  async #pruneHistoryAfterFinalization(
    queue: string,
    status: 'completed' | 'failed',
    maxAge: number,
    maxCount: number,
    now: number
  ): Promise<void> {
    if (this.#connection.isTransaction) {
      return this.#pruneHistory(queue, status, maxAge, maxCount, now)
    }

    try {
      await retryOnDeadlock(() => this.#pruneHistory(queue, status, maxAge, maxCount, now))
    } catch (error) {
      this.#logger.error(
        { err: error as Error, queue, status },
        `Failed to prune the ${status} jobs of queue "${queue}"`
      )
    }
  }

  async #pruneHistory(
    queue: string,
    status: 'completed' | 'failed',
    maxAge: number,
    maxCount: number,
    now: number
  ): Promise<void> {
    if (maxAge > 0) {
      const expired = await this.#connection(this.#jobsTable)
        .where('queue', queue)
        .where('status', status)
        .where('finished_at', '<', now - maxAge)
        .limit(SQL_PRUNE_BATCH_SIZE)
        .pluck('id')
      await this.#deleteFinished(queue, status, expired)
    }

    if (maxCount > 0) {
      // Finished jobs only leave the history by a pruning, and newer ones
      // only rank above: the jobs past the newest `maxCount` stay past them.
      const surplus = await this.#connection(this.#jobsTable)
        .where('queue', queue)
        .where('status', status)
        .orderBy([
          { column: 'finished_at', order: 'desc' },
          { column: 'id', order: 'desc' },
        ])
        .offset(maxCount)
        .limit(SQL_PRUNE_BATCH_SIZE)
        .pluck('id')
      await this.#deleteFinished(queue, status, surplus)
    }
  }

  /**
   * Delete finished jobs found by a plain read, by primary key. Unlike a
   * DELETE that scans the history, it locks no range of the status index, so
   * it does not deadlock with the finalizations of other workers on MySQL.
   */
  async #deleteFinished(queue: string, status: 'completed' | 'failed', ids: string[]) {
    if (ids.length === 0) return

    await this.#connection(this.#jobsTable)
      .whereIn('id', ids)
      .where('queue', queue)
      .where('status', status)
      .delete()
  }

  async retryJob(job: JobLease, queue: string, retryAt?: Date): Promise<boolean> {
    const now = Date.now()

    const activeJob = await this.#leasedJob(job, queue).first()

    if (!activeJob) return false

    const jobData: JobData = JSON.parse(activeJob.data)
    jobData.attempts = (jobData.attempts || 0) + 1

    const updatedData = JSON.stringify(jobData)

    // The update checks the lease again: it may have been lost since the read.
    if (retryAt && retryAt.getTime() > now) {
      // Move to delayed
      const updated = await this.#leasedJob(job, queue).update({
        status: 'delayed',
        data: updatedData,
        worker_id: null,
        acquired_at: null,
        score: null,
        execute_at: retryAt.getTime(),
      })
      if (updated > 0) await this.#signal(queue, retryAt.getTime())
      return updated > 0
    }

    // Move back to pending
    const priority = jobData.priority ?? DEFAULT_PRIORITY
    const score = calculateScore(priority, now)

    const updated = await this.#leasedJob(job, queue).update({
      status: 'pending',
      data: updatedData,
      worker_id: null,
      acquired_at: null,
      score,
      execute_at: null,
    })
    if (updated > 0) await this.#signal(queue)
    return updated > 0
  }

  async push(jobData: JobData): Promise<PushResult | void> {
    return this.pushOn('default', jobData)
  }

  async pushOn(queue: string, jobData: JobData): Promise<PushResult | void> {
    const priority = jobData.priority ?? DEFAULT_PRIORITY
    const timestamp = Date.now()
    const score = calculateScore(priority, timestamp)

    if (jobData.dedup) {
      const result = await this.#pushWithDedup(queue, jobData, {
        id: jobData.id,
        queue,
        status: 'pending',
        data: JSON.stringify(jobData),
        score,
      })
      // Other outcomes leave the existing job as it was.
      if (result.outcome === 'added') await this.#signal(queue)
      return result
    }

    await this.#insertJob(
      { id: jobData.id, queue, status: 'pending', data: JSON.stringify(jobData), score },
      queue
    )
  }

  async pushLater(jobData: JobData, delay: number): Promise<PushResult | void> {
    return this.pushLaterOn('default', jobData, delay)
  }

  async pushLaterOn(queue: string, jobData: JobData, delay: number): Promise<PushResult | void> {
    const executeAt = Date.now() + delay

    if (jobData.dedup) {
      const result = await this.#pushWithDedup(queue, jobData, {
        id: jobData.id,
        queue,
        status: 'delayed',
        data: JSON.stringify(jobData),
        execute_at: executeAt,
      })
      // A due time no job has would wake watchers in vain, and could hide
      // the one of another job signalled with it.
      if (result.outcome === 'added') await this.#signal(queue, executeAt)
      return result
    }

    await this.#insertJob(
      {
        id: jobData.id,
        queue,
        status: 'delayed',
        data: JSON.stringify(jobData),
        execute_at: executeAt,
      },
      queue,
      executeAt
    )
  }

  async #pushWithDedup(
    queue: string,
    jobData: JobData,
    insertRow: Record<string, unknown>
  ): Promise<PushResult> {
    const dedup = jobData.dedup!

    try {
      return await this.#pushWithDedupTxn(queue, jobData, insertRow, dedup)
    } catch (err) {
      if (this.#isMissingDedupColumn(err)) {
        throw new Error(
          `Dedup columns missing on "${this.#jobsTable}". Run KnexQueueSchemaService.addDedupColumns() on your jobs table before dispatching jobs with .dedup().`,
          { cause: err }
        )
      }
      throw err
    }
  }

  #isMissingDedupColumn(err: unknown): boolean {
    if (!err || typeof err !== 'object') return false
    const message = (err as { message?: string }).message
    if (!message) return false
    // Postgres: 'column "dedup_id" does not exist'
    // SQLite: 'no such column: dedup_id'
    // MySQL: "Unknown column 'dedup_id' in 'where clause'"
    return (
      /dedup_id/.test(message) && /(does not exist|no such column|Unknown column)/i.test(message)
    )
  }

  /**
   * A single job owns a dedup slot, whatever its status: the unique
   * (queue, dedup_id) index rejects a second owner. An existing owner is
   * locked for the transaction. When there is none to lock and a concurrent
   * dispatch inserts first, the transaction runs again and locks that owner.
   *
   * On a transaction of the application, the dispatch joins it and runs
   * once: an error, such as the unique violation of a concurrent dispatch,
   * goes to the application, which rolls it back.
   */
  async #pushWithDedupTxn(
    queue: string,
    jobData: JobData,
    insertRow: Record<string, unknown>,
    dedup: NonNullable<JobData['dedup']>
  ): Promise<PushResult> {
    const dispatch = async (trx: Knex.Transaction): Promise<PushResult> => {
      const now = Date.now()
      const existingResult = await this.#resolveExistingDedup(trx, queue, jobData, dedup, now)
      if (existingResult) return existingResult

      await trx(this.#jobsTable).insert({
        ...insertRow,
        dedup_id: dedup.id,
        dedup_at: now,
        dedup_ttl: dedup.ttl ?? null,
      })
      return { outcome: 'added' as DedupOutcome, jobId: jobData.id }
    }

    if (this.#connection.isTransaction) {
      const trx = this.#connection as Knex.Transaction
      await this.#lockForWrite(trx, this.#jobsTable)
      return dispatch(trx)
    }

    while (true) {
      try {
        return await this.#writeTransaction(this.#jobsTable, dispatch)
      } catch (err) {
        if (!this.#isUniqueViolation(err) && !this.#isDeadlock(err)) throw err
        // The job id itself is the duplicate
        if (await this.getJob(jobData.id, queue)) throw err
      }
    }
  }

  async #resolveExistingDedup(
    trx: Knex.Transaction,
    queue: string,
    jobData: JobData,
    dedup: NonNullable<JobData['dedup']>,
    now: number
  ): Promise<PushResult | null> {
    const owner = await trx(this.#jobsTable)
      .where('queue', queue)
      .where('dedup_id', dedup.id)
      .orderBy('dedup_at', 'desc')
      .first('id', 'dedup_id')

    if (!owner) return null

    // The owner is locked through its primary key, as workers lock a job.
    // Locking it through the dedup index deadlocks with them on MySQL.
    const existing = await trx(this.#jobsTable).where({ id: owner.id, queue }).forUpdate().first()

    // Compare stored values: SQL collation may equate different input strings.
    // Removed or released in the meantime: the insert tells whether the slot is free.
    if (!existing || existing.dedup_id !== owner.dedup_id) return null

    const dedupAt = existing.dedup_at != null ? Number(existing.dedup_at) : null
    const dedupTtl = existing.dedup_ttl != null ? Number(existing.dedup_ttl) : null
    const withinTtl = dedupTtl === null || (dedupAt !== null && now - dedupAt < dedupTtl)

    if (withinTtl) {
      const status = existing.status as JobStatus
      const replaceable = status === 'pending' || status === 'delayed'

      if (dedup.replace && replaceable) {
        const storedData =
          typeof existing.data === 'string' ? JSON.parse(existing.data) : existing.data
        const newData = { ...storedData, payload: jobData.payload }
        const updates: Record<string, unknown> = { data: JSON.stringify(newData) }
        if (dedup.extend && dedupTtl) {
          updates.dedup_at = now
        }
        await trx(this.#jobsTable).where({ id: existing.id, queue }).update(updates)
        return { outcome: 'replaced' as DedupOutcome, jobId: existing.id as string }
      }

      if (dedup.extend && dedupTtl) {
        await trx(this.#jobsTable).where({ id: existing.id, queue }).update({ dedup_at: now })
        return { outcome: 'extended' as DedupOutcome, jobId: existing.id as string }
      }

      return { outcome: 'skipped' as DedupOutcome, jobId: existing.id as string }
    }

    // Release the expired dedup slot. The old job keeps running to completion.
    await trx(this.#jobsTable)
      .where({ id: existing.id, queue })
      .update({ dedup_id: null, dedup_at: null, dedup_ttl: null })

    return null
  }

  #isUniqueViolation(err: unknown): boolean {
    if (!err || typeof err !== 'object') return false
    const e = err as { code?: string; message?: string }
    return (
      e.code === '23505' ||
      e.code === 'SQLITE_CONSTRAINT_UNIQUE' ||
      e.code === 'ER_DUP_ENTRY' ||
      /UNIQUE constraint/i.test(e.message ?? '')
    )
  }

  /**
   * InnoDB picks a victim among dispatches inserting the same dedup id.
   */
  #isDeadlock(err: unknown): boolean {
    return (err as { code?: string } | null)?.code === 'ER_LOCK_DEADLOCK'
  }

  async pushMany(jobs: JobData[]): Promise<void> {
    return this.pushManyOn('default', jobs)
  }

  async pushManyOn(queue: string, jobs: JobData[]): Promise<void> {
    if (jobs.length === 0) return

    if (jobs.some((j) => j.dedup)) {
      throw new Error('dedup is not supported in batch dispatch; use single dispatch')
    }

    const now = Date.now()
    const rows = jobs.map((job) => ({
      id: job.id,
      queue,
      status: 'pending' as const,
      data: JSON.stringify(job),
      score: calculateScore(job.priority ?? DEFAULT_PRIORITY, now),
    }))

    // Inserts in chunks within one transaction: all jobs or none. On a
    // transaction of the application, the chunks join it: after a failure,
    // the application rolls it back.
    const insertChunks = async (connection: Knex) => {
      for (let start = 0; start < rows.length; start += SQL_INSERT_CHUNK_SIZE) {
        await connection(this.#jobsTable).insert(rows.slice(start, start + SQL_INSERT_CHUNK_SIZE))
      }
      await this.#notify(connection, jobMessage(queue))
    }

    if (this.#connection.isTransaction) {
      await insertChunks(this.#connection)
    } else {
      await this.#connection.transaction(insertChunks)
    }
  }

  async size(): Promise<number> {
    return this.sizeOf('default')
  }

  async sizeOf(queue: string): Promise<number> {
    const result = await this.#connection(this.#jobsTable)
      .where('queue', queue)
      .where('status', 'pending')
      .count('* as count')
      .first()

    return Number(result?.count ?? 0)
  }

  async recoverStalledJobs(
    queue: string,
    stalledThreshold: number,
    maxStalledCount: number,
    maxExceeded: number
  ): Promise<StalledJobsRecovery> {
    const now = Date.now()
    const stalledCutoff = now - stalledThreshold

    // Use a transaction with row locking to prevent race conditions
    return this.#claimTransaction(this.#jobsTable, async (trx) => {
      let recovered = 0
      const exceeded: AcquiredJob[] = []

      let query = trx(this.#jobsTable)
        .where('queue', queue)
        .where('status', 'active')
        .where('acquired_at', '<', stalledCutoff)
        .select('id', 'data')

      if (this.#supportsSkipLocked()) {
        query = query.forUpdate().skipLocked()
      }

      const stalledJobs = await query

      for (const row of stalledJobs) {
        const jobData: JobData = JSON.parse(row.data)
        const currentStalledCount = jobData.stalledCount ?? 0

        if (currentStalledCount >= maxStalledCount) {
          // Past maxExceeded, the job stays stalled for a later recovery.
          if (exceeded.length >= maxExceeded) continue

          // Reacquire for this worker, which fails it through the regular path.
          const leaseToken = createLeaseToken(this.#workerId)
          const updated = await trx(this.#jobsTable)
            .where('id', row.id)
            .where('queue', queue)
            .where('status', 'active')
            .update({ worker_id: leaseToken, acquired_at: now })

          if (updated > 0) exceeded.push({ ...jobData, acquiredAt: now, leaseToken })
        } else {
          // Recover: increment stalledCount and put back in pending
          jobData.stalledCount = currentStalledCount + 1
          const priority = jobData.priority ?? DEFAULT_PRIORITY
          const score = calculateScore(priority, now)

          await trx(this.#jobsTable)
            .where('id', row.id)
            .where('queue', queue)
            .update({
              status: 'pending',
              data: JSON.stringify(jobData),
              worker_id: null,
              acquired_at: null,
              score,
            })

          recovered++
        }
      }

      if (recovered > 0) await this.#notify(trx, jobMessage(queue))
      return { recovered, exceeded }
    })
  }

  async renewJobs(queue: string, jobs: JobLease[]): Promise<number> {
    if (jobs.length === 0) {
      return 0
    }

    const now = Date.now()

    // Only renew jobs that are still active under their lease token; a job
    // that was already recovered, finalized, or acquired again will not match
    // and is therefore never resurrected. A token belongs to one acquisition of
    // one job, so matching ids and tokens separately cannot mix up two jobs.
    const renewed = await this.#connection(this.#jobsTable)
      .where('queue', queue)
      .where('status', 'active')
      .whereIn(
        'id',
        jobs.map((job) => job.id)
      )
      .whereIn(
        'worker_id',
        jobs.map((job) => job.leaseToken)
      )
      .update({ acquired_at: now })

    return renewed
  }

  async upsertSchedule(config: ScheduleConfig): Promise<string> {
    await this.#assertScheduleDatesMigrated()

    const id = config.id ?? randomUUID()
    const nextRunAt = config.nextRunAt?.getTime() ?? null

    const definition = {
      name: config.name,
      payload: JSON.stringify(resolveSchedulePayload(config.payload)),
      cron_expression: config.cronExpression ?? null,
      every_ms: config.everyMs ?? null,
      timezone: config.timezone,
      from_date: config.from?.getTime() ?? null,
      to_date: config.to?.getTime() ?? null,
      run_limit: config.limit ?? null,
    }

    // One statement creates or updates the row. It takes the row lock (the
    // write lock on SQLite) itself, so concurrent upserts and claims apply one
    // after the other: a transaction reading the row first can deadlock on
    // MySQL, and fail with SQLITE_BUSY on SQLite.
    const column = (name: string) => `${this.#schedulesTable}.${name}`
    const nullSafeEquals = this.#isMysql()
      ? '<=>'
      : this.#isSqlite()
        ? 'is'
        : 'is not distinct from'
    const sameTiming = SCHEDULE_TIMING_COLUMNS.map(() => `?? ${nullSafeEquals} ?`).join(' and ')

    await this.#connection(this.#schedulesTable)
      .insert({
        ...definition,
        id,
        status: 'active',
        next_run_at: nextRunAt,
        run_count: 0,
        created_at: Date.now(),
      })
      .onConflict('id')
      .merge({
        // The status, run metadata, and next run belong to the running
        // schedule; a new timing takes this next run. First, since MySQL
        // assigns in order and the timing columns must hold stored values.
        next_run_at: this.#connection.raw(`case when ${sameTiming} then ?? else ? end`, [
          ...SCHEDULE_TIMING_COLUMNS.flatMap((name) => [column(name), definition[name]]),
          column('next_run_at'),
          nextRunAt,
        ]),
        ...definition,
      })
    await this.#notifySchedules(this.#connection)

    return id
  }

  /**
   * @deprecated Use `upsertSchedule` instead.
   */
  createSchedule(config: ScheduleConfig): Promise<string> {
    return this.upsertSchedule(config)
  }

  async getSchedule(id: string): Promise<ScheduleData | null> {
    await this.#assertScheduleDatesMigrated()

    const row = (await this.#connection(this.#schedulesTable).where('id', id).first()) as
      | ScheduleRow
      | undefined
    if (!row) return null

    return this.#rowToScheduleData(row)
  }

  async listSchedules(options?: ScheduleListOptions): Promise<ScheduleData[]> {
    await this.#assertScheduleDatesMigrated()

    let query = this.#connection(this.#schedulesTable).whereNot('status', 'cancelled')

    if (options?.status) {
      query = query.where('status', options.status)
    }

    const rows = (await query) as ScheduleRow[]
    return rows.map((row) => this.#rowToScheduleData(row))
  }

  async updateSchedule(
    id: string,
    updates: Partial<Pick<ScheduleData, 'status' | 'nextRunAt' | 'lastRunAt' | 'runCount'>>
  ): Promise<void> {
    await this.#assertScheduleDatesMigrated()

    const data: Record<string, unknown> = {}

    if (updates.status !== undefined) data.status = updates.status
    if (updates.nextRunAt !== undefined) data.next_run_at = updates.nextRunAt?.getTime() ?? null
    if (updates.lastRunAt !== undefined) data.last_run_at = updates.lastRunAt?.getTime() ?? null
    if (updates.runCount !== undefined) data.run_count = updates.runCount

    if (Object.keys(data).length > 0) {
      await this.#connection(this.#schedulesTable).where('id', id).update(data)
      await this.#notifySchedules(this.#connection)
    }
  }

  async deleteSchedule(id: string): Promise<void> {
    await this.#assertScheduleDatesMigrated()

    await this.#connection(this.#schedulesTable).where('id', id).delete()
    await this.#notifySchedules(this.#connection)
  }

  async claimDueSchedule(): Promise<ScheduleData | null> {
    await this.#assertScheduleDatesMigrated()

    const now = Date.now()

    const claimed = await this.#claimTransaction(this.#schedulesTable, async (trx) => {
      // Find one due schedule with row locking
      let query = trx(this.#schedulesTable)
        .where('status', 'active')
        .whereNotNull('next_run_at')
        .where('next_run_at', '<=', now)
        .where((builder) => {
          builder.whereNull('run_limit').orWhereRaw('run_count < run_limit')
        })
        .where((builder) => {
          builder.whereNull('to_date').orWhere('to_date', '>=', now)
        })
        .orderBy('next_run_at', 'asc')
        .limit(1)

      if (this.#supportsSkipLocked()) {
        query = query.forUpdate().skipLocked()
      }

      const row = (await query.first()) as ScheduleRow | undefined
      if (!row) return null

      // Calculate next run time
      let nextRunAt: number | null = null
      const newRunCount = Number(row.run_count ?? 0) + 1

      if (row.every_ms) {
        nextRunAt = now + Number(row.every_ms)
      } else if (row.cron_expression) {
        // Import cron-parser dynamically to calculate next run
        const { CronExpressionParser } = await import('cron-parser')
        const cron = CronExpressionParser.parse(row.cron_expression, {
          currentDate: new Date(now),
          tz: row.timezone || 'UTC',
        })
        nextRunAt = cron.next().getTime()
      }

      // Check if limit will be reached
      if (row.run_limit !== null && newRunCount >= Number(row.run_limit)) {
        nextRunAt = null
      }

      // Check if past end date
      if (nextRunAt && row.to_date && nextRunAt > Number(row.to_date)) {
        nextRunAt = null
      }

      // Update atomically
      await trx(this.#schedulesTable).where('id', row.id).update({
        next_run_at: nextRunAt,
        last_run_at: now,
        run_count: newRunCount,
      })
      await this.#notifySchedules(trx)

      // Return schedule data (before update state for payload)
      return this.#rowToScheduleData(row)
    })

    // A watcher woken for a schedule that expired meanwhile, past its end
    // date or run limit, waits for nothing: tell it the next claimable one.
    // A due one is locked by a concurrent claim, which notifies on commit:
    // waking for it again would only spin.
    if (!claimed) {
      for (const watcher of this.#scheduleWatchers) {
        if (watcher.at !== null && watcher.at <= now) {
          await this.#announceNextSchedule(watcher, true)
        }
      }
    }

    return claimed
  }

  #rowToScheduleData(row: ScheduleRow): ScheduleData {
    return {
      id: row.id,
      name: row.name,
      payload: typeof row.payload === 'string' ? JSON.parse(row.payload) : row.payload,
      cronExpression: row.cron_expression ?? null,
      everyMs: row.every_ms ? Number(row.every_ms) : null,
      timezone: row.timezone ?? 'UTC',
      from: epochToDate(row.from_date),
      to: epochToDate(row.to_date),
      limit: row.run_limit ? Number(row.run_limit) : null,
      runCount: Number(row.run_count ?? 0),
      nextRunAt: epochToDate(row.next_run_at),
      lastRunAt: epochToDate(row.last_run_at),
      status: row.status === 'paused' || row.status === 'cancelled' ? 'paused' : 'active',
      createdAt: epochToDate(row.created_at) ?? new Date(),
    }
  }
}
