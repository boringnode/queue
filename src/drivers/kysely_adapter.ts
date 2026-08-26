import { randomUUID } from 'node:crypto'
import { sql, type ColumnType, type Kysely, type Transaction, type Updateable } from 'kysely'
import type { Adapter, AcquiredJob, PushResult } from '../contracts/adapter.js'
import type {
  JobData,
  JobRecord,
  JobRetention,
  JobStatus,
  ScheduleConfig,
  ScheduleData,
  ScheduleListOptions,
} from '../types/main.js'
import { DEFAULT_PRIORITY } from '../constants.js'
import { calculateScore, resolveRetention } from '../utils.js'
import type { KyselyDialect } from '../services/kysely_queue_schema.js'

export { KyselyQueueSchemaService } from '../services/kysely_queue_schema.js'
export type { KyselyDialect, KyselyQueueSchemaOptions } from '../services/kysely_queue_schema.js'

type OptionalColumn<T> = ColumnType<T, T | undefined, T | undefined>
type NumericValue = number | string | bigint
type DateValue = Date | string | number

/** Queue job table columns applications may merge into their Kysely database interface. */
export interface QueueJobTable {
  id: string
  queue: string
  status: JobStatus
  data: string
  score: OptionalColumn<NumericValue | null>
  worker_id: OptionalColumn<string | null>
  acquired_at: OptionalColumn<NumericValue | null>
  execute_at: OptionalColumn<NumericValue | null>
  finished_at: OptionalColumn<NumericValue | null>
  error: OptionalColumn<string | null>
  dedup_id: OptionalColumn<string | null>
  dedup_at: OptionalColumn<NumericValue | null>
  dedup_ttl: OptionalColumn<NumericValue | null>
}

/** Queue schedule table columns applications may merge into their Kysely database interface. */
export interface QueueScheduleTable {
  id: string
  status: OptionalColumn<'active' | 'paused' | 'cancelled'>
  name: string
  payload: string
  cron_expression: OptionalColumn<string | null>
  every_ms: OptionalColumn<NumericValue | null>
  timezone: OptionalColumn<string>
  from_date: OptionalColumn<DateValue | null>
  to_date: OptionalColumn<DateValue | null>
  run_limit: OptionalColumn<NumericValue | null>
  run_count: OptionalColumn<NumericValue>
  next_run_at: OptionalColumn<DateValue | null>
  last_run_at: OptionalColumn<DateValue | null>
  created_at: OptionalColumn<DateValue>
}

/** Default queue tables for inclusion in an application's Kysely database interface. */
export interface QueueDatabase {
  queue_jobs: QueueJobTable
  queue_schedules: QueueScheduleTable
}

export interface KyselyAdapterOptions {
  dialect: KyselyDialect
  tableName?: string
  schedulesTableName?: string
}

export interface KyselyAdapterConfig<DB> extends KyselyAdapterOptions {
  connection: Kysely<DB>
}

type JobsDatabase = Record<string, QueueJobTable>
type SchedulesDatabase = Record<string, QueueScheduleTable>
type InternalKyselyConnection<DB> = Kysely<DB> | Transaction<DB>
type JobRow = {
  [K in keyof QueueJobTable]: QueueJobTable[K] extends ColumnType<infer S, unknown, unknown>
    ? S
    : QueueJobTable[K]
}
type ScheduleRow = {
  [K in keyof QueueScheduleTable]: QueueScheduleTable[K] extends ColumnType<
    infer S,
    unknown,
    unknown
  >
    ? S
    : QueueScheduleTable[K]
}

/**
 * Create a Kysely adapter factory.
 *
 * The application owns the supplied Kysely connection and its lifecycle.
 */
export function kysely<DB>(connection: Kysely<DB>, options: KyselyAdapterOptions) {
  return () => new KyselyAdapter({ ...options, connection })
}

/** Persistent queue adapter backed by a caller-owned Kysely connection. */
export class KyselyAdapter<DB = QueueDatabase> implements Adapter {
  readonly #connection: Kysely<DB>
  readonly #dialect: KyselyDialect
  readonly #jobsTable: string
  readonly #schedulesTable: string
  #workerId: string = ''

  constructor(config: KyselyAdapterConfig<DB>) {
    this.#connection = config.connection
    this.#dialect = config.dialect
    this.#jobsTable = config.tableName ?? 'queue_jobs'
    this.#schedulesTable = config.schedulesTableName ?? 'queue_schedules'
  }

  setWorkerId(workerId: string): void {
    this.#workerId = workerId
  }

  async destroy(): Promise<void> {
    // The Kysely instance is always owned by the application.
  }

  async migrate(): Promise<void> {}

  async pop(): Promise<AcquiredJob | null> {
    return this.popFrom('default')
  }

  async popFrom(queue: string): Promise<AcquiredJob | null> {
    const now = Date.now()

    await this.#processDelayedJobs(this.#connection, queue, now)

    return this.#withTransaction(this.#connection, async (trx) => {
      let query = this.#jobs(trx)
        .selectFrom(this.#jobsTable)
        .selectAll()
        .where('queue', '=', queue)
        .where('status', '=', 'pending')
        .orderBy('score', 'asc')
        .limit(1)

      if (this.#supportsSkipLocked()) {
        query = query.forUpdate().skipLocked()
      }

      const job = await query.executeTakeFirst()
      if (!job) return null

      let update = this.#jobs(trx)
        .updateTable(this.#jobsTable)
        .set({ status: 'active', worker_id: this.#workerId, acquired_at: now })
        .where('id', '=', job.id)
        .where('queue', '=', queue)

      if (!this.#supportsSkipLocked()) {
        update = update.where('status', '=', 'pending')
      }

      const result = await update.executeTakeFirst()
      if (result.numUpdatedRows === 0n) return null

      return { ...(JSON.parse(job.data) as JobData), acquiredAt: now }
    })
  }

  async #processDelayedJobs(connection: Kysely<DB>, queue: string, now: number): Promise<void> {
    await this.#withTransaction(connection, async (trx) => {
      let query = this.#jobs(trx)
        .selectFrom(this.#jobsTable)
        .select(['id', 'data'])
        .where('queue', '=', queue)
        .where('status', '=', 'delayed')
        .where('execute_at', '<=', now)

      if (this.#supportsSkipLocked()) {
        query = query.forUpdate().skipLocked()
      }

      const delayedJobs = await query.execute()
      for (const job of delayedJobs) {
        const jobData = JSON.parse(job.data) as JobData
        await this.#jobs(trx)
          .updateTable(this.#jobsTable)
          .set({
            status: 'pending',
            score: calculateScore(jobData.priority ?? DEFAULT_PRIORITY, now),
            execute_at: null,
          })
          .where('id', '=', job.id)
          .where('queue', '=', queue)
          .where('status', '=', 'delayed')
          .execute()
      }
    })
  }

  async completeJob(jobId: string, queue: string, removeOnComplete?: JobRetention): Promise<void> {
    const { keep, maxAge, maxCount } = resolveRetention(removeOnComplete)

    if (!keep) {
      await this.#jobs(this.#connection)
        .deleteFrom(this.#jobsTable)
        .where('id', '=', jobId)
        .where('queue', '=', queue)
        .where('status', '=', 'active')
        .execute()
      return
    }

    const now = Date.now()
    const result = await this.#jobs(this.#connection)
      .updateTable(this.#jobsTable)
      .set({
        status: 'completed',
        worker_id: null,
        acquired_at: null,
        finished_at: now,
      })
      .where('id', '=', jobId)
      .where('queue', '=', queue)
      .where('status', '=', 'active')
      .executeTakeFirst()

    if (result.numUpdatedRows > 0n) {
      await this.#pruneHistory(this.#connection, queue, 'completed', maxAge, maxCount, now)
    }
  }

  async failJob(
    jobId: string,
    queue: string,
    error?: Error,
    removeOnFail?: JobRetention
  ): Promise<void> {
    const { keep, maxAge, maxCount } = resolveRetention(removeOnFail)

    if (!keep) {
      await this.#jobs(this.#connection)
        .deleteFrom(this.#jobsTable)
        .where('id', '=', jobId)
        .where('queue', '=', queue)
        .where('status', '=', 'active')
        .execute()
      return
    }

    const now = Date.now()
    const result = await this.#jobs(this.#connection)
      .updateTable(this.#jobsTable)
      .set({
        status: 'failed',
        worker_id: null,
        acquired_at: null,
        finished_at: now,
        error: error?.message ?? null,
      })
      .where('id', '=', jobId)
      .where('queue', '=', queue)
      .where('status', '=', 'active')
      .executeTakeFirst()

    if (result.numUpdatedRows > 0n) {
      await this.#pruneHistory(this.#connection, queue, 'failed', maxAge, maxCount, now)
    }
  }

  async getJob(jobId: string, queue: string): Promise<JobRecord | null> {
    const row = await this.#jobs(this.#connection)
      .selectFrom(this.#jobsTable)
      .selectAll()
      .where('id', '=', jobId)
      .where('queue', '=', queue)
      .executeTakeFirst()

    if (!row) return null

    return {
      status: row.status,
      data: JSON.parse(row.data) as JobData,
      finishedAt: row.finished_at == null ? undefined : Number(row.finished_at),
      error: row.error ?? undefined,
    }
  }

  async #pruneHistory(
    connection: Kysely<DB>,
    queue: string,
    status: 'completed' | 'failed',
    maxAge: number,
    maxCount: number,
    now: number
  ): Promise<void> {
    if (maxAge > 0) {
      await this.#jobs(connection)
        .deleteFrom(this.#jobsTable)
        .where('queue', '=', queue)
        .where('status', '=', status)
        .where('finished_at', '<', now - maxAge)
        .execute()
    }

    if (maxCount > 0) {
      const toKeep = await this.#jobs(connection)
        .selectFrom(this.#jobsTable)
        .select('id')
        .where('queue', '=', queue)
        .where('status', '=', status)
        .orderBy('finished_at', 'desc')
        .limit(maxCount)
        .execute()

      await this.#jobs(connection)
        .deleteFrom(this.#jobsTable)
        .where('queue', '=', queue)
        .where('status', '=', status)
        .where(
          'id',
          'not in',
          toKeep.map((row) => row.id)
        )
        .execute()
    }
  }

  async retryJob(jobId: string, queue: string, retryAt?: Date): Promise<void> {
    const activeJob = await this.#jobs(this.#connection)
      .selectFrom(this.#jobsTable)
      .selectAll()
      .where('id', '=', jobId)
      .where('queue', '=', queue)
      .where('status', '=', 'active')
      .executeTakeFirst()

    if (!activeJob) return

    const now = Date.now()
    const jobData = JSON.parse(activeJob.data) as JobData
    jobData.attempts = (jobData.attempts || 0) + 1

    if (retryAt && retryAt.getTime() > now) {
      await this.#jobs(this.#connection)
        .updateTable(this.#jobsTable)
        .set({
          status: 'delayed',
          data: JSON.stringify(jobData),
          worker_id: null,
          acquired_at: null,
          score: null,
          execute_at: retryAt.getTime(),
        })
        .where('id', '=', jobId)
        .where('queue', '=', queue)
        .where('status', '=', 'active')
        .execute()
      return
    }

    await this.#jobs(this.#connection)
      .updateTable(this.#jobsTable)
      .set({
        status: 'pending',
        data: JSON.stringify(jobData),
        worker_id: null,
        acquired_at: null,
        score: calculateScore(jobData.priority ?? DEFAULT_PRIORITY, now),
        execute_at: null,
      })
      .where('id', '=', jobId)
      .where('queue', '=', queue)
      .where('status', '=', 'active')
      .execute()
  }

  async push(jobData: JobData): Promise<PushResult | void> {
    return this.pushOn('default', jobData)
  }

  async pushOn(queue: string, jobData: JobData): Promise<PushResult | void> {
    const row = {
      id: jobData.id,
      queue,
      status: 'pending' as const,
      data: JSON.stringify(jobData),
      score: calculateScore(jobData.priority ?? DEFAULT_PRIORITY, Date.now()),
    }

    if (jobData.dedup) return this.#pushWithDedup(this.#connection, queue, jobData, row)

    await this.#jobs(this.#connection).insertInto(this.#jobsTable).values(row).execute()
  }

  async pushLater(jobData: JobData, delay: number): Promise<PushResult | void> {
    return this.pushLaterOn('default', jobData, delay)
  }

  async pushLaterOn(queue: string, jobData: JobData, delay: number): Promise<PushResult | void> {
    const row = {
      id: jobData.id,
      queue,
      status: 'delayed' as const,
      data: JSON.stringify(jobData),
      execute_at: Date.now() + delay,
    }

    if (jobData.dedup) return this.#pushWithDedup(this.#connection, queue, jobData, row)

    await this.#jobs(this.#connection).insertInto(this.#jobsTable).values(row).execute()
  }

  async #pushWithDedup(
    connection: Kysely<DB>,
    queue: string,
    jobData: JobData,
    insertRow: Partial<JobRow> & Pick<JobRow, 'id' | 'queue' | 'status' | 'data'>
  ): Promise<PushResult> {
    try {
      return await this.#withTransaction(connection, async (trx) => {
        let existingQuery = this.#jobs(trx)
          .selectFrom(this.#jobsTable)
          .selectAll()
          .where('queue', '=', queue)
          .where('dedup_id', '=', jobData.dedup!.id)
          .orderBy('dedup_at', 'desc')
          .limit(1)

        if (this.#supportsSkipLocked()) existingQuery = existingQuery.forUpdate()

        const existing = await existingQuery.executeTakeFirst()
        const now = Date.now()
        const dedup = jobData.dedup!

        if (existing) {
          const dedupAt = existing.dedup_at == null ? null : Number(existing.dedup_at)
          const dedupTtl = existing.dedup_ttl == null ? null : Number(existing.dedup_ttl)
          const withinTtl = dedupTtl === null || (dedupAt !== null && now - dedupAt < dedupTtl)

          if (withinTtl) {
            const replaceable = existing.status === 'pending' || existing.status === 'delayed'

            if (dedup.replace && replaceable) {
              const storedData = JSON.parse(existing.data) as JobData
              const updates: Updateable<QueueJobTable> = {
                data: JSON.stringify({ ...storedData, payload: jobData.payload }),
              }
              if (dedup.extend && dedupTtl) updates.dedup_at = now

              await this.#jobs(trx)
                .updateTable(this.#jobsTable)
                .set(updates)
                .where('id', '=', existing.id)
                .where('queue', '=', queue)
                .execute()
              return { outcome: 'replaced', jobId: existing.id }
            }

            if (dedup.extend && dedupTtl) {
              await this.#jobs(trx)
                .updateTable(this.#jobsTable)
                .set({ dedup_at: now })
                .where('id', '=', existing.id)
                .where('queue', '=', queue)
                .execute()
              return { outcome: 'extended', jobId: existing.id }
            }

            return { outcome: 'skipped', jobId: existing.id }
          }

          if (
            existing.status === 'pending' ||
            existing.status === 'delayed' ||
            existing.status === 'active'
          ) {
            await this.#jobs(trx)
              .updateTable(this.#jobsTable)
              .set({ dedup_id: null, dedup_at: null, dedup_ttl: null })
              .where('id', '=', existing.id)
              .where('queue', '=', queue)
              .execute()
          }
        }

        const savepoint = `queue_dedup_${randomUUID().replaceAll('-', '')}`
        await sql`savepoint ${sql.id(savepoint)}`.execute(trx)

        try {
          await this.#jobs(trx)
            .insertInto(this.#jobsTable)
            .values({
              ...insertRow,
              dedup_id: dedup.id,
              dedup_at: now,
              dedup_ttl: dedup.ttl ?? null,
            })
            .execute()
          await sql`release savepoint ${sql.id(savepoint)}`.execute(trx)
          return { outcome: 'added', jobId: jobData.id }
        } catch (error) {
          await sql`rollback to savepoint ${sql.id(savepoint)}`.execute(trx)
          await sql`release savepoint ${sql.id(savepoint)}`.execute(trx)
          if (!this.#isUniqueViolation(error)) throw error
        }

        const winner = await this.#jobs(trx)
          .selectFrom(this.#jobsTable)
          .select('id')
          .where('queue', '=', queue)
          .where('dedup_id', '=', dedup.id)
          .where('status', 'in', ['pending', 'delayed'])
          .orderBy('dedup_at', 'desc')
          .executeTakeFirst()

        if (!winner)
          throw new Error(`Unable to resolve concurrent dedup dispatch for "${dedup.id}"`)
        return { outcome: 'skipped', jobId: winner.id }
      })
    } catch (error) {
      if (this.#isMissingDedupColumn(error)) {
        throw new Error(
          `Dedup columns missing on "${this.#jobsTable}". Run KyselyQueueSchemaService.addDedupColumns() before dispatching jobs with .dedup().`,
          { cause: error }
        )
      }
      throw error
    }
  }

  async pushMany(jobs: JobData[]): Promise<void> {
    return this.pushManyOn('default', jobs)
  }

  async pushManyOn(queue: string, jobs: JobData[]): Promise<void> {
    if (jobs.length === 0) return
    if (jobs.some((job) => job.dedup)) {
      throw new Error('dedup is not supported in batch dispatch; use single dispatch')
    }

    const now = Date.now()
    await this.#jobs(this.#connection)
      .insertInto(this.#jobsTable)
      .values(
        jobs.map((job) => ({
          id: job.id,
          queue,
          status: 'pending' as const,
          data: JSON.stringify(job),
          score: calculateScore(job.priority ?? DEFAULT_PRIORITY, now),
        }))
      )
      .execute()
  }

  async size(): Promise<number> {
    return this.sizeOf('default')
  }

  async sizeOf(queue: string): Promise<number> {
    const result = await this.#jobs(this.#connection)
      .selectFrom(this.#jobsTable)
      .select(({ fn }) => fn.countAll<NumericValue>().as('count'))
      .where('queue', '=', queue)
      .where('status', '=', 'pending')
      .executeTakeFirst()

    return Number(result?.count ?? 0)
  }

  async recoverStalledJobs(
    queue: string,
    stalledThreshold: number,
    maxStalledCount: number
  ): Promise<number> {
    const now = Date.now()

    return this.#withTransaction(this.#connection, async (trx) => {
      let query = this.#jobs(trx)
        .selectFrom(this.#jobsTable)
        .select(['id', 'data'])
        .where('queue', '=', queue)
        .where('status', '=', 'active')
        .where('acquired_at', '<', now - stalledThreshold)

      if (this.#supportsSkipLocked()) query = query.forUpdate().skipLocked()

      const stalledJobs = await query.execute()
      let recovered = 0

      for (const row of stalledJobs) {
        const jobData = JSON.parse(row.data) as JobData
        const stalledCount = jobData.stalledCount ?? 0

        if (stalledCount >= maxStalledCount) {
          await this.#jobs(trx)
            .deleteFrom(this.#jobsTable)
            .where('id', '=', row.id)
            .where('queue', '=', queue)
            .where('status', '=', 'active')
            .execute()
          continue
        }

        jobData.stalledCount = stalledCount + 1
        const result = await this.#jobs(trx)
          .updateTable(this.#jobsTable)
          .set({
            status: 'pending',
            data: JSON.stringify(jobData),
            worker_id: null,
            acquired_at: null,
            score: calculateScore(jobData.priority ?? DEFAULT_PRIORITY, now),
          })
          .where('id', '=', row.id)
          .where('queue', '=', queue)
          .where('status', '=', 'active')
          .executeTakeFirst()
        if (result.numUpdatedRows > 0n) recovered++
      }

      return recovered
    })
  }

  async renewJobs(queue: string, jobIds: string[]): Promise<number> {
    if (jobIds.length === 0) return 0

    const result = await this.#jobs(this.#connection)
      .updateTable(this.#jobsTable)
      .set({ acquired_at: Date.now() })
      .where('queue', '=', queue)
      .where('status', '=', 'active')
      .where('worker_id', '=', this.#workerId)
      .where('id', 'in', jobIds)
      .executeTakeFirst()

    return Number(result.numUpdatedRows)
  }

  async upsertSchedule(config: ScheduleConfig): Promise<string> {
    const id = config.id ?? randomUUID()
    const data = {
      id,
      name: config.name,
      payload: JSON.stringify(config.payload),
      cron_expression: config.cronExpression ?? null,
      every_ms: config.everyMs ?? null,
      timezone: config.timezone,
      from_date: this.#dateValue(config.from ?? null),
      to_date: this.#dateValue(config.to ?? null),
      run_limit: config.limit ?? null,
      status: 'active' as const,
    }

    const insert = this.#schedules(this.#connection)
      .insertInto(this.#schedulesTable)
      .values({ ...data, run_count: 0, created_at: this.#dateValue(new Date()) })
    const updates = {
      name: data.name,
      payload: data.payload,
      cron_expression: data.cron_expression,
      every_ms: data.every_ms,
      timezone: data.timezone,
      from_date: data.from_date,
      to_date: data.to_date,
      run_limit: data.run_limit,
      status: 'active' as const,
    }

    if (this.#dialect === 'mysql') {
      await insert.onDuplicateKeyUpdate(updates).execute()
    } else {
      await insert.onConflict((conflict) => conflict.column('id').doUpdateSet(updates)).execute()
    }

    return id
  }

  createSchedule(config: ScheduleConfig): Promise<string> {
    return this.upsertSchedule(config)
  }

  async getSchedule(id: string): Promise<ScheduleData | null> {
    const row = await this.#schedules(this.#connection)
      .selectFrom(this.#schedulesTable)
      .selectAll()
      .where('id', '=', id)
      .executeTakeFirst()

    return row ? this.#rowToScheduleData(row) : null
  }

  async listSchedules(options?: ScheduleListOptions): Promise<ScheduleData[]> {
    let query = this.#schedules(this.#connection)
      .selectFrom(this.#schedulesTable)
      .selectAll()
      .where('status', '!=', 'cancelled')

    if (options?.status) query = query.where('status', '=', options.status)

    return (await query.execute()).map((row) => this.#rowToScheduleData(row))
  }

  async updateSchedule(
    id: string,
    updates: Partial<Pick<ScheduleData, 'status' | 'nextRunAt' | 'lastRunAt' | 'runCount'>>
  ): Promise<void> {
    const data: Updateable<QueueScheduleTable> = {}
    if (updates.status !== undefined) data.status = updates.status
    if (updates.nextRunAt !== undefined) data.next_run_at = this.#dateValue(updates.nextRunAt)
    if (updates.lastRunAt !== undefined) data.last_run_at = this.#dateValue(updates.lastRunAt)
    if (updates.runCount !== undefined) data.run_count = updates.runCount
    if (Object.keys(data).length === 0) return

    await this.#schedules(this.#connection)
      .updateTable(this.#schedulesTable)
      .set(data)
      .where('id', '=', id)
      .execute()
  }

  async deleteSchedule(id: string): Promise<void> {
    await this.#schedules(this.#connection)
      .deleteFrom(this.#schedulesTable)
      .where('id', '=', id)
      .execute()
  }

  async claimDueSchedule(): Promise<ScheduleData | null> {
    const now = new Date()
    const nowValue = this.#dateValue(now)

    return this.#withTransaction(this.#connection, async (trx) => {
      let query = this.#schedules(trx)
        .selectFrom(this.#schedulesTable)
        .selectAll()
        .where('status', '=', 'active')
        .where('next_run_at', 'is not', null)
        .where('next_run_at', '<=', nowValue)
        .where((expression) =>
          expression.or([
            expression('run_limit', 'is', null),
            expression('run_count', '<', expression.ref('run_limit')),
          ])
        )
        .where((expression) =>
          expression.or([expression('to_date', 'is', null), expression('to_date', '>=', nowValue)])
        )
        .orderBy('next_run_at', 'asc')
        .limit(1)

      if (this.#supportsSkipLocked()) query = query.forUpdate().skipLocked()

      const row = await query.executeTakeFirst()
      if (!row) return null

      const newRunCount = Number(row.run_count ?? 0) + 1
      let nextRunAt: Date | null = null

      if (row.every_ms) {
        nextRunAt = new Date(now.getTime() + Number(row.every_ms))
      } else if (row.cron_expression) {
        const { CronExpressionParser } = await import('cron-parser')
        nextRunAt = CronExpressionParser.parse(row.cron_expression, {
          currentDate: now,
          tz: row.timezone || 'UTC',
        })
          .next()
          .toDate()
      }

      if (row.run_limit !== null && newRunCount >= Number(row.run_limit)) nextRunAt = null
      if (nextRunAt && row.to_date && nextRunAt > new Date(row.to_date)) nextRunAt = null

      await this.#schedules(trx)
        .updateTable(this.#schedulesTable)
        .set({
          next_run_at: this.#dateValue(nextRunAt),
          last_run_at: nowValue,
          run_count: newRunCount,
        })
        .where('id', '=', row.id)
        .execute()

      return this.#rowToScheduleData(row)
    })
  }

  #withTransaction<T>(
    connection: Kysely<DB>,
    callback: (trx: Transaction<DB>) => Promise<T>
  ): Promise<T> {
    return connection.transaction().execute(callback)
  }

  #jobs(connection: InternalKyselyConnection<DB>): Kysely<JobsDatabase> {
    return connection as unknown as Kysely<JobsDatabase>
  }

  #schedules(connection: InternalKyselyConnection<DB>): Kysely<SchedulesDatabase> {
    return connection as unknown as Kysely<SchedulesDatabase>
  }

  #supportsSkipLocked(): boolean {
    return this.#dialect === 'postgres' || this.#dialect === 'mysql'
  }

  #dateValue(value: Date): Date | string
  #dateValue(value: null): null
  #dateValue(value: Date | null): Date | string | null
  #dateValue(value: Date | null): Date | string | null {
    if (!value || this.#dialect !== 'sqlite') return value
    return value.toISOString()
  }

  #rowToScheduleData(row: ScheduleRow): ScheduleData {
    return {
      id: row.id,
      name: row.name,
      payload: JSON.parse(row.payload),
      cronExpression: row.cron_expression ?? null,
      everyMs: row.every_ms ? Number(row.every_ms) : null,
      timezone: row.timezone ?? 'UTC',
      from: row.from_date ? new Date(row.from_date) : null,
      to: row.to_date ? new Date(row.to_date) : null,
      limit: row.run_limit ? Number(row.run_limit) : null,
      runCount: Number(row.run_count ?? 0),
      nextRunAt: row.next_run_at ? new Date(row.next_run_at) : null,
      lastRunAt: row.last_run_at ? new Date(row.last_run_at) : null,
      status: row.status === 'active' ? 'active' : 'paused',
      createdAt: row.created_at ? new Date(row.created_at) : new Date(),
    }
  }

  #isUniqueViolation(error: unknown): boolean {
    if (!error || typeof error !== 'object') return false
    const candidate = error as { code?: string; message?: string }
    return (
      candidate.code === '23505' ||
      candidate.code === 'SQLITE_CONSTRAINT_UNIQUE' ||
      candidate.code === 'ER_DUP_ENTRY' ||
      /unique constraint/i.test(candidate.message ?? '')
    )
  }

  #isMissingDedupColumn(error: unknown): boolean {
    if (!error || typeof error !== 'object') return false
    const message = (error as { message?: string }).message ?? ''
    return (
      /dedup_id/.test(message) && /(does not exist|no such column|unknown column)/i.test(message)
    )
  }
}
