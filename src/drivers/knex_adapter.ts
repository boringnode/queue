import { randomUUID } from 'node:crypto'
import KnexPkg from 'knex'
import type { Knex } from 'knex'
import type { Adapter, AcquiredJob, PushResult } from '../contracts/adapter.js'
import type {
  DedupOutcome,
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

export interface KnexAdapterOptions {
  connection: Knex
  tableName?: string
  schedulesTableName?: string
  ownsConnection?: boolean
}

type KnexConfig = Knex | Knex.Config
type DbRow = Record<string, unknown>

interface ScheduleRow extends DbRow {
  id: string
  name: string
  payload: unknown
  cron_expression: string | null
  every_ms: number | string | null
  timezone: string | null
  from_date: Date | string | number | null
  to_date: Date | string | number | null
  run_limit: number | string | null
  run_count: number | string | null
  next_run_at: Date | string | number | null
  last_run_at: Date | string | number | null
  status: string
  created_at: Date | string | number | null
}

/**
 * Create a new Knex adapter factory.
 * Accepts either a Knex instance or a Knex configuration object.
 *
 * When passing a config object, the adapter will create and manage
 * the connection lifecycle (closing it on destroy).
 *
 * When passing a Knex instance, the caller is responsible for
 * managing the connection lifecycle.
 */
export function knex(config: KnexConfig, tableName?: string) {
  return () => {
    const isKnexInstance = typeof config === 'function'
    const connection = isKnexInstance ? config : KnexPkg(config)
    return new KnexAdapter({ connection, tableName, ownsConnection: !isKnexInstance })
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
  #workerId: string = ''

  constructor(config: KnexAdapterOptions) {
    this.#connection = config.connection
    this.#jobsTable = config.tableName ?? 'queue_jobs'
    this.#schedulesTable = config.schedulesTableName ?? 'queue_schedules'
    this.#ownsConnection = config.ownsConnection ?? false
  }

  setWorkerId(workerId: string): void {
    this.#workerId = workerId
  }

  async destroy(): Promise<void> {
    if (this.#ownsConnection) {
      await this.#connection.destroy()
    }
  }

  async pop(): Promise<AcquiredJob | null> {
    return this.popFrom('default')
  }

  async popFrom(queue: string): Promise<AcquiredJob | null> {
    const now = Date.now()

    // First, move ready delayed jobs to pending
    await this.#processDelayedJobs(queue, now)

    // Use a transaction to atomically pop a job
    return this.#connection.transaction(async (trx) => {
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

      const updated = await updateQuery.update({
        status: 'active',
        worker_id: this.#workerId,
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

  async #processDelayedJobs(queue: string, now: number): Promise<void> {
    // Use a transaction with row locking to prevent race conditions
    await this.#connection.transaction(async (trx) => {
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
    })
  }

  async completeJob(jobId: string, queue: string, removeOnComplete?: JobRetention): Promise<void> {
    const { keep, maxAge, maxCount } = resolveRetention(removeOnComplete)

    if (!keep) {
      await this.#connection(this.#jobsTable)
        .where('id', jobId)
        .where('queue', queue)
        .where('status', 'active')
        .delete()
      return
    }

    const now = Date.now()

    const updated = await this.#connection(this.#jobsTable)
      .where('id', jobId)
      .where('queue', queue)
      .where('status', 'active')
      .update({
        status: 'completed',
        worker_id: null,
        acquired_at: null,
        finished_at: now,
      })

    if (!updated) {
      return
    }

    await this.#pruneHistory(queue, 'completed', maxAge, maxCount, now)
  }

  async failJob(
    jobId: string,
    queue: string,
    error?: Error,
    removeOnFail?: JobRetention
  ): Promise<void> {
    const { keep, maxAge, maxCount } = resolveRetention(removeOnFail)

    if (!keep) {
      await this.#connection(this.#jobsTable)
        .where('id', jobId)
        .where('queue', queue)
        .where('status', 'active')
        .delete()
      return
    }

    const now = Date.now()

    const updated = await this.#connection(this.#jobsTable)
      .where('id', jobId)
      .where('queue', queue)
      .where('status', 'active')
      .update({
        status: 'failed',
        worker_id: null,
        acquired_at: null,
        finished_at: now,
        error: error?.message || null,
      })

    if (!updated) {
      return
    }

    await this.#pruneHistory(queue, 'failed', maxAge, maxCount, now)
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

  async #pruneHistory(
    queue: string,
    status: 'completed' | 'failed',
    maxAge: number,
    maxCount: number,
    now: number
  ): Promise<void> {
    if (maxAge > 0) {
      const cutoff = now - maxAge
      await this.#connection(this.#jobsTable)
        .where('queue', queue)
        .where('status', status)
        .where('finished_at', '<', cutoff)
        .delete()
    }

    if (maxCount > 0) {
      const toKeep = this.#connection(this.#jobsTable)
        .where('queue', queue)
        .where('status', status)
        .orderBy('finished_at', 'desc')
        .limit(maxCount)
        .select('id')

      await this.#connection(this.#jobsTable)
        .where('queue', queue)
        .where('status', status)
        .whereNotIn('id', toKeep)
        .delete()
    }
  }

  async retryJob(jobId: string, queue: string, retryAt?: Date): Promise<void> {
    const now = Date.now()

    // Get the active job
    const activeJob = await this.#connection(this.#jobsTable)
      .where('id', jobId)
      .where('queue', queue)
      .where('status', 'active')
      .first()

    if (!activeJob) return

    const jobData: JobData = JSON.parse(activeJob.data)
    jobData.attempts = (jobData.attempts || 0) + 1

    const updatedData = JSON.stringify(jobData)

    if (retryAt && retryAt.getTime() > now) {
      // Move to delayed
      await this.#connection(this.#jobsTable).where('id', jobId).where('queue', queue).update({
        status: 'delayed',
        data: updatedData,
        worker_id: null,
        acquired_at: null,
        score: null,
        execute_at: retryAt.getTime(),
      })
    } else {
      // Move back to pending
      const priority = jobData.priority ?? DEFAULT_PRIORITY
      const score = calculateScore(priority, now)

      await this.#connection(this.#jobsTable).where('id', jobId).where('queue', queue).update({
        status: 'pending',
        data: updatedData,
        worker_id: null,
        acquired_at: null,
        score,
        execute_at: null,
      })
    }
  }

  async push(jobData: JobData): Promise<PushResult | void> {
    return this.pushOn('default', jobData)
  }

  async pushOn(queue: string, jobData: JobData): Promise<PushResult | void> {
    const priority = jobData.priority ?? DEFAULT_PRIORITY
    const timestamp = Date.now()
    const score = calculateScore(priority, timestamp)

    if (jobData.dedup) {
      return this.#pushWithDedup(queue, jobData, {
        id: jobData.id,
        queue,
        status: 'pending',
        data: JSON.stringify(jobData),
        score,
      })
    }

    await this.#connection(this.#jobsTable).insert({
      id: jobData.id,
      queue,
      status: 'pending',
      data: JSON.stringify(jobData),
      score,
    })
  }

  async pushLater(jobData: JobData, delay: number): Promise<PushResult | void> {
    return this.pushLaterOn('default', jobData, delay)
  }

  async pushLaterOn(queue: string, jobData: JobData, delay: number): Promise<PushResult | void> {
    const executeAt = Date.now() + delay

    if (jobData.dedup) {
      return this.#pushWithDedup(queue, jobData, {
        id: jobData.id,
        queue,
        status: 'delayed',
        data: JSON.stringify(jobData),
        execute_at: executeAt,
      })
    }

    await this.#connection(this.#jobsTable).insert({
      id: jobData.id,
      queue,
      status: 'delayed',
      data: JSON.stringify(jobData),
      execute_at: executeAt,
    })
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
          `Dedup columns missing on "${this.#jobsTable}". Run QueueSchemaService.addDedupColumns() on your jobs table before dispatching jobs with .dedup().`,
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

  #pushWithDedupTxn(
    queue: string,
    jobData: JobData,
    insertRow: Record<string, unknown>,
    dedup: NonNullable<JobData['dedup']>
  ): Promise<PushResult> {
    return this.#connection.transaction(async (trx) => {
      const existing = await trx(this.#jobsTable)
        .where('queue', queue)
        .where('dedup_id', dedup.id)
        .orderBy('dedup_at', 'desc')
        .forUpdate()
        .first()

      const now = Date.now()

      if (existing) {
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
        // TTL expired — release the dedup slot from the old row so the new
        // insert can claim it. The old job keeps running to completion; only
        // its dedup identity is cleared. Retained history rows are excluded
        // from the partial unique index predicate, so no update needed there.
        const status = existing.status as JobStatus
        if (status === 'pending' || status === 'delayed' || status === 'active') {
          await trx(this.#jobsTable)
            .where({ id: existing.id, queue })
            .update({ dedup_id: null, dedup_at: null, dedup_ttl: null })
        }
      }

      let raceLost = false
      try {
        await trx.transaction(async (sp) => {
          await sp(this.#jobsTable).insert({
            ...insertRow,
            dedup_id: dedup.id,
            dedup_at: now,
            dedup_ttl: dedup.ttl ?? null,
          })
        })
      } catch (err) {
        if (this.#isUniqueViolation(err)) {
          raceLost = true
        } else {
          throw err
        }
      }

      if (raceLost) {
        const winner = await trx(this.#jobsTable)
          .where('queue', queue)
          .where('dedup_id', dedup.id)
          .whereIn('status', ['pending', 'delayed'])
          .orderBy('dedup_at', 'desc')
          .first()
        if (winner) {
          return { outcome: 'skipped' as DedupOutcome, jobId: winner.id as string }
        }
      }

      return { outcome: 'added' as DedupOutcome, jobId: jobData.id }
    })
  }

  #isUniqueViolation(err: unknown): boolean {
    if (!err || typeof err !== 'object') return false
    const e = err as { code?: string; message?: string }
    return (
      e.code === '23505' ||
      e.code === 'SQLITE_CONSTRAINT_UNIQUE' ||
      /UNIQUE constraint/i.test(e.message ?? '')
    )
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

    await this.#connection(this.#jobsTable).insert(rows)
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
    maxStalledCount: number
  ): Promise<number> {
    const now = Date.now()
    const stalledCutoff = now - stalledThreshold

    // Use a transaction with row locking to prevent race conditions
    return this.#connection.transaction(async (trx) => {
      let recovered = 0

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
          // Fail permanently - remove the job
          await trx(this.#jobsTable).where('id', row.id).where('queue', queue).delete()
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

      return recovered
    })
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
      from_date: config.from ?? null,
      to_date: config.to ?? null,
      run_limit: config.limit ?? null,
      status: 'active',
    }

    // Atomic upsert
    await this.#connection(this.#schedulesTable)
      .insert({
        ...data,
        run_count: 0,
        created_at: this.#connection.fn.now(),
      })
      .onConflict('id')
      .merge({
        name: data.name,
        payload: data.payload,
        cron_expression: data.cron_expression,
        every_ms: data.every_ms,
        timezone: data.timezone,
        from_date: data.from_date,
        to_date: data.to_date,
        run_limit: data.run_limit,
        status: 'active',
      })

    return id
  }

  /**
   * @deprecated Use `upsertSchedule` instead.
   */
  createSchedule(config: ScheduleConfig): Promise<string> {
    return this.upsertSchedule(config)
  }

  async getSchedule(id: string): Promise<ScheduleData | null> {
    const row = (await this.#connection(this.#schedulesTable).where('id', id).first()) as
      | ScheduleRow
      | undefined
    if (!row) return null

    return this.#rowToScheduleData(row)
  }

  async listSchedules(options?: ScheduleListOptions): Promise<ScheduleData[]> {
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
    const data: Record<string, unknown> = {}

    if (updates.status !== undefined) data.status = updates.status
    if (updates.nextRunAt !== undefined) data.next_run_at = updates.nextRunAt
    if (updates.lastRunAt !== undefined) data.last_run_at = updates.lastRunAt
    if (updates.runCount !== undefined) data.run_count = updates.runCount

    if (Object.keys(data).length > 0) {
      await this.#connection(this.#schedulesTable).where('id', id).update(data)
    }
  }

  async deleteSchedule(id: string): Promise<void> {
    await this.#connection(this.#schedulesTable).where('id', id).delete()
  }

  async claimDueSchedule(): Promise<ScheduleData | null> {
    const now = new Date()

    return this.#connection.transaction(async (trx) => {
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
      let nextRunAt: Date | null = null
      const newRunCount = Number(row.run_count ?? 0) + 1

      if (row.every_ms) {
        nextRunAt = new Date(now.getTime() + Number(row.every_ms))
      } else if (row.cron_expression) {
        // Import cron-parser dynamically to calculate next run
        const { CronExpressionParser } = await import('cron-parser')
        const cron = CronExpressionParser.parse(row.cron_expression, {
          currentDate: now,
          tz: row.timezone || 'UTC',
        })
        nextRunAt = cron.next().toDate()
      }

      // Check if limit will be reached
      if (row.run_limit !== null && newRunCount >= Number(row.run_limit)) {
        nextRunAt = null
      }

      // Check if past end date
      if (nextRunAt && row.to_date && nextRunAt > new Date(row.to_date)) {
        nextRunAt = null
      }

      // Update atomically
      await trx(this.#schedulesTable).where('id', row.id).update({
        next_run_at: nextRunAt,
        last_run_at: now,
        run_count: newRunCount,
      })

      // Return schedule data (before update state for payload)
      return this.#rowToScheduleData(row)
    })
  }

  #rowToScheduleData(row: ScheduleRow): ScheduleData {
    return {
      id: row.id,
      name: row.name,
      payload: typeof row.payload === 'string' ? JSON.parse(row.payload) : row.payload,
      cronExpression: row.cron_expression ?? null,
      everyMs: row.every_ms ? Number(row.every_ms) : null,
      timezone: row.timezone ?? 'UTC',
      from: row.from_date ? new Date(row.from_date) : null,
      to: row.to_date ? new Date(row.to_date) : null,
      limit: row.run_limit ? Number(row.run_limit) : null,
      runCount: Number(row.run_count ?? 0),
      nextRunAt: row.next_run_at ? new Date(row.next_run_at) : null,
      lastRunAt: row.last_run_at ? new Date(row.last_run_at) : null,
      status: row.status === 'paused' || row.status === 'cancelled' ? 'paused' : 'active',
      createdAt: row.created_at ? new Date(row.created_at) : new Date(),
    }
  }
}
