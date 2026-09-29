import { randomUUID } from 'node:crypto'
import { Redis, type RedisOptions } from 'ioredis'
import { DEFAULT_PRIORITY } from '../constants.js'
import { calculateScore } from '../utils.js'
import type {
  Adapter,
  AcquiredJob,
  JobLease,
  PushResult,
  StalledJobsRecovery,
} from '../contracts/adapter.js'
import type { DedupOutcome } from '../types/main.js'
import type {
  JobData,
  JobRecord,
  JobRetention,
  ScheduleConfig,
  ScheduleData,
  ScheduleListOptions,
} from '../types/main.js'
import { createLeaseToken, resolveRetention, resolveSchedulePayload } from '../utils.js'
import { encodeRedisJobPayloadOverlay, hydrateRedisJob } from './redis_job_storage.js'
import {
  ACQUIRE_JOB_SCRIPT,
  CLAIM_SCHEDULE_SCRIPT,
  FINALIZE_JOB_SCRIPT,
  FINALIZE_CRON_SCHEDULE_SCRIPT,
  GET_JOB_SCRIPT,
  MIGRATE_SCHEDULES_SCRIPT,
  PUSH_DEDUP_JOB_SCRIPT,
  PUSH_DELAYED_JOB_SCRIPT,
  PUSH_JOB_SCRIPT,
  RECOVER_STALLED_JOBS_SCRIPT,
  REMOVE_JOB_SCRIPT,
  RENEW_JOBS_SCRIPT,
  RETRY_JOB_SCRIPT,
  UPDATE_SCHEDULE_SCRIPT,
  UPSERT_SCHEDULE_SCRIPT,
} from './redis_scripts.js'

const redisKey = 'jobs'
const schedulesIndexKey = 'schedules::index'
const schedulesDueKey = 'schedules::due'
// Schedule hashes live one level below the index keys so no schedule id can collide with them.
const scheduleDataPrefix = 'schedules::data::'
const legacyScheduleDataPrefix = 'schedules::'

/**
 * An ioredis connection, or the options to create one.
 */
export type RedisConfig = Redis | RedisOptions

function isRedisConnection(config?: RedisConfig): config is Redis {
  return !!config && 'defineCommand' in config && typeof config.defineCommand === 'function'
}

/**
 * Create a new Redis adapter factory.
 * Accepts either a Redis instance or Redis options.
 *
 * When passing options, the adapter will create and manage
 * the connection lifecycle (closing it on destroy).
 *
 * When passing a Redis instance, the caller is responsible for
 * managing the connection lifecycle.
 */
export function redis(config?: RedisConfig) {
  return () => {
    if (isRedisConnection(config)) {
      return new RedisAdapter(config, false)
    }

    const options: RedisOptions = {
      host: 'localhost',
      port: 6379,
      keyPrefix: 'boringnode::queue::',
      db: 0,
      ...config,
    }

    const connection = new Redis(options)
    return new RedisAdapter(connection, true)
  }
}

export class RedisAdapter implements Adapter {
  readonly #connection: Redis
  readonly #ownsConnection: boolean
  #workerId: string = ''
  constructor(connection: Redis, ownsConnection: boolean = false) {
    this.#connection = connection
    this.#ownsConnection = ownsConnection
  }

  #getKeys(queue: string) {
    return {
      data: `${redisKey}::${queue}::data`,
      pending: `${redisKey}::${queue}::pending`,
      delayed: `${redisKey}::${queue}::delayed`,
      active: `${redisKey}::${queue}::active`,
      overlay: `${redisKey}::${queue}::metadata`,
      completed: `${redisKey}::${queue}::completed`,
      completedIndex: `${redisKey}::${queue}::completed::index`,
      failed: `${redisKey}::${queue}::failed`,
      failedIndex: `${redisKey}::${queue}::failed::index`,
    }
  }

  #getDedupKey(queue: string, dedupId: string): string {
    return `${this.#getDedupPrefix(queue)}${dedupId}`
  }

  /**
   * Scripts that build dedup keys from this prefix must receive it in KEYS so
   * ioredis applies the connection key prefix to it.
   */
  #getDedupPrefix(queue: string): string {
    return `${redisKey}::${queue}::dedup::`
  }

  setWorkerId(workerId: string): void {
    this.#workerId = workerId
  }

  async destroy(): Promise<void> {
    if (this.#ownsConnection) {
      await this.#connection.quit()
    }
  }

  pop(): Promise<AcquiredJob | null> {
    return this.popFrom('default')
  }

  async popFrom(queue: string): Promise<AcquiredJob | null> {
    const keys = this.#getKeys(queue)
    const now = Date.now()
    const leaseToken = createLeaseToken(this.#workerId)

    const result = await this.#connection.eval(
      ACQUIRE_JOB_SCRIPT,
      5,
      keys.data,
      keys.pending,
      keys.active,
      keys.delayed,
      keys.overlay,
      this.#workerId,
      now.toString(),
      leaseToken
    )

    if (!result) {
      return null
    }

    const { data, overlay, acquiredAt } = JSON.parse(result as string) as {
      data: string
      overlay?: string
      acquiredAt: number
    }

    return { ...hydrateRedisJob(data, overlay), acquiredAt, leaseToken }
  }

  async completeJob(
    job: JobLease,
    queue: string,
    removeOnComplete?: JobRetention
  ): Promise<boolean> {
    const keys = this.#getKeys(queue)
    const dedupPrefix = this.#getDedupPrefix(queue)
    const { keep, maxAge, maxCount } = resolveRetention(removeOnComplete)

    if (!keep) {
      const removed = await this.#connection.eval(
        REMOVE_JOB_SCRIPT,
        4,
        keys.data,
        keys.active,
        keys.overlay,
        dedupPrefix,
        job.id,
        job.leaseToken
      )
      return removed === 1
    }

    const finalized = await this.#connection.eval(
      FINALIZE_JOB_SCRIPT,
      6,
      keys.data,
      keys.active,
      keys.completed,
      keys.completedIndex,
      keys.overlay,
      dedupPrefix,
      job.id,
      Date.now().toString(),
      maxAge.toString(),
      maxCount.toString(),
      '',
      job.leaseToken
    )
    return finalized === 1
  }

  async failJob(
    job: JobLease,
    queue: string,
    error?: Error,
    removeOnFail?: JobRetention
  ): Promise<boolean> {
    const keys = this.#getKeys(queue)
    const dedupPrefix = this.#getDedupPrefix(queue)
    const { keep, maxAge, maxCount } = resolveRetention(removeOnFail)

    if (!keep) {
      const removed = await this.#connection.eval(
        REMOVE_JOB_SCRIPT,
        4,
        keys.data,
        keys.active,
        keys.overlay,
        dedupPrefix,
        job.id,
        job.leaseToken
      )
      return removed === 1
    }

    const finalized = await this.#connection.eval(
      FINALIZE_JOB_SCRIPT,
      6,
      keys.data,
      keys.active,
      keys.failed,
      keys.failedIndex,
      keys.overlay,
      dedupPrefix,
      job.id,
      Date.now().toString(),
      maxAge.toString(),
      maxCount.toString(),
      error?.message || '',
      job.leaseToken
    )
    return finalized === 1
  }

  async retryJob(job: JobLease, queue: string, retryAt?: Date): Promise<boolean> {
    const keys = this.#getKeys(queue)
    const now = Date.now()

    const retried = await this.#connection.eval(
      RETRY_JOB_SCRIPT,
      5,
      keys.data,
      keys.active,
      keys.pending,
      keys.delayed,
      keys.overlay,
      job.id,
      retryAt ? retryAt.getTime().toString() : '0',
      now.toString(),
      job.leaseToken
    )
    return retried === 1
  }

  async getJob(jobId: string, queue: string): Promise<JobRecord | null> {
    const keys = this.#getKeys(queue)

    const result = await this.#connection.eval(
      GET_JOB_SCRIPT,
      7,
      keys.data,
      keys.pending,
      keys.delayed,
      keys.active,
      keys.completed,
      keys.failed,
      keys.overlay,
      jobId
    )

    if (!result) {
      return null
    }

    const record = JSON.parse(result as string) as Omit<JobRecord, 'data'> & {
      data: string
      overlay?: string
    }

    return { ...record, data: hydrateRedisJob(record.data, record.overlay) }
  }

  push(jobData: JobData): Promise<PushResult | void> {
    return this.pushOn('default', jobData)
  }

  pushLater(jobData: JobData, delay: number): Promise<PushResult | void> {
    return this.pushLaterOn('default', jobData, delay)
  }

  async pushLaterOn(queue: string, jobData: JobData, delay: number): Promise<PushResult | void> {
    const keys = this.#getKeys(queue)
    const executeAt = Date.now() + delay

    if (jobData.dedup) {
      const dedupKey = this.#getDedupKey(queue, jobData.dedup.id)
      const [payloadData, payloadIsUndefined] = encodeRedisJobPayloadOverlay(jobData.payload)
      const result = (await this.#connection.eval(
        PUSH_DEDUP_JOB_SCRIPT,
        5,
        keys.data,
        keys.delayed,
        dedupKey,
        keys.pending,
        keys.overlay,
        jobData.id,
        JSON.stringify(jobData),
        executeAt.toString(),
        (jobData.dedup.ttl ?? 0).toString(),
        jobData.dedup.extend ? '1' : '0',
        jobData.dedup.replace ? '1' : '0',
        payloadData,
        payloadIsUndefined
      )) as [string, string]
      return { outcome: result[0] as DedupOutcome, jobId: result[1] }
    }

    await this.#connection.eval(
      PUSH_DELAYED_JOB_SCRIPT,
      3,
      keys.data,
      keys.delayed,
      keys.overlay,
      jobData.id,
      JSON.stringify(jobData),
      executeAt.toString()
    )
  }

  async pushOn(queue: string, jobData: JobData): Promise<PushResult | void> {
    const keys = this.#getKeys(queue)
    const priority = jobData.priority ?? DEFAULT_PRIORITY
    const timestamp = Date.now()
    const score = calculateScore(priority, timestamp)

    if (jobData.dedup) {
      const dedupKey = this.#getDedupKey(queue, jobData.dedup.id)
      const [payloadData, payloadIsUndefined] = encodeRedisJobPayloadOverlay(jobData.payload)
      const result = (await this.#connection.eval(
        PUSH_DEDUP_JOB_SCRIPT,
        5,
        keys.data,
        keys.pending,
        dedupKey,
        keys.delayed,
        keys.overlay,
        jobData.id,
        JSON.stringify(jobData),
        score.toString(),
        (jobData.dedup.ttl ?? 0).toString(),
        jobData.dedup.extend ? '1' : '0',
        jobData.dedup.replace ? '1' : '0',
        payloadData,
        payloadIsUndefined
      )) as [string, string]
      return { outcome: result[0] as DedupOutcome, jobId: result[1] }
    }

    await this.#connection.eval(
      PUSH_JOB_SCRIPT,
      3,
      keys.data,
      keys.pending,
      keys.overlay,
      jobData.id,
      JSON.stringify(jobData),
      score.toString()
    )
  }

  pushMany(jobs: JobData[]): Promise<void> {
    return this.pushManyOn('default', jobs)
  }

  async pushManyOn(queue: string, jobs: JobData[]): Promise<void> {
    if (jobs.length === 0) return

    if (jobs.some((j) => j.dedup)) {
      throw new Error('dedup is not supported in batch dispatch; use single dispatch')
    }

    const keys = this.#getKeys(queue)
    const now = Date.now()
    const multi = this.#connection.multi()

    for (const job of jobs) {
      const priority = job.priority ?? DEFAULT_PRIORITY
      const score = calculateScore(priority, now)
      multi.hdel(keys.overlay, job.id)
      multi.hset(keys.data, job.id, JSON.stringify(job))
      multi.zadd(keys.pending, score, job.id)
    }

    await multi.exec()
  }

  size(): Promise<number> {
    return this.sizeOf('default')
  }

  sizeOf(queue: string): Promise<number> {
    const keys = this.#getKeys(queue)
    return this.#connection.zcard(keys.pending)
  }

  async recoverStalledJobs(
    queue: string,
    stalledThreshold: number,
    maxStalledCount: number,
    maxExceeded: number
  ): Promise<StalledJobsRecovery> {
    const keys = this.#getKeys(queue)
    const now = Date.now()
    // The script appends the job id to get one token per reacquired job.
    const leaseTokenPrefix = createLeaseToken(this.#workerId)

    const [recovered, ...exceeded] = (await this.#connection.eval(
      RECOVER_STALLED_JOBS_SCRIPT,
      4,
      keys.data,
      keys.active,
      keys.pending,
      keys.overlay,
      now.toString(),
      stalledThreshold.toString(),
      maxStalledCount.toString(),
      this.#workerId,
      maxExceeded.toString(),
      leaseTokenPrefix
    )) as [number, ...string[]]

    return {
      recovered,
      exceeded: exceeded.map((result) => {
        const { data, overlay, acquiredAt, leaseToken } = JSON.parse(result) as {
          data: string
          overlay?: string
          acquiredAt: number
          leaseToken: string
        }

        return { ...hydrateRedisJob(data, overlay), acquiredAt, leaseToken }
      }),
    }
  }

  async renewJobs(queue: string, jobs: JobLease[]): Promise<number> {
    if (jobs.length === 0) {
      return 0
    }

    const keys = this.#getKeys(queue)
    const now = Date.now()

    const renewed = await this.#connection.eval(
      RENEW_JOBS_SCRIPT,
      1,
      keys.active,
      now.toString(),
      ...jobs.flatMap((job) => [job.id, job.leaseToken])
    )

    return renewed as number
  }

  async upsertSchedule(config: ScheduleConfig): Promise<string> {
    const id = config.id ?? randomUUID()
    const now = Date.now()
    const scheduleKey = `${scheduleDataPrefix}${id}`

    const scheduleData: Record<string, string> = {
      id,
      name: config.name,
      payload: JSON.stringify(resolveSchedulePayload(config.payload)),
      timezone: config.timezone,
    }

    if (config.cronExpression !== undefined) scheduleData.cron_expression = config.cronExpression
    if (config.everyMs !== undefined) scheduleData.every_ms = config.everyMs.toString()
    if (config.from !== undefined) scheduleData.from_date = config.from.getTime().toString()
    if (config.to !== undefined) scheduleData.to_date = config.to.getTime().toString()
    if (config.limit !== undefined) scheduleData.run_limit = config.limit.toString()

    const unfinalizedClaim = (await this.#connection.eval(
      UPSERT_SCHEDULE_SCRIPT,
      3,
      scheduleKey,
      schedulesIndexKey,
      schedulesDueKey,
      id,
      now.toString(),
      JSON.stringify(scheduleData),
      config.nextRunAt?.getTime().toString() ?? ''
    )) as [string, string, string] | null

    // Finalize it as its worker would have, from the time of the claim. The
    // given next run was computed before, so it may be the claimed occurrence.
    if (unfinalizedClaim && config.cronExpression !== undefined) {
      const [claimToken, configRevision, claimedAt] = unfinalizedClaim
      if (claimedAt !== '') {
        await this.#finalizeCronClaim({
          id,
          cronExpression: config.cronExpression,
          timezone: config.timezone,
          configRevision,
          claimToken,
          claimedAt: Number(claimedAt),
        })
      }
    }

    return id
  }

  /**
   * @deprecated Use `upsertSchedule` instead.
   */
  createSchedule(config: ScheduleConfig): Promise<string> {
    return this.upsertSchedule(config)
  }

  async getSchedule(id: string): Promise<ScheduleData | null> {
    const scheduleKey = `${scheduleDataPrefix}${id}`
    const data = await this.#connection.hgetall(scheduleKey)

    if (!data || Object.keys(data).length === 0) {
      return null
    }

    return this.#hashToScheduleData(data)
  }

  async listSchedules(options?: ScheduleListOptions): Promise<ScheduleData[]> {
    const ids = await this.#connection.smembers(schedulesIndexKey)
    if (ids.length === 0) {
      return []
    }

    const pipeline = this.#connection.pipeline()

    for (const id of ids) {
      pipeline.hgetall(`${scheduleDataPrefix}${id}`)
    }

    const results = await pipeline.exec()
    if (!results) {
      return []
    }

    const schedules: ScheduleData[] = []

    for (const [, data] of results) {
      if (!data || Object.keys(data).length === 0) {
        continue
      }

      const schedule = this.#hashToScheduleData(data as Record<string, string>)

      // Filter by status if provided
      if (options?.status && schedule.status !== options.status) {
        continue
      }

      schedules.push(schedule)
    }

    return schedules
  }

  async updateSchedule(
    id: string,
    updates: Partial<Pick<ScheduleData, 'status' | 'nextRunAt' | 'lastRunAt' | 'runCount'>>
  ): Promise<void> {
    const scheduleKey = `${scheduleDataPrefix}${id}`
    const data: Record<string, string> = {}

    if (updates.status !== undefined) data.status = updates.status
    if (updates.nextRunAt !== undefined) {
      data.next_run_at = updates.nextRunAt ? updates.nextRunAt.getTime().toString() : ''
    }
    if (updates.lastRunAt !== undefined) {
      data.last_run_at = updates.lastRunAt ? updates.lastRunAt.getTime().toString() : ''
    }
    if (updates.runCount !== undefined) data.run_count = updates.runCount.toString()

    if (Object.keys(data).length === 0) return

    await this.#connection.eval(
      UPDATE_SCHEDULE_SCRIPT,
      2,
      scheduleKey,
      schedulesDueKey,
      id,
      JSON.stringify(data)
    )
  }

  async deleteSchedule(id: string): Promise<void> {
    const scheduleKey = `${scheduleDataPrefix}${id}`
    await this.#connection
      .multi()
      .del(scheduleKey)
      .srem(schedulesIndexKey, id)
      .zrem(schedulesDueKey, id)
      .exec()
  }

  async migrate(): Promise<void> {
    await this.#connection.eval(
      MIGRATE_SCHEDULES_SCRIPT,
      4,
      schedulesIndexKey,
      schedulesDueKey,
      scheduleDataPrefix,
      legacyScheduleDataPrefix
    )
  }

  async claimDueSchedule(): Promise<ScheduleData | null> {
    const now = Date.now()
    const claimToken = randomUUID()
    const result = await this.#connection.eval(
      CLAIM_SCHEDULE_SCRIPT,
      2,
      schedulesDueKey,
      scheduleDataPrefix,
      now.toString(),
      claimToken
    )

    if (!result) {
      return null
    }

    const data = JSON.parse(result as string) as Record<string, string>

    // If cron expression, we need to recalculate next_run_at properly.
    // The Lua script only handles simple interval; cron needs JS cron-parser.
    // This is safe because the schedule is already claimed (run_count incremented).
    if (data.cron_expression) {
      await this.#finalizeCronClaim({
        id: data.id,
        cronExpression: data.cron_expression,
        timezone: data.timezone,
        configRevision: data.config_revision || '',
        claimToken,
        claimedAt: now,
      })
    }

    return this.#hashToScheduleData(data)
  }

  /**
   * Writes the next run of a cron claim, the first occurrence after the
   * claim. The script applies it only while the same claim still owns the
   * schedule, and clears it when the run limit or end date is reached.
   */
  async #finalizeCronClaim(claim: {
    id: string
    cronExpression: string
    timezone: string
    configRevision: string
    claimToken: string
    claimedAt: number
  }): Promise<void> {
    const { CronExpressionParser } = await import('cron-parser')
    const nextRunAt = CronExpressionParser.parse(claim.cronExpression, {
      currentDate: new Date(claim.claimedAt),
      tz: claim.timezone || 'UTC',
    })
      .next()
      .toDate()
      .getTime()

    await this.#connection.eval(
      FINALIZE_CRON_SCHEDULE_SCRIPT,
      2,
      `${scheduleDataPrefix}${claim.id}`,
      schedulesDueKey,
      claim.id,
      claim.cronExpression,
      claim.configRevision,
      claim.claimToken,
      nextRunAt.toString()
    )
  }

  #hashToScheduleData(data: Record<string, string>): ScheduleData {
    return {
      id: data.id,
      name: data.name,
      payload: JSON.parse(data.payload || '{}'),
      cronExpression: data.cron_expression || null,
      everyMs: data.every_ms ? Number.parseInt(data.every_ms, 10) : null,
      timezone: data.timezone || 'UTC',
      from: data.from_date ? new Date(Number.parseInt(data.from_date, 10)) : null,
      to: data.to_date ? new Date(Number.parseInt(data.to_date, 10)) : null,
      limit: data.run_limit ? Number.parseInt(data.run_limit, 10) : null,
      runCount: Number.parseInt(data.run_count || '0', 10),
      nextRunAt: data.next_run_at ? new Date(Number.parseInt(data.next_run_at, 10)) : null,
      lastRunAt: data.last_run_at ? new Date(Number.parseInt(data.last_run_at, 10)) : null,
      status: (data.status as 'active' | 'paused') || 'active',
      createdAt: data.created_at ? new Date(Number.parseInt(data.created_at, 10)) : new Date(),
    }
  }
}
