import { randomUUID } from 'node:crypto'
import { Redis, type RedisOptions } from 'ioredis'
import { DEFAULT_PRIORITY } from '../constants.js'
import { calculateScore } from '../utils.js'
import type { Adapter, AcquiredJob, PushResult } from '../contracts/adapter.js'
import type { DedupOutcome } from '../types/main.js'
import type {
  JobData,
  JobRecord,
  JobRetention,
  ScheduleConfig,
  ScheduleData,
  ScheduleListOptions,
} from '../types/main.js'
import { resolveRetention } from '../utils.js'
import { encodeRedisJobPayloadOverlay, hydrateRedisJob } from './redis_job_storage.js'
import {
  ACQUIRE_JOB_SCRIPT,
  BACKFILL_SCHEDULE_DUE_INDEX_SCRIPT,
  CLAIM_SCHEDULE_SCRIPT,
  FINALIZE_JOB_SCRIPT,
  FINALIZE_CRON_SCHEDULE_SCRIPT,
  GET_JOB_SCRIPT,
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
const schedulesKey = 'schedules'
const schedulesIndexKey = 'schedules::index'
const schedulesDueKey = 'schedules::due'
type RedisConfig = Redis | RedisOptions

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

    const result = await this.#connection.eval(
      ACQUIRE_JOB_SCRIPT,
      5,
      keys.data,
      keys.pending,
      keys.active,
      keys.delayed,
      keys.overlay,
      this.#workerId,
      now.toString()
    )

    if (!result) {
      return null
    }

    const { data, overlay, acquiredAt } = JSON.parse(result as string) as {
      data: string
      overlay?: string
      acquiredAt: number
    }

    return { ...hydrateRedisJob(data, overlay), acquiredAt }
  }

  async completeJob(jobId: string, queue: string, removeOnComplete?: JobRetention): Promise<void> {
    const keys = this.#getKeys(queue)
    const dedupPrefix = this.#getDedupPrefix(queue)
    const { keep, maxAge, maxCount } = resolveRetention(removeOnComplete)

    if (!keep) {
      await this.#connection.eval(
        REMOVE_JOB_SCRIPT,
        3,
        keys.data,
        keys.active,
        keys.overlay,
        jobId,
        dedupPrefix
      )
      return
    }

    await this.#connection.eval(
      FINALIZE_JOB_SCRIPT,
      5,
      keys.data,
      keys.active,
      keys.completed,
      keys.completedIndex,
      keys.overlay,
      jobId,
      Date.now().toString(),
      maxAge.toString(),
      maxCount.toString(),
      '',
      dedupPrefix
    )
  }

  async failJob(
    jobId: string,
    queue: string,
    error?: Error,
    removeOnFail?: JobRetention
  ): Promise<void> {
    const keys = this.#getKeys(queue)
    const dedupPrefix = this.#getDedupPrefix(queue)
    const { keep, maxAge, maxCount } = resolveRetention(removeOnFail)

    if (!keep) {
      await this.#connection.eval(
        REMOVE_JOB_SCRIPT,
        3,
        keys.data,
        keys.active,
        keys.overlay,
        jobId,
        dedupPrefix
      )
      return
    }

    await this.#connection.eval(
      FINALIZE_JOB_SCRIPT,
      5,
      keys.data,
      keys.active,
      keys.failed,
      keys.failedIndex,
      keys.overlay,
      jobId,
      Date.now().toString(),
      maxAge.toString(),
      maxCount.toString(),
      error?.message || '',
      dedupPrefix
    )
  }

  async retryJob(jobId: string, queue: string, retryAt?: Date): Promise<void> {
    const keys = this.#getKeys(queue)
    const now = Date.now()

    await this.#connection.eval(
      RETRY_JOB_SCRIPT,
      5,
      keys.data,
      keys.active,
      keys.pending,
      keys.delayed,
      keys.overlay,
      jobId,
      retryAt ? retryAt.getTime().toString() : '0',
      now.toString()
    )
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
    maxStalledCount: number
  ): Promise<number> {
    const keys = this.#getKeys(queue)
    const now = Date.now()

    const recovered = await this.#connection.eval(
      RECOVER_STALLED_JOBS_SCRIPT,
      4,
      keys.data,
      keys.active,
      keys.pending,
      keys.overlay,
      now.toString(),
      stalledThreshold.toString(),
      maxStalledCount.toString(),
      this.#getDedupPrefix(queue)
    )

    return recovered as number
  }

  async renewJobs(queue: string, jobIds: string[]): Promise<number> {
    if (jobIds.length === 0) {
      return 0
    }

    const keys = this.#getKeys(queue)
    const now = Date.now()

    const renewed = await this.#connection.eval(
      RENEW_JOBS_SCRIPT,
      1,
      keys.active,
      now.toString(),
      this.#workerId,
      ...jobIds
    )

    return renewed as number
  }

  async upsertSchedule(config: ScheduleConfig): Promise<string> {
    const id = config.id ?? randomUUID()
    const now = Date.now()
    const scheduleKey = `${schedulesKey}::${id}`

    const scheduleData: Record<string, string> = {
      id,
      name: config.name,
      payload: JSON.stringify(config.payload),
      timezone: config.timezone,
      status: 'active',
    }

    if (config.cronExpression !== undefined) scheduleData.cron_expression = config.cronExpression
    if (config.everyMs !== undefined) scheduleData.every_ms = config.everyMs.toString()
    if (config.from !== undefined) scheduleData.from_date = config.from.getTime().toString()
    if (config.to !== undefined) scheduleData.to_date = config.to.getTime().toString()
    if (config.limit !== undefined) scheduleData.run_limit = config.limit.toString()

    await this.#connection.eval(
      UPSERT_SCHEDULE_SCRIPT,
      3,
      scheduleKey,
      schedulesIndexKey,
      schedulesDueKey,
      id,
      now.toString(),
      JSON.stringify(scheduleData)
    )

    return id
  }

  /**
   * @deprecated Use `upsertSchedule` instead.
   */
  createSchedule(config: ScheduleConfig): Promise<string> {
    return this.upsertSchedule(config)
  }

  async getSchedule(id: string): Promise<ScheduleData | null> {
    const scheduleKey = `${schedulesKey}::${id}`
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
      pipeline.hgetall(`${schedulesKey}::${id}`)
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
    const scheduleKey = `${schedulesKey}::${id}`
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
    const scheduleKey = `${schedulesKey}::${id}`
    await this.#connection
      .multi()
      .del(scheduleKey)
      .srem(schedulesIndexKey, id)
      .zrem(schedulesDueKey, id)
      .exec()
  }

  async migrate(): Promise<void> {
    await this.backfillDueIndex()
  }

  async claimDueSchedule(): Promise<ScheduleData | null> {
    const now = Date.now()
    const claimToken = randomUUID()
    const result = await this.#connection.eval(
      CLAIM_SCHEDULE_SCRIPT,
      2,
      schedulesDueKey,
      `${schedulesKey}::`,
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
      const { CronExpressionParser } = await import('cron-parser')
      const cron = CronExpressionParser.parse(data.cron_expression, {
        currentDate: new Date(now),
        tz: data.timezone || 'UTC',
      })
      const nextRun = cron.next().toDate().getTime()

      const runCount = Number.parseInt(data.run_count || '0', 10) + 1
      const runLimit = data.run_limit ? Number.parseInt(data.run_limit, 10) : null
      const toDate = data.to_date ? Number.parseInt(data.to_date, 10) : null

      let newNextRunAt: number | string = nextRun

      if (runLimit !== null && runCount >= runLimit) {
        newNextRunAt = ''
      } else if (toDate && nextRun > toDate) {
        newNextRunAt = ''
      }

      await this.#connection.eval(
        FINALIZE_CRON_SCHEDULE_SCRIPT,
        2,
        `${schedulesKey}::${data.id}`,
        schedulesDueKey,
        data.id,
        data.cron_expression,
        data.config_revision || '',
        claimToken,
        newNextRunAt.toString()
      )
    }

    return this.#hashToScheduleData(data)
  }

  async backfillDueIndex(): Promise<number> {
    return (await this.#connection.eval(
      BACKFILL_SCHEDULE_DUE_INDEX_SCRIPT,
      3,
      schedulesIndexKey,
      schedulesDueKey,
      `${schedulesKey}::`
    )) as number
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
