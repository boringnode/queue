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
  WorkWatcher,
} from '../contracts/adapter.js'
import { consoleLogger, type Logger } from '../logger.js'
import { RedisWorkWatcher } from './redis_work_watcher.js'
import { subscriptionHeartbeat } from './wake_messages.js'
import type { AdapterFactoryContext, DedupOutcome } from '../types/main.js'
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
  SEED_MARKERS_SCRIPT,
  UPDATE_SCHEDULE_SCRIPT,
  UPSERT_SCHEDULE_SCRIPT,
} from './redis_scripts.js'

const redisKey = 'jobs'
const schedulesIndexKey = 'schedules::index'
const schedulesDueKey = 'schedules::due'
// Schedule hashes live one level below the index keys so no schedule id can collide with them.
const scheduleDataPrefix = 'schedules::data::'
const legacyScheduleDataPrefix = 'schedules::'
// Below the due index: a 0.7 schedule hash lives at `schedules::<id>`.
const schedulesMarkerKey = 'schedules::due::marker'

/**
 * How long a watching adapter trusts the next schedule due time it read. A
 * schedule written elsewhere wakes an idle worker through its marker; a
 * busy worker finds it within this delay.
 */
const SCHEDULE_CACHE_TTL = 1000

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
 * the connection lifecycle (closing it on destroy), and logs the
 * connection errors through the QueueManager logger.
 *
 * When passing a Redis instance, the caller is responsible for
 * managing the connection lifecycle and listening to its errors.
 */
export function redis(config?: RedisConfig) {
  return ({ logger }: AdapterFactoryContext = { logger: consoleLogger }) => {
    if (isRedisConnection(config)) {
      return new RedisAdapter(config, false, logger)
    }

    const options: RedisOptions = {
      host: 'localhost',
      port: 6379,
      keyPrefix: 'boringnode::queue::',
      db: 0,
      ...config,
    }

    const connection = new Redis(options)
    // Without a listener, ioredis writes every reconnection error to stderr.
    connection.on('error', (error) => {
      logger.error({ err: error }, 'Redis connection error')
    })

    return new RedisAdapter(connection, true, logger)
  }
}

export class RedisAdapter implements Adapter {
  readonly #connection: Redis
  readonly #ownsConnection: boolean
  readonly #logger: Logger
  #workerId: string = ''
  /**
   * Set once watching: markers then announce new work, so this adapter can
   * skip a look that would find nothing. Each skip saves a round trip.
   */
  #watching = false
  /** Queues an acquisition found empty since the worker last looked for work. */
  readonly #drained = new Set<string>()
  /** The next schedule due time read by a claim, and when it was read. */
  #nextSchedule?: { at: number; readAt: number }

  constructor(connection: Redis, ownsConnection: boolean = false, logger: Logger = consoleLogger) {
    this.#connection = connection
    this.#ownsConnection = ownsConnection
    this.#logger = logger
  }

  /**
   * Wait for work with BZPOPMIN on the markers of the queues and schedules,
   * through a dedicated connection: a blocking command holds its connection.
   */
  async watch(queues: readonly string[], signal?: AbortSignal): Promise<WorkWatcher> {
    signal?.throwIfAborted()
    const markers = queues.map((queue) => ({
      key: this.#getKeys(queue).marker,
      source: this.#getKeys(queue).delayed,
    }))
    markers.push({ key: schedulesMarkerKey, source: schedulesDueKey })

    // A command interrupted by a reconnection fails instead of blocking again.
    const blocking = this.#connection.duplicate({ autoResendUnfulfilledCommands: false })
    blocking.on('error', (error) => {
      this.#logger.error({ err: error }, 'Redis blocking connection error')
    })
    // A command queued during a silent handshake, or dropped on reconnection,
    // never settles: abort and a deadline reject, so that the worker polls.
    const aborted = Promise.withResolvers<never>()
    aborted.promise.catch(() => {})
    const abort = () => aborted.reject(signal!.reason)
    signal?.addEventListener('abort', abort, { once: true })
    const deadline = setTimeout(
      () => aborted.reject(new Error('The Redis blocking connection did not start in time')),
      subscriptionHeartbeat.interval
    )

    try {
      const seedKeys = queues.flatMap((queue) => {
        const keys = this.#getKeys(queue)
        return [keys.pending, keys.delayed, keys.marker]
      })
      seedKeys.push(schedulesDueKey, schedulesMarkerKey)
      // A user without the @blocking ACL category cannot wait: probe BZPOPMIN,
      // so that such a worker polls instead. In MULTI, it returns at once.
      const probe = blocking
        .multi()
        .bzpopmin(`${markers[0].key}::probe`, 0)
        .exec()
        .catch((error: { previousErrors?: Error[] }) => {
          // EXECABORT tells nothing: the NOPERM error is the one to log.
          throw error.previousErrors?.[0] ?? error
        })
      const seed = this.#connection.eval(SEED_MARKERS_SCRIPT, seedKeys.length, ...seedKeys)
      await Promise.race([Promise.all([probe, seed]), aborted.promise])
      signal?.throwIfAborted()
    } catch (error) {
      blocking.disconnect()
      throw error
    } finally {
      clearTimeout(deadline)
      signal?.removeEventListener('abort', abort)
    }

    this.#watching = true
    return new RedisWorkWatcher(this.#connection, blocking, markers, this.#logger, {
      // A new look covers what changed since the last one.
      onReset: () => this.#drained.clear(),
      // A schedule marker may announce a schedule this adapter did not write.
      onScheduleWake: () => (this.#nextSchedule = undefined),
    })
  }

  #getKeys(queue: string) {
    return {
      data: `${redisKey}::${queue}::data`,
      pending: `${redisKey}::${queue}::pending`,
      delayed: `${redisKey}::${queue}::delayed`,
      active: `${redisKey}::${queue}::active`,
      overlay: `${redisKey}::${queue}::metadata`,
      marker: `${redisKey}::${queue}::marker`,
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
    // Jobs pushed since then left a marker: the next wait finds them.
    if (this.#drained.delete(queue)) return null
    const keys = this.#getKeys(queue)
    const now = Date.now()
    const leaseToken = createLeaseToken(this.#workerId)

    const result = await this.#connection.eval(
      ACQUIRE_JOB_SCRIPT,
      6,
      keys.data,
      keys.pending,
      keys.active,
      keys.delayed,
      keys.overlay,
      keys.marker,
      this.#workerId,
      now.toString(),
      leaseToken
    )

    if (!result) {
      return null
    }

    const { data, overlay, acquiredAt, remaining } = JSON.parse(result as string) as {
      data: string
      overlay?: string
      acquiredAt: number
      remaining: boolean
    }
    if (this.#watching && !remaining) this.#drained.add(queue)

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
      6,
      keys.data,
      keys.active,
      keys.pending,
      keys.delayed,
      keys.overlay,
      keys.marker,
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
        6,
        keys.data,
        keys.delayed,
        dedupKey,
        keys.pending,
        keys.overlay,
        keys.marker,
        jobData.id,
        JSON.stringify(jobData),
        executeAt.toString(),
        (jobData.dedup.ttl ?? 0).toString(),
        jobData.dedup.extend ? '1' : '0',
        jobData.dedup.replace ? '1' : '0',
        payloadData,
        payloadIsUndefined,
        '1'
      )) as [string, string]
      return { outcome: result[0] as DedupOutcome, jobId: result[1] }
    }

    await this.#connection.eval(
      PUSH_DELAYED_JOB_SCRIPT,
      4,
      keys.data,
      keys.delayed,
      keys.overlay,
      keys.marker,
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
        6,
        keys.data,
        keys.pending,
        dedupKey,
        keys.delayed,
        keys.overlay,
        keys.marker,
        jobData.id,
        JSON.stringify(jobData),
        score.toString(),
        (jobData.dedup.ttl ?? 0).toString(),
        jobData.dedup.extend ? '1' : '0',
        jobData.dedup.replace ? '1' : '0',
        payloadData,
        payloadIsUndefined,
        '0'
      )) as [string, string]
      return { outcome: result[0] as DedupOutcome, jobId: result[1] }
    }

    await this.#connection.eval(
      PUSH_JOB_SCRIPT,
      4,
      keys.data,
      keys.pending,
      keys.overlay,
      keys.marker,
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

    multi.zadd(keys.marker, 0, '0')
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
      5,
      keys.data,
      keys.active,
      keys.pending,
      keys.overlay,
      keys.marker,
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
    this.#nextSchedule = undefined
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
      5,
      scheduleKey,
      schedulesIndexKey,
      schedulesDueKey,
      `${legacyScheduleDataPrefix}${id}`,
      schedulesMarkerKey,
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
    this.#nextSchedule = undefined
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
      3,
      scheduleKey,
      schedulesDueKey,
      schedulesMarkerKey,
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
    const next = this.#nextSchedule
    if (next && now < next.at && now - next.readAt < SCHEDULE_CACHE_TTL) return null
    this.#nextSchedule = undefined
    const claimToken = randomUUID()

    while (true) {
      const result = (await this.#connection.eval(
        CLAIM_SCHEDULE_SCRIPT,
        3,
        schedulesDueKey,
        scheduleDataPrefix,
        schedulesMarkerKey,
        now.toString(),
        claimToken
      )) as ['claimed' | 'unfinalized' | 'none', string]

      const [outcome, json] = result
      if (outcome === 'none') {
        if (this.#watching) {
          this.#nextSchedule = { at: json === '' ? Infinity : Number(json), readAt: now }
        }
        return null
      }

      const data = JSON.parse(json) as Record<string, string>

      // The worker of this cron claim died before finalizing it. Its occurrence
      // ran already, so finalize it and look for another due schedule.
      if (outcome === 'unfinalized') {
        await this.#finalizeCronClaim({
          id: data.id,
          cronExpression: data.cron_expression,
          timezone: data.timezone,
          configRevision: data.config_revision || '',
          claimToken: data.claim_token,
          claimedAt: Number(data.claimed_at),
        })
        continue
      }

      // The Lua script only computes interval runs: cron needs cron-parser.
      // Until finalized, the claim keeps the schedule from being claimed again.
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
      3,
      `${scheduleDataPrefix}${claim.id}`,
      schedulesDueKey,
      schedulesMarkerKey,
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
