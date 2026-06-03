import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import { CronExpressionParser } from 'cron-parser'
import type { Adapter, AcquiredJob, PushResult } from '../contracts/adapter.js'
import type {
  JobData,
  JobClass,
  JobRecord,
  JobRetention,
  ScheduleConfig,
  ScheduleData,
  ScheduleListOptions,
} from '../types/main.js'
import { DEFAULT_PRIORITY } from '../constants.js'
import { parse } from '../utils.js'
import { Job } from '../job.js'

interface DedupEntry {
  jobId: string
  createdAt: number
  ttl?: number
  replace?: boolean
  extend?: boolean
}

interface ActiveJob {
  job: JobData
  acquiredAt: number
  queue: string
}

interface DelayedJob {
  job: JobData
  executeAt: number
  delay: number
}

export interface FakeJobRecord {
  queue: string
  job: JobData
  delay?: number
  pushedAt: number
}

export type FakeJobMatcher = string | JobClass | ((job: JobData) => boolean)
export type FakePayloadMatcher =
  | ((payload: unknown) => boolean)
  | object
  | string
  | number
  | boolean
  | null
  | undefined
export type FakeDelayMatcher = number | ((delay: number | undefined) => boolean)

export interface FakeJobQuery {
  queue?: string
  payload?: FakePayloadMatcher
  delay?: FakeDelayMatcher
}

/**
 * Create a fake adapter factory.
 */
export function fake() {
  return () => new FakeAdapter()
}

/**
 * In-memory adapter designed for tests with assertion helpers.
 */
export class FakeAdapter implements Adapter {
  #queues = new Map<string, JobData[]>()
  #activeJobs = new Map<string, ActiveJob>()
  #delayedJobs = new Map<string, Map<string, DelayedJob>>()
  #completedJobs = new Map<string, JobRecord[]>()
  #failedJobs = new Map<string, JobRecord[]>()
  #pendingTimeouts = new Set<NodeJS.Timeout>()
  #schedules = new Map<string, ScheduleData>()
  #pushedJobs: FakeJobRecord[] = []
  #dedupIndex = new Map<string, Map<string, DedupEntry>>()
  #onDispose?: () => void

  /**
   * Set the function to call when the fake is disposed
   */
  onDispose(fn: () => void) {
    this.#onDispose = fn
    return this
  }

  [Symbol.dispose]() {
    this.#onDispose?.()
  }

  setWorkerId(_workerId: string): void {}

  getPushedJobs(): FakeJobRecord[] {
    return [...this.#pushedJobs]
  }

  getPushedJobsOn(queue: string): FakeJobRecord[] {
    return this.#pushedJobs.filter((record) => record.queue === queue)
  }

  findPushed(matcher: FakeJobMatcher, query?: FakeJobQuery): FakeJobRecord | undefined {
    return this.#pushedJobs.find((record) => this.#matchesRecord(record, matcher, query))
  }

  clearPushedJobs(): void {
    this.#pushedJobs = []
  }

  clear(): void {
    for (const timeout of this.#pendingTimeouts) {
      clearTimeout(timeout)
    }

    this.#pendingTimeouts.clear()
    this.#queues.clear()
    this.#activeJobs.clear()
    this.#delayedJobs.clear()
    this.#completedJobs.clear()
    this.#failedJobs.clear()
    this.#schedules.clear()
    this.#pushedJobs = []
    this.#dedupIndex.clear()
  }

  assertPushed(matcher: FakeJobMatcher, query?: FakeJobQuery): void {
    const record = this.findPushed(matcher, query)
    assert.ok(record, this.#formatFailure('Expected job to be pushed', matcher, query))
  }

  assertNotPushed(matcher: FakeJobMatcher, query?: FakeJobQuery): void {
    const record = this.findPushed(matcher, query)
    assert.ok(!record, this.#formatFailure('Expected job to not be pushed', matcher, query))
  }

  assertPushedCount(count: number, options?: { queue?: string }): void {
    const actual = options?.queue
      ? this.#pushedJobs.filter((record) => record.queue === options.queue).length
      : this.#pushedJobs.length

    const suffix = options?.queue ? ` on "${options.queue}"` : ''
    assert.equal(actual, count, `Expected ${count} pushed job(s)${suffix}, got ${actual}`)
  }

  assertNothingPushed(): void {
    assert.equal(
      this.#pushedJobs.length,
      0,
      `Expected no jobs to be pushed, got ${this.#pushedJobs.length}`
    )
  }

  async size(): Promise<number> {
    return this.sizeOf('default')
  }

  async sizeOf(queue: string): Promise<number> {
    const jobs = this.#queues.get(queue) || []

    return jobs.length
  }

  async push(jobData: JobData): Promise<PushResult | void> {
    return this.pushOn('default', jobData)
  }

  async pushOn(queue: string, jobData: JobData): Promise<PushResult | void> {
    const deduped = await this.#applyDedup(queue, jobData)
    if (deduped) return deduped

    this.#recordPush(queue, jobData)
    this.#enqueue(queue, jobData)

    if (jobData.dedup) {
      return { outcome: 'added', jobId: jobData.id }
    }
  }

  async pushLater(jobData: JobData, delay: number): Promise<PushResult | void> {
    return this.pushLaterOn('default', jobData, delay)
  }

  async pushLaterOn(queue: string, jobData: JobData, delay: number): Promise<PushResult | void> {
    const deduped = await this.#applyDedup(queue, jobData)
    if (deduped) return deduped

    this.#recordPush(queue, jobData, delay)
    this.#schedulePush(queue, jobData, delay)

    if (jobData.dedup) {
      return { outcome: 'added', jobId: jobData.id }
    }
  }

  async pushMany(jobs: JobData[]): Promise<void> {
    return this.pushManyOn('default', jobs)
  }

  async pushManyOn(queue: string, jobs: JobData[]): Promise<void> {
    if (jobs.some((j) => j.dedup)) {
      throw new Error('dedup is not supported in batch dispatch; use single dispatch')
    }

    for (const job of jobs) {
      await this.pushOn(queue, job)
    }
  }

  async pop(): Promise<AcquiredJob | null> {
    return this.popFrom('default')
  }

  async popFrom(queue: string): Promise<AcquiredJob | null> {
    const jobs = this.#queues.get(queue)

    if (!jobs || jobs.length === 0) {
      return null
    }

    // Find job with highest priority (lowest priority number)
    let bestIndex = 0
    let bestPriority = jobs[0].priority ?? DEFAULT_PRIORITY

    for (let i = 1; i < jobs.length; i++) {
      const priority = jobs[i].priority ?? DEFAULT_PRIORITY
      if (priority < bestPriority) {
        bestPriority = priority
        bestIndex = i
      }
    }

    const [job] = jobs.splice(bestIndex, 1)
    if (!job) {
      return null
    }

    const acquiredAt = Date.now()
    this.#activeJobs.set(job.id, { job, acquiredAt, queue })

    return { ...job, acquiredAt }
  }

  async completeJob(jobId: string, queue: string, removeOnComplete?: JobRetention): Promise<void> {
    const active = this.#activeJobs.get(jobId)
    if (!active) return

    this.#activeJobs.delete(jobId)

    if (removeOnComplete === undefined || removeOnComplete === true) {
      this.#cleanupDedupForJob(queue, active.job)
      return
    }

    this.#storeHistory(queue, 'completed', active.job, removeOnComplete)
  }

  async failJob(
    jobId: string,
    queue: string,
    error?: Error,
    removeOnFail?: JobRetention
  ): Promise<void> {
    const active = this.#activeJobs.get(jobId)
    if (!active) return

    this.#activeJobs.delete(jobId)

    if (removeOnFail === undefined || removeOnFail === true) {
      this.#cleanupDedupForJob(queue, active.job)
      return
    }

    this.#storeHistory(queue, 'failed', active.job, removeOnFail, error)
  }

  async retryJob(jobId: string, queue: string, retryAt?: Date): Promise<void> {
    const active = this.#activeJobs.get(jobId)
    if (!active) return

    this.#activeJobs.delete(jobId)

    const updatedJob = {
      ...active.job,
      attempts: (active.job.attempts || 0) + 1,
    }

    if (retryAt) {
      const delay = retryAt.getTime() - Date.now()

      if (delay > 0) {
        this.#schedulePush(queue, updatedJob, delay)
        return
      }
    }

    this.#enqueue(queue, updatedJob)
  }

  async recoverStalledJobs(
    queue: string,
    stalledThreshold: number,
    maxStalledCount: number
  ): Promise<number> {
    const now = Date.now()
    let recovered = 0

    for (const [jobId, active] of this.#activeJobs.entries()) {
      if (active.queue !== queue) {
        continue
      }

      const isStalled = now - active.acquiredAt > stalledThreshold

      if (!isStalled) {
        continue
      }

      const currentStalledCount = active.job.stalledCount ?? 0

      // Check if job has exceeded max stalled count
      if (currentStalledCount >= maxStalledCount) {
        // Fail permanently - just remove from active
        this.#activeJobs.delete(jobId)
        this.#cleanupDedupForJob(active.queue, active.job)
        continue
      }

      // Recover the job - put back in queue with incremented stalledCount
      this.#activeJobs.delete(jobId)

      const updatedJob = {
        ...active.job,
        stalledCount: currentStalledCount + 1,
      }

      this.#enqueue(active.queue, updatedJob)
      recovered++
    }

    return recovered
  }

  async getJob(jobId: string, queue: string): Promise<JobRecord | null> {
    const active = this.#activeJobs.get(jobId)
    if (active && active.queue === queue) {
      return { status: 'active', data: active.job }
    }

    const pendingJobs = this.#queues.get(queue)
    const pending = pendingJobs?.find((job) => job.id === jobId)
    if (pending) {
      return { status: 'pending', data: pending }
    }

    const delayed = this.#delayedJobs.get(queue)?.get(jobId)
    if (delayed) {
      return { status: 'delayed', data: delayed.job }
    }

    const completed = this.#findHistory(this.#completedJobs, queue, jobId)
    if (completed) {
      return completed
    }

    const failed = this.#findHistory(this.#failedJobs, queue, jobId)
    if (failed) {
      return failed
    }

    return null
  }

  destroy(): Promise<void> {
    for (const timeout of this.#pendingTimeouts) {
      clearTimeout(timeout)
    }

    this.#pendingTimeouts.clear()

    return Promise.resolve()
  }

  async upsertSchedule(config: ScheduleConfig): Promise<string> {
    const id = config.id ?? randomUUID()
    const existing = this.#schedules.get(id)
    const now = new Date()

    const schedule: ScheduleData = {
      id,
      name: config.name,
      payload: config.payload,
      cronExpression: config.cronExpression ?? null,
      everyMs: config.everyMs ?? null,
      timezone: config.timezone,
      from: config.from ?? null,
      to: config.to ?? null,
      limit: config.limit ?? null,
      runCount: existing?.runCount ?? 0,
      nextRunAt: existing?.nextRunAt ?? null, // Will be (re)calculated by the caller
      lastRunAt: existing?.lastRunAt ?? null,
      status: 'active',
      createdAt: existing?.createdAt ?? now,
    }

    this.#schedules.set(id, schedule)
    return id
  }

  /**
   * @deprecated Use `upsertSchedule` instead.
   */
  createSchedule(config: ScheduleConfig): Promise<string> {
    return this.upsertSchedule(config)
  }

  async getSchedule(id: string): Promise<ScheduleData | null> {
    return this.#schedules.get(id) ?? null
  }

  async listSchedules(options?: ScheduleListOptions): Promise<ScheduleData[]> {
    const schedules = Array.from(this.#schedules.values())

    if (options?.status) {
      return schedules.filter((s) => s.status === options.status)
    }

    return schedules
  }

  async updateSchedule(
    id: string,
    updates: Partial<Pick<ScheduleData, 'status' | 'nextRunAt' | 'lastRunAt' | 'runCount'>>
  ): Promise<void> {
    const schedule = this.#schedules.get(id)
    if (!schedule) return

    if (updates.status !== undefined) schedule.status = updates.status
    if (updates.nextRunAt !== undefined) schedule.nextRunAt = updates.nextRunAt
    if (updates.lastRunAt !== undefined) schedule.lastRunAt = updates.lastRunAt
    if (updates.runCount !== undefined) schedule.runCount = updates.runCount
  }

  async deleteSchedule(id: string): Promise<void> {
    this.#schedules.delete(id)
  }

  async claimDueSchedule(): Promise<ScheduleData | null> {
    const now = new Date()

    // Find first due schedule
    const schedule = Array.from(this.#schedules.values()).find((s) => {
      if (s.status !== 'active') return false
      if (s.nextRunAt === null || s.nextRunAt > now) return false
      if (s.limit !== null && s.runCount >= s.limit) return false
      if (s.to !== null && now > s.to) return false
      return true
    })

    if (!schedule) return null

    // Calculate next run
    let nextRunAt: Date | null = null
    if (schedule.everyMs) {
      nextRunAt = new Date(now.getTime() + schedule.everyMs)
    } else if (schedule.cronExpression) {
      const cron = CronExpressionParser.parse(schedule.cronExpression, {
        currentDate: now,
        tz: schedule.timezone || 'UTC',
      })
      nextRunAt = cron.next().toDate()
    }

    // Check if limit will be reached after this run
    const newRunCount = schedule.runCount + 1
    if (schedule.limit !== null && newRunCount >= schedule.limit) {
      nextRunAt = null // No more runs
    }

    // Check if end date will be passed
    if (nextRunAt && schedule.to !== null && nextRunAt > schedule.to) {
      nextRunAt = null // Past end date
    }

    // Clone schedule data before updating (return old state)
    const claimedSchedule: ScheduleData = { ...schedule }

    // Update schedule atomically
    schedule.nextRunAt = nextRunAt
    schedule.lastRunAt = now
    schedule.runCount = newRunCount

    return claimedSchedule
  }

  #recordPush(queue: string, jobData: JobData, delay?: number) {
    this.#pushedJobs.push({
      queue,
      job: jobData,
      delay,
      pushedAt: Date.now(),
    })
  }

  #enqueue(queue: string, jobData: JobData) {
    if (!this.#queues.has(queue)) {
      this.#queues.set(queue, [])
    }

    this.#queues.get(queue)!.push(jobData)
  }

  #schedulePush(queue: string, jobData: JobData, delay: number) {
    if (!this.#delayedJobs.has(queue)) {
      this.#delayedJobs.set(queue, new Map())
    }

    const executeAt = Date.now() + delay
    this.#delayedJobs.get(queue)!.set(jobData.id, { job: jobData, executeAt, delay })

    const timeout = setTimeout(() => {
      this.#pendingTimeouts.delete(timeout)
      this.#delayedJobs.get(queue)?.delete(jobData.id)
      this.#enqueue(queue, jobData)
    }, delay)

    this.#pendingTimeouts.add(timeout)
  }

  #storeHistory(
    queue: string,
    status: 'completed' | 'failed',
    job: JobData,
    retention: JobRetention,
    error?: Error
  ) {
    const record: JobRecord = {
      status,
      data: job,
      finishedAt: Date.now(),
      error: error?.message,
    }

    const store = status === 'completed' ? this.#completedJobs : this.#failedJobs

    if (!store.has(queue)) {
      store.set(queue, [])
    }

    const records = store.get(queue)!
    records.push(record)

    if (retention && retention !== true) {
      this.#applyRetention(records, retention, queue)
    }
  }

  #applyRetention(records: JobRecord[], retention: JobRetention, queue: string) {
    if (retention === false || retention === true) {
      return
    }

    const pruned: JobRecord[] = []

    if (retention.age !== undefined) {
      const maxAgeMs = parse(retention.age)
      if (maxAgeMs > 0) {
        const cutoff = Date.now() - maxAgeMs
        const kept: JobRecord[] = []
        for (const record of records) {
          if ((record.finishedAt ?? 0) >= cutoff) {
            kept.push(record)
          } else {
            pruned.push(record)
          }
        }
        records.splice(0, records.length, ...kept)
      }
    }

    if (retention.count !== undefined && retention.count > 0 && records.length > retention.count) {
      const excess = records.length - retention.count
      pruned.push(...records.slice(0, excess))
      records.splice(0, excess)
    }

    for (const record of pruned) {
      this.#cleanupDedupForJob(queue, record.data)
    }
  }

  #applyDedup(queue: string, jobData: JobData): PushResult | null {
    if (!jobData.dedup) return null

    const dedupId = jobData.dedup.id
    const now = Date.now()
    const entry = this.#getDedupEntry(queue, dedupId)

    if (entry) {
      const withinTtl = !entry.ttl || now - entry.createdAt < entry.ttl
      if (withinTtl) {
        const existing = this.#findJobById(queue, entry.jobId)
        if (existing) {
          const replaceable = existing.location === 'pending' || existing.location === 'delayed'
          if (jobData.dedup.replace && replaceable) {
            existing.job.payload = structuredClone(jobData.payload)
            if (jobData.dedup.extend && entry.ttl) {
              entry.createdAt = now
            }
            return { outcome: 'replaced', jobId: entry.jobId }
          }
          if (jobData.dedup.extend && entry.ttl) {
            entry.createdAt = now
            return { outcome: 'extended', jobId: entry.jobId }
          }
          return { outcome: 'skipped', jobId: entry.jobId }
        }
      }
    }

    this.#setDedupEntry(queue, dedupId, {
      jobId: jobData.id,
      createdAt: now,
      ttl: jobData.dedup.ttl,
      replace: jobData.dedup.replace,
      extend: jobData.dedup.extend,
    })

    return null
  }

  #findJobById(
    queue: string,
    jobId: string
  ): {
    job: JobData
    location: 'pending' | 'delayed' | 'active' | 'completed' | 'failed'
  } | null {
    const active = this.#activeJobs.get(jobId)
    if (active && active.queue === queue) {
      return { job: active.job, location: 'active' }
    }
    const pending = this.#queues.get(queue)?.find((j) => j.id === jobId)
    if (pending) {
      return { job: pending, location: 'pending' }
    }
    const delayed = this.#delayedJobs.get(queue)?.get(jobId)
    if (delayed) {
      return { job: delayed.job, location: 'delayed' }
    }
    const completed = this.#findHistory(this.#completedJobs, queue, jobId)
    if (completed) {
      return { job: completed.data, location: 'completed' }
    }
    const failed = this.#findHistory(this.#failedJobs, queue, jobId)
    if (failed) {
      return { job: failed.data, location: 'failed' }
    }
    return null
  }

  #getDedupEntry(queue: string, dedupId: string): DedupEntry | undefined {
    return this.#dedupIndex.get(queue)?.get(dedupId)
  }

  #setDedupEntry(queue: string, dedupId: string, entry: DedupEntry): void {
    if (!this.#dedupIndex.has(queue)) {
      this.#dedupIndex.set(queue, new Map())
    }
    this.#dedupIndex.get(queue)!.set(dedupId, entry)
  }

  #cleanupDedupForJob(queue: string, job: JobData): void {
    if (!job.dedup) return
    const entry = this.#getDedupEntry(queue, job.dedup.id)
    // Only delete if the entry points to THIS job (replace may have re-pointed it elsewhere)
    if (entry && entry.jobId === job.id) {
      this.#dedupIndex.get(queue)?.delete(job.dedup.id)
    }
  }

  #findHistory(store: Map<string, JobRecord[]>, queue: string, jobId: string): JobRecord | null {
    const records = store.get(queue)
    if (!records) return null

    return records.find((record) => record.data.id === jobId) ?? null
  }

  #matchesRecord(record: FakeJobRecord, matcher: FakeJobMatcher, query?: FakeJobQuery): boolean {
    if (query?.queue && record.queue !== query.queue) {
      return false
    }

    const matchesJob =
      typeof matcher === 'string'
        ? record.job.name === matcher
        : this.#isJobClass(matcher)
          ? record.job.name === this.#getJobClassName(matcher)
          : matcher(record.job)

    if (!matchesJob) {
      return false
    }

    if (query?.payload !== undefined) {
      const payloadMatcher = query.payload
      const matchesPayload =
        typeof payloadMatcher === 'function'
          ? payloadMatcher(record.job.payload)
          : isDeepStrictEqual(record.job.payload, payloadMatcher)

      if (!matchesPayload) {
        return false
      }
    }

    if (query?.delay !== undefined) {
      const delayMatcher = query.delay
      const matchesDelay =
        typeof delayMatcher === 'function'
          ? delayMatcher(record.delay)
          : record.delay === delayMatcher

      if (!matchesDelay) {
        return false
      }
    }

    return true
  }

  #formatFailure(prefix: string, matcher: FakeJobMatcher, query?: FakeJobQuery): string {
    const parts = [prefix]

    const matcherName = this.#getMatcherName(matcher)
    if (matcherName) {
      parts.push(`for "${matcherName}"`)
    }

    if (query?.queue) {
      parts.push(`on "${query.queue}"`)
    }

    if (query?.payload !== undefined) {
      parts.push('with matching payload')
    }

    if (query?.delay !== undefined) {
      parts.push('with matching delay')
    }

    const suffix = this.#pushedJobs.length
      ? `Pushed jobs: ${this.#pushedJobs.map((record) => record.job.name).join(', ')}`
      : 'Pushed jobs: none'

    return `${parts.join(' ')}. ${suffix}.`
  }

  #getMatcherName(matcher: FakeJobMatcher): string | undefined {
    if (typeof matcher === 'string') {
      return matcher
    }

    if (this.#isJobClass(matcher)) {
      return this.#getJobClassName(matcher)
    }

    return undefined
  }

  #isJobClass(matcher: FakeJobMatcher): matcher is JobClass {
    return typeof matcher === 'function' && matcher.prototype instanceof Job
  }

  #getJobClassName(JobClass: JobClass): string {
    return JobClass.options?.name || JobClass.name
  }
}
