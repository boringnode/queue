import { randomUUID } from 'node:crypto'
import type { Adapter, AcquiredJob, PushResult } from '../../src/contracts/adapter.js'
import type {
  JobData,
  JobRecord,
  JobRetention,
  ScheduleConfig,
  ScheduleData,
  ScheduleListOptions,
} from '../../src/types/main.js'
import { parse } from '../../src/utils.js'

interface ActiveJob {
  job: JobData
  acquiredAt: number
  queue: string
}

interface DelayedJob {
  job: JobData
  executeAt: number
}

interface DedupEntry {
  jobId: string
  createdAt: number
  ttl?: number
  replace?: boolean
  extend?: boolean
}

export function memory() {
  return () => new MemoryAdapter()
}

export class MemoryAdapter implements Adapter {
  #queues: Map<string, JobData[]> = new Map()
  #activeJobs: Map<string, ActiveJob> = new Map()
  #delayedJobs: Map<string, Map<string, DelayedJob>> = new Map()
  #completedJobs: Map<string, JobRecord[]> = new Map()
  #failedJobs: Map<string, JobRecord[]> = new Map()
  #pendingTimeouts: Set<NodeJS.Timeout> = new Set()
  #schedules: Map<string, ScheduleData> = new Map()
  #dedupIndex: Map<string, Map<string, DedupEntry>> = new Map()

  setWorkerId(_workerId: string): void {}

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
    const deduped = this.#applyDedup(queue, jobData)
    if (deduped) return deduped

    if (!this.#queues.has(queue)) {
      this.#queues.set(queue, [])
    }

    this.#queues.get(queue)!.push(jobData)

    if (jobData.dedup) {
      return { outcome: 'added', jobId: jobData.id }
    }
  }

  async pushLater(jobData: JobData, delay: number): Promise<PushResult | void> {
    return this.pushLaterOn('default', jobData, delay)
  }

  async pushLaterOn(queue: string, jobData: JobData, delay: number): Promise<PushResult | void> {
    const deduped = this.#applyDedup(queue, jobData)
    if (deduped) return deduped

    if (!this.#delayedJobs.has(queue)) {
      this.#delayedJobs.set(queue, new Map())
    }

    const executeAt = Date.now() + delay
    this.#delayedJobs.get(queue)!.set(jobData.id, { job: jobData, executeAt })

    const timeout = setTimeout(() => {
      this.#pendingTimeouts.delete(timeout)
      this.#delayedJobs.get(queue)?.delete(jobData.id)
      if (!this.#queues.has(queue)) {
        this.#queues.set(queue, [])
      }
      this.#queues.get(queue)!.push(jobData)
    }, delay)

    this.#pendingTimeouts.add(timeout)

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
    let bestPriority = jobs[0].priority ?? 5

    for (let i = 1; i < jobs.length; i++) {
      const priority = jobs[i].priority ?? 5
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
        await this.pushLaterOn(queue, updatedJob, delay)
        return
      }
    }

    await this.pushOn(queue, updatedJob)
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
        this.#cleanupDedupForJob(queue, active.job)
        continue
      }

      // Recover the job - put back in queue with incremented stalledCount
      this.#activeJobs.delete(jobId)

      const updatedJob = {
        ...active.job,
        stalledCount: currentStalledCount + 1,
      }

      await this.pushOn(queue, updatedJob)
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
      // For memory adapter in tests, just add 24h as approximation
      // Real adapters will use cron-parser
      nextRunAt = new Date(now.getTime() + 24 * 60 * 60 * 1000)
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

  #findHistory(store: Map<string, JobRecord[]>, queue: string, jobId: string): JobRecord | null {
    const records = store.get(queue)
    if (!records) return null

    return records.find((record) => record.data.id === jobId) ?? null
  }

  #applyDedup(queue: string, jobData: JobData): PushResult | null {
    if (!jobData.dedup) return null

    const dedupId = jobData.dedup.id
    const now = Date.now()
    const entry = this.#dedupIndex.get(queue)?.get(dedupId)

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

    if (!this.#dedupIndex.has(queue)) {
      this.#dedupIndex.set(queue, new Map())
    }
    this.#dedupIndex.get(queue)!.set(dedupId, {
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

  #cleanupDedupForJob(queue: string, job: JobData): void {
    if (!job.dedup) return
    const entry = this.#dedupIndex.get(queue)?.get(job.dedup.id)
    if (entry && entry.jobId === job.id) {
      this.#dedupIndex.get(queue)?.delete(job.dedup.id)
    }
  }
}
