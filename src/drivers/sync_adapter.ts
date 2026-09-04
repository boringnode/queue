import { setTimeout as sleep } from 'node:timers/promises'
import { QueueManager } from '../queue_manager.js'
import type { Adapter, AcquiredJob } from '../contracts/adapter.js'
import type {
  JobData,
  JobRetention,
  ScheduleConfig,
  ScheduleData,
  ScheduleListOptions,
} from '../types/main.js'

/**
 * Create a sync adapter factory.
 */
export function sync() {
  return () => new SyncAdapter()
}

/**
 * Sync adapter executes jobs immediately when pushed.
 * Pop/complete/fail/retry are not supported as jobs are executed synchronously.
 */
export class SyncAdapter implements Adapter {
  setWorkerId(_workerId: string): void {}

  push(jobData: JobData): Promise<void> {
    return this.pushOn('default', jobData)
  }

  pushOn(queue: string, jobData: JobData): Promise<void> {
    return this.#execute(jobData, queue)
  }

  pushLater(jobData: JobData, delay: number): Promise<void> {
    return this.pushLaterOn('default', jobData, delay)
  }

  pushLaterOn(queue: string, jobData: JobData, delay: number): Promise<void> {
    setTimeout(() => {
      void this.#execute(jobData, queue).catch((error) => {
        QueueManager.getLogger().error(
          { err: error, jobId: jobData.id, jobName: jobData.name, queue },
          'Failed to execute delayed sync job'
        )
      })
    }, delay)

    return Promise.resolve()
  }

  pushMany(jobs: JobData[]): Promise<void> {
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

  size(): Promise<number> {
    return this.sizeOf('default')
  }

  sizeOf(_queue: string): Promise<number> {
    return Promise.resolve(0)
  }

  pop(): Promise<AcquiredJob | null> {
    return this.popFrom('default')
  }

  popFrom(_queue: string): Promise<AcquiredJob | null> {
    throw new Error('SyncAdapter does not support pop - jobs are executed immediately on push')
  }

  completeJob(_jobId: string, _queue: string, _removeOnComplete?: JobRetention): Promise<void> {
    return Promise.resolve()
  }

  failJob(
    _jobId: string,
    _queue: string,
    _error?: Error,
    _removeOnFail?: JobRetention
  ): Promise<void> {
    return Promise.resolve()
  }

  retryJob(_jobId: string, _queue: string, _retryAt?: Date): Promise<void> {
    return Promise.resolve()
  }

  recoverStalledJobs(
    _queue: string,
    _stalledThreshold: number,
    _maxStalledCount: number
  ): Promise<number> {
    // SyncAdapter has no stalled jobs - jobs are executed immediately
    return Promise.resolve(0)
  }

  renewJobs(_queue: string, _jobIds: string[]): Promise<number> {
    // SyncAdapter executes jobs immediately - there is nothing to renew
    return Promise.resolve(0)
  }

  getJob(_jobId: string, _queue: string): Promise<null> {
    return Promise.resolve(null)
  }

  destroy(): Promise<void> {
    return Promise.resolve()
  }

  migrate(): Promise<void> {
    return Promise.resolve()
  }

  upsertSchedule(_config: ScheduleConfig): Promise<string> {
    // No-op: schedules don't make sense for sync adapter
    // Return a fake ID so code doesn't break in dev
    return Promise.resolve(`sync-schedule-${Date.now()}`)
  }

  /**
   * @deprecated Use `upsertSchedule` instead.
   */
  createSchedule(config: ScheduleConfig): Promise<string> {
    return this.upsertSchedule(config)
  }

  getSchedule(_id: string): Promise<ScheduleData | null> {
    return Promise.resolve(null)
  }

  listSchedules(_options?: ScheduleListOptions): Promise<ScheduleData[]> {
    return Promise.resolve([])
  }

  updateSchedule(
    _id: string,
    _updates: Partial<Pick<ScheduleData, 'status' | 'nextRunAt' | 'lastRunAt' | 'runCount'>>
  ): Promise<void> {
    return Promise.resolve()
  }

  deleteSchedule(_id: string): Promise<void> {
    return Promise.resolve()
  }

  claimDueSchedule(): Promise<ScheduleData | null> {
    // SyncAdapter doesn't support scheduling
    return Promise.resolve(null)
  }

  async #execute(jobData: JobData, queue: string = 'default'): Promise<void> {
    const runtime = QueueManager.getJobExecutionRuntime()
    let attempts = jobData.attempts

    while (true) {
      const now = Date.now()
      const acquiredJob: AcquiredJob = { ...jobData, attempts, acquiredAt: now }
      const outcome = await runtime.execute(acquiredJob, queue)

      if (outcome.type === 'initialization-failed') {
        throw outcome.error
      }

      if (outcome.type === 'failed') {
        if (outcome.failedHookError) {
          throw outcome.failedHookError
        }

        return
      }

      if (outcome.type === 'completed') return

      attempts++

      if (outcome.retryAt) {
        const delay = outcome.retryAt.getTime() - Date.now()
        if (delay > 0) {
          await sleep(delay)
        }
      }
    }
  }
}
