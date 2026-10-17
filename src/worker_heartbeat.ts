import type { Adapter } from './contracts/adapter.js'
import type { JobPool } from './job_pool.js'
import type { Logger } from './logger.js'

export type HeartbeatOperationWrapper = <T>(operation: () => Promise<T>) => Promise<T>

export interface WorkerHeartbeatOptions {
  workerId: string
  queues: readonly string[]
  interval: number
  pool: JobPool
  adapter: Adapter
  wrapInternal: HeartbeatOperationWrapper
  logger: Logger
}

/**
 * Renews the leases of Jobs owned by one WorkerSession.
 */
export class WorkerHeartbeat {
  readonly #workerId: string
  readonly #queues: readonly string[]
  readonly #interval: number
  readonly #pool: JobPool
  readonly #adapter: Adapter
  readonly #wrapInternal: HeartbeatOperationWrapper
  readonly #logger: Logger

  #timer?: NodeJS.Timeout
  #renewal?: Promise<void>

  constructor(options: WorkerHeartbeatOptions) {
    this.#workerId = options.workerId
    this.#queues = Object.freeze([...options.queues])
    this.#interval = options.interval
    this.#pool = options.pool
    this.#adapter = options.adapter
    this.#wrapInternal = options.wrapInternal
    this.#logger = options.logger
  }

  start(): void {
    this.#stopTimer()

    this.#timer = setInterval(() => {
      if (this.#renewal) return

      const renewal = this.#renewActiveJobs()
      this.#renewal = renewal

      const clearRenewal = () => {
        if (this.#renewal === renewal) {
          this.#renewal = undefined
        }
      }

      void renewal.then(clearRenewal, clearRenewal)
    }, this.#interval)

    this.#timer.unref?.()
  }

  async stop(): Promise<void> {
    this.#stopTimer()
    await this.#renewal
  }

  #stopTimer(): void {
    if (!this.#timer) return

    clearInterval(this.#timer)
    this.#timer = undefined
  }

  async #renewActiveJobs(): Promise<void> {
    if (this.#pool.isEmpty()) return

    const jobsByQueue = this.#pool.activeJobsByQueue()

    for (const queue of this.#queues) {
      const jobs = jobsByQueue.get(queue)
      if (!jobs || jobs.length === 0) continue

      try {
        await this.#wrapInternal(() => this.#adapter.renewJobs(queue, jobs))
      } catch (error) {
        // The next renewal tries again. Until one succeeds, a job running
        // longer than stalledThreshold can be recovered as stalled.
        this.#logger.warn(
          { err: error as Error, queue, workerId: this.#workerId },
          `Failed to renew the leases of ${jobs.length} running job(s) on queue "${queue}"`
        )
      }
    }
  }
}
