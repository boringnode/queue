import debug from './debug.js'
import type { Adapter } from './contracts/adapter.js'
import type { JobPool } from './job_pool.js'

export type HeartbeatOperationWrapper = <T>(operation: () => Promise<T>) => Promise<T>

export interface WorkerHeartbeatOptions {
  workerId: string
  queues: readonly string[]
  interval: number
  pool: JobPool
  adapter: Adapter
  wrapInternal: HeartbeatOperationWrapper
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

  #timer?: NodeJS.Timeout
  #renewal?: Promise<void>

  constructor(options: WorkerHeartbeatOptions) {
    this.#workerId = options.workerId
    this.#queues = Object.freeze([...options.queues])
    this.#interval = options.interval
    this.#pool = options.pool
    this.#adapter = options.adapter
    this.#wrapInternal = options.wrapInternal
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

    const jobIdsByQueue = this.#pool.activeJobIdsByQueue()

    for (const queue of this.#queues) {
      const jobIds = jobIdsByQueue.get(queue)
      if (!jobIds || jobIds.length === 0) continue

      try {
        await this.#wrapInternal(() => this.#adapter.renewJobs(queue, jobIds))
      } catch (error) {
        debug('worker %s: failed to renew jobs on queue %s: %O', this.#workerId, queue, error)
      }
    }
  }
}
