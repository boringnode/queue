import type { AcquiredJob } from './contracts/adapter.js'

/**
 * Entry representing an active job in the pool.
 */
interface PoolEntry {
  /** Promise that resolves when the job completes */
  promise: Promise<void>
  /** The acquired job data */
  job: AcquiredJob
  /** The queue this job came from */
  queue: string
}

/**
 * Manages concurrent job execution with a fixed pool size.
 *
 * The pool tracks running jobs and returns the first one to complete,
 * allowing maximum throughput regardless of individual job duration:
 *
 * ```
 * Job A: ████████████████████░░░░░░░░░░  (slow - 10s)
 * Job B: ████ done                       (fast - 100ms) ← returns first
 * Job C: ████████████░░░░░░░░░░░░░░░░░░  (medium - 2s)
 *             ↑
 *      Slot freed, new job can start immediately
 * ```
 *
 * Key insight: slow jobs don't block the pool. As soon as any job
 * completes, its slot becomes available for new work.
 */
export class JobPool {
  #activeJobs = new Map<string, PoolEntry>()

  /** Number of currently running jobs */
  get size() {
    return this.#activeJobs.size
  }

  /**
   * Check if the pool has no running jobs.
   *
   * @returns True if no jobs are running
   */
  isEmpty() {
    return this.#activeJobs.size === 0
  }

  /**
   * Check if the pool can accept more jobs.
   *
   * @param concurrency - Maximum number of concurrent jobs
   * @returns True if there's room for more jobs
   */
  hasCapacity(concurrency: number) {
    return this.#activeJobs.size < concurrency
  }

  /**
   * Add a job to the pool.
   *
   * @param job - The acquired job data
   * @param queue - The queue the job came from
   * @param promise - Promise that resolves when the job completes
   */
  add(job: AcquiredJob, queue: string, promise: Promise<void>) {
    // Observe failures immediately so a consumer may pause after the `started`
    // cycle without exposing the process to an unhandled rejection. The original
    // promise remains rejected for waitForNextCompletion() and drain().
    void promise.catch(() => {})
    this.#activeJobs.set(job.id, { promise, job, queue })
  }

  /**
   * Get the ids of all currently running jobs, grouped by the queue they
   * came from.
   *
   * Used by the worker heartbeat to renew the acquired timestamp of in-flight
   * jobs so long-running handlers are not mistaken for stalled jobs.
   *
   * @returns A map of queue name to the job ids running for that queue
   */
  activeJobIdsByQueue(): Map<string, string[]> {
    const byQueue = new Map<string, string[]>()

    for (const { job, queue } of this.#activeJobs.values()) {
      const ids = byQueue.get(queue)
      if (ids) {
        ids.push(job.id)
      } else {
        byQueue.set(queue, [job.id])
      }
    }

    return byQueue
  }

  /**
   * Wait for the next job to complete and return it.
   *
   * Uses `Promise.race()` internally, so the fastest job wins.
   * The completed job is removed from the pool.
   *
   * @returns The first job to complete (success or failure)
   */
  async waitForNextCompletion(): Promise<PoolEntry> {
    const completedJobId = await Promise.race(
      [...this.#activeJobs.entries()].map(async ([id, { promise }]) => {
        try {
          await promise
        } catch {
          // Errors are handled in Worker#execute
        }
        return id
      })
    )

    const completed = this.#activeJobs.get(completedJobId)!
    this.#activeJobs.delete(completedJobId)

    return completed
  }

  /**
   * Wait for all running jobs to complete.
   *
   * Used during graceful shutdown to ensure no jobs are abandoned.
   * Clears the pool after all jobs finish.
   */
  async drain(): Promise<void> {
    const promises = [...this.#activeJobs.values()].map(async ({ promise }) => {
      try {
        await promise
      } catch {
        // Errors are handled in Worker#execute
      }
    })

    await Promise.all(promises)
    this.#activeJobs.clear()
  }
}
