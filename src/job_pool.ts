import type { AcquiredJob } from './contracts/adapter.js'

/**
 * Entry representing an active job in the pool.
 */
export interface PoolEntry {
  /** Promise that resolves when the job completes */
  promise: Promise<void>
  /** The acquired job data */
  job: AcquiredJob
  /** The queue this job came from */
  queue: string
}

interface CompletedEntry {
  leaseToken: string
  entry: PoolEntry
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
  /**
   * Running jobs by lease token. A job recovered as stalled can be acquired
   * again while its first execution still runs: both stay in the pool.
   */
  #activeJobs = new Map<string, PoolEntry>()
  /** Consumed slots are cleared at once: an entry holds its job and payload. */
  #completedEntries: Array<CompletedEntry | undefined> = []
  #completedHead = 0
  #completionAvailable?: PromiseWithResolvers<void>

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
   * The pool observes execution failures as soon as ownership is registered.
   * This prevents an unhandled rejection when a cycle consumer pauses before
   * asking for the next completion. The original promise is stored unchanged,
   * so completion waits and shutdown draining still observe its final state.
   *
   * @param job - The acquired job data
   * @param queue - The queue the job came from
   * @param promise - Promise that resolves when the job completes
   */
  add(job: AcquiredJob, queue: string, promise: Promise<void>) {
    const entry = { promise, job, queue }
    this.#activeJobs.set(job.leaseToken, entry)
    void promise.then(
      () => this.#enqueueCompletion(job.leaseToken, entry),
      () => this.#enqueueCompletion(job.leaseToken, entry)
    )
  }

  /**
   * Get all currently running jobs, grouped by the queue they came from.
   *
   * Used by the worker heartbeat to renew the acquired timestamp of in-flight
   * jobs so long-running handlers are not mistaken for stalled jobs.
   *
   * @returns A map of queue name to the jobs running for that queue
   */
  activeJobsByQueue(): Map<string, AcquiredJob[]> {
    const byQueue = new Map<string, AcquiredJob[]>()

    for (const { job, queue } of this.#activeJobs.values()) {
      const jobs = byQueue.get(queue)
      if (jobs) {
        jobs.push(job)
      } else {
        byQueue.set(queue, [job])
      }
    }

    return byQueue
  }

  /**
   * Wait for the next job to complete and return it.
   *
   * Completions are queued in settlement order and remain available until a
   * consumer asks for them. The completed job is removed from the pool.
   *
   * @returns The first job to complete (success or failure)
   */
  async waitForNextCompletion(): Promise<PoolEntry> {
    while (true) {
      while (this.#completedHead >= this.#completedEntries.length) {
        this.#completionAvailable ??= Promise.withResolvers<void>()
        await this.#completionAvailable.promise
      }

      const completed = this.#completedEntries[this.#completedHead]!
      this.#completedEntries[this.#completedHead++] = undefined
      this.#compactCompletedJobs()

      if (this.#activeJobs.get(completed.leaseToken) !== completed.entry) continue

      this.#activeJobs.delete(completed.leaseToken)
      return completed.entry
    }
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
    this.#completedEntries = []
    this.#completedHead = 0
  }

  #enqueueCompletion(leaseToken: string, entry: PoolEntry): void {
    if (this.#activeJobs.get(leaseToken) !== entry) return

    this.#completedEntries.push({ leaseToken, entry })
    this.#completionAvailable?.resolve()
    this.#completionAvailable = undefined
  }

  #compactCompletedJobs(): void {
    if (this.#completedHead === this.#completedEntries.length) {
      this.#completedEntries = []
      this.#completedHead = 0
      return
    }

    if (this.#completedHead < 1_024 || this.#completedHead * 2 < this.#completedEntries.length) {
      return
    }

    this.#completedEntries = this.#completedEntries.slice(this.#completedHead)
    this.#completedHead = 0
  }
}
