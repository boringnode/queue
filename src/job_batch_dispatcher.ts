import debug from './debug.js'
import { randomUUID } from 'node:crypto'
import { QueueManager } from './queue_manager.js'
import { dispatchChannel } from './tracing_channels.js'
import type { Adapter } from './contracts/adapter.js'
import type { DispatchManyResult } from './types/main.js'
import type { JobDispatchMessage } from './types/tracing_channels.js'

/**
 * Fluent builder for dispatching multiple jobs to the queue in a single batch.
 *
 * Provides a chainable API for configuring job options before dispatch.
 * Usually created via `Job.dispatchMany()` rather than directly.
 *
 * ```
 * Job.dispatchMany(payloads)
 *     .toQueue('emails')     // optional: target queue
 *     .priority(1)           // optional: 1-10, lower = higher priority
 *     .group('batch-123')    // optional: group all jobs together
 *     .with('redis')         // optional: specific adapter
 *     .run()                 // dispatch all jobs
 * ```
 *
 * @typeParam T - The payload type for these jobs
 *
 * @example
 * ```typescript
 * // Batch dispatch for newsletter
 * const { jobIds } = await SendEmailJob.dispatchMany([
 *   { to: 'user1@example.com', subject: 'Newsletter' },
 *   { to: 'user2@example.com', subject: 'Newsletter' },
 * ])
 *   .group('newsletter-jan-2025')
 *   .toQueue('emails')
 *   .run()
 *
 * console.log(`Dispatched ${jobIds.length} jobs`)
 * ```
 */
export class JobBatchDispatcher<T> {
  readonly #name: string
  readonly #payloads: T[]
  #queue: string = 'default'
  #adapter?: string | (() => Adapter)
  #priority?: number
  #groupId?: string

  /**
   * Create a new batch job dispatcher.
   *
   * @param name - The job class name (used to locate the class at runtime)
   * @param payloads - Array of data to pass to each job
   */
  constructor(name: string, payloads: T[]) {
    this.#name = name
    this.#payloads = payloads
  }

  /**
   * Set the target queue for all jobs.
   *
   * @param queue - Queue name (default: 'default')
   * @returns This dispatcher for chaining
   *
   * @example
   * ```typescript
   * await SendEmailJob.dispatchMany(payloads).toQueue('emails')
   * ```
   */
  toQueue(queue: string): this {
    this.#queue = queue

    return this
  }

  /**
   * Set the priority for all jobs.
   *
   * Lower numbers = higher priority. Jobs with lower priority values
   * are processed before jobs with higher values.
   *
   * @param priority - Priority level (1-10, default: 5)
   * @returns This dispatcher for chaining
   *
   * @example
   * ```typescript
   * await UrgentJob.dispatchMany(payloads).priority(1)
   * ```
   */
  priority(priority: number): this {
    this.#priority = priority

    return this
  }

  /**
   * Assign all jobs to a group.
   *
   * Jobs with the same groupId can be filtered and displayed together
   * in monitoring UIs. Useful for batch operations like newsletters
   * or bulk exports.
   *
   * @param groupId - Group identifier
   * @returns This dispatcher for chaining
   *
   * @example
   * ```typescript
   * await SendEmailJob.dispatchMany(recipients)
   *   .group('newsletter-jan-2025')
   *   .run()
   * ```
   */
  group(groupId: string): this {
    this.#groupId = groupId

    return this
  }

  /**
   * Use a specific adapter for these jobs.
   *
   * @param adapter - Adapter name or factory function
   * @returns This dispatcher for chaining
   *
   * @example
   * ```typescript
   * await Job.dispatchMany(payloads).with('redis')
   * ```
   */
  with(adapter: string | (() => Adapter)) {
    this.#adapter = adapter

    return this
  }

  /**
   * Dispatch all jobs to the queue.
   *
   * @returns A DispatchManyResult containing all jobIds
   *
   * @example
   * ```typescript
   * const { jobIds } = await SendEmailJob.dispatchMany(payloads).run()
   * console.log(`Dispatched ${jobIds.length} jobs`)
   * ```
   */
  async run(): Promise<DispatchManyResult> {
    if (this.#payloads.length === 0) return { jobIds: [] }

    debug('dispatching %d jobs of type %s', this.#payloads.length, this.#name)

    const adapter = this.#getAdapterInstance()
    const wrapInternal = QueueManager.getInternalOperationWrapper()

    const jobs = this.#payloads.map((payload) => ({
      id: randomUUID(),
      name: this.#name,
      payload,
      attempts: 0,
      priority: this.#priority,
      groupId: this.#groupId,
    }))

    const message: JobDispatchMessage = { jobs, queue: this.#queue }


    await dispatchChannel.tracePromise(async () => {
      await wrapInternal(() => adapter.pushManyOn(this.#queue, jobs))
    }, message)

    return { jobIds: jobs.map((job) => job.id) }
  }

  /**
   * Thenable implementation for auto-dispatch when awaited.
   *
   * Allows `await Job.dispatchMany(payloads)` without explicit `.run()`.
   *
   * @param onFulfilled - Success callback
   * @param onRejected - Error callback
   * @returns Promise resolving to the DispatchManyResult
   */
  then<TResult1 = DispatchManyResult, TResult2 = never>(
    onFulfilled?: ((value: DispatchManyResult) => TResult1 | PromiseLike<TResult1>) | null,
    onRejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
  ): Promise<TResult1 | TResult2> {
    return this.run().then(onFulfilled, onRejected)
  }

  #getAdapterInstance(): Adapter {
    if (!this.#adapter) {
      return QueueManager.use()
    }

    if (typeof this.#adapter === 'string') {
      return QueueManager.use(this.#adapter)
    }

    return this.#adapter()
  }
}
