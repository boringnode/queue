import { jobDispatchRuntime } from './job_dispatch_runtime.js'
import type { AdapterSelector, DispatchManyResult, JobOptions } from './types/main.js'

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
  readonly #jobOptions?: () => JobOptions
  #queue?: string
  #adapter?: AdapterSelector
  #priority?: number
  #groupId?: string

  /**
   * Create a new batch job dispatcher.
   *
   * @param name - The job class name (used to locate the class at runtime)
   * @param payloads - Array of data to pass to each job
   */
  constructor(name: string, payloads: T[], jobOptions?: () => JobOptions) {
    this.#name = name
    this.#payloads = payloads
    this.#jobOptions = jobOptions
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
  with(adapter: AdapterSelector) {
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
    const jobOptions = this.#jobOptions?.()

    return jobDispatchRuntime.dispatch({
      kind: 'batch',
      name: jobOptions?.name ?? this.#name,
      payloads: this.#payloads,
      jobOptions,
      overrides: {
        queue: this.#queue,
        adapter: this.#adapter,
        priority: this.#priority,
        groupId: this.#groupId,
      },
    })
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
}
