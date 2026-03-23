import debug from './debug.js'
import { randomUUID } from 'node:crypto'
import { QueueManager } from './queue_manager.js'
import { dispatchChannel } from './tracing_channels.js'
import type { Adapter } from './contracts/adapter.js'
import type { DispatchResult, Duration } from './types/main.js'
import type { JobDispatchMessage } from './types/tracing_channels.js'
import { parse } from './utils.js'

/**
 * Fluent builder for dispatching jobs to the queue.
 *
 * Provides a chainable API for configuring job options before dispatch.
 * Usually created via `Job.dispatch()` rather than directly.
 *
 * ```
 * Job.dispatch(payload)
 *     .toQueue('emails')     // optional: target queue
 *     .priority(1)           // optional: 1-10, lower = higher priority
 *     .in('5m')              // optional: delay before processing
 *     .with('redis')         // optional: specific adapter
 *     .run()                 // dispatch the job
 * ```
 *
 * @typeParam T - The payload type for this job
 *
 * @example
 * ```typescript
 * // Simple dispatch (auto-runs via thenable)
 * await SendEmailJob.dispatch({ to: 'user@example.com', subject: 'Hello' })
 *
 * // With options
 * const jobId = await SendEmailJob.dispatch({ to: 'user@example.com' })
 *   .toQueue('high-priority')
 *   .priority(1)
 *   .run()
 *
 * // Delayed job
 * await ReminderJob.dispatch({ userId: 123 }).in('24h')
 * ```
 */
export class JobDispatcher<T> {
  readonly #name: string
  readonly #payload: T
  #queue: string = 'default'
  #adapter?: string | (() => Adapter)
  #delay?: Duration
  #priority?: number
  #groupId?: string

  /**
   * Create a new job dispatcher.
   *
   * @param name - The job class name (used to locate the class at runtime)
   * @param payload - The data to pass to the job
   */
  constructor(name: string, payload: T) {
    this.#name = name
    this.#payload = payload
  }

  /**
   * Set the target queue for this job.
   *
   * @param queue - Queue name (default: 'default')
   * @returns This dispatcher for chaining
   *
   * @example
   * ```typescript
   * await SendEmailJob.dispatch(payload).toQueue('emails')
   * ```
   */
  toQueue(queue: string): this {
    this.#queue = queue

    return this
  }

  /**
   * Delay the job execution.
   *
   * The job will be stored in a delayed state and moved to pending
   * after the delay expires.
   *
   * @param delay - Delay as milliseconds or duration string ('5s', '1h', '7d')
   * @returns This dispatcher for chaining
   *
   * @example
   * ```typescript
   * // Send reminder in 24 hours
   * await ReminderJob.dispatch(payload).in('24h')
   *
   * // Process in 5 minutes
   * await CleanupJob.dispatch(payload).in('5m')
   * ```
   */
  in(delay: Duration): this {
    this.#delay = delay

    return this
  }

  /**
   * Set the job priority.
   *
   * Lower numbers = higher priority. Jobs with lower priority values
   * are processed before jobs with higher values.
   *
   * @param priority - Priority level (1-10, default: 5)
   * @returns This dispatcher for chaining
   *
   * @example
   * ```typescript
   * // High priority job
   * await UrgentJob.dispatch(payload).priority(1)
   *
   * // Low priority job
   * await BackgroundJob.dispatch(payload).priority(10)
   * ```
   */
  priority(priority: number): this {
    this.#priority = priority

    return this
  }

  /**
   * Assign this job to a group.
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
   * // Group newsletter jobs together
   * await SendEmailJob.dispatch({ to: 'user@example.com' })
   *   .group('newsletter-jan-2025')
   *   .run()
   * ```
   */
  group(groupId: string): this {
    this.#groupId = groupId

    return this
  }

  /**
   * Use a specific adapter for this job.
   *
   * @param adapter - Adapter name or factory function
   * @returns This dispatcher for chaining
   *
   * @example
   * ```typescript
   * // Use named adapter
   * await Job.dispatch(payload).with('redis')
   *
   * // Use custom adapter instance
   * await Job.dispatch(payload).with(() => new CustomAdapter())
   * ```
   */
  with(adapter: string | (() => Adapter)) {
    this.#adapter = adapter

    return this
  }

  /**
   * Dispatch the job to the queue.
   *
   * @returns A DispatchResult containing the jobId
   *
   * @example
   * ```typescript
   * const { jobId } = await SendEmailJob.dispatch(payload).run()
   * console.log(`Dispatched job: ${jobId}`)
   * ```
   */
  async run(): Promise<DispatchResult> {
    const id = randomUUID()

    debug('dispatching job %s with id %s using payload %s', this.#name, id, this.#payload)

    const adapter = this.#getAdapterInstance()
    const wrapInternal = QueueManager.getInternalOperationWrapper()
    const parsedDelay = this.#delay ? parse(this.#delay) : undefined

    const jobData = {
      id,
      name: this.#name,
      payload: this.#payload,
      attempts: 0,
      priority: this.#priority,
      groupId: this.#groupId,
    }

    const message: JobDispatchMessage = { jobs: [jobData], queue: this.#queue, delay: parsedDelay }

    await dispatchChannel.tracePromise(async () => {
      if (parsedDelay !== undefined) {
        await wrapInternal(() => adapter.pushLaterOn(this.#queue, jobData, parsedDelay))
      } else {
        await wrapInternal(() => adapter.pushOn(this.#queue, jobData))
      }
    }, message)

    return { jobId: id }
  }

  /**
   * Thenable implementation for auto-dispatch when awaited.
   *
   * Allows `await Job.dispatch(payload)` without explicit `.run()`.
   *
   * @param onFulfilled - Success callback
   * @param onRejected - Error callback
   * @returns Promise resolving to the DispatchResult
   */
  then<TResult1 = DispatchResult, TResult2 = never>(
    onFulfilled?: ((value: DispatchResult) => TResult1 | PromiseLike<TResult1>) | null,
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
