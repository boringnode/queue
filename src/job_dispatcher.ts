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
 *     .toQueue('emails')              // optional: target queue
 *     .priority(1)                    // optional: 1-10, lower = higher priority
 *     .in('5m')                       // optional: delay before processing
 *     .dedup({ id: 'order-123' })     // optional: deduplication
 *     .with('redis')                  // optional: specific adapter
 *     .run()                          // dispatch the job
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
  #dedup?: {
    id: string
    ttl?: number
    extend?: boolean
    replace?: boolean
  }

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
   * Configure deduplication for this job.
   *
   * Modes:
   * - **Simple** (`{ id }`): skip duplicates while the job exists.
   * - **Throttle** (`{ id, ttl }`): skip duplicates within a TTL window.
   * - **Extend** (`{ id, ttl, extend: true }`): reset the TTL clock on each duplicate.
   *   The window length stays at the original ttl from the first dispatch.
   * - **Replace** (`{ id, ttl, replace: true }`): swap the payload of the existing
   *   pending/delayed job on duplicate within TTL. Active jobs and retained
   *   completed/failed jobs return `'skipped'`. Only `payload` changes —
   *   priority/queue/delay/groupId are preserved.
   * - **Debounce** (`{ id, ttl, replace: true, extend: true }`): replace + reset TTL.
   *
   * The id is automatically prefixed with the job name to prevent collisions
   * between different job types.
   *
   * @param options.id - Unique deduplication key
   * @param options.ttl - TTL as Duration ('5s', 5000). Required for extend/replace.
   * @param options.extend - Reset the TTL clock on duplicate within window. Window
   *   length stays at the original ttl; this option's `ttl` arg is ignored on extend.
   * @param options.replace - Swap payload of existing pending/delayed job within
   *   window. Active and retained jobs are not modified.
   *
   * @example
   * ```typescript
   * // Simple dedup
   * await SendInvoiceJob.dispatch({ orderId: 123 })
   *   .dedup({ id: 'order-123' })
   *
   * // Throttle: 5 second window
   * await SendEmailJob.dispatch({ to: 'x' })
   *   .dedup({ id: 'welcome', ttl: '5s' })
   *
   * // Debounce: replace payload within window
   * await SaveDraftJob.dispatch({ content: 'latest' })
   *   .dedup({ id: 'draft-42', ttl: '2s', replace: true, extend: true })
   * ```
   */
  dedup(options: { id: string; ttl?: Duration; extend?: boolean; replace?: boolean }): this {
    if (!options.id) {
      throw new Error('Dedup ID must be a non-empty string')
    }

    if (options.id.length > 400) {
      throw new Error('Dedup ID must be 400 characters or less')
    }

    // The stored dedup key is `<jobName>::<id>` and must fit within the
    // adapter storage limit (Knex column is VARCHAR(510)). Reject long
    // combinations early so the failure surfaces at dispatch time rather
    // than at insert.
    const prefixedLength = this.#name.length + 2 + options.id.length
    if (prefixedLength > 510) {
      throw new Error(
        `Dedup ID combined with job name exceeds 510 characters ` +
          `(got ${prefixedLength}). Shorten either the job name or the dedup id.`
      )
    }

    if ((options.extend || options.replace) && options.ttl === undefined) {
      throw new Error('dedup.ttl is required when extend or replace is set')
    }

    let parsedTtl: number | undefined
    if (options.ttl !== undefined) {
      parsedTtl = parse(options.ttl)
      if (!Number.isFinite(parsedTtl) || parsedTtl <= 0) {
        throw new Error('dedup.ttl must be a positive duration')
      }
    }

    this.#dedup = {
      id: options.id,
      ttl: parsedTtl,
      extend: options.extend,
      replace: options.replace,
    }

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
    const dedupId = this.#dedup ? `${this.#name}::${this.#dedup.id}` : undefined

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
      createdAt: Date.now(),
      ...(dedupId
        ? {
            dedup: {
              id: dedupId,
              ttl: this.#dedup!.ttl,
              extend: this.#dedup!.extend,
              replace: this.#dedup!.replace,
            },
          }
        : {}),
    }

    const message: JobDispatchMessage = { jobs: [jobData], queue: this.#queue, delay: parsedDelay }

    let pushResult: { outcome: DispatchResult['deduped']; jobId: string } | undefined
    await dispatchChannel.tracePromise(async () => {
      const result =
        parsedDelay !== undefined
          ? await wrapInternal(() => adapter.pushLaterOn(this.#queue, jobData, parsedDelay))
          : await wrapInternal(() => adapter.pushOn(this.#queue, jobData))

      if (result && typeof result === 'object' && 'outcome' in result) {
        pushResult = { outcome: result.outcome, jobId: result.jobId }
        message.dedupOutcome = result.outcome
      }
    }, message)

    if (pushResult && this.#dedup) {
      return {
        jobId: pushResult.jobId,
        deduped: pushResult.outcome,
      }
    }

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
