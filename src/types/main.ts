import type { BackoffStrategy as BackoffStrategyClass } from '../strategies/backoff_strategy.js'
import type { Adapter } from '../contracts/adapter.js'
import type { AcquiredJob } from '../contracts/adapter.js'
import type { Logger } from '../logger.js'
import { Job } from '../job.js'

export type { Logger }

/**
 * Duration can be specified as milliseconds (number) or as a human-readable string.
 *
 * Supported string formats: '1s', '5m', '2h', '1d', etc.
 *
 * @example
 * ```typescript
 * const timeout: Duration = '30s'   // 30 seconds
 * const delay: Duration = 5000      // 5000 milliseconds
 * const interval: Duration = '5m'   // 5 minutes
 * ```
 */
export type Duration = number | string

/**
 * Retention policy for completed/failed jobs.
 *
 * - `true` (default): Remove job immediately
 * - `false`: Keep job in history indefinitely
 * - `{ age?, count? }`: Keep with pruning by age and/or count
 */
export type JobRetention =
  | boolean
  | {
      /**
       * Keep jobs newer than this duration.
       */
      age?: Duration

      /**
       * Keep at most this many jobs.
       */
      count?: number
    }

/**
 * Possible statuses for a job in the queue.
 */
export type JobStatus = 'pending' | 'active' | 'delayed' | 'completed' | 'failed'

/**
 * Result returned when dispatching a job.
 *
 * @example
 * ```typescript
 * const { jobId } = await SendEmailJob.dispatch(payload)
 * console.log(`Dispatched job: ${jobId}`)
 * ```
 */
/**
 * Outcome of a dedup-enabled dispatch.
 * - `added`: new job was inserted
 * - `skipped`: duplicate found within TTL, skipped silently
 * - `replaced`: duplicate found within TTL, existing job's payload was replaced
 * - `extended`: duplicate found within TTL, TTL window was reset
 */
export type DedupOutcome = 'added' | 'skipped' | 'replaced' | 'extended'

export interface DispatchResult {
  /** Unique identifier for this specific job instance */
  jobId: string
  /** Dedup outcome (only present when `.dedup()` was used). */
  deduped?: DedupOutcome
}

/**
 * Result returned when dispatching multiple jobs at once.
 *
 * @example
 * ```typescript
 * const { jobIds } = await SendEmailJob.dispatchMany(payloads)
 * console.log(`Dispatched ${jobIds.length} jobs`)
 * ```
 */
export interface DispatchManyResult {
  /** Unique identifiers for all dispatched job instances */
  jobIds: string[]
}

/**
 * Internal representation of a job in the queue.
 *
 * This is used by adapters to store and retrieve job data.
 * Not typically used directly by application code.
 */
export interface JobData {
  /**
   * Unique identifier for this job.
   */
  id: string

  /**
   * Job class name.
   */
  name: string

  /**
   * Serialized job payload.
   */
  payload: any

  /**
   * Number of execution attempts so far.
   */
  attempts: number

  /**
   * Job priority (lower = higher priority).
   *
   * @default 5
   */
  priority?: number

  /**
   * When to retry this job next (for failed jobs).
   */
  nextRetryAt?: Date

  /**
   * Number of times this job was recovered from stalled state.
   */
  stalledCount?: number

  /**
   * Optional group identifier for organizing related jobs.
   *
   * Jobs with the same groupId can be filtered and displayed together
   * in monitoring UIs. Useful for batch operations like newsletters
   * or bulk exports.
   *
   * @example
   * ```typescript
   * await SendEmailJob.dispatch({ to: 'user@example.com' })
   *   .group('newsletter-jan-2025')
   *   .run()
   * ```
   */
  groupId?: string

  /**
   * Timestamp (ms) when the job was dispatched.
   * Used to compute queue wait time in OTel instrumentation.
   */
  createdAt?: number

  /**
   * Serialized trace context for distributed tracing.
   * Injected by OTel plugin at dispatch time.
   */
  traceContext?: Record<string, string>

  /**
   * Deduplication configuration for this job.
   * When set, adapters apply dedup semantics keyed on `dedup.id`.
   * Set automatically when `.dedup()` is called on the dispatcher.
   */
  dedup?: {
    /**
     * Dedup key, prefixed with the job name (e.g. `SendInvoiceJob::order-123`).
     * The combined `<jobName>::<id>` length is capped at 510 characters by the
     * Knex storage column. Dispatcher validates this at `.dedup()` time.
     */
    id: string
    /**
     * TTL in milliseconds (must be positive). When set, dedup lock auto-expires
     * after TTL. After expiry, the same dedup id produces a brand-new job
     * (coexists with prior). Omit `ttl` entirely for a no-expiry lock that
     * persists until the job is removed.
     */
    ttl?: number
    /**
     * Reset the TTL clock when a duplicate arrives within the window.
     * The window length stays at the original ttl — passing a different
     * `ttl` on the duplicating dispatch does not resize the window.
     */
    extend?: boolean
    /**
     * Swap the payload of the existing pending or delayed job when a
     * duplicate arrives within the TTL window. Active jobs and retained
     * completed/failed jobs return `'skipped'` without mutation. Only
     * `payload` is swapped — priority, queue, delay, and groupId of the
     * existing job are preserved.
     */
    replace?: boolean
  }
}

/**
 * Record of a job's current state, including history for completed/failed jobs.
 */
export interface JobRecord {
  /** Current status of the job */
  status: JobStatus
  /** Original job data */
  data: JobData
  /** Timestamp when the job finished (for completed/failed jobs) */
  finishedAt?: number
  /** Error message (for failed jobs) */
  error?: string
}

/**
 * Static options for a Job class.
 *
 * Define these as a static property on your Job class to configure
 * default behavior for all instances.
 *
 * @example
 * ```typescript
 * class SendEmailJob extends Job<EmailPayload> {
 *   static options: JobOptions = {
 *     name: 'SendEmailJob',
 *     queue: 'emails',
 *     maxRetries: 3,
 *     timeout: '30s',
 *   }
 * }
 * ```
 */
export interface JobOptions {
  /**
   * Unique name for this job class.
   *
   * Used to identify the job when dispatching and processing.
   *
   * @default constructor.name
   */
  name?: string

  /**
   * Queue name for this job.
   *
   * @default 'default'
   */
  queue?: string

  /**
   * Adapter name or factory to use for this job.
   *
   * Defaults to the queue manager's configured default adapter.
   */
  adapter?: string | (() => Adapter)

  /**
   * Maximum retry attempts before permanent failure.
   *
   * This is a convenience alias for `retry.maxRetries`.
   *
   * @default 0
   */
  maxRetries?: number

  /**
   * Job priority (lower = higher priority).
   *
   * @default 5
   */
  priority?: number

  /**
   * Retry configuration for this job.
   *
   * Overrides queue-level and global retry settings.
   */
  retry?: RetryConfig

  /**
   * Maximum execution time before timeout.
   *
   * @default undefined (no timeout)
   */
  timeout?: Duration

  /**
   * Whether to mark job as failed on timeout.
   *
   * When disabled, timed out jobs follow the normal retry policy.
   *
   * @default false
   */
  failOnTimeout?: boolean

  /**
   * Retention policy for completed jobs.
   *
   * By default, completed jobs are removed immediately.
   */
  removeOnComplete?: JobRetention

  /**
   * Retention policy for failed jobs.
   *
   * By default, failed jobs are removed immediately after the failure hooks run.
   */
  removeOnFail?: JobRetention
}

/**
 * Context information available to a job during execution.
 *
 * Provides metadata about the current job execution, including
 * retry information, queue details, and timing.
 *
 * @example
 * ```typescript
 * class MyJob extends Job<Payload> {
 *   async execute() {
 *     console.log(`Attempt ${this.context.attempt} of job ${this.context.jobId}`)
 *     console.log(`Running on queue: ${this.context.queue}`)
 *   }
 * }
 * ```
 */
export interface JobContext {
  /** Unique identifier for this job */
  jobId: string

  /** Job class name */
  name: string

  /** Current attempt number (1-based: first attempt = 1) */
  attempt: number

  /** Queue name this job is being processed from */
  queue: string

  /** Job priority (lower number = higher priority) */
  priority: number

  /** When this job was acquired by the worker for processing */
  acquiredAt: Date

  /** Number of times this job has been recovered from stalled state */
  stalledCount: number
}

/**
 * Type representing a Job class constructor.
 *
 * The constructor accepts any arguments for dependency injection.
 * Payload and context are provided separately via `$hydrate()`.
 */
export type JobClass<T extends Job = Job> = (new (...args: unknown[]) => T) & {
  options?: JobOptions
}

/**
 * Factory function for custom job instantiation.
 *
 * Use this to integrate with IoC containers for dependency injection.
 * The factory receives only the job class and should return an instance
 * with all dependencies injected. The worker will call `$hydrate()` separately
 * to provide payload, context, and signal.
 *
 * @param JobClass - The job class to instantiate
 * @returns The job instance, or a Promise resolving to the instance
 *
 * @example
 * ```typescript
 * // With AdonisJS IoC container
 * await QueueManager.init({
 *   default: 'redis',
 *   adapters: { redis: redis() },
 *   jobFactory: async (JobClass) => {
 *     return app.container.make(JobClass)
 *   }
 * })
 * ```
 */
export type JobFactory = (JobClass: JobClass) => Job | Promise<Job>

/**
 * Retry policy used by jobs, queues, or the queue manager.
 */
export interface RetryConfig {
  /**
   * Number of retry attempts after the first failed execution.
   *
   * Set to `0` to disable retries.
   *
   * @default 0
   */
  maxRetries?: number

  /**
   * Factory that creates the backoff strategy used between retry attempts.
   *
   * If omitted, failed jobs are retried as soon as the adapter makes them
   * available again.
   */
  backoff?: () => BackoffStrategyClass
}

/**
 * Built-in retry delay algorithms.
 */
export type BackoffStrategy = 'exponential' | 'linear' | 'fixed'

/**
 * Configuration for built-in and custom retry backoff strategies.
 */
export interface BackoffConfig {
  /**
   * Strategy used to compute the delay before the next retry.
   */
  strategy: BackoffStrategy

  /**
   * Initial delay used by the strategy.
   */
  baseDelay: Duration

  /**
   * Upper bound for computed retry delays.
   */
  maxDelay?: Duration

  /**
   * Growth factor for exponential backoff.
   */
  multiplier?: number

  /**
   * Whether to randomize retry delays to avoid retry bursts.
   */
  jitter?: boolean
}

/**
 * Runtime configuration for a named queue.
 */
export interface QueueConfig {
  /**
   * Adapter name used by jobs dispatched to this queue.
   *
   * Falls back to the queue manager's default adapter.
   */
  adapter?: string

  /**
   * Retry policy applied to jobs in this queue unless overridden by job options.
   */
  retry?: RetryConfig

  /**
   * Default job options applied to jobs in this queue unless overridden by the job.
   */
  defaultJobOptions?: JobOptions
}

/**
 * Runtime options for workers that poll queues and execute jobs.
 */
export interface WorkerConfig {
  /**
   * Maximum number of jobs to process concurrently.
   * @default 1
   */
  concurrency?: number

  /**
   * Delay between queue polls when idle (no jobs available).
   * @default '2s'
   */
  idleDelay?: Duration

  /**
   * Maximum duration a job can run before being timed out.
   * Can be overridden per job via JobOptions.timeout.
   * @default undefined (no timeout)
   */
  timeout?: Duration

  /**
   * Duration after which an active job is considered stalled.
   * A stalled job is one that was acquired but the worker stopped
   * responding (e.g., due to a crash).
   * @default '30s'
   */
  stalledThreshold?: Duration

  /**
   * How often to check for stalled jobs.
   * @default '30s'
   */
  stalledInterval?: Duration

  /**
   * Maximum number of times a job can be recovered from stalled state
   * before being marked as failed permanently.
   * @default 1
   */
  maxStalledCount?: number

  /**
   * Whether to automatically stop the worker on SIGINT/SIGTERM signals.
   * When enabled, the worker will wait for running jobs to complete
   * before stopping.
   * @default true
   */
  gracefulShutdown?: boolean

  /**
   * Callback invoked when a shutdown signal is received.
   * Called before the worker starts stopping.
   */
  onShutdownSignal?: () => void | Promise<void>
}

/**
 * Event yielded by the low-level worker processing generator.
 */
export type WorkerCycle =
  | {
      /** A job was acquired and execution started. */
      type: 'started'
      queue: string
      job: JobData
    }
  | {
      /** A running job finished, either successfully or after failure handling. */
      type: 'completed'
      queue: string
      job: JobData
    }
  | {
      /** No work was available. Consumers should wait before polling again. */
      type: 'idle'
      suggestedDelay: Duration
    }
  | {
      /** An unexpected worker loop error occurred. */
      type: 'error'
      error: Error
      suggestedDelay: Duration
    }

/**
 * Factory used to lazily create adapter instances.
 */
export type AdapterFactory<T extends Adapter = Adapter> = () => T

/**
 * Status of a schedule.
 */
export type ScheduleStatus = 'active' | 'paused'

/**
 * Configuration for creating a schedule.
 * Used by ScheduleBuilder to collect schedule options before creation.
 */
export interface ScheduleConfig {
  /** Optional ID for the schedule (UUID if not set). Used for upsert. */
  id?: string

  /** Job class name */
  name: string

  /** Job payload */
  payload: any

  /** Cron expression (mutually exclusive with everyMs) */
  cronExpression?: string

  /** Interval in milliseconds (mutually exclusive with cronExpression) */
  everyMs?: number

  /** IANA timezone for cron evaluation */
  timezone: string

  /** Start boundary - no jobs dispatched before this */
  from?: Date

  /** End boundary - no jobs dispatched after this */
  to?: Date

  /** Maximum number of runs (null = unlimited) */
  limit?: number
}

/**
 * Persisted schedule data.
 * Represents a schedule stored in the adapter.
 */
export interface ScheduleData {
  /** Unique identifier */
  id: string

  /** Job class name */
  name: string

  /** Job payload */
  payload: any

  /** Cron expression (null if using interval) */
  cronExpression: string | null

  /** Interval in milliseconds (null if using cron) */
  everyMs: number | null

  /** IANA timezone */
  timezone: string

  /** Start boundary - no jobs dispatched before this */
  from: Date | null

  /** End boundary - no jobs dispatched after this */
  to: Date | null

  /** Maximum number of runs */
  limit: number | null

  /** Number of times this schedule has run */
  runCount: number

  /** Next scheduled run time */
  nextRunAt: Date | null

  /** Last run time */
  lastRunAt: Date | null

  /** Current status */
  status: ScheduleStatus

  /** When the schedule was created */
  createdAt: Date
}

/**
 * Result returned when creating a schedule.
 */
export interface ScheduleResult {
  /** Unique identifier for the schedule */
  scheduleId: string
}

/**
 * Options for listing schedules.
 */
export interface ScheduleListOptions {
  /** Filter by status */
  status?: ScheduleStatus
}

export interface QueueManagerConfig {
  /**
   * Name of the adapter used when a job does not select one explicitly.
   *
   * Must match one of the keys from `adapters`.
   */
  default: string

  /**
   * Available queue adapters keyed by name.
   *
   * Adapters are lazy-instantiated the first time they are used.
   */
  adapters: Record<string, AdapterFactory>

  /**
   * Global retry configuration applied to all jobs unless overridden by
   * queue-level or job-level options.
   */
  retry?: RetryConfig

  /**
   * Global job options applied to all jobs unless overridden by queue-level
   * or job-level options.
   */
  defaultJobOptions?: JobOptions

  /**
   * Per-queue configuration keyed by queue name.
   *
   * Use this to select adapters or defaults for specific queues.
   */
  queues?: Record<string, QueueConfig>

  /**
   * Worker runtime options used by `Worker` instances.
   */
  worker?: WorkerConfig

  /**
   * Glob patterns used to discover and register job classes.
   *
   * These locations are used by `init()` when `autoLoadJobs` is enabled,
   * and by `QueueManager.loadJobs()` when called without arguments.
   */
  locations?: string[]

  /**
   * Whether `init()` should immediately register jobs from configured locations.
   *
   * Framework integrations may disable this to defer job loading until a
   * command lifecycle is ready, then call `QueueManager.loadJobs()`.
   *
   * @default true
   */
  autoLoadJobs?: boolean

  /**
   * Logger used by the queue runtime.
   *
   * Defaults to the console logger.
   */
  logger?: Logger

  /**
   * Custom factory function for job instantiation.
   *
   * Use this to integrate with IoC containers for dependency injection.
   * When provided, this factory is called instead of `new JobClass()`.
   * The worker will call `$hydrate()` on the returned instance to provide
   * payload, context, and signal.
   *
   * @example
   * ```typescript
   * await QueueManager.init({
   *   default: 'redis',
   *   adapters: { redis: redis() },
   *   jobFactory: async (JobClass) => {
   *     return app.container.make(JobClass)
   *   }
   * })
   * ```
   */
  jobFactory?: JobFactory

  /**
   * Wraps internal adapter operations (Redis, Knex calls) to suppress
   * or customize instrumentation. Used by OTel to suppress child spans.
   */
  internalOperationWrapper?: <T>(fn: () => Promise<T>) => Promise<T>

  /**
   * Wraps job execution to inject tracing context or custom behavior.
   * Called around `runtime.execute()` for each job attempt.
   */
  executionWrapper?: <T>(fn: () => Promise<T>, job: AcquiredJob, queue: string) => Promise<T>
}
