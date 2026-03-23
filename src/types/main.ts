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
export type JobRetention = boolean | { age?: Duration; count?: number }

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
export interface DispatchResult {
  /** Unique identifier for this specific job instance */
  jobId: string
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
   * @default 0
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
   * Serialized trace context for distributed tracing.
   * Injected by OTel plugin at dispatch time.
   */
  traceContext?: Record<string, string>
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
   */
  adapter?: string | (() => Adapter)

  /**
   * Maximum retry attempts before permanent failure.
   *
   * @default 3
   */
  maxRetries?: number

  /**
   * Job priority (lower = higher priority).
   *
   * @default 0
   */
  priority?: number

  /**
   * Retry configuration (backoff strategy, delays, etc.).
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
   * @default true
   */
  failOnTimeout?: boolean
  removeOnComplete?: JobRetention
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

export interface RetryConfig {
  maxRetries?: number
  backoff?: () => BackoffStrategyClass
}

export type BackoffStrategy = 'exponential' | 'linear' | 'fixed'

export interface BackoffConfig {
  strategy: BackoffStrategy
  baseDelay: Duration
  maxDelay?: Duration
  multiplier?: number
  jitter?: boolean
}

export interface QueueConfig {
  adapter?: string
  retry?: RetryConfig
  defaultJobOptions?: JobOptions
}

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

export type WorkerCycle =
  | { type: 'started'; queue: string; job: JobData }
  | { type: 'completed'; queue: string; job: JobData }
  | { type: 'idle'; suggestedDelay: Duration }
  | { type: 'error'; error: Error; suggestedDelay: Duration }

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
  default: string
  adapters: Record<string, AdapterFactory>
  retry?: RetryConfig
  defaultJobOptions?: JobOptions
  queues?: Record<string, QueueConfig>
  worker?: WorkerConfig
  locations?: string[]
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
