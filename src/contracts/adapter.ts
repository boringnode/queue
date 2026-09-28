import type {
  DedupOutcome,
  JobData,
  JobRecord,
  JobRetention,
  ScheduleConfig,
  ScheduleData,
  ScheduleListOptions,
} from '../types/main.js'

/**
 * Result of a push operation when dedup was involved.
 * `outcome` tells the dispatcher what happened; `jobId` is the ID of the
 * existing job when deduped (skipped/replaced/extended).
 */
export interface PushResult {
  outcome: DedupOutcome
  /** ID of the existing job when a duplicate was detected, otherwise the newly added job's id. */
  jobId: string
}

/**
 * A job that has been acquired by a worker for processing.
 * Extends JobData with the timestamp when the job was acquired.
 */
export interface AcquiredJob extends JobData {
  /** Timestamp (in ms) when the job was acquired by the worker */
  acquiredAt: number

  /**
   * Token of this acquisition. The adapter stores it with the active job and
   * generates a new one each time the job is acquired again. Only the holder
   * of the current token can renew, complete, fail, or retry the job.
   */
  leaseToken: string
}

/**
 * The part of an acquired job that identifies its lease.
 */
export type JobLease = Pick<AcquiredJob, 'id' | 'leaseToken'>

/**
 * Result of a stalled job recovery pass.
 */
export interface StalledJobsRecovery {
  /** Number of stalled jobs moved back to pending */
  recovered: number

  /**
   * Stalled jobs that exceeded `maxStalledCount`. They stay active, reacquired
   * with a new lease token by the calling worker, which fails them through the
   * regular failure path.
   */
  exceeded: AcquiredJob[]
}

/**
 * Adapter interface for queue storage backends.
 *
 * Implementations handle job persistence, atomic operations, and
 * concurrency control. Built-in adapters: Redis, Knex, Kysely, and Sync.
 *
 * @example
 * ```typescript
 * import { redis } from '@boringnode/queue/drivers/redis_adapter'
 *
 * const config = {
 *   default: 'redis',
 *   adapters: {
 *     redis: redis({ host: 'localhost', port: 6379 })
 *   }
 * }
 * ```
 */
export interface Adapter {
  /**
   * Set the worker ID for this adapter instance.
   * Required before calling pop methods when consuming jobs. Lease tokens
   * start with it, which tells which worker holds an active job.
   *
   * @param workerId - Unique identifier for the worker
   */
  setWorkerId(workerId: string): void

  /**
   * Pop the next available job from the default queue.
   * Atomically moves the job from pending to active state.
   *
   * @returns The acquired job, or null if queue is empty
   */
  pop(): Promise<AcquiredJob | null>

  /**
   * Pop the next available job from a specific queue.
   * Atomically moves the job from pending to active state.
   *
   * @param queue - The queue name to pop from
   * @returns The acquired job, or null if queue is empty
   */
  popFrom(queue: string): Promise<AcquiredJob | null>

  /**
   * Recover stalled jobs that have been active for too long.
   * A stalled job is one where the worker stopped responding (e.g., crash).
   *
   * Jobs within maxStalledCount are moved back to pending.
   * Jobs exceeding maxStalledCount are not removed: up to `maxExceeded` of
   * them are reacquired atomically by the calling worker (new lease token,
   * acquiredAt set to now) and returned so the worker can fail them. The others are left
   * untouched, still stalled, for a later recovery. If the worker crashes
   * before failing a returned job, it stalls again and comes back later.
   *
   * @param queue - The queue to check for stalled jobs
   * @param stalledThreshold - Duration in ms after which a job is considered stalled
   * @param maxStalledCount - Maximum times a job can be recovered before failing
   * @param maxExceeded - Maximum number of exceeded jobs to reacquire in this pass
   * @returns The number of recovered jobs and the reacquired exceeded jobs
   */
  recoverStalledJobs(
    queue: string,
    stalledThreshold: number,
    maxStalledCount: number,
    maxExceeded: number
  ): Promise<StalledJobsRecovery>

  /**
   * Renew the acquired timestamp of in-flight jobs (heartbeat).
   *
   * A worker calls this periodically for the jobs it is actively processing
   * so that long-running handlers are not mistaken for stalled jobs and
   * re-delivered while they are still running. Only jobs that are still active
   * under the given lease token are renewed; jobs that have already been
   * recovered/completed, or have since been acquired again, are skipped. This
   * prevents a slow execution from resurrecting a job or sabotaging the
   * recovery of the execution that holds it now with a late heartbeat.
   *
   * @param queue - The queue the jobs belong to
   * @param jobs - The leases of the jobs currently being processed
   * @returns Number of jobs whose timestamp was renewed
   */
  renewJobs(queue: string, jobs: JobLease[]): Promise<number>

  /**
   * Mark a job as completed and remove it from the queue.
   *
   * Does nothing if the job is no longer active under `job.leaseToken`: it
   * was recovered as stalled, then acquired again or finalized.
   *
   * @param job - The lease of the job to complete
   * @param queue - The queue the job belongs to
   * @param removeOnComplete - Optional retention policy for completed jobs
   * @returns False if the lease was lost and nothing changed
   */
  completeJob(job: JobLease, queue: string, removeOnComplete?: JobRetention): Promise<boolean>

  /**
   * Mark a job as failed permanently and remove it from the queue.
   *
   * Does nothing if the job is no longer active under `job.leaseToken`.
   *
   * @param job - The lease of the job to fail
   * @param queue - The queue the job belongs to
   * @param error - Optional error that caused the failure
   * @param removeOnFail - Optional retention policy for failed jobs
   * @returns False if the lease was lost and nothing changed
   */
  failJob(
    job: JobLease,
    queue: string,
    error?: Error,
    removeOnFail?: JobRetention
  ): Promise<boolean>

  /**
   * Retry a job by moving it back to pending with incremented attempts.
   *
   * Does nothing if the job is no longer active under `job.leaseToken`.
   *
   * @param job - The lease of the job to retry
   * @param queue - The queue the job belongs to
   * @param retryAt - Optional future date to delay the retry
   * @returns False if the lease was lost and nothing changed
   */
  retryJob(job: JobLease, queue: string, retryAt?: Date): Promise<boolean>

  /**
   * Get a job record by id.
   *
   * @param jobId - The job ID to retrieve
   * @param queue - The queue the job belongs to
   * @returns The job record, or null if not found
   */
  getJob(jobId: string, queue: string): Promise<JobRecord | null>

  /**
   * Push a job to the default queue for immediate processing.
   *
   * @param jobData - The job data to push
   * @returns PushResult if jobData.dedup is set, otherwise void
   */
  push(jobData: JobData): Promise<PushResult | void>

  /**
   * Push a job to a specific queue for immediate processing.
   *
   * @param queue - The queue name to push to
   * @param jobData - The job data to push
   * @returns PushResult if jobData.dedup is set, otherwise void
   */
  pushOn(queue: string, jobData: JobData): Promise<PushResult | void>

  /**
   * Push a job to the default queue with a delay.
   *
   * @param jobData - The job data to push
   * @param delay - Delay in milliseconds before the job becomes available
   * @returns PushResult if jobData.dedup is set, otherwise void
   */
  pushLater(jobData: JobData, delay: number): Promise<PushResult | void>

  /**
   * Push a job to a specific queue with a delay.
   *
   * @param queue - The queue name to push to
   * @param jobData - The job data to push
   * @param delay - Delay in milliseconds before the job becomes available
   * @returns PushResult if jobData.dedup is set, otherwise void
   */
  pushLaterOn(queue: string, jobData: JobData, delay: number): Promise<PushResult | void>

  /**
   * Push multiple jobs to the default queue for immediate processing.
   *
   * This is more efficient than calling push() multiple times as it
   * batches the operations (e.g., Redis pipeline, SQL batch insert).
   *
   * @param jobs - Array of job data to push
   */
  pushMany(jobs: JobData[]): Promise<void>

  /**
   * Push multiple jobs to a specific queue for immediate processing.
   *
   * This is more efficient than calling pushOn() multiple times as it
   * batches the operations (e.g., Redis pipeline, SQL batch insert).
   *
   * @param queue - The queue name to push to
   * @param jobs - Array of job data to push
   */
  pushManyOn(queue: string, jobs: JobData[]): Promise<void>

  /**
   * Get the number of pending jobs in the default queue.
   *
   * @returns The number of pending jobs
   */
  size(): Promise<number>

  /**
   * Get the number of pending jobs in a specific queue.
   *
   * @param queue - The queue name to check
   * @returns The number of pending jobs
   */
  sizeOf(queue: string): Promise<number>

  /**
   * Clean up resources (close connections, etc.).
   * Called when the worker stops or the adapter is no longer needed.
   */
  destroy(): Promise<void>

  /**
   * Run adapter-specific migrations needed after a major version upgrade.
   *
   * This method is idempotent — it is always safe to call multiple times.
   * Adapters that have no pending migrations return immediately.
   *
   * Call this once during your deployment process before starting workers.
   */
  migrate(): Promise<void>

  /**
   * Create or update a schedule.
   *
   * If a schedule with the given id exists, it will be updated (upsert).
   * Otherwise, a new schedule is created.
   *
   * @param config - The schedule configuration
   * @returns The schedule ID
   */
  upsertSchedule(config: ScheduleConfig): Promise<string>

  /**
   * Create or update a schedule.
   *
   * @deprecated Use `upsertSchedule` instead.
   * @param config - The schedule configuration
   * @returns The schedule ID
   */
  createSchedule(config: ScheduleConfig): Promise<string>

  /**
   * Get a schedule by ID.
   *
   * @param id - The schedule ID
   * @returns The schedule data, or null if not found
   */
  getSchedule(id: string): Promise<ScheduleData | null>

  /**
   * List all schedules matching the given options.
   *
   * @param options - Optional filters for listing
   * @returns Array of schedule data
   */
  listSchedules(options?: ScheduleListOptions): Promise<ScheduleData[]>

  /**
   * Update a schedule's status or run metadata.
   *
   * @param id - The schedule ID
   * @param updates - The fields to update
   */
  updateSchedule(
    id: string,
    updates: Partial<Pick<ScheduleData, 'status' | 'nextRunAt' | 'lastRunAt' | 'runCount'>>
  ): Promise<void>

  /**
   * Delete a schedule permanently.
   *
   * @param id - The schedule ID to delete
   */
  deleteSchedule(id: string): Promise<void>

  /**
   * Atomically claim a due schedule for execution.
   *
   * This method:
   * 1. Finds ONE schedule where nextRunAt <= now AND status = 'active'
   * 2. Calculates and updates its nextRunAt to the next occurrence
   * 3. Increments runCount and sets lastRunAt
   * 4. Returns the schedule data for job dispatching
   *
   * The atomic nature prevents multiple workers from claiming the same schedule.
   *
   * @returns The claimed schedule, or null if no schedules are due
   */
  claimDueSchedule(): Promise<ScheduleData | null>
}
