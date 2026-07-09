import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import debug from './debug.js'
import { parse } from './utils.js'
import { QueueManager } from './queue_manager.js'
import { JobPool } from './job_pool.js'
import { Locator } from './locator.js'
import { dispatchChannel } from './tracing_channels.js'
import type { Adapter, AcquiredJob } from './contracts/adapter.js'
import type { QueueManagerConfig, WorkerCycle } from './types/main.js'
import type { JobDispatchMessage } from './types/tracing_channels.js'
import type { JobExecutionOutcome, JobExecutionRuntime } from './job_runtime.js'
import {
  DEFAULT_IDLE_DELAY,
  DEFAULT_STALLED_INTERVAL,
  DEFAULT_STALLED_THRESHOLD,
  DEFAULT_ERROR_RETRY_DELAY,
} from './constants.js'

/**
 * Job processing worker.
 *
 * The Worker continuously polls queues for jobs and executes them
 * with configurable concurrency. It handles:
 * - Concurrent job execution via JobPool
 * - Automatic retries with backoff strategies
 * - Stalled job detection and recovery
 * - Graceful shutdown on SIGINT/SIGTERM
 *
 * @example
 * ```typescript
 * import { Worker, redis } from '@boringnode/queue'
 *
 * const worker = new Worker({
 *   default: 'redis',
 *   adapters: { redis: redis() },
 *   locations: ['./jobs/**\/*.js'],
 *   worker: {
 *     concurrency: 5,
 *     idleDelay: '1s',
 *   },
 * })
 *
 * // Start processing jobs
 * await worker.start(['default', 'emails'])
 *
 * // Or for testing, process one cycle at a time
 * const cycle = await worker.processCycle(['default'])
 * ```
 */
export class Worker {
  readonly #id: string
  readonly #config: QueueManagerConfig
  readonly #idleDelay: number
  readonly #stalledInterval: number
  readonly #stalledThreshold: number
  readonly #maxStalledCount: number
  readonly #concurrency: number
  readonly #gracefulShutdown: boolean
  readonly #onShutdownSignal?: () => void | Promise<void>

  #adapter!: Adapter
  #jobExecutionRuntime!: JobExecutionRuntime
  #wrapInternal: <T>(fn: () => Promise<T>) => Promise<T> = (fn) => fn()
  #running = false
  #initialized = false
  #generator?: AsyncGenerator<WorkerCycle, void, unknown>
  #pool?: JobPool
  #lastStalledCheck = 0
  #shutdownHandler?: () => Promise<void>
  #heartbeatTimer?: NodeJS.Timeout
  #renewingJobs = false

  /** Unique identifier for this worker instance */
  get id() {
    return this.#id
  }

  /**
   * Create a new worker instance.
   *
   * @param config - Queue configuration including adapter and worker settings
   */
  constructor(config: QueueManagerConfig) {
    this.#config = config
    this.#id = randomUUID()

    // Parse worker config once at construction
    this.#idleDelay = parse(config.worker?.idleDelay ?? DEFAULT_IDLE_DELAY)
    this.#stalledInterval = parse(config.worker?.stalledInterval ?? DEFAULT_STALLED_INTERVAL)
    this.#stalledThreshold = parse(config.worker?.stalledThreshold ?? DEFAULT_STALLED_THRESHOLD)
    this.#maxStalledCount = config.worker?.maxStalledCount ?? 1
    this.#concurrency = config.worker?.concurrency ?? 1
    this.#gracefulShutdown = config.worker?.gracefulShutdown ?? true
    this.#onShutdownSignal = config.worker?.onShutdownSignal

    debug('created worker with id %s and config %O', this.#id, config)
  }

  /**
   * Initialize the worker (called automatically by `start()`).
   *
   * Sets up the QueueManager and adapter connection.
   */
  async init() {
    if (this.#initialized) {
      return
    }

    debug('initializing worker %s', this.#id)

    await QueueManager.init(this.#config)

    this.#adapter = QueueManager.use()
    this.#adapter.setWorkerId(this.#id)
    this.#jobExecutionRuntime = QueueManager.getJobExecutionRuntime()
    this.#wrapInternal = QueueManager.getInternalOperationWrapper()

    this.#initialized = true

    debug('worker %s initialized', this.#id)
  }

  /**
   * Start processing jobs from the specified queues.
   *
   * This method blocks until the worker is stopped (via `stop()` or signal).
   * Jobs are processed concurrently up to the configured concurrency limit.
   *
   * @param queues - Queue names to process (default: ['default'])
   *
   * @example
   * ```typescript
   * // Process single queue
   * await worker.start()
   *
   * // Process multiple queues (priority order)
   * await worker.start(['high-priority', 'default', 'low-priority'])
   * ```
   */
  async start(queues: string[] = ['default']): Promise<void> {
    await this.init()

    if (this.#running) {
      debug('worker %s is already running', this.#id)
      return
    }

    this.#running = true

    debug('starting worker %s on queues: %O', this.#id, queues)

    this.#setupGracefulShutdown()

    for await (const cycle of this.process(queues)) {
      if (['started', 'completed'].includes(cycle.type)) {
        continue
      }

      if (['idle', 'error'].includes(cycle.type)) {
        // @ts-expect-error - we know suggestedDelay exists for these types
        const delay = parse(cycle.suggestedDelay)

        if (cycle.type === 'error') {
          debug('worker %s encountered an error: %O', this.#id, cycle.error)
        } else {
          debug('worker %s is idle, waiting for %dms', this.#id, delay)
        }

        await setTimeout(delay)
      }
    }
  }

  /**
   * Stop the worker gracefully.
   *
   * Waits for all running jobs to complete before stopping job consumption.
   * Adapter cleanup remains the responsibility of `QueueManager.destroy()`.
   * Called automatically on SIGINT/SIGTERM if gracefulShutdown is enabled.
   */
  async stop() {
    debug('stopping worker %s', this.#id)

    this.#running = false

    if (this.#pool) {
      debug('worker %s: waiting for %d running jobs to complete', this.#id, this.#pool.size)
      await this.#pool.drain()
    }

    // Stop the heartbeat only after draining, so jobs that are still finishing
    // keep being renewed until they actually complete. The process() generator
    // also clears it in its `finally`, but clearing here guarantees prompt and
    // deterministic cleanup regardless of how the worker was driven (start() vs
    // processCycle()).
    this.#stopHeartbeat()

    this.#removeShutdownHandlers()
  }

  /**
   * Process a single cycle and return the result.
   *
   * Useful for testing or when you need fine-grained control.
   * Each cycle may start new jobs, complete a job, or return idle.
   *
   * @param queues - Queue names to process
   * @returns The cycle result, or null if the worker was stopped
   *
   * @example
   * ```typescript
   * const worker = new Worker(config)
   *
   * // Process cycles manually
   * let cycle = await worker.processCycle(['default'])
   * while (cycle) {
   *   console.log('Cycle:', cycle.type)
   *   cycle = await worker.processCycle(['default'])
   * }
   * ```
   */
  async processCycle(queues: string[]): Promise<WorkerCycle | null> {
    await this.init()

    this.#running = true

    if (!this.#generator) {
      this.#generator = this.process(queues)
    }

    const result = await this.#generator.next()

    if (result.done) {
      this.#generator = undefined
      return null
    }

    return result.value
  }

  /**
   * Generator that yields worker cycle events.
   *
   * Low-level API for processing jobs. Yields events for:
   * - `started`: A new job began execution
   * - `completed`: A job finished (success or failure)
   * - `idle`: No jobs available, suggest waiting
   * - `error`: An error occurred during processing
   *
   * @param queues - Queue names to process
   * @yields WorkerCycle events
   *
   * @example
   * ```typescript
   * for await (const cycle of worker.process(['default'])) {
   *   switch (cycle.type) {
   *     case 'started':
   *       console.log(`Started job ${cycle.job.id}`)
   *       break
   *     case 'completed':
   *       console.log(`Completed job ${cycle.job.id}`)
   *       break
   *     case 'idle':
   *       await sleep(cycle.suggestedDelay)
   *       break
   *   }
   * }
   * ```
   */
  async *process(queues: string[]): AsyncGenerator<WorkerCycle, void, unknown> {
    this.#pool = new JobPool()
    this.#startHeartbeat(queues)

    try {
      while (this.#running) {
        try {
          // Check for stalled jobs periodically
          await this.#checkStalledJobs(queues)

          // Dispatch any due scheduled jobs
          await this.#dispatchDueSchedules()

          yield* this.#fillPool(queues)

          if (this.#pool.isEmpty()) {
            yield { type: 'idle', suggestedDelay: this.#idleDelay }
            continue
          }

          const hasCapacity = this.#pool.hasCapacity(this.#concurrency)

          // If we have capacity, don't block indefinitely waiting for a completion;
          // wake up periodically to try to acquire newly enqueued jobs.
          const result = await Promise.race([
            this.#pool
              .waitForNextCompletion()
              .then((completed) => ({ kind: 'completed' as const, completed })),
            ...(hasCapacity
              ? [setTimeout(this.#idleDelay).then(() => ({ kind: 'tick' as const }))]
              : []),
          ])

          if (result.kind === 'tick') {
            // No completion yet, but we woke up to check the queue again
            continue
          }

          yield { type: 'completed', queue: result.completed.queue, job: result.completed.job }
        } catch (error) {
          yield {
            type: 'error',
            error: error as Error,
            suggestedDelay: parse(DEFAULT_ERROR_RETRY_DELAY),
          }
        }
      }
    } finally {
      this.#stopHeartbeat()
    }
  }

  async *#fillPool(queues: string[]): AsyncGenerator<WorkerCycle, void, unknown> {
    const slotsAvailable = this.#concurrency - this.#pool!.size

    if (slotsAvailable <= 0) return

    const popPromises = Array.from({ length: slotsAvailable }, () => this.#acquireNextJob(queues))

    const results = await Promise.all(popPromises)

    for (const result of results) {
      if (!result) continue

      const { job, queue } = result
      const promise = this.#execute(job, queue)
      this.#pool!.add(job, queue, promise)

      yield { type: 'started', queue, job }
    }
  }

  async #execute(job: AcquiredJob, queue: string): Promise<void> {
    const startTime = performance.now()

    debug('worker %s: executing job %s (%s)', this.#id, job.id, job.name)

    const outcome = await this.#jobExecutionRuntime.execute(job, queue)
    await this.#finalizeExecution(job, queue, outcome)

    if (outcome.type === 'completed') {
      debug(
        'worker %s: successfully executed job %s in %dms',
        this.#id,
        job.id,
        (performance.now() - startTime).toFixed(2)
      )
    }
  }

  async #finalizeExecution(
    job: AcquiredJob,
    queue: string,
    outcome: JobExecutionOutcome
  ): Promise<void> {
    if (outcome.type === 'completed') {
      await this.#wrapInternal(() =>
        this.#adapter.completeJob(job.id, queue, outcome.removeOnComplete)
      )
      return
    }

    if (outcome.type === 'initialization-failed') {
      debug('worker %s: failed to initialize job %s (%s)', this.#id, job.id, job.name)
      await this.#wrapInternal(() =>
        this.#adapter.failJob(job.id, queue, outcome.error, outcome.removeOnFail)
      )
      return
    }

    if (outcome.type === 'failed') {
      await this.#wrapInternal(() =>
        this.#adapter.failJob(job.id, queue, outcome.error, outcome.removeOnFail)
      )

      if (outcome.failedHookError) {
        throw outcome.failedHookError
      }

      return
    }

    if (outcome.retryAt) {
      debug('worker %s: job %s will retry at %s', this.#id, job.id, outcome.retryAt.toISOString())
      await this.#wrapInternal(() => this.#adapter.retryJob(job.id, queue, outcome.retryAt))
    } else {
      await this.#wrapInternal(() => this.#adapter.retryJob(job.id, queue))
    }
  }

  async #acquireNextJob(queues: string[]): Promise<{ job: AcquiredJob; queue: string } | null> {
    for (const queue of queues) {
      const job = await this.#wrapInternal(() => this.#adapter.popFrom(queue))

      if (!job) {
        continue
      }

      debug('worker %s: acquired job %s', this.#id, job.id)
      return { job, queue }
    }

    return null
  }

  async #checkStalledJobs(queues: string[]): Promise<void> {
    const now = Date.now()

    // Only check if enough time has passed since last check
    if (now - this.#lastStalledCheck < this.#stalledInterval) {
      return
    }

    this.#lastStalledCheck = now

    for (const queue of queues) {
      const recovered = await this.#wrapInternal(() =>
        this.#adapter.recoverStalledJobs(queue, this.#stalledThreshold, this.#maxStalledCount)
      )

      if (recovered > 0) {
        debug('worker %s: recovered %d stalled jobs from queue %s', this.#id, recovered, queue)
      }
    }
  }

  /**
   * Start the heartbeat timer that periodically renews the acquired timestamp
   * of in-flight jobs.
   *
   * Renewal cannot piggyback on the main process loop: at full concurrency the
   * loop blocks on `waitForNextCompletion()` with no idle tick, so exactly when
   * long-running jobs are in flight the loop is not cycling. A dedicated timer
   * guarantees healthy jobs are refreshed before `recoverStalledJobs` would
   * consider them stalled and re-deliver them.
   *
   * The interval is half the stalled threshold so a job is renewed at least
   * once within every stalled window.
   */
  #startHeartbeat(queues: string[]) {
    // Never leave a previous timer running if the loop is somehow re-entered.
    this.#stopHeartbeat()

    const interval = Math.max(Math.floor(this.#stalledThreshold / 2), 1)

    this.#heartbeatTimer = setInterval(() => {
      void this.#renewActiveJobs(queues)
    }, interval)

    // Don't let the heartbeat keep the event loop alive on its own.
    this.#heartbeatTimer.unref?.()
  }

  #stopHeartbeat() {
    if (this.#heartbeatTimer) {
      clearInterval(this.#heartbeatTimer)
      this.#heartbeatTimer = undefined
    }
  }

  /**
   * Renew the acquired timestamp of the jobs currently in the pool so that
   * long-running handlers are not treated as stalled while they are still
   * running. Only jobs still active in the adapter are renewed.
   */
  async #renewActiveJobs(queues: string[]): Promise<void> {
    // Guard against overlapping runs if a renewal takes longer than the interval.
    if (this.#renewingJobs || !this.#pool || this.#pool.isEmpty()) {
      return
    }

    this.#renewingJobs = true

    try {
      const jobIdsByQueue = this.#pool.activeJobIdsByQueue()

      for (const queue of queues) {
        const jobIds = jobIdsByQueue.get(queue)

        if (!jobIds || jobIds.length === 0) {
          continue
        }

        try {
          await this.#wrapInternal(() => this.#adapter.renewJobs(queue, jobIds))
        } catch (error) {
          // A failed heartbeat must never crash the worker; the job will simply
          // be considered stalled if renewals keep failing.
          debug('worker %s: failed to renew jobs on queue %s: %O', this.#id, queue, error)
        }
      }
    } finally {
      this.#renewingJobs = false
    }
  }

  #setupGracefulShutdown() {
    if (!this.#gracefulShutdown) {
      return
    }

    this.#shutdownHandler = async () => {
      debug('received shutdown signal, stopping worker...')

      if (this.#onShutdownSignal) {
        await this.#onShutdownSignal()
      }

      await this.stop()
    }

    process.on('SIGINT', this.#shutdownHandler)
    process.on('SIGTERM', this.#shutdownHandler)
  }

  #removeShutdownHandlers() {
    if (this.#shutdownHandler) {
      process.off('SIGINT', this.#shutdownHandler)
      process.off('SIGTERM', this.#shutdownHandler)
      this.#shutdownHandler = undefined
    }
  }

  /**
   * Dispatch any due scheduled jobs.
   *
   * Claims due schedules from the adapter and dispatches the corresponding
   * jobs to their configured queues.
   */
  async #dispatchDueSchedules(): Promise<void> {
    // Keep claiming due schedules until there are none left
    while (true) {
      const schedule = await this.#wrapInternal(() => this.#adapter.claimDueSchedule())

      if (!schedule) {
        break
      }

      debug(
        'worker %s: dispatching scheduled job %s (schedule: %s, runCount: %d)',
        this.#id,
        schedule.name,
        schedule.id,
        schedule.runCount + 1
      )

      // Get the job class to determine the target queue
      const JobClass = await Locator.resolve(schedule.name)
      const queue = JobClass?.options?.queue ?? 'default'

      const jobData = {
        id: randomUUID(),
        name: schedule.name,
        payload: schedule.payload,
        attempts: 0,
        priority: JobClass?.options?.priority,
      }

      const message: JobDispatchMessage = { jobs: [jobData], queue }
      await dispatchChannel.tracePromise(async () => {
        await this.#wrapInternal(() => this.#adapter.pushOn(queue, jobData))
      }, message)
    }
  }
}
