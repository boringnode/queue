import { randomUUID } from 'node:crypto'
import debug from './debug.js'
import { parse } from './utils.js'
import { QueueManager } from './queue_manager.js'
import { jobDispatchRuntime } from './job_dispatch_runtime.js'
import {
  WorkerSession,
  type InternalOperationWrapper,
  type WorkerSessionSettings,
} from './worker_session.js'
import type { Adapter } from './contracts/adapter.js'
import type { QueueManagerConfig, WorkerCycle } from './types/main.js'
import type { JobExecutionRuntime } from './job_runtime.js'
import {
  DEFAULT_IDLE_DELAY,
  DEFAULT_STALLED_INTERVAL,
  DEFAULT_STALLED_THRESHOLD,
} from './constants.js'

/**
 * Job processing worker.
 *
 * Worker owns configuration, initialization, and signal handling. Each
 * continuous period of processing is delegated to one WorkerSession, which
 * owns all work until it reaches quiescence.
 *
 * The Worker continuously polls queues for jobs and executes them with
 * configurable concurrency. It handles retries, stalled job recovery, and
 * graceful shutdown on SIGINT/SIGTERM.
 *
 * @example
 * ```typescript
 * import { Worker, redis } from '@boringnode/queue'
 *
 * const worker = new Worker({
 *   default: 'redis',
 *   adapters: { redis: redis() },
 *   locations: ['./jobs/**\/*.js'],
 *   worker: { concurrency: 5, idleDelay: '1s' },
 * })
 *
 * await worker.start(['default', 'emails'])
 * ```
 */
export class Worker {
  readonly #id: string
  readonly #config: QueueManagerConfig
  readonly #sessionSettings: WorkerSessionSettings
  readonly #gracefulShutdown: boolean
  readonly #onShutdownSignal?: () => void | Promise<void>

  #adapter!: Adapter
  #jobExecutionRuntime!: JobExecutionRuntime
  #wrapInternal: InternalOperationWrapper = (operation) => operation()
  #initialized = false
  #initOperation?: Promise<void>
  #session?: WorkerSession
  #stopOperation?: Promise<void>
  #shutdownGeneration = 0
  #shutdownHandler?: () => Promise<void>

  /** Unique identifier for this worker instance. */
  get id() {
    return this.#id
  }

  /**
   * Create a new Worker instance.
   *
   * @param config - Queue configuration including Adapter and Worker settings
   */
  constructor(config: QueueManagerConfig) {
    this.#config = config
    this.#id = randomUUID()
    this.#sessionSettings = {
      idleDelay: parse(config.worker?.idleDelay ?? DEFAULT_IDLE_DELAY),
      stalledInterval: parse(config.worker?.stalledInterval ?? DEFAULT_STALLED_INTERVAL),
      stalledThreshold: parse(config.worker?.stalledThreshold ?? DEFAULT_STALLED_THRESHOLD),
      maxStalledCount: config.worker?.maxStalledCount ?? 1,
      concurrency: config.worker?.concurrency ?? 1,
    }
    this.#gracefulShutdown = config.worker?.gracefulShutdown ?? true
    this.#onShutdownSignal = config.worker?.onShutdownSignal

    debug('created worker with id %s and config %O', this.#id, config)
  }

  /**
   * Initialize the Worker (called automatically by `start()`).
   *
   * Sets up QueueManager and the Adapter connection. Concurrent calls share
   * one initialization operation.
   */
  async init(): Promise<void> {
    if (this.#initialized) return

    if (!this.#initOperation) {
      this.#initOperation = this.#initialize()
    }

    const initOperation = this.#initOperation

    try {
      await initOperation
    } finally {
      if (this.#initOperation === initOperation) {
        this.#initOperation = undefined
      }
    }
  }

  async #initialize(): Promise<void> {
    debug('initializing worker %s', this.#id)

    await QueueManager.init(this.#config)

    this.#adapter = QueueManager.use(this.#config.worker?.adapter)
    this.#adapter.setWorkerId(this.#id)
    this.#jobExecutionRuntime = QueueManager.getJobExecutionRuntime()
    this.#wrapInternal = QueueManager.getInternalOperationWrapper()
    this.#initialized = true

    debug('worker %s initialized', this.#id)
  }

  /**
   * Start processing Jobs from the specified queues.
   *
   * This method blocks until the Worker is stopped. Jobs are processed
   * concurrently up to the configured concurrency limit.
   *
   * @param queues - Queue names to process in priority order
   */
  async start(queues: string[] = ['default']): Promise<void> {
    while (this.#stopOperation) {
      await this.#stopOperation
    }

    const shutdownGeneration = this.#shutdownGeneration
    await this.init()

    if (shutdownGeneration !== this.#shutdownGeneration) return

    const session = this.#useSession(queues)
    this.#setupGracefulShutdown()
    await session.start()
  }

  /**
   * Stop the active session and wait until it reaches quiescence.
   *
   * Once this method resolves, the Worker can no longer acquire, execute,
   * finalize, or renew Jobs from that session. A later start creates a fresh
   * session.
   *
   * Adapter cleanup remains the responsibility of `QueueManager.destroy()`.
   * Called automatically on SIGINT/SIGTERM when graceful shutdown is enabled.
   */
  stop(): Promise<void> {
    if (!this.#stopOperation) {
      const stopOperation = this.#stop()
      this.#stopOperation = stopOperation

      const clearStopOperation = () => {
        if (this.#stopOperation === stopOperation) {
          this.#stopOperation = undefined
        }
      }

      void stopOperation.then(clearStopOperation, clearStopOperation)
    }

    return this.#stopOperation
  }

  async #stop(): Promise<void> {
    debug('stopping worker %s', this.#id)
    this.#shutdownGeneration++

    if (this.#initOperation) {
      debug('worker %s: waiting for initialization to complete', this.#id)
      await this.#initOperation.catch(() => {})
    }

    const session = this.#session
    if (session) {
      await session.stop()
      if (this.#session === session) {
        this.#session = undefined
      }
    }

    this.#removeShutdownHandlers()
  }

  /**
   * Process a single cycle and return its event.
   *
   * Useful for tests and callers that need fine-grained control. Repeated calls
   * share one manual session until `stop()`.
   *
   * @param queues - Queue names to process in priority order
   * @returns The cycle event, or null when the session is stopping or stopped
   */
  async processCycle(queues: string[]): Promise<WorkerCycle | null> {
    while (this.#stopOperation) {
      await this.#stopOperation
    }

    const shutdownGeneration = this.#shutdownGeneration
    await this.init()

    if (shutdownGeneration !== this.#shutdownGeneration) return null

    return this.#useSession(queues).processCycle()
  }

  /**
   * Yield Worker cycle events from the active continuous session.
   *
   * This is the low-level processing interface used by callers that consume
   * `started`, `completed`, `idle`, and `error` events directly.
   *
   * @param queues - Queue names to process in priority order
   */
  async *process(queues: string[]): AsyncGenerator<WorkerCycle, void, unknown> {
    while (this.#stopOperation) {
      await this.#stopOperation
    }

    const shutdownGeneration = this.#shutdownGeneration
    await this.init()

    if (shutdownGeneration !== this.#shutdownGeneration) return

    yield* this.#useSession(queues).process()
  }

  #useSession(queues: string[]): WorkerSession {
    if (this.#session) {
      this.#session.assertQueues(queues)
      return this.#session
    }

    const session = new WorkerSession({
      workerId: this.#id,
      queues,
      adapter: this.#adapter,
      jobExecutionRuntime: this.#jobExecutionRuntime,
      scheduleDispatcher: jobDispatchRuntime,
      wrapInternal: this.#wrapInternal,
      settings: this.#sessionSettings,
    })

    this.#session = session
    return session
  }

  #setupGracefulShutdown(): void {
    if (!this.#gracefulShutdown || this.#shutdownHandler) return

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

  #removeShutdownHandlers(): void {
    if (!this.#shutdownHandler) return

    process.off('SIGINT', this.#shutdownHandler)
    process.off('SIGTERM', this.#shutdownHandler)
    this.#shutdownHandler = undefined
  }
}
