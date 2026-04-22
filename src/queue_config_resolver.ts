import type {
  Duration,
  JobOptions,
  QueueConfig,
  QueueManagerConfig,
  RetryConfig,
} from './types/main.js'

/**
 * Resolve effective queue/job runtime configuration from the initialized
 * queue config.
 *
 * This keeps merge rules in one place without coupling execution code to the
 * full `QueueManager` lifecycle and adapter concerns.
 */
export class QueueConfigResolver {
  readonly #globalRetryConfig?: RetryConfig
  readonly #globalJobOptions?: JobOptions
  readonly #queueConfigs: Map<string, QueueConfig>
  readonly #workerTimeout?: Duration

  /**
   * Create a resolver from the queue manager config.
   */
  static from(config: QueueManagerConfig): QueueConfigResolver {
    return new QueueConfigResolver({
      globalRetryConfig: config.retry,
      globalJobOptions: config.defaultJobOptions,
      queueConfigs: new Map(Object.entries(config.queues || {}) as [string, QueueConfig][]),
      workerTimeout: config.worker?.timeout,
    })
  }

  /**
   * Create a resolver from already-materialized config fragments.
   */
  constructor({
    globalRetryConfig,
    globalJobOptions,
    queueConfigs,
    workerTimeout,
  }: {
    globalRetryConfig?: RetryConfig
    globalJobOptions?: JobOptions
    queueConfigs?: Map<string, QueueConfig>
    workerTimeout?: Duration
  }) {
    this.#globalRetryConfig = globalRetryConfig
    this.#globalJobOptions = globalJobOptions
    this.#queueConfigs = queueConfigs ?? new Map()
    this.#workerTimeout = workerTimeout
  }

  /**
   * Resolve the retry policy for a job using priority: job > queue > global.
   */
  resolveRetryConfig(queue: string, jobOptions?: JobOptions): RetryConfig {
    const queueConfig = this.#queueConfigs.get(queue)
    const queueRetryConfig = queueConfig?.retry || {}
    const jobRetryConfig = this.#normalizeJobRetryConfig(jobOptions)

    const maxRetries =
      jobRetryConfig?.maxRetries ??
      queueRetryConfig.maxRetries ??
      this.#globalRetryConfig?.maxRetries ??
      0

    const backoff =
      jobRetryConfig?.backoff || queueRetryConfig.backoff || this.#globalRetryConfig?.backoff

    return { maxRetries, backoff }
  }

  /**
   * Resolve effective retention options using priority: job > queue > global.
   */
  resolveJobOptions(queue: string, jobOptions?: JobOptions): JobOptions {
    const queueConfig = this.#queueConfigs.get(queue)
    const queueJobOptions = queueConfig?.defaultJobOptions

    return {
      removeOnComplete:
        jobOptions?.removeOnComplete ??
        queueJobOptions?.removeOnComplete ??
        this.#globalJobOptions?.removeOnComplete,
      removeOnFail:
        jobOptions?.removeOnFail ??
        queueJobOptions?.removeOnFail ??
        this.#globalJobOptions?.removeOnFail,
    }
  }

  /**
   * Return the configured default worker timeout.
   */
  getWorkerTimeout(): Duration | undefined {
    return this.#workerTimeout
  }

  /**
   * Normalize job retry settings so top-level `maxRetries` participates in the
   * merge like `retry.maxRetries`.
   */
  #normalizeJobRetryConfig(jobOptions?: JobOptions): RetryConfig | undefined {
    if (!jobOptions || (jobOptions.retry === undefined && jobOptions.maxRetries === undefined)) {
      return undefined
    }

    return {
      ...jobOptions.retry,
      maxRetries: jobOptions.retry?.maxRetries ?? jobOptions.maxRetries,
    }
  }
}
