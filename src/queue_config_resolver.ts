import type {
  Duration,
  JobOptions,
  QueueConfig,
  QueueManagerConfig,
  RetryConfig,
} from './types/main.js'

/**
 * Job options applied when a job runs, after merging the job, queue, global,
 * and worker settings.
 */
export type ResolvedJobOptions = Pick<
  JobOptions,
  'removeOnComplete' | 'removeOnFail' | 'timeout'
> & { failOnTimeout: boolean }

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
   * Resolve the retry policy for a job. `maxRetries` and `backoff` each come
   * from the first layer that defines them:
   *
   * 1. the job options
   * 2. the queue `defaultJobOptions`, then the queue `retry`
   * 3. the global `defaultJobOptions`, then the global `retry`
   */
  resolveRetryConfig(queue: string, jobOptions?: JobOptions): RetryConfig {
    const queueConfig = this.#queueConfigs.get(queue)
    const layers = [
      this.#normalizeRetryConfig(jobOptions),
      this.#normalizeRetryConfig(queueConfig?.defaultJobOptions),
      queueConfig?.retry,
      this.#normalizeRetryConfig(this.#globalJobOptions),
      this.#globalRetryConfig,
    ]

    return {
      maxRetries: layers.find((layer) => layer?.maxRetries !== undefined)?.maxRetries ?? 0,
      backoff: layers.find((layer) => layer?.backoff !== undefined)?.backoff,
    }
  }

  /**
   * Resolve the options applied when a job runs, using priority:
   * job > queue `defaultJobOptions` > global `defaultJobOptions`.
   * The timeout falls back to `worker.timeout` last.
   */
  resolveJobOptions(queue: string, jobOptions?: JobOptions): ResolvedJobOptions {
    const layers = [
      jobOptions,
      this.#queueConfigs.get(queue)?.defaultJobOptions,
      this.#globalJobOptions,
    ]

    return {
      removeOnComplete: this.#firstDefined(layers, 'removeOnComplete'),
      removeOnFail: this.#firstDefined(layers, 'removeOnFail'),
      timeout: this.#firstDefined(layers, 'timeout') ?? this.#workerTimeout,
      failOnTimeout: this.#firstDefined(layers, 'failOnTimeout') ?? false,
    }
  }

  /**
   * The value of `key` in the first options that define it.
   */
  #firstDefined<K extends keyof JobOptions>(
    layers: Array<JobOptions | undefined>,
    key: K
  ): JobOptions[K] {
    return layers.find((options) => options?.[key] !== undefined)?.[key]
  }

  /**
   * Return the Adapter configured for a queue, if any.
   */
  getQueueAdapter(queue: string): string | undefined {
    return this.#queueConfigs.get(queue)?.adapter
  }

  /**
   * Normalize the retry settings of job options so top-level `maxRetries`
   * participates in the merge like `retry.maxRetries`.
   */
  #normalizeRetryConfig(jobOptions?: JobOptions): RetryConfig | undefined {
    if (!jobOptions) return undefined

    return {
      backoff: jobOptions.retry?.backoff,
      maxRetries: jobOptions.retry?.maxRetries ?? jobOptions.maxRetries,
    }
  }
}
