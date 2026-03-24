import * as errors from './exceptions.js'
import type { Job } from './job.js'
import type { Duration, JobContext, JobOptions, RetryConfig } from './types/main.js'
import { parse } from './utils.js'

export type JobExecutionOutcome =
  | { type: 'completed' }
  | { type: 'retry'; retryAt?: Date }
  | {
      type: 'failed'
      reason: 'timeout' | 'no-retries' | 'max-attempts'
      storageError: Error
      hookError: Error
    }

type JobExecutionRuntimeConfig = {
  jobName: string
  options?: JobOptions
  retryConfig: RetryConfig
  defaultTimeout?: Duration
}

type JobExecutionRuntimeFactoryOptions = {
  jobName: string
  options?: JobOptions
  retryConfig: RetryConfig
  defaultTimeout?: Duration
}

/**
 * Shared execution policy for a single job runtime.
 *
 * It encapsulates timeout resolution and retry/failure decisions so the
 * worker and the sync adapter follow the same execution rules.
 */
export class JobExecutionRuntime {
  readonly #jobName: string
  readonly #options: JobOptions
  readonly #retryConfig: RetryConfig
  readonly #timeout?: number

  /**
   * Build a runtime from already-resolved queue/job execution config.
   */
  static from({
    jobName,
    options,
    retryConfig,
    defaultTimeout,
  }: JobExecutionRuntimeFactoryOptions): JobExecutionRuntime {
    return new JobExecutionRuntime({
      jobName,
      options,
      retryConfig,
      defaultTimeout,
    })
  }

  get maxRetries(): number | undefined {
    return this.#retryConfig.maxRetries
  }

  /**
   * Create a runtime with fully resolved retry and timeout settings.
   */
  constructor({ jobName, options, retryConfig, defaultTimeout }: JobExecutionRuntimeConfig) {
    this.#jobName = jobName
    this.#options = options || {}
    this.#retryConfig = retryConfig

    const timeout = this.#options.timeout ?? defaultTimeout
    this.#timeout = timeout === undefined ? undefined : parse(timeout)
  }

  /**
   * Execute a hydrated job instance and enforce the configured timeout.
   */
  async execute(instance: Job, payload: unknown, context: JobContext): Promise<unknown> {
    if (this.#timeout === undefined) {
      instance.$hydrate(payload, context)
      return instance.execute()
    }

    const signal = AbortSignal.timeout(this.#timeout)
    instance.$hydrate(payload, context, signal)

    const { abortPromise, cleanupAbortListener } = this.#createTimeoutAbortRace(
      signal,
      instance.constructor.name
    )

    try {
      return Promise.race([instance.execute(), abortPromise])
    } finally {
      cleanupAbortListener()
    }
  }

  /**
   * Convert an execution error into a retry or permanent-failure outcome.
   */
  resolveFailure(error: Error, attempts: number): JobExecutionOutcome {
    if (error instanceof errors.E_JOB_TIMEOUT && this.#options.failOnTimeout) {
      return {
        type: 'failed',
        reason: 'timeout',
        storageError: error,
        hookError: error,
      }
    }

    if (typeof this.#retryConfig.maxRetries === 'undefined' || this.#retryConfig.maxRetries <= 0) {
      return {
        type: 'failed',
        reason: 'no-retries',
        storageError: error,
        hookError: error,
      }
    }

    if (attempts >= this.#retryConfig.maxRetries) {
      return {
        type: 'failed',
        reason: 'max-attempts',
        storageError: error,
        hookError: new errors.E_JOB_MAX_ATTEMPTS_REACHED([this.#jobName], { cause: error }),
      }
    }

    if (this.#retryConfig.backoff) {
      return {
        type: 'retry',
        retryAt: this.#retryConfig.backoff().getNextRetryAt(attempts + 1),
      }
    }

    return { type: 'retry' }
  }

  /**
   * Create the timeout race used to abort a job execution cleanly.
   */
  #createTimeoutAbortRace(signal: AbortSignal, runtimeJobName: string) {
    const timeout = this.#timeout!
    let abortHandler: (() => void) | undefined

    const abortPromise = new Promise<never>((_, reject) => {
      abortHandler = () => {
        reject(new errors.E_JOB_TIMEOUT([runtimeJobName, timeout]))
      }

      if (signal.aborted) {
        abortHandler()
        return
      }

      signal.addEventListener('abort', abortHandler, { once: true })
    })

    return {
      abortPromise,
      cleanupAbortListener: () => {
        if (abortHandler) {
          signal.removeEventListener('abort', abortHandler)
        }
      },
    }
  }
}
