import * as errors from './exceptions.js'
import { DEFAULT_PRIORITY } from './constants.js'
import { executeChannel } from './tracing_channels.js'
import type { Job } from './job.js'
import type { AcquiredJob } from './contracts/adapter.js'
import type { QueueConfigResolver } from './queue_config_resolver.js'
import type {
  JobClass,
  JobContext,
  JobFactory,
  JobOptions,
  JobRetention,
  QueueManagerConfig,
  RetryConfig,
} from './types/main.js'
import type { JobExecuteMessage } from './types/tracing_channels.js'
import { parse } from './utils.js'

type PermanentFailureReason = 'timeout' | 'no-retries' | 'max-attempts'

export type JobExecutionOutcome =
  | { type: 'completed'; removeOnComplete?: JobRetention }
  | { type: 'retry'; retryAt?: Date }
  | {
      type: 'failed'
      reason: PermanentFailureReason
      error: Error
      removeOnFail?: JobRetention
      failedHookError?: Error
    }
  | { type: 'initialization-failed'; error: Error; removeOnFail?: JobRetention }

type JobExecutionRuntimeDependencies = {
  resolveJob: (jobName: string) => JobClass | Promise<JobClass>
  configResolver: QueueConfigResolver
  jobFactory?: JobFactory
  executionWrapper?: NonNullable<QueueManagerConfig['executionWrapper']>
}

type FailureDecision =
  | { type: 'retry'; retryAt?: Date }
  | {
      type: 'failed'
      reason: PermanentFailureReason
      hookError: Error
    }

const noopExecutionWrapper: NonNullable<QueueManagerConfig['executionWrapper']> = async (run) =>
  run()

/**
 * Execute one in-process Job attempt through a single lifecycle interface.
 *
 * Resolution, instantiation, context construction, hydration, timeout,
 * tracing, failure hooks, and retry classification live behind this seam.
 * Callers retain only their adapter-specific scheduling and persistence work.
 */
export class JobExecutionRuntime {
  readonly #resolveJob: JobExecutionRuntimeDependencies['resolveJob']
  readonly #configResolver: QueueConfigResolver
  readonly #jobFactory?: JobFactory
  readonly #executionWrapper: NonNullable<QueueManagerConfig['executionWrapper']>

  constructor({
    resolveJob,
    configResolver,
    jobFactory,
    executionWrapper,
  }: JobExecutionRuntimeDependencies) {
    this.#resolveJob = resolveJob
    this.#configResolver = configResolver
    this.#jobFactory = jobFactory
    this.#executionWrapper = executionWrapper ?? noopExecutionWrapper
  }

  /**
   * Execute a single acquired Job attempt and return the adapter-facing outcome.
   */
  execute(job: AcquiredJob, queue: string): Promise<JobExecutionOutcome> {
    const executeMessage: JobExecuteMessage = { job, queue }
    const startTime = performance.now()

    const run = () =>
      executeChannel.tracePromise(async () => {
        try {
          return await this.#executeAttempt(job, queue, executeMessage)
        } finally {
          executeMessage.duration = Number((performance.now() - startTime).toFixed(2))
        }
      }, executeMessage)

    return this.#executionWrapper(run, job, queue)
  }

  async #executeAttempt(
    job: AcquiredJob,
    queue: string,
    executeMessage: JobExecuteMessage
  ): Promise<JobExecutionOutcome> {
    let instance: Job
    let options: JobOptions

    try {
      const JobClass = await this.#resolveJob(job.name)
      options = JobClass.options || {}
      instance = this.#jobFactory ? await this.#jobFactory(JobClass) : new JobClass()
    } catch (error) {
      const initializationError = error as Error
      const retention = this.#configResolver.resolveJobOptions(queue)

      executeMessage.status = 'failed'
      executeMessage.error = initializationError

      return {
        type: 'initialization-failed',
        error: initializationError,
        removeOnFail: retention.removeOnFail,
      }
    }

    const context = this.#createContext(job, queue)
    const retention = this.#configResolver.resolveJobOptions(queue, options)
    const retryConfig = this.#configResolver.resolveRetryConfig(queue, options)

    try {
      await this.#executeJob(instance, job.payload, context, options)
      executeMessage.status = 'completed'

      return { type: 'completed', removeOnComplete: retention.removeOnComplete }
    } catch (error) {
      const executionError = error as Error
      const decision = this.#resolveFailure(
        job.name,
        options,
        retryConfig,
        executionError,
        job.attempts
      )

      executeMessage.error = executionError

      if (decision.type === 'retry') {
        executeMessage.status = 'retrying'
        executeMessage.nextRetryAt = decision.retryAt
        return decision
      }

      executeMessage.status = 'failed'

      let failedHookError: Error | undefined
      try {
        await instance.failed?.(decision.hookError)
      } catch (error) {
        failedHookError = error as Error
      }

      return {
        type: 'failed',
        reason: decision.reason,
        error: executionError,
        removeOnFail: retention.removeOnFail,
        failedHookError,
      }
    }
  }

  #createContext(job: AcquiredJob, queue: string): JobContext {
    return {
      jobId: job.id,
      name: job.name,
      attempt: job.attempts + 1,
      queue,
      priority: job.priority ?? DEFAULT_PRIORITY,
      acquiredAt: new Date(job.acquiredAt),
      stalledCount: job.stalledCount ?? 0,
    }
  }

  async #executeJob(
    instance: Job,
    payload: unknown,
    context: JobContext,
    options: JobOptions
  ): Promise<void> {
    const configuredTimeout = options.timeout ?? this.#configResolver.getWorkerTimeout()

    if (configuredTimeout === undefined) {
      instance.$hydrate(payload, context)
      return instance.execute()
    }

    const timeout = parse(configuredTimeout)
    const signal = AbortSignal.timeout(timeout)
    instance.$hydrate(payload, context, signal)

    const { abortPromise, cleanupAbortListener } = this.#createTimeoutAbortRace(
      signal,
      instance.constructor.name,
      timeout
    )

    try {
      await Promise.race([instance.execute(), abortPromise])
    } finally {
      cleanupAbortListener()
    }
  }

  #resolveFailure(
    jobName: string,
    options: JobOptions,
    retryConfig: RetryConfig,
    error: Error,
    attempts: number
  ): FailureDecision {
    if (error instanceof errors.E_JOB_TIMEOUT && options.failOnTimeout) {
      return { type: 'failed', reason: 'timeout', hookError: error }
    }

    if (typeof retryConfig.maxRetries === 'undefined' || retryConfig.maxRetries <= 0) {
      return { type: 'failed', reason: 'no-retries', hookError: error }
    }

    if (attempts >= retryConfig.maxRetries) {
      return {
        type: 'failed',
        reason: 'max-attempts',
        hookError: new errors.E_JOB_MAX_ATTEMPTS_REACHED([jobName], { cause: error }),
      }
    }

    if (retryConfig.backoff) {
      return {
        type: 'retry',
        retryAt: retryConfig.backoff().getNextRetryAt(attempts + 1),
      }
    }

    return { type: 'retry' }
  }

  #createTimeoutAbortRace(signal: AbortSignal, runtimeJobName: string, timeout: number) {
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
