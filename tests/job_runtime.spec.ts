import { test } from '@japa/runner'
import { Job } from '../src/job.js'
import { JobExecutionRuntime } from '../src/job_runtime.js'
import { QueueConfigResolver } from '../src/queue_config_resolver.js'
import { executeChannel } from '../src/tracing_channels.js'
import { fixedBackoff } from '../src/strategies/backoff_strategy.js'
import * as errors from '../src/exceptions.js'
import type { AcquiredJob } from '../src/contracts/adapter.js'
import type { JobExecuteMessage } from '../src/types/tracing_channels.js'

const acquiredJob = (overrides: Partial<AcquiredJob> = {}): AcquiredJob => ({
  id: 'job-1',
  name: 'TestJob',
  payload: { value: 42 },
  attempts: 0,
  priority: 2,
  stalledCount: 1,
  acquiredAt: Date.parse('2026-07-09T12:00:00.000Z'),
  ...overrides,
})

test.group('JobExecutionRuntime', () => {
  test('executes the complete in-process lifecycle through one interface', async ({ assert }) => {
    let receivedPayload: unknown
    let receivedContext: Job['context'] | undefined
    let wrappedJob: AcquiredJob | undefined
    let wrappedQueue: string | undefined

    class TestJob extends Job {
      static options = { removeOnComplete: { age: '1h' } }

      async execute() {
        receivedPayload = this.payload
        receivedContext = this.context
      }
    }

    const runtime = new JobExecutionRuntime({
      resolveJob: async () => TestJob,
      configResolver: new QueueConfigResolver({}),
      executionWrapper: async (run, job, queue) => {
        wrappedJob = job
        wrappedQueue = queue
        return run()
      },
    })
    const job = acquiredJob()

    const outcome = await runtime.execute(job, 'emails')

    assert.deepEqual(outcome, {
      type: 'completed',
      removeOnComplete: { age: '1h' },
    })
    assert.deepEqual(receivedPayload, { value: 42 })
    assert.deepEqual(receivedContext, {
      jobId: 'job-1',
      name: 'TestJob',
      attempt: 1,
      queue: 'emails',
      priority: 2,
      acquiredAt: new Date('2026-07-09T12:00:00.000Z'),
      stalledCount: 1,
    })
    assert.strictEqual(wrappedJob, job)
    assert.equal(wrappedQueue, 'emails')
  })

  test('classifies a retry and publishes its execution metadata', async ({ assert, cleanup }) => {
    const executionError = new Error('transient failure')
    let traceMessage: JobExecuteMessage | undefined
    const captureTrace = (message: unknown) => {
      traceMessage = { ...(message as JobExecuteMessage) }
    }

    executeChannel.asyncEnd.subscribe(captureTrace)
    cleanup(() => executeChannel.asyncEnd.unsubscribe(captureTrace))

    class RetryingJob extends Job {
      async execute() {
        throw executionError
      }
    }

    const runtime = new JobExecutionRuntime({
      resolveJob: async () => RetryingJob,
      configResolver: new QueueConfigResolver({
        globalRetryConfig: { maxRetries: 2, backoff: fixedBackoff('1s') },
      }),
    })
    const beforeExecution = Date.now()

    const outcome = await runtime.execute(acquiredJob(), 'default')

    assert.equal(outcome.type, 'retry')
    if (outcome.type !== 'retry') return

    assert.instanceOf(outcome.retryAt, Date)
    assert.isAtLeast(outcome.retryAt!.getTime(), beforeExecution + 1_000)
    assert.equal(traceMessage?.status, 'retrying')
    assert.strictEqual(traceMessage?.error, executionError)
    assert.deepEqual(traceMessage?.nextRetryAt, outcome.retryAt)
    assert.isNumber(traceMessage?.duration)
  })

  test('invokes failed() after retry exhaustion and preserves a hook error', async ({ assert }) => {
    const executionError = new Error('still failing')
    const failedHookError = new Error('failed hook exploded')
    let hookError: Error | undefined

    class ExhaustedJob extends Job {
      static options = { removeOnFail: false }

      async execute() {
        throw executionError
      }

      async failed(error: Error) {
        hookError = error
        throw failedHookError
      }
    }

    const runtime = new JobExecutionRuntime({
      resolveJob: async () => ExhaustedJob,
      configResolver: new QueueConfigResolver({ globalRetryConfig: { maxRetries: 2 } }),
    })

    const outcome = await runtime.execute(acquiredJob({ attempts: 2 }), 'default')

    assert.equal(outcome.type, 'failed')
    if (outcome.type !== 'failed') return

    assert.equal(outcome.reason, 'max-attempts')
    assert.strictEqual(outcome.error, executionError)
    assert.isFalse(outcome.removeOnFail)
    assert.strictEqual(outcome.failedHookError, failedHookError)
    assert.instanceOf(hookError, errors.E_JOB_MAX_ATTEMPTS_REACHED)
    assert.strictEqual(hookError?.cause, executionError)
  })

  test('returns initialization-failed without applying Job-level policy', async ({ assert }) => {
    const initializationError = new Error('constructor failed')

    class BrokenJob extends Job {
      static options = { removeOnFail: { count: 1 } }

      async execute() {}
    }

    const runtime = new JobExecutionRuntime({
      resolveJob: async () => BrokenJob,
      jobFactory: async () => {
        throw initializationError
      },
      configResolver: new QueueConfigResolver({ globalJobOptions: { removeOnFail: false } }),
    })

    const outcome = await runtime.execute(acquiredJob(), 'default')

    assert.deepEqual(outcome, {
      type: 'initialization-failed',
      error: initializationError,
      removeOnFail: false,
    })
  })

  test('wraps and traces initialization failures as part of the attempt', async ({
    assert,
    cleanup,
  }) => {
    const initializationError = new Error('factory unavailable')
    const events: string[] = []
    let traceMessage: JobExecuteMessage | undefined
    const captureTrace = (message: unknown) => {
      events.push('trace-end')
      traceMessage = { ...(message as JobExecuteMessage) }
    }

    executeChannel.asyncEnd.subscribe(captureTrace)
    cleanup(() => executeChannel.asyncEnd.unsubscribe(captureTrace))

    class UninitializedJob extends Job {
      async execute() {}
    }

    const runtime = new JobExecutionRuntime({
      resolveJob: async () => {
        events.push('resolve')
        return UninitializedJob
      },
      jobFactory: async () => {
        events.push('factory')
        throw initializationError
      },
      configResolver: new QueueConfigResolver({}),
      executionWrapper: async (run) => {
        events.push('wrapper-start')
        const outcome = await run()
        events.push('wrapper-end')
        return outcome
      },
    })

    const outcome = await runtime.execute(acquiredJob(), 'default')

    assert.equal(outcome.type, 'initialization-failed')
    assert.deepEqual(events, ['wrapper-start', 'resolve', 'factory', 'trace-end', 'wrapper-end'])
    assert.equal(traceMessage?.status, 'failed')
    assert.strictEqual(traceMessage?.error, initializationError)
    assert.isNumber(traceMessage?.duration)
  })

  test('aborts timed out Jobs and removes the abort listener', async ({ assert, cleanup }) => {
    const controller = new AbortController()
    const originalTimeout = AbortSignal.timeout
    const originalAddEventListener = controller.signal.addEventListener.bind(controller.signal)
    const originalRemoveEventListener = controller.signal.removeEventListener.bind(
      controller.signal
    )
    let addedAbortListeners = 0
    let removedAbortListeners = 0
    let failedError: Error | undefined

    controller.signal.addEventListener = ((type: string, listener: any, options: any) => {
      if (type === 'abort') addedAbortListeners++
      return originalAddEventListener(type, listener, options)
    }) as AbortSignal['addEventListener']
    controller.signal.removeEventListener = ((type: string, listener: any, options: any) => {
      if (type === 'abort') removedAbortListeners++
      return originalRemoveEventListener(type, listener, options)
    }) as AbortSignal['removeEventListener']
    AbortSignal.timeout = (() => controller.signal) as typeof AbortSignal.timeout
    cleanup(() => {
      AbortSignal.timeout = originalTimeout
    })

    class TimedOutJob extends Job {
      static options = { timeout: '1s', failOnTimeout: true }

      async execute() {
        await new Promise(() => {})
      }

      async failed(error: Error) {
        failedError = error
      }
    }

    const runtime = new JobExecutionRuntime({
      resolveJob: async () => TimedOutJob,
      configResolver: new QueueConfigResolver({}),
    })
    const execution = runtime.execute(acquiredJob(), 'default')
    await Promise.resolve()

    controller.abort()
    const outcome = await execution

    assert.equal(outcome.type, 'failed')
    if (outcome.type !== 'failed') return

    assert.equal(outcome.reason, 'timeout')
    assert.instanceOf(outcome.error, errors.E_JOB_TIMEOUT)
    assert.strictEqual(failedError, outcome.error)
    assert.equal(addedAbortListeners, 1)
    assert.equal(removedAbortListeners, 1)
  })
})
