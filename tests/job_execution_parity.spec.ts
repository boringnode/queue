import { setTimeout as sleep } from 'node:timers/promises'
import { test } from '@japa/runner'
import { Job } from '../src/job.js'
import { Worker } from '../src/worker.js'
import { Locator } from '../src/locator.js'
import { QueueManager } from '../src/queue_manager.js'
import { sync } from '../src/drivers/sync_adapter.js'
import { memory } from './_mocks/memory_adapter.js'
import * as errors from '../src/exceptions.js'
import type { JobExecutionOutcome } from '../src/job_runtime.js'
import type {
  JobClass,
  JobContext,
  JobData,
  JobRecord,
  QueueManagerConfig,
} from '../src/types/main.js'

type ExecutionPath = 'sync' | 'worker'

async function executeWithSync(
  JobClass: JobClass,
  job: JobData,
  queue = 'default',
  executionWrapper?: QueueManagerConfig['executionWrapper']
) {
  await QueueManager.init({ default: 'sync', adapters: { sync: sync() }, executionWrapper })
  Locator.register(job.name, JobClass)

  try {
    await QueueManager.use().pushOn(queue, job)
  } finally {
    Locator.clear()
    await QueueManager.destroy()
  }
}

async function executeWithWorker(
  JobClass: JobClass,
  job: JobData,
  queue = 'default',
  executionWrapper?: QueueManagerConfig['executionWrapper']
): Promise<JobRecord | null> {
  const adapter = memory()()
  const config = {
    default: 'memory',
    adapters: { memory: () => adapter },
    executionWrapper,
  } satisfies QueueManagerConfig
  const worker = new Worker(config)
  Locator.register(job.name, JobClass)

  try {
    await adapter.pushOn(queue, job)

    for (let cycleCount = 0; cycleCount < 20; cycleCount++) {
      const cycle = await worker.processCycle([queue])
      if (cycle?.type === 'idle') return adapter.getJob(job.id, queue)
    }

    throw new Error('Worker did not become idle after executing the parity scenario')
  } finally {
    Locator.clear()
    await worker.stop()
    await QueueManager.destroy()
  }
}

test.group('Job execution parity', () => {
  test('constructs the same context for Worker and Sync', async ({ assert }) => {
    const contexts: Partial<Record<ExecutionPath, JobContext>> = {}
    let path: ExecutionPath = 'sync'

    class ContextJob extends Job {
      async execute() {
        contexts[path] = this.context
      }
    }

    const job: JobData = {
      id: 'parity-context',
      name: 'ContextJob',
      payload: {},
      attempts: 2,
      priority: 1,
      stalledCount: 3,
    }

    await executeWithSync(ContextJob, job, 'emails')
    path = 'worker'
    await executeWithWorker(ContextJob, job, 'emails')

    for (const context of [contexts.sync, contexts.worker]) {
      assert.deepInclude(context, {
        jobId: 'parity-context',
        name: 'ContextJob',
        attempt: 3,
        queue: 'emails',
        priority: 1,
        stalledCount: 3,
      })
      assert.instanceOf(context?.acquiredAt, Date)
    }
  })

  test('exhausts retries and invokes failed() identically for Worker and Sync', async ({
    assert,
  }) => {
    const attempts: Record<ExecutionPath, number[]> = { sync: [], worker: [] }
    const failedErrors: Record<ExecutionPath, Error[]> = { sync: [], worker: [] }
    let path: ExecutionPath = 'sync'

    class ExhaustedJob extends Job {
      static options = { maxRetries: 1 }

      async execute() {
        attempts[path].push(this.context.attempt)
        throw new Error('boom')
      }

      async failed(error: Error) {
        failedErrors[path].push(error)
      }
    }

    const job: JobData = {
      id: 'parity-retries',
      name: 'ExhaustedJob',
      payload: {},
      attempts: 0,
    }

    await executeWithSync(ExhaustedJob, job)
    path = 'worker'
    await executeWithWorker(ExhaustedJob, job)

    assert.deepEqual(attempts.sync, [1, 2])
    assert.deepEqual(attempts.worker, attempts.sync)
    assert.lengthOf(failedErrors.sync, 1)
    assert.lengthOf(failedErrors.worker, 1)
    assert.instanceOf(failedErrors.sync[0], errors.E_JOB_MAX_ATTEMPTS_REACHED)
    assert.instanceOf(failedErrors.worker[0], errors.E_JOB_MAX_ATTEMPTS_REACHED)
  })

  test('classifies fail-on-timeout identically for Worker and Sync', async ({ assert }) => {
    const attempts: Record<ExecutionPath, number> = { sync: 0, worker: 0 }
    const failedErrors: Record<ExecutionPath, Error[]> = { sync: [], worker: [] }
    let path: ExecutionPath = 'sync'

    class TimedOutJob extends Job {
      static options = { timeout: 0, failOnTimeout: true, maxRetries: 2 }

      async execute() {
        attempts[path]++
        await sleep(25)
      }

      async failed(error: Error) {
        failedErrors[path].push(error)
      }
    }

    const job: JobData = {
      id: 'parity-timeout',
      name: 'TimedOutJob',
      payload: {},
      attempts: 0,
    }

    await executeWithSync(TimedOutJob, job)
    path = 'worker'
    await executeWithWorker(TimedOutJob, job)

    assert.equal(attempts.sync, 1)
    assert.equal(attempts.worker, attempts.sync)
    assert.lengthOf(failedErrors.sync, 1)
    assert.lengthOf(failedErrors.worker, 1)
    assert.instanceOf(failedErrors.sync[0], errors.E_JOB_TIMEOUT)
    assert.instanceOf(failedErrors.worker[0], errors.E_JOB_TIMEOUT)
  })

  test('returns the same outcome when failed() throws', async ({ assert }) => {
    const outcomes: Partial<Record<ExecutionPath, JobExecutionOutcome>> = {}

    const captureOutcome = (path: ExecutionPath): QueueManagerConfig['executionWrapper'] => {
      return async (run) => {
        const outcome = await run()
        outcomes[path] = outcome as JobExecutionOutcome
        return outcome
      }
    }

    class BrokenFailedHookJob extends Job {
      static options = { removeOnFail: false }

      async execute() {
        throw new Error('execution failed')
      }

      async failed() {
        throw new Error('failed hook exploded')
      }
    }

    const job: JobData = {
      id: 'parity-failed-hook',
      name: 'BrokenFailedHookJob',
      payload: {},
      attempts: 0,
    }

    await assert.rejects(
      () => executeWithSync(BrokenFailedHookJob, job, 'default', captureOutcome('sync')),
      'failed hook exploded'
    )
    const workerRecord = await executeWithWorker(
      BrokenFailedHookJob,
      job,
      'default',
      captureOutcome('worker')
    )

    for (const outcome of [outcomes.sync, outcomes.worker]) {
      assert.equal(outcome?.type, 'failed')
      if (outcome?.type !== 'failed') continue

      assert.equal(outcome.error.message, 'execution failed')
      assert.equal(outcome.failedHookError?.message, 'failed hook exploded')
    }

    assert.equal(workerRecord?.status, 'failed')
    assert.equal(workerRecord?.error, 'execution failed')
  })
})
