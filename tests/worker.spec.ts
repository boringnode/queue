import { test } from '@japa/runner'
import { setTimeout } from 'node:timers/promises'
import { Worker } from '../src/worker.js'
import { MemoryAdapter, memory } from './_mocks/memory_adapter.js'
import { ChaosAdapter } from './_mocks/chaos_adapter.js'
import type { QueueManagerConfig } from '../src/types/main.js'
import { Locator } from '../src/locator.js'
import { Job } from '../src/job.js'
import { QueueManager } from '../src/queue_manager.js'
import * as errors from '../src/exceptions.js'

const config = {
  default: 'memory',
  adapters: { memory: memory() },
} satisfies QueueManagerConfig

class DestroyAwareMemoryAdapter extends MemoryAdapter {
  destroyed = false

  override async pushOn(...args: Parameters<MemoryAdapter['pushOn']>) {
    if (this.destroyed) {
      throw new Error('adapter is destroyed')
    }

    return super.pushOn(...args)
  }

  override destroy(): Promise<void> {
    this.destroyed = true

    return super.destroy()
  }
}

test.group('Worker', () => {
  test('should create a worker with a unique worker ID', ({ assert, cleanup }) => {
    const worker1 = new Worker(config)
    const worker2 = new Worker(config)

    cleanup(async () => {
      await Promise.all([worker1.stop(), worker2.stop()])
    })

    assert.isString(worker1.id)
    assert.isString(worker2.id)
    assert.notEqual(worker1.id, worker2.id)
  })

  test('should yield idle when no jobs are available', async ({ assert, cleanup }) => {
    const worker = new Worker(config)

    cleanup(async () => {
      await worker.stop()
    })

    const cycle = await worker.processCycle(['default'])

    assert.isNotNull(cycle)
    // @ts-ignore
    assert.equal(cycle.type, 'idle')
    // @ts-ignore
    assert.isNumber(cycle.suggestedDelay)
  })

  test('should yield error when an exception occurs', async ({ assert, cleanup }) => {
    const chaosAdapter = new ChaosAdapter()
    chaosAdapter.alwaysThrow()

    const localConfig = {
      default: 'chaos',
      adapters: { chaos: () => chaosAdapter },
    }

    const worker = new Worker(localConfig)

    cleanup(async () => {
      await worker.stop()
    })

    const cycle = await worker.processCycle(['default'])

    assert.isNotNull(cycle)
    // @ts-ignore
    assert.equal(cycle.type, 'error')
    // @ts-ignore
    assert.isNumber(cycle.suggestedDelay)
    // @ts-ignore
    assert.isNotNull(cycle.error)
  })

  test('should yield job when a job is available', async ({ assert, cleanup }) => {
    class TestJob extends Job {
      async execute() {}
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('TestJob', TestJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'test-job-1',
      name: 'TestJob',
      payload: { to: 'romain.lanz@example.com' },
      attempts: 0,
      priority: 0,
    })

    const cycle = await worker.processCycle(['default'])

    assert.isNotNull(cycle)
    // @ts-ignore
    assert.equal(cycle.type, 'started')
    // @ts-ignore
    assert.equal(cycle.queue, 'default')
    // @ts-ignore
    assert.equal(cycle.job.id, 'test-job-1')
    // @ts-ignore
    assert.equal(cycle.job.name, 'TestJob')
  })

  test('should execute job when a job is available', async ({ assert, cleanup }) => {
    assert.plan(6)

    const payload = { foo: 'bar' }

    class TestJob extends Job {
      async execute() {
        assert.isTrue(true)
        assert.equal(this.payload, payload)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('TestJob', TestJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'test-job-2',
      name: 'TestJob',
      payload,
      attempts: 0,
      priority: 0,
    })

    const cycle1 = await worker.processCycle(['default'])
    assert.isNotNull(cycle1)
    // @ts-ignore
    assert.equal(cycle1.type, 'started')

    const cycle2 = await worker.processCycle(['default'])
    assert.isNotNull(cycle2)
    // @ts-ignore
    assert.equal(cycle2.type, 'completed')
  })

  test('should retry failed job', async ({ assert, cleanup }) => {
    const payload = { foo: 'bar' }

    class FailingJob extends Job {
      async execute() {
        throw new Error('Job failed as expected')
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      retry: {
        maxRetries: 3,
      },
    }

    Locator.register('FailingJob', FailingJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'test-job-3',
      name: 'FailingJob',
      payload,
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (rollback)
    const cycle = await worker.processCycle(['default']) // started

    // @ts-ignore
    assert.equal(cycle.job.attempts, 1)
  })

  test('should retry failed job until maxRetries is reached', async ({ assert, cleanup }) => {
    assert.plan(2)

    const payload = { foo: 'bar' }

    class FailingJob extends Job {
      async execute() {
        throw new Error('Job failed as expected')
      }

      async failed(error: Error): Promise<void> {
        assert.instanceOf(error, errors.E_JOB_MAX_ATTEMPTS_REACHED)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },

      retry: {
        maxRetries: 2,
      },
    }

    Locator.register('FailingJob', FailingJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'test-job-3',
      name: 'FailingJob',
      payload,
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (rollback)
    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed
    await worker.processCycle(['default']) // started
    const cycle = await worker.processCycle(['default']) // started

    // @ts-ignore
    assert.equal(cycle.job.attempts, 2)
  })

  test('should not retry failed job when maxRetries is not configured', async ({
    assert,
    cleanup,
  }) => {
    assert.plan(3)

    const payload = { foo: 'bar' }

    class FailingJob extends Job {
      async execute() {
        throw new Error('Job failed as expected')
      }

      async failed(error: Error): Promise<void> {
        assert.instanceOf(error, Error)
        assert.equal(error.message, 'Job failed as expected')
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('FailingJob', FailingJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'test-job-3',
      name: 'FailingJob',
      payload,
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    const cycle = await worker.processCycle(['default']) // completed

    // @ts-ignore
    assert.equal(cycle.type, 'completed')
  })

  test('should maintain concurrency when one job is slow', async ({ assert, cleanup }) => {
    const executionOrder: string[] = []
    const startTimes: Record<string, number> = {}

    class SlowJob extends Job {
      async execute() {
        startTimes[this.payload.id] = Date.now()
        await setTimeout(200)
        executionOrder.push(this.payload.id)
      }
    }

    class FastJob extends Job {
      async execute() {
        startTimes[this.payload.id] = Date.now()
        await setTimeout(10)
        executionOrder.push(this.payload.id)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      worker: {
        concurrency: 2,
      },
    }

    Locator.register('SlowJob', SlowJob)
    Locator.register('FastJob', FastJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    // Push jobs: 1 slow job + 3 fast jobs
    await sharedAdapter.push({
      id: 'job-1',
      name: 'SlowJob',
      payload: { id: 'slow-1' },
      attempts: 0,
      priority: 0,
    })

    await sharedAdapter.push({
      id: 'job-2',
      name: 'FastJob',
      payload: { id: 'fast-1' },
      attempts: 0,
      priority: 0,
    })

    await sharedAdapter.push({
      id: 'job-3',
      name: 'FastJob',
      payload: { id: 'fast-2' },
      attempts: 0,
      priority: 0,
    })

    await sharedAdapter.push({
      id: 'job-4',
      name: 'FastJob',
      payload: { id: 'fast-3' },
      attempts: 0,
      priority: 0,
    })

    // Start the worker and let it process all jobs
    const startTime = Date.now()

    // Process until idle (all jobs done)
    let cycles = 0
    const maxCycles = 20
    while (cycles < maxCycles) {
      const cycle = await worker.processCycle(['default'])
      cycles++

      if (cycle?.type === 'idle') {
        break
      }
    }

    const totalTime = Date.now() - startTime

    // All 4 jobs should have executed
    assert.equal(executionOrder.length, 4)

    // With proper concurrency, fast jobs should complete before slow job
    // fast-1 starts with slow-1, completes quickly, then fast-2 starts, etc.
    // So execution order should be: fast-1, fast-2, fast-3, slow-1
    // (fast jobs complete before the slow job)
    assert.equal(executionOrder[executionOrder.length - 1], 'slow-1')

    // Total time should be around 200ms (slow job time) + overhead
    // NOT 200ms + 3*10ms in sequence
    // If batch processing was used, it would take ~200ms per batch
    // With proper pool, all fast jobs run while slow job runs
    assert.isBelow(totalTime, 350, 'Total time should be close to slow job time, not cumulative')
  })

  test('should timeout job that exceeds timeout duration', async ({ assert, cleanup }) => {
    assert.plan(2)

    class SlowJob extends Job {
      static options = { timeout: 50 }

      async execute() {
        await setTimeout(200)
      }

      async failed(error: Error) {
        assert.instanceOf(error, errors.E_JOB_TIMEOUT)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('SlowJob', SlowJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'timeout-job',
      name: 'SlowJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const startTime = Date.now()

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (timeout)

    const elapsed = Date.now() - startTime

    assert.isBelow(elapsed, 150, 'Job should be killed before completing')
  })

  test('should apply timeout when timeout is set to 0', async ({ assert, cleanup }) => {
    assert.plan(2)

    class ZeroTimeoutJob extends Job {
      static options = { timeout: 0 }

      async execute() {
        await setTimeout(200)
      }

      async failed(error: Error) {
        assert.instanceOf(error, errors.E_JOB_TIMEOUT)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('ZeroTimeoutJob', ZeroTimeoutJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'timeout-zero-job',
      name: 'ZeroTimeoutJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const startTime = Date.now()

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (timeout)

    const elapsed = Date.now() - startTime

    assert.isBelow(elapsed, 150, 'Job should be killed before completing')
  })

  test('should remove timeout abort listener when job completes before timeout', async ({
    assert,
    cleanup,
  }) => {
    class FastJob extends Job {
      static options = { timeout: 5_000 }

      async execute() {}
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    const controller = new AbortController()
    const originalTimeout = AbortSignal.timeout
    const originalAddEventListener = controller.signal.addEventListener.bind(controller.signal)
    const originalRemoveEventListener = controller.signal.removeEventListener.bind(
      controller.signal
    )

    let addedAbortListeners = 0
    let removedAbortListeners = 0

    controller.signal.addEventListener = ((type: string, listener: any, options: any) => {
      if (type === 'abort') {
        addedAbortListeners++
      }

      return originalAddEventListener(type, listener, options)
    }) as AbortSignal['addEventListener']

    controller.signal.removeEventListener = ((type: string, listener: any, options: any) => {
      if (type === 'abort') {
        removedAbortListeners++
      }

      return originalRemoveEventListener(type, listener, options)
    }) as AbortSignal['removeEventListener']

    AbortSignal.timeout = (() => controller.signal) as typeof AbortSignal.timeout

    Locator.register('FastJob', FastJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      AbortSignal.timeout = originalTimeout
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'cleanup-timeout-listener-job',
      name: 'FastJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed

    assert.equal(addedAbortListeners, 1)
    assert.equal(removedAbortListeners, 1)
  })

  test('should retry timed out job when failOnTimeout is false', async ({ assert, cleanup }) => {
    let attempts = 0

    class SlowJob extends Job {
      static options = { timeout: 50, retry: { maxRetries: 2 } }

      async execute() {
        attempts++
        await setTimeout(200)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('SlowJob', SlowJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'timeout-retry-job',
      name: 'SlowJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    // First attempt
    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (timeout)

    // Second attempt (retried)
    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (timeout)

    assert.equal(attempts, 2)
  })

  test('should not retry timed out job when failOnTimeout is true', async ({ assert, cleanup }) => {
    assert.plan(3)

    let attempts = 0

    class SlowJob extends Job {
      static options = { timeout: 50, failOnTimeout: true, retry: { maxRetries: 3 } }

      async execute() {
        attempts++
        await setTimeout(200)
      }

      async failed(error: Error) {
        assert.instanceOf(error, errors.E_JOB_TIMEOUT)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('SlowJob', SlowJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'timeout-fail-job',
      name: 'SlowJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (timeout, failed)

    // Should not retry
    const cycle = await worker.processCycle(['default'])
    // @ts-ignore
    assert.equal(cycle.type, 'idle')
    assert.equal(attempts, 1)
  })

  test('should use global worker timeout when job timeout is not set', async ({
    assert,
    cleanup,
  }) => {
    assert.plan(2)

    class SlowJob extends Job {
      async execute() {
        await setTimeout(200)
      }

      async failed(error: Error) {
        assert.instanceOf(error, errors.E_JOB_TIMEOUT)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      worker: {
        timeout: 50,
      },
    }

    Locator.register('SlowJob', SlowJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'global-timeout-job',
      name: 'SlowJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const startTime = Date.now()

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (timeout)

    const elapsed = Date.now() - startTime

    assert.isBelow(elapsed, 150)
  })

  test('should wait for running jobs to complete before stopping', async ({ assert, cleanup }) => {
    let jobCompleted = false

    class SlowJob extends Job {
      async execute() {
        await setTimeout(100)
        jobCompleted = true
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('SlowJob', SlowJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
    })

    await sharedAdapter.push({
      id: 'slow-job',
      name: 'SlowJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    // Start the job - it's now running in the pool
    await worker.processCycle(['default'])

    // Call stop while job is still running (job takes 100ms)
    await worker.stop()

    // Job should have completed before stop() returned
    assert.isTrue(jobCompleted, 'Job should have completed before worker stopped')
  })

  test('should not destroy the shared adapter when stopping', async ({ assert, cleanup }) => {
    const adapters: DestroyAwareMemoryAdapter[] = []

    const localConfig = {
      default: 'memory',
      adapters: {
        memory: () => {
          const adapter = new DestroyAwareMemoryAdapter()
          adapters.push(adapter)
          return adapter
        },
      },
    }

    const worker = new Worker(localConfig)

    cleanup(async () => {
      await QueueManager.destroy()
    })

    await worker.init()

    const firstAdapter = QueueManager.use()

    await worker.stop()

    const secondAdapter = QueueManager.use()

    assert.strictEqual(secondAdapter, firstAdapter)
    assert.equal(adapters.length, 1)
    assert.isFalse(adapters[0].destroyed)

    await secondAdapter.pushOn('default', {
      id: 'post-stop-job',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    await QueueManager.destroy()

    assert.isTrue(adapters[0].destroyed)
  })

  test('should handle job that fails permanently', async ({ assert, cleanup }) => {
    let failedCalled = false

    class FailingJob extends Job {
      async execute() {
        throw new Error('Job failed')
      }

      async failed() {
        failedCalled = true
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('FailingJob', FailingJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'failing-job',
      name: 'FailingJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed

    assert.isTrue(failedCalled, 'Failed callback should be called')
  })

  test('should handle job class not found', async ({ assert, cleanup }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'unknown-job',
      name: 'UnknownJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    const cycle = await worker.processCycle(['default']) // completed (job failed)

    // Job initialization failure is handled gracefully - job is marked as failed
    // @ts-ignore
    assert.equal(cycle.type, 'completed')
  })

  test('should handle job constructor that throws', async ({ assert, cleanup }) => {
    class BrokenJob extends Job {
      constructor() {
        super()
        throw new Error('Constructor failed')
      }

      async execute() {}
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('BrokenJob', BrokenJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'broken-job',
      name: 'BrokenJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    const cycle = await worker.processCycle(['default']) // completed (job failed)

    // Job initialization failure is handled gracefully - job is marked as failed
    // @ts-ignore
    assert.equal(cycle.type, 'completed')
  })

  test('should recover stalled jobs during processing', async ({ assert, cleanup }) => {
    let executionCount = 0

    class TestJob extends Job {
      async execute() {
        executionCount++
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      worker: {
        stalledThreshold: 50,
        stalledInterval: 50,
        maxStalledCount: 2,
      },
    }

    Locator.register('TestJob', TestJob)

    // Simulate a stalled job by pushing directly to adapter and acquiring it
    // without completing it
    sharedAdapter.setWorkerId('crashed-worker')
    await sharedAdapter.pushOn('default', {
      id: 'stalled-job-1',
      name: 'TestJob',
      payload: { test: true },
      attempts: 0,
    })

    // Acquire the job (simulating a worker that then crashed)
    await sharedAdapter.popFrom('default')

    // Wait for job to become stalled
    await setTimeout(100)

    // Now start a new worker that should recover the stalled job
    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    // Run a few cycles - the stalled job checker should recover the job
    // and then the worker should process it
    let cycles = 0
    let foundStarted = false
    while (cycles < 10) {
      const cycle = await worker.processCycle(['default'])
      cycles++

      if (cycle?.type === 'started') {
        foundStarted = true
      }

      if (cycle?.type === 'idle' && foundStarted) {
        break
      }
    }

    assert.isTrue(foundStarted, 'Worker should have started the recovered job')
    assert.equal(executionCount, 1, 'Job should have been executed once')
  })

  test('should fail stalled job permanently after maxStalledCount exceeded', async ({
    assert,
    cleanup,
  }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      worker: {
        stalledThreshold: 50,
        stalledInterval: 50,
        maxStalledCount: 1,
      },
    }

    class TestJob extends Job {
      async execute() {}
    }

    Locator.register('TestJob', TestJob)

    // Create a job that has already been stalled once (stalledCount = 1)
    sharedAdapter.setWorkerId('crashed-worker')
    await sharedAdapter.pushOn('default', {
      id: 'multi-stalled-job',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      stalledCount: 1, // Already stalled once
    })

    // Acquire it (simulating another crash)
    await sharedAdapter.popFrom('default')

    // Wait for it to become stalled
    await setTimeout(100)

    // Now start a worker - it should detect the stalled job but fail it permanently
    // because stalledCount (1) >= maxStalledCount (1)
    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    // Run cycles - job should NOT be recovered, just removed
    let cycles = 0
    let foundJob = false
    while (cycles < 5) {
      const cycle = await worker.processCycle(['default'])
      cycles++

      if (cycle?.type === 'started') {
        foundJob = true
      }

      if (cycle?.type === 'idle') {
        break
      }
    }

    assert.isFalse(foundJob, 'Job should not have been recovered - it exceeded maxStalledCount')
  })

  test('should not process the same job multiple times with concurrency > 1', async ({
    assert,
    cleanup,
  }) => {
    const jobExecutions: Map<string, number> = new Map()

    class TrackingJob extends Job<{ jobId: string }> {
      async execute() {
        const count = jobExecutions.get(this.payload.jobId) || 0
        jobExecutions.set(this.payload.jobId, count + 1)
        // Add a small delay to ensure concurrent execution window
        await setTimeout(50)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      worker: {
        concurrency: 5,
      },
    }

    Locator.register('TrackingJob', TrackingJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    // Push only ONE job but with concurrency of 5
    await sharedAdapter.push({
      id: 'single-job',
      name: 'TrackingJob',
      payload: { jobId: 'job-1' },
      attempts: 0,
      priority: 0,
    })

    // Process until idle
    let cycles = 0
    const maxCycles = 20
    while (cycles < maxCycles) {
      const cycle = await worker.processCycle(['default'])
      cycles++

      if (cycle?.type === 'idle') {
        break
      }
    }

    // The job should have been executed exactly ONCE
    assert.equal(
      jobExecutions.get('job-1'),
      1,
      'Job should be executed exactly once, not multiple times due to concurrency'
    )
  })

  test('should process each job exactly once with multiple jobs and high concurrency', async ({
    assert,
    cleanup,
  }) => {
    const jobExecutions: Map<string, number> = new Map()

    class TrackingJob extends Job<{ jobId: string }> {
      async execute() {
        const count = jobExecutions.get(this.payload.jobId) || 0
        jobExecutions.set(this.payload.jobId, count + 1)
        // Add delay to create overlap window
        await setTimeout(30)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      worker: {
        concurrency: 5,
      },
    }

    Locator.register('TrackingJob', TrackingJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    // Push 3 jobs with concurrency of 5
    for (let i = 1; i <= 3; i++) {
      await sharedAdapter.push({
        id: `job-${i}`,
        name: 'TrackingJob',
        payload: { jobId: `job-${i}` },
        attempts: 0,
        priority: 0,
      })
    }

    // Process until idle
    let cycles = 0
    const maxCycles = 30
    while (cycles < maxCycles) {
      const cycle = await worker.processCycle(['default'])
      cycles++

      if (cycle?.type === 'idle') {
        break
      }
    }

    // Each job should have been executed exactly ONCE
    assert.equal(jobExecutions.size, 3, 'All 3 jobs should have been executed')
    for (const [jobId, count] of jobExecutions) {
      assert.equal(count, 1, `${jobId} should be executed exactly once`)
    }
  })

  test('onShutdownSignal callback is invoked on SIGTERM', async ({ assert }) => {
    let callbackInvoked = false

    const localConfig = {
      default: 'memory',
      adapters: { memory: memory() },
      worker: {
        gracefulShutdown: true,
        onShutdownSignal: () => {
          callbackInvoked = true
        },
      },
    }

    const worker = new Worker(localConfig)
    const startPromise = worker.start(['default'])
    await setTimeout(10)

    // Emit SIGTERM to trigger the shutdown handler
    process.emit('SIGTERM')

    // Wait for the worker to stop
    await Promise.race([startPromise, setTimeout(500)])

    assert.isTrue(callbackInvoked, 'onShutdownSignal should be called on SIGTERM')
  })

  test('onShutdownSignal callback is invoked on SIGINT', async ({ assert }) => {
    let callbackInvoked = false

    const localConfig = {
      default: 'memory',
      adapters: { memory: memory() },
      worker: {
        gracefulShutdown: true,
        onShutdownSignal: () => {
          callbackInvoked = true
        },
      },
    }

    const worker = new Worker(localConfig)
    const startPromise = worker.start(['default'])
    await setTimeout(10)

    // Emit SIGINT to trigger the shutdown handler
    process.emit('SIGINT')

    // Wait for the worker to stop
    await Promise.race([startPromise, setTimeout(500)])

    assert.isTrue(callbackInvoked, 'onShutdownSignal should be called on SIGINT')
  })
})

test.group('Worker | jobFactory', () => {
  test('should use custom jobFactory to instantiate jobs', async ({ assert, cleanup }) => {
    class EmailService {
      sent = false
      async send() {
        this.sent = true
      }
    }

    class SendEmailJob extends Job<{ to: string }> {
      constructor(public emailService: EmailService) {
        super()
      }

      async execute() {
        await this.emailService.send()
      }
    }

    const emailService = new EmailService()
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      jobFactory: async (JobClass: any) => {
        return new JobClass(emailService)
      },
    }

    // SendEmailJob has a non-standard constructor (requires injected EmailService)
    // This is exactly the use case for jobFactory - jobs with DI dependencies
    Locator.register('SendEmailJob', SendEmailJob as any)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'email-job-1',
      name: 'SendEmailJob',
      payload: { to: 'test@example.com' },
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed

    assert.isTrue(emailService.sent, 'EmailService should have been used via injected dependency')
  })

  test('should pass correct JobClass to jobFactory', async ({ assert, cleanup }) => {
    let receivedJobClass: any

    class TestJob extends Job {
      async execute() {}
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      jobFactory: async (JobClass: any) => {
        receivedJobClass = JobClass
        return new JobClass()
      },
    }

    Locator.register('TestJob', TestJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'test-job-factory',
      name: 'TestJob',
      payload: { foo: 'bar', count: 42 },
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed

    assert.equal(receivedJobClass, TestJob, 'Factory should receive the correct JobClass')
  })

  test('should support async jobFactory for IoC resolution', async ({ assert, cleanup }) => {
    let asyncResolutionCompleted = false

    class TestJob extends Job {
      async execute() {}
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      jobFactory: async (JobClass: any) => {
        // Simulate async IoC container resolution
        await setTimeout(10)
        asyncResolutionCompleted = true
        return new JobClass()
      },
    }

    Locator.register('TestJob', TestJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'async-factory-job',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed

    assert.isTrue(asyncResolutionCompleted, 'Async factory should have completed resolution')
  })

  test('should fall back to default instantiation when jobFactory is not provided', async ({
    assert,
    cleanup,
  }) => {
    let executeWasCalled = false

    class TestJob extends Job {
      async execute() {
        executeWasCalled = true
      }
    }

    const sharedAdapter = memory()()

    // No jobFactory provided
    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('TestJob', TestJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'default-instantiation-job',
      name: 'TestJob',
      payload: { test: true },
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed

    assert.isTrue(executeWasCalled, 'Job should be instantiated and executed with default behavior')
  })
})

test.group('Worker | JobContext', () => {
  test('should expose jobId in context', async ({ assert, cleanup }) => {
    let receivedJobId: string | undefined

    class TestJob extends Job {
      async execute() {
        receivedJobId = this.context.jobId
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('TestJob', TestJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'my-unique-job-id',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed

    assert.equal(receivedJobId, 'my-unique-job-id')
  })

  test('should expose attempt number in context (1-based)', async ({ assert, cleanup }) => {
    const receivedAttempts: number[] = []

    class FailingJob extends Job {
      async execute() {
        receivedAttempts.push(this.context.attempt)
        throw new Error('Intentional failure')
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      retry: { maxRetries: 3 },
    }

    Locator.register('FailingJob', FailingJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'retry-job',
      name: 'FailingJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    // First attempt
    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (failed, queued for retry)

    // Second attempt
    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (failed, queued for retry)

    // Third attempt
    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (failed, queued for retry)

    assert.deepEqual(receivedAttempts, [1, 2, 3])
  })

  test('should expose queue name in context', async ({ assert, cleanup }) => {
    let receivedQueue: string | undefined

    class TestJob extends Job {
      async execute() {
        receivedQueue = this.context.queue
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('TestJob', TestJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.pushOn('emails', {
      id: 'email-job',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    await worker.processCycle(['emails']) // started
    await worker.processCycle(['emails']) // completed

    assert.equal(receivedQueue, 'emails')
  })

  test('should expose all context properties', async ({ assert, cleanup }) => {
    let receivedContext: any

    class TestJob extends Job {
      async execute() {
        receivedContext = this.context
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('TestJob', TestJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.pushOn('high-priority', {
      id: 'context-job',
      name: 'TestJob',
      payload: { foo: 'bar' },
      attempts: 2,
      priority: 1,
    })

    await worker.processCycle(['high-priority']) // started
    await worker.processCycle(['high-priority']) // completed

    assert.equal(receivedContext.jobId, 'context-job')
    assert.equal(receivedContext.name, 'TestJob')
    assert.equal(receivedContext.attempt, 3) // attempts was 2, so this is attempt 3
    assert.equal(receivedContext.queue, 'high-priority')
    assert.equal(receivedContext.priority, 1)
    assert.instanceOf(receivedContext.acquiredAt, Date)
    assert.equal(receivedContext.stalledCount, 0)
  })

  test('should expose context in failed() hook', async ({ assert, cleanup }) => {
    let contextInFailed: any

    class FailingJob extends Job {
      async execute() {
        throw new Error('Intentional failure')
      }

      async failed() {
        contextInFailed = this.context
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('FailingJob', FailingJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'failed-context-job',
      name: 'FailingJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed (failed)

    assert.equal(contextInFailed.jobId, 'failed-context-job')
    assert.equal(contextInFailed.attempt, 1)
  })

  test('context should be frozen (immutable)', async ({ assert, cleanup }) => {
    let contextIsFrozen = false

    class TestJob extends Job {
      async execute() {
        contextIsFrozen = Object.isFrozen(this.context)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('TestJob', TestJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    await sharedAdapter.push({
      id: 'frozen-context-job',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed

    assert.isTrue(contextIsFrozen, 'Context should be frozen')
  })
})

test.group('Worker | Scheduler Integration', () => {
  test('should dispatch job when schedule is due', async ({ assert, cleanup }) => {
    let jobExecuted = false
    let receivedPayload: any

    class ScheduledJob extends Job {
      async execute() {
        jobExecuted = true
        receivedPayload = this.payload
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('ScheduledJob', ScheduledJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    // Create a due schedule
    await sharedAdapter.upsertSchedule({
      id: 'test-schedule',
      name: 'ScheduledJob',
      payload: { scheduled: true },
      everyMs: 60000,
      timezone: 'UTC',
    })

    // Set nextRunAt to the past so it's due
    await sharedAdapter.updateSchedule('test-schedule', {
      nextRunAt: new Date(Date.now() - 1000),
    })

    // Process cycles - should pick up and execute the scheduled job
    await worker.processCycle(['default']) // should dispatch the scheduled job
    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed

    assert.isTrue(jobExecuted, 'Scheduled job should have been executed')
    assert.deepEqual(receivedPayload, { scheduled: true })
  })

  test('should not dispatch job when schedule is not due', async ({ assert, cleanup }) => {
    let jobExecuted = false

    class ScheduledJob extends Job {
      async execute() {
        jobExecuted = true
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('ScheduledJob', ScheduledJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    // Create a schedule that's not due yet (future)
    await sharedAdapter.upsertSchedule({
      id: 'future-schedule',
      name: 'ScheduledJob',
      payload: {},
      everyMs: 60000,
      timezone: 'UTC',
    })

    await sharedAdapter.updateSchedule('future-schedule', {
      nextRunAt: new Date(Date.now() + 60000), // 1 minute in the future
    })

    // Process a cycle
    const cycle = await worker.processCycle(['default'])

    // Should be idle since no jobs are due
    assert.equal(cycle?.type, 'idle')
    assert.isFalse(jobExecuted, 'Job should not have been executed')
  })

  test('should update schedule runCount after job is dispatched', async ({ assert, cleanup }) => {
    class ScheduledJob extends Job {
      async execute() {}
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('ScheduledJob', ScheduledJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    // Create a due schedule
    await sharedAdapter.upsertSchedule({
      id: 'count-schedule',
      name: 'ScheduledJob',
      payload: {},
      everyMs: 60000,
      timezone: 'UTC',
    })

    await sharedAdapter.updateSchedule('count-schedule', {
      nextRunAt: new Date(Date.now() - 1000),
    })

    // Check initial state
    const before = await sharedAdapter.getSchedule('count-schedule')
    assert.equal(before?.runCount, 0)

    // Process cycles
    await worker.processCycle(['default']) // dispatch
    await worker.processCycle(['default']) // started
    await worker.processCycle(['default']) // completed

    // Check that runCount was incremented
    const after = await sharedAdapter.getSchedule('count-schedule')
    assert.equal(after?.runCount, 1)
  })

  test('should dispatch to correct queue based on job options', async ({ assert, cleanup }) => {
    let executedOnQueue: string | undefined

    class QueuedScheduledJob extends Job {
      static options = { queue: 'scheduled-queue' }

      async execute() {
        executedOnQueue = this.context.queue
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('QueuedScheduledJob', QueuedScheduledJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    // Create a due schedule
    await sharedAdapter.upsertSchedule({
      id: 'queued-schedule',
      name: 'QueuedScheduledJob',
      payload: {},
      everyMs: 60000,
      timezone: 'UTC',
    })

    await sharedAdapter.updateSchedule('queued-schedule', {
      nextRunAt: new Date(Date.now() - 1000),
    })

    // Process on 'scheduled-queue'
    await worker.processCycle(['scheduled-queue']) // dispatch
    await worker.processCycle(['scheduled-queue']) // started
    await worker.processCycle(['scheduled-queue']) // completed

    assert.equal(executedOnQueue, 'scheduled-queue')
  })

  test('should not dispatch paused schedules', async ({ assert, cleanup }) => {
    let jobExecuted = false

    class ScheduledJob extends Job {
      async execute() {
        jobExecuted = true
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('ScheduledJob', ScheduledJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    // Create a due schedule but pause it
    await sharedAdapter.upsertSchedule({
      id: 'paused-schedule',
      name: 'ScheduledJob',
      payload: {},
      everyMs: 60000,
      timezone: 'UTC',
    })

    await sharedAdapter.updateSchedule('paused-schedule', {
      nextRunAt: new Date(Date.now() - 1000),
      status: 'paused',
    })

    // Process a cycle
    const cycle = await worker.processCycle(['default'])

    assert.equal(cycle?.type, 'idle')
    assert.isFalse(jobExecuted, 'Paused schedule should not dispatch jobs')
  })

  test('should handle multiple due schedules', async ({ assert, cleanup }) => {
    const executedJobs: string[] = []

    class MultiScheduleJob extends Job<{ name: string }> {
      async execute() {
        executedJobs.push(this.payload.name)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('MultiScheduleJob', MultiScheduleJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    // Create multiple due schedules
    for (const name of ['job-a', 'job-b', 'job-c']) {
      await sharedAdapter.upsertSchedule({
        id: `schedule-${name}`,
        name: 'MultiScheduleJob',
        payload: { name },
        everyMs: 60000,
        timezone: 'UTC',
      })

      await sharedAdapter.updateSchedule(`schedule-${name}`, {
        nextRunAt: new Date(Date.now() - 1000),
      })
    }

    // Process multiple cycles to handle all schedules
    for (let i = 0; i < 9; i++) {
      await worker.processCycle(['default'])
    }

    assert.equal(executedJobs.length, 3)
    assert.includeMembers(executedJobs, ['job-a', 'job-b', 'job-c'])
  })

  test('should continue processing regular jobs alongside scheduled jobs', async ({
    assert,
    cleanup,
  }) => {
    const executedJobs: string[] = []

    class RegularJob extends Job<{ type: string }> {
      async execute() {
        executedJobs.push(this.payload.type)
      }
    }

    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    Locator.register('RegularJob', RegularJob)

    const worker = new Worker(localConfig)

    cleanup(async () => {
      Locator.clear()
      await worker.stop()
    })

    // Create a due schedule
    await sharedAdapter.upsertSchedule({
      id: 'regular-schedule',
      name: 'RegularJob',
      payload: { type: 'scheduled' },
      everyMs: 60000,
      timezone: 'UTC',
    })

    await sharedAdapter.updateSchedule('regular-schedule', {
      nextRunAt: new Date(Date.now() - 1000),
    })

    // Also push a regular job
    await sharedAdapter.push({
      id: 'regular-job-1',
      name: 'RegularJob',
      payload: { type: 'regular' },
      attempts: 0,
    })

    // Process cycles
    for (let i = 0; i < 6; i++) {
      await worker.processCycle(['default'])
    }

    assert.equal(executedJobs.length, 2)
    assert.includeMembers(executedJobs, ['scheduled', 'regular'])
  })
})
