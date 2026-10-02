import { setTimeout } from 'node:timers/promises'
import { test } from '@japa/runner'
import { fake } from '../src/drivers/fake_adapter.js'
import { JobPool } from '../src/job_pool.js'
import {
  WorkerSession,
  type JobExecutor,
  type WorkerSessionOptions,
  type WorkerSessionSettings,
} from '../src/worker_session.js'
import { ControllableAdapter } from './_mocks/controllable_adapter.js'
import { trackPromise } from './_utils/track_promise.js'
import { MemoryLogger } from './_mocks/memory_logger.js'

type SessionOverrides = Partial<Omit<WorkerSessionOptions, 'settings' | 'jobExecutionRuntime'>> & {
  jobExecutionRuntime?: Partial<JobExecutor>
  settings?: Partial<WorkerSessionSettings>
}

function createSession(overrides: SessionOverrides = {}): WorkerSession {
  const { settings, jobExecutionRuntime, ...options } = overrides

  return new WorkerSession({
    workerId: 'test-worker',
    queues: ['default'],
    adapter: fake()(),
    jobExecutionRuntime: {
      execute: async () => ({ type: 'completed' as const }),
      failStalled: async () => ({
        type: 'failed' as const,
        reason: 'stalled' as const,
        error: new Error('stalled'),
      }),
      ...jobExecutionRuntime,
    },
    scheduleDispatcher: {
      dispatch: async () => ({ jobId: 'scheduled-job' }),
    },
    wrapInternal: (operation) => operation(),
    logger: new MemoryLogger(),
    settings: {
      concurrency: 1,
      idleDelay: 10,
      stalledInterval: 30_000,
      stalledThreshold: 30_000,
      maxStalledCount: 1,
      unknownJobRetries: 10,
      ...settings,
    },
    ...options,
  })
}

test.group('WorkerSession', () => {
  test('returns an idle cycle when no work is available', async ({ assert, cleanup }) => {
    const session = createSession()

    cleanup(() => session.stop())

    const cycle = await session.processCycle()

    assert.deepEqual(cycle, { type: 'idle', suggestedDelay: 10 })
  })

  test('checks each queue only once when a high-concurrency worker is idle', async ({
    assert,
    cleanup,
  }) => {
    const adapter = new ControllableAdapter()
    const session = createSession({
      adapter,
      queues: ['critical', 'default', 'low'],
      settings: { concurrency: 50 },
    })

    cleanup(() => session.stop())

    const cycle = await session.processCycle()

    assert.deepEqual(cycle, { type: 'idle', suggestedDelay: 10 })
    assert.deepEqual(adapter.polledQueues, ['critical', 'default', 'low'])
  })

  test('fans out remaining acquisitions after the initial probe finds work', async ({
    assert,
    cleanup,
  }) => {
    const adapter = new ControllableAdapter()
    const executionGate = Promise.withResolvers<void>()
    let executionsStarted = 0
    adapter.acquisitions.block(2, 3)

    const session = createSession({
      adapter,
      jobExecutionRuntime: {
        async execute() {
          executionsStarted++
          await executionGate.promise
          return { type: 'completed' }
        },
      },
      settings: { concurrency: 3 },
    })

    cleanup(async () => {
      adapter.releaseAll()
      executionGate.resolve()
      await session.stop()
    })

    for (let index = 1; index <= 3; index++) {
      await adapter.pushOn('default', {
        id: `backlog-job-${index}`,
        name: 'TestJob',
        payload: {},
        attempts: 0,
        priority: 0,
      })
    }

    const cycle = session.processCycle()
    await adapter.acquisitions.waitForStarted(3)

    assert.equal(adapter.acquisitions.calls, 3)

    adapter.acquisitions.release(2, 3)

    assert.equal((await cycle)?.type, 'started')
    assert.equal(executionsStarted, 3)
  })

  test('uses one probe per subsequent empty tick after processing one job', async ({
    assert,
    cleanup,
  }) => {
    const adapter = new ControllableAdapter()
    const session = createSession({ adapter, settings: { concurrency: 10 } })

    cleanup(() => session.stop())

    await adapter.pushOn('default', {
      id: 'only-job',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    assert.equal((await session.processCycle())?.type, 'started')
    assert.equal(adapter.acquisitions.calls, 10)
    assert.equal((await session.processCycle())?.type, 'completed')

    assert.equal((await session.processCycle())?.type, 'idle')
    assert.equal(adapter.acquisitions.calls, 11)

    assert.equal((await session.processCycle())?.type, 'idle')
    assert.equal(adapter.acquisitions.calls, 12)
  })

  test('reuses one completion wait across idle ticks and observes a later job', async ({
    assert,
    cleanup,
  }) => {
    const adapter = new ControllableAdapter()
    const longExecution = Promise.withResolvers<void>()
    const originalWaitForNextCompletion = JobPool.prototype.waitForNextCompletion
    let completionWaits = 0
    let completionObservers = 0

    JobPool.prototype.waitForNextCompletion = function () {
      completionWaits++
      const operation = originalWaitForNextCompletion.call(this)
      return {
        then(onFulfilled, onRejected) {
          completionObservers++
          return operation.then(onFulfilled, onRejected)
        },
      } as Promise<Awaited<typeof operation>>
    }

    const session = createSession({
      adapter,
      jobExecutionRuntime: {
        async execute(job) {
          if (job.id === 'long-running') {
            await longExecution.promise
          }
          return { type: 'completed' }
        },
      },
      settings: { concurrency: 2, idleDelay: 1 },
    })

    cleanup(async () => {
      JobPool.prototype.waitForNextCompletion = originalWaitForNextCompletion
      longExecution.resolve()
      await session.stop()
    })

    await adapter.pushOn('default', {
      id: 'long-running',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    assert.equal((await session.processCycle())?.type, 'started')

    const laterCycle = session.processCycle()
    await adapter.acquisitions.waitForStarted(8)
    assert.equal(completionWaits, 1)
    assert.equal(completionObservers, 1)

    await adapter.pushOn('default', {
      id: 'later-short-job',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const started = await laterCycle
    assert.equal(started?.type, 'started')
    if (started?.type === 'started') {
      assert.equal(started.job.id, 'later-short-job')
    }

    const completed = await session.processCycle()
    assert.equal(completed?.type, 'completed')
    if (completed?.type === 'completed') {
      assert.equal(completed.job.id, 'later-short-job')
    }
    assert.equal(completionWaits, 1)
    assert.equal(completionObservers, 1)
  })

  test('stops cleanly with a completion wait pending', async ({ assert, cleanup }) => {
    const adapter = new ControllableAdapter()
    const execution = Promise.withResolvers<void>()
    const session = createSession({
      adapter,
      jobExecutionRuntime: {
        async execute() {
          await execution.promise
          return { type: 'completed' }
        },
      },
      settings: { concurrency: 2, idleDelay: 60_000 },
    })

    cleanup(async () => {
      execution.resolve()
      await session.stop()
    })

    await adapter.pushOn('default', {
      id: 'pending-completion',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    assert.equal((await session.processCycle())?.type, 'started')
    const pendingCycle = trackPromise(session.processCycle())
    await setTimeout(0)

    const stop = trackPromise(session.stop())
    await setTimeout(0)

    assert.isTrue(pendingCycle.settled)
    assert.isFalse(stop.settled)

    execution.resolve()
    await Promise.all([pendingCycle.promise, stop.promise])

    assert.isTrue(stop.settled)
    assert.isNull(await session.processCycle())
  })

  test('surfaces an initial probe failure without starting fan-out acquisitions', async ({
    assert,
    cleanup,
  }) => {
    const adapter = new ControllableAdapter()
    const acquisitionError = new Error('Failed to acquire initial job')
    adapter.acquisitions.fail(1, acquisitionError)
    const session = createSession({ adapter, settings: { concurrency: 10 } })

    cleanup(() => session.stop())

    const cycle = await session.processCycle()

    assert.equal(cycle?.type, 'error')
    if (cycle?.type === 'error') {
      assert.strictEqual(cycle.error, acquisitionError)
    }
    assert.equal(adapter.acquisitions.calls, 1)
  })

  test('rejects continuous processing after manual processing has started', async ({
    assert,
    cleanup,
  }) => {
    const session = createSession()

    cleanup(() => session.stop())
    await session.processCycle()

    await assert.rejects(
      () => session.start(),
      'Configuration error. Reason: Cannot use continuous processing during a manual WorkerSession'
    )
  })

  test('rejects start when process already owns the continuous consumer', async ({
    assert,
    cleanup,
  }) => {
    const session = createSession()
    cleanup(() => session.stop())

    session.process()
    await assert.rejects(
      () => session.start(),
      'Configuration error. Reason: WorkerSession already has an active cycle consumer'
    )
  })

  test('waits for an in-flight probe and its job without starting fan-out after stopping', async ({
    assert,
    cleanup,
  }) => {
    const adapter = new ControllableAdapter()
    const executionGate = Promise.withResolvers<void>()
    const executionStarted = Promise.withResolvers<void>()
    let executionsCompleted = 0
    adapter.acquisitions.block(1)

    const session = createSession({
      adapter,
      jobExecutionRuntime: {
        async execute() {
          executionStarted.resolve()
          await executionGate.promise
          executionsCompleted++
          return { type: 'completed' }
        },
      },
      settings: { concurrency: 2 },
    })

    cleanup(async () => {
      adapter.releaseAll()
      executionGate.resolve()
      await session.stop()
    })

    await adapter.pushOn('default', {
      id: 'late-job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const start = session.start()
    await adapter.acquisitions.waitForStarted(1)

    const stop = trackPromise(session.stop())
    assert.isFalse(stop.settled, 'Session must wait for acquisitions already in flight')

    adapter.acquisitions.release(1)
    await executionStarted.promise
    await setTimeout(0)
    assert.equal(adapter.acquisitions.calls, 1, 'Session must not fan out after stopping')
    assert.isFalse(stop.settled, 'Session must wait for acquired jobs')

    executionGate.resolve()
    await Promise.all([stop.promise, start])

    assert.equal(executionsCompleted, 1)
    assert.isNull(await adapter.getJob('late-job-1', 'default'))
  })

  test('does not report or fail an unknown job whose lease was lost', async ({
    assert,
    cleanup,
  }) => {
    const adapter = new ControllableAdapter()
    const logger = new MemoryLogger()
    let failed = false
    adapter.retryJob = async () => false
    adapter.failJob = async () => {
      failed = true
      return true
    }
    const session = createSession({
      adapter,
      logger,
      jobExecutionRuntime: {
        execute: async () => ({
          type: 'initialization-failed' as const,
          error: new Error('not registered'),
          jobNotFound: true as const,
        }),
      },
    })

    cleanup(() => session.stop())

    await adapter.pushOn('default', {
      id: 'lost-unknown-job',
      name: 'UnknownJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    assert.equal((await session.processCycle())?.type, 'started')
    assert.equal((await session.processCycle())?.type, 'completed')

    // The job was acquired again elsewhere: no warning about a return that did not happen.
    assert.deepEqual(
      logger.logs.filter((entry) => entry.level === 'warn'),
      []
    )
    assert.isFalse(failed)
  })

  test('cannot restart after reaching quiescence', async ({ assert }) => {
    const session = createSession()

    await session.processCycle()
    await session.stop()

    assert.isNull(await session.processCycle())
    await session.start()
    assert.isNull(await session.processCycle())
  })

  test('does not claim another Schedule after stopping', async ({ assert, cleanup }) => {
    const adapter = new ControllableAdapter()
    const dispatches: string[] = []
    adapter.scheduleClaims.block(1)

    const session = createSession({
      adapter,
      scheduleDispatcher: {
        async dispatch(request) {
          dispatches.push(request.name)
          return { jobId: 'scheduled-job' }
        },
      },
    })

    cleanup(async () => {
      adapter.releaseAll()
      await session.stop()
    })

    await adapter.upsertSchedule({
      id: 'due-schedule',
      name: 'ScheduledJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
    })
    await adapter.updateSchedule('due-schedule', {
      nextRunAt: new Date(Date.now() - 1_000),
    })

    const cycle = session.processCycle()
    await adapter.scheduleClaims.waitForStarted()
    const stop = session.stop()
    adapter.scheduleClaims.release(1)
    await Promise.all([cycle, stop])

    assert.equal(adapter.scheduleClaims.calls, 1)
    assert.deepEqual(dispatches, ['ScheduledJob'])
  })

  test('owns an acquired job while a sibling acquisition is still pending', async ({ cleanup }) => {
    const adapter = new ControllableAdapter()
    const executionStarted = Promise.withResolvers<void>()
    adapter.acquisitions.block(2)

    const session = createSession({
      adapter,
      jobExecutionRuntime: {
        async execute() {
          executionStarted.resolve()
          return { type: 'completed' }
        },
      },
      settings: { concurrency: 2 },
    })

    cleanup(async () => {
      adapter.releaseAll()
      await session.stop()
    })

    await adapter.pushOn('default', {
      id: 'immediately-owned-job',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const start = session.start()
    await adapter.acquisitions.waitForStarted(2)
    await executionStarted.promise

    adapter.acquisitions.release(2)
    await session.stop()
    await start
  })

  test('interrupts the idle delay when stopping', async ({ assert, cleanup }) => {
    const adapter = new ControllableAdapter()
    const session = createSession({ adapter, settings: { idleDelay: 60_000 } })
    cleanup(() => session.stop())

    const start = trackPromise(session.start())
    await adapter.acquisitions.waitForSettled(1)
    await setTimeout(0)
    await session.stop()

    assert.isTrue(start.settled)
  })

  test('interrupts the error delay when stopping', async ({ assert, cleanup }) => {
    const adapter = new ControllableAdapter()
    adapter.acquisitions.fail(1, new Error('Failed to acquire job'))
    const session = createSession({ adapter })
    cleanup(() => session.stop())

    const start = trackPromise(session.start())
    await adapter.acquisitions.waitForSettled(1)
    await setTimeout(0)
    await session.stop()

    assert.isTrue(start.settled)
  })

  test('logs a failed cycle through the logger', async ({ assert, cleanup }) => {
    const adapter = new ControllableAdapter()
    const error = new Error('Failed to acquire job')
    adapter.acquisitions.fail(1, error)
    const logger = new MemoryLogger()
    const session = createSession({ adapter, logger })
    cleanup(() => session.stop())

    const start = session.start()
    await adapter.acquisitions.waitForSettled(1)
    await setTimeout(0)
    await session.stop()
    await start

    const errors = logger.logs.filter((entry) => entry.level === 'error')
    assert.lengthOf(errors, 1)
    assert.strictEqual(errors[0].obj?.err, error)
    assert.equal(errors[0].obj?.workerId, 'test-worker')
    assert.equal(errors[0].message, 'Worker cycle failed, next attempt in 5000ms')
  })

  test('suppresses an acquired cycle after stopping', async ({ assert, cleanup }) => {
    const adapter = new ControllableAdapter()
    adapter.acquisitions.block(1)
    const session = createSession({ adapter })
    cleanup(async () => {
      adapter.releaseAll()
      await session.stop()
    })

    await adapter.pushOn('default', {
      id: 'stopped-cycle-job',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const cycle = session.processCycle()
    await adapter.acquisitions.waitForStarted()
    const stop = session.stop()
    adapter.acquisitions.release(1)

    assert.isNull(await cycle)
    await stop
  })

  test('waits for stalled jobs recovered while stopping to be failed', async ({
    assert,
    cleanup,
  }) => {
    const adapter = new ControllableAdapter()
    const hookStarted = Promise.withResolvers<void>()
    const hookGate = Promise.withResolvers<void>()

    adapter.setWorkerId('crashed-worker')
    await adapter.pushOn('default', {
      id: 'stalled-job',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      stalledCount: 1,
    })
    await adapter.popFrom('default')
    await setTimeout(20)

    adapter.stalledChecks.block(1)
    const session = createSession({
      adapter,
      jobExecutionRuntime: {
        async failStalled() {
          hookStarted.resolve()
          await hookGate.promise
          return { type: 'failed', reason: 'stalled', error: new Error('stalled') }
        },
      },
      settings: { stalledThreshold: 10 },
    })
    cleanup(async () => {
      hookGate.resolve()
      adapter.releaseAll()
      await session.stop()
    })

    const cycle = session.processCycle()
    await adapter.stalledChecks.waitForStarted()
    const stop = trackPromise(session.stop())
    adapter.stalledChecks.release(1)

    await hookStarted.promise
    await setTimeout(20)
    assert.isFalse(stop.settled)

    hookGate.resolve()
    await stop.promise
    assert.isNull(await cycle)
    assert.isNull(await adapter.getJob('stalled-job', 'default'))
  })

  test('fails at most as many stalled jobs as there are free slots', async ({
    assert,
    cleanup,
  }) => {
    const adapter = new ControllableAdapter()
    const hookGate = Promise.withResolvers<void>()
    const firstHookStarted = Promise.withResolvers<void>()
    const failed: string[] = []

    adapter.setWorkerId('crashed-worker')
    for (const id of ['stalled-1', 'stalled-2']) {
      await adapter.pushOn('default', {
        id,
        name: 'TestJob',
        payload: {},
        attempts: 0,
        stalledCount: 1,
      })
      await adapter.popFrom('default')
    }
    await setTimeout(20)

    const session = createSession({
      adapter,
      jobExecutionRuntime: {
        async failStalled(job) {
          failed.push(job.id)
          firstHookStarted.resolve()
          await hookGate.promise
          return { type: 'failed', reason: 'stalled', error: new Error('stalled') }
        },
      },
      settings: { concurrency: 1, stalledThreshold: 10 },
    })
    cleanup(async () => {
      hookGate.resolve()
      await session.stop()
    })

    void session.processCycle()
    await firstHookStarted.promise
    await setTimeout(20)

    assert.lengthOf(failed, 1)
    const [other] = ['stalled-1', 'stalled-2'].filter((id) => id !== failed[0])
    assert.equal((await adapter.getJob(other, 'default'))!.status, 'active')
  })

  test('suppresses a stalled-check error after stopping', async ({ assert, cleanup }) => {
    const adapter = new ControllableAdapter()
    adapter.stalledChecks.block(1).fail(1, new Error('Failed to recover stalled jobs'))
    const session = createSession({ adapter })
    cleanup(async () => {
      adapter.releaseAll()
      await session.stop()
    })

    const cycle = session.processCycle()
    await adapter.stalledChecks.waitForStarted()
    const stop = session.stop()
    adapter.stalledChecks.release(1)

    assert.isNull(await cycle)
    await stop
  })

  test('keeps renewing a late-acquired job while stopping', async ({ cleanup }) => {
    const adapter = new ControllableAdapter()
    const executionStarted = Promise.withResolvers<void>()
    const executionGate = Promise.withResolvers<void>()
    adapter.acquisitions.block(1)

    const session = createSession({
      adapter,
      jobExecutionRuntime: {
        async execute() {
          executionStarted.resolve()
          await executionGate.promise
          return { type: 'completed' }
        },
      },
      settings: { stalledThreshold: 20 },
    })

    cleanup(async () => {
      executionGate.resolve()
      adapter.releaseAll()
      await session.stop()
    })

    await adapter.pushOn('default', {
      id: 'renewed-during-stop',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const start = session.start()
    await adapter.acquisitions.waitForStarted()
    const stop = session.stop()
    adapter.acquisitions.release(1)
    await executionStarted.promise
    await adapter.renewals.waitForStarted()

    executionGate.resolve()
    await Promise.all([stop, start])
  })

  test('waits for sibling acquisitions when one acquisition fails', async ({ assert, cleanup }) => {
    const adapter = new ControllableAdapter()
    let jobCompleted = false
    adapter.acquisitions.block(2, 3).fail(2, new Error('Failed to acquire job'))
    adapter.finalizations.block(1)

    const session = createSession({
      adapter,
      jobExecutionRuntime: {
        async execute() {
          jobCompleted = true
          return { type: 'completed' }
        },
      },
      settings: { concurrency: 3 },
    })

    cleanup(async () => {
      adapter.releaseAll()
      await session.stop()
    })

    await adapter.pushOn('default', {
      id: 'late-job-after-sibling-error',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const start = session.start()
    await adapter.acquisitions.waitForStarted(3)
    adapter.acquisitions.release(2)
    await adapter.acquisitions.waitForSettled(2)

    const stop = trackPromise(session.stop())
    assert.isFalse(stop.settled)

    adapter.acquisitions.release(3)
    await adapter.finalizations.waitForStarted()
    assert.isTrue(jobCompleted)
    assert.isFalse(stop.settled)

    adapter.finalizations.release(1)
    await Promise.all([stop.promise, start])
    assert.equal(adapter.acquisitions.calls, 3)
    assert.isNull(await adapter.getJob('late-job-after-sibling-error', 'default'))
  })

  test('emits successful acquisitions before a sibling acquisition error', async ({
    assert,
    cleanup,
  }) => {
    const adapter = new ControllableAdapter()
    adapter.acquisitions.fail(2, new Error('Failed to acquire sibling job'))
    const session = createSession({ adapter, settings: { concurrency: 2 } })
    cleanup(() => session.stop())

    await adapter.pushOn('default', {
      id: 'success-before-error',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const startedCycle = await session.processCycle()
    const errorCycle = await session.processCycle()

    assert.equal(startedCycle?.type, 'started')
    assert.equal(errorCycle?.type, 'error')
  })
})
