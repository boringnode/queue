import { setTimeout } from 'node:timers/promises'
import { test } from '@japa/runner'
import { fake } from '../src/drivers/fake_adapter.js'
import {
  WorkerSession,
  type WorkerSessionOptions,
  type WorkerSessionSettings,
} from '../src/worker_session.js'
import { ControllableAdapter } from './_mocks/controllable_adapter.js'
import { trackPromise } from './_utils/track_promise.js'

type SessionOverrides = Partial<Omit<WorkerSessionOptions, 'settings'>> & {
  settings?: Partial<WorkerSessionSettings>
}

function createSession(overrides: SessionOverrides = {}): WorkerSession {
  const { settings, ...options } = overrides

  return new WorkerSession({
    workerId: 'test-worker',
    queues: ['default'],
    adapter: fake()(),
    jobExecutionRuntime: {
      execute: async () => ({ type: 'completed' as const }),
    },
    scheduleDispatcher: {
      dispatch: async () => ({ jobId: 'scheduled-job' }),
    },
    wrapInternal: (operation) => operation(),
    settings: {
      concurrency: 1,
      idleDelay: 10,
      stalledInterval: 30_000,
      stalledThreshold: 30_000,
      maxStalledCount: 1,
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

  test('waits for in-flight acquisitions and their jobs before stopping', async ({
    assert,
    cleanup,
  }) => {
    const adapter = new ControllableAdapter()
    const executionGate = Promise.withResolvers<void>()
    const allExecutionsStarted = Promise.withResolvers<void>()
    let executionsStarted = 0
    let executionsCompleted = 0
    adapter.acquisitions.block(1, 2)

    const session = createSession({
      adapter,
      jobExecutionRuntime: {
        async execute() {
          executionsStarted++
          if (executionsStarted === 2) allExecutionsStarted.resolve()
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
    await adapter.pushOn('default', {
      id: 'late-job-2',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const start = session.start()
    await adapter.acquisitions.waitForStarted(2)

    const stop = trackPromise(session.stop())
    assert.isFalse(stop.settled, 'Session must wait for acquisitions already in flight')

    adapter.acquisitions.release(1, 2)
    await allExecutionsStarted.promise
    assert.isFalse(stop.settled, 'Session must wait for acquired jobs')

    executionGate.resolve()
    await Promise.all([stop.promise, start])

    assert.equal(executionsCompleted, 2)
    assert.isNull(await adapter.getJob('late-job-1', 'default'))
    assert.isNull(await adapter.getJob('late-job-2', 'default'))
  })

  test('waits for an in-flight heartbeat renewal before stopping', async ({ assert, cleanup }) => {
    const adapter = new ControllableAdapter()
    const executionStarted = Promise.withResolvers<void>()
    const executionGate = Promise.withResolvers<void>()
    adapter.renewals.block(1)

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
      id: 'renewed-job',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const start = session.start()
    await executionStarted.promise
    await adapter.renewals.waitForStarted()

    const stop = trackPromise(session.stop())
    executionGate.resolve()
    await adapter.finalizations.waitForSettled(1)
    assert.isFalse(stop.settled, 'Session must wait for a heartbeat already in flight')

    adapter.renewals.release(1)
    await Promise.all([stop.promise, start])
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
    adapter.acquisitions.block(1, 2)

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
    adapter.acquisitions.release(1)
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
    adapter.acquisitions.block(1, 2).fail(1, new Error('Failed to acquire job'))
    adapter.finalizations.block(1)

    const session = createSession({
      adapter,
      jobExecutionRuntime: {
        async execute() {
          jobCompleted = true
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
      id: 'late-job-after-sibling-error',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const start = session.start()
    await adapter.acquisitions.waitForStarted(2)
    adapter.acquisitions.release(1)
    await adapter.acquisitions.waitForSettled(1)

    const stop = trackPromise(session.stop())
    assert.isFalse(stop.settled)

    adapter.acquisitions.release(2)
    await adapter.finalizations.waitForStarted()
    assert.isTrue(jobCompleted)
    assert.isFalse(stop.settled)

    adapter.finalizations.release(1)
    await Promise.all([stop.promise, start])
    assert.equal(adapter.acquisitions.calls, 2)
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

  test('heartbeat prevents a running job from being recovered as stalled', async ({
    assert,
    cleanup,
  }) => {
    const adapter = new ControllableAdapter()
    const executionStarted = Promise.withResolvers<void>()
    const executionGate = Promise.withResolvers<void>()
    const session = createSession({
      adapter,
      jobExecutionRuntime: {
        async execute() {
          executionStarted.resolve()
          await executionGate.promise
          return { type: 'completed' }
        },
      },
      settings: { stalledThreshold: 40, stalledInterval: 10_000 },
    })

    cleanup(async () => {
      executionGate.resolve()
      await session.stop()
    })

    await adapter.pushOn('default', {
      id: 'long-running-job',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const start = session.start()
    await executionStarted.promise
    await adapter.renewals.waitForStarted()
    await setTimeout(60)

    const recovered = await adapter.recoverStalledJobs('default', 40, 1)
    assert.equal(recovered, 0)

    executionGate.resolve()
    await session.stop()
    await start
  })

  test('stops the heartbeat after reaching quiescence', async ({ assert, cleanup }) => {
    const adapter = new ControllableAdapter()
    const executionStarted = Promise.withResolvers<void>()
    const executionGate = Promise.withResolvers<void>()
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
      await session.stop()
    })

    await adapter.pushOn('default', {
      id: 'heartbeat-job',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 0,
    })

    const start = session.start()
    await executionStarted.promise
    await adapter.renewals.waitForStarted()

    executionGate.resolve()
    await session.stop()
    await start

    const renewalsAtStop = adapter.renewals.calls
    await setTimeout(40)
    assert.equal(adapter.renewals.calls, renewalsAtStop)
  })
})
