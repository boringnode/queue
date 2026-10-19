import { setTimeout } from 'node:timers/promises'
import { test as JapaTest } from '@japa/runner'
import type { Adapter } from '../../src/contracts/adapter.js'

interface WakeSignalSuiteOptions {
  test: typeof JapaTest
  /** Creates an adapter that implements `subscribe`. */
  createAdapter: () => Adapter
  /** See `inTransaction` of the driver test suite. */
  inTransaction?: (operation: (adapter: Adapter) => Promise<void>) => Promise<void>
}

const job = (id: string) => ({ id, name: 'TestJob', payload: {}, attempts: 0 })

/** Subscribe and record the signals received after the initial announcement. */
async function listen(adapter: Adapter, queues = ['default']) {
  const wakes: number[] = []
  const unsubscribe = await adapter.subscribe!(queues, (at) => wakes.push(at))

  return {
    wakes,
    unsubscribe,
    clear: () => wakes.splice(0),
    async next(timeout = 2000): Promise<number> {
      const start = Date.now()
      while (wakes.length === 0) {
        if (Date.now() - start > timeout) throw new Error('No wake signal received')
        await setTimeout(5)
      }
      return wakes.shift()!
    },
  }
}

/**
 * Wake signals of adapters that implement `subscribe`. The subscriber and the
 * writer use separate adapters, as a worker and a dispatcher would.
 */
export function registerWakeSignalSuite({
  test,
  createAdapter,
  inTransaction,
}: WakeSignalSuiteOptions) {
  test('signals a push to the subscribers of its queue only', async ({ assert, cleanup }) => {
    const listener = await listen(createAdapter())
    cleanup(listener.unsubscribe)
    listener.clear()
    const writer = createAdapter()

    await writer.pushOn('other', job('other-queue'))
    await setTimeout(100)
    assert.deepEqual(listener.wakes, [])

    const before = Date.now()
    await writer.pushOn('default', job('default-queue'))
    const at = await listener.next()
    assert.isAtLeast(at, before)
    assert.isAtMost(at, Date.now())
  })

  test('signals when a delayed job and a retry become due', async ({ assert, cleanup }) => {
    const listener = await listen(createAdapter())
    cleanup(listener.unsubscribe)
    listener.clear()
    const writer = createAdapter()

    const before = Date.now()
    await writer.pushLaterOn('default', job('delayed'), 60_000)
    const delayedAt = await listener.next()
    assert.isAtLeast(delayedAt, before + 60_000)
    assert.isAtMost(delayedAt, Date.now() + 60_000)

    await writer.pushOn('default', job('retried'))
    await listener.next()
    const acquired = await writer.popFrom('default')
    const retryAt = new Date(Date.now() + 30_000)
    await writer.retryJob(acquired!, 'default', retryAt)
    assert.equal(await listener.next(), retryAt.getTime())
  })

  test('signals the next run of a schedule when it changes or runs', async ({
    assert,
    cleanup,
  }) => {
    const listener = await listen(createAdapter())
    cleanup(listener.unsubscribe)
    listener.clear()
    const writer = createAdapter()

    const firstRun = Date.now() - 1000
    await writer.upsertSchedule({
      id: 'wake-schedule',
      name: 'TestJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
      nextRunAt: new Date(firstRun),
    })
    assert.equal(await listener.next(), firstRun)

    const before = Date.now()
    await writer.claimDueSchedule()
    const nextRun = await listener.next()
    assert.isAtLeast(nextRun, before + 60_000)
    assert.isAtMost(nextRun, Date.now() + 60_000)
  })

  test('announces work stored before subscribing', async ({ assert, cleanup }) => {
    const writer = createAdapter()
    const delayedBefore = Date.now()
    await writer.pushLaterOn('default', job('stored-delayed'), 60_000)
    const delayedAfter = Date.now()
    const scheduleRun = Date.now() + 30_000
    await writer.upsertSchedule({
      id: 'stored-schedule',
      name: 'TestJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
      nextRunAt: new Date(scheduleRun),
    })

    const before = Date.now()
    const listener = await listen(createAdapter())
    cleanup(listener.unsubscribe)
    const [now, delayedAt, ...rest] = listener.wakes

    assert.isAtLeast(now, before)
    assert.isAtLeast(delayedAt, delayedBefore + 60_000)
    assert.isAtMost(delayedAt, delayedAfter + 60_000)
    assert.deepEqual(rest, [scheduleRun])
  })

  test('announces the next delayed job once the previous one is promoted', async ({
    assert,
    cleanup,
  }) => {
    const writer = createAdapter()
    await writer.pushLaterOn('default', job('first-delayed'), 50)
    const before = Date.now()
    await writer.pushLaterOn('default', job('second-delayed'), 60_000)
    const after = Date.now()

    const listener = await listen(createAdapter())
    cleanup(listener.unsubscribe)
    listener.clear()

    await setTimeout(100)
    await writer.popFrom('default')
    const at = await listener.next()
    assert.isAtLeast(at, before + 60_000)
    assert.isAtMost(at, after + 60_000)
  })

  test('announces the next delayed job of each queue', async ({ assert, cleanup }) => {
    const writer = createAdapter()
    await writer.pushLaterOn('first', job('first-queue'), 30_000)
    const before = Date.now()
    await writer.pushLaterOn('second', job('second-queue'), 60_000)
    const after = Date.now()

    const listener = await listen(createAdapter(), ['first', 'second'])
    cleanup(listener.unsubscribe)

    assert.isTrue(
      listener.wakes.some((at) => at >= before + 60_000 && at <= after + 60_000),
      'the second queue is announced'
    )
  })

  test('announces the next schedule past an expired one', async ({ assert, cleanup }) => {
    const writer = createAdapter()
    const now = Date.now()
    await writer.upsertSchedule({
      id: 'expired',
      name: 'TestJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
      to: new Date(now - 500),
      nextRunAt: new Date(now - 1000),
    })
    const nextRun = now + 60_000
    await writer.upsertSchedule({
      id: 'next',
      name: 'TestJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
      nextRunAt: new Date(nextRun),
    })

    const listener = await listen(createAdapter())
    cleanup(listener.unsubscribe)

    // A worker woken for the expired schedule claims nothing.
    assert.isNull(await writer.claimDueSchedule())
    await setTimeout(100)
    assert.include(listener.wakes, nextRun)
  })

  test('announces the next schedule when the announced one expires before running', async ({
    assert,
    cleanup,
  }) => {
    const writer = createAdapter()
    const now = Date.now()
    await writer.upsertSchedule({
      id: 'expiring',
      name: 'TestJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
      to: new Date(now + 150),
      nextRunAt: new Date(now + 100),
    })
    const nextRun = now + 60_000
    await writer.upsertSchedule({
      id: 'next',
      name: 'TestJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
      nextRunAt: new Date(nextRun),
    })

    // The worker adapter is told about the expiring schedule only.
    const worker = createAdapter()
    const listener = await listen(worker)
    cleanup(listener.unsubscribe)
    assert.include(listener.wakes, now + 100)
    listener.clear()

    // It wakes once the schedule has also passed its end date.
    await setTimeout(200)
    assert.isNull(await worker.claimDueSchedule())
    await setTimeout(100)
    assert.include(listener.wakes, nextRun)
  })

  test('announces the next schedule when one is deleted', async ({ assert, cleanup }) => {
    const writer = createAdapter()
    const schedule = (id: string, nextRunAt: number) =>
      writer.upsertSchedule({
        id,
        name: 'TestJob',
        payload: {},
        everyMs: 60_000,
        timezone: 'UTC',
        nextRunAt: new Date(nextRunAt),
      })
    const secondRun = Date.now() + 60_000
    await schedule('first', Date.now() + 30_000)
    await schedule('second', secondRun)

    const listener = await listen(createAdapter())
    cleanup(listener.unsubscribe)
    listener.clear()

    await writer.deleteSchedule('first')
    assert.equal(await listener.next(), secondRun)
  })

  if (inTransaction) {
    test('signals a push in a transaction once it commits', async ({ assert, cleanup }) => {
      const listener = await listen(createAdapter())
      cleanup(listener.unsubscribe)
      listener.clear()

      await inTransaction(async (adapter) => {
        await adapter.pushOn('default', job('committed'))
        await setTimeout(100)
        assert.deepEqual(listener.wakes, [], 'no signal before the commit')
      })
      await listener.next()

      await inTransaction(async (adapter) => {
        await adapter.pushOn('default', job('rolled-back'))
        throw new Error('rollback')
      }).catch(() => {})
      await setTimeout(100)
      assert.deepEqual(listener.wakes, [], 'no signal after a rollback')
    })
  }
}
