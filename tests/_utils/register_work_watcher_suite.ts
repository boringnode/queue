import { setTimeout } from 'node:timers/promises'
import { test as JapaTest } from '@japa/runner'
import type { Adapter, WorkWatcher } from '../../src/contracts/adapter.js'

interface WorkWatcherSuiteOptions {
  test: typeof JapaTest
  /** Creates an adapter that implements `watch`. */
  createAdapter: () => Adapter
  /** See `inTransaction` of the driver test suite. */
  inTransaction?: (operation: (adapter: Adapter) => Promise<void>) => Promise<void>
}

const job = (id: string) => ({ id, name: 'TestJob', payload: {}, attempts: 0 })

/**
 * Watch, as a worker does: `next()` looks for work (reset), then waits and
 * resolves with the time the wait took, or null when nothing woke it.
 */
async function watch(adapter: Adapter, queues = ['default']) {
  const watcher: WorkWatcher = await adapter.watch!(queues)
  // The worker looks for work once ready: what is stored is covered.
  watcher.reset()

  return {
    watcher,
    close: () => watcher.close(),
    async next(maxDelay = 2000): Promise<number | null> {
      watcher.reset()
      const start = Date.now()
      const woken = await watcher.wait(maxDelay, new AbortController().signal, 0)
      return woken ? Date.now() - start : null
    },
    /** Wait for the first wake at least `atLeast` ms after now. */
    async wakeAfter(atLeast: number, maxDelay = 2000): Promise<number | null> {
      const start = Date.now()
      while (Date.now() - start < maxDelay) {
        const woken = await this.next(maxDelay - (Date.now() - start))
        if (woken === null) return null
        if (Date.now() - start >= atLeast) return Date.now() - start
      }
      return null
    },
  }
}

/**
 * Wakes of adapters that implement `watch`. The watcher and the writer use
 * separate adapters, as a worker and a dispatcher would. Due times are
 * short, so the suite checks when a wait ends, within a margin.
 */
export function registerWorkWatcherSuite({
  test,
  createAdapter,
  inTransaction,
}: WorkWatcherSuiteOptions) {
  test('wakes for a push to its queue only', async ({ assert, cleanup }) => {
    const watching = await watch(createAdapter())
    cleanup(watching.close)
    const writer = createAdapter()

    const waiting = watching.next()
    await writer.pushOn('other', job('other-queue'))
    await setTimeout(100)
    await writer.pushOn('default', job('default-queue'))

    const elapsed = await waiting
    assert.isNotNull(elapsed)
    assert.isAtLeast(elapsed!, 100, 'not woken by the other queue')
    assert.isBelow(elapsed!, 400)
  })

  test('wakes when a delayed job and a retry become due', async ({ assert, cleanup }) => {
    const watching = await watch(createAdapter())
    cleanup(watching.close)
    const writer = createAdapter()

    await writer.pushLaterOn('default', job('delayed'), 300)
    assert.closeTo((await watching.wakeAfter(250))!, 300, 100)
    await writer.popFrom('default')

    await writer.pushOn('default', job('retried'))
    await watching.next()
    const acquired = await writer.popFrom('default')
    await writer.retryJob(acquired!, 'default', new Date(Date.now() + 300))
    assert.closeTo((await watching.wakeAfter(250))!, 300, 100)
  })

  test('wakes when a schedule becomes due', async ({ assert, cleanup }) => {
    const watching = await watch(createAdapter())
    cleanup(watching.close)
    const writer = createAdapter()

    await writer.upsertSchedule({
      id: 'watched-schedule',
      name: 'TestJob',
      payload: {},
      everyMs: 300,
      timezone: 'UTC',
      nextRunAt: new Date(Date.now() + 300),
    })
    assert.closeTo((await watching.wakeAfter(250))!, 300, 100)

    // Its next run, written by the claim.
    assert.isNotNull(await writer.claimDueSchedule())
    assert.closeTo((await watching.wakeAfter(250))!, 300, 100)
  })

  test('wakes for work stored before watching', async ({ assert, cleanup }) => {
    const writer = createAdapter()
    await writer.pushLaterOn('default', job('stored-delayed'), 300)
    await writer.upsertSchedule({
      id: 'stored-schedule',
      name: 'TestJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
      nextRunAt: new Date(Date.now() + 600),
    })

    const watching = await watch(createAdapter())
    cleanup(watching.close)

    assert.closeTo((await watching.wakeAfter(250))!, 300, 100)
    await writer.popFrom('default')
    assert.closeTo((await watching.wakeAfter(200))!, 300, 100)
  })

  test('wakes for the next delayed job once one is promoted', async ({ assert, cleanup }) => {
    const writer = createAdapter()
    await writer.pushLaterOn('default', job('first-delayed'), 100)
    await writer.pushLaterOn('default', job('second-delayed'), 400)

    const watching = await watch(createAdapter())
    cleanup(watching.close)

    assert.closeTo((await watching.wakeAfter(50))!, 100, 80)
    assert.equal((await writer.popFrom('default'))?.id, 'first-delayed')
    assert.closeTo((await watching.wakeAfter(200))!, 300, 100)
  })

  test('wakes for the next delayed job of each queue', async ({ assert, cleanup }) => {
    const writer = createAdapter()
    await writer.pushLaterOn('first', job('first-queue'), 150)
    await writer.pushLaterOn('second', job('second-queue'), 450)

    const watching = await watch(createAdapter(), ['first', 'second'])
    cleanup(watching.close)

    assert.closeTo((await watching.wakeAfter(100))!, 150, 80)
    await writer.popFrom('first')
    assert.closeTo((await watching.wakeAfter(200))!, 300, 100)
  })

  test('wakes for the next schedule past an expired one', async ({ assert, cleanup }) => {
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
    await writer.upsertSchedule({
      id: 'next',
      name: 'TestJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
      nextRunAt: new Date(now + 400),
    })

    const worker = createAdapter()
    const watching = await watch(worker)
    cleanup(watching.close)

    // A worker woken for the expired schedule claims nothing.
    assert.isNull(await worker.claimDueSchedule())
    const expected = 400 - (Date.now() - now)
    assert.closeTo((await watching.wakeAfter(expected - 100))!, expected, 100)
  })

  test('wakes for the next schedule when the announced one expires before running', async ({
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
    await writer.upsertSchedule({
      id: 'next',
      name: 'TestJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
      nextRunAt: new Date(now + 600),
    })

    const worker = createAdapter()
    const watching = await watch(worker)
    cleanup(watching.close)

    // Woken for the expiring schedule once it has also passed its end date.
    await setTimeout(200)
    assert.isNull(await worker.claimDueSchedule())
    const expected = 600 - (Date.now() - now)
    assert.closeTo((await watching.wakeAfter(expected - 100))!, expected, 100)
  })

  test('wakes for the next schedule once one is deleted', async ({ assert, cleanup }) => {
    const writer = createAdapter()
    const schedule = (id: string, inMs: number) =>
      writer.upsertSchedule({
        id,
        name: 'TestJob',
        payload: {},
        everyMs: 60_000,
        timezone: 'UTC',
        nextRunAt: new Date(Date.now() + inMs),
      })
    await schedule('first', 200)
    await schedule('second', 500)

    const watching = await watch(createAdapter())
    cleanup(watching.close)
    await writer.deleteSchedule('first')

    assert.closeTo((await watching.wakeAfter(400))!, 500, 100)
  })

  if (inTransaction) {
    test('wakes for a push in a transaction once it commits', async ({ assert, cleanup }) => {
      const watching = await watch(createAdapter())
      cleanup(watching.close)

      let waiting = watching.next(300)
      await inTransaction(async (adapter) => {
        await adapter.pushOn('default', job('committed'))
        assert.isNull(await waiting, 'no wake before the commit')
        waiting = watching.next()
      })
      assert.isNotNull(await waiting)

      await inTransaction(async (adapter) => {
        await adapter.pushOn('default', job('rolled-back'))
        throw new Error('rollback')
      }).catch(() => {})
      assert.isNull(await watching.next(300), 'no wake after a rollback')
    })
  }
}
