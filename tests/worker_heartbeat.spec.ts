import { setTimeout } from 'node:timers/promises'
import { test } from '@japa/runner'
import { JobPool } from '../src/job_pool.js'
import { WorkerHeartbeat } from '../src/worker_heartbeat.js'
import { ControllableAdapter } from './_mocks/controllable_adapter.js'
import { trackPromise } from './_utils/track_promise.js'

function createHeartbeat(adapter: ControllableAdapter, pool: JobPool, interval = 10) {
  return new WorkerHeartbeat({
    workerId: 'test-worker',
    queues: ['default'],
    interval,
    pool,
    adapter,
    wrapInternal: (operation) => operation(),
  })
}

async function acquireIntoPool(
  adapter: ControllableAdapter,
  pool: JobPool,
  id: string,
  execution: Promise<void>
): Promise<void> {
  adapter.setWorkerId('test-worker')
  await adapter.pushOn('default', {
    id,
    name: 'TestJob',
    payload: {},
    attempts: 0,
    priority: 0,
  })

  const job = await adapter.popFrom('default')
  if (!job) throw new Error('Expected an acquired Job')
  pool.add(job, 'default', execution)
}

test.group('WorkerHeartbeat', () => {
  test('waits for an in-flight renewal before stopping', async ({ assert, cleanup }) => {
    const adapter = new ControllableAdapter()
    const pool = new JobPool()
    const execution = Promise.withResolvers<void>()
    await acquireIntoPool(adapter, pool, 'renewed-job', execution.promise)
    adapter.renewals.block(1)
    const heartbeat = createHeartbeat(adapter, pool, 1)

    cleanup(async () => {
      adapter.releaseAll()
      execution.resolve()
      await heartbeat.stop()
      await pool.drain()
    })

    heartbeat.start()
    await adapter.renewals.waitForStarted()

    const stop = trackPromise(heartbeat.stop())
    assert.isFalse(stop.settled)

    adapter.renewals.release(1)
    await stop.promise
  })

  test('does not overlap renewals', async ({ assert, cleanup }) => {
    const adapter = new ControllableAdapter()
    const pool = new JobPool()
    const execution = Promise.withResolvers<void>()
    await acquireIntoPool(adapter, pool, 'renewed-job', execution.promise)
    adapter.renewals.block(1)
    const heartbeat = createHeartbeat(adapter, pool, 1)

    cleanup(async () => {
      adapter.releaseAll()
      execution.resolve()
      await heartbeat.stop()
      await pool.drain()
    })

    heartbeat.start()
    await adapter.renewals.waitForStarted()
    await setTimeout(20)

    assert.equal(adapter.renewals.calls, 1)

    adapter.renewals.release(1)
    await heartbeat.stop()
  })

  test('absorbs renewal errors', async ({ assert, cleanup }) => {
    const adapter = new ControllableAdapter()
    const pool = new JobPool()
    const execution = Promise.withResolvers<void>()
    await acquireIntoPool(adapter, pool, 'renewed-job', execution.promise)
    adapter.renewals.fail(1, new Error('renewal failed'))
    const heartbeat = createHeartbeat(adapter, pool, 1)

    cleanup(async () => {
      execution.resolve()
      await heartbeat.stop()
      await pool.drain()
    })

    heartbeat.start()
    await adapter.renewals.waitForSettled(1)
    await heartbeat.stop()

    assert.isAtLeast(adapter.renewals.calls, 1)
  })

  test('prevents an active Job from being recovered as stalled', async ({ assert, cleanup }) => {
    const adapter = new ControllableAdapter()
    const pool = new JobPool()
    const execution = Promise.withResolvers<void>()
    await acquireIntoPool(adapter, pool, 'long-running-job', execution.promise)
    const heartbeat = createHeartbeat(adapter, pool, 20)

    cleanup(async () => {
      execution.resolve()
      await heartbeat.stop()
      await pool.drain()
    })

    heartbeat.start()
    await adapter.renewals.waitForStarted()
    await setTimeout(60)

    const recovered = await adapter.recoverStalledJobs('default', 40, 1)
    assert.equal(recovered, 0)
  })

  test('stops renewing after stop resolves', async ({ assert, cleanup }) => {
    const adapter = new ControllableAdapter()
    const pool = new JobPool()
    const execution = Promise.withResolvers<void>()
    await acquireIntoPool(adapter, pool, 'heartbeat-job', execution.promise)
    const heartbeat = createHeartbeat(adapter, pool)

    cleanup(async () => {
      execution.resolve()
      await heartbeat.stop()
      await pool.drain()
    })

    heartbeat.start()
    await adapter.renewals.waitForStarted()
    await heartbeat.stop()

    const renewalsAtStop = adapter.renewals.calls
    await setTimeout(40)
    assert.equal(adapter.renewals.calls, renewalsAtStop)
  })
})
