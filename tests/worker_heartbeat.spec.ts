import { setTimeout } from 'node:timers/promises'
import { test } from '@japa/runner'
import { JobPool } from '../src/job_pool.js'
import { WorkerHeartbeat } from '../src/worker_heartbeat.js'
import { ControllableAdapter } from './_mocks/controllable_adapter.js'
import { trackPromise } from './_utils/track_promise.js'
import { MemoryLogger } from './_mocks/memory_logger.js'

function createHeartbeat(
  adapter: ControllableAdapter,
  pool: JobPool,
  interval = 10,
  logger = new MemoryLogger()
) {
  return new WorkerHeartbeat({
    workerId: 'test-worker',
    queues: ['default'],
    interval,
    pool,
    adapter,
    wrapInternal: (operation) => operation(),
    logger,
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

  test('absorbs and logs renewal errors', async ({ assert, cleanup }) => {
    const adapter = new ControllableAdapter()
    const pool = new JobPool()
    const execution = Promise.withResolvers<void>()
    await acquireIntoPool(adapter, pool, 'renewed-job', execution.promise)
    const error = new Error('renewal failed')
    adapter.renewals.fail(1, error)
    const logger = new MemoryLogger()
    const heartbeat = createHeartbeat(adapter, pool, 1, logger)

    cleanup(async () => {
      execution.resolve()
      await heartbeat.stop()
      await pool.drain()
    })

    heartbeat.start()
    await adapter.renewals.waitForSettled(1)
    await heartbeat.stop()

    assert.isAtLeast(adapter.renewals.calls, 1)
    const warnings = logger.logs.filter((entry) => entry.level === 'warn')
    assert.lengthOf(warnings, 1)
    assert.strictEqual(warnings[0].obj?.err, error)
    assert.equal(warnings[0].obj?.queue, 'default')
    assert.equal(warnings[0].obj?.workerId, 'test-worker')
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

    const { recovered, exceeded } = await adapter.recoverStalledJobs('default', 40, 1, 100)
    assert.equal(recovered, 0)
    assert.deepEqual(exceeded, [])
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
