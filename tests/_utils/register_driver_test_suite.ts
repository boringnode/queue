import { test as JapaTest } from '@japa/runner'
import type { AcquiredJob, Adapter, JobLease } from '../../src/contracts/adapter.js'

interface DriverTestSuiteOptions {
  test: typeof JapaTest
  createAdapter: () => Adapter | Promise<Adapter>
  /**
   * Whether this adapter supports concurrent access from multiple instances.
   * Memory adapter doesn't share state between instances, so concurrent tests are skipped.
   * @default true
   */
  supportsConcurrency?: boolean
  /**
   * Whether concurrent dispatches have an atomic dedup constraint.
   * MySQL has no partial unique indexes, matching the documented Knex limitation.
   * @default true
   */
  supportsAtomicDedup?: boolean
}

/** A lease on a job that was never acquired. */
function unleased(id: string): JobLease {
  return { id, leaseToken: 'worker-1:never-acquired' }
}

/**
 * Acquire a job, let it stall and be recovered, then acquire it again with
 * the same worker: the first lease is stale, the second one is current.
 */
async function acquireTwice(
  adapter: Adapter
): Promise<{ stale: AcquiredJob; current: AcquiredJob }> {
  await adapter.pushOn('test-queue', { id: 'job-1', name: 'TestJob', payload: {}, attempts: 0 })

  const stale = await adapter.popFrom('test-queue')
  await new Promise((resolve) => setTimeout(resolve, 30))
  const { recovered } = await adapter.recoverStalledJobs('test-queue', 10, 1, 100)
  if (recovered !== 1) throw new Error('The job was not recovered')

  const current = await adapter.popFrom('test-queue')
  return { stale: stale!, current: current! }
}

export function registerDriverTestSuite(options: DriverTestSuiteOptions) {
  const { test } = options

  test('migrate should be safe to call repeatedly', async () => {
    const adapter = await options.createAdapter()

    await adapter.migrate()
    await adapter.migrate()
  })

  test('popFrom should return null when queue is empty', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    const job = await adapter.popFrom('test-queue')
    assert.isNull(job)
  })

  test('popFrom should return job with acquiredAt timestamp', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: { foo: 'bar' },
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')

    assert.isNotNull(job)
    assert.equal(job!.id, 'job-1')
    assert.equal(job!.name, 'TestJob')
    assert.deepEqual(job!.payload, { foo: 'bar' })
    assert.isNumber(job!.acquiredAt)
    assert.approximately(job!.acquiredAt, Date.now(), 1000)
  })

  test('popFrom should remove job from pending queue', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job1 = await adapter.popFrom('test-queue')
    assert.isNotNull(job1)

    const job2 = await adapter.popFrom('test-queue')
    assert.isNull(job2)
  })

  test('completeJob should remove job from active tracking', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')
    assert.isNotNull(job)

    await adapter.completeJob(job!, 'test-queue')

    // Retry should have no effect since job is no longer active
    await adapter.retryJob(job!, 'test-queue')

    const nextJob = await adapter.popFrom('test-queue')
    assert.isNull(nextJob)
  })

  test('getJob should return status pending for a queued job', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-pending',
      name: 'TestJob',
      payload: { foo: 'bar' },
      attempts: 0,
    })

    const record = await adapter.getJob('job-pending', 'test-queue')

    assert.isNotNull(record)
    assert.equal(record!.status, 'pending')
    assert.equal(record!.data.id, 'job-pending')
    assert.deepEqual(record!.data.payload, { foo: 'bar' })
  })

  test('getJob should return status delayed for a delayed job', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushLaterOn(
      'test-queue',
      {
        id: 'job-delayed',
        name: 'TestJob',
        payload: {},
        attempts: 0,
      },
      60000
    ) // 1 minute delay

    const record = await adapter.getJob('job-delayed', 'test-queue')

    assert.isNotNull(record)
    assert.equal(record!.status, 'delayed')
    assert.equal(record!.data.id, 'job-delayed')
  })

  test('getJob should return status active for a job being processed', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-active',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    await adapter.popFrom('test-queue')

    const record = await adapter.getJob('job-active', 'test-queue')

    assert.isNotNull(record)
    assert.equal(record!.status, 'active')
    assert.equal(record!.data.id, 'job-active')
  })

  test('getJob should not return active job from another queue', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('queue-a', {
      id: 'job-active-other-queue',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    await adapter.popFrom('queue-a')

    const wrongQueueRecord = await adapter.getJob('job-active-other-queue', 'queue-b')
    assert.isNull(wrongQueueRecord)

    const rightQueueRecord = await adapter.getJob('job-active-other-queue', 'queue-a')
    assert.isNotNull(rightQueueRecord)
    assert.equal(rightQueueRecord!.status, 'active')
  })

  test('getJob should return null for non-existent job', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    const record = await adapter.getJob('non-existent', 'test-queue')

    assert.isNull(record)
  })

  test('getJob should return finishedAt for completed job', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-finished',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')
    const beforeComplete = Date.now()
    await adapter.completeJob(job!, 'test-queue', false)
    const afterComplete = Date.now()

    const record = await adapter.getJob(job!.id, 'test-queue')

    assert.isNotNull(record)
    assert.equal(record!.status, 'completed')
    assert.isNumber(record!.finishedAt)
    assert.isAtLeast(record!.finishedAt!, beforeComplete)
    assert.isAtMost(record!.finishedAt!, afterComplete)
  })

  test('failJob should store error message', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-error',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')
    await adapter.failJob(job!, 'test-queue', new Error('Something went wrong'), false)

    const record = await adapter.getJob(job!.id, 'test-queue')

    assert.isNotNull(record)
    assert.equal(record!.status, 'failed')
    assert.equal(record!.error, 'Something went wrong')
    assert.isNumber(record!.finishedAt)
  })

  test('completeJob should keep job when removeOnComplete is false', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-keep',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')
    assert.isNotNull(job)

    await adapter.completeJob(job!, 'test-queue', false)

    const record = await adapter.getJob(job!.id, 'test-queue')
    assert.isNotNull(record)
    assert.equal(record!.status, 'completed')
  })

  test('completeJob should remove job when removeOnComplete is true', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-drop',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')
    assert.isNotNull(job)

    await adapter.completeJob(job!, 'test-queue', true)

    const record = await adapter.getJob(job!.id, 'test-queue')
    assert.isNull(record)
  })

  test('retryJob should put job back in queue with incremented attempts', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: { foo: 'bar' },
      attempts: 0,
    })

    const job1 = await adapter.popFrom('test-queue')
    assert.isNotNull(job1)
    assert.equal(job1!.attempts, 0)

    await adapter.retryJob(job1!, 'test-queue')

    const job2 = await adapter.popFrom('test-queue')
    assert.isNotNull(job2)
    assert.equal(job2!.id, 'job-1')
    assert.equal(job2!.attempts, 1)
  })

  test('retryJob with future date should delay the job', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')
    assert.isNotNull(job)

    const futureDate = new Date(Date.now() + 60000) // 1 minute in future
    await adapter.retryJob(job!, 'test-queue', futureDate)

    // Job should not be immediately available
    const nextJob = await adapter.popFrom('test-queue')
    assert.isNull(nextJob)
  })

  test('failJob should remove job from active tracking', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')
    assert.isNotNull(job)

    await adapter.failJob(job!, 'test-queue', new Error('Test error'))

    // Retry should have no effect since job is no longer active
    await adapter.retryJob(job!, 'test-queue')

    const nextJob = await adapter.popFrom('test-queue')
    assert.isNull(nextJob)
  })

  test('failJob should keep job when removeOnFail is false', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-fail-keep',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')
    assert.isNotNull(job)

    await adapter.failJob(job!, 'test-queue', new Error('Test error'), false)

    const record = await adapter.getJob(job!.id, 'test-queue')
    assert.isNotNull(record)
    assert.equal(record!.status, 'failed')
  })

  test('retention count should prune completed jobs', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    await adapter.pushOn('test-queue', {
      id: 'job-2',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job1 = await adapter.popFrom('test-queue')
    const job2 = await adapter.popFrom('test-queue')
    assert.isNotNull(job1)
    assert.isNotNull(job2)

    await adapter.completeJob(job1!, 'test-queue', { count: 1 })
    await adapter.completeJob(job2!, 'test-queue', { count: 1 })

    const record1 = await adapter.getJob(job1!.id, 'test-queue')
    const record2 = await adapter.getJob(job2!.id, 'test-queue')

    assert.isNull(record1)
    assert.isNotNull(record2)
  })

  test('retention age should prune completed jobs', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-age-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    await adapter.pushOn('test-queue', {
      id: 'job-age-2',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job1 = await adapter.popFrom('test-queue')
    assert.isNotNull(job1)

    await adapter.completeJob(job1!, 'test-queue', { age: '1ms' })

    await new Promise((resolve) => setTimeout(resolve, 5))

    const job2 = await adapter.popFrom('test-queue')
    assert.isNotNull(job2)

    await adapter.completeJob(job2!, 'test-queue', { age: '1ms' })

    const record1 = await adapter.getJob(job1!.id, 'test-queue')
    const record2 = await adapter.getJob(job2!.id, 'test-queue')

    assert.isNull(record1)
    assert.isNotNull(record2)
  })

  test('multiple jobs should be processed in order', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })
    await adapter.pushOn('test-queue', {
      id: 'job-2',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })
    await adapter.pushOn('test-queue', {
      id: 'job-3',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job1 = await adapter.popFrom('test-queue')
    const job2 = await adapter.popFrom('test-queue')
    const job3 = await adapter.popFrom('test-queue')
    const job4 = await adapter.popFrom('test-queue')

    assert.equal(job1!.id, 'job-1')
    assert.equal(job2!.id, 'job-2')
    assert.equal(job3!.id, 'job-3')
    assert.isNull(job4)
  })

  test('recoverStalledJobs should return 0 when no stalled jobs', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    // No jobs at all
    const { recovered } = await adapter.recoverStalledJobs('test-queue', 1000, 1, 100)
    assert.equal(recovered, 0)
  })

  test('recoverStalledJobs should not recover jobs within threshold', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    // Acquire the job
    const job = await adapter.popFrom('test-queue')
    assert.isNotNull(job)

    // Try to recover with a long threshold (job is not stalled yet)
    const { recovered } = await adapter.recoverStalledJobs('test-queue', 60000, 1, 100)
    assert.equal(recovered, 0)

    // Job should still be active, not back in pending
    const nextJob = await adapter.popFrom('test-queue')
    assert.isNull(nextJob)
  })

  test('recoverStalledJobs should recover stalled jobs back to pending', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: { foo: 'bar' },
      attempts: 0,
    })

    // Acquire the job
    await adapter.popFrom('test-queue')

    // Wait a bit and recover with a very short threshold
    await new Promise((resolve) => setTimeout(resolve, 50))
    const { recovered } = await adapter.recoverStalledJobs('test-queue', 10, 1, 100)
    assert.equal(recovered, 1)

    // Job should be back in pending queue
    const nextJob = await adapter.popFrom('test-queue')
    assert.isNotNull(nextJob)
    assert.equal(nextJob!.id, 'job-1')
    assert.deepEqual(nextJob!.payload, { foo: 'bar' })
  })

  test('recoverStalledJobs should preserve empty arrays in the payload', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-stalled-empty-array-payload',
      name: 'TestJob',
      payload: {
        empty: [],
        nested: {
          items: [],
        },
      },
      attempts: 0,
      stalledCount: 0,
    })

    await adapter.popFrom('test-queue')
    await new Promise((resolve) => setTimeout(resolve, 20))
    await adapter.recoverStalledJobs('test-queue', 10, 3, 100)

    const recovered = await adapter.popFrom('test-queue')

    assert.deepEqual(recovered!.payload, {
      empty: [],
      nested: {
        items: [],
      },
    })
    assert.equal(recovered!.stalledCount, 1)
  })

  test('recoverStalledJobs should increment stalledCount', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      stalledCount: 0,
    })

    // First stall cycle
    await adapter.popFrom('test-queue')
    await new Promise((resolve) => setTimeout(resolve, 50))
    await adapter.recoverStalledJobs('test-queue', 10, 3, 100)

    const job1 = await adapter.popFrom('test-queue')
    assert.isNotNull(job1)
    assert.equal(job1!.stalledCount, 1)

    // Second stall cycle
    await new Promise((resolve) => setTimeout(resolve, 50))
    await adapter.recoverStalledJobs('test-queue', 10, 3, 100)

    const job2 = await adapter.popFrom('test-queue')
    assert.isNotNull(job2)
    assert.equal(job2!.stalledCount, 2)
  })

  test('recoverStalledJobs should hand back jobs that exceeded maxStalledCount', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: { foo: 'bar' },
      attempts: 0,
      stalledCount: 0,
    })

    // First stall - recovered back to pending (stalledCount becomes 1)
    await adapter.popFrom('test-queue')
    await new Promise((resolve) => setTimeout(resolve, 50))
    const first = await adapter.recoverStalledJobs('test-queue', 10, 1, 100)
    assert.equal(first.recovered, 1)
    assert.deepEqual(first.exceeded, [])

    // Second stall - exceeds maxStalledCount=1 and is reacquired by the recovering worker
    const stalled = await adapter.popFrom('test-queue')
    await new Promise((resolve) => setTimeout(resolve, 50))
    adapter.setWorkerId('recovering-worker')
    const before = Date.now()
    const second = await adapter.recoverStalledJobs('test-queue', 10, 1, 100)

    assert.equal(second.recovered, 0)
    assert.lengthOf(second.exceeded, 1)
    assert.equal(second.exceeded[0].id, 'job-1')
    assert.deepEqual(second.exceeded[0].payload, { foo: 'bar' })
    assert.equal(second.exceeded[0].stalledCount, 1)
    assert.isAtLeast(second.exceeded[0].acquiredAt, before)

    // The job stays active: it is neither pending nor removed
    assert.isNull(await adapter.popFrom('test-queue'))
    assert.equal((await adapter.getJob('job-1', 'test-queue'))!.status, 'active')

    // Only the recovering worker holds the lease now
    assert.match(second.exceeded[0].leaseToken, /^recovering-worker:/)
    assert.equal(await adapter.renewJobs('test-queue', [second.exceeded[0]]), 1)
    assert.equal(await adapter.renewJobs('test-queue', [stalled!]), 0)
  })

  test('recoverStalledJobs should hand back an exceeded job again if it is not failed', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      stalledCount: 1,
    })

    await adapter.popFrom('test-queue')
    await new Promise((resolve) => setTimeout(resolve, 50))
    const first = await adapter.recoverStalledJobs('test-queue', 10, 1, 100)
    assert.lengthOf(first.exceeded, 1)

    // The recovering worker crashed before failing it: the job stalls again
    await new Promise((resolve) => setTimeout(resolve, 50))
    const second = await adapter.recoverStalledJobs('test-queue', 10, 1, 100)
    assert.equal(second.recovered, 0)
    assert.lengthOf(second.exceeded, 1)
    assert.equal(second.exceeded[0].id, 'job-1')
    assert.equal(second.exceeded[0].stalledCount, 1)
  })

  test('recoverStalledJobs should reacquire at most maxExceeded exceeded jobs', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    for (const id of ['job-1', 'job-2']) {
      await adapter.pushOn('test-queue', {
        id,
        name: 'TestJob',
        payload: {},
        attempts: 0,
        stalledCount: 1,
      })
      await adapter.popFrom('test-queue')
    }

    await new Promise((resolve) => setTimeout(resolve, 50))
    adapter.setWorkerId('recovering-worker')

    // No free slot: exceeded jobs are left stalled
    const none = await adapter.recoverStalledJobs('test-queue', 10, 1, 0)
    assert.deepEqual(none.exceeded, [])

    const { exceeded } = await adapter.recoverStalledJobs('test-queue', 10, 1, 1)
    assert.lengthOf(exceeded, 1)

    // Only the returned job was reacquired; the other one is left untouched
    const other = exceeded[0].id === 'job-1' ? 'job-2' : 'job-1'
    assert.equal(await adapter.renewJobs('test-queue', [exceeded[0]]), 1)
    assert.equal(
      await adapter.renewJobs('test-queue', [{ id: other, leaseToken: exceeded[0].leaseToken }]),
      0
    )
  })

  test('failJob should finalize a job handed back by recoverStalledJobs', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      stalledCount: 1,
    })

    await adapter.popFrom('test-queue')
    await new Promise((resolve) => setTimeout(resolve, 50))
    const { exceeded } = await adapter.recoverStalledJobs('test-queue', 10, 1, 100)
    assert.lengthOf(exceeded, 1)

    await adapter.failJob(exceeded[0], 'test-queue', new Error('stalled'), false)

    const record = await adapter.getJob('job-1', 'test-queue')
    assert.equal(record!.status, 'failed')
    assert.equal(record!.error, 'stalled')
  })

  test('recoverStalledJobs should handle multiple stalled jobs', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })
    await adapter.pushOn('test-queue', {
      id: 'job-2',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    // Acquire both jobs
    await adapter.popFrom('test-queue')
    await adapter.popFrom('test-queue')

    // Recover all stalled jobs
    await new Promise((resolve) => setTimeout(resolve, 50))
    const { recovered } = await adapter.recoverStalledJobs('test-queue', 10, 1, 100)
    assert.equal(recovered, 2)

    // Both jobs should be back
    const job1 = await adapter.popFrom('test-queue')
    const job2 = await adapter.popFrom('test-queue')
    const job3 = await adapter.popFrom('test-queue')

    assert.isNotNull(job1)
    assert.isNotNull(job2)
    assert.isNull(job3)
  })

  test('recoverStalledJobs should only recover jobs from the targeted queue', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('queue-a', {
      id: 'job-stalled-a',
      name: 'TestJob',
      payload: null,
      attempts: 0,
    })
    await adapter.pushOn('queue-b', {
      id: 'job-stalled-b',
      name: 'TestJob',
      payload: null,
      attempts: 0,
    })

    const jobA = await adapter.popFrom('queue-a')
    const jobB = await adapter.popFrom('queue-b')
    assert.isNotNull(jobA)
    assert.isNotNull(jobB)

    await new Promise((resolve) => setTimeout(resolve, 50))

    const { recovered: recoveredA } = await adapter.recoverStalledJobs('queue-a', 10, 1, 100)
    assert.equal(recoveredA, 1)

    const recoveredJobA = await adapter.popFrom('queue-a')
    assert.isNotNull(recoveredJobA)
    assert.equal(recoveredJobA!.id, 'job-stalled-a')

    const queueBPending = await adapter.popFrom('queue-b')
    assert.isNull(queueBPending)

    await new Promise((resolve) => setTimeout(resolve, 50))

    const { recovered: recoveredB } = await adapter.recoverStalledJobs('queue-b', 10, 1, 100)
    assert.equal(recoveredB, 1)

    const recoveredJobB = await adapter.popFrom('queue-b')
    assert.isNotNull(recoveredJobB)
    assert.equal(recoveredJobB!.id, 'job-stalled-b')
  })

  test('renewJobs should keep an active job from being recovered as stalled', async ({
    assert,
    cleanup,
  }) => {
    const stalledThreshold = 60_000
    const realNow = Date.now
    let clockOffset = 0
    Date.now = () => realNow() + clockOffset
    cleanup(() => {
      Date.now = realNow
    })
    const advancePastStalledThreshold = () => {
      clockOffset += stalledThreshold + 1
    }

    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    for (const id of ['long-running', 'not-renewed']) {
      await adapter.pushOn('test-queue', { id, name: 'TestJob', payload: {}, attempts: 0 })
    }

    // Jobs pushed in the same millisecond can be acquired in either order.
    const acquired = [await adapter.popFrom('test-queue'), await adapter.popFrom('test-queue')]
    const job = acquired.find((candidate) => candidate?.id === 'long-running')
    assert.isDefined(job)

    advancePastStalledThreshold()
    assert.equal(await adapter.renewJobs('test-queue', [job!]), 1)

    const first = await adapter.recoverStalledJobs('test-queue', stalledThreshold, 1, 100)

    // The control job proves the threshold had passed; the renewed job stays active.
    assert.equal(first.recovered, 1)
    assert.equal((await adapter.getJob('long-running', 'test-queue'))!.status, 'active')
    assert.equal((await adapter.getJob('not-renewed', 'test-queue'))!.status, 'pending')

    // Past the threshold since the first renewal, only the second renewal,
    // with the same lease, keeps the job active.
    advancePastStalledThreshold()
    assert.equal(await adapter.renewJobs('test-queue', [job!]), 1)

    const second = await adapter.recoverStalledJobs('test-queue', stalledThreshold, 1, 100)
    assert.equal(second.recovered, 0)
    assert.equal((await adapter.getJob('long-running', 'test-queue'))!.status, 'active')
  })

  test('renewJobs should only renew jobs that are still active', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')
    assert.isNotNull(job)

    // Let it stall and recover it back to pending.
    await new Promise((resolve) => setTimeout(resolve, 30))
    const { recovered } = await adapter.recoverStalledJobs('test-queue', 10, 1, 100)
    assert.equal(recovered, 1)

    // A late heartbeat for the (no longer active) job must not resurrect it.
    const renewed = await adapter.renewJobs('test-queue', [job!])
    assert.equal(renewed, 0)

    // The recovered job is still pending and can be acquired exactly once.
    const reacquired = await adapter.popFrom('test-queue')
    assert.isNotNull(reacquired)
    assert.equal(reacquired!.id, 'job-1')
  })

  test('renewJobs should only renew jobs on the targeted queue', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('queue-a', { id: 'job-a', name: 'TestJob', payload: null, attempts: 0 })
    await adapter.pushOn('queue-b', { id: 'job-b', name: 'TestJob', payload: null, attempts: 0 })

    await adapter.popFrom('queue-a')
    const jobB = await adapter.popFrom('queue-b')

    // job-b is active on queue-b, so renewing it on queue-a renews nothing.
    const renewed = await adapter.renewJobs('queue-a', [jobB!])
    assert.equal(renewed, 0)
  })

  test('renewJobs should return 0 when given no job ids', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    const renewed = await adapter.renewJobs('test-queue', [])
    assert.equal(renewed, 0)
  })

  test('completeJob with undefined retention should remove job (default behavior)', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-default',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')
    await adapter.completeJob(job!, 'test-queue')

    const record = await adapter.getJob(job!.id, 'test-queue')
    assert.isNull(record)
  })

  test('failJob with undefined retention should remove job (default behavior)', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-fail-default',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')
    await adapter.failJob(job!, 'test-queue', new Error('fail'))

    const record = await adapter.getJob(job!.id, 'test-queue')
    assert.isNull(record)
  })

  test('retention with both age and count should apply both', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    // Create 3 jobs
    for (let i = 1; i <= 3; i++) {
      await adapter.pushOn('test-queue', {
        id: `job-combo-${i}`,
        name: 'TestJob',
        payload: {},
        attempts: 0,
      })
    }

    // Complete all with count: 2
    for (let i = 1; i <= 3; i++) {
      const job = await adapter.popFrom('test-queue')
      await adapter.completeJob(job!, 'test-queue', { count: 2, age: '1h' })
    }

    // Only last 2 should remain (count: 2)
    const record1 = await adapter.getJob('job-combo-1', 'test-queue')
    const record2 = await adapter.getJob('job-combo-2', 'test-queue')
    const record3 = await adapter.getJob('job-combo-3', 'test-queue')

    assert.isNull(record1)
    assert.isNotNull(record2)
    assert.isNotNull(record3)
  })

  test('failJob retention count should prune failed jobs', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-fail-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })
    await adapter.pushOn('test-queue', {
      id: 'job-fail-2',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job1 = await adapter.popFrom('test-queue')
    const job2 = await adapter.popFrom('test-queue')

    await adapter.failJob(job1!, 'test-queue', new Error('error 1'), { count: 1 })
    await adapter.failJob(job2!, 'test-queue', new Error('error 2'), { count: 1 })

    const record1 = await adapter.getJob(job1!.id, 'test-queue')
    const record2 = await adapter.getJob(job2!.id, 'test-queue')

    assert.isNull(record1)
    assert.isNotNull(record2)
    assert.equal(record2!.status, 'failed')
  })

  test('completeJob on non-active job should be no-op', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-pending-complete',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    // Try to complete without popping (job is still pending)
    await adapter.completeJob(unleased('job-pending-complete'), 'test-queue', false)

    // Job should still be pending
    const record = await adapter.getJob('job-pending-complete', 'test-queue')
    assert.isNotNull(record)
    assert.equal(record!.status, 'pending')
  })

  test('completeJob on non-active job should not prune history', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-history-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })
    await adapter.pushOn('test-queue', {
      id: 'job-history-2',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job1 = await adapter.popFrom('test-queue')
    const job2 = await adapter.popFrom('test-queue')

    await adapter.completeJob(job1!, 'test-queue', false)
    await adapter.completeJob(job2!, 'test-queue', false)

    await adapter.pushOn('test-queue', {
      id: 'job-history-pending',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    await adapter.completeJob(unleased('job-history-pending'), 'test-queue', { count: 1 })

    const record1 = await adapter.getJob('job-history-1', 'test-queue')
    const record2 = await adapter.getJob('job-history-2', 'test-queue')

    assert.isNotNull(record1)
    assert.isNotNull(record2)
    assert.equal(record1!.status, 'completed')
    assert.equal(record2!.status, 'completed')
  })

  test('completeJob on pending job with default retention should keep job', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-pending-default',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    await adapter.completeJob(unleased('job-pending-default'), 'test-queue')

    const record = await adapter.getJob('job-pending-default', 'test-queue')
    assert.isNotNull(record)
    assert.equal(record!.status, 'pending')
  })

  test('popFrom should give each acquisition its own lease token', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    const { stale, current } = await acquireTwice(adapter)

    assert.match(stale.leaseToken, /^worker-1:/)
    assert.match(current.leaseToken, /^worker-1:/)
    assert.notEqual(stale.leaseToken, current.leaseToken)
  })

  test('completeJob should ignore a stale lease', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    const { stale, current } = await acquireTwice(adapter)

    assert.isFalse(await adapter.completeJob(stale, 'test-queue', false))
    assert.isFalse(await adapter.completeJob(stale, 'test-queue', true))
    assert.equal((await adapter.getJob('job-1', 'test-queue'))!.status, 'active')
    assert.equal(await adapter.renewJobs('test-queue', [stale]), 0)
    assert.equal(await adapter.renewJobs('test-queue', [current]), 1)

    assert.isTrue(await adapter.completeJob(current, 'test-queue', false))
    assert.equal((await adapter.getJob('job-1', 'test-queue'))!.status, 'completed')
  })

  test('failJob should ignore a stale lease', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    const { stale, current } = await acquireTwice(adapter)

    assert.isFalse(await adapter.failJob(stale, 'test-queue', new Error('late'), false))
    assert.isFalse(await adapter.failJob(stale, 'test-queue', new Error('late'), true))
    assert.equal((await adapter.getJob('job-1', 'test-queue'))!.status, 'active')

    assert.isTrue(await adapter.failJob(current, 'test-queue', new Error('boom'), false))
    const record = await adapter.getJob('job-1', 'test-queue')
    assert.equal(record!.status, 'failed')
    assert.equal(record!.error, 'boom')
  })

  test('retryJob should ignore a stale lease', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    const { stale, current } = await acquireTwice(adapter)

    assert.isFalse(await adapter.retryJob(stale, 'test-queue'))
    assert.isFalse(await adapter.retryJob(stale, 'test-queue', new Date(Date.now() + 60_000)))
    assert.equal((await adapter.getJob('job-1', 'test-queue'))!.status, 'active')
    assert.isNull(await adapter.popFrom('test-queue'))

    assert.isTrue(await adapter.retryJob(current, 'test-queue'))
    const retried = await adapter.popFrom('test-queue')
    assert.equal(retried!.id, 'job-1')
    assert.equal(retried!.attempts, 1)
  })

  test('failJob on non-active job should be no-op', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-pending-fail',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    // Try to fail without popping (job is still pending)
    await adapter.failJob(unleased('job-pending-fail'), 'test-queue', new Error('fail'), false)

    // Job should still be pending
    const record = await adapter.getJob('job-pending-fail', 'test-queue')
    assert.isNotNull(record)
    assert.equal(record!.status, 'pending')
  })

  test('double completeJob should not cause errors', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-double-complete',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')
    await adapter.completeJob(job!, 'test-queue', false)

    // Second complete should not throw
    await adapter.completeJob(job!, 'test-queue', false)

    const record = await adapter.getJob(job!.id, 'test-queue')
    assert.isNotNull(record)
    assert.equal(record!.status, 'completed')
  })

  test('jobs in different queues should be isolated', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('queue-a', {
      id: 'job-a',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })
    await adapter.pushOn('queue-b', {
      id: 'job-b',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    const jobA = await adapter.popFrom('queue-a')
    const jobB = await adapter.popFrom('queue-b')

    assert.isNotNull(jobA)
    assert.isNotNull(jobB)
    assert.equal(jobA!.id, 'job-a')
    assert.equal(jobB!.id, 'job-b')

    // Queue A should be empty now
    const nextA = await adapter.popFrom('queue-a')
    assert.isNull(nextA)
  })

  test('pruning should only affect its own queue', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    // Add jobs to two queues
    await adapter.pushOn('queue-prune-a', {
      id: 'job-prune-a',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })
    await adapter.pushOn('queue-prune-b', {
      id: 'job-prune-b',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    // Complete both with count: 0 (would prune if in same queue)
    const jobA = await adapter.popFrom('queue-prune-a')
    const jobB = await adapter.popFrom('queue-prune-b')

    await adapter.completeJob(jobA!, 'queue-prune-a', { count: 1 })
    await adapter.completeJob(jobB!, 'queue-prune-b', { count: 1 })

    // Both should still exist (different queues)
    const recordA = await adapter.getJob(jobA!.id, 'queue-prune-a')
    const recordB = await adapter.getJob(jobB!.id, 'queue-prune-b')

    assert.isNotNull(recordA)
    assert.isNotNull(recordB)
  })

  test('jobs with higher priority should be processed first', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    // Push in reverse priority order
    await adapter.pushOn('test-queue', {
      id: 'job-low',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 10, // low priority (higher number = lower priority)
    })
    await adapter.pushOn('test-queue', {
      id: 'job-high',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 1, // high priority
    })
    await adapter.pushOn('test-queue', {
      id: 'job-medium',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      priority: 5, // medium priority
    })

    const first = await adapter.popFrom('test-queue')
    const second = await adapter.popFrom('test-queue')
    const third = await adapter.popFrom('test-queue')

    assert.equal(first!.id, 'job-high')
    assert.equal(second!.id, 'job-medium')
    assert.equal(third!.id, 'job-low')
  })

  test('job lifecycle: pending -> active -> completed', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-lifecycle',
      name: 'TestJob',
      payload: { step: 1 },
      attempts: 0,
    })

    // Check pending
    let record = await adapter.getJob('job-lifecycle', 'test-queue')
    assert.equal(record!.status, 'pending')

    // Pop -> active
    const job = await adapter.popFrom('test-queue')
    record = await adapter.getJob('job-lifecycle', 'test-queue')
    assert.equal(record!.status, 'active')

    // Complete
    await adapter.completeJob(job!, 'test-queue', false)
    record = await adapter.getJob('job-lifecycle', 'test-queue')
    assert.equal(record!.status, 'completed')
    assert.isNumber(record!.finishedAt)
  })

  test('job lifecycle: pending -> active -> retry -> pending -> active -> failed', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-retry-lifecycle',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    // First attempt
    const job1 = await adapter.popFrom('test-queue')
    assert.equal(job1!.attempts, 0)

    // Retry
    await adapter.retryJob(job1!, 'test-queue')

    // Check it's back to pending
    let record = await adapter.getJob('job-retry-lifecycle', 'test-queue')
    assert.equal(record!.status, 'pending')

    // Second attempt
    const job2 = await adapter.popFrom('test-queue')
    assert.equal(job2!.attempts, 1)

    // Fail
    await adapter.failJob(job2!, 'test-queue', new Error('max retries'), false)

    record = await adapter.getJob('job-retry-lifecycle', 'test-queue')
    assert.equal(record!.status, 'failed')
    assert.equal(record!.error, 'max retries')
  })

  test('retryJob should preserve empty arrays in the payload', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-retry-empty-array-payload',
      name: 'TestJob',
      payload: {
        empty: [],
        nested: {
          items: [],
        },
      },
      attempts: 0,
    })

    const first = await adapter.popFrom('test-queue')
    await adapter.retryJob(first!, 'test-queue')

    const retried = await adapter.popFrom('test-queue')

    assert.deepEqual(retried!.payload, {
      empty: [],
      nested: {
        items: [],
      },
    })
    assert.equal(retried!.attempts, 1)
  })

  test('delayed job becomes available after delay', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushLaterOn(
      'test-queue',
      {
        id: 'job-short-delay',
        name: 'TestJob',
        payload: {},
        attempts: 0,
      },
      10
    ) // 10ms delay

    // Should be delayed initially
    let record = await adapter.getJob('job-short-delay', 'test-queue')
    assert.equal(record!.status, 'delayed')

    // Wait for delay
    await new Promise((resolve) => setTimeout(resolve, 20))

    // Pop should now work (triggers delayed job processing)
    const job = await adapter.popFrom('test-queue')
    assert.isNotNull(job)
    assert.equal(job!.id, 'job-short-delay')
  })

  // Concurrent tests only run for adapters that support multi-instance concurrency
  // Memory adapter doesn't share state between instances
  if (options.supportsConcurrency !== false) {
    test('concurrent popFrom should not return the same job twice', async ({ assert }) => {
      const adapter1 = await options.createAdapter()
      const adapter2 = await options.createAdapter()

      adapter1.setWorkerId('worker-1')
      adapter2.setWorkerId('worker-2')

      // Push a single job
      await adapter1.pushOn('test-queue', {
        id: 'job-1',
        name: 'TestJob',
        payload: {},
        attempts: 0,
      })

      // Both workers try to pop simultaneously
      const [job1, job2] = await Promise.all([
        adapter1.popFrom('test-queue'),
        adapter2.popFrom('test-queue'),
      ])

      // Only one worker should get the job
      const acquiredJobs = [job1, job2].filter((job) => job !== null)
      assert.equal(acquiredJobs.length, 1, 'Only one worker should acquire the job')
    })

    test('concurrent popFrom with multiple jobs should distribute jobs', async ({ assert }) => {
      const adapter1 = await options.createAdapter()
      const adapter2 = await options.createAdapter()

      adapter1.setWorkerId('worker-1')
      adapter2.setWorkerId('worker-2')

      // Push multiple jobs
      await adapter1.pushOn('test-queue', {
        id: 'job-1',
        name: 'TestJob',
        payload: {},
        attempts: 0,
      })
      await adapter1.pushOn('test-queue', {
        id: 'job-2',
        name: 'TestJob',
        payload: {},
        attempts: 0,
      })

      // Both workers try to pop simultaneously
      const [job1, job2] = await Promise.all([
        adapter1.popFrom('test-queue'),
        adapter2.popFrom('test-queue'),
      ])

      // Both workers should get different jobs
      assert.isNotNull(job1)
      assert.isNotNull(job2)
      assert.notEqual(job1!.id, job2!.id, 'Workers should acquire different jobs')
    })

    test('a worker cannot finalize a job acquired again by another worker', async ({ assert }) => {
      const adapter1 = await options.createAdapter()
      const adapter2 = await options.createAdapter()

      adapter1.setWorkerId('worker-1')
      adapter2.setWorkerId('worker-2')

      await adapter1.pushOn('test-queue', {
        id: 'job-1',
        name: 'TestJob',
        payload: {},
        attempts: 0,
      })

      // worker-1 stalls, worker-2 recovers the job and acquires it.
      const stale = await adapter1.popFrom('test-queue')
      await new Promise((resolve) => setTimeout(resolve, 30))
      assert.equal((await adapter2.recoverStalledJobs('test-queue', 10, 1, 100)).recovered, 1)
      const current = await adapter2.popFrom('test-queue')
      assert.match(current!.leaseToken, /^worker-2:/)

      assert.equal(await adapter1.renewJobs('test-queue', [stale!]), 0)
      assert.isFalse(await adapter1.completeJob(stale!, 'test-queue', false))
      assert.equal((await adapter2.getJob('job-1', 'test-queue'))!.status, 'active')

      assert.equal(await adapter2.renewJobs('test-queue', [current!]), 1)
      assert.isTrue(await adapter2.completeJob(current!, 'test-queue', true))
    })
  }

  test('upsertSchedule should create a new schedule', async ({ assert }) => {
    const adapter = await options.createAdapter()

    const id = await adapter.upsertSchedule({
      name: 'TestJob',
      payload: { foo: 'bar' },
      everyMs: 5000,
      timezone: 'UTC',
    })

    assert.isString(id)

    const schedule = await adapter.getSchedule(id)
    assert.isNotNull(schedule)
    assert.equal(schedule!.name, 'TestJob')
    assert.deepEqual(schedule!.payload, { foo: 'bar' })
    assert.equal(schedule!.everyMs, 5000)
    assert.equal(schedule!.status, 'active')
  })

  test('upsertSchedule should use provided id', async ({ assert }) => {
    const adapter = await options.createAdapter()

    const id = await adapter.upsertSchedule({
      id: 'my-custom-id',
      name: 'TestJob',
      payload: {},
      cronExpression: '0 0 * * *',
      timezone: 'UTC',
    })

    assert.equal(id, 'my-custom-id')

    const schedule = await adapter.getSchedule('my-custom-id')
    assert.isNotNull(schedule)
    assert.equal(schedule!.cronExpression, '0 0 * * *')
  })

  test('upsertSchedule should upsert when id exists', async ({ assert }) => {
    const adapter = await options.createAdapter()

    // Create initial schedule
    await adapter.upsertSchedule({
      id: 'upsert-test',
      name: 'TestJob',
      payload: { version: 1 },
      everyMs: 5000,
      timezone: 'UTC',
    })

    // Upsert with new values
    await adapter.upsertSchedule({
      id: 'upsert-test',
      name: 'TestJob',
      payload: { version: 2 },
      everyMs: 10000,
      timezone: 'Europe/Paris',
    })

    const schedule = await adapter.getSchedule('upsert-test')
    assert.deepEqual(schedule!.payload, { version: 2 })
    assert.equal(schedule!.everyMs, 10000)
    assert.equal(schedule!.timezone, 'Europe/Paris')
  })

  test('upsertSchedule upsert should clear stale scheduling fields', async ({ assert }) => {
    const adapter = await options.createAdapter()

    const from = new Date('2024-01-01T00:00:00.000Z')
    const to = new Date('2024-12-31T23:59:59.999Z')

    await adapter.upsertSchedule({
      id: 'upsert-stale-fields',
      name: 'TestJob',
      payload: { version: 1 },
      cronExpression: '0 0 * * *',
      timezone: 'UTC',
      from,
      to,
      limit: 10,
    })

    await adapter.upsertSchedule({
      id: 'upsert-stale-fields',
      name: 'TestJob',
      payload: { version: 2 },
      everyMs: 30000,
      timezone: 'UTC',
    })

    const schedule = await adapter.getSchedule('upsert-stale-fields')
    assert.isNotNull(schedule)
    assert.deepEqual(schedule!.payload, { version: 2 })
    assert.equal(schedule!.everyMs, 30000)
    assert.isNull(schedule!.cronExpression)
    assert.isNull(schedule!.from)
    assert.isNull(schedule!.to)
    assert.isNull(schedule!.limit)
  })

  test('upsertSchedule stores an undefined payload as an empty object', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.upsertSchedule({
      id: 'schedule-undefined-payload',
      name: 'TestJob',
      payload: undefined,
      everyMs: 60_000,
      timezone: 'UTC',
    })

    const schedule = await adapter.getSchedule('schedule-undefined-payload')
    assert.deepEqual(schedule!.payload, {})
  })

  test('upsertSchedule without a payload replaces the previous payload', async ({ assert }) => {
    const adapter = await options.createAdapter()
    const config = {
      id: 'schedule-cleared-payload',
      name: 'TestJob',
      everyMs: 60_000,
      timezone: 'UTC',
    }

    await adapter.upsertSchedule({ ...config, payload: { version: 1 } })
    await adapter.upsertSchedule({ ...config, payload: undefined })

    const schedule = await adapter.getSchedule('schedule-cleared-payload')
    assert.deepEqual(schedule!.payload, {})
  })

  test('schedule payloads keep empty arrays through claims', async ({ assert }) => {
    const adapter = await options.createAdapter()
    const payload = { items: [], nested: { tags: [] } }

    await adapter.upsertSchedule({
      id: 'schedule-empty-arrays',
      name: 'TestJob',
      payload,
      everyMs: 60_000,
      timezone: 'UTC',
    })
    await adapter.updateSchedule('schedule-empty-arrays', {
      nextRunAt: new Date(Date.now() - 1_000),
    })

    const claimed = await adapter.claimDueSchedule()
    assert.deepEqual(claimed!.payload, payload)
    assert.deepEqual((await adapter.getSchedule('schedule-empty-arrays'))!.payload, payload)
  })

  test('pushOn should store a payload larger than 64 KB', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')
    const blob = 'x'.repeat(70_000)

    await adapter.pushOn('test-queue', {
      id: 'big',
      name: 'TestJob',
      payload: { blob },
      attempts: 0,
    })

    const job = await adapter.popFrom('test-queue')
    assert.equal((job!.payload as { blob: string }).blob.length, blob.length)
  })

  test('failJob should store an error message larger than 64 KB', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')
    const message = 'e'.repeat(70_000)

    await adapter.pushOn('test-queue', {
      id: 'big-error',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })
    const job = await adapter.popFrom('test-queue')
    await adapter.failJob(job!, 'test-queue', new Error(message), false)

    const record = await adapter.getJob('big-error', 'test-queue')
    assert.equal(record!.error!.length, message.length)
  })

  test('upsertSchedule should store a payload larger than 64 KB', async ({ assert }) => {
    const adapter = await options.createAdapter()
    const blob = 'x'.repeat(70_000)

    await adapter.upsertSchedule({
      id: 'big-schedule',
      name: 'TestJob',
      payload: { blob },
      everyMs: 60_000,
      timezone: 'UTC',
    })

    const schedule = await adapter.getSchedule('big-schedule')
    assert.equal((schedule!.payload as { blob: string }).blob.length, blob.length)
  })

  test('upsertSchedule should preserve runtime runCount when id exists', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.upsertSchedule({
      id: 'upsert-preserve-run-count',
      name: 'TestJob',
      payload: { version: 1 },
      everyMs: 5000,
      timezone: 'UTC',
    })

    await adapter.updateSchedule('upsert-preserve-run-count', {
      runCount: 3,
      lastRunAt: new Date(),
      nextRunAt: new Date(Date.now() + 60_000),
    })

    await adapter.upsertSchedule({
      id: 'upsert-preserve-run-count',
      name: 'TestJob',
      payload: { version: 2 },
      cronExpression: '*/5 * * * *',
      timezone: 'Europe/Paris',
    })

    const schedule = await adapter.getSchedule('upsert-preserve-run-count')
    assert.isNotNull(schedule)
    assert.deepEqual(schedule!.payload, { version: 2 })
    assert.equal(schedule!.cronExpression, '*/5 * * * *')
    assert.isNull(schedule!.everyMs)
    assert.equal(schedule!.runCount, 3)
  })

  test('upsertSchedule should create a new schedule active with its next run', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    const nextRunAt = new Date(Date.now() + 60_000)

    await adapter.upsertSchedule({
      id: 'upsert-new-next-run',
      name: 'TestJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
      nextRunAt,
    })

    const schedule = await adapter.getSchedule('upsert-new-next-run')
    assert.equal(schedule!.status, 'active')
    assert.equal(schedule!.nextRunAt!.getTime(), nextRunAt.getTime())
  })

  test('upsertSchedule should keep the status of an existing schedule', async ({ assert }) => {
    const adapter = await options.createAdapter()
    const config = {
      id: 'upsert-keep-paused',
      name: 'TestJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
    }

    await adapter.upsertSchedule(config)
    await adapter.updateSchedule(config.id, { status: 'paused' })

    // Even a new definition does not resume a paused schedule.
    await adapter.upsertSchedule(config)
    await adapter.upsertSchedule({ ...config, everyMs: 30_000 })

    assert.equal((await adapter.getSchedule(config.id))!.status, 'paused')
  })

  test('upsertSchedule should keep the next run when the timing is unchanged', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    const config = {
      id: 'upsert-keep-next-run',
      name: 'TestJob',
      cronExpression: '0 9 * * *',
      timezone: 'Europe/Paris',
      from: new Date('2026-01-01T00:00:00.000Z'),
      to: new Date('2099-01-01T00:00:00.000Z'),
      limit: 10,
    }
    const nextRunAt = new Date(Date.now() + 60_000)

    await adapter.upsertSchedule({ ...config, payload: { version: 1 }, nextRunAt })

    // A payload change is not a timing change.
    await adapter.upsertSchedule({
      ...config,
      payload: { version: 2 },
      nextRunAt: new Date(Date.now() + 3_600_000),
    })

    const schedule = await adapter.getSchedule(config.id)
    assert.deepEqual(schedule!.payload, { version: 2 })
    assert.equal(schedule!.nextRunAt!.getTime(), nextRunAt.getTime())
  })

  test('upsertSchedule should take the new next run when the timing changes', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    const base = {
      name: 'TestJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
      from: new Date('2026-01-01T00:00:00.000Z'),
      to: new Date('2099-01-01T00:00:00.000Z'),
      limit: 10,
    }
    const changes = {
      cron: { everyMs: undefined, cronExpression: '0 9 * * *' },
      every: { everyMs: 30_000 },
      timezone: { timezone: 'Europe/Paris' },
      from: { from: new Date('2026-02-01T00:00:00.000Z') },
      to: { to: new Date('2098-01-01T00:00:00.000Z') },
      limit: { limit: 20 },
    }

    for (const [field, change] of Object.entries(changes)) {
      const id = `upsert-timing-${field}`
      const nextRunAt = new Date(Date.now() + 3_600_000)

      await adapter.upsertSchedule({ ...base, id, nextRunAt: new Date(Date.now() + 60_000) })
      await adapter.upsertSchedule({ ...base, ...change, id, nextRunAt })

      const schedule = await adapter.getSchedule(id)
      assert.equal(schedule!.nextRunAt!.getTime(), nextRunAt.getTime(), field)
    }
  })

  test('upsertSchedule should keep a finished schedule finished', async ({ assert }) => {
    const adapter = await options.createAdapter()
    const config = {
      id: 'upsert-keep-finished',
      name: 'TestJob',
      payload: {},
      everyMs: 60_000,
      timezone: 'UTC',
      limit: 1,
    }

    await adapter.upsertSchedule({ ...config, nextRunAt: new Date(Date.now() + 60_000) })
    await adapter.updateSchedule(config.id, { runCount: 1, nextRunAt: null })

    await adapter.upsertSchedule({ ...config, nextRunAt: new Date(Date.now() + 60_000) })

    assert.isNull((await adapter.getSchedule(config.id))!.nextRunAt)
  })

  test('getSchedule should return null for non-existent schedule', async ({ assert }) => {
    const adapter = await options.createAdapter()

    const schedule = await adapter.getSchedule('non-existent')
    assert.isNull(schedule)
  })

  test('listSchedules should return all schedules', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.upsertSchedule({
      id: 'list-test-1',
      name: 'Job1',
      payload: {},
      everyMs: 5000,
      timezone: 'UTC',
    })
    await adapter.upsertSchedule({
      id: 'list-test-2',
      name: 'Job2',
      payload: {},
      everyMs: 10000,
      timezone: 'UTC',
    })

    const schedules = await adapter.listSchedules()
    const ids = schedules.map((s) => s.id)

    assert.include(ids, 'list-test-1')
    assert.include(ids, 'list-test-2')
  })

  test('listSchedules should filter by status', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.upsertSchedule({
      id: 'filter-active',
      name: 'Job1',
      payload: {},
      everyMs: 5000,
      timezone: 'UTC',
    })
    await adapter.upsertSchedule({
      id: 'filter-paused',
      name: 'Job2',
      payload: {},
      everyMs: 10000,
      timezone: 'UTC',
    })

    await adapter.updateSchedule('filter-paused', { status: 'paused' })

    const activeSchedules = await adapter.listSchedules({ status: 'active' })
    const pausedSchedules = await adapter.listSchedules({ status: 'paused' })

    assert.isTrue(activeSchedules.some((s) => s.id === 'filter-active'))
    assert.isFalse(activeSchedules.some((s) => s.id === 'filter-paused'))
    assert.isTrue(pausedSchedules.some((s) => s.id === 'filter-paused'))
    assert.isFalse(pausedSchedules.some((s) => s.id === 'filter-active'))
  })

  test('updateSchedule should update status', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.upsertSchedule({
      id: 'update-status-test',
      name: 'TestJob',
      payload: {},
      everyMs: 5000,
      timezone: 'UTC',
    })

    await adapter.updateSchedule('update-status-test', { status: 'paused' })

    const schedule = await adapter.getSchedule('update-status-test')
    assert.equal(schedule!.status, 'paused')
  })

  test('updateSchedule should update run metadata', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.upsertSchedule({
      id: 'update-meta-test',
      name: 'TestJob',
      payload: {},
      everyMs: 5000,
      timezone: 'UTC',
    })

    const now = new Date()
    const nextRun = new Date(now.getTime() + 5000)

    await adapter.updateSchedule('update-meta-test', {
      runCount: 5,
      lastRunAt: now,
      nextRunAt: nextRun,
    })

    const schedule = await adapter.getSchedule('update-meta-test')
    assert.equal(schedule!.runCount, 5)
    assert.approximately(schedule!.lastRunAt!.getTime(), now.getTime(), 1000)
    assert.approximately(schedule!.nextRunAt!.getTime(), nextRun.getTime(), 1000)
  })

  test('deleteSchedule should remove schedule', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.upsertSchedule({
      id: 'delete-test',
      name: 'TestJob',
      payload: {},
      everyMs: 5000,
      timezone: 'UTC',
    })

    await adapter.deleteSchedule('delete-test')

    const schedule = await adapter.getSchedule('delete-test')
    assert.isNull(schedule)
  })

  test('claimDueSchedule should return null when no schedules are due', async ({ assert }) => {
    const adapter = await options.createAdapter()

    // Create schedule with nextRunAt in the future
    await adapter.upsertSchedule({
      id: 'future-schedule',
      name: 'TestJob',
      payload: {},
      everyMs: 60000,
      timezone: 'UTC',
    })

    await adapter.updateSchedule('future-schedule', {
      nextRunAt: new Date(Date.now() + 60000),
    })

    const claimed = await adapter.claimDueSchedule()
    assert.isNull(claimed)
  })

  test('claimDueSchedule should claim a due schedule', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.upsertSchedule({
      id: 'due-schedule',
      name: 'DueJob',
      payload: { key: 'value' },
      everyMs: 5000,
      timezone: 'UTC',
    })

    // Make it due
    await adapter.updateSchedule('due-schedule', {
      nextRunAt: new Date(Date.now() - 1000),
    })

    const claimed = await adapter.claimDueSchedule()

    assert.isNotNull(claimed)
    assert.equal(claimed!.id, 'due-schedule')
    assert.equal(claimed!.name, 'DueJob')
    assert.deepEqual(claimed!.payload, { key: 'value' })
  })

  test('claimDueSchedule should update nextRunAt after claiming', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.upsertSchedule({
      id: 'claim-update-test',
      name: 'TestJob',
      payload: {},
      everyMs: 10000,
      timezone: 'UTC',
    })

    const pastDate = new Date(Date.now() - 1000)
    await adapter.updateSchedule('claim-update-test', { nextRunAt: pastDate })

    await adapter.claimDueSchedule()

    const after = await adapter.getSchedule('claim-update-test')
    assert.isNotNull(after!.nextRunAt)
    assert.isTrue(after!.nextRunAt!.getTime() > Date.now())
  })

  test('claimDueSchedule should increment runCount', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.upsertSchedule({
      id: 'runcount-test',
      name: 'TestJob',
      payload: {},
      everyMs: 5000,
      timezone: 'UTC',
    })

    await adapter.updateSchedule('runcount-test', {
      nextRunAt: new Date(Date.now() - 1000),
    })

    const before = await adapter.getSchedule('runcount-test')
    assert.equal(before!.runCount, 0)

    await adapter.claimDueSchedule()

    const after = await adapter.getSchedule('runcount-test')
    assert.equal(after!.runCount, 1)
  })

  test('claimDueSchedule should not claim paused schedules', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.upsertSchedule({
      id: 'paused-claim-test',
      name: 'TestJob',
      payload: {},
      everyMs: 5000,
      timezone: 'UTC',
    })

    await adapter.updateSchedule('paused-claim-test', {
      nextRunAt: new Date(Date.now() - 1000),
      status: 'paused',
    })

    const claimed = await adapter.claimDueSchedule()
    assert.isNull(claimed)
  })

  test('claimDueSchedule should not claim when limit reached', async ({ assert }) => {
    const adapter = await options.createAdapter()

    await adapter.upsertSchedule({
      id: 'limit-claim-test',
      name: 'TestJob',
      payload: {},
      everyMs: 5000,
      timezone: 'UTC',
      limit: 5,
    })

    await adapter.updateSchedule('limit-claim-test', {
      nextRunAt: new Date(Date.now() - 1000),
      runCount: 5,
    })

    const claimed = await adapter.claimDueSchedule()
    assert.isNull(claimed)
  })

  // Concurrent schedule tests
  if (options.supportsConcurrency !== false) {
    test('concurrent claimDueSchedule should not claim same schedule twice', async ({ assert }) => {
      const adapter1 = await options.createAdapter()
      const adapter2 = await options.createAdapter()

      // Create a single due schedule
      await adapter1.upsertSchedule({
        id: 'concurrent-claim-test',
        name: 'TestJob',
        payload: {},
        everyMs: 60000,
        timezone: 'UTC',
      })

      await adapter1.updateSchedule('concurrent-claim-test', {
        nextRunAt: new Date(Date.now() - 1000),
      })

      // Both adapters try to claim simultaneously
      const [claimed1, claimed2] = await Promise.all([
        adapter1.claimDueSchedule(),
        adapter2.claimDueSchedule(),
      ])

      // Only one should succeed
      const claimedSchedules = [claimed1, claimed2].filter((s) => s !== null)
      assert.equal(claimedSchedules.length, 1, 'Only one adapter should claim the schedule')
    })

    test('concurrent upserts of a new schedule all succeed', async ({ assert }) => {
      const adapters = await Promise.all(Array.from({ length: 10 }, () => options.createAdapter()))
      const config = {
        id: 'concurrent-new-identical',
        name: 'TestJob',
        payload: {},
        everyMs: 60_000,
        timezone: 'UTC',
      }
      const nextRunAt = new Date(Date.now() + 60_000)

      // Workers booting together define the same schedule.
      const results = await Promise.allSettled(
        adapters.map((adapter) => adapter.upsertSchedule({ ...config, nextRunAt }))
      )

      assert.deepEqual(
        results.filter((result) => result.status === 'rejected'),
        []
      )
      const schedule = await adapters[0].getSchedule(config.id)
      assert.equal(schedule!.status, 'active')
      assert.equal(schedule!.nextRunAt!.getTime(), nextRunAt.getTime())
    })

    test('concurrent upserts keep the next run of the definition that wins', async ({ assert }) => {
      const adapters = await Promise.all(Array.from({ length: 10 }, () => options.createAdapter()))
      const base = Date.now() + 60_000

      for (let round = 0; round < 3; round++) {
        const id = `concurrent-new-timing-${round}`

        // Each definition has its own interval and the next run that matches it.
        const results = await Promise.allSettled(
          adapters.map((adapter, index) => {
            const everyMs = (index + 1) * 60_000
            return adapter.upsertSchedule({
              id,
              name: 'TestJob',
              payload: { index },
              everyMs,
              timezone: 'UTC',
              nextRunAt: new Date(base + everyMs),
            })
          })
        )

        assert.deepEqual(
          results.filter((result) => result.status === 'rejected'),
          []
        )
        const schedule = await adapters[0].getSchedule(id)
        assert.equal(schedule!.nextRunAt!.getTime() - base, schedule!.everyMs)
        assert.deepEqual(schedule!.payload, { index: schedule!.everyMs! / 60_000 - 1 })
      }
    })

    test('high-concurrency claimDueSchedule stress test', async ({ assert }) => {
      const adapters = await Promise.all(Array.from({ length: 10 }, () => options.createAdapter()))

      // Create a single due schedule
      await adapters[0].upsertSchedule({
        id: 'stress-test-schedule',
        name: 'StressJob',
        payload: { test: true },
        everyMs: 60000,
        timezone: 'UTC',
      })

      await adapters[0].updateSchedule('stress-test-schedule', {
        nextRunAt: new Date(Date.now() - 1000),
      })

      // All 10 adapters try to claim simultaneously
      const results = await Promise.all(adapters.map((adapter) => adapter.claimDueSchedule()))

      // Exactly one should succeed
      const claimedSchedules = results.filter((s) => s !== null)
      assert.equal(claimedSchedules.length, 1, 'Exactly one adapter should claim the schedule')

      // The claimed schedule should have the correct data
      const claimed = claimedSchedules[0]!
      assert.equal(claimed.id, 'stress-test-schedule')
      assert.equal(claimed.name, 'StressJob')
      assert.deepEqual(claimed.payload, { test: true })
    })
  }

  // pushManyOn tests
  test('pushManyOn should insert multiple jobs', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushManyOn('test-queue', [
      { id: 'batch-1', name: 'TestJob', payload: { idx: 1 }, attempts: 0 },
      { id: 'batch-2', name: 'TestJob', payload: { idx: 2 }, attempts: 0 },
      { id: 'batch-3', name: 'TestJob', payload: { idx: 3 }, attempts: 0 },
    ])

    const job1 = await adapter.popFrom('test-queue')
    const job2 = await adapter.popFrom('test-queue')
    const job3 = await adapter.popFrom('test-queue')
    const job4 = await adapter.popFrom('test-queue')

    assert.isNotNull(job1)
    assert.isNotNull(job2)
    assert.isNotNull(job3)
    assert.isNull(job4)

    assert.equal(job1!.id, 'batch-1')
    assert.equal(job2!.id, 'batch-2')
    assert.equal(job3!.id, 'batch-3')
  })

  test('pushManyOn with empty array should not error', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushManyOn('test-queue', [])

    const job = await adapter.popFrom('test-queue')
    assert.isNull(job)
  })

  test('pushManyOn should preserve groupId', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushManyOn('test-queue', [
      { id: 'group-1', name: 'TestJob', payload: {}, attempts: 0, groupId: 'batch-abc' },
      { id: 'group-2', name: 'TestJob', payload: {}, attempts: 0, groupId: 'batch-abc' },
    ])

    const job1 = await adapter.popFrom('test-queue')
    const job2 = await adapter.popFrom('test-queue')

    assert.equal(job1!.groupId, 'batch-abc')
    assert.equal(job2!.groupId, 'batch-abc')
  })

  test('pushManyOn should respect priority ordering', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushManyOn('test-queue', [
      { id: 'low', name: 'TestJob', payload: {}, attempts: 0, priority: 10 },
      { id: 'high', name: 'TestJob', payload: {}, attempts: 0, priority: 1 },
      { id: 'medium', name: 'TestJob', payload: {}, attempts: 0, priority: 5 },
    ])

    const first = await adapter.popFrom('test-queue')
    const second = await adapter.popFrom('test-queue')
    const third = await adapter.popFrom('test-queue')

    assert.equal(first!.id, 'high')
    assert.equal(second!.id, 'medium')
    assert.equal(third!.id, 'low')
  })

  test('pushOn with dedup should skip duplicate job', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'TestJob::order-1',
      name: 'TestJob',
      payload: { attempt: 1 },
      attempts: 0,
      dedup: { id: 'order-1' },
    })

    await adapter.pushOn('test-queue', {
      id: 'TestJob::order-1',
      name: 'TestJob',
      payload: { attempt: 2 },
      attempts: 0,
      dedup: { id: 'order-1' },
    })

    const size = await adapter.sizeOf('test-queue')
    assert.equal(size, 1)

    const job = await adapter.popFrom('test-queue')
    assert.deepEqual(job!.payload, { attempt: 1 })
  })

  test('pushOn with dedup should accept an undefined payload', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'dedup-undefined-payload',
      name: 'TestJob',
      payload: undefined,
      attempts: 0,
      dedup: { id: 'undefined-payload', ttl: 10_000 },
    })

    const job = await adapter.popFrom('test-queue')

    assert.isNotNull(job)
    assert.equal(job!.id, 'dedup-undefined-payload')
    assert.isUndefined(job!.payload)
  })

  test('pushOn without dedup should insert normally', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: { data: 'first' },
      attempts: 0,
    })

    await adapter.pushOn('test-queue', {
      id: 'job-2',
      name: 'TestJob',
      payload: { data: 'second' },
      attempts: 0,
    })

    const size = await adapter.sizeOf('test-queue')
    assert.equal(size, 2)
  })

  test('pushLaterOn with dedup should skip duplicate delayed job', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushLaterOn(
      'test-queue',
      {
        id: 'TestJob::delayed-1',
        name: 'TestJob',
        payload: { attempt: 1 },
        attempts: 0,
        dedup: { id: 'delayed-1' },
      },
      60_000
    )

    await adapter.pushLaterOn(
      'test-queue',
      {
        id: 'TestJob::delayed-1',
        name: 'TestJob',
        payload: { attempt: 2 },
        attempts: 0,
        dedup: { id: 'delayed-1' },
      },
      60_000
    )

    const job = await adapter.getJob('TestJob::delayed-1', 'test-queue')
    assert.isNotNull(job)
    assert.deepEqual(job!.data.payload, { attempt: 1 })
  })

  test('pushLaterOn dedup replace preserves the original job id', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushLaterOn(
      'rep-delayed-queue',
      {
        id: 'delayed-rep-uuid-1',
        name: 'TestJob',
        payload: { version: 1 },
        attempts: 0,
        dedup: { id: 'TestJob::delayed-rep-1', ttl: 10_000, replace: true },
      },
      50
    )

    const second = await adapter.pushLaterOn(
      'rep-delayed-queue',
      {
        id: 'delayed-rep-uuid-2',
        name: 'TestJob',
        payload: { version: 2 },
        attempts: 0,
        dedup: { id: 'TestJob::delayed-rep-1', ttl: 10_000, replace: true },
      },
      50
    )
    assert.equal(second && typeof second === 'object' && second.outcome, 'replaced')
    assert.equal(second && typeof second === 'object' && second.jobId, 'delayed-rep-uuid-1')

    await new Promise((r) => setTimeout(r, 80))

    const job = await adapter.popFrom('rep-delayed-queue')
    assert.isNotNull(job)
    assert.equal(job!.id, 'delayed-rep-uuid-1')
    assert.deepEqual(job!.payload, { version: 2 })
  })

  test('pushOn with dedup should allow same id on different queues', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('queue-a', {
      id: 'TestJob::shared-id',
      name: 'TestJob',
      payload: { queue: 'a' },
      attempts: 0,
      dedup: { id: 'shared-id' },
    })

    await adapter.pushOn('queue-b', {
      id: 'TestJob::shared-id',
      name: 'TestJob',
      payload: { queue: 'b' },
      attempts: 0,
      dedup: { id: 'shared-id' },
    })

    const sizeA = await adapter.sizeOf('queue-a')
    const sizeB = await adapter.sizeOf('queue-b')
    assert.equal(sizeA, 1)
    assert.equal(sizeB, 1)
  })

  test('dedup TTL: new job allowed after TTL expires', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('ttl-queue', {
      id: 'uuid-1',
      name: 'TestJob',
      payload: { n: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::ttl-1', ttl: 80 },
    })

    const second = await adapter.pushOn('ttl-queue', {
      id: 'uuid-2',
      name: 'TestJob',
      payload: { n: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::ttl-1', ttl: 80 },
    })
    assert.equal(second && typeof second === 'object' && second.outcome, 'skipped')

    await new Promise((r) => setTimeout(r, 150))

    const third = await adapter.pushOn('ttl-queue', {
      id: 'uuid-3',
      name: 'TestJob',
      payload: { n: 3 },
      attempts: 0,
      dedup: { id: 'TestJob::ttl-1', ttl: 80 },
    })
    assert.equal(third && typeof third === 'object' && third.outcome, 'added')
  })

  test('dedup replace: duplicate within TTL swaps payload on pending job', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('rep-queue', {
      id: 'rep-uuid-1',
      name: 'TestJob',
      payload: { version: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::rep-1', ttl: 10_000, replace: true },
    })

    const second = await adapter.pushOn('rep-queue', {
      id: 'rep-uuid-2',
      name: 'TestJob',
      payload: { version: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::rep-1', ttl: 10_000, replace: true },
    })
    assert.equal(second && typeof second === 'object' && second.outcome, 'replaced')
    assert.equal(second && typeof second === 'object' && second.jobId, 'rep-uuid-1')

    const size = await adapter.sizeOf('rep-queue')
    assert.equal(size, 1)

    const job = await adapter.popFrom('rep-queue')
    assert.equal(job!.id, 'rep-uuid-1')
    assert.deepEqual(job!.payload, { version: 2 })
  })

  test('dedup replace: accepts an undefined replacement payload', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('rep-undefined-queue', {
      id: 'rep-undefined-uuid-1',
      name: 'TestJob',
      payload: { version: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::rep-undefined-1', ttl: 10_000, replace: true },
    })

    const second = await adapter.pushOn('rep-undefined-queue', {
      id: 'rep-undefined-uuid-2',
      name: 'TestJob',
      payload: undefined,
      attempts: 0,
      dedup: { id: 'TestJob::rep-undefined-1', ttl: 10_000, replace: true },
    })

    const job = await adapter.popFrom('rep-undefined-queue')

    assert.equal(second && typeof second === 'object' && second.outcome, 'replaced')
    assert.equal(second && typeof second === 'object' && second.jobId, 'rep-undefined-uuid-1')
    assert.isUndefined(job!.payload)
  })

  test('dedup replace: preserves empty arrays in the replacement payload', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('rep-empty-array-queue', {
      id: 'rep-empty-array-uuid-1',
      name: 'TestJob',
      payload: {
        version: 1,
      },
      attempts: 0,
      priority: 1,
      dedup: { id: 'TestJob::rep-empty-array-1', ttl: 10_000, replace: true },
    })

    const second = await adapter.pushOn('rep-empty-array-queue', {
      id: 'rep-empty-array-uuid-2',
      name: 'TestJob',
      payload: {
        empty: [],
        nested: {
          items: [],
        },
      },
      attempts: 0,
      priority: 9,
      dedup: { id: 'TestJob::rep-empty-array-1', ttl: 10_000, replace: true },
    })

    const record = await adapter.getJob('rep-empty-array-uuid-1', 'rep-empty-array-queue')
    const popped = await adapter.popFrom('rep-empty-array-queue')

    assert.equal(second && typeof second === 'object' && second.outcome, 'replaced')
    assert.equal(second && typeof second === 'object' && second.jobId, 'rep-empty-array-uuid-1')
    assert.deepEqual(record!.data.payload, {
      empty: [],
      nested: {
        items: [],
      },
    })
    assert.deepEqual(popped!.payload, record!.data.payload)
    assert.equal(popped!.priority, 1)
  })

  test('dedup replace: retry keeps replacement payload and increments attempts', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('rep-retry-queue', {
      id: 'rep-retry-uuid-1',
      name: 'TestJob',
      payload: { version: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::rep-retry-1', ttl: 10_000, replace: true },
    })

    await adapter.pushOn('rep-retry-queue', {
      id: 'rep-retry-uuid-2',
      name: 'TestJob',
      payload: {
        version: 2,
        empty: [],
      },
      attempts: 0,
      dedup: { id: 'TestJob::rep-retry-1', ttl: 10_000, replace: true },
    })

    const first = await adapter.popFrom('rep-retry-queue')
    await adapter.retryJob(first!, 'rep-retry-queue')

    const retried = await adapter.popFrom('rep-retry-queue')

    assert.equal(retried!.id, 'rep-retry-uuid-1')
    assert.deepEqual(retried!.payload, {
      version: 2,
      empty: [],
    })
    assert.equal(retried!.attempts, 1)
  })

  test('dedup replace: retained completed job keeps replacement payload', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('rep-history-queue', {
      id: 'rep-history-uuid-1',
      name: 'TestJob',
      payload: { version: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::rep-history-1', ttl: 10_000, replace: true },
    })

    await adapter.pushOn('rep-history-queue', {
      id: 'rep-history-uuid-2',
      name: 'TestJob',
      payload: {
        version: 2,
        empty: [],
      },
      attempts: 0,
      dedup: { id: 'TestJob::rep-history-1', ttl: 10_000, replace: true },
    })

    const job = await adapter.popFrom('rep-history-queue')
    await adapter.completeJob(job!, 'rep-history-queue', false)

    const record = await adapter.getJob('rep-history-uuid-1', 'rep-history-queue')

    assert.equal(record!.status, 'completed')
    assert.deepEqual(record!.data.payload, {
      version: 2,
      empty: [],
    })
  })

  test('dedup extend: duplicate within TTL resets the window', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('ext-queue', {
      id: 'ext-uuid-1',
      name: 'TestJob',
      payload: { n: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::ext-1', ttl: 400, extend: true },
    })

    await new Promise((r) => setTimeout(r, 250))

    const second = await adapter.pushOn('ext-queue', {
      id: 'ext-uuid-2',
      name: 'TestJob',
      payload: { n: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::ext-1', ttl: 400, extend: true },
    })
    assert.equal(second && typeof second === 'object' && second.outcome, 'extended')

    await new Promise((r) => setTimeout(r, 250))

    // Without the extend at T=250, the window would have expired at T=400. With it,
    // only 250ms of the new 400ms window have passed.
    const third = await adapter.pushOn('ext-queue', {
      id: 'ext-uuid-3',
      name: 'TestJob',
      payload: { n: 3 },
      attempts: 0,
      dedup: { id: 'TestJob::ext-1', ttl: 400, extend: true },
    })
    assert.equal(third && typeof third === 'object' && third.outcome, 'extended')
  })

  test('dedup: cleanup removes dedup entry when job is completed without retention', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('clean-queue', {
      id: 'clean-uuid-1',
      name: 'TestJob',
      payload: { n: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::clean-1' },
    })

    const popped = await adapter.popFrom('clean-queue')
    await adapter.completeJob(popped!, 'clean-queue', true)

    // Dedup should be cleaned — new push should succeed
    const second = await adapter.pushOn('clean-queue', {
      id: 'clean-uuid-2',
      name: 'TestJob',
      payload: { n: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::clean-1' },
    })
    assert.equal(second && typeof second === 'object' && second.outcome, 'added')
  })

  test('dedup: cleanup removes dedup entry when job fails without retention', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('clean-fail', {
      id: 'fail-uuid-1',
      name: 'TestJob',
      payload: { n: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::fail-1' },
    })

    const popped = await adapter.popFrom('clean-fail')
    await adapter.failJob(popped!, 'clean-fail', new Error('boom'), true)

    const second = await adapter.pushOn('clean-fail', {
      id: 'fail-uuid-2',
      name: 'TestJob',
      payload: { n: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::fail-1' },
    })
    assert.equal(second && typeof second === 'object' && second.outcome, 'added')
  })

  test('dedup: retryJob preserves dedup entry (new dispatch stays blocked)', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('retry-queue', {
      id: 'retry-uuid-1',
      name: 'TestJob',
      payload: { n: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::retry-1' },
    })

    const popped = await adapter.popFrom('retry-queue')
    await adapter.retryJob(popped!, 'retry-queue')

    // retry puts job back — dedup entry still points to same job
    const second = await adapter.pushOn('retry-queue', {
      id: 'retry-uuid-2',
      name: 'TestJob',
      payload: { n: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::retry-1' },
    })
    assert.equal(second && typeof second === 'object' && second.outcome, 'skipped')
  })

  test('dedup: pushManyOn rejects jobs with dedup', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await assert.rejects(
      () =>
        adapter.pushManyOn('batch-queue', [
          { id: 'a', name: 'TestJob', payload: {}, attempts: 0 },
          {
            id: 'b',
            name: 'TestJob',
            payload: {},
            attempts: 0,
            dedup: { id: 'TestJob::batch-1' },
          },
        ]),
      /dedup is not supported in batch dispatch/
    )
  })

  test('dedup TTL: old pending job still runs after TTL expiry, new dispatch adds as new entry', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('ttl-keep-queue', {
      id: 'keep-uuid-1',
      name: 'TestJob',
      payload: { n: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::keep-1', ttl: 50 },
    })

    await new Promise((r) => setTimeout(r, 120))

    const second = await adapter.pushOn('ttl-keep-queue', {
      id: 'keep-uuid-2',
      name: 'TestJob',
      payload: { n: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::keep-1', ttl: 50 },
    })
    assert.equal(second && typeof second === 'object' && second.outcome, 'added')
    assert.equal(second && typeof second === 'object' && second.jobId, 'keep-uuid-2')

    assert.equal(await adapter.sizeOf('ttl-keep-queue'), 2)

    const first = await adapter.popFrom('ttl-keep-queue')
    assert.equal(first!.id, 'keep-uuid-1')
    assert.deepEqual(first!.payload, { n: 1 })

    const next = await adapter.popFrom('ttl-keep-queue')
    assert.equal(next!.id, 'keep-uuid-2')
    assert.deepEqual(next!.payload, { n: 2 })
  })

  test('dedup replace: preserves priority and groupId of the existing job', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('rep-preserve-queue', {
      id: 'preserve-uuid-1',
      name: 'TestJob',
      payload: { version: 1 },
      attempts: 0,
      priority: 1,
      groupId: 'group-a',
      dedup: { id: 'TestJob::preserve-1', ttl: 10_000, replace: true },
    })

    const second = await adapter.pushOn('rep-preserve-queue', {
      id: 'preserve-uuid-2',
      name: 'TestJob',
      payload: { version: 2 },
      attempts: 0,
      priority: 9,
      dedup: { id: 'TestJob::preserve-1', ttl: 10_000, replace: true },
    })
    assert.equal(second && typeof second === 'object' && second.outcome, 'replaced')
    assert.equal(second && typeof second === 'object' && second.jobId, 'preserve-uuid-1')

    const record = await adapter.getJob('preserve-uuid-1', 'rep-preserve-queue')
    assert.isNotNull(record)
    assert.deepEqual(record!.data.payload, { version: 2 })
    assert.equal(record!.data.priority, 1)
    assert.equal(record!.data.groupId, 'group-a')
  })

  test('dedup replace: leaves retained completed jobs untouched, returns skipped', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('rep-retain-queue', {
      id: 'retain-uuid-1',
      name: 'TestJob',
      payload: { version: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::retain-1', ttl: 10_000, replace: true },
    })

    const popped = await adapter.popFrom('rep-retain-queue')
    await adapter.completeJob(popped!, 'rep-retain-queue', false)

    const second = await adapter.pushOn('rep-retain-queue', {
      id: 'retain-uuid-2',
      name: 'TestJob',
      payload: { version: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::retain-1', ttl: 10_000, replace: true },
    })
    assert.equal(second && typeof second === 'object' && second.outcome, 'skipped')
    assert.equal(second && typeof second === 'object' && second.jobId, 'retain-uuid-1')

    const record = await adapter.getJob('retain-uuid-1', 'rep-retain-queue')
    assert.isNotNull(record)
    assert.deepEqual(record!.data.payload, { version: 1 })
  })

  test('dedup extend: window length stays the original ttl even when later dispatches pass a different ttl', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('extend-original-queue', {
      id: 'extend-orig-uuid-1',
      name: 'TestJob',
      payload: { n: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::extend-orig-1', ttl: 100, extend: true },
    })

    await new Promise((r) => setTimeout(r, 50))

    const second = await adapter.pushOn('extend-original-queue', {
      id: 'extend-orig-uuid-2',
      name: 'TestJob',
      payload: { n: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::extend-orig-1', ttl: 5000, extend: true },
    })
    assert.equal(second && typeof second === 'object' && second.outcome, 'extended')

    // 150ms after the reset (T50). Original 100ms window expired at T150.
    // If the engine were honoring the new 5000ms ttl, the slot would still
    // be alive and this dispatch would return 'extended'.
    await new Promise((r) => setTimeout(r, 200))

    const third = await adapter.pushOn('extend-original-queue', {
      id: 'extend-orig-uuid-3',
      name: 'TestJob',
      payload: { n: 3 },
      attempts: 0,
      dedup: { id: 'TestJob::extend-orig-1', ttl: 100, extend: true },
    })
    assert.equal(third && typeof third === 'object' && third.outcome, 'added')
    assert.equal(third && typeof third === 'object' && third.jobId, 'extend-orig-uuid-3')
  })

  test('dedup debounce: replace + extend swaps payload and resets the TTL window', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('debounce-queue', {
      id: 'debounce-uuid-1',
      name: 'TestJob',
      payload: { version: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::debounce-1', ttl: 400, extend: true, replace: true },
    })

    await new Promise((r) => setTimeout(r, 250))

    const second = await adapter.pushOn('debounce-queue', {
      id: 'debounce-uuid-2',
      name: 'TestJob',
      payload: { version: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::debounce-1', ttl: 400, extend: true, replace: true },
    })
    assert.equal(second && typeof second === 'object' && second.outcome, 'replaced')
    assert.equal(second && typeof second === 'object' && second.jobId, 'debounce-uuid-1')

    const midRecord = await adapter.getJob('debounce-uuid-1', 'debounce-queue')
    assert.deepEqual(midRecord!.data.payload, { version: 2 })

    // 500ms total elapsed > original 400ms TTL, but the second dispatch reset
    // the window at T=250. Only 250ms into the new window → still alive.
    await new Promise((r) => setTimeout(r, 250))

    const third = await adapter.pushOn('debounce-queue', {
      id: 'debounce-uuid-3',
      name: 'TestJob',
      payload: { version: 3 },
      attempts: 0,
      dedup: { id: 'TestJob::debounce-1', ttl: 400, extend: true, replace: true },
    })
    assert.equal(third && typeof third === 'object' && third.outcome, 'replaced')
    assert.equal(third && typeof third === 'object' && third.jobId, 'debounce-uuid-1')

    const finalRecord = await adapter.getJob('debounce-uuid-1', 'debounce-queue')
    assert.deepEqual(finalRecord!.data.payload, { version: 3 })
  })

  test('dedup replace: returns skipped when existing job is already active (in-flight)', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('active-rep-queue', {
      id: 'active-rep-uuid-1',
      name: 'TestJob',
      payload: { version: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::active-rep-1', ttl: 10_000, replace: true },
    })

    // Move job to active state — worker has popped it.
    const popped = await adapter.popFrom('active-rep-queue')
    assert.equal(popped!.id, 'active-rep-uuid-1')

    const second = await adapter.pushOn('active-rep-queue', {
      id: 'active-rep-uuid-2',
      name: 'TestJob',
      payload: { version: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::active-rep-1', ttl: 10_000, replace: true },
    })

    assert.equal(second && typeof second === 'object' && second.outcome, 'skipped')
    assert.equal(second && typeof second === 'object' && second.jobId, 'active-rep-uuid-1')

    // Payload must not be swapped while job is in-flight.
    assert.deepEqual(popped!.payload, { version: 1 })
  })

  test('dedup extend: refreshes TTL even when existing job is already active (in-flight)', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('active-ext-queue', {
      id: 'active-ext-uuid-1',
      name: 'TestJob',
      payload: { n: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::active-ext-1', ttl: 400, extend: true },
    })

    // Move to active mid-window.
    await new Promise((r) => setTimeout(r, 250))
    const popped = await adapter.popFrom('active-ext-queue')
    assert.equal(popped!.id, 'active-ext-uuid-1')

    // Extend against an in-flight job — implementation refreshes the dedup TTL
    // even though the existing job is active (not replaceable).
    const second = await adapter.pushOn('active-ext-queue', {
      id: 'active-ext-uuid-2',
      name: 'TestJob',
      payload: { n: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::active-ext-1', ttl: 400, extend: true },
    })
    assert.equal(second && typeof second === 'object' && second.outcome, 'extended')
    assert.equal(second && typeof second === 'object' && second.jobId, 'active-ext-uuid-1')

    // Without the extend, the slot would have expired by now (250 + 250 > 400).
    // With the extend at T=250, the window restarted; at T=500 only 250ms into
    // new window → still blocking.
    await new Promise((r) => setTimeout(r, 250))

    const third = await adapter.pushOn('active-ext-queue', {
      id: 'active-ext-uuid-3',
      name: 'TestJob',
      payload: { n: 3 },
      attempts: 0,
      dedup: { id: 'TestJob::active-ext-1', ttl: 400, extend: true },
    })
    assert.equal(third && typeof third === 'object' && third.outcome, 'extended')
    assert.equal(third && typeof third === 'object' && third.jobId, 'active-ext-uuid-1')
  })

  if (options.supportsAtomicDedup !== false) {
    test('dedup: concurrent pushOn with same id - only one wins, rest skipped', async ({
      assert,
    }) => {
      const adapter = await options.createAdapter()
      adapter.setWorkerId('worker-1')

      const dispatches = Array.from({ length: 5 }, (_, i) =>
        adapter.pushOn('concurrent-dedup-queue', {
          id: `concurrent-uuid-${i}`,
          name: 'TestJob',
          payload: { n: i },
          attempts: 0,
          dedup: { id: 'TestJob::concurrent-1' },
        })
      )

      const results = await Promise.all(dispatches)
      const outcomes = results.map((r) => (r && typeof r === 'object' ? r.outcome : undefined))

      assert.equal(
        outcomes.filter((o) => o === 'added').length,
        1,
        `Expected exactly one 'added' outcome, got ${JSON.stringify(outcomes)}`
      )
      assert.equal(
        outcomes.filter((o) => o === 'skipped').length,
        4,
        `Expected four 'skipped' outcomes, got ${JSON.stringify(outcomes)}`
      )

      const size = await adapter.sizeOf('concurrent-dedup-queue')
      assert.equal(size, 1)

      // All skipped results must point at the same winner job id.
      const winners = results
        .filter((r) => r && typeof r === 'object' && r.outcome === 'added')
        .map((r) => (r as { jobId: string }).jobId)
      const skippedJobIds = results
        .filter((r) => r && typeof r === 'object' && r.outcome === 'skipped')
        .map((r) => (r as { jobId: string }).jobId)
      for (const id of skippedJobIds) {
        assert.equal(id, winners[0], 'skipped dispatch should reference the winning job id')
      }
    })
  }
}
