import { test as JapaTest } from '@japa/runner'
import type { Adapter } from '../../src/contracts/adapter.js'

interface DriverTestSuiteOptions {
  test: typeof JapaTest
  createAdapter: () => Adapter | Promise<Adapter>
  /**
   * Whether this adapter supports concurrent access from multiple instances.
   * Memory adapter doesn't share state between instances, so concurrent tests are skipped.
   * @default true
   */
  supportsConcurrency?: boolean
}

export function registerDriverTestSuite(options: DriverTestSuiteOptions) {
  const { test } = options

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

    await adapter.completeJob(job!.id, 'test-queue')

    // Retry should have no effect since job is no longer active
    await adapter.retryJob(job!.id, 'test-queue')

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
    await adapter.completeJob(job!.id, 'test-queue', false)
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
    await adapter.failJob(job!.id, 'test-queue', new Error('Something went wrong'), false)

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

    await adapter.completeJob(job!.id, 'test-queue', false)

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

    await adapter.completeJob(job!.id, 'test-queue', true)

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

    await adapter.retryJob(job1!.id, 'test-queue')

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
    await adapter.retryJob(job!.id, 'test-queue', futureDate)

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

    await adapter.failJob(job!.id, 'test-queue', new Error('Test error'))

    // Retry should have no effect since job is no longer active
    await adapter.retryJob(job!.id, 'test-queue')

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

    await adapter.failJob(job!.id, 'test-queue', new Error('Test error'), false)

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

    await adapter.completeJob(job1!.id, 'test-queue', { count: 1 })
    await adapter.completeJob(job2!.id, 'test-queue', { count: 1 })

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

    await adapter.completeJob(job1!.id, 'test-queue', { age: '1ms' })

    await new Promise((resolve) => setTimeout(resolve, 5))

    const job2 = await adapter.popFrom('test-queue')
    assert.isNotNull(job2)

    await adapter.completeJob(job2!.id, 'test-queue', { age: '1ms' })

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
    const recovered = await adapter.recoverStalledJobs('test-queue', 1000, 1)
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
    const recovered = await adapter.recoverStalledJobs('test-queue', 60000, 1)
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
    const recovered = await adapter.recoverStalledJobs('test-queue', 10, 1)
    assert.equal(recovered, 1)

    // Job should be back in pending queue
    const nextJob = await adapter.popFrom('test-queue')
    assert.isNotNull(nextJob)
    assert.equal(nextJob!.id, 'job-1')
    assert.deepEqual(nextJob!.payload, { foo: 'bar' })
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
    await adapter.recoverStalledJobs('test-queue', 10, 3)

    const job1 = await adapter.popFrom('test-queue')
    assert.isNotNull(job1)
    assert.equal(job1!.stalledCount, 1)

    // Second stall cycle
    await new Promise((resolve) => setTimeout(resolve, 50))
    await adapter.recoverStalledJobs('test-queue', 10, 3)

    const job2 = await adapter.popFrom('test-queue')
    assert.isNotNull(job2)
    assert.equal(job2!.stalledCount, 2)
  })

  test('recoverStalledJobs should fail job permanently when maxStalledCount exceeded', async ({
    assert,
  }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('test-queue', {
      id: 'job-1',
      name: 'TestJob',
      payload: {},
      attempts: 0,
      stalledCount: 0,
    })

    // First stall - should recover (stalledCount becomes 1)
    await adapter.popFrom('test-queue')
    await new Promise((resolve) => setTimeout(resolve, 50))
    let recovered = await adapter.recoverStalledJobs('test-queue', 10, 1)
    assert.equal(recovered, 1)

    // Second stall - should fail permanently (stalledCount would be 2, exceeds maxStalledCount=1)
    await adapter.popFrom('test-queue')
    await new Promise((resolve) => setTimeout(resolve, 50))
    recovered = await adapter.recoverStalledJobs('test-queue', 10, 1)
    assert.equal(recovered, 0) // Not recovered, but failed

    // Job should be gone (failed permanently)
    const nextJob = await adapter.popFrom('test-queue')
    assert.isNull(nextJob)
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
    const recovered = await adapter.recoverStalledJobs('test-queue', 10, 1)
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

    const recoveredA = await adapter.recoverStalledJobs('queue-a', 10, 1)
    assert.equal(recoveredA, 1)

    const recoveredJobA = await adapter.popFrom('queue-a')
    assert.isNotNull(recoveredJobA)
    assert.equal(recoveredJobA!.id, 'job-stalled-a')

    const queueBPending = await adapter.popFrom('queue-b')
    assert.isNull(queueBPending)

    await new Promise((resolve) => setTimeout(resolve, 50))

    const recoveredB = await adapter.recoverStalledJobs('queue-b', 10, 1)
    assert.equal(recoveredB, 1)

    const recoveredJobB = await adapter.popFrom('queue-b')
    assert.isNotNull(recoveredJobB)
    assert.equal(recoveredJobB!.id, 'job-stalled-b')
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
    await adapter.completeJob(job!.id, 'test-queue')

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
    await adapter.failJob(job!.id, 'test-queue', new Error('fail'))

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
      await adapter.completeJob(job!.id, 'test-queue', { count: 2, age: '1h' })
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

    await adapter.failJob(job1!.id, 'test-queue', new Error('error 1'), { count: 1 })
    await adapter.failJob(job2!.id, 'test-queue', new Error('error 2'), { count: 1 })

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
    await adapter.completeJob('job-pending-complete', 'test-queue', false)

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

    await adapter.completeJob(job1!.id, 'test-queue', false)
    await adapter.completeJob(job2!.id, 'test-queue', false)

    await adapter.pushOn('test-queue', {
      id: 'job-history-pending',
      name: 'TestJob',
      payload: {},
      attempts: 0,
    })

    await adapter.completeJob('job-history-pending', 'test-queue', { count: 1 })

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

    await adapter.completeJob('job-pending-default', 'test-queue')

    const record = await adapter.getJob('job-pending-default', 'test-queue')
    assert.isNotNull(record)
    assert.equal(record!.status, 'pending')
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
    await adapter.failJob('job-pending-fail', 'test-queue', new Error('fail'), false)

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
    await adapter.completeJob(job!.id, 'test-queue', false)

    // Second complete should not throw
    await adapter.completeJob(job!.id, 'test-queue', false)

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

    await adapter.completeJob(jobA!.id, 'queue-prune-a', { count: 1 })
    await adapter.completeJob(jobB!.id, 'queue-prune-b', { count: 1 })

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
    await adapter.completeJob(job!.id, 'test-queue', false)
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
    await adapter.retryJob(job1!.id, 'test-queue')

    // Check it's back to pending
    let record = await adapter.getJob('job-retry-lifecycle', 'test-queue')
    assert.equal(record!.status, 'pending')

    // Second attempt
    const job2 = await adapter.popFrom('test-queue')
    assert.equal(job2!.attempts, 1)

    // Fail
    await adapter.failJob(job2!.id, 'test-queue', new Error('max retries'), false)

    record = await adapter.getJob('job-retry-lifecycle', 'test-queue')
    assert.equal(record!.status, 'failed')
    assert.equal(record!.error, 'max retries')
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

  test('dedup extend: duplicate within TTL resets the window', async ({ assert }) => {
    const adapter = await options.createAdapter()
    adapter.setWorkerId('worker-1')

    await adapter.pushOn('ext-queue', {
      id: 'ext-uuid-1',
      name: 'TestJob',
      payload: { n: 1 },
      attempts: 0,
      dedup: { id: 'TestJob::ext-1', ttl: 100, extend: true },
    })

    await new Promise((r) => setTimeout(r, 60))

    const second = await adapter.pushOn('ext-queue', {
      id: 'ext-uuid-2',
      name: 'TestJob',
      payload: { n: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::ext-1', ttl: 100, extend: true },
    })
    assert.equal(second && typeof second === 'object' && second.outcome, 'extended')

    await new Promise((r) => setTimeout(r, 60))

    // Without extend, 50ms elapsed > 40ms TTL would've expired.
    const third = await adapter.pushOn('ext-queue', {
      id: 'ext-uuid-3',
      name: 'TestJob',
      payload: { n: 3 },
      attempts: 0,
      dedup: { id: 'TestJob::ext-1', ttl: 100, extend: true },
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
    await adapter.completeJob(popped!.id, 'clean-queue', true)

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
    await adapter.failJob(popped!.id, 'clean-fail', new Error('boom'), true)

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
    await adapter.retryJob(popped!.id, 'retry-queue')

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
    await adapter.completeJob(popped!.id, 'rep-retain-queue', false)

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
      dedup: { id: 'TestJob::debounce-1', ttl: 200, extend: true, replace: true },
    })

    await new Promise((r) => setTimeout(r, 120))

    const second = await adapter.pushOn('debounce-queue', {
      id: 'debounce-uuid-2',
      name: 'TestJob',
      payload: { version: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::debounce-1', ttl: 200, extend: true, replace: true },
    })
    assert.equal(second && typeof second === 'object' && second.outcome, 'replaced')
    assert.equal(second && typeof second === 'object' && second.jobId, 'debounce-uuid-1')

    const midRecord = await adapter.getJob('debounce-uuid-1', 'debounce-queue')
    assert.deepEqual(midRecord!.data.payload, { version: 2 })

    // 240ms total elapsed > original 200ms TTL, but the second dispatch reset
    // the window at T=120. Only 120ms into the new window → still alive.
    await new Promise((r) => setTimeout(r, 120))

    const third = await adapter.pushOn('debounce-queue', {
      id: 'debounce-uuid-3',
      name: 'TestJob',
      payload: { version: 3 },
      attempts: 0,
      dedup: { id: 'TestJob::debounce-1', ttl: 200, extend: true, replace: true },
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
      dedup: { id: 'TestJob::active-ext-1', ttl: 200, extend: true },
    })

    // Move to active mid-window.
    await new Promise((r) => setTimeout(r, 80))
    const popped = await adapter.popFrom('active-ext-queue')
    assert.equal(popped!.id, 'active-ext-uuid-1')

    // Extend against an in-flight job — implementation refreshes the dedup TTL
    // even though the existing job is active (not replaceable).
    const second = await adapter.pushOn('active-ext-queue', {
      id: 'active-ext-uuid-2',
      name: 'TestJob',
      payload: { n: 2 },
      attempts: 0,
      dedup: { id: 'TestJob::active-ext-1', ttl: 200, extend: true },
    })
    assert.equal(second && typeof second === 'object' && second.outcome, 'extended')
    assert.equal(second && typeof second === 'object' && second.jobId, 'active-ext-uuid-1')

    // Without the extend, the slot would have expired by now (80 + 150 > 200).
    // With the extend at T=80, the window restarted; at T=230 only 150ms into
    // new window → still blocking.
    await new Promise((r) => setTimeout(r, 150))

    const third = await adapter.pushOn('active-ext-queue', {
      id: 'active-ext-uuid-3',
      name: 'TestJob',
      payload: { n: 3 },
      attempts: 0,
      dedup: { id: 'TestJob::active-ext-1', ttl: 200, extend: true },
    })
    assert.equal(third && typeof third === 'object' && third.outcome, 'extended')
    assert.equal(third && typeof third === 'object' && third.jobId, 'active-ext-uuid-1')
  })

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
