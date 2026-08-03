import { setTimeout } from 'node:timers/promises'
import { test } from '@japa/runner'
import { JobPool } from '../src/job_pool.js'
import type { AcquiredJob } from '../src/contracts/adapter.js'

function createJob(id: string): AcquiredJob {
  return {
    id,
    name: 'TestJob',
    payload: {},
    attempts: 0,
    priority: 0,
    acquiredAt: Date.now(),
  }
}

test.group('JobPool', () => {
  test('should start empty', ({ assert }) => {
    const pool = new JobPool()

    assert.equal(pool.size, 0)
    assert.isTrue(pool.isEmpty())
  })

  test('should track size after adding jobs', ({ assert }) => {
    const pool = new JobPool()

    pool.add(createJob('job-1'), 'default', Promise.resolve())
    assert.equal(pool.size, 1)
    assert.isFalse(pool.isEmpty())

    pool.add(createJob('job-2'), 'default', Promise.resolve())
    assert.equal(pool.size, 2)
  })

  test('should observe job promise rejections as soon as they are added', async ({
    assert,
    cleanup,
  }) => {
    const execution = Promise.withResolvers<void>()
    const originalCatch = execution.promise.catch.bind(execution.promise)
    let observed = false

    execution.promise.catch = ((...args: Parameters<Promise<void>['catch']>) => {
      observed = true
      return originalCatch(...args)
    }) as Promise<void>['catch']

    const pool = new JobPool()
    cleanup(async () => {
      execution.resolve()
      await pool.drain()
    })

    pool.add(createJob('observed-job'), 'default', execution.promise)

    assert.isTrue(observed)
  })

  test('should check capacity correctly', ({ assert }) => {
    const pool = new JobPool()

    assert.isTrue(pool.hasCapacity(2))

    pool.add(createJob('job-1'), 'default', Promise.resolve())
    assert.isTrue(pool.hasCapacity(2))

    pool.add(createJob('job-2'), 'default', Promise.resolve())
    assert.isFalse(pool.hasCapacity(2))
  })

  test('should return first completed job', async ({ assert }) => {
    const pool = new JobPool()

    const slowJob = createJob('slow')
    const fastJob = createJob('fast')

    pool.add(slowJob, 'default', setTimeout(100))
    pool.add(fastJob, 'email', setTimeout(10))

    const completed = await pool.waitForNextCompletion()

    assert.equal(completed.job.id, 'fast')
    assert.equal(completed.queue, 'email')
    assert.equal(pool.size, 1)
  })

  test('should remove job from pool after completion', async ({ assert }) => {
    const pool = new JobPool()

    pool.add(createJob('job-1'), 'default', Promise.resolve())
    pool.add(createJob('job-2'), 'default', Promise.resolve())

    assert.equal(pool.size, 2)

    await pool.waitForNextCompletion()
    assert.equal(pool.size, 1)

    await pool.waitForNextCompletion()
    assert.equal(pool.size, 0)
    assert.isTrue(pool.isEmpty())
  })

  test('should handle job errors gracefully', async ({ assert }) => {
    const pool = new JobPool()

    const failingJob = createJob('failing')
    const failingPromise = Promise.reject(new Error('Job failed'))

    pool.add(failingJob, 'default', failingPromise)

    const completed = await pool.waitForNextCompletion()

    assert.equal(completed.job.id, 'failing')
    assert.isTrue(pool.isEmpty())
  })

  test('should return failing job before slow job', async ({ assert }) => {
    const pool = new JobPool()

    const slowJob = createJob('slow')
    const failingJob = createJob('failing')

    pool.add(slowJob, 'default', setTimeout(100))
    pool.add(failingJob, 'default', Promise.reject(new Error('Job failed')))

    const completed = await pool.waitForNextCompletion()

    assert.equal(completed.job.id, 'failing')
  })

  test('drain should wait for all jobs to complete', async ({ assert }) => {
    const pool = new JobPool()
    const completedJobs: string[] = []

    pool.add(
      createJob('job-1'),
      'default',
      setTimeout(50).then(() => {
        completedJobs.push('job-1')
      })
    )
    pool.add(
      createJob('job-2'),
      'default',
      setTimeout(30).then(() => {
        completedJobs.push('job-2')
      })
    )
    pool.add(
      createJob('job-3'),
      'default',
      setTimeout(10).then(() => {
        completedJobs.push('job-3')
      })
    )

    assert.equal(pool.size, 3)

    await pool.drain()

    assert.equal(completedJobs.length, 3)
    assert.isTrue(pool.isEmpty())
  })

  test('drain should handle errors gracefully', async ({ assert }) => {
    const pool = new JobPool()

    pool.add(createJob('success'), 'default', setTimeout(10))
    pool.add(createJob('failing'), 'default', Promise.reject(new Error('Job failed')))

    await pool.drain()

    assert.isTrue(pool.isEmpty())
  })
})
