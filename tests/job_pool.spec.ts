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
    const originalThen = execution.promise.then.bind(execution.promise)
    let observed = false

    execution.promise.then = ((onFulfilled, onRejected) => {
      observed = onRejected !== undefined
      return originalThen(onFulfilled, onRejected)
    }) as Promise<void>['then']

    const pool = new JobPool()
    cleanup(async () => {
      execution.resolve()
      await pool.drain()
    })

    pool.add(createJob('observed-job'), 'default', execution.promise)

    assert.isTrue(observed)
  })

  test('attaches one settlement handler while a completion wait remains pending', async ({
    assert,
    cleanup,
  }) => {
    const execution = Promise.withResolvers<void>()
    let attachments = 0
    const observablePromise = {
      then(onFulfilled, onRejected) {
        attachments++
        return execution.promise.then(onFulfilled, onRejected)
      },
      catch(onRejected) {
        return this.then(undefined, onRejected)
      },
    } as Promise<void>

    const pool = new JobPool()
    cleanup(async () => {
      execution.resolve()
      await pool.drain()
    })

    pool.add(createJob('observed-once'), 'default', observablePromise)
    const completion = pool.waitForNextCompletion()

    for (let tick = 0; tick < 10; tick++) {
      await setTimeout(0)
    }

    assert.equal(attachments, 1)

    execution.resolve()
    await completion
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

  test('returns a completion queued before waiting begins', async ({ assert }) => {
    const pool = new JobPool()
    pool.add(createJob('already-completed'), 'default', Promise.resolve())

    await setTimeout(0)

    const completed = await pool.waitForNextCompletion()
    assert.equal(completed.job.id, 'already-completed')
    assert.isTrue(pool.isEmpty())
  })

  test('returns a newer job that settles after waiting begins', async ({ assert }) => {
    const pool = new JobPool()
    const longExecution = Promise.withResolvers<void>()
    pool.add(createJob('long-running'), 'default', longExecution.promise)

    const completion = pool.waitForNextCompletion()
    pool.add(createJob('newer-fast-job'), 'default', Promise.resolve())

    assert.equal((await completion).job.id, 'newer-fast-job')
    longExecution.resolve()
    await pool.drain()
  })

  test('ignores a stale completion after an active job id is replaced', async ({ assert }) => {
    const pool = new JobPool()
    const staleExecution = Promise.withResolvers<void>()
    const currentExecution = Promise.withResolvers<void>()

    pool.add(createJob('reused-id'), 'stale', staleExecution.promise)
    staleExecution.resolve()
    await setTimeout(0)
    pool.add(createJob('reused-id'), 'current', currentExecution.promise)

    let delivered = false
    const completion = pool.waitForNextCompletion().then((entry) => {
      delivered = true
      return entry
    })

    await setTimeout(0)
    assert.isFalse(delivered)

    currentExecution.resolve()
    assert.equal((await completion).queue, 'current')
    assert.isTrue(pool.isEmpty())
  })

  test('returns every job once in settlement order', async ({ assert }) => {
    const pool = new JobPool()
    const first = Promise.withResolvers<void>()
    const second = Promise.withResolvers<void>()
    const third = Promise.withResolvers<void>()

    pool.add(createJob('first'), 'default', first.promise)
    pool.add(createJob('second'), 'default', second.promise)
    pool.add(createJob('third'), 'default', third.promise)

    second.resolve()
    await setTimeout(0)
    first.resolve()
    await setTimeout(0)
    third.resolve()

    assert.deepEqual(
      [
        (await pool.waitForNextCompletion()).job.id,
        (await pool.waitForNextCompletion()).job.id,
        (await pool.waitForNextCompletion()).job.id,
      ],
      ['second', 'first', 'third']
    )
    assert.isTrue(pool.isEmpty())
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

  test('drain clears completions that settled before they were consumed', async ({ assert }) => {
    const pool = new JobPool()
    const running = Promise.withResolvers<void>()

    pool.add(createJob('settled'), 'default', Promise.resolve())
    pool.add(createJob('running'), 'default', running.promise)
    await setTimeout(0)

    const drain = pool.drain()
    running.resolve()
    await drain

    assert.isTrue(pool.isEmpty())

    pool.add(createJob('after-drain'), 'default', Promise.resolve())
    assert.equal((await pool.waitForNextCompletion()).job.id, 'after-drain')
  })
})
