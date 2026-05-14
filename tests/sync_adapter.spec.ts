import { setTimeout as sleep } from 'node:timers/promises'
import { test } from '@japa/runner'
import { Job } from '../src/job.js'
import { Locator } from '../src/locator.js'
import { QueueManager } from '../src/queue_manager.js'
import { sync } from '../src/drivers/sync_adapter.js'
import * as errors from '../src/exceptions.js'
import { MemoryLogger } from './_mocks/memory_logger.js'

test.group('SyncAdapter', (group) => {
  group.each.teardown(async () => {
    Locator.clear()
    await QueueManager.destroy()
  })

  test('should retry sync jobs and call failed() without bubbling execute errors', async ({
    assert,
  }) => {
    let executeAttempts = 0
    let failedCalls = 0
    let failedError: Error | undefined
    const attempts: number[] = []
    const contextJobIds: string[] = []

    class RetryingSyncJob extends Job<Record<string, never>> {
      static options = {
        maxRetries: 3,
      }

      async execute() {
        executeAttempts++
        attempts.push(this.context.attempt)
        contextJobIds.push(this.context.jobId)

        throw new Error('boom')
      }

      async failed(error: Error) {
        failedCalls++
        failedError = error
        contextJobIds.push(this.context.jobId)
      }
    }

    await QueueManager.init({
      default: 'sync',
      adapters: { sync: sync() },
    })

    Locator.register('RetryingSyncJob', RetryingSyncJob)

    const { jobId } = await RetryingSyncJob.dispatch({}).run()

    assert.equal(executeAttempts, 4)
    assert.deepEqual(attempts, [1, 2, 3, 4])
    assert.equal(failedCalls, 1)
    assert.instanceOf(failedError, errors.E_JOB_MAX_ATTEMPTS_REACHED)
    assert.deepEqual(contextJobIds, Array(contextJobIds.length).fill(jobId))
  })

  test('should log delayed sync job failures without unhandled rejections', async ({ assert }) => {
    const logger = new MemoryLogger()
    let unhandledError: unknown
    const onUnhandledRejection = (error: unknown) => {
      unhandledError = error
    }

    class DelayedFailingSyncJob extends Job<Record<string, never>> {
      async execute() {
        throw new Error('boom')
      }

      async failed() {
        throw new Error('failed hook exploded')
      }
    }

    process.once('unhandledRejection', onUnhandledRejection)

    try {
      await QueueManager.init({
        default: 'sync',
        adapters: { sync: sync() },
        logger,
      })

      Locator.register('DelayedFailingSyncJob', DelayedFailingSyncJob)

      const adapter = QueueManager.use()

      await adapter.pushLaterOn(
        'default',
        {
          id: 'delayed-sync-job',
          name: 'DelayedFailingSyncJob',
          payload: {},
          attempts: 0,
          priority: 0,
        },
        0
      )

      await sleep(20)
    } finally {
      process.removeListener('unhandledRejection', onUnhandledRejection)
    }

    assert.isUndefined(unhandledError)
    assert.lengthOf(logger.logs, 1)
    assert.equal(logger.logs[0].level, 'error')
    assert.equal(logger.logs[0].message, 'Failed to execute delayed sync job')
    assert.equal(logger.logs[0].obj?.jobId, 'delayed-sync-job')
    assert.equal(logger.logs[0].obj?.jobName, 'DelayedFailingSyncJob')
    assert.equal(logger.logs[0].obj?.queue, 'default')
    assert.instanceOf(logger.logs[0].obj?.err, Error)
    assert.equal((logger.logs[0].obj?.err as Error).message, 'failed hook exploded')
  })

  test('should ignore .dedup() and execute every dispatch inline', async ({ assert }) => {
    const executedPayloads: Array<{ n: number }> = []

    class DedupIgnoredSyncJob extends Job<{ n: number }> {
      async execute() {
        executedPayloads.push(this.payload)
      }
    }

    await QueueManager.init({
      default: 'sync',
      adapters: { sync: sync() },
    })

    Locator.register('DedupIgnoredSyncJob', DedupIgnoredSyncJob)

    const first = await DedupIgnoredSyncJob.dispatch({ n: 1 }).dedup({ id: 'sync-dedup-1' }).run()
    const second = await DedupIgnoredSyncJob.dispatch({ n: 2 }).dedup({ id: 'sync-dedup-1' }).run()
    const third = await DedupIgnoredSyncJob.dispatch({ n: 3 })
      .dedup({ id: 'sync-dedup-1', ttl: 10_000, replace: true })
      .run()

    assert.deepEqual(executedPayloads, [{ n: 1 }, { n: 2 }, { n: 3 }])
    assert.isUndefined(first.deduped)
    assert.isUndefined(second.deduped)
    assert.isUndefined(third.deduped)
  })
})
