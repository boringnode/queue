import { setTimeout } from 'node:timers/promises'
import { test } from '@japa/runner'
import { memory } from './_mocks/memory_adapter.js'
import { QueueManager } from '../src/queue_manager.js'
import { JobDispatcher } from '../src/job_dispatcher.js'
import { JobBatchDispatcher } from '../src/job_batch_dispatcher.js'

test.group('JobDispatcher', () => {
  test('should wrap adapter calls with internalOperationWrapper', async ({ assert }) => {
    const sharedAdapter = memory()()
    let internalCalls = 0

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
      internalOperationWrapper: async (fn) => {
        internalCalls++
        return fn()
      },
    })

    await new JobDispatcher('WrappedJob', { foo: 'bar' }).run()
    await new JobBatchDispatcher('WrappedBatchJob', [{ foo: 1 }, { foo: 2 }]).run()

    assert.equal(internalCalls, 2)
  })

  test('should dispatch job correctly', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('SendEmailJob', { to: 'romain.lanz@pm.me' })

    const { jobId } = await dispatcher.run()

    assert.isString(jobId)
    const job = await sharedAdapter.pop()

    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    assert.equal(job!.name, 'SendEmailJob')
    assert.deepEqual(job!.payload, { to: 'romain.lanz@pm.me' })
  })

  test('should dispatch job to specified queue', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('GenerateReportJob', { reportId: 123 })

    const { jobId } = await dispatcher.toQueue('emails').run()

    assert.isString(jobId)

    const job = await sharedAdapter.popFrom('emails')

    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    assert.equal(job!.name, 'GenerateReportJob')
    assert.deepEqual(job!.payload, { reportId: 123 })
  })

  test('should dispatch job using specific adapter', async ({ assert }) => {
    const memoryAdapter = memory()()
    const anotherMemoryAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => memoryAdapter, anotherMemory: () => anotherMemoryAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('CleanupJob', { days: 30 })

    const { jobId } = await dispatcher.with('anotherMemory').run()

    assert.isString(jobId)

    const job = await anotherMemoryAdapter.pop()
    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    assert.equal(job!.name, 'CleanupJob')
    assert.deepEqual(job!.payload, { days: 30 })

    const emptyJob = await memoryAdapter.pop()
    assert.isNull(emptyJob)
  })

  test('should dispatch job using adapter instance', async ({ assert }) => {
    const memoryAdapter = memory()()
    const anotherMemoryAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => memoryAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('CleanupJob', { days: 30 })

    const { jobId } = await dispatcher.with(() => anotherMemoryAdapter).run()

    assert.isString(jobId)

    const job = await anotherMemoryAdapter.pop()
    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    assert.equal(job!.name, 'CleanupJob')
    assert.deepEqual(job!.payload, { days: 30 })

    const emptyJob = await memoryAdapter.pop()
    assert.isNull(emptyJob)
  })

  test('should dispatch job with priority', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('HighPriorityJob', { task: 'urgent' })

    const { jobId } = await dispatcher.priority(10).run()

    assert.isString(jobId)

    const job = await sharedAdapter.pop()

    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    assert.equal(job!.name, 'HighPriorityJob')
    assert.deepEqual(job!.payload, { task: 'urgent' })
    assert.equal(job!.priority, 10)
  })

  test('should dispatch job with delay', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('DelayedJob', { info: 'wait for it' })

    const { jobId } = await dispatcher.in('1s').run()
    assert.isString(jobId)

    const job = await sharedAdapter.pop()
    assert.isNull(job)

    await setTimeout(2000)

    const delayedJob = await sharedAdapter.pop()
    assert.isNotNull(delayedJob)
    assert.equal(delayedJob!.id, jobId)
    assert.equal(delayedJob!.name, 'DelayedJob')
    assert.deepEqual(delayedJob!.payload, { info: 'wait for it' })
  }).timeout(5000)

  test('should support promise then() for auto-dispatching', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('AutoDispatchJob', { auto: true })

    const { jobId } = await dispatcher.priority(5).toQueue('auto-queue')
    assert.isString(jobId)

    const job = await sharedAdapter.popFrom('auto-queue')
    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    assert.equal(job!.name, 'AutoDispatchJob')
    assert.deepEqual(job!.payload, { auto: true })
    assert.equal(job!.priority, 5)
  })
})

test.group('JobDispatcher | DispatchResult', () => {
  test('run() should return DispatchResult with jobId', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('TestJob', { data: 'test' })

    const result = await dispatcher.run()

    // Should return an object with jobId, not just a string
    assert.isObject(result)
    assert.property(result, 'jobId')
    assert.isString(result.jobId)
  })

  test('then() should also return DispatchResult', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('TestJob', { data: 'test' })

    // Using await on dispatcher directly (via thenable)
    const result = await dispatcher

    assert.isObject(result)
    assert.property(result, 'jobId')
  })
})

test.group('JobDispatcher | groupId', () => {
  test('should dispatch job with groupId', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobDispatcher('NewsletterJob', { userId: 1 })

    const { jobId } = await dispatcher.group('newsletter-jan-2025').run()

    assert.isString(jobId)

    const job = await sharedAdapter.pop()

    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    assert.equal(job!.groupId, 'newsletter-jan-2025')
  })

  test('should dispatch multiple jobs with same groupId', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const groupId = 'batch-export-123'

    await new JobDispatcher('ExportJob', { userId: 1 }).group(groupId).run()
    await new JobDispatcher('ExportJob', { userId: 2 }).group(groupId).run()
    await new JobDispatcher('ExportJob', { userId: 3 }).group(groupId).run()

    const job1 = await sharedAdapter.pop()
    const job2 = await sharedAdapter.pop()
    const job3 = await sharedAdapter.pop()

    assert.equal(job1!.groupId, groupId)
    assert.equal(job2!.groupId, groupId)
    assert.equal(job3!.groupId, groupId)
  })

  test('should work with other options like priority and queue', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const { jobId } = await new JobDispatcher('ImportJob', { file: 'data.csv' })
      .group('import-batch-456')
      .toQueue('imports')
      .priority(2)
      .run()

    const job = await sharedAdapter.popFrom('imports')

    assert.isNotNull(job)
    assert.equal(job!.id, jobId)
    assert.equal(job!.groupId, 'import-batch-456')
    assert.equal(job!.priority, 2)
  })
})

test.group('JobDispatcher | dedup', () => {
  test('should throw error when dedup id is empty', async ({ assert }) => {
    assert.throws(
      () => new JobDispatcher('TestJob', { data: 'test' }).dedup({ id: '' }),
      'Dedup ID must be a non-empty string'
    )
  })

  test('should store dedup id prefixed with job name', async ({ assert }) => {
    const sharedAdapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    })

    const result = await new JobDispatcher('SendInvoiceJob', { orderId: 123 })
      .dedup({ id: 'order-123' })
      .run()

    assert.match(result.jobId, /^[0-9a-f-]{36}$/)
    assert.equal(result.deduped, 'added')

    const job = await sharedAdapter.pop()
    assert.isNotNull(job)
    assert.equal(job!.id, result.jobId)
    assert.equal(job!.dedup?.id, 'SendInvoiceJob::order-123')
  })

  test('should set dedup field on job data when dedup is configured', async ({ assert }) => {
    const sharedAdapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    })

    await new JobDispatcher('UniqueJob', { data: 'test' }).dedup({ id: 'my-id' }).run()

    const job = await sharedAdapter.pop()
    assert.isNotNull(job)
    assert.equal(job!.dedup?.id, 'UniqueJob::my-id')
  })

  test('should not set dedup field when dedup is not configured', async ({ assert }) => {
    const sharedAdapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    })

    await new JobDispatcher('RegularJob', { data: 'test' }).run()

    const job = await sharedAdapter.pop()
    assert.isNotNull(job)
    assert.isUndefined(job!.dedup)
  })

  test('should silently skip duplicate job with same dedup id', async ({ assert }) => {
    const sharedAdapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    })

    await new JobDispatcher('DedupJob', { attempt: 1 }).dedup({ id: 'dedup-1' }).run()
    await new JobDispatcher('DedupJob', { attempt: 2 }).dedup({ id: 'dedup-1' }).run()

    const size = await sharedAdapter.size()
    assert.equal(size, 1)

    const job = await sharedAdapter.pop()
    assert.deepEqual(job!.payload, { attempt: 1 })
  })

  test('should allow same dedup id for different job names', async ({ assert }) => {
    const sharedAdapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    })

    await new JobDispatcher('JobA', { type: 'a' }).dedup({ id: 'same-id' }).run()
    await new JobDispatcher('JobB', { type: 'b' }).dedup({ id: 'same-id' }).run()

    const size = await sharedAdapter.size()
    assert.equal(size, 2)
  })

  test('should work with other options like priority and queue', async ({ assert }) => {
    const sharedAdapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    })

    const { jobId, deduped } = await new JobDispatcher('PriorityDedupJob', { task: 'important' })
      .dedup({ id: 'task-1' })
      .toQueue('high')
      .priority(1)
      .run()

    assert.match(jobId, /^[0-9a-f-]{36}$/)
    assert.equal(deduped, 'added')

    const job = await sharedAdapter.popFrom('high')
    assert.isNotNull(job)
    assert.equal(job!.priority, 1)
    assert.equal(job!.dedup?.id, 'PriorityDedupJob::task-1')
  })

  test('should throw when extend is set without ttl', ({ assert }) => {
    assert.throws(
      () => new JobDispatcher('TestJob', {}).dedup({ id: 'x', extend: true }),
      'dedup.ttl is required when extend or replace is set'
    )
  })

  test('should throw when replace is set without ttl', ({ assert }) => {
    assert.throws(
      () => new JobDispatcher('TestJob', {}).dedup({ id: 'x', replace: true }),
      'dedup.ttl is required when extend or replace is set'
    )
  })

  test('should throw when ttl is negative', ({ assert }) => {
    assert.throws(
      () => new JobDispatcher('TestJob', {}).dedup({ id: 'x', ttl: -1 }),
      'dedup.ttl must be a positive duration'
    )
  })

  test('should throw when ttl is zero', ({ assert }) => {
    assert.throws(
      () => new JobDispatcher('TestJob', {}).dedup({ id: 'x', ttl: 0 }),
      'dedup.ttl must be a positive duration'
    )
  })

  test('should throw when dedup id exceeds 400 chars', ({ assert }) => {
    assert.throws(
      () => new JobDispatcher('TestJob', {}).dedup({ id: 'a'.repeat(401) }),
      'Dedup ID must be 400 characters or less'
    )
  })

  test('should throw when job name + dedup id combined exceeds 510 chars', ({ assert }) => {
    const longJobName = 'A'.repeat(200)
    assert.throws(
      () => new JobDispatcher(longJobName, {}).dedup({ id: 'b'.repeat(400) }),
      /combined with job name exceeds 510 characters/
    )
  })

  test('TTL: new job allowed after TTL expires', async ({ assert }) => {
    const sharedAdapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    })

    const first = await new JobDispatcher('ThrottleJob', { n: 1 })
      .dedup({ id: 'throttle-1', ttl: 80 })
      .run()
    assert.equal(first.deduped, 'added')

    const second = await new JobDispatcher('ThrottleJob', { n: 2 })
      .dedup({ id: 'throttle-1', ttl: 80 })
      .run()
    assert.equal(second.deduped, 'skipped')

    await setTimeout(150)

    const third = await new JobDispatcher('ThrottleJob', { n: 3 })
      .dedup({ id: 'throttle-1', ttl: 80 })
      .run()
    assert.equal(third.deduped, 'added')
    assert.notEqual(third.jobId, first.jobId)

    const size = await sharedAdapter.size()
    assert.equal(size, 2)
  })

  test('extend: duplicate within TTL resets the window', async ({ assert }) => {
    const sharedAdapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    })

    const first = await new JobDispatcher('ExtendJob', { n: 1 })
      .dedup({ id: 'ext-1', ttl: 100, extend: true })
      .run()
    assert.equal(first.deduped, 'added')

    await setTimeout(60)

    const second = await new JobDispatcher('ExtendJob', { n: 2 })
      .dedup({ id: 'ext-1', ttl: 100, extend: true })
      .run()
    assert.equal(second.deduped, 'extended')
    assert.equal(second.jobId, first.jobId)

    await setTimeout(60)

    // Without extend, original 40ms TTL would've expired (50ms elapsed).
    // With extend, second push reset timer → still within window.
    const third = await new JobDispatcher('ExtendJob', { n: 3 })
      .dedup({ id: 'ext-1', ttl: 100, extend: true })
      .run()
    assert.equal(third.deduped, 'extended')
  })

  test('replace: duplicate within TTL swaps the pending job payload', async ({ assert }) => {
    const sharedAdapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    })

    const first = await new JobDispatcher('ReplaceJob', { version: 1 })
      .dedup({ id: 'draft-1', ttl: 100, replace: true })
      .run()
    assert.equal(first.deduped, 'added')

    const second = await new JobDispatcher('ReplaceJob', { version: 2 })
      .dedup({ id: 'draft-1', ttl: 100, replace: true })
      .run()
    assert.equal(second.deduped, 'replaced')
    assert.equal(second.jobId, first.jobId)

    const size = await sharedAdapter.size()
    assert.equal(size, 1)

    const job = await sharedAdapter.pop()
    assert.deepEqual(job!.payload, { version: 2 })
  })

  test('replace: active job is not replaced (returns skipped)', async ({ assert }) => {
    const sharedAdapter = memory()()

    await QueueManager.init({
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    })

    await new JobDispatcher('ActiveReplaceJob', { version: 1 })
      .dedup({ id: 'ar-1', ttl: 1000, replace: true })
      .run()

    await sharedAdapter.pop() // moves to active

    const second = await new JobDispatcher('ActiveReplaceJob', { version: 2 })
      .dedup({ id: 'ar-1', ttl: 1000, replace: true })
      .run()

    assert.equal(second.deduped, 'skipped')
  })
})

test.group('JobBatchDispatcher', () => {
  test('should dispatch multiple jobs correctly', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobBatchDispatcher('SendEmailJob', [
      { to: 'user1@example.com' },
      { to: 'user2@example.com' },
      { to: 'user3@example.com' },
    ])

    const { jobIds } = await dispatcher.run()

    assert.isArray(jobIds)
    assert.lengthOf(jobIds, 3)

    const job1 = await sharedAdapter.pop()
    const job2 = await sharedAdapter.pop()
    const job3 = await sharedAdapter.pop()
    const job4 = await sharedAdapter.pop()

    assert.isNotNull(job1)
    assert.isNotNull(job2)
    assert.isNotNull(job3)
    assert.isNull(job4)

    assert.include(jobIds, job1!.id)
    assert.include(jobIds, job2!.id)
    assert.include(jobIds, job3!.id)
  })

  test('should dispatch jobs to specified queue', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobBatchDispatcher('ReportJob', [{ reportId: 1 }, { reportId: 2 }])

    const { jobIds } = await dispatcher.toQueue('reports').run()

    assert.lengthOf(jobIds, 2)

    const job1 = await sharedAdapter.popFrom('reports')
    const job2 = await sharedAdapter.popFrom('reports')

    assert.isNotNull(job1)
    assert.isNotNull(job2)
    assert.equal(job1!.name, 'ReportJob')
    assert.equal(job2!.name, 'ReportJob')
  })

  test('should dispatch jobs with shared groupId', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobBatchDispatcher('NewsletterJob', [
      { userId: 1 },
      { userId: 2 },
      { userId: 3 },
    ])

    await dispatcher.group('newsletter-jan-2025').run()

    const job1 = await sharedAdapter.pop()
    const job2 = await sharedAdapter.pop()
    const job3 = await sharedAdapter.pop()

    assert.equal(job1!.groupId, 'newsletter-jan-2025')
    assert.equal(job2!.groupId, 'newsletter-jan-2025')
    assert.equal(job3!.groupId, 'newsletter-jan-2025')
  })

  test('should dispatch jobs with shared priority', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobBatchDispatcher('UrgentJob', [{ task: 'a' }, { task: 'b' }])

    await dispatcher.priority(1).run()

    const job1 = await sharedAdapter.pop()
    const job2 = await sharedAdapter.pop()

    assert.equal(job1!.priority, 1)
    assert.equal(job2!.priority, 1)
  })

  test('should dispatch empty array without error', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobBatchDispatcher('EmptyJob', [])

    const { jobIds } = await dispatcher.run()

    assert.isArray(jobIds)
    assert.lengthOf(jobIds, 0)
  })

  test('should support promise then() for auto-dispatching', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobBatchDispatcher('AutoJob', [{ id: 1 }, { id: 2 }])

    const { jobIds } = await dispatcher.toQueue('auto-queue')

    assert.isArray(jobIds)
    assert.lengthOf(jobIds, 2)

    const size = await sharedAdapter.sizeOf('auto-queue')
    assert.equal(size, 2)
  })

  test('should work with custom adapter', async ({ assert }) => {
    const memoryAdapter = memory()()
    const anotherAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => memoryAdapter },
    }

    await QueueManager.init(localConfig)

    const dispatcher = new JobBatchDispatcher('CustomAdapterJob', [{ data: 1 }, { data: 2 }])

    await dispatcher.with(() => anotherAdapter).run()

    const job1 = await anotherAdapter.pop()
    const job2 = await anotherAdapter.pop()

    assert.isNotNull(job1)
    assert.isNotNull(job2)

    const emptyJob = await memoryAdapter.pop()
    assert.isNull(emptyJob)
  })

  test('should combine all options', async ({ assert }) => {
    const sharedAdapter = memory()()

    const localConfig = {
      default: 'memory',
      adapters: { memory: () => sharedAdapter },
    }

    await QueueManager.init(localConfig)

    const { jobIds } = await new JobBatchDispatcher('CompleteJob', [
      { item: 'a' },
      { item: 'b' },
      { item: 'c' },
    ])
      .toQueue('complete-queue')
      .group('batch-999')
      .priority(3)
      .run()

    assert.lengthOf(jobIds, 3)

    const job = await sharedAdapter.popFrom('complete-queue')

    assert.isNotNull(job)
    assert.equal(job!.groupId, 'batch-999')
    assert.equal(job!.priority, 3)
    assert.equal(job!.name, 'CompleteJob')
  })
})
