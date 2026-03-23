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
