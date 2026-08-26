import { test } from '@japa/runner'
import { Job } from '../src/job.js'
import { Locator } from '../src/locator.js'
import { QueueManager } from '../src/queue_manager.js'
import { Schedule } from '../src/schedule.js'
import { Worker } from '../src/worker.js'
import type { JobData, QueueManagerConfig } from '../src/types/main.js'
import { memory } from './_mocks/memory_adapter.js'

test('every Job dispatch path applies the same policy', async ({ assert, cleanup }) => {
  class ParityJob extends Job<{ source: string }> {
    static options = { queue: 'parity', adapter: 'owner', priority: 3 }

    async execute() {}
  }

  const defaultAdapter = memory()()
  const queueAdapter = memory()()
  const ownerAdapter = memory()()
  const config = {
    default: 'default',
    adapters: {
      default: () => defaultAdapter,
      queue: () => queueAdapter,
      owner: () => ownerAdapter,
    },
    queues: {
      parity: { adapter: 'queue' },
    },
    worker: { adapter: 'owner' },
  } satisfies QueueManagerConfig

  await QueueManager.init(config)
  Locator.register('ParityJob', ParityJob)

  const worker = new Worker(config)
  cleanup(async () => {
    Locator.clear()
    await worker.stop()
    await QueueManager.destroy()
  })

  await ParityJob.dispatch({ source: 'single' })
  const single = await ownerAdapter.popFrom('parity')

  await ParityJob.dispatchMany([{ source: 'batch-1' }, { source: 'batch-2' }])
  const batch = [await ownerAdapter.popFrom('parity'), await ownerAdapter.popFrom('parity')]

  await ParityJob.schedule({ source: 'manual' }).id('manual-schedule').every('1h').run()
  const manualSchedule = await Schedule.find('manual-schedule', { adapter: 'owner' })
  await manualSchedule!.trigger()
  const manual = await ownerAdapter.popFrom('parity')

  await ParityJob.schedule({ source: 'due' }).id('due-schedule').every('1h').run()
  await ownerAdapter.updateSchedule('due-schedule', {
    nextRunAt: new Date(Date.now() - 1000),
  })
  const dueCycle = await worker.processCycle(['parity'])
  const due = dueCycle?.type === 'started' ? dueCycle.job : null

  const describe = (job: JobData | null) => ({
    payload: job?.payload,
    priority: job?.priority,
    hasCreatedAt: typeof job?.createdAt === 'number',
    scheduleId: job?.scheduleId,
  })

  assert.deepEqual([single, ...batch, manual, due].map(describe), [
    {
      payload: { source: 'single' },
      priority: 3,
      hasCreatedAt: true,
      scheduleId: undefined,
    },
    {
      payload: { source: 'batch-1' },
      priority: 3,
      hasCreatedAt: true,
      scheduleId: undefined,
    },
    {
      payload: { source: 'batch-2' },
      priority: 3,
      hasCreatedAt: true,
      scheduleId: undefined,
    },
    {
      payload: { source: 'manual' },
      priority: 3,
      hasCreatedAt: true,
      scheduleId: 'manual-schedule',
    },
    {
      payload: { source: 'due' },
      priority: 3,
      hasCreatedAt: true,
      scheduleId: 'due-schedule',
    },
  ])
  assert.equal(await defaultAdapter.sizeOf('parity'), 0)
  assert.equal(await queueAdapter.sizeOf('parity'), 0)
})
